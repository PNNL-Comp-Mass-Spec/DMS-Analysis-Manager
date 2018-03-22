using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PRISM.Logging;
using MyEMSLReader;
using PRISM;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Methods to look for folders related to datasets
    /// </summary>
    public class clsFolderSearch : clsEventNotifier
    {
        #region "Constants"

        /// <summary>
        /// Maximum number of attempts to find a folder or file
        /// </summary>
        /// <remarks></remarks>
        public const int DEFAULT_MAX_RETRY_COUNT = 3;

        /// <summary>
        /// Default number of seconds to wait between trying to find a folder
        /// </summary>
        protected const int DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS = 5;

        private const string MYEMSL_PATH_FLAG = clsMyEMSLUtilities.MYEMSL_PATH_FLAG;

        #endregion

        #region "Module variables"

        private readonly bool m_AuroraAvailable;

        private readonly int m_DebugLevel;

        private readonly IJobParams m_jobParams;

        private readonly clsMyEMSLUtilities m_MyEMSLUtilities;

        private readonly clsFileCopyUtilities m_FileCopyUtilities;

        #endregion

        #region "Properties"

        /// <summary>
        /// Dataset name
        /// </summary>
        public string DatasetName { get; set; }

        /// <summary>
        /// True if MyEMSL search is disabled
        /// </summary>
        public bool MyEMSLSearchDisabled { get; set; }

        #endregion

        #region "Events"

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fileCopyUtilities"></param>
        /// <param name="jobParams"></param>
        /// <param name="myEmslUtilities"></param>
        /// <param name="datasetName"></param>
        /// <param name="debugLevel"></param>
        /// <param name="auroraAvailable"></param>
        public clsFolderSearch(
            clsFileCopyUtilities fileCopyUtilities,
            IJobParams jobParams,
            clsMyEMSLUtilities myEmslUtilities,
            string datasetName,
            short debugLevel,
            bool auroraAvailable)
        {
            m_FileCopyUtilities = fileCopyUtilities;
            m_jobParams = jobParams;
            m_MyEMSLUtilities = myEmslUtilities;
            DatasetName = datasetName;
            m_DebugLevel = debugLevel;
            m_AuroraAvailable = auroraAvailable;
        }

        /// <summary>
        /// Add a folder or file path to a list of paths to examine
        /// </summary>
        /// <param name="lstPathsToCheck">List of Tuples where the string is a folder or file path, and the boolean is logIfMissing</param>
        /// <param name="folderOrFilePath">Path to add</param>
        /// <param name="logIfMissing">True to log a message if the path is not found</param>
        /// <remarks></remarks>
        private void AddPathToCheck(ICollection<Tuple<string, bool>> lstPathsToCheck, string folderOrFilePath, bool logIfMissing)
        {
            lstPathsToCheck.Add(new Tuple<string, bool>(folderOrFilePath, logIfMissing));
        }

        /// <summary>
        /// Determines the full path to the dataset file
        /// Returns a folder path for data that is stored in folders (e.g. .D folders)
        /// For instruments with multiple data folders, returns the path to the first folder
        /// For instrument with multiple zipped data files, returns the dataset folder path
        /// </summary>
        /// <param name="isFolder">Output variable: true if the path returned is a folder path; false if a file</param>
        /// <param name="assumeUnpurged">
        /// When true, assume that the instrument data exists on the storage server
        /// (and thus do not search MyEMSL or the archive for the file)
        /// </param>
        /// <returns>The full path to the dataset file or folder</returns>
        /// <remarks>When assumeUnpurged is true, this function returns the expected path
        /// to the instrument data file (or folder) on the storage server, even if the file/folder wasn't actually found</remarks>
        public string FindDatasetFileOrFolder(out bool isFolder, bool assumeUnpurged)
        {
            return FindDatasetFileOrFolder(DEFAULT_MAX_RETRY_COUNT, out isFolder, assumeUnpurged: assumeUnpurged);
        }

        /// <summary>
        /// Determines the full path to the dataset file
        /// Returns a folder path for data that is stored in folders (e.g. .D folders)
        /// For instruments with multiple data folders, returns the path to the first folder
        /// For instrument with multiple zipped data files, returns the dataset folder path
        /// </summary>
        /// <param name="maxAttempts">Maximum number of attempts to look for the folder</param>
        /// <param name="isFolder">Output variable: true if the path returned is a folder path; false if a file</param>
        /// <param name="assumeUnpurged">
        /// When true, assume that the instrument data exists on the storage server
        /// (and thus do not search MyEMSL or the archive for the file)
        /// </param>
        /// <returns>The full path to the dataset file or folder</returns>
        /// <remarks>When assumeUnpurged is true, this function returns the expected path
        /// to the instrument data file (or folder) on the storage server, even if the file/folder wasn't actually found</remarks>
        public string FindDatasetFileOrFolder(int maxAttempts, out bool isFolder, bool assumeUnpurged = false)
        {
            var RawDataType = m_jobParams.GetParam("RawDataType");
            var StoragePath = m_jobParams.GetParam("DatasetStoragePath");
            var fileOrFolderPath = string.Empty;

            isFolder = false;

            var eRawDataType = clsAnalysisResources.GetRawDataType(RawDataType);
            switch (eRawDataType)
            {
                case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                    // Agilent ion trap data

                    if (StoragePath.ToLower().Contains("Agilent_SL1".ToLower()) || StoragePath.ToLower().Contains("Agilent_XCT1".ToLower()))
                    {
                        // For Agilent Ion Trap datasets acquired on Agilent_SL1 or Agilent_XCT1 in 2005,
                        //  we would pre-process the data beforehand to create MGF files
                        // The following call can be used to retrieve the files
                        fileOrFolderPath = FindMGFFile(maxAttempts, assumeUnpurged);
                    }
                    else
                    {
                        // DeconTools_V2 now supports reading the .D files directly
                        // Call RetrieveDotDFolder() to copy the folder and all subfolders
                        fileOrFolderPath = FindDotDFolder(assumeUnpurged);
                        isFolder = true;
                    }

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile:
                    // Agilent/QSTAR TOF data
                    fileOrFolderPath = FindDatasetFile(maxAttempts, clsAnalysisResources.DOT_WIFF_EXTENSION, assumeUnpurged);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders:
                    // FTICR data
                    fileOrFolderPath = FindSFolders(assumeUnpurged);
                    isFolder = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                    // Finnigan ion trap/LTQ-FT data
                    fileOrFolderPath = FindDatasetFile(maxAttempts, clsAnalysisResources.DOT_RAW_EXTENSION, assumeUnpurged);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder:
                    // Micromass QTOF data
                    fileOrFolderPath = FindDotRawFolder(assumeUnpurged);
                    isFolder = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.UIMF:
                    // IMS UIMF data
                    fileOrFolderPath = FindDatasetFile(maxAttempts, clsAnalysisResources.DOT_UIMF_EXTENSION, assumeUnpurged);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzXML:
                    fileOrFolderPath = FindDatasetFile(maxAttempts, clsAnalysisResources.DOT_MZXML_EXTENSION, assumeUnpurged);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzML:
                    fileOrFolderPath = FindDatasetFile(maxAttempts, clsAnalysisResources.DOT_MZML_EXTENSION, assumeUnpurged);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:
                    // Call RetrieveDotDFolder() to copy the folder and all subfolders

                    // Both the MSXml step tool and DeconTools require the .Baf file
                    // We previously didn't need this file for DeconTools, but, now that DeconTools is using CompassXtract, so we need the file

                    fileOrFolderPath = FindDotDFolder(assumeUnpurged);
                    isFolder = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging:
                    fileOrFolderPath = FindBrukerMALDIImagingFolders(assumeUnpurged);
                    isFolder = true;

                    break;
            }

            return fileOrFolderPath;
        }


        /// <summary>
        /// Finds the dataset folder containing Bruker Maldi imaging .zip files
        /// </summary>
        /// <param name="assumeUnpurged"></param>
        /// <returns>The full path to the dataset folder</returns>
        public string FindBrukerMALDIImagingFolders(bool assumeUnpurged = false)
        {

            const string ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK = "*R*X*.zip";

            // Look for the dataset folder; it must contain .Zip files with names like 0_R00X442.zip
            // If a matching folder isn't found, ServerPath will contain the folder path defined by Job Param "DatasetStoragePath"

            var DSFolderPath = FindValidFolder(DatasetName, ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK,
                                                  RetrievingInstrumentDataFolder: true, assumeUnpurged: assumeUnpurged);

            if (string.IsNullOrEmpty(DSFolderPath))
                return string.Empty;

            return DSFolderPath;

        }

        /// <summary>
        /// Finds a file named DatasetName.FileExtension
        /// </summary>
        /// <param name="FileExtension"></param>
        /// <returns>The full path to the folder; an empty string if no match</returns>
        /// <remarks></remarks>
        public string FindDatasetFile(string FileExtension)
        {
            return FindDatasetFile(DEFAULT_MAX_RETRY_COUNT, FileExtension);
        }

        /// <summary>
        /// Finds a file named DatasetName.FileExtension
        /// </summary>
        /// <param name="maxAttempts">Maximum number of attempts to look for the folder</param>
        /// <param name="fileExtension"></param>
        /// <param name="assumeUnpurged"></param>
        /// <returns>The full path to the folder; an empty string if no match</returns>
        /// <remarks></remarks>
        public string FindDatasetFile(int maxAttempts, string fileExtension, bool assumeUnpurged = false)
        {

            if (!fileExtension.StartsWith("."))
            {
                fileExtension = "." + fileExtension;
            }

            var DataFileName = DatasetName + fileExtension;

            var DSFolderPath = FindValidFolder(DatasetName, DataFileName, folderNameToFind: "", maxAttempts: maxAttempts,
                logFolderNotFound: true, retrievingInstrumentDataFolder: false,
                assumeUnpurged: assumeUnpurged,
                validFolderFound: out var validFolderFound, folderNotFoundMessage: out var folderNotFoundMessage);

            if (!string.IsNullOrEmpty(DSFolderPath))
            {
                return Path.Combine(DSFolderPath, DataFileName);
            }

            return string.Empty;
        }

        /// <summary>
        /// Finds a .Raw folder below the dataset folder
        /// </summary>
        /// <param name="assumeUnpurged"></param>
        /// <returns>The full path to the folder; an empty string if no match</returns>
        private string FindDotDFolder(bool assumeUnpurged = false)
        {
            return FindDotXFolder(clsAnalysisResources.DOT_D_EXTENSION, assumeUnpurged);
        }

        /// <summary>
        /// Finds a .D folder below the dataset folder
        /// </summary>
        /// <param name="assumeUnpurged"></param>
        /// <returns></returns>
        private string FindDotRawFolder(bool assumeUnpurged = false)
        {
            return FindDotXFolder(clsAnalysisResources.DOT_RAW_EXTENSION, assumeUnpurged);
        }

        /// <summary>
        /// Finds a subfolder (typically Dataset.D or Dataset.Raw) below the dataset folder
        /// </summary>
        /// <param name="folderExtension"></param>
        /// <param name="assumeUnpurged"></param>
        /// <returns>The full path to the folder; an empty string if no match</returns>
        /// <remarks></remarks>
        public string FindDotXFolder(string folderExtension, bool assumeUnpurged)
        {

            if (!folderExtension.StartsWith("."))
            {
                folderExtension = "." + folderExtension;
            }


            var fileNameToFind = string.Empty;
            var folderExtensionWildcard = "*" + folderExtension;

            var serverPath = FindValidFolder(
                DatasetName,
                fileNameToFind,
                folderExtensionWildcard,
                DEFAULT_MAX_RETRY_COUNT,
                logFolderNotFound: true,
                retrievingInstrumentDataFolder: true,
                assumeUnpurged: assumeUnpurged,
                validFolderFound: out var validFolderFound,
                folderNotFoundMessage: out var folderNotFoundMessage);

            if (serverPath.StartsWith(MYEMSL_PATH_FLAG))
            {
                return serverPath;
            }

            var diDatasetFolder = new DirectoryInfo(serverPath);

            // Find the instrument data folder (e.g. Dataset.D or Dataset.Raw) in the dataset folder
            foreach (var diSubFolder in diDatasetFolder.GetDirectories(folderExtensionWildcard))
            {
                return diSubFolder.FullName;
            }

            // No match found
            return string.Empty;

        }

        /// <summary>
        /// Finds the best .mgf file for the current dataset
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public string FindMGFFile(int maxAttempts, bool assumeUnpurged)
        {

            // Data files are in a subfolder off of the main dataset folder
            // Files are renamed with dataset name because MASIC requires this. Other analysis types don't care


            var serverPath = FindValidFolder(DatasetName, "", "*" + clsAnalysisResources.DOT_D_EXTENSION, maxAttempts,
                logFolderNotFound: true, retrievingInstrumentDataFolder: false,
                assumeUnpurged: assumeUnpurged,
                validFolderFound: out var validFolderFound, folderNotFoundMessage: out var folderNotFoundMessage);

            var diServerFolder = new DirectoryInfo(serverPath);

            // Get a list of the subfolders in the dataset folder
            // Go through the folders looking for a file with a ".mgf" extension

            foreach (var diSubFolder in diServerFolder.GetDirectories())
            {
                foreach (var fiFile in diSubFolder.GetFiles("*" + clsAnalysisResources.DOT_MGF_EXTENSION))
                {
                    // Return the first .mgf file that was found
                    return fiFile.FullName;
                }
            }

            // No match was found
            return string.Empty;

        }

        /// <summary>
        /// Finds the dataset folder containing either a 0.ser subfolder or containing zipped S-folders
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        private string FindSFolders(bool assumeUnpurged = false)
        {

            // First Check for the existence of a 0.ser Folder
            var FileNameToFind = string.Empty;

            var DSFolderPath = FindValidFolder(DatasetName, FileNameToFind, clsAnalysisResources.BRUKER_ZERO_SER_FOLDER, DEFAULT_MAX_RETRY_COUNT,
                logFolderNotFound: true, retrievingInstrumentDataFolder: true,
                assumeUnpurged: assumeUnpurged,
                validFolderFound: out var validFolderFound, folderNotFoundMessage: out var folderNotFoundMessage);

            if (!string.IsNullOrEmpty(DSFolderPath))
            {
                return Path.Combine(DSFolderPath, clsAnalysisResources.BRUKER_ZERO_SER_FOLDER);
            }

            // The 0.ser folder does not exist; look for zipped s-folders
            DSFolderPath = FindValidFolder(DatasetName, "s*.zip", RetrievingInstrumentDataFolder: true, assumeUnpurged: assumeUnpurged);

            return DSFolderPath;

        }

        /// <summary>
        /// Determines the most appropriate folder to use to obtain dataset files from
        /// Optionally, can require that a certain file also be present in the folder for it to be deemed valid
        /// If no folder is deemed valid, returns the path defined by "DatasetStoragePath"
        /// </summary>
        /// <param name="dsName">Name of the dataset</param>
        /// <param name="fileNameToFind">Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
        /// <param name="RetrievingInstrumentDataFolder">Set to True when retrieving an instrument data folder</param>
        /// <returns>Path to the most appropriate dataset folder</returns>
        /// <remarks>Although FileNameToFind could be empty, you are highly encouraged to filter by either Filename or by FolderName when using FindValidFolder</remarks>
        public string FindValidFolder(string dsName, string fileNameToFind, bool RetrievingInstrumentDataFolder)
        {

            const string folderNameToFind = "";
            return FindValidFolder(dsName, fileNameToFind, folderNameToFind, DEFAULT_MAX_RETRY_COUNT,
                logFolderNotFound: true, retrievingInstrumentDataFolder: RetrievingInstrumentDataFolder);

        }

        /// <summary>
        /// Determines the most appropriate folder to use to obtain dataset files from
        /// Optionally, can require that a certain file also be present in the folder for it to be deemed valid
        /// If no folder is deemed valid, returns the path defined by "DatasetStoragePath"
        /// </summary>
        /// <param name="dsName">Name of the dataset</param>
        /// <param name="fileNameToFind">Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
        /// <param name="RetrievingInstrumentDataFolder">Set to True when retrieving an instrument data folder</param>
        /// <param name="assumeUnpurged"></param>
        /// <returns>Path to the most appropriate dataset folder</returns>
        /// <remarks>Although FileNameToFind could be empty, you are highly encouraged to filter by either Filename or by FolderName when using FindValidFolder</remarks>
        private string FindValidFolder(string dsName, string fileNameToFind, bool RetrievingInstrumentDataFolder, bool assumeUnpurged)
        {

            const string folderNameToFind = "";

            return FindValidFolder(dsName, fileNameToFind, folderNameToFind, DEFAULT_MAX_RETRY_COUNT,
                logFolderNotFound: true, retrievingInstrumentDataFolder: RetrievingInstrumentDataFolder,
                assumeUnpurged: assumeUnpurged,
                validFolderFound: out var validFolderFound, folderNotFoundMessage: out var folderNotFoundMessage);

        }

        /// <summary>
        /// Determines the most appropriate folder to use to obtain dataset files from
        /// Optionally, can require that a certain file also be present in the folder for it to be deemed valid
        /// If no folder is deemed valid, returns the path defined by "DatasetStoragePath"
        /// </summary>
        /// <param name="dsName">Name of the dataset</param>
        /// <param name="fileNameToFind">Name of a file that must exist in the folder; can contain a wildcard, e.g. *.zip</param>
        /// <param name="folderNameToFind">Optional: Name of a folder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
        /// <param name="maxRetryCount">Maximum number of attempts</param>
        /// <returns>Path to the maxRetryCount appropriate dataset folder</returns>
        /// <remarks>Although FileNameToFind and FolderNameToFind could both be empty, you are highly encouraged to filter by either Filename or by FolderName when using FindValidFolder</remarks>
        public string FindValidFolder(string dsName, string fileNameToFind, string folderNameToFind, int maxRetryCount)
        {

            return FindValidFolder(dsName, fileNameToFind, folderNameToFind, maxRetryCount,
                logFolderNotFound: true, retrievingInstrumentDataFolder: false);

        }

        /// <summary>
        /// Determines the most appropriate folder to use to obtain dataset files from
        /// Optionally, can require that a certain file also be present in the folder for it to be deemed valid
        /// If no folder is deemed valid, returns the path defined by Job Param "DatasetStoragePath"
        /// </summary>
        /// <param name="dsName">Name of the dataset</param>
        /// <param name="fileNameToFind">Optional: Name of a file that must exist in the dataset folder; can contain a wildcard, e.g. *.zip</param>
        /// <param name="folderNameToFind">Optional: Name of a subfolder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
        /// <param name="maxRetryCount">Maximum number of attempts</param>
        /// <param name="logFolderNotFound">If true, log a warning if the folder is not found</param>
        /// <param name="retrievingInstrumentDataFolder">Set to True when retrieving an instrument data folder</param>
        /// <returns>Path to the most appropriate dataset folder</returns>
        /// <remarks>The path returned will be "\\MyEMSL" if the best folder is in MyEMSL</remarks>
        public string FindValidFolder(
            string dsName, string fileNameToFind, string folderNameToFind,
            int maxRetryCount, bool logFolderNotFound, bool retrievingInstrumentDataFolder)
        {


            return FindValidFolder(dsName, fileNameToFind, folderNameToFind, maxRetryCount, logFolderNotFound, retrievingInstrumentDataFolder,
                assumeUnpurged: false,
                validFolderFound: out var validFolderFound, folderNotFoundMessage: out var folderNotFoundMessage);

        }

        /// <summary>
        /// Determines the most appropriate folder to use to obtain dataset files from
        /// Optionally, can require that a certain file also be present in the folder for it to be deemed valid
        /// If no folder is deemed valid, returns the path defined by Job Param "DatasetStoragePath"
        /// </summary>
        /// <param name="dsName">Name of the dataset</param>
        /// <param name="fileNameToFind">Optional: Name of a file that must exist in the dataset folder; can contain a wildcard, e.g. *.zip</param>
        /// <param name="folderNameToFind">Optional: Name of a subfolder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
        /// <param name="maxAttempts">Maximum number of attempts</param>
        /// <param name="logFolderNotFound">If true, log a warning if the folder is not found</param>
        /// <param name="retrievingInstrumentDataFolder">Set to True when retrieving an instrument data folder</param>
        /// <param name="assumeUnpurged">When true, this function returns the path to the dataset folder on the storage server</param>
        /// <param name="validFolderFound">Output parameter: True if a valid folder is ultimately found, otherwise false</param>
        /// <param name="folderNotFoundMessage">Output parameter: description to be used when validFolderFound is false</param>
        /// <returns>Path to the most appropriate dataset folder</returns>
        /// <remarks>The path returned will be "\\MyEMSL" if the best folder is in MyEMSL</remarks>
        public string FindValidFolder(
            string dsName,
            string fileNameToFind,
            string folderNameToFind,
            int maxAttempts,
            bool logFolderNotFound,
            bool retrievingInstrumentDataFolder,
            bool assumeUnpurged,
            out bool validFolderFound,
            out string folderNotFoundMessage)
        {
            var bestPath = string.Empty;

            // The tuples in this list are the path to check, and True if we should warn that the folder was not found
            var lstPathsToCheck = new List<Tuple<string, bool>>();

            var validFolder = false;

            validFolderFound = false;
            folderNotFoundMessage = string.Empty;

            try
            {
                if (fileNameToFind == null)
                    fileNameToFind = string.Empty;
                if (folderNameToFind == null)
                    folderNameToFind = string.Empty;

                if (assumeUnpurged)
                {
                    maxAttempts = 1;
                    logFolderNotFound = false;
                }

                var instrumentDataPurged = m_jobParams.GetJobParameter("InstrumentDataPurged", 0);

                var datasetFolderName = m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME);

                if (retrievingInstrumentDataFolder && instrumentDataPurged != 0 && !assumeUnpurged)
                {
                    // The instrument data is purged and we're retrieving instrument data
                    // Skip the primary dataset folder since the primary data files were most likely purged
                }
                else
                {
                    AddPathToCheck(lstPathsToCheck, Path.Combine(m_jobParams.GetParam("DatasetStoragePath"), datasetFolderName), true);
                    if (datasetFolderName != dsName)
                        AddPathToCheck(lstPathsToCheck, Path.Combine(m_jobParams.GetParam("DatasetStoragePath"), dsName), false);
                }

                if (!MyEMSLSearchDisabled && !assumeUnpurged)
                {
                    // \\MyEMSL
                    AddPathToCheck(lstPathsToCheck, MYEMSL_PATH_FLAG, false);
                }

                // Optional Temp Debug: Enable compilation constant DISABLE_MYEMSL_SEARCH to disable checking MyEMSL (and thus speed things up)
#if DISABLE_MYEMSL_SEARCH
        if (m_mgrParams.GetParam("MgrName").ToLower().Contains("monroe")) {
            lstPathsToCheck.Remove(MYEMSL_PATH_FLAG);
        }
#endif
                if ((m_AuroraAvailable || MyEMSLSearchDisabled) && !assumeUnpurged)
                {
                    AddPathToCheck(lstPathsToCheck, Path.Combine(m_jobParams.GetParam("DatasetArchivePath"), datasetFolderName), true);
                    if (datasetFolderName != dsName)
                        AddPathToCheck(lstPathsToCheck, Path.Combine(m_jobParams.GetParam("DatasetArchivePath"), dsName), false);
                }

                AddPathToCheck(lstPathsToCheck, Path.Combine(m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH), datasetFolderName), false);
                if (datasetFolderName != dsName)
                    AddPathToCheck(lstPathsToCheck, Path.Combine(m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH), dsName), false);

                var fileNotFoundEncountered = false;

                bestPath = lstPathsToCheck.First().Item1;
                foreach (var pathToCheck in lstPathsToCheck)
                {
                    try
                    {
                        if (m_DebugLevel > 3)
                        {
                            var msg = "FindValidDatasetFolder, Looking for folder " + pathToCheck.Item1;
                            OnDebugEvent(msg);
                        }

                        if (pathToCheck.Item1 == MYEMSL_PATH_FLAG)
                        {
                            const bool recurseMyEMSL = false;
                            validFolder = FindValidFolderMyEMSL(dsName, fileNameToFind, folderNameToFind, false, recurseMyEMSL);
                        }
                        else
                        {
                            validFolder = FindValidFolderUNC(pathToCheck.Item1, fileNameToFind, folderNameToFind, maxAttempts, logFolderNotFound && pathToCheck.Item2);

                            if (!validFolder && !string.IsNullOrEmpty(fileNameToFind) && !string.IsNullOrEmpty(folderNameToFind))
                            {
                                // Look for a subfolder named folderNameToFind that contains file fileNameToFind
                                var pathToCheckAlt = Path.Combine(pathToCheck.Item1, folderNameToFind);
                                validFolder = FindValidFolderUNC(pathToCheckAlt, fileNameToFind, string.Empty, maxAttempts, logFolderNotFound && pathToCheck.Item2);

                                if (validFolder)
                                {
                                    var pathToCheckOverride = new Tuple<string, bool>(pathToCheckAlt, pathToCheck.Item2);
                                    bestPath = string.Copy(pathToCheck.Item1);
                                    break;
                                }
                            }

                        }

                        if (validFolder)
                        {
                            bestPath = string.Copy(pathToCheck.Item1);
                            break;
                        }

                        fileNotFoundEncountered = true;

                    }
                    catch (Exception ex)
                    {
                        OnErrorEvent("Exception looking for folder: " + pathToCheck.Item1, ex);
                    }
                } // for each item in lstPathsToCheck

                if (validFolder)
                {
                    validFolderFound = true;

                    if (m_DebugLevel >= 4 || m_DebugLevel >= 1 && fileNotFoundEncountered)
                    {
                        var msg = "FindValidFolder, Valid dataset folder has been found:  " + bestPath;
                        if (fileNameToFind.Length > 0)
                        {
                            msg += " (matched file " + fileNameToFind + ")";
                        }
                        if (folderNameToFind.Length > 0)
                        {
                            msg += " (matched folder " + folderNameToFind + ")";
                        }
                        OnDebugEvent(msg);
                    }

                }
                else
                {
                    folderNotFoundMessage = "Could not find a valid dataset folder";
                    if (fileNameToFind.Length > 0)
                    {
                        // Could not find a valid dataset folder containing file
                        folderNotFoundMessage += " containing file " + fileNameToFind;
                    }

                    if (logFolderNotFound && m_DebugLevel >= 1)
                    {
                        if (assumeUnpurged)
                        {
                            OnStatusEvent(folderNotFoundMessage);
                        }
                        else
                        {
                            var msg = folderNotFoundMessage + ", Job " + m_jobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job") + ", Dataset " + dsName;
                            OnWarningEvent(msg);
                        }
                    }

                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception looking for a valid dataset folder for dataset " + dsName, ex);
                folderNotFoundMessage = "Exception looking for a valid dataset folder";
            }

            return bestPath;

        }

        /// <summary>
        /// Determines whether the folder specified by pathToCheck is appropriate for retrieving dataset files
        /// </summary>
        /// <param name="dataset">Dataset name</param>
        /// <param name="fileNameToFind">Optional: Name of a file that must exist in the dataset folder; can contain a wildcard, e.g. *.zip</param>
        /// <param name="subFolderName">Optional: Name of a subfolder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
        /// <param name="logFolderNotFound">If true, log a warning if the folder is not found</param>
        /// <param name="recurse">True to look for fileNameToFind in all subfolders of a dataset; false to only look in the primary dataset folder</param>
        /// <returns>Path to the most appropriate dataset folder</returns>
        /// <remarks>FileNameToFind is a file in the dataset folder; it is NOT a file in FolderNameToFind</remarks>
        private bool FindValidFolderMyEMSL(string dataset, string fileNameToFind, string subFolderName, bool logFolderNotFound, bool recurse)
        {

            if (string.IsNullOrEmpty(fileNameToFind))
                fileNameToFind = "*";

            if (m_DebugLevel > 3)
            {
                OnDebugEvent("FindValidFolderMyEMSL, querying MyEMSL for this dataset's files");
            }

            List<DatasetFolderOrFileInfo> matchingMyEMSLFiles;

            if (string.IsNullOrEmpty(subFolderName))
            {
                // Simply look for the file
                matchingMyEMSLFiles = m_MyEMSLUtilities.FindFiles(fileNameToFind, string.Empty, dataset, recurse);
            }
            else
            {
                // First look for the subfolder
                // If there are multiple matching subfolders, choose the newest one
                // The entries in matchingMyEMSLFiles will be folder entries where the "Filename" field is the folder name while the "SubDirPath" field is any parent folders above the found folder
                matchingMyEMSLFiles = m_MyEMSLUtilities.FindFiles(fileNameToFind, subFolderName, dataset, recurse);
            }

            if (matchingMyEMSLFiles.Count > 0)
            {
                return true;
            }

            if (logFolderNotFound)
            {
                var msg = "MyEMSL does not have any files for dataset " + dataset;
                if (!string.IsNullOrEmpty(fileNameToFind))
                {
                    msg += " and file " + fileNameToFind;
                }

                if (!string.IsNullOrEmpty(subFolderName))
                {
                    msg += " and subfolder " + subFolderName;
                }

                OnWarningEvent(msg);
            }
            return false;
        }

        /// <summary>
        /// Determines whether the folder specified by pathToCheck is appropriate for retrieving dataset files
        /// </summary>
        /// <param name="pathToCheck">Path to examine</param>
        /// <param name="fileNameToFind">Optional: Name of a file that must exist in the dataset folder; can contain a wildcard, e.g. *.zip</param>
        /// <param name="folderNameToFind">Optional: Name of a subfolder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
        /// <param name="maxAttempts">Maximum number of attempts</param>
        /// <param name="logFolderNotFound">If true, log a warning if the folder is not found</param>
        /// <returns>Path to the most appropriate dataset folder</returns>
        /// <remarks>FileNameToFind is a file in the dataset folder; it is NOT a file in FolderNameToFind</remarks>
        private bool FindValidFolderUNC(string pathToCheck, string fileNameToFind, string folderNameToFind, int maxAttempts, bool logFolderNotFound)
        {

            // First check whether this folder exists
            // Using a 1 second holdoff between retries
            if (!FolderExistsWithRetry(pathToCheck, 1, maxAttempts, logFolderNotFound))
            {
                return false;
            }

            // Folder was found
            var validFolder = true;

            if (m_DebugLevel > 3)
            {
                OnDebugEvent("FindValidFolderUNC, Folder found " + pathToCheck);
            }

            // Optionally look for fileNameToFind

            if (!string.IsNullOrEmpty(fileNameToFind))
            {
                if (fileNameToFind.Contains("*"))
                {
                    if (m_DebugLevel > 3)
                    {
                        OnDebugEvent("FindValidFolderUNC, Looking for files matching " + fileNameToFind);
                    }

                    // Wildcard in the name
                    // Look for any files matching fileNameToFind
                    var folderInfo = new DirectoryInfo(pathToCheck);

                    // Do not recurse here
                    // If the dataset folder does not contain a target file, and if folderNameToFind is defined,
                    // FindValidFolder will append folderNameToFind to the dataset folder path and call this method again
                    if (folderInfo.GetFiles(fileNameToFind, SearchOption.TopDirectoryOnly).Length == 0)
                    {
                        validFolder = false;
                    }
                }
                else
                {
                    if (m_DebugLevel > 3)
                    {
                        OnDebugEvent("FindValidFolderUNC, Looking for file named " + fileNameToFind);
                    }

                    // Look for file fileNameToFind in this folder
                    // Note: Using a 1 second holdoff between retries
                    var fileFound = m_FileCopyUtilities.FileExistsWithRetry(
                        Path.Combine(pathToCheck, fileNameToFind), retryHoldoffSeconds: 1,
                        logMsgTypeIfNotFound: BaseLogger.LogLevels.WARN, maxAttempts: maxAttempts);

                    if (!fileFound)
                    {
                        validFolder = false;
                    }
                }
            }

            // Optionally look for folderNameToFind
            if (validFolder && !string.IsNullOrEmpty(folderNameToFind))
            {
                if (folderNameToFind.Contains("*"))
                {
                    if (m_DebugLevel > 3)
                    {
                        OnDebugEvent("FindValidFolderUNC, Looking for folders matching " + folderNameToFind);
                    }

                    // Wildcard in the name
                    // Look for any folders matching folderNameToFind
                    var folderInfo = new DirectoryInfo(pathToCheck);

                    if (folderInfo.GetDirectories(folderNameToFind).Length == 0)
                    {
                        validFolder = false;
                    }
                }
                else
                {
                    if (m_DebugLevel > 3)
                    {
                        OnDebugEvent("FindValidFolderUNC, Looking for folder named " + folderNameToFind);
                    }

                    // Look for folder folderNameToFind in this folder
                    // Note: Using a 1 second holdoff between retries
                    if (!FolderExistsWithRetry(Path.Combine(pathToCheck, folderNameToFind), 1, maxAttempts, logFolderNotFound))
                    {
                        validFolder = false;
                    }
                }
            }

            return validFolder;

        }


        /// <summary>
        /// Test for folder existence with a retry loop in case of temporary glitch
        /// </summary>
        /// <param name="folderName">Folder name to look for</param>
        /// <param name="retryHoldoffSeconds">Time, in seconds, to wait between retrying; if 0, will default to 5 seconds; maximum value is 600 seconds</param>
        /// <param name="maxAttempts">Maximum number of attempts</param>
        /// <param name="logFolderNotFound">If true, log a warning if the folder is not found</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool FolderExistsWithRetry(string folderName, int retryHoldoffSeconds, int maxAttempts, bool logFolderNotFound)
        {

            if (maxAttempts < 1)
                maxAttempts = 1;

            if (maxAttempts > 10)
                maxAttempts = 10;

            var retryCount = maxAttempts;

            if (retryHoldoffSeconds <= 0)
                retryHoldoffSeconds = DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS;

            if (retryHoldoffSeconds > 600)
                retryHoldoffSeconds = 600;

            while (retryCount > 0)
            {
                if (Directory.Exists(folderName))
                {
                    return true;
                }

                if (logFolderNotFound)
                {
                    if (m_DebugLevel >= 2 || m_DebugLevel >= 1 && retryCount == 1)
                    {
                        var errMsg = "Folder " + folderName + " not found. Retry count = " + retryCount;
                        OnWarningEvent(errMsg);
                    }
                }

                retryCount -= 1;
                if (retryCount <= 0)
                {
                    return false;
                }

                // Wait retryHoldoffSeconds seconds before retrying
                clsGlobal.IdleLoop(retryHoldoffSeconds);
            }

            return false;

        }

        #endregion
    }
}
