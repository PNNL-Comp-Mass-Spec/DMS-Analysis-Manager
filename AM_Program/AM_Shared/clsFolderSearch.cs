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
    /// Methods to look for directories related to datasets
    /// </summary>
    public class clsFolderSearch : EventNotifier
    {
        #region "Constants"

        /// <summary>
        /// Maximum number of attempts to find a directory or file
        /// </summary>
        /// <remarks></remarks>
        public const int DEFAULT_MAX_RETRY_COUNT = 3;

        /// <summary>
        /// Default number of seconds to wait between trying to find a directory
        /// </summary>
        protected const int DEFAULT_FOLDER_EXISTS_RETRY_HOLDOFF_SECONDS = 5;

        private const string MYEMSL_PATH_FLAG = clsMyEMSLUtilities.MYEMSL_PATH_FLAG;

        #endregion

        #region "Module variables"

        private readonly bool mAuroraAvailable;

        private readonly int mDebugLevel;

        private readonly IJobParams mJobParams;

        private readonly clsMyEMSLUtilities mMyEMSLUtilities;

        private readonly clsFileCopyUtilities mFileCopyUtilities;

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
            mFileCopyUtilities = fileCopyUtilities;
            mJobParams = jobParams;
            mMyEMSLUtilities = myEmslUtilities;
            DatasetName = datasetName;
            mDebugLevel = debugLevel;
            mAuroraAvailable = auroraAvailable;
        }

        /// <summary>
        /// Add a directory or file path to a list of paths to examine
        /// </summary>
        /// <param name="pathsToCheck">List of Tuples where the string is a directory or file path, and the boolean is logIfMissing</param>
        /// <param name="directoryOrFilePath">Path to add</param>
        /// <param name="logIfMissing">True to log a message if the path is not found</param>
        /// <remarks></remarks>
        private void AddPathToCheck(ICollection<Tuple<string, bool>> pathsToCheck, string directoryOrFilePath, bool logIfMissing)
        {
            pathsToCheck.Add(new Tuple<string, bool>(directoryOrFilePath, logIfMissing));
        }

        /// <summary>
        /// Determines the full path to the dataset file
        /// Returns a directory path for data that is stored in directories (e.g. .D directories)
        /// For instruments with multiple data directories, returns the path to the first directory
        /// For instrument with multiple zipped data files, returns the dataset directory path
        /// </summary>
        /// <param name="isDirectory">Output variable: true if the path returned is a directory path; false if a file</param>
        /// <param name="assumeUnpurged">
        /// When true, assume that the instrument data exists on the storage server
        /// (and thus do not search MyEMSL or the archive for the file)
        /// </param>
        /// <returns>The full path to the dataset file or directory</returns>
        /// <remarks>When assumeUnpurged is true, this function returns the expected path
        /// to the instrument data file (or directory) on the storage server, even if the file/directory wasn't actually found</remarks>
        public string FindDatasetFileOrFolder(out bool isDirectory, bool assumeUnpurged)
        {
            return FindDatasetFileOrFolder(DEFAULT_MAX_RETRY_COUNT, out isDirectory, assumeUnpurged: assumeUnpurged);
        }

        /// <summary>
        /// Determines the full path to the dataset file
        /// Returns a directory path for data that is stored in directories (e.g. .D directories)
        /// For instruments with multiple data directories, returns the path to the first directory
        /// For instrument with multiple zipped data files, returns the dataset directory path
        /// </summary>
        /// <param name="maxAttempts">Maximum number of attempts to look for the directory</param>
        /// <param name="isDirectory">Output variable: true if the path returned is a directory path; false if a file</param>
        /// <param name="assumeUnpurged">
        /// When true, assume that the instrument data exists on the storage server
        /// (and thus do not search MyEMSL or the archive for the file)
        /// </param>
        /// <returns>The full path to the dataset file or directory</returns>
        /// <remarks>When assumeUnpurged is true, this function returns the expected path
        /// to the instrument data file (or directory) on the storage server, even if the file/directory wasn't actually found</remarks>
        public string FindDatasetFileOrFolder(int maxAttempts, out bool isDirectory, bool assumeUnpurged = false)
        {
            var RawDataType = mJobParams.GetParam("RawDataType");
            var StoragePath = mJobParams.GetParam("DatasetStoragePath");
            var fileOrDirectoryPath = string.Empty;

            isDirectory = false;

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
                        fileOrDirectoryPath = FindMGFFile(maxAttempts, assumeUnpurged);
                    }
                    else
                    {
                        // DeconTools_V2 now supports reading the .D files directly
                        // Call RetrieveDotDFolder() to copy the directory and all subdirectories
                        fileOrDirectoryPath = FindDotDFolder(assumeUnpurged);
                        isDirectory = true;
                    }

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile:
                    // Agilent/QSTAR TOF data
                    fileOrDirectoryPath = FindDatasetFile(maxAttempts, clsAnalysisResources.DOT_WIFF_EXTENSION, assumeUnpurged);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders:
                    // FTICR data
                    fileOrDirectoryPath = FindSFolders(assumeUnpurged);
                    isDirectory = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                    // Finnigan ion trap/LTQ-FT data
                    fileOrDirectoryPath = FindDatasetFile(maxAttempts, clsAnalysisResources.DOT_RAW_EXTENSION, assumeUnpurged);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder:
                    // Micromass QTOF data
                    fileOrDirectoryPath = FindDotRawFolder(assumeUnpurged);
                    isDirectory = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.UIMF:
                    // IMS UIMF data
                    fileOrDirectoryPath = FindDatasetFile(maxAttempts, clsAnalysisResources.DOT_UIMF_EXTENSION, assumeUnpurged);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzXML:
                    fileOrDirectoryPath = FindDatasetFile(maxAttempts, clsAnalysisResources.DOT_MZXML_EXTENSION, assumeUnpurged);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzML:
                    fileOrDirectoryPath = FindDatasetFile(maxAttempts, clsAnalysisResources.DOT_MZML_EXTENSION, assumeUnpurged);

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:
                    // Call RetrieveDotDFolder() to copy the directory and all subdirectories

                    // Both the MSXml step tool and DeconTools require the .Baf file
                    // We previously didn't need this file for DeconTools, but, now that DeconTools is using CompassXtract, so we need the file

                    fileOrDirectoryPath = FindDotDFolder(assumeUnpurged);
                    isDirectory = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging:
                    fileOrDirectoryPath = FindBrukerMALDIImagingFolders(assumeUnpurged);
                    isDirectory = true;

                    break;
            }

            return fileOrDirectoryPath;
        }


        /// <summary>
        /// Finds the dataset directory containing Bruker Maldi imaging .zip files
        /// </summary>
        /// <param name="assumeUnpurged"></param>
        /// <returns>The full path to the dataset directory</returns>
        public string FindBrukerMALDIImagingFolders(bool assumeUnpurged = false)
        {

            const string ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK = "*R*X*.zip";

            // Look for the dataset directory; it must contain .Zip files with names like 0_R00X442.zip
            // If a matching directory isn't found, ServerPath will contain the directory path defined by Job Param "DatasetStoragePath"

            var datasetDirPath = FindValidFolder(DatasetName, ZIPPED_BRUKER_IMAGING_SECTIONS_FILE_MASK,
                                                 retrievingInstrumentDataDir: true, assumeUnpurged: assumeUnpurged);

            if (string.IsNullOrEmpty(datasetDirPath))
                return string.Empty;

            return datasetDirPath;

        }

        /// <summary>
        /// Finds a file named DatasetName.FileExtension
        /// </summary>
        /// <param name="FileExtension"></param>
        /// <returns>The full path to the directory; an empty string if no match</returns>
        /// <remarks></remarks>
        public string FindDatasetFile(string FileExtension)
        {
            return FindDatasetFile(DEFAULT_MAX_RETRY_COUNT, FileExtension);
        }

        /// <summary>
        /// Finds a file named DatasetName.FileExtension
        /// </summary>
        /// <param name="maxAttempts">Maximum number of attempts to look for the directory</param>
        /// <param name="fileExtension"></param>
        /// <param name="assumeUnpurged"></param>
        /// <returns>The full path to the directory; an empty string if no match</returns>
        /// <remarks></remarks>
        public string FindDatasetFile(int maxAttempts, string fileExtension, bool assumeUnpurged = false)
        {

            if (!fileExtension.StartsWith("."))
            {
                fileExtension = "." + fileExtension;
            }

            var DataFileName = DatasetName + fileExtension;

            var datasetDirPath = FindValidFolder(DatasetName, DataFileName, directoryNameToFind: "", maxAttempts: maxAttempts,
                logDirectoryNotFound: true, retrievingInstrumentDataDir: false,
                assumeUnpurged: assumeUnpurged,
                validDirectoryFound: out var validDirectoryFound, directoryNotFoundMessage: out var directoryNotFoundMessage);

            if (!string.IsNullOrEmpty(datasetDirPath))
            {
                return Path.Combine(datasetDirPath, DataFileName);
            }

            return string.Empty;
        }

        /// <summary>
        /// Finds a .Raw directory below the dataset directory
        /// </summary>
        /// <param name="assumeUnpurged"></param>
        /// <returns>The full path to the directory; an empty string if no match</returns>
        private string FindDotDFolder(bool assumeUnpurged = false)
        {
            return FindDotXFolder(clsAnalysisResources.DOT_D_EXTENSION, assumeUnpurged);
        }

        /// <summary>
        /// Finds a .D directory below the dataset directory
        /// </summary>
        /// <param name="assumeUnpurged"></param>
        /// <returns></returns>
        private string FindDotRawFolder(bool assumeUnpurged = false)
        {
            return FindDotXFolder(clsAnalysisResources.DOT_RAW_EXTENSION, assumeUnpurged);
        }

        /// <summary>
        /// Finds a subdirectory (typically Dataset.D or Dataset.Raw) below the dataset directory
        /// </summary>
        /// <param name="directoryExtension"></param>
        /// <param name="assumeUnpurged"></param>
        /// <returns>The full path to the directory; an empty string if no match</returns>
        /// <remarks></remarks>
        public string FindDotXFolder(string directoryExtension, bool assumeUnpurged)
        {

            if (!directoryExtension.StartsWith("."))
            {
                directoryExtension = "." + directoryExtension;
            }

            var fileNameToFind = string.Empty;
            var directoryExtensionWildcard = "*" + directoryExtension;

            var serverPath = FindValidFolder(
                DatasetName,
                fileNameToFind,
                directoryExtensionWildcard,
                DEFAULT_MAX_RETRY_COUNT,
                logDirectoryNotFound: true,
                retrievingInstrumentDataDir: true,
                assumeUnpurged: assumeUnpurged,
                validDirectoryFound: out var validDirectoryFound,
                directoryNotFoundMessage: out var directoryNotFoundMessage);

            if (serverPath.StartsWith(MYEMSL_PATH_FLAG))
            {
                return serverPath;
            }

            var datasetDirectory = new DirectoryInfo(serverPath);

            // Find the instrument data directory (e.g. Dataset.D or Dataset.Raw) in the dataset directory
            foreach (var subDirectory in datasetDirectory.GetDirectories(directoryExtensionWildcard))
            {
                return subDirectory.FullName;
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

            // Data files are in a subdirectory off of the main dataset directory
            // Files are renamed with dataset name because MASIC requires this. Other analysis types don't care

            var serverPath = FindValidFolder(DatasetName, "", "*" + clsAnalysisResources.DOT_D_EXTENSION, maxAttempts,
                logDirectoryNotFound: true, retrievingInstrumentDataDir: false,
                assumeUnpurged: assumeUnpurged,
                validDirectoryFound: out var validDirectoryFound, directoryNotFoundMessage: out var directoryNotFoundMessage);

            var datasetDirectory = new DirectoryInfo(serverPath);

            // Get a list of the subdirectories in the dataset directory
            // Go through the directories looking for a file with a ".mgf" extension

            foreach (var subDirectory in datasetDirectory.GetDirectories())
            {
                foreach (var mgfFile in subDirectory.GetFiles("*" + clsAnalysisResources.DOT_MGF_EXTENSION))
                {
                    // Return the first .mgf file that was found
                    return mgfFile.FullName;
                }
            }

            // No match was found
            return string.Empty;

        }

        /// <summary>
        /// Finds the dataset directory containing either a 0.ser subdirectory or containing zipped S-folders
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        private string FindSFolders(bool assumeUnpurged = false)
        {

            // First Check for the existence of a 0.ser directory
            var fileNameToFind = string.Empty;

            var datasetDirectoryPath = FindValidFolder(DatasetName, fileNameToFind, clsAnalysisResources.BRUKER_ZERO_SER_FOLDER, DEFAULT_MAX_RETRY_COUNT,
                logDirectoryNotFound: true, retrievingInstrumentDataDir: true,
                assumeUnpurged: assumeUnpurged,
                validDirectoryFound: out var validDirectoryFound, directoryNotFoundMessage: out var directoryNotFoundMessage);

            if (!string.IsNullOrEmpty(datasetDirectoryPath))
            {
                return Path.Combine(datasetDirectoryPath, clsAnalysisResources.BRUKER_ZERO_SER_FOLDER);
            }

            // The 0.ser directory does not exist; look for zipped s-folders
            datasetDirectoryPath = FindValidFolder(DatasetName, "s*.zip", retrievingInstrumentDataDir: true, assumeUnpurged: assumeUnpurged);

            return datasetDirectoryPath;

        }

        /// <summary>
        /// Determines the most appropriate directory to use to obtain dataset files from
        /// Optionally, can require that a certain file also be present in the directory for it to be deemed valid
        /// If no directory is deemed valid, returns the path defined by "DatasetStoragePath"
        /// </summary>
        /// <param name="dsName">Name of the dataset</param>
        /// <param name="fileNameToFind">Name of a file that must exist in the directory; can contain a wildcard, e.g. *.zip</param>
        /// <param name="retrievingInstrumentDataDir">Set to True when retrieving an instrument data directory</param>
        /// <returns>Path to the most appropriate dataset directory</returns>
        /// <remarks>Although fileNameToFind could be empty, you are highly encouraged to filter by either fileNameToFind or by directoryNameToFind when using FindValidFolder</remarks>
        public string FindValidFolder(string dsName, string fileNameToFind, bool retrievingInstrumentDataDir)
        {

            const string directoryNameToFind = "";
            return FindValidFolder(dsName, fileNameToFind, directoryNameToFind, DEFAULT_MAX_RETRY_COUNT,
                logDirectoryNotFound: true, retrievingInstrumentDataDir: retrievingInstrumentDataDir);

        }

        /// <summary>
        /// Determines the most appropriate directory to use to obtain dataset files from
        /// Optionally, can require that a certain file also be present in the directory for it to be deemed valid
        /// If no directory is deemed valid, returns the path defined by "DatasetStoragePath"
        /// </summary>
        /// <param name="dsName">Name of the dataset</param>
        /// <param name="fileNameToFind">Name of a file that must exist in the directory; can contain a wildcard, e.g. *.zip</param>
        /// <param name="retrievingInstrumentDataDir">Set to True when retrieving an instrument data directory</param>
        /// <param name="assumeUnpurged"></param>
        /// <returns>Path to the most appropriate dataset directory</returns>
        /// <remarks>Although fileNameToFind could be empty, you are highly encouraged to filter by either fileNameToFind or by directoryNameToFind when using FindValidFolder</remarks>
        private string FindValidFolder(string dsName, string fileNameToFind, bool retrievingInstrumentDataDir, bool assumeUnpurged)
        {

            const string directoryNameToFind = "";

            return FindValidFolder(dsName, fileNameToFind, directoryNameToFind, DEFAULT_MAX_RETRY_COUNT,
                logDirectoryNotFound: true, retrievingInstrumentDataDir: retrievingInstrumentDataDir,
                assumeUnpurged: assumeUnpurged,
                validDirectoryFound: out var validDirectoryFound, directoryNotFoundMessage: out var directoryNotFoundMessage);

        }

        /// <summary>
        /// Determines the most appropriate directory to use to obtain dataset files from
        /// Optionally, can require that a certain file also be present in the directory for it to be deemed valid
        /// If no directory is deemed valid, returns the path defined by "DatasetStoragePath"
        /// </summary>
        /// <param name="dsName">Name of the dataset</param>
        /// <param name="fileNameToFind">Name of a file that must exist in the directory; can contain a wildcard, e.g. *.zip</param>
        /// <param name="directoryNameToFind">Optional: Name of a directory that must exist in the dataset directory; can contain a wildcard, e.g. SEQ*</param>
        /// <param name="maxRetryCount">Maximum number of attempts</param>
        /// <returns>Path to the maxRetryCount appropriate dataset directory</returns>
        /// <remarks>Although fileNameToFind and directoryNameToFind could both be empty, you are highly encouraged to filter by either fileNameToFind or by directoryNameToFind when using FindValidFolder</remarks>
        public string FindValidFolder(string dsName, string fileNameToFind, string directoryNameToFind, int maxRetryCount)
        {

            return FindValidFolder(dsName, fileNameToFind, directoryNameToFind, maxRetryCount,
                logDirectoryNotFound: true, retrievingInstrumentDataDir: false);

        }

        /// <summary>
        /// Determines the most appropriate directory to use to obtain dataset files from
        /// Optionally, can require that a certain file also be present in the directory for it to be deemed valid
        /// If no directory is deemed valid, returns the path defined by Job Param "DatasetStoragePath"
        /// </summary>
        /// <param name="dsName">Name of the dataset</param>
        /// <param name="fileNameToFind">Optional: Name of a file that must exist in the dataset directory; can contain a wildcard, e.g. *.zip</param>
        /// <param name="directoryNameToFind">Optional: Name of a subdirectory that must exist in the dataset directory; can contain a wildcard, e.g. SEQ*</param>
        /// <param name="maxRetryCount">Maximum number of attempts</param>
        /// <param name="logDirectoryNotFound">If true, log a warning if the directory is not found</param>
        /// <param name="retrievingInstrumentDataDir">Set to True when retrieving an instrument data directory</param>
        /// <returns>Path to the most appropriate dataset directory</returns>
        /// <remarks>The path returned will be "\\MyEMSL" if the best directory is in MyEMSL</remarks>
        public string FindValidFolder(
            string dsName, string fileNameToFind, string directoryNameToFind,
            int maxRetryCount, bool logDirectoryNotFound, bool retrievingInstrumentDataDir)
        {

            return FindValidFolder(dsName, fileNameToFind, directoryNameToFind, maxRetryCount, logDirectoryNotFound, retrievingInstrumentDataDir,
                assumeUnpurged: false,
                validDirectoryFound: out var validDirectoryFound, directoryNotFoundMessage: out var directoryNotFoundMessage);
        }

        /// <summary>
        /// Determines the most appropriate directory to use to obtain dataset files from
        /// Optionally, can require that a certain file also be present in the directory for it to be deemed valid
        /// If no directory is deemed valid, returns the path defined by Job Param "DatasetStoragePath"
        /// </summary>
        /// <param name="datasetName">Name of the dataset</param>
        /// <param name="fileNameToFind">Optional: Name of a file that must exist in the dataset directory; can contain a wildcard, e.g. *.zip</param>
        /// <param name="directoryNameToFind">Optional: Name of a subdirectory that must exist in the dataset directory; can contain a wildcard, e.g. SEQ*</param>
        /// <param name="maxAttempts">Maximum number of attempts</param>
        /// <param name="logDirectoryNotFound">If true, log a warning if the directory is not found</param>
        /// <param name="retrievingInstrumentDataDir">Set to True when retrieving an instrument data directory</param>
        /// <param name="assumeUnpurged">When true, this function returns the path to the dataset directory on the storage server</param>
        /// <param name="validDirectoryFound">Output parameter: True if a valid directory is ultimately found, otherwise false</param>
        /// <param name="directoryNotFoundMessage">Output parameter: description to be used when validDirectoryFound is false</param>
        /// <returns>Path to the most appropriate dataset directory</returns>
        /// <remarks>The path returned will be "\\MyEMSL" if the best directory is in MyEMSL</remarks>
        public string FindValidFolder(
            string datasetName,
            string fileNameToFind,
            string directoryNameToFind,
            int maxAttempts,
            bool logDirectoryNotFound,
            bool retrievingInstrumentDataDir,
            bool assumeUnpurged,
            out bool validDirectoryFound,
            out string directoryNotFoundMessage)
        {
            var bestPath = string.Empty;

            // The tuples in this list are the path to check, and True if we should warn that the directory was not found
            var pathsToCheck = new List<Tuple<string, bool>>();

            var validDirectory = false;

            validDirectoryFound = false;
            directoryNotFoundMessage = string.Empty;

            try
            {
                if (fileNameToFind == null)
                    fileNameToFind = string.Empty;
                if (directoryNameToFind == null)
                    directoryNameToFind = string.Empty;

                if (assumeUnpurged)
                {
                    maxAttempts = 1;
                    logDirectoryNotFound = false;
                }

                var instrumentDataPurged = mJobParams.GetJobParameter("InstrumentDataPurged", 0);

                var datasetDirectoryName = mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME);

                if (retrievingInstrumentDataDir && instrumentDataPurged != 0 && !assumeUnpurged)
                {
                    // The instrument data is purged and we're retrieving instrument data
                    // Skip the primary dataset directory since the primary data files were most likely purged
                }
                else
                {
                    AddPathToCheck(pathsToCheck, Path.Combine(mJobParams.GetParam("DatasetStoragePath"), datasetDirectoryName), true);
                    if (datasetDirectoryName != datasetName)
                        AddPathToCheck(pathsToCheck, Path.Combine(mJobParams.GetParam("DatasetStoragePath"), datasetName), false);
                }

                if (!MyEMSLSearchDisabled && !assumeUnpurged)
                {
                    // \\MyEMSL
                    AddPathToCheck(pathsToCheck, MYEMSL_PATH_FLAG, false);
                }

                // Optional Temp Debug: Enable compilation constant DISABLE_MYEMSL_SEARCH to disable checking MyEMSL (and thus speed things up)
#if DISABLE_MYEMSL_SEARCH
        if (mMgrParams.GetParam("MgrName").ToLower().Contains("monroe")) {
            pathsToCheck.Remove(MYEMSL_PATH_FLAG);
        }
#endif
                if ((mAuroraAvailable || MyEMSLSearchDisabled) && !assumeUnpurged)
                {
                    AddPathToCheck(pathsToCheck, Path.Combine(mJobParams.GetParam("DatasetArchivePath"), datasetDirectoryName), true);
                    if (datasetDirectoryName != datasetName)
                        AddPathToCheck(pathsToCheck, Path.Combine(mJobParams.GetParam("DatasetArchivePath"), datasetName), false);
                }

                AddPathToCheck(pathsToCheck, Path.Combine(mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH), datasetDirectoryName), false);
                if (datasetDirectoryName != datasetName)
                    AddPathToCheck(pathsToCheck, Path.Combine(mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH), datasetName), false);

                var fileNotFoundEncountered = false;

                bestPath = pathsToCheck.First().Item1;
                foreach (var pathToCheck in pathsToCheck)
                {
                    try
                    {
                        if (mDebugLevel > 3)
                        {
                            var msg = "FindValidDatasetFolder, Looking for directory " + pathToCheck.Item1;
                            OnDebugEvent(msg);
                        }

                        if (pathToCheck.Item1 == MYEMSL_PATH_FLAG)
                        {
                            const bool recurseMyEMSL = false;
                            validDirectory = FindValidFolderMyEMSL(datasetName, fileNameToFind, directoryNameToFind, false, recurseMyEMSL);
                        }
                        else
                        {
                            validDirectory = FindValidFolderUNC(pathToCheck.Item1, fileNameToFind, directoryNameToFind, maxAttempts, logDirectoryNotFound && pathToCheck.Item2);

                            if (!validDirectory && !string.IsNullOrEmpty(fileNameToFind) && !string.IsNullOrEmpty(directoryNameToFind))
                            {
                                // Look for a subdirectory named directoryNameToFind that contains file fileNameToFind
                                var pathToCheckAlt = Path.Combine(pathToCheck.Item1, directoryNameToFind);
                                validDirectory = FindValidFolderUNC(pathToCheckAlt, fileNameToFind, string.Empty, maxAttempts, logDirectoryNotFound && pathToCheck.Item2);

                                if (validDirectory)
                                {
                                    // Jump out of the foreach
                                    bestPath = pathToCheckAlt;
                                    break;
                                }
                            }

                        }

                        if (validDirectory)
                        {
                            bestPath = string.Copy(pathToCheck.Item1);
                            break;
                        }

                        fileNotFoundEncountered = true;

                    }
                    catch (Exception ex)
                    {
                        OnErrorEvent("Exception looking for directory: " + pathToCheck.Item1, ex);
                    }
                } // for each item in pathsToCheck

                if (validDirectory)
                {
                    validDirectoryFound = true;

                    if (mDebugLevel >= 4 || mDebugLevel >= 1 && fileNotFoundEncountered)
                    {
                        var msg = "FindValidFolder, Valid dataset directory has been found:  " + bestPath;
                        if (fileNameToFind.Length > 0)
                        {
                            msg += " (matched file " + fileNameToFind + ")";
                        }
                        if (directoryNameToFind.Length > 0)
                        {
                            msg += " (matched directory " + directoryNameToFind + ")";
                        }
                        OnDebugEvent(msg);
                    }

                }
                else
                {
                    directoryNotFoundMessage = "Could not find a valid dataset directory";
                    if (fileNameToFind.Length > 0)
                    {
                        // Could not find a valid dataset directory containing file
                        directoryNotFoundMessage += " containing file " + fileNameToFind;
                    }

                    if (logDirectoryNotFound && mDebugLevel >= 1)
                    {
                        if (assumeUnpurged)
                        {
                            OnStatusEvent(directoryNotFoundMessage);
                        }
                        else
                        {
                            var msg = string.Format("{0}, Job {1}, Dataset {2}",
                                                    directoryNotFoundMessage,
                                                    mJobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job"),
                                                    datasetName);
                            OnWarningEvent(msg);
                        }
                    }

                }

            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception looking for a valid dataset directory for dataset " + datasetName, ex);
                directoryNotFoundMessage = "Exception looking for a valid dataset directory";
            }

            return bestPath;

        }

        /// <summary>
        /// Determines whether the directory specified by pathToCheck is appropriate for retrieving dataset files
        /// </summary>
        /// <param name="dataset">Dataset name</param>
        /// <param name="fileNameToFind">Optional: Name of a file that must exist in the dataset directory; can contain a wildcard, e.g. *.zip</param>
        /// <param name="subDirectoryName">Optional: Name of a subdirectory that must exist in the dataset directory; can contain a wildcard, e.g. SEQ*</param>
        /// <param name="logDirectoryNotFound">If true, log a warning if the directory is not found</param>
        /// <param name="recurse">True to look for fileNameToFind in all subdirectories of a dataset; false to only look in the primary dataset directory</param>
        /// <returns>Path to the most appropriate dataset directory</returns>
        /// <remarks>FileNameToFind is a file in the dataset directory; it is NOT a file in directoryNameToFind</remarks>
        private bool FindValidFolderMyEMSL(string dataset, string fileNameToFind, string subDirectoryName, bool logDirectoryNotFound, bool recurse)
        {

            if (string.IsNullOrEmpty(fileNameToFind))
                fileNameToFind = "*";

            if (mDebugLevel > 3)
            {
                OnDebugEvent("FindValidFolderMyEMSL, querying MyEMSL for this dataset's files");
            }

            List<DatasetFolderOrFileInfo> matchingMyEMSLFiles;

            if (string.IsNullOrEmpty(subDirectoryName))
            {
                // Simply look for the file
                matchingMyEMSLFiles = mMyEMSLUtilities.FindFiles(fileNameToFind, string.Empty, dataset, recurse);
            }
            else
            {
                // First look for the subdirectory
                // If there are multiple matching subdirectories, choose the newest one
                // The entries in matchingMyEMSLFiles will be directory entries where the "Filename" field is the directory name while the "SubDirPath" field is any parent directories above the found directory
                matchingMyEMSLFiles = mMyEMSLUtilities.FindFiles(fileNameToFind, subDirectoryName, dataset, recurse);
            }

            if (matchingMyEMSLFiles.Count > 0)
            {
                return true;
            }

            if (logDirectoryNotFound)
            {
                var msg = "MyEMSL does not have any files for dataset " + dataset;
                if (!string.IsNullOrEmpty(fileNameToFind))
                {
                    msg += " and file " + fileNameToFind;
                }

                if (!string.IsNullOrEmpty(subDirectoryName))
                {
                    msg += " and subdirectory " + subDirectoryName;
                }

                OnWarningEvent(msg);
            }
            return false;
        }

        /// <summary>
        /// Determines whether the directory specified by pathToCheck is appropriate for retrieving dataset files
        /// </summary>
        /// <param name="pathToCheck">Path to examine</param>
        /// <param name="fileNameToFind">Optional: Name of a file that must exist in the dataset directory; can contain a wildcard, e.g. *.zip</param>
        /// <param name="directoryNameToFind">Optional: Name of a subdirectory that must exist in the dataset directory; can contain a wildcard, e.g. SEQ*</param>
        /// <param name="maxAttempts">Maximum number of attempts</param>
        /// <param name="logDirectoryNotFound">If true, log a warning if the directory is not found</param>
        /// <returns>Path to the most appropriate dataset directory</returns>
        /// <remarks>FileNameToFind is a file in the dataset directory; it is NOT a file in directoryNameToFind</remarks>
        private bool FindValidFolderUNC(string pathToCheck, string fileNameToFind, string directoryNameToFind, int maxAttempts, bool logDirectoryNotFound)
        {

            // First check whether this directory exists
            // Using a 1 second holdoff between retries
            if (!FolderExistsWithRetry(pathToCheck, 1, maxAttempts, logDirectoryNotFound))
            {
                return false;
            }

            // Directory was found
            var validDirectory = true;

            if (mDebugLevel > 3)
            {
                OnDebugEvent("FindValidFolderUNC, Directory found " + pathToCheck);
            }

            // Optionally look for fileNameToFind

            if (!string.IsNullOrEmpty(fileNameToFind))
            {
                if (fileNameToFind.Contains("*"))
                {
                    if (mDebugLevel > 3)
                    {
                        OnDebugEvent("FindValidFolderUNC, Looking for files matching " + fileNameToFind);
                    }

                    // Wildcard in the name
                    // Look for any files matching fileNameToFind
                    var targetDirectory = new DirectoryInfo(pathToCheck);

                    // Do not recurse here
                    // If the dataset directory does not contain a target file, and if directoryNameToFind is defined,
                    // FindValidFolder will append directoryNameToFind to the dataset directory path and call this method again
                    if (targetDirectory.GetFiles(fileNameToFind, SearchOption.TopDirectoryOnly).Length == 0)
                    {
                        validDirectory = false;
                    }
                }
                else
                {
                    if (mDebugLevel > 3)
                    {
                        OnDebugEvent("FindValidFolderUNC, Looking for file named " + fileNameToFind);
                    }

                    // Look for file fileNameToFind in this directory
                    // Note: Using a 1 second holdoff between retries
                    var fileFound = mFileCopyUtilities.FileExistsWithRetry(
                        Path.Combine(pathToCheck, fileNameToFind), retryHoldoffSeconds: 1,
                        logMsgTypeIfNotFound: BaseLogger.LogLevels.WARN, maxAttempts: maxAttempts);

                    if (!fileFound)
                    {
                        validDirectory = false;
                    }
                }
            }

            // Optionally look for directoryNameToFind
            if (validDirectory && !string.IsNullOrEmpty(directoryNameToFind))
            {
                if (directoryNameToFind.Contains("*"))
                {
                    if (mDebugLevel > 3)
                    {
                        OnDebugEvent("FindValidFolderUNC, Looking for directories matching " + directoryNameToFind);
                    }

                    // Wildcard in the name
                    // Look for any directories matching directoryNameToFind
                    var targetDirectory = new DirectoryInfo(pathToCheck);

                    if (targetDirectory.GetDirectories(directoryNameToFind).Length == 0)
                    {
                        validDirectory = false;
                    }
                }
                else
                {
                    if (mDebugLevel > 3)
                    {
                        OnDebugEvent("FindValidFolderUNC, Looking for directory named " + directoryNameToFind);
                    }

                    // Look for directory directoryNameToFind in this directory
                    // Note: Using a 1 second holdoff between retries
                    if (!FolderExistsWithRetry(Path.Combine(pathToCheck, directoryNameToFind), 1, maxAttempts, logDirectoryNotFound))
                    {
                        validDirectory = false;
                    }
                }
            }

            return validDirectory;

        }


        /// <summary>
        /// Test for validDirectory existence with a retry loop in case of temporary glitch
        /// </summary>
        /// <param name="directoryName">Directory name to look for</param>
        /// <param name="retryHoldoffSeconds">Time, in seconds, to wait between retrying; if 0, will default to 5 seconds; maximum value is 600 seconds</param>
        /// <param name="maxAttempts">Maximum number of attempts</param>
        /// <param name="logDirectoryNotFound">If true, log a warning if the directory is not found</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool FolderExistsWithRetry(string directoryName, int retryHoldoffSeconds, int maxAttempts, bool logDirectoryNotFound)
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
                if (Directory.Exists(directoryName))
                {
                    return true;
                }

                if (logDirectoryNotFound)
                {
                    if (mDebugLevel >= 2 || mDebugLevel >= 1 && retryCount == 1)
                    {
                        var errMsg = "Directory " + directoryName + " not found. Retry count = " + retryCount;
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
