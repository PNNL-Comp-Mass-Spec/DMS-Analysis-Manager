
using System;
using System.Collections.Generic;
using System.Data;
using PHRPReader;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml;
using System.Xml.Linq;
using MyEMSLReader;
using ParamFileGenerator.MakeParams;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/19/2007
//*********************************************************************************************************

namespace AnalysisManagerBase
{
    /// <summary>
    /// Base class for job resource class
    /// </summary>
    public abstract class clsAnalysisResources : clsAnalysisMgrBase, IAnalysisResources
    {

        #region "Constants"

        public const string MYEMSL_PATH_FLAG = clsMyEMSLUtilities.MYEMSL_PATH_FLAG;

        // Note: All of the RAW_DATA_TYPE constants need to be all lowercase

        /// <summary>
        /// Agilent ion trap data, Agilent TOF data
        /// </summary>
        public const string RAW_DATA_TYPE_DOT_D_FOLDERS = "dot_d_folders";

        /// <summary>
        /// FTICR data, including instrument 3T_FTICR, 7T_FTICR, 9T_FTICR, 11T_FTICR, 11T_FTICR_B, and 12T_FTICR
        /// </summary>
        public const string RAW_DATA_TYPE_ZIPPED_S_FOLDERS = "zipped_s_folders";

        /// <summary>
        /// Micromass QTOF data
        /// </summary>
        public const string RAW_DATA_TYPE_DOT_RAW_FOLDER = "dot_raw_folder";

        /// <summary>
        /// Finnigan ion trap/LTQ-FT data
        /// </summary>
        public const string RAW_DATA_TYPE_DOT_RAW_FILES = "dot_raw_files";

        /// <summary>
        /// Agilent/QSTAR TOF data
        /// </summary>
        public const string RAW_DATA_TYPE_DOT_WIFF_FILES = "dot_wiff_files";

        /// <summary>
        /// IMS_UIMF (IMS_Agilent_TOF in DMS)
        /// </summary>
        public const string RAW_DATA_TYPE_DOT_UIMF_FILES = "dot_uimf_files";

        /// <summary>
        /// mzXML
        /// </summary>
        public const string RAW_DATA_TYPE_DOT_MZXML_FILES = "dot_mzxml_files";

        /// <summary>
        /// mzML
        /// </summary>
        public const string RAW_DATA_TYPE_DOT_MZML_FILES = "dot_mzml_files";

        // 12T datasets acquired prior to 7/16/2010 use a Bruker data station and have an analysis.baf file, 0.ser folder, and a XMASS_Method.m subfolder with file apexAcquisition.method
        // Datasets will have an instrument name of 12T_FTICR and raw_data_type of "zipped_s_folders"

        // 12T datasets acquired after 9/1/2010 use the Agilent data station, and thus have a .D folder
        // Datasets will have an instrument name of 12T_FTICR_B and raw_data_type of "bruker_ft"
        // 15T datasets also have raw_data_type "bruker_ft"
        // Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a apexAcquisition.method file
        public const string RAW_DATA_TYPE_BRUKER_FT_FOLDER = "bruker_ft";

        // The following is used by BrukerTOF_01 (e.g. Bruker TOF_TOF)
        // Folder has a .EMF file and a single sub-folder that has an acqu file and fid file
        public const string RAW_DATA_TYPE_BRUKER_MALDI_SPOT = "bruker_maldi_spot";

        // The following is used by instruments 9T_FTICR_Imaging and BrukerTOF_Imaging_01
        // Series of zipped subfolders, with names like 0_R00X329.zip; subfolders inside the .Zip files have fid files
        public const string RAW_DATA_TYPE_BRUKER_MALDI_IMAGING = "bruker_maldi_imaging";

        // The following is used by instrument Maxis_01
        // Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a microTOFQMaxAcquisition.method file; there is not a ser or fid file
        public const string RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER = "bruker_tof_baf";
        public const string RESULT_TYPE_SEQUEST = "Peptide_Hit";
        public const string RESULT_TYPE_XTANDEM = "XT_Peptide_Hit";
        public const string RESULT_TYPE_INSPECT = "IN_Peptide_Hit";

        // Used for MSGFDB and MSGF+
        public const string RESULT_TYPE_MSGFPLUS = "MSG_Peptide_Hit";
        public const string RESULT_TYPE_MSALIGN = "MSA_Peptide_Hit";
        public const string RESULT_TYPE_MODA = "MODa_Peptide_Hit";
        public const string RESULT_TYPE_MODPLUS = "MODPlus_Peptide_Hit";

        public const string RESULT_TYPE_MSPATHFINDER = "MSP_Peptide_Hit";
        public const string DOT_WIFF_EXTENSION = ".wiff";
        public const string DOT_D_EXTENSION = ".d";
        public const string DOT_RAW_EXTENSION = ".raw";

        public const string DOT_UIMF_EXTENSION = ".uimf";
        public const string DOT_GZ_EXTENSION = ".gz";
        public const string DOT_MZXML_EXTENSION = ".mzXML";

        public const string DOT_MZML_EXTENSION = ".mzML";
        public const string DOT_MGF_EXTENSION = ".mgf";

        public const string DOT_CDF_EXTENSION = ".cdf";

        public const string DOT_PBF_EXTENSION = ".pbf";

        public const string LOCK_FILE_EXTENSION = ".lock";

        /// <summary>
        /// Feature file, generated by the ProMex tool
        /// </summary>
        /// <remarks></remarks>
        public const string DOT_MS1FT_EXTENSION = ".ms1ft";

        public const string STORAGE_PATH_INFO_FILE_SUFFIX = clsFileCopyUtilities.STORAGE_PATH_INFO_FILE_SUFFIX;
        public const string SCAN_STATS_FILE_SUFFIX = "_ScanStats.txt";

        public const string SCAN_STATS_EX_FILE_SUFFIX = "_ScanStatsEx.txt";

        public const string REPORTERIONS_FILE_SUFFIX = "_ReporterIons.txt";

        public const string DATA_PACKAGE_SPECTRA_FILE_SUFFIX = "_SpectraFile";
        public const string BRUKER_ZERO_SER_FOLDER = "0.ser";
        public const string BRUKER_SER_FILE = "ser";

        public const string BRUKER_FID_FILE = "fid";

        public const string JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS = "PackedParam_DatasetFilePaths";

        // This is used by clsAnalysisResourcesRepoPkgr
        public const string JOB_PARAM_DICTIONARY_DATASET_RAW_DATA_TYPES = "PackedParam_DatasetRawDataTypes";

        // These are used by clsAnalysisResourcesPhosphoFdrAggregator
        public const string JOB_PARAM_DICTIONARY_JOB_DATASET_MAP = "PackedParam_JobDatasetMap";
        public const string JOB_PARAM_DICTIONARY_JOB_SETTINGS_FILE_MAP = "PackedParam_JobSettingsFileMap";

        public const string JOB_PARAM_DICTIONARY_JOB_TOOL_MAP = "PackedParam_JobToolNameMap";

        public const string JOB_PARAM_GENERATED_FASTA_NAME = "generatedFastaName";

        public const string JOB_PARAM_REMOTE_INFO = "RemoteInfo";
        public const string JOB_PARAM_REMOTE_TIMESTAMP = "RemoteTimestamp";

        // This constant is used by clsAnalysisToolRunnerMSGFDB, clsAnalysisResourcesMSGFDB, and clsAnalysisResourcesDtaRefinery
        public const string SPECTRA_ARE_NOT_CENTROIDED = "None of the spectra are centroided; unable to process";

        public enum eRawDataTypeConstants
        {
            Unknown = 0,
            ThermoRawFile = 1,
            UIMF = 2,
            mzXML = 3,
            mzML = 4,
            AgilentDFolder = 5,
            // Agilent ion trap data, Agilent TOF data
            AgilentQStarWiffFile = 6,
            MicromassRawFolder = 7,
            // Micromass QTOF data
            ZippedSFolders = 8,
            // FTICR data, including instrument 3T_FTICR, 7T_FTICR, 9T_FTICR, 11T_FTICR, 11T_FTICR_B, and 12T_FTICR
            BrukerFTFolder = 9,
            // .D folder is the analysis.baf file; there is also .m subfolder that has a apexAcquisition.method file
            BrukerMALDISpot = 10,
            // has a .EMF file and a single sub-folder that has an acqu file and fid file
            BrukerMALDIImaging = 11,
            // Series of zipped subfolders, with names like 0_R00X329.zip; subfolders inside the .Zip files have fid files
            BrukerTOFBaf = 12
            // Used by Maxis01; Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a microTOFQMaxAcquisition.method file; there is not a ser or fid file
        }

        public enum MSXMLOutputTypeConstants
        {
            mzXML = 0,
            mzML = 1
        }

        public enum DataPackageFileRetrievalModeConstants
        {
            Undefined = 0,
            Ascore = 1
        }

        #endregion

        #region "Module variables"
        protected IJobParams m_jobParams;
        protected IMgrParams m_mgrParams;
        protected string m_WorkingDir;
        protected int m_JobNum;

        /// <summary>
        /// Dataset name
        /// </summary>
        /// <remarks>
        /// Update the dataset name using property DatasetName
        /// because we also need to propogate that change
        /// into m_FolderSearch and m_FileSearch
        /// </remarks>
        private string m_DatasetName;

        protected string m_MgrName;

        protected string m_FastaToolsCnStr = "";

        protected string m_FastaFileName = "";
        protected Protein_Exporter.clsGetFASTAFromDMS m_FastaTools;

        protected readonly clsCDTAUtilities m_CDTAUtilities;

        protected clsSplitFastaFileUtilities m_SplitFastaFileUtility;
        protected DateTime m_SplitFastaLastUpdateTime;

        protected float m_SplitFastaLastPercentComplete;

        private DateTime m_LastCDTAUtilitiesUpdateTime;

        private DateTime m_FastaToolsLastLogTime;
        private double m_FastaToolFractionDoneSaved = -1;

        protected clsMyEMSLUtilities m_MyEMSLUtilities;

        private Dictionary<clsGlobal.eAnalysisResourceOptions, bool> m_ResourceOptions;
        private bool m_AuroraAvailable;

        private bool m_MyEMSLSearchDisabled;

        private clsDataPackageJobInfo mCachedDatasetAndJobInfo;

        private bool mCachedDatasetAndJobInfoIsDefined;

        public SpectraTypeClassifier.clsSpectrumTypeClassifier mSpectraTypeClassifier;

        private clsFileCopyUtilities m_FileCopyUtilities;

        private clsFolderSearch m_FolderSearch;

        private clsFileSearch m_FileSearch;

        #endregion

        #region "Properties"

        /// <summary>
        /// Dataset name
        /// </summary>
        /// <remarks>Also updates m_FolderSearch and m_FileSearch</remarks>
        public string DatasetName
        {
            get => m_DatasetName;
            set
            {
                m_DatasetName = value;

                if (m_FolderSearch != null)
                    m_FolderSearch.DatasetName = m_DatasetName;

                if (m_FileSearch != null)
                    m_FileSearch.DatasetName = m_DatasetName;
            }
        }
        public short DebugLevel => m_DebugLevel;

        public clsFolderSearch FolderSearch => m_FolderSearch;

        public clsFileSearch FileSearch => m_FileSearch;

        public bool MyEMSLSearchDisabled
        {
            get => m_MyEMSLSearchDisabled;
            set
            {
                m_MyEMSLSearchDisabled = value;
                if (m_FolderSearch != null)
                {
                    if (m_MyEMSLSearchDisabled && !m_FolderSearch.MyEMSLSearchDisabled)
                        m_FolderSearch.MyEMSLSearchDisabled = true;
                }

                if (m_FileSearch != null)
                {
                    if (m_MyEMSLSearchDisabled && !m_FileSearch.MyEMSLSearchDisabled)
                        m_FileSearch.MyEMSLSearchDisabled = true;
                }

            }
        }

        public clsMyEMSLUtilities MyEMSLUtilities => m_MyEMSLUtilities;

        /// <summary>
        ///  Explanation of what happened to last operation this class performed
        /// </summary>
        public string Message => m_message;

        public string WorkDir => m_WorkingDir;

        #endregion

        #region "Methods"
        /// <summary>

        /// Constructor
        /// </summary>
        /// <remarks></remarks>
        public clsAnalysisResources() : base("clsAnalysisResources")
        {
            m_CDTAUtilities = new clsCDTAUtilities();
            RegisterEvents(m_CDTAUtilities);
            m_CDTAUtilities.ProgressUpdate -= ProgressUpdateHandler;
            m_CDTAUtilities.ProgressUpdate += m_CDTAUtilities_ProgressEvent;

        }

        /// <summary>
        /// Initialize class
        /// </summary>
        /// <param name="mgrParams">Manager parameter object</param>
        /// <param name="jobParams">Job parameter object</param>
        /// <param name="statusTools">Object for status reporting</param>
        /// <param name="myEMSLUtilities">MyEMSL download Utilities (can be nothing)</param>
        /// <remarks></remarks>
        public virtual void Setup(IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            m_mgrParams = mgrParams;
            m_jobParams = jobParams;

            m_DebugLevel = (short)m_mgrParams.GetParam("debuglevel", 1);
            m_FastaToolsCnStr = m_mgrParams.GetParam("fastacnstring");
            m_MgrName = m_mgrParams.GetParam("MgrName", "Undefined-Manager");

            m_WorkingDir = m_mgrParams.GetParam("workdir");

            var jobNum = m_jobParams.GetParam("StepParameters", "Job");
            if (!string.IsNullOrEmpty(jobNum))
            {
                int.TryParse(jobNum, out m_JobNum);
            }

            DatasetName = m_jobParams.GetParam("JobParameters", "DatasetNum");

            InitFileTools(m_MgrName, m_DebugLevel);

            m_MyEMSLUtilities = myEMSLUtilities ?? new clsMyEMSLUtilities(m_DebugLevel, m_WorkingDir);

            RegisterEvents(m_MyEMSLUtilities);

            m_FileCopyUtilities = new clsFileCopyUtilities(m_FileTools, m_MyEMSLUtilities, m_MgrName, m_DebugLevel);
            RegisterEvents(m_FileCopyUtilities);

            m_FileCopyUtilities.ResetTimestampForQueueWaitTime += FileCopyUtilities_ResetTimestampForQueueWaitTime;

            m_ResourceOptions = new Dictionary<clsGlobal.eAnalysisResourceOptions, bool>();
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, false);
            SetOption(clsGlobal.eAnalysisResourceOptions.MyEMSLSearchDisabled, false);

            m_StatusTools = statusTools;

            m_AuroraAvailable = m_mgrParams.GetParam("AuroraAvailable", true);

            var myEmslAvailable = m_mgrParams.GetParam("MyEmslAvailable", true);

            m_FolderSearch = new clsFolderSearch(
                m_FileCopyUtilities, m_jobParams, m_MyEMSLUtilities,
                m_DatasetName, m_DebugLevel, m_AuroraAvailable);
            RegisterEvents(m_FolderSearch);

            m_FolderSearch.MyEMSLSearchDisabled = m_MyEMSLSearchDisabled || !myEmslAvailable;

            m_FileSearch = new clsFileSearch(
                m_FileCopyUtilities, m_FolderSearch, m_MyEMSLUtilities,
                m_mgrParams, m_jobParams, m_DatasetName, m_DebugLevel, m_WorkingDir, m_AuroraAvailable);

            RegisterEvents(m_FileSearch);

            m_FileSearch.MyEMSLSearchDisabled = m_MyEMSLSearchDisabled || !myEmslAvailable;
        }

        public abstract CloseOutType GetResources();

        public bool GetOption(clsGlobal.eAnalysisResourceOptions resourceOption)
        {
            if (m_ResourceOptions == null)
                return false;

            bool enabled;
            if (m_ResourceOptions.TryGetValue(resourceOption, out enabled))
            {
                return enabled;
            }

            return false;
        }

        /// <summary>
        /// Gets resources required by all step tools
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        protected CloseOutType GetSharedResources()
        {

            var success = GetExistingJobParametersFile();

            if (success)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            return CloseOutType.CLOSEOUT_FAILED;
        }

        public void SetOption(clsGlobal.eAnalysisResourceOptions resourceOption, bool enabled)
        {
            if (m_ResourceOptions == null)
            {
                m_ResourceOptions = new Dictionary<clsGlobal.eAnalysisResourceOptions, bool>();
            }

            if (m_ResourceOptions.ContainsKey(resourceOption))
            {
                m_ResourceOptions[resourceOption] = enabled;
            }
            else
            {
                m_ResourceOptions.Add(resourceOption, enabled);
            }

            if (resourceOption == clsGlobal.eAnalysisResourceOptions.MyEMSLSearchDisabled)
            {
                MyEMSLSearchDisabled = enabled;
            }
        }

        /// <summary>
        /// Add a filename extension to not move to the results folder
        /// </summary>
        /// <param name="extension"></param>
        /// <remarks>Can be a file extension (like .raw) or even a partial file name like _peaks.txt</remarks>
        public void AddResultFileExtensionToSkip(string extension)
        {
            m_jobParams.AddResultFileExtensionToSkip(extension);
        }

        /// <summary>
        /// Add a filename to not move to the results folder
        /// </summary>
        /// <param name="sourceFilename"></param>
        /// <remarks>FileName can be a file path; only the filename will be stored in m_ResultFilesToSkip</remarks>
        public void AddResultFileToSkip(string sourceFilename)
        {
            m_jobParams.AddResultFileToSkip(sourceFilename);
        }

        /// <summary>
        /// Appends file specified file path to the JobInfo file for the given Job
        /// </summary>
        /// <param name="intJob"></param>
        /// <param name="strFilePath"></param>
        /// <remarks></remarks>
        protected void AppendToJobInfoFile(int intJob, string strFilePath)
        {
            var strJobInfoFilePath = clsDataPackageFileHandler.GetJobInfoFilePath(intJob, m_WorkingDir);

            using (var swJobInfoFile = new StreamWriter(new FileStream(strJobInfoFilePath, FileMode.Append, FileAccess.Write, FileShare.Read)))
            {
                swJobInfoFile.WriteLine(strFilePath);
            }

        }

        public void CacheCurrentDataAndJobInfo()
        {
            if (mCachedDatasetAndJobInfoIsDefined)
            {
                throw new Exception("Call RestoreCachedDataAndJobInfo before calling CacheCurrentDataAndJobInfo again");
            }

            // Cache the current dataset and job info
            mCachedDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();
            mCachedDatasetAndJobInfoIsDefined = true;

        }

        public void RestoreCachedDataAndJobInfo()
        {
            if (!mCachedDatasetAndJobInfoIsDefined)
            {
                throw new Exception("Programming error: RestoreCachedDataAndJobInfo called but mCachedDatasetAndJobInfoIsDefined is false");
            }

            // Restore the dataset and job info for this aggregation job
            OverrideCurrentDatasetAndJobInfo(mCachedDatasetAndJobInfo);

            mCachedDatasetAndJobInfoIsDefined = false;

        }

        /// <summary>
        /// Look for a lock file named dataFilePath + ".lock"
        /// If found, and if less than maxWaitTimeMinutes old, waits for it to be deleted by another process or to age
        /// </summary>
        /// <param name="dataFilePath">Data file path</param>
        /// <param name="dataFileDescription">User friendly description of the data file, e.g. LipidMapsDB</param>
        /// <param name="statusTools">Status Tools object</param>
        /// <param name="maxWaitTimeMinutes">Maximum age of the lock file</param>
        /// <param name="logIntervalMinutes"></param>
        /// <remarks>
        /// Typical steps for using lock files to assure that only one manager is creating a specific file
        /// 1. Call CheckForLockFile() to check for a lock file; wait for it to age
        /// 2. Once CheckForLockFile() exits, check for the required data file; exit the function if the desired file is found
        /// 3. If the file was not found, create a new lock file by calling CreateLockFile()
        /// 4. Do the work and create the data file, including copying to the central location
        /// 5. Delete the lock file by calling DeleteLockFile() or by deleting the file path returned by CreateLockFile()
        /// </remarks>
        public static void CheckForLockFile(
            string dataFilePath,
            string dataFileDescription,
            IStatusFile statusTools,
            int maxWaitTimeMinutes = 120,
            int logIntervalMinutes = 5)
        {

            {
                var blnWaitingForLockFile = false;
                var dtLockFileCreated = DateTime.UtcNow;

                // Look for a recent .lock file
                var fiLockFile = new FileInfo(dataFilePath + LOCK_FILE_EXTENSION);

                if (fiLockFile.Exists)
                {
                    if (DateTime.UtcNow.Subtract(fiLockFile.LastWriteTimeUtc).TotalMinutes < maxWaitTimeMinutes)
                    {
                        blnWaitingForLockFile = true;
                        dtLockFileCreated = fiLockFile.LastWriteTimeUtc;

                        var debugMessage = dataFileDescription + " lock file found; will wait for file to be deleted or age; " +
                            fiLockFile.Name + " created " + fiLockFile.LastWriteTime.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT);
                        clsGlobal.LogDebug(debugMessage);
                    }
                    else
                    {
                        // Lock file has aged; delete it
                        fiLockFile.Delete();
                    }
                }

                if (blnWaitingForLockFile)
                {
                    var dtLastProgressTime = DateTime.UtcNow;
                    if (logIntervalMinutes < 1)
                        logIntervalMinutes = 1;

                    while (blnWaitingForLockFile)
                    {
                        // Wait 5 seconds
                        Thread.Sleep(5000);

                        fiLockFile.Refresh();

                        if (!fiLockFile.Exists)
                        {
                            blnWaitingForLockFile = false;
                        }
                        else if (DateTime.UtcNow.Subtract(dtLockFileCreated).TotalMinutes > maxWaitTimeMinutes)
                        {
                            blnWaitingForLockFile = false;
                        }
                        else
                        {
                            if (DateTime.UtcNow.Subtract(dtLastProgressTime).TotalMinutes >= logIntervalMinutes)
                            {
                                LogDebugMessage("Waiting for lock file " + fiLockFile.Name, statusTools);
                                dtLastProgressTime = DateTime.UtcNow;
                            }
                        }
                    }

                    fiLockFile.Refresh();
                    if (fiLockFile.Exists)
                    {
                        // Lock file is over 2 hours old; delete it
                        DeleteLockFile(dataFilePath);
                    }
                }
            }

        }

        /// <summary>
        /// Create a new lock file named dataFilePath + ".lock"
        /// </summary>
        /// <param name="dataFilePath">Data file path</param>
        /// <param name="taskDescription">Description of current task; will be written the lock file, followed by " at yyyy-MM-dd hh:mm:ss tt"</param>
        /// <returns>Full path to the lock file</returns>
        /// <remarks></remarks>
        public static string CreateLockFile(string dataFilePath, string taskDescription)
        {

            var strLockFilePath = dataFilePath + LOCK_FILE_EXTENSION;
            using (var swLockFile = new StreamWriter(new FileStream(strLockFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read)))
            {
                swLockFile.WriteLine(taskDescription + " at " + DateTime.Now.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
            }

            return strLockFilePath;

        }

        /// <summary>
        /// Delete the lock file for the correspond data file
        /// </summary>
        /// <param name="dataFilePath"></param>
        /// <remarks></remarks>
        public static void DeleteLockFile(string dataFilePath)
        {
            // Delete the lock file
            try
            {
                var lockFilePath = dataFilePath + LOCK_FILE_EXTENSION;

                var fiLockFile = new FileInfo(lockFilePath);
                if (fiLockFile.Exists)
                {
                    fiLockFile.Delete();
                }

            }
            catch (Exception)
            {
                // Ignore errors here
            }

        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>If the file was found in MyEMSL, sourceFolderPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        protected bool CopyFileToWorkDir(string sourceFileName, string sourceFolderPath, string targetFolderPath)
        {
            return m_FileCopyUtilities.CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath);
        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>If the file was found in MyEMSL, then sourceFolderPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        public bool CopyFileToWorkDir(string sourceFileName, string sourceFolderPath, string targetFolderPath, clsLogTools.LogLevels logMsgTypeIfNotFound)
        {

            return m_FileCopyUtilities.CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath, logMsgTypeIfNotFound);
        }

        /// <summary>
        /// Creates a Fasta file based on Ken's DLL
        /// </summary>
        /// <param name="proteinCollectionInfo"></param>
        /// <param name="destFolder">Folder where file will be created</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        protected bool CreateFastaFile(clsProteinCollectionInfo proteinCollectionInfo, string destFolder)
        {

            if (m_DebugLevel >= 1)
            {
                LogMessage("Creating fasta file at " + destFolder);
            }

            if (!Directory.Exists(destFolder))
            {
                Directory.CreateDirectory(destFolder);
            }

            // Instantiate fasta tool if not already done
            if (m_FastaTools == null)
            {
                if (string.IsNullOrWhiteSpace(m_FastaToolsCnStr))
                {
                    m_message = "Protein database connection string not specified";
                    LogMessage("Error in CreateFastaFile: " + m_message, 0, true);
                    return false;
                }

            }

            var retryCount = 1;

            while (retryCount > 0)
            {
                try
                {
                    m_FastaTools = new Protein_Exporter.clsGetFASTAFromDMS(m_FastaToolsCnStr);
                    RegisterEvents(m_FastaTools);

                    m_FastaTools.FileGenerationProgress += m_FastaTools_FileGenerationProgress;
                    m_FastaTools.FileGenerationCompleted += m_FastaTools_FileGenerationCompleted;

                    break;
                }
                catch (Exception ex)
                {
                    if (retryCount > 1)
                    {
                        LogError("Error instantiating clsGetFASTAFromDMS", ex);
                        // Sleep 20 seconds after the first failure and 30 seconds after the second failure
                        if (retryCount == 3)
                        {
                            Thread.Sleep(20000);
                        }
                        else
                        {
                            Thread.Sleep(30000);
                        }
                    }
                    else
                    {
                        m_message = "Error retrieving protein collection or legacy FASTA file: ";
                        if (ex.Message.Contains("could not open database connection"))
                        {
                            m_message += "could not open database connection";
                        }
                        else
                        {
                            m_message += ex.Message;
                        }
                        LogError(m_message, ex);
                        LogDebugMessage("Connection string: " + m_FastaToolsCnStr);
                        LogDebugMessage("Current user: " + Environment.UserName);
                        return false;
                    }
                    retryCount -= 1;
                }
            }

            // Initialize fasta generation state variables
            m_FastaFileName = string.Empty;

            // Set up variables for fasta creation call

            if (!proteinCollectionInfo.IsValid)
            {
                if (string.IsNullOrWhiteSpace(proteinCollectionInfo.ErrorMessage))
                {
                    m_message = "Unknown error determining the Fasta file or protein collection to use; unable to obtain Fasta file";
                }
                else
                {
                    m_message = proteinCollectionInfo.ErrorMessage + "; unable to obtain Fasta file";
                }

                LogMessage("Error in CreateFastaFile: " + m_message, 0, true);
                return false;
            }

            var stepToolName = m_jobParams.GetJobParameter("StepTool", "Unknown");

            string legacyFastaToUse;
            var orgDBDescription = string.Copy(proteinCollectionInfo.OrgDBDescription);

            if (proteinCollectionInfo.UsingSplitFasta && !string.Equals(stepToolName, "DataExtractor", StringComparison.CurrentCultureIgnoreCase))
            {
                if (!proteinCollectionInfo.UsingLegacyFasta)
                {
                    LogError("Cannot use protein collections when running a SplitFasta job; choose a Legacy fasta file instead");
                    return false;
                }

                // Running a SplitFasta job; need to update the name of the fasta file to be of the form FastaFileName_NNx_nn.fasta
                // where NN is the number of total cloned steps and nn is this job's specific step number
                int numberOfClonedSteps;

                legacyFastaToUse = GetSplitFastaFileName(m_jobParams, out m_message, out numberOfClonedSteps);

                if (string.IsNullOrEmpty(legacyFastaToUse))
                {
                    // The error should have already been logged
                    return false;
                }

                orgDBDescription = "Legacy DB: " + legacyFastaToUse;

                // Lookup connection strings
                // Proteinseqs.Protein_Sequences
                var proteinSeqsDBConnectionString = m_mgrParams.GetParam("fastacnstring");
                if (string.IsNullOrWhiteSpace(proteinSeqsDBConnectionString))
                {
                    LogError("Error in CreateFastaFile: manager parameter fastacnstring is not defined");
                    return false;
                }

                // Gigasax.DMS5
                var dmsConnectionString = m_mgrParams.GetParam("connectionstring");
                if (string.IsNullOrWhiteSpace(proteinSeqsDBConnectionString))
                {
                    LogError("Error in CreateFastaFile: manager parameter connectionstring is not defined");
                    return false;
                }

                // Lookup the MSGFPlus Index Folder path
                var strMSGFPlusIndexFilesFolderPathLegacyDB = m_mgrParams.GetParam("MSGFPlusIndexFilesFolderPathLegacyDB", @"\\Proto-7\MSGFPlus_Index_Files");
                if (string.IsNullOrWhiteSpace(strMSGFPlusIndexFilesFolderPathLegacyDB))
                {
                    strMSGFPlusIndexFilesFolderPathLegacyDB = @"\\Proto-7\MSGFPlus_Index_Files\Other";
                }
                else
                {
                    strMSGFPlusIndexFilesFolderPathLegacyDB = Path.Combine(strMSGFPlusIndexFilesFolderPathLegacyDB, "Other");
                }

                if (m_DebugLevel >= 1)
                {
                    LogMessage("Verifying that split fasta file exists: " + legacyFastaToUse);
                }

                // Make sure the original fasta file has already been split into the appropriate number parts
                // and that DMS knows about them
                //
                // Do not use RegisterEvents with m_SplitFastaFileUtility because we handle the progress with a custom handler
                m_SplitFastaFileUtility = new clsSplitFastaFileUtilities(dmsConnectionString, proteinSeqsDBConnectionString, numberOfClonedSteps, m_MgrName);

                m_SplitFastaFileUtility.SplittingBaseFastafile += m_SplitFastaFileUtility_SplittingBaseFastaFile;
                m_SplitFastaFileUtility.ErrorEvent += m_SplitFastaFileUtility_ErrorEvent;
                m_SplitFastaFileUtility.ProgressUpdate += m_SplitFastaFileUtility_ProgressUpdate;

                m_SplitFastaFileUtility.MSGFPlusIndexFilesFolderPathLegacyDB = strMSGFPlusIndexFilesFolderPathLegacyDB;

                m_SplitFastaLastUpdateTime = DateTime.UtcNow;
                m_SplitFastaLastPercentComplete = 0;

                var success = m_SplitFastaFileUtility.ValidateSplitFastaFile(proteinCollectionInfo.LegacyFastaName, legacyFastaToUse);
                if (!success)
                {
                    m_message = m_SplitFastaFileUtility.ErrorMessage;
                    return false;
                }

            }
            else
            {
                legacyFastaToUse = string.Copy(proteinCollectionInfo.LegacyFastaName);
            }

            if (m_DebugLevel >= 2)
            {
                LogMessage(
                    "ProteinCollectionList=" + proteinCollectionInfo.ProteinCollectionList + "; " +
                    "CreationOpts=" + proteinCollectionInfo.ProteinCollectionOptions + "; " +
                    "LegacyFasta=" + legacyFastaToUse);
            }

            try
            {
                if (m_FastaTools == null)
                {
                    LogError("Call to CreateFastaFile without initializing m_FastaTools");
                    return false;
                }

                var hashString = m_FastaTools.ExportFASTAFile(proteinCollectionInfo.ProteinCollectionList, proteinCollectionInfo.ProteinCollectionOptions, legacyFastaToUse, destFolder);

                if (string.IsNullOrEmpty(hashString))
                {
                    // Fasta generator returned empty hash string
                    LogError("m_FastaTools.ExportFASTAFile returned an empty Hash string for the OrgDB; unable to continue; " + orgDBDescription);
                    return false;
                }

            }
            catch (Exception ex)
            {
                if (ex.Message.StartsWith("Legacy fasta file not found:"))
                {
                    var rePathMatcher = new Regex(@"not found: (?<SourceFolder>.+)\\");
                    var reMatch = rePathMatcher.Match(ex.Message);
                    if (reMatch.Success)
                    {
                        m_message = "Legacy fasta file not found at " + reMatch.Groups["SourceFolder"].Value;
                    }
                    else
                    {
                        m_message = "Legacy fasta file not found in the organism folder for this job";
                    }
                }
                else
                {
                    m_message = "Exception generating OrgDb file";
                }

                LogError("Exception generating OrgDb file; " + orgDBDescription, ex);
                return false;
            }

            if (string.IsNullOrEmpty(m_FastaFileName))
            {
                // Fasta generator never raised event FileGenerationCompleted
                LogError("m_FastaTools did not raise event FileGenerationCompleted; unable to continue; " + orgDBDescription);
                return false;
            }

            var fiFastaFile = new FileInfo(Path.Combine(destFolder, m_FastaFileName));

            if (m_DebugLevel >= 1)
            {
                // Log the name of the .Fasta file we're using
                LogDebugMessage("Fasta generation complete, using database: " + m_FastaFileName, null);

                if (m_DebugLevel >= 2)
                {
                    // Also log the file creation and modification dates

                    try
                    {
                        var strFastaFileMsg = "Fasta file last modified: " +
                            GetHumanReadableTimeInterval(
                                DateTime.UtcNow.Subtract(fiFastaFile.LastWriteTimeUtc)) + " ago at " +
                                fiFastaFile.LastWriteTime.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT);

                        strFastaFileMsg += "; file created: " +
                            GetHumanReadableTimeInterval(DateTime.UtcNow.Subtract(fiFastaFile.CreationTimeUtc)) + " ago at " +
                            fiFastaFile.CreationTime.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT);

                        strFastaFileMsg += "; file size: " + fiFastaFile.Length.ToString() + " bytes";

                        LogDebugMessage(strFastaFileMsg);
                    }
                    catch (Exception)
                    {
                        // Ignore errors here
                    }
                }

            }

            // Create/Update the .LastUsed file for the newly created Fasta File
            var lastUsedFilePath = fiFastaFile.FullName + ".LastUsed";
            try
            {
                using (var swLastUsedFile = new StreamWriter(new FileStream(lastUsedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swLastUsedFile.WriteLine(DateTime.UtcNow.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                }
            }
            catch (Exception ex)
            {
                LogError("Warning: unable to create a new .LastUsed file at " + lastUsedFilePath, ex);
            }

            // If we got to here, everything worked OK
            return true;

        }

        /// <summary>
        /// Creates an XML formatted settings file based on data from broker
        /// </summary>
        /// <param name="FileText">String containing XML file contents</param>
        /// <param name="FileNamePath">Name of file to create</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>XML handling based on code provided by Matt Monroe</remarks>
        private bool CreateSettingsFile(string FileText, string FileNamePath)
        {

            var objFormattedXMLWriter = new clsFormattedXMLWriter();

            if (!objFormattedXMLWriter.WriteXMLToFile(FileText, FileNamePath))
            {
                LogError("Error creating settings file " + FileNamePath + ": " + objFormattedXMLWriter.ErrMsg);
                m_message = "Error creating settings file";
                return false;
            }

            return true;
        }

        /// <summary>
        /// Given two dates, returns the most recent date
        /// </summary>
        /// <param name="date1"></param>
        /// <param name="date2"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected static DateTime DateMax(DateTime date1, DateTime date2)
        {
            if (date1 > date2)
            {
                return date1;
            }

            return date2;
        }

        protected void DisableMyEMSLSearch()
        {
            m_MyEMSLUtilities.ClearDownloadQueue();
            MyEMSLSearchDisabled = true;
        }

        /// <summary>
        /// Determines the most appropriate folder to use to obtain dataset files from
        /// Optionally, can require that a certain file also be present in the folder for it to be deemed valid
        /// If no folder is deemed valid, then returns the path defined by Job Param "DatasetStoragePath"
        /// </summary>
        /// <param name="dsName">Name of the dataset</param>
        /// <param name="fileNameToFind">Optional: Name of a file that must exist in the dataset folder; can contain a wildcard, e.g. *.zip</param>
        /// <param name="folderNameToFind">Optional: Name of a subfolder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
        /// <param name="maxAttempts">Maximum number of attempts</param>
        /// <param name="logFolderNotFound">If true, then log a warning if the folder is not found</param>
        /// <param name="retrievingInstrumentDataFolder">Set to True when retrieving an instrument data folder</param>
        /// <param name="validFolderFound">Output parameter: True if a valid folder is ultimately found, otherwise false</param>
        /// <param name="assumeUnpurged">When true, this function returns the path to the dataset folder on the storage server</param>
        /// <param name="folderNotFoundMessage"></param>
        /// <returns>Path to the most appropriate dataset folder</returns>
        /// <remarks>The path returned will be "\\MyEMSL" if the best folder is in MyEMSL</remarks>
        public string FindValidFolder(string dsName, string fileNameToFind, string folderNameToFind, int maxAttempts, bool logFolderNotFound,
                                      bool retrievingInstrumentDataFolder, out bool validFolderFound, bool assumeUnpurged,
                                      out string folderNotFoundMessage)
        {
            var folderPath = m_FolderSearch.FindValidFolder(
                dsName, fileNameToFind, folderNameToFind, maxAttempts, logFolderNotFound,
                retrievingInstrumentDataFolder, assumeUnpurged, out validFolderFound, out folderNotFoundMessage);

            if (!validFolderFound && !assumeUnpurged)
            {
                m_message = folderNotFoundMessage;
            }

            return folderPath;
        }

        /// <summary>
        /// Creates the _ScanStats.txt file for this job's dataset
        /// </summary>
        /// <returns>True if success, false if a problem</returns>
        /// <remarks>Only valid for Thermo .Raw files and .UIMF files.  Will delete the .Raw (or .UIMF) after creating the ScanStats file</remarks>
        protected bool GenerateScanStatsFile()
        {

            const bool deleteRawDataFile = true;
            return GenerateScanStatsFile(deleteRawDataFile);

        }

        /// <summary>
        /// Creates the _ScanStats.txt file for this job's dataset
        /// </summary>
        /// <param name="deleteRawDataFile">True to delete the .raw (or .uimf) file after creating the ScanStats file </param>
        /// <returns>True if success, false if a problem</returns>
        /// <remarks>Only valid for Thermo .Raw files and .UIMF files</remarks>
        protected bool GenerateScanStatsFile(bool deleteRawDataFile)
        {

            var strRawDataType = m_jobParams.GetParam("RawDataType");
            var intDatasetID = m_jobParams.GetJobParameter("DatasetID", 0);

            var strMSFileInfoScannerDir = m_mgrParams.GetParam("MSFileInfoScannerDir");
            if (string.IsNullOrEmpty(strMSFileInfoScannerDir))
            {
                LogError("Manager parameter 'MSFileInfoScannerDir' is not defined (GenerateScanStatsFile)");
                return false;
            }

            var strMSFileInfoScannerDLLPath = Path.Combine(strMSFileInfoScannerDir, "MSFileInfoScanner.dll");
            if (!File.Exists(strMSFileInfoScannerDLLPath))
            {
                LogError("File Not Found (GenerateScanStatsFile): " + strMSFileInfoScannerDLLPath);
                return false;
            }

            string strInputFilePath;

            // Confirm that this dataset is a Thermo .Raw file or a .UIMF file
            switch (GetRawDataType(strRawDataType))
            {
                case eRawDataTypeConstants.ThermoRawFile:
                    strInputFilePath = m_DatasetName + DOT_RAW_EXTENSION;
                    break;
                case eRawDataTypeConstants.UIMF:
                    strInputFilePath = m_DatasetName + DOT_UIMF_EXTENSION;
                    break;
                default:
                    LogError("Invalid dataset type for auto-generating ScanStats.txt file: " + strRawDataType);
                    return false;
            }

            strInputFilePath = Path.Combine(m_WorkingDir, strInputFilePath);

            if (!File.Exists(strInputFilePath))
            {
                if (!m_FileSearch.RetrieveSpectra(strRawDataType))
                {
                    var strExtraMsg = m_message;
                    m_message = "Error retrieving spectra file";
                    if (!string.IsNullOrWhiteSpace(strExtraMsg))
                    {
                        m_message += "; " + strExtraMsg;
                    }
                    LogMessage(m_message, 0, true);
                    return false;
                }

                if (!m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    return false;
                }
            }

            // Make sure the raw data file does not get copied to the results folder
            m_jobParams.AddResultFileToSkip(Path.GetFileName(strInputFilePath));

            var objScanStatsGenerator = new clsScanStatsGenerator(strMSFileInfoScannerDLLPath, m_DebugLevel);

            // Create the _ScanStats.txt and _ScanStatsEx.txt files
            var blnSuccess = objScanStatsGenerator.GenerateScanStatsFile(strInputFilePath, m_WorkingDir, intDatasetID);

            if (blnSuccess)
            {
                if (m_DebugLevel >= 1)
                {
                    LogMessage("Generated ScanStats file using " + strInputFilePath);
                }

                Thread.Sleep(125);
                PRISM.clsProgRunner.GarbageCollectNow();

                if (deleteRawDataFile)
                {
                    try
                    {
                        File.Delete(strInputFilePath);
                    }
                    catch (Exception)
                    {
                        // Ignore errors here
                    }
                }

            }
            else
            {
                LogError("Error generating ScanStats files with clsScanStatsGenerator", objScanStatsGenerator.ErrorMessage);
                if (objScanStatsGenerator.MSFileInfoScannerErrorCount > 0)
                {
                    LogMessage("MSFileInfoScanner encountered " + objScanStatsGenerator.MSFileInfoScannerErrorCount + " errors");
                }
            }

            return blnSuccess;

        }

        protected clsDataPackageJobInfo GetCurrentDatasetAndJobInfo()
        {

            var jobNumber = m_jobParams.GetJobParameter("StepParameters", "Job", 0);
            var dataset = m_jobParams.GetJobParameter("JobParameters", "DatasetNum", m_DatasetName);

            var jobInfo = new clsDataPackageJobInfo(jobNumber, dataset)
            {
                DatasetID = m_jobParams.GetJobParameter("JobParameters", "DatasetID", 0),
                Instrument = m_jobParams.GetJobParameter("JobParameters", "Instrument", string.Empty),
                InstrumentGroup = m_jobParams.GetJobParameter("JobParameters", "InstrumentGroup", string.Empty),
                Experiment = m_jobParams.GetJobParameter("JobParameters", "Experiment", string.Empty),
                Experiment_Reason = string.Empty,
                Experiment_Comment = string.Empty,
                Experiment_Organism = string.Empty,
                Experiment_NEWT_ID = 0,
                Experiment_NEWT_Name = string.Empty,
                Tool = m_jobParams.GetJobParameter("JobParameters", "ToolName", string.Empty),
                NumberOfClonedSteps = m_jobParams.GetJobParameter("NumberOfClonedSteps", 0),
                ResultType = m_jobParams.GetJobParameter("JobParameters", "ResultType", string.Empty),
                SettingsFileName = m_jobParams.GetJobParameter("JobParameters", "SettingsFileName", string.Empty),
                ParameterFileName = m_jobParams.GetJobParameter("PeptideSearch", "ParmFileName", string.Empty),
                LegacyFastaFileName = m_jobParams.GetJobParameter("PeptideSearch", "legacyFastaFileName", string.Empty)
            };

            jobInfo.OrganismDBName = string.Copy(jobInfo.LegacyFastaFileName);

            jobInfo.ProteinCollectionList = m_jobParams.GetJobParameter("PeptideSearch", "ProteinCollectionList", string.Empty);
            jobInfo.ProteinOptions = m_jobParams.GetJobParameter("PeptideSearch", "ProteinOptions", string.Empty);

            jobInfo.ServerStoragePath = m_jobParams.GetJobParameter("JobParameters", "DatasetStoragePath", string.Empty);
            jobInfo.ArchiveStoragePath = m_jobParams.GetJobParameter("JobParameters", "DatasetArchivePath", string.Empty);
            jobInfo.ResultsFolderName = m_jobParams.GetJobParameter("JobParameters", "inputFolderName", string.Empty);
            jobInfo.DatasetFolderName = m_jobParams.GetJobParameter("JobParameters", "DatasetFolderName", string.Empty);
            jobInfo.SharedResultsFolder = m_jobParams.GetJobParameter("JobParameters", "SharedResultsFolders", string.Empty);
            jobInfo.RawDataType = m_jobParams.GetJobParameter("JobParameters", "RawDataType", string.Empty);

            return jobInfo;

        }

        /// <summary>
        /// Lookups up the storage path for a given data package
        /// </summary>
        /// <param name="connectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
        /// <param name="dataPackageID">Data Package ID</param>
        /// <returns>Storage path if successful, empty path if an error or unknown data package</returns>
        /// <remarks></remarks>
        protected static string GetDataPackageStoragePath(string connectionString, int dataPackageID)
        {

            // Requests Dataset information from a data package
            const short RETRY_COUNT = 3;

            var sqlStr = new System.Text.StringBuilder();

            sqlStr.Append("Select [Share Path] AS StoragePath ");
            sqlStr.Append("From V_DMS_Data_Packages ");
            sqlStr.Append("Where ID = " + dataPackageID.ToString());

            DataTable resultSet;

            // Get a table to hold the results of the query
            var success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), connectionString, "GetDataPackageStoragePath", RETRY_COUNT, out resultSet);

            if (!success)
            {
                var errorMessage = "GetDataPackageStoragePath; Excessive failures attempting to retrieve data package info from database";
                clsGlobal.LogError(errorMessage);
                resultSet.Dispose();
                return string.Empty;
            }

            // Verify at least one row returned
            if (resultSet.Rows.Count < 1)
            {
                // No data was returned
                // Log an error

                var errorMessage = "GetDataPackageStoragePath; Data package not found: " + dataPackageID.ToString();
                clsGlobal.LogError(errorMessage);
                return string.Empty;
            }

            var curRow = resultSet.Rows[0];

            var storagePath = clsGlobal.DbCStr(curRow[0]);

            resultSet.Dispose();
            return storagePath;
        }

        /// <summary>
        /// Examines the folder tree in strFolderPath to find the a folder with a name like 2013_2
        /// </summary>
        /// <param name="strFolderPath"></param>
        /// <returns>Matching folder name if found, otherwise an empty string</returns>
        /// <remarks></remarks>
        public static string GetDatasetYearQuarter(string strFolderPath)
        {

            if (string.IsNullOrEmpty(strFolderPath))
            {
                return string.Empty;
            }

            // RegEx to find the year_quarter folder name
            // Valid matches include: 2014_1, 2014_01, 2014_4
            var reYearQuarter = new Regex("^[0-9]{4}_0*[1-4]$", RegexOptions.Compiled);

            // Split strFolderPath on the path separator
            var lstFolders = strFolderPath.Split(Path.DirectorySeparatorChar).ToList();
            lstFolders.Reverse();

            foreach (var strFolder in lstFolders)
            {
                var reMatch = reYearQuarter.Match(strFolder);
                if (reMatch.Success)
                {
                    return reMatch.Value;
                }
            }

            return string.Empty;

        }

        /// <summary>
        /// Examine the fasta file to determine the fraction of the proteins that are decoy (reverse) proteins
        /// </summary>
        /// <param name="fiFastaFile">FASTA file to examine</param>
        /// <param name="proteinCount">Output parameter: total protein count</param>
        /// <returns>Fraction of the proteins that are decoy (for example 0.5 if half of the proteins start with Reversed_)</returns>
        /// <remarks>Decoy proteins start with Reversed_</remarks>
        public static double GetDecoyFastaCompositionStats(FileInfo fiFastaFile, out int proteinCount)
        {

            var decoyProteinPrefix = GetDefaultDecoyPrefixes().First();
            return GetDecoyFastaCompositionStats(fiFastaFile, decoyProteinPrefix, out proteinCount);

        }

        /// <summary>
        /// Examine the fasta file to determine the fraction of the proteins that are decoy (reverse) proteins
        /// </summary>
        /// <param name="fiFastaFile">FASTA file to examine</param>
        /// <param name="decoyProteinPrefix"></param>
        /// <param name="proteinCount">Output parameter: total protein count</param>
        /// <returns>Fraction of the proteins that are decoy (for example 0.5 if half of the proteins start with Reversed_)</returns>
        /// <remarks>Decoy proteins start with decoyProteinPrefix</remarks>
        public static double GetDecoyFastaCompositionStats(FileInfo fiFastaFile, string decoyProteinPrefix, out int proteinCount)
        {

            // Look for protein names that look like:
            // >decoyProteinPrefix
            // where
            // decoyProteinPrefix is typically XXX. or XXX_ or Reversed_

            var prefixToFind = ">" + decoyProteinPrefix;
            var forwardProteinCount = 0;
            var reverseProteinCount = 0;

            using (var srFastaFile = new StreamReader(new FileStream(fiFastaFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                while (!srFastaFile.EndOfStream)
                {
                    var dataLine = srFastaFile.ReadLine();
                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    if (dataLine.StartsWith(">"))
                    {
                        // Protein header line found
                        if (dataLine.StartsWith(prefixToFind))
                        {
                            reverseProteinCount += 1;
                        }
                        else
                        {
                            forwardProteinCount += 1;
                        }
                    }
                }
            }

            double fractionDecoy = 0;

            proteinCount = forwardProteinCount + reverseProteinCount;
            if (proteinCount > 0)
            {
                fractionDecoy = reverseProteinCount / (double)proteinCount;
            }

            return fractionDecoy;

        }

        public static List<string> GetDefaultDecoyPrefixes()
        {

            // Decoy proteins created by MSGF+ start with XXX_
            // Decoy proteins created by DMS start with Reversed_
            var decoyPrefixes = new List<string> {
                "Reversed_",
                "XXX_",
                "XXX:"
            };

            return decoyPrefixes;

        }

        /// <summary>
        /// Look for a JobParameters file from the previous job step
        /// If found, copy to the working directory, naming in JobParameters_JobNum_PreviousStep.xml
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        private bool GetExistingJobParametersFile()
        {

            try
            {
                var stepNum = m_jobParams.GetJobParameter("StepParameters", "Step", 1);
                if (stepNum == 1)
                {
                    // This is the first step; nothing to retrieve
                    return true;
                }

                var transferFolderPath = GetTransferFolderPathForJobStep(useInputFolder: true);
                if (string.IsNullOrEmpty(transferFolderPath))
                {
                    // Transfer folder parameter is empty; nothing to retrieve
                    return true;
                }

                // Construct the filename, for example JobParameters_1394245.xml
                var jobParametersFilename = clsAnalysisJob.JobParametersFilename(m_JobNum);
                var sourceFile = new FileInfo(Path.Combine(transferFolderPath, jobParametersFilename));

                if (!sourceFile.Exists)
                {
                    // File not found; nothing to copy
                    return true;
                }

                // Copy the file, renaming to avoid a naming collision
                var destFilePath = Path.Combine(m_WorkingDir, Path.GetFileNameWithoutExtension(sourceFile.Name) + "_PreviousStep.xml");
                if (m_FileCopyUtilities.CopyFileWithRetry(sourceFile.FullName, destFilePath, overwrite: true, maxCopyAttempts: 3))
                {
                    if (m_DebugLevel > 3)
                    {
                        LogDebugMessage("GetExistingJobParametersFile, File copied:  " + sourceFile.FullName);
                    }
                }
                else
                {
                    LogError("Error in GetExistingJobParametersFile copying file " + sourceFile.FullName);
                    // Return false
                }

                var sourceJobParamXMLFile = new FileInfo(destFilePath);

                var masterJobParamXMLFile = new FileInfo(Path.Combine(m_WorkingDir, clsAnalysisJob.JobParametersFilename(m_JobNum)));

                if (!sourceJobParamXMLFile.Exists)
                {
                    // The file wasn't copied
                    LogError("Job Parameters file from the previous job step was not found at " + sourceJobParamXMLFile.FullName);
                    return false;
                }

                if (!masterJobParamXMLFile.Exists)
                {
                    // The JobParameters XML file was not found
                    LogError("Job Parameters file not found at " + masterJobParamXMLFile.FullName);
                    return false;
                }

                // Update the JobParameters XML file that was created by this job step to include the information from the previous job steps
                var success = MergeJobParamXMLStepParameters(sourceJobParamXMLFile.FullName, masterJobParamXMLFile.FullName);

                if (success)
                {
                    m_jobParams.AddResultFileToSkip(sourceJobParamXMLFile.Name);
                }

                return success;

            }
            catch (Exception ex)
            {
                LogError("Exception in GetExistingJobParametersFile", ex);
                return false;
            }

        }

        /// <summary>
        /// Examine the specified DMS_Temp_Org folder to find the FASTA files and their corresponding .fasta.LastUsed or .hashcheck files
        /// </summary>
        /// <param name="diOrgDbFolder"></param>
        /// <returns>Dictionary of FASTA files, including the last usage date for each</returns>
        private Dictionary<FileInfo, DateTime> GetFastaFilesByLastUse(DirectoryInfo diOrgDbFolder)
        {

            // Keys are the fasta file; values are the dtLastUsed time of the file (nominally obtained from a .hashcheck or .lastused file)
            var dctFastaFiles = new Dictionary<FileInfo, DateTime>();

            foreach (var fiFile in diOrgDbFolder.GetFiles("*.fasta"))
            {
                if (!dctFastaFiles.ContainsKey(fiFile))
                {
                    var dtLastUsed = DateMax(fiFile.LastWriteTimeUtc, fiFile.CreationTimeUtc);

                    // Look for a .hashcheck file
                    var lstHashCheckfiles = diOrgDbFolder.GetFiles(fiFile.Name + "*.hashcheck").ToList();
                    if (lstHashCheckfiles.Count > 0)
                    {
                        dtLastUsed = DateMax(dtLastUsed, lstHashCheckfiles.First().LastWriteTimeUtc);
                    }

                    // Look for a .LastUsed file
                    var lstLastUsedFiles = diOrgDbFolder.GetFiles(fiFile.Name + ".LastUsed").ToList();
                    if (lstLastUsedFiles.Count > 0)
                    {
                        dtLastUsed = DateMax(dtLastUsed, lstLastUsedFiles.First().LastWriteTimeUtc);

                        try
                        {
                            // Read the date stored in the file
                            using (var srLastUsedfile = new StreamReader(new FileStream(lstLastUsedFiles.First().FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                            {
                                if (!srLastUsedfile.EndOfStream)
                                {
                                    var strLastUseDate = srLastUsedfile.ReadLine();
                                    DateTime dtLastUsedActual;
                                    if (DateTime.TryParse(strLastUseDate, out dtLastUsedActual))
                                    {
                                        dtLastUsed = DateMax(dtLastUsed, dtLastUsedActual);
                                    }
                                }
                            }
                        }
                        catch (Exception)
                        {
                            // Ignore errors here
                        }

                    }
                    dctFastaFiles.Add(fiFile, dtLastUsed);
                }
            }

            return dctFastaFiles;

        }

        /// <summary>
        /// Converts the given timespan to the total days, hours, minutes, or seconds as a string
        /// </summary>
        /// <param name="dtInterval">Timespan to convert</param>
        /// <returns>Timespan length in human readable form</returns>
        /// <remarks></remarks>
        protected string GetHumanReadableTimeInterval(TimeSpan dtInterval)
        {
            if (dtInterval.TotalDays >= 1)
            {
                // Report Days
                return dtInterval.TotalDays.ToString("0.00") + " days";
            }

            if (dtInterval.TotalHours >= 1)
            {
                // Report hours
                return dtInterval.TotalHours.ToString("0.00") + " hours";
            }

            if (dtInterval.TotalMinutes >= 1)
            {
                // Report minutes
                return dtInterval.TotalMinutes.ToString("0.00") + " minutes";
            }

            // Report seconds
            return dtInterval.TotalSeconds.ToString("0.0") + " seconds";
        }

        /// <summary>
        /// Get the MSXML Cache Folder path that is appropriate for this job
        /// </summary>
        /// <param name="cacheFolderPathBase"></param>
        /// <param name="jobParams"></param>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        /// <remarks>Uses job parameter OutputFolderName, which should be something like MSXML_Gen_1_120_275966</remarks>
        public static string GetMSXmlCacheFolderPath(string cacheFolderPathBase, IJobParams jobParams, out string errorMessage)
        {

            // Lookup the output folder; e.g. MSXML_Gen_1_120_275966
            var outputFolderName = jobParams.GetJobParameter("OutputFolderName", string.Empty);
            if (string.IsNullOrEmpty(outputFolderName))
            {
                errorMessage = "OutputFolderName is empty; cannot construct MSXmlCache path";
                return string.Empty;
            }

            string msXmlToolNameVersionFolder;
            try
            {
                msXmlToolNameVersionFolder = GetMSXmlToolNameVersionFolder(outputFolderName);
            }
            catch (Exception)
            {
                errorMessage = "OutputFolderName is not in the expected form of ToolName_Version_DatasetID (" + outputFolderName + "); cannot construct MSXmlCache path";
                return string.Empty;
            }

            return GetMSXmlCacheFolderPath(cacheFolderPathBase, jobParams, msXmlToolNameVersionFolder, out errorMessage);

        }

        /// <summary>
        /// Get the path to the cache folder; used for retrieving cached .mzML files that are stored in ToolName_Version folders
        /// </summary>
        /// <param name="cacheFolderPathBase">Cache folder base, e.g. \\Proto-11\MSXML_Cache</param>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="msXmlToolNameVersionFolder">ToolName_Version folder, e.g. MSXML_Gen_1_93</param>
        /// <param name="errorMessage">Output parameter: error message</param>
        /// <returns>Path to the cache folder; empty string if an error</returns>
        /// <remarks>Uses job parameter DatasetStoragePath to determine the Year_Quarter string to append to the end of the path</remarks>
        public static string GetMSXmlCacheFolderPath(string cacheFolderPathBase, IJobParams jobParams, string msXmlToolNameVersionFolder, out string errorMessage)
        {

            errorMessage = string.Empty;

            var strDatasetStoragePath = jobParams.GetParam("JobParameters", "DatasetStoragePath");
            if (string.IsNullOrEmpty(strDatasetStoragePath))
            {
                strDatasetStoragePath = jobParams.GetParam("JobParameters", "DatasetArchivePath");
            }

            if (string.IsNullOrEmpty(strDatasetStoragePath))
            {
                errorMessage = "JobParameters does not contain DatasetStoragePath or DatasetArchivePath; cannot construct MSXmlCache path";
                return string.Empty;
            }

            var strYearQuarter = GetDatasetYearQuarter(strDatasetStoragePath);
            if (string.IsNullOrEmpty(strYearQuarter))
            {
                errorMessage = "Unable to extract the dataset Year_Quarter code from " + strDatasetStoragePath + "; cannot construct MSXmlCache path";
                return string.Empty;
            }

            // Combine the cache folder path, ToolNameVersion, and the dataset Year_Quarter code
            var targetFolderPath = Path.Combine(cacheFolderPathBase, msXmlToolNameVersionFolder, strYearQuarter);

            return targetFolderPath;

        }

        /// <summary>
        /// Examine a folder of the form MSXML_Gen_1_93_367204 and remove the DatasetID portion
        /// </summary>
        /// <param name="toolNameVersionDatasetIDFolder">Shared results folder name</param>
        /// <returns>The trimmed folder name if a valid folder; throws an exception if the folder name is not the correct format</returns>
        /// <remarks></remarks>
        public static string GetMSXmlToolNameVersionFolder(string toolNameVersionDatasetIDFolder)
        {

            // Remove the dataset ID from the end of the folder name
            var reToolNameAndVersion = new Regex(@"^(?<ToolNameVersion>.+\d+_\d+)_\d+$");
            var reMatch = reToolNameAndVersion.Match(toolNameVersionDatasetIDFolder);
            if (!reMatch.Success)
            {
                throw new Exception("Folder name is not in the expected form of ToolName_Version_DatasetID; unable to strip out the dataset ID");
            }

            return reMatch.Groups["ToolNameVersion"].ToString();

        }

        /// <summary>
        /// Retrieve the .mzML or .mzXML file associated with this job (based on Job Parameter MSXMLOutputType)
        /// </summary>
        /// <returns>CLOSEOUT_SUCCESS or CLOSEOUT_FAILED</returns>
        /// <remarks>
        /// If MSXMLOutputType is not defined, attempts to retrieve a .mzML file
        /// If the .mzML file is not found, will attempt to create it
        /// </remarks>
        protected CloseOutType GetMsXmlFile()
        {
            var msXmlOutputType = m_jobParams.GetJobParameter("MSXMLOutputType", string.Empty);

            CloseOutType eResult;

            if (msXmlOutputType.ToLower() == "mzxml")
            {
                eResult = GetMzXMLFile();
            }
            else
            {
                eResult = GetMzMLFile();
            }

            return eResult;

        }

        protected CloseOutType GetMzMLFile()
        {

            LogMessage("Getting mzML file");

            string errorMessage;
            bool fileMissingFromCache;
            const bool unzipFile = true;

            var success = m_FileSearch.RetrieveCachedMzMLFile(unzipFile, out errorMessage, out fileMissingFromCache);
            if (!success)
            {
                return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_MZML_EXTENSION);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        protected CloseOutType GetMzXMLFile()
        {

            // Retrieve the .mzXML file for this dataset
            // Do not use RetrieveMZXmlFile since that function looks for any valid MSXML_Gen folder for this dataset
            // Instead, use FindAndRetrieveMiscFiles

            LogMessage("Getting mzXML file");

            // Note that capitalization matters for the extension; it must be .mzXML
            var fileToGet = m_DatasetName + DOT_MZXML_EXTENSION;

            if (!m_FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Look for a .mzXML file in the cache instead

                string errorMessage;
                bool fileMissingFromCache;
                const bool unzipFile = true;

                var success = m_FileSearch.RetrieveCachedMzXMLFile(unzipFile, out errorMessage, out fileMissingFromCache);
                if (!success)
                {
                    return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_MZXML_EXTENSION);
                }

            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            if (!m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        protected CloseOutType GetPBFFile()
        {

            LogMessage("Getting PBF file");

            string errorMessage;
            bool fileMissingFromCache;

            var success = m_FileSearch.RetrieveCachedPBFFile(out errorMessage, out fileMissingFromCache);
            if (!success)
            {
                return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_PBF_EXTENSION);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        protected CloseOutType HandleMsXmlRetrieveFailure(bool fileMissingFromCache, string errorMessage, string msXmlExtension)
        {

            if (fileMissingFromCache)
            {
                if (string.IsNullOrEmpty(errorMessage))
                {
                    errorMessage = "Cached " + msXmlExtension + " file does not exist; will re-generate it";
                }

                LogMessage("Warning: " + errorMessage);
                return CloseOutType.CLOSEOUT_FILE_NOT_IN_CACHE;
            }

            if (string.IsNullOrEmpty(errorMessage))
            {
                errorMessage = "Unknown error in RetrieveCached" + msXmlExtension.TrimStart('.') + "File";
            }

            LogMessage(errorMessage, 0, true);
            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;

        }

        /// <summary>
        /// Gets fake job information for a dataset that is associated with a data package yet has no analysis jobs associated with the data package
        /// </summary>
        /// <param name="udtDatasetInfo"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static clsDataPackageJobInfo GetPseudoDataPackageJobInfo(clsDataPackageDatasetInfo udtDatasetInfo)
        {

            // Store the negative of the dataset ID as the job number
            var jobInfo = new clsDataPackageJobInfo(-udtDatasetInfo.DatasetID, udtDatasetInfo.Dataset)
            {
                DatasetID = udtDatasetInfo.DatasetID,
                Instrument = udtDatasetInfo.Instrument,
                InstrumentGroup = udtDatasetInfo.InstrumentGroup,
                Experiment = udtDatasetInfo.Experiment,
                Experiment_Reason = udtDatasetInfo.Experiment_Reason,
                Experiment_Comment = udtDatasetInfo.Experiment_Comment,
                Experiment_Organism = udtDatasetInfo.Experiment_Organism,
                Experiment_NEWT_ID = udtDatasetInfo.Experiment_NEWT_ID,
                Experiment_NEWT_Name = udtDatasetInfo.Experiment_NEWT_Name,
                Tool = "Dataset info (no tool)",
                NumberOfClonedSteps = 0,
                ResultType = "Dataset info (no type)",
                PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Unknown,
                SettingsFileName = string.Empty,
                ParameterFileName = string.Empty,
                OrganismDBName = string.Empty,
                LegacyFastaFileName = string.Empty,
                ProteinCollectionList = string.Empty,
                ProteinOptions = string.Empty,
                ResultsFolderName = string.Empty,
                DatasetFolderName = udtDatasetInfo.Dataset,
                SharedResultsFolder = string.Empty,
                RawDataType = udtDatasetInfo.RawDataType
            };

            try
            {
                // Archive storage path and server storage path track the folder just above the dataset folder
                var archiveFolder = new DirectoryInfo(udtDatasetInfo.ArchiveStoragePath);
                jobInfo.ArchiveStoragePath = archiveFolder.Parent.FullName;
            }
            catch (Exception)
            {
                Console.WriteLine("Exception in GetPseudoDataPackageJobInfo determining the parent folder of " + udtDatasetInfo.ArchiveStoragePath);
                jobInfo.ArchiveStoragePath = udtDatasetInfo.ArchiveStoragePath.Replace(@" \ " + udtDatasetInfo.Dataset, "");
            }

            try
            {
                var storageFolder = new DirectoryInfo(udtDatasetInfo.ServerStoragePath);
                jobInfo.ServerStoragePath = storageFolder.Parent.FullName;
            }
            catch (Exception)
            {
                Console.WriteLine("Exception in GetPseudoDataPackageJobInfo determining the parent folder of " + udtDatasetInfo.ServerStoragePath);
                jobInfo.ServerStoragePath = udtDatasetInfo.ServerStoragePath.Replace(@" \ " + udtDatasetInfo.Dataset, "");
            }

            return jobInfo;

        }

        public static eRawDataTypeConstants GetRawDataType(string strRawDataType)
        {

            if (string.IsNullOrEmpty(strRawDataType))
            {
                return eRawDataTypeConstants.Unknown;
            }

            switch (strRawDataType.ToLower())
            {
                case RAW_DATA_TYPE_DOT_D_FOLDERS:
                    return eRawDataTypeConstants.AgilentDFolder;
                case RAW_DATA_TYPE_ZIPPED_S_FOLDERS:
                    return eRawDataTypeConstants.ZippedSFolders;
                case RAW_DATA_TYPE_DOT_RAW_FOLDER:
                    return eRawDataTypeConstants.MicromassRawFolder;
                case RAW_DATA_TYPE_DOT_RAW_FILES:
                    return eRawDataTypeConstants.ThermoRawFile;
                case RAW_DATA_TYPE_DOT_WIFF_FILES:
                    return eRawDataTypeConstants.AgilentQStarWiffFile;
                case RAW_DATA_TYPE_DOT_UIMF_FILES:
                    return eRawDataTypeConstants.UIMF;
                case RAW_DATA_TYPE_DOT_MZXML_FILES:
                    return eRawDataTypeConstants.mzXML;
                case RAW_DATA_TYPE_DOT_MZML_FILES:
                    return eRawDataTypeConstants.mzML;
                case RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                    return eRawDataTypeConstants.BrukerFTFolder;
                case RAW_DATA_TYPE_BRUKER_MALDI_SPOT:
                    return eRawDataTypeConstants.BrukerMALDISpot;
                case RAW_DATA_TYPE_BRUKER_MALDI_IMAGING:
                    return eRawDataTypeConstants.BrukerMALDIImaging;
                case RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER:
                    return eRawDataTypeConstants.BrukerTOFBaf;
                default:
                    return eRawDataTypeConstants.Unknown;
            }

        }

        public static string GetRawDataTypeName(IJobParams jobParams, out string errorMessage)
        {

            errorMessage = string.Empty;

            var msXmlOutputType = jobParams.GetParam("MSXMLOutputType");

            if (string.IsNullOrWhiteSpace(msXmlOutputType))
            {
                return jobParams.GetParam("RawDataType");
            }

            switch (msXmlOutputType.ToLower())
            {
                case "mzxml":
                    return RAW_DATA_TYPE_DOT_MZXML_FILES;
                case "mzml":
                    return RAW_DATA_TYPE_DOT_MZML_FILES;
                default:
                    return string.Empty;
            }
        }

        protected string GetRawDataTypeName()
        {

            string errorMessage;
            var rawDataTypeName = GetRawDataTypeName(m_jobParams, out errorMessage);

            if (string.IsNullOrWhiteSpace(rawDataTypeName))
            {
                if (string.IsNullOrWhiteSpace(errorMessage))
                {
                    LogError("Unable to determine the instrument data type using GetRawDataTypeName");
                }
                else
                {
                    LogError(errorMessage);
                }

                return string.Empty;
            }

            return rawDataTypeName;

        }

        /// <summary>
        /// Get the name of the split fasta file to use for this job
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="errorMessage">Output parameter: error message</param>
        /// <returns>The name of the split fasta file to use</returns>
        /// <remarks>Returns an empty string if an error</remarks>
        public static string GetSplitFastaFileName(IJobParams jobParams, out string errorMessage)
        {
            int numberOfClonedSteps;

            return GetSplitFastaFileName(jobParams, out errorMessage, out numberOfClonedSteps);

        }

        /// <summary>
        /// Get the name of the split fasta file to use for this job
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="errorMessage">Output parameter: error message</param>
        /// <param name="numberOfClonedSteps">Output parameter: total number of cloned steps</param>
        /// <returns>The name of the split fasta file to use</returns>
        /// <remarks>Returns an empty string if an error</remarks>
        public static string GetSplitFastaFileName(IJobParams jobParams, out string errorMessage, out int numberOfClonedSteps)
        {
            numberOfClonedSteps = 0;

            var legacyFastaFileName = jobParams.GetJobParameter("LegacyFastaFileName", "");
            if (string.IsNullOrEmpty(legacyFastaFileName))
            {
                errorMessage = "Parameter LegacyFastaFileName is empty for the job; cannot determine the SplitFasta file name for this job step";
                clsGlobal.LogError(errorMessage);
                return string.Empty;
            }

            numberOfClonedSteps = jobParams.GetJobParameter("NumberOfClonedSteps", 0);
            if (numberOfClonedSteps == 0)
            {
                errorMessage = "Settings file is missing parameter NumberOfClonedSteps; cannot determine the SplitFasta file name for this job step";
                clsGlobal.LogError(errorMessage);
                return string.Empty;
            }

            var iteration = GetSplitFastaIteration(jobParams, out errorMessage);
            if (iteration < 1)
            {
                var toolName = jobParams.GetJobParameter("StepTool", string.Empty);
                if (clsGlobal.IsMatch(toolName, "Mz_Refinery"))
                {
                    // Running MzRefinery
                    // Override iteration to be 1
                    iteration = 1;
                }
                else
                {
                    if (string.IsNullOrEmpty(errorMessage))
                    {
                        errorMessage = "GetSplitFastaIteration computed an iteration value of " + iteration + "; " +
                            "cannot determine the SplitFasta file name for this job step";
                        clsGlobal.LogError(errorMessage);
                    }
                    return string.Empty;
                }
            }

            var fastaNameBase = Path.GetFileNameWithoutExtension(legacyFastaFileName);
            var splitFastaName = fastaNameBase + "_" + numberOfClonedSteps.ToString() + "x_";

            if (numberOfClonedSteps < 10)
            {
                splitFastaName += iteration.ToString("0") + ".fasta";
            }
            else if (numberOfClonedSteps < 100)
            {
                splitFastaName += iteration.ToString("00") + ".fasta";
            }
            else
            {
                splitFastaName += iteration.ToString("000") + ".fasta";
            }

            return splitFastaName;

        }

        public static int GetSplitFastaIteration(IJobParams jobParams, out string errorMessage)
        {

            errorMessage = string.Empty;

            var cloneStepRenumStart = jobParams.GetJobParameter("CloneStepRenumberStart", 0);
            if (cloneStepRenumStart == 0)
            {
                errorMessage = "Settings file is missing parameter CloneStepRenumberStart; cannot determine the SplitFasta iteration value for this job step";
                clsGlobal.LogError(errorMessage);
                return 0;
            }

            var stepNumber = jobParams.GetJobParameter("StepParameters", "Step", 0);
            if (stepNumber == 0)
            {
                errorMessage = "Job parameter Step is missing; cannot determine the SplitFasta iteration value for this job step";
                clsGlobal.LogError(errorMessage);
                return 0;
            }

            return stepNumber - cloneStepRenumStart + 1;

        }

        /// <summary>
        /// Read a JobParameters XML file to find the sections named "StepParameters"
        /// </summary>
        /// <param name="doc">XDocument to scan</param>
        /// <returns>Dictionary where keys are the step number and values are the XElement node with the step parameters for the given step</returns>
        private Dictionary<int, XElement> GetStepParametersSections(XDocument doc)
        {

            var stepNumToParamsMap = new Dictionary<int, XElement>();

            var stepParamSections = doc.Elements("sections").Elements("section").ToList();
            foreach (var section in stepParamSections)
            {
                if (!section.HasAttributes)
                    continue;

                var nameAttrib = section.Attribute("name");
                if (nameAttrib == null)
                    continue;

                if (!string.Equals(nameAttrib.Value, "StepParameters"))
                    continue;

                var stepAttrib = section.Attribute("step");
                if (stepAttrib == null)
                    continue;

                int stepNumber;
                if (int.TryParse(stepAttrib.Value, out stepNumber))
                {
                    // Add or update the XML for this section
                    stepNumToParamsMap[stepNumber] = section;
                }

            }

            return stepNumToParamsMap;
        }

        /// <summary>
        /// Get the input or output transfer folder path specific to this job step
        /// </summary>
        /// <param name="useInputFolder">True to use "InputFolderName", False to use "OutputFolderName"</param>
        /// <returns></returns>
        protected string GetTransferFolderPathForJobStep(bool useInputFolder)
        {

            var transferFolderPathBase = m_jobParams.GetParam("transferFolderPath");
            if (string.IsNullOrEmpty(transferFolderPathBase))
            {
                // Transfer folder parameter is empty; return an empty string
                return string.Empty;
            }

            // Append the dataset folder name to the transfer folder path
            var datasetFolderName = m_jobParams.GetParam("StepParameters", "DatasetFolderName");
            if (string.IsNullOrWhiteSpace(datasetFolderName))
                datasetFolderName = m_DatasetName;

            string folderName;

            if (useInputFolder)
            {
                folderName = m_jobParams.GetParam("InputFolderName");
            }
            else
            {
                folderName = m_jobParams.GetParam("OutputFolderName");
            }

            if (string.IsNullOrEmpty(folderName))
            {
                // Input (or output) folder parameter is empty; return an empty string
                return string.Empty;
            }

            var transferFolderPath = Path.Combine(transferFolderPathBase, datasetFolderName, folderName);

            return transferFolderPath;

        }

        /// <summary>
        /// Unzip gzipFilePath into the working directory
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="gzipFilePath">.gz file to unzip</param>
        /// <returns>True if success; false if an error</returns>
        protected bool GUnzipFile(string gzipFilePath)
        {
            return m_FileSearch.GUnzipFile(gzipFilePath);
        }

        /// <summary>
        /// Looks up dataset information for a data package
        /// </summary>
        /// <param name="dctDataPackageDatasets"></param>
        /// <returns>True if a data package is defined and it has datasets associated with it</returns>
        /// <remarks></remarks>
        protected bool LoadDataPackageDatasetInfo(out Dictionary<int, clsDataPackageDatasetInfo> dctDataPackageDatasets)
        {

            // Gigasax.DMS_Pipeline
            var connectionString = m_mgrParams.GetParam("brokerconnectionstring");

            var dataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1);

            if (dataPackageID < 0)
            {
                dctDataPackageDatasets = new Dictionary<int, clsDataPackageDatasetInfo>();
                return false;
            }

            return LoadDataPackageDatasetInfo(connectionString, dataPackageID, out dctDataPackageDatasets);
        }

        /// <summary>
        /// Looks up dataset information for a data package
        /// </summary>
        /// <param name="connectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
        /// <param name="dataPackageID">Data Package ID</param>
        /// <param name="dctDataPackageDatasets">Datasets associated with the given data package</param>
        /// <returns>True if a data package is defined and it has datasets associated with it</returns>
        /// <remarks></remarks>
        public static bool LoadDataPackageDatasetInfo(string connectionString, int dataPackageID, out Dictionary<int, clsDataPackageDatasetInfo> dctDataPackageDatasets)
        {

            // Obtains the dataset information for a data package
            const short RETRY_COUNT = 3;

            dctDataPackageDatasets = new Dictionary<int, clsDataPackageDatasetInfo>();

            var sqlStr = new System.Text.StringBuilder();

            // View V_DMS_Data_Package_Datasets is in the DMS_Pipeline database
            // That view references view V_DMS_Data_Package_Aggregation_Datasets in the DMS_Data_Package database
            // That view pulls information from several tables in the DMS_Data_Package database, plus 3 views in DMS5:
            //   V_Dataset_Folder_Path, V_Organism_Export, and V_Dataset_Archive_Path
            // Experiment_NEWT_ID comes from the organism for the experiment, and actually comes from field NCBI_Taxonomy_ID in T_Organisms
            //
            sqlStr.Append(" SELECT Dataset, DatasetID, Instrument, InstrumentGroup, ");
            sqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism, Experiment_NEWT_ID, Experiment_NEWT_Name, ");
            sqlStr.Append("        Dataset_Folder_Path, Archive_Folder_Path, RawDataType");
            sqlStr.Append(" FROM V_DMS_Data_Package_Datasets");
            sqlStr.Append(" WHERE Data_Package_ID = " + dataPackageID.ToString());
            sqlStr.Append(" ORDER BY Dataset");

            DataTable resultSet;

            // Get a table to hold the results of the query
            var success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), connectionString, "LoadDataPackageDatasetInfo", RETRY_COUNT, out resultSet);

            if (!success)
            {
                var errorMessage = "LoadDataPackageDatasetInfo; Excessive failures attempting to retrieve data package dataset info from database";
                clsGlobal.LogError(errorMessage);
                resultSet.Dispose();
                return false;
            }

            // Verify at least one row returned
            if (resultSet.Rows.Count < 1)
            {
                // No data was returned
                var warningMessage = "LoadDataPackageDatasetInfo; No datasets were found for data package " + dataPackageID.ToString();
                clsGlobal.LogError(warningMessage);
                return false;
            }

            foreach (DataRow curRow in resultSet.Rows)
            {
                var udtDatasetInfo = ParseDataPackageDatasetInfoRow(curRow);

                if (!dctDataPackageDatasets.ContainsKey(udtDatasetInfo.DatasetID))
                {
                    dctDataPackageDatasets.Add(udtDatasetInfo.DatasetID, udtDatasetInfo);
                }
            }

            resultSet.Dispose();
            return true;

        }

        /// <summary>
        /// Looks up dataset information for the data package associated with this analysis job
        /// </summary>
        /// <param name="dctDataPackageJobs"></param>
        /// <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
        /// <remarks></remarks>
        protected bool LoadDataPackageJobInfo(out Dictionary<int, clsDataPackageJobInfo> dctDataPackageJobs)
        {

            // Gigasax.DMS_Pipeline
            var connectionString = m_mgrParams.GetParam("brokerconnectionstring");

            var dataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1);

            if (dataPackageID < 0)
            {
                dctDataPackageJobs = new Dictionary<int, clsDataPackageJobInfo>();
                return false;
            }

            return LoadDataPackageJobInfo(connectionString, dataPackageID, out dctDataPackageJobs);
        }

        /// <summary>
        /// Looks up job information for a data package
        /// </summary>
        /// <param name="ConnectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
        /// <param name="DataPackageID">Data Package ID</param>
        /// <param name="dctDataPackageJobs">Jobs associated with the given data package</param>
        /// <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
        /// <remarks></remarks>
        public static bool LoadDataPackageJobInfo(string ConnectionString, int DataPackageID, out Dictionary<int, clsDataPackageJobInfo> dctDataPackageJobs)
        {

            const short RETRY_COUNT = 3;

            dctDataPackageJobs = new Dictionary<int, clsDataPackageJobInfo>();

            var sqlStr = new System.Text.StringBuilder();

            // Note that this queries view V_DMS_Data_Package_Aggregation_Jobs in the DMS_Pipeline database
            // That view references   view V_DMS_Data_Package_Aggregation_Jobs in the DMS_Data_Package database
            // The two views have the same name, but some columns differ

            sqlStr.Append(" SELECT Job, Dataset, DatasetID, Instrument, InstrumentGroup, ");
            sqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism, Experiment_NEWT_ID, Experiment_NEWT_Name, ");
            sqlStr.Append("        Tool, ResultType, SettingsFileName, ParameterFileName, ");
            sqlStr.Append("        OrganismDBName, ProteinCollectionList, ProteinOptions,");
            sqlStr.Append("        ServerStoragePath, ArchiveStoragePath, ResultsFolder, DatasetFolder, SharedResultsFolder, RawDataType");
            sqlStr.Append(" FROM V_DMS_Data_Package_Aggregation_Jobs");
            sqlStr.Append(" WHERE Data_Package_ID = " + DataPackageID.ToString());
            sqlStr.Append(" ORDER BY Dataset, Tool");

            DataTable resultSet;

            // Get a table to hold the results of the query
            var success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), ConnectionString, "LoadDataPackageJobInfo", RETRY_COUNT, out resultSet);

            if (!success)
            {
                var errorMessage = "LoadDataPackageJobInfo; Excessive failures attempting to retrieve data package job info from database";
                clsGlobal.LogError(errorMessage);
                resultSet.Dispose();
                return false;
            }

            // Verify at least one row returned
            if (resultSet.Rows.Count < 1)
            {
                // No data was returned
                string warningMessage;

                // If the data package exists and has datasets associated with it, then Log this as a warning but return true
                // Otherwise, log an error and return false

                sqlStr.Clear();
                sqlStr.Append(" SELECT Count(*) AS Datasets");
                sqlStr.Append(" FROM S_V_DMS_Data_Package_Aggregation_Datasets");
                sqlStr.Append(" WHERE Data_Package_ID = " + DataPackageID.ToString());

                // Get a table to hold the results of the query
                success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), ConnectionString, "LoadDataPackageJobInfo", RETRY_COUNT, out resultSet);
                if (success && resultSet.Rows.Count > 0)
                {
                    foreach (DataRow curRow in resultSet.Rows)
                    {
                        var datasetCount = clsGlobal.DbCInt(curRow[0]);

                        if (datasetCount > 0)
                        {
                            warningMessage = "LoadDataPackageJobInfo; No jobs were found for data package " + DataPackageID + ", but it does have " + datasetCount + " dataset";
                            if (datasetCount > 1)
                                warningMessage += "s";
                            clsGlobal.LogWarning(warningMessage);
                            return true;
                        }
                    }
                }

                warningMessage = "LoadDataPackageJobInfo; No jobs were found for data package " + DataPackageID.ToString();
                clsGlobal.LogWarning(warningMessage);
                return false;
            }

            foreach (DataRow curRow in resultSet.Rows)
            {
                var dataPkgJob = ParseDataPackageJobInfoRow(curRow);

                if (!dctDataPackageJobs.ContainsKey(dataPkgJob.Job))
                {
                    dctDataPackageJobs.Add(dataPkgJob.Job, dataPkgJob);
                }
            }

            resultSet.Dispose();

            return true;

        }

        // ReSharper disable once MemberCanBePrivate.Global
        protected void LogDebugMessage(string debugMessage)
        {
            clsGlobal.LogDebug(debugMessage);
        }

        protected static void LogDebugMessage(string debugMessage, IStatusFile statusTools)
        {
            clsGlobal.LogDebug(debugMessage);

            if ((statusTools != null))
            {
                statusTools.CurrentOperation = debugMessage;
                statusTools.UpdateAndWrite(0);
            }
        }

        /// <summary>
        /// Retrieve the information for the specified analysis job
        /// </summary>
        /// <param name="jobNumber">Job number</param>
        /// <param name="jobInfo">Output parameter: Job Info</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>This procedure is used by clsAnalysisResourcesQCART</remarks>
        protected bool LookupJobInfo(int jobNumber, out clsDataPackageJobInfo jobInfo)
        {

            const int RETRY_COUNT = 3;

            var sqlStr = new System.Text.StringBuilder();

            // This query uses view V_Analysis_Job_Export_DataPkg in the DMS5 database
            sqlStr.Append("SELECT Job, Dataset, DatasetID, InstrumentName As Instrument, InstrumentGroup,");
            sqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism, Experiment_NEWT_ID, Experiment_NEWT_Name, ");
            sqlStr.Append("        Tool, ResultType, SettingsFileName, ParameterFileName,");
            sqlStr.Append("        OrganismDBName, ProteinCollectionList, ProteinOptions,");
            sqlStr.Append("        ServerStoragePath, ArchiveStoragePath, ResultsFolder, DatasetFolder, '' AS SharedResultsFolder, RawDataType ");
            sqlStr.Append("FROM V_Analysis_Job_Export_DataPkg ");
            sqlStr.Append("WHERE Job = " + jobNumber);

            DataTable resultSet;
            jobInfo = new clsDataPackageJobInfo(0, string.Empty);

            // Gigasax.DMS5
            var dmsConnectionString = m_mgrParams.GetParam("connectionstring");

            // Get a table to hold the results of the query
            var success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), dmsConnectionString, "LookupJobInfo", RETRY_COUNT, out resultSet);

            if (!success)
            {
                var errorMessage = "LookupJobInfo; Excessive failures attempting to retrieve data package job info from database";
                LogMessage(errorMessage, 0, true);
                resultSet.Dispose();
                return false;
            }

            // Verify at least one row returned
            if (resultSet.Rows.Count < 1)
            {
                // No data was returned
                LogError("Job " + jobNumber + " not found in view V_Analysis_Job_Export_DataPkg");
                return false;
            }

            jobInfo = ParseDataPackageJobInfoRow(resultSet.Rows[0]);

            return true;

        }

        /// <summary>
        /// Estimate the amount of disk space required for the FASTA file associated with this analysis job
        /// </summary>
        /// <param name="proteinCollectionInfo">Collection info object</param>
        /// <returns>Space required, in MB</returns>
        /// <remarks>Uses both m_jobParams and m_mgrParams; returns 0 if a problem (e.g. the legacy fasta file is not listed in V_Organism_DB_File_Export)</remarks>
        public double LookupLegacyDBDiskSpaceRequiredMB(clsProteinCollectionInfo proteinCollectionInfo)
        {

            try
            {
                var dmsConnectionString = m_mgrParams.GetParam("connectionstring");
                if (string.IsNullOrWhiteSpace(dmsConnectionString))
                {
                    LogError("Error in LookupLegacyDBSizeWithIndices: manager parameter connectionstring is not defined");
                    return 0;
                }

                string legacyFastaName;
                if (proteinCollectionInfo.UsingSplitFasta)
                {
                    string errorMessage;
                    legacyFastaName = GetSplitFastaFileName(m_jobParams, out errorMessage);
                }
                else
                {
                    legacyFastaName = proteinCollectionInfo.LegacyFastaName;
                }

                var sqlQuery = "SELECT File_Size_KB FROM V_Organism_DB_File_Export WHERE (FileName = '" + legacyFastaName + "')";

                // Results, as a list of columns (first row only if multiple rows)
                List<string> lstResults;

                var success = clsGlobal.GetQueryResultsTopRow(sqlQuery, dmsConnectionString, out lstResults, "LookupLegacyDBSizeWithIndices");

                if (!success || lstResults == null || lstResults.Count == 0)
                {
                    // Empty query results
                    var statusMessage = "Warning: Could not determine the legacy fasta file's size for job " + m_JobNum + ", file " + legacyFastaName;
                    if (proteinCollectionInfo.UsingSplitFasta)
                    {
                        // Likely the FASTA file has not yet been split
                        LogMessage(statusMessage + "; likely the split fasta file has not yet been created");
                    }
                    else
                    {
                        LogMessage(statusMessage, 0, true);
                    }

                    return 0;
                }

                int fileSizeKB;
                if (!int.TryParse(lstResults.First(), out fileSizeKB))
                {
                    LogMessage("Legacy fasta file size is not numeric, job " + m_JobNum + ", file " + legacyFastaName + ": " + lstResults.First(), 0, true);
                    return 0;
                }

                // Assume that the MSGF+ index files will be 15 times larger than the legacy FASTA file itself
                var fileSizeMB = (fileSizeKB + fileSizeKB * 15) / 1024.0;

                // Pad the expected size by an additional 15%
                return fileSizeMB * 1.15;

            }
            catch (Exception ex)
            {
                LogError("Error in LookupLegacyDBSizeWithIndices", ex);
                return 0;
            }

        }

        /// <summary>
        /// Look for StepParameter section entries in the source file and insert them into the master file
        /// If the master file already has information on the given job step, information for that step is not copied from the source
        /// This is because the master file is assumed to be newer than the source file
        /// </summary>
        /// <param name="sourceJobParamXMLFilePath"></param>
        /// <param name="masterJobParamXMLFilePath"></param>
        /// <returns></returns>
        private bool MergeJobParamXMLStepParameters(string sourceJobParamXMLFilePath, string masterJobParamXMLFilePath)
        {

            try
            {
                var sourceDoc = XDocument.Load(sourceJobParamXMLFilePath);

                var masterDoc = XDocument.Load(masterJobParamXMLFilePath);

                // Keys in the stepParamsSections dictionaries are step numbers
                // Values are the XElement node with the step parameters for the given step

                var stepParamSectionsSource = GetStepParametersSections(sourceDoc);

                var stepParamSectionsMaster = GetStepParametersSections(masterDoc);

                var stepParamSectionsMerged = new Dictionary<int, XElement>();

                // Initialize stepParamSectionsMerged using stepParamSectionsMaster
                foreach (var section in stepParamSectionsMaster)
                {

                    stepParamSectionsMerged.Add(section.Key, section.Value);
                }

                // Add missing steps to stepParamSectionsMerged
                foreach (var section in stepParamSectionsSource)
                {
                    if (!stepParamSectionsMerged.ContainsKey(section.Key))
                    {
                        stepParamSectionsMerged.Add(section.Key, section.Value);
                    }
                }

                // Remove the StepParameter items from the master, then add in the merged items
                foreach (var section in stepParamSectionsMaster)
                {
                    section.Value.Remove();
                }

                var sectionsNode = masterDoc.Elements("sections").First();

                foreach (var section in from item in stepParamSectionsMerged orderby item.Key select item.Value)
                {
                    sectionsNode.Add(section);
                }

                var settings = new XmlWriterSettings
                {
                    Indent = true,
                    IndentChars = "  ",
                    OmitXmlDeclaration = true
                };

                using (var fileWriter = new StreamWriter(new FileStream(masterJobParamXMLFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {

                    using (var writer = XmlWriter.Create(fileWriter, settings))
                    {
                        masterDoc.Save(writer);
                    }

                }

                return true;

            }
            catch (Exception ex)
            {
                LogError("Error in MergeJobParamXMLStepParameters", ex);
                return false;
            }

        }

        /// <summary>
        /// Moves a file from one folder to another folder
        /// </summary>
        /// <param name="diSourceFolder"></param>
        /// <param name="diTargetFolder"></param>
        /// <param name="sourceFileName"></param>
        /// <remarks></remarks>
        protected void MoveFileToFolder(DirectoryInfo diSourceFolder, DirectoryInfo diTargetFolder, string sourceFileName)
        {
            var fiSourceFile = new FileInfo(Path.Combine(diSourceFolder.FullName, sourceFileName));
            var targetFilePath = Path.Combine(diTargetFolder.FullName, sourceFileName);
            fiSourceFile.MoveTo(targetFilePath);
        }

        /// <summary>
        /// Override current job information, including dataset name, dataset ID, storage paths, Organism Name, Protein Collection, and protein options
        /// </summary>
        /// <param name="dataPkgJob"></param>
        /// <returns></returns>
        /// <remarks> Does not override the job number</remarks>
        public bool OverrideCurrentDatasetAndJobInfo(clsDataPackageJobInfo dataPkgJob)
        {

            var blnAggregationJob = false;

            if (string.IsNullOrEmpty(dataPkgJob.Dataset))
            {
                LogError("OverrideCurrentDatasetAndJobInfo; Column 'Dataset' not defined for job " + dataPkgJob.Job + " in the data package");
                return false;
            }

            if (clsGlobal.IsMatch(dataPkgJob.Dataset, "Aggregation"))
            {
                blnAggregationJob = true;
            }

            if (!blnAggregationJob)
            {
                // Update job params to have the details for the current dataset
                // This is required so that we can use FindDataFile to find the desired files
                if (string.IsNullOrEmpty(dataPkgJob.ServerStoragePath))
                {
                    LogError("OverrideCurrentDatasetAndJobInfo; Column 'ServerStoragePath' not defined for job " + dataPkgJob.Job + " in the data package");
                    return false;
                }

                if (string.IsNullOrEmpty(dataPkgJob.ArchiveStoragePath))
                {
                    LogError("OverrideCurrentDatasetAndJobInfo; Column 'ArchiveStoragePath' not defined for job " + dataPkgJob.Job + " in the data package");
                    return false;
                }

                if (string.IsNullOrEmpty(dataPkgJob.ResultsFolderName))
                {
                    LogError("OverrideCurrentDatasetAndJobInfo; Column 'ResultsFolderName' not defined for job " + dataPkgJob.Job + " in the data package");
                    return false;
                }

                if (string.IsNullOrEmpty(dataPkgJob.DatasetFolderName))
                {
                    LogError("OverrideCurrentDatasetAndJobInfo; Column 'DatasetFolderName' not defined for job " + dataPkgJob.Job + " in the data package");
                    return false;
                }
            }

            m_jobParams.AddDatasetInfo(dataPkgJob.Dataset, dataPkgJob.DatasetID);
            DatasetName = string.Copy(dataPkgJob.Dataset);

            m_jobParams.AddAdditionalParameter("JobParameters", "DatasetNum", dataPkgJob.Dataset);
            m_jobParams.AddAdditionalParameter("JobParameters", "DatasetID", dataPkgJob.DatasetID.ToString());

            m_jobParams.AddAdditionalParameter("JobParameters", "Instrument", dataPkgJob.Instrument);
            m_jobParams.AddAdditionalParameter("JobParameters", "InstrumentGroup", dataPkgJob.InstrumentGroup);

            m_jobParams.AddAdditionalParameter("JobParameters", "ToolName", dataPkgJob.Tool);
            m_jobParams.AddAdditionalParameter("JobParameters", "ResultType", dataPkgJob.ResultType);
            m_jobParams.AddAdditionalParameter("JobParameters", "SettingsFileName", dataPkgJob.SettingsFileName);

            m_jobParams.AddAdditionalParameter("PeptideSearch", "ParmFileName", dataPkgJob.ParameterFileName);

            if (string.IsNullOrWhiteSpace(dataPkgJob.OrganismDBName))
            {
                m_jobParams.AddAdditionalParameter("PeptideSearch", JOB_PARAM_GENERATED_FASTA_NAME, "na");
            }
            else
            {
                m_jobParams.AddAdditionalParameter("PeptideSearch", JOB_PARAM_GENERATED_FASTA_NAME, dataPkgJob.OrganismDBName);
            }

            if (string.IsNullOrWhiteSpace(dataPkgJob.ProteinCollectionList) || dataPkgJob.ProteinCollectionList == "na")
            {
                m_jobParams.AddAdditionalParameter("PeptideSearch", "legacyFastaFileName", dataPkgJob.OrganismDBName);
            }
            else
            {
                m_jobParams.AddAdditionalParameter("PeptideSearch", "legacyFastaFileName", "na");
            }

            m_jobParams.AddAdditionalParameter("PeptideSearch", "ProteinCollectionList", dataPkgJob.ProteinCollectionList);
            m_jobParams.AddAdditionalParameter("PeptideSearch", "ProteinOptions", dataPkgJob.ProteinOptions);

            m_jobParams.AddAdditionalParameter("JobParameters", "DatasetStoragePath", dataPkgJob.ServerStoragePath);
            m_jobParams.AddAdditionalParameter("JobParameters", "DatasetArchivePath", dataPkgJob.ArchiveStoragePath);
            m_jobParams.AddAdditionalParameter("JobParameters", "inputFolderName", dataPkgJob.ResultsFolderName);
            m_jobParams.AddAdditionalParameter("JobParameters", "DatasetFolderName", dataPkgJob.DatasetFolderName);
            m_jobParams.AddAdditionalParameter("JobParameters", "SharedResultsFolders", dataPkgJob.SharedResultsFolder);
            m_jobParams.AddAdditionalParameter("JobParameters", "RawDataType", dataPkgJob.RawDataType);

            return true;

        }

        private static clsDataPackageDatasetInfo ParseDataPackageDatasetInfoRow(DataRow curRow)
        {

            var datasetName = clsGlobal.DbCStr(curRow["Dataset"]);
            var datasetId = clsGlobal.DbCInt(curRow["DatasetID"]);

            var datasetInfo = new clsDataPackageDatasetInfo(datasetName, datasetId)
            {
                Instrument = clsGlobal.DbCStr(curRow["Instrument"]),
                InstrumentGroup = clsGlobal.DbCStr(curRow["InstrumentGroup"]),
                Experiment = clsGlobal.DbCStr(curRow["Experiment"]),
                Experiment_Reason = clsGlobal.DbCStr(curRow["Experiment_Reason"]),
                Experiment_Comment = clsGlobal.DbCStr(curRow["Experiment_Comment"]),
                Experiment_Organism = clsGlobal.DbCStr(curRow["Organism"]),
                Experiment_NEWT_ID = clsGlobal.DbCInt(curRow["Experiment_NEWT_ID"]),
                Experiment_NEWT_Name = clsGlobal.DbCStr(curRow["Experiment_NEWT_Name"]),
                ServerStoragePath = clsGlobal.DbCStr(curRow["Dataset_Folder_Path"]),
                ArchiveStoragePath = clsGlobal.DbCStr(curRow["Archive_Folder_Path"]),
                RawDataType = clsGlobal.DbCStr(curRow["RawDataType"])
            };

            return datasetInfo;

        }

        private static clsDataPackageJobInfo ParseDataPackageJobInfoRow(DataRow curRow)
        {

            var dataPkgJob = clsGlobal.DbCInt(curRow["Job"]);
            var dataPkgDataset = clsGlobal.DbCStr(curRow["Dataset"]);

            var jobInfo = new clsDataPackageJobInfo(dataPkgJob, dataPkgDataset)
            {
                DatasetID = clsGlobal.DbCInt(curRow["DatasetID"]),
                Instrument = clsGlobal.DbCStr(curRow["Instrument"]),
                InstrumentGroup = clsGlobal.DbCStr(curRow["InstrumentGroup"]),
                Experiment = clsGlobal.DbCStr(curRow["Experiment"]),
                Experiment_Reason = clsGlobal.DbCStr(curRow["Experiment_Reason"]),
                Experiment_Comment = clsGlobal.DbCStr(curRow["Experiment_Comment"]),
                Experiment_Organism = clsGlobal.DbCStr(curRow["Organism"]),
                Experiment_NEWT_ID = clsGlobal.DbCInt(curRow["Experiment_NEWT_ID"]),
                Experiment_NEWT_Name = clsGlobal.DbCStr(curRow["Experiment_NEWT_Name"]),
                Tool = clsGlobal.DbCStr(curRow["Tool"]),
                ResultType = clsGlobal.DbCStr(curRow["ResultType"])
            };

            jobInfo.PeptideHitResultType = clsPHRPReader.GetPeptideHitResultType(jobInfo.ResultType);
            jobInfo.SettingsFileName = clsGlobal.DbCStr(curRow["SettingsFileName"]);
            jobInfo.ParameterFileName = clsGlobal.DbCStr(curRow["ParameterFileName"]);
            jobInfo.OrganismDBName = clsGlobal.DbCStr(curRow["OrganismDBName"]);
            jobInfo.ProteinCollectionList = clsGlobal.DbCStr(curRow["ProteinCollectionList"]);
            jobInfo.ProteinOptions = clsGlobal.DbCStr(curRow["ProteinOptions"]);

            // This will be updated later for SplitFasta jobs (using function LookupJobParametersFromHistory)
            jobInfo.NumberOfClonedSteps = 0;

            if (string.IsNullOrWhiteSpace(jobInfo.ProteinCollectionList) || jobInfo.ProteinCollectionList == "na")
            {
                jobInfo.LegacyFastaFileName = string.Copy(jobInfo.OrganismDBName);
            }
            else
            {
                jobInfo.LegacyFastaFileName = "na";
            }

            jobInfo.ServerStoragePath = clsGlobal.DbCStr(curRow["ServerStoragePath"]);
            jobInfo.ArchiveStoragePath = clsGlobal.DbCStr(curRow["ArchiveStoragePath"]);
            jobInfo.ResultsFolderName = clsGlobal.DbCStr(curRow["ResultsFolder"]);
            jobInfo.DatasetFolderName = clsGlobal.DbCStr(curRow["DatasetFolder"]);
            jobInfo.SharedResultsFolder = clsGlobal.DbCStr(curRow["SharedResultsFolder"]);
            jobInfo.RawDataType = clsGlobal.DbCStr(curRow["RawDataType"]);

            return jobInfo;

        }

        /// <summary>
        /// Download any queued files from MyEMSL
        /// </summary>
        /// <returns></returns>
        public bool ProcessMyEMSLDownloadQueue()
        {
            if (m_MyEMSLUtilities.FilesToDownload.Count > 0)
            {
                if (!m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Download any queued files from MyEMSL
        /// </summary>
        /// <param name="downloadFolderPath"></param>
        /// <param name="folderLayout"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool ProcessMyEMSLDownloadQueue(string downloadFolderPath, Downloader.DownloadFolderLayout folderLayout)
        {
            var success = m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(downloadFolderPath, folderLayout);
            return success;
        }

        /// <summary>
        /// Delete the specified FASTA file and its associated files
        /// </summary>
        /// <param name="diOrgDbFolder"></param>
        /// <param name="fiFileToPurge"></param>
        /// <param name="legacyFastaFileBaseName"></param>
        /// <returns>Number of bytes deleted</returns>
        private long PurgeFastaFiles(DirectoryInfo diOrgDbFolder, FileInfo fiFileToPurge, string legacyFastaFileBaseName)
        {

            var baseName = Path.GetFileNameWithoutExtension(fiFileToPurge.Name);

            if (!string.IsNullOrWhiteSpace(legacyFastaFileBaseName) && baseName.StartsWith(legacyFastaFileBaseName, StringComparison.InvariantCultureIgnoreCase))
            {
                // The current job needs this file; do not delete it
                return 0;
            }

            // Delete all files associated with this fasta file
            var lstFilesToDelete = new List<FileInfo>();
            lstFilesToDelete.AddRange(diOrgDbFolder.GetFiles(baseName + ".*"));

            if (m_DebugLevel >= 1)
            {
                var fileText = string.Format("{0,2} file", lstFilesToDelete.Count);
                if (lstFilesToDelete.Count != 1)
                {
                    fileText += "s";
                }
                LogDebugMessage("Deleting " + fileText + " associated with " + fiFileToPurge.FullName);
            }

            long bytesDeleted = 0;

            try
            {
                foreach (var fiFileToDelete in lstFilesToDelete)
                {
                    var fileSizeBytes = fiFileToDelete.Length;
                    fiFileToDelete.Delete();
                    bytesDeleted += fileSizeBytes;
                }
            }
            catch (Exception ex)
            {
                LogError("Error in PurgeFastaFiles", ex);
            }

            return bytesDeleted;

        }

        /// <summary>
        /// Purges old fasta files (and related suffix array files) from localOrgDbFolder
        /// </summary>
        /// <param name="localOrgDbFolder"></param>
        /// <param name="freeSpaceThresholdPercent">Value between 1 and 50</param>
        /// <param name="requiredFreeSpaceMB">If greater than 0, the free space that we anticipate will be needed for the given fasta file</param>
        /// <param name="legacyFastaFileBaseName">
        /// Legacy fasta file name (without .fasta)
        /// For split fasta jobs, should not include the splitcount and segment number, e.g. should not include _25x_07 or _25x_08
        /// </param>
        /// <remarks></remarks>
        protected void PurgeFastaFilesIfLowFreeSpace(string localOrgDbFolder, int freeSpaceThresholdPercent, double requiredFreeSpaceMB, string legacyFastaFileBaseName)
        {
            if (freeSpaceThresholdPercent < 1)
                freeSpaceThresholdPercent = 1;
            if (freeSpaceThresholdPercent > 50)
                freeSpaceThresholdPercent = 50;

            try
            {
                var diOrgDbFolder = new DirectoryInfo(localOrgDbFolder);
                if (diOrgDbFolder.FullName.Length <= 2)
                {
                    LogMessage("Warning: Org DB folder length is less than 3 characters; this is unexpected: " + diOrgDbFolder.FullName);
                    return;
                }

                // Look for file MaxDirSize.txt which defines the maximum space that the files can use
                var fiMaxDirSize = new FileInfo(Path.Combine(diOrgDbFolder.FullName, "MaxDirSize.txt"));

                var driveLetter = diOrgDbFolder.FullName.Substring(0, 2);

                if (!driveLetter.EndsWith(":"))
                {
                    // The folder is not local to this computer
                    if (!fiMaxDirSize.Exists)
                    {
                        LogError("Warning: Orb DB folder path does not have a colon and could not find file " + fiMaxDirSize.Name + "; cannot manage drive space usage: " + diOrgDbFolder.FullName);
                        return;
                    }

                    // MaxDirSize.txt file found; delete the older FASTA files to free up space
                    PurgeFastaFilesUsingSpaceUsedThreshold(fiMaxDirSize, legacyFastaFileBaseName);
                    return;
                }

                var localDriveInfo = new DriveInfo(driveLetter);
                var percentFreeSpaceAtStart = localDriveInfo.AvailableFreeSpace / (double)localDriveInfo.TotalSize * 100;

                if ((percentFreeSpaceAtStart >= freeSpaceThresholdPercent))
                {
                    if (m_DebugLevel >= 2)
                    {
                        var freeSpaceGB = clsGlobal.BytesToGB(localDriveInfo.AvailableFreeSpace);
                        LogMessage(string.Format("Free space on {0} ({1:F1} GB) is over {2}% of the total space; purge not required", localDriveInfo.Name, freeSpaceGB, freeSpaceThresholdPercent));
                    }
                }
                else
                {
                    PurgeFastaFilesUsingFreeSpaceThreshold(localDriveInfo, diOrgDbFolder, legacyFastaFileBaseName, freeSpaceThresholdPercent, requiredFreeSpaceMB, percentFreeSpaceAtStart);
                }

                if (fiMaxDirSize.Exists)
                {
                    // MaxDirSize.txt file exists; possibly delete additional FASTA files to free up space
                    PurgeFastaFilesUsingSpaceUsedThreshold(fiMaxDirSize, legacyFastaFileBaseName);
                }

            }
            catch (Exception ex)
            {
                LogError("Error in PurgeFastaFilesIfLowFreeSpace", ex);
            }

        }

        private void PurgeFastaFilesUsingFreeSpaceThreshold(
            DriveInfo localDriveInfo,
            DirectoryInfo diOrgDbFolder,
            string legacyFastaFileBaseName,
            int freeSpaceThresholdPercent,
            double requiredFreeSpaceMB,
            double percentFreeSpaceAtStart)
        {
            var freeSpaceGB = clsGlobal.BytesToGB(localDriveInfo.AvailableFreeSpace);

            var logInfoMessages = m_DebugLevel >= 1 && freeSpaceGB < 100 || m_DebugLevel >= 2 && freeSpaceGB < 250 || m_DebugLevel >= 3;

            if (logInfoMessages)
            {
                LogMessage(string.Format("Free space on {0} ({1:F1} GB) is {2:F1}% of the total space; purge required since less than threshold of {3}%",
                    localDriveInfo.Name, freeSpaceGB, percentFreeSpaceAtStart, freeSpaceThresholdPercent));
            }

            // Obtain a dictionary of FASTA files where Keys are FileInfo and values are last usage date
            var dctFastaFiles = GetFastaFilesByLastUse(diOrgDbFolder);

            var lstFastaFilesByLastUse = (from item in dctFastaFiles orderby item.Value select item.Key);
            long totalBytesPurged = 0;

            foreach (var fiFileToPurge in lstFastaFilesByLastUse)
            {
                // Abort this process if the LastUsed date of this file is less than 5 days old
                DateTime dtLastUsed;
                if (dctFastaFiles.TryGetValue(fiFileToPurge, out dtLastUsed))
                {
                    if (DateTime.UtcNow.Subtract(dtLastUsed).TotalDays < 5)
                    {
                        if (logInfoMessages)
                        {
                            LogMessage("All fasta files in " + diOrgDbFolder.FullName + " are less than 5 days old; " +
                                "will not purge any more files to free disk space");
                        }
                        break;
                    }
                }

                // Delete all files associated with this fasta file
                // However, do not delete it if the name starts with legacyFastaFileBaseName
                var bytesDeleted = PurgeFastaFiles(diOrgDbFolder, fiFileToPurge, legacyFastaFileBaseName);
                totalBytesPurged += bytesDeleted;

                // Re-check the disk free space
                var percentFreeSpace = localDriveInfo.AvailableFreeSpace / (double)localDriveInfo.TotalSize * 100;
                var updatedFreeSpaceGB = clsGlobal.BytesToGB(localDriveInfo.AvailableFreeSpace);

                if (requiredFreeSpaceMB > 0 && updatedFreeSpaceGB * 1024.0 < requiredFreeSpaceMB)
                {
                    // Required free space is known, and we're not yet there
                    // Keep deleting files
                    if (m_DebugLevel >= 2)
                    {
                        LogDebugMessage(string.Format("Free space on {0} ({1:F1} GB) is now {2:F1}% of the total space", localDriveInfo.Name, updatedFreeSpaceGB, percentFreeSpace));
                    }
                }
                else
                {
                    // Either required free space is not known, or we have more than enough free space

                    if ((percentFreeSpace >= freeSpaceThresholdPercent))
                    {
                        // Target threshold reached
                        if (m_DebugLevel >= 1)
                        {
                            LogMessage(string.Format("Free space on {0} ({1:F1} GB) is now over {2}% of the total space; " + "deleted {3:F1} GB of cached files",
                                localDriveInfo.Name, updatedFreeSpaceGB, freeSpaceThresholdPercent, clsGlobal.BytesToGB(totalBytesPurged)));
                        }
                        break;
                    }

                    if (m_DebugLevel >= 2)
                    {
                        // Keep deleting until we reach the target threshold for free space
                        LogDebugMessage(string.Format("Free space on {0} ({1:F1} GB) is now {2:F1}% of the total space", localDriveInfo.Name, updatedFreeSpaceGB, percentFreeSpace));
                    }
                }
            }

            // We have deleted all of the files that can be deleted
            var finalFreeSpaceGB = clsGlobal.BytesToGB(localDriveInfo.AvailableFreeSpace);

            if (requiredFreeSpaceMB > 0 && finalFreeSpaceGB * 1024.0 < requiredFreeSpaceMB)
            {
                LogMessage(string.Format("Warning: unable to delete enough files to free up the required space on {0} " +
                                         "({1:F1} GB vs. {2:F1} GB); " + "deleted {3:F1} GB of cached files",
                    localDriveInfo.Name, finalFreeSpaceGB, requiredFreeSpaceMB / 1024.0, clsGlobal.BytesToGB(totalBytesPurged)));
            }

        }

        /// <summary>
        /// Use the space usage defined in MaxDirSize.txt to decide if any FASTA files need to be deleted
        /// </summary>
        /// <param name="fiMaxDirSize">MaxDirSize.txt file in the local organism DB folder</param>
        /// <param name="legacyFastaFileBaseName">Base FASTA file name for the current analysis job</param>
        private void PurgeFastaFilesUsingSpaceUsedThreshold(FileInfo fiMaxDirSize, string legacyFastaFileBaseName)
        {
            try
            {
                var diOrgDbFolder = fiMaxDirSize.Directory;
                var errorSuffix = "; cannot manage drive space usage: " + diOrgDbFolder.FullName;
                var maxSizeGB = 0;

                using (var reader = new StreamReader(new FileStream(fiMaxDirSize.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrEmpty(dataLine) || dataLine.StartsWith("#"))
                        {
                            continue;
                        }

                        var lineParts = dataLine.Split('=');
                        if (lineParts.Length < 2)
                        {
                            continue;
                        }

                        if (string.Equals(lineParts[0], "MaxSizeGB", StringComparison.InvariantCultureIgnoreCase))
                        {
                            if (!int.TryParse(lineParts[1], out maxSizeGB))
                            {
                                LogError("MaxSizeGB line does not contain an integer in " + fiMaxDirSize.FullName + errorSuffix);
                                return;
                            }
                            break;
                        }

                    }
                }

                if (maxSizeGB == 0)
                {
                    LogError("MaxSizeGB line not found in " + fiMaxDirSize.FullName + errorSuffix);
                    return;
                }

                long spaceUsageBytes = 0;

                foreach (var fiFile in diOrgDbFolder.GetFiles("*", SearchOption.TopDirectoryOnly))
                {
                    spaceUsageBytes += fiFile.Length;
                }

                var spaceUsageGB = clsGlobal.BytesToGB(spaceUsageBytes);
                if (spaceUsageGB <= maxSizeGB)
                {
                    // Space usage is under the threshold
                    var statusMessage = string.Format("Space usage in {0} is {1:F1} GB, which is below the threshold of {2} GB; nothing to purge", diOrgDbFolder.FullName, spaceUsageGB, maxSizeGB);
                    LogMessage(statusMessage, 3);
                    return;
                }

                // Space usage is too high; need to purge some files
                // Obtain a dictionary of FASTA files where Keys are FileInfo and values are last usage date
                var dctFastaFiles = GetFastaFilesByLastUse(diOrgDbFolder);

                var lstFastaFilesByLastUse = from item in dctFastaFiles orderby item.Value select item.Key;

                var bytesToPurge = (long)(spaceUsageBytes - maxSizeGB * 1024.0 * 1024 * 1024);
                long totalBytesPurged = 0;

                foreach (var fiFileToPurge in lstFastaFilesByLastUse)
                {
                    // Abort this process if the LastUsed date of this file is less than 5 days old
                    DateTime dtLastUsed;
                    if (dctFastaFiles.TryGetValue(fiFileToPurge, out dtLastUsed))
                    {
                        if (DateTime.UtcNow.Subtract(dtLastUsed).TotalDays < 5)
                        {
                            LogMessage("All fasta files in " + diOrgDbFolder.FullName + " are less than 5 days old; " +
                                "will not purge any more files to free disk space");
                            break;
                        }
                    }

                    // Delete all files associated with this fasta file
                    // However, do not delete it if the name starts with legacyFastaFileBaseName
                    var bytesDeleted = PurgeFastaFiles(diOrgDbFolder, fiFileToPurge, legacyFastaFileBaseName);
                    totalBytesPurged += bytesDeleted;

                    if (totalBytesPurged < bytesToPurge)
                    {
                        // Keep deleting files
                        if (m_DebugLevel >= 2)
                        {
                            LogDebugMessage(string.Format("Purging FASTA files: {0:F1} / {1:F1} MB deleted",
                                clsGlobal.BytesToMB(totalBytesPurged), clsGlobal.BytesToMB(bytesToPurge)));
                        }
                    }
                    else
                    {
                        // Enough files have been deleted
                        LogMessage(string.Format("Space usage in {0} is now below {1} GB; deleted {2:F1} GB of cached files",
                            diOrgDbFolder.FullName, maxSizeGB, clsGlobal.BytesToGB(totalBytesPurged)));
                        return;
                    }
                }

                if (totalBytesPurged < bytesToPurge)
                {
                    LogMessage(string.Format("Warning: unable to delete enough files to lower the space usage in {0} to below {1} GB; " +
                        "deleted {2:F1} GB of cached files", diOrgDbFolder.FullName, maxSizeGB, clsGlobal.BytesToGB(totalBytesPurged)));
                }

            }
            catch (Exception ex)
            {
                LogError("Error in PurgeFastaFilesUsingSpaceUsedThreshold", ex);
            }

        }

        /// <summary>
        /// Looks for the specified file in the given folder
        /// If present, returns the full path to the file
        /// If not present, looks for a file named FileName_StoragePathInfo.txt; if that file is found, opens the file and reads the path
        /// If the file isn't found (and the _StoragePathInfo.txt file isn't present), then returns an empty string
        /// </summary>
        /// <param name="FolderPath">The folder to look in</param>
        /// <param name="FileName">The file name to find</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ResolveStoragePath(string FolderPath, string FileName)
        {

            var strPhysicalFilePath = string.Empty;

            var strFilePath = Path.Combine(FolderPath, FileName);

            if (File.Exists(strFilePath))
            {
                // The desired file is located in folder FolderPath
                strPhysicalFilePath = strFilePath;
            }
            else
            {
                // The desired file was not found
                strFilePath += STORAGE_PATH_INFO_FILE_SUFFIX;

                if (File.Exists(strFilePath))
                {
                    // The _StoragePathInfo.txt file is present
                    // Open that file to read the file path on the first line of the file

                    using (var srInFile = new StreamReader(new FileStream(strFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        var strLineIn = srInFile.ReadLine();
                        strPhysicalFilePath = strLineIn;
                    }

                }
            }

            return strPhysicalFilePath;

        }

        /// <summary>
        /// Looks for the STORAGE_PATH_INFO_FILE_SUFFIX file in the working folder
        /// If present, looks for a file named _StoragePathInfo.txt; if that file is found, opens the file and reads the path
        /// If the file named _StoragePathInfo.txt isn't found, then looks for a ser file in the specified folder
        /// If found, returns the path to the ser file
        /// If not found, then looks for a 0.ser folder in the specified folder
        /// If found, returns the path to the 0.ser folder
        /// Otherwise, returns an empty string
        /// </summary>
        /// <param name="FolderPath">The folder to look in</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ResolveSerStoragePath(string FolderPath)
        {
            string strPhysicalFilePath;

            var strFilePath = Path.Combine(FolderPath, STORAGE_PATH_INFO_FILE_SUFFIX);

            if (File.Exists(strFilePath))
            {
                // The desired file is located in folder FolderPath
                // The _StoragePathInfo.txt file is present
                // Open that file to read the file path on the first line of the file

                using (var srInFile = new StreamReader(new FileStream(strFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    var strLineIn = srInFile.ReadLine();
                    strPhysicalFilePath = strLineIn;

                }
            }
            else
            {
                // The desired file was not found

                // Look for a ser file in the dataset folder
                strPhysicalFilePath = Path.Combine(FolderPath, BRUKER_SER_FILE);
                var fiFile = new FileInfo(strPhysicalFilePath);

                if (!fiFile.Exists)
                {
                    // See if a folder named 0.ser exists in FolderPath
                    strPhysicalFilePath = Path.Combine(FolderPath, BRUKER_ZERO_SER_FOLDER);
                    var diFolder = new DirectoryInfo(strPhysicalFilePath);
                    if (!diFolder.Exists)
                    {
                        strPhysicalFilePath = string.Empty;
                    }
                }

            }

            return strPhysicalFilePath;

        }

        /// <summary>
        /// Retrieve the files specified by the file processing options parameter
        /// </summary>
        /// <param name="fileSpecList">
        /// File processing options, examples:
        /// sequest:_syn.txt:nocopy,sequest:_fht.txt:nocopy,sequest:_dta.zip:nocopy,sequest:_syn_ModSummary.txt:nocopy,masic_finnigan:_ScanStatsEx.txt:nocopy
        /// sequest:_syn.txt,sequest:_syn_MSGF.txt,sequest:_fht.txt,sequest:_fht_MSGF.txt,sequest:_dta.zip,sequest:_syn_ModSummary.txt
        /// MSGFPlus:_msgfplus_syn.txt,MSGFPlus:_msgfplus_fht.txt,MSGFPlus:_dta.zip,MSGFPlus:_syn_ModSummary.txt,masic_finnigan:_ScanStatsEx.txt,masic_finnigan:_ReporterIons.txt:copy
        /// MSGFPlus:_msgfplus_syn.txt,MSGFPlus:_msgfplus_syn_ModSummary.txt,MSGFPlus:_dta.zip
        /// </param>
        /// <param name="fileRetrievalMode">Used by plugins to indicate the types of files that are required (in case fileSpecList is not configured correctly for a given data package job)</param>
        /// <param name="dctDataPackageJobs"></param>
        /// <returns>True if success, false if a problem</returns>
        /// <remarks>
        /// This function is used by plugins PhosphoFDRAggregator and PRIDEMzXML
        /// However, PrideMzXML is dormant as of September 2013
        /// </remarks>
        protected bool RetrieveAggregateFiles(List<string> fileSpecList, DataPackageFileRetrievalModeConstants fileRetrievalMode,
            out Dictionary<int, clsDataPackageJobInfo> dctDataPackageJobs)
        {

            bool blnSuccess;

            try
            {
                if (!LoadDataPackageJobInfo(out dctDataPackageJobs))
                {
                    m_message = "Error looking up datasets and jobs using LoadDataPackageJobInfo";
                    dctDataPackageJobs = null;
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError("RetrieveAggregateFiles; Exception calling LoadDataPackageJobInfo", ex);
                dctDataPackageJobs = null;
                return false;
            }

            try
            {
                var diWorkingDirectory = new DirectoryInfo(m_WorkingDir);

                // Cache the current dataset and job info
                var currentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

                foreach (var dataPkgJob in dctDataPackageJobs)
                {
                    if (!OverrideCurrentDatasetAndJobInfo(dataPkgJob.Value))
                    {
                        return false;
                    }

                    // See if this job matches any of the entries in fileSpecList
                    var fileSpecListCurrent = new List<string>();

                    foreach (var fileSpec in fileSpecList)
                    {
                        var fileSpecTerms = fileSpec.Trim().Split(':').ToList();
                        if (dataPkgJob.Value.Tool.StartsWith(fileSpecTerms[0].Trim(), StringComparison.InvariantCultureIgnoreCase))
                        {
                            fileSpecListCurrent = fileSpecList;
                            break;
                        }
                    }

                    if (fileSpecListCurrent.Count == 0)
                    {
                        switch (fileRetrievalMode)
                        {
                            case DataPackageFileRetrievalModeConstants.Ascore:

                                if (dataPkgJob.Value.Tool.StartsWith("msgf", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    // MSGF+
                                    fileSpecListCurrent = new List<string> {
                                        "MSGFPlus:_msgfplus_syn.txt",
                                        "MSGFPlus:_msgfplus_syn_ModSummary.txt",
                                        "MSGFPlus:_dta.zip"
                                    };

                                }

                                if (dataPkgJob.Value.Tool.StartsWith("sequest", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    // Sequest
                                    fileSpecListCurrent = new List<string> {
                                        "sequest:_syn.txt",
                                        "sequest:_syn_MSGF.txt",
                                        "sequest:_syn_ModSummary.txt",
                                        "sequest:_dta.zip"
                                    };

                                }

                                if (dataPkgJob.Value.Tool.StartsWith("xtandem", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    // XTandem
                                    fileSpecListCurrent = new List<string> {
                                        "xtandem:_xt_syn.txt",
                                        "xtandem:_xt_syn_ModSummary.txt",
                                        "xtandem:_dta.zip"
                                    };

                                }

                                break;
                        }
                    }

                    if (fileSpecListCurrent.Count == 0)
                    {
                        continue;
                    }

                    var spectraFileKey = "Job" + dataPkgJob.Key + DATA_PACKAGE_SPECTRA_FILE_SUFFIX;

                    foreach (var fileSpec in fileSpecListCurrent)
                    {
                        var fileSpecTerms = fileSpec.Trim().Split(':').ToList();
                        var sourceFileName = dataPkgJob.Value.Dataset + fileSpecTerms[1].Trim();
                        var sourceFolderPath = "??";

                        var saveMode = "nocopy";
                        if (fileSpecTerms.Count > 2)
                        {
                            saveMode = fileSpecTerms[2].Trim();
                        }

                        try
                        {
                            if (!dataPkgJob.Value.Tool.StartsWith(fileSpecTerms[0].Trim(), StringComparison.InvariantCultureIgnoreCase))
                            {
                                continue;
                            }

                            // To avoid collisions, files for this job will be placed in a subfolder based on the Job number
                            var diTargetFolder = new DirectoryInfo(Path.Combine(m_WorkingDir, "Job" + dataPkgJob.Key));
                            if (!diTargetFolder.Exists)
                                diTargetFolder.Create();

                            if (sourceFileName.EndsWith("_dta.zip", StringComparison.InvariantCultureIgnoreCase) &&
                                dataPkgJob.Value.Tool.EndsWith("_mzml", StringComparison.InvariantCultureIgnoreCase))
                            {
                                // This is a .mzML job; it is not going to have a _dta.zip file
                                // Setting sourceFolderPath to an empty string so that GetMzMLFile will get called below
                                sourceFolderPath = string.Empty;
                            }
                            else
                            {
                                sourceFolderPath = m_FileSearch.FindDataFile(sourceFileName);

                                if (string.IsNullOrEmpty(sourceFolderPath))
                                {
                                    // Source file not found

                                    var alternateSourceFileName = string.Empty;

                                    if (sourceFileName.ToLower().Contains("_msgfdb"))
                                    {
                                        // Auto-look for the _msgfplus version of this file
                                        alternateSourceFileName = clsGlobal.ReplaceIgnoreCase(sourceFileName, "_msgfdb", "_msgfplus");
                                    }
                                    else if (sourceFileName.ToLower().Contains("_msgfplus"))
                                    {
                                        // Auto-look for the _msgfdb version of this file
                                        alternateSourceFileName = clsGlobal.ReplaceIgnoreCase(sourceFileName, "_msgfplus", "_msgfdb");
                                    }

                                    if (!string.IsNullOrEmpty(alternateSourceFileName))
                                    {
                                        sourceFolderPath = m_FileSearch.FindDataFile(alternateSourceFileName);
                                        if (!string.IsNullOrEmpty(sourceFolderPath))
                                        {
                                            sourceFileName = alternateSourceFileName;
                                        }
                                    }
                                }

                            }

                            if (string.IsNullOrEmpty(sourceFolderPath))
                            {
                                if (sourceFileName.EndsWith("_dta.zip", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    // Look for a mzML.gz file instead

                                    string errorMessage;
                                    bool fileMissingFromCache;

                                    var success = m_FileSearch.RetrieveCachedMSXMLFile(DOT_MZML_EXTENSION, false,
                                        out errorMessage, out fileMissingFromCache);

                                    if (!success)
                                    {
                                        if (string.IsNullOrWhiteSpace(errorMessage))
                                        {
                                            errorMessage = "Unknown error looking for the .mzML file for " + dataPkgJob.Value.Dataset + ", job " + dataPkgJob.Key;
                                        }

                                        LogError(errorMessage);
                                        return false;
                                    }

                                    sourceFileName = dataPkgJob.Value.Dataset + DOT_MZML_EXTENSION + DOT_GZ_EXTENSION;
                                    m_jobParams.AddAdditionalParameter("DataPackageMetadata", spectraFileKey, sourceFileName);
                                    m_jobParams.AddResultFileExtensionToSkip(DOT_GZ_EXTENSION);

                                    MoveFileToFolder(diWorkingDirectory, diTargetFolder, sourceFileName);

                                    if (m_DebugLevel >= 1)
                                    {
                                        LogMessage("Retrieved the .mzML file for " + dataPkgJob.Value.Dataset + ", job " + dataPkgJob.Key);
                                    }

                                    continue;
                                }

                                m_message = "Could not find a valid folder with file " + sourceFileName + " for job " + dataPkgJob.Key;
                                if (m_DebugLevel >= 1)
                                {
                                    LogMessage(m_message, 0, true);
                                }
                                return false;
                            }

                            if (!m_FileCopyUtilities.CopyFileToWorkDir(sourceFileName, sourceFolderPath, m_WorkingDir, clsLogTools.LogLevels.ERROR))
                            {
                                m_message = "CopyFileToWorkDir returned False for " + sourceFileName + " using folder " + sourceFolderPath + " for job " + dataPkgJob.Key;
                                if (m_DebugLevel >= 1)
                                {
                                    LogMessage(m_message, 0, true);
                                }
                                return false;
                            }

                            if (sourceFileName.EndsWith("_dta.zip", StringComparison.InvariantCultureIgnoreCase))
                            {
                                m_jobParams.AddAdditionalParameter("DataPackageMetadata", spectraFileKey, sourceFileName);
                            }

                            if (saveMode.ToLower() != "copy")
                            {
                                m_jobParams.AddResultFileToSkip(sourceFileName);
                            }

                            MoveFileToFolder(diWorkingDirectory, diTargetFolder, sourceFileName);

                            if (m_DebugLevel >= 1)
                            {
                                LogMessage("Copied " + sourceFileName + " from folder " + sourceFolderPath);
                            }

                        }
                        catch (Exception ex)
                        {
                            LogError("RetrieveAggregateFiles; Exception during copy of file: " + sourceFileName + " from folder " + sourceFolderPath + " for job " + dataPkgJob.Key, ex);
                            return false;

                        }

                    }
                }

                if (!m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    return false;
                }

                // Restore the dataset and job info for this aggregation job
                OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo);

                blnSuccess = true;

            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveAggregateFiles", ex);
                blnSuccess = false;
            }

            return blnSuccess;

        }

        /// <summary>
        /// Retrieves the PHRP files for the PeptideHit jobs defined for the data package associated with this aggregation job
        /// Also creates a batch file that can be manually run to retrieve the instrument data files
        /// </summary>
        /// <param name="udtOptions">File retrieval options</param>
        /// <param name="lstDataPackagePeptideHitJobs">Job info for the peptide_hit jobs associated with this data package (output parameter)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool RetrieveDataPackagePeptideHitJobPHRPFiles(
            clsDataPackageFileHandler.udtDataPackageRetrievalOptionsType udtOptions,
            out List<clsDataPackageJobInfo> lstDataPackagePeptideHitJobs)
        {

            const float progressPercentAtStart = 0;
            const float progressPercentAtFinish = 20;
            return RetrieveDataPackagePeptideHitJobPHRPFiles(udtOptions, out lstDataPackagePeptideHitJobs, progressPercentAtStart, progressPercentAtFinish);
        }

        /// <summary>
        /// Retrieves the PHRP files for the PeptideHit jobs defined for the data package associated with this aggregation job
        /// Also creates a batch file that can be manually run to retrieve the instrument data files
        /// </summary>
        /// <param name="udtOptions">File retrieval options</param>
        /// <param name="lstDataPackagePeptideHitJobs">Output parameter: Job info for the peptide_hit jobs associated with this data package (output parameter)</param>
        /// <param name="progressPercentAtStart">Percent complete value to use for computing incremental progress</param>
        /// <param name="progressPercentAtFinish">Percent complete value to use for computing incremental progress</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool RetrieveDataPackagePeptideHitJobPHRPFiles(
            clsDataPackageFileHandler.udtDataPackageRetrievalOptionsType udtOptions,
            out List<clsDataPackageJobInfo> lstDataPackagePeptideHitJobs,
            float progressPercentAtStart,
            float progressPercentAtFinish)
        {

            // Gigasax.DMS_Pipeline
            var connectionString = m_mgrParams.GetParam("brokerconnectionstring");

            var dataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1);

            var dataPackageFileHander = new clsDataPackageFileHandler(connectionString, dataPackageID, this);
            RegisterEvents(dataPackageFileHander);

            var blnSuccess = dataPackageFileHander.RetrieveDataPackagePeptideHitJobPHRPFiles(udtOptions, out lstDataPackagePeptideHitJobs, progressPercentAtStart, progressPercentAtFinish);

            return blnSuccess;

        }

        /// <summary>
        /// Create a fasta file for Sequest, X!Tandem, Inspect, or MSGFPlus analysis
        /// </summary>
        /// <param name="LocalOrgDBFolder">Folder on analysis machine where fasta files are stored</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Stores the name of the FASTA file as a new job parameter named "generatedFastaName" in section "PeptideSearch"</remarks>
        protected bool RetrieveOrgDB(string LocalOrgDBFolder)
        {

            Console.WriteLine();
            if (m_DebugLevel >= 3)
            {
                LogMessage("Obtaining org db file");
            }

            try
            {
                var proteinCollectionInfo = new clsProteinCollectionInfo(m_jobParams);

                double requiredFreeSpaceMB = 0;

                if (proteinCollectionInfo.UsingLegacyFasta)
                {
                    // Estimate the drive space required to download the fasta file and its associated MSGF+ the index files
                    requiredFreeSpaceMB = LookupLegacyDBDiskSpaceRequiredMB(proteinCollectionInfo);
                }

                // Delete old fasta files and suffix array files if getting low on disk space
                // Do not delete any files related to the current Legacy Fasta file (if defined)

                const int freeSpaceThresholdPercent = 20;

                var legacyFastaFileBaseName = string.Empty;

                if (proteinCollectionInfo.UsingLegacyFasta && !string.IsNullOrWhiteSpace(proteinCollectionInfo.LegacyFastaName) &&
                    proteinCollectionInfo.LegacyFastaName.ToLower() != "na")
                {
                    legacyFastaFileBaseName = Path.GetFileNameWithoutExtension(proteinCollectionInfo.LegacyFastaName);
                }

                PurgeFastaFilesIfLowFreeSpace(LocalOrgDBFolder, freeSpaceThresholdPercent, requiredFreeSpaceMB, legacyFastaFileBaseName);

                // Make a new fasta file from scratch
                if (!CreateFastaFile(proteinCollectionInfo, LocalOrgDBFolder))
                {
                    // There was a problem. Log entries in lower-level routines provide documentation
                    return false;
                }

                // Fasta file was successfully generated. Put the name of the generated fastafile in the
                // job data class for other methods to use
                if (!m_jobParams.AddAdditionalParameter("PeptideSearch", JOB_PARAM_GENERATED_FASTA_NAME, m_FastaFileName))
                {
                    LogError("Error adding parameter 'generatedFastaName' to m_jobParams");
                    return false;
                }

                // Delete old fasta files and suffix array files if getting low on disk space
                // No need to pass a value for legacyFastaFileBaseName because a .fasta.LastUsed file will have been created/updated by CreateFastaFile
                PurgeFastaFilesIfLowFreeSpace(LocalOrgDBFolder, freeSpaceThresholdPercent, 0, "");

            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveOrgDB", ex);
                return false;
            }

            // We got to here OK, so return
            return true;

        }

        /// <summary>
        /// Overrides base class version of the function to creates a Sequest params file compatible
        /// with the Bioworks version on this System. Uses ParamFileGenerator dll provided by Ken Auberry
        /// </summary>
        /// <param name="paramFileName">Name of param file to be created</param>
        /// <returns>True for success; False for failure</returns>
        protected bool RetrieveGeneratedParamFile(string paramFileName)
        {

            IGenerateFile ParFileGen = null;

            try
            {
                LogMessage("Retrieving parameter file " + paramFileName);

                ParFileGen = new ParamFileGenerator.MakeParams.clsMakeParameterFile
                {
                    TemplateFilePath = m_mgrParams.GetParam("paramtemplateloc")
                };

                // Note that job parameter "generatedFastaName" gets defined by RetrieveOrgDB
                // Furthermore, the full path to the fasta file is only necessary when creating Sequest parameter files
                var toolName = m_jobParams.GetParam("ToolName", string.Empty);
                if (string.IsNullOrWhiteSpace(toolName))
                {
                    m_message = "Job parameter ToolName is empty";
                    return false;
                }

                var paramFileType = SetParamfileType(toolName);
                if (paramFileType == IGenerateFile.ParamFileType.Invalid)
                {
                    m_message = "Tool " + toolName + " is not supported by the ParamFileGenerator; update clsAnalysisResources and ParamFileGenerator.dll";
                    return false;
                }

                var fastaFilePath = Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("PeptideSearch", JOB_PARAM_GENERATED_FASTA_NAME));

                // Gigasax.DMS5
                var connectionString = m_mgrParams.GetParam("connectionstring");
                var datasetID = m_jobParams.GetJobParameter("JobParameters", "DatasetID", 0);

                var blnSuccess = ParFileGen.MakeFile(paramFileName, paramFileType, fastaFilePath, m_WorkingDir, connectionString, datasetID);

                // Examine the size of the ModDefs.txt file
                // Add it to the ignore list if it is empty (no point in tracking a 0-byte file)
                var fiModDefs = new FileInfo(Path.Combine(m_WorkingDir, Path.GetFileNameWithoutExtension(paramFileName) + "_ModDefs.txt"));
                if (fiModDefs.Exists && fiModDefs.Length == 0)
                {
                    m_jobParams.AddResultFileToSkip(fiModDefs.Name);
                }

                if (blnSuccess)
                {
                    if (m_DebugLevel >= 3)
                    {
                        LogMessage("Successfully retrieved param file: " + paramFileName);
                    }

                    return true;
                }

                LogError(m_message + ": " + ParFileGen.LastError);
                return false;
            }
            catch (Exception ex)
            {
                if (string.IsNullOrWhiteSpace(m_message))
                {
                    m_message = "Error retrieving parameter file";
                }

                LogError(m_message, ex);

                if (!string.IsNullOrWhiteSpace(ParFileGen?.LastError))
                {
                    LogMessage("Error converting param file: " + ParFileGen.LastError, 0, true);
                }
                return false;
            }

        }

        /// <summary>
        /// Creates the specified settings file from db info
        /// </summary>
        /// <returns>TRUE if file created successfully; FALSE otherwise</returns>
        /// <remarks>Use this overload with jobs where settings file is retrieved from database</remarks>
        protected internal bool RetrieveSettingsFileFromDb()
        {

            var OutputFile = Path.Combine(m_WorkingDir, m_jobParams.GetParam("SettingsFileName"));

            return CreateSettingsFile(m_jobParams.GetParam("ParameterXML"), OutputFile);

        }

        /// <summary>
        /// Specifies the Bioworks version for use by the Param File Generator DLL
        /// </summary>
        /// <param name="toolName">Version specified in mgr config file</param>
        /// <returns>IGenerateFile.ParamFileType based on input version</returns>
        /// <remarks></remarks>
        protected IGenerateFile.ParamFileType SetParamfileType(string toolName)
        {

            var toolNameToTypeMapping = new Dictionary<String, IGenerateFile.ParamFileType>(StringComparer.CurrentCultureIgnoreCase)
            {
                {"sequest", IGenerateFile.ParamFileType.BioWorks_Current},
                {"xtandem", IGenerateFile.ParamFileType.X_Tandem},
                {"inspect", IGenerateFile.ParamFileType.Inspect},
                {"msgfplus", IGenerateFile.ParamFileType.MSGFPlus},
                {"msalign_histone", IGenerateFile.ParamFileType.MSAlignHistone},
                {"msalign", IGenerateFile.ParamFileType.MSAlign},
                {"moda", IGenerateFile.ParamFileType.MODa},
                {"mspathfinder", IGenerateFile.ParamFileType.MSPathFinder},
                {"modplus", IGenerateFile.ParamFileType.MODPlus}
            };

            IGenerateFile.ParamFileType paramFileType;

            if (toolNameToTypeMapping.TryGetValue(toolName, out paramFileType))
            {
                return paramFileType;
            }

            var strToolNameLCase = toolName.ToLower();

            foreach (var entry in toolNameToTypeMapping)
            {
                if (strToolNameLCase.Contains(entry.Key.ToLower()))
                {
                    return entry.Value;
                }
            }

            return IGenerateFile.ParamFileType.Invalid;
        }

        /// <summary>
        /// Converts the dictionary items to a list of key/value pairs separated by an equals sign
        /// Next, calls StorePackedJobParameterList to store the list (items will be separated by tab characters)
        /// </summary>
        /// <param name="dctItems">Dictionary items to store as a packed job parameter</param>
        /// <param name="strParameterName">Packed job parameter name</param>
        /// <remarks></remarks>
        protected void StorePackedJobParameterDictionary(Dictionary<string, int> dctItems, string strParameterName)
        {
            var lstItems = new List<string>();

            foreach (var item in dctItems)
            {
                lstItems.Add(item.Key + "=" + item.Value);
            }

            StorePackedJobParameterList(lstItems, strParameterName);

        }
        /// <summary>
        /// Converts the dictionary items to a list of key/value pairs separated by an equals sign
        /// Next, calls StorePackedJobParameterList to store the list (items will be separated by tab characters)
        /// </summary>
        /// <param name="dctItems">Dictionary items to store as a packed job parameter</param>
        /// <param name="strParameterName">Packed job parameter name</param>
        /// <remarks></remarks>
        public void StorePackedJobParameterDictionary(Dictionary<string, string> dctItems, string strParameterName)
        {
            var lstItems = new List<string>();

            foreach (var item in dctItems)
            {
                lstItems.Add(item.Key + "=" + item.Value);
            }

            StorePackedJobParameterList(lstItems, strParameterName);

        }

        /// <summary>
        /// Convert a string list to a packed job parameter (items are separated by tab characters)
        /// </summary>
        /// <param name="lstItems">List items to store as a packed job parameter</param>
        /// <param name="strParameterName">Packed job parameter name</param>
        /// <remarks></remarks>
        protected void StorePackedJobParameterList(List<string> lstItems, string strParameterName)
        {
            m_jobParams.AddAdditionalParameter("JobParameters", strParameterName, clsGlobal.FlattenList(lstItems, "\t"));

        }

        /// <summary>
        /// Unzips all files in the specified Zip file
        /// If the file is less than 1.25 GB in size then uses Ionic.Zip
        /// Otherwise, uses PKZipC (provided PKZipC.exe exists)
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="outFolderPath">Target directory for the extracted files</param>
        /// <param name="callingFunctionName">Calling function name (used for debugging purposes)</param>
        /// <param name="forceExternalZipProgramUse">If True, then force use of PKZipC.exe</param>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks></remarks>
        public bool UnzipFileStart(string zipFilePath, string outFolderPath, string callingFunctionName, bool forceExternalZipProgramUse)
        {
            var success = m_FileSearch.UnzipFileStart(zipFilePath, outFolderPath, callingFunctionName, forceExternalZipProgramUse);
            return success;
        }

        /// <summary>
        /// Update m_message, which is logged in the pipeline job steps table when the job step finishes
        /// </summary>
        /// <param name="statusMessage">New status message</param>
        /// <param name="appendToExisting">True to append to m_message; false to overwrite it</param>
        public void UpdateStatusMessage(string statusMessage, bool appendToExisting = false)
        {
            if (appendToExisting)
            {
                clsGlobal.AppendToComment(m_message, statusMessage);
            }
            else
            {
                m_message = statusMessage;
            }
        }

        /// <summary>
        /// Removes any spectra with 2 or fewer ions in a _DTA.txt ifle
        /// </summary>
        /// <param name="strWorkDir">Folder with the CDTA file</param>
        /// <param name="strInputFileName">CDTA filename</param>
        /// <returns>True if success; false if an error</returns>
        protected bool ValidateCDTAFileRemoveSparseSpectra(string strWorkDir, string strInputFileName)
        {
            var blnSuccess = m_CDTAUtilities.RemoveSparseSpectra(strWorkDir, strInputFileName);
            if (!blnSuccess && string.IsNullOrEmpty(m_message))
            {
                m_message = "m_CDTAUtilities.RemoveSparseSpectra returned False";
            }

            return blnSuccess;

        }

        /// <summary>
        /// Makes sure the specified _DTA.txt file has scan=x and cs=y tags in the parent ion line
        /// </summary>
        /// <param name="strSourceFilePath">Input _DTA.txt file to parse</param>
        /// <param name="blnReplaceSourceFile">If True, then replaces the source file with and updated file</param>
        /// <param name="blnDeleteSourceFileIfUpdated">
        /// Only valid if blnReplaceSourceFile=True;
        /// If True, then the source file is deleted if an updated version is created.
        /// If false, then the source file is renamed to .old if an updated version is created.
        /// </param>
        /// <param name="strOutputFilePath">
        /// Output file path to use for the updated file; required if blnReplaceSourceFile=False; ignored if blnReplaceSourceFile=True
        /// </param>
        /// <returns>True if success; false if an error</returns>
        protected bool ValidateCDTAFileScanAndCSTags(string strSourceFilePath, bool blnReplaceSourceFile, bool blnDeleteSourceFileIfUpdated, string strOutputFilePath)
        {
            var blnSuccess = m_CDTAUtilities.ValidateCDTAFileScanAndCSTags(strSourceFilePath, blnReplaceSourceFile, blnDeleteSourceFileIfUpdated, strOutputFilePath);
            if (!blnSuccess && string.IsNullOrEmpty(m_message))
            {
                m_message = "m_CDTAUtilities.ValidateCDTAFileScanAndCSTags returned False";
            }

            return blnSuccess;

        }

        /// <summary>
        /// Condenses CDTA files that are over 2 GB in size
        /// </summary>
        /// <param name="strWorkDir"></param>
        /// <param name="strInputFileName"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool ValidateCDTAFileSize(string strWorkDir, string strInputFileName)
        {
            var blnSuccess = m_CDTAUtilities.ValidateCDTAFileSize(strWorkDir, strInputFileName);
            if (!blnSuccess && string.IsNullOrEmpty(m_message))
            {
                m_message = "m_CDTAUtilities.ValidateCDTAFileSize returned False";
            }

            return blnSuccess;

        }

        public bool ValidateCDTAFileIsCentroided(string strCDTAPath)
        {

            try
            {
                // Read the m/z values in the _dta.txt file
                // Examine the data in each spectrum to determine if it is centroided

                mSpectraTypeClassifier = new SpectraTypeClassifier.clsSpectrumTypeClassifier();
                mSpectraTypeClassifier.ErrorEvent += mSpectraTypeClassifier_ErrorEvent;
                mSpectraTypeClassifier.ReadingSpectra += mSpectraTypeClassifier_ReadingSpectra;

                var blnSuccess = mSpectraTypeClassifier.CheckCDTAFile(strCDTAPath);

                if (!blnSuccess)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("SpectraTypeClassifier encountered an error while parsing the _dta.txt file");
                    }

                    return false;
                }

                var fractionCentroided = mSpectraTypeClassifier.FractionCentroided();

                var commentSuffix = " (" + mSpectraTypeClassifier.TotalSpectra() + " total spectra)";

                if (fractionCentroided > 0.8)
                {
                    // At least 80% of the spectra are centroided

                    if (fractionCentroided > 0.999)
                    {
                        LogMessage("All of the spectra are centroided" + commentSuffix);
                    }
                    else
                    {
                        LogMessage((fractionCentroided * 100).ToString("0") + "% of the spectra are centroided" + commentSuffix);
                    }

                    return true;

                }

                if (fractionCentroided > 0.001)
                {
                    // Less than 80% of the spectra are centroided
                    // Post a message similar to:
                    //   MSGF+ will likely skip 90% of the spectra because they did not appear centroided
                    m_message = "MSGF+ will likely skip " + ((1 - fractionCentroided) * 100).ToString("0") + "% of the spectra because they do not appear centroided";
                    LogMessage(m_message + commentSuffix);
                    return false;
                }

                // None of the spectra are centroided; unable to process with MSGF+
                m_message = SPECTRA_ARE_NOT_CENTROIDED + " with MSGF+";
                LogMessage(m_message + commentSuffix, 0, true);
                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateCDTAFileIsCentroided", ex);
                return false;
            }

        }

        /// <summary>
        /// Validate that the specified file exists and has at least one tab-delimited row with a numeric value in the first column
        /// </summary>
        /// <param name="strFilePath">Path to the file</param>
        /// <param name="strFileDescription">File description, e.g. Synopsis</param>
        /// <param name="strErrorMessage"></param>
        /// <returns>True if the file has data; otherwise false</returns>
        /// <remarks></remarks>
        public static bool ValidateFileHasData(string strFilePath, string strFileDescription, out string strErrorMessage)
        {
            const int intNumericDataColIndex = 0;
            return ValidateFileHasData(strFilePath, strFileDescription, out strErrorMessage, intNumericDataColIndex);
        }

        /// <summary>
        /// Validate that the specified file exists and has at least one tab-delimited row with a numeric value
        /// </summary>
        /// <param name="strFilePath">Path to the file</param>
        /// <param name="strFileDescription">File description, e.g. Synopsis</param>
        /// <param name="strErrorMessage"></param>
        /// <param name="intNumericDataColIndex">Index of the numeric data column; use -1 to simply look for any text in the file</param>
        /// <returns>True if the file has data; otherwise false</returns>
        /// <remarks></remarks>
        public static bool ValidateFileHasData(string strFilePath, string strFileDescription, out string strErrorMessage, int intNumericDataColIndex)
        {

            var blnDataFound = false;

            strErrorMessage = string.Empty;

            try
            {
                var fiFileInfo = new FileInfo(strFilePath);

                if (!fiFileInfo.Exists)
                {
                    strErrorMessage = strFileDescription + " file not found: " + fiFileInfo.Name;
                    return false;
                }

                if (fiFileInfo.Length == 0)
                {
                    strErrorMessage = strFileDescription + " file is empty (zero-bytes)";
                    return false;
                }

                // Open the file and confirm it has data rows
                using (var srInFile = new StreamReader(new FileStream(fiFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream & !blnDataFound)
                    {
                        var strLineIn = srInFile.ReadLine();
                        if (!string.IsNullOrEmpty(strLineIn))
                        {
                            if (intNumericDataColIndex < 0)
                            {
                                blnDataFound = true;
                            }
                            else
                            {
                                // Split on the tab character and check if the first column is numeric
                                var strSplitLine = strLineIn.Split('\t');

                                if (strSplitLine.Length <= intNumericDataColIndex)
                                    continue;

                                double value;
                                if (double.TryParse(strSplitLine[intNumericDataColIndex], out value))
                                {
                                    blnDataFound = true;
                                }
                            }
                        }
                    }
                }

                if (!blnDataFound)
                {
                    strErrorMessage = strFileDescription + " is empty (no data)";
                }

            }
            catch (Exception)
            {
                strErrorMessage = "Exception validating " + strFileDescription + " file";
                return false;
            }

            return blnDataFound;

        }

        /// <summary>
        /// Validates that sufficient free memory is available to run Java
        /// </summary>
        /// <param name="strJavaMemorySizeJobParamName">Name of the job parameter that defines the amount of memory (in MB) to reserve for Java</param>
        /// <param name="strStepToolName">Step tool name to use when posting log entries</param>
        /// <returns>True if sufficient free memory; false if not enough free memory</returns>
        /// <remarks>Typical names for strJavaMemorySizeJobParamName are MSGFJavaMemorySize, MSGFDBJavaMemorySize, and MSDeconvJavaMemorySize.
        /// These parameters are loaded from DMS Settings Files (table T_Settings_Files in DMS5, copied to table T_Job_Parameters in DMS_Pipeline) </remarks>
        protected bool ValidateFreeMemorySize(string strJavaMemorySizeJobParamName, string strStepToolName)
        {

            const bool blnLogFreeMemoryOnSuccess = true;
            return ValidateFreeMemorySize(strJavaMemorySizeJobParamName, strStepToolName, blnLogFreeMemoryOnSuccess);

        }

        /// <summary>
        /// Validates that sufficient free memory is available to run Java
        /// </summary>
        /// <param name="strMemorySizeJobParamName">Name of the job parameter that defines the amount of memory (in MB) that must be available on the system</param>
        /// <param name="strStepToolName">Step tool name to use when posting log entries</param>
        /// <param name="blnLogFreeMemoryOnSuccess">If True, then post a log entry if sufficient memory is, in fact, available</param>
        /// <returns>True if sufficient free memory; false if not enough free memory</returns>
        /// <remarks>Typical names for strJavaMemorySizeJobParamName are MSGFJavaMemorySize, MSGFDBJavaMemorySize, and MSDeconvJavaMemorySize.
        /// These parameters are loaded from DMS Settings Files (table T_Settings_Files in DMS5, copied to table T_Job_Parameters in DMS_Pipeline)
        /// </remarks>
        protected bool ValidateFreeMemorySize(string strMemorySizeJobParamName, string strStepToolName, bool blnLogFreeMemoryOnSuccess)
        {
            // Lookup parameter strMemorySizeJobParamName; assume 2000 MB if not defined
            var freeMemoryRequiredMB = m_jobParams.GetJobParameter(strMemorySizeJobParamName, 2000);

            // Require freeMemoryRequiredMB be at least 0.5 GB
            if (freeMemoryRequiredMB < 512)
                freeMemoryRequiredMB = 512;

            if (m_DebugLevel < 1)
                blnLogFreeMemoryOnSuccess = false;

            return ValidateFreeMemorySize(freeMemoryRequiredMB, strStepToolName, blnLogFreeMemoryOnSuccess);

        }

        public static bool ValidateFreeMemorySize(int freeMemoryRequiredMB, string strStepToolName, bool blnLogFreeMemoryOnSuccess)
        {
            string strMessage;

            var sngFreeMemoryMB = clsGlobal.GetFreeMemoryMB();

            if (freeMemoryRequiredMB >= sngFreeMemoryMB)
            {
                strMessage = "Not enough free memory to run " + strStepToolName;

                strMessage += "; need " + freeMemoryRequiredMB.ToString() + " MB but system has " + sngFreeMemoryMB.ToString("0") + " MB available";
                clsGlobal.LogError(strMessage);
                return false;
            }

            if (blnLogFreeMemoryOnSuccess)
            {
                strMessage = strStepToolName + " will use " + freeMemoryRequiredMB.ToString() + " MB; " +
                             "system has " + sngFreeMemoryMB.ToString("0") + " MB available";
                clsGlobal.LogDebug(strMessage);
            }

            return true;
        }

        #endregion

        #region "Event Handlers"

        private void m_CDTAUtilities_ProgressEvent(string taskDescription, float percentComplete)
        {

            if (m_DebugLevel >= 1)
            {
                if (m_DebugLevel == 1 && DateTime.UtcNow.Subtract(m_LastCDTAUtilitiesUpdateTime).TotalSeconds >= 60 ||
                    m_DebugLevel > 1 && DateTime.UtcNow.Subtract(m_LastCDTAUtilitiesUpdateTime).TotalSeconds >= 20)
                {
                    m_LastCDTAUtilitiesUpdateTime = DateTime.UtcNow;

                    LogDebugMessage(" ... CDTAUtilities: " + percentComplete.ToString("0.00") + "% complete");
                }
            }

        }

        private void m_FastaTools_FileGenerationCompleted(string FullOutputPath)
        {
            // Get the name of the fasta file that was generated
            m_FastaFileName = Path.GetFileName(FullOutputPath);
        }

        private void m_FastaTools_FileGenerationProgress(string statusMsg, double fractionDone)
        {

            const int MINIMUM_LOG_INTERVAL_SEC = 10;

            var blnForcelog = m_DebugLevel >= 1 && statusMsg.Contains(Protein_Exporter.clsGetFASTAFromDMS.LOCK_FILE_PROGRESS_TEXT);

            if (m_DebugLevel >= 3 || blnForcelog)
            {
                // Limit the logging to once every MINIMUM_LOG_INTERVAL_SEC seconds
                if (blnForcelog || DateTime.UtcNow.Subtract(m_FastaToolsLastLogTime).TotalSeconds >= MINIMUM_LOG_INTERVAL_SEC ||
                    fractionDone - m_FastaToolFractionDoneSaved >= 0.25)
                {
                    m_FastaToolsLastLogTime = DateTime.UtcNow;
                    m_FastaToolFractionDoneSaved = fractionDone;
                    LogDebugMessage("Generating Fasta file, " + (fractionDone * 100).ToString("0.0") + "% complete, " + statusMsg);
                }
            }

        }

        private void m_SplitFastaFileUtility_ErrorEvent(string errorMessage, Exception ex)
        {
            LogError(errorMessage, ex);
        }

        private void m_SplitFastaFileUtility_ProgressUpdate(string progressMessage, float percentComplete)
        {

            if (m_DebugLevel >= 1)
            {

                if (m_DebugLevel == 1 && DateTime.UtcNow.Subtract(m_SplitFastaLastUpdateTime).TotalSeconds >= 60 ||
                    m_DebugLevel > 1 && DateTime.UtcNow.Subtract(m_SplitFastaLastUpdateTime).TotalSeconds >= 20 ||
                    percentComplete >= 100 & m_SplitFastaLastPercentComplete < 100)
                {
                    m_SplitFastaLastUpdateTime = DateTime.UtcNow;
                    m_SplitFastaLastPercentComplete = percentComplete;

                    if (percentComplete > 0)
                    {
                        LogDebugMessage(" ... " + progressMessage + ", " + percentComplete + "% complete");
                    }
                    else
                    {
                        LogDebugMessage(" ... SplitFastaFile: " + progressMessage);
                    }
                }
            }

        }

        private void m_SplitFastaFileUtility_SplittingBaseFastaFile(string strBaseFastaFileName, int numSplitParts)
        {
            LogDebugMessage("Splitting " + strBaseFastaFileName + " into " + numSplitParts + " parts");
        }

        #endregion

        #region "FileCopyUtilities Events"

        private void FileCopyUtilities_ResetTimestampForQueueWaitTime()
        {
            ResetTimestampForQueueWaitTimeLogging();
        }

        #endregion

        #region "SpectraTypeClassifier Events"

        private void mSpectraTypeClassifier_ErrorEvent(string strMessage)
        {
        }

        private void mSpectraTypeClassifier_ReadingSpectra(int spectraProcessed)
        {
            LogDebugMessage(" ... " + spectraProcessed + " spectra parsed in the _dta.txt file");
        }

        #endregion

    }

}