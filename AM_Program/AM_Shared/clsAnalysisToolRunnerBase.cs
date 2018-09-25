using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PRISM;
using PRISM.Logging;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/19/2007
//
//*********************************************************************************************************

// ReSharper disable UnusedMember.Global
namespace AnalysisManagerBase
{
    /// <summary>
    /// Base class for analysis tool runner
    /// </summary>
    public class clsAnalysisToolRunnerBase : clsAnalysisMgrBase, IToolRunner
    {

        #region "Constants"

        /// <summary>
        /// Stored procedure name for storing the step tool version
        /// </summary>
        protected const string SP_NAME_SET_TASK_TOOL_VERSION = "SetStepTaskToolVersion";

        /// <summary>
        /// Default date/time format
        /// </summary>
        public const string DATE_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss tt";

        /// <summary>
        /// Failed results directory (typically on the C: drive)
        /// </summary>
        public const string DMS_FAILED_RESULTS_DIRECTORY_NAME = "DMS_FailedResults";

        /// <summary>
        /// Text to store in m_message when no results passed filters
        /// </summary>
        /// <remarks>
        /// This text will be sent to the database via the CompletionMessage parameter,
        /// and the job will be assigned state No Export (14) in DMS (see stored procedure UpdateJobState)
        /// </remarks>
        public const string NO_RESULTS_ABOVE_THRESHOLD = "No results above threshold";

        /// <summary>
        /// This error message is reported by the SEQUEST plugin
        /// </summary>
        /// <remarks>The Analysis Manager looks for this message when deciding whether the manager needs to be disabled locally if a job fails</remarks>
        public const string PVM_RESET_ERROR_MESSAGE = "Error resetting PVM";

        /// <summary>
        /// Purge interval for cached server files, in minutes
        /// </summary>
        private const int CACHED_SERVER_FILES_PURGE_INTERVAL = 90;

        #endregion

        #region "Module variables"

        /// <summary>
        /// Access to the job parameters
        /// </summary>
        protected IJobParams m_jobParams;

        /// <summary>
        /// Access to manager parameters
        /// </summary>
        protected IMgrParams m_mgrParams;

        /// <summary>
        /// access to settings file parameters
        /// </summary>
        protected readonly XmlSettingsFileAccessor m_settingsFileParams = new XmlSettingsFileAccessor();

        /// <summary>
        /// Progress of run (in percent)
        /// </summary>
        /// <remarks>This is a value between 0 and 100</remarks>
        protected float m_progress;

        /// <summary>
        /// Status code
        /// </summary>
        protected EnumMgrStatus m_status;

        /// <summary>
        /// DTA count for status report
        /// </summary>
        protected int m_DtaCount = 0;

        /// <summary>
        /// Can be used to pass codes regarding the results of this analysis back to the DMS_Pipeline DB
        /// </summary>
        protected int m_EvalCode;

        /// <summary>
        /// Can be used to pass information regarding the results of this analysis back to the DMS_Pipeline DB
        /// </summary>
        protected string m_EvalMessage = string.Empty;

        /// <summary>
        /// Working directory
        /// </summary>
        protected string m_WorkDir;

        /// <summary>
        /// Machine name (aka manager name)
        /// </summary>
        protected string m_MachName;

        /// <summary>
        /// Job number
        /// </summary>
        protected int m_JobNum;

        /// <summary>
        /// Dataset name
        /// </summary>
        protected string m_Dataset;

        /// <summary>
        /// Analysis start time (UTC-based)
        /// </summary>
        protected DateTime m_StartTime;

        /// <summary>
        /// Analysis end time
        /// </summary>
        protected DateTime m_StopTime;

        /// <summary>
        /// Results folder name
        /// </summary>
        protected string m_ResFolderName;

        /// <summary>
        /// DLL file info
        /// </summary>
        protected string m_FileVersion;

        /// <summary>
        /// DLL file date
        /// </summary>
        protected string m_FileDate;

        /// <summary>
        /// DotNetZip tools
        /// </summary>
        protected clsDotNetZipTools m_DotNetZipTools;

        /// <summary>
        /// Set to true if we need to abort processing as soon as possible
        /// </summary>
        protected bool m_NeedToAbortProcessing;

        /// <summary>
        /// Analysis job summary file
        /// </summary>
        protected clsSummaryFile m_SummaryFile;

        /// <summary>
        /// MyEMSL Utilities
        /// </summary>
        protected clsMyEMSLUtilities m_MyEMSLUtilities;

        private DateTime m_LastProgressWriteTime = DateTime.UtcNow;

        private DateTime m_LastProgressConsoleTime = DateTime.UtcNow;

        private DateTime m_LastStatusFileUpdate = DateTime.UtcNow;

        private DateTime mLastSortUtilityProgress;

        private string mSortUtilityErrorMessage;

        /// <summary>
        /// Program runner start time
        /// </summary>
        protected DateTime mProgRunnerStartTime;

        private DateTime mLastCachedServerFilesPurgeCheck = DateTime.UtcNow.AddMinutes(-CACHED_SERVER_FILES_PURGE_INTERVAL * 2);

        private static DateTime mLastManagerSettingsUpdateTime = DateTime.UtcNow;

        /// <summary>
        /// Queue tracking recent CPU values from an externally spawned process
        /// </summary>
        /// <remarks>Keys are the sampling date, value is the CPU usage (number of cores in use)</remarks>
        protected readonly Queue<KeyValuePair<DateTime, float>> mCoreUsageHistory;

        #endregion

        #region "Properties"

        /// <summary>
        /// Dataset name
        /// </summary>
        public string Dataset => m_Dataset;

        /// <summary>
        /// Evaluation code to be reported to the DMS_Pipeline DB
        /// </summary>
        public int EvalCode => m_EvalCode;

        /// <summary>
        /// Evaluation message to be reported to the DMS_Pipeline DB
        /// </summary>
        public string EvalMessage => string.IsNullOrWhiteSpace(m_EvalMessage) ? string.Empty : m_EvalMessage;

        /// <summary>
        /// Job number
        /// </summary>
        public int Job => m_JobNum;

        /// <summary>
        /// Publicly accessible results folder name and path
        /// </summary>
        public string ResFolderName => m_ResFolderName;

        /// <summary>
        /// Status message related to processing tasks performed by this class
        /// </summary>
        public string Message => string.IsNullOrWhiteSpace(m_message) ? string.Empty : m_message;

        /// <summary>
        /// Set this to true if we need to abort processing as soon as possible due to a critical error
        /// </summary>
        public bool NeedToAbortProcessing => m_NeedToAbortProcessing;

        /// <summary>
        /// Progress of run (in percent)
        /// </summary>
        /// <remarks>This is a value between 0 and 100</remarks>
        public float Progress => m_progress;

        /// <summary>
        /// Time the analysis started (UTC-based)
        /// </summary>
        public DateTime StartTime => m_StartTime;

        /// <summary>
        /// Step tool name
        /// </summary>
        public string StepToolName { get; private set; }

        /// <summary>
        /// Tool version info file
        /// </summary>
        public string ToolVersionInfoFile => "Tool_Version_Info_" + StepToolName + ".txt";

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks></remarks>
        public clsAnalysisToolRunnerBase() : base("clsAnalysisToolRunnerBase")
        {
            mProgRunnerStartTime = DateTime.UtcNow;
            mCoreUsageHistory = new Queue<KeyValuePair<DateTime, float>>();
        }

        /// <summary>
        /// Initializes class
        /// </summary>
        /// <param name="stepToolName">Name of the current step tool</param>
        /// <param name="mgrParams">Object holding manager parameters</param>
        /// <param name="jobParams">Object holding job parameters</param>
        /// <param name="statusTools">Object for status reporting</param>
        /// <param name="summaryFile">Object for creating an analysis job summary file</param>
        /// <param name="myEMSLUtilities">MyEMSL download Utilities</param>
        /// <remarks></remarks>
        public virtual void Setup(
            string stepToolName,
            IMgrParams mgrParams,
            IJobParams jobParams,
            IStatusFile statusTools,
            clsSummaryFile summaryFile,
            clsMyEMSLUtilities myEMSLUtilities)
        {
            StepToolName = stepToolName;

            m_mgrParams = mgrParams;
            m_jobParams = jobParams;
            m_StatusTools = statusTools;

            TraceMode = mgrParams.TraceMode;

            m_WorkDir = m_mgrParams.GetParam("WorkDir");
            m_MachName = m_mgrParams.ManagerName;

            m_JobNum = m_jobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0);

            m_Dataset = m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_DATASET_NAME);

            m_MyEMSLUtilities = myEMSLUtilities ?? new clsMyEMSLUtilities(m_DebugLevel, m_WorkDir);
            RegisterEvents(m_MyEMSLUtilities);

            m_DebugLevel = (short)m_mgrParams.GetParam("DebugLevel", 1);
            m_StatusTools.Tool = m_jobParams.GetCurrentJobToolDescription();

            m_SummaryFile = summaryFile;

            m_ResFolderName = m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME);

            if (m_DebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerBase.Setup()");
            }

            m_DotNetZipTools = new clsDotNetZipTools(m_DebugLevel, m_WorkDir);
            RegisterEvents(m_DotNetZipTools);

            InitFileTools(m_MachName, m_DebugLevel);

            m_NeedToAbortProcessing = false;

            m_message = string.Empty;
            m_EvalCode = 0;
            m_EvalMessage = string.Empty;

        }

        /// <summary>
        /// Calculates total run time for a job
        /// </summary>
        /// <param name="startTime">Time job started</param>
        /// <param name="stopTime">Time of job completion</param>
        /// <returns>Total job run time (HH:MM)</returns>
        /// <remarks></remarks>
        protected string CalcElapsedTime(DateTime startTime, DateTime stopTime)
        {
            if (stopTime < startTime)
            {
                LogMessage("CalcElapsedTime: Stop time is less than StartTime; this is unexpected.  Assuming current time for StopTime");
                stopTime = DateTime.UtcNow;
            }

            if (stopTime < startTime || startTime == DateTime.MinValue)
            {
                return string.Empty;
            }

            var dtElapsedTime = stopTime.Subtract(startTime);

            if (m_DebugLevel >= 2)
            {
                LogDebug($"CalcElapsedTime: StartTime = {startTime}; StopTime = {stopTime}");

                LogDebug($"CalcElapsedTime: {dtElapsedTime.Hours} Hours, {dtElapsedTime.Minutes} Minutes, {dtElapsedTime.Seconds} Seconds");

                LogDebug($"CalcElapsedTime: TotalMinutes = {dtElapsedTime.TotalMinutes:0.00}");
            }

            return dtElapsedTime.Hours.ToString("###0") + ":" + dtElapsedTime.Minutes.ToString("00") + ":" + dtElapsedTime.Seconds.ToString("00");

        }

        /// <summary>
        /// Computes the incremental progress that has been made beyond currentTaskProgressAtStart, based on the number of items processed and the next overall progress level
        /// </summary>
        /// <param name="currentTaskProgressAtStart">Progress at the start of the current subtask (value between 0 and 100)</param>
        /// <param name="currentTaskProgressAtEnd">Progress at the start of the current subtask (value between 0 and 100)</param>
        /// <param name="subTaskProgress">Progress of the current subtask (value between 0 and 100)</param>
        /// <returns>Overall progress (value between 0 and 100)</returns>
        /// <remarks></remarks>
        public static float ComputeIncrementalProgress(float currentTaskProgressAtStart, float currentTaskProgressAtEnd, float subTaskProgress)
        {
            if (subTaskProgress < 0)
            {
                return currentTaskProgressAtStart;
            }

            if (subTaskProgress >= 100)
            {
                return currentTaskProgressAtEnd;
            }

            return (float)(currentTaskProgressAtStart + (subTaskProgress / 100.0) * (currentTaskProgressAtEnd - currentTaskProgressAtStart));
        }

        /// <summary>
        /// Computes the incremental progress that has been made beyond currentTaskProgressAtStart, based on the number of items processed and the next overall progress level
        /// </summary>
        /// <param name="currentTaskProgressAtStart">Progress at the start of the current subtask (value between 0 and 100)</param>
        /// <param name="currentTaskProgressAtEnd">Progress at the start of the current subtask (value between 0 and 100)</param>
        /// <param name="currentTaskItemsProcessed">Number of items processed so far during this subtask</param>
        /// <param name="currentTaskTotalItems">Total number of items to process during this subtask</param>
        /// <returns>Overall progress (value between 0 and 100)</returns>
        /// <remarks></remarks>
        public static float ComputeIncrementalProgress(float currentTaskProgressAtStart, float currentTaskProgressAtEnd, int currentTaskItemsProcessed, int currentTaskTotalItems)
        {
            if (currentTaskTotalItems < 1)
            {
                return currentTaskProgressAtStart;
            }

            if (currentTaskItemsProcessed > currentTaskTotalItems)
            {
                return currentTaskProgressAtEnd;
            }

            return currentTaskProgressAtStart +
                currentTaskItemsProcessed / (float)currentTaskTotalItems * (currentTaskProgressAtEnd - currentTaskProgressAtStart);

        }

        /// <summary>
        /// Computes the maximum threads to allow given the number of cores on the machine and
        /// the the amount of memory that each thread is allowed to reserve
        /// </summary>
        /// <param name="memorySizeMBPerThread">Amount of memory allocated to each thread</param>
        /// <returns>Maximum number of cores to use</returns>
        /// <remarks></remarks>
        protected int ComputeMaxThreadsGivenMemoryPerThread(float memorySizeMBPerThread)
        {

            if (memorySizeMBPerThread < 512)
                memorySizeMBPerThread = 512;

            var maxThreadsToAllow = clsGlobal.GetCoreCount();

            var freeMemoryMB = m_StatusTools.GetFreeMemoryMB();

            var maxThreadsBasedOnMemory = freeMemoryMB / memorySizeMBPerThread;

            // Round up maxThreadsBasedOnMemory only if it is within 0.2 of the next highest integer
            var maxThreadsRoundedUp = (int)Math.Ceiling(maxThreadsBasedOnMemory);
            if (maxThreadsRoundedUp - maxThreadsBasedOnMemory <= 0.2)
            {
                maxThreadsBasedOnMemory = maxThreadsRoundedUp;
            }
            else
            {
                maxThreadsBasedOnMemory = maxThreadsRoundedUp - 1;
            }

            if (maxThreadsBasedOnMemory < maxThreadsToAllow)
            {
                maxThreadsToAllow = (int)Math.Round(maxThreadsBasedOnMemory);
            }

            return maxThreadsToAllow;

        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        /// <remarks>
        /// Prior to calling this method, add files to ignore using
        /// m_jobParams.AddResultFileToSkip and m_jobParams.AddResultFileExtensionToSkip
        /// Step tools may override this method if additional steps are required
        /// The override method should then call base.CopyFailedResultsToArchiveFolder as the last step
        /// </remarks>
        public virtual void CopyFailedResultsToArchiveFolder()
        {
            if (clsGlobal.OfflineMode)
            {
                // Offline mode jobs each have their own work directory
                // Thus, copying of failed results is not applicable
                LogWarning("Processing interrupted; see local work directory: " + m_WorkDir);
                return;
            }

            var failedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrWhiteSpace(failedResultsFolderPath))
            {
                LogErrorToDatabase("Manager parameter FailedResultsFolderPath not defined for manager " + m_mgrParams.ManagerName);
                failedResultsFolderPath = @"C:\" + DMS_FAILED_RESULTS_DIRECTORY_NAME;
            }

            LogWarning("Processing interrupted; copying results to archive folder: " + failedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory (however, delete the _DTA.txt and _DTA.zip files first)
            var folderPathToArchive = string.Copy(m_WorkDir);

            // Make the results folder
            var success = MakeResultsFolder();
            if (success)
            {
                // Move the result files into the result folder
                var moveSucceed = MoveResultFiles();
                if (moveSucceed)
                {
                    // Move was a success; update folderPathToArchive
                    folderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName);
                }
            }

            // Copy the results folder to the Archive folder
            var analysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
            analysisResults.CopyFailedResultsToArchiveFolder(folderPathToArchive, failedResultsFolderPath);
        }

        /// <summary>
        /// Copies a file (typically a mzXML or mzML file) to a server cache folder
        /// Will store the file in a subdirectory based on job parameter OutputFolderName, and below that, in a folder with a name like 2013_2
        /// </summary>
        /// <param name="cacheFolderPath">Cache folder base path, e.g. \\proto-6\MSXML_Cache</param>
        /// <param name="sourceFilePath">Path to the data file</param>
        /// <param name="purgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
        /// <returns>Path to the remotely cached file; empty path if an error</returns>
        protected string CopyFileToServerCache(string cacheFolderPath, string sourceFilePath, bool purgeOldFilesIfNeeded)
        {

            try
            {
                // m_ResFolderName should contain the output folder; e.g. MSXML_Gen_1_120_275966
                if (string.IsNullOrEmpty(m_ResFolderName))
                {
                    LogError("m_ResFolderName (from job parameter OutputFolderName) is empty; cannot construct MSXmlCache path");
                    return string.Empty;
                }

                // Remove the dataset ID portion from the output folder
                string toolNameVersionFolder;
                try
                {
                    toolNameVersionFolder = clsAnalysisResources.GetMSXmlToolNameVersionFolder(m_ResFolderName);
                }
                catch (Exception)
                {
                    LogError("OutputFolderName is not in the expected form of ToolName_Version_DatasetID (" + m_ResFolderName + "); cannot construct MSXmlCache path");
                    return string.Empty;
                }

                // Determine the year_quarter text for this dataset
                var datasetStoragePath = m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetStoragePath");
                if (string.IsNullOrEmpty(datasetStoragePath))
                    datasetStoragePath = m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetArchivePath");

                var datasetYearQuarter = clsAnalysisResources.GetDatasetYearQuarter(datasetStoragePath);
                if (string.IsNullOrEmpty(datasetYearQuarter))
                {
                    LogError("Unable to determine DatasetYearQuarter using the DatasetStoragePath or DatasetArchivePath; cannot construct MSXmlCache path");
                    return string.Empty;
                }


                var success = CopyFileToServerCache(
                    cacheFolderPath, toolNameVersionFolder, sourceFilePath, datasetYearQuarter,
                    purgeOldFilesIfNeeded: purgeOldFilesIfNeeded, remoteCacheFilePath: out var remoteCacheFilePath);

                if (!success)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("CopyFileToServerCache returned false copying the " + Path.GetExtension(sourceFilePath) + " file to " + Path.Combine(cacheFolderPath, toolNameVersionFolder));
                        return string.Empty;
                    }
                }

                return remoteCacheFilePath;

            }
            catch (Exception ex)
            {
                LogError("Exception in CopyFileToServerCache", ex);
                return string.Empty;
            }

        }

        /// <summary>
        /// Copies a file (typically a mzXML or mzML file) to a server cache folder
        /// Will store the file in the subdirectory subDirectoryInTarget and, below that, in a directory with a name like 2013_2
        /// </summary>
        /// <param name="cacheFolderPath">Cache folder base path, e.g. \\proto-6\MSXML_Cache</param>
        /// <param name="subDirectoryInTarget">Directory name to create below cacheFolderPath (optional), e.g. MSXML_Gen_1_93 or MSConvert</param>
        /// <param name="sourceFilePath">Path to the data file</param>
        /// <param name="datasetYearQuarter">
        /// Dataset year quarter text (optional)
        /// Example value is 2013_2; if this this parameter is blank, will auto-determine using Job Parameter DatasetStoragePath
        /// </param>
        /// <param name="purgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>
        /// Determines the Year_Quarter folder named using the DatasetStoragePath or DatasetArchivePath job parameter
        /// If those parameters are not defined, copies the file anyway
        /// </remarks>
        protected bool CopyFileToServerCache(
            string cacheFolderPath,
            string subDirectoryInTarget,
            string sourceFilePath,
            string datasetYearQuarter,
            bool purgeOldFilesIfNeeded)
        {
            return CopyFileToServerCache(
                cacheFolderPath, subDirectoryInTarget, sourceFilePath,
                datasetYearQuarter, purgeOldFilesIfNeeded, out _);

        }

        /// <summary>
        /// Copies a file (typically a mzXML or mzML file) to a server cache folder
        /// Will store the file in the directory subDirectoryInTarget and, below that, in a folder with a name like 2013_2
        /// </summary>
        /// <param name="cacheFolderPath">Cache folder base path, e.g. \\proto-11\MSXML_Cache</param>
        /// <param name="subDirectoryInTarget">Directory name to create below cacheFolderPath (optional), e.g. MSXML_Gen_1_93 or MSConvert</param>
        /// <param name="sourceFilePath">Path to the data file</param>
        /// <param name="datasetYearQuarter">
        /// Dataset year quarter text (optional)
        /// Example value is 2013_2; if this this parameter is blank, will auto-determine using Job Parameter DatasetStoragePath
        /// </param>
        /// <param name="purgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
        /// <param name="remoteCacheFilePath">Output parameter: the target file path (determined by this function)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>
        /// Determines the Year_Quarter folder named using the DatasetStoragePath or DatasetArchivePath job parameter
        /// If those parameters are not defined, copies the file anyway
        /// </remarks>
        protected bool CopyFileToServerCache(
            string cacheFolderPath,
            string subDirectoryInTarget, string
            sourceFilePath,
            string datasetYearQuarter,
            bool purgeOldFilesIfNeeded,
            out string remoteCacheFilePath)
        {

            remoteCacheFilePath = string.Empty;

            try
            {
                var diCacheFolder = new DirectoryInfo(cacheFolderPath);

                if (!diCacheFolder.Exists)
                {
                    LogWarning("Cache folder not found: " + cacheFolderPath);
                    return false;
                }

                DirectoryInfo targetDirectory;

                // Define the target folder
                if (string.IsNullOrEmpty(subDirectoryInTarget))
                {
                    targetDirectory = diCacheFolder;
                }
                else
                {
                    targetDirectory = new DirectoryInfo(Path.Combine(diCacheFolder.FullName, subDirectoryInTarget));
                    if (!targetDirectory.Exists)
                        targetDirectory.Create();
                }

                if (string.IsNullOrEmpty(datasetYearQuarter))
                {
                    // Determine the year_quarter text for this dataset
                    var datasetStoragePath = m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetStoragePath");
                    if (string.IsNullOrEmpty(datasetStoragePath))
                        datasetStoragePath = m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetArchivePath");

                    datasetYearQuarter = clsAnalysisResources.GetDatasetYearQuarter(datasetStoragePath);
                }

                if (!string.IsNullOrEmpty(datasetYearQuarter))
                {
                    targetDirectory = new DirectoryInfo(Path.Combine(targetDirectory.FullName, datasetYearQuarter));
                    if (!targetDirectory.Exists)
                        targetDirectory.Create();
                }

                m_jobParams.AddResultFileExtensionToSkip(clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX);

                // Create the .hashcheck file
                var hashcheckFilePath = clsGlobal.CreateHashcheckFile(sourceFilePath, computeMD5Hash: true);

                if (string.IsNullOrEmpty(hashcheckFilePath))
                {
                    LogError("Error in CopyFileToServerCache: Hashcheck file was not created");
                    return false;
                }

                var sourceFileName = Path.GetFileName(sourceFilePath);
                if (sourceFileName == null)
                {
                    LogError("Filename not found in " + sourceFilePath + "; cannot copy");
                    return false;
                }

                var fiTargetFile = new FileInfo(Path.Combine(targetDirectory.FullName, sourceFileName));

                ResetTimestampForQueueWaitTimeLogging();
                var startTime = DateTime.UtcNow;

                var success = m_FileTools.CopyFileUsingLocks(sourceFilePath, fiTargetFile.FullName, true);
                LogCopyStats(startTime, fiTargetFile.FullName);

                if (!success)
                {
                    LogError("CopyFileUsingLocks returned false copying " + Path.GetFileName(sourceFilePath) + " to " + fiTargetFile.FullName);
                    return false;
                }

                remoteCacheFilePath = fiTargetFile.FullName;

                if (fiTargetFile.DirectoryName == null)
                {
                    LogError("DirectoryName is null for the target directory; cannot copy the file to the remote cache");
                    return false;
                }

                // Copy over the .Hashcheck file
                m_FileTools.CopyFile(hashcheckFilePath, Path.Combine(fiTargetFile.DirectoryName, Path.GetFileName(hashcheckFilePath)), true);

                if (purgeOldFilesIfNeeded)
                {
                    PurgeOldServerCacheFiles(cacheFolderPath);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in CopyFileToServerCache", ex);
                return false;
            }

        }

        /// <summary>
        /// Copies the .mzXML file to the generic MSXML_Cache folder, e.g. \\proto-6\MSXML_Cache\MSConvert
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <param name="datasetYearQuarter">Dataset year quarter text, e.g. 2013_2; if this this parameter is blank, will auto-determine using Job Parameter DatasetStoragePath</param>
        /// <param name="msXmlGeneratorName">Name of the MzXML generator, e.g. MSConvert</param>
        /// <param name="purgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>
        /// Contrast with CopyMSXmlToCache in clsAnalysisToolRunnerMSXMLGen, where the target folder is
        /// of the form \\proto-6\MSXML_Cache\MSConvert\MSXML_Gen_1_93
        /// </remarks>
        protected bool CopyMzXMLFileToServerCache(string sourceFilePath, string datasetYearQuarter, string msXmlGeneratorName, bool purgeOldFilesIfNeeded)
        {

            try
            {
                var strMSXMLCacheFolderPath = m_mgrParams.GetParam(clsAnalysisResources.JOB_PARAM_MSXML_CACHE_FOLDER_PATH, string.Empty);

                if (string.IsNullOrEmpty(msXmlGeneratorName))
                {
                    msXmlGeneratorName = m_jobParams.GetJobParameter("MSXMLGenerator", string.Empty);

                    if (!string.IsNullOrEmpty(msXmlGeneratorName))
                    {
                        msXmlGeneratorName = Path.GetFileNameWithoutExtension(msXmlGeneratorName);
                    }
                }

                var success = CopyFileToServerCache(strMSXMLCacheFolderPath, msXmlGeneratorName, sourceFilePath, datasetYearQuarter, purgeOldFilesIfNeeded);
                return success;

            }
            catch (Exception ex)
            {
                LogError("Error in CopyMzXMLFileToServerCache", ex);
                return false;
            }

        }

        /// <summary>
        /// Copies the files from the results folder to the transfer folder on the server
        /// </summary>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks></remarks>
        protected bool CopyResultsFolderToServer()
        {

            var transferFolderPath = GetTransferFolderPath();

            if (string.IsNullOrEmpty(transferFolderPath))
            {
                // Error has already been logged and m_message has been updated
                return false;
            }

            return CopyResultsFolderToServer(transferFolderPath);
        }

        /// <summary>
        /// Copies the files from the results folder to the transfer folder on the server
        /// </summary>
        /// <param name="transferFolderPath">Base transfer folder path to use
        /// e.g. \\proto-6\DMS3_Xfer\ or
        /// \\protoapps\PeptideAtlas_Staging\1000_DataPackageName</param>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks></remarks>
        protected bool CopyResultsFolderToServer(string transferFolderPath)
        {

            var sourceFolderPath = string.Empty;
            string targetDirectoryPath;

            var analysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);

            var errorEncountered = false;
            var failedFileCount = 0;

            const int retryCount = 10;
            const int retryHoldoffSeconds = 15;
            const bool increaseHoldoffOnEachRetry = true;

            try
            {
                m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.DELIVERING_RESULTS, 0);

                if (string.IsNullOrEmpty(m_ResFolderName))
                {
                    // Log this error to the database (the logger will also update the local log file)
                    LogErrorToDatabase("Results folder name is not defined, job " + Job);
                    m_message = "Results folder name is not defined";

                    // Without a source folder; there isn't much we can do
                    return false;
                }

                sourceFolderPath = Path.Combine(m_WorkDir, m_ResFolderName);

                // Verify the source folder exists
                if (!Directory.Exists(sourceFolderPath))
                {
                    // Log this error to the database
                    LogErrorToDatabase("Results folder not found, " + m_jobParams.GetJobStepDescription() + ", folder " + sourceFolderPath);
                    m_message = "Results folder not found: " + sourceFolderPath;

                    // Without a source folder; there isn't much we can do
                    return false;
                }

                // Determine the remote transfer folder path (create it if missing)
                targetDirectoryPath = CreateRemoteTransferFolder(analysisResults, transferFolderPath);
                if (string.IsNullOrEmpty(targetDirectoryPath))
                {
                    analysisResults.CopyFailedResultsToArchiveFolder(sourceFolderPath);
                    return false;
                }

            }
            catch (Exception ex)
            {
                LogError("Error creating results folder in transfer directory", ex);
                if (!string.IsNullOrEmpty(sourceFolderPath))
                {
                    analysisResults.CopyFailedResultsToArchiveFolder(sourceFolderPath);
                }

                return false;
            }

            // Copy results folder to xfer folder
            // Existing files will be overwritten if they exist in htFilesToOverwrite (with the assumption that the files created by this manager are newer, and thus supersede existing files)

            try
            {
                // Copy all of the files and subdirectories in the local result folder to the target folder

                // Copy the files and subdirectories
                var success = CopyResultsFolderRecursive(
                    sourceFolderPath, sourceFolderPath, targetDirectoryPath, analysisResults,
                    ref errorEncountered, ref failedFileCount, retryCount,
                    retryHoldoffSeconds, increaseHoldoffOnEachRetry);

                if (!success)
                    errorEncountered = true;

            }
            catch (Exception ex)
            {
                LogError("Error copying results folder to " + Path.GetPathRoot(targetDirectoryPath), ex);
                errorEncountered = true;
            }

            if (errorEncountered)
            {
                // Message will be of the form
                // Error copying 1 file to transfer folder
                // or
                // Error copying 3 files to transfer folder

                var msg = "Error copying " + failedFileCount +
                    clsGlobal.CheckPlural(failedFileCount, " file", " files") +
                    " to transfer folder";
                LogError(msg);
                analysisResults.CopyFailedResultsToArchiveFolder(sourceFolderPath);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Copies each of the files in the source folder to the target folder
        /// Uses CopyFileWithRetry to retry the copy up to retryCount times
        /// </summary>
        /// <param name="rootSourceFolderPath"></param>
        /// <param name="sourceFolderPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="analysisResults"></param>
        /// <param name="errorEncountered"></param>
        /// <param name="failedFileCount"></param>
        /// <param name="retryCount"></param>
        /// <param name="retryHoldoffSeconds"></param>
        /// <param name="increaseHoldoffOnEachRetry"></param>
        /// <returns></returns>
        private bool CopyResultsFolderRecursive(
            string rootSourceFolderPath,
            string sourceFolderPath,
            string targetDirectoryPath,
            clsAnalysisResults analysisResults,
            ref bool errorEncountered,
            ref int failedFileCount,
            int retryCount,
            int retryHoldoffSeconds,
            bool increaseHoldoffOnEachRetry)
        {

            var filesToOverwrite = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            try
            {

                if (analysisResults.FolderExistsWithRetry(targetDirectoryPath))
                {
                    // The target folder already exists

                    // Examine the files in the results folder to see if any of the files already exist in the transfer folder
                    // If they do, compare the file modification dates and post a warning if a file will be overwritten (because the file on the local computer is newer)
                    // However, if file sizes differ, replace the file

                    var resultFolder = new DirectoryInfo(sourceFolderPath);
                    foreach (var sourceFile in resultFolder.GetFiles())
                    {
                        if (!File.Exists(Path.Combine(targetDirectoryPath, sourceFile.Name)))
                            continue;

                        var targetFile = new FileInfo(Path.Combine(targetDirectoryPath, sourceFile.Name));

                        if (sourceFile.Length == targetFile.Length && sourceFile.LastWriteTimeUtc <= targetFile.LastWriteTimeUtc)
                            continue;

                        var message = "File in transfer folder on server will be overwritten by newer file in results folder: " + sourceFile.Name +
                                      "; new file date (UTC): " + sourceFile.LastWriteTimeUtc +
                                      "; old file date (UTC): " + targetFile.LastWriteTimeUtc;

                        // Log a warning, though not if the file is JobParameters_1394245.xml since we update that file after each job step
                        if (sourceFile.Name != clsAnalysisJob.JobParametersFilename(Job))
                        {
                            LogWarning(message);
                        }

                        if (!filesToOverwrite.Contains(sourceFile.Name))
                            filesToOverwrite.Add(sourceFile.Name);
                    }
                }
                else
                {
                    // Need to create the target folder
                    try
                    {
                        analysisResults.CreateFolderWithRetry(targetDirectoryPath);
                    }
                    catch (Exception ex)
                    {
                        LogError("Error creating results folder in transfer directory, " + Path.GetPathRoot(targetDirectoryPath), ex);
                        analysisResults.CopyFailedResultsToArchiveFolder(rootSourceFolderPath);
                        return false;
                    }
                }

            }
            catch (Exception ex)
            {
                LogError("Error comparing files in source folder to " + targetDirectoryPath, ex);
                analysisResults.CopyFailedResultsToArchiveFolder(rootSourceFolderPath);
                return false;
            }

            var sourceDirectory = new DirectoryInfo(sourceFolderPath);

            // Note: Entries in ResultFiles will have full file paths, not just file names
            var resultFiles = sourceDirectory.GetFiles("*");

            foreach (var fileToCopy in resultFiles)
            {
                var sourceFileName = fileToCopy.Name;

                var targetPath = Path.Combine(targetDirectoryPath, sourceFileName);

                try
                {
                    if (filesToOverwrite.Contains(sourceFileName))
                    {
                        // Copy file and overwrite existing
                        analysisResults.CopyFileWithRetry(fileToCopy.FullName, targetPath, true, retryCount, retryHoldoffSeconds, increaseHoldoffOnEachRetry);
                    }
                    else
                    {
                        // Copy file only if it doesn't currently exist
                        if (!File.Exists(targetPath))
                        {
                            analysisResults.CopyFileWithRetry(fileToCopy.FullName, targetPath, true, retryCount, retryHoldoffSeconds, increaseHoldoffOnEachRetry);
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Continue copying files; we'll fail the results at the end of this function
                    LogError(" CopyResultsFolderToServer: error copying " + fileToCopy.Name + " to " + targetPath, ex);
                    errorEncountered = true;
                    failedFileCount += 1;
                }
            }

            // Recursively call this function for each subdirectory
            // If any of the subdirectories have an error, we'll continue copying, but will set errorEncountered to True
            var success = true;

            foreach (var subDirectory in sourceDirectory.GetDirectories())
            {
                var targetDirectoryPathCurrent = Path.Combine(targetDirectoryPath, subDirectory.Name);

                success = CopyResultsFolderRecursive(rootSourceFolderPath, subDirectory.FullName, targetDirectoryPathCurrent, analysisResults,
                    ref errorEncountered, ref failedFileCount, retryCount, retryHoldoffSeconds, increaseHoldoffOnEachRetry);

                if (!success)
                    errorEncountered = true;

            }

            return success;

        }

        /// <summary>
        /// Make the local results directory, move files into that directory, then copy the files to the transfer directory on the Proto-x server
        /// </summary>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks>
        /// Uses MakeResultsFolder, MoveResultFiles, and CopyResultsFolderToServer
        /// Step tools can override this method if custom steps are required prior to packaging and transferring the results
        /// </remarks>
        public virtual bool CopyResultsToTransferDirectory(string transferFolderPathOverride = "")
        {
            if (clsGlobal.OfflineMode)
            {
                LogDebug("Offline mode is enabled; leaving results in the working directory: " + m_WorkDir);
                return true;
            }

            var success = MakeResultsFolder();
            if (!success)
            {
                // MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder";
                return false;
            }

            var moveSucceed = MoveResultFiles();
            if (!moveSucceed)
            {
                // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                m_message = "Error moving files into results folder";
                return false;
            }

            bool copySuccess;

            if (string.IsNullOrWhiteSpace(transferFolderPathOverride))
            {
                copySuccess = CopyResultsFolderToServer();
            }
            else
            {
                copySuccess = CopyResultsFolderToServer(transferFolderPathOverride);
            }

            return copySuccess;
        }

        /// <summary>
        /// Determines the path to the remote transfer folder
        /// Creates the directory if it does not exist
        /// </summary>
        /// <returns>The full path to the remote transfer folder; an empty string if an error</returns>
        /// <remarks></remarks>
        protected string CreateRemoteTransferFolder(clsAnalysisResults analysisResults)
        {

            var transferFolderPath = m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH);

            // Verify transfer directory exists
            // First make sure TransferFolderPath is defined
            if (string.IsNullOrEmpty(transferFolderPath))
            {
                var msg = "Transfer folder path not defined";
                LogError(msg, msg + "; job param 'transferFolderPath' is empty");
                return string.Empty;
            }

            return CreateRemoteTransferFolder(analysisResults, transferFolderPath);

        }

        /// <summary>
        /// Determines the path to the remote transfer folder
        /// Creates the directory if it does not exist
        /// </summary>
        /// <param name="analysisResults">Analysis results object</param>
        /// <param name="transferFolderPath">Base transfer folder path, e.g. \\proto-11\DMS3_Xfer\</param>
        /// <returns>The full path to the remote transfer folder; an empty string if an error</returns>
        protected string CreateRemoteTransferFolder(clsAnalysisResults analysisResults, string transferFolderPath)
        {

            if (string.IsNullOrEmpty(m_ResFolderName))
            {
                LogError("Results folder name is not defined, " + m_jobParams.GetJobStepDescription());
                m_message = "Results folder job parameter not defined (OutputFolderName)";
                return string.Empty;
            }

            // Verify that the transfer directory exists
            // If this is an Aggregation job, we create missing folders later in this method
            try
            {
                var folderExists = analysisResults.FolderExistsWithRetry(transferFolderPath);

                if (!folderExists && !clsGlobal.IsMatch(Dataset, "Aggregation"))
                {
                    LogError("Transfer directory not found: " + transferFolderPath);
                    return string.Empty;
                }

            }
            catch (Exception ex)
            {
                LogError("Error verifying transfer directory, " + Path.GetPathRoot(transferFolderPath), ex);
                return string.Empty;
            }

            // Determine if dataset directory in transfer directory already exists; make directory if it doesn't exist
            // First make sure "DatasetFolderName" or "DatasetNum" is defined
            if (string.IsNullOrEmpty(Dataset))
            {
                LogError("Dataset name is undefined, " + m_jobParams.GetJobStepDescription());
                m_message = "Dataset name is undefined";
                return string.Empty;
            }

            string remoteTransferFolderPath;

            if (clsGlobal.IsMatch(Dataset, "Aggregation"))
            {
                // Do not append "Aggregation" to the path since this is a generic dataset name applied to jobs that use Data Packages
                remoteTransferFolderPath = string.Copy(transferFolderPath);
            }
            else
            {
                // Append the dataset directory name to the transfer folder path
                var datasetFolderName = m_jobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME);
                if (string.IsNullOrWhiteSpace(datasetFolderName))
                    datasetFolderName = Dataset;
                remoteTransferFolderPath = Path.Combine(transferFolderPath, datasetFolderName);
            }

            // Create the target folder if it doesn't exist
            try
            {
                analysisResults.CreateFolderWithRetry(remoteTransferFolderPath, maxRetryCount: 5, retryHoldoffSeconds: 20, increaseHoldoffOnEachRetry: true);
            }
            catch (Exception ex)
            {
                LogError("Error creating dataset directory in transfer directory, " + Path.GetPathRoot(remoteTransferFolderPath), ex);
                return string.Empty;
            }

            // Now append the output folder name to remoteTransferFolderPath
            return Path.Combine(remoteTransferFolderPath, m_ResFolderName);

        }

        /// <summary>
        /// Makes up to 3 attempts to delete specified file
        /// </summary>
        /// <param name="FileNamePath">Full path to file for deletion</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Raises exception if error occurs</remarks>
        public bool DeleteFileWithRetries(string FileNamePath)
        {
            return DeleteFileWithRetries(FileNamePath, m_DebugLevel, 3);
        }

        /// <summary>
        /// Makes up to 3 attempts to delete specified file
        /// </summary>
        /// <param name="FileNamePath">Full path to file for deletion</param>
        /// <param name="debugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Raises exception if error occurs</remarks>
        public static bool DeleteFileWithRetries(string FileNamePath, int debugLevel)
        {
            return DeleteFileWithRetries(FileNamePath, debugLevel, 3);
        }

        /// <summary>
        /// Makes multiple tries to delete specified file
        /// </summary>
        /// <param name="fileNamePath">Full path to file for deletion</param>
        /// <param name="debugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
        /// <param name="maxRetryCount">Maximum number of deletion attempts</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Raises exception if error occurs</remarks>
        public static bool DeleteFileWithRetries(string fileNamePath, int debugLevel, int maxRetryCount)
        {

            var retryCount = 0;
            var errType = AMFileNotDeletedAfterRetryException.RetryExceptionType.IO_Exception;

            if (debugLevel > 4)
            {
                LogTools.LogDebug("clsAnalysisToolRunnerBase.DeleteFileWithRetries, executing method");
            }

            // Verify specified file exists
            if (!File.Exists(fileNamePath))
            {
                // Throw an exception
                throw new AMFileNotFoundException(fileNamePath, "Specified file not found");
            }

            while (retryCount < maxRetryCount)
            {
                try
                {
                    File.Delete(fileNamePath);
                    if (debugLevel > 4)
                    {
                        LogTools.LogDebug("clsAnalysisToolRunnerBase.DeleteFileWithRetries, normal exit");
                    }
                    return true;

                }
                catch (UnauthorizedAccessException ex1)
                {
                    // File may be read-only. Clear read-only flag and try again
                    if (debugLevel > 0)
                    {
                        LogTools.LogDebug("File " + fileNamePath + " exception ERR1: " + ex1.Message);
                        if (ex1.InnerException != null)
                        {
                            LogTools.LogDebug("Inner exception: " + ex1.InnerException.Message);
                        }
                        LogTools.LogDebug("File " + fileNamePath + " may be read-only, attribute reset attempt #" + retryCount);
                    }
                    File.SetAttributes(fileNamePath, File.GetAttributes(fileNamePath) & ~FileAttributes.ReadOnly);
                    errType = AMFileNotDeletedAfterRetryException.RetryExceptionType.Unauthorized_Access_Exception;
                    retryCount += 1;

                }
                catch (IOException ex2)
                {
                    // If problem is locked file, attempt to fix lock and retry
                    if (debugLevel > 0)
                    {
                        LogTools.LogDebug("File " + fileNamePath + " exception ERR2: " + ex2.Message);
                        if (ex2.InnerException != null)
                        {
                            LogTools.LogDebug("Inner exception: " + ex2.InnerException.Message);
                        }
                        LogTools.LogDebug("Error deleting file " + fileNamePath + ", attempt #" + retryCount);
                    }
                    errType = AMFileNotDeletedAfterRetryException.RetryExceptionType.IO_Exception;

                    // Delay 2 seconds
                    clsGlobal.IdleLoop(2);

                    // Do a garbage collection to assure file handles have been released
                    ProgRunner.GarbageCollectNow();
                    retryCount += 1;

                }
                catch (Exception ex3)
                {
                    var msg = "Error deleting file, exception ERR3 " + fileNamePath + ex3.Message;
                    LogTools.LogError(msg);
                    throw new AMFileNotDeletedException(fileNamePath, ex3.Message);
                }
            }

            // If we got to here, we've exceeded the max retry limit
            throw new AMFileNotDeletedAfterRetryException(fileNamePath, errType, "Unable to delete or move file after multiple retries");

        }

        /// <summary>
        /// Delete any instrument data files in the working directory
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Files to delete are determined via Job Parameter RawDataType</remarks>
        protected bool DeleteRawDataFiles()
        {
            var rawDataType = m_jobParams.GetParam("RawDataType");

            return DeleteRawDataFiles(rawDataType);
        }

        /// <summary>
        /// Delete any instrument data files in the working directory
        /// </summary>
        /// <param name="rawDataType">Raw data type string</param>
        /// <returns>True if success, false if an error</returns>
        protected bool DeleteRawDataFiles(string rawDataType)
        {
            var eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType);

            return DeleteRawDataFiles(eRawDataType);
        }

        /// <summary>
        /// Delete any instrument data files in the working directory
        /// </summary>
        /// <param name="eRawDataType">Raw data type enum</param>
        /// <returns>True if success, false if an error</returns>
        protected bool DeleteRawDataFiles(clsAnalysisResources.eRawDataTypeConstants eRawDataType)
        {

            // Deletes the raw data files/folders from the working directory
            bool isFile;
            var isNetworkDir = false;
            var fileOrFolderName = string.Empty;

            switch (eRawDataType)
            {
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                    fileOrFolderName = Path.Combine(m_WorkDir, Dataset + clsAnalysisResources.DOT_RAW_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile:
                    fileOrFolderName = Path.Combine(m_WorkDir, Dataset + clsAnalysisResources.DOT_WIFF_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.UIMF:
                    fileOrFolderName = Path.Combine(m_WorkDir, Dataset + clsAnalysisResources.DOT_UIMF_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzXML:
                    fileOrFolderName = Path.Combine(m_WorkDir, Dataset + clsAnalysisResources.DOT_MZXML_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzML:
                    fileOrFolderName = Path.Combine(m_WorkDir, Dataset + clsAnalysisResources.DOT_MZML_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                    fileOrFolderName = Path.Combine(m_WorkDir, Dataset + clsAnalysisResources.DOT_D_EXTENSION);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder:
                    fileOrFolderName = Path.Combine(m_WorkDir, Dataset + clsAnalysisResources.DOT_RAW_EXTENSION);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders:

                    var newSourceFolder = clsAnalysisResources.ResolveSerStoragePath(m_WorkDir);

                    // Check for "0.ser" folder
                    if (string.IsNullOrEmpty(newSourceFolder))
                    {
                        fileOrFolderName = Path.Combine(m_WorkDir, Dataset);
                        // isNetworkDir = false;
                    }
                    else
                    {
                        isNetworkDir = true;
                    }

                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                    // Bruker_FT folders are actually .D folders
                    fileOrFolderName = Path.Combine(m_WorkDir, Dataset + clsAnalysisResources.DOT_D_EXTENSION);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDISpot:
                    ////////////////////////////////////
                    // TODO: Finalize this code
                    //       DMS doesn't yet have a BrukerTOF dataset
                    //        so we don't know the official folder structure
                    ////////////////////////////////////

                    fileOrFolderName = Path.Combine(m_WorkDir, Dataset);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging:

                    ////////////////////////////////////
                    // TODO: Finalize this code
                    //       DMS doesn't yet have a BrukerTOF dataset
                    //        so we don't know the official folder structure
                    ////////////////////////////////////

                    fileOrFolderName = Path.Combine(m_WorkDir, Dataset);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:

                    // BrukerTOFBaf folders are actually .D folders
                    fileOrFolderName = Path.Combine(m_WorkDir, Dataset + clsAnalysisResources.DOT_D_EXTENSION);
                    isFile = false;

                    break;
                default:
                    // Should never get this value
                    m_message = "DeleteRawDataFiles, Invalid RawDataType specified: " + eRawDataType;
                    return false;
            }

            if (isFile)
            {
                // Data is a file, so use file deletion tools
                try
                {
                    if (!File.Exists(fileOrFolderName))
                    {
                        // File not found; treat this as a success
                        return true;
                    }

                    // DeleteFileWithRetries will throw an exception if it cannot delete any raw data files (e.g. the .UIMF file)
                    // Thus, need to wrap it with an Exception handler

                    if (DeleteFileWithRetries(fileOrFolderName))
                    {
                        return true;
                    }

                    LogError("Error deleting raw data file " + fileOrFolderName);
                    return false;
                }
                catch (Exception ex)
                {
                    LogError("Exception deleting raw data file " + fileOrFolderName, ex);
                    return false;
                }
            }

            if (isNetworkDir)
            {
                // The files were on the network and do not need to be deleted

            }
            else
            {
                // Use folder deletion tools
                try
                {
                    if (Directory.Exists(fileOrFolderName))
                    {
                        Directory.Delete(fileOrFolderName, true);
                    }
                    return true;
                }
                catch (Exception ex)
                {
                    LogError("Exception deleting raw data folder " + fileOrFolderName, ex);
                    return false;
                }
            }

            return true;

        }

        /// <summary>
        /// Delete the file if it exists; logging an error if the deletion fails
        /// </summary>
        /// <param name="filePath"></param>
        protected void DeleteTemporaryFile(string filePath)
        {
            try
            {
                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                }
            }
            catch (Exception ex)
            {
                LogMessage("Exception deleting temporary file " + filePath + ": " + ex.Message, 0, true);
            }

        }

        /// <summary>
        /// Determine the path to the correct version of the step tool
        /// </summary>
        /// <param name="progLocManagerParamName">The name of the manager parameter that defines the path to the directory with the exe, e.g. LCMSFeatureFinderProgLoc</param>
        /// <param name="exeName">The name of the exe file, e.g. LCMSFeatureFinder.exe</param>
        /// <returns>The path to the program, or an empty string if there is a problem</returns>
        /// <remarks>If the program is not found, m_message will be updated with the error message</remarks>
        protected string DetermineProgramLocation(string progLocManagerParamName, string exeName)
        {
            var progLoc = DetermineProgramLocation(m_mgrParams, m_jobParams, StepToolName, progLocManagerParamName, exeName, out var errorMessage);

            if (!string.IsNullOrEmpty(errorMessage))
            {
                // The error has already been logged, but we need to update m_message
                m_message = clsGlobal.AppendToComment(m_message, errorMessage);
            }

            return progLoc;
        }

        /// <summary>
        /// Determine the path to the correct version of the step tool
        /// </summary>
        /// <param name="mgrParams">Manager parameters</param>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="stepToolName">The name of the step tool, e.g. LCMSFeatureFinder</param>
        /// <param name="progLocManagerParamName">The name of the manager parameter that defines the path to the directory with the exe, e.g. LCMSFeatureFinderProgLoc</param>
        /// <param name="exeName">The name of the exe file, e.g. LCMSFeatureFinder.exe</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>The path to the program, or an empty string if there is a problem</returns>
        public static string DetermineProgramLocation(
            IMgrParams mgrParams, IJobParams jobParams,
            string stepToolName, string progLocManagerParamName, string exeName,
            out string errorMessage)
        {

            // Check whether the settings file specifies that a specific version of the step tool be used
            var stepToolVersion = jobParams.GetParam(stepToolName + "_Version");

            return DetermineProgramLocation(stepToolName, progLocManagerParamName, exeName, stepToolVersion, mgrParams, out errorMessage);
        }

        /// <summary>
        /// Determine the path to the correct version of the step tool
        /// </summary>
        /// <param name="stepToolName">The name of the step tool, e.g. LCMSFeatureFinder</param>
        /// <param name="progLocManagerParamName">The name of the manager parameter that defines the path to the directory with the exe, e.g. LCMSFeatureFinderProgLoc</param>
        /// <param name="exeName">The name of the exe file, e.g. LCMSFeatureFinder.exe</param>
        /// <param name="stepToolVersion">Specific step tool version to use (will be the name of a subdirectory located below the primary ProgLoc location)</param>
        /// <param name="mgrParams">Manager parameters</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>The path to the program, or an empty string if there is a problem</returns>
        public static string DetermineProgramLocation(
            string stepToolName,
            string progLocManagerParamName,
            string exeName,
            string stepToolVersion,
            IMgrParams mgrParams,
            out string errorMessage)
        {

            errorMessage = string.Empty;

            // Lookup the path to the directory that contains the Step tool
            var progLoc = mgrParams.GetParam(progLocManagerParamName);

            if (string.IsNullOrWhiteSpace(progLoc))
            {
                errorMessage = "Manager parameter " + progLocManagerParamName + " is not defined in the Manager Control DB";
                LogTools.LogError(errorMessage);
                return string.Empty;
            }

            // Check whether the settings file specifies that a specific version of the step tool be used

            if (!string.IsNullOrWhiteSpace(stepToolVersion))
            {
                // Specific version is defined; verify that the directory exists
                progLoc = Path.Combine(progLoc, stepToolVersion);

                if (!Directory.Exists(progLoc))
                {
                    errorMessage = "Version-specific folder not found for " + stepToolName;
                    LogTools.LogError(errorMessage + ": " + progLoc);
                    return string.Empty;
                }

                LogTools.LogMessage("Using specific version of " + stepToolName + ": " + progLoc);
            }

            // Define the path to the .Exe, then verify that it exists
            progLoc = Path.Combine(progLoc, exeName);

            if (!File.Exists(progLoc))
            {
                errorMessage = "Cannot find " + stepToolName + " program file " + exeName;
                LogTools.LogError(errorMessage + " at " + progLoc);
                return string.Empty;
            }

            return progLoc;

        }

        /// <summary>
        /// Gets the dictionary for the packed job parameter
        /// </summary>
        /// <param name="packedJobParameterName">Packaged job parameter name</param>
        /// <returns>List of strings</returns>
        /// <remarks>Data will have been stored by function clsAnalysisResources.StorePackedJobParameterDictionary</remarks>
        protected Dictionary<string, string> ExtractPackedJobParameterDictionary(string packedJobParameterName)
        {

            var dctData = new Dictionary<string, string>();

            var lstData = ExtractPackedJobParameterList(packedJobParameterName);

            foreach (var item in lstData)
            {
                var equalsIndex = item.LastIndexOf('=');
                if (equalsIndex > 0)
                {
                    var key = item.Substring(0, equalsIndex);
                    var value = item.Substring(equalsIndex + 1);

                    if (!dctData.ContainsKey(key))
                    {
                        dctData.Add(key, value);
                    }
                }
                else
                {
                    LogError("Packed dictionary item does not contain an equals sign: " + item);
                }
            }

            return dctData;

        }

        /// <summary>
        /// Gets the list of values for the packed job parameter
        /// </summary>
        /// <param name="packedJobParameterName">Packaged job parameter name</param>
        /// <returns>List of strings</returns>
        /// <remarks>Data will have been stored by function clsAnalysisResources.StorePackedJobParameterDictionary</remarks>
        protected List<string> ExtractPackedJobParameterList(string packedJobParameterName)
        {

            var list = m_jobParams.GetJobParameter(packedJobParameterName, string.Empty);

            if (string.IsNullOrEmpty(list))
            {
                return new List<string>();
            }

            // Split the list on tab characters
            return list.Split('\t').ToList();
        }

        /// <summary>
        /// Looks up the current debug level for the manager.  If the call to the server fails, m_DebugLevel will be left unchanged
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool GetCurrentMgrSettingsFromDB()
        {
            return GetCurrentMgrSettingsFromDB(0);
        }

        /// <summary>
        /// Looks up the current debug level for the manager.  If the call to the server fails, m_DebugLevel will be left unchanged
        /// </summary>
        /// <param name="updateIntervalSeconds">
        /// The minimum number of seconds between updates
        /// If fewer than updateIntervalSeconds seconds have elapsed since the last call to this function, no update will occur
        /// </param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool GetCurrentMgrSettingsFromDB(int updateIntervalSeconds)
        {
            return GetCurrentMgrSettingsFromDB(updateIntervalSeconds, m_mgrParams, ref m_DebugLevel);
        }

        /// <summary>
        /// Looks up the current debug level for the manager.  If the call to the server fails, DebugLevel will be left unchanged
        /// </summary>
        /// <param name="updateIntervalSeconds">Update interval, in seconds</param>
        /// <param name="objMgrParams">Manager params</param>
        /// <param name="debugLevel">Input/Output parameter: set to the current debug level, will be updated to the debug level in the manager control DB</param>
        /// <returns>True for success; False for error</returns>
        /// <remarks></remarks>
        public static bool GetCurrentMgrSettingsFromDB(int updateIntervalSeconds, IMgrParams objMgrParams, ref short debugLevel)
        {

            try
            {
                if (updateIntervalSeconds > 0 && DateTime.UtcNow.Subtract(mLastManagerSettingsUpdateTime).TotalSeconds < updateIntervalSeconds)
                {
                    return true;
                }
                mLastManagerSettingsUpdateTime = DateTime.UtcNow;

                if (debugLevel >= 5)
                {
                    LogTools.LogDebug("Updating manager settings from the Manager Control DB");
                }

                // Data Source=proteinseqs;Initial Catalog=manager_control
                var connectionString = objMgrParams.GetParam("MgrCnfgDbConnectStr");
                var managerName = objMgrParams.ManagerName;

                var newDebugLevel = GetManagerDebugLevel(connectionString, managerName, debugLevel, "GetCurrentMgrSettingsFromDB", 0);

                if (debugLevel > 0 && newDebugLevel != debugLevel)
                {
                    LogTools.LogDebug("Debug level changed from " + debugLevel + " to " + newDebugLevel);
                    debugLevel = newDebugLevel;
                }

                return true;

            }
            catch (Exception ex)
            {
                var errorMessage = "Exception getting current manager settings from the manager control DB";
                LogTools.LogError(errorMessage, ex);
            }

            return false;

        }

        static short GetManagerDebugLevel(string connectionString, string managerName, short currentDebugLevel, string callingFunction, int recursionLevel)
        {

            if (clsGlobal.OfflineMode)
            {
                return currentDebugLevel;
            }

            if (recursionLevel > 5)
            {
                return currentDebugLevel;
            }

            var sqlQuery =
                "SELECT ParameterName, ParameterValue " +
                "FROM V_MgrParams " +
                "WHERE ManagerName = '" + managerName + "' AND " + " ParameterName IN ('DebugLevel', 'MgrSettingGroupName')";

            var callingFunctions = clsGlobal.AppendToComment(callingFunction, "GetManagerDebugLevel");
            var success = clsGlobal.GetQueryResults(sqlQuery, connectionString, out var lstResults, callingFunctions);

            if (!success || lstResults.Count <= 0)
                return currentDebugLevel;

            foreach (var resultRow in lstResults)
            {
                var paramName = resultRow[0];
                var paramValue = resultRow[1];

                if (clsGlobal.IsMatch(paramName, "DebugLevel"))
                {
                    var debugLevel = short.Parse(paramValue);
                    return debugLevel;
                }

                if (clsGlobal.IsMatch(paramName, "MgrSettingGroupName"))
                {
                    // DebugLevel is defined by a manager settings group; repeat the query to V_MgrParams

                    var debugLevel = GetManagerDebugLevel(connectionString, paramValue, currentDebugLevel, callingFunction, recursionLevel + 1);
                    return debugLevel;
                }
            }

            return currentDebugLevel;

        }

        /// <summary>
        /// Determine the path to java.exe
        /// </summary>
        /// <returns>The path to the java.exe, or an empty string if the manager parameter is not defined or if java.exe does not exist</returns>
        /// <remarks></remarks>
        protected string GetJavaProgLoc()
        {

            var javaProgLoc = GetJavaProgLoc(m_mgrParams, out var errorMessage);

            if (!string.IsNullOrEmpty(javaProgLoc))
                return javaProgLoc;

            if (string.IsNullOrWhiteSpace(errorMessage))
                LogError("GetJavaProgLoc could not find Java");
            else
                LogError(errorMessage);

            return string.Empty;

        }

        /// <summary>
        /// Determine the path to java.exe
        /// </summary>
        /// <returns>The path to the java.exe, or an empty string if the manager parameter is not defined or if java.exe does not exist</returns>
        /// <remarks></remarks>
        public static string GetJavaProgLoc(IMgrParams mgrParams, out string errorMessage)
        {
            string paramName;

            if (clsGlobal.LinuxOS)
            {
                // On Linux, the Java location is tracked via manager parameter JavaLocLinux, loaded from file ManagerSettingsLocal.xml
                // For example: /usr/bin/java
                paramName = "JavaLocLinux";
            }
            else
            {
                // JavaLoc will typically be "C:\Program Files\Java\jre8\bin\Java.exe"
                paramName = "JavaLoc";
            }

            var javaProgLoc = mgrParams.GetParam(paramName);

            if (string.IsNullOrEmpty(javaProgLoc))
            {
                errorMessage = string.Format("Parameter '{0}' not defined for this manager", paramName);
                return string.Empty;
            }

            var javaProg = new FileInfo(javaProgLoc);

            if (javaProg.Exists)
            {
                errorMessage = string.Empty;
                return javaProgLoc;
            }

            errorMessage = "Cannot find Java: " + javaProgLoc;
            return string.Empty;
        }
        /// <summary>
        /// Returns the full path to the program to use for converting a dataset to a .mzXML file
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        protected string GetMSXmlGeneratorAppPath()
        {

            var strMSXmlGeneratorExe = GetMSXmlGeneratorExeName();

            string strMSXmlGeneratorAppPath;

            if (strMSXmlGeneratorExe.ToLower().Contains("readw"))
            {
                // ReadW
                // Note that msXmlGenerator will likely be ReAdW.exe
                strMSXmlGeneratorAppPath = DetermineProgramLocation("ReAdWProgLoc", strMSXmlGeneratorExe);

            }
            else if (strMSXmlGeneratorExe.ToLower().Contains("msconvert"))
            {
                // MSConvert
                var ProteoWizardDir = m_mgrParams.GetParam("ProteoWizardDir");
                // MSConvert.exe is stored in the ProteoWizard folder
                strMSXmlGeneratorAppPath = Path.Combine(ProteoWizardDir, strMSXmlGeneratorExe);

            }
            else
            {
                LogError("Invalid value for MSXMLGenerator; should be 'ReadW' or 'MSConvert'");
                strMSXmlGeneratorAppPath = string.Empty;
            }

            return strMSXmlGeneratorAppPath;

        }

        /// <summary>
        /// Returns the name of the .Exe to use to convert a dataset to a .mzXML file
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        protected string GetMSXmlGeneratorExeName()
        {
            // Determine the path to the XML Generator
            var strMSXmlGeneratorExe = m_jobParams.GetParam("MSXMLGenerator");
            // ReadW.exe or MSConvert.exe (code will assume ReadW.exe if an empty string)

            if (string.IsNullOrEmpty(strMSXmlGeneratorExe))
            {
                // Assume we're using MSConvert
                strMSXmlGeneratorExe = "MSConvert.exe";
            }

            return strMSXmlGeneratorExe;
        }

        /// <summary>
        /// Determines the directory that contains R.exe and Rcmd.exe (queries the registry)
        /// </summary>
        /// <returns>Folder path, e.g. C:\Program Files\R\R-3.2.2\bin\x64</returns>
        /// <remarks>This function is public because it is used by the Cyclops test harness program</remarks>
        public string GetRPathFromWindowsRegistry()
        {

            const string RCORE_SUBKEY = @"SOFTWARE\R-core";

            try
            {
                var regRCore = Microsoft.Win32.Registry.LocalMachine.OpenSubKey("SOFTWARE\\R-core");
                if (regRCore == null)
                {
                    // Local machine SOFTWARE\R-core not found; try current user
                    regRCore = Microsoft.Win32.Registry.CurrentUser.OpenSubKey("SOFTWARE\\R-core");
                    if (regRCore == null)
                    {
                        LogError("Windows Registry key'" + RCORE_SUBKEY + " not found in HKEY_LOCAL_MACHINE nor HKEY_CURRENT_USER");
                        return string.Empty;
                    }
                }

                var is64Bit = Environment.Is64BitProcess;
                var sRSubKey = is64Bit ? "R64" : "R";
                var regR = regRCore.OpenSubKey(sRSubKey);
                if (regR == null)
                {
                    LogError("Registry key is not found: " + RCORE_SUBKEY + "\\" + sRSubKey);
                    return string.Empty;
                }

                var currentVersionText = (string)regR.GetValue("Current Version");

                string bin;

                if (string.IsNullOrEmpty(currentVersionText))
                {
                    var noSubkeyMessage = "Unable to determine the R Path: " + RCORE_SUBKEY + "\\" + sRSubKey + " has no subkeys";

                    if (regR.SubKeyCount == 0)
                    {
                        LogError(noSubkeyMessage);
                        return string.Empty;
                    }

                    // Find the newest subkey
                    var subKeys = regR.GetSubKeyNames().ToList();
                    subKeys.Sort();
                    subKeys.Reverse();

                    var newestSubkey = subKeys.FirstOrDefault();

                    if (newestSubkey == null)
                    {
                        LogError(noSubkeyMessage);
                        return string.Empty;
                    }

                    var regRNewest = regR.OpenSubKey(newestSubkey);

                    if (regRNewest == null)
                    {
                        LogError(noSubkeyMessage);
                        return string.Empty;
                    }

                    var installPath = (string)regRNewest.GetValue("InstallPath");
                    if (string.IsNullOrEmpty(installPath))
                    {
                        LogError("Unable to determine the R Path: " + newestSubkey + " does not have key InstallPath");
                        return string.Empty;
                    }

                    bin = Path.Combine(installPath, "bin");

                }
                else
                {
                    var installPath = (string)regR.GetValue("InstallPath");
                    if (string.IsNullOrEmpty(installPath))
                    {
                        LogError("Unable to determine the R Path: " + RCORE_SUBKEY + "\\" + sRSubKey + " does not have key InstallPath");
                        return string.Empty;
                    }

                    bin = Path.Combine(installPath, "bin");

                    // If version is of the form "3.2.3" (for Major.Minor.Build)
                    // we can directly instantiate a new Version object from the string

                    // However, in 2016 R version "3.2.4 Revised" was released, and that
                    // string cannot be directly used to instantiate a new Version object

                    // The following checks for this and removes any non-numeric characters
                    // (though it requires that the Major version be an integer)

                    var versionParts = currentVersionText.Split('.');
                    var reconstructVersion = false;

                    Version currentVersion;

                    if (currentVersionText.Length <= 1)
                    {
                        currentVersion = new Version(currentVersionText);
                    }
                    else
                    {
                        var nonNumericChars = new Regex("[^0-9]+", RegexOptions.Compiled);

                        for (var i = 1; i <= versionParts.Length - 1; i++)
                        {
                            if (nonNumericChars.IsMatch(versionParts[i]))
                            {
                                versionParts[i] = nonNumericChars.Replace(versionParts[i], string.Empty);
                                reconstructVersion = true;
                            }
                        }

                        if (reconstructVersion)
                        {
                            currentVersion = new Version(string.Join(".", versionParts));
                        }
                        else
                        {
                            currentVersion = new Version(currentVersionText);
                        }

                    }

                    // Up to 2.11.x, DLLs are installed in R_HOME\bin
                    // From 2.12.0, DLLs are installed in either i386 or x64 (or both) below the bin folder
                    // The bin folder has an R.exe file but it does not have Rcmd.exe or R.dll
                    if (currentVersion < new Version(2, 12))
                    {
                        return bin;
                    }
                }

                if (is64Bit)
                {
                    return Path.Combine(bin, "x64");
                }

                return Path.Combine(bin, "i386");
            }
            catch (Exception ex)
            {
                LogError("Exception in GetRPathFromWindowsRegistry", ex);
                return string.Empty;
            }

        }

        /// <summary>
        /// Lookup the base transfer folder path
        /// </summary>
        /// <returns></returns>
        /// <remarks>For example, \\proto-7\DMS3_XFER\</remarks>
        protected string GetTransferFolderPath()
        {

            var transferFolderPath = m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH);

            if (string.IsNullOrEmpty(transferFolderPath))
            {
                LogError("Transfer folder path not defined; job param 'transferFolderPath' is empty");
                return string.Empty;
            }

            return transferFolderPath;

        }

        /// <summary>
        /// Gets the .zip file path to create when zipping a single file
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <returns></returns>
        public string GetZipFilePathForFile(string sourceFilePath)
        {
            return clsDotNetZipTools.GetZipFilePathForFile(sourceFilePath);
        }

        /// <summary>
        /// Decompresses the specified gzipped file
        /// Output folder is m_WorkDir
        /// </summary>
        /// <param name="gzipFilePath">File to decompress</param>
        /// <returns></returns>
        public bool GUnzipFile(string gzipFilePath)
        {
            return GUnzipFile(gzipFilePath, m_WorkDir);
        }

        /// <summary>
        /// Decompresses the specified gzipped file
        /// </summary>
        /// <param name="gzipFilePath">File to unzip</param>
        /// <param name="targetDirectory">Target directory for the extracted files</param>
        /// <returns></returns>
        public bool GUnzipFile(string gzipFilePath, string targetDirectory)
        {
            m_DotNetZipTools.DebugLevel = m_DebugLevel;

            // Note that m_DotNetZipTools logs error messages using LogTools
            return m_DotNetZipTools.GUnzipFile(gzipFilePath, targetDirectory);
        }

        /// <summary>
        /// Gzips sourceFilePath, creating a new file in the same folder, but with extension .gz appended to the name (e.g. Dataset.mzid.gz)
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="deleteSourceAfterZip">If True, will delete the file after zipping it</param>
        /// <returns>True if success; false if an error</returns>
        public bool GZipFile(string sourceFilePath, bool deleteSourceAfterZip)
        {
            m_DotNetZipTools.DebugLevel = m_DebugLevel;

            // Note that m_DotNetZipTools logs error messages using LogTools
            var success = m_DotNetZipTools.GZipFile(sourceFilePath, deleteSourceAfterZip);

            if (!success && m_DotNetZipTools.Message.ToLower().Contains("OutOfMemoryException".ToLower()))
            {
                m_NeedToAbortProcessing = true;
            }

            return success;

        }

        /// <summary>
        /// Gzips sourceFilePath, creating a new file in targetDirectoryPath; the file extension will be the original extension plus .gz
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="targetDirectoryPath">Output folder for the unzipped file</param>
        /// <param name="deleteSourceAfterZip">If True, will delete the file after zipping it</param>
        /// <returns>True if success; false if an error</returns>
        public bool GZipFile(string sourceFilePath, string targetDirectoryPath, bool deleteSourceAfterZip)
        {

            m_DotNetZipTools.DebugLevel = m_DebugLevel;

            // Note that m_DotNetZipTools logs error messages using LogTools
            var success = m_DotNetZipTools.GZipFile(sourceFilePath, targetDirectoryPath, deleteSourceAfterZip);

            if (!success && m_DotNetZipTools.Message.ToLower().Contains("OutOfMemoryException".ToLower()))
            {
                m_NeedToAbortProcessing = true;
            }

            return success;

        }

        /// <summary>
        /// GZip the given file
        /// </summary>
        /// <param name="fiResultFile">File to compress</param>
        /// <returns>FileInfo object of the new .gz file or null if an error</returns>
        /// <remarks>Deletes the original file after creating the .gz file</remarks>
        public FileInfo GZipFile(FileInfo fiResultFile)
        {
            return GZipFile(fiResultFile, true);
        }

        /// <summary>
        /// GZip the given file
        /// </summary>
        /// <param name="fiResultFile">File to compress</param>
        /// <param name="deleteSourceAfterZip">If True, will delete the file after zipping it</param>
        /// <returns>FileInfo object of the new .gz file or null if an error</returns>
        public FileInfo GZipFile(FileInfo fiResultFile, bool deleteSourceAfterZip)
        {

            try
            {
                var success = GZipFile(fiResultFile.FullName, true);

                if (!success)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("GZipFile returned false for " + fiResultFile.Name);
                    }
                    return null;
                }

                var fiGZippedFile = new FileInfo(fiResultFile.FullName + clsAnalysisResources.DOT_GZ_EXTENSION);
                if (!fiGZippedFile.Exists)
                {
                    LogError("GZip file was not created: " + fiGZippedFile.Name);
                    return null;
                }

                return fiGZippedFile;

            }
            catch (Exception ex)
            {
                LogError("Exception in GZipFile(fiResultFile As FileInfo)", ex);
                return null;
            }

        }

        /// <summary>
        /// Parse a thread count text value to determine the number of threads (cores) to use
        /// </summary>
        /// <param name="threadCountText">Can be "0" or "all" for all threads, or a number of threads, or "90%"</param>
        /// <param name="maxThreadsToAllow">Maximum number of cores to use (0 for all)</param>
        /// <returns>The core count to use</returns>
        /// <remarks>Core count will be a minimum of 1 and a maximum of Environment.ProcessorCount</remarks>
        public static int ParseThreadCount(string threadCountText, int maxThreadsToAllow)
        {

            var rePercentage = new Regex("([0-9.]+)%");

            if (string.IsNullOrWhiteSpace(threadCountText))
            {
                threadCountText = "all";
            }
            else
            {
                threadCountText = threadCountText.Trim();
            }

            var coresOnMachine = clsGlobal.GetCoreCount();

            int coreCount;

            if (threadCountText.StartsWith("all", StringComparison.OrdinalIgnoreCase))
            {
                coreCount = coresOnMachine;
            }
            else
            {
                var reMatch = rePercentage.Match(threadCountText);
                if (reMatch.Success)
                {
                    // Value is similar to 90%
                    // Convert to a double, then compute the number of cores to use
                    var coreCountPct = Convert.ToDouble(reMatch.Groups[1].Value);
                    coreCount = (int)Math.Round(coreCountPct / 100.0 * coresOnMachine);
                    if (coreCount < 1)
                        coreCount = 1;
                }
                else
                {
                    if (int.TryParse(threadCountText, out coreCount))
                    {
                        coreCount = 0;
                    }
                }
            }

            if (coreCount == 0)
                coreCount = coresOnMachine;
            if (coreCount > coresOnMachine)
                coreCount = coresOnMachine;

            if (maxThreadsToAllow > 0 && coreCount > maxThreadsToAllow)
            {
                coreCount = maxThreadsToAllow;
            }

            if (coreCount < 1)
                coreCount = 1;

            return coreCount;

        }

        /// <summary>
        /// Looks up dataset information for the data package associated with this analysis job
        /// </summary>
        /// <param name="dctDataPackageDatasets"></param>
        /// <returns>True if a data package is defined and it has datasets associated with it</returns>
        /// <remarks></remarks>
        protected bool LoadDataPackageDatasetInfo(out Dictionary<int, clsDataPackageDatasetInfo> dctDataPackageDatasets)
        {

            // Gigasax.DMS_Pipeline
            var connectionString = m_mgrParams.GetParam("BrokerConnectionString");

            var dataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1);

            if (dataPackageID < 0)
            {
                dctDataPackageDatasets = new Dictionary<int, clsDataPackageDatasetInfo>();
                return false;
            }

            return clsDataPackageInfoLoader.LoadDataPackageDatasetInfo(connectionString, dataPackageID, out dctDataPackageDatasets);
        }

        /// <summary>
        /// Lookup the Peptide Hit jobs associated with this analysis job; non-peptide hit jobs are returned via lstAdditionalJobs
        /// </summary>
        /// <param name="lstAdditionalJobs">Output: Non Peptide Hit jobs (e.g. DeconTools or MASIC)</param>
        /// <param name="errorMsg">Output: error message</param>
        /// <returns>Peptide Hit Jobs (e.g. MSGF+ or Sequest)</returns>
        /// <remarks>This method updates property NumberOfClonedSteps for the analysis jobs</remarks>
        protected List<clsDataPackageJobInfo> RetrieveDataPackagePeptideHitJobInfo(
            out List<clsDataPackageJobInfo> lstAdditionalJobs,
            out string errorMsg)
        {

            // Gigasax.DMS_Pipeline
            var connectionString = m_mgrParams.GetParam("BrokerConnectionString");

            var dataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1);

            if (dataPackageID < 0)
            {
                errorMsg = "Job parameter DataPackageID not defined";
                lstAdditionalJobs = new List<clsDataPackageJobInfo>();
                return new List<clsDataPackageJobInfo>();
            }

            return clsDataPackageInfoLoader.RetrieveDataPackagePeptideHitJobInfo(
                connectionString, dataPackageID, out lstAdditionalJobs, out errorMsg);
        }

        /// <summary>
        /// Loads the job settings file
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        /// <remarks></remarks>
        protected bool LoadSettingsFile()
        {
            var fileName = m_jobParams.GetParam("SettingsFileName");
            if (fileName != "na")
            {
                var filePath = Path.Combine(m_WorkDir, fileName);

                // XML tool LoadSettings returns True even if file is not found, so a separate check is required
                if (File.Exists(filePath))
                {
                    return m_settingsFileParams.LoadSettings(filePath);
                }

                // Settings file wasn't found
                return false;

            }

            // Settings file wasn't required
            return true;
        }

        /// <summary>
        /// Logs current progress to the log file at a given interval (track progress with m_progress)
        /// </summary>
        /// <param name="toolName"></param>
        /// <remarks>Longer log intervals when m_DebugLevel is 0 or 1; shorter intervals for 5</remarks>
        protected void LogProgress(string toolName)
        {
            int logIntervalMinutes;

            if (m_DebugLevel >= 5)
            {
                logIntervalMinutes = 1;
            }
            else if (m_DebugLevel >= 4)
            {
                logIntervalMinutes = 5;
            }
            else if (m_DebugLevel >= 3)
            {
                logIntervalMinutes = 15;
            }
            else if (m_DebugLevel >= 2)
            {
                logIntervalMinutes = 30;
            }
            else
            {
                logIntervalMinutes = 60;
            }

            LogProgress(toolName, logIntervalMinutes);
        }

        /// <summary>
        /// Logs m_progress to the log file at interval logIntervalMinutes (track progress with m_progress)
        /// </summary>
        /// <param name="toolName"></param>
        /// <param name="logIntervalMinutes"></param>
        /// <remarks>Calls GetCurrentMgrSettingsFromDB every 300 seconds</remarks>
        protected void LogProgress(string toolName, int logIntervalMinutes)
        {
            const int CONSOLE_PROGRESS_INTERVAL_MINUTES = 1;

            try
            {
                if (logIntervalMinutes < 1)
                    logIntervalMinutes = 1;

                // Log progress; example message:
                //    ... 14.1% complete for MSGF+, job 1635879
                var progressMessage = " ... " + m_progress.ToString("0.0") + "% complete for " + toolName + ", job " + Job;

                if (DateTime.UtcNow.Subtract(m_LastProgressConsoleTime).TotalMinutes >= CONSOLE_PROGRESS_INTERVAL_MINUTES)
                {
                    m_LastProgressConsoleTime = DateTime.UtcNow;
                    ConsoleMsgUtils.ShowDebug(progressMessage);
                }

                if (DateTime.UtcNow.Subtract(m_LastProgressWriteTime).TotalMinutes >= logIntervalMinutes)
                {
                    m_LastProgressWriteTime = DateTime.UtcNow;
                    LogDebug(progressMessage);
                }

                // Synchronize the stored Debug level with the value stored in the database
                const int MGR_SETTINGS_UPDATE_INTERVAL_SECONDS = 300;
                GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS);

            }
            catch (Exception)
            {
                // Ignore errors here
            }

        }

        /// <summary>
        /// Log a warning message in the manager's log file
        /// Also display it at console
        /// Optionally update m_EvalMessage
        /// </summary>
        /// <param name="warningMessage">Warning message</param>
        /// <param name="updateEvalMessage">When true, update m_EvalMessage</param>
        protected void LogWarning(string warningMessage, bool updateEvalMessage = false)
        {
            if (updateEvalMessage)
            {
                m_EvalMessage = clsGlobal.AppendToComment(m_EvalMessage, warningMessage);
            }
            base.LogWarning(warningMessage);
        }

        /// <summary>
        /// Creates a results folder after analysis complete
        /// </summary>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks></remarks>
        protected bool MakeResultsFolder()
        {

            m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.PACKAGING_RESULTS, 0);

            // Makes results folder and moves files into it

            // Log status
            LogMessage(m_MachName + ": Creating results folder, Job " + Job);
            var resFolderNamePath = Path.Combine(m_WorkDir, m_ResFolderName);

            // Make the results folder
            try
            {
                var resultsFolder = new DirectoryInfo(resFolderNamePath);
                if (!resultsFolder.Exists)
                    resultsFolder.Create();
            }
            catch (Exception ex)
            {
                // Log this error to the database
                LogError("Error making results folder, job " + Job, ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Makes results folder and moves files into it
        /// </summary>
        /// <returns></returns>
        protected bool MoveResultFiles()
        {

            const int REJECT_LOGGING_THRESHOLD = 10;
            const int ACCEPT_LOGGING_THRESHOLD = 50;
            const int LOG_LEVEL_REPORT_ACCEPT_OR_REJECT = 5;

            var resFolderNamePath = string.Empty;
            var currentFileName = string.Empty;

            var errorEncountered = false;

            // Move files into results folder
            try
            {
                m_StatusTools.UpdateAndWrite(
                    EnumMgrStatus.RUNNING,
                    EnumTaskStatus.RUNNING,
                    EnumTaskStatusDetail.PACKAGING_RESULTS, 0);

                resFolderNamePath = Path.Combine(m_WorkDir, m_ResFolderName);
                var dctRejectStats = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                var dctAcceptStats = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                // Log status
                if (m_DebugLevel >= 2)
                {
                    var logMessage = "Move Result Files to " + resFolderNamePath;
                    if (m_DebugLevel >= 3)
                    {
                        logMessage += "; ResultFilesToSkip contains " + m_jobParams.ResultFilesToSkip.Count + " entries" + "; " +
                            "ResultFileExtensionsToSkip contains " + m_jobParams.ResultFileExtensionsToSkip.Count + " entries" + "; " +
                            "ResultFilesToKeep contains " + m_jobParams.ResultFilesToKeep.Count + " entries";
                    }
                    LogMessage(logMessage, m_DebugLevel);
                }

                // Obtain a list of all files in the working directory
                // Ignore subdirectories
                var files = Directory.GetFiles(m_WorkDir, "*");

                // Check each file against m_jobParams.m_ResultFileExtensionsToSkip and m_jobParams.m_ResultFilesToKeep

                foreach (var tmpFileName in files)
                {
                    var okToMove = true;
                    currentFileName = tmpFileName;
                    var tmpFileNameLCase = Path.GetFileName(tmpFileName).ToLower();

                    // Check to see if the filename is defined in ResultFilesToSkip
                    // Note that entries in ResultFilesToSkip are not case sensitive since they were instantiated using SortedSet<string>(StringComparer.OrdinalIgnoreCase)
                    if (m_jobParams.ResultFilesToSkip.Contains(tmpFileNameLCase))
                    {
                        // File found in the ResultFilesToSkip list; do not move it
                        okToMove = false;
                    }

                    if (okToMove)
                    {
                        // Check to see if the file ends with an entry specified in m_ResultFileExtensionsToSkip
                        // Note that entries in m_ResultFileExtensionsToSkip can be extensions, or can even be partial file names, e.g. _peaks.txt
                        foreach (var ext in m_jobParams.ResultFileExtensionsToSkip)
                        {
                            if (tmpFileNameLCase.EndsWith(ext, StringComparison.OrdinalIgnoreCase))
                            {
                                okToMove = false;
                                break;
                            }
                        }
                    }

                    if (!okToMove)
                    {
                        // Check to see if the file is a result file that got captured as a non result file
                        if (m_jobParams.ResultFilesToKeep.Contains(tmpFileNameLCase))
                        {
                            okToMove = true;
                        }
                    }

                    if (okToMove && FileTools.IsVimSwapFile(tmpFileName))
                    {
                        // VIM swap file; skip it
                        okToMove = false;
                    }

                    // Look for invalid characters in the filename
                    // (Required because extract_msn.exe sometimes leaves files with names like "C3 90 68 C2" (ascii codes) in working directory)
                    // Note: now evaluating each character in the filename
                    if (okToMove)
                    {
                        foreach (var chChar in Path.GetFileName(tmpFileName))
                        {
                            var asciiValue = (int)chChar;
                            if (asciiValue <= 31 || asciiValue >= 128)
                            {
                                // Invalid character found
                                okToMove = false;
                                LogDebug(" MoveResultFiles: Accepted file:  " + tmpFileName);
                                break;
                            }
                        }
                    }
                    else
                    {
                        if (m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT)
                        {
                            var fileExtension = Path.GetExtension(tmpFileName);

                            if (dctRejectStats.TryGetValue(fileExtension, out var rejectCount))
                            {
                                dctRejectStats[fileExtension] = rejectCount + 1;
                            }
                            else
                            {
                                dctRejectStats.Add(fileExtension, 1);
                            }

                            // Only log the first 10 times files of a given extension are rejected
                            //  However, if a file was rejected due to invalid characters in the name, we don't track that rejection with dctRejectStats
                            if (dctRejectStats[fileExtension] <= REJECT_LOGGING_THRESHOLD)
                            {
                                LogDebug(" MoveResultFiles: Rejected file:  " + tmpFileName);
                            }
                        }
                    }

                    if (!okToMove)
                        continue;

                    // If valid file name, move file to results folder
                    if (m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT)
                    {
                        var fileExtension = Path.GetExtension(tmpFileName);

                        if (dctAcceptStats.TryGetValue(fileExtension, out var acceptCount))
                        {
                            dctAcceptStats[fileExtension] = acceptCount + 1;
                        }
                        else
                        {
                            dctAcceptStats.Add(fileExtension, 1);
                        }

                        // Only log the first 50 times files of a given extension are accepted
                        if (dctAcceptStats[fileExtension] <= ACCEPT_LOGGING_THRESHOLD)
                        {
                            LogDebug(" MoveResultFiles: Accepted file:  " + tmpFileName);
                        }
                    }

                    string targetFilePath = null;
                    try
                    {
                        targetFilePath = Path.Combine(resFolderNamePath, Path.GetFileName(tmpFileName));
                        File.Move(tmpFileName, targetFilePath);
                    }
                    catch (Exception)
                    {
                        try
                        {
                            if (!string.IsNullOrWhiteSpace(targetFilePath))
                            {
                                // Move failed
                                // Attempt to copy the file instead of moving the file
                                File.Copy(tmpFileName, targetFilePath, true);

                                // If we get here, the copy succeeded;
                                // The original file (in the work folder) will get deleted when the work folder is "cleaned" after the job finishes
                            }

                        }
                        catch (Exception ex2)
                        {
                            // Copy also failed
                            // Continue moving files; we'll fail the results at the end of this function
                            LogError(" MoveResultFiles: error moving/copying file: " + tmpFileName, ex2);
                            errorEncountered = true;
                        }
                    }

                }

                if (m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT)
                {
                    // Look for any extensions in dctAcceptStats that had over 50 accepted files
                    foreach (var extension in dctAcceptStats)
                    {
                        if (extension.Value > ACCEPT_LOGGING_THRESHOLD)
                        {
                            LogDebug(" MoveResultFiles: Accepted a total of " + extension.Value + " files with extension " + extension.Key);
                        }
                    }

                    // Look for any extensions in  dctRejectStats that had over 10 rejected files
                    foreach (var extension in dctRejectStats)
                    {
                        if (extension.Value > REJECT_LOGGING_THRESHOLD)
                        {
                            LogDebug(" MoveResultFiles: Rejected a total of " + extension.Value + " files with extension " + extension.Key);
                        }
                    }

                }

            }
            catch (Exception ex)
            {
                if (m_DebugLevel > 0)
                {
                    LogMessage("clsAnalysisToolRunnerBase.MoveResultFiles(); Error moving files to results folder", 0, true);
                    LogMessage("CurrentFile = " + currentFileName);
                    LogMessage("Results folder name = " + resFolderNamePath);
                }

                LogErrorToDatabase("Error moving results files, job " + Job + ex.Message);
                UpdateStatusMessage("Error moving results files");

                errorEncountered = true;
            }

            try
            {
                // Make the summary file
                OutputSummary(resFolderNamePath);
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            if (errorEncountered)
            {
                // Try to save whatever files were moved into the results folder
                var analysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
                analysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName));

                return false;
            }

            return true;
        }

        /// <summary>
        /// Notify the user that the settings file is missing a required parameter
        /// </summary>
        /// <param name="oJobParams"></param>
        /// <param name="parameterName"></param>
        /// <returns></returns>
        public static string NotifyMissingParameter(IJobParams oJobParams, string parameterName)
        {

            var settingsFile = oJobParams.GetJobParameter("SettingsFileName", "?UnknownSettingsFile?");
            var toolName = oJobParams.GetJobParameter("ToolName", "?UnknownToolName?");

            return "Settings file " + settingsFile + " for tool " + toolName + " does not have parameter " + parameterName + " defined";

        }

        /// <summary>
        /// Adds manager assembly data to job summary file
        /// </summary>
        /// <param name="OutputPath">Path to summary file</param>
        /// <remarks>Skipped if the debug level is less than 4</remarks>
        protected void OutputSummary(string OutputPath)
        {
            if (m_DebugLevel < 4)
            {
                // Do not create the AnalysisSummary file
                return;
            }

            // Saves the summary file in the results folder
            var assemblyTools = new clsAssemblyTools();

            assemblyTools.GetComponentFileVersionInfo(m_SummaryFile);

            var summaryFileName = m_jobParams.GetParam("StepTool") + "_AnalysisSummary.txt";

            if (!m_jobParams.ResultFilesToSkip.Contains(summaryFileName))
            {
                m_SummaryFile.SaveSummaryFile(Path.Combine(OutputPath, summaryFileName));
            }

        }

        /// <summary>
        /// Adds double quotes around a path if it contains a space
        /// </summary>
        /// <param name="path"></param>
        /// <returns>The path (updated if necessary)</returns>
        /// <remarks></remarks>
        public static string PossiblyQuotePath(string path)
        {
            return clsGlobal.PossiblyQuotePath(path);
        }

        /// <summary>
        /// Perform any required post processing after retrieving remote results
        /// </summary>
        /// <returns>CloseoutType enum representing completion status</returns>
        /// <remarks>
        /// Actual post-processing of remote results should only be required if the remote host running the job
        /// could not perform a step that requires database access or Windows share access.
        /// Override this method as required for specific step tools (however, still call base.PostProcessRemoteResults)
        /// </remarks>
        public virtual CloseOutType PostProcessRemoteResults()
        {
            var toolJobDescription = string.Format("remote tool {0}, job {1}", StepToolName, Job);

            var toolVersionInfoFile = new FileInfo(Path.Combine(m_WorkDir, ToolVersionInfoFile));
            if (!toolVersionInfoFile.Exists)
            {
                LogErrorNoMessageUpdate(
                    "ToolVersionInfo file not found for job " + m_JobNum +
                    "; PostProcessRemoteResults cannot store the tool version in the database", true);
            }
            else
            {
                using (var reader = new StreamReader(new FileStream(toolVersionInfoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    if (!reader.EndOfStream)
                    {
                        LogDebug("Storing tool version info in DB for " + toolJobDescription);

                        var toolVersionInfo = reader.ReadLine();
                        StoreToolVersionInDatabase(toolVersionInfo);
                    }
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Purge old server cache files
        /// </summary>
        /// <param name="cacheFolderPath"></param>
        public void PurgeOldServerCacheFiles(string cacheFolderPath)
        {
            // Value prior to December 2014: 3 TB
            // Value effective December 2014: 20 TB
            const int spaceUsageThresholdGB = 20000;
            PurgeOldServerCacheFiles(cacheFolderPath, spaceUsageThresholdGB);
        }

        /// <summary>
        /// Test method for PurgeOldServerCacheFiles
        /// </summary>
        /// <param name="cacheFolderPath"></param>
        /// <param name="spaceUsageThresholdGB"></param>
        public void PurgeOldServerCacheFilesTest(string cacheFolderPath, int spaceUsageThresholdGB)
        {
            if (cacheFolderPath.StartsWith(@"\\proto", StringComparison.OrdinalIgnoreCase))
            {
                if (!string.Equals(cacheFolderPath, @"\\proto-2\past\PurgeTest", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine(@"This function cannot be used with a \\Proto-x\ server");
                    return;
                }
            }
            PurgeOldServerCacheFiles(cacheFolderPath, spaceUsageThresholdGB);
        }

        /// <summary>
        /// Determines the space usage of data files in the cache folder, e.g. at \\proto-11\MSXML_Cache
        /// If usage is over spaceUsageThresholdGB, deletes the oldest files until usage falls below spaceUsageThresholdGB
        /// </summary>
        /// <param name="cacheFolderPath">Path to the file cache</param>
        /// <param name="spaceUsageThresholdGB">Maximum space usage, in GB (cannot be less than 1000 on Proto-x servers; 10 otherwise)</param>
        private void PurgeOldServerCacheFiles(string cacheFolderPath, int spaceUsageThresholdGB)
        {

            {

                var lstDataFiles = new List<KeyValuePair<DateTime, FileInfo>>();

                double dblTotalSizeMB = 0;

                double dblSizeDeletedMB = 0;
                var fileDeleteCount = 0;
                var fileDeleteErrorCount = 0;

                var dctErrorSummary = new Dictionary<string, int>();

                if (string.IsNullOrWhiteSpace(cacheFolderPath))
                {
                    throw new ArgumentOutOfRangeException(nameof(cacheFolderPath), "Cache folder path cannot be empty");
                }

                if (cacheFolderPath.StartsWith(@"\\proto-", StringComparison.OrdinalIgnoreCase))
                {
                    if (spaceUsageThresholdGB < 1000)
                        spaceUsageThresholdGB = 1000;
                }
                else
                {
                    if (spaceUsageThresholdGB < 10)
                        spaceUsageThresholdGB = 10;
                }

                try
                {
                    if (DateTime.UtcNow.Subtract(mLastCachedServerFilesPurgeCheck).TotalMinutes < CACHED_SERVER_FILES_PURGE_INTERVAL)
                    {
                        return;
                    }

                    var diCacheFolder = new DirectoryInfo(cacheFolderPath);

                    if (!diCacheFolder.Exists)
                    {
                        return;
                    }

                    // Look for a purge check file
                    var fiPurgeCheckFile = new FileInfo(Path.Combine(diCacheFolder.FullName, "PurgeCheckFile.txt"));
                    if (fiPurgeCheckFile.Exists)
                    {
                        if (DateTime.UtcNow.Subtract(fiPurgeCheckFile.LastWriteTimeUtc).TotalMinutes < CACHED_SERVER_FILES_PURGE_INTERVAL)
                        {
                            return;
                        }
                    }

                    // Create / update the purge check file
                    try
                    {
                        using (var swPurgeCheckFile = new StreamWriter(new FileStream(fiPurgeCheckFile.FullName, FileMode.Append, FileAccess.Write, FileShare.Read)))
                        {
                            swPurgeCheckFile.WriteLine(DateTime.Now.ToString(DATE_TIME_FORMAT) + " - " + m_MachName);
                        }

                    }
                    catch (Exception)
                    {
                        // Likely another manager tried to update the file at the same time
                        // Ignore the error and proceed to look for files to purge
                    }

                    mLastCachedServerFilesPurgeCheck = DateTime.UtcNow;

                    var dtLastProgress = DateTime.UtcNow;
                    LogMessage("Examining hashcheck files in folder " + diCacheFolder.FullName, 1);

                    // Make a list of all of the hashcheck files in diCacheFolder

                    foreach (var fiItem in diCacheFolder.GetFiles("*" + clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX, SearchOption.AllDirectories))
                    {
                        if (!fiItem.FullName.EndsWith(clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX, StringComparison.OrdinalIgnoreCase))
                            continue;

                        var dataFilePath = fiItem.FullName.Substring(0, fiItem.FullName.Length - clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX.Length);

                        var fiDataFile = new FileInfo(dataFilePath);

                        if (!fiDataFile.Exists)
                            continue;

                        try
                        {
                            lstDataFiles.Add(new KeyValuePair<DateTime, FileInfo>(fiDataFile.LastWriteTimeUtc, fiDataFile));

                            dblTotalSizeMB += clsGlobal.BytesToMB(fiDataFile.Length);
                        }
                        catch (Exception ex)
                        {
                            LogMessage("Exception adding to file list " + fiDataFile.Name + "; " + ex.Message, 0, true);
                        }

                        if (DateTime.UtcNow.Subtract(dtLastProgress).TotalSeconds >= 5)
                        {
                            dtLastProgress = DateTime.UtcNow;
                            LogMessage(string.Format(" ... {0:#,##0} files processed", lstDataFiles.Count));
                        }
                    }

                    if (dblTotalSizeMB / 1024.0 <= spaceUsageThresholdGB)
                    {
                        return;
                    }

                    // Purge files until the space usage falls below the threshold
                    // Start with the earliest file then work our way forward

                    // Keep track of the deleted file info using this list
                    var purgedFileLogEntries = new List<string>();

                    var fiPurgeLogFile = new FileInfo(Path.Combine(diCacheFolder.FullName, "PurgeLog_" + DateTime.Now.Year + ".txt"));
                    if (!fiPurgeLogFile.Exists)
                    {
                        // Create the purge log file and write the header line
                        try
                        {
                            using (var swPurgeLogFile = new StreamWriter(new FileStream(fiPurgeLogFile.FullName, FileMode.Append, FileAccess.Write, FileShare.Read)))
                            {
                                swPurgeLogFile.WriteLine(string.Join("\t", "Date", "Manager", "Size (MB)", "Modification_Date", "Path"));
                            }
                        }
                        catch (Exception)
                        {
                            // Likely another manager tried to create the file at the same time
                            // Ignore the error
                        }
                    }

                    var lstSortedFiles = (from item in lstDataFiles orderby item.Key select item);

                    foreach (var kvItem in lstSortedFiles)
                    {
                        try
                        {
                            var fileSizeMB = clsGlobal.BytesToMB(kvItem.Value.Length);

                            var hashcheckPath = kvItem.Value.FullName + clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX;
                            var fiHashCheckFile = new FileInfo(hashcheckPath);

                            dblTotalSizeMB -= fileSizeMB;

                            kvItem.Value.Delete();

                            // Keep track of the deleted file's details
                            purgedFileLogEntries.Add(string.Join("\t",
                                DateTime.Now.ToString(DATE_TIME_FORMAT),
                                m_MachName,
                                fileSizeMB.ToString("0.00"),
                                kvItem.Value.LastWriteTime.ToString(DATE_TIME_FORMAT),
                                kvItem.Value.FullName));

                            dblSizeDeletedMB += fileSizeMB;
                            fileDeleteCount += 1;

                            if (fiHashCheckFile.Exists)
                            {
                                fiHashCheckFile.Delete();
                            }

                        }
                        catch (Exception ex)
                        {
                            // Keep track of the number of times we have an exception
                            fileDeleteErrorCount += 1;

                            var exceptionName = ex.GetType().ToString();
                            if (dctErrorSummary.TryGetValue(exceptionName, out var occurrences))
                            {
                                dctErrorSummary[exceptionName] = occurrences + 1;
                            }
                            else
                            {
                                dctErrorSummary.Add(exceptionName, 1);
                            }

                        }

                        if (dblTotalSizeMB / 1024.0 < spaceUsageThresholdGB * 0.95)
                        {
                            break;
                        }
                    }

                    LogMessage("Deleted " + fileDeleteCount + " file(s) from " + cacheFolderPath + ", recovering " + dblSizeDeletedMB.ToString("0.0") + " MB in disk space");

                    if (fileDeleteErrorCount > 0)
                    {
                        LogMessage("Unable to delete " + fileDeleteErrorCount + " file(s) from " + cacheFolderPath, 0, true);
                        foreach (var kvItem in dctErrorSummary)
                        {
                            LogMessage("  " + kvItem.Key + ": " + kvItem.Value, 1, true);
                        }
                    }

                    if (purgedFileLogEntries.Count > 0)
                    {
                        // Log the info for each of the deleted files
                        try
                        {
                            using (var swPurgeLogFile = new StreamWriter(new FileStream(fiPurgeLogFile.FullName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)))
                            {
                                foreach (var purgedFileLogEntry in purgedFileLogEntries)
                                {
                                    swPurgeLogFile.WriteLine(purgedFileLogEntry);
                                }
                            }
                        }
                        catch (Exception)
                        {
                            // Likely another manager tried to create the file at the same time
                            // Ignore the error
                        }
                    }

                }
                catch (Exception ex)
                {
                    LogMessage("Error in PurgeOldServerCacheFiles: " + clsGlobal.GetExceptionStackTrace(ex), 0, true);
                }
            }

        }

        /// <summary>
        /// Updates the dataset name to the final folder name in the transferFolderPath job parameter
        /// Updates the transfer folder path to remove the final folder
        /// </summary>
        /// <remarks></remarks>
        protected void RedefineAggregationJobDatasetAndTransferFolder()
        {
            var transferFolderPath = m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH);
            var diTransferFolder = new DirectoryInfo(transferFolderPath);

            m_Dataset = diTransferFolder.Name;

            // ReSharper disable once JoinNullCheckWithUsage
            if (diTransferFolder.Parent == null)
            {
                throw new DirectoryNotFoundException("Unable to determine the parent folder of " + diTransferFolder.FullName);
            }

            transferFolderPath = diTransferFolder.Parent.FullName;
            m_jobParams.SetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH, transferFolderPath);

        }

        /// <summary>
        /// Extracts the contents of the Version= line in a Tool Version Info file
        /// </summary>
        /// <param name="dllFilePath"></param>
        /// <param name="versionInfoFilePath"></param>
        /// <param name="version"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool ReadVersionInfoFile(string dllFilePath, string versionInfoFilePath, out string version)
        {

            version = string.Empty;
            var success = false;

            try
            {
                if (!File.Exists(versionInfoFilePath))
                {
                    LogMessage("Version Info File not found: " + versionInfoFilePath, 0, true);
                    return false;
                }

                // Open versionInfoFilePath and read the Version= line
                using (var srInFile = new StreamReader(new FileStream(versionInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (!srInFile.EndOfStream)
                    {
                        var lineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(lineIn))
                        {
                            continue;
                        }

                        var equalsLoc = lineIn.IndexOf('=');

                        if (equalsLoc <= 0)
                            continue;

                        var key = lineIn.Substring(0, equalsLoc);
                        string value;

                        if (equalsLoc < lineIn.Length)
                        {
                            value = lineIn.Substring(equalsLoc + 1);
                        }
                        else
                        {
                            value = string.Empty;
                        }

                        switch (key.ToLower())
                        {
                            case "filename":
                                break;
                            case "path":
                                break;
                            case "version":
                                version = string.Copy(value);
                                if (string.IsNullOrWhiteSpace(version))
                                {
                                    LogMessage("Empty version line in Version Info file for " + Path.GetFileName(dllFilePath), 0, true);
                                    success = false;
                                }
                                else
                                {
                                    success = true;
                                }
                                break;
                            case "error":
                                LogMessage("Error reported by DLLVersionInspector for " + Path.GetFileName(dllFilePath) + ": " + value, 0, true);
                                success = false;
                                break;
                                // default:
                                // Ignore the line

                        }
                    }

                }

            }
            catch (Exception ex)
            {
                LogError("Error reading Version Info File for " + Path.GetFileName(dllFilePath), ex);
            }

            return success;

        }

        /// <summary>
        /// Deletes files in specified directory that have been previously flagged as not wanted in results folder
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>List of files to delete is tracked via m_jobParams.ServerFilesToDelete; must store full file paths in ServerFilesToDelete</remarks>
        public bool RemoveNonResultServerFiles()
        {

            var currentFile = "??";

            try
            {
                // Log status
                LogMessage("Remove Files from the storage server; " +
                    "ServerFilesToDelete contains " + m_jobParams.ServerFilesToDelete.Count + " entries", 2);

                foreach (var fileToDelete in m_jobParams.ServerFilesToDelete)
                {
                    currentFile = fileToDelete;

                    // Log file to be deleted
                    LogMessage("Deleting " + fileToDelete, 4);

                    if (File.Exists(fileToDelete))
                    {
                        // Verify file is not set to readonly, then delete it
                        File.SetAttributes(fileToDelete, File.GetAttributes(fileToDelete) & ~FileAttributes.ReadOnly);
                        File.Delete(fileToDelete);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("clsGlobal.RemoveNonResultServerFiles(), Error deleting file " + currentFile, ex);
                // Even if an exception occurred, return true since the results were already copied back to the server
                return true;
            }

            return true;

        }

        /// <summary>
        /// Replace an updated file
        /// </summary>
        /// <param name="originalFile"></param>
        /// <param name="updatedFile"></param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>First deletes the target file, then renames the original file to the updated file name</remarks>
        protected bool ReplaceUpdatedFile(FileInfo originalFile, FileInfo updatedFile)
        {

            try
            {
                var finalFilePath = originalFile.FullName;

                clsGlobal.IdleLoop(0.25);
                originalFile.Delete();

                clsGlobal.IdleLoop(0.25);
                updatedFile.MoveTo(finalFilePath);

                return true;
            }
            catch (Exception ex)
            {
                if (m_DebugLevel >= 1)
                {
                    LogError("Error in ReplaceUpdatedFile", ex);
                }

                return false;
            }

        }

        /// <summary>
        /// Change the log file back to the analysis manager log file
        /// </summary>
        protected void ResetLogFileNameToDefault()
        {
            var logFileName = m_mgrParams.GetParam("LogFileName");
            LogTools.ChangeLogFileBaseName(logFileName, appendDateToBaseName: true);
        }

        /// <summary>
        /// Retrieve results from a remote processing job; storing in the local working directory
        /// </summary>
        /// <param name="transferUtility">Remote transfer utility</param>
        /// <param name="verifyCopied">Log warnings if any files are missing.  When false, logs debug messages instead</param>
        /// <param name="retrievedFilePaths">Local paths of retrieved files</param>
        /// <returns>True on success, otherwise false</returns>
        /// <remarks>
        /// If successful, the calling procedure will typically next call
        /// PostProcessRemoteResults then CopyResultsToTransferDirectory
        /// </remarks>
        public virtual bool RetrieveRemoteResults(
            clsRemoteTransferUtility transferUtility,
            bool verifyCopied,
            out List<string> retrievedFilePaths)
        {
            throw new NotImplementedException("Plugin " + StepToolName + " must implement RetrieveRemoteResults to allow for remote processing");
        }

        /// <summary>
        /// Retrieve the specified files, verifying that each one was actually retrieved if verifyCopied is true
        /// </summary>
        /// <param name="transferUtility">Remote transfer utility</param>
        /// <param name="filesToRetrieve">Files to retrieve</param>
        /// <param name="verifyCopied">Log warnings if any files are missing.  When false, logs debug messages instead</param>
        /// <param name="retrievedFilePaths">Local paths of retrieved files</param>
        /// <returns>True on success, otherwise false</returns>
        protected bool RetrieveRemoteResults(
            clsRemoteTransferUtility transferUtility,
            List<string> filesToRetrieve,
            bool verifyCopied,
            out List<string> retrievedFilePaths)
        {
            retrievedFilePaths = new List<string>();

            try
            {

                var remoteSourceDirectory = transferUtility.RemoteJobStepWorkDirPath;
                var warnIfMissing = verifyCopied;

                transferUtility.CopyFilesFromRemote(remoteSourceDirectory, filesToRetrieve, m_WorkDir, false, warnIfMissing);

                // Verify that all files were retrieved
                foreach (var fileName in filesToRetrieve)
                {
                    var localFile = new FileInfo(Path.Combine(m_WorkDir, fileName));
                    if (localFile.Exists)
                    {
                        retrievedFilePaths.Add(localFile.FullName);
                        continue;
                    }

                    if (verifyCopied)
                        LogError("Required result file not found: " + fileName);
                }

                var paramFileName = m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_PARAMETER_FILE);
                var modDefsFile = new FileInfo(Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(paramFileName) + "_ModDefs.txt"));
                if (modDefsFile.Exists && modDefsFile.Length == 0)
                {
                    m_jobParams.AddResultFileToSkip(modDefsFile.Name);
                }

                if (filesToRetrieve.Count == retrievedFilePaths.Count || !verifyCopied)
                    return true;

                if (string.IsNullOrWhiteSpace(m_message))
                    LogError("Expected result files not found on " + transferUtility.RemoteHostName);

                return false;

            }
            catch (Exception ex)
            {
                LogError("Error in RetrieveRemoteResults", ex);
                return false;
            }
        }

        /// <summary>
        /// Runs the analysis tool
        /// Major work is performed by overrides
        /// </summary>
        /// <returns>CloseoutType enum representing completion status</returns>
        /// <remarks></remarks>
        public virtual CloseOutType RunTool()
        {

            // Synchronize the stored Debug level with the value stored in the database
            GetCurrentMgrSettingsFromDB();

            // Make log entry
            LogMessage(m_MachName + ": Starting analysis, job " + Job);

            // Start the job timer
            m_StartTime = DateTime.UtcNow;

            // Remainder of method is supplied by subclasses

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        /// <summary>
        /// Creates a Tool Version Info file
        /// </summary>
        /// <param name="folderPath"></param>
        /// <param name="toolVersionInfo"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool SaveToolVersionInfoFile(string folderPath, string toolVersionInfo)
        {

            try
            {
                var toolVersionFilePath = Path.Combine(folderPath, ToolVersionInfoFile);

                using (var swToolVersionFile = new StreamWriter(new FileStream(toolVersionFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {

                    swToolVersionFile.WriteLine("Date: " + DateTime.Now.ToString(DATE_TIME_FORMAT));
                    swToolVersionFile.WriteLine("Dataset: " + Dataset);
                    swToolVersionFile.WriteLine("Job: " + Job);
                    swToolVersionFile.WriteLine("Step: " + m_jobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step"));
                    swToolVersionFile.WriteLine("Tool: " + m_jobParams.GetParam("StepTool"));
                    swToolVersionFile.WriteLine("ToolVersionInfo:");

                    swToolVersionFile.WriteLine(toolVersionInfo.Replace("; ", Environment.NewLine));

                }

            }
            catch (Exception ex)
            {
                LogError("Exception saving tool version info", ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <param name="toolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        protected bool SetStepTaskToolVersion(string toolVersionInfo)
        {
            return SetStepTaskToolVersion(toolVersionInfo, new List<FileInfo>());
        }

        /// <summary>
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <param name="toolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <param name="toolFiles">FileSystemInfo list of program files related to the step tool</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        protected bool SetStepTaskToolVersion(string toolVersionInfo, List<FileInfo> toolFiles)
        {

            return SetStepTaskToolVersion(toolVersionInfo, toolFiles, true);
        }

        /// <summary>
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <param name="toolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <param name="toolFiles">FileSystemInfo list of program files related to the step tool</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        protected bool SetStepTaskToolVersion(string toolVersionInfo, IEnumerable<FileInfo> toolFiles)
        {

            return SetStepTaskToolVersion(toolVersionInfo, toolFiles, true);
        }

        /// <summary>
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <param name="toolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <param name="toolFiles">FileSystemInfo list of program files related to the step tool</param>
        /// <param name="saveToolVersionTextFile">If true, creates a text file with the tool version information</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        protected bool SetStepTaskToolVersion(string toolVersionInfo, IEnumerable<FileInfo> toolFiles, bool saveToolVersionTextFile)
        {

            var exeInfo = string.Empty;
            string toolVersionInfoCombined;

            if (toolFiles != null)
            {
                foreach (var toolFile in toolFiles)
                {
                    try
                    {
                        if (toolFile.Exists)
                        {
                            exeInfo = clsGlobal.AppendToComment(exeInfo, toolFile.Name + ": " + toolFile.LastWriteTime.ToString(DATE_TIME_FORMAT));
                            LogMessage("EXE Info: " + exeInfo, 2);
                        }
                        else
                        {
                            LogMessage("Warning: Tool file not found: " + toolFile.FullName);
                        }

                    }
                    catch (Exception ex)
                    {
                        LogError("Exception looking up tool version file info", ex);
                    }
                }
            }

            // Append the .Exe info to toolVersionInfo
            if (string.IsNullOrEmpty(exeInfo))
            {
                toolVersionInfoCombined = string.Copy(toolVersionInfo);
            }
            else
            {
                toolVersionInfoCombined = clsGlobal.AppendToComment(toolVersionInfo, exeInfo);
            }

            if (saveToolVersionTextFile)
            {
                SaveToolVersionInfoFile(m_WorkDir, toolVersionInfoCombined);
            }

            if (clsGlobal.OfflineMode)
                return true;

            var success = StoreToolVersionInDatabase(toolVersionInfoCombined);
            return success;
        }

        /// <summary>
        /// Sort a text file
        /// </summary>
        /// <param name="textFilePath">File to sort</param>
        /// <param name="sortedFilePath">File to write the sorted data to</param>
        /// <param name="hasHeaderLine">True if the source file has a header line</param>
        /// <returns></returns>
        protected bool SortTextFile(string textFilePath, string sortedFilePath, bool hasHeaderLine)
        {
            try
            {
                mLastSortUtilityProgress = DateTime.UtcNow;
                mSortUtilityErrorMessage = string.Empty;

                var sortUtility = new FlexibleFileSortUtility.TextFileSorter
                {
                    WorkingDirectoryPath = m_WorkDir,
                    HasHeaderLine = hasHeaderLine,
                    ColumnDelimiter = "\t",
                    MaxFileSizeMBForInMemorySort = FlexibleFileSortUtility.TextFileSorter.DEFAULT_IN_MEMORY_SORT_MAX_FILE_SIZE_MB,
                    ChunkSizeMB = FlexibleFileSortUtility.TextFileSorter.DEFAULT_CHUNK_SIZE_MB
                };

                sortUtility.ProgressUpdate += mSortUtility_ProgressChanged;
                sortUtility.ErrorEvent += mSortUtility_ErrorEvent;
                sortUtility.WarningEvent += mSortUtility_WarningEvent;
                sortUtility.StatusEvent += mSortUtility_MessageEvent;

                var success = sortUtility.SortFile(textFilePath, sortedFilePath);

                if (success)
                    return true;

                if (string.IsNullOrWhiteSpace(mSortUtilityErrorMessage))
                {
                    m_message = "Unknown error sorting " + Path.GetFileName(textFilePath);
                }
                else
                {
                    m_message = mSortUtilityErrorMessage;
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception in SortTextFile", ex);
                return false;
            }

        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <param name="progLoc">Path to the primary .exe or .DLL</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>This method is appropriate for plugins that call a .NET executable</remarks>
        public bool StoreDotNETToolVersionInfo(string progLoc)
        {
            return StoreDotNETToolVersionInfo(progLoc, new List<string>());
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <param name="progLoc">Path to the primary .exe or .DLL</param>
        /// <param name="additionalDLLs">Additional .NET DLLs to examine (either simply names or full paths)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>This method is appropriate for plugins that call a .NET executable</remarks>
        protected bool StoreDotNETToolVersionInfo(string progLoc, List<string> additionalDLLs)
        {

            var toolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var fiProgram = new FileInfo(progLoc);
            if (!fiProgram.Exists)
            {
                try
                {
                    return SetStepTaskToolVersion("Unknown", new List<FileInfo>(), saveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion", ex);
                    return false;
                }

            }

            // Lookup the version of the .NET program
            StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, fiProgram.FullName);

            // Store the path to the .exe or .dll in toolFiles
            var toolFiles = new List<FileInfo>
            {
                fiProgram
            };

            if (additionalDLLs != null)
            {
                // Add paths to key DLLs
                foreach (var dllNameOrPath in additionalDLLs)
                {
                    if (Path.IsPathRooted(dllNameOrPath) || dllNameOrPath.Contains(Path.DirectorySeparatorChar))
                    {
                        // Absolute or relative path; use as is
                        toolFiles.Add(new FileInfo(dllNameOrPath));
                        continue;
                    }

                    // Assume simply a filename
                    if (fiProgram.Directory == null)
                    {
                        // Unable to determine the directory path for fiProgram; this shouldn't happen
                        toolFiles.Add(new FileInfo(dllNameOrPath));
                    }
                    else
                    {
                        // Add it as a relative path to fiProgram
                        toolFiles.Add(new FileInfo(Path.Combine(fiProgram.Directory.FullName, dllNameOrPath)));
                    }
                }
            }

            try
            {
                var success = SetStepTaskToolVersion(toolVersionInfo, toolFiles, saveToolVersionTextFile: false);
                return success;
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }

        }

        private bool StoreToolVersionInDatabase(string toolVersionInfo)
        {

            // Setup for execution of the stored procedure
            var cmd = new SqlCommand
            {
                CommandType = CommandType.StoredProcedure,
                CommandText = SP_NAME_SET_TASK_TOOL_VERSION
            };

            cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
            cmd.Parameters.Add(new SqlParameter("@job", SqlDbType.Int)).Value = m_jobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0);
            cmd.Parameters.Add(new SqlParameter("@step", SqlDbType.Int)).Value = m_jobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step", 0);
            cmd.Parameters.Add(new SqlParameter("@ToolVersionInfo", SqlDbType.VarChar, 900)).Value = toolVersionInfo;

            var analysisTask = new clsAnalysisJob(m_mgrParams, m_DebugLevel);

            // Execute the stored procedure (retry the call up to 4 times)
            var resCode = analysisTask.PipelineDBProcedureExecutor.ExecuteSP(cmd, 4);

            if (resCode == 0)
            {
                return true;
            }

            LogMessage("Error " + resCode + " storing tool version in database for current processing step", 0, true);
            return false;
        }

        /// <summary>
        /// Uses Reflection to determine the version info for an assembly already loaded in memory
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="assemblyName">Assembly Name</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>Use StoreToolVersionInfoOneFile for DLLs not loaded in memory</remarks>
        protected bool StoreToolVersionInfoForLoadedAssembly(ref string toolVersionInfo, string assemblyName)
        {
            return StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, assemblyName, includeRevision: true);
        }

        /// <summary>
        /// Uses Reflection to determine the version info for an assembly already loaded in memory
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="assemblyName">Assembly Name</param>
        /// <param name="includeRevision">Set to True to include a version of the form 1.5.4821.24755; set to omit the revision, giving a version of the form 1.5.4821</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>Use StoreToolVersionInfoOneFile for DLLs not loaded in memory</remarks>
        protected bool StoreToolVersionInfoForLoadedAssembly(ref string toolVersionInfo, string assemblyName, bool includeRevision)
        {

            try
            {
                var assembly = System.Reflection.Assembly.Load(assemblyName).GetName();

                string nameAndVersion;
                if (includeRevision)
                {
                    nameAndVersion = assembly.Name + ", Version=" + assembly.Version;
                }
                else
                {
                    nameAndVersion = assembly.Name + ", Version=" + assembly.Version.Major + "." + assembly.Version.Minor + "." + assembly.Version.Build;
                }

                toolVersionInfo = clsGlobal.AppendToComment(toolVersionInfo, nameAndVersion);

            }
            catch (Exception ex)
            {
                LogError("Exception determining Assembly info for " + assemblyName, ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Determines the version info for a .NET DLL using reflection
        /// If reflection fails, uses System.Diagnostics.FileVersionInfo
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="dllFilePath">Path to the DLL</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        public bool StoreToolVersionInfoOneFile(ref string toolVersionInfo, string dllFilePath)
        {

            bool success;

            try
            {
                var ioFileInfo = new FileInfo(dllFilePath);

                if (!ioFileInfo.Exists)
                {
                    LogMessage("Warning: File not found by StoreToolVersionInfoOneFile: " + dllFilePath);
                    return false;

                }

                var assembly = System.Reflection.Assembly.LoadFrom(ioFileInfo.FullName);
                var assemblyName = assembly.GetName();

                var nameAndVersion = assemblyName.Name + ", Version=" + assemblyName.Version;
                toolVersionInfo = clsGlobal.AppendToComment(toolVersionInfo, nameAndVersion);

                success = true;
            }
            catch (BadImageFormatException)
            {
                // Most likely trying to read a 64-bit DLL (if this program is running as 32-bit)
                // Or, if this program is AnyCPU and running as 64-bit, the target DLL or Exe must be 32-bit

                // Instead try StoreToolVersionInfoOneFile32Bit or StoreToolVersionInfoOneFile64Bit

                // Use this when compiled as AnyCPU
                success = StoreToolVersionInfoOneFile32Bit(ref toolVersionInfo, dllFilePath);

                // Use this when compiled as 32-bit
                // success = StoreToolVersionInfoOneFile64Bit(toolVersionInfo, dllFilePath)

            }
            catch (Exception ex)
            {
                // If you get an exception regarding .NET 4.0 not being able to read a .NET 1.0 runtime, add these lines to the end of file AnalysisManagerProg.exe.config
                //  <startup useLegacyV2RuntimeActivationPolicy="true">
                //    <supportedRuntime version="v4.0" />
                //  </startup>
                LogError("Exception determining Assembly info for " + Path.GetFileName(dllFilePath), ex);
                success = false;
            }

            if (!success)
            {
                success = StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, dllFilePath);
            }

            return success;

        }

        /// <summary>
        /// Determines the version info for a .NET DLL using System.Diagnostics.FileVersionInfo
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="dllFilePath">Path to the DLL</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfoViaSystemDiagnostics(ref string toolVersionInfo, string dllFilePath)
        {

            try
            {
                var ioFileInfo = new FileInfo(dllFilePath);

                if (!ioFileInfo.Exists)
                {
                    m_message = "File not found by StoreToolVersionInfoViaSystemDiagnostics";
                    LogMessage(m_message + ": " + dllFilePath);
                    return false;
                }

                var oFileVersionInfo = FileVersionInfo.GetVersionInfo(dllFilePath);

                var name = oFileVersionInfo.FileDescription;
                if (string.IsNullOrEmpty(name))
                {
                    name = oFileVersionInfo.InternalName;
                }

                if (string.IsNullOrEmpty(name))
                {
                    name = oFileVersionInfo.FileName;
                }

                if (string.IsNullOrEmpty(name))
                {
                    name = ioFileInfo.Name;
                }

                var version = oFileVersionInfo.FileVersion;
                if (string.IsNullOrEmpty(version))
                {
                    version = oFileVersionInfo.ProductVersion;
                }

                if (string.IsNullOrEmpty(version))
                {
                    version = "??";
                }

                var nameAndVersion = name + ", Version=" + version;
                toolVersionInfo = clsGlobal.AppendToComment(toolVersionInfo, nameAndVersion);

                return true;

            }
            catch (Exception ex)
            {
                LogError("Exception determining File Version for " + Path.GetFileName(dllFilePath), ex);
                return false;
            }

        }

        /// <summary>
        /// Uses the DLLVersionInspector to determine the version of a 32-bit .NET DLL or .Exe
        /// </summary>
        /// <param name="toolVersionInfo"></param>
        /// <param name="dllFilePath"></param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfoOneFile32Bit(ref string toolVersionInfo, string dllFilePath)
        {
            return StoreToolVersionInfoOneFileUseExe(ref toolVersionInfo, dllFilePath, "DLLVersionInspector_x86.exe");
        }

        /// <summary>
        /// Uses the DLLVersionInspector to determine the version of a 64-bit .NET DLL or .Exe
        /// </summary>
        /// <param name="toolVersionInfo"></param>
        /// <param name="dllFilePath"></param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfoOneFile64Bit(ref string toolVersionInfo, string dllFilePath)
        {
            return StoreToolVersionInfoOneFileUseExe(ref toolVersionInfo, dllFilePath, "DLLVersionInspector_x64.exe");
        }

        /// <summary>
        /// Uses the specified DLLVersionInspector to determine the version of a .NET DLL or .Exe
        /// </summary>
        /// <param name="toolVersionInfo"></param>
        /// <param name="dllFilePath"></param>
        /// <param name="versionInspectorExeName">DLLVersionInspector_x86.exe or DLLVersionInspector_x64.exe</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfoOneFileUseExe(ref string toolVersionInfo, string dllFilePath, string versionInspectorExeName)
        {

            try
            {
                var appPath = Path.Combine(clsGlobal.GetAppFolderPath(), versionInspectorExeName);

                var ioFileInfo = new FileInfo(dllFilePath);

                if (!ioFileInfo.Exists)
                {
                    m_message = "File not found by StoreToolVersionInfoOneFileUseExe";
                    LogMessage(m_message + ": " + dllFilePath, 0, true);
                    return false;
                }

                if (!File.Exists(appPath))
                {
                    m_message = "DLLVersionInspector not found by StoreToolVersionInfoOneFileUseExe";
                    LogMessage(m_message + ": " + appPath, 0, true);
                    return false;
                }

                // Call DLLVersionInspector_x86.exe or DLLVersionInspector_x64.exe to determine the tool version

                var versionInfoFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(ioFileInfo.Name) + "_VersionInfo.txt");


                var args = PossiblyQuotePath(ioFileInfo.FullName) + " /O:" + PossiblyQuotePath(versionInfoFilePath);

                if (m_DebugLevel >= 3)
                {
                    LogDebug(appPath + " " + args);
                }

                var progRunner = new clsRunDosProgram(clsGlobal.GetAppFolderPath(), m_DebugLevel)
                {
                    CacheStandardOutput = false,
                    CreateNoWindow = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = false,
                    MonitorInterval = 250
                };
                RegisterEvents(progRunner);

                var success = progRunner.RunProgram(appPath, args, "DLLVersionInspector", false);

                if (!success)
                {
                    return false;
                }

                success = ReadVersionInfoFile(dllFilePath, versionInfoFilePath, out var version);

                // Delete the version info file
                try
                {
                    if (File.Exists(versionInfoFilePath))
                    {
                        File.Delete(versionInfoFilePath);
                    }
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                if (!success || string.IsNullOrWhiteSpace(version))
                {
                    return false;
                }

                toolVersionInfo = clsGlobal.AppendToComment(toolVersionInfo, version);

                return true;

            }
            catch (Exception ex)
            {
                var msg = "Exception determining Version info for " + Path.GetFileName(dllFilePath);
                LogError(msg, msg + " using " + versionInspectorExeName, ex);
            }

            return false;

        }

        /// <summary>
        /// Copies new/changed files from the source folder to the target folder
        /// </summary>
        /// <param name="sourceFolderPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool SynchronizeFolders(string sourceFolderPath, string targetDirectoryPath)
        {
            return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, "*");
        }

        /// <summary>
        /// Copies new/changed files from the source folder to the target folder
        /// </summary>
        /// <param name="sourceFolderPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="copySubdirectories">If true, recursively copies subdirectories</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool SynchronizeFolders(string sourceFolderPath, string targetDirectoryPath, bool copySubdirectories)
        {

            var lstFileNameFilterSpec = new List<string> { "*" };
            var lstFileNameExclusionSpec = new List<string>();
            const int maxRetryCount = 3;

            return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubdirectories);
        }

        /// <summary>
        /// Copies new/changed files from the source folder to the target folder
        /// </summary>
        /// <param name="sourceFolderPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="fileNameFilterSpec">Filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Will retry failed copies up to 3 times</remarks>
        protected bool SynchronizeFolders(string sourceFolderPath, string targetDirectoryPath, string fileNameFilterSpec)
        {

            var lstFileNameFilterSpec = new List<string> { fileNameFilterSpec };
            var lstFileNameExclusionSpec = new List<string>();
            const int maxRetryCount = 3;
            const bool copySubdirectories = false;

            return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubdirectories);
        }

        /// <summary>
        /// Copies new/changed files from the source folder to the target folder
        /// </summary>
        /// <param name="sourceFolderPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="lstFileNameFilterSpec">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Will retry failed copies up to 3 times</remarks>
        protected bool SynchronizeFolders(string sourceFolderPath, string targetDirectoryPath, List<string> lstFileNameFilterSpec)
        {

            var lstFileNameExclusionSpec = new List<string>();
            const int maxRetryCount = 3;
            const bool copySubdirectories = false;

            return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubdirectories);
        }

        /// <summary>
        /// Copies new/changed files from the source folder to the target folder
        /// </summary>
        /// <param name="sourceFolderPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="lstFileNameFilterSpec">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
        /// <param name="lstFileNameExclusionSpec">One or more filename filters for excluding files; can use * as a wildcard</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Will retry failed copies up to 3 times</remarks>
        protected bool SynchronizeFolders(string sourceFolderPath, string targetDirectoryPath, List<string> lstFileNameFilterSpec, List<string> lstFileNameExclusionSpec)
        {

            const int maxRetryCount = 3;
            const bool copySubdirectories = false;

            return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubdirectories);
        }

        /// <summary>
        /// Copies new/changed files from the source folder to the target folder
        /// </summary>
        /// <param name="sourceFolderPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="lstFileNameFilterSpec">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
        /// <param name="lstFileNameExclusionSpec">One or more filename filters for excluding files; can use * as a wildcard</param>
        /// <param name="maxRetryCount">Will retry failed copies up to maxRetryCount times; use 0 for no retries</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool SynchronizeFolders(string sourceFolderPath, string targetDirectoryPath, List<string> lstFileNameFilterSpec, List<string> lstFileNameExclusionSpec, int maxRetryCount)
        {

            const bool copySubdirectories = false;
            return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubdirectories);

        }

        /// <summary>
        /// Copies new/changed files from the source folder to the target folder
        /// </summary>
        /// <param name="sourceFolderPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="lstFileNameFilterSpec">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
        /// <param name="lstFileNameExclusionSpec">One or more filename filters for excluding files; can use * as a wildcard</param>
        /// <param name="maxRetryCount">Will retry failed copies up to maxRetryCount times; use 0 for no retries</param>
        /// <param name="copySubdirectories">If true, recursively copies subdirectories</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool SynchronizeFolders(
            string sourceFolderPath,
            string targetDirectoryPath,
            List<string> lstFileNameFilterSpec,
            List<string> lstFileNameExclusionSpec,
            int maxRetryCount,
            bool copySubdirectories)
        {
            try
            {
                var sourceDirectory = new DirectoryInfo(sourceFolderPath);
                var targetDirectory = new DirectoryInfo(targetDirectoryPath);

                if (!targetDirectory.Exists)
                {
                    targetDirectory.Create();
                }

                if (lstFileNameFilterSpec == null)
                {
                    lstFileNameFilterSpec = new List<string>();
                }

                if (lstFileNameFilterSpec.Count == 0)
                    lstFileNameFilterSpec.Add("*");

                var lstFilesToCopy = new SortedSet<string>();

                foreach (var filterSpec in lstFileNameFilterSpec)
                {
                    var filterSpecToUse = string.IsNullOrWhiteSpace(filterSpec) ? "*" : filterSpec;

                    foreach (var fiFile in sourceDirectory.GetFiles(filterSpecToUse))
                    {
                        if (!lstFilesToCopy.Contains(fiFile.Name))
                        {
                            lstFilesToCopy.Add(fiFile.Name);
                        }
                    }
                }

                if ((lstFileNameExclusionSpec != null) && lstFileNameExclusionSpec.Count > 0)
                {
                    // Remove any files from lstFilesToCopy that would get matched by items in lstFileNameExclusionSpec

                    foreach (var filterSpec in lstFileNameExclusionSpec)
                    {
                        if (string.IsNullOrWhiteSpace(filterSpec))
                            continue;

                        foreach (var fiFile in sourceDirectory.GetFiles(filterSpec))
                        {
                            if (lstFilesToCopy.Contains(fiFile.Name))
                            {
                                lstFilesToCopy.Remove(fiFile.Name);
                            }
                        }
                    }
                }

                foreach (var fileName in lstFilesToCopy)
                {
                    var fiSourceFile = new FileInfo(Path.Combine(sourceDirectory.FullName, fileName));
                    var fiTargetFile = new FileInfo(Path.Combine(targetDirectory.FullName, fileName));
                    var copyFile = false;

                    if (!fiTargetFile.Exists)
                    {
                        copyFile = true;
                    }
                    else if (fiTargetFile.Length != fiSourceFile.Length)
                    {
                        copyFile = true;
                    }
                    else if (fiTargetFile.LastWriteTimeUtc < fiSourceFile.LastWriteTimeUtc)
                    {
                        copyFile = true;
                    }

                    if (copyFile)
                    {
                        var retriesRemaining = maxRetryCount;

                        var success = false;
                        while (!success)
                        {
                            var startTime = DateTime.UtcNow;

                            success = m_FileTools.CopyFileUsingLocks(fiSourceFile, fiTargetFile.FullName, true);
                            if (success)
                            {
                                LogCopyStats(startTime, fiTargetFile.FullName);
                            }
                            else
                            {
                                retriesRemaining -= 1;
                                if (retriesRemaining < 0)
                                {
                                    m_message = "Error copying " + fiSourceFile.FullName + " to " + fiTargetFile.DirectoryName;
                                    return false;
                                }

                                LogMessage("Error copying " + fiSourceFile.FullName + " to " + fiTargetFile.DirectoryName + "; RetriesRemaining: " + retriesRemaining, 0, true);

                                // Wait 2 seconds then try again
                                clsGlobal.IdleLoop(2);
                            }
                        }

                    }
                }

                if (copySubdirectories)
                {
                    var subDirectories = sourceDirectory.GetDirectories();

                    foreach (var subDirectory in subDirectories)
                    {
                        var subDirectoryTargetPath = Path.Combine(targetDirectoryPath, subDirectory.Name);
                        var success = SynchronizeFolders(subDirectory.FullName, subDirectoryTargetPath,
                            lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubdirectories: true);

                        if (!success)
                        {
                            LogError("Error copying subdirectory " + subDirectory.FullName + " to " + targetDirectoryPath);
                            break;
                        }

                    }
                }

            }
            catch (Exception ex)
            {
                LogError("Error in SynchronizeFolders", ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Updates the analysis summary file
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        protected bool UpdateSummaryFile()
        {

            try
            {
                // Add a separator
                m_SummaryFile.Add(Environment.NewLine);
                m_SummaryFile.Add("=====================================================================================");
                m_SummaryFile.Add(Environment.NewLine);

                // Construct the Tool description (combination of Tool name and Step Tool name)
                var toolName = m_jobParams.GetParam("ToolName");
                var stepTool = m_jobParams.GetParam("StepTool");

                var toolAndStepTool = clsAnalysisJob.GetJobToolDescription(toolName, stepTool, string.Empty);

                // Add the data
                m_SummaryFile.Add("Job Number" + '\t' + Job);
                m_SummaryFile.Add("Job Step" + '\t' + m_jobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step"));
                m_SummaryFile.Add("Date" + '\t' + DateTime.Now);
                m_SummaryFile.Add("Processor" + '\t' + m_MachName);
                m_SummaryFile.Add("Tool" + '\t' + toolAndStepTool);
                m_SummaryFile.Add("Dataset Name" + '\t' + Dataset);
                m_SummaryFile.Add("Xfer Folder" + '\t' + m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH));
                m_SummaryFile.Add("Param File Name" + '\t' + m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_PARAMETER_FILE));
                m_SummaryFile.Add("Settings File Name" + '\t' + m_jobParams.GetParam("SettingsFileName"));
                m_SummaryFile.Add("Legacy Organism Db Name" + '\t' + m_jobParams.GetParam("LegacyFastaFileName"));
                m_SummaryFile.Add("Protein Collection List" + '\t' + m_jobParams.GetParam("ProteinCollectionList"));
                m_SummaryFile.Add("Protein Options List" + '\t' + m_jobParams.GetParam("ProteinOptions"));
                m_SummaryFile.Add("Fasta File Name" + '\t' + m_jobParams.GetParam("PeptideSearch", clsAnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

                if (m_StopTime < m_StartTime)
                    m_StopTime = DateTime.UtcNow;

                m_SummaryFile.Add("Analysis Time (hh:mm:ss)" + '\t' + CalcElapsedTime(m_StartTime, m_StopTime));

                // Add another separator
                m_SummaryFile.Add(Environment.NewLine);
                m_SummaryFile.Add("=====================================================================================");
                m_SummaryFile.Add(Environment.NewLine);

            }
            catch (Exception ex)
            {
                LogError("Error updating the summary file",
                         "Error updating the summary file, " + m_jobParams.GetJobStepDescription(), ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Unzips all files in the specified Zip file
        /// Output folder is m_WorkDir
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool UnzipFile(string zipFilePath)
        {
            return UnzipFile(zipFilePath, m_WorkDir, string.Empty);
        }

        /// <summary>
        /// Unzips all files in the specified Zip file
        /// Output folder is targetDirectory
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="targetDirectory">Target directory for the extracted files</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool UnzipFile(string zipFilePath, string targetDirectory)
        {
            return UnzipFile(zipFilePath, targetDirectory, string.Empty);
        }

        /// <summary>
        /// Unzips files in the specified Zip file that match the FileFilter spec
        /// Output folder is targetDirectory
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="targetDirectory">Target directory for the extracted files</param>
        /// <param name="FileFilter">FilterSpec to apply, for example *.txt</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool UnzipFile(string zipFilePath, string targetDirectory, string FileFilter)
        {
            m_DotNetZipTools.DebugLevel = m_DebugLevel;

            // Note that m_DotNetZipTools logs error messages using LogTools
            return m_DotNetZipTools.UnzipFile(zipFilePath, targetDirectory, FileFilter);

        }

        /// <summary>
        /// Reset the ProgRunner start time and the CPU usage queue
        /// </summary>
        /// <remarks>Public because used by clsDtaGenThermoRaw</remarks>
        public void ResetProgRunnerCpuUsage()
        {
            mProgRunnerStartTime = DateTime.UtcNow;
            mCoreUsageHistory.Clear();
        }

        /// <summary>
        /// Update the CPU usage by monitoring a process by name
        /// </summary>
        /// <param name="processName">Process name, for example chrome (do not include .exe)</param>
        /// <param name="secondsBetweenUpdates">Seconds between which this function is nominally called</param>
        /// <param name="defaultProcessID">Process ID to use if not match for processName</param>
        /// <returns>Actual CPU usage; -1 if an error</returns>
        /// <remarks>This method is used by clsAnalysisToolRunnerDtaRefinery to monitor X!Tandem and DTA_Refinery</remarks>
        protected float UpdateCpuUsageByProcessName(string processName, int secondsBetweenUpdates, int defaultProcessID)
        {
            try
            {
                var processID = defaultProcessID;

                var coreUsage = clsGlobal.ProcessInfo.GetCoreUsageByProcessName(processName, out var processIDs);

                if (processIDs.Count > 0)
                {
                    processID = processIDs.First();
                }

                if (coreUsage > -1)
                {
                    UpdateProgRunnerCpuUsage(processID, coreUsage, secondsBetweenUpdates);
                }

                return coreUsage;

            }
            catch (Exception ex)
            {
                LogError("Exception in UpdateCpuUsageByProcessName determining the processor usage of " + processName, ex);
                return -1;
            }

        }

        /// <summary>
        /// Update the evaluation code and evaluation message
        /// </summary>
        /// <param name="evalCode"></param>
        /// <param name="evalMsg"></param>
        public void UpdateEvalCode(int evalCode, string evalMsg)
        {
            m_EvalCode = evalCode;
            m_EvalMessage = evalMsg;
        }

        /// <summary>
        /// Cache the new core usage value
        /// Note: call ResetProgRunnerCpuUsage just before calling CmdRunner.RunProgram()
        /// </summary>
        /// <param name="processID">ProcessID of the externally running process</param>
        /// <param name="coreUsage">Number of cores in use by the process; -1 if unknown</param>
        /// <param name="secondsBetweenUpdates">Seconds between which this function is nominally called</param>
        /// <remarks>This method is used by this class and by clsAnalysisToolRunnerMODPlus</remarks>
        protected void UpdateProgRunnerCpuUsage(int processID, float coreUsage, int secondsBetweenUpdates)
        {
            // Cache the core usage values for the last 5 minutes
            if (coreUsage >= 0)
            {
                mCoreUsageHistory.Enqueue(new KeyValuePair<DateTime, float>(DateTime.Now, coreUsage));

                if (secondsBetweenUpdates < 10)
                {
                    if (mCoreUsageHistory.Count > 5 * 60 / 10.0)
                    {
                        mCoreUsageHistory.Dequeue();
                    }
                }
                else
                {
                    if (mCoreUsageHistory.Count > 5 * 60 / (float)secondsBetweenUpdates)
                    {
                        mCoreUsageHistory.Dequeue();
                    }
                }

            }

            if (mCoreUsageHistory.Count <= 0)
                return;

            m_StatusTools.ProgRunnerProcessID = processID;

            m_StatusTools.StoreCoreUsageHistory(mCoreUsageHistory);

            // If the Program has been running for at least 3 minutes, store the actual CoreUsage in the database
            if (DateTime.UtcNow.Subtract(mProgRunnerStartTime).TotalMinutes < 3)
                return;

            // Average the data in the history queue
            var coreUsageAvg = (from item in mCoreUsageHistory.ToArray() select item.Value).Average();

            m_StatusTools.ProgRunnerCoreUsage = coreUsageAvg;
        }

        /// <summary>
        /// Update the cached values in mCoreUsageHistory
        /// Note: call ResetProgRunnerCpuUsage just before calling CmdRunner.RunProgram()
        /// Then, when handling the LoopWaiting event from the cmdRunner instance
        /// call this method every secondsBetweenUpdates seconds (typically 30)
        /// </summary>
        /// <param name="cmdRunner">clsRunDosProgram instance used to run an external process</param>
        /// <param name="secondsBetweenUpdates">Seconds between which this function is nominally called</param>
        /// <remarks>Public because used by clsDtaGenThermoRaw</remarks>
        public void UpdateProgRunnerCpuUsage(clsRunDosProgram cmdRunner, int secondsBetweenUpdates)
        {
            try
            {
                // Note that the call to GetCoreUsage() will take at least 1 second
                var coreUsage = cmdRunner.GetCoreUsage();

                UpdateProgRunnerCpuUsage(cmdRunner.ProcessID, coreUsage, secondsBetweenUpdates);
            }
            catch (Exception ex)
            {
                // Log a warning since this is not a fatal error
                if (string.IsNullOrWhiteSpace(cmdRunner.ProcessPath))
                    LogWarning("Exception getting core usage for process ID " + cmdRunner.ProcessID + ": " + ex.Message);
                else
                {
                    var processName = Path.GetFileName(cmdRunner.ProcessPath);
                    LogWarning("Exception getting core usage for " + processName + ", process ID " + cmdRunner.ProcessID + ": " + ex.Message);
                }

            }
        }

        /// <summary>
        /// Update Status.xml every 15 seconds using m_progress
        /// </summary>
        /// <remarks></remarks>
        protected void UpdateStatusFile()
        {
            UpdateStatusFile(m_progress);
        }

        /// <summary>
        /// Update Status.xml every 15 seconds using sngPercentComplete
        /// </summary>
        /// <param name="sngPercentComplete">Percent complete</param>
        /// <remarks></remarks>
        protected void UpdateStatusFile(float sngPercentComplete)
        {
            var frequencySeconds = 15;
            UpdateStatusFile(sngPercentComplete, frequencySeconds);
        }

        /// <summary>
        /// Update Status.xml every frequencySeconds seconds using sngPercentComplete
        /// </summary>
        /// <param name="sngPercentComplete">Percent complete</param>
        /// <param name="frequencySeconds">Minimum time between updates, in seconds (must be at least 5)</param>
        /// <remarks></remarks>
        protected void UpdateStatusFile(float sngPercentComplete, int frequencySeconds)
        {
            if (frequencySeconds < 5)
                frequencySeconds = 5;

            // Update the status file (limit the updates to every x seconds)
            if (DateTime.UtcNow.Subtract(m_LastStatusFileUpdate).TotalSeconds >= frequencySeconds)
            {
                m_LastStatusFileUpdate = DateTime.UtcNow;
                UpdateStatusRunning(sngPercentComplete);
            }

        }

        /// <summary>
        /// Update m_message, which is logged in the pipeline job steps table when the job step finishes
        /// </summary>
        /// <param name="statusMessage">New status message</param>
        /// <param name="appendToExisting">True to append to m_message; false to overwrite it</param>
        /// <remarks>Text in m_message will be stored in the Completion_Message column in the database</remarks>
        protected void UpdateStatusMessage(string statusMessage, bool appendToExisting = false)
        {
            if (appendToExisting)
            {
                m_message = clsGlobal.AppendToComment(m_message, statusMessage);
            }
            else
            {
                m_message = statusMessage;
            }

            LogDebug(m_message);
        }

        /// <summary>
        /// Update Status.xml now using m_progress
        /// </summary>
        /// <remarks></remarks>
        protected void UpdateStatusRunning()
        {
            UpdateStatusRunning(m_progress);
        }

        /// <summary>
        /// Update Status.xml now using sngPercentComplete
        /// </summary>
        /// <param name="sngPercentComplete"></param>
        /// <remarks></remarks>
        protected void UpdateStatusRunning(float sngPercentComplete)
        {
            m_progress = sngPercentComplete;
            m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, 0, "", "", "", false);
        }

        /// <summary>
        /// Update Status.xml now using sngPercentComplete and spectrumCountTotal
        /// </summary>
        /// <param name="sngPercentComplete"></param>
        /// <param name="spectrumCountTotal"></param>
        /// <remarks></remarks>
        protected void UpdateStatusRunning(float sngPercentComplete, int spectrumCountTotal)
        {
            m_progress = sngPercentComplete;
            m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.RUNNING_TOOL, sngPercentComplete, spectrumCountTotal, "", "", "", false);
        }

        /// <summary>
        /// Make sure the _DTA.txt file exists and has at least one spectrum in it
        /// </summary>
        /// <returns>True if success; false if failure</returns>
        /// <remarks></remarks>
        protected bool ValidateCDTAFile()
        {
            var strDTAFilePath = Path.Combine(m_WorkDir, Dataset + clsAnalysisResources.CDTA_EXTENSION);

            return ValidateCDTAFile(strDTAFilePath);
        }

        /// <summary>
        /// Validate that a _dta.txt file is not empty
        /// </summary>
        /// <param name="strDTAFilePath"></param>
        /// <returns></returns>
        protected bool ValidateCDTAFile(string strDTAFilePath)
        {
            var dataFound = false;

            try
            {
                if (!File.Exists(strDTAFilePath))
                {
                    LogError("_DTA.txt file not found", strDTAFilePath);
                    return false;
                }

                using (var srReader = new StreamReader(new FileStream(strDTAFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (!srReader.EndOfStream)
                    {
                        var lineIn = srReader.ReadLine();

                        if (!string.IsNullOrWhiteSpace(lineIn))
                        {
                            dataFound = true;
                            break;
                        }
                    }

                }

                if (!dataFound)
                {
                    LogError("The _DTA.txt file is empty");
                }

            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateCDTAFile", ex);
                return false;
            }

            return dataFound;

        }

        /// <summary>
        /// Verifies that the zip file exists.
        /// If the file size is less than crcCheckThresholdGB, also performs a full CRC check of the data
        /// </summary>
        /// <param name="zipFilePath">Zip file to check</param>
        /// <param name="crcCheckThresholdGB">Threshold (in GB) below which a full CRC check should be performed</param>
        /// <returns>True if a valid zip file, otherwise false</returns>
        protected bool VerifyZipFile(string zipFilePath, float crcCheckThresholdGB = 4)
        {
            m_DotNetZipTools.DebugLevel = m_DebugLevel;

            // Note that m_DotNetZipTools logs error messages using LogTools
            var success = m_DotNetZipTools.VerifyZipFile(zipFilePath, crcCheckThresholdGB);

            return success;

        }

        /// <summary>
        /// Stores sourceFilePath in a zip file with the same name, but extension .zip
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="deleteSourceAfterZip">If True, will delete the file after zipping it</param>
        /// <returns>True if success; false if an error</returns>
        public bool ZipFile(string sourceFilePath, bool deleteSourceAfterZip)
        {
            m_DotNetZipTools.DebugLevel = m_DebugLevel;

            // Note that m_DotNetZipTools logs error messages using LogTools
            var success = m_DotNetZipTools.ZipFile(sourceFilePath, deleteSourceAfterZip);

            if (!success && m_DotNetZipTools.Message.ToLower().Contains("OutOfMemoryException".ToLower()))
            {
                m_NeedToAbortProcessing = true;
            }

            return success;

        }

        /// <summary>
        /// Compress a file using SharpZipLib
        /// </summary>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>IonicZip is faster, so we typically use function ZipFile</remarks>
        [Obsolete("Use ZipFile, which uses DotNetZip")]
        public bool ZipFileSharpZipLib(string sourceFilePath)
        {

            try
            {
                var fiSourceFile = new FileInfo(sourceFilePath);

                var zipFilePath = GetZipFilePathForFile(sourceFilePath);

                try
                {

                    if (File.Exists(zipFilePath))
                    {
                        if (m_DebugLevel >= 3)
                        {
                            LogDebug("Deleting target .zip file: " + zipFilePath);
                        }

                        File.Delete(zipFilePath);

                    }
                }
                catch (Exception ex)
                {
                    LogError("Error deleting target .zip file prior to zipping file " + sourceFilePath + " using SharpZipLib", ex);
                    return false;
                }

                var zipper = new ICSharpCode.SharpZipLib.Zip.FastZip();
                zipper.CreateZip(zipFilePath, fiSourceFile.DirectoryName, false, fiSourceFile.Name);

                // Verify that the zip file is not corrupt
                // Files less than 4 GB get a full CRC check
                // Large files get a quick check
                if (!VerifyZipFile(zipFilePath))
                {
                    return false;
                }

                return true;

            }
            catch (Exception ex)
            {
                LogError("Exception zipping " + sourceFilePath + " using SharpZipLib", ex);
                return false;
            }

        }

        /// <summary>
        /// Stores sourceFilePath in a zip file named zipFilePath
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="deleteSourceAfterZip">If True, will delete the file after zipping it</param>
        /// <param name="zipFilePath">Full path to the .zip file to be created.  Existing files will be overwritten</param>
        /// <returns>True if success; false if an error</returns>
        public bool ZipFile(string sourceFilePath, bool deleteSourceAfterZip, string zipFilePath)
        {
            m_DotNetZipTools.DebugLevel = m_DebugLevel;

            // Note that m_DotNetZipTools logs error messages using LogTools
            var success = m_DotNetZipTools.ZipFile(sourceFilePath, deleteSourceAfterZip, zipFilePath);

            if (!success && m_DotNetZipTools.Message.ToLower().Contains("OutOfMemoryException".ToLower()))
            {
                m_NeedToAbortProcessing = true;
            }

            return success;

        }

        /// <summary>
        /// Zip a file
        /// </summary>
        /// <param name="fiResultsFile"></param>
        /// <param name="fileDescription"></param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>The original file is not deleted, but the name is added to ResultFilesToSkip in m_jobParams</remarks>
        protected bool ZipOutputFile(FileInfo fiResultsFile, string fileDescription)
        {

            try
            {
                if (string.IsNullOrWhiteSpace(fileDescription))
                    fileDescription = "Unknown_Source";

                if (!ZipFile(fiResultsFile.FullName, false))
                {
                    LogError("Error zipping " + fileDescription + " results file");
                    return false;
                }

                // Add the unzipped file to .ResultFilesToSkip since we only want to keep the zipped version
                m_jobParams.AddResultFileToSkip(fiResultsFile.Name);

            }
            catch (Exception ex)
            {
                LogError("Exception zipping " + fileDescription + " results file", ex);
                return false;
            }

            return true;

        }

        #endregion

        #region "Event Handlers"

        private void mSortUtility_ErrorEvent(string message, Exception ex)
        {
            mSortUtilityErrorMessage = message;
            LogError("SortUtility: " + message, ex);
        }

        private void mSortUtility_MessageEvent(string message)
        {
            if (m_DebugLevel >= 1)
            {
                LogMessage(message);
            }
        }

        private void mSortUtility_ProgressChanged(string progressMessage, float percentComplete)
        {
            if (m_DebugLevel >= 1 && DateTime.UtcNow.Subtract(mLastSortUtilityProgress).TotalSeconds >= 5)
            {
                mLastSortUtilityProgress = DateTime.UtcNow;
                LogMessage(progressMessage + ": " + percentComplete.ToString("0.0") + "% complete");
            }
        }

        private void mSortUtility_WarningEvent(string message)
        {
            LogWarning("SortUtility: " + Message);
        }

        #endregion

    }

}