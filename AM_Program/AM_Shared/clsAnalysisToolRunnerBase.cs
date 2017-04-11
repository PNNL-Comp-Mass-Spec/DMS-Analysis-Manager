using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Text.RegularExpressions;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/19/2007
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{
    /// <summary>
    /// Base class for analysis tool runner
    /// </summary>
    public class clsAnalysisToolRunnerBase : clsAnalysisMgrBase, IToolRunner
    {

        #region "Constants"

        protected const string SP_NAME_SET_TASK_TOOL_VERSION = "SetStepTaskToolVersion";
        public const string DATE_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss tt";

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
        /// access to mgr parameters
        /// </summary>
        protected IMgrParams m_mgrParams;

        /// <summary>
        /// access to settings file parameters
        /// </summary>
        protected readonly PRISM.XmlSettingsFileAccessor m_settingsFileParams = new PRISM.XmlSettingsFileAccessor();

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
        /// Working directory, machine name (aka manager name), & job number (used frequently by subclasses)
        /// </summary>
        protected string m_WorkDir;
        protected string m_MachName;
        protected string m_JobNum;

        protected string m_Dataset;

        /// <summary>
        /// Elapsed time information
        /// </summary>
        protected DateTime m_StartTime;

        protected DateTime m_StopTime;

        /// <summary>
        /// Results folder name
        /// </summary>
        protected string m_ResFolderName;

        /// <summary>
        /// DLL file info
        /// </summary>
        protected string m_FileVersion;

        protected string m_FileDate;

        protected clsIonicZipTools m_IonicZipTools;

        protected bool m_NeedToAbortProcessing;

        protected clsSummaryFile m_SummaryFile;

        protected clsMyEMSLUtilities m_MyEMSLUtilities;
        private DateTime m_LastProgressWriteTime = DateTime.UtcNow;

        private DateTime m_LastProgressConsoleTime = DateTime.UtcNow;

        private DateTime m_LastStatusFileUpdate = DateTime.UtcNow;
        private DateTime mLastSortUtilityProgress;

        private string mSortUtilityErrorMessage;

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
        /// Evaluation code to be reported to the DMS_Pipeline DB
        /// </summary>
        public int EvalCode => m_EvalCode;

        /// <summary>
        /// Evaluation message to be reported to the DMS_Pipeline DB
        /// </summary>
        public string EvalMessage => m_EvalMessage;

        /// <summary>
        /// Publicly accessible results folder name and path
        /// </summary>
        public string ResFolderName => m_ResFolderName;

        /// <summary>
        /// Explanation of what happened to last operation this class performed
        /// </summary>
        public string Message => m_message;

        /// <summary>
        /// Set this to true if we need to abort processing as soon as possible due to a critical error
        /// </summary>
        public bool NeedToAbortProcessing => m_NeedToAbortProcessing;

        /// <summary>
        /// Progress of run (in percent)
        /// </summary>
        /// <remarks>This is a value between 0 and 100</remarks>
        public float Progress => m_progress;

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
        /// <param name="mgrParams">Object holding manager parameters</param>
        /// <param name="jobParams">Object holding job parameters</param>
        /// <param name="statusTools">Object for status reporting</param>
        /// <param name="summaryFile">Object for creating an analysis job summary file</param>
        /// <param name="myEMSLUtilities">MyEMSL download Utilities</param>
        /// <remarks></remarks>
        public virtual void Setup(IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsSummaryFile summaryFile, clsMyEMSLUtilities myEMSLUtilities)
        {
            m_mgrParams = mgrParams;
            m_jobParams = jobParams;
            m_StatusTools = statusTools;
            m_WorkDir = m_mgrParams.GetParam("workdir");
            m_MachName = m_mgrParams.GetParam("MgrName");
            m_JobNum = m_jobParams.GetParam("StepParameters", "Job");
            m_Dataset = m_jobParams.GetParam("JobParameters", "DatasetNum");

            m_MyEMSLUtilities = myEMSLUtilities ?? new clsMyEMSLUtilities(m_DebugLevel, m_WorkDir);

            RegisterEvents(m_MyEMSLUtilities);

            m_DebugLevel = (short)(m_mgrParams.GetParam("debuglevel", 1));
            m_StatusTools.Tool = m_jobParams.GetCurrentJobToolDescription();

            m_SummaryFile = summaryFile;

            m_ResFolderName = m_jobParams.GetParam("OutputFolderName");

            if (m_DebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerBase.Setup()");
            }

            m_IonicZipTools = new clsIonicZipTools(m_DebugLevel, m_WorkDir);
            RegisterEvents(m_IonicZipTools);

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
                LogDebug($"CalcElapsedTime: StartTime = {startTime}; Stoptime = {stopTime}");

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

            var maxThreadsToAllow = PRISMWin.clsProcessStats.GetCoreCount();

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
        /// Copies a file (typically a mzXML or mzML file) to a server cache folder
        /// Will store the file in a subfolder based on job parameter OutputFolderName, and below that, in a folder with a name like 2013_2
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
                var strDatasetStoragePath = m_jobParams.GetParam("JobParameters", "DatasetStoragePath");
                if (string.IsNullOrEmpty(strDatasetStoragePath))
                    strDatasetStoragePath = m_jobParams.GetParam("JobParameters", "DatasetArchivePath");

                var strDatasetYearQuarter = clsAnalysisResources.GetDatasetYearQuarter(strDatasetStoragePath);
                if (string.IsNullOrEmpty(strDatasetYearQuarter))
                {
                    LogError("Unable to determine DatasetYearQuarter using the DatasetStoragePath or DatasetArchivePath; cannot construct MSXmlCache path");
                    return string.Empty;
                }

                string remoteCacheFilePath;

                var success = CopyFileToServerCache(
                    cacheFolderPath, toolNameVersionFolder, sourceFilePath, strDatasetYearQuarter,
                    purgeOldFilesIfNeeded: purgeOldFilesIfNeeded, remoteCacheFilePath: out remoteCacheFilePath);

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
        /// Will store the file in the subfolder strSubfolderInTarget and, below that, in a folder with a name like 2013_2
        /// </summary>
        /// <param name="strCacheFolderPath">Cache folder base path, e.g. \\proto-6\MSXML_Cache</param>
        /// <param name="strSubfolderInTarget">Subfolder name to create below strCacheFolderPath (optional), e.g. MSXML_Gen_1_93 or MSConvert</param>
        /// <param name="strsourceFilePath">Path to the data file</param>
        /// <param name="strDatasetYearQuarter">
        /// Dataset year quarter text (optional)
        /// Example value is 2013_2; if this this parameter is blank, then will auto-determine using Job Parameter DatasetStoragePath
        /// </param>
        /// <param name="blnPurgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>
        /// Determines the Year_Quarter folder named using the DatasetStoragePath or DatasetArchivePath job parameter
        /// If those parameters are not defined, then copies the file anyway
        /// </remarks>
        protected bool CopyFileToServerCache(string strCacheFolderPath, string strSubfolderInTarget, string strsourceFilePath, string strDatasetYearQuarter, bool blnPurgeOldFilesIfNeeded)
        {
            string remoteCacheFilePath;
            return CopyFileToServerCache(strCacheFolderPath, strSubfolderInTarget, strsourceFilePath, strDatasetYearQuarter,
                blnPurgeOldFilesIfNeeded, out remoteCacheFilePath);

        }

        /// <summary>
        /// Copies a file (typically a mzXML or mzML file) to a server cache folder
        /// Will store the file in the subfolder strSubfolderInTarget and, below that, in a folder with a name like 2013_2
        /// </summary>
        /// <param name="cacheFolderPath">Cache folder base path, e.g. \\proto-11\MSXML_Cache</param>
        /// <param name="subfolderInTarget">Subfolder name to create below strCacheFolderPath (optional), e.g. MSXML_Gen_1_93 or MSConvert</param>
        /// <param name="sourceFilePath">Path to the data file</param>
        /// <param name="datasetYearQuarter">
        /// Dataset year quarter text (optional)
        /// Eexample value is 2013_2; if this this parameter is blank, then will auto-determine using Job Parameter DatasetStoragePath
        /// </param>
        /// <param name="purgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
        /// <param name="remoteCacheFilePath">Output parameter: the target file path (determined by this function)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>
        /// Determines the Year_Quarter folder named using the DatasetStoragePath or DatasetArchivePath job parameter
        /// If those parameters are not defined, then copies the file anyway
        /// </remarks>
        protected bool CopyFileToServerCache(
            string cacheFolderPath,
            string subfolderInTarget, string
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

                DirectoryInfo ditargetDirectory;

                // Define the target folder
                if (string.IsNullOrEmpty(subfolderInTarget))
                {
                    ditargetDirectory = diCacheFolder;
                }
                else
                {
                    ditargetDirectory = new DirectoryInfo(Path.Combine(diCacheFolder.FullName, subfolderInTarget));
                    if (!ditargetDirectory.Exists)
                        ditargetDirectory.Create();
                }

                if (string.IsNullOrEmpty(datasetYearQuarter))
                {
                    // Determine the year_quarter text for this dataset
                    var strDatasetStoragePath = m_jobParams.GetParam("JobParameters", "DatasetStoragePath");
                    if (string.IsNullOrEmpty(strDatasetStoragePath))
                        strDatasetStoragePath = m_jobParams.GetParam("JobParameters", "DatasetArchivePath");

                    datasetYearQuarter = clsAnalysisResources.GetDatasetYearQuarter(strDatasetStoragePath);
                }

                if (!string.IsNullOrEmpty(datasetYearQuarter))
                {
                    ditargetDirectory = new DirectoryInfo(Path.Combine(ditargetDirectory.FullName, datasetYearQuarter));
                    if (!ditargetDirectory.Exists)
                        ditargetDirectory.Create();
                }

                m_jobParams.AddResultFileExtensionToSkip(clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX);

                // Create the .hashcheck file
                var strHashcheckFilePath = clsGlobal.CreateHashcheckFile(sourceFilePath, blnComputeMD5Hash: true);

                if (string.IsNullOrEmpty(strHashcheckFilePath))
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

                var fiTargetFile = new FileInfo(Path.Combine(ditargetDirectory.FullName, sourceFileName));

                ResetTimestampForQueueWaitTimeLogging();
                var success = m_FileTools.CopyFileUsingLocks(sourceFilePath, fiTargetFile.FullName, m_MachName, true);

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
                m_FileTools.CopyFile(strHashcheckFilePath, Path.Combine(fiTargetFile.DirectoryName, Path.GetFileName(strHashcheckFilePath)), true);

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
        /// <param name="strsourceFilePath"></param>
        /// <param name="strDatasetYearQuarter">Dataset year quarter text, e.g. 2013_2;  if this this parameter is blank, then will auto-determine using Job Parameter DatasetStoragePath</param>
        /// <param name="strMSXmlGeneratorName">Name of the MzXML generator, e.g. MSConvert</param>
        /// <param name="blnPurgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>
        /// Contrast with CopyMSXmlToCache in clsAnalysisToolRunnerMSXMLGen, where the target folder is
        /// of the form \\proto-6\MSXML_Cache\MSConvert\MSXML_Gen_1_93
        /// </remarks>
        protected bool CopyMzXMLFileToServerCache(string strsourceFilePath, string strDatasetYearQuarter, string strMSXmlGeneratorName, bool blnPurgeOldFilesIfNeeded)
        {

            try
            {
                var strMSXMLCacheFolderPath = m_mgrParams.GetParam("MSXMLCacheFolderPath", string.Empty);

                if (string.IsNullOrEmpty(strMSXmlGeneratorName))
                {
                    strMSXmlGeneratorName = m_jobParams.GetJobParameter("MSXMLGenerator", string.Empty);

                    if (!string.IsNullOrEmpty(strMSXmlGeneratorName))
                    {
                        strMSXmlGeneratorName = Path.GetFileNameWithoutExtension(strMSXmlGeneratorName);
                    }
                }

                var success = CopyFileToServerCache(strMSXMLCacheFolderPath, strMSXmlGeneratorName, strsourceFilePath, strDatasetYearQuarter, blnPurgeOldFilesIfNeeded);
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
        /// <returns>CloseOutType.CLOSEOUT_SUCCESS on success</returns>
        /// <remarks></remarks>
        protected CloseOutType CopyResultsFolderToServer()
        {

            var transferFolderPath = GetTransferFolderPath();

            if (string.IsNullOrEmpty(transferFolderPath))
            {
                // Error has already geen logged and m_message has been updated
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CopyResultsFolderToServer(transferFolderPath);
        }

        /// <summary>
        /// Copies the files from the results folder to the transfer folder on the server
        /// </summary>
        /// <param name="transferFolderPath">Base transfer folder path to use
        /// e.g. \\proto-6\DMS3_Xfer\ or
        /// \\protoapps\PeptideAtlas_Staging\1000_DataPackageName</param>
        /// <returns>CloseOutType.CLOSEOUT_SUCCESS on success</returns>
        /// <remarks></remarks>
        protected CloseOutType CopyResultsFolderToServer(string transferFolderPath)
        {

            var sourceFolderPath = string.Empty;
            string targetDirectoryPath;

            var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);

            var blnErrorEncountered = false;
            var intFailedFileCount = 0;

            const int intRetryCount = 10;
            const int intRetryHoldoffSeconds = 15;
            const bool blnIncreaseHoldoffOnEachRetry = true;

            try
            {
                m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.DELIVERING_RESULTS, 0);

                if (string.IsNullOrEmpty(m_ResFolderName))
                {
                    // Log this error to the database (the logger will also update the local log file)
                    LogErrorToDatabase("Results folder name is not defined, job " + m_jobParams.GetParam("StepParameters", "Job"));
                    m_message = "Results folder name is not defined";

                    // Without a source folder; there isn't much we can do
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                sourceFolderPath = Path.Combine(m_WorkDir, m_ResFolderName);

                // Verify the source folder exists
                if (!Directory.Exists(sourceFolderPath))
                {
                    // Log this error to the database
                    LogErrorToDatabase("Results folder not found, job " + m_jobParams.GetParam("StepParameters", "Job") + ", folder " + sourceFolderPath);
                    m_message = "Results folder not found: " + sourceFolderPath;

                    // Without a source folder; there isn't much we can do
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the remote transfer folder path (create it if missing)
                targetDirectoryPath = CreateRemoteTransferFolder(objAnalysisResults, transferFolderPath);
                if (string.IsNullOrEmpty(targetDirectoryPath))
                {
                    objAnalysisResults.CopyFailedResultsToArchiveFolder(sourceFolderPath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

            }
            catch (Exception ex)
            {
                LogError("Error creating results folder in transfer directory", ex);
                m_message = clsGlobal.AppendToComment(m_message, "Error creating dataset folder in transfer directory");
                if (!string.IsNullOrEmpty(sourceFolderPath))
                {
                    objAnalysisResults.CopyFailedResultsToArchiveFolder(sourceFolderPath);
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Copy results folder to xfer folder
            // Existing files will be overwritten if they exist in htFilesToOverwrite (with the assumption that the files created by this manager are newer, and thus supersede existing files)

            try
            {
                // Copy all of the files and subdirectories in the local result folder to the target folder

                // Copy the files and subfolders
                var eResult = CopyResultsFolderRecursive(
                    sourceFolderPath, sourceFolderPath, targetDirectoryPath, objAnalysisResults,
                    ref blnErrorEncountered, ref intFailedFileCount, intRetryCount,
                    intRetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry);

                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                    blnErrorEncountered = true;

            }
            catch (Exception ex)
            {
                LogError("Error copying results folder to " + Path.GetPathRoot(targetDirectoryPath), ex);
                m_message = clsGlobal.AppendToComment(m_message, "Error copying results folder to " + Path.GetPathRoot(targetDirectoryPath));
                blnErrorEncountered = true;
            }

            if (blnErrorEncountered)
            {
                var strMessage = "Error copying " + intFailedFileCount + " file";
                if (intFailedFileCount != 1)
                {
                    strMessage += "s";
                }
                strMessage += " to transfer folder";
                m_message = clsGlobal.AppendToComment(m_message, strMessage);
                objAnalysisResults.CopyFailedResultsToArchiveFolder(sourceFolderPath);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Copies each of the files in the source folder to the target folder
        /// Uses CopyFileWithRetry to retry the copy up to intRetryCount times
        /// </summary>
        /// <param name="rootSourceFolderPath"></param>
        /// <param name="sourceFolderPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="objAnalysisResults"></param>
        /// <param name="errorEncountered"></param>
        /// <param name="failedFileCount"></param>
        /// <param name="retryCount"></param>
        /// <param name="retryHoldoffSeconds"></param>
        /// <param name="increaseHoldoffOnEachRetry"></param>
        /// <returns></returns>
        private CloseOutType CopyResultsFolderRecursive(
            string rootSourceFolderPath,
            string sourceFolderPath,
            string targetDirectoryPath,
            clsAnalysisResults objAnalysisResults,
            ref bool errorEncountered,
            ref int failedFileCount,
            int retryCount,
            int retryHoldoffSeconds,
            bool increaseHoldoffOnEachRetry)
        {

            var filesToOverwrite = new SortedSet<string>(StringComparer.InvariantCultureIgnoreCase);

            try
            {
                if (objAnalysisResults.FolderExistsWithRetry(targetDirectoryPath))
                {
                    // The target folder already exists

                    // Examine the files in the results folder to see if any of the files already exist in the transfer folder
                    // If they do, compare the file modification dates and post a warning if a file will be overwritten (because the file on the local computer is newer)
                    // However, if file sizes differ, then replace the file

                    var objSourceFolderInfo = new DirectoryInfo(sourceFolderPath);
                    foreach (var objSourceFile in objSourceFolderInfo.GetFiles())
                    {
                        if (File.Exists(Path.Combine(targetDirectoryPath, objSourceFile.Name)))
                        {
                            var objTargetFile = new FileInfo(Path.Combine(targetDirectoryPath, objSourceFile.Name));

                            if (objSourceFile.Length != objTargetFile.Length || objSourceFile.LastWriteTimeUtc > objTargetFile.LastWriteTimeUtc)
                            {
                                var message = "File in transfer folder on server will be overwritten by newer file in results folder: " + objSourceFile.Name +
                                    "; new file date (UTC): " + objSourceFile.LastWriteTimeUtc +
                                    "; old file date (UTC): " + objTargetFile.LastWriteTimeUtc;

                                // Log a warning, though not if the file is JobParameters_1394245.xml since we update that file after each job step
                                if (objSourceFile.Name != clsAnalysisJob.JobParametersFilename(m_JobNum))
                                {
                                    LogWarning(message);
                                }

                                if (!filesToOverwrite.Contains(objSourceFile.Name))
                                    filesToOverwrite.Add(objSourceFile.Name);
                            }
                        }
                    }
                }
                else
                {
                    // Need to create the target folder
                    try
                    {
                        objAnalysisResults.CreateFolderWithRetry(targetDirectoryPath);
                    }
                    catch (Exception ex)
                    {
                        LogError("Error creating results folder in transfer directory, " + Path.GetPathRoot(targetDirectoryPath), ex);
                        m_message = clsGlobal.AppendToComment(m_message, "Error creating results folder in transfer directory, " + Path.GetPathRoot(targetDirectoryPath));
                        objAnalysisResults.CopyFailedResultsToArchiveFolder(rootSourceFolderPath);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

            }
            catch (Exception ex)
            {
                LogError("Error comparing files in source folder to " + targetDirectoryPath, ex);
                m_message = clsGlobal.AppendToComment(m_message, "Error comparing files in source folder to transfer directory");
                objAnalysisResults.CopyFailedResultsToArchiveFolder(rootSourceFolderPath);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Note: Entries in ResultFiles will have full file paths, not just file names
            var resultFiles = Directory.GetFiles(sourceFolderPath, "*");

            foreach (var fileToCopy in resultFiles)
            {
                var sourceFileName = Path.GetFileName(fileToCopy);
                if (sourceFileName == null)
                    continue;

                var targetPath = Path.Combine(targetDirectoryPath, sourceFileName);

                try
                {
                    if (filesToOverwrite.Contains(sourceFileName))
                    {
                        // Copy file and overwrite existing
                        objAnalysisResults.CopyFileWithRetry(fileToCopy, targetPath, true, retryCount, retryHoldoffSeconds, increaseHoldoffOnEachRetry);
                    }
                    else
                    {
                        // Copy file only if it doesn't currently exist
                        if (!File.Exists(targetPath))
                        {
                            objAnalysisResults.CopyFileWithRetry(fileToCopy, targetPath, true, retryCount, retryHoldoffSeconds, increaseHoldoffOnEachRetry);
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Continue copying files; we'll fail the results at the end of this function
                    LogError(" CopyResultsFolderToServer: error copying " + Path.GetFileName(fileToCopy) + " to " + targetPath, ex);
                    errorEncountered = true;
                    failedFileCount += 1;
                }
            }

            // Recursively call this function for each subfolder
            // If any of the subfolders have an error, we'll continue copying, but will set blnErrorEncountered to True
            var eResult = CloseOutType.CLOSEOUT_SUCCESS;

            var diSourceFolder = new DirectoryInfo(sourceFolderPath);

            foreach (var objSubFolder in diSourceFolder.GetDirectories())
            {
                var targetDirectoryPathCurrent = Path.Combine(targetDirectoryPath, objSubFolder.Name);

                eResult = CopyResultsFolderRecursive(rootSourceFolderPath, objSubFolder.FullName, targetDirectoryPathCurrent, objAnalysisResults,
                    ref errorEncountered, ref failedFileCount, retryCount, retryHoldoffSeconds, increaseHoldoffOnEachRetry);

                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                    errorEncountered = true;

            }

            return eResult;

        }

        /// <summary>
        /// Determines the path to the remote transfer folder
        /// Creates the folder if it does not exist
        /// </summary>
        /// <returns>The full path to the remote transfer folder; an empty string if an error</returns>
        /// <remarks></remarks>
        protected string CreateRemoteTransferFolder(clsAnalysisResults objAnalysisResults)
        {

            var transferFolderPath = m_jobParams.GetParam("transferFolderPath");

            // Verify transfer directory exists
            // First make sure TransferFolderPath is defined
            if (string.IsNullOrEmpty(transferFolderPath))
            {
                LogMessage("Transfer folder path not defined; job param 'transferFolderPath' is empty", 0, true);
                m_message = clsGlobal.AppendToComment(m_message, "Transfer folder path not defined");
                return string.Empty;
            }

            return CreateRemoteTransferFolder(objAnalysisResults, transferFolderPath);

        }

        /// <summary>
        /// Determines the path to the remote transfer folder
        /// Creates the folder if it does not exist
        /// </summary>
        /// <param name="objAnalysisResults">Analysis results object</param>
        /// <param name="transferFolderPath">Base transfer folder path, e.g. \\proto-11\DMS3_Xfer\</param>
        /// <returns>The full path to the remote transfer folder; an empty string if an error</returns>
        protected string CreateRemoteTransferFolder(clsAnalysisResults objAnalysisResults, string transferFolderPath)
        {

            if (string.IsNullOrEmpty(m_ResFolderName))
            {
                LogError("Results folder name is not defined, job " + m_jobParams.GetParam("StepParameters", "Job"));
                m_message = "Results folder job parameter not defined (OutputFolderName)";
                return string.Empty;
            }

            // Now verify transfer directory exists
            try
            {
                objAnalysisResults.FolderExistsWithRetry(transferFolderPath);
            }
            catch (Exception ex)
            {
                LogError("Error verifying transfer directory, " + Path.GetPathRoot(transferFolderPath), ex);
                return string.Empty;
            }

            // Determine if dataset folder in transfer directory already exists; make directory if it doesn't exist
            // First make sure "DatasetFolderName" or "DatasetNum" is defined
            if (string.IsNullOrEmpty(m_Dataset))
            {
                LogError("Dataset name is undefined, job " + m_jobParams.GetParam("StepParameters", "Job"));
                m_message = "Dataset name is undefined";
                return string.Empty;
            }

            string strRemoteTransferFolderPath;

            if (clsGlobal.IsMatch(m_Dataset, "Aggregation"))
            {
                // Do not append "Aggregation" to the path since this is a generic dataset name applied to jobs that use Data Packages
                strRemoteTransferFolderPath = string.Copy(transferFolderPath);
            }
            else
            {
                // Append the dataset folder name to the transfer folder path
                var datasetFolderName = m_jobParams.GetParam("StepParameters", "DatasetFolderName");
                if (string.IsNullOrWhiteSpace(datasetFolderName))
                    datasetFolderName = m_Dataset;
                strRemoteTransferFolderPath = Path.Combine(transferFolderPath, datasetFolderName);
            }

            // Create the target folder if it doesn't exist
            try
            {
                objAnalysisResults.CreateFolderWithRetry(strRemoteTransferFolderPath, MaxRetryCount: 5, RetryHoldoffSeconds: 20, blnIncreaseHoldoffOnEachRetry: true);
            }
            catch (Exception ex)
            {
                LogError("Error creating dataset folder in transfer directory, " + Path.GetPathRoot(strRemoteTransferFolderPath), ex);
                return string.Empty;
            }

            // Now append the output folder name to strRemoteTransferFolderPath
            strRemoteTransferFolderPath = Path.Combine(strRemoteTransferFolderPath, m_ResFolderName);

            return strRemoteTransferFolderPath;

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
        /// <param name="intDebugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Raises exception if error occurs</remarks>
        public static bool DeleteFileWithRetries(string FileNamePath, int intDebugLevel)
        {
            return DeleteFileWithRetries(FileNamePath, intDebugLevel, 3);
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
                clsGlobal.LogDebug("clsAnalysisToolRunnerBase.DeleteFileWithRetries, executing method");
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
                        clsGlobal.LogDebug("clsAnalysisToolRunnerBase.DeleteFileWithRetries, normal exit");
                    }
                    return true;

                }
                catch (UnauthorizedAccessException Err1)
                {
                    // File may be read-only. Clear read-only flag and try again
                    if (debugLevel > 0)
                    {
                        clsGlobal.LogDebug("File " + fileNamePath + " exception ERR1: " + Err1.Message);
                        if ((Err1.InnerException != null))
                        {
                            clsGlobal.LogDebug("Inner exception: " + Err1.InnerException.Message);
                        }
                        clsGlobal.LogDebug("File " + fileNamePath + " may be read-only, attribute reset attempt #" + retryCount);
                    }
                    File.SetAttributes(fileNamePath, File.GetAttributes(fileNamePath) & ~FileAttributes.ReadOnly);
                    errType = AMFileNotDeletedAfterRetryException.RetryExceptionType.Unauthorized_Access_Exception;
                    retryCount += 1;

                }
                catch (IOException Err2)
                {
                    // If problem is locked file, attempt to fix lock and retry
                    if (debugLevel > 0)
                    {
                        clsGlobal.LogDebug("File " + fileNamePath + " exception ERR2: " + Err2.Message);
                        if ((Err2.InnerException != null))
                        {
                            clsGlobal.LogDebug("Inner exception: " + Err2.InnerException.Message);
                        }
                        clsGlobal.LogDebug("Error deleting file " + fileNamePath + ", attempt #" + retryCount);
                    }
                    errType = AMFileNotDeletedAfterRetryException.RetryExceptionType.IO_Exception;

                    // Delay 2 seconds
                    Thread.Sleep(2000);

                    // Do a garbage collection in case something is hanging onto the file that has been closed, but not GC'd
                    PRISM.clsProgRunner.GarbageCollectNow();
                    retryCount += 1;

                }
                catch (Exception Err3)
                {
                    var msg = "Error deleting file, exception ERR3 " + fileNamePath + Err3.Message;
                    clsGlobal.LogError(msg);
                    throw new AMFileNotDeletedException(fileNamePath, Err3.Message);
                }
            }

            // If we got to here, then we've exceeded the max retry limit
            throw new AMFileNotDeletedAfterRetryException(fileNamePath, errType, "Unable to delete or move file after multiple retries");

        }

        protected CloseOutType DeleteRawDataFiles()
        {
            var rawDataType = m_jobParams.GetParam("RawDataType");

            return DeleteRawDataFiles(rawDataType);
        }

        protected CloseOutType DeleteRawDataFiles(string rawDataType)
        {
            var eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType);

            return DeleteRawDataFiles(eRawDataType);
        }

        protected CloseOutType DeleteRawDataFiles(clsAnalysisResources.eRawDataTypeConstants eRawDataType)
        {

            // Deletes the raw data files/folders from the working directory
            bool isFile;
            var isNetworkDir = false;
            var fileOrFolderName = string.Empty;

            switch (eRawDataType)
            {
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                    fileOrFolderName = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_RAW_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile:
                    fileOrFolderName = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_WIFF_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.UIMF:
                    fileOrFolderName = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_UIMF_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzXML:
                    fileOrFolderName = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MZXML_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzML:
                    fileOrFolderName = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MZML_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                    fileOrFolderName = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_D_EXTENSION);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder:
                    fileOrFolderName = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_RAW_EXTENSION);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders:

                    var newSourceFolder = clsAnalysisResources.ResolveSerStoragePath(m_WorkDir);

                    // Check for "0.ser" folder
                    if (string.IsNullOrEmpty(newSourceFolder))
                    {
                        fileOrFolderName = Path.Combine(m_WorkDir, m_Dataset);
                        isNetworkDir = false;
                    }
                    else
                    {
                        isNetworkDir = true;
                    }

                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                    // Bruker_FT folders are actually .D folders
                    fileOrFolderName = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_D_EXTENSION);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDISpot:
                    ////////////////////////////////////
                    // TODO: Finalize this code
                    //       DMS doesn't yet have a BrukerTOF dataset
                    //        so we don't know the official folder structure
                    ////////////////////////////////////

                    fileOrFolderName = Path.Combine(m_WorkDir, m_Dataset);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging:

                    ////////////////////////////////////
                    // TODO: Finalize this code
                    //       DMS doesn't yet have a BrukerTOF dataset
                    //        so we don't know the official folder structure
                    ////////////////////////////////////

                    fileOrFolderName = Path.Combine(m_WorkDir, m_Dataset);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:

                    // BrukerTOFBaf folders are actually .D folders
                    fileOrFolderName = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_D_EXTENSION);
                    isFile = false;

                    break;
                default:
                    // Should never get this value
                    m_message = "DeleteRawDataFiles, Invalid RawDataType specified: " + eRawDataType.ToString();
                    return CloseOutType.CLOSEOUT_FAILED;
            }

            if (isFile)
            {
                // Data is a file, so use file deletion tools
                try
                {
                    if (!File.Exists(fileOrFolderName))
                    {
                        // File not found; treat this as a success
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }

                    // DeleteFileWithRetries will throw an exception if it cannot delete any raw data files (e.g. the .UIMF file)
                    // Thus, need to wrap it with an Exception handler

                    if (DeleteFileWithRetries(fileOrFolderName))
                    {
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }

                    LogError("Error deleting raw data file " + fileOrFolderName);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                catch (Exception ex)
                {
                    LogError("Exception deleting raw data file " + fileOrFolderName, ex);
                    return CloseOutType.CLOSEOUT_FAILED;
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
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }
                catch (Exception ex)
                {
                    LogError("Exception deleting raw data folder " + fileOrFolderName, ex);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        protected void DeleteTemporaryfile(string strFilePath)
        {
            try
            {
                if (File.Exists(strFilePath))
                {
                    File.Delete(strFilePath);
                }
            }
            catch (Exception ex)
            {
                LogMessage("Exception deleting temporary file " + strFilePath + ": " + ex.Message, 0, true);
            }

        }

        /// <summary>
        /// Determine the path to the correct version of the step tool
        /// </summary>
        /// <param name="strStepToolName">The name of the step tool, e.g. LCMSFeatureFinder</param>
        /// <param name="strProgLocManagerParamName">The name of the manager parameter that defines the path to the folder with the exe, e.g. LCMSFeatureFinderProgLoc</param>
        /// <param name="strExeName">The name of the exe file, e.g. LCMSFeatureFinder.exe</param>
        /// <returns>The path to the program, or an empty string if there is a problem</returns>
        /// <remarks></remarks>
        protected string DetermineProgramLocation(string strStepToolName, string strProgLocManagerParamName, string strExeName)
        {

            // Check whether the settings file specifies that a specific version of the step tool be used
            var strStepToolVersion = m_jobParams.GetParam(strStepToolName + "_Version");

            return DetermineProgramLocation(strStepToolName, strProgLocManagerParamName, strExeName, strStepToolVersion);

        }

        /// <summary>
        /// Determine the path to the correct version of the step tool
        /// </summary>
        /// <param name="strStepToolName">The name of the step tool, e.g. LCMSFeatureFinder</param>
        /// <param name="strProgLocManagerParamName">The name of the manager parameter that defines the path to the folder with the exe, e.g. LCMSFeatureFinderProgLoc</param>
        /// <param name="strExeName">The name of the exe file, e.g. LCMSFeatureFinder.exe</param>
        /// <param name="strStepToolVersion">Specific step tool version to use (will be the name of a subfolder located below the primary ProgLoc location)</param>
        /// <returns>The path to the program, or an empty string if there is a problem</returns>
        /// <remarks></remarks>
        protected string DetermineProgramLocation(string strStepToolName, string strProgLocManagerParamName, string strExeName, string strStepToolVersion)
        {

            return DetermineProgramLocation(strStepToolName, strProgLocManagerParamName, strExeName, strStepToolVersion, m_mgrParams, out m_message);
        }

        /// <summary>
        /// Determine the path to the correct version of the step tool
        /// </summary>
        /// <param name="strStepToolName">The name of the step tool, e.g. LCMSFeatureFinder</param>
        /// <param name="strProgLocManagerParamName">The name of the manager parameter that defines the path to the folder with the exe, e.g. LCMSFeatureFinderProgLoc</param>
        /// <param name="strExeName">The name of the exe file, e.g. LCMSFeatureFinder.exe</param>
        /// <param name="strStepToolVersion">Specific step tool version to use (will be the name of a subfolder located below the primary ProgLoc location)</param>
        /// <param name="mgrParams">Manager parameters</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>The path to the program, or an empty string if there is a problem</returns>
        /// <remarks></remarks>
        public static string DetermineProgramLocation(
            string strStepToolName,
            string strProgLocManagerParamName,
            string strExeName,
            string strStepToolVersion,
            IMgrParams mgrParams,
            out string errorMessage)
        {

            errorMessage = string.Empty;

            // Lookup the path to the folder that contains the Step tool
            var progLoc = mgrParams.GetParam(strProgLocManagerParamName);

            if (string.IsNullOrWhiteSpace(progLoc))
            {
                errorMessage = "Manager parameter " + strProgLocManagerParamName + " is not defined in the Manager Control DB";
                clsGlobal.LogError(errorMessage);
                return string.Empty;
            }

            // Check whether the settings file specifies that a specific version of the step tool be used

            if (!string.IsNullOrWhiteSpace(strStepToolVersion))
            {
                // Specific version is defined; verify that the folder exists
                progLoc = Path.Combine(progLoc, strStepToolVersion);

                if (!Directory.Exists(progLoc))
                {
                    errorMessage = "Version-specific folder not found for " + strStepToolName;
                    clsGlobal.LogError(errorMessage + ": " + progLoc);
                    return string.Empty;
                }

                clsGlobal.LogMessage("Using specific version of " + strStepToolName + ": " + progLoc);
            }

            // Define the path to the .Exe, then verify that it exists
            progLoc = Path.Combine(progLoc, strExeName);

            if (!File.Exists(progLoc))
            {
                errorMessage = "Cannot find " + strStepToolName + " program file " + strExeName;
                clsGlobal.LogError(errorMessage + " at " + progLoc);
                return string.Empty;
            }

            return progLoc;

        }

        /// <summary>
        /// Gets the dictionary for the packed job parameter
        /// </summary>
        /// <param name="strPackedJobParameterName">Packaged job parameter name</param>
        /// <returns>List of strings</returns>
        /// <remarks>Data will have been stored by function clsAnalysisResources.StorePackedJobParameterDictionary</remarks>
        protected Dictionary<string, string> ExtractPackedJobParameterDictionary(string strPackedJobParameterName)
        {

            var dctData = new Dictionary<string, string>();

            var lstData = ExtractPackedJobParameterList(strPackedJobParameterName);

            foreach (var item in lstData)
            {
                var intEqualsIndex = item.LastIndexOf('=');
                if (intEqualsIndex > 0)
                {
                    var strKey = item.Substring(0, intEqualsIndex);
                    var strValue = item.Substring(intEqualsIndex + 1);

                    if (!dctData.ContainsKey(strKey))
                    {
                        dctData.Add(strKey, strValue);
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
        /// <param name="strPackedJobParameterName">Packaged job parameter name</param>
        /// <returns>List of strings</returns>
        /// <remarks>Data will have been stored by function clsAnalysisResources.StorePackedJobParameterDictionary</remarks>
        protected List<string> ExtractPackedJobParameterList(string strPackedJobParameterName)
        {

            var strList = m_jobParams.GetJobParameter(strPackedJobParameterName, string.Empty);

            if (string.IsNullOrEmpty(strList))
            {
                return new List<string>();
            }

            // Split the list on tab characters
            return strList.Split('\t').ToList();
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
        /// <param name="intUpdateIntervalSeconds">
        /// The minimum number of seconds between updates
        /// If fewer than intUpdateIntervalSeconds seconds have elapsed since the last call to this function, then no update will occur
        /// </param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool GetCurrentMgrSettingsFromDB(int intUpdateIntervalSeconds)
        {
            return GetCurrentMgrSettingsFromDB(intUpdateIntervalSeconds, m_mgrParams, ref m_DebugLevel);
        }

        /// <summary>
        /// Looks up the current debug level for the manager.  If the call to the server fails, DebugLevel will be left unchanged
        /// </summary>
        /// <param name="intUpdateIntervalSeconds">Update interval, in seconds</param>
        /// <param name="objMgrParams">Manager params</param>
        /// <param name="debugLevel">Input/Output parameter: set to the current debug level, will be updated to the debug level in the manager control DB</param>
        /// <returns>True for success; False for error</returns>
        /// <remarks></remarks>
        public static bool GetCurrentMgrSettingsFromDB(int intUpdateIntervalSeconds, IMgrParams objMgrParams, ref short debugLevel)
        {

            try
            {
                if (intUpdateIntervalSeconds > 0 && DateTime.UtcNow.Subtract(mLastManagerSettingsUpdateTime).TotalSeconds < intUpdateIntervalSeconds)
                {
                    return true;
                }
                mLastManagerSettingsUpdateTime = DateTime.UtcNow;

                if (debugLevel >= 5)
                {
                    clsGlobal.LogDebug("Updating manager settings from the Manager Control DB");
                }

                // Data Source=proteinseqs;Initial Catalog=manager_control
                var connectionString = objMgrParams.GetParam("MgrCnfgDbConnectStr");
                var managerName = objMgrParams.GetParam("MgrName");

                var newDebugLevel = GetManagerDebugLevel(connectionString, managerName, debugLevel, 0);

                if (debugLevel > 0 && newDebugLevel != debugLevel)
                {
                    clsGlobal.LogDebug("Debug level changed from " + debugLevel + " to " + newDebugLevel);
                    debugLevel = newDebugLevel;
                }

                return true;

            }
            catch (Exception ex)
            {
                var errorMessage = "Exception getting current manager settings from the manager control DB";
                clsGlobal.LogError(errorMessage, ex);
            }

            return false;

        }

        static short GetManagerDebugLevel(string connectionString, string managerName, short currentDebugLevel, int recursionLevel)
        {

            if (recursionLevel > 3)
            {
                return currentDebugLevel;
            }

            var sqlQuery =
                "SELECT ParameterName, ParameterValue " +
                "FROM V_MgrParams " +
                "WHERE ManagerName = '" + managerName + "' AND " + " ParameterName IN ('debuglevel', 'MgrSettingGroupName')";

            List<List<string>> lstResults;
            var success = clsGlobal.GetQueryResults(sqlQuery, connectionString, out lstResults, "GetCurrentMgrSettingsFromDB");

            if (!success || lstResults.Count <= 0)
                return currentDebugLevel;

            foreach (var resultRow in lstResults)
            {
                var paramName = resultRow[0];
                var paramValue = resultRow[1];

                if (clsGlobal.IsMatch(paramName, "debuglevel"))
                {
                    var debugLevel = short.Parse(paramValue);
                    return debugLevel;
                }

                if (clsGlobal.IsMatch(paramName, "MgrSettingGroupName"))
                {
                    // DebugLevel is defined by a manager settings group; repeat the query to V_MgrParams

                    var debugLevel = GetManagerDebugLevel(connectionString, paramValue, currentDebugLevel, recursionLevel + 1);
                    return debugLevel;
                }
            }

            return currentDebugLevel;

        }

        /// <summary>
        /// Deterime the path to java.exe
        /// </summary>
        /// <returns>The path to the java.exe, or an empty string if the manager parameter is not defined or if java.exe does not exist</returns>
        /// <remarks></remarks>
        protected string GetJavaProgLoc()
        {

            // JavaLoc will typically be "C:\Program Files\Java\jre8\bin\Java.exe"
            var javaProgLoc = m_mgrParams.GetParam("JavaLoc");

            if (string.IsNullOrEmpty(javaProgLoc))
            {
                LogError("Parameter 'JavaLoc' not defined for this manager");
                return string.Empty;
            }

            if (!File.Exists(javaProgLoc))
            {
                LogError("Cannot find Java: " + javaProgLoc);
                return string.Empty;
            }

            return javaProgLoc;

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
                strMSXmlGeneratorAppPath = DetermineProgramLocation("ReAdW", "ReAdWProgLoc", strMSXmlGeneratorExe);

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
        /// Determines the folder that contains R.exe and Rcmd.exe (queries the registry)
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

            var transferFolderPath = m_jobParams.GetParam("transferFolderPath");

            if (string.IsNullOrEmpty(transferFolderPath))
            {
                LogError("Transfer folder path not defined; job param 'transferFolderPath' is empty");
                m_message = clsGlobal.AppendToComment(m_message, "Transfer folder path not defined");
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
            return clsIonicZipTools.GetZipFilePathForFile(sourceFilePath);
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
            m_IonicZipTools.DebugLevel = m_DebugLevel;

            // Note that m_IonicZipTools logs error messages using clsLogTools
            return m_IonicZipTools.GUnzipFile(gzipFilePath, targetDirectory);
        }

        /// <summary>
        /// Gzips sourceFilePath, creating a new file in the same folder, but with extension .gz appended to the name (e.g. Dataset.mzid.gz)
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="deleteSourceAfterZip">If True, then will delete the file after zipping it</param>
        /// <returns>True if success; false if an error</returns>
        public bool GZipFile(string sourceFilePath, bool deleteSourceAfterZip)
        {
            m_IonicZipTools.DebugLevel = m_DebugLevel;

            // Note that m_IonicZipTools logs error messages using clsLogTools
            var success = m_IonicZipTools.GZipFile(sourceFilePath, deleteSourceAfterZip);

            if (!success && m_IonicZipTools.Message.ToLower().Contains("OutOfMemoryException".ToLower()))
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
        /// <param name="deleteSourceAfterZip">If True, then will delete the file after zipping it</param>
        /// <returns>True if success; false if an error</returns>
        public bool GZipFile(string sourceFilePath, string targetDirectoryPath, bool deleteSourceAfterZip)
        {

            m_IonicZipTools.DebugLevel = m_DebugLevel;

            // Note that m_IonicZipTools logs error messages using clsLogTools
            var success = m_IonicZipTools.GZipFile(sourceFilePath, targetDirectoryPath, deleteSourceAfterZip);

            if (!success && m_IonicZipTools.Message.ToLower().Contains("OutOfMemoryException".ToLower()))
            {
                m_NeedToAbortProcessing = true;
            }

            return success;

        }

        /// <summary>
        /// GZip the given file
        /// </summary>
        /// <param name="fiResultFile"></param>
        /// <returns>Fileinfo object of the new .gz file or null if an error</returns>
        /// <remarks>Deletes the original file after creating the .gz file</remarks>
        public FileInfo GZipFile(FileInfo fiResultFile)
        {
            return GZipFile(fiResultFile, true);
        }

        /// <summary>
        /// GZip the given file
        /// </summary>
        /// <param name="fiResultFile"></param>
        /// <param name="deleteSourceAfterZip">If True, then will delete the file after zipping it</param>
        /// <returns>Fileinfo object of the new .gz file or null if an error</returns>
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

            var coresOnMachine = PRISMWin.clsProcessStats.GetCoreCount();
            int coreCount;

            if (threadCountText.StartsWith("all", StringComparison.InvariantCultureIgnoreCase))
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
            var connectionString = m_mgrParams.GetParam("brokerconnectionstring");

            var dataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1);

            if (dataPackageID < 0)
            {
                dctDataPackageDatasets = new Dictionary<int, clsDataPackageDatasetInfo>();
                return false;
            }

            return clsAnalysisResources.LoadDataPackageDatasetInfo(connectionString, dataPackageID, out dctDataPackageDatasets);
        }

        /// <summary>
        /// Looks up job information for the data package associated with this analysis job
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

            return clsAnalysisResources.LoadDataPackageJobInfo(connectionString, dataPackageID, out dctDataPackageJobs);
        }

        /// <summary>
        /// Loads the job settings file
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        /// <remarks></remarks>
        protected bool LoadSettingsFile()
        {
            var fileName = m_jobParams.GetParam("settingsFileName");
            if (fileName != "na")
            {
                var filePath = Path.Combine(m_WorkDir, fileName);

                // XML tool Loadsettings returns True even if file is not found, so separate check reqd
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
        /// Logs current progress to the log file at a given interval
        /// </summary>
        /// <param name="toolName"></param>
        /// <remarks>Longer log intervals when m_debuglevel is 0 or 1; shorter intervals for 5</remarks>
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
        /// Logs m_progress to the log file at interval logIntervalMinutes
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

                var progressMessage = " ... " + m_progress.ToString("0.0") + "% complete for " + toolName + ", job " + m_JobNum;

                if (DateTime.UtcNow.Subtract(m_LastProgressConsoleTime).TotalMinutes >= CONSOLE_PROGRESS_INTERVAL_MINUTES)
                {
                    m_LastProgressConsoleTime = DateTime.UtcNow;
                    Console.WriteLine(progressMessage);
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
                m_EvalMessage = warningMessage;
            }
            base.LogWarning(warningMessage);
        }

        /// <summary>
        /// Creates a results folder after analysis complete
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        protected CloseOutType MakeResultsFolder()
        {

            m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.PACKAGING_RESULTS, 0);

            // Makes results folder and moves files into it

            // Log status
            LogMessage(m_MachName + ": Creating results folder, Job " + m_JobNum);
            var resFolderNamePath = Path.Combine(m_WorkDir, m_ResFolderName);

            // Make the results folder
            try
            {
                Directory.CreateDirectory(resFolderNamePath);
            }
            catch (Exception ex)
            {
                // Log this error to the database
                LogError("Error making results folder, job " + m_JobNum, ex);
                m_message = clsGlobal.AppendToComment(m_message, "Error making results folder");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        /// <summary>
        /// Makes results folder and moves files into it
        /// </summary>
        /// <returns></returns>
        protected CloseOutType MoveResultFiles()
        {

            const int REJECT_LOGGING_THRESHOLD = 10;
            const int ACCEPT_LOGGING_THRESHOLD = 50;
            const int LOG_LEVEL_REPORT_ACCEPT_OR_REJECT = 5;

            var resFolderNamePath = string.Empty;
            var currentFileName = string.Empty;

            var blnErrorEncountered = false;

            // Move files into results folder
            try
            {
                m_StatusTools.UpdateAndWrite(
                    EnumMgrStatus.RUNNING,
                    EnumTaskStatus.RUNNING,
                    EnumTaskStatusDetail.PACKAGING_RESULTS, 0);

                resFolderNamePath = Path.Combine(m_WorkDir, m_ResFolderName);
                var dctRejectStats = new Dictionary<string, int>(StringComparer.InvariantCultureIgnoreCase);
                var dctAcceptStats = new Dictionary<string, int>(StringComparer.InvariantCultureIgnoreCase);

                // Log status
                if (m_DebugLevel >= 2)
                {
                    var strLogMessage = "Move Result Files to " + resFolderNamePath;
                    if (m_DebugLevel >= 3)
                    {
                        strLogMessage += "; ResultFilesToSkip contains " + m_jobParams.ResultFilesToSkip.Count + " entries" + "; " +
                            "ResultFileExtensionsToSkip contains " + m_jobParams.ResultFileExtensionsToSkip.Count + " entries" + "; " +
                            "ResultFilesToKeep contains " + m_jobParams.ResultFilesToKeep.Count + " entries";
                    }
                    LogMessage(strLogMessage, m_DebugLevel);
                }

                // Obtain a list of all files in the working directory
                // Ignore subdirectories
                var files = Directory.GetFiles(m_WorkDir, "*");

                // Check each file against m_jobParams.m_ResultFileExtensionsToSkip and m_jobParams.m_ResultFilesToKeep

                foreach (var tmpFileName in files)
                {
                    var okToMove = true;
                    currentFileName = tmpFileName;
                    var tmpFileNameLcase = Path.GetFileName(tmpFileName).ToLower();

                    // Check to see if the filename is defined in ResultFilesToSkip
                    // Note that entries in ResultFilesToSkip are not case sensitive since they were instantiated using SortedSet(Of String)(StringComparer.CurrentCultureIgnoreCase)
                    if (m_jobParams.ResultFilesToSkip.Contains(tmpFileNameLcase))
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
                            if (tmpFileNameLcase.EndsWith(ext, StringComparison.InvariantCultureIgnoreCase))
                            {
                                okToMove = false;
                                break;
                            }
                        }
                    }

                    if (!okToMove)
                    {
                        // Check to see if the file is a result file that got captured as a non result file
                        if (m_jobParams.ResultFilesToKeep.Contains(tmpFileNameLcase))
                        {
                            okToMove = true;
                        }
                    }

                    if (okToMove && PRISM.clsFileTools.IsVimSwapFile(tmpFileName))
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

                            int rejectCount;
                            if (dctRejectStats.TryGetValue(fileExtension, out rejectCount))
                            {
                                dctRejectStats[fileExtension] = rejectCount + 1;
                            }
                            else
                            {
                                dctRejectStats.Add(fileExtension, 1);
                            }

                            // Only log the first 10 times files of a given extension are rejected
                            //  However, if a file was rejected due to invalid characters in the name, then we don't track that rejection with dctRejectStats
                            if (dctRejectStats[fileExtension] <= REJECT_LOGGING_THRESHOLD)
                            {
                                LogDebug(" MoveResultFiles: Rejected file:  " + tmpFileName);
                            }
                        }
                    }

                    if (!okToMove)
                        continue;

                    // If valid file name, then move file to results folder
                    if (m_DebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT)
                    {
                        var fileExtension = Path.GetExtension(tmpFileName);

                        int acceptCount;
                        if (dctAcceptStats.TryGetValue(fileExtension, out acceptCount))
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

                                // If we get here, then the copy succeeded;
                                // The original file (in the work folder) will get deleted when the work folder is "cleaned" after the job finishes
                            }

                        }
                        catch (Exception ex2)
                        {
                            // Copy also failed
                            // Continue moving files; we'll fail the results at the end of this function
                            LogError(" MoveResultFiles: error moving/copying file: " + tmpFileName, ex2);
                            blnErrorEncountered = true;
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

                LogErrorToDatabase("Error moving results files, job " + m_JobNum + ex.Message);
                m_message = clsGlobal.AppendToComment(m_message, "Error moving results files");

                blnErrorEncountered = true;
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

            if (blnErrorEncountered)
            {
                // Try to save whatever files were moved into the results folder
                var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
                objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName));

                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        public static string NotifyMissingParameter(IJobParams oJobParams, string strParameterName)
        {

            var strSettingsFile = oJobParams.GetJobParameter("SettingsFileName", "?UnknownSettingsFile?");
            var strToolName = oJobParams.GetJobParameter("ToolName", "?UnknownToolName?");

            return "Settings file " + strSettingsFile + " for tool " + strToolName + " does not have parameter " + strParameterName + " defined";

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
            var objAssemblyTools = new clsAssemblyTools();

            objAssemblyTools.GetComponentFileVersionInfo(m_SummaryFile);

            var summaryFileName = m_jobParams.GetParam("StepTool") + "_AnalysisSummary.txt";

            if (!m_jobParams.ResultFilesToSkip.Contains(summaryFileName))
            {
                m_SummaryFile.SaveSummaryFile(Path.Combine(OutputPath, summaryFileName));
            }

        }

        /// <summary>
        /// Adds double quotes around a path if it contains a space
        /// </summary>
        /// <param name="strPath"></param>
        /// <returns>The path (updated if necessary)</returns>
        /// <remarks></remarks>
        public static string PossiblyQuotePath(string strPath)
        {
            return clsGlobal.PossiblyQuotePath(strPath);
        }

        public void PurgeOldServerCacheFiles(string cacheFolderPath)
        {
            // Value prior to December 2014: 3 TB
            // Value effective December 2014: 20 TB
            const int spaceUsageThresholdGB = 20000;
            PurgeOldServerCacheFiles(cacheFolderPath, spaceUsageThresholdGB);
        }

        public void PurgeOldServerCacheFilesTest(string cacheFolderPath, int spaceUsageThresholdGB)
        {
            if (cacheFolderPath.StartsWith(@"\\proto", StringComparison.InvariantCultureIgnoreCase))
            {
                if (!string.Equals(cacheFolderPath, @"\\proto-2\past\PurgeTest", StringComparison.InvariantCultureIgnoreCase))
                {
                    Console.WriteLine(@"This function cannot be used with a \\Proto-x\ server");
                    return;
                }
            }
            PurgeOldServerCacheFiles(cacheFolderPath, spaceUsageThresholdGB);
        }

        /// <summary>
        /// Determines the space usage of data files in the cache folder, e.g. at \\proto-11\MSXML_Cache
        /// If usage is over intSpaceUsageThresholdGB, then deletes the oldest files until usage falls below intSpaceUsageThresholdGB
        /// </summary>
        /// <param name="strCacheFolderPath">Path to the file cache</param>
        /// <param name="spaceUsageThresholdGB">Maximum space usage, in GB (cannot be less than 1000 on Proto-x servers; 10 otherwise)</param>
        private void PurgeOldServerCacheFiles(string strCacheFolderPath, int spaceUsageThresholdGB)
        {

            {

                var lstDataFiles = new List<KeyValuePair<DateTime, FileInfo>>();

                double dblTotalSizeMB = 0;

                double dblSizeDeletedMB = 0;
                var intFileDeleteCount = 0;
                var intFileDeleteErrorCount = 0;

                var dctErrorSummary = new Dictionary<string, int>();

                if (string.IsNullOrWhiteSpace(strCacheFolderPath))
                {
                    throw new ArgumentOutOfRangeException(nameof(strCacheFolderPath), "Cache folder path cannot be empty");
                }

                if (strCacheFolderPath.StartsWith(@"\\proto-", StringComparison.InvariantCultureIgnoreCase))
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

                    var diCacheFolder = new DirectoryInfo(strCacheFolderPath);

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

                    foreach (var fiItem in diCacheFolder.GetFiles("*.hashcheck", SearchOption.AllDirectories))
                    {
                        if (!fiItem.FullName.EndsWith(clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX, StringComparison.InvariantCultureIgnoreCase))
                            continue;

                        var strDataFilePath = fiItem.FullName.Substring(0, fiItem.FullName.Length - clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX.Length);

                        var fiDataFile = new FileInfo(strDataFilePath);

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
                    var managerName = m_mgrParams.GetParam("MgrName", m_MachName);

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
                                managerName,
                                fileSizeMB.ToString("0.00"),
                                kvItem.Value.LastWriteTime.ToString(DATE_TIME_FORMAT),
                                kvItem.Value.FullName));

                            dblSizeDeletedMB += fileSizeMB;
                            intFileDeleteCount += 1;

                            if (fiHashCheckFile.Exists)
                            {
                                fiHashCheckFile.Delete();
                            }

                        }
                        catch (Exception ex)
                        {
                            // Keep track of the number of times we have an exception
                            intFileDeleteErrorCount += 1;

                            int occurrences;
                            var strExceptionName = ex.GetType().ToString();
                            if (dctErrorSummary.TryGetValue(strExceptionName, out occurrences))
                            {
                                dctErrorSummary[strExceptionName] = occurrences + 1;
                            }
                            else
                            {
                                dctErrorSummary.Add(strExceptionName, 1);
                            }

                        }

                        if (dblTotalSizeMB / 1024.0 < spaceUsageThresholdGB * 0.95)
                        {
                            break;
                        }
                    }

                    LogMessage("Deleted " + intFileDeleteCount + " file(s) from " + strCacheFolderPath + ", recovering " + dblSizeDeletedMB.ToString("0.0") + " MB in disk space");

                    if (intFileDeleteErrorCount > 0)
                    {
                        LogMessage("Unable to delete " + intFileDeleteErrorCount + " file(s) from " + strCacheFolderPath, 0, true);
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
            var strTransferFolderPath = m_jobParams.GetParam("transferFolderPath");
            var diTransferFolder = new DirectoryInfo(strTransferFolderPath);

            m_Dataset = diTransferFolder.Name;

            if (diTransferFolder.Parent == null)
            {
                throw new DirectoryNotFoundException("Unable to determine the parent folder of " + diTransferFolder.FullName);
            }

            strTransferFolderPath = diTransferFolder.Parent.FullName;
            m_jobParams.SetParam("JobParameters", "transferFolderPath", strTransferFolderPath);

        }

        /// <summary>
        /// Extracts the contents of the Version= line in a Tool Version Info file
        /// </summary>
        /// <param name="strDLLFilePath"></param>
        /// <param name="strVersionInfoFilePath"></param>
        /// <param name="strVersion"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool ReadVersionInfoFile(string strDLLFilePath, string strVersionInfoFilePath, out string strVersion)
        {

            strVersion = string.Empty;
            var blnSuccess = false;

            try
            {
                if (!File.Exists(strVersionInfoFilePath))
                {
                    LogMessage("Version Info File not found: " + strVersionInfoFilePath, 0, true);
                    return false;
                }

                // Open strVersionInfoFilePath and read the Version= line
                using (var srInFile = new StreamReader(new FileStream(strVersionInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                        {
                            continue;
                        }

                        var intEqualsLoc = strLineIn.IndexOf('=');

                        if (intEqualsLoc <= 0)
                            continue;

                        var strKey = strLineIn.Substring(0, intEqualsLoc);
                        string strValue;

                        if (intEqualsLoc < strLineIn.Length)
                        {
                            strValue = strLineIn.Substring(intEqualsLoc + 1);
                        }
                        else
                        {
                            strValue = string.Empty;
                        }

                        switch (strKey.ToLower())
                        {
                            case "filename":
                                break;
                            case "path":
                                break;
                            case "version":
                                strVersion = string.Copy(strValue);
                                if (string.IsNullOrWhiteSpace(strVersion))
                                {
                                    LogMessage("Empty version line in Version Info file for " + Path.GetFileName(strDLLFilePath), 0, true);
                                    blnSuccess = false;
                                }
                                else
                                {
                                    blnSuccess = true;
                                }
                                break;
                            case "error":
                                LogMessage("Error reported by DLLVersionInspector for " + Path.GetFileName(strDLLFilePath) + ": " + strValue, 0, true);
                                blnSuccess = false;
                                break;
                            default:
                                // Ignore the line
                                break;

                        }
                    }

                }

            }
            catch (Exception ex)
            {
                LogError("Error reading Version Info File for " + Path.GetFileName(strDLLFilePath), ex);
            }

            return blnSuccess;

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

        protected bool ReplaceUpdatedFile(FileInfo fiOrginalFile, FileInfo fiUpdatedFile)
        {

            try
            {
                var finalFilePath = fiOrginalFile.FullName;

                Thread.Sleep(250);
                fiOrginalFile.Delete();

                Thread.Sleep(250);
                fiUpdatedFile.MoveTo(finalFilePath);

            }
            catch (Exception ex)
            {
                if (m_DebugLevel >= 1)
                {
                    LogError("Error in ReplaceUpdatedFile", ex);
                }

                return false;
            }

            return true;

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
            LogMessage(m_MachName + ": Starting analysis, job " + m_JobNum);

            // Start the job timer
            m_StartTime = DateTime.UtcNow;

            // Remainder of method is supplied by subclasses

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        /// <summary>
        /// Creates a Tool Version Info file
        /// </summary>
        /// <param name="strFolderPath"></param>
        /// <param name="strToolVersionInfo"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool SaveToolVersionInfoFile(string strFolderPath, string strToolVersionInfo)
        {

            try
            {
                var strStepToolName = m_jobParams.GetParam("StepTool");
                var strToolVersionFilePath = Path.Combine(strFolderPath, "Tool_Version_Info_" + strStepToolName + ".txt");

                using (var swToolVersionFile = new StreamWriter(new FileStream(strToolVersionFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {

                    swToolVersionFile.WriteLine("Date: " + DateTime.Now.ToString(DATE_TIME_FORMAT));
                    swToolVersionFile.WriteLine("Dataset: " + m_Dataset);
                    swToolVersionFile.WriteLine("Job: " + m_JobNum);
                    swToolVersionFile.WriteLine("Step: " + m_jobParams.GetParam("StepParameters", "Step"));
                    swToolVersionFile.WriteLine("Tool: " + m_jobParams.GetParam("StepTool"));
                    swToolVersionFile.WriteLine("ToolVersionInfo:");

                    swToolVersionFile.WriteLine(strToolVersionInfo.Replace("; ", Environment.NewLine));

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
        /// <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        protected bool SetStepTaskToolVersion(string strToolVersionInfo)
        {
            return SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>());
        }

        /// <summary>
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <param name="ioToolFiles">FileSystemInfo list of program files related to the step tool</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        protected bool SetStepTaskToolVersion(string strToolVersionInfo, List<FileInfo> ioToolFiles)
        {

            return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, true);
        }

        /// <summary>
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <param name="ioToolFiles">FileSystemInfo list of program files related to the step tool</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        protected bool SetStepTaskToolVersion(string strToolVersionInfo, IEnumerable<FileInfo> ioToolFiles)
        {

            return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, true);
        }

        /// <summary>
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <param name="strToolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <param name="ioToolFiles">FileSystemInfo list of program files related to the step tool</param>
        /// <param name="blnSaveToolVersionTextFile">if true, then creates a text file with the tool version information</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        protected bool SetStepTaskToolVersion(string strToolVersionInfo, IEnumerable<FileInfo> ioToolFiles, bool blnSaveToolVersionTextFile)
        {

            var strExeInfo = string.Empty;
            string strToolVersionInfoCombined;

            if (ioToolFiles != null)
            {
                foreach (var ioFileInfo in ioToolFiles)
                {
                    try
                    {
                        if (ioFileInfo.Exists)
                        {
                            strExeInfo = clsGlobal.AppendToComment(strExeInfo, ioFileInfo.Name + ": " + ioFileInfo.LastWriteTime.ToString(DATE_TIME_FORMAT));
                            LogMessage("EXE Info: " + strExeInfo, 2);
                        }
                        else
                        {
                            LogMessage("Warning: Tool file not found: " + ioFileInfo.FullName);
                        }

                    }
                    catch (Exception ex)
                    {
                        LogError("Exception looking up tool version file info", ex);
                    }
                }
            }

            // Append the .Exe info to strToolVersionInfo
            if (string.IsNullOrEmpty(strExeInfo))
            {
                strToolVersionInfoCombined = string.Copy(strToolVersionInfo);
            }
            else
            {
                strToolVersionInfoCombined = clsGlobal.AppendToComment(strToolVersionInfo, strExeInfo);
            }

            if (blnSaveToolVersionTextFile)
            {
                SaveToolVersionInfoFile(m_WorkDir, strToolVersionInfoCombined);
            }

            // Setup for execution of the stored procedure
            var myCmd = new SqlCommand
            {
                CommandType = CommandType.StoredProcedure,
                CommandText = SP_NAME_SET_TASK_TOOL_VERSION
            };

            myCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
            myCmd.Parameters.Add(new SqlParameter("@job", SqlDbType.Int)).Value = m_jobParams.GetJobParameter("StepParameters", "Job", 0);
            myCmd.Parameters.Add(new SqlParameter("@step", SqlDbType.Int)).Value = m_jobParams.GetJobParameter("StepParameters", "Step", 0);
            myCmd.Parameters.Add(new SqlParameter("@ToolVersionInfo", SqlDbType.VarChar, 900)).Value = strToolVersionInfoCombined;

            var objAnalysisTask = new clsAnalysisJob(m_mgrParams, m_DebugLevel);

            // Execute the stored procedure (retry the call up to 4 times)
            var resCode = objAnalysisTask.PipelineDBProcedureExecutor.ExecuteSP(myCmd, 4);

            if (resCode == 0)
            {
                return true;
            }

            LogMessage("Error " + resCode + " storing tool version for current processing step", 0, true);
            return false;
        }

        protected bool SortTextFile(string textFilePath, string mergedFilePath, bool hasHeaderLine)
        {
            try
            {
                var sortUtility = new FlexibleFileSortUtility.TextFileSorter();

                mLastSortUtilityProgress = DateTime.UtcNow;
                mSortUtilityErrorMessage = string.Empty;

                sortUtility.WorkingDirectoryPath = m_WorkDir;
                sortUtility.HasHeaderLine = hasHeaderLine;
                sortUtility.ColumnDelimiter = "\t";
                sortUtility.MaxFileSizeMBForInMemorySort = FlexibleFileSortUtility.TextFileSorter.DEFAULT_IN_MEMORY_SORT_MAX_FILE_SIZE_MB;
                sortUtility.ChunkSizeMB = FlexibleFileSortUtility.TextFileSorter.DEFAULT_CHUNK_SIZE_MB;

                sortUtility.ProgressChanged += mSortUtility_ProgressChanged;
                sortUtility.ErrorEvent += mSortUtility_ErrorEvent;
                sortUtility.WarningEvent += mSortUtility_WarningEvent;
                sortUtility.MessageEvent += mSortUtility_MessageEvent;

                var success = sortUtility.SortFile(textFilePath, mergedFilePath);

                if (!success)
                {
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

                return true;

            }
            catch (Exception ex)
            {
                LogError("Exception in SortTextFile", ex);
                return false;
            }

        }

        /// <summary>
        /// Uses Reflection to determine the version info for an assembly already loaded in memory
        /// </summary>
        /// <param name="strToolVersionInfo">Version info string to append the version info to</param>
        /// <param name="strAssemblyName">Assembly Name</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>Use StoreToolVersionInfoOneFile for DLLs not loaded in memory</remarks>
        protected bool StoreToolVersionInfoForLoadedAssembly(ref string strToolVersionInfo, string strAssemblyName)
        {
            return StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, strAssemblyName, blnIncludeRevision: true);
        }

        /// <summary>
        /// Uses Reflection to determine the version info for an assembly already loaded in memory
        /// </summary>
        /// <param name="strToolVersionInfo">Version info string to append the version info to</param>
        /// <param name="strAssemblyName">Assembly Name</param>
        /// <param name="blnIncludeRevision">Set to True to include a version of the form 1.5.4821.24755; set to omit the revision, giving a version of the form 1.5.4821</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>Use StoreToolVersionInfoOneFile for DLLs not loaded in memory</remarks>
        protected bool StoreToolVersionInfoForLoadedAssembly(ref string strToolVersionInfo, string strAssemblyName, bool blnIncludeRevision)
        {

            try
            {
                var assemblyName = System.Reflection.Assembly.Load(strAssemblyName).GetName();

                string strNameAndVersion;
                if (blnIncludeRevision)
                {
                    strNameAndVersion = assemblyName.Name + ", Version=" + assemblyName.Version;
                }
                else
                {
                    strNameAndVersion = assemblyName.Name + ", Version=" + assemblyName.Version.Major + "." + assemblyName.Version.Minor + "." + assemblyName.Version.Build;
                }

                strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion);

            }
            catch (Exception ex)
            {
                LogError("Exception determining Assembly info for " + strAssemblyName, ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Determines the version info for a .NET DLL using reflection
        /// If reflection fails, then uses System.Diagnostics.FileVersionInfo
        /// </summary>
        /// <param name="strToolVersionInfo">Version info string to append the version info to</param>
        /// <param name="strDLLFilePath">Path to the DLL</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        public bool StoreToolVersionInfoOneFile(ref string strToolVersionInfo, string strDLLFilePath)
        {

            bool blnSuccess;

            try
            {
                var ioFileInfo = new FileInfo(strDLLFilePath);

                if (!ioFileInfo.Exists)
                {
                    LogMessage("Warning: File not found by StoreToolVersionInfoOneFile: " + strDLLFilePath);
                    return false;

                }

                var assembly = System.Reflection.Assembly.LoadFrom(ioFileInfo.FullName);
                var assemblyName = assembly.GetName();

                var strNameAndVersion = assemblyName.Name + ", Version=" + assemblyName.Version;
                strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion);

                blnSuccess = true;
            }
            catch (BadImageFormatException)
            {
                // Most likely trying to read a 64-bit DLL (if this program is running as 32-bit)
                // Or, if this program is AnyCPU and running as 64-bit, the target DLL or Exe must be 32-bit

                // Instead try StoreToolVersionInfoOneFile32Bit or StoreToolVersionInfoOneFile64Bit

                // Use this when compiled as AnyCPU
                blnSuccess = StoreToolVersionInfoOneFile32Bit(ref strToolVersionInfo, strDLLFilePath);

                // Use this when compiled as 32-bit
                // blnSuccess = StoreToolVersionInfoOneFile64Bit(strToolVersionInfo, strDLLFilePath)

            }
            catch (Exception ex)
            {
                // If you get an exception regarding .NET 4.0 not being able to read a .NET 1.0 runtime, then add these lines to the end of file AnalysisManagerProg.exe.config
                //  <startup useLegacyV2RuntimeActivationPolicy="true">
                //    <supportedRuntime version="v4.0" />
                //  </startup>
                LogError("Exception determining Assembly info for " + Path.GetFileName(strDLLFilePath), ex);
                blnSuccess = false;
            }

            if (!blnSuccess)
            {
                blnSuccess = StoreToolVersionInfoViaSystemDiagnostics(ref strToolVersionInfo, strDLLFilePath);
            }

            return blnSuccess;

        }

        /// <summary>
        /// Determines the version info for a .NET DLL using System.Diagnostics.FileVersionInfo
        /// </summary>
        /// <param name="strToolVersionInfo">Version info string to append the version info to</param>
        /// <param name="strDLLFilePath">Path to the DLL</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfoViaSystemDiagnostics(ref string strToolVersionInfo, string strDLLFilePath)
        {

            try
            {
                var ioFileInfo = new FileInfo(strDLLFilePath);

                if (!ioFileInfo.Exists)
                {
                    m_message = "File not found by StoreToolVersionInfoViaSystemDiagnostics";
                    LogMessage(m_message + ": " + strDLLFilePath);
                    return false;
                }

                var oFileVersionInfo = FileVersionInfo.GetVersionInfo(strDLLFilePath);

                var strName = oFileVersionInfo.FileDescription;
                if (string.IsNullOrEmpty(strName))
                {
                    strName = oFileVersionInfo.InternalName;
                }

                if (string.IsNullOrEmpty(strName))
                {
                    strName = oFileVersionInfo.FileName;
                }

                if (string.IsNullOrEmpty(strName))
                {
                    strName = ioFileInfo.Name;
                }

                var strVersion = oFileVersionInfo.FileVersion;
                if (string.IsNullOrEmpty(strVersion))
                {
                    strVersion = oFileVersionInfo.ProductVersion;
                }

                if (string.IsNullOrEmpty(strVersion))
                {
                    strVersion = "??";
                }

                var strNameAndVersion = strName + ", Version=" + strVersion;
                strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion);

                return true;

            }
            catch (Exception ex)
            {
                LogError("Exception determining File Version for " + Path.GetFileName(strDLLFilePath), ex);
                return false;
            }

        }

        /// <summary>
        /// Uses the DLLVersionInspector to determine the version of a 32-bit .NET DLL or .Exe
        /// </summary>
        /// <param name="strToolVersionInfo"></param>
        /// <param name="strDLLFilePath"></param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfoOneFile32Bit(ref string strToolVersionInfo, string strDLLFilePath)
        {
            return StoreToolVersionInfoOneFileUseExe(ref strToolVersionInfo, strDLLFilePath, "DLLVersionInspector_x86.exe");
        }

        /// <summary>
        /// Uses the DLLVersionInspector to determine the version of a 64-bit .NET DLL or .Exe
        /// </summary>
        /// <param name="strToolVersionInfo"></param>
        /// <param name="strDLLFilePath"></param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfoOneFile64Bit(ref string strToolVersionInfo, string strDLLFilePath)
        {
            return StoreToolVersionInfoOneFileUseExe(ref strToolVersionInfo, strDLLFilePath, "DLLVersionInspector_x64.exe");
        }

        /// <summary>
        /// Uses the specified DLLVersionInspector to determine the version of a .NET DLL or .Exe
        /// </summary>
        /// <param name="strToolVersionInfo"></param>
        /// <param name="strDLLFilePath"></param>
        /// <param name="versionInspectorExeName">DLLVersionInspector_x86.exe or DLLVersionInspector_x64.exe</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfoOneFileUseExe(ref string strToolVersionInfo, string strDLLFilePath, string versionInspectorExeName)
        {

            try
            {
                var strAppPath = Path.Combine(clsGlobal.GetAppFolderPath(), versionInspectorExeName);

                var ioFileInfo = new FileInfo(strDLLFilePath);

                if (!ioFileInfo.Exists)
                {
                    m_message = "File not found by StoreToolVersionInfoOneFileUseExe";
                    LogMessage(m_message + ": " + strDLLFilePath, 0, true);
                    return false;
                }

                if (!File.Exists(strAppPath))
                {
                    m_message = "DLLVersionInspector not found by StoreToolVersionInfoOneFileUseExe";
                    LogMessage(m_message + ": " + strAppPath, 0, true);
                    return false;
                }

                // Call DLLVersionInspector_x86.exe or DLLVersionInspector_x64.exe to determine the tool version

                var strVersionInfoFilePath = Path.Combine(m_WorkDir, Path.GetFileNameWithoutExtension(ioFileInfo.Name) + "_VersionInfo.txt");

                string strVersion;

                var strArgs = PossiblyQuotePath(ioFileInfo.FullName) + " /O:" + PossiblyQuotePath(strVersionInfoFilePath);

                if (m_DebugLevel >= 3)
                {
                    LogDebug(strAppPath + " " + strArgs);
                }

                var progRunner = new clsRunDosProgram(clsGlobal.GetAppFolderPath())
                {
                    CacheStandardOutput = false,
                    CreateNoWindow = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = false,
                    DebugLevel = 1,
                    MonitorInterval = 250
                };
                RegisterEvents(progRunner);

                var blnSuccess = progRunner.RunProgram(strAppPath, strArgs, "DLLVersionInspector", false);

                if (!blnSuccess)
                {
                    return false;
                }

                Thread.Sleep(100);

                blnSuccess = ReadVersionInfoFile(strDLLFilePath, strVersionInfoFilePath, out strVersion);

                // Delete the version info file
                try
                {
                    if (File.Exists(strVersionInfoFilePath))
                    {
                        Thread.Sleep(100);
                        File.Delete(strVersionInfoFilePath);
                    }
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                if (!blnSuccess || string.IsNullOrWhiteSpace(strVersion))
                {
                    return false;
                }

                strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strVersion);

                return true;

            }
            catch (Exception ex)
            {
                m_message = "Exception determining Version info for " + Path.GetFileName(strDLLFilePath) + " using " + versionInspectorExeName;
                LogError(m_message, ex);
                strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, Path.GetFileNameWithoutExtension(strDLLFilePath));
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
        /// <param name="copySubfolders">If true, then recursively copies subfolders</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool SynchronizeFolders(string sourceFolderPath, string targetDirectoryPath, bool copySubfolders)
        {

            var lstFileNameFilterSpec = new List<string> { "*" };
            var lstFileNameExclusionSpec = new List<string>();
            const int maxRetryCount = 3;

            return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubfolders);
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
            const bool copySubfolders = false;

            return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubfolders);
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
            const bool copySubfolders = false;

            return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubfolders);
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
            const bool copySubfolders = false;

            return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubfolders);
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

            const bool copySubfolders = false;
            return SynchronizeFolders(sourceFolderPath, targetDirectoryPath, lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubfolders);

        }

        /// <summary>
        /// Copies new/changed files from the source folder to the target folder
        /// </summary>
        /// <param name="sourceFolderPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="lstFileNameFilterSpec">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
        /// <param name="lstFileNameExclusionSpec">One or more filename filters for excluding files; can use * as a wildcard</param>
        /// <param name="maxRetryCount">Will retry failed copies up to maxRetryCount times; use 0 for no retries</param>
        /// <param name="copySubfolders">If true, then recursively copies subfolders</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool SynchronizeFolders(
            string sourceFolderPath,
            string targetDirectoryPath,
            List<string> lstFileNameFilterSpec,
            List<string> lstFileNameExclusionSpec,
            int maxRetryCount,
            bool copySubfolders)
        {
            try
            {
                var diSourceFolder = new DirectoryInfo(sourceFolderPath);
                var ditargetDirectory = new DirectoryInfo(targetDirectoryPath);

                if (!ditargetDirectory.Exists)
                {
                    ditargetDirectory.Create();
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

                    foreach (var fiFile in diSourceFolder.GetFiles(filterSpecToUse))
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

                        foreach (var fiFile in diSourceFolder.GetFiles(filterSpec))
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
                    var fiSourceFile = new FileInfo(Path.Combine(diSourceFolder.FullName, fileName));
                    var fiTargetFile = new FileInfo(Path.Combine(ditargetDirectory.FullName, fileName));
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
                            success = m_FileTools.CopyFileUsingLocks(fiSourceFile, fiTargetFile.FullName, m_MachName, true);
                            if (!success)
                            {
                                retriesRemaining -= 1;
                                if (retriesRemaining < 0)
                                {
                                    m_message = "Error copying " + fiSourceFile.FullName + " to " + fiTargetFile.DirectoryName;
                                    return false;
                                }

                                LogMessage("Error copying " + fiSourceFile.FullName + " to " + fiTargetFile.DirectoryName + "; RetriesRemaining: " + retriesRemaining, 0, true);

                                // Wait 2 seconds then try again
                                Thread.Sleep(2000);
                            }
                        }

                    }
                }

                if (copySubfolders)
                {
                    var lstSubFolders = diSourceFolder.GetDirectories();

                    foreach (var diSubFolder in lstSubFolders)
                    {
                        var subfolderTargetPath = Path.Combine(targetDirectoryPath, diSubFolder.Name);
                        var success = SynchronizeFolders(diSubFolder.FullName, subfolderTargetPath,
                            lstFileNameFilterSpec, lstFileNameExclusionSpec, maxRetryCount, copySubfolders: true);

                        if (!success)
                        {
                            LogError("Error copying subfolder " + diSubFolder.FullName + " to " + targetDirectoryPath);
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
                var strTool = m_jobParams.GetParam("ToolName");

                var strToolAndStepTool = m_jobParams.GetParam("StepTool") ?? string.Empty;

                if (strToolAndStepTool != strTool)
                {
                    if (strToolAndStepTool.Length > 0)
                    {
                        strToolAndStepTool += " (" + strTool + ")";
                    }
                    else
                    {
                        strToolAndStepTool += strTool;
                    }
                }

                // Add the data
                m_SummaryFile.Add("Job Number" + '\t' + m_JobNum);
                m_SummaryFile.Add("Job Step" + '\t' + m_jobParams.GetParam("StepParameters", "Step"));
                m_SummaryFile.Add("Date" + '\t' + DateTime.Now);
                m_SummaryFile.Add("Processor" + '\t' + m_MachName);
                m_SummaryFile.Add("Tool" + '\t' + strToolAndStepTool);
                m_SummaryFile.Add("Dataset Name" + '\t' + m_Dataset);
                m_SummaryFile.Add("Xfer Folder" + '\t' + m_jobParams.GetParam("transferFolderPath"));
                m_SummaryFile.Add("Param File Name" + '\t' + m_jobParams.GetParam("parmFileName"));
                m_SummaryFile.Add("Settings File Name" + '\t' + m_jobParams.GetParam("settingsFileName"));
                m_SummaryFile.Add("Legacy Organism Db Name" + '\t' + m_jobParams.GetParam("LegacyFastaFileName"));
                m_SummaryFile.Add("Protein Collection List" + '\t' + m_jobParams.GetParam("ProteinCollectionList"));
                m_SummaryFile.Add("Protein Options List" + '\t' + m_jobParams.GetParam("ProteinOptions"));
                m_SummaryFile.Add("Fasta File Name" + '\t' + m_jobParams.GetParam("PeptideSearch", "generatedFastaName"));
                m_SummaryFile.Add("Analysis Time (hh:mm:ss)" + '\t' + CalcElapsedTime(m_StartTime, m_StopTime));

                // Add another separator
                m_SummaryFile.Add(Environment.NewLine);
                m_SummaryFile.Add("=====================================================================================");
                m_SummaryFile.Add(Environment.NewLine);

            }
            catch (Exception ex)
            {
                LogError("Error updating the summary file",
                         "Error updating the summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("StepParameters", "Step") + ": " + ex.Message);
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
            m_IonicZipTools.DebugLevel = m_DebugLevel;

            // Note that m_IonicZipTools logs error messages using clsLogTools
            return m_IonicZipTools.UnzipFile(zipFilePath, targetDirectory, FileFilter);

        }

        /// <summary>
        /// Reset the progrunner start time and the CPU usage queue
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
                List<int> processIDs;
                var processID = defaultProcessID;

                var coreUsage = PRISMWin.clsProcessStats.GetCoreUsageByProcessName(processName, out processIDs);
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
            // Note that the call to GetCoreUsage() will take at least 1 second
            var coreUsage = cmdRunner.GetCoreUsage();

            UpdateProgRunnerCpuUsage(cmdRunner.ProcessID, coreUsage, secondsBetweenUpdates);

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
            var strDTAFilePath = Path.Combine(m_WorkDir, m_Dataset + "_dta.txt");

            return ValidateCDTAFile(strDTAFilePath);
        }

        protected bool ValidateCDTAFile(string strDTAFilePath)
        {
            var blnDataFound = false;

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
                        var strLineIn = srReader.ReadLine();

                        if (!string.IsNullOrWhiteSpace(strLineIn))
                        {
                            blnDataFound = true;
                            break;
                        }
                    }

                }

                if (!blnDataFound)
                {
                    LogError("The _DTA.txt file is empty");
                }

            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateCDTAFile", ex);
                return false;
            }

            return blnDataFound;

        }

        /// <summary>
        /// Verifies that the zip file exists.
        /// If the file size is less than crcCheckThresholdGB, then also performs a full CRC check of the data
        /// </summary>
        /// <param name="zipFilePath">Zip file to check</param>
        /// <param name="crcCheckThresholdGB">Threshold (in GB) below which a full CRC check should be performed</param>
        /// <returns>True if a valid zip file, otherwise false</returns>
        protected bool VerifyZipFile(string zipFilePath, float crcCheckThresholdGB = 4)
        {
            m_IonicZipTools.DebugLevel = m_DebugLevel;

            // Note that m_IonicZipTools logs error messages using clsLogTools
            var success = m_IonicZipTools.VerifyZipFile(zipFilePath, crcCheckThresholdGB);

            return success;

        }

        /// <summary>
        /// Stores sourceFilePath in a zip file with the same name, but extension .zip
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="deleteSourceAfterZip">If True, then will delete the file after zipping it</param>
        /// <returns>True if success; false if an error</returns>
        public bool ZipFile(string sourceFilePath, bool deleteSourceAfterZip)
        {
            m_IonicZipTools.DebugLevel = m_DebugLevel;

            // Note that m_IonicZipTools logs error messages using clsLogTools
            var success = m_IonicZipTools.ZipFile(sourceFilePath, deleteSourceAfterZip);

            if (!success && m_IonicZipTools.Message.ToLower().Contains("OutOfMemoryException".ToLower()))
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
                        Thread.Sleep(250);

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
        /// <param name="deleteSourceAfterZip">If True, then will delete the file after zipping it</param>
        /// <param name="zipFilePath">Full path to the .zip file to be created.  Existing files will be overwritten</param>
        /// <returns>True if success; false if an error</returns>
        public bool ZipFile(string sourceFilePath, bool deleteSourceAfterZip, string zipFilePath)
        {
            m_IonicZipTools.DebugLevel = m_DebugLevel;

            // Note that m_IonicZipTools logs error messages using clsLogTools
            var success = m_IonicZipTools.ZipFile(sourceFilePath, deleteSourceAfterZip, zipFilePath);

            if (!success && m_IonicZipTools.Message.ToLower().Contains("OutOfMemoryException".ToLower()))
            {
                m_NeedToAbortProcessing = true;
            }

            return success;

        }

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

        private void mSortUtility_ErrorEvent(object sender, FlexibleFileSortUtility.MessageEventArgs e)
        {
            mSortUtilityErrorMessage = e.Message;
            LogMessage("SortUtility: " + e.Message, 0, true);
        }

        private void mSortUtility_MessageEvent(object sender, FlexibleFileSortUtility.MessageEventArgs e)
        {
            if (m_DebugLevel >= 1)
            {
                LogMessage(e.Message);
            }
        }

        private void mSortUtility_ProgressChanged(object sender, FlexibleFileSortUtility.ProgressChangedEventArgs e)
        {
            if (m_DebugLevel >= 1 && DateTime.UtcNow.Subtract(mLastSortUtilityProgress).TotalSeconds >= 5)
            {
                mLastSortUtilityProgress = DateTime.UtcNow;
                LogMessage(e.taskDescription + ": " + e.percentComplete.ToString("0.0") + "% complete");
            }
        }

        private void mSortUtility_WarningEvent(object sender, FlexibleFileSortUtility.MessageEventArgs e)
        {
            LogMessage("SortUtility Warning: " + e.Message);
        }

        #endregion

    }

}