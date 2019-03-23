using System;
using System.Collections.Generic;
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
        /// Default date/time format
        /// </summary>
        public const string DATE_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss tt";

        /// <summary>
        /// Failed results directory (typically on the C: drive)
        /// </summary>
        public const string DMS_FAILED_RESULTS_DIRECTORY_NAME = "DMS_FailedResults";

        /// <summary>
        /// Text to store in mMessage when no results passed filters
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
        /// Purge interval for cached server files
        /// </summary>
        private const int CACHED_SERVER_FILES_PURGE_INTERVAL_HOURS = 3;

        #endregion

        #region "Module variables"

        /// <summary>
        /// Access to the job parameters
        /// </summary>
        protected IJobParams mJobParams;

        /// <summary>
        /// Access to manager parameters
        /// </summary>
        protected IMgrParams mMgrParams;

        /// <summary>
        /// Access to settings file parameters
        /// </summary>
        protected readonly XmlSettingsFileAccessor mSettingsFileParams = new XmlSettingsFileAccessor();

        /// <summary>
        /// Progress of run (in percent)
        /// </summary>
        /// <remarks>This is a value between 0 and 100</remarks>
        protected float mProgress;

        /// <summary>
        /// Status code
        /// </summary>
        protected EnumMgrStatus mStatusCode;

        /// <summary>
        /// DTA count for status report
        /// </summary>
        protected int mDtaCount = 0;

        /// <summary>
        /// Can be used to pass codes regarding the results of this analysis back to the DMS_Pipeline DB
        /// </summary>
        protected int mEvalCode;

        /// <summary>
        /// Can be used to pass information regarding the results of this analysis back to the DMS_Pipeline DB
        /// </summary>
        protected string mEvalMessage = string.Empty;

        /// <summary>
        /// Working directory
        /// </summary>
        protected string mWorkDir;

        /// <summary>
        /// Manager name
        /// </summary>
        protected string mMgrName;

        /// <summary>
        /// Job number
        /// </summary>
        protected int mJob;

        /// <summary>
        /// Dataset name
        /// </summary>
        protected string mDatasetName;

        /// <summary>
        /// Analysis start time (UTC-based)
        /// </summary>
        protected DateTime mStartTime;

        /// <summary>
        /// Analysis end time
        /// </summary>
        protected DateTime mStopTime;

        /// <summary>
        /// Results directory name
        /// </summary>
        protected string mResultsDirectoryName;

        /// <summary>
        /// DLL file info
        /// </summary>
        protected string mFileVersion;

        /// <summary>
        /// DLL file date
        /// </summary>
        protected string mFileDate;

        /// <summary>
        /// DotNetZip tools
        /// </summary>
        protected clsDotNetZipTools mDotNetZipTools;

        /// <summary>
        /// Set to true if we need to abort processing as soon as possible
        /// </summary>
        protected bool mNeedToAbortProcessing;

        /// <summary>
        /// Analysis job summary file
        /// </summary>
        protected clsSummaryFile mSummaryFile;

        /// <summary>
        /// MyEMSL Utilities
        /// </summary>
        protected clsMyEMSLUtilities mMyEMSLUtilities;

        private DateTime mLastProgressWriteTime = DateTime.UtcNow;

        private DateTime mLastProgressConsoleTime = DateTime.UtcNow;

        private DateTime mLastStatusFileUpdate = DateTime.UtcNow;

        private DateTime mLastSortUtilityProgress;

        /// <summary>
        /// Tool Version Utilities
        /// </summary>
        protected clsToolVersionUtilities mToolVersionUtilities;

        private string mSortUtilityErrorMessage;

        /// <summary>
        /// Program runner start time
        /// </summary>
        protected DateTime mProgRunnerStartTime;

        private DateTime mLastCachedServerFilesPurgeCheck = DateTime.UtcNow.AddHours(-CACHED_SERVER_FILES_PURGE_INTERVAL_HOURS * 2);

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
        public string Dataset => mDatasetName;

        /// <summary>
        /// Evaluation code to be reported to the DMS_Pipeline DB
        /// </summary>
        public int EvalCode => mEvalCode;

        /// <summary>
        /// Evaluation message to be reported to the DMS_Pipeline DB
        /// </summary>
        public string EvalMessage => string.IsNullOrWhiteSpace(mEvalMessage) ? string.Empty : mEvalMessage;

        /// <summary>
        /// Job number
        /// </summary>
        public int Job => mJob;

        /// <summary>
        /// Publicly accessible results directory name and path
        /// </summary>
        public string ResFolderName => mResultsDirectoryName;

        /// <summary>
        /// Status message related to processing tasks performed by this class
        /// </summary>
        public string Message => string.IsNullOrWhiteSpace(mMessage) ? string.Empty : mMessage;

        /// <summary>
        /// Set this to true if we need to abort processing as soon as possible due to a critical error
        /// </summary>
        public bool NeedToAbortProcessing => mNeedToAbortProcessing;

        /// <summary>
        /// Progress of run (in percent)
        /// </summary>
        /// <remarks>This is a value between 0 and 100</remarks>
        public float Progress => mProgress;

        /// <summary>
        /// Time the analysis started (UTC-based)
        /// </summary>
        public DateTime StartTime => mStartTime;

        /// <summary>
        /// Step tool name
        /// </summary>
        public string StepToolName { get; private set; }

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

            mMgrParams = mgrParams;
            mJobParams = jobParams;
            mStatusTools = statusTools;

            TraceMode = mgrParams.TraceMode;

            mWorkDir = mMgrParams.GetParam("WorkDir");
            mMgrName = mMgrParams.ManagerName;

            mJob = mJobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0);

            mDatasetName = mJobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_DATASET_NAME);

            mMyEMSLUtilities = myEMSLUtilities ?? new clsMyEMSLUtilities(mDebugLevel, mWorkDir);
            RegisterEvents(mMyEMSLUtilities);

            mDebugLevel = (short)mMgrParams.GetParam("DebugLevel", 1);
            mStatusTools.Tool = mJobParams.GetCurrentJobToolDescription();

            mSummaryFile = summaryFile;

            mResultsDirectoryName = mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME);

            if (mDebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerBase.Setup()");
            }

            mDotNetZipTools = new clsDotNetZipTools(mDebugLevel, mWorkDir);
            RegisterEvents(mDotNetZipTools);

            InitFileTools(mMgrName, mDebugLevel);

            mToolVersionUtilities = new clsToolVersionUtilities(mgrParams, jobParams, mJob, mDatasetName, StepToolName, mDebugLevel, mWorkDir);
            RegisterEvents(mToolVersionUtilities);

            mNeedToAbortProcessing = false;

            mMessage = string.Empty;
            mEvalCode = 0;
            mEvalMessage = string.Empty;

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

            var elapsedTime = stopTime.Subtract(startTime);

            if (mDebugLevel >= 2)
            {
                LogDebug($"CalcElapsedTime: StartTime = {startTime}; StopTime = {stopTime}");

                LogDebug($"CalcElapsedTime: {elapsedTime.Hours} Hours, {elapsedTime.Minutes} Minutes, {elapsedTime.Seconds} Seconds");

                LogDebug($"CalcElapsedTime: TotalMinutes = {elapsedTime.TotalMinutes:0.00}");
            }

            return elapsedTime.Hours.ToString("###0") + ":" + elapsedTime.Minutes.ToString("00") + ":" + elapsedTime.Seconds.ToString("00");

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

            var freeMemoryMB = mStatusTools.GetFreeMemoryMB();

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
        /// mJobParams.AddResultFileToSkip and mJobParams.AddResultFileExtensionToSkip
        /// Step tools may override this method if additional steps are required
        /// The override method should then call base.CopyFailedResultsToArchiveFolder as the last step
        /// </remarks>
        public virtual void CopyFailedResultsToArchiveDirectory()
        {
            if (clsGlobal.OfflineMode)
            {
                // Offline mode jobs each have their own work directory
                // Thus, copying of failed results is not applicable
                LogWarning("Processing interrupted; see local work directory: " + mWorkDir);
                return;
            }

            var failedResultsDirectoryPath = mMgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrWhiteSpace(failedResultsDirectoryPath))
            {
                LogErrorToDatabase("Manager parameter FailedResultsFolderPath not defined for manager " + mMgrParams.ManagerName);
                failedResultsDirectoryPath = @"C:\" + DMS_FAILED_RESULTS_DIRECTORY_NAME;
            }

            LogWarning("Processing interrupted; copying results to archive directory: " + failedResultsDirectoryPath);

            // Bump up the debug level if less than 2
            if (mDebugLevel < 2)
                mDebugLevel = 2;

            // Try to save whatever files are in the work directory (however, delete the _DTA.txt and _DTA.zip files first)
            var directoryPathToArchive = string.Copy(mWorkDir);

            // Make the results directory
            var success = MakeResultsDirectory();
            if (success)
            {
                // Move the result files into the results directory
                var moveSucceed = MoveResultFiles();
                if (moveSucceed)
                {
                    // Move was a success; update folderPathToArchive
                    directoryPathToArchive = Path.Combine(mWorkDir, mResultsDirectoryName);
                }
            }

            // Copy the results directory to the Archive directory
            var analysisResults = new clsAnalysisResults(mMgrParams, mJobParams);
            analysisResults.CopyFailedResultsToArchiveDirectory(directoryPathToArchive, failedResultsDirectoryPath);
        }

        /// <summary>
        /// Copies a file (typically a mzXML or mzML file) to a server cache directory
        /// Will store the file in a subdirectory based on job parameter OutputFolderName, and below that, in a directory with a name like 2013_2
        /// </summary>
        /// <param name="cacheDirectoryPath">Cache directory base path, e.g. \\proto-6\MSXML_Cache</param>
        /// <param name="sourceFilePath">Path to the data file</param>
        /// <param name="purgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
        /// <returns>Path to the remotely cached file; empty path if an error</returns>
        protected string CopyFileToServerCache(string cacheDirectoryPath, string sourceFilePath, bool purgeOldFilesIfNeeded)
        {

            try
            {
                // mResultsDirectoryName should contain the output directory; e.g. MSXML_Gen_1_120_275966
                if (string.IsNullOrEmpty(mResultsDirectoryName))
                {
                    LogError("mResultsDirectoryName (from job parameter OutputFolderName) is empty; cannot construct MSXmlCache path");
                    return string.Empty;
                }

                // Remove the dataset ID portion from the output directory
                string toolNameVersionFolder;
                try
                {
                    toolNameVersionFolder = clsAnalysisResources.GetMSXmlToolNameVersionFolder(mResultsDirectoryName);
                }
                catch (Exception)
                {
                    LogError("OutputFolderName is not in the expected form of ToolName_Version_DatasetID (" + mResultsDirectoryName + "); cannot construct MSXmlCache path");
                    return string.Empty;
                }

                // Determine the year_quarter text for this dataset
                var datasetStoragePath = mJobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetStoragePath");
                if (string.IsNullOrEmpty(datasetStoragePath))
                    datasetStoragePath = mJobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetArchivePath");

                var datasetYearQuarter = clsAnalysisResources.GetDatasetYearQuarter(datasetStoragePath);
                if (string.IsNullOrEmpty(datasetYearQuarter))
                {
                    LogError("Unable to determine DatasetYearQuarter using the DatasetStoragePath or DatasetArchivePath; cannot construct MSXmlCache path");
                    return string.Empty;
                }


                var success = CopyFileToServerCache(
                    cacheDirectoryPath, toolNameVersionFolder, sourceFilePath, datasetYearQuarter,
                    purgeOldFilesIfNeeded: purgeOldFilesIfNeeded, remoteCacheFilePath: out var remoteCacheFilePath);

                if (!success)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError(string.Format("CopyFileToServerCache returned false copying the {0} file to {1}",
                                               Path.GetExtension(sourceFilePath),
                                               Path.Combine(cacheDirectoryPath, toolNameVersionFolder)));
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
        /// Copies a file (typically a mzXML or mzML file) to a server cache directory
        /// Will store the file in the subdirectory subDirectoryInTarget and, below that, in a directory with a name like 2013_2
        /// </summary>
        /// <param name="cacheDirectoryPath">Cache directory base path, e.g. \\proto-6\MSXML_Cache</param>
        /// <param name="subDirectoryInTarget">Directory name to create below cacheDirectoryPath (optional), e.g. MSXML_Gen_1_93 or MSConvert</param>
        /// <param name="sourceFilePath">Path to the data file</param>
        /// <param name="datasetYearQuarter">
        /// Dataset year quarter text (optional)
        /// Example value is 2013_2; if this this parameter is blank, will auto-determine using Job Parameter DatasetStoragePath
        /// </param>
        /// <param name="purgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>
        /// Determines the Year_Quarter directory named using the DatasetStoragePath or DatasetArchivePath job parameter
        /// If those parameters are not defined, copies the file anyway
        /// </remarks>
        protected bool CopyFileToServerCache(
            string cacheDirectoryPath,
            string subDirectoryInTarget,
            string sourceFilePath,
            string datasetYearQuarter,
            bool purgeOldFilesIfNeeded)
        {
            return CopyFileToServerCache(
                cacheDirectoryPath, subDirectoryInTarget, sourceFilePath,
                datasetYearQuarter, purgeOldFilesIfNeeded, out _);

        }

        /// <summary>
        /// Copies a file (typically a mzXML or mzML file) to a server cache directory
        /// Will store the file in the directory subDirectoryInTarget and, below that, in a directory with a name like 2013_2
        /// </summary>
        /// <param name="cacheDirectoryPath">Cache directory base path, e.g. \\proto-11\MSXML_Cache</param>
        /// <param name="subDirectoryInTarget">Directory name to create below cacheDirectoryPath (optional), e.g. MSXML_Gen_1_93 or MSConvert</param>
        /// <param name="sourceFilePath">Path to the data file</param>
        /// <param name="datasetYearQuarter">
        /// Dataset year quarter text (optional)
        /// Example value is 2013_2; if this this parameter is blank, will auto-determine using Job Parameter DatasetStoragePath
        /// </param>
        /// <param name="purgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
        /// <param name="remoteCacheFilePath">Output parameter: the target file path (determined by this function)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>
        /// Determines the Year_Quarter directory named using the DatasetStoragePath or DatasetArchivePath job parameter
        /// If those parameters are not defined, copies the file anyway
        /// </remarks>
        protected bool CopyFileToServerCache(
            string cacheDirectoryPath,
            string subDirectoryInTarget, string
            sourceFilePath,
            string datasetYearQuarter,
            bool purgeOldFilesIfNeeded,
            out string remoteCacheFilePath)
        {

            remoteCacheFilePath = string.Empty;

            try
            {
                var cacheDirectory = new DirectoryInfo(cacheDirectoryPath);

                if (!cacheDirectory.Exists)
                {
                    LogWarning("Cache directory not found: " + cacheDirectoryPath);
                    return false;
                }

                DirectoryInfo targetDirectory;

                // Define the target directory
                if (string.IsNullOrEmpty(subDirectoryInTarget))
                {
                    targetDirectory = cacheDirectory;
                }
                else
                {
                    targetDirectory = new DirectoryInfo(Path.Combine(cacheDirectory.FullName, subDirectoryInTarget));
                    if (!targetDirectory.Exists)
                        targetDirectory.Create();
                }

                if (string.IsNullOrEmpty(datasetYearQuarter))
                {
                    // Determine the year_quarter text for this dataset
                    var datasetStoragePath = mJobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetStoragePath");
                    if (string.IsNullOrEmpty(datasetStoragePath))
                        datasetStoragePath = mJobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetArchivePath");

                    datasetYearQuarter = clsAnalysisResources.GetDatasetYearQuarter(datasetStoragePath);
                }

                if (!string.IsNullOrEmpty(datasetYearQuarter))
                {
                    targetDirectory = new DirectoryInfo(Path.Combine(targetDirectory.FullName, datasetYearQuarter));
                    if (!targetDirectory.Exists)
                        targetDirectory.Create();
                }

                mJobParams.AddResultFileExtensionToSkip(clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX);

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

                var targetFile = new FileInfo(Path.Combine(targetDirectory.FullName, sourceFileName));

                ResetTimestampForQueueWaitTimeLogging();
                var startTime = DateTime.UtcNow;

                var success = mFileTools.CopyFileUsingLocks(sourceFilePath, targetFile.FullName, true);
                LogCopyStats(startTime, targetFile.FullName);

                if (!success)
                {
                    LogError("CopyFileUsingLocks returned false copying " + Path.GetFileName(sourceFilePath) + " to " + targetFile.FullName);
                    return false;
                }

                remoteCacheFilePath = targetFile.FullName;

                if (targetFile.DirectoryName == null)
                {
                    LogError("DirectoryName is null for the target directory; cannot copy the file to the remote cache");
                    return false;
                }

                // Copy over the .Hashcheck file
                mFileTools.CopyFile(hashcheckFilePath, Path.Combine(targetFile.DirectoryName, Path.GetFileName(hashcheckFilePath)), true);

                if (purgeOldFilesIfNeeded)
                {
                    PurgeOldServerCacheFiles(cacheDirectoryPath);
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
        /// Copies the .mzXML file to the generic MSXML_Cache directory, e.g. \\proto-6\MSXML_Cache\MSConvert
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <param name="datasetYearQuarter">Dataset year quarter text, e.g. 2013_2; if this this parameter is blank, will auto-determine using Job Parameter DatasetStoragePath</param>
        /// <param name="msXmlGeneratorName">Name of the MzXML generator, e.g. MSConvert</param>
        /// <param name="purgeOldFilesIfNeeded">Set to True to automatically purge old files if the space usage is over 20 TB</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>
        /// Contrast with CopyMSXmlToCache in clsAnalysisToolRunnerMSXMLGen, where the target directory is
        /// of the form \\proto-6\MSXML_Cache\MSConvert\MSXML_Gen_1_93
        /// </remarks>
        protected bool CopyMzXMLFileToServerCache(string sourceFilePath, string datasetYearQuarter, string msXmlGeneratorName, bool purgeOldFilesIfNeeded)
        {

            try
            {
                var msXMLCacheDirectoryPath = mMgrParams.GetParam(clsAnalysisResources.JOB_PARAM_MSXML_CACHE_FOLDER_PATH, string.Empty);

                if (string.IsNullOrEmpty(msXmlGeneratorName))
                {
                    msXmlGeneratorName = mJobParams.GetJobParameter("MSXMLGenerator", string.Empty);

                    if (!string.IsNullOrEmpty(msXmlGeneratorName))
                    {
                        msXmlGeneratorName = Path.GetFileNameWithoutExtension(msXmlGeneratorName);
                    }
                }

                var success = CopyFileToServerCache(msXMLCacheDirectoryPath, msXmlGeneratorName, sourceFilePath, datasetYearQuarter, purgeOldFilesIfNeeded);
                return success;

            }
            catch (Exception ex)
            {
                LogError("Error in CopyMzXMLFileToServerCache", ex);
                return false;
            }

        }

        /// <summary>
        /// Copies the files from the results directory to the transfer directory on the server
        /// </summary>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks></remarks>
        protected bool CopyResultsFolderToServer()
        {

            var transferDirectoryPath = GetTransferFolderPath();

            if (string.IsNullOrEmpty(transferDirectoryPath))
            {
                // Error has already been logged and mMessage has been updated
                return false;
            }

            return CopyResultsFolderToServer(transferDirectoryPath);
        }

        /// <summary>
        /// Copies the files from the results directory to the transfer directory on the server
        /// </summary>
        /// <param name="transferDirectoryPath">Base transfer directory path to use
        /// e.g. \\proto-6\DMS3_Xfer\ or
        /// \\protoapps\PeptideAtlas_Staging\1000_DataPackageName</param>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks></remarks>
        protected bool CopyResultsFolderToServer(string transferDirectoryPath)
        {

            var sourceDirectoryPath = string.Empty;
            string targetDirectoryPath;

            var analysisResults = new clsAnalysisResults(mMgrParams, mJobParams);

            var errorEncountered = false;
            var failedFileCount = 0;

            const int retryCount = 10;
            const int retryHoldoffSeconds = 15;
            const bool increaseHoldoffOnEachRetry = true;

            try
            {
                mStatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.DELIVERING_RESULTS, 0);

                if (string.IsNullOrEmpty(mResultsDirectoryName))
                {
                    // Log this error to the database (the logger will also update the local log file)
                    LogErrorToDatabase("Results directory name is not defined, job " + Job);
                    mMessage = "Results directory name is not defined";

                    // Without a source directory; there isn't much we can do
                    return false;
                }

                sourceDirectoryPath = Path.Combine(mWorkDir, mResultsDirectoryName);

                // Verify the source directory exists
                if (!Directory.Exists(sourceDirectoryPath))
                {
                    // Log this error to the database
                    LogErrorToDatabase("Results directory not found, " + mJobParams.GetJobStepDescription() + ", directory " + sourceDirectoryPath);
                    mMessage = "Results directory not found: " + sourceDirectoryPath;

                    // Without a source directory; there isn't much we can do
                    return false;
                }

                // Determine the remote transfer directory path (create it if missing)
                targetDirectoryPath = CreateRemoteTransferFolder(analysisResults, transferDirectoryPath);
                if (string.IsNullOrEmpty(targetDirectoryPath))
                {
                    analysisResults.CopyFailedResultsToArchiveDirectory(sourceDirectoryPath);
                    return false;
                }

            }
            catch (Exception ex)
            {
                LogError("Error creating results directory in transfer directory", ex);
                if (!string.IsNullOrEmpty(sourceDirectoryPath))
                {
                    analysisResults.CopyFailedResultsToArchiveDirectory(sourceDirectoryPath);
                }

                return false;
            }

            // Copy results directory to transfer directory
            // Existing files will be overwritten if they exist in htFilesToOverwrite (with the assumption that the files created by this manager are newer, and thus supersede existing files)

            try
            {
                // Copy all of the files and subdirectories in the local results directory to the target directory

                // Copy the files and subdirectories
                var success = CopyResultsFolderRecursive(
                    sourceDirectoryPath, sourceDirectoryPath, targetDirectoryPath, analysisResults,
                    ref errorEncountered, ref failedFileCount, retryCount,
                    retryHoldoffSeconds, increaseHoldoffOnEachRetry);

                if (!success)
                    errorEncountered = true;

            }
            catch (Exception ex)
            {
                LogError("Error copying results directory to " + Path.GetPathRoot(targetDirectoryPath), ex);
                errorEncountered = true;
            }

            if (errorEncountered)
            {
                // Message will be of the form
                // Error copying 1 file to transfer directory
                // or
                // Error copying 3 files to transfer directory

                var msg = "Error copying " + failedFileCount +
                    clsGlobal.CheckPlural(failedFileCount, " file", " files") +
                    " to transfer directory";
                LogError(msg);
                analysisResults.CopyFailedResultsToArchiveDirectory(sourceDirectoryPath);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Copies each of the files in the source directory to the target directory
        /// Uses CopyFileWithRetry to retry the copy up to retryCount times
        /// </summary>
        /// <param name="rootSourceDirectoryPath"></param>
        /// <param name="sourceDirectoryPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="analysisResults"></param>
        /// <param name="errorEncountered"></param>
        /// <param name="failedFileCount"></param>
        /// <param name="retryCount"></param>
        /// <param name="retryHoldoffSeconds"></param>
        /// <param name="increaseHoldoffOnEachRetry"></param>
        /// <returns></returns>
        private bool CopyResultsFolderRecursive(
            string rootSourceDirectoryPath,
            string sourceDirectoryPath,
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
                    // The target directory already exists

                    // Examine the files in the results directory to see if any of the files already exist in the transfer directory
                    // If they do, compare the file modification dates and post a warning if a file will be overwritten (because the file on the local computer is newer)
                    // However, if file sizes differ, replace the file

                    var resultsDirectory = new DirectoryInfo(sourceDirectoryPath);
                    foreach (var sourceFile in resultsDirectory.GetFiles())
                    {
                        if (!File.Exists(Path.Combine(targetDirectoryPath, sourceFile.Name)))
                            continue;

                        var targetFile = new FileInfo(Path.Combine(targetDirectoryPath, sourceFile.Name));

                        if (sourceFile.Length == targetFile.Length && sourceFile.LastWriteTimeUtc <= targetFile.LastWriteTimeUtc)
                            continue;

                        var message = "File in transfer directory on server will be overwritten by newer file in results directory: " + sourceFile.Name +
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
                    // Need to create the target directory
                    try
                    {
                        analysisResults.CreateFolderWithRetry(targetDirectoryPath);
                    }
                    catch (Exception ex)
                    {
                        LogError("Error creating results directory in transfer directory, " + Path.GetPathRoot(targetDirectoryPath), ex);
                        analysisResults.CopyFailedResultsToArchiveDirectory(rootSourceDirectoryPath);
                        return false;
                    }
                }

            }
            catch (Exception ex)
            {
                LogError("Error comparing files in source directory to " + targetDirectoryPath, ex);
                analysisResults.CopyFailedResultsToArchiveDirectory(rootSourceDirectoryPath);
                return false;
            }

            var sourceDirectory = new DirectoryInfo(sourceDirectoryPath);

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

                success = CopyResultsFolderRecursive(rootSourceDirectoryPath, subDirectory.FullName, targetDirectoryPathCurrent, analysisResults,
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
        /// Uses MakeResultsDirectory, MoveResultFiles, and CopyResultsFolderToServer
        /// Step tools can override this method if custom steps are required prior to packaging and transferring the results
        /// </remarks>
        public virtual bool CopyResultsToTransferDirectory(string transferDirectoryPathOverride = "")
        {
            if (clsGlobal.OfflineMode)
            {
                LogDebug("Offline mode is enabled; leaving results in the working directory: " + mWorkDir);
                return true;
            }

            var success = MakeResultsDirectory();
            if (!success)
            {
                // MakeResultsDirectory handles posting to local log, so set database error message and exit
                mMessage = "Error making results directory";
                return false;
            }

            var moveSucceed = MoveResultFiles();
            if (!moveSucceed)
            {
                // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                mMessage = "Error moving files into results directory";
                return false;
            }

            bool copySuccess;

            if (string.IsNullOrWhiteSpace(transferDirectoryPathOverride))
            {
                copySuccess = CopyResultsFolderToServer();
            }
            else
            {
                copySuccess = CopyResultsFolderToServer(transferDirectoryPathOverride);
            }

            return copySuccess;
        }

        /// <summary>
        /// Determines the path to the remote transfer directory
        /// Creates the directory if it does not exist
        /// </summary>
        /// <returns>The full path to the remote transfer directory; an empty string if an error</returns>
        /// <remarks></remarks>
        protected string CreateRemoteTransferFolder(clsAnalysisResults analysisResults)
        {

            var transferDirectoryPath = mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH);

            // Verify transfer directory exists
            // First make sure TransferFolderPath is defined
            if (string.IsNullOrEmpty(transferDirectoryPath))
            {
                var msg = "Transfer directory path not defined";
                LogError(msg, msg + "; job param 'transferFolderPath' is empty");
                return string.Empty;
            }

            return CreateRemoteTransferFolder(analysisResults, transferDirectoryPath);

        }

        /// <summary>
        /// Determines the path to the remote transfer directory
        /// Creates the directory if it does not exist
        /// </summary>
        /// <param name="analysisResults">Analysis results object</param>
        /// <param name="transferDirectoryPath">Base transfer directory path, e.g. \\proto-11\DMS3_Xfer\</param>
        /// <returns>The full path to the remote transfer directory; an empty string if an error</returns>
        protected string CreateRemoteTransferFolder(clsAnalysisResults analysisResults, string transferDirectoryPath)
        {

            if (string.IsNullOrEmpty(mResultsDirectoryName))
            {
                LogError("Results directory name is not defined, " + mJobParams.GetJobStepDescription());
                mMessage = "Results directory job parameter not defined (OutputFolderName)";
                return string.Empty;
            }

            // Verify that the transfer directory exists
            // If this is an Aggregation job, we create missing directories later in this method
            try
            {
                var directoryExists = analysisResults.FolderExistsWithRetry(transferDirectoryPath);

                if (!directoryExists && !clsGlobal.IsMatch(Dataset, "Aggregation"))
                {
                    LogError("Transfer directory not found: " + transferDirectoryPath);
                    return string.Empty;
                }

            }
            catch (Exception ex)
            {
                LogError("Error verifying transfer directory, " + Path.GetPathRoot(transferDirectoryPath), ex);
                return string.Empty;
            }

            // Determine if dataset directory in transfer directory already exists; make directory if it doesn't exist
            // First make sure "DatasetFolderName" or "DatasetNum" is defined
            if (string.IsNullOrEmpty(Dataset))
            {
                LogError("Dataset name is undefined, " + mJobParams.GetJobStepDescription());
                mMessage = "Dataset name is undefined";
                return string.Empty;
            }

            string remoteTransferDirectoryPath;

            if (clsGlobal.IsMatch(Dataset, "Aggregation"))
            {
                // Do not append "Aggregation" to the path since this is a generic dataset name applied to jobs that use Data Packages
                remoteTransferDirectoryPath = string.Copy(transferDirectoryPath);
            }
            else
            {
                // Append the dataset directory name to the transfer directory path
                var datasetDirectoryName = mJobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME);
                if (string.IsNullOrWhiteSpace(datasetDirectoryName))
                    datasetDirectoryName = Dataset;
                remoteTransferDirectoryPath = Path.Combine(transferDirectoryPath, datasetDirectoryName);
            }

            // Create the target directory if it doesn't exist
            try
            {
                analysisResults.CreateFolderWithRetry(remoteTransferDirectoryPath, maxRetryCount: 5, retryHoldoffSeconds: 20, increaseHoldoffOnEachRetry: true);
            }
            catch (Exception ex)
            {
                LogError("Error creating dataset directory in transfer directory, " + Path.GetPathRoot(remoteTransferDirectoryPath), ex);
                return string.Empty;
            }

            // Now append the output directory name to remoteTransferDirectoryPath
            return Path.Combine(remoteTransferDirectoryPath, mResultsDirectoryName);

        }

        /// <summary>
        /// Makes up to 3 attempts to delete specified file
        /// </summary>
        /// <param name="FileNamePath">Full path to file for deletion</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Raises exception if error occurs</remarks>
        public bool DeleteFileWithRetries(string FileNamePath)
        {
            return DeleteFileWithRetries(FileNamePath, mDebugLevel, 3);
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
                    // File may be read-only. Clear read-only and system attributes and try again
                    if (debugLevel > 0)
                    {
                        LogTools.LogDebug("File " + fileNamePath + " exception ex1: " + ex1.Message);
                        if (ex1.InnerException != null)
                        {
                            LogTools.LogDebug("Inner exception: " + ex1.InnerException.Message);
                        }
                        LogTools.LogDebug("File " + fileNamePath + " may be read-only, attribute reset attempt #" + retryCount);
                    }
                    File.SetAttributes(fileNamePath, File.GetAttributes(fileNamePath) & ~FileAttributes.ReadOnly & ~FileAttributes.System);
                    errType = AMFileNotDeletedAfterRetryException.RetryExceptionType.Unauthorized_Access_Exception;
                    retryCount += 1;

                }
                catch (IOException ex2)
                {
                    // If problem is locked file, attempt to fix lock and retry
                    if (debugLevel > 0)
                    {
                        LogTools.LogDebug("File " + fileNamePath + " exception ex2: " + ex2.Message);
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
                    var msg = "Error deleting file, exception ex3 " + fileNamePath + ex3.Message;
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
            var rawDataType = mJobParams.GetParam("RawDataType");

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

            // Deletes the raw data files/directories from the working directory
            bool isFile;
            var isNetworkDir = false;
            var fileOrDirectoryName = string.Empty;

            switch (eRawDataType)
            {
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                    fileOrDirectoryName = Path.Combine(mWorkDir, Dataset + clsAnalysisResources.DOT_RAW_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.AgilentQStarWiffFile:
                    fileOrDirectoryName = Path.Combine(mWorkDir, Dataset + clsAnalysisResources.DOT_WIFF_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.UIMF:
                    fileOrDirectoryName = Path.Combine(mWorkDir, Dataset + clsAnalysisResources.DOT_UIMF_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzXML:
                    fileOrDirectoryName = Path.Combine(mWorkDir, Dataset + clsAnalysisResources.DOT_MZXML_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzML:
                    fileOrDirectoryName = Path.Combine(mWorkDir, Dataset + clsAnalysisResources.DOT_MZML_EXTENSION);
                    isFile = true;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                    fileOrDirectoryName = Path.Combine(mWorkDir, Dataset + clsAnalysisResources.DOT_D_EXTENSION);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.MicromassRawFolder:
                    fileOrDirectoryName = Path.Combine(mWorkDir, Dataset + clsAnalysisResources.DOT_RAW_EXTENSION);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.ZippedSFolders:

                    var newSourceDirectory = clsAnalysisResources.ResolveSerStoragePath(mWorkDir);

                    // Check for "0.ser" folder
                    if (string.IsNullOrEmpty(newSourceDirectory))
                    {
                        fileOrDirectoryName = Path.Combine(mWorkDir, Dataset);
                        // isNetworkDir = false;
                    }
                    else
                    {
                        isNetworkDir = true;
                    }

                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerFTFolder:
                    // Bruker_FT directories are actually .D directories
                    fileOrDirectoryName = Path.Combine(mWorkDir, Dataset + clsAnalysisResources.DOT_D_EXTENSION);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDISpot:
                    ////////////////////////////////////
                    // TODO: Finalize this code
                    //       DMS doesn't yet have a BrukerTOF dataset
                    //       so we don't know the official directory structure
                    ////////////////////////////////////

                    fileOrDirectoryName = Path.Combine(mWorkDir, Dataset);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerMALDIImaging:

                    ////////////////////////////////////
                    // TODO: Finalize this code
                    //       DMS doesn't yet have a BrukerTOF dataset
                    //       so we don't know the official directory structure
                    ////////////////////////////////////

                    fileOrDirectoryName = Path.Combine(mWorkDir, Dataset);
                    isFile = false;

                    break;
                case clsAnalysisResources.eRawDataTypeConstants.BrukerTOFBaf:

                    // BrukerTOFBaf directories are actually .D directories
                    fileOrDirectoryName = Path.Combine(mWorkDir, Dataset + clsAnalysisResources.DOT_D_EXTENSION);
                    isFile = false;

                    break;
                default:
                    // Should never get this value
                    mMessage = "DeleteRawDataFiles, Invalid RawDataType specified: " + eRawDataType;
                    return false;
            }

            if (isFile)
            {
                // Data is a file, so use file deletion tools
                try
                {
                    if (!File.Exists(fileOrDirectoryName))
                    {
                        // File not found; treat this as a success
                        return true;
                    }

                    // DeleteFileWithRetries will throw an exception if it cannot delete any raw data files (e.g. the .UIMF file)
                    // Thus, need to wrap it with an Exception handler

                    if (DeleteFileWithRetries(fileOrDirectoryName))
                    {
                        return true;
                    }

                    LogError("Error deleting raw data file " + fileOrDirectoryName);
                    return false;
                }
                catch (Exception ex)
                {
                    LogError("Exception deleting raw data file " + fileOrDirectoryName, ex);
                    return false;
                }
            }

            if (isNetworkDir)
            {
                // The files were on the network and do not need to be deleted

            }
            else
            {
                // Use directory deletion tools
                try
                {
                    if (Directory.Exists(fileOrDirectoryName))
                    {
                        Directory.Delete(fileOrDirectoryName, true);
                    }
                    return true;
                }
                catch (Exception ex)
                {
                    LogError("Exception deleting raw data directory " + fileOrDirectoryName, ex);
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
        /// <remarks>If the program is not found, mMessage will be updated with the error message</remarks>
        protected string DetermineProgramLocation(string progLocManagerParamName, string exeName)
        {
            var progLoc = DetermineProgramLocation(mMgrParams, mJobParams, StepToolName, progLocManagerParamName, exeName, out var errorMessage);

            if (!string.IsNullOrEmpty(errorMessage))
            {
                // The error has already been logged, but we need to update mMessage
                mMessage = clsGlobal.AppendToComment(mMessage, errorMessage);
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
                    errorMessage = "Version-specific directory not found for " + stepToolName;
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

            var extractedParams = ExtractPackedJobParameterList(packedJobParameterName);

            foreach (var paramEntry in extractedParams)
            {
                var equalsIndex = paramEntry.LastIndexOf('=');
                if (equalsIndex > 0)
                {
                    var key = paramEntry.Substring(0, equalsIndex);
                    var value = paramEntry.Substring(equalsIndex + 1);

                    if (!dctData.ContainsKey(key))
                    {
                        dctData.Add(key, value);
                    }
                }
                else
                {
                    LogError("Packed dictionary item does not contain an equals sign: " + paramEntry);
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

            var packedJobParams = mJobParams.GetJobParameter(packedJobParameterName, string.Empty);

            if (string.IsNullOrEmpty(packedJobParams))
            {
                return new List<string>();
            }

            // Split the list on tab characters
            return packedJobParams.Split('\t').ToList();
        }

        /// <summary>
        /// Looks up the current debug level for the manager.  If the call to the server fails, mDebugLevel will be left unchanged
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool GetCurrentMgrSettingsFromDB()
        {
            return GetCurrentMgrSettingsFromDB(0);
        }

        /// <summary>
        /// Looks up the current debug level for the manager.  If the call to the server fails, mDebugLevel will be left unchanged
        /// </summary>
        /// <param name="updateIntervalSeconds">
        /// The minimum number of seconds between updates
        /// If fewer than updateIntervalSeconds seconds have elapsed since the last call to this function, no update will occur
        /// </param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool GetCurrentMgrSettingsFromDB(int updateIntervalSeconds)
        {
            return GetCurrentMgrSettingsFromDB(updateIntervalSeconds, mMgrParams, ref mDebugLevel);
        }

        /// <summary>
        /// Looks up the current debug level for the manager.  If the call to the server fails, DebugLevel will be left unchanged
        /// </summary>
        /// <param name="updateIntervalSeconds">Update interval, in seconds</param>
        /// <param name="mgrParams">Manager params</param>
        /// <param name="debugLevel">Input/Output parameter: set to the current debug level, will be updated to the debug level in the manager control DB</param>
        /// <returns>True for success; False for error</returns>
        /// <remarks></remarks>
        public static bool GetCurrentMgrSettingsFromDB(int updateIntervalSeconds, IMgrParams mgrParams, ref short debugLevel)
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
                var connectionString = mgrParams.GetParam("MgrCnfgDbConnectStr");
                var managerName = mgrParams.ManagerName;

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
            var success = clsGlobal.GetQueryResults(sqlQuery, connectionString, out var mgrParamsFromDb, callingFunctions);

            if (!success || mgrParamsFromDb.Count <= 0)
                return currentDebugLevel;

            foreach (var resultRow in mgrParamsFromDb)
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

            var javaProgLoc = GetJavaProgLoc(mMgrParams, out var errorMessage);

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

            var msXmlGeneratorExe = GetMSXmlGeneratorExeName();

            string msXmlGeneratorAppPath;

            if (msXmlGeneratorExe.ToLower().Contains("readw"))
            {
                // ReadW
                // Note that msXmlGenerator will likely be ReAdW.exe
                msXmlGeneratorAppPath = DetermineProgramLocation("ReAdWProgLoc", msXmlGeneratorExe);

            }
            else if (msXmlGeneratorExe.ToLower().Contains("msconvert"))
            {
                // MSConvert
                var proteoWizardDir = mMgrParams.GetParam("ProteoWizardDir");

                // MSConvert.exe is stored in the ProteoWizard directory
                msXmlGeneratorAppPath = Path.Combine(proteoWizardDir, msXmlGeneratorExe);

            }
            else
            {
                LogError("Invalid value for MSXMLGenerator; should be 'ReadW' or 'MSConvert'");
                msXmlGeneratorAppPath = string.Empty;
            }

            return msXmlGeneratorAppPath;

        }

        /// <summary>
        /// Returns the name of the .Exe to use to convert a dataset to a .mzXML file
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        protected string GetMSXmlGeneratorExeName()
        {
            // Determine the path to the XML Generator
            // ReadW.exe or MSConvert.exe (code will assume ReadW.exe if an empty string)
            var msXmlGeneratorExe = mJobParams.GetParam("MSXMLGenerator");

            if (string.IsNullOrEmpty(msXmlGeneratorExe))
            {
                // Assume we're using MSConvert
                msXmlGeneratorExe = clsToolVersionUtilities.MSCONVERT_EXE_NAME;
            }

            return msXmlGeneratorExe;
        }

        /// <summary>
        /// Determines the directory that contains R.exe and Rcmd.exe (queries the registry)
        /// </summary>
        /// <returns>Directory path, e.g. C:\Program Files\R\R-3.2.2\bin\x64</returns>
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
                    // From 2.12.0, DLLs are installed in either i386 or x64 (or both) below the bin directory
                    // The bin directory has an R.exe file but it does not have Rcmd.exe or R.dll
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
        /// Lookup the base transfer directory path
        /// </summary>
        /// <returns></returns>
        /// <remarks>For example, \\proto-7\DMS3_XFER\</remarks>
        protected string GetTransferFolderPath()
        {

            var transferDirectoryPath = mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH);

            if (string.IsNullOrEmpty(transferDirectoryPath))
            {
                LogError("Transfer directory path not defined; job param 'transferFolderPath' is empty");
                return string.Empty;
            }

            return transferDirectoryPath;

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
        /// Output directory is mWorkDir
        /// </summary>
        /// <param name="gzipFilePath">File to decompress</param>
        /// <returns></returns>
        public bool GUnzipFile(string gzipFilePath)
        {
            return GUnzipFile(gzipFilePath, mWorkDir);
        }

        /// <summary>
        /// Decompresses the specified gzipped file
        /// </summary>
        /// <param name="gzipFilePath">File to unzip</param>
        /// <param name="targetDirectory">Target directory for the extracted files</param>
        /// <returns></returns>
        public bool GUnzipFile(string gzipFilePath, string targetDirectory)
        {
            mDotNetZipTools.DebugLevel = mDebugLevel;

            // Note that mDotNetZipTools logs error messages using LogTools
            return mDotNetZipTools.GUnzipFile(gzipFilePath, targetDirectory);
        }

        /// <summary>
        /// Gzip sourceFilePath, creating a new file in the same directory, but with extension .gz appended to the name (e.g. Dataset.mzid.gz)
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="deleteSourceAfterZip">If True, will delete the file after zipping it</param>
        /// <returns>True if success; false if an error</returns>
        public bool GZipFile(string sourceFilePath, bool deleteSourceAfterZip)
        {
            mDotNetZipTools.DebugLevel = mDebugLevel;

            // Note that mDotNetZipTools logs error messages using LogTools
            var success = mDotNetZipTools.GZipFile(sourceFilePath, deleteSourceAfterZip);

            if (!success && mDotNetZipTools.Message.ToLower().Contains("OutOfMemoryException".ToLower()))
            {
                mNeedToAbortProcessing = true;
            }

            return success;

        }

        /// <summary>
        /// Gzip sourceFilePath, creating a new file in targetDirectoryPath; the file extension will be the original extension plus .gz
        /// </summary>
        /// <param name="sourceFilePath">Full path to the file to be zipped</param>
        /// <param name="targetDirectoryPath">Output directory for the unzipped file</param>
        /// <param name="deleteSourceAfterZip">If True, will delete the file after zipping it</param>
        /// <returns>True if success; false if an error</returns>
        public bool GZipFile(string sourceFilePath, string targetDirectoryPath, bool deleteSourceAfterZip)
        {

            mDotNetZipTools.DebugLevel = mDebugLevel;

            // Note that mDotNetZipTools logs error messages using LogTools
            var success = mDotNetZipTools.GZipFile(sourceFilePath, targetDirectoryPath, deleteSourceAfterZip);

            if (!success && mDotNetZipTools.Message.ToLower().Contains("OutOfMemoryException".ToLower()))
            {
                mNeedToAbortProcessing = true;
            }

            return success;

        }

        /// <summary>
        /// GZip the given file
        /// </summary>
        /// <param name="fileToCompress">File to compress</param>
        /// <returns>FileInfo object of the new .gz file or null if an error</returns>
        /// <remarks>Deletes the original file after creating the .gz file</remarks>
        public FileInfo GZipFile(FileInfo fileToCompress)
        {
            return GZipFile(fileToCompress, true);
        }

        /// <summary>
        /// GZip the given file
        /// </summary>
        /// <param name="fileToCompress">File to compress</param>
        /// <param name="deleteSourceAfterZip">If True, will delete the file after zipping it</param>
        /// <returns>FileInfo object of the new .gz file or null if an error</returns>
        public FileInfo GZipFile(FileInfo fileToCompress, bool deleteSourceAfterZip)
        {

            try
            {
                var success = GZipFile(fileToCompress.FullName, true);

                if (!success)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("GZipFile returned false for " + fileToCompress.Name);
                    }
                    return null;
                }

                var gzippedFile = new FileInfo(fileToCompress.FullName + clsAnalysisResources.DOT_GZ_EXTENSION);
                if (!gzippedFile.Exists)
                {
                    LogError("GZip file was not created: " + gzippedFile.Name);
                    return null;
                }

                return gzippedFile;

            }
            catch (Exception ex)
            {
                LogError("Exception in GZipFile", ex);
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
            var connectionString = mMgrParams.GetParam("BrokerConnectionString");

            var dataPackageID = mJobParams.GetJobParameter("DataPackageID", -1);

            if (dataPackageID < 0)
            {
                dctDataPackageDatasets = new Dictionary<int, clsDataPackageDatasetInfo>();
                return false;
            }

            return clsDataPackageInfoLoader.LoadDataPackageDatasetInfo(connectionString, dataPackageID, out dctDataPackageDatasets);
        }

        /// <summary>
        /// Lookup the Peptide Hit jobs associated with this analysis job; non-peptide hit jobs are returned via additionalJobs
        /// </summary>
        /// <param name="additionalJobs">Output: Non Peptide Hit jobs (e.g. DeconTools or MASIC)</param>
        /// <param name="errorMsg">Output: error message</param>
        /// <returns>Peptide Hit Jobs (e.g. MSGF+ or Sequest)</returns>
        /// <remarks>This method updates property NumberOfClonedSteps for the analysis jobs</remarks>
        protected List<clsDataPackageJobInfo> RetrieveDataPackagePeptideHitJobInfo(
            out List<clsDataPackageJobInfo> additionalJobs,
            out string errorMsg)
        {

            // Gigasax.DMS_Pipeline
            var connectionString = mMgrParams.GetParam("BrokerConnectionString");

            var dataPackageID = mJobParams.GetJobParameter("DataPackageID", -1);

            if (dataPackageID < 0)
            {
                errorMsg = "Job parameter DataPackageID not defined";
                additionalJobs = new List<clsDataPackageJobInfo>();
                return new List<clsDataPackageJobInfo>();
            }

            return clsDataPackageInfoLoader.RetrieveDataPackagePeptideHitJobInfo(
                connectionString, dataPackageID, out additionalJobs, out errorMsg);
        }

        /// <summary>
        /// Loads the job settings file
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        /// <remarks></remarks>
        protected bool LoadSettingsFile()
        {
            var fileName = mJobParams.GetParam("SettingsFileName");
            if (fileName != "na")
            {
                var filePath = Path.Combine(mWorkDir, fileName);

                // XML tool LoadSettings returns True even if file is not found, so a separate check is required
                if (File.Exists(filePath))
                {
                    return mSettingsFileParams.LoadSettings(filePath);
                }

                // Settings file wasn't found
                return false;

            }

            // Settings file wasn't required
            return true;
        }

        /// <summary>
        /// Logs current progress to the log file at a given interval (track progress with mProgress)
        /// </summary>
        /// <param name="toolName"></param>
        /// <remarks>Longer log intervals when mDebugLevel is 0 or 1; shorter intervals for 5</remarks>
        protected void LogProgress(string toolName)
        {
            int logIntervalMinutes;

            if (mDebugLevel >= 5)
            {
                logIntervalMinutes = 1;
            }
            else if (mDebugLevel >= 4)
            {
                logIntervalMinutes = 5;
            }
            else if (mDebugLevel >= 3)
            {
                logIntervalMinutes = 15;
            }
            else if (mDebugLevel >= 2)
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
        /// Logs mProgress to the log file at interval logIntervalMinutes (track progress with mProgress)
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
                var progressMessage = " ... " + mProgress.ToString("0.0") + "% complete for " + toolName + ", job " + Job;

                if (DateTime.UtcNow.Subtract(mLastProgressConsoleTime).TotalMinutes >= CONSOLE_PROGRESS_INTERVAL_MINUTES)
                {
                    mLastProgressConsoleTime = DateTime.UtcNow;
                    ConsoleMsgUtils.ShowDebug(progressMessage);
                }

                if (DateTime.UtcNow.Subtract(mLastProgressWriteTime).TotalMinutes >= logIntervalMinutes)
                {
                    mLastProgressWriteTime = DateTime.UtcNow;
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
        /// Optionally update mEvalMessage
        /// </summary>
        /// <param name="warningMessage">Warning message</param>
        /// <param name="updateEvalMessage">When true, update mEvalMessage</param>
        protected void LogWarning(string warningMessage, bool updateEvalMessage = false)
        {
            if (updateEvalMessage)
            {
                mEvalMessage = clsGlobal.AppendToComment(mEvalMessage, warningMessage);
            }
            base.LogWarning(warningMessage);
        }

        /// <summary>
        /// Creates a results directory after analysis complete
        /// </summary>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks></remarks>
        protected bool MakeResultsDirectory()
        {

            mStatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.PACKAGING_RESULTS, 0);

            // Makes results directory and moves files into it

            // Log status
            LogMessage(mMgrName + ": Creating results directory, Job " + Job);
            var resultsDirectoryNamePath = Path.Combine(mWorkDir, mResultsDirectoryName);

            // Make the results directory
            try
            {
                var resultsDirectory = new DirectoryInfo(resultsDirectoryNamePath);
                if (!resultsDirectory.Exists)
                    resultsDirectory.Create();
            }
            catch (Exception ex)
            {
                // Log this error to the database
                LogError("Error making results directory, job " + Job, ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Makes results directory and moves files into it
        /// </summary>
        /// <returns></returns>
        protected bool MoveResultFiles()
        {

            const int REJECT_LOGGING_THRESHOLD = 10;
            const int ACCEPT_LOGGING_THRESHOLD = 50;
            const int LOG_LEVEL_REPORT_ACCEPT_OR_REJECT = 5;

            var resultsDirectoryNamePath = string.Empty;
            var currentFileName = string.Empty;

            var errorEncountered = false;

            // Move files into results directory
            try
            {
                mStatusTools.UpdateAndWrite(
                    EnumMgrStatus.RUNNING,
                    EnumTaskStatus.RUNNING,
                    EnumTaskStatusDetail.PACKAGING_RESULTS, 0);

                resultsDirectoryNamePath = Path.Combine(mWorkDir, mResultsDirectoryName);
                var dctRejectStats = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                var dctAcceptStats = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                // Log status
                if (mDebugLevel >= 2)
                {
                    var logMessage = "Move Result Files to " + resultsDirectoryNamePath;
                    if (mDebugLevel >= 3)
                    {
                        logMessage += "; ResultFilesToSkip contains " + mJobParams.ResultFilesToSkip.Count + " entries" + "; " +
                            "ResultFileExtensionsToSkip contains " + mJobParams.ResultFileExtensionsToSkip.Count + " entries" + "; " +
                            "ResultFilesToKeep contains " + mJobParams.ResultFilesToKeep.Count + " entries";
                    }
                    LogMessage(logMessage, mDebugLevel);
                }

                // Obtain a list of all files in the working directory
                // Ignore subdirectories
                var files = Directory.GetFiles(mWorkDir, "*");

                // Check each file against mJobParams.m_ResultFileExtensionsToSkip and mJobParams.m_ResultFilesToKeep

                foreach (var tmpFileName in files)
                {
                    var okToMove = true;
                    currentFileName = tmpFileName;

                    // Assure that this is a file name and not a file path
                    var tmpFileNameLCase = Path.GetFileName(tmpFileName).ToLower();

                    // Check to see if the filename is defined in ResultFilesToSkip
                    // Note that entries in ResultFilesToSkip are not case sensitive since they were instantiated using SortedSet<string>(StringComparer.OrdinalIgnoreCase)
                    if (mJobParams.ResultFilesToSkip.Contains(tmpFileNameLCase))
                    {
                        // File found in the ResultFilesToSkip list; do not move it
                        okToMove = false;
                    }

                    if (okToMove)
                    {
                        // Check to see if the file ends with an entry specified in ResultFileExtensionsToSkip
                        // Note that entries in ResultFileExtensionsToSkip can be extensions, or can even be partial file names, e.g. _peaks.txt
                        foreach (var ext in mJobParams.ResultFileExtensionsToSkip)
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
                        if (mJobParams.ResultFilesToKeep.Contains(tmpFileNameLCase))
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
                        foreach (var character in Path.GetFileName(tmpFileName))
                        {
                            var asciiValue = (int)character;
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
                        if (mDebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT)
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

                    // If valid file name, move file to results directory
                    if (mDebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT)
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
                        targetFilePath = Path.Combine(resultsDirectoryNamePath, Path.GetFileName(tmpFileName));
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
                                // The original file (in the work directory) will get deleted when the work directory is "cleaned" after the job finishes
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

                if (mDebugLevel >= LOG_LEVEL_REPORT_ACCEPT_OR_REJECT)
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
                if (mDebugLevel > 0)
                {
                    LogMessage("clsAnalysisToolRunnerBase.MoveResultFiles(); Error moving files to results directory", 0, true);
                    LogMessage("CurrentFile = " + currentFileName);
                    LogMessage("Results directory name = " + resultsDirectoryNamePath);
                }

                LogErrorToDatabase("Error moving results files, job " + Job + ex.Message);
                UpdateStatusMessage("Error moving results files");

                errorEncountered = true;
            }

            try
            {
                // Make the summary file
                OutputSummary(resultsDirectoryNamePath);
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            if (errorEncountered)
            {
                // Try to save whatever files were moved into the results directory
                var analysisResults = new clsAnalysisResults(mMgrParams, mJobParams);
                analysisResults.CopyFailedResultsToArchiveDirectory(Path.Combine(mWorkDir, mResultsDirectoryName));

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
            if (mDebugLevel < 4)
            {
                // Do not create the AnalysisSummary file
                return;
            }

            // Saves the summary file in the results directory
            var assemblyTools = new clsAssemblyTools();

            assemblyTools.GetComponentFileVersionInfo(mSummaryFile);

            var summaryFileName = mJobParams.GetParam("StepTool") + "_AnalysisSummary.txt";

            if (!mJobParams.ResultFilesToSkip.Contains(summaryFileName))
            {
                mSummaryFile.SaveSummaryFile(Path.Combine(OutputPath, summaryFileName));
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

            var toolVersionInfoFile = new FileInfo(Path.Combine(mWorkDir, mToolVersionUtilities.ToolVersionInfoFile));
            if (!toolVersionInfoFile.Exists)
            {
                LogErrorNoMessageUpdate(
                    "ToolVersionInfo file not found for job " + mJob +
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
                        mToolVersionUtilities.StoreToolVersionInDatabase(toolVersionInfo);
                    }
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Purge old server cache files
        /// </summary>
        /// <param name="cacheDirectoryPath"></param>
        public void PurgeOldServerCacheFiles(string cacheDirectoryPath)
        {
            // Value prior to December 2014: 3 TB
            // Value effective December 2014: 20 TB
            const int spaceUsageThresholdGB = 20000;
            PurgeOldServerCacheFiles(cacheDirectoryPath, spaceUsageThresholdGB);
        }

        /// <summary>
        /// Test method for PurgeOldServerCacheFiles
        /// </summary>
        /// <param name="cacheDirectoryPath"></param>
        /// <param name="spaceUsageThresholdGB"></param>
        public void PurgeOldServerCacheFilesTest(string cacheDirectoryPath, int spaceUsageThresholdGB)
        {
            if (cacheDirectoryPath.StartsWith(@"\\proto", StringComparison.OrdinalIgnoreCase))
            {
                if (!string.Equals(cacheDirectoryPath, @"\\proto-2\past\PurgeTest", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine(@"This function cannot be used with a \\Proto-x\ server");
                    return;
                }
            }
            PurgeOldServerCacheFiles(cacheDirectoryPath, spaceUsageThresholdGB);
        }

        /// <summary>
        /// Determines the space usage of data files in the cache directory, e.g. at \\proto-11\MSXML_Cache
        /// If usage is over spaceUsageThresholdGB, deletes the oldest files until usage falls below spaceUsageThresholdGB
        /// </summary>
        /// <param name="cacheDirectoryPath">Path to the file cache</param>
        /// <param name="spaceUsageThresholdGB">Maximum space usage, in GB (cannot be less than 1000 on Proto-x servers; 10 otherwise)</param>
        private void PurgeOldServerCacheFiles(string cacheDirectoryPath, int spaceUsageThresholdGB)
        {

            {

                var dataFiles = new List<KeyValuePair<DateTime, FileInfo>>();

                double totalSizeMB = 0;

                double sizeDeletedMB = 0;
                var fileDeleteCount = 0;
                var fileDeleteErrorCount = 0;

                // Keys are exception names; values are number of times the exception was seen
                var errorSummary = new Dictionary<string, int>();

                if (string.IsNullOrWhiteSpace(cacheDirectoryPath))
                {
                    throw new ArgumentOutOfRangeException(nameof(cacheDirectoryPath), "Cache directory path cannot be empty");
                }

                if (cacheDirectoryPath.StartsWith(@"\\proto-", StringComparison.OrdinalIgnoreCase))
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
                    if (DateTime.UtcNow.Subtract(mLastCachedServerFilesPurgeCheck).TotalHours < CACHED_SERVER_FILES_PURGE_INTERVAL_HOURS)
                    {
                        return;
                    }

                    var cacheDirectory = new DirectoryInfo(cacheDirectoryPath);

                    if (!cacheDirectory.Exists)
                    {
                        return;
                    }

                    // Look for a purge check file
                    var purgeCheckFile = new FileInfo(Path.Combine(cacheDirectory.FullName, "PurgeCheckFile.txt"));
                    if (purgeCheckFile.Exists)
                    {
                        if (DateTime.UtcNow.Subtract(purgeCheckFile.LastWriteTimeUtc).TotalHours < CACHED_SERVER_FILES_PURGE_INTERVAL_HOURS)
                        {
                            return;
                        }
                    }

                    // Create / update the purge check file
                    try
                    {
                        using (var writer = new StreamWriter(new FileStream(purgeCheckFile.FullName, FileMode.Append, FileAccess.Write, FileShare.Read)))
                        {
                            writer.WriteLine(DateTime.Now.ToString(DATE_TIME_FORMAT) + " - " + mMgrName);
                        }

                    }
                    catch (Exception)
                    {
                        // Likely another manager tried to update the file at the same time
                        // Ignore the error and proceed to look for files to purge
                    }

                    mLastCachedServerFilesPurgeCheck = DateTime.UtcNow;

                    var lastProgress = DateTime.UtcNow;
                    LogMessage("Examining hashcheck files in directory " + cacheDirectory.FullName, 1);

                    // Make a list of all of the hashcheck files in cacheDirectory

                    foreach (var hashcheckFile in cacheDirectory.GetFiles("*" + clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX, SearchOption.AllDirectories))
                    {
                        if (!hashcheckFile.FullName.EndsWith(clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX, StringComparison.OrdinalIgnoreCase))
                            continue;

                        var dataFilePath = hashcheckFile.FullName.Substring(0, hashcheckFile.FullName.Length - clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX.Length);

                        var dataFile = new FileInfo(dataFilePath);

                        if (!dataFile.Exists)
                            continue;

                        try
                        {
                            dataFiles.Add(new KeyValuePair<DateTime, FileInfo>(dataFile.LastWriteTimeUtc, dataFile));

                            totalSizeMB += clsGlobal.BytesToMB(dataFile.Length);
                        }
                        catch (Exception ex)
                        {
                            LogMessage("Exception adding to file list " + dataFile.Name + "; " + ex.Message, 0, true);
                        }

                        if (DateTime.UtcNow.Subtract(lastProgress).TotalSeconds >= 5)
                        {
                            lastProgress = DateTime.UtcNow;
                            LogMessage(string.Format(" ... {0:#,##0} files processed", dataFiles.Count));
                        }
                    }

                    if (totalSizeMB / 1024.0 <= spaceUsageThresholdGB)
                    {
                        return;
                    }

                    // Purge files until the space usage falls below the threshold
                    // Start with the earliest file then work our way forward

                    // Keep track of the deleted file info using this list
                    var purgedFileLogEntries = new List<string>();

                    var purgeLogFile = new FileInfo(Path.Combine(cacheDirectory.FullName, "PurgeLog_" + DateTime.Now.Year + ".txt"));
                    if (!purgeLogFile.Exists)
                    {
                        // Create the purge log file and write the header line
                        try
                        {
                            using (var writer = new StreamWriter(new FileStream(purgeLogFile.FullName, FileMode.Append, FileAccess.Write, FileShare.Read)))
                            {
                                writer.WriteLine(string.Join("\t", "Date", "Manager", "Size (MB)", "Modification_Date", "Path"));
                            }
                        }
                        catch (Exception)
                        {
                            // Likely another manager tried to create the file at the same time
                            // Ignore the error
                        }
                    }

                    var sortedDataFiles = (from item in dataFiles orderby item.Key select item);

                    foreach (var dataFileItem in sortedDataFiles)
                    {
                        try
                        {
                            var dataFile = dataFileItem.Value;

                            var fileSizeMB = clsGlobal.BytesToMB(dataFile.Length);

                            var hashcheckPath = dataFile.FullName + clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX;
                            var hashCheckFile = new FileInfo(hashcheckPath);

                            totalSizeMB -= fileSizeMB;

                            dataFile.Delete();

                            // Keep track of the deleted file's details
                            purgedFileLogEntries.Add(string.Join("\t",
                                DateTime.Now.ToString(DATE_TIME_FORMAT),
                                mMgrName,
                                fileSizeMB.ToString("0.00"),
                                dataFile.LastWriteTime.ToString(DATE_TIME_FORMAT),
                                dataFile.FullName));

                            sizeDeletedMB += fileSizeMB;
                            fileDeleteCount += 1;

                            if (hashCheckFile.Exists)
                            {
                                hashCheckFile.Delete();
                            }

                        }
                        catch (Exception ex)
                        {
                            // Keep track of the number of times we have an exception
                            fileDeleteErrorCount += 1;

                            var exceptionName = ex.GetType().ToString();
                            if (errorSummary.TryGetValue(exceptionName, out var occurrences))
                            {
                                errorSummary[exceptionName] = occurrences + 1;
                            }
                            else
                            {
                                errorSummary.Add(exceptionName, 1);
                            }

                        }

                        if (totalSizeMB / 1024.0 < spaceUsageThresholdGB * 0.95)
                        {
                            break;
                        }
                    }

                    LogMessage("Deleted " + fileDeleteCount + " file(s) from " + cacheDirectoryPath + ", recovering " + sizeDeletedMB.ToString("0.0") + " MB in disk space");

                    if (fileDeleteErrorCount > 0)
                    {
                        LogMessage("Unable to delete " + fileDeleteErrorCount + " file(s) from " + cacheDirectoryPath, 0, true);
                        foreach (var kvItem in errorSummary)
                        {
                            var exceptionName = kvItem.Key;
                            var occurrenceCount = kvItem.Value;
                            LogMessage("  " + exceptionName + ": " + occurrenceCount, 1, true);
                        }
                    }

                    if (purgedFileLogEntries.Count > 0)
                    {
                        // Log the info for each of the deleted files
                        try
                        {
                            using (var writer = new StreamWriter(new FileStream(purgeLogFile.FullName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)))
                            {
                                foreach (var purgedFileLogEntry in purgedFileLogEntries)
                                {
                                    writer.WriteLine(purgedFileLogEntry);
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
        /// Updates the dataset name to the final directory name in the transferFolderPath job parameter
        /// Updates the transfer directory path to remove the final directory name
        /// </summary>
        /// <remarks></remarks>
        protected void RedefineAggregationJobDatasetAndTransferFolder()
        {
            var transferDirectoryPath = mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH);
            var transferDirectory = new DirectoryInfo(transferDirectoryPath);

            mDatasetName = transferDirectory.Name;

            if (transferDirectory.Parent == null)
            {
                throw new DirectoryNotFoundException("Unable to determine the parent directory of " + transferDirectory.FullName);
            }

            transferDirectoryPath = transferDirectory.Parent.FullName;
            mJobParams.SetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH, transferDirectoryPath);

        }

        /// <summary>
        /// Deletes files in specified directory that have been previously flagged as not wanted in results directory
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>List of files to delete is tracked via mJobParams.ServerFilesToDelete; must store full file paths in ServerFilesToDelete</remarks>
        public bool RemoveNonResultServerFiles()
        {

            var currentFile = "??";

            try
            {
                // Log status
                LogMessage("Remove Files from the storage server; " +
                    "ServerFilesToDelete contains " + mJobParams.ServerFilesToDelete.Count + " entries", 2);

                foreach (var fileToDelete in mJobParams.ServerFilesToDelete)
                {
                    currentFile = fileToDelete;

                    // Log file to be deleted
                    LogMessage("Deleting " + fileToDelete, 4);

                    if (File.Exists(fileToDelete))
                    {
                        // Assure that the file is not set to readonly or system, then delete it
                        File.SetAttributes(fileToDelete, File.GetAttributes(fileToDelete) & ~FileAttributes.ReadOnly & ~FileAttributes.System);
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
                if (mDebugLevel >= 1)
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
            var logFileName = mMgrParams.GetParam("LogFileName");
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
            throw new NotImplementedException("Plugin " + StepToolName + " must implement RetrieveRemoteResults to allow for remote processing; that method must call RetrieveRemoteResultFiles");
        }

        /// <summary>
        /// Retrieve the specified files, verifying that each one was actually retrieved if verifyCopied is true
        /// </summary>
        /// <param name="transferUtility">Remote transfer utility</param>
        /// <param name="filesToRetrieve">Dictionary where keys are source file names (no wildcards), and values are true if the file is required, false if optional</param>
        /// <param name="verifyCopied">Log warnings if any files are missing.  When false, logs debug messages instead</param>
        /// <param name="retrievedFilePaths">Local paths of retrieved files</param>
        /// <returns>True on success, otherwise false</returns>
        protected bool RetrieveRemoteResultFiles(
            clsRemoteTransferUtility transferUtility,
            Dictionary<string, bool> filesToRetrieve,
            bool verifyCopied,
            out List<string> retrievedFilePaths)
        {
            retrievedFilePaths = new List<string>();

            try
            {

                var remoteSourceDirectory = transferUtility.RemoteJobStepWorkDirPath;
                var warnIfMissing = verifyCopied;

                transferUtility.CopyFilesFromRemote(remoteSourceDirectory, filesToRetrieve, mWorkDir, false, warnIfMissing);

                var requiredFileCount = 0;

                // Verify that all files were retrieved
                foreach (var sourceFile in filesToRetrieve)
                {
                    var fileRequired = sourceFile.Value;
                    if (fileRequired)
                        requiredFileCount++;

                    var localFile = new FileInfo(Path.Combine(mWorkDir, sourceFile.Key));
                    if (localFile.Exists)
                    {
                        retrievedFilePaths.Add(localFile.FullName);
                        continue;
                    }

                    if (verifyCopied && fileRequired)
                        LogError("Required result file not found: " + sourceFile.Key);
                }

                var paramFileName = mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_PARAMETER_FILE);
                var modDefsFile = new FileInfo(Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(paramFileName) + "_ModDefs.txt"));
                if (modDefsFile.Exists && modDefsFile.Length == 0)
                {
                    mJobParams.AddResultFileToSkip(modDefsFile.Name);
                }

                if (retrievedFilePaths.Count >= requiredFileCount || !verifyCopied)
                    return true;

                if (string.IsNullOrWhiteSpace(mMessage))
                    LogError("Expected result files not found on " + transferUtility.RemoteHostName);

                return false;

            }
            catch (Exception ex)
            {
                LogError("Error in RetrieveRemoteResultFiles", ex);
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
            LogMessage(mMgrName + ": Starting analysis, job " + Job);

            // Start the job timer
            mStartTime = DateTime.UtcNow;

            // Remainder of method is supplied by subclasses

            return CloseOutType.CLOSEOUT_SUCCESS;

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
                    WorkingDirectoryPath = mWorkDir,
                    HasHeaderLine = hasHeaderLine,
                    ColumnDelimiter = "\t",
                    MaxFileSizeMBForInMemorySort = FlexibleFileSortUtility.TextFileSorter.DEFAULT_IN_MEMORY_SORT_MAX_FILE_SIZE_MB,
                    ChunkSizeMB = FlexibleFileSortUtility.TextFileSorter.DEFAULT_CHUNK_SIZE_MB
                };

                sortUtility.ProgressUpdate += SortUtility_ProgressChanged;
                sortUtility.ErrorEvent += SortUtility_ErrorEvent;
                sortUtility.WarningEvent += SortUtility_WarningEvent;
                sortUtility.StatusEvent += SortUtility_MessageEvent;

                var success = sortUtility.SortFile(textFilePath, sortedFilePath);

                if (success)
                    return true;

                if (string.IsNullOrWhiteSpace(mSortUtilityErrorMessage))
                {
                    mMessage = "Unknown error sorting " + Path.GetFileName(textFilePath);
                }
                else
                {
                    mMessage = mSortUtilityErrorMessage;
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
        /// Communicates with database to record the tool version(s) for the current step task
        /// </summary>
        /// <param name="toolVersionInfo">Version info (maximum length is 900 characters)</param>
        /// <param name="toolFiles">FileSystemInfo list of program files related to the step tool</param>
        /// <param name="saveToolVersionTextFile">If true, creates a text file with the tool version information</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>This procedure should be called once the version (or versions) of the tools associated with the current step have been determined</remarks>
        public bool SetStepTaskToolVersion(string toolVersionInfo, IEnumerable<FileInfo> toolFiles, bool saveToolVersionTextFile = true)
        {
            return mToolVersionUtilities.SetStepTaskToolVersion(toolVersionInfo, toolFiles, saveToolVersionTextFile);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <param name="progLoc">Path to the primary .exe or .DLL</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>This method is appropriate for plugins that call a .NET executable</remarks>
        protected bool StoreDotNETToolVersionInfo(string progLoc)
        {
            return mToolVersionUtilities.StoreDotNETToolVersionInfo(progLoc);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <param name="progLoc">Path to the primary .exe or .DLL</param>
        /// <param name="additionalDLLs">Additional .NET DLLs to examine (either simply names or full paths)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>This method is appropriate for plugins that call a .NET executable</remarks>
        protected bool StoreDotNETToolVersionInfo(string progLoc, IReadOnlyCollection<string> additionalDLLs)
        {
            return mToolVersionUtilities.StoreDotNETToolVersionInfo(progLoc, additionalDLLs);
        }

        /// <summary>
        /// Uses Reflection to determine the version info for an assembly already loaded in memory
        /// </summary>
        /// <param name="toolVersionInfo">Version info string to append the version info to</param>
        /// <param name="assemblyName">Assembly Name</param>
        /// <param name="includeRevision">Set to True to include a version of the form 1.5.4821.24755; set to omit the revision, giving a version of the form 1.5.4821</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>Use StoreToolVersionInfoOneFile for DLLs not loaded in memory</remarks>
        protected bool StoreToolVersionInfoForLoadedAssembly(ref string toolVersionInfo, string assemblyName, bool includeRevision = true)
        {
            return mToolVersionUtilities.StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, assemblyName, includeRevision);
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
            return mToolVersionUtilities.StoreToolVersionInfoOneFile(ref toolVersionInfo, dllFilePath);
        }

        /// <summary>
        /// Copies new/changed files from the source directory to the target directory
        /// </summary>
        /// <param name="sourceDirectoryPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool SynchronizeFolders(string sourceDirectoryPath, string targetDirectoryPath)
        {
            return SynchronizeFolders(sourceDirectoryPath, targetDirectoryPath, "*");
        }

        /// <summary>
        /// Copies new/changed files from the source directory to the target directory
        /// </summary>
        /// <param name="sourceDirectoryPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="copySubdirectories">If true, recursively copies subdirectories</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool SynchronizeFolders(string sourceDirectoryPath, string targetDirectoryPath, bool copySubdirectories)
        {

            var fileNameFilterSpecs = new List<string> { "*" };
            var fileNameExclusionSpecs = new List<string>();
            const int maxRetryCount = 3;

            return SynchronizeFolders(sourceDirectoryPath, targetDirectoryPath, fileNameFilterSpecs, fileNameExclusionSpecs, maxRetryCount, copySubdirectories);
        }

        /// <summary>
        /// Copies new/changed files from the source directory to the target directory
        /// </summary>
        /// <param name="sourceDirectoryPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="fileNameFilterSpec">Filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Will retry failed copies up to 3 times</remarks>
        protected bool SynchronizeFolders(string sourceDirectoryPath, string targetDirectoryPath, string fileNameFilterSpec)
        {

            var fileNameFilterSpecs = new List<string> { fileNameFilterSpec };
            var fileNameExclusionSpecs = new List<string>();
            const int maxRetryCount = 3;
            const bool copySubdirectories = false;

            return SynchronizeFolders(sourceDirectoryPath, targetDirectoryPath, fileNameFilterSpecs, fileNameExclusionSpecs, maxRetryCount, copySubdirectories);
        }

        /// <summary>
        /// Copies new/changed files from the source directory to the target directory
        /// </summary>
        /// <param name="sourceDirectoryPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="fileNameFilterSpecs">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Will retry failed copies up to 3 times</remarks>
        protected bool SynchronizeFolders(string sourceDirectoryPath, string targetDirectoryPath, List<string> fileNameFilterSpecs)
        {

            var fileNameExclusionSpecs = new List<string>();
            const int maxRetryCount = 3;
            const bool copySubdirectories = false;

            return SynchronizeFolders(sourceDirectoryPath, targetDirectoryPath, fileNameFilterSpecs, fileNameExclusionSpecs, maxRetryCount, copySubdirectories);
        }

        /// <summary>
        /// Copies new/changed files from the source directory to the target directory
        /// </summary>
        /// <param name="sourceDirectoryPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="fileNameFilterSpecs">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
        /// <param name="fileNameExclusionSpecs">One or more filename filters for excluding files; can use * as a wildcard</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Will retry failed copies up to 3 times</remarks>
        protected bool SynchronizeFolders(string sourceDirectoryPath, string targetDirectoryPath, List<string> fileNameFilterSpecs, List<string> fileNameExclusionSpecs)
        {

            const int maxRetryCount = 3;
            const bool copySubdirectories = false;

            return SynchronizeFolders(sourceDirectoryPath, targetDirectoryPath, fileNameFilterSpecs, fileNameExclusionSpecs, maxRetryCount, copySubdirectories);
        }

        /// <summary>
        /// Copies new/changed files from the source directory to the target directory
        /// </summary>
        /// <param name="sourceDirectoryPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="fileNameFilterSpecs">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
        /// <param name="fileNameExclusionSpecs">One or more filename filters for excluding files; can use * as a wildcard</param>
        /// <param name="maxRetryCount">Will retry failed copies up to maxRetryCount times; use 0 for no retries</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool SynchronizeFolders(string sourceDirectoryPath, string targetDirectoryPath, List<string> fileNameFilterSpecs, List<string> fileNameExclusionSpecs, int maxRetryCount)
        {

            const bool copySubdirectories = false;
            return SynchronizeFolders(sourceDirectoryPath, targetDirectoryPath, fileNameFilterSpecs, fileNameExclusionSpecs, maxRetryCount, copySubdirectories);

        }

        /// <summary>
        /// Copies new/changed files from the source directory to the target directory
        /// </summary>
        /// <param name="sourceDirectoryPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="fileNameFilterSpecs">One or more filename filters for including files; can use * as a wildcard; when blank then processes all files</param>
        /// <param name="fileNameExclusionSpecs">One or more filename filters for excluding files; can use * as a wildcard</param>
        /// <param name="maxRetryCount">Will retry failed copies up to maxRetryCount times; use 0 for no retries</param>
        /// <param name="copySubdirectories">If true, recursively copies subdirectories</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool SynchronizeFolders(
            string sourceDirectoryPath,
            string targetDirectoryPath,
            List<string> fileNameFilterSpecs,
            List<string> fileNameExclusionSpecs,
            int maxRetryCount,
            bool copySubdirectories)
        {
            try
            {
                var sourceDirectory = new DirectoryInfo(sourceDirectoryPath);
                var targetDirectory = new DirectoryInfo(targetDirectoryPath);

                if (!targetDirectory.Exists)
                {
                    targetDirectory.Create();
                }

                if (fileNameFilterSpecs == null)
                {
                    fileNameFilterSpecs = new List<string>();
                }

                if (fileNameFilterSpecs.Count == 0)
                    fileNameFilterSpecs.Add("*");

                var filesToCopy = new SortedSet<string>();

                foreach (var filterSpec in fileNameFilterSpecs)
                {
                    var filterSpecToUse = string.IsNullOrWhiteSpace(filterSpec) ? "*" : filterSpec;

                    foreach (var sourceFile in sourceDirectory.GetFiles(filterSpecToUse))
                    {
                        if (!filesToCopy.Contains(sourceFile.Name))
                        {
                            filesToCopy.Add(sourceFile.Name);
                        }
                    }
                }

                if ((fileNameExclusionSpecs != null) && fileNameExclusionSpecs.Count > 0)
                {
                    // Remove any files from filesToCopy that would get matched by items in fileNameExclusionSpecs

                    foreach (var filterSpec in fileNameExclusionSpecs)
                    {
                        if (string.IsNullOrWhiteSpace(filterSpec))
                            continue;

                        foreach (var sourceFile in sourceDirectory.GetFiles(filterSpec))
                        {
                            if (filesToCopy.Contains(sourceFile.Name))
                            {
                                filesToCopy.Remove(sourceFile.Name);
                            }
                        }
                    }
                }

                foreach (var sourceFileName in filesToCopy)
                {
                    var sourceFile = new FileInfo(Path.Combine(sourceDirectory.FullName, sourceFileName));
                    var targetFile = new FileInfo(Path.Combine(targetDirectory.FullName, sourceFileName));
                    var copyFile = false;

                    if (!targetFile.Exists)
                    {
                        copyFile = true;
                    }
                    else if (targetFile.Length != sourceFile.Length)
                    {
                        copyFile = true;
                    }
                    else if (targetFile.LastWriteTimeUtc < sourceFile.LastWriteTimeUtc)
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

                            success = mFileTools.CopyFileUsingLocks(sourceFile, targetFile.FullName, true);
                            if (success)
                            {
                                LogCopyStats(startTime, targetFile.FullName);
                            }
                            else
                            {
                                retriesRemaining -= 1;
                                if (retriesRemaining < 0)
                                {
                                    mMessage = "Error copying " + sourceFile.FullName + " to " + targetFile.DirectoryName;
                                    return false;
                                }

                                LogMessage("Error copying " + sourceFile.FullName + " to " + targetFile.DirectoryName + "; RetriesRemaining: " + retriesRemaining, 0, true);

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
                            fileNameFilterSpecs, fileNameExclusionSpecs, maxRetryCount, copySubdirectories: true);

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
                mSummaryFile.Add(Environment.NewLine);
                mSummaryFile.Add("=====================================================================================");
                mSummaryFile.Add(Environment.NewLine);

                // The ToolName job parameter holds the name of the job script we are executing
                var scriptName = mJobParams.GetParam("ToolName");
                var stepTool = mJobParams.GetParam("StepTool");

                // Construct the Tool description (combination of the script name and Step Tool name)
                var toolAndStepTool = clsAnalysisJob.GetJobToolDescription(scriptName, stepTool, string.Empty);

                // Add the data
                mSummaryFile.Add("Job Number" + '\t' + Job);
                mSummaryFile.Add("Job Step" + '\t' + mJobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step"));
                mSummaryFile.Add("Date" + '\t' + DateTime.Now);
                mSummaryFile.Add("Processor" + '\t' + mMgrName);
                mSummaryFile.Add("Tool" + '\t' + toolAndStepTool);
                mSummaryFile.Add("Dataset Name" + '\t' + Dataset);
                mSummaryFile.Add("Xfer Folder" + '\t' + mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH));
                mSummaryFile.Add("Param File Name" + '\t' + mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_PARAMETER_FILE));
                mSummaryFile.Add("Settings File Name" + '\t' + mJobParams.GetParam("SettingsFileName"));
                mSummaryFile.Add("Legacy Organism Db Name" + '\t' + mJobParams.GetParam("LegacyFastaFileName"));
                mSummaryFile.Add("Protein Collection List" + '\t' + mJobParams.GetParam("ProteinCollectionList"));
                mSummaryFile.Add("Protein Options List" + '\t' + mJobParams.GetParam("ProteinOptions"));
                mSummaryFile.Add("Fasta File Name" + '\t' + mJobParams.GetParam("PeptideSearch", clsAnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

                if (mStopTime < mStartTime)
                    mStopTime = DateTime.UtcNow;

                mSummaryFile.Add("Analysis Time (hh:mm:ss)" + '\t' + CalcElapsedTime(mStartTime, mStopTime));

                // Add another separator
                mSummaryFile.Add(Environment.NewLine);
                mSummaryFile.Add("=====================================================================================");
                mSummaryFile.Add(Environment.NewLine);

            }
            catch (Exception ex)
            {
                LogError("Error updating the summary file",
                         "Error updating the summary file, " + mJobParams.GetJobStepDescription(), ex);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Unzips all files in the specified Zip file
        /// Output directory is mWorkDir
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool UnzipFile(string zipFilePath)
        {
            return UnzipFile(zipFilePath, mWorkDir, string.Empty);
        }

        /// <summary>
        /// Unzips all files in the specified Zip file
        /// Output directory is targetDirectory
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
        /// Output directory is targetDirectory
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="targetDirectory">Target directory for the extracted files</param>
        /// <param name="FileFilter">FilterSpec to apply, for example *.txt</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool UnzipFile(string zipFilePath, string targetDirectory, string FileFilter)
        {
            mDotNetZipTools.DebugLevel = mDebugLevel;

            // Note that mDotNetZipTools logs error messages using LogTools
            return mDotNetZipTools.UnzipFile(zipFilePath, targetDirectory, FileFilter);

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
            mEvalCode = evalCode;
            mEvalMessage = evalMsg;
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

            mStatusTools.ProgRunnerProcessID = processID;

            mStatusTools.StoreCoreUsageHistory(mCoreUsageHistory);

            // If the Program has been running for at least 3 minutes, store the actual CoreUsage in the database
            if (DateTime.UtcNow.Subtract(mProgRunnerStartTime).TotalMinutes < 3)
                return;

            // Average the data in the history queue
            var coreUsageAvg = (from item in mCoreUsageHistory.ToArray() select item.Value).Average();

            mStatusTools.ProgRunnerCoreUsage = coreUsageAvg;
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
        /// Update Status.xml every 15 seconds using mProgress
        /// </summary>
        /// <remarks></remarks>
        protected void UpdateStatusFile()
        {
            UpdateStatusFile(mProgress);
        }

        /// <summary>
        /// Update Status.xml every 15 seconds using percentComplete
        /// </summary>
        /// <param name="percentComplete">Percent complete</param>
        /// <remarks></remarks>
        protected void UpdateStatusFile(float percentComplete)
        {
            var frequencySeconds = 15;
            UpdateStatusFile(percentComplete, frequencySeconds);
        }

        /// <summary>
        /// Update Status.xml every frequencySeconds seconds using percentComplete
        /// </summary>
        /// <param name="percentComplete">Percent complete</param>
        /// <param name="frequencySeconds">Minimum time between updates, in seconds (must be at least 5)</param>
        /// <remarks></remarks>
        protected void UpdateStatusFile(float percentComplete, int frequencySeconds)
        {
            if (frequencySeconds < 5)
                frequencySeconds = 5;

            // Update the status file (limit the updates to every x seconds)
            if (DateTime.UtcNow.Subtract(mLastStatusFileUpdate).TotalSeconds >= frequencySeconds)
            {
                mLastStatusFileUpdate = DateTime.UtcNow;
                UpdateStatusRunning(percentComplete);
            }

        }

        /// <summary>
        /// Update mMessage, which is logged in the pipeline job steps table when the job step finishes
        /// </summary>
        /// <param name="statusMessage">New status message</param>
        /// <param name="appendToExisting">True to append to mMessage; false to overwrite it</param>
        /// <remarks>Text in mMessage will be stored in the Completion_Message column in the database</remarks>
        protected void UpdateStatusMessage(string statusMessage, bool appendToExisting = false)
        {
            if (appendToExisting)
            {
                mMessage = clsGlobal.AppendToComment(mMessage, statusMessage);
            }
            else
            {
                mMessage = statusMessage;
            }

            LogDebug(mMessage);
        }

        /// <summary>
        /// Update Status.xml now using mProgress
        /// </summary>
        /// <remarks></remarks>
        protected void UpdateStatusRunning()
        {
            UpdateStatusRunning(mProgress);
        }

        /// <summary>
        /// Update Status.xml now using percentComplete
        /// </summary>
        /// <param name="percentComplete"></param>
        /// <remarks></remarks>
        protected void UpdateStatusRunning(float percentComplete)
        {
            mProgress = percentComplete;
            mStatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.RUNNING_TOOL, percentComplete, 0, "", "", "", false);
        }

        /// <summary>
        /// Update Status.xml now using percentComplete and spectrumCountTotal
        /// </summary>
        /// <param name="percentComplete"></param>
        /// <param name="spectrumCountTotal"></param>
        /// <remarks></remarks>
        protected void UpdateStatusRunning(float percentComplete, int spectrumCountTotal)
        {
            mProgress = percentComplete;
            mStatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.RUNNING_TOOL, percentComplete, spectrumCountTotal, "", "", "", false);
        }

        /// <summary>
        /// Make sure the _DTA.txt file exists and has at least one spectrum in it
        /// </summary>
        /// <returns>True if success; false if failure</returns>
        /// <remarks></remarks>
        protected bool ValidateCDTAFile()
        {
            var dtaFilePath = Path.Combine(mWorkDir, Dataset + clsAnalysisResources.CDTA_EXTENSION);

            return ValidateCDTAFile(dtaFilePath);
        }

        /// <summary>
        /// Validate that a _dta.txt file is not empty
        /// </summary>
        /// <param name="dtaFilePath"></param>
        /// <returns></returns>
        protected bool ValidateCDTAFile(string dtaFilePath)
        {
            var dataFound = false;

            try
            {
                if (!File.Exists(dtaFilePath))
                {
                    LogError("_DTA.txt file not found", dtaFilePath);
                    return false;
                }

                using (var reader = new StreamReader(new FileStream(dtaFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (!string.IsNullOrWhiteSpace(dataLine))
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
            mDotNetZipTools.DebugLevel = mDebugLevel;

            // Note that mDotNetZipTools logs error messages using LogTools
            var success = mDotNetZipTools.VerifyZipFile(zipFilePath, crcCheckThresholdGB);

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
            mDotNetZipTools.DebugLevel = mDebugLevel;

            // Note that mDotNetZipTools logs error messages using LogTools
            var success = mDotNetZipTools.ZipFile(sourceFilePath, deleteSourceAfterZip);

            if (!success && mDotNetZipTools.Message.ToLower().Contains("OutOfMemoryException".ToLower()))
            {
                mNeedToAbortProcessing = true;
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
                var sourceFile = new FileInfo(sourceFilePath);

                var zipFilePath = GetZipFilePathForFile(sourceFilePath);

                try
                {

                    if (File.Exists(zipFilePath))
                    {
                        if (mDebugLevel >= 3)
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
                zipper.CreateZip(zipFilePath, sourceFile.DirectoryName, false, sourceFile.Name);

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
            mDotNetZipTools.DebugLevel = mDebugLevel;

            // Note that mDotNetZipTools logs error messages using LogTools
            var success = mDotNetZipTools.ZipFile(sourceFilePath, deleteSourceAfterZip, zipFilePath);

            if (!success && mDotNetZipTools.Message.ToLower().Contains("OutOfMemoryException".ToLower()))
            {
                mNeedToAbortProcessing = true;
            }

            return success;

        }

        /// <summary>
        /// Zip a file
        /// </summary>
        /// <param name="fileToCompress"></param>
        /// <param name="fileDescription"></param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>The original file is not deleted, but the name is added to ResultFilesToSkip in mJobParams</remarks>
        protected bool ZipOutputFile(FileInfo fileToCompress, string fileDescription)
        {

            try
            {
                if (string.IsNullOrWhiteSpace(fileDescription))
                    fileDescription = "Unknown_Source";

                if (!ZipFile(fileToCompress.FullName, false))
                {
                    LogError("Error zipping " + fileDescription + " results file");
                    return false;
                }

                // Add the unzipped file to .ResultFilesToSkip since we only want to keep the zipped version
                mJobParams.AddResultFileToSkip(fileToCompress.Name);

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

        private void SortUtility_ErrorEvent(string message, Exception ex)
        {
            mSortUtilityErrorMessage = message;
            LogError("SortUtility: " + message, ex);
        }

        private void SortUtility_MessageEvent(string message)
        {
            if (mDebugLevel >= 1)
            {
                LogMessage(message);
            }
        }

        private void SortUtility_ProgressChanged(string progressMessage, float percentComplete)
        {
            if (mDebugLevel >= 1 && DateTime.UtcNow.Subtract(mLastSortUtilityProgress).TotalSeconds >= 5)
            {
                mLastSortUtilityProgress = DateTime.UtcNow;
                LogMessage(progressMessage + ": " + percentComplete.ToString("0.0") + "% complete");
            }
        }

        private void SortUtility_WarningEvent(string message)
        {
            LogWarning("SortUtility: " + Message);
        }

        #endregion

    }

}