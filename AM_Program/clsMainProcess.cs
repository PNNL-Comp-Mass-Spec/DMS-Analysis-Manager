//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/19/2007
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using AnalysisManagerBase;
using PRISM.Logging;
using PRISM;

namespace AnalysisManagerProg
{
    /// <summary>
    /// Master processing class for analysis manager
    /// </summary>
    /// <remarks></remarks>
    public class clsMainProcess : clsLoggerBase
    {
        #region "Constants"

        private const int MAX_ERROR_COUNT = 10;
        private const string DECON2LS_FATAL_REMOTING_ERROR = "Fatal remoting error";
        private const string DECON2LS_CORRUPTED_MEMORY_ERROR = "Corrupted memory error";
        private const string DECON2LS_TCP_ALREADY_REGISTERED_ERROR = "channel 'tcp' is already registered";

        private const string DEFAULT_BASE_LOGFILE_NAME = @"Logs\AnalysisMgr";

        private const bool ENABLE_LOGGER_TRACE_MODE = false;

        #endregion

        #region "Member variables"

        // clsAnalysisMgrSettings
        private IMgrParams m_MgrSettings;

        private clsCleanupMgrErrors m_MgrErrorCleanup;
        private readonly string m_MgrExeName;
        private readonly string m_MgrDirectoryPath;
        private string m_WorkDirPath;

        private string m_MgrName = "??";

        // clsAnalysisJob
        private clsAnalysisJob m_AnalysisTask;

        private clsPluginLoader m_PluginLoader;

        private clsSummaryFile m_SummaryFile;
        private FileSystemWatcher m_ConfigFileWatcher;
        private FileSystemWatcher m_LocalSettingsFileWatcher;

        private bool m_ConfigChanged;

        private bool mDMSProgramsSynchronized;

        private clsStatusFile m_StatusTools;

        private clsMyEMSLUtilities m_MyEMSLUtilities;
        private bool m_NeedToAbortProcessing;

        private string m_MostRecentJobInfo;

        private string m_MostRecentErrorMessage = string.Empty;

        private int mPluginLoaderErrorCount;

        private string mPluginLoaderStepTool  = string.Empty;

        #endregion

        #region "Properties"

        /// <summary>
        /// When true, do not log messages to the manager status message queue
        /// </summary>
        public bool DisableMessageQueue { get; set; }

        /// <summary>
        /// When true, do not contact MyEMSL
        /// </summary>
        public bool DisableMyEMSL { get; set; }

        /// <summary>
        /// When true, only push analysis manager files to the remote host using the DMSUpdateManager
        /// Do not request a new analysis job
        /// </summary>
        /// <remarks>Only valid if the manager has parameter RunJobsRemotely set to True in the Manager Control DB</remarks>
        public bool PushRemoteMgrFilesOnly { get; set; }

        /// <summary>
        /// When true, show additional messages at the console
        /// </summary>
        public bool TraceMode { get; set; }

        #endregion

        #region "Methods"
        /// <summary>
        /// Starts program execution
        /// </summary>
        /// <returns>0 if no error; error code if an error</returns>
        /// <remarks></remarks>
        public int Main()
        {
            try
            {
                ShowTrace("Initializing the manager");

                if (!InitMgr())
                {
                    ShowTrace("InitMgr returned false; aborting");
                    return -1;
                }

                ShowTrace("Call DoAnalysis");

                if (PushRemoteMgrFilesOnly)
                {
                    m_AnalysisTask.AddAdditionalParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job", "100");
                    m_AnalysisTask.AddAdditionalParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step", "1");
                    m_AnalysisTask.AddAdditionalParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "StepTool", "Sync");
                    m_AnalysisTask.AddAdditionalParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_DATASET_NAME, "Placeholder");

                    var transferUtility = InitializeRemoteTransferUtility();

                    if (transferUtility == null)
                    {
                        return -1;
                    }

                    ShowTrace("Pushing new/updated DMS_Programs files to remote host");

                    transferUtility.RunDMSUpdateManager();
                    return 0;
                }

                DoAnalysis();

                ShowTrace("Exiting clsMainProcess.Main with error code = 0");
                return 0;
            }
            catch (Exception ex)
            {
                // Report any exceptions not handled at a lower level to the console
                var errMsg = "Critical exception starting application: " + ex.Message;
                ShowTrace(errMsg + "; " + clsGlobal.GetExceptionStackTrace(ex, true));
                ShowTrace("Exiting clsMainProcess.Main with error code = 1");
                return 1;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public clsMainProcess(bool traceModeEnabled)
        {
            TraceMode = traceModeEnabled;
            m_ConfigChanged = false;
            m_DebugLevel = 0;
            mDMSProgramsSynchronized = false;
            m_NeedToAbortProcessing = false;
            m_MostRecentJobInfo = string.Empty;

            var exeInfo = new FileInfo(PRISM.FileProcessor.ProcessFilesOrFoldersBase.GetAppPath());
            m_MgrExeName = exeInfo.Name;
            m_MgrDirectoryPath = exeInfo.DirectoryName;
        }

        /// <summary>
        /// Initializes the manager settings
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        /// <remarks></remarks>
        private bool InitMgr()
        {

            var hostName = System.Net.Dns.GetHostName();

            // Define the default logging info
            // This will get updated below
            var baseLogFileName = clsGlobal.LinuxOS ? DEFAULT_BASE_LOGFILE_NAME.Replace('\\', '/') : DEFAULT_BASE_LOGFILE_NAME;
            LogTools.CreateFileLogger(baseLogFileName, BaseLogger.LogLevels.DEBUG);

            if (!clsGlobal.OfflineMode)
            {
                // Create a database logger connected to DMS5
                // Once the initial parameters have been successfully read,
                // we update the dbLogger to use the connection string read from the Manager Control DB
                string defaultDmsConnectionString;

                // Open AnalysisManagerProg.exe.config to look for setting DefaultDMSConnString, so we know which server to log to by default
                var dmsConnectionStringFromConfig = GetXmlConfigDefaultConnectionString();

                if (string.IsNullOrWhiteSpace(dmsConnectionStringFromConfig))
                {
                    // Use the hard-coded default that points to Gigasax
                    defaultDmsConnectionString = Properties.Settings.Default.DefaultDMSConnString;
                }
                else
                {
                    // Use the connection string from AnalysisManagerProg.exe.config
                    defaultDmsConnectionString = dmsConnectionStringFromConfig;
                }

                ShowTrace("Instantiate a DbLogger using " + defaultDmsConnectionString);

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                LogTools.CreateDbLogger(defaultDmsConnectionString, "Analysis Tool Manager: " + hostName, TraceMode && ENABLE_LOGGER_TRACE_MODE);
            }

            // Get the manager settings from the database or from ManagerSettingsLocal.xml if clsGlobal.OfflineMode is true
            // If you get an exception here while debugging in Visual Studio, be sure
            //   that "UsingDefaults" is set to False in AppName.exe.config
            try
            {
                ShowTrace("Reading application config file");

                // Load settings from config file AnalysisManagerProg.exe.config
                var lstMgrSettings = LoadMgrSettingsFromFile();

                try
                {
                    m_MgrSettings = new clsAnalysisMgrSettings(lstMgrSettings, m_MgrDirectoryPath, TraceMode);
                }
                catch (Exception ex)
                {
                    ConsoleMsgUtils.ShowError("Exception instantiating clsAnalysisMgrSettings: " + ex.Message);
                    clsGlobal.IdleLoop(0.5);
                    return false;
                }
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Exception loading settings from AnalysisManagerProg.exe.config: " + ex.Message);
                clsGlobal.IdleLoop(0.5);
                return false;
            }

            m_MgrName = m_MgrSettings.ManagerName;
            ShowTrace("Manager name is " + m_MgrName);

            // Delete any temporary files that may be left in the app directory
            RemoveTempFiles();

            // Setup the loggers

            var logFileNameBase = GetBaseLogFileName();

            // The analysis manager determines when to log or not log based on internal logic
            // Set the LogLevel tracked by FileLogger to DEBUG so that all messages sent to the class are logged
            LogTools.CreateFileLogger(logFileNameBase, BaseLogger.LogLevels.DEBUG);

            if (!clsGlobal.OfflineMode)
            {
                var logCnStr = m_MgrSettings.GetParam("connectionstring");

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                LogTools.CreateDbLogger(logCnStr, "Analysis Tool Manager: " + m_MgrName, TraceMode && ENABLE_LOGGER_TRACE_MODE);
            }

            // Make the initial log entry
            var relativeLogFilePath = LogTools.CurrentLogFilePath;
            var logFile = new FileInfo(relativeLogFilePath);
            ShowTrace("Initializing log file " + clsPathUtils.CompactPathString(logFile.FullName, 60));

            var appVersion = Assembly.GetEntryAssembly().GetName().Version;
            var startupMsg = "=== Started Analysis Manager V" + appVersion + " ===== ";
            LogMessage(startupMsg);

            var configFileName = m_MgrSettings.GetParam("configfilename");
            if (string.IsNullOrEmpty(configFileName))
            {
                // Manager parameter error; log an error and exit
                LogError("Manager parameter 'configfilename' is undefined; this likely indicates a problem retrieving manager parameters.  Shutting down the manager");
                return false;
            }

            // Setup a file watcher for the config file(s)
            m_ConfigFileWatcher = CreateConfigFileWatcher(configFileName);
            m_ConfigFileWatcher.Changed += m_ConfigFileWatcher_Changed;

            if (clsGlobal.OfflineMode)
            {
                m_LocalSettingsFileWatcher = CreateConfigFileWatcher(clsAnalysisMgrSettings.LOCAL_MANAGER_SETTINGS_FILE);
                m_LocalSettingsFileWatcher.Changed += m_ConfigFileWatcher_Changed;
            }

            // Get the debug level
            m_DebugLevel = (short)m_MgrSettings.GetParam("debuglevel", 2);

            // Make sure that the manager name matches the machine name (with a few exceptions)
            if (!hostName.StartsWith("emslmq", StringComparison.OrdinalIgnoreCase) &&
                !hostName.StartsWith("emslpub", StringComparison.OrdinalIgnoreCase) &&
                !hostName.StartsWith("monroe", StringComparison.OrdinalIgnoreCase) &&
                !hostName.StartsWith("WE27676", StringComparison.OrdinalIgnoreCase))
            {
                if (!m_MgrName.StartsWith(hostName, StringComparison.OrdinalIgnoreCase))
                {
                    LogError("Manager name does not match the host name: " + m_MgrName + " vs. " + hostName + "; update " + configFileName);
                    return false;
                }
            }

            // Setup the tool for getting tasks
            ShowTrace("Instantiate m_AnalysisTask as new clsAnalysisJob");
            m_AnalysisTask = new clsAnalysisJob(m_MgrSettings, m_DebugLevel) {
                TraceMode = TraceMode
            };

            m_WorkDirPath = m_MgrSettings.GetParam(clsAnalysisMgrSettings.MGR_PARAM_WORK_DIR);

            LogTools.WorkDirPath = m_WorkDirPath;

            // Setup the manager cleanup class
            ShowTrace("Setup the manager cleanup class");

            string mgrConfigDBConnectionString;
            if (clsGlobal.OfflineMode)
            {
                mgrConfigDBConnectionString = string.Empty;
            }
            else
            {
                // Data Source=proteinseqs;Initial Catalog=manager_control
                mgrConfigDBConnectionString = m_MgrSettings.GetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING);
            }

            m_MgrErrorCleanup = new clsCleanupMgrErrors(mgrConfigDBConnectionString, m_MgrName, m_DebugLevel, m_MgrDirectoryPath, m_WorkDirPath);

            ShowTrace("Initialize the Summary file");

            m_SummaryFile = new clsSummaryFile();
            m_SummaryFile.Clear();

            ShowTrace("Initialize the Plugin Loader");

            m_PluginLoader = new clsPluginLoader(m_SummaryFile, m_MgrDirectoryPath);
            RegisterEvents(m_PluginLoader);

            if (TraceMode)
                m_PluginLoader.TraceMode = true;

            // Use a custom error event handler
            m_PluginLoader.ErrorEvent -= ErrorEventHandler;
            m_PluginLoader.ErrorEvent += PluginLoader_ErrorEventHandler;

            // Everything worked
            return true;
        }

        /// <summary>
        /// Loop to perform all analysis jobs
        /// </summary>
        /// <remarks></remarks>
        public void DoAnalysis()
        {
            ShowTrace("Entering clsMainProcess.DoAnalysis");

            var loopCount = 0;
            var tasksStartedCount = 0;
            var errorDeletingFilesFlagFile = false;

            var dtLastConfigDBUpdate = DateTime.UtcNow;

            // Used to track critical manager errors (not necessarily failed analysis jobs when the plugin reports "no results" or similar)
            var criticalMgrErrorCount = 0;
            var successiveDeadLockCount = 0;

            try
            {
                ShowTrace("Entering clsMainProcess.DoAnalysis Try/Catch block");

                var maxLoopCount = m_MgrSettings.GetParam("maxrepetitions", 1);
                var requestJobs = true;
                var oneTaskStarted = false;
                var oneTaskPerformed = false;

                InitStatusTools();

                while (loopCount < maxLoopCount && requestJobs)
                {
                    UpdateStatusIdle("No analysis jobs found");

                    // Check for configuration change
                    // This variable will be true if the CaptureTaskManager.exe.config file has been updated
                    if (m_ConfigChanged)
                    {
                        // Local config file has changed
                        m_ConfigChanged = false;

                        ShowTrace("Reloading manager settings since config file has changed");

                        if (!ReloadManagerSettings())
                        {
                            return;
                        }

                        m_ConfigFileWatcher.EnableRaisingEvents = true;
                        if (m_LocalSettingsFileWatcher != null)
                            m_LocalSettingsFileWatcher.EnableRaisingEvents = true;
                    }
                    else
                    {
                        // Reload the manager control DB settings in case they have changed
                        // However, only reload every 2 minutes
                        if (!UpdateManagerSettings(ref dtLastConfigDBUpdate, 2))
                        {
                            // Error retrieving settings from the manager control DB
                            return;
                        }
                    }

                    // Check to see if manager is still active
                    var mgrActive = m_MgrSettings.GetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_ACTIVE, false);
                    var mgrActiveLocal = m_MgrSettings.GetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, false);

                    if (!(mgrActive && mgrActiveLocal))
                    {
                        string managerDisableReason;
                        if (!mgrActiveLocal)
                        {
                            managerDisableReason = "Disabled locally via AnalysisManagerProg.exe.config";
                            UpdateStatusDisabled(EnumMgrStatus.DISABLED_LOCAL, managerDisableReason);
                        }
                        else
                        {
                            managerDisableReason = "Disabled in Manager Control DB";
                            UpdateStatusDisabled(EnumMgrStatus.DISABLED_MC, managerDisableReason);
                        }

                        LogMessage("Manager inactive: " + managerDisableReason);
                        clsGlobal.IdleLoop(0.75);
                        return;
                    }

                    var mgrUpdateRequired = m_MgrSettings.GetParam("ManagerUpdateRequired", false);
                    if (mgrUpdateRequired)
                    {
                        var msg = "Manager update is required";
                        LogMessage(msg);
                        m_MgrSettings.AckManagerUpdateRequired();
                        UpdateStatusIdle("Manager update is required");
                        return;
                    }

                    if (m_MgrErrorCleanup.DetectErrorDeletingFilesFlagFile())
                    {
                        // Delete the Error Deleting status flag file first, so next time through this step is skipped
                        m_MgrErrorCleanup.DeleteErrorDeletingFilesFlagFile();

                        // There was a problem deleting non result files with the last job.  Attempt to delete files again
                        if (!m_MgrErrorCleanup.CleanWorkDir())
                        {
                            if (oneTaskStarted)
                            {
                                LogError("Error cleaning working directory, job " + m_AnalysisTask.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job") + "; see folder " + m_WorkDirPath);
                                m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Error cleaning working directory");
                            }
                            else
                            {
                                LogError("Error cleaning working directory; see folder " + m_WorkDirPath);
                            }
                            m_MgrErrorCleanup.CreateStatusFlagFile();
                            UpdateStatusFlagFileExists();
                            return;
                        }
                        // Successful delete of files in working directory, so delete the status flag file
                        m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel);
                    }

                    // Verify that an error hasn't left the the system in an odd state
                    if (StatusFlagFileError())
                    {
                        LogError("Flag file exists - unable to perform any further analysis jobs");
                        UpdateStatusFlagFileExists();
                        clsGlobal.IdleLoop(1.5);
                        return;
                    }

                    // Check to see if an excessive number of errors have occurred
                    if (criticalMgrErrorCount > MAX_ERROR_COUNT)
                    {
                        LogError("Excessive task failures; disabling manager via flag file");

                        // Note: We previously called DisableManagerLocally() to update AnalysisManager.config.exe
                        // We now create a flag file instead
                        // This gives the manager a chance to auto-cleanup things if ManagerErrorCleanupMode is >= 1

                        m_MgrErrorCleanup.CreateStatusFlagFile();
                        UpdateStatusFlagFileExists();

                        break;
                    }

                    // Verify working directory properly specified and empty
                    if (!ValidateWorkingDir())
                    {
                        if (oneTaskStarted)
                        {
                            // Working directory problem due to the most recently processed job
                            // Create ErrorDeletingFiles file and exit the program
                            LogError("Working directory problem, creating " + clsCleanupMgrErrors.ERROR_DELETING_FILES_FILENAME + "; see folder " + m_WorkDirPath);
                            m_MgrErrorCleanup.CreateErrorDeletingFilesFlagFile();
                            UpdateStatusIdle("Working directory not empty");
                        }
                        else
                        {
                            // Working directory problem, so create flag file and exit
                            LogError("Working directory problem, disabling manager via flag file; see folder " + m_WorkDirPath);
                            m_MgrErrorCleanup.CreateStatusFlagFile();
                            UpdateStatusFlagFileExists();
                        }
                        break;
                    }

                    if (!clsGlobal.LinuxOS)
                    {
                        if (WindowsUpdatesArePending())
                        {
                            // Check whether the computer is likely to install the monthly Windows Updates within the next few hours
                            // Do not request a task between 12 am and 6 am on Thursday in the week with the third Tuesday of the month
                            // Do not request a task between 2 am and 4 am or between 9 am and 11 am on Sunday following the week with the third Tuesday of the month
                            break;
                        }
                    }

                    if (clsGlobal.OfflineMode)
                        ShowTrace("Looking for an available offline task in the task queue directory");
                    else
                        ShowTrace("Requesting a new task from DMS_Pipeline");

                    // Re-initialize these utilities for each analysis job
                    // Note that when RetrieveResources is called, the MyEMSL certificate file (svc-dms.pfx) will be verified to exist
                    // (via GetSharedResources calling CertificateFileExists)
                    m_MyEMSLUtilities = new clsMyEMSLUtilities(m_DebugLevel, m_WorkDirPath, TraceMode);
                    RegisterEvents(m_MyEMSLUtilities);

                    // Get an analysis job, if any are available

                    var taskReturn = m_AnalysisTask.RequestTask();

                    switch (taskReturn)
                    {
                        case clsDBTask.RequestTaskResult.NoTaskFound:
                            ShowTrace("No tasks found for " + m_MgrName);

                            // No tasks found
                            if (m_DebugLevel >= 3)
                            {
                                LogMessage("No analysis jobs found for " + m_MgrName);
                            }
                            requestJobs = false;
                            criticalMgrErrorCount = 0;
                            break;

                        case clsDBTask.RequestTaskResult.ResultError:
                            ShowTrace("Error requesting a task for " + m_MgrName);

                            // There was a problem getting the task; errors were logged by RequestTaskResult
                            criticalMgrErrorCount += 1;
                            break;

                        case clsDBTask.RequestTaskResult.TaskFound:

                            ShowTrace("Task found for " + m_MgrName);

                            tasksStartedCount += 1;
                            successiveDeadLockCount = 0;

                            try
                            {
                                oneTaskStarted = true;
                                var defaultManagerWorkDir = string.Copy(m_WorkDirPath);

                                var success = DoAnalysisJob(out var runningRemote);

                                if (!string.Equals(m_WorkDirPath, defaultManagerWorkDir))
                                {
                                    // Restore the work dir path
                                    m_WorkDirPath = string.Copy(defaultManagerWorkDir);
                                    m_MgrSettings.SetParam(clsAnalysisMgrSettings.MGR_PARAM_WORK_DIR, m_WorkDirPath);
                                }

                                if (success)
                                {
                                    // Task succeeded; reset the sequential job failure counter
                                    criticalMgrErrorCount = 0;
                                    oneTaskPerformed = true;
                                }
                                else
                                {
                                    // Something went wrong; errors were logged by DoAnalysisJob
                                    if (m_MostRecentErrorMessage.Contains("None of the spectra are centroided") ||
                                        m_MostRecentErrorMessage.Contains("No peaks found") ||
                                        m_MostRecentErrorMessage.Contains("No spectra were exported"))
                                    {
                                        // Job failed, but this was not a manager error
                                        // Do not increment the error count
                                    }
                                    else if (!runningRemote)
                                    {
                                        criticalMgrErrorCount += 1;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                // Something went wrong; errors likely were not logged by DoAnalysisJob

                                LogError("Exception thrown by DoAnalysisJob", ex);
                                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysis(): " + ex.Message, m_MostRecentJobInfo, true);

                                // Set the job state to failed
                                m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Exception thrown by DoAnalysisJob");

                                criticalMgrErrorCount += 1;
                                m_NeedToAbortProcessing = true;
                            }
                            break;

                        case clsDBTask.RequestTaskResult.TooManyRetries:
                            ShowTrace("Too many retries calling the stored procedure");

                            // There were too many retries calling the stored procedure; errors were logged by RequestTaskResult
                            // Bump up loopCount to the maximum to exit the loop
                            UpdateStatusIdle("Excessive retries requesting task");
                            loopCount = maxLoopCount;
                            break;

                        case clsDBTask.RequestTaskResult.Deadlock:

                            ShowTrace("Deadlock");

                            // A deadlock error occured
                            // Query the DB again, but only if we have not had 3 deadlock results in a row
                            successiveDeadLockCount += 1;
                            if (successiveDeadLockCount >= 3)
                            {
                                var msg = "Deadlock encountered " + successiveDeadLockCount + " times in a row when requesting a new task; exiting";
                                LogWarning(msg);
                                requestJobs = false;
                            }
                            break;

                        default:
                            // Shouldn't ever get here
                            LogError("Invalid request result: " + (int)taskReturn);
                            return;
                    }

                    if (NeedToAbortProcessing())
                    {
                        ShowTrace("Need to abort processing");
                        break;
                    }
                    loopCount += 1;

                    // If the only problem was deleting non result files, we want to stop the manager
                    if (m_MgrErrorCleanup.DetectErrorDeletingFilesFlagFile())
                    {
                        ShowTrace("Error deleting files flag file");
                        errorDeletingFilesFlagFile = true;
                        loopCount = maxLoopCount;
                    }
                }

                if (loopCount >= maxLoopCount)
                {
                    if (errorDeletingFilesFlagFile)
                    {
                        if (tasksStartedCount > 0)
                        {
                            LogWarning("Error deleting file with an open file handle; closing manager. Jobs processed: " + tasksStartedCount.ToString());
                            clsGlobal.IdleLoop(1.5);
                        }
                    }
                    else
                    {
                        if (tasksStartedCount > 0)
                        {
                            var msg = "Maximum number of jobs to analyze has been reached: " + tasksStartedCount.ToString() + " job";
                            if (tasksStartedCount != 1)
                                msg += "s";
                            msg += "; closing manager";
                            LogMessage(msg);
                        }
                    }
                }

                if (oneTaskPerformed)
                {
                    LogMessage("Analysis complete for all available jobs");
                }

                ShowTrace("Closing the manager");
                UpdateClose("Closing manager.");
            }
            catch (Exception ex)
            {
                LogError("Exception in DoAnalysis", ex);
                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysis(): " + ex.Message, m_MostRecentJobInfo, true);
            }
            finally
            {
                if (m_StatusTools != null)
                {

                    // Wait 1 second to give the message queue time to flush
                    clsGlobal.IdleLoop(1);

                    ShowTrace("Disposing message queue via m_StatusTools.DisposeMessageQueue");
                    m_StatusTools.DisposeMessageQueue();
                }
            }
        }

        /// <summary>
        /// Perform an analysis job
        /// </summary>
        /// <param name="runningRemote">True if we checked the status of a remote job</param>
        /// <returns></returns>
        private bool DoAnalysisJob(out bool runningRemote)
        {
            var jobNum = m_AnalysisTask.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0);
            var stepNum = m_AnalysisTask.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step", 0);
            var cpuLoadExpected = m_AnalysisTask.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "CPU_Load", 1);

            var datasetName = m_AnalysisTask.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_DATASET_NAME);
            var jobToolDescription = m_AnalysisTask.GetCurrentJobToolDescription();

            var runJobsRemotely = m_MgrSettings.GetParam("RunJobsRemotely", false);
            var runningRemoteFlag = m_AnalysisTask.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "RunningRemote", 0);
            runningRemote = runningRemoteFlag > 0;

            if (clsGlobal.OfflineMode)
            {
                // Update the working directory path to match the current task
                // This manager setting was updated by SelectOfflineJobInfoFile in clsAnalysisJob
                m_WorkDirPath = m_MgrSettings.GetParam(clsAnalysisMgrSettings.MGR_PARAM_WORK_DIR);
            }

            ShowTrace("Processing job " + jobNum + ", " + jobToolDescription);

            // Initialize summary and status files
            m_SummaryFile.Clear();

            if (m_StatusTools == null)
            {
                InitStatusTools();
            }

            // Update the cached most recent job info
            m_MostRecentJobInfo = ConstructMostRecentJobInfoText(
                DateTime.Now.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT),
                jobNum, datasetName, jobToolDescription);

            m_StatusTools.TaskStartTime = DateTime.UtcNow;
            m_StatusTools.Dataset = datasetName;
            m_StatusTools.WorkDirPath = m_WorkDirPath;
            m_StatusTools.JobNumber = jobNum;
            m_StatusTools.JobStep = stepNum;
            m_StatusTools.Tool = jobToolDescription;
            m_StatusTools.MgrName = m_MgrName;
            m_StatusTools.ProgRunnerProcessID = 0;
            m_StatusTools.ProgRunnerCoreUsage = cpuLoadExpected;

            if (clsGlobal.OfflineMode)
            {
                m_StatusTools.OfflineJobStatusFilePath = clsRemoteTransferUtility.GetOfflineJobStatusFilePath(m_MgrSettings, m_AnalysisTask);
            }

            m_StatusTools.UpdateAndWrite(
                EnumMgrStatus.RUNNING,
                EnumTaskStatus.RUNNING,
                EnumTaskStatusDetail.RETRIEVING_RESOURCES,
                0, 0, string.Empty, string.Empty,
                m_MostRecentJobInfo, true);

            var processID = Process.GetCurrentProcess().Id;

            // Note: The format of the following text is important; be careful about changing it
            // In particular, function DetermineRecentErrorMessages in clsMainProcess looks for log entries
            //   matching RegEx: "^([^,]+),.+Started analysis job (\d+), Dataset (.+), Tool ([^,]+)"

            // Example log entries
            // 5/04/2015 12:34:46, Pub-88-3: Started analysis job 1193079, Dataset Lp_PDEC_N-sidG_PD1_1May15_Lynx_15-01-24, Tool Decon2LS_V2, Step 1, INFO,
            // 5/04/2015 10:54:49, Proto-6_Analysis-1: Started analysis job 1192426, Dataset LewyHNDCGlobFractestrecheck_SRM_HNDC_Frac46_smeagol_05Apr15_w6326a, Tool Results_Transfer (MASIC_Finnigan), Step 2, INFO,

            LogMessage(m_MgrName + ": Started analysis job " + jobNum + ", Dataset " + datasetName + ", Tool " + jobToolDescription + ", Process ID " + processID);

            if (m_DebugLevel >= 2)
            {
                // Log the debug level value whenever the debug level is 2 or higher
                LogMessage("Debug level is " + m_DebugLevel);
            }

            // Create an object to manage the job resources
            if (!SetResourceObject(out var toolResourcer))
            {
                LogError(m_MgrName + ": Unable to set the Resource object, job " + jobNum + ", Dataset " + datasetName, true);
                m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Unable to set resource object");
                m_MgrErrorCleanup.CleanWorkDir();
                UpdateStatusIdle("Error encountered: Unable to set resource object");
                return false;
            }

            // Create an object to run the analysis tool
            if (!SetToolRunnerObject(out var toolRunner))
            {
                LogError(m_MgrName + ": Unable to set the toolRunner object, job " + jobNum + ", Dataset " + datasetName, true);
                m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Unable to set tool runner object");
                m_MgrErrorCleanup.CleanWorkDir();
                UpdateStatusIdle("Error encountered: Unable to set tool runner object");
                return false;
            }

            if (NeedToAbortProcessing())
            {
                ShowTrace("NeedToAbortProcessing; closing job step task");
                m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Processing aborted");
                m_MgrErrorCleanup.CleanWorkDir();
                UpdateStatusIdle("Processing aborted");
                return false;
            }

            // Make sure we have enough free space on the drive with the working directory and on the drive with the transfer folder
            if (!ValidateFreeDiskSpace(toolResourcer, out m_MostRecentErrorMessage))
            {
                ShowTrace("Insufficient free space; closing job step task");
                if (string.IsNullOrEmpty(m_MostRecentErrorMessage))
                {
                    m_MostRecentErrorMessage = "Insufficient free space (location undefined)";
                }
                m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, m_MostRecentErrorMessage);
                m_MgrErrorCleanup.CleanWorkDir();
                UpdateStatusIdle("Processing aborted");
                return false;
            }

            // Possibly disable MyEMSL
            if (DisableMyEMSL)
            {
                toolResourcer.SetOption(clsGlobal.eAnalysisResourceOptions.MyEMSLSearchDisabled, true);
            }

            bool success;
            bool jobSucceeded;

            CloseOutType eToolRunnerResult;
            clsRemoteMonitor remoteMonitor;

            // Retrieve files required for the job
            m_MgrErrorCleanup.CreateStatusFlagFile();

            if (runningRemote)
            {
                // Job is running remotely; check its status
                // If completed (success or fail), retrieve the results
                success = CheckRemoteJobStatus(toolRunner, out eToolRunnerResult, out remoteMonitor);

                if (success && clsAnalysisJob.SuccessOrNoData(eToolRunnerResult))
                    jobSucceeded = true;
                else
                    jobSucceeded = false;
            }
            else
            {
                remoteMonitor = null;

                // Retrieve the resources for the job then either run locally or run remotely
                var resourcesRetrieved = RetrieveResources(toolResourcer, jobNum, datasetName, out eToolRunnerResult);
                if (!resourcesRetrieved)
                {
                    // Error occurred
                    // Note that m_AnalysisTask.CloseTask() should have already been called
                    var reportSuccess = HandleJobFailure(toolRunner, eToolRunnerResult);

                    return reportSuccess;
                }

                // Run the job
                m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.RUNNING_TOOL, 0);

                if (runJobsRemotely)
                {
                    // Transfer files to the remote host so that the job can run remotely
                    success = RunJobRemotely(toolResourcer, jobNum, stepNum, out eToolRunnerResult);
                    if (!success)
                    {
                        ShowTrace("Error staging the job to run remotely; closing job step task");

                        if (string.IsNullOrEmpty(m_MostRecentErrorMessage))
                        {
                            m_MostRecentErrorMessage = "Unknown error staging the job to run remotely";
                        }
                        m_AnalysisTask.CloseTask(eToolRunnerResult, m_MostRecentErrorMessage, toolRunner);
                    }

                    // jobSucceeded is always false when we stage files to run remotely
                    // Only set it to true if CheckRemoteJobStatus reports success and the eToolRunnerResult is Success or No_Data
                    jobSucceeded = false;
                }
                else
                {
                    success = RunJobLocally(toolRunner, jobNum, datasetName, out eToolRunnerResult);

                    // Note: if success is false, RunJobLocally will have already called .CloseTask

                    jobSucceeded = success;
                }
            }

            if (!success)
            {
                // Error occurred
                // Note that m_AnalysisTask.CloseTask() should have already been called
                var reportSuccess = HandleJobFailure(toolRunner, eToolRunnerResult);

                return reportSuccess;

            }

            // Close out the job
            m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.CLOSING, EnumTaskStatusDetail.CLOSING, 100);
            try
            {
                ShowTrace("Task completed successfully; closing the job step task");

                CloseOutType closeOut;
                if (runJobsRemotely)
                {
                    // eToolRunnerResult will be CLOSEOUT_RUNNING_REMOTE if RunJobRemotely was called
                    // or if CheckRemoteJobStatus was called and the job is still in progress

                    // eToolRunnerResult will be CLOSEOUT_SUCCESS if CheckRemoteJobStatus found that the job was done
                    // and successfully retrieved the results

                    closeOut = eToolRunnerResult;
                }
                else
                {
                    closeOut = CloseOutType.CLOSEOUT_SUCCESS;
                }

                // Close out the job as a success
                m_AnalysisTask.CloseTask(closeOut, string.Empty, toolRunner);
                LogMessage(m_MgrName + ": Completed job " + jobNum);

                UpdateStatusIdle("Completed job " + jobNum + ", step " + stepNum);

                var deleteRemoteJobFiles = runningRemote && jobSucceeded;

                var cleanupSuccess = CleanupAfterJob(deleteRemoteJobFiles, remoteMonitor);

                return cleanupSuccess;
            }
            catch (Exception ex)
            {
                LogError("Exception closing task after a normal run", ex);
                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " + ex.Message, m_MostRecentJobInfo, true);
                return false;
            }

        }

        private bool CheckRemoteJobStatus(
            IToolRunner toolRunner,
            out CloseOutType eToolRunnerResult,
            out clsRemoteMonitor remoteMonitor)
        {

            try
            {
                LogDebug("Instantiating clsRemoteMonitor to check remote job status");

                remoteMonitor = new clsRemoteMonitor(m_MgrSettings, m_AnalysisTask, toolRunner, m_StatusTools);
                RegisterEvents(remoteMonitor);

                remoteMonitor.StaleJobStatusFileEvent += RemoteMonitor_StaleJobStatusFileEvent;
                remoteMonitor.StaleLockFileEvent += RemoteMonitor_StaleLockFileEvent;

            }
            catch (Exception ex)
            {
                m_MostRecentErrorMessage = "Exception instantiating the RemoteMonitor class";
                LogError(m_MostRecentErrorMessage, ex);
                eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                remoteMonitor = null;
                return false;
            }

            try
            {
                var eJobStatus = remoteMonitor.GetRemoteJobStatus();

                switch (eJobStatus)
                {
                    case clsRemoteMonitor.EnumRemoteJobStatus.Undefined:
                        m_MostRecentErrorMessage = "Undefined remote job status; check the logs";
                        LogError(clsGlobal.AppendToComment(m_MostRecentErrorMessage, remoteMonitor.Message));

                        eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                        return false;

                    case clsRemoteMonitor.EnumRemoteJobStatus.Unstarted:
                        LogDebug("Remote job has not yet started", 2);
                        eToolRunnerResult = CloseOutType.CLOSEOUT_RUNNING_REMOTE;
                        return true;

                    case clsRemoteMonitor.EnumRemoteJobStatus.Running:
                        LogDebug(string.Format("Remote job is running, {0:F1}% complete", remoteMonitor.RemoteProgress), 2);
                        eToolRunnerResult = CloseOutType.CLOSEOUT_RUNNING_REMOTE;
                        return true;

                    case clsRemoteMonitor.EnumRemoteJobStatus.Success:

                        var success = HandleRemoteJobSuccess(toolRunner, remoteMonitor, out eToolRunnerResult);
                        if (!success)
                        {
                            m_MostRecentErrorMessage = toolRunner.Message;
                            m_AnalysisTask.CloseTask(eToolRunnerResult, m_MostRecentErrorMessage, toolRunner);
                        }
                        return success;

                    case clsRemoteMonitor.EnumRemoteJobStatus.Failed:

                        HandleRemoteJobFailure(toolRunner, remoteMonitor, out eToolRunnerResult);
                        m_AnalysisTask.CloseTask(eToolRunnerResult, m_MostRecentErrorMessage, toolRunner);
                        return false;

                    default:
                        m_MostRecentErrorMessage = "Unrecognized remote job status: " + eJobStatus;
                        LogError(clsGlobal.AppendToComment(m_MostRecentErrorMessage, remoteMonitor.Message));

                        eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                        return false;
                }

            }
            catch (Exception ex)
            {
                LogError("Exception checking job status on the remote host", ex);
                eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                return false;
            }
        }

        private bool CleanupAfterJob(bool deleteRemoteJobFiles, clsRemoteMonitor remoteMonitor)
        {
            try
            {
                if (deleteRemoteJobFiles)
                {
                    // Job succeeded, and the status in DMS was successfully updated
                    // Delete files on the remote host
                    remoteMonitor.DeleteRemoteJobFiles();
                }

                // If success was reported check to see if there was an error deleting non result files
                if (m_MgrErrorCleanup.DetectErrorDeletingFilesFlagFile())
                {
                    // If there was a problem deleting non result files, return success and let the manager try to delete the files one more time on the next start up
                    // However, wait another 5 seconds before continuing
                    clsProgRunner.GarbageCollectNow();
                    clsGlobal.IdleLoop(5);

                    return true;
                }

                // Clean the working directory
                try
                {
                    if (!m_MgrErrorCleanup.CleanWorkDir(1))
                    {
                        LogError("Error cleaning working directory, job " + m_AnalysisTask.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job"));
                        m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Error cleaning working directory");
                        m_MgrErrorCleanup.CreateErrorDeletingFilesFlagFile();
                        return false;
                    }
                }
                catch (Exception ex)
                {
                    LogError("Exception cleaning work directory after normal run", ex);
                    m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.CleanupAfterJob(): " + ex.Message, m_MostRecentJobInfo, true);
                    return false;
                }

                // Delete the status flag file
                m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel);

                // Note that we do not need to call m_StatusTools.UpdateIdle() here since
                // we called UpdateStatusIdle() just after m_AnalysisTask.CloseTask above

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in CleanupAfterJob", ex);
                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.CleanupAfterJob(): " + ex.Message, m_MostRecentJobInfo, true);
                return false;
            }
        }

        /// <summary>
        /// Constructs a description of the given job using the job number, step tool name, and dataset name
        /// </summary>
        /// <param name="jobStartTimeStamp">Time job started</param>
        /// <param name="job">Job name</param>
        /// <param name="dataset">Dataset name</param>
        /// <param name="toolName">Tool name (or step tool name)</param>
        /// <returns>Info string, similar to: Job 375797; DataExtractor (XTandem), Step 4; QC_Shew_09_01_b_pt5_25Mar09_Griffin_09-02-03; 3/26/2009 3:17:57 AM</returns>
        /// <remarks></remarks>
        private string ConstructMostRecentJobInfoText(string jobStartTimeStamp, int job, string dataset, string toolName)
        {
            try
            {
                if (jobStartTimeStamp == null)
                    jobStartTimeStamp = string.Empty;
                if (toolName == null)
                    toolName = "??";
                if (dataset == null)
                    dataset = "??";

                return "Job " + job + "; " + toolName + "; " + dataset + "; " + jobStartTimeStamp;
            }
            catch (Exception)
            {
                // Error combining the terms; return an empty string
                return string.Empty;
            }
        }

        private FileSystemWatcher CreateConfigFileWatcher(string configFileName)
        {
            var watcher = new FileSystemWatcher
            {
                Path = m_MgrDirectoryPath,
                IncludeSubdirectories = false,
                Filter = configFileName,
                NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size,
                EnableRaisingEvents = true
            };

            return watcher;
        }

        private bool DataPackageIdMissing()
        {
            var stepToolName = m_AnalysisTask.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "StepTool");

            var multiJobStepTools = new SortedSet<string> {
            "APE",
            "AScore",
            "Cyclops",
            "IDM",
            "Mage",
            "MultiAlign_Aggregator",
            "mzXML_Aggregator",
            "Phospho_FDR_Aggregator",
            "PRIDE_Converter",
            "RepoPkgr"
        };

            var dataPkgRequired = multiJobStepTools.Any(multiJobTool => string.Equals(stepToolName, multiJobTool, StringComparison.OrdinalIgnoreCase));

            if (dataPkgRequired)
            {
                var dataPkgID = m_AnalysisTask.GetJobParameter(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DataPackageID", 0);
                if (dataPkgID <= 0)
                {
                    // The data package ID is 0 or missing
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Given a log file with a name like AnalysisMgr_03-25-2009.txt, returns the log file name for the previous day
        /// </summary>
        /// <param name="logFilePath"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private string DecrementLogFilePath(string logFilePath)
        {
            try
            {
                var reLogFileName = new Regex(@"(?<BaseName>.+_)(?<Month>\d+)-(?<Day>\d+)-(?<Year>\d+).\S+", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var match = reLogFileName.Match(logFilePath);

                if (match.Success)
                {
                    var month = Convert.ToInt32(match.Groups["Month"].Value);
                    var day = Convert.ToInt32(match.Groups["Day"].Value);
                    var year = Convert.ToInt32(match.Groups["Year"].Value);

                    var dtCurrentDate = DateTime.Parse(year + "-" + month + "-" + day);
                    var dtNewDate = dtCurrentDate.AddDays(-1);

                    var previousLogFilePath = match.Groups["BaseName"].Value + dtNewDate.ToString(FileLogger.LOG_FILE_DATECODE) + Path.GetExtension(logFilePath);
                    return previousLogFilePath;
                }

            }
            catch (Exception ex)
            {
                LogError("Error in DecrementLogFilePath", ex);
            }

            return string.Empty;

        }

        /// <summary>
        /// Parses the log files for this manager to determine the recent error messages, returning up to errorMessageCountToReturn of them
        /// Will use objLogger to determine the most recent log file
        /// Also examines the message info stored in objLogger
        /// Lastly, if mostRecentJobInfo is empty, will update it with info on the most recent job started
        /// </summary>
        /// <param name="errorMessageCountToReturn">Maximum number of error messages to return</param>
        /// <param name="mostRecentJobInfo">Info on the most recent job started by this manager</param>
        /// <returns>List of recent errors</returns>
        /// <remarks></remarks>
        public IEnumerable<string> DetermineRecentErrorMessages(int errorMessageCountToReturn, ref string mostRecentJobInfo)
        {
            // This regex will match all text up to the first comma (this is the time stamp), followed by a comma, then the error message, then the text ", Error,"
            const string ERROR_MATCH_REGEX = "^(?<Date>[^,]+),(?<Error>.+), Error, *$";

            // This regex looks for information on a job starting
            // Note: do not try to match "Step \d+" with this regex due to variations on how the log message appears
            const string JOB_START_REGEX = @"^(?<Date>[^,]+),.+Started analysis job (?<Job>\d+), Dataset (?<Dataset>.+), Tool (?<Tool>[^,]+)";

            // Examples matching log entries
            // 5/04/2015 12:34:46, Pub-88-3: Started analysis job 1193079, Dataset Lp_PDEC_N-sidG_PD1_1May15_Lynx_15-01-24, Tool Decon2LS_V2, Step 1, INFO,
            // 5/04/2015 10:54:49, Proto-6_Analysis-1: Started analysis job 1192426, Dataset LewyHNDCGlobFractestrecheck_SRM_HNDC_Frac46_smeagol_05Apr15_w6326a, Tool Results_Transfer (MASIC_Finnigan), Step 2, INFO,

            // The following effectively defines the number of days in the past to search when finding recent errors
            const int MAX_LOG_FILES_TO_SEARCH = 5;

            // In this list, keys are error message strings and values are the corresponding time of the error
            var recentErrorMessages = new List<KeyValuePair<string, DateTime>>();

            if (mostRecentJobInfo == null)
                mostRecentJobInfo = string.Empty;

            try
            {
                var mostRecentJobInfoFromLogs = string.Empty;

                if (errorMessageCountToReturn < 1)
                    errorMessageCountToReturn = 1;

                // Initialize the RegEx that splits out the timestamp from the error message
                var reErrorLine = new Regex(ERROR_MATCH_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var reJobStartLine = new Regex(JOB_START_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // Initialize the queue that holds recent error messages
                var qErrorMsgQueue = new Queue<string>(errorMessageCountToReturn);

                // Initialize the hashtable to hold the error messages, but without date stamps
                var uniqueErrorMessages = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

                // Examine the most recent error reported by objLogger
                var lineIn = LogTools.MostRecentErrorMessage;
                bool loggerReportsError;
                if (!string.IsNullOrWhiteSpace(lineIn))
                {
                    loggerReportsError = true;
                }
                else
                {
                    loggerReportsError = false;
                }

                var logFilePath = GetRecentLogFilename();

                if (errorMessageCountToReturn > 1 || !loggerReportsError)
                {
                    // Recent error message reported by objLogger is empty or errorMessageCountToReturn is greater than one
                    // Open log file logFilePath to find the most recent error messages
                    // If not enough error messages are found, we will look through previous log files

                    var logFileCountProcessed = 0;
                    var checkForMostRecentJob = true;

                    while (qErrorMsgQueue.Count < errorMessageCountToReturn && logFileCountProcessed < MAX_LOG_FILES_TO_SEARCH)
                    {
                        if (File.Exists(logFilePath))
                        {
                            using (var srInFile = new StreamReader(new FileStream(logFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                            {

                                if (errorMessageCountToReturn < 1)
                                    errorMessageCountToReturn = 1;

                                while (!srInFile.EndOfStream)
                                {
                                    lineIn = srInFile.ReadLine();

                                    if (lineIn == null)
                                        continue;

                                    var oMatchError = reErrorLine.Match(lineIn);

                                    if (oMatchError.Success)
                                    {
                                        DetermineRecentErrorCacheError(oMatchError, lineIn, uniqueErrorMessages, qErrorMsgQueue,
                                                                       errorMessageCountToReturn);
                                    }

                                    if (!checkForMostRecentJob)
                                        continue;

                                    var oMatchJob = reJobStartLine.Match(lineIn);
                                    if (!oMatchJob.Success)
                                        continue;

                                    try
                                    {
                                        mostRecentJobInfoFromLogs = ConstructMostRecentJobInfoText(
                                            oMatchJob.Groups["Date"].Value,
                                            Convert.ToInt32(oMatchJob.Groups["Job"].Value),
                                            oMatchJob.Groups["Dataset"].Value,
                                            oMatchJob.Groups["Tool"].Value);
                                    }
                                    catch (Exception)
                                    {
                                        // Ignore errors here
                                    }
                                }

                            }

                            if (checkForMostRecentJob && mostRecentJobInfoFromLogs.Length > 0)
                            {
                                // We determine the most recent job; no need to check other log files
                                checkForMostRecentJob = false;
                            }
                        }
                        // else: Log file not found; that's OK, we'll decrement the name by one day and keep checking

                        // Increment the log file counter, regardless of whether or not the log file was found
                        logFileCountProcessed += 1;

                        if (qErrorMsgQueue.Count >= errorMessageCountToReturn)
                            continue;

                        // We still haven't found errorMessageCountToReturn error messages
                        // Keep checking older log files as long as qErrorMsgQueue.Count < errorMessageCountToReturn

                        // Decrement the log file path by one day
                        logFilePath = DecrementLogFilePath(logFilePath);
                        if (string.IsNullOrEmpty(logFilePath))
                        {
                            break;
                        }
                    }
                }

                if (loggerReportsError)
                {
                    // Append the error message reported by the Logger to the error message queue (treating it as the newest error)
                    lineIn = LogTools.MostRecentErrorMessage;
                    var match = reErrorLine.Match(lineIn);

                    if (match.Success)
                    {
                        DetermineRecentErrorCacheError(match, lineIn, uniqueErrorMessages, qErrorMsgQueue, errorMessageCountToReturn);
                    }
                }

                // Populate recentErrorMessages and dtRecentErrorMessageDates using the messages stored in qErrorMsgQueue
                while (qErrorMsgQueue.Count > 0)
                {
                    var errorMessageClean = qErrorMsgQueue.Dequeue();

                    // Find the newest timestamp for this message
                    if (!uniqueErrorMessages.TryGetValue(errorMessageClean, out var timeStamp))
                    {
                        // This code should not be reached
                        timeStamp = DateTime.MinValue;
                    }

                    recentErrorMessages.Add(new KeyValuePair<string, DateTime>(timeStamp + ", " + errorMessageClean.TrimStart(' '), timeStamp));
                }

                var sortedAndFilteredRecentErrors = (from item in recentErrorMessages
                                                     orderby item.Value descending
                                                     select item.Key).Take(errorMessageCountToReturn);

                if (string.IsNullOrEmpty(mostRecentJobInfo))
                {
                    if (!string.IsNullOrWhiteSpace(mostRecentJobInfoFromLogs))
                    {
                        // Update mostRecentJobInfo
                        mostRecentJobInfo = mostRecentJobInfoFromLogs;
                    }
                }

                return sortedAndFilteredRecentErrors;
            }
            catch (Exception ex)
            {
                // Ignore errors here
                try
                {
                    LogError("Error in DetermineRecentErrorMessages", ex);
                }
                catch (Exception)
                {
                    // Ignore errors logging the error
                }
                return new List<string>();
            }

        }

        private void DetermineRecentErrorCacheError(
            Match match,
            string errorMessage,
            IDictionary<string, DateTime> uniqueErrorMessages,
            Queue<string> qErrorMsgQueue,
            int maxErrorMessageCountToReturn)
        {
            DateTime timeStamp;
            string errorMessageClean;

            // See if this error is present in uniqueErrorMessages yet
            // If it is present, update the timestamp in uniqueErrorMessages
            // If not present, queue it

            if (match.Groups.Count >= 2)
            {
                var timestamp = match.Groups["Date"].Value;
                if (!DateTime.TryParse(timestamp, out timeStamp))
                    timeStamp = DateTime.MinValue;

                errorMessageClean = match.Groups["Error"].Value;
            }
            else
            {
                // Regex didn't match; this is unexpected
                timeStamp = DateTime.MinValue;
                errorMessageClean = errorMessage;
            }

            // Check whether errorMessageClean is in the hash table
            if (uniqueErrorMessages.TryGetValue(errorMessageClean, out var existingTimeStamp))
            {
                // The error message is present
                // Update the timestamp associated with errorMessageClean if the time stamp is newer than the stored one
                try
                {
                    if (timeStamp > existingTimeStamp)
                    {
                        uniqueErrorMessages[errorMessageClean] = timeStamp;
                    }
                }
                catch (Exception)
                {
                    // Date comparison failed; leave the existing timestamp unchanged
                }
            }
            else
            {
                // The error message is not present
                uniqueErrorMessages.Add(errorMessageClean, timeStamp);
            }

            if (qErrorMsgQueue.Contains(errorMessageClean))
                return;

            // Queue this message
            // However, if we already have errorMessageCountToReturn messages queued, dequeue the oldest one

            if (qErrorMsgQueue.Count < maxErrorMessageCountToReturn)
            {
                qErrorMsgQueue.Enqueue(errorMessageClean);
            }
            else
            {
                // Too many queued messages, so remove oldest one
                // However, only do this if the new error message has a timestamp newer than the oldest queued message
                //  (this is a consideration when processing multiple log files)

                var addItemToQueue = true;

                var queuedError = qErrorMsgQueue.Peek();

                // Get the timestamp associated with queuedError, as tracked by the hashtable
                if (!uniqueErrorMessages.TryGetValue(queuedError, out var queuedTimeStamp))
                {
                    // The error message is not in the hashtable; this is unexpected
                }
                else
                {
                    // Compare the queued error's timestamp with the timestamp of the new error message
                    try
                    {
                        if (queuedTimeStamp >= timeStamp)
                        {
                            // The queued error message's timestamp is equal to or newer than the new message's timestamp
                            // Do not add the new item to the queue
                            addItemToQueue = false;
                        }
                    }
                    catch (Exception)
                    {
                        // Date comparison failed; Do not add the new item to the queue
                        addItemToQueue = false;
                    }
                }

                if (addItemToQueue)
                {
                    qErrorMsgQueue.Dequeue();
                    qErrorMsgQueue.Enqueue(errorMessageClean);
                }
            }
        }

        /// <summary>
        /// Sets the local mgr_active flag to False for serious problems
        /// </summary>
        /// <remarks></remarks>
        private void DisableManagerLocally()
        {
            // Note: We previously called m_MgrSettings.DisableManagerLocally() to update AnalysisManager.config.exe
            // We now create a flag file instead
            // This gives the manager a chance to auto-cleanup things if ManagerErrorCleanupMode is >= 1

            m_MgrErrorCleanup.CreateStatusFlagFile();
            UpdateStatusFlagFileExists();
        }

        /// <summary>
        /// Enable offline mode
        /// </summary>
        /// <param name="runningLinux">Set to True if running Linux</param>
        /// <remarks>When offline, does not contact any databases or remote shares</remarks>
        public static void EnableOfflineMode(bool runningLinux = true)
        {
            clsGlobal.EnableOfflineMode(runningLinux);
        }

        private string GetBaseLogFileName()
        {
            return GetBaseLogFileName(m_MgrSettings);
        }

        /// <summary>
        /// Get the base log file name, as defined in the manager parameters
        /// </summary>
        /// <param name="mgrParams"></param>
        /// <returns></returns>
        public static string GetBaseLogFileName(IMgrParams mgrParams)
        {
            var logFileNameBase = mgrParams.GetParam("logfilename", DEFAULT_BASE_LOGFILE_NAME);
            return clsGlobal.LinuxOS ? logFileNameBase.Replace('\\', '/') : logFileNameBase;
        }

        private string GetRecentLogFilename()
        {
            try
            {
                // Obtain a list of log files
                var logFileNameBase = GetBaseLogFileName();
                var files = Directory.GetFiles(m_MgrDirectoryPath, logFileNameBase + "*.txt");

                if (files.Length == 0)
                    return string.Empty;

                var newestLogFile = (from item in files orderby item.ToLower() select item).Last();
                return newestLogFile;
            }
            catch (Exception)
            {
                return string.Empty;
            }

        }

        /// <summary>
        /// Extract the value DefaultDMSConnString from AnalysisManagerProg.exe.config
        /// </summary>
        /// <returns></returns>
        private string GetXmlConfigDefaultConnectionString()
        {
            return GetXmlConfigFileSetting("DefaultDMSConnString");
        }

        /// <summary>
        /// Extract the value for the given setting from AnalysisManagerProg.exe.config
        /// </summary>
        /// <returns>Setting value if found, otherwise an empty string</returns>
        /// <remarks>Uses a simple text reader in case the file has malformed XML</remarks>
        private string GetXmlConfigFileSetting(string settingName)
        {

            if (string.IsNullOrWhiteSpace(settingName))
                throw new ArgumentException("Setting name cannot be blank", nameof(settingName));

            try
            {
                var configFilePath = Path.Combine(m_MgrDirectoryPath, m_MgrExeName + ".config");
                var configfile = new FileInfo(configFilePath);

                if (!configfile.Exists)
                {
                    LogError("File not found: " + configFilePath);
                    return string.Empty;
                }

                var configXml = new StringBuilder();

                // Open AnalysisManagerProg.exe.config using a simple text reader in case the file has malformed XML

                ShowTrace(string.Format("Extracting setting {0} from {1}", settingName, configfile.FullName));

                using (var reader = new StreamReader(new FileStream(configfile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        configXml.Append(dataLine);
                    }
                }

                var matcher = new Regex(settingName + ".+?<value>(?<ConnString>.+?)</value>", RegexOptions.IgnoreCase);

                var match = matcher.Match(configXml.ToString());

                if (match.Success)
                    return match.Groups["ConnString"].Value;

                LogError(settingName + " setting not found in " + configFilePath);
                return string.Empty;
            }
            catch (Exception ex)
            {
                LogError("Exception reading setting " + settingName + " in AnalysisManagerProg.exe.config", ex);
                return string.Empty;
            }

        }

        private clsCleanupMgrErrors.eCleanupModeConstants GetManagerErrorCleanupMode()
        {
            clsCleanupMgrErrors.eCleanupModeConstants eManagerErrorCleanupMode;

            var managerErrorCleanupMode = m_MgrSettings.GetParam("ManagerErrorCleanupMode", "0");

            switch (managerErrorCleanupMode.Trim())
            {
                case "0":
                    eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.Disabled;
                    break;
                case "1":
                    eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.CleanupOnce;
                    break;
                case "2":
                    eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.CleanupAlways;
                    break;
                default:
                    eManagerErrorCleanupMode = clsCleanupMgrErrors.eCleanupModeConstants.Disabled;
                    break;
            }

            return eManagerErrorCleanupMode;
        }

        private bool HandleJobFailure(IToolRunner toolRunner, CloseOutType eToolRunnerResult)
        {

            ShowTrace("Tool run error; cleaning up");

            try
            {
                if (!m_AnalysisTask.TaskClosed)
                {
                    LogWarning("Upstream code typically calls .CloseTask before HandleJobFailure is reached; closing the task now");

                    m_AnalysisTask.CloseTask(eToolRunnerResult, m_MostRecentErrorMessage, toolRunner);
                }

                if (m_MgrErrorCleanup.CleanWorkDir())
                {
                    m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel);
                }
                else
                {
                    m_MgrErrorCleanup.CreateErrorDeletingFilesFlagFile();
                }

                if (eToolRunnerResult == CloseOutType.CLOSEOUT_NO_DTA_FILES && m_AnalysisTask.GetParam("StepTool").ToLower() == "sequest")
                {
                    // This was a Sequest job, but no .DTA files were found
                    // Return True; do not count this as a manager failure
                    return true;
                }

                if (eToolRunnerResult == CloseOutType.CLOSEOUT_NO_DATA)
                {
                    // Return True; do not count this as a manager failure
                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception in cleaning up after RunTool error", ex);
                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.HandleJobFailure(): " + ex.Message, m_MostRecentJobInfo, true);
                return false;
            }
        }

        private void HandleRemoteJobFailure(IToolRunner toolRunner, clsRemoteMonitor remoteMonitor, out CloseOutType eToolRunnerResult)
        {
            // Job failed
            // Parse the .fail file to read the result codes and messages (the file was already retrieved by GetRemoteJobStatus, if it existed)

            var jobResultFile = new FileInfo(Path.Combine(m_WorkDirPath, remoteMonitor.TransferUtility.ProcessingFailureFile));

            if (!jobResultFile.Exists)
            {
                m_MostRecentErrorMessage = ".fail file not found in the working directory: " + remoteMonitor.TransferUtility.ProcessingFailureFile;
                eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                return;
            }

            var statusParsed = remoteMonitor.ParseStatusResultFile(jobResultFile.FullName, out eToolRunnerResult, out var completionMessage);

            if (!statusParsed)
            {
                if (string.IsNullOrWhiteSpace(m_MostRecentErrorMessage))
                    m_MostRecentErrorMessage = "Status file parse error";

                eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                return;
            }

            m_MostRecentErrorMessage = completionMessage;

            if (string.IsNullOrWhiteSpace(m_MostRecentErrorMessage))
                m_MostRecentErrorMessage = "Remote job failed: " + eToolRunnerResult;

            LogError(clsGlobal.AppendToComment(m_MostRecentErrorMessage, remoteMonitor.Message));

            // Retrieve result files then store in the DMS_FailedResults folder

            toolRunner.RetrieveRemoteResults(remoteMonitor.TransferUtility, false, out var retrievedFilePaths);

            if (retrievedFilePaths.Count > 0)
            {
                toolRunner.CopyFailedResultsToArchiveFolder();
            }

            eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
        }

        private bool HandleRemoteJobSuccess(IToolRunner toolRunner, clsRemoteMonitor remoteMonitor, out CloseOutType eToolRunnerResult)
        {
            // Job succeeded
            // Parse the .success file to read the result codes and messages (the file was already retrieved by GetRemoteJobStatus()

            var jobResultFilePath = Path.Combine(m_WorkDirPath, remoteMonitor.TransferUtility.ProcessingSuccessFile);

            var statusParsed = remoteMonitor.ParseStatusResultFile(jobResultFilePath, out eToolRunnerResult, out var completionMessage);

            m_MostRecentErrorMessage = completionMessage;

            if (!statusParsed)
            {
                eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                return false;
            }

            // Retrieve result files then call PostProcess
            var resultsRetrieved = toolRunner.RetrieveRemoteResults(remoteMonitor.TransferUtility, true, out _);

            if (!resultsRetrieved)
            {
                eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                return false;
            }

            var postProcessResult = toolRunner.PostProcessRemoteResults();
            if (!clsAnalysisJob.SuccessOrNoData(postProcessResult))
            {
                eToolRunnerResult = postProcessResult;
            }

            if (!clsAnalysisJob.SuccessOrNoData(eToolRunnerResult))
            {
                toolRunner.CopyFailedResultsToArchiveFolder();
                return false;
            }

            // Skip the status files when transferring results
            foreach (var statusFile in remoteMonitor.TransferUtility.StatusFileNames)
            {
                m_AnalysisTask.AddResultFileToSkip(statusFile);
            }

            var success = toolRunner.CopyResultsToTransferDirectory();
            if (success)
                return true;

            eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
            return false;
        }

        /// <summary>
        /// Initialize the remote transfer utility
        /// Used by RunJobRemotely and when PushFilesToRemoteHost is true
        /// </summary>
        /// <returns></returns>
        private clsRemoteTransferUtility InitializeRemoteTransferUtility()
        {
            var transferUtility = new clsRemoteTransferUtility(m_MgrSettings, m_AnalysisTask);
            RegisterEvents(transferUtility);

            try
            {
                transferUtility.UpdateParameters(true);
                return transferUtility;
            }
            catch (Exception ex)
            {
                m_MostRecentErrorMessage = "Exception initializing the remote transfer utility: " + ex.Message;
                LogError(m_MostRecentErrorMessage, ex);
                return null;
            }
        }

        /// <summary>
        /// Initializes the status file writing tool
        /// </summary>
        /// <remarks></remarks>
        private void InitStatusTools()
        {
            if (m_StatusTools != null)
                return;

            var statusFileLoc = Path.Combine(m_MgrDirectoryPath, m_MgrSettings.GetParam("statusfilelocation", "Status.xml"));

            ShowTrace("Initialize m_StatusTools using " + statusFileLoc);

            m_StatusTools = new clsStatusFile(statusFileLoc, m_DebugLevel)
            {
                TaskStartTime = DateTime.UtcNow,
                Dataset = string.Empty,
                WorkDirPath = m_WorkDirPath,
                JobNumber = 0,
                JobStep = 0,
                Tool = string.Empty,
                MgrName = m_MgrName,
                MgrStatus = EnumMgrStatus.RUNNING,
                TaskStatus = EnumTaskStatus.NO_TASK,
                TaskStatusDetail = EnumTaskStatusDetail.NO_TASK
            };
            RegisterEvents(m_StatusTools);

            var runJobsRemotely = m_MgrSettings.GetParam("RunJobsRemotely", false);
            if (runJobsRemotely)
            {
                m_StatusTools.RemoteMgrName = m_MgrSettings.GetParam("RemoteHostName");
            }

            UpdateStatusToolLoggingSettings(m_StatusTools);
        }

        /// <summary>
        /// Read settings from file AnalysisManagerProg.exe.config
        /// </summary>
        /// <returns>String dictionary of settings as key/value pairs; null on error</returns>
        /// <remarks>Uses an XML reader instead of Properties.Settings.Default (see the comments in LoadMgrSettingsFromFile)</remarks>
        private Dictionary<string, string> ReadMgrSettingsFile(out string configFilePath)
        {

            XmlDocument configDoc;
            configFilePath = string.Empty;

            try
            {
                // Construct the path to the config document
                configFilePath = Path.Combine(m_MgrDirectoryPath, m_MgrExeName + ".config");
                var configfile = new FileInfo(configFilePath);
                if (!configfile.Exists)
                {
                    LogError("ReadMgrSettingsFile; manager config file not found: " + configFilePath);
                    return null;
                }

                // Load the config document
                configDoc = new XmlDocument();
                configDoc.Load(configFilePath);
            }
            catch (Exception ex)
            {
                LogError("ReadMgrSettingsFile; exception loading settings file", ex);
                return null;
            }

            try
            {
                // Retrieve the settings node
                var appSettingsNode = configDoc.SelectSingleNode("//applicationSettings");

                if (appSettingsNode == null)
                {
                    LogError("ReadMgrSettingsFile; applicationSettings node not found");
                    return null;
                }

                // Read each of the settings
                var settingNodes = appSettingsNode.SelectNodes("//setting[@name]");
                if (settingNodes == null)
                {
                    LogError("ReadMgrSettingsFile; applicationSettings/*/setting nodes not found");
                    return null;
                }

                return clsAnalysisMgrSettings.ParseXMLSettings(settingNodes, TraceMode);

            }
            catch (Exception ex)
            {
                LogError("ReadMgrSettingsFile; Exception reading settings file", ex);
                return null;
            }
        }

        /// <summary>
        /// Loads the initial settings from application config file AnalysisManagerProg.exe.config
        /// </summary>
        /// <returns>String dictionary containing initial settings if successful; null on error</returns>
        /// <remarks>This method is public because clsCodeTest uses it</remarks>
        public Dictionary<string, string> LoadMgrSettingsFromFile()
        {
            // Note: When you are editing this project using the Visual Studio IDE, if you edit the values
            //  ->My Project>Settings.settings, then when you run the program (from within the IDE), it
            //  will update file AnalysisManagerProg.exe.config with your settings
            // The manager will exit if the "UsingDefaults" value is "True", thus you need to have
            //  "UsingDefaults" be "False" to run (and/or debug) the application

            // We should be able to load settings auto-magically using "Properties.Settings.Default.MgrCnfgDbConnectStr" and "Properties.Settings.Default.MgrName"
            // But that mechanism only works if the AnalysisManagerProg.exe.config is of the form:
            //   <applicationSettings>
            //     <AnalysisManagerProg.Properties.Settings>
            //       <setting name="MgrActive_Local" serializeAs="String">

            // Older VB.NET based versions of the AnalysisManagerProg.exe.config file have:
            //   <applicationSettings>
            //     <My.MySettings>
            //       <setting name="MgrActive_Local" serializeAs="String">

            // Method ReadMgrSettingsFile() works with both versions of the .exe.config file

            // Load initial settings into string dictionary
            var lstMgrSettings = ReadMgrSettingsFile(out var configFilePath);

            if (lstMgrSettings == null)
                return null;

            // Manager Config DB connection string
            if (!lstMgrSettings.ContainsKey(clsAnalysisMgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING))
            {
                lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING, Properties.Settings.Default.MgrCnfgDbConnectStr);
            }

            // Manager active flag
            if (!lstMgrSettings.ContainsKey(clsAnalysisMgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL))
            {
                lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, "False");
            }

            // Manager name
            if (!lstMgrSettings.ContainsKey(clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME))
            {
                lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME, "LoadMgrSettingsFromFile__Undefined_manager_name");
            }

            // If the MgrName setting in the AnalysisManagerProg.exe.config file contains the text $ComputerName$
            // that text is replaced with this computer's domain name
            // This is a case-sensitive comparison
            var managerName = lstMgrSettings[clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME];
            var autoDefinedName = managerName.Replace("$ComputerName$", Environment.MachineName);

            if (!string.Equals(managerName, autoDefinedName))
            {
                ShowTrace("Auto-defining the manager name as " + autoDefinedName);
                lstMgrSettings[clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME] = autoDefinedName;
            }

            // Default settings in use flag
            if (!lstMgrSettings.ContainsKey(clsAnalysisMgrSettings.MGR_PARAM_USING_DEFAULTS))
            {
                lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_USING_DEFAULTS, Properties.Settings.Default.UsingDefaults.ToString());
            }

            // Default connection string for logging errors to the database
            // Will get updated later when manager settings are loaded from the manager control database
            if (!lstMgrSettings.ContainsKey(clsAnalysisMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING))
            {
                lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING, Properties.Settings.Default.DefaultDMSConnString);
            }

            if (TraceMode)
            {
                ShowTrace("Settings loaded from " + clsPathUtils.CompactPathString(configFilePath, 60));
                clsAnalysisMgrSettings.ShowDictionaryTrace(lstMgrSettings);
            }

            return lstMgrSettings;
        }

        private bool NeedToAbortProcessing()
        {
            if (m_NeedToAbortProcessing)
            {
                LogError("Analysis manager has encountered a fatal error - aborting processing (m_NeedToAbortProcessing is True)");
                return true;
            }

            if (m_StatusTools == null)
                return false;

            if (!m_StatusTools.AbortProcessingNow)
                return false;

            LogError("Found file " + clsStatusFile.ABORT_PROCESSING_NOW_FILENAME + " - aborting processing");
            return true;
        }

        private Dictionary<string, DateTime> LoadCachedLogMessages(FileSystemInfo messageCacheFile)
        {
            var cachedMessages = new Dictionary<string, DateTime>();

            char[] sepChars = { '\t' };

            using (var reader = new StreamReader(new FileStream(messageCacheFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                var lineCount = 0;
                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    lineCount += 1;

                    // Assume that the first line is the header line, which we'll skip
                    if (lineCount == 1 || string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    var lineParts = dataLine.Split(sepChars, 2);

                    var timeStampText = lineParts[0];
                    var message = lineParts[1];

                    if (DateTime.TryParse(timeStampText, out var timeStamp))
                    {
                        // Valid message; store it

                        if (cachedMessages.TryGetValue(message, out var cachedTimeStamp))
                        {
                            if (timeStamp > cachedTimeStamp)
                            {
                                cachedMessages[message] = timeStamp;
                            }
                        }
                        else
                        {
                            cachedMessages.Add(message, timeStamp);
                        }
                    }
                }
            }

            return cachedMessages;
        }

        private void LogErrorToDatabasePeriodically(string errorMessage, int logIntervalHours)
        {
            const string PERIODIC_LOG_FILE = "Periodic_ErrorMessages.txt";

            try
            {
                Dictionary<string, DateTime> cachedMessages;

                var messageCacheFile = new FileInfo(Path.Combine(clsGlobal.GetAppFolderPath(), PERIODIC_LOG_FILE));

                if (messageCacheFile.Exists)
                {
                    cachedMessages = LoadCachedLogMessages(messageCacheFile);
                }
                else
                {
                    cachedMessages = new Dictionary<string, DateTime>();
                }

                if (cachedMessages.TryGetValue(errorMessage, out var timeStamp))
                {
                    if (DateTime.UtcNow.Subtract(timeStamp).TotalHours < logIntervalHours)
                    {
                        // Do not log to the database
                        return;
                    }
                    cachedMessages[errorMessage] = DateTime.UtcNow;
                }
                else
                {
                    cachedMessages.Add(errorMessage, DateTime.UtcNow);
                }

                LogError(errorMessage, true);

                // Update the message cache file
                using (var writer = new StreamWriter(new FileStream(messageCacheFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    writer.WriteLine("{0}\t{1}", "TimeStamp", "Message");
                    foreach (var message in cachedMessages)
                    {
                        writer.WriteLine("{0}\t{1}", message.Value.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT), message.Key);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in LogErrorToDatabasePeriodically", ex);
            }
        }

        /// <summary>
        /// Reload the settings from AnalysisManagerProg.exe.config
        /// </summary>
        /// <returns>True if success, false if now disabled locally or if an error</returns>
        /// <remarks></remarks>
        private bool ReloadManagerSettings()
        {
            try
            {
                ShowTrace("Reading application config file");

                // Load settings from config file AnalysisManagerProg.exe.config
                var lstMgrSettings = LoadMgrSettingsFromFile();

                if (lstMgrSettings == null)
                    return false;

                ShowTrace("Storing manager settings in m_MgrSettings");

                // Store the new settings then retrieve updated settings from the database
                // or from ManagerSettingsLocal.xml if clsGlobal.OfflineMode is true
                if (m_MgrSettings.LoadSettings(lstMgrSettings))
                    return true;

                if (!string.IsNullOrWhiteSpace(m_MgrSettings.ErrMsg))
                {
                    // Manager has been deactivated, so report this
                    LogMessage(m_MgrSettings.ErrMsg);
                    UpdateStatusDisabled(EnumMgrStatus.DISABLED_LOCAL, "Disabled Locally");
                }
                else
                {
                    // Unknown problem reading config file
                    LogError("Error re-reading config file in ReloadManagerSettings");
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error re-loading manager settings", ex);
                return false;
            }

        }

        private void RemoveTempFiles()
        {
            var diMgrFolder = new DirectoryInfo(m_MgrDirectoryPath);

            // Files starting with the name IgnoreMe are created by log4NET when it is first instantiated
            // This name is defined in the RollingFileAppender section of the Logging.config file via this XML:
            // <file value="IgnoreMe" />

            foreach (var fiFile in diMgrFolder.GetFiles("IgnoreMe*.txt"))
            {
                try
                {
                    fiFile.Delete();
                }
                catch (Exception ex)
                {
                    LogError("Error deleting IgnoreMe file: " + fiFile.Name, ex);
                }
            }

            // Files named tmp.iso.#### and tmp.peak.#### (where #### are integers) are files created by Decon2LS
            // These files indicate a previous, failed Decon2LS task and can be safely deleted
            // For safety, we will not delete files less than 24 hours old

            var lstFilesToDelete = diMgrFolder.GetFiles("tmp.iso.*").ToList();

            lstFilesToDelete.AddRange(diMgrFolder.GetFiles("tmp.peak.*"));

            foreach (var fiFile in lstFilesToDelete)
            {
                try
                {
                    if (DateTime.UtcNow.Subtract(fiFile.LastWriteTimeUtc).TotalHours > 24)
                    {
                        ShowTrace("Deleting temp file " + fiFile.FullName);
                        fiFile.Delete();
                    }
                }
                catch (Exception)
                {
                    LogError("Error deleting file: " + fiFile.Name);
                }
            }
        }

        private void ResetPluginLoaderErrorCount(string stepToolName)
        {
            mPluginLoaderErrorCount = 0;
            mPluginLoaderStepTool = stepToolName;
        }

        private bool RetrieveResources(
            IAnalysisResources toolResourcer,
            int jobNum,
            string datasetName,
            out CloseOutType eToolRunnerResult)
        {
            eToolRunnerResult = CloseOutType.CLOSEOUT_SUCCESS;

            try
            {
                ShowTrace("Getting job resources");

                eToolRunnerResult = toolResourcer.GetResources();
                if (eToolRunnerResult == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return true;
                }

                m_MostRecentErrorMessage = "GetResources returned result: " + eToolRunnerResult;
                ShowTrace(m_MostRecentErrorMessage + "; closing job step task");

                LogError(m_MgrName + ": " + clsGlobal.AppendToComment(m_MostRecentErrorMessage, toolResourcer.Message) + ", Job " + jobNum + ", Dataset " + datasetName);
                m_AnalysisTask.CloseTask(eToolRunnerResult, toolResourcer.Message);

                m_MgrErrorCleanup.CleanWorkDir();
                UpdateStatusIdle("Error encountered: " + m_MostRecentErrorMessage);
                m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel);

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error getting resources", ex);

                m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Exception getting resources");

                if (m_MgrErrorCleanup.CleanWorkDir())
                {
                    m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel);
                }
                else
                {
                    m_MgrErrorCleanup.CreateErrorDeletingFilesFlagFile();
                }

                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.RetrieveResources(): " + ex.Message, m_MostRecentJobInfo, true);
                return false;
            }

        }

        private bool RunJobLocally(
            IToolRunner toolRunner,
            int jobNum,
            string datasetName,
            out CloseOutType eToolRunnerResult)
        {
            var success = true;

            try
            {

                ShowTrace("Running the step tool locally");

                eToolRunnerResult = toolRunner.RunTool();

                if (eToolRunnerResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    m_MostRecentErrorMessage = toolRunner.Message;

                    if (string.IsNullOrEmpty(m_MostRecentErrorMessage))
                    {
                        m_MostRecentErrorMessage = "Unknown ToolRunner Error";
                    }

                    ShowTrace("Error running the tool; closing job step task");

                    LogError(m_MgrName + ": " + m_MostRecentErrorMessage + ", Job " + jobNum + ", Dataset " + datasetName);
                    m_AnalysisTask.CloseTask(eToolRunnerResult, m_MostRecentErrorMessage, toolRunner);

                    try
                    {
                        if (m_MostRecentErrorMessage.Contains(DECON2LS_FATAL_REMOTING_ERROR) ||
                            m_MostRecentErrorMessage.Contains(DECON2LS_CORRUPTED_MEMORY_ERROR))
                        {
                            m_NeedToAbortProcessing = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError("Exception examining MostRecentErrorMessage", ex);
                    }

                    if (eToolRunnerResult == CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE)
                    {
                        m_NeedToAbortProcessing = true;
                    }

                    if (m_NeedToAbortProcessing && m_MostRecentErrorMessage.StartsWith(clsAnalysisToolRunnerBase.PVM_RESET_ERROR_MESSAGE))
                    {
                        DisableManagerLocally();
                    }

                    success = false;
                }

                if (toolRunner.NeedToAbortProcessing)
                {
                    m_NeedToAbortProcessing = true;
                    ShowTrace("toolRunner.NeedToAbortProcessing = True; closing job step task");
                    m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, m_MostRecentErrorMessage, toolRunner);
                }

                return success;
            }
            catch (Exception ex)
            {
                LogError("Exception running job " + jobNum, ex);

                if (ex.Message.Contains(DECON2LS_TCP_ALREADY_REGISTERED_ERROR))
                {
                    m_NeedToAbortProcessing = true;
                }

                eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED;
                m_AnalysisTask.CloseTask(eToolRunnerResult, "Exception running tool", toolRunner);

                return false;
            }

        }

        private bool RunJobRemotely(
            IAnalysisResources toolResourcer,
            int jobNum,
            int stepNum,
            out CloseOutType eToolRunnerResult)
        {
            try
            {

                ShowTrace("Instantiating clsRemoteTransferUtility");

                var transferUtility = InitializeRemoteTransferUtility();

                if (transferUtility == null)
                {
                    eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                var remoteTimestamp = transferUtility.UpdateRemoteTimestamp();

                ShowTrace("Pushing new/updated DMS_Programs files to remote host");

                try
                {
                    // We run the DMS Update Manager for the first job processed, but not for subsequent jobs
                    var successCopying = mDMSProgramsSynchronized || transferUtility.RunDMSUpdateManager();

                    if (!successCopying)
                    {
                        m_MostRecentErrorMessage = "Error copying manager-related files to the remote host";
                        LogError(m_MostRecentErrorMessage);

                        eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED;
                        return false;
                    }

                    mDMSProgramsSynchronized = true;
                }
                catch (Exception ex)
                {
                    m_MostRecentErrorMessage = "Exception copying manager-related files to the remote host: " + ex.Message;
                    LogError(m_MostRecentErrorMessage, ex);

                    eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                ShowTrace("Transferring job-related files to remote host to run remotely");

                try
                {
                    var successCopying = toolResourcer.CopyResourcesToRemote(transferUtility);

                    if (!successCopying)
                    {
                        m_MostRecentErrorMessage = "Error copying job-related files to the remote host";
                        LogError(m_MostRecentErrorMessage);

                        eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED;
                        return false;
                    }
                }
                catch (NotImplementedException ex)
                {
                    // Plugin XYZ must implement CopyResourcesToRemote to allow for remote processing"
                    m_MostRecentErrorMessage = ex.Message;

                    // Don't send ex to LogError; no need to log a stack trace
                    LogError(m_MostRecentErrorMessage);

                    eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }
                catch (Exception ex)
                {
                    m_MostRecentErrorMessage = "Exception copying job-related files to the remote host: " + ex.Message;
                    LogError(m_MostRecentErrorMessage, ex);

                    eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                ShowTrace("Creating the .info file in the remote task queue directory");

                // All files have been copied remotely
                // Create the .info file so remote managers can start processing
                var success = transferUtility.CreateJobTaskInfoFile(remoteTimestamp, out var infoFilePathRemote);

                if (!success)
                {
                    m_MostRecentErrorMessage = "Error creating the remote job task info file";
                    LogError(m_MostRecentErrorMessage);

                    eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                LogMessage(string.Format("Job {0}, step {1} staged to run remotely on {2}; remote info file at {3}",
                                         jobNum, stepNum, transferUtility.RemoteHostName, infoFilePathRemote));

                eToolRunnerResult = CloseOutType.CLOSEOUT_RUNNING_REMOTE;
                return true;

            }
            catch (Exception ex)
            {
                m_MostRecentErrorMessage = "Exception staging job to run remotely";
                LogError(m_MostRecentErrorMessage + ", job " + jobNum, ex);

                eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED;
                return false;
            }
        }

        private bool SetResourceObject(out IAnalysisResources toolResourcer)
        {
            var stepToolName = m_AnalysisTask.GetParam("StepTool");
            ResetPluginLoaderErrorCount(stepToolName);
            ShowTrace("Loading the resourcer for tool " + stepToolName);

            toolResourcer = m_PluginLoader.GetAnalysisResources(stepToolName.ToLower());

            if (toolResourcer == null && stepToolName.StartsWith("Test_", StringComparison.OrdinalIgnoreCase))
            {
                stepToolName = stepToolName.Substring("Test_".Length);
                ResetPluginLoaderErrorCount(stepToolName);
                ShowTrace("Loading the resourcer for tool " + stepToolName);

                toolResourcer = m_PluginLoader.GetAnalysisResources(stepToolName.ToLower());
            }

            if (toolResourcer == null)
            {
                return false;
            }

            if (m_DebugLevel > 0)
            {
                LogMessage("Loaded resourcer for StepTool " + stepToolName);
            }

            try
            {
                toolResourcer.Setup(stepToolName, m_MgrSettings, m_AnalysisTask, m_StatusTools, m_MyEMSLUtilities);
            }
            catch (Exception ex)
            {
                LogError("Unable to load resource object", ex);
                return false;
            }

            return true;
        }

        private bool SetToolRunnerObject(out IToolRunner toolRunner)
        {
            var stepToolName = m_AnalysisTask.GetParam("StepTool");
            ResetPluginLoaderErrorCount(stepToolName);
            ShowTrace("Loading the ToolRunner for tool " + stepToolName);

            toolRunner = m_PluginLoader.GetToolRunner(stepToolName.ToLower());

            if (toolRunner == null && stepToolName.StartsWith("Test_", StringComparison.OrdinalIgnoreCase))
            {
                stepToolName = stepToolName.Substring("Test_".Length);
                ResetPluginLoaderErrorCount(stepToolName);
                ShowTrace("Loading the ToolRunner for tool " + stepToolName);

                toolRunner = m_PluginLoader.GetToolRunner(stepToolName.ToLower());
            }

            if (toolRunner == null)
            {
                return false;
            }

            if (m_DebugLevel > 0)
            {
                LogMessage("Loaded tool runner for StepTool " + m_AnalysisTask.GetCurrentJobToolDescription());
            }

            try
            {
                // Setup the new tool runner
                toolRunner.Setup(stepToolName, m_MgrSettings, m_AnalysisTask, m_StatusTools, m_SummaryFile, m_MyEMSLUtilities);
            }
            catch (Exception ex)
            {
                LogError("Exception calling toolRunner.Setup()", ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Show a trace message only if TraceMode is true
        /// </summary>
        /// <param name="message"></param>
        private void ShowTrace(string message)
        {
            if (!TraceMode)
                return;

            ShowTraceMessage(message);
        }

        /// <summary>
        /// Show a message at the console, preceded by a time stamp
        /// </summary>
        /// <param name="message"></param>
        public static void ShowTraceMessage(string message)
        {
            ConsoleMsgUtils.ShowDebug(DateTime.Now.ToString("hh:mm:ss.fff tt") + ": " + message);
        }

        /// <summary>
        /// Look for flagFile.txt in the .exe folder
        /// Auto clean errors if AutoCleanupManagerErrors is enabled
        /// </summary>
        /// <returns>True if a flag file exists, false if safe to proceed</returns>
        private bool StatusFlagFileError()
        {
            bool mgrCleanupSuccess;

            if (!m_MgrErrorCleanup.DetectStatusFlagFile())
            {
                // No error; return false
                return false;
            }

            try
            {
                mgrCleanupSuccess = m_MgrErrorCleanup.AutoCleanupManagerErrors(GetManagerErrorCleanupMode(), m_DebugLevel);
            }
            catch (Exception ex)
            {
                LogError("Error calling AutoCleanupManagerErrors", ex);
                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.StatusFlagFileError(): " + ex.Message, m_MostRecentJobInfo, true);

                mgrCleanupSuccess = false;
            }

            if (mgrCleanupSuccess)
            {
                LogWarning("Flag file found; automatically cleaned the work directory and deleted the flag file(s)");

                // No error; return false
                return false;
            }

            // Error removing flag file (or manager not set to auto-remove flag files)

            // Periodically log errors to the database
            var flagFile = new FileInfo(m_MgrErrorCleanup.FlagFilePath);
            string errorMessage;
            if (flagFile.Directory == null)
            {
                errorMessage = "Flag file exists in the manager folder";
            }
            else
            {
                errorMessage = "Flag file exists in folder " + flagFile.Directory.Name;
            }

            // Post a log entry to the database every 4 hours
            LogErrorToDatabasePeriodically(errorMessage, 4);

            // Return true (indicating a flag file exists)
            return true;
        }

        private void UpdateClose(string ManagerCloseMessage)
        {
            var recentErrorMessages = DetermineRecentErrorMessages(5, ref m_MostRecentJobInfo);

            m_StatusTools.UpdateClose(ManagerCloseMessage, recentErrorMessages, m_MostRecentJobInfo, true);
        }

        /// <summary>
        /// Reloads the manager settings from the manager control database
        /// if at least MinutesBetweenUpdates minutes have elapsed since the last update
        /// </summary>
        /// <param name="dtLastConfigDBUpdate"></param>
        /// <param name="minutesBetweenUpdates"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool UpdateManagerSettings(ref DateTime dtLastConfigDBUpdate, double minutesBetweenUpdates)
        {

            if (!(DateTime.UtcNow.Subtract(dtLastConfigDBUpdate).TotalMinutes >= minutesBetweenUpdates))
                return true;

            dtLastConfigDBUpdate = DateTime.UtcNow;

            ShowTrace("Loading manager settings from the manager control DB");

            if (!m_MgrSettings.LoadDBSettings())
            {
                string msg;

                if (string.IsNullOrWhiteSpace(m_MgrSettings.ErrMsg))
                {
                    msg = "Error calling m_MgrSettings.LoadMgrSettingsFromDB to update manager settings";
                }
                else
                {
                    msg = m_MgrSettings.ErrMsg;
                }

                LogError(msg);

                return false;
            }

            // Need to synchronize some of the settings
            UpdateStatusToolLoggingSettings(m_StatusTools);

            return true;
        }

        private void UpdateStatusDisabled(EnumMgrStatus managerStatus, string managerDisableMessage)
        {
            var recentErrorMessages = DetermineRecentErrorMessages(5, ref m_MostRecentJobInfo);
            m_StatusTools.UpdateDisabled(managerStatus, managerDisableMessage, recentErrorMessages, m_MostRecentJobInfo);
            Console.WriteLine(managerDisableMessage);
        }

        private void UpdateStatusFlagFileExists()
        {
            var recentErrorMessages = DetermineRecentErrorMessages(5, ref m_MostRecentJobInfo);
            m_StatusTools.UpdateFlagFileExists(recentErrorMessages, m_MostRecentJobInfo);
            Console.WriteLine("Flag file exists");
        }

        private void UpdateStatusIdle(string managerIdleMessage)
        {
            ShowTrace("Manager is idle");
            var recentErrorMessages = DetermineRecentErrorMessages(5, ref m_MostRecentJobInfo);
            m_StatusTools.UpdateIdle(managerIdleMessage, recentErrorMessages, m_MostRecentJobInfo, true);
        }

        private void UpdateStatusToolLoggingSettings(clsStatusFile objStatusFile)
        {
            var logMemoryUsage = m_MgrSettings.GetParam("LogMemoryUsage", false);
            float minimumMemoryUsageLogInterval = m_MgrSettings.GetParam("MinimumMemoryUsageLogInterval", 1);

            // Most managers have logStatusToBrokerDb=False and logStatusToMessageQueue=True
            var logStatusToBrokerDb = m_MgrSettings.GetParam("LogStatusToBrokerDB", false);
            var brokerDbConnectionString = m_MgrSettings.GetParam("brokerconnectionstring");

            // Gigasax.DMS_Pipeline
            float brokerDbStatusUpdateIntervalMinutes = m_MgrSettings.GetParam("BrokerDBStatusUpdateIntervalMinutes", 60);

            var logStatusToMessageQueue = m_MgrSettings.GetParam("LogStatusToMessageQueue", false);
            if (DisableMessageQueue)
            {
                // Command line has switch /NQ
                // Disable message queue logging
                logStatusToMessageQueue = false;
            }

            var messageQueueUri = m_MgrSettings.GetParam("MessageQueueURI");
            var messageQueueTopicMgrStatus = m_MgrSettings.GetParam("MessageQueueTopicMgrStatus");

            objStatusFile.ConfigureMemoryLogging(logMemoryUsage, minimumMemoryUsageLogInterval, m_MgrDirectoryPath);
            objStatusFile.ConfigureBrokerDBLogging(logStatusToBrokerDb, brokerDbConnectionString, brokerDbStatusUpdateIntervalMinutes);
            objStatusFile.ConfigureMessageQueueLogging(logStatusToMessageQueue, messageQueueUri, messageQueueTopicMgrStatus);
        }

        /// <summary>
        /// Confirms that the drive with the working directory has sufficient free space
        /// Confirms that the remote share for storing results is accessible and has sufficient free space
        /// </summary>
        /// <param name="toolResourcer"></param>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        /// <remarks>Disables the manager if the working directory drive does not have enough space</remarks>
        private bool ValidateFreeDiskSpace(IAnalysisResources toolResourcer, out string errorMessage)
        {
            const int DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB = 10;
            const int DEFAULT_TRANSFER_DIR_MIN_FREE_SPACE_GB = 10;

            const int DEFAULT_WORKING_DIR_MIN_FREE_SPACE_MB = 750;
            const int DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB = 750;

            errorMessage = string.Empty;

            try
            {
                var stepToolNameLCase = m_AnalysisTask.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "StepTool").ToLower();

                if (stepToolNameLCase == "results_transfer")
                {
                    // We only need to evaluate the dataset storage folder for free space

                    var datasetStoragePath = m_AnalysisTask.GetParam("DatasetStoragePath");
                    var datasetStorageMinFreeSpaceGB = m_MgrSettings.GetParam("DatasetStorageMinFreeSpaceGB", DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB);

                    if (string.IsNullOrEmpty(datasetStoragePath))
                    {
                        errorMessage = "DatasetStoragePath job parameter is empty";
                        LogError(errorMessage);
                        return false;
                    }

                    var diDatasetStoragePath = new DirectoryInfo(datasetStoragePath);
                    if (!diDatasetStoragePath.Exists)
                    {
                        // Dataset folder not found; that's OK, since the Results Transfer plugin will auto-create it
                        // Try to use the parent folder (or the parent of the parent)
                        while (!diDatasetStoragePath.Exists && diDatasetStoragePath.Parent != null)
                        {
                            diDatasetStoragePath = diDatasetStoragePath.Parent;
                        }

                        datasetStoragePath = diDatasetStoragePath.FullName;
                    }

                    if (!ValidateFreeDiskSpaceWork("Dataset directory", datasetStoragePath, datasetStorageMinFreeSpaceGB * 1024, out errorMessage))
                    {
                        return false;
                    }

                    return true;
                }

                var workingDirMinFreeSpaceMB = m_MgrSettings.GetParam("WorkDirMinFreeSpaceMB", DEFAULT_WORKING_DIR_MIN_FREE_SPACE_MB);

                var transferDir = m_AnalysisTask.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH);
                var transferDirMinFreeSpaceGB = m_MgrSettings.GetParam("TransferDirMinFreeSpaceGB", DEFAULT_TRANSFER_DIR_MIN_FREE_SPACE_GB);

                var orgDbDir = m_MgrSettings.GetParam("orgdbdir");
                var orgDbDirMinFreeSpaceMB = m_MgrSettings.GetParam("OrgDBDirMinFreeSpaceMB", DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB);

                ShowTrace("Validating free space for the working directory: " + m_WorkDirPath);

                // Verify that the working directory exists and that its drive has sufficient free space
                if (!ValidateFreeDiskSpaceWork("Working directory", m_WorkDirPath, workingDirMinFreeSpaceMB, out errorMessage, true))
                {
                    LogError("Disabling manager since working directory problem");
                    DisableManagerLocally();
                    return false;
                }

                if (!clsGlobal.OfflineMode)
                {
                    if (string.IsNullOrEmpty(transferDir))
                    {
                        errorMessage = "Transfer directory for the job is empty; cannot continue";

                        if (DataPackageIdMissing())
                        {
                            errorMessage += ". Data package ID cannot be 0 for this job type";
                        }

                        LogError(errorMessage);
                        return false;
                    }

                    ShowTrace("Validating free space for the transfer directory: " + transferDir);

                    // Verify that the remote transfer directory exists and that its drive has sufficient free space
                    if (!ValidateFreeDiskSpaceWork("Transfer directory", transferDir, transferDirMinFreeSpaceGB * 1024, out errorMessage))
                    {
                        return false;
                    }
                }

                var orgDbRequired = toolResourcer.GetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired);

                if (orgDbRequired)
                {
                    // Verify that the local fasta file cache directory has sufficient free space

                    ShowTrace("Validating free space for the Org DB directory: " + orgDbDir);

                    if (!ValidateFreeDiskSpaceWork("Organism DB directory", orgDbDir, orgDbDirMinFreeSpaceMB, out errorMessage))
                    {
                        DisableManagerLocally();
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception validating free space", ex);
                return false;
            }

            return true;
        }

        private bool ValidateFreeDiskSpaceWork(
            string directoryDescription,
            string directoryPath,
            int minFreeSpaceMB,
            out string errorMessage,
            bool logToDatabase = false)
        {
            return clsGlobal.ValidateFreeDiskSpace(directoryDescription, directoryPath, minFreeSpaceMB, out errorMessage, logToDatabase);
        }

        private bool VerifyWorkDir()
        {
            // Verify working directory is valid
            if (!Directory.Exists(m_WorkDirPath))
            {
                LogError("Invalid working directory: " + m_WorkDirPath);
                clsGlobal.IdleLoop(1.5);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Verifies working directory is properly specified and is empty
        /// </summary>
        /// <returns></returns>
        private bool ValidateWorkingDir()
        {
            // Verify working directory is valid
            if (!VerifyWorkDir())
            {
                return false;
            }

            // Verify the working directory is empty
            var workDir = new DirectoryInfo(m_WorkDirPath);
            var workDirFiles = workDir.GetFiles();
            var workDirFolders = workDir.GetDirectories();

            if (workDirFolders.Length == 0 && workDirFiles.Length == 1)
            {
                // If the only file in the working directory is a JobParameters xml file, try to delete it.
                // It is likely left over from a previous job that never actually started
                var firstFile = workDirFiles.First();

                if (firstFile.Name.StartsWith(clsGlobal.JOB_PARAMETERS_FILE_PREFIX, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(Path.GetExtension(firstFile.Name), ".xml", StringComparison.OrdinalIgnoreCase))
                {
                    try
                    {
                        LogWarning("Working directory contains a stray JobParameters file, deleting it: " + firstFile.FullName);

                        firstFile.Delete();

                        // Now obtain a new listing of files
                        workDir.Refresh();

                        if (workDir.GetFiles().Length == 0)
                        {
                            // The directory is now empty
                            return true;
                        }

                    }
                    catch (Exception ex)
                    {
                        // Deletion failed
                        LogError("Error deleting files in the working directory: " + ex.Message);
                    }
                }
            }

            var errorCount = workDirFiles.Count(item => !clsFileTools.IsVimSwapFile(item.FullName));

            if (errorCount == 0)
            {
                // No problems found
                return true;
            }

            LogError("Working directory not empty: " + m_WorkDirPath);
            return false;
        }

        /// <summary>
        /// Check whether the computer is likely to install the monthly Windows Updates within the next few hours
        /// </summary>
        /// <returns>True if Windows updates are pending</returns>
        private bool WindowsUpdatesArePending()
        {
            if (!clsWindowsUpdateStatus.UpdatesArePending(out var pendingWindowsUpdateMessage))
                return false;

            LogMessage(pendingWindowsUpdateMessage);
            UpdateStatusIdle(pendingWindowsUpdateMessage);
            return true;
        }

        #endregion

        #region "clsEventNotifier events"

        private void RegisterEvents(clsEventNotifier oProcessingClass, bool writeDebugEventsToLog = true)
        {
            if (writeDebugEventsToLog)
            {
                oProcessingClass.DebugEvent += DebugEventHandler;
            }
            else
            {
                oProcessingClass.DebugEvent += DebugEventHandlerConsoleOnly;
            }

            oProcessingClass.StatusEvent += StatusEventHandler;
            oProcessingClass.ErrorEvent += ErrorEventHandler;
            oProcessingClass.WarningEvent += WarningEventHandler;
            oProcessingClass.ProgressUpdate += ProgressUpdateHandler;
        }

        private void DebugEventHandlerConsoleOnly(string statusMessage)
        {
            LogTools.LogDebug(statusMessage, writeToLog: false);
        }

        private void DebugEventHandler(string statusMessage)
        {
            LogDebug(statusMessage);
        }

        private void StatusEventHandler(string statusMessage)
        {
            LogMessage(statusMessage);
        }

        private void ErrorEventHandler(string errorMessage, Exception ex)
        {
            LogError(errorMessage, ex);
        }

        private void WarningEventHandler(string warningMessage)
        {
            LogWarning(warningMessage);
        }

        /// <summary>
        /// Logs the first error while loading a plugin as an error
        /// Subsequent errors are logged as warnings
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <param name="ex"></param>
        private void PluginLoader_ErrorEventHandler(string errorMessage, Exception ex)
        {
            mPluginLoaderErrorCount++;
            if (mPluginLoaderErrorCount == 1)
            {
                LogError(mPluginLoaderStepTool + " load error: " + errorMessage, ex);
            }
            else
            {
                string formattedError;
                if (ex == null || errorMessage.EndsWith(ex.Message, StringComparison.OrdinalIgnoreCase))
                {
                    formattedError = errorMessage;
                }
                else
                {
                    if (errorMessage.Contains(ex.Message))
                        formattedError = errorMessage;
                    else
                        formattedError = errorMessage + ": " + ex.Message;
                }

                LogWarning(formattedError);
            }

        }

        private void ProgressUpdateHandler(string progressMessage, float percentComplete)
        {
            m_StatusTools.CurrentOperation = progressMessage;
            m_StatusTools.UpdateAndWrite(percentComplete);
        }

        #endregion

        #region "RemoteMonitor events"

        private void RemoteMonitor_StaleLockFileEvent(string fileName, int ageHours)
        {
            var msg = string.Format("Stale remote lock file for {0}; {1} last modified {2} hours ago",
                                    m_AnalysisTask.GetJobStepDescription(), fileName, ageHours);

            LogErrorToDatabasePeriodically(msg, 12);
        }

        private void RemoteMonitor_StaleJobStatusFileEvent(string fileName, int ageHours)
        {
            var msg = string.Format("Stale remote status file for {0}; {1} last modified {2} hours ago",
                                    m_AnalysisTask.GetJobStepDescription(), fileName, ageHours);

            LogErrorToDatabasePeriodically(msg, 12);
        }

        #endregion

        /// <summary>
        /// Event handler for file watcher
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        /// <remarks></remarks>
        private void m_ConfigFileWatcher_Changed(object sender, FileSystemEventArgs e)
        {
            m_ConfigFileWatcher.EnableRaisingEvents = false;

            if (m_LocalSettingsFileWatcher != null)
                m_LocalSettingsFileWatcher.EnableRaisingEvents = false;

            m_ConfigChanged = true;

            if (m_DebugLevel > 3)
            {
                LogDebug("Config file changed");
            }
        }

    }
}
