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
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.OfflineJobs;
using AnalysisManagerBase.StatusReporting;
using PRISM;
using PRISM.AppSettings;
using PRISM.Logging;
using PRISMDatabaseUtils;
using PRISMDatabaseUtils.Logging;

namespace AnalysisManagerProg
{
    /// <summary>
    /// Master processing class for analysis manager
    /// </summary>
    public class MainProcess : LoggerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: centroided, dir, dms, Fractestrecheck, Lewy, Lp, Mage, MM-dd-yyyy,
        // Ignore Spelling: Prog, proteinseqs, remoting, resourcer, sid, smeagol, svc, tcp

        // ReSharper restore CommentTypo

        private const int MAX_ERROR_COUNT = 10;
        private const string DECON2LS_FATAL_REMOTING_ERROR = "Fatal remoting error";
        private const string DECON2LS_CORRUPTED_MEMORY_ERROR = "Corrupted memory error";
        private const string DECON2LS_TCP_ALREADY_REGISTERED_ERROR = "channel 'tcp' is already registered";

        private const string DEFAULT_BASE_LOGFILE_NAME = @"Logs\AnalysisMgr";

        private const bool ENABLE_LOGGER_TRACE_MODE = false;

        // Instance of class AnalysisMgrSettings
        private IMgrParams mMgrParams;

        private CleanupMgrErrors mMgrErrorCleanup;
        private readonly string mMgrExeName;
        private readonly string mMgrDirectoryPath;
        private string mWorkDirPath;

        private string mMgrName = "??";

        private AnalysisJob mAnalysisTask;

        private FileSystemWatcher mConfigFileWatcher;

        private FileSystemWatcher mLocalSettingsFileWatcher;

        private bool mConfigChanged;

        private readonly DirectorySpaceTools mDirectorySpaceTools;

        private bool mDMSProgramsSynchronized;

        private bool mInsufficientFreeMemory;

        private StatusFile mStatusTools;

        private MyEMSLUtilities mMyEMSLUtilities;

        private bool mNeedToAbortProcessing;

        private string mMostRecentJobInfo;

        private string mMostRecentErrorMessage = string.Empty;

        private PluginLoader mPluginLoader;

        private int mPluginLoaderErrorCount;

        private string mPluginLoaderStepTool = string.Empty;

        private SummaryFile mSummaryFile;

        /// <summary>
        /// When true, do not log messages to the manager status message queue
        /// </summary>
        public bool DisableMessageQueue { get; set; }

        /// <summary>
        /// When true, do not contact MyEMSL
        /// </summary>
        public bool DisableMyEMSL { get; set; }

        private CommandLineOptions Options { get; }

        /// <summary>
        /// When true, only push analysis manager files to the remote host using the DMSUpdateManager
        /// Do not request a new analysis job
        /// </summary>
        /// <remarks>Only valid if the manager has parameter RunJobsRemotely set to true in the Manager Control DB</remarks>
        public bool PushRemoteMgrFilesOnly { get; set; }

        /// <summary>
        /// When true, show additional messages at the console
        /// </summary>
        public bool TraceMode { get; set; }

        /// <summary>
        /// Starts program execution
        /// </summary>
        /// <returns>0 if no error; error code if an error</returns>
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
                    mAnalysisTask.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Job", "100");
                    mAnalysisTask.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Step", "1");
                    mAnalysisTask.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "StepTool", "Sync");
                    mAnalysisTask.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_DATASET_NAME, "Placeholder");

                    var transferUtility = InitializeRemoteTransferUtility();

                    if (transferUtility == null)
                    {
                        return -1;
                    }

                    ShowTrace("Pushing new/updated DMS_Programs files to remote host");

                    CheckStopTrace("RunDMSUpdateManager");
                    transferUtility.RunDMSUpdateManager();
                    return 0;
                }

                DoAnalysis();

                ShowTrace("Exiting MainProcess.Main with error code = 0");
                return 0;
            }
            catch (Exception ex)
            {
                // Report any exceptions not handled at a lower level to the console
                var errMsg = "Critical exception starting application: " + ex.Message;
                ShowTrace(errMsg + "; " + Global.GetExceptionStackTrace(ex, true));
                ShowTrace("Exiting MainProcess.Main with error code = 1");
                return 1;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public MainProcess(CommandLineOptions options)
        {
            Options = options;
            Global.TraceMode = options.TraceMode;
            Global.TraceStopPoint = options.TraceStopPoint;

            TraceMode = options.TraceMode;

            mConfigChanged = false;
            mDirectorySpaceTools = new DirectorySpaceTools(true);
            mDebugLevel = 0;
            mDMSProgramsSynchronized = false;
            mInsufficientFreeMemory = false;
            mNeedToAbortProcessing = false;
            mMostRecentJobInfo = string.Empty;

            var exeInfo = new FileInfo(AppUtils.GetAppPath());
            mMgrExeName = exeInfo.Name;
            mMgrDirectoryPath = exeInfo.DirectoryName;
        }

        private void CheckStopTrace(string currentTraceLocation)
        {
            Global.CheckStopTrace(Options.TraceStopPoint, currentTraceLocation, Options.TraceMode);
        }

        /// <summary>
        /// Initializes the database logger in static class PRISM.Logging.LogTools
        /// </summary>
        /// <remarks>Supports both SQL Server and Postgres connection strings</remarks>
        /// <param name="connectionString">Database connection string</param>
        /// <param name="moduleName">Module name used by logger</param>
        /// <param name="traceMode">When true, show additional debug messages at the console</param>
        /// <param name="logLevel">Log threshold level</param>
        internal static void CreateDbLogger(
            string connectionString,
            string moduleName,
            bool traceMode = false,
            BaseLogger.LogLevels logLevel = BaseLogger.LogLevels.INFO)
        {
            var databaseType = DbToolsFactory.GetServerTypeFromConnectionString(connectionString);

            DatabaseLogger dbLogger = databaseType switch
            {
                DbServerTypes.MSSQLServer => new PRISMDatabaseUtils.Logging.SQLServerDatabaseLogger(),
                DbServerTypes.PostgreSQL => new PostgresDatabaseLogger(),
                _ => throw new Exception("Unsupported database connection string: should be SQL Server or Postgres")
            };

            dbLogger.ChangeConnectionInfo(moduleName, connectionString);

            LogTools.SetDbLogger(dbLogger, logLevel, traceMode);
        }

        /// <summary>
        /// Initializes the manager settings
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        private bool InitMgr()
        {
            var hostName = System.Net.Dns.GetHostName();
            var applicationName = "AnalysisManager_" + hostName;

            CheckStopTrace("CreateDefaultFileLogger");

            // Define the default logging info
            // This will get updated below
            var baseLogFileName = Global.LinuxOS ? DEFAULT_BASE_LOGFILE_NAME.Replace('\\', '/') : DEFAULT_BASE_LOGFILE_NAME;
            LogTools.CreateFileLogger(baseLogFileName, BaseLogger.LogLevels.DEBUG);

            // Give the file logger a chance to zip old log files by year
            FileLogger.ArchiveOldLogFilesNow();

            if (!Global.OfflineMode)
            {
                // Create a database logger connected to the DMS database on prismdb2 (previously, DMS5 on Gigasax)

                // Once the initial parameters have been successfully read,
                // we update the dbLogger to use the connection string read from the Manager Control DB
                string defaultDmsConnectionString;

                // Open AnalysisManagerProg.exe.config to look for setting DefaultDMSConnString, so we know which server to log to by default
                var dmsConnectionStringFromConfig = GetXmlConfigDefaultConnectionString();

                if (string.IsNullOrWhiteSpace(dmsConnectionStringFromConfig))
                {
                    // Use the hard-coded default that points to PrismDB2 (previously Gigasax)
                    defaultDmsConnectionString = Properties.Settings.Default.DefaultDMSConnString;
                }
                else
                {
                    // Use the connection string from AnalysisManagerProg.exe.config
                    defaultDmsConnectionString = dmsConnectionStringFromConfig;
                }

                CheckStopTrace("CreateDefaultDbLogger");

                var defaultDbLoggerConnectionString = DbToolsFactory.AddApplicationNameToConnectionString(defaultDmsConnectionString, applicationName);

                ShowTrace("Instantiate a DbLogger using " + defaultDbLoggerConnectionString);

                CreateDbLogger(defaultDbLoggerConnectionString, "Analysis Tool Manager: " + hostName, TraceMode && ENABLE_LOGGER_TRACE_MODE);
            }

            // Get the manager settings from the database or from ManagerSettingsLocal.xml if Global.OfflineMode is true
            // If you get an exception here while debugging in Visual Studio, be sure
            // that "UsingDefaults" is set to false in AppName.exe.config
            try
            {
                ShowTrace("Reading application config file");

                try
                {
                    CheckStopTrace("InitMgrSettings");

                    InitMgrSettings(true);

                    CheckStopTrace("LoadMgrSettingsFromFile");

                    // Load settings from config file AnalysisManagerProg.exe.config
                    var configFileSettings = LoadMgrSettingsFromFile();

                    var settingsClass = (AnalysisMgrSettings)mMgrParams;

                    if (settingsClass != null)
                    {
                        RegisterEvents(settingsClass);
                        settingsClass.CriticalErrorEvent += CriticalErrorEvent;
                    }

                    Console.WriteLine();
                    mMgrParams.ValidatePgPass(configFileSettings);

                    CheckStopTrace("LoadSettings");

                    var success = mMgrParams.LoadSettings(configFileSettings);

                    if (!success)
                    {
                        if (!string.IsNullOrEmpty(mMgrParams.ErrMsg))
                        {
                            throw new ApplicationException("Unable to initialize manager settings class: " + mMgrParams.ErrMsg);
                        }

                        throw new ApplicationException("Unable to initialize manager settings class: unknown error");
                    }

                    if (TraceMode)
                    {
                        ShowTraceMessage("Initialized MgrParams");
                    }
                }
                catch (Exception ex)
                {
                    ConsoleMsgUtils.ShowError("Error instantiating AnalysisMgrSettings", ex);
                    Global.IdleLoop(0.5);
                    return false;
                }
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Exception loading settings from AnalysisManagerProg.exe.config", ex);
                Global.IdleLoop(0.5);
                return false;
            }

            mMgrName = mMgrParams.ManagerName;
            ShowTrace("Manager name is " + mMgrName);

            // Delete any temporary files that may be left in the manager directory
            RemoveTempFiles();

            // Configure the loggers

            var logFileNameBase = GetBaseLogFileName();

            CheckStopTrace("CreateFileLogger");

            // The analysis manager determines when to log or not log based on internal logic
            // Set the LogLevel tracked by FileLogger to DEBUG so that all messages sent to the class are logged
            LogTools.CreateFileLogger(logFileNameBase, BaseLogger.LogLevels.DEBUG);

            if (!Global.OfflineMode)
            {
                // This connection string points to the DMS database on prismdb2 (previously, DMS5 on Gigasax)
                var logCnStr = mMgrParams.GetParam("ConnectionString");

                var dbLoggerConnectionString = DbToolsFactory.AddApplicationNameToConnectionString(logCnStr, applicationName);

                CheckStopTrace("CreateDbLogger");

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                CreateDbLogger(dbLoggerConnectionString, "Analysis Tool Manager: " + mMgrName, TraceMode && ENABLE_LOGGER_TRACE_MODE);
            }

            // Make the initial log entry
            var relativeLogFilePath = LogTools.CurrentLogFilePath;
            var logFile = new FileInfo(relativeLogFilePath);
            ShowTrace("Initializing log file " + PathUtils.CompactPathString(logFile.FullName, 60));

            CheckStopTrace("LogStartup");

            var appVersion = Assembly.GetEntryAssembly()?.GetName().Version;
            var startupMsg = "=== Started Analysis Manager V" + appVersion + " ===== ";
            LogMessage(startupMsg);

            var configFileName = mMgrParams.GetParam("ConfigFileName");

            if (string.IsNullOrEmpty(configFileName))
            {
                // Manager parameter error; log an error and exit
                LogError("Manager parameter 'ConfigFileName' is undefined; this likely indicates a problem retrieving manager parameters.  Shutting down the manager");
                return false;
            }

            // Set up a file watcher for the config file(s)
            mConfigFileWatcher = CreateConfigFileWatcher(configFileName);
            mConfigFileWatcher.Changed += ConfigFileWatcher_Changed;

            if (Global.OfflineMode)
            {
                CheckStopTrace("CreateConfigFileWatcher");

                mLocalSettingsFileWatcher = CreateConfigFileWatcher(AnalysisMgrSettings.LOCAL_MANAGER_SETTINGS_FILE);
                mLocalSettingsFileWatcher.Changed += ConfigFileWatcher_Changed;
            }

            if (Global.LinuxOS)
            {
                // Make sure System.Data.SQLite.dll is correct for this OS
                // Do this prior to initializing mAnalysisTask via new AnalysisJob
                ValidateSQLiteDLL();
            }

            // Get the debug level
            mDebugLevel = (short)mMgrParams.GetParam("DebugLevel", 2);

            // Make sure that the manager name matches the machine name (with a few exceptions)
            if (!hostName.StartsWith("EMSLMQ", StringComparison.OrdinalIgnoreCase) &&
                !hostName.StartsWith("EMSLPub", StringComparison.OrdinalIgnoreCase) &&
                !Global.RunningOnDeveloperComputer())
            {
                if (!mMgrName.StartsWith(hostName, StringComparison.OrdinalIgnoreCase))
                {
                    LogError("Manager name does not match the host name: " + mMgrName + " vs. " + hostName + "; update " + configFileName);
                    return false;
                }
            }

            CheckStopTrace("CreateAnalysisTask");

            // Set up the tool for getting tasks
            ShowTrace("Instantiate mAnalysisTask as new AnalysisJob");
            mAnalysisTask = new AnalysisJob(mMgrParams, mDebugLevel)
            {
                TraceMode = TraceMode
            };

            mWorkDirPath = mMgrParams.GetParam(AnalysisMgrSettings.MGR_PARAM_WORK_DIR);

            LogTools.WorkDirPath = mWorkDirPath;

            // Set up the manager cleanup class
            ShowTrace("Set up the manager cleanup class");

            string mgrConfigDBConnectionString;

            if (Global.OfflineMode)
            {
                mgrConfigDBConnectionString = string.Empty;
            }
            else
            {
                // SQL Server: Data Source=proteinseqs;Initial Catalog=manager_control
                // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=d3l243;
                mgrConfigDBConnectionString = mMgrParams.GetParam(MgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING);
            }

            mMgrErrorCleanup = new CleanupMgrErrors(mgrConfigDBConnectionString, mMgrName, mDebugLevel, mMgrDirectoryPath, mWorkDirPath, TraceMode);

            ShowTrace("Initialize the Summary file");

            mSummaryFile = new SummaryFile();
            mSummaryFile.Clear();

            ShowTrace("Initialize the Plugin Loader");

            CheckStopTrace("CreatePluginLoader");

            mPluginLoader = new PluginLoader(mSummaryFile, mMgrDirectoryPath);
            RegisterEvents(mPluginLoader);

            if (TraceMode)
                mPluginLoader.TraceMode = true;

            // Use a custom error event handler
            UnregisterEventHandler(mPluginLoader, BaseLogger.LogLevels.ERROR);
            mPluginLoader.ErrorEvent += PluginLoader_ErrorEventHandler;

            CheckStopTrace("ExitInitMgr");

            // Everything worked
            return true;
        }

        /// <summary>
        /// Initialize mMgrParams using the manager directory path and the TraceMode flag
        /// </summary>
        /// <param name="throwExceptions">
        /// When true, if an exception is encountered, immediately throws
        /// the exception so that the calling method can handle it
        /// </param>
        public void InitMgrSettings(bool throwExceptions)
        {
            try
            {
                mMgrParams = new AnalysisMgrSettings(mMgrDirectoryPath, TraceMode);
            }
            catch (Exception ex) when (!throwExceptions)
            {
                ConsoleMsgUtils.ShowError("Error instantiating AnalysisMgrSettings", ex);
                Global.IdleLoop(0.5);
            }
        }

        /// <summary>
        /// Loop to perform all analysis jobs
        /// </summary>
        public void DoAnalysis()
        {
            ShowTrace("Entering MainProcess.DoAnalysis");

            var loopCount = 0;
            var tasksStartedCount = 0;
            var errorDeletingFilesFlagFile = false;

            var lastConfigDBUpdate = DateTime.UtcNow;

            // Used to track critical manager errors (not necessarily failed analysis jobs when the plugin reports "no results" or similar)
            var criticalMgrErrorCount = 0;
            var successiveDeadLockCount = 0;

            try
            {
                ShowTrace("Entering MainProcess.DoAnalysis Try/Catch block");

                var maxLoopCount = mMgrParams.GetParam("MaxRepetitions", 1);
                var requestJobs = true;
                var oneTaskStarted = false;
                var oneTaskPerformed = false;

                CheckStopTrace("InitStatusTools");
                InitStatusTools();

                while (loopCount < maxLoopCount && requestJobs)
                {
                    CheckStopTrace("UpdateStatusIdle");
                    UpdateStatusIdle("No analysis jobs found");

                    CheckStopTrace("CheckConfigChanged");

                    // Check for configuration change
                    // This variable will be true if the AnalysisManagerProg.exe.config file has been updated
                    if (mConfigChanged)
                    {
                        // Local config file has changed
                        mConfigChanged = false;

                        ShowTrace("Reloading manager settings since config file has changed");

                        CheckStopTrace("ReloadManagerSettings");

                        if (!ReloadManagerSettings())
                        {
                            return;
                        }

                        mConfigFileWatcher.EnableRaisingEvents = true;

                        if (mLocalSettingsFileWatcher != null)
                            mLocalSettingsFileWatcher.EnableRaisingEvents = true;
                    }
                    else
                    {
                        CheckStopTrace("UpdateManagerSettings");

                        // Reload the manager control DB settings in case they have changed
                        // However, only reload every 2 minutes
                        if (!UpdateManagerSettings(ref lastConfigDBUpdate, 2))
                        {
                            // Error retrieving settings from the manager control DB
                            return;
                        }
                    }

                    // Check to see if manager is still active
                    var mgrActive = mMgrParams.GetParam(AnalysisMgrSettings.MGR_PARAM_MGR_ACTIVE, false);
                    var mgrActiveLocal = mMgrParams.GetParam(MgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, false);
                    var taskRequestEnableTime = mMgrParams.GetParam(AnalysisMgrSettings.MGR_PARAM_TASK_REQUEST_ENABLE_TIME);

                    var taskRequestDisabled = DateTime.TryParse(taskRequestEnableTime, out var enableTimeValue) && enableTimeValue > DateTime.Now;

                    if (!(mgrActive && mgrActiveLocal) || taskRequestDisabled)
                    {
                        string managerDisableReason;

                        if (!mgrActiveLocal)
                        {
                            managerDisableReason = "Disabled locally via AnalysisManagerProg.exe.config";
                            UpdateStatusDisabled(MgrStatusCodes.DISABLED_LOCAL, managerDisableReason);
                        }
                        else if (taskRequestDisabled)
                        {
                            // Manager parameter TaskRequestEnableTime in the Manager Control DB has a future date/time
                            managerDisableReason = "Requesting new tasks is disabled until " + taskRequestEnableTime;
                            UpdateStatusDisabled(MgrStatusCodes.DISABLED_MC, managerDisableReason);
                        }
                        else
                        {
                            managerDisableReason = "Disabled in Manager Control DB";
                            UpdateStatusDisabled(MgrStatusCodes.DISABLED_MC, managerDisableReason);
                        }

                        LogMessage("Manager inactive: " + managerDisableReason);
                        Global.IdleLoop(0.75);
                        return;
                    }

                    CheckStopTrace("CheckUpdateRequired");

                    var mgrUpdateRequired = mMgrParams.GetParam("ManagerUpdateRequired", false);

                    if (mgrUpdateRequired)
                    {
                        const string msg = "Manager update is required";
                        LogMessage(msg);
                        mMgrParams.AckManagerUpdateRequired();
                        UpdateStatusIdle(msg);
                        return;
                    }

                    if (mMgrErrorCleanup.DetectErrorDeletingFilesFlagFile())
                    {
                        // Delete the Error Deleting status flag file first, so next time through this step is skipped
                        mMgrErrorCleanup.DeleteErrorDeletingFilesFlagFile();

                        // There was a problem deleting non result files with the last job.  Attempt to delete files again
                        if (!mMgrErrorCleanup.CleanWorkDir())
                        {
                            if (oneTaskStarted)
                            {
                                LogError("Error cleaning working directory, job " + mAnalysisTask.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "Job") + "; see directory " + mWorkDirPath);
                                mAnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Error cleaning working directory");
                            }
                            else
                            {
                                LogError("Error cleaning working directory; see directory " + mWorkDirPath);
                            }
                            mMgrErrorCleanup.CreateStatusFlagFile();
                            UpdateStatusFlagFileExists();
                            return;
                        }
                        // Successful delete of files in working directory, so delete the status flag file
                        mMgrErrorCleanup.DeleteStatusFlagFile(mDebugLevel);
                    }

                    // Verify that an error hasn't left the system in an odd state
                    if (StatusFlagFileError())
                    {
                        LogError("Flag file exists - unable to perform any further analysis jobs");
                        UpdateStatusFlagFileExists();
                        Global.IdleLoop(1.5);
                        return;
                    }

                    // Check to see if an excessive number of errors have occurred
                    if (criticalMgrErrorCount > MAX_ERROR_COUNT)
                    {
                        LogError("Excessive task failures; disabling manager via flag file");

                        // Note: We previously called DisableManagerLocally() to update AnalysisManager.config.exe
                        // We now create a flag file instead
                        // This gives the manager a chance to auto-cleanup things if ManagerErrorCleanupMode is >= 1

                        mMgrErrorCleanup.CreateStatusFlagFile();
                        UpdateStatusFlagFileExists();

                        break;
                    }

                    CheckStopTrace("ValidateWorkingDir");

                    // Verify working directory properly specified and empty
                    if (!ValidateWorkingDir())
                    {
                        if (oneTaskStarted)
                        {
                            // Working directory problem due to the most recently processed job
                            // Create ErrorDeletingFiles file and exit the program
                            LogError("Working directory problem, creating " + CleanupMgrErrors.ERROR_DELETING_FILES_FILENAME + "; see directory " + mWorkDirPath);
                            mMgrErrorCleanup.CreateErrorDeletingFilesFlagFile();
                            UpdateStatusIdle("Working directory not empty");
                        }
                        else
                        {
                            // Working directory problem, so create flag file and exit
                            LogError("Working directory problem, disabling manager via flag file; see directory " + mWorkDirPath);
                            mMgrErrorCleanup.CreateStatusFlagFile();
                            UpdateStatusFlagFileExists();
                        }
                        break;
                    }

                    if (!Global.LinuxOS)
                    {
                        CheckStopTrace("CheckPendingUpdates");

                        if (WindowsUpdatesArePending())
                        {
                            // Check whether the computer is likely to install the monthly Windows Updates within the next few hours
                            // Do not request a task between 12 am and 6 am on Thursday in the week with the third Tuesday of the month
                            // Do not request a task between 2 am and 4 am or between 9 am and 11 am on Sunday following the week with the third Tuesday of the month
                            break;
                        }
                    }

                    if (Global.OfflineMode)
                    {
                        ShowTrace("Looking for an available offline task in the task queue directory");
                    }
                    else
                    {
                        ShowTrace("Requesting a new task from DMS_Pipeline");
                    }

                    CheckStopTrace("CreateMyEMSLUtilities");

                    // Re-initialize these utilities for each analysis job
                    // Note that when RetrieveResources is called, the MyEMSL certificate file (svc-dms.pfx) will be verified to exist
                    // (via GetSharedResources calling CertificateFileExists)
                    mMyEMSLUtilities = new MyEMSLUtilities(mDebugLevel, mWorkDirPath, TraceMode);
                    RegisterEvents(mMyEMSLUtilities);

                    CheckStopTrace("RequestTask");

                    // Get an analysis job, if any are available
                    var taskReturn = mAnalysisTask.RequestTask();

                    switch (taskReturn)
                    {
                        case DBTask.RequestTaskResult.NoTaskFound:
                            ShowTrace("No tasks found for " + mMgrName);

                            // No tasks found
                            if (mDebugLevel >= 3)
                            {
                                LogMessage("No analysis jobs found for " + mMgrName);
                            }
                            requestJobs = false;
                            criticalMgrErrorCount = 0;
                            break;

                        case DBTask.RequestTaskResult.ResultError:
                            ShowTrace("Error requesting a task for " + mMgrName);

                            // There was a problem getting the task; errors were logged by RequestTaskResult
                            criticalMgrErrorCount++;

                            // Delay for an increasing amount of time as an increasing number of deadlocks and/or resultErrors occur
                            PauseManagerForCooldown(criticalMgrErrorCount + successiveDeadLockCount);

                            break;

                        case DBTask.RequestTaskResult.TaskFound:

                            ShowTrace("Task found for " + mMgrName);

                            tasksStartedCount++;
                            successiveDeadLockCount = 0;

                            try
                            {
                                oneTaskStarted = true;
                                var defaultManagerWorkDir = mWorkDirPath;

                                var resultCode = DoAnalysisJob(out var runningRemote);

                                if (!string.Equals(mWorkDirPath, defaultManagerWorkDir))
                                {
                                    // Restore the work dir path
                                    mWorkDirPath = defaultManagerWorkDir;
                                    mMgrParams.SetParam(AnalysisMgrSettings.MGR_PARAM_WORK_DIR, mWorkDirPath);
                                }

                                if (resultCode == CloseOutType.CLOSEOUT_SUCCESS)
                                {
                                    // Task succeeded; reset the sequential job failure counter
                                    criticalMgrErrorCount = 0;
                                    oneTaskPerformed = true;
                                }
                                else
                                {
                                    // Something went wrong; errors were logged by DoAnalysisJob
                                    if (resultCode != CloseOutType.CLOSEOUT_FAILED ||
                                        mMostRecentErrorMessage.Contains("None of the spectra are centroided") ||
                                        mMostRecentErrorMessage.Contains("No peaks found") ||
                                        mMostRecentErrorMessage.Contains("No spectra were exported"))
                                    {
                                        // Job failed, but this was not a manager error
                                        // Do not increment the error count
                                    }
                                    else if (!runningRemote)
                                    {
                                        criticalMgrErrorCount++;
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                // Something went wrong; errors likely were not logged by DoAnalysisJob

                                LogError("Exception thrown by DoAnalysisJob", ex);
                                mStatusTools.UpdateIdle("Error encountered", "MainProcess.DoAnalysis(): " + ex.Message, mMostRecentJobInfo, true);

                                // Set the job state to failed
                                mAnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Exception thrown by DoAnalysisJob");

                                criticalMgrErrorCount++;
                                mNeedToAbortProcessing = true;
                            }
                            break;

                        case DBTask.RequestTaskResult.TooManyRetries:
                            ShowTrace("Too many retries calling the procedure");

                            // There were too many retries calling the procedure; errors were logged by RequestTaskResult
                            // Bump up loopCount to the maximum to exit the loop
                            UpdateStatusIdle("Excessive retries requesting task");
                            loopCount = maxLoopCount;
                            break;

                        case DBTask.RequestTaskResult.Deadlock:
                            ShowTrace("Deadlock");

                            // A deadlock error occurred
                            // Deadlocks can occur when too many managers are requesting a job at once

                            successiveDeadLockCount++;

                            if (successiveDeadLockCount >= 3)
                            {
                                // Encountered 3 deadlocks in a row
                                LogWarning("Deadlock encountered {0} times in a row when requesting a new task; exiting", successiveDeadLockCount);
                                requestJobs = false;
                            }
                            else
                            {
                                // Delay for an increasing amount of time as an increasing number of deadlocks and/or resultErrors occur
                                PauseManagerForCooldown(criticalMgrErrorCount + successiveDeadLockCount);
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
                    loopCount++;

                    // If the only problem was deleting non result files, we want to stop the manager
                    if (mMgrErrorCleanup.DetectErrorDeletingFilesFlagFile())
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
                            Global.IdleLoop(1.5);
                        }
                    }
                    else
                    {
                        if (tasksStartedCount > 0)
                        {
                            LogMessage(
                                "Maximum number of jobs to analyze has been reached: {0} {1}; closing manager",
                                tasksStartedCount, Global.CheckPlural(tasksStartedCount, "job", "jobs"));
                        }
                    }
                }

                if (oneTaskPerformed)
                {
                    LogMessage("Analysis complete for all available jobs");
                }

                CheckStopTrace("CloseManager");

                ShowTrace("Closing the manager");
                UpdateClose("Closing manager.");
            }
            catch (Exception ex)
            {
                LogError("Error in DoAnalysis", ex);
                mStatusTools.UpdateIdle("Error encountered", "MainProcess.DoAnalysis(): " + ex.Message, mMostRecentJobInfo, true);
            }
            finally
            {
                if (mStatusTools != null)
                {
                    // Wait 1 second to give the message queue time to flush
                    Global.IdleLoop(1);

                    CheckStopTrace("DisposeMessageQueue");

                    ShowTrace("Disposing message queue via mStatusTools.DisposeMessageQueue");
                    mStatusTools.DisposeMessageQueue();
                }
            }
        }

        /// <summary>
        /// Perform an analysis job
        /// </summary>
        /// <param name="runningRemote">Output: True if we checked the status of a remote job</param>
        /// <returns>Tool resourcer or tool runner result code</returns>
        private CloseOutType DoAnalysisJob(out bool runningRemote)
        {
            var jobNum = mAnalysisTask.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0);
            var stepNum = mAnalysisTask.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "Step", 0);
            var cpuLoadExpected = mAnalysisTask.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "CPU_Load", 1);

            var datasetName = AnalysisResources.GetDatasetName(mAnalysisTask);
            var jobToolDescription = mAnalysisTask.GetCurrentJobToolDescription();

            var runJobsRemotely = mMgrParams.GetParam("RunJobsRemotely", false);
            var runningRemoteFlag = mAnalysisTask.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, "RunningRemote", 0);
            runningRemote = runningRemoteFlag > 0;

            if (Global.OfflineMode)
            {
                // Update the working directory path to match the current task
                // This manager setting was updated by SelectOfflineJobInfoFile in AnalysisJob
                mWorkDirPath = mMgrParams.GetParam(AnalysisMgrSettings.MGR_PARAM_WORK_DIR);
            }

            ShowTrace("Processing job " + jobNum + ", " + jobToolDescription);

            // Initialize summary and status files
            mSummaryFile.Clear();

            if (mStatusTools == null)
            {
                InitStatusTools();

                if (mStatusTools == null)
                {
                    throw new Exception("mStatusTools is still null after calling InitStatusTools");
                }
            }

            // Update the cached most recent job info
            mMostRecentJobInfo = ConstructMostRecentJobInfoText(
                DateTime.Now.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT),
                jobNum, datasetName, jobToolDescription);

            mStatusTools.TaskStartTime = DateTime.UtcNow;
            mStatusTools.Dataset = datasetName;
            mStatusTools.WorkDirPath = mWorkDirPath;
            mStatusTools.JobNumber = jobNum;
            mStatusTools.JobStep = stepNum;
            mStatusTools.Tool = jobToolDescription;
            mStatusTools.MgrName = mMgrName;
            mStatusTools.ProgRunnerProcessID = 0;
            mStatusTools.ProgRunnerCoreUsage = cpuLoadExpected;

            if (Global.OfflineMode)
            {
                mStatusTools.OfflineJobStatusFilePath = RemoteTransferUtility.GetOfflineJobStatusFilePath(mMgrParams, mAnalysisTask);
            }

            mStatusTools.UpdateAndWrite(
                MgrStatusCodes.RUNNING,
                TaskStatusCodes.RUNNING,
                TaskStatusDetailCodes.RETRIEVING_RESOURCES,
                0, 0, string.Empty, string.Empty,
                mMostRecentJobInfo, true);

            var processID = Process.GetCurrentProcess().Id;

            // Note: The format of the following text is important; be careful about changing it
            // In particular, method DetermineRecentErrorMessages in MainProcess looks for log entries
            //   matching RegEx: "^([^,]+),.+Started analysis job (\d+), Dataset (.+), Tool ([^,]+)"

            // Example log entries

            // ReSharper disable CommentTypo
            // 5/04/2015 12:34:46, Pub-88-3: Started analysis job 1193079, Dataset Lp_PDEC_N-sidG_PD1_1May15_Lynx_15-01-24, Tool Decon2LS_V2, Step 1, INFO,
            // 5/04/2015 10:54:49, Proto-6_Analysis-1: Started analysis job 1192426, Dataset LewyHNDCGlobFractestrecheck_SRM_HNDC_Frac46_smeagol_05Apr15_w6326a, Tool Results_Transfer (MASIC_Finnigan), Step 2, INFO,
            // ReSharper restore CommentTypo

            LogMessage(mMgrName + ": Started analysis job " + jobNum + ", Dataset " + datasetName + ", Tool " + jobToolDescription + ", Process ID " + processID);

            if (mDebugLevel >= 2)
            {
                // Log the debug level value whenever the debug level is 2 or higher
                LogMessage("Debug level is " + mDebugLevel);
            }

            // Create an object to manage the job resources
            if (!SetResourceObject(out var toolResourcer))
            {
                LogError(mMgrName + ": Unable to set the Resource object, job " + jobNum + ", Dataset " + datasetName, true);
                mAnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Unable to set resource object");
                mMgrErrorCleanup.CleanWorkDir();
                UpdateStatusIdle("Error encountered: Unable to set resource object");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Create an object to run the analysis tool
            if (!SetToolRunnerObject(out var toolRunner))
            {
                const string errMsg = "Unable to set the tool runner object";
                LogError(string.Format("{0}: {1}, job {2}, Dataset {3}", mMgrName, errMsg, jobNum, datasetName), true);
                mAnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, errMsg);
                mMgrErrorCleanup.CleanWorkDir();
                UpdateStatusIdle(string.Format("Error encountered: {0}", errMsg));
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (NeedToAbortProcessing())
            {
                ShowTrace("NeedToAbortProcessing; closing job step task");

                var closeOut = mInsufficientFreeMemory ? CloseOutType.CLOSEOUT_RESET_JOB_STEP_INSUFFICIENT_MEMORY : CloseOutType.CLOSEOUT_FAILED;
                mAnalysisTask.CloseTask(closeOut, "Processing aborted");

                mMgrErrorCleanup.CleanWorkDir();
                UpdateStatusIdle("Processing aborted");

                if (mInsufficientFreeMemory)
                {
                    LogWarning("Pausing the manager for 60 minutes since not enough free system memory");
                    mMgrParams.PauseManagerTaskRequests();
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make sure we have enough free space on the drive with the working directory, in the transfer directory, and in the local organism DB cache directory
            if (!ValidateFreeDiskSpace(toolResourcer, out mMostRecentErrorMessage, out var directoryWithInsufficientSpace))
            {
                ShowTrace("Insufficient free space; closing job step task");

                if (string.IsNullOrEmpty(mMostRecentErrorMessage))
                {
                    mMostRecentErrorMessage = string.Format(
                        "Insufficient free space: {0}",
                        string.IsNullOrWhiteSpace(directoryWithInsufficientSpace)
                            ? "location undefined"
                            : directoryWithInsufficientSpace);

                    LogError(mMostRecentErrorMessage);
                }

                mAnalysisTask.CloseTask(CloseOutType.CLOSEOUT_RESET_JOB_STEP, mMostRecentErrorMessage);
                mMgrErrorCleanup.CleanWorkDir();
                UpdateStatusIdle("Processing aborted");

                // Drive free space issues are unlikely to resolve themselves quickly
                // Pausing the manager for 6 hours since not enough free disk space

                const int pauseDurationHours = 6;

                LogWarning(
                    "Pausing the manager for {0} hours since not enough free disk space: {1}",
                    pauseDurationHours,
                    string.IsNullOrWhiteSpace(directoryWithInsufficientSpace)
                        ? "location undefined"
                        : directoryWithInsufficientSpace);

                mMgrParams.PauseManagerTaskRequests(pauseDurationHours * 60);

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Possibly disable MyEMSL
            if (DisableMyEMSL)
            {
                toolResourcer.SetOption(Global.AnalysisResourceOptions.MyEMSLSearchDisabled, true);
            }

            bool success;
            bool jobSucceeded;

            RemoteMonitor remoteMonitor;

            // Retrieve files required for the job
            mMgrErrorCleanup.CreateStatusFlagFile();

            CloseOutType resultCode;

            if (runningRemote)
            {
                // Job is running remotely; check its status
                // If completed (success or fail), retrieve the results
                success = CheckRemoteJobStatus(toolRunner, out resultCode, out remoteMonitor);

                jobSucceeded = success && AnalysisJob.SuccessOrNoData(resultCode);
            }
            else
            {
                remoteMonitor = null;

                // Retrieve the resources for the job then either run locally or run remotely
                var resourcesRetrieved = RetrieveResources(toolResourcer, jobNum, datasetName, out resultCode);

                if (!resourcesRetrieved)
                {
                    if (SkippedStepTool(resultCode))
                    {
                        // This is not an error; we're simply skipping the step tool or waiting for another job to finish creating a file that this job needs
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }

                    // Error occurred
                    // Note that mAnalysisTask.CloseTask() should have already been called
                    var reportSuccess = HandleJobFailure(toolRunner, resultCode);

                    if (reportSuccess)
                        return CloseOutType.CLOSEOUT_SUCCESS;

                    return resultCode == CloseOutType.CLOSEOUT_SUCCESS ? CloseOutType.CLOSEOUT_FAILED : resultCode;
                }

                // Run the job
                mStatusTools.UpdateAndWrite(MgrStatusCodes.RUNNING, TaskStatusCodes.RUNNING, TaskStatusDetailCodes.RUNNING_TOOL, 0);

                if (runJobsRemotely)
                {
                    // Transfer files to the remote host so that the job can run remotely
                    success = RunJobRemotely(toolResourcer, jobNum, stepNum, out resultCode);

                    if (!success)
                    {
                        ShowTrace("Error staging the job to run remotely; closing job step task");

                        if (string.IsNullOrEmpty(mMostRecentErrorMessage))
                        {
                            mMostRecentErrorMessage = "Unknown error staging the job to run remotely";
                        }
                        mAnalysisTask.CloseTask(resultCode, mMostRecentErrorMessage, toolRunner);
                    }

                    // jobSucceeded is always false when we stage files to run remotely
                    // Only set it to true if CheckRemoteJobStatus reports success and the resultCode is Success or No_Data
                    jobSucceeded = false;
                }
                else
                {
                    success = RunJobLocally(toolRunner, jobNum, datasetName, out resultCode);

                    // Note: if success is false, RunJobLocally will have already called .CloseTask

                    jobSucceeded = success;
                }
            }

            if (!success)
            {
                // Error occurred
                // Note that mAnalysisTask.CloseTask() should have already been called
                var reportSuccess = HandleJobFailure(toolRunner, resultCode);

                if (reportSuccess)
                    return CloseOutType.CLOSEOUT_SUCCESS;

                return resultCode == CloseOutType.CLOSEOUT_SUCCESS ? CloseOutType.CLOSEOUT_FAILED : resultCode;
            }

            // Close out the job
            mStatusTools.UpdateAndWrite(MgrStatusCodes.RUNNING, TaskStatusCodes.CLOSING, TaskStatusDetailCodes.CLOSING, 100);

            try
            {
                ShowTrace("Task completed successfully; closing the job step task");

                CloseOutType closeOut;

                if (runJobsRemotely)
                {
                    // resultCode will be CLOSEOUT_RUNNING_REMOTE if RunJobRemotely was called
                    // or if CheckRemoteJobStatus was called and the job is still in progress

                    // resultCode will be CLOSEOUT_SUCCESS if CheckRemoteJobStatus found that the job was done
                    // and successfully retrieved the results

                    closeOut = resultCode;
                }
                else
                {
                    closeOut = CloseOutType.CLOSEOUT_SUCCESS;
                }

                // Close out the job as a success
                // Examine toolRunner.Message to determine if we should use it as the completion message

                // Prior to March 2023, we only updated the completion message if toolRunner.Message started with "Calibration failed"
                // The current behavior is to always update the completion message, but certain messages may be excluded in the future

                string compMsg;

                if (!toolRunner.Message.StartsWith("Future tool runner message to ignore"))
                {
                    compMsg = toolRunner.Message;
                }
                else
                {
                    compMsg = string.Empty;
                }

                mAnalysisTask.CloseTask(closeOut, compMsg, toolRunner);
                LogMessage(mMgrName + ": Completed job " + jobNum);

                UpdateStatusIdle("Completed job " + jobNum + ", step " + stepNum);

                var deleteRemoteJobFiles = runningRemote && jobSucceeded;

                var cleanupSuccess = CleanupAfterJob(deleteRemoteJobFiles, remoteMonitor);

                return cleanupSuccess ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Exception closing task after a normal run", ex);
                mStatusTools.UpdateIdle("Error encountered", "MainProcess.DoAnalysisJob(): " + ex.Message, mMostRecentJobInfo, true);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool CheckRemoteJobStatus(
            IToolRunner toolRunner,
            out CloseOutType resultCode,
            out RemoteMonitor remoteMonitor)
        {
            try
            {
                LogDebug("Instantiating RemoteMonitor to check remote job status");

                remoteMonitor = new RemoteMonitor(mMgrParams, mAnalysisTask, toolRunner, mStatusTools);
                RegisterEvents(remoteMonitor);

                remoteMonitor.StaleJobStatusFileEvent += RemoteMonitor_StaleJobStatusFileEvent;
                remoteMonitor.StaleLockFileEvent += RemoteMonitor_StaleLockFileEvent;
            }
            catch (Exception ex)
            {
                mMostRecentErrorMessage = "Error instantiating the RemoteMonitor class";
                LogError(mMostRecentErrorMessage, ex);
                resultCode = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                remoteMonitor = null;
                return false;
            }

            try
            {
                var jobStatus = remoteMonitor.GetRemoteJobStatus();

                switch (jobStatus)
                {
                    case RemoteMonitor.RemoteJobStatusCodes.Undefined:
                        LogError(Global.AppendToComment("Undefined remote job status; check the logs", remoteMonitor.Message));

                        resultCode = CloseOutType.CLOSEOUT_RUNNING_REMOTE;
                        return true;

                    case RemoteMonitor.RemoteJobStatusCodes.Unstarted:
                        LogDebug("Remote job has not yet started", 2);
                        resultCode = CloseOutType.CLOSEOUT_RUNNING_REMOTE;
                        return true;

                    case RemoteMonitor.RemoteJobStatusCodes.Running:
                        LogDebug(string.Format("Remote job is running, {0:F1}% complete", remoteMonitor.RemoteProgress), 2);
                        resultCode = CloseOutType.CLOSEOUT_RUNNING_REMOTE;
                        return true;

                    case RemoteMonitor.RemoteJobStatusCodes.Success:

                        var success = HandleRemoteJobSuccess(toolRunner, remoteMonitor, out resultCode);

                        if (!success)
                        {
                            mMostRecentErrorMessage = toolRunner.Message;
                            mAnalysisTask.CloseTask(resultCode, mMostRecentErrorMessage, toolRunner);
                        }
                        return success;

                    case RemoteMonitor.RemoteJobStatusCodes.Failed:

                        HandleRemoteJobFailure(toolRunner, remoteMonitor, out resultCode);
                        mAnalysisTask.CloseTask(resultCode, mMostRecentErrorMessage, toolRunner);
                        return false;

                    default:
                        mMostRecentErrorMessage = "Unrecognized remote job status: " + jobStatus;
                        LogError(Global.AppendToComment(mMostRecentErrorMessage, remoteMonitor.Message));

                        resultCode = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                        return false;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception checking job status on the remote host", ex);
                resultCode = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                return false;
            }
        }

        private bool CleanupAfterJob(bool deleteRemoteJobFiles, RemoteMonitor remoteMonitor)
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
                if (mMgrErrorCleanup.DetectErrorDeletingFilesFlagFile())
                {
                    // If there was a problem deleting non result files, return success and let the manager try to delete the files one more time on the next startup
                    // However, wait another 5 seconds before continuing
                    AppUtils.GarbageCollectNow();
                    Global.IdleLoop(5);

                    return true;
                }

                // Clean the working directory
                try
                {
                    if (!mMgrErrorCleanup.CleanWorkDir(1))
                    {
                        LogError("Error cleaning working directory, job " + mAnalysisTask.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "Job"));
                        mAnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Error cleaning working directory");
                        mMgrErrorCleanup.CreateErrorDeletingFilesFlagFile();
                        return false;
                    }
                }
                catch (Exception ex)
                {
                    LogError("Exception cleaning work directory after normal run", ex);
                    mStatusTools.UpdateIdle("Error encountered", "MainProcess.CleanupAfterJob(): " + ex.Message, mMostRecentJobInfo, true);
                    return false;
                }

                // Delete the status flag file
                mMgrErrorCleanup.DeleteStatusFlagFile(mDebugLevel);

                // Note that we do not need to call mStatusTools.UpdateIdle() here since
                // we called UpdateStatusIdle() just after mAnalysisTask.CloseTask above

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in CleanupAfterJob", ex);
                mStatusTools.UpdateIdle("Error encountered", "MainProcess.CleanupAfterJob(): " + ex.Message, mMostRecentJobInfo, true);
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
        private string ConstructMostRecentJobInfoText(string jobStartTimeStamp, int job, string dataset, string toolName)
        {
            try
            {
                jobStartTimeStamp ??= string.Empty;
                toolName ??= "??";
                dataset ??= "??";

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
                Path = mMgrDirectoryPath,
                IncludeSubdirectories = false,
                Filter = configFileName,
                NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size,
                EnableRaisingEvents = true
            };

            return watcher;
        }

        private bool DataPackageIdMissing()
        {
            var stepToolName = mAnalysisTask.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "StepTool");

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
                var dataPkgID = mAnalysisTask.GetJobParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "DataPackageID", 0);

                if (dataPkgID <= 0)
                {
                    // The data package ID is 0 or missing
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Given a log file with a name like AnalysisMgr_2009-03-25.txt or AnalysisMgr_03-25-2009.txt, returns the log file name for the previous day
        /// </summary>
        /// <param name="logFilePath">Log file path</param>
        private string DecrementLogFilePath(string logFilePath)
        {
            try
            {
                var nameMatcher = new Regex(@"(?<BaseName>.+_)(?<Year>\d{4,4})-(?<Month>\d+)-(?<Day>\d+).\S+", RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var nameMatcherLegacy = new Regex(@"(?<BaseName>.+_)(?<Month>\d+)-(?<Day>\d+)-(?<Year>\d{4,4}).\S+", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var match1 = nameMatcher.Match(logFilePath);

                bool legacyDateCode;
                Match match;

                if (match1.Success)
                {
                    legacyDateCode = false;
                    match = match1;
                }
                else
                {
                    var match2 = nameMatcherLegacy.Match(logFilePath);
                    legacyDateCode = match2.Success;
                    match = match2;
                }

                if (match.Success)
                {
                    var month = int.Parse(match.Groups["Month"].Value);
                    var day = int.Parse(match.Groups["Day"].Value);
                    var year = int.Parse(match.Groups["Year"].Value);

                    var currentDate = new DateTime(year, month, day);
                    var newDate = currentDate.AddDays(-1);

                    var dateFormat = legacyDateCode ? "MM-dd-yyyy" : FileLogger.LOG_FILE_DATE_CODE;

                    return match.Groups["BaseName"].Value + newDate.ToString(dateFormat) + Path.GetExtension(logFilePath);
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
        /// Will use logger to determine the most recent log file
        /// Also examines the message info stored in logger
        /// Lastly, if mostRecentJobInfo is empty, will update it with info on the most recent job started
        /// </summary>
        /// <param name="errorMessageCountToReturn">Maximum number of error messages to return</param>
        /// <param name="mostRecentJobInfo">Info on the most recent job started by this manager</param>
        /// <returns>List of recent errors</returns>
        public IEnumerable<string> DetermineRecentErrorMessages(int errorMessageCountToReturn, ref string mostRecentJobInfo)
        {
            // This RegEx will match all text up to the first comma (this is the time stamp), followed by a comma, then the error message, then the text ", Error,"
            const string ERROR_MATCH_REGEX = "^(?<Date>[^,]+),(?<Error>.+), Error, *$";

            // This RegEx looks for information on a job starting
            // Note: do not try to match "Step \d+" with this RegEx due to variations on how the log message appears
            const string JOB_START_REGEX = @"^(?<Date>[^,]+),.+Started analysis job (?<Job>\d+), Dataset (?<Dataset>.+), Tool (?<Tool>[^,]+)";

            // Examples matching log entries

            // ReSharper disable CommentTypo
            // 5/04/2015 12:34:46, Pub-88-3: Started analysis job 1193079, Dataset Lp_PDEC_N-sidG_PD1_1May15_Lynx_15-01-24, Tool Decon2LS_V2, Step 1, INFO,
            // 5/04/2015 10:54:49, Proto-6_Analysis-1: Started analysis job 1192426, Dataset LewyHNDCGlobFractestrecheck_SRM_HNDC_Frac46_smeagol_05Apr15_w6326a, Tool Results_Transfer (MASIC_Finnigan), Step 2, INFO,
            // ReSharper restore CommentTypo

            // The following effectively defines the number of days in the past to search when finding recent errors
            const int MAX_LOG_FILES_TO_SEARCH = 5;

            // In this list, keys are error message strings and values are the corresponding time of the error
            var recentErrorMessages = new List<KeyValuePair<string, DateTime>>();

            mostRecentJobInfo ??= string.Empty;

            try
            {
                var mostRecentJobInfoFromLogs = string.Empty;

                if (errorMessageCountToReturn < 1)
                    errorMessageCountToReturn = 1;

                // Initialize the RegEx that splits out the timestamp from the error message
                var errorLineMatcher = new Regex(ERROR_MATCH_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var jobStartLineMatcher = new Regex(JOB_START_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // Initialize the queue that holds recent error messages
                var msgQueue = new Queue<string>(errorMessageCountToReturn);

                // Initialize the dictionary to hold the error messages, but without date stamps
                var uniqueErrorMessages = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

                // Examine the most recent error reported by the logger
                var mostRecentErrorMsg = LogTools.MostRecentErrorMessage;

                var loggerReportsError = !string.IsNullOrWhiteSpace(mostRecentErrorMsg);

                var logFilePath = GetRecentLogFilename();

                if (errorMessageCountToReturn > 1 || !loggerReportsError)
                {
                    // Recent error message reported by the logger is empty or errorMessageCountToReturn is greater than one
                    // Open log file logFilePath to find the most recent error messages
                    // If not enough error messages are found, we will look through previous log files

                    var logFileCountProcessed = 0;
                    var checkForMostRecentJob = true;

                    while (msgQueue.Count < errorMessageCountToReturn && logFileCountProcessed < MAX_LOG_FILES_TO_SEARCH)
                    {
                        if (File.Exists(logFilePath))
                        {
                            using (var reader = new StreamReader(new FileStream(logFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                            {
                                while (!reader.EndOfStream)
                                {
                                    var dataLine = reader.ReadLine();

                                    if (dataLine == null)
                                        continue;

                                    var matchError = errorLineMatcher.Match(dataLine);

                                    if (matchError.Success)
                                    {
                                        DetermineRecentErrorCacheError(matchError, dataLine, uniqueErrorMessages, msgQueue,
                                                                       errorMessageCountToReturn);
                                    }

                                    if (!checkForMostRecentJob)
                                        continue;

                                    var jobMatch = jobStartLineMatcher.Match(dataLine);

                                    if (!jobMatch.Success)
                                        continue;

                                    try
                                    {
                                        mostRecentJobInfoFromLogs = ConstructMostRecentJobInfoText(
                                            jobMatch.Groups["Date"].Value,
                                            int.Parse(jobMatch.Groups["Job"].Value),
                                            jobMatch.Groups["Dataset"].Value,
                                            jobMatch.Groups["Tool"].Value);
                                    }
                                    catch (Exception)
                                    {
                                        // Ignore errors here
                                    }
                                }
                            }

                            if (checkForMostRecentJob && mostRecentJobInfoFromLogs.Length > 0)
                            {
                                // We determined the most recent job; no need to check other log files
                                checkForMostRecentJob = false;
                            }
                        }
                        // else: Log file not found; that's OK, we'll decrement the name by one day and keep checking

                        // Increment the log file counter, regardless of whether the log file was found
                        logFileCountProcessed++;

                        if (msgQueue.Count >= errorMessageCountToReturn)
                            continue;

                        // We still haven't found errorMessageCountToReturn error messages
                        // Keep checking older log files as long as msgQueue.Count < errorMessageCountToReturn

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
                    var match = errorLineMatcher.Match(LogTools.MostRecentErrorMessage);

                    if (match.Success)
                    {
                        DetermineRecentErrorCacheError(match, LogTools.MostRecentErrorMessage, uniqueErrorMessages, msgQueue, errorMessageCountToReturn);
                    }
                }

                // Populate recentErrorMessages and recentErrorMessageDates using the messages stored in msgQueue
                while (msgQueue.Count > 0)
                {
                    var errorMessageClean = msgQueue.Dequeue();

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
                // RegEx didn't match; this is unexpected
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
                // Too many queued messages, so remove the oldest one
                // However, only do this if the new error message has a timestamp newer than the oldest queued message
                //  (this is a consideration when processing multiple log files)

                var addItemToQueue = true;

                var queuedError = qErrorMsgQueue.Peek();

                // Get the timestamp associated with queuedError, as tracked by the dictionary
                if (!uniqueErrorMessages.TryGetValue(queuedError, out var queuedTimeStamp))
                {
                    // The error message is not in the dictionary; this is unexpected
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
        /// Sets the local mgr_active flag to false for serious problems
        /// </summary>
        private void DisableManagerLocally()
        {
            // Note: We previously called mMgrParams.DisableManagerLocally() to update AnalysisManager.config.exe
            // We now create a flag file instead
            // This gives the manager a chance to auto-cleanup things if ManagerErrorCleanupMode is >= 1

            mMgrErrorCleanup.CreateStatusFlagFile();
            UpdateStatusFlagFileExists();
        }

        /// <summary>
        /// Enable offline mode
        /// </summary>
        /// <remarks>When offline, does not contact any databases or remote shares</remarks>
        /// <param name="runningLinux">Set to true if running Linux</param>
        public static void EnableOfflineMode(bool runningLinux = true)
        {
            Global.EnableOfflineMode(runningLinux);
        }

        private string GetBaseLogFileName()
        {
            return GetBaseLogFileName(mMgrParams);
        }

        /// <summary>
        /// Get the base log file name, as defined in the manager parameters
        /// </summary>
        /// <param name="mgrParams">Manager parameters</param>
        public static string GetBaseLogFileName(IMgrParams mgrParams)
        {
            var logFileNameBase = mgrParams.GetParam("LogFileName", DEFAULT_BASE_LOGFILE_NAME);
            return Global.LinuxOS ? logFileNameBase.Replace('\\', '/') : logFileNameBase;
        }

        private string GetRecentLogFilename()
        {
            try
            {
                // Obtain a list of log files
                var logFileNameBase = GetBaseLogFileName();
                var files = Directory.GetFiles(mMgrDirectoryPath, logFileNameBase + "*.txt");

                if (files.Length == 0)
                    return string.Empty;

                return (from item in files orderby item.ToLower() select item).Last();
            }
            catch (Exception)
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Extract the value DefaultDMSConnString from AnalysisManagerProg.exe.config
        /// </summary>
        private string GetXmlConfigDefaultConnectionString()
        {
            return GetXmlConfigFileSetting("DefaultDMSConnString");
        }

        /// <summary>
        /// Extract the value for the given setting from AnalysisManagerProg.exe.config
        /// If the setting name is MgrCnfgDbConnectStr or DefaultDMSConnString, first checks file AnalysisManagerProg.exe.db.config
        /// </summary>
        /// <remarks>Uses a simple text reader in case the file has malformed XML</remarks>
        /// <returns>Setting value if found, otherwise an empty string</returns>
        private string GetXmlConfigFileSetting(string settingName)
        {
            if (string.IsNullOrWhiteSpace(settingName))
                throw new ArgumentException("Setting name cannot be blank", nameof(settingName));

            var configFilePaths = new List<string>();

            if (settingName.Equals("MgrCnfgDbConnectStr", StringComparison.OrdinalIgnoreCase) ||
                settingName.Equals("DefaultDMSConnString", StringComparison.OrdinalIgnoreCase))
            {
                configFilePaths.Add(Path.Combine(mMgrDirectoryPath, mMgrExeName + ".db.config"));
            }

            configFilePaths.Add(Path.Combine(mMgrDirectoryPath, mMgrExeName + ".config"));

            var mgrSettings = new MgrSettings();
            RegisterEvents(mgrSettings);

            var valueFound = mgrSettings.GetXmlConfigFileSetting(configFilePaths, settingName, out var settingValue);

            if (valueFound)
                return settingValue;

            return string.Empty;
        }

        private CleanupMgrErrors.CleanupModes GetManagerErrorCleanupMode()
        {
            var managerErrorCleanupMode = mMgrParams.GetParam("ManagerErrorCleanupMode", "0");

            return managerErrorCleanupMode.Trim() switch
            {
                "0" => CleanupMgrErrors.CleanupModes.Disabled,
                "1" => CleanupMgrErrors.CleanupModes.CleanupOnce,
                "2" => CleanupMgrErrors.CleanupModes.CleanupAlways,
                _ => CleanupMgrErrors.CleanupModes.Disabled
            };
        }

        private bool HandleJobFailure(IToolRunner toolRunner, CloseOutType resultCode)
        {
            ShowTrace("Tool run error; cleaning up");

            try
            {
                if (!mAnalysisTask.TaskClosed)
                {
                    LogWarning("Upstream code typically calls .CloseTask before HandleJobFailure is reached; closing the task now");

                    mAnalysisTask.CloseTask(resultCode, mMostRecentErrorMessage, toolRunner);
                }

                if (mMgrErrorCleanup.CleanWorkDir())
                {
                    mMgrErrorCleanup.DeleteStatusFlagFile(mDebugLevel);
                }
                else
                {
                    mMgrErrorCleanup.CreateErrorDeletingFilesFlagFile();
                }

                if (resultCode == CloseOutType.CLOSEOUT_NO_DTA_FILES &&
                    string.Equals(mAnalysisTask.GetParam("StepTool"), "SEQUEST", StringComparison.OrdinalIgnoreCase))
                {
                    // This was a SEQUEST job, but no .DTA files were found
                    // Return true; do not count this as a manager failure
                    return true;
                }

                if (resultCode == CloseOutType.CLOSEOUT_NO_DATA)
                {
                    // Return true; do not count this as a manager failure
                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in cleaning up after RunTool error", ex);
                mStatusTools.UpdateIdle("Error encountered", "MainProcess.HandleJobFailure(): " + ex.Message, mMostRecentJobInfo, true);
                return false;
            }
        }

        private void HandleRemoteJobFailure(IToolRunner toolRunner, RemoteMonitor remoteMonitor, out CloseOutType resultCode)
        {
            // Job failed
            // Parse the .fail file to read the result codes and messages (the file was already retrieved by GetRemoteJobStatus, if it existed)

            var jobResultFile = new FileInfo(Path.Combine(mWorkDirPath, remoteMonitor.TransferUtility.ProcessingFailureFile));

            if (!jobResultFile.Exists)
            {
                mMostRecentErrorMessage = ".fail file not found in the working directory: " + jobResultFile.Name;
                resultCode = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                return;
            }

            var statusParsed = ParseStatusResultFile(remoteMonitor, jobResultFile.FullName, out resultCode, out var completionMessage);

            if (!statusParsed)
            {
                if (string.IsNullOrWhiteSpace(mMostRecentErrorMessage))
                    mMostRecentErrorMessage = "Status file parse error";

                resultCode = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                return;
            }

            mMostRecentErrorMessage = completionMessage;

            if (string.IsNullOrWhiteSpace(mMostRecentErrorMessage))
                mMostRecentErrorMessage = "Remote job failed: " + resultCode;

            LogError(Global.AppendToComment(mMostRecentErrorMessage, remoteMonitor.Message));

            // Retrieve result files then store in the DMS_FailedResults directory

            toolRunner.RetrieveRemoteResults(remoteMonitor.TransferUtility, false, out var retrievedFilePaths);

            if (retrievedFilePaths.Count > 0)
            {
                toolRunner.CopyFailedResultsToArchiveDirectory();
            }

            resultCode = CloseOutType.CLOSEOUT_FAILED_REMOTE;
        }

        private bool HandleRemoteJobSuccess(IToolRunner toolRunner, RemoteMonitor remoteMonitor, out CloseOutType resultCode)
        {
            // Job succeeded
            // Parse the .success file to read the result codes and messages (the file was already retrieved by GetRemoteJobStatus()

            var jobResultFile = new FileInfo(Path.Combine(mWorkDirPath, remoteMonitor.TransferUtility.ProcessingSuccessFile));

            if (!jobResultFile.Exists)
            {
                mMostRecentErrorMessage = ".success file not found in the working directory: " + jobResultFile.Name;
                resultCode = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                return false;
            }

            var statusParsed = ParseStatusResultFile(remoteMonitor, jobResultFile.FullName, out resultCode, out var completionMessage);

            mMostRecentErrorMessage = completionMessage;

            if (!statusParsed)
            {
                resultCode = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                return false;
            }

            // Retrieve result files then call PostProcess
            var resultsRetrieved = toolRunner.RetrieveRemoteResults(remoteMonitor.TransferUtility, true, out _);

            if (!resultsRetrieved)
            {
                resultCode = CloseOutType.CLOSEOUT_FAILED_REMOTE;
                return false;
            }

            var postProcessResult = toolRunner.PostProcessRemoteResults();

            if (!AnalysisJob.SuccessOrNoData(postProcessResult))
            {
                resultCode = postProcessResult;
            }

            if (!AnalysisJob.SuccessOrNoData(resultCode))
            {
                toolRunner.CopyFailedResultsToArchiveDirectory();
                return false;
            }

            // Skip the status files when transferring results
            foreach (var statusFile in remoteMonitor.TransferUtility.StatusFileNames)
            {
                mAnalysisTask.AddResultFileToSkip(statusFile);
            }

            var success = toolRunner.CopyResultsToTransferDirectory();

            if (success)
                return true;

            resultCode = CloseOutType.CLOSEOUT_FAILED_REMOTE;
            return false;
        }

        /// <summary>
        /// Initialize the remote transfer utility
        /// Used by RunJobRemotely and when PushFilesToRemoteHost is true
        /// </summary>
        private RemoteTransferUtility InitializeRemoteTransferUtility()
        {
            var transferUtility = new RemoteTransferUtility(mMgrParams, mAnalysisTask);
            RegisterEvents(transferUtility);

            try
            {
                transferUtility.UpdateParameters(true);
                return transferUtility;
            }
            catch (Exception ex)
            {
                mMostRecentErrorMessage = "Error initializing the remote transfer utility: " + ex.Message;
                LogError(mMostRecentErrorMessage, ex);
                return null;
            }
        }

        /// <summary>
        /// Initializes the status file writing tool
        /// </summary>
        private void InitStatusTools()
        {
            if (mStatusTools != null)
                return;

            var statusFileLoc = Path.Combine(mMgrDirectoryPath, mMgrParams.GetParam("StatusFileLocation", "Status.xml"));

            ShowTrace("Initialize mStatusTools using " + statusFileLoc);

            mStatusTools = new StatusFile(mMgrParams, statusFileLoc, mDebugLevel)
            {
                MgrName = mMgrName,
                MgrStatus = MgrStatusCodes.RUNNING,
                WorkDirPath = mWorkDirPath
            };

            RegisterEvents(mStatusTools);

            var runJobsRemotely = mMgrParams.GetParam("RunJobsRemotely", false);

            if (runJobsRemotely)
            {
                mStatusTools.RemoteMgrName = mMgrParams.GetParam("RemoteHostName");
            }

            UpdateStatusToolLoggingSettings(mStatusTools);
        }

        /// <summary>
        /// Loads the initial settings from application config file AnalysisManagerProg.exe.config
        /// </summary>
        /// <remarks>This method is public because CodeTest uses it</remarks>
        /// <returns>String dictionary containing initial settings if successful; null on error</returns>
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

            // Construct the path to the config document
            var configFilePath = Path.Combine(mMgrDirectoryPath, mMgrExeName + ".config");

            var mgrSettings = mMgrParams.LoadMgrSettingsFromFile(configFilePath);

            if (mgrSettings == null)
                return null;

            // Manager Config DB connection string
            if (!mgrSettings.ContainsKey(MgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING))
            {
                mgrSettings.Add(MgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING, Properties.Settings.Default.MgrCnfgDbConnectStr);
            }

            // Manager active flag
            if (!mgrSettings.ContainsKey(MgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL))
            {
                mgrSettings.Add(MgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, "False");
            }

            // Manager name
            // The manager name may contain $ComputerName$
            // If it does, InitializeMgrSettings in MgrSettings will replace "$ComputerName$" with the local host name
            if (!mgrSettings.ContainsKey(MgrSettings.MGR_PARAM_MGR_NAME))
            {
                mgrSettings.Add(MgrSettings.MGR_PARAM_MGR_NAME, "LoadMgrSettingsFromFile__Undefined_manager_name");
            }

            // Default settings in use flag
            if (!mgrSettings.ContainsKey(MgrSettings.MGR_PARAM_USING_DEFAULTS))
            {
                mgrSettings.Add(MgrSettings.MGR_PARAM_USING_DEFAULTS, Properties.Settings.Default.UsingDefaults.ToString());
            }

            // Default connection string for logging errors to the database
            // Will get updated later when manager settings are loaded from the manager control database
            if (!mgrSettings.ContainsKey(AnalysisMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING))
            {
                mgrSettings.Add(AnalysisMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING, Properties.Settings.Default.DefaultDMSConnString);
            }

            if (TraceMode)
            {
                ShowTrace("Settings loaded from " + PathUtils.CompactPathString(configFilePath, 60));
                MgrSettings.ShowDictionaryTrace(mgrSettings);
            }

            return mgrSettings;
        }

        private bool NeedToAbortProcessing()
        {
            if (mInsufficientFreeMemory)
            {
                LogWarning("Analysis manager does not have enough free system memory - aborting processing");
                return true;
            }

            if (mNeedToAbortProcessing)
            {
                LogError("Analysis manager has encountered a fatal error - aborting processing (mNeedToAbortProcessing is true)");
                return true;
            }

            if (mStatusTools == null)
                return false;

            // ReSharper disable once MergeIntoPattern
            if (!mStatusTools.AbortProcessingNow)
                return false;

            LogError("Found file " + StatusFile.ABORT_PROCESSING_NOW_FILENAME + " - aborting processing");
            return true;
        }

        private Dictionary<string, DateTime> LoadCachedLogMessages(FileSystemInfo messageCacheFile)
        {
            var cachedMessages = new Dictionary<string, DateTime>();

            char[] sepChars = ['\t'];

            using var reader = new StreamReader(new FileStream(messageCacheFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            var lineCount = 0;

            while (!reader.EndOfStream)
            {
                var dataLine = reader.ReadLine();
                lineCount++;

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

            return cachedMessages;
        }

        private void LogErrorToDatabasePeriodically(string errorMessage, int logIntervalHours)
        {
            const string PERIODIC_LOG_FILE = "Periodic_ErrorMessages.txt";

            try
            {
                Dictionary<string, DateTime> cachedMessages;

                var messageCacheFile = new FileInfo(Path.Combine(Global.GetAppDirectoryPath(), PERIODIC_LOG_FILE));

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
                using var writer = new StreamWriter(new FileStream(messageCacheFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                writer.WriteLine("{0}\t{1}", "TimeStamp", "Message");

                foreach (var message in cachedMessages)
                {
                    writer.WriteLine("{0}\t{1}", message.Value.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT), message.Key);
                }
            }
            catch (Exception ex)
            {
                LogError("Error in LogErrorToDatabasePeriodically", ex);
            }
        }

        private bool ParseStatusResultFile(RemoteMonitor remoteMonitor, string jobResultFilePath, out CloseOutType resultCode, out string completionMessage)
        {
            var statusParsed = remoteMonitor.ParseStatusResultFile(
                jobResultFilePath,
                out resultCode, out completionMessage,
                out var remoteStart, out var remoteFinish);

            if (remoteStart > DateTime.MinValue)
            {
                // Store the remote start time, using format code "{0:O}" to format as "2018-04-17T10:30:59.0000000"
                mAnalysisTask.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION,
                                                      RemoteTransferUtility.STEP_PARAM_REMOTE_START,
                                                      string.Format("{0:O}", remoteStart));
            }

            if (remoteFinish > DateTime.MinValue)
            {
                // Store the remote finish time, using format code "{0:O}" to format as "2018-04-17T10:30:59.0000000"
                mAnalysisTask.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION,
                                                      RemoteTransferUtility.STEP_PARAM_REMOTE_FINISH,
                                                      string.Format("{0:O}", remoteFinish));
            }

            return statusParsed;
        }

        private void PauseManagerForCooldown(int errorOrDeadlockCount)
        {
            var cooldownSeconds = 10 + 10 * errorOrDeadlockCount;

            LogMessage(string.Format("Pausing manager for {0} seconds", cooldownSeconds));
            AppUtils.SleepMilliseconds(cooldownSeconds * 1000);
        }

        /// <summary>
        /// Reload the settings from AnalysisManagerProg.exe.config
        /// </summary>
        /// <returns>True if success, false if now disabled locally or if an error</returns>
        private bool ReloadManagerSettings()
        {
            try
            {
                ShowTrace("Reading application config file");

                // Load settings from config file AnalysisManagerProg.exe.config
                var configFileSettings = LoadMgrSettingsFromFile();

                if (configFileSettings == null)
                    return false;

                ShowTrace("Storing manager settings in mMgrParams");

                // Store the new settings then retrieve updated settings from the database
                // or from ManagerSettingsLocal.xml if Global.OfflineMode is true
                if (mMgrParams.LoadSettings(configFileSettings))
                    return true;

                if (!string.IsNullOrWhiteSpace(mMgrParams.ErrMsg))
                {
                    // Log the error
                    LogMessage(mMgrParams.ErrMsg);
                    UpdateStatusDisabled(MgrStatusCodes.DISABLED_LOCAL, "Disabled Locally");
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
            var mgrDirectory = new DirectoryInfo(mMgrDirectoryPath);

            // Files starting with the name IgnoreMe are created by log4NET when it is first instantiated
            // This name is defined in the RollingFileAppender section of the Logging.config file via this XML:
            // <file value="IgnoreMe" />

            foreach (var ignoreMeFile in mgrDirectory.GetFiles("IgnoreMe*.txt"))
            {
                try
                {
                    ignoreMeFile.Delete();
                }
                catch (Exception ex)
                {
                    LogError("Error deleting IgnoreMe file: " + ignoreMeFile.Name, ex);
                }
            }

            // Files named tmp.iso.#### and tmp.peak.#### (where #### are integers) are files created by Decon2LS
            // These files indicate a previous, failed Decon2LS task and can be safely deleted
            // For safety, we will not delete files less than 24 hours old

            var filesToDelete = mgrDirectory.GetFiles("tmp.iso.*").ToList();

            filesToDelete.AddRange(mgrDirectory.GetFiles("tmp.peak.*"));

            foreach (var tempFile in filesToDelete)
            {
                try
                {
                    if (DateTime.UtcNow.Subtract(tempFile.LastWriteTimeUtc).TotalHours > 24)
                    {
                        ShowTrace("Deleting temp file " + tempFile.FullName);
                        tempFile.Delete();
                    }
                }
                catch (Exception)
                {
                    LogError("Error deleting file: " + tempFile.Name);
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
            out CloseOutType resultCode)
        {
            resultCode = CloseOutType.CLOSEOUT_SUCCESS;

            try
            {
                ShowTrace("Obtaining job resources");

                resultCode = toolResourcer.GetResources();

                if (resultCode == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return true;
                }

                if (toolResourcer.NeedToAbortProcessing ||
                    toolResourcer.InsufficientFreeMemory ||
                    resultCode == CloseOutType.CLOSEOUT_RESET_JOB_STEP ||
                    resultCode == CloseOutType.CLOSEOUT_RESET_JOB_STEP_INSUFFICIENT_MEMORY)
                {
                    mInsufficientFreeMemory = toolResourcer.InsufficientFreeMemory;
                    mNeedToAbortProcessing = true;
                    ShowTrace("toolResourcer.NeedToAbortProcessing is true; closing job step task");

                    var closeOut = mInsufficientFreeMemory ? CloseOutType.CLOSEOUT_RESET_JOB_STEP_INSUFFICIENT_MEMORY : CloseOutType.CLOSEOUT_FAILED;
                    mAnalysisTask.CloseTask(closeOut, toolResourcer.Message);

                    if (mInsufficientFreeMemory)
                    {
                        LogWarning("Pausing the manager for 60 minutes since not enough free system memory");
                        mMgrParams.PauseManagerTaskRequests();
                    }

                    return false;
                }

                var statusMessage = "GetResources returned result: " + resultCode;
                ShowTrace(statusMessage + "; closing job step task");

                string compMsg;
                bool errorProcessing;

                if (SkippedStepTool(resultCode))
                {
                    compMsg = string.IsNullOrWhiteSpace(toolResourcer.Message) ? string.Empty : toolResourcer.Message;
                    errorProcessing = false;
                }
                else
                {
                    compMsg = string.IsNullOrWhiteSpace(toolResourcer.Message) ? resultCode.ToString() : toolResourcer.Message;
                    errorProcessing = true;
                    mMostRecentErrorMessage = statusMessage;
                    LogError(mMgrName + ": " + Global.AppendToComment(mMostRecentErrorMessage, toolResourcer.Message) + ", Job " + jobNum + ", Dataset " + datasetName);
                }

                var evalMsg = string.IsNullOrWhiteSpace(toolResourcer.EvalMessage) ? string.Empty : toolResourcer.EvalMessage;

                mAnalysisTask.CloseTask(resultCode, compMsg, evalMsg);

                mMgrErrorCleanup.CleanWorkDir();

                if (errorProcessing)
                {
                    UpdateStatusIdle("Error encountered: " + mMostRecentErrorMessage);
                }

                mMgrErrorCleanup.DeleteStatusFlagFile(mDebugLevel);

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error getting resources", ex);

                mAnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Exception getting resources");

                if (mMgrErrorCleanup.CleanWorkDir())
                {
                    mMgrErrorCleanup.DeleteStatusFlagFile(mDebugLevel);
                }
                else
                {
                    mMgrErrorCleanup.CreateErrorDeletingFilesFlagFile();
                }

                mStatusTools.UpdateIdle("Error encountered", "MainProcess.RetrieveResources(): " + ex.Message, mMostRecentJobInfo, true);
                return false;
            }
        }

        private bool RunJobLocally(
            IToolRunner toolRunner,
            int jobNum,
            string datasetName,
            out CloseOutType resultCode)
        {
            var success = true;

            try
            {
                ShowTrace("Running the step tool locally");

                resultCode = toolRunner.RunTool();

                if (resultCode != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    mMostRecentErrorMessage = toolRunner.Message;

                    if (string.IsNullOrEmpty(mMostRecentErrorMessage))
                    {
                        mMostRecentErrorMessage = "Unknown ToolRunner Error";
                    }

                    ShowTrace("Error running the tool; closing job step task");

                    LogError(mMgrName + ": " + mMostRecentErrorMessage + ", Job " + jobNum + ", Dataset " + datasetName);
                    mAnalysisTask.CloseTask(resultCode, mMostRecentErrorMessage, toolRunner);

                    try
                    {
                        if (mMostRecentErrorMessage.Contains(DECON2LS_FATAL_REMOTING_ERROR) ||
                            mMostRecentErrorMessage.Contains(DECON2LS_CORRUPTED_MEMORY_ERROR))
                        {
                            mNeedToAbortProcessing = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError("Exception examining MostRecentErrorMessage", ex);
                    }

                    if (resultCode == CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE)
                    {
                        mNeedToAbortProcessing = true;
                    }

                    if (mNeedToAbortProcessing && mMostRecentErrorMessage.StartsWith(AnalysisToolRunnerBase.PVM_RESET_ERROR_MESSAGE))
                    {
                        DisableManagerLocally();
                    }

                    success = false;
                }

                if (toolRunner.NeedToAbortProcessing || toolRunner.InsufficientFreeMemory)
                {
                    mInsufficientFreeMemory = toolRunner.InsufficientFreeMemory;
                    mNeedToAbortProcessing = true;
                    ShowTrace("toolRunner.NeedToAbortProcessing is true; closing job step task");

                    var closeOut = mInsufficientFreeMemory ? CloseOutType.CLOSEOUT_RESET_JOB_STEP_INSUFFICIENT_MEMORY : CloseOutType.CLOSEOUT_FAILED;
                    mAnalysisTask.CloseTask(closeOut, mMostRecentErrorMessage, toolRunner);

                    if (mInsufficientFreeMemory)
                    {
                        LogWarning("Pausing the manager for 60 minutes since not enough free system memory");
                        mMgrParams.PauseManagerTaskRequests();
                    }
                }

                return success;
            }
            catch (Exception ex)
            {
                LogError("Exception running job " + jobNum, ex);

                if (ex.Message.Contains(DECON2LS_TCP_ALREADY_REGISTERED_ERROR))
                {
                    mNeedToAbortProcessing = true;
                }

                resultCode = CloseOutType.CLOSEOUT_FAILED;
                mAnalysisTask.CloseTask(resultCode, "Exception running tool", toolRunner);

                return false;
            }
        }

        private bool RunJobRemotely(
            IAnalysisResources toolResourcer,
            int jobNum,
            int stepNum,
            out CloseOutType resultCode)
        {
            try
            {
                ShowTrace("Instantiating RemoteTransferUtility");

                var transferUtility = InitializeRemoteTransferUtility();

                if (transferUtility == null)
                {
                    resultCode = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                var remoteTimestamp = transferUtility.UpdateRemoteTimestamp();

                ShowTrace("Pushing new/updated DMS_Programs files to remote host");

                try
                {
                    // We run the DMS Update Manager for the first job processed, but not for subsequent jobs
                    var copySuccess = mDMSProgramsSynchronized || transferUtility.RunDMSUpdateManager();

                    if (!copySuccess)
                    {
                        mMostRecentErrorMessage = "Error copying manager-related files to the remote host";
                        LogError(mMostRecentErrorMessage);

                        resultCode = CloseOutType.CLOSEOUT_FAILED;
                        return false;
                    }

                    mDMSProgramsSynchronized = true;
                }
                catch (Exception ex)
                {
                    mMostRecentErrorMessage = "Exception copying manager-related files to the remote host: " + ex.Message;
                    LogError(mMostRecentErrorMessage, ex);

                    resultCode = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                ShowTrace("Transferring job-related files to remote host to run remotely");

                try
                {
                    var copySuccess = toolResourcer.CopyResourcesToRemote(transferUtility);

                    if (!copySuccess)
                    {
                        mMostRecentErrorMessage = "Error copying job-related files to the remote host";
                        LogError(mMostRecentErrorMessage);

                        resultCode = CloseOutType.CLOSEOUT_FAILED;
                        return false;
                    }
                }
                catch (NotImplementedException ex)
                {
                    // Plugin XYZ must implement CopyResourcesToRemote to allow for remote processing
                    mMostRecentErrorMessage = ex.Message;

                    // Don't send ex to LogError; no need to log a stack trace
                    LogError(mMostRecentErrorMessage);

                    resultCode = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }
                catch (Exception ex)
                {
                    mMostRecentErrorMessage = "Exception copying job-related files to the remote host: " + ex.Message;
                    LogError(mMostRecentErrorMessage, ex);

                    resultCode = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                ShowTrace("Creating the .info file in the remote task queue directory");

                // All files have been copied remotely
                // Create the .info file so remote managers can start processing
                var success = transferUtility.CreateJobTaskInfoFile(remoteTimestamp, out var infoFilePathRemote);

                if (!success)
                {
                    mMostRecentErrorMessage = "Error creating the remote job task info file";
                    LogError(mMostRecentErrorMessage);

                    resultCode = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                LogMessage(
                    "Job {0}, step {1} staged to run remotely on {2}; remote info file at {3}",
                    jobNum, stepNum, transferUtility.RemoteHostName, infoFilePathRemote);

                resultCode = CloseOutType.CLOSEOUT_RUNNING_REMOTE;
                return true;
            }
            catch (Exception ex)
            {
                mMostRecentErrorMessage = "Exception staging job to run remotely";
                LogError(mMostRecentErrorMessage + ", job " + jobNum, ex);

                resultCode = CloseOutType.CLOSEOUT_FAILED;
                return false;
            }
        }

        private bool SetResourceObject(out IAnalysisResources toolResourcer)
        {
            var stepToolName = mAnalysisTask.GetParam("StepTool");
            ResetPluginLoaderErrorCount(stepToolName);
            ShowTrace("Loading the resourcer for tool " + stepToolName);

            toolResourcer = mPluginLoader.GetAnalysisResources(stepToolName.ToLower());

            if (toolResourcer == null && stepToolName.StartsWith("Test_", StringComparison.OrdinalIgnoreCase))
            {
                stepToolName = stepToolName.Substring("Test_".Length);
                ResetPluginLoaderErrorCount(stepToolName);
                ShowTrace("Loading the resourcer for tool " + stepToolName);

                toolResourcer = mPluginLoader.GetAnalysisResources(stepToolName.ToLower());
            }

            if (toolResourcer == null)
            {
                return false;
            }

            if (mDebugLevel > 0)
            {
                LogMessage("Loaded resourcer for StepTool " + stepToolName);
            }

            try
            {
                toolResourcer.Setup(stepToolName, mMgrParams, mAnalysisTask, mStatusTools, mMyEMSLUtilities);
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
            var stepToolName = mAnalysisTask.GetParam("StepTool");
            ResetPluginLoaderErrorCount(stepToolName);
            ShowTrace("Loading the ToolRunner for tool " + stepToolName);

            toolRunner = mPluginLoader.GetToolRunner(stepToolName.ToLower());

            if (toolRunner == null && stepToolName.StartsWith("Test_", StringComparison.OrdinalIgnoreCase))
            {
                stepToolName = stepToolName.Substring("Test_".Length);
                ResetPluginLoaderErrorCount(stepToolName);
                ShowTrace("Loading the ToolRunner for tool " + stepToolName);

                toolRunner = mPluginLoader.GetToolRunner(stepToolName.ToLower());
            }

            if (toolRunner == null)
            {
                return false;
            }

            if (mDebugLevel > 0)
            {
                LogMessage("Loaded tool runner for StepTool " + mAnalysisTask.GetCurrentJobToolDescription());
            }

            try
            {
                // Set up the new tool runner
                toolRunner.Setup(stepToolName, mMgrParams, mAnalysisTask, mStatusTools, mSummaryFile, mMyEMSLUtilities);
            }
            catch (Exception ex)
            {
                LogError("Error calling toolRunner.Setup()", ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Show a trace message only if TraceMode is true
        /// </summary>
        /// <param name="message">Trace message</param>
        /// <param name="emptyLinesBeforeMessage">Number of empty lines to display before showing the message</param>
        private void ShowTrace(string message, int emptyLinesBeforeMessage = 1)
        {
            if (!TraceMode)
                return;

            ShowTraceMessage(message, emptyLinesBeforeMessage);
        }

        /// <summary>
        /// Show a message at the console, preceded by a time stamp
        /// </summary>
        /// <param name="message">Trace message</param>
        /// <param name="emptyLinesBeforeMessage">Number of empty lines to display before showing the message</param>
        public static void ShowTraceMessage(string message, int emptyLinesBeforeMessage = 1)
        {
            BaseLogger.ShowTraceMessage(message, false, "  ", emptyLinesBeforeMessage);
        }

        /// <summary>
        /// Return true if the result code indicates a step tool was skipped,
        /// or if waiting for another job to finish creating a file that this job needs
        /// </summary>
        /// <param name="resultCode">Result code</param>
        private bool SkippedStepTool(CloseOutType resultCode)
        {
            return resultCode is
                CloseOutType.CLOSEOUT_SKIPPED_DIA_NN_SPEC_LIB or
                CloseOutType.CLOSEOUT_SKIPPED_MAXQUANT or
                CloseOutType.CLOSEOUT_SKIPPED_MSXML_GEN or
                CloseOutType.CLOSEOUT_SKIPPED_MZ_REFINERY or
                CloseOutType.CLOSEOUT_WAITING_FOR_DIA_NN_SPEC_LIB;
        }

        /// <summary>
        /// Look for flagFile.txt in the .exe directory
        /// Auto clean errors if AutoCleanupManagerErrors is enabled
        /// </summary>
        /// <returns>True if a flag file exists, false if safe to proceed</returns>
        private bool StatusFlagFileError()
        {
            bool mgrCleanupSuccess;

            if (!mMgrErrorCleanup.DetectStatusFlagFile())
            {
                // No error; return false
                return false;
            }

            try
            {
                mgrCleanupSuccess = mMgrErrorCleanup.AutoCleanupManagerErrors(GetManagerErrorCleanupMode(), mDebugLevel);
            }
            catch (Exception ex)
            {
                LogError("Error calling AutoCleanupManagerErrors", ex);
                mStatusTools.UpdateIdle("Error encountered", "MainProcess.StatusFlagFileError(): " + ex.Message, mMostRecentJobInfo, true);

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
            var flagFile = new FileInfo(mMgrErrorCleanup.FlagFilePath);
            string errorMessage;

            if (flagFile.Directory == null)
            {
                errorMessage = "Flag file exists in the manager directory";
            }
            else
            {
                errorMessage = "Flag file exists in directory " + flagFile.Directory.Name;
            }

            // Post a log entry to the database every 4 hours
            LogErrorToDatabasePeriodically(errorMessage, 4);

            // Return true (indicating a flag file exists)
            return true;
        }

        /// <summary>
        /// Updates status file to indicate that the manager is closing
        /// </summary>
        /// <param name="managerCloseMessage">Manager close message</param>
        private void UpdateClose(string managerCloseMessage)
        {
            var recentErrorMessages = DetermineRecentErrorMessages(5, ref mMostRecentJobInfo);

            mStatusTools.UpdateClose(managerCloseMessage, recentErrorMessages, mMostRecentJobInfo, true);
        }

        /// <summary>
        /// Reloads the manager settings from the manager control database
        /// if at least MinutesBetweenUpdates minutes have elapsed since the last update
        /// </summary>
        /// <param name="lastConfigDBUpdate">Last config DB update time</param>
        /// <param name="minutesBetweenUpdates">Minimum elapsed time, in minutes</param>
        private bool UpdateManagerSettings(ref DateTime lastConfigDBUpdate, double minutesBetweenUpdates)
        {
            if (DateTime.UtcNow.Subtract(lastConfigDBUpdate).TotalMinutes < minutesBetweenUpdates)
                return true;

            lastConfigDBUpdate = DateTime.UtcNow;

            ShowTrace("Loading manager settings from the manager control DB");

            if (!mMgrParams.LoadDBSettings())
            {
                string msg;

                if (string.IsNullOrWhiteSpace(mMgrParams.ErrMsg))
                {
                    msg = "Error calling mMgrParams.LoadMgrSettingsFromDB to update manager settings";
                }
                else
                {
                    msg = mMgrParams.ErrMsg;
                }

                LogError(msg);

                return false;
            }

            // Need to synchronize some of the settings
            UpdateStatusToolLoggingSettings(mStatusTools);

            return true;
        }

        private void UpdateStatusDisabled(MgrStatusCodes managerStatus, string managerDisableMessage)
        {
            var recentErrorMessages = DetermineRecentErrorMessages(5, ref mMostRecentJobInfo);
            mStatusTools.UpdateDisabled(managerStatus, managerDisableMessage, recentErrorMessages, mMostRecentJobInfo);
            ConsoleMsgUtils.ShowWarning(managerDisableMessage);
            Console.WriteLine();
        }

        private void UpdateStatusFlagFileExists()
        {
            var recentErrorMessages = DetermineRecentErrorMessages(5, ref mMostRecentJobInfo);
            mStatusTools.UpdateFlagFileExists(recentErrorMessages, mMostRecentJobInfo);
            Console.WriteLine("Flag file exists");
        }

        private void UpdateStatusIdle(string managerIdleMessage)
        {
            ShowTrace("Manager is idle");

            CheckStopTrace("DetermineRecentErrorMessages");
            var recentErrorMessages = DetermineRecentErrorMessages(5, ref mMostRecentJobInfo);

            CheckStopTrace("UpdateIdle");
            mStatusTools.UpdateIdle(managerIdleMessage, recentErrorMessages, mMostRecentJobInfo, true);
        }

        private void UpdateStatusToolLoggingSettings(StatusFile statusFile)
        {
            var logMemoryUsage = mMgrParams.GetParam("LogMemoryUsage", false);
            float minimumMemoryUsageLogInterval = mMgrParams.GetParam("MinimumMemoryUsageLogInterval", 1);

            // Analysis managers typically have logStatusToBrokerDb = false and logStatusToMessageQueue = true
            var logStatusToBrokerDb = mMgrParams.GetParam("LogStatusToBrokerDB", false);

            // SQL Server: Data Source=Gigasax;Initial Catalog=DMS_Pipeline
            // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
            var brokerDbConnectionString = mMgrParams.GetParam("BrokerConnectionString");
            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(brokerDbConnectionString, mMgrName);

            float brokerDbStatusUpdateIntervalMinutes = mMgrParams.GetParam("BrokerDBStatusUpdateIntervalMinutes", 60);

            var logStatusToMessageQueue = mMgrParams.GetParam("LogStatusToMessageQueue", false);

            if (DisableMessageQueue)
            {
                // Command line has switch /NQ
                // Disable message queue logging
                logStatusToMessageQueue = false;
            }

            var messageQueueUri = mMgrParams.GetParam("MessageQueueURI");
            var messageQueueTopicMgrStatus = mMgrParams.GetParam("MessageQueueTopicMgrStatus");

            statusFile.ConfigureMemoryLogging(logMemoryUsage, minimumMemoryUsageLogInterval, mMgrDirectoryPath);
            statusFile.ConfigureBrokerDBLogging(logStatusToBrokerDb, connectionStringToUse, brokerDbStatusUpdateIntervalMinutes);
            statusFile.ConfigureMessageQueueLogging(logStatusToMessageQueue, messageQueueUri, messageQueueTopicMgrStatus);
        }

        /// <summary>
        /// Confirms that the drive with the working directory has sufficient free space
        /// Confirms that the remote share for storing results is accessible and has sufficient free space
        /// </summary>
        /// <remarks>Disables the manager if the working directory drive does not have enough space</remarks>
        /// <param name="toolResourcer">Tool resourcer instance</param>
        /// <param name="errorMessage">Output: error message (empty string if no issues)</param>
        /// <param name="directoryWithInsufficientSpace">Output: path to the directory with insufficient space (empty string if no issues)</param>
        private bool ValidateFreeDiskSpace(IAnalysisResources toolResourcer, out string errorMessage, out string directoryWithInsufficientSpace)
        {
            const int DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB = 10;
            const int DEFAULT_TRANSFER_DIR_MIN_FREE_SPACE_GB = 10;

            const int DEFAULT_WORKING_DIR_MIN_FREE_SPACE_MB = 750;
            const int DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB = 2048;

            const string CACHE_FOLDER = "cache folder";

            errorMessage = string.Empty;

            try
            {
                var stepToolNameLCase = mAnalysisTask.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "StepTool").ToLower();

                if (stepToolNameLCase == "results_transfer")
                {
                    // We only need to evaluate the dataset storage directory for free space

                    var datasetStoragePath = mAnalysisTask.GetParam("DatasetStoragePath");
                    var datasetStorageMinFreeSpaceGB = mMgrParams.GetParam("DatasetStorageMinFreeSpaceGB", DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB);

                    if (string.IsNullOrEmpty(datasetStoragePath))
                    {
                        var datasetName = AnalysisResources.GetDatasetName(mAnalysisTask);

                        if (Global.IsMatch(datasetName, AnalysisResources.AGGREGATION_JOB_DATASET))
                        {
                            datasetStoragePath = mAnalysisTask.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_DATA_PACKAGE_PATH);

                            if (string.IsNullOrEmpty(datasetStoragePath))
                            {
                                errorMessage = "DataPackagePath job parameter is empty";
                                LogError(errorMessage);
                                directoryWithInsufficientSpace = string.Empty;
                                return false;
                            }
                        }
                    }

                    if (string.IsNullOrEmpty(datasetStoragePath))
                    {
                        errorMessage = "DatasetStoragePath job parameter is empty";
                        LogError(errorMessage);
                        directoryWithInsufficientSpace = string.Empty;
                        return false;
                    }

                    var datasetStorageDirectory = new DirectoryInfo(datasetStoragePath);

                    if (!datasetStorageDirectory.Exists)
                    {
                        // Dataset directory not found; that's OK, since the Results Transfer plugin will auto-create it
                        // Try to use the parent directory (or the parent of the parent)
                        while (!datasetStorageDirectory.Exists && datasetStorageDirectory.Parent != null)
                        {
                            datasetStorageDirectory = datasetStorageDirectory.Parent;
                        }

                        datasetStoragePath = datasetStorageDirectory.FullName;
                    }

                    var sufficientSpace = ValidateFreeDiskSpaceWork("Dataset directory", datasetStoragePath, datasetStorageMinFreeSpaceGB * 1024, out errorMessage);

                    if (sufficientSpace)
                    {
                        directoryWithInsufficientSpace = string.Empty;
                        return true;
                    }

                    directoryWithInsufficientSpace = datasetStoragePath;
                    return false;
                }

                var workingDirMinFreeSpaceMB = mMgrParams.GetParam("WorkDirMinFreeSpaceMB", DEFAULT_WORKING_DIR_MIN_FREE_SPACE_MB);

                // The CacheFolderRootPath parameter is normally in section JobParameters, but could be in section MSFragger or FragPipe
                // Thus, search for the parameter in any section
                var cacheFolderRootPath = mAnalysisTask.GetParam(AnalysisResources.JOB_PARAM_CACHE_FOLDER_ROOT_PATH);

                var transferDir = mAnalysisTask.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_TRANSFER_DIRECTORY_PATH);
                var transferDirMinFreeSpaceGB = mMgrParams.GetParam("TransferDirMinFreeSpaceGB", DEFAULT_TRANSFER_DIR_MIN_FREE_SPACE_GB);

                var orgDbDir = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);
                var orgDbDirMinFreeSpaceMB = mMgrParams.GetParam("OrgDBDirMinFreeSpaceMB", DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB);

                ShowTrace("Validating free space for the working directory: " + mWorkDirPath);

                // Verify that the working directory exists and that its drive has sufficient free space
                if (!ValidateFreeDiskSpaceWork("Working directory", mWorkDirPath, workingDirMinFreeSpaceMB, out errorMessage, true))
                {
                    LogError("Disabling manager since working directory problem; see " + mWorkDirPath);
                    DisableManagerLocally();
                    directoryWithInsufficientSpace = mWorkDirPath;
                    return false;
                }

                if (!Global.OfflineMode)
                {
                    string directoryDescription;

                    if (string.IsNullOrEmpty(transferDir))
                    {
                        if (!string.IsNullOrWhiteSpace(cacheFolderRootPath))
                        {
                            LogWarning("Transfer directory parameter for the job is empty; but the cache folder parameter is defined ({0})", cacheFolderRootPath);
                            transferDir = cacheFolderRootPath;
                            directoryDescription = CACHE_FOLDER;
                        }
                        else
                        {
                            errorMessage = "Transfer directory parameter for the job is empty; cannot continue";

                            if (DataPackageIdMissing())
                            {
                                errorMessage += ". Data package ID cannot be 0 for this job type";
                            }

                            LogError(errorMessage);
                            directoryWithInsufficientSpace = string.Empty;
                            return false;
                        }
                    }
                    else
                    {
                        var targetDirectory = new DirectoryInfo(transferDir);

                        if (targetDirectory.Exists)
                        {
                            directoryDescription = "transfer directory";
                        }
                        else if (!string.IsNullOrWhiteSpace(cacheFolderRootPath))
                        {
                            LogWarning("Transfer directory parameter refers to a folder that does not exist ({0}); will instead use the cache folder parameter ({1})", transferDir, cacheFolderRootPath);
                            transferDir = cacheFolderRootPath;
                            directoryDescription = CACHE_FOLDER;
                        }
                        else
                        {
                            directoryDescription = "transfer directory";
                        }
                    }

                    // Validating free space for the transfer directory
                    // Validating free space for the cache folder
                    ShowTrace(string.Format("Validating free space for the {0}: {1}", directoryDescription, transferDir));

                    // Verify that the remote transfer directory exists and that its drive has sufficient free space
                    if (!ValidateFreeDiskSpaceWork("Transfer directory", transferDir, transferDirMinFreeSpaceGB * 1024, out errorMessage))
                    {
                        directoryWithInsufficientSpace = transferDir;
                        return false;
                    }

                    if (directoryDescription.Equals(CACHE_FOLDER))
                    {
                        // Override the transfer directory job parameter
                        mAnalysisTask.SetParam(AnalysisResources.JOB_PARAM_TRANSFER_DIRECTORY_PATH, cacheFolderRootPath);

                        LogMessage("Updated the transfer directory parameter to use the cache folder path: {0}", cacheFolderRootPath);
                    }
                }

                var orgDbRequired = toolResourcer.GetOption(Global.AnalysisResourceOptions.OrgDbRequired);

                if (orgDbRequired)
                {
                    // Verify that the local FASTA file cache directory has sufficient free space

                    ShowTrace("Validating free space for the Org DB directory: " + orgDbDir);

                    if (!ValidateFreeDiskSpaceWork("Organism DB directory", orgDbDir, orgDbDirMinFreeSpaceMB, out errorMessage))
                    {
                        DisableManagerLocally();
                        directoryWithInsufficientSpace = orgDbDir;
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception validating free space", ex);
                directoryWithInsufficientSpace = string.Empty;
                return false;
            }

            directoryWithInsufficientSpace = string.Empty;
            return true;
        }

        private bool ValidateFreeDiskSpaceWork(
            string directoryDescription,
            string directoryPath,
            int minFreeSpaceMB,
            out string errorMessage,
            bool logToDatabase = false)
        {
            return mDirectorySpaceTools.ValidateFreeDiskSpace(directoryDescription, directoryPath, minFreeSpaceMB, out errorMessage, logToDatabase);
        }

        /// <summary>
        /// Validate the SQLite DLL and interop.so file
        /// (only called if Global.LinuxOS is true)
        /// </summary>
        /// <remarks>
        /// Looks for subdirectories named SQLite_1.0.108
        /// Compares the file size of System.Data.SQLite.dll and libSQLite.Interop.so
        /// in the subdirectory with the newest version with the versions in the manager directory
        /// </remarks>
        private void ValidateSQLiteDLL()
        {
            const string SQLITE_DLL = "System.Data.SQLite.dll";
            const string SQLITE_INTEROP_FILE = "libSQLite.Interop.so";

            try
            {
                var activeDll = new FileInfo(Path.Combine(mMgrDirectoryPath, SQLITE_DLL));

                var mgrDir = new DirectoryInfo(mMgrDirectoryPath);

                // Find the newest SQLite_1.*.* directory
                // For example, SQLite_1.0.108
                var sourceDirs = mgrDir.GetDirectories("SQLite_1.*.*");

                if (sourceDirs.Length == 0)
                {
                    LogWarning("Did not find any SQLite_1.x.y directories in the manager directory; " +
                               "cannot confirm that {0} is up-to-date for this OS", SQLITE_DLL);

                    return;
                }

                var sourceDirVersions = new Dictionary<Version, DirectoryInfo>();

                var sortedVersions = new SortedSet<Version>();

                var versionMatcher = new Regex(@"(?<Major>\d+)\.(?<Minor>\d+)\.(?<Revision>\d+)$", RegexOptions.Compiled);

                foreach (var sourceDir in sourceDirs)
                {
                    var match = versionMatcher.Match(sourceDir.Name);

                    if (!match.Success)
                    {
                        LogDebug("Ignoring {0} since did not end with a version of the form X.Y.Z", sourceDir.Name);

                        continue;
                    }

                    var sourceDirVersion = new Version(
                        int.Parse(match.Groups["Major"].Value),
                        int.Parse(match.Groups["Minor"].Value),
                        int.Parse(match.Groups["Revision"].Value));

                    if (sortedVersions.Contains(sourceDirVersion))
                        continue;

                    sourceDirVersions.Add(sourceDirVersion, sourceDir);
                    sortedVersions.Add(sourceDirVersion);
                }

                if (sourceDirVersions.Count == 0)
                {
                    LogWarning("Did not find any SQLite_1.x.y directories in the manager directory; " +
                               "cannot confirm that {0} is up-to-date for this OS", SQLITE_DLL);

                    return;
                }

                // Select the newest SQLite_1.*.* directory
                var newestSrcDir = sourceDirVersions[sortedVersions.Last()];

                var newestDll = new FileInfo(Path.Combine(newestSrcDir.FullName, SQLITE_DLL));

                if (!newestDll.Exists)
                {
                    LogWarning(
                        "{0} is missing file {1}; cannot validate against the DLL in the manager directory",
                        newestSrcDir.FullName, newestDll.Name);

                    return;
                }

                if (!activeDll.Exists || activeDll.Length != newestDll.Length)
                {
                    // File sizes differ; replace the active DLL
                    LogMessage("Copying {0} to {1}", newestDll.FullName, mgrDir.FullName);
                    newestDll.CopyTo(activeDll.FullName, true);
                }

                // Also look for a .so file below the SQLite_1.*.* directory

                var soFiles = newestSrcDir.GetFiles(SQLITE_INTEROP_FILE, SearchOption.AllDirectories);

                if (soFiles.Length == 0)
                {
                    LogWarning(
                        "{0} is missing file {1}; cannot validate against the .so file in the manager directory",
                        newestSrcDir.FullName, SQLITE_INTEROP_FILE);

                    return;
                }

                if (soFiles.Length > 1)
                {
                    LogWarning(
                        "Found multiple {0} files in {1} (including subdirectories); cannot validate against the .so file in the manager directory",
                        SQLITE_INTEROP_FILE, newestSrcDir.FullName);

                    return;
                }

                var activeInterop = new FileInfo(Path.Combine(mgrDir.FullName, SQLITE_INTEROP_FILE));
                var newestInterop = soFiles.First();

                if (!activeInterop.Exists || activeInterop.Length != newestInterop.Length)
                {
                    // File sizes differ; replace the active Interop.so file
                    LogMessage("Copying {0} to {1}", newestInterop.FullName, mgrDir.FullName);
                    newestInterop.CopyTo(activeInterop.FullName, true);
                }
            }
            catch (Exception ex)
            {
                LogError("Exception validating System.Data.SQLite.dll", ex);
            }
        }

        /// <summary>
        /// Verifies working directory is properly specified and is empty
        /// </summary>
        private bool ValidateWorkingDir()
        {
            // Verify working directory is valid
            if (!VerifyWorkDir())
            {
                return false;
            }

            // Verify the working directory is empty
            var workDir = new DirectoryInfo(mWorkDirPath);
            var workDirFiles = workDir.GetFiles();
            var workDirDirectories = workDir.GetDirectories();

            if (workDirDirectories.Length == 0 && workDirFiles.Length == 1)
            {
                // If the only file in the working directory is a job parameters XML file, try to delete it;
                // it is likely left over from a previous job that never actually started
                var firstFile = workDirFiles.First();

                if (firstFile.Name.StartsWith(Global.JOB_PARAMETERS_FILE_PREFIX, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(Path.GetExtension(firstFile.Name), ".xml", StringComparison.OrdinalIgnoreCase))
                {
                    try
                    {
                        LogWarning("Working directory contains a stray job parameters file, deleting it: " + firstFile.FullName);

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

            var errorCount = workDirFiles.Count(item => !FileTools.IsVimSwapFile(item.FullName));

            if (errorCount == 0)
            {
                // No problems found
                return true;
            }

            LogError("Working directory not empty: " + mWorkDirPath);
            return false;
        }

        private bool VerifyWorkDir()
        {
            // Verify working directory is valid
            if (!Directory.Exists(mWorkDirPath))
            {
                LogError("Invalid working directory: " + mWorkDirPath);
                Global.IdleLoop(1.5);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Check whether the computer is likely to install the monthly Windows Updates within the next few hours
        /// </summary>
        /// <returns>True if Windows updates are pending</returns>
        private bool WindowsUpdatesArePending()
        {
            if (!WindowsUpdateStatus.UpdatesArePending(out var pendingWindowsUpdateMessage))
                return false;

            LogMessage(pendingWindowsUpdateMessage);
            UpdateStatusIdle(pendingWindowsUpdateMessage);
            return true;
        }

        private void CriticalErrorEvent(string message, Exception ex)
        {
            LogError(message, true);
        }

        /// <summary>
        /// Logs the first error while loading a plugin as an error
        /// Subsequent errors are logged as warnings
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="ex">Exception (can be null)</param>
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

        private void RemoteMonitor_StaleLockFileEvent(string fileName, int ageHours)
        {
            var msg = string.Format("Stale remote lock file for {0}; {1} last modified {2} hours ago",
                                    mAnalysisTask.GetJobStepDescription(), fileName, ageHours);

            LogErrorToDatabasePeriodically(msg, 12);
        }

        private void RemoteMonitor_StaleJobStatusFileEvent(string fileName, int ageHours)
        {
            var msg = string.Format("Stale remote status file for {0}; {1} last modified {2} hours ago",
                                    mAnalysisTask.GetJobStepDescription(), fileName, ageHours);

            LogErrorToDatabasePeriodically(msg, 12);
        }

        /// <summary>
        /// Event handler for file watcher
        /// </summary>
        /// <param name="sender">Sender</param>
        /// <param name="e">File system event arguments</param>
        private void ConfigFileWatcher_Changed(object sender, FileSystemEventArgs e)
        {
            mConfigFileWatcher.EnableRaisingEvents = false;

            if (mLocalSettingsFileWatcher != null)
                mLocalSettingsFileWatcher.EnableRaisingEvents = false;

            mConfigChanged = true;

            if (mDebugLevel > 3)
            {
                LogDebug("Config file changed");
            }
        }
    }
}
