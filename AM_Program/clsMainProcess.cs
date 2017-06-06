//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
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
using System.Threading;
using System.Xml;
using AnalysisManagerBase;
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

        /// <summary>
        /// This is used to create the Windows Event log (aka the EmergencyLog) that this program rights to
        ///  when the manager is disabled or cannot make an entry in the log file
        /// </summary>
        private const string CUSTOM_LOG_SOURCE_NAME = "Analysis Manager";

        /// <summary>
        /// Windows application log name for the analysis manager
        /// </summary>
        public const string CUSTOM_LOG_NAME = "DMS_AnalysisMgr";

        private const int MAX_ERROR_COUNT = 10;
        private const string DECON2LS_FATAL_REMOTING_ERROR = "Fatal remoting error";
        private const string DECON2LS_CORRUPTED_MEMORY_ERROR = "Corrupted memory error";
        private const string DECON2LS_TCP_ALREADY_REGISTERED_ERROR = "channel 'tcp' is already registered";
        #endregion

        #region "Member variables"

        // clsAnalysisMgrSettings
        private IMgrParams m_MgrSettings;

        private clsCleanupMgrErrors m_MgrErrorCleanup;
        private readonly string m_MgrExeName;
        private readonly string m_MgrFolderPath;
        private string m_WorkDirPath;

        private string m_MgrName = "??";

        // clsAnalysisJob
        private IJobParams m_AnalysisTask;
        private clsPluginLoader m_PluginLoader;

        private clsSummaryFile m_SummaryFile;
        private FileSystemWatcher m_ConfigFileWatcher;
        private FileSystemWatcher m_LocalSettingsFileWatcher;

        private bool m_ConfigChanged;

        private clsStatusFile m_StatusTools;

        private clsMyEMSLUtilities m_MyEMSLUtilities;
        private bool m_NeedToAbortProcessing;

        private string m_MostRecentJobInfo;

        private string m_MostRecentErrorMessage = string.Empty;

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
        /// When true, show additional trace log messages
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
                if (TraceMode)
                    ShowTraceMessage("Initializing the manager");

                if (!InitMgr())
                {
                    if (TraceMode)
                        ShowTraceMessage("InitMgr returned false; aborting");
                    return -1;
                }

                if (TraceMode)
                    ShowTraceMessage("Call DoAnalysis");

                DoAnalysis();

                if (TraceMode)
                    ShowTraceMessage("Exiting clsMainProcess.Main with error code = 0");
                return 0;
            }
            catch (Exception ex)
            {
                // Report any exceptions not handled at a lower level to the system application log
                var errMsg = "Critical exception starting application: " + ex.Message;
                if (TraceMode)
                    ShowTraceMessage(errMsg + "; " + clsGlobal.GetExceptionStackTrace(ex, true));
                PostToEventLog(errMsg + "; " + clsGlobal.GetExceptionStackTrace(ex, false));
                if (TraceMode)
                    ShowTraceMessage("Exiting clsMainProcess.Main with error code = 1");
                return 1;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public clsMainProcess(bool blnTraceModeEnabled)
        {
            TraceMode = blnTraceModeEnabled;
            m_ConfigChanged = false;
            m_DebugLevel = 0;
            m_NeedToAbortProcessing = false;
            m_MostRecentJobInfo = string.Empty;

            var exeName = Path.GetFileName(Assembly.GetExecutingAssembly().Location);
            if (exeName == null)
                throw new Exception("Unable to determine the Exe path of the currently executing assembly");

            var fiMgr = new FileInfo(exeName);
            m_MgrExeName = fiMgr.Name;
            m_MgrFolderPath = fiMgr.DirectoryName;
        }

        private static void ConfirmWindowsEventLog()
        {
            // Confirm that the application event log exists
            if (EventLog.SourceExists(CUSTOM_LOG_SOURCE_NAME))
                return;

            var sourceData = new EventSourceCreationData(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME);
            EventLog.CreateEventSource(sourceData);
        }

        /// <summary>
        /// Initializes the manager settings
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        /// <remarks></remarks>
        private bool InitMgr()
        {
            var hostName = System.Net.Dns.GetHostName();

            if (!clsGlobal.OfflineMode)
            {
                // Create a database logger connected to DMS5
                // Once the initial parameters have been successfully read,
                // we remove this logger than make a new one using the connection string read from the Manager Control DB
                var defaultDmsConnectionString = Properties.Settings.Default.DefaultDMSConnString;

                clsLogTools.CreateDbLogger(defaultDmsConnectionString, "Analysis Tool Manager: " + hostName, true);
            }

            try
            {
                if (TraceMode)
                    ShowTraceMessage("Reading application config file");

                var lstMgrSettings = LoadMgrSettingsFromFile();

                // Get the manager settings
                // If you get an exception here while debugging in Visual Studio, be sure
                //   that "UsingDefaults" is set to False in CaptureTaskManager.exe.config
                try
                {
                    if (TraceMode)
                    {
                        ShowTraceMessage("Instantiating clsAnalysisMgrSettings");
                        foreach (var setting in lstMgrSettings)
                            ShowTraceMessage(string.Format("  {0}: {1}", setting.Key, setting.Value));
                    }
                    m_MgrSettings = new clsAnalysisMgrSettings(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME, lstMgrSettings, m_MgrFolderPath, TraceMode);
                }
                catch (Exception ex)
                {
                    // Failures are logged by clsMgrSettings to application event logs;
                    //  this includes MgrActive_Local = False
                    //
                    // If the DMS_AnalysisMgr application log does not exist yet, the SysLogger will create it
                    // However, in order to do that, the program needs to be running from an elevated (administrative level) command prompt
                    // Thus, it is advisable to run this program once from an elevated command prompt while MgrActive_Local is set to false

                    Console.WriteLine();
                    Console.WriteLine("===============================================================");
                    Console.WriteLine("Exception instantiating clsAnalysisMgrSettings: " + ex.Message);
                    Console.WriteLine("===============================================================");
                    Console.WriteLine();
                    Console.WriteLine("You may need to start this application once from an elevated (administrative level) command prompt " +
                        "using the /EL switch so that it can create the " + CUSTOM_LOG_NAME + " application log");
                    Console.WriteLine();
                    Thread.Sleep(500);

                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine("===============================================================");
                Console.WriteLine("Exception loading settings from AnalysisManagerProg.exe.config: " + ex.Message);
                Console.WriteLine("===============================================================");
                Console.WriteLine();
                Thread.Sleep(500);
                return false;
            }

            m_MgrName = m_MgrSettings.ManagerName;
            if (TraceMode)
                ShowTraceMessage("Manager name is " + m_MgrName);

            // Delete any temporary files that may be left in the app directory
            RemoveTempFiles();

            if (!clsGlobal.OfflineMode)
            {
                ConfirmWindowsEventLog();
            }

            // Setup the loggers

            var logFileNameBase = GetBaseLogFileName();

            clsLogTools.CreateFileLogger(logFileNameBase);

            var logCnStr = m_MgrSettings.GetParam("connectionstring");

            if (!clsGlobal.OfflineMode)
            {
                clsLogTools.RemoveDefaultDbLogger();
                clsLogTools.CreateDbLogger(logCnStr, "Analysis Tool Manager: " + m_MgrName, false);
            }

            // Make the initial log entry
            if (TraceMode)
                ShowTraceMessage("Initializing log file " + clsLogTools.CurrentFileAppenderPath);

            var startupMsg = "=== Started Analysis Manager V" + System.Windows.Forms.Application.ProductVersion + " ===== ";
            LogMessage(startupMsg);

            var configFileName = m_MgrSettings.GetParam("configfilename");
            if ((string.IsNullOrEmpty(configFileName)))
            {
                //  Manager parameter error; log an error and exit
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
            m_DebugLevel = (short)(m_MgrSettings.GetParam("debuglevel", 2));

            // Make sure that the manager name matches the machine name (with a few exceptions)

            if (!hostName.StartsWith("emslmq", StringComparison.InvariantCultureIgnoreCase) &&
                !hostName.StartsWith("emslpub", StringComparison.InvariantCultureIgnoreCase) &&
                !hostName.StartsWith("monroe", StringComparison.InvariantCultureIgnoreCase))
            {
                if (!m_MgrName.StartsWith(hostName, StringComparison.InvariantCultureIgnoreCase))
                {
                    LogError("Manager name does not match the host name: " + m_MgrName + " vs. " + hostName + "; update AnalysisManagerProg.exe.config");
                    return false;
                }
            }

            // Setup the tool for getting tasks
            if (TraceMode)
                ShowTraceMessage("Instantiate m_AnalysisTask as new clsAnalysisJob");
            m_AnalysisTask = new clsAnalysisJob(m_MgrSettings, m_DebugLevel);

            m_WorkDirPath = m_MgrSettings.GetParam("workdir");

            // Setup the manager cleanup class
            if (TraceMode)
                ShowTraceMessage("Setup the manager cleanup class");

            m_MgrErrorCleanup = new clsCleanupMgrErrors(m_MgrSettings.GetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING), m_MgrName, m_DebugLevel, m_MgrFolderPath, m_WorkDirPath);

            if (TraceMode)
                ShowTraceMessage("Initialize the Summary file");

            m_SummaryFile = new clsSummaryFile();
            m_SummaryFile.Clear();

            if (TraceMode)
                ShowTraceMessage("Initialize the Plugin Loader");

            m_PluginLoader = new clsPluginLoader(m_SummaryFile, m_MgrFolderPath);

            // Everything worked
            return true;
        }

        /// <summary>
        /// Loop to perform all analysis jobs
        /// </summary>
        /// <remarks></remarks>
        public void DoAnalysis()
        {
            if (TraceMode)
                ShowTraceMessage("Entering clsMainProcess.DoAnalysis");

            var loopCount = 0;
            var tasksStartedCount = 0;
            var errorDeletingFilesFlagFile = false;

            var dtLastConfigDBUpdate = DateTime.UtcNow;

            // Used to track critical manager errors (not necessarily failed analysis jobs when the plugin reports "no results" or similar)
            var criticalMgrErrorCount = 0;
            var successiveDeadLockCount = 0;

            try
            {
                if (TraceMode)
                    ShowTraceMessage("Entering clsMainProcess.DoAnalysis Try/Catch block");

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

                        if (TraceMode)
                            ShowTraceMessage("Reloading manager settings since config file has changed");

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
                    var mgrActive = m_MgrSettings.GetParam("mgractive", false);
                    var mgrActiveLocal = m_MgrSettings.GetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, false);

                    if (!(mgrActive && mgrActiveLocal))
                    {
                        string strManagerDisableReason;
                        if (!mgrActiveLocal)
                        {
                            strManagerDisableReason = "Disabled locally via AnalysisManagerProg.exe.config";
                            UpdateStatusDisabled(EnumMgrStatus.DISABLED_LOCAL, strManagerDisableReason);
                        }
                        else
                        {
                            strManagerDisableReason = "Disabled in Manager Control DB";
                            UpdateStatusDisabled(EnumMgrStatus.DISABLED_MC, strManagerDisableReason);
                        }

                        LogMessage("Manager inactive: " + strManagerDisableReason);
                        Thread.Sleep(750);
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

                    // Verify that the working directory exists
                    if (!VerifyWorkDir())
                    {
                        return;
                    }

                    // Verify that an error hasn't left the the system in an odd state
                    if (StatusFlagFileError())
                    {
                        LogError("Flag file exists - unable to perform any further analysis jobs");
                        UpdateStatusFlagFileExists();
                        Thread.Sleep(1500);
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

                    // Check whether the computer is likely to install the monthly Windows Updates within the next few hours
                    // Do not request a job between 12 am and 6 am on Thursday in the week with the second Tuesday of the month
                    // Do not request a job between 2 am and 4 am or between 9 am and 11 am on Sunday in the week with the second Tuesday of the month
                    string pendingWindowsUpdateMessage;
                    if (clsWindowsUpdateStatus.UpdatesArePending(out pendingWindowsUpdateMessage))
                    {
                        LogMessage(pendingWindowsUpdateMessage);
                        UpdateStatusIdle(pendingWindowsUpdateMessage);
                        break;
                    }

                    if (TraceMode)
                        ShowTraceMessage("Requesting a new task from DMS_Pipeline");

                    // Re-initialize these utilities for each analysis job
                    m_MyEMSLUtilities = new clsMyEMSLUtilities(m_DebugLevel, m_WorkDirPath);
                    RegisterEvents(m_MyEMSLUtilities);

                    // Get an analysis job, if any are available

                    var taskReturn = m_AnalysisTask.RequestTask();

                    switch (taskReturn)
                    {
                        case clsDBTask.RequestTaskResult.NoTaskFound:
                            if (TraceMode)
                                ShowTraceMessage("No tasks found");

                            // No tasks found
                            if (m_DebugLevel >= 3)
                            {
                                LogMessage("No analysis jobs found");
                            }
                            requestJobs = false;
                            criticalMgrErrorCount = 0;
                            UpdateStatusIdle("No analysis jobs found");

                            break;
                        case clsDBTask.RequestTaskResult.ResultError:
                            if (TraceMode)
                                ShowTraceMessage("Error requesting a task");

                            // There was a problem getting the task; errors were logged by RequestTaskResult
                            criticalMgrErrorCount += 1;

                            break;
                        case clsDBTask.RequestTaskResult.TaskFound:

                            if (TraceMode)
                                ShowTraceMessage("Task found");

                            tasksStartedCount += 1;
                            successiveDeadLockCount = 0;

                            try
                            {
                                oneTaskStarted = true;
                                if (DoAnalysisJob())
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
                                    else
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
                            if (TraceMode)
                                ShowTraceMessage("Too many retries calling the stored procedure");

                            // There were too many retries calling the stored procedure; errors were logged by RequestTaskResult
                            // Bump up loopCount to the maximum to exit the loop
                            UpdateStatusIdle("Excessive retries requesting task");
                            loopCount = maxLoopCount;

                            break;
                        case clsDBTask.RequestTaskResult.Deadlock:

                            if (TraceMode)
                                ShowTraceMessage("Deadlock");

                            // A deadlock error occured
                            // Query the DB again, but only if we have not had 3 deadlock results in a row
                            successiveDeadLockCount += 1;
                            if (successiveDeadLockCount >= 3)
                            {
                                var msg = "Deadlock encountered " + successiveDeadLockCount.ToString() + " times in a row when requesting a new task; exiting";
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
                        if (TraceMode)
                            ShowTraceMessage("Need to abort processing");
                        break;
                    }
                    loopCount += 1;

                    // If the only problem was deleting non result files, we want to stop the manager
                    if (m_MgrErrorCleanup.DetectErrorDeletingFilesFlagFile())
                    {
                        if (TraceMode)
                            ShowTraceMessage("Error deleting files flag file");
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
                            Thread.Sleep(1500);
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

                if (TraceMode)
                    ShowTraceMessage("Closing the manager");
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
                    if (TraceMode)
                        ShowTraceMessage("Disposing message queue via m_StatusTools.DisposeMessageQueue");
                    m_StatusTools.DisposeMessageQueue();
                }
            }
        }

        private bool DoAnalysisJob()
        {
            var jobNum = m_AnalysisTask.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0);
            var stepNum = m_AnalysisTask.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step", 0);
            var cpuLoadExpected = m_AnalysisTask.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "CPU_Load", 1);

            var datasetName = m_AnalysisTask.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetNum");
            var jobToolDescription = m_AnalysisTask.GetCurrentJobToolDescription();

            var runJobsRemotely = m_MgrSettings.GetParam("RunJobsRemotely", false);
            var runningRemoteFlag = m_AnalysisTask.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "RunningRemote", 0);
            var runningRemote = (runningRemoteFlag > 0);

            if (TraceMode)
                ShowTraceMessage("Processing job " + jobNum + ", " + jobToolDescription);

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
                if (TraceMode)
                    ShowTraceMessage("NeedToAbortProcessing; closing job step task");
                m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Processing aborted");
                m_MgrErrorCleanup.CleanWorkDir();
                UpdateStatusIdle("Processing aborted");
                return false;
            }

            // Make sure we have enough free space on the drive with the working directory and on the drive with the transfer folder
            if (!ValidateFreeDiskSpace(toolResourcer, out m_MostRecentErrorMessage))
            {
                if (TraceMode)
                    ShowTraceMessage("Insufficient free space; closing job step task");
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
                        if (TraceMode)
                            ShowTraceMessage("Error staging the job to run remotely; closing job step task");

                        if (string.IsNullOrEmpty(m_MostRecentErrorMessage))
                        {
                            m_MostRecentErrorMessage = "Unknown error staging the job to run remotely";
                        }
                        m_AnalysisTask.CloseTask(eToolRunnerResult, m_MostRecentErrorMessage, toolRunner.EvalCode, toolRunner.EvalMessage);
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
                if (TraceMode)
                    ShowTraceMessage("Task completed successfully; closing the job step task");

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
                m_AnalysisTask.CloseTask(closeOut, string.Empty, toolRunner.EvalCode, toolRunner.EvalMessage);
                LogMessage(m_MgrName + ": Completed job " + jobNum);

                UpdateStatusIdle("Completed job " + jobNum + ", step " + stepNum);

                var cleanupSuccess = CleanupAfterJob(jobSucceeded, runningRemote, remoteMonitor);

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
                            m_AnalysisTask.CloseTask(eToolRunnerResult, m_MostRecentErrorMessage, toolRunner.EvalCode, toolRunner.EvalMessage);
                        }
                        return success;

                    case clsRemoteMonitor.EnumRemoteJobStatus.Failed:

                        HandleRemoteJobFailure(toolRunner, remoteMonitor, out eToolRunnerResult);
                        m_AnalysisTask.CloseTask(eToolRunnerResult, m_MostRecentErrorMessage, toolRunner.EvalCode, toolRunner.EvalMessage);
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

        private bool CleanupAfterJob(bool jobSucceeded, bool runningRemote, clsRemoteMonitor remoteMonitor)
        {
            try
            {
                if (jobSucceeded && runningRemote)
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
                    Thread.Sleep(5000);

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

        /// <summary>
        /// Initialize the analysis manager application log,
        /// </summary>
        public static void CreateAnalysisManagerEventLog()
        {
            if (clsGlobal.LinuxOS)
            {
                Console.WriteLine("The Windows event log cannot be initialized on Linux (CreateAnalysisManagerEventLog)");
                return;
            }
            var blnSuccess = CreateAnalysisManagerEventLog(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME);

            if (blnSuccess)
            {
                Console.WriteLine();
                clsGlobal.LogDebug("Windows Event Log '" + CUSTOM_LOG_NAME + "' has been validated for source '" + CUSTOM_LOG_SOURCE_NAME + "'", false);
                Console.WriteLine();
            }
        }

        private static bool CreateAnalysisManagerEventLog(string SourceName, string LogName)
        {
            try
            {
                if (string.IsNullOrEmpty(SourceName))
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Error creating the Windows Event Log: SourceName cannot be blank");
                    Console.ResetColor();
                    return false;
                }

                if (string.IsNullOrEmpty(LogName))
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Error creating the Windows Event Log: LogName cannot be blank");
                    Console.ResetColor();
                    return false;
                }

                if (!EventLog.SourceExists(SourceName))
                {
                    clsGlobal.LogDebug("Creating Windows Event Log " + LogName + " for source " + SourceName, false);
                    var SourceData = new EventSourceCreationData(SourceName, LogName);
                    EventLog.CreateEventSource(SourceData);
                }

                // Create custom event logging object and update it's configuration
                var ELog = new EventLog
                {
                    Log = LogName,
                    Source = SourceName
                };

                try
                {
                    ELog.MaximumKilobytes = 1024;
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Warning: unable to update the maximum log size to 1024 KB: \n  " + ex.Message);
                    Console.ResetColor();
                }

                try
                {
                    ELog.ModifyOverflowPolicy(OverflowAction.OverwriteAsNeeded, 90);
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Warning: unable to update the overflow policy to keep events for 90 days and overwrite as needed: \n  " + ex.Message);
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Exception creating the Windows Event Log named '" + LogName + "' for source '" + SourceName + "': " + ex.Message);
                Console.ResetColor();
                return false;
            }

            return true;
        }

        private FileSystemWatcher CreateConfigFileWatcher(string configFileName)
        {
            var watcher = new FileSystemWatcher
            {
                Path = m_MgrFolderPath,
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

            var dataPkgRequired = multiJobStepTools.Any(multiJobTool => string.Equals(stepToolName, multiJobTool, StringComparison.InvariantCultureIgnoreCase));

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
        /// <param name="strLogFilePath"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private string DecrementLogFilePath(string strLogFilePath)
        {
            try
            {
                var reLogFileName = new Regex(@"(?<BaseName>.+_)(?<Month>\d+)-(?<Day>\d+)-(?<Year>\d+).\S+", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var objMatch = reLogFileName.Match(strLogFilePath);

                if (objMatch.Success)
                {
                    var intMonth = Convert.ToInt32(objMatch.Groups["Month"].Value);
                    var intDay = Convert.ToInt32(objMatch.Groups["Day"].Value);
                    var intYear = Convert.ToInt32(objMatch.Groups["Year"].Value);

                    var dtCurrentDate = DateTime.Parse(intYear + "-" + intMonth + "-" + intDay);
                    var dtNewDate = dtCurrentDate.Subtract(new TimeSpan(1, 0, 0, 0));

                    var strPreviousLogFilePath = objMatch.Groups["BaseName"].Value + dtNewDate.ToString("MM-dd-yyyy") + Path.GetExtension(strLogFilePath);
                    return strPreviousLogFilePath;
                }

            }
            catch (Exception ex)
            {
                LogError("Error in DecrementLogFilePath", ex);
            }

            return string.Empty;

        }

        /// <summary>
        /// Parses the log files for this manager to determine the recent error messages, returning up to intErrorMessageCountToReturn of them
        /// Will use objLogger to determine the most recent log file
        /// Also examines the message info stored in objLogger
        /// Lastly, if strMostRecentJobInfo is empty, then will update it with info on the most recent job started
        /// </summary>
        /// <param name="intErrorMessageCountToReturn">Maximum number of error messages to return</param>
        /// <param name="strMostRecentJobInfo">Info on the most recent job started by this manager</param>
        /// <returns>List of recent errors</returns>
        /// <remarks></remarks>
        public IEnumerable<string> DetermineRecentErrorMessages(int intErrorMessageCountToReturn, ref string strMostRecentJobInfo)
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

            if (strMostRecentJobInfo == null)
                strMostRecentJobInfo = string.Empty;

            try
            {
                var strMostRecentJobInfoFromLogs = string.Empty;

                if (intErrorMessageCountToReturn < 1)
                    intErrorMessageCountToReturn = 1;

                // Initialize the RegEx that splits out the timestamp from the error message
                var reErrorLine = new Regex(ERROR_MATCH_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var reJobStartLine = new Regex(JOB_START_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // Initialize the queue that holds recent error messages
                var qErrorMsgQueue = new Queue<string>(intErrorMessageCountToReturn);

                // Initialize the hashtable to hold the error messages, but without date stamps
                var uniqueErrorMessages = new Dictionary<string, DateTime>((StringComparer.InvariantCultureIgnoreCase));

                // Examine the most recent error reported by objLogger
                var strLineIn = clsLogTools.MostRecentErrorMessage;
                bool blnLoggerReportsError;
                if (!string.IsNullOrWhiteSpace(strLineIn))
                {
                    blnLoggerReportsError = true;
                }
                else
                {
                    blnLoggerReportsError = false;
                }

                var strLogFilePath = GetRecentLogFilename();

                if (intErrorMessageCountToReturn > 1 || !blnLoggerReportsError)
                {
                    // Recent error message reported by objLogger is empty or intErrorMessageCountToReturn is greater than one
                    // Open log file strLogFilePath to find the most recent error messages
                    // If not enough error messages are found, we will look through previous log files

                    var intLogFileCountProcessed = 0;
                    var blnCheckForMostRecentJob = true;

                    while (qErrorMsgQueue.Count < intErrorMessageCountToReturn && intLogFileCountProcessed < MAX_LOG_FILES_TO_SEARCH)
                    {
                        if (File.Exists(strLogFilePath))
                        {
                            var srInFile = new StreamReader(new FileStream(strLogFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                            if (intErrorMessageCountToReturn < 1)
                                intErrorMessageCountToReturn = 1;

                            while (!srInFile.EndOfStream)
                            {
                                strLineIn = srInFile.ReadLine();

                                if (strLineIn == null)
                                    continue;

                                var oMatchError = reErrorLine.Match(strLineIn);

                                if (oMatchError.Success)
                                {
                                    DetermineRecentErrorCacheError(oMatchError, strLineIn, uniqueErrorMessages, qErrorMsgQueue, intErrorMessageCountToReturn);
                                }

                                if (!blnCheckForMostRecentJob)
                                    continue;

                                var oMatchJob = reJobStartLine.Match(strLineIn);
                                if (!oMatchJob.Success)
                                    continue;

                                try
                                {
                                    strMostRecentJobInfoFromLogs = ConstructMostRecentJobInfoText(
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

                            srInFile.Close();

                            if (blnCheckForMostRecentJob && strMostRecentJobInfoFromLogs.Length > 0)
                            {
                                // We determine the most recent job; no need to check other log files
                                blnCheckForMostRecentJob = false;
                            }
                        }
                        // else: Log file not found; that's OK, we'll decrement the name by one day and keep checking

                        // Increment the log file counter, regardless of whether or not the log file was found
                        intLogFileCountProcessed += 1;

                        if (qErrorMsgQueue.Count >= intErrorMessageCountToReturn)
                            continue;

                        // We still haven't found intErrorMessageCountToReturn error messages
                        // Keep checking older log files as long as qErrorMsgQueue.Count < intErrorMessageCountToReturn

                        // Decrement the log file path by one day
                        strLogFilePath = DecrementLogFilePath(strLogFilePath);
                        if (string.IsNullOrEmpty(strLogFilePath))
                        {
                            break;
                        }
                    }
                }

                if (blnLoggerReportsError)
                {
                    // Append the error message reported by the Logger to the error message queue (treating it as the newest error)
                    strLineIn = clsLogTools.MostRecentErrorMessage;
                    var objMatch = reErrorLine.Match(strLineIn);

                    if (objMatch.Success)
                    {
                        DetermineRecentErrorCacheError(objMatch, strLineIn, uniqueErrorMessages, qErrorMsgQueue, intErrorMessageCountToReturn);
                    }
                }

                // Populate strRecentErrorMessages and dtRecentErrorMessageDates using the messages stored in qErrorMsgQueue
                while (qErrorMsgQueue.Count > 0)
                {
                    var strErrorMessageClean = qErrorMsgQueue.Dequeue();

                    // Find the newest timestamp for this message
                    DateTime timeStamp;
                    if (!uniqueErrorMessages.TryGetValue(strErrorMessageClean, out timeStamp))
                    {
                        // This code should not be reached
                        timeStamp = DateTime.MinValue;
                    }

                    recentErrorMessages.Add(new KeyValuePair<string, DateTime>(timeStamp + ", " + strErrorMessageClean.TrimStart(' '), timeStamp));
                }

                var sortedAndFilteredRecentErrors = (from item in recentErrorMessages
                                                     orderby item.Value descending
                                                     select item.Key).Take(intErrorMessageCountToReturn);

                if (string.IsNullOrEmpty(strMostRecentJobInfo))
                {
                    if (!string.IsNullOrWhiteSpace(strMostRecentJobInfoFromLogs))
                    {
                        // Update strMostRecentJobInfo
                        strMostRecentJobInfo = strMostRecentJobInfoFromLogs;
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
            Match oMatch,
            string strErrorMessage,
            IDictionary<string, DateTime> uniqueErrorMessages,
            Queue<string> qErrorMsgQueue,
            int intMaxErrorMessageCountToReturn)
        {
            DateTime timeStamp;
            string strErrorMessageClean;

            // See if this error is present in uniqueErrorMessages yet
            // If it is present, update the timestamp in uniqueErrorMessages
            // If not present, queue it

            if (oMatch.Groups.Count >= 2)
            {
                var strTimestamp = oMatch.Groups["Date"].Value;
                if (!DateTime.TryParse(strTimestamp, out timeStamp))
                    timeStamp = DateTime.MinValue;

                strErrorMessageClean = oMatch.Groups["Error"].Value;
            }
            else
            {
                // Regex didn't match; this is unexpected
                timeStamp = DateTime.MinValue;
                strErrorMessageClean = strErrorMessage;
            }

            // Check whether strErrorMessageClean is in the hash table
            DateTime existingTimeStamp;
            if (uniqueErrorMessages.TryGetValue(strErrorMessageClean, out existingTimeStamp))
            {
                // The error message is present
                // Update the timestamp associated with strErrorMessageClean if the time stamp is newer than the stored one
                try
                {
                    if (timeStamp > existingTimeStamp)
                    {
                        uniqueErrorMessages[strErrorMessageClean] = timeStamp;
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
                uniqueErrorMessages.Add(strErrorMessageClean, timeStamp);
            }

            if (qErrorMsgQueue.Contains(strErrorMessageClean))
                return;

            // Queue this message
            // However, if we already have intErrorMessageCountToReturn messages queued, then dequeue the oldest one

            if (qErrorMsgQueue.Count < intMaxErrorMessageCountToReturn)
            {
                qErrorMsgQueue.Enqueue(strErrorMessageClean);
            }
            else
            {
                // Too many queued messages, so remove oldest one
                // However, only do this if the new error message has a timestamp newer than the oldest queued message
                //  (this is a consideration when processing multiple log files)

                var blnAddItemToQueue = true;

                var strQueuedError = qErrorMsgQueue.Peek();

                // Get the timestamp associated with strQueuedError, as tracked by the hashtable
                DateTime queuedTimeStamp;
                if (!uniqueErrorMessages.TryGetValue(strQueuedError, out queuedTimeStamp))
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
                            blnAddItemToQueue = false;
                        }
                    }
                    catch (Exception)
                    {
                        // Date comparison failed; Do not add the new item to the queue
                        blnAddItemToQueue = false;
                    }
                }

                if (blnAddItemToQueue)
                {
                    qErrorMsgQueue.Dequeue();
                    qErrorMsgQueue.Enqueue(strErrorMessageClean);
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

            var logFileNameBase = m_MgrSettings.GetParam("logfilename");
            if (string.IsNullOrWhiteSpace(logFileNameBase))
                return "AnalysisMgr";

            return logFileNameBase;
        }

        private string GetRecentLogFilename()
        {
            string lastFilename;

            try
            {
                // Obtain a list of log files
                var logFileNameBase = GetBaseLogFileName();
                var files = Directory.GetFiles(m_MgrFolderPath, logFileNameBase + "*.txt");

                // Change the file names to lowercase (to assure that the sorting works)
                for (var x = 0; x <= files.Length - 1; x++)
                {
                    files[x] = files[x].ToLower();
                }

                // Sort the files by filename
                Array.Sort(files);

                // Return the last filename in the list
                lastFilename = files[files.Length - 1];
            }
            catch (Exception)
            {
                return string.Empty;
            }

            return lastFilename;
        }

        private clsCleanupMgrErrors.eCleanupModeConstants GetManagerErrorCleanupMode()
        {
            clsCleanupMgrErrors.eCleanupModeConstants eManagerErrorCleanupMode;

            var strManagerErrorCleanupMode = m_MgrSettings.GetParam("ManagerErrorCleanupMode");

            switch (strManagerErrorCleanupMode.Trim())
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

            if (TraceMode)
                ShowTraceMessage("Tool run error; cleaning up");

            try
            {
                if (!m_AnalysisTask.TaskClosed)
                {
                    LogWarning("Upstream code typically calls .CloseTask before HandleJobFailure is reached; closing the task now");

                    m_AnalysisTask.CloseTask(eToolRunnerResult, m_MostRecentErrorMessage, toolRunner.EvalCode, toolRunner.EvalMessage);
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
                m_MostRecentErrorMessage = "No status files were found, not even a .info file";
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
                m_AnalysisTask.AddResultFileToSkip(statusFile);

            var success = toolRunner.CopyResultsToTransferDirectory();
            if (success)
                return true;

            eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED_REMOTE;
            return false;
        }

        /// <summary>
        /// Initializes the status file writing tool
        /// </summary>
        /// <remarks></remarks>
        private void InitStatusTools()
        {
            if (m_StatusTools == null)
            {
                var statusFileLoc = Path.Combine(m_MgrFolderPath, m_MgrSettings.GetParam("statusfilelocation"));

                if (TraceMode)
                    ShowTraceMessage("Initialize m_StatusTools using " + statusFileLoc);

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
        }

        /// <summary>
        /// Read settings from file AnalysisManagerProg.exe.config
        /// </summary>
        /// <returns>String dictionary of settings as key/value pairs; null on error</returns>
        private Dictionary<string, string> ReadMgrSettingsFile()
        {

            XmlDocument configDoc;

            try
            {
                // Construct the path to the config document
                var configFilePath = Path.Combine(m_MgrFolderPath, m_MgrExeName + ".config");
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
            var lstMgrSettings = ReadMgrSettingsFile();

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
                if (TraceMode)
                    ShowTraceMessage("Auto-defining the manager name as " + autoDefinedName);
                lstMgrSettings[clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME] = autoDefinedName;
            }

            // Default settings in use flag
            if (!lstMgrSettings.ContainsKey(clsAnalysisMgrSettings.MGR_PARAM_USING_DEFAULTS))
            {
                lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_USING_DEFAULTS, Properties.Settings.Default.UsingDefaults.ToString());
            }

            // Default connection string for logging errors to the databsae
            // Will get updated later when manager settings are loaded from the manager control database
            if (!lstMgrSettings.ContainsKey(clsAnalysisMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING))
            {
                lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING, Properties.Settings.Default.DefaultDMSConnString);
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

        // ReSharper disable once SuggestBaseTypeForParameter
        private Dictionary<string, DateTime> LoadCachedLogMessages(FileInfo messageCacheFile)
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

                    DateTime timeStamp;
                    if (DateTime.TryParse(timeStampText, out timeStamp))
                    {
                        // Valid message; store it

                        DateTime cachedTimeStamp;
                        if (cachedMessages.TryGetValue(message, out cachedTimeStamp))
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
                    Thread.Sleep(150);
                }
                else
                {
                    cachedMessages = new Dictionary<string, DateTime>();
                }

                DateTime timeStamp;
                if (cachedMessages.TryGetValue(errorMessage, out timeStamp))
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
                        // ReSharper disable once UseFormatSpecifierInFormatString
                        writer.WriteLine("{0}\t{1}", message.Value.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT), message.Key);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in LogErrorToDatabasePeriodically", ex);
            }
        }

        private void LogPluginLoaderErrors(string pluginType, IReadOnlyCollection<string> errorMessages)
        {
            if (errorMessages.Count <= 0)
            {
                LogError(string.Format("Unable to load {0}, unknown error", pluginType));
                return;
            }

            // Unable to load resource object for StepTool ...
            LogError(string.Format("Unable to load {0}: {1}", pluginType, errorMessages.First()));
            if (errorMessages.Count <= 1)
                return;

            var firstSkipped = false;
            foreach (var item in errorMessages)
            {
                if (!firstSkipped)
                {
                    LogDebug("Additional errors:", 10);
                    firstSkipped = true;
                    continue;
                }
                LogDebug(item, 10);
            }
        }

        private void PostToEventLog(string ErrMsg)
        {
            const string EVENT_LOG_NAME = "DMSAnalysisManager";

            try
            {
                Console.WriteLine();
                Console.WriteLine("===============================================================");
                Console.WriteLine(ErrMsg);
                Console.WriteLine("===============================================================");
                Console.WriteLine();
                Console.WriteLine("You may need to start this application once from an elevated (administrative level) command prompt " +
                                  "using the /EL switch so that it can create the " + EVENT_LOG_NAME + " application log");
                Console.WriteLine();

                var ev = new EventLog("Application", ".", EVENT_LOG_NAME);
                Trace.Listeners.Add(new EventLogTraceListener(EVENT_LOG_NAME));
                Trace.WriteLine(ErrMsg);
                ev.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine("Exception logging to the event log: " + ex.Message);
            }

            Thread.Sleep(500);
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
                if (TraceMode)
                    ShowTraceMessage("Reading application config file");

                // Load settings from config file AnalysisManagerProg.exe.config
                var lstMgrSettings = LoadMgrSettingsFromFile();

                if (lstMgrSettings == null)
                    return false;

                if (TraceMode)
                    ShowTraceMessage("Storing manager settings in m_MgrSettings");

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
            var diMgrFolder = new DirectoryInfo(m_MgrFolderPath);

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
                        if (TraceMode)
                            ShowTraceMessage("Deleting temp file " + fiFile.FullName);
                        fiFile.Delete();
                    }
                }
                catch (Exception)
                {
                    LogError("Error deleting file: " + fiFile.Name);
                }
            }
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
                if (TraceMode)
                    ShowTraceMessage("Getting job resources");

                eToolRunnerResult = toolResourcer.GetResources();
                if (eToolRunnerResult == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return true;
                }

                m_MostRecentErrorMessage = "GetResources returned result: " + eToolRunnerResult;
                if (TraceMode)
                    ShowTraceMessage(m_MostRecentErrorMessage + "; closing job step task");

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

                if (TraceMode)
                    ShowTraceMessage("Running the step tool locally");

                eToolRunnerResult = toolRunner.RunTool();

                if (eToolRunnerResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    m_MostRecentErrorMessage = toolRunner.Message;

                    if (string.IsNullOrEmpty(m_MostRecentErrorMessage))
                    {
                        m_MostRecentErrorMessage = "Unknown ToolRunner Error";
                    }

                    if (TraceMode)
                        ShowTraceMessage("Error running the tool; closing job step task");

                    LogError(m_MgrName + ": " + m_MostRecentErrorMessage + ", Job " + jobNum + ", Dataset " + datasetName);
                    m_AnalysisTask.CloseTask(eToolRunnerResult, m_MostRecentErrorMessage, toolRunner.EvalCode, toolRunner.EvalMessage);

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
                    if (TraceMode)
                        ShowTraceMessage("toolRunner.NeedToAbortProcessing = True; closing job step task");
                    m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, m_MostRecentErrorMessage, toolRunner.EvalCode, toolRunner.EvalMessage);
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
                m_AnalysisTask.CloseTask(eToolRunnerResult, "Exception running tool", toolRunner.EvalCode, toolRunner.EvalMessage);

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

                if (TraceMode)
                    ShowTraceMessage("Instantiating clsRemoteTransferUtility");

                var transferUtility = new clsRemoteTransferUtility(m_MgrSettings, m_AnalysisTask);
                RegisterEvents(transferUtility);

                try
                {
                    transferUtility.UpdateParameters(true);
                }
                catch (Exception ex)
                {
                    m_MostRecentErrorMessage = "Exception initializing the remote transfer utility: " + ex.Message;
                    LogError(m_MostRecentErrorMessage, ex);

                    eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                if (TraceMode)
                    ShowTraceMessage("Transferring files to remote host to run remotely");

                try
                {
                    var successCopying = toolResourcer.CopyResourcesToRemote(transferUtility);

                    if (!successCopying)
                    {
                        m_MostRecentErrorMessage = "Error copying files to the remote host";
                        LogError(m_MostRecentErrorMessage);

                        eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED;
                        return false;
                    }
                }
                catch (Exception ex)
                {
                    m_MostRecentErrorMessage = "Exception copying files to the remote host: " + ex.Message;
                    LogError(m_MostRecentErrorMessage, ex);

                    eToolRunnerResult = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                if (TraceMode)
                    ShowTraceMessage("Creating the .info file in the remote task queue folder ");

                // All files have been copied remotely
                // Create the .info file so remote managers can start processing
                var success = transferUtility.CreateJobTaskInfoFile(out var infoFilePathRemote);

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

            m_PluginLoader.ClearMessageList();
            toolResourcer = m_PluginLoader.GetAnalysisResources(stepToolName.ToLower());

            if (toolResourcer == null && stepToolName.StartsWith("Test_", StringComparison.OrdinalIgnoreCase))
            {
                m_PluginLoader.ClearMessageList();
                stepToolName = stepToolName.Substring("Test_".Length);
                toolResourcer = m_PluginLoader.GetAnalysisResources(stepToolName.ToLower());
            }

            if (toolResourcer == null)
            {
                LogPluginLoaderErrors("resource object for StepTool " + stepToolName, m_PluginLoader.ErrorMessages);
                return false;
            }

            if (m_DebugLevel > 0)
            {
                LogMessage("Loaded resourcer for StepTool " + stepToolName);
                foreach (var item in m_PluginLoader.ErrorMessages)
                {
                    LogWarning(item);
                }

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

            m_PluginLoader.ClearMessageList();
            toolRunner = m_PluginLoader.GetToolRunner(stepToolName.ToLower());

            if (toolRunner == null && stepToolName.StartsWith("Test_", StringComparison.OrdinalIgnoreCase))
            {
                m_PluginLoader.ClearMessageList();
                stepToolName = stepToolName.Substring("Test_".Length);
                toolRunner = m_PluginLoader.GetToolRunner(stepToolName.ToLower());
            }

            if (toolRunner == null)
            {
                LogPluginLoaderErrors("tool runner for StepTool " + stepToolName, m_PluginLoader.ErrorMessages);
                return false;
            }

            if (m_DebugLevel > 0)
            {
                LogMessage("Loaded tool runner for StepTool " + m_AnalysisTask.GetCurrentJobToolDescription());
                foreach (var item in m_PluginLoader.ErrorMessages)
                {
                    LogWarning(item);
                }

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
        /// Display a trace message at the console, preceded by a time stamp
        /// </summary>
        /// <param name="strMessage"></param>
        public static void ShowTraceMessage(string strMessage)
        {
            clsGlobal.EnableConsoleTraceColor();
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss.fff tt") + ": " + strMessage);
            Console.ResetColor();
        }

        /// <summary>
        /// Look for flagFile.txt in the .exe folder
        /// Auto clean errors if AutoCleanupManagerErrors is enabled
        /// </summary>
        /// <returns>True if a flag file exists, false if safe to proceed</returns>
        private bool StatusFlagFileError()
        {
            bool blnMgrCleanupSuccess;

            if (!m_MgrErrorCleanup.DetectStatusFlagFile())
            {
                // No error; return false
                return false;
            }

            try
            {
                blnMgrCleanupSuccess = m_MgrErrorCleanup.AutoCleanupManagerErrors(GetManagerErrorCleanupMode(), m_DebugLevel);
            }
            catch (Exception ex)
            {
                LogError("Error calling AutoCleanupManagerErrors", ex);
                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.StatusFlagFileError(): " + ex.Message, m_MostRecentJobInfo, true);

                blnMgrCleanupSuccess = false;
            }

            if (blnMgrCleanupSuccess)
            {
                LogWarning("Flag file found; automatically cleaned the work directory and deleted the flag file(s)");

                // No error; return false
                return false;
            }

            // Error removing flag file (or manager not set to auto-remove flag files)

            // Periodically log errors to the database
            var flagFile = new FileInfo(m_MgrErrorCleanup.FlagFilePath);
            string errorMessage;
            if ((flagFile.Directory == null))
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
        /// <param name="MinutesBetweenUpdates"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool UpdateManagerSettings(ref DateTime dtLastConfigDBUpdate, double MinutesBetweenUpdates)
        {
            var blnSuccess = true;

            if ((DateTime.UtcNow.Subtract(dtLastConfigDBUpdate).TotalMinutes >= MinutesBetweenUpdates))
            {
                dtLastConfigDBUpdate = DateTime.UtcNow;

                if (TraceMode)
                    ShowTraceMessage("Loading manager settings from the manager control DB");

                if (!m_MgrSettings.LoadDBSettings())
                {
                    string msg;

                    if (string.IsNullOrEmpty(m_MgrSettings.ErrMsg))
                    {
                        msg = "Error calling m_MgrSettings.LoadMgrSettingsFromDB to update manager settings";
                    }
                    else
                    {
                        msg = m_MgrSettings.ErrMsg;
                    }

                    LogError(msg);

                    blnSuccess = false;
                }
                else
                {
                    // Need to synchronize some of the settings
                    UpdateStatusToolLoggingSettings(m_StatusTools);
                }
            }

            return blnSuccess;
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

        private void UpdateStatusIdle(string ManagerIdleMessage)
        {
            var recentErrorMessages = DetermineRecentErrorMessages(5, ref m_MostRecentJobInfo);

            m_StatusTools.UpdateIdle(ManagerIdleMessage, recentErrorMessages, m_MostRecentJobInfo, true);
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

            objStatusFile.ConfigureMemoryLogging(logMemoryUsage, minimumMemoryUsageLogInterval, m_MgrFolderPath);
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
                        while (!diDatasetStoragePath.Exists && (diDatasetStoragePath.Parent != null))
                        {
                            diDatasetStoragePath = diDatasetStoragePath.Parent;
                        }

                        datasetStoragePath = diDatasetStoragePath.FullName;
                    }

                    if (!ValidateFreeDiskSpaceWork("Dataset directory", datasetStoragePath, datasetStorageMinFreeSpaceGB * 1024, out errorMessage, clsLogTools.LoggerTypes.LogFile))
                    {
                        return false;
                    }

                    return true;
                }

                var workingDirMinFreeSpaceMB = m_MgrSettings.GetParam("WorkDirMinFreeSpaceMB", DEFAULT_WORKING_DIR_MIN_FREE_SPACE_MB);

                var transferDir = m_AnalysisTask.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "transferFolderPath");
                var transferDirMinFreeSpaceGB = m_MgrSettings.GetParam("TransferDirMinFreeSpaceGB", DEFAULT_TRANSFER_DIR_MIN_FREE_SPACE_GB);

                var orgDbDir = m_MgrSettings.GetParam("orgdbdir");
                var orgDbDirMinFreeSpaceMB = m_MgrSettings.GetParam("OrgDBDirMinFreeSpaceMB", DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB);

                // Verify that the working directory exists and that its drive has sufficient free space
                if (!ValidateFreeDiskSpaceWork("Working directory", m_WorkDirPath, workingDirMinFreeSpaceMB, out errorMessage, clsLogTools.LoggerTypes.LogDb))
                {
                    LogError("Disabling manager since working directory problem");
                    DisableManagerLocally();
                    return false;
                }

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

                // Verify that the remote transfer directory exists and that its drive has sufficient free space
                if (!ValidateFreeDiskSpaceWork("Transfer directory", transferDir, transferDirMinFreeSpaceGB * 1024, out errorMessage, clsLogTools.LoggerTypes.LogFile))
                {
                    return false;
                }

                var orgDbRequired = toolResourcer.GetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired);

                if (orgDbRequired)
                {
                    // Verify that the local fasta file cache directory has sufficient free space

                    if (!ValidateFreeDiskSpaceWork("Organism DB directory", orgDbDir, orgDbDirMinFreeSpaceMB, out errorMessage, clsLogTools.LoggerTypes.LogFile))
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

        private bool ValidateFreeDiskSpaceWork(string directoryDescription, string directoryPath, int minFreeSpaceMB, out string errorMessage, clsLogTools.LoggerTypes eLogLocationIfNotFound)
        {
            return clsGlobal.ValidateFreeDiskSpace(directoryDescription, directoryPath, minFreeSpaceMB, eLogLocationIfNotFound, out errorMessage);
        }

        private bool VerifyWorkDir()
        {
            // Verify working directory is valid
            if (!Directory.Exists(m_WorkDirPath))
            {
                var msg = "Invalid working directory: " + m_WorkDirPath;
                LogError(msg);
                Thread.Sleep(1500);
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

                        // Wait 100 msec then refresh the listing
                        Thread.Sleep(100);

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
            clsGlobal.LogDebug(statusMessage, writeToLog: false);
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
