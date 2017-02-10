//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/19/2007
//
//*********************************************************************************************************

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;

using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
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
        private readonly string m_MgrFolderPath;
        private string m_WorkDirPath;

        private string m_MgrName = "??";
        // clsAnalysisJob
        private IJobParams m_AnalysisTask;
        private clsPluginLoader m_PluginLoader;

        private clsSummaryFile m_SummaryFile;
        private FileSystemWatcher m_FileWatcher;

        private bool m_ConfigChanged;
        private IAnalysisResources m_Resource;
        private IToolRunner m_ToolRunner;
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

            var fiMgr = new FileInfo(System.Windows.Forms.Application.ExecutablePath);
            m_MgrFolderPath = fiMgr.DirectoryName;
        }

        /// <summary>
        /// Initializes the manager settings
        /// </summary>
        /// <returns>TRUE for success, FALSE for failure</returns>
        /// <remarks></remarks>
        private bool InitMgr()
        {
            // Create a database logger connected to DMS5
            // Once the initial parameters have been successfully read,
            // we remove this logger than make a new one using the connection string read from the Manager Control DB
            var defaultDmsConnectionString = Properties.Settings.Default.DefaultDMSConnString;
            var hostName = System.Net.Dns.GetHostName();

            clsLogTools.CreateDbLogger(defaultDmsConnectionString, "Analysis Tool Manager: " + hostName, true);

            // Get settings from config file

            try
            {
                if (TraceMode)
                    ShowTraceMessage("Reading application config file");
                var lstMgrSettings = LoadMgrSettingsFromFile();

                // Get the manager settings
                // If you get an exception here while debugging in Visual Studio, then be sure
                //   that "UsingDefaults" is set to False in CaptureTaskManager.exe.config
                try
                {
                    if (TraceMode)
                        ShowTraceMessage("Instantiating clsAnalysisMgrSettings");
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
                    Console.WriteLine("You may need to start this application once from an elevated (administrative level) command prompt using the /EL switch so that it can create the " + CUSTOM_LOG_NAME + " application log");
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

            m_MgrName = m_MgrSettings.GetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME);
            if (TraceMode)
                ShowTraceMessage("Manager name is " + m_MgrName);

            // Delete any temporary files that may be left in the app directory
            RemoveTempFiles();

            // Confirm that the application event log exists
            if (!EventLog.SourceExists(CUSTOM_LOG_SOURCE_NAME))
            {
                var sourceData = new EventSourceCreationData(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME);
                EventLog.CreateEventSource(sourceData);
            }

            // Setup the loggers

            var logFileNameBase = m_MgrSettings.GetParam("logfilename");

            clsLogTools.CreateFileLogger(logFileNameBase);

            var logCnStr = m_MgrSettings.GetParam("connectionstring");

            clsLogTools.RemoveDefaultDbLogger();
            clsLogTools.CreateDbLogger(logCnStr, "Analysis Tool Manager: " + m_MgrName, false);

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

            // Setup a file watcher for the config file
            m_FileWatcher = new FileSystemWatcher
            {
                Path = m_MgrFolderPath,
                IncludeSubdirectories = false,
                Filter = configFileName,
                NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size,
                EnableRaisingEvents = true
            };

            m_FileWatcher.Changed += m_FileWatcher_Changed;

            // Get the debug level
            m_DebugLevel = Convert.ToInt16(m_MgrSettings.GetParam("debuglevel", 2));

            // Make sure that the manager name matches the machine name (with a few exceptions)

            if (!hostName.ToLower().StartsWith("emslmq") && !hostName.ToLower().StartsWith("emslpub") && !hostName.ToLower().StartsWith("monroe"))
            {
                if (!m_MgrName.ToLower().StartsWith(hostName.ToLower()))
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

                var maxLoopCount = Convert.ToInt32(m_MgrSettings.GetParam("maxrepetitions"));
                var requestJobs = true;
                var oneTaskStarted = false;
                var oneTaskPerformed = false;

                InitStatusTools();

                while ((loopCount < maxLoopCount) & requestJobs)
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
                        m_FileWatcher.EnableRaisingEvents = true;
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
                    var MgrActive = m_MgrSettings.GetParam("mgractive", false);
                    var MgrActiveLocal = m_MgrSettings.GetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, false);

                    if (!(MgrActive & MgrActiveLocal))
                    {
                        string strManagerDisableReason;
                        if (!MgrActiveLocal)
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

                    var MgrUpdateRequired = m_MgrSettings.GetParam("ManagerUpdateRequired", false);
                    if (MgrUpdateRequired)
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
                                LogError("Error cleaning working directory, job " + m_AnalysisTask.GetParam("StepParameters", "Job") + "; see folder " + m_WorkDirPath);
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
                                    if (m_MostRecentErrorMessage.Contains("None of the spectra are centroided") || m_MostRecentErrorMessage.Contains("No peaks found") || m_MostRecentErrorMessage.Contains("No spectra were exported"))
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

                                LogError("clsMainProcess.DoAnalysis(), Exception thrown by DoAnalysisJob, " + ex.Message, ex);
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
                            LogError("clsMainProcess.DoAnalysis; Invalid request result: " + Convert.ToInt32(taskReturn).ToString());
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
                LogError("clsMainProcess.DoAnalysis(), Error encountered, " + ex.Message, ex);
                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysis(): " + ex.Message, m_MostRecentJobInfo, true);
            }
            finally
            {
                if ((m_StatusTools != null))
                {
                    if (TraceMode)
                        ShowTraceMessage("Disposing message queue via m_StatusTools.DisposeMessageQueue");
                    m_StatusTools.DisposeMessageQueue();
                }
            }
        }

        private bool DoAnalysisJob()
        {
            CloseOutType eToolRunnerResult;
            var jobNum = m_AnalysisTask.GetJobParameter("StepParameters", "Job", 0);
            var stepNum = m_AnalysisTask.GetJobParameter("StepParameters", "Step", 0);
            var cpuLoadExpected = m_AnalysisTask.GetJobParameter("StepParameters", "CPU_Load", 1);

            var datasetName = m_AnalysisTask.GetParam("JobParameters", "DatasetNum");
            var jobToolDescription = m_AnalysisTask.GetCurrentJobToolDescription();

            var blnRunToolError = false;

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
            m_StatusTools.JobNumber = jobNum;
            m_StatusTools.JobStep = stepNum;
            m_StatusTools.Tool = jobToolDescription;
            m_StatusTools.MgrName = m_MgrName;
            m_StatusTools.ProgRunnerProcessID = 0;
            m_StatusTools.ProgRunnerCoreUsage = cpuLoadExpected;
            m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.RETRIEVING_RESOURCES, 0, 0, "", "", m_MostRecentJobInfo, true);

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
                LogMessage("Debug level is " + m_DebugLevel.ToString());
            }

            // Create an object to manage the job resources
            if (!SetResourceObject())
            {
                LogError(m_MgrName + ": Unable to set the Resource object, job " + jobNum + ", Dataset " + datasetName, true);
                m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Unable to set resource object");
                m_MgrErrorCleanup.CleanWorkDir();
                UpdateStatusIdle("Error encountered: Unable to set resource object");
                return false;
            }

            // Create an object to run the analysis tool
            if (!SetToolRunnerObject())
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
            if (!ValidateFreeDiskSpace(out m_MostRecentErrorMessage))
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
                m_Resource.SetOption(clsGlobal.eAnalysisResourceOptions.MyEMSLSearchDisabled, true);
            }

            // Retrieve files required for the job
            m_MgrErrorCleanup.CreateStatusFlagFile();
            try
            {
                if (TraceMode)
                    ShowTraceMessage("Getting job resources");

                eToolRunnerResult = m_Resource.GetResources();
                if (eToolRunnerResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    m_MostRecentErrorMessage = "GetResources returned result: " + eToolRunnerResult.ToString();
                    if (TraceMode)
                        ShowTraceMessage(m_MostRecentErrorMessage + "; closing job step task");
                    if ((m_Resource.Message != null))
                    {
                        m_MostRecentErrorMessage += "; " + m_Resource.Message;
                    }

                    LogError(m_MgrName + ": " + m_MostRecentErrorMessage + ", Job " + jobNum + ", Dataset " + datasetName);
                    m_AnalysisTask.CloseTask(eToolRunnerResult, m_Resource.Message);

                    m_MgrErrorCleanup.CleanWorkDir();
                    UpdateStatusIdle("Error encountered: " + m_MostRecentErrorMessage);
                    m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError("clsMainProcess.DoAnalysisJob(), Getting resources, " + ex.Message, ex);

                m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Exception getting resources");

                if (m_MgrErrorCleanup.CleanWorkDir())
                {
                    m_MgrErrorCleanup.DeleteStatusFlagFile(m_DebugLevel);
                }
                else
                {
                    m_MgrErrorCleanup.CreateErrorDeletingFilesFlagFile();
                }

                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " + ex.Message, m_MostRecentJobInfo, true);
                return false;
            }

            // Run the job
            m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.RUNNING_TOOL, 0);
            try
            {
                if (TraceMode)
                    ShowTraceMessage("Running the step tool");

                eToolRunnerResult = m_ToolRunner.RunTool();
                if (eToolRunnerResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    m_MostRecentErrorMessage = m_ToolRunner.Message;

                    if (string.IsNullOrEmpty(m_MostRecentErrorMessage))
                    {
                        m_MostRecentErrorMessage = "Unknown ToolRunner Error";
                    }

                    if (TraceMode)
                        ShowTraceMessage("Error running the tool; closing job step task");

                    LogError(m_MgrName + ": " + m_MostRecentErrorMessage + ", Job " + jobNum + ", Dataset " + datasetName);
                    m_AnalysisTask.CloseTask(eToolRunnerResult, m_MostRecentErrorMessage, m_ToolRunner.EvalCode, m_ToolRunner.EvalMessage);

                    try
                    {
                        if (m_MostRecentErrorMessage.Contains(DECON2LS_FATAL_REMOTING_ERROR) || m_MostRecentErrorMessage.Contains(DECON2LS_CORRUPTED_MEMORY_ERROR))
                        {
                            m_NeedToAbortProcessing = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError("clsMainProcess.DoAnalysisJob(), Exception examining MostRecentErrorMessage", ex);
                    }

                    if (eToolRunnerResult == CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE)
                    {
                        m_NeedToAbortProcessing = true;
                    }

                    if (m_NeedToAbortProcessing && m_MostRecentErrorMessage.StartsWith(clsAnalysisToolRunnerBase.PVM_RESET_ERROR_MESSAGE))
                    {
                        DisableManagerLocally();
                    }

                    blnRunToolError = true;
                }

                if (m_ToolRunner.NeedToAbortProcessing)
                {
                    m_NeedToAbortProcessing = true;
                    if (TraceMode)
                        ShowTraceMessage("ToolRunner.NeedToAbortProcessing = True; closing job step task");
                    m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, m_MostRecentErrorMessage, m_ToolRunner.EvalCode, m_ToolRunner.EvalMessage);
                }
            }
            catch (Exception ex)
            {
                LogError("clsMainProcess.DoAnalysisJob(), running tool, " + ex.Message, ex);

                if (ex.Message.Contains(DECON2LS_TCP_ALREADY_REGISTERED_ERROR))
                {
                    m_NeedToAbortProcessing = true;
                }

                m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Exception running tool", m_ToolRunner.EvalCode, m_ToolRunner.EvalMessage);

                blnRunToolError = true;
            }

            if (blnRunToolError)
            {
                // Note: the above code should have already called m_AnalysisTask.CloseTask()

                if (TraceMode)
                    ShowTraceMessage("Tool run error; cleaning up");

                try
                {
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
                    LogError("clsMainProcess.DoAnalysisJob(), cleaning up after RunTool error," + ex.Message, ex);
                    m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " + ex.Message, m_MostRecentJobInfo, true);
                    return false;
                }
            }

            // Close out the job
            m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.CLOSING, EnumTaskStatusDetail.CLOSING, 100);
            try
            {
                if (TraceMode)
                    ShowTraceMessage("Task completed successfully; closing the job step task");

                // Close out the job as a success
                m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_SUCCESS, string.Empty, m_ToolRunner.EvalCode, m_ToolRunner.EvalMessage);
                LogMessage(m_MgrName + ": Completed job " + jobNum);

                UpdateStatusIdle("Completed job " + jobNum + ", step " + stepNum);
            }
            catch (Exception ex)
            {
                LogError("clsMainProcess.DoAnalysisJob(), Close task after normal run," + ex.Message, ex);
                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " + ex.Message, m_MostRecentJobInfo, true);
                return false;
            }

            try
            {
                // If success was reported check to see if there was an error deleting non result files
                if (m_MgrErrorCleanup.DetectErrorDeletingFilesFlagFile())
                {
                    // If there was a problem deleting non result files, return success and let the manager try to delete the files one more time on the next start up
                    // However, wait another 5 seconds before continuing
                    PRISM.Processes.clsProgRunner.GarbageCollectNow();
                    Thread.Sleep(5000);

                    return true;
                }

                // Clean the working directory
                try
                {
                    if (!m_MgrErrorCleanup.CleanWorkDir(1))
                    {
                        LogError("Error cleaning working directory, job " + m_AnalysisTask.GetParam("StepParameters", "Job"));
                        m_AnalysisTask.CloseTask(CloseOutType.CLOSEOUT_FAILED, "Error cleaning working directory");
                        m_MgrErrorCleanup.CreateErrorDeletingFilesFlagFile();
                        return false;
                    }
                }
                catch (Exception ex)
                {
                    LogError("clsMainProcess.DoAnalysisJob(), Clean work directory after normal run," + ex.Message, ex);
                    m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " + ex.Message, m_MostRecentJobInfo, true);
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
                LogError("clsMainProcess.DoAnalysisJob(), " + ex.Message, ex);
                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysisJob(): " + ex.Message, m_MostRecentJobInfo, true);
                return false;
            }
        }

        /// <summary>
        /// Constructs a description of the given job using the job number, step tool name, and dataset name
        /// </summary>
        /// <param name="JobStartTimeStamp">Time job started</param>
        /// <param name="Job">Job name</param>
        /// <param name="Dataset">Dataset name</param>
        /// <param name="ToolName">Tool name (or step tool name)</param>
        /// <returns>Info string, similar to: Job 375797; DataExtractor (XTandem), Step 4; QC_Shew_09_01_b_pt5_25Mar09_Griffin_09-02-03; 3/26/2009 3:17:57 AM</returns>
        /// <remarks></remarks>
        private string ConstructMostRecentJobInfoText(string JobStartTimeStamp, int Job, string Dataset, string ToolName)
        {
            try
            {
                if (JobStartTimeStamp == null)
                    JobStartTimeStamp = string.Empty;
                if (ToolName == null)
                    ToolName = "??";
                if (Dataset == null)
                    Dataset = "??";

                return "Job " + Job.ToString() + "; " + ToolName + "; " + Dataset + "; " + JobStartTimeStamp;
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
            var blnSuccess = CreateAnalysisManagerEventLog(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME);

            if (blnSuccess)
            {
                Console.WriteLine();
                Console.WriteLine("Windows Event Log '" + CUSTOM_LOG_NAME + "' has been validated for source '" + CUSTOM_LOG_SOURCE_NAME + "'");
                Console.WriteLine();
            }
        }

        private static bool CreateAnalysisManagerEventLog(string SourceName, string LogName)
        {
            try
            {
                if (string.IsNullOrEmpty(SourceName))
                {
                    Console.WriteLine("Error creating the Windows Event Log: SourceName cannot be blank");
                    return false;
                }

                if (string.IsNullOrEmpty(LogName))
                {
                    Console.WriteLine("Error creating the Windows Event Log: LogName cannot be blank");
                    return false;
                }

                if (!EventLog.SourceExists(SourceName))
                {
                    Console.WriteLine("Creating Windows Event Log " + LogName + " for source " + SourceName);
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
                    Console.WriteLine("Warning: unable to update the maximum log size to 1024 KB: \n  " + ex.Message);
                }

                try
                {
                    ELog.ModifyOverflowPolicy(OverflowAction.OverwriteAsNeeded, 90);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Warning: unable to update the overflow policy to keep events for 90 days and overwrite as needed: \n  " + ex.Message);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception creating the Windows Event Log named '" + LogName + "' for source '" + SourceName + "': " + ex.Message);
                return false;
            }

            return true;
        }

        private bool DataPackageIdMissing()
        {
            var stepToolName = m_AnalysisTask.GetParam("JobParameters", "StepTool");

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

            var dataPkgRequired = false;
            if (multiJobStepTools.Any(multiJobTool => string.Equals(stepToolName, multiJobTool, StringComparison.InvariantCultureIgnoreCase)))
            {
                dataPkgRequired = true;
            }

            if (dataPkgRequired)
            {
                var dataPkgID = m_AnalysisTask.GetJobParameter("JobParameters", "DataPackageID", 0);
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
                var reLogFileName = new Regex(@"(.+_)(\d+)-(\d+)-(\d+).\S+", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var objMatch = reLogFileName.Match(strLogFilePath);

                if (objMatch.Success && objMatch.Groups.Count >= 4)
                {
                    var intMonth = Convert.ToInt32(objMatch.Groups[2].Value);
                    var intDay = Convert.ToInt32(objMatch.Groups[3].Value);
                    var intYear = Convert.ToInt32(objMatch.Groups[4].Value);

                    var dtCurrentDate = DateTime.Parse(intYear + "-" + intMonth + "-" + intDay);
                    var dtNewDate = dtCurrentDate.Subtract(new TimeSpan(1, 0, 0, 0));

                    var strPreviousLogFilePath = objMatch.Groups[1].Value + dtNewDate.ToString("MM-dd-yyyy") + Path.GetExtension(strLogFilePath);
                    return strPreviousLogFilePath;
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error in DecrementLogFilePath: " + ex.Message);
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
        public List<string> DetermineRecentErrorMessages(int intErrorMessageCountToReturn, ref string strMostRecentJobInfo)
        {
            // This regex will match all text up to the first comma (this is the time stamp), followed by a comma, then the error message, then the text ", Error,"
            const string ERROR_MATCH_REGEX = "^([^,]+),(.+), Error, *$";

            // This regex looks for information on a job starting
            // Note: do not try to match "Step \d+" with this regex due to variations on how the log message appears
            const string JOB_START_REGEX = "^([^,]+),.+Started analysis job (\\d+), Dataset (.+), Tool ([^,]+)";

            // Examples matching log entries
            // 5/04/2015 12:34:46, Pub-88-3: Started analysis job 1193079, Dataset Lp_PDEC_N-sidG_PD1_1May15_Lynx_15-01-24, Tool Decon2LS_V2, Step 1, INFO,
            // 5/04/2015 10:54:49, Proto-6_Analysis-1: Started analysis job 1192426, Dataset LewyHNDCGlobFractestrecheck_SRM_HNDC_Frac46_smeagol_05Apr15_w6326a, Tool Results_Transfer (MASIC_Finnigan), Step 2, INFO,

            // The following effectively defines the number of days in the past to search when finding recent errors
            const int MAX_LOG_FILES_TO_SEARCH = 5;

            // Note that strRecentErrorMessages() and dtRecentErrorMessageDates() are parallel arrays
            var strRecentErrorMessages = new string[0];
            DateTime[] dtRecentErrorMessageDates = null;

            if (strMostRecentJobInfo == null)
                strMostRecentJobInfo = string.Empty;

            try
            {
                var strMostRecentJobInfoFromLogs = string.Empty;

                //If objLogger Is Nothing Then
                //    intRecentErrorMessageCount = 0
                //    ReDim strRecentErrorMessages(-1)
                //Else
                if (intErrorMessageCountToReturn < 1)
                    intErrorMessageCountToReturn = 1;

                var intRecentErrorMessageCount = 0;

                // Initialize the RegEx that splits out the timestamp from the error message
                var reErrorLine = new Regex(ERROR_MATCH_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var reJobStartLine = new Regex(JOB_START_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // Initialize the queue that holds recent error messages
                var qErrorMsgQueue = new Queue(intErrorMessageCountToReturn);

                // Initialize the hashtable to hold the error messages, but without date stamps
                var htUniqueErrorMessages = new Hashtable();

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

                                if ((strLineIn != null))
                                {
                                    var objMatch = reErrorLine.Match(strLineIn);

                                    if (objMatch.Success)
                                    {
                                        DetermineRecentErrorCacheError(objMatch, strLineIn, htUniqueErrorMessages, qErrorMsgQueue, intErrorMessageCountToReturn);
                                    }

                                    if (blnCheckForMostRecentJob)
                                    {
                                        objMatch = reJobStartLine.Match(strLineIn);
                                        if (objMatch.Success)
                                        {
                                            try
                                            {
                                                strMostRecentJobInfoFromLogs = ConstructMostRecentJobInfoText(objMatch.Groups[1].Value, Convert.ToInt32(objMatch.Groups[2].Value), objMatch.Groups[3].Value, objMatch.Groups[4].Value);
                                            }
                                            catch (Exception ex)
                                            {
                                                // Ignore errors here
                                            }
                                        }
                                    }
                                }
                            }

                            srInFile.Close();

                            if (blnCheckForMostRecentJob && strMostRecentJobInfoFromLogs.Length > 0)
                            {
                                // We determine the most recent job; no need to check other log files
                                blnCheckForMostRecentJob = false;
                            }
                        }
                        else
                        {
                            // Log file not found; that's OK, we'll decrement the name by one day and keep checking
                        }

                        // Increment the log file counter, regardless of whether or not the log file was found
                        intLogFileCountProcessed += 1;

                        if (qErrorMsgQueue.Count < intErrorMessageCountToReturn)
                        {
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
                }

                if (blnLoggerReportsError)
                {
                    // Append the error message reported by the Logger to the error message queue (treating it as the newest error)
                    strLineIn = clsLogTools.MostRecentErrorMessage;
                    var objMatch = reErrorLine.Match(strLineIn);

                    if (objMatch.Success)
                    {
                        DetermineRecentErrorCacheError(objMatch, strLineIn, htUniqueErrorMessages, qErrorMsgQueue, intErrorMessageCountToReturn);
                    }
                }

                // Populate strRecentErrorMessages and dtRecentErrorMessageDates using the messages stored in qErrorMsgQueue
                while (qErrorMsgQueue.Count > 0)
                {
                    var strErrorMessageClean = Convert.ToString(qErrorMsgQueue.Dequeue());

                    // Find the newest timestamp for this message
                    string strTimestamp;
                    if (htUniqueErrorMessages.ContainsKey(strErrorMessageClean))
                    {
                        strTimestamp = Convert.ToString(htUniqueErrorMessages[strErrorMessageClean]);
                    }
                    else
                    {
                        // This code should not be reached
                        strTimestamp = "";
                    }

                    if (intRecentErrorMessageCount >= strRecentErrorMessages.Length)
                    {
                        // Need to reserve more memory; this is unexpected
                        Array.Resize(ref strRecentErrorMessages, strRecentErrorMessages.Length * 2);
                        Array.Resize(ref dtRecentErrorMessageDates, strRecentErrorMessages.Length);
                    }

                    strRecentErrorMessages[intRecentErrorMessageCount] = strTimestamp + ", " + strErrorMessageClean.TrimStart(' ');

                    try
                    {
                        dtRecentErrorMessageDates[intRecentErrorMessageCount] = Convert.ToDateTime(strTimestamp);
                    }
                    catch (Exception ex)
                    {
                        // Error converting date;
                        dtRecentErrorMessageDates[intRecentErrorMessageCount] = DateTime.MinValue;
                    }

                    intRecentErrorMessageCount += 1;
                }

                if (intRecentErrorMessageCount < strRecentErrorMessages.Length)
                {
                    // Shrink the arrays
                    Array.Resize(ref strRecentErrorMessages, intRecentErrorMessageCount);
                    Array.Resize(ref dtRecentErrorMessageDates, intRecentErrorMessageCount);
                }

                if (intRecentErrorMessageCount > 1)
                {
                    // Sort the arrays by descending date
                    Array.Sort(dtRecentErrorMessageDates, strRecentErrorMessages);
                    Array.Reverse(dtRecentErrorMessageDates);
                    Array.Reverse(strRecentErrorMessages);
                }

                if (string.IsNullOrEmpty(strMostRecentJobInfo))
                {
                    if (!string.IsNullOrWhiteSpace(strMostRecentJobInfoFromLogs))
                    {
                        // Update strMostRecentJobInfo
                        strMostRecentJobInfo = strMostRecentJobInfoFromLogs;
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                try
                {
                    LogError("Error in DetermineRecentErrorMessages", ex);
                }
                catch (Exception ex2)
                {
                    // Ignore errors logging the error
                }
            }

            return strRecentErrorMessages.ToList();
        }

        private void DetermineRecentErrorCacheError(Match objMatch, string strErrorMessage, Hashtable htUniqueErrorMessages, Queue qErrorMsgQueue, int intMaxErrorMessageCountToReturn)
        {
            string strTimestamp = null;
            string strErrorMessageClean = null;
            string strQueuedError = null;

            bool blnAddItemToQueue = false;

            // See if this error is present in htUniqueErrorMessages yet
            // If it is present, update the timestamp in htUniqueErrorMessages
            // If not present, queue it

            if (objMatch.Groups.Count >= 2)
            {
                strTimestamp = objMatch.Groups[1].Value;
                strErrorMessageClean = objMatch.Groups[2].Value;
            }
            else
            {
                // Regex didn't match; this is unexpected
                strTimestamp = DateTime.MinValue.ToString();
                strErrorMessageClean = strErrorMessage;
            }

            // Check whether strErrorMessageClean is in the hash table
            var objItem = htUniqueErrorMessages[strErrorMessageClean];
            if ((objItem != null))
            {
                // The error message is present
                // Update the timestamp associated with strErrorMessageClean if the time stamp is newer than the stored one
                try
                {
                    if (DateTime.Parse(strTimestamp) > DateTime.Parse(Convert.ToString(objItem)))
                    {
                        htUniqueErrorMessages[strErrorMessageClean] = strTimestamp;
                    }
                }
                catch (Exception ex)
                {
                    // Date comparison failed; leave the existing timestamp unchanged
                }
            }
            else
            {
                // The error message is not present
                htUniqueErrorMessages.Add(strErrorMessageClean, strTimestamp);
            }

            if (!qErrorMsgQueue.Contains(strErrorMessageClean))
            {
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

                    blnAddItemToQueue = true;

                    strQueuedError = Convert.ToString(qErrorMsgQueue.Peek());

                    // Get the timestamp associated with strQueuedError, as tracked by the hashtable
                    objItem = htUniqueErrorMessages[strQueuedError];
                    if (objItem == null)
                    {
                        // The error message is not in the hashtable; this is unexpected
                    }
                    else
                    {
                        // Compare the queued error's timestamp with the timestamp of the new error message
                        try
                        {
                            if (DateTime.Parse(Convert.ToString(objItem)) >= DateTime.Parse(strTimestamp))
                            {
                                // The queued error message's timestamp is equal to or newer than the new message's timestamp
                                // Do not add the new item to the queue
                                blnAddItemToQueue = false;
                            }
                        }
                        catch (Exception ex)
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

        private string GetRecentLogFilename()
        {
            string lastFilename;

            try
            {
                // Obtain a list of log files
                var files = Directory.GetFiles(m_MgrFolderPath, m_MgrSettings.GetParam("logfilename") + "*.txt");

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
                    Dataset = "",
                    JobNumber = 0,
                    JobStep = 0,
                    Tool = "",
                    MgrName = m_MgrName,
                    MgrStatus = EnumMgrStatus.RUNNING,
                    TaskStatus = EnumTaskStatus.NO_TASK,
                    TaskStatusDetail = EnumTaskStatusDetail.NO_TASK
                };
                RegisterEvents(m_StatusTools);

                UpdateStatusToolLoggingSettings(m_StatusTools);
            }
        }

        /// <summary>
        /// Loads the initial settings from application config file
        /// </summary>
        /// <returns>String dictionary containing initial settings if suceessful; NOTHING on error</returns>
        /// <remarks></remarks>
        internal static Dictionary<string, string> LoadMgrSettingsFromFile()
        {
            // Load initial settings into string dictionary for return
            var lstMgrSettings = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            // Note: When you are editing this project using the Visual Studio IDE, if you edit the values
            //  ->My Project>Settings.settings, then when you run the program (from within the IDE), it
            //  will update file AnalysisManagerProg.exe.config with your settings
            // The manager will exit if the "UsingDefaults" value is "True", thus you need to have
            //  "UsingDefaults" be "False" to run (and/or debug) the application

            Properties.Settings.Default.Reload();

            // Manager config db connection string
            lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_MGR_CFG_DB_CONN_STRING, Properties.Settings.Default.MgrCnfgDbConnectStr);

            // Manager active flag
            lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_MGR_ACTIVE_LOCAL, Properties.Settings.Default.MgrActive_Local.ToString());

            // Manager name
            // If the MgrName setting in the AnalysisManagerProg.exe.config file contains the text $ComputerName$
            // that text is replaced with this computer's domain name
            // This is a case-sensitive comparison

            lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME, Properties.Settings.Default.MgrName.Replace("$ComputerName$", Environment.MachineName));

            // Default settings in use flag
            var usingDefaults = Properties.Settings.Default.UsingDefaults.ToString();
            lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_USING_DEFAULTS, usingDefaults);

            // Default connection string for logging errors to the databsae
            // Will get updated later when manager settings are loaded from the manager control database
            var defaultDMSConnectionString = Properties.Settings.Default.DefaultDMSConnString;
            lstMgrSettings.Add(clsAnalysisMgrSettings.MGR_PARAM_DEFAULT_DMS_CONN_STRING, defaultDMSConnectionString);

            return lstMgrSettings;
        }

        private bool NeedToAbortProcessing()
        {
            if (m_NeedToAbortProcessing)
            {
                LogError("Analysis manager has encountered a fatal error - aborting processing (m_NeedToAbortProcessing = True)");
                return true;
            }

            if ((m_StatusTools != null))
            {
                if (m_StatusTools.AbortProcessingNow)
                {
                    LogError("Found file " + clsStatusFile.ABORT_PROCESSING_NOW_FILENAME + " - aborting processing");
                    return true;
                }
            }

            return false;
        }

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
                        writer.WriteLine("{0}\t{1}", message.Value.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT), message.Key);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in LogErrorToDatabasePeriodically", ex);
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
                Console.WriteLine("You may need to start this application once from an elevated (administrative level) command prompt using the /EL switch so that it can create the " + EVENT_LOG_NAME + " application log");
                Console.WriteLine();

                var Ev = new EventLog("Application", ".", EVENT_LOG_NAME);
                Trace.Listeners.Add(new EventLogTraceListener(EVENT_LOG_NAME));
                Trace.WriteLine(ErrMsg);
                Ev.Close();
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

                // Get settings from config file
                var lstMgrSettings = LoadMgrSettingsFromFile();

                if (TraceMode)
                    ShowTraceMessage("Storing manager settings in m_MgrSettings");
                if (!m_MgrSettings.LoadSettings(lstMgrSettings))
                {
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
            }
            catch (Exception ex)
            {
                LogError("Error re-loading manager settings: " + ex.Message);
                return false;
            }

            return true;
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

        private bool SetResourceObject()
        {
            var stepToolName = m_AnalysisTask.GetParam("StepTool");

            m_PluginLoader.ClearMessageList();
            m_Resource = m_PluginLoader.GetAnalysisResources(stepToolName.ToLower());
            if (m_Resource == null)
            {
                LogError("Unable to load resource object, " + m_PluginLoader.Message);
                return false;
            }

            if (m_DebugLevel > 0)
            {
                strMessage = "Loaded resourcer for StepTool " + stepToolName;
                if (m_PluginLoader.Message.Length > 0)
                    strMessage += ": " + m_PluginLoader.Message;
                LogMessage(strMessage);
            }

            try
            {
                m_Resource.Setup(m_MgrSettings, m_AnalysisTask, m_StatusTools, m_MyEMSLUtilities);
            }
            catch (Exception ex)
            {
                LogError("Unable to load resource object, " + ex.Message);
                return false;
            }

            return true;
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
                LogError("Error calling AutoCleanupManagerErrors, " + ex.Message, ex);
                m_StatusTools.UpdateIdle("Error encountered", "clsMainProcess.DoAnalysis(): " + ex.Message, m_MostRecentJobInfo, true);

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

        private bool SetToolRunnerObject()
        {
            var stepToolName = m_AnalysisTask.GetParam("StepTool");

            m_PluginLoader.ClearMessageList();
            m_ToolRunner = m_PluginLoader.GetToolRunner(stepToolName.ToLower());
            if (m_ToolRunner == null)
            {
                LogError("Unable to load tool runner for StepTool " + stepToolName + ": " + m_PluginLoader.Message);
                return false;
            }

            if (m_DebugLevel > 0)
            {
                var msg = "Loaded tool runner for StepTool " + m_AnalysisTask.GetCurrentJobToolDescription();
                if (m_PluginLoader.Message.Length > 0)
                    msg += ": " + m_PluginLoader.Message;
                LogMessage(msg);
            }

            try
            {
                // Setup the new tool runner
                m_ToolRunner.Setup(m_MgrSettings, m_AnalysisTask, m_StatusTools, m_SummaryFile, m_MyEMSLUtilities);
            }
            catch (Exception ex)
            {
                LogError("Exception calling ToolRunner.Setup(): " + ex.Message);
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
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine(DateTime.Now.ToString("hh:mm:ss.fff tt") + ": " + strMessage);
            Console.ResetColor();
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
            objStatusFile.ConfigureMessageQueueLogging(logStatusToMessageQueue, messageQueueUri, messageQueueTopicMgrStatus, m_MgrName);
        }

        /// <summary>
        /// Confirms that the drive with the working directory has sufficient free space
        /// Confirms that the remote share for storing results is accessible and has sufficient free space
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        /// <remarks>Disables the manager if the working directory drive does not have enough space</remarks>
        private bool ValidateFreeDiskSpace(out string errorMessage)
        {
            const int DEFAULT_DATASET_STORAGE_MIN_FREE_SPACE_GB = 10;
            const int DEFAULT_TRANSFER_DIR_MIN_FREE_SPACE_GB = 10;

            const int DEFAULT_WORKING_DIR_MIN_FREE_SPACE_MB = 750;
            const int DEFAULT_ORG_DB_DIR_MIN_FREE_SPACE_MB = 750;

            errorMessage = string.Empty;

            try
            {
                var stepToolNameLCase = m_AnalysisTask.GetParam("JobParameters", "StepTool").ToLower();

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

                var transferDir = m_AnalysisTask.GetParam("JobParameters", "transferFolderPath");
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

                var orgDbRequired = m_Resource.GetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired);

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
                LogError("Exception validating free space: " + ex.Message);
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
                Console.WriteLine(msg);
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

            if ((workDirFolders.Length == 0) & (workDirFiles.Length == 1))
            {
                // If the only file in the working directory is a JobParameters xml file,
                //  then try to delete it, since it's likely left over from a previous job that never actually started
                var firstFile = workDirFiles.First();

                if (firstFile.Name.StartsWith(clsGlobal.XML_FILENAME_PREFIX) && firstFile.Name.EndsWith(clsGlobal.XML_FILENAME_EXTENSION))
                {
                    try
                    {
                        LogWarning("Working directory contains a stray JobParameters file, deleting it: " + firstFile.FullName);

                        firstFile.Delete();

                        // Wait 0.5 second and then refresh tmpFilArray
                        Thread.Sleep(500);

                        // Now obtain a new listing of files
                        if (workDir.GetFiles(m_WorkDirPath).Length == 0)
                        {
                            // The directory is now empty
                            return true;
                        }
                    }
                    catch (Exception)
                    {
                        // Deletion failed
                    }
                }
            }

            var errorCount = workDirFiles.Count(item => !PRISM.Files.clsFileTools.IsVimSwapFile(item.FullName));

            if (errorCount == 0)
            {
                // No problems found
                return true;
            }

            LogError("Working directory not empty: " + m_WorkDirPath);
            return false;
        }

        #endregion

        /// <summary>
        /// Event handler for file watcher
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        /// <remarks></remarks>
        private void m_FileWatcher_Changed(object sender, FileSystemEventArgs e)
        {
            m_FileWatcher.EnableRaisingEvents = false;
            m_ConfigChanged = true;

            if (m_DebugLevel > 3)
            {
                LogDebug("Config file changed");
            }
        }
    }
}
