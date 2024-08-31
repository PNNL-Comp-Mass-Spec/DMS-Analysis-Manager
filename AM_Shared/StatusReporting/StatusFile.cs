using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using PRISM;
using PRISMDatabaseUtils;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

namespace AnalysisManagerBase.StatusReporting
{
    /// <summary>
    /// Provides tools for creating and updating an analysis status file
    /// </summary>
    /// <remarks>
    /// Additional functionality:
    ///  1) Can log memory usage stats to a file using MemoryUsageLogger
    ///  2) Looks for the presence of file "AbortProcessingNow.txt"; if found, it sets AbortProcessingNow to true
    ///  and renames the file to "AbortProcessingNow.txt.Done"
    ///  3) Posts status messages to the DMS broker DB at the specified interval
    /// </remarks>
    public class StatusFile : EventNotifier, IStatusFile
    {
        // Ignore Spelling: dta, GlyQ, hyperthreading, ModPlus, PerfLib, Prog, tcp, yyyy-MM-dd, hh:mm:ss, tt

        /// <summary>
        /// Filename that indicates that processing needs to be aborted
        /// </summary>
        public const string ABORT_PROCESSING_NOW_FILENAME = "AbortProcessingNow.txt";

        private const int MAX_ERROR_MESSAGE_COUNT_TO_CACHE = 10;

        private static readonly Regex mFindAmpersand = new("[&]", RegexOptions.Compiled);

        private static readonly Regex mFindLessThanOrGreaterThan = new("[<>]", RegexOptions.Compiled);

        private int mRecentErrorMessageCount;

        private readonly string[] mRecentErrorMessages = new string[MAX_ERROR_MESSAGE_COUNT_TO_CACHE];

        private readonly int mDebugLevel;

        // Instance of class AnalysisMgrSettings
        private readonly IMgrParams mMgrParams;

        /// <summary>
        /// Used to log the memory usage to a status file
        /// </summary>
        private MemoryUsageLogger mMemoryUsageLogger;

        /// <summary>
        /// Used to log messages to the broker DB
        /// </summary>
        private DBStatusLogger mBrokerDBLogger;

        private MessageSender mMessageSender;

        private MessageQueueLogger mQueueLogger;

        private DateTime mLastFileWriteTime;

        private int mWritingErrorCountSaved;

        private DateTime mLastMessageQueueErrorTime;

        private DateTime mLastMessageQueueWarningTime;

        private readonly Dictionary<MgrStatusCodes, string> mMgrStatusMap;

        private readonly Dictionary<TaskStatusCodes, string> mTaskStatusMap;

        private readonly Dictionary<TaskStatusDetailCodes, string> mTaskStatusDetailMap;

        /// <summary>
        /// When true, status messages are being sent directly to the broker database
        /// using stored procedure update_manager_and_task_status
        /// </summary>
        public bool LogToBrokerQueue { get; private set; }

        /// <summary>
        /// Broker database connection string
        /// </summary>
        public string BrokerDBConnectionString
        {
            get
            {
                if (mBrokerDBLogger == null)
                    return string.Empty;

                return mBrokerDBLogger.DBConnectionString;
            }
        }

        /// <summary>
        /// Broker database update interval, in minutes
        /// </summary>
        public float BrokerDBUpdateIntervalMinutes
        {
            get
            {
                if (mBrokerDBLogger == null)
                    return 0;

                return mBrokerDBLogger.DBStatusUpdateIntervalMinutes;
            }
        }

        /// <summary>
        /// Status file path
        /// </summary>
        public string FileNamePath { get; set; }

        /// <summary>
        /// Manager name
        /// </summary>
        public string MgrName { get; set; }

        /// <summary>
        /// Manager status
        /// </summary>
        public MgrStatusCodes MgrStatus { get; set; }

        /// <summary>
        /// Path to the .jobstatus file for jobs running offline
        /// </summary>
        public string OfflineJobStatusFilePath { get; set; }

        /// <summary>
        /// Name of the manager remotely running the job, or of the remote host that this manager pushes jobs to
        /// </summary>
        public string RemoteMgrName { get; set; }

        /// <summary>
        /// Overall CPU utilization of all threads
        /// </summary>
        public int CpuUtilization { get; set; }

        /// <summary>
        /// Step tool name
        /// </summary>
        public string Tool { get; set; }

        /// <summary>
        /// Task status
        /// </summary>
        public TaskStatusCodes TaskStatus { get; set; }

        /// <summary>
        /// Task start time (UTC-based)
        /// </summary>
        public DateTime TaskStartTime { get; set; }

        /// <summary>
        /// Progress (value between 0 and 100)
        /// </summary>
        public float Progress { get; set; }

        /// <summary>
        /// Core usage history for a process being run by the ProgRunner
        /// </summary>
        public Queue<KeyValuePair<DateTime, float>> ProgRunnerCoreUsageHistory { get; private set; }

        /// <summary>
        /// ProcessID of an externally spawned process
        /// </summary>
        /// <remarks>0 if no external process running</remarks>
        public int ProgRunnerProcessID { get; set; }

        /// <summary>
        /// Number of cores in use by an externally spawned process
        /// </summary>
        public float ProgRunnerCoreUsage { get; set; }

        /// <summary>
        /// Current task
        /// </summary>
        public string CurrentOperation { get; set; }

        /// <summary>
        /// Task status detail
        /// </summary>
        public TaskStatusDetailCodes TaskStatusDetail { get; set; }

        /// <summary>
        /// Job number
        /// </summary>
        public int JobNumber { get; set; }

        /// <summary>
        /// Step number
        /// </summary>
        public int JobStep { get; set; }

        /// <summary>
        /// Working directory path
        /// </summary>
        public string WorkDirPath { get; set; }

        /// <summary>
        /// Dataset name
        /// </summary>
        public string Dataset { get; set; }

        /// <summary>
        /// Most recent log message
        /// </summary>
        public string MostRecentLogMessage { get; set; }

        /// <summary>
        /// Most recent job info
        /// </summary>
        public string MostRecentJobInfo { get; set; }

        /// <summary>
        /// Recent error messages
        /// </summary>
        public List<string> RecentErrorMessages
        {
            get
            {
                if (mRecentErrorMessageCount == 0)
                    return new List<string>();

                var messages = new List<string>();

                for (var i = 0; i < mRecentErrorMessageCount; i++)
                {
                    messages.Add(mRecentErrorMessages[i]);
                }
                return messages;
            }
        }

        /// <summary>
        /// Number of spectrum files created or number of scans being searched
        /// </summary>
        public int SpectrumCount { get; set; }

        /// <summary>
        /// URI for the manager status message queue, e.g. tcp://Proto-7.pnl.gov:61616
        /// </summary>
        public string MessageQueueURI { get; private set; }

        /// <summary>
        /// Topic name for the manager status message queue
        /// </summary>
        public string MessageQueueTopic { get; private set; }

        /// <summary>
        /// When true, the status XML is being sent to the manager status message queue
        /// </summary>
        public bool LogToMsgQueue { get; private set; }

        /// <summary>
        /// Set to true to abort processing due to a critical error
        /// </summary>
        /// <remarks>Flag to indicate that the ABORT_PROCESSING_NOW_FILENAME file was detected</remarks>
        public bool AbortProcessingNow { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Manager parameters</param>
        /// <param name="statusFilePath">Full path to status file</param>
        /// <param name="debugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
        public StatusFile(IMgrParams mgrParams, string statusFilePath, int debugLevel)
        {
            mMgrParams = mgrParams;

            mMgrStatusMap = new Dictionary<MgrStatusCodes, string>();
            mTaskStatusMap = new Dictionary<TaskStatusCodes, string>();
            mTaskStatusDetailMap = new Dictionary<TaskStatusDetailCodes, string>();

            DefineEnumToStringMapping(mMgrStatusMap, mTaskStatusMap, mTaskStatusDetailMap);

            FileNamePath = statusFilePath;
            MgrName = string.Empty;
            RemoteMgrName = string.Empty;

            MgrStatus = MgrStatusCodes.STOPPED;

            OfflineJobStatusFilePath = string.Empty;

            TaskStatus = TaskStatusCodes.NO_TASK;
            TaskStatusDetail = TaskStatusDetailCodes.NO_TASK;
            TaskStartTime = DateTime.UtcNow;

            WorkDirPath = string.Empty;

            CurrentOperation = string.Empty;
            MostRecentJobInfo = string.Empty;

            mDebugLevel = debugLevel;

            mLastFileWriteTime = DateTime.MinValue;

            ClearCachedInfo();
        }

        /// <summary>
        /// Looks for file "AbortProcessingNow.txt"
        /// If found, sets property AbortProcessingNow to true
        /// </summary>
        public void CheckForAbortProcessingFile()
        {
            try
            {
                var pathToCheck = Path.Combine(GetStatusFileDirectory(), ABORT_PROCESSING_NOW_FILENAME);

                if (!File.Exists(pathToCheck))
                    return;

                AbortProcessingNow = true;

                var newPath = pathToCheck + ".done";

                File.Delete(newPath);
                File.Move(pathToCheck, newPath);
            }
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Clears the cached information about dataset, job, progress, etc.
        /// </summary>
        private void ClearCachedInfo()
        {
            Progress = 0;
            SpectrumCount = 0;
            Dataset = string.Empty;
            WorkDirPath = string.Empty;
            JobNumber = 0;
            JobStep = 0;
            Tool = string.Empty;

            ProgRunnerProcessID = 0;
            ProgRunnerCoreUsage = 0;

            // Only clear the recent job info if the variable is null
            MostRecentJobInfo ??= string.Empty;

            MostRecentLogMessage = string.Empty;

            mRecentErrorMessageCount = 0;
            mRecentErrorMessages[0] = string.Empty;
        }

        /// <summary>
        /// Configure the Broker DB logging settings
        /// </summary>
        /// <remarks>
        /// When logStatusToBrokerDB is true, status messages are sent directly to the broker database using stored procedure update_manager_and_task_status
        /// Analysis managers typically have logStatusToBrokerDB is false and logStatusToMessageQueue is true
        /// </remarks>
        /// <param name="logStatusToBrokerDB"></param>
        /// <param name="brokerDBConnectionString">Connection string to DMS_Pipeline</param>
        /// <param name="brokerDBStatusUpdateIntervalMinutes"></param>
        public void ConfigureBrokerDBLogging(bool logStatusToBrokerDB, string brokerDBConnectionString, float brokerDBStatusUpdateIntervalMinutes)
        {
            if (Global.OfflineMode)
            {
                LogToBrokerQueue = false;
                return;
            }

            LogToBrokerQueue = logStatusToBrokerDB;

            if (logStatusToBrokerDB)
            {
                if (mBrokerDBLogger == null)
                {
                    mBrokerDBLogger = new DBStatusLogger(brokerDBConnectionString, brokerDBStatusUpdateIntervalMinutes);
                    RegisterEvents(mBrokerDBLogger);
                }
                else
                {
                    mBrokerDBLogger.DBStatusUpdateIntervalMinutes = brokerDBStatusUpdateIntervalMinutes;
                }
            }
            else
            {
                // ReSharper disable once RedundantCheckBeforeAssignment
                if (mBrokerDBLogger != null)
                {
                    // Stop logging to the broker
                    mBrokerDBLogger = null;
                }
            }
        }

        /// <summary>
        /// Configure the memory logging settings
        /// </summary>
        /// <param name="logMemoryUsage"></param>
        /// <param name="minimumMemoryUsageLogIntervalMinutes"></param>
        /// <param name="memoryUsageLogFolderPath"></param>
        public void ConfigureMemoryLogging(bool logMemoryUsage, float minimumMemoryUsageLogIntervalMinutes, string memoryUsageLogFolderPath)
        {
            if (logMemoryUsage)
            {
                if (mMemoryUsageLogger == null)
                {
                    mMemoryUsageLogger = new MemoryUsageLogger(memoryUsageLogFolderPath, minimumMemoryUsageLogIntervalMinutes);
                    RegisterEvents(mMemoryUsageLogger);
                }
                else
                {
                    mMemoryUsageLogger.MinimumLogIntervalMinutes = minimumMemoryUsageLogIntervalMinutes;
                }
            }
            else
            {
                // Stop logging memory usage
                mMemoryUsageLogger = null;
            }
        }

        /// <summary>
        /// Configure the Message Queue logging settings
        /// </summary>
        /// <remarks>
        /// Analysis managers typically have logStatusToBrokerDB is false and logStatusToMessageQueue is true
        /// </remarks>
        /// <param name="logStatusToMessageQueue"></param>
        /// <param name="msgQueueURI"></param>
        /// <param name="messageQueueTopicMgrStatus"></param>
        public void ConfigureMessageQueueLogging(bool logStatusToMessageQueue, string msgQueueURI, string messageQueueTopicMgrStatus)
        {
            if (Global.OfflineMode)
            {
                LogToMsgQueue = false;
                return;
            }

            LogToMsgQueue = logStatusToMessageQueue;
            MessageQueueURI = msgQueueURI;
            MessageQueueTopic = messageQueueTopicMgrStatus;
        }

        /// <summary>
        /// Converts the manager status enum to a string value
        /// </summary>
        /// <param name="statusEnum">A MgrStatus enum</param>
        /// <returns>String representation of input object (sentence case and underscores to spaces)</returns>
        private string ConvertMgrStatusToString(MgrStatusCodes statusEnum)
        {
            return ConvertMgrStatusToString(mMgrStatusMap, statusEnum);
        }

        /// <summary>
        /// Converts the manager status enum to a string value
        /// </summary>
        /// <param name="mgrStatusMap"></param>
        /// <param name="statusEnum">A MgrStatus enum</param>
        /// <returns>String representation of input object (sentence case and underscores to spaces)</returns>
        public static string ConvertMgrStatusToString(Dictionary<MgrStatusCodes, string> mgrStatusMap, MgrStatusCodes statusEnum)
        {
            if (mgrStatusMap.TryGetValue(statusEnum, out var statusText))
                return statusText;

            // Unknown enum
            return "Unknown Mgr Status";
        }

        /// <summary>
        /// Converts the task status enum to a string value
        /// </summary>
        /// <param name="statusEnum">A Task Status enum</param>
        /// <returns>String representation of input object (sentence case and underscores to spaces)</returns>
        private string ConvertTaskStatusToString(TaskStatusCodes statusEnum)
        {
            return ConvertTaskStatusToString(mTaskStatusMap, statusEnum);
        }

        /// <summary>
        /// Converts the task status enum to a string value
        /// </summary>
        /// <param name="taskStatusMap"></param>
        /// <param name="statusEnum">A Task Status enum</param>
        /// <returns>String representation of input object (sentence case and underscores to spaces)</returns>
        public static string ConvertTaskStatusToString(Dictionary<TaskStatusCodes, string> taskStatusMap, TaskStatusCodes statusEnum)
        {
            if (taskStatusMap.TryGetValue(statusEnum, out var statusText))
                return statusText;

            // Unknown enum
            return "Unknown Task Status";
        }

        /// <summary>
        /// Converts the task status detail enum to a string value
        /// </summary>
        /// <param name="statusEnum">A TaskStatusDetail enum</param>
        /// <returns>String representation of input object (sentence case and underscores to spaces)</returns>
        private string ConvertTaskStatusDetailToString(TaskStatusDetailCodes statusEnum)
        {
            return ConvertTaskStatusDetailToString(mTaskStatusDetailMap, statusEnum);
        }

        /// <summary>
        /// Converts the task status detail enum to a string value
        /// </summary>
        /// <param name="taskStatusDetailMap"></param>
        /// <param name="statusEnum">A TaskStatusDetail enum</param>
        /// <returns>String representation of input object (sentence case and underscores to spaces)</returns>
        public static string ConvertTaskStatusDetailToString(Dictionary<TaskStatusDetailCodes, string> taskStatusDetailMap, TaskStatusDetailCodes statusEnum)
        {
            if (taskStatusDetailMap.TryGetValue(statusEnum, out var statusText))
                return statusText;

            // Unknown enum
            return "Unknown Task Status Detail";
        }

        /// <summary>
        /// Converts the string representation of manager status to the enum
        /// </summary>
        /// <param name="statusText">Text from ConvertMgrStatusToString or the string representation of the enum</param>
        /// <returns>Task status enum</returns>
        public MgrStatusCodes ConvertToMgrStatusFromText(string statusText)
        {
            foreach (var item in mMgrStatusMap)
            {
                if (string.Equals(item.Value, statusText, StringComparison.OrdinalIgnoreCase))
                    return item.Key;
            }

            if (Enum.TryParse(statusText, true, out MgrStatusCodes taskStatus))
                return taskStatus;

            return MgrStatusCodes.STOPPED;
        }

        /// <summary>
        /// Converts the string representation of task status to the enum
        /// </summary>
        /// <param name="statusText">Text from ConvertTaskStatusToString or the string representation of the enum</param>
        /// <returns>Task status enum</returns>
        public TaskStatusCodes ConvertToTaskStatusFromText(string statusText)
        {
            foreach (var item in mTaskStatusMap)
            {
                if (string.Equals(item.Value, statusText, StringComparison.OrdinalIgnoreCase))
                    return item.Key;
            }

            if (Enum.TryParse(statusText, true, out TaskStatusCodes taskStatus))
                return taskStatus;

            return TaskStatusCodes.NO_TASK;
        }

        /// <summary>
        /// Converts the string representation of task status detail to the enum
        /// </summary>
        /// <param name="statusText">Text from ConvertTaskStatusDetailToString or the string representation of the enum</param>
        /// <returns>Task status enum</returns>
        public TaskStatusDetailCodes ConvertToTaskDetailStatusFromText(string statusText)
        {
            foreach (var item in mTaskStatusDetailMap)
            {
                if (string.Equals(item.Value, statusText, StringComparison.OrdinalIgnoreCase))
                    return item.Key;
            }

            if (Enum.TryParse(statusText, true, out TaskStatusDetailCodes taskStatus))
                return taskStatus;

            return TaskStatusDetailCodes.NO_TASK;
        }

        /// <summary>
        /// Populate the status code to status description dictionaries
        /// </summary>
        /// <param name="mgrStatusMap"></param>
        /// <param name="taskStatusMap"></param>
        /// <param name="taskStatusDetailMap"></param>
        public static void DefineEnumToStringMapping(
            IDictionary<MgrStatusCodes, string> mgrStatusMap,
            IDictionary<TaskStatusCodes, string> taskStatusMap,
            IDictionary<TaskStatusDetailCodes, string> taskStatusDetailMap)
        {
            mgrStatusMap.Clear();
            mgrStatusMap.Add(MgrStatusCodes.DISABLED_LOCAL, "Disabled Local");
            mgrStatusMap.Add(MgrStatusCodes.DISABLED_MC, "Disabled MC");
            mgrStatusMap.Add(MgrStatusCodes.RUNNING, "Running");
            mgrStatusMap.Add(MgrStatusCodes.STOPPED, "Stopped");
            mgrStatusMap.Add(MgrStatusCodes.STOPPED_ERROR, "Stopped Error");

            taskStatusMap.Clear();
            taskStatusMap.Add(TaskStatusCodes.CLOSING, "Closing");
            taskStatusMap.Add(TaskStatusCodes.NO_TASK, "No Task");
            taskStatusMap.Add(TaskStatusCodes.RUNNING, "Running");
            taskStatusMap.Add(TaskStatusCodes.REQUESTING, "Requesting");
            taskStatusMap.Add(TaskStatusCodes.STOPPED, "Stopped");
            taskStatusMap.Add(TaskStatusCodes.FAILED, "Failed");

            taskStatusDetailMap.Clear();
            taskStatusDetailMap.Add(TaskStatusDetailCodes.DELIVERING_RESULTS, "Delivering Results");
            taskStatusDetailMap.Add(TaskStatusDetailCodes.NO_TASK, "No Task");
            taskStatusDetailMap.Add(TaskStatusDetailCodes.PACKAGING_RESULTS, "Packaging Results");
            taskStatusDetailMap.Add(TaskStatusDetailCodes.RETRIEVING_RESOURCES, "Retrieving Resources");
            taskStatusDetailMap.Add(TaskStatusDetailCodes.RUNNING_TOOL, "Running Tool");
            taskStatusDetailMap.Add(TaskStatusDetailCodes.CLOSING, "Closing");
        }

        /// <summary>
        /// Dispose the message queue objects now
        /// </summary>
        public void DisposeMessageQueue()
        {
            mQueueLogger?.Dispose();
            mMessageSender?.Dispose();
        }

        /// <summary>
        /// Enable logging messages directly to the broker database (DMS_Pipeline)
        /// </summary>
        /// <remarks>If mBrokerDBLogger is already configured, simply exits the method</remarks>
        /// <param name="statusIntervalMinutes"></param>
        public void EnableBrokerDbLoggingNow(int statusIntervalMinutes = 15)
        {
            try
            {
                if (mBrokerDBLogger != null)
                {
                    if ((int)Math.Round(mBrokerDBLogger.DBStatusUpdateIntervalMinutes, 0) != statusIntervalMinutes)
                    {
                        mBrokerDBLogger.DBStatusUpdateIntervalMinutes = statusIntervalMinutes;
                    }

                    return;
                }

                // ReSharper disable once ConvertIfStatementToSwitchStatement
                if (statusIntervalMinutes < 0)
                {
                    statusIntervalMinutes = 15;
                }
                else if (statusIntervalMinutes < 5)
                {
                    statusIntervalMinutes = 5;
                }

                // SQL Server: Data Source=Gigasax;Initial Catalog=DMS_Pipeline
                // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
                var brokerDbConnectionString = mMgrParams.GetParam("BrokerConnectionString");
                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(brokerDbConnectionString, mMgrParams.ManagerName);

                float brokerDbStatusUpdateIntervalMinutes = Math.Min(statusIntervalMinutes, mMgrParams.GetParam("BrokerDBStatusUpdateIntervalMinutes", 60));

                ConfigureBrokerDBLogging(true, connectionStringToUse, brokerDbStatusUpdateIntervalMinutes);
            }
            catch (Exception ex)
            {
                OnWarningEvent("Error enabling broker database logging: {0}", ex.Message);
            }
        }

        /// <summary>
        /// Generate the status XML
        /// </summary>
        /// <param name="status">Status info</param>
        /// <param name="lastUpdate">Last update time (UTC)</param>
        /// <param name="processId">Manager process ID</param>
        /// <param name="cpuUtilization">CPU utilization</param>
        /// <param name="freeMemoryMB">Free memory in MB</param>
        /// <param name="runTimeHours">Runtime, in hours</param>
        private static string GenerateStatusXML(
            StatusFile status,
            DateTime lastUpdate,
            int processId,
            int cpuUtilization,
            float freeMemoryMB,
            float runTimeHours)
        {
            // Note that we use this instead of using .ToString("o")
            // because .NET includes 7 digits of precision for the milliseconds,
            // and SQL Server only allows 3 digits of precision
            const string ISO_8601_DATE = "yyyy-MM-ddTHH:mm:ss.fffK";

            const string LOCAL_TIME_FORMAT = "yyyy-MM-dd hh:mm:ss tt";

            // Create a new memory stream in which to write the XML
            var memStream = new MemoryStream();

            var settings = new XmlWriterSettings
            {
                Encoding = Encoding.UTF8,
                Indent = true,
                IndentChars = "  ",
                NewLineHandling = NewLineHandling.None,
                OmitXmlDeclaration = true
            };

            using var writer = XmlWriter.Create(memStream, settings);

            // Create the XML document in memory
            writer.WriteStartDocument(true);
            writer.WriteComment("Analysis manager job status");

            // General job information
            // Root level element
            writer.WriteStartElement("Root");
            writer.WriteStartElement("Manager");
            writer.WriteElementString("MgrName", ValidateTextLength(status.MgrName, 128));
            writer.WriteElementString("RemoteMgrName", ValidateTextLength(status.RemoteMgrName, 128));
            writer.WriteElementString("MgrStatus", ValidateTextLength(status.ConvertMgrStatusToString(status.MgrStatus), 50));

            var lastUpdateLocal = lastUpdate.ToLocalTime().ToString(LOCAL_TIME_FORMAT);
            var lastStartTimeLocal = lastUpdate.ToLocalTime().ToString(LOCAL_TIME_FORMAT);

            writer.WriteComment("Local status log time: " + lastUpdateLocal);
            writer.WriteComment("Local last start time: " + lastStartTimeLocal);

            // Write out times in the format 2017-07-06T23:23:14.337Z
            writer.WriteElementString("LastUpdate", lastUpdate.ToUniversalTime().ToString(ISO_8601_DATE));
            writer.WriteElementString("LastStartTime", status.TaskStartTime.ToUniversalTime().ToString(ISO_8601_DATE));

            // Write out times in the format 2017-07-06 04:23:14 pm
            writer.WriteElementString("LastUpdateLocal", lastUpdateLocal);
            writer.WriteElementString("LastStartTimeLocal", lastStartTimeLocal);

            writer.WriteElementString("CPUUtilization", cpuUtilization.ToString("##0.0"));
            writer.WriteElementString("FreeMemoryMB", freeMemoryMB.ToString("##0.0"));
            writer.WriteElementString("ProcessID", processId.ToString());
            writer.WriteElementString("ProgRunnerProcessID", status.ProgRunnerProcessID.ToString());
            writer.WriteElementString("ProgRunnerCoreUsage", status.ProgRunnerCoreUsage.ToString("0.00"));
            writer.WriteStartElement("RecentErrorMessages");

            var recentErrorMessages = status.RecentErrorMessages;

            if (recentErrorMessages.Count == 0)
            {
                writer.WriteElementString("ErrMsg", string.Empty);
            }
            else
            {
                foreach (var errMsg in recentErrorMessages)
                {
                    writer.WriteElementString("ErrMsg", ValidateTextLength(errMsg, 1950));
                }
            }
            writer.WriteEndElement(); // RecentErrorMessages
            writer.WriteEndElement(); // Manager

            writer.WriteStartElement("Task");
            writer.WriteElementString("Tool", ValidateTextLength(status.Tool, 128));
            writer.WriteElementString("Status", ValidateTextLength(status.ConvertTaskStatusToString(status.TaskStatus), 50));

            if (status.TaskStatus is TaskStatusCodes.STOPPED or TaskStatusCodes.FAILED or TaskStatusCodes.NO_TASK)
            {
                runTimeHours = 0;
            }

            writer.WriteElementString("Duration", runTimeHours.ToString("0.00"));
            writer.WriteElementString("DurationMinutes", (runTimeHours * 60).ToString("0.0"));

            writer.WriteElementString("Progress", status.Progress.ToString("##0.00"));
            writer.WriteElementString("CurrentOperation", ValidateTextLength(status.CurrentOperation, 255));

            writer.WriteStartElement("TaskDetails");
            writer.WriteElementString("Status", ValidateTextLength(status.ConvertTaskStatusDetailToString(status.TaskStatusDetail), 50));
            writer.WriteElementString("Job", status.JobNumber.ToString());
            writer.WriteElementString("Step", status.JobStep.ToString());
            writer.WriteElementString("Dataset", ValidateTextLength(status.Dataset, 255));
            writer.WriteElementString("WorkDirPath", status.WorkDirPath);
            writer.WriteElementString("MostRecentLogMessage", ValidateTextLength(status.MostRecentLogMessage, 1950));
            writer.WriteElementString("MostRecentJobInfo", ValidateTextLength(status.MostRecentJobInfo, 255));
            writer.WriteElementString("SpectrumCount", status.SpectrumCount.ToString());
            writer.WriteEndElement(); // TaskDetails
            writer.WriteEndElement(); // Task

            var progRunnerCoreUsageHistory = status.ProgRunnerCoreUsageHistory;

            if (status.ProgRunnerProcessID != 0 && progRunnerCoreUsageHistory != null)
            {
                writer.WriteStartElement("ProgRunnerCoreUsage");
                writer.WriteAttributeString("Count", progRunnerCoreUsageHistory.Count.ToString());

                // Dumping the items from the queue to a list because another thread might
                // update ProgRunnerCoreUsageHistory while we're iterating over the items
                var coreUsageHistory = progRunnerCoreUsageHistory.ToList();

                foreach (var coreUsageSample in coreUsageHistory)
                {
                    writer.WriteStartElement("CoreUsageSample");
                    writer.WriteAttributeString("Date", coreUsageSample.Key.ToString(LOCAL_TIME_FORMAT));
                    writer.WriteValue(coreUsageSample.Value.ToString("0.0"));
                    writer.WriteEndElement(); // CoreUsageSample
                }
                writer.WriteEndElement(); // ProgRunnerCoreUsage
            }

            writer.WriteEndElement(); // Root

            // Close out the XML document (but do not close the writer yet)
            writer.WriteEndDocument();
            writer.Flush();

            // Now use a StreamReader to copy the XML text to a string variable
            memStream.Seek(0, SeekOrigin.Begin);
            var memoryStreamReader = new StreamReader(memStream);

            return memoryStreamReader.ReadToEnd();
        }

        /// <summary>
        /// Returns the number of cores
        /// </summary>
        /// <remarks>Should not be affected by hyperthreading, so a computer with two 4-core chips will report 8 cores</remarks>
        /// <returns>The number of cores on this computer</returns>
        public int GetCoreCount()
        {
            return Global.GetCoreCount();
        }

        /// <summary>
        /// Returns the CPU usage
        /// </summary>
        /// <remarks>
        /// This is CPU usage for all running applications, not just this application
        /// For CPU usage of a single application use Global.ProcessInfo.GetCoreUsageByProcessID()
        /// </remarks>
        /// <returns>Value between 0 and 100</returns>
        private float GetCPUUtilization()
        {
            return Global.ProcessInfo.GetCPUUtilization();
        }

        /// <summary>
        /// Returns the amount of free memory
        /// </summary>
        /// <returns>Amount of free memory, in MB</returns>
        public float GetFreeMemoryMB()
        {
            return Global.GetFreeMemoryMB();
        }

        /// <summary>
        /// Return the ProcessID of the Analysis manager
        /// </summary>
        public int GetProcessID()
        {
            return Process.GetCurrentProcess().Id;
        }

        /// <summary>
        /// Total time the job has been running
        /// </summary>
        /// <returns>Number of hours manager has been processing job</returns>
        private float GetRunTime()
        {
            return (float)DateTime.UtcNow.Subtract(TaskStartTime).TotalHours;
        }

        /// <summary>
        /// Get the directory path for the status file tracked by FileNamePath
        /// </summary>
        private string GetStatusFileDirectory()
        {
            var statusFileDirectory = Path.GetDirectoryName(FileNamePath);

            return statusFileDirectory ?? ".";
        }

        private void LogStatusToMessageQueue(string xmlText, string managerName)
        {
            const float MINIMUM_LOG_FAILURE_INTERVAL_MINUTES = 10;

            try
            {
                Global.CheckStopTrace("LogStatusToMessageQueue");

                if (mMessageSender == null)
                {
                    if (mDebugLevel >= 5)
                    {
                        OnStatusEvent("Initializing message queue with URI '" + MessageQueueURI + "' and Topic '" + MessageQueueTopic + "'");
                    }

                    Global.CheckStopTrace("CreateMessageSender");

                    mMessageSender = new MessageSender(MessageQueueURI, MessageQueueTopic, MgrName);

                    RegisterEvents(mMessageSender);

                    // message queue logger sets up local message buffering (so calls to log don't block)
                    // and uses message sender (as a delegate) to actually send off the messages
                    mQueueLogger = new MessageQueueLogger(mMessageSender, this);
                    RegisterEvents(mQueueLogger);

                    var timeOfDay = DateTime.Now;

                    // This variable is true if the local time is between 12:00 am and 12:05 am or 12:00 pm and 12:05 pm
                    var midnightOrNoon = timeOfDay.Hour is 0 or 12 && timeOfDay.Minute is >= 0 and < 5;

                    if (mDebugLevel >= 3 || mDebugLevel >= 1 && midnightOrNoon)
                    {
                        OnStatusEvent("Message queue initialized with URI '" + MessageQueueURI + "'; posting to Topic '" + MessageQueueTopic + "'");
                    }

                    var logTimeInit = DateTime.UtcNow.AddMinutes(-MINIMUM_LOG_FAILURE_INTERVAL_MINUTES * 2);
                    mLastMessageQueueErrorTime = logTimeInit;
                    mLastMessageQueueWarningTime = logTimeInit;
                }

                Global.CheckStopTrace("LogToQueueLogger");

                if (mQueueLogger != null)
                {
                    mQueueLogger?.LogStatusMessage(xmlText, managerName);
                    return;
                }

                if (DateTime.UtcNow.Subtract(mLastMessageQueueWarningTime).TotalMinutes < MINIMUM_LOG_FAILURE_INTERVAL_MINUTES)
                    return;

                mLastMessageQueueWarningTime = DateTime.UtcNow;
                OnWarningEvent("Cannot send message to the queue because mQueueLogger is null");
            }
            catch (Exception ex)
            {
                if (DateTime.UtcNow.Subtract(mLastMessageQueueErrorTime).TotalMinutes >= MINIMUM_LOG_FAILURE_INTERVAL_MINUTES)
                {
                    mLastMessageQueueErrorTime = DateTime.UtcNow;
                    OnErrorEvent("Error in LogStatusToMessageQueue", ex);
                }
            }
        }

        /// <summary>
        /// Send status information to the database
        /// </summary>
        /// <remarks>this method is valid, but the primary way that we track status is when WriteStatusFile calls LogStatusToMessageQueue</remarks>
        /// <param name="forceLogToBrokerDB">If true, will force mBrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        private void LogStatusToBrokerDatabase(bool forceLogToBrokerDB)
        {
            if (mBrokerDBLogger == null)
                return;

            Global.CheckStopTrace("LogStatusToBrokerDatabase");

            var statusInfo = new DBStatusLogger.StatusInfo
            {
                MgrName = MgrName,
                MgrStatus = MgrStatus,
                LastUpdate = DateTime.UtcNow,
                LastStartTime = TaskStartTime,
                CPUUtilization = CpuUtilization,
                FreeMemoryMB = GetFreeMemoryMB(),
                ProcessID = GetProcessID(),
                ProgRunnerProcessID = ProgRunnerProcessID,
                ProgRunnerCoreUsage = ProgRunnerCoreUsage
            };

            if (mRecentErrorMessageCount == 0)
            {
                statusInfo.MostRecentErrorMessage = string.Empty;
            }
            else
            {
                statusInfo.MostRecentErrorMessage = mRecentErrorMessages[0];

                if (mRecentErrorMessageCount > 1)
                {
                    // Append the next two error messages
                    for (var index = 1; index <= mRecentErrorMessageCount - 1; index++)
                    {
                        statusInfo.MostRecentErrorMessage += Environment.NewLine + mRecentErrorMessages[index];

                        if (index >= 2)
                            break;
                    }
                }
            }

            Global.CheckStopTrace("CreateTaskInfoType");

            var task = new DBStatusLogger.TaskInfo
            {
                Tool = Tool,
                Status = TaskStatus,
                DurationHours = GetRunTime(),
                Progress = Progress,
                CurrentOperation = CurrentOperation
            };

            Global.CheckStopTrace("CreateTaskDetailsType");

            task.Details = new DBStatusLogger.TaskDetails
            {
                Status = TaskStatusDetail,
                Job = JobNumber,
                JobStep = JobStep,
                Dataset = Dataset,
                MostRecentLogMessage = MostRecentLogMessage,
                MostRecentJobInfo = MostRecentJobInfo,
                SpectrumCount = SpectrumCount
            };

            statusInfo.Task = task;

            Global.CheckStopTrace("LogStatusToBroker");

            mBrokerDBLogger.LogStatus(statusInfo, forceLogToBrokerDB);
        }

        /// <summary>
        /// Store core usage history
        /// </summary>
        /// <param name="coreUsageHistory"></param>
        public void StoreCoreUsageHistory(Queue<KeyValuePair<DateTime, float>> coreUsageHistory)
        {
            ProgRunnerCoreUsageHistory = coreUsageHistory;
        }

        private void StoreRecentJobInfo(string jobInfo)
        {
            if (!string.IsNullOrEmpty(jobInfo))
            {
                MostRecentJobInfo = jobInfo;
            }
        }

        private void StoreNewErrorMessage(string errorMessage, bool clearExistingMessages)
        {
            if (clearExistingMessages)
            {
                if (errorMessage == null)
                {
                    mRecentErrorMessageCount = 0;
                }
                else
                {
                    mRecentErrorMessageCount = 1;
                    mRecentErrorMessages[0] = errorMessage;
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(errorMessage))
                {
                    if (mRecentErrorMessageCount < MAX_ERROR_MESSAGE_COUNT_TO_CACHE)
                    {
                        mRecentErrorMessageCount++;
                    }

                    // Shift each of the entries by one
                    for (var index = mRecentErrorMessageCount; index >= 1; index += -1)
                    {
                        mRecentErrorMessages[index] = mRecentErrorMessages[index - 1];
                    }

                    // Store the new message
                    mRecentErrorMessages[0] = errorMessage;
                }
            }
        }

        /// <summary>
        /// Copies messages from recentErrorMessages to mRecentErrorMessages; ignores messages that are Nothing or blank
        /// </summary>
        /// <param name="recentErrorMessages"></param>
        private void StoreRecentErrorMessages(IEnumerable<string> recentErrorMessages)
        {
            if (recentErrorMessages == null)
            {
                StoreNewErrorMessage("", true);
            }
            else
            {
                mRecentErrorMessageCount = 0;

                foreach (var errorMsg in recentErrorMessages)
                {
                    if (mRecentErrorMessageCount >= mRecentErrorMessages.Length)
                        break;

                    if (string.IsNullOrWhiteSpace(errorMsg))
                        continue;

                    mRecentErrorMessages[mRecentErrorMessageCount] = errorMsg;
                    mRecentErrorMessageCount++;
                }

                if (mRecentErrorMessageCount == 0)
                {
                    // No valid messages were found in recentErrorMessages
                    // Call StoreNewErrorMessage to clear the stored error messages
                    StoreNewErrorMessage(string.Empty, true);
                }
            }
        }

        /// <summary>
        /// Updates status file to indicate that the manager is closing
        /// </summary>
        /// <param name="managerIdleMessage"></param>
        /// <param name="recentErrorMessages"></param>
        /// <param name="jobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, will force mBrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        public void UpdateClose(string managerIdleMessage, IEnumerable<string> recentErrorMessages, string jobInfo, bool forceLogToBrokerDB)
        {
            ClearCachedInfo();

            MgrStatus = MgrStatusCodes.STOPPED;
            TaskStatus = TaskStatusCodes.NO_TASK;
            TaskStatusDetail = TaskStatusDetailCodes.NO_TASK;
            MostRecentLogMessage = managerIdleMessage;

            StoreRecentErrorMessages(recentErrorMessages);
            StoreRecentJobInfo(jobInfo);

            WriteStatusFile(forceLogToBrokerDB, false);
        }

        /// <summary>
        /// Updates status file
        /// (Overload to update when completion percentage is the only change)
        /// </summary>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        public void UpdateAndWrite(float percentComplete)
        {
            Progress = percentComplete;
            WriteStatusFile();
        }

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="mgrStatus">Job status enum</param>
        /// <param name="taskStatus">Task status enum</param>
        /// <param name="taskStatusDetail">Task status detail enum</param>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        public void UpdateAndWrite(
            MgrStatusCodes mgrStatus,
            TaskStatusCodes taskStatus,
            TaskStatusDetailCodes taskStatusDetail,
            float percentComplete)
        {
            MgrStatus = mgrStatus;
            TaskStatus = taskStatus;
            TaskStatusDetail = taskStatusDetail;
            Progress = percentComplete;
            WriteStatusFile();
        }

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="status">Job status enum</param>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <param name="spectrumCountTotal">Number of DTA files (i.e., spectra files); relevant for SEQUEST, X!Tandem, and Inspect</param>
        public void UpdateAndWrite(TaskStatusCodes status, float percentComplete, int spectrumCountTotal)
        {
            TaskStatus = status;
            Progress = percentComplete;
            SpectrumCount = spectrumCountTotal;

            WriteStatusFile();
        }

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="mgrStatus">Job status code</param>
        /// <param name="taskStatus">Task status code</param>
        /// <param name="taskStatusDetail">Detailed task status</param>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <param name="dtaCount">Number of DTA files (i.e., spectra files); relevant for SEQUEST, X!Tandem, and Inspect</param>
        /// <param name="mostRecentLogMessage">Most recent message posted to the logger (leave blank if unknown)</param>
        /// <param name="mostRecentErrorMessage">Most recent error posted to the logger (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, will force mBrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        public void UpdateAndWrite(
            MgrStatusCodes mgrStatus,
            TaskStatusCodes taskStatus,
            TaskStatusDetailCodes taskStatusDetail,
            float percentComplete,
            int dtaCount,
            string mostRecentLogMessage,
            string mostRecentErrorMessage,
            string recentJobInfo,
            bool forceLogToBrokerDB)
        {
            MgrStatus = mgrStatus;
            TaskStatus = taskStatus;
            TaskStatusDetail = taskStatusDetail;
            Progress = percentComplete;
            SpectrumCount = dtaCount;

            MostRecentLogMessage = mostRecentLogMessage;
            StoreNewErrorMessage(mostRecentErrorMessage, true);
            StoreRecentJobInfo(recentJobInfo);

            WriteStatusFile(forceLogToBrokerDB);
        }

        /// <summary>
        /// Sets status file to show manager idle
        /// </summary>
        public void UpdateIdle()
        {
            UpdateIdle("Manager Idle", false);
        }

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="managerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="forceLogToBrokerDB">If true, will force mBrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        public void UpdateIdle(string managerIdleMessage, bool forceLogToBrokerDB)
        {
            ClearCachedInfo();
            TaskStatus = TaskStatusCodes.NO_TASK;
            MostRecentLogMessage = managerIdleMessage;

            WriteStatusFile(forceLogToBrokerDB, false);
        }

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="managerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="idleErrorMessage">Error message explaining why the manager is idle</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, will force mBrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        public void UpdateIdle(string managerIdleMessage, string idleErrorMessage, string recentJobInfo, bool forceLogToBrokerDB)
        {
            StoreNewErrorMessage(idleErrorMessage, true);
            UpdateIdleWork(managerIdleMessage, recentJobInfo, forceLogToBrokerDB);
        }

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="managerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, will force mBrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        public void UpdateIdle(string managerIdleMessage, IEnumerable<string> recentErrorMessages, string recentJobInfo, bool forceLogToBrokerDB)
        {
            StoreRecentErrorMessages(recentErrorMessages);
            UpdateIdleWork(managerIdleMessage, recentJobInfo, forceLogToBrokerDB);
        }

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="managerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, will force mBrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        private void UpdateIdleWork(string managerIdleMessage, string recentJobInfo, bool forceLogToBrokerDB)
        {
            ClearCachedInfo();
            MgrStatus = MgrStatusCodes.RUNNING;
            TaskStatus = TaskStatusCodes.NO_TASK;
            TaskStatusDetail = TaskStatusDetailCodes.NO_TASK;

            MostRecentLogMessage = managerIdleMessage;

            StoreRecentJobInfo(recentJobInfo);

            OfflineJobStatusFilePath = string.Empty;

            WriteStatusFile(forceLogToBrokerDB, false);
        }

        /// <summary>
        /// Updates status file to show manager disabled
        /// (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        public void UpdateDisabled(MgrStatusCodes managerStatus)
        {
            UpdateDisabled(managerStatus, "Manager Disabled");
        }

        /// <summary>
        /// Logs to the status file that the manager is disabled
        /// (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        /// <param name="managerStatus"></param>
        /// <param name="managerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
        public void UpdateDisabled(MgrStatusCodes managerStatus, string managerDisableMessage)
        {
            UpdateDisabled(managerStatus, managerDisableMessage, new List<string>(), MostRecentJobInfo);
        }

        /// <summary>
        /// Logs to the status file that the manager is disabled
        /// (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        /// <param name="managerStatus"></param>
        /// <param name="managerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        public void UpdateDisabled(MgrStatusCodes managerStatus, string managerDisableMessage, IEnumerable<string> recentErrorMessages,
                                   string recentJobInfo)
        {
            ClearCachedInfo();

            if (managerStatus is not (MgrStatusCodes.DISABLED_LOCAL or MgrStatusCodes.DISABLED_MC))
            {
                managerStatus = MgrStatusCodes.DISABLED_LOCAL;
            }

            MgrStatus = managerStatus;
            TaskStatus = TaskStatusCodes.NO_TASK;
            TaskStatusDetail = TaskStatusDetailCodes.NO_TASK;
            MostRecentLogMessage = managerDisableMessage;

            StoreRecentJobInfo(recentJobInfo);
            StoreRecentErrorMessages(recentErrorMessages);

            WriteStatusFile(true, false);
        }

        /// <summary>
        /// Updates status file to show manager stopped due to a flag file
        /// </summary>
        public void UpdateFlagFileExists()
        {
            UpdateFlagFileExists(new List<string>(), MostRecentJobInfo);
        }

        /// <summary>
        /// Logs to the status file that a flag file exists, indicating that the manager did not exit cleanly on a previous run
        /// </summary>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        public void UpdateFlagFileExists(IEnumerable<string> recentErrorMessages, string recentJobInfo)
        {
            ClearCachedInfo();

            MgrStatus = MgrStatusCodes.STOPPED_ERROR;
            MostRecentLogMessage = "Flag file";
            StoreRecentErrorMessages(recentErrorMessages);
            StoreRecentJobInfo(recentJobInfo);

            WriteStatusFile(true, false);
        }

        /// <summary>
        /// Update the status of a remotely running job
        /// </summary>
        /// <remarks>Pushes the status to the message queue; does not write the XML to disk</remarks>
        /// <param name="status">Status info</param>
        /// <param name="lastUpdate">Last update time (UTC)</param>
        /// <param name="processId">Manager process ID</param>
        /// <param name="cpuUtilization">CPU utilization</param>
        /// <param name="freeMemoryMB">Free memory in MB</param>
        public void UpdateRemoteStatus(StatusFile status, DateTime lastUpdate, int processId, int cpuUtilization, float freeMemoryMB)
        {
            var runTimeHours = (float)lastUpdate.Subtract(status.TaskStartTime).TotalHours;

            WriteStatusFile(status, lastUpdate, processId, cpuUtilization, freeMemoryMB, runTimeHours, false);
        }

        /// <summary>
        /// Examines the length of value; if longer than maxLength characters, the return value is truncated
        /// </summary>
        /// <param name="value">Text value to examine</param>
        /// <param name="maxLength">Maximum allowed number of characters</param>
        /// <param name="accountForXmlEscaping">When true, assume that the text will be converted to XML and less than and greater than signs will be converted to &lt; and &gt;</param>
        /// <returns>Either the original value, or the value truncated to maxLength characters</returns>
        public static string ValidateTextLength(string value, int maxLength, bool accountForXmlEscaping = true)
        {
            if (string.IsNullOrEmpty(value))
                return string.Empty;

            var textLength = value.Length;

            int effectiveLength;

            if (accountForXmlEscaping)
            {
                var textToCheck = textLength <= maxLength ? value : value.Substring(0, maxLength);

                var matches1 = mFindAmpersand.Matches(textToCheck);
                var matches2 = mFindLessThanOrGreaterThan.Matches(textToCheck);

                // & will be replaced with &amp; so add 4 for each character found
                // < and > will be replaced with &lt; and &gt; so add 3 for each character found
                effectiveLength = textLength + matches1.Count * 4 + matches2.Count * 3;
            }
            else
            {
                effectiveLength = textLength;
            }

            return effectiveLength <= maxLength ? value : value.Substring(0, maxLength - (effectiveLength - textLength));
        }

        /// <summary>
        /// Writes the status file
        /// </summary>
        public void WriteStatusFile()
        {
            WriteStatusFile(false);
        }

        /// <summary>
        /// Updates the status in various locations, including on disk and with the message broker and/or broker DB
        /// </summary>
        /// <remarks>The Message queue is always updated if LogToMsgQueue is true</remarks>
        /// <param name="forceLogToBrokerDB">If true, will force mBrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        /// <param name ="usePerformanceCounters">
        /// When true, include the total CPU utilization percent in the status file and call mMemoryUsageLogger.WriteMemoryUsageLogEntry()
        /// This can lead to PerfLib warnings and errors in the Windows Event Log;
        /// thus this should be set to false if simply reporting that the manager is idle
        /// </param>
        public void WriteStatusFile(bool forceLogToBrokerDB, bool usePerformanceCounters = true)
        {
            var lastUpdate = DateTime.MinValue;
            var processId = 0;
            var cpuUtilization = 0;
            float freeMemoryMB = 0;

            try
            {
                lastUpdate = DateTime.UtcNow;

                Global.CheckStopTrace("GetProcessID");
                processId = GetProcessID();

                if (usePerformanceCounters)
                {
                    Global.CheckStopTrace("GetCPUUtilization");
                    cpuUtilization = (int)GetCPUUtilization();
                }
                else if (Global.TraceMode)
                {
                    ConsoleMsgUtils.ShowDebug("Skipping call to GetCPUUtilization in WriteStatusFile");
                }

                Global.CheckStopTrace("GetFreeMemoryMB");
                freeMemoryMB = GetFreeMemoryMB();
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowDebug("Exception getting process ID, CPU utilization, or free memory: " + ex.Message);
            }

            if (Global.TraceMode)
            {
                ConsoleMsgUtils.ShowDebug(
                    "Call WriteStatusFile with processId {0}, CPU {1}%, Free Memory {2:F0} MB",
                    processId, cpuUtilization, freeMemoryMB);
            }

            Global.CheckStopTrace("WriteStatusFile");
            WriteStatusFile(lastUpdate, processId, cpuUtilization, freeMemoryMB, forceLogToBrokerDB, usePerformanceCounters);
        }

        /// <summary>
        /// Updates the status in various locations, including on disk and with the message queue and/or broker DB
        /// </summary>
        /// <remarks>The Message queue is always updated if LogToMsgQueue is true</remarks>
        /// <param name="lastUpdate"></param>
        /// <param name="processId"></param>
        /// <param name="cpuUtilization"></param>
        /// <param name="freeMemoryMB"></param>
        /// <param name="forceLogToBrokerDB">
        /// If true, will force mBrokerDBLogger to report the manager status directly to the database (if initialized)
        /// Otherwise, mBrokerDBLogger only logs the status periodically
        /// Typically false
        /// </param>
        /// <param name ="usePerformanceCounters">
        /// When true, include the total CPU utilization percent in the status file and call mMemoryUsageLogger.WriteMemoryUsageLogEntry()
        /// This can lead to PerfLib warnings and errors in the Windows Event Log;
        /// thus this should be set to false if simply reporting that the manager is idle
        /// </param>
        public void WriteStatusFile(
            DateTime lastUpdate,
            int processId,
            int cpuUtilization,
            float freeMemoryMB,
            bool forceLogToBrokerDB = false,
            bool usePerformanceCounters = true)
        {
            var runTimeHours = GetRunTime();
            WriteStatusFile(this, lastUpdate, processId, cpuUtilization, freeMemoryMB, runTimeHours, true, forceLogToBrokerDB);

            Global.CheckStopTrace("CheckForAbortProcessingFile");
            CheckForAbortProcessingFile();

            if (usePerformanceCounters)
            {
                Global.CheckStopTrace("WriteMemoryUsageLogEntry");

                // Log the memory usage to a local file
                mMemoryUsageLogger?.WriteMemoryUsageLogEntry();
            }
            else if (Global.TraceMode)
            {
                ConsoleMsgUtils.ShowDebug("Skipping call to WriteMemoryUsageLogEntry in WriteStatusFile");
            }
        }

        /// <summary>
        /// Updates the status in various locations, including on disk and with the message queue (and optionally directly to the Broker DB)
        /// </summary>
        /// <remarks>The Message queue is always updated if LogToMsgQueue is true</remarks>
        /// <param name="status">Status info</param>
        /// <param name="lastUpdate">Last update time (UTC)</param>
        /// <param name="processId">Manager process ID</param>
        /// <param name="cpuUtilization">CPU utilization</param>
        /// <param name="freeMemoryMB">Free memory in MB</param>
        /// <param name="runTimeHours">Runtime, in hours</param>
        /// <param name="writeToDisk">If true, write the status file to disk, otherwise, just push to the message queue and/or the Broker DB</param>
        /// <param name="forceLogToBrokerDB">
        /// If true, will force mBrokerDBLogger to report the manager status directly to the database (if initialized)
        /// Otherwise, mBrokerDBLogger only logs the status periodically
        /// Typically false</param>
        private void WriteStatusFile(
            StatusFile status,
            DateTime lastUpdate,
            int processId,
            int cpuUtilization,
            float freeMemoryMB,
            float runTimeHours,
            bool writeToDisk,
            bool forceLogToBrokerDB = false)
        {
            string xmlText;

            try
            {
                Global.CheckStopTrace("GenerateStatusXML");
                xmlText = GenerateStatusXML(status, lastUpdate, processId, cpuUtilization, freeMemoryMB, runTimeHours);

                if (writeToDisk)
                {
                    Global.CheckStopTrace("WriteStatusFileToDisk");
                    WriteStatusFileToDisk(xmlText);
                }
            }
            catch (Exception ex)
            {
                OnWarningEvent("Error generating status info: {0}", ex.Message);
                xmlText = string.Empty;
            }

            if (LogToMsgQueue)
            {
                // Send the XML text to a message queue
                LogStatusToMessageQueue(xmlText, status.MgrName);
            }

            if (mBrokerDBLogger != null)
            {
                // Send the status info to the Broker DB
                // Note that mBrokerDBLogger() only logs the status every x minutes (unless forceLogToBrokerDB is true)
                LogStatusToBrokerDatabase(forceLogToBrokerDB);
            }
        }

        private void WriteStatusFileToDisk(string xmlText)
        {
            const int MIN_FILE_WRITE_INTERVAL_SECONDS = 2;

            if (DateTime.UtcNow.Subtract(mLastFileWriteTime).TotalSeconds < MIN_FILE_WRITE_INTERVAL_SECONDS)
            {
                return;
            }

            // We will write out the Status XML to a temporary file, then rename the temp file to the primary file

            if (FileNamePath == null)
            {
                return;
            }

            var tempStatusFilePath = Path.Combine(GetStatusFileDirectory(), Path.GetFileNameWithoutExtension(FileNamePath) + "_Temp.xml");

            mLastFileWriteTime = DateTime.UtcNow;

            var logWarning = true;

            // ReSharper disable once StringLiteralTypo
            if (Tool.IndexOf("GlyQ", StringComparison.OrdinalIgnoreCase) >= 0 || Tool.IndexOf("ModPlus", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                if (mDebugLevel < 3)
                    logWarning = false;
            }

            var success = WriteStatusFileToDisk(tempStatusFilePath, xmlText, logWarning);

            if (success)
            {
                try
                {
                    File.Copy(tempStatusFilePath, FileNamePath, true);
                }
                catch (Exception ex)
                {
                    // Copy failed; this is normal when running GlyQ-IQ or MODPlus because they have multiple threads running
                    if (logWarning)
                    {
                        // Log a warning that the file copy failed
                        OnWarningEvent("Unable to copy temporary status file to the final status file ({0} to {1}): {2}",
                            Path.GetFileName(tempStatusFilePath),
                            Path.GetFileName(FileNamePath),
                            ex.Message);
                    }
                }

                try
                {
                    File.Delete(tempStatusFilePath);
                }
                catch (Exception ex)
                {
                    // Delete failed; this is normal when running GlyQ-IQ or MODPlus because they have multiple threads running
                    if (logWarning)
                    {
                        // Log a warning that the file delete failed
                        OnWarningEvent("Unable to delete temporary status file ({0}): {1}", Path.GetFileName(tempStatusFilePath), ex.Message);
                    }
                }
            }
            else
            {
                // Error writing to the temporary status file; try the primary file
                WriteStatusFileToDisk(FileNamePath, xmlText, logWarning);
            }

            if (string.IsNullOrWhiteSpace(OfflineJobStatusFilePath))
                return;

            try
            {
                // We're running an offline analysis job (Global.OfflineMode is true)
                // Update the JobStatus file in the TaskQueue directory
                File.Copy(FileNamePath, OfflineJobStatusFilePath, true);
            }
            catch (Exception ex)
            {
                OnDebugEvent("Error copying the status file to " + OfflineJobStatusFilePath + ": " + ex.Message);
            }
        }

        private bool WriteStatusFileToDisk(string statusFilePath, string xmlText, bool logWarning)
        {
            const int WRITE_FAILURE_LOG_THRESHOLD = 5;

            bool success;

            try
            {
                // Write out the XML text to a file
                // If the file is in use by another process, the writing will fail
                using (var writer = new StreamWriter(new FileStream(statusFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine(xmlText);
                }

                // Reset the error counter
                mWritingErrorCountSaved = 0;

                success = true;
            }
            catch (Exception ex)
            {
                // Increment the error counter
                mWritingErrorCountSaved++;

                if (mWritingErrorCountSaved >= WRITE_FAILURE_LOG_THRESHOLD && logWarning)
                {
                    // 5 or more errors in a row have occurred
                    // Post an entry to the log, only when writingErrorCountSaved is 5, 10, 20, 30, etc.
                    if (mWritingErrorCountSaved == WRITE_FAILURE_LOG_THRESHOLD || mWritingErrorCountSaved % 10 == 0)
                    {
                        OnWarningEvent("Error writing status file {0}: {1}", Path.GetFileName(statusFilePath), ex.Message);
                    }
                }
                success = false;
            }

            return success;
        }
    }
}
