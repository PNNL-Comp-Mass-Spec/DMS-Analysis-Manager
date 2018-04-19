using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Xml;
using PRISM;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{

    /// <summary>
    /// Provides tools for creating and updating an analysis status file
    /// </summary>
    /// <remarks>
    /// Additional functionality:
    ///  1) Can log memory usage stats to a file using clsMemoryUsageLogger
    ///  2) Looks for the presence of file "AbortProcessingNow.txt"; if found, it sets AbortProcessingNow to true
    ///  and renames the file to "AbortProcessingNow.txt.Done"
    ///  3) Posts status messages to the DMS broker DB at the specified interval
    /// </remarks>
    public class clsStatusFile : clsEventNotifier, IStatusFile
    {

        #region "Module variables"

        /// <summary>
        /// Filename that indicates that processing needs to be aborted
        /// </summary>
        public const string ABORT_PROCESSING_NOW_FILENAME = "AbortProcessingNow.txt";

        /// <summary>
        /// Flag to indicate that the ABORT_PROCESSING_NOW_FILENAME file was detected
        /// </summary>
        private bool m_AbortProcessingNow;

        const int MAX_ERROR_MESSAGE_COUNT_TO_CACHE = 10;

        private int m_RecentErrorMessageCount;
        private readonly string[] m_RecentErrorMessages = new string[MAX_ERROR_MESSAGE_COUNT_TO_CACHE];

        private Queue<KeyValuePair<DateTime, float>> m_ProgRunnerCoreUsageHistory;

        private readonly int m_DebugLevel;

        /// <summary>
        /// Used to log the memory usage to a status file
        /// </summary>
        private clsMemoryUsageLogger m_MemoryUsageLogger;

        /// <summary>
        /// Used to log messages to the broker DB
        /// </summary>
        private clsDBStatusLogger m_BrokerDBLogger;

        private clsMessageSender m_MessageSender;

        private clsMessageQueueLogger m_QueueLogger;

        private DateTime m_LastFileWriteTime;

        private int m_WritingErrorCountSaved;

        private DateTime m_LastMessageQueueErrorTime;

        private DateTime m_LastMessageQueueWarningTime;

        private readonly Dictionary<EnumMgrStatus, string> mMgrStatusMap;

        private readonly Dictionary<EnumTaskStatus, string> mTaskStatusMap;

        private readonly Dictionary<EnumTaskStatusDetail, string> mTaskStatusDetailMap;

        #endregion

        #region "Properties"

        /// <summary>
        /// When true, status messages are being sent directly to the broker database
        /// using stored procedure UpdateManagerAndTaskStatus
        /// </summary>
        public bool LogToBrokerQueue { get; private set; }

        /// <summary>
        /// Broker database connection string
        /// </summary>
        public string BrokerDBConnectionString
        {
            get
            {
                if (m_BrokerDBLogger == null)
                    return string.Empty;

                return m_BrokerDBLogger.DBConnectionString;
            }

        }

        /// <summary>
        /// Broker database update interval, in minutes
        /// </summary>
        public float BrokerDBUpdateIntervalMinutes
        {
            get
            {
                if (m_BrokerDBLogger == null)
                    return 0;

                return m_BrokerDBLogger.DBStatusUpdateIntervalMinutes;
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
        public EnumMgrStatus MgrStatus { get; set; }

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
        /// <remarks></remarks>
        public int CpuUtilization { get; set; }

        /// <summary>
        /// Step tool name
        /// </summary>
        public string Tool { get; set; }

        /// <summary>
        /// Task status
        /// </summary>
        public EnumTaskStatus TaskStatus { get; set; }

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
        public Queue<KeyValuePair<DateTime, float>> ProgRunnerCoreUsageHistory => m_ProgRunnerCoreUsageHistory;

        /// <summary>
        /// ProcessID of an externally spawned process
        /// </summary>
        /// <remarks>0 if no external process running</remarks>
        public int ProgRunnerProcessID { get; set; }

        /// <summary>
        /// Number of cores in use by an externally spawned process
        /// </summary>
        /// <remarks></remarks>
        public float ProgRunnerCoreUsage { get; set; }

        /// <summary>
        /// Current task
        /// </summary>
        public string CurrentOperation { get; set; }

        /// <summary>
        /// Task status detail
        /// </summary>
        public EnumTaskStatusDetail TaskStatusDetail { get; set; }

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
                if (m_RecentErrorMessageCount == 0)
                    return new List<string>();

                var messages = new List<string>();
                for (var i = 0; i < m_RecentErrorMessageCount; i++)
                {
                    messages.Add(m_RecentErrorMessages[i]);
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
        public bool AbortProcessingNow => m_AbortProcessingNow;

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="statusFilePath">Full path to status file</param>
        /// <param name="debugLevel"></param>
        /// <remarks></remarks>
        public clsStatusFile(string statusFilePath, int debugLevel)
        {
            mMgrStatusMap = new Dictionary<EnumMgrStatus, string>();
            mTaskStatusMap = new Dictionary<EnumTaskStatus, string>();
            mTaskStatusDetailMap = new Dictionary<EnumTaskStatusDetail, string>();

            DefineEnumToStringMapping(mMgrStatusMap, mTaskStatusMap, mTaskStatusDetailMap);

            FileNamePath = statusFilePath;
            MgrName = string.Empty;
            RemoteMgrName = string.Empty;

            MgrStatus = EnumMgrStatus.STOPPED;

            OfflineJobStatusFilePath = string.Empty;

            TaskStatus = EnumTaskStatus.NO_TASK;
            TaskStatusDetail = EnumTaskStatusDetail.NO_TASK;
            TaskStartTime = DateTime.UtcNow;

            WorkDirPath = string.Empty;

            CurrentOperation = string.Empty;
            MostRecentJobInfo = string.Empty;

            m_DebugLevel = debugLevel;

            m_LastFileWriteTime = DateTime.MinValue;

            ClearCachedInfo();

        }

        /// <summary>
        /// Configure the memory logging settings
        /// </summary>
        /// <param name="logMemoryUsage"></param>
        /// <param name="minimumMemoryUsageLogIntervalMinutes"></param>
        /// <param name="memoryUsageLogFolderPath"></param>
        /// <remarks></remarks>
        public void ConfigureMemoryLogging(bool logMemoryUsage, float minimumMemoryUsageLogIntervalMinutes, string memoryUsageLogFolderPath)
        {
            if (logMemoryUsage)
            {
                if (m_MemoryUsageLogger == null)
                {
                    m_MemoryUsageLogger = new clsMemoryUsageLogger(memoryUsageLogFolderPath, minimumMemoryUsageLogIntervalMinutes);
                    RegisterEvents(m_MemoryUsageLogger);
                }
                else
                {
                    m_MemoryUsageLogger.MinimumLogIntervalMinutes = minimumMemoryUsageLogIntervalMinutes;
                }
            }
            else
            {
                // Stop logging memory usage
                m_MemoryUsageLogger = null;
            }
        }

        /// <summary>
        /// Configure the Broker DB logging settings
        /// </summary>
        /// <param name="logStatusToBrokerDB"></param>
        /// <param name="brokerDBConnectionString"></param>
        /// <param name="brokerDBStatusUpdateIntervalMinutes"></param>
        /// <remarks>
        /// When logStatusToBrokerDB is true, status messages are sent directly to the broker database
        /// using stored procedure UpdateManagerAndTaskStatus
        /// </remarks>
        public void ConfigureBrokerDBLogging(bool logStatusToBrokerDB, string brokerDBConnectionString, float brokerDBStatusUpdateIntervalMinutes)
        {
            if (clsGlobal.OfflineMode)
            {
                LogToBrokerQueue = false;
                return;
            }

            LogToBrokerQueue = logStatusToBrokerDB;

            if (logStatusToBrokerDB)
            {
                if (m_BrokerDBLogger == null)
                {
                    m_BrokerDBLogger = new clsDBStatusLogger(brokerDBConnectionString, brokerDBStatusUpdateIntervalMinutes);
                }
                else
                {
                    m_BrokerDBLogger.DBStatusUpdateIntervalMinutes = brokerDBStatusUpdateIntervalMinutes;
                }
            }
            else
            {
                // ReSharper disable once RedundantCheckBeforeAssignment
                if (m_BrokerDBLogger != null)
                {
                    // Stop logging to the broker
                    m_BrokerDBLogger = null;
                }
            }
        }

        /// <summary>
        /// Configure the Message Queue logging settings
        /// </summary>
        /// <param name="logStatusToMessageQueue"></param>
        /// <param name="msgQueueURI"></param>
        /// <param name="messageQueueTopicMgrStatus"></param>
        /// <remarks></remarks>
        public void ConfigureMessageQueueLogging(bool logStatusToMessageQueue, string msgQueueURI, string messageQueueTopicMgrStatus)
        {
            if (clsGlobal.OfflineMode)
            {
                LogToMsgQueue = false;
                return;
            }

            LogToMsgQueue = logStatusToMessageQueue;
            MessageQueueURI = msgQueueURI;
            MessageQueueTopic = messageQueueTopicMgrStatus;
        }

        /// <summary>
        /// Looks for file "AbortProcessingNow.txt"
        /// If found, sets property AbortProcessingNow to True
        /// </summary>
        /// <remarks></remarks>
        public void CheckForAbortProcessingFile()
        {
            try
            {
                var pathToCheck = Path.Combine(GetStatusFileDirectory(), ABORT_PROCESSING_NOW_FILENAME);

                if (!File.Exists(pathToCheck))
                    return;

                m_AbortProcessingNow = true;

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
        /// <remarks></remarks>
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
            if (MostRecentJobInfo == null)
            {
                MostRecentJobInfo = string.Empty;
            }

            MostRecentLogMessage = string.Empty;

            m_RecentErrorMessageCount = 0;
            m_RecentErrorMessages[0] = string.Empty;
        }

        /// <summary>
        /// Converts the string representation of manager status to the enum
        /// </summary>
        /// <param name="statusText">Text from ConvertMgrStatusToString or the string representation of the enum</param>
        /// <returns>Task status enum</returns>
        /// <remarks></remarks>
        public EnumMgrStatus ConvertToMgrStatusFromText(string statusText)
        {
            foreach (var item in mMgrStatusMap)
            {
                if (string.Equals(item.Value, statusText, StringComparison.OrdinalIgnoreCase))
                    return item.Key;
            }

            if (Enum.TryParse(statusText, true, out EnumMgrStatus taskStatus))
                return taskStatus;

            return EnumMgrStatus.STOPPED;
        }

        /// <summary>
        /// Converts the string representation of task status to the enum
        /// </summary>
        /// <param name="statusText">Text from ConvertTaskStatusToString or the string representation of the enum</param>
        /// <returns>Task status enum</returns>
        /// <remarks></remarks>
        public EnumTaskStatus ConvertToTaskStatusFromText(string statusText)
        {
            foreach (var item in mTaskStatusMap)
            {
                if (string.Equals(item.Value, statusText, StringComparison.OrdinalIgnoreCase))
                    return item.Key;
            }

            if (Enum.TryParse(statusText, true, out EnumTaskStatus taskStatus))
                return taskStatus;

            return EnumTaskStatus.NO_TASK;
        }

        /// <summary>
        /// Converts the string representation of task status detail to the enum
        /// </summary>
        /// <param name="statusText">Text from ConvertTaskStatusDetailToString or the string representation of the enum</param>
        /// <returns>Task status enum</returns>
        /// <remarks></remarks>
        public EnumTaskStatusDetail ConvertToTaskDetailStatusFromText(string statusText)
        {
            foreach (var item in mTaskStatusDetailMap)
            {
                if (string.Equals(item.Value, statusText, StringComparison.OrdinalIgnoreCase))
                    return item.Key;
            }

            if (Enum.TryParse(statusText, true, out EnumTaskStatusDetail taskStatus))
                return taskStatus;

            return EnumTaskStatusDetail.NO_TASK;
        }

        /// <summary>
        /// Converts the manager status enum to a string value
        /// </summary>
        /// <param name="statusEnum">A MgrStatus enum</param>
        /// <returns>String representation of input object (sentence case and underscores to spaces)</returns>
        /// <remarks></remarks>
        private string ConvertMgrStatusToString(EnumMgrStatus statusEnum)
        {
            if (mMgrStatusMap.TryGetValue(statusEnum, out var statusText))
                return statusText;

            // Unknown enum
            return "Unknown Mgr Status";

        }

        /// <summary>
        /// Converts the task status enum to a string value
        /// </summary>
        /// <param name="statusEnum">A Task Status enum</param>
        /// <returns>String representation of input object (sentence case and underscores to spaces)</returns>
        /// <remarks></remarks>
        private string ConvertTaskStatusToString(EnumTaskStatus statusEnum)
        {

            if (mTaskStatusMap.TryGetValue(statusEnum, out var statusText))
                return statusText;

            // Unknown enum
            return "Unknown Task Status";

        }

        /// <summary>
        /// Converts the task status detail enum to a string value
        /// </summary>
        /// <param name="statusEnum">A TaskStatusDetail enum</param>
        /// <returns>String representation of input object (sentence case and underscores to spaces)</returns>
        /// <remarks></remarks>
        private string ConvertTaskStatusDetailToString(EnumTaskStatusDetail statusEnum)
        {

            if (mTaskStatusDetailMap.TryGetValue(statusEnum, out var statusText))
                return statusText;

            // Unknown enum
            return "Unknown Task Status Detail";

        }

        private void DefineEnumToStringMapping(
            IDictionary<EnumMgrStatus, string> mgrStatusMap,
            IDictionary<EnumTaskStatus, string> taskStatusMap,
            IDictionary<EnumTaskStatusDetail, string> taskStatusDetailMap)
        {

            mgrStatusMap.Clear();
            mgrStatusMap.Add(EnumMgrStatus.DISABLED_LOCAL, "Disabled Local");
            mgrStatusMap.Add(EnumMgrStatus.DISABLED_MC, "Disabled MC");
            mgrStatusMap.Add(EnumMgrStatus.RUNNING, "Running");
            mgrStatusMap.Add(EnumMgrStatus.STOPPED, "Stopped");
            mgrStatusMap.Add(EnumMgrStatus.STOPPED_ERROR, "Stopped Error");

            taskStatusMap.Clear();
            taskStatusMap.Add(EnumTaskStatus.CLOSING, "Closing");
            taskStatusMap.Add(EnumTaskStatus.NO_TASK, "No Task");
            taskStatusMap.Add(EnumTaskStatus.RUNNING, "Running");
            taskStatusMap.Add(EnumTaskStatus.REQUESTING, "Requesting");
            taskStatusMap.Add(EnumTaskStatus.STOPPED, "Stopped");
            taskStatusMap.Add(EnumTaskStatus.FAILED, "Failed");

            taskStatusDetailMap.Clear();
            taskStatusDetailMap.Add(EnumTaskStatusDetail.DELIVERING_RESULTS, "Delivering Results");
            taskStatusDetailMap.Add(EnumTaskStatusDetail.NO_TASK, "No Task");
            taskStatusDetailMap.Add(EnumTaskStatusDetail.PACKAGING_RESULTS, "Packaging Results");
            taskStatusDetailMap.Add(EnumTaskStatusDetail.RETRIEVING_RESOURCES, "Retrieving Resources");
            taskStatusDetailMap.Add(EnumTaskStatusDetail.RUNNING_TOOL, "Running Tool");
            taskStatusDetailMap.Add(EnumTaskStatusDetail.CLOSING, "Closing");

        }

        /// <summary>
        /// Returns the number of cores
        /// </summary>
        /// <returns>The number of cores on this computer</returns>
        /// <remarks>Should not be affected by hyperthreading, so a computer with two 4-core chips will report 8 cores</remarks>
        public int GetCoreCount()
        {
            return clsGlobal.GetCoreCount();
        }

        /// <summary>
        /// Returns the CPU usage
        /// </summary>
        /// <returns>Value between 0 and 100</returns>
        /// <remarks>
        /// This is CPU usage for all running applications, not just this application
        /// For CPU usage of a single application use clsGlobal.ProcessInfo.GetCoreUsageByProcessID()
        /// </remarks>
        private float GetCPUUtilization()
        {
            return clsGlobal.ProcessInfo.GetCPUUtilization();
        }

        /// <summary>
        /// Returns the amount of free memory
        /// </summary>
        /// <returns>Amount of free memory, in MB</returns>
        public float GetFreeMemoryMB()
        {
            var freeMemoryMB = clsGlobal.GetFreeMemoryMB();

            return freeMemoryMB;
        }

        /// <summary>
        /// Return the ProcessID of the Analysis manager
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public int GetProcessID()
        {
            var processID = Process.GetCurrentProcess().Id;
            return processID;
        }

        /// <summary>
        /// Get the folder path for the status file tracked by FileNamePath
        /// </summary>
        /// <returns></returns>
        private string GetStatusFileDirectory()
        {
            var statusFileDirectory = Path.GetDirectoryName(FileNamePath);

            if (statusFileDirectory == null)
                return ".";

            return statusFileDirectory;
        }

        private void LogStatusToMessageQueue(string xmlText, string managerName)
        {
            const float MINIMUM_LOG_FAILURE_INTERVAL_MINUTES = 10;

            try
            {

                if (m_MessageSender == null)
                {
                    if (m_DebugLevel >= 5)
                    {
                        OnStatusEvent("Initializing message queue with URI '" + MessageQueueURI + "' and Topic '" + MessageQueueTopic + "'");
                    }

                    m_MessageSender = new clsMessageSender(MessageQueueURI, MessageQueueTopic, MgrName);
                    m_MessageSender.ErrorEvent += MessageSender_ErrorEvent;

                    // message queue logger sets up local message buffering (so calls to log don't block)
                    // and uses message sender (as a delegate) to actually send off the messages
                    m_QueueLogger = new clsMessageQueueLogger();
                    RegisterEvents(m_QueueLogger);
                    m_QueueLogger.Sender += m_MessageSender.SendMessage;

                    var timeOfDay = DateTime.Now;

                    // This variable is true if the local time is between 12:00 am and 12:05 am or 12:00 pm and 12:05 pm
                    var midnightOrNoon = (timeOfDay.Hour == 0 || timeOfDay.Hour == 12) && timeOfDay.Minute >= 0 && timeOfDay.Minute < 5;

                    if (m_DebugLevel >= 3 || m_DebugLevel >= 1 && midnightOrNoon)
                    {
                        OnStatusEvent("Message queue initialized with URI '" + MessageQueueURI + "'; posting to Topic '" + MessageQueueTopic + "'");
                    }

                    var logTimeInit = DateTime.UtcNow.AddMinutes(-MINIMUM_LOG_FAILURE_INTERVAL_MINUTES * 2);
                    m_LastMessageQueueErrorTime = logTimeInit;
                    m_LastMessageQueueWarningTime = logTimeInit;
                }

                if (m_QueueLogger != null)
                {
                    m_QueueLogger?.LogStatusMessage(xmlText, managerName);
                    return;
                }

                if (DateTime.UtcNow.Subtract(m_LastMessageQueueWarningTime).TotalMinutes < MINIMUM_LOG_FAILURE_INTERVAL_MINUTES)
                    return;

                m_LastMessageQueueWarningTime = DateTime.UtcNow;
                OnWarningEvent("Cannot send message to the queue because m_QueueLogger is null");
            }
            catch (Exception ex)
            {
                if (DateTime.UtcNow.Subtract(m_LastMessageQueueErrorTime).TotalMinutes >= MINIMUM_LOG_FAILURE_INTERVAL_MINUTES)
                {
                    m_LastMessageQueueErrorTime = DateTime.UtcNow;
                    var msg = "Error in LogStatusToMessageQueue: " + ex.Message;
                    OnErrorEvent(msg, ex);
                }

            }

        }

        /// <summary>
        /// Send status information to the database
        /// </summary>
        /// <param name="forceLogToBrokerDB">If true, will force m_BrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        /// <remarks>This function is valid, but the primary way that we track status is when WriteStatusFile calls LogStatusToMessageQueue</remarks>
        private void LogStatusToBrokerDatabase(bool forceLogToBrokerDB)
        {
            if (m_BrokerDBLogger == null)
                return;

            var udtStatusInfo = new clsDBStatusLogger.udtStatusInfoType
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

            if (m_RecentErrorMessageCount == 0)
            {
                udtStatusInfo.MostRecentErrorMessage = string.Empty;
            }
            else
            {
                udtStatusInfo.MostRecentErrorMessage = m_RecentErrorMessages[0];
                if (m_RecentErrorMessageCount > 1)
                {
                    // Append the next two error messages
                    for (var index = 1; index <= m_RecentErrorMessageCount - 1; index++)
                    {
                        udtStatusInfo.MostRecentErrorMessage += Environment.NewLine + m_RecentErrorMessages[index];
                        if (index >= 2)
                            break;
                    }
                }
            }

            var udtTask = new clsDBStatusLogger.udtTaskInfoType
            {
                Tool = Tool,
                Status = TaskStatus,
                DurationHours = GetRunTime(),
                Progress = Progress,
                CurrentOperation = CurrentOperation
            };

            var udtTaskDetails = new clsDBStatusLogger.udtTaskDetailsType
            {
                Status = TaskStatusDetail,
                Job = JobNumber,
                JobStep = JobStep,
                Dataset = Dataset,
                MostRecentLogMessage = MostRecentLogMessage,
                MostRecentJobInfo = MostRecentJobInfo,
                SpectrumCount = SpectrumCount
            };

            udtTask.TaskDetails = udtTaskDetails;
            udtStatusInfo.Task = udtTask;

            m_BrokerDBLogger.LogStatus(udtStatusInfo, forceLogToBrokerDB);
        }

        /// <summary>
        /// Store core usage history
        /// </summary>
        /// <param name="coreUsageHistory"></param>
        public void StoreCoreUsageHistory(Queue<KeyValuePair<DateTime, float>> coreUsageHistory)
        {
            m_ProgRunnerCoreUsageHistory = coreUsageHistory;
        }

        private void StoreRecentJobInfo(string JobInfo)
        {
            if (!string.IsNullOrEmpty(JobInfo))
            {
                MostRecentJobInfo = JobInfo;
            }
        }

        private void StoreNewErrorMessage(string errorMessage, bool clearExistingMessages)
        {

            if (clearExistingMessages)
            {
                if (errorMessage == null)
                {
                    m_RecentErrorMessageCount = 0;
                }
                else
                {
                    m_RecentErrorMessageCount = 1;
                    m_RecentErrorMessages[0] = errorMessage;
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(errorMessage))
                {
                    if (m_RecentErrorMessageCount < MAX_ERROR_MESSAGE_COUNT_TO_CACHE)
                    {
                        m_RecentErrorMessageCount += 1;
                    }

                    // Shift each of the entries by one
                    for (var index = m_RecentErrorMessageCount; index >= 1; index += -1)
                    {
                        m_RecentErrorMessages[index] = m_RecentErrorMessages[index - 1];
                    }

                    // Store the new message
                    m_RecentErrorMessages[0] = errorMessage;
                }
            }

        }

        /// <summary>
        /// Copies messages from recentErrorMessages to m_RecentErrorMessages; ignores messages that are Nothing or blank
        /// </summary>
        /// <param name="recentErrorMessages"></param>
        /// <remarks></remarks>
        private void StoreRecentErrorMessages(IEnumerable<string> recentErrorMessages)
        {
            if (recentErrorMessages == null)
            {
                StoreNewErrorMessage("", true);
            }
            else
            {
                m_RecentErrorMessageCount = 0;

                foreach (var errorMsg in recentErrorMessages)
                {
                    if (m_RecentErrorMessageCount >= m_RecentErrorMessages.Length)
                        break;

                    if (string.IsNullOrWhiteSpace(errorMsg))
                        continue;

                    m_RecentErrorMessages[m_RecentErrorMessageCount] = errorMsg;
                    m_RecentErrorMessageCount += 1;
                }


                if (m_RecentErrorMessageCount == 0)
                {
                    // No valid messages were found in recentErrorMessages
                    // Call StoreNewErrorMessage to clear the stored error messages
                    StoreNewErrorMessage("", true);
                }
            }
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
        /// <param name="forceLogToBrokerDB">If true, will force m_BrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        /// <remarks>The Message queue is always updated if LogToMsgQueue is true</remarks>
        public void WriteStatusFile(bool forceLogToBrokerDB)
        {
            var lastUpdate = DateTime.MinValue;
            var processId = 0;
            var cpuUtilization = 0;
            float freeMemoryMB = 0;

            try
            {
                lastUpdate = DateTime.UtcNow;
                processId = GetProcessID();

                cpuUtilization = (int)GetCPUUtilization();
                freeMemoryMB = GetFreeMemoryMB();
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            WriteStatusFile(lastUpdate, processId, cpuUtilization, freeMemoryMB, forceLogToBrokerDB);
        }

        /// <summary>
        /// Updates the status in various locations, including on disk and with the message queue and/or broker DB
        /// </summary>
        /// <param name="lastUpdate"></param>
        /// <param name="processId"></param>
        /// <param name="cpuUtilization"></param>
        /// <param name="freeMemoryMB"></param>
        /// <param name="forceLogToBrokerDB">
        /// If true, will force m_BrokerDBLogger to report the manager status directly to the database (if initialized)
        /// Otherwise, m_BrokerDBLogger only logs the status periodically
        /// Typically false</param>
        /// <remarks>The Message queue is always updated if LogToMsgQueue is true</remarks>
        public void WriteStatusFile(DateTime lastUpdate, int processId, int cpuUtilization, float freeMemoryMB, bool forceLogToBrokerDB = false)
        {

            var runTimeHours = GetRunTime();
            WriteStatusFile(this, lastUpdate, processId, cpuUtilization, freeMemoryMB, runTimeHours, true, forceLogToBrokerDB);

            CheckForAbortProcessingFile();

            // Log the memory usage to a local file
            m_MemoryUsageLogger?.WriteMemoryUsageLogEntry();

        }

        /// <summary>
        /// Updates the status in various locations, including on disk and with the message queue (and optionally directly to the Broker DB)
        /// </summary>
        /// <param name="status"></param>
        /// <param name="lastUpdate"></param>
        /// <param name="processId"></param>
        /// <param name="cpuUtilization">CPU utilization</param>
        /// <param name="freeMemoryMB">Free memory in MB</param>
        /// <param name="runTimeHours">Runtime, in hours</param>
        /// <param name="writeToDisk">If true, write the status file to disk, otherwise, just push to the message queue and/or the Broker DB</param>
        /// <param name="forceLogToBrokerDB">
        /// If true, will force m_BrokerDBLogger to report the manager status directly to the database (if initialized)
        /// Otherwise, m_BrokerDBLogger only logs the status periodically
        /// Typically false</param>
        /// <remarks>The Message queue is always updated if LogToMsgQueue is true</remarks>
        private void WriteStatusFile(
            clsStatusFile status,
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
                xmlText = GenerateStatusXML(status, lastUpdate, processId, cpuUtilization, freeMemoryMB, runTimeHours);

                if (writeToDisk)
                {
                    WriteStatusFileToDisk(xmlText);
                }
            }
            catch (Exception ex)
            {
                var msg = "Error generating status info: " + ex.Message;
                OnWarningEvent(msg);
                xmlText = string.Empty;
            }

            if (LogToMsgQueue)
            {
                // Send the XML text to a message queue
                LogStatusToMessageQueue(xmlText, status.MgrName);
            }

            if (m_BrokerDBLogger != null)
            {
                // Send the status info to the Broker DB
                // Note that m_BrokerDBLogger() only logs the status every x minutes (unless forceLogToBrokerDB = True)

                LogStatusToBrokerDatabase(forceLogToBrokerDB);
            }
        }

        private static string GenerateStatusXML(
            clsStatusFile status,
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
            using (var xWriter = new XmlTextWriter(memStream, System.Text.Encoding.UTF8))
            {
                xWriter.Formatting = Formatting.Indented;
                xWriter.Indentation = 2;

                // Create the XML document in memory
                xWriter.WriteStartDocument(true);
                xWriter.WriteComment("Analysis manager job status");

                // General job information
                // Root level element
                xWriter.WriteStartElement("Root");
                xWriter.WriteStartElement("Manager");
                xWriter.WriteElementString("MgrName", status.MgrName);
                xWriter.WriteElementString("RemoteMgrName", status.RemoteMgrName);
                xWriter.WriteElementString("MgrStatus", status.ConvertMgrStatusToString(status.MgrStatus));

                xWriter.WriteComment("Local status log time: " + lastUpdate.ToLocalTime().ToString(LOCAL_TIME_FORMAT));
                xWriter.WriteComment("Local last start time: " + status.TaskStartTime.ToLocalTime().ToString(LOCAL_TIME_FORMAT));

                // Write out times in the format 2017-07-06T23:23:14.337Z
                xWriter.WriteElementString("LastUpdate", lastUpdate.ToUniversalTime().ToString(ISO_8601_DATE));

                xWriter.WriteElementString("LastStartTime", status.TaskStartTime.ToUniversalTime().ToString(ISO_8601_DATE));

                xWriter.WriteElementString("CPUUtilization", cpuUtilization.ToString("##0.0"));
                xWriter.WriteElementString("FreeMemoryMB", freeMemoryMB.ToString("##0.0"));
                xWriter.WriteElementString("ProcessID", processId.ToString());
                xWriter.WriteElementString("ProgRunnerProcessID", status.ProgRunnerProcessID.ToString());
                xWriter.WriteElementString("ProgRunnerCoreUsage", status.ProgRunnerCoreUsage.ToString("0.00"));
                xWriter.WriteStartElement("RecentErrorMessages");

                var recentErrorMessages = status.RecentErrorMessages;
                if (recentErrorMessages.Count == 0)
                {
                    xWriter.WriteElementString("ErrMsg", string.Empty);
                }
                else
                {
                    foreach (var errMsg in recentErrorMessages)
                    {
                        xWriter.WriteElementString("ErrMsg", errMsg);
                    }
                }
                xWriter.WriteEndElement(); // RecentErrorMessages
                xWriter.WriteEndElement(); // Manager

                xWriter.WriteStartElement("Task");
                xWriter.WriteElementString("Tool", status.Tool);
                xWriter.WriteElementString("Status", status.ConvertTaskStatusToString(status.TaskStatus));

                if (status.TaskStatus == EnumTaskStatus.STOPPED ||
                    status.TaskStatus == EnumTaskStatus.FAILED ||
                    status.TaskStatus == EnumTaskStatus.NO_TASK)
                {
                    runTimeHours = 0;
                }

                xWriter.WriteElementString("Duration", runTimeHours.ToString("0.00"));
                xWriter.WriteElementString("DurationMinutes", (runTimeHours * 60).ToString("0.0"));

                xWriter.WriteElementString("Progress", status.Progress.ToString("##0.00"));
                xWriter.WriteElementString("CurrentOperation", status.CurrentOperation);

                xWriter.WriteStartElement("TaskDetails");
                xWriter.WriteElementString("Status", status.ConvertTaskStatusDetailToString(status.TaskStatusDetail));
                xWriter.WriteElementString("Job", Convert.ToString(status.JobNumber));
                xWriter.WriteElementString("Step", Convert.ToString(status.JobStep));
                xWriter.WriteElementString("Dataset", status.Dataset);
                xWriter.WriteElementString("WorkDirPath", status.WorkDirPath);
                xWriter.WriteElementString("MostRecentLogMessage", status.MostRecentLogMessage);
                xWriter.WriteElementString("MostRecentJobInfo", status.MostRecentJobInfo);
                xWriter.WriteElementString("SpectrumCount", status.SpectrumCount.ToString());
                xWriter.WriteEndElement(); // TaskDetails
                xWriter.WriteEndElement(); // Task

                var progRunnerCoreUsageHistory = status.ProgRunnerCoreUsageHistory;

                if (status.ProgRunnerProcessID != 0 && progRunnerCoreUsageHistory != null)
                {
                    xWriter.WriteStartElement("ProgRunnerCoreUsage");
                    xWriter.WriteAttributeString("Count", progRunnerCoreUsageHistory.Count.ToString());

                    // Dumping the items from the queue to a list because another thread might
                    // update m_ProgRunnerCoreUsageHistory while we're iterating over the items
                    var coreUsageHistory = progRunnerCoreUsageHistory.ToList();

                    foreach (var coreUsageSample in coreUsageHistory)
                    {
                        xWriter.WriteStartElement("CoreUsageSample");
                        xWriter.WriteAttributeString("Date", coreUsageSample.Key.ToString(LOCAL_TIME_FORMAT));
                        xWriter.WriteValue(coreUsageSample.Value.ToString("0.0"));
                        xWriter.WriteEndElement(); // CoreUsageSample
                    }
                    xWriter.WriteEndElement(); // ProgRunnerCoreUsage
                }

                xWriter.WriteEndElement(); // Root

                // Close out the XML document (but do not close XWriter yet)
                xWriter.WriteEndDocument();
                xWriter.Flush();

                // Now use a StreamReader to copy the XML text to a string variable
                memStream.Seek(0, SeekOrigin.Begin);
                var srMemoryStreamReader = new StreamReader(memStream);
                var xmlText = srMemoryStreamReader.ReadToEnd();

                srMemoryStreamReader.Close();
                memStream.Close();

                return xmlText;
            }

        }

        private void WriteStatusFileToDisk(string xmlText)
        {
            const int MIN_FILE_WRITE_INTERVAL_SECONDS = 2;

            if (!(DateTime.UtcNow.Subtract(m_LastFileWriteTime).TotalSeconds >= MIN_FILE_WRITE_INTERVAL_SECONDS))
                return;

            // We will write out the Status XML to a temporary file, then rename the temp file to the primary file

            if (FileNamePath == null)
                return;

            var tempStatusFilePath = Path.Combine(GetStatusFileDirectory(), Path.GetFileNameWithoutExtension(FileNamePath) + "_Temp.xml");

            m_LastFileWriteTime = DateTime.UtcNow;

            var logWarning = true;
            if (Tool.ToLower().Contains("glyq") || Tool.ToLower().Contains("modplus"))
            {
                if (m_DebugLevel < 3)
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
                        OnWarningEvent("Unable to copy temporary status file to the final status file (" + Path.GetFileName(tempStatusFilePath) +
                                       " to " + Path.GetFileName(FileNamePath) + "):" + ex.Message);
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
                        OnWarningEvent("Unable to delete temporary status file (" + Path.GetFileName(tempStatusFilePath) + "): " + ex.Message);
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
                // We're running an offline analysis job (clsGlobal.OfflineMode is true)
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
                m_WritingErrorCountSaved = 0;

                success = true;

            }
            catch (Exception ex)
            {
                // Increment the error counter
                m_WritingErrorCountSaved += 1;

                if (m_WritingErrorCountSaved >= WRITE_FAILURE_LOG_THRESHOLD && logWarning)
                {
                    // 5 or more errors in a row have occurred
                    // Post an entry to the log, only when writingErrorCountSaved is 5, 10, 20, 30, etc.
                    if (m_WritingErrorCountSaved == WRITE_FAILURE_LOG_THRESHOLD || m_WritingErrorCountSaved % 10 == 0)
                    {
                        var msg = "Error writing status file " + Path.GetFileName(statusFilePath) + ": " + ex.Message;
                        OnWarningEvent(msg);
                    }
                }
                success = false;
            }

            return success;

        }

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="managerIdleMessage"></param>
        /// <param name="recentErrorMessages"></param>
        /// <param name="jobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, will force m_BrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        /// <remarks></remarks>
        public void UpdateClose(string managerIdleMessage, IEnumerable<string> recentErrorMessages, string jobInfo, bool forceLogToBrokerDB)
        {
            ClearCachedInfo();

            MgrStatus = EnumMgrStatus.STOPPED;
            TaskStatus = EnumTaskStatus.NO_TASK;
            TaskStatusDetail = EnumTaskStatusDetail.NO_TASK;
            MostRecentLogMessage = managerIdleMessage;

            StoreRecentErrorMessages(recentErrorMessages);
            StoreRecentJobInfo(jobInfo);

            WriteStatusFile(forceLogToBrokerDB);

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
        /// <param name="eMgrStatus">Job status enum</param>
        /// <param name="eTaskStatus">Task status enum</param>
        /// <param name="eTaskStatusDetail">Task status detail enum</param>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <remarks></remarks>
        public void UpdateAndWrite(EnumMgrStatus eMgrStatus, EnumTaskStatus eTaskStatus, EnumTaskStatusDetail eTaskStatusDetail,
                                   float percentComplete)
        {
            MgrStatus = eMgrStatus;
            TaskStatus = eTaskStatus;
            TaskStatusDetail = eTaskStatusDetail;
            Progress = percentComplete;
            WriteStatusFile();
        }

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="status">Job status enum</param>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <param name="spectrumCountTotal">Number of DTA files (i.e., spectra files); relevant for Sequest, X!Tandem, and Inspect</param>
        /// <remarks></remarks>
        public void UpdateAndWrite(EnumTaskStatus status, float percentComplete, int spectrumCountTotal)
        {
            TaskStatus = status;
            Progress = percentComplete;
            SpectrumCount = spectrumCountTotal;

            WriteStatusFile();
        }

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="eMgrStatus">Job status code</param>
        /// <param name="eTaskStatus">Task status code</param>
        /// <param name="eTaskStatusDetail">Detailed task status</param>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <param name="dtaCount">Number of DTA files (i.e., spectra files); relevant for Sequest, X!Tandem, and Inspect</param>
        /// <param name="mostRecentLogMessage">Most recent message posted to the logger (leave blank if unknown)</param>
        /// <param name="mostRecentErrorMessage">Most recent error posted to the logger (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, will force m_BrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        /// <remarks></remarks>
        public void UpdateAndWrite(
            EnumMgrStatus eMgrStatus,
            EnumTaskStatus eTaskStatus,
            EnumTaskStatusDetail eTaskStatusDetail,
            float percentComplete,
            int dtaCount,
            string mostRecentLogMessage,
            string mostRecentErrorMessage,
            string recentJobInfo,
            bool forceLogToBrokerDB)
        {
            MgrStatus = eMgrStatus;
            TaskStatus = eTaskStatus;
            TaskStatusDetail = eTaskStatusDetail;
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
        /// <remarks></remarks>
        public void UpdateIdle()
        {
            UpdateIdle("Manager Idle", false);
        }

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="managerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="forceLogToBrokerDB">If true, will force m_BrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        /// <remarks></remarks>
        public void UpdateIdle(string managerIdleMessage, bool forceLogToBrokerDB)
        {
            ClearCachedInfo();
            TaskStatus = EnumTaskStatus.NO_TASK;
            MostRecentLogMessage = managerIdleMessage;

            WriteStatusFile(forceLogToBrokerDB);
        }

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="managerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="idleErrorMessage">Error message explaining why the manager is idle</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, will force m_BrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        /// <remarks></remarks>
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
        /// <param name="forceLogToBrokerDB">If true, will force m_BrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        /// <remarks></remarks>
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
        /// <param name="forceLogToBrokerDB">If true, will force m_BrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        private void UpdateIdleWork(string managerIdleMessage, string recentJobInfo, bool forceLogToBrokerDB)
        {
            ClearCachedInfo();
            MgrStatus = EnumMgrStatus.RUNNING;
            TaskStatus = EnumTaskStatus.NO_TASK;
            TaskStatusDetail = EnumTaskStatusDetail.NO_TASK;

            MostRecentLogMessage = managerIdleMessage;

            StoreRecentJobInfo(recentJobInfo);

            OfflineJobStatusFilePath = string.Empty;

            WriteStatusFile(forceLogToBrokerDB);
        }

        /// <summary>
        /// Updates status file to show manager disabled
        /// (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        /// <remarks></remarks>
        public void UpdateDisabled(EnumMgrStatus managerStatus)
        {
            UpdateDisabled(managerStatus, "Manager Disabled");
        }

        /// <summary>
        /// Logs to the status file that the manager is disabled
        /// (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        /// <param name="managerStatus"></param>
        /// <param name="managerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
        /// <remarks></remarks>
        public void UpdateDisabled(EnumMgrStatus managerStatus, string managerDisableMessage)
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
        /// <remarks></remarks>
        public void UpdateDisabled(EnumMgrStatus managerStatus, string managerDisableMessage, IEnumerable<string> recentErrorMessages,
                                   string recentJobInfo)
        {
            ClearCachedInfo();

            if (!(managerStatus == EnumMgrStatus.DISABLED_LOCAL || managerStatus == EnumMgrStatus.DISABLED_MC))
            {
                managerStatus = EnumMgrStatus.DISABLED_LOCAL;
            }
            MgrStatus = managerStatus;
            TaskStatus = EnumTaskStatus.NO_TASK;
            TaskStatusDetail = EnumTaskStatusDetail.NO_TASK;
            MostRecentLogMessage = managerDisableMessage;

            StoreRecentJobInfo(recentJobInfo);
            StoreRecentErrorMessages(recentErrorMessages);

            WriteStatusFile(true);
        }

        /// <summary>
        /// Updates status file to show manager stopped due to a flag file
        /// </summary>
        /// <remarks></remarks>
        public void UpdateFlagFileExists()
        {
            UpdateFlagFileExists(new List<string>(), MostRecentJobInfo);
        }

        /// <summary>
        /// Logs to the status file that a flag file exists, indicating that the manager did not exit cleanly on a previous run
        /// </summary>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <remarks></remarks>
        public void UpdateFlagFileExists(IEnumerable<string> recentErrorMessages, string recentJobInfo)
        {
            ClearCachedInfo();

            MgrStatus = EnumMgrStatus.STOPPED_ERROR;
            MostRecentLogMessage = "Flag file";
            StoreRecentErrorMessages(recentErrorMessages);
            StoreRecentJobInfo(recentJobInfo);

            WriteStatusFile(true);
        }

        /// <summary>
        /// Update the status of a remotely running job
        /// </summary>
        /// <param name="status"></param>
        /// <param name="lastUpdate"></param>
        /// <param name="processId"></param>
        /// <param name="cpuUtilization"></param>
        /// <param name="freeMemoryMB"></param>
        /// <remarks>Pushes the status to the message queue; does not write the XML to disk</remarks>
        public void UpdateRemoteStatus(clsStatusFile status, DateTime lastUpdate, int processId, int cpuUtilization, float freeMemoryMB)
        {
            var runTimeHours = (float)lastUpdate.Subtract(status.TaskStartTime).TotalHours;

            WriteStatusFile(status, lastUpdate, processId, cpuUtilization, freeMemoryMB, runTimeHours, false);
        }

        /// <summary>
        /// Total time the job has been running
        /// </summary>
        /// <returns>Number of hours manager has been processing job</returns>
        /// <remarks></remarks>
        private float GetRunTime()
        {
            return (float)DateTime.UtcNow.Subtract(TaskStartTime).TotalHours;
        }

        /// <summary>
        /// Dispose the message queue objects now
        /// </summary>
        public void DisposeMessageQueue()
        {
            m_QueueLogger?.Dispose();
            m_MessageSender?.Dispose();
        }

        #endregion

        #region "Event handlers"

        private void MessageSender_ErrorEvent(string message, Exception ex)
        {
            OnErrorEvent(message, ex);
        }

        #endregion

    }
}
