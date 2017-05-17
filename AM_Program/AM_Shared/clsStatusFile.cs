using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Xml;
using PRISM;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
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
        private clsMessageQueueLogger m_MessageQueueLogger;
        private clsMessageSender m_MessageSender;

        private clsMessageQueueLogger m_QueueLogger;
        private PerformanceCounter mCPUUsagePerformanceCounter;

        private DateTime m_LastFileWriteTime;
        private int m_WritingErrorCountSaved;
        private DateTime m_LastMessageQueueErrorTime;

        #endregion

        #region "Properties"
        public string FileNamePath { get; set; }

        public string MgrName { get; set; }

        public EnumMgrStatus MgrStatus { get; set; }

        /// <summary>
        /// Overall CPU utilization of all threads
        /// </summary>
        /// <remarks></remarks>
        public int CpuUtilization { get; set; }

        public string Tool { get; set; }

        public EnumTaskStatus TaskStatus { get; set; }

        public DateTime TaskStartTime { get; set; }

        /// <summary>
        /// Progress (value between 0 and 100)
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public float Progress { get; set; }

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

        public string CurrentOperation { get; set; }

        public EnumTaskStatusDetail TaskStatusDetail { get; set; }

        public int JobNumber { get; set; }

        public int JobStep { get; set; }

        public string WorkDirPath { get; set; }

        /// <summary>
        /// Dataset name
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string Dataset { get; set; }

        /// <summary>
        /// Most recent log message
        /// </summary>
        public string MostRecentLogMessage { get; set; }

        /// <summary>
        /// Most recent job info
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string MostRecentJobInfo { get; set; }

        /// <summary>
        /// Number of spectrum files created
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public int SpectrumCount { get; set; }

        /// <summary>
        /// URI for the manager status message queue, e.g. tcp://Proto-7.pnl.gov:61616
        /// </summary>
        public string MessageQueueURI { get; set; }

        /// <summary>
        /// Topic name for the manager status message queue
        /// </summary>
        public string MessageQueueTopic { get; set; }

        /// <summary>
        /// When true, log messages to the manager status message queue
        /// </summary>
        public bool LogToMsgQueue { get; set; }

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
            FileNamePath = statusFilePath;
            MgrName = string.Empty;
            MgrStatus = EnumMgrStatus.STOPPED;

            TaskStatus = EnumTaskStatus.NO_TASK;
            TaskStatusDetail = EnumTaskStatusDetail.NO_TASK;
            TaskStartTime = DateTime.UtcNow;

            Dataset = string.Empty;
            WorkDirPath = string.Empty;

            CurrentOperation = string.Empty;
            MostRecentJobInfo = string.Empty;

            m_DebugLevel = debugLevel;

            m_LastFileWriteTime = DateTime.MinValue;

            ClearCachedInfo();

            InitializePerformanceCounters();
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
                }
                else
                {
                    m_MemoryUsageLogger.MinimumLogIntervalMinutes = minimumMemoryUsageLogIntervalMinutes;
                }
            }
            else
            {
                if ((m_MemoryUsageLogger != null))
                {
                    // Stop logging memory usage
                    m_MemoryUsageLogger = null;
                }
            }
        }

        /// <summary>
        /// Configure the Broker DB logging settings
        /// </summary>
        /// <param name="LogStatusToBrokerDB"></param>
        /// <param name="BrokerDBConnectionString"></param>
        /// <param name="BrokerDBStatusUpdateIntervalMinutes"></param>
        /// <remarks></remarks>
        public void ConfigureBrokerDBLogging(bool LogStatusToBrokerDB, string BrokerDBConnectionString, float BrokerDBStatusUpdateIntervalMinutes)
        {
            if (LogStatusToBrokerDB)
            {
                if (m_BrokerDBLogger == null)
                {
                    m_BrokerDBLogger = new clsDBStatusLogger(BrokerDBConnectionString, BrokerDBStatusUpdateIntervalMinutes);
                }
                else
                {
                    m_BrokerDBLogger.DBStatusUpdateIntervalMinutes = BrokerDBStatusUpdateIntervalMinutes;
                }
            }
            else
            {
                if ((m_BrokerDBLogger != null))
                {
                    // Stop logging to the broker
                    m_BrokerDBLogger = null;
                }
            }
        }

        /// <summary>
        /// Configure the Message Queue logging settings
        /// </summary>
        /// <param name="LogStatusToMessageQueue"></param>
        /// <param name="MsgQueueURI"></param>
        /// <param name="MessageQueueTopicMgrStatus"></param>
        /// <param name="ClientName"></param>
        /// <remarks></remarks>
        public void ConfigureMessageQueueLogging(bool LogStatusToMessageQueue, string MsgQueueURI, string MessageQueueTopicMgrStatus, string ClientName)
        {
            LogToMsgQueue = LogStatusToMessageQueue;
            MessageQueueURI = MsgQueueURI;
            MessageQueueTopic = MessageQueueTopicMgrStatus;

            if (!LogToMsgQueue & (m_MessageQueueLogger != null))
            {
                // Stop logging to the message queue
                m_MessageQueueLogger = null;
            }
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
                var strPathToCheck = Path.Combine(GetStatusFileDirectory(), ABORT_PROCESSING_NOW_FILENAME);

                if (!File.Exists(strPathToCheck))
                    return;

                m_AbortProcessingNow = true;

                var strNewPath = strPathToCheck + ".done";

                File.Delete(strNewPath);
                File.Move(strPathToCheck, strNewPath);
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

            // Only clear the recent job info if the variable is Nothing
            if (MostRecentJobInfo == null)
            {
                MostRecentJobInfo = string.Empty;
            }

            MostRecentLogMessage = string.Empty;

            m_RecentErrorMessageCount = 0;
            m_RecentErrorMessages[0] = string.Empty;
        }

        /// <summary>
        /// Converts the job status enum to a string value
        /// </summary>
        /// <param name="StatusEnum">An JobStatus object</param>
        /// <returns>String representation of input object</returns>
        /// <remarks></remarks>
        private string ConvertTaskStatusToString(EnumTaskStatus StatusEnum)
        {

            // Converts a status enum to a string
            switch (StatusEnum)
            {
                case EnumTaskStatus.CLOSING:
                    return "Closing";
                case EnumTaskStatus.NO_TASK:
                    return "No Task";
                case EnumTaskStatus.RUNNING:
                    return "Running";
                case EnumTaskStatus.REQUESTING:
                    return "Requesting";
                case EnumTaskStatus.STOPPED:
                    return "Stopped";
                case EnumTaskStatus.FAILED:
                    return "Failed";
                default:
                    // Should never get here
                    return "Unknown Task Status";
            }

        }

        /// <summary>
        /// Converts the job status enum to a string value
        /// </summary>
        /// <param name="StatusEnum">An JobStatus object</param>
        /// <returns>String representation of input object</returns>
        /// <remarks></remarks>
        private string ConvertMgrStatusToString(EnumMgrStatus StatusEnum)
        {

            // Converts a status enum to a string
            switch (StatusEnum)
            {
                case EnumMgrStatus.DISABLED_LOCAL:
                    return "Disabled Local";
                case EnumMgrStatus.DISABLED_MC:
                    return "Disabled MC";
                case EnumMgrStatus.RUNNING:
                    return "Running";
                case EnumMgrStatus.STOPPED:
                    return "Stopped";
                case EnumMgrStatus.STOPPED_ERROR:
                    return "Stopped Error";
                default:
                    // Should never get here
                    return "Unknown Mgr Status";
            }

        }

        /// <summary>
        /// Converts the job status enum to a string value
        /// </summary>
        /// <param name="StatusEnum">An JobStatus object</param>
        /// <returns>String representation of input object</returns>
        /// <remarks></remarks>
        private string ConvertTaskStatusDetailToString(EnumTaskStatusDetail StatusEnum)
        {

            // Converts a status enum to a string
            switch (StatusEnum)
            {
                case EnumTaskStatusDetail.DELIVERING_RESULTS:
                    return "Delivering Results";
                case EnumTaskStatusDetail.NO_TASK:
                    return "No Task";
                case EnumTaskStatusDetail.PACKAGING_RESULTS:
                    return "Packaging Results";
                case EnumTaskStatusDetail.RETRIEVING_RESOURCES:
                    return "Retrieving Resources";
                case EnumTaskStatusDetail.RUNNING_TOOL:
                    return "Running Tool";
                case EnumTaskStatusDetail.CLOSING:
                    return "Closing";
                default:
                    // Should never get here
                    return "Unknown Task Status Detail";
            }

        }

        private void InitializePerformanceCounters()
        {
            var blnVirtualMachineOnPIC = clsGlobal.UsingVirtualMachineOnPIC();

            try
            {
                mCPUUsagePerformanceCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total")
                {
                    ReadOnly = true
                };
            }
            catch (Exception ex)
            {
                // To avoid seeing this in the logs continually, we will only post this log message between 12 am and 12:30 am
                if (!blnVirtualMachineOnPIC && DateTime.Now.Hour == 0 && DateTime.Now.Minute <= 30)
                {
                    OnErrorEvent("Error instantiating the Processor.[% Processor Time] performance counter " +
                                 "(this message is only logged between 12 am and 12:30 am): " + ex.Message);
                }
            }

        }

        /// <summary>
        /// Returns the CPU usage
        /// </summary>
        /// <returns>Value between 0 and 100</returns>
        /// <remarks>
        /// This is CPU usage for all running applications, not just this application
        /// For CPU usage of a single application use PRISM.clsProgRunner.GetCoreUsageByProcessID()
        /// </remarks>
        private float GetCPUUtilization()
        {
            float cpuUtilization = 0;

            try
            {
                if ((mCPUUsagePerformanceCounter != null))
                {
                    cpuUtilization = mCPUUsagePerformanceCounter.NextValue();
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            return cpuUtilization;

        }

        /// <summary>
        /// Returns the number of cores
        /// </summary>
        /// <returns>The number of cores on this computer</returns>
        /// <remarks>Should not be affected by hyperthreading, so a computer with two 4-core chips will report 8 cores</remarks>
        public int GetCoreCount()
        {
            return PRISMWin.clsProcessStats.GetCoreCount();
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

        private void LogStatusToMessageQueue(string strStatusXML)
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
                    m_QueueLogger.Sender += m_MessageSender.SendMessage;

                    if (m_DebugLevel >= 3)
                    {
                        OnStatusEvent("Message queue initialized with URI '" + MessageQueueURI + "'; posting to Topic '" + MessageQueueTopic + "'");
                    }

                }

                m_QueueLogger?.LogStatusMessage(strStatusXML);
            }
            catch (Exception ex)
            {
                if (DateTime.UtcNow.Subtract(m_LastMessageQueueErrorTime).TotalMinutes >= MINIMUM_LOG_FAILURE_INTERVAL_MINUTES)
                {
                    m_LastMessageQueueErrorTime = DateTime.UtcNow;
                    var msg = "Error in clsStatusFile.LogStatusToMessageQueue (B): " + ex.Message;
                    OnErrorEvent(msg, ex);
                }

            }


        }

        /// <summary>
        /// Send status information to the database
        /// </summary>
        /// <remarks>This function is valid, but the primary way that we track status is when WriteStatusFile calls LogStatusToMessageQueue</remarks>
        private void LogStatusToBrokerDatabase(bool forceLogToBrokerDB)
        {
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
                    for (var intIndex = 1; intIndex <= m_RecentErrorMessageCount - 1; intIndex++)
                    {
                        udtStatusInfo.MostRecentErrorMessage += Environment.NewLine + m_RecentErrorMessages[intIndex];
                        if (intIndex >= 2)
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

        private void StoreNewErrorMessage(string strErrorMessage, bool blnClearExistingMessages)
        {

            if (blnClearExistingMessages)
            {
                if (strErrorMessage == null)
                {
                    m_RecentErrorMessageCount = 0;
                }
                else
                {
                    m_RecentErrorMessageCount = 1;
                    m_RecentErrorMessages[0] = strErrorMessage;
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(strErrorMessage))
                {
                    if (m_RecentErrorMessageCount < MAX_ERROR_MESSAGE_COUNT_TO_CACHE)
                    {
                        m_RecentErrorMessageCount += 1;
                    }

                    // Shift each of the entries by one
                    for (var intIndex = m_RecentErrorMessageCount; intIndex >= 1; intIndex += -1)
                    {
                        m_RecentErrorMessages[intIndex] = m_RecentErrorMessages[intIndex - 1];
                    }

                    // Store the new message
                    m_RecentErrorMessages[0] = strErrorMessage;
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
        /// <remarks></remarks>
        public void WriteStatusFile()
        {
            WriteStatusFile(false);
        }

        /// <summary>
        /// Updates the status in various locations, including on disk and with the message broker and/or broker DB
        /// </summary>
        /// <param name="forceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
        /// <remarks></remarks>
        public void WriteStatusFile(bool forceLogToBrokerDB)
        {
            // Writes a status file for external monitor to read

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
        /// Updates the status in various locations, including on disk and with the message broker and/or broker DB
        /// </summary>
        /// <param name="lastUpdate"></param>
        /// <param name="processId"></param>
        /// <param name="cpuUtilization"></param>
        /// <param name="freeMemoryMB"></param>
        /// <param name="forceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
        /// <remarks></remarks>
        public void WriteStatusFile(DateTime lastUpdate, int processId, int cpuUtilization, float freeMemoryMB, bool forceLogToBrokerDB)
        {

            var strXMLText = string.Empty;

            var runTimeHours = GetRunTime();

            // Set up the XML writer
            try
            {
                // Create a new memory stream in which to write the XML
                var objMemoryStream = new MemoryStream();
                using (var xWriter = new XmlTextWriter(objMemoryStream, System.Text.Encoding.UTF8))
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
                    xWriter.WriteElementString("MgrName", MgrName);
                    xWriter.WriteElementString("MgrStatus", ConvertMgrStatusToString(MgrStatus));
                    xWriter.WriteElementString("LastUpdate", lastUpdate.ToLocalTime().ToString(CultureInfo.InvariantCulture));
                    xWriter.WriteElementString("LastStartTime", TaskStartTime.ToLocalTime().ToString(CultureInfo.InvariantCulture));
                    xWriter.WriteElementString("CPUUtilization", cpuUtilization.ToString("##0.0"));
                    xWriter.WriteElementString("FreeMemoryMB", freeMemoryMB.ToString("##0.0"));
                    xWriter.WriteElementString("ProcessID", processId.ToString());
                    xWriter.WriteElementString("ProgRunnerProcessID", ProgRunnerProcessID.ToString());
                    xWriter.WriteElementString("ProgRunnerCoreUsage", ProgRunnerCoreUsage.ToString("0.00"));
                    xWriter.WriteStartElement("RecentErrorMessages");
                    if (m_RecentErrorMessageCount == 0)
                    {
                        xWriter.WriteElementString("ErrMsg", string.Empty);
                    }
                    else
                    {
                        for (var intErrorMsgIndex = 0; intErrorMsgIndex <= m_RecentErrorMessageCount - 1; intErrorMsgIndex++)
                        {
                            xWriter.WriteElementString("ErrMsg", m_RecentErrorMessages[intErrorMsgIndex]);
                        }
                    }
                    xWriter.WriteEndElement();      // RecentErrorMessages
                    xWriter.WriteEndElement();      // Manager

                    xWriter.WriteStartElement("Task");
                    xWriter.WriteElementString("Tool", Tool);
                    xWriter.WriteElementString("Status", ConvertTaskStatusToString(TaskStatus));
                    xWriter.WriteElementString("Duration", runTimeHours.ToString("0.00"));
                    xWriter.WriteElementString("DurationMinutes", (runTimeHours * 60).ToString("0.0"));
                    xWriter.WriteElementString("Progress", Progress.ToString("##0.00"));
                    xWriter.WriteElementString("CurrentOperation", CurrentOperation);

                    xWriter.WriteStartElement("TaskDetails");
                    xWriter.WriteElementString("Status", ConvertTaskStatusDetailToString(TaskStatusDetail));
                    xWriter.WriteElementString("Job", Convert.ToString(JobNumber));
                    xWriter.WriteElementString("Step", Convert.ToString(JobStep));
                    xWriter.WriteElementString("Dataset", Dataset);
                    xWriter.WriteElementString("WorkDirPath", WorkDirPath);
                    xWriter.WriteElementString("MostRecentLogMessage", MostRecentLogMessage);
                    xWriter.WriteElementString("MostRecentJobInfo", MostRecentJobInfo);
                    xWriter.WriteElementString("SpectrumCount", SpectrumCount.ToString());
                    xWriter.WriteEndElement();      // TaskDetails
                    xWriter.WriteEndElement();      // Task

                    if (ProgRunnerProcessID != 0 && (m_ProgRunnerCoreUsageHistory != null))
                    {
                        xWriter.WriteStartElement("ProgRunnerCoreUsage");
                        xWriter.WriteAttributeString("Count", m_ProgRunnerCoreUsageHistory.Count.ToString());

                        // Dumping the items from the queue to a list because another thread might
                        // update m_ProgRunnerCoreUsageHistory while we're iterating over the items
                        var coreUsageHistory = m_ProgRunnerCoreUsageHistory.ToList();

                        foreach (var coreUsageSample in coreUsageHistory)
                        {
                            xWriter.WriteStartElement("CoreUsageSample");
                            xWriter.WriteAttributeString("Date", coreUsageSample.Key.ToString("yyyy-MM-dd hh:mm:ss tt"));
                            xWriter.WriteValue(coreUsageSample.Value.ToString("0.0"));
                            xWriter.WriteEndElement();  // CoreUsageSample
                        }
                        xWriter.WriteEndElement();      // ProgRunnerCoreUsage
                    }

                    xWriter.WriteEndElement();          // Root

                    // Close out the XML document (but do not close XWriter yet)
                    xWriter.WriteEndDocument();
                    xWriter.Flush();

                    // Now use a StreamReader to copy the XML text to a string variable
                    objMemoryStream.Seek(0, SeekOrigin.Begin);
                    var srMemoryStreamReader = new StreamReader(objMemoryStream);
                    strXMLText = srMemoryStreamReader.ReadToEnd();

                    srMemoryStreamReader.Close();
                    objMemoryStream.Close();

                    // Since strXMLText now contains the XML, we can now safely close XWriter
                }

                WriteStatusFileToDisk(strXMLText);

            }
            catch (Exception ex)
            {
                var msg = "Error generating status info: " + ex.Message;
                OnWarningEvent(msg);
            }

            CheckForAbortProcessingFile();

            if (LogToMsgQueue)
            {
                // Send the XML text to a message queue
                LogStatusToMessageQueue(strXMLText);
            }

            // Log the memory usage to a local file
            m_MemoryUsageLogger?.WriteMemoryUsageLogEntry();

            if (m_BrokerDBLogger != null)
            {
                // Send the status info to the Broker DB
                // Note that m_BrokerDBLogger() only logs the status every x minutes (unless forceLogToBrokerDB = True)

                LogStatusToBrokerDatabase(forceLogToBrokerDB);
            }
        }

        private void WriteStatusFileToDisk(string strXMLText)
        {
            const int MIN_FILE_WRITE_INTERVAL_SECONDS = 2;

            if (!(DateTime.UtcNow.Subtract(m_LastFileWriteTime).TotalSeconds >= MIN_FILE_WRITE_INTERVAL_SECONDS))
                return;

            // We will write out the Status XML to a temporary file, then rename the temp file to the primary file

            if (FileNamePath == null)
                return;

            var strTempStatusFilePath = Path.Combine(GetStatusFileDirectory(), Path.GetFileNameWithoutExtension(FileNamePath) + "_Temp.xml");

            m_LastFileWriteTime = DateTime.UtcNow;

            var logWarning = true;
            if (Tool.ToLower().Contains("glyq") || Tool.ToLower().Contains("modplus"))
            {
                if (m_DebugLevel < 3)
                    logWarning = false;
            }

            var blnSuccess = WriteStatusFileToDisk(strTempStatusFilePath, strXMLText, logWarning);
            if (blnSuccess)
            {
                try
                {
                    File.Copy(strTempStatusFilePath, FileNamePath, true);
                }
                catch (Exception ex)
                {
                    // Copy failed; this is normal when running GlyQ-IQ or MODPlus because they have multiple threads running
                    if (logWarning)
                    {
                        // Log a warning that the file copy failed
                        OnWarningEvent("Unable to copy temporary status file to the final status file (" + Path.GetFileName(strTempStatusFilePath) +
                                       " to " + Path.GetFileName(FileNamePath) + "):" + ex.Message);
                    }

                }

                try
                {
                    File.Delete(strTempStatusFilePath);
                }
                catch (Exception ex)
                {
                    // Delete failed; this is normal when running GlyQ-IQ or MODPlus because they have multiple threads running
                    if (logWarning)
                    {
                        // Log a warning that the file delete failed
                        OnWarningEvent("Unable to delete temporary status file (" + Path.GetFileName(strTempStatusFilePath) + "): " + ex.Message);
                    }
                }

            }
            else
            {
                // Error writing to the temporary status file; try the primary file
                WriteStatusFileToDisk(FileNamePath, strXMLText, logWarning);
            }
        }

        private bool WriteStatusFileToDisk(string strFilePath, string strXMLText, bool logWarning)
        {
            const int WRITE_FAILURE_LOG_THRESHOLD = 5;

            bool blnSuccess;

            try
            {
                // Write out the XML text to a file
                // If the file is in use by another process, then the writing will fail
                using (var srOutFile = new StreamWriter(new FileStream(strFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    srOutFile.WriteLine(strXMLText);
                }

                // Reset the error counter
                m_WritingErrorCountSaved = 0;

                blnSuccess = true;

            }
            catch (Exception ex)
            {
                // Increment the error counter
                m_WritingErrorCountSaved += 1;

                if (m_WritingErrorCountSaved >= WRITE_FAILURE_LOG_THRESHOLD && logWarning)
                {
                    // 5 or more errors in a row have occurred
                    // Post an entry to the log, only when intWritingErrorCountSaved is 5, 10, 20, 30, etc.
                    if (m_WritingErrorCountSaved == WRITE_FAILURE_LOG_THRESHOLD || m_WritingErrorCountSaved % 10 == 0)
                    {
                        var msg = "Error writing status file " + Path.GetFileName(strFilePath) + ": " + ex.Message;
                        OnWarningEvent(msg);
                    }
                }
                blnSuccess = false;
            }

            return blnSuccess;

        }

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="managerIdleMessage"></param>
        /// <param name="recentErrorMessages"></param>
        /// <param name="JobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
        /// <remarks></remarks>
        public void UpdateClose(string managerIdleMessage, IEnumerable<string> recentErrorMessages, string JobInfo, bool forceLogToBrokerDB)
        {
            ClearCachedInfo();

            MgrStatus = EnumMgrStatus.STOPPED;
            TaskStatus = EnumTaskStatus.NO_TASK;
            TaskStatusDetail = EnumTaskStatusDetail.NO_TASK;
            MostRecentLogMessage = managerIdleMessage;

            StoreRecentErrorMessages(recentErrorMessages);
            StoreRecentJobInfo(JobInfo);

            WriteStatusFile(forceLogToBrokerDB);

        }

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <remarks></remarks>
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
        public void UpdateAndWrite(EnumMgrStatus eMgrStatus, EnumTaskStatus eTaskStatus, EnumTaskStatusDetail eTaskStatusDetail, float percentComplete)
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
        /// <param name="SpectrumCountTotal">Number of DTA files (i.e., spectra files); relevant for Sequest, X!Tandem, and Inspect</param>
        /// <remarks></remarks>
        public void UpdateAndWrite(EnumTaskStatus status, float percentComplete, int SpectrumCountTotal)
        {
            TaskStatus = status;
            Progress = percentComplete;
            SpectrumCount = SpectrumCountTotal;

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
        /// <param name="forceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
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
        /// Sets status file to show mahager idle
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
        /// <param name="forceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
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
        /// <param name="forceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
        /// <remarks></remarks>
        public void UpdateIdle(string managerIdleMessage, string idleErrorMessage, string recentJobInfo, bool forceLogToBrokerDB)
        {
            ClearCachedInfo();
            TaskStatus = EnumTaskStatus.NO_TASK;
            MostRecentLogMessage = managerIdleMessage;

            StoreNewErrorMessage(idleErrorMessage, true);
            StoreRecentJobInfo(recentJobInfo);

            WriteStatusFile(forceLogToBrokerDB);

        }

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="managerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
        /// <remarks></remarks>
        public void UpdateIdle(string managerIdleMessage, IEnumerable<string> recentErrorMessages, string recentJobInfo, bool forceLogToBrokerDB)
        {
            ClearCachedInfo();
            MgrStatus = EnumMgrStatus.RUNNING;
            TaskStatus = EnumTaskStatus.NO_TASK;
            TaskStatusDetail = EnumTaskStatusDetail.NO_TASK;
            MostRecentLogMessage = managerIdleMessage;

            StoreRecentErrorMessages(recentErrorMessages);
            StoreRecentJobInfo(recentJobInfo);

            WriteStatusFile(forceLogToBrokerDB);
        }

        /// <summary>
        /// Updates status file to show manager disabled
        /// </summary>
        /// <remarks></remarks>
        public void UpdateDisabled(EnumMgrStatus managerStatus)
        {
            UpdateDisabled(managerStatus, "Manager Disabled");
        }

        /// <summary>
        /// Logs to the status file that the manager is disabled (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        /// <param name="managerStatus"></param>
        /// <param name="managerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
        /// <remarks></remarks>
        public void UpdateDisabled(EnumMgrStatus managerStatus, string managerDisableMessage)
        {
            UpdateDisabled(managerStatus, managerDisableMessage, new List<string>(), MostRecentJobInfo);
        }

        /// <summary>
        /// Logs to the status file that the manager is disabled (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        /// <param name="managerStatus"></param>
        /// <param name="managerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <remarks></remarks>
        public void UpdateDisabled(EnumMgrStatus managerStatus, string managerDisableMessage, IEnumerable<string> recentErrorMessages, string recentJobInfo)
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
        /// Total time the job has been running
        /// </summary>
        /// <returns>Number of hours manager has been processing job</returns>
        /// <remarks></remarks>
        private float GetRunTime()
        {

            return (float)DateTime.UtcNow.Subtract(TaskStartTime).TotalHours;

        }

        public void DisposeMessageQueue()
        {
            if (m_MessageSender != null)
            {
                m_QueueLogger.Dispose();
                m_MessageSender.Dispose();
            }

        }

        #endregion

        #region "Event handlers"
        private void MessageSender_ErrorEvent(string strMessage, Exception ex)
        {
            OnErrorEvent(strMessage, ex);
        }

        #endregion
    }
}
