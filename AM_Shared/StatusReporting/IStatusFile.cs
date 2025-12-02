using System;
using System.Collections;
using System.Collections.Generic;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

// ReSharper disable UnusedMemberInSuper.Global

namespace AnalysisManagerBase.StatusReporting
{
    /// <summary>
    /// Manager Status constants
    /// </summary>
    public enum MgrStatusCodes : short
    {
        /// <summary>
        /// Stopped
        /// </summary>
        STOPPED,

        /// <summary>
        /// Stopped
        /// </summary>
        STOPPED_ERROR,

        /// <summary>
        /// Running
        /// </summary>
        RUNNING,

        /// <summary>
        /// Disabled
        /// </summary>
        DISABLED_LOCAL,

        /// <summary>
        /// Disabled
        /// </summary>
        DISABLED_MC
    }

    /// <summary>
    /// Task status constants
    /// </summary>
    public enum TaskStatusCodes : short
    {
        /// <summary>
        /// Stopped
        /// </summary>
        STOPPED,

        /// <summary>
        /// Requesting
        /// </summary>
        REQUESTING,

        /// <summary>
        /// Running
        /// </summary>
        RUNNING,

        /// <summary>
        /// Closing
        /// </summary>
        CLOSING,

        /// <summary>
        /// Failed
        /// </summary>
        FAILED,

        /// <summary>
        /// No
        /// </summary>
        NO_TASK
    }

    /// <summary>
    /// Task status detail constants
    /// </summary>
    public enum TaskStatusDetailCodes : short
    {
        /// <summary>
        /// Retrieving resources
        /// </summary>
        RETRIEVING_RESOURCES,

        /// <summary>
        /// Running tool
        /// </summary>
        RUNNING_TOOL,

        /// <summary>
        /// Packaging results
        /// </summary>
        PACKAGING_RESULTS,

        /// <summary>
        /// Delivering results
        /// </summary>
        DELIVERING_RESULTS,

        /// <summary>
        /// Closing
        /// </summary>
        CLOSING,

        /// <summary>
        /// No task
        /// </summary>
        NO_TASK
    }

    /// <summary>
    /// Interface used by classes that create and update analysis status file
    /// </summary>
    public interface IStatusFile
    {
        // Ignore Spelling: hyperthreading, tcp

        /// <summary>
        /// Broker database connection string
        /// </summary>
        string BrokerDBConnectionString { get; }

        /// <summary>
        /// Broker database update interval, in minutes
        /// </summary>
        float BrokerDBUpdateIntervalMinutes { get; }

        /// <summary>
        /// Overall CPU utilization of all threads
        /// </summary>
        int CpuUtilization { get; set; }

        /// <summary>
        /// Current task
        /// </summary>
        string CurrentOperation { get; set; }

        /// <summary>
        /// Dataset name
        /// </summary>
        string Dataset { get; set; }

        /// <summary>
        /// Status file path
        /// </summary>
        string FileNamePath { get; set; }

        /// <summary>
        /// Job number
        /// </summary>
        int JobNumber { get; set; }

        /// <summary>
        /// Step number
        /// </summary>
        int JobStep { get; set; }

        /// <summary>
        /// When true, status messages are being sent directly to the broker database
        /// </summary>
        bool LogToBrokerQueue { get; }

        /// <summary>
        /// When true, the status XML is being sent to the manager status message queue
        /// </summary>
        bool LogToMsgQueue { get; }

        /// <summary>
        /// Keeps track of the 25 most recent free memory MB values
        /// </summary>
        public Queue<float> MemoryUsageQueue { get; }

        /// <summary>
        /// Topic name for the manager status message queue
        /// </summary>
        string MessageQueueTopic { get; }

        /// <summary>
        /// URI for the manager status message queue, e.g. tcp://Proto-7.pnl.gov:61616
        /// </summary>
        string MessageQueueURI { get; }

        /// <summary>
        /// Manager name
        /// </summary>
        string MgrName { get; set; }

        /// <summary>
        /// Manager status
        /// </summary>
        MgrStatusCodes MgrStatus { get; set; }

        /// <summary>
        /// Most recent job info
        /// </summary>
        string MostRecentJobInfo { get; set; }

        /// <summary>
        /// Progress (value between 0 and 100)
        /// </summary>
        float Progress { get; set; }

        /// <summary>
        /// Number of cores in use by an externally spawned process
        /// </summary>
        float ProgRunnerCoreUsage { get; set; }

        /// <summary>
        /// ProcessID of an externally spawned process
        /// </summary>
        /// <remarks>0 if no external process running</remarks>
        int ProgRunnerProcessID { get; set; }

        /// <summary>
        /// Name of the manager remotely running the job
        /// </summary>
        /// <remarks>When this is defined, it is implied that stats like CpuUtilization  and CoreUsage apply to the remote manager</remarks>
        string RemoteMgrName { get; set; }

        /// <summary>
        /// Number of spectrum files created or number of scans being searched
        /// </summary>
        int SpectrumCount { get; set; }

        /// <summary>
        /// Task start time (UTC-based)
        /// </summary>
        DateTime TaskStartTime { get; set; }

        /// <summary>
        /// Task status
        /// </summary>
        TaskStatusCodes TaskStatus { get; set; }

        /// <summary>
        /// Task status detail
        /// </summary>
        TaskStatusDetailCodes TaskStatusDetail { get; set; }

        /// <summary>
        /// Step tool name
        /// </summary>
        string Tool { get; set; }

        /// <summary>
        /// Compute the average value of recent free memory MB
        /// </summary>
        /// <param name="countToAverage">Number of values to average</param>
        /// <returns>Free memory, in MB</returns>
        float GetAverageRecentFreeMemoryMB(int countToAverage);

        /// <summary>
        /// Returns the number of cores
        /// </summary>
        /// <remarks>Should not be affected by hyperthreading, so a computer with two 4-core chips will report 8 cores</remarks>
        int GetCoreCount();

        /// <summary>
        /// Returns the amount of free memory
        /// </summary>
        /// <returns>Amount of free memory, in MB</returns>
        float GetFreeMemoryMB();

        /// <summary>
        /// Return the ProcessID of the running process
        /// </summary>
        int GetProcessID();

        /// <summary>
        /// Store core usage history
        /// </summary>
        /// <param name="coreUsageHistory">Core usage history queue</param>
        void StoreCoreUsageHistory(Queue<KeyValuePair<DateTime, float>> coreUsageHistory);

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        void UpdateAndWrite(float percentComplete);

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="mgrStatus">Job status code</param>
        /// <param name="taskStatus">Task status code</param>
        /// <param name="taskStatusDetail">Detailed task status</param>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        void UpdateAndWrite(MgrStatusCodes mgrStatus, TaskStatusCodes taskStatus, TaskStatusDetailCodes taskStatusDetail, float percentComplete);

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="status">Job status enum</param>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <param name="spectrumCountTotal">Number of DTA files (i.e., spectra files); relevant for SEQUEST, X!Tandem, and Inspect</param>
        void UpdateAndWrite(TaskStatusCodes status, float percentComplete, int spectrumCountTotal);

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
        void UpdateAndWrite(
            MgrStatusCodes mgrStatus,
            TaskStatusCodes taskStatus,
            TaskStatusDetailCodes taskStatusDetail,
            float percentComplete,
            int dtaCount,
            string mostRecentLogMessage,
            string mostRecentErrorMessage,
            string recentJobInfo,
            bool forceLogToBrokerDB);

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="managerIdleMessage">Manager idle message</param>
        /// <param name="recentErrorMessages">Recent error messages</param>
        /// <param name="jobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, will force mBrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        void UpdateClose(string managerIdleMessage, IEnumerable<string> recentErrorMessages, string jobInfo, bool forceLogToBrokerDB);

        /// <summary>
        /// Logs to the status file that the manager is disabled
        /// (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        void UpdateDisabled(MgrStatusCodes managerStatus);

        /// <summary>
        /// Logs to the status file that the manager is disabled
        /// (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        /// <param name="managerStatus">Manager status instance</param>
        /// <param name="managerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
        void UpdateDisabled(MgrStatusCodes managerStatus, string managerDisableMessage);

        /// <summary>
        /// Logs to the status file that the manager is disabled
        /// (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        /// <param name="managerStatus">Manager status instance</param>
        /// <param name="managerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        void UpdateDisabled(MgrStatusCodes managerStatus, string managerDisableMessage, IEnumerable<string> recentErrorMessages, string recentJobInfo);

        /// <summary>
        /// Logs to the status file that a flag file exists, indicating that the manager did not exit cleanly on a previous run
        /// </summary>
        void UpdateFlagFileExists();

        /// <summary>
        /// Logs to the status file that a flag file exists, indicating that the manager did not exit cleanly on a previous run
        /// </summary>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        void UpdateFlagFileExists(IEnumerable<string> recentErrorMessages, string recentJobInfo);

        /// <summary>
        /// Sets status file to show manager idle
        /// </summary>
        void UpdateIdle();

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="managerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="forceLogToBrokerDB">If true, will force mBrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        void UpdateIdle(string managerIdleMessage, bool forceLogToBrokerDB);

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="managerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="idleErrorMessage">Error message explaining why the manager is idle</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, will force mBrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        void UpdateIdle(string managerIdleMessage, string idleErrorMessage, string recentJobInfo, bool forceLogToBrokerDB);

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="managerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, will force mBrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        void UpdateIdle(string managerIdleMessage, IEnumerable<string> recentErrorMessages, string recentJobInfo, bool forceLogToBrokerDB);

        /// <summary>
        /// Writes out a new status file, indicating that the manager is still alive
        /// </summary>
        void WriteStatusFile();

        /// <summary>
        /// Writes the status file
        /// </summary>
        /// <param name="forceLogToBrokerDB">If true, will force mBrokerDBLogger to report the manager status to the database</param>
        /// <param name ="includeCpuUsage">
        /// When true, include the total CPU utilization percent in the status file.
        /// This can lead to PerfLib warnings and errors in the Windows Event Log;
        /// thus this should be set to false if simply reporting that the manager is idle
        /// </param>
        void WriteStatusFile(bool forceLogToBrokerDB, bool includeCpuUsage);
    }
}