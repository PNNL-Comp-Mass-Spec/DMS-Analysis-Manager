
using System;
using System.Collections.Generic;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{

    #region "Enums"

    /// <summary>
    /// Manager Status constants
    /// </summary>
    public enum EnumMgrStatus : short
    {
        STOPPED,
        STOPPED_ERROR,
        RUNNING,
        DISABLED_LOCAL,
        DISABLED_MC
    }

    /// <summary>
    /// Task status constants
    /// </summary>
    public enum EnumTaskStatus : short
    {
        STOPPED,
        REQUESTING,
        RUNNING,
        CLOSING,
        FAILED,
        NO_TASK
    }

    /// <summary>
    /// Task status detail constants
    /// </summary>
    public enum EnumTaskStatusDetail : short
    {
        RETRIEVING_RESOURCES,
        RUNNING_TOOL,
        PACKAGING_RESULTS,
        DELIVERING_RESULTS,
        CLOSING,
        NO_TASK
    }
    #endregion

    /// <summary>
    /// Interface used by classes that create and update analysis status file
    /// </summary>
    public interface IStatusFile
    {

        #region "Properties"

        /// <summary>
        /// When true, status messages are being sent directly to the broker database
        /// </summary>
        bool LogToBrokerQueue { get; }

        /// <summary>
        /// Broker database connection string
        /// </summary>
        string BrokerDBConnectionString { get; }

        /// <summary>
        /// Broker database update interval, in minutes
        /// </summary>
        float BrokerDBUpdateIntervalMinutes { get; }

        /// <summary>
        /// Status file path
        /// </summary>
        string FileNamePath { get; set; }

        /// <summary>
        /// Manager name
        /// </summary>
        string MgrName { get; set; }

        /// <summary>
        /// Manager status
        /// </summary>
        EnumMgrStatus MgrStatus { get; set; }

        /// <summary>
        /// Name of the manager remotely running the job
        /// </summary>
        /// <remarks>When this is defined, it is implied that stats like CpuUtilization  and CoreUsage apply to the remote manager</remarks>
        string RemoteMgrName { get; set; }

        /// <summary>
        /// Overall CPU utilization of all threads
        /// </summary>
        /// <remarks></remarks>
        int CpuUtilization { get; set; }

        /// <summary>
        /// Step tool name
        /// </summary>
        string Tool { get; set; }

        /// <summary>
        /// Task status
        /// </summary>
        EnumTaskStatus TaskStatus { get; set; }

        /// <summary>
        /// Task start time (UTC-based)
        /// </summary>
        DateTime TaskStartTime { get; set; }

        /// <summary>
        /// Progress (value between 0 and 100)
        /// </summary>
        float Progress { get; set; }

        /// <summary>
        /// Current task
        /// </summary>
        string CurrentOperation { get; set; }

        /// <summary>
        /// Task status detail
        /// </summary>
        EnumTaskStatusDetail TaskStatusDetail { get; set; }

        /// <summary>
        /// Job number
        /// </summary>
        int JobNumber { get; set; }

        /// <summary>
        /// Step number
        /// </summary>
        int JobStep { get; set; }

        /// <summary>
        /// Dataset name
        /// </summary>
        string Dataset { get; set; }

        /// <summary>
        /// Most recent job info
        /// </summary>
        string MostRecentJobInfo { get; set; }

        /// <summary>
        /// ProcessID of an externally spawned process
        /// </summary>
        /// <remarks>0 if no external process running</remarks>
        int ProgRunnerProcessID { get; set; }

        /// <summary>
        /// Number of cores in use by an externally spawned process
        /// </summary>
        /// <remarks></remarks>
        float ProgRunnerCoreUsage { get; set; }

        /// <summary>
        /// Number of spectrum files created or number of scans being searched
        /// </summary>
        int SpectrumCount { get; set; }

        /// <summary>
        /// URI for the manager status message queue, e.g. tcp://Proto-7.pnl.gov:61616
        /// </summary>
        string MessageQueueURI { get; }

        /// <summary>
        /// Topic name for the manager status message queue
        /// </summary>
        string MessageQueueTopic { get; }

        /// <summary>
        /// When true, the status XML is being sent to the manager status message queue
        /// </summary>
        bool LogToMsgQueue { get; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Returns the number of cores
        /// </summary>
        /// <returns></returns>
        /// <remarks>Not affected by hyperthreading, so a computer with two 4-core chips will report 8 cores</remarks>
        int GetCoreCount();

        /// <summary>
        /// Returns the amount of free memory
        /// </summary>
        /// <returns>Amount of free memory, in MB</returns>
        float GetFreeMemoryMB();

        /// <summary>
        /// Return the ProcessID of the running process
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        int GetProcessID();

        /// <summary>
        /// Store core usage history
        /// </summary>
        /// <param name="coreUsageHistory"></param>
        void StoreCoreUsageHistory(Queue<KeyValuePair<DateTime, float>> coreUsageHistory);

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="managerIdleMessage"></param>
        /// <param name="recentErrorMessages"></param>
        /// <param name="jobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, will force m_BrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        /// <remarks></remarks>
        void UpdateClose(string managerIdleMessage, IEnumerable<string> recentErrorMessages, string jobInfo, bool forceLogToBrokerDB);

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <remarks></remarks>
        void UpdateAndWrite(float percentComplete);

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="eMgrStatus">Job status code</param>
        /// <param name="eTaskStatus">Task status code</param>
        /// <param name="eTaskStatusDetail">Detailed task status</param>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <remarks></remarks>
        void UpdateAndWrite(EnumMgrStatus eMgrStatus, EnumTaskStatus eTaskStatus, EnumTaskStatusDetail eTaskStatusDetail, float percentComplete);

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="status">Job status enum</param>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <param name="spectrumCountTotal">Number of DTA files (i.e., spectra files); relevant for Sequest, X!Tandem, and Inspect</param>
        /// <remarks></remarks>
        void UpdateAndWrite(EnumTaskStatus status, float percentComplete, int spectrumCountTotal);

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
        void UpdateAndWrite(
            EnumMgrStatus eMgrStatus,
            EnumTaskStatus eTaskStatus,
            EnumTaskStatusDetail eTaskStatusDetail,
            float percentComplete,
            int dtaCount,
            string mostRecentLogMessage,
            string mostRecentErrorMessage,
            string recentJobInfo,
            bool forceLogToBrokerDB);

        /// <summary>
        /// Sets status file to show mahager idle
        /// </summary>
        /// <remarks></remarks>
        void UpdateIdle();

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="managerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="forceLogToBrokerDB">If true, will force m_BrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        /// <remarks></remarks>
        void UpdateIdle(string managerIdleMessage, bool forceLogToBrokerDB);

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="managerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="idleErrorMessage">Error message explaining why the manager is idle</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, will force m_BrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        /// <remarks></remarks>
        void UpdateIdle(string managerIdleMessage, string idleErrorMessage, string recentJobInfo, bool forceLogToBrokerDB);

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="managerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <param name="forceLogToBrokerDB">If true, will force m_BrokerDBLogger to report the manager status directly to the database (if initialized)</param>
        /// <remarks></remarks>
        void UpdateIdle(string managerIdleMessage, IEnumerable<string> recentErrorMessages, string recentJobInfo, bool forceLogToBrokerDB);

        /// <summary>
        /// Logs to the status file that the manager is disabled
        /// (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        /// <remarks></remarks>
        void UpdateDisabled(EnumMgrStatus managerStatus);

        /// <summary>
        /// Logs to the status file that the manager is disabled
        /// (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        /// <param name="managerStatus"></param>
        /// <param name="managerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
        /// <remarks></remarks>
        void UpdateDisabled(EnumMgrStatus managerStatus, string managerDisableMessage);

        /// <summary>
        /// Logs to the status file that the manager is disabled
        /// (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        /// <param name="managerStatus"></param>
        /// <param name="managerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <remarks></remarks>
        void UpdateDisabled(EnumMgrStatus managerStatus, string managerDisableMessage, IEnumerable<string> recentErrorMessages, string recentJobInfo);

        /// <summary>
        /// Logs to the status file that a flag file exists, indicating that the manager did not exit cleanly on a previous run
        /// </summary>
        /// <remarks></remarks>
        void UpdateFlagFileExists();

        /// <summary>
        /// Logs to the status file that a flag file exists, indicating that the manager did not exit cleanly on a previous run
        /// </summary>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="recentJobInfo">Information on the job that started most recently</param>
        /// <remarks></remarks>
        void UpdateFlagFileExists(IEnumerable<string> recentErrorMessages, string recentJobInfo);

        /// <summary>
        /// Writes out a new status file, indicating that the manager is still alive
        /// </summary>
        /// <remarks></remarks>
        void WriteStatusFile();

        /// <summary>
        /// Writes the status file
        /// </summary>
        /// <param name="forceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
        /// <remarks></remarks>
        void WriteStatusFile(bool forceLogToBrokerDB);

        #endregion

    }

}