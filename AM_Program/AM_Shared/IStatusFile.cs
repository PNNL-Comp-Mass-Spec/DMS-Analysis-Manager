
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

        string FileNamePath { get; set; }

        string MgrName { get; set; }

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

        string Tool { get; set; }

        EnumTaskStatus TaskStatus { get; set; }

        float Progress { get; set; }

        string CurrentOperation { get; set; }

        EnumTaskStatusDetail TaskStatusDetail { get; set; }

        int JobNumber { get; set; }

        int JobStep { get; set; }

        string Dataset { get; set; }

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

        int SpectrumCount { get; set; }

        string MessageQueueURI { get; set; }

        string MessageQueueTopic { get; set; }

        bool LogToMsgQueue { get; set; }
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

        void StoreCoreUsageHistory(Queue<KeyValuePair<DateTime, float>> coreUsageHistory);

        /// <summary>
        ///
        /// </summary>
        /// <param name="ManagerIdleMessage"></param>
        /// <param name="recentErrorMessages"></param>
        /// <param name="JobInfo"></param>
        /// <param name="ForceLogToBrokerDB"></param>
        /// <remarks></remarks>
        void UpdateClose(string ManagerIdleMessage, IEnumerable<string> recentErrorMessages, string JobInfo, bool ForceLogToBrokerDB);

        /// <summary>
        /// Update the current status
        /// </summary>
        /// <param name="PercentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <remarks></remarks>
        void UpdateAndWrite(float PercentComplete);

        /// <summary>
        /// Update the current status
        /// </summary>
        /// <param name="eMgrStatus">Job status code</param>
        /// <param name="eTaskStatus">Task status code</param>
        /// <param name="eTaskStatusDetail">Detailed task status</param>
        /// <param name="PercentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <remarks></remarks>
        void UpdateAndWrite(EnumMgrStatus eMgrStatus, EnumTaskStatus eTaskStatus, EnumTaskStatusDetail eTaskStatusDetail, float PercentComplete);

        /// <summary>
        /// Update the current status
        /// </summary>
        /// <param name="Status">Job status code</param>
        /// <param name="PercentComplete">VJob completion percentage (value between 0 and 100)</param>
        /// <param name="SpectrumCountTotal">Number of DTA files (i.e., spectra files); relevant for Sequest, X!Tandem, and Inspect</param>
        /// <remarks></remarks>
        void UpdateAndWrite(EnumTaskStatus Status, float PercentComplete, int SpectrumCountTotal);

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="eMgrStatus">Job status code</param>
        /// <param name="eTaskStatus">Task status code</param>
        /// <param name="eTaskStatusDetail">Detailed task status</param>
        /// <param name="PercentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <param name="DTACount">Number of DTA files (i.e., spectra files); relevant for Sequest, X!Tandem, and Inspect</param>
        /// <param name="MostRecentLogMessage">Most recent message posted to the logger (leave blank if unknown)</param>
        /// <param name="MostRecentErrorMessage">Most recent error posted to the logger (leave blank if unknown)</param>
        /// <param name="RecentJobInfo">Information on the job that started most recently</param>
        /// <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
        /// <remarks></remarks>
        void UpdateAndWrite(
            EnumMgrStatus eMgrStatus,
            EnumTaskStatus eTaskStatus,
            EnumTaskStatusDetail eTaskStatusDetail,
            float PercentComplete,
            int DTACount,
            string MostRecentLogMessage,
            string MostRecentErrorMessage,
            string RecentJobInfo,
            bool ForceLogToBrokerDB);

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <remarks></remarks>
        void UpdateIdle();

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="ManagerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
        /// <remarks></remarks>
        void UpdateIdle(string ManagerIdleMessage, bool ForceLogToBrokerDB);

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="ManagerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="IdleErrorMessage">Error message explaining why the manager is idle</param>
        /// <param name="RecentJobInfo">Information on the job that started most recently</param>
        /// <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
        /// <remarks></remarks>
        void UpdateIdle(string ManagerIdleMessage, string IdleErrorMessage, string RecentJobInfo, bool ForceLogToBrokerDB);

        /// <summary>
        /// Logs to the status file that the manager is idle
        /// </summary>
        /// <param name="ManagerIdleMessage">Reason why the manager is idle (leave blank if unknown)</param>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="RecentJobInfo">Information on the job that started most recently</param>
        /// <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
        /// <remarks></remarks>
        void UpdateIdle(string ManagerIdleMessage, IEnumerable<string> recentErrorMessages, string RecentJobInfo, bool ForceLogToBrokerDB);

        /// <summary>
        /// Logs to the status file that the manager is disabled (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        /// <remarks></remarks>
        void UpdateDisabled(EnumMgrStatus ManagerStatus);

        /// <summary>
        /// Logs to the status file that the manager is disabled (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        /// <param name="ManagerStatus"></param>
        /// <param name="ManagerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
        /// <remarks></remarks>
        void UpdateDisabled(EnumMgrStatus ManagerStatus, string ManagerDisableMessage);

        /// <summary>
        /// Logs to the status file that the manager is disabled (either in the manager control DB or via the local AnalysisManagerProg.exe.config file)
        /// </summary>
        /// <param name="ManagerStatus">Should be EnumMgrStatus.DISABLED_LOCAL or EnumMgrStatus.DISABLED_MC</param>
        /// <param name="ManagerDisableMessage">Description of why the manager is disabled (leave blank if unknown)</param>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="RecentJobInfo">Information on the job that started most recently</param>
        /// <remarks></remarks>
        void UpdateDisabled(EnumMgrStatus ManagerStatus, string ManagerDisableMessage, IEnumerable<string> recentErrorMessages, string RecentJobInfo);

        /// <summary>
        /// Logs to the status file that a flag file exists, indicating that the manager did not exit cleanly on a previous run
        /// </summary>
        /// <remarks></remarks>
        void UpdateFlagFileExists();

        /// <summary>
        /// Logs to the status file that a flag file exists, indicating that the manager did not exit cleanly on a previous run
        /// </summary>
        /// <param name="recentErrorMessages">Recent error messages written to the log file (leave blank if unknown)</param>
        /// <param name="RecentJobInfo">Information on the job that started most recently</param>
        /// <remarks></remarks>
        void UpdateFlagFileExists(IEnumerable<string> recentErrorMessages, string RecentJobInfo);

        /// <summary>
        /// Writes out a new status file, indicating that the manager is still alive
        /// </summary>
        /// <remarks></remarks>
        void WriteStatusFile();

        /// <summary>
        /// Writes the status file
        /// </summary>
        /// <param name="ForceLogToBrokerDB">If true, then will force m_BrokerDBLogger to report the manager status to the database</param>
        /// <remarks></remarks>
        void WriteStatusFile(bool ForceLogToBrokerDB);

        #endregion

    }

}