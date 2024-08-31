using System;
using System.Collections.Generic;
using System.Data;
using PRISM;
using PRISMDatabaseUtils;

//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 02/09/2009
//
//*********************************************************************************************************

namespace AnalysisManagerBase.StatusReporting
{
    /// <summary>
    /// Status logger
    /// </summary>
    public class DBStatusLogger : EventNotifier
    {
        /// <summary>
        /// Status info
        /// </summary>
        public struct StatusInfo
        {
            /// <summary>
            /// Manager name
            /// </summary>
            public string MgrName;

            /// <summary>
            /// Manager status
            /// </summary>
            public MgrStatusCodes MgrStatus;

            /// <summary>
            /// Last update time (UTC-based)
            /// </summary>
            public DateTime LastUpdate;

            /// <summary>
            /// Last start time (UTC-based)
            /// </summary>
            public DateTime LastStartTime;

            /// <summary>
            /// Overall CPU utilization of all threads
            /// </summary>
            public float CPUUtilization;

            /// <summary>
            /// System-wide free memory
            /// </summary>
            public float FreeMemoryMB;

            /// <summary>
            /// Return the ProcessID of the Analysis manager
            /// </summary>
            public int ProcessID;

            /// <summary>
            /// ProcessID of an externally spawned process
            /// </summary>
            /// <remarks>0 if no external process running</remarks>
            public int ProgRunnerProcessID;

            /// <summary>
            /// Number of cores in use by an externally spawned process
            /// </summary>
            public float ProgRunnerCoreUsage;

            /// <summary>
            /// Most recent error message
            /// </summary>
            public string MostRecentErrorMessage;

            /// <summary>
            /// Current task
            /// </summary>
            public TaskInfo Task;
        }

        /// <summary>
        /// Task info
        /// </summary>
        public struct TaskInfo
        {
            /// <summary>
            /// Analysis tool name
            /// </summary>
            public string Tool;

            /// <summary>
            /// Task status
            /// </summary>
            public TaskStatusCodes Status;

            /// <summary>
            /// Task duration, in hours
            /// </summary>
            public float DurationHours;

            /// <summary>
            /// Percent complete, value between 0 and 100
            /// </summary>
            public float Progress;

            /// <summary>
            /// Current operation
            /// </summary>
            public string CurrentOperation;

            /// <summary>
            /// Task details
            /// </summary>
            public TaskDetails Details;
        }

        /// <summary>
        /// Task details
        /// </summary>
        public struct TaskDetails
        {
            /// <summary>
            /// Task status detail
            /// </summary>
            public TaskStatusDetailCodes Status;

            /// <summary>
            /// Job number
            /// </summary>
            public int Job;

            /// <summary>
            /// Job step
            /// </summary>
            public int JobStep;

            /// <summary>
            /// Dataset name
            /// </summary>
            public string Dataset;

            /// <summary>
            /// Most recent log message
            /// </summary>
            public string MostRecentLogMessage;

            /// <summary>
            /// Most recent job info
            /// </summary>
            public string MostRecentJobInfo;

            /// <summary>
            /// Number of spectra in the instrument data file
            /// </summary>
            /// <remarks>Only used by certain tools (e.g. SEQUEST, X!Tandem, and Inspect)</remarks>
            public int SpectrumCount;
        }

        /// <summary>
        /// Stored procedure that could be used to report manager status; typically not used
        /// </summary>
        /// <remarks>This stored procedure is valid, but the primary way that we track status is when WriteStatusFile calls LogStatusToMessageQueue</remarks>
        private const string SP_NAME_UPDATE_MANAGER_STATUS = "update_manager_and_task_status";

        /// <summary>
        /// The minimum interval between updating the manager status in the database
        /// </summary>
        private float mDBStatusUpdateIntervalMinutes;

        private DateTime mLastWriteTime;

        private readonly Dictionary<MgrStatusCodes, string> mMgrStatusMap;

        private readonly Dictionary<TaskStatusCodes, string> mTaskStatusMap;

        private readonly Dictionary<TaskStatusDetailCodes, string> mTaskStatusDetailMap;

        /// <summary>
        /// Database connection string
        /// </summary>
        public string DBConnectionString { get; }

        /// <summary>
        /// Status update interval, in minutes
        /// </summary>
        public float DBStatusUpdateIntervalMinutes
        {
            get => mDBStatusUpdateIntervalMinutes;
            set
            {
                if (value < 0)
                    value = 0;
                mDBStatusUpdateIntervalMinutes = value;
            }
        }

        /// <summary>
        /// Pipeline database stored procedure executor
        /// </summary>
        private IDBTools PipelineDBProcedureExecutor { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dbConnectionString">Database connection string</param>
        /// <param name="dbStatusUpdateIntervalMinutes">Minimum interval between updating the manager status in the database</param>
        public DBStatusLogger(string dbConnectionString, float dbStatusUpdateIntervalMinutes)
        {
            dbConnectionString ??= string.Empty;

            PipelineDBProcedureExecutor = DbToolsFactory.GetDBTools(dbConnectionString, debugMode: false);
            RegisterEvents(PipelineDBProcedureExecutor);

            DBConnectionString = dbConnectionString;
            mDBStatusUpdateIntervalMinutes = dbStatusUpdateIntervalMinutes;
            mLastWriteTime = DateTime.MinValue;

            mMgrStatusMap = new Dictionary<MgrStatusCodes, string>();
            mTaskStatusMap = new Dictionary<TaskStatusCodes, string>();
            mTaskStatusDetailMap = new Dictionary<TaskStatusDetailCodes, string>();

            StatusFile.DefineEnumToStringMapping(mMgrStatusMap, mTaskStatusMap, mTaskStatusDetailMap);
        }

        /// <summary>
        /// Send status information to the database
        /// </summary>
        /// <remarks>
        /// This method is only used if manager parameter LogStatusToBrokerDB is true
        /// Typically LogStatusToBrokerDB is false, and the manager instead reports status via LogStatusToMessageQueue
        /// </remarks>
        /// <param name="statusInfo"></param>
        /// <param name="forceLogToDB"></param>
        public void LogStatus(StatusInfo statusInfo, bool forceLogToDB)
        {
            try
            {
                if (string.IsNullOrEmpty(PipelineDBProcedureExecutor.ConnectStr))
                {
                    // Connection string not defined; unable to continue
                    return;
                }

                if (!forceLogToDB && DateTime.UtcNow.Subtract(mLastWriteTime).TotalMinutes < mDBStatusUpdateIntervalMinutes)
                {
                    // Not enough time has elapsed since the last write; exit method
                    return;
                }
                mLastWriteTime = DateTime.UtcNow;

                // Set up the command object prior to SP execution
                var cmd = PipelineDBProcedureExecutor.CreateCommand(SP_NAME_UPDATE_MANAGER_STATUS, CommandType.StoredProcedure);

                // Manager items
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@mgrName", SqlType.VarChar, 128, statusInfo.MgrName);

                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@mgrStatus", SqlType.VarChar, 50, StatusFile.ConvertMgrStatusToString(mMgrStatusMap, statusInfo.MgrStatus));

                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@lastUpdate", SqlType.DateTime, 0, statusInfo.LastUpdate.ToLocalTime());
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@lastStartTime", SqlType.DateTime, 0, statusInfo.LastStartTime.ToLocalTime());
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@cPUUtilization", SqlType.Float, 0, statusInfo.CPUUtilization);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@freeMemoryMB", SqlType.Float, 0, statusInfo.FreeMemoryMB);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@processID", SqlType.Int, 0, statusInfo.ProcessID);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@progRunnerProcessID", SqlType.Int, 0, statusInfo.ProgRunnerProcessID);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@progRunnerCoreUsage", SqlType.Float, 0, statusInfo.ProgRunnerCoreUsage);

                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@mostRecentErrorMessage", SqlType.VarChar, 1024, statusInfo.MostRecentErrorMessage);

                // Task items
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@stepTool", SqlType.VarChar, 128, statusInfo.Task.Tool);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@taskStatus", SqlType.VarChar, 50, StatusFile.ConvertTaskStatusToString(mTaskStatusMap, statusInfo.Task.Status));

                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@durationHours", SqlType.Float, 0, statusInfo.Task.DurationHours);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@progress", SqlType.Float, 0, statusInfo.Task.Progress);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@currentOperation", SqlType.VarChar, 256, statusInfo.Task.CurrentOperation);

                // Task detail items
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@taskDetailStatus", SqlType.VarChar, 50, StatusFile.ConvertTaskStatusDetailToString(mTaskStatusDetailMap, statusInfo.Task.Details.Status));
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@job", SqlType.Int, 0, statusInfo.Task.Details.Job);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@jobStep", SqlType.Int, 0, statusInfo.Task.Details.JobStep);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@dataset", SqlType.VarChar, 256, statusInfo.Task.Details.Dataset);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@mostRecentLogMessage", SqlType.VarChar, 1024, statusInfo.Task.Details.MostRecentLogMessage);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@mostRecentJobInfo", SqlType.VarChar, 256, statusInfo.Task.Details.MostRecentJobInfo);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@spectrumCount", SqlType.Int, 0, statusInfo.Task.Details.SpectrumCount);

                PipelineDBProcedureExecutor.AddParameter(cmd, "@message", SqlType.VarChar, 512, string.Empty, ParameterDirection.InputOutput);

                // Call the procedure
                PipelineDBProcedureExecutor.ExecuteSP(cmd);
            }
            catch (Exception ex)
            {
                // Ignore errors here
                Console.WriteLine("Error in DBStatusLogger.LogStatus: " + ex.Message);
            }
        }
    }
}
