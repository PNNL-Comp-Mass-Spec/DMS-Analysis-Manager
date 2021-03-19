using System;
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
        #region "Structures"

        /// <summary>
        /// Status info
        /// </summary>
        public struct udtStatusInfoType
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
            public udtTaskInfoType Task;
        }

        /// <summary>
        /// Task info
        /// </summary>
        public struct udtTaskInfoType
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
            public udtTaskDetailsType TaskDetails;
        }

        /// <summary>
        /// Task details
        /// </summary>
        public struct udtTaskDetailsType
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

        #endregion

        #region "Module variables"

        /// <summary>
        /// Stored procedure that could be used to report manager status; typically not used
        /// </summary>
        /// <remarks>This stored procedure is valid, but the primary way that we track status is when WriteStatusFile calls LogStatusToMessageQueue</remarks>
        private const string SP_NAME_UPDATE_MANAGER_STATUS = "UpdateManagerAndTaskStatus";

        /// <summary>
        /// The minimum interval between updating the manager status in the database
        /// </summary>
        private float mDBStatusUpdateIntervalMinutes;

        private DateTime mLastWriteTime;

        #endregion

        #region "Properties"

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

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dbConnectionString">Database connection string</param>
        /// <param name="dbStatusUpdateIntervalMinutes">Minimum interval between updating the manager status in the database</param>
        public DBStatusLogger(string dbConnectionString, float dbStatusUpdateIntervalMinutes)
        {
            if (dbConnectionString == null)
                dbConnectionString = string.Empty;

            PipelineDBProcedureExecutor = DbToolsFactory.GetDBTools(dbConnectionString, debugMode: false);
            RegisterEvents(PipelineDBProcedureExecutor);

            DBConnectionString = dbConnectionString;
            mDBStatusUpdateIntervalMinutes = dbStatusUpdateIntervalMinutes;
            mLastWriteTime = DateTime.MinValue;
        }

        /// <summary>
        /// Send status information to the database
        /// </summary>
        /// <param name="statusInfo"></param>
        /// <param name="forceLogToDB"></param>
        /// <remarks>This function is valid, but the primary way that we track status is when WriteStatusFile calls LogStatusToMessageQueue</remarks>
        public void LogStatus(udtStatusInfoType statusInfo, bool forceLogToDB)
        {
            try
            {
                if (string.IsNullOrEmpty(DBConnectionString))
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
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@MgrName", SqlType.VarChar, 128, statusInfo.MgrName);

                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@MgrStatusCode", SqlType.Int, 0, (int)statusInfo.MgrStatus);

                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@LastUpdate", SqlType.DateTime, 0, statusInfo.LastUpdate.ToLocalTime());
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@LastStartTime", SqlType.DateTime, 0, statusInfo.LastStartTime.ToLocalTime());
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@CPUUtilization", SqlType.Float, 0, statusInfo.CPUUtilization);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@FreeMemoryMB", SqlType.Float, 0, statusInfo.FreeMemoryMB);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@ProcessID", SqlType.Int, 0, statusInfo.ProcessID);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@ProgRunnerProcessID", SqlType.Int, 0, statusInfo.ProgRunnerProcessID);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@ProgRunnerCoreUsage", SqlType.Float, 0, statusInfo.ProgRunnerCoreUsage);

                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@MostRecentErrorMessage", SqlType.VarChar, 1024, statusInfo.MostRecentErrorMessage);

                // Task items
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@StepTool", SqlType.VarChar, 128, statusInfo.Task.Tool);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@TaskStatusCode", SqlType.Int, 0, (int)statusInfo.Task.Status);

                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@DurationHours", SqlType.Float, 0, statusInfo.Task.DurationHours);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@Progress", SqlType.Float, 0, statusInfo.Task.Progress);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@CurrentOperation", SqlType.VarChar, 256, statusInfo.Task.CurrentOperation);

                // Task detail items
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@TaskDetailStatusCode", SqlType.Int, 0, (int)statusInfo.Task.TaskDetails.Status);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@Job", SqlType.Int, 0, statusInfo.Task.TaskDetails.Job);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@JobStep", SqlType.Int, 0, statusInfo.Task.TaskDetails.JobStep);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@Dataset", SqlType.VarChar, 256, statusInfo.Task.TaskDetails.Dataset);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@MostRecentLogMessage", SqlType.VarChar, 1024, statusInfo.Task.TaskDetails.MostRecentLogMessage);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@MostRecentJobInfo", SqlType.VarChar, 256, statusInfo.Task.TaskDetails.MostRecentJobInfo);
                PipelineDBProcedureExecutor.AddTypedParameter(cmd, "@SpectrumCount", SqlType.Int, 0, statusInfo.Task.TaskDetails.SpectrumCount);

                PipelineDBProcedureExecutor.AddParameter(cmd, "@message", SqlType.VarChar, 512, string.Empty, ParameterDirection.Output);

                // Execute the SP
                PipelineDBProcedureExecutor.ExecuteSP(cmd);
            }
            catch (Exception ex)
            {
                // Ignore errors here
                Console.WriteLine("Error in DBStatusLogger.LogStatus: " + ex.Message);
            }
        }

        #endregion

    }
}
