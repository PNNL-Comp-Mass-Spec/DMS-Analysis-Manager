using System;
using System.Data;
using System.Data.SqlClient;

//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 02/09/2009
//
//*********************************************************************************************************


namespace AnalysisManagerBase
{

    /// <summary>
    /// Status logger
    /// </summary>
    public class clsDBStatusLogger
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
            public EnumMgrStatus MgrStatus;

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
            /// <remarks></remarks>
            public float CPUUtilization;

            /// <summary>
            /// System-wide free memory
            /// </summary>
            /// <remarks></remarks>
            public float FreeMemoryMB;

            /// <summary>
            /// Return the ProcessID of the Analysis manager
            /// </summary>
            /// <remarks></remarks>
            public int ProcessID;

            /// <summary>
            /// ProcessID of an externally spawned process
            /// </summary>
            /// <remarks>0 if no external process running</remarks>
            public int ProgRunnerProcessID;

            /// <summary>
            /// Number of cores in use by an externally spawned process
            /// </summary>
            /// <remarks></remarks>
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
            public EnumTaskStatus Status;

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
            public EnumTaskStatusDetail Status;

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
        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dbConnectionString">Database connection string</param>
        /// <param name="dbStatusUpdateIntervalMinutes">Minimum interval between updating the manager status in the database</param>
        /// <remarks></remarks>
        public clsDBStatusLogger(string dbConnectionString, float dbStatusUpdateIntervalMinutes)
        {
            if (dbConnectionString == null)
                dbConnectionString = string.Empty;

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

                var myConnection = new SqlConnection(DBConnectionString);
                myConnection.Open();

                // Set up the command object prior to SP execution
                var cmd = new SqlCommand
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandText = SP_NAME_UPDATE_MANAGER_STATUS,
                    Connection = myConnection
                };

                cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int));
                cmd.Parameters["@Return"].Direction = ParameterDirection.ReturnValue;


                // Manager items
                AddSPParameter(cmd.Parameters, "@MgrName", statusInfo.MgrName, 128);
                AddSPParameter(cmd.Parameters, "@MgrStatusCode", (int)statusInfo.MgrStatus);

                AddSPParameter(cmd.Parameters, "@LastUpdate", statusInfo.LastUpdate.ToLocalTime());
                AddSPParameter(cmd.Parameters, "@LastStartTime", statusInfo.LastStartTime.ToLocalTime());
                AddSPParameter(cmd.Parameters, "@CPUUtilization", statusInfo.CPUUtilization);
                AddSPParameter(cmd.Parameters, "@FreeMemoryMB", statusInfo.FreeMemoryMB);
                AddSPParameter(cmd.Parameters, "@ProcessID", statusInfo.ProcessID);
                AddSPParameter(cmd.Parameters, "@ProgRunnerProcessID", statusInfo.ProgRunnerProcessID);
                AddSPParameter(cmd.Parameters, "@ProgRunnerCoreUsage", statusInfo.ProgRunnerCoreUsage);

                AddSPParameter(cmd.Parameters, "@MostRecentErrorMessage", statusInfo.MostRecentErrorMessage, 1024);

                // Task items
                AddSPParameter(cmd.Parameters, "@StepTool", statusInfo.Task.Tool, 128);
                AddSPParameter(cmd.Parameters, "@TaskStatusCode", (int)statusInfo.Task.Status);
                AddSPParameter(cmd.Parameters, "@DurationHours", statusInfo.Task.DurationHours);
                AddSPParameter(cmd.Parameters, "@Progress", statusInfo.Task.Progress);
                AddSPParameter(cmd.Parameters, "@CurrentOperation", statusInfo.Task.CurrentOperation, 256);

                // Task detail items
                AddSPParameter(cmd.Parameters, "@TaskDetailStatusCode", (int)statusInfo.Task.TaskDetails.Status);
                AddSPParameter(cmd.Parameters, "@Job", statusInfo.Task.TaskDetails.Job);
                AddSPParameter(cmd.Parameters, "@JobStep", statusInfo.Task.TaskDetails.JobStep);
                AddSPParameter(cmd.Parameters, "@Dataset", statusInfo.Task.TaskDetails.Dataset, 256);
                AddSPParameter(cmd.Parameters, "@MostRecentLogMessage", statusInfo.Task.TaskDetails.MostRecentLogMessage, 1024);
                AddSPParameter(cmd.Parameters, "@MostRecentJobInfo", statusInfo.Task.TaskDetails.MostRecentJobInfo, 256);
                AddSPParameter(cmd.Parameters, "@SpectrumCount", statusInfo.Task.TaskDetails.SpectrumCount);

                AddSPParameterOutput(cmd.Parameters, "@message", string.Empty, 512);

                // Execute the SP
                cmd.ExecuteNonQuery();

            }
            catch (Exception ex)
            {
                // Ignore errors here
                Console.WriteLine("Error in clsDBStatusLogger.LogStatus: " + ex.Message);
            }

        }

        private void AddSPParameter(SqlParameterCollection parameters, string paramName, string value, int varCharLength)
        {
            // Make sure the parameter starts with an @ sign
            if (!paramName.StartsWith("@"))
            {
                paramName = "@" + paramName;
            }

            parameters.Add(new SqlParameter(paramName, SqlDbType.VarChar, varCharLength)).Value = value;
        }

        private void AddSPParameter(SqlParameterCollection parameters, string paramName, int value)
        {
            // Make sure the parameter starts with an @ sign
            if (!paramName.StartsWith("@"))
            {
                paramName = "@" + paramName;
            }

            parameters.Add(new SqlParameter(paramName, SqlDbType.Int)).Value = value;
        }

        private void AddSPParameter(SqlParameterCollection parameters, string paramName, DateTime value)
        {
            // Make sure the parameter starts with an @ sign
            if (!paramName.StartsWith("@"))
            {
                paramName = "@" + paramName;
            }

            parameters.Add(new SqlParameter(paramName, SqlDbType.DateTime)).Value = value;
        }

        private void AddSPParameter(SqlParameterCollection parameters, string paramName, float value)
        {
            // Make sure the parameter starts with an @ sign
            if (!paramName.StartsWith("@"))
            {
                paramName = "@" + paramName;
            }

            parameters.Add(new SqlParameter(paramName, SqlDbType.Real)).Value = value;
        }

        private void AddSPParameterOutput(SqlParameterCollection parameters, string paramName, string value, int varCharLength)
        {
            // Make sure the parameter starts with an @ sign
            if (!paramName.StartsWith("@"))
            {
                paramName = "@" + paramName;
            }

            parameters.Add(new SqlParameter(paramName, SqlDbType.VarChar, varCharLength));
            parameters[paramName].Direction = ParameterDirection.Output;
            parameters[paramName].Value = value;
        }

        #endregion

    }
}
