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
    public class clsDBStatusLogger
    {

        #region "Structures"

        public struct udtStatusInfoType
        {
            public string MgrName;

            public EnumMgrStatus MgrStatus;

            /// <summary>
            /// Last update time (UTC-based)
            /// </summary>
            public DateTime LastUpdate;

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
            public string MostRecentErrorMessage;
            public udtTaskInfoType Task;
        }

        public struct udtTaskInfoType
        {
            public string Tool;
            public EnumTaskStatus Status;
            // Task duration, in hours
            public float DurationHours;
            // Percent complete, value between 0 and 100
            public float Progress;
            public string CurrentOperation;
            public udtTaskDetailsType TaskDetails;
        }

        public struct udtTaskDetailsType
        {
            public EnumTaskStatusDetail Status;
            public int Job;
            public int JobStep;
            public string Dataset;
            public string MostRecentLogMessage;
            public string MostRecentJobInfo;
            public int SpectrumCount;
        }

        #endregion

        #region "Module variables"

        /// <summary>
        /// Stored procedure that could be used to report manager status; typically not used
        /// </summary>
        /// <remarks>This stored procedure is valid, but the primary way that we track status is when WriteStatusFile calls LogStatusToMessageQueue</remarks>
        private const string SP_NAME_UPDATE_MANAGER_STATUS = "UpdateManagerAndTaskStatus";

        private readonly string m_DBConnectionString;

        /// <summary>
        /// The minimum interval between updating the manager status in the database
        /// </summary>
        private float m_DBStatusUpdateIntervalMinutes;


        private DateTime m_LastWriteTime;

        #endregion

        #region "Properties"

        public string DBConnectionString => m_DBConnectionString;

        public float DBStatusUpdateIntervalMinutes
        {
            get => m_DBStatusUpdateIntervalMinutes;
            set
            {
                if (value < 0)
                    value = 0;
                m_DBStatusUpdateIntervalMinutes = value;
            }
        }
        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="strDBConnectionString">Database connection string</param>
        /// <param name="sngDBStatusUpdateIntervalMinutes">Minimum interval between updating the manager status in the database</param>
        /// <remarks></remarks>
        public clsDBStatusLogger(string strDBConnectionString, float sngDBStatusUpdateIntervalMinutes)
        {
            if (strDBConnectionString == null)
                strDBConnectionString = string.Empty;
            m_DBConnectionString = strDBConnectionString;
            m_DBStatusUpdateIntervalMinutes = sngDBStatusUpdateIntervalMinutes;
            m_LastWriteTime = DateTime.MinValue;
        }

        /// <summary>
        /// Send status information to the database
        /// </summary>
        /// <param name="udtStatusInfo"></param>
        /// <param name="blnForceLogToDB"></param>
        /// <remarks>This function is valid, but the primary way that we track status is when WriteStatusFile calls LogStatusToMessageQueue</remarks>
        public void LogStatus(udtStatusInfoType udtStatusInfo, bool blnForceLogToDB)
        {

            try
            {
                if (string.IsNullOrEmpty(m_DBConnectionString))
                {
                    // Connection string not defined; unable to continue
                    return;
                }

                if (!blnForceLogToDB && DateTime.UtcNow.Subtract(m_LastWriteTime).TotalMinutes < m_DBStatusUpdateIntervalMinutes)
                {
                    // Not enough time has elapsed since the last write; exit sub
                    return;
                }
                m_LastWriteTime = DateTime.UtcNow;


                var myConnection = new SqlConnection(m_DBConnectionString);
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
                AddSPParameter(cmd.Parameters, "@MgrName", udtStatusInfo.MgrName, 128);
                AddSPParameter(cmd.Parameters, "@MgrStatusCode", (int)udtStatusInfo.MgrStatus);

                AddSPParameter(cmd.Parameters, "@LastUpdate", udtStatusInfo.LastUpdate.ToLocalTime());
                AddSPParameter(cmd.Parameters, "@LastStartTime", udtStatusInfo.LastStartTime.ToLocalTime());
                AddSPParameter(cmd.Parameters, "@CPUUtilization", udtStatusInfo.CPUUtilization);
                AddSPParameter(cmd.Parameters, "@FreeMemoryMB", udtStatusInfo.FreeMemoryMB);
                AddSPParameter(cmd.Parameters, "@ProcessID", udtStatusInfo.ProcessID);
                AddSPParameter(cmd.Parameters, "@ProgRunnerProcessID", udtStatusInfo.ProgRunnerProcessID);
                AddSPParameter(cmd.Parameters, "@ProgRunnerCoreUsage", udtStatusInfo.ProgRunnerCoreUsage);

                AddSPParameter(cmd.Parameters, "@MostRecentErrorMessage", udtStatusInfo.MostRecentErrorMessage, 1024);

                // Task items
                AddSPParameter(cmd.Parameters, "@StepTool", udtStatusInfo.Task.Tool, 128);
                AddSPParameter(cmd.Parameters, "@TaskStatusCode", (int)udtStatusInfo.Task.Status);
                AddSPParameter(cmd.Parameters, "@DurationHours", udtStatusInfo.Task.DurationHours);
                AddSPParameter(cmd.Parameters, "@Progress", udtStatusInfo.Task.Progress);
                AddSPParameter(cmd.Parameters, "@CurrentOperation", udtStatusInfo.Task.CurrentOperation, 256);

                // Task detail items
                AddSPParameter(cmd.Parameters, "@TaskDetailStatusCode", (int)udtStatusInfo.Task.TaskDetails.Status);
                AddSPParameter(cmd.Parameters, "@Job", udtStatusInfo.Task.TaskDetails.Job);
                AddSPParameter(cmd.Parameters, "@JobStep", udtStatusInfo.Task.TaskDetails.JobStep);
                AddSPParameter(cmd.Parameters, "@Dataset", udtStatusInfo.Task.TaskDetails.Dataset, 256);
                AddSPParameter(cmd.Parameters, "@MostRecentLogMessage", udtStatusInfo.Task.TaskDetails.MostRecentLogMessage, 1024);
                AddSPParameter(cmd.Parameters, "@MostRecentJobInfo", udtStatusInfo.Task.TaskDetails.MostRecentJobInfo, 256);
                AddSPParameter(cmd.Parameters, "@SpectrumCount", udtStatusInfo.Task.TaskDetails.SpectrumCount);

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

        private void AddSPParameter(SqlParameterCollection objParameters, string strParamName, string strValue, int intVarCharLength)
        {
            // Make sure the parameter starts with an @ sign
            if (!strParamName.StartsWith("@"))
            {
                strParamName = "@" + strParamName;
            }

            objParameters.Add(new SqlParameter(strParamName, SqlDbType.VarChar, intVarCharLength)).Value = strValue;
        }

        private void AddSPParameter(SqlParameterCollection objParameters, string strParamName, int intValue)
        {
            // Make sure the parameter starts with an @ sign
            if (!strParamName.StartsWith("@"))
            {
                strParamName = "@" + strParamName;
            }

            objParameters.Add(new SqlParameter(strParamName, SqlDbType.Int)).Value = intValue;
        }

        private void AddSPParameter(SqlParameterCollection objParameters, string strParamName, DateTime dtValue)
        {
            // Make sure the parameter starts with an @ sign
            if (!strParamName.StartsWith("@"))
            {
                strParamName = "@" + strParamName;
            }

            objParameters.Add(new SqlParameter(strParamName, SqlDbType.DateTime)).Value = dtValue;
        }

        private void AddSPParameter(SqlParameterCollection objParameters, string strParamName, float sngValue)
        {
            // Make sure the parameter starts with an @ sign
            if (!strParamName.StartsWith("@"))
            {
                strParamName = "@" + strParamName;
            }

            objParameters.Add(new SqlParameter(strParamName, SqlDbType.Real)).Value = sngValue;
        }

        private void AddSPParameterOutput(SqlParameterCollection objParameters, string strParamName, string strValue, int intVarCharLength)
        {
            // Make sure the parameter starts with an @ sign
            if (!strParamName.StartsWith("@"))
            {
                strParamName = "@" + strParamName;
            }

            objParameters.Add(new SqlParameter(strParamName, SqlDbType.VarChar, intVarCharLength));
            objParameters[strParamName].Direction = ParameterDirection.Output;
            objParameters[strParamName].Value = strValue;
        }


        #endregion

    }
}
