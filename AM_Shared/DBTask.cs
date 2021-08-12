using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Text;
using System.Xml.Linq;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;
using PRISMDatabaseUtils;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 10/26/2007
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{
    /// <summary>
    /// Base class for handling task-related data
    /// </summary>
    public abstract class DBTask : LoggerBase
    {
        #region "Enums"

        /// <summary>
        /// Tracks the outcome of requesting a new task
        /// </summary>
        public enum RequestTaskResult
        {
            /// <summary>
            /// Task found
            /// </summary>
            TaskFound = 0,

            /// <summary>
            /// No task found
            /// </summary>
            NoTaskFound = 1,

            /// <summary>
            /// Error requesting a task
            /// </summary>
            ResultError = 2,

            /// <summary>
            /// Too many retries
            /// </summary>
            TooManyRetries = 3,

            /// <summary>
            /// Deadlock
            /// </summary>
            Deadlock = 4
        }

        #endregion

        #region "Constants"

        /// <summary>
        /// Return value for success
        /// </summary>
        public const int RET_VAL_OK = 0;

        /// <summary>
        /// Return value when a task is not available
        /// </summary>
        public const int RET_VAL_TASK_NOT_AVAILABLE = 53000;

        #endregion

        #region "Module variables"

        /// <summary>
        /// Manager parameters
        /// </summary>
        /// <remarks>Instance of class AnalysisMgrSettings</remarks>
        protected IMgrParams mMgrParams;

        /// <summary>
        /// Connection string
        /// </summary>
        /// <remarks>Typically DMS5 on Gigasax</remarks>
        protected string mConnStr;

        /// <summary>
        /// Broker connection string
        /// </summary>
        /// <remarks>Typically DMS_Pipeline on Gigasax</remarks>
        protected string mBrokerConnStr;

        /// <summary>
        /// DMS stored procedure executor
        /// </summary>
        public IDBTools DMSProcedureExecutor { get; }

        /// <summary>
        /// Pipeline database stored procedure executor
        /// </summary>
        public IDBTools PipelineDBProcedureExecutor { get; }

        #endregion

        #region "Properties"
        /// <summary>
        /// Value showing if a transfer task was assigned
        /// </summary>
        /// <returns>True if task was assigned, otherwise false</returns>
        public bool TaskWasAssigned { get; protected set; } = false;

        /// <summary>
        /// Debug level
        /// </summary>
        /// <remarks>Values from 0 (minimum output) to 5 (max detail)</remarks>
        public short DebugLevel
        {
            get => mDebugLevel;
            set => mDebugLevel = value;
        }

        /// <summary>
        /// Manager name
        /// </summary>
        public string ManagerName { get; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">An IMgrParams object containing manager parameters</param>
        /// <param name="debugLvl">Debug level</param>
        protected DBTask(IMgrParams mgrParams, short debugLvl)
        {
            mMgrParams = mgrParams;

            ManagerName = mMgrParams.ManagerName;

            // Gigasax.DMS5
            mConnStr = mMgrParams.GetParam("ConnectionString");

            // Gigasax.DMS_Pipeline
            mBrokerConnStr = mMgrParams.GetParam("BrokerConnectionString");

            mDebugLevel = debugLvl;

            DMSProcedureExecutor = DbToolsFactory.GetDBTools(mConnStr, debugMode: mMgrParams.TraceMode);
            PipelineDBProcedureExecutor = DbToolsFactory.GetDBTools(mBrokerConnStr, debugMode: mMgrParams.TraceMode);

            DMSProcedureExecutor.DebugEvent += ProcedureExecutor_DebugEvent;
            PipelineDBProcedureExecutor.DebugEvent += ProcedureExecutor_DebugEvent;

            DMSProcedureExecutor.StatusEvent += ProcedureExecutor_StatusEvent;
            PipelineDBProcedureExecutor.StatusEvent += ProcedureExecutor_StatusEvent;

            DMSProcedureExecutor.WarningEvent += ProcedureExecutor_WarningEvent;
            PipelineDBProcedureExecutor.WarningEvent += ProcedureExecutor_WarningEvent;

            DMSProcedureExecutor.ErrorEvent += ProcedureExecutor_DBErrorEvent;
            PipelineDBProcedureExecutor.ErrorEvent += ProcedureExecutor_DBErrorEvent;

            if (mDebugLevel > 1)
            {
                DMSProcedureExecutor.DebugMessagesEnabled = true;
                PipelineDBProcedureExecutor.DebugMessagesEnabled = true;
            }
        }

        /// <summary>
        /// Populate the job parameters list using XML-based job parameters
        /// </summary>
        /// <param name="jobParamsXML"></param>
        protected IEnumerable<JobParameter> ParseXMLJobParameters(string jobParamsXML)
        {
            try
            {
                var jobParameters = new List<JobParameter>();

                using var reader = new StringReader(jobParamsXML);

                // Note that XDocument supersedes XmlDocument and XPathDocument
                // XDocument can often be easier to use since XDocument is LINQ-based

                var contents = reader.ReadToEnd();
                if (string.IsNullOrWhiteSpace(contents))
                {
                    LogError("Empty job parameters passed to ParseXMLJobParameters");
                    return new List<JobParameter>();
                }

                var doc = XDocument.Parse(contents);

                foreach (var section in doc.Elements("sections").Elements("section"))
                {
                    string sectionName;
                    if (section.HasAttributes)
                    {
                        var sectionNameAttrib = section.Attribute("name");

                        if (sectionNameAttrib == null)
                        {
                            LogWarning("Job params XML section found without a name attribute");
                            sectionName = string.Empty;
                        }
                        else
                        {
                            sectionName = sectionNameAttrib.Value;
                        }
                    }
                    else
                    {
                        sectionName = string.Empty;
                    }

                    foreach (var item in section.Elements("item"))
                    {
                        if (!item.HasAttributes)
                            continue;

                        var keyAttrib = item.Attribute("key");
                        if (keyAttrib == null)
                            continue;

                        var valueAttrib = item.Attribute("value");
                        if (valueAttrib == null)
                            continue;

                        var parameter = new JobParameter(sectionName, keyAttrib.Value, valueAttrib.Value);
                        jobParameters.Add(parameter);
                    }
                }

                return jobParameters;
            }
            catch (Exception ex)
            {
                LogError("Exception determining job parameters in ParseXMLJobParameters", ex);
                return new List<JobParameter>();
            }
        }

        /// <summary>
        /// Debugging routine for printing SP calling params
        /// </summary>
        /// <param name="sqlCmd">SQL command object containing params</param>
        protected void PrintCommandParams(DbCommand sqlCmd)
        {
            // Verify there really are command parameters
            if (sqlCmd == null)
                return;
            if (sqlCmd.Parameters.Count < 1)
                return;

            var paramDetails = new StringBuilder();

            foreach (DbParameter param in sqlCmd.Parameters)
            {
                paramDetails.AppendLine(string.Format(
                    "  Name= {0,-25}  Value= {1}",
                    param.ParameterName,
                    param.Value.CastDBVal<string>()));
            }

            LogDebug(paramDetails.ToString().TrimStart());
        }

        #endregion

        #region "Event Handlers"

        private void ProcedureExecutor_DebugEvent(string message)
        {
            LogDebug(message, (int)BaseLogger.LogLevels.DEBUG);
        }

        private void ProcedureExecutor_StatusEvent(string message)
        {
            LogDebug(message, (int)BaseLogger.LogLevels.INFO);
        }

        private void ProcedureExecutor_WarningEvent(string message)
        {
            LogDebug(message, (int)BaseLogger.LogLevels.WARN);
        }

        private void ProcedureExecutor_DBErrorEvent(string message, Exception ex)
        {
            if (message.IndexOf("permission was denied", StringComparison.OrdinalIgnoreCase) >= 0 ||
                message.IndexOf("permission denied", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                try
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogDb, BaseLogger.LogLevels.ERROR, message);
                }
                catch (Exception ex2)
                {
                    Global.ErrorWritingToLog(message, ex2);
                }
            }

            LogError(message);
        }

        #endregion

    }
}