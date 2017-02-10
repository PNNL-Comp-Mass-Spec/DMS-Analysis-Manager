
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Xml.XPath;
using System.Xml;
using System.IO;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
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
    public abstract class clsDBTask : clsLoggerBase
    {

        #region "Enums"
        public enum RequestTaskResult
        {
            TaskFound = 0,
            NoTaskFound = 1,
            ResultError = 2,
            TooManyRetries = 3,
            Deadlock = 4
        }
        #endregion

        #region "Constants"
        public const int RET_VAL_OK = 0;
        // Timeout expired
        public const int RET_VAL_EXCESSIVE_RETRIES = -5;
        // Transaction (Process ID 143) was deadlocked on lock resources with another process and has been chosen as the deadlock victim
        public const int RET_VAL_DEADLOCK = -4;
        public const int RET_VAL_TASK_NOT_AVAILABLE = 53000;
        #endregion
        public const int DEFAULT_SP_RETRY_COUNT = 3;

        #region "Module variables"

        /// <summary>
        /// Manager parameters
        /// </summary>
        protected IMgrParams m_MgrParams;
        protected string m_ConnStr;

        protected string m_BrokerConnStr;

        /// <summary>
        /// Job status
        /// </summary>
        protected bool m_TaskWasAssigned = false;

        protected string m_Xml_Text;
        public readonly PRISM.DataBase.clsExecuteDatabaseSP DMSProcedureExecutor;

        public readonly PRISM.DataBase.clsExecuteDatabaseSP PipelineDBProcedureExecutor;
        #endregion

        #region "Structures"
        public struct udtParameterInfoType
        {
            public string Section;
            public string ParamName;
            public string Value;
        }
        #endregion

        #region "Properties"
        /// <summary>
        /// Value showing if a transfer task was assigned
        /// </summary>
        /// <value></value>
        /// <returns>TRUE if task was assigned; otherwise false</returns>
        /// <remarks></remarks>
        public bool TaskWasAssigned => m_TaskWasAssigned;

        /// <summary>
        /// Debug level
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks>Values from 0 (minimum output) to 5 (max detail)</remarks>
        public short DebugLevel
        {
            get { return m_DebugLevel; }
            set { m_DebugLevel = value; }
        }
        #endregion

        #region "Methods"
        
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">An IMgrParams object containing manager parameters</param>
        /// <param name="debugLvl">Debug level</param>
        /// <remarks></remarks>
        protected clsDBTask(IMgrParams mgrParams, short debugLvl)
        {
            m_MgrParams = mgrParams;

            // Gigasax.DMS5
            m_ConnStr = m_MgrParams.GetParam("ConnectionString");

            // Gigasax.DMS_Pipeline
            m_BrokerConnStr = m_MgrParams.GetParam("brokerconnectionstring");
            
            m_DebugLevel = debugLvl;

            DMSProcedureExecutor = new PRISM.DataBase.clsExecuteDatabaseSP(m_ConnStr);
            PipelineDBProcedureExecutor = new PRISM.DataBase.clsExecuteDatabaseSP(m_BrokerConnStr);

            DMSProcedureExecutor.DebugEvent += ProcedureExecutor_DebugEvent;
            PipelineDBProcedureExecutor.DebugEvent += ProcedureExecutor_DebugEvent;

            DMSProcedureExecutor.DBErrorEvent += ProcedureExecutor_DBErrorEvent;
            PipelineDBProcedureExecutor.DBErrorEvent += ProcedureExecutor_DBErrorEvent;

            if (m_DebugLevel > 1)
            {
                DMSProcedureExecutor.DebugMessagesEnabled = true;
                PipelineDBProcedureExecutor.DebugMessagesEnabled = true;
            }

        }

        /// <summary>
        /// Requests a task
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public abstract RequestTaskResult RequestTask();


        /// <summary>
        /// Closes out a task
        /// </summary>
        /// <param name="CloseOut"></param>
        /// <param name="CompMsg"></param>
        /// <remarks></remarks>
        public abstract void CloseTask(CloseOutType CloseOut, string CompMsg);

        /// <summary>
        /// Closes out a task (includes EvalCode and EvalMessgae)
        /// </summary>
        /// <param name="CloseOut"></param>
        /// <param name="CompMsg"></param>
        /// <param name="EvalCode">Evaluation code (0 if no special evaulation message)</param>
        /// <param name="EvalMessage">Evaluation message ("" if no special message)</param>
        /// <remarks></remarks>
        public abstract void CloseTask(CloseOutType CloseOut, string CompMsg, int EvalCode, string EvalMessage);

        protected IEnumerable<udtParameterInfoType> FillParamDictXml(string jobParamsXML)
        {

            try
            {
                // Read XML string into XPathDocument object 
                // and set up navigation objects to traverse it

                using (XmlReader xReader = new XmlTextReader(new StringReader(jobParamsXML)))
                {

                    var xdoc = new XPathDocument(xReader);
                    var xpn = xdoc.CreateNavigator();
                    var nodes = xpn.Select("//item");

                    var dctParameters = new List<udtParameterInfoType>();

                    // Traverse the parsed XML document and extract the key and value for each item
                    while (nodes.MoveNext())
                    {
                        // Extract section, key, and value from XML element and append entry to dctParameterInfo
                        var udtParamInfo = new udtParameterInfoType
                        {
                            ParamName = nodes.Current.GetAttribute("key", ""),
                            Value = nodes.Current.GetAttribute("value", "")
                        };


                        // Extract the section name for the current item and dump it to output
                        var nav2 = nodes.Current.Clone();
                        nav2.MoveToParent();

                        udtParamInfo.Section = nav2.GetAttribute("name", "");

                        dctParameters.Add(udtParamInfo);

                    }

                    return dctParameters;

                }

            }
            catch (Exception ex)
            {
                LogError("clsDBTask.FillParamDict(), exception filling dictionary", ex);
                return null;
            }

        }

        /// <summary>
        /// Debugging routine for printing SP calling params
        /// </summary>
        /// <param name="InpCmd">SQL command object containing params</param>
        /// <remarks></remarks>
        protected void PrintCommandParams(SqlCommand InpCmd)
        {
            //Verify there really are command paramters
            if (InpCmd == null)
                return;
            if (InpCmd.Parameters.Count < 1)
                return;

            var MyMsg = "";

            foreach (SqlParameter MyParam in InpCmd.Parameters)
            {
                MyMsg += Environment.NewLine + "Name= " + MyParam.ParameterName + "\t, Value= " + clsGlobal.DbCStr(MyParam.Value);
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parameter list:" + MyMsg);

        }

        #endregion

        #region "Event Handlers"

        private void ProcedureExecutor_DebugEvent(string message)
        {
            LogDebug(message, (int)clsLogTools.LogLevels.DEBUG);
        }

        private void ProcedureExecutor_DBErrorEvent(string message)
        {
            if (message.Contains("permission was denied"))
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, message);
            }
            LogError(message);
        }

        #endregion

    }

}