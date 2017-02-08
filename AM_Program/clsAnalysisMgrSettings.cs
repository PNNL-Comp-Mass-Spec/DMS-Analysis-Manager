//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/18/2007
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Xml;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerProg
{
    public class clsAnalysisMgrSettings : IMgrParams
    {
        //*********************************************************************************************************
        //Class for loading, storing and accessing manager parameters.
        //	Loads initial settings from local config file, then checks to see if remainder of settings should be
        //		loaded or manager set to inactive. If manager active, retrieves remainder of settings from manager
        //		parameters database.
        //*********************************************************************************************************

        #region "Constants"

        public const string DEACTIVATED_LOCALLY = "Manager deactivated locally";
        public const string MGR_PARAM_MGR_CFG_DB_CONN_STRING = "MgrCnfgDbConnectStr";
        public const string MGR_PARAM_MGR_ACTIVE_LOCAL = "MgrActive_Local";
        public const string MGR_PARAM_MGR_NAME = "MgrName";
        public const string MGR_PARAM_USING_DEFAULTS = "UsingDefaults";

        public const string MGR_PARAM_DEFAULT_DMS_CONN_STRING = "DefaultDMSConnString";

        #endregion

        #region "Module variables"

        private const string SP_NAME_ACKMANAGERUPDATE = "AckManagerUpdateRequired";
        private Dictionary<string, string> mParamDictionary;

        private string mErrMsg = "";
        private readonly string mEmergencyLogSource;
        private readonly string mEmergencyLogName;

        private readonly string mMgrFolderPath;
        private readonly bool mTraceMode;

        #endregion

        #region "Properties"

        public string ErrMsg
        {
            get { return mErrMsg; }
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Calls stored procedure AckManagerUpdateRequired in the Manager Control DB
        /// </summary>
        /// <remarks></remarks>
        public void AckManagerUpdateRequired()
        {
            try
            {
                // Data Source=proteinseqs;Initial Catalog=manager_control
                var connectionString = this.GetParam(MGR_PARAM_MGR_CFG_DB_CONN_STRING);

                if (mTraceMode == true)
                    ShowTraceMessage("AckManagerUpdateRequired using " + connectionString);

                var myConnection = new SqlConnection(connectionString);
                myConnection.Open();

                //Set up the command object prior to SP execution
                var myCmd = new SqlCommand(SP_NAME_ACKMANAGERUPDATE, myConnection) {CommandType = CommandType.StoredProcedure};

                myCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                myCmd.Parameters.Add(new SqlParameter("@managerName", SqlDbType.VarChar, 128)).Value = this.GetParam(MGR_PARAM_MGR_NAME);
                myCmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;

                //Execute the SP
                myCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                var msg = "Exception calling " + SP_NAME_ACKMANAGERUPDATE + ex.Message;
                Console.WriteLine(msg);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
            }
        }

        public bool DisableManagerLocally()
        {
            return WriteConfigSetting(MGR_PARAM_MGR_ACTIVE_LOCAL, "False");
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="emergencyLogSource">Source name registered for emergency logging</param>
        /// <param name="emergencyLogName">Name of system log for emergency logging</param>
        /// <param name="lstMgrSettings"></param>
        /// <param name="mgrFolderPath"></param>
        /// <param name="traceMode"></param>
        /// <remarks></remarks>
        public clsAnalysisMgrSettings(string emergencyLogSource, string emergencyLogName, Dictionary<string, string> lstMgrSettings,
            string mgrFolderPath, bool traceMode)
        {
            mEmergencyLogName = emergencyLogName;
            mEmergencyLogSource = emergencyLogSource;
            mMgrFolderPath = mgrFolderPath;
            mTraceMode = traceMode;

            if (!LoadSettings(lstMgrSettings))
            {
                if (!string.IsNullOrEmpty(mErrMsg))
                {
                    throw new ApplicationException("Unable to initialize manager settings class: " + mErrMsg);
                }
                else
                {
                    throw new ApplicationException("Unable to initialize manager settings class: unknown error");
                }
            }

            if (mTraceMode == true)
                ShowTraceMessage("Initialized clsAnalysisMgrSettings");
        }

        /// <summary>
        /// Loads manager settings from config file and database
        /// </summary>
        /// <param name="paramDictionary">Manager settings loaded from file AnalysisManagerProg.exe.config</param>
        /// <returns>True if successful; False on error</returns>
        /// <remarks></remarks>
        public bool LoadSettings(Dictionary<string, string> paramDictionary)
        {
            mErrMsg = "";

            mParamDictionary = paramDictionary;

            //Test the settings retrieved from the config file
            if (!CheckInitialSettings(mParamDictionary))
            {
                //Error logging handled by CheckInitialSettings
                return false;
            }

            //Determine if manager is deactivated locally
            if (!Convert.ToBoolean(mParamDictionary[MGR_PARAM_MGR_ACTIVE_LOCAL]))
            {
                WriteToEmergencyLog(mEmergencyLogSource, mEmergencyLogName, DEACTIVATED_LOCALLY);
                mErrMsg = DEACTIVATED_LOCALLY;
                return false;
            }

            //Get settings from Manager Control DB and Broker DB
            if (!LoadDBSettings())
            {
                // Errors have already been logged; return False
                return false;
            }

            //No problems found
            return true;
        }

        /// <summary>
        /// Tests initial settings retrieved from config file
        /// </summary>
        /// <param name="paramDictionary"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool CheckInitialSettings(Dictionary<string, string> paramDictionary)
        {
            string errorMessage = null;

            //Verify manager settings dictionary exists
            if (paramDictionary == null)
            {
                errorMessage = "clsMgrSettings.CheckInitialSettings(); Manager parameter string dictionary not found";

                if (mTraceMode == true)
                    ShowTraceMessage("Error in " + errorMessage);

                WriteToEmergencyLog(mEmergencyLogSource, mEmergencyLogName, errorMessage);
                return false;
            }

            //Verify intact config file was found
            string strValue = string.Empty;
            if (!paramDictionary.TryGetValue(MGR_PARAM_USING_DEFAULTS, out strValue))
            {
                errorMessage = "clsMgrSettings.CheckInitialSettings(); 'UsingDefaults' entry not found in Config file";

                if (mTraceMode == true)
                    ShowTraceMessage("Error in " + errorMessage);

                WriteToEmergencyLog(mEmergencyLogSource, mEmergencyLogName, errorMessage);
            }
            else
            {
                bool blnValue = false;
                if (bool.TryParse(strValue, out blnValue))
                {
                    if (blnValue)
                    {
                        errorMessage = "clsMgrSettings.CheckInitialSettings(); Config file problem, contains UsingDefaults=True";

                        if (mTraceMode == true)
                            ShowTraceMessage("Error in " + errorMessage);

                        WriteToEmergencyLog(mEmergencyLogSource, mEmergencyLogName, errorMessage);
                        return false;
                    }
                }
            }

            //No problems found
            return true;
        }

        private string GetGroupNameFromSettings(DataTable dtSettings)
        {
            foreach (DataRow oRow in dtSettings.Rows)
            {
                //Add the column heading and value to the dictionary
                string paramKey = DbCStr(oRow[dtSettings.Columns["ParameterName"]]);

                if (clsGlobal.IsMatch(paramKey, "MgrSettingGroupName"))
                {
                    string groupName = DbCStr(oRow[dtSettings.Columns["ParameterValue"]]);
                    if (!string.IsNullOrWhiteSpace(groupName))
                    {
                        return groupName;
                    }
                    else
                    {
                        return string.Empty;
                    }
                }
            }
            return string.Empty;
        }

        // Retrieves the manager and global settings from various databases
        public bool LoadDBSettings()
        {
            bool success = false;

            success = LoadMgrSettingsFromDB();

            if (success)
            {
                success = LoadBrokerDBSettings();
            }

            return success;
        }

        /// <summary>
        /// Gets manager config settings from manager control DB
        /// </summary>
        /// <returns>True for success; False for error</returns>
        /// <remarks></remarks>
        protected bool LoadMgrSettingsFromDB()
        {
            //Requests manager specific settings from database. Performs retries if necessary.

            var managerName = GetParam(MGR_PARAM_MGR_NAME, "");
            if (string.IsNullOrEmpty(managerName))
            {
                mErrMsg = "MgrName parameter not found in m_ParamDictionary; it should be defined in the AnalysisManagerProg.exe.config file";

                if (mTraceMode == true)
                    ShowTraceMessage("Error in LoadMgrSettingsFromDB: " + mErrMsg);

                return false;
            }

            DataTable dtSettings = null;
            var blnReturnErrorIfNoParameters = true;
            var success = LoadMgrSettingsFromDBWork(managerName, out dtSettings, blnReturnErrorIfNoParameters);
            if (!success)
            {
                return false;
            }

            var blnSkipExistingParameters = false;
            success = StoreParameters(dtSettings, blnSkipExistingParameters, managerName);
            if (!success)
                return false;

            while (success)
            {
                var strMgrSettingsGroup = GetGroupNameFromSettings(dtSettings);
                if (string.IsNullOrEmpty(strMgrSettingsGroup))
                {
                    break;
                }

                // This manager has group-based settings defined; load them now

                blnReturnErrorIfNoParameters = false;
                dtSettings = null;
                success = LoadMgrSettingsFromDBWork(strMgrSettingsGroup, out dtSettings, blnReturnErrorIfNoParameters);

                if (success)
                {
                    blnSkipExistingParameters = true;
                    success = StoreParameters(dtSettings, blnSkipExistingParameters, managerName);
                }
            }

            return success;
        }

        private bool LoadMgrSettingsFromDBWork(string ManagerName, out DataTable dtSettings, bool blnReturnErrorIfNoParameters)
        {
            const short retryCount = 6;
            dtSettings = null;

            // Data Source=proteinseqs;Initial Catalog=manager_control
            string connectionString = GetParam(MGR_PARAM_MGR_CFG_DB_CONN_STRING, string.Empty);

            if (string.IsNullOrEmpty(ManagerName))
            {
                mErrMsg =
                    "MgrCnfgDbConnectStr parameter not found in m_ParamDictionary; it should be defined in the AnalysisManagerProg.exe.config file";
                if (mTraceMode == true)
                    ShowTraceMessage("LoadMgrSettingsFromDBWork: " + mErrMsg);
                dtSettings = null;
                return false;
            }

            if (mTraceMode == true)
                ShowTraceMessage("LoadMgrSettingsFromDBWork using [" + connectionString + "] for manager " + ManagerName);

            string sqlStr = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" + ManagerName + "'";

            //Get a table to hold the results of the query
            var success = clsGlobal.GetDataTableByQuery(sqlStr, connectionString, "LoadMgrSettingsFromDBWork", retryCount, out dtSettings);

            // If unable to retrieve the data, return false
            if (!success)
            {
                // Log the message to the DB if the monthly Windows updates are not pending
                var allowLogToDB = !(clsWindowsUpdateStatus.ServerUpdatesArePending());

                mErrMsg = "clsMgrSettings.LoadMgrSettingsFromDBWork; Excessive failures attempting to retrieve manager settings from database " +
                          "for manager '" + ManagerName + "'";
                WriteErrorMsg(mErrMsg, allowLogToDB);

                if ((dtSettings != null))
                    dtSettings.Dispose();
                return false;
            }

            // Verify at least one row returned
            if (dtSettings.Rows.Count < 1 & blnReturnErrorIfNoParameters)
            {
                // No data was returned
                mErrMsg = "clsMgrSettings.LoadMgrSettingsFromDBWork; Manager '" + ManagerName + "' not defined in the manager control database; using " + connectionString;
                WriteErrorMsg(mErrMsg);
                dtSettings.Dispose();
                return false;
            }

            return true;
        }

        private bool StoreParameters(DataTable dtSettings, bool blnSkipExisting, string ManagerName)
        {
            bool success = false;

            //Fill a string dictionary with the manager parameters that have been found
            try
            {
                foreach (DataRow oRow in dtSettings.Rows)
                {
                    //Add the column heading and value to the dictionary
                    string paramKey = DbCStr(oRow[dtSettings.Columns["ParameterName"]]);
                    string paramVal = DbCStr(oRow[dtSettings.Columns["ParameterValue"]]);

                    if (mParamDictionary.ContainsKey(paramKey))
                    {
                        if (!blnSkipExisting)
                        {
                            mParamDictionary[paramKey] = paramVal;
                        }
                    }
                    else
                    {
                        mParamDictionary.Add(paramKey, paramVal);
                    }
                }
                success = true;
            }
            catch (Exception ex)
            {
                mErrMsg = "clsAnalysisMgrSettings.StoreParameters; Exception filling string dictionary from table for manager '" + ManagerName + "': " +
                          ex.Message;
                WriteErrorMsg(mErrMsg);
                success = false;
            }
            finally
            {
                if ((dtSettings != null))
                    dtSettings.Dispose();
            }

            return success;
        }

        /// <summary>
        /// Gets global settings from Broker DB (aka Pipeline DB)
        /// </summary>
        /// <returns>True for success; False for error</returns>
        /// <remarks></remarks>
        protected bool LoadBrokerDBSettings()
        {
            // Retrieves global settings from the Broker DB. Performs retries if necessary.
            //
            // At present, the only settings being retrieved are the param file storage paths for each step tool
            // The storage path for each step tool will be stored in the manager settings dictionary
            // For example: the LCMSFeatureFinder step tool will have an entry with
            //   Name="StepTool_ParamFileStoragePath_LCMSFeatureFinder"
            //   Value="\\gigasax\dms_parameter_Files\LCMSFeatureFinder"

            short retryCount = 6;
            string paramKey = null;
            string paramVal = null;
            string connectionString = this.GetParam("brokerconnectionstring");
            // Gigasax.DMS_Pipeline

            if (mTraceMode == true)
                ShowTraceMessage("LoadBrokerDBSettings has brokerconnectionstring = " + connectionString);

            // Construct the Sql to obtain the information:
            //   SELECT 'StepTool_ParamFileStoragePath_' + Name AS ParameterName, [Param File Storage Path] AS ParameterValue
            //   FROM V_Pipeline_Step_Tools_Detail_Report
            //   WHERE ISNULL([Param File Storage Path], '') <> ''
            //
            const string sqlStr =
                " SELECT '" + clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "' + Name AS ParameterName, " +
                " [Param File Storage Path] AS ParameterValue" + " FROM V_Pipeline_Step_Tools_Detail_Report" +
                " WHERE ISNULL([Param File Storage Path], '') <> ''";

            DataTable dt = null;

            if (mTraceMode == true)
                ShowTraceMessage("Query V_Pipeline_Step_Tools_Detail_Report in broker");

            //Get a table to hold the results of the query
            var success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), connectionString, "LoadBrokerDBSettings", retryCount, out dt);

            //If loop exited due to errors, return false
            if (!success)
            {
                var statusMessage = "clsMgrSettings.LoadBrokerDBSettings; Excessive failures attempting to retrieve settings from broker database";
                WriteErrorMsg(statusMessage);
                dt.Dispose();
                return false;
            }

            //Verify at least one row returned
            if (dt.Rows.Count < 1)
            {
                // No data was returned
                var statusMessage = "clsMgrSettings.LoadBrokerDBSettings; V_Pipeline_Step_Tools_Detail_Report returned no rows using " +
                                    connectionString;
                WriteErrorMsg(statusMessage);
                dt.Dispose();
                return false;
            }

            // Fill a string dictionary with the new parameters that have been found
            try
            {
                foreach (DataRow curRow in dt.Rows)
                {
                    //Add the column heading and value to the dictionary
                    paramKey = DbCStr(curRow[dt.Columns["ParameterName"]]);
                    paramVal = DbCStr(curRow[dt.Columns["ParameterValue"]]);

                    this.SetParam(paramKey, paramVal);
                }
                success = true;
            }
            catch (Exception ex)
            {
                var statusMessage = "clsMgrSettings.LoadBrokerDBSettings; Exception filling string dictionary from table: " + ex.Message;
                WriteErrorMsg(statusMessage);
                success = false;
            }
            finally
            {
                dt.Dispose();
            }

            return success;
        }

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <returns>String value associated with specified key</returns>
        /// <remarks>Returns empty string if key isn't found</remarks>
        public string GetParam(string itemKey)
        {
            string value = string.Empty;

            if ((mParamDictionary != null))
            {
                if (mParamDictionary.TryGetValue(itemKey, out value))
                {
                    if (string.IsNullOrWhiteSpace(value))
                    {
                        return string.Empty;
                    }
                }
                else
                {
                    return string.Empty;
                }
            }

            return value;
        }

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public bool GetParam(string itemKey, bool valueIfMissing)
        {
            return clsGlobal.CBoolSafe(GetParam(itemKey), valueIfMissing);
        }

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public int GetParam(string itemKey, int valueIfMissing)
        {
            return clsGlobal.CIntSafe(GetParam(itemKey), valueIfMissing);
        }

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public string GetParam(string itemKey, string valueIfMissing)
        {
            string strValue = null;
            strValue = GetParam(itemKey);
            if (string.IsNullOrEmpty(strValue))
            {
                return valueIfMissing;
            }
            else
            {
                return strValue;
            }
        }

        private static void ShowTraceMessage(string strMessage)
        {
            Console.WriteLine(System.DateTime.Now.ToString("hh:mm:ss.fff tt") + ": " + strMessage);
        }

        /// <summary>
        /// Sets a parameter in the parameters string dictionary
        /// </summary>
        /// <param name="itemKey">Key name for the item</param>
        /// <param name="itemValue">Value to assign to the key</param>
        /// <remarks></remarks>
        public void SetParam(string itemKey, string itemValue)
        {
            if (mParamDictionary.ContainsKey(itemKey))
            {
                mParamDictionary[itemKey] = itemValue;
            }
            else
            {
                mParamDictionary.Add(itemKey, itemValue);
            }
        }

        /// <summary>
        /// Writes an error message to the application log and the database
        /// </summary>
        /// <param name="errorMessage">Message to write</param>
        /// <param name="allowLogToDB"></param>
        /// <remarks></remarks>
        private void WriteErrorMsg(string errorMessage, bool allowLogToDB = true)
        {
            WriteToEmergencyLog(mEmergencyLogSource, mEmergencyLogName, errorMessage);
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage);

            if ((allowLogToDB))
            {
                // Also post a log to the database
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, errorMessage);
            }

            if (mTraceMode)
            {
                ShowTraceMessage(errorMessage);
            }
        }

        /// <summary>
        /// Converts a database output object that could be dbNull to a string
        /// </summary>
        /// <param name="inpObj"></param>
        /// <returns>String equivalent of object; empty string if object is dbNull</returns>
        /// <remarks></remarks>
        protected string DbCStr(object inpObj)
        {
            //If input object is DbNull, returns "", otherwise returns String representation of object
            if (object.ReferenceEquals(inpObj, DBNull.Value))
            {
                return string.Empty;
            }
            else
            {
                return Convert.ToString(inpObj);
            }
        }

        /// <summary>
        /// Writes specfied value to an application config file.
        /// </summary>
        /// <param name="key">Name for parameter (case sensitive)</param>
        /// <param name="value">New value for parameter</param>
        /// <returns>TRUE for success; FALSE for error (ErrMsg property contains reason)</returns>
        /// <remarks>This bit of lunacy is needed because MS doesn't supply a means to write to an app config file</remarks>
        public bool WriteConfigSetting(string key, string value)
        {
            mErrMsg = "";

            //Load the config document
            XmlDocument myDoc = LoadConfigDocument();
            if (myDoc == null)
            {
                //Error message has already been produced by LoadConfigDocument
                return false;
            }

            //Retrieve the settings node
            XmlNode myNode = myDoc.SelectSingleNode("//applicationSettings");

            if (myNode == null)
            {
                mErrMsg = "clsMgrSettings.WriteConfigSettings; appSettings node not found";
                return false;
            }

            try
            {
                //Select the element containing the value for the specified key containing the key
                XmlElement myElement = (XmlElement) myNode.SelectSingleNode(string.Format("//setting[@name='{0}']/value", key));
                if (myElement != null)
                {
                    //Set key to specified value
                    myElement.InnerText = value;
                }
                else
                {
                    //Key was not found
                    mErrMsg = "clsMgrSettings.WriteConfigSettings; specified key not found: " + key;
                    return false;
                }
                myDoc.Save(GetConfigFilePath());
                return true;
            }
            catch (Exception ex)
            {
                mErrMsg = "clsMgrSettings.WriteConfigSettings; Exception updating settings file: " + ex.Message;
                return false;
            }
        }

        protected void WriteToEmergencyLog(string sourceName, string logName, string message)
        {
            // Post a message to the the Windows application event log named LogName
            // If the application log does not exist yet, we will try to create it
            // However, in order to do that, the program needs to be running from an elevated (administrative level) command prompt
            // Thus, it is advisable to run this program once from an elevated command prompt while MgrActive_Local is set to false

            //If custom event log doesn't exist yet, create it
            if (!EventLog.SourceExists(sourceName))
            {
                EventSourceCreationData sourceData = new EventSourceCreationData(sourceName, logName);
                EventLog.CreateEventSource(sourceData);
            }

            //Create custom event logging object and write to log
            EventLog customEventLog = new EventLog();
            customEventLog.Log = logName;
            customEventLog.Source = sourceName;

            try
            {
                customEventLog.MaximumKilobytes = 1024;
            }
            catch (Exception ex)
            {
                // Leave this as the default
            }

            try
            {
                customEventLog.ModifyOverflowPolicy(OverflowAction.OverwriteAsNeeded, 90);
            }
            catch (Exception ex)
            {
                // Leave this as the default
            }

            EventLog.WriteEntry(sourceName, message, EventLogEntryType.Error);
        }

        /// <summary>
        /// Loads an app config file for changing parameters
        /// </summary>
        /// <returns>App config file as an XML document if successful; NOTHING on failure</returns>
        /// <remarks></remarks>
        private XmlDocument LoadConfigDocument()
        {
            try
            {
                var myDoc = new XmlDocument();
                myDoc.Load(GetConfigFilePath());
                return myDoc;
            }
            catch (Exception ex)
            {
                mErrMsg = "clsMgrSettings.LoadConfigDocument; Exception loading settings file: " + ex.Message;
                return null;
            }
        }

        /// <summary>
        /// Specifies the full name and path for the application config file
        /// </summary>
        /// <returns>String containing full name and path</returns>
        /// <remarks></remarks>
        private string GetConfigFilePath()
        {
            var exeName = Path.GetFileName(Assembly.GetExecutingAssembly().Location);
            return Path.Combine(mMgrFolderPath, exeName + ".config");
        }

        #endregion
    }
}
