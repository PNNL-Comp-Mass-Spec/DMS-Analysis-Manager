//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/18/2007
//
//*********************************************************************************************************

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Xml;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerProg
{
    /// <summary>
    /// Class for loading, storing and accessing manager parameters.
    /// </summary>
    /// <remarks>
    /// Loads initial settings from local config file, then checks to see if remainder of settings should be loaded or manager set to inactive.
    /// If manager active, retrieves remainder of settings manager parameters database.
    /// </remarks>
    public class clsAnalysisMgrSettings : clsLoggerBase, IMgrParams
    {

        #region "Constants"

        const string SP_NAME_ACKMANAGERUPDATE = "AckManagerUpdateRequired";

        /// <summary>
        /// Status message for when the manager is deactivated locally
        /// </summary>
        /// <remarks>Used when MgrActive_Local is False in AppName.exe.config</remarks>
        public const string DEACTIVATED_LOCALLY = "Manager deactivated locally";

        /// <summary>
        /// File with settings loaded when OfflineMode is enabled
        /// </summary>
        /// <remarks>Includes key settings that are retrieved from the Manager_Control database when OfflineMode is false</remarks>
        public const string LOCAL_MANAGER_SETTINGS_FILE = "ManagerSettingsLocal.xml";

        /// <summary>
        /// Manager parameter: config database connection string
        /// </summary>
        public const string MGR_PARAM_MGR_CFG_DB_CONN_STRING = "MgrCnfgDbConnectStr";

        /// <summary>
        /// Manager parameter: manager active
        /// </summary>
        /// <remarks>
        /// Defined in the manager control database
        /// If clsGlobal.OfflineMode is true, equivalent to MgrActive_Local
        /// </remarks>
        public const string MGR_PARAM_MGR_ACTIVE = "mgractive";

        /// <summary>
        /// Manager parameter: manager active
        /// </summary>
        /// <remarks>Defined in AppName.exe.config</remarks>
        public const string MGR_PARAM_MGR_ACTIVE_LOCAL = "MgrActive_Local";

        /// <summary>
        /// Manager parameter: manager name
        /// </summary>
        public const string MGR_PARAM_MGR_NAME = "MgrName";

        /// <summary>
        /// Manager parameter: using defaults flag
        /// </summary>
        public const string MGR_PARAM_USING_DEFAULTS = "UsingDefaults";

        /// <summary>
        /// Connection string to DMS5
        /// </summary>
        public const string MGR_PARAM_DEFAULT_DMS_CONN_STRING = "DefaultDMSConnString";

        /// <summary>
        /// Manager parameter: failed results folder path
        /// </summary>
        /// <remarks>Directory where results from failed analysis tasks are stored</remarks>
        public const string MGR_PARAM_FAILED_RESULTS_FOLDER_PATH = "FailedResultsFolderPath";

        /// <summary>
        /// Manager parameter: local task queue path
        /// Used by managers running in Offline mode
        /// </summary>
        /// <remarks>
        /// Directory with a Job .info, .status, .lock, etc. files,
        /// organized by subdirectories named after each step tool
        /// </remarks>
        public const string MGR_PARAM_LOCAL_TASK_QUEUE_PATH = "LocalTaskQueuePath";

        /// <summary>
        /// Manager parameter: local work dir path
        /// Used by managers running in Offline mode
        /// </summary>
        /// <remarks>
        /// Directory with JobX_StepY subdirectories containing files required for a single job step
        /// Each manager also has its own subdirectory for staging files
        /// </remarks>
        public const string MGR_PARAM_LOCAL_WORK_DIR_PATH = "LocalWorkDirPath";

        /// <summary>
        /// Working directory for the manager
        /// </summary>
        /// <remarks>When running offline jobs, this path will be updated for each job task</remarks>
        public const string MGR_PARAM_WORK_DIR = "WorkDir";

        #endregion

        #region "Class variables"

        private readonly Dictionary<string, string> mParamDictionary;

        private bool mMCParamsLoaded;

        private string mErrMsg = string.Empty;

        private readonly string mMgrFolderPath;

        #endregion

        #region "Properties"

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrMsg => mErrMsg;

        /// <summary>
        /// Manager name
        /// </summary>
        public string ManagerName => GetParam(MGR_PARAM_MGR_NAME, Environment.MachineName + "_Undefined-Manager");

        /// <summary>
        /// When true, show additional messages at the console
        /// </summary>
        public bool TraceMode { get; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="lstMgrSettings">Manager settings loaded from file AppName.exe.config</param>
        /// <param name="mgrFolderPath"></param>
        /// <param name="traceMode"></param>
        /// <remarks></remarks>
        public clsAnalysisMgrSettings(
            Dictionary<string, string> lstMgrSettings,
            string mgrFolderPath,
            bool traceMode)
        {
            mMgrFolderPath = mgrFolderPath;
            TraceMode = traceMode;

            mParamDictionary = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            if (!LoadSettings(lstMgrSettings))
            {
                if (!string.IsNullOrEmpty(mErrMsg))
                {
                    throw new ApplicationException("Unable to initialize manager settings class: " + mErrMsg);
                }

                throw new ApplicationException("Unable to initialize manager settings class: unknown error");
            }


            if (!TraceMode)
                return;

            ShowTraceMessage("Initialized IMgrParams");
            ShowDictionaryTrace(mParamDictionary);
        }

        /// <summary>
        /// Calls stored procedure AckManagerUpdateRequired to acknowledge that the manager has exited so that an update can be applied
        /// </summary>
        public void AckManagerUpdateRequired()
        {
            try
            {
                // Data Source=proteinseqs;Initial Catalog=manager_control
                var connectionString = GetParam(MGR_PARAM_MGR_CFG_DB_CONN_STRING);

                if (string.IsNullOrWhiteSpace(connectionString))
                {
                    if (clsGlobal.OfflineMode)
                        LogDebug("Skipping call to " + SP_NAME_ACKMANAGERUPDATE + " since offline");
                    else
                        LogError("Skipping call to " + SP_NAME_ACKMANAGERUPDATE + " since the Manager Control connection string is empty");

                    return;
                }

                ShowTrace("AckManagerUpdateRequired using " + connectionString);

                var conn = new SqlConnection(connectionString);
                conn.Open();

                // Set up the command object prior to SP execution
                var cmd = new SqlCommand(SP_NAME_ACKMANAGERUPDATE, conn)
                {
                    CommandType = CommandType.StoredProcedure
                };

                cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                cmd.Parameters.Add(new SqlParameter("@managerName", SqlDbType.VarChar, 128)).Value = ManagerName;
                cmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;

                // Execute the SP
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                LogError("Exception calling " + SP_NAME_ACKMANAGERUPDATE, ex);
            }
        }

        /// <summary>
        /// Disable the manager by changing MgrActive_Local to False in AnalysisManagerProg.exe.config
        /// </summary>
        /// <returns></returns>
        public bool DisableManagerLocally()
        {
            return WriteConfigSetting(MGR_PARAM_MGR_ACTIVE_LOCAL, "False");
        }

        /// <summary>
        /// Updates manager settings, then loads settings from the database or from ManagerSettingsLocal.xml if clsGlobal.OfflineMode is true
        /// </summary>
        /// <param name="configFileSettings">Manager settings loaded from file AppName.exe.config</param>
        /// <returns>True if successful; False on error</returns>
        /// <remarks></remarks>
        public bool LoadSettings(Dictionary<string, string> configFileSettings)
        {
            mErrMsg = string.Empty;

            mParamDictionary.Clear();

            foreach (var item in configFileSettings)
            {
                mParamDictionary.Add(item.Key, item.Value);
            }

            // Test the settings retrieved from the config file
            if (!CheckInitialSettings(mParamDictionary))
            {
                // Error logging handled by CheckInitialSettings
                return false;
            }

            // Determine if manager is deactivated locally
            if (!mParamDictionary.TryGetValue(MGR_PARAM_MGR_ACTIVE_LOCAL, out var activeLocalText))
            {
                mErrMsg = "Manager parameter " + MGR_PARAM_MGR_ACTIVE_LOCAL + " is missing from file " + Path.GetFileName(GetConfigFilePath());
                LogError(mErrMsg);
            }

            if (!bool.TryParse(activeLocalText, out var activeLocal) || !activeLocal)
            {
                LogWarning(DEACTIVATED_LOCALLY);
                mErrMsg = DEACTIVATED_LOCALLY;
                return false;
            }

            // Load settings from Manager Control DB and Broker DB
            // or from ManagerSettingsLocal.xml if clsGlobal.OfflineMode is true
            if (!LoadDBSettings())
            {
                // Error logging handled by LoadDBSettings
                return false;
            }

            // Set flag indicating manager parameters have been loaded
            mMCParamsLoaded = true;

            // No problems found
            return true;
        }

        /// <summary>
        /// Tests initial settings retrieved from config file
        /// </summary>
        /// <param name="paramDictionary"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool CheckInitialSettings(IReadOnlyDictionary<string, string> paramDictionary)
        {
            // Verify manager settings dictionary exists
            if (paramDictionary == null)
            {
                mErrMsg = "CheckInitialSettings: Manager parameter string dictionary not found";
                LogError(mErrMsg, true);
                return false;
            }

            // Verify intact config file was found
            if (!paramDictionary.TryGetValue(MGR_PARAM_USING_DEFAULTS, out var usingDefaultsText))
            {
                mErrMsg = "CheckInitialSettings: 'UsingDefaults' entry not found in Config file";
                LogError(mErrMsg, true);
            }
            else
            {
                if (bool.TryParse(usingDefaultsText, out var usingDefaults) && usingDefaults)
                {
                    mErrMsg = "CheckInitialSettings: Config file problem, contains UsingDefaults=True";
                    LogError(mErrMsg, true);
                    return false;
                }
            }

            // No problems found
            return true;
        }

        private string GetGroupNameFromSettings(DataTable dtSettings)
        {
            foreach (DataRow currentRow in dtSettings.Rows)
            {
                // Add the column heading and value to the dictionary
                var paramKey = DbCStr(currentRow[dtSettings.Columns["ParameterName"]]);

                if (string.Equals(paramKey, "MgrSettingGroupName", StringComparison.OrdinalIgnoreCase))
                {
                    var groupName = DbCStr(currentRow[dtSettings.Columns["ParameterValue"]]);
                    if (!string.IsNullOrWhiteSpace(groupName))
                    {
                        return groupName;
                    }

                    return string.Empty;
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// Retrieves the manager and global settings from the Manager Control and Broker databases
        /// Or, if clsGlobal.OfflineMode is true, load settings from file ManagerSettingsLocal.xml
        /// </summary>
        /// <returns></returns>
        public bool LoadDBSettings()
        {
            if (clsGlobal.OfflineMode)
            {
                var successLocal = LoadLocalSettings();
                if (!successLocal)
                    return false;

                var foldersValidated = ValidateOfflineTaskDirectories();
                return foldersValidated;
            }

            var success = LoadMgrSettingsFromDB();

            if (!success)
                return false;

            var brokerSuccess = LoadBrokerDBSettings();
            return brokerSuccess;
        }

        /// <summary>
        /// Gets manager config settings from manager control DB (Manager_Control)
        /// </summary>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks>Performs retries if necessary.</remarks>
        private bool LoadMgrSettingsFromDB()
        {

            var managerName = GetParam(MGR_PARAM_MGR_NAME, string.Empty);

            if (string.IsNullOrEmpty(managerName))
            {
                mErrMsg = "Manager parameter " + MGR_PARAM_MGR_NAME + " is missing from file " + Path.GetFileName(GetConfigFilePath());
                LogError(mErrMsg);
                return false;
            }

            var success = LoadMgrSettingsFromDBWork(managerName, out var dtSettings, logConnectionErrors: true, returnErrorIfNoParameters: true);
            if (!success)
            {
                return false;
            }

            success = StoreParameters(dtSettings, skipExistingParameters: false, managerName: managerName);

            if (!success)
                return false;

            while (success)
            {
                var mgrSettingsGroup = GetGroupNameFromSettings(dtSettings);
                if (string.IsNullOrEmpty(mgrSettingsGroup))
                {
                    break;
                }

                // This manager has group-based settings defined; load them now

                success = LoadMgrSettingsFromDBWork(mgrSettingsGroup, out dtSettings, logConnectionErrors: true, returnErrorIfNoParameters: false);

                if (success)
                {
                    success = StoreParameters(dtSettings, skipExistingParameters: true, managerName: mgrSettingsGroup);
                }
            }

            return success;
        }

        private bool LoadMgrSettingsFromDBWork(
            string managerName,
            out DataTable dtSettings,
            bool logConnectionErrors,
            bool returnErrorIfNoParameters)
        {
            const short retryCount = 6;

            // Data Source=proteinseqs;Initial Catalog=manager_control
            var connectionString = GetParam(MGR_PARAM_MGR_CFG_DB_CONN_STRING, string.Empty);

            if (string.IsNullOrEmpty(managerName))
            {
                mErrMsg = "MgrCnfgDbConnectStr parameter not found in m_ParamDictionary; " +
                          "it should be defined in the " + Path.GetFileName(GetConfigFilePath()) + " file";

                ShowTrace("LoadMgrSettingsFromDBWork: " + mErrMsg);

                dtSettings = null;
                return false;
            }

            if (string.IsNullOrEmpty(connectionString))
            {
                mErrMsg = MGR_PARAM_MGR_CFG_DB_CONN_STRING +
                           " parameter not found in mParamDictionary; it should be defined in the " + Path.GetFileName(GetConfigFilePath()) + " file";
                WriteErrorMsg(mErrMsg);
                dtSettings = null;
                return false;
            }

            ShowTrace("LoadMgrSettingsFromDBWork using [" + connectionString + "] for manager " + managerName);

            var sqlStr = "SELECT ParameterName, ParameterValue FROM V_MgrParams WHERE ManagerName = '" + managerName + "'";

            // Get a table to hold the results of the query
            var success = clsGlobal.GetDataTableByQuery(sqlStr, connectionString, "LoadMgrSettingsFromDBWork", retryCount, out dtSettings);

            // If unable to retrieve the data, return false
            if (!success)
            {
                // Log the message to the DB if the monthly Windows updates are not pending
                var allowLogToDB = !clsWindowsUpdateStatus.ServerUpdatesArePending();

                mErrMsg = "LoadMgrSettingsFromDBWork; Excessive failures attempting to retrieve manager settings from database " +
                          "for manager '" + managerName + "'";
                if (logConnectionErrors)
                    WriteErrorMsg(mErrMsg, allowLogToDB);
                dtSettings?.Dispose();
                return false;
            }

            // Verify at least one row returned
            if (dtSettings.Rows.Count < 1 && returnErrorIfNoParameters)
            {
                // No data was returned
                mErrMsg = "LoadMgrSettingsFromDBWork; Manager '" + managerName + "' not defined in the manager control database; using " + connectionString;
                if (logConnectionErrors)
                    WriteErrorMsg(mErrMsg);
                dtSettings?.Dispose();
                return false;
            }

            return true;
        }

        /// <summary>
        /// Update mParamDictionary using settings in file ManagerSettingsLocal.xml
        /// </summary>
        /// <returns></returns>
        private bool LoadLocalSettings()
        {
            var settings = ReadLocalSettingsFile();
            if (settings == null)
                return false;

            // Add/Update settings
            foreach (var setting in settings)
            {
                if (mParamDictionary.ContainsKey(setting.Key))
                {
                    mParamDictionary[setting.Key] = setting.Value;
                }
                else
                {
                    mParamDictionary.Add(setting.Key, setting.Value);
                }
            }

            // Validate several key local manager settings

            var taskQueuePath = GetParam(MGR_PARAM_LOCAL_TASK_QUEUE_PATH);
            if (string.IsNullOrWhiteSpace(taskQueuePath))
            {
                mErrMsg = "Manager parameter " + MGR_PARAM_LOCAL_TASK_QUEUE_PATH + " is missing from file " + LOCAL_MANAGER_SETTINGS_FILE;
                LogError(mErrMsg);
                return false;
            }

            var workDirPath = GetParam(MGR_PARAM_LOCAL_WORK_DIR_PATH);
            if (string.IsNullOrWhiteSpace(workDirPath))
            {
                mErrMsg = "Manager parameter " + MGR_PARAM_LOCAL_WORK_DIR_PATH + " is missing from file " + LOCAL_MANAGER_SETTINGS_FILE;
                LogError(mErrMsg);
                return false;
            }

            if (GetParam(MGR_PARAM_MGR_ACTIVE_LOCAL, false))
            {
                // MgrActive_Local is true, set MgrActive to true
                SetParam(MGR_PARAM_MGR_ACTIVE, "true");
            }

            if (string.IsNullOrWhiteSpace(GetParam(MGR_PARAM_WORK_DIR)))
            {
                // Define the work directory based on the manager name
                if (workDirPath.Contains("/"))
                    SetParam(MGR_PARAM_WORK_DIR, clsPathUtils.CombineLinuxPaths(workDirPath, ManagerName));
                else
                    SetParam(MGR_PARAM_WORK_DIR, Path.Combine(workDirPath, ManagerName));
            }

            return true;
        }

        /// <summary>
        /// Parse a list of XML nodes from AnalysisManagerProg.exe.config or ManagerSettingsLocal.xml
        /// </summary>
        /// <param name="settingNodes">XML nodes</param>
        /// <param name="traceEnabled">If true, display trace statements</param>
        /// <returns>Dictionary of settings</returns>
        public static Dictionary<string, string> ParseXMLSettings(IEnumerable settingNodes, bool traceEnabled)
        {

            var settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (XmlNode settingNode in settingNodes)
            {
                if (settingNode.Attributes == null)
                {
                    if (traceEnabled)
                        ShowTraceMessage(string.Format("Skipping setting node because no attributes: {0}", settingNode));
                    continue;
                }

                var settingName = settingNode.Attributes["name"].Value;

                var valueNode = settingNode.SelectSingleNode("value");
                if (valueNode == null)
                {
                    if (traceEnabled)
                        ShowTraceMessage(string.Format("Skipping setting node because no value node: <setting name=\"{0}\"/>", settingName));
                    continue;
                }

                var value = valueNode.InnerText;

                settings.Add(settingName, value);
            }

            return settings;
        }

        /// <summary>
        /// Read settings from file ManagerSettingsLocal.xml
        /// </summary>
        /// <returns></returns>
        private Dictionary<string, string> ReadLocalSettingsFile()
        {
            XmlDocument configDoc;

            try
            {
                // Construct the path to the config document
                var configFilePath = Path.Combine(mMgrFolderPath, LOCAL_MANAGER_SETTINGS_FILE);
                var configFile = new FileInfo(configFilePath);
                if (!configFile.Exists)
                {
                    mErrMsg = "ReadLocalSettingsFile; manager config file not found: " + configFilePath;
                    LogError(mErrMsg);
                    return null;
                }

                // Load the config document
                configDoc = new XmlDocument();
                configDoc.Load(configFilePath);
            }
            catch (Exception ex)
            {
                mErrMsg = "ReadLocalSettingsFile; exception loading settings file";
                LogError(mErrMsg, ex);
                return null;
            }

            try
            {
                // Retrieve the settings node
                var appSettingsNode = configDoc.SelectSingleNode("//settings");

                if (appSettingsNode == null)
                {
                    mErrMsg = "ReadLocalSettingsFile; settings node not found";
                    LogError(mErrMsg);
                    return null;
                }

                // Read each of the settings
                var settingNodes = appSettingsNode.SelectNodes("//setting[@name]");
                if (settingNodes == null)
                {
                    mErrMsg = "ReadLocalSettingsFile; settings/*/setting nodes not found";
                    LogError(mErrMsg);
                    return null;
                }

                return ParseXMLSettings(settingNodes, TraceMode);

            }
            catch (Exception ex)
            {
                mErrMsg = "ReadLocalSettingsFile; exception reading settings file";
                LogError(mErrMsg, ex);
                return null;
            }

        }

        /// <summary>
        /// Update mParamDictionary with settings in dtSettings, optionally skipping existing parameters
        /// </summary>
        /// <param name="dtSettings"></param>
        /// <param name="skipExistingParameters"></param>
        /// <param name="managerName"></param>
        /// <returns></returns>
        private bool StoreParameters(DataTable dtSettings, bool skipExistingParameters, string managerName)
        {
            bool success;

            try
            {
                foreach (DataRow currentRow in dtSettings.Rows)
                {
                    // Add the column heading and value to the dictionary
                    var paramKey = DbCStr(currentRow[dtSettings.Columns["ParameterName"]]);
                    var paramVal = DbCStr(currentRow[dtSettings.Columns["ParameterValue"]]);

                    if (mParamDictionary.ContainsKey(paramKey))
                    {
                        if (!skipExistingParameters)
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
                mErrMsg = "clsAnalysisMgrSettings.StoreParameters; Exception filling string dictionary from table for manager " +
                          "'" + managerName + "': " + ex.Message;
                WriteErrorMsg(mErrMsg);
                success = false;
            }
            finally
            {
                dtSettings?.Dispose();
            }

            return success;
        }

        /// <summary>
        /// Gets global settings from Broker DB (typically DMS_Pipeline)
        /// </summary>
        /// <returns>True for success; False for error</returns>
        /// <remarks></remarks>
        private bool LoadBrokerDBSettings()
        {
            // Retrieves global settings from the Broker DB. Performs retries if necessary.
            //
            // At present, the only settings being retrieved are the param file storage paths for each step tool
            // The storage path for each step tool will be stored in the manager settings dictionary
            // For example: the LCMSFeatureFinder step tool will have an entry with
            //   Name="StepTool_ParamFileStoragePath_LCMSFeatureFinder"
            //   Value="\\gigasax\dms_parameter_Files\LCMSFeatureFinder"

            short retryCount = 6;

            // Gigasax.DMS_Pipeline
            var connectionString = GetParam("BrokerConnectionString");

            ShowTrace("LoadBrokerDBSettings has BrokerConnectionString = " + connectionString);

            // Construct the Sql to obtain the information:
            //   SELECT 'StepTool_ParamFileStoragePath_' + Name AS ParameterName, [Param File Storage Path] AS ParameterValue
            //   FROM V_Pipeline_Step_Tools_Detail_Report
            //   WHERE ISNULL([Param File Storage Path], '') <> ''
            //
            const string sqlStr =
                " SELECT '" + clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "' + Name AS ParameterName, " +
                " [Param File Storage Path] AS ParameterValue" + " FROM V_Pipeline_Step_Tools_Detail_Report" +
                " WHERE ISNULL([Param File Storage Path], '') <> ''";

            ShowTrace("Query V_Pipeline_Step_Tools_Detail_Report in broker");

            // Get a table to hold the results of the query
            var success = clsGlobal.GetDataTableByQuery(sqlStr, connectionString, "LoadBrokerDBSettings", retryCount, out var dt);

            // If loop exited due to errors, return false
            if (!success)
            {
                var statusMessage = "clsAnalysisMgrSettings.LoadBrokerDBSettings; Excessive failures attempting to retrieve settings from broker database";
                WriteErrorMsg(statusMessage);
                dt.Dispose();
                return false;
            }

            // Verify at least one row returned
            if (dt.Rows.Count < 1)
            {
                // No data was returned
                var statusMessage = "clsAnalysisMgrSettings.LoadBrokerDBSettings; V_Pipeline_Step_Tools_Detail_Report returned no rows using " +
                                    connectionString;
                WriteErrorMsg(statusMessage);
                dt.Dispose();
                return false;
            }

            // Fill a string dictionary with the new parameters that have been found
            try
            {
                foreach (DataRow currentRow in dt.Rows)
                {
                    // Add the column heading and value to the dictionary
                    var paramKey = DbCStr(currentRow[dt.Columns["ParameterName"]]);
                    var paramVal = DbCStr(currentRow[dt.Columns["ParameterValue"]]);

                    SetParam(paramKey, paramVal);
                }
                return true;
            }
            catch (Exception ex)
            {
                var statusMessage = "clsAnalysisMgrSettings.LoadBrokerDBSettings; Exception filling string dictionary from table: " + ex.Message;
                WriteErrorMsg(statusMessage);
                return false;
            }
            finally
            {
                dt.Dispose();
            }

        }

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <returns>String value associated with specified key</returns>
        /// <remarks>Returns empty string if key isn't found</remarks>
        public string GetParam(string itemKey)
        {
            if (mParamDictionary == null)
                return string.Empty;

            if (!mParamDictionary.TryGetValue(itemKey, out var value))
                return string.Empty;

            return string.IsNullOrWhiteSpace(value) ? string.Empty : value;
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
            var value = GetParam(itemKey);
            if (string.IsNullOrEmpty(value))
            {
                return valueIfMissing;
            }

            return value;
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
        /// Show contents of a dictionary
        /// </summary>
        /// <param name="settings"></param>
        public static void ShowDictionaryTrace(IReadOnlyDictionary<string, string> settings)
        {
            Console.ForegroundColor = ConsoleMsgUtils.DebugFontColor;
            foreach (var key in from item in settings.Keys orderby item select item)
            {
                var value = settings[key];
                var keyWidth = Math.Max(30, Math.Ceiling(key.Length / 15.0) * 15);
                var formatString = "  {0,-" + keyWidth + "} {1}";
                Console.WriteLine(formatString, key, value);
            }
            Console.ResetColor();
        }

        /// <summary>
        /// Show a message at the console, preceded by a time stamp
        /// </summary>
        /// <param name="message"></param>
        private void ShowTrace(string message)
        {
            if (!TraceMode)
                return;

            ShowTraceMessage(message);
        }

        /// <summary>
        /// Show a message at the console, preceded with a timestamp
        /// </summary>
        /// <param name="message"></param>
        public static void ShowTraceMessage(string message)
        {
            clsMainProcess.ShowTraceMessage(DateTime.Now.ToString("hh:mm:ss.fff tt") + ": " + message);
        }


        private bool ValidateOfflineTaskDirectories()
        {
            // These manager parameters are defined in file ManagerSettingsLocal.xml
            try
            {
                var taskQueuePath = GetParam(MGR_PARAM_LOCAL_TASK_QUEUE_PATH);
                var taskQueue = new DirectoryInfo(taskQueuePath);
                if (!taskQueue.Exists)
                {
                    mErrMsg = "Local task queue directory not found: " + taskQueuePath;
                    LogError(mErrMsg);
                    return false;
                }

                var localWorkDirPath = GetParam(MGR_PARAM_LOCAL_WORK_DIR_PATH);
                var localWorkDir = new DirectoryInfo(localWorkDirPath);
                if (!localWorkDir.Exists)
                {
                    mErrMsg = "Working directory not found: " + localWorkDirPath;
                    LogError(mErrMsg);
                    return false;
                }

                var workDirPath = GetParam(MGR_PARAM_WORK_DIR);
                var workDir = new DirectoryInfo(workDirPath);
                if (workDir.Exists)
                    return true;

                LogMessage("Working directory not found, will try to create it: " + workDirPath);

                workDir.Create();
                workDir.Refresh();

                if (workDir.Exists)
                    return true;

                mErrMsg = "Working directory not found and unable to create it: " + workDirPath;
                LogError(mErrMsg);
                return false;
            }
            catch (Exception ex)
            {
                mErrMsg = "Exception validating the offline task directories";
                LogError(mErrMsg, ex);
                return false;
            }
        }

        /// <summary>
        /// Converts a database output object that could be dbNull to a string
        /// </summary>
        /// <param name="inpObj"></param>
        /// <returns>String equivalent of object; empty string if object is dbNull</returns>
        /// <remarks></remarks>
        private string DbCStr(object inpObj)
        {
            // If input object is DbNull, returns "", otherwise returns String representation of object
            if (ReferenceEquals(inpObj, DBNull.Value))
            {
                return string.Empty;
            }

            return Convert.ToString(inpObj);
        }

        /// <summary>
        /// Writes an error message to the application log and the database
        /// </summary>
        /// <param name="errorMessage">Message to write</param>
        /// <param name="allowLogToDB"></param>
        /// <remarks></remarks>
        private void WriteErrorMsg(string errorMessage, bool allowLogToDB = true)
        {
            var logToDb = !mMCParamsLoaded && allowLogToDB;
            LogError(errorMessage, logToDb);

            ShowTrace(errorMessage);
        }

        /// <summary>
        /// Writes specified value to an application config file.
        /// </summary>
        /// <param name="key">Name for parameter (case sensitive)</param>
        /// <param name="value">New value for parameter</param>
        /// <returns>TRUE for success; FALSE for error (ErrMsg property contains reason)</returns>
        /// <remarks>This bit of lunacy is needed because MS doesn't supply a means to write to an app config file</remarks>
        public bool WriteConfigSetting(string key, string value)
        {
            mErrMsg = string.Empty;

            // Load the config document
            var myDoc = LoadConfigDocument();
            if (myDoc == null)
            {
                // Error message has already been produced by LoadConfigDocument
                return false;
            }

            // Retrieve the settings node
            var myNode = myDoc.SelectSingleNode("// applicationSettings");

            if (myNode == null)
            {
                mErrMsg = "clsAnalysisMgrSettings.WriteConfigSettings; applicationSettings node not found";
                return false;
            }

            try
            {
                // Select the element containing the value for the specified key containing the key
                var myElement = (XmlElement)myNode.SelectSingleNode(string.Format("// setting[@name='{0}']/value", key));
                if (myElement != null)
                {
                    // Set key to specified value
                    myElement.InnerText = value;
                }
                else
                {
                    // Key was not found
                    mErrMsg = "clsAnalysisMgrSettings.WriteConfigSettings; specified key not found: " + key;
                    return false;
                }
                myDoc.Save(GetConfigFilePath());
                return true;
            }
            catch (Exception ex)
            {
                mErrMsg = "clsAnalysisMgrSettings.WriteConfigSettings; Exception updating settings file: " + ex.Message;
                return false;
            }
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
                var doc = new XmlDocument();
                doc.Load(GetConfigFilePath());
                return doc;
            }
            catch (Exception ex)
            {
                mErrMsg = "clsAnalysisMgrSettings.LoadConfigDocument; Exception loading settings file: " + ex.Message;
                return null;
            }
        }

        /// <summary>
        /// Specifies the full name and path for the application config file
        /// </summary>
        /// <returns>String containing full name and path</returns>
        private string GetConfigFilePath()
        {
            var configFilePath = PRISM.FileProcessor.ProcessFilesOrFoldersBase.GetAppPath() + ".config";
            return configFilePath;
        }

        #endregion
    }
}
