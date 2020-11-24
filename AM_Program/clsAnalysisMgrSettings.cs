//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/18/2007
//
//*********************************************************************************************************

using AnalysisManagerBase;
using PRISM;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Xml;
using PRISMDatabaseUtils;
using PRISMDatabaseUtils.AppSettings;

namespace AnalysisManagerProg
{
    /// <summary>
    /// Class for loading, storing and accessing manager parameters.
    /// </summary>
    /// <remarks>
    /// Loads initial settings from local config file, then checks to see if remainder of settings should be loaded or manager set to inactive.
    /// If manager active, retrieves remainder of settings manager parameters database.
    /// </remarks>
    public class clsAnalysisMgrSettings : MgrSettingsDB, IMgrParams
    {
        // Ignore Spelling: ack

        #region "Constants"

        /// <summary>
        /// Stored procedure used to acknowledge that a manager update is required
        /// </summary>
        private const string SP_NAME_ACK_MANAGER_UPDATE = "AckManagerUpdateRequired";

        /// <summary>
        /// File with settings loaded when OfflineMode is enabled
        /// </summary>
        /// <remarks>Includes key settings that are retrieved from the Manager_Control database when OfflineMode is false</remarks>
        public const string LOCAL_MANAGER_SETTINGS_FILE = "ManagerSettingsLocal.xml";

        /// <summary>
        /// Manager parameter: manager active
        /// </summary>
        /// <remarks>
        /// Defined in the manager control database
        /// If clsGlobal.OfflineMode is true, equivalent to MgrActive_Local
        /// </remarks>
        public const string MGR_PARAM_MGR_ACTIVE = "mgractive";

        /// <summary>
        /// Connection string to DMS5
        /// </summary>
        public const string MGR_PARAM_DEFAULT_DMS_CONN_STRING = "DefaultDMSConnString";

        /// <summary>
        /// Manager parameter: failed results folder path
        /// </summary>
        /// <remarks>Directory where results from failed analysis tasks are stored</remarks>
        public const string MGR_PARAM_FAILED_RESULTS_DIRECTORY_PATH = "FailedResultsDirectoryPath";

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

        private readonly string mMgrDirectoryPath;

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrDirectoryPath"></param>
        /// <param name="traceMode"></param>
        /// <remarks>Call LoadSettings after instantiating this class</remarks>
        public clsAnalysisMgrSettings(
            string mgrDirectoryPath,
            bool traceMode)
        {
            mMgrDirectoryPath = mgrDirectoryPath;
            TraceMode = traceMode;
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
                        OnDebugEvent("Skipping call to " + SP_NAME_ACK_MANAGER_UPDATE + " since offline");
                    else
                        OnDebugEvent("Skipping call to " + SP_NAME_ACK_MANAGER_UPDATE + " since the Manager Control connection string is empty");

                    return;
                }

                ShowTrace("AckManagerUpdateRequired using " + connectionString);

                var dbTools = DbToolsFactory.GetDBTools(connectionString, debugMode: TraceMode);
                RegisterEvents(dbTools);

                // Set up the command object prior to SP execution
                var cmd = dbTools.CreateCommand(SP_NAME_ACK_MANAGER_UPDATE, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@managerName", SqlType.VarChar, 128, ManagerName);
                dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.Output);

                // Execute the SP
                dbTools.ExecuteSP(cmd);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception calling " + SP_NAME_ACK_MANAGER_UPDATE, ex);
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
            var loadSettingsFromDB = !clsGlobal.OfflineMode;

            var success = LoadSettings(configFileSettings, loadSettingsFromDB);
            if (!success)
            {
                return false;
            }

            if (clsGlobal.OfflineMode)
            {
                var successLocal = LoadLocalSettings();
                if (!successLocal)
                    return false;

                var foldersValidated = ValidateOfflineTaskDirectories();
                return foldersValidated;
            }

            // LoadSettings already loaded settings from Manager Control DB
            // Now load settings from the Broker DB (DMS_Pipeline)
            var brokerSuccess = LoadBrokerDBSettings();
            return brokerSuccess;
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

            var success = LoadMgrSettingsFromDB(retryCount: 6);

            if (!success)
                return false;

            var brokerSuccess = LoadBrokerDBSettings();
            return brokerSuccess;
        }

        /// <summary>
        /// Update MgrParams using settings in file ManagerSettingsLocal.xml
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
                if (MgrParams.ContainsKey(setting.Key))
                {
                    MgrParams[setting.Key] = setting.Value;
                }
                else
                {
                    MgrParams.Add(setting.Key, setting.Value);
                }
            }

            // Validate several key local manager settings

            var taskQueuePath = GetParam(MGR_PARAM_LOCAL_TASK_QUEUE_PATH);
            if (string.IsNullOrWhiteSpace(taskQueuePath))
            {
                ErrMsg = "Manager parameter " + MGR_PARAM_LOCAL_TASK_QUEUE_PATH + " is missing from file " + LOCAL_MANAGER_SETTINGS_FILE;
                ReportError(ErrMsg);
                return false;
            }

            var workDirPath = GetParam(MGR_PARAM_LOCAL_WORK_DIR_PATH);
            if (string.IsNullOrWhiteSpace(workDirPath))
            {
                ErrMsg = "Manager parameter " + MGR_PARAM_LOCAL_WORK_DIR_PATH + " is missing from file " + LOCAL_MANAGER_SETTINGS_FILE;
                ReportError(ErrMsg);
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
                    SetParam(MGR_PARAM_WORK_DIR, PathUtils.CombineLinuxPaths(workDirPath, ManagerName));
                else
                    SetParam(MGR_PARAM_WORK_DIR, Path.Combine(workDirPath, ManagerName));
            }

            return true;
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
                var configFilePath = Path.Combine(mMgrDirectoryPath, LOCAL_MANAGER_SETTINGS_FILE);
                var configFile = new FileInfo(configFilePath);
                if (!configFile.Exists)
                {
                    ErrMsg = "ReadLocalSettingsFile; manager config file not found: " + configFilePath;
                    ReportError(ErrMsg);
                    return null;
                }

                // Load the config document
                configDoc = new XmlDocument();
                configDoc.Load(configFilePath);
            }
            catch (Exception ex)
            {
                ErrMsg = "ReadLocalSettingsFile; exception loading settings file";
                ReportError(ErrMsg, ex);
                return null;
            }

            try
            {
                // Retrieve the settings node
                var appSettingsNode = configDoc.SelectSingleNode("//settings");

                if (appSettingsNode == null)
                {
                    ErrMsg = "ReadLocalSettingsFile; settings node not found";
                    ReportError(ErrMsg);
                    return null;
                }

                // Read each of the settings
                var settingNodes = appSettingsNode.SelectNodes("//setting[@name]");
                if (settingNodes == null)
                {
                    ErrMsg = "ReadLocalSettingsFile; settings/*/setting nodes not found";
                    ReportError(ErrMsg);
                    return null;
                }

                return ParseXMLSettings(settingNodes, TraceMode);
            }
            catch (Exception ex)
            {
                ErrMsg = "ReadLocalSettingsFile; exception reading settings file";
                ReportError(ErrMsg, ex);
                return null;
            }
        }

        /// <summary>
        /// Gets global settings from Broker DB (typically DMS_Pipeline)
        /// </summary>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <returns>True for success; False for error</returns>
        /// <remarks></remarks>
        private bool LoadBrokerDBSettings(short retryCount = 6)
        {
            // Retrieves global settings from the Broker DB. Performs retries if necessary.
            //
            // At present, the only settings being retrieved are the param file storage paths for each step tool
            // The storage path for each step tool will be stored in the manager settings dictionary
            // For example: the LCMSFeatureFinder step tool will have an entry with
            //   Name="StepTool_ParamFileStoragePath_LCMSFeatureFinder"
            //   Value="\\gigasax\dms_parameter_Files\LCMSFeatureFinder"

            // Gigasax.DMS_Pipeline
            var connectionString = GetParam("BrokerConnectionString");

            ShowTrace("LoadBrokerDBSettings has BrokerConnectionString = " + connectionString);

            // Construct the Sql to obtain the information:
            //   SELECT 'StepTool_ParamFileStoragePath_' + Name AS ParameterName, [Param File Storage Path] AS ParameterValue
            //   FROM V_Pipeline_Step_Tools_Detail_Report
            //   WHERE ISNULL([Param File Storage Path], '') <> ''
            //
            const string sqlQuery =
                " SELECT '" + clsGlobal.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "' + Name AS ParameterName, " +
                " [Param File Storage Path] AS ParameterValue" + " FROM V_Pipeline_Step_Tools_Detail_Report" +
                " WHERE ISNULL([Param File Storage Path], '') <> ''";

            ShowTrace("Query V_Pipeline_Step_Tools_Detail_Report in broker");

            // Query the database
            var dbTools = DbToolsFactory.GetDBTools(connectionString, debugMode: TraceMode);
            RegisterEvents(dbTools);

            var success = dbTools.GetQueryResults(sqlQuery, out var queryResults, retryCount);

            // If loop exited due to errors, return false
            if (!success)
            {
                var statusMessage = "clsAnalysisMgrSettings.LoadBrokerDBSettings; Excessive failures attempting to retrieve settings from broker database";
                ReportError(statusMessage, false);
                return false;
            }

            // Verify at least one row returned
            if (queryResults.Count < 1)
            {
                // No data was returned
                var statusMessage = "clsAnalysisMgrSettings.LoadBrokerDBSettings; V_Pipeline_Step_Tools_Detail_Report returned no rows using " +
                                    connectionString;
                ReportError(statusMessage, false);
                return false;
            }

            // Store the parameters
            foreach (var item in queryResults)
            {
                SetParam(item[0], item[1]);
            }

            return true;
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
                    ErrMsg = "Local task queue directory not found: " + taskQueuePath;
                    ReportError(ErrMsg);
                    return false;
                }

                var localWorkDirPath = GetParam(MGR_PARAM_LOCAL_WORK_DIR_PATH);
                var localWorkDir = new DirectoryInfo(localWorkDirPath);
                if (!localWorkDir.Exists)
                {
                    ErrMsg = "Working directory not found: " + localWorkDirPath;
                    ReportError(ErrMsg);
                    return false;
                }

                var workDirPath = GetParam(MGR_PARAM_WORK_DIR);
                var workDir = new DirectoryInfo(workDirPath);
                if (workDir.Exists)
                    return true;

                ReportError("Working directory not found, will try to create it: " + workDirPath);

                workDir.Create();
                workDir.Refresh();

                if (workDir.Exists)
                    return true;

                ErrMsg = "Working directory not found and unable to create it: " + workDirPath;
                ReportError(ErrMsg);
                return false;
            }
            catch (Exception ex)
            {
                ErrMsg = "Exception validating the offline task directories";
                ReportError(ErrMsg, ex);
                return false;
            }
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
            ErrMsg = string.Empty;

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
                ErrMsg = "clsAnalysisMgrSettings.WriteConfigSettings; applicationSettings node not found";
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
                    ErrMsg = "clsAnalysisMgrSettings.WriteConfigSettings; specified key not found: " + key;
                    return false;
                }
                myDoc.Save(GetConfigFilePath());
                return true;
            }
            catch (Exception ex)
            {
                ErrMsg = "clsAnalysisMgrSettings.WriteConfigSettings; Exception updating settings file: " + ex.Message;
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
                ErrMsg = "clsAnalysisMgrSettings.LoadConfigDocument; Exception loading settings file: " + ex.Message;
                return null;
            }
        }

        #endregion

    }
}
