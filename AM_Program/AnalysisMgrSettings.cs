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
    public class AnalysisMgrSettings : MgrSettingsDB, IMgrParams
    {
        // ReSharper disable once CommentTypo
        // Ignore Spelling: ack, DMS, holdoff, mgractive, Prog, proteinseqs

        /// <summary>
        /// Stored procedure used to acknowledge that a manager update is required
        /// </summary>
        private const string SP_NAME_ACK_MANAGER_UPDATE = "ack_manager_update_required";

        private const string SP_NAME_PAUSE_MANAGER_TASK_REQUESTS = "pause_manager_task_requests";
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
        /// If Global.OfflineMode is true, equivalent to MgrActive_Local
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
        /// Manager parameter: local working directory path
        /// Used by managers running in Offline mode
        /// </summary>
        /// <remarks>
        /// Directory with JobX_StepY subdirectories containing files required for a single job step
        /// Each manager also has its own subdirectory for staging files
        /// </remarks>
        public const string MGR_PARAM_LOCAL_WORK_DIR_PATH = "LocalWorkDirPath";

        /// <summary>
        /// Manager parameter that specifies the date/time after which tasks can be requested
        /// </summary>
        public const string MGR_PARAM_TASK_REQUEST_ENABLE_TIME = "TaskRequestEnableTime";

        /// <summary>
        /// Working directory for the manager
        /// </summary>
        /// <remarks>When running offline jobs, this path will be updated for each job task</remarks>
        public const string MGR_PARAM_WORK_DIR = "WorkDir";

        private readonly string mMgrDirectoryPath;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>Call LoadSettings after instantiating this class</remarks>
        /// <param name="mgrDirectoryPath"></param>
        /// <param name="traceMode"></param>
        public AnalysisMgrSettings(
            string mgrDirectoryPath,
            bool traceMode)
        {
            mMgrDirectoryPath = mgrDirectoryPath;
            TraceMode = traceMode;
        }

        /// <summary>
        /// Calls stored procedure ack_manager_update_required to acknowledge that the manager has exited so that an update can be applied
        /// </summary>
        public void AckManagerUpdateRequired()
        {
            try
            {
                // SQL Server: Data Source=proteinseqs;Initial Catalog=manager_control
                // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
                var connectionString = GetParam(MGR_PARAM_MGR_CFG_DB_CONN_STRING);

                if (string.IsNullOrWhiteSpace(connectionString))
                {
                    if (Global.OfflineMode)
                        OnDebugEvent("Skipping call to " + SP_NAME_ACK_MANAGER_UPDATE + " since offline");
                    else
                        OnDebugEvent("Skipping call to " + SP_NAME_ACK_MANAGER_UPDATE + " since the Manager Control connection string is empty");

                    return;
                }

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, ManagerName);

                ShowTrace("AckManagerUpdateRequired using " + connectionStringToUse);

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
                RegisterEvents(dbTools);

                // Set up the command object prior to SP execution
                var cmd = dbTools.CreateCommand(SP_NAME_ACK_MANAGER_UPDATE, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@managerName", SqlType.VarChar, 128, ManagerName);
                dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, string.Empty, ParameterDirection.InputOutput);

                // Execute the SP
                var resCode = dbTools.ExecuteSP(cmd);

                if (resCode != 0)
                {
                    OnErrorEvent("ExecuteSP() reported result code {0} calling {1}",
                        resCode, SP_NAME_ACK_MANAGER_UPDATE);
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error calling " + SP_NAME_ACK_MANAGER_UPDATE, ex);
            }
        }

        /// <summary>
        /// Disable the manager by changing MgrActive_Local to false in AnalysisManagerProg.exe.config
        /// </summary>
        public bool DisableManagerLocally()
        {
            return WriteConfigSetting(MGR_PARAM_MGR_ACTIVE_LOCAL, "False");
        }

        /// <summary>
        /// Check for the existence of a job task parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <returns>True if the parameter is defined, false if not</returns>
        public bool HasParam(string name)
        {
            return MgrParams.ContainsKey(name);
        }

        /// <summary>
        /// Gets global settings from Broker DB (typically DMS_Pipeline)
        /// </summary>
        /// <param name="retryCount">Number of times to retry (in case of a problem)</param>
        /// <returns>True for success; false for error</returns>
        private bool LoadBrokerDBSettings(short retryCount = 6)
        {
            // Retrieves global settings from the Broker DB. Performs retries if necessary.

            // At present, the only settings being retrieved are the param file storage paths for each step tool
            // The storage path for each step tool will be stored in the manager settings dictionary
            // For example: the LCMSFeatureFinder step tool will have an entry with
            //   Name="StepTool_ParamFileStoragePath_LCMSFeatureFinder"
            //   Value="\\gigasax\dms_parameter_Files\LCMSFeatureFinder"

            // SQL Server: Data Source=Gigasax;Initial Catalog=DMS_Pipeline
            // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
            var connectionString = GetParam("BrokerConnectionString");

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, ManagerName);

            ShowTrace("LoadBrokerDBSettings has BrokerConnectionString = " + connectionStringToUse);

            // Lookup the storage path for each step tool

            const string sqlQuery =
                "SELECT step_tool, param_file_storage_path " +
                "FROM V_Pipeline_Step_Tool_Storage_Paths " +
                "WHERE param_file_storage_path <> ''";

            ShowTrace("Query V_Pipeline_Step_Tool_Storage_Paths in broker");

            // Query the database
            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
            RegisterEvents(dbTools);

            var success = dbTools.GetQueryResults(sqlQuery, out var queryResults, retryCount);

            // If loop exited due to errors, return false
            if (!success)
            {
                const string statusMessage = "AnalysisMgrSettings.LoadBrokerDBSettings; Excessive failures attempting to retrieve settings from broker database";
                ReportError(statusMessage, false);
                return false;
            }

            // Verify at least one row returned
            if (queryResults.Count < 1)
            {
                // No data was returned
                var statusMessage = string.Format(
                    "AnalysisMgrSettings.LoadBrokerDBSettings; V_Pipeline_Step_Tool_Storage_Paths returned no rows using {0}",
                    connectionStringToUse);

                ReportError(statusMessage, false);
                return false;
            }

            // Store the storage paths in the manager settings
            foreach (var item in queryResults)
            {
                var paramName = "Step_Tool_Param_File_Storage_Path_" + item[0];
                SetParam(paramName, item[1]);
            }

            return true;
        }

        /// <summary>
        /// Retrieves the manager and global settings from the Manager Control and Broker databases
        /// Or, if Global.OfflineMode is true, load settings from file ManagerSettingsLocal.xml
        /// </summary>
        public bool LoadDBSettings()
        {
            if (Global.OfflineMode)
            {
                var successLocal = LoadLocalSettings();

                if (!successLocal)
                    return false;

                return ValidateOfflineTaskDirectories();
            }

            // This method calls LoadMgrSettingsFromDBWork
            var success = LoadMgrSettingsFromDB(retryCount: 6);

            if (!success)
                return false;

            return LoadBrokerDBSettings();
        }

        /// <summary>
        /// Update MgrParams using settings in file ManagerSettingsLocal.xml
        /// </summary>
        private bool LoadLocalSettings()
        {
            var settings = ReadLocalSettingsFile();

            if (settings == null)
                return false;

            // Add/Update settings
            foreach (var setting in settings)
            {
                // Add/update the setting
                MgrParams[setting.Key] = setting.Value;
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
        /// Updates manager settings, then loads settings from the database or from ManagerSettingsLocal.xml if Global.OfflineMode is true
        /// </summary>
        /// <param name="configFileSettings">Manager settings loaded from file AppName.exe.config</param>
        /// <returns>True if success, false if an error</returns>
        public bool LoadSettings(Dictionary<string, string> configFileSettings)
        {
            var loadSettingsFromDB = !Global.OfflineMode;

            // This calls LoadMgrSettingsFromDBWork
            var success = LoadSettings(configFileSettings, loadSettingsFromDB);

            if (!success)
            {
                return false;
            }

            if (Global.OfflineMode)
            {
                var successLocal = LoadLocalSettings();

                if (!successLocal)
                    return false;

                return ValidateOfflineTaskDirectories();
            }

            // LoadSettings already loaded settings from Manager Control DB
            // Now load settings from the Broker DB (DMS_Pipeline)
            return LoadBrokerDBSettings();
        }

        /// <summary>
        /// Calls stored procedure pause_manager_task_requests to update manager parameter TaskRequestEnableTime
        /// </summary>
        /// <remarks>
        /// This will effectively put the manager to sleep, since it will not request new jobs
        /// until the date/time is later than TaskRequestEnableTime
        /// </remarks>
        /// <param name="holdoffIntervalMinutes">Holdoff interval, in minutes</param>
        public void PauseManagerTaskRequests(int holdoffIntervalMinutes = 30)
        {
            try
            {
                // SQL Server: Data Source=proteinseqs;Initial Catalog=manager_control
                // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
                var connectionString = GetParam(MGR_PARAM_MGR_CFG_DB_CONN_STRING);

                if (string.IsNullOrWhiteSpace(connectionString))
                {
                    if (Global.OfflineMode)
                        OnDebugEvent("Skipping call to " + SP_NAME_PAUSE_MANAGER_TASK_REQUESTS + " since offline");
                    else
                        OnDebugEvent("Skipping call to " + SP_NAME_PAUSE_MANAGER_TASK_REQUESTS + " since the Manager Control connection string is empty");

                    return;
                }

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, ManagerName);

                ShowTrace("Pause manager tasks using " + connectionStringToUse);

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
                RegisterEvents(dbTools);

                // Set up the command object prior to SP execution
                var cmd = dbTools.CreateCommand(SP_NAME_PAUSE_MANAGER_TASK_REQUESTS, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@managerName", SqlType.VarChar, 128, ManagerName);
                dbTools.AddParameter(cmd, "@holdoffIntervalMinutes", SqlType.Int).Value = holdoffIntervalMinutes;
                dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, string.Empty, ParameterDirection.InputOutput);

                // Execute the SP
                var resCode = dbTools.ExecuteSP(cmd);

                if (resCode != 0)
                {
                    OnErrorEvent("ExecuteSP() reported result code {0} calling {1}",
                        resCode, SP_NAME_PAUSE_MANAGER_TASK_REQUESTS);
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error calling " + SP_NAME_PAUSE_MANAGER_TASK_REQUESTS, ex);
            }
        }

        /// <summary>
        /// Read settings from file ManagerSettingsLocal.xml
        /// </summary>
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
        /// Writes specified value to an application config file
        /// </summary>
        /// <param name="key">Name for parameter (case-sensitive)</param>
        /// <param name="value">New value for parameter</param>
        /// <returns>True if success, false if an error (ErrMsg property contains reason)</returns>
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
                ErrMsg = "AnalysisMgrSettings.WriteConfigSettings; applicationSettings node not found";
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
                    ErrMsg = "AnalysisMgrSettings.WriteConfigSettings; specified key not found: " + key;
                    return false;
                }
                myDoc.Save(GetConfigFilePath());
                return true;
            }
            catch (Exception ex)
            {
                ErrMsg = "AnalysisMgrSettings.WriteConfigSettings; Exception updating settings file: " + ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Loads an application config file for changing parameters
        /// </summary>
        /// <returns>application config file as an XML document if successful; null if an error</returns>
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
                ErrMsg = "AnalysisMgrSettings.LoadConfigDocument; Exception loading settings file: " + ex.Message;
                return null;
            }
        }
    }
}
