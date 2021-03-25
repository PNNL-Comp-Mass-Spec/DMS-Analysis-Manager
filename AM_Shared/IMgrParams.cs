using System.Collections.Generic;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 12/18/2007
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{
    /// <summary>
    /// Interface for manager params storage class
    /// </summary>
    public interface IMgrParams
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: PostgreSQL, pgpass, Ack

        // ReSharper restore CommentTypo

        #region "Properties"

        /// <summary>
        /// Error message
        /// </summary>
        string ErrMsg { get; }

        /// <summary>
        /// Manager name
        /// </summary>
        string ManagerName { get; }

        /// <summary>
        /// Dictionary of manager parameters
        /// </summary>
        Dictionary<string, string> MgrParams { get; }

        /// <summary>
        /// True when TraceMode has been enabled at the command line via /trace
        /// </summary>
        bool TraceMode { get; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Calls stored procedure AckManagerUpdateRequired in the Manager Control DB
        /// </summary>
        void AckManagerUpdateRequired();

        /// <summary>
        /// Disable the manager by changing MgrActive_Local to False in AnalysisManagerProg.exe.config
        /// </summary>
        bool DisableManagerLocally();

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary (case insensitive keys)
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <returns>String value associated with specified key</returns>
        /// <remarks>Returns empty string if key isn't found</remarks>
        string GetParam(string itemKey);

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary (case insensitive keys)
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        string GetParam(string itemKey, string valueIfMissing);

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary (case insensitive keys)
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        bool GetParam(string itemKey, bool valueIfMissing);

        /// <summary>
        /// Gets a parameter from the manager parameters dictionary (case insensitive keys)
        /// </summary>
        /// <param name="itemKey">Key name for item</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        int GetParam(string itemKey, int valueIfMissing);

        /// <summary>
        /// Check for the existence of a job task parameter (case insensitive parameter names)
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <returns>True if the parameter is defined, false if not</returns>
        bool HasParam(string name);

        /// <summary>
        /// Sets a parameter in the parameters string dictionary
        /// </summary>
        /// <param name="itemKey">Key name for the item</param>
        /// <param name="itemValue">Value to assign to the key</param>
        void SetParam(string itemKey, string itemValue);

        /// <summary>
        /// Retrieves the manager and global settings from various databases
        /// </summary>
        /// <returns>True if successful; False on error</returns>
        bool LoadDBSettings();

        /// <summary>
        /// Read settings from file AppName.exe.config
        /// </summary>
        /// <returns>Dictionary of settings as key/value pairs; null on error</returns>
        Dictionary<string, string> LoadMgrSettingsFromFile(string configFilePath);

        /// <summary>
        /// Updates manager settings, then loads settings from the database or from ManagerSettingsLocal.xml if Global.OfflineMode is true
        /// </summary>
        /// <param name="configFileSettings">Manager settings loaded from file AnalysisManagerProg.exe.config</param>
        /// <returns>True if successful; False on error</returns>
        bool LoadSettings(Dictionary<string, string> configFileSettings);

        /// <summary>
        /// Examine configFileSettings to look for parameters MgrCnfgDbConnectStr and/or DefaultDMSConnString
        /// If defined, and if pointing to a PostgreSQL server, look for a pgpass file for the current user
        /// </summary>
        /// <param name="configFileSettings"></param>
        void ValidatePgPass(IReadOnlyDictionary<string, string> configFileSettings);

        #endregion
    }
}