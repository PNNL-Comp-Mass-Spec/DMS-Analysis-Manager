using System.Collections.Generic;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/18/2007
//*********************************************************************************************************

namespace AnalysisManagerBase
{
    #region "Enums"

    /// <summary>
    /// Job result codes
    /// </summary>
    /// <remarks></remarks>
    public enum CloseOutType
    {
        CLOSEOUT_SUCCESS = 0,
        CLOSEOUT_FAILED = 1,
        CLOSEOUT_NO_DTA_FILES = 2,
        CLOSEOUT_NO_OUT_FILES = 3,
        CLOSEOUT_NO_ANN_FILES = 5,
        CLOSEOUT_NO_FAS_FILES = 6,
        CLOSEOUT_NO_PARAM_FILE = 7,
        CLOSEOUT_NO_SETTINGS_FILE = 8,
        CLOSEOUT_NO_MODDEFS_FILE = 9,
        CLOSEOUT_NO_MASSCORRTAG_FILE = 10,
        CLOSEOUT_NO_XT_FILES = 12,
        CLOSEOUT_NO_INSP_FILES = 13,
        CLOSEOUT_FILE_NOT_FOUND = 14,
        CLOSEOUT_ERROR_ZIPPING_FILE = 15,
        CLOSEOUT_FILE_NOT_IN_CACHE = 16,
        CLOSEOUT_UNABLE_TO_USE_MZ_REFINERY = 17,
        CLOSEOUT_NO_DATA = 20
    }

    #endregion

    /// <summary>
    /// Interface for the analysis job parameter storage class
    /// Also has the methods for Requesting a task and Closing a task
    /// </summary>
    /// <remarks>Implemented in clsAnalysisJob</remarks>
    public interface IJobParams
    {

        #region "Properties"

        Dictionary<string, int> DatasetInfoList { get; }
        SortedSet<string> ResultFilesToKeep { get; }
        SortedSet<string> ResultFilesToSkip { get; }
        SortedSet<string> ResultFileExtensionsToSkip { get; }

        SortedSet<string> ServerFilesToDelete { get; }
        #endregion

        #region "Methods"

        /// <summary>
        /// Adds (or updates) a job parameter
        /// </summary>
        /// <param name="ParamSection">Section name for parameter</param>
        /// <param name="ParamName">Name of parameter</param>
        /// <param name="ParamValue">Value for parameter</param>
        /// <returns>True if success, False if an error</returns>
        /// <remarks></remarks>
        bool AddAdditionalParameter(string ParamSection, string ParamName, string ParamValue);

        /// <summary>
        /// Adds (or updates) a job parameter
        /// </summary>
        /// <param name="ParamSection">Section name for parameter</param>
        /// <param name="ParamName">Name of parameter</param>
        /// <param name="ParamValue">Boolean alue for parameter</param>
        /// <returns>True if success, False if an error</returns>
        /// <remarks></remarks>
        bool AddAdditionalParameter(string ParamSection, string ParamName, bool ParamValue);

        /// <summary>
        /// Add new dataset name and ID to DatasetInfoList
        /// </summary>
        /// <param name="DatasetName"></param>
        /// <param name="DatasetID"></param>
        /// <remarks></remarks>
        void AddDatasetInfo(string DatasetName, int DatasetID);
        
        /// <summary>
        /// Add a filename to definitely move to the results folder
        /// </summary>
        /// <param name="FileName"></param>
        /// <remarks>FileName can be a file path; only the filename will be stored in m_ResultFilesToKeep</remarks>
        void AddResultFileToKeep(string FileName);
        
        /// <summary>
        /// Add a file to be deleted from the storage server (requires full file path)
        /// </summary>
        /// <param name="FilePath">Full path to the file</param>
        /// <remarks>To delete the files, call clsAnalysisToolRunnerBase.RemoveNonResultServerFiles</remarks>
        void AddServerFileToDelete(string FilePath);
        
        /// <summary>
        /// Add a filename to not move to the results folder
        /// </summary>
        /// <param name="FileName"></param>
        /// <remarks>FileName can be a file path; only the filename will be stored in m_ResultFilesToSkip</remarks>
        void AddResultFileToSkip(string FileName);
        
        /// <summary>
        /// Add a filename extension to not move to the results folder
        /// </summary>
        /// <param name="Extension"></param>
        /// <remarks>Can be a file extension (like .raw) or even a partial file name like _peaks.txt</remarks>
        void AddResultFileExtensionToSkip(string Extension);
        
        /// <summary>
        /// Contact the Pipeline database to close the analysis job
        /// </summary>
        /// <param name="CloseOut"></param>
        /// <param name="CompMsg"></param>
        /// <remarks>Implemented in clsAnalysisJob</remarks>
        void CloseTask(CloseOutType CloseOut, string CompMsg);

        /// <summary>
        /// Contact the Pipeline database to close the analysis job
        /// </summary>
        /// <param name="CloseOut"></param>
        /// <param name="CompMsg"></param>
        /// <param name="EvalCode"></param>
        /// <param name="EvalMessage"></param>
        /// <remarks>Implemented in clsAnalysisJob</remarks>
        void CloseTask(CloseOutType CloseOut, string CompMsg, int EvalCode, string EvalMessage);

        /// <summary>
        /// Uses the "ToolName" and "StepTool" entries in m_JobParamsTable to generate the tool name for the current analysis job
        /// Example tool names are "Sequest" or "DTA_Gen (Sequest)" or "DataExtractor (XTandem)"
        /// </summary>
        /// <returns>Tool name</returns>
        /// <remarks></remarks>
        string GetCurrentJobToolDescription();

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section)
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <returns>Value for specified parameter; empty string if not found</returns>
        /// <remarks></remarks>
        string GetParam(string name);

        /// <summary>
        /// Gets a job parameter with the given name, preferentially using the specified parameter section
        /// </summary>
        /// <param name="section">Section name for parameter</param>
        /// <param name="name">Key name for parameter</param>
        /// <returns>Value for specified parameter; empty string if not found</returns>
        /// <remarks></remarks>
        string GetParam(string section, string name);

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section)
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing"></param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        /// <remarks>
        /// If the value associated with the parameter is found, yet is not True or False, an exception will be occur; 
        /// the calling procedure must handle this exception
        /// </remarks>
        bool GetJobParameter(string name, bool valueIfMissing);

        string GetJobParameter(string name, string valueIfMissing);

        int GetJobParameter(string name, int valueIfMissing);

        short GetJobParameter(string name, short valueIfMissing);

        float GetJobParameter(string name, float valueIfMissing);


        /// <summary>
        /// Gets a job parameter with the given name, preferentially using the specified parameter section
        /// </summary>
        /// <param name="section">Section name for parameter</param>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        bool GetJobParameter(string section, string name, bool valueIfMissing);

        string GetJobParameter(string section, string name, string valueIfMissing);

        int GetJobParameter(string section, string name, int valueIfMissing);

        float GetJobParameter(string section, string name, float valueIfMissing);

        /// <summary>
        /// Remove a filename that was previously added to ResultFilesToSkip
        /// </summary>
        /// <param name="FileName"></param>
        /// <remarks></remarks>
        void RemoveResultFileToSkip(string FileName);
        
        /// <summary>
        /// Requests a task from the database
        /// </summary>
        /// <returns>Enum indicating if task was found</returns>
        /// <remarks></remarks>
        clsDBTask.RequestTaskResult RequestTask();

        /// <summary>
        /// Add/updates the value for the given parameter (searches all sections)
        /// </summary>
        /// <param name="keyName">Parameter name</param>
        /// <param name="value">Parameter value</param>
        /// <remarks></remarks>
        void SetParam(string keyName, string value);

        /// <summary>
        /// Add/updates the value for the given parameter
        /// </summary>
        /// <param name="section">Section name</param>
        /// <param name="keyName">Parameter name</param>
        /// <param name="value">Parameter value</param>
        /// <remarks></remarks>
        void SetParam(string section, string keyName, string value);

        #endregion

    }
}