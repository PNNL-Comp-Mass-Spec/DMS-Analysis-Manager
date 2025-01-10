using System.Collections.Generic;
using AnalysisManagerBase.AnalysisTool;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/18/2007
//*********************************************************************************************************

// ReSharper disable UnusedMemberInSuper.Global
// ReSharper disable UnusedMember.Global

namespace AnalysisManagerBase.JobConfig
{
    // Ignore Spelling: ann, bool, Quant, Sequest

    /// <summary>
    /// Job result codes (aka completion codes)
    /// </summary>
    /// <remarks>Sent to parameter completionCode when calling set_step_task_complete</remarks>
    public enum CloseOutType
    {
        /// <summary>
        /// Success
        /// </summary>
        CLOSEOUT_SUCCESS = 0,

        /// <summary>
        /// Failed
        /// </summary>
        CLOSEOUT_FAILED = 1,

        /// <summary>
        /// No DTA files
        /// </summary>
        CLOSEOUT_NO_DTA_FILES = 2,

        /// <summary>
        /// No out files
        /// </summary>
        CLOSEOUT_NO_OUT_FILES = 3,

        /// <summary>
        /// FASTA file not defined or not found
        /// </summary>
        CLOSEOUT_NO_FAS_FILES = 6,

        /// <summary>
        /// No parameter file (either not defined or not found)
        /// </summary>
        CLOSEOUT_NO_PARAM_FILE = 7,

        /// <summary>
        /// No settings file
        /// </summary>
        CLOSEOUT_NO_SETTINGS_FILE = 8,

        /// <summary>
        /// No X!Tandem files
        /// </summary>
        CLOSEOUT_NO_XT_FILES = 12,

        /// <summary>
        /// No inspect files
        /// </summary>
        CLOSEOUT_NO_INSPECT_FILES = 13,

        /// <summary>
        /// File not found or file verification failed
        /// </summary>
        CLOSEOUT_FILE_NOT_FOUND = 14,

        /// <summary>
        /// Error zipping a file
        /// </summary>
        CLOSEOUT_ERROR_ZIPPING_FILE = 15,

        /// <summary>
        /// File not found in cache
        /// </summary>
        CLOSEOUT_FILE_NOT_IN_CACHE = 16,

        /// <summary>
        /// Unable to use MZ Refinery
        /// </summary>
        CLOSEOUT_UNABLE_TO_USE_MZ_REFINERY = 17,

        /// <summary>
        /// Skipped running MZ Refinery because parameter MzRefParamFile has "SkipAll"
        /// </summary>
        CLOSEOUT_SKIPPED_MZ_REFINERY = 18,

        /// <summary>
        /// No data (no results)
        /// </summary>
        CLOSEOUT_NO_DATA = 20,

        /// <summary>
        /// Skipped MSXmlGen since job parameter MSXMLGenerator is "skip"
        /// </summary>
        CLOSEOUT_SKIPPED_MSXML_GEN = 21,

        /// <summary>
        /// Skipped MaxQuant because a previous job step should have already run it to completion
        /// </summary>
        CLOSEOUT_SKIPPED_MAXQUANT = 22,

        /// <summary>
        /// There is a problem with this computer, e.g. not enough free disk space (prior to January 2025, also used for not enough free memory to run the job)
        /// Since there is nothing wrong with the job itself, set the job step's state to Enabled
        /// </summary>
        CLOSEOUT_RESET_JOB_STEP = 23,

        /// <summary>
        /// The computer does not have enough free memory to run the job
        /// Since there is nothing wrong with the job itself, set the job step's state to Enabled
        /// </summary>
        /// <remarks>
        /// If a job step fails with this error over 35 times, procedure set_step_task_complete will set the job step's state to Failed,
        /// since it's possible the required memory is too large for any of the processing nodes
        /// </remarks>
        CLOSEOUT_RESET_JOB_STEP_INSUFFICIENT_MEMORY = 24,

        /// <summary>
        /// Job is running remote
        /// </summary>
        CLOSEOUT_RUNNING_REMOTE = 25,

        /// <summary>
        /// Job failed while running remote
        /// </summary>
        CLOSEOUT_FAILED_REMOTE = 26,

        /// <summary>
        /// Skipped using DIA-NN to create a spectral library using an in-silico digest of a FASTA file
        /// </summary>
        CLOSEOUT_SKIPPED_DIA_NN_SPEC_LIB = 27,

        /// <summary>
        /// Waiting for another job to create an in-silico digest of a FASTA file
        /// </summary>
        CLOSEOUT_WAITING_FOR_DIA_NN_SPEC_LIB = 28
    }

    /// <summary>
    /// Interface for the analysis job parameter storage class
    /// Also has the methods for Requesting a task and Closing a task
    /// </summary>
    /// <remarks>Implemented in AnalysisJob</remarks>
    public interface IJobParams
    {
        /// <summary>
        /// Dataset info list
        /// </summary>
        Dictionary<string, int> DatasetInfoList { get; }

        /// <summary>
        /// Result files to keep
        /// </summary>
        SortedSet<string> ResultFilesToKeep { get; }

        /// <summary>
        /// Result files to skip
        /// </summary>
        SortedSet<string> ResultFilesToSkip { get; }

        /// <summary>
        /// Result file extensions to keep
        /// </summary>
        SortedSet<string> ResultFileExtensionsToSkip { get; }

        /// <summary>
        /// Server files to delete
        /// </summary>
        SortedSet<string> ServerFilesToDelete { get; }

        /// <summary>
        /// Flag set to true when .CloseTask is called
        /// </summary>
        bool TaskClosed { get; set; }

        /// <summary>
        /// Adds (or updates) a job parameter
        /// </summary>
        /// <remarks>
        /// The section name is typically "JobParameters" or "StepParameters"
        /// See constants JOB_PARAMETERS_SECTION and STEP_PARAMETERS_SECTION
        /// </remarks>
        /// <param name="sectionName">Section name for parameter</param>
        /// <param name="paramName">Name of parameter</param>
        /// <param name="paramValue">Value for parameter</param>
        /// <returns>True if success, false if an error</returns>
        bool AddAdditionalParameter(string sectionName, string paramName, string paramValue);

        /// <summary>
        /// Adds (or updates) a job parameter
        /// </summary>
        /// <param name="sectionName">Section name for parameter</param>
        /// <param name="paramName">Name of parameter</param>
        /// <param name="paramValue">Boolean value for parameter</param>
        /// <returns>True if success, false if an error</returns>
        bool AddAdditionalParameter(string sectionName, string paramName, bool paramValue);

        /// <summary>
        /// Adds (or updates) a job parameter
        /// </summary>
        /// <param name="sectionName">Section name for parameter</param>
        /// <param name="paramName">Name of parameter</param>
        /// <param name="paramValue">Integer value for parameter</param>
        /// <returns>True if success, false if an error</returns>
        bool AddAdditionalParameter(string sectionName, string paramName, int paramValue);

        /// <summary>
        /// Add new dataset name and ID to DatasetInfoList
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="datasetID">Dataset ID</param>
        void AddDatasetInfo(string datasetName, int datasetID);

        /// <summary>
        /// Add a filename to definitely move to the results directory
        /// </summary>
        /// <remarks>fileName can be a file path; only the filename will be stored in ResultFilesToKeep</remarks>
        /// <param name="fileName">File name</param>
        void AddResultFileToKeep(string fileName);

        /// <summary>
        /// Add a file to be deleted from the storage server (requires full file path)
        /// </summary>
        /// <remarks>To delete the files, call AnalysisToolRunnerBase.RemoveNonResultServerFiles</remarks>
        /// <param name="filePath">Full path to the file</param>
        void AddServerFileToDelete(string filePath);

        /// <summary>
        /// Add a filename to not move to the results directory
        /// </summary>
        /// <remarks>
        /// <para>
        /// FileName can be a full path to a file; only the filename will be stored in ResultFilesToSkip
        /// </para>
        /// <para>
        /// For partial file names, use <a cref="AddResultFileExtensionToSkip">AddResultFileExtensionToSkip</a>
        /// </para>
        /// </remarks>
        /// <param name="fileName">File name</param>
        void AddResultFileToSkip(string fileName);

        /// <summary>
        /// Add a filename extension to not move to the results directory
        /// </summary>
        /// <remarks>Can be a file extension (like .raw) or even a partial file name like _peaks.txt</remarks>
        /// <param name="fileExtension">File extension</param>
        void AddResultFileExtensionToSkip(string fileExtension);

        /// <summary>
        /// Contact the Pipeline database to close the analysis job
        /// </summary>
        /// <remarks>Implemented in AnalysisJob</remarks>
        /// <param name="closeOut">Closeout code</param>
        /// <param name="compMsg">Closeout message</param>
        void CloseTask(CloseOutType closeOut, string compMsg);

        /// <summary>
        /// Contact the Pipeline database to close the analysis job
        /// </summary>
        /// <remarks>Implemented in AnalysisJob</remarks>
        /// <param name="closeOut">Closeout code</param>
        /// <param name="compMsg">Closeout message</param>
        /// <param name="toolRunner">ToolRunner instance</param>
        void CloseTask(CloseOutType closeOut, string compMsg, IToolRunner toolRunner);

        /// <summary>
        /// Get all job parameters for the given section
        /// </summary>
        /// <returns>Dictionary where keys are parameter names and values are parameter values</returns>
        Dictionary<string, string> GetAllParametersForSection(string sectionName);

        /// <summary>
        /// Get job parameter section names
        /// </summary>
        List<string> GetAllSectionNames();

        /// <summary>
        /// Get a description of the step tool and script name
        /// </summary>
        /// <returns>Tool name, e.g. "Sequest" or "DTA_Gen (Sequest)" or "DataExtractor (XTandem)"</returns>
        string GetCurrentJobToolDescription();

        /// <summary>
        /// Get a description of the current job number and step number
        /// </summary>
        /// <returns>String in the form "job x, step y"</returns>
        string GetJobStepDescription();

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section); names are not case-sensitive
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <returns>Value for specified parameter; empty string if not found</returns>
        string GetParam(string name);

        /// <summary>
        /// Gets a job parameter with the given name, preferentially using the specified parameter section; names are not case-sensitive
        /// </summary>
        /// <param name="section">Section name for parameter</param>
        /// <param name="name">Key name for parameter</param>
        /// <returns>Value for specified parameter; empty string if not found</returns>
        string GetParam(string section, string name);

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section); names are not case-sensitive
        /// </summary>
        /// <remarks>
        /// If the value associated with the parameter is found, yet is not true or false, an exception will occur;
        /// the calling procedure must handle this exception
        /// </remarks>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found (bool)</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        bool GetJobParameter(string name, bool valueIfMissing);

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section); names are not case-sensitive
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found (string)</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        string GetJobParameter(string name, string valueIfMissing);

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section); names are not case-sensitive
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found (int)</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        int GetJobParameter(string name, int valueIfMissing);

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section); names are not case-sensitive
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found (short)</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        short GetJobParameter(string name, short valueIfMissing);

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section); names are not case-sensitive
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found (float)</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        float GetJobParameter(string name, float valueIfMissing);

        /// <summary>
        /// Gets a job parameter with the given name, preferentially using the specified parameter section; names are not case-sensitive
        /// </summary>
        /// <param name="section">Section name for parameter</param>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found (bool)</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        bool GetJobParameter(string section, string name, bool valueIfMissing);

        /// <summary>
        /// Gets a job parameter with the given name, preferentially using the specified parameter section; names are not case-sensitive
        /// </summary>
        /// <param name="section">Section name for parameter</param>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found (string)</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        string GetJobParameter(string section, string name, string valueIfMissing);

        /// <summary>
        /// Gets a job parameter with the given name, preferentially using the specified parameter section; names are not case-sensitive
        /// </summary>
        /// <param name="section">Section name for parameter</param>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found (int)</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        int GetJobParameter(string section, string name, int valueIfMissing);

        /// <summary>
        /// Gets a job parameter with the given name, preferentially using the specified parameter section; names are not case-sensitive
        /// </summary>
        /// <param name="section">Section name for parameter</param>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found (float)</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        float GetJobParameter(string section, string name, float valueIfMissing);

        /// <summary>
        /// Remove a filename that was previously added to ResultFilesToSkip
        /// </summary>
        /// <param name="fileName">File name</param>
        void RemoveResultFileToSkip(string fileName);

        /// <summary>
        /// Requests a task from the database
        /// </summary>
        /// <returns>Enum indicating if task was found</returns>
        DBTask.RequestTaskResult RequestTask();

        /// <summary>
        /// Add/updates the value for the given parameter (searches all sections)
        /// </summary>
        /// <param name="keyName">Parameter name</param>
        /// <param name="value">Parameter value</param>
        void SetParam(string keyName, string value);

        /// <summary>
        /// Add/updates the value for the given parameter
        /// </summary>
        /// <param name="section">Section name</param>
        /// <param name="keyName">Parameter name</param>
        /// <param name="value">Parameter value</param>
        void SetParam(string section, string keyName, string value);

        /// <summary>
        /// Attempts to retrieve the specified parameter (looks in all parameter sections)
        /// </summary>
        /// <param name="paramName">Parameter Name</param>
        /// <param name="paramValue">Output: parameter value</param>
        /// <returns>True if success, false if not found</returns>
        public bool TryGetParam(string paramName, out string paramValue);

        /// <summary>
        /// Attempts to retrieve the specified parameter in the specified parameter section
        /// </summary>
        /// <param name="section">Section Name</param>
        /// <param name="paramName">Parameter Name</param>
        /// <param name="paramValue">Output: parameter value</param>
        /// <returns>True if success, false if not found</returns>
        public bool TryGetParam(string section, string paramName, out string paramValue);

        /// <summary>
        /// Attempts to retrieve the specified parameter in the specified parameter section
        /// </summary>
        /// <param name="section">Section Name</param>
        /// <param name="paramName">Parameter Name</param>
        /// <param name="paramValue">Output: parameter value</param>
        /// <param name="searchAllSectionsIfNotFound">If true, searches other sections for the parameter if not found in the specified section</param>
        /// <returns>True if success, false if not found</returns>
        public bool TryGetParam(string section, string paramName, out string paramValue, bool searchAllSectionsIfNotFound);
    }
}
