using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.OfflineJobs;
using PRISM;
using PRISM.Logging;
using PRISMDatabaseUtils;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/18/2007
//
//*********************************************************************************************************

namespace AnalysisManagerBase.JobConfig
{
    /// <summary>
    /// Provides DB access and tools for one analysis job
    /// </summary>
    public class AnalysisJob : DBTask, IJobParams
    {
        // Ignore Spelling: dir, dirs, lvl, ok

        /// <summary>
        /// Job parameters section
        /// </summary>
        public const string JOB_PARAMETERS_SECTION = "JobParameters";

        /// <summary>
        /// Peptide search section
        /// </summary>
        public const string PEPTIDE_SEARCH_SECTION = "PeptideSearch";

        /// <summary>
        /// Step parameters section
        /// </summary>
        public const string STEP_PARAMETERS_SECTION = "StepParameters";

        /// <summary>
        /// Stored procedure to call once the analysis finishes
        /// </summary>
        protected const string SP_NAME_SET_COMPLETE = "set_step_task_complete";

        /// <summary>
        /// Stored procedure the manager calls to indicate that a deadlock occurred, and this manager was not assigned a job
        /// </summary>
        private const string SP_NAME_REPORT_IDLE = "report_manager_idle";

        /// <summary>
        /// "request_step_task"
        /// </summary>
        protected const string SP_NAME_REQUEST_TASK = "request_step_task_xml";

        /// <summary>
        /// XML file with job parameters used when running job remotely
        /// </summary>
        public const string OFFLINE_JOB_PARAMS_FILE = "JobParams.xml";

        /// <summary>
        /// Job parameters
        /// The outer dictionary tracks section names; the inner dictionary tracks key/value pairs within each section
        /// </summary>
        protected readonly Dictionary<string, Dictionary<string, string>> mJobParams;

        /// <summary>
        /// Current job number
        /// </summary>
        protected int mJobId;

        /// <summary>
        /// List of file names to NOT move to the results directory; this list is used by MoveResultFiles()
        /// </summary>
        protected SortedSet<string> mResultFilesToSkip = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// List of file extensions (or even partial file names like _peaks.txt) to NOT move to the results directory
        /// </summary>
        /// <remarks>
        /// Comparison checks if the end of the fileName matches any entry ResultFileExtensionsToSkip:
        /// if (tmpFileNameLCase.EndsWith(ext, StringComparison.OrdinalIgnoreCase)) { okToMove = false; }
        /// </remarks>
        protected SortedSet<string> mResultFileExtensionsToSkip = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// List of file names that WILL be moved to the results directory, even if they are in ResultFilesToSkip or ResultFileExtensionsToSkip
        /// </summary>
        protected SortedSet<string> mResultFilesToKeep = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// List of file path to delete from the storage server (must be full file paths)
        /// </summary>
        protected SortedSet<string> mServerFilesToDelete = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// List of dataset names and dataset IDs
        /// </summary>
        protected Dictionary<string, int> mDatasetInfoList = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Offline job info file
        /// </summary>
        protected FileInfo mOfflineJobInfoFile;

        private DateTime mStartTime;

        /// <summary>
        /// List of dataset names and dataset IDs associated with this aggregation job
        /// </summary>
        public Dictionary<string, int> DatasetInfoList => mDatasetInfoList;

        /// <summary>
        /// List of file names that WILL be moved to the results directory, even if they are in ResultFilesToSkip or ResultFileExtensionsToSkip
        /// </summary>
        public SortedSet<string> ResultFilesToKeep => mResultFilesToKeep;

        /// <summary>
        /// List of file names to NOT move to the results directory
        /// </summary>
        public SortedSet<string> ResultFilesToSkip => mResultFilesToSkip;

        /// <summary>
        /// List of file extensions to NOT move to the results directory; comparison checks if the end of the fileName matches any entry in ResultFileExtensionsToSkip
        /// </summary>
        public SortedSet<string> ResultFileExtensionsToSkip => mResultFileExtensionsToSkip;

        /// <summary>
        /// List of file paths to remove from the storage server (full file paths)
        /// </summary>
        /// <remarks>Used by AnalysisToolRunnerBase.RemoveNonResultServerFiles</remarks>
        public SortedSet<string> ServerFilesToDelete => mServerFilesToDelete;

        /// <summary>
        /// Flag set to true when .CloseTask is called
        /// </summary>
        public bool TaskClosed { get; set; }

        /// <summary>
        /// When true, show additional messages at the console
        /// </summary>
        public bool TraceMode { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">IMgrParams object containing manager parameters</param>
        /// <param name="debugLvl">Debug level</param>
        public AnalysisJob(IMgrParams mgrParams, short debugLvl) : base(mgrParams, debugLvl)
        {
            mJobParams = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
            Reset();
        }

        /// <summary>
        /// Adds (or updates) a job parameter
        /// </summary>
        /// <param name="sectionName">Section name for parameter</param>
        /// <param name="paramName">Name of parameter</param>
        /// <param name="paramValue">Boolean value for parameter</param>
        /// <returns>True if success, false if an error</returns>
        public bool AddAdditionalParameter(string sectionName, string paramName, bool paramValue)
        {
            try
            {
                SetParam(sectionName, paramName, paramValue.ToString());
                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception adding parameter: " + paramName + " Value: " + paramValue, ex);
                return false;
            }
        }

        /// <summary>
        /// Adds (or updates) a job parameter
        /// </summary>
        /// <param name="sectionName">Section name for parameter</param>
        /// <param name="paramName">Name of parameter</param>
        /// <param name="paramValue">Integer value for parameter</param>
        /// <returns>True if success, false if an error</returns>
        public bool AddAdditionalParameter(string sectionName, string paramName, int paramValue)
        {
            try
            {
                SetParam(sectionName, paramName, paramValue.ToString());
                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception adding parameter: " + paramName + " Value: " + paramValue, ex);
                return false;
            }
        }

        /// <summary>
        /// Adds (or updates) a job parameter
        /// </summary>
        /// <param name="sectionName">Section name for parameter</param>
        /// <param name="paramName">Name of parameter</param>
        /// <param name="paramValue">Value for parameter</param>
        /// <returns>True if success, false if an error</returns>
        public bool AddAdditionalParameter(string sectionName, string paramName, string paramValue)
        {
            try
            {
                paramValue ??= string.Empty;

                SetParam(sectionName, paramName, paramValue);
                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception adding parameter: " + paramName + " Value: " + paramValue, ex);
                return false;
            }
        }

        /// <summary>
        /// Add new dataset name and ID to DatasetInfoList
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="datasetID">Dataset ID</param>
        public void AddDatasetInfo(string datasetName, int datasetID)
        {
            if (string.IsNullOrWhiteSpace(datasetName))
                return;

            if (!mDatasetInfoList.ContainsKey(datasetName))
            {
                mDatasetInfoList.Add(datasetName, datasetID);
            }
        }

        /// <summary>
        /// Add a fileName extension to not move to the results directory
        /// </summary>
        /// <remarks>Can be a file extension (like .raw) or even a partial file name like _peaks.txt</remarks>
        /// <param name="fileExtension">File extension</param>
        public void AddResultFileExtensionToSkip(string fileExtension)
        {
            if (string.IsNullOrWhiteSpace(fileExtension))
                return;

            // Add the file extension if not yet present
            mResultFileExtensionsToSkip.Add(fileExtension);
        }

        /// <summary>
        /// Add a filename to definitely move to the results directory
        /// </summary>
        /// <param name="fileName">File name</param>
        public void AddResultFileToKeep(string fileName)
        {
            if (string.IsNullOrWhiteSpace(fileName))
                return;

            fileName = Path.GetFileName(fileName);

            // Add the file name if not yet present
            mResultFilesToKeep.Add(fileName);
        }

        /// <summary>
        /// Add a filename to not move to the results directory
        /// </summary>
        /// <param name="fileName">File name</param>
        public void AddResultFileToSkip(string fileName)
        {
            if (string.IsNullOrWhiteSpace(fileName))
                return;

            fileName = Path.GetFileName(fileName);

            // Add the file name if not yet present
            mResultFilesToSkip.Add(fileName);
        }

        /// <summary>
        /// Add a file to be deleted from the storage server (requires full file path)
        /// </summary>
        /// <param name="filePath">Full path to the file</param>
        public void AddServerFileToDelete(string filePath)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                return;

            // Add the file path if not yet present
            mServerFilesToDelete.Add(filePath);
        }

        /// <summary>
        /// Look for files matching fileSpec that are over thresholdHours old
        /// Delete any that are found
        /// </summary>
        /// <param name="taskQueueDirectory">Task queue directory</param>
        /// <param name="fileSpec">Files to find, for example *.oldlock</param>
        /// <param name="thresholdHours">Threshold, in hours, for example 24</param>
        /// <param name="ignoreIfRecentJobStatusFile">When true, do not delete the file if a recent .jobstatus file exists</param>
        private void DeleteOldFiles(
            DirectoryInfo taskQueueDirectory, string fileSpec,
            int thresholdHours, bool ignoreIfRecentJobStatusFile = false)
        {
            try
            {
                string targetFileDescription;

                if (fileSpec.StartsWith("*.") && fileSpec.Length > 2)
                {
                    targetFileDescription = fileSpec.Substring(2);
                }
                else
                {
                    targetFileDescription = fileSpec;
                }

                var foundFiles = taskQueueDirectory.GetFiles(fileSpec);

                if (foundFiles.Length == 0)
                    return;

                if (thresholdHours < 1)
                    thresholdHours = 1;

                var agedFileThreshold = DateTime.UtcNow.AddHours(-thresholdHours);

                var agedFiles = (from item in foundFiles where item.LastWriteTimeUtc < agedFileThreshold select item).ToList();

                if (agedFiles.Count == 0)
                    return;

                var jobStatusFiles = new Dictionary<string, FileInfo>();

                if (ignoreIfRecentJobStatusFile)
                {
                    foreach (var jobStatusFile in taskQueueDirectory.GetFiles("*.jobstatus"))
                    {
                        var baseName = Path.GetFileName(jobStatusFile.Name);

                        if (jobStatusFiles.ContainsKey(baseName))
                            continue;

                        jobStatusFiles.Add(baseName, jobStatusFile);
                    }
                }

                foreach (var agedFile in agedFiles)
                {
                    var baseName = Path.GetFileName(agedFile.Name);

                    if (jobStatusFiles.TryGetValue(baseName, out var jobStatusFile) &&
                        DateTime.UtcNow.Subtract(jobStatusFile.LastWriteTimeUtc).TotalHours < 12)
                    {
                        // The file is aged, but the job status file is less than 12 hours old
                        continue;
                    }

                    var fileAgeHours = DateTime.UtcNow.Subtract(agedFile.LastWriteTimeUtc).TotalHours;

                    // Example message:
                    // Deleting aged lock file modified 26 hours ago: /file1/temp/DMSTasks/Test_MSGFPlus/Job1451055_Step3_20180308_2148.lock
                    LogWarning(
                        "Deleting aged {0} file modified {1:F0} hours ago: {2}",
                        targetFileDescription, fileAgeHours, agedFile.FullName);

                    agedFile.Delete();
                }
            }
            catch (Exception ex)
            {
                LogError("Exception deleting aged files in " + taskQueueDirectory.FullName, ex);
            }
        }

        /// <summary>
        /// Delete old files in the task queue directory
        /// </summary>
        /// <param name="taskQueueDirectory">Task queue directory</param>
        private void DeleteOldTaskQueueFiles(DirectoryInfo taskQueueDirectory)
        {
            // Look for .lock files that are over 24 hours old and do not have a .jobstatus file modified within the last 12 hours
            DeleteOldFiles(taskQueueDirectory, "*.lock", 24, true);

            // Delete other old files over 48 hours old
            DeleteOldFiles(taskQueueDirectory, "*.oldinfo", 48);
            DeleteOldFiles(taskQueueDirectory, "*.oldlock", 48);

            // Delete .jobstatus files over 1 week old
            DeleteOldFiles(taskQueueDirectory, "*.jobstatus", 168);
        }

        /// <summary>
        /// Get all job parameters for the given section
        /// </summary>
        /// <returns>Dictionary where keys are parameter names and values are parameter values</returns>
        public Dictionary<string, string> GetAllParametersForSection(string sectionName)
        {
            if (mJobParams.TryGetValue(sectionName, out var parameters))
            {
                return parameters;
            }

            return new Dictionary<string, string>();
        }

        /// <summary>
        /// Get job parameter section names
        /// </summary>
        public List<string> GetAllSectionNames()
        {
            return mJobParams.Keys.ToList();
        }

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section)
        /// </summary>
        /// <remarks>
        /// If the value associated with the parameter is found, yet is not true or false, an exception will occur;
        /// the calling procedure must handle this exception
        /// </remarks>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value if missing</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public bool GetJobParameter(string name, bool valueIfMissing)
        {
            string value;

            try
            {
                value = GetParam(name);

                if (string.IsNullOrWhiteSpace(value))
                {
                    return valueIfMissing;
                }
            }
            catch
            {
                return valueIfMissing;
            }

            // Note: if value is not true or false, this will throw an exception; the calling procedure will need to handle that exception
            return bool.Parse(value);
        }

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section)
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value if missing</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public string GetJobParameter(string name, string valueIfMissing)
        {
            string value;

            try
            {
                value = GetParam(name);

                if (string.IsNullOrEmpty(value))
                {
                    return valueIfMissing;
                }
            }
            catch
            {
                return valueIfMissing;
            }

            return value;
        }

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section)
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value if missing</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public int GetJobParameter(string name, int valueIfMissing)
        {
            string value;

            try
            {
                value = GetParam(name);

                if (string.IsNullOrWhiteSpace(value))
                {
                    return valueIfMissing;
                }
            }
            catch
            {
                return valueIfMissing;
            }

            // Note: if value is not a number, this will throw an exception; the calling procedure will need to handle that exception
            return int.Parse(value);
        }

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section)
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value if missing</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public short GetJobParameter(string name, short valueIfMissing)
        {
            return (short)GetJobParameter(name, (int)valueIfMissing);
        }

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section)
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value if missing</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public float GetJobParameter(string name, float valueIfMissing)
        {
            return Global.CSngSafe(GetParam(name), valueIfMissing);
        }

        /// <summary>
        /// Gets a job parameter with the given name, preferentially using the specified parameter section
        /// </summary>
        /// <param name="section">Section name for parameter</param>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public bool GetJobParameter(string section, string name, bool valueIfMissing)
        {
            var valueText = GetParam(section, name);

            if (string.IsNullOrWhiteSpace(valueText))
                return valueIfMissing;

            if (bool.TryParse(valueText, out var value))
                return value;

            if (int.TryParse(valueText, out var integerValue))
            {
                return integerValue != 0;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Gets a job parameter with the given name, preferentially using the specified parameter section
        /// </summary>
        /// <param name="section">Section name for parameter</param>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public int GetJobParameter(string section, string name, int valueIfMissing)
        {
            return Global.CIntSafe(GetParam(section, name), valueIfMissing);
        }

        /// <summary>
        /// Gets a job parameter with the given name, preferentially using the specified parameter section
        /// </summary>
        /// <param name="section">Section name for parameter</param>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public string GetJobParameter(string section, string name, string valueIfMissing)
        {
            var value = GetParam(section, name);

            return string.IsNullOrEmpty(value) ? valueIfMissing : value;
        }

        /// <summary>
        /// Gets a job parameter with the given name, preferentially using the specified parameter section
        /// </summary>
        /// <param name="section">Section name for parameter</param>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value to return if the parameter is not found</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public float GetJobParameter(string section, string name, float valueIfMissing)
        {
            return Global.CSngSafe(GetParam(section, name), valueIfMissing);
        }

        /// <summary>
        /// Get a description of the current job number and step number
        /// </summary>
        /// <returns>String in the form "job x, step y"</returns>
        public string GetJobStepDescription()
        {
            var job = GetJobParameter(STEP_PARAMETERS_SECTION, "Job", 0);
            var step = GetJobParameter(STEP_PARAMETERS_SECTION, "Step", 0);

            return string.Format("job {0}, step {1}", job, step);
        }

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section)
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <returns>Value for specified parameter; empty string if not found</returns>
        public string GetParam(string name)
        {
            return TryGetParam(name, out var value) ? value : string.Empty;
        }

        /// <summary>
        /// Gets a job parameter with the given name, preferentially using the specified parameter section
        /// </summary>
        /// <param name="section">Section name for parameter</param>
        /// <param name="name">Key name for parameter</param>
        /// <returns>Value for specified parameter; empty string if not found</returns>
        public string GetParam(string section, string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                // User actually wanted to look for the parameter that is currently in the Section Variable, using an empty string as the default value
                return GetParam(section);
            }

            return TryGetParam(section, name, out var value) ? value : string.Empty;
        }

        /// <summary>
        /// Job parameters file
        /// </summary>
        /// <param name="jobNum">Job number</param>
        public static string JobParametersFilename(int jobNum)
        {
            return Global.JOB_PARAMETERS_FILE_PREFIX + jobNum + ".xml";
        }

        /// <summary>
        /// Add/updates the value for the given parameter (searches all sections)
        /// </summary>
        /// <param name="paramName">Parameter name</param>
        /// <param name="paramValue">Parameter value</param>
        public void SetParam(string paramName, string paramValue)
        {
            var matchFound = false;

            paramValue ??= string.Empty;

            foreach (var section in mJobParams)
            {
                if (section.Value.ContainsKey(paramName))
                {
                    section.Value[paramName] = paramValue;
                    matchFound = true;
                }
            }

            if (!matchFound && mJobParams.Count > 0)
            {
                // Add the parameter to the first section
                mJobParams.First().Value.Add(paramName, paramValue);
            }
        }

        /// <summary>
        /// Add/updates the value for the given parameter
        /// </summary>
        /// <param name="section">Section name</param>
        /// <param name="paramName">Parameter name</param>
        /// <param name="paramValue">Parameter value</param>
        public void SetParam(string section, string paramName, string paramValue)
        {
            if (!mJobParams.TryGetValue(section, out var parameters))
            {
                // New section; add it
                parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                mJobParams.Add(section, parameters);
            }

            paramValue ??= string.Empty;

            // Add/update paramName
            parameters[paramName] = paramValue;
        }

        /// <summary>
        /// Return true if toolRunnerResult is CLOSEOUT_SUCCESS or CLOSEOUT_NO_DATA or if a step tool was skipped
        /// </summary>
        /// <param name="toolRunnerResult">Result code</param>
        public static bool SuccessOrNoData(CloseOutType toolRunnerResult)
        {
            return toolRunnerResult is
                       CloseOutType.CLOSEOUT_SUCCESS or
                       CloseOutType.CLOSEOUT_NO_DATA or
                       CloseOutType.CLOSEOUT_SKIPPED_DIA_NN_SPEC_LIB or
                       CloseOutType.CLOSEOUT_SKIPPED_MAXQUANT or
                       CloseOutType.CLOSEOUT_SKIPPED_MSXML_GEN or
                       CloseOutType.CLOSEOUT_SKIPPED_MZ_REFINERY;
        }

        /// <summary>
        /// Attempts to retrieve the specified parameter (looks in all parameter sections)
        /// </summary>
        /// <param name="paramName">Parameter Name</param>
        /// <param name="paramValue">Output: parameter value</param>
        /// <returns>True if success, false if not found</returns>
        public bool TryGetParam(string paramName, out string paramValue)
        {
            paramValue = string.Empty;

            foreach (var entry in mJobParams)
            {
                if (entry.Value.TryGetValue(paramName, out paramValue))
                {
                    if (string.IsNullOrWhiteSpace(paramValue))
                    {
                        paramValue = string.Empty;
                    }
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Attempts to retrieve the specified parameter in the specified parameter section
        /// </summary>
        /// <param name="section">Section Name</param>
        /// <param name="paramName">Parameter Name</param>
        /// <param name="paramValue">Output: parameter value</param>
        /// <returns>True if success, false if not found</returns>
        public bool TryGetParam(string section, string paramName, out string paramValue)
        {
            return TryGetParam(section, paramName, out paramValue, true);
        }

        /// <summary>
        /// Attempts to retrieve the specified parameter in the specified parameter section
        /// </summary>
        /// <param name="section">Section Name</param>
        /// <param name="paramName">Parameter Name</param>
        /// <param name="paramValue">Output: parameter value</param>
        /// <param name="searchAllSectionsIfNotFound">If true, searches other sections for the parameter if not found in the specified section</param>
        /// <returns>True if success, false if not found</returns>
        public bool TryGetParam(string section, string paramName, out string paramValue, bool searchAllSectionsIfNotFound)
        {
            paramValue = string.Empty;

            if (mJobParams.TryGetValue(section, out var parameters) &&
                parameters.TryGetValue(paramName, out paramValue))
            {
                if (string.IsNullOrWhiteSpace(paramValue))
                {
                    paramValue = string.Empty;
                }
                return true;
            }

            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (searchAllSectionsIfNotFound)
            {
                // Parameter not found in the specified section
                // Search for the entry in other sections
                return TryGetParam(paramName, out paramValue);
            }

            return false;
        }

        /// <summary>
        /// Remove a fileName that was previously added to ResultFilesToSkip
        /// </summary>
        /// <param name="fileName">File name</param>
        public void RemoveResultFileToSkip(string fileName)
        {
            if (mResultFilesToSkip.Contains(fileName))
            {
                mResultFilesToSkip.Remove(fileName);
            }
        }

        /// <summary>
        /// Filter the job parameters in paramXml to remove extra items from the given section
        /// </summary>
        /// <param name="paramXml">Job Parameters XML to filter</param>
        /// <param name="sectionName">sectionName to match</param>
        /// <param name="paramNamesToIgnore">
        /// Keys are parameter names to ignore
        /// Values are another parameter name that must be present if we're going to ignore the given parameter
        /// </param>
        /// <param name="paramsToAddAsAttribute">
        /// Parameters to convert to an attribute at the section level
        /// Keys are the parameter name to match, Values are the new attribute name to add
        /// </param>
        /// <returns>Updated XML, as a string</returns>
        private string FilterXmlSection(
            string paramXml,
            string sectionName,
            IReadOnlyDictionary<string, string> paramNamesToIgnore,
            IReadOnlyDictionary<string, string> paramsToAddAsAttribute)
        {
            try
            {
                // Note that XDocument supersedes XmlDocument and can often be easier to use since XDocument is LINQ-based
                var doc = XDocument.Parse(paramXml);

                foreach (var section in doc.Elements("sections").Elements("section"))
                {
                    if (!section.HasAttributes)
                        continue;

                    var nameAttrib = section.Attribute("name");

                    if (nameAttrib == null)
                        continue;

                    if (nameAttrib.Value != sectionName)
                        continue;

                    var parameterItems = section.Elements("item").ToList();

                    // Construct a list of the parameter names in this section
                    var paramNames = new List<string>();

                    foreach (var item in parameterItems)
                    {
                        if (!item.HasAttributes) continue;

                        var itemKey = item.Attribute("key");

                        if (itemKey == null) continue;

                        paramNames.Add(itemKey.Value);
                    }

                    var paramsToRemove = new List<XElement>();
                    var attributesToAdd = new Dictionary<string, string>();

                    foreach (var paramItem in parameterItems)
                    {
                        if (!paramItem.HasAttributes)
                            continue;

                        var paramName = paramItem.Attribute("key");

                        if (paramName == null)
                            continue;

                        if (paramNamesToIgnore.ContainsKey(paramName.Value))
                        {
                            var requiredParameter = paramNamesToIgnore[paramName.Value];

                            if (string.IsNullOrWhiteSpace(requiredParameter) || paramNames.Contains(requiredParameter))
                            {
                                // Remove this parameter from this section
                                paramsToRemove.Add(paramItem);
                            }
                        }

                        if (!paramsToAddAsAttribute.ContainsKey(paramName.Value))
                            continue;

                        // Add an attribute to the section with the value for this parameter
                        // This is most commonly used to add attribute step="1"

                        var attribName = paramsToAddAsAttribute[paramName.Value];

                        if (string.IsNullOrWhiteSpace(attribName))
                            attribName = paramName.Value;

                        var paramValue = paramItem.Attribute("value");

                        attributesToAdd.Add(attribName, paramValue == null ? string.Empty : paramValue.Value);
                    }

                    foreach (var paramItem in paramsToRemove)
                    {
                        paramItem.Remove();
                    }

                    foreach (var attribItem in attributesToAdd)
                    {
                        section.SetAttributeValue(attribItem.Key, attribItem.Value);
                    }
                }

                var builder = new StringBuilder();

                var settings = new XmlWriterSettings
                {
                    Indent = true,
                    IndentChars = "  ",
                    OmitXmlDeclaration = true
                };

                using (var writer = XmlWriter.Create(builder, settings))
                {
                    doc.Save(writer);
                }

                // Return the filtered XML
                return builder.ToString();
            }
            catch (Exception ex)
            {
                LogError("Error in FilterXmlSection", ex);
                return paramXml;
            }
        }

        /// <summary>
        /// Rename or delete old directories in the working directories specified by any .info files below the base task queue directory
        /// Ignores files in the /Completed/ directory
        /// </summary>
        /// <param name="taskQueuePathBase">Task queue base path</param>
        private void PurgeOldOfflineWorkDirs(string taskQueuePathBase)
        {
            const int ORPHANED_THRESHOLD_DAYS = 5;
            const int PURGE_THRESHOLD_DAYS = 14;

            try
            {
                var jobStepMatcher = new Regex(@"^Job\d+_Step\d+$", RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var xJobStepMatcher = new Regex(@"^x_Job\d+_Step\d+$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // Find all .info, .fail, or .success files below taskQueuePathBase

                var taskQueuePathBaseDir = new DirectoryInfo(taskQueuePathBase);
                var taskInfoFiles = new List<FileInfo>();

                taskInfoFiles.AddRange(taskQueuePathBaseDir.GetFiles("*.info", SearchOption.AllDirectories));
                taskInfoFiles.AddRange(taskQueuePathBaseDir.GetFiles("*.success", SearchOption.AllDirectories));
                taskInfoFiles.AddRange(taskQueuePathBaseDir.GetFiles("*.fail", SearchOption.AllDirectories));

                if (taskInfoFiles.Count == 0)
                    return;

                // Construct a list of WorkDir paths tracked by the current .info, .fail, and .success files
                // Also keep track of the parent directory (or directories) of the job-specific working directories

                var activeWorkDirs = new Dictionary<string, DirectoryInfo>();
                var parentWorkDirs = new Dictionary<string, DirectoryInfo>();

                var archivedDirName = Path.DirectorySeparatorChar + RemoteMonitor.ARCHIVED_TASK_QUEUE_DIRECTORY_NAME + Path.DirectorySeparatorChar;

                foreach (var taskInfoFile in taskInfoFiles)
                {
                    if (taskInfoFile.FullName.Contains(archivedDirName))
                    {
                        // Ignore files in the Completed directory
                        continue;
                    }

                    if (TraceMode)
                        ConsoleMsgUtils.ShowDebug("  Reading task info file " + taskInfoFile.FullName);

                    var success = ReadOfflineJobInfoFile(taskInfoFile, out _, out _, out var workDirPath, out _);

                    if (!success || string.IsNullOrWhiteSpace(workDirPath))
                        continue;

                    var jobWorkDir = new DirectoryInfo(workDirPath);

                    if (!activeWorkDirs.ContainsKey(jobWorkDir.FullName))
                        activeWorkDirs.Add(jobWorkDir.FullName, jobWorkDir);

                    var parentWorkDir = jobWorkDir.Parent;

                    if (parentWorkDir == null)
                        continue;

                    if (!parentWorkDirs.ContainsKey(parentWorkDir.FullName))
                        parentWorkDirs.Add(parentWorkDir.FullName, parentWorkDir);
                }

                if (parentWorkDirs.Count == 0)
                    return;

                foreach (var parentWorkDir in parentWorkDirs.Values)
                {
                    // Find all directories named JobX_StepY in parentWorkDir

                    if (TraceMode)
                        ConsoleMsgUtils.ShowDebug("Finding Job_Step directories in " + parentWorkDir.FullName);

                    var workDirs = parentWorkDir.GetDirectories("Job*_Step*", SearchOption.TopDirectoryOnly);

                    if (workDirs.Length == 0)
                        continue;

                    // JobX_StepY directories older than this time are renamed to x_
                    var orphanedDateThresholdUtc = DateTime.UtcNow.AddDays(-ORPHANED_THRESHOLD_DAYS);

                    // x_JobX_StepY directories older than this date are deleted
                    var purgeThresholdUtc = DateTime.UtcNow.AddDays(-PURGE_THRESHOLD_DAYS);

                    foreach (var workDir in workDirs)
                    {
                        if (activeWorkDirs.ContainsKey(workDir.FullName))
                        {
                            if (TraceMode)
                            {
                                ConsoleMsgUtils.ShowDebug(
                                    "  Ignore {0} since referred to by a recent task info file", workDir.Name);
                            }
                            // This work dir is active; ignore it
                            continue;
                        }

                        workDir.Refresh();

                        if (!workDir.Exists)
                        {
                            // WorkDir no longer exists
                            continue;
                        }

                        if (!jobStepMatcher.IsMatch(workDir.Name))
                        {
                            if (TraceMode)
                            {
                                ConsoleMsgUtils.ShowDebug(
                                    "  Ignore WorkDir directory since not of the form JobX_StepY: " + workDir.FullName);
                            }
                            continue;
                        }

                        // Determine when the directory (or any files in it) were last updated
                        var lastModifiedUtc = GetDirectoryLastWriteTime(workDir);

                        if (lastModifiedUtc >= orphanedDateThresholdUtc)
                        {
                            if (TraceMode)
                            {
                                ConsoleMsgUtils.ShowDebug("  Ignoring {0} since last modified {1}",
                                                          workDir.Name, lastModifiedUtc.ToLocalTime());
                            }

                            // Files in the WorkDir are less than 5 days old; leave it unchanged
                            continue;
                        }

                        if (workDir.Parent == null)
                        {
                            LogWarning("Unable to determine the parent directory of {0}; cannot update the name to contain x_", workDir.FullName);
                            continue;
                        }

                        var newWorkDirPath = Path.Combine(workDir.Parent.FullName, "x_" + workDir.Name);

                        try
                        {
                            if (Directory.Exists(newWorkDirPath))
                            {
                                LogMessage(
                                    "Deleting old working directory prior to renaming another old working directory; deleting {0}",
                                    newWorkDirPath);

                                Directory.Delete(newWorkDirPath, true);
                            }

                            LogMessage(
                                "Renaming old working directory since no current .info files refer to it; moving {0} to {1}",
                                workDir.FullName, Path.GetFileName(newWorkDirPath));

                            workDir.MoveTo(newWorkDirPath);
                        }
                        catch (Exception ex)
                        {
                            LogWarning("Error renaming {0} to {1}: {2}", workDir.FullName, Path.GetFileName(newWorkDirPath), ex.Message);
                        }
                    }

                    if (TraceMode)
                        ConsoleMsgUtils.ShowDebug("Finding x_Job_Step directories in " + parentWorkDir.FullName);

                    var oldWorkDirs = parentWorkDir.GetDirectories("x_Job*_Step*", SearchOption.TopDirectoryOnly);

                    if (oldWorkDirs.Length == 0)
                        continue;

                    foreach (var oldWorkDir in oldWorkDirs)
                    {
                        oldWorkDir.Refresh();

                        if (!oldWorkDir.Exists)
                        {
                            // WorkDir no longer exists
                            continue;
                        }

                        if (!xJobStepMatcher.IsMatch(oldWorkDir.Name))
                        {
                            if (TraceMode)
                                LogDebug("  Ignore x_WorkDir directory since not of the form x_JobX_StepY: " + oldWorkDir.FullName);
                            continue;
                        }

                        // Determine when the directory (or any files in it) were last updated
                        var lastModifiedUtc = GetDirectoryLastWriteTime(oldWorkDir);

                        if (lastModifiedUtc >= purgeThresholdUtc)
                        {
                            if (TraceMode)
                            {
                                ConsoleMsgUtils.ShowDebug(
                                    "  Ignoring {0} since last modified {1}",
                                    oldWorkDir.Name, lastModifiedUtc.ToLocalTime());
                            }

                            // Files in the old WorkDir are less than 14 days old; leave it unchanged
                            continue;
                        }

                        try
                        {
                            LogMessage("Deleting old working directory since over {0} days old: {1}", PURGE_THRESHOLD_DAYS, oldWorkDir.FullName);

                            oldWorkDir.Delete(true);
                        }
                        catch (Exception ex)
                        {
                            LogWarning("Error deleting {0}: {1}", oldWorkDir.FullName, ex.Message);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception looking for old WorkDirs for .info, .success, and .fail files below " + taskQueuePathBase, ex);
            }
        }

        private bool ReadOfflineJobInfoFile(FileSystemInfo infoFile, out int jobId, out int stepNum, out string workDirPath, out string staged)
        {
            jobId = 0;
            stepNum = 0;
            workDirPath = string.Empty;
            staged = string.Empty;

            if (!infoFile.Exists)
                return false;

            var splitChars = new[] { '=' };

            using var reader = new StreamReader(new FileStream(infoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            while (!reader.EndOfStream)
            {
                var dataLine = reader.ReadLine();

                if (string.IsNullOrWhiteSpace(dataLine))
                    continue;

                var lineParts = dataLine.Split(splitChars, 2);

                if (lineParts.Length < 2)
                    continue;

                var key = lineParts[0];
                var value = lineParts[1];

                switch (key)
                {
                    case "Job":
                        jobId = int.Parse(value);
                        break;
                    case "Step":
                        stepNum = int.Parse(value);
                        break;
                    case "WorkDir":
                        workDirPath = value;
                        break;
                    case "Staged":
                        staged = value;
                        break;
                }
            }

            return true;
        }

        /// <summary>
        /// Requests a task from the database
        /// </summary>
        /// <returns>Enum indicating if task was found</returns>
        public RequestTaskResult RequestTask()
        {
            RequestTaskResult result;

            if (Global.OfflineMode)
            {
                result = RequestOfflineAnalysisJob();
            }
            else
            {
                var runJobsRemotely = mMgrParams.GetParam("RunJobsRemotely", false);
                result = RequestAnalysisJobFromDB(runJobsRemotely);
            }

            switch (result)
            {
                case RequestTaskResult.NoTaskFound:
                    TaskWasAssigned = false;
                    break;

                case RequestTaskResult.TaskFound:
                    TaskWasAssigned = true;
                    break;

                case RequestTaskResult.TooManyRetries:
                case RequestTaskResult.Deadlock:
                    // Make sure the database didn't actually assign a job to this manager
                    ReportManagerIdle();
                    TaskWasAssigned = false;
                    break;

                default:
                    TaskWasAssigned = false;
                    break;
            }

            return result;
        }

        /// <summary>
        /// Requests a single analysis job using request_step_task_xml
        /// </summary>
        /// <returns>Enum indicating if task was found</returns>
        private RequestTaskResult RequestAnalysisJobFromDB(bool runJobsRemotely)
        {
            if (Global.OfflineMode)
            {
                throw new Exception("RequestAnalysisJobFromDB should not be called when offline mode is enabled");
            }

            var productVersion = Global.GetAssemblyVersion() ?? "??";

            var dotNetVersion = Global.GetDotNetVersion();

            string managerVersion;

            if (!string.IsNullOrWhiteSpace(dotNetVersion))
            {
                if (!dotNetVersion.StartsWith("v", StringComparison.OrdinalIgnoreCase))
                    dotNetVersion = "v" + dotNetVersion;

                managerVersion = productVersion + "; .NET " + dotNetVersion;
            }
            else
            {
                managerVersion = productVersion + "; Unknown .NET Version";
            }

            // Reset various tracking variables, including TaskClosed
            Reset();

            try
            {
                // Call request_step_task_xml to look for an available step task
                // If a task is available, the procedure will return the task parameters as a result set
                // On Postgres, the procedure returns the parameters using a RefCursor, but ExecuteSPData auto-converts that to a result set

                // Set up the command object prior to SP execution
                var cmd = PipelineDBProcedureExecutor.CreateCommand(SP_NAME_REQUEST_TASK, CommandType.StoredProcedure);

                PipelineDBProcedureExecutor.AddParameter(cmd, "@processorName", SqlType.VarChar, 128, ManagerName);
                var jobNumberParam = PipelineDBProcedureExecutor.AddParameter(cmd, "@job", SqlType.Int, 0, ParameterDirection.InputOutput);
                var jobParamsParam = PipelineDBProcedureExecutor.AddParameter(cmd, "@parameters", SqlType.VarChar, 8000, string.Empty, ParameterDirection.InputOutput);
                var messageParam = PipelineDBProcedureExecutor.AddParameter(cmd, "@message", SqlType.VarChar, 512, string.Empty, ParameterDirection.InputOutput);
                var returnCodeParam = PipelineDBProcedureExecutor.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, string.Empty, ParameterDirection.InputOutput);

                PipelineDBProcedureExecutor.AddParameter(cmd, "@infoOnly", SqlType.TinyInt).Value = 0;
                jobNumberParam.Value = 0;
                jobParamsParam.Value = string.Empty;

                PipelineDBProcedureExecutor.AddParameter(cmd, "@analysisManagerVersion", SqlType.VarChar, 128, managerVersion);

                var remoteInfo = runJobsRemotely ? RemoteTransferUtility.GetRemoteInfoXml(mMgrParams) : string.Empty;
                PipelineDBProcedureExecutor.AddParameter(cmd, "@remoteInfo", SqlType.VarChar, 900, remoteInfo);

                if (mDebugLevel > 4 || TraceMode)
                {
                    LogDebug("AnalysisJob.RequestAnalysisJob(), connection string: " + mBrokerConnStr, (int)BaseLogger.LogLevels.DEBUG);
                    LogDebug("AnalysisJob.RequestAnalysisJob(), printing param list", (int)BaseLogger.LogLevels.DEBUG);
                    PrintCommandParams(cmd);
                }

                // Execute the SP
                var resCode = PipelineDBProcedureExecutor.ExecuteSP(cmd, 1);

                var returnCode = DBToolsBase.GetReturnCode(returnCodeParam);

                if (returnCode is RET_VAL_TASK_NOT_AVAILABLE or RET_VAL_TASK_NOT_AVAILABLE_ALT)
                {
                    // No jobs found
                    return RequestTaskResult.NoTaskFound;
                }

                if (resCode == 0 && returnCode == 0)
                {
                    // Step task was found; get the data for it

                    mJobId = jobNumberParam.Value.CastDBVal<int>();

                    if (mJobId == 0)
                    {
                        LogError("Error requesting a step task, {0} returned 0 for both the result code and the return code indicating that an available step task was found; " +
                                 "however, the job number procedure argument is 0, meaning a step task was not actually assigned", SP_NAME_REQUEST_TASK);

                        return RequestTaskResult.ResultError;
                    }

                    var jobParamsXML = jobParamsParam.Value.CastDBVal<string>();

                    var jobParameters = ParseXMLJobParameters(jobParamsXML).ToList();

                    if (jobParameters.Count == 0)
                    {
                        LogError("Unable to parse out job parameters from the job parameters XML");
                        return RequestTaskResult.ResultError;
                    }

                    foreach (var paramInfo in jobParameters)
                    {
                        // Check for conflicting values
                        if (mJobParams.TryGetValue(paramInfo.Section, out var existingSection) &&
                            existingSection.TryGetValue(paramInfo.ParamName, out var existingValue))
                        {
                            if (string.Equals(existingValue, paramInfo.Value))
                            {
                                LogDebug(
                                    "Skipping duplicate task parameter in section {0} named {1}: the new value matches the existing value of '{2}'",
                                    paramInfo.Section, paramInfo.ParamName, existingValue);

                                continue;
                            }

                            LogError(
                                "Duplicate task parameters in section {0} have the same name ({1}), but conflicting values: existing value is '{2}' vs. new value of '{3}'",
                                paramInfo.Section, paramInfo.ParamName, existingValue, paramInfo.Value);

                            return RequestTaskResult.ResultError;
                        }

                        SetParam(paramInfo.Section, paramInfo.ParamName, paramInfo.Value);
                    }

                    SaveJobParameters(mMgrParams.GetParam("WorkDir"), jobParamsXML, mJobId);
                    return RequestTaskResult.TaskFound;
                }

                var outputMessage = messageParam.Value.CastDBVal<string>();
                var message = string.IsNullOrWhiteSpace(outputMessage) ? "Unknown error" : outputMessage;

                if (resCode != 0 && returnCode == 0)
                {
                    switch (resCode)
                    {
                        case DbUtilsConstants.RET_VAL_EXCESSIVE_RETRIES:
                            // Too many retries
                            return RequestTaskResult.TooManyRetries;

                        case DbUtilsConstants.RET_VAL_DEADLOCK:
                            // Transaction was deadlocked on lock resources with another process and has been chosen as the deadlock victim
                            return RequestTaskResult.Deadlock;

                        default:
                            // There was an SP error
                            LogError("ExecuteSP() reported result code {0} calling {1}, message: {2}", resCode, SP_NAME_REQUEST_TASK, message);
                            return RequestTaskResult.ResultError;
                    }
                }

                // The return code was not an empty string, which indicates an error
                LogError("Error requesting a step task, {0} returned {1}; message: {2}",
                    SP_NAME_REQUEST_TASK, returnCodeParam.Value.CastDBVal<string>(), message);

                return RequestTaskResult.ResultError;
            }
            catch (Exception ex)
            {
                LogError("Exception requesting analysis job", ex);
                return RequestTaskResult.ResultError;
            }
        }

        private RequestTaskResult RequestOfflineAnalysisJob()
        {
            try
            {
                var reJobStepTimestamp = new Regex(@"(?<JobStep>Job\d+_Step\d+)_(?<TimeStamp>.+)\.info", RegexOptions.Compiled);

                var stepToolList = mMgrParams.GetParam("StepToolsEnabled");

                if (string.IsNullOrWhiteSpace(stepToolList))
                {
                    LogError("No step tools are enabled; update manager parameter StepToolsEnabled in ManagerSettingsLocal.xml");
                    return RequestTaskResult.ResultError;
                }

                var stepTools = stepToolList.Split(',');

                var taskQueuePathBase = mMgrParams.GetParam("LocalTaskQueuePath");

                if (string.IsNullOrWhiteSpace(taskQueuePathBase))
                {
                    LogError("Manager parameter LocalTaskQueuePath is empty; update ManagerSettingsLocal.xml");
                    return RequestTaskResult.ResultError;
                }

                // Reset various tracking variables, including TaskClosed
                Reset();

                PurgeOldOfflineWorkDirs(taskQueuePathBase);

                foreach (var stepTool in stepTools)
                {
                    var taskQueueDirectory = new DirectoryInfo(Path.Combine(taskQueuePathBase, stepTool.Trim()));

                    if (!taskQueueDirectory.Exists)
                    {
                        LogWarning("Task queue directory not found: " + taskQueueDirectory.FullName);
                        continue;
                    }

                    DeleteOldTaskQueueFiles(taskQueueDirectory);

                    var infoFiles = taskQueueDirectory.GetFiles("*.info");

                    if (infoFiles.Length == 0)
                        continue;

                    // Keys in this dictionary are of the form Job1449939_Step3
                    // Values are KeyValuePair of Timestamp and .info file, where Timestamp is of the form 20170518_0353
                    var jobStepInfoFiles = new Dictionary<string, KeyValuePair<string, FileInfo>>();

                    // Step through the info files, keeping track of the newest file for each Job/Step combo
                    foreach (var infoFile in infoFiles)
                    {
                        var match = reJobStepTimestamp.Match(infoFile.Name);

                        if (!match.Success)
                        {
                            LogDebug("Ignoring .info file that has an unrecognized name format: " + infoFile.FullName);
                            continue;
                        }

                        var jobStep = match.Groups["JobStep"].Value;
                        var timeStamp = match.Groups["TimeStamp"].Value;

                        if (!jobStepInfoFiles.TryGetValue(jobStep, out var existingInfo))
                        {
                            jobStepInfoFiles.Add(jobStep, new KeyValuePair<string, FileInfo>(timeStamp, infoFile));
                            continue;
                        }

                        var existingTimestamp = existingInfo.Key;

                        if (string.CompareOrdinal(timeStamp, existingTimestamp) > 0)
                        {
                            RenameOldInfoFile(existingInfo.Value);

                            // Add the new .info file to the dictionary
                            jobStepInfoFiles[jobStep] = new KeyValuePair<string, FileInfo>(timeStamp, infoFile);
                        }
                        else
                        {
                            RenameOldInfoFile(infoFile);
                        }
                    }

                    if (jobStepInfoFiles.Count == 0)
                    {
                        continue;
                    }

                    // Find the oldest file in jobStepInfoFiles that does not have a .lock file
                    foreach (var infoFileToProcess in (from item in jobStepInfoFiles.Values orderby item.Value.LastWriteTimeUtc select item.Value))
                    {
                        if (SelectOfflineJobInfoFile(infoFileToProcess))
                            return RequestTaskResult.TaskFound;
                    }
                }

                return RequestTaskResult.NoTaskFound;
            }
            catch (Exception ex)
            {
                LogError("Exception checking for an available offline analysis job", ex);
                return RequestTaskResult.ResultError;
            }
        }

        /// <summary>
        /// Reset the class-wide variables to their defaults
        /// </summary>
        public void Reset()
        {
            TaskClosed = false;

            mResultFilesToSkip.Clear();
            mResultFileExtensionsToSkip.Clear();
            mResultFilesToKeep.Clear();
            mServerFilesToDelete.Clear();

            mDatasetInfoList.Clear();

            mJobParams.Clear();

            mStartTime = DateTime.UtcNow;
        }

        /// <summary>
        /// Saves job Parameters to an XML File in the working directory
        /// </summary>
        /// <remarks>
        /// While saving the job parameters, several StepParameters items are skipped to avoid storing duplicate information in the parameter file
        /// Additionally, each StepParameters section will have attribute "step" added to it, for example step="1" or step="2"
        /// </remarks>
        /// <param name="workDir">Full path to work directory</param>
        /// <param name="jobParamsXML">Contains the xml for all the job parameters</param>
        /// <param name="jobNum">Job number</param>
        private void SaveJobParameters(string workDir, string jobParamsXML, int jobNum)
        {
            var xmlParameterFilePath = string.Empty;

            try
            {
                var xmlParameterFilename = JobParametersFilename(jobNum);
                xmlParameterFilePath = Path.Combine(workDir, xmlParameterFilename);

                var xmlParameterFile = new FileInfo(xmlParameterFilePath);

                // Keys are parameter names to ignore
                // Values are another parameter name that must be present if we're going to ignore the given parameter
                var paramNamesToIgnore = new Dictionary<string, string>
                {
                    {AnalysisResources.JOB_PARAM_SHARED_RESULTS_FOLDERS, ""},
                    {"CPU_Load", ""},
                    {"Job", ""},
                    {"Step", ""},
                    {"StepInputFolderName", AnalysisResources.JOB_PARAM_INPUT_FOLDER_NAME},
                    {"StepOutputFolderName", AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME}
                };

                var paramsToAddAsAttribute = new Dictionary<string, string>
                {
                    { "Step", "step"}
                };

                // Remove extra parameters from the StepParameters section that we don't want to include in the XML
                // Also update the section to have an attribute that is the step number, for example step="1"
                var filteredXML = FilterXmlSection(jobParamsXML, STEP_PARAMETERS_SECTION, paramNamesToIgnore, paramsToAddAsAttribute);

                var xmlWriter = new FormattedXMLWriter();
                xmlWriter.WriteXMLToFile(filteredXML, xmlParameterFile.FullName);

                AddAdditionalParameter(JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_XML_PARAMS_FILE, xmlParameterFilename);

                // Copy the Job Parameter file to the Analysis Manager directory so that we can inspect it if the job fails
                Global.CopyAndRenameFileWithBackup(xmlParameterFile.FullName, Global.GetAppDirectoryPath(), "RecentJobParameters.xml", 5);

                LogDebug(string.Format(
                    "Job Parameters successfully saved to file: {0}", xmlParameterFile.FullName),
                    (int)BaseLogger.LogLevels.DEBUG);
            }
            catch (Exception ex)
            {
                LogError("Exception saving analysis job parameters to " + xmlParameterFilePath, ex);
            }
        }

        /// <summary>
        /// Try to create a .lock file for a given candidate .info file
        /// </summary>
        /// <param name="infoFile">Info file, for example Job1451055_Step3_20170622_2205.info</param>
        /// <returns>True if success, false if the file could not be made</returns>
        private bool SelectOfflineJobInfoFile(FileInfo infoFile)
        {
            try
            {
                var startTime = DateTime.Now;

                // Example name: Job1451055_Step3_20170622_2205.lock
                var lockFilePath = Path.ChangeExtension(infoFile.FullName, Global.LOCK_FILE_EXTENSION);

                if (File.Exists(lockFilePath))
                {
                    // Another process already created the lock file
                    // Note that DeleteOldTaskQueueFiles will eventually delete this .lock file if the other manager crashed
                    return false;
                }

                CreateLocalLockFile(lockFilePath);

                // Parse the .info file

                mJobId = 0;
                var success = ReadOfflineJobInfoFile(infoFile, out var jobId, out var stepNum, out var workDirPath, out var staged);

                if (!success || jobId == 0)
                {
                    FinalizeFailedOfflineJob(infoFile, startTime, "Job missing from .info file");
                    return false;
                }

                mJobId = jobId;

                if (stepNum == 0)
                {
                    FinalizeFailedOfflineJob(infoFile, startTime, "Step missing from .info file");
                    return false;
                }

                if (string.IsNullOrWhiteSpace(workDirPath))
                {
                    FinalizeFailedOfflineJob(infoFile, startTime, "WorkDir missing from .info file");
                    return false;
                }

                LogMessage("Processing offline job {0}, step {1}, WorkDir {2}, staged {3}", mJobId, stepNum, workDirPath, staged);

                mOfflineJobInfoFile = infoFile;

                // Update the working directory in the manager parameters
                // If necessary, switch from a Linux-style path to a Windows-style path
                // (this will be the case when debugging offline jobs on a Windows computer)
                var workDir = new DirectoryInfo(workDirPath);

                if (!string.Equals(workDirPath, workDir.FullName))
                {
                    workDirPath = workDir.FullName;
                }

                mMgrParams.SetParam("WorkDir", workDirPath);

                // Read JobParams.xml and update the job parameters
                var jobParamsFile = new FileInfo(Path.Combine(workDirPath, OFFLINE_JOB_PARAMS_FILE));

                if (!jobParamsFile.Exists)
                {
                    FinalizeFailedOfflineJob(infoFile, startTime, "JobParams.xml file not found in the working directory: " + jobParamsFile.FullName);
                    return false;
                }

                string jobParamsXML;

                using (var reader = new StreamReader(new FileStream(jobParamsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    jobParamsXML = reader.ReadToEnd();
                }

                var jobParameters = ParseXMLJobParameters(jobParamsXML).ToList();

                if (jobParameters.Count == 0)
                {
                    FinalizeFailedOfflineJob(infoFile, startTime, "Unable to parse out job parameters from the job parameters file: " + jobParamsFile.FullName);
                    return false;
                }

                foreach (var paramInfo in jobParameters)
                {
                    SetParam(paramInfo.Section, paramInfo.ParamName, paramInfo.Value);
                }

                return true;
            }
            catch (IOException ex)
            {
                // .lock file was created by another process
                LogError("Unable to create the lock file (likely already exists)", ex);
                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in LockOfflineJobInfoFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Finalize a failed offline job
        /// </summary>
        /// <param name="infoFile">Info file</param>
        /// <param name="startTime">Start time</param>
        /// <param name="errorMessage">Error message</param>
        private void FinalizeFailedOfflineJob(FileSystemInfo infoFile, DateTime startTime, string errorMessage)
        {
            LogError(errorMessage);
            OfflineProcessing.FinalizeJob(infoFile.FullName, ManagerName, false, startTime, 1, errorMessage);
        }

        /// <summary>
        /// Create a new lock file at the given path
        /// </summary>
        /// <remarks>
        /// An exception will be thrown if the lock file already exists, or if another manager overwrites the lock file
        /// This method is similar to CreateRemoteLockFile in RemoteTransferUtility
        /// </remarks>
        /// <param name="lockFilePath">Full path to the .lock file</param>
        /// <returns>Full path to the lock file; empty string if a problem</returns>
        private void CreateLocalLockFile(string lockFilePath)
        {
            var lockFileContents = new List<string>
            {
                "Date: " + DateTime.Now.ToString(AnalysisToolRunnerBase.DATE_TIME_FORMAT),
                "Manager: " + ManagerName
            };

            LogDebug("  creating lock file at " + lockFilePath);

            using (var lockFileWriter = new StreamWriter(new FileStream(lockFilePath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read)))
            {
                // Successfully created the lock file
                foreach (var dataLine in lockFileContents)
                {
                    lockFileWriter.WriteLine(dataLine);
                }
            }

            // Wait 2 to 5 seconds, then re-open the file to make sure it was created by this manager
            var random = new Random();
            Global.IdleLoop(random.Next(2, 5));

            var lockFileContentsNew = new List<string>();

            using (var reader = new StreamReader(new FileStream(lockFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                while (!reader.EndOfStream)
                {
                    lockFileContentsNew.Add(reader.ReadLine());
                }
            }

            if (!Global.LockFilesMatch(lockFilePath, lockFileContents, lockFileContentsNew, out var errorMessage))
            {
                // Lock file content doesn't match the expected value
                throw new Exception(errorMessage);
            }
        }

        /// <summary>
        /// Contact the Pipeline database to close the analysis job
        /// </summary>
        /// <param name="closeOut">IJobParams enum specifying close out type</param>
        /// <param name="compMsg">Completion message to be added to database upon closeOut</param>
        public void CloseTask(CloseOutType closeOut, string compMsg)
        {
            CloseTask(closeOut, compMsg, 0, string.Empty, mStartTime);
        }

        /// <summary>
        /// Contact the Pipeline database to close the analysis job
        /// </summary>
        /// <param name="closeOut">IJobParams enum specifying close out type</param>
        /// <param name="compMsg">Completion message to be added to database upon closeOut</param>
        /// <param name="evalMsg">Evaluation message ("" if no special message)</param>
        public void CloseTask(CloseOutType closeOut, string compMsg, string evalMsg)
        {
            CloseTask(closeOut, compMsg, 0, evalMsg, mStartTime);
        }

        /// <summary>
        /// Contact the Pipeline database to close the analysis job
        /// </summary>
        /// <param name="closeOut">IJobParams enum specifying close out type</param>
        /// <param name="compMsg">Completion message to be added to database upon closeOut</param>
        /// <param name="toolRunner">ToolRunner instance (AnalysisToolRunnerBase)</param>
        public void CloseTask(CloseOutType closeOut, string compMsg, IToolRunner toolRunner)
        {
            CloseTask(closeOut, compMsg, toolRunner.EvalCode, toolRunner.EvalMessage, toolRunner.StartTime);
        }

        /// <summary>
        /// Contact the Pipeline database to close the analysis job
        /// </summary>
        /// <param name="closeOut">IJobParams enum specifying close out type</param>
        /// <param name="compMsg">Completion message to be added to database upon closeOut</param>
        /// <param name="evalCode">Evaluation code (0 if no special evaluation message)</param>
        /// <param name="evalMsg">Evaluation message ("" if no special message)</param>
        /// <param name="startTime">Time the analysis started (UTC-based)</param>
        private void CloseTask(CloseOutType closeOut, string compMsg, int evalCode, string evalMsg, DateTime startTime)
        {
            var compCode = (int)closeOut;

            compMsg ??= string.Empty;

            evalMsg ??= string.Empty;

            if (TaskClosed && Global.OfflineMode)
            {
                // Make sure a .lock file does not exist
                var lockFile = new FileInfo(Path.ChangeExtension(mOfflineJobInfoFile.FullName, Global.LOCK_FILE_EXTENSION));

                if (lockFile.Exists)
                {
                    LogWarning("Job {0} has already been closed; however, a lock file still exists at {1}; re-trying the call to FinalizeJob", mJobId, lockFile.FullName);

                    TaskClosed = false;
                }
            }

            if (TaskClosed)
            {
                // Job 1234567 has already been closed; will not call set_step_task_complete again
                LogWarning("Job {0} has already been closed; will not call {1} again", mJobId, SP_NAME_SET_COMPLETE);

                return;
            }

            TaskClosed = true;

            if (Global.OfflineMode)
            {
                if (mOfflineJobInfoFile == null)
                {
                    LogError("Cannot finalize offline job; mOfflineJobInfoFile is null for job" + mJobId);
                    return;
                }

                var succeeded = SuccessOrNoData(closeOut);
                OfflineProcessing.FinalizeJob(mOfflineJobInfoFile.FullName, ManagerName, succeeded, startTime, compCode, compMsg, evalCode, evalMsg);
            }
            else if (!SetAnalysisJobComplete(compCode, compMsg, evalCode, evalMsg))
            {
                LogError("Error setting job complete in database, job " + mJobId);
            }
        }

        /// <summary>
        /// Determine the most recent time that a file in a directory was changed, or that the directory itself was changed
        /// </summary>
        /// <remarks>
        /// If the directory has no files, the returned file info will be for a
        /// non-existent file named Placeholder.txt, with the date of the directory's last write time</remarks>
        /// <param name="directory">Directory info</param>
        /// <param name="recurse">When true, recurse</param>
        /// <returns>UTC time of last change to files in the directory or the directory itself</returns>
        private DateTime GetDirectoryLastWriteTime(DirectoryInfo directory, bool recurse = false)
        {
            var newestDateUtc = DateTime.MinValue;

            var searchOption = recurse ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;

            foreach (var workDirFile in directory.GetFiles("*", searchOption))
            {
                if (workDirFile.LastWriteTimeUtc > newestDateUtc)
                {
                    newestDateUtc = workDirFile.LastWriteTimeUtc;
                }
            }

            if (directory.LastWriteTimeUtc > newestDateUtc)
                newestDateUtc = directory.LastWriteTimeUtc;

            return newestDateUtc;
        }

        /// <summary>
        /// Rename an old .info file to .oldinfo
        /// Also check for a .lock file that corresponds to the .info file
        /// </summary>
        /// <param name="oldInfoFile">Old info file</param>
        private static void RenameOldInfoFile(FileInfo oldInfoFile)
        {
            // Old .info file with existingTimestamp; rename to .oldinfo
            OfflineProcessing.RenameFileChangeExtension(oldInfoFile, ".oldinfo", true);

            // Also check for a .lock file; if found, rename it
            var oldLockFile = new FileInfo(Path.ChangeExtension(oldInfoFile.FullName, Global.LOCK_FILE_EXTENSION));
            OfflineProcessing.RenameFileChangeExtension(oldLockFile, ".oldlock", true);
        }

        /// <summary>
        /// Call stored procedure report_manager_idle to inform the database that this manager did not receive a job
        /// </summary>
        /// <remarks>This is used when a Deadlock occurs while requesting a job</remarks>
        private void ReportManagerIdle()
        {
            if (Global.OfflineMode)
            {
                LogWarning("ReportManagerIdle should not be called when offline mode is enabled");
                return;
            }

            // Setup for execution of the stored procedure
            var cmd = PipelineDBProcedureExecutor.CreateCommand(SP_NAME_REPORT_IDLE, CommandType.StoredProcedure);

            PipelineDBProcedureExecutor.AddParameter(cmd, "@managerName", SqlType.VarChar, 128, ManagerName);
            PipelineDBProcedureExecutor.AddParameter(cmd, "@infoOnly", SqlType.TinyInt).Value = 0;
            var messageParam = PipelineDBProcedureExecutor.AddParameter(cmd, "@message", SqlType.VarChar, 512, string.Empty, ParameterDirection.InputOutput);

            var returnCodeParam = PipelineDBProcedureExecutor.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.InputOutput);

            // Execute the Stored Procedure (retry the call, up to 3 times)
            var resCode = PipelineDBProcedureExecutor.ExecuteSP(cmd);

            var returnCode = DBToolsBase.GetReturnCode(returnCodeParam);

            if (resCode == 0 && returnCode == 0)
            {
                return;
            }

            if (resCode != 0 && returnCode == 0)
            {
                LogError("ExecuteSP() reported result code {0} calling stored procedure {1}", resCode, SP_NAME_REPORT_IDLE);
                return;
            }

            var outputMessage = messageParam.Value.CastDBVal<string>();
            var message = string.IsNullOrWhiteSpace(outputMessage) ? "Unknown error" : outputMessage;

            LogError("Stored procedure {0} reported return code {1}, message: {2}",
                SP_NAME_REPORT_IDLE, returnCodeParam.Value.CastDBVal<string>(), message);
        }

        /// <summary>
        /// Communicates with database to perform job closeOut
        /// </summary>
        /// <remarks>evalCode and EvalMsg not presently used</remarks>
        /// <param name="compCode">Integer version of enum CloseOutType specifying the completion code</param>
        /// <param name="compMsg">Comment to insert in database</param>
        /// <param name="evalCode">Integer results evaluation code</param>
        /// <param name="evalMsg">Message describing evaluation results</param>
        /// <returns>True if success, false if an error</returns>
        protected bool SetAnalysisJobComplete(int compCode, string compMsg, int evalCode, string evalMsg)
        {
            if (Global.OfflineMode)
            {
                throw new Exception("SetAnalysisJobComplete should not be called when offline mode is enabled");
            }

            compMsg ??= string.Empty;

            evalMsg ??= string.Empty;

            // Setup for execution of stored procedure set_step_task_complete
            var cmd = PipelineDBProcedureExecutor.CreateCommand(SP_NAME_SET_COMPLETE, CommandType.StoredProcedure);

            var job = GetJobParameter(STEP_PARAMETERS_SECTION, "Job", 0);

            PipelineDBProcedureExecutor.AddParameter(cmd, "@job", SqlType.Int).Value = job;
            PipelineDBProcedureExecutor.AddParameter(cmd, "@step", SqlType.Int).Value = GetJobParameter(STEP_PARAMETERS_SECTION, "Step", 0);
            PipelineDBProcedureExecutor.AddParameter(cmd, "@completionCode", SqlType.Int).Value = compCode;
            PipelineDBProcedureExecutor.AddParameter(cmd, "@completionMessage", SqlType.VarChar, 512, compMsg.Trim('\r', '\n'));
            PipelineDBProcedureExecutor.AddParameter(cmd, "@evaluationCode", SqlType.Int).Value = evalCode;
            PipelineDBProcedureExecutor.AddParameter(cmd, "@evaluationMessage", SqlType.VarChar, 512, evalMsg.Trim('\r', '\n'));
            var returnCodeParam = PipelineDBProcedureExecutor.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.InputOutput);

            if (!TryGetParam(PEPTIDE_SEARCH_SECTION, AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME, out var orgDbName))
            {
                orgDbName = string.Empty;
            }
            PipelineDBProcedureExecutor.AddParameter(cmd, "@organismDBName", SqlType.VarChar, 128, orgDbName);

            if (!TryGetParam(STEP_PARAMETERS_SECTION, RemoteTransferUtility.STEP_PARAM_REMOTE_INFO, out var remoteInfo, false))
            {
                remoteInfo = string.Empty;
            }
            PipelineDBProcedureExecutor.AddParameter(cmd, "@remoteInfo", SqlType.VarChar, 900, remoteInfo);

            // Note: leave remoteTimestampParam.Value as null if job parameter RemoteTimestamp is empty
            if (TryGetParam(STEP_PARAMETERS_SECTION, RemoteTransferUtility.STEP_PARAM_REMOTE_TIMESTAMP, out var remoteTimestamp, false) &&
                string.IsNullOrWhiteSpace(remoteTimestamp))
            {
                remoteTimestamp = null;
            }
            PipelineDBProcedureExecutor.AddParameter(cmd, "@remoteTimestamp", SqlType.VarChar, 24, remoteTimestamp);

            // Note: leave remoteProgressParam.Value as null if job parameter RemoteProgress is empty
            object remoteProgress = null;

            if (TryGetParam(STEP_PARAMETERS_SECTION, RemoteTransferUtility.STEP_PARAM_REMOTE_PROGRESS, out var remoteProgressText, false))
            {
                remoteProgress = Global.CSngSafe(remoteProgressText, 0);
            }
            PipelineDBProcedureExecutor.AddParameter(cmd, "@remoteProgress", SqlType.Real).Value = remoteProgress;

            // Note: leave remoteStartParam.Value as null if job parameter RemoteStart is empty
            object remoteStart = null;

            if (TryGetParam(STEP_PARAMETERS_SECTION, RemoteTransferUtility.STEP_PARAM_REMOTE_START, out var remoteStartText, false))
            {
                // remoteStartText should be UTC-based
                if (DateTime.TryParse(remoteStartText, out var remoteStartDt))
                    remoteStart = remoteStartDt;
            }
            PipelineDBProcedureExecutor.AddParameter(cmd, "@remoteStart", SqlType.DateTime).Value = remoteStart;

            // Note: leave remoteFinishParam.Value as null if job parameter RemoteFinish is empty
            object remoteFinish = null;

            if (TryGetParam(STEP_PARAMETERS_SECTION, RemoteTransferUtility.STEP_PARAM_REMOTE_FINISH, out var remoteFinishText, false))
            {
                // remoteFinishText should be UTC-based
                if (DateTime.TryParse(remoteFinishText, out var remoteFinishDt))
                    remoteFinish = remoteFinishDt;
            }
            PipelineDBProcedureExecutor.AddParameter(cmd, "@remoteFinish", SqlType.DateTime).Value = remoteFinish;

            PipelineDBProcedureExecutor.AddParameter(cmd, "@processorName", SqlType.VarChar, 128, ManagerName);

            var messageParam = PipelineDBProcedureExecutor.AddParameter(cmd, "@message", SqlType.VarChar, 512, string.Empty, ParameterDirection.InputOutput);

            // Call Stored Procedure set_step_task_complete (retry the call, up to 20 times)
            var resCode = PipelineDBProcedureExecutor.ExecuteSP(cmd, 20);

            var returnCode = DBToolsBase.GetReturnCode(returnCodeParam);

            if (resCode == 0 && returnCode == 0)
            {
                return true;
            }

            var errorMessage = resCode != 0 && returnCode == 0
                ? string.Format("ExecuteSP() reported result code {0} setting analysis job complete, job {1}", resCode, job)
                : string.Format(
                    "Stored procedure {0} reported return code {1}, job {2}",
                    SP_NAME_SET_COMPLETE, returnCodeParam.Value.CastDBVal<string>(), job);

            var messageDetails = messageParam.Value?.CastDBVal<string>();

            if (!string.IsNullOrWhiteSpace(messageDetails))
            {
                LogError(errorMessage + ": " + messageDetails);
            }
            else
            {
                LogError(errorMessage);
            }

            return false;
        }

        /// <summary>
        /// Uses the "ToolName" and "StepTool" entries in mJobParamsTable to generate the tool name for the current analysis job
        /// Example tool names are "SEQUEST, Step 3" or "DTA_Gen (SEQUEST), Step 1" or "DataExtractor (XTandem), Step 4"
        /// </summary>
        /// <returns>Tool name and step number</returns>
        public string GetCurrentJobToolDescription()
        {
            // The ToolName job parameter holds the name of the job script we are executing
            var scriptName = GetParam("ToolName");

            var stepTool = GetParam("StepTool");

            var stepNumber = GetParam(STEP_PARAMETERS_SECTION, "Step");

            return GetJobToolDescription(scriptName, stepTool, stepNumber);
        }

        /// <summary>
        /// Generate a description of the tool (aka Pipeline Script), step tool, and optionally the step number for the current analysis job
        /// Example tool names are "SEQUEST, Step 3" or "DTA_Gen (SEQUEST), Step 1" or "DataExtractor (XTandem), Step 4"
        /// </summary>
        /// <param name="toolName">Tool name</param>
        /// <param name="stepTool">Step tool name (allowed to be equivalent to toolName, or blank)</param>
        /// <param name="stepNumber">Step number (if blank, step number is not included)</param>
        /// <returns>Tool name, possibly including step tool name, and optionally including step number</returns>
        public static string GetJobToolDescription(string toolName, string stepTool, string stepNumber)
        {
            string toolAndStepTool;

            if (!string.IsNullOrWhiteSpace(stepTool))
            {
                if (string.IsNullOrWhiteSpace(toolName) || string.Equals(stepTool, toolName))
                    toolAndStepTool = stepTool;
                else
                    toolAndStepTool = stepTool + " (" + toolName + ")";
            }
            else
            {
                toolAndStepTool = toolName;
            }

            if (!string.IsNullOrWhiteSpace(stepNumber))
            {
                return toolAndStepTool + ", Step " + stepNumber;
            }

            return toolAndStepTool;
        }
    }
}
