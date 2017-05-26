
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/18/2007
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{

    /// <summary>
    /// Provides DB access and tools for one analysis job
    /// </summary>
    /// <remarks></remarks>
    public class clsAnalysisJob : clsDBTask, IJobParams
    {

        #region "Constants"

        public const string JOB_PARAMETERS_SECTION = "JobParameters";

        public const string STEP_PARAMETERS_SECTION = "StepParameters";

        protected const string SP_NAME_SET_COMPLETE = "SetStepTaskComplete";

        /// <summary>
        /// "RequestStepTask"
        /// </summary>
        protected const string SP_NAME_REQUEST_TASK = "RequestStepTaskXML";

        #endregion

        #region "Module variables"

        /// <summary>
        /// Job parameters
        /// The outer dictionary tracks section names, then the inner dictionary tracks key/value pairs within each section
        /// </summary>
        protected readonly Dictionary<string, Dictionary<string, string>> m_JobParams;

        /// <summary>
        /// Current job number
        /// </summary>
        protected int m_JobId;

        /// <summary>
        /// List of file names to NOT move to the result folder; this list is used by MoveResultFiles()
        /// </summary>
        protected SortedSet<string> m_ResultFilesToSkip = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase);

        /// <summary>
        /// List of file extensions (or even partial file names like _peaks.txt) to NOT move to the result folder
        /// </summary>
        /// <remarks>
        /// Comparison checks if the end of the fileName matches any entry ResultFileExtensionsToSkip:
        /// If TmpFileNameLcase.EndsWith(ext.ToLower()) Then OkToMove = False
        /// </remarks>
        protected SortedSet<string> m_ResultFileExtensionsToSkip = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase);

        /// <summary>
        /// List of file names that WILL be moved to the result folder, even if they are in ResultFilesToSkip or ResultFileExtensionsToSkip
        /// </summary>
        protected SortedSet<string> m_ResultFilesToKeep = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase);

        /// <summary>
        /// List of file path to delete from the storage server (must be full file paths)
        /// </summary>
        protected SortedSet<string> m_ServerFilesToDelete = new SortedSet<string>(StringComparer.CurrentCultureIgnoreCase);

        /// <summary>
        /// List of dataset names and dataset IDs
        /// </summary>
        protected Dictionary<string, int> m_DatasetInfoList = new Dictionary<string, int>(StringComparer.CurrentCultureIgnoreCase);

        #endregion

        #region "Properties"

        /// <summary>
        /// List of dataset names and dataset IDs associated with this aggregation job
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public Dictionary<string, int> DatasetInfoList => m_DatasetInfoList;

        /// <summary>
        /// List of file names that WILL be moved to the result folder, even if they are in ResultFilesToSkip or ResultFileExtensionsToSkip
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public SortedSet<string> ResultFilesToKeep => m_ResultFilesToKeep;

        /// <summary>
        /// List of file names to NOT move to the result folder
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public SortedSet<string> ResultFilesToSkip => m_ResultFilesToSkip;

        /// <summary>
        /// List of file extensions to NOT move to the result folder; comparison checks if the end of the fileName matches any entry in ResultFileExtensionsToSkip
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public SortedSet<string> ResultFileExtensionsToSkip => m_ResultFileExtensionsToSkip;

        /// <summary>
        /// List of file paths to remove from the storage server (full file paths)
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks>Used by clsAnalysisToolRunnerBase.RemoveNonResultServerFiles</remarks>
        public SortedSet<string> ServerFilesToDelete => m_ServerFilesToDelete;

        /// <summary>
        /// Flag set to True when .CloseTask is called
        /// </summary>
        public bool TaskClosed { get; set; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">IMgrParams object containing manager parameters</param>
        /// <param name="debugLvl">Debug level</param>
        /// <remarks></remarks>
        public clsAnalysisJob(IMgrParams mgrParams, short debugLvl) : base(mgrParams, debugLvl)
        {
            m_JobParams = new Dictionary<string, Dictionary<string, string>>(StringComparer.CurrentCultureIgnoreCase);
            Reset();
        }

        /// <summary>
        /// Adds (or updates) a job parameter
        /// </summary>
        /// <param name="sectionName">Section name for parameter</param>
        /// <param name="paramName">Name of parameter</param>
        /// <param name="paramValue">Boolean value for parameter</param>
        /// <returns>True if success, False if an error</returns>
        /// <remarks></remarks>
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
        /// <param name="paramValue">Value for parameter</param>
        /// <returns>True if success, False if an error</returns>
        /// <remarks></remarks>
        public bool AddAdditionalParameter(string sectionName, string paramName, string paramValue)
        {

            try
            {
                if (paramValue == null)
                    paramValue = String.Empty;

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
        /// <param name="datasetName"></param>
        /// <param name="datasetID"></param>
        /// <remarks></remarks>
        public void AddDatasetInfo(string datasetName, int datasetID)
        {
            if (String.IsNullOrWhiteSpace(datasetName))
                return;
            if (!m_DatasetInfoList.ContainsKey(datasetName))
            {
                m_DatasetInfoList.Add(datasetName, datasetID);
            }
        }

        /// <summary>
        /// Add a fileName extension to not move to the results folder
        /// </summary>
        /// <param name="fileExtension"></param>
        /// <remarks>Can be a file extension (like .raw) or even a partial file name like _peaks.txt</remarks>
        public void AddResultFileExtensionToSkip(string fileExtension)
        {
            if (String.IsNullOrWhiteSpace(fileExtension))
                return;

            if (!m_ResultFileExtensionsToSkip.Contains(fileExtension))
            {
                m_ResultFileExtensionsToSkip.Add(fileExtension);
            }
        }

        /// <summary>
        /// Add a fileName to definitely move to the results folder
        /// </summary>
        /// <param name="fileName"></param>
        /// <remarks></remarks>
        public void AddResultFileToKeep(string fileName)
        {
            if (String.IsNullOrWhiteSpace(fileName))
                return;

            fileName = Path.GetFileName(fileName);
            if (!m_ResultFilesToKeep.Contains(fileName))
            {
                m_ResultFilesToKeep.Add(fileName);
            }
        }

        /// <summary>
        /// Add a fileName to not move to the results folder
        /// </summary>
        /// <param name="fileName"></param>
        /// <remarks></remarks>
        public void AddResultFileToSkip(string fileName)
        {
            if (String.IsNullOrWhiteSpace(fileName))
                return;

            fileName = Path.GetFileName(fileName);
            if (!m_ResultFilesToSkip.Contains(fileName))
            {
                m_ResultFilesToSkip.Add(fileName);
            }
        }

        /// <summary>
        /// Add a file to be deleted from the storage server (requires full file path)
        /// </summary>
        /// <param name="filePath">Full path to the file</param>
        /// <remarks></remarks>
        public void AddServerFileToDelete(string filePath)
        {
            if (String.IsNullOrWhiteSpace(filePath))
                return;

            if (!m_ServerFilesToDelete.Contains(filePath))
            {
                m_ServerFilesToDelete.Add(filePath);
            }
        }

        /// <summary>
        /// Get all job parameters for the given section
        /// </summary>
        /// <returns>Dictionary where keys are parameter names and values are parameter values</returns>
        public Dictionary<string, string> GetAllParametersForSection(string sectionName)
        {
            if (m_JobParams.TryGetValue(sectionName, out var oParams))
            {
                return oParams;
            }

            return new Dictionary<string, string>();
        }

        /// <summary>
        /// Get job parameter section names
        /// </summary>
        /// <returns></returns>
        public List<string> GetAllSectionNames()
        {
            return m_JobParams.Keys.ToList();
        }

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section)
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value if missing</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        /// <remarks>If the value associated with the parameter is found, yet is not True or False, then an exception will be occur; the calling procedure must handle this exception</remarks>
        public bool GetJobParameter(string name, bool valueIfMissing)
        {

            string value;

            try
            {
                value = GetParam(name);

                if (String.IsNullOrEmpty(value))
                {
                    return valueIfMissing;
                }

            }
            catch
            {
                return valueIfMissing;
            }

            // Note: if value is not True or False, this will throw an exception; the calling procedure will need to handle that exception
            return Convert.ToBoolean(value);

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

                if (String.IsNullOrEmpty(value))
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

                if (String.IsNullOrEmpty(value))
                {
                    return valueIfMissing;
                }

            }
            catch
            {
                return valueIfMissing;
            }

            // Note: if value is not a number, this will throw an exception; the calling procedure will need to handle that exception
            return Convert.ToInt32(value);

        }

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section)
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value if missing</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public short GetJobParameter(string name, short valueIfMissing)
        {
            return (short)(GetJobParameter(name, (int)valueIfMissing));
        }

        /// <summary>
        /// Gets a job parameter with the given name (in any parameter section)
        /// </summary>
        /// <param name="name">Key name for parameter</param>
        /// <param name="valueIfMissing">Value if missing</param>
        /// <returns>Value for specified parameter; valueIfMissing if not found</returns>
        public float GetJobParameter(string name, float valueIfMissing)
        {
            return clsGlobal.CSngSafe(GetParam(name), valueIfMissing);
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
            return clsGlobal.CBoolSafe(GetParam(section, name), valueIfMissing);
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
            return clsGlobal.CIntSafe(GetParam(section, name), valueIfMissing);
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
            if (String.IsNullOrEmpty(value))
            {
                return valueIfMissing;
            }
            return value;
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
            return clsGlobal.CSngSafe(GetParam(section, name), valueIfMissing);
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
        /// <remarks></remarks>
        public string GetParam(string name)
        {

            if (TryGetParam(name, out var value))
            {
                return value;
            }
            return String.Empty;
        }

        /// <summary>
        /// Gets a job parameter with the given name, preferentially using the specified parameter section
        /// </summary>
        /// <param name="section">Section name for parameter</param>
        /// <param name="name">Key name for parameter</param>
        /// <returns>Value for specified parameter; empty string if not found</returns>
        /// <remarks></remarks>
        public string GetParam(string section, string name)
        {

            if (String.IsNullOrEmpty(name))
            {
                // User actually wanted to look for the parameter that is currently in the Section Variable, using an empty string as the default value
                return GetParam(section);
            }

            if (TryGetParam(section, name, out var value))
            {
                return value;
            }
            return String.Empty;
        }

        public static string JobParametersFilename(int jobNum)
        {
            return clsGlobal.JOB_PARAMETERS_FILE_PREFIX + jobNum + ".xml";
        }

        [Obsolete("Use the version that takex an integer")]
        public static string JobParametersFilename(string jobNum)
        {
            return clsGlobal.JOB_PARAMETERS_FILE_PREFIX + jobNum + ".xml";
        }

        /// <summary>
        /// Add/updates the value for the given parameter (searches all sections)
        /// </summary>
        /// <param name="paramName">Parameter name</param>
        /// <param name="paramValue">Parameter value</param>
        /// <remarks></remarks>
        public void SetParam(string paramName, string paramValue)
        {
            var blnMatchFound = false;

            if (paramValue == null)
                paramValue = String.Empty;

            foreach (var section in m_JobParams)
            {
                if (section.Value.ContainsKey(paramName))
                {
                    section.Value[paramName] = paramValue;
                    blnMatchFound = true;
                }
            }

            if (!blnMatchFound && m_JobParams.Count > 0)
            {
                // Add the parameter to the first section
                m_JobParams.First().Value.Add(paramName, paramValue);
            }

        }

        /// <summary>
        /// Add/updates the value for the given parameter
        /// </summary>
        /// <param name="section">Section name</param>
        /// <param name="paramName">Parameter name</param>
        /// <param name="paramValue">Parameter value</param>
        /// <remarks></remarks>
        public void SetParam(string section, string paramName, string paramValue)
        {
            if (!m_JobParams.TryGetValue(section, out var oParams))
            {
                // Need to add a section with a blank name
                oParams = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
                m_JobParams.Add(section, oParams);
            }

            if (paramValue == null)
                paramValue = String.Empty;

            if (oParams.ContainsKey(paramName))
            {
                oParams[paramName] = paramValue;
            }
            else
            {
                oParams.Add(paramName, paramValue);
            }

        }

        /// <summary>
        /// Return true if toolRunnerResult is CLOSEOUT_SUCCESS or CLOSEOUT_NO_DATA
        /// </summary>
        /// <param name="toolRunnerResult"></param>
        /// <returns></returns>
        public static bool SuccessOrNoData(CloseOutType toolRunnerResult)
        {
            if (toolRunnerResult == CloseOutType.CLOSEOUT_SUCCESS || toolRunnerResult == CloseOutType.CLOSEOUT_NO_DATA)
                return true;

            return false;
        }

        /// <summary>
        /// Attempts to retrieve the specified parameter (looks in all parameter sections)
        /// </summary>
        /// <param name="paramName">Parameter Name</param>
        /// <param name="paramValue">Output: parameter value</param>
        /// <returns>True if success, False if not found</returns>
        /// <remarks></remarks>
        public bool TryGetParam(string paramName, out string paramValue)
        {

            paramValue = String.Empty;

            foreach (var oEntry in m_JobParams)
            {
                if (oEntry.Value.TryGetValue(paramName, out paramValue))
                {
                    if (String.IsNullOrWhiteSpace(paramValue))
                    {
                        paramValue = String.Empty;
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
        /// <returns>True if success, False if not found</returns>
        /// <remarks></remarks>
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
        /// <param name="searchAllSectionsIfNotFound">If True, then searches other sections for the parameter if not found in the specified section</param>
        /// <returns>True if success, False if not found</returns>
        /// <remarks></remarks>
        public bool TryGetParam(string section, string paramName, out string paramValue, bool searchAllSectionsIfNotFound)
        {
            paramValue = String.Empty;

            if (m_JobParams.TryGetValue(section, out var oParams))
            {
                if (oParams.TryGetValue(paramName, out paramValue))
                {
                    if (String.IsNullOrWhiteSpace(paramValue))
                    {
                        paramValue = String.Empty;
                    }
                    return true;
                }
            }

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
        /// <param name="fileName"></param>
        /// <remarks></remarks>
        public void RemoveResultFileToSkip(string fileName)
        {
            if (m_ResultFilesToSkip.Contains(fileName))
            {
                m_ResultFilesToSkip.Remove(fileName);
            }

        }

        /// <summary>
        /// Filter the job parameters in paramXml to remove extra items from section sectionName
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

                var sections = doc.Elements("sections").Elements("section");
                foreach (var section in sections)
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
                            if (String.IsNullOrEmpty(requiredParameter) || paramNames.Contains(requiredParameter))
                            {
                                // Remove this parameter from this section
                                paramsToRemove.Add(paramItem);
                            }
                        }

                        if (paramsToAddAsAttribute.ContainsKey(paramName.Value))
                        {
                            var attribName = paramsToAddAsAttribute[paramName.Value];
                            if (String.IsNullOrEmpty(attribName))
                                attribName = paramName.Value;

                            var paramValue = paramItem.Attribute("value");
                            if (paramValue == null)
                            {
                                attributesToAdd.Add(attribName, String.Empty);
                            }
                            else
                            {
                                attributesToAdd.Add(attribName, paramValue.Value);
                            }

                        }
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

                var sbOutput = new StringBuilder();

                var settings = new XmlWriterSettings
                {
                    Indent = true,
                    IndentChars = "  ",
                    OmitXmlDeclaration = true
                };

                using (var writer = XmlWriter.Create(sbOutput, settings))
                {
                    doc.Save(writer);
                }

                var filteredXML = sbOutput.ToString();

                return filteredXML;

            }
            catch (Exception ex)
            {
                LogError("Error in FilterXmlSection", ex);
                return paramXml;
            }

        }

        /// <summary>
        /// Requests a task from the database
        /// </summary>
        /// <returns>Enum indicating if task was found</returns>
        /// <remarks></remarks>
        public override RequestTaskResult RequestTask()
        {

            var retVal = RequestAnalysisJob();
            switch (retVal)
            {
                case RequestTaskResult.NoTaskFound:
                    m_TaskWasAssigned = false;
                    break;
                case RequestTaskResult.TaskFound:
                    m_TaskWasAssigned = true;
                    break;
                default:
                    m_TaskWasAssigned = false;
                    break;
            }
            return retVal;

        }

        /// <summary>
        /// Requests a single analysis job using RequestStepTaskXML
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        private RequestTaskResult RequestAnalysisJob()
        {

            RequestTaskResult taskResult;

            var productVersion = clsGlobal.GetAssemblyVersion() ?? "??";

            var dotNetVersion = clsGlobal.GetDotNetVersion();

            string managerVersion;
            if (!String.IsNullOrEmpty(dotNetVersion))
            {
                if (!dotNetVersion.StartsWith("v", StringComparison.InvariantCultureIgnoreCase))
                    dotNetVersion = "v" + dotNetVersion;

                managerVersion = productVersion + "; .NET " + dotNetVersion;
            }
            else
            {
                managerVersion = productVersion + "; Unkown .NET Version";
            }

            Reset();

            try
            {
                // Set up the command object prior to SP execution
                var cmd = new SqlCommand(SP_NAME_REQUEST_TASK) { CommandType = CommandType.StoredProcedure };

                cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                cmd.Parameters.Add(new SqlParameter("@processorName", SqlDbType.VarChar, 128)).Value = m_MgrParams.GetParam("MgrName");
                cmd.Parameters.Add(new SqlParameter("@jobNumber", SqlDbType.Int)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new SqlParameter("@parameters", SqlDbType.VarChar, 8000)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new SqlParameter("@infoOnly", SqlDbType.TinyInt)).Value = 0;
                cmd.Parameters.Add(new SqlParameter("@analysisManagerVersion", SqlDbType.VarChar, 128)).Value = managerVersion;

                var remoteInfo = clsRemoteTransferUtility.GetRemoteInfoXml(m_MgrParams);
                cmd.Parameters.Add(new SqlParameter("@remoteInfo", SqlDbType.VarChar, 900)).Value = remoteInfo;

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisJob.RequestAnalysisJob(), connection string: " + m_BrokerConnStr, (int)clsLogTools.LogLevels.DEBUG);
                    LogDebug("clsAnalysisJob.RequestAnalysisJob(), printing param list", (int)clsLogTools.LogLevels.DEBUG);
                    PrintCommandParams(cmd);
                }

                // Execute the SP
                var retVal = PipelineDBProcedureExecutor.ExecuteSP(cmd, 1);

                switch (retVal)
                {
                    case RET_VAL_OK:
                        // No errors found in SP call, so see if any step tasks were found
                        m_JobId = Convert.ToInt32(cmd.Parameters["@jobNumber"].Value);
                        var jobParamsXML = Convert.ToString(cmd.Parameters["@parameters"].Value);

                        // Step task was found; get the data for it
                        var dctParameters = FillParamDictXml(jobParamsXML).ToList();

                        foreach (var udtParamInfo in dctParameters)
                        {
                            SetParam(udtParamInfo.Section, udtParamInfo.ParamName, udtParamInfo.Value);
                        }

                        SaveJobParameters(m_MgrParams.GetParam("WorkDir"), jobParamsXML, m_JobId);
                        taskResult = RequestTaskResult.TaskFound;
                        break;
                    case RET_VAL_TASK_NOT_AVAILABLE:
                        // No jobs found
                        taskResult = RequestTaskResult.NoTaskFound;
                        break;
                    case RET_VAL_EXCESSIVE_RETRIES:
                        // Too many retries
                        taskResult = RequestTaskResult.TooManyRetries;
                        break;
                    case RET_VAL_DEADLOCK:
                        // Transaction was deadlocked on lock resources with another process and has been chosen as the deadlock victim
                        taskResult = RequestTaskResult.Deadlock;
                        break;
                    default:
                        // There was an SP error
                        LogError("clsAnalysisJob.RequestAnalysisJob(), SP execution error " + retVal + "; " +
                            "Msg text = " + Convert.ToString(cmd.Parameters["@message"].Value));
                        taskResult = RequestTaskResult.ResultError;
                        break;
                }

            }
            catch (Exception ex)
            {
                LogError("Exception requesting analysis job", ex);
                taskResult = RequestTaskResult.ResultError;
            }

            return taskResult;

        }

        /// <summary>
        /// Reset the class-wide variables to their defaults
        /// </summary>
        /// <remarks></remarks>
        public void Reset()
        {
            TaskClosed = false;

            m_ResultFilesToSkip.Clear();
            m_ResultFileExtensionsToSkip.Clear();
            m_ResultFilesToKeep.Clear();
            m_ServerFilesToDelete.Clear();

            m_DatasetInfoList.Clear();

            m_JobParams.Clear();

        }

        /// <summary>
        /// Saves job Parameters to an XML File
        /// </summary>
        /// <param name="workDir">Full path to work directory</param>
        /// <param name="jobParamsXML">Contains the xml for all the job parameters</param>
        /// <param name="jobNum">Job number</param>
        /// <remarks></remarks>
        private void SaveJobParameters(string workDir, string jobParamsXML, int jobNum)
        {
            var xmlParameterFilePath = String.Empty;

            try
            {
                var xmlParameterFilename = JobParametersFilename(jobNum);
                xmlParameterFilePath = Path.Combine(workDir, xmlParameterFilename);

                var xmlParameterFile = new FileInfo(xmlParameterFilePath);

                // Keys are parameter names to ignore
                // Values are another parameter name that must be present if we're going to ignore the given parameter
                var paramNamesToIgnore = new Dictionary<string, string>
                {
                    {"SharedResultsFolders", ""},
                    {"CPU_Load", ""},
                    {"Job", ""},
                    {"Step", ""},
                    {"StepInputFolderName", "InputFolderName"},
                    {"StepOutputFolderName", "OutputFolderName"}
                };

                var paramsToAddAsAttribute = new Dictionary<string, string>
                {
                    { "Step", "step"}
                };

                // Remove extra parameters from the StepParameters section that we don't want to include in the XML
                // Also update the section to have an attribute that is the step number
                var filteredXML = FilterXmlSection(jobParamsXML, STEP_PARAMETERS_SECTION, paramNamesToIgnore, paramsToAddAsAttribute);

                var xmlWriter = new clsFormattedXMLWriter();
                xmlWriter.WriteXMLToFile(filteredXML, xmlParameterFile.FullName);

                AddAdditionalParameter(JOB_PARAMETERS_SECTION, "genJobParamsFilename", xmlParameterFilename);

                var msg = "Job Parameters successfully saved to file: " + xmlParameterFile.FullName;

                // Copy the Job Parameter file to the Analysis Manager folder so that we can inspect it if the job fails
                clsGlobal.CopyAndRenameFileWithBackup(xmlParameterFile.FullName, clsGlobal.GetAppFolderPath(), "RecentJobParameters.xml", 5);

                LogDebug(msg, (int)clsLogTools.LogLevels.DEBUG);

            }
            catch (Exception ex)
            {
                LogError("Exception saving analysis job parameters to " + xmlParameterFilePath, ex);
            }

        }

        /// <summary>
        /// Contact the Pipeline database to close the analysis job
        /// </summary>
        /// <param name="closeOut">IJobParams enum specifying close out type</param>
        /// <param name="compMsg">Completion message to be added to database upon closeOut</param>
        public override void CloseTask(CloseOutType closeOut, string compMsg)
        {
            CloseTask(closeOut, compMsg, 0, String.Empty);
        }

        /// <summary>
        /// Contact the Pipeline database to close the analysis job
        /// </summary>
        /// <param name="closeOut">IJobParams enum specifying close out type</param>
        /// <param name="compMsg">Completion message to be added to database upon closeOut</param>
        /// <param name="evalCode">Evaluation code (0 if no special evaulation message)</param>
        /// <param name="evalMsg">Evaluation message ("" if no special message)</param>
        public override void CloseTask(CloseOutType closeOut, string compMsg, int evalCode, string evalMsg)
        {
            var compCode = (int)closeOut;

            if (evalMsg == null)
                evalMsg = string.Empty;

            if (TaskClosed)
            {
                LogWarning("Job " + m_JobId + " has already been closed; will not call " + SP_NAME_SET_COMPLETE + " again");
            }
            else
            {
                TaskClosed = true;
                if (!SetAnalysisJobComplete(compCode, compMsg, evalCode, evalMsg))
                {
                    LogError("Error setting job complete in database, job " + m_JobId);
                }
            }

        }

        /// <summary>
        /// Communicates with database to perform job closeOut
        /// </summary>
        /// <param name="compCode">Integer version of ITaskParams specifying closeOut type (enum CloseOutType)</param>
        /// <param name="compMsg">Comment to insert in database</param>
        /// <param name="evalCode">Integer results evaluation code</param>
        /// <param name="evalMsg">Message describing evaluation results</param>
        /// <returns>True for success, False for failure</returns>
        /// <remarks>evalCode and EvalMsg not presently used</remarks>
        protected bool SetAnalysisJobComplete(int compCode, string compMsg, int evalCode, string evalMsg)
        {

            // Setup for execution of the stored procedure
            var cmd = new SqlCommand(SP_NAME_SET_COMPLETE) {
                CommandType = CommandType.StoredProcedure
            };

            cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
            cmd.Parameters.Add(new SqlParameter("@job", SqlDbType.Int)).Value = GetJobParameter(STEP_PARAMETERS_SECTION, "Job", 0);
            cmd.Parameters.Add(new SqlParameter("@step", SqlDbType.Int)).Value = GetJobParameter(STEP_PARAMETERS_SECTION, "Step", 0);
            cmd.Parameters.Add(new SqlParameter("@completionCode", SqlDbType.Int)).Value = compCode;
            cmd.Parameters.Add(new SqlParameter("@completionMessage", SqlDbType.VarChar, 256)).Value = compMsg;
            cmd.Parameters.Add(new SqlParameter("@evaluationCode", SqlDbType.Int)).Value = evalCode;
            cmd.Parameters.Add(new SqlParameter("@evaluationMessage", SqlDbType.VarChar, 256)).Value = evalMsg;

            var orgDbNameParam = cmd.Parameters.Add(new SqlParameter("@organismDBName", SqlDbType.VarChar, 128));

            if (TryGetParam("PeptideSearch", clsAnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME, out var orgDbName))
            {
                orgDbNameParam.Value = orgDbName;
            }
            else
            {
                orgDbNameParam.Value = String.Empty;
            }

            var remoteInfoParam = cmd.Parameters.Add(new SqlParameter("@remoteInfo", SqlDbType.VarChar, 900));
            if (TryGetParam(STEP_PARAMETERS_SECTION, clsRemoteTransferUtility.STEP_PARAM_REMOTE_INFO, out var remoteInfo, false))
            {
                remoteInfoParam.Value = remoteInfo;
            }
            else
            {
                remoteInfoParam.Value = String.Empty;
            }

            // Note: leave remoteTimestampParam.Value as null if remoteTimestamp is empty
            var remoteTimestampParam = cmd.Parameters.Add(new SqlParameter("@remoteTimestamp", SqlDbType.VarChar, 24));
            if (TryGetParam(STEP_PARAMETERS_SECTION, clsRemoteTransferUtility.STEP_PARAM_REMOTE_TIMESTAMP, out var remoteTimestamp, false))
            {
                if (!String.IsNullOrWhiteSpace(remoteTimestamp))
                    remoteTimestampParam.Value = remoteTimestamp;
            }

            // Note: leave remoteTimestampParam.Value as null if remoteTimestamp is empty
            var remoteProgressParam = cmd.Parameters.Add(new SqlParameter("@remoteProgress", SqlDbType.Real));
            if (TryGetParam(STEP_PARAMETERS_SECTION, clsRemoteTransferUtility.STEP_PARAM_REMOTE_PROGRESS, out var remoteProgressText, false))
            {
                remoteProgressParam.Value = clsGlobal.CSngSafe(remoteProgressText, 0);
            }

            // Execute the Stored Procedure (retry the call up to 20 times)
            var returnCode = PipelineDBProcedureExecutor.ExecuteSP(cmd, 20);

            if (returnCode == 0)
            {
                return true;
            }

            LogError("Error " + returnCode + " setting analysis job complete");
            return false;
        }

        /// <summary>
        /// Uses the "ToolName" and "StepTool" entries in m_JobParamsTable to generate the tool name for the current analysis job
        /// Example tool names are "Sequest, Step 3" or "DTA_Gen (Sequest), Step 1" or "DataExtractor (XTandem), Step 4"
        /// </summary>
        /// <returns>Tool name and step number</returns>
        /// <remarks></remarks>
        public string GetCurrentJobToolDescription()
        {

            var toolName = GetParam("ToolName");

            var toolAndStepTool = GetParam("StepTool");
            if (String.IsNullOrWhiteSpace(toolAndStepTool))
                toolAndStepTool = String.Empty;

            var stepNumber = GetParam(STEP_PARAMETERS_SECTION, "Step") ?? String.Empty;

            if (!String.IsNullOrWhiteSpace(toolName) && !String.Equals(toolAndStepTool, toolName))
            {
                if (toolAndStepTool.Length > 0)
                {
                    toolAndStepTool += " (" + toolName + ")";
                }
                else
                {
                    toolAndStepTool += toolName;
                }
            }

            if (stepNumber.Length > 0)
            {
                toolAndStepTool += ", Step " + stepNumber;
            }

            return toolAndStepTool;

        }

        #endregion
    }
}