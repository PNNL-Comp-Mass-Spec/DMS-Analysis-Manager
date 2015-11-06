/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 10/05/2015                                           **
**                                                              **
*****************************************************************/

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.XPath;
using AnalysisManagerBase;

namespace AnalysisManagerQCARTPlugin
{
    public class clsAnalysisResourcesQCART : clsAnalysisResources
    {
        public const string JOB_PARAMETER_QCART_BASELINE_DATASET_NAMES_AND_JOBS = "QC-ART_Baseline_Dataset_NamesAndJobs";
        
        public const string JOB_PARAMETER_QCART_BASELINE_RESULTS_CACHE_FOLDER = "QC-ART_Baseline_Results_Cache_Folder_Path";

        public const string JOB_PARAMETER_QCART_BASELINE_METADATA_FILENAME = "QC-ART_Baseline_Metadata_File_Name";
        public const string JOB_PARAMETER_QCART_BASELINE_METADATA_LOCKFILE = "QC-ART_Baseline_Metadata_LockFilePath";

        // Existing baseline results data file name
        public const string JOB_PARAMETER_QCART_BASELINE_RESULTS_FILENAME = "QC-ART_Baseline_Results_File_Name";

        public const string JOB_PARAMETER_QCART_PROJECT_NAME = "QC-ART_Project_Name";

        public const string DATASET_METRIC_FILE_NAME = "DatasetMetricFile.txt";
        public const string REPORT_IONS_FILE_SUFFIX = "_ReporterIons.txt";

        private string mProjectName;

        /// <summary>
        /// Constructor
        /// </summary>
        public clsAnalysisResourcesQCART()
        {
            mProjectName = string.Empty;
        }

        public override IJobParams.CloseOutType GetResources()
        {

            var currentTask = "Initializing";

            mProjectName = string.Empty;

            try
            {
                // Retrieve the parameter file
                currentTask = "Retrieve the parameter file";
                var paramFileName = m_jobParams.GetParam("ParmFileName");
                var paramFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");

                var success = RetrieveFile(paramFileName, paramFileStoragePath);
                if (!success)
                {
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // Parse the parameter file to discover the baseline datasets
                currentTask = "Read the parameter file";

                // Keys are dataset names; values are MASIC job numbers
                Dictionary<string, int> baselineDatasets;

                // The unique key is a MD5 hash of the dataset names, appended with JobFirst_JobLast
                string baselineMetadataKey;

                var paramFilePathRemote = Path.Combine(paramFileStoragePath, paramFileName);
                var paramFilePathLocal = Path.Combine(m_WorkingDir, paramFileName);

                success = ParseQCARTParamFile(paramFilePathLocal, out baselineDatasets, out baselineMetadataKey);
                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                        LogError("ParseQCARTParamFile returned false (unknown error)");

                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // Look for an existing QC-ART baseline results file for the given set of datasets
                // If one is not found, we need to obtain the necessary files for the 
                // baseline datasets so that the QC-ART tool can compute a new baseline

                currentTask = "Get existing baseline results";
                
                string baselineMetadataFilePath;
                bool criticalError;

                var baselineResultsFound = FindBaselineResults(paramFilePathRemote, baselineMetadataKey, out baselineMetadataFilePath, out criticalError);
                if (criticalError)
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;

                if (!baselineResultsFound)
                {
                    // Create a lock file indicating that this manager will be creating the baseline results data file
                    var lockFilePath = CreateLockFile(baselineMetadataFilePath, "Creating QCART baseline data via " + m_MgrName);

                    m_jobParams.AddAdditionalParameter("JobParameters", JOB_PARAMETER_QCART_BASELINE_METADATA_LOCKFILE, lockFilePath);
                    
                }
                // This list contains the dataset names for which we need to obtain QC Metric values (P_2C, MS1_2B, etc.)
                var datasetNamesToRetrieveMectrics = new List<string>
                {
                    m_DatasetName
                };

                // Cache the current dataset and job info
                var udtCurrentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

                currentTask = "Retrieve " + REPORT_IONS_FILE_SUFFIX + " file for " + m_DatasetName;

                success = RetrieveReporterIonsFile(udtCurrentDatasetAndJobInfo);
                if (!success)
                {
                    return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (!baselineResultsFound)
                {
                    currentTask = "Retrieve " + REPORT_IONS_FILE_SUFFIX + " files for the baseline datasets";

                    success = RetrieveDataForBaselineDatasets(paramFileName, baselineDatasets);
                    if (!success)
                        return IJobParams.CloseOutType.CLOSEOUT_FAILED;

                    // Restore the dataset and job info using udtCurrentDatasetAndJobInfo
                    OverrideCurrentDatasetAndJobInfo(udtCurrentDatasetAndJobInfo);

                    datasetNamesToRetrieveMectrics.AddRange(baselineDatasets.Keys);
                }

                currentTask = "Process the MyEMSL download queue";

                success = ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders);
                if (!success)
                {
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the baseline dataset names and Masic Jobs so they can be used by clsAnalysisToolRunnerQCART
                StorePackedJobParameterDictionary(baselineDatasets, JOB_PARAMETER_QCART_BASELINE_DATASET_NAMES_AND_JOBS);

                currentTask = "Retrieve QC Metrics from DMS";
                success = RetrieveQCMetricsFromDB(datasetNamesToRetrieveMectrics);
                if (!success)
                {
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;

            }
            catch (Exception ex)
            {
                LogError("Exception in GetResources; task = " + currentTask, ex);
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Extract the SCX fraction number from the dataset name, which must itself be in a canonical format
        /// </summary>
        /// <param name="datasetName"></param>
        /// <returns>Fraction number on success; 0 if an error</returns>
        /// <remarks>
        /// Supported formated #1, based on TEDDY_DISCOVERY_SET_34_23_20Oct15_Frodo_15-08-15
        ///   Look for "_Set_\d+_\d+_"
        ///   SCX fraction is the second number
        /// </remarks>
        private static int ExtractFractionFromDatasetName(string datasetName)
        {
            var reSCXMatcher = new Regex(@"_SET_\d+_(\d+)_", RegexOptions.IgnoreCase);

            var match = reSCXMatcher.Match(datasetName);

            if (!match.Success)
                return 0;

            var scxFraction = int.Parse(match.Groups[1].Value);
            return scxFraction;

        }

        /// <summary>
        /// Look for existing baseline results
        /// </summary>
        /// <param name="paramFilePath">Parameter file path</param>
        /// <param name="baselineMetadataKey">Baseline metadata file unique key</param>
        /// <param name="baselineMetadataFilePath">Output: baseline metadata file path (remote path in the cache folder)</param>
        /// <param name="criticalError">Output: true if a critical error occurred and the job should be aborted</param>
        /// <returns>True if existing results were found and successfully copied locally; otherwise false</returns>
        /// <remarks>Also uses mProjectName and baselineDatasets</remarks>
        private bool FindBaselineResults(
            string paramFilePath, 
            string baselineMetadataKey, 
            out string baselineMetadataFilePath, 
            out bool criticalError)
        {
            baselineMetadataFilePath = String.Empty;
            criticalError = false;
            var currentTask = "Initializing";

            try
            {
                string errorMessage;

                // Find the parameter file in the remote location
                currentTask = "Finding parameter file";
                var fiParamFile = new FileInfo(paramFilePath);
                if (!fiParamFile.Exists)
                {
                    errorMessage = "Parameter file not found";
                    LogError(errorMessage, errorMessage + ": " + paramFilePath);
                    criticalError = true;
                    return false;
                }

                if (fiParamFile.Directory == null)
                {
                    errorMessage = "Parameter file directory is null";
                    LogError(errorMessage, errorMessage + ": " + paramFilePath);
                    criticalError = true;
                    return false;
                }

                currentTask = "Finding cache folder";
                var diCacheFolder = new DirectoryInfo(Path.Combine(fiParamFile.Directory.FullName, "Cache"));

                if (!diCacheFolder.Exists)
                {
                    // Create the folder (we'll use it later)
                    diCacheFolder.Create();
                    return false;
                }

                currentTask = "Finding project-specific cache folder";
                var diProjectFolder = new DirectoryInfo(Path.Combine(diCacheFolder.FullName, mProjectName));

                if (!diProjectFolder.Exists)
                {
                    // Create the folder (we'll use it later)
                    diProjectFolder.Create();
                    return false;
                }

                baselineMetadataFilePath = GetBaselineMetadataFilePath(diProjectFolder.FullName, baselineMetadataKey);
                var baselineMetadataFileName = Path.GetFileName(baselineMetadataFilePath);

                m_jobParams.AddAdditionalParameter("JobParameters", JOB_PARAMETER_QCART_BASELINE_RESULTS_CACHE_FOLDER, diProjectFolder.FullName);

                m_jobParams.AddAdditionalParameter("JobParameters", JOB_PARAMETER_QCART_BASELINE_METADATA_FILENAME, baselineMetadataFileName);


                currentTask = "Looking for a QCART baseline metadata lock file";

                // Look for a QCART_Cache lock file in the Project folder
                // Wait up to 1 hour for an existing lock file to be deleted or age
                CheckForLockFile(baselineMetadataFilePath, "QCART baseline metadata file", m_StatusTools, 60);

                // Now check for an existing baseline results file
                currentTask = "Looking for " + baselineMetadataFilePath;

                var fiBaselineMetadata = new FileInfo(baselineMetadataFilePath);

                if (!fiBaselineMetadata.Exists)
                {
                    // Valid baseline results metadata file was not found
                    return false;
                }

                currentTask = "Retrieving cached results";

                var success = RetrieveExistingBaselineResultFile(fiBaselineMetadata, out criticalError);
                return success;

            }
            catch (Exception ex)
            {
                LogError("Exception in FindBaselineResults: " + currentTask, ex);
                criticalError = true;
                return false;
            }

        }

        public static string GetBaselineMetadataFilePath(string outputFolderPath, string baselineMetadataKey)
        {
            var baselineMetadataFileName = "QCART_Cache_" + baselineMetadataKey + ".xml";
            var baselineMetadataPath = Path.Combine(outputFolderPath, baselineMetadataFileName);

            return baselineMetadataPath;
        }

        /// <summary>
        /// Log an error message with one of these formats:
        ///   Could not do X for A
        ///   Could not do X for A or B
        ///   Could not do X for A or 3 other datsets
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <param name="datasetNames"></param>
        private void LogErrorForOneOrMoreDatasets(string errorMessage, ICollection<string> datasetNames)
        {

            if (datasetNames.Count == 1)
                LogError(errorMessage + " for " + datasetNames.FirstOrDefault());
            else
            {
                if (datasetNames.Count == 2)
                    LogError(errorMessage + " for " + datasetNames.FirstOrDefault() + " or " + datasetNames.LastOrDefault());
                else
                    LogError(errorMessage + " for " + datasetNames.FirstOrDefault() + " or the other " + (datasetNames.Count - 1) + " datasets");
            }
        }

        /// <summary>
        /// Parse the QC-ART parameter file
        /// </summary>
        /// <param name="paramFilePath">Parameter file path</param>
        /// <param name="baselineDatasets">Output: List of baseline datasets (dataset name and dataset ID)</param>
        /// <param name="baselineMetadataKey">Output: baseline file unique key</param>
        /// <returns></returns>
        private bool ParseQCARTParamFile(string paramFilePath, out Dictionary<string, int> baselineDatasets, out string baselineMetadataKey)
        {
            const string NO_BASELINE_INFO = "One or more baseline datasets not found in the QC-ART parameter file; " +
                                            "expected at <Parameters><BaselineList><BaselineDataset>";

            baselineDatasets = new Dictionary<string, int>();
            baselineMetadataKey = string.Empty;

            try
            {
                var paramFile = new XPathDocument(new FileStream(paramFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));
                var contents = paramFile.CreateNavigator();

                var projectNode = contents.Select("/Parameters/Metadata/Project");
                if (!projectNode.MoveNext())
                {
                    LogError("Project node not found in the QC-ART parameter file; " +
                             "expected at <Parameters><Metadata><Project>");
                    return false;
                }

                mProjectName = projectNode.Current.Value;
                if (string.IsNullOrWhiteSpace(mProjectName))
                {
                    mProjectName = "Unknown";
                }

                m_jobParams.AddAdditionalParameter("JobParameters", JOB_PARAMETER_QCART_PROJECT_NAME, mProjectName);

                var baselineDatasetEntries = contents.Select("/Parameters/BaselineList/BaselineDataset");
                if (baselineDatasetEntries.Count == 0)
                {
                    LogError(NO_BASELINE_INFO);
                    return false;
                }

                while (baselineDatasetEntries.MoveNext())
                {
                    if (baselineDatasetEntries.Current.HasChildren)
                    {
                        var masicJob = baselineDatasetEntries.Current.SelectChildren("MasicJob", string.Empty);
                        var datasetName = baselineDatasetEntries.Current.SelectChildren("Dataset", string.Empty);

                        if (masicJob.MoveNext() && datasetName.MoveNext())
                        {
                            baselineDatasets.Add(datasetName.Current.Value, masicJob.Current.ValueAsInt);
                            continue;
                        }
                    }

                    LogError("Nodes MasicJob and Dataset not found beneath a BaselineDataset node " +
                             "in the QC-ART parameter file");
                    return false;
                }

                if (baselineDatasets.Count == 0)
                {
                    LogError(NO_BASELINE_INFO);
                    return false;
                }


                // Compute the MD5 Hash of the datasets and jobs in baselineDatasets
                
                var sbTextToHash = new StringBuilder();
                var query = (from item in baselineDatasets orderby item.Key select item);

                foreach (var item in query)
                {
                    sbTextToHash.Append(item.Key);
                    sbTextToHash.Append(item.Value);
                }

                // Hash contents of this stream
                // We will use the first 8 characters of the hash as a uniquifier for the baseline unique key
                var md5Hash = clsGlobal.ComputeStringHashMD5(sbTextToHash.ToString());

                // Key format: FirstJob_LastJob_DatasetCount_FirstEightCharsFromHash
                baselineMetadataKey = baselineDatasets.Values.Min() + "_" + baselineDatasets.Values.Max() + "_" + baselineDatasets.Count + "_" + md5Hash.Substring(0, 8);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in ParseQCARTParamFile", ex);
                return false;
            }

        }

        /// <summary>
        /// Retrieve the MASIC _ReporterIons.txt file for the baseline datasets
        /// </summary>
        /// <param name="paramFileName">Parameter file name</param>
        /// <param name="baselineDatasets">List of baseline datasets (dataset name and dataset ID)</param>
        /// <returns>True if success; otherwise false</returns>
        private bool RetrieveDataForBaselineDatasets(string paramFileName, Dictionary<string, int> baselineDatasets)
        {

            foreach (var datasetJobInfo in baselineDatasets)
            {
                var baselineDatasetName = datasetJobInfo.Key;
                var baselineDatasetJob = datasetJobInfo.Value;
                udtDataPackageJobInfoType udtJobInfo;

                if (!LookupJobInfo(baselineDatasetJob, out udtJobInfo))
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                    {
                        LogError("Unknown error retrieving the details for baseline job " + baselineDatasetJob);
                    }

                    return false;
                }

                if (!clsGlobal.IsMatch(udtJobInfo.Dataset, baselineDatasetName))
                {
                    var logMessage = "Mismatch in the QC-ART parameter file (" + paramFileName + "); " +
                                     "baseline job " + baselineDatasetJob + " is not dataset " + baselineDatasetName;

                    LogError(logMessage, logMessage + "; it is actually dataset " + udtJobInfo.Dataset);

                    return false;
                }

                OverrideCurrentDatasetAndJobInfo(udtJobInfo);

                RetrieveReporterIonsFile(udtJobInfo);
            }

            return true;

        }

        /// <summary>
        /// Parse the baseline results metadata file to determine the baseline results filename then copy it locally
        /// </summary>
        /// <param name="fiBaselineMetadata"></param>
        /// <param name="criticalError"></param>
        /// <returns>True if the baseline results were successfully copied locally; otherwise false</returns>
        private bool RetrieveExistingBaselineResultFile(FileInfo fiBaselineMetadata, out bool criticalError)
        {

            criticalError = false;

            try
            {
                if (fiBaselineMetadata.Directory == null)
                {
                    var warningMessage = "QC-ART baseline metadata file directory is null";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, warningMessage);
                    return false;
                }

                var paramFile = new XPathDocument(new FileStream(fiBaselineMetadata.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));
                var contents = paramFile.CreateNavigator();

                var projectNode = contents.Select("/Parameters/Results/BaselineResultsDataFile");
                if (!projectNode.MoveNext())
                {
                    var warningMessage = "BaselineResultsDataFile node not found in the QC-ART baseline results metadata file; " +
                                         "expected at <Parameters><Results><BaselineResultsDataFile>";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, warningMessage);
                    return false;
                }

                var baselineResultsDataFileName = projectNode.Current.Value;
                if (string.IsNullOrWhiteSpace(baselineResultsDataFileName))
                {
                    var warningMessage = "BaselineResultsDataFile node is empty in the QC-ART baseline results metadata file";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, warningMessage);
                    return false;
                }

                var baselineResultsFilePath = Path.Combine(fiBaselineMetadata.Directory.FullName, baselineResultsDataFileName);

                var fiBaselineResultsFileSource = new FileInfo(baselineResultsFilePath);

                if (!fiBaselineResultsFileSource.Exists || fiBaselineResultsFileSource.Directory == null)
                {
                    var warningMessage = "QC-ART baseline results data file not found: " + baselineResultsFilePath;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, warningMessage);

                    warningMessage = "Deleting invalid QC-ART baseline metadata file: " + fiBaselineMetadata.FullName;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, warningMessage);

                    fiBaselineMetadata.Delete();
                    return false;
                }

                var success = CopyFileToWorkDir(fiBaselineResultsFileSource.Name, fiBaselineResultsFileSource.Directory.FullName, m_WorkingDir);

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                        LogError("Unknown error retrieving " + fiBaselineResultsFileSource.Name);

                    criticalError = true;
                    return false;
                }

                m_jobParams.AddAdditionalParameter("JobParameters",
                                                   JOB_PARAMETER_QCART_BASELINE_RESULTS_FILENAME,
                                                   fiBaselineResultsFileSource.Name);

                m_jobParams.AddResultFileToSkip(fiBaselineResultsFileSource.Name);

                return true;

            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveExistingBaselineResultFile", ex);
                criticalError = true;
                return false;
            }
        }

        /// <summary>
        /// Query the database for specific QC Metrics required by QC-ART
        /// </summary>
        /// <param name="datasetNamesToRetrieveMectrics"></param>
        /// <returns>True if success, otherwise an error</returns>
        private bool RetrieveQCMetricsFromDB(ICollection<string> datasetNamesToRetrieveMectrics)
        {
            const int RETRY_COUNT = 3;
            const string DATASET_COLUMN = "Dataset";

            try
            {
                var datasetParseErrors = new List<string>();
                var datasetsMatched = new List<string>();

                if (datasetNamesToRetrieveMectrics.Count == 0)
                {
                    LogError("datasetNamesToRetrieveMectrics is empty");
                    return false;
                }

                var metricNames = new List<string>
                {
                    "P_2C",
                    "MS1_2B",
                    "RT_MS_Q1",
                    "RT_MS_Q4",
                    "RT_MSMS_Q1",
                    "RT_MSMS_Q4"
                };

                var sqlStr = new StringBuilder();

                sqlStr.Append("SELECT Dataset_ID, Acq_Time_Start AS StartTime, Dataset_Rating AS Rating, " + DATASET_COLUMN + ", ");
                sqlStr.Append(string.Join(", ", metricNames) + " ");
                sqlStr.Append("FROM V_Dataset_QC_Metrics_Export ");
                sqlStr.Append("WHERE Dataset IN (");

                sqlStr.Append("'" + string.Join("', '", datasetNamesToRetrieveMectrics) + "'");
                sqlStr.Append(")");

                DataTable resultSet;

                // Gigasax.DMS5
                var dmsConnectionString = m_mgrParams.GetParam("connectionstring");

                // Get a table to hold the results of the query
                var success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), dmsConnectionString, "RetrieveQCMetricsFromDB", RETRY_COUNT, out resultSet);

                string errorMessage;

                if (!success)
                {
                    m_message = "Excessive failures attempting to retrieve QC metric data from database";
                    errorMessage = "RetrieveQCMetricsFromDB; " + m_message;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage);
                    resultSet.Dispose();
                    return false;
                }

                // Verify at least one row returned
                if (resultSet.Rows.Count < 1)
                {
                    // No data was returned
                    errorMessage = "QC Metrics not found";
                    LogErrorForOneOrMoreDatasets(errorMessage, datasetNamesToRetrieveMectrics);

                    return false;
                }

                var datasetAndMetricFilePath = Path.Combine(m_WorkingDir, DATASET_METRIC_FILE_NAME);
                using (var writer = new StreamWriter(new FileStream(datasetAndMetricFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Write the header line
                    var headers = (from DataColumn item in resultSet.Columns select item.ColumnName).ToList();

                    // Add the SCX fraction header
                    headers.Add("SCX_Fraction");

                    writer.WriteLine(clsGlobal.CollapseList(headers));

                    foreach (DataRow row in resultSet.Rows)
                    {
                        var dataValues = new List<string>();

                        foreach (var dataVal in row.ItemArray)
                        {
                            dataValues.Add(clsGlobal.DbCStr(dataVal));
                        }

                        var datasetName = row[DATASET_COLUMN].ToString();

                        // Parse the dataset name to determine the SCX Fraction
                        var fractionNumber = ExtractFractionFromDatasetName(datasetName);

                        if (fractionNumber < 0)
                        {
                            datasetParseErrors.Add(datasetName);
                        }

                        dataValues.Add(fractionNumber.ToString());

                        if (datasetsMatched.Contains(datasetName, StringComparer.CurrentCultureIgnoreCase))
                        {
                            // Dataset already defined
                            // Unexpected, but skip it
                            continue;
                        }

                        datasetsMatched.Add(datasetName);
                        writer.WriteLine(clsGlobal.CollapseList(dataValues));
                    }
                }

                if (datasetParseErrors.Count == 0)
                {
                    // No errors; confirm that all of the datasets were found
                    if (datasetsMatched.Count == datasetNamesToRetrieveMectrics.Count)
                    {
                        return true;
                    }

                    var missingDatasets = datasetNamesToRetrieveMectrics.Except(datasetsMatched).ToList();

                    errorMessage = "QC metrics not found in V_Dataset_QC_Metrics_Export";
                    LogErrorForOneOrMoreDatasets(errorMessage, missingDatasets);

                    return false;
                }

                // SCX fraction parse errors

                errorMessage = "Could not determine SCX fraction number";
                LogErrorForOneOrMoreDatasets(errorMessage, datasetParseErrors);

                return false;

            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveQCMetricsFromDB", ex);
                return false;
            }

        }

        /// <summary>
        /// Retrieve the MASIC _ReporterIons.txt file
        /// for the dataset and job in udtJobInfo
        /// </summary>
        /// <param name="udtJobInfo"></param>
        /// <returns></returns>
        private bool RetrieveReporterIonsFile(udtDataPackageJobInfoType udtJobInfo)
        {
            try
            {

                var targetDatasetName = m_jobParams.GetJobParameter("SourceJob2Dataset", string.Empty);
                var targetDatasetMasicJob = m_jobParams.GetJobParameter("SourceJob2", 0);

                if (string.IsNullOrWhiteSpace(targetDatasetName) || targetDatasetMasicJob == 0)
                {
                    var baseMessage = "Job parameters SourceJob2Dataset and SourceJob2 not found; populated via the [Special Processing] job parameter";
                    LogError(baseMessage, baseMessage + "; for example 'SourceJob:Auto{Tool = \"SMAQC_MSMS\"}, Job2:Auto{Tool = \"MASIC_Finnigan\"}'");
                    return false;
                }

                var inputFolderNameCached = m_jobParams.GetJobParameter("JobParameters", "inputFolderName", string.Empty);

                if (clsGlobal.IsMatch(udtJobInfo.Dataset, targetDatasetName) && m_JobNum == udtJobInfo.Job)
                {
                    // Retrieving the _ReporterIons.txt file for the dataset associated with this QC-ART job
                    var masicFolderPath = m_jobParams.GetJobParameter("SourceJob2FolderPath", string.Empty);

                    if (string.IsNullOrWhiteSpace(masicFolderPath))
                    {
                        var baseMessage = "Job parameter SourceJob2FolderPath not found; populated via the [Special Processing] job parameter";
                        LogError(baseMessage, baseMessage + "; for example 'SourceJob:Auto{Tool = \"SMAQC_MSMS\"}, Job2:Auto{Tool = \"MASIC_Finnigan\"}'");
                        return false;
                    }

                    // Override inputFolderName
                    m_jobParams.AddAdditionalParameter("JobParameters", "inputFolderName", Path.GetFileName(masicFolderPath));
                }
                else
                {
                    // Baseline dataset
                    m_jobParams.AddAdditionalParameter("JobParameters", "inputFolderName", udtJobInfo.ResultsFolderName);
                }

                var reporterIonsFileName = m_DatasetName + REPORT_IONS_FILE_SUFFIX;

                var success = FindAndRetrieveMiscFiles(reporterIonsFileName, false);
                if (!success)
                {
                    LogError(REPORT_IONS_FILE_SUFFIX + " file not found for dataset " + udtJobInfo.Dataset + ", job " + udtJobInfo.Job);
                    return false;
                }

                m_jobParams.AddResultFileToSkip(reporterIonsFileName);

                // Restore the inputFolderName
                m_jobParams.AddAdditionalParameter("JobParameters", "inputFolderName", inputFolderNameCached);

                return true;

            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveReporterIonsFile", ex);
                return false;
            }

        }

    }
}
