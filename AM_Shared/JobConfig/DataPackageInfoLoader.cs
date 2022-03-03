using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.StatusReporting;
using PHRPReader;
using PRISM;
using PRISM.Logging;
using PRISMDatabaseUtils;

namespace AnalysisManagerBase.JobConfig
{
    // Ignore Spelling: Maxq

    /// <summary>
    /// Data package info loader
    /// </summary>
    public sealed class DataPackageInfoLoader : LoggerBase
    {
        // Ignore Spelling: quantitation

        /// <summary>
        /// Experiment group name to use when an experiment group is not defined in the data package comment of a dataset in a data package
        /// </summary>
        public const string UNDEFINED_EXPERIMENT_GROUP = "__UNDEFINED_EXPERIMENT_GROUP__";

        private static DateTime mLastJobParameterFromHistoryLookup = DateTime.UtcNow;

        private readonly AnalysisMgrBase mCallingClass;

        /// <summary>
        /// Instance of IDBTools
        /// </summary>
        /// <remarks>Typically Gigasax.DMS_Pipeline</remarks>
        public IDBTools DBTools { get; }

        /// <summary>
        /// Data package ID
        /// </summary>
        public int DataPackageID { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="callingClass"></param>
        /// <param name="dbTools"></param>
        /// <param name="dataPackageID"></param>
        public DataPackageInfoLoader(AnalysisMgrBase callingClass, IDBTools dbTools, int dataPackageID)
        {
            DBTools = dbTools;
            DataPackageID = dataPackageID;
            mCallingClass = callingClass;
        }

        /// <summary>
        /// Group the datasets in this data package by experiment group name (MSFragger experiment group)
        /// </summary>
        /// <remarks>
        /// Datasets that do not have an experiment group defined will be assigned to __UNDEFINED_EXPERIMENT_GROUP__
        /// </remarks>
        /// <returns>Dictionary where keys are experiment group name and values are dataset ID</returns>
        public static SortedDictionary<string, SortedSet<int>> GetDataPackageDatasetsByExperimentGroup(
            Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets)
        {
            // Keys in this dictionary are experiment group name; values are a list of dataset IDs
            // If a dataset does not have an experiment group name, it will be assigned to __UNDEFINED_EXPERIMENT_GROUP__
            var datasetIDsByExperimentGroup = new SortedDictionary<string, SortedSet<int>>();

            foreach (var item in dataPackageDatasets)
            {
                var datasetId = item.Key;
                var datasetInfo = item.Value;

                var experimentGroup = datasetInfo.DatasetExperimentGroup;

                if (string.IsNullOrWhiteSpace(experimentGroup) && dataPackageDatasets.Count == 1)
                {
                    var experimentName = datasetInfo.Experiment;

                    var singleDatasetGroup = new SortedSet<int>
                    {
                        datasetId
                    };

                    datasetIDsByExperimentGroup.Add(experimentName, singleDatasetGroup);
                    continue;
                }

                var experimentGroupToUse = string.IsNullOrWhiteSpace(experimentGroup) ? UNDEFINED_EXPERIMENT_GROUP : experimentGroup;

                if (datasetIDsByExperimentGroup.TryGetValue(experimentGroupToUse, out var matchedDatasetsForGroup))
                {
                    matchedDatasetsForGroup.Add(datasetId);
                    continue;
                }

                var datasetsForGroup = new SortedSet<int>
                {
                    datasetId
                };

                datasetIDsByExperimentGroup.Add(experimentGroupToUse, datasetsForGroup);
            }

            return datasetIDsByExperimentGroup;
        }

        /// <summary>
        /// Looks up dataset information for a data package
        /// </summary>
        /// <param name="dataPackageDatasets">Datasets associated with the given data package; keys are DatasetID</param>
        /// <returns>True if a data package is defined and it has datasets associated with it</returns>
        public bool LoadDataPackageDatasetInfo(out Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets)
        {
            if (DataPackageID <= 0)
            {
                dataPackageDatasets = new Dictionary<int, DataPackageDatasetInfo>();
                return false;
            }

            return LoadDataPackageDatasetInfo(mJobParams, DBTools, DataPackageID, out dataPackageDatasets);
        }

        /// <summary>
        /// Looks up dataset information for a data package
        /// </summary>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="dbTools">Instance of IDbTools</param>
        /// <param name="dataPackageID">Data Package ID</param>
        /// <param name="dataPackageDatasets">Datasets associated with the given data package; keys are DatasetID</param>
        /// <returns>True if a data package is defined and it has datasets associated with it</returns>
        public static bool LoadDataPackageDatasetInfo(
            IJobParams jobParams,
            IDBTools dbTools,
            int dataPackageID,
            out Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets)
        {
            dataPackageDatasets = new Dictionary<int, DataPackageDatasetInfo>();

            var sqlStr = new StringBuilder();

            // Note that this queries view V_DMS_Data_Package_Datasets in the DMS_Pipeline database
            // That view references   view V_DMS_Data_Package_Aggregation_Datasets in the DMS_Data_Package database

            sqlStr.Append(" SELECT Dataset, DatasetID, Instrument, InstrumentGroup, PackageComment, ");
            sqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism,");
            sqlStr.Append("        Experiment_NEWT_ID, Experiment_NEWT_Name, Experiment_Tissue_ID, Experiment_Tissue_Name,");
            sqlStr.Append("        Dataset_Folder_Path, Archive_Folder_Path, RawDataType");
            sqlStr.Append(" FROM V_DMS_Data_Package_Datasets");
            sqlStr.Append(" WHERE Data_Package_ID = " + dataPackageID);
            sqlStr.Append(" ORDER BY Dataset");

            var success = dbTools.GetQueryResultsDataTable(sqlStr.ToString(), out var resultSet);

            if (!success)
            {
                const string errorMessage = "LoadDataPackageDatasetInfo: Excessive failures attempting to retrieve data package dataset info from database";
                LogTools.LogError(errorMessage);
                return false;
            }

            // Verify at least one row returned
            if (resultSet.Rows.Count < 1)
            {
                // No data was returned
                var warningMessage = "LoadDataPackageDatasetInfo: No datasets were found for data package " + dataPackageID;
                LogTools.LogWarning(warningMessage);
                return false;
            }

            var autoDefineExperimentGroupWithDatasetName = jobParams.GetJobParameter("Philosopher", "AutoDefineExperimentGroupWithDatasetName", false);
            var autoDefineExperimentGroupWithExperimentName = jobParams.GetJobParameter("Philosopher", "AutoDefineExperimentGroupWithExperimentName", false);

            foreach (DataRow curRow in resultSet.Rows)
            {
                var datasetInfo = ParseDataPackageDatasetInfoRow(curRow);

                if (autoDefineExperimentGroupWithDatasetName)
                {
                    datasetInfo.DatasetExperimentGroup = datasetInfo.Dataset;
                }
                else if (autoDefineExperimentGroupWithExperimentName)
                {
                    datasetInfo.DatasetExperimentGroup = datasetInfo.Experiment;
                }

                if (!dataPackageDatasets.ContainsKey(datasetInfo.DatasetID))
                {
                    dataPackageDatasets.Add(datasetInfo.DatasetID, datasetInfo);
                }
            }

            return true;
        }

        /// <summary>
        /// Looks up job information for the given data package
        /// </summary>
        /// <remarks>
        /// Property NumberOfClonedSteps is not updated for the analysis jobs returned by this method
        /// In contrast, RetrieveDataPackagePeptideHitJobInfo does update NumberOfClonedSteps
        /// </remarks>
        /// <param name="dbTools">Instance of IDbTools</param>
        /// <param name="dataPackageID">Data Package ID</param>
        /// <param name="dataPackageJobs">Jobs associated with the given data package</param>
        /// <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
        public static bool LoadDataPackageJobInfo(
            IDBTools dbTools,
            int dataPackageID,
            out Dictionary<int, DataPackageJobInfo> dataPackageJobs)
        {
            dataPackageJobs = new Dictionary<int, DataPackageJobInfo>();

            var sqlStr = new StringBuilder();

            // Note that this queries view V_DMS_Data_Package_Aggregation_Jobs in the DMS_Pipeline database
            // That view references   view V_DMS_Data_Package_Aggregation_Jobs in the DMS_Data_Package database
            // The two views have the same name, but some columns differ

            // Jobs that have more than one job step with a shared results folder will have multiple rows in view V_DMS_Data_Package_Aggregation_Jobs
            // Order by Step ascending, since the SharedResultsFolders list is processed in reverse (last item first)

            sqlStr.Append(" SELECT Job, Dataset, DatasetID, Instrument, InstrumentGroup, ");
            sqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism, Experiment_NEWT_ID, Experiment_NEWT_Name, ");
            sqlStr.Append("        Tool, ResultType, SettingsFileName, ParameterFileName, ");
            sqlStr.Append("        OrganismDBName, ProteinCollectionList, ProteinOptions,");
            sqlStr.Append("        ServerStoragePath, ArchiveStoragePath, ResultsFolder, DatasetFolder,");
            sqlStr.Append("        Step, SharedResultsFolder, RawDataType");
            sqlStr.Append(" FROM V_DMS_Data_Package_Aggregation_Jobs");
            sqlStr.Append(" WHERE Data_Package_ID = " + dataPackageID);
            sqlStr.Append(" ORDER BY Dataset, Tool, Job, Step");

            var successForJobs = dbTools.GetQueryResultsDataTable(sqlStr.ToString(), out var dataPackageJobQueryResults);

            if (!successForJobs)
            {
                const string errorMessage = "LoadDataPackageJobInfo: Excessive failures attempting to retrieve data package job info from database";
                LogTools.LogError(errorMessage);
                return false;
            }

            // Verify at least one row returned
            if (dataPackageJobQueryResults.Rows.Count < 1)
            {
                // No data was returned
                string warningMessage;

                // If the data package exists and has datasets associated with it, log this as a warning but return true
                // Otherwise, log an error and return false

                sqlStr.Clear();
                sqlStr.Append(" SELECT Count(*) AS Datasets");
                sqlStr.Append(" FROM S_V_DMS_Data_Package_Aggregation_Datasets");
                sqlStr.Append(" WHERE Data_Package_ID = " + dataPackageID);

                var successForDatasets = dbTools.GetQueryResultsDataTable(sqlStr.ToString(), out var dataPackageDatasets);

                if (successForDatasets && dataPackageDatasets.Rows.Count > 0)
                {
                    foreach (DataRow curRow in dataPackageDatasets.Rows)
                    {
                        var datasetCount = curRow[0].CastDBVal<int>();

                        // ReSharper disable once InvertIf
                        if (datasetCount > 0)
                        {
                            warningMessage = string.Format(
                                "LoadDataPackageJobInfo: No jobs were found for data package {0}, " +
                                "but it does have {1} dataset{2}",
                                dataPackageID, datasetCount, datasetCount > 1 ? "s" : string.Empty);

                            LogTools.LogWarning(warningMessage);
                            return true;
                        }
                    }
                }

                warningMessage = "LoadDataPackageJobInfo: No jobs were found for data package " + dataPackageID;
                LogTools.LogError(warningMessage);
                return false;
            }

            foreach (DataRow curRow in dataPackageJobQueryResults.Rows)
            {
                var dataPkgJob = ParseDataPackageJobInfoRow(curRow);

                if (!dataPackageJobs.ContainsKey(dataPkgJob.Job))
                {
                    dataPackageJobs.Add(dataPkgJob.Job, dataPkgJob);
                }
                else
                {
                    // Existing job; append an additional SharedResultsFolder
                    var existingPkgJob = dataPackageJobs[dataPkgJob.Job];
                    foreach (var sharedResultsFolder in dataPkgJob.SharedResultsFolders)
                    {
                        if (existingPkgJob.SharedResultsFolders.Contains(sharedResultsFolder))
                            continue;

                        existingPkgJob.SharedResultsFolders.Add(sharedResultsFolder);
                    }
                }
            }

            return true;
        }

        private static void LogDebugMessage(string debugMessage)
        {
            LogTools.LogDebug(debugMessage);
        }

        /// <summary>
        /// Retrieve the job parameters from the pipeline database for the given analysis job
        /// The analysis job must have completed successfully, since the parameters
        /// are retrieved from tables T_Jobs_History, T_Job_Steps_History, and T_Job_Parameters_History
        /// </summary>
        /// <remarks>This procedure is used by AnalysisToolRunnerPRIDEConverter</remarks>
        /// <param name="dbTools">DMS_Pipeline database connection</param>
        /// <param name="jobNumber">Job number</param>
        /// <param name="jobParameters">Output parameter: Dictionary of job parameters where keys are parameter names (section names are ignored)</param>
        /// <param name="errorMsg"></param>
        /// <returns>True if success, false if an error</returns>
        private static bool LookupJobParametersFromHistory(
            IDBTools dbTools,
            int jobNumber,
            out Dictionary<string, string> jobParameters,
            out string errorMsg)
        {
            jobParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            errorMsg = string.Empty;

            // Throttle the calls to this method to avoid overloading the database for data packages with hundreds of jobs
            while (DateTime.UtcNow.Subtract(mLastJobParameterFromHistoryLookup).TotalMilliseconds < 50)
            {
                ProgRunner.SleepMilliseconds(25);
            }

            mLastJobParameterFromHistoryLookup = DateTime.UtcNow;

            try
            {
                var cmd = dbTools.CreateCommand("GetJobStepParamsAsTableUseHistory", CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@jobNumber", SqlType.Int).Value = jobNumber;
                dbTools.AddParameter(cmd, "@stepNumber", SqlType.Int).Value = 1;

                // Execute the SP
                var returnCode = dbTools.ExecuteSPDataTable(cmd, out var resultSet);
                var success = returnCode == 0;

                if (!success)
                {
                    errorMsg = "Unable to retrieve job parameters from history for job " + jobNumber;
                    return false;
                }

                // Verify at least one row returned
                if (resultSet.Rows.Count < 1)
                {
                    // No data was returned
                    // Log an error

                    errorMsg = "Historical parameters were not found for job " + jobNumber;
                    return false;
                }

                foreach (DataRow curRow in resultSet.Rows)
                {
                    // var section = curRow[0]?.CastDBVal<string>()
                    var parameter = curRow[1]?.CastDBVal<string>();
                    var value = curRow[2]?.CastDBVal<string>();

                    if (string.IsNullOrWhiteSpace(parameter))
                        continue;

                    if (jobParameters.ContainsKey(parameter))
                    {
                        LogTools.LogWarning("Job {0} has multiple values for parameter {1}; only using the first occurrence", jobNumber, parameter);
                    }
                    else
                    {
                        jobParameters.Add(parameter, value);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                errorMsg = "Exception retrieving parameters from history for job " + jobNumber;
                LogTools.LogError(errorMsg, ex);
                return false;
            }
        }

        private static DataPackageDatasetInfo ParseDataPackageDatasetInfoRow(DataRow curRow)
        {
            var datasetName = curRow["Dataset"].CastDBVal<string>();
            var datasetId = curRow["DatasetID"].CastDBVal<int>();

            // Look for an Experiment Group name in the data package comment for a dataset
            // This only applies to MSFragger jobs, but could be in the MaxQuant parameter group ID format

            var packageComment = curRow["PackageComment"].CastDBVal<string>();

            // Examine the comment to look for "MSFragger Group GroupName" (or similar)
            // Example allowed comments:
            //   MSFragger Group CohortA
            //   MSFragger Group 1
            //   MSFrag Group CohortA
            //   MSFrag Group 10
            //   FragPipe Group CohortA
            //   FragPipe Group 5

            // Also match MaxQuant prefixes, in case the same data package is used for both MSFragger and MaxQuant
            //   MaxQuant Group CohortA
            //   MaxQuant Group 5
            //   Maxq Group: CohortA
            //   Maxq Group: 5
            //   MQ Group CohortA
            //   MQ Group 5
            var experimentGroupMatcher = new Regex("(?<PrefixName>MSFragger|MSFrag|FragPipe|MaxQuant|Maxq|MQ)[_ ]*Group[_ :]+(?<GroupName>[a-z0-9][a-z0-9_-]*)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var match1 = experimentGroupMatcher.Match(packageComment);

            string datasetExperimentGroup;

            if (match1.Success)
            {
                var prefixName = match1.Groups["PrefixName"].Value;
                var groupNameOrId = match1.Groups["GroupName"].Value;

                if ((prefixName.StartsWith("MaxQ", StringComparison.OrdinalIgnoreCase) ||
                    prefixName.StartsWith("MQ", StringComparison.OrdinalIgnoreCase)) &&
                    int.TryParse(groupNameOrId, out _))
                {
                    // Matched a MaxQuant Group ID
                    // To avoid integer-based result file names, store Group1, Group2, etc.
                    datasetExperimentGroup = "Group" + groupNameOrId;
                }
                else
                {
                    datasetExperimentGroup = groupNameOrId;
                }
            }
            else
            {
                datasetExperimentGroup = string.Empty;
            }

            // Examine the comment to look for MaxQuant parameter groups (must be numeric)
            // Parameter groups are most commonly used to group datasets when using label-free quantitation (LFQ).
            // Datasets grouped together will be normalized together.

            // Example allowed comments:
            //   MaxQuant Group 1
            //   Maxq Group: 3
            //   MQ Group 10
            var maxQuantGroupMatcher = new Regex(@"(MaxQuant|Maxq|MQ)[_ ]*Group[_ :]*(?<GroupIndex>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var match2 = maxQuantGroupMatcher.Match(packageComment);

            var paramGroupIndexOrNumber = match2.Success ? int.Parse(match2.Groups["GroupIndex"].Value) : 0;

            return new DataPackageDatasetInfo(datasetName, datasetId)
            {
                Instrument = curRow["Instrument"].CastDBVal<string>(),
                InstrumentGroup = curRow["InstrumentGroup"].CastDBVal<string>(),
                Experiment = curRow["Experiment"].CastDBVal<string>(),
                Experiment_Reason = curRow["Experiment_Reason"].CastDBVal<string>(),
                Experiment_Comment = curRow["Experiment_Comment"].CastDBVal<string>(),
                Experiment_Organism = curRow["Organism"].CastDBVal<string>(),
                Experiment_Tissue_ID = curRow["Experiment_Tissue_ID"].CastDBVal<string>(),
                Experiment_Tissue_Name = curRow["Experiment_Tissue_Name"].CastDBVal<string>(),
                Experiment_NEWT_ID = curRow["Experiment_NEWT_ID"].CastDBVal<int>(),
                Experiment_NEWT_Name = curRow["Experiment_NEWT_Name"].CastDBVal<string>(),
                DatasetDirectoryPath = curRow["Dataset_Folder_Path"].CastDBVal<string>(),
                DatasetArchivePath = curRow["Archive_Folder_Path"].CastDBVal<string>(),
                RawDataType = curRow["RawDataType"].CastDBVal<string>(),
                DataPackageComment = packageComment,
                DatasetExperimentGroup = datasetExperimentGroup,
                MaxQuantParamGroup = paramGroupIndexOrNumber
            };
        }

        /// <summary>
        /// Parse results from V_DMS_Data_Package_Aggregation_Jobs
        /// or from
        /// </summary>
        /// <param name="curRow"></param>
        public static DataPackageJobInfo ParseDataPackageJobInfoRow(DataRow curRow)
        {
            var dataPkgJob = curRow["Job"].CastDBVal<int>();
            var dataPkgDataset = curRow["Dataset"].CastDBVal<string>();

            var jobInfo = new DataPackageJobInfo(dataPkgJob, dataPkgDataset)
            {
                DatasetID = curRow["DatasetID"].CastDBVal<int>(),
                Instrument = curRow["Instrument"].CastDBVal<string>(),
                InstrumentGroup = curRow["InstrumentGroup"].CastDBVal<string>(),
                Experiment = curRow["Experiment"].CastDBVal<string>(),
                Experiment_Reason = curRow["Experiment_Reason"].CastDBVal<string>(),
                Experiment_Comment = curRow["Experiment_Comment"].CastDBVal<string>(),
                Experiment_Organism = curRow["Organism"].CastDBVal<string>(),
                Experiment_NEWT_ID = curRow["Experiment_NEWT_ID"].CastDBVal<int>(),
                Experiment_NEWT_Name = curRow["Experiment_NEWT_Name"].CastDBVal<string>(),
                Tool = curRow["Tool"].CastDBVal<string>(),
                ResultType = curRow["ResultType"].CastDBVal<string>()
            };

            jobInfo.PeptideHitResultType = ReaderFactory.GetPeptideHitResultType(jobInfo.ResultType);
            jobInfo.SettingsFileName = curRow["SettingsFileName"].CastDBVal<string>();
            jobInfo.ParameterFileName = curRow["ParameterFileName"].CastDBVal<string>();
            jobInfo.LegacyFastaFileName = curRow["OrganismDBName"].CastDBVal<string>();
            jobInfo.ProteinCollectionList = curRow["ProteinCollectionList"].CastDBVal<string>();
            jobInfo.ProteinOptions = curRow["ProteinOptions"].CastDBVal<string>();

            // This will be updated later for SplitFasta jobs (using method LookupJobParametersFromHistory)
            jobInfo.NumberOfClonedSteps = 0;

            jobInfo.ServerStoragePath = curRow["ServerStoragePath"].CastDBVal<string>();
            jobInfo.ArchiveStoragePath = curRow["ArchiveStoragePath"].CastDBVal<string>();
            jobInfo.ResultsFolderName = curRow["ResultsFolder"].CastDBVal<string>();
            jobInfo.DatasetFolderName = curRow["DatasetFolder"].CastDBVal<string>();

            var sharedResultsFolder = curRow["SharedResultsFolder"].CastDBVal<string>();
            if (!string.IsNullOrWhiteSpace(sharedResultsFolder))
            {
                jobInfo.SharedResultsFolders.Add(sharedResultsFolder);
            }

            jobInfo.RawDataType = curRow["RawDataType"].CastDBVal<string>();

            return jobInfo;
        }

        /// <summary>
        /// Lookup the Peptide Hit jobs associated with the current job
        /// </summary>
        /// <param name="additionalJobs">Non Peptide Hit jobs (e.g. DeconTools or MASIC)</param>
        /// <returns>Peptide Hit Jobs (e.g. MS-GF+ or SEQUEST)</returns>
        public List<DataPackageJobInfo> RetrieveDataPackagePeptideHitJobInfo(out List<DataPackageJobInfo> additionalJobs)
        {
            if (DataPackageID <= 0)
            {
                LogError("DataPackageID is not defined for this analysis job");
                additionalJobs = new List<DataPackageJobInfo>();
                return new List<DataPackageJobInfo>();
            }

            var dataPackagePeptideHitJobs = RetrieveDataPackagePeptideHitJobInfo(DBTools, DataPackageID, out additionalJobs, out var errorMsg);

            if (!string.IsNullOrWhiteSpace(errorMsg))
            {
                LogError(errorMsg);
            }

            return dataPackagePeptideHitJobs;
        }

        /// <summary>
        /// Lookup the Peptide Hit jobs associated with the data package
        /// </summary>
        /// <remarks>Alternatively use the overloaded version that includes additionalJobs</remarks>
        /// <param name="dbTools">DMS_Pipeline database connection</param>
        /// <param name="dataPackageID">Data package ID</param>
        /// <param name="errorMsg">Output: error message</param>
        /// <returns>Peptide Hit Jobs (e.g. MS-GF+ or SEQUEST)</returns>
        // ReSharper disable once UnusedMember.Global
        public static List<DataPackageJobInfo> RetrieveDataPackagePeptideHitJobInfo(
            IDBTools dbTools,
            int dataPackageID,
            out string errorMsg)
        {
            return RetrieveDataPackagePeptideHitJobInfo(dbTools, dataPackageID, out _, out errorMsg);
        }

        /// <summary>
        /// Lookup the Peptide Hit jobs associated with the data package; non-peptide hit jobs are returned via additionalJobs
        /// </summary>
        /// <remarks>This method updates property NumberOfClonedSteps for the analysis jobs</remarks>
        /// <param name="dbTools">DMS_Pipeline database connection</param>
        /// <param name="dataPackageID">Data package ID</param>
        /// <param name="additionalJobs">Output: Non Peptide Hit jobs (e.g. DeconTools or MASIC)</param>
        /// <param name="errorMsg">Output: error message</param>
        /// <returns>Peptide Hit Jobs (e.g. MS-GF+ or SEQUEST)</returns>
        public static List<DataPackageJobInfo> RetrieveDataPackagePeptideHitJobInfo(
            IDBTools dbTools,
            int dataPackageID,
            out List<DataPackageJobInfo> additionalJobs,
            out string errorMsg)
        {
            // This list tracks the info for the Peptide Hit jobs (e.g. MS-GF+ or SEQUEST) associated with the data package
            var dataPackagePeptideHitJobs = new List<DataPackageJobInfo>();
            errorMsg = string.Empty;

            // This list tracks the info for the non Peptide Hit jobs (e.g. DeconTools or MASIC) associated with the data package
            additionalJobs = new List<DataPackageJobInfo>();

            // This dictionary will track the jobs associated with this aggregation job's data package
            // Key is job number, value is an instance of DataPackageJobInfo
            Dictionary<int, DataPackageJobInfo> dataPackageJobs;

            try
            {
                if (!LoadDataPackageJobInfo(dbTools, dataPackageID, out dataPackageJobs))
                {
                    errorMsg = "Error looking up datasets and jobs using LoadDataPackageJobInfo";
                    return dataPackagePeptideHitJobs;
                }
            }
            catch (Exception ex)
            {
                errorMsg = "Error calling LoadDataPackageJobInfo: " + ex.Message;
                return dataPackagePeptideHitJobs;
            }

            try
            {
                foreach (var kvItem in dataPackageJobs)
                {
                    if (kvItem.Value.PeptideHitResultType == PeptideHitResultTypes.Unknown)
                    {
                        additionalJobs.Add(kvItem.Value);
                    }
                    else
                    {
                        // Cache this job info in dataPackagePeptideHitJobs
                        dataPackagePeptideHitJobs.Add(kvItem.Value);
                    }
                }
            }
            catch (Exception ex)
            {
                errorMsg = "Exception determining data package jobs for this aggregation job (RetrieveDataPackagePeptideHitJobInfo): " + ex.Message;
            }

            try
            {
                // Look for any SplitFasta jobs
                // If present, we need to determine the value for job parameter NumberOfClonedSteps

                var splitFastaJobs = (from dataPkgJob in dataPackagePeptideHitJobs
                                      where dataPkgJob.Tool.IndexOf("SplitFasta", StringComparison.OrdinalIgnoreCase) >= 0
                                      select dataPkgJob).ToList();

                if (splitFastaJobs.Count > 0)
                {
                    var lastStatusTime = DateTime.UtcNow;
                    var statusIntervalSeconds = 4;
                    var jobsProcessed = 0;

                    foreach (var dataPkgJob in splitFastaJobs)
                    {
                        var success = LookupJobParametersFromHistory(dbTools, dataPkgJob.Job, out var dataPkgJobParameters, out errorMsg);

                        if (!success)
                        {
                            return new List<DataPackageJobInfo>();
                        }

                        if (dataPkgJobParameters.TryGetValue("NumberOfClonedSteps", out var numberOfClonedSteps))
                        {
                            if (int.TryParse(numberOfClonedSteps, out var clonedStepCount))
                                dataPkgJob.NumberOfClonedSteps = clonedStepCount;
                        }

                        jobsProcessed++;

                        if (DateTime.UtcNow.Subtract(lastStatusTime).TotalSeconds >= statusIntervalSeconds)
                        {
                            var pctComplete = jobsProcessed / (float)splitFastaJobs.Count * 100;
                            LogDebugMessage("Retrieving job parameters from history for SplitFasta jobs; " + pctComplete.ToString("0") + "% complete");

                            lastStatusTime = DateTime.UtcNow;

                            // Double the status interval, allowing for a maximum of 30 seconds
                            statusIntervalSeconds = Math.Min(30, statusIntervalSeconds * 2);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                errorMsg = "Error calling LookupJobParametersFromHistory (RetrieveDataPackagePeptideHitJobInfo): " + ex.Message;
                return new List<DataPackageJobInfo>();
            }

            return dataPackagePeptideHitJobs;
        }
    }
}
