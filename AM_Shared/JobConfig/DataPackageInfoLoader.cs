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
        public IDBTools DBTools { get; }

        /// <summary>
        /// Data package ID
        /// </summary>
        public int DataPackageID { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="callingClass">Calling class instance</param>
        /// <param name="dbTools">DB tools instance</param>
        /// <param name="dataPackageID">Data package ID</param>
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
        /// However, if there is only one dataset in dataPackageDatasets, the experiment name of the dataset will be used
        /// </remarks>
        /// <returns>Dictionary where keys are experiment group name and values are dataset ID</returns>
        public static SortedDictionary<string, SortedSet<int>> GetDataPackageDatasetsByExperimentGroup(
            Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets)
        {
            // Keys in this dictionary are experiment group name (ignoring case); values are a list of dataset IDs
            // If a dataset does not have an experiment group name, it will be assigned to __UNDEFINED_EXPERIMENT_GROUP__
            // However, if there is only one dataset in dataPackageDatasets, the experiment name of the dataset will be used
            var datasetIDsByExperimentGroup = new SortedDictionary<string, SortedSet<int>>(StringComparer.OrdinalIgnoreCase);

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
        /// <param name="errorMessage">Output: error message</param>
        /// <param name="logErrors">Log errors if true (default)</param>
        /// <returns>True if a data package is defined and it has datasets associated with it</returns>
        public bool LoadDataPackageDatasetInfo(
            out Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets,
            out string errorMessage,
            bool logErrors = true)
        {
            if (DataPackageID <= 0)
            {
                dataPackageDatasets = new Dictionary<int, DataPackageDatasetInfo>();
                errorMessage = string.Empty;
                return false;
            }

            return LoadDataPackageDatasetInfo(mCallingClass, DBTools, DataPackageID, out dataPackageDatasets, out errorMessage, logErrors);
        }

        /// <summary>
        /// Looks up dataset information for a data package
        /// </summary>
        /// <param name="callingClass">Analysis resources or analysis tools class</param>
        /// <param name="dbTools">Instance of IDbTools</param>
        /// <param name="dataPackageID">Data Package ID</param>
        /// <param name="dataPackageDatasets">Output: datasets associated with the given data package; keys are DatasetID</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <param name="logErrors">Log errors if true (default)</param>
        /// <returns>True if a data package is defined, and it has datasets associated with it; false if an error (including if the data package has a placeholder dataset)</returns>
        public static bool LoadDataPackageDatasetInfo(
            AnalysisMgrBase callingClass,
            IDBTools dbTools,
            int dataPackageID,
            out Dictionary<int, DataPackageDatasetInfo> dataPackageDatasets,
            out string errorMessage,
            bool logErrors = true)
        {
            dataPackageDatasets = new Dictionary<int, DataPackageDatasetInfo>();

            var placeholderDatasetMatcher = new Regex(@"^DataPackage_\d+", RegexOptions.IgnoreCase | RegexOptions.Compiled);

            var sqlStr = new StringBuilder();

            // Query view sw.v_dms_data_package_datasets                              (on SQL Server, view V_DMS_Data_Package_Datasets in the DMS_Pipeline database)
            // That view references view dpkg.v_dms_data_package_aggregation_datasets (on SQL Server, view V_DMS_Data_Package_Aggregation_Datasets in the DMS_Data_Package database)

            sqlStr.Append("SELECT dataset, dataset_id, instrument_name, instrument_group, package_comment,");
            sqlStr.Append("       experiment, experiment_reason, experiment_comment, organism,");
            sqlStr.Append("       experiment_newt_id, experiment_newt_name, experiment_tissue_id, experiment_tissue_name,");
            sqlStr.Append("       dataset_folder_path, archive_folder_path, dataset_type, raw_data_type ");
            sqlStr.Append("FROM V_DMS_Data_Package_Datasets ");
            sqlStr.Append("WHERE data_pkg_id = " + dataPackageID + " ");
            sqlStr.Append("ORDER BY dataset");

            var success = dbTools.GetQueryResultsDataTable(sqlStr.ToString(), out var resultSet);

            if (!success)
            {
                errorMessage = "LoadDataPackageDatasetInfo: Excessive failures attempting to retrieve data package dataset info from database";

                if (logErrors)
                {
                    callingClass.LogError(errorMessage);
                }

                return false;
            }

            // Verify at least one row returned
            if (resultSet.Rows.Count < 1)
            {
                // No data was returned
                errorMessage = "LoadDataPackageDatasetInfo: No datasets were found for data package " + dataPackageID;

                if (logErrors)
                {
                    callingClass.LogWarning(errorMessage);
                }

                return false;
            }

            // For MSFragger, these parameters are in section "Philosopher"
            // For FragPipe, these parameters are in section "FragPipe"
            var autoDefineExperimentGroupWithDatasetName = callingClass.JobParams.GetJobParameter("AutoDefineExperimentGroupWithDatasetName", false);
            var autoDefineExperimentGroupWithExperimentName = callingClass.JobParams.GetJobParameter("AutoDefineExperimentGroupWithExperimentName", false);

            // The ToolName job parameter holds the name of the job script we are executing
            var scriptName = callingClass.JobParams.GetJobParameter("ToolName", "MSFragger or FragPipe");

            if (autoDefineExperimentGroupWithDatasetName)
            {
                // Auto defining FragPipe experiment group names using dataset names
                // Auto defining MSFragger experiment group names using dataset names
                callingClass.LogMessage("Auto defining {0} experiment group names using dataset names", scriptName);
            }
            else if (autoDefineExperimentGroupWithExperimentName)
            {
                // Auto defining FragPipe experiment group names using experiment names
                // Auto defining MSFragger experiment group names using experiment names
                callingClass.LogMessage("Auto defining {0} experiment group names using experiment names", scriptName);
            }

            // ReSharper disable once NotAccessedVariable
            var autoDefinedExperimentGroupCount = 0;
            var customNameExperimentGroupCount = 0;

            foreach (DataRow curRow in resultSet.Rows)
            {
                var datasetInfo = ParseDataPackageDatasetInfoRow(curRow, out var isMaxQuant);

                if (placeholderDatasetMatcher.IsMatch(datasetInfo.Dataset))
                {
                    // ReSharper disable once CommentTypo

                    // Somebody added a data package placeholder dataset to the data package (e.g. DataPackage_3442_PlexedPiperTestData)
                    // See also https://dms2.pnl.gov/data_package_dataset/report/-/StartsWith__DataPackage/-/-/-/-

                    errorMessage = string.Format("Data package {0} contains a data package placeholder dataset, which is not allowed; remove dataset {1} from the data package", dataPackageID, datasetInfo.Dataset);

                    if (logErrors)
                    {
                        callingClass.LogError(errorMessage);
                    }

                    return false;
                }

                if (string.IsNullOrWhiteSpace(datasetInfo.DatasetExperimentGroup) || isMaxQuant && int.TryParse(datasetInfo.DatasetExperimentGroup, out _))
                {
                    // The data package did not have a custom experiment group name defined in the dataset comment of the data package
                    // Or, the dataset experiment group is an integer, which indicates a MaxQuant Parameter Group (described at https://prismwiki.pnl.gov/wiki/MaxQuant#MaxQuant_Parameter_Groups)

                    // Optionally use the dataset name or experiment name for Dataset Experiment Group
                    if (autoDefineExperimentGroupWithDatasetName)
                    {
                        datasetInfo.DatasetExperimentGroup = datasetInfo.Dataset;
                        autoDefinedExperimentGroupCount++;
                    }
                    else if (autoDefineExperimentGroupWithExperimentName)
                    {
                        datasetInfo.DatasetExperimentGroup = datasetInfo.Experiment;
                        autoDefinedExperimentGroupCount++;
                    }
                }
                else if (!string.IsNullOrWhiteSpace(datasetInfo.DatasetExperimentGroup))
                {
                    customNameExperimentGroupCount++;
                }

                if (!dataPackageDatasets.ContainsKey(datasetInfo.DatasetID))
                {
                    dataPackageDatasets.Add(datasetInfo.DatasetID, datasetInfo);
                }
            }

            if (dataPackageDatasets.Count == 0)
            {
                errorMessage = string.Format("No datasets were found for data package ID {0} using view V_DMS_Data_Package_Datasets", dataPackageID);

                if (logErrors)
                {
                    callingClass.LogError(errorMessage);
                }

                return false;
            }

            if (customNameExperimentGroupCount == 0)
            {
                errorMessage = string.Empty;
                return true;
            }

            if (dataPackageDatasets.Count == 1)
            {
                // ReSharper disable once StringLiteralTypo
                callingClass.LogMessage(string.Format("Dataset ID {0} had a custom experiment group defined in the dataset's 'Package Comment' field", dataPackageDatasets[0].DatasetID));
                errorMessage = string.Empty;
                return true;
            }

            var datasetDescription = customNameExperimentGroupCount == dataPackageDatasets.Count
                ? string.Format("All {0} datasets", dataPackageDatasets.Count)
                : string.Format("{0} / {1} datasets", customNameExperimentGroupCount, dataPackageDatasets.Count);

            // ReSharper disable once StringLiteralTypo
            callingClass.LogMessage("{0} had a custom experiment group defined in the dataset's 'Package Comment' field", datasetDescription);

            errorMessage = string.Empty;
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
        /// <param name="dataPackageJobs">Output: dictionary tracking jobs associated with the given data package; keys are job numbers, values are job info</param>
        /// <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
        public static bool LoadDataPackageJobInfo(
            IDBTools dbTools,
            int dataPackageID,
            out Dictionary<int, DataPackageJobInfo> dataPackageJobs)
        {
            dataPackageJobs = new Dictionary<int, DataPackageJobInfo>();

            var sqlStr = new StringBuilder();

            // Note that this queries view sw.v_dms_data_package_aggregation_jobs (on SQL Server, view V_DMS_Data_Package_Aggregation_Jobs in the DMS_Pipeline database)
            // That view references view dpkg.v_dms_data_package_aggregation_jobs (on SQL Server, view V_DMS_Data_Package_Aggregation_Jobs in the DMS_Data_Package database)
            // The two views have the same name, but some columns differ

            // Jobs that have more than one job step with a shared results folder will have multiple rows in view V_DMS_Data_Package_Aggregation_Jobs
            // Order by Step ascending, since the SharedResultsFolders list is processed in reverse (last item first)

            sqlStr.Append("SELECT job, dataset, dataset_id, instrument_name, instrument_group,");
            sqlStr.Append("       experiment, experiment_reason, experiment_comment, organism, experiment_newt_id, experiment_newt_name,");
            sqlStr.Append("       tool, result_type, settings_file_name, parameter_file_name,");
            sqlStr.Append("       organism_db_name, protein_collection_list, protein_options,");
            sqlStr.Append("       server_storage_path, archive_storage_path, results_folder, dataset_folder,");
            sqlStr.Append("       step, shared_results_folder, raw_data_type ");
            sqlStr.Append("FROM V_DMS_Data_Package_Aggregation_Jobs ");
            sqlStr.Append("WHERE data_pkg_id = " + dataPackageID + " ");
            sqlStr.Append("ORDER BY dataset, tool, job, step");

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

                // Use V_DMS_Data_Package_Datasets in the DMS_Pipeline database to count the number of datasets in the data package

                sqlStr.Clear();
                sqlStr.Append("SELECT Count(*) AS datasets ");
                sqlStr.Append("FROM V_DMS_Data_Package_Datasets ");
                sqlStr.Append("WHERE data_pkg_id = " + dataPackageID);

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

                if (dataPackageJobs.TryGetValue(dataPkgJob.Job, out var existingPkgJob))
                {
                    // Existing job; append an additional shared results folder

                    foreach (var sharedResultsFolder in dataPkgJob.SharedResultsFolders)
                    {
                        if (existingPkgJob.SharedResultsFolders.Contains(sharedResultsFolder))
                            continue;

                        existingPkgJob.SharedResultsFolders.Add(sharedResultsFolder);
                    }
                }
                else
                {
                    dataPackageJobs.Add(dataPkgJob.Job, dataPkgJob);
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
        /// <param name="jobParameters">Output: Dictionary of job parameters where keys are parameter names (section names are ignored)</param>
        /// <param name="errorMsg">Output: error message</param>
        /// <returns>True if success, false if an error</returns>
        private static bool LookupJobParametersFromHistory(
            IDBTools dbTools,
            int jobNumber,
            out Dictionary<string, string> jobParameters,
            out string errorMsg)
        {
            jobParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Throttle the calls to this method to avoid overloading the database for data packages with hundreds of jobs
            while (DateTime.UtcNow.Subtract(mLastJobParameterFromHistoryLookup).TotalMilliseconds < 50)
            {
                AppUtils.SleepMilliseconds(25);
            }

            mLastJobParameterFromHistoryLookup = DateTime.UtcNow;

            try
            {
                DataTable resultSet;
                const int stepNumber = 1;

                if (dbTools.DbServerType == DbServerTypes.PostgreSQL)
                {
                    // Query function sw.get_job_step_params_as_table_use_history()

                    var sqlStr = string.Format(
                        "SELECT Section, Name, Value FROM sw.get_job_step_params_as_table_use_history({0}, {1})",
                        jobNumber, stepNumber);

                    dbTools.GetQueryResultsDataTable(sqlStr, out resultSet);
                }
                else
                {
                    // Call stored procedure get_job_step_params_as_table_use_history

                    var cmd = dbTools.CreateCommand("get_job_step_params_as_table_use_history", CommandType.StoredProcedure);

                    dbTools.AddParameter(cmd, "@job", SqlType.Int).Value = jobNumber;
                    dbTools.AddParameter(cmd, "@step", SqlType.Int).Value = stepNumber;

                    // Call the procedure
                    var resCode = dbTools.ExecuteSPDataTable(cmd, out resultSet);

                    if (resCode != 0)
                    {
                        errorMsg = "Unable to retrieve job parameters from history for job " + jobNumber;
                        return false;
                    }
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

                errorMsg = string.Empty;
                return true;
            }
            catch (Exception ex)
            {
                errorMsg = "Exception retrieving parameters from history for job " + jobNumber;
                LogTools.LogError(errorMsg, ex);
                return false;
            }
        }

        private static DataPackageDatasetInfo ParseDataPackageDatasetInfoRow(DataRow curRow, out bool isMaxQuant)
        {
            var datasetName = curRow["dataset"].CastDBVal<string>();
            var datasetId = curRow["dataset_id"].CastDBVal<int>();

            // Look for an Experiment Group name in the data package comment for a dataset
            // This only applies to MSFragger jobs, but could be in the MaxQuant parameter group ID format

            var packageComment = curRow["package_comment"].CastDBVal<string>();

            // Examine the comment to look for "MSFragger Group GroupName" (or similar)
            // Example comments:
            //   MSFragger Group CohortA
            //   MSFragger Group 1
            //   MSFrag Group CohortA
            //   MSFrag Group 10
            //   FragPipe Group CohortA
            //   FragPipe Group 5

            // Also match MaxQuant prefixes, in case the same data package is used for both MSFragger and MaxQuant
            // Example comments:
            //   MaxQuant Group CohortA
            //   MaxQuant Group 5
            //   Maxq Group: CohortA
            //   Maxq Group: 5
            //   MQ Group CohortA
            //   MQ Group 5
            var experimentGroupMatcher = new Regex("(?<PrefixName>MSFragger|MSFrag|FragPipe|MaxQuant|Maxq|MQ)[_ ]*Group[_ :]+(?<GroupName>[a-z0-9][a-z0-9_-]*)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var match1 = experimentGroupMatcher.Match(packageComment);

            string datasetExperimentGroup;

            bool isMaxQuantA;

            if (match1.Success)
            {
                var prefixName = match1.Groups["PrefixName"].Value;
                var groupNameOrId = match1.Groups["GroupName"].Value;

                isMaxQuantA = prefixName.StartsWith("MaxQ", StringComparison.OrdinalIgnoreCase) ||
                              prefixName.StartsWith("MQ", StringComparison.OrdinalIgnoreCase);

                if (isMaxQuantA && int.TryParse(groupNameOrId, out _))
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
                // Repeat the search but look for "Experiment" instead of "Group"
                // This will match
                //   MSFragger Experiment CohortA
                //   MaxQuant Experiment CohortA

                var experimentNameMatcher = new Regex("(?<PrefixName>MSFragger|MSFrag|FragPipe|MaxQuant|Maxq|MQ)[_ ]*Experiment[_ :]+(?<GroupName>[a-z0-9][a-z0-9_-]*)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var match2 = experimentNameMatcher.Match(packageComment);

                if (match2.Success)
                {
                    var prefixName = match2.Groups["PrefixName"].Value;

                    datasetExperimentGroup = match2.Groups["GroupName"].Value;

                    isMaxQuantA = prefixName.StartsWith("MaxQ", StringComparison.OrdinalIgnoreCase) ||
                                  prefixName.StartsWith("MQ", StringComparison.OrdinalIgnoreCase);
                }
                else
                {
                    datasetExperimentGroup = string.Empty;
                    isMaxQuantA = false;
                }
            }

            // Examine the comment to look for MaxQuant parameter groups (must be numeric)
            // Parameter groups are most commonly used to group datasets when using label-free quantitation (LFQ).
            // Datasets grouped together will be normalized together.

            // Example comments:
            //   MaxQuant Group 1
            //   Maxq Group: 3
            //   MQ Group 10
            var maxQuantGroupMatcher = new Regex(@"(?<PrefixName>MaxQuant|Maxq|MQ)[_ ]*Group[_ :]*(?<GroupIndex>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var match3 = maxQuantGroupMatcher.Match(packageComment);

            int paramGroupIndexOrNumber;
            bool isMaxQuantB;

            if (match3.Success)
            {
                var prefixName = match3.Groups["PrefixName"].Value;

                isMaxQuantB = prefixName.StartsWith("MaxQ", StringComparison.OrdinalIgnoreCase) ||
                              prefixName.StartsWith("MQ", StringComparison.OrdinalIgnoreCase);

                paramGroupIndexOrNumber = int.Parse(match3.Groups["GroupIndex"].Value);
            }
            else
            {
                paramGroupIndexOrNumber = 0;
                isMaxQuantB = false;
            }

            // Examine the comment to look for MaxQuant fraction numbers (must be numeric)
            // Fraction numbers are used during Match Between Runs to determine which datasets to examine to find additional PSMs
            // From the documentation:
            //   Fraction 1 will be matched with all fractions 1 and 2
            //   Fraction 2 will be matched against all fractions 1, 2, and 3
            //   Fraction 3 will be matched against all fractions 2, 3, and 4

            //   Furthermore, if only the fractions of one sample are to be matched against each other,
            //   but not to the fractions of another sample, introduce gaps between the different groups, e.g.:
            //   - Use fractions  1,  2,  3, etc. for the first sample group
            //   - Use fractions 11, 12, 13, etc. for the second sample group
            //   - Use fractions 21, 22, 23, etc. for the third sample group

            // Example comments:
            //   MaxQuant Fraction 1
            //   Maxq Fraction: 3
            //   MQ Fraction 10
            var maxQuantFractionMatcher = new Regex(@"(?<PrefixName>MaxQuant|Maxq|MQ)[_ ]*Fraction[_ :]*(?<FractionNumber>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var match4 = maxQuantFractionMatcher.Match(packageComment);

            int fractionNumber;
            bool isMaxQuantC;

            if (match4.Success)
            {
                var prefixName = match4.Groups["PrefixName"].Value;

                isMaxQuantC = prefixName.StartsWith("MaxQ", StringComparison.OrdinalIgnoreCase) ||
                              prefixName.StartsWith("MQ", StringComparison.OrdinalIgnoreCase);

                fractionNumber = int.Parse(match4.Groups["FractionNumber"].Value);
            }
            else
            {
                fractionNumber = 0;
                isMaxQuantC = false;
            }

            isMaxQuant = isMaxQuantA || isMaxQuantB || isMaxQuantC;

            return new DataPackageDatasetInfo(datasetName, datasetId)
            {
                Instrument = curRow["instrument_name"].CastDBVal<string>(),
                InstrumentGroup = curRow["instrument_group"].CastDBVal<string>(),
                Experiment = curRow["experiment"].CastDBVal<string>(),
                Experiment_Reason = curRow["experiment_reason"].CastDBVal<string>(),
                Experiment_Comment = curRow["experiment_comment"].CastDBVal<string>(),
                Experiment_Organism = curRow["organism"].CastDBVal<string>(),
                Experiment_Tissue_ID = curRow["experiment_tissue_id"].CastDBVal<string>(),
                Experiment_Tissue_Name = curRow["experiment_tissue_name"].CastDBVal<string>(),
                Experiment_NEWT_ID = curRow["experiment_newt_id"].CastDBVal<int>(),
                Experiment_NEWT_Name = curRow["experiment_newt_name"].CastDBVal<string>(),
                DatasetDirectoryPath = curRow["dataset_folder_path"].CastDBVal<string>(),
                DatasetArchivePath = curRow["archive_folder_path"].CastDBVal<string>(),
                DatasetType = curRow["dataset_type"].CastDBVal<string>(),
                RawDataType = curRow["raw_data_type"].CastDBVal<string>(),
                DataPackageComment = packageComment,
                DatasetExperimentGroup = datasetExperimentGroup,
                MaxQuantParamGroup = paramGroupIndexOrNumber,
                MaxQuantFractionNumber = fractionNumber
            };
        }

        /// <summary>
        /// Parse results from V_DMS_Data_Package_Aggregation_Jobs
        /// or from
        /// </summary>
        /// <param name="curRow">Current row</param>
        public static DataPackageJobInfo ParseDataPackageJobInfoRow(DataRow curRow)
        {
            var dataPkgJob = curRow["job"].CastDBVal<int>();
            var dataPkgDataset = curRow["dataset"].CastDBVal<string>();

            var jobInfo = new DataPackageJobInfo(dataPkgJob, dataPkgDataset)
            {
                DatasetID = curRow["dataset_id"].CastDBVal<int>(),
                Instrument = curRow["instrument_name"].CastDBVal<string>(),
                InstrumentGroup = curRow["instrument_group"].CastDBVal<string>(),
                Experiment = curRow["experiment"].CastDBVal<string>(),
                Experiment_Reason = curRow["experiment_reason"].CastDBVal<string>(),
                Experiment_Comment = curRow["experiment_comment"].CastDBVal<string>(),
                Experiment_Organism = curRow["organism"].CastDBVal<string>(),
                Experiment_NEWT_ID = curRow["experiment_newt_id"].CastDBVal<int>(),
                Experiment_NEWT_Name = curRow["experiment_newt_name"].CastDBVal<string>(),
                Tool = curRow["tool"].CastDBVal<string>(),
                ResultType = curRow["result_type"].CastDBVal<string>()
            };

            jobInfo.PeptideHitResultType = ReaderFactory.GetPeptideHitResultType(jobInfo.ResultType);
            jobInfo.SettingsFileName = curRow["settings_file_name"].CastDBVal<string>();
            jobInfo.ParameterFileName = curRow["parameter_file_name"].CastDBVal<string>();
            jobInfo.LegacyFastaFileName = curRow["organism_db_name"].CastDBVal<string>();
            jobInfo.ProteinCollectionList = curRow["protein_collection_list"].CastDBVal<string>();
            jobInfo.ProteinOptions = curRow["protein_options"].CastDBVal<string>();

            // This will be updated later for SplitFasta jobs (using method LookupJobParametersFromHistory)
            jobInfo.NumberOfClonedSteps = 0;

            jobInfo.ServerStoragePath = curRow["server_storage_path"].CastDBVal<string>();
            jobInfo.ArchiveStoragePath = curRow["archive_storage_path"].CastDBVal<string>();
            jobInfo.ResultsFolderName = curRow["results_folder"].CastDBVal<string>();
            jobInfo.DatasetFolderName = curRow["dataset_folder"].CastDBVal<string>();

            var sharedResultsFolder = curRow["shared_results_folder"].CastDBVal<string>();

            if (!string.IsNullOrWhiteSpace(sharedResultsFolder))
            {
                jobInfo.SharedResultsFolders.Add(sharedResultsFolder);
            }

            jobInfo.RawDataType = curRow["raw_data_type"].CastDBVal<string>();

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
            // Keys is job number, value is an instance of DataPackageJobInfo
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
