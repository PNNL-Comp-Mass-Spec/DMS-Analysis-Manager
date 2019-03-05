using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using PHRPReader;
using PRISM.Logging;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Data package info loader
    /// </summary>
    public class clsDataPackageInfoLoader : clsLoggerBase
    {

        private static DateTime mLastJobParameterFromHistoryLookup = DateTime.UtcNow;

        /// <summary>
        /// Database connection string
        /// </summary>
        /// <remarks>Typically Gigasax.DMS_Pipeline</remarks>
        public string ConnectionString { get; }

        /// <summary>
        /// Data package ID
        /// </summary>
        public int DataPackageID { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="brokerDbConnectionString"></param>
        /// <param name="dataPackageID"></param>
        public clsDataPackageInfoLoader(string brokerDbConnectionString, int dataPackageID)
        {
            ConnectionString = brokerDbConnectionString;
            DataPackageID = dataPackageID;
        }

        /// <summary>
        /// Looks up dataset information for a data package
        /// </summary>
        /// <param name="dctDataPackageDatasets"></param>
        /// <returns>True if a data package is defined and it has datasets associated with it</returns>
        /// <remarks></remarks>
        public bool LoadDataPackageDatasetInfo(out Dictionary<int, clsDataPackageDatasetInfo> dctDataPackageDatasets)
        {
            if (DataPackageID < 0)
            {
                dctDataPackageDatasets = new Dictionary<int, clsDataPackageDatasetInfo>();
                return false;
            }

            return LoadDataPackageDatasetInfo(ConnectionString, DataPackageID, out dctDataPackageDatasets);
        }

        /// <summary>
        /// Looks up dataset information for a data package
        /// </summary>
        /// <param name="connectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
        /// <param name="dataPackageID">Data Package ID</param>
        /// <param name="dctDataPackageDatasets">Datasets associated with the given data package</param>
        /// <returns>True if a data package is defined and it has datasets associated with it</returns>
        /// <remarks></remarks>
        public static bool LoadDataPackageDatasetInfo(
            string connectionString,
            int dataPackageID,
            out Dictionary<int, clsDataPackageDatasetInfo> dctDataPackageDatasets)
        {

            // Obtains the dataset information for a data package
            const short RETRY_COUNT = 3;

            dctDataPackageDatasets = new Dictionary<int, clsDataPackageDatasetInfo>();

            var sqlStr = new System.Text.StringBuilder();

            // Note that this queries view V_DMS_Data_Package_Datasets in the DMS_Pipeline database
            // That view references   view V_DMS_Data_Package_Aggregation_Datasets in the DMS_Data_Package database

            sqlStr.Append(" SELECT Dataset, DatasetID, Instrument, InstrumentGroup, ");
            sqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism,");
            sqlStr.Append("        Experiment_NEWT_ID, Experiment_NEWT_Name, Experiment_Tissue_ID, Experiment_Tissue_Name,");
            sqlStr.Append("        Dataset_Folder_Path, Archive_Folder_Path, RawDataType");
            sqlStr.Append(" FROM V_DMS_Data_Package_Datasets");
            sqlStr.Append(" WHERE Data_Package_ID = " + dataPackageID);
            sqlStr.Append(" ORDER BY Dataset");


            // Get a table to hold the results of the query
            var success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), connectionString, "LoadDataPackageDatasetInfo", RETRY_COUNT, out var resultSet);

            if (!success)
            {
                var errorMessage = "LoadDataPackageDatasetInfo; Excessive failures attempting to retrieve data package dataset info from database";
                LogTools.LogError(errorMessage);
                resultSet.Dispose();
                return false;
            }

            // Verify at least one row returned
            if (resultSet.Rows.Count < 1)
            {
                // No data was returned
                var warningMessage = "LoadDataPackageDatasetInfo; No datasets were found for data package " + dataPackageID;
                LogTools.LogWarning(warningMessage);
                return false;
            }

            foreach (DataRow curRow in resultSet.Rows)
            {
                var datasetInfo = ParseDataPackageDatasetInfoRow(curRow);

                if (!dctDataPackageDatasets.ContainsKey(datasetInfo.DatasetID))
                {
                    dctDataPackageDatasets.Add(datasetInfo.DatasetID, datasetInfo);
                }
            }

            resultSet.Dispose();
            return true;

        }

        /// <summary>
        /// Looks up job information for the given data package
        /// </summary>
        /// <param name="connectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
        /// <param name="dataPackageID">Data Package ID</param>
        /// <param name="dctDataPackageJobs">Jobs associated with the given data package</param>
        /// <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
        /// <remarks>
        /// Property NumberOfClonedSteps is not updated for the analysis jobs returned by this method
        /// In contrast, RetrieveDataPackagePeptideHitJobInfo does update NumberOfClonedSteps
        /// </remarks>
        public static bool LoadDataPackageJobInfo(
            string connectionString,
            int dataPackageID,
            out Dictionary<int, clsDataPackageJobInfo> dctDataPackageJobs)
        {

            // Obtains the job information for a data package
            const short RETRY_COUNT = 3;

            dctDataPackageJobs = new Dictionary<int, clsDataPackageJobInfo>();

            var sqlStr = new System.Text.StringBuilder();

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


            // Get a table to hold the results of the query
            var success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), connectionString, "LoadDataPackageJobInfo", RETRY_COUNT, out var resultSet);

            if (!success)
            {
                var errorMessage = "LoadDataPackageJobInfo; Excessive failures attempting to retrieve data package job info from database";
                LogTools.LogError(errorMessage);
                resultSet.Dispose();
                return false;
            }

            // Verify at least one row returned
            if (resultSet.Rows.Count < 1)
            {
                // No data was returned
                string warningMessage;

                // If the data package exists and has datasets associated with it, Log this as a warning but return true
                // Otherwise, log an error and return false

                sqlStr.Clear();
                sqlStr.Append(" SELECT Count(*) AS Datasets");
                sqlStr.Append(" FROM S_V_DMS_Data_Package_Aggregation_Datasets");
                sqlStr.Append(" WHERE Data_Package_ID = " + dataPackageID);

                // Get a table to hold the results of the query
                success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), connectionString, "LoadDataPackageJobInfo", RETRY_COUNT, out resultSet);
                if (success && resultSet.Rows.Count > 0)
                {
                    foreach (DataRow curRow in resultSet.Rows)
                    {
                        var datasetCount = clsGlobal.DbCInt(curRow[0]);

                        if (datasetCount > 0)
                        {
                            warningMessage = "LoadDataPackageJobInfo; " +
                                             "No jobs were found for data package " + dataPackageID +
                                             ", but it does have " + datasetCount + " dataset";

                            if (datasetCount > 1)
                                warningMessage += "s";

                            LogTools.LogWarning(warningMessage);
                            return true;
                        }
                    }
                }

                warningMessage = "LoadDataPackageJobInfo; No jobs were found for data package " + dataPackageID;
                LogTools.LogError(warningMessage);
                return false;
            }

            foreach (DataRow curRow in resultSet.Rows)
            {
                var dataPkgJob = ParseDataPackageJobInfoRow(curRow);

                if (!dctDataPackageJobs.ContainsKey(dataPkgJob.Job))
                {
                    dctDataPackageJobs.Add(dataPkgJob.Job, dataPkgJob);
                }
                else
                {
                    // Existing job; append an additional SharedResultsFolder
                    var existingPkgJob = dctDataPackageJobs[dataPkgJob.Job];
                    foreach (var sharedResultsFolder in dataPkgJob.SharedResultsFolders)
                    {
                        if (existingPkgJob.SharedResultsFolders.Contains(sharedResultsFolder))
                            continue;

                        existingPkgJob.SharedResultsFolders.Add(sharedResultsFolder);
                    }
                }
            }

            resultSet.Dispose();

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
        /// <param name="brokerConnection">DMS_Pipeline database connection (must already be open)</param>
        /// <param name="jobNumber">Job number</param>
        /// <param name="jobParameters">Output parameter: Dictionary of job parameters where keys are parameter names (section names are ignored)</param>
        /// <param name="errorMsg"></param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>This procedure is used by clsAnalysisToolRunnerPRIDEConverter</remarks>
        private static bool LookupJobParametersFromHistory(
            SqlConnection brokerConnection,
            int jobNumber,
            out Dictionary<string, string> jobParameters,
            out string errorMsg)
        {

            const int RETRY_COUNT = 3;
            const int TIMEOUT_SECONDS = 30;

            jobParameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            errorMsg = string.Empty;

            // Throttle the calls to this function to avoid overloading the database for data packages with hundreds of jobs
            while (DateTime.UtcNow.Subtract(mLastJobParameterFromHistoryLookup).TotalMilliseconds < 50)
            {
                PRISM.ProgRunner.SleepMilliseconds(25);
            }

            mLastJobParameterFromHistoryLookup = DateTime.UtcNow;

            try
            {
                var cmd = new SqlCommand("GetJobStepParamsAsTableUseHistory")
                {
                    CommandType = CommandType.StoredProcedure,
                    Connection = brokerConnection,
                    CommandTimeout = TIMEOUT_SECONDS
                };

                cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                cmd.Parameters.Add(new SqlParameter("@jobNumber", SqlDbType.Int)).Value = jobNumber;
                cmd.Parameters.Add(new SqlParameter("@stepNumber", SqlDbType.Int)).Value = 1;

                // Execute the SP

                DataTable resultSet = null;
                var retryCount = RETRY_COUNT;
                var success = false;

                while (retryCount > 0 && !success)
                {
                    try
                    {
                        using (var Da = new SqlDataAdapter(cmd))
                        {
                            using (var Ds = new DataSet())
                            {
                                Da.Fill(Ds);
                                resultSet = Ds.Tables[0];
                            }
                        }

                        success = true;
                    }
                    catch (Exception ex)
                    {
                        retryCount -= 1;
                        var msg = "Exception running stored procedure " + cmd.CommandText + ": " + ex.Message + "; RetryCount = " + retryCount;

                        LogTools.LogError(msg);

                        // Delay for 5 seconds before trying again
                        clsGlobal.IdleLoop(5);
                    }
                }

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
                    // var section = clsGlobal.DbCStr(curRow[0])
                    var parameter = clsGlobal.DbCStr(curRow[1]);
                    var value = clsGlobal.DbCStr(curRow[2]);

                    if (jobParameters.ContainsKey(parameter))
                    {
                        var msg = "Job " + jobNumber + " has multiple values for parameter " + parameter + "; only using the first occurrence";
                        LogTools.LogWarning(msg);
                    }
                    else
                    {
                        jobParameters.Add(parameter, value);
                    }
                }

                resultSet.Dispose();

                return true;

            }
            catch (Exception ex)
            {
                errorMsg = "Exception retrieving parameters from history for job " + jobNumber;
                LogTools.LogError(errorMsg, ex);
                return false;
            }

        }

        private static clsDataPackageDatasetInfo ParseDataPackageDatasetInfoRow(DataRow curRow)
        {

            var datasetName = clsGlobal.DbCStr(curRow["Dataset"]);
            var datasetId = clsGlobal.DbCInt(curRow["DatasetID"]);

            var datasetInfo = new clsDataPackageDatasetInfo(datasetName, datasetId)
            {
                Instrument = clsGlobal.DbCStr(curRow["Instrument"]),
                InstrumentGroup = clsGlobal.DbCStr(curRow["InstrumentGroup"]),
                Experiment = clsGlobal.DbCStr(curRow["Experiment"]),
                Experiment_Reason = clsGlobal.DbCStr(curRow["Experiment_Reason"]),
                Experiment_Comment = clsGlobal.DbCStr(curRow["Experiment_Comment"]),
                Experiment_Organism = clsGlobal.DbCStr(curRow["Organism"]),
                Experiment_Tissue_ID = clsGlobal.DbCStr(curRow["Experiment_Tissue_ID"]),
                Experiment_Tissue_Name = clsGlobal.DbCStr(curRow["Experiment_Tissue_Name"]),
                Experiment_NEWT_ID = clsGlobal.DbCInt(curRow["Experiment_NEWT_ID"]),
                Experiment_NEWT_Name = clsGlobal.DbCStr(curRow["Experiment_NEWT_Name"]),
                ServerStoragePath = clsGlobal.DbCStr(curRow["Dataset_Folder_Path"]),
                ArchiveStoragePath = clsGlobal.DbCStr(curRow["Archive_Folder_Path"]),
                RawDataType = clsGlobal.DbCStr(curRow["RawDataType"])
            };

            return datasetInfo;

        }

        /// <summary>
        /// Parse results from V_DMS_Data_Package_Aggregation_Jobs
        /// or from
        /// </summary>
        /// <param name="curRow"></param>
        /// <returns></returns>
        public static clsDataPackageJobInfo ParseDataPackageJobInfoRow(DataRow curRow)
        {

            var dataPkgJob = clsGlobal.DbCInt(curRow["Job"]);
            var dataPkgDataset = clsGlobal.DbCStr(curRow["Dataset"]);

            var jobInfo = new clsDataPackageJobInfo(dataPkgJob, dataPkgDataset)
            {
                DatasetID = clsGlobal.DbCInt(curRow["DatasetID"]),
                Instrument = clsGlobal.DbCStr(curRow["Instrument"]),
                InstrumentGroup = clsGlobal.DbCStr(curRow["InstrumentGroup"]),
                Experiment = clsGlobal.DbCStr(curRow["Experiment"]),
                Experiment_Reason = clsGlobal.DbCStr(curRow["Experiment_Reason"]),
                Experiment_Comment = clsGlobal.DbCStr(curRow["Experiment_Comment"]),
                Experiment_Organism = clsGlobal.DbCStr(curRow["Organism"]),
                Experiment_NEWT_ID = clsGlobal.DbCInt(curRow["Experiment_NEWT_ID"]),
                Experiment_NEWT_Name = clsGlobal.DbCStr(curRow["Experiment_NEWT_Name"]),
                Tool = clsGlobal.DbCStr(curRow["Tool"]),
                ResultType = clsGlobal.DbCStr(curRow["ResultType"])
            };

            jobInfo.PeptideHitResultType = clsPHRPReader.GetPeptideHitResultType(jobInfo.ResultType);
            jobInfo.SettingsFileName = clsGlobal.DbCStr(curRow["SettingsFileName"]);
            jobInfo.ParameterFileName = clsGlobal.DbCStr(curRow["ParameterFileName"]);
            jobInfo.OrganismDBName = clsGlobal.DbCStr(curRow["OrganismDBName"]);
            jobInfo.ProteinCollectionList = clsGlobal.DbCStr(curRow["ProteinCollectionList"]);
            jobInfo.ProteinOptions = clsGlobal.DbCStr(curRow["ProteinOptions"]);

            // This will be updated later for SplitFasta jobs (using function LookupJobParametersFromHistory)
            jobInfo.NumberOfClonedSteps = 0;

            if (string.IsNullOrWhiteSpace(jobInfo.ProteinCollectionList) || jobInfo.ProteinCollectionList == "na")
            {
                jobInfo.LegacyFastaFileName = string.Copy(jobInfo.OrganismDBName);
            }
            else
            {
                jobInfo.LegacyFastaFileName = "na";
            }

            jobInfo.ServerStoragePath = clsGlobal.DbCStr(curRow["ServerStoragePath"]);
            jobInfo.ArchiveStoragePath = clsGlobal.DbCStr(curRow["ArchiveStoragePath"]);
            jobInfo.ResultsFolderName = clsGlobal.DbCStr(curRow["ResultsFolder"]);
            jobInfo.DatasetFolderName = clsGlobal.DbCStr(curRow["DatasetFolder"]);

            var sharedResultsFolder = clsGlobal.DbCStr(curRow["SharedResultsFolder"]);
            if (!string.IsNullOrWhiteSpace(sharedResultsFolder))
            {
                jobInfo.SharedResultsFolders.Add(sharedResultsFolder);
            }

            jobInfo.RawDataType = clsGlobal.DbCStr(curRow["RawDataType"]);

            return jobInfo;

        }

        /// <summary>
        /// Lookup the Peptide Hit jobs associated with the current job
        /// </summary>
        /// <param name="additionalJobs">Non Peptide Hit jobs (e.g. DeconTools or MASIC)</param>
        /// <returns>Peptide Hit Jobs (e.g. MSGF+ or Sequest)</returns>
        /// <remarks></remarks>
        public List<clsDataPackageJobInfo> RetrieveDataPackagePeptideHitJobInfo(out List<clsDataPackageJobInfo> additionalJobs)
        {

            // Gigasax.DMS_Pipeline
            var connectionString = ConnectionString;

            if (DataPackageID < 0)
            {
                LogError("DataPackageID is not defined for this analysis job");
                additionalJobs = new List<clsDataPackageJobInfo>();
                return new List<clsDataPackageJobInfo>();
            }

            var dataPackagePeptideHitJobs = RetrieveDataPackagePeptideHitJobInfo(connectionString, DataPackageID, out additionalJobs, out var errorMsg);

            if (!string.IsNullOrWhiteSpace(errorMsg))
            {
                LogError(errorMsg);
            }

            return dataPackagePeptideHitJobs;
        }

        /// <summary>
        /// Lookup the Peptide Hit jobs associated with the data package
        /// </summary>
        /// <param name="connectionString">Connection string to the DMS_Pipeline database</param>
        /// <param name="dataPackageID">Data package ID</param>
        /// <param name="errorMsg">Output: error message</param>
        /// <returns>Peptide Hit Jobs (e.g. MSGF+ or Sequest)</returns>
        /// <remarks>Alternatively use the overloaded version that includes additionalJobs</remarks>
        public static List<clsDataPackageJobInfo> RetrieveDataPackagePeptideHitJobInfo(
            string connectionString,
            int dataPackageID,
            out string errorMsg)
        {
            return RetrieveDataPackagePeptideHitJobInfo(connectionString, dataPackageID, out _, out errorMsg);
        }

        /// <summary>
        /// Lookup the Peptide Hit jobs associated with the data package; non-peptide hit jobs are returned via additionalJobs
        /// </summary>
        /// <param name="connectionString">Connection string to the DMS_Pipeline database</param>
        /// <param name="dataPackageID">Data package ID</param>
        /// <param name="additionalJobs">Output: Non Peptide Hit jobs (e.g. DeconTools or MASIC)</param>
        /// <param name="errorMsg">Output: error message</param>
        /// <returns>Peptide Hit Jobs (e.g. MSGF+ or Sequest)</returns>
        /// <remarks>This method updates property NumberOfClonedSteps for the analysis jobs</remarks>
        public static List<clsDataPackageJobInfo> RetrieveDataPackagePeptideHitJobInfo(
            string connectionString,
            int dataPackageID,
            out List<clsDataPackageJobInfo> additionalJobs,
            out string errorMsg)
        {

            // This list tracks the info for the Peptide Hit jobs (e.g. MSGF+ or Sequest) associated with the data package
            var dataPackagePeptideHitJobs = new List<clsDataPackageJobInfo>();
            errorMsg = string.Empty;

            // This list tracks the info for the non Peptide Hit jobs (e.g. DeconTools or MASIC) associated with the data package
            additionalJobs = new List<clsDataPackageJobInfo>();

            // This dictionary will track the jobs associated with this aggregation job's data package
            // Key is job number, value is an instance of clsDataPackageJobInfo
            Dictionary<int, clsDataPackageJobInfo> dctDataPackageJobs;

            try
            {
                if (!LoadDataPackageJobInfo(connectionString, dataPackageID, out dctDataPackageJobs))
                {
                    errorMsg = "Error looking up datasets and jobs using LoadDataPackageJobInfo";
                    return dataPackagePeptideHitJobs;
                }
            }
            catch (Exception ex)
            {
                errorMsg = "Exception calling LoadDataPackageJobInfo: " + ex.Message;
                return dataPackagePeptideHitJobs;
            }

            try
            {

                foreach (var kvItem in dctDataPackageJobs)
                {
                    if (kvItem.Value.PeptideHitResultType == clsPHRPReader.ePeptideHitResultType.Unknown)
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
                                      where dataPkgJob.Tool.ToLower().Contains("SplitFasta".ToLower())
                                      select dataPkgJob).ToList();


                if (splitFastaJobs.Count > 0)
                {
                    var lastStatusTime = DateTime.UtcNow;
                    var statusIntervalSeconds = 4;
                    var jobsProcessed = 0;

                    using (var brokerConnection = new SqlConnection(connectionString))
                    {
                        brokerConnection.Open();


                        foreach (var dataPkgJob in splitFastaJobs)
                        {

                            var success = LookupJobParametersFromHistory(brokerConnection, dataPkgJob.Job, out var dataPkgJobParameters, out errorMsg);

                            if (!success)
                            {
                                return new List<clsDataPackageJobInfo>();
                            }

                            if (dataPkgJobParameters.TryGetValue("NumberOfClonedSteps", out var numberOfClonedSteps))
                            {
                                if (int.TryParse(numberOfClonedSteps, out var clonedStepCount))
                                    dataPkgJob.NumberOfClonedSteps = clonedStepCount;
                            }

                            jobsProcessed += 1;

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

            }
            catch (Exception ex)
            {
                errorMsg = "Exception calling LookupJobParametersFromHistory (RetrieveDataPackagePeptideHitJobInfo): " + ex.Message;
                return new List<clsDataPackageJobInfo>();
            }

            return dataPackagePeptideHitJobs;

        }


    }
}