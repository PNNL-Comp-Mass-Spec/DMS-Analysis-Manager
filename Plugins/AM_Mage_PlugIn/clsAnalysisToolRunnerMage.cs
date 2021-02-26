using AnalysisManagerBase;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;

namespace AnalysisManager_Mage_PlugIn
{
    // ReSharper disable once UnusedMember.Global
    public class clsAnalysisToolRunnerMage : clsAnalysisToolRunnerMAC
    {
        public const string T_ALIAS_FILE = "t_alias.txt";
        public const string T_ALIAS_TABLE = "T_alias";

        /// <summary>
        /// Sequentially run the Mage operations listed in "MageOperations" parameter
        /// </summary>
        protected override bool RunMACTool()
        {
            // Change the name of the log file for the local log file to the plugin log filename
            var logFilePath = Path.Combine(mWorkDir, MAGE_LOG_FILE_NAME);
            const bool appendDateToBaseName = false;
            LogTools.ChangeLogFileBaseName(logFilePath, appendDateToBaseName);

            // run the appropriate Mage pipeline(s) according to operations list parameter
            var mageOperations = mJobParams.GetParam("MageOperations");
            var ops = new MageAMOperations(mJobParams, mMgrParams, logFilePath, appendDateToBaseName);
            RegisterEvents(ops);

            var success = ops.RunMageOperations(mageOperations);

            // Change the name of the log file back to the analysis manager log file
            ResetLogFileNameToDefault();

            if (!string.IsNullOrEmpty(ops.WarningMsg))
            {
                // Update EvalMessage (the warning has already been logged)
                mEvalMessage = ops.WarningMsg;
            }

            if (!success)
                return false;

            // Make sure the Results.db3 file was created
            var resultsDB = new FileInfo(Path.Combine(mWorkDir, "Results.db3"));
            if (!resultsDB.Exists)
            {
                LogError("Results.db3 file was not created");
                return false;
            }

            success = ValidateSqliteDB(mageOperations, resultsDB);

            return success;
        }

        /// <summary>
        /// Get name and version info for primary Mage MAC tool assembly
        /// </summary>
        protected override string GetToolNameAndVersion()
        {
            var toolVersionInfo = string.Empty;
            var assemblyName = System.Reflection.Assembly.Load("Mage").GetName();
            var nameAndVersion = assemblyName.Name + ", Version=" + assemblyName.Version;
            toolVersionInfo = clsGlobal.AppendToComment(toolVersionInfo, nameAndVersion);
            return toolVersionInfo;
        }

        /// <summary>
        /// Get file version info for supplemental Mage assemblies
        /// </summary>
        /// <returns>List of file info for supplemental DLLs</returns>
        protected override List<FileInfo> GetToolSupplementalVersionInfo()
        {
            var toolFiles = new List<FileInfo>
                                  {
                                      new("Mage.dll"),
                                      new("MageExtContentFilters.dll"),
                                      new("MageExtExtractionFilters.dll")
                                  };
            return toolFiles;
        }

        protected bool ValidateFactors(FileInfo resultsDB, out string errorMessage, out string exceptionDetail)
        {
            const string FACTOR_URL = "http://dms2.pnl.gov/requested_run_factors/param";

            try
            {
                // Verify that table t_factors exists and has columns Dataset_ID and Sample
                var columns = new List<string>()
                {
                    "Dataset_ID",
                    "Sample"
                };

                if (!TableContainsDataAndColumns(resultsDB, "t_factors", columns, out errorMessage, out exceptionDetail))
                {
                    errorMessage = "table t_factors in Results.db3 " + errorMessage +
                                "; use " + FACTOR_URL + " to define factors named Sample for the datasets in this data package";
                    return false;
                }

                // Lookup the Dataset_ID values defined in t_results
                var datasetIDs = new List<int>();

                var connectionString = "Data Source = " + resultsDB.FullName + "; Version=3;";
                using var conn = new SQLiteConnection(connectionString);
                conn.Open();

                var query = "SELECT Distinct DPJ.dataset_id " +
                            "FROM t_results R " +
                            " INNER JOIN t_data_package_analysis_jobs DPJ " +
                            " ON R.job = DPJ.job";

                using (var cmd = new SQLiteCommand(query, conn))
                {
                    var drReader = cmd.ExecuteReader();

                    if (!drReader.HasRows)
                    {
                        errorMessage = "no results joining t_results and t_data_package_analysis_jobs on Job";
                        return false;
                    }

                    while (drReader.Read())
                    {
                        datasetIDs.Add(drReader.GetInt32(0));
                    }
                }

                // Lookup the Sample Names defined in t_factors
                var sampleNames = new List<string>();

                query = "SELECT Dataset_ID, Sample " +
                        "FROM t_factors " +
                        "WHERE Dataset_ID IN (" + string.Join(",", datasetIDs) + ")";

                using (var cmd = new SQLiteCommand(query, conn))
                {
                    var drReader = cmd.ExecuteReader();

                    if (!drReader.HasRows)
                    {
                        errorMessage = "no results querying t_factors with the DatasetIDs in t_results";
                        return false;
                    }

                    var validDatasetIDs = 0;

                    while (drReader.Read())
                    {
                        var sampleName = drReader.GetString(1);
                        if (!sampleNames.Contains(sampleName, StringComparer.OrdinalIgnoreCase))
                            sampleNames.Add(sampleName);

                        validDatasetIDs++;
                    }

                    if (validDatasetIDs < datasetIDs.Count)
                    {
                        errorMessage = "Of the " + datasetIDs.Count + " datasets in t_results, " + (datasetIDs.Count - validDatasetIDs) +
                                       " do not have a factor named Sample defined in DMS; go to " + FACTOR_URL + " to make changes";
                        return false;
                    }
                }

                // Make sure the sample names in sampleNames correspond to the names defined in table t_alias
                // At the same time, count the number of ions defined for each sample
                var sampleToIonMapping = new Dictionary<string, byte>(StringComparer.OrdinalIgnoreCase);

                query = "SELECT Sample, Count(Ion) as Ions " +
                        "FROM " + T_ALIAS_TABLE + " " +
                        "GROUP BY Sample ";

                using (var cmd = new SQLiteCommand(query, conn))
                {
                    var drReader = cmd.ExecuteReader();

                    if (!drReader.HasRows)
                    {
                        errorMessage = "no results querying t_factors with the DatasetIDs in t_results";
                        return false;
                    }

                    while (drReader.Read())
                    {
                        var sampleName = drReader.GetString(0);
                        var ionCount = drReader.GetByte(1);

                        sampleToIonMapping.Add(sampleName, ionCount);
                    }
                }

                var ionCounts = new List<byte>();

                foreach (var sampleName in sampleNames)
                {
                    if (!sampleToIonMapping.TryGetValue(sampleName, out var ionCount))
                    {
                        errorMessage = T_ALIAS_TABLE + " table does not have an entry for Sample '" + sampleName + "'; " +
                                       "this Sample name is a dataset factor and must be defined in the " + T_ALIAS_FILE + " file";
                        return false;
                    }

                    ionCounts.Add(ionCount);
                }

                // Make sure all of the ion counts are the same
                var ionCountFirst = ionCounts.First();
                var lookupQ = (from item in ionCounts where item != ionCountFirst select item).ToList();
                if (lookupQ.Count > 0)
                {
                    // Example message:
                    // Not all entries in the t_alias table have 4 ions; edit the T_alias.txt file
                    errorMessage = "Not all entries in the " + T_ALIAS_TABLE + " table have " + ionCountFirst + " ions; " +
                                   "edit the " + T_ALIAS_FILE + " file";

                    return false;
                }

                var ionColumns = new List<string>();
                var labelingScheme = string.Empty;

                var workFlowSteps = mJobParams.GetParam("ApeWorkflowStepList", string.Empty);
                if (workFlowSteps.Contains("4plex"))
                {
                    // 4-plex iTraq
                    labelingScheme = "4plex";
                    ionColumns.Add("Ion_114");
                    ionColumns.Add("Ion_115");
                    ionColumns.Add("Ion_116");
                    ionColumns.Add("Ion_117");
                }

                if (workFlowSteps.Contains("6plex"))
                {
                    // 6-plex TMT
                    labelingScheme = "6plex";
                    ionColumns.Add("Ion_126");
                    ionColumns.Add("Ion_127");
                    ionColumns.Add("Ion_128");
                    ionColumns.Add("Ion_129");
                    ionColumns.Add("Ion_130");
                    ionColumns.Add("Ion_131");
                }

                if (workFlowSteps.Contains("8plex"))
                {
                    // 8-plex iTraq
                    labelingScheme = "8plex";
                    ionColumns.Add("Ion_113");
                    ionColumns.Add("Ion_114");
                    ionColumns.Add("Ion_115");
                    ionColumns.Add("Ion_116");
                    ionColumns.Add("Ion_117");
                    ionColumns.Add("Ion_118");
                    ionColumns.Add("Ion_119");
                    ionColumns.Add("Ion_121");
                }

                if (workFlowSteps.Contains("TMT10Plex"))
                {
                    // 10-plex TMT
                    labelingScheme = "TMT10Plex";
                    ionColumns.Add("Ion_126.128");
                    ionColumns.Add("Ion_127.125");
                    ionColumns.Add("Ion_127.131");
                    ionColumns.Add("Ion_128.128");
                    ionColumns.Add("Ion_128.134");
                    ionColumns.Add("Ion_129.131");
                    ionColumns.Add("Ion_129.138");
                    ionColumns.Add("Ion_130.135");
                    ionColumns.Add("Ion_130.141");
                }

                if (workFlowSteps.Contains("TMT16Plex"))
                {
                    // 16-plex TMT
                    labelingScheme = "TMT16Plex";
                    ionColumns.Add("Ion_126.128");
                    ionColumns.Add("Ion_127.125");
                    ionColumns.Add("Ion_127.131");
                    ionColumns.Add("Ion_128.128");
                    ionColumns.Add("Ion_128.134");
                    ionColumns.Add("Ion_129.131");
                    ionColumns.Add("Ion_129.138");
                    ionColumns.Add("Ion_130.135");
                    ionColumns.Add("Ion_130.141");
                    ionColumns.Add("Ion_131.138");
                    ionColumns.Add("Ion_131.144");
                    ionColumns.Add("Ion_132.142");
                    ionColumns.Add("Ion_132.148");
                    ionColumns.Add("Ion_133.145");
                    ionColumns.Add("Ion_133.151");
                    ionColumns.Add("Ion_134.148");
                }

                if (ionColumns.Count > 0)
                {
                    if (!TableContainsDataAndColumns(resultsDB, "T_Reporter_Ions", ionColumns, out errorMessage, out exceptionDetail))
                    {
                        errorMessage = "table T_Reporter_Ions in Results.db3 " + errorMessage +
                                       "; you need to specify " + labelingScheme + " in the ApeWorkflowStepList parameter of the Ape step";
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                errorMessage = "threw an exception while querying";
                exceptionDetail = ex.Message;
                LogError("Exception in ValidateFactors: " + ex.Message, ex);
                return false;
            }

            return true;
        }

        protected bool ValidateSqliteDB(string mageOperations, FileInfo resultsDB)
        {
            // If the Mage Operations list contains "ExtractFromJobs", make sure that table "t_results" was created
            // If it wasn't, no matching jobs were found and we should fail out this job step
            if (mageOperations.Contains("ExtractFromJobs"))
            {
                if (!TableExists(resultsDB, "t_results"))
                {
                    LogError("Results.db3 file does not have table T_Results; Mage did not extract results from any jobs");
                    return false;
                }
            }

            var itraqMode = false;
            var analysisType = mJobParams.GetJobParameter("AnalysisType", string.Empty);
            if (analysisType.Contains("iTRAQ"))
                itraqMode = true;

            // If the Mage Operations list contains "ImportDataPackageFiles", make sure that table "T_alias" was created
            // If it wasn't, we should fail out this job step
            if (itraqMode || mageOperations.Contains("ImportDataPackageFiles"))
            {
                if (!TableExists(resultsDB, T_ALIAS_TABLE))
                {
                    // Results.db3 file does not have table t_alias.txt; place a valid t_alias.txt file in the the data package's ImportFiles directory
                    LogError("Results.db3 file does not have table " + T_ALIAS_TABLE + "; " +
                        "place a valid " + T_ALIAS_FILE + " file in the the data package's ImportFiles directory");
                    return false;
                }

                // Confirm that the T_alias table contains columns Sample and Ion and that it contains data

                var columns = new List<string>
                {
                    "Sample",
                    "Ion"
                };

                // Look for the T_alias table
                if (!TableContainsDataAndColumns(resultsDB, T_ALIAS_TABLE, columns, out var errorMessage, out var exceptionDetail))
                {
                    // Example messages
                    // Table T_alias in Results.db3 is empty
                    // Table T_alias in Results.db3 is missing column Tissue
                    LogError("Table " + T_ALIAS_TABLE + " in Results.db3 " + errorMessage +
                                "; place a valid " + T_ALIAS_FILE + " file in the the data package's ImportFiles directory; " + exceptionDetail);
                    return false;
                }

                if (!ValidateFactors(resultsDB, out errorMessage, out exceptionDetail))
                {
                    if (!mMessage.Contains(errorMessage))
                    {
                        LogError("Error validating factors: " + errorMessage + "; " + exceptionDetail);
                    }
                    return false;
                }
            }

            return true;
        }
    }
}