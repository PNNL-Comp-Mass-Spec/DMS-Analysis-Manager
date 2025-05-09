using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;
using AnalysisManagerBase;
using AnalysisManagerBase.JobConfig;
using CsvHelper;
using CsvHelper.Configuration;
using PRISMDatabaseUtils;

namespace AnalysisManagerDiaNNPlugIn
{
    /// <summary>
    /// This class reads file report.stats.tsv created by DIA-NN, extracting the parent ion mass error information
    /// It passes on the information to DMS for storage in table T_Dataset_QC
    /// </summary>
    public class DiaNNMassErrorStatsExtractor
    {
        // Ignore Spelling: PSM

        private const string STORE_MASS_ERROR_STATS_SP_NAME = "store_dta_ref_mass_error_stats";
        private readonly IMgrParams mMgrParams;
        private readonly short mDebugLevel;

        private readonly bool mPostResultsToDB;

        public string ErrorMessage { get; private set; }

        /// <summary>
        /// Mass error info; populated by ParsePPMErrorCharterOutput
        /// </summary>
        public MassErrorInfo MassErrorStats { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Manager parameters</param>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        /// <param name="postResultsToDB">When true, post the results to the database</param>
        public DiaNNMassErrorStatsExtractor(IMgrParams mgrParams, short debugLevel, bool postResultsToDB = true)
        {
            mMgrParams = mgrParams;
            mDebugLevel = debugLevel;
            mPostResultsToDB = postResultsToDB;

            ErrorMessage = string.Empty;

            MassErrorStats = new MassErrorInfo();
            MassErrorStats.Clear();
        }

        private string ConstructXML()
        {
            var builder = new StringBuilder();

            try
            {
                builder.Append("<DTARef_MassErrorStats>");

                builder.AppendFormat("<Dataset>{0}</Dataset>", MassErrorStats.DatasetName);
                builder.AppendFormat("<PSM_Source_Job>{0}</PSM_Source_Job>", MassErrorStats.PSMJob);

                builder.Append("<Measurements>");
                builder.AppendFormat("<Measurement Name=\"{0}\">{1}</Measurement>", "MassErrorPPM", MassErrorStats.MassErrorPPM);
                builder.AppendFormat("<Measurement Name=\"{0}\">{1}</Measurement>", "MassErrorPPM_Refined", MassErrorStats.MassErrorPPMCorrected);
                builder.Append("</Measurements>");

                builder.Append("</DTARef_MassErrorStats>");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error converting Mass Error stats to XML; details:");
                Console.WriteLine(ex);
                return string.Empty;
            }

            return builder.ToString();
        }

        /// <summary>
        /// Parse the DIA-NN report.stats.tsv output file to extract the parent ion mass errors (uncorrected and corrected)
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="datasetID">Dataset ID</param>
        /// <param name="psmJob">PSM job number</param>
        /// <param name="reportStatsTsvFile">DIA-NN report.stats.tsv output file path</param>
        /// <returns>True if successful, false if an error</returns>
        public bool ParseDiaNNReportStatsTsv(string datasetName, int datasetID, int psmJob, FileInfo reportStatsTsvFile)
        {
            // The report.stats.tsv file should have a header row, followed by one row for each .mzML input file
            // Since this method is only called for DIA-NN searches of individual datasets, there should only be one .mzML file listed

            // ... Median.Mass.Acc.MS1   Median.Mass.Acc.MS1.Corrected   Median.Mass.Acc.MS2   Median.Mass.Acc.MS2.Corrected ...
            // ... 4.16583               0.683939                        4.32735               1.81076                       ...

            const string MASS_ERROR_PPM = "Median.Mass.Acc.MS1";
            const string MASS_ERROR_PPM_CORRECTED = "Median.Mass.Acc.MS1.Corrected";

            try
            {
                MassErrorStats.Clear();
                MassErrorStats.DatasetName = datasetName;
                MassErrorStats.PSMJob = psmJob;

                if (!reportStatsTsvFile.Exists)
                {
                    ErrorMessage = "DIA-NN report.stats.tsv file not found";
                    return false;
                }

                using (var reader = new StreamReader(new FileStream(reportStatsTsvFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.CurrentCulture) { Delimiter = "\t"}))
                {
                    csv.Read();
                    csv.ReadHeader();
                    while (csv.Read())
                    {
                        MassErrorStats.MassErrorPPM = csv.GetField<double>(MASS_ERROR_PPM);
                        MassErrorStats.MassErrorPPMCorrected = csv.GetField<double>(MASS_ERROR_PPM_CORRECTED);
                    }
                }

                if (Math.Abs(MassErrorStats.MassErrorPPM - double.MinValue) < float.Epsilon)
                {
                    // Did not find 'Median.Mass.Acc.MS1' in the report.stats.tsv file
                    ErrorMessage = string.Format("Did not find '{0}' in the DIA-NN report.stats.tsv", MASS_ERROR_PPM);
                    return false;
                }

                if (Math.Abs(MassErrorStats.MassErrorPPMCorrected - double.MinValue) < float.Epsilon)
                {
                    // Did not find 'Median.Mass.Acc.MS1.Corrected' in the report.stats.tsv file
                    ErrorMessage = string.Format("Did not find '{0}' in the DIA-NN report.stats.tsv", MASS_ERROR_PPM_CORRECTED);
                    return false;
                }

                var xmlResults = ConstructXML();

                if (string.IsNullOrWhiteSpace(xmlResults))
                {
                    ErrorMessage = "Method ConstructXML returned an empty string in class DiaNNMassErrorStatsExtractor";
                    return false;
                }

                if (mPostResultsToDB)
                {
                    var success = PostMassErrorInfoToDB(datasetID, xmlResults);

                    if (!success)
                    {
                        if (string.IsNullOrEmpty(ErrorMessage))
                        {
                            ErrorMessage = "Unknown error posting Mass Error results from DIA-NN to the database";
                        }
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = "Error in ParseDiaNNReportStatsTsv: " + ex.Message;
                return false;
            }

            return true;
        }

        private bool PostMassErrorInfoToDB(int datasetID, string xmlResults)
        {
            try
            {
                var analysisTask = new AnalysisJob(mMgrParams, mDebugLevel);
                var dbTools = analysisTask.DMSProcedureExecutor;

                // Call procedure store_dta_ref_mass_error_stats in DMS5
                // Data is stored in table T_Dataset_QC
                var sqlCmd = dbTools.CreateCommand(STORE_MASS_ERROR_STATS_SP_NAME, CommandType.StoredProcedure);

                // Define parameter for procedure's return value
                // If querying a Postgres DB, dbTools will auto-change "@return" to "_returnCode"
                var returnParam = dbTools.AddParameter(sqlCmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);

                dbTools.AddTypedParameter(sqlCmd, "@datasetID", SqlType.Int, value: datasetID);
                dbTools.AddParameter(sqlCmd, "@resultsXML", SqlType.XML).Value = xmlResults;

                // Call the procedure (retry the call, up to 3 times)
                var resCode = dbTools.ExecuteSP(sqlCmd);

                var returnCode = DBToolsBase.GetReturnCode(returnParam);

                if (resCode == 0 && returnCode == 0)
                {
                    return true;
                }

                if (resCode != 0 && returnCode == 0)
                {
                    ErrorMessage = string.Format(
                        "ExecuteSP() reported result code {0} storing DIA-NN Mass Error results in database using {1}",
                        resCode, STORE_MASS_ERROR_STATS_SP_NAME);
                }
                else
                {
                    ErrorMessage = string.Format(
                        "Error storing DIA-NN Mass Error results in database, {0} returned {1}",
                        STORE_MASS_ERROR_STATS_SP_NAME, returnParam.Value.CastDBVal<string>());
                }

                return false;
            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception storing DIA-NN Mass Error Results in the database: " + ex.Message;
                return false;
            }
        }
    }
}
