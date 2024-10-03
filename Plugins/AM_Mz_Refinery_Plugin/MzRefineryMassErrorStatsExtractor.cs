using AnalysisManagerBase;
using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using AnalysisManagerBase.JobConfig;
using PRISMDatabaseUtils;

namespace AnalysisManagerMzRefineryPlugIn
{
    /// <summary>
    /// This class reads the console text from the PPMErrorCharter's console output and extracts the parent ion mass error information
    /// It passes on the information to DMS for storage in table T_Dataset_QC
    /// </summary>
    public class MzRefineryMassErrorStatsExtractor
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
        public MzRefineryMassErrorStatsExtractor(IMgrParams mgrParams, short debugLevel, bool postResultsToDB = true)
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
                builder.AppendFormat("<Measurement Name=\"{0}\">{1}</Measurement>", "MassErrorPPM_Refined", MassErrorStats.MassErrorPPMRefined);
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
        /// Parse the PPM Error Charter console output file to extract the mass error reported in this table
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="datasetID">Dataset ID</param>
        /// <param name="psmJob">PSM job number</param>
        /// <param name="ppmErrorCharterConsoleOutputFilePath">PPM Error Charter console output file path</param>
        public bool ParsePPMErrorCharterOutput(string datasetName, int datasetID, int psmJob, string ppmErrorCharterConsoleOutputFilePath)
        {
            // Example console output:

            // Using fixed data file "E:\DMS_WorkDir\DatasetName_FIXED.mzML"
            // Statistic                   Original    Refined
            // MeanMassErrorPPM:              2.430      1.361
            // MedianMassErrorPPM:            1.782      0.704
            // StDev(Mean):                   6.969      6.972
            // StDev(Median):                 6.999      7.003
            // PPM Window for 99%: 0 +/-     22.779     21.712
            // PPM Window for 99%: high:     22.779     21.712
            // PPM Window for 99%:  low:    -19.216    -20.305

            const string MASS_ERROR_PPM = "MedianMassErrorPPM:";

            try
            {
                MassErrorStats.Clear();
                MassErrorStats.DatasetName = datasetName;
                MassErrorStats.PSMJob = psmJob;

                var sourceFile = new FileInfo(ppmErrorCharterConsoleOutputFilePath);

                if (!sourceFile.Exists)
                {
                    ErrorMessage = "MzRefinery Log file not found";
                    return false;
                }

                using (var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        dataLine = dataLine.Trim();

                        if (!dataLine.StartsWith(MASS_ERROR_PPM))
                            continue;

                        var dataString = dataLine.Substring(MASS_ERROR_PPM.Length).Trim();

                        var dataValues = dataString.Split(' ').ToList();

                        if (double.TryParse(dataValues.First(), out var massError))
                        {
                            MassErrorStats.MassErrorPPM = massError;
                        }

                        if (dataValues.Count > 1 && double.TryParse(dataValues.Last(), out massError))
                        {
                            MassErrorStats.MassErrorPPMRefined = massError;
                        }
                    }
                }

                if (Math.Abs(MassErrorStats.MassErrorPPM - double.MinValue) < float.Epsilon)
                {
                    // Did not find 'MedianMassErrorPPM' in the PPM Error Charter output
                    ErrorMessage = "Did not find '" + MASS_ERROR_PPM + "' in the PPM Error Charter output";
                    return false;
                }

                if (Math.Abs(MassErrorStats.MassErrorPPMRefined - double.MinValue) < float.Epsilon)
                {
                    // Did not find 'MedianMassErrorPPM' with two values in the PPM Error Charter output
                    ErrorMessage = "Did not find '" + MASS_ERROR_PPM + "' with two values in the PPM Error Charter output";
                    return false;
                }

                var xmlResults = ConstructXML();

                if (mPostResultsToDB)
                {
                    var success = PostMassErrorInfoToDB(datasetID, xmlResults);

                    if (!success)
                    {
                        if (string.IsNullOrEmpty(ErrorMessage))
                        {
                            ErrorMessage = "Unknown error posting Mass Error results from MzRefinery to the database";
                        }
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = "Error in ParsePPMErrorCharterOutput: " + ex.Message;
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

                // Call stored procedure store_dta_ref_mass_error_stats in DMS5
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
                        "ExecuteSP() reported result code {0} storing MzRefinery Mass Error results in database using {1}",
                        resCode, STORE_MASS_ERROR_STATS_SP_NAME);
                }
                else
                {
                    ErrorMessage = string.Format(
                        "Error storing MzRefinery Mass Error results in database, {0} returned {1}",
                        STORE_MASS_ERROR_STATS_SP_NAME, returnParam.Value.CastDBVal<string>());
                }

                return false;
            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception storing MzRefinery Mass Error Results in the database: " + ex.Message;
                return false;
            }
        }
    }
}
