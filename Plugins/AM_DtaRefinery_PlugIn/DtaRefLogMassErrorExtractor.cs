using System;
using System.Data;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.JobConfig;
using PRISM;
using PRISMDatabaseUtils;

namespace AnalysisManagerDtaRefineryPlugIn
{
    /// <summary>
    /// This class reads a DTA_Refinery log file to extract the parent ion mass error information
    /// It passes on the information to DMS for storage in table T_Dataset_QC
    /// </summary>
    public class DtaRefLogMassErrorExtractor : EventNotifier
    {
        // Ignore Spelling: dta

        private const string STORE_MASS_ERROR_STATS_SP_NAME = "store_dta_ref_mass_error_stats";

        private readonly IMgrParams mMgrParams;
        private readonly string mWorkDir;
        private readonly short mDebugLevel;
        private readonly bool mPostResultsToDB;

        private struct MassErrorInfo
        {
            public string DatasetName;
            public int PSMJob;
            public double MassErrorPPM;                 // Parent Ion Mass Error, before refinement
            public double MassErrorPPMRefined;          // Parent Ion Mass Error, after refinement
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams"></param>
        /// <param name="workDir"></param>
        /// <param name="debugLevel"></param>
        /// <param name="postResultsToDB"></param>
        public DtaRefLogMassErrorExtractor(IMgrParams mgrParams, string workDir, short debugLevel, bool postResultsToDB)
        {
            mMgrParams = mgrParams;
            mWorkDir = workDir;
            mDebugLevel = debugLevel;
            mPostResultsToDB = postResultsToDB;
        }

        private string ConstructXML(MassErrorInfo massErrorInfo)
        {
            var builder = new StringBuilder();

            try
            {
                builder.Append("<DTARef_MassErrorStats>");

                builder.AppendFormat("<Dataset>{0}</Dataset>", massErrorInfo.DatasetName);
                builder.AppendFormat("<PSM_Source_Job>{0}</PSM_Source_Job>", massErrorInfo.PSMJob);

                builder.Append("<Measurements>");
                builder.AppendFormat("<Measurement Name=\"MassErrorPPM\">{0}</Measurement>", massErrorInfo.MassErrorPPM);
                builder.AppendFormat("<Measurement Name=\"MassErrorPPM_Refined\">{0}</Measurement>", massErrorInfo.MassErrorPPMRefined);
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
        /// Parse the DTA Refinery log file
        /// </summary>
        /// <param name="datasetName"></param>
        /// <param name="datasetID"></param>
        /// <param name="psmJob"></param>
        public bool ParseDTARefineryLogFile(string datasetName, int datasetID, int psmJob)
        {
            return ParseDTARefineryLogFile(datasetName, datasetID, psmJob, mWorkDir);
        }

        /// <summary>
        /// Parse the DTA Refinery log file
        /// </summary>
        /// <param name="datasetName"></param>
        /// <param name="datasetID"></param>
        /// <param name="psmJob"></param>
        /// <param name="workDirPath"></param>
        public bool ParseDTARefineryLogFile(string datasetName, int datasetID, int psmJob, string workDirPath)
        {
            var originalDistributionSection = false;
            var refinedDistributionSection = false;

            var reMassError = new Regex(@"Robust estimate[ \t]+([^\t ]+)", RegexOptions.Compiled);

            try
            {
                var massErrorInfo = new MassErrorInfo
                {
                    DatasetName = datasetName,
                    PSMJob = psmJob,
                    MassErrorPPM = double.MinValue,
                    MassErrorPPMRefined = double.MinValue
                };

                var sourceFile = new FileInfo(Path.Combine(workDirPath, datasetName + "_dta_DtaRefineryLog.txt"));

                if (!sourceFile.Exists)
                {
                    OnErrorEvent("DtaRefinery Log file not found; " + sourceFile.FullName);
                    return false;
                }

                using (var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (dataLine.IndexOf("ORIGINAL parent ion mass error distribution", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            originalDistributionSection = true;
                            refinedDistributionSection = false;
                        }

                        if (dataLine.IndexOf("REFINED parent ion mass error distribution", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            originalDistributionSection = false;
                            refinedDistributionSection = true;
                        }

                        if (!dataLine.StartsWith("Robust estimate", StringComparison.OrdinalIgnoreCase))
                            continue;

                        var reMatch = reMassError.Match(dataLine);

                        if (reMatch.Success)
                        {
                            if (double.TryParse(reMatch.Groups[1].Value, out var massError))
                            {
                                if (originalDistributionSection)
                                {
                                    massErrorInfo.MassErrorPPM = massError;
                                }

                                if (refinedDistributionSection)
                                {
                                    massErrorInfo.MassErrorPPMRefined = massError;
                                }
                            }
                            else
                            {
                                OnErrorEvent("Unable to extract mass error value from 'Robust estimate' line in the DTA Refinery log file; " +
                                             "RegEx capture is not a number: " + reMatch.Groups[1].Value);
                                return false;
                            }
                        }
                        else
                        {
                            OnErrorEvent("Unable to extract mass error value from 'Robust estimate' line in the DTA Refinery log file; RegEx match failed");
                            return false;
                        }
                    }
                }

                if (massErrorInfo.MassErrorPPM > double.MinValue)
                {
                    var xmlResults = ConstructXML(massErrorInfo);

                    if (mPostResultsToDB)
                    {
                        var success = PostMassErrorInfoToDB(datasetID, xmlResults);

                        if (!success)
                        {
                            // The error should have already been reported
                            return false;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in ParseDTARefineryLogFile: " + ex.Message, ex);
                return false;
            }

            return true;
        }

        private bool PostMassErrorInfoToDB(int datasetID, string xmlResults)
        {
            try
            {
                // Call procedure STORE_MASS_ERROR_STATS_SP_NAME in DMS5

                var analysisTask = new AnalysisJob(mMgrParams, mDebugLevel);
                var dbTools = analysisTask.DMSProcedureExecutor;

                var cmd = dbTools.CreateCommand(STORE_MASS_ERROR_STATS_SP_NAME, CommandType.StoredProcedure);

                // Define parameter for procedure's return value
                // If querying a Postgres DB, dbTools will auto-change "@return" to "_returnCode"
                var returnParam = dbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);

                dbTools.AddTypedParameter(cmd, "@datasetID", SqlType.Int, value: datasetID);
                dbTools.AddParameter(cmd, "@resultsXML", SqlType.XML).Value = xmlResults;

                // Call the procedure (retry the call, up to 3 times)
                var resCode = dbTools.ExecuteSP(cmd);

                var returnCode = DBToolsBase.GetReturnCode(returnParam);

                if (resCode == 0 && returnCode == 0)
                {
                    return true;
                }

                if (resCode != 0 && returnCode == 0)
                {
                    OnErrorEvent(
                        "ExecuteSP() reported result code {0} storing DTA Refinery Mass Error results in database using {1}",
                        resCode, STORE_MASS_ERROR_STATS_SP_NAME);

                    return false;
                }

                OnErrorEvent(
                    "Error storing DTA Refinery Mass Error results in the database, {0} returned {1}",
                    STORE_MASS_ERROR_STATS_SP_NAME, returnParam.Value.CastDBVal<string>());

                return false;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception storing DTA Refinery Mass Error Results in the database: " + ex.Message, ex);
                return false;
            }
        }
    }
}
