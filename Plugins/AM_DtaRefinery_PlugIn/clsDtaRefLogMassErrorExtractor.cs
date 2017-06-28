using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerDtaRefineryPlugIn
{
    /// <summary>
    /// This class reads a DTA_Refinery log file to extract the parent ion mass error information
    /// It passes on the information to DMS for storage in table T_Dataset_QC
    /// </summary>
    /// <remarks></remarks>
    public class clsDtaRefLogMassErrorExtractor : clsEventNotifier
    {
        private const string STORE_MASS_ERROR_STATS_SP_NAME = "StoreDTARefMassErrorStats";

        private readonly IMgrParams m_mgrParams;
        private readonly string m_WorkDir;
        private readonly short m_DebugLevel;
        private readonly bool mPostResultsToDB;

        private struct udtMassErrorInfoType
        {
            public string DatasetName;
            public int PSMJob;
            public double MassErrorPPM;                 // Parent Ion Mass Error, before refinement
            public double MassErrorPPMRefined;          // Parent Ion Mass Error, after refinement
        }

        public clsDtaRefLogMassErrorExtractor(IMgrParams mgrParams, string strWorkDir, short intDebugLevel, bool blnPostResultsToDB)
        {
            m_mgrParams = mgrParams;
            m_WorkDir = strWorkDir;
            m_DebugLevel = intDebugLevel;
            mPostResultsToDB = blnPostResultsToDB;
        }

        private string ConstructXML(udtMassErrorInfoType udtMassErrorInfo)
        {
            var sbXml = new StringBuilder();

            try
            {
                sbXml.Append("<DTARef_MassErrorStats>");

                sbXml.Append(Convert.ToString("<Dataset>") + udtMassErrorInfo.DatasetName + "</Dataset>");
                sbXml.Append(Convert.ToString("<PSM_Source_Job>") + udtMassErrorInfo.PSMJob + "</PSM_Source_Job>");

                sbXml.Append("<Measurements>");
                sbXml.Append(Convert.ToString("<Measurement Name=\"" + "MassErrorPPM" + "\">") + udtMassErrorInfo.MassErrorPPM + "</Measurement>");
                sbXml.Append(Convert.ToString("<Measurement Name=\"" + "MassErrorPPM_Refined" + "\">") + udtMassErrorInfo.MassErrorPPMRefined + "</Measurement>");
                sbXml.Append("</Measurements>");

                sbXml.Append("</DTARef_MassErrorStats>");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error converting Mass Error stats to XML; details:");
                Console.WriteLine(ex);
                return string.Empty;
            }

            return sbXml.ToString();
        }

        public bool ParseDTARefineryLogFile(string strDatasetName, int intDatasetID, int intPSMJob)
        {
            return ParseDTARefineryLogFile(strDatasetName, intDatasetID, intPSMJob, m_WorkDir);
        }

        public bool ParseDTARefineryLogFile(string strDatasetName, int intDatasetID, int intPSMJob, string strWorkDirPath)
        {
            var blnOriginalDistributionSection = false;
            var blnRefinedDistributionSection = false;

            var reMassError = new Regex(@"Robust estimate[ \t]+([^\t ]+)", RegexOptions.Compiled);

            try
            {
                var udtMassErrorInfo = new udtMassErrorInfoType
                {
                    DatasetName = strDatasetName,
                    PSMJob = intPSMJob,
                    MassErrorPPM = double.MinValue,
                    MassErrorPPMRefined = double.MinValue
                };

                var fiSourceFile = new FileInfo(Path.Combine(strWorkDirPath, strDatasetName + "_dta_DtaRefineryLog.txt"));
                if (!fiSourceFile.Exists)
                {
                    OnErrorEvent("DtaRefinery Log file not found; " + fiSourceFile.FullName);
                    return false;
                }

                using (var srSourceFile = new StreamReader(new FileStream(fiSourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srSourceFile.EndOfStream)
                    {
                        var strLineIn = srSourceFile.ReadLine();
                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        if (strLineIn.IndexOf("ORIGINAL parent ion mass error distribution", StringComparison.InvariantCultureIgnoreCase) >= 0)
                        {
                            blnOriginalDistributionSection = true;
                            blnRefinedDistributionSection = false;
                        }

                        if (strLineIn.IndexOf("REFINED parent ion mass error distribution", StringComparison.InvariantCultureIgnoreCase) >= 0)
                        {
                            blnOriginalDistributionSection = false;
                            blnRefinedDistributionSection = true;
                        }

                        if (!strLineIn.StartsWith("Robust estimate", StringComparison.InvariantCultureIgnoreCase))
                            continue;

                        var reMatch = reMassError.Match(strLineIn);

                        if (reMatch.Success)
                        {
                            double dblMassError;
                            if (double.TryParse(reMatch.Groups[1].Value, out dblMassError))
                            {
                                if (blnOriginalDistributionSection)
                                {
                                    udtMassErrorInfo.MassErrorPPM = dblMassError;
                                }

                                if (blnRefinedDistributionSection)
                                {
                                    udtMassErrorInfo.MassErrorPPMRefined = dblMassError;
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

                if (udtMassErrorInfo.MassErrorPPM > double.MinValue)
                {
                    var strXMLResults = ConstructXML(udtMassErrorInfo);

                    if (mPostResultsToDB)
                    {
                        var blnSuccess = PostMassErrorInfoToDB(intDatasetID, strXMLResults);

                        if (!blnSuccess)
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

        private bool PostMassErrorInfoToDB(int intDatasetID, string strXMLResults)
        {
            const int MAX_RETRY_COUNT = 3;

            try
            {
                // Call stored procedure STORE_MASS_ERROR_STATS_SP_NAME in DMS5

                var objCommand = new SqlCommand
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandText = STORE_MASS_ERROR_STATS_SP_NAME
                };

                objCommand.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                objCommand.Parameters.Add(new SqlParameter("@DatasetID", SqlDbType.Int)).Value = intDatasetID;
                objCommand.Parameters.Add(new SqlParameter("@ResultsXML", SqlDbType.Xml)).Value = strXMLResults;

                var objAnalysisTask = new clsAnalysisJob(m_mgrParams, m_DebugLevel);

                // Execute the SP (retry the call up to 4 times)
                var ResCode = objAnalysisTask.DMSProcedureExecutor.ExecuteSP(objCommand, MAX_RETRY_COUNT);

                if (ResCode == 0)
                {
                    return true;
                }

                OnErrorEvent("Error storing DTA Refinery Mass Error Results in the database, " + STORE_MASS_ERROR_STATS_SP_NAME + " returned " + ResCode);
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
