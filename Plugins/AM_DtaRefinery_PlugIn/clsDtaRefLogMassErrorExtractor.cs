using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerDtaRefineryPlugIn
{
    /// <summary>
    /// This class reads a DTA_Refinery log file to extract the parent ion mass error information
    /// It passes on the information to DMS for storage in table T_Dataset_QC
    /// </summary>
    /// <remarks></remarks>
    public class clsDtaRefLogMassErrorExtractor
    {
        private const string STORE_MASS_ERROR_STATS_SP_NAME = "StoreDTARefMassErrorStats";

        private readonly IMgrParams m_mgrParams;
        private readonly string m_WorkDir;
        private readonly short m_DebugLevel;
        private readonly bool mPostResultsToDB;

        private string mErrorMessage;

        private struct udtMassErrorInfoType
        {
            public string DatasetName;
            public int DatasetID;
            public int PSMJob;
            public double MassErrorPPM;                 // Parent Ion Mass Error, before refinement
            public double MassErrorPPMRefined;          // Parent Ion Mass Error, after refinement
        }

        public string ErrorMessage
        {
            get { return mErrorMessage; }
        }

        public clsDtaRefLogMassErrorExtractor(IMgrParams mgrParams, string strWorkDir, short intDebugLevel, bool blnPostResultsToDB)
        {
            m_mgrParams = mgrParams;
            m_WorkDir = strWorkDir;
            m_DebugLevel = intDebugLevel;
            mPostResultsToDB = blnPostResultsToDB;

            mErrorMessage = string.Empty;
        }

        private string ConstructXML(udtMassErrorInfoType udtMassErrorInfo)
        {
            var sbXml = new StringBuilder();

            try
            {
                sbXml.Append("<DTARef_MassErrorStats>");

                sbXml.Append((Convert.ToString("<Dataset>") + udtMassErrorInfo.DatasetName) + "</Dataset>");
                sbXml.Append((Convert.ToString("<PSM_Source_Job>") + udtMassErrorInfo.PSMJob) + "</PSM_Source_Job>");

                sbXml.Append("<Measurements>");
                sbXml.Append((Convert.ToString("<Measurement Name=\"" + "MassErrorPPM" + "\">") + udtMassErrorInfo.MassErrorPPM) + "</Measurement>");
                sbXml.Append((Convert.ToString("<Measurement Name=\"" + "MassErrorPPM_Refined" + "\">") + udtMassErrorInfo.MassErrorPPMRefined) + "</Measurement>");
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

            udtMassErrorInfoType udtMassErrorInfo = new udtMassErrorInfoType();

            var reMassError = new Regex(@"Robust estimate[ \t]+([^\t ]+)", RegexOptions.Compiled);

            string strLineIn = null;

            try
            {
                udtMassErrorInfo = new udtMassErrorInfoType();
                udtMassErrorInfo.DatasetName = strDatasetName;
                udtMassErrorInfo.DatasetID = intDatasetID;
                udtMassErrorInfo.PSMJob = intPSMJob;
                udtMassErrorInfo.MassErrorPPM = double.MinValue;
                udtMassErrorInfo.MassErrorPPMRefined = double.MinValue;

                var fiSourceFile = new FileInfo(Path.Combine(strWorkDirPath, strDatasetName + "_dta_DtaRefineryLog.txt"));
                if (!fiSourceFile.Exists)
                {
                    mErrorMessage = "DtaRefinery Log file not found";
                    return false;
                }

                using (var srSourceFile = new StreamReader(new FileStream(fiSourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srSourceFile.EndOfStream)
                    {
                        strLineIn = srSourceFile.ReadLine();

                        if (strLineIn.Contains("ORIGINAL parent ion mass error distribution"))
                        {
                            blnOriginalDistributionSection = true;
                            blnRefinedDistributionSection = false;
                        }

                        if (strLineIn.Contains("REFINED parent ion mass error distribution"))
                        {
                            blnOriginalDistributionSection = false;
                            blnRefinedDistributionSection = true;
                        }

                        if (strLineIn.StartsWith("Robust estimate"))
                        {
                            var reMatch = reMassError.Match(strLineIn);

                            double dblMassError = 0;
                            if (reMatch.Success)
                            {
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
                                    mErrorMessage = "Unable to extract mass error value from 'Robust estimate' line in the DTA Refinery log file; RegEx capture is not a number: " + reMatch.Groups[1].Value;
                                    return false;
                                }
                            }
                            else
                            {
                                mErrorMessage = "Unable to extract mass error value from 'Robust estimate' line in the DTA Refinery log file; RegEx match failed";
                                return false;
                            }
                        }
                    }
                }

                if (udtMassErrorInfo.MassErrorPPM > double.MinValue)
                {
                    string strXMLResults = null;

                    strXMLResults = ConstructXML(udtMassErrorInfo);

                    if (mPostResultsToDB)
                    {
                        bool blnSuccess = false;

                        blnSuccess = PostMassErrorInfoToDB(intDatasetID, strXMLResults);

                        if (!blnSuccess)
                        {
                            if (string.IsNullOrEmpty(mErrorMessage))
                            {
                                mErrorMessage = "Unknown error posting Mass Error results from DTA Refinery to the database";
                            }
                            return false;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception in ParseDTARefineryLogFile: " + ex.Message;
                return false;
            }

            return true;
        }

        private bool PostMassErrorInfoToDB(int intDatasetID, string strXMLResults)
        {
            const int MAX_RETRY_COUNT = 3;

            bool blnSuccess = false;

            try
            {
                // Call stored procedure STORE_MASS_ERROR_STATS_SP_NAME in DMS5

                var objCommand = new SqlCommand();

                objCommand.CommandType = CommandType.StoredProcedure;
                objCommand.CommandText = STORE_MASS_ERROR_STATS_SP_NAME;

                objCommand.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                objCommand.Parameters.Add(new SqlParameter("@DatasetID", SqlDbType.Int)).Value = intDatasetID;
                objCommand.Parameters.Add(new SqlParameter("@ResultsXML", SqlDbType.Xml)).Value = strXMLResults;

                var objAnalysisTask = new clsAnalysisJob(m_mgrParams, m_DebugLevel);

                //Execute the SP (retry the call up to 4 times)
                int ResCode = 0;
                ResCode = objAnalysisTask.DMSProcedureExecutor.ExecuteSP(objCommand, MAX_RETRY_COUNT);

                if (ResCode == 0)
                {
                    blnSuccess = true;
                }
                else
                {
                    mErrorMessage = "Error storing DTA Refinery Mass Error Results in the database, " + STORE_MASS_ERROR_STATS_SP_NAME + " returned " + ResCode.ToString();
                    blnSuccess = false;
                }
            }
            catch (Exception ex)
            {
                mErrorMessage = "Exception storing DTA Refinery Mass Error Results in the database: " + ex.Message;
                blnSuccess = false;
            }

            return blnSuccess;
        }
    }
}
