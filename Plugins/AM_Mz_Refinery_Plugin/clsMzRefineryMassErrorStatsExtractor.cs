using System;
using System.Data;
using System.Data.SqlClient;
using AnalysisManagerBase;
using System.IO;
using System.Linq;
using System.Text;

namespace AnalysisManagerMzRefineryPlugIn
{
    /// <summary>
    /// This class reads the console text from the PPMErrorCharter's console output and extracts the parent ion mass error information
    /// It passes on the information to DMS for storage in table T_Dataset_QC
    /// </summary>
    /// <remarks></remarks>
    public class clsMzRefineryMassErrorStatsExtractor
    {
        private const string STORE_MASS_ERROR_STATS_SP_NAME = "StoreDTARefMassErrorStats";
        private readonly IMgrParams m_mgrParams;
        private readonly short m_DebugLevel;

        private readonly bool mPostResultsToDB;

        public string ErrorMessage { get; private set; }

        private struct udtMassErrorInfoType
        {
            /// <summary>
            /// Dataset name
            /// </summary>
            public string DatasetName;

            /// <summary>
            /// Analysis Job number
            /// </summary>
            public int PSMJob;

            /// <summary>
            /// Parent Ion Mass Error, before refinement
            /// </summary>
            public double MassErrorPPM;

            /// <summary>
            /// Parent Ion Mass Error, after refinement
            /// </summary>
            public double MassErrorPPMRefined;
        }


        public clsMzRefineryMassErrorStatsExtractor(IMgrParams mgrParams, short debugLevel, bool postResultsToDB)
        {
            m_mgrParams = mgrParams;
            m_DebugLevel = debugLevel;
            mPostResultsToDB = postResultsToDB;

            ErrorMessage = string.Empty;
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

        public bool ParsePPMErrorCharterOutput(string strDatasetName, int intDatasetID, int intPSMJob, string ppmErrorCharterConsoleOutputFilePath)
        {
            // Parse the Console Output file to extract the mass error reported in this table
            //
            // Using fixed data file "E:\DMS_WorkDir\Pcarb001_LTQFT_run1_23Sep05_Andro_0705-06_FIXED.mzML"
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
                var udtMassErrorInfo = new udtMassErrorInfoType
                {
                    DatasetName = strDatasetName,
                    PSMJob = intPSMJob,
                    MassErrorPPM = double.MinValue,
                    MassErrorPPMRefined = double.MinValue
                };

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
                            udtMassErrorInfo.MassErrorPPM = massError;
                        }

                        if (dataValues.Count > 1 && double.TryParse(dataValues.Last(), out massError))
                        {
                            udtMassErrorInfo.MassErrorPPMRefined = massError;
                        }
                    }
                }

                if (Math.Abs(udtMassErrorInfo.MassErrorPPM - double.MinValue) < float.Epsilon)
                {
                    ErrorMessage = "Did not find '" + MASS_ERROR_PPM + "' in the PPM Error Charter output";
                    return false;
                }

                if (Math.Abs(udtMassErrorInfo.MassErrorPPMRefined - double.MinValue) < float.Epsilon)
                {
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
                ErrorMessage = "Exception in ParsePPMErrorCharterOutput: " + ex.Message;
                return false;
            }

            return true;
        }

        private bool PostMassErrorInfoToDB(int datasetID, string xmlResults)
        {
            const int MAX_RETRY_COUNT = 3;

            bool success;

            try
            {
                // Call stored procedure StoreDTARefMassErrorStats in DMS5
                // Data is stored in table T_Dataset_QC

                var sqlCmd = new SqlCommand
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandText = STORE_MASS_ERROR_STATS_SP_NAME
                };

                sqlCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                sqlCmd.Parameters.Add(new SqlParameter("@DatasetID", SqlDbType.Int)).Value = datasetID;
                sqlCmd.Parameters.Add(new SqlParameter("@ResultsXML", SqlDbType.Xml)).Value = xmlResults;

                var analysisTask = new clsAnalysisJob(m_mgrParams, m_DebugLevel);

                // Execute the SP (retry the call up to 4 times)
                var resCode = analysisTask.DMSProcedureExecutor.ExecuteSP(sqlCmd, MAX_RETRY_COUNT);

                if (resCode == 0)
                {
                    success = true;
                }
                else
                {
                    ErrorMessage = "Error storing MzRefinery Mass Error Results in the database, " + STORE_MASS_ERROR_STATS_SP_NAME + " returned " + resCode;
                    success = false;
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = "Exception storing MzRefinery Mass Error Results in the database: " + ex.Message;
                success = false;
            }

            return success;
        }
    }
}
