using System;
using System.Collections.Generic;
using System.IO;
using System.Data.SqlClient;
using AnalysisManagerBase;
using Ape;

namespace AnalysisManager_Ape_PlugIn
{
    class clsApeAMGetImprovResults : clsApeAMBase
    {
        #region Member Variables
   
        /// <summary>
        /// The parameters for the running a workflow
        /// </summary>
        private static bool _shouldExit = false;

        #endregion

            #region Constructors 

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParms"></param>
        /// <param name="mgrParms"></param>
        /// <param name="monitor"></param>
        public clsApeAMGetImprovResults(IJobParams jobParms, IMgrParams mgrParms) : base(jobParms, mgrParms)
        {           
        }

        #endregion

        /// <summary>
        /// Setup and run Mage Extractor pipleline according to job parameters
        /// </summary>
        public bool GetImprovResults(String dataPackageID)
        {
            bool blnSuccess = true;
            blnSuccess = GetImprovResultsAll();
            return blnSuccess;
        }

        private bool GetImprovResultsAll()
        {
            bool blnSuccess = true;
            SqlConversionHandler mHandle = new SqlConversionHandler(delegate(bool done, bool success, int percent, string msg)
            {
                Console.WriteLine(msg);

                if (done)
                {
                    if (success)
                    {
                        //m_message = "Ape successfully ran workflow" + GetJobParam("ApeWorkflowName");
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Ape successfully ran workflow" + GetJobParam("ApeWorkflowName"));
                        blnSuccess = true;
                    }
                    else
                    {
                        if (!_shouldExit)
                        {
                            //m_message = "Error running Ape";
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running Ape");
                            blnSuccess = false;
                        }
                    }
                }

            });

            string apeImprovMTSServerName = GetJobParam("ImprovMTSServer");
            string apeImprovMTSDatabaseName = GetJobParam("ImprovMTSDatabase");
            string apeImprovMinPMTQuality = GetJobParam("ImprovMinPMTQuality");
            string apeDatabase = Path.Combine(mWorkingDir, "Results.db3");

            List<string> paramList = new List<string>();
            paramList.Add(apeImprovMTSDatabaseName + ";@MTDBName;" + apeImprovMTSDatabaseName + ";False;sqldbtype.varchar;;");
            paramList.Add(apeImprovMinPMTQuality + ";@minimumPMTQualityScore;0;False;sqldbtype.real;;");
            paramList.Add("1;@ReturnPeptidesTable;1;True;sqldbtype.tinyint;" + apeImprovMTSDatabaseName + "_Peptides;sqldbtype.tinyint");
            paramList.Add("1;@ReturnExperimentsTable;1;True;sqldbtype.tinyint;" + apeImprovMTSDatabaseName + "_Experiments;sqldbtype.tinyint");

            string dotnetConnString = "Server=" + apeImprovMTSServerName + ";database=PRISM_IFC;uid=mtuser;Password=mt4fun";

            SqlServerToSQLite.ConvertDatasetToSQLiteFile(paramList, 4, dotnetConnString, GetExperimentList(), apeDatabase, mHandle);

            return blnSuccess;
        }

        private string GetExperimentList()
        {
            string constr = RequireMgrParam("connectionstring");
            string sqlText = "Select Experiment From dbo.V_MAC_Data_Package_Experiments Where Data_Package_ID = " + GetJobParam("DataPackageID");
            string expList = string.Empty;
            using (SqlConnection conn = new SqlConnection(constr))
            {
                conn.Open();
                // Get the experiments from the Data Package
                SqlCommand query = new SqlCommand(sqlText, conn);
                using (SqlDataReader reader = query.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        expList += reader[0].ToString() + ", ";
                    }
                }
            }

            return expList;
        }



    }
}
