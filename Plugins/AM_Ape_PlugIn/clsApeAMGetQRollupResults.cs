using System;
using System.Collections.Generic;
using System.IO;
using System.Data.SqlClient;
using AnalysisManagerBase;

namespace AnalysisManager_Ape_PlugIn
{
    class clsApeAMGetQRollupResults : clsApeAMBase
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
        public clsApeAMGetQRollupResults(IJobParams jobParms, IMgrParams mgrParms) : base(jobParms, mgrParms)
        {           
        }

        #endregion

        /// <summary>
        /// Setup and run Ape pipeline according to job parameters
        /// </summary>
        public bool GetQRollupResults(String dataPackageID)
        {
            bool blnSuccess = true;
            blnSuccess = GetQRollupResultsAll();
            return blnSuccess;
        }

        private bool GetQRollupResultsAll()
        {
            bool blnSuccess = true;
			Ape.SqlConversionHandler mHandle = new Ape.SqlConversionHandler(delegate(bool done, bool success, int percent, string msg)
            {
                Console.WriteLine(msg);

                if (done)
                {
                    if (success)
                    {
                        //m_message = "Ape successfully ran workflow" + GetJobParam("ApeWorkflowName");
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Ape successfully created QRollup database." + GetJobParam("ApeWorkflowName"));
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

            string apeQRollupMTSServerName = GetJobParam("QRollupMTSServer");
            string apeQRollupMTSDatabaseName = GetJobParam("QRollupMTSDatabase");
            string apeDatabase = Path.Combine(mWorkingDir, "Results.db3");

            List<string> paramList = new List<string>();
            paramList.Add(apeQRollupMTSDatabaseName + ";@MTDBName;" + apeQRollupMTSDatabaseName + ";False;sqldbtype.varchar;;");
            paramList.Add("1;@ReturnPeptidesTable;1;True;sqldbtype.tinyint;" + apeQRollupMTSDatabaseName + "_Peptides;sqldbtype.tinyint");
            paramList.Add("1;@ReturnExperimentsTable;1;True;sqldbtype.tinyint;" + apeQRollupMTSDatabaseName + "_Experiments;sqldbtype.tinyint");

            string dotnetConnString = "Server=" + apeQRollupMTSServerName + ";database=" + apeQRollupMTSDatabaseName + ";uid=mtuser;Password=mt4fun";

			Ape.SqlServerToSQLite.ProgressChanged += new Ape.SqlServerToSQLite.ProgressChangedEventHandler(OnProgressChanged);
			Ape.SqlServerToSQLite.ConvertDatasetToSQLiteFile(paramList, 5, dotnetConnString, GetIDList(), apeDatabase, mHandle);
            
            return blnSuccess;
        }

        private string GetIDList()
        {
            string constr = RequireMgrParam("connectionstring");
            string sqlText = "SELECT vmts.QID FROM V_Mage_Data_Package_Analysis_Jobs vdp " +
                             "join V_MTS_PM_Results_List_Report vmts on vmts.Job = vdp.Job " + 
                             "WHERE Data_Package_ID = " + GetJobParam("DataPackageID") + " and Tool = 'Decon2LS_V2'";
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
