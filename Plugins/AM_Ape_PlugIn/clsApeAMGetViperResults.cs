using System;
using System.Collections.Generic;
using System.IO;
using System.Data.SqlClient;
using AnalysisManagerBase;

namespace AnalysisManager_Ape_PlugIn
{
    class clsApeAMGetViperResults : clsApeAMBase
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
        public clsApeAMGetViperResults(IJobParams jobParms, IMgrParams mgrParms) : base(jobParms, mgrParms)
        {           
        }

        #endregion


        /// <summary>
        /// Setup and run Ape pipeline according to job parameters
        /// </summary>
        public bool GetQRollupResults(String dataPackageID)
        {
            bool blnSuccess = true;
            blnSuccess = GetViperResultsAll();
            return blnSuccess;
        }

        private bool GetViperResultsAll()
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

            string apeMTSServerName = GetJobParam("ApeMTSServer");
            string apeMTSDatabaseName = GetJobParam("ApeMTSDatabase");
            string apeDatabase = Path.Combine(mWorkingDir, "Results.db3");

            List<string> paramList = new List<string>();
            paramList.Add("1;@ReturnMTTable;1;True;sqldbtype.tinyint;T_Mass_Tags;sqldbtype.tinyint");
            paramList.Add("1;@ReturnProteinTable;1;True;sqldbtype.tinyint;T_Proteins;sqldbtype.tinyint");
            paramList.Add("1;@ReturnProteinMapTable;1;True;sqldbtype.tinyint;T_Mass_Tag_to_Protein_Map;sqldbtype.tinyint");

            string dotnetConnString = "Server=" + apeMTSServerName + ";database=" + apeMTSDatabaseName + ";uid=mtuser;Password=mt4fun";

            Ape.SqlServerToSQLite.ProgressChanged += new Ape.SqlServerToSQLite.ProgressChangedEventHandler(OnProgressChanged);
            Ape.SqlServerToSQLite.ConvertDatasetToSQLiteFile(paramList, 0, dotnetConnString, GetIDList(), apeDatabase, mHandle);

            return blnSuccess;
        }

        private string GetIDList()
        {
            string constr = RequireMgrParam("connectionstring");
            string sqlText = "SELECT vmts.MD_ID FROM V_Mage_Data_Package_Analysis_Jobs vdp " +
                             "join V_MTS_PM_Results_List_Report vmts on vmts.Job = vdp.Job " +
                             "WHERE Data_Package_ID = " + GetJobParam("DataPackageID") + " and Tool like 'Decon2LS%'";

            //Add State if defined MD_State will typically be 2=OK or 5=Superseded
            if (!string.IsNullOrEmpty(GetJobParam("ApeMDState")))
            {
            sqlText = sqlText + " and MD_State = " + GetJobParam("ApeMDState");
            };

            //Add ini filename if defined
            if (!string.IsNullOrEmpty(GetJobParam("ApeMDIniFilename")))
            {
                sqlText = sqlText + " and Ini_File_Name = '" + GetJobParam("ApeMDIniFilename") + "'";
            };

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
                        if (!string.IsNullOrEmpty(reader[0].ToString()))
                        {
                            expList += reader[0].ToString() + ", ";
                        }
                    }
                }
            }

            return expList;
        }


    }
}
