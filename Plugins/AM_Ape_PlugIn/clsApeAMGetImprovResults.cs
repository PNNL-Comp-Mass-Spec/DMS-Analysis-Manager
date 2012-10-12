using System;
using System.Collections.Generic;
using System.IO;
using System.Data.SqlClient;
using AnalysisManagerBase;

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
        /// Setup and run Ape pipeline according to job parameters
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
			Ape.SqlConversionHandler mHandle = new Ape.SqlConversionHandler(delegate(bool done, bool success, int percent, string msg)
            {
                Console.WriteLine(msg);

                if (done)
                {
                    if (success)
                    {
                        //m_message = "Ape successfully ran workflow" + GetJobParam("ApeWorkflowName");
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Ape successfully created Improv datbase." + GetJobParam("ApeWorkflowName"));
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
            //Need these for backward compatibility
            if (string.IsNullOrEmpty(apeMTSServerName))
            {
                apeMTSServerName = GetJobParam("ImprovMTSServer");
            }
            if (string.IsNullOrEmpty(apeMTSDatabaseName))
            {
                apeMTSDatabaseName = GetJobParam("ImprovMTSDatabase");
            }
            string apeImprovMinPMTQuality = GetJobParam("ImprovMinPMTQuality");
            string apeMSGFThreshold = GetJobParam("ImprovMSGFThreshold");
            string apeDatabase = Path.Combine(mWorkingDir, "Results.db3");

            List<string> paramList = new List<string>();
//            paramList.Add(apeMTSDatabaseName + ";@MTDBName;" + apeMTSDatabaseName + ";False;sqldbtype.varchar;;");
            paramList.Add(apeImprovMinPMTQuality + ";@MinimumPMTQualityScore;0;False;sqldbtype.real;;");
            paramList.Add("0.1;@MSGFThreshold;0.1;False;sqldbtype.real;;");

            
            paramList.Add("1;@ReturnJobInfoTable;1;True;sqldbtype.tinyint;T_Analysis_Description;sqldbtype.tinyint");
            paramList.Add("1;@ReturnProteinMapTable;1;True;sqldbtype.tinyint;T_Mass_Tag_to_Protein_Map;sqldbtype.tinyint");
            paramList.Add("1;@ReturnProteinTable;1;True;sqldbtype.tinyint;T_Proteins;sqldbtype.tinyint");
            paramList.Add("1;@ReturnMTTable;1;True;sqldbtype.tinyint;T_Mass_Tags;sqldbtype.tinyint");
            paramList.Add("1;@ReturnPeptideTable;1;True;sqldbtype.tinyint;T_Peptides;sqldbtype.tinyint");


            string dotnetConnString = "Server=" + apeMTSServerName + ";database=" + apeMTSDatabaseName+ ";uid=mtuser;Password=mt4fun";
            //mCurrentDBConnectionString = "Provider=sqloledb;Data Source=Albert;Initial Catalog=MT_Sea_Sediments_SBI_P590;User ID=mtuser;Password=mt4fun"
			Ape.SqlServerToSQLite.ProgressChanged += new Ape.SqlServerToSQLite.ProgressChangedEventHandler(OnProgressChanged);
			string jobList = GetJobIDList();
			if (string.IsNullOrEmpty(jobList))
			{
				return false;
			}

			Ape.SqlServerToSQLite.ConvertDatasetToSQLiteFile(paramList, (int)eSqlServerToSqlLiteConversionMode.AMTTagDbJobs, dotnetConnString, jobList, apeDatabase, mHandle);

            return blnSuccess;
        }

		// Unused function
		//private string GetExperimentList()
		//{
		//    string constr = RequireMgrParam("connectionstring");
		//    string sqlText = "Select Experiment From dbo.V_MAC_Data_Package_Experiments Where Data_Package_ID = " + GetJobParam("DataPackageID");
		//    string expList = string.Empty;
		//    using (SqlConnection conn = new SqlConnection(constr))
		//    {
		//        conn.Open();
		//        // Get the experiments from the Data Package
		//        SqlCommand query = new SqlCommand(sqlText, conn);
		//        using (SqlDataReader reader = query.ExecuteReader())
		//        {
		//            while (reader.Read())
		//            {
		//                if (!string.IsNullOrEmpty(reader[0].ToString()))
		//                {
		//                expList += reader[0].ToString() + ", ";
		//                }
		//            }
		//        }
		//    }

		//    return expList;
		//}

        private string GetJobIDList()
        {
            string constr = RequireMgrParam("connectionstring");
			string dataPackageID = GetJobParam("DataPackageID");

			if (string.IsNullOrEmpty(dataPackageID))
			{
				mErrorMessage = "Data Package ID not defined via job parameter dataPackageID";
				return string.Empty;
			}

            string sqlText = "SELECT Job FROM V_Mage_Data_Package_Analysis_Jobs " +
							 "WHERE Data_Package_ID = " + dataPackageID + " and Tool Like 'Sequest%'";

            string jobList = string.Empty;
			int intJobCount = 0;
            using (SqlConnection conn = new SqlConnection(constr))
            {
                conn.Open();
                // Get the matching jobs from the Data Package
                SqlCommand query = new SqlCommand(sqlText, conn);
                using (SqlDataReader reader = query.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        if (!string.IsNullOrEmpty(reader[0].ToString()))
                        {
                            jobList += reader[0].ToString() + ", ";
							intJobCount += 1;
                        }
                    }
                }
            }

			if (string.IsNullOrEmpty(jobList))
			{
				mErrorMessage = "Jobs not found via query " + sqlText;
			}
			else
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving " + intJobCount + " jobs in clsApeAMGetImprovResults");
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Job list: " + jobList);
			}

            return jobList;
        }

    }
	
}
