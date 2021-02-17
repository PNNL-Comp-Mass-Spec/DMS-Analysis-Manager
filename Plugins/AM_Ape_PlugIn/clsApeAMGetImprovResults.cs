using AnalysisManagerBase;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PRISMDatabaseUtils;

namespace AnalysisManager_Ape_PlugIn
{
    internal class clsApeAMGetImprovResults : clsApeAMBase
    {
        #region Member Variables

        #endregion

        #region Constructors

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="mgrParams"></param>
        public clsApeAMGetImprovResults(IJobParams jobParams, IMgrParams mgrParams) : base(jobParams, mgrParams)
        {
        }

        #endregion

        /// <summary>
        /// Setup and run Ape pipeline according to job parameters
        /// </summary>
        public bool GetImprovResults(string dataPackageID)
        {
            var success = GetImprovResultsAll();
            return success;
        }

        private bool GetImprovResultsAll()
        {
            var success = true;
            var mHandle = new Ape.SqlConversionHandler(delegate (bool done, bool conversionSuccess, int percent, string msg)
            {
                OnStatusEvent(msg);

                if (done)
                {
                    if (conversionSuccess)
                    {
                        OnStatusEvent("Ape successfully created Improv database." + GetJobParam("ApeWorkflowName"));
                        success = true;
                    }
                    else
                    {
                        mErrorMessage = "Error running Ape in GetImprovResultsAll";
                        OnErrorEvent(mErrorMessage);
                        success = false;
                    }
                }
            });

            var apeMTSServerName = GetJobParam("ApeMTSServer");
            var apeMTSDatabaseName = GetJobParam("ApeMTSDatabase");

            // Need these for backward compatibility
            if (string.IsNullOrEmpty(apeMTSServerName))
            {
                apeMTSServerName = GetJobParam("ImprovMTSServer");
            }
            if (string.IsNullOrEmpty(apeMTSDatabaseName))
            {
                apeMTSDatabaseName = GetJobParam("ImprovMTSDatabase");
            }
            var apeImprovMinPMTQuality = GetJobParam("ImprovMinPMTQuality");
            // var apeMSGFThreshold = GetJobParam("ImprovMSGFThreshold");
            var apeDatabase = Path.Combine(mWorkingDir, "Results.db3");

            var paramList = new List<string>
            {
                //  paramList.Add(apeMTSDatabaseName + ";@MTDBName;" + apeMTSDatabaseName + ";False;sqldbtype.varchar;;");
                apeImprovMinPMTQuality + ";@MinimumPMTQualityScore;0;False;sqldbtype.real;;",
                "0.1;@MSGFThreshold;0.1;False;sqldbtype.real;;",
                "1;@ReturnJobInfoTable;1;True;sqldbtype.tinyint;T_Analysis_Description;sqldbtype.tinyint",
                "1;@ReturnProteinMapTable;1;True;sqldbtype.tinyint;T_Mass_Tag_to_Protein_Map;sqldbtype.tinyint",
                "1;@ReturnProteinTable;1;True;sqldbtype.tinyint;T_Proteins;sqldbtype.tinyint",
                "1;@ReturnMTTable;1;True;sqldbtype.tinyint;T_Mass_Tags;sqldbtype.tinyint",
                "1;@ReturnPeptideTable;1;True;sqldbtype.tinyint;T_Peptides;sqldbtype.tinyint"
            };

            var dotnetConnString = "Server=" + apeMTSServerName + ";database=" + apeMTSDatabaseName + ";uid=mtuser;Password=mt4fun";
            // mCurrentDBConnectionString = "Provider=sqloledb;Data Source=Albert;Initial Catalog=MT_Sea_Sediments_SBI_P590;User ID=mtuser;Password=mt4fun"
            Ape.SqlServerToSQLite.ProgressChanged += OnProgressChanged;
            var jobList = GetJobIDList();
            if (string.IsNullOrEmpty(jobList))
            {
                return false;
            }

            Ape.SqlServerToSQLite.ConvertDatasetToSQLiteFile(paramList, (int)eSqlServerToSqlLiteConversionMode.AMTTagDbJobs, dotnetConnString, jobList, apeDatabase, mHandle);

            return success;
        }

        // Unused function
        // private string GetExperimentList()
        // {
        //    string connectionString = RequireMgrParam("ConnectionString");
        //    string sqlText = "Select Experiment From dbo.V_MAC_Data_Package_Experiments Where Data_Package_ID = " + GetJobParam("DataPackageID");
        //    string expList = string.Empty;
        //    using (SqlConnection conn = new SqlConnection(connectionString))
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
        // }

        private string GetJobIDList()
        {
            var connectionString = RequireMgrParam("ConnectionString");
            var dataPackageID = GetJobParam("DataPackageID");

            if (string.IsNullOrEmpty(dataPackageID))
            {
                mErrorMessage = "Data Package ID not defined via job parameter dataPackageID";
                return string.Empty;
            }

            var sqlText = "SELECT Job FROM V_Mage_Data_Package_Analysis_Jobs " +
                          "WHERE Data_Package_ID = " + dataPackageID + " and Tool Like 'Sequest%'";

            var jobList = string.Empty;
            var jobCount = 0;

            var dbTools = DbToolsFactory.GetDBTools(connectionString, debugMode: mMgrParams.TraceMode);
            RegisterEvents(dbTools);

            // Get the matching jobs from the Data Package
            var success = dbTools.GetQueryResults(sqlText, out var results);
            if (success)
            {
                foreach (var result in results.SelectMany(x => x).Where(x => !string.IsNullOrWhiteSpace(x)))
                {
                    jobList += result + ", ";
                    jobCount++;
                }
            }

            if (string.IsNullOrEmpty(jobList))
            {
                mErrorMessage = "Jobs not found via query " + sqlText;
            }
            else
            {
                OnStatusEvent("Retrieving " + jobCount + " jobs in clsApeAMGetImprovResults");
                OnDebugEvent("Job list: " + jobList);
            }

            return jobList;
        }
    }
}
