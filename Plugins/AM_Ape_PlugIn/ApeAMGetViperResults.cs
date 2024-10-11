using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using AnalysisManagerBase;
using AnalysisManagerBase.JobConfig;
using PRISMDatabaseUtils;

namespace AnalysisManager_Ape_PlugIn
{
    internal class ApeAMGetViperResults : ApeAMBase
    {
        // ReSharper disable once CommentTypo
        // Ignore Spelling: ini, mtuser, uid, workflow

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="mgrParams"></param>
        public ApeAMGetViperResults(IJobParams jobParams, IMgrParams mgrParams) : base(jobParams, mgrParams)
        {
        }

        /// <summary>
        /// Setup and run Ape pipeline according to job parameters
        /// </summary>
        public bool GetQRollupResults()
        {
            var success = GetViperResultsAll();
            return success;
        }

        private bool GetViperResultsAll()
        {
            var success = true;
            var mHandle = new Ape.SqlConversionHandler((done, conversionSuccess, _, msg) =>
            {
                Console.WriteLine(msg);

                if (done)
                {
                    if (conversionSuccess)
                    {
                        OnStatusEvent("Ape successfully created QRollup database." + GetJobParam("ApeWorkflowName"));
                        success = true;
                    }
                    else
                    {
                        mErrorMessage = "Error using APE to create QRollup database for workflow " + GetJobParam("ApeWorkflowName");
                        OnErrorEvent(mErrorMessage);
                        success = false;
                    }
                }
            });

            var apeMTSServerName = GetJobParam("ApeMTSServer");
            var apeMTSDatabaseName = GetJobParam("ApeMTSDatabase");
            var apeDatabase = Path.Combine(mWorkingDir, "Results.db3");

            var paramList = new List<string>
            {
                "1;@ReturnMTTable;1;True;sqldbtype.tinyint;T_Mass_Tags;sqldbtype.tinyint",
                "1;@ReturnProteinTable;1;True;sqldbtype.tinyint;T_Proteins;sqldbtype.tinyint",
                "1;@ReturnProteinMapTable;1;True;sqldbtype.tinyint;T_Mass_Tag_to_Protein_Map;sqldbtype.tinyint"
            };

            // ReSharper disable once StringLiteralTypo
            var dotnetConnString = "Server=" + apeMTSServerName + ";database=" + apeMTSDatabaseName + ";uid=mtuser;Password=mt4fun";

            Ape.SqlServerToSQLite.ProgressChanged += OnProgressChanged;

            var mdidList = GetMDIDList();

            if (string.IsNullOrEmpty(mdidList))
            {
                return false;
            }

            Ape.SqlServerToSQLite.ConvertDatasetToSQLiteFile(paramList, (int)SqlServerToSqlLiteConversionMode.ViperResults, dotnetConnString, mdidList, apeDatabase, mHandle);

            return success;
        }

        private string GetMDIDList()
        {
            var connectionString = RequireMgrParam("ConnectionString");
            var apeMTSDatabaseName = GetJobParam("ApeMTSDatabase");
            var dataPackageID = GetJobParam("DataPackageID");

            if (string.IsNullOrEmpty(apeMTSDatabaseName))
            {
                mErrorMessage = "MTS Database not defined via job parameter ApeMTSDatabase";
                return string.Empty;
            }

            if (string.IsNullOrEmpty(dataPackageID))
            {
                mErrorMessage = "Data Package ID not defined via job parameter dataPackageID";
                return string.Empty;
            }

            var sqlText = new StringBuilder();
            sqlText.Append("SELECT DISTINCT R.md_id FROM V_Mage_Data_Package_Analysis_Jobs J ");
            sqlText.Append("INNER JOIN V_MTS_PM_Results_List_Report R on R.job = J.job ");
            sqlText.AppendFormat("WHERE J.data_package_id = {0} AND R.task_database = '{1}'", dataPackageID, apeMTSDatabaseName);

            // Add State if defined MD_State will typically be 2=OK or 5=Superseded
            if (!string.IsNullOrEmpty(GetJobParam("ApeMDState")))
            {
                sqlText.AppendFormat(" AND R.md_state = {0}", GetJobParam("ApeMDState"));
            }

            // Add ini filename if defined
            if (!string.IsNullOrEmpty(GetJobParam("ApeMDIniFilename")))
            {
                sqlText.AppendFormat(" AND R.ini_file_name = '{0}'", GetJobParam("ApeMDIniFilename"));
            }

            var mdidList = string.Empty;
            var mdidCount = 0;

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrParams.ManagerName);

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: mMgrParams.TraceMode);
            RegisterEvents(dbTools);

            // Get the matching MD_IDs for this data package
            var success = dbTools.GetQueryResults(sqlText.ToString(), out var results);

            if (success)
            {
                foreach (var result in results.SelectMany(x => x).Where(x => !string.IsNullOrWhiteSpace(x)))
                {
                    mdidList += result + ", ";
                    mdidCount++;
                }
            }

            if (string.IsNullOrEmpty(mdidList))
            {
                mErrorMessage = "MDIDs not found via query " + sqlText;
            }
            else
            {
                OnStatusEvent("Retrieving " + mdidCount + " MDIDs in ApeAMGetViperResults");
                OnDebugEvent("MDID list: " + mdidList);
            }

            return mdidList;
        }
    }
}
