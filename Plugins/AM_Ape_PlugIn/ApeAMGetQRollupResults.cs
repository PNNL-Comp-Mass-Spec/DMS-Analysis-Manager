using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using AnalysisManagerBase;
using AnalysisManagerBase.JobConfig;
using PRISMDatabaseUtils;

namespace AnalysisManager_Ape_PlugIn
{
    internal class ApeAMGetQRollupResults : ApeAMBase
    {
        // ReSharper disable once CommentTypo
        // Ignore Spelling: mtuser, uid

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="mgrParams"></param>
        public ApeAMGetQRollupResults(IJobParams jobParams, IMgrParams mgrParams) : base(jobParams, mgrParams)
        {
        }

        /// <summary>
        /// Setup and run Ape pipeline according to job parameters
        /// </summary>
        public bool GetQRollupResults()
        {
            var success = GetQRollupResultsAll();
            return success;
        }

        private bool GetQRollupResultsAll()
        {
            var success = true;
            var mHandle = new Ape.SqlConversionHandler((done, conversionSuccess, _, msg) =>
            {
                OnStatusEvent(msg);

                if (done)
                {
                    if (conversionSuccess)
                    {
                        OnStatusEvent("Ape successfully created QRollup database." + GetJobParam("ApeWorkflowName"));
                        success = true;
                    }
                    else
                    {
                        mErrorMessage = "Error running Ape in GetQRollupResultsAll";
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
                apeMTSServerName = GetJobParam("QRollupMTSServer");
            }
            if (string.IsNullOrEmpty(apeMTSDatabaseName))
            {
                apeMTSDatabaseName = GetJobParam("QRollupMTSDatabase");
            }

            var apeDatabase = Path.Combine(mWorkingDir, "Results.db3");

            // ReSharper disable StringLiteralTypo
            var paramList = new List<string>
            {
                apeMTSDatabaseName + ";@MTDBName;" + apeMTSDatabaseName + ";False;sqldbtype.varchar;;",
                "1;@ReturnPeptidesTable;1;True;sqldbtype.tinyint;" + apeMTSDatabaseName + "_Peptides;sqldbtype.tinyint",
                "1;@ReturnExperimentsTable;1;True;sqldbtype.tinyint;" + apeMTSDatabaseName + "_Experiments;sqldbtype.tinyint"
            };

            var dotnetConnString = "Server=" + apeMTSServerName + ";database=" + apeMTSDatabaseName + ";uid=mtuser;Password=mt4fun";

            // ReSharper restore StringLiteralTypo

            Ape.SqlServerToSQLite.ProgressChanged += OnProgressChanged;
            var qidList = GetQIDList();

            if (string.IsNullOrEmpty(qidList))
            {
                return false;
            }

            Ape.SqlServerToSQLite.ConvertDatasetToSQLiteFile(paramList, (int)SqlServerToSqlLiteConversionMode.QRollupResults, dotnetConnString, qidList, apeDatabase, mHandle);

            return success;
        }

        private string GetQIDList()
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
            sqlText.Append("SELECT DISTINCT R.qid FROM V_Mage_Data_Package_Analysis_Jobs J ");
            sqlText.Append("INNER JOIN V_MTS_PM_Results_List_Report R on R.Job = J.job ");
            sqlText.AppendFormat("WHERE J.data_package_id = {0} AND R.task_database = '{1}'", dataPackageID, apeMTSDatabaseName);

            // Add State if defined MD_State will typically be 2=OK or 5=Superseded
            if (!string.IsNullOrEmpty(GetJobParam("ApeMDState")))
            {
                sqlText.AppendFormat(" AND R.md_state = {0}", GetJobParam("ApeMDState"));
            }

            // Add INI filename if defined
            if (!string.IsNullOrEmpty(GetJobParam("ApeMDIniFilename")))
            {
                sqlText.AppendFormat(" AND R.ini_file_name = '{0}'", GetJobParam("ApeMDIniFilename"));
            }

            var qidList = string.Empty;
            var qidCount = 0;

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrParams.ManagerName);

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: mMgrParams.TraceMode);
            RegisterEvents(dbTools);

            // Get the matching QIDs for this data package
            var success = dbTools.GetQueryResults(sqlText.ToString(), out var results);

            if (success)
            {
                foreach (var result in results.SelectMany(x => x).Where(x => !string.IsNullOrWhiteSpace(x)))
                {
                    qidList += result + ", ";
                    qidCount++;
                }
            }

            if (string.IsNullOrEmpty(qidList))
            {
                mErrorMessage = "QIDs not found via query " + sqlText;
            }
            else
            {
                OnStatusEvent("Retrieving " + qidCount + " QIDs in ApeAMGetQRollupResults");
                OnDebugEvent("QID list: " + qidList);
            }

            return qidList;
        }
    }
}
