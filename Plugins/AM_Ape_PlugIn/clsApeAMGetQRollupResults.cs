using AnalysisManagerBase;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;

namespace AnalysisManager_Ape_PlugIn
{
    class clsApeAMGetQRollupResults : clsApeAMBase
    {

        #region Member Variables

        #endregion

        #region Constructors

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="mgrParams"></param>
        public clsApeAMGetQRollupResults(IJobParams jobParams, IMgrParams mgrParams) : base(jobParams, mgrParams)
        {
        }

        #endregion

        /// <summary>
        /// Setup and run Ape pipeline according to job parameters
        /// </summary>
        public bool GetQRollupResults(string dataPackageID)
        {
            var success = GetQRollupResultsAll();
            return success;
        }

        private bool GetQRollupResultsAll()
        {
            var success = true;
            var mHandle = new Ape.SqlConversionHandler(delegate (bool done, bool conversionSuccess, int percent, string msg)
            {
                Console.WriteLine(msg);

                if (done)
                {
                    if (conversionSuccess)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, "Ape successfully created QRollup database." + GetJobParam("ApeWorkflowName"));
                        success = true;
                    }
                    else
                    {
                        mErrorMessage = "Error running Ape in GetQRollupResultsAll";
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mErrorMessage);
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

            var paramList = new List<string>
            {
                apeMTSDatabaseName + ";@MTDBName;" + apeMTSDatabaseName + ";False;sqldbtype.varchar;;",
                "1;@ReturnPeptidesTable;1;True;sqldbtype.tinyint;" + apeMTSDatabaseName + "_Peptides;sqldbtype.tinyint",
                "1;@ReturnExperimentsTable;1;True;sqldbtype.tinyint;" + apeMTSDatabaseName + "_Experiments;sqldbtype.tinyint"
            };

            var dotnetConnString = "Server=" + apeMTSServerName + ";database=" + apeMTSDatabaseName + ";uid=mtuser;Password=mt4fun";

            Ape.SqlServerToSQLite.ProgressChanged += OnProgressChanged;
            var QIDList = GetQIDList();
            if (string.IsNullOrEmpty(QIDList))
            {
                return false;
            }

            Ape.SqlServerToSQLite.ConvertDatasetToSQLiteFile(paramList, (int)eSqlServerToSqlLiteConversionMode.QRollupResults, dotnetConnString, QIDList, apeDatabase, mHandle);

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

            var sqlText = "SELECT DISTINCT vmts.QID FROM V_Mage_Data_Package_Analysis_Jobs vdp " +
                             "join V_MTS_PM_Results_List_Report vmts on vmts.Job = vdp.Job " +
                             "WHERE Data_Package_ID = " + dataPackageID + " and Task_Database = '" + apeMTSDatabaseName + "'";

            // Add State if defined MD_State will typically be 2=OK or 5=Superseded
            if (!string.IsNullOrEmpty(GetJobParam("ApeMDState")))
            {
                sqlText = sqlText + " and MD_State = " + GetJobParam("ApeMDState");
            }

            // Add ini filename if defined
            if (!string.IsNullOrEmpty(GetJobParam("ApeMDIniFilename")))
            {
                sqlText = sqlText + " and Ini_File_Name = '" + GetJobParam("ApeMDIniFilename") + "'";
            }

            var qidList = string.Empty;
            var qidCount = 0;
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                // Get the matching QIDs for this data package
                var query = new SqlCommand(sqlText, conn);
                using (var reader = query.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        if (!string.IsNullOrEmpty(reader[0].ToString()))
                        {
                            qidList += reader[0] + ", ";
                            qidCount += 1;
                        }
                    }
                }
            }

            if (string.IsNullOrEmpty(qidList))
            {
                mErrorMessage = "QIDs not found via query " + sqlText;
            }
            else
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, "Retrieving " + qidCount + " QIDs in clsApeAMGetQRollupResults");
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "QID list: " + qidList);
            }

            return qidList;
        }

    }

}
