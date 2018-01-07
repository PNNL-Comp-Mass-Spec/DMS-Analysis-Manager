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

        #endregion

        #region Constructors

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParms"></param>
        /// <param name="mgrParms"></param>
        public clsApeAMGetQRollupResults(IJobParams jobParms, IMgrParams mgrParms) : base(jobParms, mgrParms)
        {
        }

        #endregion

        /// <summary>
        /// Setup and run Ape pipeline according to job parameters
        /// </summary>
        public bool GetQRollupResults(string dataPackageID)
        {
            var blnSuccess = GetQRollupResultsAll();
            return blnSuccess;
        }

        private bool GetQRollupResultsAll()
        {
            var blnSuccess = true;
            var mHandle = new Ape.SqlConversionHandler(delegate(bool done, bool success, int percent, string msg)
            {
                Console.WriteLine(msg);

                if (done)
                {
                    if (success)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, PRISM.Logging.BaseLogger.LogLevels.INFO, "Ape successfully created QRollup database." + GetJobParam("ApeWorkflowName"));
                        blnSuccess = true;
                    }
                    else
                    {
                        mErrorMessage = "Error running Ape in GetQRollupResultsAll";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, PRISM.Logging.BaseLogger.LogLevels.ERROR, mErrorMessage);
                        blnSuccess = false;
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

            return blnSuccess;
        }

        private string GetQIDList()
        {
            var constr = RequireMgrParam("connectionstring");
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

            var QIDList = string.Empty;
            var intQIDCount = 0;
            using (var conn = new SqlConnection(constr))
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
                            QIDList += reader[0] + ", ";
                            intQIDCount += 1;
                        }
                    }
                }
            }

            if (string.IsNullOrEmpty(QIDList))
            {
                mErrorMessage = "QIDs not found via query " + sqlText;
            }
            else
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, PRISM.Logging.BaseLogger.LogLevels.INFO, "Retrieving " + intQIDCount + " QIDs in clsApeAMGetQRollupResults");
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, PRISM.Logging.BaseLogger.LogLevels.DEBUG, "QID list: " + QIDList);
            }

            return QIDList;
        }

    }

}
