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

        #endregion

        #region Constructors

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParms"></param>
        /// <param name="mgrParms"></param>
        public clsApeAMGetViperResults(IJobParams jobParms, IMgrParams mgrParms) : base(jobParms, mgrParms)
        {
        }

        #endregion

        /// <summary>
        /// Setup and run Ape pipeline according to job parameters
        /// </summary>
        public bool GetQRollupResults(string dataPackageID)
        {
            var blnSuccess = GetViperResultsAll();
            return blnSuccess;
        }

        private bool GetViperResultsAll()
        {
            var blnSuccess = true;
            var mHandle = new Ape.SqlConversionHandler(delegate(bool done, bool success, int percent, string msg)
            {
                Console.WriteLine(msg);

                if (done)
                {
                    if (success)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Ape successfully created QRollup database." + GetJobParam("ApeWorkflowName"));
                        blnSuccess = true;
                    }
                    else
                    {
                        mErrorMessage = "Error using APE to create QRollup database for workflow " + GetJobParam("ApeWorkflowName");
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage);
                        blnSuccess = false;
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

            var dotnetConnString = "Server=" + apeMTSServerName + ";database=" + apeMTSDatabaseName + ";uid=mtuser;Password=mt4fun";

            Ape.SqlServerToSQLite.ProgressChanged += OnProgressChanged;
            var MDIDList = GetMDIDList();
            if (string.IsNullOrEmpty(MDIDList))
            {
                return false;
            }

            Ape.SqlServerToSQLite.ConvertDatasetToSQLiteFile(paramList, (int)eSqlServerToSqlLiteConversionMode.ViperResults, dotnetConnString, MDIDList, apeDatabase, mHandle);

            return blnSuccess;
        }

        private string GetMDIDList()
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

            var sqlText = "SELECT DISTINCT vmts.MD_ID FROM V_Mage_Data_Package_Analysis_Jobs vdp " +
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

            var MDIDList = string.Empty;
            var intMDIDCount = 0;
            using (var conn = new SqlConnection(constr))
            {
                conn.Open();
                // Get the matching MD_IDs for this data package
                var query = new SqlCommand(sqlText, conn);
                using (var reader = query.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        if (!string.IsNullOrEmpty(reader[0].ToString()))
                        {
                            MDIDList += reader[0] + ", ";
                            intMDIDCount += 1;
                        }
                    }
                }
            }

            if (string.IsNullOrEmpty(MDIDList))
            {
                mErrorMessage = "MDIDs not found via query " + sqlText;
            }
            else
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving " + intMDIDCount + " MDIDs in clsApeAMGetViperResults");
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MDID list: " + MDIDList);
            }

            return MDIDList;
        }

    }

}
