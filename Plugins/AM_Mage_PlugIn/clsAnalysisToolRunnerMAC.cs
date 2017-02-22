using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManager_Mage_PlugIn
{

    /// <summary>
    /// This class provides a generic base for MAC tool run operations common to all MAC tool plug-ins.
    /// </summary>
    public abstract class clsAnalysisToolRunnerMAC : clsAnalysisToolRunnerBase
    {

        #region "Module Variables"

        protected const float ProgressPctMacDone = 95;

        #endregion

        /// <summary>
        /// Run the MAC tool and disposition the results
        /// </summary>
        /// <returns></returns>
        public override CloseOutType RunTool()
        {
            try
            {
                //Do the base class stuff
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the MAC tool version info in the database
                StoreToolVersionInfo();

                LogMessage("Running MAC Plugin");


                bool blnSuccess;
                try
                {

                    // run the appropriate MAC pipeline(s) according to mode parameter
                    blnSuccess = RunMACTool();

                    if (!blnSuccess && !string.IsNullOrWhiteSpace(m_message))
                    {
                        LogError("Error running MAC: " + m_message);
                    }
                }
                catch (Exception ex)
                {
                    // Change the name of the log file back to the analysis manager log file
                    var logFileName = m_mgrParams.GetParam("logfilename");
                    log4net.GlobalContext.Properties["LogName"] = logFileName;
                    clsLogTools.ChangeLogFileName(logFileName);

                    LogError("Error running MAC: " + ex.Message, ex);
                    blnSuccess = false;

                    m_message = "Error running MAC: " + ex.Message;
                    var sDataPackageSourceFolderName = m_jobParams.GetJobParameter("DataPackageSourceFolderName", "ImportFiles");
                    if (ex.Message.Contains(sDataPackageSourceFolderName + "\\--No Files Found"))
                    {
                        m_message += "; " + sDataPackageSourceFolderName + " folder in the data package is empty or does not exist";
                    }

                }

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;
                m_progress = ProgressPctMacDone;

                //Add the current job data to the summary file
                UpdateSummaryFile();

                //Make sure objects are released
                //2 second delay
                System.Threading.Thread.Sleep(2000);
                PRISM.clsProgRunner.GarbageCollectNow();

                if (!blnSuccess)
                {
                    // Move the source files and any results to the Failed Job folder
                    // Useful for debugging problems
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_ResFolderName = m_jobParams.GetParam("StepOutputFolderName");
                m_Dataset = m_jobParams.GetParam("OutputFolderName");
                if (!string.IsNullOrEmpty(m_ResFolderName))
                    m_jobParams.SetParam("StepParameters", "OutputFolderName", m_ResFolderName);

                var result = MakeResultsFolder();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return result;
                }

                result = MoveResultFiles();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    m_message = "Error moving files into results folder";
                    return result;
                }

                result = CopyResultsFolderToServer();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return result;
                }

            }
            catch (Exception ex)
            {
                m_message = "Error in MAC Plugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Run the specific MAC tool (override in a subclass)
        /// </summary>
        protected abstract bool RunMACTool();

        /// <summary>
        /// 
        /// </summary>
        protected void CopyFailedResultsToArchiveFolder()
        {
            var strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrEmpty(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            LogWarning("Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory
            var strFolderPathToArchive = string.Copy(m_WorkDir);

            // Make the results folder
            var result = MakeResultsFolder();
            if (result == CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Move the result files into the result folder
                result = MoveResultFiles();
                if (result == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Move was a success; update strFolderPathToArchive
                    strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName);
                }
            }

            // Copy the results folder to the Archive folder
            var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
            objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive);
        }


        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {

            string strToolVersionInfo;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info for primary tool assembly");
            }

            try
            {
                strToolVersionInfo = GetToolNameAndVersion();
            }
            catch (Exception ex)
            {
                LogError("Exception determining Assembly info for primary tool assembly: " + ex.Message);
                return false;
            }

            // Store paths to key DLLs
            var ioToolFiles = GetToolSupplementalVersionInfo();

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }

        }

        /// <summary>
        /// Confirms that the table has 1 or more rows and has the specified columns
        /// </summary>
        /// <param name="fiSqlLiteDatabase"></param>
        /// <param name="tableName"></param>
        /// <param name="lstColumns"></param>
        /// <param name="errorMessage">Error message</param>
        /// <param name="exceptionDetail">Exception details (empty if errorMessage is empty)</param>
        /// <returns></returns>
        protected bool TableContainsDataAndColumns(
          FileInfo fiSqlLiteDatabase,
          string tableName,
          IEnumerable<string> lstColumns,
          out string errorMessage,
          out string exceptionDetail)
        {
            errorMessage = string.Empty;
            exceptionDetail = string.Empty;

            try
            {
                var connectionString = "Data Source = " + fiSqlLiteDatabase.FullName + "; Version=3;";
                using (var conn = new SQLiteConnection(connectionString))
                {
                    conn.Open();

                    var query = "select * From " + tableName;
                    using (var cmd = new SQLiteCommand(query, conn))
                    {
                        var drReader = cmd.ExecuteReader();

                        if (!drReader.HasRows)
                        {
                            errorMessage = "is empty";
                            return false;
                        }

                        drReader.Read();
                        foreach (var columnName in lstColumns)
                        {
                            try
                            {
                                var result = drReader[columnName];
                            }
                            catch (Exception)
                            {
                                errorMessage = "is missing column " + columnName;
                                return false;
                            }

                        }
                    }
                }

            }
            catch (Exception ex)
            {
                errorMessage = "threw an exception while querying";
                exceptionDetail = ex.Message;
                LogError("Exception confirming table's columns in SqLite file: " + ex.Message);
                return false;
            }

            return true;
        }

        protected bool TableExists(FileInfo fiSqlLiteDatabase, string tableName)
        {
            var tableFound = false;

            try
            {
                var connectionString = "Data Source = " + fiSqlLiteDatabase.FullName + "; Version=3;";
                using (var conn = new SQLiteConnection(connectionString))
                {
                    conn.Open();

                    var query = "select count(*) as Items From sqlite_master where type = 'table' and name = '" + tableName + "' COLLATE NOCASE";
                    using (var cmd = new SQLiteCommand(query, conn))
                    {
                        var result = cmd.ExecuteScalar();
                        if (Convert.ToInt32(result) > 0)
                            tableFound = true;

                    }
                }

            }
            catch (Exception ex)
            {
                LogError("Exception looking for table in SqLite file: " + ex.Message);
                return false;
            }

            return tableFound;
        }

        /// <summary>
        /// Subclass must override this to provide version info for primary MAC tool assembly
        /// </summary>
        /// <returns></returns>
        protected abstract string GetToolNameAndVersion();

        /// <summary>
        /// Subclass must override this to provide version info for any supplemental MAC tool assemblies
        /// </summary>
        /// <returns></returns>
        protected abstract List<FileInfo> GetToolSupplementalVersionInfo();


    }
}
