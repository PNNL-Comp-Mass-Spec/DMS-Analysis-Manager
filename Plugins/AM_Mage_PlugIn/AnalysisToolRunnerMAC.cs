﻿using AnalysisManagerBase;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;

namespace AnalysisManager_Mage_PlugIn
{
    /// <summary>
    /// This class provides a generic base for MAC tool run operations common to all MAC tool plug-ins.
    /// </summary>
    public abstract class AnalysisToolRunnerMAC : AnalysisToolRunnerBase
    {
        #region "Constants"

        protected const string MAGE_LOG_FILE_NAME = "Mage_Log.txt";

        protected const float ProgressPctMacDone = 95;

        #endregion

        /// <summary>
        /// Primary entry point for running MAC tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Do the base class stuff
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the MAC tool version info in the database
                StoreToolVersionInfo();

                var cachedWarningMessage = mJobParams.GetJobParameter("AnalysisResourcesClass", "Evaluation_Message", string.Empty);
                if (!string.IsNullOrWhiteSpace(cachedWarningMessage))
                {
                    mEvalMessage = Global.AppendToComment(mEvalMessage, cachedWarningMessage);
                }

                LogMessage("Running MAC Plugin");

                bool processingSuccess;
                try
                {
                    // Run the appropriate MAC pipeline(s) according to mode parameter
                    processingSuccess = RunMACTool();

                    if (string.Equals(MAGE_LOG_FILE_NAME, Path.GetFileName(FileLogger.LogFilePath)))
                    {
                        ResetLogFileNameToDefault();
                    }

                    if (!processingSuccess)
                    {
                        if (string.IsNullOrWhiteSpace(mMessage))
                        {
                            LogError("Error running MAC: RunMACTool returned false");
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Change the name of the log file back to the analysis manager log file
                    ResetLogFileNameToDefault();

                    LogError("Exception running MAC: " + ex.Message, ex);

                    processingSuccess = false;

                    var dataPackageSourceDirectoryName = mJobParams.GetJobParameter("DataPackageSourceFolderName", "ImportFiles");
                    if (ex.Message.Contains(dataPackageSourceDirectoryName + "\\--No Files Found"))
                    {
                        LogError(dataPackageSourceDirectoryName + " directory in the data package is empty or does not exist");
                    }
                }

                // Stop the job timer
                mStopTime = DateTime.UtcNow;
                mProgress = ProgressPctMacDone;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.ProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result directory,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Override the output directory name and the dataset name (since this is a dataset aggregation job)
                mResultsDirectoryName = mJobParams.GetParam("StepOutputFolderName");
                mDatasetName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME);
                if (!string.IsNullOrEmpty(mResultsDirectoryName))
                    mJobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME, mResultsDirectoryName);

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                var msg = "Error in MAC Plugin->RunTool: " + ex.Message;
                LogError(msg, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Run the specific MAC tool (override in a subclass)
        /// </summary>
        protected abstract bool RunMACTool();

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        protected bool StoreToolVersionInfo()
        {
            string toolVersionInfo;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info for primary tool assembly");
            }

            try
            {
                toolVersionInfo = GetToolNameAndVersion();
            }
            catch (Exception ex)
            {
                LogError("Exception determining Assembly info for primary tool assembly: " + ex.Message);
                return false;
            }

            // Store paths to key DLLs
            var toolFiles = GetToolSupplementalVersionInfo();

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
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

                using var conn = new SQLiteConnection(connectionString);
                conn.Open();

                var query = "select * From " + tableName;
                using var cmd = new SQLiteCommand(query, conn);

                var reader = cmd.ExecuteReader();

                if (!reader.HasRows)
                {
                    errorMessage = "is empty";
                    return false;
                }

                reader.Read();
                foreach (var columnName in lstColumns)
                {
                    try
                    {
                        var result = reader[columnName];
                    }
                    catch (Exception)
                    {
                        errorMessage = "is missing column " + columnName;
                        return false;
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

                using var conn = new SQLiteConnection(connectionString);
                conn.Open();

                var query = "select count(*) as Items From sqlite_master where type = 'table' and name = '" + tableName + "' COLLATE NOCASE";
                using var cmd = new SQLiteCommand(query, conn);

                var result = cmd.ExecuteScalar();
                if (Convert.ToInt32(result) > 0)
                    tableFound = true;
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
        protected abstract string GetToolNameAndVersion();

        /// <summary>
        /// Subclass must override this to provide version info for any supplemental MAC tool assemblies
        /// </summary>
        protected abstract List<FileInfo> GetToolSupplementalVersionInfo();
    }
}