//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2008, Battelle Memorial Institute
// Created 10/30/2008
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerResultsXferPlugin
{
    /// <summary>
    /// Derived class for performing analysis results transfer
    /// </summary>
    /// <remarks></remarks>
    public class clsResultXferToolRunner : clsAnalysisToolRunnerBase
    {
        #region "Methods"

        /// <summary>
        /// Runs the results transfer tool
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            try
            {
                //Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the AnalysisManager version info in the database
                if (!StoreToolVersionInfo())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining AnalysisManager version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Transfer the results
                var Result = PerformResultsXfer();
                if (Result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Unknown error calling PerformResultsXfer";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                DeleteTransferFolderIfEmpty();

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                m_message = "Error in ResultsXferPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //If we got to here, everything worked, so exit
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected bool ChangeFolderPathsToLocal(string serverName, ref string transferFolderPath, ref string datasetStoragePath)
        {
            var connectionString = m_mgrParams.GetParam("connectionstring");

            var datasetStorageVolServer = LookupLocalPath(serverName, datasetStoragePath, "raw-storage", connectionString);
            if (string.IsNullOrWhiteSpace(datasetStorageVolServer))
            {
                m_message = "Unable to determine the local drive letter for " + Path.Combine("\\\\" + serverName, datasetStoragePath);
                return false;
            }
            else
            {
                datasetStoragePath = datasetStorageVolServer;
            }

            var transferVolServer = LookupLocalPath(serverName, transferFolderPath, "results_transfer", connectionString);
            if (string.IsNullOrWhiteSpace(transferVolServer))
            {
                m_message = "Unable to determine the local drive letter for " + Path.Combine("\\\\" + serverName, transferFolderPath);
                return false;
            }
            else
            {
                transferFolderPath = transferVolServer;
            }

            return true;
        }

        protected void DeleteTransferFolderIfEmpty()
        {
            // If there are no more folders or files in the dataset folder in the xfer directory, then delete the folder
            // Note that another manager might be simultaneously examining this folder to see if it's empty
            // If that manager deletes this folder first, then an exception could occur in this manager
            // Thus, we will log any exceptions that occur, but we won't treat them as a job failure

            try
            {
                var diTransferFolder = new DirectoryInfo(Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_Dataset));

                if (diTransferFolder.Exists && diTransferFolder.GetFileSystemInfos("*", SearchOption.AllDirectories).Length == 0)
                {
                    // Dataset folder in transfer folder is empty; delete it
                    try
                    {
                        if (m_DebugLevel >= 3)
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                "Deleting empty dataset folder in transfer directory: " + diTransferFolder.FullName);
                        }

                        diTransferFolder.Delete();
                    }
                    catch (Exception ex)
                    {
                        // Log this exception, but don't treat it is a job failure
                        var msg = "clsResultXferToolRunner.RunTool(); Exception deleting dataset folder " + m_jobParams.GetParam("DatasetFolderName") +
                                  " in xfer folder(another results manager may have deleted it): " + ex.Message;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                    }
                }
                else
                {
                    if (m_DebugLevel >= 3)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "Dataset folder in transfer directory still has files/folders; will not delete: " + diTransferFolder.FullName);
                    }
                }
            }
            catch (Exception ex)
            {
                // Log this exception, but don't treat it is a job failure
                var msg = "clsResultXferToolRunner.RunTool(); Exception looking for dataset folder " + m_jobParams.GetParam("DatasetFolderName") +
                          " in xfer folder (another results manager may have deleted it): " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
            }
        }

        private string GetMachineNameFromPath(string uncFolderPath)
        {
            var charIndex = uncFolderPath.IndexOf('\\', 2);

            if (charIndex < 0 || !uncFolderPath.StartsWith("\\\\"))
            {
                return string.Empty;
            }

            var machineName = uncFolderPath.Substring(2, charIndex - 2);
            return machineName;
        }

        protected string LookupLocalPath(string serverName, string uncFolderPath, string folderFunction, string connectionString)
        {
            const short retryCount = 3;
            string strMsg = null;

            if (!uncFolderPath.StartsWith("\\\\"))
            {
                // Not a network path; cannot convert
                return string.Empty;
            }

            // Remove the server name from the start of folderPath
            // For example, change
            //   from: \\proto-6\LTQ_Orb_3\2013_2
            //   to:   LTQ_Orb_3\2013_2

            // First starting from index 2 in the string, find the next slash
            var charIndex = uncFolderPath.IndexOf('\\', 2);

            if (charIndex < 0)
            {
                // Match not found
                return string.Empty;
            }

            uncFolderPath = uncFolderPath.Substring(charIndex + 1);

            // Make sure folderPath does not end in a slash
            if (uncFolderPath.EndsWith("\\"))
            {
                uncFolderPath = uncFolderPath.TrimEnd('\\');
            }

            var sbSql = new StringBuilder();

            // Query V_Storage_Path_Export for the local volume name of the given path
            //
            sbSql.Append(" SELECT TOP 1 VolServer, [Path]");
            sbSql.Append(" FROM V_Storage_Path_Export");
            sbSql.Append(" WHERE (MachineName = '" + serverName + "') AND");
            sbSql.Append("       ([Path] = '" + uncFolderPath + "' OR");
            sbSql.Append("        [Path] = '" + uncFolderPath + "\\')");
            sbSql.Append(" ORDER BY CASE WHEN [Function] = '" + folderFunction + "' THEN 1 ELSE 2 END, ID DESC");

            // Get a table to hold the results of the query
            DataTable dt = null;
            var blnSuccess = clsGlobal.GetDataTableByQuery(sbSql.ToString(), connectionString, "LookupLocalPath", retryCount, out dt);

            if (!blnSuccess)
            {
                strMsg = "LookupLocalPath; Excessive failures attempting to retrieve folder info from database";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMsg);
                return string.Empty;
            }

            foreach (DataRow curRow in dt.Rows)
            {
                var volServer = clsGlobal.DbCStr(curRow["VolServer"]);
                var localFolderPath = Path.Combine(volServer, uncFolderPath);
                return localFolderPath;
            }

            // No data was returned
            strMsg = "LookupLocalPath; could not resolve a local volume name for path '" + uncFolderPath + "' on server " + serverName;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, strMsg);
            return string.Empty;
        }

        /// <summary>
        /// Moves files from one local directory to another local directory
        /// </summary>
        /// <param name="sourceFolderpath"></param>
        /// <param name="targetFolderPath"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool MoveFilesLocally(string sourceFolderpath, string targetFolderPath, bool overwriteExisting)
        {
            var success = true;
            var errorCount = 0;
            var errorMessage = string.Empty;

            try
            {
                if (sourceFolderpath.StartsWith("\\\\"))
                {
                    m_message = "MoveFilesLocally cannot be used with files on network shares; " + sourceFolderpath;
                    return false;
                }

                if (targetFolderPath.StartsWith("\\\\"))
                {
                    m_message = "MoveFilesLocally cannot be used with files on network shares; " + targetFolderPath;
                    return false;
                }

                var diSourceFolder = new DirectoryInfo(sourceFolderpath);
                var diTargetFolder = new DirectoryInfo(targetFolderPath);

                if (!diTargetFolder.Exists)
                    diTargetFolder.Create();

                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "Moving files locally to " + diTargetFolder.FullName);
                }

                foreach (var fiSourceFile in diSourceFolder.GetFiles())
                {
                    try
                    {
                        var fiTargetFile = new FileInfo(Path.Combine(diTargetFolder.FullName, fiSourceFile.Name));

                        if (fiTargetFile.Exists)
                        {
                            if (!overwriteExisting)
                            {
                                if (m_DebugLevel >= 2)
                                {
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                        "Skipping existing file: " + fiTargetFile.FullName);
                                }
                                continue;
                            }
                            fiTargetFile.Delete();
                        }

                        fiSourceFile.MoveTo(fiTargetFile.FullName);
                    }
                    catch (Exception ex)
                    {
                        errorCount += 1;
                        if (errorCount == 1)
                        {
                            errorMessage = "Error moving file " + fiSourceFile.Name + ": " + ex.Message;
                        }
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                            "Error moving file " + fiSourceFile.Name + ": " + ex.Message);
                        success = false;
                    }
                }

                if (errorCount > 0)
                {
                    m_message = clsGlobal.AppendToComment(m_message, errorMessage);
                }

                // Recursively call this function for each subdirectory
                foreach (var diSubFolder in diSourceFolder.GetDirectories())
                {
                    var subDirSuccess = MoveFilesLocally(diSubFolder.FullName, Path.Combine(diTargetFolder.FullName, diSubFolder.Name),
                        overwriteExisting);
                    if (!subDirSuccess)
                    {
                        success = false;
                    }
                }

                // Delete this folder if it is empty
                diSourceFolder.Refresh();
                if (diSourceFolder.GetFileSystemInfos("*", SearchOption.AllDirectories).Length == 0)
                {
                    try
                    {
                        diSourceFolder.Delete();
                    }
                    catch (Exception ex)
                    {
                        // Log a warning, but ignore this error
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            "Unable to delete folder " + diSourceFolder.FullName, ex);
                    }
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Error moving directory " + sourceFolderpath + ": " + ex.Message);
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Performs the results transfer
        /// </summary>
        /// <returns>CloseOutType indicating success or failure></returns>
        /// <remarks></remarks>
        protected virtual CloseOutType PerformResultsXfer()
        {
            string msg = null;
            string FolderToMove = null;
            string DatasetDir = null;
            string TargetDir = null;

            // Set this to True to overwrite existing results folders
            const bool blnOverwriteExisting = true;

            var transferFolderPath = m_jobParams.GetParam("transferFolderPath");
            var datasetStoragePath = m_jobParams.GetParam("DatasetStoragePath");

            // Check whether the transfer folder and the dataset folder reside on the same server as this manager
            var serverName = Environment.MachineName;
            var movingLocalFiles = false;

            if (string.Compare(GetMachineNameFromPath(transferFolderPath), serverName, true) == 0 &&
                string.Compare(GetMachineNameFromPath(datasetStoragePath), serverName, true) == 0)
            {
                // Update the paths to use local file paths instead of network share paths

                if (!ChangeFolderPathsToLocal(serverName, ref transferFolderPath, ref datasetStoragePath))
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                        m_message = "Unknown error calling ChangeFolderPathsToLocal";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                movingLocalFiles = true;
            }

            // Verify input folder exists in storage server xfer folder
            FolderToMove = Path.Combine(transferFolderPath, m_jobParams.GetParam("DatasetFolderName"));
            FolderToMove = Path.Combine(FolderToMove, m_jobParams.GetParam("InputFolderName"));

            if (!Directory.Exists(FolderToMove))
            {
                msg = "clsResultXferToolRunner.PerformResultsXfer(); results folder " + FolderToMove + " not found";
                m_message = clsGlobal.AppendToComment(m_message, "results folder not found");
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            else if (m_DebugLevel >= 4)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Results folder to move: " + FolderToMove);
            }

            // Verify dataset folder exists on storage server
            // If it doesn't exist, we will auto-create it (this behavior was added 4/24/2009)
            DatasetDir = Path.Combine(datasetStoragePath, m_jobParams.GetParam("DatasetFolderName"));
            var diDatasetFolder = new DirectoryInfo(DatasetDir);
            if (!diDatasetFolder.Exists)
            {
                msg = "clsResultXferToolRunner.PerformResultsXfer(); dataset folder " + DatasetDir + " not found; will attempt to make it";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);

                try
                {
                    var diParentFolder = diDatasetFolder.Parent;

                    if (!diParentFolder.Exists)
                    {
                        // Parent folder doesn't exist; try to go up one more level and create the parent

                        if ((diParentFolder.Parent != null))
                        {
                            // Parent of the parent exist; try to create the parent folder
                            diParentFolder.Create();

                            // Wait 500 msec then verify that the folder was created
                            Thread.Sleep(500);
                            diParentFolder.Refresh();
                            diDatasetFolder.Refresh();
                        }
                    }

                    if (diParentFolder.Exists)
                    {
                        // Parent folder exists; try to create the dataset folder
                        diDatasetFolder.Create();

                        // Wait 500 msec then verify that the folder now exists
                        Thread.Sleep(500);
                        diDatasetFolder.Refresh();

                        if (!diDatasetFolder.Exists)
                        {
                            // Creation of the dataset folder failed; unable to continue
                            msg = "clsResultXferToolRunner.PerformResultsXfer(); error trying to create missing dataset folder " + DatasetDir +
                                  ": folder creation failed for unknown reason";
                            m_message = clsGlobal.AppendToComment(m_message, "error trying to create missing dataset folder");
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                    else
                    {
                        msg = "clsResultXferToolRunner.PerformResultsXfer(); parent folder not found: " + diDatasetFolder.Parent.FullName +
                              "; unable to continue";
                        m_message = clsGlobal.AppendToComment(m_message, "parent folder not found: " + diDatasetFolder.Parent.FullName);
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                catch (Exception ex)
                {
                    msg = "clsResultXferToolRunner.PerformResultsXfer(); error trying to create missing dataset folder " + DatasetDir + ": " +
                          ex.Message;
                    m_message = clsGlobal.AppendToComment(m_message, "exception trying to create missing dataset folder");
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            else if (m_DebugLevel >= 4)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Dataset folder path: " + DatasetDir);
            }

            TargetDir = Path.Combine(DatasetDir, m_jobParams.GetParam("inputfoldername"));

            // Determine if output folder already exists on storage server
            if (Directory.Exists(TargetDir))
            {
                if (blnOverwriteExisting)
                {
                    msg = "Warning: overwriting existing results folder: " + TargetDir;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                }
                else
                {
                    msg = "clsResultXferToolRunner.PerformResultsXfer(); destination directory " + DatasetDir + " already exists";
                    m_message = clsGlobal.AppendToComment(m_message, "results folder already exists at destination and overwrite is disabled");
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Move the directory
            try
            {
                if (m_DebugLevel >= 3)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "Moving '" + FolderToMove + "' to '" + TargetDir + "'");
                }

                if (movingLocalFiles)
                {
                    var success = MoveFilesLocally(FolderToMove, TargetDir, blnOverwriteExisting);
                    if (!success)
                        return CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    // Call MoveDirectory, which will copy the files using locks
                    if (m_DebugLevel >= 2)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "Using m_FileTools.MoveDirectory to copy files to " + TargetDir);
                    }
                    ResetTimestampForQueueWaitTimeLogging();
                    m_FileTools.MoveDirectory(FolderToMove, TargetDir, blnOverwriteExisting, m_mgrParams.GetParam("MgrName", "Undefined-Manager"));
                }
            }
            catch (Exception ex)
            {
                msg = "clsResultXferToolRunner.PerformResultsXfer(); Exception moving results folder " + FolderToMove + ": " + ex.Message;
                m_message = clsGlobal.AppendToComment(m_message, "exception moving results folder");
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            string strToolVersionInfo = string.Empty;
            string strAppFolderPath = clsGlobal.GetAppFolderPath();

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            // Lookup the version of the Analysis Manager
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManagerProg"))
            {
                return false;
            }

            // Lookup the version of AnalysisManagerResultsXferPlugin
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManagerResultsXferPlugin"))
            {
                return false;
            }

            // Store the path to AnalysisManagerProg.exe and AnalysisManagerResultsXferPlugin.dll in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(Path.Combine(strAppFolderPath, "AnalysisManagerProg.exe")));
            ioToolFiles.Add(new FileInfo(Path.Combine(strAppFolderPath, "AnalysisManagerResultsXferPlugin.dll")));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, false);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        #endregion
    }
}
