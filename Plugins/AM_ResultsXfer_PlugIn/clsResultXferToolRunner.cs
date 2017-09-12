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
        public override CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the AnalysisManager version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
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

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                m_message = "Error in ResultsXferPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything worked, so exit
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool ChangeFolderPathsToLocal(string serverName, ref string transferFolderPath, ref string datasetStoragePath)
        {
            var connectionString = m_mgrParams.GetParam("connectionstring");

            var datasetStorageVolServer = LookupLocalPath(serverName, datasetStoragePath, "raw-storage", connectionString);
            if (string.IsNullOrWhiteSpace(datasetStorageVolServer))
            {
                m_message = "Unable to determine the local drive letter for " + Path.Combine(@"\\" + serverName, datasetStoragePath);
                return false;
            }

            datasetStoragePath = datasetStorageVolServer;

            var transferVolServer = LookupLocalPath(serverName, transferFolderPath, "results_transfer", connectionString);
            if (string.IsNullOrWhiteSpace(transferVolServer))
            {
                m_message = "Unable to determine the local drive letter for " + Path.Combine(@"\\" + serverName, transferFolderPath);
                return false;
            }

            transferFolderPath = transferVolServer;

            return true;
        }

        private void DeleteTransferFolderIfEmpty()
        {
            // If there are no more folders or files in the dataset folder in the xfer directory, delete the folder
            // Note that another manager might be simultaneously examining this folder to see if it's empty
            // If that manager deletes this folder first, an exception could occur in this manager
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
                            LogDebug("Deleting empty dataset folder in transfer directory: " + diTransferFolder.FullName);
                        }

                        diTransferFolder.Delete();
                    }
                    catch (Exception ex)
                    {
                        // Log this exception, but don't treat it is a job failure
                        var msg = "clsResultXferToolRunner.RunTool(); Exception deleting dataset folder " + m_jobParams.GetParam("DatasetFolderName") +
                                  " in xfer folder(another results manager may have deleted it): " + ex.Message;
                        LogWarning(msg);
                    }
                }
                else
                {
                    if (m_DebugLevel >= 3)
                    {
                        LogDebug("Dataset folder in transfer directory still has files/folders; will not delete: " + diTransferFolder.FullName);
                    }
                }
            }
            catch (Exception ex)
            {
                // Log this exception, but don't treat it is a job failure
                var msg = "clsResultXferToolRunner.RunTool(); Exception looking for dataset folder " + m_jobParams.GetParam("DatasetFolderName") +
                          " in xfer folder (another results manager may have deleted it): " + ex.Message;
                LogWarning(msg);
            }
        }

        private string GetMachineNameFromPath(string uncFolderPath)
        {
            var charIndex = uncFolderPath.IndexOf('\\', 2);

            if (charIndex < 0 || !uncFolderPath.StartsWith(@"\\"))
            {
                return string.Empty;
            }

            var machineName = uncFolderPath.Substring(2, charIndex - 2);
            return machineName;
        }

        private string LookupLocalPath(string serverName, string uncFolderPath, string folderFunction, string connectionString)
        {
            const short retryCount = 3;
            string strMsg;

            if (!uncFolderPath.StartsWith(@"\\"))
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
            var blnSuccess = clsGlobal.GetDataTableByQuery(sbSql.ToString(), connectionString, "LookupLocalPath", retryCount, out var dt);

            if (!blnSuccess)
            {
                strMsg = "LookupLocalPath; Excessive failures attempting to retrieve folder info from database";
                LogError(strMsg);
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
            LogError(strMsg);
            return string.Empty;
        }

        /// <summary>
        /// Moves files from one local directory to another local directory
        /// </summary>
        /// <param name="sourceFolderpath"></param>
        /// <param name="targetFolderPath"></param>
        /// <param name="overwriteExisting"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool MoveFilesLocally(string sourceFolderpath, string targetFolderPath, bool overwriteExisting)
        {
            var success = true;
            var errorCount = 0;

            try
            {
                if (sourceFolderpath.StartsWith(@"\\"))
                {
                    m_message = "MoveFilesLocally cannot be used with files on network shares; " + sourceFolderpath;
                    return false;
                }

                if (targetFolderPath.StartsWith(@"\\"))
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
                    LogDebug("Moving files locally to " + diTargetFolder.FullName);
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
                                    LogDebug("Skipping existing file: " + fiTargetFile.FullName);
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
                            LogError("Error moving file " + fiSourceFile.Name + ": " + ex.Message, ex);
                        }
                        else
                        {
                            LogErrorNoMessageUpdate("Error moving file " + fiSourceFile.Name + ": " + ex.Message);
                        }
                        success = false;
                    }
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
                        LogWarning("Unable to delete folder " + diSourceFolder.FullName + ": " + ex);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error moving directory " + sourceFolderpath + ": " + ex.Message);
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
            var transferFolderPath = m_jobParams.GetParam("transferFolderPath");
            var datasetStoragePath = m_jobParams.GetParam("DatasetStoragePath");

            // Check whether the transfer folder and the dataset folder reside on the same server as this manager
            var serverName = Environment.MachineName;
            var movingLocalFiles = false;

            if (string.Equals(GetMachineNameFromPath(transferFolderPath), serverName, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(GetMachineNameFromPath(datasetStoragePath), serverName, StringComparison.OrdinalIgnoreCase))
            {
                // Update the paths to use local file paths instead of network share paths

                if (!ChangeFolderPathsToLocal(serverName, ref transferFolderPath, ref datasetStoragePath))
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                        m_message = "Unknown error calling ChangeFolderPathsToLocal";
                    LogError(m_message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                movingLocalFiles = true;
            }

            // Verify input folder exists in storage server xfer folder
            var folderToMove = Path.Combine(transferFolderPath, m_jobParams.GetParam("DatasetFolderName"), m_jobParams.GetParam("InputFolderName"));

            if (!Directory.Exists(folderToMove))
            {
                LogError("Results transfer failed, directory " + folderToMove + " not found");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (m_DebugLevel >= 4)
            {
                LogDebug("Results folder to move: " + folderToMove);
            }

            // Verify dataset folder exists on storage server
            // If it doesn't exist, we will auto-create it (this behavior was added 4/24/2009)
            var datasetDir = Path.Combine(datasetStoragePath, m_jobParams.GetParam("DatasetFolderName"));
            var diDatasetFolder = new DirectoryInfo(datasetDir);
            if (!diDatasetFolder.Exists)
            {
                LogWarning("Dataset folder " + datasetDir + " not found for results transfer; will attempt to make it");

                try
                {
                    var diParentFolder = diDatasetFolder.Parent;

                    if (diParentFolder == null)
                    {
                        LogError("Unable to determine the parent folder of " + diDatasetFolder.FullName);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

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
                            var msg = "Error trying to create missing dataset folder";
                            LogError(msg, msg + datasetDir + ": folder creation failed for unknown reason");
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                    else
                    {
                        LogError("Parent directory not found: " + diParentFolder.FullName);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                catch (Exception ex)
                {
                    var msg = "Error trying to create missing dataset folder";
                    LogError(msg, msg + ": " + datasetDir, ex);

                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            else if (m_DebugLevel >= 4)
            {
                LogDebug("Dataset folder path: " + datasetDir);
            }

            var targetDir = Path.Combine(datasetDir, m_jobParams.GetParam("inputfoldername"));

            // Determine if output folder already exists on storage server
            if (Directory.Exists(targetDir))
            {
                LogWarning("Warning: overwriting existing results folder: " + targetDir);
            }

            // Move the directory
            try
            {
                if (m_DebugLevel >= 3)
                {
                    LogDebug("Moving '" + folderToMove + "' to '" + targetDir + "'");
                }

                if (movingLocalFiles)
                {
                    var success = MoveFilesLocally(folderToMove, targetDir, overwriteExisting: true);
                    if (!success)
                        return CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    // Call MoveDirectory, which will copy the files using locks
                    if (m_DebugLevel >= 2)
                    {
                        LogDebug("Using m_FileTools.MoveDirectory to copy files to " + targetDir);
                    }
                    ResetTimestampForQueueWaitTimeLogging();
                    m_FileTools.MoveDirectory(folderToMove, targetDir, overwriteFiles: true);
                }
            }
            catch (Exception ex)
            {
                var msg = "Exception moving results folder";
                LogError(msg, msg + ": " + folderToMove, ex);
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
            var toolVersionInfo = string.Empty;
            var appFolderPath = clsGlobal.GetAppFolderPath();

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of the Analysis Manager
            if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "AnalysisManagerProg"))
            {
                return false;
            }

            // Lookup the version of AnalysisManagerResultsXferPlugin
            if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "AnalysisManagerResultsXferPlugin"))
            {
                return false;
            }

            // Store the path to AnalysisManagerProg.exe and AnalysisManagerResultsXferPlugin.dll in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                new FileInfo(Path.Combine(appFolderPath, "AnalysisManagerProg.exe")),
                new FileInfo(Path.Combine(appFolderPath, "AnalysisManagerResultsXferPlugin.dll"))
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, ioToolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        #endregion
    }
}
