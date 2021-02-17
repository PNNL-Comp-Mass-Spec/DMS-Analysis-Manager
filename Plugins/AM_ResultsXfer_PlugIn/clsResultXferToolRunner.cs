//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2008, Battelle Memorial Institute
// Created 10/30/2008
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using PRISMDatabaseUtils;

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
                    mMessage = "Error determining AnalysisManager version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Transfer the results
                var result = PerformResultsXfer();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Unknown error calling PerformResultsXfer";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                DeleteTransferFolderIfEmpty();

                // Stop the job timer
                mStopTime = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                mMessage = "Error in ResultsXferPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything worked, so exit
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool ChangeFolderPathsToLocal(string serverName, ref string transferFolderPath, ref string datasetStoragePath)
        {
            var connectionString = mMgrParams.GetParam("ConnectionString");

            var datasetStorageVolServer = LookupLocalPath(serverName, datasetStoragePath, "raw-storage", connectionString);
            if (string.IsNullOrWhiteSpace(datasetStorageVolServer))
            {
                mMessage = "Unable to determine the local drive letter for " + Path.Combine(@"\\" + serverName, datasetStoragePath);
                return false;
            }

            datasetStoragePath = datasetStorageVolServer;

            var transferVolServer = LookupLocalPath(serverName, transferFolderPath, "results_transfer", connectionString);
            if (string.IsNullOrWhiteSpace(transferVolServer))
            {
                mMessage = "Unable to determine the local drive letter for " + Path.Combine(@"\\" + serverName, transferFolderPath);
                return false;
            }

            transferFolderPath = transferVolServer;

            return true;
        }

        /// <summary>
        /// If there are no more folders or files in the dataset directory in the xfer directory, delete the directory
        /// </summary>
        /// <remarks>
        /// Another manager might be simultaneously examining this folder to see if it's empty
        /// If that manager deletes this folder first, an exception could occur in this manager
        /// Thus, log any exceptions that occur, but don't treat as a job failure
        /// </remarks>
        private void DeleteTransferFolderIfEmpty()
        {
            var transferFolderPath = mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH);
            if (string.IsNullOrWhiteSpace(transferFolderPath))
            {
                LogError("Job parameter transferFolderPath is empty or not defined");
                return;
            }

            try
            {
                var diTransferFolder = new DirectoryInfo(Path.Combine(transferFolderPath, mDatasetName));

                if (diTransferFolder.Exists && diTransferFolder.GetFileSystemInfos("*", SearchOption.AllDirectories).Length == 0)
                {
                    // Dataset directory in the transfer folder is empty; delete it
                    try
                    {
                        if (mDebugLevel >= 3)
                        {
                            LogDebug("Deleting empty dataset directory in transfer directory: " + diTransferFolder.FullName);
                        }

                        diTransferFolder.Delete();
                    }
                    catch (Exception ex)
                    {
                        // Log this exception, but don't treat it is a job failure
                        var msg = "clsResultXferToolRunner.RunTool(); Exception deleting dataset directory " + mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME) +
                                  " in xfer folder (another results manager may have deleted it): " + ex.Message;
                        LogWarning(msg);

                        UpdateEvalCode(0, "Exception deleting dataset directory in xfer folder: " + ex.Message + "; " + diTransferFolder.FullName);
                    }
                }
                else
                {
                    if (mDebugLevel >= 3)
                    {
                        LogDebug("Dataset directory in transfer directory still has files/folders; will not delete: " + diTransferFolder.FullName);
                    }
                }
            }
            catch (Exception ex)
            {
                // Log this exception, but don't treat it is a job failure
                var msg = "clsResultXferToolRunner.RunTool(); Exception looking for dataset directory " + mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME) +
                          " in xfer folder (another results manager may have deleted it): " + ex.Message;
                LogWarning(msg);

                UpdateEvalCode(0, "Exception looking for dataset directory in xfer folder " + ex.Message + "; " + transferFolderPath);
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
            sbSql.AppendFormat(" WHERE (MachineName = '{0}') AND", serverName);
            sbSql.AppendFormat("       ([Path] = '{0}' OR", uncFolderPath);
            sbSql.AppendFormat("        [Path] = '{0}\\')", uncFolderPath);
            sbSql.AppendFormat(" ORDER BY CASE WHEN [Function] = '{0}' THEN 1 ELSE 2 END, ID DESC", folderFunction);

            var dbTools = DbToolsFactory.GetDBTools(connectionString, debugMode: mMgrParams.TraceMode);
            RegisterEvents(dbTools);

            // Get a table to hold the results of the query
            var success = dbTools.GetQueryResultsDataTable(sbSql.ToString(), out var dt);

            if (!success)
            {
                strMsg = "LookupLocalPath; Excessive failures attempting to retrieve folder info from database";
                LogError(strMsg);
                return string.Empty;
            }

            foreach (DataRow curRow in dt.Rows)
            {
                var volServer = curRow["VolServer"].CastDBVal<string>();
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
        /// <param name="sourceDirectoryPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="overwriteExisting"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool MoveFilesLocally(string sourceDirectoryPath, string targetDirectoryPath, bool overwriteExisting)
        {
            var success = true;
            var errorCount = 0;

            try
            {
                if (sourceDirectoryPath.StartsWith(@"\\"))
                {
                    mMessage = "MoveFilesLocally cannot be used with files on network shares; " + sourceDirectoryPath;
                    return false;
                }

                if (targetDirectoryPath.StartsWith(@"\\"))
                {
                    mMessage = "MoveFilesLocally cannot be used with files on network shares; " + targetDirectoryPath;
                    return false;
                }

                var sourceDirectory = new DirectoryInfo(sourceDirectoryPath);
                var targetDirectory = new DirectoryInfo(targetDirectoryPath);

                if (!targetDirectory.Exists)
                    targetDirectory.Create();

                if (mDebugLevel >= 2)
                {
                    LogDebug("Moving files locally to " + targetDirectory.FullName);
                }

                foreach (var fiSourceFile in sourceDirectory.GetFiles())
                {
                    try
                    {
                        var fiTargetFile = new FileInfo(Path.Combine(targetDirectory.FullName, fiSourceFile.Name));

                        if (fiTargetFile.Exists)
                        {
                            if (!overwriteExisting)
                            {
                                if (mDebugLevel >= 2)
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
                        errorCount++;
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
                foreach (var subdirectory in sourceDirectory.GetDirectories())
                {
                    var subDirSuccess = MoveFilesLocally(subdirectory.FullName, Path.Combine(targetDirectory.FullName, subdirectory.Name),
                        overwriteExisting);
                    if (!subDirSuccess)
                    {
                        success = false;
                    }
                }

                // Delete this folder if it is empty
                sourceDirectory.Refresh();
                if (sourceDirectory.GetFileSystemInfos("*", SearchOption.AllDirectories).Length == 0)
                {
                    try
                    {
                        sourceDirectory.Delete();
                    }
                    catch (Exception ex)
                    {
                        // Log a warning, but ignore this error
                        LogWarning("Unable to delete folder " + sourceDirectory.FullName + ": " + ex);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error moving directory " + sourceDirectoryPath + ": " + ex.Message);
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
            var transferFolderPath = mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH);
            var datasetStoragePath = mJobParams.GetParam("DatasetStoragePath");

            // Check whether the transfer folder and the dataset directory reside on the same server as this manager
            var serverName = Environment.MachineName;
            var movingLocalFiles = false;

            if (string.Equals(GetMachineNameFromPath(transferFolderPath), serverName, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(GetMachineNameFromPath(datasetStoragePath), serverName, StringComparison.OrdinalIgnoreCase))
            {
                // Update the paths to use local file paths instead of network share paths

                if (!ChangeFolderPathsToLocal(serverName, ref transferFolderPath, ref datasetStoragePath))
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                        mMessage = "Unknown error calling ChangeFolderPathsToLocal";
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                movingLocalFiles = true;
            }

            var inputDirectory = mJobParams.GetParam("InputFolderName");
            if (string.IsNullOrWhiteSpace(inputDirectory))
            {
                LogError("Results transfer failed, job parameter InputFolderName is empty; reset the transfer step to state 1 in T_Job_Steps");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var datasetDirectoryName = mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME);
            if (string.IsNullOrWhiteSpace(datasetDirectoryName))
            {
                LogError("Results transfer failed, job parameter DatasetFolderName is empty; cannot continue");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Verify input folder exists in storage server xfer folder
            var folderToMove = Path.Combine(transferFolderPath, datasetDirectoryName, inputDirectory);

            if (!Directory.Exists(folderToMove))
            {
                LogError("Results transfer failed, directory " + folderToMove + " not found");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (mDebugLevel >= 4)
            {
                LogDebug("Results folder to move: " + folderToMove);
            }

            // Verify that the dataset directory exists on storage server
            // If it doesn't exist, we will auto-create it (this behavior was added 4/24/2009)
            var datasetDir = Path.Combine(datasetStoragePath, mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME));
            var diDatasetFolder = new DirectoryInfo(datasetDir);
            if (!diDatasetFolder.Exists)
            {
                LogWarning("Dataset directory " + datasetDir + " not found for results transfer; will attempt to make it");

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

                        if (diParentFolder.Parent != null)
                        {
                            // Parent of the parent exists; try to create the parent folder
                            diParentFolder.Create();

                            // Verify that the directory was created
                            diParentFolder.Refresh();
                            diDatasetFolder.Refresh();
                        }
                    }

                    if (diParentFolder.Exists)
                    {
                        // Parent folder exists; try to create the dataset directory
                        diDatasetFolder.Create();

                        // Verify that the directory now exists
                        diDatasetFolder.Refresh();

                        if (!diDatasetFolder.Exists)
                        {
                            // Creation of the dataset directory failed; unable to continue
                            const string msg = "Error trying to create missing dataset directory";
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
                    const string msg = "Error trying to create missing dataset directory";
                    LogError(msg, msg + ": " + datasetDir, ex);

                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            else if (mDebugLevel >= 4)
            {
                LogDebug("Dataset directory path: " + datasetDir);
            }

            var targetDir = Path.Combine(datasetDir, inputDirectory);

            // Determine if output folder already exists on storage server
            if (Directory.Exists(targetDir))
            {
                LogWarning("Warning: overwriting existing results folder: " + targetDir);
            }

            // Move the directory
            try
            {
                if (mDebugLevel >= 3)
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
                    if (mDebugLevel >= 2)
                    {
                        LogDebug("Using mFileTools.MoveDirectory to copy files to " + targetDir);
                    }
                    ResetTimestampForQueueWaitTimeLogging();
                    mFileTools.MoveDirectory(folderToMove, targetDir, overwriteFiles: true);
                }
            }
            catch (Exception ex)
            {
                const string msg = "Exception moving results folder";
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
            var appFolderPath = clsGlobal.GetAppDirectoryPath();

            if (mDebugLevel >= 2)
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

            // Store the path to AnalysisManagerProg.exe and AnalysisManagerResultsXferPlugin.dll in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new FileInfo(Path.Combine(appFolderPath, "AnalysisManagerProg.exe")),
                new FileInfo(Path.Combine(appFolderPath, "AnalysisManagerResultsXferPlugin.dll"))
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
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
