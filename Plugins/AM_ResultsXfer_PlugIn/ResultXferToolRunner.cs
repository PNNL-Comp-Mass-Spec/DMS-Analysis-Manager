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
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISMDatabaseUtils;

namespace AnalysisManagerResultsXferPlugin
{
    /// <summary>
    /// Derived class for performing analysis results transfer
    /// </summary>
    public class ResultXferToolRunner : AnalysisToolRunnerBase
    {
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

                DeleteTransferDirectoryIfEmpty();

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

        private bool ChangeDirectoryPathsToLocal(string serverName, ref string transferDirectoryPath, ref string datasetStoragePath)
        {
            var connectionString = mMgrParams.GetParam("ConnectionString");

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrName);

            var datasetStorageVolServer = LookupLocalPath(serverName, datasetStoragePath, "raw-storage", connectionStringToUse);

            if (string.IsNullOrWhiteSpace(datasetStorageVolServer))
            {
                mMessage = "Unable to determine the local drive letter for " + Path.Combine(@"\\" + serverName, datasetStoragePath);
                return false;
            }

            datasetStoragePath = datasetStorageVolServer;

            var transferVolServer = LookupLocalPath(serverName, transferDirectoryPath, "results_transfer", connectionStringToUse);

            if (string.IsNullOrWhiteSpace(transferVolServer))
            {
                mMessage = "Unable to determine the local drive letter for " + Path.Combine(@"\\" + serverName, transferDirectoryPath);
                return false;
            }

            transferDirectoryPath = transferVolServer;

            return true;
        }

        /// <summary>
        /// If there are no more subdirectories or files in the dataset directory in the transfer directory, delete the directory
        /// </summary>
        /// <remarks>
        /// Another manager might be simultaneously examining this directory to see if it's empty
        /// If that manager deletes this directory first, an exception could occur in this manager
        /// Thus, log any exceptions that occur, but don't treat as a job failure
        /// </remarks>
        private void DeleteTransferDirectoryIfEmpty()
        {
            var transferDirectoryPath = mJobParams.GetParam(AnalysisResources.JOB_PARAM_TRANSFER_DIRECTORY_PATH);

            if (string.IsNullOrWhiteSpace(transferDirectoryPath))
            {
                LogError("Job parameter transferDirectoryPath is empty or not defined");
                return;
            }

            try
            {
                var transferDirectory = new DirectoryInfo(Path.Combine(transferDirectoryPath, mDatasetName));

                if (transferDirectory.Exists && transferDirectory.GetFileSystemInfos("*", SearchOption.AllDirectories).Length == 0)
                {
                    // Dataset directory in the transfer directory is empty; delete it
                    try
                    {
                        if (mDebugLevel >= 3)
                        {
                            LogDebug("Deleting empty dataset directory in transfer directory: " + transferDirectory.FullName);
                        }

                        transferDirectory.Delete();
                    }
                    catch (Exception ex)
                    {
                        // Log this exception, but don't treat it is a job failure
                        LogWarning(
                            "ResultXferToolRunner.RunTool(); Exception deleting dataset directory {0} in transfer directory " +
                            "(another results manager may have deleted it): {1}",
                            mJobParams.GetParam(AnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME), ex.Message);

                        UpdateEvalCode(0, "Exception deleting dataset directory in transfer directory: " + ex.Message + "; " + transferDirectory.FullName);
                    }
                }
                else
                {
                    if (mDebugLevel >= 3)
                    {
                        LogDebug("Dataset directory in transfer directory still has files/directories; will not delete: {0}", transferDirectory.FullName);
                    }
                }
            }
            catch (Exception ex)
            {
                // Log this exception, but don't treat it is a job failure
                LogWarning(
                    "ResultXferToolRunner.RunTool(); Exception looking for dataset directory {0} in transfer directory " +
                    "(another results manager may have deleted it): {1}",
                    mJobParams.GetParam(AnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME), ex.Message);

                UpdateEvalCode(0, "Exception looking for dataset directory in transfer directory " + ex.Message + "; " + transferDirectoryPath);
            }
        }

        private string GetMachineNameFromPath(string uncSharePath)
        {
            var charIndex = uncSharePath.IndexOf('\\', 2);

            if (charIndex < 0 || !uncSharePath.StartsWith(@"\\"))
            {
                return string.Empty;
            }

            var machineName = uncSharePath.Substring(2, charIndex - 2);
            return machineName;
        }

        private string LookupLocalPath(string serverName, string uncSharePath, string directoryFunction, string connectionString)
        {
            string msg;

            if (!uncSharePath.StartsWith(@"\\"))
            {
                // Not a network path; cannot convert
                return string.Empty;
            }

            // Remove the server name from the start of uncSharePath
            // For example, change
            //   from: \\proto-6\LTQ_Orb_3\2013_2
            //   to:   LTQ_Orb_3\2013_2

            // First starting from index 2 in the string, find the next slash
            var charIndex = uncSharePath.IndexOf('\\', 2);

            if (charIndex < 0)
            {
                // Match not found
                return string.Empty;
            }

            uncSharePath = uncSharePath.Substring(charIndex + 1);

            // Make sure uncSharePath does not end in a slash
            if (uncSharePath.EndsWith("\\"))
            {
                uncSharePath = uncSharePath.TrimEnd('\\');
            }

            var sql = new StringBuilder();

            // Query V_Storage_Path_Export for the local volume name of the given path
            //
            sql.Append(" SELECT vol_server, storage_path");
            sql.Append(" FROM V_Storage_Path_Export");
            sql.AppendFormat(" WHERE (machine_name = '{0}') AND", serverName);
            sql.AppendFormat("       (storage_path = '{0}' OR", uncSharePath);
            sql.AppendFormat("        storage_path = '{0}\\')", uncSharePath);
            sql.AppendFormat(" ORDER BY CASE WHEN storage_path_function = '{0}' THEN 1 ELSE 2 END, id DESC", directoryFunction);

            var dbTools = DbToolsFactory.GetDBTools(connectionString, debugMode: mMgrParams.TraceMode);
            RegisterEvents(dbTools);

            // Get a table to hold the results of the query
            var success = dbTools.GetQueryResultsDataTable(sql.ToString(), out var dt);

            if (!success)
            {
                msg = "LookupLocalPath; Excessive failures attempting to retrieve directory info from database";
                LogError(msg);
                return string.Empty;
            }

            foreach (DataRow curRow in dt.Rows)
            {
                // Only return the first result
                var volServer = curRow["vol_server"].CastDBVal<string>();
                return Path.Combine(volServer, uncSharePath);
            }

            // No data was returned
            msg = "LookupLocalPath; could not resolve a local volume name for path '" + uncSharePath + "' on server " + serverName;
            LogError(msg);
            return string.Empty;
        }

        /// <summary>
        /// Moves files from one local directory to another local directory
        /// </summary>
        /// <param name="sourceDirectoryPath"></param>
        /// <param name="targetDirectoryPath"></param>
        /// <param name="overwriteExisting"></param>
        private bool MoveFilesLocally(string sourceDirectoryPath, string targetDirectoryPath, bool overwriteExisting)
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

                foreach (var sourceFile in sourceDirectory.GetFiles())
                {
                    try
                    {
                        var targetFile = new FileInfo(Path.Combine(targetDirectory.FullName, sourceFile.Name));

                        if (targetFile.Exists)
                        {
                            if (!overwriteExisting)
                            {
                                if (mDebugLevel >= 2)
                                {
                                    LogDebug("Skipping existing file: " + targetFile.FullName);
                                }
                                continue;
                            }
                            targetFile.Delete();
                        }

                        sourceFile.MoveTo(targetFile.FullName);
                    }
                    catch (Exception ex)
                    {
                        errorCount++;

                        if (errorCount == 1)
                        {
                            LogError("Error moving file " + sourceFile.Name + ": " + ex.Message, ex);
                        }
                        else
                        {
                            LogErrorNoMessageUpdate("Error moving file " + sourceFile.Name + ": " + ex.Message);
                        }
                        success = false;
                    }
                }

                // Recursively call this method for each subdirectory
                foreach (var subdirectory in sourceDirectory.GetDirectories())
                {
                    var subDirSuccess = MoveFilesLocally(subdirectory.FullName, Path.Combine(targetDirectory.FullName, subdirectory.Name),
                        overwriteExisting);

                    if (!subDirSuccess)
                    {
                        success = false;
                    }
                }

                // Delete this directory if it is empty
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
                        LogWarning("Unable to delete directory " + sourceDirectory.FullName + ": " + ex);
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
        protected virtual CloseOutType PerformResultsXfer()
        {
            var transferDirectoryPath = mJobParams.GetParam(AnalysisResources.JOB_PARAM_TRANSFER_DIRECTORY_PATH);
            string datasetStoragePath;

            bool appendDatasetDirectoryNameForSource;
            bool appendDatasetDirectoryNameForTarget;

            if (Global.IsMatch(mDatasetName, AnalysisResources.AGGREGATION_JOB_DATASET))
            {
                appendDatasetDirectoryNameForSource = false;
                appendDatasetDirectoryNameForTarget = false;

                datasetStoragePath = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_DATA_PACKAGE_PATH);

                if (string.IsNullOrEmpty(datasetStoragePath))
                {
                    LogError("DataPackagePath job parameter is empty");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            else
            {
                appendDatasetDirectoryNameForSource = !AnalysisResources.IsDataPackageDataset(mDatasetName);
                appendDatasetDirectoryNameForTarget = true;

                datasetStoragePath = mJobParams.GetParam("DatasetStoragePath");

                if (string.IsNullOrEmpty(datasetStoragePath))
                {
                    LogError("DatasetStoragePath job parameter is empty");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            if (!Directory.Exists(transferDirectoryPath) && AnalysisResources.IsDataPackageDataset(mDatasetName))
            {
                var cacheFolderRootPath = mJobParams.GetParam(AnalysisResources.JOB_PARAM_CACHE_FOLDER_ROOT_PATH);

                if (!string.IsNullOrWhiteSpace(cacheFolderRootPath) && Directory.Exists(cacheFolderRootPath))
                {
                    LogWarning(string.Format(
                        "Transfer directory not found ({0}) but the cache folder root path does exist; updating the transfer directory to: {1}",
                        transferDirectoryPath, cacheFolderRootPath));

                    transferDirectoryPath = cacheFolderRootPath;
                }
            }

            // Check whether the transfer directory and the dataset directory reside on the same server as this manager
            var serverName = Environment.MachineName;
            var movingLocalFiles = false;

            if (string.Equals(GetMachineNameFromPath(transferDirectoryPath), serverName, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(GetMachineNameFromPath(datasetStoragePath), serverName, StringComparison.OrdinalIgnoreCase))
            {
                // Update the paths to use local file paths instead of network share paths

                if (!ChangeDirectoryPathsToLocal(serverName, ref transferDirectoryPath, ref datasetStoragePath))
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                        mMessage = "Unknown error calling ChangeDirectoryPathsToLocal";
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

            string directoryToMove;

            if (appendDatasetDirectoryNameForSource)
            {
                var datasetDirectoryName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME);

                if (string.IsNullOrWhiteSpace(datasetDirectoryName))
                {
                    LogError("Results transfer failed, job parameter DatasetFolderName is empty; cannot continue");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Verify input directory exists in storage server transfer directory
                directoryToMove = Path.Combine(transferDirectoryPath, datasetDirectoryName, inputDirectory);
            }
            else
            {
                directoryToMove = Path.Combine(transferDirectoryPath, inputDirectory);
            }

            if (!Directory.Exists(directoryToMove))
            {
                LogError(string.Format("Results transfer failed, directory {0} not found", directoryToMove));
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (mDebugLevel >= 4)
            {
                LogDebug("Results directory to move: " + directoryToMove);
            }

            // Verify that the dataset directory exists on storage server
            // If it doesn't exist, we will auto-create it

            string datasetDirectoryPath;

            if (appendDatasetDirectoryNameForTarget)
            {
                datasetDirectoryPath = Path.Combine(datasetStoragePath, mJobParams.GetParam(AnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME));
            }
            else
            {
                datasetDirectoryPath = datasetStoragePath;
            }

            var datasetDirectory = new DirectoryInfo(datasetDirectoryPath);

            if (!datasetDirectory.Exists)
            {
                LogWarning("Dataset directory " + datasetDirectoryPath + " not found for results transfer; will attempt to make it");

                try
                {
                    var parentDirectory = datasetDirectory.Parent;

                    if (parentDirectory == null)
                    {
                        LogError("Unable to determine the parent folder of " + datasetDirectory.FullName);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (!parentDirectory.Exists)
                    {
                        // Parent directory doesn't exist; try to go up one more level and create the parent

                        if (parentDirectory.Parent != null)
                        {
                            // Parent of the parent exists; try to create the parent directory
                            parentDirectory.Create();

                            // Verify that the directory was created
                            parentDirectory.Refresh();
                            datasetDirectory.Refresh();
                        }
                    }

                    if (parentDirectory.Exists)
                    {
                        // Parent directory exists; try to create the dataset directory
                        datasetDirectory.Create();

                        // Verify that the directory now exists
                        datasetDirectory.Refresh();

                        if (!datasetDirectory.Exists)
                        {
                            // Creation of the dataset directory failed; unable to continue
                            const string msg = "Error trying to create missing dataset directory";
                            LogError(msg, msg + datasetDirectoryPath + ": directory creation failed for unknown reason");
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                    else
                    {
                        LogError("Parent directory not found: " + parentDirectory.FullName);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                catch (Exception ex)
                {
                    const string msg = "Error trying to create missing dataset directory";
                    LogError(msg, msg + ": " + datasetDirectoryPath, ex);

                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            else if (mDebugLevel >= 4)
            {
                LogDebug("Dataset directory path: " + datasetDirectoryPath);
            }

            var targetDir = Path.Combine(datasetDirectoryPath, inputDirectory);

            // Determine if output directory already exists on storage server
            if (Directory.Exists(targetDir))
            {
                LogWarning("Warning: overwriting existing results directory: " + targetDir);
            }

            // Move the directory
            try
            {
                if (mDebugLevel >= 3)
                {
                    LogDebug("Moving '" + directoryToMove + "' to '" + targetDir + "'");
                }

                if (movingLocalFiles)
                {
                    var success = MoveFilesLocally(directoryToMove, targetDir, overwriteExisting: true);

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
                    mFileTools.MoveDirectory(directoryToMove, targetDir, overwriteFiles: true);
                }
            }
            catch (Exception ex)
            {
                const string msg = "Exception moving results directory";
                LogError(msg, msg + ": " + directoryToMove, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            var toolVersionInfo = string.Empty;
            var appFolderPath = Global.GetAppDirectoryPath();

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
                new(Path.Combine(appFolderPath, "AnalysisManagerProg.exe")),
                new(Path.Combine(appFolderPath, "AnalysisManagerResultsXferPlugin.dll"))
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
    }
}
