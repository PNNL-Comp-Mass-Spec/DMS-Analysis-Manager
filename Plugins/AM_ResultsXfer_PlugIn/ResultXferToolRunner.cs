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
using AnalysisManagerBase;
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
        // Ignore Spelling: Xfer

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

        private bool ChangeDirectoryPathsToLocal(string serverName, ref string transferDirectoryPath, ref string datasetStoragePath, out bool useNetworkShare)
        {
            var connectionString = mMgrParams.GetParam("ConnectionString");

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrName);

            var datasetStorageVolServer = LookupLocalPath(
                serverName,
                datasetStoragePath,
                "raw-storage",
                connectionStringToUse,
                out var rawStorageQuery,
                out var useNetworkShareForRawStorage);

            if (useNetworkShareForRawStorage)
            {
                useNetworkShare = true;
                return true;
            }

            if (string.IsNullOrWhiteSpace(datasetStorageVolServer))
            {
                mMessage = "Unable to determine the local drive letter for " + Path.Combine(@"\\" + serverName, datasetStoragePath);
                LogError("{0} using query {1}", mMessage, rawStorageQuery);
                mMessage += "; see manager log file for query";

                useNetworkShare = false;
                return false;
            }

            var transferVolServer = LookupLocalPath(
                serverName,
                transferDirectoryPath,
                "results_transfer",
                connectionStringToUse,
                out var resultsTransferQuery,
                out var useNetworkShareForResultsTransfer);

            if (useNetworkShareForResultsTransfer)
            {
                useNetworkShare = true;
                return true;
            }

            useNetworkShare = false;

            if (string.IsNullOrWhiteSpace(transferVolServer))
            {
                mMessage = "Unable to determine the local drive letter for " + Path.Combine(@"\\" + serverName, transferDirectoryPath);
                LogError("{0} using query {1}", mMessage, resultsTransferQuery);
                mMessage += "; see manager log file for query";

                return false;
            }

            datasetStoragePath = datasetStorageVolServer;
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

            return uncSharePath.Substring(2, charIndex - 2);
        }

        private string LookupLocalPath(
            string serverName,
            string uncSharePath,
            string directoryFunction,
            string connectionString,
            out string sqlQuery,
            out bool useNetworkShare)
        {
            if (!uncSharePath.StartsWith(@"\\"))
            {
                // Not a network path; cannot convert
                sqlQuery = "N/A: path does not start with two backslashes: " + uncSharePath;
                useNetworkShare = false;
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
                sqlQuery = "N/A: path does not have at least three backslashes: " + uncSharePath;
                useNetworkShare = false;
                return string.Empty;
            }

            // Remove the leading back slashes and the trailing backslash (if present)
            var trimmedPath = uncSharePath.Substring(charIndex + 1).TrimEnd('\\');

            var sql = new StringBuilder();

            // Query V_Storage_Path_Export for the local drive letter (aka volume name) of the given path

            sql.Append("SELECT vol_server, storage_path ");
            sql.Append("FROM V_Storage_Path_Export ");
            sql.AppendFormat("WHERE (machine_name = '{0}') AND", serverName);
            sql.AppendFormat("      (storage_path = '{0}' OR", trimmedPath);
            sql.AppendFormat("       storage_path = '{0}\\') ", trimmedPath);
            sql.AppendFormat("ORDER BY CASE WHEN storage_path_function = '{0}' THEN 1 ELSE 2 END, id DESC", directoryFunction);

            var dbTools = DbToolsFactory.GetDBTools(connectionString, debugMode: mMgrParams.TraceMode);
            RegisterEvents(dbTools);

            sqlQuery = sql.ToString();

            // Get a table to hold the results of the query
            var success = dbTools.GetQueryResultsDataTable(sqlQuery, out var dt);

            if (!success)
            {
                LogError("LookupLocalPath; Excessive failures attempting to retrieve directory info from database");
                useNetworkShare = false;
                return string.Empty;
            }

            foreach (DataRow curRow in dt.Rows)
            {
                // Only return the first result
                var volServer = curRow["vol_server"].CastDBVal<string>();
                useNetworkShare = false;
                return Path.Combine(volServer, trimmedPath);
            }

            if (trimmedPath.StartsWith("MassIVE_Staging") ||
                trimmedPath.StartsWith("MaxQuant_Staging") ||
                trimmedPath.StartsWith("MSFragger_Staging") ||
                trimmedPath.StartsWith("DiaNN_Staging"))
            {
                // Use the UNC path
                LogMessage(
                    "Could not resolve a local drive letter for path '{0}' on server {1}; will use the UNC path since this is a file staging share: {2}",
                    trimmedPath, serverName, uncSharePath);

                useNetworkShare = true;
                return uncSharePath;
            }

            // No data was returned
            LogError("LookupLocalPath; could not resolve a local drive letter for path '{0}' on server {1}", trimmedPath, serverName);

            // ReSharper disable CommentTypo
            // ReSharper disable StringLiteralTypo

            if (!trimmedPath.StartsWith(@"DataPkgs\", StringComparison.OrdinalIgnoreCase))
            {
                useNetworkShare = false;
                return string.Empty;
            }
            // This job's dataset is a data package based dataset
            // Re-query V_Storage_Path_Export using only "DataPkgs\"

            sql.Clear();
            sql.Append("SELECT vol_server, storage_path ");
            sql.Append("FROM V_Storage_Path_Export ");
            sql.AppendFormat("WHERE machine_name = '{0}' AND ", serverName);
            sql.Append("storage_path IN ('DataPkgs', 'DataPkgs\\')");

            // ReSharper restore StringLiteralTypo
            // ReSharper restore CommentTypo

            var dataPackageStorageQuery = sql.ToString();

            LogMessage("Looking for data package storage path using query {0}", dataPackageStorageQuery);

            // Get a table to hold the results of the query
            var dataPackageSuccess = dbTools.GetQueryResultsDataTable(dataPackageStorageQuery, out var dataPackageResult);

            if (!dataPackageSuccess)
            {
                LogError("LookupLocalPath; Excessive failures attempting to retrieve directory info from database");
                useNetworkShare = false;
                return string.Empty;
            }

            foreach (DataRow curRow in dataPackageResult.Rows)
            {
                // Only return the first result
                var volServer = curRow["vol_server"].CastDBVal<string>();

                var localDataPkgStoragePath = Path.Combine(volServer, trimmedPath);

                LogMessage("Local data package storage path found: {0}", localDataPkgStoragePath);
                useNetworkShare = false;
                return localDataPkgStoragePath;
            }

            // No data was returned
            LogError("Data package storage path not found in V_Storage_Path_Export on server " + serverName);

            useNetworkShare = false;
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

                        if (sourceFile.FullName.Length >= PRISM.NativeIOFileTools.FILE_PATH_LENGTH_THRESHOLD ||
                            targetFile.FullName.Length >= PRISM.NativeIOFileTools.FILE_PATH_LENGTH_THRESHOLD)
                        {
                            // Source or target file's full path length is 260 characters or longer
                            // If we're running Windows, CopyFileEx will use CopyFileW in kernel32.dll to copy the file

                            try
                            {
                                mFileTools.CopyFile(sourceFile.FullName, targetFile.FullName);
                            }
                            catch (Exception ex)
                            {
                                throw new Exception("Error copying file " + sourceFile.FullName + " to " + targetDirectoryPath + "; CopyFileEx reported exception " + ex.Message, ex);
                            }

                            // Delete the source file
                            try
                            {
                                sourceFile.Delete();
                            }
                            catch (Exception ex)
                            {
                                throw new Exception("Error deleting file " + sourceFile.FullName + " after copying it to the job directory in the dataset directory: " + ex.Message, ex);
                            }
                        }
                        else
                        {
                            sourceFile.MoveTo(targetFile.FullName);
                        }
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
            // Note that ChangeDirectoryPathsToLocal will update the transfer directory path to use local drive letters
            // if the transfer directory and the dataset directory are on the server running this instance of the analysis manager

            // This data is tracked in table T_Storage_Path and is determined using queries of the form:

            // SELECT vol_server, storage_path FROM V_Storage_Path_Export
            // WHERE (machine_name = 'Proto-5') AND (storage_path = 'LTQ_Orb_1\2008_1' OR storage_path = 'LTQ_Orb_1\2008_1\')
            // ORDER BY CASE WHEN storage_path_function = 'raw-storage' THEN 1 ELSE 2 END, id DESC

            // SELECT vol_server, storage_path FROM V_Storage_Path_Export
            // WHERE (machine_name = 'Proto-5') AND (storage_path = 'DMS3_Xfer' OR storage_path = 'DMS3_Xfer\')
            // ORDER BY CASE WHEN storage_path_function = 'results_transfer' THEN 1 ELSE 2 END, id DESC

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
                    LogWarning(
                        "Transfer directory not found ({0}) but the cache folder root path does exist; updating the transfer directory to: {1}",
                        transferDirectoryPath, cacheFolderRootPath);

                    transferDirectoryPath = cacheFolderRootPath;
                }
            }

            // Check whether the transfer directory and the dataset directory reside on the same server as this manager
            var serverName = Environment.MachineName;
            bool movingLocalFiles;

            if (string.Equals(GetMachineNameFromPath(transferDirectoryPath), serverName, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(GetMachineNameFromPath(datasetStoragePath), serverName, StringComparison.OrdinalIgnoreCase))
            {
                // Update the paths to use local file paths instead of network share paths

                // If LookupLocalPath() is unable to determine a local path and we are transferring data to a data package,
                // or to a staging share (like \\protoapps\MaxQuant_Staging), useNetworkShare will have been set to true

                if (!ChangeDirectoryPathsToLocal(serverName, ref transferDirectoryPath, ref datasetStoragePath, out var useNetworkShare))
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                        mMessage = "Unknown error calling ChangeDirectoryPathsToLocal";

                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                movingLocalFiles = !useNetworkShare;
            }
            else
            {
                movingLocalFiles = false;
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
                LogError(string.Format("Results transfer failed, directory {0} not found (see V_Storage_Path_Export and SELECT * FROM T_Storage_Path WHERE SP_function = 'results_transfer')", directoryToMove));
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
