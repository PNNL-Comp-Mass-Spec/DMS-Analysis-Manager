using System;
using System.IO;
using AnalysisManagerBase.JobConfig;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/19/2007
//
//*********************************************************************************************************

namespace AnalysisManagerBase.AnalysisTool
{
    /// <summary>
    /// Analysis job results handling class
    /// </summary>
    public class AnalysisResults : AnalysisMgrBase
    {
        // Ignore Spelling: holdoff

        private const string FAILED_RESULTS_FOLDER_INFO_TEXT = "FailedResultsFolderInfo_";

        private const int FAILED_RESULTS_FOLDER_RETAIN_DAYS = 31;

        private const int DEFAULT_RETRY_COUNT = 3;

        private const int DEFAULT_RETRY_HOLDOFF_SEC = 15;

        /// <summary>
        /// Manager parameters
        /// </summary>
        /// <remarks>Instance of class AnalysisMgrSettings</remarks>
        private readonly IMgrParams mMgrParams;

        /// <summary>
        /// explanation of what happened to last operation this class performed
        /// </summary>
        public string Message => mMessage;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Manager parameter object</param>
        /// <param name="jobParams">Job parameter object</param>
        public AnalysisResults(IMgrParams mgrParams, IJobParams jobParams) : base("AnalysisResults")
        {
            mMgrParams = mgrParams;
            mJobParams = jobParams;
            var mgrName = mMgrParams.ManagerName;
            mDebugLevel = (short)mMgrParams.GetParam("DebugLevel", 1);

            InitFileTools(mgrName, mDebugLevel);
        }

        /// <summary>
        /// Copies a source directory to the destination directory. Allows overwriting.
        /// </summary>
        /// <param name="sourcePath">The source directory path.</param>
        /// <param name="destinationPath">The destination directory path.</param>
        /// <param name="overwrite">True if the destination file can be overwritten; otherwise, false.</param>
        // ReSharper disable once UnusedMember.Global
        public void CopyDirectory(string sourcePath, string destinationPath, bool overwrite)
        {
            CopyDirectory(sourcePath, destinationPath, overwrite, maxRetryCount: DEFAULT_RETRY_COUNT, continueOnError: true);
        }

        /// <summary>
        /// Copies a source directory to the destination directory. Allows overwriting.
        /// </summary>
        /// <param name="sourceDirPath">The source directory path.</param>
        /// <param name="targetDirPath">The destination directory path.</param>
        /// <param name="overwrite">True if the destination file can be overwritten; otherwise, false.</param>
        /// <param name="maxRetryCount">The number of times to retry a failed copy of a file; if 0 or 1 then only tries once</param>
        /// <param name="continueOnError">When true, continue copying even if an error occurs</param>
        public void CopyDirectory(string sourceDirPath, string targetDirPath, bool overwrite, int maxRetryCount, bool continueOnError)
        {
            var sourceDirectory = new DirectoryInfo(sourceDirPath);
            var targetDirectory = new DirectoryInfo(targetDirPath);

            string message;

            // The source directory must exist, otherwise throw an exception
            if (!DirectoryExistsWithRetry(sourceDirectory.FullName, 3, 3))
            {
                message = "Source directory does not exist: " + sourceDirectory.FullName;

                if (continueOnError)
                {
                    LogError(message);
                    return;
                }

                throw new DirectoryNotFoundException(message);
            }

            // If the parent subdirectory of the destination subdirectory does not exist, throw an exception
            if (targetDirectory.Parent == null)
            {
                message = "Unable to determine the parent directory of " + targetDirectory.FullName;

                if (continueOnError)
                {
                    LogError(message);
                    return;
                }

                throw new DirectoryNotFoundException(message);
            }

            if (!DirectoryExistsWithRetry(targetDirectory.Parent.FullName, 1, 1))
            {
                message = "Destination directory does not exist: " + targetDirectory.Parent.FullName;

                if (continueOnError)
                {
                    LogError(message);
                    return;
                }

                throw new DirectoryNotFoundException(message);
            }

            if (!DirectoryExistsWithRetry(targetDirectory.FullName, 3, 3))
            {
                CreateDirectoryWithRetry(targetDirectory.FullName, maxRetryCount, DEFAULT_RETRY_HOLDOFF_SEC);
            }

            // Copy all the files of the current directory
            foreach (var childFile in sourceDirectory.GetFiles())
            {
                try
                {
                    var targetPath = Path.Combine(targetDirectory.FullName, childFile.Name);

                    if (overwrite)
                    {
                        CopyFileWithRetry(childFile.FullName, targetPath, true, maxRetryCount, DEFAULT_RETRY_HOLDOFF_SEC);
                    }
                    else
                    {
                        // Only copy if the file does not yet exist
                        // We are not throwing an error if the file exists in the target
                        if (!File.Exists(targetPath))
                        {
                            CopyFileWithRetry(childFile.FullName, targetPath, false, maxRetryCount, DEFAULT_RETRY_HOLDOFF_SEC);
                        }
                    }
                }
                catch (Exception ex) when (continueOnError)
                {
                    LogError("AnalysisResults,CopyDirectory", ex);
                }
            }

            // Copy each subdirectory by recursively calling this same method
            foreach (var subDir in sourceDirectory.GetDirectories())
            {
                CopyDirectory(subDir.FullName, Path.Combine(targetDirectory.FullName, subDir.Name), overwrite, maxRetryCount, continueOnError);
            }
        }

        /// <summary>
        /// Copy the file, optionally overwriting
        /// </summary>
        /// <remarks>Tries up to 3 times, waiting 15 seconds between attempts</remarks>
        /// <param name="sourceFilePath">Source file path</param>
        /// <param name="destinationFilePath">Destination file path</param>
        /// <param name="overwrite">True to overwrite if it exists</param>
        // ReSharper disable once UnusedMember.Global
        public void CopyFileWithRetry(string sourceFilePath, string destinationFilePath, bool overwrite)
        {
            const bool increaseHoldoffOnEachRetry = false;
            CopyFileWithRetry(sourceFilePath, destinationFilePath, overwrite, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, increaseHoldoffOnEachRetry);
        }

        /// <summary>
        /// Copy the file, optionally overwriting
        /// </summary>
        /// <param name="sourceFilePath">Source file path</param>
        /// <param name="destinationFilePath">Destination file path</param>
        /// <param name="overwrite">True to overwrite if it exists</param>
        /// <param name="increaseHoldoffOnEachRetry">If true, increase the holdoff between each retry</param>
        // ReSharper disable once UnusedMember.Global
        public void CopyFileWithRetry(string sourceFilePath, string destinationFilePath, bool overwrite, bool increaseHoldoffOnEachRetry)
        {
            CopyFileWithRetry(sourceFilePath, destinationFilePath, overwrite, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, increaseHoldoffOnEachRetry);
        }

        /// <summary>
        /// Copy the file, optionally overwriting
        /// </summary>
        /// <param name="sourceFilePath">Source file path</param>
        /// <param name="destinationFilePath">Destination file path</param>
        /// <param name="overwrite">True to overwrite if it exists</param>
        /// <param name="maxRetryCount">Maximum attempts</param>
        /// <param name="retryHoldoffSeconds">Seconds between attempts</param>
        public void CopyFileWithRetry(string sourceFilePath, string destinationFilePath, bool overwrite, int maxRetryCount, int retryHoldoffSeconds)
        {
            const bool increaseHoldoffOnEachRetry = false;
            CopyFileWithRetry(sourceFilePath, destinationFilePath, overwrite, maxRetryCount, retryHoldoffSeconds, increaseHoldoffOnEachRetry);
        }

        /// <summary>
        /// Copy the file, optionally overwriting
        /// </summary>
        /// <param name="sourceFilePath">Source file path</param>
        /// <param name="destinationFilePath">Destination file path</param>
        /// <param name="overwrite">True to overwrite if it exists</param>
        /// <param name="maxRetryCount">Maximum attempts</param>
        /// <param name="retryHoldoffSeconds">Seconds between attempts</param>
        /// <param name="increaseHoldoffOnEachRetry">If true, increase the holdoff between each attempt</param>
        public void CopyFileWithRetry(string sourceFilePath, string destinationFilePath, bool overwrite, int maxRetryCount, int retryHoldoffSeconds, bool increaseHoldoffOnEachRetry)
        {
            var attemptCount = 0;
            float actualRetryHoldoffSeconds = retryHoldoffSeconds;

            if (actualRetryHoldoffSeconds < 1)
                actualRetryHoldoffSeconds = 1;

            if (maxRetryCount < 1)
                maxRetryCount = 1;

            // First make sure the source file exists
            if (!File.Exists(sourceFilePath))
            {
                throw new IOException("AnalysisResults,CopyFileWithRetry: Source file not found for copy operation: " + sourceFilePath);
            }

            while (attemptCount <= maxRetryCount)
            {
                attemptCount++;

                try
                {
                    ResetTimestampForQueueWaitTimeLogging();
                    var startTime = DateTime.UtcNow;

                    if (mFileTools.CopyFileUsingLocks(sourceFilePath, destinationFilePath, overwrite))
                    {
                        LogCopyStats(startTime, destinationFilePath);
                        return;
                    }

                    LogError("CopyFileUsingLocks returned false copying " + sourceFilePath + " to " + destinationFilePath);
                }
                catch (Exception ex)
                {
                    LogError("AnalysisResults,CopyFileWithRetry: error copying " + sourceFilePath + " to " + destinationFilePath, ex);

                    if (!overwrite && File.Exists(destinationFilePath))
                    {
                        throw new IOException("Tried to overwrite an existing file when overwrite = false: " + destinationFilePath);
                    }

                    if (attemptCount > maxRetryCount)
                        break;

                    // Wait several seconds before retrying
                    Global.IdleLoop(actualRetryHoldoffSeconds);

                    PRISM.AppUtils.GarbageCollectNow();
                }

                if (increaseHoldoffOnEachRetry)
                {
                    actualRetryHoldoffSeconds *= 1.5f;
                }
            }

            throw new IOException("Excessive failures during file copy");
        }

        /// <summary>
        /// Copy failed results from sourceDirectoryPath to the DMS_FailedResults directory on the local computer
        /// </summary>
        /// <param name="sourceDirectoryPath">Source directory path</param>
        public void CopyFailedResultsToArchiveDirectory(string sourceDirectoryPath)
        {
            if (Global.OfflineMode)
            {
                // Offline mode jobs each have their own work directory
                // Thus, copying of failed results is not applicable
                return;
            }

            var failedResultsDirectoryPath = mMgrParams.GetParam("FailedResultsFolderPath");

            if (string.IsNullOrEmpty(failedResultsDirectoryPath))
            {
                // Failed results directory path is not defined; don't try to copy the results anywhere
                LogError("FailedResultsFolderPath is not defined for this manager; cannot copy results");
                return;
            }

            CopyFailedResultsToArchiveDirectory(sourceDirectoryPath, failedResultsDirectoryPath);
        }

        /// <summary>
        /// Copy failed results from sourceDirectoryPath to the DMS_FailedResults directory on the local computer
        /// </summary>
        /// <param name="sourceDirectoryPath">Source directory path</param>
        /// <param name="failedResultsDirectoryPath">Failed results directory path, e.g. C:\DMS_FailedResults</param>
        public void CopyFailedResultsToArchiveDirectory(string sourceDirectoryPath, string failedResultsDirectoryPath)
        {
            if (Global.OfflineMode)
            {
                // Offline mode jobs each have their own work directory
                // Thus, copying of failed results is not applicable
                return;
            }

            try
            {
                // Make sure the target directory exists
                CreateDirectoryWithRetry(failedResultsDirectoryPath, 2, 5);

                var sourceDirectory = new DirectoryInfo(sourceDirectoryPath);
                var targetDirectory = new DirectoryInfo(failedResultsDirectoryPath);
                var folderInfoFilePath = "??";

                // Create an info file that describes the saved results
                try
                {
                    folderInfoFilePath = Path.Combine(targetDirectory.FullName, FAILED_RESULTS_FOLDER_INFO_TEXT + sourceDirectory.Name + ".txt");
                    CopyFailedResultsCreateInfoFile(folderInfoFilePath, sourceDirectory.Name);
                }
                catch (Exception ex)
                {
                    LogError("Error creating the results folder info file at " + folderInfoFilePath, ex);
                }

                // Make sure the source directory exists
                if (!sourceDirectory.Exists)
                {
                    LogError("Results directory not found; cannot copy results: " + sourceDirectoryPath);
                }
                else
                {
                    // Look for failed results directories that were archived over FAILED_RESULTS_FOLDER_RETAIN_DAYS days ago
                    DeleteOldFailedResultsFolders(targetDirectory);

                    var targetDirectoryPath = Path.Combine(targetDirectory.FullName, sourceDirectory.Name);

                    // Actually copy the results directory
                    LogMessage("Copying results directory to failed results archive: " + targetDirectoryPath);

                    CopyDirectory(sourceDirectory.FullName, targetDirectoryPath, true, 2, true);

                    LogMessage("Copy complete");
                }
            }
            catch (Exception ex)
            {
                LogError("Error copying results from " + sourceDirectoryPath + " to " + failedResultsDirectoryPath, ex);
            }
        }

        private void CopyFailedResultsCreateInfoFile(string folderInfoFilePath, string resultsFolderName)
        {
            using var writer = new StreamWriter(new FileStream(folderInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

            writer.WriteLine("Date" + '\t' + DateTime.Now);
            writer.WriteLine("ResultsFolderName" + '\t' + resultsFolderName);
            writer.WriteLine("Manager" + '\t' + mMgrParams.ManagerName);

            if (mJobParams != null)
            {
                writer.WriteLine("JobToolDescription" + '\t' + mJobParams.GetCurrentJobToolDescription());
                writer.WriteLine("Job" + '\t' + mJobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "Job"));
                writer.WriteLine("Step" + '\t' + mJobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "Step"));
            }

            writer.WriteLine("Date" + '\t' + DateTime.Now);

            if (mJobParams == null)
                return;

            // The ToolName job parameter holds the name of the job script we are executing
            writer.WriteLine("Tool" + '\t' + mJobParams.GetParam("ToolName"));
            writer.WriteLine("StepTool" + '\t' + mJobParams.GetParam("StepTool"));
            writer.WriteLine("Dataset" + '\t' + AnalysisResources.GetDatasetName(mJobParams));
            writer.WriteLine("XferFolder" + '\t' + mJobParams.GetParam(AnalysisResources.JOB_PARAM_TRANSFER_DIRECTORY_PATH));
            writer.WriteLine("ParamFileName" + '\t' + mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE));
            writer.WriteLine("SettingsFileName" + '\t' + mJobParams.GetParam("SettingsFileName"));
            writer.WriteLine("LegacyOrganismDBName" + '\t' + mJobParams.GetParam("LegacyFastaFileName"));
            writer.WriteLine("ProteinCollectionList" + '\t' + mJobParams.GetParam("ProteinCollectionList"));
            writer.WriteLine("ProteinOptionsList" + '\t' + mJobParams.GetParam("ProteinOptions"));
            writer.WriteLine("FastaFileName" + '\t' + mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));
        }

        /// <summary>
        /// Create the directory (if it does not yet exist)
        /// </summary>
        /// <remarks>Tries up to 3 times, waiting 15 seconds between attempts</remarks>
        /// <param name="directoryPath">Directory to create</param>
        public void CreateDirectoryWithRetry(string directoryPath)
        {
            const bool increaseHoldoffOnEachRetry = false;
            CreateDirectoryWithRetry(directoryPath, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, increaseHoldoffOnEachRetry);
        }

        /// <summary>
        /// Create the directory (if it does not yet exist)
        /// </summary>
        /// <param name="directoryPath">Directory to create</param>
        /// <param name="maxRetryCount">Maximum attempts</param>
        /// <param name="retryHoldoffSeconds">Seconds between attempts</param>
        public void CreateDirectoryWithRetry(string directoryPath, int maxRetryCount, int retryHoldoffSeconds)
        {
            const bool increaseHoldoffOnEachRetry = false;
            CreateDirectoryWithRetry(directoryPath, maxRetryCount, retryHoldoffSeconds, increaseHoldoffOnEachRetry);
        }

        /// <summary>
        /// Create the directory (if it does not yet exist)
        /// </summary>
        /// <param name="directoryPath">Directory to create</param>
        /// <param name="maxRetryCount">Maximum attempts</param>
        /// <param name="retryHoldoffSeconds">Seconds between attempts</param>
        /// <param name="increaseHoldoffOnEachRetry">If true, increase the holdoff between each attempt</param>
        public void CreateDirectoryWithRetry(string directoryPath, int maxRetryCount, int retryHoldoffSeconds, bool increaseHoldoffOnEachRetry)
        {
            var attemptCount = 0;
            float actualRetryHoldoffSeconds = retryHoldoffSeconds;

            if (actualRetryHoldoffSeconds < 1)
                actualRetryHoldoffSeconds = 1;

            if (maxRetryCount < 1)
                maxRetryCount = 1;

            if (string.IsNullOrWhiteSpace(directoryPath))
            {
                throw new DirectoryNotFoundException("Directory path cannot be empty when calling CreateDirectoryWithRetry");
            }

            while (attemptCount <= maxRetryCount)
            {
                attemptCount++;

                try
                {
                    if (Directory.Exists(directoryPath))
                    {
                        // If the directory already exists, there is nothing to do
                        return;
                    }

                    // Note that .NET will automatically create any missing parent directories
                    Directory.CreateDirectory(directoryPath);
                    return;
                }
                catch (Exception ex)
                {
                    LogError("AnalysisResults: error creating directory " + directoryPath, ex);

                    if (attemptCount > maxRetryCount)
                        break;

                    // Wait several seconds before retrying
                    Global.IdleLoop(actualRetryHoldoffSeconds);

                    PRISM.AppUtils.GarbageCollectNow();
                }

                if (increaseHoldoffOnEachRetry)
                {
                    actualRetryHoldoffSeconds *= 1.5f;
                }
            }

            if (!DirectoryExistsWithRetry(directoryPath, 1, 3))
            {
                throw new IOException("Excessive failures during directory creation");
            }
        }

        private void DeleteOldFailedResultsFolders(DirectoryInfo targetDirectory)
        {
            var targetFilePath = "";

            // Determine the directory archive time by reading the modification times on the ResultsFolderInfo_ files
            foreach (var folderInfoFile in targetDirectory.GetFiles(FAILED_RESULTS_FOLDER_INFO_TEXT + "*"))
            {
                if (DateTime.UtcNow.Subtract(folderInfoFile.LastWriteTimeUtc).TotalDays < FAILED_RESULTS_FOLDER_RETAIN_DAYS)
                    continue;

                // File was modified before the threshold; delete the results directory, then rename this file

                try
                {
                    var oldResultsDirectoryName = Path.GetFileNameWithoutExtension(folderInfoFile.Name).Substring(FAILED_RESULTS_FOLDER_INFO_TEXT.Length);

                    if (folderInfoFile.DirectoryName == null)
                    {
                        LogWarning("Unable to determine the parent directory of " + folderInfoFile.FullName);
                        continue;
                    }

                    var oldResultsDirectory = new DirectoryInfo(Path.Combine(folderInfoFile.DirectoryName, oldResultsDirectoryName));

                    if (oldResultsDirectory.Exists)
                    {
                        LogMessage("Deleting old failed results directory: " + oldResultsDirectory.FullName);
                        oldResultsDirectory.Delete(true);
                    }

                    try
                    {
                        targetFilePath = Path.Combine(folderInfoFile.DirectoryName, "x_" + folderInfoFile.Name);
                        folderInfoFile.CopyTo(targetFilePath, true);
                        folderInfoFile.Delete();
                    }
                    catch (Exception ex)
                    {
                        LogError("Error renaming failed results info file to " + targetFilePath, ex);
                    }
                }
                catch (Exception ex)
                {
                    LogError("Error deleting old failed results directory", ex);
                }
            }
        }

        /// <summary>
        /// Check for the existence of a directory, retrying if an error
        /// </summary>
        /// <remarks>Checks up to 3 times, waiting 15 seconds between attempts</remarks>
        /// <param name="directoryPath">Directory to check</param>
        /// <returns>True if the directory exists, otherwise false</returns>
        public bool DirectoryExistsWithRetry(string directoryPath)
        {
            const bool increaseHoldoffOnEachRetry = false;
            return DirectoryExistsWithRetry(directoryPath, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, increaseHoldoffOnEachRetry);
        }

        /// <summary>
        /// Check for the existence of a directory, retrying if an error
        /// </summary>
        /// <param name="directoryPath">Directory to check</param>
        /// <param name="maxRetryCount">Maximum attempts</param>
        /// <param name="retryHoldoffSeconds">Seconds between attempts</param>
        /// <returns>True if the directory exists, otherwise false</returns>
        public bool DirectoryExistsWithRetry(string directoryPath, int maxRetryCount, int retryHoldoffSeconds)
        {
            const bool increaseHoldoffOnEachRetry = false;
            return DirectoryExistsWithRetry(directoryPath, maxRetryCount, retryHoldoffSeconds, increaseHoldoffOnEachRetry);
        }

        /// <summary>
        /// Check for the existence of a directory, retrying if an error
        /// </summary>
        /// <param name="directoryPath">Directory to check</param>
        /// <param name="maxRetryCount">Maximum attempts</param>
        /// <param name="retryHoldoffSeconds">Seconds between attempts</param>
        /// <param name="increaseHoldoffOnEachRetry">If true, increase the holdoff between each attempt</param>
        /// <returns>True if the directory exists, otherwise false</returns>
        public bool DirectoryExistsWithRetry(string directoryPath, int maxRetryCount, int retryHoldoffSeconds, bool increaseHoldoffOnEachRetry)
        {
            var attemptCount = 0;

            float actualRetryHoldoffSeconds = retryHoldoffSeconds;

            if (actualRetryHoldoffSeconds < 1)
                actualRetryHoldoffSeconds = 1;

            if (maxRetryCount < 1)
                maxRetryCount = 1;

            while (attemptCount <= maxRetryCount)
            {
                attemptCount++;

                try
                {
                    var directoryExists = Directory.Exists(directoryPath);
                    return directoryExists;
                }
                catch (Exception ex)
                {
                    LogError("AnalysisResults: error looking for directory " + directoryPath, ex);

                    if (attemptCount > maxRetryCount)
                        break;

                    // Wait several seconds before retrying
                    Global.IdleLoop(actualRetryHoldoffSeconds);

                    PRISM.AppUtils.GarbageCollectNow();
                }

                if (increaseHoldoffOnEachRetry)
                {
                    actualRetryHoldoffSeconds *= 1.5f;
                }
            }

            // Exception occurred; return false
            return false;
        }
    }
}
