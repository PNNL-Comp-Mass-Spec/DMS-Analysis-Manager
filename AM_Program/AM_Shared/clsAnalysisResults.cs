
using System;
using System.IO;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/19/2007
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{

    /// <summary>
    /// Analysis job results handling class
    /// </summary>
    public class clsAnalysisResults : clsAnalysisMgrBase
    {

        #region "Module variables"
        private const string FAILED_RESULTS_FOLDER_INFO_TEXT = "FailedResultsFolderInfo_";

        private const int FAILED_RESULTS_FOLDER_RETAIN_DAYS = 31;
        private const int DEFAULT_RETRY_COUNT = 3;

        private const int DEFAULT_RETRY_HOLDOFF_SEC = 15;

        /// <summary>
        /// access to the job parameters
        /// </summary>
        private readonly IJobParams m_jobParams;

        /// <summary>
        /// Access to manager parameters
        /// </summary>
        private readonly IMgrParams m_mgrParams;

        #endregion

        #region "Properties"

        /// <summary>
        /// explanation of what happened to last operation this class performed
        /// </summary>
        public string Message => m_message;

        #endregion

        #region "Methods"
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">Manager parameter object</param>
        /// <param name="jobParams">Job parameter object</param>
        /// <remarks></remarks>
        public clsAnalysisResults(IMgrParams mgrParams, IJobParams jobParams) : base("clsAnalysisResults")
        {

            m_mgrParams = mgrParams;
            m_jobParams = jobParams;
            var mgrName = m_mgrParams.ManagerName;
            m_DebugLevel = (short)m_mgrParams.GetParam("DebugLevel", 1);

            InitFileTools(mgrName, m_DebugLevel);

        }

        /// <summary>
        /// Copies a source directory to the destination directory. Allows overwriting.
        /// </summary>
        /// <param name="sourcePath">The source directory path.</param>
        /// <param name="destPath">The destination directory path.</param>
        /// <param name="overwrite">True if the destination file can be overwritten; otherwise, false.</param>
        /// <remarks></remarks>
        public void CopyDirectory(string sourcePath, string destPath, bool overwrite)
        {
            CopyDirectory(sourcePath, destPath, overwrite, maxRetryCount: DEFAULT_RETRY_COUNT, continueOnError: true);
        }

        /// <summary>
        /// Copies a source directory to the destination directory. Allows overwriting.
        /// </summary>
        /// <param name="sourcePath">The source directory path.</param>
        /// <param name="destPath">The destination directory path.</param>
        /// <param name="overwrite">True if the destination file can be overwritten; otherwise, false.</param>
        /// <param name="maxRetryCount">The number of times to retry a failed copy of a file; if 0 or 1 then only tries once</param>
        /// <param name="continueOnError">When true, then will continue copying even if an error occurs</param>
        /// <remarks></remarks>
        public void CopyDirectory(string sourcePath, string destPath, bool overwrite, int maxRetryCount, bool continueOnError)
        {
            var diSourceDir = new DirectoryInfo(sourcePath);
            var diDestDir = new DirectoryInfo(destPath);

            string message;

            // The source directory must exist, otherwise throw an exception
            if (!FolderExistsWithRetry(diSourceDir.FullName, 3, 3))
            {
                message = "Source directory does not exist: " + diSourceDir.FullName;
                if (continueOnError)
                {
                    LogError(message);
                    return;
                }

                throw new DirectoryNotFoundException(message);
            }

            // If destination SubDir's parent SubDir does not exist throw an exception
            if (diDestDir.Parent == null)
            {
                message = "Unable to determine the parent folder of " + diDestDir.FullName;
                if (continueOnError)
                {
                    LogError(message);
                    return;
                }

                throw new DirectoryNotFoundException(message);
            }

            if (!FolderExistsWithRetry(diDestDir.Parent.FullName, 1, 1))
            {
                message = "Destination directory does not exist: " + diDestDir.Parent.FullName;
                if (continueOnError)
                {
                    LogError(message);
                    return;
                }

                throw new DirectoryNotFoundException(message);
            }

            if (!FolderExistsWithRetry(diDestDir.FullName, 3, 3))
            {
                CreateFolderWithRetry(destPath, maxRetryCount, DEFAULT_RETRY_HOLDOFF_SEC);
            }

            // Copy all the files of the current directory
            foreach (var childFile in diSourceDir.GetFiles())
            {
                try
                {
                    var targetPath = Path.Combine(diDestDir.FullName, childFile.Name);
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
                catch (Exception ex)
                {
                    if (continueOnError)
                    {
                        LogError("clsAnalysisResults,CopyDirectory", ex);
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            // Copy all the sub-directories by recursively calling this same routine
            foreach (var subDir in diSourceDir.GetDirectories())
            {
                CopyDirectory(subDir.FullName, Path.Combine(diDestDir.FullName, subDir.Name), overwrite, maxRetryCount, continueOnError);
            }
        }

        /// <summary>
        /// Copy the file, optionally overwriting
        /// </summary>
        /// <param name="srcFilePath">Source file path</param>
        /// <param name="destFilePath">Destination file path</param>
        /// <param name="overwrite">True to overwrite if it exists</param>
        /// <remarks>Tries up to 3 times, waiting 15 seconds between attempts</remarks>
        public void CopyFileWithRetry(string srcFilePath, string destFilePath, bool overwrite)
        {
            const bool increaseHoldoffOnEachRetry = false;
            CopyFileWithRetry(srcFilePath, destFilePath, overwrite, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, increaseHoldoffOnEachRetry);
        }

        /// <summary>
        /// Copy the file, optionally overwriting
        /// </summary>
        /// <param name="srcFilePath">Source file path</param>
        /// <param name="destFilePath">Destination file path</param>
        /// <param name="overwrite">True to overwrite if it exists</param>
        /// <param name="increaseHoldoffOnEachRetry"></param>
        public void CopyFileWithRetry(string srcFilePath, string destFilePath, bool overwrite, bool increaseHoldoffOnEachRetry)
        {
            CopyFileWithRetry(srcFilePath, destFilePath, overwrite, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, increaseHoldoffOnEachRetry);
        }

        /// <summary>
        /// Copy the file, optionally overwriting
        /// </summary>
        /// <param name="srcFilePath">Source file path</param>
        /// <param name="destFilePath">Destination file path</param>
        /// <param name="overwrite">True to overwrite if it exists</param>
        /// <param name="maxRetryCount">Maximum attempts</param>
        /// <param name="retryHoldoffSeconds">Seconds between attempts</param>
        public void CopyFileWithRetry(string srcFilePath, string destFilePath, bool overwrite, int maxRetryCount, int retryHoldoffSeconds)
        {
            const bool increaseHoldoffOnEachRetry = false;
            CopyFileWithRetry(srcFilePath, destFilePath, overwrite, maxRetryCount, retryHoldoffSeconds, increaseHoldoffOnEachRetry);
        }

        /// <summary>
        /// Copy the file, optionally overwriting
        /// </summary>
        /// <param name="srcFilePath">Source file path</param>
        /// <param name="destFilePath">Destination file path</param>
        /// <param name="overwrite">True to overwrite if it exists</param>
        /// <param name="maxRetryCount">Maximum attempts</param>
        /// <param name="retryHoldoffSeconds">Seconds between attempts</param>
        /// <param name="increaseHoldoffOnEachRetry">If true, increase the holdoff between each attempt</param>
        public void CopyFileWithRetry(string srcFilePath, string destFilePath, bool overwrite, int maxRetryCount, int retryHoldoffSeconds, bool increaseHoldoffOnEachRetry)
        {
            var attemptCount = 0;
            float actualRetryHoldoffSeconds = retryHoldoffSeconds;

            if (actualRetryHoldoffSeconds < 1)
                actualRetryHoldoffSeconds = 1;
            if (maxRetryCount < 1)
                maxRetryCount = 1;

            // First make sure the source file exists
            if (!File.Exists(srcFilePath))
            {
                throw new IOException("clsAnalysisResults,CopyFileWithRetry: Source file not found for copy operation: " + srcFilePath);
            }

            while (attemptCount <= maxRetryCount)
            {
                attemptCount += 1;

                try
                {
                    ResetTimestampForQueueWaitTimeLogging();
                    var startTime = DateTime.UtcNow;

                    if (m_FileTools.CopyFileUsingLocks(srcFilePath, destFilePath, overwrite))
                    {
                        LogCopyStats(startTime, destFilePath);
                        return;
                    }

                    LogError("CopyFileUsingLocks returned false copying " + srcFilePath + " to " + destFilePath);
                }
                catch (Exception ex)
                {
                    LogError("clsAnalysisResults,CopyFileWithRetry: error copying " + srcFilePath + " to " + destFilePath, ex);

                    if (!overwrite && File.Exists(destFilePath))
                    {
                        throw new IOException("Tried to overwrite an existing file when overwrite = False: " + destFilePath);
                    }

                    if (attemptCount > maxRetryCount)
                        break;

                    // Wait several seconds before retrying
                    clsGlobal.IdleLoop(actualRetryHoldoffSeconds);

                    PRISM.ProgRunner.GarbageCollectNow();
                }

                if (increaseHoldoffOnEachRetry)
                {
                    actualRetryHoldoffSeconds *= 1.5f;
                }
            }

            throw new IOException("Excessive failures during file copy");

        }

        /// <summary>
        /// Copy failed results from sourceFolderPath to the DMS_FailedResults directory on the local computer
        /// </summary>
        /// <param name="sourceFolderPath">Source folder path</param>
        public void CopyFailedResultsToArchiveFolder(string sourceFolderPath)
        {
            if (clsGlobal.OfflineMode)
            {
                // Offline mode jobs each have their own work directory
                // Thus, copying of failed results is not applicable
                return;
            }

            var failedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");

            if (string.IsNullOrEmpty(failedResultsFolderPath))
            {
                // Failed results folder path is not defined; don't try to copy the results anywhere
                LogError("FailedResultsFolderPath is not defined for this manager; cannot copy results");
                return;
            }

            CopyFailedResultsToArchiveFolder(sourceFolderPath, failedResultsFolderPath);
        }

        /// <summary>
        /// Copy failed results from sourceFolderPath to the DMS_FailedResults directory on the local computer
        /// </summary>
        /// <param name="sourceFolderPath">Source folder path</param>
        /// <param name="failedResultsFolderPath">Failed results folder path, e.g. C:\DMS_FailedResults</param>
        public void CopyFailedResultsToArchiveFolder(string sourceFolderPath, string failedResultsFolderPath)
        {
            if (clsGlobal.OfflineMode)
            {
                // Offline mode jobs each have their own work directory
                // Thus, copying of failed results is not applicable
                return;
            }

            try
            {

                // Make sure the target folder exists
                CreateFolderWithRetry(failedResultsFolderPath, 2, 5);

                var diSourceFolder = new DirectoryInfo(sourceFolderPath);
                var diTargetFolder = new DirectoryInfo(failedResultsFolderPath);
                var folderInfoFilePath = "??";

                // Create an info file that describes the saved results
                try
                {
                    folderInfoFilePath = Path.Combine(diTargetFolder.FullName, FAILED_RESULTS_FOLDER_INFO_TEXT + diSourceFolder.Name + ".txt");
                    CopyFailedResultsCreateInfoFile(folderInfoFilePath, diSourceFolder.Name);
                }
                catch (Exception ex)
                {
                    LogError("Error creating the results folder info file at " + folderInfoFilePath, ex);
                }

                // Make sure the source folder exists
                if (!diSourceFolder.Exists)
                {
                    LogError("Results folder not found; cannot copy results: " + sourceFolderPath);
                }
                else
                {
                    // Look for failed results folders that were archived over FAILED_RESULTS_FOLDER_RETAIN_DAYS days ago
                    DeleteOldFailedResultsFolders(diTargetFolder);

                    var targetFolderPath = Path.Combine(diTargetFolder.FullName, diSourceFolder.Name);

                    // Actually copy the results folder
                    LogMessage("Copying results folder to failed results archive: " + targetFolderPath);

                    CopyDirectory(diSourceFolder.FullName, targetFolderPath, true, 2, true);

                    LogMessage("Copy complete");
                }

            }
            catch (Exception ex)
            {
                LogError("Error copying results from " + sourceFolderPath + " to " + failedResultsFolderPath, ex);
            }

        }

        private void CopyFailedResultsCreateInfoFile(string folderInfoFilePath, string resultsFolderName)
        {
            using (var swInfoFile = new StreamWriter(new FileStream(folderInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                swInfoFile.WriteLine("Date" + '\t' + DateTime.Now);
                swInfoFile.WriteLine("ResultsFolderName" + '\t' + resultsFolderName);
                swInfoFile.WriteLine("Manager" + '\t' + m_mgrParams.ManagerName);

                if ((m_jobParams != null))
                {
                    swInfoFile.WriteLine("JobToolDescription" + '\t' + m_jobParams.GetCurrentJobToolDescription());
                    swInfoFile.WriteLine("Job" + '\t' + m_jobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job"));
                    swInfoFile.WriteLine("Step" + '\t' + m_jobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step"));
                }

                swInfoFile.WriteLine("Date" + '\t' + DateTime.Now);
                if ((m_jobParams != null))
                {
                    swInfoFile.WriteLine("Tool" + '\t' + m_jobParams.GetParam("ToolName"));
                    swInfoFile.WriteLine("StepTool" + '\t' + m_jobParams.GetParam("StepTool"));
                    swInfoFile.WriteLine("Dataset" + '\t' + m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_DATASET_NAME));
                    swInfoFile.WriteLine("XferFolder" + '\t' + m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH));
                    swInfoFile.WriteLine("ParamFileName" + '\t' + m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_PARAMETER_FILE));
                    swInfoFile.WriteLine("SettingsFileName" + '\t' + m_jobParams.GetParam("SettingsFileName"));
                    swInfoFile.WriteLine("LegacyOrganismDBName" + '\t' + m_jobParams.GetParam("LegacyFastaFileName"));
                    swInfoFile.WriteLine("ProteinCollectionList" + '\t' + m_jobParams.GetParam("ProteinCollectionList"));
                    swInfoFile.WriteLine("ProteinOptionsList" + '\t' + m_jobParams.GetParam("ProteinOptions"));
                    swInfoFile.WriteLine("FastaFileName" + '\t' + m_jobParams.GetParam("PeptideSearch", clsAnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));
                }
            }

        }

        /// <summary>
        /// Create the directory (if it does not yet exist)
        /// </summary>
        /// <param name="folderPath">Folder to create</param>
        /// <remarks>Tries up to 3 times, waiting 15 seconds between attempts</remarks>
        public void CreateFolderWithRetry(string folderPath)
        {
            const bool increaseHoldoffOnEachRetry = false;
            CreateFolderWithRetry(folderPath, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, increaseHoldoffOnEachRetry);
        }

        /// <summary>
        /// Create the directory (if it does not yet exist)
        /// </summary>
        /// <param name="folderPath">Folder to create</param>
        /// <param name="maxRetryCount">Maximum attempts</param>
        /// <param name="retryHoldoffSeconds">Seconds between attempts</param>
        public void CreateFolderWithRetry(string folderPath, int maxRetryCount, int retryHoldoffSeconds)
        {
            const bool increaseHoldoffOnEachRetry = false;
            CreateFolderWithRetry(folderPath, maxRetryCount, retryHoldoffSeconds, increaseHoldoffOnEachRetry);
        }

        /// <summary>
        /// Create the directory (if it does not yet exist)
        /// </summary>
        /// <param name="folderPath">Folder to create</param>
        /// <param name="maxRetryCount">Maximum attempts</param>
        /// <param name="retryHoldoffSeconds">Seconds between attempts</param>
        /// <param name="increaseHoldoffOnEachRetry">If true, increase the holdoff between each attempt</param>
        public void CreateFolderWithRetry(string folderPath, int maxRetryCount, int retryHoldoffSeconds, bool increaseHoldoffOnEachRetry)
        {
            var attemptCount = 0;
            float actualRetryHoldoffSeconds = retryHoldoffSeconds;

            if (actualRetryHoldoffSeconds < 1)
                actualRetryHoldoffSeconds = 1;
            if (maxRetryCount < 1)
                maxRetryCount = 1;

            if (string.IsNullOrWhiteSpace(folderPath))
            {
                throw new DirectoryNotFoundException("Folder path cannot be empty when calling CreateFolderWithRetry");
            }

            while (attemptCount <= maxRetryCount)
            {
                attemptCount += 1;

                try
                {
                    if (Directory.Exists(folderPath))
                    {
                        // If the directory already exists, there is nothing to do
                        return;
                    }

                    // Note that .NET will automatically create any missing parent directories
                    Directory.CreateDirectory(folderPath);
                    return;
                }
                catch (Exception ex)
                {
                    LogError("clsAnalysisResults: error creating folder " + folderPath, ex);

                    if (attemptCount > maxRetryCount)
                        break;

                    // Wait several seconds before retrying
                    clsGlobal.IdleLoop(actualRetryHoldoffSeconds);

                    PRISM.ProgRunner.GarbageCollectNow();
                }

                if (increaseHoldoffOnEachRetry)
                {
                    actualRetryHoldoffSeconds *= 1.5f;
                }
            }

            if (!FolderExistsWithRetry(folderPath, 1, 3))
            {
                throw new IOException("Excessive failures during folder creation");
            }

        }

        private void DeleteOldFailedResultsFolders(DirectoryInfo diTargetFolder)
        {
            var targetFilePath = "";

            // Determine the directory archive time by reading the modification times on the ResultsFolderInfo_ files
            foreach (var fiFileInfo in diTargetFolder.GetFiles(FAILED_RESULTS_FOLDER_INFO_TEXT + "*"))
            {
                if (DateTime.UtcNow.Subtract(fiFileInfo.LastWriteTimeUtc).TotalDays < FAILED_RESULTS_FOLDER_RETAIN_DAYS)
                    continue;

                // File was modified before the threshold; delete the results folder, then rename this file

                try
                {
                    var oldResultsFolderName = Path.GetFileNameWithoutExtension(fiFileInfo.Name).Substring(FAILED_RESULTS_FOLDER_INFO_TEXT.Length);
                    if (fiFileInfo.DirectoryName == null)
                    {
                        LogWarning("Unable to determine the parent directory of " + fiFileInfo.FullName);
                        continue;
                    }

                    var diOldResultsFolder = new DirectoryInfo(Path.Combine(fiFileInfo.DirectoryName, oldResultsFolderName));

                    if (diOldResultsFolder.Exists)
                    {
                        LogMessage("Deleting old failed results folder: " + diOldResultsFolder.FullName);
                        diOldResultsFolder.Delete(true);
                    }

                    try
                    {
                        targetFilePath = Path.Combine(fiFileInfo.DirectoryName, "x_" + fiFileInfo.Name);
                        fiFileInfo.CopyTo(targetFilePath, true);
                        fiFileInfo.Delete();
                    }
                    catch (Exception ex)
                    {
                        LogError("Error renaming failed results info file to " + targetFilePath, ex);
                    }

                }
                catch (Exception ex)
                {
                    LogError("Error deleting old failed results folder", ex);
                }
            }

        }

        /// <summary>
        /// Check for the existence of a folder, retrying if an error
        /// </summary>
        /// <param name="folderPath"></param>
        /// <returns>True if the directory exists, otherwise false</returns>
        /// <remarks>Checks up to 3 times, waiting 15 seconds between attempts</remarks>
        public bool FolderExistsWithRetry(string folderPath)
        {
            const bool increaseHoldoffOnEachRetry = false;
            return FolderExistsWithRetry(folderPath, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, increaseHoldoffOnEachRetry);
        }

        /// <summary>
        /// Check for the existence of a folder, retrying if an error
        /// </summary>
        /// <param name="folderPath">Folder to check</param>
        /// <param name="maxRetryCount">Maximum attempts</param>
        /// <param name="retryHoldoffSeconds">Seconds between attempts</param>
        /// <returns>True if the directory exists, otherwise false</returns>
        public bool FolderExistsWithRetry(string folderPath, int maxRetryCount, int retryHoldoffSeconds)
        {
            const bool increaseHoldoffOnEachRetry = false;
            return FolderExistsWithRetry(folderPath, maxRetryCount, retryHoldoffSeconds, increaseHoldoffOnEachRetry);
        }

        /// <summary>
        /// Check for the existence of a folder, retrying if an error
        /// </summary>
        /// <param name="folderPath">Folder to check</param>
        /// <param name="maxRetryCount">Maximum attempts</param>
        /// <param name="retryHoldoffSeconds">Seconds between attempts</param>
        /// <param name="increaseHoldoffOnEachRetry">If true, increase the holdoff between each attempt</param>
        /// <returns>True if the directory exists, otherwise false</returns>
        public bool FolderExistsWithRetry(string folderPath, int maxRetryCount, int retryHoldoffSeconds, bool increaseHoldoffOnEachRetry)
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
                    var folderExists = Directory.Exists(folderPath);
                    return folderExists;

                }
                catch (Exception ex)
                {
                    LogError("clsAnalysisResults: error looking for folder " + folderPath, ex);

                    if (attemptCount > maxRetryCount)
                        break;

                    // Wait several seconds before retrying
                    clsGlobal.IdleLoop(actualRetryHoldoffSeconds);

                    PRISM.ProgRunner.GarbageCollectNow();
                }

                if (increaseHoldoffOnEachRetry)
                {
                    actualRetryHoldoffSeconds *= 1.5f;
                }

            }

            // Exception occurred; return False
            return false;

        }

        #endregion

    }

}