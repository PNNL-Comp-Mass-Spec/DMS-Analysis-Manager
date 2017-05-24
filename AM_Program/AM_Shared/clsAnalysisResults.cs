
using System;
using System.IO;
using System.Threading;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
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
        // access to the job parameters

        private readonly IJobParams m_jobParams;
        // access to mgr parameters
        private readonly IMgrParams m_mgrParams;

        private readonly string m_MgrName;
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
            m_MgrName = m_mgrParams.GetParam("MgrName", "Undefined-Manager");
            m_DebugLevel = (short)(m_mgrParams.GetParam("debuglevel", 1));

            base.InitFileTools(m_MgrName, m_DebugLevel);

        }

        /// <summary>
        /// Copies a source directory to the destination directory. Allows overwriting.
        /// </summary>
        /// <param name="SourcePath">The source directory path.</param>
        /// <param name="DestPath">The destination directory path.</param>
        /// <param name="Overwrite">True if the destination file can be overwritten; otherwise, false.</param>
        /// <remarks></remarks>
        public void CopyDirectory(string SourcePath, string DestPath, bool Overwrite)
        {
            CopyDirectory(SourcePath, DestPath, Overwrite, MaxRetryCount: DEFAULT_RETRY_COUNT, continueOnError: true);
        }

        /// <summary>
        /// Copies a source directory to the destination directory. Allows overwriting.
        /// </summary>
        /// <param name="SourcePath">The source directory path.</param>
        /// <param name="DestPath">The destination directory path.</param>
        /// <param name="Overwrite">True if the destination file can be overwritten; otherwise, false.</param>
        /// <param name="MaxRetryCount">The number of times to retry a failed copy of a file; if 0 or 1 then only tries once</param>
        /// <param name="continueOnError">When true, then will continue copying even if an error occurs</param>
        /// <remarks></remarks>
        public void CopyDirectory(string SourcePath, string DestPath, bool Overwrite, int MaxRetryCount, bool continueOnError)
        {
            var diSourceDir = new DirectoryInfo(SourcePath);
            var diDestDir = new DirectoryInfo(DestPath);

            string strMessage;

            // The source directory must exist, otherwise throw an exception
            if (!FolderExistsWithRetry(diSourceDir.FullName, 3, 3))
            {
                strMessage = "Source directory does not exist: " + diSourceDir.FullName;
                if (continueOnError)
                {
                    LogError(strMessage);
                    return;
                }

                throw new DirectoryNotFoundException(strMessage);
            }

            // If destination SubDir's parent SubDir does not exist throw an exception
            if (diDestDir.Parent == null)
            {
                strMessage = "Unable to determine the parent folder of " + diDestDir.FullName;
                if (continueOnError)
                {
                    LogError(strMessage);
                    return;
                }

                throw new DirectoryNotFoundException(strMessage);
            }

            if (!FolderExistsWithRetry(diDestDir.Parent.FullName, 1, 1))
            {
                strMessage = "Destination directory does not exist: " + diDestDir.Parent.FullName;
                if (continueOnError)
                {
                    LogError(strMessage);
                    return;
                }

                throw new DirectoryNotFoundException(strMessage);
            }

            if (!FolderExistsWithRetry(diDestDir.FullName, 3, 3))
            {
                CreateFolderWithRetry(DestPath, MaxRetryCount, DEFAULT_RETRY_HOLDOFF_SEC);
            }

            // Copy all the files of the current directory
            foreach (var childFile in diSourceDir.GetFiles())
            {
                try
                {
                    var strTargetPath = Path.Combine(diDestDir.FullName, childFile.Name);
                    if (Overwrite)
                    {
                        CopyFileWithRetry(childFile.FullName, strTargetPath, true, MaxRetryCount, DEFAULT_RETRY_HOLDOFF_SEC);

                    }
                    else
                    {
                        // Only copy if the file does not yet exist
                        // We are not throwing an error if the file exists in the target
                        if (!File.Exists(strTargetPath))
                        {
                            CopyFileWithRetry(childFile.FullName, strTargetPath, false, MaxRetryCount, DEFAULT_RETRY_HOLDOFF_SEC);
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
                CopyDirectory(subDir.FullName, Path.Combine(diDestDir.FullName, subDir.Name), Overwrite, MaxRetryCount, continueOnError);
            }
        }

        public void CopyFileWithRetry(string SrcFilePath, string DestFilePath, bool Overwrite)
        {
            const bool blnIncreaseHoldoffOnEachRetry = false;
            CopyFileWithRetry(SrcFilePath, DestFilePath, Overwrite, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, blnIncreaseHoldoffOnEachRetry);
        }

        public void CopyFileWithRetry(string SrcFilePath, string DestFilePath, bool Overwrite, bool blnIncreaseHoldoffOnEachRetry)
        {
            CopyFileWithRetry(SrcFilePath, DestFilePath, Overwrite, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, blnIncreaseHoldoffOnEachRetry);
        }

        public void CopyFileWithRetry(string SrcFilePath, string DestFilePath, bool Overwrite, int MaxRetryCount, int RetryHoldoffSeconds)
        {
            const bool blnIncreaseHoldoffOnEachRetry = false;
            CopyFileWithRetry(SrcFilePath, DestFilePath, Overwrite, MaxRetryCount, RetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry);
        }


        public void CopyFileWithRetry(string SrcFilePath, string DestFilePath, bool Overwrite, int MaxRetryCount, int RetryHoldoffSeconds, bool blnIncreaseHoldoffOnEachRetry)
        {
            var AttemptCount = 0;
            var blnSuccess = false;
            float sngRetryHoldoffSeconds = RetryHoldoffSeconds;

            if (sngRetryHoldoffSeconds < 1)
                sngRetryHoldoffSeconds = 1;
            if (MaxRetryCount < 1)
                MaxRetryCount = 1;

            // First make sure the source file exists
            if (!File.Exists(SrcFilePath))
            {
                throw new IOException("clsAnalysisResults,CopyFileWithRetry: Source file not found for copy operation: " + SrcFilePath);
            }

            while (AttemptCount <= MaxRetryCount & !blnSuccess)
            {
                AttemptCount += 1;

                try
                {
                    ResetTimestampForQueueWaitTimeLogging();
                    if (m_FileTools.CopyFileUsingLocks(SrcFilePath, DestFilePath, m_MgrName, Overwrite))
                    {
                        blnSuccess = true;
                    }
                    else
                    {
                        LogError("CopyFileUsingLocks returned false copying " + SrcFilePath + " to " + DestFilePath);
                    }

                }
                catch (Exception ex)
                {
                    LogError("clsAnalysisResults,CopyFileWithRetry: error copying " + SrcFilePath + " to " + DestFilePath, ex);

                    if (!Overwrite && File.Exists(DestFilePath))
                    {
                        throw new IOException("Tried to overwrite an existing file when Overwrite = False: " + DestFilePath);
                    }

                    // Wait several seconds before retrying
                    Thread.Sleep((int)(Math.Floor(sngRetryHoldoffSeconds * 1000)));

                    PRISM.clsProgRunner.GarbageCollectNow();
                }

                if (!blnSuccess && blnIncreaseHoldoffOnEachRetry)
                {
                    sngRetryHoldoffSeconds *= 1.5f;
                }
            }

            if (!blnSuccess)
            {
                throw new IOException("Excessive failures during file copy");
            }

        }


        public void CopyFailedResultsToArchiveFolder(string ResultsFolderPath)
        {
            var strFailedResultsFolderPath = string.Empty;

            try
            {
                strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");

                if (string.IsNullOrEmpty(strFailedResultsFolderPath))
                {
                    // Failed results folder path is not defined; don't try to copy the results anywhere
                    LogError("FailedResultsFolderPath is not defined for this manager; cannot copy results");
                    return;
                }

                // Make sure the target folder exists
                CreateFolderWithRetry(strFailedResultsFolderPath, 2, 5);

                var diSourceFolder = new DirectoryInfo(ResultsFolderPath);
                var diTargetFolder = new DirectoryInfo(strFailedResultsFolderPath);
                var strFolderInfoFilePath = "??";

                // Create an info file that describes the saved results
                try
                {
                    strFolderInfoFilePath = Path.Combine(diTargetFolder.FullName, FAILED_RESULTS_FOLDER_INFO_TEXT + diSourceFolder.Name + ".txt");
                    CopyFailedResultsCreateInfoFile(strFolderInfoFilePath, diSourceFolder.Name);
                }
                catch (Exception ex)
                {
                    LogError("Error creating the results folder info file at " + strFolderInfoFilePath, ex);
                }

                // Make sure the source folder exists
                if (!diSourceFolder.Exists)
                {
                    LogError("Results folder not found; cannot copy results: " + ResultsFolderPath);
                }
                else
                {
                    // Look for failed results folders that were archived over FAILED_RESULTS_FOLDER_RETAIN_DAYS days ago
                    DeleteOldFailedResultsFolders(diTargetFolder);

                    var strTargetFolderPath = Path.Combine(diTargetFolder.FullName, diSourceFolder.Name);

                    // Actually copy the results folder
                    LogMessage("Copying results folder to failed results archive: " + strTargetFolderPath);

                    CopyDirectory(diSourceFolder.FullName, strTargetFolderPath, true, 2, true);

                    LogMessage("Copy complete");
                }

            }
            catch (Exception ex)
            {
                LogError("Error copying results from " + ResultsFolderPath + " to " + strFailedResultsFolderPath, ex);
            }

        }


        private void CopyFailedResultsCreateInfoFile(string strFolderInfoFilePath, string strResultsFolderName)
        {
            using (var swInfoFile = new StreamWriter(new FileStream(strFolderInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                swInfoFile.WriteLine("Date" + '\t' + DateTime.Now);
                swInfoFile.WriteLine("ResultsFolderName" + '\t' + strResultsFolderName);
                swInfoFile.WriteLine("Manager" + '\t' + m_mgrParams.GetParam("MgrName"));

                if ((m_jobParams != null))
                {
                    swInfoFile.WriteLine("JobToolDescription" + '\t' + m_jobParams.GetCurrentJobToolDescription());
                    swInfoFile.WriteLine("Job" + '\t' + m_jobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job"));
                    swInfoFile.WriteLine("Step" + '\t' + m_jobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step"));
                }

                swInfoFile.WriteLine("Date" + '\t' + DateTime.Now);
                if ((m_jobParams != null))
                {
                    swInfoFile.WriteLine("Tool" + '\t' + m_jobParams.GetParam("toolname"));
                    swInfoFile.WriteLine("StepTool" + '\t' + m_jobParams.GetParam("StepTool"));
                    swInfoFile.WriteLine("Dataset" + '\t' + m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetNum"));
                    swInfoFile.WriteLine("XferFolder" + '\t' + m_jobParams.GetParam("transferFolderPath"));
                    swInfoFile.WriteLine("ParamFileName" + '\t' + m_jobParams.GetParam("parmFileName"));
                    swInfoFile.WriteLine("SettingsFileName" + '\t' + m_jobParams.GetParam("settingsFileName"));
                    swInfoFile.WriteLine("LegacyOrganismDBName" + '\t' + m_jobParams.GetParam("LegacyFastaFileName"));
                    swInfoFile.WriteLine("ProteinCollectionList" + '\t' + m_jobParams.GetParam("ProteinCollectionList"));
                    swInfoFile.WriteLine("ProteinOptionsList" + '\t' + m_jobParams.GetParam("ProteinOptions"));
                    swInfoFile.WriteLine("FastaFileName" + '\t' + m_jobParams.GetParam("PeptideSearch", clsAnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));
                }
            }

        }

        public void CreateFolderWithRetry(string FolderPath)
        {
            const bool blnIncreaseHoldoffOnEachRetry = false;
            CreateFolderWithRetry(FolderPath, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, blnIncreaseHoldoffOnEachRetry);
        }

        public void CreateFolderWithRetry(string FolderPath, int MaxRetryCount, int RetryHoldoffSeconds)
        {
            const bool blnIncreaseHoldoffOnEachRetry = false;
            CreateFolderWithRetry(FolderPath, MaxRetryCount, RetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry);
        }


        public void CreateFolderWithRetry(string FolderPath, int MaxRetryCount, int RetryHoldoffSeconds, bool blnIncreaseHoldoffOnEachRetry)
        {
            var AttemptCount = 0;
            var blnSuccess = false;
            float sngRetryHoldoffSeconds = RetryHoldoffSeconds;

            if (sngRetryHoldoffSeconds < 1)
                sngRetryHoldoffSeconds = 1;
            if (MaxRetryCount < 1)
                MaxRetryCount = 1;

            if (string.IsNullOrWhiteSpace(FolderPath))
            {
                throw new DirectoryNotFoundException("Folder path cannot be empty when calling CreateFolderWithRetry");
            }

            while (AttemptCount <= MaxRetryCount & !blnSuccess)
            {
                AttemptCount += 1;

                try
                {
                    if (Directory.Exists(FolderPath))
                    {
                        // If the folder already exists, then there is nothing to do
                        blnSuccess = true;
                    }
                    else
                    {
                        Directory.CreateDirectory(FolderPath);
                        blnSuccess = true;
                    }

                }
                catch (Exception ex)
                {
                    LogError("clsAnalysisResults: error creating folder " + FolderPath, ex);

                    // Wait several seconds before retrying
                    Thread.Sleep((int)(Math.Floor(sngRetryHoldoffSeconds * 1000)));

                    PRISM.clsProgRunner.GarbageCollectNow();
                }

                if (!blnSuccess && blnIncreaseHoldoffOnEachRetry)
                {
                    sngRetryHoldoffSeconds *= 1.5f;
                }
            }

            if (!blnSuccess)
            {
                if (!FolderExistsWithRetry(FolderPath, 1, 3))
                {
                    throw new IOException("Excessive failures during folder creation");
                }
            }

        }


        private void DeleteOldFailedResultsFolders(DirectoryInfo diTargetFolder)
        {
            var strTargetFilePath = "";

            // Determine the folder archive time by reading the modification times on the ResultsFolderInfo_ files
            foreach (var fiFileInfo in diTargetFolder.GetFiles(FAILED_RESULTS_FOLDER_INFO_TEXT + "*"))
            {
                if (DateTime.UtcNow.Subtract(fiFileInfo.LastWriteTimeUtc).TotalDays > FAILED_RESULTS_FOLDER_RETAIN_DAYS)
                {
                    // File was modified before the threshold; delete the results folder, then rename this file

                    try
                    {
                        var strOldResultsFolderName = Path.GetFileNameWithoutExtension(fiFileInfo.Name).Substring(FAILED_RESULTS_FOLDER_INFO_TEXT.Length);
                        if (fiFileInfo.DirectoryName == null)
                        {
                            LogWarning("Unable to determine the parent directory of " + fiFileInfo.FullName);
                            continue;
                        }

                        var diOldResultsFolder = new DirectoryInfo(Path.Combine(fiFileInfo.DirectoryName, strOldResultsFolderName));

                        if (diOldResultsFolder.Exists)
                        {
                            LogMessage("Deleting old failed results folder: " + diOldResultsFolder.FullName);
                            diOldResultsFolder.Delete(true);
                        }

                        try
                        {
                            strTargetFilePath = Path.Combine(fiFileInfo.DirectoryName, "x_" + fiFileInfo.Name);
                            fiFileInfo.CopyTo(strTargetFilePath, true);
                            fiFileInfo.Delete();
                        }
                        catch (Exception ex)
                        {
                            LogError("Error renaming failed results info file to " + strTargetFilePath, ex);
                        }

                    }
                    catch (Exception ex)
                    {
                        LogError("Error deleting old failed results folder", ex);
                    }

                }
            }

        }

        public bool FolderExistsWithRetry(string FolderPath)
        {
            const bool blnIncreaseHoldoffOnEachRetry = false;
            return FolderExistsWithRetry(FolderPath, DEFAULT_RETRY_COUNT, DEFAULT_RETRY_HOLDOFF_SEC, blnIncreaseHoldoffOnEachRetry);
        }

        public bool FolderExistsWithRetry(string FolderPath, int MaxRetryCount, int RetryHoldoffSeconds)
        {
            const bool blnIncreaseHoldoffOnEachRetry = false;
            return FolderExistsWithRetry(FolderPath, MaxRetryCount, RetryHoldoffSeconds, blnIncreaseHoldoffOnEachRetry);
        }

        public bool FolderExistsWithRetry(string FolderPath, int MaxRetryCount, int RetryHoldoffSeconds, bool blnIncreaseHoldoffOnEachRetry)
        {

            var AttemptCount = 0;
            var blnSuccess = false;
            var blnFolderExists = false;

            float sngRetryHoldoffSeconds = RetryHoldoffSeconds;

            if (sngRetryHoldoffSeconds < 1)
                sngRetryHoldoffSeconds = 1;
            if (MaxRetryCount < 1)
                MaxRetryCount = 1;

            while (AttemptCount <= MaxRetryCount & !blnSuccess)
            {
                AttemptCount += 1;

                try
                {
                    blnFolderExists = Directory.Exists(FolderPath);
                    blnSuccess = true;

                }
                catch (Exception ex)
                {
                    LogError("clsAnalysisResults: error looking for folder " + FolderPath, ex);

                    // Wait several seconds before retrying
                    Thread.Sleep((int)(Math.Floor(sngRetryHoldoffSeconds * 1000)));

                    PRISM.clsProgRunner.GarbageCollectNow();
                }

                if (!blnSuccess && blnIncreaseHoldoffOnEachRetry)
                {
                    sngRetryHoldoffSeconds *= 1.5f;
                }

            }

            if (!blnSuccess)
            {
                // Exception occurred; return False
                return false;
            }

            return blnFolderExists;

        }

        #endregion

    }

}