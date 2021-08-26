using PRISM.Logging;
using System;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerICR2LSPlugIn
{
    /// <summary>
    /// Retrieve resources for the ICR-2LS plugin
    /// </summary>
    public class AnalysisResourcesIcr2ls : AnalysisResources
    {
        // Ignore Spelling: deisotoped, fid, ParmFile, pek, ser, SerFile

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Retrieve param file
            if (!FileSearchTool.RetrieveFile(mJobParams.GetParam("ParmFileName"), mJobParams.GetParam("ParmFileStoragePath")))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Look for an in-progress .PEK file in the transfer folder
            var eExistingPEKFileResult = RetrieveExistingTempPEKFile();

            if (eExistingPEKFileResult == CloseOutType.CLOSEOUT_FAILED)
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Call to RetrieveExistingTempPEKFile failed";
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Get input data file
            if (!FileSearchTool.RetrieveSpectra(mJobParams.GetParam("RawDataType")))
            {
                LogDebug("AnalysisResourcesIcr2ls.GetResources: Error occurred retrieving spectra.");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // NOTE: GetBrukerSerFile is not MyEMSL-compatible
            if (!GetBrukerSerFile())
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool GetBrukerSerFile()
        {
            try
            {
                var rawDataTypeName = mJobParams.GetParam("RawDataType");

                if (rawDataTypeName == RAW_DATA_TYPE_DOT_RAW_FILES)
                {
                    // Thermo datasets do not have ser files
                    return true;
                }

                var remoteDatasetFolderPath = Path.Combine(mJobParams.GetParam("DatasetArchivePath"), mJobParams.GetParam(JOB_PARAM_DATASET_FOLDER_NAME));

                string localDatasetFolderPath;
                if (rawDataTypeName.ToLower() == RAW_DATA_TYPE_BRUKER_FT_FOLDER)
                {
                    localDatasetFolderPath = Path.Combine(mWorkDir, DatasetName + ".d");
                    remoteDatasetFolderPath = Path.Combine(remoteDatasetFolderPath, DatasetName + ".d");
                }
                else
                {
                    localDatasetFolderPath = mWorkDir;
                }

                var serFileOrFolderPath = FindSerFileOrFolder(localDatasetFolderPath, out var isFolder);

                if (string.IsNullOrEmpty(serFileOrFolderPath))
                {
                    // Ser file, fid file, or 0.ser folder not found in the working directory
                    // See if the file exists in the archive

                    serFileOrFolderPath = FindSerFileOrFolder(remoteDatasetFolderPath, out isFolder);

                    if (!string.IsNullOrEmpty(serFileOrFolderPath))
                    {
                        // File found in the archive; need to copy it locally

                        var startTime = DateTime.UtcNow;

                        if (isFolder)
                        {
                            var sourceFolder = new DirectoryInfo(serFileOrFolderPath);

                            LogMessage("Copying 0.ser folder from archive to working directory: " + serFileOrFolderPath);
                            ResetTimestampForQueueWaitTimeLogging();
                            mFileTools.CopyDirectory(serFileOrFolderPath, Path.Combine(localDatasetFolderPath, sourceFolder.Name));

                            if (mDebugLevel >= 1)
                            {
                                LogMessage(
                                    "Successfully copied 0.ser folder in " + DateTime.UtcNow.Subtract(startTime).TotalSeconds.ToString("0") +
                                    " seconds");
                            }
                        }
                        else
                        {
                            var sourceFile = new FileInfo(serFileOrFolderPath);

                            LogMessage("Copying " + Path.GetFileName(serFileOrFolderPath) + " file from archive to working directory: " + serFileOrFolderPath);

                            if (!CopyFileToWorkDir(sourceFile.Name, sourceFile.Directory.FullName, localDatasetFolderPath, BaseLogger.LogLevels.ERROR))
                            {
                                return false;
                            }

                            if (mDebugLevel >= 1)
                            {
                                LogMessage(
                                    "Successfully copied " + Path.GetFileName(serFileOrFolderPath) + " file in " +
                                    DateTime.UtcNow.Subtract(startTime).TotalSeconds.ToString("0") + " seconds");
                            }
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in GetBrukerSerFile";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Looks for a ser file, fid file, or 0.ser folder in folderToCheck
        /// </summary>
        /// <param name="folderToCheck"></param>
        /// <param name="isFolder"></param>
        /// <returns>The path to the ser file, fid file, or 0.ser folder, if found.  An empty string if not found</returns>
        public static string FindSerFileOrFolder(string folderToCheck, out bool isFolder)
        {
            isFolder = false;

            // Look for a ser file in the working directory
            var serFileOrFolderPath = Path.Combine(folderToCheck, BRUKER_SER_FILE);

            if (File.Exists(serFileOrFolderPath))
            {
                // Ser file found
                return serFileOrFolderPath;
            }

            // Ser file not found; look for a fid file
            serFileOrFolderPath = Path.Combine(folderToCheck, BRUKER_FID_FILE);

            if (File.Exists(serFileOrFolderPath))
            {
                // Fid file found
                return serFileOrFolderPath;
            }

            // Fid file not found; look for a 0.ser folder in the working directory
            serFileOrFolderPath = Path.Combine(folderToCheck, BRUKER_ZERO_SER_FOLDER);
            if (Directory.Exists(serFileOrFolderPath))
            {
                isFolder = true;
                return serFileOrFolderPath;
            }

            return string.Empty;
        }

        /// <summary>
        /// Look for file .pek.tmp in the transfer folder
        /// Retrieves the file if it is found
        /// </summary>
        /// <remarks>
        /// Does not validate that the ICR-2LS param file matches (in contrast, AnalysisResourcesSeq.vb does valid the param file).
        /// This is done on purpose to allow us to update the param file mid job.
        /// Scans already deisotoped will have used one parameter file; scans processed from this point forward
        /// will use a different one; this is OK and allows us to adjust the settings mid-job.
        /// To prevent this behavior, delete the .pek.tmp file from the transfer folder
        /// </remarks>
        /// <returns>
        /// CLOSEOUT_SUCCESS if an existing file was found and copied,
        /// CLOSEOUT_FILE_NOT_FOUND if an existing file was not found, and
        /// CLOSEOUT_FAILURE if an error
        /// </returns>
        private CloseOutType RetrieveExistingTempPEKFile()
        {
            try
            {
                var job = mJobParams.GetParam("Job");
                var transferDirectoryPath = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_TRANSFER_DIRECTORY_PATH);

                if (string.IsNullOrWhiteSpace(transferDirectoryPath))
                {
                    // Transfer folder path is not defined
                    LogWarning("transferDirectoryPath is empty; this is unexpected");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                transferDirectoryPath = Path.Combine(transferDirectoryPath, mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_DATASET_FOLDER_NAME));
                transferDirectoryPath = Path.Combine(transferDirectoryPath, mJobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, JOB_PARAM_OUTPUT_FOLDER_NAME));

                if (mDebugLevel >= 4)
                {
                    LogDebug("Checking for " + AnalysisToolRunnerICRBase.PEK_TEMP_FILE + " file at " + transferDirectoryPath);
                }

                var sourceFolder = new DirectoryInfo(transferDirectoryPath);

                if (!sourceFolder.Exists)
                {
                    // Transfer folder not found; return false
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("  ... Transfer folder not found: " + sourceFolder.FullName);
                    }
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                var pekTempFilePath = Path.Combine(sourceFolder.FullName, DatasetName + AnalysisToolRunnerICRBase.PEK_TEMP_FILE);

                var tempPekFile = new FileInfo(pekTempFilePath);
                if (!tempPekFile.Exists)
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("  ... " + AnalysisToolRunnerICRBase.PEK_TEMP_FILE + " file not found");
                    }
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (mDebugLevel >= 1)
                {
                    LogDebug(
                        AnalysisToolRunnerICRBase.PEK_TEMP_FILE + " file found for job " + job + " (file size = " +
                        (tempPekFile.Length / 1024.0).ToString("#,##0") + " KB)");
                }

                // Copy the pek file locally
                try
                {
                    tempPekFile.CopyTo(Path.Combine(mWorkDir, tempPekFile.Name), true);

                    if (mDebugLevel >= 1)
                    {
                        LogDebug("Copied " + tempPekFile.Name + " locally; will resume ICR-2LS analysis");
                    }

                    // If the job succeeds, we should delete the .pek.tmp file from the transfer folder
                    // Add the full path to ServerFilesToDelete using AddServerFileToDelete
                    mJobParams.AddServerFileToDelete(tempPekFile.FullName);
                }
                catch (Exception ex)
                {
                    // Error copying the file; treat this as a failed job
                    mMessage = " Exception copying " + AnalysisToolRunnerICRBase.PEK_TEMP_FILE + " file locally";
                    LogError("  ... Exception copying " + tempPekFile.FullName + " locally; unable to resume: " + ex.Message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrieveExistingTempPEKFile";
                LogError(mMessage + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}