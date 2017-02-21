using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerICR2LSPlugIn
{
    public class clsAnalysisResourcesIcr2ls : clsAnalysisResources
    {
        #region "Methods"

        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Retrieve param file
            if (!FileSearch.RetrieveFile(m_jobParams.GetParam("ParmFileName"), m_jobParams.GetParam("ParmFileStoragePath")))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Look for an in-progress .PEK file in the transfer folder
            var eExistingPEKFileResult = RetrieveExistingTempPEKFile();

            if (eExistingPEKFileResult == CloseOutType.CLOSEOUT_FAILED)
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Call to RetrieveExistingTempPEKFile failed";
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Get input data file
            if (!FileSearch.RetrieveSpectra(m_jobParams.GetParam("RawDataType")))
            {
                LogDebug(
                    "clsAnalysisResourcesIcr2ls.GetResources: Error occurred retrieving spectra.");
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
            string strLocalDatasetFolderPath = null;

            bool blnIsFolder = false;

            try
            {
                string RawDataType = m_jobParams.GetParam("RawDataType");

                if (RawDataType == RAW_DATA_TYPE_DOT_RAW_FILES)
                {
                    // Thermo datasets do not have ser files
                    return true;
                }

                var strRemoteDatasetFolderPath = Path.Combine(m_jobParams.GetParam("DatasetArchivePath"), m_jobParams.GetParam("DatasetFolderName"));

                if (RawDataType.ToLower() == RAW_DATA_TYPE_BRUKER_FT_FOLDER)
                {
                    strLocalDatasetFolderPath = Path.Combine(m_WorkingDir, DatasetName + ".d");
                    strRemoteDatasetFolderPath = Path.Combine(strRemoteDatasetFolderPath, DatasetName + ".d");
                }
                else
                {
                    strLocalDatasetFolderPath = string.Copy(m_WorkingDir);
                }

                var serFileOrFolderPath = FindSerFileOrFolder(strLocalDatasetFolderPath, ref blnIsFolder);

                if (string.IsNullOrEmpty(serFileOrFolderPath))
                {
                    // Ser file, fid file, or 0.ser folder not found in the working directory
                    // See if the file exists in the archive

                    serFileOrFolderPath = FindSerFileOrFolder(strRemoteDatasetFolderPath, ref blnIsFolder);

                    if (!string.IsNullOrEmpty(serFileOrFolderPath))
                    {
                        // File found in the archive; need to copy it locally

                        DateTime dtStartTime = System.DateTime.UtcNow;

                        if (blnIsFolder)
                        {
                            var diSourceFolder = new DirectoryInfo(serFileOrFolderPath);

                            LogMessage(
                                "Copying 0.ser folder from archive to working directory: " + serFileOrFolderPath);
                            ResetTimestampForQueueWaitTimeLogging();
                            m_FileTools.CopyDirectory(serFileOrFolderPath, Path.Combine(strLocalDatasetFolderPath, diSourceFolder.Name));

                            if (m_DebugLevel >= 1)
                            {
                                LogMessage(
                                    "Successfully copied 0.ser folder in " + System.DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds.ToString("0") +
                                    " seconds");
                            }
                        }
                        else
                        {
                            var fiSourceFile = new FileInfo(serFileOrFolderPath);

                            LogMessage(
                                "Copying " + Path.GetFileName(serFileOrFolderPath) + " file from archive to working directory: " + serFileOrFolderPath);

                            if (!CopyFileToWorkDir(fiSourceFile.Name, fiSourceFile.Directory.FullName, strLocalDatasetFolderPath, clsLogTools.LogLevels.ERROR))
                            {
                                return false;
                            }
                            else
                            {
                                if (m_DebugLevel >= 1)
                                {
                                    LogMessage(
                                        "Successfully copied " + Path.GetFileName(serFileOrFolderPath) + " file in " +
                                        System.DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds.ToString("0") + " seconds");
                                }
                            }
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                m_message = "Exception in GetBrukerSerFile";
                LogError(m_message + ": " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Looks for a ser file, fid file, or 0.ser folder in strFolderToCheck
        /// </summary>
        /// <param name="strFolderToCheck"></param>
        /// <param name="blnIsFolder"></param>
        /// <returns>The path to the ser file, fid file, or 0.ser folder, if found.  An empty string if not found</returns>
        /// <remarks></remarks>
        public static string FindSerFileOrFolder(string strFolderToCheck, ref bool blnIsFolder)
        {
            blnIsFolder = false;

            // Look for a ser file in the working directory
            var serFileOrFolderPath = Path.Combine(strFolderToCheck, BRUKER_SER_FILE);

            if (File.Exists(serFileOrFolderPath))
            {
                // Ser file found
                return serFileOrFolderPath;
            }

            // Ser file not found; look for a fid file
            serFileOrFolderPath = Path.Combine(strFolderToCheck, BRUKER_FID_FILE);

            if (File.Exists(serFileOrFolderPath))
            {
                // Fid file found
                return serFileOrFolderPath;
            }

            // Fid file not found; look for a 0.ser folder in the working directory
            serFileOrFolderPath = Path.Combine(strFolderToCheck, BRUKER_ZERO_SER_FOLDER);
            if (Directory.Exists(serFileOrFolderPath))
            {
                blnIsFolder = true;
                return serFileOrFolderPath;
            }

            return string.Empty;
        }

        /// <summary>
        /// Look for file .pek.tmp in the transfer folder
        /// Retrieves the file if it is found
        /// </summary>
        /// <returns>
        /// CLOSEOUT_SUCCESS if an existing file was found and copied,
        /// CLOSEOUT_FILE_NOT_FOUND if an existing file was not found, and
        /// CLOSEOUT_FAILURE if an error
        /// </returns>
        /// <remarks>
        /// Does not validate that the ICR-2LS param file matches (in contrast, clsAnalysisResourcesSeq.vb does valid the param file).
        /// This is done on purpose to allow us to update the param file mid job.
        /// Scans already deisotoped will have used one parameter file; scans processed from this point forward
        /// will use a different one; this is OK and allows us to adjust the settings mid-job.
        /// To prevent this behavior, delete the .pek.tmp file from the transfer folder
        /// </remarks>
        private CloseOutType RetrieveExistingTempPEKFile()
        {
            try
            {
                var strJob = m_jobParams.GetParam("Job");
                var transferFolderPath = m_jobParams.GetParam("JobParameters", "transferFolderPath");

                if (string.IsNullOrWhiteSpace(transferFolderPath))
                {
                    // Transfer folder path is not defined
                    LogWarning(
                        "transferFolderPath is empty; this is unexpected");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    transferFolderPath = Path.Combine(transferFolderPath, m_jobParams.GetParam("JobParameters", "DatasetFolderName"));
                    transferFolderPath = Path.Combine(transferFolderPath, m_jobParams.GetParam("StepParameters", "OutputFolderName"));
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug(
                        "Checking for " + clsAnalysisToolRunnerICRBase.PEK_TEMP_FILE + " file at " + transferFolderPath);
                }

                var diSourceFolder = new DirectoryInfo(transferFolderPath);

                if (!diSourceFolder.Exists)
                {
                    // Transfer folder not found; return false
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug(
                            "  ... Transfer folder not found: " + diSourceFolder.FullName);
                    }
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                var pekTempFilePath = Path.Combine(diSourceFolder.FullName, DatasetName + clsAnalysisToolRunnerICRBase.PEK_TEMP_FILE);

                var fiTempPekFile = new FileInfo(pekTempFilePath);
                if (!fiTempPekFile.Exists)
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug(
                            "  ... " + clsAnalysisToolRunnerICRBase.PEK_TEMP_FILE + " file not found");
                    }
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (m_DebugLevel >= 1)
                {
                    LogDebug(
                        clsAnalysisToolRunnerICRBase.PEK_TEMP_FILE + " file found for job " + strJob + " (file size = " +
                        (fiTempPekFile.Length / 1024.0).ToString("#,##0") + " KB)");
                }

                // Copy fiTempPekFile locally
                try
                {
                    fiTempPekFile.CopyTo(Path.Combine(m_WorkingDir, fiTempPekFile.Name), true);

                    if (m_DebugLevel >= 1)
                    {
                        LogDebug(
                            "Copied " + fiTempPekFile.Name + " locally; will resume ICR-2LS analysis");
                    }

                    // If the job succeeds, we should delete the .pek.tmp file from the transfer folder
                    // Add the full path to m_ServerFilesToDelete using AddServerFileToDelete
                    m_jobParams.AddServerFileToDelete(fiTempPekFile.FullName);
                }
                catch (Exception ex)
                {
                    // Error copying the file; treat this as a failed job
                    m_message = " Exception copying " + clsAnalysisToolRunnerICRBase.PEK_TEMP_FILE + " file locally";
                    LogError(
                        "  ... Exception copying " + fiTempPekFile.FullName + " locally; unable to resume: " + ex.Message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                m_message = "Exception in RetrieveExistingTempPEKFile";
                LogError(m_message + ": " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        #endregion
    }
}