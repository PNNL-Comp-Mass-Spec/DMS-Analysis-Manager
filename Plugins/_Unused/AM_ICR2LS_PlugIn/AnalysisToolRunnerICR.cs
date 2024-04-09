using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerICR2LSPlugIn
{
    /// <summary>
    /// Performs deisotoping using ICR-2LS on Bruker S-folder MS data
    /// </summary>
    public class AnalysisToolRunnerICR : AnalysisToolRunnerICRBase
    {
        // Ignore Spelling: Bruker, deisotoping, fid, pek, ser

        // Example folder layout when processing S-folders

        // C:\DMS_WorkDir1\   contains the .Par file
        // C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\   is empty
        // C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s001   contains 100 files (see below)
        // C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s002   contains another 100 files (see below)
        // C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s003
        // C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s004
        // C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s005
        // etc.

        // Files in C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s001\
        // 110409_His.00001
        // 110409_His.00002
        // 110409_His.00003
        // ...
        // 110409_His.00099
        // 110409_His.00100

        // Files in C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s002\
        // 110409_His.00101
        // 110409_His.00102
        // 110409_His.00103
        // etc.

        /// <summary>
        /// Primary entry point for running this tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
        public override CloseOutType RunTool()
        {
            var currentTask = "Initializing";

            try
            {
                // Start with base class method to get settings information
                var resultCode = base.RunTool();

                if (resultCode != CloseOutType.CLOSEOUT_SUCCESS)
                    return resultCode;

                // Store the ICR2LS version info in the database
                currentTask = "StoreToolVersionInfo";

                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining ICR2LS version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Verify a param file has been specified
                currentTask = "Verify param file path";
                var paramFilePath = Path.Combine(mWorkDir, mJobParams.GetParam("parmFileName"));

                currentTask = "Verify param file path: " + paramFilePath;

                if (!File.Exists(paramFilePath))
                {
                    // Param file wasn't specified, but is required for ICR-2LS analysis
                    mMessage = "ICR-2LS Param file not found";
                    LogError(mMessage + ": " + paramFilePath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Add handling of settings file info here if it becomes necessary in the future

                // Get scan settings from settings file
                var minScan = mJobParams.GetJobParameter("ScanStart", 0);
                var maxScan = mJobParams.GetJobParameter("ScanStop", 0);

                // Determine whether or not we should be processing MS2 spectra
                var skipMS2 = !mJobParams.GetJobParameter("ProcessMS2", false);

                // ReSharper disable once ArrangeRedundantParentheses
                var useAllScans = (minScan == 0 && maxScan == 0) || minScan > maxScan || maxScan > 500000;

                // Assemble the dataset path
                var datasetDirectoryPath = Path.Combine(mWorkDir, mDatasetName);
                var rawDataTypeName = mJobParams.GetParam("RawDataType");

                // Assemble the output file name and path
                var outFileNamePath = Path.Combine(mWorkDir, mDatasetName + ".pek");

                // Determine the location of the ser file (or fid file)
                // It could be in a "0.ser" folder, a ser file inside a .D folder, or a fid file inside a .D folder

                string datasetFolderPathBase;
                bool brukerFT;

                if (string.Equals(rawDataTypeName.ToLower(), AnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER, StringComparison.InvariantCultureIgnoreCase))
                {
                    datasetFolderPathBase = Path.Combine(mWorkDir, mDatasetName + ".d");
                    brukerFT = true;
                }
                else
                {
                    datasetFolderPathBase = mWorkDir;
                    brukerFT = false;
                }

                // Look for a ser file or fid file in the working directory
                currentTask = "FindSerFileOrFolder";

                var serFileOrFolderPath = AnalysisResourcesIcr2ls.FindSerFileOrFolder(datasetFolderPathBase, out var isFolder);

                if (string.IsNullOrEmpty(serFileOrFolderPath))
                {
                    // Did not find a ser file, fid file, or 0.ser folder

                    if (brukerFT)
                    {
                        mMessage = "ser file or fid file not found; unable to process with ICR-2LS";
                        LogError(mMessage);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    // Assume we are processing zipped s-folders, and thus there should be a folder with the Dataset's name in the work directory
                    //  and in that folder will be unzipped contents of the s-folders (one file per spectrum)
                    if (mDebugLevel >= 1)
                    {
                        LogDebug("Did not find a ser file, fid file, or 0.ser folder; assuming we are processing zipped s-folders");
                    }
                }
                else
                {
                    if (mDebugLevel >= 1)
                    {
                        if (isFolder)
                        {
                            LogDebug("0.ser folder found: " + serFileOrFolderPath);
                        }
                        else
                        {
                            LogDebug(
                                Path.GetFileName(serFileOrFolderPath) + " file found: " + serFileOrFolderPath);
                        }
                    }
                }

                ICR2LSProcessingModeConstants eICR2LSMode;
                bool success;

                if (!string.IsNullOrEmpty(serFileOrFolderPath))
                {
                    string serTypeName;

                    if (!isFolder)
                    {
                        eICR2LSMode = ICR2LSProcessingModeConstants.SerFilePEK;
                        serTypeName = "file";
                    }
                    else
                    {
                        eICR2LSMode = ICR2LSProcessingModeConstants.SerFolderPEK;
                        serTypeName = "folder";
                    }

                    currentTask = "StartICR2LS for " + serFileOrFolderPath;
                    success = StartICR2LS(
                        serFileOrFolderPath, paramFilePath, outFileNamePath, eICR2LSMode, useAllScans,
                        skipMS2, minScan, maxScan);

                    if (!success)
                    {
                        LogError("Error running ICR-2LS on " + serTypeName + " " + serFileOrFolderPath);
                    }
                }
                else
                {
                    // Processing zipped s-folders
                    if (!Directory.Exists(datasetDirectoryPath))
                    {
                        mMessage = "Data file folder not found: " + datasetDirectoryPath;
                        LogError(mMessage);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    currentTask = "StartICR2LS for zipped s-folders in " + datasetDirectoryPath;

                    eICR2LSMode = ICR2LSProcessingModeConstants.SFoldersPEK;
                    success = StartICR2LS(datasetDirectoryPath, paramFilePath, outFileNamePath, eICR2LSMode, useAllScans, skipMS2, minScan, maxScan);

                    if (!success)
                    {
                        LogError("Error running ICR-2LS on zipped s-files in " + datasetDirectoryPath);
                    }
                }

                if (!success)
                {
                    // If a .PEK file exists, call PerfPostAnalysisTasks() to move the .Pek file into the results folder, which we'll then archive in the Failed Results folder
                    currentTask = "VerifyPEKFileExists";

                    if (VerifyPEKFileExists(mWorkDir, mDatasetName))
                    {
                        mMessage = "ICR-2LS returned false (see .PEK file in Failed results folder)";
                        LogDebug(".Pek file was found, so will save results to the failed results archive folder");

                        PerfPostAnalysisTasks(false);

                        // Try to save whatever files were moved into the results directory
                        var analysisResults = new AnalysisResults(mMgrParams, mJobParams);
                        analysisResults.CopyFailedResultsToArchiveDirectory(Path.Combine(mWorkDir, mResultsDirectoryName));
                    }
                    else
                    {
                        mMessage = "Error running ICR-2LS (.Pek file not found in " + mWorkDir + ")";
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run the cleanup routine from the base class
                currentTask = "PerfPostAnalysisTasks";

                if (PerfPostAnalysisTasks(true) != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Error performing post analysis tasks";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error running ICR-2LS, current task " + currentTask, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        protected override CloseOutType DeleteDataFile()
        {
            // Deletes the dataset directory containing s-folders from the working directory
            var retryCount = 0;
            var errorMsg = string.Empty;

            while (retryCount < 3)
            {
                try
                {
                    // Allow extra time for ICR2LS to release file locks
                    Global.IdleLoop(5);

                    if (Directory.Exists(Path.Combine(mWorkDir, mDatasetName)))
                    {
                        Directory.Delete(Path.Combine(mWorkDir, mDatasetName), true);
                    }
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }
                catch (IOException ex)
                {
                    // If problem is locked file, retry
                    if (mDebugLevel > 0)
                    {
                        LogError("Error deleting data file, attempt #" + retryCount);
                    }
                    errorMsg = ex.Message;
                    retryCount++;
                }
                catch (Exception ex)
                {
                    LogError("Error deleting raw data files, job " + mJob + ": " + ex.Message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            // If we got to here, we've exceeded the max retry limit
            LogError("Unable to delete raw data file after multiple tries: " + errorMsg);
            return CloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Look for the .PEK and .PAR files in the specified directory
        /// Make sure they are named Dataset_m_dd_yyyy.PAR and Dataset_m_dd_yyyy.Pek
        /// </summary>
        /// <param name="directoryPath">Folder to examine</param>
        /// <param name="datasetName">Dataset name</param>
        [Obsolete("Unused")]
        private void FixICR2LSResultFileNames(string directoryPath, string datasetName)
        {
            var extensionsToCheck = new List<string>();

            try
            {
                extensionsToCheck.Add("PAR");
                extensionsToCheck.Add("Pek");

                var directory = new DirectoryInfo(directoryPath);

                if (!directory.Exists)
                {
                    LogError("Error in FixICR2LSResultFileNames; directory not found: " + directoryPath);
                    return;
                }

                foreach (var extension in extensionsToCheck)
                {
                    foreach (var file in directory.GetFiles("*." + extension))
                    {
                        if (!file.Name.StartsWith(datasetName, StringComparison.OrdinalIgnoreCase))
                            continue;

                        // Name should be of the form: Dataset_1_24_2010.PAR
                        // The date stamp in the name is month_day_year
                        var newName = datasetName + "_" + DateTime.Now.ToString("M_d_yyyy") + "." + extension;

                        if (!string.Equals(file.Name, newName, StringComparison.OrdinalIgnoreCase))
                        {
                            try
                            {
                                if (mDebugLevel >= 1)
                                {
                                    LogDebug("Renaming " + extension + " file from " + file.Name + " to " + newName);
                                }

                                file.MoveTo(Path.Combine(directory.FullName, newName));
                            }
                            catch (Exception ex)
                            {
                                // Rename failed; that means the correct file already exists; this is OK
                                LogError("Rename failed: " + ex.Message);
                            }
                        }

                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in FixICR2LSResultFileNames: " + ex.Message);
            }
        }
    }
}