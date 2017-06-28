using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerICR2LSPlugIn
{
    public class clsAnalysisToolRunnerICR : clsAnalysisToolRunnerICRBase
    {
        // Performs PEK analysis using ICR-2LS on Bruker S-folder MS data

        // Example folder layout when processing S-folders
        //
        // C:\DMS_WorkDir1\   contains the .Par file
        // C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\   is empty
        // C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s001   contains 100 files (see below)
        // C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s002   contains another 100 files (see below)
        // C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s003
        // C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s004
        // C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s005
        // etc.
        //
        // Files in C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s001\
        // 110409_His.00001
        // 110409_His.00002
        // 110409_His.00003
        // ...
        // 110409_His.00099
        // 110409_His.00100
        //
        // Files in C:\DMS_WorkDir1\110409_His_Ctrl_052209_5ul_A40ACN\s002\
        // 110409_His.00101
        // 110409_His.00102
        // 110409_His.00103
        // etc.
        //
        //

        public override CloseOutType RunTool()
        {

            var currentTask = "Initializing";

            try
            {
                // Start with base class function to get settings information
                var ResCode = base.RunTool();
                if (ResCode != CloseOutType.CLOSEOUT_SUCCESS)
                    return ResCode;

                // Store the ICR2LS version info in the database
                currentTask = "StoreToolVersionInfo";

                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining ICR2LS version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Verify a param file has been specified
                currentTask = "Verify param file path";
                var paramFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"));

                currentTask = "Verify param file path: " + paramFilePath;
                if (!File.Exists(paramFilePath))
                {
                    // Param file wasn't specified, but is required for ICR-2LS analysis
                    m_message = "ICR-2LS Param file not found";
                    LogError(m_message + ": " + paramFilePath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Add handling of settings file info here if it becomes necessary in the future

                // Get scan settings from settings file
                var MinScan = m_jobParams.GetJobParameter("scanstart", 0);
                var MaxScan = m_jobParams.GetJobParameter("ScanStop", 0);

                // Determine whether or not we should be processing MS2 spectra
                var SkipMS2 = !m_jobParams.GetJobParameter("ProcessMS2", false);

                bool useAllScans;
                if ((MinScan == 0 && MaxScan == 0) || MinScan > MaxScan || MaxScan > 500000)
                {
                    useAllScans = true;
                }
                else
                {
                    useAllScans = false;
                }

                // Assemble the dataset name
                var DSNamePath = Path.Combine(m_WorkDir, m_Dataset);
                var RawDataType = m_jobParams.GetParam("RawDataType");

                // Assemble the output file name and path
                var OutFileNamePath = Path.Combine(m_WorkDir, m_Dataset + ".pek");

                // Determine the location of the ser file (or fid file)
                // It could be in a "0.ser" folder, a ser file inside a .D folder, or a fid file inside a .D folder

                string datasetFolderPathBase;
                bool blnBrukerFT;

                if (string.Equals(RawDataType.ToLower(), clsAnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER, StringComparison.InvariantCultureIgnoreCase))
                {
                    datasetFolderPathBase = Path.Combine(m_WorkDir, m_Dataset + ".d");
                    blnBrukerFT = true;
                }
                else
                {
                    datasetFolderPathBase = string.Copy(m_WorkDir);
                    blnBrukerFT = false;
                }

                // Look for a ser file or fid file in the working directory
                currentTask = "FindSerFileOrFolder";

                bool blnIsFolder;
                var serFileOrFolderPath = clsAnalysisResourcesIcr2ls.FindSerFileOrFolder(datasetFolderPathBase, out blnIsFolder);

                if (string.IsNullOrEmpty(serFileOrFolderPath))
                {
                    // Did not find a ser file, fid file, or 0.ser folder

                    if (blnBrukerFT)
                    {
                        m_message = "ser file or fid file not found; unable to process with ICR-2LS";
                        LogError(m_message);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                    else
                    {
                        // Assume we are processing zipped s-folders, and thus there should be a folder with the Dataset's name in the work directory
                        //  and in that folder will be unzipped contents of the s-folders (one file per spectrum)
                        if (m_DebugLevel >= 1)
                        {
                            LogDebug("Did not find a ser file, fid file, or 0.ser folder; assuming we are processing zipped s-folders");
                        }
                    }
                }
                else
                {
                    if (m_DebugLevel >= 1)
                    {
                        if (blnIsFolder)
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
                    string strSerTypeName;
                    if (!blnIsFolder)
                    {
                        eICR2LSMode = ICR2LSProcessingModeConstants.SerFilePEK;
                        strSerTypeName = "file";
                    }
                    else
                    {
                        eICR2LSMode = ICR2LSProcessingModeConstants.SerFolderPEK;
                        strSerTypeName = "folder";
                    }

                    currentTask = "StartICR2LS for " + serFileOrFolderPath;
                    success = base.StartICR2LS(
                        serFileOrFolderPath, paramFilePath, OutFileNamePath, eICR2LSMode, useAllScans,
                        SkipMS2, MinScan,MaxScan);

                    if (!success)
                    {
                        LogError("Error running ICR-2LS on " + strSerTypeName + " " + serFileOrFolderPath);
                    }
                }
                else
                {
                    // Processing zipped s-folders
                    if (!Directory.Exists(DSNamePath))
                    {
                        m_message = "Data file folder not found: " + DSNamePath;
                        LogError(m_message);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    currentTask = "StartICR2LS for zippsed s-folders in " + DSNamePath;

                    eICR2LSMode = ICR2LSProcessingModeConstants.SFoldersPEK;
                    success = base.StartICR2LS(DSNamePath, paramFilePath, OutFileNamePath, eICR2LSMode, useAllScans, SkipMS2, MinScan, MaxScan);

                    if (!success)
                    {
                        LogError("Error running ICR-2LS on zipped s-files in " + DSNamePath);
                    }
                }

                if (!success)
                {
                    // If a .PEK file exists, then call PerfPostAnalysisTasks() to move the .Pek file into the results folder, which we'll then archive in the Failed Results folder
                    currentTask = "VerifyPEKFileExists";
                    if (VerifyPEKFileExists(m_WorkDir, m_Dataset))
                    {
                        m_message = "ICR-2LS returned false (see .PEK file in Failed results folder)";
                        LogDebug(".Pek file was found, so will save results to the failed results archive folder");

                        PerfPostAnalysisTasks(false);

                        // Try to save whatever files were moved into the results folder
                        var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
                        objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName));
                    }
                    else
                    {
                        m_message = "Error running ICR-2LS (.Pek file not found in " + m_WorkDir + ")";
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run the cleanup routine from the base class
                currentTask = "PerfPostAnalysisTasks";

                if (PerfPostAnalysisTasks(true) != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Error performing post analysis tasks";
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
            // Deletes the dataset folder containing s-folders from the working directory
            var RetryCount = 0;
            var ErrMsg = string.Empty;

            while (RetryCount < 3)
            {
                try
                {
                    // Allow extra time for ICR2LS to release file locks
                    Thread.Sleep(5000);
                    if (Directory.Exists(Path.Combine(m_WorkDir, m_Dataset)))
                    {
                        Directory.Delete(Path.Combine(m_WorkDir, m_Dataset), true);
                    }
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }
                catch (IOException ex)
                {
                    // If problem is locked file, retry
                    if (m_DebugLevel > 0)
                    {
                        LogError("Error deleting data file, attempt #" + RetryCount);
                    }
                    ErrMsg = ex.Message;
                    RetryCount += 1;
                }
                catch (Exception ex)
                {
                    LogError("Error deleting raw data files, job " + m_JobNum + ": " + ex.Message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            // If we got to here, then we've exceeded the max retry limit
            LogError("Unable to delete raw data file after multiple tries: " + ErrMsg);
            return CloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Look for the .PEK and .PAR files in the specified folder
        /// Make sure they are named Dataset_m_dd_yyyy.PAR and Dataset_m_dd_yyyy.Pek
        /// </summary>
        /// <param name="strFolderPath">Folder to examine</param>
        /// <param name="strDatasetName">Dataset name</param>
        /// <remarks></remarks>
        private void FixICR2LSResultFileNames(string strFolderPath, string strDatasetName)
        {
            var objExtensionsToCheck = new List<string>();

            try
            {
                objExtensionsToCheck.Add("PAR");
                objExtensionsToCheck.Add("Pek");

                var fiFolder = new DirectoryInfo(strFolderPath);

                if (fiFolder.Exists)
                {
                    foreach (var strExtension in objExtensionsToCheck)
                    {
                        foreach (var fiFile in fiFolder.GetFiles("*." + strExtension))
                        {
                            if (!fiFile.Name.StartsWith(strDatasetName, StringComparison.InvariantCultureIgnoreCase))
                                continue;

                            // Name should be of the form: Dataset_1_24_2010.PAR
                            // The datestamp in the name is month_day_year
                            var strDesiredName = strDatasetName + "_" + System.DateTime.Now.ToString("M_d_yyyy") + "." + strExtension;

                            if (!string.Equals(fiFile.Name, strDesiredName, StringComparison.InvariantCultureIgnoreCase))
                            {
                                try
                                {
                                    if (m_DebugLevel >= 1)
                                    {
                                        LogDebug("Renaming " + strExtension + " file from " + fiFile.Name + " to " + strDesiredName);
                                    }

                                    fiFile.MoveTo(Path.Combine(fiFolder.FullName, strDesiredName));
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
                else
                {
                    LogError("Error in FixICR2LSResultFileNames; folder not found: " + strFolderPath);
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in FixICR2LSResultFileNames: " + ex.Message);
            }
        }
    }
}