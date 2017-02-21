// This class was created to support being loaded as a pluggable DLL into the New DMS
// Analysis Tool Manager program.  Each DLL requires a Resource class.  The new ATM
// supports the mini-pipeline. It uses class MsMsSpectrumFilter.DLL to filter the .DTA
// files present in a given folder
//
// Written by John Sandoval for the Department of Energy (PNNL, Richland, WA)
// Copyright 2009, Battelle Memorial Institute
// Started January 20, 2009

using System;
using System.Collections.Generic;

using System.IO;
using System.Linq;
using AnalysisManagerBase;
using MSMSSpectrumFilter;
using MyEMSLReader;

namespace MSMSSpectrumFilterAM
{
    public class clsAnalysisResourcesMsMsSpectrumFilter : clsAnalysisResources
    {
        #region "Methods"

        /// <summary>
        /// Retrieves files necessary for performance of Sequest analysis
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
            if (!FileSearch.RetrieveDtaFiles())
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Add the _dta.txt file to the list of extensions to delete after the tool finishes
            m_jobParams.AddResultFileExtensionToSkip(DatasetName + "_dta.txt");
            //Unzipped, concatenated DTA

            // Add the _Dta.zip file to the list of files to move to the results folder
            // Note that this .Zip file will contain the filtered _Dta.txt file (not the original _Dta.txt file)
            m_jobParams.AddResultFileToKeep("_dta.zip");
            //Zipped DTA

            // Look at the job parameters
            // If ScanTypeFilter is defined, or MSCollisionModeFilter is defined, or MSLevelFilter is defined, then we need either of the following
            //  a) The _ScanStats.txt file and _ScanStatsEx.txt file from a MASIC job for this dataset
            //       This is essentially a job-depending-on a job
            //  b) The .Raw file
            //

            var strMSLevelFilter = m_jobParams.GetJobParameter("MSLevelFilter", "0");

            var strScanTypeFilter = m_jobParams.GetJobParameter("ScanTypeFilter", "");
            var strScanTypeMatchType = m_jobParams.GetJobParameter("ScanTypeMatchType", clsMsMsSpectrumFilter.TEXT_MATCH_TYPE_CONTAINS);

            var strMSCollisionModeFilter = m_jobParams.GetJobParameter("MSCollisionModeFilter", "");
            var strMSCollisionModeMatchType = m_jobParams.GetJobParameter("MSCollisionModeMatchType", clsMsMsSpectrumFilter.TEXT_MATCH_TYPE_CONTAINS);

            var blnNeedScanStatsFiles = false;

            if ((strMSLevelFilter != null) && strMSLevelFilter.Length > 0 && strMSLevelFilter != "0")
            {
                if (m_DebugLevel >= 1)
                {
                    LogDebug("GetResources: MSLevelFilter is defined (" + strMSLevelFilter + "); will retrieve or generate the ScanStats files");
                }
                blnNeedScanStatsFiles = true;
            }

            if ((strScanTypeFilter != null) && strScanTypeFilter.Length > 0)
            {
                if (m_DebugLevel >= 1)
                {
                    LogDebug("GetResources: ScanTypeFilter is defined (" + strScanTypeFilter + " with match type " + strScanTypeMatchType + "); will retrieve or generate the ScanStats files");
                }
                blnNeedScanStatsFiles = true;
            }

            if ((strMSCollisionModeFilter != null) && strMSCollisionModeFilter.Length > 0)
            {
                if (m_DebugLevel >= 1)
                {
                    LogDebug("GetResources: MSCollisionModeFilter is defined (" + strMSCollisionModeFilter + " with match type " + strMSCollisionModeMatchType + "); will retrieve or generate the ScanStats files");
                }
                blnNeedScanStatsFiles = true;
            }

            if (blnNeedScanStatsFiles)
            {
                // Find and copy the ScanStats files from an existing job rather than copying over the .Raw file
                // However, if the _ScanStats.txt file does not have column ScanTypeName, then we will need the .raw file

                var blnIsFolder = false;
                string strDatasetFileOrFolderPath = null;
                var blnScanStatsFilesRetrieved = false;

                strDatasetFileOrFolderPath = FolderSearch.FindDatasetFileOrFolder(out blnIsFolder, assumeUnpurged: false);

                if (!string.IsNullOrEmpty(strDatasetFileOrFolderPath) & !strDatasetFileOrFolderPath.StartsWith(MYEMSL_PATH_FLAG))
                {
                    DirectoryInfo diDatasetFolder;
                    if (blnIsFolder)
                    {
                        diDatasetFolder = new DirectoryInfo(strDatasetFileOrFolderPath);
                        diDatasetFolder = diDatasetFolder.Parent;
                    }
                    else
                    {
                        var fiDatasetFile = new FileInfo(strDatasetFileOrFolderPath);
                        diDatasetFolder = fiDatasetFile.Directory;
                    }

                    if (FindExistingScanStatsFile(diDatasetFolder.FullName))
                    {
                        blnScanStatsFilesRetrieved = true;
                    }
                }

                if (!blnScanStatsFilesRetrieved)
                {
                    // Find the dataset file and either create a StoragePathInfo file or copy it locally

                    var CreateStoragePathInfoOnly = false;
                    string RawDataType = m_jobParams.GetParam("RawDataType");

                    switch (RawDataType.ToLower())
                    {
                        case RAW_DATA_TYPE_DOT_RAW_FILES:
                        case RAW_DATA_TYPE_DOT_WIFF_FILES:
                        case RAW_DATA_TYPE_DOT_UIMF_FILES:
                        case RAW_DATA_TYPE_DOT_MZXML_FILES:
                            // Don't actually copy the .Raw (or .wiff, .uimf, etc.) file locally; instead,
                            //  determine where it is located then create a text file named "DatesetName.raw_StoragePathInfo.txt"
                            //  This new file contains just one line of text: the full path to the actual file
                            CreateStoragePathInfoOnly = true;
                            break;
                        default:
                            CreateStoragePathInfoOnly = false;
                            break;
                    }

                    if (!FileSearch.RetrieveSpectra(RawDataType, CreateStoragePathInfoOnly))
                    {
                        LogDebug("clsAnalysisResourcesMsMsSpectrumFilter.GetResources: Error occurred retrieving spectra.");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                // Add additional extensions to delete after the tool finishes
                m_jobParams.AddResultFileExtensionToSkip("_ScanStats.txt");
                m_jobParams.AddResultFileExtensionToSkip("_ScanStatsEx.txt");
                m_jobParams.AddResultFileExtensionToSkip("_StoragePathInfo.txt");

                m_jobParams.AddResultFileExtensionToSkip(DOT_WIFF_EXTENSION);
                m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                m_jobParams.AddResultFileExtensionToSkip(DOT_UIMF_EXTENSION);
                m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);

                m_jobParams.AddResultFileExtensionToSkip(DOT_MGF_EXTENSION);
                m_jobParams.AddResultFileExtensionToSkip(DOT_CDF_EXTENSION);
            }

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool FindExistingScanStatsFile(string strDatasetFolderPath)
        {
            var diDatasetFolder = new DirectoryInfo(strDatasetFolderPath);
            var blnFilesFound = false;

            try
            {
                if (!diDatasetFolder.Exists)
                {
                    LogError("Dataset folder not found: " + strDatasetFolderPath);
                }

                var lstFiles = diDatasetFolder.GetFiles(DatasetName + "_ScanStats.txt", SearchOption.AllDirectories).ToList();

                if (lstFiles.Count == 0)
                {
                    if (m_DebugLevel >= 2)
                    {
                        LogWarning("No _ScanStats.txt files were found in subfolders below " + strDatasetFolderPath);
                    }
                    return false;
                }

                // Find the newest file in lstFiles
                List<FileInfo> lstSortedFiles = (from item in lstFiles orderby item.LastWriteTime descending select item).ToList();

                FileInfo fiNewestScanStatsFile = lstSortedFiles[0];

                // Copy the ScanStats file locally
                fiNewestScanStatsFile.CopyTo(Path.Combine(m_WorkingDir, fiNewestScanStatsFile.Name));

                // Read the first line of the file and confirm that the _ScanTypeName column exists
                using (var srScanStatsFile = new StreamReader(new FileStream(fiNewestScanStatsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    string strLineIn = null;
                    strLineIn = srScanStatsFile.ReadLine();

                    if (!strLineIn.Contains(clsMsMsSpectrumFilter.SCANSTATS_COL_SCAN_TYPE_NAME))
                    {
                        if (m_DebugLevel >= 1)
                        {
                            LogMessage("The newest _ScanStats.txt file for this dataset does not contain column " + clsMsMsSpectrumFilter.SCANSTATS_COL_SCAN_TYPE_NAME + "; will need to re-generate the file using the .Raw file");
                        }
                        return false;
                    }
                }

                // Look for the _ScanStatsEx.txt file
                string strScanStatsExPath = Path.Combine(fiNewestScanStatsFile.Directory.FullName, Path.GetFileNameWithoutExtension(fiNewestScanStatsFile.Name) + "Ex.txt");

                if (File.Exists(strScanStatsExPath))
                {
                    // Copy it locally
                    File.Copy(strScanStatsExPath, Path.Combine(m_WorkingDir, Path.GetFileName(strScanStatsExPath)));

                    if (m_DebugLevel >= 1)
                    {
                        LogMessage("Using existing _ScanStats.txt from " + fiNewestScanStatsFile.FullName);
                    }

                    blnFilesFound = true;
                }
                else
                {
                    if (m_DebugLevel >= 1)
                    {
                        LogWarning("The _ScanStats.txt file was found at " + fiNewestScanStatsFile.FullName + " but the _ScanStatsEx.txt file was not present");
                    }
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in FindExistingScanStatsFile", ex);
                return false;
            }

            return blnFilesFound;
        }

        #endregion
    }
}
