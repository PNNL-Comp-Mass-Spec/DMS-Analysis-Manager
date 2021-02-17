// This class was created to support being loaded as a pluggable DLL into the New DMS
// Analysis Tool Manager program.  Each DLL requires a Resource class.  The new ATM
// supports the mini-pipeline. It uses class MsMsSpectrumFilter.DLL to filter the .DTA
// files present in a given folder
//
// Written by John Sandoval for the Department of Energy (PNNL, Richland, WA)
// Copyright 2009, Battelle Memorial Institute
// Started January 20, 2009

using AnalysisManagerBase;
using MSMSSpectrumFilter;
using MyEMSLReader;
using System;
using System.IO;
using System.Linq;

namespace MSMSSpectrumFilterAM
{
    /// <summary>
    /// Retrieve resources for the MsMs Spectrum Filter plugin
    /// </summary>
    public class clsAnalysisResourcesMsMsSpectrumFilter : clsAnalysisResources
    {
        // Ignore Spelling: Dta

        #region "Methods"

        /// <summary>
        /// Retrieves files necessary for performance of Sequest analysis
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
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
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Add the _dta.txt file to the list of extensions to delete after the tool finishes
            // This is the unzipped, concatenated DTA
            mJobParams.AddResultFileExtensionToSkip(DatasetName + "_dta.txt");


            // Add the _Dta.zip file to the list of files to move to the results folder
            // Note that this .Zip file will contain the filtered _Dta.txt file (not the original _Dta.txt file)
            // Zipped _dta.txt file
            mJobParams.AddResultFileToKeep("_dta.zip");

            // Look at the job parameters
            // If ScanTypeFilter is defined, or MSCollisionModeFilter is defined, or MSLevelFilter is defined, we need either of the following
            //  a) The _ScanStats.txt file and _ScanStatsEx.txt file from a MASIC job for this dataset
            //       This is essentially a job-depending-on a job
            //  b) The .Raw file

            var strMSLevelFilter = mJobParams.GetJobParameter("MSLevelFilter", "0");

            var strScanTypeFilter = mJobParams.GetJobParameter("ScanTypeFilter", "");
            var strScanTypeMatchType = mJobParams.GetJobParameter("ScanTypeMatchType", clsMsMsSpectrumFilter.TEXT_MATCH_TYPE_CONTAINS);

            var strMSCollisionModeFilter = mJobParams.GetJobParameter("MSCollisionModeFilter", "");
            var strMSCollisionModeMatchType = mJobParams.GetJobParameter("MSCollisionModeMatchType", clsMsMsSpectrumFilter.TEXT_MATCH_TYPE_CONTAINS);

            var blnNeedScanStatsFiles = false;

            if (!string.IsNullOrEmpty(strMSLevelFilter) && strMSLevelFilter != "0")
            {
                if (mDebugLevel >= 1)
                {
                    LogDebug("GetResources: MSLevelFilter is defined (" + strMSLevelFilter + "); will retrieve or generate the ScanStats files");
                }
                blnNeedScanStatsFiles = true;
            }

            if (!string.IsNullOrEmpty(strScanTypeFilter))
            {
                if (mDebugLevel >= 1)
                {
                    LogDebug("GetResources: ScanTypeFilter is defined (" + strScanTypeFilter + " with match type " + strScanTypeMatchType + "); will retrieve or generate the ScanStats files");
                }
                blnNeedScanStatsFiles = true;
            }

            if (!string.IsNullOrEmpty(strMSCollisionModeFilter))
            {
                if (mDebugLevel >= 1)
                {
                    LogDebug("GetResources: MSCollisionModeFilter is defined (" + strMSCollisionModeFilter +
                        " with match type " + strMSCollisionModeMatchType + "); will retrieve or generate the ScanStats files");
                }
                blnNeedScanStatsFiles = true;
            }

            if (blnNeedScanStatsFiles)
            {
                // Find and copy the ScanStats files from an existing job rather than copying over the .Raw file
                // However, if the _ScanStats.txt file does not have column ScanTypeName, we will need the .raw file

                var blnScanStatsFilesRetrieved = false;

                var strDatasetFileOrFolderPath = DirectorySearch.FindDatasetFileOrDirectory(out var blnIsFolder, assumeUnpurged: false);

                if (!string.IsNullOrEmpty(strDatasetFileOrFolderPath) && !strDatasetFileOrFolderPath.StartsWith(MYEMSL_PATH_FLAG))
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

                    var createStoragePathInfoOnly = false;
                    var rawDataTypeName = mJobParams.GetParam("RawDataType");

                    switch (rawDataTypeName.ToLower())
                    {
                        case RAW_DATA_TYPE_DOT_RAW_FILES:
                        case RAW_DATA_TYPE_DOT_WIFF_FILES:
                        case RAW_DATA_TYPE_DOT_UIMF_FILES:
                        case RAW_DATA_TYPE_DOT_MZXML_FILES:
                            // Don't actually copy the .Raw (or .wiff, .uimf, etc.) file locally; instead,
                            //  determine where it is located then create a text file named "DatasetName.raw_StoragePathInfo.txt"
                            //  This new file contains just one line of text: the full path to the actual file
                            createStoragePathInfoOnly = true;
                            break;
                        default:
                            createStoragePathInfoOnly = false;
                            break;
                    }

                    if (!FileSearch.RetrieveSpectra(rawDataTypeName, createStoragePathInfoOnly))
                    {
                        LogDebug("clsAnalysisResourcesMsMsSpectrumFilter.GetResources: Error occurred retrieving spectra.");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                // Add additional extensions to delete after the tool finishes
                mJobParams.AddResultFileExtensionToSkip("_ScanStats.txt");
                mJobParams.AddResultFileExtensionToSkip("_ScanStatsEx.txt");
                mJobParams.AddResultFileExtensionToSkip("_StoragePathInfo.txt");

                mJobParams.AddResultFileExtensionToSkip(DOT_WIFF_EXTENSION);
                mJobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                mJobParams.AddResultFileExtensionToSkip(DOT_UIMF_EXTENSION);
                mJobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);

                mJobParams.AddResultFileExtensionToSkip(DOT_MGF_EXTENSION);
                mJobParams.AddResultFileExtensionToSkip(DOT_CDF_EXTENSION);
            }

            if (!base.ProcessMyEMSLDownloadQueue(mWorkDir, Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool FindExistingScanStatsFile(string datasetDirectoryPath)
        {
            var datasetDirectory = new DirectoryInfo(datasetDirectoryPath);

            try
            {
                if (!datasetDirectory.Exists)
                {
                    LogError("Dataset directory not found: " + datasetDirectoryPath);
                }

                var scanStatsFiles = datasetDirectory.GetFiles(DatasetName + "_ScanStats.txt", SearchOption.AllDirectories).ToList();

                if (scanStatsFiles.Count == 0)
                {
                    if (mDebugLevel >= 2)
                    {
                        LogWarning("No _ScanStats.txt files were found in subdirectories below " + datasetDirectoryPath);
                    }
                    return false;
                }

                // Find the newest file in scanStatsFiles
                var sortedScanStatsFiles = (from item in scanStatsFiles orderby item.LastWriteTime descending select item).ToList();

                var newestScanStatsFile = sortedScanStatsFiles[0];

                // Copy the ScanStats file locally
                newestScanStatsFile.CopyTo(Path.Combine(mWorkDir, newestScanStatsFile.Name));

                // Read the first line of the file and confirm that the _ScanTypeName column exists
                using (var reader = new StreamReader(new FileStream(newestScanStatsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    var dataLine = reader.ReadLine();

                    if (!dataLine.Contains(clsMsMsSpectrumFilter.SCANSTATS_COL_SCAN_TYPE_NAME))
                    {
                        if (mDebugLevel >= 1)
                        {
                            LogMessage("The newest _ScanStats.txt file for this dataset does not contain column " + clsMsMsSpectrumFilter.SCANSTATS_COL_SCAN_TYPE_NAME +
                                "; will need to re-generate the file using the .Raw file");
                        }
                        return false;
                    }
                }

                // Look for the _ScanStatsEx.txt file
                var strScanStatsExPath = Path.Combine(newestScanStatsFile.Directory.FullName, Path.GetFileNameWithoutExtension(newestScanStatsFile.Name) + "Ex.txt");

                if (File.Exists(strScanStatsExPath))
                {
                    // Copy it locally
                    File.Copy(strScanStatsExPath, Path.Combine(mWorkDir, Path.GetFileName(strScanStatsExPath)));

                    if (mDebugLevel >= 1)
                    {
                        LogMessage("Using existing _ScanStats.txt from " + newestScanStatsFile.FullName);
                    }

                    return true;
                }

                if (mDebugLevel >= 1)
                {
                    LogWarning("The _ScanStats.txt file was found at " + newestScanStatsFile.FullName + " but the _ScanStatsEx.txt file was not present");
                }
                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception in FindExistingScanStatsFile", ex);
                return false;
            }
        }

        #endregion
    }
}
