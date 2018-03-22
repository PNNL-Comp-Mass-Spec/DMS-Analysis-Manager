//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/29/2011
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;

namespace AnalysisManagerMSGFDBPlugIn
{
    /// <summary>
    /// Retrieve resources for the MSGFDB (aka MSGF+) plugin
    /// </summary>
    public class clsAnalysisResourcesMSGFDB : clsAnalysisResources
    {

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieves files necessary for running MSGF+
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        public override CloseOutType GetResources()
        {
            var currentTask = "Initializing";

            try
            {
                currentTask = "Retrieve shared resources";

                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                // Make sure the machine has enough free memory to run MSGF+
                // Setting MSGFDBJavaMemorySize is stored in the settings file for the job
                currentTask = "ValidateFreeMemorySize";
                if (!ValidateFreeMemorySize("MSGFDBJavaMemorySize", false))
                {
                    m_message = "Not enough free memory to run MSGF+";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Retrieve the Fasta file
                var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");

                currentTask = "RetrieveOrgDB to " + localOrgDbFolder;

                if (!RetrieveOrgDB(localOrgDbFolder))
                    return CloseOutType.CLOSEOUT_FAILED;

                LogMessage("Getting param file", 2);

                // Retrieve the parameter file
                // This will also obtain the _ModDefs.txt file using query
                //  SELECT Local_Symbol, Monoisotopic_Mass_Correction, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
                //  FROM V_Param_File_Mass_Mod_Info
                //  WHERE Param_File_Name = 'ParamFileName'

                var paramFileName = m_jobParams.GetParam("ParmFileName");
                currentTask = "RetrieveGeneratedParamFile " + paramFileName;

                if (!RetrieveGeneratedParamFile(paramFileName))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // The ToolName job parameter holds the name of the job script we are executing
                var scriptName = m_jobParams.GetParam("ToolName");

                if (scriptName.ToLower().Contains("mzxml") || scriptName.ToLower().Contains("msgfplus_bruker"))
                {
                    currentTask = "Get mzXML file";
                    result = GetMzXMLFile();
                }
                else if (scriptName.ToLower().Contains("mzml"))
                {
                    currentTask = "Get mzML file";
                    result = GetMzMLFile();
                }
                else
                {
                    currentTask = "RetrieveDtaFiles";
                    result = GetCDTAFile();

                    if (result == CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        currentTask = "GetMasicFiles";
                        result = GetMasicFiles();
                    }

                    if (result == CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        currentTask = "ValidateCDTAFile";
                        result = ValidateCDTAFile();
                    }
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                currentTask = "Add extensions to skip";

                // Add all the extensions of the files to delete after run
                // Do not skip all .gz files because the MSGF+ results are compressed using .gz
                m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);
                m_jobParams.AddResultFileExtensionToSkip("_dta.zip"); // Zipped DTA
                m_jobParams.AddResultFileExtensionToSkip("_dta.txt"); // Unzipped, concatenated DTA
                m_jobParams.AddResultFileExtensionToSkip("temp.tsv"); // MSGFDB creates .txt.temp.tsv files, which we don't need

                m_jobParams.AddResultFileExtensionToSkip(SCAN_STATS_FILE_SUFFIX);
                m_jobParams.AddResultFileExtensionToSkip(SCAN_STATS_EX_FILE_SUFFIX);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Exception in GetResources (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Copy resources to the remote host: working directory files, job parameters file, FASTA file
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        public override bool CopyResourcesToRemote(clsRemoteTransferUtility transferUtility)
        {
            try
            {
                // Save job parameters to XML file JobParams.xml
                // Required because the tool runner needs to load various info, including the RemoteTimestamp,
                // the name of the generated parameter file, and the generated OrgDB name (generatedFastaName)
                SaveCurrentJobParameters();
            }
            catch (Exception ex)
            {
                LogError("Exception saving current job parameters", ex);
                return false;
            }

            try
            {

                // Construct a list of any files that we don't want to copy
                var filesToIgnore = GetDefaultWorkDirFilesToIgnore();

                var copyWorkDirSuccess = CopyWorkDirFilesToRemote(transferUtility, filesToIgnore);

                if (!copyWorkDirSuccess)
                    return false;

                var attemptCount = 0;

                while (true)
                {

                    // Copy the FASTA file to the remote computer
                    var copyOrgDbSuccess = CopyGeneratedOrgDBToRemote(transferUtility);
                    if (copyOrgDbSuccess)
                        return true;

                    attemptCount++;
                    if (attemptCount >= 3)
                        return false;

                    LogWarning("Copy of FASTA file to remote host failed; will try again in 5 seconds");

                    clsGlobal.IdleLoop(5);
                }

            }
            catch (Exception ex)
            {
                LogError("Exception copying resources to remote host " + transferUtility.RemoteHostName, ex);
                return false;
            }
        }

        private CloseOutType GetCDTAFile()
        {
            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file

            if (!FileSearch.RetrieveDtaFiles())
            {
                var sharedResultsFolder = m_jobParams.GetParam("SharedResultsFolders");
                if (!string.IsNullOrEmpty(sharedResultsFolder))
                {
                    m_message += "; shared results folder is " + sharedResultsFolder;
                }

                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMasicFiles()
        {
            var assumedScanType = m_jobParams.GetParam("AssumedScanType");

            if (!string.IsNullOrWhiteSpace(assumedScanType))
            {
                // Scan type is assumed; we don't need the Masic ScanStats.txt files or the .Raw file
                switch (assumedScanType.ToUpper())
                {
                    case "CID":
                    case "ETD":
                    case "HCD":
                        LogMessage("Assuming scan type is '" + assumedScanType + "'", 1);
                        break;
                    default:
                        LogError("Invalid assumed scan type '" + assumedScanType + "'; must be CID, ETD, or HCD");
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            // Retrieve the MASIC ScanStats.txt file (and possibly the ScanStatsEx.txt file)

            var success = FileSearch.RetrieveScanStatsFiles(createStoragePathInfoOnly: false, retrieveScanStatsFile: true, retrieveScanStatsExFile: false);

            if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (success)
            {
                // Open the ScanStats file and read the header line to see if column ScanTypeName is present
                // Also confirm that there are MSn spectra labeled as HCD, CID, or ETD
                var scanStatsOrExFilePath = Path.Combine(m_WorkingDir, DatasetName + "_ScanStats.txt");

                var scanTypeColumnFound = ValidateScanStatsFileHasScanTypeNameColumn(scanStatsOrExFilePath);

                if (!scanTypeColumnFound)
                {
                    // We also have to retrieve the _ScanStatsEx.txt file
                    success = FileSearch.RetrieveScanStatsFiles(createStoragePathInfoOnly: false, retrieveScanStatsFile: false, retrieveScanStatsExFile: true);

                    if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (success)
                    {
                        scanStatsOrExFilePath = Path.Combine(m_WorkingDir, DatasetName + "_ScanStatsEx.txt");
                    }
                }

                if (scanTypeColumnFound || success)
                {
                    var detailedScanTypesDefined = ValidateScanStatsFileHasDetailedScanTypes(scanStatsOrExFilePath);

                    if (!detailedScanTypesDefined)
                    {
                        if (scanTypeColumnFound)
                        {
                            m_message = "ScanTypes defined in the ScanTypeName column";
                        }
                        else
                        {
                            m_message = "ScanTypes defined in the \"Collision Mode\" column or \"Scan Filter Text\" column";
                        }

                        m_message += " do not contain detailed CID, ETD, or HCD information; MSGF+ could use the wrong scoring model; fix this problem before running MSGF+";

                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            if (success)
            {
                LogMessage("Retrieved MASIC ScanStats and ScanStatsEx files", 1);
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            // _ScanStats.txt file not found
            // If processing a .Raw file or .UIMF file, we can create the file using the MSFileInfoScanner
            if (!GenerateScanStatsFile())
            {
                // Error message should already have been logged and stored in m_message
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var scanStatsFilePath = Path.Combine(m_WorkingDir, DatasetName + "_ScanStats.txt");
            var detailedScanTypesDefinedNewFile = ValidateScanStatsFileHasDetailedScanTypes(scanStatsFilePath);

            if (!detailedScanTypesDefinedNewFile)
            {
                m_message = "ScanTypes defined in the ScanTypeName column do not contain detailed CID, ETD, or HCD information; " +
                    "MSGF+ could use the wrong scoring model; fix this problem before running MSGF+";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType ValidateCDTAFile()
        {
            // If the _dta.txt file is over 2 GB in size, condense it
            if (!ValidateCDTAFileSize(m_WorkingDir, DatasetName + "_dta.txt"))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Remove any spectra from the _DTA.txt file with fewer than 3 ions
            if (!ValidateCDTAFileRemoveSparseSpectra(m_WorkingDir, DatasetName + "_dta.txt"))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make sure that the spectra are centroided
            var cdtaPath = Path.Combine(m_WorkingDir, DatasetName + "_dta.txt");

            LogMessage("Validating that the _dta.txt file has centroided spectra");

            if (!ValidateCDTAFileIsCentroided(cdtaPath))
            {
                // m_message is already updated
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Check whether the ScanStats file has detailed scan type info
        /// </summary>
        /// <param name="scanStatsFilePath"></param>
        /// <returns></returns>
        public static bool ValidateScanStatsFileHasDetailedScanTypes(string scanStatsFilePath)
        {
            var columnNameWithScanType = new List<string>
            {
                "ScanTypeName",
                "Collision Mode",
                "Scan Filter Text"
            };
            var columnIndicesToCheck = new List<int>();

            var detailedScanTypesDefined = false;

            using (var srScanStatsFile = new StreamReader(new FileStream(scanStatsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                string headerLine = null;

                while (!srScanStatsFile.EndOfStream)
                {
                    headerLine = srScanStatsFile.ReadLine();
                    if (!string.IsNullOrWhiteSpace(headerLine))
                        break;
                }

                if (string.IsNullOrWhiteSpace(headerLine))
                {
                    return false;
                }

                // Parse the scan headers
                var headerColumns = headerLine.Split('\t').ToList();

                foreach (var columnName in columnNameWithScanType)
                {
                    var scanTypeIndex = headerColumns.IndexOf(columnName);
                    if (scanTypeIndex >= 0)
                    {
                        columnIndicesToCheck.Add(scanTypeIndex);
                    }
                }

                if (columnIndicesToCheck.Count == 0)
                {
                    if (float.TryParse(headerColumns[0], out var _) || float.TryParse(headerColumns[1], out _))
                    {
                        // This file does not have a header line
                        if (headerColumns.Count >= 11)
                        {
                            // Check whether column 11 has ScanTypeName info
                            if (headerColumns[10].IndexOf("MS", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                headerColumns[10].IndexOf("SRM", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                headerColumns[10].IndexOf("MRM", StringComparison.OrdinalIgnoreCase) >= 0)
                            {
                                return true;
                            }
                        }

                        if (headerColumns.Count >= 16)
                        {
                            // Check whether column 15 has "Collision Mode" values
                            if (headerColumns[15].IndexOf("HCD", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                headerColumns[15].IndexOf("CID", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                headerColumns[15].IndexOf("ETD", StringComparison.OrdinalIgnoreCase) >= 0)
                            {
                                return true;
                            }
                        }

                        if (headerColumns.Count >= 17)
                        {
                            // Check whether column 15 has "Collision Mode" values
                            if (headerColumns[16].IndexOf("HCD", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                headerColumns[16].IndexOf("CID", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                headerColumns[16].IndexOf("ETD", StringComparison.OrdinalIgnoreCase) >= 0)
                            {
                                return true;
                            }
                        }
                    }
                }

                if (columnIndicesToCheck.Count <= 0)
                    return false;

                while (!srScanStatsFile.EndOfStream && !detailedScanTypesDefined)
                {
                    var dataLine = srScanStatsFile.ReadLine();
                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var columns = dataLine.Split('\t').ToList();

                    foreach (var columnIndex in columnIndicesToCheck)
                    {
                        var scanType = columns[columnIndex];

                        if (scanType.IndexOf("HCD", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            detailedScanTypesDefined = true;
                        }
                        else if (scanType.IndexOf("CID", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            detailedScanTypesDefined = true;
                        }
                        else if (scanType.IndexOf("ETD", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            detailedScanTypesDefined = true;
                        }
                    }
                }
            }

            return detailedScanTypesDefined;
        }

        private bool ValidateScanStatsFileHasScanTypeNameColumn(string scanStatsFilePath)
        {

            using (var srScanStatsFile = new StreamReader(new FileStream(scanStatsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                if (srScanStatsFile.EndOfStream)
                    return false;

                var headerLine = srScanStatsFile.ReadLine();
                if (string.IsNullOrWhiteSpace(headerLine))
                    return false;

                // Parse the scan headers to look for ScanTypeName

                var columns = headerLine.Split('\t').ToList();

                if (columns.Contains("ScanTypeName"))
                {
                    return true;
                }

                if (!float.TryParse(columns[0], out var _) && !float.TryParse(columns[1], out _))
                    return false;

                // This file does not have a header line
                if (columns.Count < 11)
                    return false;

                // Assume column 11 is the ScanTypeName column
                if (columns[10].IndexOf("MS", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    columns[10].IndexOf("SRM", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    columns[10].IndexOf("MRM", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return true;
                }
            }

            return false;
        }

    }
}
