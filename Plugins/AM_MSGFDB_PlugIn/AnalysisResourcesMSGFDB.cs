//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/29/2011
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.OfflineJobs;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerMSGFDBPlugIn
{
    /// <summary>
    /// Retrieve resources for the MS-GF+ (aka MSGF+) plugin
    /// </summary>
    public class AnalysisResourcesMSGFDB : AnalysisResources
    {
        // Ignore Spelling: centroided, Dta, Fto, Mgf, MSConvert

        public const string MSCONVERT_FILENAME = "msconvert.exe";

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieves files necessary for running MS-GF+
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

                // Make sure the machine has enough free memory to run MS-GF+
                // Setting MSGFPlusJavaMemorySize is stored in the settings file for this job

                currentTask = "ValidateFreeMemorySize";

                if (!ValidateFreeMemorySize("MSGFPlusJavaMemorySize", false))
                {
                    mInsufficientFreeMemory = true;
                    mMessage = "Not enough free memory to run MS-GF+";
                    return CloseOutType.CLOSEOUT_RESET_JOB_STEP;
                }

                // Retrieve the FASTA file
                var orgDbDirectoryPath = mMgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);

                currentTask = "RetrieveOrgDB to " + orgDbDirectoryPath;

                if (!RetrieveOrgDB(orgDbDirectoryPath, out result))
                {
                    return result;
                }

                var fastaFile = new FileInfo(Path.Combine(orgDbDirectoryPath, mFastaFileName));

                if (!fastaFile.Exists)
                {
                    LogError("FASTA file not found: " + fastaFile.FullName);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                var splitFastaEnabled = mJobParams.GetJobParameter("SplitFasta", false);

                // Abort the job if a split FASTA search is enabled and the FASTA file is less than 0.1 MB (which is around 250 proteins)
                // The user probably chose the wrong settings file

                var fastaFileSizeMB = fastaFile.Length / 1024.0 / 1024;

                if (splitFastaEnabled && fastaFileSizeMB < 0.1)
                {
                    LogError("FASTA file is too small to be used in a split FASTA search ({0:F0} KB); make a new job that does not use MSGFPlus_MzML_SplitFasta", fastaFileSizeMB * 1024.0);

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // We reserve more memory for large FASTA files
                var javaMemorySizeMB = AnalysisToolRunnerMSGFDB.GetMemoryRequiredForFASTA(mJobParams, fastaFile, out _);

                // Compare the required memory size vs. actual free memory
                var validFreeMemory = ValidateFreeMemorySize(javaMemorySizeMB, StepToolName, false);

                if (!validFreeMemory)
                {
                    mInsufficientFreeMemory = true;
                    mMessage = "Not enough free memory to run MS-GF+";
                    return CloseOutType.CLOSEOUT_RESET_JOB_STEP;
                }

                LogMessage("Getting param file", 2);

                // Retrieve the parameter file
                // This will also obtain the _ModDefs.txt file using query
                //  SELECT Local_Symbol, Monoisotopic_Mass, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag, MaxQuant_Mod_Name, UniMod_Mod_Name
                //  FROM V_Param_File_Mass_Mod_Info
                //  WHERE Param_File_Name = 'ParamFileName'

                var paramFileName = mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE);
                currentTask = "RetrieveGeneratedParamFile " + paramFileName;

                if (!RetrieveGeneratedParamFile(paramFileName))
                {
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                // The ToolName job parameter holds the name of the job script we are executing
                var scriptName = mJobParams.GetParam("ToolName");

                if (scriptName.IndexOf("mzXML", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    scriptName.IndexOf("MSGFPlus_Bruker", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    currentTask = "Get mzXML file";
                    result = GetMzXMLFile();
                }
                else if (scriptName.IndexOf("mzML", StringComparison.OrdinalIgnoreCase) >= 0 ||
                         scriptName.IndexOf("DeconMSn_MzRefinery", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    currentTask = "Get mzML file";
                    result = GetMzMLFile();
                }
                else
                {
                    currentTask = "RetrieveDtaOrMGFFiles";

                    var dtaGenerator = mJobParams.GetJobParameter("DtaGenerator", string.Empty);
                    var convertToCDTA = mJobParams.GetJobParameter("DtaGenerator", "ConvertMGFtoCDTA", true);

                    if (string.Equals(dtaGenerator, MSCONVERT_FILENAME, StringComparison.OrdinalIgnoreCase) && !convertToCDTA)
                    {
                        result = GetMGFFile();

                        // Future: ValidateMGFFile
                    }
                    else
                    {
                        result = GetCDTAFile();

                        if (result == CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            currentTask = "ValidateCDTAFile";
                            result = ValidateCDTAFile();
                        }
                    }

                    if (result == CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        currentTask = "GetMasicFiles";

                        result = GetMasicFiles();

                        if (result == CloseOutType.CLOSEOUT_FILE_NOT_FOUND)
                        {
                            Global.AppendToComment(mMessage, "Use a settings file with parameter AssumedScanType");
                        }
                    }
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                currentTask = "Add extensions to skip";

                // Add all the extensions of the files to delete after run
                // Do not skip all .gz files because the MS-GF+ results are compressed using .gz
                mJobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);

                mJobParams.AddResultFileExtensionToSkip(CDTA_ZIPPED_EXTENSION);
                mJobParams.AddResultFileExtensionToSkip(CDTA_EXTENSION);

                mJobParams.AddResultFileExtensionToSkip(MGF_ZIPPED_EXTENSION);
                mJobParams.AddResultFileExtensionToSkip(DOT_MGF_EXTENSION);

                // MSGFDB creates .txt.temp.tsv files, which we don't need
                mJobParams.AddResultFileExtensionToSkip("temp.tsv");

                mJobParams.AddResultFileExtensionToSkip(SCAN_STATS_FILE_SUFFIX);
                mJobParams.AddResultFileExtensionToSkip(SCAN_STATS_EX_FILE_SUFFIX);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in GetResources (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void AppendSharedResultDirectoriesToComment()
        {
            var sharedResultsFolders = mJobParams.GetParam(JOB_PARAM_SHARED_RESULTS_FOLDERS);

            if (string.IsNullOrEmpty(sharedResultsFolders))
            {
                mMessage = Global.AppendToComment(mMessage, "Job parameter SharedResultsFolders is empty");
                return;
            }

            if (sharedResultsFolders.Contains(","))
            {
                mMessage = Global.AppendToComment(mMessage, "shared results folders: " + sharedResultsFolders);
            }
            else
            {
                mMessage = Global.AppendToComment(mMessage, "shared results folder " + sharedResultsFolders);
            }
        }

        /// <summary>
        /// Copy resources to the remote host: working directory files, job parameters file, FASTA file
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        public override bool CopyResourcesToRemote(RemoteTransferUtility transferUtility)
        {
            // Lookup scan stats
            try
            {
                var msgfPlusUtils = new MSGFPlusUtils(mMgrParams, mJobParams, mWorkDir, mDebugLevel);
                RegisterEvents(msgfPlusUtils);

                var success = msgfPlusUtils.LookupScanTypesForDataset(DatasetName, out var countLowResMSn, out var countHighResMSn, out var countLowResHCD, out var countHighResHCD);

                if (!success)
                {
                    var assumedScanType = mJobParams.GetParam("AssumedScanType");

                    if (string.IsNullOrWhiteSpace(assumedScanType))
                    {
                        LogError("LookupScanTypesForDataset returned false for dataset " + DatasetName);
                        return false;
                    }

                    LogMessage("LookupScanTypesForDataset returned false for dataset {0}; however, AssumedScanType is {1} and thus the scan stats are not required",
                        DatasetName,
                        assumedScanType);
                }

                mJobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, MSGFPlusUtils.SCAN_COUNT_LOW_RES_MSN, countLowResMSn);
                mJobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, MSGFPlusUtils.SCAN_COUNT_HIGH_RES_MSN, countHighResMSn);
                mJobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, MSGFPlusUtils.SCAN_COUNT_LOW_RES_HCD, countLowResHCD);
                mJobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, MSGFPlusUtils.SCAN_COUNT_HIGH_RES_HCD, countHighResHCD);
            }
            catch (Exception ex)
            {
                LogError("Exception looking up scan counts for dataset " + DatasetName, ex);
                return false;
            }

            try
            {
                // Save job parameters to XML file JobParams.xml
                // Required because the tool runner needs to load various info, including the RemoteTimestamp,
                // the name of the generated parameter file, and the generated OrgDB name (GeneratedFastaName)
                SaveCurrentJobParameters();
            }
            catch (Exception ex)
            {
                LogError("Exception saving current job parameters", ex);
                return false;
            }

            try
            {
                // Index the FASTA file (either by copying from a remote share, or by re-generating the index)
                var msgfPlusUtils = new MSGFPlusUtils(mMgrParams, mJobParams, mWorkDir, mDebugLevel);
                RegisterEvents(msgfPlusUtils);

                msgfPlusUtils.IgnorePreviousErrorEvent += MSGFPlusUtils_IgnorePreviousErrorEvent;

                // Get the FASTA file and index it if necessary
                // Passing in the path to the parameter file so we can look for TDA=0 when using large FASTA files
                var parameterFilePath = Path.Combine(mWorkDir, mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE));

                // javaProgLoc will typically be "C:\DMS_Programs\Java\jre11\bin\java.exe"
                var javaProgLoc = AnalysisToolRunnerBase.GetJavaProgLoc(mMgrParams, out var javaLocErrorMessage);

                if (string.IsNullOrEmpty(javaProgLoc))
                {
                    mMessage = Global.AppendToComment(mMessage, javaLocErrorMessage);
                    return false;
                }

                var msgfPlusProgLoc = AnalysisToolRunnerBase.DetermineProgramLocation(
                    mMgrParams, mJobParams, StepToolName,
                    "MSGFPlusProgLoc", MSGFPlusUtils.MSGFPLUS_JAR_NAME,
                    out var msgfPlusLocErrorMessage, out _);

                if (string.IsNullOrEmpty(msgfPlusProgLoc))
                {
                    mMessage = Global.AppendToComment(mMessage, msgfPlusLocErrorMessage);
                    return false;
                }

                var msgfPlusJarFilePath = msgfPlusProgLoc;

                var result = msgfPlusUtils.InitializeFastaFile(
                    javaProgLoc, msgfPlusJarFilePath,
                    out _, out _,
                    out _, parameterFilePath);

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (string.IsNullOrWhiteSpace(mMessage) &&
                        !string.IsNullOrWhiteSpace(msgfPlusUtils.ErrorMessage))
                    {
                        mMessage = Global.AppendToComment(mMessage, msgfPlusUtils.ErrorMessage);
                    }

                    return false;
                }

                // Construct a list of any files that we don't want to copy
                var filesToIgnore = GetDefaultWorkDirFilesToIgnore();
                filesToIgnore.Add(DatasetName + MGF_ZIPPED_EXTENSION);

                var copyWorkDirSuccess = CopyWorkDirFilesToRemote(transferUtility, filesToIgnore);

                if (!copyWorkDirSuccess)
                    return false;

                var attemptCount = 0;

                while (true)
                {
                    // Copy the FASTA file and related index files to the remote computer
                    var copyOrgDbSuccess = CopyGeneratedOrgDBToRemote(transferUtility);

                    if (copyOrgDbSuccess)
                        return true;

                    attemptCount++;

                    if (attemptCount >= 3)
                        return false;

                    LogWarning("Copy of FASTA file to remote host failed; will try again in 5 seconds");

                    Global.IdleLoop(5);
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
            // Retrieve the _DTA.txt or .mgf file
            // Note that if the file was found in MyEMSL, RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file

            if (FileSearchTool.RetrieveDtaFiles())
                return CloseOutType.CLOSEOUT_SUCCESS;

            AppendSharedResultDirectoriesToComment();

            // Errors were reported in method call, so just return
            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
        }

        private CloseOutType GetMasicFiles()
        {
            var assumedScanType = mJobParams.GetParam("AssumedScanType");

            if (!string.IsNullOrWhiteSpace(assumedScanType))
            {
                // Scan type is assumed; we don't need the MASIC ScanStats.txt files or the .Raw file
                switch (assumedScanType.ToUpper())
                {
                    case "CID":
                    case "ETD":
                    case "HCD":
                        LogMessage("Assuming scan type is '" + assumedScanType + "'", 1);
                        break;
                    default:
                        LogError("Invalid assumed scan type '" + assumedScanType + "'; must be CID, ETD, or HCD");
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            // Retrieve the MASIC ScanStats.txt file (and possibly the ScanStatsEx.txt file)

            var success = FileSearchTool.RetrieveScanStatsFiles(createStoragePathInfoOnly: false, retrieveScanStatsFile: true, retrieveScanStatsExFile: false);

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (success)
            {
                // Open the ScanStats file and read the header line to see if column ScanTypeName is present
                // Also confirm that there are MSn spectra labeled as HCD, CID, or ETD
                var scanStatsOrExFilePath = Path.Combine(mWorkDir, DatasetName + SCAN_STATS_FILE_SUFFIX);

                var scanTypeColumnFound = ValidateScanStatsFileHasScanTypeNameColumn(scanStatsOrExFilePath);

                if (!scanTypeColumnFound)
                {
                    // We also have to retrieve the _ScanStatsEx.txt file
                    success = FileSearchTool.RetrieveScanStatsFiles(createStoragePathInfoOnly: false, retrieveScanStatsFile: false, retrieveScanStatsExFile: true);

                    if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                    {
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    if (success)
                    {
                        scanStatsOrExFilePath = Path.Combine(mWorkDir, DatasetName + "_ScanStatsEx.txt");
                    }
                }

                if (scanTypeColumnFound || success)
                {
                    var detailedScanTypesDefined = ValidateScanStatsFileHasDetailedScanTypes(scanStatsOrExFilePath);

                    if (!detailedScanTypesDefined)
                    {
                        if (scanTypeColumnFound)
                        {
                            mMessage = "ScanTypes defined in the ScanTypeName column";
                        }
                        else
                        {
                            mMessage = "ScanTypes defined in the \"Collision Mode\" column or \"Scan Filter Text\" column";
                        }

                        mMessage += " do not contain detailed CID, ETD, or HCD information; MS-GF+ could use the wrong scoring model; fix this problem before running MS-GF+";

                        LogWarning(mMessage);

                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
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
            if (!GenerateScanStatsFiles())
            {
                // Error message should already have been logged and stored in mMessage
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            var scanStatsFilePath = Path.Combine(mWorkDir, DatasetName + SCAN_STATS_FILE_SUFFIX);
            var detailedScanTypesDefinedNewFile = ValidateScanStatsFileHasDetailedScanTypes(scanStatsFilePath);

            if (!detailedScanTypesDefinedNewFile)
            {
                mMessage = "ScanTypes defined in the ScanTypeName column do not contain detailed CID, ETD, or HCD information; " +
                    "MS-GF+ could use the wrong scoring model; fix this problem before running MS-GF+";

                LogWarning(mMessage);

                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMGFFile()
        {
            // Retrieve the _mgf.zip file and extract the .mgf file
            // Note that if the file was found in MyEMSL, GetZippedMgfFile will auto-call ProcessMyEMSLDownloadQueue to download the file

            var mgfFile = new FileInfo(Path.Combine(mWorkDir, DatasetName + DOT_MGF_EXTENSION));

            if (mgfFile.Exists)
            {
                // The .mgf file is already in the working directory
                // This will be the case when running jobs remotely
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            var result = GetZippedMgfFile();

            if (result == CloseOutType.CLOSEOUT_SUCCESS)
                return CloseOutType.CLOSEOUT_SUCCESS;

            AppendSharedResultDirectoriesToComment();

            // Errors were reported in method call, so just return
            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
        }

        private CloseOutType ValidateCDTAFile()
        {
            // If the _dta.txt file is over 2 GB in size, condense it
            if (!ValidateCDTAFileSize(mWorkDir, DatasetName + CDTA_EXTENSION))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Remove any spectra from the _DTA.txt file with fewer than 3 ions
            if (!ValidateCDTAFileRemoveSparseSpectra(mWorkDir, DatasetName + CDTA_EXTENSION))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            var cDtaValidated = mJobParams.GetJobParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "ValidatedCDtaIsCentroided", false);

            if (cDtaValidated)
            {
                LogMessage("Previously validated that the _dta.txt file has centroided spectra");
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            // Make sure that the spectra are centroided
            var cdtaPath = Path.Combine(mWorkDir, DatasetName + CDTA_EXTENSION);

            LogMessage("Validating that the _dta.txt file has centroided spectra");

            if (!ValidateCDTAFileIsCentroided(cdtaPath))
            {
                // mMessage is already updated
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "ValidatedCDtaIsCentroided", true);
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Check whether the ScanStats file has detailed scan type info
        /// </summary>
        /// <param name="scanStatsFilePath">Scan stats file path</param>
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

            using var reader = new StreamReader(new FileStream(scanStatsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            string headerLine = null;

            while (!reader.EndOfStream)
            {
                headerLine = reader.ReadLine();

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
                if (float.TryParse(headerColumns[0], out _) || float.TryParse(headerColumns[1], out _))
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

            if (columnIndicesToCheck.Count == 0)
                return false;

            while (!reader.EndOfStream && !detailedScanTypesDefined)
            {
                var dataLine = reader.ReadLine();

                if (string.IsNullOrWhiteSpace(dataLine))
                    continue;

                var dataCols = dataLine.Split('\t').ToList();

                foreach (var columnIndex in columnIndicesToCheck)
                {
                    var scanType = dataCols[columnIndex];

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

            return detailedScanTypesDefined;
        }

        private bool ValidateScanStatsFileHasScanTypeNameColumn(string scanStatsFilePath)
        {
            using var reader = new StreamReader(new FileStream(scanStatsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            if (reader.EndOfStream)
                return false;

            var headerLine = reader.ReadLine();

            if (string.IsNullOrWhiteSpace(headerLine))
                return false;

            // Parse the scan headers to look for ScanTypeName

            var columns = headerLine.Split('\t').ToList();

            if (columns.Contains("ScanTypeName"))
            {
                return true;
            }

            if (!float.TryParse(columns[0], out _) && !float.TryParse(columns[1], out _))
                return false;

            // This file does not have a header line
            if (columns.Count < 11)
                return false;

            // Assume column 11 is the ScanTypeName column
            return columns[10].IndexOf("MS", StringComparison.OrdinalIgnoreCase) >= 0 ||
                   columns[10].IndexOf("SRM", StringComparison.OrdinalIgnoreCase) >= 0 ||
                   columns[10].IndexOf("MRM", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private void MSGFPlusUtils_IgnorePreviousErrorEvent(string messageToIgnore)
        {
            mMessage = mMessage.Replace(messageToIgnore, string.Empty).Trim(';', ' ');
        }
    }
}
