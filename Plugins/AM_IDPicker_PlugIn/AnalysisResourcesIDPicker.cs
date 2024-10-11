using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;
using PHRPReader;
using PHRPReader.Reader;
using PRISMDatabaseUtils;

namespace AnalysisManagerIDPickerPlugIn
{
    /// <summary>
    /// Retrieve resources for the IDPicker plugin
    /// </summary>
    public class AnalysisResourcesIDPicker : AnalysisResources
    {
        // Ignore Spelling: IDPicker, msgfdb, PHRP

        /// <summary>
        /// ID Picker parameter file name
        /// </summary>
        public const string IDPICKER_PARAM_FILENAME_LOCAL = "IDPickerParamFileLocal";

        /// <summary>
        /// Default IDPicker parameter file name
        /// </summary>
        public const string DEFAULT_IDPICKER_PARAM_FILE_NAME = "IDPicker_Defaults.txt";

        /// <summary>
        /// Additional job parameter added if this is an aggregation job
        /// </summary>
        public const string JOB_PARAM_AGGREGATION_JOB_SYNOPSIS_FILE = "AggregationJobSynopsisFileName";

        /// <summary>
        /// Additional job parameter added if this is an aggregation job
        /// </summary>
        public const string JOB_PARAM_AGGREGATION_JOB_PHRP_BASE_NAME = "AggregationJobPhrpBaseName";

        private bool mSynopsisFileIsEmpty;

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            try
            {
                // Retrieve shared resources, including the JobParameters file from the previous job step
                var sharedResourceResult = GetSharedResources();

                if (sharedResourceResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return sharedResourceResult;
                }

                // Retrieve the parameter file for the associated peptide search tool (SEQUEST, XTandem, MS-GF+, etc.)
                var paramFileName = mJobParams.GetParam("ParamFileName");

                if (!FileSearchTool.FindAndRetrieveMiscFiles(paramFileName, false))
                {
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }
                mJobParams.AddResultFileToSkip(paramFileName);

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (!AnalysisToolRunnerIDPicker.ALWAYS_SKIP_IDPICKER)
                {
#pragma warning disable CS0162
                    // Retrieve the IDPicker parameter file specified for this job
                    if (!RetrieveIDPickerParamFile())
                    {
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }
#pragma warning restore CS0162
                }

                var rawDataTypeName = mJobParams.GetParam("RawDataType");
                var rawDataType = GetRawDataType(rawDataTypeName);
                var mgfInstrumentData = mJobParams.GetJobParameter("MGFInstrumentData", false);

                // Retrieve the PSM result files, PHRP files, and MSGF file
                if (!GetInputFiles(DatasetName, paramFileName, out var result))
                {
                    return result == CloseOutType.CLOSEOUT_SUCCESS ? CloseOutType.CLOSEOUT_FILE_NOT_FOUND : result;
                }

                if (mSynopsisFileIsEmpty)
                {
                    // Don't retrieve any additional files
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                if (!mgfInstrumentData)
                {
                    // Retrieve the MASIC ScanStats.txt and ScanStatsEx.txt files

                    if (rawDataType == RawDataTypeConstants.ThermoRawFile || rawDataType == RawDataTypeConstants.UIMF)
                    {
                        var noScanStats = mJobParams.GetJobParameter("PepXMLNoScanStats", false);

                        if (noScanStats)
                        {
                            LogMessage("Not retrieving MASIC files since PepXMLNoScanStats is true");
                        }
                        else
                        {
                            var masicResult = RetrieveMASICFilesWrapper();

                            if (masicResult != CloseOutType.CLOSEOUT_SUCCESS)
                            {
                                return masicResult;
                            }
                        }
                    }
                    else
                    {
                        LogWarning("Not retrieving MASIC files since unsupported data type: " + rawDataTypeName);
                    }
                }

                if (!mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                var splitFasta = mJobParams.GetJobParameter("SplitFasta", false);

                if (splitFasta)
                {
                    // Override the SplitFasta job parameter
                    mJobParams.SetParam("SplitFasta", "False");
                }

                if (splitFasta && AnalysisToolRunnerIDPicker.ALWAYS_SKIP_IDPICKER)
                {
                    // Do not retrieve the FASTA file
                    // However, do contact DMS to lookup the name of the legacy FASTA file that was used for this job
                    mFastaFileName = LookupLegacyFastaFileName();

                    if (string.IsNullOrEmpty(mFastaFileName))
                    {
                        if (string.IsNullOrEmpty(mMessage))
                        {
                            LogError("Unable to determine the legacy FASTA file name");
                        }
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    mJobParams.AddAdditionalParameter(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName", mFastaFileName);
                }
                else
                {
                    // Retrieve the FASTA file
                    var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

                    if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                        return resultCode;
                }

                if (splitFasta)
                {
                    // Restore the setting for SplitFasta
                    mJobParams.SetParam("SplitFasta", "True");
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error retrieving input files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void FindMaxQuantSynopsisFile(FileSystemInfo directoryToCheck, out string synopsisFileName, out int fileCountFound)
        {
            if (directoryToCheck.Exists)
            {
                var synopsisFiles = FileSearchTool.FindMaxQuantSynopsisFiles(directoryToCheck.FullName);
                fileCountFound = synopsisFiles.Count;
                synopsisFileName = synopsisFiles.Count > 0 ? synopsisFiles[0].Name : string.Empty;
                return;
            }

            fileCountFound = 0;
            synopsisFileName = string.Empty;
        }

        private string LookupLegacyFastaFileName()
        {
            var dmsConnectionString = mMgrParams.GetParam("ConnectionString");

            if (string.IsNullOrWhiteSpace(dmsConnectionString))
            {
                LogError("Error in LookupLegacyFastaFileName: manager parameter ConnectionString is not defined");
                return string.Empty;
            }

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(dmsConnectionString, mMgrName);

            var sqlQuery = "SELECT organism_db_name FROM V_Analysis_Job WHERE job = " + mJob;

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
            RegisterEvents(dbTools);

            var success = Global.GetQueryResultsTopRow(dbTools, sqlQuery, out var orgDbNameForJob);

            if (!success || orgDbNameForJob == null || orgDbNameForJob.Count == 0)
            {
                LogError("Could not determine the legacy FASTA file name (organism_db_name in V_Analysis_Job) for job " + mJob);
                return string.Empty;
            }

            return orgDbNameForJob.First();
        }

        /// <summary>
        /// Copies the required input files to the working directory
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="searchEngineParamFileName">Search engine parameter file name</param>
        /// <param name="returnCode">Return code</param>
        /// <returns>True if success, otherwise false</returns>
        private bool GetInputFiles(string datasetName, string searchEngineParamFileName, out CloseOutType returnCode)
        {
            returnCode = CloseOutType.CLOSEOUT_SUCCESS;

            var resultTypeName = GetResultType(mJobParams);

            // Make sure the ResultType is valid
            var resultType = ReaderFactory.GetPeptideHitResultType(resultTypeName);

            if (resultType is not (
                PeptideHitResultTypes.Sequest or
                PeptideHitResultTypes.XTandem or
                PeptideHitResultTypes.Inspect or
                PeptideHitResultTypes.MSGFPlus or
                PeptideHitResultTypes.MODa or
                PeptideHitResultTypes.MODPlus or
                PeptideHitResultTypes.MaxQuant))
            {
                LogError("Invalid tool result type (not supported by IDPicker): " + resultType);
                returnCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                return false;
            }

            if (Global.IsMatch(datasetName, AGGREGATION_JOB_DATASET))
            {
                // Update datasetName using the base name used by the _maxq_syn.txt file for this aggregation job
                if (!GetMaxQuantSynopsisFileBaseName(out var baseName))
                {
                    // The error has already been logged
                    returnCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    return false;
                }

                datasetName = baseName;
            }

            // fileNamesToGet tracks the filenames to find
            // Keys are filenames to find and values are true if the file is required, false if not required

            var fileNamesToGet = GetPHRPFileNames(resultType, datasetName);
            mSynopsisFileIsEmpty = false;

            if (mDebugLevel >= 2)
            {
                LogDebug("Retrieving the " + resultType + " files");
            }

            var toolVersionUtility = new ToolVersionUtilities(mMgrParams, mJobParams, mJob, DatasetName, StepToolName, mDebugLevel, mWorkDir);
            RegisterEvents(toolVersionUtility);

            var toolVersionFileFound = toolVersionUtility.RetrieveToolVersionInfoFile(FileSearchTool, resultType);

            if (!toolVersionFileFound)
            {
                LogError("Tool version info file not found; this is required to store the MS/MS search tool info in the .pepXML file");
                returnCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                return false;
            }

            var synFileNameExpected = ReaderFactory.GetPHRPSynopsisFileName(resultType, datasetName);
            var synFilePath = string.Empty;

            foreach (var kvEntry in fileNamesToGet)
            {
                var fileToGet = kvEntry.Key;
                var fileRequired = kvEntry.Value;

                // Note that the contents of fileToGet will be updated by FindAndRetrievePHRPDataFile if we're looking for a _msgfplus file but we find a _msgfdb file
                var success = FileSearchTool.FindAndRetrievePHRPDataFile(ref fileToGet, synFilePath);

                if (!success)
                {
                    // File not found; is it required?
                    if (fileRequired)
                    {
                        // Errors were reported in method call, so just return
                        returnCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        return false;
                    }
                }

                mJobParams.AddResultFileToSkip(fileToGet);

                if (kvEntry.Key != synFileNameExpected)
                {
                    continue;
                }

                // Check whether the synopsis file is empty
                synFilePath = Path.Combine(mWorkDir, Path.GetFileName(fileToGet));

                // ReSharper disable once InvertIf
                if (!ValidateFileHasData(synFilePath, "Synopsis", out var errorMessage))
                {
                    // The synopsis file is empty
                    LogWarning(errorMessage);

                    // We don't want to fail the job out yet; instead, we'll exit now, then let the ToolRunner exit with a Completion message of "Synopsis file is empty"
                    mSynopsisFileIsEmpty = true;
                    return true;
                }
            }

            if (resultType == PeptideHitResultTypes.XTandem)
            {
                // X!Tandem requires a few additional parameter files

                foreach (var fileName in XTandemSynFileReader.GetAdditionalSearchEngineParamFileNames(Path.Combine(mWorkDir, searchEngineParamFileName)))
                {
                    if (!FileSearchTool.FindAndRetrieveMiscFiles(fileName, false))
                    {
                        // File not found
                        returnCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        return false;
                    }

                    mJobParams.AddResultFileToSkip(fileName);
                }
            }

            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (!mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return false;
            }

            return true;
        }

        private bool GetMaxQuantSynopsisFileBaseName(out string baseName)
        {
            baseName = string.Empty;

            try
            {
                var transferDirectoryPath = GetTransferDirectoryPathForJobStep(true, false);

                if (string.IsNullOrWhiteSpace(transferDirectoryPath))
                {
                    LogError("GetTransferDirectoryPathForJobStep returned an empty string; cannot determine the synopsis file base name");
                    return false;
                }

                var transferDirectory = new DirectoryInfo(transferDirectoryPath);

                string sourceDirectoryDescription;
                string dataPackageResultDirectoryPath;

                string synopsisFileName;
                int fileCountFound;

                FindMaxQuantSynopsisFile(transferDirectory, out var transferDirectorySynopsisFileName, out var transferDirectoryFileCountFound);

                if (transferDirectoryFileCountFound == 0)
                {
                    // The job may have finished, and we're re-running a job step
                    // Look for the synopsis file in the data package directory

                    sourceDirectoryDescription = "data package directory";

                    var dataPackageDirectoryPath = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_DATA_PACKAGE_PATH);

                    if (string.IsNullOrWhiteSpace(dataPackageDirectoryPath))
                    {
                        LogError(string.Format("Job parameter {0} is an empty string; unable to find the synopsis file", JOB_PARAM_DATA_PACKAGE_PATH));
                        return false;
                    }

                    var inputDirectoryName = mJobParams.GetParam(JOB_PARAM_INPUT_FOLDER_NAME);

                    if (string.IsNullOrWhiteSpace(dataPackageDirectoryPath))
                    {
                        LogError(string.Format("Job parameter {0} is an empty string; unable to find the synopsis file", JOB_PARAM_INPUT_FOLDER_NAME));
                        return false;
                    }

                    dataPackageResultDirectoryPath = Path.Combine(dataPackageDirectoryPath, inputDirectoryName);
                    var dataPackageResultDirectory = new DirectoryInfo(dataPackageResultDirectoryPath);

                    FindMaxQuantSynopsisFile(dataPackageResultDirectory, out synopsisFileName, out fileCountFound);
                }
                else
                {
                    sourceDirectoryDescription = "transfer directory";
                    dataPackageResultDirectoryPath = string.Empty;

                    synopsisFileName = transferDirectorySynopsisFileName;
                    fileCountFound = transferDirectoryFileCountFound;
                }

                if (fileCountFound == 0)
                {
                    const string msg = "PHRP synopsis file not found in the transfer directory or the data package directory for this aggregation job";
                    LogError(msg, string.Format("{0}: {1} and {2}", msg, transferDirectoryPath, dataPackageResultDirectoryPath));
                    return false;
                }

                if (fileCountFound > 1)
                {
                    LogError("Multiple PHRP synopsis files were found in the {0} for this aggregation job: {1}", sourceDirectoryDescription, string.IsNullOrWhiteSpace(dataPackageResultDirectoryPath) ? transferDirectoryPath : dataPackageResultDirectoryPath);

                    return false;
                }

                var index = synopsisFileName.IndexOf(MaxQuantSynFileReader.FILENAME_SUFFIX_SYN, StringComparison.OrdinalIgnoreCase);

                if (index <= 0)
                {
                    LogError("Cannot determine the base name from the MaxQuant synopsis file name: " + synopsisFileName);
                    return false;
                }

                baseName = synopsisFileName.Substring(0, index);

                mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_AGGREGATION_JOB_SYNOPSIS_FILE, synopsisFileName);

                mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_AGGREGATION_JOB_PHRP_BASE_NAME, baseName);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error determining the base name of the MaxQuant synopsis file", ex);
                return false;
            }
        }

        /// <summary>
        /// Retrieve the ID Picker parameter file
        /// </summary>
        private bool RetrieveIDPickerParamFile()
        {
            var idPickerParamFileName = mJobParams.GetParam("IDPickerParamFile");

            if (string.IsNullOrEmpty(idPickerParamFileName))
            {
                idPickerParamFileName = DEFAULT_IDPICKER_PARAM_FILE_NAME;
            }

            const string paramFileStoragePathKeyName = Global.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "IDPicker";
            var idPickerParamFilePath = mMgrParams.GetParam(paramFileStoragePathKeyName);

            if (string.IsNullOrEmpty(idPickerParamFilePath))
            {
                idPickerParamFilePath = @"\\gigasax\dms_parameter_Files\IDPicker";
                LogWarning("Parameter '" + paramFileStoragePathKeyName +
                           "' is not defined (obtained using V_Pipeline_Step_Tool_Storage_Paths in the Broker DB); will assume: " +
                           idPickerParamFilePath);
            }

            if (!CopyFileToWorkDir(idPickerParamFileName, idPickerParamFilePath, mWorkDir))
            {
                // Errors were reported in method call, so just return
                return false;
            }

            // Store the param file name so that we can load later
            mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, IDPICKER_PARAM_FILENAME_LOCAL, idPickerParamFileName);

            return true;
        }

        private CloseOutType RetrieveMASICFilesWrapper()
        {
            var retrievalAttempts = 0;

            while (retrievalAttempts < 2)
            {
                retrievalAttempts++;

                if (!RetrieveMASICFiles(DatasetName))
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (mMyEMSLUtilities.FilesToDownload.Count == 0)
                {
                    break;
                }

                if (mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    break;
                }

                // Look for the MASIC files on the Samba share
                DisableMyEMSLSearch();
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Retrieve the MASIC ScanStats.txt and ScanStatsEx.txt files
        /// </summary>
        /// <param name="datasetName"></param>
        private bool RetrieveMASICFiles(string datasetName)
        {
            if (!FileSearchTool.RetrieveScanStatsFiles(false))
            {
                // _ScanStats.txt file not found
                // If processing a .Raw file or .UIMF file, we can create the file using the MSFileInfoScanner
                if (!GenerateScanStatsFiles())
                {
                    // Error message should already have been logged and stored in mMessage
                    return false;
                }
            }
            else
            {
                if (mDebugLevel >= 1)
                {
                    LogMessage("Retrieved MASIC ScanStats and ScanStatsEx files");
                }
            }

            mJobParams.AddResultFileToSkip(datasetName + SCAN_STATS_FILE_SUFFIX);
            mJobParams.AddResultFileToSkip(datasetName + SCAN_STATS_EX_FILE_SUFFIX);
            return true;
        }

        /// <summary>
        /// Determines the files that need to be copied to the work directory, based on the result type
        /// </summary>
        /// <param name="resultType">PHRP result type (SEQUEST, X!Tandem, etc.)</param>
        /// <param name="datasetName">Dataset name</param>
        /// <returns>A sorted list where keys are filenames to find and values are true if the file is required, false if not required</returns>
        private SortedList<string, bool> GetPHRPFileNames(PeptideHitResultTypes resultType, string datasetName)
        {
            var fileNamesToGet = new SortedList<string, bool>();

            var synFileName = ReaderFactory.GetPHRPSynopsisFileName(resultType, datasetName);

            fileNamesToGet.Add(synFileName, true);
            fileNamesToGet.Add(ReaderFactory.GetPHRPModSummaryFileName(resultType, datasetName), false);
            fileNamesToGet.Add(ReaderFactory.GetPHRPResultToSeqMapFileName(resultType, datasetName), true);
            fileNamesToGet.Add(ReaderFactory.GetPHRPSeqInfoFileName(resultType, datasetName), true);
            fileNamesToGet.Add(ReaderFactory.GetPHRPSeqToProteinMapFileName(resultType, datasetName), true);
            fileNamesToGet.Add(ReaderFactory.GetPHRPPepToProteinMapFileName(resultType, datasetName), false);

            if (resultType != PeptideHitResultTypes.MODa &&
                resultType != PeptideHitResultTypes.MODPlus &&
                resultType != PeptideHitResultTypes.MaxQuant)
            {
                fileNamesToGet.Add(ReaderFactory.GetMSGFFileName(synFileName), true);
            }

            return fileNamesToGet;
        }
    }
}
