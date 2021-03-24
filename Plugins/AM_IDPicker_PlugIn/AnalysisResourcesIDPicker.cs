using AnalysisManagerBase;
using PHRPReader;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;
using PHRPReader.Reader;
using PRISMDatabaseUtils;

namespace AnalysisManagerIDPickerPlugIn
{
    /// <summary>
    /// Retrieve resources for the IDPicker plugin
    /// </summary>
    public class AnalysisResourcesIDPicker : AnalysisResources
    {
        // Ignore Spelling: ParmFileName, msgfdb

        /// <summary>
        /// ID Picker parameter file name
        /// </summary>
        public const string IDPICKER_PARAM_FILENAME_LOCAL = "IDPickerParamFileLocal";

        /// <summary>
        /// Default IDPicker parameter file name
        /// </summary>
        public const string DEFAULT_IDPICKER_PARAM_FILE_NAME = "IDPicker_Defaults.txt";

        private bool mSynopsisFileIsEmpty;

        /// <summary>
        /// This dictionary holds any filenames that we need to rename after copying locally
        /// </summary>
        private Dictionary<string, string> mInputFileRenames;

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
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var sharedResourceResult = GetSharedResources();
            if (sharedResourceResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return sharedResourceResult;
            }

            // Retrieve the parameter file for the associated peptide search tool (Sequest, XTandem, MSGF+, etc.)
            var paramFileName = mJobParams.GetParam("ParmFileName");

            if (!FileSearch.FindAndRetrieveMiscFiles(paramFileName, false))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }
            mJobParams.AddResultFileToSkip(paramFileName);

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (!AnalysisToolRunnerIDPicker.ALWAYS_SKIP_IDPICKER)
            {
#pragma warning disable 162
                // Retrieve the IDPicker parameter file specified for this job
                if (!RetrieveIDPickerParamFile())
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }
#pragma warning restore 162
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
                        LogMessage("Not retrieving MASIC files since PepXMLNoScanStats is True");
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
                // Do not retrieve the fasta file
                // However, do contact DMS to lookup the name of the legacy fasta file that was used for this job
                mFastaFileName = LookupLegacyFastaFileName();

                if (string.IsNullOrEmpty(mFastaFileName))
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Unable to determine the legacy fasta file name";
                        LogError(mMessage);
                    }
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                mJobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", mFastaFileName);
            }
            else
            {
                // Retrieve the Fasta file
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

        private string LookupLegacyFastaFileName()
        {
            var dmsConnectionString = mMgrParams.GetParam("ConnectionString");
            if (string.IsNullOrWhiteSpace(dmsConnectionString))
            {
                mMessage = "Error in LookupLegacyFastaFileName: manager parameter ConnectionString is not defined";
                LogError(mMessage);
                return string.Empty;
            }

            var sqlQuery = "SELECT OrganismDBName FROM V_Analysis_Job WHERE (Job = " + mJob + ")";

            var dbTools = DbToolsFactory.GetDBTools(dmsConnectionString, debugMode: TraceMode);
            RegisterEvents(dbTools);

            var success = Global.GetQueryResultsTopRow(dbTools, sqlQuery, out var orgDbNameForJob);

            if (!success || orgDbNameForJob == null || orgDbNameForJob.Count == 0)
            {
                mMessage = "Could not determine the legacy fasta file name (OrganismDBName in V_Analysis_Job) for job " + mJob;
                return string.Empty;
            }

            return orgDbNameForJob.First();
        }

        /// <summary>
        /// Copies the required input files to the working directory
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="searchEngineParamFileName">Search engine parameter file name</param>
        /// <param name="eReturnCode">Return code</param>
        /// <returns>True if success, otherwise false</returns>
        private bool GetInputFiles(string datasetName, string searchEngineParamFileName, out CloseOutType eReturnCode)
        {
            // This tracks the filenames to find.  The Boolean value is True if the file is Required, false if not required

            eReturnCode = CloseOutType.CLOSEOUT_SUCCESS;

            var resultTypeName = mJobParams.GetParam("ResultType");

            // Make sure the ResultType is valid
            var resultType = ReaderFactory.GetPeptideHitResultType(resultTypeName);

            if (!(resultType == Enums.PeptideHitResultTypes.Sequest || resultType == Enums.PeptideHitResultTypes.XTandem ||
                  resultType == Enums.PeptideHitResultTypes.Inspect || resultType == Enums.PeptideHitResultTypes.MSGFPlus ||
                  resultType == Enums.PeptideHitResultTypes.MODa || resultType == Enums.PeptideHitResultTypes.MODPlus))
            {
                mMessage = "Invalid tool result type (not supported by IDPicker): " + resultType;
                LogError(mMessage);
                eReturnCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                return false;
            }

            mInputFileRenames = new Dictionary<string, string>();

            var fileNamesToGet = GetPHRPFileNames(resultType, datasetName);
            mSynopsisFileIsEmpty = false;

            if (mDebugLevel >= 2)
            {
                LogDebug("Retrieving the " + resultType + " files");
            }

            var synFileNameExpected = ReaderFactory.GetPHRPSynopsisFileName(resultType, datasetName);
            var synFilePath = string.Empty;

            foreach (var kvEntry in fileNamesToGet)
            {
                var fileToGet = string.Copy(kvEntry.Key);
                var fileRequired = kvEntry.Value;

                // Note that the contents of fileToGet will be updated by FindAndRetrievePHRPDataFile if we're looking for a _msgfplus file but we find a _msgfdb file
                var success = FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, synFilePath);

                var toolVersionInfoFile = ReaderFactory.GetToolVersionInfoFilename(resultType);

                if (!success && resultType == Enums.PeptideHitResultTypes.MSGFPlus &&
                    toolVersionInfoFile != null && fileToGet.Contains(Path.GetFileName(toolVersionInfoFile)))
                {
                    const string toolVersionFileLegacy = "Tool_Version_Info_MSGFDB.txt";
                    success = FileSearch.FindAndRetrieveMiscFiles(toolVersionFileLegacy, false, false);
                    if (success)
                    {
                        // Rename the Tool_Version file to the expected name (Tool_Version_Info_MSGFPlus.txt)
                        File.Move(Path.Combine(mWorkDir, toolVersionFileLegacy), Path.Combine(mWorkDir, fileToGet));
                    }
                }

                if (!success)
                {
                    // File not found; is it required?
                    if (fileRequired)
                    {
                        // Errors were reported in function call, so just return
                        eReturnCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        return false;
                    }
                }

                mJobParams.AddResultFileToSkip(fileToGet);

                if (kvEntry.Key == synFileNameExpected)
                {
                    // Check whether the synopsis file is empty
                    synFilePath = Path.Combine(mWorkDir, Path.GetFileName(fileToGet));

                    if (!ValidateFileHasData(synFilePath, "Synopsis file", out var errorMessage))
                    {
                        // The synopsis file is empty
                        LogWarning(errorMessage);

                        // We don't want to fail the job out yet; instead, we'll exit now, then let the ToolRunner exit with a Completion message of "Synopsis file is empty"
                        mSynopsisFileIsEmpty = true;
                        return true;
                    }
                }
            }

            if (resultType == Enums.PeptideHitResultTypes.XTandem)
            {
                // X!Tandem requires a few additional parameter files
                var extraFilesToGet = XTandemSynFileReader.GetAdditionalSearchEngineParamFileNames(Path.Combine(mWorkDir, searchEngineParamFileName));

                foreach (var fileName in extraFilesToGet)
                {
                    if (!FileSearch.FindAndRetrieveMiscFiles(fileName, false))
                    {
                        // File not found
                        eReturnCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        return false;
                    }

                    mJobParams.AddResultFileToSkip(fileName);
                }
            }

            if (!mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return false;
            }

            foreach (var item in mInputFileRenames)
            {
                var file = new FileInfo(Path.Combine(mWorkDir, item.Key));
                if (!file.Exists)
                {
                    mMessage = "File " + item.Key + " not found; unable to rename to " + item.Value;
                    LogError(mMessage);
                    eReturnCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    return false;
                }

                try
                {
                    file.MoveTo(Path.Combine(mWorkDir, item.Value));
                }
                catch (Exception ex)
                {
                    mMessage = "Error renaming file " + item.Key + " to " + item.Value;
                    LogError(mMessage + "; " + ex.Message);
                    eReturnCode = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                mJobParams.AddResultFileToSkip(item.Value);
            }

            return true;
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
                           "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " +
                           idPickerParamFilePath);
            }

            if (!CopyFileToWorkDir(idPickerParamFileName, idPickerParamFilePath, mWorkDir))
            {
                // Errors were reported in function call, so just return
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
            if (!FileSearch.RetrieveScanStatsFiles(false))
            {
                // _ScanStats.txt file not found
                // If processing a .Raw file or .UIMF file, we can create the file using the MSFileInfoScanner
                if (!GenerateScanStatsFile())
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
        /// <param name="resultType">PHRP result type (Sequest, X!Tandem, etc.)</param>
        /// <param name="datasetName">Dataset name</param>
        /// <returns>A generic list with the filenames to find.  The Boolean value is True if the file is Required, false if not required</returns>
        private SortedList<string, bool> GetPHRPFileNames(Enums.PeptideHitResultTypes resultType, string datasetName)
        {
            var fileNamesToGet = new SortedList<string, bool>();

            var synFileName = ReaderFactory.GetPHRPSynopsisFileName(resultType, datasetName);

            fileNamesToGet.Add(synFileName, true);
            fileNamesToGet.Add(ReaderFactory.GetPHRPModSummaryFileName(resultType, datasetName), false);
            fileNamesToGet.Add(ReaderFactory.GetPHRPResultToSeqMapFileName(resultType, datasetName), true);
            fileNamesToGet.Add(ReaderFactory.GetPHRPSeqInfoFileName(resultType, datasetName), true);
            fileNamesToGet.Add(ReaderFactory.GetPHRPSeqToProteinMapFileName(resultType, datasetName), true);
            fileNamesToGet.Add(ReaderFactory.GetPHRPPepToProteinMapFileName(resultType, datasetName), false);

            if (resultType != Enums.PeptideHitResultTypes.MODa && resultType != Enums.PeptideHitResultTypes.MODPlus)
            {
                fileNamesToGet.Add(ReaderFactory.GetMSGFFileName(synFileName), true);
            }

            var toolVersionFile = ReaderFactory.GetToolVersionInfoFilename(resultType);
            var toolNameForScript = mJobParams.GetJobParameter("ToolName", "");
            if (resultType == Enums.PeptideHitResultTypes.MSGFPlus && toolNameForScript == "MSGFPlus_IMS")
            {
                // PeptideListToXML expects the ToolVersion file to be named "Tool_Version_Info_MSGFPlus.txt"
                // However, this is the MSGFPlus_IMS script, so the file is currently "Tool_Version_Info_MSGFPlus_IMS.txt"
                // We'll copy the current file locally, then rename it to the expected name
                const string originalName = "Tool_Version_Info_MSGFPlus_IMS.txt";
                mInputFileRenames.Add(originalName, toolVersionFile);
                toolVersionFile = string.Copy(originalName);
            }

            fileNamesToGet.Add(toolVersionFile, true);

            return fileNamesToGet;
        }
    }
}
