using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;
using PHRPReader;

namespace AnalysisManagerIDPickerPlugIn
{
    /// <summary>
    /// Retrieve resources for the IDPicker plugin
    /// </summary>
    public class clsAnalysisResourcesIDPicker : clsAnalysisResources
    {
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
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
        }

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

            // Retrieve the parameter file for the associated peptide search tool (Sequest, XTandem, MSGF+, etc.)
            var strParamFileName = mJobParams.GetParam("ParmFileName");

            if (!FileSearch.FindAndRetrieveMiscFiles(strParamFileName, false))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }
            mJobParams.AddResultFileToSkip(strParamFileName);

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (!clsAnalysisToolRunnerIDPicker.ALWAYS_SKIP_IDPICKER)
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
            var eRawDataType = GetRawDataType(rawDataTypeName);
            var blnMGFInstrumentData = mJobParams.GetJobParameter("MGFInstrumentData", false);

            // Retrieve the PSM result files, PHRP files, and MSGF file
            if (!GetInputFiles(DatasetName, strParamFileName, out result))
            {
                if (result == CloseOutType.CLOSEOUT_SUCCESS)
                    result = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                return result;
            }

            if (mSynopsisFileIsEmpty)
            {
                // Don't retrieve any additional files
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            if (!blnMGFInstrumentData)
            {
                // Retrieve the MASIC ScanStats.txt and ScanStatsEx.txt files

                if (eRawDataType == eRawDataTypeConstants.ThermoRawFile | eRawDataType == eRawDataTypeConstants.UIMF)
                {
                    var noScanStats = mJobParams.GetJobParameter("PepXMLNoScanStats", false);
                    if (noScanStats)
                    {
                        LogMessage("Not retrieving MASIC files since PepXMLNoScanStats is True");
                    }
                    else
                    {
                        var eResult = RetrieveMASICFilesWrapper();
                        if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            return eResult;
                        }
                    }
                }
                else
                {
                    LogWarning("Not retrieving MASIC files since unsupported data type: " + rawDataTypeName);
                }
            }

            if (!mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            var blnSplitFasta = mJobParams.GetJobParameter("SplitFasta", false);

            if (blnSplitFasta)
            {
                // Override the SplitFasta job parameter
                mJobParams.SetParam("SplitFasta", "False");
            }

            if (blnSplitFasta && clsAnalysisToolRunnerIDPicker.ALWAYS_SKIP_IDPICKER)
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

            if (blnSplitFasta)
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

            var success = clsGlobal.GetQueryResultsTopRow(sqlQuery, dmsConnectionString, out var lstResults, "LookupLegacyFastaFileName");

            if (!success || lstResults == null || lstResults.Count == 0)
            {
                mMessage = "Could not determine the legacy fasta file name (OrganismDBName in V_Analysis_Job) for job " + mJob;
                return string.Empty;
            }

            return lstResults.First();
        }

        /// <summary>
        /// Copies the required input files to the working directory
        /// </summary>
        /// <param name="strDatasetName">Dataset name</param>
        /// <param name="strSearchEngineParamFileName">Search engine parameter file name</param>
        /// <param name="eReturnCode">Return code</param>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks></remarks>
        private bool GetInputFiles(string strDatasetName, string strSearchEngineParamFileName, out CloseOutType eReturnCode)
        {
            // This tracks the filenames to find.  The Boolean value is True if the file is Required, false if not required

            eReturnCode = CloseOutType.CLOSEOUT_SUCCESS;

            var strResultType = mJobParams.GetParam("ResultType");

            // Make sure the ResultType is valid
            var eResultType = clsPHRPReader.GetPeptideHitResultType(strResultType);

            if (!(eResultType == clsPHRPReader.ePeptideHitResultType.Sequest || eResultType == clsPHRPReader.ePeptideHitResultType.XTandem ||
                  eResultType == clsPHRPReader.ePeptideHitResultType.Inspect || eResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB ||
                  eResultType == clsPHRPReader.ePeptideHitResultType.MODa || eResultType == clsPHRPReader.ePeptideHitResultType.MODPlus))
            {
                mMessage = "Invalid tool result type (not supported by IDPicker): " + strResultType;
                LogError(mMessage);
                eReturnCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                return false;
            }

            mInputFileRenames = new Dictionary<string, string>();

            var lstFileNamesToGet = GetPHRPFileNames(eResultType, strDatasetName);
            mSynopsisFileIsEmpty = false;

            if (mDebugLevel >= 2)
            {
                LogDebug("Retrieving the " + eResultType + " files");
            }

            var synFileNameExpected = clsPHRPReader.GetPHRPSynopsisFileName(eResultType, strDatasetName);
            var synFilePath = string.Empty;

            foreach (var kvEntry in lstFileNamesToGet)
            {
                var fileToGet = string.Copy(kvEntry.Key);
                var fileRequired = kvEntry.Value;

                // Note that the contents of fileToGet will be updated by FindAndRetrievePHRPDataFile if we're looking for a _msgfplus file but we find a _msgfdb file
                var success = FileSearch.FindAndRetrievePHRPDataFile(ref fileToGet, synFilePath);

                var toolVersionInfoFile = clsPHRPReader.GetToolVersionInfoFilename(eResultType);

                if (!success && eResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB &&
                    toolVersionInfoFile != null && fileToGet.Contains(Path.GetFileName(toolVersionInfoFile)))
                {
                    var strToolVersionFileLegacy = "Tool_Version_Info_MSGFDB.txt";
                    success = FileSearch.FindAndRetrieveMiscFiles(strToolVersionFileLegacy, false, false);
                    if (success)
                    {
                        // Rename the Tool_Version file to the expected name (Tool_Version_Info_MSGFPlus.txt)
                        File.Move(Path.Combine(mWorkDir, strToolVersionFileLegacy), Path.Combine(mWorkDir, fileToGet));
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

                    if (!ValidateFileHasData(synFilePath, "Synopsis file", out var strErrorMessage))
                    {
                        // The synopsis file is empty
                        LogWarning(strErrorMessage);

                        // We don't want to fail the job out yet; instead, we'll exit now, then let the ToolRunner exit with a Completion message of "Synopsis file is empty"
                        mSynopsisFileIsEmpty = true;
                        return true;
                    }
                }
            }

            if (eResultType == clsPHRPReader.ePeptideHitResultType.XTandem)
            {
                // X!Tandem requires a few additional parameter files
                var lstExtraFilesToGet = clsPHRPParserXTandem.GetAdditionalSearchEngineParamFileNames(Path.Combine(mWorkDir, strSearchEngineParamFileName));

                foreach (var strFileName in lstExtraFilesToGet)
                {
                    if (!FileSearch.FindAndRetrieveMiscFiles(strFileName, false))
                    {
                        // File not found
                        eReturnCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        return false;
                    }

                    mJobParams.AddResultFileToSkip(strFileName);
                }
            }

            if (!mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return false;
            }

            foreach (var item in mInputFileRenames)
            {
                var fiFile = new FileInfo(Path.Combine(mWorkDir, item.Key));
                if (!fiFile.Exists)
                {
                    mMessage = "File " + item.Key + " not found; unable to rename to " + item.Value;
                    LogError(mMessage);
                    eReturnCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    return false;
                }

                try
                {
                    fiFile.MoveTo(Path.Combine(mWorkDir, item.Value));
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
        /// <returns></returns>
        /// <remarks></remarks>
        private bool RetrieveIDPickerParamFile()
        {
            var strIDPickerParamFileName = mJobParams.GetParam("IDPickerParamFile");

            if (string.IsNullOrEmpty(strIDPickerParamFileName))
            {
                strIDPickerParamFileName = DEFAULT_IDPICKER_PARAM_FILE_NAME;
            }

            var strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "IDPicker";
            var strIDPickerParamFilePath = mMgrParams.GetParam(strParamFileStoragePathKeyName);
            if (string.IsNullOrEmpty(strIDPickerParamFilePath))
            {
                strIDPickerParamFilePath = @"\\gigasax\dms_parameter_Files\IDPicker";
                LogWarning("Parameter '" + strParamFileStoragePathKeyName +
                           "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " +
                           strIDPickerParamFilePath);
            }

            if (!CopyFileToWorkDir(strIDPickerParamFileName, strIDPickerParamFilePath, mWorkDir))
            {
                // Errors were reported in function call, so just return
                return false;
            }

            // Store the param file name so that we can load later
            mJobParams.AddAdditionalParameter(clsAnalysisJob.JOB_PARAMETERS_SECTION, IDPICKER_PARAM_FILENAME_LOCAL, strIDPickerParamFileName);

            return true;
        }

        private CloseOutType RetrieveMASICFilesWrapper()
        {
            var retrievalAttempts = 0;

            while (retrievalAttempts < 2)
            {
                retrievalAttempts += 1;
                if (!RetrieveMASICFiles(DatasetName))
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (mMyEMSLUtilities.FilesToDownload.Count == 0)
                {
                    break;
                }

                if (mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
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
        /// <param name="strDatasetName"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool RetrieveMASICFiles(string strDatasetName)
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

            mJobParams.AddResultFileToSkip(strDatasetName + SCAN_STATS_FILE_SUFFIX);
            mJobParams.AddResultFileToSkip(strDatasetName + SCAN_STATS_EX_FILE_SUFFIX);
            return true;
        }

        /// <summary>
        /// Determines the files that need to be copied to the work directory, based on the result type
        /// </summary>
        /// <param name="eResultType">PHRP result type (Seqest, X!Tandem, etc.)</param>
        /// <param name="strDatasetName">Dataset name</param>
        /// <returns>A generic list with the filenames to find.  The Boolean value is True if the file is Required, false if not required</returns>
        /// <remarks></remarks>
        private SortedList<string, bool> GetPHRPFileNames(clsPHRPReader.ePeptideHitResultType eResultType, string strDatasetName)
        {
            var lstFileNamesToGet = new SortedList<string, bool>();

            var synFileName = clsPHRPReader.GetPHRPSynopsisFileName(eResultType, strDatasetName);

            lstFileNamesToGet.Add(synFileName, true);
            lstFileNamesToGet.Add(clsPHRPReader.GetPHRPModSummaryFileName(eResultType, strDatasetName), false);
            lstFileNamesToGet.Add(clsPHRPReader.GetPHRPResultToSeqMapFileName(eResultType, strDatasetName), true);
            lstFileNamesToGet.Add(clsPHRPReader.GetPHRPSeqInfoFileName(eResultType, strDatasetName), true);
            lstFileNamesToGet.Add(clsPHRPReader.GetPHRPSeqToProteinMapFileName(eResultType, strDatasetName), true);
            lstFileNamesToGet.Add(clsPHRPReader.GetPHRPPepToProteinMapFileName(eResultType, strDatasetName), false);

            if (eResultType != clsPHRPReader.ePeptideHitResultType.MODa && eResultType != clsPHRPReader.ePeptideHitResultType.MODPlus)
            {
                lstFileNamesToGet.Add(clsPHRPReader.GetMSGFFileName(synFileName), true);
            }

            var strToolVersionFile = clsPHRPReader.GetToolVersionInfoFilename(eResultType);
            var strToolNameForScript = mJobParams.GetJobParameter("ToolName", "");
            if (eResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB && strToolNameForScript == "MSGFPlus_IMS")
            {
                // PeptideListToXML expects the ToolVersion file to be named "Tool_Version_Info_MSGFPlus.txt"
                // However, this is the MSGFPlus_IMS script, so the file is currently "Tool_Version_Info_MSGFPlus_IMS.txt"
                // We'll copy the current file locally, then rename it to the expected name
                const string strOriginalName = "Tool_Version_Info_MSGFPlus_IMS.txt";
                mInputFileRenames.Add(strOriginalName, strToolVersionFile);
                strToolVersionFile = string.Copy(strOriginalName);
            }

            lstFileNamesToGet.Add(strToolVersionFile, true);

            return lstFileNamesToGet;
        }
    }
}
