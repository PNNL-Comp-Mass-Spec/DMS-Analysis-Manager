using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;
using PHRPReader;

namespace AnalysisManagerIDPickerPlugIn
{
    public class clsAnalysisResourcesIDPicker : clsAnalysisResources
    {
        public const string IDPICKER_PARAM_FILENAME_LOCAL = "IDPickerParamFileLocal";
        public const string DEFAULT_IDPICKER_PARAM_FILE_NAME = "IDPicker_Defaults.txt";

        private bool mSynopsisFileIsEmpty;

        // This dictionary holds any filenames that we need to rename after copying locally
        private Dictionary<string, string> mInputFileRenames;

        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
        }

        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Retrieve the parameter file for the associated peptide search tool (Sequest, XTandem, MSGF+, etc.)
            var strParamFileName = m_jobParams.GetParam("ParmFileName");

            if (!FileSearch.FindAndRetrieveMiscFiles(strParamFileName, false))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }
            m_jobParams.AddResultFileToSkip(strParamFileName);

            if (!clsAnalysisToolRunnerIDPicker.ALWAYS_SKIP_IDPICKER)
            {
                // Retrieve the IDPicker parameter file specified for this job
                if (!RetrieveIDPickerParamFile())
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }
            }

            var RawDataType = m_jobParams.GetParam("RawDataType");
            var eRawDataType = GetRawDataType(RawDataType);
            var blnMGFInstrumentData = m_jobParams.GetJobParameter("MGFInstrumentData", false);

            // Retrieve the PSM result files, PHRP files, and MSGF file
            if (!GetInputFiles(DatasetName, strParamFileName, out result))
            {
                if (result == CloseOutType.CLOSEOUT_SUCCESS)
                    result = CloseOutType.CLOSEOUT_FAILED;
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
                    var noScanStats = m_jobParams.GetJobParameter("PepXMLNoScanStats", false);
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
                    LogWarning("Not retrieving MASIC files since unsupported data type: " + RawDataType);
                }
            }

            if (!m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var blnSplitFasta = m_jobParams.GetJobParameter("SplitFasta", false);

            if (blnSplitFasta)
            {
                // Override the SplitFasta job parameter
                m_jobParams.SetParam("SplitFasta", "False");
            }

            if (blnSplitFasta && clsAnalysisToolRunnerIDPicker.ALWAYS_SKIP_IDPICKER)
            {
                // Do not retrieve the fasta file
                // However, do contact DMS to lookup the name of the legacy fasta file that was used for this job
                m_FastaFileName = LookupLegacyFastaFileName();

                if (string.IsNullOrEmpty(m_FastaFileName))
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Unable to determine the legacy fasta file name";
                        LogError(m_message);
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_jobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", m_FastaFileName);
            }
            else
            {
                // Retrieve the Fasta file
                if (!RetrieveOrgDB(m_mgrParams.GetParam("orgdbdir")))
                    return CloseOutType.CLOSEOUT_FAILED;
            }

            if (blnSplitFasta)
            {
                // Restore the setting for SplitFasta
                m_jobParams.SetParam("SplitFasta", "True");
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private string LookupLegacyFastaFileName()
        {
            var dmsConnectionString = m_mgrParams.GetParam("connectionstring");
            if (string.IsNullOrWhiteSpace(dmsConnectionString))
            {
                m_message = "Error in LookupLegacyFastaFileName: manager parameter connectionstring is not defined";
                LogError(m_message);
                return string.Empty;
            }

            var sqlQuery = "SELECT OrganismDBName FROM V_Analysis_Job WHERE (Job = " + m_JobNum + ")";

            var success = clsGlobal.GetQueryResultsTopRow(sqlQuery, dmsConnectionString, out var lstResults, "LookupLegacyFastaFileName");

            if (!success || lstResults == null || lstResults.Count == 0)
            {
                m_message = "Could not determine the legacy fasta file name (OrganismDBName in V_Analysis_Job) for job " + m_JobNum;
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

            var strResultType = m_jobParams.GetParam("ResultType");

            // Make sure the ResultType is valid
            var eResultType = clsPHRPReader.GetPeptideHitResultType(strResultType);

            if (!(eResultType == clsPHRPReader.ePeptideHitResultType.Sequest || eResultType == clsPHRPReader.ePeptideHitResultType.XTandem ||
                  eResultType == clsPHRPReader.ePeptideHitResultType.Inspect || eResultType == clsPHRPReader.ePeptideHitResultType.MSGFDB ||
                  eResultType == clsPHRPReader.ePeptideHitResultType.MODa || eResultType == clsPHRPReader.ePeptideHitResultType.MODPlus))
            {
                m_message = "Invalid tool result type (not supported by IDPicker): " + strResultType;
                LogError(m_message);
                eReturnCode = CloseOutType.CLOSEOUT_FAILED;
                return false;
            }

            mInputFileRenames = new Dictionary<string, string>();

            var lstFileNamesToGet = GetPHRPFileNames(eResultType, strDatasetName);
            mSynopsisFileIsEmpty = false;

            if (m_DebugLevel >= 2)
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
                        File.Move(Path.Combine(m_WorkingDir, strToolVersionFileLegacy), Path.Combine(m_WorkingDir, fileToGet));
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

                m_jobParams.AddResultFileToSkip(fileToGet);

                if (kvEntry.Key == synFileNameExpected)
                {
                    // Check whether the synopsis file is empty
                    synFilePath = Path.Combine(m_WorkingDir, Path.GetFileName(fileToGet));

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
                var lstExtraFilesToGet = clsPHRPParserXTandem.GetAdditionalSearchEngineParamFileNames(Path.Combine(m_WorkingDir, strSearchEngineParamFileName));

                foreach (var strFileName in lstExtraFilesToGet)
                {
                    if (!FileSearch.FindAndRetrieveMiscFiles(strFileName, false))
                    {
                        // File not found
                        eReturnCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                        return false;
                    }

                    m_jobParams.AddResultFileToSkip(strFileName);
                }
            }

            if (!m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return false;
            }

            foreach (var item in mInputFileRenames)
            {
                var fiFile = new FileInfo(Path.Combine(m_WorkingDir, item.Key));
                if (!fiFile.Exists)
                {
                    m_message = "File " + item.Key + " not found; unable to rename to " + item.Value;
                    LogError(m_message);
                    eReturnCode = CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    return false;
                }

                try
                {
                    fiFile.MoveTo(Path.Combine(m_WorkingDir, item.Value));
                }
                catch (Exception ex)
                {
                    m_message = "Error renaming file " + item.Key + " to " + item.Value;
                    LogError(m_message + "; " + ex.Message);
                    eReturnCode = CloseOutType.CLOSEOUT_FAILED;
                    return false;
                }

                m_jobParams.AddResultFileToSkip(item.Value);
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
            var strIDPickerParamFileName = m_jobParams.GetParam("IDPickerParamFile");

            if (string.IsNullOrEmpty(strIDPickerParamFileName))
            {
                strIDPickerParamFileName = DEFAULT_IDPICKER_PARAM_FILE_NAME;
            }

            var strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "IDPicker";
            var strIDPickerParamFilePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName);
            if (string.IsNullOrEmpty(strIDPickerParamFilePath))
            {
                strIDPickerParamFilePath = @"\\gigasax\dms_parameter_Files\IDPicker";
                LogWarning("Parameter '" + strParamFileStoragePathKeyName +
                           "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " +
                           strIDPickerParamFilePath);
            }

            if (!CopyFileToWorkDir(strIDPickerParamFileName, strIDPickerParamFilePath, m_WorkingDir))
            {
                // Errors were reported in function call, so just return
                return false;
            }

            // Store the param file name so that we can load later
            m_jobParams.AddAdditionalParameter("JobParameters", IDPICKER_PARAM_FILENAME_LOCAL, strIDPickerParamFileName);

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

                if (m_MyEMSLUtilities.FilesToDownload.Count == 0)
                {
                    break;
                }

                if (m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
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
                // If processing a .Raw file or .UIMF file then we can create the file using the MSFileInfoScanner
                if (!GenerateScanStatsFile())
                {
                    // Error message should already have been logged and stored in m_message
                    return false;
                }
            }
            else
            {
                if (m_DebugLevel >= 1)
                {
                    LogMessage("Retrieved MASIC ScanStats and ScanStatsEx files");
                }
            }

            m_jobParams.AddResultFileToSkip(strDatasetName + SCAN_STATS_FILE_SUFFIX);
            m_jobParams.AddResultFileToSkip(strDatasetName + SCAN_STATS_EX_FILE_SUFFIX);
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
            var strToolNameForScript = m_jobParams.GetJobParameter("ToolName", "");
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
