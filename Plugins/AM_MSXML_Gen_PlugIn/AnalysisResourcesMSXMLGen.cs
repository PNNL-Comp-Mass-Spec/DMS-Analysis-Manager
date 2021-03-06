﻿using AnalysisManagerBase;
using System;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMsXmlGenPlugIn
{
    /// <summary>
    /// Retrieve resources for the MSXMLGen plugin
    /// </summary>
    public class AnalysisResourcesMSXMLGen : AnalysisResources
    {
        /// <summary>
        /// Retrieves files necessary for creating the .mzXML file
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

                currentTask = "Examine processing options";
                var msXmlGenerator = mJobParams.GetParam("MSXMLGenerator");    // ReAdW.exe or MSConvert.exe
                var msXmlFormat = mJobParams.GetParam("MSXMLOutputType");      // Typically mzXML or mzML

                if (string.IsNullOrWhiteSpace(msXmlGenerator) || string.IsNullOrWhiteSpace(msXmlFormat))
                {
                    LogError("Job parameters are invalid: MSXMLGenerator and MSXMLOutputType must be defined for this step tool");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // The ToolName job parameter holds the name of the job script we are executing
                var scriptName = mJobParams.GetParam("ToolName");

                if (Global.IsMatch(scriptName, "MaxQuant_DataPkg"))
                {
                    var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);
                    var usingMzML = mJobParams.GetJobParameter("CreateMzMLFiles", false);

                    if (dataPackageID > 0 && !usingMzML)
                    {
                        EvalMessage = string.Format("Skipping MSXMLGen since script is {0} and job parameter CreateMzMLFiles is false", scriptName);
                        LogMessage(EvalMessage);
                        return CloseOutType.CLOSEOUT_SKIPPED_MSXML_GEN;
                    }
                }

                if (Global.IsMatch(msXmlGenerator, "skip"))
                {
                    EvalMessage = "Skipping MSXMLGen since job parameter MSXMLGenerator is 'skip'";
                    LogMessage(EvalMessage);
                    return CloseOutType.CLOSEOUT_SKIPPED_MSXML_GEN;
                }

                currentTask = "Determine RawDataType";

                var proMexBruker = scriptName.StartsWith("ProMex_Bruker", StringComparison.OrdinalIgnoreCase);

                if (proMexBruker)
                {
                    // Make sure the settings file has MSXMLOutputType=mzML, not mzXML

                    if (string.IsNullOrWhiteSpace(msXmlFormat))
                    {
                        LogError("Job parameter MSXMLOutputType must be defined in the settings file");
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }

                    if (msXmlFormat.IndexOf(MSXmlGen.MZML_FILE_FORMAT, StringComparison.OrdinalIgnoreCase) < 0)
                    {
                        LogError("ProMex_Bruker jobs require mzML files, not " + msXmlFormat + " files");
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }
                }

                // Get input data file
                var rawDataTypeName = mJobParams.GetParam("RawDataType");
                var instrumentName = mJobParams.GetParam("Instrument");

                var retrievalAttempts = 0;

                while (retrievalAttempts < 2)
                {
                    retrievalAttempts++;
                    switch (rawDataTypeName.ToLower())
                    {
                        case RAW_DATA_TYPE_DOT_RAW_FILES:
                        case RAW_DATA_TYPE_DOT_D_FOLDERS:
                        case RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER:
                        case RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                            currentTask = string.Format("Retrieve spectra: {0}; instrument: {1}", rawDataTypeName, instrumentName);
                            var datasetResult = GetDatasetFile(rawDataTypeName);
                            if (datasetResult == CloseOutType.CLOSEOUT_FILE_NOT_FOUND)
                                return datasetResult;

                            break;

                        case RAW_DATA_TYPE_DOT_UIMF_FILES:
                            // Check whether the dataset directory has an Agilent .D directory
                            // If it does, and if PreferUIMF is false, retrieve it; otherwise, retrieve the .UIMF file
                            // Instruments IMS08_AgQTOF05 and IMS09_AgQToF06 should have .D directories

                            var isAgilentDotD = DatasetHasAgilentDotD();
                            var preferUIMF = mJobParams.GetJobParameter("PreferUIMF", false);

                            if (isAgilentDotD && !preferUIMF)
                            {
                                // Retrieve the .D directory
                                currentTask = string.Format("Retrieve .D directory; instrument: {0}", instrumentName);
                                var dotDSuccess = FileSearch.RetrieveDotDFolder(false, skipBafAndTdfFiles: true);
                                if (!dotDSuccess)
                                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;

                                mJobParams.AddAdditionalParameter("MSXMLGenerator", "ProcessingAgilentDotD", true);
                            }
                            else
                            {
                                // Retrieve the .uimf file for these
                                currentTask = string.Format("Retrieve .UIMF file; instrument: {0}", instrumentName);
                                var uimfResult = GetDatasetFile(rawDataTypeName);
                                if (uimfResult == CloseOutType.CLOSEOUT_FILE_NOT_FOUND)
                                    return uimfResult;

                                mJobParams.AddAdditionalParameter("MSXMLGenerator", "ProcessingAgilentDotD", false);
                            }

                            break;

                        default:
                            mMessage = "Dataset type " + rawDataTypeName + " is not supported";
                            LogDebug(
                                "AnalysisResourcesMSXMLGen.GetResources: " + mMessage + "; must be " +
                                RAW_DATA_TYPE_DOT_RAW_FILES + ", " +
                                RAW_DATA_TYPE_DOT_D_FOLDERS + ", " +
                                RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER + ", " +
                                RAW_DATA_TYPE_DOT_UIMF_FILES + ", or " +
                                RAW_DATA_TYPE_BRUKER_FT_FOLDER);

                            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    if (mMyEMSLUtilities.FilesToDownload.Count == 0)
                    {
                        break;
                    }

                    currentTask = "ProcessMyEMSLDownloadQueue";
                    if (mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                    {
                        break;
                    }

                    // Look for this file on the Samba share
                    DisableMyEMSLSearch();
                }

                var mzMLRefParamFile = mJobParams.GetJobParameter("MzMLRefParamFile", string.Empty);

                if (!string.IsNullOrEmpty(mzMLRefParamFile))
                {
                    // Retrieve the Fasta file
                    var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

                    currentTask = "RetrieveOrgDB to " + orgDbDirectoryPath;

                    if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                        return resultCode;

                    currentTask = "Retrieve the MzML Refinery parameter file " + mzMLRefParamFile;

                    const string paramFileStoragePathKeyName = Global.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "MzML_Refinery";

                    var mzMLRefineryParmFileStoragePath = mMgrParams.GetParam(paramFileStoragePathKeyName);
                    if (string.IsNullOrWhiteSpace(mzMLRefineryParmFileStoragePath))
                    {
                        mzMLRefineryParmFileStoragePath = @"\\gigasax\dms_parameter_Files\MzMLRefinery";
                        LogWarning(
                            "Parameter '" + paramFileStoragePathKeyName +
                            "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " +
                            mzMLRefineryParmFileStoragePath);
                    }

                    // Retrieve param file
                    if (!FileSearch.RetrieveFile(mzMLRefParamFile, mJobParams.GetParam("ParmFileStoragePath")))
                    {
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }
                }
            }
            catch (Exception ex)
            {
                mMessage = "Exception in GetResources: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + Global.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Look for a .D directory for this dataset
        /// </summary>
        /// <returns>True if found, otherwise empty string</returns>
        private bool DatasetHasAgilentDotD()
        {
            FindValidDirectory(
                DatasetName, string.Empty, DatasetName + ".d", 2,
                false,
                false,
                out var validDirectoryFound,
                false,
                out var _);

            if (validDirectoryFound)
            {
                LogMessage("Found .d directory for " + DatasetName);
                return true;
            }

            LogMessage("Did not find a .d directory for " + DatasetName + "; will process the dataset's .UIMF file");
            return false;
        }

        private CloseOutType GetDatasetFile(string rawDataTypeName)
        {
            if (FileSearch.RetrieveSpectra(rawDataTypeName))
            {
                // Raw file
                mJobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            LogDebug("AnalysisResourcesMSXMLGen.GetDatasetFile: Error occurred retrieving spectra.");
            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
        }
    }
}
