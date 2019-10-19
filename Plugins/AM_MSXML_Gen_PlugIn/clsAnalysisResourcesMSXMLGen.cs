using AnalysisManagerBase;
using System;

namespace AnalysisManagerMsXmlGenPlugIn
{
    /// <summary>
    /// Retrieve resources for the MSXMLGen plugin
    /// </summary>
    public class clsAnalysisResourcesMSXMLGen : clsAnalysisResources
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

                currentTask = "Determine RawDataType";

                // The ToolName job parameter holds the name of the job script we are executing
                var scriptName = mJobParams.GetParam("ToolName");

                var proMexBruker = scriptName.StartsWith("ProMex_Bruker", StringComparison.OrdinalIgnoreCase);

                if (proMexBruker)
                {
                    // Make sure the settings file has MSXMLOutputType=mzML, not mzXML

                    var msXmlFormat = mJobParams.GetParam("MSXMLOutputType");
                    if (string.IsNullOrWhiteSpace(msXmlFormat))
                    {
                        LogError("Job parameter MSXMLOutputType must be defined in the settings file");
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }

                    if (msXmlFormat.IndexOf(clsMSXmlGen.MZML_FILE_FORMAT, StringComparison.OrdinalIgnoreCase) < 0)
                    {
                        LogError("ProMex_Bruker jobs require mzML files, not " + msXmlFormat + " files");
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }
                }

                // Get input data file
                var strRawDataType = mJobParams.GetParam("RawDataType");

                var retrievalAttempts = 0;

                while (retrievalAttempts < 2)
                {
                    retrievalAttempts += 1;
                    switch (strRawDataType.ToLower())
                    {
                        case RAW_DATA_TYPE_DOT_RAW_FILES:
                        case RAW_DATA_TYPE_DOT_D_FOLDERS:
                        case RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER:
                        case RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                            currentTask = "Retrieve spectra: " + strRawDataType;

                            if (FileSearch.RetrieveSpectra(strRawDataType))
                            {
                                // Raw file
                                mJobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                            }
                            else
                            {
                                LogDebug("clsAnalysisResourcesMSXMLGen.GetResources: Error occurred retrieving spectra.");
                                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                            }
                            break;
                        default:
                            mMessage = "Dataset type " + strRawDataType + " is not supported";
                            LogDebug(
                                "clsAnalysisResourcesMSXMLGen.GetResources: " + mMessage + "; must be " + RAW_DATA_TYPE_DOT_RAW_FILES + ", " +
                                RAW_DATA_TYPE_DOT_D_FOLDERS + ", " + RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER + ", or " + RAW_DATA_TYPE_BRUKER_FT_FOLDER);

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

                    const string paramFileStoragePathKeyName = clsGlobal.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "MzML_Refinery";

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
                LogError(mMessage + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

    }
}
