using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerProSightQuantPlugIn
{

    /// <summary>
    /// Retrieve resources for the ProSight Quant plugin
    /// </summary>
    public class clsAnalysisResourcesProSightQuant : clsAnalysisResources
    {

        /// <summary>
        /// ProSight PC results file name
        /// </summary>
        public const string PROSIGHT_PC_RESULT_FILE = "ProSightPC_Results.xls";

        /// <summary>
        /// Used to track whether this tool is disabled
        /// </summary>
        public const bool TOOL_DISABLED = true;

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

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (TOOL_DISABLED)
            {
                // This tool is currently disabled, so just return Success
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

#pragma warning disable 162

            // Retrieve the MSAlign_Quant parameter file
            // For example, MSAlign_Quant_Workflow_2012-07-25

            string strParamFileStoragePathKeyName = null;
            string strParamFileStoragePath = null;
            strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "MSAlign_Quant";

            strParamFileStoragePath = mMgrParams.GetParam(strParamFileStoragePathKeyName);
            if (string.IsNullOrEmpty(strParamFileStoragePath))
            {
                strParamFileStoragePath = @"\\gigasax\DMS_Parameter_Files\DeconToolsWorkflows";
                LogWarning(
                    "Parameter '" + strParamFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " + strParamFileStoragePath);
            }

            var strParamFileName = mJobParams.GetParam("ProSightQuantParamFile");
            if (string.IsNullOrEmpty(strParamFileName))
            {
                mMessage = clsAnalysisToolRunnerBase.NotifyMissingParameter(mJobParams, "ProSightQuantParamFile");
                LogError(mMessage);
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            LogMessage("Getting data files");

            if (!FileSearch.RetrieveFile(strParamFileName, strParamFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            // Retrieve the ProSightPC results for this job
            string strProSightPCResultsFile = null;
            strProSightPCResultsFile = PROSIGHT_PC_RESULT_FILE;
            if (!FileSearch.FindAndRetrieveMiscFiles(strProSightPCResultsFile, false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(strProSightPCResultsFile);

            // Get the instrument data file
            var strRawDataType = mJobParams.GetParam("RawDataType");

            switch (strRawDataType.ToLower())
            {
                case RAW_DATA_TYPE_DOT_RAW_FILES:
                case RAW_DATA_TYPE_BRUKER_FT_FOLDER:

                    if (FileSearch.RetrieveSpectra(strRawDataType))
                    {
                        if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                        {
                            return CloseOutType.CLOSEOUT_FAILED;
                        }

                        // Confirm that the .Raw or .D folder was actually copied locally
                        if (strRawDataType.ToLower() == RAW_DATA_TYPE_DOT_RAW_FILES)
                        {
                            if (!File.Exists(Path.Combine(mWorkDir, DatasetName + DOT_RAW_EXTENSION)))
                            {
                                mMessage = "Thermo .Raw file not successfully copied to WorkDir; likely a timeout error";
                                LogError("clsDtaGenResources.GetResources: " + mMessage);
                                return CloseOutType.CLOSEOUT_FAILED;
                            }

                            // Raw file
                            mJobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                        }
                        else if (strRawDataType.ToLower() == RAW_DATA_TYPE_BRUKER_FT_FOLDER)
                        {
                            if (!Directory.Exists(Path.Combine(mWorkDir, DatasetName + DOT_D_EXTENSION)))
                            {
                                mMessage = "Bruker .D folder not successfully copied to WorkDir; likely a timeout error";
                                LogError("clsDtaGenResources.GetResources: " + mMessage);
                                return CloseOutType.CLOSEOUT_FAILED;
                            }
                        }
                    }
                    else
                    {
                        LogDebug("clsDtaGenResources.GetResources: Error occurred retrieving spectra.");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                    break;
                default:
                    mMessage = "Dataset type " + strRawDataType + " is not supported";
                    LogError("clsDtaGenResources.GetResources: " + mMessage +
                             "; must be " + RAW_DATA_TYPE_DOT_RAW_FILES + " or " + RAW_DATA_TYPE_BRUKER_FT_FOLDER);
                    return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;

#pragma warning restore 162
        }
    }
}
