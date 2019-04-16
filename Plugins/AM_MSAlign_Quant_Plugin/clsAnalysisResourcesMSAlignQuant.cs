using AnalysisManagerBase;
using System.IO;

namespace AnalysisManagerMSAlignQuantPlugIn
{

    /// <summary>
    /// Retrieve resources for the MSAlign Quant plugin
    /// </summary>
    public class clsAnalysisResourcesMSAlignQuant : clsAnalysisResources
    {

        /// <summary>
        /// MSAlign results table suffix
        /// </summary>
        public const string MSALIGN_RESULT_TABLE_SUFFIX = "_MSAlign_ResultTable.txt";

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

            // Retrieve the MSAlign_Quant parameter file
            // For example, MSAlign_Quant_Workflow_2012-07-25

            var strParamFileStoragePathKeyName = clsGlobal.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "MSAlign_Quant";
            var strParamFileStoragePath = mMgrParams.GetParam(strParamFileStoragePathKeyName);
            if (string.IsNullOrEmpty(strParamFileStoragePath))
            {
                strParamFileStoragePath = @"\\gigasax\DMS_Parameter_Files\DeconToolsWorkflows";
                LogWarning(
                    "Parameter '" + strParamFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " + strParamFileStoragePath);
            }

            var strParamFileName = mJobParams.GetJobParameter("MSAlignQuantParamFile", string.Empty);
            if (string.IsNullOrEmpty(strParamFileName))
            {
                mMessage = clsAnalysisToolRunnerBase.NotifyMissingParameter(mJobParams, "MSAlignQuantParamFile");
                LogError(mMessage);
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            LogMessage("Getting data files");

            if (!FileSearch.RetrieveFile(strParamFileName, strParamFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            // Retrieve the MSAlign results for this job
            var msAlignResultsTableFile = DatasetName + MSALIGN_RESULT_TABLE_SUFFIX;
            if (!FileSearch.FindAndRetrieveMiscFiles(msAlignResultsTableFile, false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(msAlignResultsTableFile);

            // Get the instrument data file
            var rawDataType = mJobParams.GetParam("RawDataType");

            switch (rawDataType.ToLower())
            {
                case RAW_DATA_TYPE_DOT_RAW_FILES:
                case RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                case RAW_DATA_TYPE_DOT_D_FOLDERS:
                    if (FileSearch.RetrieveSpectra(rawDataType))
                    {
                        if (!base.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                        {
                            return CloseOutType.CLOSEOUT_FAILED;
                        }

                        // Confirm that the .Raw or .D folder was actually copied locally
                        if (rawDataType.ToLower() == RAW_DATA_TYPE_DOT_RAW_FILES)
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
                        else if (rawDataType.ToLower() == RAW_DATA_TYPE_BRUKER_FT_FOLDER)
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
                    mMessage = "Dataset type " + rawDataType + " is not supported";
                    LogError("clsDtaGenResources.GetResources: " + mMessage +
                             "; must be " + RAW_DATA_TYPE_DOT_RAW_FILES + " or " + RAW_DATA_TYPE_BRUKER_FT_FOLDER);
                    return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!base.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
