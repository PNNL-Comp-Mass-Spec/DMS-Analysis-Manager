using AnalysisManagerBase;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerProSightQuantPlugIn
{
    /// <summary>
    /// Retrieve resources for the ProSight Quant plugin
    /// </summary>
    public class AnalysisResourcesProSightQuant : AnalysisResources
    {
        // Ignore Spelling: Bruker, Quant

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

#pragma warning disable CS0162

            // Retrieve the MSAlign_Quant parameter file
            // For example, MSAlign_Quant_Workflow_2012-07-25

            string paramFileStoragePathKeyName = null;
            string paramFileStoragePath = null;
            paramFileStoragePathKeyName = Global.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "MSAlign_Quant";

            paramFileStoragePath = mMgrParams.GetParam(paramFileStoragePathKeyName);

            if (string.IsNullOrEmpty(paramFileStoragePath))
            {
                paramFileStoragePath = @"\\gigasax\DMS_Parameter_Files\DeconToolsWorkflows";
                LogWarning(
                    "Parameter '" + paramFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tool_Storage_Paths in the Broker DB); will assume: " + paramFileStoragePath);
            }

            var paramFileName = mJobParams.GetParam("ProSightQuantParamFile");

            if (string.IsNullOrEmpty(paramFileName))
            {
                mMessage = AnalysisToolRunnerBase.NotifyMissingParameter(mJobParams, "ProSightQuantParamFile");
                LogError(mMessage);
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            LogMessage("Getting data files");

            if (!FileSearchTool.RetrieveFile(paramFileName, paramFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            // Retrieve the ProSightPC results for this job
            const string proSightPCResultsFile = PROSIGHT_PC_RESULT_FILE;

            if (!FileSearchTool.FindAndRetrieveMiscFiles(proSightPCResultsFile, false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(proSightPCResultsFile);

            // Get the instrument data file
            var rawDataType = mJobParams.GetParam("RawDataType");

            switch (rawDataType.ToLower())
            {
                case RAW_DATA_TYPE_DOT_RAW_FILES:
                case RAW_DATA_TYPE_BRUKER_FT_FOLDER:

                    if (FileSearchTool.RetrieveSpectra(rawDataType))
                    {
                        if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                        {
                            return CloseOutType.CLOSEOUT_FAILED;
                        }

                        // Confirm that the .Raw or .D folder was actually copied locally
                        if (rawDataType.ToLower() == RAW_DATA_TYPE_DOT_RAW_FILES)
                        {
                            if (!File.Exists(Path.Combine(mWorkDir, DatasetName + DOT_RAW_EXTENSION)))
                            {
                                mMessage = "Thermo .Raw file not successfully copied to WorkDir; likely a timeout error";
                                LogError("DtaGenResources.GetResources: " + mMessage);
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
                                LogError("DtaGenResources.GetResources: " + mMessage);
                                return CloseOutType.CLOSEOUT_FAILED;
                            }
                        }
                    }
                    else
                    {
                        LogDebug("DtaGenResources.GetResources: Error occurred retrieving spectra.");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                    break;
                default:
                    mMessage = "Dataset type " + rawDataType + " is not supported";
                    LogError("DtaGenResources.GetResources: " + mMessage +
                             "; must be " + RAW_DATA_TYPE_DOT_RAW_FILES + " or " + RAW_DATA_TYPE_BRUKER_FT_FOLDER);
                    return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;

#pragma warning restore CS0162
        }
    }
}
