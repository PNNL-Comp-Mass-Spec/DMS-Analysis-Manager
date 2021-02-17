using System;
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
        /// Job parameter tracking the input file name for MSAlign Quant
        /// </summary>
        public const string MSALIGN_QUANT_INPUT_FILE_NAME_PARAM = "MSAlignQuantInputFileName";

        /// <summary>
        /// MSAlign results file suffix
        /// </summary>
        private const string MSALIGN_RESULT_TABLE_SUFFIX = "_MSAlign_ResultTable.txt";

        /// <summary>
        /// TopPIC results file suffix
        /// </summary>
        private const string TOPPIC_RESULT_FILE_SUFFIX = "_TopPIC_PrSMs.txt";

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

            var resultType = mJobParams.GetParam("ResultType");

            // Get analysis results files
            if (GetInputFiles(resultType) != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Get the instrument data file
            var rawDataTypeName = mJobParams.GetParam("RawDataType");

            switch (rawDataTypeName.ToLower())
            {
                case RAW_DATA_TYPE_DOT_RAW_FILES:
                case RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                case RAW_DATA_TYPE_DOT_D_FOLDERS:
                    if (FileSearch.RetrieveSpectra(rawDataTypeName))
                    {
                        if (!base.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                        {
                            return CloseOutType.CLOSEOUT_FAILED;
                        }

                        // Confirm that the .Raw or .D folder was actually copied locally
                        if (rawDataTypeName.ToLower() == RAW_DATA_TYPE_DOT_RAW_FILES)
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
                        else if (rawDataTypeName.ToLower() == RAW_DATA_TYPE_BRUKER_FT_FOLDER)
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
                    mMessage = "Dataset type " + rawDataTypeName + " is not supported";
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

        /// <summary>
        /// Retrieves input files needed for this step tool
        /// </summary>
        /// <param name="resultType">String specifying analysis tool result type</param>
        /// <returns>CloseOutType specifying results</returns>
        private CloseOutType GetInputFiles(string resultType)
        {
            try
            {
                var inputFolderName = mJobParams.GetParam("inputFolderName");
                if (string.IsNullOrWhiteSpace(inputFolderName))
                {
                    LogError("Input_Folder is not defined for this job step (job parameter inputFolderName); cannot retrieve input files");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                CloseOutType result;
                switch (resultType)
                {
                    case RESULT_TYPE_MSALIGN:
                        result = GetMSAlignFiles();
                        break;

                    case RESULT_TYPE_TOPPIC:
                        result = GetTopPICFiles();
                        break;

                    default:
                        LogError("Invalid tool result type: " + resultType);
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }
            }
            catch (Exception ex)
            {
                LogError("Error retrieving input files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetMSAlignFiles()
        {
            LogMessage("Getting data files");

            // Retrieve the MSAlign_Quant parameter file
            var resultCode = RetrieveMSAlignQuantParameterFile();
            if (resultCode != CloseOutType.CLOSEOUT_SUCCESS)
                return resultCode;

            // Retrieve the MSAlign results for this job
            var msAlignResultsTableFile = DatasetName + MSALIGN_RESULT_TABLE_SUFFIX;
            if (!FileSearch.FindAndRetrieveMiscFiles(msAlignResultsTableFile, false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(msAlignResultsTableFile);
            mJobParams.AddAdditionalParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, MSALIGN_QUANT_INPUT_FILE_NAME_PARAM, msAlignResultsTableFile);

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetTopPICFiles()
        {
            LogMessage("Getting data files");

            // Retrieve the MSAlign_Quant parameter file
            var resultCode = RetrieveMSAlignQuantParameterFile();
            if (resultCode != CloseOutType.CLOSEOUT_SUCCESS)
                return resultCode;

            // Retrieve the TopPIC results for this job
            var topPICResultsFile = DatasetName + TOPPIC_RESULT_FILE_SUFFIX;
            if (!FileSearch.FindAndRetrieveMiscFiles(topPICResultsFile, false))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(topPICResultsFile);
            mJobParams.AddAdditionalParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, MSALIGN_QUANT_INPUT_FILE_NAME_PARAM, topPICResultsFile);

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RetrieveMSAlignQuantParameterFile()
        {
            // For example, MSAlign_Quant_Workflow_2012-07-25
            // The parameter name is MSAlignQuantParamFile even when the step tool is TopPIC

            const string paramFileStoragePathKeyName = clsGlobal.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "MSAlign_Quant";
            var paramFileStoragePath = mMgrParams.GetParam(paramFileStoragePathKeyName);
            if (string.IsNullOrEmpty(paramFileStoragePath))
            {
                paramFileStoragePath = @"\\gigasax\DMS_Parameter_Files\DeconToolsWorkflows";
                LogWarning(
                    "Parameter '" + paramFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " + paramFileStoragePath);
            }

            var paramFileName = mJobParams.GetJobParameter("MSAlignQuantParamFile", string.Empty);
            if (string.IsNullOrEmpty(paramFileName))
            {
                mMessage = clsAnalysisToolRunnerBase.NotifyMissingParameter(mJobParams, "MSAlignQuantParamFile");
                LogError(mMessage);
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }


            if (FileSearch.RetrieveFile(paramFileName, paramFileStoragePath))
                return CloseOutType.CLOSEOUT_SUCCESS;

            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
        }
    }
}
