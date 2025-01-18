using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMSAlignQuantPlugIn
{
    /// <summary>
    /// Retrieve resources for the MSAlign Quant plugin
    /// </summary>
    public class AnalysisResourcesMSAlignQuant : AnalysisResources
    {
        // Ignore Spelling: Bruker, Quant

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

            var resultType = GetResultType(mJobParams);

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
                    if (FileSearchTool.RetrieveSpectra(rawDataTypeName))
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
                                LogError("DtaGenResources.GetResources: " + mMessage);
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
                    mMessage = "Dataset type " + rawDataTypeName + " is not supported";
                    LogError("DtaGenResources.GetResources: " + mMessage +
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
            LogMessage("Retrieving data files required for MSAlign");

            // Retrieve the MSAlign_Quant parameter file
            var resultCode = RetrieveMSAlignQuantParameterFile();

            if (resultCode != CloseOutType.CLOSEOUT_SUCCESS)
                return resultCode;

            // Retrieve the MSAlign results for this job
            var msAlignResultsTableFile = DatasetName + MSALIGN_RESULT_TABLE_SUFFIX;

            if (!FileSearchTool.FindAndRetrieveMiscFiles(msAlignResultsTableFile, false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            mJobParams.AddResultFileToSkip(msAlignResultsTableFile);
            mJobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, MSALIGN_QUANT_INPUT_FILE_NAME_PARAM, msAlignResultsTableFile);

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetTopPICFiles()
        {
            LogMessage("Retrieving data files required for TopPIC");

            // Retrieve the MSAlign_Quant parameter file
            var resultCode = RetrieveMSAlignQuantParameterFile();

            if (resultCode != CloseOutType.CLOSEOUT_SUCCESS)
                return resultCode;

            // Retrieve the TopPIC results for this job

            var filesToGet = DatasetName + "*" + TOPPIC_RESULT_FILE_SUFFIX;

            if (!FileSearchTool.FindAndRetrieveMiscFiles(filesToGet, false))
            {
                // Errors were reported in method call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            var workingDirectory = new DirectoryInfo(mWorkDir);
            var topPICResultFileNames = new List<string>();

            foreach (var item in workingDirectory.GetFiles(filesToGet))
            {
                mJobParams.AddResultFileToSkip(item.Name);

                topPICResultFileNames.Add(item.Name);
            }

            // If there are multiple _TopPIC_PrSMs.txt files, separate them with a tab
            mJobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION, MSALIGN_QUANT_INPUT_FILE_NAME_PARAM,
                string.Join("\t", topPICResultFileNames));

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RetrieveMSAlignQuantParameterFile()
        {
            // For example, MSAlign_Quant_Workflow_2012-07-25
            // The parameter name is MSAlignQuantParamFile even when the step tool is TopPIC

            const string paramFileStoragePathKeyName = Global.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "MSAlign_Quant";
            var paramFileStoragePath = mMgrParams.GetParam(paramFileStoragePathKeyName);

            if (string.IsNullOrEmpty(paramFileStoragePath))
            {
                paramFileStoragePath = @"\\gigasax\DMS_Parameter_Files\DeconToolsWorkflows";
                LogWarning(
                    "Parameter '" + paramFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tool_Storage_Paths in the Broker DB); will assume: " + paramFileStoragePath);
            }

            var paramFileName = mJobParams.GetJobParameter("MSAlignQuantParamFile", string.Empty);

            if (string.IsNullOrEmpty(paramFileName))
            {
                mMessage = AnalysisToolRunnerBase.NotifyMissingParameter(mJobParams, "MSAlignQuantParamFile");
                LogError(mMessage);
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            if (FileSearchTool.RetrieveFile(paramFileName, paramFileStoragePath))
                return CloseOutType.CLOSEOUT_SUCCESS;

            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
        }
    }
}
