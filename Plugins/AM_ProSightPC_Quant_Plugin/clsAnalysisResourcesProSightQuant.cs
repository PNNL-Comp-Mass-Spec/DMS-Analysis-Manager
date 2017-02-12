using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerProSightQuantPlugIn
{
    public class clsAnalysisResourcesProSightQuant : clsAnalysisResources
    {
        public const string PROSIGHT_PC_RESULT_FILE = "ProSightPC_Results.xls";

        public const bool TOOL_DISABLED = true;

        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            if (TOOL_DISABLED)
            {
                // This tool is currently disabled, so just return Success
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            // Retrieve the MSAlign_Quant parameter file
            // For example, MSAlign_Quant_Workflow_2012-07-25

            string strParamFileStoragePathKeyName = null;
            string strParamFileStoragePath = null;
            strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "MSAlign_Quant";

            strParamFileStoragePath = m_mgrParams.GetParam(strParamFileStoragePathKeyName);
            if (strParamFileStoragePath == null || strParamFileStoragePath.Length == 0)
            {
                strParamFileStoragePath = "\\\\gigasax\\DMS_Parameter_Files\\DeconToolsWorkflows";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                    "Parameter '" + strParamFileStoragePathKeyName +
                    "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " + strParamFileStoragePath);
            }

            string strParamFileName = m_jobParams.GetParam("ProSightQuantParamFile");
            if (string.IsNullOrEmpty(strParamFileName))
            {
                m_message = clsAnalysisToolRunnerBase.NotifyMissingParameter(m_jobParams, "ProSightQuantParamFile");
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Getting data files");

            if (!RetrieveFile(strParamFileName, strParamFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            // Retrieve the ProSightPC results for this job
            string strProSightPCResultsFile = null;
            strProSightPCResultsFile = PROSIGHT_PC_RESULT_FILE;
            if (!FindAndRetrieveMiscFiles(strProSightPCResultsFile, false))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }
            m_jobParams.AddResultFileToSkip(strProSightPCResultsFile);

            // Get the instrument data file
            string strRawDataType = m_jobParams.GetParam("RawDataType");

            switch (strRawDataType.ToLower())
            {
                case RAW_DATA_TYPE_DOT_RAW_FILES:
                case RAW_DATA_TYPE_BRUKER_FT_FOLDER:

                    if (RetrieveSpectra(strRawDataType))
                    {
                        if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                        {
                            return CloseOutType.CLOSEOUT_FAILED;
                        }

                        // Confirm that the .Raw or .D folder was actually copied locally
                        if (strRawDataType.ToLower() == RAW_DATA_TYPE_DOT_RAW_FILES)
                        {
                            if (!File.Exists(Path.Combine(m_WorkingDir, m_DatasetName + DOT_RAW_EXTENSION)))
                            {
                                m_message = "Thermo .Raw file not successfully copied to WorkDir; likely a timeout error";
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                    "clsDtaGenResources.GetResources: " + m_message);
                                return CloseOutType.CLOSEOUT_FAILED;
                            }
                            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION);  //Raw file
                        }
                        else if (strRawDataType.ToLower() == RAW_DATA_TYPE_BRUKER_FT_FOLDER)
                        {
                            if (!Directory.Exists(Path.Combine(m_WorkingDir, m_DatasetName + DOT_D_EXTENSION)))
                            {
                                m_message = "Bruker .D folder not successfully copied to WorkDir; likely a timeout error";
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                    "clsDtaGenResources.GetResources: " + m_message);
                                return CloseOutType.CLOSEOUT_FAILED;
                            }
                        }
                    }
                    else
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "clsDtaGenResources.GetResources: Error occurred retrieving spectra.");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                    break;
                default:
                    m_message = "Dataset type " + strRawDataType + " is not supported";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "clsDtaGenResources.GetResources: " + m_message + "; must be " + RAW_DATA_TYPE_DOT_RAW_FILES + " or " +
                        RAW_DATA_TYPE_BRUKER_FT_FOLDER);
                    return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
