using System;
using AnalysisManagerBase;
using System.IO;

namespace AnalysisManager_IDP_PlugIn
{
    public class clsAnalysisResourcesIDP : clsAnalysisResources
    {
        public static string AppFilePath = "";

        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {

            try
            {

                if (m_DebugLevel >= 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving input files");
                }
                
                string dataPackageFolderPath = Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("OutputFolderName"));
                string analysisType = m_jobParams.GetParam("AnalysisType");

                if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, m_jobParams.GetParam("StepInputFolderName")), m_WorkingDir))
                {
                    m_message = "Results.db3 file from Step2 failed to copy over to working directory";
                    //Errors were reported in function call, so just return
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                string strInputFileExtension = string.Empty;

                // Retrieve the Cyclops Workflow file specified for this job
                string strIDPsWorkflowFileName = m_jobParams.GetParam("CyclopsWorkflowName");

                // Retrieve the Workflow file name specified for this job
                if (string.IsNullOrEmpty(strIDPsWorkflowFileName))
                {
                    m_message = "IDP Workflow not defined in the job parameters for this job";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; unable to continue");
                    return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                System.IO.DirectoryInfo diRemoteIDPScriptFolder;

                string strIDPsWorkflowFileStoragePath = "\\\\gigasax\\DMS_Workflows\\IDP\\" + analysisType;
                diRemoteIDPScriptFolder = new System.IO.DirectoryInfo(strIDPsWorkflowFileStoragePath);
                
                //Now copy the IDP workflow file to the working directory
                if (!CopyFileToWorkDir(strIDPsWorkflowFileStoragePath, diRemoteIDPScriptFolder.FullName, m_WorkingDir))
                {
                    //Errors were reported in function call, so just return
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception retrieving resources", ex);
            }

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
