using System;
using AnalysisManagerBase;
using System.IO;

namespace AnalysisManager_Cyclops_PlugIn
{
    public class clsAnalysisResourcesCyclops : clsAnalysisResources
    {

        public static string AppFilePath = "";

        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {

            Directory.CreateDirectory(@"C:\DMS_WorkDir\R_Scripts");
            
            //Clear out list of files to delete or keep when packaging the blnSuccesss
            clsGlobal.ResetFilesToDeleteOrKeep();

            string dataPackageFolderPath = Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("OutputFolderName"));
            string analysisType = m_jobParams.GetParam("AnalysisType");
            //string analysisType = "SpectralCounting";

            if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, m_jobParams.GetParam("StepInputFolderName")), m_WorkingDir))
            {
                //Errors were reported in function call, so just return
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            string strInputFileExtension = string.Empty;

            //// Retrieve the Cyclops Workflow file specified for this job
            string strCyclopsWorkflowFileName = m_jobParams.GetParam("CyclopsWorkflowName");
            // Retrieve the Workflow file name specified for this job
            if (strCyclopsWorkflowFileName == null || strCyclopsWorkflowFileName.Length == 0)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Cyclops Workflow not defined in the job parameters for this job; unable to continue");
                return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }
            
            string strCyclopsWorkflowFileStoragePath = "\\\\gigasax\\DMS_Workflows\\Cyclops\\" + analysisType;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Copying FROM: " + "\\\\gigasax\\DMS_Workflows\\Cyclops\\" + analysisType);
            foreach(string s in Directory.GetFiles(strCyclopsWorkflowFileStoragePath, "*.R"))
            {

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Copying Files: " + Path.Combine(m_WorkingDir, "R_Scripts", Path.GetFileName(s)));
                File.Copy(s, Path.Combine(m_WorkingDir, "R_Scripts", Path.GetFileName(s)));
            }                

            //Now copy the Cyclops workflow file to the working directory
            if (!CopyFileToWorkDir(strCyclopsWorkflowFileName, strCyclopsWorkflowFileStoragePath, m_WorkingDir))
            {
                //Errors were reported in function call, so just return
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            if (m_DebugLevel >= 1)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving input files");
            }

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }

    }
}
