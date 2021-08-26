using System;
using AnalysisManagerBase;
using System.IO;

namespace AnalysisManager_IDP_PlugIn
{
    /// <summary>
    /// Retrieve resources for the IDP plugin
    /// </summary>
    public class AnalysisResourcesIDP : AnalysisResources
    {
        public static string AppFilePath = "";

        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {

            try
            {

                if (m_DebugLevel >= 1)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, LogTools.LogLevels.INFO, "Retrieving input files");
                }

                string dataPackageFolderPath = Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("OutputFolderName"));
                string analysisType = m_jobParams.GetParam("AnalysisType");

                if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, m_jobParams.GetParam("StepInputFolderName")), m_WorkingDir))
                {
                    m_message = "Results.db3 file from Step2 failed to copy over to working directory";
                    //Errors were reported in function call, so just return
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                string inputFileExtension = string.Empty;

                // Retrieve the Cyclops Workflow file specified for this job
                string cyclopsWorkflowFileName = m_jobParams.GetParam("CyclopsWorkflowName");

                // Retrieve the Workflow file name specified for this job
                if (string.IsNullOrEmpty(cyclopsWorkflowFileName))
                {
                    m_message = "Parameter CyclopsWorkflowName not defined in the job parameters for this job";
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, LogTools.LogLevels.ERROR, m_message + "; unable to continue");
                    return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                string dmsworkflowsFolderPath = m_mgrParams.GetParam("DMSWorkflowsFolderPath", @"\\gigasax\DMS_Workflows");
                string cyclopsWorkflowDirectory = Path.Combine(dmsworkflowsFolderPath, "Cyclops", analysisType);

                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, LogTools.LogLevels.INFO, "Retrieving workflow file: " + System.IO.Path.Combine(cyclopsWorkflowDirectory, cyclopsWorkflowFileName));

                // Now copy the Cyclops workflow file to the working directory
                if (!CopyFileToWorkDir(cyclopsWorkflowFileName, cyclopsWorkflowDirectory, m_WorkingDir))
                {
                    //Errors were reported in function call, so just return
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, LogTools.LogLevels.ERROR, "Exception retrieving resources", ex);
            }

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
