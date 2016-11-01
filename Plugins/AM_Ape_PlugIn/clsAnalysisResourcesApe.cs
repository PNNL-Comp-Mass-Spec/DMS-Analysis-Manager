using System.IO;
using AnalysisManagerBase;

namespace AnalysisManager_Ape_PlugIn
{
    public class clsAnalysisResourcesApe : clsAnalysisResources
    {
        public static string AppFilePath = "";

        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {
            bool blnSuccess = true;
            blnSuccess = RunApeGetResources();

            if (!blnSuccess) return IJobParams.CloseOutType.CLOSEOUT_FAILED;

            if (m_DebugLevel >= 1)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving input files");
            }

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }


        /// <summary>
        /// run the Ape pipeline(s) listed in "ApeOperations" parameter
        /// </summary>
        protected bool RunApeGetResources()
        {
            bool blnSuccess = false;

            string apeOperations = m_jobParams.GetParam("ApeOperations");

            if (string.IsNullOrWhiteSpace(apeOperations)) {
                m_message = "ApeOperations parameter is not defined";
                return false;
            }

            foreach (string apeOperation in apeOperations.Split(','))
            {
                if (!string.IsNullOrWhiteSpace(apeOperation)) {
                    blnSuccess = RunApeOperation(apeOperation.Trim());
                    if (!blnSuccess) {
                        if (string.IsNullOrEmpty(m_message))
                            m_message = "Error running Ape resources operation " + apeOperation;
                        break;
                    }
                }
            }

            return blnSuccess;

        }

        /// <summary>
        /// Run a single Ape operation
        /// </summary>
        /// <param name="apeOperation"></param>
        /// <returns></returns>
        private bool RunApeOperation(string apeOperation)
        {
            bool blnSuccess =  true;

            // Note: case statements must be lowercase
            switch (apeOperation.ToLower())
            {
                case "runworkflow":
                    blnSuccess = GetWorkflowFiles();
                    break;
                case "getimprovresults":
                    break;
                case "getviperresults":
                    break;
                case "getqrollupresults":
                    blnSuccess = GetQRollupFiles();
                    break;
                default:
                    // Future: throw an error
                    break;
            }
            return blnSuccess;
        }


        #region Ape Operations

        private bool GetWorkflowFiles()
        {
            bool blnSuccess = true;

            string dataPackageFolderPath = Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("OutputFolderName"));
            string analysisType = m_jobParams.GetParam("AnalysisType");

            string strStepInputFolderPath = Path.Combine(dataPackageFolderPath, m_jobParams.GetParam("StepInputFolderName"));
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving SQlite database: " + System.IO.Path.Combine(strStepInputFolderPath, "Results.db3"));
            if (!CopyFileToWorkDir("Results.db3", strStepInputFolderPath, m_WorkingDir))
            {
                //Errors were reported in function call, so just return
                return false;
            }

            string strInputFileExtension = string.Empty;

            //// Retrieve the Ape Workflow file specified for this job
            string strApeWorkflowFileName = m_jobParams.GetParam("ApeWorkflowName");
            // Retrieve the Workflow file name specified for this job
            if (string.IsNullOrEmpty(strApeWorkflowFileName))
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Parameter ApeWorkflowName not defined in the job parameters for this job; unable to continue");
                return false;
            }

            string strDMSWorkflowsFolderPath = m_mgrParams.GetParam("DMSWorkflowsFolderPath", @"\\gigasax\DMS_Workflows");
            string strApeWorkflowDirectory = Path.Combine(strDMSWorkflowsFolderPath, "Ape", analysisType);

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving workflow file: " + System.IO.Path.Combine(strApeWorkflowDirectory, strApeWorkflowFileName));

            // Now copy the Ape workflow file to the working directory
            if (!CopyFileToWorkDir(strApeWorkflowFileName, strApeWorkflowDirectory, m_WorkingDir))
            {
                //Errors were reported in function call, so just return
                return false;
            }

            return blnSuccess;
        }

        private bool GetQRollupFiles()
        {
            bool blnSuccess = true;

            string dataPackageFolderPath = Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("OutputFolderName"));

            if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, m_jobParams.GetParam("StepInputFolderName")), m_WorkingDir))
            {
                //Errors were reported in function call, so just return
                return false;
            }

            return blnSuccess;
        }


        #endregion



    }
}
