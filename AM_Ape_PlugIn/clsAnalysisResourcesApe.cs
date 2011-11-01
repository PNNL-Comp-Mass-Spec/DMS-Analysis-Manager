using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManager_Ape_PlugIn
{
    public class clsAnalysisResourcesApe : clsAnalysisResources
    {
        public static string AppFilePath = "";

        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {
            //Clear out list of files to delete or keep when packaging the blnSuccesss
            clsGlobal.ResetFilesToDeleteOrKeep();

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

            string mageOperations = m_jobParams.GetParam("ApeOperations");
            foreach (string mageOperation in mageOperations.Split(','))
            {
                blnSuccess = RunApeOperation(mageOperation.Trim());
                if (!blnSuccess) break;
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

            switch (apeOperation)
            {
                case "RunWorkflow":
                    blnSuccess = GetWorkflowFiles();
                    break;
                case "GetImprovResults":
                    break;
                case "GetViperResults":
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

            if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, m_jobParams.GetParam("StepInputFolderName")), m_WorkingDir))
            {
                //Errors were reported in function call, so just return
                return false;
            }

            string strInputFileExtension = string.Empty;

            //// Retrieve the Ape Workflow file specified for this job
            string strApeWorkflowFileName = m_jobParams.GetParam("ApeWorkflowName");
            // Retrieve the Workflow file name specified for this job
            if (strApeWorkflowFileName == null || strApeWorkflowFileName.Length == 0)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Ape Workflow not defined in the job parameters for this job; unable to continue");
                return false;
            }

            string strApeWorkflowFileStoragePath = "\\\\gigasax\\DMS_Workflows\\Ape\\" + analysisType;

            //Now copy the Ape workflow file to the working directory
            if (!CopyFileToWorkDir(strApeWorkflowFileName, strApeWorkflowFileStoragePath, m_WorkingDir))
            {
                //Errors were reported in function call, so just return
                return false;
            }

            return blnSuccess;
        }

        private bool GetImprovResults()
        {
            bool blnSuccess = true;

            return blnSuccess;
        }

        #endregion



    }
}
