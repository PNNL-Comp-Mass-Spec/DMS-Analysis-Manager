using System.IO;
using AnalysisManagerBase;

namespace AnalysisManager_Ape_PlugIn
{
    /// <summary>
    /// Retrieve resources for the Ape plugin
    /// </summary>
    public class clsAnalysisResourcesApe : clsAnalysisResources
    {
        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS) {
                return result;
            }

            var blnSuccess = RunApeGetResources();

            if (!blnSuccess) return CloseOutType.CLOSEOUT_FAILED;

            if (mDebugLevel >= 1)
            {
                LogMessage("Retrieving input files");
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// run the Ape pipeline(s) listed in "ApeOperations" parameter
        /// </summary>
        protected bool RunApeGetResources()
        {
            var blnSuccess = false;

            var apeOperations = mJobParams.GetParam("ApeOperations");

            if (string.IsNullOrWhiteSpace(apeOperations)) {
                mMessage = "ApeOperations parameter is not defined";
                return false;
            }

            foreach (var apeOperation in apeOperations.Split(','))
            {
                if (!string.IsNullOrWhiteSpace(apeOperation)) {
                    blnSuccess = RunApeOperation(apeOperation.Trim());
                    if (!blnSuccess) {
                        if (string.IsNullOrEmpty(mMessage))
                            mMessage = "Error running Ape resources operation " + apeOperation;
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
            var blnSuccess =  true;

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
            var dataPackageFolderPath = Path.Combine(mJobParams.GetParam(JOB_PARAM_TRANSFER_FOLDER_PATH), mJobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME));
            var analysisType = mJobParams.GetParam("AnalysisType");

            var strStepInputFolderPath = Path.Combine(dataPackageFolderPath, mJobParams.GetParam("StepInputFolderName"));
            LogMessage("Retrieving SQlite database: " + Path.Combine(strStepInputFolderPath, "Results.db3"));
            if (!CopyFileToWorkDir("Results.db3", strStepInputFolderPath, mWorkDir))
            {
                // Errors were reported in function call, so just return
                return false;
            }

            // Retrieve the Ape Workflow file specified for this job
            var strApeWorkflowFileName = mJobParams.GetParam("ApeWorkflowName");
            // Retrieve the Workflow file name specified for this job
            if (string.IsNullOrEmpty(strApeWorkflowFileName))
            {
                LogError("Parameter ApeWorkflowName not defined in the job parameters for this job; unable to continue");
                return false;
            }

            var strDMSWorkflowsFolderPath = mMgrParams.GetParam("DMSWorkflowsFolderPath", @"\\gigasax\DMS_Workflows");
            var strApeWorkflowDirectory = Path.Combine(strDMSWorkflowsFolderPath, "Ape", analysisType);

            LogMessage("Retrieving workflow file: " + Path.Combine(strApeWorkflowDirectory, strApeWorkflowFileName));

            // Now copy the Ape workflow file to the working directory
            if (!CopyFileToWorkDir(strApeWorkflowFileName, strApeWorkflowDirectory, mWorkDir))
            {
                // Errors were reported in function call, so just return
                return false;
            }

            return true;
        }

        private bool GetQRollupFiles()
        {

            var dataPackageFolderPath = Path.Combine(mJobParams.GetParam(JOB_PARAM_TRANSFER_FOLDER_PATH), mJobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME));

            if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, mJobParams.GetParam("StepInputFolderName")), mWorkDir))
            {
                // Errors were reported in function call, so just return
                return false;
            }

            return true;
        }


        #endregion



    }
}
