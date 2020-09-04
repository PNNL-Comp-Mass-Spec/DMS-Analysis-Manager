using System;
using AnalysisManagerBase;
using System.IO;

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
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            var success = RunApeGetResources();

            if (!success) return CloseOutType.CLOSEOUT_FAILED;

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
            var success = false;

            var apeOperations = mJobParams.GetParam("ApeOperations");

            if (string.IsNullOrWhiteSpace(apeOperations))
            {
                mMessage = "ApeOperations parameter is not defined";
                return false;
            }

            foreach (var apeOperation in apeOperations.Split(','))
            {
                if (string.IsNullOrWhiteSpace(apeOperation))
                    continue;

                success = RunApeOperation(apeOperation.Trim());
                if (success)
                    continue;

                if (string.IsNullOrEmpty(mMessage))
                    mMessage = "Error running Ape resources operation " + apeOperation;

                break;
            }

            return success;
        }

        /// <summary>
        /// Run a single Ape operation
        /// </summary>
        /// <param name="apeOperation"></param>
        /// <returns></returns>
        private bool RunApeOperation(string apeOperation)
        {
            if (apeOperation.Equals("RunWorkFlow", StringComparison.OrdinalIgnoreCase))
            {
                var success = GetWorkflowFiles();
                return success;
            }

            if (apeOperation.Equals("GetQRollupResults", StringComparison.OrdinalIgnoreCase))
            {
                var success = GetQRollupFiles();
                return success;
            }

            // For other modes, simply return true
            //   GetImprovResults
            //   GetViperResults
            //   GetQRollupResults

            return true;
        }

        #region Ape Operations

        private bool GetWorkflowFiles()
        {
            var dataPackagePath = Path.Combine(mJobParams.GetParam(JOB_PARAM_TRANSFER_FOLDER_PATH), mJobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME));
            var analysisType = mJobParams.GetParam("AnalysisType");

            var stepInputDirectoryPath = Path.Combine(dataPackagePath, mJobParams.GetParam("StepInputFolderName"));
            LogMessage("Retrieving SQLite database: " + Path.Combine(stepInputDirectoryPath, "Results.db3"));
            if (!CopyFileToWorkDir("Results.db3", stepInputDirectoryPath, mWorkDir))
            {
                // Errors were reported in function call, so just return
                return false;
            }

            // Retrieve the Ape Workflow file specified for this job
            var apeWorkflowFileName = mJobParams.GetParam("ApeWorkflowName");
            // Retrieve the Workflow file name specified for this job
            if (string.IsNullOrEmpty(apeWorkflowFileName))
            {
                LogError("Parameter ApeWorkflowName not defined in the job parameters for this job; unable to continue");
                return false;
            }

            var dmsWorkflowsDirectoryPath = mMgrParams.GetParam("DMSWorkflowsFolderPath", @"\\gigasax\DMS_Workflows");
            var apeWorkflowDirectory = Path.Combine(dmsWorkflowsDirectoryPath, "Ape", analysisType);

            LogMessage("Retrieving workflow file: " + Path.Combine(apeWorkflowDirectory, apeWorkflowFileName));

            // Now copy the Ape workflow file to the working directory
            if (!CopyFileToWorkDir(apeWorkflowFileName, apeWorkflowDirectory, mWorkDir))
            {
                // Errors were reported in function call, so just return
                return false;
            }

            return true;
        }

        private bool GetQRollupFiles()
        {
            var dataPackagePath = Path.Combine(mJobParams.GetParam(JOB_PARAM_TRANSFER_FOLDER_PATH), mJobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME));

            if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackagePath, mJobParams.GetParam("StepInputFolderName")), mWorkDir))
            {
                // Errors were reported in function call, so just return
                return false;
            }

            return true;
        }

        #endregion

    }
}
