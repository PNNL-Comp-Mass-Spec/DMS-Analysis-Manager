using System;
using AnalysisManagerBase;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManager_Ape_PlugIn
{
    /// <summary>
    /// Retrieve resources for the Ape plugin
    /// </summary>
    public class AnalysisResourcesApe : AnalysisResources
    {
        // Ignore Spelling: QRollup, Improv, Workflows

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

            if (!success)
                return CloseOutType.CLOSEOUT_FAILED;

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
            // ReSharper disable once IdentifierTypo
            const string ITRAQ_ANALYSIS_TYPE = "iTRAQ";

            var dataPackagePath = Path.Combine(mJobParams.GetParam(JOB_PARAM_TRANSFER_FOLDER_PATH), mJobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME));
            var analysisType = mJobParams.GetParam("AnalysisType");

            if (analysisType.IndexOf("TMT", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // The workflow file for TMT jobs is in the iTRAQ subdirectory below \\gigasax\DMS_Workflows\Ape
                // Override the analysis type
                LogDebugMessage(string.Format("Changing analysis type from {0} to {1}", analysisType, ITRAQ_ANALYSIS_TYPE));
                analysisType = ITRAQ_ANALYSIS_TYPE;
            }
            else if (analysisType.IndexOf(ITRAQ_ANALYSIS_TYPE, StringComparison.OrdinalIgnoreCase) >= 0 && analysisType.Length > 5)
            {
                // The user likely specified iTRAQ8
                // Override to be simply iTRAQ
                LogDebugMessage(string.Format("Changing analysis type from {0} to {1}", analysisType, ITRAQ_ANALYSIS_TYPE));
                analysisType = ITRAQ_ANALYSIS_TYPE;
            }

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
