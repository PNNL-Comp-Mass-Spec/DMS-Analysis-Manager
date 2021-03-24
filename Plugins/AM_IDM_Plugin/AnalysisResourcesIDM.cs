using System;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManager_IDM_Plugin
{
    /// <summary>
    /// Retrieve resources for the IDM plugin
    /// </summary>
    public class AnalysisResourcesIDM : AnalysisResources
    {
        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            try
            {
                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                if (mDebugLevel >= 1)
                {
                    LogMessage("Retrieving input files");
                }

                var dataPackageFolderPath = Path.Combine(mJobParams.GetParam(JOB_PARAM_TRANSFER_FOLDER_PATH), mJobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME));

                if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, mJobParams.GetParam("StepInputFolderName")), mWorkDir))
                {
                    // Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var useExistingIDMResults = mJobParams.GetJobParameter("UseExistingIDMResults", false);

                if (useExistingIDMResults)
                {
                    var idmResultsDB = new FileInfo(Path.Combine(dataPackageFolderPath, mJobParams.GetParam("StepOutputFolderName"), "Results.db3"));
                    if (idmResultsDB.Exists)
                    {
                        var targetFilePath = Path.Combine(mWorkDir, AnalysisToolRunnerIDM.EXISTING_IDM_RESULTS_FILE_NAME);
                        idmResultsDB.CopyTo(targetFilePath);

                        mJobParams.AddResultFileToSkip(AnalysisToolRunnerIDM.EXISTING_IDM_RESULTS_FILE_NAME);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception retrieving resources", ex);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
