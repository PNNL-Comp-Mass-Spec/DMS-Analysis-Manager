using System;
using AnalysisManagerBase;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISM.Logging;

namespace AnalysisManager_IDP_PlugIn
{
    /// <summary>
    /// Retrieve resources for the IDP plugin
    /// </summary>
    public class AnalysisResourcesIDP : AnalysisResources
    {
        public static string AppFilePath = "";

        public override AnalysisManagerBase.JobConfig.CloseOutType GetResources()
        {

            try
            {

                if (mDebugLevel >= 1)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, "Retrieving input files");
                }

                string dataPackageFolderPath = Path.Combine(mJobParams.GetParam("transferFolderPath"), mJobParams.GetParam("OutputFolderName"));
                string analysisType = mJobParams.GetParam("AnalysisType");

                if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, mJobParams.GetParam("StepInputFolderName")), mWorkDir))
                {
                    mMessage = "Results.db3 file from Step2 failed to copy over to working directory";
                    //Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                string inputFileExtension = string.Empty;

                // Retrieve the Cyclops Workflow file specified for this job
                string cyclopsWorkflowFileName = mJobParams.GetParam("CyclopsWorkflowName");

                // Retrieve the Workflow file name specified for this job
                if (string.IsNullOrEmpty(cyclopsWorkflowFileName))
                {
                    mMessage = "Parameter CyclopsWorkflowName not defined in the job parameters for this job";
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mMessage + "; unable to continue");
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                string dmsworkflowsFolderPath = mMgrParams.GetParam("DMSWorkflowsFolderPath", @"\\gigasax\DMS_Workflows");
                string cyclopsWorkflowDirectory = Path.Combine(dmsworkflowsFolderPath, "Cyclops", analysisType);

                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, "Retrieving workflow file: " + System.IO.Path.Combine(cyclopsWorkflowDirectory, cyclopsWorkflowFileName));

                // Now copy the Cyclops workflow file to the working directory
                if (!CopyFileToWorkDir(cyclopsWorkflowFileName, cyclopsWorkflowDirectory, mWorkDir))
                {
                    //Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception retrieving resources", ex);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
