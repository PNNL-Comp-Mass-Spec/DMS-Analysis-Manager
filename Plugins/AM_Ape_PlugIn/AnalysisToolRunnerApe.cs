using PRISM.Logging;
using System;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManager_Ape_PlugIn
{
    /// <summary>
    /// Class for running Ape
    /// </summary>
    public class AnalysisToolRunnerApe : AnalysisToolRunnerBase
    {
        private const int PROGRESS_PCT_APE_START = 1;
        private const int PROGRESS_PCT_APE_DONE = 99;

        private string mCurrentApeTask = string.Empty;

        /// <summary>
        /// Primary entry point for running this tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Do the base class stuff
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the Ape version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining Ape version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mCurrentApeTask = "Running Ape";
                UpdateStatusRunning();

                LogMessage(mCurrentApeTask);

                // Change the name of the log file for the local log file to the plugin log filename
                var logFileName = Path.Combine(mWorkDir, "Ape_Log.txt");
                LogTools.ChangeLogFileBaseName(logFileName, appendDateToBaseName: false);

                bool processingSuccess;

                try
                {
                    mProgress = PROGRESS_PCT_APE_START;

                    processingSuccess = RunApe();

                    // Change the name of the log file back to the analysis manager log file
                    ResetLogFileNameToDefault();

                    if (!processingSuccess)
                    {
                        if (string.IsNullOrWhiteSpace(mMessage))
                            LogError("Error running Ape");
                        else
                            LogError("Error running Ape: " + mMessage);
                    }
                }
                catch (Exception ex)
                {
                    // Change the name of the log file back to the analysis manager log file
                    ResetLogFileNameToDefault();

                    LogError("Error running Ape: " + ex.Message);
                    processingSuccess = false;
                    mMessage = "Error running Ape";
                }

                // Stop the job timer
                mStopTime = DateTime.UtcNow;
                mProgress = PROGRESS_PCT_APE_DONE;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Override the output folder name and the dataset name (since this is a dataset aggregation job)
                mResultsDirectoryName = mJobParams.GetParam("StepOutputFolderName");
                mDatasetName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME);
                mJobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME, mResultsDirectoryName);

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in ApePlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Run the Ape pipeline(s) listed in "ApeOperations" parameter
        /// </summary>
        private bool RunApe()
        {
            // run the appropriate Mage pipeline(s) according to operations list parameter
            var apeOperations = mJobParams.GetParam("ApeOperations");

            var ops = new ApeAMOperations(mJobParams, mMgrParams);
            RegisterEvents(ops);

            var success = ops.RunApeOperations(apeOperations);

            if (!success)
                mMessage = "Error running ApeOperations: " + ops.ErrorMessage;

            return success;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "Ape");

            // Store paths to key DLLs
            var toolFiles = new System.Collections.Generic.List<FileInfo> {
                new("Ape.dll")
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message, ex);
                return false;
            }
        }
    }
}
