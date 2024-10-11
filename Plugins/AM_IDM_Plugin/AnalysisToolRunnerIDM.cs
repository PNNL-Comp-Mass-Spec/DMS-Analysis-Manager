using System;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;
using InterDetect;
using PRISM;
using PRISM.Logging;

namespace AnalysisManager_IDM_Plugin
{
    /// <summary>
    /// Class for running the IDM utility
    /// </summary>
    internal class AnalysisToolRunnerIDM : AnalysisToolRunnerBase
    {
        public const string EXISTING_IDM_RESULTS_FILE_NAME = "ExistingIDMResults.db3";

        /// <summary>
        /// Primary entry point for running this tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                var skipIDM = false;

                // Do the base class stuff
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var idmResultsDB = new FileInfo(Path.Combine(mWorkDir, EXISTING_IDM_RESULTS_FILE_NAME));

                if (idmResultsDB.Exists)
                {
                    // Existing results file was copied to the working directory
                    // Copy the t_precursor_interference table into Results.db3

                    try
                    {
                        var sqLiteUtils = new SqLiteUtilities();

                        if (mDebugLevel >= 1)
                        {
                            LogMessage("Copying table t_precursor_interference from " + idmResultsDB.Name + " to Results.db3");
                        }

                        var cloneSuccess = sqLiteUtils.CloneDB(idmResultsDB.FullName, Path.Combine(mWorkDir, "Results.db3"), appendToExistingDB: true);

                        if (cloneSuccess)
                            skipIDM = true;

                        // success = sqLiteUtils.CopySqliteTable(idmResultsDB.FullName, "t_precursor_interference", Path.Combine(mWorkDir, "Results.db3"));

                    }
                    catch (Exception ex)
                    {
                        LogError(ex.Message + "; will run IDM instead of using existing results");
                    }
                }

                var processingSuccess = false;

                if (!skipIDM)
                {
                    LogMessage("Running IDM");

                    // Store the Version info in the database
                    StoreToolVersionInfo();

                    mProgress = 0;
                    UpdateStatusFile();

                    if (mDebugLevel > 4)
                    {
                        LogDebug("AnalysisToolRunnerIDM.RunTool(): Enter");
                    }

                    // Change the name of the log file for the local log file to the plugin log filename
                    var logFileName = Path.Combine(mWorkDir, "IDM_Log.txt");
                    LogTools.ChangeLogFileBaseName(logFileName, appendDateToBaseName: false);

                    try
                    {
                        // Create in instance of IDM and run the tool
                        var idm = new InterferenceDetector();

                        // Attach the progress event
                        idm.ProgressChanged += InterferenceDetectorProgressHandler;

                        idm.WorkDir = mWorkDir;

                        processingSuccess = idm.Run(mWorkDir, "Results.db3");

                        // Change the name of the log file back to the default
                        ResetLogFileNameToDefault();
                    }
                    catch (Exception ex)
                    {
                        // Change the name of the log file back to the default
                        ResetLogFileNameToDefault();

                        LogError("Error running IDM: " + ex.Message);
                        processingSuccess = false;
                    }
                }

                // Stop the job timer
                mStopTime = DateTime.UtcNow;
                mProgress = 100;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                AppUtils.GarbageCollectNow();

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
                mMessage = "Error in IDMPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void InterferenceDetectorProgressHandler(InterferenceDetector id, ProgressInfo e)
        {
            mProgress = e.Value;

            UpdateStatusFile(mProgress, 60);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private void StoreToolVersionInfo()
        {
            var idmDLL = Path.Combine(Global.GetAppDirectoryPath(), "InterDetect.dll");

            StoreDotNETToolVersionInfo(idmDLL, true);
        }
    }
}
