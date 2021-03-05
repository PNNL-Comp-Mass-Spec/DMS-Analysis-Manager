using AnalysisManagerBase;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.IO;

namespace AnalysisManagerMultiAlign_AggregatorPlugIn
{
    /// <summary>
    /// Class for running the MultiAlign Aggregator
    /// </summary>
    public class AnalysisToolRunnerMultiAlignAggregator : AnalysisToolRunnerBase
    {
        protected const float PROGRESS_PCT_MULTIALIGN_START = 1;
        protected const float PROGRESS_PCT_MULTIALIGN_DONE = 99;

        protected string mCurrentMultiAlignTask = string.Empty;
        protected DateTime mLastStatusUpdateTime;

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

                LogMessage("Running MultiAlign Aggregator");

                // Determine the path to the MultiAlign directory
                var progLoc = DetermineProgramLocation("MultiAlignProgLoc", "MultiAlignConsole.exe");

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the MultiAlign version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining MultiAlign version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mCurrentMultiAlignTask = "Running MultiAlign";
                mLastStatusUpdateTime = DateTime.UtcNow;
                UpdateStatusRunning(mProgress);

                LogMessage(mCurrentMultiAlignTask);

                // Change the name of the log file for the local log file to the plugin log filename
                var logFileName = Path.Combine(mWorkDir, "MultiAlign_Log.txt");
                LogTools.ChangeLogFileBaseName(logFileName, appendDateToBaseName: false);

                bool processingSuccess;

                try
                {
                    mProgress = PROGRESS_PCT_MULTIALIGN_START;
                    processingSuccess = RunMultiAlign(progLoc);
                }
                catch (Exception ex)
                {
                    mMessage = "Error running MultiAlign: " + ex.Message;
                    processingSuccess = false;
                }

                // Change the name of the log file back to the analysis manager log file
                ResetLogFileNameToDefault();

                if (processingSuccess)
                {
                    LogMessage("MultiAlign complete");
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                        mMessage = "Unknown error running MultiAlign";
                    else
                        mMessage = "Error running MultiAlign: " + mMessage;

                    LogError(mMessage);
                }

                // Stop the job timer
                mStopTime = DateTime.UtcNow;
                mProgress = PROGRESS_PCT_MULTIALIGN_DONE;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.ProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result directory,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Override the output directory name and the dataset name (since this is a dataset aggregation job)
                mResultsDirectoryName = mJobParams.GetParam("StepOutputFolderName");
                mDatasetName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME);
                mJobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME, mResultsDirectoryName);

                var resultsDirectoryCreated = MakeResultsDirectory();
                if (!resultsDirectoryCreated)
                {
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Move the Plots directory to the result files directory
                var plotsDirectory = new DirectoryInfo(Path.Combine(mWorkDir, "Plots"));

                if (plotsDirectory.Exists)
                {
                    var targetDirectoryPath = Path.Combine(Path.Combine(mWorkDir, mResultsDirectoryName), "Plots");

                    try
                    {
                        plotsDirectory.MoveTo(targetDirectoryPath);
                    }
                    catch (Exception ex)
                    {
                        LogWarning("Exception moving Plots directory " + plotsDirectory.FullName + ": " + ex.Message);
                        mFileTools.CopyDirectory(plotsDirectory.FullName, targetDirectoryPath, true);
                    }

                    // Zip up (then delete) the PNG files in the plots directory
                    ZipPlotsDirectory(plotsDirectory);
                }

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in MultiAlignPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Run the MultiAlign pipeline(s) listed in "MultiAlignOperations" parameter
        /// </summary>
        protected bool RunMultiAlign(string sMultiAlignConsolePath)
        {
            bool bSuccess;

            try
            {
                var oMultiAlignMage = new MultiAlignMage(mJobParams, mMgrParams, mStatusTools);
                RegisterEvents(oMultiAlignMage);

                bSuccess = oMultiAlignMage.Run(sMultiAlignConsolePath);

                if (!bSuccess)
                {
                    if (!string.IsNullOrWhiteSpace(oMultiAlignMage.Message))
                        mMessage = oMultiAlignMage.Message;
                    else
                        mMessage = "Unknown error running MultiAlign";
                }
            }
            catch (Exception ex)
            {
                mMessage = "Unknown error running MultiAlign: " + ex.Message;
                LogError(mMessage);
                return false;
            }

            return bSuccess;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        protected bool StoreToolVersionInfo(string strMultiAlignProgLoc)
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var ioMultiAlignInfo = new FileInfo(strMultiAlignProgLoc);
            if (!ioMultiAlignInfo.Exists)
            {
                try
                {
                    toolVersionInfo = "Unknown";
                    SetStepTaskToolVersion(toolVersionInfo, new List<FileInfo>());
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                    return false;
                }

                return false;
            }

            // Lookup the version of the Feature Finder
            var blnSuccess = mToolVersionUtilities.StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, ioMultiAlignInfo.FullName);
            if (!blnSuccess)
                return false;

            // Store paths to key DLLs in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new(strMultiAlignProgLoc)
            };

            if (ioMultiAlignInfo.DirectoryName != null)
            {
                // Lookup the version of MultiAlignEngine (in the MultiAlign directory)
                var strMultiAlignEngineDllLoc = Path.Combine(ioMultiAlignInfo.DirectoryName, "MultiAlignEngine.dll");
                blnSuccess = mToolVersionUtilities.StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, strMultiAlignEngineDllLoc);
                if (!blnSuccess)
                    return false;

                // Lookup the version of MultiAlignCore (in the MultiAlign directory)
                var strMultiAlignCoreDllLoc = Path.Combine(ioMultiAlignInfo.DirectoryName, "MultiAlignCore.dll");
                blnSuccess = mToolVersionUtilities.StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, strMultiAlignCoreDllLoc);
                if (!blnSuccess)
                    return false;

                toolFiles.Add(new FileInfo(strMultiAlignEngineDllLoc));
                toolFiles.Add(new FileInfo(strMultiAlignCoreDllLoc));
            }

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Zip the files in the Plots directory if there are over fileCountThreshold files (default 50, minimum 10)
        /// </summary>
        /// <param name="plotsDirectory"></param>
        /// <param name="fileCountThreshold"></param>
        private bool ZipPlotsDirectory(DirectoryInfo plotsDirectory, int fileCountThreshold = 50)
        {
            try
            {
                if (fileCountThreshold < 10)
                    fileCountThreshold = 10;

                plotsDirectory.Refresh();
                if (plotsDirectory.Exists)
                {
                    var pngFileCount = plotsDirectory.GetFiles("*.png").Length;

                    if (pngFileCount == 0)
                    {
                        LogWarning("No .PNG files were found in the Plots directory");
                        return true;
                    }

                    if (pngFileCount == 1)
                    {
                        if (mDebugLevel >= 2)
                            LogMessage("Only 1 .PNG file exists in the Plots directory; file will not be zipped");
                        return true;
                    }

                    if (pngFileCount < fileCountThreshold)
                    {
                        if (mDebugLevel >= 2)
                            LogMessage("Only " + pngFileCount + " .PNG files exist in the Plots directory; files will not be zipped");
                        return true;
                    }

                    var strZipFilePath = Path.Combine(plotsDirectory.FullName, "PlotFiles.zip");

                    var success = mDotNetZipTools.ZipDirectory(plotsDirectory.FullName, strZipFilePath, false, "*.png");

                    if (!success)
                    {
                        const string msg = "Error zipping the plot files";
                        if (!string.IsNullOrEmpty(mDotNetZipTools.Message))
                            LogError(msg + ": " + mDotNetZipTools.Message);
                        else
                            LogError(msg);

                        return false;
                    }

                    // Delete the PNG files in the Plots directory
                    var errorCount = 0;
                    foreach (var fiFile in plotsDirectory.GetFiles("*.png"))
                    {
                        try
                        {
                            fiFile.Delete();
                        }
                        catch (Exception ex)
                        {
                            errorCount++;
                            if (errorCount < 10)
                                LogError("Exception deleting file " + fiFile.Name + ": " + ex.Message);
                            else if (errorCount == 10)
                                LogError("Over 10 exceptions deleting plot files; additional exceptions will not be logged");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception zipping plot files", ex);
                return false;
            }

            return true;
        }
    }
}


