using System;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;
using Mage;
using PRISM;
using PRISM.Logging;

namespace AnalysisManager_AScore_PlugIn
{
    /// <summary>
    /// Class for running ASCore
    /// </summary>
    public class AnalysisToolRunnerAScore : AnalysisToolRunnerBase
    {
        // Ignore Spelling: Mage

        private const int PROGRESS_PCT_ASCORE_START = 1;
        private const int PROGRESS_PCT_ASCORE_DONE = 99;

        private string mCurrentAScoreTask = string.Empty;

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

                // Make sure _dta.txt files are not copied to the server
                mJobParams.AddResultFileExtensionToSkip("_dta.txt");
                mJobParams.AddResultFileExtensionToSkip("extracted_results.txt");
                mJobParams.AddResultFileExtensionToSkip("AScoreFile.txt");

                // Store the AScore version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining AScore version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var ascoreParamFile = mJobParams.GetJobParameter("AScoreParamFilename", string.Empty);
                bool processingSuccess;

                if (string.IsNullOrEmpty(ascoreParamFile))
                {
                    mMessage = "Skipping AScore since AScoreParamFilename is not defined for this job";
                    processingSuccess = true;
                }
                else
                {
                    mCurrentAScoreTask = "Running AScore";
                    mStatusTools.UpdateAndWrite(MgrStatusCodes.RUNNING, TaskStatusCodes.RUNNING,
                                                 TaskStatusDetailCodes.RUNNING_TOOL, mProgress);

                    LogMessage(mCurrentAScoreTask);

                    // Change the name of the log file for the local log file to the plugin log filename
                    var logFileName = Path.Combine(mWorkDir, "Ascore_Log.txt");
                    LogTools.ChangeLogFileBaseName(logFileName, appendDateToBaseName: false);

                    try
                    {
                        mProgress = PROGRESS_PCT_ASCORE_START;

                        processingSuccess = RunAScore();

                        // Change the name of the log file back to the analysis manager log file
                        ResetLogFileNameToDefault();

                        if (!processingSuccess && !string.IsNullOrWhiteSpace(mMessage))
                        {
                            LogError("Error running AScore: " + mMessage);
                        }

                        if (processingSuccess)
                        {
                            // Export the AScore result table as a tab-delimited text file
                            var exportSuccess = ExportAScoreResults();

                            if (!exportSuccess)
                            {
                                mMessage = "Export of table t_results_ascore failed";
                                processingSuccess = false;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Change the name of the log file back to the analysis manager log file
                        ResetLogFileNameToDefault();

                        LogError("Error running AScore: " + ex.Message, ex);
                        processingSuccess = false;
                        mMessage = "Error running AScore";
                    }
                }

                // Stop the job timer
                mStopTime = DateTime.UtcNow;
                mProgress = PROGRESS_PCT_ASCORE_DONE;

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

                // Override the output directory name and the dataset name (since this is a dataset aggregation job)
                mResultsDirectoryName = mJobParams.GetParam("StepOutputFolderName");
                mDatasetName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME);
                mJobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME, mResultsDirectoryName);

                mJobParams.AddResultFileExtensionToSkip("_ModSummary.txt");

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in AScorePlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool ExportAScoreResults()
        {
            try
            {
                var sqlLiteDB = new FileInfo(Path.Combine(mWorkDir, "Results.db3"));

                if (!sqlLiteDB.Exists)
                {
                    mMessage = "Cannot export AScore results since Results.db3 does not exist";
                    LogError(mMessage);
                    return false;
                }

                var outputFile = new FileInfo(Path.Combine(mWorkDir, "AScore_Results.txt"));
                ExportTable(sqlLiteDB, "t_results_ascore", outputFile);

                outputFile = new FileInfo(Path.Combine(mWorkDir, "AScore_Job_Map.txt"));
                ExportTable(sqlLiteDB, "t_results_file_list_metadata", outputFile);
            }
            catch (Exception ex)
            {
                mMessage = "Error in AScorePlugin->ExportAScoreResults";
                LogError(mMessage, ex);
                return false;
            }

            return true;
        }

        private void ExportTable(FileSystemInfo sqlLiteDB, string tableName, FileSystemInfo targetFile)
        {
            var reader = new SQLiteReader
            {
                Database = sqlLiteDB.FullName,
                SQLText = "SELECT * FROM [" + tableName + "]"
            };

            var writer = new DelimitedFileWriter
            {
                FilePath = targetFile.FullName,
                Delimiter = "\t"
            };

            LogMessage("Exporting table t_results_ascore to " + Path.GetFileName(writer.FilePath));

            var pipeline = ProcessingPipeline.Assemble("ExportTable", reader, writer);
            pipeline.RunRoot(null);
        }

        /// <summary>
        /// Run the AScore pipeline(s) listed in "AScoreOperations" parameter
        /// </summary>
        private bool RunAScore()
        {
            // Run the appropriate Mage pipeline(s) according to operations list parameter
            var ascoreMage = new AScoreMagePipeline(mJobParams, mMgrParams, mZipTools);
            RegisterEvents(ascoreMage);

            var success = ascoreMage.Run();

            // Delete any PeptideToProteinMapEngine_log files
            var workingDirectory = new DirectoryInfo(mWorkDir);
            var matchingFiles = workingDirectory.GetFiles("PeptideToProteinMapEngine_log*");

            if (matchingFiles.Length > 0)
            {
                foreach (var logFile in matchingFiles)
                {
                    try
                    {
                        DeleteFileWithRetries(logFile.FullName, 1, 2);
                    }
                    // ReSharper disable once EmptyGeneralCatchClause
                    catch (Exception)
                    {
                        // Ignore errors here
                    }
                }
            }

            foreach (var filename in ascoreMage.GetTempFileNames())
            {
                mJobParams.AddResultFileToSkip(filename);
            }

            if (!string.IsNullOrEmpty(ascoreMage.ErrorMessage))
            {
                mMessage = ascoreMage.ErrorMessage;
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            var ascoreDll = Path.Combine(Global.GetAppDirectoryPath(), "AScore_DLL.dll");
            var success = StoreDotNETToolVersionInfo(ascoreDll, false);

            return success;
        }
    }
}
