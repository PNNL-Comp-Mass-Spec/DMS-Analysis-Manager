using System.IO;
using System.Threading;
using AnalysisManagerBase;
using System;
using Mage;
using PRISM;
using PRISM.Logging;

namespace AnalysisManager_AScore_PlugIn
{
    /// <summary>
    /// Class for running ASCore
    /// </summary>
    public class clsAnalysisToolRunnerAScore : clsAnalysisToolRunnerBase
    {
        private const float PROGRESS_PCT_ASCORE_START = 1;
        private const float PROGRESS_PCT_ASCORE_DONE = 99;

        private string m_CurrentAScoreTask = string.Empty;

        /// <summary>
        /// Primary entry point for running this tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
        public override CloseOutType RunTool()
        {
            try
            {

                m_jobParams.SetParam("JobParameters", "DatasetNum", m_jobParams.GetParam("OutputFolderPath"));

                // Do the base class stuff
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Make sure _dta.txt files are not copied to the server
                m_jobParams.AddResultFileExtensionToSkip("_dta.txt");
                m_jobParams.AddResultFileExtensionToSkip("extracted_results.txt");
                m_jobParams.AddResultFileExtensionToSkip("AScoreFile.txt");

                // Store the AScore version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining AScore version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var ascoreParamFile = m_jobParams.GetJobParameter("AScoreParamFilename", string.Empty);
                bool processingSuccess;

                if (string.IsNullOrEmpty(ascoreParamFile))
                {
                    m_message = "Skipping AScore since AScoreParamFilename is not defined for this job";
                    processingSuccess = true;
                }
                else
                {

                    m_CurrentAScoreTask = "Running AScore";
                    m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING,
                                                 EnumTaskStatusDetail.RUNNING_TOOL, m_progress);

                    LogMessage(m_CurrentAScoreTask);

                    // Change the name of the log file for the local log file to the plugin log filename
                    var logFileName = Path.Combine(m_WorkDir, "Ascore_Log.txt");
                    LogTools.ChangeLogFileBaseName(logFileName, appendDateToBaseName: false);

                    try
                    {
                        m_progress = PROGRESS_PCT_ASCORE_START;

                        processingSuccess = RunAScore();

                        // Change the name of the log file back to the analysis manager log file
                        ResetLogFileNameToDefault();

                        if (!processingSuccess && !string.IsNullOrWhiteSpace(m_message))
                        {
                            LogError("Error running AScore: " + m_message);
                        }

                        if (processingSuccess)
                        {
                            // Export the AScore result table as a tab-delimited text file
                            var exportSuccess = ExportAScoreResults();
                            if (!exportSuccess)
                            {
                                m_message = "Export of table t_results_ascore failed";
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
                        m_message = "Error running AScore";
                    }
                }

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;
                m_progress = PROGRESS_PCT_ASCORE_DONE;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                Thread.Sleep(500);
                clsProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Override the output folder name and the dataset name (since this is a dataset aggregation job)
                m_ResFolderName = m_jobParams.GetParam("StepOutputFolderName");
                m_Dataset = m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME);
                m_jobParams.SetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME, m_ResFolderName);

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Error in AScorePlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;

            }


        }

        private bool ExportAScoreResults()
        {
            try
            {
                var sqlLiteDB = new FileInfo(Path.Combine(m_WorkDir, "Results.db3"));
                if (!sqlLiteDB.Exists)
                {
                    m_message = "Cannot export AScore results since Results.db3 does not exist";
                    LogError(m_message);
                    return false;
                }

                var outputFile = new FileInfo(Path.Combine(m_WorkDir, "AScore_Results.txt"));
                ExportTable(sqlLiteDB, "t_results_ascore", outputFile);

                outputFile = new FileInfo(Path.Combine(m_WorkDir, "AScore_Job_Map.txt"));
                ExportTable(sqlLiteDB, "t_results_file_list_metadata", outputFile);

            }
            catch (Exception ex)
            {
                m_message = "Error in AScorePlugin->ExportAScoreResults";
                LogError(m_message, ex);
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

            var msg = "Exporting table t_results_ascore to " + Path.GetFileName(writer.FilePath);
            LogMessage(msg);

            var pipeline = ProcessingPipeline.Assemble("ExportTable", reader, writer);
            pipeline.RunRoot(null);

        }

        /// <summary>
        /// Run the AScore pipeline(s) listed in "AScoreOperations" parameter
        /// </summary>
        private bool RunAScore()
        {
            // run the appropriate Mage pipeline(s) according to operations list parameter
            var ascoreMage = new clsAScoreMagePipeline(m_jobParams, m_mgrParams, m_DotNetZipTools);
            RegisterEvents(ascoreMage);

            var success = ascoreMage.Run();

            // Delete any PeptideToProteinMapEngine_log files
            var diWorkDir = new DirectoryInfo(m_WorkDir);
            var fiFiles = diWorkDir.GetFiles("PeptideToProteinMapEngine_log*");
            if (fiFiles.Length > 0)
            {
                foreach (var fiFile in fiFiles)
                {
                    try
                    {
                        DeleteFileWithRetries(fiFile.FullName, 1, 2);
                    }
                    // ReSharper disable once EmptyGeneralCatchClause
                    catch (Exception)
                    {
                        // Igore errors here
                    }

                }
            }

            foreach (var filename in ascoreMage.GetTempFileNames())
            {
                m_jobParams.AddResultFileToSkip(filename);
            }

            if (!string.IsNullOrEmpty(ascoreMage.ErrorMessage))
            {
                m_message = ascoreMage.ErrorMessage;
                success = false;
            }

            return success;

        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            var cyclopsDll = Path.Combine(clsGlobal.GetAppFolderPath(), "AScore_DLL.dll");
            var success = StoreDotNETToolVersionInfo(cyclopsDll);

            return success;
        }


    }
}


