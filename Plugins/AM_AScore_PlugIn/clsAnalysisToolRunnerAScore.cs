using System.IO;
using System.Threading;
using AnalysisManagerBase;
using System;
using log4net;
using System.Collections.Generic;
using Mage;
using PRISM;

namespace AnalysisManager_AScore_PlugIn
{
    public class clsAnalysisToolRunnerAScore : clsAnalysisToolRunnerBase
    {
        protected const float PROGRESS_PCT_ASCORE_START = 1;
        protected const float PROGRESS_PCT_ASCORE_DONE = 99;

        protected string m_CurrentAScoreTask = string.Empty;
        protected DateTime m_LastStatusUpdateTime;

        public override CloseOutType RunTool()
        {
            try
            {

                m_jobParams.SetParam("JobParameters", "DatasetNum", m_jobParams.GetParam("OutputFolderPath"));
                bool success;

                //Do the base class stuff
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
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining AScore version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var ascoreParamFile = m_jobParams.GetJobParameter("AScoreParamFilename", string.Empty);

                if (string.IsNullOrEmpty(ascoreParamFile))
                {
                    m_message = "Skipping AScore since AScoreParamFilename is not defined for this job";
                    success = true;
                }
                else
                {
                    
                    m_CurrentAScoreTask = "Running AScore";
                    m_LastStatusUpdateTime = DateTime.UtcNow;
                    m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING,
                                                 EnumTaskStatusDetail.RUNNING_TOOL, m_progress);

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_CurrentAScoreTask);

                    //Change the name of the log file for the local log file to the plugin log filename
                    String LogFileName = Path.Combine(m_WorkDir, "Ascore_Log");
                    GlobalContext.Properties["LogName"] = LogFileName;
                    clsLogTools.ChangeLogFileName(LogFileName);

                    try
                    {
                        m_progress = PROGRESS_PCT_ASCORE_START;

                        success = RunAScore();

                        // Change the name of the log file back to the analysis manager log file
                        LogFileName = m_mgrParams.GetParam("logfilename");
                        GlobalContext.Properties["LogName"] = LogFileName;
                        clsLogTools.ChangeLogFileName(LogFileName);

                        if (!success && !string.IsNullOrWhiteSpace(m_message))
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                                 "Error running AScore: " + m_message);
                        }

                        if (success)
                        {
                            // Export the AScore result table as a tab-delimited text file
                            success = ExportAScoreResults();
                            if (!success)
                            {
                                m_message = "Export of table t_results_ascore failed";
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Change the name of the log file back to the analysis manager log file
                        LogFileName = m_mgrParams.GetParam("logfilename");
                        GlobalContext.Properties["LogName"] = LogFileName;
                        clsLogTools.ChangeLogFileName(LogFileName);

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                             "Error running AScore: " + ex.Message);
                        success = false;
                        m_message = "Error running AScore";
                    }
                }

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;
                m_progress = PROGRESS_PCT_ASCORE_DONE;

                //Add the current job data to the summary file
                if (!UpdateSummaryFile())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                }

                //Make sure objects are released
                //2 second delay
                Thread.Sleep(2000);
                clsProgRunner.GarbageCollectNow();

                if (!success)
                {
                    // Move the source files and any results to the Failed Job folder
                    // Useful for debugging MultiAlign problems
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_ResFolderName = m_jobParams.GetParam("StepOutputFolderName");
                m_Dataset = m_jobParams.GetParam("OutputFolderName");
                m_jobParams.SetParam("StepParameters", "OutputFolderName", m_ResFolderName);

                CloseOutType result = MakeResultsFolder();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return result;
                }

                result = MoveResultFiles();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    m_message = "Error moving files into results folder";
                    return result;
                }

                result = CopyResultsFolderToServer();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return result;
                }

            }
            catch (Exception ex)
            {
                m_message = "Error in AScorePlugin->RunTool";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;

            }

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        protected bool ExportAScoreResults()
        {
            try
            {
                var sqlLiteDB = new FileInfo(Path.Combine(m_WorkDir, "Results.db3"));
                if (!sqlLiteDB.Exists)
                {
                    m_message = "Cannot export AScore results since Results.db3 does not exist";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
                return false;
            }

            return true;

        }

        protected void ExportTable(FileInfo sqlLiteDB, string tableName, FileInfo targetFile)
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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);

            ProcessingPipeline pipeline = ProcessingPipeline.Assemble("ExportTable", reader, writer);
            pipeline.RunRoot(null);

        }

        /// <summary>
        /// Run the AScore pipeline(s) listed in "AScoreOperations" parameter
        /// </summary>
        protected bool RunAScore()
        {
            // run the appropriate Mage pipeline(s) according to operations list parameter
            var ascoreMage = new clsAScoreMagePipeline(m_jobParams, m_mgrParams, m_IonicZipTools);
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


        protected void CopyFailedResultsToArchiveFolder()
        {
            string strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrEmpty(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory
            string strFolderPathToArchive = string.Copy(m_WorkDir);

            // If necessary, delete extra files with the following
            /* 
                try
                {
                    System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset + ".UIMF"));
                    System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset + "*.csv"));
                }
                catch
                {
                    // Ignore errors here
                }
            */

            // Make the results folder
            CloseOutType result = MakeResultsFolder();
            if (result == CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Move the result files into the result folder
                result = MoveResultFiles();
                if (result == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Move was a success; update strFolderPathToArchive
                    strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName);
                }
            }

            // Copy the results folder to the Archive folder
            var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
            objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive);

        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            string strAppFolderPath = clsGlobal.GetAppFolderPath();

            var fiIDMdll = new FileInfo(Path.Combine(strAppFolderPath, "AScore_DLL.dll"));

            return StoreToolVersionInfoDLL(fiIDMdll.FullName);
        }


        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfoDLL(string strAScoreDLLPath)
        {

            string strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            // Lookup the version of the DLL
            StoreToolVersionInfoOneFile(ref strToolVersionInfo, strAScoreDLLPath);

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                new FileInfo(strAScoreDLLPath)
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }

        }

    }
}


