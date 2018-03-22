using System.IO;
using System.Collections.Generic;
using System;
using AnalysisManagerBase;

using Cyclops;

namespace AnalysisManager_Cyclops_PlugIn
{
    /// <summary>
    /// Class for running Cyclops
    /// </summary>
    public class clsAnalysisToolRunnerCyclops : clsAnalysisToolRunnerBase
    {

        private const float PROGRESS_PCT_CYCLOPS_START = 5;
        private const float PROGRESS_PCT_CYCLOPS_DONE = 95;

        private const string INITIALIZING_LOG_FILE = "Initializing the Cyclops Controller";

        private StreamWriter mCyclopsLogWriter;

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

                LogMessage("Running Cyclops");
                m_progress = PROGRESS_PCT_CYCLOPS_START;
                UpdateStatusRunning();


                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerApe.RunTool(): Enter");
                }

                // Store the Cyclops version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining Cyclops version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to R
                var rProgLocFromRegistry = GetRPathFromWindowsRegistry();
                if (string.IsNullOrEmpty(rProgLocFromRegistry))
                    return CloseOutType.CLOSEOUT_FAILED;

                if (!Directory.Exists(rProgLocFromRegistry))
                {
                    m_message = "R folder not found (path determined from the Windows Registry)";
                    LogError(m_message + " at " + rProgLocFromRegistry);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var d_Params = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    {"Job", m_jobParams.GetParam("Job")},
                    {"RDLL", rProgLocFromRegistry},
                    {"CyclopsWorkflowName", m_jobParams.GetParam("CyclopsWorkflowName")},
                    {"workDir", m_WorkDir},
                    {"Consolidation_Factor", m_jobParams.GetParam("Consolidation_Factor")},
                    {"Fixed_Effect", m_jobParams.GetParam("Fixed_Effect")},
                    {"RunProteinProphet", m_jobParams.GetParam("RunProteinProphet")},
                    {"orgdbdir", m_mgrParams.GetParam("orgdbdir")}
                };


                // Create the cyclops log file
                // This class will write messages to the log file
                var cyclopsLogFile = Path.Combine(m_WorkDir, "Cyclops_Log.txt");
                mCyclopsLogWriter = new StreamWriter(new FileStream(cyclopsLogFile, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
                {
                    AutoFlush = true
                };

                bool processingSuccess;

                try
                {
                    AppendToCyclopsLog(INITIALIZING_LOG_FILE);

                    var cyclops = new CyclopsController(d_Params);
                    RegisterEvents(cyclops);

                    cyclops.ErrorEvent += Cyclops_ErrorEvent;
                    cyclops.WarningEvent += Cyclops_WarningEvent;
                    cyclops.StatusEvent += Cyclops_StatusEvent;

                    // Don't log these:
                    // cyclops.OperationsDatabasePath (always blank)
                    // cyclops.WorkFlowFileName       (always blank)
                    // cyclops.WorkingDirectory       (different for each manager)

                    AppendToCyclopsLog("Parameters:");
                    foreach (var entry in cyclops.Parameters)
                    {
                        AppendToCyclopsLog("  " + entry.Key + ": " + entry.Value);
                    }

                    processingSuccess = cyclops.Run();

                }
                catch (Exception ex)
                {
                    AppendToCyclopsLog("Error running Cyclops: " + ex.Message);
                    LogError("Error running Cyclops: " + ex.Message, ex);
                    processingSuccess = false;
                }

                mCyclopsLogWriter.Flush();
                mCyclopsLogWriter.Dispose();

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;
                m_progress = PROGRESS_PCT_CYCLOPS_DONE;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.clsProgRunner.GarbageCollectNow();

                // Delete the log file if it only has the "initializing log file" line
                PossiblyDeleteCyclopsLogFile(cyclopsLogFile);

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

                var resultsFolderCreated = MakeResultsFolder();
                if (!resultsFolderCreated)
                {
                    // MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Move the Plots folder to the result files folder
                var diPlotsFolder = new DirectoryInfo(Path.Combine(m_WorkDir, "Plots"));

                if (diPlotsFolder.Exists)
                {
                    var strTargetFolderPath = Path.Combine(Path.Combine(m_WorkDir, m_ResFolderName), "Plots");
                    diPlotsFolder.MoveTo(strTargetFolderPath);
                }

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Error in CyclopsPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }


        }

        private void PossiblyDeleteCyclopsLogFile(string cyclopsLogFile)
        {
            try
            {

                var fiCyclopsLogFile = new FileInfo(cyclopsLogFile);
                if (!fiCyclopsLogFile.Exists)
                {
                    return;
                }

                var deleteFile = false;
                if (fiCyclopsLogFile.Length == 0)
                {
                    deleteFile = true;
                }
                else
                {
                    var lineCount = 0;

                    using (var logFileReader = new StreamReader(new FileStream(fiCyclopsLogFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        while (!logFileReader.EndOfStream)
                        {
                            var dataLine = logFileReader.ReadLine();
                            lineCount++;

                            if (lineCount == 1)
                            {
                                if (string.Equals(dataLine, INITIALIZING_LOG_FILE))
                                {
                                    deleteFile = true;
                                }

                                continue;
                            }

                            // There is more than one line in the log file
                            deleteFile = false;
                            break;
                        }
                    }
                }

                if (deleteFile)
                {
                    m_FileTools.DeleteFileWithRetry(fiCyclopsLogFile, out _);
                }

            }
            catch (Exception ex)
            {
                LogError("Exception in PossiblyDeleteCyclopsLogFile: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Add a message to the cyclops log file
        /// </summary>
        /// <param name="message"></param>
        private void AppendToCyclopsLog(string message = "")
        {
            if (mCyclopsLogWriter == null)
                return;

            try
            {
                mCyclopsLogWriter.WriteLine(message);
            }
            catch (Exception ex)
            {
                LogError("Exception in AppendToCyclopsLog: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            var cyclopsDll = Path.Combine(clsGlobal.GetAppFolderPath(), "Cyclops.dll");
            var success = StoreDotNETToolVersionInfo(cyclopsDll);

            return success;
        }


        private void Cyclops_ErrorEvent(string message, Exception ex)
        {
            AppendToCyclopsLog();
            string errorMessage;
            if (message.StartsWith("Error", StringComparison.InvariantCultureIgnoreCase))
                errorMessage = message;
            else
                errorMessage = "Error: " + message;

            AppendToCyclopsLog(errorMessage);

            // Cyclops error messages sometimes contain a carriage return followed by a stack trace
            // We don't want that information in m_message so split on \r and \n
            var messageParts = message.Split('\r', '\n');
            LogError(messageParts[0]);
        }

        private void Cyclops_WarningEvent(string message)
        {
            AppendToCyclopsLog();

            string warningMessage;
            if (message.StartsWith("Warning", StringComparison.InvariantCultureIgnoreCase))
                warningMessage = message;
            else
                warningMessage = "Warning: " + message;

            AppendToCyclopsLog(warningMessage);

            // Cyclops messages sometimes contain a carriage return followed by a stack trace
            // We don't want that information in m_message so split on \r and \n
            var messageParts = message.Split('\r', '\n');
            LogWarning(messageParts[0]);
        }


        private void Cyclops_StatusEvent(string message)
        {
            AppendToCyclopsLog(message);
        }

    }
}
