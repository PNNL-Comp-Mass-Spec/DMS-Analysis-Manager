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
                mProgress = PROGRESS_PCT_CYCLOPS_START;
                UpdateStatusRunning();


                if (mDebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerApe.RunTool(): Enter");
                }

                // Store the Cyclops version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining Cyclops version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to R
                var rProgLocFromRegistry = GetRPathFromWindowsRegistry();
                if (string.IsNullOrEmpty(rProgLocFromRegistry))
                    return CloseOutType.CLOSEOUT_FAILED;

                if (!Directory.Exists(rProgLocFromRegistry))
                {
                    mMessage = "R folder not found (path determined from the Windows Registry)";
                    LogError(mMessage + " at " + rProgLocFromRegistry);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var paramDictionary = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    {"Job", mJobParams.GetParam("Job")},
                    {"RDLL", rProgLocFromRegistry},
                    {"CyclopsWorkflowName", mJobParams.GetParam("CyclopsWorkflowName")},
                    {"workDir", mWorkDir},
                    {"Consolidation_Factor", mJobParams.GetParam("Consolidation_Factor")},
                    {"Fixed_Effect", mJobParams.GetParam("Fixed_Effect")},
                    {"RunProteinProphet", mJobParams.GetParam("RunProteinProphet")},
                    {"orgdbdir", mMgrParams.GetParam("OrgDbDir")}
                };


                // Create the cyclops log file
                // This class will write messages to the log file
                var cyclopsLogFile = Path.Combine(mWorkDir, "Cyclops_Log.txt");
                mCyclopsLogWriter = new StreamWriter(new FileStream(cyclopsLogFile, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
                {
                    AutoFlush = true
                };

                bool processingSuccess;

                try
                {
                    AppendToCyclopsLog(INITIALIZING_LOG_FILE);

                    var cyclops = new CyclopsController(paramDictionary);
                    RegisterEvents(cyclops);

                    cyclops.ErrorEvent += Cyclops_ErrorEvent;
                    cyclops.WarningEvent += Cyclops_WarningEvent;
                    cyclops.StatusEvent += Cyclops_StatusEvent;

                    // Don't log these:
                    // cyclops.OperationsDatabasePath (always blank)
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
                mStopTime = DateTime.UtcNow;
                mProgress = PROGRESS_PCT_CYCLOPS_DONE;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.ProgRunner.GarbageCollectNow();

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
                mResultsFolderName = mJobParams.GetParam("StepOutputFolderName");
                mDatasetName = mJobParams.GetParam(clsAnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME);
                mJobParams.SetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME, mResultsFolderName);

                var resultsFolderCreated = MakeResultsFolder();
                if (!resultsFolderCreated)
                {
                    // MakeResultsFolder handles posting to local log, so set database error message and exit
                    mMessage = "Error making results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Move the Plots folder to the result files folder
                var diPlotsFolder = new DirectoryInfo(Path.Combine(mWorkDir, "Plots"));

                if (diPlotsFolder.Exists)
                {
                    var strTargetFolderPath = Path.Combine(Path.Combine(mWorkDir, mResultsFolderName), "Plots");
                    diPlotsFolder.MoveTo(strTargetFolderPath);
                }

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                mMessage = "Error in CyclopsPlugin->RunTool";
                LogError(mMessage, ex);
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
                    mFileTools.DeleteFileWithRetry(fiCyclopsLogFile, out _);
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
            // We don't want that information in mMessage so split on \r and \n
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
            // We don't want that information in mMessage so split on \r and \n
            var messageParts = message.Split('\r', '\n');
            LogWarning(messageParts[0]);
        }


        private void Cyclops_StatusEvent(string message)
        {
            AppendToCyclopsLog(message);
        }

    }
}
