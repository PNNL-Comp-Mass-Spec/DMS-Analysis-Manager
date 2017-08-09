/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 09/23/2016                                           **
**                                                              **
*****************************************************************/

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerMetaboliteDetectorPlugin
{
    public class clsAnalysisToolRunnerMetaboliteDetector : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        private const float PROGRESS_PCT_STARTING = 5;
        private const float PROGRESS_PCT_COMPLETE = 99;

        private const string METABOLITE_DETECTOR_CONSOLE_OUTPUT = "MetaboliteDetector_ConsoleOutput.txt";

        private const string METABOLITE_DETECTOR_RESULTS_FILE = "MetabDetector_Results.csv";

        #endregion

        #region "Module Variables"

        private string mConsoleOutputFile;
        private string mConsoleOutputErrorMsg;

        private string mMetaboliteDetectorProgLoc;

        private DateTime mLastConsoleOutputParse;
        private DateTime mLastProgressWriteTime;

        #endregion

        #region "Methods"

        /// <summary>
        /// Processes data usingthe Metabolite Detector
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerMetaboliteDetector.RunTool(): Enter");
                }

                // Determine the path to the MetaboliteDetector program
                mMetaboliteDetectorProgLoc = DetermineProgramLocation("MetaboliteDetectorProgLoc", "MetaboliteDetector.exe");

                if (string.IsNullOrWhiteSpace(mMetaboliteDetectorProgLoc))
                    return CloseOutType.CLOSEOUT_FAILED;


                // Store the MetaboliteDetector version info in the database
                if (!StoreToolVersionInfo(mMetaboliteDetectorProgLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining MetaboliteDetector version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }


                // Initialize classwide variables
                mLastConsoleOutputParse = DateTime.UtcNow;
                mLastProgressWriteTime = DateTime.UtcNow;

                var processingError = false;

                var processingSuccess = ProcessDatasetWithMetaboliteDetector();

                if (!processingSuccess)
                {
                    processingError = true;
                }
                else
                {
                    // Look for the result files
                    var postProcessSuccess = PostProcessResults();
                    if (!postProcessSuccess)
                        processingError = true;
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Could use the following to create a summary file:
                // Add the current job data to the summary file
                // UpdateSummaryFile();

                // Make sure objects are released
                Thread.Sleep(500);
                PRISM.clsProgRunner.GarbageCollectNow();

                if (processingError)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // No need to keep several files; exclude them now
                m_jobParams.AddResultFileToSkip(m_jobParams.GetParam("ParmFileName"));

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Error in MetaboliteDetectorPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            // Example Console output
            //
            // ...
            //

            try
            {
                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Parsing file " + strConsoleOutputFilePath);
                }

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                        {
                            continue;
                        }

                        if (strLineIn.ToLower().StartsWith("error "))
                        {
                            StoreConsoleErrorMessage(srInFile, strLineIn);
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }

        }

        /// <summary>
        /// Read the MetaboliteDetector results file to check for valid results
        /// </summary>
        /// <returns></returns>
        private bool PostProcessResults()
        {
            try
            {
                var fiResultsFile = new FileInfo(Path.Combine(m_WorkDir, METABOLITE_DETECTOR_RESULTS_FILE));

                if (!fiResultsFile.Exists)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("Metabolite Detector results not found: " + fiResultsFile.Name);
                    }
                    return false;
                }

                return true;

            }
            catch (Exception ex)
            {
                m_message = "Exception post processing results";
                LogError(m_message + ": " + ex.Message);
                return false;
            }
        }

        private bool ProcessDatasetWithMetaboliteDetector()
        {
            // Set up and execute a program runner to run the Metabolite Detector

            var cmdStr = "xyz";

            if (m_DebugLevel >= 1)
            {
                LogDebug(cmdStr);
            }

            mConsoleOutputFile = Path.Combine(m_WorkDir, METABOLITE_DETECTOR_CONSOLE_OUTPUT);

            var cmdRunner = new clsRunDosProgram(m_WorkDir)
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = false,
                ConsoleOutputFilePath = mConsoleOutputFile
            };
            RegisterEvents(cmdRunner);
            cmdRunner.LoopWaiting += cmdRunner_LoopWaiting;

            m_progress = PROGRESS_PCT_STARTING;

            var success = cmdRunner.RunProgram(mMetaboliteDetectorProgLoc, cmdStr, "MetaboliteDetector", true);

            if (!cmdRunner.WriteConsoleOutputToFile && cmdRunner.CachedConsoleOutput.Length > 0)
            {
                // Write the console output to a text file
                Thread.Sleep(250);

                var swConsoleOutputfile = new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));
                swConsoleOutputfile.WriteLine(cmdRunner.CachedConsoleOutput);
                swConsoleOutputfile.Close();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(
                                     mConsoleOutputErrorMsg);
            }

            Thread.Sleep(250);

            // Parse the ConsoleOutput file to look for errors
            ParseConsoleOutputFile(mConsoleOutputFile);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (success)
            {
                return true;
            }

            m_message = "Error running MetaboliteDetector";

            LogError(m_message + ", job " + m_JobNum);

            if (cmdRunner.ExitCode != 0)
            {
                LogWarning("MetaboliteDetector returned a non-zero exit code: " + cmdRunner.ExitCode);
            }
            else
            {
                LogWarning("MetaboliteDetector failed (but exit code is 0)");
            }

            return false;
        }

        private void StoreConsoleErrorMessage(StreamReader srInFile, string strLineIn)
        {
            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                mConsoleOutputErrorMsg = "Error running MetaboliteDetector:";
            }
            mConsoleOutputErrorMsg += "; " + strLineIn;

            while (!srInFile.EndOfStream)
            {
                // Store the remaining console output lines
                strLineIn = srInFile.ReadLine();

                if (!string.IsNullOrWhiteSpace(strLineIn) && !strLineIn.StartsWith("========"))
                {
                    mConsoleOutputErrorMsg += "; " + strLineIn;
                }

            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string strProgLoc)
        {

            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var fiProgram = new FileInfo(strProgLoc);
            if (!fiProgram.Exists)
            {
                try
                {
                    strToolVersionInfo = "Unknown";
                    return SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), saveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion", ex);
                    return false;
                }

            }

            // Lookup the version of the .NET program
            StoreToolVersionInfoViaSystemDiagnostics(ref strToolVersionInfo, fiProgram.FullName);

            // Store paths to key DLLs in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                fiProgram
            };

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }

        }

        #endregion

        #region "Event Handlers"

        void cmdRunner_LoopWaiting()
        {

            // Synchronize the stored Debug level with the value stored in the database

            {
                UpdateStatusFile();

                // Parse the console output file every 15 seconds
                if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
                {
                    mLastConsoleOutputParse = DateTime.UtcNow;

                    ParseConsoleOutputFile(Path.Combine(m_WorkDir, mConsoleOutputFile));

                    LogProgress("MetaboliteDetector");
                }
            }

        }

        #endregion
    }
}
