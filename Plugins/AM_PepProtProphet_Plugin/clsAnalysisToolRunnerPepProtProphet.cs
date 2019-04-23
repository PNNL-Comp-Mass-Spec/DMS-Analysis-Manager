//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;

namespace AnalysisManagerPepProtProphetPlugIn
{
    /// <summary>
    /// Class for running peptide prophet and protein prophet using Philosopher
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsAnalysisToolRunnerPepProtProphet : clsAnalysisToolRunnerBase
    {

        #region "Constants and Enums"

        private const string Philosopher_CONSOLE_OUTPUT = "Philosopher_ConsoleOutput.txt";
        private const string Philosopher_EXE_NAME = "philosopheexer_windows_amd64.jar";

        private const float PROGRESS_PCT_STARTING = 1;
        private const float PROGRESS_PCT_COMPLETE = 99;

        #endregion

        #region "Module Variables"

        private bool mToolVersionWritten;

        // Populate this with a tool version reported to the console
        private string mPhilosopherVersion;

        private string mPhilosopherProgLoc;
        private string mConsoleOutputErrorMsg;

        private DateTime mLastConsoleOutputParse;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs peptide and protein prophet using Philosopher
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerPepProtProphet.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to Philosopher
                mPhilosopherProgLoc = DetermineProgramLocation("PhilosopherProgLoc", Philosopher_EXE_NAME);

                if (string.IsNullOrWhiteSpace(mPhilosopherProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the Philosopher version info in the database after the first line is written to file Philosopher_ConsoleOutput.txt
                mToolVersionWritten = false;
                mPhilosopherVersion = string.Empty;
                mConsoleOutputErrorMsg = string.Empty;

                if (!ValidateFastaFile())
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Process the XML files using Philosopher
                var processingResult = StartPepProtProphet();

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                clsGlobal.IdleLoop(0.5);
                PRISM.ProgRunner.GarbageCollectNow();

                if (!clsAnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();
                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                return processingResult;

            }
            catch (Exception ex)
            {
                LogError("Error in PepProtProphetPlugin->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileToSkip(Dataset + clsAnalysisResources.DOT_MZML_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory();
        }

        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {

            // Example Console output
            //
            // Running Philosopher

            var processingSteps = new SortedList<string, int>
            {
                {"???????", 0},
                {"???????", 2},
                {"???????", 5},
                {"Done", 98}
            };

            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 4)
                {
                    LogDebug("Parsing file " + consoleOutputFilePath);
                }

                mConsoleOutputErrorMsg = string.Empty;
                var currentProgress = 0;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var linesRead = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead += 1;

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var dataLineLCase = dataLine.ToLower();

                        if (linesRead <= 5)
                        {
                            // The first line has the path to the Philosopher .exe file and the command line arguments
                            // The second line is dashes
                            // The third line should be: ????????????????
                            // The fourth line should have ????????????????

                            if (string.IsNullOrEmpty(mPhilosopherVersion) &&
                                dataLine.StartsWith("Philosopher version", StringComparison.OrdinalIgnoreCase))
                            {
                                if (mDebugLevel >= 2)
                                {
                                    LogDebug(dataLine);
                                }

                                mPhilosopherVersion = string.Copy(dataLine);
                            }
                        }
                        else
                        {
                            foreach (var processingStep in processingSteps)
                            {
                                if (!dataLine.StartsWith(processingStep.Key, StringComparison.OrdinalIgnoreCase))
                                    continue;

                                currentProgress = processingStep.Value;
                            }

                            if (linesRead > 12 &&
                                dataLineLCase.Contains("error") &&
                                string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg = "Error running Philosopher: " + dataLine;
                            }
                        }
                    }
                }

                if (currentProgress > mProgress)
                {
                    mProgress = currentProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private CloseOutType StartPepProtProphet()
        {
            LogMessage("Running Philosopher");

            // Set up and execute a program runner to run Philosopher

            var mzMLFile = mDatasetName + clsAnalysisResources.DOT_MZML_EXTENSION;

            // Set up and execute a program runner to run Philosopher
            var arguments = Path.Combine(mWorkDir, mzMLFile);

            LogDebug(mPhilosopherProgLoc + " " + arguments);

            mCmdRunner = new clsRunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = Path.Combine(mWorkDir, Philosopher_CONSOLE_OUTPUT)
            };
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mProgress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mPhilosopherVersion))
                {
                    ParseConsoleOutputFile(Path.Combine(mWorkDir, Philosopher_CONSOLE_OUTPUT));
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!processingSuccess)
            {
                LogError("Error running Philosopher");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("Philosopher returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to Philosopher failed (but exit code is 0)");
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Validate that Philosopher created a .pep.XML file
            var pepXmlFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + ".pep.XML"));
            if (!pepXmlFile.Exists)
            {
                LogError("Philosopher did not create a .pep.XML file");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (pepXmlFile.Length == 0)
            {
                LogError(".pep.XML file created by Philosopher is empty");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Zip the .pep.XML file
            var zipSuccess = ZipOutputFile(pepXmlFile, ".pep.XML file");
            if (!zipSuccess)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            mStatusTools.UpdateAndWrite(mProgress);
            if (mDebugLevel >= 3)
            {
                LogDebug("Philosopher Search Complete");
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool StoreToolVersionInfo()
        {
            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var toolVersionInfo = string.Copy(mPhilosopherVersion);

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new FileInfo(mPhilosopherProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private bool ValidateFastaFile()
        {
            throw new NotImplementedException();
        }

        #endregion

        #region "Event Handlers"

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            if (!(DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE))
                return;

            mLastConsoleOutputParse = DateTime.UtcNow;

            ParseConsoleOutputFile(Path.Combine(mWorkDir, Philosopher_CONSOLE_OUTPUT));

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mPhilosopherVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("Philosopher");
        }

        #endregion
    }
}
