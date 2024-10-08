/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 09/23/2016                                           **
**                                                              **
*****************************************************************/

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMetaboliteDetectorPlugin
{
    /// <summary>
    /// Class for running the Metabolite Detector
    /// </summary>
    public class AnalysisToolRunnerMetaboliteDetector : AnalysisToolRunnerBase
    {
        private const int PROGRESS_PCT_STARTING = 5;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private const string METABOLITE_DETECTOR_CONSOLE_OUTPUT = "MetaboliteDetector_ConsoleOutput.txt";

        private const string METABOLITE_DETECTOR_RESULTS_FILE = "MetabDetector_Results.csv";

        private string mConsoleOutputFile;
        private string mConsoleOutputErrorMsg;

        private string mMetaboliteDetectorProgLoc;

        private DateTime mLastConsoleOutputParse;

        /// <summary>
        /// Processes data using the Metabolite Detector
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
                    LogDebug("AnalysisToolRunnerMetaboliteDetector.RunTool(): Enter");
                }

                // Determine the path to the MetaboliteDetector program
                mMetaboliteDetectorProgLoc = DetermineProgramLocation("MetaboliteDetectorProgLoc", "MetaboliteDetector.exe");

                if (string.IsNullOrWhiteSpace(mMetaboliteDetectorProgLoc))
                    return CloseOutType.CLOSEOUT_FAILED;

                // Store the MetaboliteDetector version info in the database
                if (!StoreToolVersionInfo(mMetaboliteDetectorProgLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining MetaboliteDetector version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Initialize class-wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

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

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Could use the following to create a summary file:
                // Add the current job data to the summary file
                // UpdateSummaryFile();

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                if (processingError)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // No need to keep several files; exclude them now
                mJobParams.AddResultFileToSkip(mJobParams.GetParam("ParamFileName"));

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in MetaboliteDetectorPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example Console output
            //
            // ...
            //

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

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    if (dataLine.StartsWith("error ", StringComparison.OrdinalIgnoreCase))
                    {
                        StoreConsoleErrorMessage(reader, dataLine);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Read the MetaboliteDetector results file to check for valid results
        /// </summary>
        private bool PostProcessResults()
        {
            try
            {
                var resultsFile = new FileInfo(Path.Combine(mWorkDir, METABOLITE_DETECTOR_RESULTS_FILE));

                if (!resultsFile.Exists)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("Metabolite Detector results not found: " + resultsFile.Name);
                    }
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Exception post processing results";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }
        }

        private bool ProcessDatasetWithMetaboliteDetector()
        {
            // Set up and execute a program runner to run the Metabolite Detector

            const string arguments = "xyz";

            if (mDebugLevel >= 1)
            {
                LogDebug(arguments);
            }

            mConsoleOutputFile = Path.Combine(mWorkDir, METABOLITE_DETECTOR_CONSOLE_OUTPUT);

            var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = false,
                ConsoleOutputFilePath = mConsoleOutputFile
            };

            RegisterEvents(cmdRunner);
            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mProgress = PROGRESS_PCT_STARTING;

            var success = cmdRunner.RunProgram(mMetaboliteDetectorProgLoc, arguments, "MetaboliteDetector", true);

            if (!cmdRunner.WriteConsoleOutputToFile && cmdRunner.CachedConsoleOutput.Length > 0)
            {
                // Write the console output to a text file
                Global.IdleLoop(0.25);

                using var writer = new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine(cmdRunner.CachedConsoleOutput);
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            Global.IdleLoop(0.25);

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

            mMessage = "Error running MetaboliteDetector";

            LogError(mMessage + ", job " + mJob);

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

        private void StoreConsoleErrorMessage(StreamReader reader, string firstDataLine)
        {
            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                mConsoleOutputErrorMsg = "Error running MetaboliteDetector:";
            }
            mConsoleOutputErrorMsg += "; " + firstDataLine;

            while (!reader.EndOfStream)
            {
                // Store the remaining console output lines
                var dataLine = reader.ReadLine();

                if (!string.IsNullOrWhiteSpace(dataLine) && !dataLine.StartsWith("========"))
                {
                    mConsoleOutputErrorMsg += "; " + dataLine;
                }
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var program = new FileInfo(progLoc);

            if (!program.Exists)
            {
                try
                {
                    toolVersionInfo = "Unknown";
                    return SetStepTaskToolVersion(toolVersionInfo, new List<FileInfo>(), false);
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion", ex);
                    return false;
                }
            }

            // Lookup the version of the .NET program
            mToolVersionUtilities.StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, program.FullName);

            // Store paths to key DLLs in toolFiles
            var toolFiles = new List<FileInfo>
            {
                program
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private void CmdRunner_LoopWaiting()
        {
            // Synchronize the stored Debug level with the value stored in the database

            {
                UpdateStatusFile();

                // Parse the console output file every 15 seconds
                if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
                {
                    mLastConsoleOutputParse = DateTime.UtcNow;

                    ParseConsoleOutputFile(Path.Combine(mWorkDir, mConsoleOutputFile));

                    LogProgress("MetaboliteDetector");
                }
            }
        }
    }
}
