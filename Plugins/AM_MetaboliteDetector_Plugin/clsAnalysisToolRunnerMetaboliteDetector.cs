/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 09/23/2016                                           **
**                                                              **
*****************************************************************/

using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerMetaboliteDetectorPlugin
{

    /// <summary>
    /// Class for running the Metabolite Detector
    /// </summary>
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

        #endregion

        #region "Methods"

        /// <summary>
        /// Processes data usingthe Metabolite Detector
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
                    mMessage = "Error determining MetaboliteDetector version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }


                // Initialize classwide variables
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
                PRISM.ProgRunner.GarbageCollectNow();

                if (processingError)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // No need to keep several files; exclude them now
                mJobParams.AddResultFileToSkip(mJobParams.GetParam("ParmFileName"));

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
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 4)
                {
                    LogDebug("Parsing file " + strConsoleOutputFilePath);
                }

                using (var reader = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            continue;
                        }

                        if (dataLine.ToLower().StartsWith("error "))
                        {
                            StoreConsoleErrorMessage(reader, dataLine);
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
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
                var fiResultsFile = new FileInfo(Path.Combine(mWorkDir, METABOLITE_DETECTOR_RESULTS_FILE));

                if (!fiResultsFile.Exists)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("Metabolite Detector results not found: " + fiResultsFile.Name);
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

            var cmdStr = "xyz";

            if (mDebugLevel >= 1)
            {
                LogDebug(cmdStr);
            }

            mConsoleOutputFile = Path.Combine(mWorkDir, METABOLITE_DETECTOR_CONSOLE_OUTPUT);

            var cmdRunner = new clsRunDosProgram(mWorkDir, mDebugLevel)
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

            var success = cmdRunner.RunProgram(mMetaboliteDetectorProgLoc, cmdStr, "MetaboliteDetector", true);

            if (!cmdRunner.WriteConsoleOutputToFile && cmdRunner.CachedConsoleOutput.Length > 0)
            {
                // Write the console output to a text file
                clsGlobal.IdleLoop(0.25);

                using (var writer = new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine(cmdRunner.CachedConsoleOutput);
                }
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            clsGlobal.IdleLoop(0.25);

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
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string strProgLoc)
        {

            var strToolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
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

            // Store paths to key DLLs in toolFiles
            var toolFiles = new List<FileInfo>
            {
                fiProgram
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }

        }

        #endregion

        #region "Event Handlers"

        void CmdRunner_LoopWaiting()
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

        #endregion
    }
}
