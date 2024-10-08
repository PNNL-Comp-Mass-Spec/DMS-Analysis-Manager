/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 08/08/2017                                           **
**                                                              **
*****************************************************************/

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerUIMFtoMassHunterPlugin
{
    /// <summary>
    /// Class for running the UIMF to MassHunter converter
    /// </summary>
    public class AnalysisToolRunnerUIMFtoMassHunter : AnalysisToolRunnerBase
    {
        private const int PROGRESS_PCT_STARTING = 5;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private const string UIMF_CONVERTER_CONSOLE_OUTPUT = "UIMFtoMassHunter_ConsoleOutput.txt";

        private string mConsoleOutputFile;
        private string mConsoleOutputErrorMsg;

        private string mUIMFConverterProgLoc;

        private DateTime mLastConsoleOutputParse;

        /// <summary>
        /// Converts .UIMF files to Agilent .D folders
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
                    LogDebug("AnalysisToolRunnerUIMFtoMassHunter.RunTool(): Enter");
                }

                // Determine the path to the UimfToMassHunter program
                mUIMFConverterProgLoc = DetermineProgramLocation("UimfToMassHunterProgLoc", "UimfToMassHunter.exe");

                if (string.IsNullOrWhiteSpace(mUIMFConverterProgLoc))
                {
                    // Error has already been logged
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the UimfToMassHunter version info in the database
                if (!StoreToolVersionInfo(mUIMFConverterProgLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining UIMFtoMassHunter version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Initialize class-wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                var processingSuccess = ConvertToAgilentDotD();

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Could use the following to create a summary file:
                // Add the current job data to the summary file
                // UpdateSummaryFile();

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                if (!processingSuccess)
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
                mMessage = "Error in UIMFtoMassHunterPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool ConvertToAgilentDotD()
        {
            // Set up and execute a program runner to run the UIMF to MassHunter converter

            // Valid dataset type
            var uimfFileName = mDatasetName + ".uimf";
            var uimfFilePath = AnalysisResources.ResolveStoragePath(mWorkDir, uimfFileName);

            if (string.IsNullOrWhiteSpace(uimfFilePath))
            {
                LogError("Cannot convert; UIMF file not found in " + mWorkDir);
                return false;
            }

            var uimfFile = new FileInfo(uimfFilePath);

            if (!uimfFile.Exists)
            {
                LogError("UIMF file not found: " + uimfFile.FullName);
                return false;
            }

            var arguments = "UimfToMassHunter.exe " + Global.PossiblyQuotePath(uimfFile.FullName);

            if (mDebugLevel >= 1)
            {
                LogDebug(arguments);
            }

            mConsoleOutputFile = Path.Combine(mWorkDir, UIMF_CONVERTER_CONSOLE_OUTPUT);

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

            var success = cmdRunner.RunProgram(mUIMFConverterProgLoc, arguments, "UIMFtoMassHunter", true);

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

            mMessage = "Error running UIMFtoMassHunter";

            LogError(mMessage + ", job " + mJob);

            if (cmdRunner.ExitCode != 0)
            {
                LogWarning("UIMFtoMassHunter returned a non-zero exit code: " + cmdRunner.ExitCode);
            }
            else
            {
                LogWarning("UIMFtoMassHunter failed (but exit code is 0)");
            }

            return false;
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

        private void StoreConsoleErrorMessage(StreamReader reader, string firstDataLine)
        {
            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                mConsoleOutputErrorMsg = "Error running UIMFtoMassHunter:";
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
            var additionalDLLs = new List<string>
            {
                "UIMFLibrary.dll"
            };

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDLLs, true);

            return success;
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

                    LogProgress("UIMFtoMassHunter");
                }
            }
        }
    }
}
