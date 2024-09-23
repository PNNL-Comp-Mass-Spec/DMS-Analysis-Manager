//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 09/17/2018
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerThermoPeakDataExporterPlugIn
{
    /// <summary>
    /// Class for running ThermoPeakDataExporter to extract data from a Thermo .raw file
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerThermoPeakDataExporter : AnalysisToolRunnerBase
    {
        private const string THERMO_DATA_EXPORTER_CONSOLE_OUTPUT = "ThermoDataExporter_ConsoleOutput.txt";
        private const string THERMO_DATA_EXPORTER_EXE_NAME = "ThermoPeakDataExporter.exe";

        private const int PROGRESS_PCT_STARTING = 1;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private string mConsoleOutputErrorMsg;

        private readonly Regex reExtractPercentFinished = new("(?<PercentComplete>[0-9.]+)% finished", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private DateTime mLastConsoleOutputParse;

        private RunDosProgram mCmdRunner;

        /// <summary>
        /// Runs ThermoPeakDataExporter tool
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
                    LogDebug("AnalysisToolRunnerThermoPeakDataExporter.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to the ThermoPeakDataExporter program
                var progLoc = DetermineProgramLocation("ThermoPeakDataExporterProgLoc", THERMO_DATA_EXPORTER_EXE_NAME);

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the ThermoPeakDataExporter version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining ThermoPeakDataExporter version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mConsoleOutputErrorMsg = string.Empty;

                var processingResult = StartThermoPeakDataExporter(progLoc);

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                if (!AnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Do not keep the console output file since export was successful
                mJobParams.AddResultFileToSkip(THERMO_DATA_EXPORTER_CONSOLE_OUTPUT);

                var success = CopyResultsToTransferDirectory();

                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                return processingResult;
            }
            catch (Exception ex)
            {
                mMessage = "Error in TopFDPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Parse the ThermoPeakDataExporter console output file to determine the ThermoPeakDataExporter version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example Console output

            // Using options:
            //  Thermo Instrument file: DatasetName.raw
            //  Output file: C:\DMS_WorkDir\DatasetName.tsv
            //  Minimum S/N: 2.0
            //   11.7% finished: Processing scan 2100
            //   31.2% finished: Processing scan 5600
            //   62.5% finished: Processing scan 11200
            //   82.0% finished: Processing scan 14700
            //   97.6% finished: Processing scan 17500
            // Processing complete; created file C:\DMS_WorkDir1\DatasetName.tsv

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

                float actualProgress = 0;

                mConsoleOutputErrorMsg = string.Empty;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var match = reExtractPercentFinished.Match(dataLine);

                        if (match.Success)
                        {
                            actualProgress = float.Parse(match.Groups["PercentComplete"].Value);
                        }
                        else if (dataLine.StartsWith("error", StringComparison.OrdinalIgnoreCase) &&
                                 string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                        {
                            mConsoleOutputErrorMsg = "Error running ThermoPeakDataExporter: " + dataLine;
                        }
                    }
                }

                if (mProgress < actualProgress)
                {
                    mProgress = actualProgress;
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

        private CloseOutType StartThermoPeakDataExporter(string progLoc)
        {
            LogMessage("Running ThermoPeakDataExporter");

            // Future, if needed: filter by intensity, m/z, and/or scan

            var resultsFile = new FileInfo(Path.Combine(mWorkDir, Dataset + ".tsv"));

            var minimumSignalToNoiseRatio = mJobParams.GetJobParameter("MinimumSignalToNoiseRatio", 0);

            var arguments = Dataset + AnalysisResources.DOT_RAW_EXTENSION +
                            " /O:" + Global.PossiblyQuotePath(resultsFile.FullName) +
                            " /minSN:" + minimumSignalToNoiseRatio;

            LogDebug(progLoc + " " + arguments);

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, THERMO_DATA_EXPORTER_CONSOLE_OUTPUT);

            mProgress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var processingSuccess = mCmdRunner.RunProgram(progLoc, arguments, "ThermoPeakDataExporter", true);

            // Parse the console output file one more time to check for errors
            ParseConsoleOutputFile(Path.Combine(mWorkDir, THERMO_DATA_EXPORTER_CONSOLE_OUTPUT));

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!processingSuccess)
            {
                LogError("Error running ThermoPeakDataExporter");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("ThermoPeakDataExporter returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to ThermoPeakDataExporter failed (but exit code is 0)");
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make sure the output file was created and is not zero-bytes

            resultsFile.Refresh();

            if (!resultsFile.Exists)
            {
                LogError(string.Format("{0} file was not created by ThermoPeakDataExporter", resultsFile.Name));
                return CloseOutType.CLOSEOUT_FAILED;
            }
            if (resultsFile.Length == 0)
            {
                LogError(string.Format("{0} file created by ThermoPeakDataExporter is empty; " +
                                       "assure that the input .mzML file has MS/MS spectra", resultsFile.Name));
                return CloseOutType.CLOSEOUT_FAILED;
            }

            mStatusTools.UpdateAndWrite(mProgress);

            if (mDebugLevel >= 3)
            {
                LogDebug("ThermoPeakDataExporter analysis complete");
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDLLs = new List<string>
            {
                "ThermoRawFileReader.dll"
            };

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDLLs, true);

            return success;
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds < SECONDS_BETWEEN_UPDATE)
                return;

            mLastConsoleOutputParse = DateTime.UtcNow;

            ParseConsoleOutputFile(Path.Combine(mWorkDir, THERMO_DATA_EXPORTER_CONSOLE_OUTPUT));

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("ThermoPeakDataExporter");
        }
    }
}
