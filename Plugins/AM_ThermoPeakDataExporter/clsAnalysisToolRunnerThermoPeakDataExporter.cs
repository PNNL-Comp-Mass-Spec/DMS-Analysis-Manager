//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 09/17/2018
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerThermoPeakDataExporterPlugIn
{
    /// <summary>
    /// Class for running ThermoPeakDataExporter to extract data from a Thermo .raw file
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsAnalysisToolRunnerThermoPeakDataExporter : clsAnalysisToolRunnerBase
    {

        #region "Constants"

        private const string THERMO_DATA_EXPORTER_CONSOLE_OUTPUT = "ThermoDataExporter_ConsoleOutput.txt";
        private const string THERMO_DATA_EXPORTER_EXE_NAME = "ThermoPeakDataExporter.exe";

        private const float PROGRESS_PCT_STARTING = 1;
        private const float PROGRESS_PCT_COMPLETE = 99;

        #endregion

        #region "Module Variables"

        private string mConsoleOutputErrorMsg;

        private readonly Regex reExtractPercentFinished = new Regex(@"(?<PercentComplete>[0-9.]+)% finished", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private DateTime mLastConsoleOutputParse;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

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

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerThermoPeakDataExporter.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to the ThermoPeakDataExporter program
                var progLoc = DetermineProgramLocation("ThermoPeakDataExporterProgLoc", THERMO_DATA_EXPORTER_EXE_NAME);

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }


                // Store the ProMex version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining ProMex version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mConsoleOutputErrorMsg = string.Empty;

                var processingResult = StartThermoPeakDataExporter(progLoc);

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                PRISM.ProgRunner.GarbageCollectNow();

                if (!clsAnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Do not keep the console output file since export was successful
                m_jobParams.AddResultFileToSkip(THERMO_DATA_EXPORTER_CONSOLE_OUTPUT);

                var success = CopyResultsToTransferDirectory();
                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                return processingResult;

            }
            catch (Exception ex)
            {
                m_message = "Error in TopFDPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Parse the ThermoPeakDataExporter console output file to determine the ThermoPeakDataExporter version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <remarks></remarks>
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
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
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

                if (m_progress < actualProgress)
                {
                    m_progress = actualProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private CloseOutType StartThermoPeakDataExporter(string progLoc)
        {

            LogMessage("Running ThermoPeakDataExporter");

            // Future, if needed: filter by intensity, m/z, and/or scan

            var resultsFile = new FileInfo(Path.Combine(m_WorkDir, Dataset + ".tsv"));

            var cmdStr = Dataset + clsAnalysisResources.DOT_RAW_EXTENSION +
                         " /O:" + clsGlobal.PossiblyQuotePath(resultsFile.FullName);

            LogDebug(progLoc + " " + cmdStr);

            mCmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, THERMO_DATA_EXPORTER_CONSOLE_OUTPUT);

            m_progress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var processingSuccess = mCmdRunner.RunProgram(progLoc, cmdStr, "ThermoPeakDataExporter", true);

            // Parse the console output file one more time to check for errors
            ParseConsoleOutputFile(Path.Combine(m_WorkDir, THERMO_DATA_EXPORTER_CONSOLE_OUTPUT));

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

            m_StatusTools.UpdateAndWrite(m_progress);
            if (m_DebugLevel >= 3)
            {
                LogDebug("ThermoPeakDataExporter analysis complete");
            }

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDLLs = new List<string>
            {
                "ThermoRawFileReader.dll"
            };

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDLLs);

            return success;
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

            ParseConsoleOutputFile(Path.Combine(m_WorkDir, THERMO_DATA_EXPORTER_CONSOLE_OUTPUT));

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("ThermoPeakDataExporter");
        }

        #endregion
    }
}
