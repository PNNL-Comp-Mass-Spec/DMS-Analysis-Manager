//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISM;

namespace AnalysisManagerXTandemPlugIn
{
    /// <summary>
    /// Class for running XTandem analysis
    /// </summary>
    public class AnalysisToolRunnerXT : AnalysisToolRunnerBase
    {
        // Ignore Spelling: xt

        private const string XTANDEM_CONSOLE_OUTPUT = "XTandem_ConsoleOutput.txt";

        private const int PROGRESS_PCT_XTANDEM_STARTING = 1;
        private const int PROGRESS_PCT_XTANDEM_LOADING_SPECTRA = 5;
        private const int PROGRESS_PCT_XTANDEM_COMPUTING_MODELS = 10;
        private const int PROGRESS_PCT_XTANDEM_REFINEMENT = 50;
        private const int PROGRESS_PCT_XTANDEM_REFINEMENT_PARTIAL_CLEAVAGE = 50;
        private const int PROGRESS_PCT_XTANDEM_REFINEMENT_UNANTICIPATED_CLEAVAGE = 70;
        private const int PROGRESS_PCT_XTANDEM_REFINEMENT_FINISHING = 85;
        private const int PROGRESS_PCT_XTANDEM_MERGING_RESULTS = 90;
        private const int PROGRESS_PCT_XTANDEM_CREATING_REPORT = 95;
        private const int PROGRESS_PCT_XTANDEM_COMPLETE = 99;

        private RunDosProgram mCmdRunner;

        private bool mToolVersionWritten;
        private string mXTandemVersion = string.Empty;

        /// <summary>
        /// This is initially set to -1; it will be updated to the value reported by "Valid models" in the X!Tandem Console Output file
        /// </summary>
        private int mXTandemResultsCount;

        /// <summary>
        /// Runs XTandem tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Note: we will store the XTandem version info in the database after the first line is written to file XTandem_ConsoleOutput.txt
            mToolVersionWritten = false;
            mXTandemVersion = string.Empty;
            mXTandemResultsCount = -1;
            var noResults = false;

            // Make sure the _DTA.txt file is valid
            if (!ValidateCDTAFile())
            {
                return CloseOutType.CLOSEOUT_NO_DTA_FILES;
            }

            LogMessage("Running XTandem");

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (mDebugLevel > 4)
            {
                LogDebug("AnalysisToolRunnerXT.OperateAnalysisTool(): Enter");
            }

            // Define the path to the X!Tandem .Exe
            var progLoc = mMgrParams.GetParam("xtProgLoc");

            if (progLoc.Length == 0)
            {
                mMessage = "Parameter 'xtProgLoc' not defined for this manager";
                LogError(mMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Check whether we need to update the program location to use a specific version of X!Tandem
            progLoc = DetermineXTandemProgramLocation(progLoc);

            if (string.IsNullOrWhiteSpace(progLoc))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!File.Exists(progLoc))
            {
                mMessage = "Cannot find XTandem program file";
                LogError(mMessage + ": " + progLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Set up and execute a program runner to run X!Tandem
            const string arguments = "input.xml";

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = true;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, XTANDEM_CONSOLE_OUTPUT);

            mProgress = PROGRESS_PCT_XTANDEM_STARTING;

            var processingSuccess = mCmdRunner.RunProgram(progLoc, arguments, "XTandem", true);

            // Parse the console output file one more time to determine the number of peptides found
            ParseConsoleOutputFile(Path.Combine(mWorkDir, XTANDEM_CONSOLE_OUTPUT));

            if (!mToolVersionWritten)
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!processingSuccess)
            {
                LogError("Error running XTandem, job " + mJob);

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("Tandem.exe returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to Tandem.exe failed (but exit code is 0)");
                }

                // Note: Job 553883 returned error code -1073740777, which indicated that the _xt.xml file was not fully written

                // Move the source files and any results to the Failed Job folder
                // Useful for debugging XTandem problems
                CopyFailedResultsToArchiveDirectory();

                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (mXTandemResultsCount < 0)
            {
                mMessage = @"X!Tandem did not report a ""Valid models"" count";
                LogError(mMessage);
                noResults = true;
            }
            else if (mXTandemResultsCount == 0)
            {
                // Storing "No results above threshold" in mMessage will result in the job being assigned state No Export (14) in DMS
                // See stored procedure update_job_state
                mMessage = NO_RESULTS_ABOVE_THRESHOLD;
                LogError(mMessage);
                noResults = true;
            }

            // Stop the job timer
            mStopTime = DateTime.UtcNow;

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Make sure objects are released
            AppUtils.GarbageCollectNow();

            // Zip the output file
            var result = ZipMainOutputFile();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Something went wrong
                // In order to help diagnose things, move the output files into the results directory,
                // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                CopyFailedResultsToArchiveDirectory();
                return result;
            }

            var success = CopyResultsToTransferDirectory();

            if (!success)
                return CloseOutType.CLOSEOUT_FAILED;

            return noResults ? CloseOutType.CLOSEOUT_NO_DATA : CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileToSkip(Dataset + "_dta.zip");
            mJobParams.AddResultFileToSkip(Dataset + "_dta.txt");

            base.CopyFailedResultsToArchiveDirectory();
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var toolVersionInfo = mXTandemVersion;

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(mMgrParams.GetParam("xtProgLoc"))
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        private string DetermineXTandemProgramLocation(string progLoc)
        {
            // Check whether the settings file specifies that a specific version of the step tool be used
            var xtandemStepToolVersion = mJobParams.GetParam("XTandem_Version");

            if (!string.IsNullOrWhiteSpace(xtandemStepToolVersion))
            {
                // progLoc is currently "C:\DMS_Programs\DMS5\XTandem\bin\Tandem.exe" or "C:\DMS_Programs\XTandem\bin\x64\Tandem.exe"
                // xtandemStepToolVersion will be similar to "v2011.12.1.1"
                // Insert the specific version just before \bin\ in progLoc

                var insertIndex = progLoc.ToLower().IndexOf("\\bin\\", StringComparison.Ordinal);

                if (insertIndex > 0)
                {
                    var newProgLoc = Path.Combine(progLoc.Substring(0, insertIndex), xtandemStepToolVersion);
                    newProgLoc = Path.Combine(newProgLoc, progLoc.Substring(insertIndex + 1));
                    progLoc = newProgLoc;
                }
                else
                {
                    mMessage = "XTandem program path does not contain \\bin\\";
                    LogError(mMessage + ": " + progLoc);
                    progLoc = string.Empty;
                }
            }

            return progLoc;
        }

        /// <summary>
        /// Parse the X!Tandem console output file to determine the X!Tandem version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            var valueMatcher = new Regex(@"= *(\d+)", RegexOptions.Compiled);

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

                if (mDebugLevel >= 3)
                {
                    LogDebug("Parsing file " + consoleOutputFilePath);
                }

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var linesRead = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    if (linesRead <= 4 && string.IsNullOrEmpty(mXTandemVersion) && dataLine.StartsWith("X!"))
                    {
                        // Originally the first line was the X!Tandem version
                        // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                        // The third or fourth line should be the X!Tandem version

                        if (mDebugLevel >= 2)
                        {
                            LogDebug("X!Tandem version: " + dataLine);
                        }

                        mXTandemVersion = dataLine;
                    }
                    else
                    {
                        // Update progress if the line starts with one of the expected phrases
                        if (dataLine.StartsWith("Loading spectra"))
                        {
                            mProgress = PROGRESS_PCT_XTANDEM_LOADING_SPECTRA;
                        }
                        else if (dataLine.StartsWith("Computing models"))
                        {
                            mProgress = PROGRESS_PCT_XTANDEM_COMPUTING_MODELS;
                        }
                        else if (dataLine.StartsWith("Model refinement"))
                        {
                            mProgress = PROGRESS_PCT_XTANDEM_REFINEMENT;
                        }
                        else if (dataLine.StartsWith("\tpartial cleavage"))
                        {
                            mProgress = PROGRESS_PCT_XTANDEM_REFINEMENT_PARTIAL_CLEAVAGE;
                        }
                        else if (dataLine.StartsWith("\tunanticipated cleavage"))
                        {
                            mProgress = PROGRESS_PCT_XTANDEM_REFINEMENT_UNANTICIPATED_CLEAVAGE;
                        }
                        else if (dataLine.StartsWith("\tfinishing refinement "))
                        {
                            mProgress = PROGRESS_PCT_XTANDEM_REFINEMENT_FINISHING;
                        }
                        else if (dataLine.StartsWith("Merging results"))
                        {
                            mProgress = PROGRESS_PCT_XTANDEM_MERGING_RESULTS;
                        }
                        else if (dataLine.StartsWith("Creating report"))
                        {
                            mProgress = PROGRESS_PCT_XTANDEM_CREATING_REPORT;
                        }
                        else if (dataLine.StartsWith("Estimated false positives"))
                        {
                            mProgress = PROGRESS_PCT_XTANDEM_COMPLETE;
                        }
                        else if (dataLine.StartsWith("Valid models"))
                        {
                            var match = valueMatcher.Match(dataLine);

                            if (match.Success)
                            {
                                int.TryParse(match.Groups[1].Value, out mXTandemResultsCount);
                            }
                        }
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
        /// Zips concatenated XML output file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType ZipMainOutputFile()
        {
            try
            {
                var fileList = Directory.GetFiles(mWorkDir, "*_xt.xml");

                foreach (var file in fileList)
                {
                    var filePath = Path.Combine(mWorkDir, Path.GetFileName(file));

                    if (!ZipFile(filePath, true))
                    {
                        LogError("Error zipping output files");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception zipping output files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make sure the XML output files have been deleted (the call to MyBase.ZipFile() above should have done this)
            try
            {
                var fileList = Directory.GetFiles(mWorkDir, "*_xt.xml");

                foreach (var file in fileList)
                {
                    File.SetAttributes(file, File.GetAttributes(file) & (~FileAttributes.ReadOnly));
                    File.Delete(file);
                }
            }
            catch (Exception ex)
            {
                LogError("AnalysisToolRunnerXT.ZipMainOutputFile, Error deleting _xt.xml file, job " + mJob + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(mWorkDir, XTANDEM_CONSOLE_OUTPUT));

                if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mXTandemVersion))
                {
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                LogProgress("XTandem");
            }
        }
    }
}