//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 07/12/2007
//
// Program converted from original version written by J.D. Sandoval, PNNL.
// Conversion performed as part of upgrade to VB.Net 2005, modification for use with manager and broker databases
//
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISM;

namespace AnalysisManagerExtractionPlugin
{
    /// <summary>
    /// Calls the PeptideProphetRunner application
    /// </summary>
    public class PeptideProphetWrapper : EventNotifier
    {
        public const int MAX_PEPTIDE_PROPHET_RUNTIME_MINUTES = 120;

        private readonly string mPeptideProphetRunnerLocation;

        private RunDosProgram mCmdRunner;

        /// <summary>
        /// Even used to report progress
        /// </summary>
        public event PeptideProphetRunningEventHandler PeptideProphetRunning;

        public delegate void PeptideProphetRunningEventHandler(string pepProphetStatus, float percentComplete);

        public short DebugLevel { get; set; } = 1;

        public string ErrorMessage { get; private set; } = string.Empty;

        public string InputFile { get; set; } = string.Empty;

        public string Enzyme { get; set; } = string.Empty;

        public string OutputFolderPath { get; set; } = string.Empty;

        public PeptideProphetWrapper(string peptideProphetRunnerLocation)
        {
            mPeptideProphetRunnerLocation = peptideProphetRunnerLocation;
        }

        /// <summary>
        /// Run PeptideProphet using PeptideProphetRunner.exe
        /// </summary>
        /// <remarks>
        /// Note that PeptideProphetRunner.exe and PeptideProphetLibrary.dll compiled in 2021 require .NET 4.8 and the
        /// Microsoft Visual C++ 2015-2019 Redistributable (x86) (files msvcp140.dll and msvcr140)
        /// </remarks>
        public CloseOutType CallPeptideProphet()
        {
            try
            {
                ErrorMessage = string.Empty;

                var inputFile = new FileInfo(InputFile);

                if (inputFile.Directory == null)
                {
                    ReportError("Unable to determine the parent directory of the input file for peptide prophet: " + InputFile);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                var peptideProphetConsoleOutputFilePath = Path.Combine(inputFile.Directory.FullName, "PeptideProphetConsoleOutput.txt");

                // Verify that program file exists
                if (!File.Exists(mPeptideProphetRunnerLocation))
                {
                    ReportError("PeptideProphetRunner not found at " + mPeptideProphetRunnerLocation);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Set up and execute a program runner to run the Peptide Prophet Runner
                var arguments =
                    Global.PossiblyQuotePath(inputFile.FullName) + " " +
                    Global.PossiblyQuotePath(inputFile.Directory.FullName) +
                    " /T:" + MAX_PEPTIDE_PROPHET_RUNTIME_MINUTES;

                if (DebugLevel >= 2)
                {
                    OnDebugEvent(mPeptideProphetRunnerLocation + " " + arguments);
                }

                mCmdRunner = new RunDosProgram(inputFile.Directory.FullName, DebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = peptideProphetConsoleOutputFilePath
                };
                mCmdRunner.ErrorEvent += CmdRunner_ErrorEvent;
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                if (!mCmdRunner.RunProgram(mPeptideProphetRunnerLocation, arguments, "PeptideProphetRunner", true))
                {
                    ReportError("Error running PeptideProphetRunner");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mCmdRunner.ExitCode != 0)
                {
                    ReportError("Peptide prophet runner returned a non-zero error code: " + mCmdRunner.ExitCode);

                    // Parse the console output file for any lines that contain "Error"
                    // Append them to mErrMsg

                    var consoleOutputFile = new FileInfo(peptideProphetConsoleOutputFilePath);
                    var errorMessageFound = false;

                    if (consoleOutputFile.Exists)
                    {
                        var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                        while (!reader.EndOfStream)
                        {
                            var lineIn = reader.ReadLine();

                            if (!string.IsNullOrWhiteSpace(lineIn))
                            {
                                if (lineIn.IndexOf("error", StringComparison.OrdinalIgnoreCase) < 0)
                                {
                                    continue;
                                }

                                ErrorMessage = Global.AppendToComment(ErrorMessage, lineIn);

                                OnWarningEvent(lineIn);
                                errorMessageFound = true;
                            }
                        }
                        reader.Close();
                    }

                    if (!errorMessageFound)
                    {
                        ErrorMessage += "; Unknown error message";
                        OnWarningEvent("Unknown PeptideProphet error message");
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                PeptideProphetRunning?.Invoke("Complete", 100);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                ReportError("Exception while running peptide prophet: " + ex.Message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void ReportError(string message, Exception ex = null)
        {
            ErrorMessage = message;
            OnErrorEvent(ErrorMessage, ex);
        }

        private void CmdRunner_ErrorEvent(string message, Exception ex)
        {
            ReportError("PeptideProphetRunner: " + ErrorMessage);
        }

        private DateTime mLastStatusUpdate = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks>Always reports 50% complete</remarks>
        private void CmdRunner_LoopWaiting()
        {
            // Update the status (limit the updates to every 5 seconds)
            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds >= 5)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                PeptideProphetRunning?.Invoke("Running", 50);
            }
        }
    }
}
