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

namespace AnalysisManagerExtractionPlugin
{
    /// <summary>
    /// Calls the PeptideProphetRunner application
    /// </summary>
    /// <remarks></remarks>
    public class clsPeptideProphetWrapper
    {
        #region "Constants"

        public const int MAX_PEPTIDE_PROPHET_RUNTIME_MINUTES = 120;

        #endregion

        #region "Module variables"

        private readonly string m_PeptideProphetRunnerLocation;

        protected clsRunDosProgram mCmdRunner;

        #endregion

        public event PeptideProphetRunningEventHandler PeptideProphetRunning;

        public delegate void PeptideProphetRunningEventHandler(string PepProphetStatus, float PercentComplete);

        #region "Properties"

        public short DebugLevel { get; set; } = 1;

        public string ErrMsg { get; private set; } = string.Empty;

        public string InputFile { get; set; } = string.Empty;

        public string Enzyme { get; set; } = string.Empty;

        public string OutputFolderPath { get; set; } = string.Empty;

        #endregion

        #region "Methods"

        public clsPeptideProphetWrapper(string peptideProphetRunnerLocation)
        {
            m_PeptideProphetRunnerLocation = peptideProphetRunnerLocation;
        }

        public CloseOutType CallPeptideProphet()
        {
            try
            {
                ErrMsg = string.Empty;

                var ioInputFile = new FileInfo(InputFile);
                var peptideProphetConsoleOutputFilePath = Path.Combine(ioInputFile.DirectoryName, "PeptideProphetConsoleOutput.txt");

                // verify that program file exists
                if (!File.Exists(m_PeptideProphetRunnerLocation))
                {
                    ErrMsg = "PeptideProphetRunner not found at " + m_PeptideProphetRunnerLocation;
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Set up and execute a program runner to run the Peptide Prophet Runner
                var cmdStr = ioInputFile.FullName + " " + ioInputFile.DirectoryName + " /T:" + MAX_PEPTIDE_PROPHET_RUNTIME_MINUTES;
                if (DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_PeptideProphetRunnerLocation + " " + cmdStr);
                }

                mCmdRunner = new clsRunDosProgram(ioInputFile.DirectoryName)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = peptideProphetConsoleOutputFilePath
                };
                mCmdRunner.ErrorEvent += CmdRunner_ErrorEvent;
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                if (!mCmdRunner.RunProgram(m_PeptideProphetRunnerLocation, cmdStr, "PeptideProphetRunner", true))
                {
                   ErrMsg = "Error running PeptideProphetRunner";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mCmdRunner.ExitCode != 0)
                {
                    ErrMsg = "Peptide prophet runner returned a non-zero error code: " + mCmdRunner.ExitCode;

                    // Parse the console output file for any lines that contain "Error"
                    // Append them to m_ErrMsg

                    var ioConsoleOutputFile = new FileInfo(peptideProphetConsoleOutputFilePath);
                    var errorMessageFound = false;

                    if (ioConsoleOutputFile.Exists)
                    {
                        var srInFile = new StreamReader(new FileStream(ioConsoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                        while (!srInFile.EndOfStream)
                        {
                            var lineIn = srInFile.ReadLine();
                            if (!string.IsNullOrWhiteSpace(lineIn))
                            {
                                if (lineIn.ToLower().Contains("error"))
                                {
                                    ErrMsg += "; " + ErrMsg;
                                    errorMessageFound = true;
                                }
                            }
                        }
                        srInFile.Close();
                    }

                    if (!errorMessageFound)
                    {
                        ErrMsg += "; Unknown error message";
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                PeptideProphetRunning?.Invoke("Complete", 100);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                ErrMsg = "Exception while running peptide prophet: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void CmdRunner_ErrorEvent(string message, Exception ex)
        {
            ErrMsg = message;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "PeptideProphetRunner: " + ErrMsg);
        }

        private DateTime dtLastStatusUpdate = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            // Update the status (limit the updates to every 5 seconds)
            if (DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5)
            {
                dtLastStatusUpdate = DateTime.UtcNow;
                PeptideProphetRunning?.Invoke("Running", 50);
            }
        }

        #endregion
    }
}
