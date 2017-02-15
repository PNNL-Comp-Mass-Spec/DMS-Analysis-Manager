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

        private readonly string m_PeptideProphetRunnerLocation = string.Empty;
        private string m_ErrMsg = string.Empty;
        private short m_DebugLevel = 1;
        private string m_InputFile = string.Empty;

        protected clsRunDosProgram mCmdRunner;

        #endregion

        public event PeptideProphetRunningEventHandler PeptideProphetRunning;

        public delegate void PeptideProphetRunningEventHandler(string PepProphetStatus, float PercentComplete);

        #region "Properties"

        public short DebugLevel
        {
            get { return m_DebugLevel; }
            set { m_DebugLevel = value; }
        }

        public string ErrMsg
        {
            get
            {
                if (m_ErrMsg == null)
                {
                    return string.Empty;
                }
                else
                {
                    return m_ErrMsg;
                }
            }
        }

        public string InputFile
        {
            get { return m_InputFile; }
            set { m_InputFile = value; }
        }

        public string Enzyme { get; set; } = string.Empty;

        public string OutputFolderPath { get; set; } = string.Empty;

        #endregion

        #region "Methods"

        public clsPeptideProphetWrapper(string strPeptideProphetRunnerLocation)
        {
            m_PeptideProphetRunnerLocation = strPeptideProphetRunnerLocation;
        }

        public CloseOutType CallPeptideProphet()
        {
            string CmdStr = null;
            string strPeptideProphetConsoleOutputFilePath = null;

            try
            {
                m_ErrMsg = string.Empty;

                var ioInputFile = new FileInfo(m_InputFile);
                strPeptideProphetConsoleOutputFilePath = Path.Combine(ioInputFile.DirectoryName, "PeptideProphetConsoleOutput.txt");

                // verify that program file exists
                if (!File.Exists(m_PeptideProphetRunnerLocation))
                {
                    m_ErrMsg = "PeptideProphetRunner not found at " + m_PeptideProphetRunnerLocation;
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Set up and execute a program runner to run the Peptide Prophet Runner
                CmdStr = ioInputFile.FullName + " " + ioInputFile.DirectoryName + " /T:" + MAX_PEPTIDE_PROPHET_RUNTIME_MINUTES;
                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_PeptideProphetRunnerLocation + " " + CmdStr);
                }

                mCmdRunner = new clsRunDosProgram(ioInputFile.DirectoryName)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = strPeptideProphetConsoleOutputFilePath
                };
                mCmdRunner.ErrorEvent += CmdRunner_ErrorEvent;
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                if (!mCmdRunner.RunProgram(m_PeptideProphetRunnerLocation, CmdStr, "PeptideProphetRunner", true))
                {
                    m_ErrMsg = "Error running PeptideProphetRunner";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mCmdRunner.ExitCode != 0)
                {
                    m_ErrMsg = "Peptide prophet runner returned a non-zero error code: " + mCmdRunner.ExitCode.ToString();

                    // Parse the console output file for any lines that contain "Error"
                    // Append them to m_ErrMsg

                    var ioConsoleOutputFile = new FileInfo(strPeptideProphetConsoleOutputFilePath);
                    var blnErrorMessageFound = false;

                    if (ioConsoleOutputFile.Exists)
                    {
                        var srInFile = new StreamReader(new FileStream(ioConsoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                        while (!srInFile.EndOfStream)
                        {
                            string strLineIn = null;
                            strLineIn = srInFile.ReadLine();
                            if (!string.IsNullOrWhiteSpace(strLineIn))
                            {
                                if (strLineIn.ToLower().Contains("error"))
                                {
                                    m_ErrMsg += "; " + m_ErrMsg;
                                    blnErrorMessageFound = true;
                                }
                            }
                        }
                        srInFile.Close();
                    }

                    if (!blnErrorMessageFound)
                    {
                        m_ErrMsg += "; Unknown error message";
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (PeptideProphetRunning != null)
                {
                    PeptideProphetRunning("Complete", 100);
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                m_ErrMsg = "Exception while running peptide prophet: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void CmdRunner_ErrorEvent(string message, Exception ex)
        {
            m_ErrMsg = message;
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "PeptideProphetRunner: " + m_ErrMsg);
        }

        private DateTime dtLastStatusUpdate = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            //Update the status (limit the updates to every 5 seconds)
            if (System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5)
            {
                dtLastStatusUpdate = System.DateTime.UtcNow;
                if (PeptideProphetRunning != null)
                {
                    PeptideProphetRunning("Running", 50);
                }
            }
        }

        #endregion
    }
}
