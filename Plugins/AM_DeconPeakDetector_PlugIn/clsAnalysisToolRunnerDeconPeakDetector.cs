//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 05/23/2014
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerDeconPeakDetectorPlugIn
{
    /// <summary>
    /// Class for running the Decon Peak Detector
    /// </summary>
    public class clsAnalysisToolRunnerDeconPeakDetector : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        private const string DECON_PEAK_DETECTOR_EXE_NAME = "HammerOrDeconSimplePeakDetector.exe";

        private const string DECON_PEAK_DETECTOR_CONSOLE_OUTPUT = "DeconPeakDetector_ConsoleOutput.txt";

        private const float PROGRESS_PCT_STARTING = 1;
        private const float PROGRESS_PCT_COMPLETE = 99;

        #endregion

        #region "Module Variables"

        private string mConsoleOutputErrorMsg;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs DeconPeakDetector tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            try
            {
                //Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerDeconPeakDetector.RunTool(): Enter");
                }

                // Verify that program files exist

                // Determine the path to the PeakDetector program
                string progLoc = null;
                progLoc = DetermineProgramLocation("DeconPeakDetectorProgLoc", DECON_PEAK_DETECTOR_EXE_NAME);

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the PeakDetector version info in the database
                m_message = string.Empty;
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Error determining DeconPeakDetector version";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run DeconPeakDetector
                var blnSuccess = RunDeconPeakDetector(progLoc);

                if (blnSuccess)
                {
                    // Look for the DeconPeakDetector results file
                    var peakDetectorResultsFilePath = Path.Combine(m_WorkDir, m_Dataset + "_peaks.txt");

                    var fiResultsFile = new FileInfo(peakDetectorResultsFilePath);

                    if (!fiResultsFile.Exists)
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            m_message = "DeconPeakDetector results file not found: " + Path.GetFileName(peakDetectorResultsFilePath);
                        }
                        blnSuccess = false;
                    }
                }

                if (blnSuccess)
                {
                    m_jobParams.AddResultFileExtensionToSkip("_ConsoleOutput.txt");
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;

                //Add the current job data to the summary file
                UpdateSummaryFile();

                //Make sure objects are released
                Thread.Sleep(500);
                PRISM.clsProgRunner.GarbageCollectNow();

                if (!blnSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                LogError("Error in DeconPeakDetectorPlugin->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        // Example Console output
        //
        // Started Peak Detector
        // There are 6695 MS1 scans
        // Using Decon Peak Detector
        //
        // Peak creation progress: 0%
        // Peak creation progress: 1%
        // Peak creation progress: 2%
        // Peak creation progress: 2%
        // Peak creation progress: 3%
        // Peak creation progress: 4%
        private Regex reProgress = new Regex(@"Peak creation progress: (?<Progress>\d+)%", RegexOptions.Compiled);

        /// <summary>
        /// Parse the DeconPeakDetector console output file to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Parsing file " + strConsoleOutputFilePath);
                }

                string strLineIn = null;
                var peakDetectProgress = 0;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        var reMatch = reProgress.Match(strLineIn);

                        if (reMatch.Success)
                        {
                            peakDetectProgress = int.Parse(reMatch.Groups["Progress"].Value);
                        }
                    }
                }

                var sngActualProgress = ComputeIncrementalProgress(PROGRESS_PCT_STARTING, PROGRESS_PCT_COMPLETE, peakDetectProgress, 100);

                if (m_progress < sngActualProgress)
                {
                    m_progress = sngActualProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogWarning("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private bool RunDeconPeakDetector(string strPeakDetectorProgLoc)
        {
            string CmdStr = null;
            var blnSuccess = false;

            var peakDetectorParamFileName = m_jobParams.GetJobParameter("PeakDetectorParamFile", "");
            var paramFilePath = Path.Combine(m_WorkDir, peakDetectorParamFileName);

            mConsoleOutputErrorMsg = string.Empty;

            var rawDataType = m_jobParams.GetParam("RawDataType");
            var eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType);

            if (eRawDataType == clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile)
            {
                m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION);
            }
            else
            {
                m_message = "The DeconPeakDetector presently only supports Thermo .Raw files";
                return false;
            }

            LogMessage("Running DeconPeakDetector");

            //Set up and execute a program runner to run the Peak Detector
            CmdStr = m_Dataset + clsAnalysisResources.DOT_RAW_EXTENSION;
            CmdStr += " /P:" + PossiblyQuotePath(paramFilePath);
            CmdStr += " /O:" + PossiblyQuotePath(m_WorkDir);

            LogDebug(strPeakDetectorProgLoc + " " + CmdStr);

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, DECON_PEAK_DETECTOR_CONSOLE_OUTPUT);

            m_progress = PROGRESS_PCT_STARTING;

            blnSuccess = mCmdRunner.RunProgram(strPeakDetectorProgLoc, CmdStr, "PeakDetector", true);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!blnSuccess)
            {
                LogError("Error running DeconPeakDetector");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("PeakDetector returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to PeakDetector failed (but exit code is 0)");
                }

                return false;
            }

            m_progress = PROGRESS_PCT_COMPLETE;
            m_StatusTools.UpdateAndWrite(m_progress);
            if (m_DebugLevel >= 3)
            {
                LogDebug("DeconPeakDetector Search Complete");
            }

            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string strPeakDetectorPath)
        {
            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of the DeconConsole application
            var fiPeakDetector = new FileInfo(strPeakDetectorPath);

            var blnSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, fiPeakDetector.FullName);
            if (!blnSuccess)
                return false;

            var dllPath = Path.Combine(fiPeakDetector.Directory.FullName, "SimplePeakDetectorEngine.dll");
            base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, dllPath);

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(strPeakDetectorPath));
            ioToolFiles.Add(new FileInfo(dllPath));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message, ex);
                return false;
            }
        }

        #endregion

        #region "Event Handlers"

        private DateTime dtLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15)
            {
                dtLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(m_WorkDir, DECON_PEAK_DETECTOR_CONSOLE_OUTPUT));

                LogProgress("DeconPeakDetector");
            }
        }

        #endregion
    }
}
