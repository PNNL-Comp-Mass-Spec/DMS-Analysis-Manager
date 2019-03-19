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
                    LogDebug("clsAnalysisToolRunnerDeconPeakDetector.RunTool(): Enter");
                }

                // Verify that program files exist

                // Determine the path to the PeakDetector program
                var progLoc = DetermineProgramLocation("DeconPeakDetectorProgLoc", DECON_PEAK_DETECTOR_EXE_NAME);

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the PeakDetector version info in the database
                mMessage = string.Empty;
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Error determining DeconPeakDetector version";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run DeconPeakDetector
                var blnSuccess = RunDeconPeakDetector(progLoc);

                if (blnSuccess)
                {
                    // Look for the DeconPeakDetector results file
                    var peakDetectorResultsFilePath = Path.Combine(mWorkDir, mDatasetName + "_peaks.txt");

                    var fiResultsFile = new FileInfo(peakDetectorResultsFilePath);

                    if (!fiResultsFile.Exists)
                    {
                        if (string.IsNullOrEmpty(mMessage))
                        {
                            mMessage = "DeconPeakDetector results file not found: " + Path.GetFileName(peakDetectorResultsFilePath);
                        }
                        blnSuccess = false;
                    }
                }

                if (blnSuccess)
                {
                    mJobParams.AddResultFileExtensionToSkip("_ConsoleOutput.txt");
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.ProgRunner.GarbageCollectNow();

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
        private readonly Regex reProgress = new Regex(@"Peak creation progress: (?<Progress>\d+)%", RegexOptions.Compiled);

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

                var peakDetectProgress = 0;

                using (var reader = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var reMatch = reProgress.Match(dataLine);

                        if (reMatch.Success)
                        {
                            peakDetectProgress = int.Parse(reMatch.Groups["Progress"].Value);
                        }
                    }
                }

                var sngActualProgress = ComputeIncrementalProgress(PROGRESS_PCT_STARTING, PROGRESS_PCT_COMPLETE, peakDetectProgress, 100);

                if (mProgress < sngActualProgress)
                {
                    mProgress = sngActualProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogWarning("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private bool RunDeconPeakDetector(string strPeakDetectorProgLoc)
        {
            var peakDetectorParamFileName = mJobParams.GetJobParameter("PeakDetectorParamFile", "");
            var paramFilePath = Path.Combine(mWorkDir, peakDetectorParamFileName);

            mConsoleOutputErrorMsg = string.Empty;

            var rawDataType = mJobParams.GetParam("RawDataType");
            var eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType);

            if (eRawDataType == clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile)
            {
                mJobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION);
            }
            else
            {
                mMessage = "The DeconPeakDetector presently only supports Thermo .Raw files";
                return false;
            }

            LogMessage("Running DeconPeakDetector");

            // Set up and execute a program runner to run the Peak Detector
            var arguments = mDatasetName + clsAnalysisResources.DOT_RAW_EXTENSION +
                            " /P:" + PossiblyQuotePath(paramFilePath) +
                            " /O:" + PossiblyQuotePath(mWorkDir);

            LogDebug(strPeakDetectorProgLoc + " " + arguments);

            mCmdRunner = new clsRunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, DECON_PEAK_DETECTOR_CONSOLE_OUTPUT);

            mProgress = PROGRESS_PCT_STARTING;

            var blnSuccess = mCmdRunner.RunProgram(strPeakDetectorProgLoc, arguments, "PeakDetector", true);

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

            mProgress = PROGRESS_PCT_COMPLETE;
            mStatusTools.UpdateAndWrite(mProgress);
            if (mDebugLevel >= 3)
            {
                LogDebug("DeconPeakDetector Search Complete");
            }

            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDLLs = new List<string>
            {
                "SimplePeakDetectorEngine.dll"
            };

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDLLs);

            return success;
        }

        #endregion

        #region "Event Handlers"

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(mWorkDir, DECON_PEAK_DETECTOR_CONSOLE_OUTPUT));

                LogProgress("DeconPeakDetector");
            }
        }

        #endregion
    }
}
