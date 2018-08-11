//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using MSDataFileReader;

namespace AnalysisManagerTopFDPlugIn
{
    /// <summary>
    /// Class for running TopFD analysis
    /// </summary>
    public class clsAnalysisToolRunnerTopFD : clsAnalysisToolRunnerBase
    {

        #region "Constants"

        /// <summary>
        /// _ms2.msalign file created by TopFD
        /// </summary>
        private const string MSALIGN_FILE_SUFFIX = "_ms2.msalign";


        private const string TopFD_CONSOLE_OUTPUT = "TopFD_ConsoleOutput.txt";
        private const string TopFD_EXE_NAME = "topfd.exe";

        private const float PROGRESS_PCT_STARTING = 1;
        private const float PROGRESS_PCT_COMPLETE = 99;

        #endregion

        #region "Module Variables"

        private bool mToolVersionWritten;

        // Populate this with a tool version reported to the console
        private string mTopFDVersion;

        private string mTopFDProgLoc;
        private string mConsoleOutputErrorMsg;

        private DateTime mLastConsoleOutputParse;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs TopFD tool
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
                    LogDebug("clsAnalysisToolRunnerTopFD.RunTool(): Enter");
                }

                // Initialize classwide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to the TopFD program
                mTopFDProgLoc = DetermineProgramLocation("TopFDProgLoc", TopFD_EXE_NAME);

                if (string.IsNullOrWhiteSpace(mTopFDProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the TopFD version info in the database after the first line is written to file TopFD_ConsoleOutput.txt
                mToolVersionWritten = false;
                mTopFDVersion = string.Empty;
                mConsoleOutputErrorMsg = string.Empty;

                var processingResult = RunTopFD(mTopFDProgLoc);

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                PRISM.clsProgRunner.GarbageCollectNow();

                // Trim the console output file to remove the majority of the % finished messages
                TrimConsoleOutputFile(Path.Combine(m_WorkDir, TopFD_CONSOLE_OUTPUT));

                if (!clsAnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

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

        // Example Console output:
        //
        // MS-Deconv 0.8.0.7199 2012-01-16
        // ********* parameters begin **********
        // output file format:    msalign
        // data type:             centroided
        // orignal precursor:     false
        // maximum charge:        30
        // maximum mass:          49000.0
        // m/z error tolerance:   0.02
        // sn ratio:              1.0
        // keep unused peak:      false
        // output multiple mass:  false
        // ********* parameters end   **********
        // Processing spectrum Scan_2...           0% finished.
        // Processing spectrum Scan_3...           0% finished.
        // Processing spectrum Scan_4...           0% finished.
        // Deconvolution finished.
        // Result is in Syne_LI_CID_09092011_TopFD.msalign
        private readonly Regex reExtractPercentFinished = new Regex(@"(\d+)% finished", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the TopFD console output file to determine the TopFD version and to track the search progress
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

                short intActualProgress = 0;

                mConsoleOutputErrorMsg = string.Empty;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var intLinesRead = 0;
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();
                        intLinesRead += 1;

                        if (!string.IsNullOrWhiteSpace(strLineIn))
                        {
                            if (intLinesRead <= 3)
                            {
                                // Originally the first line was the MS-Deconv version
                                // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                                // The third line is the TopFD version
                                if (string.IsNullOrEmpty(mTopFDVersion) && strLineIn.ToLower().Contains("ms-deconv"))
                                {
                                    if (m_DebugLevel >= 2 && string.IsNullOrWhiteSpace(mTopFDVersion))
                                    {
                                        LogDebug("TopFD version: " + strLineIn);
                                    }

                                    mTopFDVersion = string.Copy(strLineIn);
                                }
                                else
                                {
                                    if (strLineIn.ToLower().Contains("error"))
                                    {
                                        if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                        {
                                            mConsoleOutputErrorMsg = "Error running TopFD:";
                                        }
                                        mConsoleOutputErrorMsg += "; " + strLineIn;
                                    }
                                }
                            }
                            else
                            {
                                // Update progress if the line starts with Processing spectrum
                                if (strLineIn.StartsWith("Processing spectrum"))
                                {
                                    var oMatch = reExtractPercentFinished.Match(strLineIn);
                                    if (oMatch.Success)
                                    {
                                        if (short.TryParse(oMatch.Groups[1].Value, out var intProgress))
                                        {
                                            intActualProgress = intProgress;
                                        }
                                    }
                                }
                                else if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                {
                                    if (strLineIn.ToLower().StartsWith("error"))
                                    {
                                        mConsoleOutputErrorMsg += "; " + strLineIn;
                                    }
                                }
                            }
                        }
                    }
                }

                if (m_progress < intActualProgress)
                {
                    m_progress = intActualProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private CloseOutType RunTopFD(string progLoc)
        {

            LogMessage("Running TopFD");

            var missingMs1Spectra = m_jobParams.GetJobParameter("TopFDMissingMS1Spectra", false);

            var cmdStr = " " + m_Dataset + clsAnalysisResources.DOT_MZML_EXTENSION;

            if (missingMs1Spectra)
            {
                cmdStr += " --missing-level-one";
            }

            LogDebug(progLoc + " " + cmdStr);

            mCmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, TopFD_CONSOLE_OUTPUT);

            m_progress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var processingSuccess = mCmdRunner.RunProgram(progLoc, cmdStr, "TopFD", true);

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mTopFDVersion))
                {
                    ParseConsoleOutputFile(Path.Combine(m_WorkDir, TopFD_CONSOLE_OUTPUT));
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }


            CloseOutType eResult;
            if (!processingSuccess)
            {
                LogError("Error running TopPIC");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("TopPIC returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to TopPIC failed (but exit code is 0)");
                }

                eResult = CloseOutType.CLOSEOUT_FAILED;
            }
            else
            {
                // Make sure the output file was created and is not zero-bytes
                // If the input .mzML file only has MS spectra and no MS/MS spectra, the output file will be empty
                var resultsFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset, MSALIGN_FILE_SUFFIX));
                if (!resultsFile.Exists)
                {
                    var msg = "TopFD results file not found";
                    LogError(msg, msg + " (" + resultsFile + ")");

                    eResult = CloseOutType.CLOSEOUT_FAILED;
                }
                else if (resultsFile.Length == 0)
                {
                    var msg = "TopFD results file is empty; assure that the input .mzML file has MS/MS spectra";
                    LogError(msg, msg + " (" + resultsFile + ")");

                    eResult = CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    m_StatusTools.UpdateAndWrite(m_progress);
                    if (m_DebugLevel >= 3)
                    {
                        LogDebug("TopFD analysis complete");
                    }
                    eResult = CloseOutType.CLOSEOUT_SUCCESS;
                }

            }

            return eResult;

        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var strToolVersionInfo = string.Copy(mTopFDVersion);

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo> {
                new FileInfo(mTopFDProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        private readonly Regex reExtractScan = new Regex(@"Processing spectrum Scan_(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Reads the console output file and removes the majority of the percent finished messages
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void TrimConsoleOutputFile(string strConsoleOutputFilePath)
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
                    LogDebug("Trimming console output file at " + strConsoleOutputFilePath);
                }

                var strMostRecentProgressLine = string.Empty;
                var strMostRecentProgressLineWritten = string.Empty;

                var strTrimmedFilePath = strConsoleOutputFilePath + ".trimmed";

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var swOutFile = new StreamWriter(new FileStream(strTrimmedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var intScanNumberOutputThreshold = 0;
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                        {
                            swOutFile.WriteLine(strLineIn);
                            continue;
                        }

                        var blnKeepLine = true;

                        var oMatch = reExtractScan.Match(strLineIn);
                        if (oMatch.Success)
                        {
                            if (int.TryParse(oMatch.Groups[1].Value, out var intScanNumber))
                            {
                                if (intScanNumber < intScanNumberOutputThreshold)
                                {
                                    blnKeepLine = false;
                                }
                                else
                                {
                                    // Write out this line and bump up intScanNumberOutputThreshold by 100
                                    intScanNumberOutputThreshold += 100;
                                    strMostRecentProgressLineWritten = string.Copy(strLineIn);
                                }
                            }
                            strMostRecentProgressLine = string.Copy(strLineIn);
                        }
                        else if (strLineIn.StartsWith("Deconvolution finished"))
                        {
                            // Possibly write out the most recent progress line
                            if (!clsGlobal.IsMatch(strMostRecentProgressLine, strMostRecentProgressLineWritten))
                            {
                                swOutFile.WriteLine(strMostRecentProgressLine);
                            }
                        }

                        if (blnKeepLine)
                        {
                            swOutFile.WriteLine(strLineIn);
                        }
                    }
                }

                // Swap the files

                try
                {
                    File.Delete(strConsoleOutputFilePath);
                    File.Move(strTrimmedFilePath, strConsoleOutputFilePath);
                }
                catch (Exception ex)
                {
                    if (m_DebugLevel >= 1)
                    {
                        LogError("Error replacing original console output file (" + strConsoleOutputFilePath + ") with trimmed version: " + ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error trimming console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
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

            ParseConsoleOutputFile(Path.Combine(m_WorkDir, TopFD_CONSOLE_OUTPUT));

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mTopFDVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("TopFD");
        }

        #endregion
    }
}
