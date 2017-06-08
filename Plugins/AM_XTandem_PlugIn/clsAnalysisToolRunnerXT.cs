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
using System.Threading;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerXTandemPlugIn
{
    /// <summary>
    /// Class for running XTandem analysis
    /// </summary>
    /// <remarks></remarks>
    public class clsAnalysisToolRunnerXT : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

        protected const string XTANDEM_CONSOLE_OUTPUT = "XTandem_ConsoleOutput.txt";

        protected const float PROGRESS_PCT_XTANDEM_STARTING = 1;
        protected const float PROGRESS_PCT_XTANDEM_LOADING_SPECTRA = 5;
        protected const float PROGRESS_PCT_XTANDEM_COMPUTING_MODELS = 10;
        protected const float PROGRESS_PCT_XTANDEM_REFINEMENT = 50;
        protected const float PROGRESS_PCT_XTANDEM_REFINEMENT_PARTIAL_CLEAVAGE = 50;
        protected const float PROGRESS_PCT_XTANDEM_REFINEMENT_UNANTICIPATED_CLEAVAGE = 70;
        protected const float PROGRESS_PCT_XTANDEM_REFINEMENT_FINISHING = 85;
        protected const float PROGRESS_PCT_XTANDEM_MERGING_RESULTS = 90;
        protected const float PROGRESS_PCT_XTANDEM_CREATING_REPORT = 95;
        protected const float PROGRESS_PCT_XTANDEM_COMPLETE = 99;

        protected clsRunDosProgram mCmdRunner;

        protected bool mToolVersionWritten;
        protected string mXTandemVersion = string.Empty;
        // This is initially set to -1; it will be updated to the value reported by "Valid models" in the X!Tandem Console Output file
        protected int mXTandemResultsCount;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs XTandem tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
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

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (m_DebugLevel > 4)
            {
                LogDebug("clsAnalysisToolRunnerXT.OperateAnalysisTool(): Enter");
            }

            // Define the path to the X!Tandem .Exe
            var progLoc = m_mgrParams.GetParam("xtprogloc");
            if (progLoc.Length == 0)
            {
                m_message = "Parameter 'xtprogloc' not defined for this manager";
                LogError(m_message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Check whether we need to update the program location to use a specific version of X!Tandem
            progLoc = DetermineXTandemProgramLocation(progLoc);

            if (string.IsNullOrWhiteSpace(progLoc))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }
            else if (!File.Exists(progLoc))
            {
                m_message = "Cannot find XTandem program file";
                LogError(m_message + ": " + progLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Set up and execute a program runner to run X!Tandem
            var cmdStr = "input.xml";

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = true;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, XTANDEM_CONSOLE_OUTPUT);

            m_progress = PROGRESS_PCT_XTANDEM_STARTING;

            var processingSuccess = mCmdRunner.RunProgram(progLoc, cmdStr, "XTandem", true);

            // Parse the console output file one more time to determine the number of peptides found
            ParseConsoleOutputFile(Path.Combine(m_WorkDir, XTANDEM_CONSOLE_OUTPUT));

            if (!mToolVersionWritten)
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!processingSuccess)
            {
                LogError("Error running XTandem, job " + m_JobNum);

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
                CopyFailedResultsToArchiveFolder();

                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (mXTandemResultsCount < 0)
            {
                m_message = @"X!Tandem did not report a ""Valid models"" count";
                LogError(m_message);
                noResults = true;
            }
            else if (mXTandemResultsCount == 0)
            {
                m_message = "No results above threshold";
                LogError(m_message);
                noResults = true;
            }

            // Stop the job timer
            m_StopTime = DateTime.UtcNow;

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Make sure objects are released
            Thread.Sleep(500);
            clsProgRunner.GarbageCollectNow();

            // Zip the output file
            var result = ZipMainOutputFile();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Something went wrong
                // In order to help diagnose things, we will move whatever files were created into the result folder,
                //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                CopyFailedResultsToArchiveFolder();
                return result;
            }

            var success = CopyResultsToTransferDirectory();

            if (!success)
                return CloseOutType.CLOSEOUT_FAILED;

            return noResults ? CloseOutType.CLOSEOUT_NO_DATA : CloseOutType.CLOSEOUT_SUCCESS;
        }

        public override void CopyFailedResultsToArchiveFolder()
        {
            m_jobParams.AddResultFileToSkip(Dataset + "_dta.zip");
            m_jobParams.AddResultFileToSkip(Dataset + "_dta.txt");

            base.CopyFailedResultsToArchiveFolder();
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var strToolVersionInfo = string.Copy(mXTandemVersion);

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(m_mgrParams.GetParam("xtprogloc")));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: true);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        protected string DetermineXTandemProgramLocation(string progLoc)
        {
            // Check whether the settings file specifies that a specific version of the step tool be used
            var strXTandemStepToolVersion = m_jobParams.GetParam("XTandem_Version");

            if (!string.IsNullOrWhiteSpace(strXTandemStepToolVersion))
            {
                // progLoc is currently "C:\DMS_Programs\DMS5\XTandem\bin\Tandem.exe" or "C:\DMS_Programs\XTandem\bin\x64\Tandem.exe"
                // strXTandemStepToolVersion will be similar to "v2011.12.1.1"
                // Insert the specific version just before \bin\ in progLoc

                var intInsertIndex = 0;
                intInsertIndex = progLoc.ToLower().IndexOf("\\bin\\", StringComparison.Ordinal);

                if (intInsertIndex > 0)
                {
                    string strNewProgLoc = null;
                    strNewProgLoc = Path.Combine(progLoc.Substring(0, intInsertIndex), strXTandemStepToolVersion);
                    strNewProgLoc = Path.Combine(strNewProgLoc, progLoc.Substring(intInsertIndex + 1));
                    progLoc = string.Copy(strNewProgLoc);
                }
                else
                {
                    m_message = "XTandem program path does not contain \\bin\\";
                    LogError(m_message + ": " + progLoc);
                    progLoc = string.Empty;
                }
            }

            return progLoc;
        }

        /// <summary>
        /// Parse the X!Tandem console output file to determine the X!Tandem version and to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            var reExtraceValue = new Regex(@"= *(\d+)", RegexOptions.Compiled);

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

                if (m_DebugLevel >= 3)
                {
                    LogDebug("Parsing file " + strConsoleOutputFilePath);
                }

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var intLinesRead = 0;
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();
                        intLinesRead += 1;

                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        if (intLinesRead <= 4 && string.IsNullOrEmpty(mXTandemVersion) && strLineIn.StartsWith("X!"))
                        {
                            // Originally the first line was the X!Tandem version
                            // Starting in November 2016, the first line is the command line and the second line is a separator (series of dashes)
                            // The third or fourth line should be the X!Tandem version

                            if (m_DebugLevel >= 2)
                            {
                                LogDebug("X!Tandem version: " + strLineIn);
                            }

                            mXTandemVersion = string.Copy(strLineIn);
                        }
                        else
                        {
                            // Update progress if the line starts with one of the expected phrases
                            if (strLineIn.StartsWith("Loading spectra"))
                            {
                                m_progress = PROGRESS_PCT_XTANDEM_LOADING_SPECTRA;
                            }
                            else if (strLineIn.StartsWith("Computing models"))
                            {
                                m_progress = PROGRESS_PCT_XTANDEM_COMPUTING_MODELS;
                            }
                            else if (strLineIn.StartsWith("Model refinement"))
                            {
                                m_progress = PROGRESS_PCT_XTANDEM_REFINEMENT;
                            }
                            else if (strLineIn.StartsWith("\tpartial cleavage"))
                            {
                                m_progress = PROGRESS_PCT_XTANDEM_REFINEMENT_PARTIAL_CLEAVAGE;
                            }
                            else if (strLineIn.StartsWith("\tunanticipated cleavage"))
                            {
                                m_progress = PROGRESS_PCT_XTANDEM_REFINEMENT_UNANTICIPATED_CLEAVAGE;
                            }
                            else if (strLineIn.StartsWith("\tfinishing refinement "))
                            {
                                m_progress = PROGRESS_PCT_XTANDEM_REFINEMENT_FINISHING;
                            }
                            else if (strLineIn.StartsWith("Merging results"))
                            {
                                m_progress = PROGRESS_PCT_XTANDEM_MERGING_RESULTS;
                            }
                            else if (strLineIn.StartsWith("Creating report"))
                            {
                                m_progress = PROGRESS_PCT_XTANDEM_CREATING_REPORT;
                            }
                            else if (strLineIn.StartsWith("Estimated false positives"))
                            {
                                m_progress = PROGRESS_PCT_XTANDEM_COMPLETE;
                            }
                            else if (strLineIn.StartsWith("Valid models"))
                            {
                                var reMatch = reExtraceValue.Match(strLineIn);
                                if (reMatch.Success)
                                {
                                    int.TryParse(reMatch.Groups[1].Value, out mXTandemResultsCount);
                                }
                            }
                        }
                    }
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

        /// <summary>
        /// Zips concatenated XML output file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType ZipMainOutputFile()
        {
            string[] FileList = null;
            string TmpFilePath = null;

            try
            {
                FileList = Directory.GetFiles(m_WorkDir, "*_xt.xml");
                foreach (var TmpFile in FileList)
                {
                    TmpFilePath = Path.Combine(m_WorkDir, Path.GetFileName(TmpFile));
                    if (!base.ZipFile(TmpFilePath, true))
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
                FileList = Directory.GetFiles(m_WorkDir, "*_xt.xml");
                foreach (var TmpFile in FileList)
                {
                    File.SetAttributes(TmpFile, File.GetAttributes(TmpFile) & (~FileAttributes.ReadOnly));
                    File.Delete(TmpFile);
                }
            }
            catch (Exception Err)
            {
                LogError("clsAnalysisToolRunnerXT.ZipMainOutputFile, Error deleting _xt.xml file, job " + m_JobNum + Err.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

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

                ParseConsoleOutputFile(Path.Combine(m_WorkDir, XTANDEM_CONSOLE_OUTPUT));
                if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mXTandemVersion))
                {
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                LogProgress("XTandem");
            }
        }

        #endregion
    }
}