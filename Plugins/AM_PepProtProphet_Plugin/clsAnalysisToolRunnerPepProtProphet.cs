//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.IO;

namespace AnalysisManagerPepProtProphetPlugIn
{
    /// <summary>
    /// Class for running peptide prophet and protein prophet using Philosopher
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsAnalysisToolRunnerPepProtProphet : clsAnalysisToolRunnerBase
    {

        #region "Constants and Enums"

        private const string Philosopher_CONSOLE_OUTPUT = "Philosopher_ConsoleOutput.txt";
        private const string Philosopher_EXE_NAME = "philosopheexer_windows_amd64.jar";

        private const float PROGRESS_PCT_STARTING = 1;
        private const float PROGRESS_PCT_COMPLETE = 99;

        #endregion

        #region "Module Variables"

        private bool mToolVersionWritten;

        // Populate this with a tool version reported to the console
        private string mPhilosopherVersion;

        private string mPhilosopherProgLoc;
        private string mConsoleOutputErrorMsg;

        private string mValidatedFASTAFilePath;

        private DateTime mLastConsoleOutputParse;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs peptide and protein prophet using Philosopher
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
                    LogDebug("clsAnalysisToolRunnerPepProtProphet.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to Philosopher
                mPhilosopherProgLoc = DetermineProgramLocation("PhilosopherProgLoc", Philosopher_EXE_NAME);

                if (string.IsNullOrWhiteSpace(mPhilosopherProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the Philosopher version info in the database after the first line is written to file Philosopher_ConsoleOutput.txt
                mToolVersionWritten = false;
                mPhilosopherVersion = string.Empty;
                mConsoleOutputErrorMsg = string.Empty;

                if (!ValidateFastaFile(out var fastaFileIsDecoy))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Process the XML files using Philosopher
                var processingResult = StartPepProtProphet(fastaFileIsDecoy, mPhilosopherProgLoc);

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                clsGlobal.IdleLoop(0.5);
                PRISM.ProgRunner.GarbageCollectNow();

                if (!clsAnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();
                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                return processingResult;

            }
            catch (Exception ex)
            {
                LogError("Error in PepProtProphetPlugin->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileToSkip(Dataset + clsAnalysisResources.DOT_MZML_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory();
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

            ParseConsoleOutputFile(Path.Combine(mWorkDir, Philosopher_CONSOLE_OUTPUT));

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mPhilosopherVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("Philosopher");
        }

        #endregion
    }
}
