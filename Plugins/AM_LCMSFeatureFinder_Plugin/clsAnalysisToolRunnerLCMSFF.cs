//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerLCMSFeatureFinderPlugIn
{
    /// <summary>
    /// Class for running the LCMS Feature Finder
    /// </summary>
    public class clsAnalysisToolRunnerLCMSFF : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

        private const float PROGRESS_PCT_FEATURE_FINDER_RUNNING = 5;
        private const float PROGRESS_PCT_FEATURE_FINDER_DONE = 95;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs LCMS Feature Finder tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            LogMessage("Running LCMSFeatureFinder");

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            // Determine the path to the LCMSFeatureFinder folder
            var progLoc = DetermineProgramLocation("LCMSFeatureFinderProgLoc", "LCMSFeatureFinder.exe");

            if (string.IsNullOrWhiteSpace(progLoc))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the FeatureFinder version info in the database
            var blnSuccess = StoreToolVersionInfo(progLoc);
            if (!blnSuccess)
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                m_message = "Error determining LCMS FeatureFinder version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Set up and execute a program runner to run the LCMS Feature Finder
            var cmdStr = Path.Combine(m_WorkDir, m_jobParams.GetParam("LCMSFeatureFinderIniFile"));
            if (m_DebugLevel >= 1)
            {
                LogDebug(progLoc + " " + cmdStr);
            }

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = true;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = false;

            if (!mCmdRunner.RunProgram(progLoc, cmdStr, "LCMSFeatureFinder", true))
            {
                m_message = "Error running LCMSFeatureFinder";
                LogError(m_message + ", job " + m_JobNum);
                blnSuccess = false;
            }

            // Stop the job timer
            m_StopTime = DateTime.UtcNow;
            m_progress = PROGRESS_PCT_FEATURE_FINDER_DONE;

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Make sure objects are released
            Thread.Sleep(500);
            clsProgRunner.GarbageCollectNow();

            if (!blnSuccess)
            {
                // Move the source files and any results to the Failed Job folder
                // Useful for debugging FeatureFinder problems
                CopyFailedResultsToArchiveFolder();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveFolder()
        {
            m_jobParams.AddResultFileToSkip(Dataset + ".UIMF");
            m_jobParams.AddResultFileExtensionToSkip(Dataset + ".csv");

            base.CopyFailedResultsToArchiveFolder();
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile(PROGRESS_PCT_FEATURE_FINDER_RUNNING);

            LogProgress("LCMSFeatureFinder");
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string featureFinderProgLoc)
        {
            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var ioFeatureFinderInfo = new FileInfo(featureFinderProgLoc);
            if (!ioFeatureFinderInfo.Exists)
            {
                try
                {
                    strToolVersionInfo = "Unknown";
                    SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), saveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                    return false;
                }

                return false;
            }


            // Store paths to key DLLs in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                new FileInfo(featureFinderProgLoc)
            };

            // Lookup the version of the Feature Finder
            var blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, ioFeatureFinderInfo.FullName);
            if (!blnSuccess)
                return false;

            // Lookup the version of the FeatureFinder Library (in the feature finder folder)
            var strFeatureFinderDllLoc = Path.Combine(ioFeatureFinderInfo.DirectoryName, "FeatureFinder.dll");

            ioToolFiles.Add(new FileInfo(strFeatureFinderDllLoc));

            blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, strFeatureFinderDllLoc);
            if (!blnSuccess)
                return false;

            // Lookup the version of the UIMF Library (in the feature finder folder)
            blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, Path.Combine(ioFeatureFinderInfo.DirectoryName, "UIMFLibrary.dll"));
            if (!blnSuccess)
                return false;

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

        #endregion

    }
}
