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

        protected const float PROGRESS_PCT_FEATURE_FINDER_RUNNING = 5;
        protected const float PROGRESS_PCT_FEATURE_FINDER_DONE = 95;

        protected clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs LCMS Feature Finder tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            string CmdStr = null;
            bool blnSuccess = false;

            //Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            LogMessage("Running LCMSFeatureFinder");

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            // Determine the path to the LCMSFeatureFinder folder
            string progLoc = null;
            progLoc = base.DetermineProgramLocation("LCMSFeatureFinder", "LCMSFeatureFinderProgLoc", "LCMSFeatureFinder.exe");

            if (string.IsNullOrWhiteSpace(progLoc))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the FeatureFinder version info in the database
            blnSuccess = StoreToolVersionInfo(progLoc);
            if (!blnSuccess)
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                m_message = "Error determining LCMS FeatureFinder version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Set up and execute a program runner to run the LCMS Feature Finder
            CmdStr = Path.Combine(m_WorkDir, m_jobParams.GetParam("LCMSFeatureFinderIniFile"));
            if (m_DebugLevel >= 1)
            {
                LogDebug(progLoc + " " + CmdStr);
            }

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = true;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = false;

            if (!mCmdRunner.RunProgram(progLoc, CmdStr, "LCMSFeatureFinder", true))
            {
                m_message = "Error running LCMSFeatureFinder";
                LogError(m_message + ", job " + m_JobNum);
                blnSuccess = false;
            }
            else
            {
                blnSuccess = true;
            }

            //Stop the job timer
            m_StopTime = DateTime.UtcNow;
            m_progress = PROGRESS_PCT_FEATURE_FINDER_DONE;

            //Add the current job data to the summary file
            UpdateSummaryFile();

            //Make sure objects are released
            Thread.Sleep(500);         // 1 second delay
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
        protected bool StoreToolVersionInfo(string strFeatureFinderProgLoc)
        {
            string strToolVersionInfo = string.Empty;
            bool blnSuccess = false;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var ioFeatureFinderInfo = new FileInfo(strFeatureFinderProgLoc);
            if (!ioFeatureFinderInfo.Exists)
            {
                try
                {
                    strToolVersionInfo = "Unknown";
                    base.SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), blnSaveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                    return false;
                }

                return false;
            }

            // Lookup the version of the Feature Finder
            blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, ioFeatureFinderInfo.FullName);
            if (!blnSuccess)
                return false;

            // Lookup the version of the FeatureFinder Library (in the feature finder folder)
            string strFeatureFinderDllLoc = Path.Combine(ioFeatureFinderInfo.DirectoryName, "FeatureFinder.dll");
            blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, strFeatureFinderDllLoc);
            if (!blnSuccess)
                return false;

            // Lookup the version of the UIMF Library (in the feature finder folder)
            blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, Path.Combine(ioFeatureFinderInfo.DirectoryName, "UIMFLibrary.dll"));
            if (!blnSuccess)
                return false;

            // Store paths to key DLLs in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(strFeatureFinderProgLoc));
            ioToolFiles.Add(new FileInfo(strFeatureFinderDllLoc));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: false);
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
