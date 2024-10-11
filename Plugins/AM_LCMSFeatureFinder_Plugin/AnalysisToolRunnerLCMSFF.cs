//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISM;

namespace AnalysisManagerLCMSFeatureFinderPlugIn
{
    /// <summary>
    /// Class for running the LCMS Feature Finder
    /// </summary>
    public class AnalysisToolRunnerLCMSFF : AnalysisToolRunnerBase
    {
        // Ignore Spelling: Ini

        private const int PROGRESS_PCT_FEATURE_FINDER_RUNNING = 5;
        private const int PROGRESS_PCT_FEATURE_FINDER_DONE = 95;

        private RunDosProgram mCmdRunner;

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

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            // Determine the path to the LCMSFeatureFinder folder
            var progLoc = DetermineProgramLocation("LCMSFeatureFinderProgLoc", "LCMSFeatureFinder.exe");

            if (string.IsNullOrWhiteSpace(progLoc))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the FeatureFinder version info in the database
            var success = StoreToolVersionInfo(progLoc);

            if (!success)
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                mMessage = "Error determining LCMS FeatureFinder version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Set up and execute a program runner to run the LCMS Feature Finder
            var arguments = Path.Combine(mWorkDir, mJobParams.GetParam("LCMSFeatureFinderIniFile"));

            if (mDebugLevel >= 1)
            {
                LogDebug(progLoc + " " + arguments);
            }

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = true;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = false;

            if (!mCmdRunner.RunProgram(progLoc, arguments, "LCMSFeatureFinder", true))
            {
                mMessage = "Error running LCMSFeatureFinder";
                LogError(mMessage + ", job " + mJob);
                success = false;
            }

            // Stop the job timer
            mStopTime = DateTime.UtcNow;
            mProgress = PROGRESS_PCT_FEATURE_FINDER_DONE;

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Make sure objects are released
            AppUtils.GarbageCollectNow();

            if (!success)
            {
                // Move the source files and any results to the Failed Job folder
                // Useful for debugging FeatureFinder problems
                CopyFailedResultsToArchiveDirectory();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var copyResult = CopyResultsToTransferDirectory();

            return copyResult ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileToSkip(Dataset + ".UIMF");
            mJobParams.AddResultFileExtensionToSkip(Dataset + ".csv");

            base.CopyFailedResultsToArchiveDirectory();
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile(PROGRESS_PCT_FEATURE_FINDER_RUNNING);

            LogProgress("LCMSFeatureFinder");
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string featureFinderProgLoc)
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var featureFinderInfo = new FileInfo(featureFinderProgLoc);

            if (!featureFinderInfo.Exists)
            {
                try
                {
                    toolVersionInfo = "Unknown";
                    SetStepTaskToolVersion(toolVersionInfo, new List<FileInfo>(), false);
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                    return false;
                }

                return false;
            }

            // Store paths to key DLLs in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new(featureFinderProgLoc)
            };

            // Lookup the version of the Feature Finder
            var success = mToolVersionUtilities.StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, featureFinderInfo.FullName);

            if (!success)
                return false;

            if (featureFinderInfo.DirectoryName != null)
            {
                // Lookup the version of the FeatureFinder Library (in the feature finder folder)
                var featureFinderDllLoc = Path.Combine(featureFinderInfo.DirectoryName, "FeatureFinder.dll");

                toolFiles.Add(new FileInfo(featureFinderDllLoc));

                success = mToolVersionUtilities.StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, featureFinderDllLoc);

                if (!success)
                    return false;

                // Lookup the version of the UIMF Library (in the feature finder folder)
                success = mToolVersionUtilities.StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, Path.Combine(featureFinderInfo.DirectoryName, "UIMFLibrary.dll"));

                if (!success)
                    return false;
            }

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }
    }
}
