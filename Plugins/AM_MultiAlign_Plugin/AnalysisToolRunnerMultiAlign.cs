//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMultiAlignPlugIn
{
    /// <summary>
    /// Class for running MultiAlign
    /// </summary>
    public class AnalysisToolRunnerMultiAlign : AnalysisToolRunnerBase
    {
        // Ignore Spelling: ParmFile

        private const int PROGRESS_PCT_MULTIALIGN_RUNNING = 5;
        private const int PROGRESS_PCT_MULTI_ALIGN_DONE = 95;

        private RunDosProgram mCmdRunner;

        /// <summary>
        /// Runs MultiAlign tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            LogMessage("Running MultiAlign");

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (mDebugLevel > 4)
            {
                LogDebug("AnalysisToolRunnerMultiAlign.OperateAnalysisTool(): Enter");
            }

            // Determine the path to the MultiAlign folder
            var progLoc = DetermineProgramLocation("MultiAlignProgLoc", "MultiAlignConsole.exe");

            if (string.IsNullOrWhiteSpace(progLoc))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the MultiAlign version info in the database
            if (!StoreToolVersionInfo(progLoc))
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                mMessage = "Error determining MultiAlign version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Note that MultiAlign will append ".db3" to this filename
            var multiAlignDatabaseName = mDatasetName;

            // Set up and execute a program runner to run MultiAlign
            var arguments = " input.txt" +
                            " " + Path.Combine(mWorkDir, mJobParams.GetParam("ParmFileName")) +
                            " " + mWorkDir +
                            " " + multiAlignDatabaseName;

            if (mDebugLevel >= 1)
            {
                LogDebug(progLoc + " " + arguments);
            }

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = true;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = false;

            bool processingSuccess;
            if (!mCmdRunner.RunProgram(progLoc, arguments, "MultiAlign", true))
            {
                mMessage = "Error running MultiAlign";
                LogError(mMessage + ", job " + mJob);
                processingSuccess = false;
            }
            else
            {
                processingSuccess = true;
            }

            // Stop the job timer
            mStopTime = DateTime.UtcNow;
            mProgress = PROGRESS_PCT_MULTI_ALIGN_DONE;

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Make sure objects are released
            PRISM.ProgRunner.GarbageCollectNow();

            if (!processingSuccess)
            {
                // Something went wrong
                // In order to help diagnose things, we will move whatever files were created into the result folder,
                //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                CopyFailedResultsToArchiveDirectory();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Rename the log file so it is consistent with other log files. MultiAlign will add ability to specify log file name
            RenameLogFile();

            var resultsFolderCreated = MakeResultsDirectory();
            if (!resultsFolderCreated)
            {
                CopyFailedResultsToArchiveDirectory();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Move the Plots folder to the result files folder
            var plotsFolder = new DirectoryInfo(Path.Combine(mWorkDir, "Plots"));

            var targetFolderPath = Path.Combine(Path.Combine(mWorkDir, mResultsDirectoryName), "Plots");
            plotsFolder.MoveTo(targetFolderPath);

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        private CloseOutType RenameLogFile()
        {
            const string LogExtension = "-log.txt";
            var newFilename = mDatasetName + LogExtension;

            // This is what MultiAlign is currently naming the log file
            var logNameFilter = mDatasetName + ".db3-log*.txt";
            try
            {
                // Get the log file name.  There should only be one log file
                var matchingFiles = Directory.GetFiles(mWorkDir, logNameFilter);

                // Go through each log file found.  Again, there should only be one log file
                foreach (var targetFile in matchingFiles)
                {
                    // Check to see if the log file exists.  If so, only rename one of them
                    if (!File.Exists(newFilename))
                    {
                        File.Move(targetFile, newFilename);
                    }
                }
            }
            catch (Exception)
            {
                // Even if the rename failed, go ahead and continue
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileExtensionToSkip(".UIMF");
            mJobParams.AddResultFileExtensionToSkip(".csv");

            base.CopyFailedResultsToArchiveDirectory();
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string multiAlignProgLoc)
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var multiAlignProg = new FileInfo(multiAlignProgLoc);
            if (!multiAlignProg.Exists)
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

            // Lookup the version of MultiAlign
            var success = mToolVersionUtilities.StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, multiAlignProg.FullName);
            if (!success)
                return false;

            var toolFiles = new List<FileInfo>();

            if (multiAlignProg.DirectoryName != null)
            {
                // Lookup the version of additional DLLs
                success = mToolVersionUtilities.StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, Path.Combine(multiAlignProg.DirectoryName, "PNNLOmics.dll"));
                if (!success)
                    return false;

                success = mToolVersionUtilities.StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo,
                                                              Path.Combine(multiAlignProg.DirectoryName, "MultiAlignEngine.dll"));
                if (!success)
                    return false;

                success = mToolVersionUtilities.StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo,
                                                              Path.Combine(multiAlignProg.DirectoryName, "MultiAlignCore.dll"));
                if (!success)
                    return false;

                success = mToolVersionUtilities.StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo,
                                                              Path.Combine(multiAlignProg.DirectoryName, "PNNLControls.dll"));
                if (!success)
                    return false;

                // Store paths to key DLLs in toolFiles
                toolFiles.Add(new FileInfo(Path.Combine(multiAlignProg.DirectoryName, "MultiAlignEngine.dll")));
                toolFiles.Add(new FileInfo(Path.Combine(multiAlignProg.DirectoryName, "PNNLOmics.dll")));
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

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile(PROGRESS_PCT_MULTIALIGN_RUNNING);

            LogProgress("MultiAlign");
        }
    }
}
