//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
//
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerPRIDEMzXMLPlugIn
{
    /// <summary>
    /// Class for running PRIDEMzXML analysis
    /// </summary>
    public class AnalysisToolRunnerPRIDEMzXML : AnalysisToolRunnerBase
    {
        #region "Module Variables"

        private const float PROGRESS_PCT_PRIDEMZXML_RUNNING = 5;

        private RunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs MSDataFileTrimmer tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the MSDataFileTrimmer version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                mMessage = "Error determining MSDataFileTrimmer version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            LogMessage("Running MSDataFileTrimmer");

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (mDebugLevel > 4)
            {
                LogDebug("AnalysisToolRunnerPRIDEMzXML.RunTool(): Enter");
            }

            // verify that program file exists
            // progLoc will be something like this: "C:\DMS_Programs\MSDataFileTrimmer\MSDataFileTrimmer.exe"
            var progLoc = mMgrParams.GetParam("MSDataFileTrimmerprogloc");
            if (!File.Exists(progLoc))
            {
                if (progLoc.Length == 0)
                    progLoc = "Parameter 'MSDataFileTrimmerprogloc' not defined for this manager";
                LogError("Cannot find MSDataFileTrimmer program file: " + progLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var arguments =
                " /M:" + Path.Combine(mWorkDir, mJobParams.GetParam("PRIDEMzXMLInputFile")) +
                " /G /O:" + mWorkDir +
                " /L:" + Path.Combine(mWorkDir, "MSDataFileTrimmer_Log.txt");

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = true;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, "MSDataFileTrimmer_ConsoleOutput.txt");

            if (!mCmdRunner.RunProgram(progLoc, arguments, "MSDataFileTrimmer", true))
            {
                LogError("Error running MSDataFileTrimmer");

                // Move the source files and any results to the Failed Job folder
                // Useful for debugging XTandem problems
                CopyFailedResultsToArchiveDirectory();

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Stop the job timer
            mStopTime = DateTime.UtcNow;

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Make sure objects are released
            PRISM.ProgRunner.GarbageCollectNow();

            // Override the dataset name and transfer folder path so that the results get copied to the correct location
            RedefineAggregationJobDatasetAndTransferFolder();

            // Update list of files to be deleted after run
            var groupedFiles = Directory.GetFiles(mWorkDir, "*_grouped*");
            foreach (var fileToSave in groupedFiles)
            {
                mJobParams.AddResultFileToKeep(Path.GetFileName(fileToSave));
            }

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        protected bool StoreToolVersionInfo()
        {
            var progLoc = mMgrParams.GetParam("MSDataFileTrimmerprogloc");
            var success = StoreDotNETToolVersionInfo(progLoc);

            return success;
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile(PROGRESS_PCT_PRIDEMZXML_RUNNING);

            LogProgress("PrideMzXML");
        }

        #endregion
    }
}
