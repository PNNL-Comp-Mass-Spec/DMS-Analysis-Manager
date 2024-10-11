//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerProg
{
    /// <summary>
    /// Test tool runner class
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class CodeTestAM : AnalysisToolRunnerBase
    {
        // Ignore Spelling: Prog

        /// <summary>
        /// Initializes class
        /// </summary>
        /// <param name="stepToolName">Name of the current step tool</param>
        /// <param name="mgrParams">Object containing manager parameters</param>
        /// <param name="jobParams">Object containing job parameters</param>
        /// <param name="statusTools">Object for updating status file as job progresses</param>
        /// <param name="summaryFile">Summary file</param>
        /// <param name="myEMSLUtilities">MyEMSL utilities</param>
        public override void Setup(
            string stepToolName,
            IMgrParams mgrParams,
            IJobParams jobParams,
            IStatusFile statusTools,
            SummaryFile summaryFile,
            MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, summaryFile, myEMSLUtilities);

            if (mDebugLevel > 3)
            {
                LogDebug("AnalysisToolRunnerSeqBase.Setup()");
            }
        }

        /// <summary>
        /// Runs the analysis tool
        /// </summary>
        /// <returns>CloseOutType value indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            var returnCode = CloseOutType.CLOSEOUT_SUCCESS;

            // Create some dummy results files

            CreateTestFiles(mWorkDir, 5, "TestResultFile");

            // Make some subdirectories with more files
            var subdirectoryPath = Path.Combine(mWorkDir, "Plots");
            Directory.CreateDirectory(subdirectoryPath);
            CreateTestFiles(subdirectoryPath, 4, "Plot");

            subdirectoryPath = Path.Combine(subdirectoryPath, "MoreStuff");
            Directory.CreateDirectory(subdirectoryPath);
            var success = CreateTestFiles(subdirectoryPath, 5, "Stuff");

            // Stop the job timer
            mStopTime = System.DateTime.UtcNow;

            if (!success)
            {
                // Something went wrong
                // In order to help diagnose things, move the output files into the results directory,
                // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                returnCode = CloseOutType.CLOSEOUT_FAILED;
            }

            // Add the current job data to the summary file
            if (!UpdateSummaryFile())
            {
                LogWarning("Error creating summary file, job " + Job + ", step " + mJobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "Step"));
            }

            // Make sure objects are released
            PRISM.AppUtils.GarbageCollectNow();

            var folderCreateSuccess = MakeResultsDirectory();

            if (!folderCreateSuccess)
            {
                // MakeResultsDirectory handles posting to local log, so set database error message and exit
                mMessage = "Error making results folder";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var moveSucceed = MoveResultFiles();

            if (!moveSucceed)
            {
                // MoveResultFiles moves the result files to the result folder
                mMessage = "Error moving files into results folder";
                returnCode = CloseOutType.CLOSEOUT_FAILED;
            }

            // Move the Plots folder to the result files folder
            var plotsFolder = new DirectoryInfo(Path.Combine(mWorkDir, "Plots"));

            var targetFolderPath = Path.Combine(Path.Combine(mWorkDir, mResultsDirectoryName), "Plots");

            if (plotsFolder.Exists && !Directory.Exists(targetFolderPath))
                plotsFolder.MoveTo(targetFolderPath);

            if (!success || returnCode == CloseOutType.CLOSEOUT_FAILED)
            {
                // Try to save whatever files were moved into the results directory
                var analysisResults = new AnalysisResults(mMgrParams, mJobParams);
                analysisResults.CopyFailedResultsToArchiveDirectory(Path.Combine(mWorkDir, mResultsDirectoryName));

                return CloseOutType.CLOSEOUT_FAILED;
            }

            var copySuccess = CopyResultsFolderToServer();

            if (!copySuccess)
            {
                // Note that CopyResultsFolderToServer should have already called AnalysisResults.CopyFailedResultsToArchiveDirectory
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool CreateTestFiles(string folderPath, int filesToCreate, string fileNameBase)
        {
            var randGenerator = new System.Random();

            for (var index = 1; index <= filesToCreate; index++)
            {
                var outFilePath = Path.Combine(folderPath, fileNameBase + index + "_" + randGenerator.Next(1, 99) + ".txt");

                using (var writer = new StreamWriter(new FileStream(outFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine(System.DateTime.Now.ToString(DATE_TIME_FORMAT) + " - This is a test file.");
                }

                Global.IdleLoop(0.5);
            }

            return true;
        }
    }
}
