//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerProg
{
    /// <summary>
    /// Base class for Sequest analysis
    /// </summary>
    public class clsCodeTestAM : clsAnalysisToolRunnerBase
    {
        #region "Methods"

        /// <summary>
        /// Initializes class
        /// </summary>
        /// <param name="stepToolName">Name of the current step tool</param>
        /// <param name="mgrParams">Object containing manager parameters</param>
        /// <param name="jobParams">Object containing job parameters</param>
        /// <param name="statusTools">Object for updating status file as job progresses</param>
        /// <param name="summaryFile"></param>
        /// <param name="myEMSLUtilities"></param>
        /// <remarks></remarks>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsSummaryFile summaryFile, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, summaryFile, myEMSLUtilities);

            if (m_DebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerSeqBase.Setup()");
            }
        }

        /// <summary>
        /// Runs the analysis tool
        /// </summary>
        /// <returns>CloseOutType value indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            var eReturnCode = CloseOutType.CLOSEOUT_SUCCESS;

            // Create some dummy results files

            CreateTestFiles(m_WorkDir, 5, "TestResultFile");

            // Make some subfolders with more files
            var subFolderPath = Path.Combine(m_WorkDir, "Plots");
            Directory.CreateDirectory(subFolderPath);
            CreateTestFiles(subFolderPath, 4, "Plot");

            subFolderPath = Path.Combine(subFolderPath, "MoreStuff");
            Directory.CreateDirectory(subFolderPath);
            var success = CreateTestFiles(subFolderPath, 5, "Stuff");

            // Stop the job timer
            m_StopTime = System.DateTime.UtcNow;

            if (!success)
            {
                // Something went wrong
                // In order to help diagnose things, we will move whatever files were created into the result folder,
                //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                eReturnCode = CloseOutType.CLOSEOUT_FAILED;
            }

            // Add the current job data to the summary file
            if (!UpdateSummaryFile())
            {
                LogWarning("Error creating summary file, job " + Job + ", step " + m_jobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step"));
            }

            // Make sure objects are released
            System.Threading.Thread.Sleep(500);
            PRISM.clsProgRunner.GarbageCollectNow();

            var folderCreateSuccess = MakeResultsFolder();
            if (!folderCreateSuccess)
            {
                // MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var moveSucceed = MoveResultFiles();
            if (!moveSucceed)
            {
                // MoveResultFiles moves the result files to the result folder
                m_message = "Error moving files into results folder";
                eReturnCode = CloseOutType.CLOSEOUT_FAILED;
            }

            // Move the Plots folder to the result files folder
            var diPlotsFolder = new DirectoryInfo(Path.Combine(m_WorkDir, "Plots"));

            var targetFolderPath = Path.Combine(Path.Combine(m_WorkDir, m_ResFolderName), "Plots");
            if (diPlotsFolder.Exists && !Directory.Exists(targetFolderPath))
                diPlotsFolder.MoveTo(targetFolderPath);

            if (!success || eReturnCode == CloseOutType.CLOSEOUT_FAILED)
            {
                // Try to save whatever files were moved into the results folder
                var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
                objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName));

                return CloseOutType.CLOSEOUT_FAILED;
            }

            var copySuccess = CopyResultsFolderToServer();
            if (!copySuccess)
            {
                // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool CreateTestFiles(string folderPath, int filesToCreate, string fileNameBase)
        {
            var objRand = new System.Random();

            for (var index = 1; index <= filesToCreate; index++)
            {
                var outFilePath = Path.Combine(folderPath, fileNameBase + index + "_" + objRand.Next(1, 99) + ".txt");

                using (var swOutFile = new StreamWriter(new FileStream(outFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swOutFile.WriteLine(System.DateTime.Now.ToString(DATE_TIME_FORMAT) + " - This is a test file.");
                }

                System.Threading.Thread.Sleep(50);
            }

            return true;
        }
        #endregion
    }
}
