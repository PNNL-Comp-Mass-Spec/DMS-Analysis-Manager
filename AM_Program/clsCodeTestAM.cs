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
        /// <param name="mgrParams">Object containing manager parameters</param>
        /// <param name="jobParams">Object containing job parameters</param>
        /// <param name="statusTools">Object for updating status file as job progresses</param>
        /// <param name="summaryFile"></param>
        /// <param name="myEMSLUtilities"></param>
        /// <remarks></remarks>
        public override void Setup(IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsSummaryFile summaryFile, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(mgrParams, jobParams, statusTools, summaryFile, myEMSLUtilities);

            if (m_DebugLevel > 3)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSeqBase.Setup()");
            }
        }

        /// <summary>
        /// Runs the analysis tool
        /// </summary>
        /// <returns>IJobParams.CloseOutType value indicating success or failure</returns>
        /// <remarks></remarks>
        public override IJobParams.CloseOutType RunTool()
        {
            bool blnProcessingError = false;
            IJobParams.CloseOutType eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED;

            // Create some dummy results files
            string strSubFolderPath = null;

            CreateTestFiles(m_WorkDir, 5, "TestResultFile");

            // Make some subfolders with more files
            strSubFolderPath = System.IO.Path.Combine(m_WorkDir, "Plots");
            System.IO.Directory.CreateDirectory(strSubFolderPath);
            CreateTestFiles(strSubFolderPath, 4, "Plot");

            strSubFolderPath = System.IO.Path.Combine(strSubFolderPath, "MoreStuff");
            System.IO.Directory.CreateDirectory(strSubFolderPath);
            blnProcessingError = CreateTestFiles(strSubFolderPath, 5, "Stuff");

            //Stop the job timer
            m_StopTime = System.DateTime.UtcNow;

            if (blnProcessingError)
            {
                // Something went wrong
                // In order to help diagnose things, we will move whatever files were created into the result folder,
                //  archive it using CopyFailedResultsToArchiveFolder, then return IJobParams.CloseOutType.CLOSEOUT_FAILED
                eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            //Add the current job data to the summary file
            if (!UpdateSummaryFile())
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("StepParameters", "Step"));
            }

            //Make sure objects are released
            System.Threading.Thread.Sleep(500);
            // 1 second delay
            PRISM.Processes.clsProgRunner.GarbageCollectNow();

            var Result = MakeResultsFolder();
            if (Result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
            {
                //MakeResultsFolder handles posting to local log, so set database error message and exit
                m_message = "Error making results folder";
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            Result = MoveResultFiles();
            if (Result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
            {
                //MoveResultFiles moves the result files to the result folder
                m_message = "Error moving files into results folder";
                eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            // Move the Plots folder to the result files folder
            System.IO.DirectoryInfo diPlotsFolder = default(System.IO.DirectoryInfo);
            diPlotsFolder = new System.IO.DirectoryInfo(System.IO.Path.Combine(m_WorkDir, "Plots"));

            string strTargetFolderPath = null;
            strTargetFolderPath = System.IO.Path.Combine(System.IO.Path.Combine(m_WorkDir, m_ResFolderName), "Plots");
            diPlotsFolder.MoveTo(strTargetFolderPath);

            if (blnProcessingError | eReturnCode == IJobParams.CloseOutType.CLOSEOUT_FAILED)
            {
                // Try to save whatever files were moved into the results folder
                clsAnalysisResults objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
                objAnalysisResults.CopyFailedResultsToArchiveFolder(System.IO.Path.Combine(m_WorkDir, m_ResFolderName));

                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            Result = CopyResultsFolderToServer();
            if (Result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
            {
                //TODO: What do we do here?
                // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                return Result;
            }

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool CreateTestFiles(string strFolderPath, int intFilesToCreate, string strFileNameBase)
        {
            System.Random objRand = new System.Random();

            for (int intIndex = 1; intIndex <= intFilesToCreate; intIndex++)
            {
                var strOutFilePath = System.IO.Path.Combine(strFolderPath, strFileNameBase + intIndex.ToString() + "_" + objRand.Next(1, 99) + ".txt");

                var swOutFile = new System.IO.StreamWriter(new System.IO.FileStream(strOutFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));
                swOutFile.WriteLine(System.DateTime.Now.ToString() + " - This is a test file.");
                swOutFile.Close();

                System.Threading.Thread.Sleep(50);
            }

            return true;
        }
        #endregion
    }
}
