//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerPRIDEMzXMLPlugIn
{
    /// <summary>
    /// Class for running PRIDEMzXML analysis
    /// </summary>
    public class clsAnalysisToolRunnerPRIDEMzXML : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

        protected const float PROGRESS_PCT_PRIDEMZXML_RUNNING = 5;
        protected const float PROGRESS_PCT_START = 95;
        protected const float PROGRESS_PCT_COMPLETE = 99;

        protected clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs MSDataFileTrimmer tool
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

            // Store the MSDataFileTrimmer version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError(
                    "Aborting since StoreToolVersionInfo returned false");
                m_message = "Error determining MSDataFileTrimmer version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            LogMessage("Running MSDataFileTrimmer");

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (m_DebugLevel > 4)
            {
                LogDebug("clsAnalysisToolRunnerPRIDEMzXML.RunTool(): Enter");
            }

            // verify that program file exists
            // progLoc will be something like this: "C:\DMS_Programs\MSDataFileTrimmer\MSDataFileTrimmer.exe"
            string progLoc = m_mgrParams.GetParam("MSDataFileTrimmerprogloc");
            if (!File.Exists(progLoc))
            {
                if (progLoc.Length == 0)
                    progLoc = "Parameter 'MSDataFileTrimmerprogloc' not defined for this manager";
                LogError(
                    "Cannot find MSDataFileTrimmer program file: " + progLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            string CmdStr = null;
            CmdStr = "/M:" + Path.Combine(m_WorkDir, m_jobParams.GetParam("PRIDEMzXMLInputFile"));
            CmdStr += " /G /O:" + m_WorkDir;
            CmdStr += " /L:" + Path.Combine(m_WorkDir, "MSDataFileTrimmer_Log.txt");

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = true;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, "MSDataFileTrimmer_ConsoleOutput.txt");

            if (!mCmdRunner.RunProgram(progLoc, CmdStr, "MSDataFileTrimmer", true))
            {
                LogError("Error running MSDataFileTrimmer");

                // Move the source files and any results to the Failed Job folder
                // Useful for debugging XTandem problems
                CopyFailedResultsToArchiveFolder();

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Stop the job timer
            m_StopTime = DateTime.UtcNow;

            // Add the current job data to the summary file
            UpdateSummaryFile();

            //Make sure objects are released
            Thread.Sleep(500);        // 500 msec delay
            PRISM.clsProgRunner.GarbageCollectNow();

            // Override the dataset name and transfer folder path so that the results get copied to the correct location
            base.RedefineAggregationJobDatasetAndTransferFolder();

            var result = MakeResultsFolder();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                //TODO: What do we do here?
                return result;
            }

            string[] DumFiles = null;

            // Update list of files to be deleted after run
            DumFiles = Directory.GetFiles(m_WorkDir, "*_grouped*");
            foreach (string FileToSave in DumFiles)
            {
                m_jobParams.AddResultFileToKeep(Path.GetFileName(FileToSave));
            }

            result = MoveResultFiles();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                //TODO: What do we do here?
                return result;
            }

            result = CopyResultsFolderToServer();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                //TODO: What do we do here?
                return result;
            }

            return CloseOutType.CLOSEOUT_SUCCESS; //ZipResult
        }

        protected void CopyFailedResultsToArchiveFolder()
        {
            string strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrEmpty(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            LogWarning(
                "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory
            string strFolderPathToArchive = null;
            strFolderPathToArchive = string.Copy(m_WorkDir);

            // Make the results folder
            var result = MakeResultsFolder();
            if (result == CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Move the result files into the result folder
                result = MoveResultFiles();
                if (result == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Move was a success; update strFolderPathToArchive
                    strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName);
                }
            }

            // Copy the results folder to the Archive folder
            var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
            objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            string strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Store paths to key files in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(m_mgrParams.GetParam("MSDataFileTrimmerprogloc")));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                LogError(
                    "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile(PROGRESS_PCT_PRIDEMZXML_RUNNING);

            LogProgress("PrideMzXML");
        }

        #endregion
    }
}
