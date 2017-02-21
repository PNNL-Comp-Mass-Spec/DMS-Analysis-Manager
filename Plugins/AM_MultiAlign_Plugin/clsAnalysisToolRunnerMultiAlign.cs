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

namespace AnalysisManagerMultiAlignPlugIn
{
    /// <summary>
    /// Class for running MultiAlign
    /// </summary>
    public class clsAnalysisToolRunnerMultiAlign : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

        protected const float PROGRESS_PCT_MULTIALIGN_RUNNING = 5;
        protected const float PROGRESS_PCT_MULTI_ALIGN_DONE = 95;

        protected clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs MultiAlign tool
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

            LogMessage("Running MultiAlign");

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (m_DebugLevel > 4)
            {
                LogDebug(
                    "clsAnalysisToolRunnerMultiAlign.OperateAnalysisTool(): Enter");
            }

            // Determine the path to the MultiAlign folder
            string progLoc = null;
            progLoc = DetermineProgramLocation("MultiAlign", "MultiAlignProgLoc", "MultiAlignConsole.exe");

            if (string.IsNullOrWhiteSpace(progLoc))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the MultiAlign version info in the database
            if (!StoreToolVersionInfo(progLoc))
            {
                LogError(
                    "Aborting since StoreToolVersionInfo returned false");
                m_message = "Error determining MultiAlign version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Note that MultiAlign will append ".db3" to this filename
            string MultiAlignDatabaseName = string.Copy(m_Dataset);

            // Set up and execute a program runner to run MultiAlign
            CmdStr = " input.txt " + Path.Combine(m_WorkDir, m_jobParams.GetParam("ParmFileName")) + " " + m_WorkDir + " " + MultiAlignDatabaseName;
            if (m_DebugLevel >= 1)
            {
                LogDebug(progLoc + " " + CmdStr);
            }

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = true;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = false;

            if (!mCmdRunner.RunProgram(progLoc, CmdStr, "MultiAlign", true))
            {
                m_message = "Error running MultiAlign";
                LogError(m_message + ", job " + m_JobNum);
                blnSuccess = false;
            }
            else
            {
                blnSuccess = true;
            }

            //Stop the job timer
            m_StopTime = DateTime.UtcNow;
            m_progress = PROGRESS_PCT_MULTI_ALIGN_DONE;

            //Add the current job data to the summary file
            if (!UpdateSummaryFile())
            {
                LogWarning(
                    "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
            }

            //Make sure objects are released
            Thread.Sleep(500);        // 500 msec delay
            PRISM.clsProgRunner.GarbageCollectNow();

            if (!blnSuccess)
            {
                // Move the source files and any results to the Failed Job folder
                // Useful for debugging MultiAlign problems
                CopyFailedResultsToArchiveFolder();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var result = MakeResultsFolder();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                //TODO: What do we do here?
                return result;
            }

            //Rename the log file so it is consistent with other log files. MultiAlign will add ability to specify log file name
            RenameLogFile();

            result = MoveResultFiles();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                //TODO: What do we do here?
                // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                return result;
            }

            // Move the Plots folder to the result files folder
            var diPlotsFolder = new DirectoryInfo(Path.Combine(m_WorkDir, "Plots"));

            string strTargetFolderPath = null;
            strTargetFolderPath = Path.Combine(Path.Combine(m_WorkDir, m_ResFolderName), "Plots");
            diPlotsFolder.MoveTo(strTargetFolderPath);

            result = CopyResultsFolderToServer();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                //TODO: What do we do here?
                // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                return result;
            }

            return CloseOutType.CLOSEOUT_SUCCESS; //ZipResult
        }

        protected CloseOutType RenameLogFile()
        {
            string[] Files = null;
            var LogExtension = "-log.txt";
            string NewFilename = m_Dataset + LogExtension;
            //This is what MultiAlign is currently naming the log file
            string LogNameFilter = m_Dataset + ".db3-log*.txt";
            try
            {
                //Get the log file name.  There should only be one log file
                Files = Directory.GetFiles(m_WorkDir, LogNameFilter);
                //go through each log file found.  Again, there should only be one log file
                foreach (string TmpFile in Files)
                {
                    //Check to see if the log file exists.  If so, only rename one of them
                    if (!File.Exists(NewFilename))
                    {
                        File.Move(TmpFile, NewFilename);
                    }
                }
            }
            catch (Exception)
            {
                //Even if the rename failed, go ahead and continue
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
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

            // Try to save whatever files are in the work directory (however, delete the .UIMF file first, plus also the Decon2LS .csv files)
            string strFolderPathToArchive = null;
            strFolderPathToArchive = string.Copy(m_WorkDir);

            try
            {
                File.Delete(Path.Combine(m_WorkDir, m_Dataset + ".UIMF"));
                File.Delete(Path.Combine(m_WorkDir, m_Dataset + "*.csv"));
            }
            catch (Exception)
            {
                // Ignore errors here
            }

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
        protected bool StoreToolVersionInfo(string strMultiAlignProgLoc)
        {
            string strToolVersionInfo = string.Empty;
            bool blnSuccess = false;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var ioMultiAlignProg = new FileInfo(strMultiAlignProgLoc);
            if (!ioMultiAlignProg.Exists)
            {
                try
                {
                    strToolVersionInfo = "Unknown";
                    base.SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), blnSaveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    LogError(
                        "Exception calling SetStepTaskToolVersion: " + ex.Message);
                    return false;
                }

                return false;
            }

            // Lookup the version of MultiAlign
            blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, ioMultiAlignProg.FullName);
            if (!blnSuccess)
                return false;

            // Lookup the version of additional DLLs
            blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, Path.Combine(ioMultiAlignProg.DirectoryName, "PNNLOmics.dll"));
            if (!blnSuccess)
                return false;

            blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, Path.Combine(ioMultiAlignProg.DirectoryName, "MultiAlignEngine.dll"));
            if (!blnSuccess)
                return false;

            blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, Path.Combine(ioMultiAlignProg.DirectoryName, "MultiAlignCore.dll"));
            if (!blnSuccess)
                return false;

            blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, Path.Combine(ioMultiAlignProg.DirectoryName, "PNNLControls.dll"));
            if (!blnSuccess)
                return false;

            // Store paths to key DLLs in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(Path.Combine(ioMultiAlignProg.DirectoryName, "MultiAlignEngine.dll")));
            ioToolFiles.Add(new FileInfo(Path.Combine(ioMultiAlignProg.DirectoryName, "PNNLOmics.dll")));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: false);
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
            UpdateStatusFile(PROGRESS_PCT_MULTIALIGN_RUNNING);

            LogProgress("MultiAlign");
        }

        #endregion
    }
}
