using System.IO;
using AnalysisManagerBase;
using System;

namespace AnalysisManager_Ape_PlugIn
{
    class clsAnalysisToolRunnerApe : clsAnalysisToolRunnerBase
    {

        #region "Module Variables"
        protected const float PROGRESS_PCT_APE_RUNNING = 5;
        protected const float PROGRESS_PCT_APE_DONE = 95;
        protected clsRunDosProgram CmdRunner;

        #endregion
        
        public override IJobParams.CloseOutType RunTool()
        {

            string CmdStr = null;
            IJobParams.CloseOutType result = default(IJobParams.CloseOutType);
            bool blnSuccess = false;

            //Do the base class stuff
            if (!(base.RunTool() == IJobParams.CloseOutType.CLOSEOUT_SUCCESS))
            {
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running Ape");

            CmdRunner = new clsRunDosProgram(m_WorkDir);

            if (m_DebugLevel > 4)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerApe.RunTool(): Enter");
            }

            // Determine the path to the MultiAlign folder
            string progLoc = null;
            //progLoc = DetermineProgramLocation("Ape", "ApeProgLoc", "ApeConsole.exe");
            progLoc = "C:\\Development\\MDART_Versions\\ApeConsole\\bin\\x86\\Debug\\ApeConsole.exe";

            if (string.IsNullOrWhiteSpace(progLoc))
            {
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the MultiAlign version info in the database
            StoreToolVersionInfo(progLoc);

            string MultiAlignResultFilename = m_jobParams.GetParam("ResultFilename");

            if (string.IsNullOrWhiteSpace(MultiAlignResultFilename))
            {
                MultiAlignResultFilename = m_Dataset;
            }

            // Set up and execute a program runner to run MultiAlign
            CmdStr = " -dbname " + Path.Combine(m_WorkDir, m_jobParams.GetParam("ApeDbName")) + " -workflow " + Path.Combine(m_WorkDir, m_jobParams.GetParam("ApeWorkflow"));
            if (m_DebugLevel >= 1)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc + " " + CmdStr);
            }

            CmdRunner.CreateNoWindow = true;
            CmdRunner.CacheStandardOutput = true;
            CmdRunner.EchoOutputToConsole = true;

            CmdRunner.WriteConsoleOutputToFile = false;
            

            if (!CmdRunner.RunProgram(progLoc, CmdStr, "Ape", true))
            {
                m_message = "Error running MultiAlign";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ", job " + m_JobNum);
                blnSuccess = false;
            }
            else
            {
                blnSuccess = true;
            }

            //Stop the job timer
            m_StopTime = System.DateTime.UtcNow;
            m_progress = PROGRESS_PCT_APE_DONE;

            //Add the current job data to the summary file
            if (!UpdateSummaryFile())
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
            }

            //Make sure objects are released
            System.Threading.Thread.Sleep(2000);
            //2 second delay
            GC.Collect();
            GC.WaitForPendingFinalizers();

            if (!blnSuccess)
            {
                // Move the source files and any results to the Failed Job folder
                // Useful for debugging MultiAlign problems
                CopyFailedResultsToArchiveFolder();
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            result = MakeResultsFolder();
            if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
            {
                //TODO: What do we do here?
                return result;
            }

            result = MoveResultFiles();
            if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
            {
                //TODO: What do we do here?
                // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                return result;
            }

            //// Move the Plots folder to the result files folder
            //System.IO.DirectoryInfo diPlotsFolder = default(System.IO.DirectoryInfo);
            //diPlotsFolder = new System.IO.DirectoryInfo(System.IO.Path.Combine(m_WorkDir, "Plots"));

            //string strTargetFolderPath = null;
            //strTargetFolderPath = System.IO.Path.Combine(System.IO.Path.Combine(m_WorkDir, m_ResFolderName), "Plots");
            //diPlotsFolder.MoveTo(strTargetFolderPath);

            //result = CopyResultsFolderToServer();
            //if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
            //{
            //    //TODO: What do we do here?
            //    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
            //    return result;
            //}

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
            ////ZipResult

        }

        protected void CopyFailedResultsToArchiveFolder()
        {
            IJobParams.CloseOutType result = default(IJobParams.CloseOutType);

            string strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrEmpty(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory (however, delete the .UIMF file first, plus also the Decon2LS .csv files)
            string strFolderPathToArchive = null;
            strFolderPathToArchive = string.Copy(m_WorkDir);

            try
            {
                System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset + ".UIMF"));
                System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset + "*.csv"));
            }
            catch (Exception ex)
            {
                // Ignore errors here
            }

            // Make the results folder
            result = MakeResultsFolder();
            if (result == IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Move the result files into the result folder
                result = MoveResultFiles();
                if (result == IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Move was a success; update strFolderPathToArchive
                    strFolderPathToArchive = System.IO.Path.Combine(m_WorkDir, m_ResFolderName);
                }
            }

            // Copy the results folder to the Archive folder
            clsAnalysisResults objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
            objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive);

        }


        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string strMultiAlignProgLoc)
        {

            string strToolVersionInfo = string.Empty;
            System.IO.FileInfo ioMultiAlignProg = default(System.IO.FileInfo);

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            ioMultiAlignProg = new System.IO.FileInfo(strMultiAlignProgLoc);

            // Lookup the version of MultiAlign
            base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, ioMultiAlignProg.FullName);

            // Lookup the version of additional DLLs
            base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "Ape.dll"));

            // Store paths to key DLLs in ioToolFiles
            System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
            ioToolFiles.Add(new System.IO.FileInfo(System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "Ape.dll")));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }

        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        //handles CmdRunner.LoopWaiting
        {
            System.DateTime dtLastStatusUpdate = System.DateTime.UtcNow;

            //Synchronize the stored Debug level with the value stored in the database
            const int MGR_SETTINGS_UPDATE_INTERVAL_SECONDS = 300;
            base.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS);

            //Update the status file (limit the updates to every 5 seconds)
            if (System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5)
            {
                dtLastStatusUpdate = System.DateTime.UtcNow;
                m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, PROGRESS_PCT_APE_RUNNING, 0, "", "", "", false);
            }

        }

    }
}
    
    
