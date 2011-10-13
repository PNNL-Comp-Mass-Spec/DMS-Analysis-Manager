using System.IO;
using AnalysisManagerBase;
using System;
using Ape;

namespace AnalysisManager_Ape_PlugIn
{
   class clsAnalysisToolRunnerApe : clsAnalysisToolRunnerBase
   {

       #region "Module Variables"
       private static bool _shouldExit = false;
        //protected const float PROGRESS_PCT_APE_RUNNING = 5;
       protected const float PROGRESS_PCT_APE_DONE = 95;
       #endregion

       public override IJobParams.CloseOutType RunTool()
        {
            m_jobParams.SetParam("DatasetNum", m_jobParams.GetParam("OutputFolderPath")); 
           IJobParams.CloseOutType result = default(IJobParams.CloseOutType);
            bool blnSuccess = false;

            //Do the base class stuff
            if (!(base.RunTool() == IJobParams.CloseOutType.CLOSEOUT_SUCCESS))
            {
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running Ape");
           
            blnSuccess =  RunApe();
           
            // Store the Ape version info in the database
            //StoreToolVersionInfo("");

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

            m_ResFolderName = m_jobParams.GetParam("StepOutputFolderName");
            m_Dataset = m_jobParams.GetParam("OutputFolderName");
            m_jobParams.SetParam("OutputFolderName", m_jobParams.GetParam("StepOutputFolderName"));

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

            //m_Dataset = m_jobParams.GetParam("StepOutputFolderName");
            //m_jobParams.SetParam("transferFolderPath", Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("OutputFolderName")));

            result = CopyResultsFolderToServer();
            if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
            {
                //TODO: What do we do here?
                // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                return result;
            }

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
            ////ZipResult

        }

       protected bool RunApe()
       {
           bool blnSuccess = false;

           SqlConversionHandler mHandle = new SqlConversionHandler(delegate(bool done, bool success, int percent, string msg)
           {
               Console.WriteLine(msg);

               if (done)
               {
                   if (success)
                   {
                       m_message = "Ape successfully ran workflow" + m_jobParams.GetParam("ApeWorkflowName");
                       clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_message + ", job " + m_JobNum);
                       blnSuccess = true;
                   }
                   else
                   {
                       if (!_shouldExit)
                       {
                           m_message = "Error running Ape";
                           clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ", job " + m_JobNum);
                           blnSuccess = false;
                       }
                   }
               }

           });

           string apeWorkflow = Path.Combine(m_WorkDir, m_jobParams.GetParam("ApeWorkflowName"));
           string apeDatabase = Path.Combine(m_WorkDir, "Results.db3");

           SqlServerToSQLite.StartWorkflow(1, 3, apeWorkflow, apeDatabase, apeDatabase, false, false, mHandle);

           return blnSuccess;

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

    }
}
    
    
