using System.IO;
using AnalysisManagerBase;
using System;
using log4net;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace AnalysisManager_AScore_PlugIn
{
   public class clsAnalysisToolRunnerAScore : clsAnalysisToolRunnerBase
   {
	   protected const float PROGRESS_PCT_ASCORE_START = 1;
	   protected const float PROGRESS_PCT_ASCORE_DONE = 99;

	   protected string m_CurrentAScoreTask = string.Empty;
	   protected System.DateTime m_LastStatusUpdateTime;

       public override IJobParams.CloseOutType RunTool()
       {
			try 
			{

				m_jobParams.SetParam("JobParameters", "DatasetNum", m_jobParams.GetParam("OutputFolderPath")); 
				IJobParams.CloseOutType result = default(IJobParams.CloseOutType);
				bool blnSuccess = false;

				//Do the base class stuff
				if (!(base.RunTool() == IJobParams.CloseOutType.CLOSEOUT_SUCCESS))
				{
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

				// Store the AScore version info in the database
                //if (!StoreToolVersionInfo())
                //{
                //    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
                //    m_message = "Error determining AScore version";
                //    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                //}

				m_CurrentAScoreTask = "Running AScore";
				m_LastStatusUpdateTime = System.DateTime.UtcNow;
				m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress);

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_CurrentAScoreTask);

				//Change the name of the log file for the local log file to the plugin log filename
                String LogFileName = Path.Combine(m_WorkDir, "Ascore_Log");
                log4net.GlobalContext.Properties["LogName"] = LogFileName;
                clsLogTools.ChangeLogFileName(LogFileName);

				try
				{
                    m_progress = PROGRESS_PCT_ASCORE_START;

					blnSuccess = RunAScore();

					// Change the name of the log file back to the analysis manager log file
                    LogFileName = m_mgrParams.GetParam("logfilename");
                    log4net.GlobalContext.Properties["LogName"] = LogFileName;
                    clsLogTools.ChangeLogFileName(LogFileName);

					if (!blnSuccess && !string.IsNullOrWhiteSpace(m_message)) {
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running AScore: " + m_message);
					}
				}
				catch (Exception ex)
				{
					// Change the name of the log file back to the analysis manager log file
                    LogFileName = m_mgrParams.GetParam("logfilename");
                    log4net.GlobalContext.Properties["LogName"] = LogFileName;
                    clsLogTools.ChangeLogFileName(LogFileName);

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running AScore: " + ex.Message);
					blnSuccess = false;
					m_message = "Error running AScore";
				}

				//Stop the job timer
				m_StopTime = System.DateTime.UtcNow;
				m_progress = PROGRESS_PCT_ASCORE_DONE;

				//Add the current job data to the summary file
				if (!UpdateSummaryFile())
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
				}

				//Make sure objects are released
				//2 second delay
				System.Threading.Thread.Sleep(2000);
				PRISM.Processes.clsProgRunner.GarbageCollectNow();

				if (!blnSuccess)
				{
					// Move the source files and any results to the Failed Job folder
					// Useful for debugging MultiAlign problems
					CopyFailedResultsToArchiveFolder();
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

				m_ResFolderName = m_jobParams.GetParam("StepOutputFolderName");
				m_Dataset = m_jobParams.GetParam("OutputFolderName");
				m_jobParams.SetParam("StepParameters", "OutputFolderName", m_ResFolderName);

				result = MakeResultsFolder();
				if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
				{
					// MakeResultsFolder handles posting to local log, so set database error message and exit
					m_message = "Error making results folder";
					return result;
				}

			   result = MoveResultFiles();
				if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
				{
					// Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
					m_message = "Error moving files into results folder";
					return result;
				}

                //result = CopyResultsFolderToServer();
                if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return result;
                }

			} catch (Exception ex) {
				m_message = "Error in AScorePlugin->RunTool";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
				return IJobParams.CloseOutType.CLOSEOUT_FAILED;

			}

			return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;

        }

       /// <summary>
       /// Run the AScore pipeline(s) listed in "AScoreOperations" parameter
       /// </summary>
       protected bool RunAScore()
       {
           // run the appropriate Mage pipeline(s) according to operations list parameter
           clsAScoreMage dvas = new clsAScoreMage(m_jobParams, m_mgrParams);
           dvas.Run();
           return true;
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

            // Try to save whatever files are in the work directory
            string strFolderPathToArchive = null;
            strFolderPathToArchive = string.Copy(m_WorkDir);

			// If necessary, delete extra files with the following
			/* 
				try
				{
					System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset + ".UIMF"));
					System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset + "*.csv"));
				}
				catch
				{
					// Ignore errors here
				}
		    */
			
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
        protected bool StoreToolVersionInfo()
        {

            string strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2) {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

			try
			{
				System.Reflection.AssemblyName oAssemblyName = System.Reflection.Assembly.Load("AScore_DLL").GetName();

				string strNameAndVersion = null;
				strNameAndVersion = oAssemblyName.Name + ", Version=" + oAssemblyName.Version.ToString();
				strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion);
			}
			catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for AScore: " + ex.Message);
                return false;
            }

			// Store paths to key DLLs
			System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();

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
    
    
