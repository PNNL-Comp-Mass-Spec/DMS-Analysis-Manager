using System.IO;
using AnalysisManagerBase;
using System;

namespace AnalysisManager_Ape_PlugIn
{
   public class clsAnalysisToolRunnerApe : clsAnalysisToolRunnerBase
   {
	   protected const float PROGRESS_PCT_APE_START = 1;
	   protected const float PROGRESS_PCT_APE_DONE = 99;

	   protected string m_CurrentApeTask = string.Empty;
	   protected DateTime m_LastStatusUpdateTime;

       public override IJobParams.CloseOutType RunTool()
       {
			try 
			{

				m_jobParams.SetParam("JobParameters", "DatasetNum", m_jobParams.GetParam("OutputFolderPath"));
				bool blnSuccess;

				//Do the base class stuff
				if (base.RunTool() != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
				{
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

				// Store the Ape version info in the database
                if (!StoreToolVersionInfo())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining Ape version";
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

				m_CurrentApeTask = "Running Ape";
				m_LastStatusUpdateTime = DateTime.UtcNow;
				m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress);

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_CurrentApeTask);

				//Change the name of the log file for the local log file to the plugin log filename
				String LogFileName = Path.Combine(m_WorkDir, "Ape_Log");
				log4net.GlobalContext.Properties["LogName"] = LogFileName;
				clsLogTools.ChangeLogFileName(LogFileName);

				try
				{
					m_progress = PROGRESS_PCT_APE_START;

					blnSuccess = RunApe();

					// Change the name of the log file back to the analysis manager log file
					LogFileName = m_mgrParams.GetParam("logfilename");
					log4net.GlobalContext.Properties["LogName"] = LogFileName;
					clsLogTools.ChangeLogFileName(LogFileName);

					if (!blnSuccess) {
						if (string.IsNullOrWhiteSpace(m_message))
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running Ape");
						else
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running Ape: " + m_message);
					}
				}
				catch (Exception ex)
				{
					// Change the name of the log file back to the analysis manager log file
					LogFileName = m_mgrParams.GetParam("logfilename");
					log4net.GlobalContext.Properties["LogName"] = LogFileName;
					clsLogTools.ChangeLogFileName(LogFileName);

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running Ape: " + ex.Message);
					blnSuccess = false;
					m_message = "Error running Ape";
				}

				//Stop the job timer
				m_StopTime = DateTime.UtcNow;
				m_progress = PROGRESS_PCT_APE_DONE;

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

				IJobParams.CloseOutType result = MakeResultsFolder();
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

                result = CopyResultsFolderToServer();
                if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return result;
                }

			} catch (Exception ex) {
				m_message = "Error in ApePlugin->RunTool";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
				return IJobParams.CloseOutType.CLOSEOUT_FAILED;

			}

			return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;

        }

       /// <summary>
       /// Run the Ape pipeline(s) listed in "ApeOperations" parameter
       /// </summary>
       protected bool RunApe()
       {
           // run the appropriate Mage pipeline(s) according to operations list parameter
           string apeOperations = m_jobParams.GetParam("ApeOperations");
           var ops = new clsApeAMOperations(m_jobParams, m_mgrParams);
           bool bSuccess = ops.RunApeOperations(apeOperations);

		   if (!bSuccess)
			   m_message = "Error running ApeOperations: " + ops.ErrorMessage;

		   return bSuccess;

       }
       

       protected void CopyFailedResultsToArchiveFolder()
        {
	       string strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrEmpty(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory
	       string strFolderPathToArchive = string.Copy(m_WorkDir);

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
            IJobParams.CloseOutType result = MakeResultsFolder();
            if (result == IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Move the result files into the result folder
                result = MoveResultFiles();
                if (result == IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
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

            if (m_DebugLevel >= 2) {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

			try
			{
				System.Reflection.AssemblyName oAssemblyName = System.Reflection.Assembly.Load("Ape").GetName();

				string strNameAndVersion = oAssemblyName.Name + ", Version=" + oAssemblyName.Version;
				strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion);
			}
			catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for Ape: " + ex.Message);
                return false;
            }

			// Store paths to key DLLs
			var ioToolFiles = new System.Collections.Generic.List<FileInfo>();
			ioToolFiles.Add(new FileInfo("Ape.dll"));

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
    
    
