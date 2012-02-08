using System.IO;
using System.Collections.Generic;
using System;
using AnalysisManagerBase;
using log4net;

using Cyclops;

namespace AnalysisManager_Cyclops_PlugIn
{
    class clsAnalysisToolRunnerCyclops: clsAnalysisToolRunnerBase
    {

		protected const float PROGRESS_PCT_CYCLOPS_START = 5;
		protected const float PROGRESS_PCT_CYCLOPS_DONE = 95;

        public override IJobParams.CloseOutType RunTool()
        {
			try 
			{
			
				IJobParams.CloseOutType result = default(IJobParams.CloseOutType);
				bool blnSuccess = false;

				//Do the base class stuff
				if (!(base.RunTool() == IJobParams.CloseOutType.CLOSEOUT_SUCCESS))
				{
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running Cyclops");
				m_progress = PROGRESS_PCT_CYCLOPS_START;
				m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress);


				if (m_DebugLevel > 4)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerApe.RunTool(): Enter");
				}            
           
				// Store the Cyclops version info in the database
				if (!StoreToolVersionInfo()) {
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
					m_message = "Error determining Cyclops version";
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

				// Determine the path to the R DLLs
				string RProgLoc = m_mgrParams.GetParam("RProgLoc");
				if (string.IsNullOrWhiteSpace(RProgLoc)) {
					m_message = "RProgLoc manager parameter is not defined";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; update the Mangager_Control database");
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

				RProgLoc = System.IO.Path.Combine(RProgLoc, "bin", "i386");
				if (!System.IO.Directory.Exists(RProgLoc)) {
					m_message = "R folder not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + " at " + RProgLoc);
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

				Dictionary<string, string> d_Params = new Dictionary<string, string>();
				d_Params.Add("Job", m_jobParams.GetParam("Job"));
				d_Params.Add("RDLL", RProgLoc);
				d_Params.Add("CyclopsWorkflowName", m_jobParams.GetParam("CyclopsWorkflowName"));
				d_Params.Add("workDir", m_WorkDir);
				d_Params.Add("Consolidation_Factor", m_jobParams.GetParam("Consolidation_Factor"));
				d_Params.Add("Fixed_Effect", m_jobParams.GetParam("Fixed_Effect"));

				//Change the name of the log file for the local log file to the plug in log filename
				String LogFileName = Path.Combine(m_WorkDir, "Cyclops_Log");
				log4net.GlobalContext.Properties["LogName"] = LogFileName;
				clsLogTools.ChangeLogFileName(LogFileName);

				try
				{
					clsCyclopsModel cm = new clsCyclopsModel(d_Params);
					cm.AssembleModulesFromXML();
					blnSuccess = cm.Run();

					//Change the name of the log file for the local log file to the plug in log filename
					LogFileName = m_mgrParams.GetParam("logfilename");
					log4net.GlobalContext.Properties["LogName"] = LogFileName;
					clsLogTools.ChangeLogFileName(LogFileName);
				}
				catch (Exception ex)
				{
					//Change the name of the log file for the local log file to the plug in log filename
					LogFileName = m_mgrParams.GetParam("logfilename");
					log4net.GlobalContext.Properties["LogName"] = LogFileName;
					clsLogTools.ChangeLogFileName(LogFileName);

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running Cyclops: " + ex.Message);
					blnSuccess = false;
				}
  
				//Stop the job timer
				m_StopTime = System.DateTime.UtcNow;
				m_progress = PROGRESS_PCT_CYCLOPS_DONE;

				//Add the current job data to the summary file
				if (!UpdateSummaryFile())
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
				}

				//Make sure objects are released
				//2 second delay
				System.Threading.Thread.Sleep(2000);
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
				m_jobParams.SetParam("StepParameters", "OutputFolderName", m_ResFolderName);

				result = MakeResultsFolder();
				if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
				{
					// MakeResultsFolder handles posting to local log, so set database error message and exit
					return result;
				}

				result = MoveResultFiles();
				if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
				{
					// Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
					return result;
				}
				
				// Move the Plots folder to the result files folder
				System.IO.DirectoryInfo diPlotsFolder = default(System.IO.DirectoryInfo);
				diPlotsFolder = new System.IO.DirectoryInfo(System.IO.Path.Combine(m_WorkDir, "Plots"));

				if (diPlotsFolder.Exists) 
				{
					string strTargetFolderPath = System.IO.Path.Combine(System.IO.Path.Combine(m_WorkDir, m_ResFolderName), "Plots");
					diPlotsFolder.MoveTo(strTargetFolderPath);
				}

				result = CopyResultsFolderToServer();
				if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
				{
					// Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
					return result;
				}
			
			} catch (Exception ex) {
				m_message = "Error in CyclopsPlugin->RunTool";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
				return IJobParams.CloseOutType.CLOSEOUT_FAILED;
			}

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;            

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
            string strFolderPathToArchive;
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

			try {
				System.Reflection.AssemblyName oAssemblyName = System.Reflection.Assembly.Load("Cyclops").GetName();

				string strNameAndVersion;
				strNameAndVersion = oAssemblyName.Name + ", Version=" + oAssemblyName.Version.ToString();
				strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion);
			} catch (Exception ex) {
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for Cyclops: " + ex.Message);
				return false;
			}

			// Store paths to key DLLs
			System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
			ioToolFiles.Add(new System.IO.FileInfo("Cyclops.dll"));

			try {
				return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
			} catch (Exception ex) {
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
				return false;
			}


        }


	}
}
