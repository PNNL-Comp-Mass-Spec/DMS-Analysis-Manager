using System.IO;
using AnalysisManagerBase;
using System;
using Ape;
using log4net;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace AnalysisManager_Ape_PlugIn
{
   class clsAnalysisToolRunnerApe : clsAnalysisToolRunnerBase
   {
	   protected const float PROGRESS_PCT_APE_START = 1;
	   protected const float PROGRESS_PCT_APE_DONE = 99;

	   protected string m_CurrentApeTask = string.Empty;
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

				// Store the Ape version info in the database
				if (!StoreToolVersionInfo()) {
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
					m_message = "Error determining Ape version";
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

				m_CurrentApeTask = "Running Ape";
				m_LastStatusUpdateTime = System.DateTime.UtcNow;
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

					if (!blnSuccess && !string.IsNullOrWhiteSpace(m_message)) {
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
				m_StopTime = System.DateTime.UtcNow;
				m_progress = PROGRESS_PCT_APE_DONE;

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
           bool blnSuccess = false;
		   int iOperations = 0;

           string apeOperations = m_jobParams.GetParam("ApeOperations");

		   if (string.IsNullOrWhiteSpace(apeOperations)) {
			   m_message = "ApeOperations parameter is not defined";
			   return false;
		   }

           foreach (string apeOperation in apeOperations.Split(','))
           {
			   if (!string.IsNullOrWhiteSpace(apeOperation)) {
				   iOperations += 1;

				   blnSuccess = RunApeOperation(apeOperation.Trim());
				   if (!blnSuccess) {
					   m_message = "Error running Ape operation " + apeOperation;
					   break;
				   }
			   }
           }

		   if (iOperations == 0) {
			   m_message = "ApeOperations parameter was empty";
			   return false;
		   }

           return blnSuccess;

       }
       
        #region Ape Operations

       /// <summary>
       /// Run defined Ape operation(s)
       /// </summary>
       /// <param name="apeOperation"></param>
       /// <returns></returns>
       private bool RunApeOperation(string apeOperation)
       {
           bool blnSuccess = false;

		   // Note: case statements must be lowercase
           switch (apeOperation.ToLower())
           {
               case "runworkflow":
				   clsApeAMRunWorkflow apeWfObj = new clsApeAMRunWorkflow(m_jobParams, m_mgrParams);

				   // Attach the progress event handler
				   apeWfObj.ProgressChanged += new clsApeAMBase.ProgressChangedEventHandler(ApeProgressChanged);

                   blnSuccess = apeWfObj.RunWorkflow(m_jobParams.GetParam("DataPackageID"));

				   // Sleep for a few seconds to give SqlServerToSQLite.ConvertDatasetToSQLiteFile a chance to finish
				   System.Threading.Thread.Sleep(2000);

				   break;

               case "getimprovresults":
                   clsApeAMGetImprovResults apeImpObj = new clsApeAMGetImprovResults(m_jobParams, m_mgrParams);

				   // Attach the progress event handler
				   apeImpObj.ProgressChanged += new clsApeAMBase.ProgressChangedEventHandler(ApeProgressChanged);

                   blnSuccess = apeImpObj.GetImprovResults(m_jobParams.GetParam("DataPackageID"));

				   // Sleep for a few seconds to give SqlServerToSQLite.ConvertDatasetToSQLiteFile a chance to finish
				   System.Threading.Thread.Sleep(2000);

                   break;

               default:
                   blnSuccess = false;
                   m_message = "Ape Operation: " + apeOperation + "not recognized.";
                   // Future: throw an error
                   break;
           }
           return blnSuccess;
       }

	   void ApeProgressChanged(object sender, clsApeAMBase.ProgressChangedEventArgs e) {

			// Update the step tool progress
			// However, Ape routinely reports progress of 0% or 100% at the start and end of certain subtasks, so ignore those values
			if (e.percentComplete > 0 && e.percentComplete < 100)
				m_progress = PROGRESS_PCT_APE_START + (PROGRESS_PCT_APE_DONE - PROGRESS_PCT_APE_START) * e.percentComplete / 100.0F;

			if (!string.IsNullOrEmpty(e.taskDescription))
				m_CurrentApeTask = e.taskDescription;

			if (System.DateTime.UtcNow.Subtract(m_LastStatusUpdateTime).TotalSeconds >= 10) {
				m_LastStatusUpdateTime = System.DateTime.UtcNow;
				m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress);
			}
	   }

       #endregion

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
				System.Reflection.AssemblyName oAssemblyName = System.Reflection.Assembly.Load("Ape").GetName();

				string strNameAndVersion = null;
				strNameAndVersion = oAssemblyName.Name + ", Version=" + oAssemblyName.Version.ToString();
				strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion);
			}
			catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for Ape: " + ex.Message);
                return false;
            }

			// Store paths to key DLLs
			System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
			ioToolFiles.Add(new System.IO.FileInfo("Ape.dll"));

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
    
    
