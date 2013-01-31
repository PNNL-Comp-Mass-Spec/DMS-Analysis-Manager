using System;
using System.Collections.Generic;
using System.IO;

using AnalysisManagerBase;
using log4net;
using InterDetect;

namespace AnalysisManager_IDM_Plugin
{
    class clsAnalysisToolRunnerIDM : clsAnalysisToolRunnerBase
    {
        #region Members
		protected System.DateTime mLastProgressUpdate;
        #endregion

        #region Methods
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

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running IDM");
				
				// Store the Version info in the database
				StoreToolVersionInfo();

				mLastProgressUpdate = System.DateTime.UtcNow;
                m_progress = 0;
                m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress);


                if (m_DebugLevel > 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerIDM.RunTool(): Enter");
                }

                //Change the name of the log file for the local log file to the plug in log filename
                String LogFileName = Path.Combine(m_WorkDir, "IDM_Log");
                log4net.GlobalContext.Properties["LogName"] = LogFileName;
                clsLogTools.ChangeLogFileName(LogFileName);

                try
                {
                    // Create in instance of IDM and run the tool
                    InterferenceDetector idm = new InterferenceDetector();
                    
					// Attach the progress event
					idm.ProgressChanged += new InterferenceDetector.ProgressChangedHandler(InterfenceDetectorProgressHandler);

                    blnSuccess = idm.Run(m_WorkDir, "Results.db3"); 

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

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running IDM: " + ex.Message);
                    blnSuccess = false;
                }


                //Stop the job timer
                m_StopTime = System.DateTime.UtcNow;
                m_progress = 100;

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
					return result;
				}

				result = MoveResultFiles();
				if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
				{
					// Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
					return result;
				}
				
				result = CopyResultsFolderToServer();
				if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
				{
					// Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
					return result;
				}
            }
            catch (Exception ex)
            {
                m_message = "Error in IDMPlugin->RunTool";
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

		protected void InterfenceDetectorProgressHandler(InterferenceDetector id, ProgressInfo e)
		{
			
			m_progress = e.Value;

			if (System.DateTime.UtcNow.Subtract(mLastProgressUpdate).TotalMinutes >= 1)
			{
				mLastProgressUpdate = System.DateTime.UtcNow;
				m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress);
			}
			
			
		}

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
			string strAppFolderPath = clsGlobal.GetAppFolderPath();

			System.IO.FileInfo fiIDMdll = new System.IO.FileInfo(System.IO.Path.Combine(strAppFolderPath, "InterDetect.dll"));

			return StoreToolVersionInfoDLL(fiIDMdll.FullName);
        }

		protected bool StoreToolVersionInfoDLL(string strIDMdllPath)
		{

			string strToolVersionInfo = string.Empty;

			if (m_DebugLevel >= 2)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
			}

			// Lookup the version of the DLL
			base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strIDMdllPath);

			// Store paths to key files in ioToolFiles
			System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
			ioToolFiles.Add(new System.IO.FileInfo(strIDMdllPath));
		
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
				
        #endregion
    }
}
