using AnalysisManagerBase;
using log4net;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace AnalysisManager_MultiAlign_Aggregator_PlugIn
{
   public class clsAnalysisToolRunnerMultiAlignAggregator : clsAnalysisToolRunnerBase
   {
	   protected const float PROGRESS_PCT_MULTIALIGN_START = 1;
       protected const float PROGRESS_PCT_MULTIALIGN_DONE = 99;

	   protected string m_CurrentMultiAlignTask = string.Empty;
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

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MultiAlign Aggregator");

				// Determine the path to the LCMSFeatureFinder folder
				string progLoc;
				progLoc = base.DetermineProgramLocation("MultiAlign", "MultiAlignProgLoc", "MultiAlignConsole.exe");

				if (string.IsNullOrWhiteSpace(progLoc))
				{
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}
				
				// Store the MultiAlign version info in the database
				if (!StoreToolVersionInfo(progLoc))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining MultiAlign version";
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

				m_CurrentMultiAlignTask = "Running MultiAlign";
				m_LastStatusUpdateTime = System.DateTime.UtcNow;
				m_StatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, m_progress);

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_CurrentMultiAlignTask);

				//Change the name of the log file for the local log file to the plugin log filename
                String LogFileName = System.IO.Path.Combine(m_WorkDir, "MultiAlign_Log");
                log4net.GlobalContext.Properties["LogName"] = LogFileName;
                clsLogTools.ChangeLogFileName(LogFileName);

				try
				{
                    m_progress = PROGRESS_PCT_MULTIALIGN_START;

					blnSuccess = RunMultiAlign(progLoc);

					// Change the name of the log file back to the analysis manager log file
                    LogFileName = m_mgrParams.GetParam("logfilename");
                    log4net.GlobalContext.Properties["LogName"] = LogFileName;
                    clsLogTools.ChangeLogFileName(LogFileName);

					if (!blnSuccess && !string.IsNullOrWhiteSpace(m_message)) {
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running MultiAlign: " + m_message);
					}
				}
				catch (System.Exception ex)
				{
					// Change the name of the log file back to the analysis manager log file
                    LogFileName = m_mgrParams.GetParam("logfilename");
                    log4net.GlobalContext.Properties["LogName"] = LogFileName;
                    clsLogTools.ChangeLogFileName(LogFileName);

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running MultiAlign: " + ex.Message);
					blnSuccess = false;
                    m_message = "Error running MultiAlign";
				}

				//Stop the job timer
				m_StopTime = System.DateTime.UtcNow;
				m_progress = PROGRESS_PCT_MULTIALIGN_DONE;

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

                // Move the Plots folder to the result files folder
                System.IO.DirectoryInfo diPlotsFolder = default(System.IO.DirectoryInfo);
                diPlotsFolder = new System.IO.DirectoryInfo(System.IO.Path.Combine(m_WorkDir, "Plots"));

                if (diPlotsFolder.Exists)
                {
                    string strTargetFolderPath = System.IO.Path.Combine(System.IO.Path.Combine(m_WorkDir, m_ResFolderName), "Plots");
					PRISM.Files.clsFileTools.CopyDirectory(diPlotsFolder.FullName, strTargetFolderPath, true);

					try
					{
						diPlotsFolder.Delete(true);
					}
					catch (Exception ex)
					{
						Console.WriteLine("Warning: Exception deleting " + diPlotsFolder.FullName + ": " + ex.Message);
					}
					
                }

                result = CopyResultsFolderToServer();
                if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return result;
                }

			}
			catch (System.Exception ex)
			{
                m_message = "Error in MultiAlignPlugin->RunTool";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
				return IJobParams.CloseOutType.CLOSEOUT_FAILED;

			}

			return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;

        }

       /// <summary>
       /// Run the MultiAlign pipeline(s) listed in "MultiAlignOperations" parameter
       /// </summary>
	   protected bool RunMultiAlign(string sMultiAlignConsolePath)
       {
            // run the appropriate Mage pipeline(s) according to operations list parameter
		   clsMultiAlignMage oMultiAlignMage = new clsMultiAlignMage(m_jobParams, m_mgrParams);
		   bool bSuccess = oMultiAlignMage.Run(sMultiAlignConsolePath);

			if (!bSuccess)
			{
				if (oMultiAlignMage.Message.Length > 0)
					m_message = oMultiAlignMage.Message;
				else
					m_message = "Unknown error running multialign";
			}

			return bSuccess;

       }

       /// <summary>
       /// 
       /// </summary>
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
		protected bool StoreToolVersionInfo(string strMultiAlignProgLoc)
		{

			string strToolVersionInfo = string.Empty;
			System.IO.FileInfo ioMultiAlignInfo;
			bool blnSuccess;

            if (m_DebugLevel >= 2) {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

			ioMultiAlignInfo = new System.IO.FileInfo(strMultiAlignProgLoc);
			if (!ioMultiAlignInfo.Exists)
			{
				try
				{
					strToolVersionInfo = "Unknown";
					base.SetStepTaskToolVersion(strToolVersionInfo, new System.Collections.Generic.List<System.IO.FileInfo>());
				}
				catch (Exception ex)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
					return false;
				}

				return false;
			}

			// Lookup the version of the Feature Finder
			blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, ioMultiAlignInfo.FullName);
			if (!blnSuccess)
				return false;

			// Lookup the version of MultiAlignEngine (in the MultiAlign folder)
			string strMultiAlignEngineDllLoc = System.IO.Path.Combine(ioMultiAlignInfo.DirectoryName, "MultiAlignEngine.dll");
			blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, strMultiAlignEngineDllLoc);
			if (!blnSuccess)
				return false;

			// Lookup the version of MultiAlignCore (in the MultiAlign folder)
			string strMultiAlignCoreDllLoc = System.IO.Path.Combine(ioMultiAlignInfo.DirectoryName, "MultiAlignCore.dll");
			blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, strMultiAlignCoreDllLoc);
			if (!blnSuccess)
				return false;

			// Store paths to key DLLs in ioToolFiles
			System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
			ioToolFiles.Add(new System.IO.FileInfo(strMultiAlignProgLoc));
			ioToolFiles.Add(new System.IO.FileInfo(strMultiAlignEngineDllLoc));
			ioToolFiles.Add(new System.IO.FileInfo(strMultiAlignCoreDllLoc));

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
    
    
