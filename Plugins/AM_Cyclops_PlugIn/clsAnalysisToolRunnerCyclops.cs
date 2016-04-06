using System.IO;
using System.Collections.Generic;
using System;
using AnalysisManagerBase;

using Cyclops;

namespace AnalysisManager_Cyclops_PlugIn
{
    public class clsAnalysisToolRunnerCyclops: clsAnalysisToolRunnerBase
    {

		protected const float PROGRESS_PCT_CYCLOPS_START = 5;
		protected const float PROGRESS_PCT_CYCLOPS_DONE = 95;

        public override IJobParams.CloseOutType RunTool()
        {
			try 
			{
				bool blnSuccess;

				// Do the base class stuff
				if (base.RunTool() != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
				{
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running Cyclops");
				m_progress = PROGRESS_PCT_CYCLOPS_START;
                UpdateStatusRunning();


				if (m_DebugLevel > 4)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerApe.RunTool(): Enter");
				}            
           
				// Store the Cyclops version info in the database
                if (!StoreToolVersionInfo())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining Cyclops version";
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

				// Determine the path to R
				var rProgLocFromRegistry = GetRPathFromWindowsRegistry();
                if (string.IsNullOrEmpty(rProgLocFromRegistry))
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;

                if (!Directory.Exists(rProgLocFromRegistry))
				{
					m_message = "R folder not found (path determined from the Windows Registry)";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + " at " + rProgLocFromRegistry);
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

				var d_Params = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
				{
					{"Job", m_jobParams.GetParam("Job")},
					{"RDLL", rProgLocFromRegistry},
					{"CyclopsWorkflowName", m_jobParams.GetParam("CyclopsWorkflowName")},
					{"workDir", m_WorkDir},
					{"Consolidation_Factor", m_jobParams.GetParam("Consolidation_Factor")},
					{"Fixed_Effect", m_jobParams.GetParam("Fixed_Effect")},
					{"RunProteinProphet", m_jobParams.GetParam("RunProteinProphet")},
					{"orgdbdir", m_mgrParams.GetParam("orgdbdir")}
				};


			    // Change the name of the log file for the local log file to the plug in log filename
				var cyclopsLogFile = Path.Combine(m_WorkDir, "Cyclops_Log.txt");
                log4net.GlobalContext.Properties["LogName"] = cyclopsLogFile;
                clsLogTools.ChangeLogFileName(cyclopsLogFile);

				try
				{

                    var cyclops = new CyclopsController(d_Params);
                    
                    // This is blank; don't show at console: cyclops.OperationsDatabasePath
                    // This is blank; don't show at console: cyclops.WorkFlowFileName                    
                    Console.WriteLine("WorkingDirectory: " + cyclops.WorkingDirectory);

				    Console.WriteLine("Parameters:");
				    foreach (var entry in cyclops.Parameters)
				    {
                        Console.WriteLine("  " + entry.Key + ": " + entry.Value);
				    }
				    
					blnSuccess = cyclops.Run();

					//Change the name of the log file for the local log file to the plugin log filename
					var logFilePath = m_mgrParams.GetParam("logfilename");
					log4net.GlobalContext.Properties["LogName"] = logFilePath;
					clsLogTools.ChangeLogFileName(logFilePath);

				}
				catch (Exception ex)
				{
					//Change the name of the log file for the local log file to the plugin log filename
					var logFilePath = m_mgrParams.GetParam("logfilename");
					log4net.GlobalContext.Properties["LogName"] = logFilePath;
					clsLogTools.ChangeLogFileName(logFilePath);

					LogError("Error running Cyclops: " + ex.Message);
					blnSuccess = false;
				}
  
				// Stop the job timer
				m_StopTime = DateTime.UtcNow;
				m_progress = PROGRESS_PCT_CYCLOPS_DONE;

				// Add the current job data to the summary file
				if (!UpdateSummaryFile())
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
				}

				// Make sure objects are released
				System.Threading.Thread.Sleep(500);
				PRISM.Processes.clsProgRunner.GarbageCollectNow();

                // Delete the log file if it is empty
                var fiCyclopsLogFile = new FileInfo(cyclopsLogFile);
			    if (fiCyclopsLogFile.Exists && fiCyclopsLogFile.Length == 0)
			    {
			        string errorMessage;
			        m_FileTools.DeleteFileWithRetry(fiCyclopsLogFile, out errorMessage);
			    }

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

				var result = MakeResultsFolder();
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
				var diPlotsFolder = new DirectoryInfo(Path.Combine(m_WorkDir, "Plots"));

				if (diPlotsFolder.Exists) 
				{
					var strTargetFolderPath = Path.Combine(Path.Combine(m_WorkDir, m_ResFolderName), "Plots");
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
	        var strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrEmpty(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory
	        var strFolderPathToArchive = string.Copy(m_WorkDir);

			// If necessary, delete extra files with the following
			/* 
				try
				{
					File.Delete(Path.Combine(m_WorkDir, m_Dataset + ".UIMF"));
					File.Delete(Path.Combine(m_WorkDir, m_Dataset + "*.csv"));
				}
				catch
				{
					// Ignore errors here
				}
		    */

            // Make the results folder
            var result = MakeResultsFolder();
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

			var strToolVersionInfo = string.Empty;

			if (m_DebugLevel >= 2) {
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
			}

			try {
				var oAssemblyName = System.Reflection.Assembly.Load("Cyclops").GetName();

				var strNameAndVersion = oAssemblyName.Name + ", Version=" + oAssemblyName.Version;
				strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion);
			} catch (Exception ex) {
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for Cyclops: " + ex.Message);
				return false;
			}

			// Store paths to key DLLs
			var ioToolFiles = new List<FileInfo>
			{
				new FileInfo(Path.Combine(clsGlobal.GetAppFolderPath(), "Cyclops.dll"))
			};

	        try {
				return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, false);
			} catch (Exception ex) {
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
				return false;
			}


        }
		
	}
}
