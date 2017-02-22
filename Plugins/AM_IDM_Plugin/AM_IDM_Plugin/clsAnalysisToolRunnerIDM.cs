using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using AnalysisManagerBase;
using log4net;
using InterDetect;
using PRISM;

namespace AnalysisManager_IDM_Plugin
{
    class clsAnalysisToolRunnerIDM : clsAnalysisToolRunnerBase
    {
        #region "Constants"
        public const string EXISTING_IDM_RESULTS_FILE_NAME = "ExistingIDMResults.db3";
        #endregion

        #region Members

        #endregion

        #region Methods
        public override CloseOutType RunTool()
        {
            try
            {
                bool success = false;
                bool skipIDM = false;

                //Do the base class stuff
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var fiIDMResultsDB = new FileInfo(Path.Combine(m_WorkDir, EXISTING_IDM_RESULTS_FILE_NAME));
                if (fiIDMResultsDB.Exists)
                {
                    // Existing results file was copied to the working directory
                    // Copy the t_precursor_interference table into Results.db3

                    try
                    {
                        var sqLiteUtils = new clsSqLiteUtilities();

                        if (m_DebugLevel >= 1)
                        {
                            LogMessage("Copying table t_precursor_interference from " + fiIDMResultsDB.Name + " to Results.db3");
                        }

                        success = sqLiteUtils.CloneDB(fiIDMResultsDB.FullName, Path.Combine(m_WorkDir, "Results.db3"), appendToExistingDB: true);

                        if (success)
                            skipIDM = true;

                        //success = sqLiteUtils.CopySqliteTable(fiIDMResultsDB.FullName, "t_precursor_interference", Path.Combine(m_WorkDir, "Results.db3"));
                        
                    }
                    catch (Exception ex)
                    {
                        LogError(ex.Message + "; will run IDM instead of using existing results");
                    }
                }

                if (!skipIDM)
                {
                    LogMessage("Running IDM");

                    // Store the Version info in the database
                    StoreToolVersionInfo();

                    m_progress = 0;
                    UpdateStatusFile();

                    if (m_DebugLevel > 4)
                    {
                        LogDebug("clsAnalysisToolRunnerIDM.RunTool(): Enter");
                    }

                    //Change the name of the log file for the local log file to the plug in log filename
                    String LogFileName = Path.Combine(m_WorkDir, "IDM_Log");
                    GlobalContext.Properties["LogName"] = LogFileName;
                    clsLogTools.ChangeLogFileName(LogFileName);

                    try
                    {
                        // Create in instance of IDM and run the tool
                        var idm = new InterferenceDetector();

                        // Attach the progress event
                        idm.ProgressChanged += InterfenceDetectorProgressHandler;

                        idm.WorkDir = m_WorkDir;

                        success = idm.Run(m_WorkDir, "Results.db3");

                        //Change the name of the log file for the local log file to the plug in log filename
                        LogFileName = m_mgrParams.GetParam("logfilename");
                        GlobalContext.Properties["LogName"] = LogFileName;
                        clsLogTools.ChangeLogFileName(LogFileName);
                    }
                    catch (Exception ex)
                    {
                        //Change the name of the log file for the local log file to the plug in log filename
                        LogFileName = m_mgrParams.GetParam("logfilename");
                        GlobalContext.Properties["LogName"] = LogFileName;
                        clsLogTools.ChangeLogFileName(LogFileName);

                        LogError("Error running IDM: " + ex.Message);
                        success = false;
                    }
                }

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;
                m_progress = 100;

                //Add the current job data to the summary file
                UpdateSummaryFile();

                //Make sure objects are released
                //2 second delay
                Thread.Sleep(2000);
                clsProgRunner.GarbageCollectNow();

                if (!success)
                {
                    // Move the source files and any results to the Failed Job folder
                    // Useful for debugging MultiAlign problems
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_ResFolderName = m_jobParams.GetParam("StepOutputFolderName");
                m_Dataset = m_jobParams.GetParam("OutputFolderName");
                m_jobParams.SetParam("StepParameters", "OutputFolderName", m_ResFolderName);

                CloseOutType result = MakeResultsFolder();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // MakeResultsFolder handles posting to local log, so set database error message and exit
                    return result;
                }

                result = MoveResultFiles();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return result;
                }

                result = CopyResultsFolderToServer();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return result;
                }
            }
            catch (Exception ex)
            {
                m_message = "Error in IDMPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        protected void CopyFailedResultsToArchiveFolder()
        {
            string strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrEmpty(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            LogWarning("Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

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
            CloseOutType result = MakeResultsFolder();
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

        protected void InterfenceDetectorProgressHandler(InterferenceDetector id, ProgressInfo e)
        {

            m_progress = e.Value;

            UpdateStatusFile(m_progress, 60);

        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            string strAppFolderPath = clsGlobal.GetAppFolderPath();

            var fiIDMdll = new FileInfo(Path.Combine(strAppFolderPath, "InterDetect.dll"));

            return StoreToolVersionInfoDLL(fiIDMdll.FullName);
        }

        protected bool StoreToolVersionInfoDLL(string strIDMdllPath)
        {

            string strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of the DLL
            StoreToolVersionInfoOneFile(ref strToolVersionInfo, strIDMdllPath);

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                new FileInfo(strIDMdllPath)
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }

        }

        #endregion
    }
}
