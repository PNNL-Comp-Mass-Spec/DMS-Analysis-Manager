using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace AnalysisManagerMultiAlign_AggregatorPlugIn
{
    public class clsAnalysisToolRunnerMultiAlignAggregator : clsAnalysisToolRunnerBase
    {
        protected const float PROGRESS_PCT_MULTIALIGN_START = 1;
        protected const float PROGRESS_PCT_MULTIALIGN_DONE = 99;

        protected string m_CurrentMultiAlignTask = string.Empty;
        protected DateTime m_LastStatusUpdateTime;

        public override CloseOutType RunTool()
        {
            try
            {

                m_jobParams.SetParam("JobParameters", "DatasetNum", m_jobParams.GetParam("OutputFolderPath"));
                bool blnSuccess;

                //Do the base class stuff
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                LogMessage("Running MultiAlign Aggregator");

                // Determine the path to the LCMSFeatureFinder folder
                var progLoc = DetermineProgramLocation("MultiAlign", "MultiAlignProgLoc", "MultiAlignConsole.exe");

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the MultiAlign version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining MultiAlign version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_CurrentMultiAlignTask = "Running MultiAlign";
                m_LastStatusUpdateTime = DateTime.UtcNow;
                UpdateStatusRunning(m_progress);

                LogMessage(m_CurrentMultiAlignTask);

                // Change the name of the log file for the local log file to the plugin log filename
                var LogFileName = Path.Combine(m_WorkDir, "MultiAlign_Log");
                log4net.GlobalContext.Properties["LogName"] = LogFileName;
                clsLogTools.ChangeLogFileName(LogFileName);

                try
                {
                    m_progress = PROGRESS_PCT_MULTIALIGN_START;
                    blnSuccess = RunMultiAlign(progLoc);
                }
                catch (Exception ex)
                {
                    m_message = "Error running MultiAlign: " + ex.Message;
                    blnSuccess = false;
                }

                // Change the name of the log file back to the analysis manager log file
                LogFileName = m_mgrParams.GetParam("logfilename");
                log4net.GlobalContext.Properties["LogName"] = LogFileName;
                clsLogTools.ChangeLogFileName(LogFileName);

                if (blnSuccess)
                {
                    LogMessage("MultiAlign complete");
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                        m_message = "Unknown error running MultiAlign";
                    else
                        m_message = "Error running MultiAlign: " + m_message;

                    LogError(m_message);
                }

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;
                m_progress = PROGRESS_PCT_MULTIALIGN_DONE;

                //Add the current job data to the summary file
                if (!UpdateSummaryFile())
                {
                    LogWarning("Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                }

                //Make sure objects are released
                //2 second delay
                Thread.Sleep(2000);
                PRISM.clsProgRunner.GarbageCollectNow();

                if (!blnSuccess)
                {
                    // Move the source files and any results to the Failed Job folder
                    // Useful for debugging MultiAlign problems
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_ResFolderName = m_jobParams.GetParam("StepOutputFolderName");
                m_Dataset = m_jobParams.GetParam("OutputFolderName");
                m_jobParams.SetParam("StepParameters", "OutputFolderName", m_ResFolderName);

                var result = MakeResultsFolder();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return result;
                }

                result = MoveResultFiles();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    m_message = "Error moving files into results folder";
                    return result;
                }

                // Move the Plots folder to the result files folder
                var diPlotsFolder = new DirectoryInfo(Path.Combine(m_WorkDir, "Plots"));

                if (diPlotsFolder.Exists)
                {
                    var strTargetFolderPath = Path.Combine(Path.Combine(m_WorkDir, m_ResFolderName), "Plots");

                    try
                    {
                        diPlotsFolder.MoveTo(strTargetFolderPath);
                    }
                    catch (Exception ex)
                    {
                        LogWarning("Exception moving Plot Folder " + diPlotsFolder.FullName + ": " + ex.Message);
                        m_FileTools.CopyDirectory(diPlotsFolder.FullName, strTargetFolderPath, true);
                    }

                    // Zip up (then delete) the PNG files in the plots folder
                    ZipPlotsFolder(diPlotsFolder);
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
                m_message = "Error in MultiAlignPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;

            }

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        /// <summary>
        /// Run the MultiAlign pipeline(s) listed in "MultiAlignOperations" parameter
        /// </summary>
        protected bool RunMultiAlign(string sMultiAlignConsolePath)
        {
            bool bSuccess;

            try
            {
                var oMultiAlignMage = new clsMultiAlignMage(m_jobParams, m_mgrParams, m_StatusTools);
                RegisterEvents(oMultiAlignMage);

                bSuccess = oMultiAlignMage.Run(sMultiAlignConsolePath);

                if (!bSuccess)
                {
                    if (!string.IsNullOrWhiteSpace(oMultiAlignMage.Message))
                        m_message = oMultiAlignMage.Message;
                    else
                        m_message = "Unknown error running MultiAlign";
                }

            }
            catch (Exception ex)
            {
                m_message = "Unknown error running MultiAlign: " + ex.Message;
                LogError(m_message);
                return false;
            }

            return bSuccess;

        }

        /// <summary>
        /// 
        /// </summary>
        protected void CopyFailedResultsToArchiveFolder()
        {
            var strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrEmpty(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            LogWarning("Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

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


        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string strMultiAlignProgLoc)
        {

            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var ioMultiAlignInfo = new FileInfo(strMultiAlignProgLoc);
            if (!ioMultiAlignInfo.Exists)
            {
                try
                {
                    strToolVersionInfo = "Unknown";
                    base.SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>());
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                    return false;
                }

                return false;
            }

            // Lookup the version of the Feature Finder
            var blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, ioMultiAlignInfo.FullName);
            if (!blnSuccess)
                return false;

            // Lookup the version of MultiAlignEngine (in the MultiAlign folder)
            var strMultiAlignEngineDllLoc = Path.Combine(ioMultiAlignInfo.DirectoryName, "MultiAlignEngine.dll");
            blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, strMultiAlignEngineDllLoc);
            if (!blnSuccess)
                return false;

            // Lookup the version of MultiAlignCore (in the MultiAlign folder)
            var strMultiAlignCoreDllLoc = Path.Combine(ioMultiAlignInfo.DirectoryName, "MultiAlignCore.dll");
            blnSuccess = StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, strMultiAlignCoreDllLoc);
            if (!blnSuccess)
                return false;

            // Store paths to key DLLs in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                new FileInfo(strMultiAlignProgLoc),
                new FileInfo(strMultiAlignEngineDllLoc),
                new FileInfo(strMultiAlignCoreDllLoc)
            };

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }

        }

        /// <summary>
        /// Zip the files in the plots folder if there are over 50 files
        /// </summary>
        /// <param name="diPlotsFolder"></param>
        /// <returns></returns>
        private bool ZipPlotsFolder(DirectoryInfo diPlotsFolder)
        {
            return ZipPlotsFolder(diPlotsFolder, 50);
        }

        /// <summary>
        /// Zip the files in the plots folder if there are over fileCountThreshold files
        /// </summary>
        /// <param name="diPlotsFolder"></param>
        /// <param name="fileCountThreshold"></param>
        /// <returns></returns>
        private bool ZipPlotsFolder(DirectoryInfo diPlotsFolder, int fileCountThreshold)
        {

            try
            {
                if (fileCountThreshold < 10)
                    fileCountThreshold = 10;

                diPlotsFolder.Refresh();
                if (diPlotsFolder.Exists)
                {
                    var pngFileCount = diPlotsFolder.GetFiles("*.png").Length;

                    if (pngFileCount == 0)
                    {
                        LogWarning("No .PNG files were found in the Plots folder");
                        return true;
                    }

                    if (pngFileCount == 1)
                    {
                        if (m_DebugLevel >= 2)
                            LogMessage("Only 1 .PNG file exists in the Plots folder; file will not be zipped");
                        return true;
                    }

                    if (pngFileCount < fileCountThreshold)
                    {
                        if (m_DebugLevel >= 2)
                            LogMessage("Only " + pngFileCount + " .PNG files exist in the Plots folder; files will not be zipped");
                        return true;
                    }

                    var strZipFilePath = Path.Combine(diPlotsFolder.FullName, "PlotFiles.zip");

                    var success = m_IonicZipTools.ZipDirectory(diPlotsFolder.FullName, strZipFilePath, false, "*.png");

                    if (!success)
                    {
                        var Msg = "Error zipping the plot files";
                        if (!string.IsNullOrEmpty(m_IonicZipTools.Message))
                            Msg += ": " + m_IonicZipTools.Message;

                        LogError(Msg);
                        m_message = clsGlobal.AppendToComment(m_message, Msg);
                        return false;
                    }

                    // Delete the PNG files in the plots folder
                    var errorCount = 0;
                    foreach (var fiFile in diPlotsFolder.GetFiles("*.png"))
                    {
                        try
                        {
                            fiFile.Delete();
                        }
                        catch (Exception ex)
                        {
                            errorCount++;
                            if (errorCount < 10)
                                LogError("Exception deleting file " + fiFile.Name + ": " + ex.Message);
                            else if (errorCount == 10)
                                LogError("Over 10 exceptions deleting plot files; additional exceptions will not be logged");
                        }

                    }
                }

            }
            catch (Exception ex)
            {
                var Msg = "Exception zipping plot files, job " + m_JobNum + ": " + ex.Message;
                LogError(Msg);
                m_message = clsGlobal.AppendToComment(m_message, "Error zipping the plot files");
                return false;
            }

            return true;

        }

    }
}


