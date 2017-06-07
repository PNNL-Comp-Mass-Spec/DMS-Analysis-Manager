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

                //Do the base class stuff
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                LogMessage("Running MultiAlign Aggregator");

                // Determine the path to the LCMSFeatureFinder folder
                var progLoc = DetermineProgramLocation("MultiAlignProgLoc", "MultiAlignConsole.exe");

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

                bool processingSuccess;

                try
                {
                    m_progress = PROGRESS_PCT_MULTIALIGN_START;
                    processingSuccess = RunMultiAlign(progLoc);
                }
                catch (Exception ex)
                {
                    m_message = "Error running MultiAlign: " + ex.Message;
                    processingSuccess = false;
                }

                // Change the name of the log file back to the analysis manager log file
                LogFileName = m_mgrParams.GetParam("logfilename");
                log4net.GlobalContext.Properties["LogName"] = LogFileName;
                clsLogTools.ChangeLogFileName(LogFileName);

                if (processingSuccess)
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
                UpdateSummaryFile();

                //Make sure objects are released
                Thread.Sleep(500);
                PRISM.clsProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Override the output folder name and the dataset name (since this is a dataset aggregation job)
                m_ResFolderName = m_jobParams.GetParam("StepOutputFolderName");
                m_Dataset = m_jobParams.GetParam("OutputFolderName");
                m_jobParams.SetParam("StepParameters", "OutputFolderName", m_ResFolderName);

                var resultsFolderCreated = MakeResultsFolder();
                if (!resultsFolderCreated)
                {
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
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

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Error in MultiAlignPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;

            }

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
        /// Zip the files in the plots folder if there are over fileCountThreshold files (default 50, minimum 10)
        /// </summary>
        /// <param name="diPlotsFolder"></param>
        /// <param name="fileCountThreshold"></param>
        /// <returns></returns>
        private bool ZipPlotsFolder(DirectoryInfo diPlotsFolder, int fileCountThreshold = 50)
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
                        var msg = "Error zipping the plot files";
                        if (!string.IsNullOrEmpty(m_IonicZipTools.Message))
                            LogError(msg + ": " + m_IonicZipTools.Message);
                        else
                            LogError(msg);

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
                LogError("Exception zipping plot files", ex);
                return false;
            }

            return true;

        }

    }
}


