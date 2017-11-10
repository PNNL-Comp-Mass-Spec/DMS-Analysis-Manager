using System;
using System.IO;
using System.Threading;
using AnalysisManagerBase;
using log4net;
using InterDetect;
using PRISM;

namespace AnalysisManager_IDM_Plugin
{

    /// <summary>
    /// Class for running the IDM utility
    /// </summary>
    class clsAnalysisToolRunnerIDM : clsAnalysisToolRunnerBase
    {
        #region "Constants"
        public const string EXISTING_IDM_RESULTS_FILE_NAME = "ExistingIDMResults.db3";
        #endregion

        #region Members

        #endregion

        #region Methods

        /// <summary>
        /// Primary entry point for running this tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                var skipIDM = false;

                // Do the base class stuff
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

                        var cloneSuccess = sqLiteUtils.CloneDB(fiIDMResultsDB.FullName, Path.Combine(m_WorkDir, "Results.db3"), appendToExistingDB: true);

                        if (cloneSuccess)
                            skipIDM = true;

                        // success = sqLiteUtils.CopySqliteTable(fiIDMResultsDB.FullName, "t_precursor_interference", Path.Combine(m_WorkDir, "Results.db3"));

                    }
                    catch (Exception ex)
                    {
                        LogError(ex.Message + "; will run IDM instead of using existing results");
                    }
                }

                var processingSuccess = false;

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

                    // Change the name of the log file for the local log file to the plugin log filename
                    var logFileName = Path.Combine(m_WorkDir, "IDM_Log.txt");
                    GlobalContext.Properties["LogName"] = logFileName;
                    clsLogTools.ChangeLogFileName(logFileName);

                    try
                    {
                        // Create in instance of IDM and run the tool
                        var idm = new InterferenceDetector();

                        // Attach the progress event
                        idm.ProgressChanged += InterfenceDetectorProgressHandler;

                        idm.WorkDir = m_WorkDir;

                        processingSuccess = idm.Run(m_WorkDir, "Results.db3");

                        // Change the name of the log file back to the default
                        ResetLogFileNameToDefault();
                    }
                    catch (Exception ex)
                    {
                        // Change the name of the log file back to the default
                        ResetLogFileNameToDefault();

                        LogError("Error running IDM: " + ex.Message);
                        processingSuccess = false;
                    }
                }

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;
                m_progress = 100;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                Thread.Sleep(500);
                clsProgRunner.GarbageCollectNow();

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

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Error in IDMPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        private void InterfenceDetectorProgressHandler(InterferenceDetector id, ProgressInfo e)
        {

            m_progress = e.Value;

            UpdateStatusFile(m_progress, 60);

        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private void StoreToolVersionInfo()
        {
            var fiIDMdll = Path.Combine(clsGlobal.GetAppFolderPath(), "InterDetect.dll");

            StoreDotNETToolVersionInfo(fiIDMdll);

        }

        #endregion
    }
}
