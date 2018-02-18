using System.IO;
using AnalysisManagerBase;
using System;
using PRISM.Logging;

namespace AnalysisManager_Ape_PlugIn
{
    /// <summary>
    /// Class for running Ape
    /// </summary>
   public class clsAnalysisToolRunnerApe : clsAnalysisToolRunnerBase
   {
       protected const float PROGRESS_PCT_APE_START = 1;
       protected const float PROGRESS_PCT_APE_DONE = 99;

       protected string m_CurrentApeTask = string.Empty;
       protected DateTime m_LastStatusUpdateTime;

        /// <summary>
        /// Primary entry point for running this tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
       public override CloseOutType RunTool()
       {
            try
            {

                m_jobParams.SetParam("JobParameters", "DatasetNum", m_jobParams.GetParam("OutputFolderPath"));

                // Do the base class stuff
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the Ape version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining Ape version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_CurrentApeTask = "Running Ape";
                m_LastStatusUpdateTime = DateTime.UtcNow;
                UpdateStatusRunning();

                LogMessage(m_CurrentApeTask);

                // Change the name of the log file for the local log file to the plugin log filename
                var logFileName = Path.Combine(m_WorkDir, "Ape_Log.txt");
                LogTools.ChangeLogFileBaseName(logFileName, appendDateToBaseName: false);

                bool processingSuccess;

                try
                {
                    m_progress = PROGRESS_PCT_APE_START;

                    processingSuccess = RunApe();

                    // Change the name of the log file back to the analysis manager log file
                    ResetLogFileNameToDefault();

                    if (!processingSuccess) {
                        if (string.IsNullOrWhiteSpace(m_message))
                            LogError("Error running Ape");
                        else
                            LogError("Error running Ape: " + m_message);
                    }
                }
                catch (Exception ex)
                {
                    // Change the name of the log file back to the analysis manager log file
                    ResetLogFileNameToDefault();

                    LogError("Error running Ape: " + ex.Message);
                    processingSuccess = false;
                    m_message = "Error running Ape";
                }

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;
                m_progress = PROGRESS_PCT_APE_DONE;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                System.Threading.Thread.Sleep(500);
                PRISM.clsProgRunner.GarbageCollectNow();

                if(!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Override the output folder name and the dataset name (since this is a dataset aggregation job)
                m_ResFolderName = m_jobParams.GetParam("StepOutputFolderName");
                m_Dataset = m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME);
                m_jobParams.SetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME, m_ResFolderName);

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex) {
                m_message = "Error in ApePlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;

            }


        }

       /// <summary>
       /// Run the Ape pipeline(s) listed in "ApeOperations" parameter
       /// </summary>
       protected bool RunApe()
       {
           // run the appropriate Mage pipeline(s) according to operations list parameter
           var apeOperations = m_jobParams.GetParam("ApeOperations");
           var ops = new clsApeAMOperations(m_jobParams, m_mgrParams);
           var bSuccess = ops.RunApeOperations(apeOperations);

           if (!bSuccess)
               m_message = "Error running ApeOperations: " + ops.ErrorMessage;

           return bSuccess;

       }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {

            var toolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2) {
                LogDebug("Determining tool version info");
            }

            StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "Ape");

            // Store paths to key DLLs
            var ioToolFiles = new System.Collections.Generic.List<FileInfo> {
                new FileInfo("Ape.dll")
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message, ex);
                return false;
            }

        }

    }
}


