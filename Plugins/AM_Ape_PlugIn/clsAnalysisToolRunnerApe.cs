using System.IO;
using AnalysisManagerBase;
using System;

namespace AnalysisManager_Ape_PlugIn
{
   public class clsAnalysisToolRunnerApe : clsAnalysisToolRunnerBase
   {
       protected const float PROGRESS_PCT_APE_START = 1;
       protected const float PROGRESS_PCT_APE_DONE = 99;

       protected string m_CurrentApeTask = string.Empty;
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

                //Change the name of the log file for the local log file to the plugin log filename
                String LogFileName = Path.Combine(m_WorkDir, "Ape_Log");
                log4net.GlobalContext.Properties["LogName"] = LogFileName;
                clsLogTools.ChangeLogFileName(LogFileName);

                bool processingSuccess;

                try
                {
                    m_progress = PROGRESS_PCT_APE_START;

                    processingSuccess = RunApe();

                    // Change the name of the log file back to the analysis manager log file
                    LogFileName = m_mgrParams.GetParam("logfilename");
                    log4net.GlobalContext.Properties["LogName"] = LogFileName;
                    clsLogTools.ChangeLogFileName(LogFileName);

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
                    LogFileName = m_mgrParams.GetParam("logfilename");
                    log4net.GlobalContext.Properties["LogName"] = LogFileName;
                    clsLogTools.ChangeLogFileName(LogFileName);

                    LogError("Error running Ape: " + ex.Message);
                    processingSuccess = false;
                    m_message = "Error running Ape";
                }

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;
                m_progress = PROGRESS_PCT_APE_DONE;

                //Add the current job data to the summary file
                UpdateSummaryFile();

                //Make sure objects are released
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
                m_Dataset = m_jobParams.GetParam("OutputFolderName");
                m_jobParams.SetParam("StepParameters", "OutputFolderName", m_ResFolderName);

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
           string apeOperations = m_jobParams.GetParam("ApeOperations");
           var ops = new clsApeAMOperations(m_jobParams, m_mgrParams);
           bool bSuccess = ops.RunApeOperations(apeOperations);

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

            string strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2) {
                LogDebug("Determining tool version info");
            }

            try
            {
                System.Reflection.AssemblyName oAssemblyName = System.Reflection.Assembly.Load("Ape").GetName();

                string strNameAndVersion = oAssemblyName.Name + ", Version=" + oAssemblyName.Version;
                strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion);
            }
            catch (Exception ex)
            {
                LogError("Exception determining Assembly info for Ape: " + ex.Message, ex);
                return false;
            }

            // Store paths to key DLLs
            var ioToolFiles = new System.Collections.Generic.List<FileInfo>();
            ioToolFiles.Add(new FileInfo("Ape.dll"));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message, ex);
                return false;
            }

        }

    }
}
    
    
