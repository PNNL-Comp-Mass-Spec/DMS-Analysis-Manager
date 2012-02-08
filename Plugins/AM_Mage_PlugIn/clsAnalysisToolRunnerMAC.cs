using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase;


namespace AnalysisManager_MAC {

    /// <summary>
    /// This class provides a generic base for MAC tool run operations common to all MAC tool plug-ins.
    /// </summary>
    public abstract class clsAnalysisToolRunnerMAC : clsAnalysisToolRunnerBase {

        #region "Module Variables"

        protected const float PROGRESS_PCT_MAC_DONE = 95;

        #endregion

        /// <summary>
        /// Run the MAC tool and disposition the results
        /// </summary>
        /// <returns></returns>
        public override IJobParams.CloseOutType RunTool() {

            IJobParams.CloseOutType result = default(IJobParams.CloseOutType);

            try {

                bool blnSuccess = false;

                //Do the base class stuff
                if (!(base.RunTool() == IJobParams.CloseOutType.CLOSEOUT_SUCCESS)) {
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the MAC tool version info in the database
                StoreToolVersionInfo();

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MAC Plugin");


                try {

                    // run the appropriate MAC pipeline(s) according to mode parameter
                    blnSuccess = RunMACTool();

                    if (!blnSuccess && !string.IsNullOrWhiteSpace(m_message)) {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running MAC: " + m_message);
                    }
                } catch (Exception ex) {
                    // Change the name of the log file back to the analysis manager log file
                    string LogFileName = m_mgrParams.GetParam("logfilename");
                    log4net.GlobalContext.Properties["LogName"] = LogFileName;
                    clsLogTools.ChangeLogFileName(LogFileName);

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running MAC: " + ex.Message);
                    blnSuccess = false;

                    m_message = "Error running MAC";
                    if (ex.Message.Contains("ImportFiles\\--No Files Found")) {
                        m_message += "; ImportFiles folder in the data package is empty or does not exist";
                    }

                }

                //Stop the job timer
                m_StopTime = System.DateTime.UtcNow;
                m_progress = PROGRESS_PCT_MAC_DONE;

                //Add the current job data to the summary file
                if (!UpdateSummaryFile()) {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                }

                //Make sure objects are released
                //2 second delay
                System.Threading.Thread.Sleep(2000);
                GC.Collect();
                GC.WaitForPendingFinalizers();

                if (!blnSuccess) {
                    // Move the source files and any results to the Failed Job folder
                    // Useful for debugging problems
                    CopyFailedResultsToArchiveFolder();
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                m_ResFolderName = m_jobParams.GetParam("StepOutputFolderName");
                m_Dataset = m_jobParams.GetParam("OutputFolderName");
                m_jobParams.SetParam("StepParameters", "OutputFolderName", m_ResFolderName);

                result = MakeResultsFolder();
                if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS) {
                    // MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return result;
                }

                result = MoveResultFiles();
                if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS) {
                    // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    m_message = "Error moving files into results folder";
                    return result;
                }

                result = CopyResultsFolderToServer();
                if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS) {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return result;
                }

            } catch (Exception ex) {
                m_message = "Error in MAC Plugin->RunTool";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Run the specific MAC tool (override in a subclass)
        /// </summary>
        protected abstract bool RunMACTool();

        /// <summary>
        /// 
        /// </summary>
        protected void CopyFailedResultsToArchiveFolder() {
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

            // Make the results folder
            result = MakeResultsFolder();
            if (result == IJobParams.CloseOutType.CLOSEOUT_SUCCESS) {
                // Move the result files into the result folder
                result = MoveResultFiles();
                if (result == IJobParams.CloseOutType.CLOSEOUT_SUCCESS) {
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
        protected bool StoreToolVersionInfo() {

            string strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2) {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info for primary tool assembly");
            }

            try {
                strToolVersionInfo = GetToolNameAndVersion();
            } catch (Exception ex) {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for primary tool assembly: " + ex.Message);
                return false;
            }

            // Store paths to key DLLs
            List<System.IO.FileInfo> ioToolFiles = GetToolSupplementalVersionInfo();

            try {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            } catch (Exception ex) {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }

        }

        /// <summary>
        /// Subclass must override this to provide version info for primary MAC tool assembly
        /// </summary>
        /// <returns></returns>
        protected abstract string GetToolNameAndVersion();

        /// <summary>
        /// Subclass must override this to provide version info for any supplemental MAC tool assemblies
        /// </summary>
        /// <returns></returns>
        protected abstract List<System.IO.FileInfo> GetToolSupplementalVersionInfo();

 
    }
}
