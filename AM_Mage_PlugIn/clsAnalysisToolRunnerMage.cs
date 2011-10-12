using AnalysisManagerBase;
using System.IO;
using System;
using Mage;
using MageDisplayLib;
using log4net;

namespace AnalysisManager_Mage_PlugIn {

    public class clsAnalysisToolRunnerMage : clsAnalysisToolRunnerBase, IPipelineMonitor {

        private ILog traceLog;

        public clsAnalysisToolRunnerMage() {
            Initialize();
        }

        private void Initialize() {
            // Set log4net path and kick the logger into action
            string LogFileName = Path.Combine(SavedState.DataDirectory, "log.txt");
            log4net.GlobalContext.Properties["LogName"] = LogFileName;
            traceLog = LogManager.GetLogger("TraceLog");
            traceLog.Info("Starting");
        }
 
        public override IJobParams.CloseOutType RunTool() {

            IJobParams.CloseOutType result = default(IJobParams.CloseOutType);
            bool blnSuccess = false;

            //Do the base class stuff
            if (!(base.RunTool() == IJobParams.CloseOutType.CLOSEOUT_SUCCESS)) {
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MageExtractor");

            // run the appropriate Mage pipeline(s) according to mode parameter
            RunMage();

            //Add the current job data to the summary file
            if (!UpdateSummaryFile()) {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
            }

            if (!blnSuccess) {
                // Move the source files and any results to the Failed Job folder
                // Useful for debugging MultiAlign problems
                CopyFailedResultsToArchiveFolder();
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            result = MakeResultsFolder();
            if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS) {
                //TODO: What do we do here?
                return result;
            }

            result = MoveResultFiles();
            if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS) {
                //TODO: What do we do here?
                // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                return result;
            }

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// run the appropriate Mage pipeline(s) according to mode parameter
        /// </summary>
        private void RunMage() {

            string mageMode = m_jobParams.GetParam("MageMode");
            switch (mageMode) {
                case "ExtractJobsFromDataPackage":
                    MageAMExtractionPipelines mageObj = new MageAMExtractionPipelines(m_jobParams, m_mgrParams, this);
                    break;
                default: 
                    // Future: throw an error
                    break;
            }
        }

        protected void CopyFailedResultsToArchiveFolder() {
            IJobParams.CloseOutType result = default(IJobParams.CloseOutType);

            string strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrEmpty(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory (however, delete the .UIMF file first, plus also the Decon2LS .csv files)
            string strFolderPathToArchive = null;
            strFolderPathToArchive = string.Copy(m_WorkDir);

            try {
                System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset + ".UIMF"));
                System.IO.File.Delete(System.IO.Path.Combine(m_WorkDir, m_Dataset + "*.csv"));
            } catch (Exception ex) {
                // Ignore errors here
            }

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
        protected bool StoreToolVersionInfo(string strMultiAlignProgLoc) {

            string strToolVersionInfo = string.Empty;
            System.IO.FileInfo ioMultiAlignProg = default(System.IO.FileInfo);

            if (m_DebugLevel >= 2) {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            ioMultiAlignProg = new System.IO.FileInfo(strMultiAlignProgLoc);

            // Lookup the version of MultiAlign
            base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, ioMultiAlignProg.FullName);

            // Lookup the version of additional DLLs
            base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "PNNLOmics.dll"));
            base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "MultiAlignEngine.dll"));
            base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "PNNLProteomics.dll"));
            base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "PNNLControls.dll"));

            // Store paths to key DLLs in ioToolFiles
            System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
            ioToolFiles.Add(new System.IO.FileInfo(System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "MultiAlignEngine.dll")));
            ioToolFiles.Add(new System.IO.FileInfo(System.IO.Path.Combine(ioMultiAlignProg.DirectoryName, "PNNLOmics.dll")));

            try {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            } catch (Exception ex) {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }

        }

        #region Pipeline Utilities

        public void ConnectPipelineToStatusHandlers(ProcessingPipeline pipeline) {
            pipeline.OnStatusMessageUpdated += HandlePipelineUpdate;
            pipeline.OnRunCompleted += HandlePipelineCompletion;
        }

        public void ConnectPipelineQueueToStatusHandlers(PipelineQueue pipelineQueue) {
            pipelineQueue.OnRunCompleted += HandlePipelineUpdate;
            pipelineQueue.OnPipelineStarted += HandlePipelineCompletion;
        }
 
        #endregion

        #region Pipeline Update Message Handlers

        private void HandlePipelineUpdate(object sender, MageStatusEventArgs args) {
            Console.WriteLine(args.Message);
        }

        private void HandlePipelineCompletion(object sender, MageStatusEventArgs args) {
            Console.WriteLine(args.Message);
        }


        #endregion



    }
}