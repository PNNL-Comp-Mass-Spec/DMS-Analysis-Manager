using AnalysisManagerBase;
using System.IO;
using System;
using Mage;
using MageDisplayLib;

namespace AnalysisManager_Mage_PlugIn {

    public class clsAnalysisToolRunnerMage : clsAnalysisToolRunnerBase {

        //    private ILog traceLog;

        public clsAnalysisToolRunnerMage() {
        }

        /// <summary>
        /// Run the Mage tool and disposition the results
        /// </summary>
        /// <returns></returns>
        public override IJobParams.CloseOutType RunTool() {

            IJobParams.CloseOutType result = default(IJobParams.CloseOutType);
            bool blnSuccess = false;

            //Do the base class stuff
            if (!(base.RunTool() == IJobParams.CloseOutType.CLOSEOUT_SUCCESS)) {
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running MageExtractor");

            // run the appropriate Mage pipeline(s) according to mode parameter
            blnSuccess = RunMage();

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

            m_ResFolderName = m_jobParams.GetParam("StepOutputFolderName");
            m_Dataset = m_jobParams.GetParam("OutputFolderName");
            m_jobParams.SetParam("OutputFolderName", m_jobParams.GetParam("StepOutputFolderName"));

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

            result = CopyResultsFolderToServer();
            if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS) {
                //TODO: What do we do here?
                // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                return result;
            }
            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// sequentially run the Mage operations listed in "MageOperations" parameter
        /// </summary>
        private bool RunMage() {
            bool ok = false;
            string mageOperations = m_jobParams.GetParam("MageOperations");
            foreach (string mageOperation in mageOperations.Split(',')) {
                ok = RunMageOperation(mageOperation.Trim());
                if (!ok) break;
            }
            return ok;
        }

        /// <summary>
        /// Run a single Mage operation
        /// </summary>
        /// <param name="mageOperation"></param>
        /// <returns></returns>
        private bool RunMageOperation(string mageOperation) {
            bool ok = false;
            switch (mageOperation) {
                case "ExtractFromJobs":
                    ok = ExtractFromJobs();
                    break;
                case "GetFactors":
                    ok = GetFactors();
                    break;
                case "ImportDataPackageFiles":
                    ok = ImportDataPackageFiles();
                    break;
                case "GetFDRTables":
                    ok = ImportFDRTables();
                    break;
                default:
                    // Future: throw an error
                    break;
            }
            return ok;
        }

        #region Mage Operations


        private bool GetFactors() {
            bool ok = true;
            String sql = SQL.GetSQL("FactorsSource", m_jobParams);
            MageAMExtractionPipelines mageObj = new MageAMExtractionPipelines(m_jobParams, m_mgrParams);
            mageObj.GetDatasetFactors(sql);
            return ok;
        }

        private bool ExtractFromJobs() {
            bool ok = true;
            String sql = SQL.GetSQL("ExtractionSource", m_jobParams);
            MageAMExtractionPipelines mageObj = new MageAMExtractionPipelines(m_jobParams, m_mgrParams);
            mageObj.ExtractFromJobs(sql);
            return ok;
        }

        private bool ImportFDRTables() {
            bool ok = true;
            MageAMFileProcessingPipelines mageObj = new MageAMFileProcessingPipelines(m_jobParams, m_mgrParams);
            string inputFolderPath = @"\\gigasax\DMS_Workflows\Mage\SpectralCounting\FDR";
            string inputfileList = mageObj.GetJobParam("MageFDRFiles");
            mageObj.ImportFilesToSQLiteResultsDB(inputFolderPath, inputfileList);
            return ok;
        }

       private bool ImportDataPackageFiles() {
            bool ok = true;
            MageAMFileProcessingPipelines mageObj = new MageAMFileProcessingPipelines(m_jobParams, m_mgrParams);
            string dataPackageStorageFolderRoot = mageObj.RequireJobParam("transferFolderPath");
            string inputFolderPath = Path.Combine(dataPackageStorageFolderRoot, mageObj.RequireJobParam("DataPackageSourceFolderName"));
            mageObj.ImportFilesToSQLiteResultsDB(inputFolderPath, "");
            return ok;
       }

        #endregion

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


    }
}