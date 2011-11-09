using AnalysisManagerBase;
using System.IO;
using System;
using Mage;
using MageDisplayLib;

namespace AnalysisManager_Mage_PlugIn {

    public class clsAnalysisToolRunnerMage : clsAnalysisToolRunnerBase {

		#region "Module Variables"

		protected const float PROGRESS_PCT_MAGE_DONE = 95;

		#endregion

        /// <summary>
        /// Run the Mage tool and disposition the results
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

				// Store the Mage version info in the database
				StoreToolVersionInfo();

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running Mage Plugin");

				//Change the name of the log file for the local log file to the plug in log filename
				String LogFileName = Path.Combine(m_WorkDir, "Mage_Log");
				log4net.GlobalContext.Properties["LogName"] = LogFileName;
				clsLogTools.ChangeLogFileName(LogFileName);

				try {

					// run the appropriate Mage pipeline(s) according to mode parameter
					blnSuccess = RunMage();

					// Change the name of the log file back to the analysis manager log file
					LogFileName = m_mgrParams.GetParam("logfilename");
					log4net.GlobalContext.Properties["LogName"] = LogFileName;
					clsLogTools.ChangeLogFileName(LogFileName);

					if (!blnSuccess && !string.IsNullOrWhiteSpace(m_message)) {
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running Mage: " + m_message);
					}
				} catch (Exception ex) {
					// Change the name of the log file back to the analysis manager log file
					LogFileName = m_mgrParams.GetParam("logfilename");
					log4net.GlobalContext.Properties["LogName"] = LogFileName;
					clsLogTools.ChangeLogFileName(LogFileName);

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error running Mage: " + ex.Message);
					blnSuccess = false;

					m_message = "Error running Mage";
					if (ex.Message.Contains("ImportFiles\\--No Files Found")) {
						m_message += "; ImportFiles folder in the data package is empty or does not exist";
					}

				}

				//Stop the job timer
				m_StopTime = System.DateTime.UtcNow;
				m_progress = PROGRESS_PCT_MAGE_DONE;

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
				m_message = "Error in MagePlugin->RunTool";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
				return IJobParams.CloseOutType.CLOSEOUT_FAILED;
			}

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// sequentially run the Mage operations listed in "MageOperations" parameter
        /// </summary>
        private bool RunMage() {
			bool blnSuccess = false;
			int iOperations = 0;

            string mageOperations = m_jobParams.GetParam("MageOperations");

			if (string.IsNullOrWhiteSpace(mageOperations)) {
				m_message = "MageOperations parameter is not defined";
				return false;
			}

            foreach (string mageOperation in mageOperations.Split(',')) {
				if (!string.IsNullOrWhiteSpace(mageOperation)) {
					iOperations += 1;
					blnSuccess = RunMageOperation(mageOperation.Trim());
					if (!blnSuccess) {
						m_message = "Error running Mage operation " + mageOperation;
						break;
					}
				}
            }

			if (iOperations == 0) {
				m_message = "MageOperations parameter was empty";
				return false;
			}

			return blnSuccess;
        }

        /// <summary>
        /// Run a single Mage operation
        /// </summary>
        /// <param name="mageOperation"></param>
        /// <returns></returns>
        private bool RunMageOperation(string mageOperation) {
            bool blnSuccess = false;

			// Note: case statements must be lowercase
            switch (mageOperation.ToLower()) {
                case "extractfromjobs":
                    blnSuccess = ExtractFromJobs();
                    break;
                case "getfactors":
                    blnSuccess = GetFactors();
                    break;
                case "importdatapackagefiles":
                    blnSuccess = ImportDataPackageFiles();
                    break;
                case "getfdrtables":
                    blnSuccess = ImportFDRTables();
                    break;
                default:
                    // Future: throw an error
                    break;
            }
            return blnSuccess;
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

            // Try to save whatever files are in the work directory
            string strFolderPathToArchive = null;
            strFolderPathToArchive = string.Copy(m_WorkDir);

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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

			try {
				System.Reflection.AssemblyName oAssemblyName = System.Reflection.Assembly.Load("Mage").GetName();

				string strNameAndVersion = null;
				strNameAndVersion = oAssemblyName.Name + ", Version=" + oAssemblyName.Version.ToString();
				strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion);
			} catch (Exception ex) {
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception determining Assembly info for Mage: " + ex.Message);
				return false;
			}

			// Store paths to key DLLs
			System.Collections.Generic.List<System.IO.FileInfo> ioToolFiles = new System.Collections.Generic.List<System.IO.FileInfo>();
			ioToolFiles.Add(new System.IO.FileInfo("Mage.dll"));
			ioToolFiles.Add(new System.IO.FileInfo("MageExtContentFilters.dll"));
			ioToolFiles.Add(new System.IO.FileInfo("MageExtExtractionFilters.dll"));

            try {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            } catch (Exception ex) {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }

        }

    }
}