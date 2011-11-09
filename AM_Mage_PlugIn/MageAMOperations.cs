using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase;
using System.IO;

namespace AnalysisManager_Mage_PlugIn {

    class MageAMOperations {

        #region Member Variables

        protected IJobParams m_jobParams;

        protected IMgrParams m_mgrParams;

        #endregion

        #region Constructors

        public MageAMOperations(IJobParams jobParms, IMgrParams mgrParms) {
            m_jobParams = jobParms;
            m_mgrParams = mgrParms;
        }

        #endregion

        /// <summary>
        /// Run a list of Mage operations
        /// </summary>
        /// <param name="mageOperations"></param>
        /// <returns></returns>
        public bool RunMageOperations(string mageOperations) {
            bool ok = false;
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
        public bool RunMageOperation(string mageOperation) {
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
    }
}
