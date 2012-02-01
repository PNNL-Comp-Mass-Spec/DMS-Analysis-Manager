using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase;
using System.IO;

namespace AnalysisManager_Mage_PlugIn {

    /// <summary>
    /// Class that defines Mac Mage operations that can be selected by the 
    /// "MageOperations" parameter
    /// </summary>
    public class MageAMOperations {

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
                case "importfirsthits":
                    blnSuccess = ImportFirstHits();
                    break;
                case "importreporterions":
                    blnSuccess = ImportReporterIons();
                    break;
                case "importrawfilelist":
                    blnSuccess = ImportRawFileList();
                    break;
                case "importjoblist":
                    blnSuccess = ImportJobList();
                    break;
                default:
                    // Future: throw an error
                    break;
            }
            return blnSuccess;
        }

        #region Mage Operations Functions

        /// <summary>
        /// Import factors for set of datasets referenced by analysis jobs in data package
        /// to table in the SQLite step results database (in crosstab format). 
        /// </summary>
        /// <returns></returns>
        private bool GetFactors() {
            bool ok = true;
            MageAMFileProcessingPipelines mageObj = new MageAMFileProcessingPipelines(m_jobParams, m_mgrParams);
            string sql = GetSQLFromParameter("FactorsSource", mageObj);
            mageObj.GetDatasetFactors(sql);
            return ok;
        }

        /// <summary>
        /// Setup and run Mage Extractor pipleline according to job parameters
        /// </summary>
        /// <returns></returns>
        private bool ExtractFromJobs() {
            bool ok = true;
            MageAMExtractionPipelines mageObj = new MageAMExtractionPipelines(m_jobParams, m_mgrParams);
            string sql = GetSQLFromParameter("ExtractionSource", mageObj);
            mageObj.ExtractFromJobs(sql);
            return ok;
        }

        /// <summary>
        /// Import contents of set of master FDR template files (set members defined by job parameters) 
        /// to tables in the SQLite step results database.
        /// </summary>
        /// <returns></returns>
        private bool ImportFDRTables() {
            bool ok = true;
            MageAMFileProcessingPipelines mageObj = new MageAMFileProcessingPipelines(m_jobParams, m_mgrParams);
            string inputFolderPath = @"\\gigasax\DMS_Workflows\Mage\SpectralCounting\FDR";
            string inputfileList = mageObj.GetJobParam("MageFDRFiles");
            mageObj.ImportFilesInFolderToSQLite(inputFolderPath, inputfileList, "CopyAndImport");
            return ok;
        }

        /// <summary>
        /// Copy files in data package folder named by "DataPackageSourceFolderName" job parameter
        /// to the step results folder and import contents to tables in the SQLite step results database.  
        /// </summary>
        /// <returns></returns>
        private bool ImportDataPackageFiles() {
            bool ok = true;
            MageAMFileProcessingPipelines mageObj = new MageAMFileProcessingPipelines(m_jobParams, m_mgrParams);
            string dataPackageStorageFolderRoot = mageObj.RequireJobParam("transferFolderPath");
            string inputFolderPath = Path.Combine(dataPackageStorageFolderRoot, mageObj.RequireJobParam("DataPackageSourceFolderName"));
            mageObj.ImportFilesInFolderToSQLite(inputFolderPath, "", "CopyAndImport");
            return ok;
        }

        /// <summary>
        /// Import contents of reporter ion results files for MASIC jobs in data package
        /// into a table in the SQLite step results database. 
        /// </summary>
        /// <returns></returns>
        private bool ImportReporterIons() {
            bool ok = true;
            m_jobParams.AddAdditionalParameter("runtime", "Tool", "MASIC_Finnigan");
            MageAMFileProcessingPipelines mageObj = new MageAMFileProcessingPipelines(m_jobParams, m_mgrParams);
            string sql = GetSQLFromParameter("ReporterIonSource", mageObj);
            mageObj.ImportJobResults(sql, "_ReporterIons.txt", "reporter_ions", "SimpleImport");
            return ok;
        }

        /// <summary>
        /// Import contents of Sequest first hits results files for jobs in a data package
        /// into a table in the SQLite step results database.  Add dataset ID to imported data rows.
        /// </summary>
        /// <returns></returns>
        private bool ImportFirstHits() {
            bool ok = true;
            m_jobParams.AddAdditionalParameter("runtime", "Tool", "Sequest");
            MageAMFileProcessingPipelines mageObj = new MageAMFileProcessingPipelines(m_jobParams, m_mgrParams);
            string sql = GetSQLFromParameter("FirstHitsSource", mageObj);
            mageObj.ImportJobResults(sql, "_fht.txt", "first_hits", "AddDatasetIDToImport");
            return ok;
        }

        /// <summary>
        /// Import list of .raw files (full paths) for for datasets for sequest jobs in data package 
        /// into a table in the SQLite step results database
        /// </summary>
        /// <returns></returns>
        private bool ImportRawFileList() {
            bool ok = true;
            m_jobParams.AddAdditionalParameter("runtime", "Tool", "Sequest");
            MageAMFileProcessingPipelines mageObj = new MageAMFileProcessingPipelines(m_jobParams, m_mgrParams);
            string sql = GetSQLForTemplate("JobDatasetsFromDataPackageIDForTool", mageObj);
            mageObj.ImportFileList(sql, ".raw", "t_msms_raw_files");
            return ok;
        }

        /// <summary>
        /// Get list of jobs (with metadata) in a data package into a table in the SQLite step results database
        /// </summary>
        /// <returns></returns>
        private bool ImportJobList() {
            bool ok = true;
            MageAMFileProcessingPipelines mageObj = new MageAMFileProcessingPipelines(m_jobParams, m_mgrParams);
            string sql = GetSQLForTemplate("JobsFromDataPackageID", mageObj);
            mageObj.ImportJobList(sql, "t_data_package_analysis_jobs");
            return ok;
        }


        #endregion

        #region Utility Functions

        /// <summary>
        /// Get an executable SQL statement by populating a given query template
        /// with actual parameter values
        /// </summary>
        /// <param name="sourceName">Name of parameter that contains name of query template to use</param>
        /// <param name="mageObject">Object holding a copy of job parameters</param>
        /// <returns>Executable SQL statement</returns>
        private string GetSQLFromParameter(string sourceName, MageAMPipelineBase mageObject) {
            string sqlTemplateName = mageObject.GetJobParam(sourceName);
            return GetSQLForTemplate(sqlTemplateName, mageObject);
        }

        /// <summary>
        /// Get an executable SQL statement by populating a given query template
        /// with actual parameter values
        /// </summary>
        /// <param name="sqlTemplateName">Name of SQL template to use to build query</param>
        /// <param name="mageObject">Object holding a copy of job parameters</param>
        /// <returns></returns>
        private static string GetSQLForTemplate(string sqlTemplateName, MageAMPipelineBase mageObject) {
            SQL.QueryTemplate qt = SQL.GetQueryTemplate(sqlTemplateName);
            string[] ps = GetParamValues(mageObject, qt.paramNameList);
            return SQL.GetSQL(qt, ps);
        }

        /// <summary>
        /// Returns an array of parameter values for the given list of parameter names
        /// </summary>
        /// <param name="parms">Parameter object holding parameter values</param>
        /// <param name="paramNameList">Comma delimited list of parameter names to retrieve values for</param>
        /// <param name="mageObject">Object holding a copy of job parameters</param>
        /// <returns>Array of values (order will match param name list)</returns>
        public static string[] GetParamValues(MageAMPipelineBase mageObject, string paramNameList) {
            List<string> paramValues = new List<string>();
            foreach (string paramName in paramNameList.Split(',')) {
                string val = mageObject.GetJobParam(paramName.Trim());
                paramValues.Add(val);
            }
            return paramValues.ToArray();
        }

        #endregion
    }
}

/*
ExtractFromJobs() {
GetSQLFromParameter("ExtractionSource", mageObj);
RequireJobParam("ExtractionType"); //"Sequest First Hits"
ResultType.TypeList[extractionType];
GetJobParam("KeepAllResults", "Yes");
GetJobParam("ResultFilterSetID", "All Pass");
GetJobParam("MSGFCutoff", "All Pass");


GetFactors() {
GetSQLFromParameter("FactorsSource", mageObj);


ImportFDRTables() {
mageObj.GetJobParam("MageFDRFiles");
GetJobParam("FileNameSelector");
GetJobParam("FileSelectorMode", "RegEx");
GetJobParam("IncludeFilesOrFolders", "File");
GetJobParam("RecursiveSearch", "No");



ImportDataPackageFiles() {
mageObj.RequireJobParam("transferFolderPath");
RequireJobParam("DataPackageSourceFolderName"));


ImportReporterIons() {
GetSQLFromParameter("ReporterIonSource", mageObj);


ImportFirstHits() {
GetSQLFromParameter("FirstHitsSource", mageObj);


ImportRawFileList() {
GetSQLForTemplate("JobDatasetsFromDataPackageIDForTool", mageObj);


ImportJobList() {
GetSQLForTemplate("JobsFromDataPackageID", mageObj);
*/