using System.Linq;
using AnalysisManagerBase;
using System.IO;

namespace AnalysisManager_Mage_PlugIn {

    /// <summary>
    /// Class that defines Mac Mage operations that can be selected by the 
    /// "MageOperations" parameter
    /// </summary>
    public class MageAMOperations {

        #region Member Variables

        private readonly IJobParams _jobParams;

        private readonly IMgrParams _mgrParams;

        private bool _previousStepResultsImported;

        #endregion

        #region Constructors

        public MageAMOperations(IJobParams jobParms, IMgrParams mgrParms) {
            _previousStepResultsImported = false;
            _jobParams = jobParms;
            _mgrParams = mgrParms;
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
                case "nooperation":
                    blnSuccess = NoOperation();
                    break;
                case "makemetadatadb":
                    blnSuccess = MakeMetadataDB();
                    break;
                    // Future: throw an error
            }
            return blnSuccess;
        }

        /// <summary>
        /// Create SQLite db file containing metadata for data package
        /// </summary>
        /// <returns></returns>
        private bool MakeMetadataDB()
        {
            var mageObj = new MageAMMetadataPipelines(_jobParams, _mgrParams);
            //GetPriorStepResults();
            mageObj.MakeMetadataDB();
            return true;
        }

        #region Mage Operations Functions

        /// <summary>
        ///  don't do anything
        /// </summary>
        /// <returns></returns>
        private bool NoOperation() {
            GetPriorStepResults();
            return true;
        }

        /// <summary>
        /// Import factors for set of datasets referenced by analysis jobs in data package
        /// to table in the SQLite step results database (in crosstab format). 
        /// </summary>
        /// <returns></returns>
        private bool GetFactors() {
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            string sql = GetSQLFromParameter("FactorsSource", mageObj);
            GetPriorStepResults();
            mageObj.GetDatasetFactors(sql);
            return true;
        }

        /// <summary>
        /// Setup and run Mage Extractor pipleline according to job parameters
        /// </summary>
        /// <returns></returns>
        private bool ExtractFromJobs() {
            var mageObj = new MageAMExtractionPipelines(_jobParams, _mgrParams);
            string sql = GetSQLFromParameter("ExtractionSource", mageObj);
            GetPriorStepResults();
            mageObj.ExtractFromJobs(sql);
            return true;
        }

        /// <summary>
        /// Import contents of set of master FDR template files (set members defined by job parameters) 
        /// to tables in the SQLite step results database.
        /// </summary>
        /// <returns></returns>
        private bool ImportFDRTables() {
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            const string inputFolderPath = @"\\gigasax\DMS_Workflows\Mage\SpectralCounting\FDR";
            string inputfileList = mageObj.GetJobParam("MageFDRFiles");
            GetPriorStepResults();
            mageObj.ImportFilesInFolderToSQLite(inputFolderPath, inputfileList, "CopyAndImport");
            return true;
        }

        /// <summary>
        /// Copy files in data package folder named by "DataPackageSourceFolderName" job parameter
        /// to the step results folder and import contents to tables in the SQLite step results database.  
        /// </summary>
        /// <returns></returns>
        private bool ImportDataPackageFiles() {
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            string dataPackageStorageFolderRoot = mageObj.RequireJobParam("transferFolderPath");
            string inputFolderPath = Path.Combine(dataPackageStorageFolderRoot, mageObj.RequireJobParam("DataPackageSourceFolderName"));
            GetPriorStepResults();
            mageObj.ImportFilesInFolderToSQLite(inputFolderPath, "", "CopyAndImport");
            return true;
        }

        /// <summary>
        /// Import contents of reporter ion results files for MASIC jobs in data package
        /// into a table in the SQLite step results database. 
        /// </summary>
        /// <returns></returns>
        private bool ImportReporterIons() {
            _jobParams.AddAdditionalParameter("runtime", "Tool", "MASIC_Finnigan");
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            string sql = GetSQLFromParameter("ReporterIonSource", mageObj);
            GetPriorStepResults();
            mageObj.ImportJobResults(sql, "_ReporterIons.txt", "t_reporter_ions", "SimpleImport");
            return true;
        }

        /// <summary>
        /// Import contents of Sequest first hits results files for jobs in a data package
        /// into a table in the SQLite step results database.  Add dataset ID to imported data rows.
        /// </summary>
        /// <returns></returns>
        private bool ImportFirstHits() {
            _jobParams.AddAdditionalParameter("runtime", "Tool", "Sequest");
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            string sql = GetSQLFromParameter("FirstHitsSource", mageObj);
            GetPriorStepResults();
            mageObj.ImportJobResults(sql, "_fht.txt", "first_hits", "AddDatasetIDToImport");
            return true;
        }

        /// <summary>
        /// Import list of .raw files (full paths) for for datasets for sequest jobs in data package 
        /// into a table in the SQLite step results database
        /// </summary>
        /// <returns></returns>
        private bool ImportRawFileList() {
            _jobParams.AddAdditionalParameter("runtime", "Tool", "Sequest");
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            string sql = GetSQLForTemplate("JobDatasetsFromDataPackageIDForTool", mageObj);
            GetPriorStepResults();
            mageObj.ImportFileList(sql, ".raw", "t_msms_raw_files");
            return true;
        }

        /// <summary>
        /// Get list of jobs (with metadata) in a data package into a table in the SQLite step results database
        /// </summary>
        /// <returns></returns>
        private bool ImportJobList() {
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            string sql = GetSQLForTemplate("JobsFromDataPackageID", mageObj);
            GetPriorStepResults();
            mageObj.ImportJobList(sql, "t_data_package_analysis_jobs");
            return true;
        }


        #endregion

        #region Utility Functions

        /// <summary>
        /// Import any results from previous step, if there are any, and if they haven't already be imported
        /// </summary>
        /// <returns></returns>
        private void GetPriorStepResults() {
            if (!_previousStepResultsImported) {
                _previousStepResultsImported = true;
                var mageObj = new MageAMPipelineBase(_jobParams, _mgrParams);
                mageObj.GetPriorResultsToWorkDir();
            }
        }


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
            string[] ps = GetParamValues(mageObject, qt.ParamNameList);
            return SQL.GetSQL(qt, ps);
        }

        /// <summary>
        /// Returns an array of parameter values for the given list of parameter names
        /// </summary>
        /// <param name="paramNameList">Comma delimited list of parameter names to retrieve values for</param>
        /// <param name="mageObject">Object holding a copy of job parameters</param>
        /// <returns>Array of values (order will match param name list)</returns>
        public static string[] GetParamValues(MageAMPipelineBase mageObject, string paramNameList) {
            return paramNameList.Split(',').Select(paramName => mageObject.GetJobParam(paramName.Trim())).ToArray();
        }

        #endregion
    }


}
