using System.Linq;
using AnalysisManagerBase;
using System.IO;

namespace AnalysisManager_Mage_PlugIn
{

    /// <summary>
    /// Class that defines Mac Mage operations that can be selected by the
    /// "MageOperations" parameter
    /// </summary>
    public class MageAMOperations
    {

        #region Member Variables

        private readonly IJobParams _jobParams;

        private readonly IMgrParams _mgrParams;

        private bool _previousStepResultsImported;

        private string _warningMsg = string.Empty;
        private string _warningMsgVerbose = string.Empty;

        #endregion

        #region Constructors

        #region "Properties"

        public string WarningMsg => _warningMsg;

        public string WarningMsgVerbose => _warningMsgVerbose;

        #endregion

        public MageAMOperations(IJobParams jobParms, IMgrParams mgrParms)
        {
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
        public bool RunMageOperations(string mageOperations)
        {
            var ok = false;
            foreach (var mageOperation in mageOperations.Split(','))
            {
                ok = RunMageOperation(mageOperation.Trim());
                if (!ok)
                    break;
            }
            return ok;
        }

        /// <summary>
        /// Run a single Mage operation
        /// </summary>
        /// <param name="mageOperation"></param>
        /// <returns></returns>
        public bool RunMageOperation(string mageOperation)
        {
            var blnSuccess = false;

            // Note: case statements must be lowercase
            switch (mageOperation.ToLower())
            {
                case "extractfromjobs":
                    blnSuccess = ExtractFromJobs();
                    break;
                case "getfactors":
                    blnSuccess = GetFactors();
                    break;
                case "importdatapackagefiles":
                    blnSuccess = ImportDataPackageFiles();
                    break;
                case "importimprovclusterfiles":
                    blnSuccess = ImportIMPROVClusterDataPackageFile();
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
                    // Future: throw an error
            }
            return blnSuccess;
        }


        #region Mage Operations Functions

        private void AppendToWarningMessage(string message, string verboseMessage)
        {
            _warningMsg = clsGlobal.AppendToComment(_warningMsg, message);
            _warningMsgVerbose = clsGlobal.AppendToComment(_warningMsgVerbose, verboseMessage);
        }

        /// <summary>
        ///  don't do anything
        /// </summary>
        /// <returns></returns>
        private bool NoOperation()
        {
            GetPriorStepResults();
            return true;
        }

        /// <summary>
        /// Import factors for set of datasets referenced by analysis jobs in data package
        /// to table in the SQLite step results database (in crosstab format).
        /// </summary>
        /// <returns></returns>
        private bool GetFactors()
        {
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            var sql = GetSQLFromParameter("FactorsSource", mageObj);
            GetPriorStepResults();
            mageObj.GetDatasetFactors(sql);
            return true;
        }

        /// <summary>
        /// Setup and run Mage Extractor pipleline according to job parameters
        /// </summary>
        /// <returns></returns>
        private bool ExtractFromJobs()
        {
            var mageObj = new MageAMExtractionPipelines(_jobParams, _mgrParams);
            var sql = GetSQLFromParameter("ExtractionSource", mageObj);
            GetPriorStepResults();
            mageObj.ExtractFromJobs(sql);
            return true;
        }

        /// <summary>
        /// Import contents of set of master FDR template files (set members defined by job parameters)
        /// to tables in the SQLite step results database.
        /// </summary>
        /// <returns></returns>
        private bool ImportFDRTables()
        {
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            const string inputFolderPath = @"\\gigasax\DMS_Workflows\Mage\SpectralCounting\FDR";
            var inputfileList = mageObj.GetJobParam("MageFDRFiles");
            GetPriorStepResults();
            mageObj.ImportFilesInFolderToSQLite(inputFolderPath, inputfileList, "CopyAndImport");
            return true;
        }

        /// <summary>
        /// Imports contents of files specified by job parameter "DataPackageSourceFolderName"
        /// to tables in the SQLite step results database.
        /// </summary>
        /// <returns></returns>
        private bool ImportDataPackageFiles()
        {
            // Note: Switched from CopyAndImport to SimpleImport in March 2014
            const string importMode = "SimpleImport";
            return ImportDataPackageFiles(importMode);
        }

        /// <summary>
        /// Imports the specified files
        /// </summary>
        /// <param name="importMode">Valid modes: CopyAndImport, SimpleImport, AddDatasetIDToImport, IMPROVClusterImport</param>
        /// <returns></returns>
        private bool ImportDataPackageFiles(string importMode)
        {
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            var dataPackageStorageFolderRoot = mageObj.RequireJobParam("transferFolderPath");
            var inputFolderPath = Path.Combine(dataPackageStorageFolderRoot, mageObj.RequireJobParam("DataPackageSourceFolderName"));

            var diInputFolder = new DirectoryInfo(inputFolderPath);
            if (!diInputFolder.Exists)
                throw new DirectoryNotFoundException("Folder specified by job parameter DataPackageSourceFolderName does not exist: " + inputFolderPath);

            var lstInputFolderFiles = diInputFolder.GetFiles().ToList();

            if (lstInputFolderFiles.Count == 0)
                throw new DirectoryNotFoundException("DataPackageSourceFolderName is empty (should typically be ImportFiles and it should have a file named T_alias.txt): " + inputFolderPath);

            var lstMatchingFiles = (from item in lstInputFolderFiles
                                    where item.Name.ToLower() == "t_alias.txt"
                                    select item).ToList();

            if (lstMatchingFiles.Count == 0)
            {
                var analysisType = _jobParams.GetJobParameter("AnalysisType", string.Empty);
                if (analysisType.IndexOf("iTRAQ", System.StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    throw new System.Exception("File T_alias.txt was not found in " + inputFolderPath + "; this file is required because this is an iTRAQ analysis");
                }

                const string msg = "File T_alias.txt was not found in the DataPackageSourceFolderName folder; this may result in a failure during Ape processing";
                var msgVerbose = msg + ": " + inputFolderPath;
                AppendToWarningMessage(msg, msgVerbose);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msgVerbose);
            }

            GetPriorStepResults();
            mageObj.ImportFilesInFolderToSQLite(inputFolderPath, "", importMode);
            return true;
        }

        /// <summary>
        /// Copy files in data package folder named by "DataPackageSourceFolderName" job parameter
        /// to the step results folder and import contents to tables in the SQLite step results database,
        /// process import through filter that fills in missing cluster ID values
        /// </summary>
        /// <returns></returns>
        private bool ImportIMPROVClusterDataPackageFile()
        {
            const string importMode = "IMPROVClusterImport";
            return ImportDataPackageFiles(importMode);
        }

        /// <summary>
        /// Import contents of reporter ion results files for MASIC jobs in data package
        /// into a table in the SQLite step results database.
        /// </summary>
        /// <returns></returns>
        private bool ImportReporterIons()
        {
            _jobParams.AddAdditionalParameter("runtime", "Tool", "MASIC_Finnigan");
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            var sql = GetSQLFromParameter("ReporterIonSource", mageObj);
            GetPriorStepResults();
            mageObj.ImportJobResults(sql, "_ReporterIons.txt", "t_reporter_ions", "SimpleImport");
            return true;
        }

        /// <summary>
        /// Import contents of Sequest first hits results files for jobs in a data package
        /// into a table in the SQLite step results database.  Add dataset ID to imported data rows.
        /// </summary>
        /// <returns></returns>
        private bool ImportFirstHits()
        {
            _jobParams.AddAdditionalParameter("runtime", "Tool", "Sequest");
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            var sql = GetSQLFromParameter("FirstHitsSource", mageObj);
            GetPriorStepResults();
            mageObj.ImportJobResults(sql, "_fht.txt", "first_hits", "AddDatasetIDToImport");
            return true;
        }

        /// <summary>
        /// Import list of .raw files (full paths) for for datasets for sequest jobs in data package
        /// into a table in the SQLite step results database
        /// </summary>
        /// <returns></returns>
        private bool ImportRawFileList()
        {
            _jobParams.AddAdditionalParameter("runtime", "Tool", "Sequest");
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            var sql = GetSQLForTemplate("JobDatasetsFromDataPackageIDForTool", mageObj);
            GetPriorStepResults();
            mageObj.ImportFileList(sql, ".raw", "t_msms_raw_files");
            return true;
        }

        /// <summary>
        /// Get list of jobs (with metadata) in a data package into a table in the SQLite step results database
        /// </summary>
        /// <returns></returns>
        private bool ImportJobList()
        {
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            var sql = GetSQLForTemplate("JobsFromDataPackageID", mageObj);
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
        private void GetPriorStepResults()
        {
            if (!_previousStepResultsImported)
            {
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
        private string GetSQLFromParameter(string sourceName, MageAMPipelineBase mageObject)
        {
            var sqlTemplateName = mageObject.GetJobParam(sourceName);
            return GetSQLForTemplate(sqlTemplateName, mageObject);
        }

        /// <summary>
        /// Get an executable SQL statement by populating a given query template
        /// with actual parameter values
        /// </summary>
        /// <param name="sqlTemplateName">Name of SQL template to use to build query</param>
        /// <param name="mageObject">Object holding a copy of job parameters</param>
        /// <returns></returns>
        private static string GetSQLForTemplate(string sqlTemplateName, MageAMPipelineBase mageObject)
        {
            var qt = SQL.GetQueryTemplate(sqlTemplateName);
            var ps = GetParamValues(mageObject, qt.ParamNameList);
            return SQL.GetSQL(qt, ps);
        }

        /// <summary>
        /// Returns an array of parameter values for the given list of parameter names
        /// </summary>
        /// <param name="paramNameList">Comma delimited list of parameter names to retrieve values for</param>
        /// <param name="mageObject">Object holding a copy of job parameters</param>
        /// <returns>Array of values (order will match param name list)</returns>
        public static string[] GetParamValues(MageAMPipelineBase mageObject, string paramNameList)
        {
            return paramNameList.Split(',').Select(paramName => mageObject.GetJobParam(paramName.Trim())).ToArray();
        }

        #endregion
    }


}
