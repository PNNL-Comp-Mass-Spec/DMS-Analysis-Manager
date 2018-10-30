using System;
using System.Collections.Generic;
using System.Linq;
using AnalysisManagerBase;
using System.IO;
using PRISM;
using PRISM.Logging;

namespace AnalysisManager_Mage_PlugIn
{

    /// <summary>
    /// Class that defines Mac Mage operations that can be selected by the "MageOperations" parameter
    /// </summary>
    public class MageAMOperations : EventNotifier
    {

        #region Member Variables

        private readonly IJobParams _jobParams;

        private readonly IMgrParams _mgrParams;

        private bool _previousStepResultsImported;

        private string _warningMsg = string.Empty;
        private string _warningMsgVerbose = string.Empty;

        #endregion

        #region Properties

        public string WarningMsg => _warningMsg;

        public string WarningMsgVerbose => _warningMsgVerbose;

        #endregion

        #region Constructors

        public MageAMOperations(IJobParams jobParams, IMgrParams mgrParams, string logFilePath, bool appendDateToLogFileName)
        {
            _previousStepResultsImported = false;
            _jobParams = jobParams;
            _mgrParams = mgrParams;

            LogTools.ChangeLogFileBaseName(logFilePath, appendDateToLogFileName);
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
        /// Don't do anything
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
            RegisterMageEvents(mageObj);

            var sql = GetSQLFromParameter("FactorsSource", mageObj);
            OnDebugEvent("Adding factors to SQLite using: " + sql);

            GetPriorStepResults();
            mageObj.GetDatasetFactors(sql);
            return true;
        }

        /// <summary>
        /// Setup and run Mage Extractor pipeline according to job parameters
        /// </summary>
        /// <returns></returns>
        private bool ExtractFromJobs()
        {
            var mageObj = new MageAMExtractionPipelines(_jobParams, _mgrParams);
            RegisterMageEvents(mageObj);

            var sql = GetSQLFromParameter("ExtractionSource", mageObj);
            OnDebugEvent("Running Mage Extractor pipeline: " + sql);

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
            RegisterMageEvents(mageObj);

            const string inputDirectoryPath = @"\\gigasax\DMS_Workflows\Mage\SpectralCounting\FDR";
            var inputFileList = mageObj.GetJobParam("MageFDRFiles");

            OnDebugEvent("Importing FDR tables from " + inputDirectoryPath + ", files: " + inputFileList);

            GetPriorStepResults();
            mageObj.ImportFilesInDirectoryToSQLite(inputDirectoryPath, inputFileList, "CopyAndImport");
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
            RegisterMageEvents(mageObj);

            var dataPackageStoragePathRoot = mageObj.RequireJobParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH);
            var inputDirectoryPath = Path.Combine(dataPackageStoragePathRoot, mageObj.RequireJobParam("DataPackageSourceFolderName"));

            var inputDirectory = new DirectoryInfo(inputDirectoryPath);
            if (!inputDirectory.Exists)
                throw new DirectoryNotFoundException("Directory specified by job parameter DataPackageSourceFolderName does not exist: " + inputDirectoryPath);

            var filesInDirectory = inputDirectory.GetFiles().ToList();

            if (filesInDirectory.Count == 0)
                throw new DirectoryNotFoundException("DataPackageSourceFolderName has no files (should typically be named ImportFiles " +
                                                     "and it should have a file named " + clsAnalysisToolRunnerMage.T_ALIAS_FILE + "): " + inputDirectoryPath);

            var lstMatchingFiles = (from item in filesInDirectory
                                    where string.Equals(item.Name, clsAnalysisToolRunnerMage.T_ALIAS_FILE, StringComparison.OrdinalIgnoreCase)
                                    select item).ToList();

            if (lstMatchingFiles.Count == 0)
            {
                var analysisType = _jobParams.GetJobParameter("AnalysisType", string.Empty);
                if (analysisType.IndexOf("iTRAQ", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    // File T_alias.txt was not found in ...
                    throw new Exception(string.Format("File {0} was not found in {1}; this file is required because this is an iTRAQ analysis",
                        clsAnalysisToolRunnerMage.T_ALIAS_FILE, inputDirectoryPath));
                }

                var msg = string.Format(
                    "File {0} was not found in the DataPackageSourceFolderName directory; this may result in a failure during Ape processing",
                    clsAnalysisToolRunnerMage.T_ALIAS_FILE);

                var msgVerbose = msg + ": " + inputDirectoryPath;
                AppendToWarningMessage(msg, msgVerbose);
                OnWarningEvent(msgVerbose);
            }
            else
            {
                // Validate the T_alias.txt file to remove blank rows and remove extra columns
                ValidateAliasFile(lstMatchingFiles.First());
            }

            OnDebugEvent("Importing data package files into SQLite, source directory " + inputDirectoryPath + ", import mode " + importMode);

            GetPriorStepResults();
            mageObj.ImportFilesInDirectoryToSQLite(inputDirectoryPath, "", importMode);
            return true;
        }

        /// <summary>
        /// Copy files in data package directory named by "DataPackageSourceFolderName" job parameter
        /// to the step results directory and import contents to tables in the SQLite step results database,
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
            RegisterMageEvents(mageObj);

            var sql = GetSQLFromParameter("ReporterIonSource", mageObj);
            OnDebugEvent("Adding MASIC-based reporter ions to SQLite using: " + sql);

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
            RegisterMageEvents(mageObj);

            var sql = GetSQLFromParameter("FirstHitsSource", mageObj);
            OnDebugEvent("Adding FirstHits file data to SQLite using: " + sql);

            GetPriorStepResults();
            mageObj.ImportJobResults(sql, "_fht.txt", "first_hits", "AddDatasetIDToImport");
            return true;
        }

        /// <summary>
        /// Import list of .raw files (full paths) for datasets for jobs in data package
        /// into a table in the SQLite step results database
        /// </summary>
        /// <returns></returns>
        private bool ImportRawFileList()
        {
            _jobParams.AddAdditionalParameter("runtime", "Tool", "Sequest");
            var mageObj = new MageAMFileProcessingPipelines(_jobParams, _mgrParams);
            RegisterMageEvents(mageObj);

            var sql = GetSQLForTemplate("JobDatasetsFromDataPackageIDForTool", mageObj);
            OnDebugEvent("Adding dataset metadata to SQLite using: " + sql);

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
            RegisterMageEvents(mageObj);

            var sql = GetSQLForTemplate("JobsFromDataPackageID", mageObj);
            OnDebugEvent("Adding job info to SQLite using: " + sql);

            GetPriorStepResults();
            mageObj.ImportJobList(sql, "t_data_package_analysis_jobs");
            return true;
        }

        private void ValidateAliasFile(FileSystemInfo tAliasFile)
        {
            try
            {
                var updatedFilePath = Path.GetTempFileName();
                var replaceOriginal = false;

                using (var reader = new StreamReader(new FileStream(tAliasFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(updatedFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {

                    // List of column indices to write to the output file (we skip columns with an empty column name)
                    var columnsIndicesToUse = new SortedSet<int>();

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            replaceOriginal = true;
                            continue;
                        }

                        var lineParts = dataLine.Split('\t');

                        if (columnsIndicesToUse.Count == 0)
                        {
                            var skipList = new List<int>();

                            for (var i = 0; i < lineParts.Length; i++)
                            {
                                if (string.IsNullOrWhiteSpace(lineParts[i]))
                                {
                                    skipList.Add(i);
                                    continue;
                                }
                                columnsIndicesToUse.Add(i);
                            }

                            if (skipList.Count > 0)
                            {
                                replaceOriginal = true;
                                if (skipList.Count == 1)
                                {
                                    OnWarningEvent(string.Format("Skipped column {0} in {1} because it had an empty column name",
                                                                 skipList.First() + 1, tAliasFile.Name));
                                }
                                else
                                {
                                    OnWarningEvent(string.Format("Skipped {0} columns in {1} due to empty column names",
                                                                 skipList.Count, tAliasFile.Name));
                                }
                            }

                        }

                        // Add data for columns that had a valid header
                        var dataToWrite = new List<string>();
                        foreach (var colIndex in columnsIndicesToUse)
                        {
                            if (colIndex < lineParts.Length)
                            {
                                dataToWrite.Add(lineParts[colIndex]);
                            }
                        }

                        // Add any missing columns
                        while (dataToWrite.Count < columnsIndicesToUse.Count)
                        {
                            dataToWrite.Add(string.Empty);
                            replaceOriginal = true;
                        }

                        writer.WriteLine(string.Join("\t", dataToWrite));
                    }
                }

                if (!replaceOriginal)
                    return;

                // Rename the original to .old
                var invalidFile = new FileInfo(tAliasFile.FullName + ".old");
                if (invalidFile.Exists)
                    invalidFile.Delete();

                File.Move(tAliasFile.FullName, invalidFile.FullName);

                // Copy the temp file to the remote server
                File.Copy(updatedFilePath, tAliasFile.FullName);

                // Delete the temp file
                File.Delete(updatedFilePath);

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error validating the t_alias file: " + ex.Message, ex);
            }
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
                RegisterMageEvents(mageObj);

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

        private void RegisterMageEvents(EventNotifier sourceClass)
        {
            sourceClass.DebugEvent += Mage_DebugEvent;
            sourceClass.StatusEvent += OnStatusEvent;
            sourceClass.ErrorEvent += OnErrorEvent;
            sourceClass.WarningEvent += OnWarningEvent;
            sourceClass.ProgressUpdate += OnProgressUpdate;
        }

        private void Mage_DebugEvent(string message)
        {
            if (string.IsNullOrWhiteSpace(message))
                return;

            switch (message)
            {
                case "Running...":
                case "Process Complete":
                    break;
                default:
                    OnDebugEvent(message);
                    break;
            }

        }

        #endregion
    }

}
