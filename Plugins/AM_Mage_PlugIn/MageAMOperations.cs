using AnalysisManagerBase;
using PRISM;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManager_Mage_PlugIn
{
    /// <summary>
    /// Class that defines Mac Mage operations that can be selected by the "MageOperations" parameter
    /// </summary>
    public class MageAMOperations : EventNotifier
    {
        // ReSharper disable once CommentTypo
        // Ignore Spelling: crosstab, mage, Improv, Sequest, Workflows

        private readonly IJobParams mJobParams;

        private readonly IMgrParams mMgrParams;

        private bool mPreviousStepResultsImported;

        public string WarningMsg { get; private set; } = "";

        public string WarningMsgVerbose { get; private set; } = "";

        public MageAMOperations(IJobParams jobParams, IMgrParams mgrParams, string logFilePath, bool appendDateToLogFileName)
        {
            mPreviousStepResultsImported = false;
            mJobParams = jobParams;
            mMgrParams = mgrParams;

            LogTools.ChangeLogFileBaseName(logFilePath, appendDateToLogFileName);
        }

        /// <summary>
        /// Run a list of Mage operations
        /// </summary>
        /// <param name="mageOperations"></param>
        /// <param name="jobCountLimit">Optionally set this to a positive value to limit the number of jobs to process (useful when debugging)</param>
        public bool RunMageOperations(string mageOperations, int jobCountLimit)
        {
            if (string.IsNullOrWhiteSpace(mageOperations))
            {
                OnWarningEvent("The mageOperations argument is empty; nothing for RunMageOperations to do");
                return false;
            }

            foreach (var mageOperation in mageOperations.Split(','))
            {
                var success = RunMageOperation(mageOperation.Trim(), jobCountLimit);

                if (!success)
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Run a single Mage operation
        /// </summary>
        /// <param name="mageOperation"></param>
        /// <param name="jobCountLimit">Optionally set this to a positive value to limit the number of jobs to process (useful when debugging)</param>
        public bool RunMageOperation(string mageOperation, int jobCountLimit)
        {
            if (jobCountLimit > 0)
            {
                OnWarningEvent("Limiting the number of jobs to process to {0} jobs", jobCountLimit);
            }

            if (mageOperation.Equals("ExtractFromJobs", StringComparison.OrdinalIgnoreCase))
                return ExtractFromJobs(jobCountLimit);

            if (mageOperation.Equals("GetFactors", StringComparison.OrdinalIgnoreCase))
                return GetFactors();

            if (mageOperation.Equals("ImportDataPackageFiles", StringComparison.OrdinalIgnoreCase))
                return ImportDataPackageFiles();

            if (mageOperation.Equals("ImportImprovClusterFiles", StringComparison.OrdinalIgnoreCase))
                return ImportIMPROVClusterDataPackageFile();

            if (mageOperation.Equals("GetFDRTables", StringComparison.OrdinalIgnoreCase))
                return ImportFDRTables();

            if (mageOperation.Equals("ImportFirstHits", StringComparison.OrdinalIgnoreCase))
                return ImportFirstHits(jobCountLimit);

            if (mageOperation.Equals("ImportReporterIons", StringComparison.OrdinalIgnoreCase))
                return ImportReporterIons(jobCountLimit);

            if (mageOperation.Equals("ImportRawFileList", StringComparison.OrdinalIgnoreCase))
                return ImportRawFileList(jobCountLimit);

            if (mageOperation.Equals("ImportJobList", StringComparison.OrdinalIgnoreCase))
                return ImportJobList(jobCountLimit);

            if (mageOperation.Equals("NoOperation", StringComparison.OrdinalIgnoreCase))
                return NoOperation();

            OnWarningEvent("Unrecognized Mage operation: " + mageOperation);
            return false;
        }

        private void AppendToWarningMessage(string message, string verboseMessage)
        {
            WarningMsg = Global.AppendToComment(WarningMsg, message);
            WarningMsgVerbose = Global.AppendToComment(WarningMsgVerbose, verboseMessage);
        }

        /// <summary>
        /// Don't do anything
        /// </summary>
        private bool NoOperation()
        {
            GetPriorStepResults();
            return true;
        }

        /// <summary>
        /// Import factors for set of datasets referenced by analysis jobs in data package
        /// to table in the SQLite step results database (in crosstab format).
        /// </summary>
        private bool GetFactors()
        {
            var mageObj = new MageAMFileProcessingPipelines(mJobParams, mMgrParams);
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
        /// <param name="jobCountLimit">Optionally set this to a positive value to limit the number of jobs to process (useful when debugging)</param>
        private bool ExtractFromJobs(int jobCountLimit)
        {
            var mageObj = new MageAMExtractionPipelines(mJobParams, mMgrParams);
            RegisterMageEvents(mageObj);

            var sql = GetSQLFromParameter("ExtractionSource", mageObj);
            OnDebugEvent("Running Mage Extractor pipeline: " + sql);

            GetPriorStepResults();
            mageObj.ExtractFromJobs(sql, jobCountLimit);
            return true;
        }

        /// <summary>
        /// Import contents of set of master FDR template files (set members defined by job parameters)
        /// to tables in the SQLite step results database.
        /// </summary>
        private bool ImportFDRTables()
        {
            var mageObj = new MageAMFileProcessingPipelines(mJobParams, mMgrParams);
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
        private bool ImportDataPackageFiles(string importMode)
        {
            var mageObj = new MageAMFileProcessingPipelines(mJobParams, mMgrParams);
            RegisterMageEvents(mageObj);

            var dataPackageStoragePathRoot = mageObj.RequireJobParam(AnalysisResources.JOB_PARAM_TRANSFER_DIRECTORY_PATH);
            var inputDirectoryPath = Path.Combine(dataPackageStoragePathRoot, mageObj.RequireJobParam("DataPackageSourceFolderName"));

            var inputDirectory = new DirectoryInfo(inputDirectoryPath);

            if (!inputDirectory.Exists)
            {
                throw new DirectoryNotFoundException(string.Format(
                    "Directory specified by job parameter DataPackageSourceFolderName does not exist: {0}",
                    inputDirectoryPath));
            }

            var filesInDirectory = inputDirectory.GetFiles().ToList();

            if (filesInDirectory.Count == 0)
            {
                throw new DirectoryNotFoundException(string.Format(
                    "Directory specified by job parameter DataPackageSourceFolderName has no files " +
                    "(the directory should typically be named ImportFiles and it should have a file named {0}): {1}",
                    AnalysisToolRunnerMage.T_ALIAS_FILE, inputDirectoryPath));
            }

            var matchingFiles =
                (from item in filesInDirectory
                 where string.Equals(item.Name, AnalysisToolRunnerMage.T_ALIAS_FILE, StringComparison.OrdinalIgnoreCase)
                 select item).ToList();

            if (matchingFiles.Count == 0)
            {
                // Look for a file named t_alias.txt.txt
                var misnamedFiles =
                    (from item in filesInDirectory
                     where string.Equals(item.Name, AnalysisToolRunnerMage.T_ALIAS_FILE + ".txt", StringComparison.OrdinalIgnoreCase)
                     select item).ToList();

                if (misnamedFiles.Count > 0)
                {
                    // DataPackageSourceFolderName has a mis-named t_alias.txt file
                    throw new DirectoryNotFoundException(string.Format(
                        "Directory specified by job parameter DataPackageSourceFolderName has a mis-named {0} file; rename it to remove the duplicate .txt extension: {1}",
                        AnalysisToolRunnerMage.T_ALIAS_FILE, inputDirectoryPath));
                }

                var analysisType = mJobParams.GetJobParameter("AnalysisType", string.Empty);

                if (analysisType.IndexOf("iTRAQ", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    analysisType.IndexOf("TMT", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    var typeDescription = analysisType.IndexOf("TMT", StringComparison.OrdinalIgnoreCase) >= 0
                        ? "a TMT"
                        : "an iTRAQ";

                    // File t_alias.txt was not found in ... this file is required because this is an iTRAQ analysis
                    //                                   ... this file is required because this is a TMT analysis
                    throw new Exception(string.Format("File {0} was not found in {1}; this file is required because this is {2} analysis",
                        AnalysisToolRunnerMage.T_ALIAS_FILE, inputDirectoryPath, typeDescription));
                }

                var msg = string.Format(
                    "File {0} was not found in the directory specified by job parameter DataPackageSourceFolderName; this may result in a failure during Ape processing; see {1}",
                    AnalysisToolRunnerMage.T_ALIAS_FILE, inputDirectory.FullName);

                var msgVerbose = msg + ": " + inputDirectoryPath;

                AppendToWarningMessage(msg, msgVerbose);
                OnWarningEvent(msgVerbose);

                return true;
            }

            // Validate the t_alias.txt file to remove blank rows and remove extra columns
            // In addition, round m/z values to three decimal places (since the MasterWorkflowSyn.xml file has them rounded to three decimal places)
            var success = ValidateAliasFile(matchingFiles[0], out var importDirectoryToUse);

            if (!success)
                return false;

            OnDebugEvent("Importing data package files into SQLite, source directory {0}, import mode {1}", importDirectoryToUse, importMode);

            GetPriorStepResults();
            mageObj.ImportFilesInDirectoryToSQLite(importDirectoryToUse, "", importMode);
            return true;
        }

        /// <summary>
        /// Copy files in data package directory named by "DataPackageSourceFolderName" job parameter
        /// to the step results directory and import contents to tables in the SQLite step results database,
        /// process import through filter that fills in missing cluster ID values
        /// </summary>
        private bool ImportIMPROVClusterDataPackageFile()
        {
            const string importMode = "IMPROVClusterImport";
            return ImportDataPackageFiles(importMode);
        }

        /// <summary>
        /// Import contents of reporter ion results files for MASIC jobs in data package
        /// into a table in the SQLite step results database.
        /// </summary>
        /// <param name="jobCountLimit">Optionally set this to a positive value to limit the number of jobs to process (useful when debugging)</param>
        private bool ImportReporterIons(int jobCountLimit)
        {
            mJobParams.AddAdditionalParameter("runtime", "Tool", "MASIC_Finnigan");
            var mageObj = new MageAMFileProcessingPipelines(mJobParams, mMgrParams);
            RegisterMageEvents(mageObj);

            var sql = GetSQLFromParameter("ReporterIonSource", mageObj);
            OnDebugEvent("Adding MASIC-based reporter ions to SQLite using: " + sql);

            GetPriorStepResults();
            mageObj.ImportJobResults(sql, "_ReporterIons.txt", "t_reporter_ions", "SimpleImport", jobCountLimit);
            return true;
        }

        /// <summary>
        /// Import contents of first hits results files for jobs in a data package
        /// into a table in the SQLite step results database.  Add dataset ID to imported data rows.
        /// </summary>
        /// <param name="jobCountLimit">Optionally set this to a positive value to limit the number of jobs to process (useful when debugging)</param>
        private bool ImportFirstHits(int jobCountLimit)
        {
            mJobParams.AddAdditionalParameter("runtime", "Tool", "Sequest");
            var mageObj = new MageAMFileProcessingPipelines(mJobParams, mMgrParams);
            RegisterMageEvents(mageObj);

            var sql = GetSQLFromParameter("FirstHitsSource", mageObj);
            OnDebugEvent("Adding FirstHits file data to SQLite using: " + sql);

            GetPriorStepResults();
            mageObj.ImportJobResults(sql, "_fht.txt", "first_hits", "AddDatasetIDToImport", jobCountLimit);
            return true;
        }

        /// <summary>
        /// Import list of .raw files (full paths) for datasets for jobs in data package
        /// into a table in the SQLite step results database
        /// </summary>
        /// <param name="jobCountLimit">Optionally set this to a positive value to limit the number of jobs to process (useful when debugging)</param>
        private bool ImportRawFileList(int jobCountLimit)
        {
            var mageObj = new MageAMFileProcessingPipelines(mJobParams, mMgrParams);
            RegisterMageEvents(mageObj);

            var sql = GetSQLForTemplate("JobDatasetsFromDataPackageID", mageObj);
            OnDebugEvent("Adding dataset metadata to SQLite using: " + sql);

            GetPriorStepResults();
            mageObj.ImportFileList(sql, ".raw", "t_msms_raw_files", jobCountLimit);
            return true;
        }

        /// <summary>
        /// Get list of jobs (with metadata) in a data package into a table in the SQLite step results database
        /// </summary>
        /// <param name="jobCountLimit">Optionally set this to a positive value to limit the number of jobs to process (useful when debugging)</param>
        private bool ImportJobList(int jobCountLimit)
        {
            var mageObj = new MageAMFileProcessingPipelines(mJobParams, mMgrParams);
            RegisterMageEvents(mageObj);

            var sql = GetSQLForTemplate("JobsFromDataPackageID", mageObj);
            OnDebugEvent("Adding job info to SQLite using: " + sql);

            GetPriorStepResults();
            mageObj.ImportJobList(sql, "t_data_package_analysis_jobs", jobCountLimit);
            return true;
        }

        /// <summary>
        /// Validate the t_alias.txt file (which should be in the ImportFiles directory below the data package)
        /// </summary>
        /// <param name="tAliasFile">t_alias.txt file to validate</param>
        /// <param name="importDirectoryToUse">
        /// Output: directory to actually import the t_alias.txt file from
        /// Defaults to the t_alias file's directory, but will be the local working directory if there was a write error (or rename error)
        /// </param>
        /// <returns>True if successful, false if an error</returns>
        private bool ValidateAliasFile(FileInfo tAliasFile, out string importDirectoryToUse)
        {
            importDirectoryToUse = string.Empty;

            try
            {
                if (tAliasFile.Directory == null)
                {
                    OnErrorEvent("Unable to determine the parent directory of file {0}", tAliasFile.FullName);
                    importDirectoryToUse = string.Empty;
                    return false;
                }

                importDirectoryToUse = tAliasFile.Directory.FullName;

                var updatedFilePath = Path.GetTempFileName();
                var replaceOriginal = false;

                OnDebugEvent("Validating the alias file: {0}", tAliasFile.FullName);

                using (var reader = new StreamReader(new FileStream(tAliasFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(updatedFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    // List of column indices to write to the output file (we skip columns with an empty column name)
                    // The expected column names are:
                    // Alias    Sample    Ion

                    var columnsIndicesToUse = new SortedSet<int>();
                    var ionColumnIndex = -1;

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

                                if (lineParts[i].Equals("Ion", StringComparison.OrdinalIgnoreCase) && ionColumnIndex < 0)
                                {
                                    ionColumnIndex = i;
                                }
                            }

                            if (skipList.Count > 0)
                            {
                                replaceOriginal = true;

                                if (skipList.Count == 1)
                                {
                                    OnWarningEvent("Skipped column {0} in {1} because it had an empty column name", skipList[0] + 1, tAliasFile.Name);
                                }
                                else
                                {
                                    OnWarningEvent("Skipped {0} columns in {1} due to empty column names", skipList.Count, tAliasFile.Name);
                                }
                            }
                        }

                        // Add data for columns that had a valid header
                        var dataToWrite = new List<string>();

                        foreach (var colIndex in columnsIndicesToUse)
                        {
                            if (colIndex >= lineParts.Length)
                                break;

                            if (colIndex == ionColumnIndex && lineParts[colIndex].IndexOf(".", StringComparison.OrdinalIgnoreCase) > 0)
                            {
                                // This is the reporter ion m/z column
                                // Round to three decimal places, since the MasterWorkflowSyn.xml file has m/z values rounded to three decimal places
                                // Workflow file location: \\gigasax\DMS_Workflows\Ape\iTRAQ\MasterWorkflowSyn.xml

                                if (!double.TryParse(lineParts[colIndex], out var mz))
                                {
                                    OnErrorEvent("The t_alias.txt file has a non-numeric reporter ion m/z value: {0} in {1}", lineParts[colIndex], tAliasFile.FullName);
                                    importDirectoryToUse = string.Empty;
                                    return false;
                                }

                                var formattedMz = mz.ToString("0.000");

                                if (!formattedMz.Equals(lineParts[colIndex]))
                                {
                                    replaceOriginal = true;
                                }

                                dataToWrite.Add(formattedMz);
                            }
                            else
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
                    return true;

                OnStatusEvent("Replacing the original t_alias.txt file with the reformatted one");

                try
                {
                    // Rename the original to .old
                    var invalidFile = new FileInfo(tAliasFile.FullName + ".old");

                    if (invalidFile.Exists)
                    {
                        OnDebugEvent("Deleting existing .old file: {0}", invalidFile.FullName);
                        invalidFile.Delete();
                    }

                    OnDebugEvent("Renaming {0} to {1}", tAliasFile.FullName, invalidFile.Name);

                    File.Move(tAliasFile.FullName, invalidFile.FullName);

                    OnDebugEvent("Copying {0} to {1}", updatedFilePath, tAliasFile.FullName);

                    // Copy the temp file to the remote server
                    File.Copy(updatedFilePath, tAliasFile.FullName);

                    try
                    {
                        OnDebugEvent("Deleting {0}", updatedFilePath);

                        // Delete the temp file
                        File.Delete(updatedFilePath);
                    }
                    catch (Exception ex)
                    {
                        OnWarningEvent("Unable to delete the file: {0}", ex.Message);
                    }

                    return true;
                }
                catch (Exception ex)
                {
                    OnWarningEvent("Error updating the t_alias.txt file on the remote share: {0}", ex.Message);
                }

                // Copy the updated t_alias.txt file to the manager's local working directory

                var workDir = mMgrParams.GetParam("WorkDir");

                if (string.IsNullOrWhiteSpace(workDir))
                {
                    OnErrorEvent("Manager parameter WorkDir is empty; unable to copy the t_alias.txt file locally");
                    return false;
                }

                var localImportFilesDirectory = new DirectoryInfo(Path.Combine(workDir, "ImportFiles"));

                try
                {
                    var targetFilePath = Path.Combine(localImportFilesDirectory.FullName, tAliasFile.Name);

                    if (!localImportFilesDirectory.Exists)
                        localImportFilesDirectory.Create();

                    File.Copy(updatedFilePath, targetFilePath, true);

                    importDirectoryToUse = localImportFilesDirectory.FullName;
                    mJobParams.AddResultFileToSkip(tAliasFile.Name);

                    OnStatusEvent("Due to errors updating the remote t_alias.txt file, will instead import {0}", targetFilePath);

                    return true;
                }
                catch (Exception ex)
                {
                    OnErrorEvent(string.Format("Error copying the updated t_alias.txt file to {0}: {1}", localImportFilesDirectory.FullName, ex.Message), ex);
                    return false;
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error validating the t_alias file: " + ex.Message, ex);

                return false;
            }
        }

        /// <summary>
        /// Import any results from previous step, if there are any, and if they haven't already be imported
        /// </summary>
        private void GetPriorStepResults()
        {
            if (mPreviousStepResultsImported)
                return;

            mPreviousStepResultsImported = true;
            var mageObj = new MageAMPipelineBase(mJobParams, mMgrParams);
            RegisterMageEvents(mageObj);

            mageObj.GetPriorResultsToWorkDir();
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
        private static string GetSQLForTemplate(string sqlTemplateName, MageAMPipelineBase mageObject)
        {
            var qt = SQL.GetQueryTemplate(sqlTemplateName);
            var ps = GetParamValues(mageObject, qt.ParamNameList);
            return SQL.GetSQL(qt, ps);
        }

        /// <summary>
        /// Returns an array of parameter values for the given list of parameter names
        /// </summary>
        /// <param name="mageObject">Object holding a copy of job parameters</param>
        /// <param name="paramNameList">Comma-delimited list of parameter names to retrieve values for</param>
        /// <returns>Array of values (order will match param name list)</returns>
        public static string[] GetParamValues(MageAMPipelineBase mageObject, string paramNameList)
        {
            return paramNameList.Split(',').Select(paramName => mageObject.GetJobParam(paramName.Trim())).ToArray();
        }

        private void RegisterMageEvents(IEventNotifier sourceClass)
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
    }
}
