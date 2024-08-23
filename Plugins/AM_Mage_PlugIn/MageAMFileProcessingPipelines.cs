using AnalysisManagerBase;
using Mage;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManager_Mage_PlugIn
{
    /// <summary>
    /// Class that defines basic Mage pipelines and functions that
    /// provide sub-operations that make up operations that Mac Mage plug-in can execute
    /// </summary>
    public class MageAMFileProcessingPipelines : MageAMPipelineBase
    {
        // ReSharper disable once CommentTypo
        // Ignore Spelling: crosstab, Improv, mage, Proc, sql

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="mgrParams"></param>
        public MageAMFileProcessingPipelines(IJobParams jobParams, IMgrParams mgrParams)
            : base(jobParams, mgrParams)
        {
        }

        /// <summary>
        /// Import contents of results files for jobs given by SQL query that satisfy given file name selector
        /// into given table in output SQLite database.  Apply given file process to the contents.
        /// </summary>
        /// <param name="jobListQuery">Query to run to get list of jobs</param>
        /// <param name="fileNameSelector">File name selector to select result files from list of jobs</param>
        /// <param name="tableName">SQLite table name that receives extracted contents of files</param>
        /// <param name="fileProcessName">Process to apply to file content extraction</param>
        /// <param name="jobCountLimit">Optionally set this to a positive value to limit the number of jobs to process (useful when debugging)</param>
        public void ImportJobResults(string jobListQuery, string fileNameSelector, string tableName, string fileProcessName, int jobCountLimit)
        {
            // get list of jobs from data package that have ReporterIon results
            BaseModule jobList = GetListOfDMSItems(jobListQuery, jobCountLimit);

            // get selected list of reporter ion files from list of jobs
            const string columnsToIncludeInOutput = "Job, Dataset, Dataset_ID, Tool, Settings_File, Parameter_File, Instrument";
            var fileList = GetListOfFilesFromDirectoryList(jobList, fileNameSelector, columnsToIncludeInOutput);

            // Importing job result data into SQLite, storing in table t_reporter_ions
            NotifyImportStarting("Importing job result data", tableName, fileList, "files");

            // import contents of each file in list
            var contentProc = new MageAMFileContentProcessor(this) { DBTableName = tableName, Operation = fileProcessName };
            var p = ProcessingPipeline.Assemble("Proc", fileList, contentProc);
            ConnectPipelineToStatusHandlers(p);
            p.RunRoot(null);
        }

        /// <summary>
        /// Import list of results files (full paths) for jobs given by SQL query that satisfy given file name selector
        /// into given table in output SQLite database.
        /// </summary>
        /// <param name="jobListQuery">Query to run to get list of jobs</param>
        /// <param name="fileNameSelector">File name selector to select result files from list of jobs</param>
        /// <param name="tableName">SQLite table name that receives extracted contents of files</param>
        /// <param name="jobCountLimit">Optionally set this to a positive value to limit the number of jobs to process (useful when debugging)</param>
        public void ImportFileList(string jobListQuery, string fileNameSelector, string tableName, int jobCountLimit)
        {
            // get list of datasets from jobs from data package (Note: NOT the data package dataset list)
            var jobList = GetListOfDMSItems(jobListQuery, jobCountLimit);

            // get selected list of files from list of datasets
            const string columnsToIncludeInOutput = "dataset_id, dataset, experiment, campaign, state, instrument, created, dataset_type";
            var fileList = GetListOfFilesFromDirectoryList(jobList, fileNameSelector, columnsToIncludeInOutput);

            // Importing result file metadata into SQLite, storing in table t_msms_raw_files
            NotifyImportStarting("Importing result file metadata", tableName, fileList, "files");

            // import file list to SQLite
            var dbFilePath = GetResultsDBFilePath();
            var writer = new SQLiteWriter { DbPath = dbFilePath, TableName = tableName };
            ProcessingPipeline.Assemble("Pipeline", fileList, writer).RunRoot(null);
        }

        /// <summary>
        /// Import the contents of files in the given source directory that pass the given name filter
        /// into the results SQLite database.
        /// </summary>
        /// <param name="inputDirectoryPath">Path to directory containing files to be imported</param>
        /// <param name="fileNameList">List of specific file names that will be imported (ignored if blank)</param>
        /// <param name="importMode">Valid modes: CopyAndImport, SimpleImport, AddDatasetIDToImport, IMPROVClusterImport</param>
        public void ImportFilesInDirectoryToSQLite(string inputDirectoryPath, string fileNameList, string importMode)
        {
            var reader = new FileListFilter();
            reader.AddDirectoryPath(inputDirectoryPath);
            reader.FileNameSelector = GetJobParam("FileNameSelector");
            reader.FileSelectorMode = GetJobParam("FileSelectorMode", "RegEx");

            var filesOrDirectories = GetJobParam("IncludeFilesOrFolders", "File");
            reader.IncludeFilesOrDirectories = GetJobParam("IncludeFilesOrDirectories", filesOrDirectories);

            reader.RecursiveSearch = GetJobParam("RecursiveSearch", "No");

            var fileList = new SimpleSink();

            var fileListPipeline = ProcessingPipeline.Assemble("GetFileListPipeline", reader, fileList);
            ConnectPipelineToStatusHandlers(fileListPipeline);
            fileListPipeline.RunRoot(null);

            var contentProc = new MageAMFileContentProcessor(this)
            {
                SourceDirectoryColumnName = reader.SourceDirectoryColumnName,
                SourceFileColumnName = reader.FileColumnName,
                Operation = importMode,
                DBTableName = "",
                FileNameList = fileNameList
            };

            // Importing files in directory into SQLite, storing in table auto-defined;
            NotifyImportStarting("Importing files in directory", "auto-defined", fileList, "files");

            var fileImportPipeline = ProcessingPipeline.Assemble("Proc", fileList, contentProc);
            ConnectPipelineToStatusHandlers(fileImportPipeline);
            fileImportPipeline.RunRoot(null);
        }

        /// <summary>
        /// Import the contents of the given file into the given table in the given results SQLite database
        /// </summary>
        /// <param name="inputFilePath">Full path to file whose contents are will be imported</param>
        /// <param name="dbFilePath">Full path to SQLite DB file into which file contents will be imported</param>
        /// <param name="dbTableName">Name of table in SQLite DB that will receive imported results</param>
        public void ImportFileToSQLite(string inputFilePath, string dbFilePath, string dbTableName)
        {
            var reader = new DelimitedFileReader { FilePath = inputFilePath };

            var writer = new SQLiteWriter();
            var tableName = (!string.IsNullOrEmpty(dbTableName)) ? dbTableName : Path.GetFileNameWithoutExtension(inputFilePath);
            writer.DbPath = dbFilePath;
            writer.TableName = tableName;

            OnDebugEvent("Importing file into SQLite, storing in table " + tableName + "; file " + inputFilePath);

            var pipeline = ProcessingPipeline.Assemble("DefaultFileProcessingPipeline", reader, writer);
            ConnectPipelineToStatusHandlers(pipeline);
            pipeline.RunRoot(null);
        }

        /// <summary>
        /// Import the contents of the given file into the given table in the given results SQLite database
        /// and perform given column mapping
        /// </summary>
        /// <param name="inputFilePath">Full path to file whose contents are will be imported</param>
        /// <param name="dbFilePath">Full path to SQLite DB file into which file contents will be imported</param>
        /// <param name="dbTableName">Name of table in SQLite DB that will receive imported results</param>
        /// <param name="outputColumnList">Mage output column spec</param>
        /// <param name="context">Mage context (dictionary to supply lookup values for new output columns)</param>
        public void ImportFileToSQLiteWithColumnMods(string inputFilePath, string dbFilePath, string dbTableName, string outputColumnList, Dictionary<string, string> context)
        {
            var reader = new DelimitedFileReader { FilePath = inputFilePath };

            BaseModule filter = new NullFilter
            {
                OutputColumnList = outputColumnList
            };

            filter.SetContext(context);

            var writer = new SQLiteWriter();
            var tableName = (!string.IsNullOrEmpty(dbTableName)) ? dbTableName : Path.GetFileNameWithoutExtension(inputFilePath);
            writer.DbPath = dbFilePath;
            writer.TableName = tableName;

            NotifyImportStarting("Importing file with remapped columns", tableName, null, "file");

            var pipeline = ProcessingPipeline.Assemble("DefaultFileProcessingPipeline", reader, filter, writer);
            ConnectPipelineToStatusHandlers(pipeline);
            pipeline.RunRoot(null);
        }

        public void ImportImprovClusterFileToSQLite(string inputFilePath, string dbFilePath, string dbTableName)
        {
            var reader = new DelimitedFileReader { FilePath = inputFilePath };

            var filter = new MissingValueFilter { FillColumnName = "Group_Num" };

            var writer = new SQLiteWriter();
            var tableName = (!string.IsNullOrEmpty(dbTableName)) ? dbTableName : Path.GetFileNameWithoutExtension(inputFilePath);
            writer.DbPath = dbFilePath;
            writer.TableName = tableName;

            NotifyImportStarting("Importing Improv cluster file", tableName, null, "file");

            var pipeline = ProcessingPipeline.Assemble("DefaultFileProcessingPipeline", reader, filter, writer);
            ConnectPipelineToStatusHandlers(pipeline);
            pipeline.RunRoot(null);
        }

        /// <summary>
        /// Get list of selected files from list of directories
        /// </summary>
        /// <param name="directoryListSource">Mage object that contains list of directories</param>
        /// <param name="fileNameSelector">File name selector to select files to be included in output list</param>
        /// <param name="passThroughColumns">List of columns from source object to pass through to output list object</param>
        /// <returns>Mage object containing list of files</returns>
        public SimpleSink GetListOfFilesFromDirectoryList(IBaseModule directoryListSource, string fileNameSelector, string passThroughColumns)
        {
            var sinkObject = new SimpleSink();

            // create file filter module and initialize it
            var fileFilter = new FileListFilter
            {
                FileNameSelector = fileNameSelector,
                SourceDirectoryColumnName = "Directory",
                FileColumnName = "Name",
                OutputColumnList = "Item|+|text, Name|+|text, File_Size_KB|+|text, Directory, " + passThroughColumns,
                FileSelectorMode = "RegEx",
                IncludeFilesOrDirectories = "File",
                RecursiveSearch = "No",
                SubdirectorySearchName = "*"
            };

            // build, wire, and run pipeline
            ProcessingPipeline.Assemble("FileListPipeline", directoryListSource, fileFilter, sinkObject).RunRoot(null);
            return sinkObject;
        }

        /// <summary>
        /// Make Mage pipeline using given SQL as source of factors and use it
        /// to create and populate a factors table in a SQLite database (in CrossTab format)
        /// </summary>
        /// <param name="sql">Query to use a source of factors</param>
        public void GetDatasetFactors(string sql)
        {
            // first pipeline - get factors crosstab to sink object
            var reader = MakeDBReaderModule(sql);

            var crosstab = new CrosstabFilter
            {
                EntityNameCol = "Dataset",
                EntityIDCol = "Dataset_ID",
                FactorNameCol = "Factor",
                FactorValueCol = "Value"
            };

            var sink = new SimpleSink();

            var readPipeline = ProcessingPipeline.Assemble("ReadFactors", reader, crosstab, sink);
            ConnectPipelineToStatusHandlers(readPipeline);
            readPipeline.RunRoot(null);

            // if there are factors, add them to results database
            if (sink.Rows.Count > 0)
            {
                // Second pipeline - write factors to SQLite DB
                // (and add "Alias" factor if not already present in factors)
                ProcessingPipeline writePipeline;

                var writer = new SQLiteWriter { DbPath = Path.Combine(WorkingDirPath, ResultsDBFileName), TableName = "t_factors" };

                if (!sink.ColumnIndex.ContainsKey("Alias"))
                {
                    var filter = new ModuleAddAlias();
                    filter.SetupAliasLookup(sink);
                    writePipeline = ProcessingPipeline.Assemble("WriteFactors", sink, filter, writer);
                }
                else
                {
                    writePipeline = ProcessingPipeline.Assemble("WriteFactors", sink, writer);
                }

                // Importing factors into SQLite, storing in table t_factors
                NotifyImportStarting("Importing factors", "t_factors", sink, "factors");

                ConnectPipelineToStatusHandlers(writePipeline);
                writePipeline.RunRoot(null);
            }
        }

        /// <summary>
        /// Make Mage pipeline to use given SQL to get list of jobs
        /// from data package into a SQLite database table
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="tableName"> </param>
        /// <param name="jobCountLimit">Optionally set this to a positive value to limit the number of jobs to process (useful when debugging)</param>
        public void ImportJobList(string sql, string tableName, int jobCountLimit)
        {
            var jobList = GetListOfDMSItems(sql, jobCountLimit);
            var writer = new SQLiteWriter { DbPath = GetResultsDBFilePath(), TableName = tableName };

            // Importing job list into SQLite, storing in table t_data_package_analysis_jobs
            NotifyImportStarting("Importing job list", tableName, jobList, "jobs");

            ProcessingPipeline.Assemble("JobListPipeline", jobList, writer).RunRoot(null);
        }

        private void NotifyImportStarting(string taskDescription, string tableName, ISinkModule itemList, string itemDescription)
        {
            int itemCount;

            if (itemList != null && itemList is SimpleSink jobsToProcess)
            {
                itemCount = jobsToProcess.Rows.Count;
            }
            else
            {
                itemCount = 1;
            }

            // Example message:
            // Importing result files into SQLite, storing in table T_Results; 6 jobs
            OnDebugEvent("{0} into SQLite, storing in table {1}; {2} {3}", taskDescription, tableName, itemCount, itemDescription);
        }
    }
}
