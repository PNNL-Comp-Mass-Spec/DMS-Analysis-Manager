using AnalysisManagerBase;
using Mage;
using MageExtExtractionFilters;
using MyEMSLReader;
using PRISM;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using PRISMDatabaseUtils;

namespace AnalysisManager_AScore_PlugIn
{
    public class AScoreMagePipeline : EventNotifier
    {
        // Ignore Spelling: const, Mage, msgfdb, msgfplus, SQL

        private string mResultsDBFileName = string.Empty;
        private string mWorkingDir;

        private JobParameters mJobParams;
        private ManagerParameters mMgrParams;

        private string mSearchType = string.Empty;
        private string mParamFilename = string.Empty;
        private string mFastaFilePath = string.Empty;
        private string mErrorMessage = string.Empty;

        private ZipFileTools mZipTools;

        public static DatasetListInfo mMyEMSLDatasetInfo;

        public string ErrorMessage => mErrorMessage;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="mgrParams"></param>
        /// <param name="zipTools"></param>
        public AScoreMagePipeline(IJobParams jobParams, IMgrParams mgrParams, ZipFileTools zipTools)
        {
            Initialize(jobParams, mgrParams, zipTools);

            if (mMyEMSLDatasetInfo == null)
            {
                var debugLevel = (short)mgrParams.GetParam("DebugLevel", 2);
                mMyEMSLDatasetInfo = new DatasetListInfo
                {
                    ReportMetadataURLs = mgrParams.TraceMode || debugLevel >= 2,
                    ThrowErrors = true,
                    TraceMode = mgrParams.TraceMode
                };
                RegisterEvents(mMyEMSLDatasetInfo);
            }
        }

        /// <summary>
        /// Set up internal variables
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="mgrParams"></param>
        /// <param name="zipTools"></param>
        private void Initialize(IJobParams jobParams, IMgrParams mgrParams, ZipFileTools zipTools)
        {
            mJobParams = new JobParameters(jobParams);
            mMgrParams = new ManagerParameters(mgrParams);
            mResultsDBFileName = mJobParams.RequireJobParam("ResultsBaseName") + ".db3";
            mWorkingDir = mMgrParams.RequireMgrParam("WorkDir");

            mSearchType = mJobParams.RequireJobParam("AScoreSearchType");

            if (mSearchType == "msgfdb")
                mSearchType = "msgfplus";

            mParamFilename = mJobParams.GetJobParam("AScoreParamFilename");

            // Define the path to the FASTA file
            mFastaFilePath = string.Empty;

            var localOrgDbDirectory = mMgrParams.RequireMgrParam("OrgDbDir");
            var fastaFileName = jobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName");

            if (!string.IsNullOrEmpty(fastaFileName))
            {
                var fastaFilePath = Path.Combine(localOrgDbDirectory, fastaFileName);

                var fastaFile = new FileInfo(fastaFilePath);

                if (fastaFile.Exists)
                    mFastaFilePath = fastaFile.FullName;
            }

            // Remove the file extension from mParamFilename
            mParamFilename = Path.GetFileNameWithoutExtension(mParamFilename);

            mZipTools = zipTools;
        }

        /// <summary>
        /// Do processing
        /// </summary>
        public bool Run()
        {
            var dataPackageID = mJobParams.RequireJobParam("DataPackageID");

            if (string.IsNullOrWhiteSpace(mParamFilename))
                return true;

            if (!GetAScoreParameterFile())
            {
                return false;
            }

            // Not sure how to show that this was a success
            var ascoreJobsToProcess = GetListOfDataPackageJobsToProcess(dataPackageID, mSearchType);
            ApplyAScoreToJobs(ascoreJobsToProcess);

            //  SimpleSink reporterIonJobsToProcess = GetListOfDataPackageJobsToProcess(dataPackageID, "MASIC_Finnigan");
            //  ImportReporterIons(reporterIonJobsToProcess, "t_reporter_ions");

            return true;
        }

        private bool GetAScoreParameterFile()
        {
            if (string.IsNullOrEmpty(mParamFilename))
            {
                mErrorMessage = "AScore ParamFileName not defined in the settings for this job; unable to continue";
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mErrorMessage);
                return false;
            }

            const string paramFileStoragePathKeyName = Global.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "AScore";
            var parameterFileStoragePath = mMgrParams.RequireMgrParam(paramFileStoragePathKeyName);

            if (string.IsNullOrEmpty(parameterFileStoragePath))
            {
                parameterFileStoragePath = @"\\gigasax\DMS_Parameter_Files\AScore";
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                    "Parameter " + paramFileStoragePathKeyName + " is not defined " +
                    "(obtained using V_Pipeline_Step_Tool_Storage_Paths in the Broker DB); " +
                    "will assume: " + parameterFileStoragePath);
            }

            // Find all parameter files that match the base name and copy to working directory
            var paramFileDirectory = new DirectoryInfo(parameterFileStoragePath);

            // Define the file mask to search for
            var fileMask = Path.GetFileNameWithoutExtension(mParamFilename) + "*.xml";

            var parameterFiles = paramFileDirectory.GetFiles(fileMask).ToList();

            if (parameterFiles.Count == 0)
            {
                mErrorMessage = "No parameter files matching " + fileMask + " were found at " + paramFileDirectory.FullName;
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mErrorMessage);
                return false;
            }

            foreach (var parameterFile in parameterFiles)
            {
                try
                {
                    parameterFile.CopyTo(Path.Combine(mWorkingDir, parameterFile.Name));
                }
                catch (Exception ex)
                {
                    mErrorMessage = "Error copying parameter file: " + ex.Message;
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mErrorMessage);
                }
            }

            return true;
        }

        /// <summary>
        /// query DMS to get list of data package jobs to process
        /// </summary>
        /// <param name="dataPackageID"></param>
        /// <param name="tool"></param>
        private SimpleSink GetListOfDataPackageJobsToProcess(string dataPackageID, string tool)
        {
            const string sqlTemplate = "SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE data_package_id = {0} AND tool LIKE '%{1}%'";
            var connStr = mMgrParams.RequireMgrParam("ConnectionString");
            var sql = string.Format(sqlTemplate, new object[] { dataPackageID, tool });

            return GetListOfItemsFromDB(sql, connStr);
        }

        /// <summary>
        /// Make a Mage pipeline that applies AScore processing to each job in the job list
        /// </summary>
        /// <param name="jobsToProcess"></param>
        private void ApplyAScoreToJobs(ISinkModule jobsToProcess)
        {
            var connectionString = mMgrParams.RequireMgrParam("ConnectionString");

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrParams.ManagerName);

            var ascoreModule = new MageAScoreModule(connectionStringToUse);

            ascoreModule.ErrorEvent += OnErrorEvent;
            ascoreModule.WarningEvent += OnWarningEvent;

            ascoreModule.ExtractionParams = GetExtractionParametersFromJobParameters();

            ascoreModule.TraceMode = mMgrParams.TraceMode;
            ascoreModule.WorkingDir = mWorkingDir;
            ascoreModule.ResultsDBFileName = mResultsDBFileName;
            ascoreModule.AscoreParamFileName = mParamFilename;
            ascoreModule.SearchType = mSearchType;

            ascoreModule.FastaFilePath = mFastaFilePath;

            ascoreModule.Initialize(mZipTools);

            var pipeline = ProcessingPipeline.Assemble("Process", jobsToProcess, ascoreModule);
            pipeline.RunRoot(null);
        }

        // <summary>
        // Import reporter ions into results SQLite database from given list of jobs
        // </summary>
        // <param name="reporterIonJobsToProcess"></param>
        // <param name="tableName"></param>
        // private void ImportReporterIons(SimpleSink reporterIonJobsToProcess, string tableName)
        // {
        //    // Get selected list of reporter ion files from list of jobs
        //    const string columnsToIncludeInOutput = "Job, Dataset, Dataset_ID, Tool, Settings_File, Parameter_File, Instrument";
        //    SimpleSink fileList = GetListOfFilesFromDirectoryList(reporterIonJobsToProcess, "_ReporterIons.txt", columnsToIncludeInOutput);

        //    // Module to import contents of each file in list
        //    var importer = new MageFileImport
        //    {
        //        DBTableName = tableName,
        //        DBFilePath = Path.Combine(mWorkingDir, mResultsDBFileName),
        //        ImportColumnList = "Dataset_ID|+|text, *"
        //    };

        //    var pipeline = ProcessingPipeline.Assemble("File_Import", fileList, importer);
        //    pipeline.RunRoot(null);
        // }

        /// <summary>
        /// Get a list of items using a database query
        /// </summary>
        /// <param name="sql">Query to use a source of jobs</param>
        /// <param name="connectionString"></param>
        /// <returns>A Mage module containing list of jobs</returns>
        public static SimpleSink GetListOfItemsFromDB(string sql, string connectionString)
        {
            var itemList = new SimpleSink();
            var reader = MakeDBReaderModule(sql, connectionString);
            var pipeline = ProcessingPipeline.Assemble("Get Items", reader, itemList);
            pipeline.RunRoot(null);
            return itemList;
        }

        /// <summary>
        /// Create a new SQLReader module to do a specific query
        /// </summary>
        /// <param name="sql">Query to use</param>
        /// <param name="connectionString"></param>
        public static SQLReader MakeDBReaderModule(string sql, string connectionString)
        {
            return new SQLReader(connectionString)
            {
                SQLText = sql
            };
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
            var pipeline = ProcessingPipeline.Assemble("FileListPipeline", directoryListSource, fileFilter, sinkObject);
            pipeline.RunRoot(null);

            return sinkObject;
        }

        /// <summary>
        /// Import the contents of the given file into the given table in the given results SQLite database
        /// </summary>
        /// <param name="inputFilePath">Full path to file whose contents are will be imported</param>
        /// <param name="dbFilePath">Full path to SQLite DB file into which file contents will be imported</param>
        /// <param name="dbTableName">Name of table in SQLite DB that will receive imported results</param>
        public static void ImportFileToSQLite(string inputFilePath, string dbFilePath, string dbTableName)
        {
            var reader = new DelimitedFileReader
            {
                FilePath = inputFilePath
            };

            var writer = new SQLiteWriter();
            var tableName = (!string.IsNullOrEmpty(dbTableName)) ? dbTableName : Path.GetFileNameWithoutExtension(inputFilePath);
            writer.DbPath = dbFilePath;
            writer.TableName = tableName;

            var pipeline = ProcessingPipeline.Assemble("ImportFileToSQLitePipeline", reader, writer);
            pipeline.RunRoot(null);
        }
        /*--*/
        /// <summary>
        /// Import the contents of the given file into the given table in the given results SQLite database
        /// and perform given column mapping
        /// </summary>
        /// <param name="inputFilePath">Full path to file whose contents are will be imported</param>
        /// <param name="dbFilePath">Full path to SQLite DB file into which file contents will be imported</param>
        /// <param name="dbTableName">Name of table in SQLite DB that will receive imported results</param>
        /// <param name="outputColumnList">Mage output column spec</param>
        /// <param name="context">Mage context (dictionary to supply lookup values for new output columns)</param>
        public static void ImportFileToSQLiteWithColumnMods(string inputFilePath, string dbFilePath, string dbTableName, string outputColumnList, Dictionary<string, string> context)
        {
            var reader = new DelimitedFileReader
            {
                FilePath = inputFilePath
            };

            BaseModule filter = new NullFilter
            {
                OutputColumnList = outputColumnList
            };

            filter.SetContext(context);

            var writer = new SQLiteWriter();
            var tableName = (!string.IsNullOrEmpty(dbTableName)) ? dbTableName : Path.GetFileNameWithoutExtension(inputFilePath);
            writer.DbPath = dbFilePath;
            writer.TableName = tableName;

            var pipeline = ProcessingPipeline.Assemble("DefaultFileProcessingPipeline", reader, filter, writer);
            pipeline.RunRoot(null);
        }

        /// <summary>
        /// Make a set of parameters for the extraction pipeline modules using the job parameters
        /// </summary>
        private ExtractionType GetExtractionParametersFromJobParameters()
        {
            var extractionParams = new ExtractionType();

            // extractionType should be 'SEQUEST First Hits' or 'MSGF+ First Hits'
            // Legacy jobs may have 'MSGFDB First Hits'
            var extractionType = mJobParams.RequireJobParam("ExtractionType");

            if (extractionType == "MSGFDB First Hits")
                extractionType = "MSGF+ First Hits";

            if (!ResultType.TypeList.ContainsKey(extractionType))
                throw new Exception("Invalid extractionType not supported by Mage: " + extractionType);

            extractionParams.RType = ResultType.TypeList[extractionType];
            extractionParams.KeepAllResults = mJobParams.GetJobParam("KeepAllResults", "Yes");
            extractionParams.ResultFilterSetID = mJobParams.GetJobParam("ResultFilterSetID", "All Pass");
            extractionParams.MSGFCutoff = mJobParams.GetJobParam("MSGFCutoff", "All Pass");

            return extractionParams;
        }

        public IEnumerable<string> GetTempFileNames()
        {
            var tempFileNames = new List<string>
            {
                MageAScoreModule.ASCORE_OUTPUT_FILE_NAME_BASE + "_Peptides.txt",
                MageAScoreModule.ASCORE_OUTPUT_FILE_NAME_BASE + "_ProteinMap.txt",
                MageAScoreModule.ASCORE_OUTPUT_FILE_NAME_BASE + "_Peptides_ProteinToPeptideMapping.txt"
            };

            return tempFileNames;
        }
    }
}
