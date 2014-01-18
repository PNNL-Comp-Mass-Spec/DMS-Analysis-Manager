using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using AnalysisManagerBase;
using Mage;
using MageExtExtractionFilters;
using MyEMSLReader;

namespace AnalysisManager_AScore_PlugIn
{

	public class clsAScoreMagePipeline
	{

		#region Member Variables

		protected string mResultsDBFileName = string.Empty;
		protected string mWorkingDir;

		protected JobParameters mJP;
		protected ManagerParameters mMP;
		protected static clsIonicZipTools m_IonicZipTools;
		protected const int IONIC_ZIP_MAX_FILESIZE_MB = 1280;
		protected static string mMessage = string.Empty;

		protected string mSearchType = string.Empty;
		protected string mParamFilename = string.Empty;
		protected string mErrorMessage = string.Empty;

		protected clsIonicZipTools mIonicZipTools;

		public static DatasetListInfo mMyEMSLDatasetInfo;

		#endregion

		#region Properties

		public string ErrorMessage
		{
			get
			{
				return mErrorMessage;
			}
		}

		#endregion

		#region Constructors

		public clsAScoreMagePipeline(IJobParams jobParms, IMgrParams mgrParms, clsIonicZipTools ionicZipTools)
		{
			Intialize(jobParms, mgrParms, ionicZipTools);

			if (mMyEMSLDatasetInfo == null)
			{
				mMyEMSLDatasetInfo = new DatasetListInfo();
				mMyEMSLDatasetInfo.ErrorEvent += mReader_ErrorEvent;
				mMyEMSLDatasetInfo.MessageEvent += mReader_MessageEvent;
			}
		}

		#endregion

		#region Initialization

		/// <summary>
		/// Set up internal variables
		/// </summary>
		/// <param name="jobParms"></param>
		/// <param name="mgrParms"></param>
		/// <param name="ionicZipTools"></param>
		private void Intialize(IJobParams jobParms, IMgrParams mgrParms, clsIonicZipTools ionicZipTools)
		{
			mJP = new JobParameters(jobParms);
			mMP = new ManagerParameters(mgrParms);
			mResultsDBFileName = mJP.RequireJobParam("ResultsBaseName") + ".db3";
			mWorkingDir = mMP.RequireMgrParam("workdir");

			mSearchType = mJP.RequireJobParam("AScoreSearchType");

			if (mSearchType == "msgfdb")
				mSearchType = "msgfplus";
			mParamFilename = mJP.GetJobParam("AScoreParamFilename");

			// Remove the file extension from mParamFilename
			mParamFilename = Path.GetFileNameWithoutExtension(mParamFilename);

			mIonicZipTools = ionicZipTools;
		}

		#endregion

		#region Processing

		/// <summary>
		/// Do processing
		/// </summary>
		public bool Run()
		{
			string dataPackageID = mJP.RequireJobParam("DataPackageID");

			if (mParamFilename == string.Empty)
				return true;

			if (!GetAScoreParameterFile())
			{
				return false;
			}

			//not sure how to show that this was a success
			SimpleSink ascoreJobsToProcess = GetListOfDataPackageJobsToProcess(dataPackageID, mSearchType);
			ApplyAScoreToJobs(ascoreJobsToProcess);

			//  SimpleSink reporterIonJobsToProcess = GetListOfDataPackageJobsToProcess(dataPackageID, "MASIC_Finnigan");
			//  ImportReporterIons(reporterIonJobsToProcess, "t_reporter_ions");

			return true;
		}

		#endregion

		#region Mage Pipelines and Utilities

		private bool GetAScoreParameterFile()
		{

			if (string.IsNullOrEmpty(mParamFilename))
			{
				mErrorMessage = "AScore ParmFileName not defined in the settings for this job; unable to continue";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage);
				return false;
			}

			const string strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "AScore";
			string strMAParameterFileStoragePath = mMP.RequireMgrParam(strParamFileStoragePathKeyName);
			if (string.IsNullOrEmpty(strMAParameterFileStoragePath))
			{
				strMAParameterFileStoragePath = @"\\gigasax\DMS_Parameter_Files\AScore";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter " + strParamFileStoragePathKeyName + " is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " + strMAParameterFileStoragePath);
			}

			//Find all parameter files that match the base name and copy to working directory
			var diParamFileFolder = new DirectoryInfo(strMAParameterFileStoragePath);

			// Define the file mask to search for
			var fileMask = Path.GetFileNameWithoutExtension(mParamFilename) + "*.xml";

			var fiParamFiles = diParamFileFolder.GetFiles(fileMask).ToList();

			if (fiParamFiles.Count == 0)
			{
				mErrorMessage = "No parameter files matching " + fileMask + " were found at " + diParamFileFolder.FullName;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage);
				return false;
			}

			foreach (var fiFile in fiParamFiles)
			{
				try
				{
					fiFile.CopyTo(Path.Combine(mWorkingDir, fiFile.Name));
				}
				catch (Exception ex)
				{
					mErrorMessage = "Error copying parameter file: " + ex.Message;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage);
				}
			}

			return true;

		}

		/// <summary>
		/// query DMS to get list of data package jobs to process
		/// </summary>
		/// <param name="dataPackageID"></param>
		/// <param name="tool"></param>
		/// <returns></returns>
		private SimpleSink GetListOfDataPackageJobsToProcess(string dataPackageID, string tool)
		{
			const string sqlTemplate = @"SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0} AND Tool LIKE '%{1}%'";
			string connStr = mMP.RequireMgrParam("ConnectionString");
			string sql = string.Format(sqlTemplate, new object[] { dataPackageID, tool });
			SimpleSink jobList = GetListOfItemsFromDB(sql, connStr);
			return jobList;
		}

		/// <summary>
		/// make a Mage pipeline that applies AScore processint to each job in job list
		/// </summary>
		/// <param name="jobsToProcess"></param>
		private void ApplyAScoreToJobs(SimpleSink jobsToProcess)
		{
			var ascoreModule = new MageAScoreModule();
			ascoreModule.WarningMessageUpdated += ascoreModule_WarningMessageUpdated;

			ascoreModule.ExtractionParms = GetExtractionParametersFromJobParameters();
			ascoreModule.WorkingDir = mWorkingDir;
			ascoreModule.ResultsDBFileName = mResultsDBFileName;
			ascoreModule.ascoreParamFileName = mParamFilename;
			ascoreModule.searchType = mSearchType;

			ascoreModule.Initialize(mIonicZipTools);

			ProcessingPipeline.Assemble("Process", jobsToProcess, ascoreModule).RunRoot(null);
		}

		// <summary>
		// Import reporter ions into results SQLite database from given list of jobs
		// </summary>
		// <param name="reporterIonJobsToProcess"></param>
		// <param name="tableName"></param>
		//private void ImportReporterIons(SimpleSink reporterIonJobsToProcess, string tableName)
		//{
		//    // get selected list of reporter ion files from list of jobs
		//    const string columnsToIncludeInOutput = "Job, Dataset, Dataset_ID, Tool, Settings_File, Parameter_File, Instrument";
		//    SimpleSink fileList = GetListOfFilesFromFolderList(reporterIonJobsToProcess, "_ReporterIons.txt", columnsToIncludeInOutput);

		//    // make module to import contents of each file in list
		//    var importer = new MageFileImport
		//    {
		//        DBTableName = tableName,
		//        DBFilePath = Path.Combine(mWorkingDir, mResultsDBFileName),
		//        ImportColumnList = "Dataset_ID|+|text, *"
		//    };

		//    ProcessingPipeline.Assemble("File_Import", fileList, importer).RunRoot(null);
		//}

		/// <summary>
		/// Get a list of items using a database query
		/// </summary>
		/// <param name="sql">Query to use a source of jobs</param>
		/// <param name="connectionString"></param>
		/// <returns>A Mage module containing list of jobs</returns>
		public static SimpleSink GetListOfItemsFromDB(string sql, string connectionString)
		{
			var itemList = new SimpleSink();
			MSSQLReader reader = MakeDBReaderModule(sql, connectionString);
			ProcessingPipeline pipeline = ProcessingPipeline.Assemble("Get Items", reader, itemList);
			pipeline.RunRoot(null);
			return itemList;
		}

		/// <summary>
		/// Create a new MSSQLReader module to do a specific query
		/// </summary>
		/// <param name="sql">Query to use</param>
		/// <param name="connectionString"></param>
		/// <returns></returns>
		public static MSSQLReader MakeDBReaderModule(String sql, string connectionString)
		{
			var reader = new MSSQLReader
			{
				ConnectionString = connectionString, SQLText = sql
			};
			return reader;
		}

		/// <summary>
		/// Get list of selected files from list of folders
		/// </summary>
		/// <param name="folderListSource">Mage object that contains list of folders</param>
		/// <param name="fileNameSelector">File name selector to select files to be included in output list</param>
		/// <param name="passThroughColumns">List of columns from source object to pass through to output list object</param>
		/// <returns>Mage object containing list of files</returns>
		public SimpleSink GetListOfFilesFromFolderList(IBaseModule folderListSource, string fileNameSelector, string passThroughColumns)
		{
			var sinkObject = new SimpleSink();

			// create file filter module and initialize it
			var fileFilter = new FileListFilter
			{
				FileNameSelector = fileNameSelector,
				SourceFolderColumnName = "Folder",
				FileColumnName = "Name",
				OutputColumnList = "Item|+|text, Name|+|text, File_Size_KB|+|text, Folder, " + passThroughColumns,
				FileSelectorMode = "RegEx",
				IncludeFilesOrFolders = "File",
				RecursiveSearch = "No",
				SubfolderSearchName = "*"
			};

			// build, wire, and run pipeline
			ProcessingPipeline.Assemble("FileListPipeline", folderListSource, fileFilter, sinkObject).RunRoot(null);
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
			string tableName = (!string.IsNullOrEmpty(dbTableName)) ? dbTableName : Path.GetFileNameWithoutExtension(inputFilePath);
			writer.DbPath = dbFilePath;
			writer.TableName = tableName;

			ProcessingPipeline.Assemble("ImportFileToSQLitePipeline", reader, writer).RunRoot(null);
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

			BaseModule filter = new NullFilter();
			filter.OutputColumnList = outputColumnList;
			filter.SetContext(context);

			var writer = new SQLiteWriter();
			string tableName = (!string.IsNullOrEmpty(dbTableName)) ? dbTableName : Path.GetFileNameWithoutExtension(inputFilePath);
			writer.DbPath = dbFilePath;
			writer.TableName = tableName;

			ProcessingPipeline.Assemble("DefaultFileProcessingPipeline", reader, filter, writer).RunRoot(null);
		}

		/// <summary>
		/// make a set of parameters for the extraction pipeline modules using the the job parameters
		/// </summary>
		protected ExtractionType GetExtractionParametersFromJobParameters()
		{
			var extractionParms = new ExtractionType();

			// extractionType should be 'Sequest First Hits' or 'MSGF+ First Hits'
			// Legacy jobs may have 'MSGFDB First Hits'
			String extractionType = mJP.RequireJobParam("ExtractionType");

			if (extractionType == "MSGFDB First Hits")
				extractionType = "MSGF+ First Hits";

			if (!ResultType.TypeList.ContainsKey(extractionType))
				throw new Exception("Invalid extractionType not supported by Mage: " + extractionType);

			extractionParms.RType = ResultType.TypeList[extractionType];
			extractionParms.KeepAllResults = mJP.GetJobParam("KeepAllResults", "Yes");
			extractionParms.ResultFilterSetID = mJP.GetJobParam("ResultFilterSetID", "All Pass");
			extractionParms.MSGFCutoff = mJP.GetJobParam("MSGFCutoff", "All Pass");
			return extractionParms;
		}

		#endregion

		#region Event Handlers

		void ascoreModule_WarningMessageUpdated(object sender, MageStatusEventArgs e)
		{
			var msg = "AScore warning: " + e.Message;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
		}

		void mReader_MessageEvent(object sender, MessageEventArgs e)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, e.Message);
		}

		void mReader_ErrorEvent(object sender, MessageEventArgs e)
		{
			mErrorMessage = "MyEMSL Reader error: " + e.Message;
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage);
		}

		#endregion
	
	}
}
