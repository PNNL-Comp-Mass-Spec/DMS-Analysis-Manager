using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.IO;
using System.Threading;
using System.Xml;
using AnalysisManagerBase;
using AScore_DLL;
using Mage;
using MageExtExtractionFilters;
using AScore_DLL.Managers;
using AScore_DLL.Managers.DatasetManagers;
using MyEMSLReader;
using PRISM.Processes;
using MessageEventArgs = AScore_DLL.MessageEventArgs;

namespace AnalysisManager_AScore_PlugIn
{

	public class clsAScoreMage
	{

		#region Member Variables

		protected string mResultsDBFileName = "";
		protected string mWorkingDir;

		protected JobParameters mJP;
		protected ManagerParameters mMP;
		protected static clsIonicZipTools m_IonicZipTools;
		protected const int IONIC_ZIP_MAX_FILESIZE_MB = 1280;
		protected static string mMessage = "";

		protected string mSearchType = "";
		protected string mParamFilename = "";

		protected clsIonicZipTools mIonicZipTools;

		protected static DatasetListInfo mMyEMSLDatasetInfo;

		#endregion

		#region Constructors

		public clsAScoreMage(IJobParams jobParms, IMgrParams mgrParms, clsIonicZipTools ionicZipTools)
		{
			Intialize(jobParms, mgrParms, ionicZipTools);

			if (mMyEMSLDatasetInfo == null)
			{
				mMyEMSLDatasetInfo = new DatasetListInfo();
				mMyEMSLDatasetInfo.ErrorEvent += new MessageEventHandler(mReader_ErrorEvent);
				mMyEMSLDatasetInfo.MessageEvent += new MessageEventHandler(mReader_MessageEvent);
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
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "AScore ParmFileName not defined in the settings for this job; unable to continue");
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
			var fiParamFiles = diParamFileFolder.GetFiles(mParamFilename + "*.xml").ToList();

			if (fiParamFiles.Count == 0)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "No parameter files present");
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
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error copying parameter file: " + ex.Message);
				}
			}

			return true;

		}

		/// <summary>
		/// query DMS to get list of data package jobs to process
		/// </summary>
		/// <param name="dataPackageID"></param>
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
			var ascoreModule = new MageAScore();
			ascoreModule.WarningMessageUpdated += new EventHandler<MageStatusEventArgs>(ascoreModule_WarningMessageUpdated);

			ascoreModule.ExtractionParms = GetExtractionParametersFromJobParameters();
			ascoreModule.WorkingDir = mWorkingDir;
			ascoreModule.ResultsDBFileName = mResultsDBFileName;
			ascoreModule.ascoreParamFileName = mParamFilename;
			ascoreModule.searchType = mSearchType;

			ascoreModule.Initialize(mIonicZipTools);

			ProcessingPipeline.Assemble("Process", jobsToProcess, ascoreModule).RunRoot(null);
		}

		/// <summary>
		/// Import reporter ions into results SQLite database from given list of jobs
		/// </summary>
		/// <param name="reporterIonJobsToProcess"></param>
		private void ImportReporterIons(SimpleSink reporterIonJobsToProcess, string tableName)
		{
			// get selected list of reporter ion files from list of jobs
			const string columnsToIncludeInOutput = "Job, Dataset, Dataset_ID, Tool, Settings_File, Parameter_File, Instrument";
			SimpleSink fileList = GetListOfFilesFromFolderList(reporterIonJobsToProcess, "_ReporterIons.txt", columnsToIncludeInOutput);

			// make module to import contents of each file in list
			var importer = new MageFileImport();
			importer.DBTableName = tableName;
			importer.DBFilePath = Path.Combine(mWorkingDir, mResultsDBFileName);
			importer.ImportColumnList = "Dataset_ID|+|text, *";

			ProcessingPipeline.Assemble("File_Import", fileList, importer).RunRoot(null);
		}

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
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "AScore error: " + e.Message);
		}

		void mReader_MessageEvent(object sender, MyEMSLReader.MessageEventArgs e)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, e.Message);
		}

		void mReader_ErrorEvent(object sender, MyEMSLReader.MessageEventArgs e)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MyEMSL Reader error: " + e.Message);
		}

		#endregion

		// ------------------------------------------------------------------------------
		#region Mage AScore class
		//TODO: I have implemented the mzXML accessor for msgfdb data, now need to add functionality to look for mzxml files to copy filfe.

		/// <summary>
		/// This is a Mage module that does AScore processing 
		/// of results for jobs that are supplied to it via standard tabular input
		/// </summary>
		public class MageAScore : ContentFilter
		{

			#region Member Variables

			protected string[] jobFieldNames;

			// indexes to look up values for some key job fields
			protected int toolIdx;
			protected int paramFileIdx;
			protected int resultsFldrIdx;
			protected int datasetNameIdx;
			protected int settingsFileIdx;

			protected clsIonicZipTools mIonicZipTools;

			#endregion

			#region Properties

			public ExtractionType ExtractionParms { get; set; }
			public string ExtractedResultsFileName { get; set; }
			public string WorkingDir { get; set; }
			public string ResultsDBFileName { get; set; }
			public string searchType { get; set; }
			public string ascoreParamFileName { get; set; }

			#endregion

			#region Constructors

			// constructor
			public MageAScore()
			{
				ExtractedResultsFileName = "extracted_results.txt";
			}

			public void Initialize(clsIonicZipTools ionicZipTools)
			{
				mIonicZipTools = ionicZipTools;
			}

			#endregion

			#region Overrides of Mage ContentFilter

			// set up internal references
			protected override void ColumnDefsFinished()
			{
				// get array of column names
				jobFieldNames = InputColumnDefs.Select(colDef => colDef.Name).ToArray();

				// set up column indexes
				toolIdx = InputColumnPos["Tool"];
				paramFileIdx = InputColumnPos["Parameter_File"];
				resultsFldrIdx = InputColumnPos["Folder"];
				datasetNameIdx = InputColumnPos["Dataset"];
				settingsFileIdx = InputColumnPos["Settings_File"];

			}

			// process the job described by the fields in the input vals object
			protected override bool CheckFilter(ref string[] vals)
			{

				try
				{
					string fragtype = "";
					// extract contents of results file for current job to local file in working directory
					BaseModule currentJob = MakeJobSourceModule(jobFieldNames, vals);
					ExtractResultsForJob(currentJob, ExtractionParms, ExtractedResultsFileName);

					// copy DTA file for current job to working directory
					string resultsFolderPath = vals[resultsFldrIdx];
					string paramFileNameForPSMTool = vals[paramFileIdx];
					string datasetName = vals[datasetNameIdx];

					string dtaFilePath = CopyDTAResults(datasetName, resultsFolderPath);
					if (string.IsNullOrEmpty(dtaFilePath))
					{
						return false;
					}

					string settingsFileName = vals[settingsFileIdx];
					string findFragmentation = (paramFileNameForPSMTool + settingsFileName).ToLower();
					if (findFragmentation.Contains("hcd"))
					{
						fragtype = "hcd";
					}
					else if (findFragmentation.Contains("etd"))
					{
						fragtype = "etd";
					}
					else
					{
						fragtype = "cid";
					}

					// process extracted results file and DTA file with AScore
					const string ascoreOutputFile = "AScoreFile.txt"; // TODO: how do we name it
					string ascoreOutputFilePath = Path.Combine(WorkingDir, ascoreOutputFile);

					// TODO: make the call to AScore
					string fhtFile = Path.Combine(WorkingDir, ExtractedResultsFileName);
					string dtaFile = Path.Combine(WorkingDir, dtaFilePath);
					string paramFileToUse = Path.Combine(WorkingDir, Path.GetFileNameWithoutExtension(ascoreParamFileName) + "_" + fragtype + ".xml");

					if (!File.Exists(paramFileToUse))
					{
						Console.WriteLine("Parameter file not found: " + paramFileToUse);

						string paramFileToUse2 = Path.Combine(WorkingDir, ascoreParamFileName);
						if (Path.GetExtension(paramFileToUse2).Length == 0)
							paramFileToUse2 += ".xml";

						if (File.Exists(paramFileToUse2))
						{
							Console.WriteLine(" ... will instead use: " + paramFileToUse2);
							paramFileToUse = paramFileToUse2;
						}
						else
						{
							return false;
						}
					}

					var paramManager = new ParameterFileManager(paramFileToUse);
					var dtaManager = new DtaManager(dtaFile);
					DatasetManager datasetManager;

					switch (searchType)
					{
						case "xtandem":
							datasetManager = new XTandemFHT(fhtFile);
							break;
						case "sequest":
							datasetManager = new SequestFHT(fhtFile);
							break;
						case "inspect":
							datasetManager = new InspectFHT(fhtFile);
							break;
						case "msgfdb":
						case "msgfplus":
							datasetManager = new MsgfdbFHT(fhtFile);
							break;
						default:
							Console.WriteLine("Incorrect search type check again");
							return false;
					}

					var ascoreAlgorithm = new Algorithm();

					// Attach the eventes
					ascoreAlgorithm.ErrorEvent += new MessageEventBase.MessageEventHandler(ascoreAlgorithm_ErrorEvent);
					ascoreAlgorithm.WarningEvent += new MessageEventBase.MessageEventHandler(ascoreAlgorithm_WarningEvent);
					ascoreAlgorithm.AlgorithmRun(dtaManager, datasetManager, paramManager, ascoreOutputFilePath);
					
					Console.WriteLine();

					// load AScore results into SQLite database
					string tableName = "t_results"; // TODO: how do we name table
					string dbFilePath = Path.Combine(WorkingDir, ResultsDBFileName);
					ImportFileToSQLite(fhtFile, dbFilePath, tableName);
					tableName = "t_results_ascore";
					ImportFileToSQLite(ascoreOutputFilePath, dbFilePath, tableName);

					dtaManager.Abort();
					if (File.Exists(ascoreOutputFilePath))
					{
						try
						{
							clsAnalysisToolRunnerBase.DeleteFileWithRetries(ascoreOutputFilePath, intDebugLevel: 1, MaxRetryCount: 2);
						}
						catch (Exception ex)
						{
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error deleting file " + Path.GetFileName(ascoreOutputFilePath) + "; may lead to duplicate values in Results.db3");
						}
						
					}

					// Delete extracted_results file and DTA file
					if (File.Exists(fhtFile))
					{
						File.Delete(fhtFile);
					}

					if (File.Exists(dtaFilePath))
					{
						File.Delete(dtaFilePath);
					}

					return true;
				}
				catch (Exception ex)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in clsAScoreMage.CheckFilter: " + ex.Message);
					Console.WriteLine(ex.Message);
					return false;
				}

			}

			#endregion

			#region MageAScore Mage Pipelines

			// Build and run Mage pipeline to to extract contents of job
			private void ExtractResultsForJob(BaseModule currentJob, ExtractionType extractionParms, string extractedResultsFileName)
			{
				// search job results folders for list of results files to process and accumulate into buffer module
				var fileList = new SimpleSink();
				ProcessingPipeline plof = ExtractionPipelines.MakePipelineToGetListOfFiles(currentJob, fileList, extractionParms);
				plof.RunRoot(null);

				// add job metadata to results database via a Mage pipeline
				string resultsDBPath = Path.Combine(WorkingDir, ResultsDBFileName);
				var resultsDB = new DestinationType("SQLite_Output", resultsDBPath, "t_results_metadata");
				ExtractionPipelines.MakePipelineToExportJobMetadata(currentJob, resultsDB).RunRoot(null);

				// add file metadata to results database via a Mage pipeline
				resultsDB = new DestinationType("SQLite_Output", resultsDBPath, "t_results_file_list");
				ExtractionPipelines.MakePipelineToExportJobMetadata(new SinkWrapper(fileList), resultsDB).RunRoot(null);

				// extract contents of files
				//DestinationType destination = new DestinationType("SQLite_Output", Path.Combine(mWorkingDir, mResultsDBFileName), "t_results");
				var destination = new DestinationType("File_Output", WorkingDir, extractedResultsFileName);
				ProcessingPipeline pefc = ExtractionPipelines.MakePipelineToExtractFileContents(new SinkWrapper(fileList), extractionParms, destination);
				pefc.RunRoot(null);
			}

			#endregion

			#region MageAScore Utility Methods

			// look for "_dta.zip" file in job results folder and copy it to working directory and unzip it
			private string CopyDTAResults(string datasetName, string resultsFolderPath)
			{
				string dtaZipPathLocal;

				var diResultsFolder = new DirectoryInfo(resultsFolderPath);

				if (resultsFolderPath.StartsWith(clsAnalysisResources.MYEMSL_PATH_FLAG))
				{
					// Need to retrieve the _DTA.zip file from MyEMSL

					dtaZipPathLocal = CopyDtaResultsFromMyEMSL(datasetName, diResultsFolder);				
				}
				else
				{
					dtaZipPathLocal = CopyDTAResultsFromServer(diResultsFolder);					
				}

				// If we have changed the string from empty we have found the correct _dta.zip file
				if (string.IsNullOrEmpty(dtaZipPathLocal))
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DTA File not found");
					return null;
				}

				try
				{
					// Unzip the file
					mIonicZipTools.UnzipFile(dtaZipPathLocal);
				}
				catch (Exception ex)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception copying and unzipping _DTA.zip file: " + ex.Message);
					return null;
				}

				try
				{
					// Perform garage collection to force the Unzip tool to release the file handle
					Thread.Sleep(250);
					clsProgRunner.GarbageCollectNow();

					clsAnalysisToolRunnerBase.DeleteFileWithRetries(dtaZipPathLocal, intDebugLevel: 1, MaxRetryCount: 2);
				}
				catch (Exception ex)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to delete _dta.zip file: " + ex.Message);
				}

				string unzippedDtaResultsFilePath = Path.ChangeExtension(dtaZipPathLocal, ".txt");
				return unzippedDtaResultsFilePath;
			}

			protected string CopyDtaResultsFromMyEMSL(string datasetName, DirectoryInfo diResultsFolder)
			{
				mMyEMSLDatasetInfo.AddDataset(datasetName);
				var lstArchiveFiles = mMyEMSLDatasetInfo.FindFiles("*_dta.zip", diResultsFolder.Name, datasetName);

				if (lstArchiveFiles.Count == 0)
				{
					// Look for the JobParameters file
					lstArchiveFiles = mMyEMSLDatasetInfo.FindFiles("JobParameters_*.xml", diResultsFolder.Name, datasetName);
					if (lstArchiveFiles.Count == 0)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
						                     "JobParameters XML file not found in folder " + diResultsFolder.FullName +
						                     "; unable to determine the DTA folder");
						return null;
					}

					mMyEMSLDatasetInfo.AddFileToDownloadQueue(lstArchiveFiles.First().FileInfo);

					if (!mMyEMSLDatasetInfo.ProcessDownloadQueue(Path.GetTempPath(), Downloader.DownloadFolderLayout.FlatNoSubfolders))
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
						                     "Error downloading the JobParameters XML file from MyEMSL");
						return null;
					}

					string jobParamFile = Path.Combine(Path.GetTempPath(), lstArchiveFiles.First().FileInfo.Filename);

					string dtaFolderName = ReadJobParametersFile(jobParamFile);

					try
					{
						File.Delete(jobParamFile);
					}
						// ReSharper disable once EmptyGeneralCatchClause
					catch
					{
						// Ignore errors here
					}

					if (string.IsNullOrEmpty(dtaFolderName))
					{
						return null;
					}

					mMyEMSLDatasetInfo.ClearDownloadQueue();
					lstArchiveFiles = mMyEMSLDatasetInfo.FindFiles("*_dta.zip", dtaFolderName, datasetName);

					if (lstArchiveFiles.Count == 0)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
						                     "DTA file not found in folder " + dtaFolderName + " in MyEMSL");
						return null;
					}
				}

				mMyEMSLDatasetInfo.AddFileToDownloadQueue(lstArchiveFiles.First().FileInfo);

				if (!mMyEMSLDatasetInfo.ProcessDownloadQueue(WorkingDir, Downloader.DownloadFolderLayout.FlatNoSubfolders))
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
					                     "Error downloading the _DTA.zip file from MyEMSL");
					return null;
				}

				string dtaZipPathLocal = Path.Combine(WorkingDir, lstArchiveFiles.First().FileInfo.Filename);

				return dtaZipPathLocal;
			}


			protected string CopyDTAResultsFromServer(DirectoryInfo diResultsFolder)
			{
				// Check if the dta is in the search tool's directory					
				string dtaZipSourceFilePath;
				
				var lstFiles = diResultsFolder.GetFiles("*_dta.zip").ToList();
				if (lstFiles.Count > 0)
				{
					dtaZipSourceFilePath = lstFiles.First().FullName;
				}
				else
				{
					// File not found
					// Examine the JobParameters file to determine the appropriate dta directory

					lstFiles = diResultsFolder.GetFiles("JobParameters_*.xml").ToList();
					if (lstFiles.Count == 0)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
											 "JobParameters XML file not found in folder " + diResultsFolder.FullName +
											 "; unable to determine the DTA folder");
						return null;
					}

					string jobParamFile = lstFiles.First().FullName;

					string dtaFolderName = ReadJobParametersFile(jobParamFile);

					if (string.IsNullOrEmpty(dtaFolderName))
					{
						// Error has already been logged
						return null;
					}

					if (diResultsFolder.Parent == null || !diResultsFolder.Parent.Exists)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
											 "DTA directory not found; " + diResultsFolder.FullName + " does not have a parent folder");
						return null;
					}

					var diAlternateDtaFolder = new DirectoryInfo(Path.Combine(diResultsFolder.Parent.FullName, dtaFolderName));
					if (!diAlternateDtaFolder.Exists)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
											 "DTA directory not found: " + diAlternateDtaFolder.FullName);
						return null;
					}

					lstFiles = diAlternateDtaFolder.GetFiles("*_dta.zip").ToList();
					if (lstFiles.Count == 0)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
											 "DTA file not found in folder " + diAlternateDtaFolder.FullName);
						return null;
					}

					dtaZipSourceFilePath = lstFiles.First().FullName;
				}

				var fiDtaZipRemote = new FileInfo(dtaZipSourceFilePath);
				string dtaZipPathLocal = Path.Combine(WorkingDir, fiDtaZipRemote.Name);

				// Copy the DTA file locally, overwriting if it already exists
				fiDtaZipRemote.CopyTo(dtaZipPathLocal, true);

				return dtaZipPathLocal;
			}

			private string ReadJobParametersFile(string jobParameterFilePath)
			{
				string dtaFolderName = string.Empty;

				try
				{
					var oXmlDoc = new XmlDocument();
					oXmlDoc.Load(jobParameterFilePath);

					string folderVals = GetIniValue(oXmlDoc, "StepParameters", "SharedResultsFolders");
					List<string> folders = folderVals.Split(',').ToList();
					dtaFolderName = folders.Last(); //this is the default folder if all else fails
					if (folders.Count > 1)
					{
						folders.RemoveAll(entries => entries.Contains("DTA_Gen"));	//I love lambda expressions
						if (folders.Count > 0)
						{
							dtaFolderName = folders.Last();
						}
					}
				}
				catch (Exception ex)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error determining DTA directory from JobParameters XML file by looking for job parameter SharedResultsFolders: " + ex.Message);
				}

				if (string.IsNullOrWhiteSpace(dtaFolderName))
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Unable to determine the DTA directory from JobParameters XML file by looking for job parameter SharedResultsFolders");
					return string.Empty;
				}
				else
					return dtaFolderName.Trim();

			}

			/// <summary>
			/// The function gets the name of the "value" attribute.
			/// </summary>
			/// <param name="sectionName">The name of the section.</param>
			/// <param name="keyName">The name of the key.</param>
			///<return>The function returns the name of the "value" attribute.</return>
			private string GetIniValue(XmlDocument oXmlDoc, string sectionName, string keyName)
			{
				XmlNode N = GetItem(oXmlDoc, sectionName, keyName);
				if (N != null)
				{
					return (N.Attributes.GetNamedItem("value").Value);
				}
				return null;
			}


			/// <summary>
			/// The function gets an item.
			/// </summary>
			/// <param name="sectionName">The name of the section.</param>
			/// <param name="keyName">The name of the key.</param>
			/// <return>The function returns a XML element.</return>
			private XmlElement GetItem(XmlDocument oXmlDoc, string sectionName, string keyName)
			{
				XmlElement section = default(XmlElement);
				if (!string.IsNullOrEmpty(keyName))
				{
					section = GetSection(oXmlDoc, sectionName);
					if (section != null)
					{
						return (XmlElement)section.SelectSingleNode("item[@key='" + keyName + "']");
					}
				}
				return null;
			}

			/// <summary>
			/// The function gets a section as XmlElement.
			/// </summary>
			/// <param name="sectionName">The name of a section.</param>
			/// <return>The function returns a section as XmlElement.</return>
			private XmlElement GetSection(XmlDocument oXmlDoc, string sectionName)
			{
				if (!string.IsNullOrEmpty(sectionName))
				{
					return (XmlElement)oXmlDoc.SelectSingleNode("//section[@name='" + sectionName + "']");
				}
				return null;
			}


			// Build Mage source module containing one job to process
			private BaseModule MakeJobSourceModule(string[] jobFieldNames, string[] jobFields)
			{
				var currentJob = new DataGenerator
				{
					AddAdHocRow = jobFieldNames
				};
				currentJob.AddAdHocRow = jobFields;
				return currentJob;
			}


			#endregion

			#region "Event handlers"

			void ascoreAlgorithm_WarningEvent(object sender, MessageEventArgs e)
			{
				OnWarningMessage(new MageStatusEventArgs(e.Message));
			}

			void ascoreAlgorithm_ErrorEvent(object sender, MessageEventArgs e)
			{
				OnWarningMessage(new MageStatusEventArgs(e.Message));
			}
			#endregion
		}

		#endregion

		// ------------------------------------------------------------------------------
		#region Mage File Import Class

		/// <summary>
		/// Simple Mage FileContentProcessor module
		/// that imports the contents of files that it receives via standard tabular input
		/// to the given SQLite database table
		/// </summary>
		public class MageFileImport : FileContentProcessor
		{

			#region Properties

			public string DBTableName { get; set; }
			public string DBFilePath { get; set; }
			public string ImportColumnList { get; set; }

			#endregion

			#region Constructors

			// constructor
			public MageFileImport()
			{
				base.SourceFolderColumnName = "Folder";
				base.SourceFileColumnName = "Name";
				base.OutputFolderPath = "ignore";
				base.OutputFileName = "ignore";
			}


			#endregion

			#region Overrides of Mage ContentFilter

			// import contents of given file to SQLite database table
			protected override void ProcessFile(string sourceFile, string sourcePath, string destPath, Dictionary<string, string> context)
			{
				if (string.IsNullOrEmpty(ImportColumnList))
				{
					ImportFileToSQLite(sourcePath, DBFilePath, DBTableName);
				}
				else
				{
					ImportFileToSQLiteWithColumnMods(sourcePath, DBFilePath, DBTableName, ImportColumnList, context);
				}
			}

			#endregion
		}

		#endregion

		// ------------------------------------------------------------------------------
		#region Classes for handling parameters

		// class for managing IJobParams object 
		public class JobParameters
		{
			protected IJobParams mJobParms;

			public JobParameters(IJobParams jobParms)
			{
				mJobParms = jobParms;
			}

			public string RequireJobParam(string paramName)
			{
				string val = mJobParms.GetParam(paramName);
				if (string.IsNullOrWhiteSpace(val))
				{
					throw new MageException(string.Format("Required job parameter '{0}' was missing.", paramName));
				}
				return val;
			}

			public string GetJobParam(string paramName)
			{
				return mJobParms.GetParam(paramName);
			}

			public string GetJobParam(string paramName, string defaultValue)
			{
				string val = mJobParms.GetParam(paramName);
				if (string.IsNullOrWhiteSpace(val))
					val = defaultValue;
				return val;
			}
		}

		// class for managing IMgrParams object
		public class ManagerParameters
		{
			protected IMgrParams mMgrParms;

			public ManagerParameters(IMgrParams mgrParms)
			{
				mMgrParms = mgrParms;
			}

			public string RequireMgrParam(string paramName)
			{
				string val = mMgrParms.GetParam(paramName);
				if (string.IsNullOrWhiteSpace(val))
				{
					throw new MageException(string.Format("Required manager parameter '{0}' was missing.", paramName));
				}
				return val;
			}
		}


		#endregion
	}
}
