using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using AnalysisManagerBase;
using Mage;
using MageExtExtractionFilters;
using AScore_DLL.Managers;
using AScore_DLL.Managers.DatasetManagers;
using PRISM;
using Ionic;

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
		protected string mStrExternalUnzipperFilePath = null;
		protected string mSearchType = "";
		protected string mParamFilename = "";

		#endregion

		#region Constructors

		public clsAScoreMage(IJobParams jobParms, IMgrParams mgrParms)
		{
			Intialize(jobParms, mgrParms);
		}

		#endregion

		#region Initialization

		/// <summary>
		/// Set up internal variables
		/// </summary>
		/// <param name="jobParms"></param>
		/// <param name="mgrParms"></param>
		private void Intialize(IJobParams jobParms, IMgrParams mgrParms)
		{
			this.mJP = new JobParameters(jobParms);
			this.mMP = new ManagerParameters(mgrParms);
			this.mResultsDBFileName = mJP.RequireJobParam("ResultsBaseName") + ".db3";
			this.mWorkingDir = mMP.RequireMgrParam("workdir");
			this.mStrExternalUnzipperFilePath = mMP.RequireMgrParam("zipprogram");
			this.mSearchType = mJP.RequireJobParam("AScoreSearchType");
			if (this.mSearchType == "msgfdb")
				this.mSearchType = "msgfplus";
			this.mParamFilename = mJP.GetJobParam("AScoreParamFilename");
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
			}

			string strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "AScore";
			string strMAParameterFileStoragePath = mMP.RequireMgrParam(strParamFileStoragePathKeyName);
			if (string.IsNullOrEmpty(strMAParameterFileStoragePath))
			{
				strMAParameterFileStoragePath = @"\\gigasax\DMS_Parameter_Files\AScore";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter " + strParamFileStoragePathKeyName + " is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " + strMAParameterFileStoragePath);
			}

			IEnumerable<string> ms = Directory.EnumerateFiles(strMAParameterFileStoragePath, mParamFilename + "*.xml");

			//Find all parameter files that match the base name and copy to working directory
			if (ms.Count() == 0)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "No parameter files present");
				return false;
			}
			foreach (string pfile in ms)
			{
				try
				{
					File.Copy(pfile, Path.Combine(mWorkingDir, Path.GetFileName(pfile)));
				}
				catch
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Parameter file already written");
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
			string sqlTemplate = @"SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0} AND Tool LIKE '%{1}%'";
			string connStr = mMP.RequireMgrParam("ConnectionString");
			string sql = string.Format(sqlTemplate, new string[] { dataPackageID, tool });
			SimpleSink jobList = GetListOfItemsFromDB(sql, connStr);
			return jobList;
		}

		/// <summary>
		/// make a Mage pipeline that applies AScore processint to each job in job list
		/// </summary>
		/// <param name="jobsToProcess"></param>
		private void ApplyAScoreToJobs(SimpleSink jobsToProcess)
		{
			MageAScore ascoreModule = new MageAScore();
			ascoreModule.ExtractionParms = GetExtractionParametersFromJobParameters();
			ascoreModule.WorkingDir = mWorkingDir;
			ascoreModule.ResultsDBFileName = mResultsDBFileName;
			ascoreModule.strExternalUnzipperFilePath = mStrExternalUnzipperFilePath;
			ascoreModule.paramFilename = mParamFilename;
			ascoreModule.searchType = mSearchType;
			ProcessingPipeline.Assemble("Process", jobsToProcess, ascoreModule).RunRoot(null);
		}

		/// <summary>
		/// Import reporter ions into results SQLite database from given list of jobs
		/// </summary>
		/// <param name="reporterIonJobsToProcess"></param>
		private void ImportReporterIons(SimpleSink reporterIonJobsToProcess, string tableName)
		{
			// get selected list of reporter ion files from list of jobs
			string columnsToIncludeInOutput = "Job, Dataset, Dataset_ID, Tool, Settings_File, Parameter_File, Instrument";
			SimpleSink fileList = GetListOfFilesFromFolderList(reporterIonJobsToProcess, "_ReporterIons.txt", columnsToIncludeInOutput);

			// make module to import contents of each file in list
			MageFileImport importer = new MageFileImport();
			importer.DBTableName = tableName;
			importer.DBFilePath = Path.Combine(mWorkingDir, mResultsDBFileName);
			importer.ImportColumnList = "Dataset_ID|+|text, *";

			ProcessingPipeline.Assemble("File_Import", fileList, importer).RunRoot(null);
		}

		/// <summary>
		/// Get a list of items using a database query
		/// </summary>
		/// <param name="sql">Query to use a source of jobs</param>
		/// <returns>A Mage module containing list of jobs</returns>
		public static SimpleSink GetListOfItemsFromDB(string sql, string connectionString)
		{
			SimpleSink itemList = new SimpleSink();
			MSSQLReader reader = MakeDBReaderModule(sql, connectionString);
			ProcessingPipeline pipeline = ProcessingPipeline.Assemble("Get Items", reader, itemList);
			pipeline.RunRoot(null);
			return itemList;
		}

		/// <summary>
		/// Create a new MSSQLReader module to do a specific query
		/// </summary>
		/// <param name="sql">Query to use</param>
		/// <returns></returns>
		public static MSSQLReader MakeDBReaderModule(String sql, string connectionString)
		{
			MSSQLReader reader = new MSSQLReader();
			reader.ConnectionString = connectionString;
			reader.SQLText = sql;
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
			SimpleSink sinkObject = new SimpleSink();

			// create file filter module and initialize it
			FileListFilter fileFilter = new FileListFilter();
			fileFilter.FileNameSelector = fileNameSelector;
			fileFilter.SourceFolderColumnName = "Folder";
			fileFilter.FileColumnName = "Name";
			fileFilter.OutputColumnList = "Item|+|text, Name|+|text, File_Size_KB|+|text, Folder, " + passThroughColumns;
			fileFilter.FileSelectorMode = "RegEx";
			fileFilter.IncludeFilesOrFolders = "File";
			fileFilter.RecursiveSearch = "No";
			fileFilter.SubfolderSearchName = "*";

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
			DelimitedFileReader reader = new DelimitedFileReader();
			reader.FilePath = inputFilePath;

			SQLiteWriter writer = new SQLiteWriter();
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
			DelimitedFileReader reader = new DelimitedFileReader();
			reader.FilePath = inputFilePath;

			BaseModule filter = new NullFilter();
			filter.OutputColumnList = outputColumnList;
			filter.SetContext(context);

			SQLiteWriter writer = new SQLiteWriter();
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
			ExtractionType extractionParms = new ExtractionType();

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

		// ------------------------------------------------------------------------------
		#region Mage AScore class
		//TODO: I have implemented the mzXML accessor for msgfdb data, now need to add functionality
		//to look for mzxml files to copy filfe.


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
			protected int settingsFileIdx;

			#endregion

			#region Properties

			public ExtractionType ExtractionParms { get; set; }
			public string ExtractedResultsFileName { get; set; }
			public string WorkingDir { get; set; }
			public string ResultsDBFileName { get; set; }
			public string strExternalUnzipperFilePath { get; set; }
			public string searchType { get; set; }
			public string paramFilename { get; set; }

			#endregion

			#region Constructors

			// constructor
			public MageAScore()
			{
				ExtractedResultsFileName = "extracted_results.txt";
			}

			#endregion

			#region Overrides of Mage ContentFilter

			// set up internal references
			protected override void ColumnDefsFinished()
			{
				// get array of column names
				List<string> cols = new List<string>();
				foreach (MageColumnDef colDef in this.InputColumnDefs)
				{
					cols.Add(colDef.Name);
				}
				jobFieldNames = cols.ToArray();

				// set up column indexes
				toolIdx = InputColumnPos["Tool"];
				paramFileIdx = InputColumnPos["Parameter_File"];
				resultsFldrIdx = InputColumnPos["Folder"];
				settingsFileIdx = InputColumnPos["Settings_File"];

			}

			// process the job described by the fields in the input vals object
			protected override bool CheckFilter(ref object[] vals)
			{

				try
				{
					string fragtype = "";
					// extract contents of results file for current job to local file in working directory
					BaseModule currentJob = MakeJobSourceModule(jobFieldNames, vals);
					ExtractResultsForJob(currentJob, ExtractionParms, ExtractedResultsFileName);

					// copy DTA file for current job to working directory
					string resultsFolderPath = vals[resultsFldrIdx].ToString();
					string paramFileName = vals[paramFileIdx].ToString();
					string dtaFilePath;
					if ((dtaFilePath = CopyDTAResults(resultsFolderPath)) == null)
					{
						return false;
					}

					string settingsFileName = vals[settingsFileIdx].ToString();
					string findFragmentation = (paramFileName + settingsFileName).ToLower();
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
					string ascoreOutputFile = "AScoreFile.txt"; // TODO: how do we name it
					string ascoreOutputFilePath = Path.Combine(WorkingDir, ascoreOutputFile);

					// TODO: make the call to AScore
					string fhtFile = Path.Combine(WorkingDir, ExtractedResultsFileName);
					string dtaFile = Path.Combine(WorkingDir, dtaFilePath);
					string paramFile = Path.Combine(WorkingDir, Path.GetFileNameWithoutExtension(paramFilename) + "_" + fragtype + ".xml"); //paramFileName);

					if (!File.Exists(paramFile))
					{
						Console.WriteLine("This type of parameter file does not exist");
						return false;
					}

					ParameterFileManager paramManager = new ParameterFileManager(paramFile);
					DtaManager dtaManager = new DtaManager(dtaFile);
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

					AScore_DLL.Algorithm.AlgorithmRun(dtaManager, datasetManager, paramManager, ascoreOutputFilePath);

					// load AScore results into SQLite database
					string tableName = "t_results"; // TODO: how do we name table
					string dbFilePath = Path.Combine(WorkingDir, ResultsDBFileName);
					clsAScoreMage.ImportFileToSQLite(fhtFile, dbFilePath, tableName);
					tableName = "t_results_ascore";
					clsAScoreMage.ImportFileToSQLite(ascoreOutputFilePath, dbFilePath, tableName);

					dtaManager.Abort();
					if (System.IO.File.Exists(ascoreOutputFilePath))
					{
						File.Delete(ascoreOutputFilePath);
					}
					// Delete extracted_results file and DTA file
					if (System.IO.File.Exists(fhtFile))
						File.Delete(fhtFile);
					if (System.IO.File.Exists(dtaFilePath))
					{
						File.Delete(dtaFilePath);
					}
					// optionally delete AScore results file
					// TODO: do the deletions


					return true;
				}
				catch (Exception e)
				{
					Console.WriteLine(e.Message);
					return false;
				}

			}


			#endregion

			#region MageAScore Mage Pipelines

			// Build and run Mage pipeline to to extract contents of job
			private void ExtractResultsForJob(BaseModule currentJob, ExtractionType extractionParms, string extractedResultsFileName)
			{
				// search job results folders for list of results files to process and accumulate into buffer module
				SimpleSink fileList = new SimpleSink();
				ProcessingPipeline plof = ExtractionPipelines.MakePipelineToGetListOfFiles(currentJob, fileList, extractionParms);
				plof.RunRoot(null);

				// add job metadata to results database via a Mage pipeline
				DestinationType resultsDB;
				string resultsDBPath = Path.Combine(WorkingDir, ResultsDBFileName);
				resultsDB = new DestinationType("SQLite_Output", resultsDBPath, "t_results_metadata");
				ExtractionPipelines.MakePipelineToExportJobMetadata(currentJob, resultsDB).RunRoot(null);

				// add file metadata to results database via a Mage pipeline
				resultsDB = new DestinationType("SQLite_Output", resultsDBPath, "t_results_file_list");
				ExtractionPipelines.MakePipelineToExportJobMetadata(new SinkWrapper(fileList), resultsDB).RunRoot(null);

				// extract contents of files
				//DestinationType destination = new DestinationType("SQLite_Output", Path.Combine(mWorkingDir, mResultsDBFileName), "t_results");
				DestinationType destination = new DestinationType("File_Output", WorkingDir, extractedResultsFileName);
				ProcessingPipeline pefc = ExtractionPipelines.MakePipelineToExtractFileContents(new SinkWrapper(fileList), extractionParms, destination);
				pefc.RunRoot(null);
			}

			#endregion

			#region MageAScore Utility Methods

			// look for "_dta.zip" file in job results folder and copy it to working directory and unzip it
			private string CopyDTAResults(string resultsFolderPath)
			{
				string dtaResultsFilename = ""; //"dta_results.zip";
				string zippedDTAResultsFilePath = ""; // Path.Combine(WorkingDir, dtaResultsFilename);
				string unzippedDTAResultsFileName = ""; // "dta_results.txt";
				string unzippedDTAResultsFilePath = Path.Combine(WorkingDir, unzippedDTAResultsFileName);
				string[] files = Directory.GetFiles(resultsFolderPath, "*_dta.zip");
				if (files.Length > 0) //check if the dta is in the search tool's directory
				{
					dtaResultsFilename = Path.GetFileName(files[0]);
				}
				else
				{
					string jobPFile = string.Empty;
					files = null;
					files = Directory.GetFiles(resultsFolderPath, "JobParameters_*.xml");//find the appropriate dta directory
					if (files.Length > 0)
					{
						jobPFile = System.IO.Path.GetFullPath(files[0]);
					}
					string DTAfolderName = null;
					ReadJobParametersFile(jobPFile, ref DTAfolderName);
					if (DTAfolderName != null)
					{
						files = null;

						string DTAdirectory = Path.Combine(Directory.GetParent(resultsFolderPath).FullName, DTAfolderName);
						if (Directory.Exists(DTAdirectory))
						{
							files = Directory.GetFiles(DTAdirectory, "*_dta.zip");
							if (files.Length > 0)
							{
								dtaResultsFilename = Path.GetFileName(files[0]);
							}
						}
					}
				}
				if (dtaResultsFilename == string.Empty)//if we have changed the string from empty we have found the correct dta
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "DTA File not found");
					return null;
				}

				zippedDTAResultsFilePath = Path.Combine(WorkingDir, dtaResultsFilename);
				unzippedDTAResultsFilePath = Path.Combine(WorkingDir, dtaResultsFilename.Replace(".zip", ".txt"));
				if (File.Exists(files[0]))
				{
					try
					{
						// Copy the DTA file locally, overwriting if it already exists
						// Unzip the file
						File.Copy(files[0], zippedDTAResultsFilePath, true);
						if (UnzipFileStart(zippedDTAResultsFilePath, WorkingDir, "clsAnalysisResources.RetrieveDtaFiles", false))
						{
						}
					}
					catch (Exception ex)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception copying and unzipping _DTA.zip file: " + ex.Message);
						return null;
					}

					try
					{
						// Delay 1 seconds then perform garage collection to force the Unzip tool to release the file handle
						System.Threading.Thread.Sleep(2000);
						PRISM.Processes.clsProgRunner.GarbageCollectNow();

						clsAnalysisToolRunnerBase.DeleteFileWithRetries(zippedDTAResultsFilePath, intDebugLevel:1, MaxRetryCount:2);
					}
					catch (Exception ex)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to delete _dta.zip file: " + ex.Message);
					}

				}
				return unzippedDTAResultsFilePath;
			}


			private void ReadJobParametersFile(string JobParameterFilePath, ref string DTAGenFolderName)
			{
				System.Xml.XmlDocument oXmlDoc = default(System.Xml.XmlDocument);

				try
				{
					oXmlDoc = new System.Xml.XmlDocument();
					oXmlDoc.Load(JobParameterFilePath);

					string folderVals = GetIniValue(oXmlDoc, "StepParameters", "SharedResultsFolders");
					List<string> folders = folderVals.Split(',').ToList();
					DTAGenFolderName = folders[folders.Count - 1]; //this is the default folder if all else fails
					if (folders.Count > 1)
					{
						folders.RemoveAll(entries => entries.Contains("DTA_Gen"));//I love lambda expressions
						if (folders.Count == 1)
						{
							DTAGenFolderName = folders[0];
						}
						else if (folders.Count > 1)
						{
							DTAGenFolderName = folders[folders.Count - 1];
						}
					}
				}
				catch (Exception ex)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error determining DTA directory via job parameter SharedResultsFolders: " + ex.Message);
				}

				DTAGenFolderName = DTAGenFolderName.Trim();
			}

			/// <summary>
			/// The function gets the name of the "value" attribute.
			/// </summary>
			/// <param name="sectionName">The name of the section.</param>
			/// <param name="keyName">The name of the key.</param>
			///<return>The function returns the name of the "value" attribute.</return>
			private string GetIniValue(System.Xml.XmlDocument oXmlDoc, string sectionName, string keyName)
			{
				System.Xml.XmlNode N = GetItem(oXmlDoc, sectionName, keyName);
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
			private System.Xml.XmlElement GetItem(System.Xml.XmlDocument oXmlDoc, string sectionName, string keyName)
			{
				System.Xml.XmlElement section = default(System.Xml.XmlElement);
				if (!string.IsNullOrEmpty(keyName))
				{
					section = GetSection(oXmlDoc, sectionName);
					if (section != null)
					{
						return (System.Xml.XmlElement)section.SelectSingleNode("item[@key='" + keyName + "']");
					}
				}
				return null;
			}

			/// <summary>
			/// The function gets a section as XmlElement.
			/// </summary>
			/// <param name="sectionName">The name of a section.</param>
			/// <return>The function returns a section as XmlElement.</return>
			private System.Xml.XmlElement GetSection(System.Xml.XmlDocument oXmlDoc, string sectionName)
			{
				if (!string.IsNullOrEmpty(sectionName))
				{
					return (System.Xml.XmlElement)oXmlDoc.SelectSingleNode("//section[@name='" + sectionName + "']");
				}
				return null;
			}




			// Build Mage source module containing one job to process
			private BaseModule MakeJobSourceModule(string[] jobFieldNames, object[] jobFields)
			{
				DataGenerator currentJob = new DataGenerator();
				currentJob.AddAdHocRow = jobFieldNames;
				currentJob.AddAdHocRow = ConvertObjectArrayToStringArray(jobFields);
				return currentJob;
			}

			// Convert array of objects to array of strings
			private static string[] ConvertObjectArrayToStringArray(object[] row)
			{
				List<string> obj = new List<string>();
				foreach (object fld in row)
				{
					obj.Add(fld.ToString());
				}
				return obj.ToArray();
			}


			private bool UnzipFileStart(string ZipFilePath, string OutFolderPath, string CallingFunctionName, bool ForceExternalZipProgramUse)
			{

				System.IO.FileInfo fiFileInfo = null;
				float sngFileSizeMB = 0;

				bool blnUseExternalUnzipper = false;
				bool blnSuccess = false;

				string strUnzipperName = string.Empty;

				DateTime dtStartTime = default(DateTime);
				DateTime dtEndTime = default(DateTime);
				m_IonicZipTools = new clsIonicZipTools(1, WorkingDir);

				try
				{
					if (ZipFilePath == null)
						ZipFilePath = string.Empty;

					if (strExternalUnzipperFilePath == null)
						strExternalUnzipperFilePath = string.Empty;

					fiFileInfo = new System.IO.FileInfo(ZipFilePath);
					sngFileSizeMB = Convert.ToSingle(fiFileInfo.Length / 1024.0 / 1024);

					// Use the external zipper if the file size is over IONIC_ZIP_MAX_FILESIZE_MB or if ForceExternalZipProgramUse = True
					// However, if the .Exe file for the external zipper is not found, then fall back to use Ionic.Zip
					if (ForceExternalZipProgramUse || sngFileSizeMB >= IONIC_ZIP_MAX_FILESIZE_MB)
					{
						if (strExternalUnzipperFilePath.Length > 0 && strExternalUnzipperFilePath.ToLower() != "na")
						{
							if (System.IO.File.Exists(strExternalUnzipperFilePath))
							{
								blnUseExternalUnzipper = true;
							}
						}

						if (!blnUseExternalUnzipper)
						{
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "External zip program not found: " + strExternalUnzipperFilePath + "; will instead use Ionic.Zip");
						}
					}

					if (blnUseExternalUnzipper)
					{
						strUnzipperName = System.IO.Path.GetFileName(strExternalUnzipperFilePath);

						PRISM.Files.ZipTools UnZipper = new PRISM.Files.ZipTools(OutFolderPath, strExternalUnzipperFilePath);

						dtStartTime = DateTime.UtcNow;
						blnSuccess = UnZipper.UnzipFile("", ZipFilePath, OutFolderPath);
						dtEndTime = DateTime.UtcNow;

						if (blnSuccess)
						{
							m_IonicZipTools.ReportZipStats(fiFileInfo, dtStartTime, dtEndTime, false, strUnzipperName);
						}
						else
						{
							mMessage = "Error unzipping " + System.IO.Path.GetFileName(ZipFilePath) + " using " + strUnzipperName;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, CallingFunctionName + ": " + mMessage);
							UnZipper = null;
						}
					}
					else
					{
						// Use Ionic.Zip
						strUnzipperName = clsIonicZipTools.IONIC_ZIP_NAME;
						blnSuccess = m_IonicZipTools.UnzipFile(ZipFilePath, OutFolderPath);
					}

				}
				catch (Exception ex)
				{
					mMessage = "Exception while unzipping '" + ZipFilePath + "'";
					if (!string.IsNullOrEmpty(strUnzipperName))
						mMessage += " using " + strUnzipperName;

					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMessage + ": " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex));
					blnSuccess = false;
				}

				return blnSuccess;

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
					clsAScoreMage.ImportFileToSQLite(sourcePath, DBFilePath, DBTableName);
				}
				else
				{
					clsAScoreMage.ImportFileToSQLiteWithColumnMods(sourcePath, DBFilePath, DBTableName, this.ImportColumnList, context);
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
