using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using AnalysisManager_RepoPkgr_PlugIn;
using AnalysisManagerBase;
using AnalysisManagerMsXmlGenPlugIn;

namespace AnalysisManager_RepoPkgr_Plugin
{
	//*********************************************************************************************************
	//Class for running RepoPkgr
	//*********************************************************************************************************
	public class clsAnalysisToolRunnerRepoPkgr : clsAnalysisToolRunnerBase
	{
		#region Constants

		protected const int PROGRESS_PCT_FASTA_FILES_COPIED = 10;
		protected const int PROGRESS_PCT_SEQUEST_RESULTS_COPIED = 15;
		protected const int PROGRESS_PCT_MSGF_PLUS_RESULTS_COPIED = 25;
		protected const int PROGRESS_PCT_MZID_RESULTS_COPIED = 50;
		protected const int PROGRESS_PCT_INSTRUMENT_DATA_COPIED = 95;

		#endregion

		#region Fields

		private bool _bIncludeInstrumentData;
		private bool _bIncludeSequestResults;
		private bool _bIncludeMzXMLFiles;
		private bool _bIncludeMSGFPlusResults;
		private bool _bIncludeMZidFiles;

		private string _outputResultsFolderPath;
		private MageRepoPkgrPipelines _mgr;

		private string _MSXmlGeneratorAppPath;

		#endregion

		#region Main Logic

		public override IJobParams.CloseOutType RunTool()
		{
			try
			{
				//Do the base class stuff
				var result = base.RunTool();
				if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
				{
					return result;
				}

				// Store the RepoPkgr version info in the database
				if (!StoreToolVersionInfo())
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
					m_message = "Error determining RepoPkgr version";
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

				result = BuildRepoCache();
				return result;
			}
			catch (Exception ex)
			{
				m_message = "Error in RepoPkgr Plugin->RunTool";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
				return IJobParams.CloseOutType.CLOSEOUT_FAILED;
			}
		}

		/// <summary>
		/// Find (or generate) necessary files and copy them to repository cache folder for upload
		/// </summary>
		/// <returns></returns>
		private IJobParams.CloseOutType BuildRepoCache()
		{
			SetOptions();
			SetOutputFolderPath();
			SetMagePipelineManager(_outputResultsFolderPath);

			// do operations for repository specified in job parameters
			var targetRepository = m_jobParams.GetJobParameter("Repository", "");
			bool success = false;
			switch (targetRepository)
			{
				case "PeptideAtlas":
					success = DoPeptideAtlasOperation();
					break;
				// FUTURE: code for other repositories to go here someday
			}

			if (success)
				return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
			else
				return IJobParams.CloseOutType.CLOSEOUT_FAILED;
		}

		private void CopyFastaFiles()
		{
			var localOrgDBFolder = m_mgrParams.GetParam("orgdbdir");
			var targetFolderPath = Path.Combine(_outputResultsFolderPath, "Organism_Database");

			var lstGeneratedOrgDBNames = ExtractPackedJobParameterList(clsAnalysisResourcesRepoPkgr.FASTA_FILES_FOR_DATA_PACKAGE);

			foreach (var orgDbName in lstGeneratedOrgDBNames)
			{
				var sourceFilePath = Path.Combine(localOrgDBFolder, orgDbName);
				var destFilePath = Path.Combine(targetFolderPath, orgDbName);
				m_FileTools.CopyFile(sourceFilePath, destFilePath, true);
			}
		}

		/// <summary>
		/// Gather files for submission to PeptideAtlas repository
		/// and copy them to repo cache
		/// </summary>
		private bool DoPeptideAtlasOperation()
		{
			// Copy *.fasta files from organism db to appropriate cache subfolder
			// Files to copy are stored in job parameters named "Job123456_GeneratedFasta"
			CopyFastaFiles();
			m_progress = PROGRESS_PCT_FASTA_FILES_COPIED;
			m_StatusTools.UpdateAndWrite(m_progress);

			if (_bIncludeSequestResults)
			{
				// find any sequest jobs in data package and copy their first hits files to appropriate cache subfolder
				_mgr.GetItemsToRepoPkg("DataPkgJobsQueryTemplate", "SEQUEST", "*_fht.txt", "SEQUEST_Results", "Job");
			}
			m_progress = PROGRESS_PCT_SEQUEST_RESULTS_COPIED;
			m_StatusTools.UpdateAndWrite(m_progress);

			if (_bIncludeMSGFPlusResults)
			{
				// find any MSGFPlus jobs in data package and copy their first hits files to appropriate cache subfolder
				_mgr.GetItemsToRepoPkg("DataPkgJobsQueryTemplate", "MSGFPlus", "*_msgfdb_fht.txt;*_msgfdb_fht_MSGF.txt", "MSGFPlus_Results", "Job");
			}
			m_progress = PROGRESS_PCT_MSGF_PLUS_RESULTS_COPIED;
			m_StatusTools.UpdateAndWrite(m_progress);

			if (_bIncludeMZidFiles)
			{
				// find any MSGFPlus jobs in data package and copy their MZID files to appropriate cache subfolder
				_mgr.GetItemsToRepoPkg("DataPkgJobsQueryTemplate", "MSGFPlus", "*_msgfplus.zip;*_msgfplus.mzid.gz", @"MSGFPlus_Results\MZID_Files", "Job");
				FileUtils.ConvertZipsToGZips(Path.Combine(_outputResultsFolderPath, @"MSGFPlus_Results\MZID_Files"), m_WorkDir);
			}
			m_progress = PROGRESS_PCT_MZID_RESULTS_COPIED;
			m_StatusTools.UpdateAndWrite(m_progress);

			// [Obsolete]
			// Mage-specific method of retrieving .raw and .mzXML
			// Doesn't support .mzXML file caching
			//
			//if (_bIncludeInstrumentData || _bIncludeMzXMLFiles) {
			//    // find any datasets in data package and copy their raw data files to appropriate cache subfolder
			//    _mgr.GetItemsToRepoPkg("DataPkgDatasetsQueryTemplate", "", "*.raw", "Instrument_Data", "");
			//}
			//
			//if (_bIncludeMzXMLFiles) {
			//    // generate mzXML files from raw data files in cache subfolder
			//    var instrumentDataFolderPath = Path.Combine(_outputResultsFolderPath, "Instrument_Data");
			//    GenerateMzXMLFilesFromDataFiles(instrumentDataFolderPath);
			//    //
			//    if (!_bIncludeInstrumentData) {
			//        // delete raw data files if they are not part of final repo cache
			//        FileUtils.DeleteFiles(instrumentDataFolderPath, "*.raw");
			//    }
			//}

			var success = RetrieveInstrumentData();
			if (!success)
				return false;

			m_progress = PROGRESS_PCT_INSTRUMENT_DATA_COPIED;
			m_StatusTools.UpdateAndWrite(m_progress);

			// todo Do some logging on the above pipeline runs using pipeline intermediate results (_mgr.DataPackageItems; _mgr.AssociatedFiles; _mgr.ManifestForCopy;)?

			return true;
		}

		/// <summary>
		/// SetCnStr the report option flags from job parameters
		/// </summary>
		private void SetOptions()
		{
			// New parameters:
			_bIncludeMSGFPlusResults = m_jobParams.GetJobParameter("IncludeMSGFResults", true);
			_bIncludeMZidFiles = _bIncludeMSGFPlusResults;
			_bIncludeSequestResults = m_jobParams.GetJobParameter("IncludeSequestResults", false);
			_bIncludeInstrumentData = m_jobParams.GetJobParameter("IncludeInstrumentData", true);
			_bIncludeMzXMLFiles = m_jobParams.GetJobParameter("IncludeMzXMLFiles", true);

		}

		/// <summary>
		/// SetCnStr the path for the repo cache folder
		/// </summary>
		private void SetOutputFolderPath()
		{
			var resultsFolderName = m_jobParams.GetJobParameter("OutputFolderName", "");
			var outputRootFolderPath = m_jobParams.GetJobParameter("CacheFolderPath", "");
			_outputResultsFolderPath = Path.Combine(outputRootFolderPath, resultsFolderName);
		}

		/// <summary>
		/// Generate handler that provides pre-packaged Mage pipelines
		/// that do the heavy lifting tasks that get data package items,
		/// find associated files, and copy them to repo cache folders    
		/// </summary>
		/// <returns>Pipeline handler objet</returns>
		private void SetMagePipelineManager(string outputFolderPath = "")
		{
			var qd = new QueryDefinitions();
			qd.SetCnStr(QueryDefinitions.TagName.Main, m_mgrParams.GetParam("connectionstring"));
			qd.SetCnStr(QueryDefinitions.TagName.Broker, m_mgrParams.GetParam("brokerconnectionstring"));
			_mgr = new MageRepoPkgrPipelines
			{
				QueryDefs = qd,
				DataPkgId = m_jobParams.GetJobParameter("DataPackageID", "")
			};
			if (!string.IsNullOrEmpty(outputFolderPath))
			{
				_mgr.OutputResultsFolderPath = outputFolderPath;
			}
		}

		///// <summary>
		///// for each raw data file in the given repo cache folder
		///// generate a corresponding mzXML file
		///// </summary>
		///// <param name="instrumentDataFolderPath"></param>
		/// [Obsolete("Superseded by RetrieveInstrumentData and ProcessDataset")]
		//private void GenerateMzXMLFilesFromDataFiles(string instrumentDataFolderPath)
		//{
		//    // under construction
		//    m_jobParams.AddAdditionalParameter("JobParameters", "RawDataType", "dot_raw_files"); // use packed list from resourcer?

		//    var dir = new DirectoryInfo(instrumentDataFolderPath);
		//    foreach (var fi in dir.GetFiles("*.raw"))
		//    {
		//        var dataset = Path.GetFileNameWithoutExtension(fi.Name);
		//        GenerateMzXMLFile(dataset, instrumentDataFolderPath);
		//    }
		//}


		/// <summary>
		/// Retrieves or creates the .MzXML file for this dataset
		/// </summary>
		/// <param name="objMSXmlCreator">MzXML Creator</param>
		/// <param name="datasetName">Dataset name</param>
		/// <param name="objAnalysisResults">Analysis Results class</param>
		/// <param name="dctDatasetRawFilePaths">Dictionary with dataset names and dataset raw file paths</param>
		/// <param name="dctDatasetYearQuarter">Dictionary with dataset names and year/quarter information</param>
		/// <param name="dctDatasetRawDataTypes">Dictionary with dataset names and the raw data type of the instrument data file</param>
		/// <param name="strDatasetFilePathLocal">Output parameter: Path to the locally cached dataset file</param>
		/// <returns>The full path to the locally created MzXML file</returns>
		protected string CreateMzXMLFileIfMissing(
			clsMSXMLCreator objMSXmlCreator,
			string datasetName,
			clsAnalysisResults objAnalysisResults,
			Dictionary<string, string> dctDatasetRawFilePaths,
			Dictionary<string, string> dctDatasetYearQuarter,
			Dictionary<string, string> dctDatasetRawDataTypes,
		  out string strDatasetFilePathLocal)
		{
			string strDestPath = string.Empty;
			strDatasetFilePathLocal = string.Empty;

			try
			{

				// Look in m_WorkDir for the .mzXML file for this dataset
				var fiMzXmlFilePathLocal = new FileInfo(Path.Combine(m_WorkDir, datasetName + clsAnalysisResources.DOT_MZXML_EXTENSION));

				if (fiMzXmlFilePathLocal.Exists)
				{
					return fiMzXmlFilePathLocal.FullName;
				}

				// .mzXML file not found
				// Look for a StoragePathInfo file
				string strMzXmlStoragePathFile = fiMzXmlFilePathLocal.FullName + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX;

				bool blnSuccess;
				if (File.Exists(strMzXmlStoragePathFile))
				{
					blnSuccess = RetrieveStoragePathInfoTargetFile(strMzXmlStoragePathFile, objAnalysisResults, ref strDestPath);
					if (blnSuccess)
					{
						return strDestPath;
					}
				}

				// Need to create the .mzXML file
				if (!dctDatasetRawFilePaths.ContainsKey(datasetName))
				{
					m_message = "Dataset " + datasetName + " not found in job parameter " + clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS + "; unable to create the missing .mzXML file";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
					return string.Empty;
				}

				m_jobParams.AddResultFileToSkip("MSConvert_ConsoleOutput.txt");

				objMSXmlCreator.UpdateDatasetName(datasetName);

				// Make sure the dataset file is present in the working directory
				// Copy it locally if necessary

				var strDatasetFilePathRemote = dctDatasetRawFilePaths[datasetName];
				var blnDatasetFileIsAFolder = Directory.Exists(strDatasetFilePathRemote);

				// ReSharper disable once AssignNullToNotNullAttribute
				strDatasetFilePathLocal = Path.Combine(m_WorkDir, Path.GetFileName(strDatasetFilePathRemote));

				if (blnDatasetFileIsAFolder)
				{
					// Confirm that the dataset folder exists in the working directory

					if (!Directory.Exists(strDatasetFilePathLocal))
					{
						// Copy the dataset folder locally
						objAnalysisResults.CopyDirectory(strDatasetFilePathRemote, strDatasetFilePathLocal, Overwrite: true);
					}

				}
				else
				{
					// Confirm that the dataset file exists in the working directory
					if (!File.Exists(strDatasetFilePathLocal))
					{
						// Copy the dataset file locally
						objAnalysisResults.CopyFileWithRetry(strDatasetFilePathRemote, strDatasetFilePathLocal, Overwrite: true);
					}
				}

				string rawDataType;
				if (!dctDatasetRawDataTypes.TryGetValue(datasetName, out rawDataType))
					rawDataType = "dot_raw_files";
				
				m_jobParams.AddAdditionalParameter("JobParameters", "RawDataType", rawDataType); 

				blnSuccess = objMSXmlCreator.CreateMZXMLFile();

				if (!blnSuccess && string.IsNullOrEmpty(m_message))
				{
					m_message = objMSXmlCreator.ErrorMessage;
					if (string.IsNullOrEmpty(m_message))
					{
						m_message = "Unknown error creating the mzXML file for dataset " + datasetName;
					}
					else if (!m_message.Contains(datasetName))
					{
						m_message += "; dataset " + datasetName;
					}
				}

				if (!blnSuccess)
					return string.Empty;

				fiMzXmlFilePathLocal.Refresh();
				if (!fiMzXmlFilePathLocal.Exists)
				{
					m_message = "MSXmlCreator did not create the .mzXML file for dataset " + datasetName;
					return string.Empty;
				}

				// Copy the .mzXML file to the cache

				string strMSXmlGeneratorName = Path.GetFileNameWithoutExtension(_MSXmlGeneratorAppPath);
				string strDatasetYearQuarter;
				if (!dctDatasetYearQuarter.TryGetValue(datasetName, out strDatasetYearQuarter))
				{
					strDatasetYearQuarter = string.Empty;
				}

				CopyMzXMLFileToServerCache(fiMzXmlFilePathLocal.FullName, strDatasetYearQuarter, strMSXmlGeneratorName, blnPurgeOldFilesIfNeeded: true);

				m_jobParams.AddResultFileToSkip(Path.GetFileName(fiMzXmlFilePathLocal.FullName + clsGlobal.SERVER_CACHE_HASHCHECK_FILE_SUFFIX));

				Thread.Sleep(250);
				PRISM.Processes.clsProgRunner.GarbageCollectNow();

				return fiMzXmlFilePathLocal.FullName;
			}
			catch (Exception ex)
			{
				m_message = "Exception in CreateMzXMLFileIfMissing";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
				return string.Empty;
			}

		}

		private void DeleteFileIgnoreErrors(string filePath)
		{
			try
			{
				File.Delete(filePath);
			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Unable to delete file " + filePath + ": " + ex.Message);
			}
		}


		protected Dictionary<string, string> ExtractPackedJobParameterDictionary(string strPackedJobParameterName)
		{
			var dctData = new Dictionary<string, string>();
			List<string> lstData = ExtractPackedJobParameterList(strPackedJobParameterName);

			foreach (var item in lstData)
			{

				var intEqualsIndex = item.LastIndexOf('=');
				if (intEqualsIndex > 0)
				{
					string strKey = item.Substring(0, intEqualsIndex);
					string strValue = item.Substring(intEqualsIndex + 1);

					if (!dctData.ContainsKey(strKey))
					{
						dctData.Add(strKey, strValue);
					}
				}
				else
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Packed dictionary item does not contain an equals sign: " + item);
				}
			}

			return dctData;

		}

		protected List<string> ExtractPackedJobParameterList(string strParameterName)
		{

			string strList = null;

			strList = m_jobParams.GetJobParameter(strParameterName, string.Empty);

			if (string.IsNullOrEmpty(strList))
			{
				return new List<string>();
			}
			else
			{
				return strList.Split('\t').ToList();
			}

		}

		/// <summary>
		/// Produce an mzXML file from dataset raw data file.
		/// Function expects raw data file to exist in work directory,
		/// and will produce the mzXML file in the same directory
		/// </summary>
		/// <param name="dataset">Dataset to generate the mzXML for</param>
		/// <param name="workingDir">Directory containing the raw data files</param>
		/// <returns></returns>
		[Obsolete("Superseded by RetrieveInstrumentData and ProcessDataset")]
		private void GenerateMzXMLFile(string dataset, string workingDir)
		{
			// todo Someday use mzXML cache?
			var mMSXmlGeneratorAppPath = GetMSXmlGeneratorAppPath();
			var mMsXmlCreator = new clsMSXMLCreator(mMSXmlGeneratorAppPath, workingDir, dataset, m_DebugLevel, m_jobParams);
			var blnSuccess = mMsXmlCreator.CreateMZXMLFile();

			if (!blnSuccess && string.IsNullOrEmpty(m_message))
			{
				m_message = mMsXmlCreator.ErrorMessage;
				if (string.IsNullOrEmpty(m_message))
				{
					m_message = "Unknown error creating the mzXML file for dataset " + dataset;
				}
				else if (!m_message.Contains(dataset))
				{
					m_message += "; dataset " + dataset;
				}
			}
		}


		protected bool ProcessDataset(
			clsAnalysisResults objAnalysisResults, 
			clsMSXMLCreator objMSXmlCreator, 
			string datasetName, 
			Dictionary<string, string> dctDatasetRawFilePaths,
			Dictionary<string, string> dctDatasetYearQuarter,
			Dictionary<string, string> dctDatasetRawDataTypes)
		{
			
			string strDatasetFilePathLocal = string.Empty;

			try
			{

				var instrumentDataFolderPath = Path.Combine(_outputResultsFolderPath, "Instrument_Data");

				if (_bIncludeMzXMLFiles)
				{

					// Create the .mzXML file if it is missing
					var mzXmlFilePathLocal = CreateMzXMLFileIfMissing(objMSXmlCreator, datasetName, objAnalysisResults,
																	  dctDatasetRawFilePaths,
																	  dctDatasetYearQuarter,
																	  dctDatasetRawDataTypes,
																	  out strDatasetFilePathLocal);

					if (string.IsNullOrEmpty(mzXmlFilePathLocal))
					{
						return false;
					}

					// Copy the .MzXml file to the final folder
					var targetFilePath = Path.Combine(instrumentDataFolderPath, Path.GetFileName(mzXmlFilePathLocal));
					m_FileTools.CopyFileUsingLocks(mzXmlFilePathLocal, targetFilePath, m_MachName);

					// Delete the local .mzXml file
					DeleteFileIgnoreErrors(mzXmlFilePathLocal);
				}

				if (_bIncludeInstrumentData)
				{
					// Copy the .raw file, either from the local working directory or from the remote dataset folder
					var strDatasetFilePathSource = dctDatasetRawFilePaths[datasetName];
					if (!string.IsNullOrEmpty(strDatasetFilePathLocal))
					{
						// Dataset was already copied locally; copy it from the local computer to the staging folder
						strDatasetFilePathSource = strDatasetFilePathLocal;
					}

					var blnDatasetFileIsAFolder = Directory.Exists(strDatasetFilePathSource);

					if (blnDatasetFileIsAFolder)
					{
						var diDatasetFolder = new DirectoryInfo(strDatasetFilePathSource);
						var strDatasetFilePathTarget = Path.Combine(instrumentDataFolderPath, diDatasetFolder.Name);
						m_FileTools.CopyDirectory(strDatasetFilePathSource, strDatasetFilePathTarget);
					}
					else
					{
						var fiDatasetFile = new FileInfo(strDatasetFilePathSource);
						var strDatasetFilePathTarget = Path.Combine(instrumentDataFolderPath, fiDatasetFile.Name);
						m_FileTools.CopyFileUsingLocks(strDatasetFilePathSource, strDatasetFilePathTarget, m_MachName);
					}

					if (!string.IsNullOrEmpty(strDatasetFilePathLocal))
					{
						if (blnDatasetFileIsAFolder)
						{
							// Delete the local dataset folder
							if (Directory.Exists(strDatasetFilePathLocal))
							{
								Directory.Delete(strDatasetFilePathLocal, true);
							}
						}
						else
						{
							// Delete the local dataset file
							DeleteFileIgnoreErrors(strDatasetFilePathLocal);
						}
					}
				}
			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error retrieving instrument data for " + datasetName, ex);
				return false;
			}

			return true;

		}

		private bool RetrieveInstrumentData()
		{

			// Extract the packed parameters
			var dctDatasetRawFilePaths = ExtractPackedJobParameterDictionary(clsAnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS);
			var dctDatasetYearQuarter = ExtractPackedJobParameterDictionary(clsAnalysisResourcesRepoPkgr.JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER);
			var dctDatasetRawDataTypes = ExtractPackedJobParameterDictionary(clsAnalysisResourcesRepoPkgr.JOB_PARAM_DICTIONARY_DATASET_RAW_DATA_TYPES);

			int intDatasetsProcessed = 0;

			// The objAnalysisResults object is used to copy files to/from this computer
			var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);

			_MSXmlGeneratorAppPath = GetMSXmlGeneratorAppPath();
			var objMSXmlCreator = new clsMSXMLCreator(_MSXmlGeneratorAppPath, m_WorkDir, m_Dataset, m_DebugLevel, m_jobParams);
			
			// Process each dataset
			foreach (var datasetName in dctDatasetRawFilePaths.Keys)
			{

				bool success = ProcessDataset(objAnalysisResults, objMSXmlCreator, datasetName, dctDatasetRawFilePaths, dctDatasetYearQuarter ,dctDatasetRawDataTypes);

				if (!success)
					return false;

				intDatasetsProcessed += 1;
				m_progress = ComputeIncrementalProgress(PROGRESS_PCT_MZID_RESULTS_COPIED, PROGRESS_PCT_INSTRUMENT_DATA_COPIED, intDatasetsProcessed, dctDatasetRawFilePaths.Count);
				m_StatusTools.UpdateAndWrite(m_progress);

			}

			return true;
		}

		protected bool RetrieveStoragePathInfoTargetFile(string strStoragePathInfoFilePath, clsAnalysisResults objAnalysisResults, ref string strDestPath)
		{
			const bool IsFolder = false;
			return RetrieveStoragePathInfoTargetFile(strStoragePathInfoFilePath, objAnalysisResults, IsFolder, ref strDestPath);
		}

		protected bool RetrieveStoragePathInfoTargetFile(string strStoragePathInfoFilePath, clsAnalysisResults objAnalysisResults, bool IsFolder, ref string strDestPath)
		{
			string strSourceFilePath = string.Empty;

			try
			{
				strDestPath = string.Empty;

				if (!File.Exists(strStoragePathInfoFilePath))
				{
					m_message = "StoragePathInfo file not found";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + strStoragePathInfoFilePath);
					return false;
				}

				using (var srInfoFile = new StreamReader(new FileStream(strStoragePathInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
				{
					if (srInfoFile.Peek() > -1)
					{
						strSourceFilePath = srInfoFile.ReadLine();
					}
				}

				if (string.IsNullOrEmpty(strSourceFilePath))
				{
					m_message = "StoragePathInfo file was empty";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + strStoragePathInfoFilePath);
					return false;
				}

				strDestPath = Path.Combine(m_WorkDir, Path.GetFileName(strSourceFilePath));

				if (IsFolder)
				{
					objAnalysisResults.CopyDirectory(strSourceFilePath, strDestPath, Overwrite: true);
				}
				else
				{
					objAnalysisResults.CopyFileWithRetry(strSourceFilePath, strDestPath, Overwrite: true);
				}

			}
			catch (Exception ex)
			{
				m_message = "Error in RetrieveStoragePathInfoTargetFile";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
				return false;
			}

			return true;

		}

		private bool StoreToolVersionInfo()
		{
			string strToolVersionInfo = string.Empty;

			// Store paths to key files in ioToolFiles
			var ioToolFiles = new List<FileInfo>();

			// Lookup the version of the AnalysisManagerPrideConverter plugin
			if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "AnalysisManager_RepoPkgr_Plugin", blnIncludeRevision: false))
				return false;

			try
			{
				return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: false);
			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion", ex);
				return false;
			}

		}

		#endregion
	}
}
