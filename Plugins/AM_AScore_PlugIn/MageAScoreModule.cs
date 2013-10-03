using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Xml;
using AnalysisManagerBase;
using AScore_DLL;
using AScore_DLL.Managers;
using AScore_DLL.Managers.DatasetManagers;
using Mage;
using MageExtExtractionFilters;
using MyEMSLReader;
using PRISM.Processes;
using MessageEventArgs = AScore_DLL.MessageEventArgs;

namespace AnalysisManager_AScore_PlugIn
{
	/// <summary>
	/// This is a Mage module that does AScore processing 
	/// of results for jobs that are supplied to it via standard tabular input
	/// </summary>
	public class MageAScoreModule : ContentFilter
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
		public MageAScoreModule()
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
				clsAScoreMagePipeline.ImportFileToSQLite(fhtFile, dbFilePath, tableName);
				tableName = "t_results_ascore";
				clsAScoreMagePipeline.ImportFileToSQLite(ascoreOutputFilePath, dbFilePath, tableName);

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
			clsAScoreMagePipeline.mMyEMSLDatasetInfo.AddDataset(datasetName);
			var lstArchiveFiles = clsAScoreMagePipeline.mMyEMSLDatasetInfo.FindFiles("*_dta.zip", diResultsFolder.Name, datasetName);

			if (lstArchiveFiles.Count == 0)
			{
				// Look for the JobParameters file
				lstArchiveFiles = clsAScoreMagePipeline.mMyEMSLDatasetInfo.FindFiles("JobParameters_*.xml", diResultsFolder.Name, datasetName);
				if (lstArchiveFiles.Count == 0)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
										 "JobParameters XML file not found in folder " + diResultsFolder.FullName +
										 "; unable to determine the DTA folder");
					return null;
				}

				clsAScoreMagePipeline.mMyEMSLDatasetInfo.AddFileToDownloadQueue(lstArchiveFiles.First().FileInfo);

				if (!clsAScoreMagePipeline.mMyEMSLDatasetInfo.ProcessDownloadQueue(Path.GetTempPath(), Downloader.DownloadFolderLayout.FlatNoSubfolders))
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

				clsAScoreMagePipeline.mMyEMSLDatasetInfo.ClearDownloadQueue();
				lstArchiveFiles = clsAScoreMagePipeline.mMyEMSLDatasetInfo.FindFiles("*_dta.zip", dtaFolderName, datasetName);

				if (lstArchiveFiles.Count == 0)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
										 "DTA file not found in folder " + dtaFolderName + " in MyEMSL");
					return null;
				}
			}

			clsAScoreMagePipeline.mMyEMSLDatasetInfo.AddFileToDownloadQueue(lstArchiveFiles.First().FileInfo);

			if (!clsAScoreMagePipeline.mMyEMSLDatasetInfo.ProcessDownloadQueue(WorkingDir, Downloader.DownloadFolderLayout.FlatNoSubfolders))
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
}
