using System;
using System.IO;
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

		#region Fields

		private bool _bIncludeRawDataFiles;
		private bool _bIncludeSequestFiles;
		private bool _bIncludeMzXMLFiles;
		private bool _bIncludeMGFPFiles;
		private bool _bIncludeMZidFiles;

		private string _outputResultsFolderPath;
		private MageRepoPkgrPipelines _mgr;

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
			switch (targetRepository)
			{
				case "PeptideAtlas":
					DoPeptideAtlasOperation();
					break;
				// FUTURE: code for other repositories to go here someday
			}
			return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
		}

		/// <summary>
		/// Gather files for submission to PeptideAtlas repository
		/// and copy them to repo cache
		/// </summary>
		private void DoPeptideAtlasOperation()
		{
			// copy *.fasta files from organism db working folder (created by resourcer) to appropriate cache subfolder
			FileUtils.CopyFiles(m_mgrParams.GetParam("orgdbdir"), "*.fasta", Path.Combine(_outputResultsFolderPath, "Organism_Database"));
			//
			if (_bIncludeSequestFiles)
			{
				// find any sequest jobs in data package and copy their first hits files to appropriate cache subfolder
				_mgr.GetItemsToRepoPkg("DataPkgJobsQueryTemplate", "SEQUEST", "*_fht.txt", "SEQUEST_Results", "Job");
			}
			if (_bIncludeMGFPFiles)
			{
				// find any MSGFPlus jobs in data package and copy their first hits files to appropriate cache subfolder
				_mgr.GetItemsToRepoPkg("DataPkgJobsQueryTemplate", "MSGFPlus", "*_msgfdb_fht.txt;*_msgfdb_fht_MSGF.txt", "MSGFPlus_Results", "Job");
			}
			if (_bIncludeMZidFiles)
			{
				// find any MSGFPlus jobs in data package and copy their MZID files to appropriate cache subfolder
				_mgr.GetItemsToRepoPkg("DataPkgJobsQueryTemplate", "MSGFPlus", "*_msgfplus.zip", @"MSGFPlus_Results\MZID_Files", "Job");
			}
			if (_bIncludeRawDataFiles || _bIncludeMzXMLFiles)
			{
				// find any datasets in data package and copy their raw data files to appropriate cache subfolder
				_mgr.GetItemsToRepoPkg("DataPkgDatasetsQueryTemplate", "", "*.raw", "Instrument_Data", "");
			}
			if (_bIncludeMzXMLFiles)
			{
				// generate mzXML files from raw data files in cache subfolder
				var instrumentDataFolderPath = Path.Combine(_outputResultsFolderPath, "Instrument_Data");
				GenerateMzXMLFilesFromDataFiles(instrumentDataFolderPath);
				//
				if (!_bIncludeRawDataFiles)
				{
					// delete raw data files if they are not part of final repo cache
					FileUtils.DeleteFiles(instrumentDataFolderPath, "*.raw");
				}
			}
			// todo Do some logging on the above pipeline runs using pipeline intermediate results (_mgr.DataPackageItems; _mgr.AssociatedFiles; _mgr.ManifestForCopy;)?
		}

		/// <summary>
		/// SetCnStr the report option flags from job parameters
		/// </summary>
		private void SetOptions()
		{
			_bIncludeSequestFiles = m_jobParams.GetJobParameter("IncludeSequestFiles", true);
			_bIncludeMGFPFiles = m_jobParams.GetJobParameter("IncludeMGFFiles", true);
			_bIncludeMZidFiles = m_jobParams.GetJobParameter("IncludeMZidFiles", true);
			_bIncludeRawDataFiles = m_jobParams.GetJobParameter("IncludeRawDataFiles", true);
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

		/// <summary>
		/// for each raw data file in the given repo cache folder
		/// generate a corresponding mzXML file
		/// </summary>
		/// <param name="instrumentDataFolderPath"></param>
		private void GenerateMzXMLFilesFromDataFiles(string instrumentDataFolderPath)
		{
			// under construction
			m_jobParams.AddAdditionalParameter("JobParameters", "RawDataType", "dot_raw_files"); // use packed list from resourcer?

			var dir = new DirectoryInfo(instrumentDataFolderPath);
			foreach (var fi in dir.GetFiles("*.raw"))
			{
				var dataset = Path.GetFileNameWithoutExtension(fi.Name);
				GenerateMzXMLFile(dataset, instrumentDataFolderPath);
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

		#endregion
	}
}
