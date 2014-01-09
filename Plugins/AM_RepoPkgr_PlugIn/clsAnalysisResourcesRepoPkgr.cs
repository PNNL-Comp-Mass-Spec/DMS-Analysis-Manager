using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManager_RepoPkgr_PlugIn;
using AnalysisManagerBase;

namespace AnalysisManager_RepoPkgr_Plugin
{
	public class clsAnalysisResourcesRepoPkgr : clsAnalysisResources
	{
		#region Constants

		public const string FASTA_FILES_FOR_DATA_PACKAGE = "FastaFilesForDataPackage";
		public const string JOB_PARAM_DATASETS_MISSING_MZXML_FILES = "PackedParam_DatasetsMissingMzXMLFiles";
		public const string JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER = "PackedParam_DatasetStorage_YearQuarter";

		#endregion

		#region Member_Functions

		/// <summary>
		/// Do any resource-gathering tasks here
		/// </summary>
		/// <returns></returns>
		public override IJobParams.CloseOutType GetResources()
		{
			string localOrgDBFolder = m_mgrParams.GetParam("orgdbdir");

			// get fasta file(s) for jobs in data package and copy to local organism database working directory
			int dataPkgId = -1;
			List<udtDataPackageJobInfoType> lstDataPackagePeptideHitJobs = RetrieveDataPackagePeptideHitJobInfo(ref dataPkgId);
			bool success = RetrieveFastaFiles(localOrgDBFolder, lstDataPackagePeptideHitJobs);

			if (!success)
				return IJobParams.CloseOutType.CLOSEOUT_NO_FAS_FILES;

			bool includeMzXmlFiles = m_jobParams.GetJobParameter("IncludeMzXMLFiles", true);

			success = FindInstrumentDataFiles(lstDataPackagePeptideHitJobs, includeMzXmlFiles);

			if (includeMzXmlFiles)
				FindMissingMzXmlFiles(lstDataPackagePeptideHitJobs);

			if (success)
				return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
			else
				return IJobParams.CloseOutType.CLOSEOUT_FAILED;
		}

		#endregion // Member_Functions

		#region Code_Adapted_From_Pride_Plugin

		private bool FindInstrumentDataFiles(IEnumerable<udtDataPackageJobInfoType> lstDataPackagePeptideHitJobs, bool includeMzXmlFiles)
		{

			// The keys in this dictionary are udtJobInfo entries; the values in this dictionary are KeyValuePairs of path to the .mzXML file and path to the .hashcheck file (if any)
			// The KeyValuePair will have empty strings if the .Raw file needs to be retrieved
			var dctInstrumentDataToRetrieve = new Dictionary<udtDataPackageJobInfoType, KeyValuePair<string, string>>();

			// Keys in this dictionary are dataset name, values are the full path to the instrument data file for the dataset
			var dctDatasetRawFilePaths = new Dictionary<String, String>();

			// Keys in this dictionary are dataset name, values are the raw_data_type for the dataset
			var dctDatasetRawDataTypes = new Dictionary<string, string>();

			// Cache the current dataset and job info
			var udtCurrentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

			foreach (udtDataPackageJobInfoType udtJobInfo in lstDataPackagePeptideHitJobs)
			{
				if (!OverrideCurrentDatasetAndJobInfo(udtJobInfo))
				{
					// Error message has already been logged
					return false;
				}

				if (includeMzXmlFiles)
				{

					// See if a .mzXML file already exists for this dataset
					string strHashcheckFilePath = string.Empty;

					string strMzXMLFilePath = FindMZXmlFile(ref strHashcheckFilePath);

					if (string.IsNullOrEmpty(strMzXMLFilePath))
					{
						// mzXML file not found
						if (udtJobInfo.RawDataType == RAW_DATA_TYPE_DOT_RAW_FILES)
						{
							// Will need to retrieve the .Raw file for this dataset
							dctInstrumentDataToRetrieve.Add(udtJobInfo, new KeyValuePair<String, String>(String.Empty, String.Empty));
						}
						else
						{
							m_message = "mzXML file not found for dataset " + udtJobInfo.Dataset +
										" and dataset file type is not a .Raw file and we thus cannot auto-create the missing mzXML file";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
							return false;
						}
					}
					else
					{
						dctInstrumentDataToRetrieve.Add(udtJobInfo, new KeyValuePair<String, String>(strMzXMLFilePath, strHashcheckFilePath));
					}
				}


				bool blnIsFolder = false;
				string strRawFilePath = FindDatasetFileOrFolder(ref blnIsFolder);

				if (!string.IsNullOrEmpty(strRawFilePath))
				{
					if (!dctDatasetRawFilePaths.ContainsKey(udtJobInfo.Dataset))
					{
						dctDatasetRawFilePaths.Add(udtJobInfo.Dataset, strRawFilePath);
					}
				}

				if (!dctDatasetRawDataTypes.ContainsKey(udtJobInfo.Dataset))
				{
					dctDatasetRawDataTypes.Add(udtJobInfo.Dataset, udtJobInfo.RawDataType);
				}

			}

			// Restore the dataset and job info for this aggregation job
			OverrideCurrentDatasetAndJobInfo(udtCurrentDatasetAndJobInfo);

			// Store the dataset paths in a Packed Job Parameter
			StorePackedJobParameterDictionary(dctDatasetRawFilePaths, JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS);

			// Store the dataset RawDataTypes in a Packed Job Parameter
			StorePackedJobParameterDictionary(dctDatasetRawDataTypes, JOB_PARAM_DICTIONARY_DATASET_RAW_DATA_TYPES);

			var udtOptions = new udtDataPackageRetrievalOptionsType
			{
				CreateJobPathFiles = true,
				RetrieveMzXMLFile = true
			};

			bool success = RetrieveDataPackageMzXMLFiles(dctInstrumentDataToRetrieve, udtOptions);

			return success;

		}


		/// <summary>
		/// Find datasets that do not have a .mzXML file
		/// Datasets that need to have .mzXML files created will be added to the packed job parameters, storing the dataset names in "PackedParam_DatasetsMissingMzXMLFiles"
		/// and the dataset Year_Quarter values in "PackedParam_DatasetStorage_YearQuarter"
		/// </summary>
		/// <param name="lstDataPackagePeptideHitJobs"></param>
		/// <remarks></remarks>
		protected void FindMissingMzXmlFiles(IEnumerable<udtDataPackageJobInfoType> lstDataPackagePeptideHitJobs)
		{
			var lstDatasets = new SortedSet<string>();
			var lstDatasetYearQuarter = new SortedSet<string>();

			try
			{
				foreach (var udtJob in lstDataPackagePeptideHitJobs)
				{
					string strMzXmlFilePath = Path.Combine(m_WorkingDir, udtJob.Dataset + DOT_MZXML_EXTENSION);

					if (!File.Exists(strMzXmlFilePath))
					{
						// Look for a StoragePathInfo file
						strMzXmlFilePath += STORAGE_PATH_INFO_FILE_SUFFIX;
						if (!File.Exists(strMzXmlFilePath))
						{
							if (!lstDatasets.Contains(udtJob.Dataset))
							{
								lstDatasets.Add(udtJob.Dataset);
								lstDatasetYearQuarter.Add(udtJob.Dataset + "=" + GetDatasetYearQuarter(udtJob.ServerStoragePath));
							}
						}

					}
				}

				if (lstDatasets.Count > 0)
				{
					StorePackedJobParameterList(lstDatasets.ToList(), JOB_PARAM_DATASETS_MISSING_MZXML_FILES);
					StorePackedJobParameterList(lstDatasetYearQuarter.ToList(), JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER);
				}

			}
			catch (Exception ex)
			{
				m_message = "Exception in FindMissingMzXmlFiles";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
			}

		}

		private bool RetrieveFastaFiles(string localOrgDBFolder, IEnumerable<udtDataPackageJobInfoType> lstDataPackagePeptideHitJobs)
		{
			try
			{
				// This dictionary is used to avoid calling RetrieveOrgDB() for every job
				// The dictionary keys are LegacyFastaFileName, ProteinOptions, and ProteinCollectionList combined with underscores
				// The dictionary values are the name of the generated (or retrieved) fasta file
				var dctOrgDBParamsToGeneratedFileNameMap = new Dictionary<string, string>();

				// This list tracks the generated fasta file name
				var lstGeneratedOrgDBNames = new List<string>();

				// Cache the current dataset and job info
				udtDataPackageJobInfoType udtCurrentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

				foreach (var udtJob in lstDataPackagePeptideHitJobs)
				{
					string strDictionaryKey = string.Format("{0}_{1}_{2}", udtJob.LegacyFastaFileName, udtJob.ProteinCollectionList,
					                                        udtJob.ProteinOptions);
					string strOrgDBNameGenerated;
					if (dctOrgDBParamsToGeneratedFileNameMap.TryGetValue(strDictionaryKey, out strOrgDBNameGenerated))
					{
						// Organism DB was already generated
					}
					else
					{
						OverrideCurrentDatasetAndJobInfo(udtJob);
						m_jobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", string.Empty);
						if (!RetrieveOrgDB(localOrgDBFolder))
						{
							if (string.IsNullOrEmpty(m_message))
								m_message = "Call to RetrieveOrgDB returned false in clsAnalysisResourcesRepoPkgr.RetrieveFastaFiles";
							return false;
						}
						strOrgDBNameGenerated = m_jobParams.GetJobParameter("PeptideSearch", "generatedFastaName", string.Empty);
						if (string.IsNullOrEmpty(strOrgDBNameGenerated))
						{
							m_message = "FASTA file was not generated when RetrieveFastaFiles called RetrieveOrgDB";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
							                     m_message + " (class clsAnalysisResourcesRepoPkgr)");
							return false;
						}
						if (strOrgDBNameGenerated != udtJob.OrganismDBName)
						{
							m_message = "Generated FASTA file name (" + strOrgDBNameGenerated + ") does not match expected fasta file name (" +
							            udtJob.OrganismDBName + "); aborting";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
							                     m_message + " (class clsAnalysisResourcesRepoPkgr)");
							return false;
						}
						dctOrgDBParamsToGeneratedFileNameMap.Add(strDictionaryKey, strOrgDBNameGenerated);
						
						lstGeneratedOrgDBNames.Add(strOrgDBNameGenerated);
					}
					// Add a new job parameter that associates strOrgDBNameGenerated with this job
					m_jobParams.AddAdditionalParameter("PeptideSearch", GetGeneratedFastaParamNameForJob(udtJob.Job),
					                                   strOrgDBNameGenerated);
				}

				// Store the names of the generated fasta files
				// This is a tab separated list of filenames
				StorePackedJobParameterList(lstGeneratedOrgDBNames, FASTA_FILES_FOR_DATA_PACKAGE);
				
				// Restore the dataset and job info for this aggregation job
				OverrideCurrentDatasetAndJobInfo(udtCurrentDatasetAndJobInfo);
			}
			catch (Exception ex)
			{
				m_message = "Exception in RetrieveFastaFiles";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
				return false;
			}
			return true;
		}
	

	private static string GetGeneratedFastaParamNameForJob(int job)
		{
			return "Job" + job + "_GeneratedFasta";
		}

		#endregion // Code_Adapted_From_Pride_Plugin

	}
}
