using System;
using System.Collections.Generic;
using System.Globalization;
using AnalysisManager_RepoPkgr_PlugIn;
using AnalysisManagerBase;

namespace AnalysisManager_RepoPkgr_Plugin
{
	public class clsAnalysisResourcesRepoPkgr : clsAnalysisResources
	{

		#region Member_Functions

		/// <summary>
		/// Do any resource-gathering tasks here
		/// </summary>
		/// <returns></returns>
		public override IJobParams.CloseOutType GetResources()
		{
			// assure that local organism database working directory exists and is empty
			FileUtils.SetupWorkDir(m_mgrParams.GetParam("orgdbdir"));

			// get fasta file(s) for jobs in data package and copy to local organism database working directory
			var dataPkgId = m_jobParams.GetJobParameter("DataPackageID", 0);
			var lstDataPackagePeptideHitJobs = RetrieveDataPackagePeptideHitJobInfo(ref dataPkgId);
			return !RetrieveFastaFiles(lstDataPackagePeptideHitJobs) ? IJobParams.CloseOutType.CLOSEOUT_NO_FAS_FILES : IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
		}

		#endregion // Member_Functions

		#region Code_Adapted_From_Pride_Plugin

		private bool RetrieveFastaFiles(IEnumerable<udtDataPackageJobInfoType> lstDataPackagePeptideHitJobs)
		{
			string strLocalOrgDBFolder = m_mgrParams.GetParam("orgdbdir");
			// This dictionary is used to avoid calling RetrieveOrgDB() for every job
			// The dictionary keys are LegacyFastaFileName, ProteinOptions, and ProteinCollectionList combined with underscores
			// The dictionary values are the name of the generated (or retrieved) fasta file
			try
			{
				var dctOrgDBParamsToGeneratedFileNameMap = new Dictionary<string, string>();

				// Cache the current dataset and job info
				udtDataPackageJobInfoType udtCurrentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

				foreach (var udtJob in lstDataPackagePeptideHitJobs)
				{
					string strDictionaryKey = string.Format("{0}_{1}_{2}", udtJob.LegacyFastaFileName, udtJob.ProteinCollectionList, udtJob.ProteinOptions);
					string strOrgDBNameGenerated;
					if (dctOrgDBParamsToGeneratedFileNameMap.TryGetValue(strDictionaryKey, out strOrgDBNameGenerated))
					{
						// Organism DB was already generated
					}
					else
					{
						OverrideCurrentDatasetAndJobInfo(udtJob);
						m_jobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", string.Empty);
						if (!RetrieveOrgDB(strLocalOrgDBFolder))
						{
							if (string.IsNullOrEmpty(m_message))
								m_message = "Call to RetrieveOrgDB returned false in clsAnalysisResourcesRepoPkgr.RetrieveFastaFiles";
							return false;
						}
						strOrgDBNameGenerated = m_jobParams.GetJobParameter("PeptideSearch", "generatedFastaName", string.Empty);
						if (string.IsNullOrEmpty(strOrgDBNameGenerated))
						{
							m_message = "FASTA file was not generated when RetrieveFastaFiles called RetrieveOrgDB";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + " (class clsAnalysisResourcesRepoPkgr)");
							return false;
						}
						if (strOrgDBNameGenerated != udtJob.OrganismDBName)
						{
							m_message = "Generated FASTA file name (" + strOrgDBNameGenerated + ") does not match expected fasta file name (" + udtJob.OrganismDBName + "); aborting";
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + " (class clsAnalysisResourcesRepoPkgr)");
							return false;
						}
						dctOrgDBParamsToGeneratedFileNameMap.Add(strDictionaryKey, strOrgDBNameGenerated);
					}
					// Add a new job parameter that associates strOrgDBNameGenerated with this job
					m_jobParams.AddAdditionalParameter("PeptideSearch", GetGeneratedFastaParamNameForJob(udtJob.Job), strOrgDBNameGenerated);
				}
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
			return "Job" + job.ToString(CultureInfo.InvariantCulture) + "_GeneratedFasta";
		}

		#endregion // Code_Adapted_From_Pride_Plugin

	}
}
