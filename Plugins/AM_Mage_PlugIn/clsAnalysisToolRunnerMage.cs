using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using AnalysisManager_MAC;

namespace AnalysisManager_Mage_PlugIn
{

	public class clsAnalysisToolRunnerMage : clsAnalysisToolRunnerMAC
	{

		/// <summary>
		/// Sequentially run the Mage operations listed in "MageOperations" parameter
		/// </summary>
		protected override bool RunMACTool()
		{
			//Change the name of the log file for the local log file to the plug in log filename
			String logFileName = Path.Combine(m_WorkDir, "Mage_Log");
			log4net.GlobalContext.Properties["LogName"] = logFileName;
			clsLogTools.ChangeLogFileName(logFileName);

			// run the appropriate Mage pipeline(s) according to operations list parameter
			string mageOperations = m_jobParams.GetParam("MageOperations");
			var ops = new MageAMOperations(m_jobParams, m_mgrParams);
			bool success = ops.RunMageOperations(mageOperations);

			// Change the name of the log file back to the analysis manager log file
			logFileName = m_mgrParams.GetParam("logfilename");
			log4net.GlobalContext.Properties["LogName"] = logFileName;
			clsLogTools.ChangeLogFileName(logFileName);

			if (!string.IsNullOrEmpty(ops.WarningMsg))
			{
				m_EvalMessage = ops.WarningMsg;
			}

			if (!success)
				return false;

			// Make sure the Results.db3 file was created
			var fiResultsDB = new FileInfo(System.IO.Path.Combine(m_WorkDir, "Results.db3"));
			if (!fiResultsDB.Exists)
			{
				m_message = "Results.db3 file was not created";
				return false;
			}

			success = ValidateSqliteDB(mageOperations, fiResultsDB);

			return success;
		}


		/// <summary>
		/// Get name and version info for primary Mage MAC tool assembly
		/// </summary>
		/// <returns></returns>
		protected override string GetToolNameAndVersion()
		{
			string strToolVersionInfo = string.Empty;
			System.Reflection.AssemblyName oAssemblyName = System.Reflection.Assembly.Load("Mage").GetName();
			string strNameAndVersion = oAssemblyName.Name + ", Version=" + oAssemblyName.Version;
			strToolVersionInfo = clsGlobal.AppendToComment(strToolVersionInfo, strNameAndVersion);
			return strToolVersionInfo;
		}

		/// <summary>
		/// Get file version info for supplemental Mage assemblies
		/// </summary>
		/// <returns>List of file info for supplemental DLLs</returns>
		protected override List<FileInfo> GetToolSupplementalVersionInfo()
		{
			var ioToolFiles = new List<FileInfo>
                                  {
                                      new FileInfo("Mage.dll"),
                                      new FileInfo("MageExtContentFilters.dll"),
                                      new FileInfo("MageExtExtractionFilters.dll")
                                  };
			return ioToolFiles;
		}


		protected bool ValidateFactors(FileInfo fiResultsDB, out string errorMessage)
		{
			const string FACTOR_URL = "http://dms2.pnl.gov/requested_run_factors/param";

			errorMessage = string.Empty;

			try
			{
				// Verify that table t_factors exists and has columns Dataset_ID and Sample
				var lstColumns = new List<string>()
				{
					"Dataset_ID",
					"Sample"
				};

				if (!TableContainsDataAndColumns(fiResultsDB, "t_factors", lstColumns, out errorMessage))
				{
					errorMessage = "table t_factors in Results.db3 " + errorMessage +
								"; use " + FACTOR_URL + " to define factors named Sample for the datasets in this data package";
					return false;
				}

				// Lookup the Dataset_ID values defined in t_results
				var datasetIDs = new List<int>();

				string connectionString = "Data Source = " + fiResultsDB.FullName + "; Version=3;";
				using (var conn = new SQLiteConnection(connectionString))
				{
					conn.Open();

					string query = "SELECT Distinct DPJ.dataset_id " +
								   "FROM t_results R " +
										 " INNER JOIN t_data_package_analysis_jobs DPJ " +
										   " ON R.job = DPJ.job";

					using (var cmd = new SQLiteCommand(query, conn))
					{
						var drReader = cmd.ExecuteReader();

						if (!drReader.HasRows)
						{
							errorMessage = "no results joining t_results and t_data_package_analysis_jobs on Job";
							return false;
						}

						while (drReader.Read())
						{
							datasetIDs.Add(drReader.GetInt32(0));
						}
					}					

					// Lookup the Sample Names defined in t_factors
					var sampleNames = new List<string>();

					query = "SELECT Dataset_ID, Sample " +
							"FROM t_factors " +
							"WHERE Dataset_ID IN (" + string.Join(",", datasetIDs) + ")";

					using (var cmd = new SQLiteCommand(query, conn))
					{
						var drReader = cmd.ExecuteReader();

						if (!drReader.HasRows)
						{
							errorMessage = "no results querying t_factors with the DatasetIDs in t_results";
							return false;
						}

						int validDatasetIDs = 0;

						while (drReader.Read())
						{
							
							var sampleName = drReader.GetString(1);
							if (!sampleNames.Contains(sampleName, StringComparer.CurrentCultureIgnoreCase))
								sampleNames.Add(sampleName);

							validDatasetIDs++;
						}

						if (validDatasetIDs < datasetIDs.Count)
						{
							errorMessage = "Of the " + datasetIDs.Count + " datasets in t_results, " + (datasetIDs.Count - validDatasetIDs) +
							               " do not have a factor named Sample defined in DMS; go to " + FACTOR_URL + " to make changes";
							return false;
						}
					}

					// Make sure the sample names in sampleNames correspond to the names defined in t_alias
					// At the same time, count the number of ions defined for each sample
					var sampleToIonMapping = new Dictionary<string, byte>(StringComparer.CurrentCultureIgnoreCase);

					query = "SELECT Sample, Count(Ion) as Ions " +
					        "FROM T_alias " +
					        "GROUP BY Sample ";

					using (var cmd = new SQLiteCommand(query, conn))
					{
						var drReader = cmd.ExecuteReader();

						if (!drReader.HasRows)
						{
							errorMessage = "no results querying t_factors with the DatasetIDs in t_results";
							return false;
						}

						while (drReader.Read())
						{

							var sampleName = drReader.GetString(0);
							var ionCount = drReader.GetByte(1);

							sampleToIonMapping.Add(sampleName, ionCount);

						}
					}

					var lstIonCounts = new List<byte>();

					foreach (var sampleName in sampleNames)
					{
						byte ionCount;
						if (!sampleToIonMapping.TryGetValue(sampleName, out ionCount))
						{
							errorMessage = "t_alias table does not have an entry for Sample '" + sampleName + "'; " +
							               "this Sample name is a dataset factor and must be defined in the t_alias.txt file";
							return false;
						}

						lstIonCounts.Add(ionCount);
					}

					// Make sure all of the ion counts are the same
					byte ionCountFirst = lstIonCounts.First();
					var lookupQ = (from item in lstIonCounts where item != ionCountFirst select item).ToList();
					if (lookupQ.Count > 0)
					{
						errorMessage = "not all entries in the t_alias table have " + ionCountFirst + " ions; edit the t_alias.txt file";
						return false;
					}

					var lstIonColumns = new List<string>();
					var labelingScheme = string.Empty;

					var workFlowSteps = m_jobParams.GetParam("ApeWorkflowStepList", string.Empty);
					if (workFlowSteps.Contains("4plex"))
					{
						// 4-plex iTraq
						labelingScheme = "4plex";
						lstIonColumns.Add("Ion_114");
						lstIonColumns.Add("Ion_115");
						lstIonColumns.Add("Ion_116");
						lstIonColumns.Add("Ion_117");
					}

					if (workFlowSteps.Contains("6plex"))
					{
						// 6-plex TMT
						labelingScheme = "6plex";
						lstIonColumns.Add("Ion_126");
						lstIonColumns.Add("Ion_127");
						lstIonColumns.Add("Ion_128");
						lstIonColumns.Add("Ion_129");
						lstIonColumns.Add("Ion_130");
						lstIonColumns.Add("Ion_131");

					}

					if (workFlowSteps.Contains("8plex"))
					{
						// 8-plex iTraq
						labelingScheme = "8plex";
						lstIonColumns.Add("Ion_113");
						lstIonColumns.Add("Ion_114");
						lstIonColumns.Add("Ion_115");
						lstIonColumns.Add("Ion_116");
						lstIonColumns.Add("Ion_117");
						lstIonColumns.Add("Ion_118");
						lstIonColumns.Add("Ion_119");
						lstIonColumns.Add("Ion_121");
					}

                    if (workFlowSteps.Contains("TMT10Plex"))
                    {
                        // 10-plex TMT
                        labelingScheme = "TMT10Plex";
                        lstIonColumns.Add("Ion_126.128");
                        lstIonColumns.Add("Ion_127.125");
                        lstIonColumns.Add("Ion_127.131");
                        lstIonColumns.Add("Ion_128.128");
                        lstIonColumns.Add("Ion_128.134");
                        lstIonColumns.Add("Ion_129.131");
                        lstIonColumns.Add("Ion_129.138");
                        lstIonColumns.Add("Ion_130.135");
                        lstIonColumns.Add("Ion_130.141");
                    }

					if (lstIonColumns.Count > 0)
					{
						if (!TableContainsDataAndColumns(fiResultsDB, "T_Reporter_Ions", lstIonColumns, out errorMessage))
						{
							errorMessage = "table T_Reporter_Ions in Results.db3 " + errorMessage +
										"; you need to specify " + labelingScheme + " in the ApeWorkflowStepList parameter of the Ape step";
							return false;
						}

					}

				}

			}
			catch (Exception ex)
			{
				errorMessage = "threw an exception while querying (" + ex.Message + ")";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in ValidateFactors: " + ex.Message);
				return false;
			}

			return true;
		}


		protected bool ValidateSqliteDB(string mageOperations, FileInfo fiResultsDB)
		{

			// If the Mage Operations list contains "ExtractFromJobs", then make sure that table "t_results" was created 
			// If it wasn't, then no matching jobs were found and we should fail out this job step
			if (mageOperations.Contains("ExtractFromJobs"))
			{
				if (!TableExists(fiResultsDB, "t_results"))
				{
					m_message = "Results.db3 file does not have table T_Results; Mage did not extract results from any jobs";
					return false;
				}
			}

			bool itraqMode = false;
			string analysisType = m_jobParams.GetJobParameter("AnalysisType", string.Empty);
			if (analysisType.Contains("iTRAQ"))
				itraqMode = true;

			// If the Mage Operations list contains "ImportDataPackageFiles", then make sure that table "T_alias" was created 
			// If it wasn't, then we should fail out this job step
			if (itraqMode || mageOperations.Contains("ImportDataPackageFiles"))
			{
				if (!TableExists(fiResultsDB, "T_alias"))
				{
					m_message =
						"Results.db3 file does not have table T_alias; place a valid T_alias.txt file in the the data package's ImportFiles folder";
					return false;
				}

				// Confirm that the T_alias table contains columns Sample and Ion

				var lstColumns = new List<string>()
				{
					"Sample",
					"Ion"
				};
				string errorMessage;

				if (!TableContainsDataAndColumns(fiResultsDB, "T_alias", lstColumns, out errorMessage))
				{
					m_message = "Table T_alias in Results.db3 " + errorMessage +
								"; place a valid T_alias.txt file in the the data package's ImportFiles folder";
					return false;
				}


				if (!ValidateFactors(fiResultsDB, out errorMessage))
				{
					m_message = "Error validating factors: " + errorMessage;
					return false;
				}

			}

			return true;

		}
	}
}