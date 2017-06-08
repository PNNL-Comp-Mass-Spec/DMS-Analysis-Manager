using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManager_Ape_PlugIn
{
	class clsApeAMRunWorkflow : clsApeAMBase
	{
		#region Member Variables

		#endregion

		#region Constructors

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="jobParms"></param>
		/// <param name="mgrParms"></param>
		public clsApeAMRunWorkflow(IJobParams jobParms, IMgrParams mgrParms)
			: base(jobParms, mgrParms)
		{
		}

		#endregion

		public bool RunWorkflow()
		{
			var blnSuccess = true;
			var progressHandler = new Ape.SqlConversionHandler(delegate(bool done, bool success, int percent, string msg)
			{
				Console.WriteLine(msg);

				if (done)
				{
					if (success)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Ape successfully ran workflow " + GetJobParam("ApeWorkflowName"));
						blnSuccess = true;
					}
					else
					{
						mErrorMessage = "Error running Ape in clsApeAMRunWorkflow";
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mErrorMessage);
						blnSuccess = false;
					}
				}

			});

			var apeWorkflow = Path.Combine(mWorkingDir, GetJobParam("ApeWorkflowName"));
			var apeDatabase = Path.Combine(mWorkingDir, "Results.db3");
			var apeWorkflowStepList = Convert.ToString(GetJobParam("ApeWorkflowStepList"));

			if (string.IsNullOrEmpty(apeWorkflowStepList))
			{
				// The job parameter originally was missing the "k" in workflow; try that version instead
				apeWorkflowStepList = Convert.ToString(GetJobParam("ApeWorflowStepList"));
			}

			// Check whether we should compact the database
			var apeCompactDatabase = Convert.ToBoolean(GetJobParam("ApeCompactDatabase"));

			Ape.SqlServerToSQLite.ProgressChanged += OnProgressChanged;
			Ape.SqlServerToSQLite.StartWorkflow(apeWorkflowStepList, apeWorkflow, apeDatabase, apeDatabase, false, apeCompactDatabase, progressHandler);

			if (!blnSuccess)
			{
				if (string.IsNullOrEmpty(mErrorMessage))
					mErrorMessage = "Ape.SqlServerToSQLite.StartWorkflow returned false";
			}
			else
			{
				var analysisType = GetJobParam("AnalysisType", string.Empty);
				if (!string.Equals(analysisType, "improv", StringComparison.OrdinalIgnoreCase))
				{
					// Add the protein parsimony tables
					blnSuccess = StartProteinParsimony(apeDatabase);
				}
				
			}

			return blnSuccess;

		}

		private bool StartProteinParsimony(string apeDatabase)
		{
			const string SOURCE_TABLE = "T_Row_Metadata";

			bool success;
			try
			{				
				var parsimonyRunner = new SetCover.Runner();

				var fiDatabase = new FileInfo(apeDatabase);
				if (fiDatabase.Directory == null)
					throw new IOException("Error determining the parent folder path for the Ape database");

				// Add the protein parsimony tables
				success = parsimonyRunner.RunAlgorithm(fiDatabase.Directory.FullName, fiDatabase.Name, SOURCE_TABLE);

			}
			catch (Exception ex)
			{
				mErrorMessage = "Error adding the parsimony tables: " + ex.Message;
				success = false;
			}

			return success;
		}
	}
}
