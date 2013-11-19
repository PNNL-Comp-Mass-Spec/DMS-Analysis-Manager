using System;
using AnalysisManagerBase;
using System.IO;

namespace AnalysisManager_Cyclops_PlugIn
{
	public class clsAnalysisResourcesCyclops : clsAnalysisResources
	{

		public static string AppFilePath = "";

		public override IJobParams.CloseOutType GetResources()
		{

			try
			{

				if (m_DebugLevel >= 1)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving input files");
				}

				var dirLocalRScriptFolder = new DirectoryInfo(Path.Combine(m_WorkingDir, "R_Scripts"));

				if (!dirLocalRScriptFolder.Exists)
				{
					dirLocalRScriptFolder.Create();
				}

				string dataPackageFolderPath = Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("OutputFolderName"));
				string analysisType = m_jobParams.GetParam("AnalysisType");
				string sourceFolderName = m_jobParams.GetParam("StepInputFolderName");

				if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, sourceFolderName), m_WorkingDir))
			{
					m_message = "Results.db3 file from " + sourceFolderName + " failed to copy over to working directory";
					//Errors were reported in function call, so just return
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

				// Retrieve the Cyclops Workflow file specified for this job
				string strCyclopsWorkflowFileName = m_jobParams.GetParam("CyclopsWorkflowName");

				// Retrieve the Workflow file name specified for this job
				if (string.IsNullOrEmpty(strCyclopsWorkflowFileName))
				{
					m_message = "Parameter CyclopsWorkflowName not defined in the job parameters for this job";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; unable to continue");
					return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE;
				}

				string strProteinProphet = m_jobParams.GetParam("RunProteinProphet");

				if (!string.IsNullOrEmpty(strProteinProphet))
				{
					// If User requests to run ProteinProphet
					if (strProteinProphet.ToLower().Equals("true"))
					{
						string sProteinOptions = m_jobParams.GetParam("ProteinOptions");

						if (sProteinOptions.Length > 0 && !sProteinOptions.ToLower().Equals("na"))
						{
							// Override the Protein Options to force forward direction only
							m_jobParams.SetParam("PeptideSearch", "ProteinOptions", "seq_direction=forward,filetype=fasta");
						}

						// Generate the path Fasta File
						string s_FastaDir = m_mgrParams.GetParam("orgdbdir");
						if (!RetrieveOrgDB(s_FastaDir))
						{
							m_message = "Cyclops Resourcer failed to retrieve the path to the Fasta file to run ProteinProphet";
							return IJobParams.CloseOutType.CLOSEOUT_FAILED;
						}
					}
				}

				string strDMSWorkflowsFolderPath = m_mgrParams.GetParam("DMSWorkflowsFolderPath", @"\\gigasax\DMS_Workflows");
				var diRemoteRScriptFolder = new DirectoryInfo(Path.Combine(strDMSWorkflowsFolderPath, "Cyclops", "RScript"));

				if (!diRemoteRScriptFolder.Exists)
				{
					m_message = "R Script folder not found: " + diRemoteRScriptFolder.FullName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
					return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
				}

				if (m_DebugLevel >= 2)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Copying FROM: " + diRemoteRScriptFolder.FullName);

				foreach (FileInfo diRFile in diRemoteRScriptFolder.GetFileSystemInfos("*.R"))
				{

					if (m_DebugLevel >= 3)
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Copying " + diRFile.Name + " to " + dirLocalRScriptFolder.FullName);

					diRFile.CopyTo(Path.Combine(dirLocalRScriptFolder.FullName, diRFile.Name));
				}

				string strCyclopsWorkflowDirectory = Path.Combine(strDMSWorkflowsFolderPath, "Cyclops", analysisType);

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving workflow file: " + Path.Combine(strCyclopsWorkflowDirectory, strCyclopsWorkflowFileName));

				// Now copy the Cyclops workflow file to the working directory
				if (!CopyFileToWorkDir(strCyclopsWorkflowFileName, strCyclopsWorkflowDirectory, m_WorkingDir))
				{
					//Errors were reported in function call, so just return
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception retrieving resources", ex);
			}

			return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
		}

	}
}
