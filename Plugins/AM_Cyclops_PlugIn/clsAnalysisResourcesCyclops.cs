using System;
using AnalysisManagerBase;
using System.IO;

namespace AnalysisManager_Cyclops_PlugIn
{
    public class clsAnalysisResourcesCyclops : clsAnalysisResources
    {

        public static string AppFilePath = "";

        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {

			try {

				if (m_DebugLevel >= 1) {
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Retrieving input files");
				}

				//Clear out list of files to delete or keep when packaging the blnSuccesss
				clsGlobal.ResetFilesToDeleteOrKeep();

				System.IO.DirectoryInfo dirLocalRScriptFolder = new System.IO.DirectoryInfo(System.IO.Path.Combine(m_WorkingDir, "R_Scripts"));

				if (!dirLocalRScriptFolder.Exists) {
					dirLocalRScriptFolder.Create();
				}

				string dataPackageFolderPath = Path.Combine(m_jobParams.GetParam("transferFolderPath"), m_jobParams.GetParam("OutputFolderName"));
				string analysisType = m_jobParams.GetParam("AnalysisType");

				if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, m_jobParams.GetParam("StepInputFolderName")), m_WorkingDir)) {
					//Errors were reported in function call, so just return
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

				string strInputFileExtension = string.Empty;

				// Retrieve the Cyclops Workflow file specified for this job
				string strCyclopsWorkflowFileName = m_jobParams.GetParam("CyclopsWorkflowName");

				// Retrieve the Workflow file name specified for this job
				if (string.IsNullOrEmpty(strCyclopsWorkflowFileName)) {
					m_message = "Cyclops Workflow not defined in the job parameters for this job";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; unable to continue");
					return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE;
				}

				System.IO.DirectoryInfo diRemoteRScriptFolder;

				string strCyclopsWorkflowFileStoragePath = "\\\\gigasax\\DMS_Workflows\\Cyclops\\" + analysisType;
				diRemoteRScriptFolder = new System.IO.DirectoryInfo(strCyclopsWorkflowFileStoragePath);

				if (!diRemoteRScriptFolder.Exists) {
					m_message = "R Script folder not found: " + diRemoteRScriptFolder.FullName;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
					return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
				}

				if (m_DebugLevel >= 2)
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Copying FROM: " + diRemoteRScriptFolder.FullName);

				foreach (System.IO.FileInfo diRFile in diRemoteRScriptFolder.GetFileSystemInfos("*.R")) {

					if (m_DebugLevel >= 3)
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Copying " + diRFile.Name + " to " + dirLocalRScriptFolder.FullName);

					diRFile.CopyTo(Path.Combine(dirLocalRScriptFolder.FullName, diRFile.Name));					
				}

				//Now copy the Cyclops workflow file to the working directory
				if (!CopyFileToWorkDir(strCyclopsWorkflowFileName, diRemoteRScriptFolder.FullName, m_WorkingDir)) {
					//Errors were reported in function call, so just return
					return IJobParams.CloseOutType.CLOSEOUT_FAILED;
				}

			} catch (Exception ex) {
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception retrieving resources", ex);
			}

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }

    }
}
