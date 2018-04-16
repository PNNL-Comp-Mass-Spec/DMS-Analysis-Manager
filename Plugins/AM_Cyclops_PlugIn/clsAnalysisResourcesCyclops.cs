using System;
using AnalysisManagerBase;
using System.IO;

namespace AnalysisManager_Cyclops_PlugIn
{

    /// <summary>
    /// Retrieve resources for the Cyclops plugin
    /// </summary>
    public class clsAnalysisResourcesCyclops : clsAnalysisResources
    {

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {

            try
            {

                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();
                if (result != CloseOutType.CLOSEOUT_SUCCESS) {
                    return result;
                }

                if (m_DebugLevel >= 1)
                {
                    LogMessage("Retrieving input files");
                }

                var dirLocalRScriptFolder = new DirectoryInfo(Path.Combine(m_WorkingDir, "R_Scripts"));

                if (!dirLocalRScriptFolder.Exists)
                {
                    dirLocalRScriptFolder.Create();
                }

                var dataPackageFolderPath = Path.Combine(m_jobParams.GetParam(JOB_PARAM_TRANSFER_FOLDER_PATH), m_jobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME));
                var analysisType = m_jobParams.GetParam("AnalysisType");
                var sourceFolderName = m_jobParams.GetParam("StepInputFolderName");

                // Retrieve the Cyclops Workflow file specified for this job
                var cyclopsWorkflowFileName = m_jobParams.GetParam("CyclopsWorkflowName");

                // Retrieve the Workflow file name specified for this job
                if (string.IsNullOrEmpty(cyclopsWorkflowFileName))
                {
                    m_message = "Parameter CyclopsWorkflowName not defined in the job parameters for this job";
                    LogError(m_message + "; unable to continue");
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                var proteinProphet = m_jobParams.GetParam("RunProteinProphet");

                if (!string.IsNullOrEmpty(proteinProphet))
                {
                    // If User requests to run ProteinProphet
                    if (proteinProphet.ToLower().Equals("true"))
                    {
                        var proteinOptions = m_jobParams.GetParam("ProteinOptions");

                        if (proteinOptions.Length > 0 && !proteinOptions.ToLower().Equals("na"))
                        {
                            // Override the Protein Options to force forward direction only
                            m_jobParams.SetParam("PeptideSearch", "ProteinOptions", "seq_direction=forward,filetype=fasta");
                        }

                        // Generate the path Fasta File
                        var orgDbDirectoryPath = m_mgrParams.GetParam("orgdbdir");
                        if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultcode))
                        {
                            m_message = "Cyclops Resourcer failed to retrieve the path to the Fasta file to run ProteinProphet";
                            return resultcode;
                        }
                    }
                }

                var dmsWorkflowsFolderPath = m_mgrParams.GetParam("DMSWorkflowsFolderPath", @"\\gigasax\DMS_Workflows");
                var remoteRScriptFolder = new DirectoryInfo(Path.Combine(dmsWorkflowsFolderPath, "Cyclops", "RScript"));

                if (!remoteRScriptFolder.Exists)
                {
                    m_message = "R Script folder not found: " + remoteRScriptFolder.FullName;
                    LogError(m_message);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (m_DebugLevel >= 2)
                    LogMessage("Copying FROM: " + remoteRScriptFolder.FullName);

                foreach (var rFile in remoteRScriptFolder.GetFiles("*.R"))
                {
                    if (m_DebugLevel >= 3)
                        LogMessage("Copying " + rFile.Name + " to " + dirLocalRScriptFolder.FullName);

                    rFile.CopyTo(Path.Combine(dirLocalRScriptFolder.FullName, rFile.Name));
                }

                var strCyclopsWorkflowDirectory = Path.Combine(dmsWorkflowsFolderPath, "Cyclops", analysisType);

                LogMessage("Retrieving workflow file: " + Path.Combine(strCyclopsWorkflowDirectory, cyclopsWorkflowFileName));

                // Now copy the Cyclops workflow file to the working directory
                if (!CopyFileToWorkDir(cyclopsWorkflowFileName, strCyclopsWorkflowDirectory, m_WorkingDir))
                {
                    // Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, sourceFolderName), m_WorkingDir))
                {
                    m_message = "Results.db3 file from " + sourceFolderName + " failed to copy over to working directory";
                    // Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

            }
            catch (Exception ex)
            {
                LogError("Exception retrieving resources", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

    }
}
