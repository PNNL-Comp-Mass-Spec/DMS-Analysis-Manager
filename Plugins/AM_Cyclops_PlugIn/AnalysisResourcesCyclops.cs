using System;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManager_Cyclops_PlugIn
{

    /// <summary>
    /// Retrieve resources for the Cyclops plugin
    /// </summary>
    public class AnalysisResourcesCyclops : AnalysisResources
    {
        // Ignore Spelling: fasta, filetype, na, resourcer, workflow, workflows

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            // ReSharper disable once IdentifierTypo
            const string ITRAQ_ANALYSIS_TYPE = "iTRAQ";

            try
            {

                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();
                if (result != CloseOutType.CLOSEOUT_SUCCESS) {
                    return result;
                }

                if (mDebugLevel >= 1)
                {
                    LogMessage("Retrieving input files");
                }

                var dirLocalRScriptFolder = new DirectoryInfo(Path.Combine(mWorkDir, "R_Scripts"));

                if (!dirLocalRScriptFolder.Exists)
                {
                    dirLocalRScriptFolder.Create();
                }

                var dataPackageFolderPath = Path.Combine(mJobParams.GetParam(JOB_PARAM_TRANSFER_DIRECTORY_PATH), mJobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME));
                var analysisType = mJobParams.GetParam("AnalysisType");

                if (analysisType.IndexOf("TMT", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    // The workflow file for TMT jobs is in the iTRAQ subdirectory below \\gigasax\DMS_Workflows\Cyclops
                    // Override the analysis type
                    LogDebugMessage(string.Format("Changing analysis type from {0} to {1}", analysisType, ITRAQ_ANALYSIS_TYPE));
                    analysisType = ITRAQ_ANALYSIS_TYPE;
                }
                else if (analysisType.IndexOf(ITRAQ_ANALYSIS_TYPE, StringComparison.OrdinalIgnoreCase) >= 0 && analysisType.Length > 5)
                {
                    // The user likely specified iTRAQ8
                    // Override to be simply iTRAQ
                    LogDebugMessage(string.Format("Changing analysis type from {0} to {1}", analysisType, ITRAQ_ANALYSIS_TYPE));
                    analysisType = ITRAQ_ANALYSIS_TYPE;
                }

                var sourceFolderName = mJobParams.GetParam("StepInputFolderName");

                // Retrieve the Cyclops Workflow file specified for this job
                var cyclopsWorkflowFileName = mJobParams.GetParam("CyclopsWorkflowName");

                // Retrieve the Workflow file name specified for this job
                if (string.IsNullOrEmpty(cyclopsWorkflowFileName))
                {
                    mMessage = "Parameter CyclopsWorkflowName not defined in the job parameters for this job";
                    LogError(mMessage + "; unable to continue");
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                var proteinProphet = mJobParams.GetParam("RunProteinProphet");

                if (!string.IsNullOrEmpty(proteinProphet))
                {
                    // If User requests to run ProteinProphet
                    if (proteinProphet.ToLower().Equals("true"))
                    {
                        var proteinOptions = mJobParams.GetParam("ProteinOptions");

                        if (proteinOptions.Length > 0 && !proteinOptions.Equals("na", StringComparison.OrdinalIgnoreCase))
                        {
                            // Override the Protein Options to force forward direction only
                            mJobParams.SetParam("PeptideSearch", "ProteinOptions", "seq_direction=forward,filetype=fasta");
                        }

                        // Generate the path FASTA File
                        var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");
                        if (!RetrieveOrgDB(orgDbDirectoryPath, out var resultCode))
                        {
                            mMessage = "Cyclops Resourcer failed to retrieve the path to the FASTA file to run ProteinProphet";
                            return resultCode;
                        }
                    }
                }

                var dmsWorkflowsFolderPath = mMgrParams.GetParam("DMSWorkflowsFolderPath", @"\\gigasax\DMS_Workflows");
                var remoteRScriptFolder = new DirectoryInfo(Path.Combine(dmsWorkflowsFolderPath, "Cyclops", "RScript"));

                if (!remoteRScriptFolder.Exists)
                {
                    mMessage = "R Script folder not found: " + remoteRScriptFolder.FullName;
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (mDebugLevel >= 2)
                    LogMessage("Copying FROM: " + remoteRScriptFolder.FullName);

                foreach (var rFile in remoteRScriptFolder.GetFiles("*.R"))
                {
                    if (mDebugLevel >= 3)
                        LogMessage("Copying " + rFile.Name + " to " + dirLocalRScriptFolder.FullName);

                    rFile.CopyTo(Path.Combine(dirLocalRScriptFolder.FullName, rFile.Name));
                }

                var cyclopsWorkflowDirectory = Path.Combine(dmsWorkflowsFolderPath, "Cyclops", analysisType);

                LogMessage("Retrieving workflow file: " + Path.Combine(cyclopsWorkflowDirectory, cyclopsWorkflowFileName));

                // Now copy the Cyclops workflow file to the working directory
                if (!CopyFileToWorkDir(cyclopsWorkflowFileName, cyclopsWorkflowDirectory, mWorkDir))
                {
                    // Errors were reported in function call, so just return
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                if (!CopyFileToWorkDir("Results.db3", Path.Combine(dataPackageFolderPath, sourceFolderName), mWorkDir))
                {
                    mMessage = "Results.db3 file from " + sourceFolderName + " failed to copy over to working directory";
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
