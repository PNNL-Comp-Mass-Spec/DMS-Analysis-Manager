using System;

using AnalysisManagerBase;
using System.IO;

namespace AnalysisManagerMzRefineryPlugIn
{

    /// <summary>
    /// Retrieve resources for the MzRefinery plugin
    /// </summary>
    public class clsAnalysisResourcesMzRefinery : clsAnalysisResources
    {

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, clsMyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieves files necessary for running MzRefinery
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType GetResources()
        {
            var currentTask = "Initializing";

            try
            {
                currentTask = "Retrieve shared resources";

                // Retrieve shared resources, including the JobParameters file from the previous job step
                GetSharedResources();

                var mzRefParamFile = m_jobParams.GetJobParameter("MzRefParamFile", string.Empty);
                if (string.IsNullOrEmpty(mzRefParamFile))
                {
                    LogError("MzRefParamFile parameter is empty");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                currentTask = "Get Input file";

                var eResult = GetMsXmlFile();

                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return eResult;
                }

                // Retrieve the Fasta file
                var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");

                currentTask = "RetrieveOrgDB to " + localOrgDbFolder;

                if (!RetrieveOrgDB(localOrgDbFolder))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Retrieve the Mz Refinery parameter file
                currentTask = "Retrieve the Mz Refinery parameter file " + mzRefParamFile;

                const string paramFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "Mz_Refinery";

                var mzRefineryParmFileStoragePath = m_mgrParams.GetParam(paramFileStoragePathKeyName);
                if (string.IsNullOrWhiteSpace(mzRefineryParmFileStoragePath))
                {
                    mzRefineryParmFileStoragePath = @"\\gigasax\dms_parameter_Files\MzRefinery";
                    LogWarning("Parameter '" + paramFileStoragePathKeyName + "' is not defined " +
                        "(obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); " +
                        "will assume: " + mzRefineryParmFileStoragePath);
                }

                if (!FileSearch.RetrieveFile(mzRefParamFile, mzRefineryParmFileStoragePath))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Look for existing MSGF+ results in the transfer folder
                currentTask = "Find existing MSGF+ results";

                if (!FindExistingMSGFPlusResults(mzRefParamFile))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception in GetResources: " + ex.Message;
                LogError(m_message + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Check for existing MSGF+ results in the transfer directory
        /// </summary>
        /// <returns>True if no errors, false if a problem</returns>
        /// <remarks>Will retrun True even if existing results are not found</remarks>
        private bool FindExistingMSGFPlusResults(string mzRefParamFileName)
        {
            var resultsFolderName = m_jobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME);
            var transferFolderPath = m_jobParams.GetParam(JOB_PARAM_TRANSFER_FOLDER_PATH);

            if (string.IsNullOrWhiteSpace(resultsFolderName))
            {
                m_message = "Results folder not defined (job parameter OutputFolderName)";
                LogError(m_message);
                return false;
            }

            if (string.IsNullOrWhiteSpace(transferFolderPath))
            {
                m_message = "Transfer folder not defined (job parameter transferFolderPath)";
                LogError(m_message);
                return false;
            }

            var diTransferFolder = new DirectoryInfo(Path.Combine(transferFolderPath, DatasetName, resultsFolderName));
            if (!diTransferFolder.Exists)
            {
                // This is not an error -- it just means there are no existing MSGF+ results to use
                return true;
            }

            // Look for the required files in the transfer folder
            var resultsFileName = DatasetName + clsAnalysisToolRunnerMzRefinery.MSGFPLUS_MZID_SUFFIX + ".gz";
            var fiMSGFPlusResults = new FileInfo(Path.Combine(diTransferFolder.FullName, resultsFileName));

            if (!fiMSGFPlusResults.Exists)
            {
                // This is not an error -- it just means there are no existing MSGF+ results to use
                return true;
            }

            var fiMSGFPlusConsoleOutput = new FileInfo(Path.Combine(diTransferFolder.FullName, "MSGFPlus_ConsoleOutput.txt"));
            if (!fiMSGFPlusResults.Exists)
            {
                // This is unusual; typically if the mzid.gz file exists there should be a ConsoleOutput file
                LogWarning("Found " + fiMSGFPlusResults.FullName + " but did not find " + fiMSGFPlusConsoleOutput.Name + "; will re-run MSGF+");
                return true;
            }

            var fiMzRefParamFile = new FileInfo(Path.Combine(diTransferFolder.FullName, mzRefParamFileName));
            if (!fiMzRefParamFile.Exists)
            {
                // This is unusual; typically if the mzid.gz file exists there should be a MzRefinery parameter file
                LogWarning("Found " + fiMSGFPlusResults.FullName + " but did not find " + fiMzRefParamFile.Name + "; will re-run MSGF+");
                return true;
            }

            // Compare the remote parameter file and the local one to make sure they match
            if (!clsGlobal.TextFilesMatch(fiMzRefParamFile.FullName, Path.Combine(m_WorkingDir, mzRefParamFileName), true))
            {
                LogMessage("MzRefinery parameter file in transfer folder does not match the official MzRefinery paramter file; will re-run MSGF+");
                return true;
            }

            // Existing results found
            // Copy the MSGF+ results locally
            var localFilePath = Path.Combine(m_WorkingDir, fiMSGFPlusResults.Name);
            fiMSGFPlusResults.CopyTo(localFilePath, true);

            GUnzipFile(localFilePath);

            localFilePath = Path.Combine(m_WorkingDir, fiMSGFPlusConsoleOutput.Name);
            fiMSGFPlusConsoleOutput.CopyTo(localFilePath, true);

            LogMessage("Found existing MSGF+ results to use for MzRefinery");

            return true;
        }

    }
}
