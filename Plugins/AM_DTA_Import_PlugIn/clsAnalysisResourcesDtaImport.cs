using AnalysisManagerBase;
using System;
using System.IO;

namespace AnalysisManagerDtaImportPlugIn
{
    /// <summary>
    /// Retrieve resources for the DTA Import plugin
    /// </summary>
    public class clsAnalysisResourcesDtaImport : clsAnalysisResources
    {
        // Ignore Spelling: dta, pnl

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // There are really no resources to get, so just clear the list of files to delete or keep and validate zip file
            result = ValidateDTA();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType ValidateDTA()
        {
            try
            {
                // Note: the DTAFolderLocation is defined in the Manager_Control DB, and is specific for this manager
                //       for example: \\pnl\projects\MSSHARE\SPurvine
                // This folder must contain subdirectories whose name matches the output_folder name assigned to each job
                // Furthermore, each subdirectory must have a file named Dataset_dta.zip

                var sourceFolderNamePath = Path.Combine(mMgrParams.GetParam("DTAFolderLocation"), mJobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME));

                // Determine if Dta folder in source directory exists
                if (!Directory.Exists(sourceFolderNamePath))
                {
                    mMessage = "Source Directory for Manually created Dta does not exist";
                    LogErrorToDatabase(mMessage + ": " + sourceFolderNamePath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var zipFileName = DatasetName + "_dta.zip";
                var fileEntries = Directory.GetFiles(sourceFolderNamePath, zipFileName);

                // Process the list of files found in the directory.
                if (fileEntries.Length < 1)
                {
                    mMessage = "DTA zip file was not found in source directory";
                    LogErrorToDatabase(mMessage + ": " + Path.Combine(sourceFolderNamePath, zipFileName));
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // If valid zip file is found, unzip the contents
                foreach (var fileName in fileEntries)
                {
                    if (UnzipFileStart(Path.Combine(mWorkDir, fileName), mWorkDir, "clsAnalysisResourcesDtaImport.ValidateDTA", false))
                    {
                        if (mDebugLevel >= 1)
                        {
                            LogMessage("Manual DTA file unzipped");
                        }
                    }
                    else
                    {
                        LogError("An error occurred while unzipping the DTA file");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                var txtFileName = DatasetName + "_dta.txt";
                fileEntries = Directory.GetFiles(mWorkDir, txtFileName);
                if (fileEntries.Length < 1)
                {
                    LogError("DTA text file in the zip file was named incorrectly or not valid");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                LogError("An exception occurred while validating manually created DTA zip file", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

    }
}
