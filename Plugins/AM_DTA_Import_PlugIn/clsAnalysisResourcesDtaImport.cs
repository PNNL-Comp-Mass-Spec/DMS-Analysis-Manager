using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerDtaImportPlugIn
{
    public class clsAnalysisResourcesDtaImport : clsAnalysisResources
    {
        #region "Methods"

        /// <summary>
        /// Retrieves files necessary for performance of Sequest analysis
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        /// <remarks></remarks>
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
            string SourceFolderNamePath = string.Empty;
            try
            {
                // Note: the DTAFolderLocation is defined in the Manager_Control DB, and is specific for this manager
                //       for example: \\pnl\projects\MSSHARE\SPurvine
                // This folder must contain subfolders whose name matches the output_folder name assigned to each job
                // Furthermore, each subfolder must have a file named Dataset_dta.zip

                SourceFolderNamePath = Path.Combine(m_mgrParams.GetParam("DTAFolderLocation"), m_jobParams.GetParam("OutputFolderName"));

                //Determine if Dta folder in source directory exists
                if (!Directory.Exists(SourceFolderNamePath))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN,
                        "Source Directory for Manually created Dta does not exist: " + SourceFolderNamePath);
                    return CloseOutType.CLOSEOUT_FAILED;
                    //TODO: Handle errors
                }

                string zipFileName = DatasetName + "_dta.zip";
                string[] fileEntries = Directory.GetFiles(SourceFolderNamePath, zipFileName);

                // Process the list of files found in the directory.
                if (fileEntries.Length < 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN,
                        "DTA zip file was not found in source directory: " + Path.Combine(SourceFolderNamePath, zipFileName));
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                //If valid zip file is found, then uzip the contents
                foreach (string fileName in fileEntries)
                {
                    if (UnzipFileStart(Path.Combine(m_WorkingDir, fileName), m_WorkingDir, "clsAnalysisResourcesDtaImport.ValidateDTA", false))
                    {
                        if (m_DebugLevel >= 1)
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Manual DTA file unzipped");
                        }
                    }
                    else
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN,
                            "An error occurred while unzipping the DTA file: " + Path.Combine(SourceFolderNamePath, zipFileName));
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                string txtFileName = DatasetName + "_dta.txt";
                fileEntries = Directory.GetFiles(m_WorkingDir, txtFileName);
                if (fileEntries.Length < 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN,
                        "DTA text file in the zip file was named incorrectly or not valid: " + Path.Combine(SourceFolderNamePath, txtFileName));
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN,
                    "An exception occurred while validating manually created DTA zip file. " + SourceFolderNamePath + " : " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        #endregion
    }
}
