/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 05/24/2018                                           **
**                                                              **
*****************************************************************/

using System;
using System.IO;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerFormularityPlugin
{
    /// <summary>
    /// Retrieve resources for the Formularity plugin
    /// </summary>
    class clsAnalysisResourcesFormularity : clsAnalysisResources
    {

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {

            var currentTask = "Initializing";

            try
            {
                currentTask = "Retrieve shared resources";

                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                // Retrieve the parameter file
                currentTask = "Retrieve the parameter file";
                var paramFileName = m_jobParams.GetParam(JOB_PARAM_PARAMETER_FILE);
                var paramFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");

                if (!FileSearch.RetrieveFile(paramFileName, paramFileStoragePath))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Retrieve the database
                currentTask = "Retrieve the CIA database";
                var databaseFileName = m_jobParams.GetParam("cia_db_name");
                if (string.IsNullOrWhiteSpace(databaseFileName))
                {
                    LogError("Parameter cia_db_name not found in the settings file");
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                var sourceDirectory = new DirectoryInfo(Path.Combine(paramFileStoragePath, "CIA_DB"));
                if (!sourceDirectory.Exists)
                {
                    LogError("CIA database directory not found: " + sourceDirectory.FullName);
                    return CloseOutType.CLOSEOUT_NO_FAS_FILES;
                }

                var fileSyncUtil = new FileSyncUtils(m_FileTools);
                RegisterEvents(fileSyncUtil);

                var remoteCiaDbPath = Path.Combine(sourceDirectory.FullName, databaseFileName);
                var orgDbDirectory = m_mgrParams.GetParam(MGR_PARAM_ORG_DB_DIR);
                if (string.IsNullOrWhiteSpace(orgDbDirectory))
                {
                    LogError(string.Format("Manager parameter {0} is not defined", MGR_PARAM_ORG_DB_DIR));
                    return CloseOutType.CLOSEOUT_NO_FAS_FILES;
                }

                var recheckIntervalDays = 7;
                var ciaDbIsValid = fileSyncUtil.CopyFileToLocal(remoteCiaDbPath, orgDbDirectory, out var errorMessage, recheckIntervalDays);

                if (!ciaDbIsValid)
                {
                    if (string.IsNullOrEmpty(errorMessage))
                        LogError("Error copying remote CIA database locally");
                    else
                        LogError("Error copying remote CIA database locally: " + errorMessage);

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Also copy the _VerifiedNoDuplicates.txt file if it exists
                var noDuplicatesFile = Path.GetFileNameWithoutExtension(databaseFileName) + "_VerifiedNoDuplicates.txt";
                var remoteNoDuplicatesFile = new FileInfo(Path.Combine(sourceDirectory.FullName, noDuplicatesFile));
                var localNoDuplicatesFile = new FileInfo(Path.Combine(orgDbDirectory, noDuplicatesFile));

                if (!localNoDuplicatesFile.Exists && remoteNoDuplicatesFile.Exists)
                {
                    remoteNoDuplicatesFile.CopyTo(localNoDuplicatesFile.FullName, true);
                }

                // Retrieve the zip file that has the XML files from the Bruker_Data_Analysis step
                currentTask = "Retrieve the Bruker_Data_Analysis _scans.zip file";
                var fileToGet = DatasetName + "_scans.zip";

                if (!FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
                {
                    // Errors should have already been logged
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                m_jobParams.AddResultFileToSkip(fileToGet);

                currentTask = "Process the MyEMSL download queue";
                if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;

            }
            catch (Exception ex)
            {
                m_message = "Exception in GetResources: " + ex.Message;
                LogError(m_message + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));

                return CloseOutType.CLOSEOUT_FAILED;
            }

        }
    }
}
