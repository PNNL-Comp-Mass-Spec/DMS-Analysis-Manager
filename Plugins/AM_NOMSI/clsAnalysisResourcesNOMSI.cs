/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 04/29/2015                                           **
**                                                              **
*****************************************************************/

using System;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerNOMSIPlugin
{
    public class clsAnalysisResourcesNOMSI : clsAnalysisResources
    {
        public override IJobParams.CloseOutType GetResources()
        {

            var currentTask = "Initializing";

            try
            {
                // Retrieve the parameter file
                currentTask = "Retrieve the parameter file";
                var paramFileName = m_jobParams.GetParam("ParmFileName");
                var paramFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");

                if (!RetrieveFile(paramFileName, paramFileStoragePath))
                {
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // Retrieve the targets file
                currentTask = "Retrieve the targets file";
                var targetsFileName = m_jobParams.GetParam("dm_target_file");
                if (string.IsNullOrWhiteSpace(targetsFileName))
                {
                    LogError("Parameter dm_target_file not found in the settings file");
                    return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                currentTask = "Retrieve the transformations file";
                paramFileStoragePath = Path.Combine(paramFileStoragePath, "Transformations");
                if (!Directory.Exists(paramFileStoragePath))
                {
                    LogError("Transformations folder not found", "Transformations folder not found: " + paramFileStoragePath);
                    return IJobParams.CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                if (!RetrieveFile(targetsFileName, paramFileStoragePath))
                {
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // Retrieve the zip file that has the XML files from the Bruker_Data_Analysis step
                currentTask = "Retrieve the Bruker_Data_Analysis _scans.zip file";
                var fileToGet = m_DatasetName + "_scans.zip";

                if (!FindAndRetrieveMiscFiles(fileToGet, false))
                {
                    // Errors should have already been logged
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }
                m_jobParams.AddResultFileToSkip(fileToGet);

                currentTask = "Process the MyEMSL download queue";
                if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;

            }
            catch (Exception ex)
            {
                m_message = "Exception in GetResources: " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     m_message + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));

                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

        }
    }
}
