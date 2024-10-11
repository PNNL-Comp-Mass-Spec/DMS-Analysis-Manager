/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 04/29/2015                                           **
**                                                              **
*****************************************************************/

using System;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerNOMSIPlugin
{
    /// <summary>
    /// Retrieve resources for the NOMSI plugin
    /// </summary>
    public class AnalysisResourcesNOMSI : AnalysisResources
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
                var paramFileName = mJobParams.GetParam(JOB_PARAM_PARAMETER_FILE);
                var paramFileStoragePath = mJobParams.GetParam("ParamFileStoragePath");

                if (!FileSearchTool.RetrieveFile(paramFileName, paramFileStoragePath))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Retrieve the targets file
                currentTask = "Retrieve the targets file";
                var targetsFileName = mJobParams.GetParam("dm_target_file");

                if (string.IsNullOrWhiteSpace(targetsFileName))
                {
                    LogError("Parameter dm_target_file not found in the settings file");
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                currentTask = "Retrieve the transformations file";
                paramFileStoragePath = Path.Combine(paramFileStoragePath, "Transformations");

                if (!Directory.Exists(paramFileStoragePath))
                {
                    LogError("Transformations folder not found", "Transformations folder not found: " + paramFileStoragePath);
                    return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                }

                if (!FileSearchTool.RetrieveFile(targetsFileName, paramFileStoragePath))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Retrieve the zip file that has the XML files from the Bruker_Data_Analysis step
                currentTask = "Retrieve the Bruker_Data_Analysis _scans.zip file";
                var fileToGet = DatasetName + "_scans.zip";

                if (!FileSearchTool.FindAndRetrieveMiscFiles(fileToGet, false))
                {
                    // Errors should have already been logged
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                mJobParams.AddResultFileToSkip(fileToGet);

                currentTask = "Process the MyEMSL download queue";

                if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in GetResources: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + Global.GetExceptionStackTrace(ex));

                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
