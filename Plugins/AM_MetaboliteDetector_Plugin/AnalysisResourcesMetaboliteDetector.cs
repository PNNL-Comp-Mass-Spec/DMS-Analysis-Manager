/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 09/23/2016                                           **
**                                                              **
*****************************************************************/

using AnalysisManagerBase;
using System;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMetaboliteDetectorPlugin
{
    /// <summary>
    /// Retrieve resources for the Metabolite Detector plugin
    /// </summary>
    public class AnalysisResourcesMetaboliteDetector : AnalysisResources
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
                var paramFileName = mJobParams.GetParam("ParmFileName");
                var paramFileStoragePath = mJobParams.GetParam("ParmFileStoragePath");

                var success = FileSearch.RetrieveFile(paramFileName, paramFileStoragePath);
                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                currentTask = "Process the MyEMSL download queue";

                success = ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories);
                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Exception in GetResources; task = " + currentTask, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
