/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 08/08/2017                                           **
**                                                              **
*****************************************************************/

using System;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerUIMFtoMassHunterPlugin
{
    /// <summary>
    /// Retrieve resources for the UIMF to MassHunter plugin
    /// </summary>
    public class AnalysisResourcesUIMFtoMassHunter : AnalysisResources
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

                // Retrieve shared resources (likely none for this tool)
                var result = GetSharedResources();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                /*
                 * Future, if needed

                // Retrieve the parameter file
                currentTask = "Retrieve the parameter file";
                var paramFileName = mJobParams.GetParam("ParmFileName");
                var paramFileStoragePath = mJobParams.GetParam("ParmFileStoragePath");

                var success = FileSearch.RetrieveFile(paramFileName, paramFileStoragePath);
                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                 *
                 */

                // Retrieve the .UIMF file
                var rawDataTypeName = mJobParams.GetParam("rawDataType");

                // The ToolName job parameter holds the name of the job script we are executing
                // var scriptName = mJobParams.GetParam("ToolName");

                switch (rawDataTypeName.ToLower())
                {
                    case RAW_DATA_TYPE_DOT_UIMF_FILES:
                        // Valid dataset type
                        break;
                    default:
                        LogError("Dataset type not supported: " + rawDataTypeName);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!FileSearch.RetrieveSpectra(rawDataTypeName))
                {
                    LogDebug("AnalysisResourcesMASIC.GetResources: Error occurred retrieving spectra.");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (rawDataTypeName.ToLower() == RAW_DATA_TYPE_DOT_UIMF_FILES)
                {
                    // Valid dataset type
                    var uimfFileName = DatasetName + ".uimf";
                    var inputFilePath = ResolveStoragePath(mWorkDir, uimfFileName);

                    if (string.IsNullOrWhiteSpace(inputFilePath))
                    {
                        // Unable to resolve the file path
                        LogError("Could not find " + inputFilePath + " or " + inputFilePath + STORAGE_PATH_INFO_FILE_SUFFIX +
                                 " in the working folder; unable to run the UIMFtoMassHunter converter");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
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