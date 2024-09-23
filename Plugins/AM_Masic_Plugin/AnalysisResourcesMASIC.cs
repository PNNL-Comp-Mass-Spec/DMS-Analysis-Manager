using AnalysisManagerBase;
using System;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMasicPlugin
{
    // ReSharper disable once UnusedMember.Global

    /// <summary>
    /// Retrieve resources for the MASIC plugin
    /// </summary>
    public class AnalysisResourcesMASIC : AnalysisResources
    {
        // Ignore Spelling: MASIC

        /// <summary>
        /// Retrieves files necessary for MASIC
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Get input data file
            bool createStoragePathInfoOnly;
            var rawDataTypeName = mJobParams.GetParam("rawDataType");

            // The ToolName job parameter holds the name of the job script we are executing
            var scriptName = mJobParams.GetParam("ToolName");

            switch (rawDataTypeName.ToLower())
            {
                case RAW_DATA_TYPE_DOT_RAW_FILES:
                case RAW_DATA_TYPE_DOT_WIFF_FILES:
                case RAW_DATA_TYPE_DOT_UIMF_FILES:
                case RAW_DATA_TYPE_DOT_MZXML_FILES:
                case RAW_DATA_TYPE_DOT_D_FOLDERS:
                    // If desired, set the following to true to not actually copy the .Raw
                    // (or .wiff, .uimf, etc.) file locally, and instead determine where it is
                    // located, then create a text file named "DatasetName.raw_StoragePathInfo.txt"
                    // This file would contain just one line of text: the full path to the actual file

                    // However, we have found that this can create undo strain on the storage servers
                    // Thus, we are now setting this to false
                    createStoragePathInfoOnly = false;
                    break;

                default:
                    createStoragePathInfoOnly = false;
                    break;
            }

            if (!FileSearchTool.RetrieveSpectra(rawDataTypeName, createStoragePathInfoOnly))
            {
                LogDebug("AnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!base.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (Global.IsMatch(rawDataTypeName, RAW_DATA_TYPE_DOT_RAW_FILES) &&
                scriptName.StartsWith("MASIC_Finnigan", StringComparison.OrdinalIgnoreCase))
            {
                var rawFileName = DatasetName + ".raw";
                var inputFilePath = ResolveStoragePath(mWorkDir, rawFileName);

                if (string.IsNullOrWhiteSpace(inputFilePath))
                {
                    // Unable to resolve the file path
                    mMessage = "Could not find " + rawFileName + " or " + rawFileName + STORAGE_PATH_INFO_FILE_SUFFIX +
                                " in the working folder; unable to run MASIC";
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Add additional extensions to delete after the tool finishes
            mJobParams.AddResultFileExtensionToSkip("_StoragePathInfo.txt");

            // We'll add the following extensions to FilesToDeleteExt
            // Note, though, that the DeleteDataFile method will delete the .Raw or .mgf/.cdf files
            mJobParams.AddResultFileExtensionToSkip(DOT_WIFF_EXTENSION);
            mJobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
            mJobParams.AddResultFileExtensionToSkip(DOT_UIMF_EXTENSION);
            mJobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);

            mJobParams.AddResultFileExtensionToSkip(DOT_MGF_EXTENSION);
            mJobParams.AddResultFileExtensionToSkip(DOT_CDF_EXTENSION);

            // Retrieve param file
            if (!FileSearchTool.RetrieveFile(mJobParams.GetParam("ParamFileName"), mJobParams.GetParam("ParamFileStoragePath")))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
