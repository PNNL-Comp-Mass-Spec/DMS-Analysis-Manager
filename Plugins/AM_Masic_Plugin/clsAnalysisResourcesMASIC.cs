using System;
using AnalysisManagerBase;

namespace AnalysisManagerMasicPlugin
{
    public class clsAnalysisResourcesMASIC : clsAnalysisResources
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

            // Get input data file
            bool createStoragePathInfoOnly;
            var rawDataType = m_jobParams.GetParam("rawDataType");
            var toolName = m_jobParams.GetParam("ToolName");

            switch (rawDataType.ToLower())
            {
                case RAW_DATA_TYPE_DOT_RAW_FILES:
                case RAW_DATA_TYPE_DOT_WIFF_FILES:
                case RAW_DATA_TYPE_DOT_UIMF_FILES:
                case RAW_DATA_TYPE_DOT_MZXML_FILES:
                case RAW_DATA_TYPE_DOT_D_FOLDERS:
                    // If desired, set the following to True to not actually copy the .Raw
                    // (or .wiff, .uimf, etc.) file locally, and instead determine where it is
                    // located, then create a text file named "DatesetName.raw_StoragePathInfo.txt"
                    // This file would contain just one line of text: the full path to the actual file

                    // However, we have found that this can create undo strain on the storage servers (or NWFS Archive)
                    // Thus, we are now setting this to False
                    createStoragePathInfoOnly = false;
                    break;
                default:
                    createStoragePathInfoOnly = false;
                    break;
            }

            if (!FileSearch.RetrieveSpectra(rawDataType, createStoragePathInfoOnly))
            {
                LogDebug("clsAnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (string.Compare(rawDataType, RAW_DATA_TYPE_DOT_RAW_FILES, StringComparison.OrdinalIgnoreCase) == 0 &&
                toolName.ToLower().StartsWith("MASIC_Finnigan".ToLower()))
            {
                var strRawFileName = DatasetName + ".raw";
                var strInputFilePath = ResolveStoragePath(m_WorkingDir, strRawFileName);

                if (string.IsNullOrWhiteSpace(strInputFilePath))
                {
                    // Unable to resolve the file path
                    m_message = "Could not find " + strRawFileName + " or " + strRawFileName + STORAGE_PATH_INFO_FILE_SUFFIX +
                                " in the working folder; unable to run MASIC";
                    LogError(m_message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

            }

            // Add additional extensions to delete after the tool finishes
            m_jobParams.AddResultFileExtensionToSkip("_StoragePathInfo.txt");

            // We'll add the following extensions to m_FilesToDeleteExt
            // Note, though, that the DeleteDataFile function will delete the .Raw or .mgf/.cdf files
            m_jobParams.AddResultFileExtensionToSkip(DOT_WIFF_EXTENSION);
            m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
            m_jobParams.AddResultFileExtensionToSkip(DOT_UIMF_EXTENSION);
            m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);

            m_jobParams.AddResultFileExtensionToSkip(DOT_MGF_EXTENSION);
            m_jobParams.AddResultFileExtensionToSkip(DOT_CDF_EXTENSION);

            // Retrieve param file
            if (!FileSearch.RetrieveFile(m_jobParams.GetParam("ParmFileName"), m_jobParams.GetParam("ParmFileStoragePath")))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        #endregion
    }
}
