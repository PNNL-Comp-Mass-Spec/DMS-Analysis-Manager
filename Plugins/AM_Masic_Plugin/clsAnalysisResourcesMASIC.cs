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
            var CreateStoragePathInfoOnly = false;
            string RawDataType = m_jobParams.GetParam("RawDataType");
            string toolName = m_jobParams.GetParam("ToolName");

            switch (RawDataType.ToLower())
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
                    CreateStoragePathInfoOnly = false;
                    break;
                default:
                    CreateStoragePathInfoOnly = false;
                    break;
            }

            if (!FileSearch.RetrieveSpectra(RawDataType, CreateStoragePathInfoOnly))
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                    "clsAnalysisResourcesDecon2ls.GetResources: Error occurred retrieving spectra.");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (string.Compare(RawDataType, RAW_DATA_TYPE_DOT_RAW_FILES, true) == 0 && toolName.ToLower().StartsWith("MASIC_Finnigan".ToLower()))
            {
                var strRawFileName = DatasetName + ".raw";
                var strInputFilePath = ResolveStoragePath(m_WorkingDir, strRawFileName);

                if (string.IsNullOrWhiteSpace(strInputFilePath))
                {
                    // Unable to resolve the file path
                    m_message = "Could not find " + strRawFileName + " or " + strRawFileName + STORAGE_PATH_INFO_FILE_SUFFIX +
                                " in the working folder; unable to run MASIC";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Deprecated in December 2016
                //// Examine the size of the .Raw file
                //var fiInputFile = new System.IO.FileInfo(strInputFilePath);
                //
                //if (clsAnalysisToolRunnerMASICFinnigan.NeedToConvertRawToMzXML(fiInputFile))
                //{
                //    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Generating the ScanStats files from the .Raw file since it is over 2 GB (and MASIC will therefore process a .mzXML file)");
                //
                //    if (!GenerateScanStatsFile(deleteRawDataFile: false))
                //    {
                //        // Error message should already have been logged and stored in m_message
                //        return CloseOutType.CLOSEOUT_FAILED;
                //    }
                //}
            }

            // Add additional extensions to delete after the tool finishes
            m_jobParams.AddResultFileExtensionToSkip("_StoragePathInfo.txt");

            // We'll add the following extensions to m_FilesToDeleteExt
            // Note, though, that the DeleteDataFile function will delete the .Raw or .mgf/.cdf files
            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_WIFF_EXTENSION);
            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION);
            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_UIMF_EXTENSION);
            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MZXML_EXTENSION);

            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_MGF_EXTENSION);
            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_CDF_EXTENSION);

            //Retrieve param file
            if (!FileSearch.RetrieveFile(m_jobParams.GetParam("ParmFileName"), m_jobParams.GetParam("ParmFileStoragePath")))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        #endregion
    }
}
