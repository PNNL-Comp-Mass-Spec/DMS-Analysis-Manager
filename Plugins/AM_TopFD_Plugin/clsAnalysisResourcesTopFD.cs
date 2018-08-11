//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using AnalysisManagerBase;

namespace AnalysisManagerTopFDPlugIn
{
    /// <summary>
    /// Retrieve resources for the TopFD plugin
    /// </summary>
    public class clsAnalysisResourcesTopFD : clsAnalysisResources
    {

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

            LogMessage("Getting mzML file");

            const bool unzipFile = true;

            var success = FileSearch.RetrieveCachedMzMLFile(unzipFile, out var errorMessage, out var fileMissingFromCache, out _);
            if (!success)
            {
                return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_MZML_EXTENSION);
            }

            // Make sure we don't move the .mzML file into the results folder
            m_jobParams.AddResultFileExtensionToSkip(DOT_MZML_EXTENSION);

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
