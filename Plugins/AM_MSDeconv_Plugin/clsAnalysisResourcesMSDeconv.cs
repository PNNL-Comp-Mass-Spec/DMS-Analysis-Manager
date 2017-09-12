//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 10/12/2011
//
//*********************************************************************************************************

using AnalysisManagerBase;

namespace AnalysisManagerMSDeconvPlugIn
{
    /// <summary>
    /// Retrieve resources for the MSDeconv plugin
    /// </summary>
    public class clsAnalysisResourcesMSDeconv : clsAnalysisResources
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

            // Make sure the machine has enough free memory to run MSDeconv
            if (!ValidateFreeMemorySize("MSDeconvJavaMemorySize"))
            {
                m_message = "Not enough free memory to run MSDeconv";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            LogMessage("Getting mzXML file");

            // var eResult = GetMzXMLFile();
            // if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            // {
            //    return eResult;
            // }

            string errorMessage;
            bool fileMissingFromCache;
            const bool unzipFile = true;

            var success = FileSearch.RetrieveCachedMzXMLFile(unzipFile, out errorMessage, out fileMissingFromCache);
            if (!success)
            {
                return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_MZXML_EXTENSION);
            }

            // Make sure we don't move the .mzXML file into the results folder
            m_jobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
