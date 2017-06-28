using AnalysisManagerBase;
using MyEMSLReader;

namespace AnalysisManagerDtaSplitPlugIn
{
    public class clsAnalysisResourcesDtaSplit : clsAnalysisResources
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

            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
            if (!FileSearch.RetrieveDtaFiles())
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!ProcessMyEMSLDownloadQueue(m_WorkingDir, Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Add all the extensions of the files to delete after run
            m_jobParams.AddResultFileExtensionToSkip("_dta.zip"); // Zipped DTA
            m_jobParams.AddResultFileExtensionToSkip("_dta.txt"); // Unzipped, concatenated DTA

            // All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        #endregion
    }
}
