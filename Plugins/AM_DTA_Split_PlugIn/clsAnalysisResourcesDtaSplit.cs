using AnalysisManagerBase;
using MyEMSLReader;

namespace AnalysisManagerDtaSplitPlugIn
{
    /// <summary>
    /// Retrieve resources for the DTA Split plugin
    /// </summary>
    public class clsAnalysisResourcesDtaSplit : clsAnalysisResources
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

            // Retrieve the _DTA.txt file
            // Note that if the file was found in MyEMSL then RetrieveDtaFiles will auto-call ProcessMyEMSLDownloadQueue to download the file
            if (!FileSearch.RetrieveDtaFiles())
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Add all the extensions of the files to delete after run
            mJobParams.AddResultFileExtensionToSkip("_dta.zip"); // Zipped DTA
            mJobParams.AddResultFileExtensionToSkip("_dta.txt"); // Unzipped, concatenated DTA

            // All finished
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

    }
}
