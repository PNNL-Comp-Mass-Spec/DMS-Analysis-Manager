//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 10/12/2011
//
//*********************************************************************************************************

using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMSDeconvPlugIn
{
    /// <summary>
    /// Retrieve resources for the MSDeconv plugin
    /// </summary>
    public class AnalysisResourcesMSDeconv : AnalysisResources
    {
        // Ignore Spelling: MSDeconv

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
                mInsufficientFreeMemory = true;
                mMessage = "Not enough free memory to run MSDeconv";
                return CloseOutType.CLOSEOUT_RESET_JOB_STEP_INSUFFICIENT_MEMORY;
            }

            LogMessage("Getting mzXML file");

            const bool unzipFile = true;

            var success = FileSearchTool.RetrieveCachedMzXMLFile(unzipFile, out var errorMessage, out var fileMissingFromCache, out _);

            if (!success)
            {
                return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_MZXML_EXTENSION);
            }

            // Make sure we don't move the .mzXML file into the results folder
            mJobParams.AddResultFileExtensionToSkip(DOT_MZXML_EXTENSION);

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
