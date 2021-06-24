using System;
using System.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerPRIDEMzXMLPlugIn
{
    /// <summary>
    /// Retrieve resources for the PRIDE MzXML plugin
    /// </summary>
    public class AnalysisResourcesPRIDEMzXML : AnalysisResources
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

            var fileSpecList = mJobParams.GetParam("TargetJobFileList").Split(',').ToList();

            foreach (var fileSpec in fileSpecList.ToList())
            {
                var fileSpecTerms = fileSpec.Split(':').ToList();
                if (fileSpecTerms.Count <= 2 || !string.Equals(fileSpecTerms[2].ToLower(), "copy", StringComparison.OrdinalIgnoreCase))
                {
                    mJobParams.AddResultFileExtensionToSkip(fileSpecTerms[1]);
                }
            }

            LogMessage("Getting PRIDE MzXML Input file");

            if (!FileSearch.RetrieveFile(mJobParams.GetParam("PRIDEMzXMLInputFile"), mJobParams.GetParam(JOB_PARAM_TRANSFER_DIRECTORY_PATH)))
                return CloseOutType.CLOSEOUT_FAILED;

            mJobParams.AddResultFileToSkip(mJobParams.GetParam("PRIDEMzXMLInputFile"));

            LogMessage("Retrieving input files");
            const bool callingMethodCanRegenerateMissingFile = true;

            if (!RetrieveAggregateFiles(fileSpecList, DataPackageFileRetrievalModeConstants.Undefined, callingMethodCanRegenerateMissingFile, out _))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
