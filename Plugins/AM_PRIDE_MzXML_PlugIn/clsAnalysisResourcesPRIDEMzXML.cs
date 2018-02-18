using System;
using System.Linq;
using AnalysisManagerBase;

namespace AnalysisManagerPRIDEMzXMLPlugIn
{
    /// <summary>
    /// Retrieve resources for the PRIDE MzXML plugin
    /// </summary>
    public class clsAnalysisResourcesPRIDEMzXML : clsAnalysisResources
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

            var fileSpecList = m_jobParams.GetParam("TargetJobFileList").Split(',').ToList();

            foreach (var fileSpec in fileSpecList.ToList())
            {
                var fileSpecTerms = fileSpec.Split(':').ToList();
                if (fileSpecTerms.Count <= 2 || !string.Equals(fileSpecTerms[2].ToLower(), "copy", StringComparison.OrdinalIgnoreCase))
                {
                    m_jobParams.AddResultFileExtensionToSkip(fileSpecTerms[1]);
                }
            }

            LogMessage("Getting PRIDE MzXML Input file");

            if (!FileSearch.RetrieveFile(m_jobParams.GetParam("PRIDEMzXMLInputFile"), m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH)))
                return CloseOutType.CLOSEOUT_FAILED;

            m_jobParams.AddResultFileToSkip(m_jobParams.GetParam("PRIDEMzXMLInputFile"));

            LogMessage("Retrieving input files");

            if (!RetrieveAggregateFiles(fileSpecList, DataPackageFileRetrievalModeConstants.Undefined, out _))
            {
                // Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
