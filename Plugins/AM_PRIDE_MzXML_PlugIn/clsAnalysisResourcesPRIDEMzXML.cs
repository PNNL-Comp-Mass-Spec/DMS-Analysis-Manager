using System.Collections.Generic;
using System.Linq;
using AnalysisManagerBase;

namespace AnalysisManagerPRIDEMzXMLPlugIn
{
    public class clsAnalysisResourcesPRIDEMzXML : clsAnalysisResources
    {
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            var fileSpecList = m_jobParams.GetParam("TargetJobFileList").Split(',').ToList();

            foreach (string fileSpec in fileSpecList.ToList())
            {
                var fileSpecTerms = fileSpec.Split(':').ToList();
                if (fileSpecTerms.Count <= 2 || !(fileSpecTerms[2].ToLower() == "copy"))
                {
                    m_jobParams.AddResultFileExtensionToSkip(fileSpecTerms[1]);
                }
            }

            LogMessage("Getting PRIDE MzXML Input file");

            if (!FileSearch.RetrieveFile(m_jobParams.GetParam("PRIDEMzXMLInputFile"), m_jobParams.GetParam("transferFolderPath")))
                return CloseOutType.CLOSEOUT_FAILED;

            m_jobParams.AddResultFileToSkip(m_jobParams.GetParam("PRIDEMzXMLInputFile"));

            LogMessage("Retrieving input files");

            Dictionary<int, clsDataPackageJobInfo> dctDataPackageJobs = null;

            if (!RetrieveAggregateFiles(fileSpecList, DataPackageFileRetrievalModeConstants.Undefined, out dctDataPackageJobs))
            {
                //Errors were reported in function call, so just return
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
