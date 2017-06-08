//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 05/23/2014
//
//*********************************************************************************************************

using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerDeconPeakDetectorPlugIn
{
    public class clsAnalysisResourcesDeconPeakDetector : clsAnalysisResources
    {
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            var strRawDataType = m_jobParams.GetJobParameter("RawDataType", "");

            // Retrieve the peak detector parameter file

            var peakDetectorParamFileName = m_jobParams.GetJobParameter("PeakDetectorParamFile", "");
            var paramFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");

            paramFileStoragePath = Path.Combine(paramFileStoragePath, "PeakDetection");

            if (!FileSearch.RetrieveFile(peakDetectorParamFileName, paramFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve the instrument data file
            if (!FileSearch.RetrieveSpectra(strRawDataType))
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Error retrieving instrument data file";
                }

                LogDebug("clsDtaGenResources.GetResources: " + m_message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
