//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 05/23/2014
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System.IO;

namespace AnalysisManagerDeconPeakDetectorPlugIn
{

    /// <summary>
    /// Retrieve resources for the Decon Peak Detector plugin
    /// </summary>
    public class clsAnalysisResourcesDeconPeakDetector : clsAnalysisResources
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

            var strRawDataType = mJobParams.GetJobParameter("RawDataType", "");

            // Retrieve the peak detector parameter file

            var peakDetectorParamFileName = mJobParams.GetJobParameter("PeakDetectorParamFile", "");
            var paramFileStoragePath = mJobParams.GetParam("ParmFileStoragePath");

            paramFileStoragePath = Path.Combine(paramFileStoragePath, "PeakDetection");

            if (!FileSearch.RetrieveFile(peakDetectorParamFileName, paramFileStoragePath))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Retrieve the instrument data file
            if (!FileSearch.RetrieveSpectra(strRawDataType))
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Error retrieving instrument data file";
                }

                LogDebug("clsDtaGenResources.GetResources: " + mMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!base.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
