//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 03/30/2011
//
//*********************************************************************************************************

using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMsXmlBrukerPlugIn
{
    /// <summary>
    /// Retrieve resources for the MSXml Bruker plugin
    /// </summary>
    public class AnalysisResourcesMSXMLBruker : AnalysisResources
    {
        // Ignore Spelling: Bruker

        /// <summary>
        /// Retrieves files necessary for creating the .mzXML file
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Get input data file
            var rawDataTypeName = mJobParams.GetParam("RawDataType");
            var rawDataType = GetRawDataType(rawDataTypeName);

            switch (rawDataType)
            {
                case RawDataTypeConstants.BrukerFTFolder:
                case RawDataTypeConstants.BrukerTOFBaf:
                    break;
                // This dataset type is acceptable
                default:
                    mMessage = "Dataset type " + rawDataType + " is not supported";
                    LogDebug(
                        "DtaGenResources.GetResources: " + mMessage + "; must be " + RAW_DATA_TYPE_BRUKER_FT_FOLDER + " or " +
                        RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER);

                    return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!FileSearchTool.RetrieveSpectra(rawDataTypeName))
            {
                LogDebug("DtaGenResources.GetResources: Error occurred retrieving spectra.");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.SingleDataset))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
