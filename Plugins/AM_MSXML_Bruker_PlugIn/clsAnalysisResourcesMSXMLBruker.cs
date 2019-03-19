//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 03/30/2011
//
//*********************************************************************************************************

using AnalysisManagerBase;

namespace AnalysisManagerMsXmlBrukerPlugIn
{
    /// <summary>
    /// Retrieve resources for the MSXml Bruker plugin
    /// </summary>
    public class clsAnalysisResourcesMSXMLBruker : clsAnalysisResources
    {
        #region "Methods"

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
            var rawDataType = mJobParams.GetParam("RawDataType");
            var eRawDataType = GetRawDataType(rawDataType);

            switch (eRawDataType)
            {
                case eRawDataTypeConstants.BrukerFTFolder:
                case eRawDataTypeConstants.BrukerTOFBaf:
                    break;
                // This dataset type is acceptable
                default:
                    mMessage = "Dataset type " + rawDataType + " is not supported";
                    LogDebug(
                        "clsDtaGenResources.GetResources: " + mMessage + "; must be " + RAW_DATA_TYPE_BRUKER_FT_FOLDER + " or " +
                        RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER);

                    return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!FileSearch.RetrieveSpectra(rawDataType))
            {
                LogDebug("clsDtaGenResources.GetResources: Error occurred retrieving spectra.");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!base.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.SingleDataset))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        #endregion
    }
}
