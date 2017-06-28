//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 03/30/2011
//
//*********************************************************************************************************

using AnalysisManagerBase;

namespace AnalysisManagerMsXmlBrukerPlugIn
{
    public class clsAnalysisResourcesMSXMLBruker : clsAnalysisResources
    {
        #region "Methods"

        /// <summary>
        /// Retrieves files necessary for creating the .mzXML file
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

            // Get input data file
            var strRawDataType = m_jobParams.GetParam("RawDataType");
            var eRawDataType = GetRawDataType(strRawDataType);

            switch (eRawDataType)
            {
                case eRawDataTypeConstants.BrukerFTFolder:
                case eRawDataTypeConstants.BrukerTOFBaf:
                    break;
                // This dataset type is acceptable
                default:
                    m_message = "Dataset type " + strRawDataType + " is not supported";
                    LogDebug(
                        "clsDtaGenResources.GetResources: " + m_message + "; must be " + RAW_DATA_TYPE_BRUKER_FT_FOLDER + " or " +
                        RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER);

                    return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!FileSearch.RetrieveSpectra(strRawDataType))
            {
                LogDebug("clsDtaGenResources.GetResources: Error occurred retrieving spectra.");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.SingleDataset))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        #endregion
    }
}
