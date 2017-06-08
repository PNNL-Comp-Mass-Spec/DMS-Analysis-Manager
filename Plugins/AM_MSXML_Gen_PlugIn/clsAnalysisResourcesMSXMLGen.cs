using System;
using AnalysisManagerBase;

namespace AnalysisManagerMsXmlGenPlugIn
{
    public class clsAnalysisResourcesMSXMLGen : clsAnalysisResources
    {
        #region "Methods"

        /// <summary>
        /// Retrieves files necessary for creating the .mzXML file
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType GetResources()
        {
            var currentTask = "Initializing";

            try
            {
                currentTask = "Retrieve shared resources";

                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                currentTask = "Determine RawDataType";

                var toolName = m_jobParams.GetParam("ToolName");
                var proMexBruker = toolName.StartsWith("ProMex_Bruker", StringComparison.OrdinalIgnoreCase);

                if (proMexBruker)
                {
                    // Make sure the settings file has MSXMLOutputType=mzML, not mzXML

                    var msXmlFormat = m_jobParams.GetParam("MSXMLOutputType");
                    if (string.IsNullOrWhiteSpace(msXmlFormat))
                    {
                        LogError("Job parameter MSXMLOutputType must be defined in the settings file");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (!msXmlFormat.ToLower().Contains("mzml"))
                    {
                        LogError("ProMex_Bruker jobs require mzML files, not " + msXmlFormat + " files");
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                // Get input data file
                var strRawDataType = m_jobParams.GetParam("RawDataType");

                var retrievalAttempts = 0;

                while (retrievalAttempts < 2)
                {
                    retrievalAttempts += 1;
                    switch (strRawDataType.ToLower())
                    {
                        case RAW_DATA_TYPE_DOT_RAW_FILES:
                        case RAW_DATA_TYPE_DOT_D_FOLDERS:
                        case RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER:
                        case RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                            currentTask = "Retrieve spectra: " + strRawDataType;

                            if (FileSearch.RetrieveSpectra(strRawDataType))
                            {
                                m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                                //Raw file
                            }
                            else
                            {
                                LogDebug("clsAnalysisResourcesMSXMLGen.GetResources: Error occurred retrieving spectra.");
                                return CloseOutType.CLOSEOUT_FAILED;
                            }
                            break;
                        default:
                            m_message = "Dataset type " + strRawDataType + " is not supported";
                            LogDebug(
                                "clsAnalysisResourcesMSXMLGen.GetResources: " + m_message + "; must be " + RAW_DATA_TYPE_DOT_RAW_FILES + ", " +
                                RAW_DATA_TYPE_DOT_D_FOLDERS + ", " + RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER + ", or " + RAW_DATA_TYPE_BRUKER_FT_FOLDER);

                            return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (m_MyEMSLUtilities.FilesToDownload.Count == 0)
                    {
                        break;
                    }

                    currentTask = "ProcessMyEMSLDownloadQueue";
                    if (m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                    {
                        break;
                    }

                    // Look for this file on the Samba share
                    base.DisableMyEMSLSearch();
                }

                var mzMLRefParamFile = m_jobParams.GetJobParameter("MzMLRefParamFile", string.Empty);

                if (!string.IsNullOrEmpty(mzMLRefParamFile))
                {
                    // Retrieve the Fasta file
                    var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");

                    currentTask = "RetrieveOrgDB to " + localOrgDbFolder;

                    if (!RetrieveOrgDB(localOrgDbFolder))
                        return CloseOutType.CLOSEOUT_FAILED;

                    currentTask = "Retrieve the MzML Refinery parameter file " + mzMLRefParamFile;

                    const string paramFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "MzML_Refinery";

                    var mzMLRefineryParmFileStoragePath = m_mgrParams.GetParam(paramFileStoragePathKeyName);
                    if (string.IsNullOrWhiteSpace(mzMLRefineryParmFileStoragePath))
                    {
                        mzMLRefineryParmFileStoragePath = @"\\gigasax\dms_parameter_Files\MzMLRefinery";
                        LogWarning(
                            "Parameter '" + paramFileStoragePathKeyName +
                            "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " +
                            mzMLRefineryParmFileStoragePath);
                    }

                    //Retrieve param file
                    if (!FileSearch.RetrieveFile(mzMLRefParamFile, m_jobParams.GetParam("ParmFileStoragePath")))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception in GetResources: " + ex.Message;
                LogError(
                    m_message + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        #endregion
    }
}
