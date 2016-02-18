/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 04/29/2015                                           **
**                                                              **
*****************************************************************/

using System;
using AnalysisManagerBase;

namespace AnalysisManagerBrukerDAExportPlugin
{
    public class clsAnalysisResourcesBrukerDAExport : clsAnalysisResources
    {

        public override IJobParams.CloseOutType GetResources()
        {

            var currentTask = "Initializing";

            try
            {
                // Retrieve the export script
				currentTask = "Get parameter BrukerSpectraExportScriptFile";
                var exportScriptName = m_jobParams.GetJobParameter("BrukerSpectraExportScriptFile", string.Empty);
                if (string.IsNullOrEmpty(exportScriptName))
                {
                    LogError("BrukerSpectraExportScriptFile parameter is empty");
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // Retrieve the script file
                currentTask = "Retrieve the export script file: " + exportScriptName;

                const string paramFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "Bruker_DA_Export";

                var exportScriptStoragePath = m_mgrParams.GetParam(paramFileStoragePathKeyName);
                if (string.IsNullOrWhiteSpace(exportScriptStoragePath))
                {
                    exportScriptStoragePath = @"F:\My Documents\Gigasax_Data\DMS_Parameter_Files\Bruker_Data_Analysis";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter '" + paramFileStoragePathKeyName + "' is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " + exportScriptStoragePath);
                }

                if (!RetrieveFile(exportScriptName, exportScriptStoragePath))
                {
                    // Errors should have already been logged
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // Get the instrument data
                var strRawDataType = m_jobParams.GetParam("RawDataType");

                var retrievalAttempts = 0;

                while (retrievalAttempts < 2)
                {
                    retrievalAttempts += 1;
                    switch (strRawDataType.ToLower())
                    {
                        case RAW_DATA_TYPE_DOT_D_FOLDERS:
                        case RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER:
                        case RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                            currentTask = "Retrieve spectra: " + strRawDataType;

                            if (!RetrieveSpectra(strRawDataType))
                            {
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "AnalysisManagerBrukerDAExportPlugin.GetResources: Error occurred retrieving spectra.");
                                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                            }
                            break;
                        default:
                            m_message = "Dataset type " + strRawDataType + " is not supported";
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                "AnalysisManagerBrukerDAExportPlugin.GetResources: " + m_message + "; must be " +
                                RAW_DATA_TYPE_DOT_D_FOLDERS + " or " + RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER);
                            return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (m_MyEMSLUtilities.FilesToDownload.Count == 0)
                    {
                        break;
                    }

                    currentTask = "Process the MyEMSL download queue";
                    if (ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                    {
                        break;
                    }

                    // Look for this file on the Samba share
                    base.DisableMyEMSLSearch();
                }

                return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;

            }
            catch (Exception ex)
            {
                m_message = "Exception in GetResources: " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     m_message + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));

                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

        }

    }
}
