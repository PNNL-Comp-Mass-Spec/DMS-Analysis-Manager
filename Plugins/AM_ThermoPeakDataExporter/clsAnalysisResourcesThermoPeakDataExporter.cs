//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 09/17/2018
//
//*********************************************************************************************************

using System;
using AnalysisManagerBase;


namespace AnalysisManagerThermoPeakDataExporterPlugIn
{
    /// <summary>
    /// Retrieve resources for the ThermoPeakDataExporter plugin
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsAnalysisResourcesThermoPeakDataExporter : clsAnalysisResources
    {

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
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

                currentTask = "Get Input file";

                // Get input data file
                const bool createStoragePathInfoOnly = false;
                var rawDataType = m_jobParams.GetParam("rawDataType");
                var toolName = m_jobParams.GetParam("ToolName");

                switch (rawDataType.ToLower())
                {
                    case RAW_DATA_TYPE_DOT_RAW_FILES:
                        // Processing a Thermo .raw file
                        break;
                    default:
                        LogError("This tool is not compatible with datasets of type " + rawDataType);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!FileSearch.RetrieveSpectra(rawDataType, createStoragePathInfoOnly))
                {
                    LogDebug("clsAnalysisResourcesThermoPeakDataExporter.GetResources: Error occurred retrieving spectra.");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (clsGlobal.IsMatch(rawDataType, RAW_DATA_TYPE_DOT_RAW_FILES) &&
                    toolName.StartsWith("MASIC_Finnigan", StringComparison.OrdinalIgnoreCase))
                {
                    var rawFileName = DatasetName + ".raw";
                    var inputFilePath = ResolveStoragePath(m_WorkingDir, rawFileName);

                    if (string.IsNullOrWhiteSpace(inputFilePath))
                    {
                        // Unable to resolve the file path
                        m_message = "Could not find " + rawFileName + " or " + rawFileName + STORAGE_PATH_INFO_FILE_SUFFIX +
                                    " in the working folder; unable to run MASIC";
                        LogError(m_message);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                m_message = "Exception in GetResources: " + ex.Message;
                LogError(m_message + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }
    }
}
