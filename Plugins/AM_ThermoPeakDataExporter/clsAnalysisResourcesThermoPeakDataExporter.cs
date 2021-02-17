//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 09/17/2018
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;

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
                var rawDataTypeName = mJobParams.GetParam("rawDataType");

                switch (rawDataTypeName.ToLower())
                {
                    case RAW_DATA_TYPE_DOT_RAW_FILES:
                        // Processing a Thermo .raw file
                        break;
                    default:
                        LogError("This tool is not compatible with datasets of type " + rawDataTypeName);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!FileSearch.RetrieveSpectra(rawDataTypeName, createStoragePathInfoOnly))
                {
                    LogDebug("clsAnalysisResourcesThermoPeakDataExporter.GetResources: Error occurred retrieving spectra.");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!base.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mJobParams.AddResultFileToSkip(DatasetName + DOT_RAW_EXTENSION);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in GetResources: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
