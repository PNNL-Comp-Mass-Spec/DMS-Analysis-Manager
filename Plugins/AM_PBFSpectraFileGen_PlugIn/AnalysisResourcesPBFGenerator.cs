//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/16/2014
//
//*********************************************************************************************************

using System;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerPBFGenerator
{
    /// <summary>
    /// Retrieve resources for the PBF Generator plugin
    /// </summary>
    public class AnalysisResourcesPBFGenerator : AnalysisResources
    {
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            if (!RetrieveInstrumentData())
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool RetrieveInstrumentData()
        {
            var currentTask = "Initializing";

            try
            {
                var rawDataTypeName = mJobParams.GetJobParameter("RawDataType", "");
                var rawDataType = GetRawDataType(rawDataTypeName);

                if (rawDataType == RawDataTypeConstants.ThermoRawFile)
                {
                    mJobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                }
                else
                {
                    mMessage = "PbfGen presently only supports Thermo .Raw files";
                    return false;
                }

                currentTask = "Retrieve instrument data";

                // Retrieve the instrument data file
                if (!FileSearchTool.RetrieveSpectra(rawDataTypeName))
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Error retrieving instrument data file";
                    }

                    LogError("AnalysisResourcesPBFGenerator.GetResources: " + mMessage);
                    return false;
                }

                currentTask = "Process MyEMSL Download Queue";

                var success = ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories);
                return success;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrieveInstrumentData: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + Global.GetExceptionStackTrace(ex));
                return false;
            }
        }
    }
}
