//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/16/2014
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;

namespace AnalysisManagerPBFGenerator
{

    /// <summary>
    /// Retrieve resources for the PBF Generator plugin
    /// </summary>
    public class clsAnalysisResourcesPBFGenerator : clsAnalysisResources
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

        protected bool RetrieveInstrumentData()
        {
            var currentTask = "Initializing";

            try
            {
                var rawDataType = mJobParams.GetJobParameter("RawDataType", "");
                var eRawDataType = GetRawDataType(rawDataType);

                if (eRawDataType == eRawDataTypeConstants.ThermoRawFile)
                {
                    mJobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                }
                else
                {
                    mMessage = "PbfGen presently only supports Thermo .Raw files";
                    return false;
                }

                currentTask = "Retrieve intrument data";

                // Retrieve the instrument data file
                if (!FileSearch.RetrieveSpectra(rawDataType))
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Error retrieving instrument data file";
                    }

                    LogError("clsAnalysisResourcesPBFGenerator.GetResources: " + mMessage);
                    return false;
                }

                currentTask = "Process MyEMSL Download Queue";

                var success = ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories);
                return success;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrieveInstrumentData: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));
                return false;
            }
        }
    }
}
