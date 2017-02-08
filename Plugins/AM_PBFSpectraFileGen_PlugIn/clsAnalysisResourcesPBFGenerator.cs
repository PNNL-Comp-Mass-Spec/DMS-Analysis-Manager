//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/16/2014
//
//*********************************************************************************************************

using System;

using AnalysisManagerBase;

namespace AnalysisManagerPBFGenerator
{
    public class clsAnalysisResourcesPBFGenerator : clsAnalysisResources
    {
        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            if (!RetrieveInstrumentData())
            {
                return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected bool RetrieveInstrumentData()
        {
            string currentTask = "Initializing";

            try
            {
                string rawDataType = m_jobParams.GetJobParameter("RawDataType", "");
                var eRawDataType = GetRawDataType(rawDataType);

                if (eRawDataType == eRawDataTypeConstants.ThermoRawFile)
                {
                    m_jobParams.AddResultFileExtensionToSkip(DOT_RAW_EXTENSION);
                }
                else
                {
                    m_message = "PbfGen presently only supports Thermo .Raw files";
                    return false;
                }

                currentTask = "Retrieve intrument data";

                // Retrieve the instrument data file
                if (!RetrieveSpectra(rawDataType))
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Error retrieving instrument data file";
                    }

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsAnalysisResourcesPBFGenerator.GetResources: " + m_message);
                    return false;
                }

                currentTask = "Process MyEMSL Download Queue";

                if (!base.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    return false;
                }

                System.Threading.Thread.Sleep(500);

                return true;
            }
            catch (Exception ex)
            {
                m_message = "Exception in RetrieveInstrumentData: " + ex.Message;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + "; task = " + currentTask + "; " + clsGlobal.GetExceptionStackTrace(ex));
                return false;
            }
        }
    }
}
