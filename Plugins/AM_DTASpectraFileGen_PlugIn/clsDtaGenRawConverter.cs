//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 10/21/2016
//
// Uses RawConverter to create a .MGF file from a .Raw file (He and Yates. Anal Chem. 2015 Nov 17; 87 (22): 11361-11367)
// Next, converts the .MGF file to a _DTA.txt file
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase;

namespace DTASpectraFileGen
{
    public class clsDtaGenRawConverter : clsDtaGenThermoRaw
    {
        public override void Setup(SpectraFileProcessorParams initParams, clsAnalysisToolRunnerBase toolRunner)
        {
            base.Setup(initParams, toolRunner);
        }

        /// <summary>
        /// Returns the default path to the DTA generator tool
        /// </summary>
        /// <returns></returns>
        /// <remarks>The default path can be overridden by updating m_DtaToolNameLoc using clsDtaGen.UpdateDtaToolNameLoc</remarks>
        protected override string ConstructDTAToolPath()
        {
            string strDTAToolPath = null;

            var rawConverterDir = m_MgrParams.GetParam("RawConverterProgLoc");
            strDTAToolPath = Path.Combine(rawConverterDir, RAWCONVERTER_FILENAME);

            return strDTAToolPath;
        }

        protected override void MakeDTAFilesThreaded()
        {
            m_Status = ProcessStatus.SF_RUNNING;
            m_ErrMsg = string.Empty;

            m_Progress = 10;

            if (!ConvertRawToMGF(m_RawDataType))
            {
                if (m_Status != ProcessStatus.SF_ABORTING)
                {
                    m_Results = ProcessResults.SF_FAILURE;
                    m_Status = ProcessStatus.SF_ERROR;
                }
                return;
            }

            m_Progress = 75;

            if (!ConvertMGFtoDTA())
            {
                if (m_Status != ProcessStatus.SF_ABORTING)
                {
                    m_Results = ProcessResults.SF_FAILURE;
                    m_Status = ProcessStatus.SF_ERROR;
                }
                return;
            }

            m_Results = ProcessResults.SF_SUCCESS;
            m_Status = ProcessStatus.SF_COMPLETE;
        }

        /// <summary>
        /// Convert .mgf file to _DTA.txt using MascotGenericFileToDTA.dll
        /// This function is called by MakeDTAFilesThreaded
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool ConvertMGFtoDTA()
        {
            try
            {
                var strRawDataType = m_JobParams.GetJobParameter("RawDataType", "");

                var oMGFConverter = new clsMGFConverter(m_DebugLevel, m_WorkDir)
                {
                    IncludeExtraInfoOnParentIonLine = true,
                    MinimumIonsPerSpectrum = m_JobParams.GetJobParameter("IonCounts", "IonCount", 0)
                };

                RegisterEvents(oMGFConverter);

                var eRawDataType = clsAnalysisResources.GetRawDataType(strRawDataType);
                var blnSuccess = oMGFConverter.ConvertMGFtoDTA(eRawDataType, m_Dataset);

                if (!blnSuccess)
                {
                    m_ErrMsg = oMGFConverter.ErrorMessage;
                }

                m_SpectraFileCount = oMGFConverter.SpectraCountWritten;
                m_Progress = 95;

                return blnSuccess;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in ConvertMGFtoDTA", ex);
                return false;
            }
        }

        /// <summary>
        /// Create .mgf file using RawConverter
        /// This function is called by MakeDTAFilesThreaded
        /// </summary>
        /// <param name="eRawDataType">Raw data file type</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool ConvertRawToMGF(clsAnalysisResources.eRawDataTypeConstants eRawDataType)
        {
            try
            {
                if (m_DebugLevel > 0)
                {
                    OnStatusEvent("Creating .MGF file using RawConverter");
                }

                string rawFilePath = null;

                // Construct the path to the .raw file
                switch (eRawDataType)
                {
                    case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                        rawFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_RAW_EXTENSION);
                        break;
                    default:
                        m_ErrMsg = "Raw data file type not supported: " + eRawDataType;
                        return false;
                }

                m_InstrumentFileName = Path.GetFileName(rawFilePath);
                m_JobParams.AddResultFileToSkip(m_InstrumentFileName);

                var fiRawConverter = new FileInfo(m_DtaToolNameLoc);

                // Set up command
                var cmdStr = " " + clsGlobal.PossiblyQuotePath(rawFilePath) + " --mgf";

                if (m_DebugLevel > 0)
                {
                    OnStatusEvent(m_DtaToolNameLoc + " " + cmdStr);
                }

                // Setup a program runner tool to make the spectra files
                // The working directory must be the folder that has RawConverter.exe
                // Otherwise, the program creates the .mgf file in C:\  (and will likely get Access Denied)

                mCmdRunner = new clsRunDosProgram(fiRawConverter.Directory.FullName, m_DebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = string.Empty      // Allow the console output filename to be auto-generated
                };
                mCmdRunner.ErrorEvent += CmdRunner_ErrorEvent;
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                if (!mCmdRunner.RunProgram(m_DtaToolNameLoc, cmdStr, "RawConverter", true))
                {
                    // .RunProgram returned False
                    LogDTACreationStats("ConvertRawToMGF", Path.GetFileNameWithoutExtension(m_DtaToolNameLoc), "mCmdRunner.RunProgram returned False");

                    m_ErrMsg = "Error running " + Path.GetFileNameWithoutExtension(m_DtaToolNameLoc);
                    return false;
                }

                if (m_DebugLevel >= 2)
                {
                    OnStatusEvent(" ... MGF file created using RawConverter");
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in ConvertRawToMGF", ex);
                return false;
            }
        }
    }
}
