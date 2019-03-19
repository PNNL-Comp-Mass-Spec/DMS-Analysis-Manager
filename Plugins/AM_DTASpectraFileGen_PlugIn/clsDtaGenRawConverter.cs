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
        /// <remarks>The default path can be overridden by updating mDtaToolNameLoc using clsDtaGen.UpdateDtaToolNameLoc</remarks>
        protected override string ConstructDTAToolPath()
        {
            string strDTAToolPath = null;

            var rawConverterDir = mMgrParams.GetParam("RawConverterProgLoc");
            strDTAToolPath = Path.Combine(rawConverterDir, RAWCONVERTER_FILENAME);

            return strDTAToolPath;
        }

        protected override void MakeDTAFilesThreaded()
        {
            mStatus = ProcessStatus.SF_RUNNING;
            mErrMsg = string.Empty;

            mProgress = 10;

            if (!ConvertRawToMGF(mRawDataType))
            {
                if (mStatus != ProcessStatus.SF_ABORTING)
                {
                    mResults = ProcessResults.SF_FAILURE;
                    mStatus = ProcessStatus.SF_ERROR;
                }
                return;
            }

            mProgress = 75;

            if (!ConvertMGFtoDTA())
            {
                if (mStatus != ProcessStatus.SF_ABORTING)
                {
                    mResults = ProcessResults.SF_FAILURE;
                    mStatus = ProcessStatus.SF_ERROR;
                }
                return;
            }

            mResults = ProcessResults.SF_SUCCESS;
            mStatus = ProcessStatus.SF_COMPLETE;
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
                var strRawDataType = mJobParams.GetJobParameter("RawDataType", "");

                var oMGFConverter = new clsMGFConverter(mDebugLevel, mWorkDir)
                {
                    IncludeExtraInfoOnParentIonLine = true,
                    MinimumIonsPerSpectrum = mJobParams.GetJobParameter("IonCounts", "IonCount", 0)
                };

                RegisterEvents(oMGFConverter);

                var eRawDataType = clsAnalysisResources.GetRawDataType(strRawDataType);
                var blnSuccess = oMGFConverter.ConvertMGFtoDTA(eRawDataType, mDatasetName);

                if (!blnSuccess)
                {
                    mErrMsg = oMGFConverter.ErrorMessage;
                }

                mSpectraFileCount = oMGFConverter.SpectraCountWritten;
                mProgress = 95;

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
                if (mDebugLevel > 0)
                {
                    OnStatusEvent("Creating .MGF file using RawConverter");
                }

                string rawFilePath = null;

                // Construct the path to the .raw file
                switch (eRawDataType)
                {
                    case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                        rawFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_RAW_EXTENSION);
                        break;
                    default:
                        mErrMsg = "Raw data file type not supported: " + eRawDataType;
                        return false;
                }

                mInstrumentFileName = Path.GetFileName(rawFilePath);
                mJobParams.AddResultFileToSkip(mInstrumentFileName);

                var fiRawConverter = new FileInfo(mDtaToolNameLoc);

                // Set up command
                var arguments = " " + clsGlobal.PossiblyQuotePath(rawFilePath) +
                                " --mgf";

                if (mDebugLevel > 0)
                {
                    OnStatusEvent(mDtaToolNameLoc + " " + arguments);
                }

                // Setup a program runner tool to make the spectra files
                // The working directory must be the directory that has RawConverter.exe
                // Otherwise, the program creates the .mgf file in C:\  (and will likely get Access Denied)

                mCmdRunner = new clsRunDosProgram(fiRawConverter.Directory.FullName, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = string.Empty      // Allow the console output filename to be auto-generated
                };
                mCmdRunner.ErrorEvent += CmdRunner_ErrorEvent;
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                if (!mCmdRunner.RunProgram(mDtaToolNameLoc, arguments, "RawConverter", true))
                {
                    // .RunProgram returned False
                    LogDTACreationStats("ConvertRawToMGF", Path.GetFileNameWithoutExtension(mDtaToolNameLoc), "mCmdRunner.RunProgram returned False");

                    mErrMsg = "Error running " + Path.GetFileNameWithoutExtension(mDtaToolNameLoc);
                    return false;
                }

                if (mDebugLevel >= 2)
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
