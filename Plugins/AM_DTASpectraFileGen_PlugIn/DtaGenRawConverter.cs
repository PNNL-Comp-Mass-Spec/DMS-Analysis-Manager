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
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;

namespace DTASpectraFileGen
{
    public class DtaGenRawConverter : DtaGenThermoRaw
    {
        // Ignore Spelling: mgf

        /// <summary>
        /// Returns the default path to the DTA generator tool
        /// </summary>
        /// <remarks>The default path can be overridden by updating mDtaToolNameLoc using DtaGen.UpdateDtaToolNameLoc</remarks>
        protected override string ConstructDTAToolPath()
        {
            var rawConverterDir = mMgrParams.GetParam("RawConverterProgLoc");
            var dtaToolPath = Path.Combine(rawConverterDir, RAWCONVERTER_FILENAME);

            return dtaToolPath;
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
        /// this method is called by MakeDTAFilesThreaded
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        private bool ConvertMGFtoDTA()
        {
            try
            {
                var rawDataTypeName = mJobParams.GetJobParameter("RawDataType", "");

                var mgfConverter = new MGFConverter(mDebugLevel, mWorkDir)
                {
                    IncludeExtraInfoOnParentIonLine = true,
                    MinimumIonsPerSpectrum = mJobParams.GetJobParameter("IonCounts", "IonCount", 0)
                };

                RegisterEvents(mgfConverter);

                var rawDataType = AnalysisResources.GetRawDataType(rawDataTypeName);
                var success = mgfConverter.ConvertMGFtoDTA(rawDataType, mDatasetName);

                if (!success)
                {
                    mErrMsg = mgfConverter.ErrorMessage;
                }

                mSpectraFileCount = mgfConverter.SpectraCountWritten;
                mProgress = 95;

                return success;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in ConvertMGFtoDTA", ex);
                return false;
            }
        }

        /// <summary>
        /// Create .mgf file using RawConverter
        /// this method is called by MakeDTAFilesThreaded
        /// </summary>
        /// <param name="rawDataType">Raw data file type</param>
        /// <returns>True if success, false if an error</returns>
        private bool ConvertRawToMGF(AnalysisResources.RawDataTypeConstants rawDataType)
        {
            try
            {
                if (mDebugLevel > 0)
                {
                    OnStatusEvent("Creating .MGF file using RawConverter");
                }

                string rawFilePath;

                // Construct the path to the .raw file
                switch (rawDataType)
                {
                    case AnalysisResources.RawDataTypeConstants.ThermoRawFile:
                        rawFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_RAW_EXTENSION);
                        break;
                    default:
                        mErrMsg = "Raw data file type not supported: " + rawDataType;
                        return false;
                }

                mInstrumentFileName = Path.GetFileName(rawFilePath);
                mJobParams.AddResultFileToSkip(mInstrumentFileName);

                // Determine the raw file converter .exe name
                var rawConverter = new FileInfo(mDtaToolNameLoc);

                if (rawConverter.Directory == null)
                {
                    mErrMsg = "Unable to determine the parent directory of " + rawConverter.FullName;
                    return false;
                }

                // Set up command
                var arguments = " " + Global.PossiblyQuotePath(rawFilePath) + " --mgf";

                if (mDebugLevel > 0)
                {
                    OnStatusEvent(mDtaToolNameLoc + " " + arguments);
                }

                // Setup a program runner tool to make the spectra files
                // The working directory must be the directory that has RawConverter.exe
                // Otherwise, the program creates the .mgf file in C:\  (and will likely get Access Denied)

                mCmdRunner = new RunDosProgram(rawConverter.Directory.FullName, mDebugLevel)
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
                    // .RunProgram returned false
                    LogDTACreationStats("ConvertRawToMGF", Path.GetFileNameWithoutExtension(mDtaToolNameLoc), "mCmdRunner.RunProgram returned false");

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
