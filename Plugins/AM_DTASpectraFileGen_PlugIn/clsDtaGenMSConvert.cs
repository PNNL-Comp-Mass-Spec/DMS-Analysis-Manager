//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 04/12/2012
//
// Uses MSConvert to create a .MGF file from a .Raw file or .mzXML file or .mzML file
// Next, converts the .MGF file to a _DTA.txt file
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.IO;

namespace DTASpectraFileGen
{
    public class clsDtaGenMSConvert : clsDtaGenThermoRaw
    {
        public const int DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN = 250;

        public bool ForceCentroidOn { get; set; }

        public override void Setup(SpectraFileProcessorParams initParams, clsAnalysisToolRunnerBase toolRunner)
        {
            base.Setup(initParams, toolRunner);

            // Tool setup for MSConvert involves creating a
            //  registry entry at HKEY_CURRENT_USER\Software\ProteoWizard
            //  to indicate that we agree to the Thermo license

            var proteoWizardTools = new clsProteowizardTools(mDebugLevel);

            if (!proteoWizardTools.RegisterProteoWizard())
            {
                throw new Exception("Unable to register ProteoWizard");
            }
        }

        /// <summary>
        /// Returns the default path to the DTA generator tool
        /// </summary>
        /// <returns></returns>
        /// <remarks>The default path can be overridden by updating mDtaToolNameLoc using clsDtaGen.UpdateDtaToolNameLoc</remarks>
        protected override string ConstructDTAToolPath()
        {
            var proteoWizardDir = mMgrParams.GetParam("ProteoWizardDir");         // MSConvert.exe is stored in the ProteoWizard folder
            var dtaToolPath = Path.Combine(proteoWizardDir, MSCONVERT_FILENAME);

            return dtaToolPath;
        }

        protected override void MakeDTAFilesThreaded()
        {
            mStatus = ProcessStatus.SF_RUNNING;
            mErrMsg = string.Empty;

            mProgress = 10;

            // Use MSConvert to create a .mgf file
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

            // Convert the .mgf file to _dta.txt
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
                var rawDataType = mJobParams.GetJobParameter("RawDataType", string.Empty);

                var mgfConverter = new clsMGFConverter(mDebugLevel, mWorkDir)
                {
                    IncludeExtraInfoOnParentIonLine = true,
                    MinimumIonsPerSpectrum = 0
                };

                RegisterEvents(mgfConverter);

                var eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType);
                var success = mgfConverter.ConvertMGFtoDTA(eRawDataType, mDatasetName);

                if (!success)
                {
                    // The error has already been logged
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
        /// Create .mgf file using MSConvert
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
                    OnStatusEvent("Creating .MGF file using MSConvert");
                }

                string rawFilePath;

                // Construct the path to the .raw file
                switch (eRawDataType)
                {
                    case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                        rawFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_RAW_EXTENSION);
                        break;
                    case clsAnalysisResources.eRawDataTypeConstants.mzXML:
                        rawFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_MZXML_EXTENSION);
                        break;
                    case clsAnalysisResources.eRawDataTypeConstants.mzML:
                        rawFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_MZML_EXTENSION);
                        break;
                    default:
                        LogError("Raw data file type not supported: " + eRawDataType);
                        return false;
                }

                mInstrumentFileName = Path.GetFileName(rawFilePath);
                mJobParams.AddResultFileToSkip(mInstrumentFileName);

                const int SCAN_START = 1;
                var scanStop = DEFAULT_SCAN_STOP;

                if (eRawDataType == clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile)
                {
                    // Get the maximum number of scans in the file
                    mMaxScanInFile = GetMaxScan(rawFilePath);
                }
                else
                {
                    mMaxScanInFile = scanStop;
                }

                switch (mMaxScanInFile)
                {
                    case -1:
                        // Generic error getting number of scans
                        LogError("Unknown error getting number of scans; MaxScan = " + mMaxScanInFile);
                        return false;
                    case 0:
                        // Unable to read file; treat this is a warning
                        LogError("Warning: unable to get MaxScan; MaxScan is 0");
                        break;
                    default:
                        if (mMaxScanInFile > 0)
                        {
                            // This is normal, do nothing
                            break;
                        }
                        // This should never happen
                        LogError("Critical error getting number of scans; MaxScan = " + mMaxScanInFile);
                        return false;
                }

                var limitingScanRange = false;

                // Verify max scan specified is in file
                if (mMaxScanInFile > 0)
                {
                    if (scanStop == 999999 && scanStop < mMaxScanInFile)
                    {
                        // The default scan range for processing all scans has traditionally been 1 to 999999
                        // This scan range is defined for this job's settings file, but this dataset has over 1 million spectra
                        // Assume that the user actually wants to analyze all of the spectra
                        scanStop = mMaxScanInFile;
                    }

                    if (scanStop > mMaxScanInFile)
                        scanStop = mMaxScanInFile;

                    if (scanStop < mMaxScanInFile)
                        limitingScanRange = true;

                }
                else
                {
                    if (scanStop < DEFAULT_SCAN_STOP)
                        limitingScanRange = true;
                }

                // Determine max number of scans to be used
                mNumScans = scanStop - SCAN_START + 1;

                // Lookup Centroid Settings
                var centroidMGF = mJobParams.GetJobParameter("CentroidMGF", false);

                // Look for parameter CentroidPeakCountToRetain in the DtaGenerator section
                var centroidPeakCountToRetain = mJobParams.GetJobParameter("DtaGenerator", "CentroidPeakCountToRetain", 0);

                if (centroidPeakCountToRetain == 0)
                {
                    // Look for parameter CentroidPeakCountToRetain in any section
                    centroidPeakCountToRetain = mJobParams.GetJobParameter("CentroidPeakCountToRetain", DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN);
                }

                if (ForceCentroidOn)
                {
                    centroidMGF = true;
                }

                // Set up command
                var arguments = " " + rawFilePath;

                if (centroidMGF)
                {
                    // Centroid the data by first applying the peak-picking algorithm, then keeping the top N data points
                    // Syntax details:
                    //   peakPicking prefer_vendor:<true|false>  int_set(MS levels)
                    //   threshold <count|count-after-ties|absolute|bpi-relative|tic-relative|tic-cutoff> <threshold> <most-intense|least-intense> [int_set(MS levels)]

                    // So, the following means to apply peak picking to all spectra (MS1 and MS2) and then keep the top 250 peaks (sorted by intensity)
                    // --filter "peakPicking true 1-" --filter "threshold count 250 most-intense"

                    if (centroidPeakCountToRetain == 0)
                    {
                        centroidPeakCountToRetain = DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN;
                    }
                    else if (centroidPeakCountToRetain < 25)
                    {
                        centroidPeakCountToRetain = 25;
                    }

                    arguments += " --filter \"peakPicking true 1-\" --filter \"threshold count " + centroidPeakCountToRetain + " most-intense\"";
                }

                if (limitingScanRange)
                {
                    arguments += " --filter \"scanNumber [" + SCAN_START + "," + scanStop + "]\"";
                }

                arguments += " --mgf -o " + mWorkDir;

                if (mDebugLevel > 0)
                {
                    OnStatusEvent(mDtaToolNameLoc + " " + arguments);
                }

                // Setup a program runner tool to make the spectra files
                mCmdRunner = new clsRunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = string.Empty      // Allow the console output filename to be auto-generated
                };
                mCmdRunner.ErrorEvent += CmdRunner_ErrorEvent;
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                if (!mCmdRunner.RunProgram(mDtaToolNameLoc, arguments, "MSConvert", true))
                {
                    // .RunProgram returned False
                    LogDTACreationStats("ConvertRawToMGF", Path.GetFileNameWithoutExtension(mDtaToolNameLoc), "mCmdRunner.RunProgram returned False");

                    mErrMsg = "Error running " + Path.GetFileNameWithoutExtension(mDtaToolNameLoc);
                    return false;
                }

                if (mDebugLevel >= 2)
                {
                    OnStatusEvent(" ... MGF file created using MSConvert");
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
