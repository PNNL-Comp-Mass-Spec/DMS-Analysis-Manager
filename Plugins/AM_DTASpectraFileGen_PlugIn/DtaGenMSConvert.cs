//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 04/12/2012
//
// Uses MSConvert to create a .MGF file from a .Raw file or .mzXML file or .mzML file
// Next, converts the .MGF file to a _DTA.txt file
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;

namespace DTASpectraFileGen
{
    public class DtaGenMSConvert : DtaGenThermoRaw
    {
        // Ignore Spelling: mgf, dta, MGFtoCDTA, mslevel, Tol

        public const int DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN = 250;

        public bool ForceCentroidOn { get; set; }

        public override void Setup(SpectraFileProcessorParams initParams, AnalysisToolRunnerBase toolRunner)
        {
            base.Setup(initParams, toolRunner);

            // Tool setup for MSConvert involves creating a
            //  registry entry at HKEY_CURRENT_USER\Software\ProteoWizard
            //  to indicate that we agree to the Thermo license

            var proteoWizardTools = new ProteowizardTools(mDebugLevel);

            if (!proteoWizardTools.RegisterProteoWizard())
            {
                throw new Exception("Unable to register ProteoWizard");
            }
        }

        /// <summary>
        /// Returns the default path to the DTA generator tool
        /// </summary>
        /// <remarks>The default path can be overridden by updating mDtaToolNameLoc using DtaGen.UpdateDtaToolNameLoc</remarks>
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

            var convertToCDTA = mJobParams.GetJobParameter("DtaGenerator", "ConvertMGFtoCDTA", true);

            if (convertToCDTA)
            {
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
                var rawDataTypeName = mJobParams.GetJobParameter("RawDataType", string.Empty);

                var mgfConverter = new MGFConverter(mDebugLevel, mWorkDir)
                {
                    IncludeExtraInfoOnParentIonLine = true,
                    MinimumIonsPerSpectrum = 0
                };

                RegisterEvents(mgfConverter);

                var rawDataType = AnalysisResources.GetRawDataType(rawDataTypeName);
                var success = mgfConverter.ConvertMGFtoDTA(rawDataType, mDatasetName);

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
                    OnStatusEvent("Creating .MGF file using MSConvert");
                }

                string instrumentFilePath;

                // Construct the path to the instrument data file
                switch (rawDataType)
                {
                    case AnalysisResources.RawDataTypeConstants.ThermoRawFile:
                        instrumentFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_RAW_EXTENSION);
                        break;
                    case AnalysisResources.RawDataTypeConstants.mzXML:
                        instrumentFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_MZXML_EXTENSION);
                        break;
                    case AnalysisResources.RawDataTypeConstants.mzML:
                        instrumentFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_MZML_EXTENSION);
                        break;
                    case AnalysisResources.RawDataTypeConstants.BrukerTOFTdf:
                        instrumentFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_D_EXTENSION);
                        break;

                    default:
                        LogError("Raw data file type not supported: " + rawDataType);
                        return false;
                }

                mInstrumentFileName = Path.GetFileName(instrumentFilePath);
                mJobParams.AddResultFileToSkip(mInstrumentFileName);

                const int SCAN_START = 1;
                var scanStop = DEFAULT_SCAN_STOP;

                if (rawDataType == AnalysisResources.RawDataTypeConstants.ThermoRawFile)
                {
                    // Get the maximum number of scans in the file
                    mMaxScanInFile = GetMaxScan(instrumentFilePath);
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
                        // The default scan range for processing all scans was traditionally from scan 1 to scan 999999.
                        // This scan range may be defined in the job's settings file.
                        // This dataset has over 1 million spectra; assume that the user actually wants to analyze all of the spectra
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
                var centroidMGF = mJobParams.GetJobParameter("CentroidMGF", true);

                // Look for parameter CentroidPeakCountToRetain in the DtaGenerator section
                var centroidPeakCountToRetain = mJobParams.GetJobParameter("DtaGenerator", "CentroidPeakCountToRetain", 0);

                if (centroidPeakCountToRetain == 0)
                {
                    // Look for parameter CentroidPeakCountToRetain in any section
                    centroidPeakCountToRetain = mJobParams.GetJobParameter("CentroidPeakCountToRetain", DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN);
                }

                var centroidPeakCountMinimum = mJobParams.GetJobParameter("DtaGenerator", "CentroidPeakCountMinimum", 0);

                var combineIonMobilitySpectra = mJobParams.GetJobParameter("DtaGenerator", "CombineIonMobilitySpectra", false);

                var combineIonMobilityPrecursorTol = string.Empty;
                var combineIonMobilityScanTimeTol = string.Empty;
                var combineIonMobilityIonMobilityTol = string.Empty;

                if (combineIonMobilitySpectra)
                {
                    combineIonMobilityPrecursorTol = mJobParams.GetJobParameter("DtaGenerator", "CombineIMSPrecursorTol", "0.005");
                    combineIonMobilityScanTimeTol = mJobParams.GetJobParameter("DtaGenerator", "CombineIMSScanTimeTol", "0.5");
                    combineIonMobilityIonMobilityTol = mJobParams.GetJobParameter("DtaGenerator", "CombineIMSIonMobilityTol", "0.01");
                }

                if (ForceCentroidOn)
                {
                    centroidMGF = true;
                }

                // Construct a list of arguments to send to MSConvert
                var argumentList = new List<string> {
                    instrumentFilePath
                };

                if (centroidMGF)
                {
                    // Centroid the data by first applying the peak-picking algorithm, then keeping the top N data points
                    // Syntax details:
                    //   peakPicking [<PickerType> [msLevel=<ms_levels>]]
                    //   threshold <type> <threshold> <orientation> [<mslevels>]

                    // The following means to apply peak picking to all spectra (MS1 and MS2) and then keep the top 150 peaks (sorted by intensity)
                    // --filter "peakPicking vendor mslevel=1-" --filter "threshold count 150 most-intense"

                    if (centroidPeakCountToRetain == 0)
                    {
                        centroidPeakCountToRetain = DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN;
                    }
                    else if (centroidPeakCountToRetain < 25)
                    {
                        centroidPeakCountToRetain = 25;
                    }

                    argumentList.Add("--filter \"peakPicking vendor mslevel=1-\"");
                    argumentList.Add(string.Format("--filter \"threshold count {0} most-intense\"", centroidPeakCountToRetain));

                    if (centroidPeakCountMinimum > 0)
                    {
                        // Assure that spectra have a minimum number of data points
                        argumentList.Add(string.Format("--filter \"defaultArrayLength {0}-\"", centroidPeakCountMinimum));
                    }
                }

                if (limitingScanRange)
                {
                    argumentList.Add(string.Format("--filter \"scanNumber [{0},{1}]\"", SCAN_START, scanStop));
                }

                if (combineIonMobilitySpectra)
                {
                    argumentList.Add("--combineIonMobilitySpectra");

                    // Customize the title line to be of the form
                    // TITLE=DatasetName.223560.223560.5 NativeID:'merged=223559', IonMobility:'1.392141709988'
                    argumentList.Add("--filter \"titleMaker <RunId>.<ScanNumber>.<ScanNumber>.<ChargeState> NativeID:'<Id>', IonMobility:'<IonMobility>'\"");

                    // Combine MS/MS spectra with similar precursor m/z, scan time, and ion mobility drift times
                    argumentList.Add(string.Format("--filter \"scanSumming precursorTol={0} scanTimeTol={1} ionMobilityTol={2}\"",
                                                   combineIonMobilityPrecursorTol,
                                                   combineIonMobilityScanTimeTol,
                                                   combineIonMobilityIonMobilityTol));
                }

                argumentList.Add("--32");
                argumentList.Add("--mgf");

                argumentList.Add("-o " + mWorkDir);

                var arguments = string.Join(" ", argumentList);

                if (mDebugLevel > 0)
                {
                    OnStatusEvent(mDtaToolNameLoc + " " + arguments);
                }

                // Setup a program runner tool to make the spectra files
                mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
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
                    // .RunProgram returned false
                    LogDTACreationStats("ConvertRawToMGF", Path.GetFileNameWithoutExtension(mDtaToolNameLoc), "mCmdRunner.RunProgram returned false");

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
