//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 04/12/2012
//
// Uses MSConvert to create a .MGF file from a .Raw file or .mzXML file or .mzML file
// Next, converts the .MGF file to a _DTA.txt file
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase;

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

            var objProteowizardTools = new clsProteowizardTools(m_DebugLevel);

            if (!objProteowizardTools.RegisterProteoWizard())
            {
                throw new Exception("Unable to register ProteoWizard");
            }
        }

        /// <summary>
        /// Returns the default path to the DTA generator tool
        /// </summary>
        /// <returns></returns>
        /// <remarks>The default path can be overridden by updating m_DtaToolNameLoc using clsDtaGen.UpdateDtaToolNameLoc</remarks>
        protected override string ConstructDTAToolPath()
        {
            string strDTAToolPath = null;

            var ProteoWizardDir = m_MgrParams.GetParam("ProteoWizardDir");         // MSConvert.exe is stored in the ProteoWizard folder
            strDTAToolPath = Path.Combine(ProteoWizardDir, MSCONVERT_FILENAME);

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
                var strRawDataType = m_JobParams.GetJobParameter("RawDataType", string.Empty);

                var oMGFConverter = new clsMGFConverter(m_DebugLevel, m_WorkDir)
                {
                    IncludeExtraInfoOnParentIonLine = true,
                    MinimumIonsPerSpectrum = 0
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
                if (m_DebugLevel > 0)
                {
                    OnStatusEvent("Creating .MGF file using MSConvert");
                }

                string rawFilePath = null;

                // Construct the path to the .raw file
                switch (eRawDataType)
                {
                    case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                        rawFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_RAW_EXTENSION);
                        break;
                    case clsAnalysisResources.eRawDataTypeConstants.mzXML:
                        rawFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MZXML_EXTENSION);
                        break;
                    case clsAnalysisResources.eRawDataTypeConstants.mzML:
                        rawFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MZML_EXTENSION);
                        break;
                    default:
                        m_ErrMsg = "Raw data file type not supported: " + eRawDataType.ToString();
                        return false;
                }

                m_InstrumentFileName = Path.GetFileName(rawFilePath);
                m_JobParams.AddResultFileToSkip(m_InstrumentFileName);

                const int SCAN_START = 1;
                var scanStop = DEFAULT_SCAN_STOP;

                if (eRawDataType == clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile)
                {
                    //Get the maximum number of scans in the file
                    m_MaxScanInFile = GetMaxScan(rawFilePath);
                }
                else
                {
                    m_MaxScanInFile = scanStop;
                }

                switch (m_MaxScanInFile)
                {
                    case -1:
                        // Generic error getting number of scans
                        m_ErrMsg = "Unknown error getting number of scans; Maxscan = " + m_MaxScanInFile.ToString();
                        return false;
                    case 0:
                        // Unable to read file; treat this is a warning
                        m_ErrMsg = "Warning: unable to get maxscan; Maxscan = 0";
                        break;
                    default:
                        if (m_MaxScanInFile > 0)
                        {
                            // This is normal, do nothing
                            break;
                        }
                        // This should never happen
                        m_ErrMsg = "Critical error getting number of scans; Maxscan = " + m_MaxScanInFile.ToString();
                        return false;
                }

                var blnLimitingScanRange = false;

                //Verify max scan specified is in file
                if (m_MaxScanInFile > 0)
                {
                    if (scanStop == 999999 && scanStop < m_MaxScanInFile)
                    {
                        // The default scan range for processing all scans has traditionally be 1 to 999999
                        // This scan range is defined for this job's settings file, but this dataset has over 1 million spectra
                        // Assume that the user actually wants to analyze all of the spectra
                        scanStop = m_MaxScanInFile;
                    }

                    if (scanStop > m_MaxScanInFile)
                        scanStop = m_MaxScanInFile;

                    if (scanStop < m_MaxScanInFile)
                        blnLimitingScanRange = true;

                }
                else
                {
                    if (scanStop < DEFAULT_SCAN_STOP)
                        blnLimitingScanRange = true;
                }

                //Determine max number of scans to be used
                m_NumScans = scanStop - SCAN_START + 1;

                // Lookup Centroid Settings
                var centroidMGF = m_JobParams.GetJobParameter("CentroidMGF", false);

                // Look for parameter CentroidPeakCountToRetain in the DtaGenerator section
                var centroidPeakCountToRetain = m_JobParams.GetJobParameter("DtaGenerator", "CentroidPeakCountToRetain", 0);

                if (centroidPeakCountToRetain == 0)
                {
                    // Look for parameter CentroidPeakCountToRetain in any section
                    centroidPeakCountToRetain = m_JobParams.GetJobParameter("CentroidPeakCountToRetain", DEFAULT_CENTROID_PEAK_COUNT_TO_RETAIN);
                }

                if (ForceCentroidOn)
                {
                    centroidMGF = true;
                }

                //Set up command
                var cmdStr = " " + rawFilePath;

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

                    cmdStr += " --filter \"peakPicking true 1-\" --filter \"threshold count " + centroidPeakCountToRetain + " most-intense\"";
                }

                if (blnLimitingScanRange)
                {
                    cmdStr += " --filter \"scanNumber [" + SCAN_START + "," + scanStop + "]\"";
                }

                cmdStr += " --mgf -o " + m_WorkDir;

                if (m_DebugLevel > 0)
                {
                    OnStatusEvent(m_DtaToolNameLoc + " " + cmdStr);
                }

                //Setup a program runner tool to make the spectra files
                mCmdRunner = new clsRunDosProgram(m_WorkDir)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = string.Empty      // Allow the console output filename to be auto-generated
                };
                mCmdRunner.ErrorEvent += CmdRunner_ErrorEvent;
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                if (!mCmdRunner.RunProgram(m_DtaToolNameLoc, cmdStr, "MSConvert", true))
                {
                    // .RunProgram returned False
                    LogDTACreationStats("ConvertRawToMGF", Path.GetFileNameWithoutExtension(m_DtaToolNameLoc), "mCmdRunner.RunProgram returned False");

                    m_ErrMsg = "Error running " + Path.GetFileNameWithoutExtension(m_DtaToolNameLoc);
                    return false;
                }

                if (m_DebugLevel >= 2)
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
