//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
// Uses DeconMSn or ExtractMSn to create _DTA.txt file from a .Raw file

//*********************************************************************************************************

using System;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace DTASpectraFileGen
{
    /// <summary>
    /// This class creates DTA files using either DeconMSn.exe or ExtractMSn.exe
    /// </summary>
    /// <remarks></remarks>
    public class clsDtaGenThermoRaw : clsDtaGen
    {
        #region "Constants"

        protected const int DEFAULT_SCAN_STOP = 999999;

        private const string CONSOLE_OUTPUT_FILENAME = "DeconMSn_ConsoleOutput.txt";

        #endregion

        #region "Module variables"

        protected int m_NumScans;
        protected clsRunDosProgram mCmdRunner;
        private Thread m_thThread;

        protected int m_MaxScanInFile;
        private bool m_RunningExtractMSn;
        protected string m_InstrumentFileName = string.Empty;

        private FileSystemWatcher mDTAWatcher;
        private FileSystemWatcher mDeconMSnProgressWatcher;

        #endregion

        #region "API Declares"

        // API constants
        private const short OF_READ = 0x0;

        private const short OF_READWRITE = 0x2;
        private const short OF_WRITE = 0x1;
        private const short OF_SHARE_COMPAT = 0x0;
        private const short OF_SHARE_DENY_NONE = 0x40;
        private const short OF_SHARE_DENY_READ = 0x30;
        private const short OF_SHARE_DENY_WRITE = 0x20;

        private const short OF_SHARE_EXCLUSIVE = 0x10;
        public const string DECONMSN_FILENAME = "DeconMSn.exe";
        public const string EXTRACT_MSN_FILENAME = "extract_msn.exe";
        public const string MSCONVERT_FILENAME = "msconvert.exe";
        public const string DECON_CONSOLE_FILENAME = "DeconConsole.exe";
        public const string RAWCONVERTER_FILENAME = "RawConverter.exe";

        public const string DECONMSN_FILENAME_LOWER = "deconmsn.exe";
        public const string EXTRACT_MSN_FILENAME_LOWER = "extract_msn.exe";
        public const string MSCONVERT_FILENAME_LOWER = "msconvert.exe";
        public const string DECON_CONSOLE_FILENAME_LOWER = "deconconsole.exe";
        public const string RAWCONVERTER_FILENAME_LOWER = "rawconverter.exe";

        #endregion

        #region "Methods"

        public override void Setup(SpectraFileProcessorParams initParams, clsAnalysisToolRunnerBase toolRunner)
        {
            base.Setup(initParams, toolRunner);

            m_DtaToolNameLoc = ConstructDTAToolPath();
        }

        /// <summary>
        /// Starts DTA creation
        /// </summary>
        /// <returns>ProcessStatus value indicating success or failure</returns>
        /// <remarks></remarks>
        public override ProcessStatus Start()
        {
            m_Status = ProcessStatus.SF_STARTING;

            // Verify necessary files are in specified locations
            if (!InitSetup())
            {
                m_Results = ProcessResults.SF_FAILURE;
                m_Status = ProcessStatus.SF_ERROR;
                return m_Status;
            }

            if (!VerifyFileExists(m_DtaToolNameLoc))
            {
                m_Results = ProcessResults.SF_FAILURE;
                m_Status = ProcessStatus.SF_ERROR;
                return m_Status;
            }

            // Note that clsDtaGenMSConvert will update m_InstrumentFileName if processing a .mzXml file
            m_InstrumentFileName = m_Dataset + ".raw";

            // Make the DTA files (the process runs in a separate thread)
            try
            {
                m_thThread = new Thread(MakeDTAFilesThreaded);
                m_thThread.Start();
                m_Status = ProcessStatus.SF_RUNNING;
            }
            catch (Exception ex)
            {
                m_ErrMsg = "Error calling MakeDTAFilesThreaded";
                OnErrorEvent(m_ErrMsg, ex);
                m_Status = ProcessStatus.SF_ERROR;
            }

            return m_Status;
        }

        /// <summary>
        /// Returns the default path to the DTA generator tool
        /// </summary>
        /// <returns></returns>
        /// <remarks>The default path can be overridden by updating m_DtaToolNameLoc using clsDtaGen.UpdateDtaToolNameLoc</remarks>
        protected virtual string ConstructDTAToolPath()
        {
            var strDTAGenProgram = m_JobParams.GetJobParameter("DtaGenerator", "");
            string strDTAToolPath;

            if (strDTAGenProgram.ToLower() == EXTRACT_MSN_FILENAME.ToLower())
            {
                // Extract_MSn uses the lcqdtaloc folder path
                strDTAToolPath = Path.Combine(m_MgrParams.GetParam("lcqdtaloc", ""), strDTAGenProgram);
            }
            else
            {
                // DeconMSn uses the XcalDLLPath
                strDTAToolPath = Path.Combine(m_MgrParams.GetParam("XcalDLLPath", ""), strDTAGenProgram);
            }

            return strDTAToolPath;
        }

        /// <summary>
        /// Tests for existence of .raw file in specified location
        /// </summary>
        /// <param name="WorkDir">Directory where .raw file should be found</param>
        /// <param name="DSName">Name of dataset being processed</param>
        /// <returns>TRUE if file found; FALSE otherwise</returns>
        /// <remarks></remarks>
        private bool VerifyRawFileExists(string WorkDir, string DSName)
        {
            string dataFileExtension;

            // Verifies a the data file exists in specfied directory
            switch (m_RawDataType)
            {
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:
                    dataFileExtension = clsAnalysisResources.DOT_RAW_EXTENSION;
                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzXML:
                    dataFileExtension = clsAnalysisResources.DOT_MZXML_EXTENSION;
                    break;
                case clsAnalysisResources.eRawDataTypeConstants.mzML:
                    dataFileExtension = clsAnalysisResources.DOT_MZML_EXTENSION;
                    break;
                default:
                    LogError("Unsupported data type: " + m_RawDataType);
                    return false;
            }

            m_JobParams.AddResultFileToSkip(DSName + dataFileExtension);

            if (File.Exists(Path.Combine(WorkDir, DSName + dataFileExtension)))
            {
                m_ErrMsg = string.Empty;
                return true;
            }

            var mgfFileExtension = clsAnalysisResources.DOT_MGF_EXTENSION;
            if (File.Exists(Path.Combine(WorkDir, DSName + mgfFileExtension)))
            {
                m_ErrMsg = string.Empty;
                return true;
            }

            LogError("Instrument data file not found in working directory for dataset " + DSName);
            return false;
        }

        /// <summary>
        /// Initializes the class
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        protected override bool InitSetup()
        {
            // Verifies all necessary files exist in the specified locations

            if (m_DebugLevel > 0)
            {
                OnStatusEvent("clsDtaGenThermoRaw.InitSetup: Initializing DTA generator setup");
            }

            // Do tests specfied in base class
            if (!base.InitSetup())
                return false;

            // Raw data file exists?
            if (!VerifyRawFileExists(m_WorkDir, m_Dataset))
                return false; // Error message handled by VerifyRawFileExists

            // DTA creation tool exists?
            if (!VerifyFileExists(m_DtaToolNameLoc))
                return false; // Error message handled by VerifyFileExists

            // If we got to here, there was no problem
            return true;
        }

        /// <summary>
        /// Determines the maximum scan number in the .raw file
        /// </summary>
        /// <param name="rawFilePath">Data file name</param>
        /// <returns>Number of scans found</returns>
        protected int GetMaxScan(string rawFilePath)
        {
            var numScans = 0;

            var XRawFile = new MSFileReaderLib.MSFileReader_XRawfile();
            XRawFile.Open(rawFilePath);
            XRawFile.SetCurrentController(0, 1);
            XRawFile.GetNumSpectra(numScans);
            // XRawFile.GetFirstSpectrumNumber(StartScan)
            // XRawFile.GetLastSpectrumNumber(StopScan)
            XRawFile.Close();

            // Pause and garbage collect to allow release of file lock on .raw file
            // 1.5 second delay
            clsGlobal.IdleLoop(1.5);
            PRISM.clsProgRunner.GarbageCollectNow();

            return numScans;
        }

        /// <summary>
        /// Update m_ErrMsg and raise event OnErrorEvent
        /// </summary>
        /// <param name="errorMessage"></param>
        private void LogError(string errorMessage)
        {
            m_ErrMsg = errorMessage;
            OnErrorEvent(m_ErrMsg);
        }

        /// <summary>
        /// Thread for creation of DTA files
        /// </summary>
        /// <remarks></remarks>
        protected virtual void MakeDTAFilesThreaded()
        {
            m_Status = ProcessStatus.SF_RUNNING;
            if (!MakeDTAFiles())
            {
                if (m_Status != ProcessStatus.SF_ABORTING)
                {
                    m_Results = ProcessResults.SF_FAILURE;
                    m_Status = ProcessStatus.SF_ERROR;
                }
            }

            // Remove any files with non-standard file names (extract_msn artifact)
            if (!DeleteNonDosFiles())
            {
                if (m_Status != ProcessStatus.SF_ABORTING)
                {
                    m_Results = ProcessResults.SF_FAILURE;
                    m_Status = ProcessStatus.SF_ERROR;
                }
            }

            if (m_Status == ProcessStatus.SF_ABORTING)
            {
                m_Results = ProcessResults.SF_ABORTED;
            }
            else if (m_Status == ProcessStatus.SF_ERROR)
            {
                m_Results = ProcessResults.SF_FAILURE;
            }
            else
            {
                // Verify at least one dta file was created
                if (!VerifyDtaCreation())
                {
                    m_Results = ProcessResults.SF_NO_FILES_CREATED;
                }
                else
                {
                    // Processing succeded
                    // We don't need to keep the console output file long-term
                    m_JobParams.AddResultFileToSkip(CONSOLE_OUTPUT_FILENAME);
                    m_Results = ProcessResults.SF_SUCCESS;
                }

                m_Status = ProcessStatus.SF_COMPLETE;
            }
        }

        /// <summary>
        /// Method that actually makes the DTA files
        /// This functon is called by MakeDTAFilesThreaded
        /// </summary>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        private bool MakeDTAFiles()
        {
            const int LOOPING_CHUNK_SIZE = 25000;

            // Makes DTA files using extract_msn.exe or DeconMSn.exe
            // Warning: do not centroid spectra using DeconMSn since the masses reported when centroiding are not properly calibrated and thus could be off by 0.3 m/z or more

            Thread.CurrentThread.Name = "MakeDTAFiles";

            if (m_DebugLevel >= 1)
            {
                OnStatusEvent("Creating DTA files using " + Path.GetFileName(m_DtaToolNameLoc));
            }

            // Get the parameters from the various parameter dictionaries

            var strInstrumentDataFilePath = Path.Combine(m_WorkDir, m_InstrumentFileName);

            // Note: Defaults are used if certain parameters are not present in m_JobParams

            var scanStart = m_JobParams.GetJobParameter("ScanControl", "ScanStart", 1);
            var scanStop = m_JobParams.GetJobParameter("ScanControl", "ScanStop", DEFAULT_SCAN_STOP);

            // Note: Set MaxIntermediateScansWhenGrouping to 0 to disable grouping
            var maxIntermediateScansWhenGrouping = m_JobParams.GetJobParameter("MaxIntermediateScansWhenGrouping", 1);

            var mwLower = m_JobParams.GetJobParameter("MWControl", "MWStart", "200");
            var mwUpper = m_JobParams.GetJobParameter("MWControl", "MWStop", "5000");
            var ionCount = m_JobParams.GetJobParameter("IonCounts", "IonCount", "35");
            var massTol = m_JobParams.GetJobParameter("MassTol", "MassTol", "3");

            var createDefaultCharges = m_JobParams.GetJobParameter("Charges", "CreateDefaultCharges", true);

            var explicitChargeStart = (short)m_JobParams.GetJobParameter("Charges", "ExplicitChargeStart", 0);
            var explicitChargeEnd = (short)m_JobParams.GetJobParameter("Charges", "ExplicitChargeEnd", 0);

            // Get the maximum number of scans in the file
            var rawFilePath = string.Copy(strInstrumentDataFilePath);
            if (Path.GetExtension(strInstrumentDataFilePath).ToLower() != clsAnalysisResources.DOT_RAW_EXTENSION)
            {
                rawFilePath = Path.ChangeExtension(rawFilePath, clsAnalysisResources.DOT_RAW_EXTENSION);
            }

            if (File.Exists(rawFilePath))
            {
                m_MaxScanInFile = GetMaxScan(rawFilePath);
            }
            else
            {
                m_MaxScanInFile = 0;
            }

            switch (m_MaxScanInFile)
            {
                case -1:
                    // Generic error getting number of scans
                    LogError("Unknown error getting number of scans; Maxscan = " + m_MaxScanInFile);
                    return false;
                case 0:
                    // Unable to read file; treat this is a warning
                    LogError("Warning: unable to get maxscan; Maxscan = 0");
                    break;
                default:
                    if (m_MaxScanInFile > 0)
                    {
                        // This is normal, do nothing
                        break;
                    }
                    // This should never happen
                    LogError("Critical error getting number of scans; Maxscan = " + m_MaxScanInFile);
                    return false;
            }

            // Verify max scan specified is in file
            if (m_MaxScanInFile > 0)
            {
                if (scanStart == 1 && scanStop == 999999 && scanStop < m_MaxScanInFile)
                {
                    // The default scan range for processing all scans has traditionally be 1 to 999999
                    // This scan range is defined for this job's settings file, but this dataset has over 1 million spectra
                    // Assume that the user actually wants to analyze all of the spectra
                    scanStop = m_MaxScanInFile;
                }

                if (scanStop > m_MaxScanInFile)
                    scanStop = m_MaxScanInFile;
            }

            // Determine max number of scans to be performed
            m_NumScans = scanStop - scanStart + 1;

            // Setup a program runner tool to make the spectra files
            mCmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
            mCmdRunner.ErrorEvent += CmdRunner_ErrorEvent;
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            int locCharge;

            // Loop through the requested charge states, starting first with the default charges if appropriate
            if (createDefaultCharges)
            {
                locCharge = 0;
            }
            else
            {
                locCharge = explicitChargeStart;
            }

            m_RunningExtractMSn = m_DtaToolNameLoc.ToLower().Contains(EXTRACT_MSN_FILENAME.ToLower());

            if (m_RunningExtractMSn)
            {
                // Setup a FileSystemWatcher to watch for new .Dta files being created
                // We can compare the scan number of new .Dta files to the m_MaxScanInFile value to determine % complete
                mDTAWatcher = new FileSystemWatcher(m_WorkDir, "*.dta");
                mDTAWatcher.Created += mDTAWatcher_Created;

                mDTAWatcher.IncludeSubdirectories = false;
                mDTAWatcher.NotifyFilter = NotifyFilters.FileName | NotifyFilters.CreationTime;

                mDTAWatcher.EnableRaisingEvents = true;
            }
            else
            {
                // Running DeconMSn; it directly creates a _dta.txt file and we need to instead monitor the _DeconMSn_progress.txt file
                // Setup a FileSystemWatcher to watch for changes to this file
                mDeconMSnProgressWatcher = new FileSystemWatcher(m_WorkDir, m_Dataset + "_DeconMSn_progress.txt");
                mDeconMSnProgressWatcher.Changed += mDeconMSnProgressWatcher_Changed;

                mDeconMSnProgressWatcher.IncludeSubdirectories = false;
                mDeconMSnProgressWatcher.NotifyFilter = NotifyFilters.LastWrite;

                mDeconMSnProgressWatcher.EnableRaisingEvents = true;
            }

            while (locCharge <= explicitChargeEnd && !m_AbortRequested)
            {
                if (locCharge == 0 && createDefaultCharges || locCharge > 0)
                {
                    // If we are using extract_msn.exe, need to loop through .dta creation until no more files are created
                    // Limit to chunks of LOOPING_CHUNK_SIZE scans due to limitation of extract_msn.exe
                    // (only used if selected in manager settings, but "UseDTALooping" is typically set to True)

                    var LocScanStart = scanStart;
                    var LocScanStop = 0;

                    if (m_RunningExtractMSn && m_MgrParams.GetParam("UseDTALooping", false))
                    {
                        if (scanStop > (LocScanStart + LOOPING_CHUNK_SIZE))
                        {
                            LocScanStop = LocScanStart + LOOPING_CHUNK_SIZE;
                        }
                        else
                        {
                            LocScanStop = scanStop;
                        }
                    }
                    else
                    {
                        LocScanStop = scanStop;
                    }

                    // Loop until no more .dta files are created or ScanStop is reached
                    while (LocScanStart <= scanStop)
                    {
                        // Check for abort
                        if (m_AbortRequested)
                        {
                            m_Status = ProcessStatus.SF_ABORTING;
                            break;
                        }

                        // Set up command
                        var cmdStr = "-I" + ionCount + " -G1";
                        if (locCharge > 0)
                        {
                            cmdStr += " -C" + locCharge;
                        }

                        cmdStr += " -F" + LocScanStart + " -L" + LocScanStop;

                        // For ExtractMSn, -S means the number of allowed different intermediate scans for grouping (default=1), for example -S1
                        // For DeconMSn, -S means the type of spectra to process, for example -SALL or -SCID

                        if (m_RunningExtractMSn)
                        {
                            cmdStr += " -S" + maxIntermediateScansWhenGrouping;
                        }

                        cmdStr += " -B" + mwLower + " -T" + mwUpper + " -M" + massTol;
                        cmdStr += " -D" + m_WorkDir;

                        if (!m_RunningExtractMSn)
                        {
                            cmdStr += " -XCDTA -Progress";
                        }
                        cmdStr += " " + clsAnalysisToolRunnerBase.PossiblyQuotePath(Path.Combine(m_WorkDir, m_InstrumentFileName));

                        if (m_DebugLevel >= 1)
                        {
                            OnStatusEvent(m_DtaToolNameLoc + " " + cmdStr);
                        }

                        if (m_RunningExtractMSn)
                        {
                            // If running Extract_MSn, cannot cache the standard output
                            // clsProgRunner sometimes freezes on certain datasets (e.g. QC_Shew_10_05_pt5_1_24Jun10_Earth_10-05-10)
                            mCmdRunner.CreateNoWindow = false;
                            mCmdRunner.CacheStandardOutput = false;
                            mCmdRunner.EchoOutputToConsole = false;
                        }
                        else
                        {
                            mCmdRunner.CreateNoWindow = true;
                            mCmdRunner.CacheStandardOutput = true;
                            mCmdRunner.EchoOutputToConsole = true;

                            mCmdRunner.WriteConsoleOutputToFile = true;
                            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, CONSOLE_OUTPUT_FILENAME);

                            mCmdRunner.WorkDir = m_WorkDir;
                        }

                        m_ToolRunner.ResetProgRunnerCpuUsage();

                        if (!mCmdRunner.RunProgram(m_DtaToolNameLoc, cmdStr, "DTA_LCQ", true))
                        {
                            // .RunProgram returned False
                            LogDTACreationStats("clsDtaGenThermoRaw.MakeDTAFiles", Path.GetFileNameWithoutExtension(m_DtaToolNameLoc),
                                "m_RunProgTool.RunProgram returned False");

                            LogError("Error running " + Path.GetFileNameWithoutExtension(m_DtaToolNameLoc));
                            return false;
                        }

                        if (m_DebugLevel >= 2)
                        {
                            OnStatusEvent("clsDtaGenThermoRaw.MakeDTAFiles, RunProgram complete, thread " + Thread.CurrentThread.Name);
                        }

                        // Update loopy parameters
                        LocScanStart = LocScanStop + 1;
                        LocScanStop = LocScanStart + LOOPING_CHUNK_SIZE;
                        if (LocScanStop > scanStop)
                        {
                            LocScanStop = scanStop;
                        }
                    }
                }

                if (locCharge == 0)
                {
                    if (explicitChargeStart <= 0 | explicitChargeEnd <= 0)
                    {
                        break;
                    }

                    locCharge = explicitChargeStart;
                }
                else
                {
                    locCharge += 1;
                }
            }

            if (m_AbortRequested)
            {
                m_Status = ProcessStatus.SF_ABORTING;
            }

            // Disable the watchers
            if ((mDTAWatcher != null))
            {
                mDTAWatcher.EnableRaisingEvents = false;
            }

            if ((mDeconMSnProgressWatcher != null))
            {
                mDeconMSnProgressWatcher.EnableRaisingEvents = false;
            }

            if (m_DebugLevel >= 2)
            {
                OnStatusEvent("clsDtaGenThermoRaw.MakeDTAFiles, DTA creation loop complete, thread " + Thread.CurrentThread.Name);
            }

            // We got this far, everything must have worked
            if (m_Status == ProcessStatus.SF_ABORTING)
            {
                LogDTACreationStats("clsDtaGenThermoRaw.MakeDTAFiles", Path.GetFileNameWithoutExtension(m_DtaToolNameLoc),
                    "m_Status = ProcessStatus.SF_ABORTING");
                return false;
            }

            if (m_Status == ProcessStatus.SF_ERROR)
            {
                LogDTACreationStats("clsDtaGenThermoRaw.MakeDTAFiles", Path.GetFileNameWithoutExtension(m_DtaToolNameLoc),
                                    "m_Status = ProcessStatus.SF_ERROR ");
                return false;
            }

            return true;
        }

        protected virtual void MonitorProgress()
        {
            var FileList = Directory.GetFiles(m_WorkDir, "*.dta");
            m_SpectraFileCount = FileList.GetLength(0);
        }

        private void UpdateDeconMSnProgress(string progressFilePath)
        {
            var reNumber = new Regex(@"(\d+)");

            try
            {
                using (var swProgress = new StreamReader(new FileStream(progressFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!swProgress.EndOfStream)
                    {
                        var strLineIn = swProgress.ReadLine();
                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        if (strLineIn.StartsWith("Percent complete"))
                        {
                            var reMatch = reNumber.Match(strLineIn);
                            if (reMatch.Success)
                            {
                                float.TryParse(reMatch.Groups[1].Value, out m_Progress);
                            }
                        }

                        if (strLineIn.StartsWith("Number of MSn scans processed"))
                        {
                            var reMatch = reNumber.Match(strLineIn);
                            if (reMatch.Success)
                            {
                                int.TryParse(reMatch.Groups[1].Value, out m_SpectraFileCount);
                            }
                        }
                    }
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        private readonly Regex reDTAFile = new Regex(@"(\d+)\.\d+\.\d+\.dta$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private void UpdateDTAProgress(string DTAFileName)
        {
            try
            {
                // Extract out the scan number from the DTA filename
                var reMatch = reDTAFile.Match(DTAFileName);
                if (reMatch.Success)
                {
                    if (int.TryParse(reMatch.Groups[1].Value, out var scanNumber))
                    {
                        m_Progress = scanNumber / (float)m_MaxScanInFile * 100f;
                    }
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Verifies at least one DTA file was created
        /// </summary>
        /// <returns>TRUE if at least 1 file created; FALSE otherwise</returns>
        /// <remarks></remarks>
        private bool VerifyDtaCreation()
        {
            if (m_RunningExtractMSn)
            {
                // Verify at least one .dta file has been created
                // Returns the number of dta files in the working directory
                var FileList = Directory.GetFiles(m_WorkDir, "*.dta");

                if (FileList.GetLength(0) < 1)
                {
                    LogError("No dta files created");
                    return false;
                }
            }
            else
            {
                // Verify that the _dta.txt file was created
                var FileList = Directory.GetFiles(m_WorkDir, m_Dataset + "_dta.txt");

                if (FileList.GetLength(0) == 0)
                {
                    LogError("_dta.txt file was not created");
                    return false;
                }
            }

            return true;
        }

        #endregion

        #region "Event Handlers"

        /// <summary>
        /// Event handler for event CmdRunner.ErrorEvent
        /// </summary>
        /// <param name="strMessage"></param>
        /// <param name="ex"></param>
        protected void CmdRunner_ErrorEvent(string strMessage, Exception ex)
        {
            m_ErrMsg = strMessage;
            OnErrorEvent(strMessage, ex);
        }

        private DateTime dtLastDtaCountTime = DateTime.MinValue;
        private DateTime dtLastStatusUpdate = DateTime.MinValue;

        /// <summary>
        /// Event handler for LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        protected void CmdRunner_LoopWaiting()
        {
            // Synchronize the stored Debug level with the value stored in the database
            const int MGR_SETTINGS_UPDATE_INTERVAL_SECONDS = 300;
            clsAnalysisToolRunnerBase.GetCurrentMgrSettingsFromDB(MGR_SETTINGS_UPDATE_INTERVAL_SECONDS, m_MgrParams, ref m_DebugLevel);

            const int SECONDS_BETWEEN_UPDATE = 15;

            // Count the number of .Dta files or monitor the log file to determine the percent complete
            // (only count the files every 15 seconds)
            if (DateTime.UtcNow.Subtract(dtLastDtaCountTime).TotalSeconds >= SECONDS_BETWEEN_UPDATE)
            {
                dtLastDtaCountTime = DateTime.UtcNow;
                MonitorProgress();

                m_ToolRunner.UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);
            }

            // Update the status file (limit the updates to every 5 seconds)
            if (DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5)
            {
                dtLastStatusUpdate = DateTime.UtcNow;
                m_StatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.RUNNING_TOOL, m_Progress,
                    m_SpectraFileCount, "", "", "", false);
            }
        }

        private void mDTAWatcher_Created(object sender, FileSystemEventArgs e)
        {
            UpdateDTAProgress(e.Name);
        }

        #endregion

        private void mDeconMSnProgressWatcher_Changed(object sender, FileSystemEventArgs e)
        {
            UpdateDeconMSnProgress(e.FullPath);
        }
    }
}
