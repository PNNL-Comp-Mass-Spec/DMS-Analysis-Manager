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
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.StatusReporting;
using ThermoRawFileReader;

namespace DTASpectraFileGen
{
    /// <summary>
    /// This class creates DTA files using either DeconMSn.exe or ExtractMSn.exe
    /// </summary>
    public class DtaGenThermoRaw : DtaGen
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: dta, lcqdtaloc, centroiding, Decon, DeconMSn, MassTol, msconvert, msn, RawConverter, XcalDLL

        // ReSharper restore CommentTypo

        protected const int DEFAULT_SCAN_STOP = 99999999;

        private const string CONSOLE_OUTPUT_FILENAME = "DeconMSn_ConsoleOutput.txt";

        protected int mNumScans;
        protected RunDosProgram mCmdRunner;
        private System.Threading.Thread mDTAFileCreationThread;

        protected int mMaxScanInFile;
        private bool mRunningExtractMSn;
        protected string mInstrumentFileName = string.Empty;

        private FileSystemWatcher mDTAWatcher;
        private FileSystemWatcher mDeconMSnProgressWatcher;

        // ReSharper disable UnusedMember.Local

        // API constants
        private const short OF_READ = 0x0;

        private const short OF_READWRITE = 0x2;
        private const short OF_WRITE = 0x1;
        private const short OF_SHARE_COMPAT = 0x0;
        private const short OF_SHARE_DENY_NONE = 0x40;
        private const short OF_SHARE_DENY_READ = 0x30;
        private const short OF_SHARE_DENY_WRITE = 0x20;
        private const short OF_SHARE_EXCLUSIVE = 0x10;

        // ReSharper restore UnusedMember.Local

        public const string DECONMSN_FILENAME = "DeconMSn.exe";
        public const string EXTRACT_MSN_FILENAME = "extract_msn.exe";
        public const string MSCONVERT_FILENAME = "msconvert.exe";
        public const string DECON_CONSOLE_FILENAME = "DeconConsole.exe";

        // ReSharper disable once IdentifierTypo
        public const string RAWCONVERTER_FILENAME = "RawConverter.exe";

        public const string DECONMSN_FILENAME_LOWER = "deconmsn.exe";
        public const string EXTRACT_MSN_FILENAME_LOWER = "extract_msn.exe";
        public const string MSCONVERT_FILENAME_LOWER = "msconvert.exe";
        public const string DECON_CONSOLE_FILENAME_LOWER = "deconconsole.exe";

        // ReSharper disable once IdentifierTypo
        public const string RAWCONVERTER_FILENAME_LOWER = "rawconverter.exe";

        public override void Setup(SpectraFileProcessorParams initParams, AnalysisToolRunnerBase toolRunner)
        {
            base.Setup(initParams, toolRunner);

            mDtaToolNameLoc = ConstructDTAToolPath();
        }

        /// <summary>
        /// Starts DTA creation
        /// </summary>
        /// <returns>ProcessStatus value indicating success or failure</returns>
        public override ProcessStatus Start()
        {
            mStatus = ProcessStatus.SF_STARTING;

            // Verify necessary files are in specified locations
            if (!InitSetup())
            {
                mResults = ProcessResults.SF_FAILURE;
                mStatus = ProcessStatus.SF_ERROR;
                return mStatus;
            }

            if (!VerifyFileExists(mDtaToolNameLoc))
            {
                mResults = ProcessResults.SF_FAILURE;
                mStatus = ProcessStatus.SF_ERROR;
                return mStatus;
            }

            // Note that DtaGenMSConvert will update mInstrumentFileName if processing a .mzXml file
            mInstrumentFileName = mDatasetName + ".raw";

            const bool useSingleThread= false;

            // Make the DTA files
            try
            {
                // ReSharper disable HeuristicUnreachableCode
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
#pragma warning disable CS0162
                if (useSingleThread)
                {
                    MakeDTAFilesThreaded();
                }
#pragma warning restore CS0162
                // ReSharper restore HeuristicUnreachableCode
                else
                {
                    // Run the process in a separate thread
                    mDTAFileCreationThread = new System.Threading.Thread(MakeDTAFilesThreaded);
                    mDTAFileCreationThread.Start();
                    mStatus = ProcessStatus.SF_RUNNING;
                }
            }
            catch (Exception ex)
            {
                mErrMsg = "Error calling MakeDTAFilesThreaded";
                OnErrorEvent(mErrMsg, ex);
                mStatus = ProcessStatus.SF_ERROR;
            }

            return mStatus;
        }

        /// <summary>
        /// Returns the default path to the DTA generator tool
        /// </summary>
        /// <remarks>The default path can be overridden by updating mDtaToolNameLoc using DtaGen.UpdateDtaToolNameLoc</remarks>
        protected virtual string ConstructDTAToolPath()
        {
            var dtaGenProgram = mJobParams.GetJobParameter("DtaGenerator", "");

            // ReSharper disable CommentTypo
            // ReSharper disable StringLiteralTypo

            if (string.Equals(dtaGenProgram, EXTRACT_MSN_FILENAME, StringComparison.OrdinalIgnoreCase))
            {
                // Extract_MSn uses the lcqdtaloc folder path
                return Path.Combine(mMgrParams.GetParam("lcqdtaloc", ""), dtaGenProgram);
            }

            // DeconMSn uses the XcalDLLPath
            return Path.Combine(mMgrParams.GetParam("XcalDLLPath", ""), dtaGenProgram);

            // ReSharper restore StringLiteralTypo
            // ReSharper enable CommentTypo
        }

        /// <summary>
        /// Tests for existence of .raw file in specified location
        /// </summary>
        /// <param name="workDir">Directory where .raw file should be found</param>
        /// <param name="datasetName">Name of dataset being processed</param>
        /// <returns>True if file found, otherwise false</returns>
        private bool VerifyRawFileExists(string workDir, string datasetName)
        {
            string dataFileExtension;

            // Verifies that the data file exists in the specified directory
            switch (mRawDataType)
            {
                case AnalysisResources.RawDataTypeConstants.ThermoRawFile:
                    dataFileExtension = AnalysisResources.DOT_RAW_EXTENSION;
                    break;

                case AnalysisResources.RawDataTypeConstants.mzXML:
                    dataFileExtension = AnalysisResources.DOT_MZXML_EXTENSION;
                    break;

                case AnalysisResources.RawDataTypeConstants.mzML:
                    dataFileExtension = AnalysisResources.DOT_MZML_EXTENSION;
                    break;

                case AnalysisResources.RawDataTypeConstants.BrukerTOFTdf:
                    if (Directory.Exists(Path.Combine(workDir, datasetName + AnalysisResources.DOT_D_EXTENSION)))
                    {
                        mErrMsg = string.Empty;
                        return true;
                    }

                    LogError("Instrument directory not found in working directory for dataset " + datasetName);
                    return false;

                default:
                    LogError("Unsupported data type: " + mRawDataType);
                    return false;
            }

            mJobParams.AddResultFileToSkip(datasetName + dataFileExtension);

            if (File.Exists(Path.Combine(workDir, datasetName + dataFileExtension)))
            {
                mErrMsg = string.Empty;
                return true;
            }

            const string mgfFileExtension = AnalysisResources.DOT_MGF_EXTENSION;

            if (File.Exists(Path.Combine(workDir, datasetName + mgfFileExtension)))
            {
                mErrMsg = string.Empty;
                return true;
            }

            LogError("Instrument data file not found in working directory for dataset " + datasetName);
            return false;
        }

        /// <summary>
        /// Initializes the class
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        protected override bool InitSetup()
        {
            // Verifies all necessary files exist in the specified locations

            if (mDebugLevel > 0)
            {
                OnStatusEvent("DtaGenThermoRaw.InitSetup: Initializing DTA generator setup");
            }

            // Do the tests specified in the base class
            if (!base.InitSetup())
                return false;

            // Raw data file exists?
            if (!VerifyRawFileExists(mWorkDir, mDatasetName))
                return false; // Error message handled by VerifyRawFileExists

            // DTA creation tool exists?
            if (!VerifyFileExists(mDtaToolNameLoc))
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
            try
            {
                if (mDebugLevel >= 3)
                {
                    OnDebugEvent("Opening .raw file with ThermoRawFileReader.XRawFileIO: " + rawFilePath);
                }

                using var reader = new XRawFileIO(rawFilePath);

                var numScans = reader.FileInfo.ScanEnd;

                if (mDebugLevel >= 2)
                {
                    OnDebugEvent("Max scan for {0} is {1:N0}", Path.GetFileName(rawFilePath), numScans);
                }
                return numScans;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error determining the max scan number in the .raw file", ex);
                return 0;
            }
        }

        /// <summary>
        /// Update mErrMsg and raise event OnErrorEvent
        /// </summary>
        /// <param name="errorMessage"></param>
        protected void LogError(string errorMessage)
        {
            mErrMsg = errorMessage;
            OnErrorEvent(mErrMsg);
        }

        /// <summary>
        /// Thread for creation of DTA files
        /// </summary>
        protected virtual void MakeDTAFilesThreaded()
        {
            mStatus = ProcessStatus.SF_RUNNING;

            if (!MakeDTAFiles())
            {
                if (mStatus != ProcessStatus.SF_ABORTING)
                {
                    mResults = ProcessResults.SF_FAILURE;
                    mStatus = ProcessStatus.SF_ERROR;
                }
            }

            // Remove any files with non-standard file names (extract_msn artifact)
            if (!DeleteNonDosFiles())
            {
                if (mStatus != ProcessStatus.SF_ABORTING)
                {
                    mResults = ProcessResults.SF_FAILURE;
                    mStatus = ProcessStatus.SF_ERROR;
                }
            }

            if (mStatus == ProcessStatus.SF_ABORTING)
            {
                mResults = ProcessResults.SF_ABORTED;
            }
            else if (mStatus == ProcessStatus.SF_ERROR)
            {
                mResults = ProcessResults.SF_FAILURE;
            }
            else
            {
                // Verify at least one dta file was created
                if (!VerifyDtaCreation())
                {
                    mResults = ProcessResults.SF_NO_FILES_CREATED;
                }
                else
                {
                    // Processing succeeded
                    // We don't need to keep the console output file long-term
                    mJobParams.AddResultFileToSkip(CONSOLE_OUTPUT_FILENAME);
                    mResults = ProcessResults.SF_SUCCESS;
                }

                mStatus = ProcessStatus.SF_COMPLETE;
            }
        }

        /// <summary>
        /// Method that actually makes the DTA files
        /// This method is called by MakeDTAFilesThreaded
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        private bool MakeDTAFiles()
        {
            const int LOOPING_CHUNK_SIZE = 25000;

            // Makes DTA files using extract_msn.exe or DeconMSn.exe
            // Warning: do not centroid spectra using DeconMSn since the masses reported when centroiding are not properly calibrated and thus could be off by 0.3 m/z or more

            System.Threading.Thread.CurrentThread.Name = "MakeDTAFiles";

            if (mDebugLevel >= 1)
            {
                OnStatusEvent("Creating DTA files using " + Path.GetFileName(mDtaToolNameLoc));
            }

            // Get the parameters from the various parameter dictionaries

            var instrumentDataFilePath = Path.Combine(mWorkDir, mInstrumentFileName);

            // Note: Defaults are used if certain parameters are not present in mJobParams

            var scanStart = mJobParams.GetJobParameter("ScanControl", "ScanStart", 1);
            var scanStop = mJobParams.GetJobParameter("ScanControl", "ScanStop", DEFAULT_SCAN_STOP);

            // Note: Set MaxIntermediateScansWhenGrouping to 0 to disable grouping
            var maxIntermediateScansWhenGrouping = mJobParams.GetJobParameter("MaxIntermediateScansWhenGrouping", 1);

            var mwLower = mJobParams.GetJobParameter("MWControl", "MWStart", "200");
            var mwUpper = mJobParams.GetJobParameter("MWControl", "MWStop", "5000");
            var ionCount = mJobParams.GetJobParameter("IonCounts", "IonCount", "35");
            var massTol = mJobParams.GetJobParameter("MassTol", "MassTol", "3");

            var createDefaultCharges = mJobParams.GetJobParameter("Charges", "CreateDefaultCharges", true);

            var explicitChargeStart = (short)mJobParams.GetJobParameter("Charges", "ExplicitChargeStart", 0);
            var explicitChargeEnd = (short)mJobParams.GetJobParameter("Charges", "ExplicitChargeEnd", 0);

            // Get the maximum number of scans in the file
            var rawFilePath = instrumentDataFilePath;

            if (!string.Equals(Path.GetExtension(instrumentDataFilePath), AnalysisResources.DOT_RAW_EXTENSION, StringComparison.OrdinalIgnoreCase))
            {
                rawFilePath = Path.ChangeExtension(rawFilePath, AnalysisResources.DOT_RAW_EXTENSION);
            }

            if (File.Exists(rawFilePath))
            {
                mMaxScanInFile = GetMaxScan(rawFilePath);
            }
            else
            {
                mMaxScanInFile = 0;
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

            // Verify max scan specified is in file
            if (mMaxScanInFile > 0)
            {
                if (scanStop == 999999 && scanStop < mMaxScanInFile)
                {
                    // The default scan range for processing all scans has traditionally been 1 to 999999
                    // This scan range is defined for this job's settings file, but this dataset has over 1 million spectra
                    // Assume that the user actually wants to analyze every spectrum
                    scanStop = mMaxScanInFile;
                }

                if (scanStop > mMaxScanInFile)
                    scanStop = mMaxScanInFile;
            }

            // Determine max number of scans to be performed
            mNumScans = scanStop - scanStart + 1;

            // Set up a program runner tool to make the spectra files
            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
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

            mRunningExtractMSn = mDtaToolNameLoc.IndexOf(EXTRACT_MSN_FILENAME, StringComparison.OrdinalIgnoreCase) >= 0;

            if (mRunningExtractMSn)
            {
                // Set up a FileSystemWatcher to watch for new .Dta files being created
                // We can compare the scan number of new .Dta files to the mMaxScanInFile value to determine % complete
                mDTAWatcher = new FileSystemWatcher(mWorkDir, "*.dta");
                mDTAWatcher.Created += DTAWatcher_Created;

                mDTAWatcher.IncludeSubdirectories = false;
                mDTAWatcher.NotifyFilter = NotifyFilters.FileName | NotifyFilters.CreationTime;

                mDTAWatcher.EnableRaisingEvents = true;
            }
            else
            {
                // Running DeconMSn; it directly creates a _dta.txt file, and we need to instead monitor the _DeconMSn_progress.txt file
                // Set up a FileSystemWatcher to watch for changes to this file
                mDeconMSnProgressWatcher = new FileSystemWatcher(mWorkDir, mDatasetName + "_DeconMSn_progress.txt");
                mDeconMSnProgressWatcher.Changed += DeconMSnProgressWatcher_Changed;

                mDeconMSnProgressWatcher.IncludeSubdirectories = false;
                mDeconMSnProgressWatcher.NotifyFilter = NotifyFilters.LastWrite;

                mDeconMSnProgressWatcher.EnableRaisingEvents = true;
            }

            while (locCharge <= explicitChargeEnd && !mAbortRequested)
            {
                if (locCharge == 0 && createDefaultCharges || locCharge > 0)
                {
                    // If we are using extract_msn.exe, need to loop through .dta creation until no more files are created
                    // Limit to chunks of LOOPING_CHUNK_SIZE scans due to limitation of extract_msn.exe
                    // (only used if selected in manager settings, but "UseDTALooping" is typically set to true)

                    var locScanStart = scanStart;
                    int locScanStop;

                    if (mRunningExtractMSn && mMgrParams.GetParam("UseDTALooping", false))
                    {
                        if (scanStop > (locScanStart + LOOPING_CHUNK_SIZE))
                        {
                            locScanStop = locScanStart + LOOPING_CHUNK_SIZE;
                        }
                        else
                        {
                            locScanStop = scanStop;
                        }
                    }
                    else
                    {
                        locScanStop = scanStop;
                    }

                    // Loop until no more .dta files are created or ScanStop is reached
                    while (locScanStart <= scanStop)
                    {
                        // Check for abort
                        if (mAbortRequested)
                        {
                            mStatus = ProcessStatus.SF_ABORTING;
                            break;
                        }

                        // ReSharper disable StringLiteralTypo

                        // Set up command
                        var arguments = " -I" + ionCount +
                                        " -G1";

                        if (locCharge > 0)
                        {
                            arguments += " -C" + locCharge;
                        }

                        arguments += " -F" + locScanStart +
                                     " -L" + locScanStop;

                        // For ExtractMSn, -S means the number of allowed different intermediate scans for grouping (default=1), for example -S1
                        // For DeconMSn, -S means the type of spectra to process, for example -SALL or -SCID

                        if (mRunningExtractMSn)
                        {
                            arguments += " -S" + maxIntermediateScansWhenGrouping;
                        }

                        arguments += " -B" + mwLower +
                                     " -T" + mwUpper +
                                     " -M" + massTol +
                                     " -D" + mWorkDir;

                        if (!mRunningExtractMSn)
                        {
                            arguments += " -XCDTA -Progress";
                        }
                        arguments += " " + AnalysisToolRunnerBase.PossiblyQuotePath(Path.Combine(mWorkDir, mInstrumentFileName));

                        // ReSharper restore StringLiteralTypo

                        if (mDebugLevel >= 1)
                        {
                            OnStatusEvent(mDtaToolNameLoc + " " + arguments);
                        }

                        if (mRunningExtractMSn)
                        {
                            // If running Extract_MSn, cannot cache the standard output
                            // ProgRunner sometimes freezes on certain datasets (e.g. QC_Shew_10_05_pt5_1_24Jun10_Earth_10-05-10)
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
                            mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, CONSOLE_OUTPUT_FILENAME);

                            mCmdRunner.WorkDir = mWorkDir;
                        }

                        mToolRunner.ResetProgRunnerCpuUsage();

                        if (!mCmdRunner.RunProgram(mDtaToolNameLoc, arguments, "DTA_LCQ", true))
                        {
                            // .RunProgram returned false
                            LogDTACreationStats("DtaGenThermoRaw.MakeDTAFiles", Path.GetFileNameWithoutExtension(mDtaToolNameLoc),
                                "m_RunProgTool.RunProgram returned false");

                            LogError("Error running " + Path.GetFileNameWithoutExtension(mDtaToolNameLoc));
                            return false;
                        }

                        if (mDebugLevel >= 2)
                        {
                            OnStatusEvent("DtaGenThermoRaw.MakeDTAFiles, RunProgram complete, thread " + System.Threading.Thread.CurrentThread.Name);
                        }

                        // Update loopy parameters
                        locScanStart = locScanStop + 1;
                        locScanStop = locScanStart + LOOPING_CHUNK_SIZE;

                        if (locScanStop > scanStop)
                        {
                            locScanStop = scanStop;
                        }
                    }
                }

                if (locCharge == 0)
                {
                    if (explicitChargeStart <= 0 || explicitChargeEnd <= 0)
                    {
                        break;
                    }

                    locCharge = explicitChargeStart;
                }
                else
                {
                    locCharge++;
                }
            }

            if (mAbortRequested)
            {
                mStatus = ProcessStatus.SF_ABORTING;
            }

            // Disable the watchers
            if (mDTAWatcher != null)
            {
                mDTAWatcher.EnableRaisingEvents = false;
            }

            if (mDeconMSnProgressWatcher != null)
            {
                mDeconMSnProgressWatcher.EnableRaisingEvents = false;
            }

            if (mDebugLevel >= 2)
            {
                OnStatusEvent("DtaGenThermoRaw.MakeDTAFiles, DTA creation loop complete, thread " + System.Threading.Thread.CurrentThread.Name);
            }

            // We got this far, everything must have worked
            if (mStatus == ProcessStatus.SF_ABORTING)
            {
                LogDTACreationStats("DtaGenThermoRaw.MakeDTAFiles", Path.GetFileNameWithoutExtension(mDtaToolNameLoc),
                    "mStatus = ProcessStatus.SF_ABORTING");
                return false;
            }

            if (mStatus == ProcessStatus.SF_ERROR)
            {
                LogDTACreationStats("DtaGenThermoRaw.MakeDTAFiles", Path.GetFileNameWithoutExtension(mDtaToolNameLoc),
                                    "mStatus = ProcessStatus.SF_ERROR ");
                return false;
            }

            return true;
        }

        protected virtual void MonitorProgress()
        {
            var foundFiles = Directory.GetFiles(mWorkDir, "*.dta");
            mSpectraFileCount = foundFiles.GetLength(0);
        }

        private void UpdateDeconMSnProgress(string progressFilePath)
        {
            var reNumber = new Regex(@"(\d+)");

            try
            {
                using var reader = new StreamReader(new FileStream(progressFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    if (dataLine.StartsWith("Percent complete"))
                    {
                        var reMatch = reNumber.Match(dataLine);

                        if (reMatch.Success)
                        {
                            float.TryParse(reMatch.Groups[1].Value, out mProgress);
                        }
                    }

                    if (dataLine.StartsWith("Number of MSn scans processed"))
                    {
                        var reMatch = reNumber.Match(dataLine);

                        if (reMatch.Success)
                        {
                            int.TryParse(reMatch.Groups[1].Value, out mSpectraFileCount);
                        }
                    }
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        private readonly Regex reDTAFile = new(@"(\d+)\.\d+\.\d+\.dta$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private void UpdateDTAProgress(string dtaFileName)
        {
            try
            {
                // Extract out the scan number from the DTA filename
                var reMatch = reDTAFile.Match(dtaFileName);

                if (reMatch.Success)
                {
                    if (int.TryParse(reMatch.Groups[1].Value, out var scanNumber))
                    {
                        mProgress = scanNumber / (float)mMaxScanInFile * 100f;
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
        /// <returns>True if at least 1 file created, otherwise false</returns>
        private bool VerifyDtaCreation()
        {
            if (mRunningExtractMSn)
            {
                // Verify at least one .dta file has been created
                // Returns the number of dta files in the working directory
                var foundFiles = Directory.GetFiles(mWorkDir, "*.dta");

                if (foundFiles.GetLength(0) < 1)
                {
                    LogError("No dta files created");
                    return false;
                }
            }
            else
            {
                // Verify that the _dta.txt file was created
                var foundFiles = Directory.GetFiles(mWorkDir, mDatasetName + AnalysisResources.CDTA_EXTENSION);

                if (foundFiles.GetLength(0) == 0)
                {
                    LogError("_dta.txt file was not created");
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Event handler for event CmdRunner.ErrorEvent
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex"></param>
        protected void CmdRunner_ErrorEvent(string message, Exception ex)
        {
            mErrMsg = message;
            OnErrorEvent(message, ex);
        }

        private DateTime mLastDtaCountTime = DateTime.MinValue;
        private DateTime mLastStatusUpdate = DateTime.MinValue;

        /// <summary>
        /// Event handler for LoopWaiting event
        /// </summary>
        protected void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 15;

            // Count the number of .Dta files or monitor the log file to determine the percent complete
            // (only count the files every 15 seconds)
            if (DateTime.UtcNow.Subtract(mLastDtaCountTime).TotalSeconds >= SECONDS_BETWEEN_UPDATE)
            {
                mLastDtaCountTime = DateTime.UtcNow;
                MonitorProgress();

                mToolRunner.UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);
            }

            // Update the status file (limit the updates to every 5 seconds)
            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds >= 5)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                mStatusTools.UpdateAndWrite(MgrStatusCodes.RUNNING, TaskStatusCodes.RUNNING, TaskStatusDetailCodes.RUNNING_TOOL, mProgress,
                    mSpectraFileCount, "", "", "", false);
            }
        }

        private void DTAWatcher_Created(object sender, FileSystemEventArgs e)
        {
            UpdateDTAProgress(e.Name);
        }

        private void DeconMSnProgressWatcher_Changed(object sender, FileSystemEventArgs e)
        {
            UpdateDeconMSnProgress(e.FullPath);
        }
    }
}
