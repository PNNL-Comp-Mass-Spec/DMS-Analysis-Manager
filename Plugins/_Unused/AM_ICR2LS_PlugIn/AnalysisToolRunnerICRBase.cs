using AnalysisManagerBase;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using PRISM;

namespace AnalysisManagerICR2LSPlugIn
{
    /// <summary>
    /// Base class for running ICR-2LS
    /// </summary>
    public abstract class AnalysisToolRunnerICRBase : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: acqus, CmdRunner, deisotoping, PEKtoCSV
        // Ignore Spelling: ticgeneration, lcqticgeneration, qtofpekgeneration, mmtofpekgeneration, ltqftpekgeneration

        // ReSharper restore CommentTypo

        private const string ICR2LS_STATE_UNKNOWN = "unknown";
        private const string ICR2LS_STATE_IDLE = "idle";
        private const string ICR2LS_STATE_PROCESSING = "processing";
        private const string ICR2LS_STATE_KILLED = "killed";
        private const string ICR2LS_STATE_ERROR = "error";
        private const string ICR2LS_STATE_FINISHED = "finished";
        private const string ICR2LS_STATE_GENERATING = "generating";
        private const string ICR2LS_STATE_TICGENERATION = "ticgeneration";
        private const string ICR2LS_STATE_LCQTICGENERATION = "lcqticgeneration";
        private const string ICR2LS_STATE_QTOFPEKGENERATION = "qtofpekgeneration";
        private const string ICR2LS_STATE_MMTOFPEKGENERATION = "mmtofpekgeneration";
        private const string ICR2LS_STATE_LTQFTPEKGENERATION = "ltqftpekgeneration";

        public const string PEK_TEMP_FILE = ".pek.tmp";

        private const string APEX_ACQUISITION_METHOD_FILE = "apexAcquisition.method";

        public enum ICR2LSProcessingModeConstants
        {
            LTQFTPEK = 0,
            LTQFTTIC = 1,
            SFoldersPEK = 2,
            SFoldersTIC = 3,
            SerFolderPEK = 4,
            SerFolderTIC = 5,
            SerFilePEK = 6,
            SerFileTIC = 7
        }

        private struct ICR2LSStatus
        {
            public int ScansProcessed;
            public float PercentComplete;
            public string ProcessingState;              // Typical values: Processing, Finished, etc.
            public string ProcessingStatus;             // Typical values: LTQFTPEKGENERATION, GENERATING

            public void Initialize()
            {
                ScansProcessed = 0;
                PercentComplete = 0;
                ProcessingState = ICR2LS_STATE_UNKNOWN;
                ProcessingStatus = string.Empty;
            }
        }

        private string mStatusFilePath = string.Empty;

        // Obsolete
        // Private mMinScanOffset As Integer = 0

        private DateTime mLastErrorPostingTime;
        private DateTime mLastMissingStatusFileTime;
        private DateTime mLastInvalidStatusFileTime;

        private DateTime mLastStatusParseTime = DateTime.UtcNow;
        private DateTime mLastStatusLogTime = DateTime.UtcNow;

        private FileInfo mPEKResultsFile;
        private DateTime mLastCheckpointTime = DateTime.UtcNow;

        private ICR2LSStatus mICR2LSStatus;

        private RunDosProgram mCmdRunner;
        private FileSystemWatcher mStatusFileWatcher;
        private PEKtoCSVConverter.PEKtoCSVConverter mPEKtoCSVConverter;

        private DateTime mLastPekToCsvPercentCompleteTime;

        protected AnalysisToolRunnerICRBase()
        {
            ResetStatusLogTimes();

            mICR2LSStatus.Initialize();
        }

        /// <summary>
        /// Primary entry point for running this tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
        public override CloseOutType RunTool()
        {
            // Get the settings file info via the base class
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                return CloseOutType.CLOSEOUT_FAILED;

            // Start the job timer
            mStartTime = DateTime.UtcNow;

            ResetStatusLogTimes();
            mICR2LSStatus.Initialize();

            // Remainder of tasks are in subclass (which should call this using MyBase.Runtool)
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool ConvertPekToCsv(string pekFilePath)
        {
            try
            {
                var scansFilePath = Path.Combine(mWorkDir, mDatasetName + "_scans.csv");
                var isosFilePath = Path.Combine(mWorkDir, mDatasetName + "_isos.csv");
                var rawFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_RAW_EXTENSION);

                if (!File.Exists(rawFilePath))
                {
                    rawFilePath = string.Empty;
                }

                mPEKtoCSVConverter = new PEKtoCSVConverter.PEKtoCSVConverter(pekFilePath, scansFilePath, isosFilePath, rawFilePath);
                mPEKtoCSVConverter.ErrorEvent += PEKtoCSVConverter_ErrorEvent;
                mPEKtoCSVConverter.MessageEvent += PEKtoCSVConverter_MessageEvent;

                LogMessage("Creating _isos.csv and _scans.csv files using the PEK file");
                mLastPekToCsvPercentCompleteTime = DateTime.UtcNow;

                var success = mPEKtoCSVConverter.Convert();

                mPEKtoCSVConverter = null;
                PRISM.ProgRunner.GarbageCollectNow();

                return success;
            }
            catch (Exception ex)
            {
                mMessage = "Error converting the PEK file to DeconTools-compatible _isos.csv";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }
        }

        private void CopyCheckpointFile()
        {
            try
            {
                if (mPEKResultsFile == null)
                    return;

                if (!mPEKResultsFile.Exists)
                {
                    mPEKResultsFile.Refresh();

                    if (!mPEKResultsFile.Exists)
                        return;
                }

                var transferDirectoryPath = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_TRANSFER_DIRECTORY_PATH);

                if (string.IsNullOrEmpty(transferDirectoryPath))
                    return;

                transferDirectoryPath = Path.Combine(transferDirectoryPath, mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME));
                transferDirectoryPath = Path.Combine(transferDirectoryPath, mJobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME));

                var transferDirectory = new DirectoryInfo(transferDirectoryPath);

                if (!transferDirectory.Exists)
                {
                    transferDirectory.Create();
                }

                var targetFileFinal = new FileInfo(Path.Combine(transferDirectory.FullName, Path.GetFileNameWithoutExtension(mPEKResultsFile.Name) + PEK_TEMP_FILE));
                var targetFileTemp = new FileInfo(targetFileFinal.FullName + ".new");

                mPEKResultsFile.CopyTo(targetFileTemp.FullName, true);

                targetFileTemp.Refresh();

                if (targetFileFinal.Exists)
                    targetFileFinal.Delete();

                targetFileTemp.MoveTo(targetFileFinal.FullName);

                mJobParams.AddServerFileToDelete(targetFileFinal.FullName);
            }
            catch (Exception ex)
            {
                LogError("Exception copying the interim .PEK file to the transfer folder: " + ex.Message);
            }
        }

        protected abstract CloseOutType DeleteDataFile();

        private int GetLastScanInPEKFile(string pekTempFilePath)
        {
            var currentScan = 0;
            var lastValidScan = 0;
            var reScanNumber = new Regex(@"Scan = (\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var reScanNumberFromFilename = new Regex(@"Filename: .+ Scan.(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var closingMessages = new List<string>
            {
                "Number of isotopic distributions detected",
                "Processing stop time",
                "Number of peaks in spectrum"
            };

            try
            {
                using var reader = new StreamReader(new FileStream(pekTempFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var reMatch = reScanNumber.Match(dataLine);

                    if (!reMatch.Success)
                    {
                        reMatch = reScanNumberFromFilename.Match(dataLine);
                    }

                    if (reMatch.Success)
                    {
                        int.TryParse(reMatch.Groups[1].Value, out currentScan);
                    }

                    foreach (var closingMessage in closingMessages)
                    {
                        if (dataLine.StartsWith(closingMessage))
                        {
                            lastValidScan = currentScan;
                        }
                    }
                }

                return lastValidScan;
            }
            catch (Exception ex)
            {
                LogError("Exception in GetLastScanInPEKFile: " + ex.Message);
                return 0;
            }
        }

        /// <summary>
        /// Reads the ICR2LS Status file and updates mICR2LSStatus
        /// </summary>
        /// <param name="statusFilePath"></param>
        /// <param name="forceParse"></param>
        private bool ParseICR2LSStatusFile(string statusFilePath, bool forceParse)
        {
            const int MINIMUM_PARSING_INTERVAL_SECONDS = 4;

            var processingState = mICR2LSStatus.ProcessingState;
            var processingStatus = string.Empty;
            var scansProcessed = 0;

            var statusDate = string.Empty;
            var statusTime = string.Empty;

            try
            {
                if (string.IsNullOrEmpty(statusFilePath))
                {
                    return false;
                }

                if (!forceParse && DateTime.UtcNow.Subtract(mLastStatusParseTime).TotalSeconds < MINIMUM_PARSING_INTERVAL_SECONDS)
                {
                    // Not enough time has elapsed, exit the procedure (returning True)
                    return true;
                }

                mLastStatusParseTime = DateTime.UtcNow;

                if (!File.Exists(statusFilePath))
                {
                    // Status.log file not found; if the job just started, this will be the case
                    // For this reason, ResetStatusLogTimes will set mLastMissingStatusFileTime to the time the job starts, meaning
                    //  we won't log an error about a missing Status.log file until 60 minutes into a job
                    if (DateTime.UtcNow.Subtract(mLastMissingStatusFileTime).TotalMinutes >= 60)
                    {
                        mLastMissingStatusFileTime = DateTime.UtcNow;
                        LogWarning("ICR2LS Status.Log file not found: " + statusFilePath);
                    }

                    return true;
                }

                // Read the file
                using (var reader = new StreamReader(new FileStream(statusFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var charIndex = dataLine.IndexOf('=');

                        if (charIndex <= 0)
                            continue;

                        var key = dataLine.Substring(0, charIndex).Trim();
                        var value = dataLine.Substring(charIndex + 1).Trim();

                        if (key.Equals("date", StringComparison.OrdinalIgnoreCase))
                        {
                            statusDate = value;
                        }
                        else if (key.Equals("time", StringComparison.OrdinalIgnoreCase))
                        {
                            statusTime = value;
                        }
                        else if (key.Equals("ScansProcessed", StringComparison.OrdinalIgnoreCase))
                        {
                            if (int.TryParse(value, out var result))
                            {
                                // Old: The ScansProcessed value reported by ICR-2LS is actually the scan number of the most recently processed scan
                                // If we use /F to start with a scan other than 1, this ScansProcessed value does not reflect reality
                                // To correct for this, subtract out mMinScanOffset
                                // scansProcessed = result - mMinScanOffset

                                // New: ScansProcessed is truly the number of scans processed
                                scansProcessed = result;

                                if (scansProcessed < 0)
                                {
                                    scansProcessed = result;
                                }
                            }
                        }
                        else if (key.Equals("PercentComplete", StringComparison.OrdinalIgnoreCase))
                        {
                            if (float.TryParse(value, out var result))
                            {
                                mICR2LSStatus.PercentComplete = result;
                            }
                        }
                        else if (key.Equals("state", StringComparison.OrdinalIgnoreCase))
                        {
                            // Example values: Processing, Finished
                            processingState = value;
                        }
                        else if (key.Equals("status", StringComparison.OrdinalIgnoreCase))
                        {
                            // Example value: LTQFTPEKGENERATION
                            processingStatus = value;
                        }
                        else if (key.Equals("ErrorMessage", StringComparison.OrdinalIgnoreCase))
                        {
                            ConsoleMsgUtils.ShowWarning("Error message from the ICR2LS Status File: " + value);
                        }
                    }
                }

                if (statusDate.Length > 0 && statusTime.Length > 0)
                {
                    statusDate += " " + statusTime;

                    if (!DateTime.TryParse(statusDate, out _))
                    {
                    }
                }

                if (scansProcessed > mICR2LSStatus.ScansProcessed)
                {
                    // Only update .ScansProcessed if the new value is larger than the previous one
                    // This is necessary since ICR-2LS will set ScansProcessed to 0 when the state is Finished
                    mICR2LSStatus.ScansProcessed = scansProcessed;
                }

                if (!string.IsNullOrEmpty(processingState))
                {
                    mICR2LSStatus.ProcessingState = processingState;

                    if (!ValidateICR2LSStatus(processingState))
                    {
                        if (DateTime.UtcNow.Subtract(mLastInvalidStatusFileTime).TotalMinutes >= 15)
                        {
                            mLastInvalidStatusFileTime = DateTime.UtcNow;
                            LogWarning("Invalid processing state reported by ICR2LS: " + processingState);
                        }
                    }
                }

                if (processingStatus.Length > 0)
                {
                    mICR2LSStatus.ProcessingStatus = processingStatus;
                }

                mProgress = mICR2LSStatus.PercentComplete;

                // Update the local status file (and post the status to the message queue)
                UpdateStatusRunning(mProgress, mICR2LSStatus.ScansProcessed);

                return true;
            }
            catch (Exception ex)
            {
                // Limit logging of errors to once every 60 minutes

                if (DateTime.UtcNow.Subtract(mLastErrorPostingTime).TotalMinutes >= 60)
                {
                    mLastErrorPostingTime = DateTime.UtcNow;
                    LogWarning("Error reading the ICR2LS Status.Log file (" + statusFilePath + "): " + ex.Message);
                }

                return false;
            }
        }

        private void InitializeStatusLogFileWatcher(string workDir, string filenameToWatch)
        {
            mStatusFileWatcher = new FileSystemWatcher();
            mStatusFileWatcher.Changed += StatusFileWatcher_Changed;
            mStatusFileWatcher.BeginInit();
            mStatusFileWatcher.Path = workDir;
            mStatusFileWatcher.IncludeSubdirectories = false;
            mStatusFileWatcher.Filter = Path.GetFileName(filenameToWatch);
            mStatusFileWatcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size;
            mStatusFileWatcher.EndInit();
            mStatusFileWatcher.EnableRaisingEvents = true;
        }

        protected virtual CloseOutType PerfPostAnalysisTasks(bool copyResultsToServer)
        {
            // Stop the job timer
            mStopTime = DateTime.UtcNow;

            UpdateSummaryFile();

            // Use the PEK file to create DeconTools compatible _isos.csv and _scans.csv files
            // Create this CSV file even if ICR-2LS did not successfully finish
            var pekFilePath = Path.Combine(mWorkDir, mDatasetName + ".pek");
            var pekConversionSuccess = ConvertPekToCsv(pekFilePath);

            // Get rid of raw data file
            var result = DeleteDataFile();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Error deleting raw files; the error will have already been logged
                // Since the results might still be good, we will not return an error at this point
            }

            mJobParams.AddResultFileToSkip("Status.log");

            var copySuccess = CopyResultsToTransferDirectory();

            if (!copySuccess)
                return CloseOutType.CLOSEOUT_FAILED;

            if (pekConversionSuccess)
            {
                // We can now safely delete the .pek.tmp file from the server
                RemoveNonResultServerFiles();

                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            if (string.IsNullOrEmpty(mMessage))
            {
                mMessage = "Unknown error converting the .PEK file to a DeconTools-compatible _isos.csv file";
            }
            return CloseOutType.CLOSEOUT_FAILED;
        }

        private void ResetStatusLogTimes()
        {
            // Initialize the last error posting time to 2 hours before the present
            mLastErrorPostingTime = DateTime.UtcNow.Subtract(new TimeSpan(2, 0, 0));

            // Initialize the last MissingStatusFileTime to the time the job starts
            mLastMissingStatusFileTime = DateTime.UtcNow;

            mLastInvalidStatusFileTime = DateTime.UtcNow.Subtract(new TimeSpan(2, 0, 0));
        }

        /// <summary>
        /// Starts ICR-2LS by running the .Exe at the command line
        /// </summary>
        /// <param name="datasetNamePath"></param>
        /// <param name="paramFilePath"></param>
        /// <param name="resultsFileNamePath"></param>
        /// <param name="icr2lsMode"></param>
        /// <returns>True if successfully started; otherwise false</returns>
        protected bool StartICR2LS(string datasetNamePath, string paramFilePath, string resultsFileNamePath, ICR2LSProcessingModeConstants icr2lsMode)
        {
            return StartICR2LS(datasetNamePath, paramFilePath, resultsFileNamePath, icr2lsMode, true, false, 0, 0);
        }

        /// <summary>
        /// Run ICR-2LS on the file (or 0.ser folder) specified by datasetNamePath
        /// </summary>
        /// <param name="instrumentFilePath"></param>
        /// <param name="paramFilePath"></param>
        /// <param name="resultsFileNamePath"></param>
        /// <param name="icr2lsMode"></param>
        /// <param name="useAllScans"></param>
        /// <param name="skipMS2"></param>
        /// <param name="minScan"></param>
        /// <param name="maxScan"></param>
        protected bool StartICR2LS(string instrumentFilePath, string paramFilePath, string resultsFileNamePath,
            ICR2LSProcessingModeConstants icr2lsMode, bool useAllScans, bool skipMS2, int minScan, int maxScan)
        {
            const int MONITOR_INTERVAL_SECONDS = 4;

            var logFolder = Path.GetDirectoryName(resultsFileNamePath);

            if (string.IsNullOrWhiteSpace(logFolder))
                mStatusFilePath = "Status.log";
            else
                mStatusFilePath = Path.Combine(logFolder, "Status.log");

            // Create a file watcher to monitor the status.log file created by ICR-2LS
            // This file is updated after each scan is processed
            InitializeStatusLogFileWatcher(Path.GetDirectoryName(mStatusFilePath), Path.GetFileName(mStatusFilePath));

            var exeFilePath = mMgrParams.GetParam("ICR2LSProgLoc");

            if (string.IsNullOrEmpty(exeFilePath))
            {
                LogError("Job parameter ICR2LSProgLoc is not defined; unable to run ICR-2LS");
                return false;
            }

            if (!File.Exists(exeFilePath))
            {
                LogError("ICR-2LS path not found: " + exeFilePath);
                return false;
            }

            // Look for an existing .pek.tmp file
            var scanToResumeAfter = 0;

            mPEKResultsFile = new FileInfo(resultsFileNamePath);

            var pekTempFilePath = Path.Combine(mPEKResultsFile.Directory.FullName,
                Path.GetFileNameWithoutExtension(mPEKResultsFile.Name) + PEK_TEMP_FILE);

            var tempResultsFile = new FileInfo(pekTempFilePath);

            if (tempResultsFile.Exists)
            {
                // Open the .pek.tmp file and determine the last scan number that has "Number of isotopic distributions detected"
                scanToResumeAfter = GetLastScanInPEKFile(pekTempFilePath);

                if (scanToResumeAfter > 0)
                {
                    useAllScans = false;
                    tempResultsFile.MoveTo(resultsFileNamePath);
                }
            }

            // Syntax for calling ICR-2LS via the command line:
            // ICR-2LS.exe /I:InputFilePath /P:ParameterFilePath /O:OutputFilePath /M:[PEK|TIC] /T:[1|2] /F:FirstScan /L:LastScan /NoMS2

            // /M:PEK means to make a PEK file while /M:TIC means to generate the .TIC file
            // /T:0 is likely auto-determine based on input file name
            // /T:1 means the input file is a Thermo .Raw file, and /I specifies a file path
            // /T:2 means the input files are s-files in s-folders (ICR-2LS file format), and thus /I specifies a folder path

            // /F and /L are optional.  They can be used to limit the range of scan numbers to process
            // /NoMS2 is optional.  When provided, /MS2 spectra will be skipped

            // See AnalysisToolRunnerICR for a description of the expected folder layout when processing S-folders

            string arguments;

            switch (icr2lsMode)
            {
                case ICR2LSProcessingModeConstants.SerFolderPEK:
                case ICR2LSProcessingModeConstants.SerFolderTIC:
                    arguments = " /I:" + PossiblyQuotePath(instrumentFilePath) + "\\acqus /P:" + PossiblyQuotePath(paramFilePath) + " /O:" +
                                   PossiblyQuotePath(resultsFileNamePath);

                    break;
                case ICR2LSProcessingModeConstants.SerFilePEK:
                case ICR2LSProcessingModeConstants.SerFileTIC:
                    // Need to find the location of the apexAcquisition.method file
                    var apexAcqFilePath = FileSearch.FindFileInDirectoryTree(Path.GetDirectoryName(instrumentFilePath), APEX_ACQUISITION_METHOD_FILE);

                    if (string.IsNullOrEmpty(apexAcqFilePath))
                    {
                        LogError("Could not find the " + APEX_ACQUISITION_METHOD_FILE + " file in folder " + instrumentFilePath);
                        return false;
                    }

                    arguments = " /I:" + PossiblyQuotePath(apexAcqFilePath) + " /P:" + PossiblyQuotePath(paramFilePath) + " /O:" +
                                   PossiblyQuotePath(resultsFileNamePath);

                    break;
                default:
                    arguments = " /I:" + PossiblyQuotePath(instrumentFilePath) + " /P:" + PossiblyQuotePath(paramFilePath) + " /O:" +
                                   PossiblyQuotePath(resultsFileNamePath);
                    break;
            }

            switch (icr2lsMode)
            {
                case ICR2LSProcessingModeConstants.LTQFTPEK:
                    arguments += " /M:PEK /T:1";
                    break;
                case ICR2LSProcessingModeConstants.LTQFTTIC:
                    arguments += " /M:TIC /T:1";
                    break;
                case ICR2LSProcessingModeConstants.SFoldersPEK:
                    arguments += " /M:PEK /T:2";
                    break;
                case ICR2LSProcessingModeConstants.SFoldersTIC:
                    arguments += " /M:TIC /T:2";
                    break;
                case ICR2LSProcessingModeConstants.SerFolderPEK:
                case ICR2LSProcessingModeConstants.SerFilePEK:
                    arguments += " /M:PEK /T:0";
                    break;
                case ICR2LSProcessingModeConstants.SerFolderTIC:
                case ICR2LSProcessingModeConstants.SerFileTIC:
                    arguments += " /M:TIC /T:0";
                    break;
                default:
                    // Unknown mode
                    LogError("Unknown ICR2LS processing Mode: " + icr2lsMode);
                    return false;
            }

            if (useAllScans)
            {
                // Obsolete
                // mMinScanOffset = 0
            }
            else
            {
                // Obsolete
                // mMinScanOffset = MinScan

                if (scanToResumeAfter > 0 && minScan < scanToResumeAfter)
                    minScan = scanToResumeAfter + 1;

                arguments += " /F:" + minScan + " /L:" + maxScan;
            }

            if (skipMS2)
            {
                arguments += " /NoMS2";
            }

            // Possibly enable preview mode (skips the actual deisotoping)
            if (false && Environment.MachineName.IndexOf("WE31383", StringComparison.InvariantCultureIgnoreCase) >= 0)
            {
                arguments += " /preview";
            }

            // Initialize the program runner
            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.MonitorInterval = MONITOR_INTERVAL_SECONDS * 1000;

            // Set up and execute a program runner to run ICR2LS.exe
            if (mDebugLevel >= 1)
            {
                LogDebug(exeFilePath + arguments);
            }

            if (arguments.Length > 250)
            {
                // VB6 programs cannot parse command lines over 255 characters in length
                // Save the arguments to a text file and then call ICR2LS using the /R switch

                var commandLineFilePath = Path.GetTempFileName();
                using (var writer = new StreamWriter(new FileStream(commandLineFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine(arguments);
                }

                arguments = "/R:" + PossiblyQuotePath(commandLineFilePath);

                if (mDebugLevel >= 1)
                {
                    LogDebug("Command line is over 250 characters long; will use /R instead");
                    LogDebug("  " + exeFilePath + " " + arguments);
                }
            }

            // Start ICR-2LS.  Note that .RunProgram will not return until after the ICR2LS.exe closes
            // However, it will raise a Loop Waiting event every MONITOR_INTERVAL_SECONDS seconds (see CmdRunner_LoopWaiting)
            var success = mCmdRunner.RunProgram(exeFilePath, arguments, "ICR2LS.exe", true);

            // Pause for another 500 msec to make sure ICR-2LS closes
            Global.IdleLoop(0.5);

            // Make sure the status file is parsed one final time
            ParseICR2LSStatusFile(mStatusFilePath, true);

            if (mStatusFileWatcher != null)
            {
                mStatusFileWatcher.EnableRaisingEvents = false;
                mStatusFileWatcher = null;
            }

            // Stop the job timer
            mStopTime = DateTime.UtcNow;

            if (!success)
            {
                // ProgRunner returned false, check the Exit Code
                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("ICR2LS.exe returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to ICR2LS.exe failed (but exit code is 0)");
                }

                LogWarning(
                    "Most recent ICR-2LS State: " + mICR2LSStatus.ProcessingState + " with " + mICR2LSStatus.ScansProcessed + " scans processed (" +
                    mICR2LSStatus.PercentComplete.ToString("0.0") + "% done); Status = " + mICR2LSStatus.ProcessingStatus);

                LogError("Error running ICR-2LS.exe: " + mJob);
                return false;
            }

            // Verify ICR-2LS exited due to job completion

            if (!string.Equals(mICR2LSStatus.ProcessingState, ICR2LS_STATE_FINISHED, StringComparison.InvariantCultureIgnoreCase))
            {
                BaseLogger.LogLevels eLogLevel;

                if (string.Equals(mICR2LSStatus.ProcessingState, ICR2LS_STATE_ERROR, StringComparison.InvariantCultureIgnoreCase) ||
                    string.Equals(mICR2LSStatus.ProcessingState, ICR2LS_STATE_KILLED, StringComparison.InvariantCultureIgnoreCase) || mProgress < 100)
                {
                    eLogLevel = BaseLogger.LogLevels.ERROR;
                }
                else
                {
                    eLogLevel = BaseLogger.LogLevels.WARN;
                }

                var msg = string.Format(
                    "ICR-2LS processing state not Finished: {0}; Processed {1} scans ({2:F1}% complete); Status = {3}",
                    mICR2LSStatus.ProcessingState,
                    mICR2LSStatus.ScansProcessed,
                    mICR2LSStatus.PercentComplete,
                    mICR2LSStatus.ProcessingStatus);

                if (eLogLevel == BaseLogger.LogLevels.WARN)
                    LogWarning(msg);
                else
                    LogError(msg);

                if (mProgress >= 100)
                {
                    LogWarning("Progress reported by ICR-2LS is 100%, so will assume the job is complete");
                    return true;
                }

                return false;
            }

            if (mDebugLevel > 0)
            {
                LogDebug("Processing state Finished; Processed " + mICR2LSStatus.ScansProcessed + " scans");
            }
            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        protected bool StoreToolVersionInfo()
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var progLoc = mMgrParams.GetParam("ICR2LSProgLoc");

            if (string.IsNullOrEmpty(progLoc))
            {
                mMessage = "Manager parameter ICR2LSProgLoc is not defined";
                LogError("Error in SetStepTaskToolVersion: " + mMessage);
                return false;
            }

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(progLoc)
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        private bool ValidateICR2LSStatus(string processingState)
        {
            var valid = processingState.ToLower() switch
            {
                ICR2LS_STATE_UNKNOWN => true,
                ICR2LS_STATE_IDLE => true,
                ICR2LS_STATE_PROCESSING => true,
                ICR2LS_STATE_KILLED => true,
                ICR2LS_STATE_ERROR => true,
                ICR2LS_STATE_FINISHED => true,
                ICR2LS_STATE_GENERATING => true,
                ICR2LS_STATE_TICGENERATION => true,
                ICR2LS_STATE_LCQTICGENERATION => true,
                ICR2LS_STATE_QTOFPEKGENERATION => true,
                ICR2LS_STATE_MMTOFPEKGENERATION => true,
                ICR2LS_STATE_LTQFTPEKGENERATION => true,
                _ => false
            };

            return valid;
        }

        protected bool VerifyPEKFileExists(string folderPath, string datasetName)
        {
            var matchFound = false;

            try
            {
                var folder = new DirectoryInfo(folderPath);

                if (folder.Exists)
                {
                    if (folder.GetFiles(datasetName + "*.pek").Length > 0)
                    {
                        matchFound = true;
                    }
                }
                else
                {
                    LogError("Error in VerifyPEKFileExists; folder not found: " + folderPath);
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in VerifyPEKFileExists: " + ex.Message);
            }

            return matchFound;
        }

        private void CmdRunner_LoopWaiting()
        {
            const int NORMAL_LOG_INTERVAL_MINUTES = 30;
            const int DEBUG_LOG_INTERVAL_MINUTES = 5;

            const int CHECKPOINT_SAVE_INTERVAL_MINUTES = 1;

            var logStatus = false;

            var minutesElapsed = DateTime.UtcNow.Subtract(mLastStatusLogTime).TotalMinutes;

            if (mDebugLevel > 0)
            {
                if (minutesElapsed >= DEBUG_LOG_INTERVAL_MINUTES && mDebugLevel >= 2)
                {
                    logStatus = true;
                }
                else if (minutesElapsed >= NORMAL_LOG_INTERVAL_MINUTES)
                {
                    logStatus = true;
                }

                if (logStatus)
                {
                    mLastStatusLogTime = DateTime.UtcNow;

                    LogDebug(
                        string.Format(
                            "AnalysisToolRunnerICRBase.CmdRunner_LoopWaiting(); " +
                            "Processing Time = {0:0.0} minutes; Progress = {1:0.00}; Scans Processed = {2}",
                            DateTime.UtcNow.Subtract(mStartTime).TotalMinutes, mProgress, mICR2LSStatus.ScansProcessed));
                }
            }

            if (DateTime.UtcNow.Subtract(mLastCheckpointTime).TotalMinutes >= CHECKPOINT_SAVE_INTERVAL_MINUTES)
            {
                mLastCheckpointTime = DateTime.UtcNow;
                CopyCheckpointFile();
            }
        }

        private void StatusFileWatcher_Changed(object sender, FileSystemEventArgs e)
        {
            ParseICR2LSStatusFile(mStatusFilePath, false);
        }

        private void PEKtoCSVConverter_ErrorEvent(object sender, PEKtoCSVConverter.PEKtoCSVConverter.MessageEventArgs e)
        {
            LogError("PEKtoCSVConverter error: " + e.Message);
        }

        private void PEKtoCSVConverter_MessageEvent(object sender, PEKtoCSVConverter.PEKtoCSVConverter.MessageEventArgs e)
        {
            if (e.Message.Contains("% complete; scan"))
            {
                // Message is of the form: 35% complete; scan 2602
                // Only log this message every 15 seconds
                if (DateTime.UtcNow.Subtract(mLastPekToCsvPercentCompleteTime).TotalSeconds < 15)
                {
                    return;
                }
                mLastPekToCsvPercentCompleteTime = DateTime.UtcNow;
            }

            LogMessage(e.Message);
        }
    }
}
