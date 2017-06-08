using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerICR2LSPlugIn
{
    public abstract class clsAnalysisToolRunnerICRBase : clsAnalysisToolRunnerBase
    {
        public const string ICR2LS_STATE_UNKNOWN = "unknown";                        // "Unknown"
        public const string ICR2LS_STATE_IDLE = "idle";                              // "Idle"
        public const string ICR2LS_STATE_PROCESSING = "processing";                  // "Processing"
        public const string ICR2LS_STATE_KILLED = "killed";                          // "Killed"
        public const string ICR2LS_STATE_ERROR = "error";                            // "Error"
        public const string ICR2LS_STATE_FINISHED = "finished";                      // "Finished"
        public const string ICR2LS_STATE_GENERATING = "generating";                  // "Generating"
        public const string ICR2LS_STATE_TICGENERATION = "ticgeneration";            // "TICGeneration"
        public const string ICR2LS_STATE_LCQTICGENERATION = "lcqticgeneration";      // "LCQTICGeneration"
        public const string ICR2LS_STATE_QTOFPEKGENERATION = "qtofpekgeneration";    // "QTOFPEKGeneration"
        public const string ICR2LS_STATE_MMTOFPEKGENERATION = "mmtofpekgeneration";  // "MMTOFPEKGeneration"
        public const string ICR2LS_STATE_LTQFTPEKGENERATION = "ltqftpekgeneration";  // "LTQFTPEKGeneration"

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

        private struct udtICR2LSStatusType
        {
            public DateTime StatusDate;
            public int ScansProcessed;
            public float PercentComplete;
            public string ProcessingState;              // Typical values: Processing, Finished, etc.
            public string ProcessingStatus;             // Typical values: LTQFTPEKGENERATION, GENERATING
            public string ErrorMessage;

            public void Initialize()
            {
                StatusDate = DateTime.Now;
                ScansProcessed = 0;
                PercentComplete = 0;
                ProcessingState = ICR2LS_STATE_UNKNOWN;
                ProcessingStatus = string.Empty;
                ErrorMessage = string.Empty;
            }
        }

        private string mStatusFilePath = string.Empty;

        // Obsolete
        // Private mMinScanOffset As Integer = 0

        private DateTime mLastErrorPostingTime;
        private DateTime mLastMissingStatusFiletime;
        private DateTime mLastInvalidStatusFiletime;

        private DateTime mLastStatusParseTime = DateTime.UtcNow;
        private DateTime mLastStatusLogTime = DateTime.UtcNow;

        private FileInfo mPEKResultsFile;
        private DateTime mLastCheckpointTime = DateTime.UtcNow;

        private udtICR2LSStatusType mICR2LSStatus;

        private clsRunDosProgram mCmdRunner;
        private FileSystemWatcher mStatusFileWatcher;
        private PEKtoCSVConverter.PEKtoCSVConverter mPEKtoCSVConverter;

        private DateTime mLastPekToCsvPercentCompleteTime;

        protected clsAnalysisToolRunnerICRBase()
        {
            ResetStatusLogTimes();

            mICR2LSStatus.Initialize();
        }

        public override CloseOutType RunTool()
        {
            // Get the settings file info via the base class
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                return CloseOutType.CLOSEOUT_FAILED;

            //Start the job timer
            m_StartTime = DateTime.UtcNow;

            ResetStatusLogTimes();
            mICR2LSStatus.Initialize();

            // Remainder of tasks are in subclass (which should call this using MyBase.Runtool)
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool ConvertPekToCsv(string pekFilePath)
        {
            try
            {
                var scansFilePath = Path.Combine(m_WorkDir, m_Dataset + "_scans.csv");
                var isosFilePath = Path.Combine(m_WorkDir, m_Dataset + "_isos.csv");
                var rawFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_RAW_EXTENSION);

                if (!File.Exists(rawFilePath))
                {
                    rawFilePath = string.Empty;
                }

                mPEKtoCSVConverter = new PEKtoCSVConverter.PEKtoCSVConverter(pekFilePath, scansFilePath, isosFilePath, rawFilePath);
                mPEKtoCSVConverter.ErrorEvent += mPEKtoCSVConverter_ErrorEvent;
                mPEKtoCSVConverter.MessageEvent += mPEKtoCSVConverter_MessageEvent;

                LogMessage("Creating _isos.csv and _scans.csv files using the PEK file");
                mLastPekToCsvPercentCompleteTime = DateTime.UtcNow;

                var success = mPEKtoCSVConverter.Convert();

                mPEKtoCSVConverter = null;
                PRISM.clsProgRunner.GarbageCollectNow();

                return success;
            }
            catch (Exception ex)
            {
                m_message = "Error converting the PEK file to DeconTools-compatible _isos.csv";
                LogError(m_message + ": " + ex.Message);
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

                var transferFolderPath = m_jobParams.GetParam("JobParameters", "transferFolderPath");
                if (string.IsNullOrEmpty(transferFolderPath))
                    return;

                transferFolderPath = Path.Combine(transferFolderPath, m_jobParams.GetParam("JobParameters", "DatasetFolderName"));
                transferFolderPath = Path.Combine(transferFolderPath, m_jobParams.GetParam("StepParameters", "OutputFolderName"));

                var diTransferFolder = new DirectoryInfo(transferFolderPath);
                if (!diTransferFolder.Exists)
                {
                    diTransferFolder.Create();
                }

                var fiTargetFileFinal = new FileInfo(Path.Combine(diTransferFolder.FullName, Path.GetFileNameWithoutExtension(mPEKResultsFile.Name) + PEK_TEMP_FILE));
                var fiTargetFileTemp = new FileInfo(fiTargetFileFinal.FullName + ".new");

                mPEKResultsFile.CopyTo(fiTargetFileTemp.FullName, true);

                Thread.Sleep(500);
                fiTargetFileTemp.Refresh();

                if (fiTargetFileFinal.Exists)
                    fiTargetFileFinal.Delete();

                fiTargetFileTemp.MoveTo(fiTargetFileFinal.FullName);

                m_jobParams.AddServerFileToDelete(fiTargetFileFinal.FullName);
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

            var lstClosingMessages = new List<string>
            {
                "Number of isotopic distributions detected",
                "Processing stop time",
                "Number of peaks in spectrum"
            };

            try
            {
                using (var srPekFile = new StreamReader(new FileStream(pekTempFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srPekFile.EndOfStream)
                    {
                        var dataLine = srPekFile.ReadLine();
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

                        foreach (var closingMessage in lstClosingMessages)
                        {
                            if (dataLine.StartsWith(closingMessage))
                            {
                                lastValidScan = currentScan;
                            }
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

        // Reads the ICR2LS Status file and updates mICR2LSStatus
        private bool ParseICR2LSStatusFile(string strStatusFilePath, bool blnForceParse)
        {
            const int MINIMUM_PARSING_INTERVAL_SECONDS = 4;

            var strProcessingState = mICR2LSStatus.ProcessingState;
            var strProcessingStatus = string.Empty;
            var intScansProcessed = 0;

            var strStatusDate = string.Empty;
            var strStatusTime = string.Empty;

            try
            {

                if (string.IsNullOrEmpty(strStatusFilePath))
                {
                    return false;
                }

                if (!blnForceParse && DateTime.UtcNow.Subtract(mLastStatusParseTime).TotalSeconds < MINIMUM_PARSING_INTERVAL_SECONDS)
                {
                    // Not enough time has elapsed, exit the procedure (returning True)
                    return true;
                }

                mLastStatusParseTime = DateTime.UtcNow;

                if (File.Exists(strStatusFilePath))
                {
                    // Read the file
                    using (var srInFile = new StreamReader(new FileStream(strStatusFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        while (!srInFile.EndOfStream)
                        {
                            var strLineIn = srInFile.ReadLine();

                            if (string.IsNullOrWhiteSpace(strLineIn))
                                continue;

                            var charIndex = strLineIn.IndexOf('=');
                            if (charIndex <= 0)
                                continue;

                            var strKey = strLineIn.Substring(0, charIndex).Trim();
                            var strValue = strLineIn.Substring(charIndex + 1).Trim();

                            switch (strKey.ToLower())
                            {
                                case "date":
                                    strStatusDate = string.Copy(strValue);
                                    break;
                                case "time":
                                    strStatusTime = string.Copy(strValue);
                                    break;
                                case "scansprocessed":
                                    if (int.TryParse(strValue, out var intResult))
                                    {
                                        // Old: The ScansProcessed value reported by ICR-2LS is actually the scan number of the most recently processed scan
                                        // If we use /F to start with a scan other than 1, then this ScansProcessed value does not reflect reality
                                        // To correct for this, subtract out mMinScanOffset
                                        // intScansProcessed = intResult - mMinScanOffset

                                        // New: ScansProcessed is truly the number of scans processed
                                        intScansProcessed = intResult;
                                        if (intScansProcessed < 0)
                                        {
                                            intScansProcessed = intResult;
                                        }
                                    }

                                    break;
                                case "percentcomplete":
                                    if (float.TryParse(strValue, out var sngResult))
                                    {
                                        mICR2LSStatus.PercentComplete = sngResult;
                                    }

                                    break;
                                case "state":
                                    // Example values: Processing, Finished
                                    strProcessingState = string.Copy(strValue);

                                    break;
                                case "status":
                                    // Example value: LTQFTPEKGENERATION
                                    strProcessingStatus = string.Copy(strValue);

                                    break;
                                case "errormessage":
                                    mICR2LSStatus.ErrorMessage = string.Copy(strValue);

                                    break;
                                default:
                                    break;
                                // Ignore the line
                            }
                        }

                    }

                    if (strStatusDate.Length > 0 && strStatusTime.Length > 0)
                    {
                        strStatusDate += " " + strStatusTime;
                        if (!DateTime.TryParse(strStatusDate, out mICR2LSStatus.StatusDate))
                        {
                            mICR2LSStatus.StatusDate = DateTime.Now;
                        }
                    }

                    if (intScansProcessed > mICR2LSStatus.ScansProcessed)
                    {
                        // Only update .ScansProcessed if the new value is larger than the previous one
                        // This is necessary since ICR-2LS will set ScansProcessed to 0 when the state is Finished
                        mICR2LSStatus.ScansProcessed = intScansProcessed;
                    }

                    if (!string.IsNullOrEmpty(strProcessingState))
                    {
                        mICR2LSStatus.ProcessingState = strProcessingState;

                        if (!ValidateICR2LSStatus(strProcessingState))
                        {
                            if (DateTime.UtcNow.Subtract(mLastInvalidStatusFiletime).TotalMinutes >= 15)
                            {
                                mLastInvalidStatusFiletime = DateTime.UtcNow;
                                LogWarning("Invalid processing state reported by ICR2LS: " + strProcessingState);
                            }
                        }
                    }

                    if (strProcessingStatus.Length > 0)
                    {
                        mICR2LSStatus.ProcessingStatus = strProcessingStatus;
                    }

                    m_progress = mICR2LSStatus.PercentComplete;

                    // Update the local status file (and post the status to the message queue)
                    UpdateStatusRunning(m_progress, mICR2LSStatus.ScansProcessed);

                    return true;
                }

                // Status.log file not found; if the job just started, this will be the case
                // For this reason, ResetStatusLogTimes will set mLastMissingStatusFiletime to the time the job starts, meaning
                //  we won't log an error about a missing Status.log file until 60 minutes into a job
                if (DateTime.UtcNow.Subtract(mLastMissingStatusFiletime).TotalMinutes >= 60)
                {
                    mLastMissingStatusFiletime = DateTime.UtcNow;
                    LogWarning("ICR2LS Status.Log file not found: " + strStatusFilePath);
                }

                return true;
            }
            catch (Exception ex)
            {
                // Limit logging of errors to once every 60 minutes

                if (DateTime.UtcNow.Subtract(mLastErrorPostingTime).TotalMinutes >= 60)
                {
                    mLastErrorPostingTime = DateTime.UtcNow;
                    LogWarning("Error reading the ICR2LS Status.Log file (" + strStatusFilePath + "): " + ex.Message);
                }

                return false;
            }

        }

        private void InitializeStatusLogFileWatcher(string strWorkDir, string strFilenameToWatch)
        {
            mStatusFileWatcher = new FileSystemWatcher();
            mStatusFileWatcher.Changed += mStatusFileWatcher_Changed;
            mStatusFileWatcher.BeginInit();
            mStatusFileWatcher.Path = strWorkDir;
            mStatusFileWatcher.IncludeSubdirectories = false;
            mStatusFileWatcher.Filter = Path.GetFileName(strFilenameToWatch);
            mStatusFileWatcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size;
            mStatusFileWatcher.EndInit();
            mStatusFileWatcher.EnableRaisingEvents = true;
        }

        protected virtual CloseOutType PerfPostAnalysisTasks(bool blnCopyResultsToServer)
        {
            //Stop the job timer
            m_StopTime = DateTime.UtcNow;

            UpdateSummaryFile();

            // Use the PEK file to create DeconTools compatible _isos.csv and _scans.csv files
            // Create this CSV file even if ICR-2LS did not successfully finish
            var pekFilePath = Path.Combine(m_WorkDir, m_Dataset + ".pek");
            var pekConversionSuccess = ConvertPekToCsv(pekFilePath);

            // Get rid of raw data file
            var result = DeleteDataFile();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Error deleting raw files; the error will have already been logged
                // Since the results might still be good, we will not return an error at this point
            }

            m_jobParams.AddResultFileToSkip("Status.log");

            var copySuccess = CopyResultsToTransferDirectory();

            if (!copySuccess)
                return CloseOutType.CLOSEOUT_FAILED;

            if (pekConversionSuccess)
            {
                // We can now safely delete the .pek.tmp file from the server
                RemoveNonResultServerFiles();

                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            if (string.IsNullOrEmpty(m_message))
            {
                m_message = "Unknown error converting the .PEK file to a DeconTools-compatible _isos.csv file";
            }
            return CloseOutType.CLOSEOUT_FAILED;
        }

        private void ResetStatusLogTimes()
        {
            // Initialize the last error posting time to 2 hours before the present
            mLastErrorPostingTime = DateTime.UtcNow.Subtract(new TimeSpan(2, 0, 0));

            // Initialize the last MissingStatusFileTime to the time the job starts
            mLastMissingStatusFiletime = DateTime.UtcNow;

            mLastInvalidStatusFiletime = DateTime.UtcNow.Subtract(new TimeSpan(2, 0, 0));
        }

        /// <summary>
        /// Starts ICR-2LS by running the .Exe at the command line
        /// </summary>
        /// <param name="DSNamePath"></param>
        /// <param name="ParamFilePath"></param>
        /// <param name="ResultsFileNamePath"></param>
        /// <param name="eICR2LSMode"></param>
        /// <returns>True if successfully started; otherwise false</returns>
        /// <remarks></remarks>
        protected bool StartICR2LS(string DSNamePath, string ParamFilePath, string ResultsFileNamePath, ICR2LSProcessingModeConstants eICR2LSMode)
        {
            return StartICR2LS(DSNamePath, ParamFilePath, ResultsFileNamePath, eICR2LSMode, true, false, 0, 0);
        }

        /// <summary>
        /// Run ICR-2LS on the file (or 0.ser folder) specified by DSNamePath
        /// </summary>
        /// <param name="instrumentFilePath"></param>
        /// <param name="paramFilePath"></param>
        /// <param name="resultsFileNamePath"></param>
        /// <param name="eICR2LSMode"></param>
        /// <param name="useAllScans"></param>
        /// <param name="skipMS2"></param>
        /// <param name="minScan"></param>
        /// <param name="maxScan"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool StartICR2LS(string instrumentFilePath, string paramFilePath, string resultsFileNamePath,
            ICR2LSProcessingModeConstants eICR2LSMode, bool useAllScans, bool skipMS2, int minScan, int maxScan)
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

            var strExeFilePath = m_mgrParams.GetParam("ICR2LSprogloc");

            if (string.IsNullOrEmpty(strExeFilePath))
            {
                LogError("Job parameter ICR2LSprogloc is not defined; unable to run ICR-2LS");
                return false;
            }

            if (!File.Exists(strExeFilePath))
            {
                LogError("ICR-2LS path not found: " + strExeFilePath);
                return false;
            }

            // Look for an existing .pek.tmp file
            var scanToResumeAfter = 0;

            mPEKResultsFile = new FileInfo(resultsFileNamePath);

            if (mPEKResultsFile.Directory == null)
            {
                LogError("Unable to determine the parent directory of " + resultsFileNamePath);
                return false;
            }

            var pekTempFilePath = Path.Combine(mPEKResultsFile.Directory.FullName,
                Path.GetFileNameWithoutExtension(mPEKResultsFile.Name) + PEK_TEMP_FILE);

            var fiTempResultsFile = new FileInfo(pekTempFilePath);
            if (fiTempResultsFile.Exists)
            {
                // Open the .pek.tmp file and determine the last scan number that has "Number of isotopic distributions detected"
                scanToResumeAfter = GetLastScanInPEKFile(pekTempFilePath);

                if (scanToResumeAfter > 0)
                {
                    useAllScans = false;
                    Thread.Sleep(200);
                    fiTempResultsFile.MoveTo(resultsFileNamePath);
                }
            }

            // Syntax for calling ICR-2LS via the command line:
            // ICR-2LS.exe /I:InputFilePath /P:ParameterFilePath /O:OutputFilePath /M:[PEK|TIC] /T:[1|2] /F:FirstScan /L:LastScan /NoMS2
            //
            // /M:PEK means to make a PEK file while /M:TIC means to generate the .TIC file
            // /T:0 is likely auto-determine based on input file name
            // /T:1 means the input file is a Thermo .Raw file, and /I specifies a file path
            // /T:2 means the input files are s-files in s-folders (ICR-2LS file format), and thus /I specifies a folder path
            //
            // /F and /L are optional.  They can be used to limit the range of scan numbers to process
            //
            // /NoMS2 is optional.  When provided, /MS2 spectra will be skipped
            //
            // See clsAnalysisToolRunnerICR for a description of the expected folder layout when processing S-folders

            string strArguments;

            switch (eICR2LSMode)
            {
                case ICR2LSProcessingModeConstants.SerFolderPEK:
                case ICR2LSProcessingModeConstants.SerFolderTIC:
                    strArguments = " /I:" + PossiblyQuotePath(instrumentFilePath) + "\\acqus /P:" + PossiblyQuotePath(paramFilePath) + " /O:" +
                                   PossiblyQuotePath(resultsFileNamePath);

                    break;
                case ICR2LSProcessingModeConstants.SerFilePEK:
                case ICR2LSProcessingModeConstants.SerFileTIC:
                    // Need to find the location of the apexAcquisition.method file
                    var strApexAcqFilePath = clsFileSearch.FindFileInDirectoryTree(Path.GetDirectoryName(instrumentFilePath), APEX_ACQUISITION_METHOD_FILE);

                    if (string.IsNullOrEmpty(strApexAcqFilePath))
                    {
                        LogError("Could not find the " + APEX_ACQUISITION_METHOD_FILE + " file in folder " + instrumentFilePath);
                        return false;
                    }

                    strArguments = " /I:" + PossiblyQuotePath(strApexAcqFilePath) + " /P:" + PossiblyQuotePath(paramFilePath) + " /O:" +
                                   PossiblyQuotePath(resultsFileNamePath);

                    break;
                default:
                    strArguments = " /I:" + PossiblyQuotePath(instrumentFilePath) + " /P:" + PossiblyQuotePath(paramFilePath) + " /O:" +
                                   PossiblyQuotePath(resultsFileNamePath);
                    break;
            }

            switch (eICR2LSMode)
            {
                case ICR2LSProcessingModeConstants.LTQFTPEK:
                    strArguments += " /M:PEK /T:1";
                    break;
                case ICR2LSProcessingModeConstants.LTQFTTIC:
                    strArguments += " /M:TIC /T:1";
                    break;
                case ICR2LSProcessingModeConstants.SFoldersPEK:
                    strArguments += " /M:PEK /T:2";
                    break;
                case ICR2LSProcessingModeConstants.SFoldersTIC:
                    strArguments += " /M:TIC /T:2";
                    break;
                case ICR2LSProcessingModeConstants.SerFolderPEK:
                case ICR2LSProcessingModeConstants.SerFilePEK:
                    strArguments += " /M:PEK /T:0";
                    break;
                case ICR2LSProcessingModeConstants.SerFolderTIC:
                case ICR2LSProcessingModeConstants.SerFileTIC:
                    strArguments += " /M:TIC /T:0";
                    break;
                default:
                    // Unknown mode
                    LogError("Unknown ICR2LS processing Mode: " + eICR2LSMode);
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

                strArguments += " /F:" + minScan + " /L:" + maxScan;
            }

            if (skipMS2)
            {
                strArguments += " /NoMS2";
            }

            // Possibly enable preview mode (skips the actual deisotoping)
            if (false && Environment.MachineName.IndexOf("monroe", StringComparison.InvariantCultureIgnoreCase) >= 0)
            {
                strArguments += " /preview";
            }

            // Initialize the program runner
            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.MonitorInterval = MONITOR_INTERVAL_SECONDS * 1000;

            // Set up and execute a program runner to run ICR2LS.exe
            if (m_DebugLevel >= 1)
            {
                LogDebug(strExeFilePath + strArguments);
            }

            if (strArguments.Length > 250)
            {
                // VB6 programs cannot parse command lines over 255 characters in length
                // Save the arguments to a text file and then call ICR2LS using the /R switch

                var commandLineFilePath = Path.GetTempFileName();
                using (var swCommandLineFile = new StreamWriter(new FileStream(commandLineFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swCommandLineFile.WriteLine(strArguments);
                }

                strArguments = "/R:" + PossiblyQuotePath(commandLineFilePath);

                if (m_DebugLevel >= 1)
                {
                    LogDebug("Command line is over 250 characters long; will use /R instead");
                    LogDebug("  " + strExeFilePath + " " + strArguments);
                }
            }

            // Start ICR-2LS.  Note that .Runprogram will not return until after the ICR2LS.exe closes
            // However, it will raise a Loop Waiting event every MONITOR_INTERVAL_SECONDS seconds (see CmdRunner_LoopWaiting)
            var success = mCmdRunner.RunProgram(strExeFilePath, strArguments, "ICR2LS.exe", true);

            // Pause for another 500 msec to make sure ICR-2LS closes
            Thread.Sleep(500);

            // Make sure the status file is parsed one final time
            ParseICR2LSStatusFile(mStatusFilePath, true);

            if (mStatusFileWatcher != null)
            {
                mStatusFileWatcher.EnableRaisingEvents = false;
                mStatusFileWatcher = null;
            }

            //Stop the job timer
            m_StopTime = DateTime.UtcNow;

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

                LogError("Error running ICR-2LS.exe: " + m_JobNum);
                return false;
            }

            //Verify ICR-2LS exited due to job completion

            if (!string.Equals(mICR2LSStatus.ProcessingState, ICR2LS_STATE_FINISHED, StringComparison.InvariantCultureIgnoreCase))
            {
                clsLogTools.LogLevels eLogLevel;
                if (string.Equals(mICR2LSStatus.ProcessingState, ICR2LS_STATE_ERROR, StringComparison.InvariantCultureIgnoreCase) |
                    string.Equals(mICR2LSStatus.ProcessingState, ICR2LS_STATE_KILLED, StringComparison.InvariantCultureIgnoreCase) | m_progress < 100)
                {
                    eLogLevel = clsLogTools.LogLevels.ERROR;
                }
                else
                {
                    eLogLevel = clsLogTools.LogLevels.WARN;
                }

                var msg = "ICR-2LS processing state not Finished: " + mICR2LSStatus.ProcessingState + "; Processed " +
                          mICR2LSStatus.ScansProcessed + " scans (" + mICR2LSStatus.PercentComplete.ToString("0.0") + "% complete); " +
                          "Status = " + mICR2LSStatus.ProcessingStatus;

                if (eLogLevel == clsLogTools.LogLevels.WARN)
                    LogWarning(msg);
                else
                    LogError(msg);

                if (m_progress >= 100)
                {
                    LogWarning("Progress reported by ICR-2LS is 100%, so will assume the job is complete");
                    return true;
                }

                return false;
            }

            if (m_DebugLevel > 0)
            {
                LogDebug("Processing state Finished; Processed " + mICR2LSStatus.ScansProcessed + " scans");
            }
            return true;
            
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var progLoc = m_mgrParams.GetParam("ICR2LSprogloc");
            if (string.IsNullOrEmpty(progLoc))
            {
                m_message = "Manager parameter ICR2LSprogloc is not defined";
                LogError("Error in SetStepTaskToolVersion: " + m_message);
                return false;
            }

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo> {
                new FileInfo(progLoc)
            };

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        private bool ValidateICR2LSStatus(string strProcessingState)
        {
            bool blnValid;

            switch (strProcessingState.ToLower())
            {
                case ICR2LS_STATE_UNKNOWN:
                    blnValid = true;
                    break;
                case ICR2LS_STATE_IDLE:
                    blnValid = true;
                    break;
                case ICR2LS_STATE_PROCESSING:
                    blnValid = true;
                    break;
                case ICR2LS_STATE_KILLED:
                    blnValid = true;
                    break;
                case ICR2LS_STATE_ERROR:
                    blnValid = true;
                    break;
                case ICR2LS_STATE_FINISHED:
                    blnValid = true;
                    break;
                case ICR2LS_STATE_GENERATING:
                    blnValid = true;
                    break;
                case ICR2LS_STATE_TICGENERATION:
                    blnValid = true;
                    break;
                case ICR2LS_STATE_LCQTICGENERATION:
                    blnValid = true;
                    break;
                case ICR2LS_STATE_QTOFPEKGENERATION:
                    blnValid = true;
                    break;
                case ICR2LS_STATE_MMTOFPEKGENERATION:
                    blnValid = true;
                    break;
                case ICR2LS_STATE_LTQFTPEKGENERATION:
                    blnValid = true;
                    break;
                default:
                    blnValid = false;
                    break;
            }

            return blnValid;
        }

        protected bool VerifyPEKFileExists(string strFolderPath, string strDatasetName)
        {
            var blnMatchFound = false;

            try
            {
                var fiFolder = new DirectoryInfo(strFolderPath);
                if (fiFolder.Exists)
                {
                    if (fiFolder.GetFiles(strDatasetName + "*.pek").Length > 0)
                    {
                        blnMatchFound = true;
                    }
                }
                else
                {
                    LogError("Error in VerifyPEKFileExists; folder not found: " + strFolderPath);
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in VerifyPEKFileExists: " + ex.Message);
            }

            return blnMatchFound;
        }

        private void CmdRunner_LoopWaiting()
        {
            const int NORMAL_LOG_INTERVAL_MINUTES = 30;
            const int DEBUG_LOG_INTERVAL_MINUTES = 5;

            const int CHECKPOINT_SAVE_INTERVAL_MINUTES = 1;

            var blnLogStatus = false;

            var dblMinutesElapsed = DateTime.UtcNow.Subtract(mLastStatusLogTime).TotalMinutes;
            if (m_DebugLevel > 0)
            {
                if (dblMinutesElapsed >= DEBUG_LOG_INTERVAL_MINUTES && m_DebugLevel >= 2)
                {
                    blnLogStatus = true;
                }
                else if (dblMinutesElapsed >= NORMAL_LOG_INTERVAL_MINUTES)
                {
                    blnLogStatus = true;
                }

                if (blnLogStatus)
                {
                    mLastStatusLogTime = DateTime.UtcNow;

                    LogDebug(
                        "clsAnalysisToolRunnerICRBase.CmdRunner_LoopWaiting(); " + "Processing Time = " +
                        DateTime.UtcNow.Subtract(m_StartTime).TotalMinutes.ToString("0.0") + " minutes; " + "Progress = " +
                        m_progress.ToString("0.00") + "; " + "Scans Processed = " + mICR2LSStatus.ScansProcessed);
                }
            }

            if (DateTime.UtcNow.Subtract(mLastCheckpointTime).TotalMinutes >= CHECKPOINT_SAVE_INTERVAL_MINUTES)
            {
                mLastCheckpointTime = DateTime.UtcNow;
                CopyCheckpointFile();
            }
        }

        private void mStatusFileWatcher_Changed(object sender, FileSystemEventArgs e)
        {
            ParseICR2LSStatusFile(mStatusFilePath, false);
        }

        private void mPEKtoCSVConverter_ErrorEvent(object sender, PEKtoCSVConverter.PEKtoCSVConverter.MessageEventArgs e)
        {
            LogError("PEKtoCSVConverter error: " + e.Message);
        }

        private void mPEKtoCSVConverter_MessageEvent(object sender, PEKtoCSVConverter.PEKtoCSVConverter.MessageEventArgs e)
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
