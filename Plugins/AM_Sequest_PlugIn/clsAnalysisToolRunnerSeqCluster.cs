//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Timers;
using AnalysisManagerBase;
using PRISM;
using PRISM.Logging;

namespace AnalysisManagerSequestPlugin
{
    /// <summary>
    /// Overrides Sequest tool runner to provide cluster-specific methods
    /// </summary>
    /// <remarks></remarks>
    public class clsAnalysisToolRunnerSeqCluster : clsAnalysisToolRunnerSeqBase
    {
        #region "Constants"

        private const int TEMP_FILE_COPY_INTERVAL_SECONDS = 300;
        private const int OUT_FILE_APPEND_INTERVAL_SECONDS = 30;
        private const int OUT_FILE_APPEND_HOLDOFF_SECONDS = 30;
        private const int STALE_NODE_THRESHOLD_MINUTES = 5;
        private const int MAX_NODE_RESPAWN_ATTEMPTS = 6;

        #endregion

        #region "Structures"

        private struct udtSequestNodeProcessingStats
        {
            public int NumNodeMachines;
            public int NumSlaveProcesses;
            public double TotalSearchTimeSeconds;
            public int SearchedFileCount;
            public float AvgSearchTime;

            public void Clear()
            {
                NumNodeMachines = 0;
                NumSlaveProcesses = 0;
                TotalSearchTimeSeconds = 0;
                SearchedFileCount = 0;
                AvgSearchTime = 0;
            }
        }

        #endregion

        #region "Module Variables"

        private FileSystemWatcher mOutFileWatcher;
        private System.Timers.Timer mOutFileAppenderTimer;

        /// <summary>
        /// This tracks the names of out files that have been created
        /// </summary>
        /// <remarks>
        /// Every OUT_FILE_APPEND_INTERVAL_SECONDS, this plugin looks for candidates older than OUT_FILE_APPEND_HOLDOFF_SECONDS
        /// For each, it appends the data to the _out.txt.tmp file, deletes the corresponding DTA file, and removes it from mOutFileCandidates
        /// </remarks>
        private readonly Queue<KeyValuePair<string, DateTime>> mOutFileCandidates = new Queue<KeyValuePair<string, DateTime>>();

        private readonly Dictionary<string, DateTime> mOutFileCandidateInfo = new Dictionary<string, DateTime>();
        private DateTime mLastOutFileStoreTime;
        private bool mSequestAppearsStalled;
        private bool mAbortSinceSequestIsStalled;

        private bool mSequestVersionInfoStored;

        private bool mTempJobParamsCopied;
        private DateTime mLastTempFileCopyTime;
        private string mTransferFolderPath;

        private DateTime mLastOutFileCountTime = DateTime.UtcNow;
        private DateTime mLastActiveNodeQueryTime = DateTime.UtcNow;

        private DateTime mLastActiveNodeLogTime;

        private bool mResetPVM;
        private int mNodeCountSpawnErrorOccurences;
        private int mNodeCountActiveErrorOccurences;
        private DateTime mLastSequestStartTime;

        private clsRunDosProgram mCmdRunner;
        private clsRunDosProgram m_UtilityRunner;

        private string mUtilityRunnerTaskName = string.Empty;

        private string m_ErrMsg = "";

        // This dictionary tracks the most recent time each node was observed via PVM command "ps -a"
        private readonly Dictionary<string, DateTime> mSequestNodes = new Dictionary<string, DateTime>();

        private bool mSequestLogNodesFound;
        private int mSequestNodesSpawned;
        private bool mIgnoreNodeCountActiveErrors;

        private udtSequestNodeProcessingStats mSequestNodeProcessingStats;

        private DateTime mSequestSearchStartTime;
        private DateTime mSequestSearchEndTime;

        private readonly Regex m_ActiveNodeRegEx = new Regex(@"\s+(?<node>[a-z0-9-.]+\s+[a-z0-9]+)\s+.+sequest.+slave.*",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

        #endregion

        #region "Methods"

        /// <summary>
        /// Modifies MakeOUTFiles to remove multiple processes used on non-clustered machines
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        protected override CloseOutType MakeOUTFiles()
        {
            // Creates Sequest .out files from DTA files

            int intDTACountRemaining;
            var blnProcessingError = false;

            mOutFileCandidates.Clear();
            mOutFileCandidateInfo.Clear();
            mOutFileNamesAppended.Clear();
            mOutFileHandlerInUse = 0;

            mSequestNodeProcessingStats.Clear();

            mSequestVersionInfoStored = false;
            mTempJobParamsCopied = false;

            mLastTempFileCopyTime = DateTime.UtcNow;
            mLastActiveNodeLogTime = DateTime.UtcNow;
            mLastOutFileStoreTime = DateTime.UtcNow;
            mSequestAppearsStalled = false;
            mAbortSinceSequestIsStalled = false;

            mTransferFolderPath = m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH);
            mTransferFolderPath = Path.Combine(mTransferFolderPath, m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME));
            mTransferFolderPath = Path.Combine(mTransferFolderPath, m_jobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME));

            // Initialize the out file watcher
            mOutFileWatcher = new FileSystemWatcher();
            mOutFileWatcher.Created += mOutFileWatcher_Created;
            mOutFileWatcher.Changed += mOutFileWatcher_Changed;
            mOutFileWatcher.BeginInit();
            mOutFileWatcher.Path = m_WorkDir;
            mOutFileWatcher.IncludeSubdirectories = false;
            mOutFileWatcher.Filter = "*.out";
            mOutFileWatcher.NotifyFilter = NotifyFilters.FileName;
            mOutFileWatcher.EndInit();
            mOutFileWatcher.EnableRaisingEvents = true;

            var ProgLoc = m_mgrParams.GetParam("seqprogloc");
            if (!File.Exists(ProgLoc))
            {
                m_message = "Sequest .Exe not found";
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, m_message + " at " + ProgLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Initialize the Out File Appender timer
            mOutFileAppenderTimer = new System.Timers.Timer(OUT_FILE_APPEND_INTERVAL_SECONDS * 1000);
            mOutFileAppenderTimer.Elapsed += mOutFileAppenderTime_Elapsed;
            mOutFileAppenderTimer.Start();

            var diWorkDir = new DirectoryInfo(m_WorkDir);

            if (diWorkDir.GetFiles("sequest*.log*").Length > 0)
            {
                // Parse any sequest.log files present in the work directory to determine the number of spectra already processed
                UpdateSequestNodeProcessingStats(true);
            }

            mNodeCountSpawnErrorOccurences = 0;
            mNodeCountActiveErrorOccurences = 0;
            mLastSequestStartTime = DateTime.UtcNow;

            mIgnoreNodeCountActiveErrors = m_jobParams.GetJobParameter("IgnoreSequestNodeCountActiveErrors", false);

            do
            {
                // Reset several pieces of information on each iteration of this Do Loop
                mSequestNodes.Clear();
                mSequestLogNodesFound = false;
                mSequestNodesSpawned = 0;
                mResetPVM = false;
                mSequestSearchStartTime = DateTime.UtcNow;
                mSequestSearchEndTime = DateTime.UtcNow;

                mLastOutFileCountTime = DateTime.UtcNow;
                mLastActiveNodeQueryTime = DateTime.UtcNow;

                mCmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                // Define the arguments to pass to the Sequest .Exe
                var cmdStr = " -P" + m_jobParams.GetParam("parmFileName") + " *.dta";
                if (m_DebugLevel >= 1)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "  " + ProgLoc + " " + cmdStr);
                }

                // Run Sequest to generate OUT files
                mLastSequestStartTime = DateTime.UtcNow;
                var blnSuccess = mCmdRunner.RunProgram(ProgLoc, cmdStr, "Seq", true);

                mSequestSearchEndTime = DateTime.UtcNow;

                if (blnSuccess && !mResetPVM && !mAbortSinceSequestIsStalled)
                {
                    intDTACountRemaining = 0;
                }
                else
                {
                    if (!mResetPVM && !mAbortSinceSequestIsStalled)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                            " ... CmdRunner returned false; ExitCode = " + mCmdRunner.ExitCode);
                    }

                    // Check whether any .DTA files remain for this dataset
                    intDTACountRemaining = GetDTAFileCountRemaining();

                    if (intDTACountRemaining > 0)
                    {
                        blnSuccess = false;
                        if (mNodeCountSpawnErrorOccurences < MAX_NODE_RESPAWN_ATTEMPTS && mNodeCountActiveErrorOccurences < MAX_NODE_RESPAWN_ATTEMPTS)
                        {
                            var intMaxPVMResetAttempts = 4;

                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, "Resetting PVM in MakeOUTFiles");
                            blnSuccess = ResetPVMWithRetry(intMaxPVMResetAttempts);
                        }

                        if (!blnSuccess)
                        {
                            // Log message "Error resetting PVM; disabling manager locally"
                            m_message = PVM_RESET_ERROR_MESSAGE;
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                                m_message + "; disabling manager locally");
                            m_NeedToAbortProcessing = true;
                            blnProcessingError = true;
                            break;
                        }
                    }
                    else
                    {
                        // No .DTAs remain; if we have as many .out files as the original source .dta files, treat this as success, otherwise as a failure
                        var intOutFileCount = GetOUTFileCountRemaining() + mTotalOutFileCount;

                        if (intOutFileCount == m_DtaCount)
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                                " ... The number of OUT files (" + intOutFileCount + ") is equivalent to the original DTA count (" + m_DtaCount +
                                "); we'll consider this a successful job despite the Sequest CmdRunner error");
                        }
                        else if (intOutFileCount > m_DtaCount)
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                                " ... The number of OUT files (" + intOutFileCount + ") is greater than the original DTA count (" + m_DtaCount +
                                "); we'll consider this a successful job despite the Sequest CmdRunner error");
                        }
                        else if (intOutFileCount >= (int)(m_DtaCount * 0.999))
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                                " ... The number of OUT files (" + intOutFileCount + ") is within 0.1% of the original DTA count (" + m_DtaCount +
                                "); we'll consider this a successful job despite the Sequest CmdRunner error");
                        }
                        else
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                                "No DTA files remain and the number of OUT files (" + intOutFileCount + ") is less than the original DTA count (" +
                                m_DtaCount + "); treating this as a job failure");
                            blnProcessingError = true;
                        }
                    }
                }
            } while (intDTACountRemaining > 0);

            // Disable the Out File Watcher and the Out File Appender timers
            mOutFileWatcher.EnableRaisingEvents = false;
            mOutFileAppenderTimer.Stop();

            // Make sure objects are released
            clsGlobal.IdleLoop(5);
            clsProgRunner.GarbageCollectNow();

            UpdateSequestNodeProcessingStats(false);

            // Verify out file creation
            if (m_DebugLevel >= 2 && !blnProcessingError)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, " ... Verifying out file creation");
            }

            var OutFiles = Directory.GetFiles(m_WorkDir, "*.out");
            if (m_DebugLevel >= 1)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                    " ... Outfile count: " + (OutFiles.Length + mTotalOutFileCount).ToString("#,##0") + " files");
            }

            if (!mSequestVersionInfoStored)
            {
                // Tool version not yet recorded; record it now
                if (OutFiles.Length > 0)
                {
                    // Pass the path to the first out file created
                    mSequestVersionInfoStored = StoreToolVersionInfo(OutFiles[0]);
                }
                else
                {
                    mSequestVersionInfoStored = StoreToolVersionInfo(string.Empty);
                }
            }

            if ((mTotalOutFileCount + OutFiles.Length) < 1)
            {
                LogErrorToDatabase("No OUT files created, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                UpdateStatusMessage("No OUT files created");
                blnProcessingError = true;
            }

            var intIterationsRemaining = 3;
            do
            {
                // Process the remaining .Out files in mOutFileCandidates
                if (!ProcessCandidateOutFiles(true))
                {
                    // Wait 5 seconds, then try again (up to 3 times)
                    clsGlobal.IdleLoop(5);
                }

                intIterationsRemaining -= 1;
            } while (mOutFileCandidates.Count > 0 && intIterationsRemaining >= 0);

            // Append any remaining .out files to the _out.txt.tmp file, then rename it to _out.txt
            if (ConcatOutFiles(m_WorkDir, m_Dataset, m_JobNum))
            {
                // Add .out extension to list of file extensions to delete
                m_jobParams.AddResultFileExtensionToSkip(".out");
            }
            else
            {
                blnProcessingError = true;
            }

            if (blnProcessingError)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Zip concatenated .out files
            if (!ZipConcatOutFile(m_WorkDir, m_JobNum))
            {
                return CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE;
            }

            // Add cluster statistics to summary file
            AddClusterStatsToSummaryFile();

            // If we got here, everything worked
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private DateTime dtLastStatusUpdate = DateTime.MinValue;

        /// <summary>
        /// Provides a wait loop while Sequest is running
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            // Compute the progress by comparing the number of .Out files to the number of .Dta files
            // (only count the files every 15 seconds)
            if (DateTime.UtcNow.Subtract(mLastOutFileCountTime).TotalSeconds >= 15)
            {
                mLastOutFileCountTime = DateTime.UtcNow;
                CalculateNewStatus();
            }

            if (DateTime.UtcNow.Subtract(mLastActiveNodeQueryTime).TotalSeconds >= 120)
            {
                mLastActiveNodeQueryTime = DateTime.UtcNow;

                // Verify that nodes are still analyzing .dta files
                // This procedure will set mResetPVM to True if less than 50% of the nodes are creating .Out files
                ValidateProcessorsAreActive();

                // Look for .Out files that aren't yet tracked by mOutFileCandidateInfo
                CacheNewOutFiles();
            }

            // Update the status file (limit the updates to every 5 seconds)
            if (DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5)
            {
                dtLastStatusUpdate = DateTime.UtcNow;
                UpdateStatusRunning(m_progress, m_DtaCount);
            }

            LogProgress("Sequest");

            if (mResetPVM)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                    " ... calling m_CmdRunner.AbortProgramNow in LoopWaiting since mResetPVM = True");
                mCmdRunner.AbortProgramNow(false);
            }
            else if (mAbortSinceSequestIsStalled)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                    " ... calling m_CmdRunner.AbortProgramNow in LoopWaiting since mAbortSinceSequestIsStalled = True");
                mCmdRunner.AbortProgramNow(false);
            }
        }

        /// <summary>
        /// Reads sequest.log file after Sequest finishes and adds cluster statistics info to summary file
        /// </summary>
        /// <remarks></remarks>
        private void AddClusterStatsToSummaryFile()
        {
            // Write the statistics to the summary file
            m_SummaryFile.Add(Environment.NewLine + "Cluster node count: ".PadRight(24) + mSequestNodeProcessingStats.NumNodeMachines);
            m_SummaryFile.Add("Sequest process count: ".PadRight(24) + mSequestNodeProcessingStats.NumSlaveProcesses);
            m_SummaryFile.Add("Searched file count: ".PadRight(24) + mSequestNodeProcessingStats.SearchedFileCount.ToString("#,##0"));
            m_SummaryFile.Add("Total search time: ".PadRight(24) + mSequestNodeProcessingStats.TotalSearchTimeSeconds.ToString("#,##0") + " secs");
            m_SummaryFile.Add("Average search time: ".PadRight(24) + mSequestNodeProcessingStats.AvgSearchTime.ToString("##0.000") + " secs/spectrum");
        }

        private void CacheNewOutFiles()
        {
            try
            {
                var diWorkDir = new DirectoryInfo(m_WorkDir);

                foreach (var fiFile in diWorkDir.GetFiles("*.out", SearchOption.TopDirectoryOnly))
                {
                    HandleOutFileChange(fiFile.Name);
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, "Error in CacheNewOutFiles: " + ex.Message);
            }
        }

        private void CheckForStalledSequest()
        {
            const int SEQUEST_STALLED_WAIT_TIME_MINUTES = 30;

            try
            {
                var dblMinutesSinceLastOutFileStored = DateTime.UtcNow.Subtract(mLastOutFileStoreTime).TotalMinutes;

                if (dblMinutesSinceLastOutFileStored > SEQUEST_STALLED_WAIT_TIME_MINUTES)
                {
                    var blnResetPVM = false;

                    if (mSequestAppearsStalled)
                    {
                        if (dblMinutesSinceLastOutFileStored > SEQUEST_STALLED_WAIT_TIME_MINUTES * 2)
                        {
                            // We already reset SEQUEST once, and another 30 minutes has elapsed
                            // Examine the number of .dta files that remain
                            var intDTAsRemaining = GetDTAFileCountRemaining();

                            if (intDTAsRemaining <= (int)(m_DtaCount * 0.999))
                            {
                                // Just a handful of DTA files remain; assume they're corrupt
                                var diWorkDir = new DirectoryInfo(m_WorkDir);

                                LogWarning("Sequest is stalled, and " + intDTAsRemaining + " .DTA file" + CheckForPlurality(intDTAsRemaining) + " remain; " +
                                           "assuming they are corrupt and deleting them");

                                m_EvalMessage = "Sequest is stalled, but only " + intDTAsRemaining + " .DTA file" +
                                                CheckForPlurality(intDTAsRemaining) + " remain";

                                foreach (var fiFile in diWorkDir.GetFiles("*.dta", SearchOption.TopDirectoryOnly).ToList())
                                {
                                    fiFile.Delete();
                                }
                            }
                            else
                            {
                                // Too many DTAs remain unprocessed and Sequest is stalled
                                // Abort the job

                                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                                    "Sequest is stalled, and " + intDTAsRemaining + " .DTA files remain; aborting processing");
                                m_message = "Sequest is stalled and too many .DTA files are un-processed";
                                mAbortSinceSequestIsStalled = true;
                            }

                            blnResetPVM = true;
                        }
                    }
                    else
                    {
                        // Sequest appears stalled
                        // Reset PVM, then wait another 30 minutes

                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                            "Sequest has not created a new .out file in the last " + SEQUEST_STALLED_WAIT_TIME_MINUTES +
                            " minutes; will Reset PVM then wait another " + SEQUEST_STALLED_WAIT_TIME_MINUTES + " minutes");

                        blnResetPVM = true;

                        mSequestAppearsStalled = true;
                    }

                    if (blnResetPVM)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO,
                            "Setting mResetPVM to True in CheckForStalledSequest");
                        mResetPVM = true;
                    }
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, "Error in CheckForStalledSequest: " + ex.Message);
            }
        }

        private bool CopyFileToTransferFolder(string strSourceFileName, string strTargetFileName, bool blnAddToListOfServerFilesToDelete)
        {
            try
            {
                var strSourceFilePath = Path.Combine(m_WorkDir, strSourceFileName);
                var strTargetFilePath = Path.Combine(mTransferFolderPath, strTargetFileName);

                if (File.Exists(strSourceFilePath))
                {
                    if (!Directory.Exists(mTransferFolderPath))
                    {
                        Directory.CreateDirectory(mTransferFolderPath);
                    }

                    File.Copy(strSourceFilePath, strTargetFilePath, true);

                    if (blnAddToListOfServerFilesToDelete)
                    {
                        m_jobParams.AddServerFileToDelete(strTargetFilePath);
                    }
                }
            }
            catch (Exception ex)
            {
                if (m_DebugLevel >= 1)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                        "Error copying file " + strSourceFileName + " to " + mTransferFolderPath + ": " + ex.Message);
                }
                return false;
            }

            return true;
        }

        /// <summary>
        /// Finds specified integer value in a sequest.log file
        /// </summary>
        /// <param name="InpFileStr">A string containing the contents of the sequest.log file</param>
        /// <param name="RegexStr">Regular expresion match string to uniquely identify the line containing the count of interest</param>
        /// <returns>Count from desired line in sequest.log file if successful; 0 if count not found; -1 for error</returns>
        /// <remarks>If -1 returned, error message is in module variable m_ErrMsg</remarks>
        private int GetIntegerFromSeqLogFileString(string InpFileStr, string RegexStr)
        {
            try
            {
                // Find the specified substring in the input file string
                var TmpStr = Regex.Match(InpFileStr, RegexStr, RegexOptions.IgnoreCase | RegexOptions.Multiline).Value;

                if (string.IsNullOrWhiteSpace(TmpStr))
                {
                    return 0;
                }

                // Find the item count in the substring
                if (int.TryParse(Regex.Match(TmpStr, @"\d+").Value, out var retVal))
                {
                    return retVal;
                }

                m_ErrMsg = "Numeric value not found in the matched text";
                return -1;
            }
            catch (Exception ex)
            {
                m_ErrMsg = ex.Message;
                return -1;
            }
        }

        private bool GetNodeNamesFromSequestLog(string strLogFilePath)
        {
            var blnFoundSpawned = false;

            try
            {
                if (!File.Exists(strLogFilePath))
                {
                    return false;
                }

                if (m_DebugLevel >= 3)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, " ... extracting node names from sequest.log");
                }

                // Initialize the RegEx objects
                var reReceivedReadyMsg = new Regex(@"received ready messsage from (.+)\(", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                var reSpawnedSlaveProcesses = new Regex(@"Spawned (\d+) slave processes", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

                mSequestNodesSpawned = 0;
                using (var srLogFile = new StreamReader(new FileStream(strLogFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    // Read each line from the input file
                    while (!srLogFile.EndOfStream)
                    {
                        var strLineIn = srLogFile.ReadLine();

                        if (!string.IsNullOrWhiteSpace(strLineIn))
                        {
                            // Check whether line looks like:
                            //    9.  received ready messsage from p6(c0002)

                            var reMatch = reReceivedReadyMsg.Match(strLineIn);
                            if (reMatch.Success)
                            {
                                mSequestNodesSpawned += 1;
                            }
                            else
                            {
                                reMatch = reSpawnedSlaveProcesses.Match(strLineIn);
                                if (reMatch.Success)
                                {
                                    blnFoundSpawned = true;
                                }
                            }
                        }
                    }
                }

                if (blnFoundSpawned)
                {
                    if (m_DebugLevel >= 2)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                            " ... found " + mSequestNodesSpawned + " nodes in the sequest.log file");
                    }

                    var intNodeCountExpected = m_mgrParams.GetParam("SequestNodeCountExpected", 0);
                    var intNodeCountMinimum = (int)Math.Floor(0.85 * intNodeCountExpected);

                    if (mSequestNodesSpawned < intNodeCountMinimum)
                    {
                        // If fewer than intNodeCountMinimum .DTA files are present in the work directory, the node count spawned will be small
                        // Thus, need to count the number of DTAs
                        var intDTACountRemaining = GetDTAFileCountRemaining();

                        if (intDTACountRemaining > mSequestNodesSpawned)
                        {
                            mNodeCountSpawnErrorOccurences += 1;

                            var strMessage = "Not enough nodes were spawned (Threshold = " + intNodeCountMinimum + " nodes): " + mSequestNodesSpawned +
                                             " spawned vs. " + intNodeCountExpected + " expected; " +
                                             "mNodeCountSpawnErrorOccurences=" + mNodeCountSpawnErrorOccurences;
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, strMessage);

                            mResetPVM = true;
                        }
                    }
                    else if (mNodeCountSpawnErrorOccurences > 0)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO,
                            "Resetting mNodeCountSpawnErrorOccurences from " + mNodeCountSpawnErrorOccurences + " to 0");
                        mNodeCountSpawnErrorOccurences = 0;
                    }

                    return true;
                }

                if (m_DebugLevel >= 1)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                                         " ... Did not find 'Spawned xx slave processes' in the sequest.log file; node names not yet determined");
                }

                if (DateTime.UtcNow.Subtract(mLastSequestStartTime).TotalMinutes > 15)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                                         " ... Over 15 minutes have elapsed since sequest.exe was called; aborting since node names could not be determined");

                    mNodeCountSpawnErrorOccurences += 1;
                    mResetPVM = true;
                }
            }
            catch (Exception ex)
            {
                // Error occurred
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                    "Error parsing Sequest.log file in ValidateSequestNodeCount: " + ex.Message);
                return false;
            }

            return false;
        }

        /// <summary>
        /// Finds specified integer value in a sequest.log file
        /// </summary>
        /// <param name="InpFileStr">A string containing the contents of the sequest.log file</param>
        /// <param name="RegexStr">Regular expresion match string to uniquely identify the line containing the count of interest</param>
        /// <returns>Count from desired line in sequest.log file if successful; 0 if count not found; -1 for error</returns>
        /// <remarks>If -1 returned, error message is in module variable m_ErrMsg</remarks>
        private float GetSingleFromSeqLogFileString(string InpFileStr, string RegexStr)
        {
            try
            {
                // Find the specified substring in the input file string
                var TmpStr = Regex.Match(InpFileStr, RegexStr, RegexOptions.IgnoreCase | RegexOptions.Multiline).Value;
                if (string.IsNullOrEmpty(TmpStr))
                    return 0.0f;

                // Find the item count in the substring
                var RetVal = Convert.ToSingle(Regex.Match(TmpStr, "\\d+\\.\\d+").Value);
                return RetVal;
            }
            catch (Exception ex)
            {
                m_ErrMsg = ex.Message;
                return -1.0f;
            }
        }

        private float ComputeMedianProcessingTime()
        {
            int intMidPoint;

            if (mRecentOutFileSearchTimes.Count < 1)
                return 0;

            // Determine the median out file processing time
            // Note that search times in mRecentOutFileSearchTimes are in seconds
            var sngOutFileProcessingTimes = new float[mRecentOutFileSearchTimes.Count];

            mRecentOutFileSearchTimes.CopyTo(sngOutFileProcessingTimes, 0);

            Array.Sort(sngOutFileProcessingTimes);
            if (sngOutFileProcessingTimes.Length <= 2)
            {
                intMidPoint = 0;
            }
            else
            {
                intMidPoint = (int)Math.Floor(sngOutFileProcessingTimes.Length / 2.0);
            }

            return sngOutFileProcessingTimes[intMidPoint];
        }

        /// <summary>
        /// Adds newly created .Out file to mOutFileCandidates and mOutFileCandidateInfo
        /// </summary>
        /// <param name="OutFileName"></param>
        /// <remarks></remarks>
        private void HandleOutFileChange(string OutFileName)
        {
            try
            {
                if (string.IsNullOrEmpty(OutFileName))
                {
                    if (m_DebugLevel >= 3)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "OutFileName is empty; this is unexpected");
                    }
                }
                else
                {
                    if (m_DebugLevel >= 5)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Caching new out file: " + OutFileName);
                    }

                    if (!mOutFileCandidateInfo.ContainsKey(OutFileName))
                    {
                        var dtQueueTime = DateTime.UtcNow;
                        var objEntry = new KeyValuePair<string, DateTime>(OutFileName, dtQueueTime);

                        mOutFileCandidates.Enqueue(objEntry);
                        mOutFileCandidateInfo.Add(OutFileName, dtQueueTime);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "Error adding new candidate to mOutFileCandidates (" + OutFileName + "): " + ex.Message);
                }
            }
        }

        private bool InitializeUtilityRunner(string strTaskName, string strWorkDir)
        {
            return InitializeUtilityRunner(strTaskName, strWorkDir, intMonitoringIntervalMsec: 1000);
        }

        private bool InitializeUtilityRunner(string strTaskName, string strWorkDir, int intMonitoringIntervalMsec)
        {
            try
            {
                if (m_UtilityRunner == null)
                {
                    m_UtilityRunner = new clsRunDosProgram(strWorkDir, m_DebugLevel);
                    RegisterEvents(m_UtilityRunner);
                    m_UtilityRunner.Timeout += m_UtilityRunner_Timeout;
                }
                else
                {
                    if (m_UtilityRunner.State != clsProgRunner.States.NotMonitoring)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                            "Cannot re-initialize the UtilityRunner to perform task " + strTaskName + " since already running task " +
                            mUtilityRunnerTaskName);
                        return false;
                    }
                }

                if (intMonitoringIntervalMsec < 250)
                    intMonitoringIntervalMsec = 250;
                m_UtilityRunner.MonitorInterval = intMonitoringIntervalMsec;

                mUtilityRunnerTaskName = strTaskName;
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                    "Exception in InitializeUtilityRunner for task " + strTaskName + ": " + ex.Message);
                return false;
            }

            return true;
        }

        private bool ProcessCandidateOutFiles(bool blnProcessAllRemainingFiles)
        {
            bool blnAppendSuccess;

            var intItemsProcessed = 0;

            // Examine mOutFileHandlerInUse; if greater then zero, exit the sub
            if (System.Threading.Interlocked.Read(ref mOutFileHandlerInUse) > 0)
            {
                return false;
            }

            try
            {
                System.Threading.Interlocked.Increment(ref mOutFileHandlerInUse);

                if (m_DebugLevel >= 4)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                        "Examining out file creation dates (Candidate Count = " + mOutFileCandidates.Count + ")");
                }

                blnAppendSuccess = true;

                KeyValuePair<string, DateTime> objEntry;
                if (mOutFileCandidates.Count > 0 && !mSequestVersionInfoStored)
                {
                    // Determine tool version

                    // Pass the path to the first out file created
                    objEntry = mOutFileCandidates.Peek();
                    if (StoreToolVersionInfo(Path.Combine(m_WorkDir, objEntry.Key)))
                    {
                        mSequestVersionInfoStored = true;
                    }
                }

                if (string.IsNullOrEmpty(mTempConcatenatedOutFilePath))
                {
                    mTempConcatenatedOutFilePath = Path.Combine(m_WorkDir, m_Dataset + "_out.txt.tmp");
                }

                if (mOutFileCandidates.Count > 0)
                {
                    // Examine the time associated with the next item that would be dequeued
                    objEntry = mOutFileCandidates.Peek();

                    if (blnProcessAllRemainingFiles || DateTime.UtcNow.Subtract(objEntry.Value).TotalSeconds >= OUT_FILE_APPEND_HOLDOFF_SECONDS)
                    {
                        // Open the _out.txt.tmp file
                        using (var swTargetFile = new StreamWriter(new FileStream(mTempConcatenatedOutFilePath, FileMode.Append, FileAccess.Write, FileShare.Read)))
                        {
                            intItemsProcessed = 0;

                            while (mOutFileCandidates.Count > 0 && blnAppendSuccess &&
                                   (blnProcessAllRemainingFiles ||
                                    DateTime.UtcNow.Subtract(objEntry.Value).TotalSeconds >= OUT_FILE_APPEND_HOLDOFF_SECONDS))
                            {
                                // Entry is old enough (or blnProcessAllRemainingFiles=True); pop it off the queue
                                objEntry = mOutFileCandidates.Dequeue();
                                intItemsProcessed += 1;

                                try
                                {
                                    var fiOutFile = new FileInfo(Path.Combine(m_WorkDir, objEntry.Key));
                                    AppendOutFile(fiOutFile, swTargetFile);
                                    mLastOutFileStoreTime = DateTime.UtcNow;
                                    mSequestAppearsStalled = false;
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("Warning, exception appending out file: " + ex.Message);
                                    blnAppendSuccess = false;
                                }

                                if (mOutFileCandidates.Count > 0)
                                {
                                    objEntry = mOutFileCandidates.Peek();
                                }
                            }
                        }
                    }
                }

                if (intItemsProcessed > 0 || blnProcessAllRemainingFiles)
                {
                    if (m_DebugLevel >= 3)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                            "Appended " + intItemsProcessed + " .out file" + CheckForPlurality(intItemsProcessed) + " to the _out.txt.tmp file; " +
                            mOutFileCandidates.Count + " out file" + CheckForPlurality(mOutFileCandidates.Count) + " remain in the queue");
                    }

                    if (blnProcessAllRemainingFiles || DateTime.UtcNow.Subtract(mLastTempFileCopyTime).TotalSeconds >= TEMP_FILE_COPY_INTERVAL_SECONDS)
                    {
                        string strSourceFileName;
                        bool blnSuccess;
                        if (!mTempJobParamsCopied)
                        {
                            strSourceFileName = "JobParameters_" + m_JobNum + ".xml";
                            blnSuccess = CopyFileToTransferFolder(strSourceFileName, strSourceFileName + ".tmp", true);

                            if (blnSuccess)
                            {
                                strSourceFileName = m_jobParams.GetParam("ParmFileName");
                                blnSuccess = CopyFileToTransferFolder(strSourceFileName, strSourceFileName + ".tmp", true);
                            }

                            if (blnSuccess)
                            {
                                mTempJobParamsCopied = true;
                            }
                        }

                        if (intItemsProcessed > 0)
                        {
                            // Copy the _out.txt.tmp file
                            strSourceFileName = Path.GetFileName(mTempConcatenatedOutFilePath);
                            blnSuccess = CopyFileToTransferFolder(strSourceFileName, strSourceFileName, true);
                        }

                        // Copy the sequest.log file (rename to sequest.log.tmp when copying)
                        strSourceFileName = "sequest.log";
                        blnSuccess = CopyFileToTransferFolder(strSourceFileName, strSourceFileName + ".tmp", true);

                        mLastTempFileCopyTime = DateTime.UtcNow;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Warning, error in ProcessCandidateOutFiles: " + ex.Message);
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Error in ProcessCandidateOutFiles: " + ex.Message);
                blnAppendSuccess = false;
            }
            finally
            {
                // Make sure mOutFileHandlerInUse is now zero
                long lngZero = 0;
                System.Threading.Interlocked.Exchange(ref mOutFileHandlerInUse, lngZero);
            }

            return blnAppendSuccess;
        }

        private void RenameSequestLogFile()
        {
            var strNewName = "??";

            try
            {
                var fiFileInfo = new FileInfo(Path.Combine(m_WorkDir, "sequest.log"));

                if (fiFileInfo.Exists)
                {
                    strNewName = Path.GetFileNameWithoutExtension(fiFileInfo.Name) + "_" + fiFileInfo.LastWriteTime.ToString("yyyyMMdd_HHmm") + ".log";
                    fiFileInfo.MoveTo(Path.Combine(m_WorkDir, strNewName));

                    // Copy the renamed sequest.log file to the transfer directory
                    CopyFileToTransferFolder(strNewName, strNewName, false);
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                    "Error renaming sequest.log file to " + strNewName + ": " + ex.Message);
            }
        }

        private bool ResetPVMWithRetry(int intMaxPVMResetAttempts)
        {
            var blnSuccess = false;

            if (intMaxPVMResetAttempts < 1)
                intMaxPVMResetAttempts = 1;

            while (intMaxPVMResetAttempts > 0)
            {
                blnSuccess = ResetPVM();
                if (blnSuccess)
                {
                    break;
                }

                intMaxPVMResetAttempts -= 1;
                if (intMaxPVMResetAttempts > 0)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                                         " ... Error resetting PVM; will try " + intMaxPVMResetAttempts + " more time" + CheckForPlurality(intMaxPVMResetAttempts));
                }
            }

            if (blnSuccess)
            {
                UpdateSequestNodeProcessingStats(false);
                RenameSequestLogFile();
            }

            return blnSuccess;
        }

        private bool ResetPVM()
        {
            try
            {
                // Folder with PVM
                var PVMProgFolder = m_mgrParams.GetParam("PVMProgLoc");
                if (string.IsNullOrWhiteSpace(PVMProgFolder))
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "PVMProgLoc parameter not defined for this manager");
                    return false;
                }

                // Full path to PVM exe
                var ExePath = Path.Combine(PVMProgFolder, "pvm.exe");
                if (!File.Exists(ExePath))
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "PVM not found: " + ExePath);
                    return false;
                }

                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, " ... Resetting PVM");

                var blnSuccess = ResetPVMHalt(PVMProgFolder);
                if (!blnSuccess)
                {
                    return false;
                }

                blnSuccess = ResetPVMWipeTemp(PVMProgFolder);
                if (!blnSuccess)
                {
                    return false;
                }

                blnSuccess = ResetPVMStartPVM(PVMProgFolder);
                if (!blnSuccess)
                {
                    return false;
                }

                blnSuccess = ResetPVMAddNodes(PVMProgFolder);
                if (!blnSuccess)
                {
                    return false;
                }

                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, " ... PVM restarted");
                mLastActiveNodeQueryTime = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception in ResetPVM: " + ex.Message);
                return false;
            }

            return true;
        }

        private bool ResetPVMHalt(string PVMProgFolder)
        {
            try
            {
                var strBatchFilePath = Path.Combine(PVMProgFolder, "HaltPVM.bat");
                if (!File.Exists(strBatchFilePath))
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Batch file not found: " + strBatchFilePath);
                    return false;
                }

                // Run the batch file
                if (m_DebugLevel >= 2)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "     " + strBatchFilePath);
                }

                var strTaskName = "HaltPVM";
                if (!InitializeUtilityRunner(strTaskName, PVMProgFolder))
                {
                    return false;
                }

                var intMaxRuntimeSeconds = 90;
                var blnSuccess = m_UtilityRunner.RunProgram(strBatchFilePath, "", strTaskName, false, intMaxRuntimeSeconds);

                if (!blnSuccess)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "UtilityRunner returned False for " + strBatchFilePath);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception in ResetPVMHalt: " + ex.Message);
                return false;
            }

            // Wait 5 seconds
            clsGlobal.IdleLoop(5);

            return true;
        }

        private bool ResetPVMWipeTemp(string PVMProgFolder)
        {
            try
            {
                var strBatchFilePath = Path.Combine(PVMProgFolder, "wipe_temp.bat");
                if (!File.Exists(strBatchFilePath))
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Batch file not found: " + strBatchFilePath);
                    return false;
                }

                // Run the batch file
                if (m_DebugLevel >= 2)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "     " + strBatchFilePath);
                }

                var strTaskName = "WipeTemp";
                if (!InitializeUtilityRunner(strTaskName, PVMProgFolder))
                {
                    return false;
                }

                var intMaxRuntimeSeconds = 120;
                var blnSuccess = m_UtilityRunner.RunProgram(strBatchFilePath, "", strTaskName, true, intMaxRuntimeSeconds);

                if (!blnSuccess)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "UtilityRunner returned False for " + strBatchFilePath);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception in ResetPVMWipeTemp: " + ex.Message);
                return false;
            }

            // Wait 5 seconds
            clsGlobal.IdleLoop(5);

            return true;
        }

        private bool ResetPVMStartPVM(string PVMProgFolder)
        {
            try
            {
                // StartPVM.bat should have a line like this:
                //  pvm.exe -n192.168.1.102 c:\cluster\pvmhosts.txt < QuitNow.txt
                // or like this:
                //  pvm.exe c:\cluster\pvmhosts.txt < QuitNow.txt

                // QuitNow.txt should have this line:
                // quit

                var strBatchFilePath = Path.Combine(PVMProgFolder, "StartPVM.bat");
                if (!File.Exists(strBatchFilePath))
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Batch file not found: " + strBatchFilePath);
                    return false;
                }

                // Run the batch file
                if (m_DebugLevel >= 2)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "     " + strBatchFilePath);
                }

                var strTaskName = "StartPVM";
                if (!InitializeUtilityRunner(strTaskName, PVMProgFolder))
                {
                    return false;
                }

                var intMaxRuntimeSeconds = 120;
                var blnSuccess = m_UtilityRunner.RunProgram(strBatchFilePath, "", strTaskName, true, intMaxRuntimeSeconds);

                if (!blnSuccess)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "UtilityRunner returned False for " + strBatchFilePath);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception in ResetPVMStartPVM: " + ex.Message);
                return false;
            }

            // Wait 5 seconds
            clsGlobal.IdleLoop(5);

            return true;
        }

        private bool ResetPVMAddNodes(string PVMProgFolder)
        {
            try
            {
                var strBatchFilePath = Path.Combine(PVMProgFolder, "AddHosts.bat");
                if (!File.Exists(strBatchFilePath))
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Batch file not found: " + strBatchFilePath);
                    return false;
                }

                // Run the batch file
                if (m_DebugLevel >= 2)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "     " + strBatchFilePath);
                }

                var strTaskName = "AddHosts";
                if (!InitializeUtilityRunner(strTaskName, PVMProgFolder))
                {
                    return false;
                }

                var intMaxRuntimeSeconds = 120;
                var blnSuccess = m_UtilityRunner.RunProgram(strBatchFilePath, "", strTaskName, true, intMaxRuntimeSeconds);

                if (!blnSuccess)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "UtilityRunner returned False for " + strBatchFilePath);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception in ResetPVMAddNodes: " + ex.Message);
                return false;
            }

            // Wait 5 seconds
            clsGlobal.IdleLoop(5);

            return true;
        }

        private void UpdateSequestNodeProcessingStats(bool blnProcessAllSequestLogFiles)
        {
            if (blnProcessAllSequestLogFiles)
            {
                var diWorkDir = new DirectoryInfo(m_WorkDir);

                foreach (var fiFile in diWorkDir.GetFiles("sequest*.log*"))
                {
                    UpdateSequestNodeProcessingStatsOneFile(fiFile.FullName);
                }
            }
            else
            {
                UpdateSequestNodeProcessingStatsOneFile(Path.Combine(m_WorkDir, "sequest.log"));
            }
        }

        private void UpdateSequestNodeProcessingStatsOneFile(string SeqLogFilePath)
        {
            // Verify sequest.log file exists
            if (!File.Exists(SeqLogFilePath))
            {
                if (m_DebugLevel >= 2)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                        "Sequest log file not found, cannot update Node Processing Stats");
                    return;
                }
            }

            // Read the sequest.log file
            var sbContents = new StringBuilder();
            var intDTAsSearched = 0;

            try
            {
                using (var srInFile = new StreamReader(new FileStream(SeqLogFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (strLineIn != null && strLineIn.StartsWith("Searched dta file"))
                        {
                            intDTAsSearched += 1;
                        }

                        sbContents.AppendLine(strLineIn);
                    }
                }
            }
            catch (Exception ex)
            {
                var Msg = "UpdateNodeStats: Exception reading sequest log file: " + ex.Message;
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, Msg);
                return;
            }

            var strFileContents = sbContents.ToString();

            // Node machine count
            var NumNodeMachines = GetIntegerFromSeqLogFileString(strFileContents, "starting the sequest task on\\s+\\d+\\s+node");
            if (NumNodeMachines == 0)
            {
                var Msg = "UpdateNodeStats: node machine count line not found";
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, Msg);
            }
            else if (NumNodeMachines < 0)
            {
                var Msg = "UpdateNodeStats: Exception retrieving node machine count: " + m_ErrMsg;
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, Msg);
            }

            if (NumNodeMachines > mSequestNodeProcessingStats.NumNodeMachines)
            {
                mSequestNodeProcessingStats.NumNodeMachines = NumNodeMachines;
            }

            // Sequest process count
            var NumSlaveProcesses = GetIntegerFromSeqLogFileString(strFileContents, "Spawned\\s+\\d+\\s+slave processes");
            if (NumSlaveProcesses == 0)
            {
                var Msg = "UpdateNodeStats: slave process count line not found";
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, Msg);
            }
            else if (NumSlaveProcesses < 0)
            {
                var Msg = "UpdateNodeStats: Exception retrieving slave process count: " + m_ErrMsg;
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, Msg);
            }

            if (NumSlaveProcesses > mSequestNodeProcessingStats.NumSlaveProcesses)
            {
                mSequestNodeProcessingStats.NumSlaveProcesses = NumSlaveProcesses;
            }

            // Total search time
            double TotalSearchTimeSeconds = GetIntegerFromSeqLogFileString(strFileContents, "Total search time:\\s+\\d+");
            if (TotalSearchTimeSeconds <= 0)
            {
                // Total search time line not found (or error)
                // Use internal tracking variables instead
                TotalSearchTimeSeconds = mSequestSearchEndTime.Subtract(mSequestSearchStartTime).TotalSeconds;
            }

            mSequestNodeProcessingStats.TotalSearchTimeSeconds += TotalSearchTimeSeconds;

            // Searched file count
            var SearchedFileCount = GetIntegerFromSeqLogFileString(strFileContents, "secs for\\s+\\d+\\s+files");
            if (SearchedFileCount <= 0)
            {
                // Searched file count line not found (or error)
                // Use intDTAsSearched instead
                SearchedFileCount = intDTAsSearched;
            }

            mSequestNodeProcessingStats.SearchedFileCount += intDTAsSearched;

            if (mSequestNodeProcessingStats.SearchedFileCount > 0)
            {
                // Compute average search time
                mSequestNodeProcessingStats.AvgSearchTime = (float)(mSequestNodeProcessingStats.TotalSearchTimeSeconds /
                                                                    mSequestNodeProcessingStats.SearchedFileCount);
            }
        }

        /// <summary>
        /// Uses PVM command ps -a to determine the number of active nodes
        /// Sets mResetPVM to True if fewer than 50% of the nodes are creating .Out files
        /// </summary>
        /// <remarks></remarks>
        private void ValidateProcessorsAreActive()
        {
            try
            {
                if (!mSequestLogNodesFound)
                {
                    // Parse the Sequest.Log file to determine the names of the spawned nodes

                    var strLogFilePath = Path.Combine(m_WorkDir, "sequest.log");

                    mSequestLogNodesFound = GetNodeNamesFromSequestLog(strLogFilePath);

                    if (!mSequestLogNodesFound || mResetPVM)
                    {
                        return;
                    }
                }

                // Determine the number of Active Nodes using PVM
                var PVMProgFolder = m_mgrParams.GetParam("PVMProgLoc");
                if (string.IsNullOrWhiteSpace(PVMProgFolder))
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "PVMProgLoc parameter not defined for this manager");
                    return;
                }

                var strBatchFilePath = Path.Combine(PVMProgFolder, "CheckActiveNodes.bat");
                if (!File.Exists(strBatchFilePath))
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Batch file not found: " + strBatchFilePath);
                    return;
                }

                var strActiveNodesFilePath = Path.Combine(m_WorkDir, "ActiveNodesOutput.tmp");

                if (m_DebugLevel >= 4)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "     " + strBatchFilePath);
                }

                var strTaskName = "CheckActiveNodes";
                if (!InitializeUtilityRunner(strTaskName, PVMProgFolder))
                {
                    return;
                }

                var intMaxRuntimeSeconds = 60;
               var  blnSuccess = m_UtilityRunner.RunProgram(strBatchFilePath, "", strTaskName, true, intMaxRuntimeSeconds);

                if (!blnSuccess)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "UtilityRunner returned False for " + strBatchFilePath);
                }

                if (!File.Exists(strActiveNodesFilePath))
                {
                    if (m_DebugLevel >= 1)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                            "Warning, ActiveNodes files not found: " + strActiveNodesFilePath);
                    }

                    return;
                }

                // Parse the ActiveNodesOutput.tmp file
                var intNodeCountCurrent = 0;
                using (var srInFile = new StreamReader(new FileStream(strActiveNodesFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();
                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        // Check whether line looks like:
                        //    p6    c0007     6/c,f sequest27_slave

                        var reMatch = m_ActiveNodeRegEx.Match(strLineIn);
                        if (reMatch.Success)
                        {
                            var strNodeName = reMatch.Groups["node"].Value;

                            if (mSequestNodes.TryGetValue(strNodeName, out _))
                            {
                                mSequestNodes[strNodeName] = DateTime.UtcNow;
                            }
                            else
                            {
                                mSequestNodes.Add(strNodeName, DateTime.UtcNow);
                            }

                            intNodeCountCurrent += 1;
                        }
                    }
                }

                // Log the number of active nodes every 10 minutes
                if (m_DebugLevel >= 4 || DateTime.UtcNow.Subtract(mLastActiveNodeLogTime).TotalSeconds >= 600)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                        " ... " + intNodeCountCurrent + " / " + mSequestNodesSpawned + " Sequest nodes are active; median processing time = " +
                        ComputeMedianProcessingTime().ToString("0.0") + " seconds/spectrum; " + m_progress.ToString("0.0") + "% complete");
                    mLastActiveNodeLogTime = DateTime.UtcNow;
                }

                // Look for nodes that have been missing for at least 5 minutes
                var intNodeCountActive = 0;
                foreach (var objItem in mSequestNodes)
                {
                    if (DateTime.UtcNow.Subtract(objItem.Value).TotalMinutes <= STALE_NODE_THRESHOLD_MINUTES)
                    {
                        intNodeCountActive += 1;
                    }
                }

                // Define the minimum node count as 50% of the number of nodes spawned
                var intActiveNodeCountMinimum = (int)Math.Floor(0.5 * mSequestNodesSpawned);

                if (intNodeCountActive < intActiveNodeCountMinimum && !mIgnoreNodeCountActiveErrors)
                {
                    mNodeCountActiveErrorOccurences += 1;
                    var strMessage = "Too many nodes are inactive (Threshold = " + intActiveNodeCountMinimum + " nodes): " + intNodeCountActive +
                                     " active vs. " + mSequestNodesSpawned + " total nodes at start; " +
                                     "mNodeCountActiveErrorOccurences=" + mNodeCountActiveErrorOccurences;

                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, strMessage);
                    mResetPVM = true;
                }
                else if (mNodeCountActiveErrorOccurences > 0)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO,
                        "Resetting mNodeCountActiveErrorOccurences from " + mNodeCountActiveErrorOccurences + " to 0");
                    mNodeCountActiveErrorOccurences = 0;
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                    "Exception in ValidateProcessorsAreActive: " + ex.Message);
            }
        }

        private void mOutFileWatcher_Created(object sender, FileSystemEventArgs e)
        {
            HandleOutFileChange(e.Name);
        }

        private void mOutFileWatcher_Changed(object sender, FileSystemEventArgs e)
        {
            HandleOutFileChange(e.Name);
        }

        private void mOutFileAppenderTime_Elapsed(object sender, ElapsedEventArgs e)
        {
            ProcessCandidateOutFiles(false);
            CheckForStalledSequest();
        }

        private void m_UtilityRunner_Timeout()
        {
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                "UtilityRunner task " + mUtilityRunnerTaskName + " has timed out; " + m_UtilityRunner.MaxRuntimeSeconds + " seconds has elapsed");
        }

        #endregion
    }
}
