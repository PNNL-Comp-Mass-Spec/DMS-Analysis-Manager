//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using AnalysisManagerBase;
using PRISM;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Timers;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerSequestPlugin
{
    /// <summary>
    /// Overrides Sequest tool runner to provide cluster-specific methods
    /// </summary>
    public class AnalysisToolRunnerSeqCluster : AnalysisToolRunnerSeqBase
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
        private Timer mOutFileAppenderTimer;

        /// <summary>
        /// This tracks the names of out files that have been created
        /// </summary>
        /// <remarks>
        /// Every OUT_FILE_APPEND_INTERVAL_SECONDS, this plugin looks for candidates older than OUT_FILE_APPEND_HOLDOFF_SECONDS
        /// For each, it appends the data to the _out.txt.tmp file, deletes the corresponding DTA file, and removes it from mOutFileCandidates
        /// </remarks>
        private readonly Queue<KeyValuePair<string, DateTime>> mOutFileCandidates = new();

        private readonly Dictionary<string, DateTime> mOutFileCandidateInfo = new();
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
        private int mNodeCountSpawnErrorOccurrences;
        private int mNodeCountActiveErrorOccurrences;
        private DateTime mLastSequestStartTime;

        private RunDosProgram mCmdRunner;
        private RunDosProgram mUtilityRunner;

        private string mUtilityRunnerTaskName = string.Empty;

        private string mErrMsg = "";

        // This dictionary tracks the most recent time each node was observed via PVM command "ps -a"
        private readonly Dictionary<string, DateTime> mSequestNodes = new();

        private bool mSequestLogNodesFound;
        private int mSequestNodesSpawned;
        private bool mIgnoreNodeCountActiveErrors;

        private udtSequestNodeProcessingStats mSequestNodeProcessingStats;

        private DateTime mSequestSearchStartTime;
        private DateTime mSequestSearchEndTime;

        private readonly Regex mActiveNodeRegEx = new(@"\s+(?<node>[a-z0-9-.]+\s+[a-z0-9]+)\s+.+sequest.+slave.*",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

        #endregion

        #region "Methods"

        /// <summary>
        /// Modifies MakeOUTFiles to remove multiple processes used on non-clustered machines
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        protected override CloseOutType MakeOUTFiles()
        {
            // Creates Sequest .out files from DTA files

            int dtaCountRemaining;
            var processingError = false;

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

            mTransferFolderPath = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_TRANSFER_FOLDER_PATH);
            mTransferFolderPath = Path.Combine(mTransferFolderPath, mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_DATASET_FOLDER_NAME));
            mTransferFolderPath = Path.Combine(mTransferFolderPath, mJobParams.GetParam(AnalysisJob.STEP_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME));

            // Initialize the out file watcher
            mOutFileWatcher = new FileSystemWatcher();
            mOutFileWatcher.Created += OutFileWatcher_Created;
            mOutFileWatcher.Changed += OutFileWatcher_Changed;
            mOutFileWatcher.BeginInit();
            mOutFileWatcher.Path = mWorkDir;
            mOutFileWatcher.IncludeSubdirectories = false;
            mOutFileWatcher.Filter = "*.out";
            mOutFileWatcher.NotifyFilter = NotifyFilters.FileName;
            mOutFileWatcher.EndInit();
            mOutFileWatcher.EnableRaisingEvents = true;

            var ProgLoc = mMgrParams.GetParam("SeqProgLoc");
            if (!File.Exists(ProgLoc))
            {
                mMessage = "Sequest .Exe not found";
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, mMessage + " at " + ProgLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Initialize the Out File Appender timer
            mOutFileAppenderTimer = new Timer(OUT_FILE_APPEND_INTERVAL_SECONDS * 1000);
            mOutFileAppenderTimer.Elapsed += OutFileAppenderTime_Elapsed;
            mOutFileAppenderTimer.Start();

            var workDir = new DirectoryInfo(mWorkDir);

            if (workDir.GetFiles("sequest*.log*").Length > 0)
            {
                // Parse any sequest.log files present in the work directory to determine the number of spectra already processed
                UpdateSequestNodeProcessingStats(true);
            }

            mNodeCountSpawnErrorOccurrences = 0;
            mNodeCountActiveErrorOccurrences = 0;
            mLastSequestStartTime = DateTime.UtcNow;

            mIgnoreNodeCountActiveErrors = mJobParams.GetJobParameter("IgnoreSequestNodeCountActiveErrors", false);

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

                mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                // Define the arguments to pass to the Sequest .Exe
                var arguments = " -P" + mJobParams.GetParam("parmFileName") + " *.dta";
                if (mDebugLevel >= 1)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "  " + ProgLoc + " " + arguments);
                }

                // Run Sequest to generate OUT files
                mLastSequestStartTime = DateTime.UtcNow;
                var success = mCmdRunner.RunProgram(ProgLoc, arguments, "Seq", true);

                mSequestSearchEndTime = DateTime.UtcNow;

                if (success && !mResetPVM && !mAbortSinceSequestIsStalled)
                {
                    dtaCountRemaining = 0;
                }
                else
                {
                    if (!mResetPVM && !mAbortSinceSequestIsStalled)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                            " ... CmdRunner returned false; ExitCode = " + mCmdRunner.ExitCode);
                    }

                    // Check whether any .DTA files remain for this dataset
                    dtaCountRemaining = GetDTAFileCountRemaining();

                    if (dtaCountRemaining > 0)
                    {
                        success = false;
                        if (mNodeCountSpawnErrorOccurrences < MAX_NODE_RESPAWN_ATTEMPTS && mNodeCountActiveErrorOccurrences < MAX_NODE_RESPAWN_ATTEMPTS)
                        {
                            const int maxPVMResetAttempts = 4;

                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO, "Resetting PVM in MakeOUTFiles");
                            success = ResetPVMWithRetry(maxPVMResetAttempts);
                        }

                        if (!success)
                        {
                            // Log message "Error resetting PVM; disabling manager locally"
                            mMessage = PVM_RESET_ERROR_MESSAGE;
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                                mMessage + "; disabling manager locally");
                            mNeedToAbortProcessing = true;
                            processingError = true;
                            break;
                        }
                    }
                    else
                    {
                        // No .DTAs remain; if we have as many .out files as the original source .dta files, treat this as success, otherwise as a failure
                        var outFileCount = GetOUTFileCountRemaining() + mTotalOutFileCount;

                        if (outFileCount == mDtaCount)
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                                " ... The number of OUT files (" + outFileCount + ") is equivalent to the original DTA count (" + mDtaCount +
                                "); we'll consider this a successful job despite the Sequest CmdRunner error");
                        }
                        else if (outFileCount > mDtaCount)
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                                " ... The number of OUT files (" + outFileCount + ") is greater than the original DTA count (" + mDtaCount +
                                "); we'll consider this a successful job despite the Sequest CmdRunner error");
                        }
                        else if (outFileCount >= (int)(mDtaCount * 0.999))
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                                " ... The number of OUT files (" + outFileCount + ") is within 0.1% of the original DTA count (" + mDtaCount +
                                "); we'll consider this a successful job despite the Sequest CmdRunner error");
                        }
                        else
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                                "No DTA files remain and the number of OUT files (" + outFileCount + ") is less than the original DTA count (" +
                                mDtaCount + "); treating this as a job failure");
                            processingError = true;
                        }
                    }
                }
            } while (dtaCountRemaining > 0);

            // Disable the Out File Watcher and the Out File Appender timers
            mOutFileWatcher.EnableRaisingEvents = false;
            mOutFileAppenderTimer.Stop();

            // Make sure objects are released
            Global.IdleLoop(5);
            ProgRunner.GarbageCollectNow();

            UpdateSequestNodeProcessingStats(false);

            // Verify out file creation
            if (mDebugLevel >= 2 && !processingError)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, " ... Verifying out file creation");
            }

            var OutFiles = Directory.GetFiles(mWorkDir, "*.out");
            if (mDebugLevel >= 1)
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
                LogErrorToDatabase("No OUT files created, job " + mJob + ", step " + mJobParams.GetParam("Step"));
                UpdateStatusMessage("No OUT files created");
                processingError = true;
            }

            var iterationsRemaining = 3;
            do
            {
                // Process the remaining .Out files in mOutFileCandidates
                if (!ProcessCandidateOutFiles(true))
                {
                    // Wait 5 seconds, then try again (up to 3 times)
                    Global.IdleLoop(5);
                }

                iterationsRemaining--;
            } while (mOutFileCandidates.Count > 0 && iterationsRemaining >= 0);

            // Append any remaining .out files to the _out.txt.tmp file, then rename it to _out.txt
            if (ConcatOutFiles(mWorkDir, mDatasetName, mJob))
            {
                // Add .out extension to list of file extensions to delete
                mJobParams.AddResultFileExtensionToSkip(".out");
            }
            else
            {
                processingError = true;
            }

            if (processingError)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Zip concatenated .out files
            if (!ZipConcatOutFile(mWorkDir, mJob))
            {
                return CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE;
            }

            // Add cluster statistics to summary file
            AddClusterStatsToSummaryFile();

            // If we got here, everything worked
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private DateTime mLastStatusUpdate = DateTime.MinValue;

        /// <summary>
        /// Provides a wait loop while Sequest is running
        /// </summary>
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
            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds >= 5)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                UpdateStatusRunning(mProgress, mDtaCount);
            }

            LogProgress("Sequest");

            if (mResetPVM)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                    " ... calling mCmdRunner.AbortProgramNow in LoopWaiting since mResetPVM = True");
                mCmdRunner.AbortProgramNow(false);
            }
            else if (mAbortSinceSequestIsStalled)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                    " ... calling mCmdRunner.AbortProgramNow in LoopWaiting since mAbortSinceSequestIsStalled = True");
                mCmdRunner.AbortProgramNow(false);
            }
        }

        /// <summary>
        /// Reads sequest.log file after Sequest finishes and adds cluster statistics info to summary file
        /// </summary>
        private void AddClusterStatsToSummaryFile()
        {
            // Write the statistics to the summary file
            mSummaryFile.Add(Environment.NewLine + "Cluster node count: ".PadRight(24) + mSequestNodeProcessingStats.NumNodeMachines);
            mSummaryFile.Add("Sequest process count: ".PadRight(24) + mSequestNodeProcessingStats.NumSlaveProcesses);
            mSummaryFile.Add("Searched file count: ".PadRight(24) + mSequestNodeProcessingStats.SearchedFileCount.ToString("#,##0"));
            mSummaryFile.Add("Total search time: ".PadRight(24) + mSequestNodeProcessingStats.TotalSearchTimeSeconds.ToString("#,##0") + " secs");
            mSummaryFile.Add("Average search time: ".PadRight(24) + mSequestNodeProcessingStats.AvgSearchTime.ToString("##0.000") + " secs/spectrum");
        }

        private void CacheNewOutFiles()
        {
            try
            {
                var workDir = new DirectoryInfo(mWorkDir);

                foreach (var outFile in workDir.GetFiles("*.out", SearchOption.TopDirectoryOnly))
                {
                    HandleOutFileChange(outFile.Name);
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
                var minutesSinceLastOutFileStored = DateTime.UtcNow.Subtract(mLastOutFileStoreTime).TotalMinutes;

                if (minutesSinceLastOutFileStored > SEQUEST_STALLED_WAIT_TIME_MINUTES)
                {
                    var resetPVM = false;

                    if (mSequestAppearsStalled)
                    {
                        if (minutesSinceLastOutFileStored > SEQUEST_STALLED_WAIT_TIME_MINUTES * 2)
                        {
                            // We already reset SEQUEST once, and another 30 minutes has elapsed
                            // Examine the number of .dta files that remain
                            var dtaCountRemaining = GetDTAFileCountRemaining();

                            if (dtaCountRemaining <= (int)(mDtaCount * 0.999))
                            {
                                // Just a handful of DTA files remain; assume they're corrupt
                                var workDir = new DirectoryInfo(mWorkDir);

                                LogWarning("Sequest is stalled, and " + dtaCountRemaining + " .DTA file" + CheckForPlurality(dtaCountRemaining) + " remain; " +
                                           "assuming they are corrupt and deleting them");

                                mEvalMessage = "Sequest is stalled, but only " + dtaCountRemaining + " .DTA file" +
                                                CheckForPlurality(dtaCountRemaining) + " remain";

                                foreach (var dtaFile in workDir.GetFiles("*.dta", SearchOption.TopDirectoryOnly).ToList())
                                {
                                    dtaFile.Delete();
                                }
                            }
                            else
                            {
                                // Too many DTAs remain unprocessed and Sequest is stalled
                                // Abort the job

                                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                                    "Sequest is stalled, and " + dtaCountRemaining + " .DTA files remain; aborting processing");
                                mMessage = "Sequest is stalled and too many .DTA files are un-processed";
                                mAbortSinceSequestIsStalled = true;
                            }

                            resetPVM = true;
                        }
                    }
                    else
                    {
                        // Sequest appears stalled
                        // Reset PVM, then wait another 30 minutes

                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                            "Sequest has not created a new .out file in the last " + SEQUEST_STALLED_WAIT_TIME_MINUTES +
                            " minutes; will Reset PVM then wait another " + SEQUEST_STALLED_WAIT_TIME_MINUTES + " minutes");

                        resetPVM = true;

                        mSequestAppearsStalled = true;
                    }

                    if (resetPVM)
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

        private bool CopyFileToTransferFolder(string sourceFileName, string targetFileName, bool addToListOfServerFilesToDelete)
        {
            try
            {
                var sourceFilePath = Path.Combine(mWorkDir, sourceFileName);
                var targetFilePath = Path.Combine(mTransferFolderPath, targetFileName);

                if (File.Exists(sourceFilePath))
                {
                    if (!Directory.Exists(mTransferFolderPath))
                    {
                        Directory.CreateDirectory(mTransferFolderPath);
                    }

                    File.Copy(sourceFilePath, targetFilePath, true);

                    if (addToListOfServerFilesToDelete)
                    {
                        mJobParams.AddServerFileToDelete(targetFilePath);
                    }
                }
            }
            catch (Exception ex)
            {
                if (mDebugLevel >= 1)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                        "Error copying file " + sourceFileName + " to " + mTransferFolderPath + ": " + ex.Message);
                }
                return false;
            }

            return true;
        }

        /// <summary>
        /// Finds specified integer value in a sequest.log file
        /// </summary>
        /// <param name="InpFileStr">A string containing the contents of the sequest.log file</param>
        /// <param name="RegexStr">Regular expression match string to uniquely identify the line containing the count of interest</param>
        /// <returns>Count from desired line in sequest.log file if successful; 0 if count not found; -1 for error</returns>
        /// <remarks>If -1 returned, error message is in module variable mErrMsg</remarks>
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

                mErrMsg = "Numeric value not found in the matched text";
                return -1;
            }
            catch (Exception ex)
            {
                mErrMsg = ex.Message;
                return -1;
            }
        }

        private bool GetNodeNamesFromSequestLog(string logFilePath)
        {
            var foundSpawned = false;

            try
            {
                if (!File.Exists(logFilePath))
                {
                    return false;
                }

                if (mDebugLevel >= 3)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, " ... extracting node names from sequest.log");
                }

                // Initialize the RegEx objects
                var reReceivedReadyMsg = new Regex(@"received ready message from (.+)\(", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                var reSpawnedSlaveProcesses = new Regex(@"Spawned (\d+) slave processes", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

                mSequestNodesSpawned = 0;
                using (var reader = new StreamReader(new FileStream(logFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    // Read each line from the input file
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (!string.IsNullOrWhiteSpace(dataLine))
                        {
                            // Check whether line looks like:
                            //    9.  received ready message from p6(c0002)

                            var reMatch = reReceivedReadyMsg.Match(dataLine);
                            if (reMatch.Success)
                            {
                                mSequestNodesSpawned++;
                            }
                            else
                            {
                                reMatch = reSpawnedSlaveProcesses.Match(dataLine);
                                if (reMatch.Success)
                                {
                                    foundSpawned = true;
                                }
                            }
                        }
                    }
                }

                if (foundSpawned)
                {
                    if (mDebugLevel >= 2)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                            " ... found " + mSequestNodesSpawned + " nodes in the sequest.log file");
                    }

                    var nodeCountExpected = mMgrParams.GetParam("SequestNodeCountExpected", 0);
                    var nodeCountMinimum = (int)Math.Floor(0.85 * nodeCountExpected);

                    if (mSequestNodesSpawned < nodeCountMinimum)
                    {
                        // If fewer than nodeCountMinimum .DTA files are present in the work directory, the node count spawned will be small
                        // Thus, need to count the number of DTAs
                        var dtaCountRemaining = GetDTAFileCountRemaining();

                        if (dtaCountRemaining > mSequestNodesSpawned)
                        {
                            mNodeCountSpawnErrorOccurrences++;

                            var message = "Not enough nodes were spawned (Threshold = " + nodeCountMinimum + " nodes): " + mSequestNodesSpawned +
                                             " spawned vs. " + nodeCountExpected + " expected; " +
                                             "mNodeCountSpawnErrorOccurrences=" + mNodeCountSpawnErrorOccurrences;
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, message);

                            mResetPVM = true;
                        }
                    }
                    else if (mNodeCountSpawnErrorOccurrences > 0)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO,
                            "Resetting mNodeCountSpawnErrorOccurrences from " + mNodeCountSpawnErrorOccurrences + " to 0");
                        mNodeCountSpawnErrorOccurrences = 0;
                    }

                    return true;
                }

                if (mDebugLevel >= 1)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                                         " ... Did not find 'Spawned xx slave processes' in the sequest.log file; node names not yet determined");
                }

                if (DateTime.UtcNow.Subtract(mLastSequestStartTime).TotalMinutes > 15)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                                         " ... Over 15 minutes have elapsed since sequest.exe was called; aborting since node names could not be determined");

                    mNodeCountSpawnErrorOccurrences++;
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

        private float ComputeMedianProcessingTime()
        {
            int midPoint;

            if (mRecentOutFileSearchTimes.Count < 1)
                return 0;

            // Determine the median out file processing time
            // Note that search times in mRecentOutFileSearchTimes are in seconds
            var outFileProcessingTimes = new float[mRecentOutFileSearchTimes.Count];

            mRecentOutFileSearchTimes.CopyTo(outFileProcessingTimes, 0);

            Array.Sort(outFileProcessingTimes);
            if (outFileProcessingTimes.Length <= 2)
            {
                midPoint = 0;
            }
            else
            {
                midPoint = (int)Math.Floor(outFileProcessingTimes.Length / 2.0);
            }

            return outFileProcessingTimes[midPoint];
        }

        /// <summary>
        /// Adds newly created .Out file to mOutFileCandidates and mOutFileCandidateInfo
        /// </summary>
        /// <param name="OutFileName"></param>
        private void HandleOutFileChange(string OutFileName)
        {
            try
            {
                if (string.IsNullOrEmpty(OutFileName))
                {
                    if (mDebugLevel >= 3)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "OutFileName is empty; this is unexpected");
                    }
                }
                else
                {
                    if (mDebugLevel >= 5)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Caching new out file: " + OutFileName);
                    }

                    if (!mOutFileCandidateInfo.ContainsKey(OutFileName))
                    {
                        var queueTime = DateTime.UtcNow;
                        var entry = new KeyValuePair<string, DateTime>(OutFileName, queueTime);

                        mOutFileCandidates.Enqueue(entry);
                        mOutFileCandidateInfo.Add(OutFileName, queueTime);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "Error adding new candidate to mOutFileCandidates (" + OutFileName + "): " + ex.Message);
                }
            }
        }

        private bool InitializeUtilityRunner(string taskName, string workDir)
        {
            return InitializeUtilityRunner(taskName, workDir, monitoringIntervalMsec: 1000);
        }

        private bool InitializeUtilityRunner(string taskName, string workDir, int monitoringIntervalMsec)
        {
            try
            {
                if (mUtilityRunner == null)
                {
                    mUtilityRunner = new RunDosProgram(workDir, mDebugLevel);
                    RegisterEvents(mUtilityRunner);
                    mUtilityRunner.Timeout += UtilityRunner_Timeout;
                }
                else
                {
                    if (mUtilityRunner.State != ProgRunner.States.NotMonitoring)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                            "Cannot re-initialize the UtilityRunner to perform task " + taskName + " since already running task " +
                            mUtilityRunnerTaskName);
                        return false;
                    }
                }

                if (monitoringIntervalMsec < 250)
                    monitoringIntervalMsec = 250;
                mUtilityRunner.MonitorInterval = monitoringIntervalMsec;

                mUtilityRunnerTaskName = taskName;
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                    "Exception in InitializeUtilityRunner for task " + taskName + ": " + ex.Message);
                return false;
            }

            return true;
        }

        private bool ProcessCandidateOutFiles(bool processAllRemainingFiles)
        {
            bool appendSuccess;

            var itemsProcessed = 0;

            // Examine mOutFileHandlerInUse; if greater then zero, exit the sub
            if (System.Threading.Interlocked.Read(ref mOutFileHandlerInUse) > 0)
            {
                return false;
            }

            try
            {
                System.Threading.Interlocked.Increment(ref mOutFileHandlerInUse);

                if (mDebugLevel >= 4)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                        "Examining out file creation dates (Candidate Count = " + mOutFileCandidates.Count + ")");
                }

                appendSuccess = true;

                KeyValuePair<string, DateTime> entry;
                if (mOutFileCandidates.Count > 0 && !mSequestVersionInfoStored)
                {
                    // Determine tool version

                    // Pass the path to the first out file created
                    entry = mOutFileCandidates.Peek();
                    if (StoreToolVersionInfo(Path.Combine(mWorkDir, entry.Key)))
                    {
                        mSequestVersionInfoStored = true;
                    }
                }

                if (string.IsNullOrEmpty(mTempConcatenatedOutFilePath))
                {
                    mTempConcatenatedOutFilePath = Path.Combine(mWorkDir, mDatasetName + "_out.txt.tmp");
                }

                if (mOutFileCandidates.Count > 0)
                {
                    // Examine the time associated with the next item that would be dequeued
                    entry = mOutFileCandidates.Peek();

                    if (processAllRemainingFiles || DateTime.UtcNow.Subtract(entry.Value).TotalSeconds >= OUT_FILE_APPEND_HOLDOFF_SECONDS)
                    {
                        // Open the _out.txt.tmp file
                        using var writer = new StreamWriter(new FileStream(mTempConcatenatedOutFilePath, FileMode.Append, FileAccess.Write, FileShare.Read));

                        itemsProcessed = 0;

                        while (mOutFileCandidates.Count > 0 && appendSuccess &&
                               (processAllRemainingFiles ||
                                DateTime.UtcNow.Subtract(entry.Value).TotalSeconds >= OUT_FILE_APPEND_HOLDOFF_SECONDS))
                        {
                            // Entry is old enough (or processAllRemainingFiles=True); pop it off the queue
                            entry = mOutFileCandidates.Dequeue();
                            itemsProcessed++;

                            try
                            {
                                var outFile = new FileInfo(Path.Combine(mWorkDir, entry.Key));
                                AppendOutFile(outFile, writer);
                                mLastOutFileStoreTime = DateTime.UtcNow;
                                mSequestAppearsStalled = false;
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Warning, exception appending out file: " + ex.Message);
                                appendSuccess = false;
                            }

                            if (mOutFileCandidates.Count > 0)
                            {
                                entry = mOutFileCandidates.Peek();
                            }
                        }
                    }
                }

                if (itemsProcessed > 0 || processAllRemainingFiles)
                {
                    if (mDebugLevel >= 3)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                            "Appended " + itemsProcessed + " .out file" + CheckForPlurality(itemsProcessed) + " to the _out.txt.tmp file; " +
                            mOutFileCandidates.Count + " out file" + CheckForPlurality(mOutFileCandidates.Count) + " remain in the queue");
                    }

                    if (processAllRemainingFiles || DateTime.UtcNow.Subtract(mLastTempFileCopyTime).TotalSeconds >= TEMP_FILE_COPY_INTERVAL_SECONDS)
                    {
                        string sourceFileName;
                        bool success;
                        if (!mTempJobParamsCopied)
                        {
                            sourceFileName = "JobParameters_" + mJob + ".xml";
                            success = CopyFileToTransferFolder(sourceFileName, sourceFileName + ".tmp", true);

                            if (success)
                            {
                                sourceFileName = mJobParams.GetParam("ParmFileName");
                                success = CopyFileToTransferFolder(sourceFileName, sourceFileName + ".tmp", true);
                            }

                            if (success)
                            {
                                mTempJobParamsCopied = true;
                            }
                        }

                        if (itemsProcessed > 0)
                        {
                            // Copy the _out.txt.tmp file
                            sourceFileName = Path.GetFileName(mTempConcatenatedOutFilePath);
                            success = CopyFileToTransferFolder(sourceFileName, sourceFileName, true);
                        }

                        // Copy the sequest.log file (rename to sequest.log.tmp when copying)
                        sourceFileName = "sequest.log";
                        success = CopyFileToTransferFolder(sourceFileName, sourceFileName + ".tmp", true);

                        mLastTempFileCopyTime = DateTime.UtcNow;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Warning, error in ProcessCandidateOutFiles: " + ex.Message);
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Error in ProcessCandidateOutFiles: " + ex.Message);
                appendSuccess = false;
            }
            finally
            {
                // Make sure mOutFileHandlerInUse is now zero
                const long zero = 0;
                System.Threading.Interlocked.Exchange(ref mOutFileHandlerInUse, zero);
            }

            return appendSuccess;
        }

        private void RenameSequestLogFile()
        {
            var newName = "??";

            try
            {
                var sequestLogFile = new FileInfo(Path.Combine(mWorkDir, "sequest.log"));

                if (sequestLogFile.Exists)
                {
                    newName = Path.GetFileNameWithoutExtension(sequestLogFile.Name) + "_" + sequestLogFile.LastWriteTime.ToString("yyyyMMdd_HHmm") + ".log";
                    sequestLogFile.MoveTo(Path.Combine(mWorkDir, newName));

                    // Copy the renamed sequest.log file to the transfer directory
                    CopyFileToTransferFolder(newName, newName, false);
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                    "Error renaming sequest.log file to " + newName + ": " + ex.Message);
            }
        }

        private bool ResetPVMWithRetry(int maxPVMResetAttempts)
        {
            var success = false;

            if (maxPVMResetAttempts < 1)
                maxPVMResetAttempts = 1;

            while (maxPVMResetAttempts > 0)
            {
                success = ResetPVM();
                if (success)
                {
                    break;
                }

                maxPVMResetAttempts--;
                if (maxPVMResetAttempts > 0)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                                         " ... Error resetting PVM; will try " + maxPVMResetAttempts + " more time" + CheckForPlurality(maxPVMResetAttempts));
                }
            }

            if (success)
            {
                UpdateSequestNodeProcessingStats(false);
                RenameSequestLogFile();
            }

            return success;
        }

        private bool ResetPVM()
        {
            try
            {
                // Folder with PVM
                var PVMProgFolder = mMgrParams.GetParam("PVMProgLoc");
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

                var success = ResetPVMHalt(PVMProgFolder);
                if (!success)
                {
                    return false;
                }

                success = ResetPVMWipeTemp(PVMProgFolder);
                if (!success)
                {
                    return false;
                }

                success = ResetPVMStartPVM(PVMProgFolder);
                if (!success)
                {
                    return false;
                }

                success = ResetPVMAddNodes(PVMProgFolder);
                if (!success)
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
                var batchFilePath = Path.Combine(PVMProgFolder, "HaltPVM.bat");
                if (!File.Exists(batchFilePath))
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Batch file not found: " + batchFilePath);
                    return false;
                }

                // Run the batch file
                if (mDebugLevel >= 2)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "     " + batchFilePath);
                }

                const string taskName = "HaltPVM";
                if (!InitializeUtilityRunner(taskName, PVMProgFolder))
                {
                    return false;
                }

                const int maxRuntimeSeconds = 90;
                var success = mUtilityRunner.RunProgram(batchFilePath, "", taskName, false, maxRuntimeSeconds);

                if (!success)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "UtilityRunner returned False for " + batchFilePath);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception in ResetPVMHalt: " + ex.Message);
                return false;
            }

            // Wait 5 seconds
            Global.IdleLoop(5);

            return true;
        }

        private bool ResetPVMWipeTemp(string PVMProgFolder)
        {
            try
            {
                var batchFilePath = Path.Combine(PVMProgFolder, "wipe_temp.bat");
                if (!File.Exists(batchFilePath))
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Batch file not found: " + batchFilePath);
                    return false;
                }

                // Run the batch file
                if (mDebugLevel >= 2)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "     " + batchFilePath);
                }

                const string taskName = "WipeTemp";
                if (!InitializeUtilityRunner(taskName, PVMProgFolder))
                {
                    return false;
                }

                const int maxRuntimeSeconds = 120;
                var success = mUtilityRunner.RunProgram(batchFilePath, "", taskName, true, maxRuntimeSeconds);

                if (!success)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "UtilityRunner returned False for " + batchFilePath);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception in ResetPVMWipeTemp: " + ex.Message);
                return false;
            }

            // Wait 5 seconds
            Global.IdleLoop(5);

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

                var batchFilePath = Path.Combine(PVMProgFolder, "StartPVM.bat");
                if (!File.Exists(batchFilePath))
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Batch file not found: " + batchFilePath);
                    return false;
                }

                // Run the batch file
                if (mDebugLevel >= 2)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "     " + batchFilePath);
                }

                const string taskName = "StartPVM";
                if (!InitializeUtilityRunner(taskName, PVMProgFolder))
                {
                    return false;
                }

                const int maxRuntimeSeconds = 120;
                var success = mUtilityRunner.RunProgram(batchFilePath, "", taskName, true, maxRuntimeSeconds);

                if (!success)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "UtilityRunner returned False for " + batchFilePath);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception in ResetPVMStartPVM: " + ex.Message);
                return false;
            }

            // Wait 5 seconds
            Global.IdleLoop(5);

            return true;
        }

        private bool ResetPVMAddNodes(string PVMProgFolder)
        {
            try
            {
                var batchFilePath = Path.Combine(PVMProgFolder, "AddHosts.bat");
                if (!File.Exists(batchFilePath))
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Batch file not found: " + batchFilePath);
                    return false;
                }

                // Run the batch file
                if (mDebugLevel >= 2)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "     " + batchFilePath);
                }

                const string taskName = "AddHosts";
                if (!InitializeUtilityRunner(taskName, PVMProgFolder))
                {
                    return false;
                }

                const int maxRuntimeSeconds = 120;
                var success = mUtilityRunner.RunProgram(batchFilePath, "", taskName, true, maxRuntimeSeconds);

                if (!success)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "UtilityRunner returned False for " + batchFilePath);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Exception in ResetPVMAddNodes: " + ex.Message);
                return false;
            }

            // Wait 5 seconds
            Global.IdleLoop(5);

            return true;
        }

        private void UpdateSequestNodeProcessingStats(bool processAllSequestLogFiles)
        {
            if (processAllSequestLogFiles)
            {
                var workDir = new DirectoryInfo(mWorkDir);

                foreach (var sequestLogFile in workDir.GetFiles("sequest*.log*"))
                {
                    UpdateSequestNodeProcessingStatsOneFile(sequestLogFile.FullName);
                }
            }
            else
            {
                UpdateSequestNodeProcessingStatsOneFile(Path.Combine(mWorkDir, "sequest.log"));
            }
        }

        private void UpdateSequestNodeProcessingStatsOneFile(string SeqLogFilePath)
        {
            // Verify sequest.log file exists
            if (!File.Exists(SeqLogFilePath))
            {
                if (mDebugLevel >= 2)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                        "Sequest log file not found, cannot update Node Processing Stats");
                    return;
                }
            }

            // Read the sequest.log file
            var sbContents = new StringBuilder();
            var dtaCountSearched = 0;

            try
            {
                using var reader = new StreamReader(new FileStream(SeqLogFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (dataLine != null && dataLine.StartsWith("Searched dta file"))
                    {
                        dtaCountSearched++;
                    }

                    sbContents.AppendLine(dataLine);
                }
            }
            catch (Exception ex)
            {
                var Msg = "UpdateNodeStats: Exception reading sequest log file: " + ex.Message;
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, Msg);
                return;
            }

            var fileContents = sbContents.ToString();

            // Node machine count
            var NumNodeMachines = GetIntegerFromSeqLogFileString(fileContents, "starting the sequest task on\\s+\\d+\\s+node");
            if (NumNodeMachines == 0)
            {
                const string Msg = "UpdateNodeStats: node machine count line not found";
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, Msg);
            }
            else if (NumNodeMachines < 0)
            {
                var Msg = "UpdateNodeStats: Exception retrieving node machine count: " + mErrMsg;
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, Msg);
            }

            if (NumNodeMachines > mSequestNodeProcessingStats.NumNodeMachines)
            {
                mSequestNodeProcessingStats.NumNodeMachines = NumNodeMachines;
            }

            // Sequest process count
            var NumSlaveProcesses = GetIntegerFromSeqLogFileString(fileContents, "Spawned\\s+\\d+\\s+slave processes");
            if (NumSlaveProcesses == 0)
            {
                const string Msg = "UpdateNodeStats: slave process count line not found";
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, Msg);
            }
            else if (NumSlaveProcesses < 0)
            {
                var Msg = "UpdateNodeStats: Exception retrieving slave process count: " + mErrMsg;
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, Msg);
            }

            if (NumSlaveProcesses > mSequestNodeProcessingStats.NumSlaveProcesses)
            {
                mSequestNodeProcessingStats.NumSlaveProcesses = NumSlaveProcesses;
            }

            // Total search time
            double TotalSearchTimeSeconds = GetIntegerFromSeqLogFileString(fileContents, "Total search time:\\s+\\d+");
            if (TotalSearchTimeSeconds <= 0)
            {
                // Total search time line not found (or error)
                // Use internal tracking variables instead
                TotalSearchTimeSeconds = mSequestSearchEndTime.Subtract(mSequestSearchStartTime).TotalSeconds;
            }

            mSequestNodeProcessingStats.TotalSearchTimeSeconds += TotalSearchTimeSeconds;

            // Searched file count
            var SearchedFileCount = GetIntegerFromSeqLogFileString(fileContents, "secs for\\s+\\d+\\s+files");
            if (SearchedFileCount <= 0)
            {
                // Searched file count line not found (or error)
                // Use dtaCountSearched instead
                SearchedFileCount = dtaCountSearched;
            }

            mSequestNodeProcessingStats.SearchedFileCount += dtaCountSearched;

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
        private void ValidateProcessorsAreActive()
        {
            try
            {
                if (!mSequestLogNodesFound)
                {
                    // Parse the Sequest.Log file to determine the names of the spawned nodes

                    var logFilePath = Path.Combine(mWorkDir, "sequest.log");

                    mSequestLogNodesFound = GetNodeNamesFromSequestLog(logFilePath);

                    if (!mSequestLogNodesFound || mResetPVM)
                    {
                        return;
                    }
                }

                // Determine the number of Active Nodes using PVM
                var PVMProgFolder = mMgrParams.GetParam("PVMProgLoc");
                if (string.IsNullOrWhiteSpace(PVMProgFolder))
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "PVMProgLoc parameter not defined for this manager");
                    return;
                }

                var batchFilePath = Path.Combine(PVMProgFolder, "CheckActiveNodes.bat");
                if (!File.Exists(batchFilePath))
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, "Batch file not found: " + batchFilePath);
                    return;
                }

                var activeNodesFilePath = Path.Combine(mWorkDir, "ActiveNodesOutput.tmp");

                if (mDebugLevel >= 4)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "     " + batchFilePath);
                }

                const string taskName = "CheckActiveNodes";
                if (!InitializeUtilityRunner(taskName, PVMProgFolder))
                {
                    return;
                }

                const int maxRuntimeSeconds = 60;
                var success = mUtilityRunner.RunProgram(batchFilePath, "", taskName, true, maxRuntimeSeconds);

                if (!success)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                        "UtilityRunner returned False for " + batchFilePath);
                }

                if (!File.Exists(activeNodesFilePath))
                {
                    if (mDebugLevel >= 1)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                            "Warning, ActiveNodes files not found: " + activeNodesFilePath);
                    }

                    return;
                }

                // Parse the ActiveNodesOutput.tmp file
                var nodeCountCurrent = 0;
                using (var reader = new StreamReader(new FileStream(activeNodesFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        // Check whether line looks like:
                        //    p6    c0007     6/c,f sequest27_slave

                        var reMatch = mActiveNodeRegEx.Match(dataLine);
                        if (reMatch.Success)
                        {
                            var nodeName = reMatch.Groups["node"].Value;

                            if (mSequestNodes.TryGetValue(nodeName, out _))
                            {
                                mSequestNodes[nodeName] = DateTime.UtcNow;
                            }
                            else
                            {
                                mSequestNodes.Add(nodeName, DateTime.UtcNow);
                            }

                            nodeCountCurrent++;
                        }
                    }
                }

                // Log the number of active nodes every 10 minutes
                if (mDebugLevel >= 4 || DateTime.UtcNow.Subtract(mLastActiveNodeLogTime).TotalSeconds >= 600)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                        " ... " + nodeCountCurrent + " / " + mSequestNodesSpawned + " Sequest nodes are active; median processing time = " +
                        ComputeMedianProcessingTime().ToString("0.0") + " seconds/spectrum; " + mProgress.ToString("0.0") + "% complete");
                    mLastActiveNodeLogTime = DateTime.UtcNow;
                }

                // Look for nodes that have been missing for at least 5 minutes
                var nodeCountActive = 0;
                foreach (var sequestNode in mSequestNodes)
                {
                    if (DateTime.UtcNow.Subtract(sequestNode.Value).TotalMinutes <= STALE_NODE_THRESHOLD_MINUTES)
                    {
                        nodeCountActive++;
                    }
                }

                // Define the minimum node count as 50% of the number of nodes spawned
                var activeNodeCountMinimum = (int)Math.Floor(0.5 * mSequestNodesSpawned);

                if (nodeCountActive < activeNodeCountMinimum && !mIgnoreNodeCountActiveErrors)
                {
                    mNodeCountActiveErrorOccurrences++;
                    var message = "Too many nodes are inactive (Threshold = " + activeNodeCountMinimum + " nodes): " + nodeCountActive +
                                  " active vs. " + mSequestNodesSpawned + " total nodes at start; " +
                                  "mNodeCountActiveErrorOccurrences=" + mNodeCountActiveErrorOccurrences;

                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, message);
                    mResetPVM = true;
                }
                else if (mNodeCountActiveErrorOccurrences > 0)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.INFO,
                        "Resetting mNodeCountActiveErrorOccurrences from " + mNodeCountActiveErrorOccurrences + " to 0");
                    mNodeCountActiveErrorOccurrences = 0;
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                    "Exception in ValidateProcessorsAreActive: " + ex.Message);
            }
        }

        private void OutFileWatcher_Created(object sender, FileSystemEventArgs e)
        {
            HandleOutFileChange(e.Name);
        }

        private void OutFileWatcher_Changed(object sender, FileSystemEventArgs e)
        {
            HandleOutFileChange(e.Name);
        }

        private void OutFileAppenderTime_Elapsed(object sender, ElapsedEventArgs e)
        {
            ProcessCandidateOutFiles(false);
            CheckForStalledSequest();
        }

        private void UtilityRunner_Timeout()
        {
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                "UtilityRunner task " + mUtilityRunnerTaskName + " has timed out; " + mUtilityRunner.MaxRuntimeSeconds + " seconds has elapsed");
        }

        #endregion
    }
}
