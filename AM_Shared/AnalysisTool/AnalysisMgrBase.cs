using System;
using System.IO;
using AnalysisManagerBase.StatusReporting;
using PRISM;
using PRISM.Logging;

namespace AnalysisManagerBase.AnalysisTool
{
    /// <summary>
    /// Analysis manager base class
    /// </summary>
    public abstract class AnalysisMgrBase : LoggerBase
    {
        private DateTime mLastLockQueueWaitTimeLog = DateTime.UtcNow;

        private DateTime mLockQueueWaitTimeStart = DateTime.UtcNow;

        /// <summary>
        /// File tools
        /// </summary>
        protected FileTools mFileTools;

        /// <summary>
        /// Will be set to true if the job cannot be run due to not enough free memory
        /// </summary>
        protected bool mInsufficientFreeMemory;

        /// <summary>
        /// Status message
        /// </summary>
        /// <remarks>Text here will be stored in the Completion_Message column in the database when the job is closed</remarks>
        protected string mMessage;

        /// <summary>
        /// Will be set to true if we need to abort processing as soon as possible
        /// </summary>
        protected bool mNeedToAbortProcessing;

        private readonly string mDerivedClassName;

        /// <summary>
        /// status tools
        /// </summary>
        protected IStatusFile mStatusTools;

        /// <summary>
        /// When true, show additional messages at the console
        /// </summary>
        /// <remarks>
        /// This property is updated when the Setup method is called in AnalysisResources or AnalysisToolRunnerBase
        /// </remarks>
        public bool TraceMode { get; protected set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="derivedClassName"></param>
        protected AnalysisMgrBase(string derivedClassName)
        {
            mDerivedClassName = derivedClassName;
        }

        private bool IsLockQueueLogMessageNeeded(ref DateTime lockQueueWaitTimeStart, ref DateTime lastLockQueueWaitTimeLog)
        {
            int waitTimeLogIntervalSeconds;

            if (lockQueueWaitTimeStart == DateTime.MinValue)
                lockQueueWaitTimeStart = DateTime.UtcNow;

            var waitTimeMinutes = DateTime.UtcNow.Subtract(lockQueueWaitTimeStart).TotalMinutes;

            if (waitTimeMinutes >= 30)
            {
                waitTimeLogIntervalSeconds = 240;
            }
            else if (waitTimeMinutes >= 15)
            {
                waitTimeLogIntervalSeconds = 120;
            }
            else if (waitTimeMinutes >= 5)
            {
                waitTimeLogIntervalSeconds = 60;
            }
            else
            {
                waitTimeLogIntervalSeconds = 30;
            }

            return DateTime.UtcNow.Subtract(lastLockQueueWaitTimeLog).TotalSeconds >= waitTimeLogIntervalSeconds;
        }

        /// <summary>
        /// Initialize mFileTools
        /// </summary>
        /// <param name="mgrName">Manager name</param>
        /// <param name="debugLevel">Debug Level for logging; 1=minimal logging; 5=detailed logging</param>
        protected void InitFileTools(string mgrName, short debugLevel)
        {
            ResetTimestampForQueueWaitTimeLogging();
            mFileTools = new FileTools(mgrName, debugLevel);
            RegisterEvents(mFileTools, false);

            // Use a custom event handler for status messages
            UnregisterEventHandler(mFileTools, BaseLogger.LogLevels.INFO);
            mFileTools.StatusEvent += FileTools_StatusEvent;

            mFileTools.LockQueueTimedOut += FileTools_LockQueueTimedOut;
            mFileTools.LockQueueWaitComplete += FileTools_LockQueueWaitComplete;
            mFileTools.WaitingForLockQueue += FileTools_WaitingForLockQueue;
            mFileTools.WaitingForLockQueueNotifyLockFilePaths += FileTools_WaitingForLockQueueNotifyLockFilePaths;
        }

        /// <summary>
        /// Log stats related to copying a file
        /// </summary>
        /// <remarks>Only writes to the log if the copy time exceeds logThresholdSeconds</remarks>
        /// <param name="startTimeUtc">Time the copy started (or the time that CopyFileUsingLocks was called)</param>
        /// <param name="destinationFilePath">Destination file path (used to determine the file size)</param>
        /// <param name="logThresholdSeconds">Threshold for logging the file copy details</param>
        protected void LogCopyStats(DateTime startTimeUtc, string destinationFilePath, int logThresholdSeconds = 10)
        {
            var elapsedSeconds = DateTime.UtcNow.Subtract(startTimeUtc).TotalSeconds;
            if (elapsedSeconds < logThresholdSeconds)
                return;

            var destinationFile = new FileInfo(destinationFilePath);
            if (destinationFile.Exists)
            {
                var fileSizeMB = Global.BytesToMB(destinationFile.Length);
                var copyRateMBPerSec = fileSizeMB / elapsedSeconds;

                // Note that mFileTools may have been waiting for a lock file queue to subside,
                // in which case the copyRate doesn't represent the actual connection speed between the two computers
                LogDebug(string.Format("  Copied {0:N0} MB file in {1:N0} seconds, transferring at {2:N0} MB/sec: {3}",
                    fileSizeMB, elapsedSeconds, copyRateMBPerSec, destinationFile.Name));
            }
            else
            {
                LogError("Cannot log copy time since target file not found: " + destinationFilePath);
            }
        }

        /// <summary>
        /// Log an error to the database and the local log file
        /// </summary>
        /// <remarks>Does not update mMessage</remarks>
        /// <param name="errorMessage">Error message</param>
        protected void LogErrorToDatabase(string errorMessage)
        {
            base.LogError(errorMessage, logToDb: true);
        }

        /// <summary>
        /// Update mMessage with an error message and record the error in the manager's log file, plus optionally in the database
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        public override void LogError(string errorMessage, bool logToDb = false)
        {
            mMessage = Global.AppendToComment(mMessage, errorMessage);
            base.LogError(errorMessage, logToDb);
        }

        /// <summary>
        /// Log an error message, but do not update mMessage
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        protected void LogErrorNoMessageUpdate(string errorMessage, bool logToDb = false)
        {
            base.LogError(errorMessage, logToDb);
        }

        /// <summary>
        /// Update mMessage with an error message and record the error in the manager's log file
        /// </summary>
        /// <param name="errorMessage">Error message (do not include ex.message, unless you want that message to appear in the job comment)</param>
        /// <param name="ex">Exception to log</param>
        /// <param name="logToDatabase">When true, log to the database (and to the file)</param>
        protected override void LogError(string errorMessage, Exception ex, bool logToDatabase = false)
        {
            mMessage = Global.AppendToComment(mMessage, errorMessage);
            base.LogError(errorMessage, ex, logToDatabase);
        }

        /// <summary>
        /// Update mMessage with an error message and record the error in the manager's log file
        /// Also write the detailed error message to the local log file
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="detailedMessage">Detailed error message</param>
        /// <param name="ex">Exception</param>
        protected void LogError(string errorMessage, string detailedMessage, Exception ex)
        {
            LogError(errorMessage, ex);

            if (string.IsNullOrEmpty(detailedMessage))
                return;

            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine(detailedMessage);
            Console.ResetColor();

            try
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, detailedMessage);
            }
            catch (Exception logException)
            {
                Global.ErrorWritingToLog(errorMessage, logException);
            }
        }

        /// <summary>
        /// Update mMessage with an error message and record the error in the manager's log file
        /// Also write the detailed error message to the local log file
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="detailedMessage">Detailed error message</param>
        /// <param name="logToDb">True to log this error to the database</param>
        protected void LogError(string errorMessage, string detailedMessage, bool logToDb = false)
        {
            LogError(errorMessage, logToDb);

            if (string.IsNullOrEmpty(detailedMessage))
                return;

            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine(detailedMessage);
            Console.ResetColor();

            try
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, detailedMessage);
            }
            catch (Exception ex)
            {
                Global.ErrorWritingToLog(errorMessage, ex);
            }
        }

        /// <summary>
        /// Reset the timestamp for logging that we are waiting for a lock file queue to decrease
        /// </summary>
        protected void ResetTimestampForQueueWaitTimeLogging()
        {
            mLastLockQueueWaitTimeLog = DateTime.UtcNow;
            mLockQueueWaitTimeStart = DateTime.UtcNow;
        }

        private void FileTools_LockQueueTimedOut(string sourceFilePath, string targetFilePath, double waitTimeMinutes)
        {
            if (mDebugLevel >= 1)
            {
                var msg = "Lock file queue timed out after " + waitTimeMinutes.ToString("0") + " minutes (" + mDerivedClassName + "); Source=" + sourceFilePath + ", Target=" + targetFilePath;
                LogWarning(msg);
            }
        }

        private void FileTools_LockQueueWaitComplete(string sourceFilePath, string targetFilePath, double waitTimeMinutes)
        {
            if (mDebugLevel >= 1 && waitTimeMinutes >= 1)
            {
                // Round to the nearest minute
                var minutesText = waitTimeMinutes.ToString("0");
                var timeUnits = minutesText == "1" ? "minute" : "minutes";

                var msg = string.Format("Exited lock file queue after {0} {1} ({2}); will now copy file", minutesText, timeUnits, mDerivedClassName);
                LogDebug(msg);
            }
        }

        private void FileTools_StatusEvent(string message)
        {
            // Do not log certain common messages
            if (message.StartsWith("Created lock file") ||
                message.StartsWith("Copying file with CopyFileEx") ||
                message.StartsWith("File to copy is") && message.Contains("will use CopyFileEx for"))
            {
                if (TraceMode)
                    ConsoleMsgUtils.ShowDebugCustom(message, emptyLinesBeforeMessage: 0);

                return;
            }

            LogMessage(message);
        }

        private void FileTools_WaitingForLockQueue(string sourceFilePath, string targetFilePath, int backlogSourceMB, int backlogTargetMB)
        {
            if (!IsLockQueueLogMessageNeeded(ref mLockQueueWaitTimeStart, ref mLastLockQueueWaitTimeLog))
                return;

            mLastLockQueueWaitTimeLog = DateTime.UtcNow;
            if (mDebugLevel >= 1)
            {
                LogDebug(string.Format(
                    "Waiting for lock file queue to fall below threshold ({0}); " +
                    "SourceBacklog={1:N0} MB, TargetBacklog={2:N0} MB, " +
                    "Source={3}, Target={4}",
                    mDerivedClassName, backlogSourceMB, backlogTargetMB, sourceFilePath, targetFilePath));
            }
        }

        private void FileTools_WaitingForLockQueueNotifyLockFilePaths(string sourceLockFilePath, string targetLockFilePath, string adminBypassMessage)
        {
            if (string.IsNullOrWhiteSpace(adminBypassMessage))
            {
                LogMessage(string.Format("Waiting for lock file queue to fall below threshold; see lock file(s) at {0} and {1}",
                                         sourceLockFilePath ?? "(n/a)", targetLockFilePath ?? "(n/a)"));
                return;
            }

            LogMessage(adminBypassMessage);
        }

        /// <summary>
        /// Register event handlers
        /// </summary>
        /// <param name="processingClass"></param>
        /// <param name="writeDebugEventsToLog"></param>
        protected override void RegisterEvents(IEventNotifier processingClass, bool writeDebugEventsToLog = true)
        {
            base.RegisterEvents(processingClass, writeDebugEventsToLog);

            // Note that ProgressUpdateHandler does not display a message at console
            // Instead, it calls mStatusTools.UpdateAndWrite, which updates the status file
            processingClass.ProgressUpdate += ProgressUpdateHandler;
        }

        /// <summary>
        /// Update progress and re-write the analysis status file
        /// </summary>
        /// <remarks>This does not display a message at console (intentionally)</remarks>
        /// <param name="progressMessage"></param>
        /// <param name="percentComplete"></param>
        protected void ProgressUpdateHandler(string progressMessage, float percentComplete)
        {
            mStatusTools.CurrentOperation = progressMessage;
            mStatusTools.UpdateAndWrite(percentComplete);
        }
    }
}
