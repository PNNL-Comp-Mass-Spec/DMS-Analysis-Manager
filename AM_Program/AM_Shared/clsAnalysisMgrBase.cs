
using System;
using System.IO;
using PRISM.Logging;
using PRISM;

namespace AnalysisManagerBase
{

    /// <summary>
    /// Analysis manager base class
    /// </summary>
    public abstract class clsAnalysisMgrBase : clsLoggerBase
    {
        #region "Module variables"

        private DateTime m_LastLockQueueWaitTimeLog = DateTime.UtcNow;

        private DateTime m_LockQueueWaitTimeStart = DateTime.UtcNow;

        /// <summary>
        /// File tools
        /// </summary>
        protected clsFileTools m_FileTools;

        /// <summary>
        /// Status message
        /// </summary>
        /// <remarks>Text here will be stored in the Completion_Message column in the database when the job is closed</remarks>
        protected string m_message;

        private readonly string m_DerivedClassName;

        /// <summary>
        /// status tools
        /// </summary>
        protected IStatusFile m_StatusTools;

        #endregion

        #region "Properties"

        /// <summary>
        /// When true, show additional messages at the console
        /// </summary>
        /// <remarks>
        /// This property is updated when the Setup method is called in clsAnalysisResources or clsAnalysisToolRunnerBase
        /// </remarks>
        protected bool TraceMode { get; set; }

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="derivedClassName"></param>
        protected clsAnalysisMgrBase(string derivedClassName)
        {
            m_DerivedClassName = derivedClassName;
        }

        private bool IsLockQueueLogMessageNeeded(ref DateTime dtLockQueueWaitTimeStart, ref DateTime dtLastLockQueueWaitTimeLog)
        {

            int waitTimeLogIntervalSeconds;

            if (dtLockQueueWaitTimeStart == DateTime.MinValue)
                dtLockQueueWaitTimeStart = DateTime.UtcNow;

            var waitTimeMinutes = DateTime.UtcNow.Subtract(dtLockQueueWaitTimeStart).TotalMinutes;

            if (waitTimeMinutes >= 30)
            {
                waitTimeLogIntervalSeconds = 240;
            } else if (waitTimeMinutes >= 15)
            {
                waitTimeLogIntervalSeconds = 120;
            } else if (waitTimeMinutes >= 5)
            {
                waitTimeLogIntervalSeconds = 60;
            }
            else
            {
                waitTimeLogIntervalSeconds = 30;
            }

            if (DateTime.UtcNow.Subtract(dtLastLockQueueWaitTimeLog).TotalSeconds >= waitTimeLogIntervalSeconds)
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Initialize m_FileTools
        /// </summary>
        /// <param name="mgrName"></param>
        /// <param name="debugLevel"></param>
        protected void InitFileTools(string mgrName, short debugLevel)
        {
            ResetTimestampForQueueWaitTimeLogging();
            m_FileTools = new clsFileTools(mgrName, debugLevel);
            RegisterEvents(m_FileTools, false);

            // Use a custom event handler for status messages
            UnregisterEventHandler(m_FileTools, BaseLogger.LogLevels.INFO);
            m_FileTools.StatusEvent += m_FileTools_StatusEvent;

            m_FileTools.LockQueueTimedOut += m_FileTools_LockQueueTimedOut;
            m_FileTools.LockQueueWaitComplete += m_FileTools_LockQueueWaitComplete;
            m_FileTools.WaitingForLockQueue += m_FileTools_WaitingForLockQueue;
        }

        /// <summary>
        /// Log stats related to copying a file
        /// </summary>
        /// <param name="startTimeUtc">Time the copy started (or the time that CopyFileUsingLocks was called)</param>
        /// <param name="destFilePath">Destination file path (used to determine the file size)</param>
        /// <param name="logThresholdSeconds">Threshold for logging the file copy details</param>
        /// <remarks>Only writes to the log if the copy time exceeds logThresholdSeconds</remarks>
        protected void LogCopyStats(DateTime startTimeUtc, string destFilePath, int logThresholdSeconds = 10)
        {
            var elapsedSeconds = DateTime.UtcNow.Subtract(startTimeUtc).TotalSeconds;
            if (elapsedSeconds < logThresholdSeconds)
                return;

            var destFile = new FileInfo(destFilePath);
            if (destFile.Exists)
            {
                var fileSizeMB = clsGlobal.BytesToMB(destFile.Length);
                var copyRateMBPerSec = fileSizeMB / elapsedSeconds;

                // Note that m_FileTools may have been waiting for a lock file queue to subside,
                // in which case the copyRate doesn't represent the actual connection speed between the two computers
                LogDebug(string.Format("  Retrieved {0:N0} MB file in {1:N0} seconds, copying at {2:N0} MB/sec: {3}",
                    fileSizeMB, elapsedSeconds, copyRateMBPerSec, destFile.Name));
            }
            else
            {
                LogError("Cannot log copy time since target file not found: " + destFilePath);
            }
        }

        /// <summary>
        /// Log an error to the database and the local log file
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <remarks>Does not update m_message</remarks>
        protected void LogErrorToDatabase(string errorMessage)
        {
            base.LogError(errorMessage, logToDb: true);
        }

        /// <summary>
        /// Update m_message with an error message and record the error in the manager's log file, plus optionally in the database
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        protected override void LogError(string errorMessage, bool logToDb = false)
        {
            m_message = clsGlobal.AppendToComment(m_message, errorMessage);
            base.LogError(errorMessage, logToDb);
        }

        /// <summary>
        /// Log an error message, but do not update m_message
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        protected void LogErrorNoMessageUpdate(string errorMessage, bool logToDb = false)
        {
            base.LogError(errorMessage, logToDb);
        }

        /// <summary>
        /// Update m_message with an error message and record the error in the manager's log file
        /// </summary>
        /// <param name="errorMessage">Error message (do not include ex.message, unless you want that message to appear in the job comment)</param>
        /// <param name="ex">Exception to log</param>
        /// <param name="logToDatabase">When true, log to the database (and to the file)</param>
        protected override void LogError(string errorMessage, Exception ex, bool logToDatabase = false)
        {
            m_message = clsGlobal.AppendToComment(m_message, errorMessage);
            base.LogError(errorMessage, ex, logToDatabase);
        }

        /// <summary>
        /// Update m_message with an error message and record the error in the manager's log file
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
                clsGlobal.ErrorWritingToLog(errorMessage, logException);
            }
        }

        /// <summary>
        /// Update m_message with an error message and record the error in the manager's log file
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
                clsGlobal.ErrorWritingToLog(errorMessage, ex);
            }
        }

        /// <summary>
        /// Reset the timestamp for logging that we are waiting for a lock file queue to decrease
        /// </summary>
        protected void ResetTimestampForQueueWaitTimeLogging()
        {
            m_LastLockQueueWaitTimeLog = DateTime.UtcNow;
            m_LockQueueWaitTimeStart = DateTime.UtcNow;
        }

        #region "Event Handlers"

        private void m_FileTools_LockQueueTimedOut(string sourceFilePath, string targetFilePath, double waitTimeMinutes)
        {
            if (m_DebugLevel >= 1)
            {
                var msg = "Lockfile queue timed out after " + waitTimeMinutes.ToString("0") + " minutes " + "(" + m_DerivedClassName + "); Source=" + sourceFilePath + ", Target=" + targetFilePath;
                LogWarning(msg);
            }
        }

        private void m_FileTools_LockQueueWaitComplete(string sourceFilePath, string targetFilePath, double waitTimeMinutes)
        {
            if (m_DebugLevel >= 1 && waitTimeMinutes >= 1)
            {
                var msg = "Exited lockfile queue after " + waitTimeMinutes.ToString("0") + " minutes (" + m_DerivedClassName + "); will now copy file";
                LogDebug(msg);
            }
        }

        private void m_FileTools_StatusEvent(string message)
        {
            // Do not log certain common messages
            if (message.StartsWith("Created lock file") ||
                message.StartsWith("Copying file with CopyFileEx") ||
                message.StartsWith("File to copy is") && message.Contains("will use CopyFileEx for"))
            {
                if (TraceMode)
                    ConsoleMsgUtils.ShowDebug(message);

                return;
            }

            LogMessage(message);
        }

        private void m_FileTools_WaitingForLockQueue(string sourceFilePath, string targetFilePath, int backlogSourceMB, int backlogTargetMB)
        {
            if (IsLockQueueLogMessageNeeded(ref m_LockQueueWaitTimeStart, ref m_LastLockQueueWaitTimeLog))
            {
                m_LastLockQueueWaitTimeLog = DateTime.UtcNow;
                if (m_DebugLevel >= 1)
                {
                    var msg = "Waiting for lockfile queue to fall below threshold (" + m_DerivedClassName + "); " +
                        "SourceBacklog=" + backlogSourceMB + " MB, " +
                        "TargetBacklog=" + backlogTargetMB + " MB, " +
                        "Source=" + sourceFilePath + ", " +
                        "Target=" + targetFilePath;
                    LogDebug(msg);
                }
            }

        }

        #endregion

        #region "clsEventNotifier events"

        /// <summary>
        /// Register event handlers
        /// </summary>
        /// <param name="processingClass"></param>
        /// <param name="writeDebugEventsToLog"></param>
        protected void RegisterEvents(clsEventNotifier processingClass, bool writeDebugEventsToLog = true)
        {
            if (writeDebugEventsToLog)
            {
                processingClass.DebugEvent += DebugEventHandler;
            }
            else
            {
                processingClass.DebugEvent += DebugEventHandlerConsoleOnly;
            }

            processingClass.StatusEvent += StatusEventHandler;
            processingClass.ErrorEvent += ErrorEventHandler;
            processingClass.WarningEvent += WarningEventHandler;

            // Note that ProgressUpdateHandler does not display a message at console
            // Instead, it calls m_StatusTools.UpdateAndWrite, which updates the status file
            processingClass.ProgressUpdate += ProgressUpdateHandler;
        }

        /// <summary>
        /// Unregister the event handler for the given LogLevel
        /// </summary>
        /// <param name="processingClass"></param>
        /// <param name="messageType"></param>
        protected void UnregisterEventHandler(clsEventNotifier processingClass, BaseLogger.LogLevels messageType)
        {
            switch (messageType)
            {
                case BaseLogger.LogLevels.DEBUG:
                    processingClass.DebugEvent -= DebugEventHandler;
                    processingClass.DebugEvent -= DebugEventHandlerConsoleOnly;
                    break;
                case BaseLogger.LogLevels.ERROR:
                    processingClass.ErrorEvent -= ErrorEventHandler;
                    break;
                case BaseLogger.LogLevels.WARN:
                    processingClass.WarningEvent -= WarningEventHandler;
                    break;
                case BaseLogger.LogLevels.INFO:
                    processingClass.StatusEvent -= StatusEventHandler;
                    break;
                default:
                    throw new Exception("Log level not supported for unregistering");
            }
        }

        private void DebugEventHandlerConsoleOnly(string statusMessage)
        {
            LogTools.LogDebug(statusMessage, writeToLog: false);
        }

        private void DebugEventHandler(string statusMessage)
        {
            LogDebug(statusMessage);
        }

        private void StatusEventHandler(string statusMessage)
        {
            LogMessage(statusMessage);
        }

        private void ErrorEventHandler(string errorMessage, Exception ex)
        {
            LogError(errorMessage, ex);
        }

        private void WarningEventHandler(string warningMessage)
        {
            LogWarning(warningMessage);
        }

        /// <summary>
        /// Update progress and re-write the analysis status file
        /// </summary>
        /// <param name="progressMessage"></param>
        /// <param name="percentComplete"></param>
        /// <remarks>This does not display a message at console (intentionally)</remarks>
        protected void ProgressUpdateHandler(string progressMessage, float percentComplete)
        {
            m_StatusTools.CurrentOperation = progressMessage;
            m_StatusTools.UpdateAndWrite(percentComplete);
        }

        #endregion
    }
}