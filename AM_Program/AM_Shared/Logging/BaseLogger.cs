using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AnalysisManagerBase.Logging
{
    /// <summary>
    /// Base class for the FileLogger
    /// </summary>
    public abstract class BaseLogger
    {
        /// <summary>
        /// Log levels
        /// </summary>
        public enum LogLevels
        {
            /// <summary>
            /// Debug message
            /// </summary>
            DEBUG = 5,

            /// <summary>
            /// Informational message
            /// </summary>
            INFO = 4,

            /// <summary>
            /// Warning message
            /// </summary>
            WARN = 3,

            /// <summary>
            /// Error message
            /// </summary>
            ERROR = 2,

            /// <summary>
            /// Fatal error message
            /// </summary>
            FATAL = 1,

            /// <summary>
            /// Disables all logging
            /// </summary>
            NOLOGGING = 0
        }

        /// <summary>
        /// Set to True if we cannot log to the official log file, we try to log to the local log file, and even that file cannot be written
        /// </summary>
        private static bool mLocalLogFileAccessError;

        #region "Properties"

        /// <summary>
        /// Most recent error message
        /// </summary>
        public static string MostRecentErrorMessage { get; protected set; } = "";

        /// <summary>
        /// When true, show additional debug messages at the console
        /// </summary>
        public static bool TraceMode { get; set; }

        #endregion

        /// <summary>
        /// Compare logLevel to mLogLevel
        /// </summary>
        /// <param name="messageLogLevel"></param>
        /// <param name="logThresholdLevel"></param>
        /// <returns>True if this message should be logged</returns>
        protected bool AllowLog(LogLevels messageLogLevel, LogLevels logThresholdLevel)
        {
            return messageLogLevel <= logThresholdLevel;
        }

        /// <summary>
        /// Log a local message regarding a message queue dequeue error
        /// </summary>
        /// <param name="failedDeqeueueEvents"></param>
        /// <param name="messageQueueCount"></param>
        protected static void LogDequeueError(int failedDeqeueueEvents, int messageQueueCount)
        {
            bool warnUser;

            if (failedDeqeueueEvents < 5)
            {
                warnUser = true;
            }
            else
            {
                var modDivisor = (int)(Math.Ceiling(Math.Log10(failedDeqeueueEvents)) * 10);
                warnUser = failedDeqeueueEvents % modDivisor == 0;
            }

            if (!warnUser)
                return;

            if (messageQueueCount == 1)
            {
                LogLocalMessage(LogLevels.WARN, "Unable to dequeue the next message to log to the database");
            }
            else
            {
                LogLocalMessage(LogLevels.WARN, string.Format("Unable to dequeue the next log message to log to the database; {0} pending messages", messageQueueCount));
            }
        }

        /// <summary>
        /// Log a message to the local, generic log file
        /// </summary>
        /// <param name="logLevel"></param>
        /// <param name="message"></param>
        /// <param name="localLogFilePath"></param>
        /// <remarks>Used to log errors and warnings when the standard log file (or database) cannot be written to</remarks>
        protected static void LogLocalMessage(LogLevels logLevel, string message, string localLogFilePath = "FileLoggerErrors.txt")
        {
            var logMessage = new LogMessage(logLevel, message);
            LogLocalMessage(logMessage, localLogFilePath);
        }

        /// <summary>
        /// Log a message to the local, generic log file
        /// </summary>
        /// <param name="logMessage"></param>
        /// <param name="localLogFilePath"></param>
        /// <remarks>Used to log errors and warnings when the standard log file (or database) cannot be written to</remarks>
        protected static void LogLocalMessage(LogMessage logMessage, string localLogFilePath = "FileLoggerErrors.txt")
        {
            switch (logMessage.LogLevel)
            {
                case LogLevels.DEBUG:
                    PRISM.ConsoleMsgUtils.ShowDebug(logMessage.Message);
                    break;
                case LogLevels.WARN:
                    PRISM.ConsoleMsgUtils.ShowWarning(logMessage.Message);
                    break;
                case LogLevels.ERROR:
                case LogLevels.FATAL:
                    PRISM.ConsoleMsgUtils.ShowError(logMessage.Message);
                    break;
                default:
                    Console.WriteLine(logMessage.Message);
                    break;
            }

            if (string.IsNullOrWhiteSpace(localLogFilePath))
                localLogFilePath = "FileLoggerErrors.txt";

            try
            {
                var localLogFile = new FileInfo(localLogFilePath);
                localLogFilePath = localLogFile.FullName;

                using (var localLogWriter = new StreamWriter(new FileStream(localLogFile.FullName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)))
                {
                    localLogWriter.WriteLine(logMessage.GetFormattedMessage());
                }
            }
            catch (Exception ex)
            {
                if (!mLocalLogFileAccessError)
                {
                    mLocalLogFileAccessError = true;
                    PRISM.ConsoleMsgUtils.ShowError("Error writing to the local log file: " + localLogFilePath);
                }

                PRISM.ConsoleMsgUtils.ShowError(
                    string.Format("Error writing '{0}' to the local log file: {1}", logMessage.GetFormattedMessage(), ex), false, false);
            }

        }

        /// <summary>
        /// Immediately write out any queued messages (using the current thread)
        /// </summary>
        public abstract void FlushPendingMessages();

        /// <summary>
        /// Log a message (provided logLevel is LogLevel or higher)
        /// </summary>
        /// <param name="logLevel"></param>
        /// <param name="message"></param>
        /// <param name="ex"></param>

        public void LogMessage(LogLevels logLevel, string message, Exception ex = null)
        {
            // Send the log message
            switch (logLevel)
            {
                case LogLevels.DEBUG:
                    Debug(message, ex);
                    break;
                case LogLevels.ERROR:
                    Error(message, ex);
                    break;
                case LogLevels.FATAL:
                    Fatal(message, ex);
                    break;
                case LogLevels.INFO:
                    Info(message, ex);
                    break;
                case LogLevels.WARN:
                    Warn(message, ex);
                    break;
                default:
                    throw new Exception("Invalid log level specified");
            }
        }

        /// <summary>
        /// Show a trace message at the console if TraceMode is true
        /// </summary>
        /// <param name="message"></param>
        protected static void ShowTraceMessage(string message)
        {
            if (TraceMode)
                clsGlobal.ShowTimestampTrace(message);
        }

        #region "Methods to be defined in derived classes"

        /// <summary>
        /// Log a debug message (provided LogLevel is LogLevels.DEBUG)
        /// </summary>
        /// <param name="message">Log message</param>
        /// <param name="ex">Optional exception; can be null</param>
        public abstract void Debug(string message, Exception ex = null);

        /// <summary>
        /// Log an error message (provided LogLevel is LogLevels.ERROR or higher)
        /// </summary>
        /// <param name="message">Log message</param>
        /// <param name="ex">Optional exception; can be null</param>
        public abstract void Error(string message, Exception ex = null);

        /// <summary>
        /// Log a fatal error message (provided LogLevel is LogLevels.FATAL or higher)
        /// </summary>
        /// <param name="message">Log message</param>
        /// <param name="ex">Optional exception; can be null</param>
        public abstract void Fatal(string message, Exception ex = null);

        /// <summary>
        /// Log an informational message (provided LogLevel is LogLevels.INFO or higher)
        /// </summary>
        /// <param name="message">Log message</param>
        /// <param name="ex">Optional exception; can be null</param>
        public abstract void Info(string message, Exception ex = null);

        /// <summary>
        /// Log a warning message (provided LogLevel is LogLevels.WARN or higher)
        /// </summary>
        /// <param name="message">Log message</param>
        /// <param name="ex">Optional exception; can be null</param>
        public abstract void Warn(string message, Exception ex = null);

        #endregion
    }

}
