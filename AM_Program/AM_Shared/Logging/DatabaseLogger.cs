using System;

namespace AnalysisManagerBase.Logging
{
    /// <summary>
    /// Logs messages to a database by calling a stored procedure
    /// </summary>
    public abstract class DatabaseLogger : BaseLogger
    {

        #region "Constants"

        /// <summary>
        /// Interval, in milliseconds, between flushing log messages to the database
        /// </summary>
        protected const int LOG_INTERVAL_MILLISECONDS = 1000;

        /// <summary>
        /// Database timeout length, in seconds
        /// </summary>
        protected const int TIMEOUT_SECONDS = 15;

        #endregion

        #region "Member variables"

        private LogLevels mLogLevel;

        #endregion

        #region "Properties"

        /// <summary>
        /// When true, also send any messages to the file logger
        /// </summary>
        public bool EchoMessagesToFileLogger { get; set; } = true;

        /// <summary>
        /// When true, log type will be changed from all caps to InitialCaps (e.g. INFO to Info)
        /// </summary>
        public static bool InitialCapsLogTypes { get; set; } = true;

        /// <summary>
        /// True if info level logging is enabled (LogLevel is LogLevels.DEBUG or higher)
        /// </summary>
        public bool IsDebugEnabled { get; private set; }

        /// <summary>
        /// True if info level logging is enabled (LogLevel is LogLevels.ERROR or higher)
        /// </summary>
        public bool IsErrorEnabled { get; private set; }

        /// <summary>
        /// True if info level logging is enabled (LogLevel is LogLevels.FATAL or higher)
        /// </summary>
        public bool IsFatalEnabled { get; private set; }

        /// <summary>
        /// True if info level logging is enabled (LogLevel is LogLevels.INFO or higher)
        /// </summary>
        public bool IsInfoEnabled { get; private set; }

        /// <summary>
        /// True if info level logging is enabled (LogLevel is LogLevels.WARN or higher)
        /// </summary>
        public bool IsWarnEnabled { get; private set; }

        /// <summary>
        /// Get or set the current log level
        /// </summary>
        /// <remarks>
        /// If the LogLevel is DEBUG, all messages are logged
        /// If the LogLevel is INFO, all messages except DEBUG messages are logged
        /// If the LogLevel is ERROR, only FATAL and ERROR messages are logged
        /// </remarks>
        public LogLevels LogLevel
        {
            get => mLogLevel;
            set => SetLogLevel(value);
        }

        #endregion

        /// <summary>
        /// Update the database connection info
        /// </summary>
        /// <param name="moduleName">Program name to be sent to the PostedBy field when contacting the database</param>
        /// <param name="connectionString">ODBC-style connection string</param>
        /// <param name="storedProcedure">Stored procedure to call</param>
        /// <param name="logTypeParamName">LogType parameter name</param>
        /// <param name="messageParamName">Message parameter name</param>
        /// <param name="postedByParamName">Log source parameter name</param>
        /// <param name="logTypeParamSize">LogType parameter size</param>
        /// <param name="messageParamSize">Message parameter size</param>
        /// <param name="postedByParamSize">Log source parameter size</param>
        /// <remarks>Will append today's date to the base name</remarks>
        public abstract void ChangeConnectionInfo(
            string moduleName,
            string connectionString,
            string storedProcedure,
            string logTypeParamName,
            string messageParamName,
            string postedByParamName,
            int logTypeParamSize = 128,
            int messageParamSize = 4000,
            int postedByParamSize = 128);

        /// <summary>
        /// Immediately write out any queued messages (using the current thread)
        /// </summary>
        /// <remarks>
        /// There is no need to call this method since you must create an instance of a database logging class to use it
        /// and when that class is disposed, it calls StartLogQueuedMessages()
        /// </remarks>
        public override void FlushPendingMessages()
        {
        }

        /// <summary>
        /// Convert log level to a string, optionally changing from all caps to initial caps
        /// </summary>
        /// <param name="logLevel"></param>
        /// <returns></returns>
        protected static string LogLevelToString(LogLevels logLevel)
        {
            var logLevelText = logLevel.ToString();

            if (!InitialCapsLogTypes)
                return logLevelText;

            return logLevelText.Substring(0, 1).ToUpper() + logLevelText.Substring(1).ToLower();
        }

        /// <summary>
        /// Disable database logging
        /// </summary>
        public abstract void RemoveConnectionInfo();

        /// <summary>
        /// Update the Log Level (called by property LogLevel)
        /// </summary>
        /// <param name="logLevel"></param>
        private void SetLogLevel(LogLevels logLevel)
        {
            mLogLevel = logLevel;
            IsDebugEnabled = mLogLevel >= LogLevels.DEBUG;
            IsErrorEnabled = mLogLevel >= LogLevels.ERROR;
            IsFatalEnabled = mLogLevel >= LogLevels.FATAL;
            IsInfoEnabled = mLogLevel >= LogLevels.INFO;
            IsWarnEnabled = mLogLevel >= LogLevels.WARN;
        }

        #region "Message logging methods"

        /// <summary>
        /// Log a debug message (provided LogLevel is LogLevels.DEBUG)
        /// </summary>
        /// <param name="message">Log message</param>
        /// <param name="ex">Optional exception; can be null</param>
        public override void Debug(string message, Exception ex = null)
        {
            if (!AllowLog(LogLevels.DEBUG, mLogLevel))
                return;

            WriteLog(LogLevels.DEBUG, message, ex);
        }

        /// <summary>
        /// Log an error message (provided LogLevel is LogLevels.ERROR or higher)
        /// </summary>
        /// <param name="message">Log message</param>
        /// <param name="ex">Optional exception; can be null</param>
        public override void Error(string message, Exception ex = null)
        {
            if (!AllowLog(LogLevels.ERROR, mLogLevel))
                return;

            WriteLog(LogLevels.ERROR, message, ex);
        }

        /// <summary>
        /// Log a fatal error message (provided LogLevel is LogLevels.FATAL or higher)
        /// </summary>
        /// <param name="message">Log message</param>
        /// <param name="ex">Optional exception; can be null</param>
        public override void Fatal(string message, Exception ex = null)
        {
            if (!AllowLog(LogLevels.FATAL, mLogLevel))
                return;

            WriteLog(LogLevels.FATAL, message, ex);
        }

        /// <summary>
        /// Log an informational message (provided LogLevel is LogLevels.INFO or higher)
        /// </summary>
        /// <param name="message">Log message</param>
        /// <param name="ex">Optional exception; can be null</param>
        public override void Info(string message, Exception ex = null)
        {
            if (!AllowLog(LogLevels.INFO, mLogLevel))
                return;

            WriteLog(LogLevels.INFO, message, ex);
        }

        /// <summary>
        /// Log a warning message (provided LogLevel is LogLevels.WARN or higher)
        /// </summary>
        /// <param name="message">Log message</param>
        /// <param name="ex">Optional exception; can be null</param>
        public override void Warn(string message, Exception ex = null)
        {
            if (!AllowLog(LogLevels.WARN, mLogLevel))
                return;

            WriteLog(LogLevels.WARN, message, ex);
        }

        /// <summary>
        /// Log a message (regardless of base.LogLevel)
        /// </summary>
        /// <param name="logLevel"></param>
        /// <param name="message"></param>
        /// <param name="ex"></param>
        public void WriteLog(LogLevels logLevel, string message, Exception ex = null)
        {
            var logMessage = new LogMessage(logLevel, message, ex);
            WriteLog(logMessage);
        }

        /// <summary>
        /// Log a message (regardless of base.LogLevel)
        /// </summary>
        /// <param name="logMessage"></param>
        public abstract void WriteLog(LogMessage logMessage);

        #endregion

    }
}
