using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;

namespace AnalysisManagerBase.Logging
{
    /// <summary>
    /// Logs messages to a file
    /// </summary>
    public class FileLogger : BaseLogger
    {
        #region "Constants"

        /// <summary>
        /// Interval, in milliseconds, between flushing log messages to disk
        /// </summary>
        private const int LOG_INTERVAL_MILLISECONDS = 500;

        /// <summary>
        /// Date format for log file names
        /// </summary>
        public const string LOG_FILE_DATECODE = "MM-dd-yyyy";

        private const string LOG_FILE_MATCH_SPEC = "??-??-????";

        private const string LOG_FILE_DATE_REGEX = @"(?<Month>\d+)-(?<Day>\d+)-(?<Year>\d{4,4})";

        private const string LOG_FILE_EXTENSION = ".txt";

        private const int OLD_LOG_FILE_AGE_THRESHOLD_DAYS = 32;

        #endregion

        #region "Static variables"

        private static readonly ConcurrentQueue<LogMessage> mMessageQueue = new ConcurrentQueue<LogMessage>();

        private static bool mQueueLoggerInitialized;

        private static readonly List<string> mMessageQueueEntryFlag = new List<string>();

        private static readonly Timer mQueueLogger = new Timer(LogMessagesCallback, null, 0, 0);

        /// <summary>
        /// Tracks the number of successive dequeue failures
        /// </summary>
        private static int mFailedDequeueEvents;

        /// <summary>
        /// Base log file name
        /// </summary>
        /// <remarks>This is updated by ChangeLogFileBaseName or via the constructor</remarks>
        private static string mBaseFileName = "";

        /// <summary>
        /// Log file date (as a string)
        /// </summary>
        private static string mLogFileDate = "";

        /// <summary>
        /// Relative file path to the current log file
        /// </summary>
        private static string mLogFilePath = "";

        private static DateTime mLastCheckOldLogs = DateTime.UtcNow.AddDays(-1);

        #endregion

        #region "Member variables"

        private LogLevels mLogLevel;

        #endregion

        #region "Properties"

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
        /// Current log file path
        /// </summary>
        public string LogFilePath => mLogFilePath;

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
        /// Constructor
        /// </summary>
        /// <param name="baseFileName">Base log file name</param>
        /// <param name="logLevel">Log level</param>
        public FileLogger(string baseFileName = "AppLogFile", LogLevels logLevel = LogLevels.INFO)
        {
            ChangeLogFileBaseName(baseFileName);

            LogLevel = logLevel;

            if (mQueueLoggerInitialized)
                return;

            ShowTraceMessage("Starting the FileLogger QueueLogger");

            mQueueLoggerInitialized = true;
            mQueueLogger.Change(500, LOG_INTERVAL_MILLISECONDS);

        }

        /// <summary>
        /// Look for log files over 32 days old that can be moved into a subdirectory
        /// </summary>
        /// <param name="logFilePath"></param>
        private static void ArchiveOldLogs(string logFilePath)
        {
            var targetPath = "??";

            try
            {
                var currentLogFile = new FileInfo(logFilePath);

                var matchSpec = "*_" + LOG_FILE_MATCH_SPEC + LOG_FILE_EXTENSION;

                var logDirectory = currentLogFile.Directory;
                if (logDirectory == null)
                {

                    WriteLog(LogLevels.WARN, "Error archiving old log files; cannot determine the parent directory of " + currentLogFile);
                    return;
                }

                mLastCheckOldLogs = DateTime.UtcNow;

                var logFiles = logDirectory.GetFiles(matchSpec);

                var matcher = new Regex(LOG_FILE_DATE_REGEX, RegexOptions.Compiled);

                foreach (var logFile in logFiles)
                {
                    var match = matcher.Match(logFile.Name);

                    if (!match.Success)
                        continue;

                    var logFileYear = int.Parse(match.Groups["Year"].Value);
                    var logFileMonth = int.Parse(match.Groups["Month"].Value);
                    var logFileDay = int.Parse(match.Groups["Day"].Value);

                    var logDate = new DateTime(logFileYear, logFileMonth, logFileDay);

                    if (DateTime.Now.Subtract(logDate).TotalDays <= OLD_LOG_FILE_AGE_THRESHOLD_DAYS)
                        continue;

                    var targetDirectory = new DirectoryInfo(Path.Combine(logDirectory.FullName, logFileYear.ToString()));
                    if (!targetDirectory.Exists)
                        targetDirectory.Create();

                    targetPath = Path.Combine(targetDirectory.FullName, logFile.Name);

                    logFile.MoveTo(targetPath);
                }
            }
            catch (Exception ex)
            {
                WriteLog(LogLevels.ERROR, "Error moving old log file to " + targetPath, ex);
            }
        }

        /// <summary>
        /// Update the log file's base name
        /// </summary>
        /// <param name="baseName"></param>
        /// <remarks>Will append today's date to the base name</remarks>
        public static void ChangeLogFileBaseName(string baseName)
        {
            mBaseFileName = baseName;
            ChangeLogFileName();
        }

        /// <summary>
        /// Changes the base log file name
        /// </summary>
        public static void ChangeLogFileName()
        {
            mLogFileDate = DateTime.Now.ToString(LOG_FILE_DATECODE);
            ChangeLogFileName(mBaseFileName + "_" + mLogFileDate + LOG_FILE_EXTENSION);
        }

        /// <summary>
        /// Changes the base log file name
        /// </summary>
        /// <param name="relativeFilePath">Log file base name and path (relative to program folder)</param>
        /// <remarks>This method is called by the Mage, Ascore, and Multialign plugins</remarks>
        public static void ChangeLogFileName(string relativeFilePath)
        {
            mLogFilePath = relativeFilePath;
        }

        /// <summary>
        /// Immediately write out any queued messages (using the current thread)
        /// </summary>
        /// <remarks>
        /// There is no need to call this method if you create an instance of this class.
        /// On the other hand, if you only call static methods in this class, call this method
        /// before ending the program to assure that all messages have been logged.
        /// </remarks>
        public override void FlushPendingMessages()
        {
            StartLogQueuedMessages();
        }

        /// <summary>
        /// Callback invoked by the mQueueLogger timer
        /// </summary>
        /// <param name="state"></param>
        private static void LogMessagesCallback(object state)
        {
            ShowTraceMessage("FileLogger.mQueueLogger callback raised");
            StartLogQueuedMessages();
        }

        private static void LogQueuedMessages()
        {
            StreamWriter writer = null;
            var messagesWritten = 0;

            try
            {

                while (!mMessageQueue.IsEmpty)
                {
                    if (!mMessageQueue.TryDequeue(out var logMessage))
                    {
                        mFailedDequeueEvents += 1;
                        LogDequeueError(mFailedDequeueEvents, mMessageQueue.Count);
                        return;
                    }

                    mFailedDequeueEvents = 0;

                    try
                    {
                        // Check to determine if a new file should be started
                        var testFileDate = logMessage.MessageDateLocal.ToString(LOG_FILE_DATECODE);
                        if (!string.Equals(testFileDate, mLogFileDate))
                        {
                            mLogFileDate = testFileDate;
                            ChangeLogFileName();

                            writer?.Close();
                            writer = null;
                        }
                    }
                    catch (Exception ex2)
                    {
                        PRISM.ConsoleMsgUtils.ShowError("Error defining the new log file name: " + ex2.Message, ex2, false, false);
                    }

                    if (logMessage.LogLevel == LogLevels.ERROR || logMessage.LogLevel == LogLevels.FATAL)
                    {
                        MostRecentErrorMessage = logMessage.Message;
                    }

                    if (writer == null)
                    {
                        ShowTraceMessage(string.Format("Opening log file: {0}", mLogFilePath));

                        var logFile = new FileInfo(mLogFilePath);
                        if (logFile.Directory == null)
                        {
                            // Create the log file in the current directory
                            ChangeLogFileName(logFile.Name);
                            logFile = new FileInfo(mLogFilePath);

                        }
                        else if (!logFile.Directory.Exists)
                        {
                            logFile.Directory.Create();
                        }

                        writer = new StreamWriter(new FileStream(logFile.FullName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite));
                    }
                    writer.WriteLine(logMessage.GetFormattedMessage());

                    if (logMessage.MessageException != null)
                    {
                        writer.WriteLine(PRISM.clsStackTraceFormatter.GetExceptionStackTraceMultiLine(logMessage.MessageException));
                    }

                    messagesWritten++;

                }

                if (DateTime.UtcNow.Subtract(mLastCheckOldLogs).TotalHours > 24)
                {
                    mLastCheckOldLogs = DateTime.UtcNow;

                    ArchiveOldLogs(mLogFilePath);
                }

            }
            catch (Exception ex)
            {
                PRISM.ConsoleMsgUtils.ShowError("Error writing queued log messages to disk: " + ex.Message, ex, false, false);
            }
            finally
            {
                writer?.Close();
                ShowTraceMessage(string.Format("FileLogger writer closed; wrote {0} messages", messagesWritten));
            }
        }

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

        /// <summary>
        /// Check for queued messages
        /// If found, try to log them, wrapping then attempt with Monitor.TryEnter and Monitor.Exit
        /// </summary>
        private static void StartLogQueuedMessages()
        {
            if (mMessageQueue.IsEmpty)
                return;

            if (Monitor.TryEnter(mMessageQueueEntryFlag))
            {
                try
                {
                    LogQueuedMessages();
                }
                finally
                {
                    Monitor.Exit(mMessageQueueEntryFlag);
                }
            }
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
        public static void WriteLog(LogLevels logLevel, string message, Exception ex = null)
        {
            var logMessage = new LogMessage(logLevel, message, ex);
            WriteLog(logMessage);
        }

        /// <summary>
        /// Log a message (regardless of base.LogLevel)
        /// </summary>
        /// <param name="logMessage"></param>
        public static void WriteLog(LogMessage logMessage)
        {
            mMessageQueue.Enqueue(logMessage);
        }

        #endregion

        /// <summary>
        /// Class is disposing; write out any queued messages
        /// </summary>
        ~FileLogger()
        {
            ShowTraceMessage("Disposing FileLogger");
            FlushPendingMessages();
        }
    }
}
