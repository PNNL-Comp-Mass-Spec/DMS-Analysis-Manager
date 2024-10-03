using System;
using JetBrains.Annotations;
using PRISM;
using PRISM.Logging;

namespace AnalysisManagerBase.StatusReporting
{
    /// <summary>
    /// Methods to be inherited by classes that use LogTools
    /// </summary>
    public abstract class LoggerBase
    {
        /// <summary>
        /// Debug level
        /// </summary>
        /// <remarks>Ranges from 0 (minimum output) to 5 (max detail)</remarks>
        protected short mDebugLevel = 1;

        /// <summary>
        /// Show a status message at the console, tagging it as a debug message
        /// </summary>
        /// <param name="format">Status message format string</param>
        /// <param name="args">Values to substitute in the format string</param>
        [StringFormatMethod("format")]
        public void LogDebug(string format, params object[] args)
        {
            LogDebug(string.Format(format, args));
        }

        /// <summary>
        /// Show a status message at the console and optionally include in the log file, tagging it as a debug message
        /// </summary>
        /// <remarks>The message is shown in dark gray in the console</remarks>
        /// <param name="statusMessage">Status message</param>
        /// <param name="logFileDebugLevel">
        /// Log level for whether to log to disk:
        /// 0 to always log
        /// 1 to log if mDebugLevel is >= 1
        /// 2 to log if mDebugLevel is >= 2
        /// 10 to not log to disk
        /// </param>
        public void LogDebug(string statusMessage, int logFileDebugLevel = 0)
        {
            var writeToLog = logFileDebugLevel < 10 && (logFileDebugLevel == 0 || logFileDebugLevel <= mDebugLevel);
            LogTools.LogDebug(statusMessage, writeToLog);
        }

        /// <summary>
        /// Log an error message
        /// </summary>
        /// <remarks>The error is shown in red in the console</remarks>
        /// <param name="format">Error message format string</param>
        /// <param name="args">Values to substitute in the format string</param>
        [StringFormatMethod("format")]
        public void LogError(string format, params object[] args)
        {
            LogError(string.Format(format, args));
        }

        /// <summary>
        /// Log an error message, optionally logging to the database in addition to the log file
        /// </summary>
        /// <remarks>The error is shown in red in the console</remarks>
        /// <param name="errorMessage">Error message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        public virtual void LogError(string errorMessage, bool logToDb = false)
        {
            ConsoleMsgUtils.ShowErrorCustom(errorMessage, false);

            LogTools.LoggerTypes loggerType;

            if (logToDb && !Global.OfflineMode)
            {
                loggerType = LogTools.LoggerTypes.LogDb;
            }
            else
            {
                loggerType = LogTools.LoggerTypes.LogFile;
            }

            try
            {
                LogTools.WriteLog(loggerType, BaseLogger.LogLevels.ERROR, errorMessage);
            }
            catch (Exception ex)
            {
                Global.ErrorWritingToLog(errorMessage, ex);
            }
        }

        /// <summary>
        /// Log an error message and exception
        /// </summary>
        /// <remarks>The error is shown in red in the console.  The exception stack trace is shown in cyan</remarks>
        /// <param name="errorMessage">Error message (do not include ex.message)</param>
        /// <param name="ex">Exception to log (allowed to be null)</param>
        /// <param name="logToDatabase">When true, log to the database (and to the file)</param>
        protected virtual void LogError(string errorMessage, Exception ex, bool logToDatabase = false)
        {
            LogTools.LogError(errorMessage, ex, logToDatabase);
        }

        /// <summary>
        /// Show a status message at the console
        /// </summary>
        /// <param name="format">Status message format string</param>
        /// <param name="args">Values to substitute in the format string</param>
        [StringFormatMethod("format")]
        public void LogMessage(string format, params object[] args)
        {
            LogMessage(string.Format(format, args));
        }

        /// <summary>
        /// Show a status message at the console and optionally include in the log file
        /// </summary>
        /// <param name="statusMessage">Status message</param>
        /// <param name="logFileDebugLevel">
        /// Log level for whether to log to disk:
        /// 0 to always log
        /// 1 to log if mDebugLevel is >= 1
        /// 2 to log if mDebugLevel is >= 2
        /// 10 to not log to disk
        /// </param>
        /// <param name="isError">True if this is an error</param>
        public void LogMessage(string statusMessage, int logFileDebugLevel = 0, bool isError = false)
        {
            var writeToLog = logFileDebugLevel < 10 && (logFileDebugLevel == 0 || logFileDebugLevel <= mDebugLevel);
            LogTools.LogMessage(statusMessage, isError, writeToLog);
        }

        /// <summary>
        /// Show a warning message at the console and write to the log file
        /// </summary>
        /// <param name="format">Warning message format string</param>
        /// <param name="args">Values to substitute in the format string</param>
        [StringFormatMethod("format")]
        public void LogWarning(string format, params object[] args)
        {
            LogWarning(string.Format(format, args));
        }

        /// <summary>
        /// Show a warning message at the console and write to the log file
        /// </summary>
        /// <remarks>The warning is shown in yellow in the console</remarks>
        /// <param name="warningMessage">Warning message</param>
        protected void LogWarning(string warningMessage)
        {
            LogTools.LogWarning(warningMessage);
        }

        /// <summary>
        /// Register event handlers
        /// </summary>
        /// <param name="processingClass">Processing class instance</param>
        /// <param name="writeDebugEventsToLog">If true, write debug events to the log</param>
        protected virtual void RegisterEvents(IEventNotifier processingClass, bool writeDebugEventsToLog = true)
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

            // Do not watch the ProgressUpdate event
            // AnalysisMgrBase does monitor ProgressUpdate
        }

        /// <summary>
        /// Unregister the event handler for the given LogLevel
        /// </summary>
        /// <param name="processingClass">Processing class instance</param>
        /// <param name="messageType">Message type</param>
        protected void UnregisterEventHandler(EventNotifier processingClass, BaseLogger.LogLevels messageType)
        {
            UnregisterEventHandler((IEventNotifier)processingClass, messageType);
        }

        /// <summary>
        /// Unregister the event handler for the given LogLevel
        /// </summary>
        /// <param name="processingClass">Processing class instance</param>
        /// <param name="messageType">Message type</param>
        protected void UnregisterEventHandler(IEventNotifier processingClass, BaseLogger.LogLevels messageType)
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
    }
}
