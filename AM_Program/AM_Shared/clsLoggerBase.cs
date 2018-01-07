
using System;
using PRISM.Logging;

using PRISM;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Methods to be inherited by classes that use clsLogTools
    /// </summary>
    public abstract class clsLoggerBase
    {

        /// <summary>
        /// Debug level
        /// </summary>
        /// <remarks>Ranges from 0 (minimum output) to 5 (max detail)</remarks>
        protected short m_DebugLevel = 1;

        /// <summary>
        /// Show a status message at the console and optionally include in the log file, tagging it as a debug message
        /// </summary>
        /// <param name="statusMessage">Status message</param>
        /// <param name="logFileDebugLevel">
        /// Log level for whether to log to disk:
        /// 0 to always log
        /// 1 to log if m_DebugLevel is >= 1
        /// 2 to log if m_DebugLevel is >= 2
        /// 10 to not log to disk
        /// </param>
        /// <remarks>The message is shown in dark grey in the console.</remarks>
        protected void LogDebug(string statusMessage, int logFileDebugLevel = 0)
        {
            var writeToLog = (logFileDebugLevel < 10 && (logFileDebugLevel == 0 || logFileDebugLevel <= m_DebugLevel));
            clsGlobal.LogDebug(statusMessage, writeToLog);
        }

        /// <summary>
        /// Log an error message, optionally logging to the database in addition to the log file
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        /// <remarks>The error is shown in red in the console</remarks>
        protected virtual void LogError(string errorMessage, bool logToDb = false)
        {
            ConsoleMsgUtils.ShowError(errorMessage, false);

            clsLogTools.LoggerTypes loggerType;
            if (logToDb && !clsGlobal.OfflineMode)
            {
                loggerType = clsLogTools.LoggerTypes.LogDb;
            }
            else
            {
                loggerType = clsLogTools.LoggerTypes.LogFile;
            }

            try
            {
                clsLogTools.WriteLog(loggerType, BaseLogger.LogLevels.ERROR, errorMessage);
            }
            catch (Exception ex)
            {
                clsGlobal.ErrorWritingToLog(errorMessage, ex);
            }

        }

        /// <summary>
        /// Log an error message and exception
        /// </summary>
        /// <param name="errorMessage">Error message (do not include ex.message)</param>
        /// <param name="ex">Exception to log (allowed to be nothing)</param>
        /// <param name="logToDatabase">When true, log to the database (and to the file)</param>
        /// <remarks>The error is shown in red in the console.  The exception stack trace is shown in cyan</remarks>
        protected virtual void LogError(string errorMessage, Exception ex, bool logToDatabase = false)
        {
            clsGlobal.LogError(errorMessage, ex, logToDatabase);
        }

        /// <summary>
        /// Show a status message at the console and optionally include in the log file
        /// </summary>
        /// <param name="statusMessage">Status message</param>
        /// <param name="logFileDebugLevel">
        /// Log level for whether to log to disk:
        /// 0 to always log
        /// 1 to log if m_DebugLevel is >= 1
        /// 2 to log if m_DebugLevel is >= 2
        /// 10 to not log to disk
        /// </param>
        /// <param name="isError">True if this is an error</param>
        protected void LogMessage(string statusMessage, int logFileDebugLevel = 0, bool isError = false)
        {
            var writeToLog = (logFileDebugLevel < 10 && (logFileDebugLevel == 0 || logFileDebugLevel <= m_DebugLevel));
            clsGlobal.LogMessage(statusMessage, isError, writeToLog);
        }

        /// <summary>
        /// Display a warning message at the console and write to the log file
        /// </summary>
        /// <param name="warningMessage">Warning message</param>
        /// <remarks>The warning is shown in yellow in the console.</remarks>
        protected void LogWarning(string warningMessage)
        {
            clsGlobal.LogWarning(warningMessage);
        }

    }
}