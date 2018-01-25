//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 01/01/2009
//*********************************************************************************************************

using System;
using System.Globalization;
using System.IO;
using PRISM.Logging;

namespace AnalysisManagerBase
{
    /// <summary>
    /// Class for handling logging via the FileLogger and DatabaseLogger in PRISM.dll
    /// </summary>
    /// <remarks>
    /// Call method CreateFileLogger to define the log file name
    /// </remarks>
    public static class clsLogTools
    {

        #region "Enums"

        /// <summary>
        /// Log types
        /// </summary>
        public enum LoggerTypes
        {
            /// <summary>
            /// Log to a log file
            /// </summary>
            LogFile,

            /// <summary>
            /// Log to the database and to the log file
            /// </summary>
            LogDb
        }

        #endregion

        #region "Class variables"

        /// <summary>
        /// File Logger
        /// </summary>
        private static readonly FileLogger m_FileLogger = new FileLogger();

        /// <summary>
        /// Database logger
        /// </summary>
        private static readonly DatabaseLogger m_DbLogger = new SQLServerDatabaseLogger();

        #endregion

        #region "Properties"

        /// <summary>
        /// File path for the current log file used by the FileAppender
        /// </summary>
        public static string CurrentFileAppenderPath
        {
            get
            {
                if (string.IsNullOrEmpty(FileLogger.LogFilePath))
                {
                    return string.Empty;
                }

                return FileLogger.LogFilePath;
            }
        }

        /// <summary>
        /// Tells calling program file debug status
        /// </summary>
        public static bool FileLogDebugEnabled => m_FileLogger.IsDebugEnabled;

        /// <summary>
        /// Most recent error message
        /// </summary>
        public static string MostRecentErrorMessage => BaseLogger.MostRecentErrorMessage;

        /// <summary>
        /// Working directory path
        /// </summary>
        public static string WorkDirPath { get; set; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Write a message to the logging system
        /// </summary>
        /// <param name="loggerType">Type of logger to use</param>
        /// <param name="logLevel">Level of log reporting</param>
        /// <param name="message">Message to be logged</param>
        public static void WriteLog(LoggerTypes loggerType, BaseLogger.LogLevels logLevel, string message)
        {
            WriteLogWork(loggerType, logLevel, message, null);
        }

        /// <summary>
        /// Write a message and exception to the logging system
        /// </summary>
        /// <param name="loggerType">Type of logger to use</param>
        /// <param name="logLevel">Level of log reporting</param>
        /// <param name="message">Message to be logged</param>
        /// <param name="ex">Exception to be logged</param>
        public static void WriteLog(LoggerTypes loggerType, BaseLogger.LogLevels logLevel, string message, Exception ex)
        {
            WriteLogWork(loggerType, logLevel, message, ex);
        }

        /// <summary>
        /// Write a message and possibly an exception to the logging system
        /// </summary>
        /// <param name="loggerType">Type of logger to use</param>
        /// <param name="logLevel">Level of log reporting</param>
        /// <param name="message">Message to be logged</param>
        /// <param name="ex">Exception to be logged; null if no exception</param>
        private static void WriteLogWork(LoggerTypes loggerType, BaseLogger.LogLevels logLevel, string message, Exception ex)
        {
            BaseLogger myLogger;

            if (clsGlobal.LinuxOS && (loggerType == LoggerTypes.LogDb))
                loggerType = LoggerTypes.LogFile;
            else if (clsGlobal.OfflineMode && loggerType == LoggerTypes.LogDb)
                loggerType = LoggerTypes.LogFile;

            // Establish which logger will be used
            switch (loggerType)
            {
                case LoggerTypes.LogDb:
                    // Note that the Database logger will (by default) also echo messages to the file logger
                    myLogger = m_DbLogger;
                    message = System.Net.Dns.GetHostName() + ": " + message;
                    break;

                case LoggerTypes.LogFile:
                    myLogger = m_FileLogger;

                    if (!string.IsNullOrWhiteSpace(FileLogger.LogFilePath) &&
                        !FileLogger.LogFilePath.Contains(Path.DirectorySeparatorChar.ToString()))
                    {
                        var logFileName = Path.GetFileName(FileLogger.LogFilePath);
                        string workDirLogPath;
                        if (string.IsNullOrEmpty(WorkDirPath))
                            workDirLogPath = Path.Combine(".", logFileName);
                        else
                            workDirLogPath = Path.Combine(WorkDirPath, logFileName);

                        ChangeLogFileBaseName(workDirLogPath, FileLogger.AppendDateToBaseFileName);
                    }

                    break;

                default:
                    throw new Exception("Invalid logger type specified");
            }

            MessageLogged?.Invoke(message, logLevel);

            // Send the log message
            myLogger?.LogMessage(logLevel, message, ex);

        }

        /// <summary>
        /// Update the log file's base name (or relative path)
        /// However, if appendDateToBaseName is false, baseName is the full path to the log file
        /// </summary>
        /// <param name="baseName">Base log file name (or relative path)</param>
        /// <param name="appendDateToBaseName">
        /// When true, the actual log file name will have today's date appended to it, in the form mm-dd-yyyy.txt
        /// When false, the actual log file name will be the base name plus .txt (unless the base name already has an extension)
        /// </param>
        /// <remarks>If baseName is null or empty, the log file name will be named DefaultLogFileName</remarks>
        public static void ChangeLogFileBaseName(string baseName, bool appendDateToBaseName)
        {
            FileLogger.ChangeLogFileBaseName(baseName, appendDateToBaseName);
        }

        /// <summary>
        /// Sets the file logging level via an integer value (Overloaded)
        /// </summary>
        /// <param name="logLevel">Integer corresponding to level (1-5, 5 being most verbose)</param>
        public static void SetFileLogLevel(int logLevel)
        {
            var logLevelEnumType = typeof(BaseLogger.LogLevels);

            // Verify input level is a valid log level
            if (!Enum.IsDefined(logLevelEnumType, logLevel))
            {
                m_FileLogger.Error("Invalid value specified for level: " + logLevel);
                return;
            }

            // Convert input integer into the associated enum
            var logLevelEnum = (BaseLogger.LogLevels)Enum.Parse(logLevelEnumType, logLevel.ToString(CultureInfo.InvariantCulture));

            SetFileLogLevel(logLevelEnum);
        }

        /// <summary>
        /// Sets file logging level based on enumeration (Overloaded)
        /// </summary>
        /// <param name="logLevel">LogLevels value defining level (Debug is most verbose)</param>
        public static void SetFileLogLevel(BaseLogger.LogLevels logLevel)
        {
            m_FileLogger.LogLevel = logLevel;
        }

        /// <summary>
        /// Configures the file logger
        /// </summary>
        /// <param name="logFileNameBase">Base name for log file</param>
        /// <param name="traceMode">When true, show additional debug messages at the console</param>
        public static void CreateFileLogger(string logFileNameBase, bool traceMode = false)
        {
            if (traceMode && !BaseLogger.TraceMode)
                BaseLogger.TraceMode = true;

            BaseLogger.TimestampFormat = LogMessage.TimestampFormatMode.YearMonthDay24hr;

            FileLogger.ChangeLogFileBaseName(logFileNameBase, appendDateToBaseName: true);

            // The analysis manager determines when to log or not log based on internal logic
            // Set the LogLevel tracked by FileLogger to DEBUG so that all messages sent to this class are logged
            SetFileLogLevel(BaseLogger.LogLevels.DEBUG);
        }

        /// <summary>
        /// Configures the database logger
        /// </summary>
        /// <param name="connStr">System.Data.SqlClient style connection string</param>
        /// <param name="moduleName">Module name used by logger</param>
        /// <param name="traceMode">When true, show additional debug messages at the console</param>
        public static void CreateDbLogger(string connStr, string moduleName, bool traceMode = false)
        {
            m_DbLogger.LogLevel = BaseLogger.LogLevels.INFO;

            if (traceMode && !BaseLogger.TraceMode)
                BaseLogger.TraceMode = true;

            m_DbLogger.ChangeConnectionInfo(moduleName, connStr, "PostLogEntry", "type", "message", "postedBy", 128, 4096, 128);

        }

        /// <summary>
        /// Remove the default database logger that was created when the program first started
        /// </summary>
        public static void RemoveDefaultDbLogger()
        {
            m_DbLogger.RemoveConnectionInfo();
        }

        #endregion

        #region "Events"

        /// <summary>
        /// Delegate for event MessageLogged
        /// </summary>
        public delegate void MessageLoggedEventHandler(string message, BaseLogger.LogLevels logLevel);

        /// <summary>
        /// This event is raised when a message is logged
        /// </summary>
        public static event MessageLoggedEventHandler MessageLogged;

        #endregion
    }
}
