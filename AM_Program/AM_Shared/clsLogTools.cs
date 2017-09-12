using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using log4net;
using log4net.Appender;
using log4net.Util.TypeConverters;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 01/01/2009
//
//*********************************************************************************************************

// This assembly attribute tells Log4Net where to find the config file
[assembly: log4net.Config.XmlConfigurator(ConfigFile = "Logging.config", Watch = true)]
namespace AnalysisManagerBase
{
    /// <summary>
    /// Class for handling logging via Log4Net
    /// </summary>
    public class clsLogTools
    {


        #region "Constants"

        /// <summary>
        /// Name of the manager control database logger
        /// </summary>
        public const string DB_LOGGER_MGR_CONTROL = "MgrControlDbDefinedAppender";

        /// <summary>
        /// Name of the logger before manager control parameters have been loaded
        /// </summary>
        public const string DB_LOGGER_NO_MGR_CONTROL_PARAMS = "DbAppenderBeforeMgrControlParams";

        private const string LOG_FILE_APPENDER = "FileAppender";

        #endregion

        #region "Enums"

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
            FATAL = 1
        }

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
            LogDb,

            /// <summary>
            /// Log to the system event log and to the log file
            /// </summary>
            LogSystem
        }

        #endregion

        #region "Class variables"

        private static readonly ILog m_FileLogger = LogManager.GetLogger("FileLogger");
        private static readonly ILog m_DbLogger = LogManager.GetLogger("DbLogger");
        private static readonly ILog m_SysLogger = LogManager.GetLogger("SysLogger");

        private static string m_FileDate = "";
        private static string m_BaseFileName = "";
        private static string m_MostRecentErrorMessage = string.Empty;

        private static FileAppender m_FileAppender;

        #endregion

        #region "Properties"

        /// <summary>
        /// File path for the current log file used by the FileAppender
        /// </summary>
        public static string CurrentFileAppenderPath
        {
            get
            {
                if (string.IsNullOrEmpty(m_FileAppender?.File))
                {
                    return string.Empty;
                }

                return m_FileAppender.File;
            }
        }

        /// <summary>
        /// Tells calling program file debug status
        /// </summary>
        /// <returns>TRUE if debug level enabled for file logger; FALSE otherwise</returns>
        /// <remarks></remarks>
        public static bool FileLogDebugEnabled => m_FileLogger.IsDebugEnabled;

        /// <summary>
        /// Most recent error message
        /// </summary>
        public static string MostRecentErrorMessage => m_MostRecentErrorMessage;

        #endregion

        #region "Methods"

        /// <summary>
        /// Write a message to the logging system
        /// </summary>
        /// <param name="loggerType">Type of logger to use</param>
        /// <param name="logLevel">Level of log reporting</param>
        /// <param name="message">Message to be logged</param>
        public static void WriteLog(LoggerTypes loggerType, LogLevels logLevel, string message)
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
        public static void WriteLog(LoggerTypes loggerType, LogLevels logLevel, string message, Exception ex)
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
        private static void WriteLogWork(LoggerTypes loggerType, LogLevels logLevel, string message, Exception ex)
        {
            ILog myLogger;

            if (clsGlobal.LinuxOS && (loggerType == LoggerTypes.LogDb || loggerType == LoggerTypes.LogSystem))
                loggerType = LoggerTypes.LogFile;
            else if (clsGlobal.OfflineMode && loggerType == LoggerTypes.LogDb)
                loggerType = LoggerTypes.LogFile;

            // Establish which logger will be used
            switch (loggerType)
            {
                case LoggerTypes.LogDb:
                    myLogger = m_DbLogger;
                    message = System.Net.Dns.GetHostName() + ": " + message;
                    break;
                case LoggerTypes.LogFile:
                    myLogger = m_FileLogger;
                    // Check to determine if a new file should be started
                    var testFileDate = DateTime.Now.ToString("MM-dd-yyyy");
                    if (!string.Equals(testFileDate, m_FileDate))
                    {
                        m_FileDate = testFileDate;
                        ChangeLogFileName();
                    }
                    break;
                case LoggerTypes.LogSystem:
                    myLogger = m_SysLogger;
                    break;
                default:
                    throw new Exception("Invalid logger type specified");
            }

            if (myLogger == null)
                return;

            // Send the log message
            switch (logLevel)
            {
                case LogLevels.DEBUG:
                    if (myLogger.IsDebugEnabled)
                    {
                        if (ex == null)
                        {
                            myLogger.Debug(message);
                        }
                        else
                        {
                            myLogger.Debug(message, ex);
                        }
                    }
                    break;
                case LogLevels.ERROR:
                    if (myLogger.IsErrorEnabled)
                    {
                        if (ex == null)
                        {
                            myLogger.Error(message);
                        }
                        else
                        {
                            myLogger.Error(message, ex);
                        }
                    }
                    break;
                case LogLevels.FATAL:
                    if (myLogger.IsFatalEnabled)
                    {
                        if (ex == null)
                        {
                            myLogger.Fatal(message);
                        }
                        else
                        {
                            myLogger.Fatal(message, ex);
                        }
                    }
                    break;
                case LogLevels.INFO:
                    if (myLogger.IsInfoEnabled)
                    {
                        if (ex == null)
                        {
                            myLogger.Info(message);
                        }
                        else
                        {
                            myLogger.Info(message, ex);
                        }
                    }
                    break;
                case LogLevels.WARN:
                    if (myLogger.IsWarnEnabled)
                    {
                        if (ex == null)
                        {
                            myLogger.Warn(message);
                        }
                        else
                        {
                            myLogger.Warn(message, ex);
                        }
                    }
                    break;
                default:
                    throw new Exception("Invalid log level specified");
            }

            if (logLevel <= LogLevels.ERROR)
            {
                m_MostRecentErrorMessage = message;
            }
        }

        /// <summary>
        /// Changes the base log file name
        /// </summary>
        public static void ChangeLogFileName()
        {
            ChangeLogFileName(m_BaseFileName + "_" + m_FileDate + ".txt");
        }

        /// <summary>
        /// Changes the base log file name
        /// </summary>
        /// <param name="fileName">Log file base name and path (relative to program folder)</param>
        /// <remarks>This method is called by the Mage, Ascore, and Multialign plugins</remarks>
        public static void ChangeLogFileName(string fileName)
        {
            // Get a list of appenders
            var appendList = FindAppenders(LOG_FILE_APPENDER);
            if (appendList == null)
            {
                WriteLog(LoggerTypes.LogSystem, LogLevels.WARN, "Unable to change file name. No appender found");
                return;
            }

            foreach (var selectedAppender in appendList)
            {
                // Convert the IAppender object to a FileAppender instance
                if (!(selectedAppender is FileAppender appenderToChange))
                {
                    WriteLog(LoggerTypes.LogSystem, LogLevels.ERROR, "Unable to convert appender");
                    return;
                }

                // Change the file name and activate change
                appenderToChange.File = fileName;
                appenderToChange.ActivateOptions();
            }
        }

        /// <summary>
        /// Gets the specified appender
        /// </summary>
        /// <param name="appenderName">Name of appender to find</param>
        /// <returns>List(IAppender) objects if found; null otherwise</returns>
        private static IEnumerable<IAppender> FindAppenders(string appenderName)
        {

            // Get a list of the current loggers
            var loggerList = LogManager.GetCurrentLoggers();
            if (loggerList.GetLength(0) < 1)
                return null;

            // Create a List of appenders matching the criteria for each logger
            var retList = new List<IAppender>();
            foreach (var testLogger in loggerList)
            {
                foreach (var testAppender in testLogger.Logger.Repository.GetAppenders())
                {
                    if (testAppender.Name == appenderName)
                        retList.Add(testAppender);
                }
            }

            // Return the list of appenders, if any found
            if (retList.Count > 0)
            {
                return retList;
            }

            return null;
        }

        /// <summary>
        /// Sets the file logging level via an integer value (Overloaded)
        /// </summary>
        /// <param name="logLevel">Integer corresponding to level (1-5, 5 being most verbose)</param>
        public static void SetFileLogLevel(int logLevel)
        {
            var logLevelEnumType = typeof(LogLevels);

            // Verify input level is a valid log level
            if (!Enum.IsDefined(logLevelEnumType, logLevel))
            {
                WriteLog(LoggerTypes.LogFile, LogLevels.ERROR, "Invalid value specified for level: " + logLevel);
                return;
            }

            // Convert input integer into the associated enum
            var logLevelEnum = (LogLevels)Enum.Parse(logLevelEnumType, logLevel.ToString(CultureInfo.InvariantCulture));

            SetFileLogLevel(logLevelEnum);
        }

        /// <summary>
        /// Sets file logging level based on enumeration (Overloaded)
        /// </summary>
        /// <param name="logLevel">LogLevels value defining level (Debug is most verbose)</param>
        public static void SetFileLogLevel(LogLevels logLevel)
        {
            var logger = (log4net.Repository.Hierarchy.Logger)m_FileLogger.Logger;

            switch (logLevel)
            {
                case LogLevels.DEBUG:
                    logger.Level = logger.Hierarchy.LevelMap["DEBUG"];
                    break;
                case LogLevels.ERROR:
                    logger.Level = logger.Hierarchy.LevelMap["ERROR"];
                    break;
                case LogLevels.FATAL:
                    logger.Level = logger.Hierarchy.LevelMap["FATAL"];
                    break;
                case LogLevels.INFO:
                    logger.Level = logger.Hierarchy.LevelMap["INFO"];
                    break;
                case LogLevels.WARN:
                    logger.Level = logger.Hierarchy.LevelMap["WARN"];
                    break;
            }
        }

        /// <summary>
        /// Creates a file appender
        /// </summary>
        /// <param name="logFileNameBase">Base name for log file</param>
        /// <returns>A configured file appender</returns>
        private static FileAppender CreateFileAppender(string logFileNameBase)
        {
            m_FileDate = DateTime.Now.ToString("MM-dd-yyyy");
            m_BaseFileName = logFileNameBase;

            log4net.Layout.PatternLayout layout;
            if (clsGlobal.LinuxOS | System.IO.Path.DirectorySeparatorChar == '/')
            {
                layout = new log4net.Layout.PatternLayout
                {
                    ConversionPattern = "%date{MM/dd/yyyy HH:mm:ss}, %message, %level,%newline"
                };
            }
            else
            {
                layout = new CustomPatternLayout
                {
                    ConversionPattern = "%date{MM/dd/yyyy HH:mm:ss}, %message, %level,%newline%stack"
                };

            }

            layout.ActivateOptions();

            var returnAppender = new FileAppender
            {
                Name = LOG_FILE_APPENDER,
                File = m_BaseFileName + "_" + m_FileDate + ".txt",
                AppendToFile = true,
                Layout = layout
            };

            returnAppender.ActivateOptions();

            return returnAppender;
        }

        /// <summary>
        /// Configures the file logger
        /// </summary>
        /// <param name="logFileName">Base name for log file</param>
        public static void CreateFileLogger(string logFileName)
        {
            var curLogger = (log4net.Repository.Hierarchy.Logger)m_FileLogger.Logger;
            m_FileAppender = CreateFileAppender(logFileName);
            curLogger.AddAppender(m_FileAppender);

            // The analysis manager determines when to log or not log based on internal logic
            // Set the LogLevel tracked by log4net to DEBUG so that all messages sent to this class are logged
            SetFileLogLevel(LogLevels.DEBUG);
        }

        /// <summary>
        /// Configures the database logger
        /// </summary>
        /// <param name="connStr">Database connection string</param>
        /// <param name="moduleName">Module name used by logger</param>
        /// <param name="isBeforeMgrControlParams">Should be True when this function is called before parameters are retrieved from the Manager Control DB</param>
        public static void CreateDbLogger(string connStr, string moduleName, bool isBeforeMgrControlParams)
        {
            var curLogger = (log4net.Repository.Hierarchy.Logger)m_DbLogger.Logger;
            curLogger.Level = log4net.Core.Level.Info;

            if (isBeforeMgrControlParams)
            {
                curLogger.AddAppender(CreateDbAppender(connStr, moduleName, DB_LOGGER_NO_MGR_CONTROL_PARAMS));
            }
            else
            {
                curLogger.AddAppender(CreateDbAppender(connStr, moduleName, DB_LOGGER_MGR_CONTROL));
            }

            if (m_FileAppender == null)
            {
                return;
            }

            var addFileAppender = true;
            foreach (var appender in curLogger.Appenders)
            {
                if (ReferenceEquals(appender, m_FileAppender))
                {
                    addFileAppender = false;
                    break;
                }
            }

            if (addFileAppender)
            {
                curLogger.AddAppender(m_FileAppender);
            }
        }

        /// <summary>
        /// Remove the default database logger that was created when the program first started
        /// </summary>
        public static void RemoveDefaultDbLogger()
        {
            var curLogger = (log4net.Repository.Hierarchy.Logger)m_DbLogger.Logger;

            foreach (var item in curLogger.Appenders)
            {
                if (item.Name == DB_LOGGER_NO_MGR_CONTROL_PARAMS)
                {
                    curLogger.RemoveAppender(item);
                    item.Close();
                    break;
                }
            }
        }


        /// <summary>
        /// Creates a database appender
        /// </summary>
        /// <param name="connectionString">Database connection string</param>
        /// <param name="moduleName">Module name used by logger</param>
        /// <param name="appenderName">Appender name</param>
        /// <returns>ADONet database appender</returns>
        public static AdoNetAppender CreateDbAppender(string connectionString, string moduleName, string appenderName)
        {
            var returnAppender = new AdoNetAppender
            {
                BufferSize = 1,
                ConnectionType = "System.Data.SqlClient.SqlConnection, System.Data, Version=1.0.3300.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                ConnectionString = connectionString,
                CommandType = CommandType.StoredProcedure,
                CommandText = "PostLogEntry",
                Name = appenderName
            };

            // Type parameter
            var typeParam = new AdoNetAppenderParameter
            {
                ParameterName = "@type",
                DbType = DbType.String,
                Size = 50,
                Layout = CreateLayout("%level")
            };

            returnAppender.AddParameter(typeParam);

            // Message parameter
            var msgParam = new AdoNetAppenderParameter
            {
                ParameterName = "@message",
                DbType = DbType.String,
                Size = 4000,
                Layout = CreateLayout("%message")
            };

            returnAppender.AddParameter(msgParam);

            // PostedBy parameter
            var postByParam = new AdoNetAppenderParameter
            {
                ParameterName = "@postedBy",
                DbType = DbType.String,
                Size = 128,
                Layout = CreateLayout(moduleName)
            };

            returnAppender.AddParameter(postByParam);

            returnAppender.ActivateOptions();

            return returnAppender;
        }

        /// <summary>
        /// Creates a layout object for a Db appender parameter
        /// </summary>
        /// <param name="layoutStr">Name of parameter</param>
        /// <returns></returns>
        private static log4net.Layout.IRawLayout CreateLayout(string layoutStr)
        {
            var layoutConvert = new log4net.Layout.RawLayoutConverter();
            var returnLayout = new log4net.Layout.PatternLayout
            {
                ConversionPattern = layoutStr
            };

            returnLayout.ActivateOptions();

            var retItem = (log4net.Layout.IRawLayout)layoutConvert.ConvertFrom(returnLayout);

            if (retItem == null)
            {
                throw new ConversionNotSupportedException("Error converting a PatternLayout to IRawLayout");
            }

            return retItem;

        }

        #endregion

    }
}

