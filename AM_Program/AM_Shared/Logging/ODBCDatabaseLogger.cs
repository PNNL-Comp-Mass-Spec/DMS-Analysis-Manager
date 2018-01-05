using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Threading;

namespace AnalysisManagerBase.Logging
{
    /// <summary>
    /// Logs messages to a database by calling a stored procedure
    /// Connect using an ODBC driver
    /// </summary>
    /// <remarks>Connect using an ODBC driver</remarks>
    public sealed class ODBCDatabaseLogger : DatabaseLogger
    {
        #region "Member variables"

        private static readonly ConcurrentQueue<LogMessage> mMessageQueue = new ConcurrentQueue<LogMessage>();

        private static bool mQueueLoggerInitialized;

        private static readonly List<string> mMessageQueueEntryFlag = new List<string>();

        private static readonly Timer mQueueLogger = new Timer(LogMessagesCallback, null, 0, 0);

        /// <summary>
        /// Tracks the number of successive dequeue failures
        /// </summary>
        private static int mFailedDequeueEvents;

        #endregion

        #region "Properties"

        /// <summary>
        /// ODBC style connection string
        /// </summary>
        public static string ConnectionString { get; private set; }

        /// <summary>
        /// Program name to pass to the PostedBy field when contacting the database
        /// </summary>
        public static string ModuleName { get; set; } = "ODBCDatabaseLogger";

        /// <summary>
        /// Stored procedure where log messages will be posted
        /// </summary>
        public static string StoredProcedureName { get; private set; }

        private static OdbcParameter LogTypeParam { get; set; }

        private static OdbcParameter MessageParam { get; set; }

        private static OdbcParameter PostedByParam { get; set; }

        #endregion

        /// <summary>
        /// Constructor when the connection info is unknown
        /// </summary>
        /// <param name="logLevel"></param>
        /// <remarks>No database logging will occur until ChangeConnectionInfo is called</remarks>
        public ODBCDatabaseLogger(LogLevels logLevel = LogLevels.INFO) : this("", "", "", "", "", "")
        {
            LogLevel = logLevel;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="moduleName">Program name to pass to the postedByParamName field when contacting the database</param>
        /// <param name="connectionString">ODBC-style connection string</param>
        /// <param name="storedProcedure">Stored procedure to call</param>
        /// <param name="logTypeParamName">LogType parameter name (string representation of logLevel</param>
        /// <param name="messageParamName">Message parameter name</param>
        /// <param name="postedByParamName">Log source parameter name</param>
        /// <param name="logTypeParamSize">LogType parameter size</param>
        /// <param name="messageParamSize">Message parameter size</param>
        /// <param name="postedByParamSize">Log source parameter size</param>
        /// <param name="logLevel">Log level</param>
        public ODBCDatabaseLogger(
            string moduleName,
            string connectionString,
            string storedProcedure,
            string logTypeParamName,
            string messageParamName,
            string postedByParamName,
            int logTypeParamSize = 128,
            int messageParamSize = 4000,
            int postedByParamSize = 128,
            LogLevels logLevel = LogLevels.INFO)
        {
            ChangeConnectionInfo(
                moduleName, connectionString, storedProcedure,
                logTypeParamName, messageParamName, postedByParamName,
                logTypeParamSize, messageParamSize, postedByParamSize);

            LogLevel = logLevel;

            if (mQueueLoggerInitialized)
                return;

            ShowTraceMessage("Starting the ODBCDatabaseLogger QueueLogger");

            mQueueLoggerInitialized = true;
            mQueueLogger.Change(500, LOG_INTERVAL_MILLISECONDS);
        }

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
        public override void ChangeConnectionInfo(
            string moduleName,
            string connectionString,
            string storedProcedure,
            string logTypeParamName,
            string messageParamName,
            string postedByParamName,
            int logTypeParamSize = 128,
            int messageParamSize = 4000,
            int postedByParamSize = 128)
        {
            ModuleName = moduleName;

            ConnectionString = connectionString;
            StoredProcedureName = storedProcedure;
            LogTypeParam = new OdbcParameter("@" + logTypeParamName, OdbcType.VarChar, logTypeParamSize);
            MessageParam = new OdbcParameter("@" + messageParamName, OdbcType.VarChar, messageParamSize);
            PostedByParam = new OdbcParameter("@" + postedByParamName, OdbcType.VarChar, postedByParamSize);
        }

        /// <summary>
        /// Convert a .NET framework SQL Server connection string to an ODBC-style connection string
        /// </summary>
        /// <param name="sqlServerConnectionString">SQL Server connection string</param>
        /// <param name="odbcDriverName">
        /// Typical values are:
        ///   "SQL Server Native Client 11.0" for SQL Server 2012
        ///   "SQL Server Native Client 10.0" for SQL Server 2008
        ///   "SQL Native Client"             for SQL Server 2005
        ///   "SQL Server"                    for SQL Server 2000
        /// </param>
        /// <returns></returns>
        public static string ConvertSqlServerConnectionStringToODBC(string sqlServerConnectionString, string odbcDriverName = "SQL Server Native Client 11.0")
        {
            // Example connection strings available online at:
            // https://www.connectionstrings.com/sql-server/

            // Integrated authentication:
            // Convert from one of these forms:
            //   Server=myServerAddress;Database=myDataBase;Trusted_Connection=True;
            //   Data Source=myServerAddress;Initial Catalog=myDataBase;Integrated Security=SSPI
            // To ODBC style:
            //   Driver={SQL Server Native Client 11.0};Server=myServerAddress;Database=myDataBase;Trusted_Connection=yes;


            // Standard security:
            // Convert from one of these forms:
            //   Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;
            //   Data Source=myServerAddress;Initial Catalog=myDataBase;User ID=myDomain\myUsername;Password=myPassword
            // To ODBC style:
            //   Driver={SQL Server Native Client 11.0};Server=myServerAddress;Database=myDataBase;Uid=myUsername;Pwd=myPassword;

            var settingsList = sqlServerConnectionString.Split(';');
            var keyParts = new[] { '=' };

            var sqlServerSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var setting in settingsList)
            {
                var settingParts = setting.Split(keyParts, 2);
                if (settingParts.Length < 2)
                    continue;

                var settingKey = settingParts[0].Trim();
                var settingValue = settingParts[1].Trim();

                if (string.IsNullOrWhiteSpace(settingKey))
                    continue;

                if (sqlServerSettings.ContainsKey(settingKey))
                    continue;

                sqlServerSettings.Add(settingKey, settingValue);
            }

            var userPassword = "";

            var namedUser = sqlServerSettings.TryGetValue("User Id", out var userId) &&
                            sqlServerSettings.TryGetValue("Password", out userPassword);

            var odbcConnectionParts = new List<string> {
                "Driver={" + odbcDriverName + "}"
            };

            if (sqlServerSettings.TryGetValue("Server", out var serverName))
            {
                if (sqlServerSettings.TryGetValue("Database", out var databaseName))
                {
                    odbcConnectionParts.Add("Server=" + serverName);
                    odbcConnectionParts.Add("Database=" + databaseName);
                }
                else
                {
                    PRISM.ConsoleMsgUtils.ShowError(
                        "SQL Server connection string had 'Server' but did not have 'Database': " + sqlServerConnectionString);
                    return string.Empty;
                }
            }

            if (sqlServerSettings.TryGetValue("Data Source", out var dataSource))
            {
                if (sqlServerSettings.TryGetValue("Initial Catalog", out var databaseName))
                {
                    odbcConnectionParts.Add("Server=" + dataSource);
                    odbcConnectionParts.Add("Database=" + databaseName);
                }
                else
                {
                    PRISM.ConsoleMsgUtils.ShowError(
                        "SQL Server connection string had 'Data Source' but did not have 'Initial catalog': " + sqlServerConnectionString);
                    return string.Empty;
                }
            }

            if (odbcConnectionParts.Count <= 1)
            {
                PRISM.ConsoleMsgUtils.ShowError(
                    "SQL Server connection string did not have 'Server' or 'Data Source': " + sqlServerConnectionString);
                return string.Empty;
            }

            if (namedUser)
            {
                odbcConnectionParts.Add(string.Format("Uid={0};Pwd={1}", userId, userPassword));
            }
            else
            {
                odbcConnectionParts.Add("Trusted_Connection=yes");
            }

            var odbcConnectionString = string.Join(";", odbcConnectionParts) + ";";

            return odbcConnectionString;
        }

        /// <summary>
        /// Callback invoked by the mQueueLogger timer
        /// </summary>
        /// <param name="state"></param>
        private static void LogMessagesCallback(object state)
        {
            ShowTraceMessage("ODBCDatabaseLogger.mQueueLogger callback raised");
            StartLogQueuedMessages();
        }

        private static void LogQueuedMessages()
        {
            var messagesWritten = 0;

            try
            {
                ShowTraceMessage(string.Format("ODBCDatabaseLogger connecting to {0}", ConnectionString));

                using (var odbcConnection = new OdbcConnection(ConnectionString))
                {
                    odbcConnection.Open();

                    // Set up the command object prior to SP execution
                    // The syntax for calling procedure PostLogEntry is
                    // {call PostLogEntry (?,?,?)}

                    var spCmdText = "{call " + StoredProcedureName + " (?,?,?)}";

                    var spCmd = new OdbcCommand(spCmdText) {
                        CommandType = CommandType.StoredProcedure
                    };

                    // Not including this parameter because it doesn't get populated when we use ExecuteNonQuery
                    // spCmd.Parameters.Add(new OdbcParameter("@Return", OdbcType.Int)).Direction = ParameterDirection.ReturnValue;

                    var logTypeParam = spCmd.Parameters.Add(LogTypeParam);
                    var logMessageParam = spCmd.Parameters.Add(MessageParam);
                    spCmd.Parameters.Add(PostedByParam).Value = ModuleName;

                    spCmd.Connection = odbcConnection;
                    spCmd.CommandTimeout = TIMEOUT_SECONDS;

                    while (!mMessageQueue.IsEmpty)
                    {
                        if (!mMessageQueue.TryDequeue(out var logMessage))
                        {
                            mFailedDequeueEvents += 1;
                            LogDequeueError(mFailedDequeueEvents, mMessageQueue.Count);
                            return;
                        }

                        mFailedDequeueEvents = 0;

                        if (logMessage.LogLevel == LogLevels.ERROR || logMessage.LogLevel == LogLevels.FATAL)
                        {
                            MostRecentErrorMessage = logMessage.Message;
                        }

                        if (string.IsNullOrWhiteSpace(ConnectionString) || string.IsNullOrWhiteSpace(StoredProcedureName) || MessageParam == null)
                            continue;

                        logTypeParam.Value = LogLevelToString(logMessage.LogLevel);
                        logMessageParam.Value = logMessage.Message;

                        var retryCount = 2;

                        while (retryCount > 0)
                        {
                            var returnValue = 0;

                            try
                            {
                                spCmd.ExecuteNonQuery();
                                // returnValue = Convert.ToInt32(spCmd.Parameters["@Return"].Value);

                                messagesWritten++;
                                break;
                            }
                            catch (Exception ex)
                            {
                                --retryCount;
                                var errorMessage = "Exception calling stored procedure " +
                                                   spCmd.CommandText + ": " + ex.Message +
                                                   "; resultCode = " + returnValue + "; Retry count = " + retryCount + "; " +
                                                   PRISM.Utilities.GetExceptionStackTrace(ex);

                                if (retryCount == 0)
                                    FileLogger.WriteLog(LogLevels.ERROR, errorMessage);
                                else
                                    FileLogger.WriteLog(LogLevels.WARN, errorMessage);

                                if (!ex.Message.StartsWith("Could not find stored procedure " + spCmd.CommandText))
                                {
                                    // Try again
                                }
                                else
                                    break;
                            }

                        }


                    }
                }

                ShowTraceMessage(string.Format("ODBCDatabaseLogger connection closed; wrote {0} messages", messagesWritten));

            }
            catch (Exception ex)
            {
                PRISM.ConsoleMsgUtils.ShowError("Error writing queued log messages to the database using ODBC: " + ex.Message, ex, false, false);
            }

        }

        /// <summary>
        /// Disable database logging
        /// </summary>
        public override void RemoveConnectionInfo()
        {
            ChangeConnectionInfo(ModuleName, "", "", "", "", "");
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
        /// Log a message (regardless of base.LogLevel)
        /// </summary>
        /// <param name="logMessage"></param>
        public override void WriteLog(LogMessage logMessage)
        {
            mMessageQueue.Enqueue(logMessage);

            if (EchoMessagesToFileLogger)
                FileLogger.WriteLog(logMessage);
        }

        #endregion

        /// <summary>
        /// Class is disposing; write out any queued messages
        /// </summary>
        ~ODBCDatabaseLogger()
        {
            ShowTraceMessage("Disposing ODBCDatabaseLogger");
            StartLogQueuedMessages();
        }
    }
}
