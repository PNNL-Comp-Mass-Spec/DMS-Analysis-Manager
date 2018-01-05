using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;

namespace AnalysisManagerBase.Logging
{
    /// <summary>
    /// Logs messages to a database by calling a stored procedure
    /// Connect using System.Data.SqlClient
    /// </summary>
    /// <remarks>Can only log to a single database at a time</remarks>
    public sealed class SQLServerDatabaseLogger : DatabaseLogger
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
        public static string ModuleName { get; set; } = "SQLServerDatabaseLogger";

        /// <summary>
        /// Stored procedure where log messages will be posted
        /// </summary>
        public static string StoredProcedureName { get; private set; }

        private static SqlParameter LogTypeParam { get; set; }

        private static SqlParameter MessageParam { get; set; }

        private static SqlParameter PostedByParam { get; set; }

        #endregion

        /// <summary>
        /// Constructor when the connection info is unknown
        /// </summary>
        /// <param name="logLevel"></param>
        /// <remarks>No database logging will occur until ChangeConnectionInfo is called</remarks>
        public SQLServerDatabaseLogger(LogLevels logLevel = LogLevels.INFO) : this("", "", "", "", "", "")
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
        public SQLServerDatabaseLogger(
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

            ShowTraceMessage("Starting the SQLServerDatabaseLogger QueueLogger");

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
            LogTypeParam = new SqlParameter(logTypeParamName, SqlDbType.VarChar, logTypeParamSize);
            MessageParam = new SqlParameter(messageParamName, SqlDbType.VarChar, messageParamSize);
            PostedByParam = new SqlParameter(postedByParamName, SqlDbType.VarChar, postedByParamSize);
        }

        /// <summary>
        /// Callback invoked by the mQueueLogger timer
        /// </summary>
        /// <param name="state"></param>
        private static void LogMessagesCallback(object state)
        {
            ShowTraceMessage("SQLServerDatabaseLogger.mQueueLogger callback raised");
            StartLogQueuedMessages();
        }

        private static void LogQueuedMessages()
        {
            var messagesWritten = 0;

            try
            {

                ShowTraceMessage(string.Format("SQLServerDatabaseLogger connecting to {0}", ConnectionString));

                using (var sqlConnection = new SqlConnection(ConnectionString))
                {
                    sqlConnection.Open();

                    var spCmd = new SqlCommand(StoredProcedureName) {
                        CommandType = CommandType.StoredProcedure
                    };

                    spCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;

                    var logTypeParam = spCmd.Parameters.Add(LogTypeParam);
                    var logMessageParam = spCmd.Parameters.Add(MessageParam);
                    spCmd.Parameters.Add(PostedByParam).Value = ModuleName;

                    spCmd.Connection = sqlConnection;
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
                                returnValue = Convert.ToInt32(spCmd.Parameters["@Return"].Value);

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

                ShowTraceMessage(string.Format("SQLServerDatabaseLogger connection closed; wrote {0} messages", messagesWritten));

            }
            catch (Exception ex)
            {
                PRISM.ConsoleMsgUtils.ShowError("Error writing queued log messages to the database using SQL Server: " + ex.Message, ex, false, false);
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
        ~SQLServerDatabaseLogger()
        {
            ShowTraceMessage("Disposing SQLServerDatabaseLogger");
            StartLogQueuedMessages();
        }
    }
}
