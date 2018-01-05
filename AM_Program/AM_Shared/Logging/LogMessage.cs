using System;


namespace AnalysisManagerBase.Logging
{
    /// <summary>
    /// Class for tracking individual messages to log
    /// </summary>
    public class LogMessage
    {
        /// <summary>
        /// Log level (aka log message type)
        /// </summary>
        public BaseLogger.LogLevels LogLevel { get; }

        /// <summary>
        /// Log message
        /// </summary>
        public string Message { get; }

        /// <summary>
        /// Exception associated with the message (may be null)
        /// </summary>
        public Exception MessageException { get; }

        /// <summary>
        /// Message date (UTC-based time)
        /// </summary>
        public DateTime MessageDateUTC { get; }

        /// <summary>
        /// Message date (Local time)
        /// </summary>
        public DateTime MessageDateLocal => MessageDateUTC.ToLocalTime();

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="logLevel"></param>
        /// <param name="message"></param>
        /// <param name="ex"></param>
        public LogMessage(BaseLogger.LogLevels logLevel, string message, Exception ex = null)
        {
            LogLevel = logLevel;
            Message = message;
            MessageException = ex;
            MessageDateUTC = DateTime.UtcNow;
        }

        /// <summary>
        /// Get the log message, formatted in the form Date, Message, LogType
        /// </summary>
        /// <param name="useLocalTime">When true, use the local time, otherwise use UTC time</param>
        /// <returns>Formatted message (does not include anything regarding MessageException)</returns>
        public string GetFormattedMessage(bool useLocalTime = true)
        {
            string timeStamp;
            if (useLocalTime)
                timeStamp = MessageDateLocal.ToString("MM/dd/yyyy HH:mm:ss");
            else
                timeStamp = MessageDateUTC.ToString("MM/dd/yyyy HH:mm:ss");

            return string.Format("{0}, {1}, {2}", timeStamp, Message, LogLevel.ToString());
        }

        /// <summary>
        /// The log message and log type, separated by a comma
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Message + ", " + LogLevel;
        }
    }
}
