
using System;

namespace AnalysisManagerBase
{

    public abstract class clsAnalysisMgrBase : clsLoggerBase
    {

        private DateTime m_LastLockQueueWaitTimeLog = DateTime.UtcNow;

        private DateTime m_LockQueueWaitTimeStart = DateTime.UtcNow;

        protected PRISM.Files.clsFileTools m_FileTools;

        protected string m_message;

        private readonly string m_derivedClassName;
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="derivedClassName"></param>
        protected clsAnalysisMgrBase(string derivedClassName)
        {
            m_derivedClassName = derivedClassName;
        }

        private bool IsLockQueueLogMessageNeeded(ref DateTime dtLockQueueWaitTimeStart, ref DateTime dtLastLockQueueWaitTimeLog)
        {

            int intWaitTimeLogIntervalSeconds;

            if (dtLockQueueWaitTimeStart == DateTime.MinValue)
                dtLockQueueWaitTimeStart = DateTime.UtcNow;

            var waitTimeMinutes = DateTime.UtcNow.Subtract(dtLockQueueWaitTimeStart).TotalMinutes;

            if (waitTimeMinutes >= 30)
            {
                intWaitTimeLogIntervalSeconds = 240;
            } else if (waitTimeMinutes >= 15)
            {
                intWaitTimeLogIntervalSeconds = 120;
            } else if (waitTimeMinutes >= 5)
            {
                intWaitTimeLogIntervalSeconds = 60;
            }
            else
            {
                intWaitTimeLogIntervalSeconds = 30;
            }

            if (DateTime.UtcNow.Subtract(dtLastLockQueueWaitTimeLog).TotalSeconds >= intWaitTimeLogIntervalSeconds)
            {
                return true;
            }

            return false;
        }

        protected void InitFileTools(string mgrName, short debugLevel)
        {
            ResetTimestampForQueueWaitTimeLogging();
            m_FileTools = new PRISM.Files.clsFileTools(mgrName, debugLevel);
            m_FileTools.LockQueueTimedOut += m_FileTools_LockQueueTimedOut;
            m_FileTools.LockQueueWaitComplete += m_FileTools_LockQueueWaitComplete;
            m_FileTools.WaitingForLockQueue += m_FileTools_WaitingForLockQueue;
            m_FileTools.DebugEvent += m_FileTools_DebugEvent;
            m_FileTools.WarningEvent += m_FileTools_WarningEvent;
        }

        /// <summary>
        /// Update m_message with an error message and record the error in the manager's log file
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="logToDb">When true, log the message to the database and the local log file</param>
        protected override void LogError(string errorMessage, bool logToDb = false)
        {
            m_message = clsGlobal.AppendToComment(m_message, errorMessage);
            base.LogError(errorMessage, logToDb);
        }

        /// <summary>
        /// Update m_message with an error message and record the error in the manager's log file
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="ex">Exception to log</param>
        protected override void LogError(string errorMessage, Exception ex)
        {
            m_message = clsGlobal.AppendToComment(m_message, errorMessage);
            base.LogError(errorMessage, ex);
        }

        /// <summary>
        /// Update m_message with an error message and record the error in the manager's log file
        /// Also write the detailed error message to the local log file
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="detailedMessage">Detailed error message</param>
        /// <param name="logToDb">True to log this error to the database</param>
        protected void LogError(string errorMessage, string detailedMessage, bool logToDb = false)
        {
            this.LogError(errorMessage, logToDb);

            if (string.IsNullOrEmpty(detailedMessage))
                return;

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, detailedMessage);
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine(detailedMessage);
            Console.ResetColor();
        }

        protected void ResetTimestampForQueueWaitTimeLogging()
        {
            m_LastLockQueueWaitTimeLog = DateTime.UtcNow;
            m_LockQueueWaitTimeStart = DateTime.UtcNow;
        }

        #region "Event Handlers"

        private void m_FileTools_DebugEvent(string currentTask, string taskDetail)
        {
            if (m_DebugLevel >= 1)
            {
                Console.WriteLine("  " + currentTask);
                Console.WriteLine("   " + taskDetail);

                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, currentTask + "; " + taskDetail);
                }
                else
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, currentTask);
                }
            }
        }

        private void m_FileTools_WarningEvent(string warningMessage, string warningDetail)
        {
            if (m_DebugLevel >= 1)
            {
                Console.WriteLine(warningMessage);
                Console.WriteLine("  " + warningDetail);

                string msg;
                if (m_DebugLevel >= 2)
                {
                    msg = warningMessage + "; " + warningDetail;
                }
                else
                {
                    msg = warningMessage;
                }
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
            }
        }

        private void m_FileTools_LockQueueTimedOut(string sourceFilePath, string targetFilePath, double waitTimeMinutes)
        {
            if (m_DebugLevel >= 1)
            {
                var msg = "Lockfile queue timed out after " + waitTimeMinutes.ToString("0") + " minutes " + "(" + m_derivedClassName + "); Source=" + sourceFilePath + ", Target=" + targetFilePath;
                Console.WriteLine(msg);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
            }
        }

        private void m_FileTools_LockQueueWaitComplete(string sourceFilePath, string targetFilePath, double waitTimeMinutes)
        {
            if (m_DebugLevel >= 1 && waitTimeMinutes >= 1)
            {
                var msg = "Exited lockfile queue after " + waitTimeMinutes.ToString("0") + " minutes (" + m_derivedClassName + "); will now copy file";
                Console.WriteLine(msg);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
            }
        }


        private void m_FileTools_WaitingForLockQueue(string sourceFilePath, string targetFilePath, int backlogSourceMB, int backlogTargetMB)
        {
            if (IsLockQueueLogMessageNeeded(ref m_LockQueueWaitTimeStart, ref m_LastLockQueueWaitTimeLog))
            {
                m_LastLockQueueWaitTimeLog = DateTime.UtcNow;
                if (m_DebugLevel >= 1)
                {
                    var msg = "Waiting for lockfile queue to fall below threshold (" + m_derivedClassName + "); " + "SourceBacklog=" + backlogSourceMB + " MB, " + "TargetBacklog=" + backlogTargetMB + " MB, " + "Source=" + sourceFilePath + ", Target=" + targetFilePath;
                    Console.WriteLine(msg);
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                }
            }

        }

        #endregion
    }
}