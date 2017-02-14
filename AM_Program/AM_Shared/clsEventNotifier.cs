
using System;

namespace AnalysisManagerBase
{
    public abstract class clsEventNotifier
    {

        #region "Events and Event Handlers"

        public event DebugEventEventHandler DebugEvent;
        public delegate void DebugEventEventHandler(string strMessage);

        public event ErrorEventEventHandler ErrorEvent;
        public delegate void ErrorEventEventHandler(string strMessage, Exception ex);

        public event ProgressUpdateEventHandler ProgressUpdate;
        public delegate void ProgressUpdateEventHandler(string progressMessage, float percentComplete);

        public event StatusEventEventHandler StatusEvent;
        public delegate void StatusEventEventHandler(string strMessage);

        public event WarningEventEventHandler WarningEvent;
        public delegate void WarningEventEventHandler(string strMessage);

        /// <summary>
        /// Report a debug message
        /// </summary>
        /// <param name="strMessage"></param>
        protected void OnDebugEvent(string strMessage)
        {
            DebugEvent?.Invoke(strMessage);
        }

        /// <summary>
        /// Report an error
        /// </summary>
        /// <param name="strMessage"></param>
        protected void OnErrorEvent(string strMessage)
        {
            ErrorEvent?.Invoke(strMessage, null);
        }

        /// <summary>
        /// Report an error
        /// </summary>
        /// <param name="strMessage"></param>
        /// <param name="ex">Exception (allowed to be nothing)</param>
        protected void OnErrorEvent(string strMessage, Exception ex)
        {
            ErrorEvent?.Invoke(strMessage, ex);
        }

        /// <summary>
        /// Progress udpate
        /// </summary>
        /// <param name="progressMessage">Progress message</param>
        /// <param name="percentComplete">Value between 0 and 100</param>
        protected void OnProgressUpdate(string progressMessage, float percentComplete)
        {
            ProgressUpdate?.Invoke(progressMessage, percentComplete);
        }

        /// <summary>
        /// Report a status message
        /// </summary>
        /// <param name="strMessage"></param>
        protected void OnStatusEvent(string strMessage)
        {
            StatusEvent?.Invoke(strMessage);
        }

        /// <summary>
        /// Report a warning
        /// </summary>
        /// <param name="strMessage"></param>
        protected void OnWarningEvent(string strMessage)
        {
            WarningEvent?.Invoke(strMessage);
        }

        /// <summary>
        /// Use this method to chain events between classes
        /// </summary>
        /// <param name="oProcessingClass"></param>
        protected void RegisterEvents(clsEventNotifier oProcessingClass)
        {
            oProcessingClass.DebugEvent += OnDebugEvent;
            oProcessingClass.StatusEvent += OnStatusEvent;
            oProcessingClass.ErrorEvent += OnErrorEvent;
            oProcessingClass.WarningEvent += OnWarningEvent;
            oProcessingClass.ProgressUpdate += OnProgressUpdate;
        }

        #endregion
    }
}
