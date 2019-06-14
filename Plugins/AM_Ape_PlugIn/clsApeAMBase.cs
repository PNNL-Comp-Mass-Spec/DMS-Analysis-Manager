using AnalysisManagerBase;
using PRISM.Logging;
using System;

namespace AnalysisManager_Ape_PlugIn
{
    class clsApeAMBase
    {
        #region "Event Delegates and Classes"
        public event ProgressChangedEventHandler ProgressChanged;
        public delegate void ProgressChangedEventHandler(object sender, ProgressChangedEventArgs e);
        #endregion

        #region Enums
        public enum eSqlServerToSqlLiteConversionMode
        {
            ViperResults = 0,
            PTDB = 1,
            AMTTagDBAll = 2,
            AMTTagDbJobs = 3,
            ImproveDB = 4,
            QRollupResults = 5
        }

        #endregion

        #region Member Variables
        protected string mResultsDBFileName;

        protected string mWorkingDir;

        protected IJobParams mJobParams;

        protected IMgrParams mMgrParams;

        protected string mErrorMessage = string.Empty;

        #endregion

        #region "Properties"

        public string ErrorMessage => mErrorMessage;

        #endregion

        #region Constructors

        public clsApeAMBase(IJobParams jobParams, IMgrParams mgrParams)
        {
            mJobParams = jobParams;
            mMgrParams = mgrParams;
            mResultsDBFileName = RequireJobParam("ResultsBaseName") + ".db3";
            mWorkingDir = RequireMgrParam("workdir");
        }

        #endregion


        #region Utility Methods

        public string RequireMgrParam(string paramName)
        {
            var val = mMgrParams.GetParam(paramName);
            if (string.IsNullOrEmpty(val))
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, string.Format("Required job parameter '{0}' was missing.", paramName));
            }
            return val;
        }

        public string RequireJobParam(string paramName)
        {
            var val = mJobParams.GetParam(paramName);
            if (string.IsNullOrEmpty(val))
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN, string.Format("Required job parameter '{0}' was missing.", paramName));
            }
            return val;
        }

        public string GetJobParam(string paramName)
        {
            return mJobParams.GetParam(paramName);
        }

        public string GetJobParam(string paramName, string defaultValue)
        {
            return mJobParams.GetJobParameter(paramName, defaultValue);
        }


        #endregion

        protected void OnProgressChanged(string TaskDescription, float PctComplete)
        {
            ProgressChanged?.Invoke(this, new ProgressChangedEventArgs(TaskDescription, PctComplete));
        }

        public class ProgressChangedEventArgs : EventArgs
        {
            public readonly string taskDescription;     // Current task
            public readonly float percentComplete;      // number between 0 and 100

            public ProgressChangedEventArgs(string strTaskDescription, float fPercentComplete)
            {
                taskDescription = strTaskDescription;
                percentComplete = fPercentComplete;
            }
        }

    }
}
