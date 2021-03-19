using AnalysisManagerBase;
using PRISM.Logging;
using System;
using AnalysisManagerBase.JobConfig;
using PRISM;

namespace AnalysisManager_Ape_PlugIn
{
    internal class ApeAMBase : EventNotifier
    {
        #region Enums
        public enum SqlServerToSqlLiteConversionMode
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

        public ApeAMBase(IJobParams jobParams, IMgrParams mgrParams)
        {
            mJobParams = jobParams;
            mMgrParams = mgrParams;
            mResultsDBFileName = RequireJobParam("ResultsBaseName") + ".db3";
            mWorkingDir = RequireMgrParam("WorkDir");
        }

        #endregion

        #region Utility Methods

        public string RequireMgrParam(string paramName)
        {
            var val = mMgrParams.GetParam(paramName);
            if (string.IsNullOrEmpty(val))
            {
                OnWarningEvent(string.Format("Required job parameter '{0}' was missing.", paramName));
            }
            return val;
        }

        public string RequireJobParam(string paramName)
        {
            var val = mJobParams.GetParam(paramName);
            if (string.IsNullOrEmpty(val))
            {
                OnWarningEvent(string.Format("Required job parameter '{0}' was missing.", paramName));
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

        /// <summary>Progress update</summary>
        /// <param name="progressMessage">Progress message</param>
        /// <param name="percentComplete">Value between 0 and 100</param>
        protected void OnProgressChanged(string progressMessage, float percentComplete)
        {
            base.OnProgressUpdate(progressMessage, percentComplete);
        }
    }
}
