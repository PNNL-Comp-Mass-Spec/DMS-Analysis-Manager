using AnalysisManagerBase;
using AnalysisManagerBase.JobConfig;
using PRISM;

namespace AnalysisManager_Ape_PlugIn
{
    internal class ApeAMBase : EventNotifier
    {
        public enum SqlServerToSqlLiteConversionMode
        {
            ViperResults = 0,
            PTDB = 1,
            AMTTagDBAll = 2,
            AMTTagDbJobs = 3,
            ImproveDB = 4,
            QRollupResults = 5
        }

        protected string mResultsDBFileName;

        protected string mWorkingDir;

        protected IJobParams mJobParams;

        protected IMgrParams mMgrParams;

        protected string mErrorMessage = string.Empty;

        public string ErrorMessage => mErrorMessage;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="mgrParams"></param>
        public ApeAMBase(IJobParams jobParams, IMgrParams mgrParams)
        {
            mJobParams = jobParams;
            mMgrParams = mgrParams;
            mResultsDBFileName = RequireJobParam("ResultsBaseName") + ".db3";
            mWorkingDir = RequireMgrParam("WorkDir");
        }

        public string RequireMgrParam(string paramName)
        {
            var val = mMgrParams.GetParam(paramName);

            if (string.IsNullOrEmpty(val))
            {
                OnWarningEvent("Required job parameter '{0}' was missing.", paramName);
            }
            return val;
        }

        public string RequireJobParam(string paramName)
        {
            var val = mJobParams.GetParam(paramName);

            if (string.IsNullOrEmpty(val))
            {
                OnWarningEvent("Required job parameter '{0}' was missing.", paramName);
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

        public string GetJobParam(string sectionName, string paramName, string defaultValue)
        {
            return mJobParams.GetJobParameter(sectionName, paramName, defaultValue);
        }

        /// <summary>Progress update</summary>
        /// <param name="progressMessage">Progress message</param>
        /// <param name="percentComplete">Value between 0 and 100</param>
        protected void OnProgressChanged(string progressMessage, float percentComplete)
        {
            base.OnProgressUpdate(progressMessage, percentComplete);
        }
    }
}
