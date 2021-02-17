using AnalysisManagerBase;
using Mage;

namespace AnalysisManager_AScore_PlugIn
{
    /// <summary>
    /// Class for managing IJobParams object
    /// </summary>
    public class JobParameters
    {
        private readonly IJobParams mJobParms;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParms"></param>
        public JobParameters(IJobParams jobParms)
        {
            mJobParms = jobParms;
        }

        /// <summary>
        /// Verify that a job parameter is defined
        /// </summary>
        /// <param name="paramName"></param>
        public string RequireJobParam(string paramName)
        {
            var val = mJobParms.GetParam(paramName);
            if (string.IsNullOrWhiteSpace(val))
            {
                throw new MageException(string.Format("Required job parameter '{0}' was missing.", paramName));
            }
            return val;
        }

        /// <summary>
        /// Get a job parameter
        /// </summary>
        /// <param name="paramName"></param>
        public string GetJobParam(string paramName)
        {
            return mJobParms.GetParam(paramName);
        }

        /// <summary>
        /// Get a job parameter
        /// </summary>
        /// <param name="paramName"></param>
        /// <param name="defaultValue"></param>
        public string GetJobParam(string paramName, string defaultValue)
        {
            var val = mJobParms.GetParam(paramName);
            if (string.IsNullOrWhiteSpace(val))
                val = defaultValue;
            return val;
        }
    }
}
