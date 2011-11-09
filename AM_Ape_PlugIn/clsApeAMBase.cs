using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase;

namespace AnalysisManager_Ape_PlugIn
{
    class clsApeAMBase
    {
        #region Member Variables
        protected string mResultsDBFileName = "";

        protected string mWorkingDir;

        protected IJobParams mJobParms;

        protected IMgrParams mMgrParms;

        #endregion


        #region Constructors

        public clsApeAMBase(IJobParams jobParms, IMgrParams mgrParms) {
            this.mJobParms = jobParms;
            this.mMgrParms = mgrParms;
            this.mResultsDBFileName = RequireJobParam("ResultsBaseName") + ".db3";
            this.mWorkingDir = RequireMgrParam("workdir");
        }

        #endregion


        #region Utility Methods

        public string RequireMgrParam(string paramName)
        {
            string val = mMgrParms.GetParam(paramName);
            if (string.IsNullOrEmpty(val))
            {
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, string.Format("Required job parameter '{0}' was missing.", paramName));
            }
            return val;
        }

        public string RequireJobParam(string paramName)
        {
            string val = mJobParms.GetParam(paramName);
			if (string.IsNullOrEmpty(val)) {
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, string.Format("Required job parameter '{0}' was missing.", paramName));
			}
			return val;
        }

        public string GetJobParam(string paramName)
        {
            return mJobParms.GetParam(paramName);
        }

        public string GetJobParam(string paramName, string defaultValue)
        {
            string val = mJobParms.GetParam(paramName);
			if (string.IsNullOrEmpty(val))
				val = defaultValue;

            return val;
        }


        #endregion




    }
}
