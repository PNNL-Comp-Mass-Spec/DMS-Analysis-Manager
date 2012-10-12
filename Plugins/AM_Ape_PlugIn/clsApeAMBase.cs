using System;
using System.Collections.Generic;
using System.Text;
using AnalysisManagerBase;

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
				ViperResults=0,
				PTDB=1,
				AMTTagDBAll=2,
				AMTTagDbJobs=3,
				ImproveDB=4,
				QRollupResults=5
			}

		#endregion

		#region Member Variables
		protected string mResultsDBFileName = "";

        protected string mWorkingDir;

        protected IJobParams mJobParms;

        protected IMgrParams mMgrParms;

		protected string mErrorMessage = string.Empty;

		#endregion

		#region "Properties"

		public string ErrorMessage
		{
			get { return mErrorMessage; }
		}

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

		protected void OnProgressChanged(string TaskDescription, float PctComplete) {
			if (ProgressChanged != null)
				ProgressChanged(this, new ProgressChangedEventArgs(TaskDescription, PctComplete));
		}	

		public class ProgressChangedEventArgs : EventArgs {
			public readonly string taskDescription;     // Current task
			public readonly float percentComplete;      // number between 0 and 100

			public ProgressChangedEventArgs(string strTaskDescription, float fPercentComplete) {
				taskDescription = strTaskDescription;
				percentComplete = fPercentComplete;
			}
		}

    }
}
