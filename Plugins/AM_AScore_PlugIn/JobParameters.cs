using AnalysisManagerBase;
using Mage;

namespace AnalysisManager_AScore_PlugIn
{

	/// <summary>
	/// Class for managing IJobParams object 
	/// </summary>
	public class JobParameters
	{
		protected IJobParams mJobParms;

		public JobParameters(IJobParams jobParms)
		{
			mJobParms = jobParms;
		}

		public string RequireJobParam(string paramName)
		{
			var val = mJobParms.GetParam(paramName);
			if (string.IsNullOrWhiteSpace(val))
			{
				throw new MageException(string.Format("Required job parameter '{0}' was missing.", paramName));
			}
			return val;
		}

		public string GetJobParam(string paramName)
		{
			return mJobParms.GetParam(paramName);
		}

		public string GetJobParam(string paramName, string defaultValue)
		{
			var val = mJobParms.GetParam(paramName);
			if (string.IsNullOrWhiteSpace(val))
				val = defaultValue;
			return val;
		}
	}
}
