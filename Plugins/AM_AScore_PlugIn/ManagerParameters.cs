using AnalysisManagerBase;
using Mage;

namespace AnalysisManager_AScore_PlugIn
{

    // class for managing IMgrParams object
    public class ManagerParameters
	{
		private readonly IMgrParams mMgrParms;

		public ManagerParameters(IMgrParams mgrParms)
		{
			mMgrParms = mgrParms;
		}

		public string RequireMgrParam(string paramName)
		{
			var val = mMgrParms.GetParam(paramName);
			if (string.IsNullOrWhiteSpace(val))
			{
				throw new MageException(string.Format("Required manager parameter '{0}' was missing.", paramName));
			}
			return val;
		}
	}


}
