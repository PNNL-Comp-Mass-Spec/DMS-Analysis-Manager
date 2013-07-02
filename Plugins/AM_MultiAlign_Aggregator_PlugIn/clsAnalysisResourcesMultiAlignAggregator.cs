using System;
using AnalysisManagerBase;

namespace AnalysisManagerMultiAlign_AggregatorPlugIn
{
	public class clsAnalysisResourcesMultiAlignAggregator : clsAnalysisResources
	{

		public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
		{
			string SearchType = m_jobParams.GetParam("MultiAlignSearchType");

			if (string.IsNullOrEmpty(SearchType))
			{
				m_message = "Parameter MultiAlignSearchType must be \"_LCMSFeatures.txt\" or \"_isos.csv\"";
				return IJobParams.CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
			}

			m_jobParams.AddResultFileExtensionToSkip(SearchType);

			return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;

		}

	}
}
