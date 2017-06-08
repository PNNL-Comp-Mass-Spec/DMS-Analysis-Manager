using System;
using AnalysisManagerBase;

namespace AnalysisManagerMultiAlign_AggregatorPlugIn
{
	public class clsAnalysisResourcesMultiAlignAggregator : clsAnalysisResources
	{

		public override AnalysisManagerBase.CloseOutType GetResources()
		{
			var SearchType = m_jobParams.GetParam("MultiAlignSearchType");

			if (string.IsNullOrEmpty(SearchType))
			{
				m_message = "Parameter MultiAlignSearchType must be \"_LCMSFeatures.txt\" or \"_isos.csv\"";
				return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
			}

			m_jobParams.AddResultFileExtensionToSkip(SearchType);

			return CloseOutType.CLOSEOUT_SUCCESS;

		}

	}
}
