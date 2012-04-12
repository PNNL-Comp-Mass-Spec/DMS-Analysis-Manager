using System;
using AnalysisManagerBase;

namespace AnalysisManager_MultiAlign_Aggregator_PlugIn
{
    public class clsAnalysisResourcesMultiAlignAggregator : clsAnalysisResources
    {

        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {           
           string SearchType = m_jobParams.GetParam("MultiAlignSearchType");

		   m_jobParams.AddResultFileExtensionToSkip(SearchType);

           return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;

        }

    }
}
