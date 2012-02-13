using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManager_MultiAlign_Aggregator_PlugIn
{
    public class clsAnalysisResourcesMultiAlignAggregator : clsAnalysisResources
    {

        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {
            //Clear out list of files to delete or keep when packaging the blnSuccesss
            clsGlobal.ResetFilesToDeleteOrKeep();
           string SearchType = m_jobParams.GetParam("MultiAlignSearchType");

           clsGlobal.m_FilesToDeleteExt.Add(SearchType);

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }

    }
}
