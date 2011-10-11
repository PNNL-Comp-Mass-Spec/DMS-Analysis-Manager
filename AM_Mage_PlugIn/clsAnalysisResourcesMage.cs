using System;
using AnalysisManagerBase;
using Mage;

namespace AnalysisManager_Mage_PlugIn
{

    public class clsAnalysisResourcesMage : clsAnalysisResources
    {


        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {
              return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }
 
    }
}