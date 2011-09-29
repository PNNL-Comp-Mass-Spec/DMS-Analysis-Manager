using System;
using AnalysisManagerBase;
using PRISM;
using Mage;

namespace AnalysisManager_MageExtractor_PlugIn
{

    public class clsAnalysisResourcesMageExtractor : clsAnalysisResources
    {


        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {
              return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }
 
    }
}