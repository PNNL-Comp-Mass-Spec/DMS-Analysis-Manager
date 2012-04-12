using System;
using System.Collections.Generic;
using AnalysisManagerBase;

namespace AnalysisManager_MAC {

    public class clsAnalysisResourcesMAC : clsAnalysisResources {

        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources() {
            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
