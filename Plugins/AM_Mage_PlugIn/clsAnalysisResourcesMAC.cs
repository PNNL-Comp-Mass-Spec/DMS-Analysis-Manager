using AnalysisManagerBase;

namespace AnalysisManager_Mage_PlugIn {

    public class clsAnalysisResourcesMAC : clsAnalysisResources {

        public override IJobParams.CloseOutType GetResources() {
            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
