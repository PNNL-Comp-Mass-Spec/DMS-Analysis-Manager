using AnalysisManagerBase;

namespace AnalysisManager_Mage_PlugIn
{

    public class clsAnalysisResourcesMAC : clsAnalysisResources
    {

        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            return result;
        }
    }
}
