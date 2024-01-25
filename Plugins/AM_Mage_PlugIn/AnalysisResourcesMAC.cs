using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManager_Mage_PlugIn
{
    /// <summary>
    /// Retrieve resources for the MAC plugin
    /// </summary>
    public class AnalysisResourcesMAC : AnalysisResources
    {
        // Ignore Spelling: mage

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            return result;
        }
    }
}
