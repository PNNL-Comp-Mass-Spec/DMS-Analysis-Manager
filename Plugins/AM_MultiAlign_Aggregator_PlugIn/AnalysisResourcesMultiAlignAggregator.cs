using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMultiAlign_AggregatorPlugIn
{
    /// <summary>
    /// Retrieve resources for the MultiAlign Aggregator plugin
    /// </summary>
    public class AnalysisResourcesMultiAlignAggregator : AnalysisResources
    {
        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            var SearchType = mJobParams.GetParam("MultiAlignSearchType");

            if (string.IsNullOrEmpty(SearchType))
            {
                mMessage = "Parameter MultiAlignSearchType must be \"_LCMSFeatures.txt\" or \"_isos.csv\"";
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            mJobParams.AddResultFileExtensionToSkip(SearchType);

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
