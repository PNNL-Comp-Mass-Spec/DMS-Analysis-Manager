﻿using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMultiAlign_AggregatorPlugIn
{
    /// <summary>
    /// Retrieve resources for the MultiAlign Aggregator plugin
    /// </summary>
    public class AnalysisResourcesMultiAlignAggregator : AnalysisResources
    {
        // Ignore Spelling: Aggregator

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            var searchType = mJobParams.GetParam("MultiAlignSearchType");

            if (string.IsNullOrEmpty(searchType))
            {
                mMessage = "Parameter MultiAlignSearchType must be \"_LCMSFeatures.txt\" or \"_isos.csv\"";
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            mJobParams.AddResultFileExtensionToSkip(searchType);

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
