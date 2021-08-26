//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 04/17/2013
//
//*********************************************************************************************************

using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerResultsCleanupPlugin
{
    /// <summary>
    /// Retrieve resources for the Results Cleanup plugin
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisResourcesResultsCleanup : AnalysisResources
    {
        /// <summary>
        /// Obtains resources necessary for performing analysis results cleanup
        /// </summary>
        /// <remarks>No resources needed for performing results transfer. This method merely meets inheritance requirements</remarks>
        /// <returns>CloseOutType indicating success or failure</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }
    }
}
