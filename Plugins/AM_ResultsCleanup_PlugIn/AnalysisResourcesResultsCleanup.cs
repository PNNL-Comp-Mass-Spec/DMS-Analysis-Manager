//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 04/17/2013
//
//*********************************************************************************************************

using AnalysisManagerBase;

namespace AnalysisManagerResultsCleanupPlugin
{
    /// <summary>
    /// Retrieve resources for the Results Cleanup plugin
    /// </summary>
    public class AnalysisResourcesResultsCleanup : AnalysisResources
    {
        #region "Methods"

        /// <summary>
        /// Obtains resources necessary for performing analysis results cleanup
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        /// <remarks>No resources needed for performing results transfer. Function merely meets inheritance requirements</remarks>
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

        #endregion
    }
}
