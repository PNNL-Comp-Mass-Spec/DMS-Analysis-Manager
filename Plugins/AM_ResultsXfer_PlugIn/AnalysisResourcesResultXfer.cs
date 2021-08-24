
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2008, Battelle Memorial Institute
// Created 10/30/2008
//
//*********************************************************************************************************

using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerResultsXferPlugin
{
    /// <summary>
    /// Retrieve resources for the Results transfer plugin
    /// </summary>
    public class AnalysisResourcesResultXfer : AnalysisResources
    {
        /// <summary>
        /// Obtains resources necessary for performing analysis results transfer
        /// </summary>
        /// <remarks>No resources needed for performing results transfer. Function merely meets inheritance requirements</remarks>
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
