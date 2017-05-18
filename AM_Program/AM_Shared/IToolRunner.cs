
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{

    public interface IToolRunner
    {

        #region "Properties"

        /// <summary>
        /// Evaluation code to be reported to the DMS_Pipeline DB
        /// </summary>
        int EvalCode { get; }

        /// <summary>
        /// Evaluation message to be reported to the DMS_Pipeline DB
        /// </summary>
        string EvalMessage { get; }

        /// <summary>
        /// Publicly accessible results folder name and path
        /// </summary>
        string ResFolderName { get; }

        /// <summary>
        /// Explanation of what happened to last operation this class performed
        /// </summary>
        string Message { get; }

        /// <summary>
        /// Set this to true if we need to abort processing as soon as possible due to a critical error
        /// </summary>
        bool NeedToAbortProcessing { get; }

        #endregion

        /// <summary>
        /// The state of completion of the job (as a percentage)
        /// </summary>
        float Progress { get; }

        #region "Methods"

        /// <summary>
        /// Initializes class
        /// </summary>
        /// <param name="stepToolName">Name of the current step tool</param>
        /// <param name="mgrParams">Object holding manager parameters</param>
        /// <param name="jobParams">Object holding job parameters</param>
        /// <param name="statusTools">Object for status reporting</param>
        /// <param name="summaryFile">Object for creating an analysis job summary file</param>
        /// <param name="myEMSLUtilities">MyEMSL download Utilities</param>
        /// <remarks></remarks>
        void Setup(
            string stepToolName,
            IMgrParams mgrParams,
            IJobParams jobParams,
            IStatusFile statusTools,
            clsSummaryFile summaryFile,
            clsMyEMSLUtilities myEMSLUtilities);

        /// <summary>
        /// Runs the analysis tool
        /// Major work is performed by overrides
        /// </summary>
        /// <returns>CloseoutType enum representing completion status</returns>
        /// <remarks></remarks>
        CloseOutType RunTool();

        #endregion

    }

}