//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//*********************************************************************************************************

namespace AnalysisManagerBase
{
    /// <summary>
    /// Interface for analysis resources
    /// </summary>
    public interface IAnalysisResources
    {
        #region "Properties"

        /// <summary>
        /// Status message
        /// </summary>
        /// <remarks>
        /// If the resourcer decides to skip the step tool, this message will be stored in the Completion_Message field in the database
        /// </remarks>
        string Message { get; }

        /// <summary>
        /// Additional status message
        /// </summary>
        /// <remarks>
        /// If the resourcer decides to skip the step tool, this message will be stored in the Evaluation_Message field in the database
        /// </remarks>
        string EvalMessage { get; }

        /// <summary>
        /// Set this to true if we need to abort processing as soon as possible due to a critical error
        /// </summary>
        bool NeedToAbortProcessing { get; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Initialize class
        /// </summary>
        /// <param name="stepToolName">Name of the current step tool</param>
        /// <param name="mgrParams">Manager parameter object</param>
        /// <param name="jobParams">Job parameter object</param>
        /// <param name="statusTools">Status tools object</param>
        /// <param name="myEMSLUtilities">MyEMSL download utilities</param>
        void Setup(
            string stepToolName,
            IMgrParams mgrParams,
            IJobParams jobParams,
            IStatusFile statusTools,
            MyEMSLUtilities myEMSLUtilities);

        /// <summary>
        /// Main processing function for obtaining the required resources
        /// </summary>
        /// <returns>Status value indicating success or failure</returns>
        CloseOutType GetResources();

        /// <summary>
        /// Call this function to copy files from the working directory to a remote host for remote processing
        /// Plugins that implement this will skip files that are not be needed by the ToolRunner class of the plugin
        /// Plugins should also copy fasta files if appropriate
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        bool CopyResourcesToRemote(RemoteTransferUtility transferUtility);

        /// <summary>
        /// Check the status of an analysis resource option
        /// </summary>
        /// <param name="resourceOption">Option to get</param>
        /// <returns>The option value (true or false)</returns>
        bool GetOption(Global.AnalysisResourceOptions resourceOption);

        /// <summary>
        /// Set the status of an analysis resource option
        /// </summary>
        /// <param name="resourceOption">Option to set</param>
        /// <param name="enabled">True or false</param>
        void SetOption(Global.AnalysisResourceOptions resourceOption, bool enabled);

        #endregion

    }
}