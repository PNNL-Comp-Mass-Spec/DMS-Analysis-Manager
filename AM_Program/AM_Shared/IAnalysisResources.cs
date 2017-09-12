//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//*********************************************************************************************************

namespace AnalysisManagerBase
{

    /// <summary>
    /// Interface for analysis resources
    /// </summary>
    /// <remarks></remarks>
    public interface IAnalysisResources
    {

        #region "Properties"

        /// <summary>
        /// Status message
        /// </summary>
        string Message { get; }

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
        /// <remarks></remarks>
        void Setup(
            string stepToolName,
            IMgrParams mgrParams,
            IJobParams jobParams,
            IStatusFile statusTools,
            clsMyEMSLUtilities myEMSLUtilities);

        /// <summary>
        /// Main processing function for obtaining the required resources
        /// </summary>
        /// <returns>Status value indicating success or failure</returns>
        /// <remarks></remarks>
        CloseOutType GetResources();

        /// <summary>
        /// Call this function to copy files from the working directory to a remote host for remote processing
        /// Plugins that implement this will skip files that are not be needed by the ToolRunner class of the plugin
        /// Plugins should also copy fasta files if appropriate
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        bool CopyResourcesToRemote(clsRemoteTransferUtility transferUtility);

        /// <summary>
        /// Check the status of an analysis resource option
        /// </summary>
        /// <param name="resourceOption">Option to get</param>
        /// <returns>The option value (true or false)</returns>
        /// <remarks></remarks>
        bool GetOption(clsGlobal.eAnalysisResourceOptions resourceOption);

        /// <summary>
        /// Set the status of an analysis resource option
        /// </summary>
        /// <param name="resourceOption">Option to set</param>
        /// <param name="enabled">True or false</param>
        /// <remarks></remarks>
        void SetOption(clsGlobal.eAnalysisResourceOptions resourceOption, bool enabled);

        #endregion

    }
}