
//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.OfflineJobs;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerBase.AnalysisTool
{
    /// <summary>
    /// Tool runner interface
    /// </summary>
    /// <remarks>Implemented by AnalysisToolRunnerBase</remarks>
    public interface IToolRunner
    {
        /// <summary>
        /// Evaluation code to be reported to the DMS_Pipeline DB
        /// </summary>
        int EvalCode { get; }

        /// <summary>
        /// Evaluation message to be reported to the DMS_Pipeline DB
        /// </summary>
        string EvalMessage { get; }

        /// <summary>
        /// Will be set to true if the job cannot be run due to not enough free memory
        /// </summary>
        bool InsufficientFreeMemory { get; }

        /// <summary>
        /// Status message related to processing tasks performed by this class
        /// </summary>
        string Message { get; }

        /// <summary>
        /// Set this to true if we need to abort processing as soon as possible due to a critical error
        /// </summary>
        bool NeedToAbortProcessing { get; }

        /// <summary>
        /// The state of completion of the job (as a percentage)
        /// </summary>
        float Progress { get; }

        /// <summary>
        /// Time the analysis started (UTC-based)
        /// </summary>
        /// <remarks>RunTool sets this in AnalysisToolRunnerBase</remarks>
        DateTime StartTime { get; }

        /// <summary>
        /// Initializes class
        /// </summary>
        /// <param name="stepToolName">Name of the current step tool</param>
        /// <param name="mgrParams">Object holding manager parameters</param>
        /// <param name="jobParams">Object holding job parameters</param>
        /// <param name="statusTools">Object for status reporting</param>
        /// <param name="summaryFile">Object for creating an analysis job summary file</param>
        /// <param name="myEMSLUtilities">MyEMSL download Utilities</param>
        void Setup(
            string stepToolName,
            IMgrParams mgrParams,
            IJobParams jobParams,
            IStatusFile statusTools,
            SummaryFile summaryFile,
            MyEMSLUtilities myEMSLUtilities);

        /// <summary>
        /// Runs the analysis tool
        /// Major work is performed by overrides
        /// </summary>
        /// <returns>CloseoutType enum representing completion status</returns>
        CloseOutType RunTool();

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        void CopyFailedResultsToArchiveDirectory();

        /// <summary>
        /// Perform any required post-processing after retrieving remote results
        /// </summary>
        /// <remarks>
        /// Actual post-processing of remote results should only be required if the remote host running the job
        /// could not perform a step that requires database access or Windows share access
        /// </remarks>
        /// <returns>CloseoutType enum representing completion status</returns>
        CloseOutType PostProcessRemoteResults();

        /// <summary>
        /// Make the local results directory, move files into that directory, then copy the files to the transfer directory on the Proto-x server
        /// </summary>
        /// <remarks>Uses MakeResultsDirectory, MoveResultFiles, and CopyResultsFolderToServer</remarks>
        /// <param name="transferDirectoryPathOverride">Optional: specific transfer folder path to use; if empty, uses job param transferDirectoryPath</param>
        /// <returns>True if success, otherwise false</returns>
        bool CopyResultsToTransferDirectory(string transferDirectoryPathOverride = "");

        /// <summary>
        /// Retrieve results from a remote processing job; storing in the local working directory
        /// </summary>
        /// <remarks>
        /// If successful, the calling procedure will typically next call
        /// PostProcessRemoteResults then CopyResultsToTransferDirectory
        /// </remarks>
        /// <param name="transferUtility">Transfer utility</param>
        /// <param name="verifyCopied">Log warnings and an error if any files are missing.  When false, logs debug messages instead</param>
        /// <param name="retrievedFilePaths">Local paths of retrieved files</param>
        /// <returns>True on success, otherwise false</returns>
        bool RetrieveRemoteResults(RemoteTransferUtility transferUtility, bool verifyCopied, out List<string> retrievedFilePaths);

        /// <summary>
        /// Update the evaluation code and evaluation message
        /// </summary>
        /// <param name="evalCode">Evaluation code</param>
        /// <param name="evalMsg">Evaluation message</param>
        void UpdateEvalCode(int evalCode, string evalMsg);
    }
}
