
//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerBase.DataFileTools
{
    /// <summary>
    /// Initialization parameters for classes that implement ISpectraFileProcessor
    /// </summary>
    public struct SpectraFileProcessorParams
    {
        /// <summary>
        /// Debug level
        /// </summary>
        public int DebugLevel;

        /// <summary>
        /// Manager parameters
        /// </summary>
        public IMgrParams MgrParams;

        /// <summary>
        /// Job parameters
        /// </summary>
        public IJobParams JobParams;

        /// <summary>
        /// Status tools
        /// </summary>
        public IStatusFile StatusTools;

        /// <summary>
        /// Working directory
        /// </summary>
        public string WorkDir;

        /// <summary>
        /// Dataset name
        /// </summary>
        public string DatasetName;
    }

    /// <summary>
    /// Defines minimum required functionality for classes that will generate spectra files
    /// </summary>
    public interface ISpectraFileProcessor
    {
        /// <summary>
        /// Allows calling program to get current status
        /// </summary>
        ProcessStatus Status { get; }

        /// <summary>
        /// Allows calling program to determine if DTA creation succeeded
        /// </summary>
        ProcessResults Results { get; }

        /// <summary>
        /// Error message describing any errors encountered
        /// </summary>
        string ErrMsg { get; }

        /// <summary>
        /// Allows control of debug information verbosity; 0=minimum, 5=maximum verbosity
        /// </summary>
        int DebugLevel { get; set; }

        /// <summary>
        /// Path to the program used to create .DTA files
        /// </summary>
        string DtaToolNameLoc { get; }

        /// <summary>
        /// Count of spectra files that have been created
        /// </summary>
        int SpectraFileCount { get; }

        /// <summary>
        /// Percent complete (Value between 0 and 100)
        /// </summary>
        float Progress { get; }

        /// <summary>
        /// Machine-specific parameters, such as file locations
        /// </summary>
        IMgrParams MgrParams { set; }

        /// <summary>
        /// Job-specific parameters
        /// </summary>
        IJobParams JobParams { set; }

        /// <summary>
        /// Interface for updating task status
        /// </summary>
        IStatusFile StatusTools { set; }

        /// <summary>
        /// Initializes parameters. Must be called before executing Start()
        /// </summary>
        /// <param name="InitParams"></param>
        /// <param name="toolRunner"></param>
        void Setup(SpectraFileProcessorParams InitParams, AnalysisToolRunnerBase toolRunner);

        /// <summary>
        /// Starts the spectra file creation process
        /// </summary>
        ProcessStatus Start();

        /// <summary>
        /// Aborts spectra file creation
        /// </summary>
        ProcessStatus Abort();
    }
}