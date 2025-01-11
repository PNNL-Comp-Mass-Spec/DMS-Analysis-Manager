//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using System;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;
using PRISM.Logging;

namespace AnalysisManagerBase.DataFileTools
{
    /// <summary>
    /// Return values for MakeProcess and Abort functions
    /// </summary>
    public enum ProcessResults
    {
        /// <summary>
        /// Spectra filter did not alter any spectra
        /// </summary>
        SF_NO_SPECTRA_ALTERED = -4,

        /// <summary>
        /// Spectra filter operation aborted
        /// </summary>
        SF_ABORTED = -3,

        /// <summary>
        /// Spectra filter operation didn't fail, but no output files were created
        /// </summary>
        SF_NO_FILES_CREATED = -2,

        /// <summary>
        /// Operation failed
        /// </summary>
        SF_FAILURE = -1,

        /// <summary>
        /// Operation succeeded
        /// </summary>
        SF_SUCCESS = 0
    }

    /// <summary>
    /// Return value for status property
    /// </summary>
    public enum ProcessStatus
    {
        /// <summary>
        /// Plugin initialization in progress
        /// </summary>
        SF_STARTING = 0,

        /// <summary>
        /// Plugin is attempting to do its job
        /// </summary>
        SF_RUNNING = 1,

        /// <summary>
        /// Plugin successfully completed its job
        /// </summary>
        SF_COMPLETE = 2,

        /// <summary>
        /// There was an error somewhere
        /// </summary>
        SF_ERROR = 3,

        /// <summary>
        /// An ABORT command has been received; plugin shutdown in progress
        /// </summary>
        SF_ABORTING = 4
    }

    /// <summary>
    /// Initialization parameters for classes that implement ISpectraFilter
    /// </summary>
    public struct SpectraFilterParams
    {
        /// <summary>
        /// Source folder path
        /// </summary>
        public string SourceFolderPath;

        /// <summary>
        /// Output folder path
        /// </summary>
        public string OutputFolderPath;

        /// <summary>
        /// Debug level
        /// </summary>
        public int DebugLevel;

        /// <summary>
        /// Logging class
        /// </summary>
        public BaseLogger Logger;

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
    }

    /// <summary>
    /// Defines minimum required functionality for classes that will filter spectra files
    /// </summary>
    [Obsolete("This interface is unused")]
    public interface ISpectraFilter
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
        /// Count of spectra files that remain after filtering
        /// </summary>
        int SpectraFileCount { get; }

        /// <summary>
        /// Initializes parameters. Must be called before executing Start()
        /// </summary>
        /// <param name="initParams">Spectrum filter parameters</param>
        void Setup(SpectraFilterParams initParams);

        /// <summary>
        /// Starts the spectra filter operation
        /// </summary>
        ProcessStatus Start();

        /// <summary>
        /// Abort processing
        /// </summary>
        ProcessStatus Abort();
    }
}