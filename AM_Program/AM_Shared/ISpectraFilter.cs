//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{

    #region "Enums"

    /// <summary>
    /// Return values for MakeProcess and Abort functions
    /// </summary>
    public enum ProcessResults
    {
        /// <summary>
        /// Operation succeeded
        /// </summary>
        SFILT_SUCCESS = 0,

        /// <summary>
        /// Operation failed
        /// </summary>
        SFILT_FAILURE = -1,

        /// <summary>
        /// Spectra filter operation didn't fail, but no output files were created
        /// </summary>
        SFILT_NO_FILES_CREATED = -2,

        /// <summary>
        /// Spectra filter operation aborted
        /// </summary>
        SFILT_ABORTED = -3,

        /// <summary>
        /// Spectra filter did not alter any spectra
        /// </summary>
        SFILT_NO_SPECTRA_ALTERED = -4
        
    }

    /// <summary>
    /// Return value for status property
    /// </summary>
    public enum ProcessStatus
    {
        /// <summary>
        /// Plugin initialization in progress
        /// </summary>
        SFILT_STARTING = 0,

        /// <summary>
        /// Plugin is attempting to do its job
        /// </summary>
        SFILT_RUNNING = 1,

        /// <summary>
        /// Plugin successfully completed its job
        /// </summary>
        SFILT_COMPLETE = 2,

        /// <summary>
        /// There was an error somewhere
        /// </summary>
        SFILT_ERROR = 3,

        /// <summary>
        /// An ABORT command has been received; plugin shutdown in progress
        /// </summary>
        SFILT_ABORTING = 4
    }
    #endregion

    #region "Structures"
    
    /// <summary>
    /// Initialization parameters for classes that implement ISpectraFilter
    /// </summary>
    public struct SpectraFilterParams
    {
        public string SourceFolderPath;
        public string OutputFolderPath;
        //' Unused: public Specialized.StringDictionary MiscParams;
        public int DebugLevel;
        public PRISM.Logging.ILogger Logger;
        public IMgrParams MgrParams;
        public IJobParams JobParams;
        public IStatusFile StatusTools;
    }
    #endregion

    /// <summary>
    /// Defines minimum required functionality for classes that will filter spectra files
    /// </summary>
    public interface ISpectraFilter
    {

        #region "Properties"

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
        #endregion

        #region "Methods"

        /// <summary>
        /// Initializes parameters. Must be called before executing Start()
        /// </summary>
        /// <param name="InitParams"></param>
        void Setup(SpectraFilterParams InitParams);

        /// <summary>
        /// Starts the spectra filter operation
        /// </summary>
        /// <returns></returns>
        ProcessStatus Start();

        /// <summary>
        /// //
        /// </summary>
        /// <returns></returns>
        ProcessStatus Abort();
        
        #endregion

    }
}