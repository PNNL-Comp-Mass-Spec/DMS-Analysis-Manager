
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{
    #region "Structures"

    /// <summary>
    /// Initialization parameters for classes that implement ISpectraFileProcessor
    /// </summary>
    public struct SpectraFileProcessorParams
    {
        public int DebugLevel;
        public IMgrParams MgrParams;
        public IJobParams JobParams;
        public IStatusFile StatusTools;
        public string WorkDir;
        public string DatasetName;
    }
    #endregion

    /// <summary>
    /// Defines minimum required functionality for classes that will generate spectra files
    /// </summary>
    public interface ISpectraFileProcessor
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
        //
        IMgrParams MgrParams { set; }

        /// <summary>
        /// Job-specific parameters
        /// </summary>     
        IJobParams JobParams { set; }

        /// <summary>
        /// Interface for updating task status
        /// </summary>
        IStatusFile StatusTools { set; }

        #endregion

        #region "Methods"

        /// <summary>
        /// Initializes parameters. Must be called before executing Start()
        /// </summary>
        /// <param name="InitParams"></param>
        /// <param name="toolRunner"></param>
        void Setup(SpectraFileProcessorParams InitParams, clsAnalysisToolRunnerBase toolRunner);

        /// <summary>
        /// Starts the spectra file creation process
        /// </summary>
        /// <returns></returns>
        ProcessStatus Start();

        /// <summary>
        /// Aborts spectra file creation
        /// </summary>
        /// <returns></returns>
        ProcessStatus Abort();
   
        #endregion

    }
}