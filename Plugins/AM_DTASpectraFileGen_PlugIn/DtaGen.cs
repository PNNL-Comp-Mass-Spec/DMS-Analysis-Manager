//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/18/2007
//
//*********************************************************************************************************

using AnalysisManagerBase;
using PRISM;
using System;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace DTASpectraFileGen
{
    /// <summary>
    /// This is the base class that implements a specific spectra file generator.
    /// </summary>
    public abstract class DtaGen : EventNotifier, ISpectraFileProcessor
    {
        // Ignore Spelling: DTA, Loc, Prog

        protected string mErrMsg = string.Empty;
        protected string mWorkDir = string.Empty;    // Working directory on analysis machine
        protected string mDatasetName = string.Empty;

        protected AnalysisResources.RawDataTypeConstants mRawDataType = AnalysisResources.RawDataTypeConstants.Unknown;

        protected string mDtaToolNameLoc = string.Empty;             // Path to the program used to create DTA files

        protected ProcessStatus mStatus;
        protected ProcessResults mResults;
        protected IMgrParams mMgrParams;
        protected IJobParams mJobParams;
        protected short mDebugLevel;
        protected int mSpectraFileCount;
        protected IStatusFile mStatusTools;

        protected AnalysisToolRunnerBase mToolRunner;

        protected bool mAbortRequested;

        /// <summary>
        /// Progress (value between 0 and 100)
        /// </summary>
        protected float mProgress;

        public IStatusFile StatusTools
        {
            set => mStatusTools = value;
        }

        public string DtaToolNameLoc => mDtaToolNameLoc;

        public string ErrMsg => mErrMsg;

        public IMgrParams MgrParams
        {
            set => mMgrParams = value;
        }

        public IJobParams JobParams
        {
            set => mJobParams = value;
        }

        public ProcessStatus Status => mStatus;

        public ProcessResults Results => mResults;

        public int DebugLevel
        {
            get => mDebugLevel;
            set => mDebugLevel = (short)value;
        }

        public int SpectraFileCount => mSpectraFileCount;

        public float Progress => mProgress;

        /// <summary>
        /// Aborts processing
        /// </summary>
        /// <returns>ProcessStatus value indicating process was aborted</returns>
        public ProcessStatus Abort()
        {
            mAbortRequested = true;
            return ProcessStatus.SF_ABORTING;
        }

        public abstract ProcessStatus Start();

        public virtual void Setup(SpectraFileProcessorParams initParams, AnalysisToolRunnerBase toolRunner)
        {
            // Copies all input data required for plugin operation to appropriate memory variables
            mDebugLevel = (short)initParams.DebugLevel;
            mJobParams = initParams.JobParams;
            mMgrParams = initParams.MgrParams;
            mStatusTools = initParams.StatusTools;
            mWorkDir = initParams.WorkDir;
            mDatasetName = initParams.DatasetName;

            mToolRunner = toolRunner;

            mRawDataType = AnalysisResources.GetRawDataType(mJobParams.GetJobParameter("RawDataType", ""));

            mProgress = 0;
        }

        public void UpdateDtaToolNameLoc(string progLoc)
        {
            mDtaToolNameLoc = progLoc;
        }

        protected bool VerifyDirExists(string testDir)
        {
            // Verifies that the specified directory exists
            if (Directory.Exists(testDir))
            {
                mErrMsg = "";
                return true;
            }

            mErrMsg = "Directory " + testDir + " not found";
            return false;
        }

        protected bool VerifyFileExists(string testFile)
        {
            // Verifies specified file exists
            if (File.Exists(testFile))
            {
                mErrMsg = "";
                return true;
            }

            mErrMsg = "File " + testFile + " not found";
            return false;
        }

        protected virtual bool InitSetup()
        {
            // Initializes module variables and verifies mandatory parameters have been properly specified

            // Manager parameters
            if (mMgrParams == null)
            {
                mErrMsg = "Manager parameters not specified";
                return false;
            }

            // Job parameters
            if (mJobParams == null)
            {
                mErrMsg = "Job parameters not specified";
                return false;
            }

            // Status tools
            if (mStatusTools == null)
            {
                mErrMsg = "Status tools object not set";
                return false;
            }

            // If we got here, everything's OK
            return true;
        }

        protected bool DeleteNonDosFiles()
        {
            // extract_msn.exe and lcq_dta.exe sometimes leave files with funky filenames containing non-DOS characters.
            // this method removes those files

            var workDir = new DirectoryInfo(mWorkDir);

            var reValidFiles = new Regex(".dta$|.txt$|.csv$|.raw$|.params$|.wiff$|.xml$|.mgf$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            foreach (var dataFile in workDir.GetFiles())
            {
                var reMatch = reValidFiles.Match(dataFile.Extension);

                if (!reMatch.Success)
                {
                    try
                    {
                        dataFile.Delete();
                    }
                    catch (Exception ex)
                    {
                        mErrMsg = "Error removing non-DOS files: " + ex.Message;
                        return false;
                    }
                }
            }

            return true;
        }

        protected void LogDTACreationStats(string procedureName, string dtaToolName, string errorMessage)
        {
            var mostRecentBlankDTA = string.Empty;
            var mostRecentValidDTA = string.Empty;

            procedureName ??= "DtaGen.??";
            dtaToolName ??= "Unknown DTA Tool";
            errorMessage ??= "Unknown error";

            OnErrorEvent(procedureName + ", Error running " + dtaToolName + "; " + errorMessage);

            // Now count the number of .Dta files in the working folder

            try
            {
                var workDir = new DirectoryInfo(mWorkDir);
                var dtaFiles = workDir.GetFiles("*.dta");
                int dtaCount;

                if (dtaFiles.Length == 0)
                {
                    dtaCount = 0;
                }
                else
                {
                    dtaCount = dtaFiles.Length;

                    var mostRecentValidDTAIndex = -1;
                    var mostRecentBlankDTAIndex = -1;
                    long dtaFileSize = 0;

                    // Find the most recently created .Dta file
                    // However, track blank (zero-length) .Dta files separate from those with data
                    for (var index = 1; index <= dtaFiles.Length - 1; index++)
                    {
                        if (dtaFiles[index].Length == 0)
                        {
                            if (mostRecentBlankDTAIndex < 0)
                            {
                                mostRecentBlankDTAIndex = index;
                            }
                            else
                            {
                                if (dtaFiles[index].LastWriteTime > dtaFiles[mostRecentBlankDTAIndex].LastWriteTime)
                                {
                                    mostRecentBlankDTAIndex = index;
                                }
                            }
                        }
                        else
                        {
                            if (mostRecentValidDTAIndex < 0)
                            {
                                mostRecentValidDTAIndex = index;
                            }
                            else
                            {
                                if (dtaFiles[index].LastWriteTime > dtaFiles[mostRecentValidDTAIndex].LastWriteTime)
                                {
                                    mostRecentValidDTAIndex = index;
                                }
                            }
                        }
                    }

                    if (mostRecentBlankDTAIndex >= 0)
                    {
                        mostRecentBlankDTA = dtaFiles[mostRecentBlankDTAIndex].Name;
                    }

                    if (mostRecentValidDTAIndex >= 0)
                    {
                        mostRecentValidDTA = dtaFiles[mostRecentValidDTAIndex].Name;
                        dtaFileSize = dtaFiles[mostRecentValidDTAIndex].Length;
                    }

                    if (dtaCount > 0)
                    {
                        // Log the name of the most recently created .Dta file
                        if (mostRecentValidDTAIndex >= 0)
                        {
                            OnStatusEvent(procedureName + ", The most recent .Dta file created is " + mostRecentValidDTA + " with size " +
                                          dtaFileSize + " bytes");
                        }
                        else
                        {
                            OnWarningEvent(procedureName + ", No valid (non zero length) .Dta files were created");
                        }

                        if (mostRecentBlankDTAIndex >= 0)
                        {
                            OnStatusEvent(procedureName + ", The most recent blank (zero-length) .Dta file created is " + mostRecentBlankDTA);
                        }
                    }
                }

                // Log the number of .Dta files that were found
                OnStatusEvent(procedureName + ", " + dtaToolName + " created " + dtaCount + " .dta files");
            }
            catch (Exception ex)
            {
                OnErrorEvent(", Error finding the most recently created .Dta file: " + ex.Message);
            }
        }
    }
}
