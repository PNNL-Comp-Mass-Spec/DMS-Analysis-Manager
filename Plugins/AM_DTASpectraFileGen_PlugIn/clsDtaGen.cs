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

namespace DTASpectraFileGen
{
    /// <summary>
    /// This is the base class that implements a specific spectra file generator.
    /// </summary>
    /// <remarks></remarks>
    public abstract class clsDtaGen : EventNotifier, ISpectraFileProcessor
    {
        #region "Module variables"

        protected string mErrMsg = string.Empty;
        protected string mWorkDir = string.Empty;    // Working directory on analysis machine
        protected string mDatasetName = string.Empty;

        protected clsAnalysisResources.eRawDataTypeConstants mRawDataType = clsAnalysisResources.eRawDataTypeConstants.Unknown;

        protected string mDtaToolNameLoc = string.Empty;             // Path to the program used to create DTA files

        protected ProcessStatus mStatus;
        protected ProcessResults mResults;
        protected IMgrParams mMgrParams;
        protected IJobParams mJobParams;
        protected short mDebugLevel;
        protected int mSpectraFileCount;
        protected IStatusFile mStatusTools;

        protected clsAnalysisToolRunnerBase mToolRunner;

        protected bool mAbortRequested;

        // The following is a value between 0 and 100
        protected float mProgress;

        #endregion

        #region "Properties"

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

        #endregion

        #region "Methods"

        /// <summary>
        /// Aborts processing
        /// </summary>
        /// <returns>ProcessStatus value indicating process was aborted</returns>
        /// <remarks></remarks>
        public ProcessStatus Abort()
        {
            mAbortRequested = true;
            return ProcessStatus.SF_ABORTING;
        }

        public abstract ProcessStatus Start();

        public virtual void Setup(SpectraFileProcessorParams initParams, clsAnalysisToolRunnerBase toolRunner)
        {
            // Copies all input data required for plugin operation to appropriate memory variables
            mDebugLevel = (short)initParams.DebugLevel;
            mJobParams = initParams.JobParams;
            mMgrParams = initParams.MgrParams;
            mStatusTools = initParams.StatusTools;
            mWorkDir = initParams.WorkDir;
            mDatasetName = initParams.DatasetName;

            mToolRunner = toolRunner;

            mRawDataType = clsAnalysisResources.GetRawDataType(mJobParams.GetJobParameter("RawDataType", ""));

            mProgress = 0;
        }

        public void UpdateDtaToolNameLoc(string progLoc)
        {
            mDtaToolNameLoc = progLoc;
        }

        protected bool VerifyDirExists(string TestDir)
        {
            // Verifies that the specified directory exists
            if (Directory.Exists(TestDir))
            {
                mErrMsg = "";
                return true;
            }

            mErrMsg = "Directory " + TestDir + " not found";
            return false;
        }

        protected bool VerifyFileExists(string TestFile)
        {
            // Verifies specified file exists
            if (File.Exists(TestFile))
            {
                mErrMsg = "";
                return true;
            }

            mErrMsg = "File " + TestFile + " not found";
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
            // This function removes those files

            var workDir = new DirectoryInfo(mWorkDir);

            var reValidFiles = new Regex(@".dta$|.txt$|.csv$|.raw$|.params$|.wiff$|.xml$|.mgf$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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

        protected void LogDTACreationStats(string strProcedureName, string strDTAToolName, string strErrorMessage)
        {
            var strMostRecentBlankDTA = string.Empty;
            var strMostRecentValidDTA = string.Empty;

            if (strProcedureName == null)
            {
                strProcedureName = "clsDtaGen.??";
            }
            if (strDTAToolName == null)
            {
                strDTAToolName = "Unknown DTA Tool";
            }

            if (strErrorMessage == null)
            {
                strErrorMessage = "Unknown error";
            }

            OnErrorEvent(strProcedureName + ", Error running " + strDTAToolName + "; " + strErrorMessage);

            // Now count the number of .Dta files in the working folder

            try
            {
                var objFolderInfo = new DirectoryInfo(mWorkDir);
                var objFiles = objFolderInfo.GetFiles("*.dta");
                int intDTACount;

                if (objFiles.Length <= 0)
                {
                    intDTACount = 0;
                }
                else
                {
                    intDTACount = objFiles.Length;

                    var intMostRecentValidDTAIndex = -1;
                    var intMostRecentBlankDTAIndex = -1;
                    long lngDTAFileSize = 0;

                    // Find the most recently created .Dta file
                    // However, track blank (zero-length) .Dta files separate from those with data
                    for (var intIndex = 1; intIndex <= objFiles.Length - 1; intIndex++)
                    {
                        if (objFiles[intIndex].Length == 0)
                        {
                            if (intMostRecentBlankDTAIndex < 0)
                            {
                                intMostRecentBlankDTAIndex = intIndex;
                            }
                            else
                            {
                                if (objFiles[intIndex].LastWriteTime > objFiles[intMostRecentBlankDTAIndex].LastWriteTime)
                                {
                                    intMostRecentBlankDTAIndex = intIndex;
                                }
                            }
                        }
                        else
                        {
                            if (intMostRecentValidDTAIndex < 0)
                            {
                                intMostRecentValidDTAIndex = intIndex;
                            }
                            else
                            {
                                if (objFiles[intIndex].LastWriteTime > objFiles[intMostRecentValidDTAIndex].LastWriteTime)
                                {
                                    intMostRecentValidDTAIndex = intIndex;
                                }
                            }
                        }
                    }

                    if (intMostRecentBlankDTAIndex >= 0)
                    {
                        strMostRecentBlankDTA = objFiles[intMostRecentBlankDTAIndex].Name;
                    }

                    if (intMostRecentValidDTAIndex >= 0)
                    {
                        strMostRecentValidDTA = objFiles[intMostRecentValidDTAIndex].Name;
                        lngDTAFileSize = objFiles[intMostRecentValidDTAIndex].Length;
                    }

                    if (intDTACount > 0)
                    {
                        // Log the name of the most recently created .Dta file
                        if (intMostRecentValidDTAIndex >= 0)
                        {
                            OnStatusEvent(strProcedureName + ", The most recent .Dta file created is " + strMostRecentValidDTA + " with size " +
                                          lngDTAFileSize + " bytes");
                        }
                        else
                        {
                            OnWarningEvent(strProcedureName + ", No valid (non zero length) .Dta files were created");
                        }

                        if (intMostRecentBlankDTAIndex >= 0)
                        {
                            OnStatusEvent(strProcedureName + ", The most recent blank (zero-length) .Dta file created is " + strMostRecentBlankDTA);
                        }
                    }
                }

                // Log the number of .Dta files that were found
                OnStatusEvent(strProcedureName + ", " + strDTAToolName + " created " + intDTACount + " .dta files");
            }
            catch (Exception ex)
            {
                OnErrorEvent(", Error finding the most recently created .Dta file: " + ex.Message);
            }
        }

        #endregion
    }
}
