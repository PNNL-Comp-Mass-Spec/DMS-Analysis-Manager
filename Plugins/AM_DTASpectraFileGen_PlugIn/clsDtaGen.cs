//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/18/2007
//
//*********************************************************************************************************

using System;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace DTASpectraFileGen
{
    /// <summary>
    /// This is the base class that implements a specific spectra file generator.
    /// </summary>
    /// <remarks></remarks>
    public abstract class clsDtaGen : clsEventNotifier, ISpectraFileProcessor
    {
        #region "Module variables"

        protected string m_ErrMsg = string.Empty;
        protected string m_WorkDir = string.Empty;    // Working directory on analysis machine
        protected string m_Dataset = string.Empty;

        protected clsAnalysisResources.eRawDataTypeConstants m_RawDataType = clsAnalysisResources.eRawDataTypeConstants.Unknown;

        protected string m_DtaToolNameLoc = string.Empty;             // Path to the program used to create DTA files

        protected ProcessStatus m_Status;
        protected ProcessResults m_Results;
        protected IMgrParams m_MgrParams;
        protected IJobParams m_JobParams;
        protected short m_DebugLevel = 0;
        protected int m_SpectraFileCount;
        protected IStatusFile m_StatusTools;

        protected clsAnalysisToolRunnerBase m_ToolRunner;

        protected bool m_AbortRequested = false;

        // The following is a value between 0 and 100
        protected float m_Progress = 0;

        #endregion

        #region "Properties"

        public IStatusFile StatusTools
        {
            set { m_StatusTools = value; }
        }

        public string DtaToolNameLoc
        {
            get { return m_DtaToolNameLoc; }
        }

        public string ErrMsg
        {
            get { return m_ErrMsg; }
        }

        public IMgrParams MgrParams
        {
            set { m_MgrParams = value; }
        }

        public IJobParams JobParams
        {
            set { m_JobParams = value; }
        }

        public ProcessStatus Status
        {
            get { return m_Status; }
        }

        public ProcessResults Results
        {
            get { return m_Results; }
        }

        public int DebugLevel
        {
            get { return m_DebugLevel; }
            set { m_DebugLevel = Convert.ToInt16(value); }
        }

        public int SpectraFileCount
        {
            get { return m_SpectraFileCount; }
        }

        public float Progress
        {
            get { return m_Progress; }
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Aborts processing
        /// </summary>
        /// <returns>ProcessStatus value indicating process was aborted</returns>
        /// <remarks></remarks>
        public ProcessStatus Abort()
        {
            m_AbortRequested = true;
            return ProcessStatus.SF_ABORTING;
        }

        public abstract ProcessStatus Start();

        public virtual void Setup(SpectraFileProcessorParams initParams, clsAnalysisToolRunnerBase toolRunner)
        {
            // Copies all input data required for plugin operation to appropriate memory variables
            m_DebugLevel = Convert.ToInt16(initParams.DebugLevel);
            m_JobParams = initParams.JobParams;
            m_MgrParams = initParams.MgrParams;
            m_StatusTools = initParams.StatusTools;
            m_WorkDir = initParams.WorkDir;
            m_Dataset = initParams.DatasetName;

            m_ToolRunner = toolRunner;

            m_RawDataType = clsAnalysisResources.GetRawDataType(m_JobParams.GetJobParameter("RawDataType", ""));

            m_Progress = 0;
        }

        public void UpdateDtaToolNameLoc(string progLoc)
        {
            m_DtaToolNameLoc = progLoc;
        }

        protected bool VerifyDirExists(string TestDir)
        {
            // Verifies that the specified directory exists
            if (Directory.Exists(TestDir))
            {
                m_ErrMsg = "";
                return true;
            }
            else
            {
                m_ErrMsg = "Directory " + TestDir + " not found";
                return false;
            }
        }

        protected bool VerifyFileExists(string TestFile)
        {
            // Verifies specified file exists
            if (File.Exists(TestFile))
            {
                m_ErrMsg = "";
                return true;
            }
            else
            {
                m_ErrMsg = "File " + TestFile + " not found";
                return false;
            }
        }

        protected virtual bool InitSetup()
        {
            // Initializes module variables and verifies mandatory parameters have been propery specified

            // Manager parameters
            if (m_MgrParams == null)
            {
                m_ErrMsg = "Manager parameters not specified";
                return false;
            }

            // Job parameters
            if (m_JobParams == null)
            {
                m_ErrMsg = "Job parameters not specified";
                return false;
            }

            // Status tools
            if (m_StatusTools == null)
            {
                m_ErrMsg = "Status tools object not set";
                return false;
            }

            // If we got here, everything's OK
            return true;
        }

        protected bool DeleteNonDosFiles()
        {
            // extract_msn.exe and lcq_dta.exe sometimes leave files with funky filenames containing non-DOS characters.
            // This function removes those files

            DirectoryInfo workDir = new DirectoryInfo(m_WorkDir);

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
                        m_ErrMsg = "Error removing non-DOS files: " + ex.Message;
                        return false;
                    }
                }
            }

            return true;
        }

        protected void LogDTACreationStats(string strProcedureName, string strDTAToolName, string strErrorMessage)
        {
            string strMostRecentBlankDTA = string.Empty;
            string strMostRecentValidDTA = string.Empty;

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
                var objFolderInfo = new DirectoryInfo(m_WorkDir);
                var objFiles = objFolderInfo.GetFiles("*.dta");
                int intDTACount = 0;

                if (objFiles == null || objFiles.Length <= 0)
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
                                          lngDTAFileSize.ToString() + " bytes");
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
                OnStatusEvent(strProcedureName + ", " + strDTAToolName + " created " + intDTACount.ToString() + " .dta files");
            }
            catch (Exception ex)
            {
                OnErrorEvent(", Error finding the most recently created .Dta file: " + ex.Message);
            }
        }

        #endregion
    }
}
