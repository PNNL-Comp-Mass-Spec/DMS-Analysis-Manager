//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2008, Battelle Memorial Institute
// Created 07/25/2008
//
//*********************************************************************************************************

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerInSpecTPlugIn
{
    /// <summary>
    /// Class for running InSpecT analysis
    /// </summary>
    /// <remarks></remarks>
    public class clsAnalysisToolRunnerIN : clsAnalysisToolRunnerBase
    {
        #region "Structures"

        protected struct udtModInfoType
        {
            public string ModName;
            public string ModMass;             // Storing as a string since reading from a text file and writing to a text file
            public string Residues;
        }

        protected struct udtCachedSpectraCountInfoType
        {
            public string MostRecentSpectrumInfo;
            public int MostRecentLineNumber;
            public int CachedCount;
        }

        #endregion

        #region "Module Variables"

        public const string INSPECT_INPUT_PARAMS_FILENAME = "inspect_input.txt";
        protected const string INSPECT_EXE_NAME = "inspect.exe";

        protected clsRunDosProgram mCmdRunner;

        protected string mInspectCustomParamFileName;

        protected string mInspectConcatenatedDtaFilePath = "";
        protected string mInspectResultsFilePath = "";
        protected string mInspectErrorFilePath = "";

        protected bool m_isParallelInspect;

        protected string mInspectSearchLogFilePath = "InspectSearchLog.txt";      // This value gets updated in function RunTool
        protected string mInspectSearchLogMostRecentEntry = string.Empty;

        protected string mInspectConsoleOutputFilePath;

        protected FileSystemWatcher mSearchLogFileWatcher;
        protected string m_CloneStepRenum;
        protected string m_StepNum;

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks>Presently not used</remarks>
        public clsAnalysisToolRunnerIN()
        {
        }

        /// <summary>
        /// Runs InSpecT tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            string InspectDir = null;
            string OrgDbDir = null;

            string strBaseFilePath = null;
            clsCreateInspectIndexedDB objIndexedDBCreator = new clsCreateInspectIndexedDB();

            string strFileNameAdder = null;
            string strParallelizedText = null;

            try
            {
                //Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerIN.RunTool(): Enter");
                }

                InspectDir = m_mgrParams.GetParam("inspectdir");
                OrgDbDir = m_mgrParams.GetParam("orgdbdir");

                // Store the Inspect version info in the database
                if (!StoreToolVersionInfo(InspectDir))
                {
                    LogError(
                        "Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining Inspect version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel >= 3)
                {
                    LogMessage("Indexing Fasta file to create .trie file");
                }

                // Index the fasta file to create the .trie file
                var result = objIndexedDBCreator.CreateIndexedDbFiles(ref m_mgrParams, ref m_jobParams, m_DebugLevel, m_JobNum, InspectDir, OrgDbDir);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                //Determine if this is a parallelized job
                m_CloneStepRenum = m_jobParams.GetParam("CloneStepRenumberStart");
                m_StepNum = m_jobParams.GetParam("Step");
                strBaseFilePath = Path.Combine(m_WorkDir, m_Dataset);

                //Determine if this is parallelized inspect job
                if (string.IsNullOrEmpty(m_CloneStepRenum))
                {
                    strFileNameAdder = "";
                    strParallelizedText = "non-parallelized";
                    m_isParallelInspect = false;
                }
                else
                {
                    strFileNameAdder = "_" + (Convert.ToInt32(m_StepNum) - Convert.ToInt32(m_CloneStepRenum) + 1).ToString();
                    strParallelizedText = "parallelized";
                    m_isParallelInspect = true;
                }

                mInspectConcatenatedDtaFilePath = strBaseFilePath + strFileNameAdder + "_dta.txt";
                mInspectResultsFilePath = strBaseFilePath + strFileNameAdder + "_inspect.txt";
                mInspectErrorFilePath = strBaseFilePath + strFileNameAdder + "_error.txt";
                mInspectSearchLogFilePath = Path.Combine(m_WorkDir, "InspectSearchLog" + strFileNameAdder + ".txt");
                mInspectConsoleOutputFilePath = Path.Combine(m_WorkDir, "InspectConsoleOutput" + strFileNameAdder + ".txt");

                // Make sure the _DTA.txt file is valid
                if (!ValidateCDTAFile(mInspectConcatenatedDtaFilePath))
                {
                    return CloseOutType.CLOSEOUT_NO_DTA_FILES;
                }

                if (m_DebugLevel >= 3)
                {
                    LogDebug(
                        "Running " + strParallelizedText + " inspect on " + Path.GetFileName(mInspectConcatenatedDtaFilePath));
                }

                result = RunInSpecT(InspectDir);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                //If not a parallelized job, then zip the _Inspect.txt file
                if (!m_isParallelInspect)
                {
                    //Zip the output file
                    bool blnSuccess = false;
                    blnSuccess = base.ZipFile(mInspectResultsFilePath, true);
                    if (!blnSuccess)
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;

                //Add the current job data to the summary file
                UpdateSummaryFile();

                //Make sure objects are released
                Thread.Sleep(500);        // 500 msec delay
                clsProgRunner.GarbageCollectNow();

                result = MakeResultsFolder();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    //MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                result = MoveResultFiles();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    //MoveResultFiles moves the result files to the result folder
                    m_message = "Error moving files into results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                result = CopyResultsFolderToServer();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    //TODO: What do we do here?
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return result;
                }
            }
            catch (Exception ex)
            {
                m_message = "Error in InspectPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS; //No failures so everything must have succeeded
        }

        /// <summary>
        /// Build inspect input file from base parameter file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private string BuildInspectInputFile()
        {
            string result = string.Empty;

            // set up input to reference spectra file, and parameter file
            string ParamFilename = null;
            string orgDbDir = null;
            string fastaFilename = null;
            string dbFilePath = null;
            string inputFilename = null;
            string strInputSpectra = null;

            bool blnUseShuffledDB = false;

            try
            {
                ParamFilename = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"));
                orgDbDir = m_mgrParams.GetParam("orgdbdir");
                fastaFilename = m_jobParams.GetParam("PeptideSearch", "generatedFastaName");
                inputFilename = Path.Combine(m_WorkDir, INSPECT_INPUT_PARAMS_FILENAME);
                strInputSpectra = string.Empty;

                blnUseShuffledDB = m_jobParams.GetJobParameter("InspectUsesShuffledDB", false);

                if (blnUseShuffledDB)
                {
                    // Using shuffled version of the .trie file
                    // The Pvalue.py script does much better at computing p-values if a decoy search is performed (i.e. shuffleDB.py is used)
                    // Note that shuffleDB will add a prefix of XXX to the shuffled protein names
                    dbFilePath = Path.GetFileNameWithoutExtension(fastaFilename) + "_shuffle.trie";
                }
                else
                {
                    dbFilePath = Path.GetFileNameWithoutExtension(fastaFilename) + ".trie";
                }

                dbFilePath = Path.Combine(orgDbDir, dbFilePath);

                //add extra lines to the parameter files
                //the parameter file will become the input file for inspect
                var swInspectInputFile = new StreamWriter((new FileStream(inputFilename, FileMode.Create, FileAccess.Write, FileShare.Read)));

                // Create an instance of StreamReader to read from a file.
                var srInputBase = new StreamReader((new FileStream(ParamFilename, FileMode.Open, FileAccess.Read, FileShare.Read)));

                string strParamLine = null;

                swInspectInputFile.WriteLine("#Use the following to define the name of the log file created by Inspect (default is InspectSearchLog.txt if not defined)");
                swInspectInputFile.WriteLine("SearchLogFileName," + mInspectSearchLogFilePath);
                swInspectInputFile.WriteLine();

                swInspectInputFile.WriteLine("#Spectrum file to search; preferred formats are .mzXML and .mgf");

                //The code below was commented out since we are only supporting dta files.
                //var mzXMLFilename = Path.Combine(m_WorkDir, Dataset + ".mzXML");
                //if (m_jobParams.GetJobParameter("UseMzXML", false))
                //{
                //    strInputSpectra = mzXMLFilename;
                //}
                //else
                //{

                strInputSpectra = string.Copy(mInspectConcatenatedDtaFilePath);
                //}

                if (m_DebugLevel >= 3)
                {
                    LogDebug("Inspect input spectra: " + strInputSpectra);
                }

                swInspectInputFile.WriteLine("spectra," + strInputSpectra);
                swInspectInputFile.WriteLine();

                swInspectInputFile.WriteLine("#Note: The fully qualified database (.trie file) filename");
                swInspectInputFile.WriteLine("DB," + dbFilePath);

                // Read and display the lines from the file until the end
                // of the file is reached.
                do
                {
                    strParamLine = srInputBase.ReadLine();
                    if (strParamLine == null)
                    {
                        break;
                    }
                    swInspectInputFile.WriteLine(strParamLine);
                } while (!(strParamLine == null));
                srInputBase.Close();
                swInspectInputFile.Close();

                if (m_DebugLevel >= 2)
                {
                    LogDebug(
                        "Created Inspect input file '" + inputFilename + "' using '" + ParamFilename + "'");
                    LogDebug(
                        "Using DB '" + dbFilePath + "' and input spectra '" + strInputSpectra + "'");
                }
            }
            catch (Exception ex)
            {
                // Let the user know what went wrong.
                LogError(
                    "clsAnalysisToolRunnerIN.BuildInspectInputFile-> error while writing file: " + ex.Message);
                return string.Empty;
            }

            return inputFilename;
        }

        // Unused function
        //private int ExtractScanCountValueFromMzXML(string strMZXMLFilename)
        //{
        //    int intScanCount = 0;
        //
        //    try
        //    {
        //        var objMZXmlFile = new MSDataFileReader.clsMzXMLFileReader();
        //
        //        // Open the file
        //        objMZXmlFile.OpenFile(strMZXMLFilename);
        //
        //        // Read the first spectrum (required to determine the ScanCount)
        //        MSDataFileReader.clsSpectrumInfo objSpectrumInfo;
        //        if (objMZXmlFile.ReadNextSpectrum(out objSpectrumInfo))
        //        {
        //            intScanCount = objMZXmlFile.ScanCount;
        //        }
        //
        //        if (objMZXmlFile != null)
        //        {
        //            objMZXmlFile.CloseFile();
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        LogError(
        //            "clsAnalysisToolRunnerIN.ExtractScanCountValueFromMzXML, Error determining the scan count in the .mzXML file: " + ex.Message);
        //        return 0;
        //    }
        //
        //    return intScanCount;
        //}

        /// <summary>
        /// Run -p threshold value
        /// </summary>
        /// <returns>Value as a string or empty string means failure</returns>
        /// <remarks></remarks>
        private string getPthresh()
        {
            var defPvalThresh = "0.1";
            var tmpPvalThresh = m_mgrParams.GetParam("InspectPvalueThreshold");
            if (!string.IsNullOrEmpty(tmpPvalThresh))
            {
                return tmpPvalThresh; //return pValueThreshold value in settings file
            }
            else
            {
                return defPvalThresh; //if not found, return default of 0.1
            }
        }

        private void InitializeInspectSearchLogFileWatcher(string strWorkDir)
        {
            mSearchLogFileWatcher = new FileSystemWatcher();
            mSearchLogFileWatcher.Changed += mSearchLogFileWatcher_Changed;
            mSearchLogFileWatcher.BeginInit();
            mSearchLogFileWatcher.Path = strWorkDir;
            mSearchLogFileWatcher.IncludeSubdirectories = false;
            mSearchLogFileWatcher.Filter = Path.GetFileName(mInspectSearchLogFilePath);
            mSearchLogFileWatcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size;
            mSearchLogFileWatcher.EndInit();
            mSearchLogFileWatcher.EnableRaisingEvents = true;
        }

        /// <summary>
        /// Looks for the inspect _errors.txt file in the working folder.  If present, reads and parses it
        /// </summary>
        /// <param name="errorFilename"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool ParseInspectErrorsFile(string errorFilename)
        {
            string strInputFilePath = null;
            string strLineIn = string.Empty;

            try
            {
                if (m_DebugLevel > 4)
                {
                    LogDebug(
                        "clsAnalysisToolRunnerIN.ParseInspectErrorsFile(): Reading " + errorFilename);
                }

                strInputFilePath = Path.Combine(m_WorkDir, errorFilename);

                if (!File.Exists(strInputFilePath))
                {
                    // File not found; that means no errors occurred
                    return true;
                }
                else
                {
                    var fi = new FileInfo(errorFilename);
                    if (fi.Length == 0)
                    {
                        // Error file is 0 bytes, which means no errors occurred
                        // Delete the file
                        File.Delete(errorFilename);
                        return true;
                    }
                }

                // Initialize htMessages
                var htMessages = new Hashtable();

                // Read the contents of strInputFilePath
                var srInFile = new StreamReader(new FileStream(strInputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!srInFile.EndOfStream)
                {
                    strLineIn = srInFile.ReadLine();

                    if (strLineIn == null)
                        continue;

                    strLineIn = strLineIn.Trim();

                    if (strLineIn.Length > 0)
                    {
                        if (!htMessages.Contains(strLineIn))
                        {
                            htMessages.Add(strLineIn, 1);
                            LogWarning("Inspect warning/error: " + strLineIn);
                        }
                    }
                }

                Console.WriteLine();

                if ((srInFile != null))
                {
                    srInFile.Close();
                }
            }
            catch (Exception)
            {
                LogError(
                    "clsAnalysisToolRunnerIN.ParseInspectErrorsFile, Error reading the Inspect _errors.txt file (" + errorFilename + ")");
                return false;
            }

            return true;
        }

        /// <summary>
        /// Run InSpecT
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType RunInSpecT(string InspectDir)
        {
            string CmdStr = null;
            string ParamFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"));
            bool blnSuccess = false;

            // Build the Inspect Input Parameters file
            mInspectCustomParamFileName = BuildInspectInputFile();
            if (mInspectCustomParamFileName.Length == 0)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            mCmdRunner = new clsRunDosProgram(InspectDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (m_DebugLevel > 4)
            {
                LogDebug("clsAnalysisToolRunnerIN.RunInSpecT(): Enter");
            }

            // verify that program file exists
            string progLoc = Path.Combine(InspectDir, INSPECT_EXE_NAME);
            if (!File.Exists(progLoc))
            {
                LogError("Cannot find Inspect program file: " + progLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Create a file watcher to monitor Search Log created by Inspect
            // This file is updated after each chunk of 100 spectra are processed
            // The 4th column of this file displays the PercentComplete value for the overall search
            InitializeInspectSearchLogFileWatcher(m_WorkDir);

            // Let the user know what went wrong.
            LogMessage("Starting Inspect");

            // Set up and execute a program runner to run Inspect.exe
            CmdStr = " -i " + mInspectCustomParamFileName + " -o " + mInspectResultsFilePath + " -e " + mInspectErrorFilePath;
            if (m_DebugLevel >= 1)
            {
                LogDebug(progLoc + " " + CmdStr);
            }

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = true;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = mInspectConsoleOutputFilePath;

            if (!mCmdRunner.RunProgram(progLoc, CmdStr, "Inspect", true))
            {
                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning(
                        "Inspect returned a non-zero exit code: " + mCmdRunner.ExitCode.ToString());
                }
                else
                {
                    LogWarning("Call to Inspect failed (but exit code is 0)");
                }

                switch (mCmdRunner.ExitCode)
                {
                    case -1073741819:
                        // Corresponds to message "{W0010} .\PValue.c:453:Only 182 top-scoring matches for charge state; not recalibrating the p-value curve."
                        // This is a warning, and not an error
                        LogWarning(
                            "Exit code indicates message from PValue.c concerning not enough top-scoring matches for a given charge state; we ignore this error since it only affects the p-values");
                        blnSuccess = true;
                        break;
                    case -1073741510:
                        // Corresponds to the user pressing Ctrl+Break to stop Inspect
                        LogError(
                            "Exit code indicates user pressed Ctrl+Break; job failed");
                        break;
                    default:
                        // Any other code
                        LogError("Unknown exit code; job failed");
                        blnSuccess = false;
                        break;
                }

                if (mCmdRunner.ExitCode != 0)
                {
                    if (mInspectSearchLogMostRecentEntry.Length > 0)
                    {
                        LogWarning(
                            "Most recent Inspect search log entry: " + mInspectSearchLogMostRecentEntry);
                    }
                    else
                    {
                        LogWarning("Most recent Inspect search log entry: n/a");
                    }
                }
            }
            else
            {
                blnSuccess = true;
            }

            if (!blnSuccess)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, "Error running Inspect : " + m_JobNum);
            }
            else
            {
                m_progress = 100;
                UpdateStatusRunning();
            }

            if ((mSearchLogFileWatcher != null))
            {
                mSearchLogFileWatcher.EnableRaisingEvents = false;
                mSearchLogFileWatcher = null;
            }

            // Parse the _errors.txt file (if it exists) and copy any errors to the analysis manager log
            ParseInspectErrorsFile(mInspectErrorFilePath);

            //even though success is returned, check for the result file
            if (File.Exists(mInspectResultsFilePath))
            {
                blnSuccess = true;
            }
            else
            {
                LogError(
                    "Inspect results file not found; job failed: " + Path.GetFileName(mInspectResultsFilePath));
                blnSuccess = false;
            }

            if (blnSuccess)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private DateTime dtLastStatusUpdate = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            // Update the status file (limit the updates to every 5 seconds)
            if (DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5)
            {
                dtLastStatusUpdate = DateTime.UtcNow;
                UpdateStatusRunning(m_progress, m_DtaCount);
            }

            LogProgress("Inspect");
        }

        private void ParseInspectSearchLogFile(string strSearchLogFilePath)
        {
            string strLineIn = string.Empty;
            string strLastEntry = string.Empty;
            string[] strSplitline = null;
            string strProgress = null;

            try
            {
                var ioFile = new FileInfo(strSearchLogFilePath);
                if (ioFile.Exists && ioFile.Length > 0)
                {
                    // Search log file has been updated
                    // Open the file and read the contents

                    using (var srLogFile = new StreamReader(new FileStream(strSearchLogFilePath, FileMode.Open, FileAccess.Read, FileShare.Write)))
                    {
                        // Read to the end of the file
                        while (!srLogFile.EndOfStream)
                        {
                            strLineIn = srLogFile.ReadLine();

                            if (!string.IsNullOrEmpty(strLineIn))
                            {
                                strLastEntry = string.Copy(strLineIn);
                            }
                        }
                    }

                    if ((strLastEntry != null) && strLastEntry.Length > 0)
                    {
                        if (m_DebugLevel >= 4)
                        {
                            // Store the new search log entry in the log
                            if (mInspectSearchLogMostRecentEntry.Length == 0 || mInspectSearchLogMostRecentEntry != strLastEntry)
                            {
                                LogDebug(
                                    "Inspect search log entry: " + strLastEntry);
                            }
                        }

                        // Cache the log entry
                        mInspectSearchLogMostRecentEntry = string.Copy(strLastEntry);

                        strSplitline = strLastEntry.Split('\t');

                        if (strSplitline.Length >= 4)
                        {
                            // Parse out the number of spectra from the 3rd column
                            int.TryParse(strSplitline[2], out m_DtaCount);

                            // Parse out the % complete from the 4th column
                            // Use .TrimEnd to remove the trailing % sign
                            strProgress = strSplitline[3].TrimEnd(new char[] { '%' });
                            float.TryParse(strProgress, out m_progress);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError(
                    "clsAnalysisToolRunnerIN.ParseInspectSearchLogFile, error reading Inspect search log" + ex.Message);
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string strInspectFolder)
        {
            string strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Store paths to key files in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(Path.Combine(strInspectFolder, INSPECT_EXE_NAME)));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                LogError(
                    "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Event handler for mSearchLogFileWatcher.Changed event
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        /// <remarks></remarks>
        private void mSearchLogFileWatcher_Changed(object sender, FileSystemEventArgs e)
        {
            ParseInspectSearchLogFile(mInspectSearchLogFilePath);
        }

        #endregion
    }
}
