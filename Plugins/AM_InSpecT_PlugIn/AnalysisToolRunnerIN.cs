//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2008, Battelle Memorial Institute
// Created 07/25/2008
//
//*********************************************************************************************************

using AnalysisManagerBase;
using PRISM;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerInSpecTPlugIn
{
    /// <summary>
    /// Class for running InSpecT analysis
    /// </summary>
    public class AnalysisToolRunnerIN : AnalysisToolRunnerBase
    {
        #region "Structures"

        protected struct ModInfo
        {
            public string ModName;
            public string ModMass;             // Storing as a string since reading from a text file and writing to a text file
            public string Residues;
        }

        protected struct CachedSpectraCountInfo
        {
            public string MostRecentSpectrumInfo;
            public int MostRecentLineNumber;
            public int CachedCount;
        }

        #endregion

        #region "Module Variables"

        public const string INSPECT_INPUT_PARAMS_FILENAME = "inspect_input.txt";
        protected const string INSPECT_EXE_NAME = "inspect.exe";

        protected RunDosProgram mCmdRunner;

        protected string mInspectCustomParamFileName;

        protected string mInspectConcatenatedDtaFilePath = "";
        protected string mInspectResultsFilePath = "";
        protected string mInspectErrorFilePath = "";

        protected bool mIsParallelInspect;

        protected string mInspectSearchLogFilePath = "InspectSearchLog.txt";      // This value gets updated in function RunTool
        protected string mInspectSearchLogMostRecentEntry = string.Empty;

        protected string mInspectConsoleOutputFilePath;

        protected FileSystemWatcher mSearchLogFileWatcher;
        protected string mCloneStepRenumber;
        protected string mStepNum;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs InSpecT tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            var indexedDBCreator = new CreateInspectIndexedDB();

            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerIN.RunTool(): Enter");
                }

                var inspectDir = mMgrParams.GetParam("InspectDir");
                var orgDbDir = mMgrParams.GetParam("OrgDbDir");

                // Store the Inspect version info in the database
                if (!StoreToolVersionInfo(inspectDir))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining Inspect version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel >= 3)
                {
                    LogMessage("Indexing Fasta file to create .trie file");
                }

                // Index the fasta file to create the .trie file
                var result = indexedDBCreator.CreateIndexedDbFiles(ref mMgrParams, ref mJobParams, mDebugLevel, mJob, inspectDir, orgDbDir);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                // Determine if this is a parallelized job
                mCloneStepRenumber = mJobParams.GetParam("CloneStepRenumberStart");
                mStepNum = mJobParams.GetParam("Step");
                var baseFilePath = Path.Combine(mWorkDir, mDatasetName);

                string fileNameAdder;
                string parallelizedText;

                // Determine if this is parallelized inspect job
                if (string.IsNullOrEmpty(mCloneStepRenumber))
                {
                    fileNameAdder = "";
                    parallelizedText = "non-parallelized";
                    mIsParallelInspect = false;
                }
                else
                {
                    fileNameAdder = "_" + (Convert.ToInt32(mStepNum) - Convert.ToInt32(mCloneStepRenumber) + 1);
                    parallelizedText = "parallelized";
                    mIsParallelInspect = true;
                }

                mInspectConcatenatedDtaFilePath = baseFilePath + fileNameAdder + "_dta.txt";
                mInspectResultsFilePath = baseFilePath + fileNameAdder + "_inspect.txt";
                mInspectErrorFilePath = baseFilePath + fileNameAdder + "_error.txt";
                mInspectSearchLogFilePath = Path.Combine(mWorkDir, "InspectSearchLog" + fileNameAdder + ".txt");
                mInspectConsoleOutputFilePath = Path.Combine(mWorkDir, "InspectConsoleOutput" + fileNameAdder + ".txt");

                // Make sure the _DTA.txt file is valid
                if (!ValidateCDTAFile(mInspectConcatenatedDtaFilePath))
                {
                    return CloseOutType.CLOSEOUT_NO_DTA_FILES;
                }

                if (mDebugLevel >= 3)
                {
                    LogDebug("Running " + parallelizedText + " inspect on " + Path.GetFileName(mInspectConcatenatedDtaFilePath));
                }

                result = RunInSpecT(inspectDir);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                // If not a parallelized job, zip the _Inspect.txt file
                if (!mIsParallelInspect)
                {
                    // Zip the output file
                    var blnSuccess = ZipFile(mInspectResultsFilePath, true);
                    if (!blnSuccess)
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                ProgRunner.GarbageCollectNow();

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in InspectPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Build inspect input file from base parameter file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private string BuildInspectInputFile()
        {
            string inputFilename;

            try
            {
                var paramFilename = Path.Combine(mWorkDir, mJobParams.GetParam("parmFileName"));
                var orgDbDir = mMgrParams.GetParam("OrgDbDir");
                var fastaFilename = mJobParams.GetParam("PeptideSearch", "generatedFastaName");
                inputFilename = Path.Combine(mWorkDir, INSPECT_INPUT_PARAMS_FILENAME);

                var useShuffledDB = mJobParams.GetJobParameter("InspectUsesShuffledDB", false);
                string dbFilePath;

                if (useShuffledDB)
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

                // Add extra lines to the parameter files
                // The parameter file will become the input file for inspect
                using (var writer = new StreamWriter(new FileStream(inputFilename, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine("#Use the following to define the name of the log file created by Inspect (default is InspectSearchLog.txt if not defined)");
                    writer.WriteLine("SearchLogFileName," + mInspectSearchLogFilePath);
                    writer.WriteLine();

                    writer.WriteLine("#Spectrum file to search; preferred formats are .mzXML and .mgf");

                    if (mDebugLevel >= 3)
                    {
                        LogDebug("Inspect input spectra: " + mInspectConcatenatedDtaFilePath);
                    }

                    writer.WriteLine("spectra," + mInspectConcatenatedDtaFilePath);
                    writer.WriteLine();

                    writer.WriteLine("#Note: The fully qualified database (.trie file) filename");
                    writer.WriteLine("DB," + dbFilePath);

                    // Append the parameter file contents to the Inspect input file
                    using var reader = new StreamReader(new FileStream(paramFilename, FileMode.Open, FileAccess.Read, FileShare.Read));

                    while (!reader.EndOfStream)
                    {
                        var paramLine = reader.ReadLine();
                        if (paramLine != null)
                        {
                            writer.WriteLine(paramLine);
                        }
                    }
                }

                if (mDebugLevel >= 2)
                {
                    LogDebug("Created Inspect input file '" + inputFilename + "' using '" + paramFilename + "'");
                    LogDebug("Using DB '" + dbFilePath + "' and input spectra '" + mInspectConcatenatedDtaFilePath + "'");
                }
            }
            catch (Exception ex)
            {
                // Let the user know what went wrong.
                LogError("AnalysisToolRunnerIN.BuildInspectInputFile-> error while writing file: " + ex.Message);
                return string.Empty;
            }

            return inputFilename;
        }

        // Unused function
        // private int ExtractScanCountValueFromMzXML(string strMZXMLFilename)
        // {
        //    int intScanCount = 0;
        //
        //    try
        //    {
        //        var objMZXmlFile = new MSDataFileReader.MzXMLFileReader();
        //
        //        // Open the file
        //        objMZXmlFile.OpenFile(strMZXMLFilename);
        //
        //        // Read the first spectrum (required to determine the ScanCount)
        //        MSDataFileReader.SpectrumInfo objSpectrumInfo;
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
        //        LogError("AnalysisToolRunnerIN.ExtractScanCountValueFromMzXML, Error determining the scan count in the .mzXML file: " + ex.Message);
        //        return 0;
        //    }
        //
        //    return intScanCount;
        // }

        private void InitializeInspectSearchLogFileWatcher(string strWorkDir)
        {
            mSearchLogFileWatcher = new FileSystemWatcher();
            mSearchLogFileWatcher.Changed += SearchLogFileWatcher_Changed;
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
        protected bool ParseInspectErrorsFile(string errorFilename)
        {
            try
            {
                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerIN.ParseInspectErrorsFile(): Reading " + errorFilename);
                }

                var errorFilePath = Path.Combine(mWorkDir, errorFilename);

                if (!File.Exists(errorFilePath))
                {
                    // File not found; that means no errors occurred
                    return true;
                }

                var fi = new FileInfo(errorFilename);
                if (fi.Length == 0)
                {
                    // Error file is 0 bytes, which means no errors occurred
                    // Delete the file
                    File.Delete(errorFilename);
                    return true;
                }

                // Initialize htMessages
                var htMessages = new Hashtable();

                // Read the contents of errorFilePath
                using (var reader = new StreamReader(new FileStream(errorFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (dataLine == null)
                            continue;

                        var dataLineTrimmed = dataLine.Trim();

                        if (dataLineTrimmed.Length > 0)
                        {
                            if (!htMessages.Contains(dataLineTrimmed))
                            {
                                htMessages.Add(dataLineTrimmed, 1);
                                LogWarning("Inspect warning/error: " + dataLineTrimmed);
                            }
                        }
                    }
                }

                Console.WriteLine();
            }
            catch (Exception)
            {
                LogError("AnalysisToolRunnerIN.ParseInspectErrorsFile, Error reading the Inspect _errors.txt file (" + errorFilename + ")");
                return false;
            }

            return true;
        }

        /// <summary>
        /// Run InSpecT
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType RunInSpecT(string InspectDir)
        {
            var blnSuccess = false;

            // Build the Inspect Input Parameters file
            mInspectCustomParamFileName = BuildInspectInputFile();
            if (mInspectCustomParamFileName.Length == 0)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            mCmdRunner = new RunDosProgram(InspectDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            if (mDebugLevel > 4)
            {
                LogDebug("AnalysisToolRunnerIN.RunInSpecT(): Enter");
            }

            // verify that program file exists
            var progLoc = Path.Combine(InspectDir, INSPECT_EXE_NAME);
            if (!File.Exists(progLoc))
            {
                LogError("Cannot find Inspect program file: " + progLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Create a file watcher to monitor Search Log created by Inspect
            // This file is updated after each chunk of 100 spectra are processed
            // The 4th column of this file displays the PercentComplete value for the overall search
            InitializeInspectSearchLogFileWatcher(mWorkDir);

            // Let the user know what went wrong.
            LogMessage("Starting Inspect");

            // Set up and execute a program runner to run Inspect.exe
            var arguments = " -i " + mInspectCustomParamFileName +
                            " -o " + mInspectResultsFilePath +
                            " -e " + mInspectErrorFilePath;

            if (mDebugLevel >= 1)
            {
                LogDebug(progLoc + " " + arguments);
            }

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = true;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = mInspectConsoleOutputFilePath;

            if (!mCmdRunner.RunProgram(progLoc, arguments, "Inspect", true))
            {
                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("Inspect returned a non-zero exit code: " + mCmdRunner.ExitCode);
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
                        LogError("Exit code indicates user pressed Ctrl+Break; job failed");
                        break;
                    default:
                        // Any other code
                        LogError("Unknown exit code; job failed");
                        break;
                }

                if (mCmdRunner.ExitCode != 0)
                {
                    if (mInspectSearchLogMostRecentEntry.Length > 0)
                    {
                        LogWarning("Most recent Inspect search log entry: " + mInspectSearchLogMostRecentEntry);
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
                LogError("Error running Inspect");
            }
            else
            {
                mProgress = 100;
                UpdateStatusRunning();
            }

            if (mSearchLogFileWatcher != null)
            {
                mSearchLogFileWatcher.EnableRaisingEvents = false;
                mSearchLogFileWatcher = null;
            }

            // Parse the _errors.txt file (if it exists) and copy any errors to the analysis manager log
            ParseInspectErrorsFile(mInspectErrorFilePath);

            // Even though success is returned, check for the result file
            if (File.Exists(mInspectResultsFilePath))
            {
                blnSuccess = true;
            }
            else
            {
                LogError("Inspect results file not found; job failed: " + Path.GetFileName(mInspectResultsFilePath));
                blnSuccess = false;
            }

            if (blnSuccess)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            return CloseOutType.CLOSEOUT_FAILED;
        }

        private DateTime mLastStatusUpdate = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            // Update the status file (limit the updates to every 5 seconds)
            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds >= 5)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                UpdateStatusRunning(mProgress, mDtaCount);
            }

            LogProgress("Inspect");
        }

        private void ParseInspectSearchLogFile(string strSearchLogFilePath)
        {
            try
            {
                var ioFile = new FileInfo(strSearchLogFilePath);
                if (ioFile.Exists && ioFile.Length > 0)
                {
                    // Search log file has been updated
                    // Open the file and read the contents
                    string strLastEntry = null;

                    using (var reader = new StreamReader(new FileStream(strSearchLogFilePath, FileMode.Open, FileAccess.Read, FileShare.Write)))
                    {
                        // Read to the end of the file
                        while (!reader.EndOfStream)
                        {
                            var dataLine = reader.ReadLine();

                            if (!string.IsNullOrEmpty(dataLine))
                            {
                                strLastEntry = string.Copy(dataLine);
                            }
                        }
                    }

                    if (!string.IsNullOrEmpty(strLastEntry))
                    {
                        if (mDebugLevel >= 4)
                        {
                            // Store the new search log entry in the log
                            if (mInspectSearchLogMostRecentEntry.Length == 0 || mInspectSearchLogMostRecentEntry != strLastEntry)
                            {
                                LogDebug("Inspect search log entry: " + strLastEntry);
                            }
                        }

                        // Cache the log entry
                        mInspectSearchLogMostRecentEntry = string.Copy(strLastEntry);

                        var dataCols = strLastEntry.Split('\t');

                        if (dataCols.Length >= 4)
                        {
                            // Parse out the number of spectra from the 3rd column
                            int.TryParse(dataCols[2], out mDtaCount);

                            // Parse out the % complete from the 4th column
                            // Use .TrimEnd to remove the trailing % sign
                            var strProgress = dataCols[3].TrimEnd('%');
                            float.TryParse(strProgress, out mProgress);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("AnalysisToolRunnerIN.ParseInspectSearchLogFile, error reading Inspect search log" + ex.Message);
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        protected bool StoreToolVersionInfo(string strInspectFolder)
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(Path.Combine(strInspectFolder, INSPECT_EXE_NAME))
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Event handler for mSearchLogFileWatcher.Changed event
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void SearchLogFileWatcher_Changed(object sender, FileSystemEventArgs e)
        {
            ParseInspectSearchLogFile(mInspectSearchLogFilePath);
        }

        #endregion
    }
}
