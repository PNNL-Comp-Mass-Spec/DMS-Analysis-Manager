//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 03/26/2014
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerMODaPlugIn
{
    /// <summary>
    /// Class for running MODa analysis
    /// </summary>
    public class clsAnalysisToolRunnerMODa : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        protected const string MODa_CONSOLE_OUTPUT = "MODa_ConsoleOutput.txt";
        protected const string MODa_JAR_NAME = "moda.jar";

        protected const string REGEX_MODa_PROGRESS = @"MOD-A \| (\d+)/(\d+)";

        protected const float PROGRESS_PCT_STARTING = 1;
        protected const float PROGRESS_PCT_COMPLETE = 99;

        protected const string MODA_RESULTS_FILE_SUFFIX = "_moda.txt";

        #endregion

        #region "Module Variables"

        protected bool mToolVersionWritten;
        protected string mMODaVersion;

        protected string mMODaProgLoc;
        protected string mConsoleOutputErrorMsg;

        protected string mMODaResultsFilePath;

        protected clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs MODa tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerMODa.RunTool(): Enter");
                }

                mMODaResultsFilePath = string.Empty;

                // Verify that program files exist

                // JavaProgLoc will typically be "C:\Program Files\Java\jre8\bin\java.exe"
                var JavaProgLoc = GetJavaProgLoc();
                if (string.IsNullOrEmpty(JavaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to the MODa program
                mMODaProgLoc = DetermineProgramLocation("MODaProgLoc", MODa_JAR_NAME);

                if (string.IsNullOrWhiteSpace(mMODaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run MODa, then post process the results
                var processingSuccess = StartMODa(JavaProgLoc);

                if (processingSuccess)
                {
                    // Look for the MODa results file
                    // If it exists, zip it
                    var fiResultsFile = new FileInfo(mMODaResultsFilePath);

                    if (fiResultsFile.Exists)
                    {
                        // Zip the output file
                        var zipSuccess = ZipOutputFile(fiResultsFile, "MODa");

                        if (zipSuccess)
                        {
                            m_jobParams.AddResultFileToSkip(fiResultsFile.Name);
                        }
                        else
                        {
                            if (string.IsNullOrEmpty(m_message))
                            {
                                m_message = "Unknown error zipping the MODa results";
                            }
                            processingSuccess = false;
                        }
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            m_message = "MODa results file not found: " + Path.GetFileName(mMODaResultsFilePath);
                        }
                        processingSuccess = false;
                    }
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                Thread.Sleep(500);
                PRISM.clsProgRunner.GarbageCollectNow();

                // Trim the console output file to remove the majority of the status messages (since there is currently one per scan)
                TrimConsoleOutputFile(Path.Combine(m_WorkDir, MODa_CONSOLE_OUTPUT));

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Error in MODaPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        protected bool StartMODa(string JavaProgLoc)
        {
            // We will store the MODa version info in the database after the header block is written to file MODa_ConsoleOutput.txt

            mToolVersionWritten = false;
            mMODaVersion = string.Empty;
            mConsoleOutputErrorMsg = string.Empty;

            // Customize the parameter file
            var paramFileName = m_jobParams.GetParam("ParmFileName");

            var spectrumFileName = m_Dataset + ".mgf";

            // Define the path to the fasta file
            // Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
            var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
            var dbFilename = m_jobParams.GetParam("PeptideSearch", "generatedFastaName");
            var fastaFilePath = Path.Combine(localOrgDbFolder, dbFilename);

            if (!UpdateParameterFile(paramFileName, spectrumFileName, fastaFilePath))
            {
                return false;
            }

            LogMessage("Running MODa");

            // Lookup the amount of memory to reserve for Java; default to 2 GB
            var intJavaMemorySize = m_jobParams.GetJobParameter("MODaJavaMemorySize", 2000);
            if (intJavaMemorySize < 512)
                intJavaMemorySize = 512;

            var paramFilePath = Path.Combine(m_WorkDir, paramFileName);
            mMODaResultsFilePath = Path.Combine(m_WorkDir, m_Dataset + MODA_RESULTS_FILE_SUFFIX);

            // Set up and execute a program runner to run MODa
            var cmdStr = " -Xmx" + intJavaMemorySize + "M -jar " + mMODaProgLoc;
            cmdStr += " -i " + paramFilePath;
            cmdStr += " -o " + mMODaResultsFilePath;

            LogDebug(JavaProgLoc + " " + cmdStr);

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, MODa_CONSOLE_OUTPUT);

            m_progress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var blnSuccess = mCmdRunner.RunProgram(JavaProgLoc, cmdStr, "MODa", true);

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mMODaVersion))
                {
                    ParseConsoleOutputFile(Path.Combine(m_WorkDir, MODa_CONSOLE_OUTPUT));
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!blnSuccess)
            {
                LogError("Error running MODa");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("MODa returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to MODa failed (but exit code is 0)");
                }

                return false;
            }

            m_progress = PROGRESS_PCT_COMPLETE;
            m_StatusTools.UpdateAndWrite(m_progress);
            if (m_DebugLevel >= 3)
            {
                LogDebug("MODa Search Complete");
            }

            return true;
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveFolder()
        {
            m_jobParams.AddResultFileToSkip(Dataset + ".mzXML");

            base.CopyFailedResultsToArchiveFolder();
        }

        // Example Console output
        //
        // *********************************************************
        // MODa v1.20: Multi-Blind Modification Search
        // Release Date: February 01, 2013
        // Hanyang University, Seoul, Korea
        // *********************************************************
        //
        // Reading parameter.....
        // Input datasest : E:\DMS_WorkDir\QC_Shew_13_05b_CID_500ng_24Mar14_Tiger_14-03-04.mgf
        // Input datasest : E:\DMS_WorkDir\ID_003456_9B916A8B_Decoy_Scrambled.fasta
        //
        // Starting MOD-Alignment for multi-blind modification search!
        // Performing mass correction for precursor
        // Reading MS/MS spectra.....  132 scans
        // Reading protein database.....  8632 proteins / 2820896 residues (1)

        // MOD-A | 1/132
        // MOD-A | 2/132
        // MOD-A | 3/132
        // Look for lines of the form MOD-A | 6947/13253
        private readonly Regex reExtractScan = new Regex(REGEX_MODa_PROGRESS, RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the MODa console output file to determine the MODa version and to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Parsing file " + strConsoleOutputFilePath);
                }

                int intLinesRead;

                var intScansProcessed = 0;
                var intTotalScans = 0;
                var strMODaVersionAndDate = string.Empty;

                mConsoleOutputErrorMsg = string.Empty;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    intLinesRead = 0;
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();
                        intLinesRead += 1;

                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        var strLineInLCase = strLineIn.ToLower();

                        if (intLinesRead < 6 && string.IsNullOrEmpty(strMODaVersionAndDate) && strLineInLCase.StartsWith("moda"))
                        {
                            strMODaVersionAndDate = string.Copy(strLineIn);
                            continue;
                        }

                        if (intLinesRead < 6 && strLineInLCase.StartsWith("release date"))
                        {
                            strMODaVersionAndDate += ", " + strLineIn;
                            continue;
                        }

                        if (strLineInLCase.StartsWith("abnormal termination"))
                        {
                            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg = "Error running MODa:";
                            }
                            mConsoleOutputErrorMsg += "; " + strLineIn;
                            continue;
                        }

                        if (strLineInLCase.Contains("failed to read msms spectra file"))
                        {
                            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg = "Error running MODa:";
                            }
                            mConsoleOutputErrorMsg += "; Fasta file not found";
                            continue;
                        }

                        if (strLineInLCase.Contains("exception") && strLineInLCase.StartsWith("java"))
                        {
                            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg = "Error running MODa:";
                            }
                            mConsoleOutputErrorMsg += "; " + strLineIn;
                            continue;
                        }

                        var oMatch = reExtractScan.Match(strLineIn);
                        if (oMatch.Success)
                        {
                            int intValue;
                            if (int.TryParse(oMatch.Groups[1].Value, out intValue))
                            {
                                intScansProcessed = intValue;
                            }

                            if (intTotalScans == 0)
                            {
                                if (int.TryParse(oMatch.Groups[2].Value, out intValue))
                                {
                                    intTotalScans = intValue;
                                }
                            }
                        }
                    }
                }

                if (intLinesRead >= 5 && string.IsNullOrEmpty(mMODaVersion) && !string.IsNullOrEmpty(strMODaVersionAndDate))
                {
                    mMODaVersion = strMODaVersionAndDate;
                }

                var sngActualProgress = ComputeIncrementalProgress(PROGRESS_PCT_STARTING, PROGRESS_PCT_COMPLETE, intScansProcessed, intTotalScans);

                if (m_progress < sngActualProgress)
                {
                    m_progress = sngActualProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var strToolVersionInfo = string.Copy(mMODaVersion);

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo> {
                new FileInfo(mMODaProgLoc)
            };

            try
            {
                // Tool_Version_Info_MODa.txt is required by IDPicker
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, saveToolVersionTextFile: true);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Reads the console output file and removes the majority of the percent finished messages
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void TrimConsoleOutputFile(string strConsoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Trimming console output file at " + strConsoleOutputFilePath);
                }

                var fiConsoleOutputFile = new FileInfo(strConsoleOutputFilePath);
                var fiTrimmedFilePath = new FileInfo(strConsoleOutputFilePath + ".trimmed");

                using (var srInFile = new StreamReader(new FileStream(fiConsoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var swOutFile = new StreamWriter(new FileStream(fiTrimmedFilePath.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var intScanNumberOutputThreshold = 0;
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (strLineIn == null)
                        {
                            swOutFile.WriteLine();
                            continue;
                        }

                        var blnKeepLine = true;

                        var oMatch = reExtractScan.Match(strLineIn);
                        if (oMatch.Success)
                        {
                            if (int.TryParse(oMatch.Groups[1].Value, out var intScanNumber))
                            {
                                if (intScanNumber < intScanNumberOutputThreshold)
                                {
                                    blnKeepLine = false;
                                }
                                else
                                {
                                    // Write out this line and bump up intScanNumberOutputThreshold by 100
                                    intScanNumberOutputThreshold += 100;
                                }
                            }
                        }

                        if (blnKeepLine)
                        {
                            swOutFile.WriteLine(strLineIn);
                        }
                    }
                }

                // Replace the original file with the new one
                ReplaceUpdatedFile(fiConsoleOutputFile, fiTrimmedFilePath);
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error trimming console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        protected bool UpdateParameterFile(string paramFileName, string spectrumFileName, string fastaFilePath)
        {
            const string SPEC_FILE_PATH = "spectra"; // "Spectra"
            const string FASTA_FILE_PATH = "fasta"; // "Fasta"

            var specFileDefined = false;
            var fastaFileDefined = false;

            try
            {
                var fiSourceParamFile = new FileInfo(Path.Combine(m_WorkDir, paramFileName));
                var fiTempParamFile = new FileInfo(Path.Combine(m_WorkDir, paramFileName + ".temp"));

                var fiSpecFile = new FileInfo(Path.Combine(m_WorkDir, spectrumFileName));

                // Open the input file
                using (var srInFile = new StreamReader(new FileStream(fiSourceParamFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                // Create the output file
                using (var swOutFile = new StreamWriter(new FileStream(fiTempParamFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn) || strLineIn.TrimStart().StartsWith("#"))
                        {
                            // Comment line or blank line; write it out as-is
                            swOutFile.WriteLine(strLineIn);
                            continue;
                        }

                        // Look for an equals sign
                        var intEqualsIndex = strLineIn.IndexOf('=');

                        if (intEqualsIndex <= 0)
                        {
                            // Unknown line format; skip it
                            continue;
                        }

                        // Split the line on the equals sign
                        var strKeyName = strLineIn.Substring(0, intEqualsIndex).TrimEnd();

                        // Examine the key name to determine what to do
                        switch (strKeyName.ToLower())
                        {
                            case SPEC_FILE_PATH:
                                strLineIn = SPEC_FILE_PATH + "=" + fiSpecFile.FullName;
                                specFileDefined = true;

                                break;
                            case FASTA_FILE_PATH:
                                strLineIn = FASTA_FILE_PATH + "=" + fastaFilePath;
                                fastaFileDefined = true;

                                break;
                        }

                        swOutFile.WriteLine(strLineIn);
                    }

                    if (!specFileDefined)
                    {
                        swOutFile.WriteLine();
                        swOutFile.WriteLine(SPEC_FILE_PATH + "=" + fiSpecFile.FullName);
                    }

                    if (!fastaFileDefined)
                    {
                        swOutFile.WriteLine();
                        swOutFile.WriteLine(FASTA_FILE_PATH + "=" + fastaFilePath);
                    }
                }

                // Replace the original parameter file with the updated one
                if (!ReplaceUpdatedFile(fiSourceParamFile, fiTempParamFile))
                {
                    m_message = "Error replacing the original parameter file with the customized version";
                    return false;
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception in UpdateParameterFile";
                LogError("Exception in UpdateParameterFile: " + ex.Message);
                return false;
            }

            return true;
        }

        #endregion

        #region "Event Handlers"

        private DateTime dtLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE)
            {
                dtLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(m_WorkDir, MODa_CONSOLE_OUTPUT));

                if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mMODaVersion))
                {
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

                LogProgress("MODa");
            }
        }

        #endregion
    }
}
