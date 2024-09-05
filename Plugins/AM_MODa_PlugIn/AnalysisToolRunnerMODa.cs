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
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMODaPlugIn
{
    /// <summary>
    /// Class for running MODa analysis
    /// </summary>
    public class AnalysisToolRunnerMODa : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: datasest, fasta, Hanyang, moda, MODa, msms, Xmx

        // ReSharper restore CommentTypo

        private const string MODa_CONSOLE_OUTPUT = "MODa_ConsoleOutput.txt";
        private const string MODa_JAR_NAME = "moda.jar";

        private const int PROGRESS_PCT_STARTING = 1;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private const string MODA_RESULTS_FILE_SUFFIX = "_moda.txt";

        private bool mToolVersionWritten;
        private string mMODaVersion;

        private string mMODaProgLoc;
        private string mConsoleOutputErrorMsg;

        private string mMODaResultsFilePath;

        private RunDosProgram mCmdRunner;

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

                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerMODa.RunTool(): Enter");
                }

                mMODaResultsFilePath = string.Empty;

                // Verify that program files exist

                // javaProgLoc will typically be "C:\Program Files\Java\jre11\bin\java.exe"
                var javaProgLoc = GetJavaProgLoc();

                if (string.IsNullOrEmpty(javaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to the MODa program
                mMODaProgLoc = DetermineProgramLocation("MODaProgLoc", MODa_JAR_NAME);

                if (string.IsNullOrWhiteSpace(mMODaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run MODa, then post-process the results
                var processingSuccess = StartMODa(javaProgLoc);

                if (processingSuccess)
                {
                    // Look for the MODa results file
                    // If it exists, zip it
                    var resultsFile = new FileInfo(mMODaResultsFilePath);

                    if (resultsFile.Exists)
                    {
                        // Zip the output file
                        var zipSuccess = ZipOutputFile(resultsFile, "MODa");

                        if (zipSuccess)
                        {
                            mJobParams.AddResultFileToSkip(resultsFile.Name);
                        }
                        else
                        {
                            if (string.IsNullOrEmpty(mMessage))
                            {
                                mMessage = "Unknown error zipping the MODa results";
                            }
                            processingSuccess = false;
                        }
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(mMessage))
                        {
                            mMessage = "MODa results file not found: " + Path.GetFileName(mMODaResultsFilePath);
                        }
                        processingSuccess = false;
                    }
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                // Trim the console output file to remove the majority of the status messages (since there is currently one per scan)
                TrimConsoleOutputFile(Path.Combine(mWorkDir, MODa_CONSOLE_OUTPUT));

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in MODaPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool StartMODa(string javaProgLoc)
        {
            // We will store the MODa version info in the database after the header block is written to file MODa_ConsoleOutput.txt

            mToolVersionWritten = false;
            mMODaVersion = string.Empty;
            mConsoleOutputErrorMsg = string.Empty;

            // Customize the parameter file
            var paramFileName = mJobParams.GetParam("ParamFileName");

            var spectrumFileName = mDatasetName + ".mgf";

            // Define the path to the FASTA file
            // Note that job parameter "GeneratedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
            var localOrgDbFolder = mMgrParams.GetParam("OrgDbDir");
            var dbFilename = mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName");
            var fastaFilePath = Path.Combine(localOrgDbFolder, dbFilename);

            if (!UpdateParameterFile(paramFileName, spectrumFileName, fastaFilePath))
            {
                return false;
            }

            LogMessage("Running MODa");

            // Lookup the amount of memory to reserve for Java; default to 2 GB
            var javaMemorySize = mJobParams.GetJobParameter("MODaJavaMemorySize", 2000);

            if (javaMemorySize < 512)
                javaMemorySize = 512;

            var paramFilePath = Path.Combine(mWorkDir, paramFileName);
            mMODaResultsFilePath = Path.Combine(mWorkDir, mDatasetName + MODA_RESULTS_FILE_SUFFIX);

            // Set up and execute a program runner to run MODa
            var arguments = " -Xmx" + javaMemorySize + "M" +
                            " -jar " + mMODaProgLoc +
                            " -i " + paramFilePath +
                            " -o " + mMODaResultsFilePath;

            LogDebug(javaProgLoc + " " + arguments);

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, MODa_CONSOLE_OUTPUT);

            mProgress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var success = mCmdRunner.RunProgram(javaProgLoc, arguments, "MODa", true);

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mMODaVersion))
                {
                    ParseConsoleOutputFile(Path.Combine(mWorkDir, MODa_CONSOLE_OUTPUT));
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!success)
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

            mProgress = PROGRESS_PCT_COMPLETE;
            mStatusTools.UpdateAndWrite(mProgress);

            if (mDebugLevel >= 3)
            {
                LogDebug("MODa Search Complete");
            }

            return true;
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileToSkip(Dataset + ".mzXML");

            base.CopyFailedResultsToArchiveDirectory();
        }

        // ReSharper disable CommentTypo

        // MODa console output shows datasest instead of dataset

        // Example Console output

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

        // moda | 2 at 2-thread
        // moda | 3 at 3-thread
        // moda | 4 at 4-thread
        // moda | 5 at 5-thread
        // moda | 11 at 10-thread

        // ReSharper restore CommentTypo

        private readonly Regex mScanCountMatcher = new(@"Reading .+spectra[. ]+(?<ScanCount>\d+) +scans",
                                                             RegexOptions.Compiled | RegexOptions.IgnoreCase);

        // Look for lines of the form MOD-A | 6947/13253
        // This format was used in older versions of MODa
        private readonly Regex mCurrentScanMatcherV1 = new(@"MOD-A \| (?<ScansProcessed>\d+)/(?<ScanCount>\d+)",
                                                                 RegexOptions.Compiled | RegexOptions.IgnoreCase);

        // Look for lines of the form moda | 10926 at 15-thread
        private readonly Regex mCurrentScanMatcherV2 = new(@"moda \| (?<ScansProcessed>\d+) at (?<ThreadNum>\d+-thread)",
                                                                 RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the MODa console output file to determine the MODa version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 4)
                {
                    LogDebug("Parsing file " + consoleOutputFilePath);
                }

                int linesRead;

                var scansProcessed = 0;
                var totalScans = 0;
                var modaVersionAndDate = string.Empty;

                mConsoleOutputErrorMsg = string.Empty;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    linesRead = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead++;

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (linesRead < 6 && string.IsNullOrEmpty(modaVersionAndDate) && dataLine.StartsWith("moda", StringComparison.OrdinalIgnoreCase))
                        {
                            modaVersionAndDate = dataLine;
                            continue;
                        }

                        if (linesRead < 6 && dataLine.StartsWith("release date", StringComparison.OrdinalIgnoreCase))
                        {
                            modaVersionAndDate += ", " + dataLine;
                            continue;
                        }

                        if (dataLine.StartsWith("abnormal termination", StringComparison.OrdinalIgnoreCase))
                        {
                            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg = "Error running MODa:";
                            }
                            mConsoleOutputErrorMsg += "; " + dataLine;
                            continue;
                        }

                        if (dataLine.IndexOf("failed to read msms spectra file", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg = "Error running MODa:";
                            }
                            mConsoleOutputErrorMsg += "; FASTA file not found";
                            continue;
                        }

                        if (dataLine.IndexOf("exception", StringComparison.OrdinalIgnoreCase) >=0 && dataLine.StartsWith("Java", StringComparison.OrdinalIgnoreCase))
                        {
                            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg = "Error running MODa:";
                            }
                            mConsoleOutputErrorMsg += "; " + dataLine;
                            continue;
                        }

                        if (totalScans == 0)
                        {
                            var scanCountMatch = mScanCountMatcher.Match(dataLine);

                            if (scanCountMatch.Success)
                            {
                                totalScans = int.Parse(scanCountMatch.Groups["ScanCount"].Value);
                            }
                        }

                        var matchA = mCurrentScanMatcherV1.Match(dataLine);

                        if (matchA.Success)
                        {
                            scansProcessed = int.Parse(matchA.Groups["ScansProcessed"].Value);

                            if (totalScans == 0)
                            {
                                totalScans = int.Parse(matchA.Groups["ScanCount"].Value);
                            }
                        }
                        else
                        {
                            var matchB = mCurrentScanMatcherV2.Match(dataLine);

                            if (matchB.Success)
                            {
                                if (true)
                                {
                                    scansProcessed = int.Parse(matchB.Groups["ScansProcessed"].Value);
                                }
                            }
                        }
                    }
                }

                if (linesRead >= 5 && string.IsNullOrEmpty(mMODaVersion) && !string.IsNullOrEmpty(modaVersionAndDate))
                {
                    mMODaVersion = modaVersionAndDate;
                }

                var actualProgress = ComputeIncrementalProgress(PROGRESS_PCT_STARTING, PROGRESS_PCT_COMPLETE, scansProcessed, totalScans);

                if (mProgress < actualProgress)
                {
                    mProgress = actualProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var toolVersionInfo = mMODaVersion;

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(mMODaProgLoc)
            };

            try
            {
                // Tool_Version_Info_MODa.txt is required by IDPicker
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
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
        /// <param name="consoleOutputFilePath"></param>
        private void TrimConsoleOutputFile(string consoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 4)
                {
                    LogDebug("Trimming console output file at " + consoleOutputFilePath);
                }

                var consoleOutputFile = new FileInfo(consoleOutputFilePath);
                var trimmedFilePath = new FileInfo(consoleOutputFilePath + ".trimmed");

                using (var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(trimmedFilePath.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var scanNumberOutputThreshold = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (dataLine == null)
                        {
                            writer.WriteLine();
                            continue;
                        }

                        var keepLine = true;
                        int scansProcessed;

                        var matchA = mCurrentScanMatcherV1.Match(dataLine);

                        if (matchA.Success)
                        {
                            scansProcessed = int.Parse(matchA.Groups["ScansProcessed"].Value);
                        }
                        else
                        {
                            var matchB = mCurrentScanMatcherV2.Match(dataLine);

                            if (matchB.Success)
                            {
                                scansProcessed = int.Parse(matchB.Groups["ScansProcessed"].Value);
                            }
                            else
                            {
                                scansProcessed = 0;
                            }
                        }

                        if (scansProcessed > 0)
                        {
                            if (scansProcessed < scanNumberOutputThreshold)
                            {
                                keepLine = false;
                            }
                            else
                            {
                                // Write out this line and bump up scanNumberOutputThreshold by 100
                                scanNumberOutputThreshold += 100;
                            }
                        }

                        if (keepLine)
                        {
                            writer.WriteLine(dataLine);
                        }
                    }
                }

                // Replace the original file with the new one
                ReplaceUpdatedFile(consoleOutputFile, trimmedFilePath);
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error trimming console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private bool UpdateParameterFile(string paramFileName, string spectrumFileName, string fastaFilePath)
        {
            const string SPEC_FILE_PATH = "spectra";
            const string FASTA_FILE_PATH = "fasta";

            var specFileDefined = false;
            var fastaFileDefined = false;

            try
            {
                var sourceParamFile = new FileInfo(Path.Combine(mWorkDir, paramFileName));
                var tempParamFile = new FileInfo(Path.Combine(mWorkDir, paramFileName + ".temp"));

                var specFile = new FileInfo(Path.Combine(mWorkDir, spectrumFileName));

                // Open the input file
                using (var reader = new StreamReader(new FileStream(sourceParamFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                // Create the output file
                using (var writer = new StreamWriter(new FileStream(tempParamFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine) || dataLine.TrimStart().StartsWith("#"))
                        {
                            // Comment line or blank line; write it out as-is
                            writer.WriteLine(dataLine);
                            continue;
                        }

                        // Look for an equals sign
                        var equalsIndex = dataLine.IndexOf('=');

                        if (equalsIndex <= 0)
                        {
                            // Unknown line format; skip it
                            continue;
                        }

                        // Split the line on the equals sign
                        var keyName = dataLine.Substring(0, equalsIndex).TrimEnd();

                        // Examine the key name to determine what to do
                        switch (keyName.ToLower())
                        {
                            case SPEC_FILE_PATH:
                                dataLine = SPEC_FILE_PATH + "=" + specFile.FullName;
                                specFileDefined = true;

                                break;
                            case FASTA_FILE_PATH:
                                dataLine = FASTA_FILE_PATH + "=" + fastaFilePath;
                                fastaFileDefined = true;

                                break;
                        }

                        writer.WriteLine(dataLine);
                    }

                    if (!specFileDefined)
                    {
                        writer.WriteLine();
                        writer.WriteLine(SPEC_FILE_PATH + "=" + specFile.FullName);
                    }

                    if (!fastaFileDefined)
                    {
                        writer.WriteLine();
                        writer.WriteLine(FASTA_FILE_PATH + "=" + fastaFilePath);
                    }
                }

                // Replace the original parameter file with the updated one
                if (!ReplaceUpdatedFile(sourceParamFile, tempParamFile))
                {
                    mMessage = "Error replacing the original parameter file with the customized version";
                    return false;
                }
            }
            catch (Exception ex)
            {
                mMessage = "Exception in UpdateParameterFile";
                LogError("Exception in UpdateParameterFile: " + ex.Message);
                return false;
            }

            return true;
        }

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(mWorkDir, MODa_CONSOLE_OUTPUT));

                if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mMODaVersion))
                {
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

                LogProgress("MODa");
            }
        }
    }
}
