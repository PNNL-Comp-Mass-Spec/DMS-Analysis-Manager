//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 01/30/2015
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISM;

namespace AnalysisManagerProMexPlugIn
{
    /// <summary>
    /// Class for running ProMex to deisotope high resolution spectra
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerProMex : AnalysisToolRunnerBase
    {
        // Ignore Spelling: Csv, Da, deisotope, deisotoped, ver

        private const string PROMEX_CONSOLE_OUTPUT = "ProMex_ConsoleOutput.txt";

        private const int PROGRESS_PCT_STARTING = 1;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private string mConsoleOutputErrorMsg;

        private string mProMexParamFileName;

        private RunDosProgram mCmdRunner;

        /// <summary>
        /// Runs ProMex
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
                    LogDebug("AnalysisToolRunnerProMex.RunTool(): Enter");
                }

                // Determine the path to the ProMex program
                var progLoc = DetermineProgramLocation("ProMexProgLoc", "ProMex.exe");

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the ProMex version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining ProMex version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run ProMex
                var processingSuccess = StartProMex(progLoc);

                if (processingSuccess)
                {
                    // Look for the results file

                    var resultsFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_MS1FT_EXTENSION));

                    if (resultsFile.Exists)
                    {
                        var postProcessSuccess = PostProcessProMexResults(resultsFile);

                        if (!postProcessSuccess)
                        {
                            if (string.IsNullOrEmpty(mMessage))
                            {
                                mMessage = "Unknown error post-processing the ProMex results";
                            }
                            processingSuccess = false;
                        }
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(mMessage))
                        {
                            mMessage = "ProMex results file not found: " + resultsFile.Name;
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
                AppUtils.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // There is no need to keep the parameter file since it is fairly simple, and the ProMex_ConsoleOutput.txt file displays every parameter used
                mJobParams.AddResultFileToSkip(mProMexParamFileName);

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in ProMexPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory(bool includeSubdirectories = false)
        {
            mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_MZXML_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory(includeSubdirectories);
        }

        private const string REGEX_ProMex_PROGRESS = @"Processing ([0-9.]+)\%";
        private readonly Regex reCheckProgress = new(REGEX_ProMex_PROGRESS, RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the ProMex console output file to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example Console output

            // ****** ProMex   ver. 1.0 (Jan 29, 2014) ************
            // -i      CPTAC_Intact_100k_01_Run1_9Dec14_Bane_C2-14-08-02RZ.pbf
            // -minCharge      2
            // -maxCharge      60
            // -minMass        3000.0
            // -maxMass        50000.0
            // -score  n
            // -csv    y
            // -maxThreads     0
            // Start loading MS1 data from CPTAC_Intact_100k_01_Run1_9Dec14_Bane_C2-14-08-02RZ.pbf
            // Complete loading MS1 data. Elapsed Time = 17.514 sec
            // Start MS1 feature extracting...
            // Mass Range 3000 - 50000
            // Charge Range 2 - 60
            // Output File     CPTAC_Intact_100k_01_Run1_9Dec14_Bane_C2-14-08-02RZ.ms1ft
            // Csv Output File CPTAC_Intact_100k_01_Run1_9Dec14_Bane_C2-14-08-02RZ_ms1ft.csv
            // Processing 2.25 % of mass bins (3187.563 Da); Elapsed Time = 16.035 sec; # of features = 283
            // Processing 4.51 % of mass bins (3375.063 Da); Elapsed Time = 26.839 sec; # of features = 770
            // Processing 6.76 % of mass bins (3562.563 Da); Elapsed Time = 40.169 sec; # of features = 1426
            // Processing 9.02 % of mass bins (3750.063 Da); Elapsed Time = 51.633 sec; # of features = 2154

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

                // Value between 0 and 100
                float progressComplete = 0;
                mConsoleOutputErrorMsg = string.Empty;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var dataLineLCase = dataLine.ToLower();

                        if (dataLineLCase.StartsWith("error:") || dataLineLCase.Contains("unhandled exception"))
                        {
                            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg = "Error running ProMex:";
                            }

                            if (dataLineLCase.StartsWith("error:"))
                            {
                                mConsoleOutputErrorMsg += " " + dataLine.Substring("error:".Length).Trim();
                            }
                            else
                            {
                                mConsoleOutputErrorMsg += " " + dataLine;
                            }

                            continue;
                        }

                        var match = reCheckProgress.Match(dataLine);

                        if (match.Success)
                        {
                            float.TryParse(match.Groups[1].ToString(), out progressComplete);
                        }
                    }
                }

                if (mProgress < progressComplete)
                {
                    mProgress = progressComplete;
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

        private bool PostProcessProMexResults(FileSystemInfo resultsFile)
        {
            // Make sure there are at least two features in the .ms1ft file

            try
            {
                using (var resultsReader = new StreamReader(new FileStream(resultsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    var lineCount = 0;

                    while (!resultsReader.EndOfStream)
                    {
                        var lineIn = resultsReader.ReadLine();

                        if (!string.IsNullOrEmpty(lineIn))
                        {
                            lineCount++;

                            if (lineCount > 2)
                            {
                                return true;
                            }
                        }
                    }
                }

                mMessage = "The ProMex results file has fewer than 2 deisotoped features";

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception examining the ms1ft file: " + ex.Message);
                return false;
            }
        }

        private bool StartProMex(string progLoc)
        {
            mConsoleOutputErrorMsg = string.Empty;

            mProMexParamFileName = mJobParams.GetParam("ProMexParamFile");

            var binResolutionPPM = mJobParams.GetJobParameter("ProMex", "BinResolutionPPM", 0);

            string msFilePath;

            var proMexBruker = AnalysisResourcesProMex.IsProMexBrukerJob(mJobParams);

            if (proMexBruker)
            {
                msFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_MZML_EXTENSION);
            }
            else
            {
                msFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_PBF_EXTENSION);
            }

            LogMessage("Running ProMex");

            // Set up and execute a program runner to run ProMex

            var binResolutionArgument = binResolutionPPM == 0 ? string.Empty : string.Format(" -BinningResolutionPPM:{0}", binResolutionPPM);

            var arguments =
                " -i:" + msFilePath +
                binResolutionArgument +
                " -ParamFile:" + mProMexParamFileName;

            if (mDebugLevel >= 1)
            {
                LogDebug(progLoc + arguments);
            }

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, PROMEX_CONSOLE_OUTPUT);

            mProgress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var success = mCmdRunner.RunProgram(progLoc, arguments, "ProMex", true);

            if (!mCmdRunner.WriteConsoleOutputToFile)
            {
                // Write the console output to a text file
                Global.IdleLoop(0.25);

                using var writer = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine(mCmdRunner.CachedConsoleOutput);
            }

            // Parse the console output file one more time to check for errors
            Global.IdleLoop(0.25);
            ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!success)
            {
                LogError("Error running ProMex");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("ProMex returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to ProMex failed (but exit code is 0)");
                }

                return false;
            }

            if (mConsoleOutputErrorMsg.Contains("Data file has no MS1 spectra"))
            {
                mMessage = mConsoleOutputErrorMsg;
                return false;
            }

            if (proMexBruker)
            {
                success = StorePbfFileInCache();

                if (!success)
                {
                    return false;
                }
            }

            mProgress = PROGRESS_PCT_COMPLETE;
            mStatusTools.UpdateAndWrite(mProgress);

            if (mDebugLevel >= 3)
            {
                LogDebug("ProMex Search Complete");
            }

            return true;
        }

        private bool StorePbfFileInCache()
        {
            var autoGeneratedPbfFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_PBF_EXTENSION));

            if (!autoGeneratedPbfFile.Exists)
            {
                LogError("Auto-generated .PBF file not found; this is unexpected: " + autoGeneratedPbfFile.Name);
                return false;
            }

            // Store the PBF file in the spectra cache folder; not in the job result folder

            var msXmlCacheFolderPath = mMgrParams.GetParam("MSXMLCacheFolderPath", string.Empty);
            var msXmlCacheFolder = new DirectoryInfo(msXmlCacheFolderPath);

            if (!msXmlCacheFolder.Exists)
            {
                LogError("MSXmlCache folder not found: " + msXmlCacheFolderPath);
                return false;
            }

            // Temporarily override the result folder name
            var resultFolderNameSaved = mResultsDirectoryName;

            mResultsDirectoryName = "PBF_Gen_1_193_000000";

            // Copy the .pbf file to the MSXML cache
            var remoteCacheFilePath = CopyFileToServerCache(msXmlCacheFolder.FullName, autoGeneratedPbfFile.FullName, purgeOldFilesIfNeeded: true);

            // Restore the result folder name
            mResultsDirectoryName = resultFolderNameSaved;

            if (string.IsNullOrEmpty(remoteCacheFilePath))
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    LogError("CopyFileToServerCache returned false for " + autoGeneratedPbfFile.Name);
                }
                return false;
            }

            // Create the _CacheInfo.txt file
            var cacheInfoFilePath = autoGeneratedPbfFile.FullName + "_CacheInfo.txt";
            using (var writer = new StreamWriter(new FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                writer.WriteLine(remoteCacheFilePath);
            }

            mJobParams.AddResultFileToSkip(autoGeneratedPbfFile.Name);

            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDLLs = new List<string>
            {
                "InformedProteomics.Backend.dll"
            };

            return StoreDotNETToolVersionInfo(progLoc, additionalDLLs, true);
        }

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            // Parse the console output file every 15 seconds
            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(mWorkDir, PROMEX_CONSOLE_OUTPUT));

                UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

                LogProgress("ProMex");
            }
        }
    }
}
