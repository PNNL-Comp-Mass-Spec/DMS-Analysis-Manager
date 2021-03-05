//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;

namespace AnalysisManagerMSFraggerPlugIn
{
    // Ignore Spelling: Fragger

    /// <summary>
    /// Class for running MSFragger analysis
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerMSFragger : AnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        private const string MSFragger_CONSOLE_OUTPUT = "MSFragger_ConsoleOutput.txt";
        private const string MSFragger_JAR_NAME = "MSFragger.jar";

        private const float PROGRESS_PCT_STARTING = 1;
        private const float PROGRESS_PCT_COMPLETE = 99;

        #endregion

        #region "Module Variables"

        private bool mToolVersionWritten;

        // Populate this with a tool version reported to the console
        private string mMSFraggerVersion;

        private string mMSFraggerProgLoc;
        private string mConsoleOutputErrorMsg;

        private string mLocalFASTAFilePath;

        private DateTime mLastConsoleOutputParse;

        private RunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs MSFragger tool
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
                    LogDebug("AnalysisToolRunnerMSFragger.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to MSFragger
                mMSFraggerProgLoc = DetermineProgramLocation("MSFraggerProgLoc", MSFragger_JAR_NAME);

                if (string.IsNullOrWhiteSpace(mMSFraggerProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the MSFragger version info in the database after the first line is written to file MSFragger_ConsoleOutput.txt
                mToolVersionWritten = false;
                mMSFraggerVersion = string.Empty;
                mConsoleOutputErrorMsg = string.Empty;

                if (!ValidateFastaFile())
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Process the mzML file using MSFragger
                var processingResult = StartMSFragger();

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                Global.IdleLoop(0.5);
                PRISM.ProgRunner.GarbageCollectNow();

                if (!AnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();
                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                return processingResult;
            }
            catch (Exception ex)
            {
                LogError("Error in MSFraggerPlugin->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileToSkip(Dataset + AnalysisResources.DOT_MZML_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory();
        }

        /// <summary>
        /// Determine the number of threads to use for MSFragger
        /// </summary>
        private int GetNumThreadsToUse()
        {
            var coreCount = Global.GetCoreCount();

            if (coreCount > 4)
            {
                return coreCount - 1;
            }

            return coreCount;
        }

        /// <summary>
        /// Parse the MSFragger console output file to determine the MSFragger version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            const string MSFTBX = "MSFTBX";

            // Example Console output
            //
            // Running MSFragger
            // MSFragger version MSFragger-20190222
            // ...
            //
            // Sequence database filtered and tagged in 62ms
            // Digestion completed in 438ms
            // Merged digestion results in 106ms
            // Sorting digested sequences...
            //    	of length 7: 583315
            // 	  	of length 8: 565236
            // 	  	of length 9: 575341
            // 	  	of length 10: 546050
            //    	of length 11: 527565
            //     	of length 50: 3491
            // 	DONE
            // Removing duplicates and compacting...
            // Reduced to 6653770  peptides in 22392ms
            // Generating modified peptides...DONE in 1135ms
            //   Generated 11025300 modified peptides
            // Merging peptide pools from threads... DONE in 62ms
            // Sorting modified peptides by mass...DONE in 759ms
            // Peptide index written in 627ms
            // Selected fragment tolerance 0.10 Da and maximum fragment slice size of 6361.86MB
            //   1001868396 fragments to be searched in 2 slices (7.46GB total)
            // Operating on slice 1 of 2: 4463ms
            // 	DatasetName.mzML 7042ms
            // 	DatasetName.mzML 7042ms [progress: 29940/50420 (59.38%) - 5945.19 spectra/s]
            // 	DatasetName.mzML 7042ms [progress: 50420/50420 (100.00%) - 4926.63 spectra/s] - completed 9332ms
            //   Operating on slice 2 of 2: 3769ms
            //	DatasetName.mzML 2279ms
            //	DatasetName.mzML 2279ms [progress: 50420/50420 (100.00%) - 18550.40 spectra/s] - completed 2788ms

            var processingSteps = new SortedList<string, int>
            {
                {"JVM started", 0},
                {"Sequence database filtered and tagged", 2},
                {"Digestion completed", 5},
                {"Sorting digested sequences", 6},
                {"Removing duplicates and compacting", 8},
                {"Generating modified peptides", 10},
                {"Peptide index written", 11},
                {"Operating on slice", 12},
                {"Done", 98}
            };

            // RegEx to match lines like:
            // Operating on slice 1 of 2: 4463ms
            var sliceMatcher = new Regex(@"Operating on slice (?<Current>\d+) of (?<Total>\d+)",
                                         RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // Regex to match lines like:
            // DatasetName.mzML 7042ms [progress: 29940/50420 (59.38%) - 5945.19 spectra/s]
            var progressMatcher = new Regex(@"progress: \d+/\d+ \((?<PercentComplete>[0-9.]+)%\)",
                                            RegexOptions.Compiled | RegexOptions.IgnoreCase);

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

                mConsoleOutputErrorMsg = string.Empty;
                var currentProgress = 0;
                float subtaskProgress = 0;
                var currentSlice = 0;
                var totalSlices = 0;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var linesRead = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead++;

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (linesRead <= 5)
                        {
                            // The first line has the path to the MSFragger .jar file and the command line arguments
                            // The second line is dashes
                            // The third line should be: Running MSFragger
                            // The fourth line should have the version

                            if (string.IsNullOrEmpty(mMSFraggerVersion) &&
                                dataLine.StartsWith("MSFragger version", StringComparison.OrdinalIgnoreCase))
                            {
                                if (mDebugLevel >= 2)
                                {
                                    LogDebug(dataLine);
                                }

                                mMSFraggerVersion = string.Copy(dataLine);
                            }

                            if (dataLine.StartsWith(MSFTBX, StringComparison.OrdinalIgnoreCase) &&
                                mMSFraggerVersion.IndexOf(MSFTBX, StringComparison.OrdinalIgnoreCase) < 0)
                            {
                                mMSFraggerVersion = mMSFraggerVersion + "; " + dataLine;
                            }
                        }
                        else
                        {
                            foreach (var processingStep in processingSteps)
                            {
                                if (!dataLine.StartsWith(processingStep.Key, StringComparison.OrdinalIgnoreCase))
                                    continue;

                                currentProgress = processingStep.Value;
                            }

                            if (linesRead > 12 &&
                                dataLine.IndexOf("error", StringComparison.OrdinalIgnoreCase) >= 0 &&
                                string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg = "Error running MSFragger: " + dataLine;
                            }

                            var sliceMatch = sliceMatcher.Match(dataLine);
                            if (sliceMatch.Success)
                            {
                                if (int.TryParse(sliceMatch.Groups["Current"].Value, out var itemValue))
                                    currentSlice = itemValue;

                                if (int.TryParse(sliceMatch.Groups["Total"].Value, out var totalValue))
                                    totalSlices = totalValue;
                            } else if (currentSlice > 0)
                            {
                                var progressMatch = progressMatcher.Match(dataLine);
                                if (progressMatch.Success)
                                {
                                    if (float.TryParse(progressMatch.Groups["PercentComplete"].Value, out var progressValue))
                                    {
                                        subtaskProgress = progressValue;
                                    }
                                }
                            }
                        }
                    }
                }

                float effectiveProgress;
                if (currentSlice > 0 && totalSlices > 0)
                {
                    var nextProgress = 100;

                    // Find the % progress value for step following the current step
                    foreach (var item in processingSteps)
                    {
                        if (item.Value > currentProgress && item.Value < nextProgress)
                            nextProgress = item.Value;
                    }

                    // First compute the effective progress for the start of this slice
                    var sliceProgress = ComputeIncrementalProgress(currentProgress, nextProgress,
                                                                   currentSlice - 1, totalSlices);

                    // Now bump up the effective progress based on subtaskProgress
                    var addonProgress = (nextProgress - currentProgress) / (float)totalSlices * subtaskProgress / 100;
                    effectiveProgress = sliceProgress + addonProgress;
                }
                else
                {
                    effectiveProgress = currentProgress;
                }

                mProgress = effectiveProgress;
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private CloseOutType StartMSFragger()
        {
            LogMessage("Running MSFragger");

            // Customize the path to the FASTA file and the number of threads to use
            var resultCode = UpdateMSFraggerParameterFile(out var paramFilePath);

            if (resultCode != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return resultCode;
            }

            if (string.IsNullOrWhiteSpace(paramFilePath))
            {
                LogError("MSFragger parameter file name returned by UpdateMSFraggerParameterFile is empty");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // javaProgLoc will typically be "C:\Program Files\Java\jre8\bin\Java.exe"
            var javaProgLoc = GetJavaProgLoc();
            if (string.IsNullOrEmpty(javaProgLoc))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var javaMemorySizeMB = mJobParams.GetJobParameter("MSFraggerJavaMemorySize", 10000);
            if (javaMemorySizeMB < 2000)
                javaMemorySizeMB = 2000;

            // Set up and execute a program runner to run MSFragger

            var mzMLFile = mDatasetName + AnalysisResources.DOT_MZML_EXTENSION;

            // Set up and execute a program runner to run MSFragger
            var arguments = " -Xmx" + javaMemorySizeMB + "M -jar " + mMSFraggerProgLoc;

            arguments += " " + paramFilePath;
            arguments += " " + Path.Combine(mWorkDir, mzMLFile);

            LogDebug(javaProgLoc + " " + arguments);

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = Path.Combine(mWorkDir, MSFragger_CONSOLE_OUTPUT)
            };
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mProgress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var processingSuccess = mCmdRunner.RunProgram(javaProgLoc, arguments, "MSFragger", true);

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mMSFraggerVersion))
                {
                    ParseConsoleOutputFile(Path.Combine(mWorkDir, MSFragger_CONSOLE_OUTPUT));
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!processingSuccess)
            {
                LogError("Error running MSFragger");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("MSFragger returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to MSFragger failed (but exit code is 0)");
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Validate that MSFragger created a .pepXML file
            var pepXmlFile = new FileInfo(Path.Combine(mWorkDir, Dataset + ".pepXML"));
            if (!pepXmlFile.Exists)
            {
                LogError("MSFragger did not create a .pepXML file");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (pepXmlFile.Length == 0)
            {
                LogError("pepXML file created by MSFragger is empty");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Zip the .pepXML file
            var zipSuccess = ZipOutputFile(pepXmlFile, ".pepXML file");
            if (!zipSuccess)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Rename the zipped file
            var zipFile = new FileInfo(Path.ChangeExtension(pepXmlFile.FullName, ".zip"));
            if (!zipFile.Exists)
            {
                LogError("Zipped pepXML file not found");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var newZipFilePath = Path.Combine(mWorkDir, Dataset + "_pepXML.zip");
            zipFile.MoveTo(newZipFilePath);

            mStatusTools.UpdateAndWrite(mProgress);
            if (mDebugLevel >= 3)
            {
                LogDebug("MSFragger Search Complete");
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType UpdateMSFraggerParameterFile(out string paramFilePath)
        {
            paramFilePath = string.Empty;

            try
            {
                var paramFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);
                var sourceFile = new FileInfo(Path.Combine(mWorkDir, paramFileName));
                var updatedFile = new FileInfo(Path.Combine(mWorkDir, paramFileName + ".new"));

                var fastaFileDefined = false;
                var threadsDefined = false;

                var numThreadsToUse = GetNumThreadsToUse();

                using (var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(updatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine();
                            continue;
                        }

                        if (dataLine.Trim().StartsWith("database_name"))
                        {
                            writer.WriteLine("database_name = " + mLocalFASTAFilePath);
                            fastaFileDefined = true;
                            continue;
                        }

                        if (dataLine.Trim().StartsWith("num_threads"))
                        {
                            writer.WriteLine("num_threads = " + numThreadsToUse);
                            threadsDefined = true;
                            continue;
                        }

                        writer.WriteLine(dataLine);
                    }

                    if (!fastaFileDefined)
                    {
                        writer.WriteLine("database_name = " + mLocalFASTAFilePath);
                    }

                    if (!threadsDefined)
                    {
                        writer.WriteLine("num_threads = " + numThreadsToUse);
                    }
                }

                // Replace the original parameter file with the updated one
                sourceFile.Delete();
                updatedFile.MoveTo(Path.Combine(mWorkDir, paramFileName));

                paramFilePath = updatedFile.FullName;

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Exception updating the MSFragger parameter file", ex);
                return CloseOutType.CLOSEOUT_FAILED;
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

            var toolVersionInfo = string.Copy(mMSFraggerVersion);

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new FileInfo(mMSFraggerProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private bool ValidateFastaFile()
        {
            // Define the path to the fasta file
            var localOrgDbFolder = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);

            // Note that job parameter "generatedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
            var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam("PeptideSearch", AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

            var fastaFile = new FileInfo(fastaFilePath);

            if (!fastaFile.Exists)
            {
                // Fasta file not found
                LogError("Fasta file not found: " + fastaFile.Name, "Fasta file not found: " + fastaFile.FullName);
                return false;
            }

            var proteinOptions = mJobParams.GetParam("ProteinOptions");
            if (!string.IsNullOrEmpty(proteinOptions))
            {
                if (proteinOptions.IndexOf("seq_direction=decoy", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    // fastaFileIsDecoy = true;
                }
            }

            // Copy the FASTA file to the working directory
            // This is done because MSFragger indexes the file based on the dynamic and static mods,
            // and we want that index file to be in the working directory
            // Example filename: ID_007564_FEA6EC69.fasta.1.pepindex
            mLocalFASTAFilePath = Path.Combine(mWorkDir, fastaFile.Name);

            fastaFile.CopyTo(mLocalFASTAFilePath, true);

            mJobParams.AddResultFileToSkip(fastaFile.Name);
            mJobParams.AddResultFileExtensionToSkip("pepindex");

            return true;
        }

        #endregion

        #region "Event Handlers"

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            if (!(DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE))
                return;

            mLastConsoleOutputParse = DateTime.UtcNow;

            ParseConsoleOutputFile(Path.Combine(mWorkDir, MSFragger_CONSOLE_OUTPUT));

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mMSFraggerVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("MSFragger");
        }

        #endregion
    }
}
