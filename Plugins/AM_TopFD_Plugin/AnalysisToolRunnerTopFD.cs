//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerTopFDPlugIn
{
    /// <summary>
    /// Class for running TopFD analysis
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerTopFD : AnalysisToolRunnerBase
    {
        // Ignore Spelling: Aragorn, centroided, cv, html, Orbitrap, sn, TopFD

        /// <summary>
        /// .feature file created by TopFD
        /// </summary>
        /// <remarks>Tracks LC/MS features</remarks>
        public const string TOPFD_FEATURE_FILE_SUFFIX = ".feature";

        /// <summary>
        /// _ms2.msalign file created by TopFD
        /// </summary>
        private const string MSALIGN_FILE_SUFFIX = "_ms2.msalign";

        private const string TOPFD_CONSOLE_OUTPUT = "TopFD_ConsoleOutput.txt";
        public const string TOPFD_EXE_NAME = "topfd.exe";

        private const int PROGRESS_PCT_STARTING = 1;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private bool mToolVersionWritten;

        /// <summary>
        /// This will initially be 1.3 or 1.4, indicating the version of .exe that should be used
        /// </summary>
        /// <remarks>
        /// After TopFD starts, we'll update this variable with the tool version reported to the console
        /// </remarks>
        private Version mTopFDVersion;

        private string mTopFDVersionText;

        private string mTopFDProgLoc;

        private string mConsoleOutputErrorMsg;

        private readonly Regex mExtractPercentFinished = new("(?<PercentComplete>[0-9]+)% finished", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private DateTime mLastConsoleOutputParse;

        private bool mMzMLInstrumentIdAdjustmentRequired;

        private RunDosProgram mCmdRunner;

        /// <summary>
        /// Runs TopFD tool
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
                    LogDebug("AnalysisToolRunnerTopFD.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to the TopFD program
                mTopFDProgLoc = DetermineProgramLocation("TopFDProgLoc", TOPFD_EXE_NAME, out var specificStepToolVersion);

                if (string.IsNullOrWhiteSpace(mTopFDProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (specificStepToolVersion.StartsWith("v1.3"))
                {
                    mTopFDVersion = new Version(1, 3);
                }
                else
                {
                    // We're probably running TopPIC v1.5 (or newer)
                    mTopFDVersion = new Version(1, 5);
                }

                // Store the TopFD version info in the database after the first line is written to file TopFD_ConsoleOutput.txt
                mToolVersionWritten = false;
                mTopFDVersionText = string.Empty;
                mConsoleOutputErrorMsg = string.Empty;

                // Check whether an existing TopFD results directory was found
                var existingTopFDResultsDirectory = mJobParams.GetJobParameter(
                    "StepParameters",
                    AnalysisResourcesTopFD.JOB_PARAM_EXISTING_TOPFD_RESULTS_DIRECTORY,
                    string.Empty);

                CloseOutType processingResult;
                bool zipSubdirectories;
                bool htmlOutputDisabled;

                if (!string.IsNullOrWhiteSpace(existingTopFDResultsDirectory))
                {
                    processingResult = RetrieveExistingTopFDResults(existingTopFDResultsDirectory);
                    zipSubdirectories = false;
                    htmlOutputDisabled = false;
                }
                else
                {
                    mMzMLInstrumentIdAdjustmentRequired = false;
                    var mzMLFileName = Dataset + AnalysisResources.DOT_MZML_EXTENSION;

                    processingResult = StartTopFD(mTopFDProgLoc, mzMLFileName, out htmlOutputDisabled);

                    if (mMzMLInstrumentIdAdjustmentRequired)
                    {
                        var updatedMzMLFileName = UpdateInstrumentInMzMLFile(mzMLFileName);
                        mConsoleOutputErrorMsg = string.Empty;

                        processingResult = StartTopFD(mTopFDProgLoc, updatedMzMLFileName, out _);
                    }
                    zipSubdirectories = true;
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                // Trim the console output file to remove the majority of the % finished messages
                TrimConsoleOutputFile(Path.Combine(mWorkDir, TOPFD_CONSOLE_OUTPUT));

                if (!AnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory(zipSubdirectories, htmlOutputDisabled);

                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                return processingResult;
            }
            catch (Exception ex)
            {
                mMessage = "Error in TopFDPlugin->RunTool: " + ex.Message;
                LogError(mMessage + "; " + Global.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Zip the _file and _html directories, if they exist
        /// Next, Make the local results directory, move files into that directory, then copy the files to the transfer directory on the Proto-x server
        /// </summary>
        /// <param name="zipSubdirectories"></param>
        /// <param name="htmlOutputDisabled"></param>
        /// <returns>True if success, otherwise false</returns>
        private bool CopyResultsToTransferDirectory(bool zipSubdirectories, bool htmlOutputDisabled)
        {
            if (zipSubdirectories)
            {
                var zipSuccess = ZipTopFDDirectories(htmlOutputDisabled);

                if (!zipSuccess)
                    return false;
            }

            return base.CopyResultsToTransferDirectory();
        }

        /// <summary>
        /// Returns a dictionary mapping parameter names to argument names
        /// </summary>
        private Dictionary<string, string> GetTopFDParameterNames()
        {
            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                {"MaxCharge", "max-charge"},
                {"MaxMass", "max-mass"},
                {"MzError", "mz-error"},
                {"SNRatio", "ms-two-sn-ratio"},
                {"SNRatioMS1", "ms-one-sn-ratio"},
                {"SNRatioMS2", "ms-two-sn-ratio"},
                {"PrecursorWindow", "precursor-window"},
                {"MS1Missing", "missing-level-one"},
                {"DisableHtmlOutput", "skip-html-folder"}
            };
        }

        /// <summary>
        /// Parse the TopFD console output file to determine the TopFD version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example Console output (yes, TopFD misspells "running"):

            // ReSharper disable CommentTypo

            // TopFD 1.1.2
            // Timestamp: Mon Aug 13 17:54:19 2018
            // ********************** Parameters **********************
            // Input file:                             QC_ShewIntact_1_2Aug18_HCD28_Aragorn_18-7-02.mzML
            // Data type:                              centroided
            // Maximum charge:                         30
            // Maximum monoisotopic mass:              100000 Dalton
            // Error tolerance:                        0.02 m/z
            // Signal/noise ratio:                     1
            // Precursor window size:                  3 m/z
            // ********************** Parameters **********************
            // Processing spectrum Scan_349...         3% finished.
            // Processing spectrum Scan_350...         3% finished.
            // Processing spectrum Scan_351...         3% finished.
            // Deconvolution finished.
            // Runing time: 51 seconds.
            // TopFD finished.

            // ReSharper restore CommentTypo

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

                short actualProgress = 0;

                mConsoleOutputErrorMsg = string.Empty;

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var linesRead = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    if (linesRead <= 3)
                    {
                        // The first line has the TopFD executable name and the command line arguments
                        // The second line is dashes
                        // The third line has the TopFD version
                        if (string.IsNullOrEmpty(mTopFDVersionText) &&
                            dataLine.IndexOf("TopFD", StringComparison.OrdinalIgnoreCase) == 0 &&
                            dataLine.IndexOf(TOPFD_EXE_NAME, StringComparison.OrdinalIgnoreCase) < 0)
                        {
                            if (mDebugLevel >= 2)
                            {
                                LogDebug("TopFD version: " + dataLine);
                            }

                            var versionMatcher = new Regex(@"(?<Major>\d+)\.(?<Minor>\d+)\.(?<Build>\d+)", RegexOptions.Compiled);
                            var match = versionMatcher.Match(dataLine);

                            if (match.Success)
                            {
                                mTopFDVersion = new Version(match.Value);
                            }

                            mTopFDVersionText = dataLine;
                        }

                        continue;
                    }

                    // Update progress if the line starts with Processing MS/MS spectrum
                    // If a .mzML file has multiple CV values, the % complete will cycle from 0% to 100% for each CV value
                    if (dataLine.StartsWith("Processing MS/MS spectrum", StringComparison.OrdinalIgnoreCase))
                    {
                        var match = mExtractPercentFinished.Match(dataLine);

                        if (match.Success)
                        {
                            actualProgress = short.Parse(match.Groups["PercentComplete"].Value);
                        }

                        continue;
                    }

                    if (linesRead < 12)
                        continue;

                    if (string.IsNullOrEmpty(mConsoleOutputErrorMsg) &&
                        dataLine.IndexOf("Error tolerance:", StringComparison.OrdinalIgnoreCase) < 0 &&
                        (dataLine.IndexOf("error", StringComparison.OrdinalIgnoreCase) >= 0 ||
                         dataLine.IndexOf("terminate called after throwing an instance", StringComparison.OrdinalIgnoreCase) >= 0))
                    {
                        mConsoleOutputErrorMsg = "Error running TopFD: " + dataLine;
                        continue;
                    }

                    if (dataLine.IndexOf("Invalid cvParam accession", StringComparison.OrdinalIgnoreCase) > 0)
                    {
                        if (dataLine.Contains("1003029"))
                        {
                            LogWarning(
                                "TopFD is unable to process this .mzML file since it comes from " +
                                "an unrecognized instrument (Orbitrap Eclipse, MS:1003029); " +
                                "will update the .mzML file and try again");

                            mMzMLInstrumentIdAdjustmentRequired = true;
                            break;
                        }

                        if (dataLine.Contains("1003356"))
                        {
                            LogWarning(
                                "TopFD is unable to process this .mzML file since it comes from " +
                                "an unrecognized instrument (Orbitrap Ascend, MS:1003356); " +
                                "will update the .mzML file and try again");

                            mMzMLInstrumentIdAdjustmentRequired = true;
                            break;
                        }
                    }
                }

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
        /// Read the TopFD options file and convert the options to command line switches
        /// </summary>
        /// <param name="cmdLineArguments">Output: TopFD command line arguments</param>
        /// <param name="htmlOutputDisabled">Output: True if the parameter file has DisableHtmlOutput=True</param>
        /// <param name="threadCount">Output: thread count to use</param>
        /// <returns>Options string if success; empty string if an error</returns>
        public CloseOutType ParseTopFDParameterFile(out StringBuilder cmdLineArguments, out bool htmlOutputDisabled, out int threadCount)
        {
            cmdLineArguments = new StringBuilder();

            var parameterFileName = mJobParams.GetParam("TopFD_ParamFile");

            // Although ParseKeyValueParameterFile checks for paramFileName being an empty string,
            // we check for it here since the name comes from the settings file, so we want to customize the error message
            if (string.IsNullOrWhiteSpace(parameterFileName))
            {
                LogError("TopFD parameter file not defined in the job settings (param name TopFD_ParamFile)");
                htmlOutputDisabled = false;
                threadCount = 0;
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            var result = LoadSettingsFromKeyValueParameterFile("TopFD", parameterFileName, out var paramFileEntries, out var paramFileReader);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                htmlOutputDisabled = false;
                threadCount = 0;
                return result;
            }

            // Obtain the dictionary that maps parameter names to argument names
            var paramToArgMapping = GetTopFDParameterNames();

            var paramNamesToSkip = new SortedSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "MS1Missing",
                "DisableHtmlOutput"
            };

            cmdLineArguments.Append(paramFileReader.ConvertParamsToArgs(paramFileEntries, paramToArgMapping, paramNamesToSkip, "--"));

            htmlOutputDisabled = paramFileReader.ParamIsEnabled(paramFileEntries, "DisableHtmlOutput");

            if (cmdLineArguments.Length == 0)
            {
                mMessage = paramFileReader.ErrorMessage;
                threadCount = 0;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
            foreach (var paramName in paramNamesToSkip)
            {
                if (!paramFileReader.ParamIsEnabled(paramFileEntries, paramName))
                    continue;

                if (paramToArgMapping.TryGetValue(paramName, out var argumentName))
                {
                    cmdLineArguments.AppendFormat(" --{0}", argumentName);
                }
                else
                {
                    // Example error messages:
                    //   Parameter to argument mapping dictionary does not have MS1Missing
                    //   Parameter to argument mapping dictionary does not have DisableHtmlOutput

                    LogError(string.Format("Parameter to argument mapping dictionary does not have {0}", paramName));
                    threadCount = 0;
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            if (mTopFDVersion >= new Version(1, 4))
            {
                // Specify the number of threads to use
                // Allow TopFD to use 88% of the physical cores
                var coreCount = Global.GetCoreCount();
                var threadsToUse = (int)Math.Floor(coreCount * 0.88);

                LogMessage("The system has {0} cores; TopFD will use {1} threads ", coreCount, threadsToUse);
                cmdLineArguments.AppendFormat(" --thread-number {0}", threadsToUse);

                threadCount = threadsToUse;
            }
            else
            {
                threadCount = 1;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RetrieveExistingTopFDResults(string sourceDirectoryPath)
        {
            try
            {
                // Copy the existing TopFD results files to the working directory
                // Do not copy the _html.zip file; TopPIC doesn't need it (and it can be huge)

                // TopPIC v1.4 creates files named
                // DatasetName_ms1.feature
                // DatasetName_ms2.feature
                // DatasetName_ms2.msalign
                // DatasetName_feature.xml

                // TopPIC v1.5 creates three versions of each file type
                // DatasetName_0_ms1.feature
                // DatasetName_1_ms1.feature
                // DatasetName_2_ms1.feature
                // DatasetName_0_ms2.feature
                // DatasetName_1_ms2.feature
                // DatasetName_2_ms2.feature
                // DatasetName_0_ms2.msalign
                // DatasetName_1_ms2.msalign
                // DatasetName_2_ms2.msalign
                // DatasetName_0_feature.xml
                // DatasetName_1_feature.xml
                // DatasetName_2_feature.xml

                var filesToFind = new List<string>
                {
                    mDatasetName + "*_ms1.feature",
                    mDatasetName + "*_ms2.feature",
                    mDatasetName + "*_ms2.msalign",
                    mDatasetName + "*_feature.xml",
                    "TopFD_ConsoleOutput.txt"
                };

                var paramFileName = mJobParams.GetParam("TopFD_ParamFile");

                if (string.IsNullOrWhiteSpace(paramFileName))
                {
                    LogWarning("TopFD parameter file not defined in the job settings; will use the TopFD parameter file that was already copied locally");
                }
                else
                {
                    filesToFind.Add(paramFileName);
                }

                var sourceDirectory = new DirectoryInfo(sourceDirectoryPath);
                var analysisResults = new AnalysisResults(mMgrParams, mJobParams);

                foreach (var fileToFind in filesToFind)
                {
                    var candidateFiles = sourceDirectory.GetFiles(fileToFind).ToList();

                    if (candidateFiles.Count == 0)
                    {
                        LogError("Cannot retrieve existing TopFD results; existing TopFD results file matching {0} not found in {1}", fileToFind, sourceDirectory.FullName);
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    foreach (var sourceFile in candidateFiles)
                    {
                        var destinationFilePath = Path.Combine(mWorkDir, sourceFile.Name);
                        analysisResults.CopyFileWithRetry(sourceFile.FullName, destinationFilePath, true);
                    }
                }

                mEvalMessage = string.Format(
                    "Retrieved {0} existing TopFD result files from {1}",
                    filesToFind.Count, sourceDirectory.FullName);

                LogMessage(EvalMessage);

                var toolVersionInfo = mJobParams.GetJobParameter(
                    "StepParameters",
                    AnalysisResourcesTopFD.JOB_PARAM_EXISTING_TOPFD_TOOL_VERSION,
                    string.Empty);

                SetStepTaskToolVersion(toolVersionInfo, new List<FileInfo>(), false);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                // Ignore errors here
                LogError("Error retrieving existing TopFD results: " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType StartTopFD(string progLoc, string mzMLFileName, out bool htmlOutputDisabled)
        {
            LogMessage("Running TopFD");

            var result = ParseTopFDParameterFile(out var cmdLineArguments, out htmlOutputDisabled, out var threadCount);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            var arguments = string.Format("{0} {1}", cmdLineArguments.ToString().Trim(), mzMLFileName);

            LogDebug(progLoc + " " + arguments);

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = Path.Combine(mWorkDir, TOPFD_CONSOLE_OUTPUT)
            };

            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mProgress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var processingSuccess = mCmdRunner.RunProgram(progLoc, arguments, "TopFD", true);

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mTopFDVersionText))
                {
                    ParseConsoleOutputFile(Path.Combine(mWorkDir, TOPFD_CONSOLE_OUTPUT));
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!processingSuccess)
            {
                LogError("Error running TopFD");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("TopFD returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to TopFD failed (but exit code is 0)");
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // TopFD 1.7 does not delete the .msalign file created by each thread; flag the files to be skipped, e.g.:
            // .msalign_0, .msalign_1, .msalign_2, etc.

            for (var i = 0; i < threadCount; i++)
            {
                mJobParams.AddResultFileExtensionToSkip(string.Format(".msalign_{0}", i));
            }

            // Make sure the output files were created and are not zero-bytes
            // If the input .mzML file only has MS spectra and no MS/MS spectra, the output files will be empty

            // Require that the .feature and _ms2.msalign files were created

            // Starting with TopPIC 1.3, the program creates _ms1.feature and _ms2.feature instead of a single .feature file
            // TopFD likely also created a _ms1.msalign file, but it's not required for TopPIC, so we don't check for it

            var workDir = new DirectoryInfo(mWorkDir);

            // resultsFiles is a dictionary mapping a results file suffix to the full results file name
            var resultsFiles = new Dictionary<string, string>();

            // For FAIMS datasets with multiple CV values, TopPIC 1.5 creates one _ms2.msalign file for each CV
            // Example filenames are Dataset_0_ms2.msalign, Dataset_1_ms2.msalign, etc.

            // TopPIC v1.7 uses the actual CV value, e.g. Dataset_-35_ms2.msalign, Dataset_-45_ms2.msalign, etc.

            // This RegEx matches both the single digit format, and the CV value format
            // It is used to match both _ms2.msalign and _ms2.feature files
            var cvSuffixMatcher = new Regex(@"[-+]?\d+_ms2.(msalign|feature)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var msAlignFiles = workDir.GetFiles(string.Format("{0}_*{1}", mDatasetName, MSALIGN_FILE_SUFFIX));

            if (msAlignFiles.Length > 0)
            {
                foreach (var item in msAlignFiles)
                {
                    var match = cvSuffixMatcher.Match(item.Name);

                    resultsFiles.Add(
                        match.Success
                            ? match.Value
                            : item.Name.Substring(item.Name.Length - MSALIGN_FILE_SUFFIX.Length - 1), item.Name);
                }
            }
            else
            {
                resultsFiles.Add(MSALIGN_FILE_SUFFIX, mDatasetName + MSALIGN_FILE_SUFFIX);
            }

            var legacyFeatureFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + TOPFD_FEATURE_FILE_SUFFIX));

            if (legacyFeatureFile.Exists)
            {
                resultsFiles.Add(TOPFD_FEATURE_FILE_SUFFIX, legacyFeatureFile.Name);
            }
            else
            {
                // For FAIMS datasets with multiple CV values, TopPIC 1.5 creates one _ms2.feature file for each CV
                // Example file names are Dataset_0_ms2.feature, Dataset_1_ms2.feature, etc.

                var featureFiles = workDir.GetFiles(string.Format("{0}_*_ms2{1}", mDatasetName, TOPFD_FEATURE_FILE_SUFFIX));

                if (featureFiles.Length > 0)
                {
                    foreach (var item in featureFiles)
                    {
                        var match = cvSuffixMatcher.Match(item.Name);

                        resultsFiles.Add(
                            match.Success
                                ? match.Value
                                : item.Name.Substring(item.Name.Length - TOPFD_FEATURE_FILE_SUFFIX.Length - 5), item.Name);
                    }
                }
                else
                {
                    resultsFiles.Add("_ms2" + TOPFD_FEATURE_FILE_SUFFIX, mDatasetName + "_ms2" + TOPFD_FEATURE_FILE_SUFFIX);
                }
            }

            var validResultFiles = 0;

            foreach (var resultFile in resultsFiles)
            {
                var resultsFile = new FileInfo(Path.Combine(mWorkDir, resultFile.Value));

                if (!resultsFile.Exists)
                {
                    LogError(string.Format("{0} file was not created by TopFD", resultFile.Key));
                }
                else if (resultsFile.Length == 0)
                {
                    LogError(string.Format("{0} file created by TopFD is empty; assure that the input .mzML file has MS/MS spectra", resultFile.Key));
                }
                else
                {
                    validResultFiles++;
                }
            }

            if (validResultFiles < resultsFiles.Count)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            mStatusTools.UpdateAndWrite(mProgress);

            if (mDebugLevel >= 3)
            {
                LogDebug("TopFD analysis complete");
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
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

            var toolVersionInfo = mTopFDVersionText;

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(mTopFDProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        /// <summary>
        /// Reads the console output file and removes the majority of the "Processing" messages
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void TrimConsoleOutputFile(string consoleOutputFilePath)
        {
            // This RegEx matches lines of the form:
            //   Processing spectrum scan 100 ...    25% finished.
            //   Processing spectrum scan_100 ...    25% finished.
            //   Processing MS1 spectrum scan 100 ...    25% finished.
            //   Processing MS/MS spectrum scan 200 ...  11% finished.
            var reExtractScan = new Regex(@"Processing.+spectrum Scan[ _](?<Scan>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // This RegEx matches lines of the form:
            //   Processing feature 10000 ...       10% finished.
            var reExtractFeature = new Regex(@"Processing feature (?<Feature>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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

                var mostRecentProgressLine = string.Empty;
                var mostRecentProgressLineWritten = string.Empty;

                var trimmedFilePath = consoleOutputFilePath + ".trimmed";

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(trimmedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var scanNumberOutputThreshold = 0;
                    var featureNumberOutputThreshold = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine(dataLine);
                            continue;
                        }

                        var keepLine = true;

                        var scanMatch = reExtractScan.Match(dataLine);

                        if (scanMatch.Success)
                        {
                            var scanNumber = int.Parse(scanMatch.Groups["Scan"].Value);

                            if (scanNumber < scanNumberOutputThreshold)
                            {
                                keepLine = false;
                            }
                            else
                            {
                                // Write out this line and bump up scanNumberOutputThreshold by 100
                                scanNumberOutputThreshold += 100;
                                mostRecentProgressLineWritten = dataLine;
                            }

                            mostRecentProgressLine = dataLine;
                        }
                        else if (dataLine.StartsWith("Deconvolution finished", StringComparison.OrdinalIgnoreCase))
                        {
                            // Possibly write out the most recent progress line
                            if (!Global.IsMatch(mostRecentProgressLine, mostRecentProgressLineWritten))
                            {
                                writer.WriteLine(mostRecentProgressLine);
                            }
                        }
                        else
                        {
                            var featureMatch = reExtractFeature.Match(dataLine);

                            if (featureMatch.Success)
                            {
                                var featureNumber = int.Parse(featureMatch.Groups["Feature"].Value);

                                if (featureNumber < featureNumberOutputThreshold)
                                {
                                    keepLine = false;
                                }
                                else
                                {
                                    // Write out this line and bump up featureNumberOutputThreshold by 1000
                                    featureNumberOutputThreshold += 1000;
                                    mostRecentProgressLineWritten = dataLine;
                                }

                                mostRecentProgressLine = dataLine;
                            }
                        }

                        if (keepLine)
                        {
                            writer.WriteLine(dataLine);
                        }
                    }
                }

                // Swap the files

                try
                {
                    var oldConsoleOutputFilePath = consoleOutputFilePath + ".old";
                    File.Move(consoleOutputFilePath, oldConsoleOutputFilePath);
                    File.Move(trimmedFilePath, consoleOutputFilePath);
                    File.Delete(oldConsoleOutputFilePath);
                }
                catch (Exception ex)
                {
                    if (mDebugLevel >= 1)
                    {
                        LogError("Error replacing original console output file (" + consoleOutputFilePath + ") with trimmed version", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error trimming console output file (" + consoleOutputFilePath + ")", ex);
                }
            }
        }

        private string UpdateInstrumentInMzMLFile(string sourceMzMLFilename)
        {
            try
            {
                // CV params to update
                // Keys are the accession to find; values are the new accession
                // The instrument name for the cvParam will be left unchanged to assure that the file size doesn't change
                var cvParamsToUpdate = new Dictionary<string, string> {
                    {"MS:1003029", "MS:1002732"},   // Replace the accession for "Orbitrap Eclipse" with that for "Orbitrap Fusion Lumos"
                    {"MS:1003356", "MS:1002732"}    // Replace the accession for "Orbitrap Ascend" with that for "Orbitrap Fusion Lumos"
                };

                var mzMLFilePath = Path.Combine(mWorkDir, sourceMzMLFilename);
                var sourceMzMLFile = new FileInfo(mzMLFilePath);

                if (!sourceMzMLFile.Exists)
                {
                    LogError("Unable to create an updated .mzML file with a new instrument class: source mzML file not found");
                    return string.Empty;
                }

                // Rename the source file
                var oldMzMLFilename = Path.GetFileNameWithoutExtension(sourceMzMLFilename) + "_old" + Path.GetExtension(sourceMzMLFilename);
                sourceMzMLFile.MoveTo(Path.Combine(mWorkDir, oldMzMLFilename));

                var updatedMzMLFile = new FileInfo(mzMLFilePath);

                mJobParams.AddResultFileToSkip(oldMzMLFilename);
                mJobParams.AddResultFileToSkip(updatedMzMLFile.Name);

                using var reader = new StreamReader(new FileStream(sourceMzMLFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
                using var writer = new StreamWriter(new FileStream(updatedMzMLFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrEmpty(dataLine))
                        continue;

                    if (!dataLine.Trim().StartsWith("<cvParam"))
                    {
                        writer.WriteLine(dataLine);
                        continue;
                    }

                    var matchFound = false;

                    foreach (var item in cvParamsToUpdate)
                    {
                        if (dataLine.Contains(item.Key))
                        {
                            var updatedLine = dataLine.Replace(item.Key, item.Value);
                            writer.WriteLine(updatedLine);
                            matchFound = true;
                            break;
                        }
                    }

                    if (matchFound)
                    {
                        break;
                    }

                    writer.WriteLine(dataLine);
                }

                // Read/write the remaining lines
                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrEmpty(dataLine))
                        continue;

                    writer.WriteLine(dataLine);
                }

                return updatedMzMLFile.FullName;
            }
            catch (Exception ex)
            {
                LogError("Error creating the updated .mzML file with a new instrument class", ex);
                return string.Empty;
            }
        }

        private bool ZipTopFDDirectories(bool htmlOutputDisabled)
        {
            var currentDirectory = "?undefined?";

            try
            {
                var subdirectoriesToZip = new List<string> {
                    Dataset + "_file"
                };

                if (!htmlOutputDisabled)
                {
                    subdirectoriesToZip.Add(Dataset + "_html");
                }

                foreach (var subdirectoryName in subdirectoriesToZip)
                {
                    currentDirectory = subdirectoryName;

                    var directoryToFind = new DirectoryInfo(Path.Combine(mWorkDir, subdirectoryName));
                    currentDirectory = directoryToFind.FullName;

                    if (!directoryToFind.Exists)
                    {
                        LogMessage("Subdirectory not found; nothing to zip: " + directoryToFind.FullName);
                        continue;
                    }

                    var zipFilePath = Path.Combine(mWorkDir, subdirectoryName + ".zip");
                    LogMessage("Zipping {0} to create {1}", directoryToFind.FullName, Path.GetFileName(zipFilePath));

                    mZipTools.ZipDirectory(directoryToFind.FullName, zipFilePath, true);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError(string.Format("Error zipping {0}", currentDirectory), ex);
                return false;
            }
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds < SECONDS_BETWEEN_UPDATE)
                return;

            mLastConsoleOutputParse = DateTime.UtcNow;

            ParseConsoleOutputFile(Path.Combine(mWorkDir, TOPFD_CONSOLE_OUTPUT));

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mTopFDVersionText))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("TopFD");
        }
    }
}
