//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 03/11/2023
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
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using PRISM;

namespace AnalysisManagerDiaNNPlugIn
{
    /// <summary>
    /// Class for running DIA-NN
    /// </summary>
    public class AnalysisToolRunnerDiaNN : AnalysisToolRunnerBase
    {
        private const string DIA_NN_CONSOLE_OUTPUT = "DIA-NN_ConsoleOutput.txt";

        private const string DIA_NN_EXE_NAME = "diann.exe";

        /// <summary>
        /// Progress value to use when preparing to run DIA-NN
        /// </summary>
        public const float PROGRESS_PCT_INITIALIZING = 1;

        private enum ProgressPercentValues
        {
            Initializing = 0,
            StartingDiaNN = 1,
            DiaNNComplete = 90,
            ProcessingComplete = 99
        }

        private bool mToolVersionWritten;

        // Populate this with a tool version reported to the console
        private string mDiaNNVersion;

        private string mDiaNNProgLoc;

        private string mConsoleOutputErrorMsg;

        private DateTime mLastConsoleOutputParse;

        private RunDosProgram mCmdRunner;

        private static DotNetZipTools mZipTool;

        /// <summary>
        /// Runs DIA-NN tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                mProgress = (int)ProgressPercentValues.Initializing;

                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerDiaNN.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;
                mConsoleOutputErrorMsg = string.Empty;

                // Determine the path to DIA-NN
                mDiaNNProgLoc = DetermineProgramLocation("DiaNNProgLoc", DIA_NN_EXE_NAME);

                if (string.IsNullOrWhiteSpace(mDiaNNProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the DIA-NN version info in the database after the first line is written to file DIA-NN_ConsoleOutput.txt
                mToolVersionWritten = false;
                mDiaNNVersion = string.Empty;

                mConsoleOutputErrorMsg = string.Empty;

                if (!ValidateFastaFile(out var fastaFile))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Process the mzML files using DIA-NN
                var processingResult = StartDiaNN(fastaFile);

                mProgress = (int)ProgressPercentValues.ProcessingComplete;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                Global.IdleLoop(0.5);
                AppUtils.GarbageCollectNow();

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
                LogError("Error in DiaNNPlugIn->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void AppendAdditionalArguments(DiaNNOptions options, int datasetCount, StringBuilder arguments)
        {
            if (options.MatchBetweenRuns && datasetCount > 1)
            {
                // ReSharper disable once StringLiteralTypo

                // Enable Match-Between-Runs
                arguments.Append(" --reanalyse");
            }

            if (options.HeuristicProteinInference)
            {
                arguments.Append(" --relaxed-prot-inf");
            }

            if (options.SmartProfilingLibraryGeneration)
            {
                arguments.Append(" --smart-profiling");
            }
        }

        private void AppendModificationArguments(DiaNNOptions options, StringBuilder arguments)
        {
            if (options.StaticCysCarbamidomethyl)
            {
                arguments.Append(" --unimod4");
            }

            arguments.AppendFormat(" --var-mods {0}", options.MaxDynamicModsPerPeptide);

            foreach (var dynamicMod in options.DynamicModDefinitions)
            {
                arguments.AppendFormat(" --var-mod {0}", dynamicMod.ModificationDefinition);

                if (dynamicMod.MonitorMod)
                {
                    arguments.AppendFormat(" --monitor-mod {0}", dynamicMod.ModificationName);
                }

                if (dynamicMod.NoCutAfterMod)
                {
                    arguments.AppendFormat(" --no-cut-after-mod {0}", dynamicMod.ModificationName);
                }
            }

            foreach (var staticMod in options.StaticModDefinitions)
            {
                arguments.AppendFormat(" --fixed-mod {0}", staticMod.ModificationDefinition);

                if (staticMod.IsFixedLabelMod)
                {
                    arguments.AppendFormat(" --lib-fixed-mod {0}", staticMod.ModificationName);
                }
            }
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_MZML_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory();
        }

        /// <summary>
        /// Given a linked list of progress values (which should have populated in ascending order), find the next progress value
        /// </summary>
        /// <param name="progressValues"></param>
        /// <param name="currentProgress"></param>
        /// <returns>Next progress value, or 100 if either the current value is not found, or the next value is not defined</returns>
        private static int GetNextProgressValue(LinkedList<int> progressValues, int currentProgress)
        {
            var currentNode = progressValues.Find(currentProgress);

            if (currentNode?.Next == null)
                return 100;

            return currentNode.Next.Value;
        }

        /// <summary>
        /// Determine the number of threads to use for DIA-NN
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

        private static Regex GetRegEx(string matchPattern, bool ignoreCase = true)
        {
            var options = ignoreCase ? RegexOptions.Compiled | RegexOptions.IgnoreCase : RegexOptions.Compiled;
            return new Regex(matchPattern, options);
        }

        /// <summary>
        /// Parse the DIA-NN console output file to determine the DIA-NN version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseDiaNNConsoleOutputFile(string consoleOutputFilePath)
        {
            // ReSharper disable once IdentifierTypo
            // ReSharper disable once StringLiteralTypo
            const string BATMASS_IO_VERSION = "Batmass-IO version";

            // ReSharper disable CommentTypo

            // ----------------------------------------------------
            // Example Console output
            // ----------------------------------------------------

            // DIA-NN version DIA-NN-3.3
            // Batmass-IO version 1.23.4
            // timsdata library version timsdata-2-8-7-1
            // (c) University of Michigan
            // RawFileReader reading tool. Copyright (c) 2016 by Thermo Fisher Scientific, Inc. All rights reserved.
            // System OS: Windows 10, Architecture: AMD64
            // Java Info: 1.8.0_232, OpenJDK 64-Bit Server VM,
            // JVM started with 8 GB memory
            // Checking database...
            // Checking spectral files...
            // C:\DMS_WorkDir\QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.mzML: Scans = 9698
            // ***********************************FIRST SEARCH************************************
            // Parameters:
            // ...
            // Number of unique peptides
            //         of length 7: 28622
            //         of length 8: 27618
            //         of length 9: 25972
            // ...
            //         of length 50: 3193
            // In total 590010 peptides.
            // Generated 1061638 modified peptides.
            // Number of peptides with more than 5000 modification patterns: 0
            // Selected fragment index width 0.10 Da.
            // 50272922 fragments to be searched in 1 slices (0.75 GB total)
            // Operating on slice 1 of 1:
            //         Fragment index slice generated in 1.38 s
            //         001. QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.mzML 1.2 s | deisotoping 0.6 s
            //                 [progress: 9451/9451 (100%) - 14191 spectra/s] 0.7s | postprocessing 0.1 s
            // ***************************FIRST SEARCH DONE IN 0.153 MIN**************************
            //
            // *********************MASS CALIBRATION AND PARAMETER OPTIMIZATION*******************
            // ...
            // ************MASS CALIBRATION AND PARAMETER OPTIMIZATION DONE IN 0.523 MIN*********
            //
            // ************************************MAIN SEARCH************************************
            // Checking database...
            // Parameters:
            // ...
            // Number of unique peptides
            //         of length 7: 29253
            //         of length 8: 28510
            // ...
            //         of length 50: 6832
            // In total 778855 peptides.
            // Generated 1469409 modified peptides.
            // Number of peptides with more than 5000 modification patterns: 0
            // Selected fragment index width 0.10 Da.
            // 76707996 fragments to be searched in 1 slices (1.14 GB total)
            // Operating on slice 1 of 1:
            //         Fragment index slice generated in 1.38 s
            //         001. QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.mzBIN_calibrated 0.1 s
            //                 [progress: 9380/9380 (100%) - 15178 spectra/s] 0.6s | postprocessing 1.5 s
            // ***************************MAIN SEARCH DONE IN 0.085 MIN***************************
            //
            // *******************************TOTAL TIME 0.761 MIN********************************

            // ----------------------------------------------------
            // Output when multiple datasets:
            // ----------------------------------------------------
            // Operating on slice 1 of 1:
            //         Fragment index slice generated in 1.69 s
            //         001. CHI_XN_ALKY_44_Bane_06May21_20-11-16.mzML 1.1 s | deisotoping 0.7 s
            //                 [progress: 8271/8271 (100%) - 23631 spectra/s] 0.4s | postprocessing 0.0 s
            //         002. CHI_XN_DA_25_Bane_06May21_20-11-16.mzML 1.2 s | deisotoping 0.3 s
            //                 [progress: 20186/20186 (100%) - 19317 spectra/s] 1.0s | postprocessing 0.1 s
            //         003. CHI_XN_DA_26_Bane_06May21_20-11-16.mzML 1.2 s | deisotoping 0.1 s
            //                 [progress: 18994/18994 (100%) - 20336 spectra/s] 0.9s | postprocessing 0.1 s

            // ----------------------------------------------------
            // Output when multiple slices (and multiple datasets)
            // ----------------------------------------------------
            // Selected fragment index width 0.02 Da.
            // 649333606 fragments to be searched in 2 slices (9.68 GB total)
            // Operating on slice 1 of 2:
            //         Fragment index slice generated in 7.69 s
            //         001. CHI_XN_ALKY_44_Bane_06May21_20-11-16.mzML 0.6 s | deisotoping 0.0 s
            //                 [progress: 8271/8271 (100%) - 38115 spectra/s] 0.2s
            //         002. CHI_XN_DA_25_Bane_06May21_20-11-16.mzBIN_calibrated 0.3 s
            //                 [progress: 19979/19979 (100%) - 13518 spectra/s] 1.5s
            //         003. CHI_XN_DA_26_Bane_06May21_20-11-16.mzBIN_calibrated 0.3 s
            //                 [progress: 18812/18812 (100%) - 13563 spectra/s] 1.4s
            // Operating on slice 2 of 2:
            //         Fragment index slice generated in 9.02 s
            //         001. CHI_XN_ALKY_44_Bane_06May21_20-11-16.mzML 0.6 s | deisotoping 0.0 s
            //                 [progress: 8271/8271 (100%) - 37767 spectra/s] 0.2s | postprocessing 1.0 s
            //         002. CHI_XN_DA_25_Bane_06May21_20-11-16.mzBIN_calibrated 0.3 s
            //                 [progress: 19979/19979 (100%) - 30690 spectra/s] 0.7s | postprocessing 2.4 s
            //         003. CHI_XN_DA_26_Bane_06May21_20-11-16.mzBIN_calibrated 0.2 s
            //                 [progress: 18812/18812 (100%) - 29348 spectra/s] 0.6s | postprocessing 1.7 s

            // ----------------------------------------------------
            // Output when running a split FASTA search
            // ----------------------------------------------------
            // STARTED: slice 1 of 8
            // ...
            // DONE: slice 1 of 8
            // STARTED: slice 2 of 8
            // ...
            // DONE: slice 8 of 8

            // ReSharper restore CommentTypo

            const int FIRST_SEARCH_START = (int)ProgressPercentValues.StartingDiaNN + 1;
            const int FIRST_SEARCH_DONE = 44;

            const int MAIN_SEARCH_START = 50;
            const int MAIN_SEARCH_DONE = (int)ProgressPercentValues.DiaNNComplete;

            // ToDo: Update this to match DIA-NN output messages
            var processingSteps = new SortedList<int, Regex>
            {
                { (int)ProgressPercentValues.StartingDiaNN, GetRegEx("^JVM started") },
                { FIRST_SEARCH_START                      , GetRegEx(@"^\*+FIRST SEARCH\*+") },
                { FIRST_SEARCH_DONE                       , GetRegEx(@"^\*+FIRST SEARCH DONE") },
                { FIRST_SEARCH_DONE + 1                   , GetRegEx(@"^\*+MASS CALIBRATION AND PARAMETER OPTIMIZATION\*+") },
                { MAIN_SEARCH_START                       , GetRegEx(@"^\*+MAIN SEARCH\*+") },
                { MAIN_SEARCH_DONE                        , GetRegEx(@"^\*+MAIN SEARCH DONE") }
            };

            // Use a linked list to keep track of the progress values
            // This makes lookup of the next progress value easier
            var progressValues = new LinkedList<int>();

            foreach (var item in (from progressValue in processingSteps.Keys orderby progressValue select progressValue))
            {
                progressValues.AddLast(item);
            }
            progressValues.AddLast(100);

            // RegEx to match lines like:
            //  001. Sample_Bane_06May21_20-11-16.mzML 1.0 s | deisotoping 0.6 s
            //	001. QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.mzBIN_calibrated 0.1 s
            var datasetMatcher = new Regex(@"^[\t ]+(?<DatasetNumber>\d+)\. .+\.(mzML|mzBIN)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // RegEx to match lines like:
            // Operating on slice 1 of 2: 4463ms
            var sliceMatcher = new Regex(@"Operating on slice (?<Current>\d+) of (?<Total>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // RegEx to match lines like:
            // DatasetName.mzML 7042ms [progress: 29940/50420 (59.38%) - 5945.19 spectra/s]
            var progressMatcher = new Regex(@"progress: \d+/\d+ \((?<PercentComplete>[0-9.]+)%\)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var splitFastaMatcher = new Regex(@"^[\t ]*(?<Action>STARTED|DONE): slice (?<CurrentSplitFile>\d+) of (?<TotalSplitFiles>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var linesRead = 0;
                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    if (linesRead < 5)
                    {
                        // The first line has the path to the DIA-NN .jar file and the command line arguments
                        // The second line is dashes
                        // The third line should have the DIA-NN version

                        if (string.IsNullOrEmpty(mDiaNNVersion) &&
                            dataLine.StartsWith("DIA-NN version", StringComparison.OrdinalIgnoreCase))
                        {
                            LogDebug(dataLine, mDebugLevel);
                            mDiaNNVersion = string.Copy(dataLine);
                        }

                        if (dataLine.StartsWith(BATMASS_IO_VERSION, StringComparison.OrdinalIgnoreCase) &&
                            mDiaNNVersion.IndexOf(BATMASS_IO_VERSION, StringComparison.OrdinalIgnoreCase) < 0)
                        {
                            mDiaNNVersion = mDiaNNVersion + "; " + dataLine;
                        }

                        continue;
                    }

                    foreach (var processingStep in processingSteps)
                    {
                        if (!processingStep.Value.IsMatch(dataLine))
                            continue;

                        currentProgress = processingStep.Key;
                        break;
                    }

                    // Check whether the line starts with the text error
                    // Future: possibly adjust this check

                    if (currentProgress > 1 &&
                        dataLine.StartsWith("error", StringComparison.OrdinalIgnoreCase) &&
                        string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                    {
                        mConsoleOutputErrorMsg = "Error running DIA-NN: " + dataLine;
                    }
                }

                var effectiveProgressOverall = currentProgress;

                if (float.IsNaN(effectiveProgressOverall))
                {
                    return;
                }

                mProgress = effectiveProgressOverall;
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate(string.Format(
                        "Error parsing the DIA-NN console output file ({0}): {1}",
                        consoleOutputFilePath, ex.Message));
                }
            }
        }

        private CloseOutType StartDiaNN(FileInfo fastaFile)
        {
            try
            {
                LogMessage("Preparing to run DIA-NN");

                // If this job applies to a single dataset, dataPackageID will be 0
                // We still need to create an instance of DataPackageInfo to retrieve the experiment name associated with the job's dataset
                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                // The constructor for DataPackageInfo reads data package metadata from packed job parameters, which were created by the resource class
                var dataPackageInfo = new DataPackageInfo(dataPackageID, this);
                RegisterEvents(dataPackageInfo);

                if (dataPackageInfo.DatasetFiles.Count == 0)
                {
                    LogError("No datasets were found (dataPackageInfo.DatasetFiles is empty)");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Load the parameter file
                var paramFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);

                var paramFile = new FileInfo(Path.Combine(mWorkDir, paramFileName));

                var options = new DiaNNOptions();
                RegisterEvents(options);

                if (!options.LoadDiaNNOptions(paramFile.FullName))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(mWorkDir, DIA_NN_CONSOLE_OUTPUT)
                };
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                var fastaFileSizeMB = fastaFile.Length / 1024.0 / 1024;

                bool buildingSpectralLibrary;

                switch (StepToolName)
                {
                    case AnalysisResourcesDiaNN.DIA_NN_SPEC_LIB_STEP_TOOL:
                        buildingSpectralLibrary = true;
                        break;

                    case AnalysisResourcesDiaNN.DIA_NN_STEP_TOOL:
                        buildingSpectralLibrary = false;
                        break;

                    default:
                        LogError("Unrecognized step tool name: " + StepToolName);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                LogMessage("Running DIA-NN");
                mProgress = (int)ProgressPercentValues.StartingDiaNN;

                // Set up and execute a program runner to run DIA-NN

                var processingSuccess = StartDiaNN(options, fastaFile, buildingSpectralLibrary, dataPackageInfo);

                if (!mToolVersionWritten)
                {
                    if (string.IsNullOrWhiteSpace(mDiaNNVersion))
                    {
                        ParseDiaNNConsoleOutputFile(Path.Combine(mWorkDir, DIA_NN_CONSOLE_OUTPUT));
                    }
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputErrorMsg);
                }

                if (!processingSuccess)
                {
                    LogError("Error running DIA-NN");

                    if (mCmdRunner.ExitCode != 0)
                    {
                        LogWarning("DIA-NN returned a non-zero exit code: " + mCmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to DIA-NN failed (but exit code is 0)");
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var successCount = 0;

                // ToDo: Validate the output file(s) from DIA-NN

                mStatusTools.UpdateAndWrite(mProgress);
                LogDebug("DIA-NN Search Complete", mDebugLevel);

                return successCount == dataPackageInfo.Datasets.Count ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error in StartDiaNN", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool StartDiaNN(
            DiaNNOptions options,
            FileSystemInfo fastaFile,
            bool buildingSpectralLibrary,
            DataPackageInfo dataPackageInfo)
        {
            var defaultThreadCount = GetNumThreadsToUse();

            var numThreadsToUse = options.ThreadCount > 0
                ? Math.Min(options.ThreadCount, defaultThreadCount)
                : defaultThreadCount;

            var arguments = new StringBuilder();

            var datasetCount = dataPackageInfo.DatasetFiles.Count;

            if (buildingSpectralLibrary)
            {
                // Example command line arguments to have DIA-NN create a spectral library using an in-silico digest of a FASTA file
                // "C:\DMS_Programs\DIA-NN\DiaNN.exe"

                // ReSharper disable once CommentTypo

                // --lib "" --threads 8 --verbose 2 --temp "C:\DMS_WorkDir2" --predictor
                // --fasta "C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_Filtered.fasta" --fasta-search
                // --min-fr-mz 200 --max-fr-mz 1800 --met-excision --cut K*,R* --missed-cleavages 2
                // --min-pep-len 7 --max-pep-len 30 --min-pr-mz 350 --max-pr-mz 1800 --min-pr-charge 2 --max-pr-charge 4
                // --unimod4 --var-mods 3
                // --var-mod UniMod:35,15.994915,M
                // --var-mod UniMod:1,42.010565,*n --monitor-mod UniMod:1
                // --reanalyse --relaxed-prot-inf --smart-profiling

                arguments.AppendFormat(" --lib {0}", "\"\"");
                arguments.AppendFormat(" --threads {0}", numThreadsToUse);

                arguments.AppendFormat(" --verbose {0}", options.LogLevel);
                arguments.AppendFormat(" --temp {0}", mWorkDir);
                arguments.Append(" --predictor");
                arguments.AppendFormat(" --fasta {0}", fastaFile.FullName);
                arguments.Append(" --fasta-search");
                arguments.AppendFormat(" --min-fr-mz {0}", options.FragmentIonMzMin);
                arguments.AppendFormat(" --max-fr-mz {0}", options.FragmentIonMzMax);
                arguments.Append(" --met-excision");
                arguments.AppendFormat(" --cut {0}", options.CleavageSpecificity);
                arguments.AppendFormat(" --missed-cleavages {0}", options.MissedCleavages);
                arguments.AppendFormat(" --min-pep-len {0}", options.PeptideLengthMin);
                arguments.AppendFormat(" --max-pep-len {0}", options.PeptideLengthMax);
                arguments.AppendFormat(" --min-pr-mz {0}", options.PrecursorMzMin);
                arguments.AppendFormat(" --max-pr-mz {0}", options.PrecursorMzMax);
                arguments.AppendFormat(" --min-pr-charge {0}", options.PrecursorChargeMin);
                arguments.AppendFormat(" --max-pr-charge {0}", options.PrecursorChargeMax);

                AppendModificationArguments(options, arguments);

                AppendAdditionalArguments(options, datasetCount, arguments);
            }
            else
            {
                // Example command line arguments for using an existing spectral library to search DIA spectra
                // "C:\DMS_Programs\DIA-NN\DiaNN.exe"

                // ReSharper disable CommentTypo

                // --f "C:\DMS_WorkDir2\Dataset.mzML
                // --lib lib.predicted.speclib" --threads 8 --verbose 2
                // --out "C:\DMS_WorkDir1\report.tsv"
                // --qvalue 0.01 --matrices
                // --temp "C:\DMS_WorkDir1"
                // --out-lib "C:\DMS_WorkDir1\report-lib.tsv"
                // --gen-spec-lib
                // --fasta "C:\DMS_WorkDir1\H_sapiens_UniProt_SPROT_2021-06-20.fasta"
                // --met-excision --cut K*,R* --var-mods 3
                // --var-mod UniMod:35,15.994915,M
                // --var-mod UniMod:1,42.010565,*n
                // --monitor-mod UniMod:1
                // --window 0
                // --mass-acc 0 --mass-acc-ms1 0
                // --reanalyse --relaxed-prot-inf
                // --smart-profiling  --pg-level 2

                // ReSharper restore CommentTypo

                // Append the .mzML files
                foreach (var item in dataPackageInfo.DatasetFiles)
                {
                    arguments.AppendFormat(" --f \"{0}\"", Path.Combine(mWorkDir, item.Value));
                }

                arguments.AppendFormat(" --lib {0}", options.ExistingSpectralLibrary);
                arguments.AppendFormat(" --threads {0}", numThreadsToUse);
                arguments.AppendFormat(" --verbose {0}", 2);
                arguments.AppendFormat(" --out {0}", Path.Combine(mWorkDir, "report.tsv"));
                arguments.AppendFormat(" --qvalue {0}", options.PrecursorQValue);
                arguments.Append(" --matrices");
                arguments.AppendFormat(" --temp {0}", mWorkDir);
                arguments.AppendFormat(" --out-lib {0}", Path.Combine(mWorkDir, "report-lib.tsv"));
                arguments.Append(" --gen-spec-lib");
                arguments.AppendFormat(" --fasta {0}", fastaFile.FullName);
                arguments.Append(" --met-excision");
                arguments.AppendFormat(" --cut {0}", options.CleavageSpecificity);
                arguments.AppendFormat(" --var-mods {0}", options.MaxDynamicModsPerPeptide);

                AppendModificationArguments(options, arguments);

                if (options.ScanWindow != 0)
                    arguments.AppendFormat(" --window {0}", options.ScanWindow);

                if (options.MS2MassAccuracy > 0)
                    arguments.AppendFormat(" --mass-acc {0}", options.MS2MassAccuracy);

                if (options.MS1MassAccuracy > 0)
                    arguments.AppendFormat(" --mass-acc-ms1 {0}", options.MS1MassAccuracy);

                AppendAdditionalArguments(options, datasetCount, arguments);

                arguments.AppendFormat(" --pg-level {0}", (int)options.ProteinInferenceMode);
            }

            LogDebug(mDiaNNProgLoc + " " + arguments);

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            return mCmdRunner.RunProgram(mDiaNNProgLoc, arguments.ToString(), "DIA-NN", true);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            LogDebug("Determining tool version info", mDebugLevel);

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(mDiaNNProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(mDiaNNVersion, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Error calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private bool ValidateFastaFile(out FileInfo fastaFile)
        {
            // Define the path to the FASTA file
            var localOrgDbDirectory = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);
            var generatedFastaFileName = mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME);

            // Note that job parameter "GeneratedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
            var fastaFilePath = Path.Combine(localOrgDbDirectory, generatedFastaFileName);

            fastaFile = new FileInfo(fastaFilePath);

            if (!fastaFile.Exists)
            {
                // FASTA file not found
                LogError("FASTA file not found: " + fastaFile.Name, "FASTA file not found: " + fastaFile.FullName);
                return false;
            }

            var proteinOptions = mJobParams.GetParam("ProteinOptions");

            if (!string.IsNullOrEmpty(proteinOptions) && proteinOptions.IndexOf("seq_direction=decoy", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // The FASTA file has decoy sequences
                LogError("Protein options for this analysis job must contain seq_direction=forward, not seq_direction=decoy " +
                         "(since DIA-NN will auto-add decoy sequences)");
                return false;
            }

            return true;
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

            ParseDiaNNConsoleOutputFile(Path.Combine(mWorkDir, DIA_NN_CONSOLE_OUTPUT));

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mDiaNNVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("DIA-NN");
        }
    }
}
