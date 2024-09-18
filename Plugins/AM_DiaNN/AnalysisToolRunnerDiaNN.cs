//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 03/11/2023
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;
using PRISM;
using PRISMDatabaseUtils;

namespace AnalysisManagerDiaNNPlugIn
{
    /// <summary>
    /// Class for running DIA-NN
    /// </summary>
    public class AnalysisToolRunnerDiaNN : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: acc, analyse, carbamidomethylation, Cysteine, dia, evalue, fasta, Initialising, isoforms, len
        // Ignore Spelling: optimise, optimising, qvalue, pre, prot, proteotypic, reanalyse, Regex, silico, Xeon

        // ReSharper restore CommentTypo

#pragma warning disable VSSpell001

        /// <summary>
        /// DIA-NN executable name
        /// </summary>
        public const string DIA_NN_EXE_NAME = "diann.exe";
#pragma warning restore VSSpell001

        private bool mBuildingSpectralLibrary;

        private int mDatasetCount;

        private string mDiaNNConsoleOutputFile;

        /// <summary>
        /// Progress value to use when preparing to run DIA-NN
        /// </summary>
        public const float PROGRESS_PCT_INITIALIZING = 1;

        private enum LibraryCreationCompletionCode
        {
            Success = 0,
            UnknownError = 1,
            ParameterFileError = 2,
            DiaNNError = 3,
            FileCopyError = 4,
            FileRenameError = 5,
            LibraryNotCreated = 6,
            MultipleLibrariesCreated = 7,
            NoFilterPassingResults = 8
        }

        private enum ProgressPercentValues
        {
            Initializing = 0,
            StartingDiaNN = 1,
            DiaNNComplete = 98,
            ProcessingComplete = 99
        }

        private RunDosProgram mCmdRunner;

        private string mConsoleOutputErrorMsg;

        private string mDiaNNVersion;

        private string mDiaNNProgLoc;

        /// <summary>
        /// This Regex matches the file number line in the console output, e.g. File #1/2
        /// </summary>
        private readonly Regex mFileNumberMatcher = new(@"File\s*#(?<FileNumber>\d+)/(?<FileCount>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private DateTime mLastConsoleOutputParse;

        /// <summary>
        /// This Regex matches the runtime values at the start of console output lines, e.g. [3:56]
        /// </summary>
        /// <remarks>
        /// It also matches a series of times values on the same line, e.g. [0:03] [0:05] [3:56] [4:22]
        /// </remarks>
        private readonly Regex mRuntimeMatcher = new(@"\[[\d: \[\]]+\](?<ProgressMessage>.+)", RegexOptions.Compiled);

        private bool mToolVersionWritten;

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

                // Determine the path to DIA-NN, typically "C:\DMS_Programs\DIA-NN\DiaNN.exe"
                mDiaNNProgLoc = DetermineProgramLocation("DiaNNProgLoc", DIA_NN_EXE_NAME);

                if (string.IsNullOrWhiteSpace(mDiaNNProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the DIA-NN version info in the database after the first line is written to file DiaNN_ConsoleOutput.txt
                mToolVersionWritten = false;
                mDiaNNVersion = string.Empty;

                mConsoleOutputErrorMsg = string.Empty;

                if (!ValidateFastaFile(out var fastaFile))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                switch (StepToolName)
                {
                    case AnalysisResourcesDiaNN.DIA_NN_SPEC_LIB_STEP_TOOL:
                        mBuildingSpectralLibrary = true;
                        mDiaNNConsoleOutputFile = "DiaNN_ConsoleOutput_CreateLibrary.txt";
                        break;

                    case AnalysisResourcesDiaNN.DIA_NN_STEP_TOOL:
                        mBuildingSpectralLibrary = false;
                        mDiaNNConsoleOutputFile = "DiaNN_ConsoleOutput_SearchSpectra.txt";
                        break;

                    default:
                        LogError("Unrecognized step tool name: " + StepToolName);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                var spectralLibraryFile = GetSpectralLibraryFile(out var remoteSpectralLibraryFile, out var spectralLibraryID);

                if (spectralLibraryFile == null)
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        LogError("GetSpectralLibraryFile returned null for the spectral library file");
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // If mBuildingSpectralLibrary is true, create the spectral library file
                // If mBuildingSpectralLibrary is false, process the mzML files using DIA-NN

                var processingResult = StartDiaNN(fastaFile, spectralLibraryFile, out var completionCode);

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

                    // ReSharper disable once InvertIf
                    if (mBuildingSpectralLibrary)
                    {
                        if (completionCode == LibraryCreationCompletionCode.Success)
                        {
                            completionCode = LibraryCreationCompletionCode.UnknownError;
                        }

                        // Update the library state
                        SetSpectralLibraryCreateTaskComplete(spectralLibraryID, completionCode);
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Define additional files to skip
                mJobParams.AddResultFileToSkip("lib.log.txt");
                mJobParams.AddResultFileToSkip(GetDiannResultsFilePath("report-lib.tsv").Name);
                mJobParams.AddResultFileToSkip("report.log.txt");
                mJobParams.AddResultFileExtensionToSkip("_mzML.quant");

                var success = CopyResultsToTransferDirectory(spectralLibraryFile, remoteSpectralLibraryFile, ref completionCode);

                bool success2;

                // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                if (mBuildingSpectralLibrary)
                {
                    // Update the library state
                    success2 = SetSpectralLibraryCreateTaskComplete(spectralLibraryID, completionCode);
                }
                else
                {
                    success2 = true;
                }

                if (success && success2)
                    return processingResult;

                return CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error in DiaNNPlugIn->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void AppendAdditionalArguments(DiaNNOptions options, StringBuilder arguments)
        {
            if (options.MatchBetweenRuns && mDatasetCount > 1)
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

                /*
                 * Removed in DIA-NN 1.9
                 *

                    if (dynamicMod.MonitorMod)
                    {
                        arguments.AppendFormat(" --monitor-mod {0}", dynamicMod.ModificationName);
                    }
                */

                if (dynamicMod.DisableScoring)
                {
                    arguments.AppendFormat(" --mod-no-scoring {0}", dynamicMod.ModificationName);
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

        private bool CopyResultsToTransferDirectory(
            FileSystemInfo spectralLibraryFile,
            FileSystemInfo remoteSpectralLibraryFile,
            ref LibraryCreationCompletionCode completionCode)
        {
            var currentTask = "preparing to copy results to the transfer directory";

            try
            {
                if (mBuildingSpectralLibrary)
                {
                    // Copy the spectral library file to the remote location
                    currentTask = "copying the spectral library to " + remoteSpectralLibraryFile.FullName;

                    mFileTools.CopyFile(spectralLibraryFile.FullName, remoteSpectralLibraryFile.FullName, true);

                    mJobParams.AddResultFileToSkip(remoteSpectralLibraryFile.Name);
                }

                currentTask = "copying results to the transfer directory";
                base.CopyResultsToTransferDirectory();

                return true;
            }
            catch (Exception ex)
            {
                // Error copying the spectral library to ...
                // Error copying results to the transfer directory

                LogError(string.Format("Error {0}", currentTask), ex);

                completionCode = LibraryCreationCompletionCode.FileCopyError;
                return false;
            }
        }

        private bool GeneratePdfReport(FileSystemInfo reportFile, FileSystemInfo reportStatsFile, FileSystemInfo reportPdfFile)
        {
            try
            {
                var diannProgram = new FileInfo(mDiaNNProgLoc);

                if (diannProgram.DirectoryName == null)
                {
                    LogError(string.Format("Unable to determine the parent directory of the DIA-NN executable: {0}", diannProgram.FullName));
                    return false;
                }

                var diannPlotterProgram = new FileInfo(Path.Combine(diannProgram.DirectoryName, "DIA-NN-plotter.exe"));

                if (!diannPlotterProgram.Exists)
                {
                    LogError(string.Format("DIA-NN Plotter executable not found: {0}", diannPlotterProgram.FullName));
                    return false;
                }

                var arguments = new StringBuilder();

                // ReSharper disable CommentTypo

                // Example command line:
                // DIA-NN-plotter.exe "C:\DMS_WorkDir\report.stats.tsv" "C:\DMS_WorkDir\report.tsv" "C:\DMS_WorkDir\report.pdf"

                // ReSharper restore CommentTypo

                arguments.AppendFormat("{0} {1} {2}", reportStatsFile.FullName, reportFile.FullName, reportPdfFile.FullName);

                LogDebug(diannPlotterProgram + " " + arguments);

                var diaNNPlotterConsoleOutputFile = new FileInfo(Path.Combine(mWorkDir, "DiaNN-Plotter_ConsoleOutput.txt"));

                // Start the program and wait for it to finish

                var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = diaNNPlotterConsoleOutputFile.FullName
                };
                RegisterEvents(cmdRunner);

                return cmdRunner.RunProgram(diannPlotterProgram.FullName, arguments.ToString(), "DiaNN-Plotter", true);
            }
            catch (Exception ex)
            {
                LogError("Error in GeneratePdfReport", ex);

                return false;
            }
        }

        private int GetCurrentProgress(
            SortedList<int, Regex> processingSteps,
            string dataLine)
        {
            // Look for, and remove the runtime value from the start of dataLine
            // For example, remove [4:38] from "[4:38] Cross-run analysis"
            var match = mRuntimeMatcher.Match(dataLine);

            var progressMessage = match.Success ? match.Groups["ProgressMessage"].Value.Trim() : dataLine;

            // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
            foreach (var processingStep in processingSteps)
            {
                if (!processingStep.Value.IsMatch(progressMessage))
                    continue;

                return processingStep.Key;
            }

            return 0;
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
        /// Get the spectral library file info
        /// </summary>
        /// <param name="remoteSpectralLibraryFile">Output: Remote file that corresponds to the local file</param>
        /// <param name="spectralLibraryID">Output: Spectral Library ID</param>
        /// <returns>FileInfo instance for the local spectral library file to use or create; null if an error</returns>
        private FileInfo GetSpectralLibraryFile(out FileInfo remoteSpectralLibraryFile, out int spectralLibraryID)
        {
            spectralLibraryID = mJobParams.GetJobParameter(
                AnalysisJob.STEP_PARAMETERS_SECTION,
                AnalysisResourcesDiaNN.SPECTRAL_LIBRARY_FILE_ID,
                0);

            if (spectralLibraryID == 0)
            {
                LogError(
                    "Cannot determine the spectral library ID since job parameter {0} is not defined in section {1}",
                    AnalysisResourcesDiaNN.SPECTRAL_LIBRARY_FILE_ID, AnalysisJob.STEP_PARAMETERS_SECTION);

                remoteSpectralLibraryFile = null;
                return null;
            }

            var remoteSpectralLibraryFilePath = mJobParams.GetJobParameter(
                AnalysisJob.STEP_PARAMETERS_SECTION,
                AnalysisResourcesDiaNN.SPECTRAL_LIBRARY_FILE_REMOTE_PATH_JOB_PARAM,
                string.Empty);

            if (string.IsNullOrWhiteSpace(remoteSpectralLibraryFilePath))
            {
                LogError(
                    "Cannot determine the spectral library file name since job parameter {0} is not defined in section {1}",
                    AnalysisResourcesDiaNN.SPECTRAL_LIBRARY_FILE_REMOTE_PATH_JOB_PARAM, AnalysisJob.STEP_PARAMETERS_SECTION);

                remoteSpectralLibraryFile = null;
                spectralLibraryID = 0;
                return null;
            }

            remoteSpectralLibraryFile = new FileInfo(remoteSpectralLibraryFilePath);

            var spectralLibraryFile = Path.Combine(mWorkDir, remoteSpectralLibraryFile.Name);

            return new FileInfo(spectralLibraryFile);
        }

        private void ParseDiaNNConsoleOutputFile()
        {
            var consoleOutputFilePath = Path.Combine(mWorkDir, mDiaNNConsoleOutputFile);

            if (mBuildingSpectralLibrary)
                ParseDiaNNConsoleOutputFileCreateSpecLib(consoleOutputFilePath);
            else
                ParseDiaNNConsoleOutputFileSearchDIA(consoleOutputFilePath);
        }

        /// <summary>
        /// Parse the DIA-NN console output file to determine the DIA-NN version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseDiaNNConsoleOutputFileCreateSpecLib(string consoleOutputFilePath)
        {
            // ReSharper disable CommentTypo

            // ----------------------------------------------------
            // Example Console output when creating a spectral library using an in-silico digest of a FASTA file
            // ----------------------------------------------------

            // DIA-NN 1.8.1 (Data-Independent Acquisition by Neural Networks)
            // Compiled on Apr 14 2022 15:31:19
            // Current date and time: Wed Feb 22 12:52:36 2023
            // CPU: GenuineIntel Intel(R) Xeon(R) W-2245 CPU @ 3.90GHz
            // SIMD instructions: AVX AVX2 AVX512CD AVX512F FMA SSE4.1 SSE4.2
            // Logical CPU cores: 16
            // C:\DMS_Programs\DIA-NN\DiaNN.exe --lib  --threads 8 --verbose 2 --predictor --fasta C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\H_sapiens_UniProt_SPROT_2021-06-20_Filtered.fasta --fasta-search --min-fr-mz 200 --max-fr-mz 1800 --met-excision --cut K*,R* --missed-cleavages 2 --min-pep-len 7 --max-pep-len 30 --min-pr-mz 350 --max-pr-mz 1800 --min-pr-charge 2 --max-pr-charge 4 --unimod4 --var-mods 3 --var-mod UniMod:35,15.994915,M --reanalyse --relaxed-prot-inf --smart-profiling

            // Thread number set to 8
            // Deep learning will be used to generate a new in silico spectral library from peptides provided
            // Library-free search enabled
            // Min fragment m/z set to 200
            // Max fragment m/z set to 1800
            // N-terminal methionine excision enabled
            // In silico digest will involve cuts at K*,R*
            // Maximum number of missed cleavages set to 2
            // Min peptide length set to 7
            // Max peptide length set to 30
            // Min precursor m/z set to 350
            // Max precursor m/z set to 1800
            // Min precursor charge set to 2
            // Max precursor charge set to 4
            // Cysteine carbamidomethylation enabled as a fixed modification
            // Maximum number of variable modifications set to 3
            // Modification UniMod:35 with mass delta 15.9949 at M will be considered as variable
            // A spectral library will be created from the DIA runs and used to reanalyse them; .quant files will only be saved to disk during the first step
            // Highly heuristic protein grouping will be used, to reduce the number of protein groups obtained; this mode is recommended for benchmarking protein ID numbers; use with caution for anything else
            // When generating a spectral library, in silico predicted spectra will be retained if deemed more reliable than experimental ones
            // Exclusion of fragments shared between heavy and light peptides from quantification is not supported in FASTA digest mode - disabled; to enable, generate an in silico predicted spectral library and analyse with this library

            // 0 files will be processed
            // [0:00] Loading FASTA C:\DMS_Temp_Org\ID_008358_7EC82878.fasta
            // [0:01] Processing FASTA
            // [0:03] Assembling elution groups
            // [0:04] 802515 precursors generated
            // [0:04] Gene names missing for some isoforms
            // [0:04] Library contains 1506 proteins, and 1505 genes
            // [0:04] Encoding peptides for spectra and RTs prediction
            // [0:05] Predicting spectra and IMs
            // [4:10] Predicting RTs
            // [4:37] Decoding predicted spectra and IMs
            // [4:41] Decoding RTs
            // [4:41] Saving the library to lib.predicted.speclib
            // [4:42] Initialising library
            // [4:43] Log saved to lib.log.txt
            // Finished

            // ReSharper restore CommentTypo

            var processingSteps = new SortedList<int, Regex>
            {
                { (int)ProgressPercentValues.StartingDiaNN, GetRegEx("^Loading FASTA") },
                { 2                                       , GetRegEx("^Processing FASTA") },
                { 3                                       , GetRegEx("^Assembling elution groups") },
                { 5                                       , GetRegEx("^Library contains") },
                { 90                                      , GetRegEx("^Predicting RTs") },
                { 92                                      , GetRegEx("^Decoding predicted spectra and IMs") },
                { 93                                      , GetRegEx("^Decoding RTs") },
                { 95                                      , GetRegEx("^Saving the library to") },
                // ReSharper disable once StringLiteralTypo
                { 97                                       , GetRegEx("^Initialising library") },
                { (int)ProgressPercentValues.DiaNNComplete , GetRegEx("^Finished") }
            };

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
                var currentProgress = (int)ProgressPercentValues.StartingDiaNN;

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
                        // The first line has the DIA-NN version number

                        if (string.IsNullOrEmpty(mDiaNNVersion) &&
                            dataLine.StartsWith("DIA-NN", StringComparison.OrdinalIgnoreCase))
                        {
                            StoreDiaNNVersion(dataLine);
                            continue;
                        }
                    }

                    var progressForLine = GetCurrentProgress(processingSteps, dataLine);

                    if (progressForLine > currentProgress)
                        currentProgress = progressForLine;

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

        /// <summary>
        /// Parse the DIA-NN console output file to determine the DIA-NN version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseDiaNNConsoleOutputFileSearchDIA(string consoleOutputFilePath)
        {
            // ReSharper disable CommentTypo

            // ----------------------------------------------------
            // Example Console output when searching DIA spectra using a spectral library
            // ----------------------------------------------------

            // DIA-NN 1.9.1 (Data-Independent Acquisition by Neural Networks)
            // Compiled on Jul 15 2024 15:40:36
            // Current date and time: Sat Aug 31 16:59:30 2024
            // CPU: GenuineIntel Intel(R) Xeon(R) W-2245 CPU @ 3.90GHz
            // SIMD instructions: AVX AVX2 AVX512CD AVX512F FMA SSE4.1 SSE4.2
            // Logical CPU cores: 16
            // C:\DMS_Programs\DIA-NN\DiaNN.exe --f C:\DMS_WorkDir2\MM_Strap_IMAC_FT_10xDilution_FAIMS_ID_01_FAIMS_Merry_03Feb23_REP-22-11-13.mzML  --lib lib.predicted.speclib --threads 8 --verbose 2 --out C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\report.tsv --qvalue 0.01 --matrices --temp C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2" --out-lib C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\report-lib.tsv --gen-spec-lib --fasta C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\H_sapiens_UniProt_SPROT_2021-06-20_Filtered.fasta --met-excision --cut K*,R* --var-mods 3 --var-mod UniMod:35,15.994915,M --reanalyse --relaxed-prot-inf --smart-profiling
            //
            // Thread number set to 7
            // Output will be filtered at 0.01 FDR
            // Precursor/protein x samples expression level matrices will be saved along with the main report
            // A spectral library will be generated
            // N-terminal methionine excision enabled
            // In silico digest will involve cuts at K*,R*
            // Maximum number of variable modifications set to 3
            // Cysteine carbamidomethylation enabled as a fixed modification
            // Maximum number of variable modifications set to 3
            // Modification UniMod:35 with mass delta 15.9949 at M will be considered as variable
            // Heuristic protein grouping will be used, to reduce the number of protein groups obtained; this mode is recommended for benchmarking protein ID numbers, GO/pathway and system-scale analyses
            // When generating a spectral library, in silico predicted spectra will be retained if deemed more reliable than experimental ones
            // Implicit protein grouping: genes; this determines which peptides are considered 'proteotypic' and thus affects protein FDR calculation
            // DIA-NN will optimise the mass accuracy automatically using the first run in the experiment. This is useful primarily for quick initial analyses, when it is not yet known which mass accuracy setting works best for a particular acquisition scheme.
            // WARNING: peptidoform scoring enabled because variable modifications have been declared; to disable, use --no-peptidoforms
            // The following variable modifications will be scored: UniMod:35
            // 1 files will be processed
            // [0:00] Loading spectral library C:\DMS_WorkDir\H_sapiens_UniProt_SPROT_2023-03-01_Tryp_Pig_Bov_7918CB1B.predicted.speclib
            // [0:10] Library annotated with sequence database(s): E:\DMS_Temp_Org\ID_008368_98FC29DE.fasta
            // [0:12] Spectral library loaded: 20403 protein isoforms, 30350 protein groups and 7717737 precursors in 2997333 elution groups.
            // [0:12] Loading protein annotations from FASTA C:\DMS_Temp_Org\ID_008368_98FC29DE.fasta
            // [0:13] Annotating library proteins with information from the FASTA database
            // [0:13] Gene names missing for some isoforms
            // [0:13] Library contains 20399 proteins, and 20195 genes
            // [0:24] Initialising library
            // [0:45] File #1/1
            // [0:45] Loading run C:\DMS_WorkDir\MM_Strap_IMAC_FT_10xDilution_FAIMS_ID_01_FAIMS_Merry_03Feb23_REP-22-11-13.mzML
            // [1:02] Run loaded: 3702 MS1 scans and 88848 MS2 scans
            // [1:03] 7590809 library precursors are potentially detectable
            // [1:11] Processing batch #1 out of 3795
            // [1:11] Precursor search
            // [1:13] Optimising weights
            // [1:14] Calculating q-values
            // [1:14] Number of IDs at 0.01 FDR: 0
            // [1:14] Calculating q-values
            // [1:14] Number of IDs at 0.01 FDR: 0
            // [1:14] Calibrating retention times
            // [1:14] 150 precursors used for iRT estimation.
            // [1:14] Processing batch #2 out of 3795
            // [1:14] Precursor search
            // ...
            // [1:21] Processing batches #6-7 out of 3795
            // [1:21] Precursor search
            // [1:24] Optimising weights
            // [1:25] Calculating q-values
            // [1:25] Number of IDs at 0.01 FDR: 0
            // [1:25] Calibrating retention times
            // [1:25] 150 precursors used for iRT estimation.
            // [1:25] Processing batches #8-9 out of 3795
            // [1:25] Precursor search
            // [1:28] Optimising weights
            // [1:28] Calculating q-values
            // ...
            // [1:41] 150 precursors used for iRT estimation.
            // [1:41] Top 70% mass accuracy: 2.07368 ppm
            // [1:41] Top 70% mass accuracy without correction: 5.38199ppm
            // [1:41] Cannot perform MS1 mass calibration, too few confidently identified precursors
            // [1:41] Recalibrating with mass accuracy 1.03684e-05, 2e-05 (MS2, MS1)
            // [1:41] Processing batch #1 out of 3795
            // [1:41] Precursor search
            // [1:43] Optimising weights
            // [1:44] Calculating q-values
            // ...
            // [2:14] Processing batches #12-14 out of 394
            // [2:14] Precursor search
            // [2:15] Optimising weights
            // [2:15] Calculating q-values
            // [2:15] Number of IDs at 0.01 FDR: 520
            // ...
            // [4:30] Calculating q-values
            // [4:30] Number of IDs at 0.01 FDR: 28852
            // [4:30] Calibrating retention times
            // [4:30] 28852 precursors used for iRT estimation.
            // [4:30] Optimising weights
            // [4:31] Training neural networks: 67444 targets, 40370 decoys
            // [4:36] Calculating q-values
            // [4:36] Number of IDs at 0.01 FDR: 35620
            // [4:36] Calibrating retention times
            // [4:36] 35620 precursors used for iRT estimation.
            // [4:37] Calculating protein q-values
            // [4:37] Number of genes identified at 1% FDR: 1493 (precursor-level), 1493 (protein-level) (inference performed using proteotypic peptides only)
            // [4:37] Quantification
            // [4:38] Quantification information saved to C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2/C__DMS_WorkDir2_MM_Strap_IMAC_FT_10xDilution_FAIMS_ID_01_FAIMS_Merry_03Feb23_REP-22-11-13_mzML.quant.

            // [4:38] Cross-run analysis
            // [4:38] Reading quantification information: 1 files
            // [4:38] Quantifying peptides
            // [4:38] Assembling protein groups
            // [4:38] Quantifying proteins
            // [4:38] Calculating q-values for protein and gene groups
            // [4:38] Calculating global q-values for protein and gene groups
            // [4:39] Writing report
            // [4:40] Report saved to C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\report.tsv.
            // [4:40] Saving precursor levels matrix
            // [4:41] Precursor levels matrix (1% precursor and protein group FDR) saved to C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\report.pr_matrix.tsv.
            // [4:41] Saving protein group levels matrix
            // [4:41] Protein group levels matrix (1% precursor FDR and protein group FDR) saved to C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\report.pg_matrix.tsv.
            // [4:41] Saving gene group levels matrix
            // [4:41] Gene groups levels matrix (1% precursor FDR and protein group FDR) saved to C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\report.gg_matrix.tsv.
            // [4:41] Saving unique genes levels matrix
            // [4:41] Unique genes levels matrix (1% precursor FDR and protein group FDR) saved to C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\report.unique_genes_matrix.tsv.
            // [4:41] Stats report saved to C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\report.stats.tsv
            // [4:41] Generating spectral library:
            // [4:41] 35620 precursors passing the FDR threshold are to be extracted
            // [4:41] Loading run C:\DMS_WorkDir2\MM_Strap_IMAC_FT_10xDilution_FAIMS_ID_01_FAIMS_Merry_03Feb23_REP-22-11-13.mzML
            // [4:54] Run loaded
            // [4:54] 789178 library precursors are potentially detectable
            // [4:56] 29046 spectra added to the library
            // [4:56] Saving spectral library to C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\report-lib.tsv
            // [4:59] 35620 precursors saved
            // [4:59] Loading the generated library and saving it in the .speclib format
            // [4:59] Loading spectral library C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\report-lib.tsv
            // [5:01] Spectral library loaded: 1494 protein isoforms, 1556 protein groups and 35620 precursors in 30422 elution groups.
            // [5:01] Loading protein annotations from FASTA C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\H_sapiens_UniProt_SPROT_2021-06-20_Filtered.fasta
            // [5:01] Gene names missing for some isoforms
            // [5:01] Library contains 1494 proteins, and 1493 genes
            // [5:01] Saving the library to C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\report-lib.tsv.speclib

            // Finished

            // ----------------------------------------------------
            // Output when searching DIA spectra in multiple .mzML files
            // ----------------------------------------------------

            // 2 files will be processed
            // [0:00] Loading FASTA C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_Filtered.fasta
            // [0:00] Processing FASTA
            // [0:01] Assembling elution groups
            // [0:02] 536642 precursors generated
            // [0:02] Gene names missing for some isoforms
            // [0:02] Library contains 1494 proteins, and 1493 genes
            // [0:02] [0:03] [2:39] [2:57] [2:59] [2:59] Saving the library to C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\report-lib.predicted.speclib
            // [3:00] Initialising library

            // [3:00] First pass: generating a spectral library from DIA data
            // [3:00] File #1/2
            // [3:00] Loading run C:\DMS_WorkDir2\MM_Strap_IMAC_FT_10xDilution_FAIMS_ID_01_FAIMS_Merry_03Feb23_REP-22-11-13.mzML
            // [3:15] Run loaded
            // [3:15] 530823 library precursors are potentially detectable
            // [3:15] Processing batch #1 out of 265
            // [3:15] Precursor search
            // ...
            // [5:36] Training neural networks: 58686 targets, 33017 decoys
            // [5:41] Calculating q-values
            // [5:41] Number of IDs at 0.01 FDR: 31539
            // [5:41] Calibrating retention times
            // [5:41] 31539 precursors used for iRT estimation.
            // [5:41] Calculating protein q-values
            // [5:41] Number of genes identified at 1% FDR: 1493 (precursor-level), 1493 (protein-level) (inference performed using proteotypic peptides only)
            // [5:41] Quantification
            // [5:42] Quantification information saved to C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2/C__DMS_WorkDir2_MM_Strap_IMAC_FT_10xDilution_FAIMS_ID_01_FAIMS_Merry_03Feb23_REP-22-11-13_mzML.quant.

            // [5:43] File #2/2
            // [5:43] Loading run C:\DMS_WorkDir2\CHI_XN_DA_25_Bane_06May21_20-11-16.mzML
            // WARNING: more than 1000 different isolation windows - is this intended?
            // [5:46] Run loaded
            // [5:46] 406510 library precursors are potentially detectable
            // [5:46] Processing batch #1 out of 203
            // ...
            // [6:56] 160 precursors used for iRT estimation.
            // [6:56] Calculating protein q-values
            // [6:56] Number of genes identified at 1% FDR: 19 (precursor-level), 0 (protein-level) (inference performed using proteotypic peptides only)
            // [6:56] Quantification
            // [6:56] Quantification information saved to C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2/C__DMS_WorkDir2_CHI_XN_DA_25_Bane_06May21_20-11-16_mzML.quant.

            // [6:56] Cross-run analysis
            // [6:56] Reading quantification information: 2 files
            // [6:56] Quantifying peptides
            // [6:56] Assembling protein groups
            // [6:56] Quantifying proteins
            // [6:56] Calculating q-values for protein and gene groups
            // [6:56] Calculating global q-values for protein and gene groups
            // [6:56] Writing report
            // ...
            // [6:58] Generating spectral library:
            // ...
            // [7:20] Library contains 1494 proteins, and 1493 genes
            // [7:20] Saving the library to C:\DMS_WorkDir2\H_sapiens_UniProt_SPROT_2021-06-20_excerpt2\report-lib.tsv.speclib

            // [7:21] Second pass: using the newly created spectral library to reanalyse the data
            // [7:21] File #1/2
            // [7:21] Loading run C:\DMS_WorkDir2\MM_Strap_IMAC_FT_10xDilution_FAIMS_ID_01_FAIMS_Merry_03Feb23_REP-22-11-13.mzML
            // [7:35] Run loaded
            // ...
            // [7:45] 31099 precursors used for iRT estimation.
            // [7:46] Calculating protein q-values
            // [7:46] Number of genes identified at 1% FDR: 1493 (precursor-level), 1493 (protein-level) (inference performed using proteotypic peptides only)
            // [7:46] Quantification

            // [7:46] File #2/2
            // [7:46] Loading run C:\DMS_WorkDir2\CHI_XN_DA_25_Bane_06May21_20-11-16.mzML
            // WARNING: more than 1000 different isolation windows - is this intended?
            // [7:49] Run loaded
            // [7:49] 28822 library precursors are potentially detectable
            // ...
            // [7:53] 156 precursors used for iRT estimation.
            // [7:53] Optimising weights
            // [7:53] Too few confident identifications, neural networks will not be used
            // [7:53] Calculating q-values
            // [7:53] Number of IDs at 0.01 FDR: 155
            // [7:53] Calculating q-values
            // [7:53] Number of IDs at 0.01 FDR: 155
            // [7:53] Calibrating retention times
            // [7:53] 155 precursors used for iRT estimation.
            // [7:53] Calculating protein q-values
            // [7:53] Number of genes identified at 1% FDR: 21 (precursor-level), 0 (protein-level) (inference performed using proteotypic peptides only)
            // [7:53] Quantification

            // [7:53] Cross-run analysis
            // [7:53] Reading quantification information: 2 files
            // [7:53] Quantifying peptides
            // [7:54] Quantifying proteins
            // [7:54] Calculating q-values for protein and gene groups
            // [7:54] Calculating global q-values for protein and gene groups
            // [7:54] Writing report
            // [7:55] Report saved to C:\DMS_WorkDir2\report.tsv.
            // [7:55] Saving precursor levels matrix
            // [7:55] Precursor levels matrix (1% precursor and protein group FDR) saved to C:\DMS_WorkDir2\report.pr_matrix.tsv.
            // [7:55] Saving protein group levels matrix
            // [7:55] Protein group levels matrix (1% precursor FDR and protein group FDR) saved to C:\DMS_WorkDir2\report.pg_matrix.tsv.
            // [7:55] Saving gene group levels matrix
            // [7:55] Gene groups levels matrix (1% precursor FDR and protein group FDR) saved to C:\DMS_WorkDir2\report.gg_matrix.tsv.
            // [7:55] Saving unique genes levels matrix
            // [7:55] Unique genes levels matrix (1% precursor FDR and protein group FDR) saved to C:\DMS_WorkDir2\report.unique_genes_matrix.tsv.
            // [7:55] Stats report saved to C:\DMS_WorkDir2\report.stats.tsv

            // Finished

            // ReSharper restore CommentTypo

            var processingSteps = new SortedList<int, Regex>
            {
                { (int)ProgressPercentValues.StartingDiaNN, GetRegEx("^Loading spectral library") },
                // ReSharper disable once StringLiteralTypo
                { 2, GetRegEx("^Initialising library") }
            };

            const int PROGRESS_FIRST_PASS = 5;
            const int PROGRESS_SECOND_PASS = 75;
            const int PROGRESS_CROSS_RUN_ANALYSIS = 90;

            if (mDatasetCount <= 1)
            {
                processingSteps.Add(5, GetRegEx("^File #1"));
                processingSteps.Add(6, GetRegEx("^Run loaded"));
                processingSteps.Add(80, GetRegEx("^Number of genes identified"));
            }
            else
            {
                processingSteps.Add(PROGRESS_FIRST_PASS, GetRegEx("^First pass"));
                processingSteps.Add(PROGRESS_SECOND_PASS, GetRegEx("^Second pass"));
            }

            processingSteps.Add(PROGRESS_CROSS_RUN_ANALYSIS, GetRegEx("^Cross-run analysis"));
            processingSteps.Add(91, GetRegEx("^Writing report"));
            processingSteps.Add(92, GetRegEx("^Generating spectral library"));
            processingSteps.Add(97, GetRegEx("^Saving the library to"));
            processingSteps.Add((int)ProgressPercentValues.DiaNNComplete, GetRegEx("^Finished"));

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
                var currentProgress = (int)ProgressPercentValues.StartingDiaNN;

                var effectiveProgressOverall = 0.0f;

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var linesRead = 0;
                var currentFileNumber = 0;
                var totalFileCount = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    if (linesRead < 5)
                    {
                        // The first line has the DIA-NN version number

                        if (string.IsNullOrEmpty(mDiaNNVersion) &&
                            dataLine.StartsWith("DIA-NN", StringComparison.OrdinalIgnoreCase))
                        {
                            StoreDiaNNVersion(dataLine);
                            continue;
                        }
                    }

                    var match = mFileNumberMatcher.Match(dataLine);

                    if (match.Success)
                    {
                        currentFileNumber = int.Parse(match.Groups["FileNumber"].Value);
                        totalFileCount = int.Parse(match.Groups["FileCount"].Value);
                        continue;
                    }

                    var progressForLine = GetCurrentProgress(processingSteps, dataLine);

                    if (progressForLine > currentProgress)
                        currentProgress = progressForLine;

                    // Check whether the line starts with the text error
                    // Future: possibly adjust this check

                    if (currentProgress > 1 &&
                        dataLine.StartsWith("error", StringComparison.OrdinalIgnoreCase) &&
                        string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                    {
                        mConsoleOutputErrorMsg = "Error running DIA-NN: " + dataLine;
                    }

                    if (mDatasetCount <= 1)
                    {
                        effectiveProgressOverall = currentProgress;
                        continue;
                    }

                    effectiveProgressOverall = currentProgress switch
                    {
                        PROGRESS_FIRST_PASS => ComputeIncrementalProgress(PROGRESS_FIRST_PASS, PROGRESS_SECOND_PASS, currentFileNumber - 1, totalFileCount),
                        PROGRESS_SECOND_PASS => ComputeIncrementalProgress(PROGRESS_SECOND_PASS, PROGRESS_CROSS_RUN_ANALYSIS, currentFileNumber - 1, totalFileCount),
                        _ => currentProgress
                    };
                }

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

        private bool RenameSpectralLibraryFile(
            FileInfo specLibFile,
            FileSystemInfo spectralLibraryFile,
            out LibraryCreationCompletionCode completionCode)
        {
            try
            {
                var newFilePath = Path.Combine(mWorkDir, spectralLibraryFile.Name);
                LogMessage("Renaming {0} to {1}", specLibFile.Name, newFilePath);

                specLibFile.MoveTo(newFilePath);

                completionCode = LibraryCreationCompletionCode.Success;
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error renaming the newly created spectral library file", ex);
                completionCode = LibraryCreationCompletionCode.FileRenameError;
                return false;
            }
        }

        /// <summary>
        /// Contact the database to determine if an existing spectral library exists, or if a new one needs to be created
        /// </summary>
        /// <param name="spectralLibraryID">Spectral library ID</param>
        /// <param name="completionCode">CompletionCode</param>
        /// <returns>FileInfo instance for the remote spectral library file (the file will not exist if a new library); null if an error</returns>
        private bool SetSpectralLibraryCreateTaskComplete(int spectralLibraryID, LibraryCreationCompletionCode completionCode)
        {
            const string SP_NAME_SET_CREATE_TASK_COMPLETE = "set_spectral_library_create_task_complete";

            try
            {
                // SQL Server: Data Source=Gigasax;Initial Catalog=DMS5
                // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
                var dmsConnectionString = mMgrParams.GetParam("ConnectionString");

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(dmsConnectionString, mMgrName);

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
                RegisterEvents(dbTools);

                var cmd = dbTools.CreateCommand(SP_NAME_SET_CREATE_TASK_COMPLETE, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@libraryId", SqlType.Int).Value = spectralLibraryID;
                dbTools.AddParameter(cmd, "@completionCode", SqlType.Int).Value = (int)completionCode;

                var messageParam = dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 255, ParameterDirection.InputOutput);
                var returnCodeParam = dbTools.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.InputOutput);

                messageParam.Value = string.Empty;
                returnCodeParam.Value = string.Empty;

                // Call the procedure
                var resCode = dbTools.ExecuteSP(cmd, out var errorMessage);

                var returnCode = DBToolsBase.GetReturnCode(returnCodeParam);

                if (resCode == 0 && returnCode == 0)
                {
                    return true;
                }

                if (resCode != 0 && returnCode == 0)
                {
                    LogError("ExecuteSP() reported result code {0} calling {1}",
                        resCode, SP_NAME_SET_CREATE_TASK_COMPLETE);

                    return false;
                }

                LogError(
                    "Procedure {0} returned error code {1}{2}",
                    SP_NAME_SET_CREATE_TASK_COMPLETE, returnCodeParam.Value.CastDBVal<string>(),
                    string.IsNullOrWhiteSpace(errorMessage)
                        ? string.Empty
                        : ": " + errorMessage);

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error calling " + SP_NAME_SET_CREATE_TASK_COMPLETE, ex);
                return false;
            }
        }

        private CloseOutType StartDiaNN(
            FileSystemInfo fastaFile,
            FileSystemInfo spectralLibraryFile,
            out LibraryCreationCompletionCode completionCode)
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

                if (dataPackageInfo.DatasetFiles.Count == 0 && !mBuildingSpectralLibrary)
                {
                    LogError("No datasets were found (dataPackageInfo.DatasetFiles is empty)");
                    completionCode = LibraryCreationCompletionCode.UnknownError;
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                mDatasetCount = dataPackageInfo.DatasetFiles.Count;

                // Load the parameter file
                var paramFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);

                var paramFile = new FileInfo(Path.Combine(mWorkDir, paramFileName));

                var options = new DiaNNOptions();
                RegisterEvents(options);

                if (!options.LoadDiaNNOptions(paramFile.FullName))
                {
                    completionCode = LibraryCreationCompletionCode.ParameterFileError;
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(mWorkDir, mDiaNNConsoleOutputFile)
                };
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                LogMessage("Running DIA-NN");
                mProgress = (int)ProgressPercentValues.StartingDiaNN;

                // Set up and execute a program runner to run DIA-NN

                var processingSuccess = StartDiaNN(options, fastaFile, spectralLibraryFile, dataPackageInfo);

                if (!mToolVersionWritten)
                {
                    if (string.IsNullOrWhiteSpace(mDiaNNVersion))
                    {
                        ParseDiaNNConsoleOutputFile();
                    }
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                var averageFreeMemoryInfo = string.Format("Average recent free memory: {0:0.0} MB", mStatusTools.GetAverageRecentFreeMemoryMB(3));
                var freeMemoryLogged = false;

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError("{0}; {1}", mConsoleOutputErrorMsg, averageFreeMemoryInfo);
                    freeMemoryLogged = true;
                }

                if (!processingSuccess)
                {
                    if (freeMemoryLogged)
                    {
                        LogError("Error running DIA-NN");
                    }
                    else
                    {
                        LogError("{0}; {1}", "Error running DIA-NN", averageFreeMemoryInfo);
                    }

                    if (mCmdRunner.ExitCode != 0)
                    {
                        LogWarning("DIA-NN returned a non-zero exit code: " + mCmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to DIA-NN failed (but exit code is 0)");
                    }

                    completionCode = LibraryCreationCompletionCode.DiaNNError;
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Verify that the expected output file(s) were created

                bool validResults;
                bool emptyResultsFile;

                if (mBuildingSpectralLibrary)
                {
                    validResults = ValidateSpectralLibraryOutputFile(spectralLibraryFile, out completionCode);
                    emptyResultsFile = !validResults;
                }
                else
                {
                    validResults = ValidateSearchResultFiles(out emptyResultsFile);

                    if (emptyResultsFile)
                    {
                        completionCode = LibraryCreationCompletionCode.NoFilterPassingResults;
                        LogError("No filter-passing results");
                    }
                    else
                    {
                        completionCode = LibraryCreationCompletionCode.Success;
                    }
                }

                mStatusTools.UpdateAndWrite(mProgress);
                LogDebug("DIA-NN Search Complete", mDebugLevel);

                if (emptyResultsFile)
                    return CloseOutType.CLOSEOUT_NO_DATA;

                return validResults ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error in StartDiaNN", ex);

                completionCode = LibraryCreationCompletionCode.UnknownError;
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool StartDiaNN(
            DiaNNOptions options,
            FileSystemInfo fastaFile,
            FileSystemInfo spectralLibraryFile,
            DataPackageInfo dataPackageInfo)
        {
            var defaultThreadCount = GetNumThreadsToUse();

            var numThreadsToUse = options.ThreadCount > 0
                ? Math.Min(options.ThreadCount, defaultThreadCount)
                : defaultThreadCount;

            var arguments = new StringBuilder();

            if (mBuildingSpectralLibrary)
            {
                // Example command line arguments to have DIA-NN create a spectral library using an in-silico digest of a FASTA file

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

                AppendAdditionalArguments(options, arguments);
            }
            else
            {
                // Example command line arguments for using an existing spectral library to search DIA spectra

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

                switch (dataPackageInfo.DatasetFiles.Count)
                {
                    case 0:
                        LogError("DatasetFiles list in dataPackageInfo is empty");
                        return false;

                    case > 1:
                        // One way to run DIA-NN is by listing each .mzML file to process
                        // Since the Windows command line is limited to 8191 characters, if there are too many datasets, we'll use the "--dir" switch instead

                        // Construct the list of dataset files to append

                        var inputFileList = new StringBuilder();

                        foreach (var item in dataPackageInfo.DatasetFiles)
                        {
                            inputFileList.AppendFormat(" --f \"{0}\"", Path.Combine(mWorkDir, item.Value));
                        }

                        if (inputFileList.Length < 7000)
                        {
                            arguments.Append(inputFileList);
                        }
                        else
                        {
                            arguments.AppendFormat(" --dir {0}", mWorkDir);
                        }

                        break;

                    default:
                        // Processing a single .mzML file
                        arguments.AppendFormat(" --f \"{0}\"", Path.Combine(mWorkDir, dataPackageInfo.DatasetFiles.First().Value));
                        break;
                }

                arguments.AppendFormat(" --lib {0}", spectralLibraryFile.FullName);
                arguments.AppendFormat(" --threads {0}", numThreadsToUse);
                arguments.AppendFormat(" --verbose {0}", 2);
                arguments.AppendFormat(" --out {0}", GetDiannResultsFilePath("report.tsv").FullName);
                arguments.AppendFormat(" --qvalue {0}", options.PrecursorQValue);

                if (options.CreateQuantitiesMatrices)
                    arguments.Append(" --matrices");

                if (options.CreateExtractedChromatograms)
                    arguments.Append(" --xic");

                arguments.AppendFormat(" --temp {0}", mWorkDir);
                arguments.AppendFormat(" --out-lib {0}", GetDiannResultsFilePath("report-lib.tsv").FullName);
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

                if (options.NoPeptidoforms)
                    arguments.AppendFormat(" --no-peptidoforms");

                switch (options.ProteinInferenceMode)
                {
                    case ProteinInferenceModes.IsoformIDs:
                        arguments.AppendFormat(" --pg-level 0");
                        break;

                    case ProteinInferenceModes.ProteinNames:
                        arguments.AppendFormat(" --pg-level 1");
                        break;

                    case ProteinInferenceModes.Genes:
                        arguments.AppendFormat(" --pg-level 2");
                        break;

                    default:
                    case ProteinInferenceModes.Off:
                        arguments.AppendFormat(" --no-prot-inf");
                        break;
                }

                if (options.SpeciesGenes)
                    arguments.AppendFormat(" --species-genes");

                switch (options.QuantificationStrategy)
                {
                    case QuantificationAlgorithms.Legacy:
                        arguments.AppendFormat(" --direct-quant");
                        break;

                    case QuantificationAlgorithms.HighAccuracy:
                        arguments.AppendFormat(" --high-acc");
                        break;

                    default:
                    case QuantificationAlgorithms.HighPrecision:
                        break;
                }

                switch (options.CrossRunNormalization)
                {
                    case CrossRunNormalizationModes.Global:
                        arguments.AppendFormat(" --global-norm");
                        break;

                    case CrossRunNormalizationModes.Off:
                        arguments.AppendFormat(" --no-norm");
                        break;

                    default:
                    case CrossRunNormalizationModes.RTDependent:
                        break;
                }

                AppendAdditionalArguments(options, arguments);
            }

            LogDebug(mDiaNNProgLoc + " " + arguments);

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            return mCmdRunner.RunProgram(mDiaNNProgLoc, arguments.ToString(), "DIA-NN", true);
        }

        /// <summary>
        /// Store the DIA-NN version
        /// </summary>
        /// <remarks>
        /// Example values for dataLine:
        /// DIA-NN 1.8.1 (Data-Independent Acquisition by Neural Networks)
        /// DIA-NN 1.9.1 (Data-Independent Acquisition by Neural Networks)
        /// </remarks>
        /// <param name="dataLine"></param>
        private void StoreDiaNNVersion(string dataLine)
        {
            var charIndex = dataLine.IndexOf('(');

            mDiaNNVersion = charIndex > 0 ? dataLine.Substring(0, charIndex).Trim() : dataLine;

            LogDebug(mDiaNNVersion, mDebugLevel);
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

        private bool ValidateSearchResultFiles(out bool emptyResultsFile)
        {
            var reportFile = GetDiannResultsFilePath("report.tsv");
            var reportStatsFile = GetDiannResultsFilePath("report.stats.tsv");
            var reportPdfFile = GetDiannResultsFilePath("report.pdf");

            if (!reportFile.Exists)
            {
                // report.tsv file not created by DIA-NN
                LogError(string.Format("{0} file not created by DIA-NN", reportFile.Name));
                emptyResultsFile = false;
                return false;
            }

            if (!AnalysisResources.ValidateFileHasData(reportFile.FullName, "DIA-NN report.tsv", out var errorMessage, -1, true))
            {
                LogWarning(errorMessage);
                emptyResultsFile = true;
                return true;
            }

            emptyResultsFile = false;

            // Use the DIA-NN plotter to create the PDF report
            var validResults = GeneratePdfReport(reportFile, reportStatsFile, reportPdfFile);

            if (!reportPdfFile.Exists)
            {
                // report.pdf file not created by DIA-NN-plotter
                LogWarning("{0} file not created by DIA-NN-plotter", reportPdfFile.Name);
            }

            if (!reportStatsFile.Exists)
            {
                // report.stats.tsv file not created by DIA-NN
                LogWarning("{0} file not created by DIA-NN", reportStatsFile.Name);
            }

            return validResults;
        }

        private bool ValidateSpectralLibraryOutputFile(
            FileSystemInfo spectralLibraryFile,
            out LibraryCreationCompletionCode completionCode)
        {
            // DIA-NN v1.8 creates a spectral library named lib.predicted.speclib
            // DIA-NN v1.9 creates a spectral library named report-lib.predicted.speclib

            var specLib2023 = new FileInfo(Path.Combine(mWorkDir, "lib.predicted.speclib"));
            var specLib = new FileInfo(Path.Combine(mWorkDir, "report-lib.predicted.speclib"));

            if (specLib.Exists)
            {
                return RenameSpectralLibraryFile(specLib, spectralLibraryFile, out completionCode);
            }

            if (specLib2023.Exists)
            {
                return RenameSpectralLibraryFile(specLib2023, spectralLibraryFile, out completionCode);
            }

            // ReSharper disable CommentTypo

            // Look for any files with extension .speclib

            var workingDirectory = new DirectoryInfo(mWorkDir);

            var specLibFiles = workingDirectory.GetFiles("*.speclib");

            // ReSharper disable once ConvertIfStatementToSwitchStatement
            if (specLibFiles.Length < 1)
            {
                // report-lib.predicted.speclib file not created by DIA-NN
                LogError(string.Format("{0} file not created by DIA-NN", specLib.Name));
                completionCode = LibraryCreationCompletionCode.LibraryNotCreated;
                return false;
            }

            // ReSharper restore CommentTypo

            if (specLibFiles.Length > 1)
            {
                // ReSharper disable once StringLiteralTypo
                LogError(string.Format("{0} file not created by DIA-NN; instead, multiple .speclib files were created", specLib.Name));
                completionCode = LibraryCreationCompletionCode.MultipleLibrariesCreated;
                return false;
            }

            // report-lib.predicted.speclib file not created by DIA-NN, but found file lib.predicted.speclib instead
            LogWarning("{0} file not created by DIA-NN, but found file {1} instead", specLib.Name, specLibFiles[0]);

            return RenameSpectralLibraryFile(specLibFiles[0], spectralLibraryFile, out completionCode);
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

            ParseDiaNNConsoleOutputFile();

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mDiaNNVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("DIA-NN");
        }
    }
}
