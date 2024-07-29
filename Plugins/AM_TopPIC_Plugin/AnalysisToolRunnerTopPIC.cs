//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using CsvHelper;
using CsvHelper.Configuration;

namespace AnalysisManagerTopPICPlugIn
{
    /// <summary>
    /// Class for running TopPIC analysis
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerTopPIC : AnalysisToolRunnerBase
    {
        // Ignore Spelling: cmd, Csv, fasta, html, json, msalign, num, proteoform, proteoforms, prsm, ptm, toppic, Unimod

        private const string TOPPIC_CONSOLE_OUTPUT = "TopPIC_ConsoleOutput.txt";
        private const string TOPPIC_EXE_NAME = "toppic.exe";

        private const int PROGRESS_PCT_STARTING = 1;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private const string PRSM_TSV_OUTPUT_TABLE_NAME_SUFFIX_ORIGINAL = "_ms2.OUTPUT_TABLE";
        private const string PRSM_CSV_RESULT_TABLE_NAME_SUFFIX_ORIGINAL = "_ms2_toppic_prsm.csv";
        private const string PRSM_TSV_RESULT_TABLE_NAME_SUFFIX_ORIGINAL = "_ms2_toppic_prsm.tsv";
        private const string PRSM_RESULT_TABLE_NAME_SUFFIX_FINAL = "_TopPIC_PrSMs.txt";

        private const string PROTEOFORM_TSV_OUTPUT_TABLE_NAME_SUFFIX_ORIGINAL = "_ms2.FORM_OUTPUT_TABLE";
        private const string PROTEOFORM_CSV_RESULT_TABLE_NAME_SUFFIX_ORIGINAL = "_ms2_toppic_proteoform.csv";
        private const string PROTEOFORM_TSV_RESULT_TABLE_NAME_SUFFIX_ORIGINAL = "_ms2_toppic_proteoform.tsv";
        private const string PROTEOFORM_RESULT_TABLE_NAME_SUFFIX_FINAL = "_TopPIC_Proteoforms.txt";

        private bool mToolVersionWritten;

        /// <summary>
        /// This will initially be 1.3 or 1.7, indicating the version of .exe that should be used
        /// </summary>
        /// <remarks>
        /// After TopPIC starts, we'll update this variable with the tool version reported to the console
        /// </remarks>
        private Version mTopPICVersion;

        private string mTopPICVersionText;

        private string mTopPICProgLoc;

        private string mConsoleOutputErrorMsg;

        private int mMsAlignFileCount;

        private string mValidatedFASTAFilePath;

        private DateTime mLastConsoleOutputParse;

        private RunDosProgram mCmdRunner;

        /// <summary>
        /// Runs TopPIC tool
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
                    LogDebug("AnalysisToolRunnerTopPIC.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to TopPIC
                mTopPICProgLoc = DetermineProgramLocation("TopPICProgLoc", TOPPIC_EXE_NAME, out var specificStepToolVersion);

                if (string.IsNullOrWhiteSpace(mTopPICProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (specificStepToolVersion.StartsWith("v1.3"))
                {
                    mTopPICVersion = new Version(1, 3);
                }
                else
                {
                    // We're probably running TopPIC v1.7 (or newer)
                    mTopPICVersion = new Version(1, 7);
                }

                // Store the TopPIC version info in the database after the first line is written to file TopPIC_ConsoleOutput.txt
                mToolVersionWritten = false;
                mTopPICVersionText = string.Empty;
                mConsoleOutputErrorMsg = string.Empty;

                if (!ValidateFastaFile(out var fastaFileIsDecoy))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Process the _ms2.msalign file(s) using TopPIC
                var processingResult = StartTopPIC(fastaFileIsDecoy, mTopPICProgLoc);

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                Global.IdleLoop(0.5);
                PRISM.AppUtils.GarbageCollectNow();

                // Trim the console output file to remove the majority of the "processing" messages
                TrimConsoleOutputFile(Path.Combine(mWorkDir, TOPPIC_CONSOLE_OUTPUT));

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
                LogError("Error in TopPICPlugin->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private FileInfo AppendSuffixToBaseName(FileInfo sourceFile, string suffix)
        {
            var baseName = Path.GetFileNameWithoutExtension(sourceFile.Name);
            var extension = Path.GetExtension(sourceFile.Name);

            var updatedName = string.Format("{0}{1}{2}", baseName, suffix, extension);

            return sourceFile.Directory == null
                ? new FileInfo(updatedName)
                : new FileInfo(Path.Combine(sourceFile.Directory.FullName, updatedName));
        }

        private static float ComputeOverallProgress(
            SortedList<string, int> processingSteps,
            int currentProgress,
            int currentTaskItemsProcessed,
            int currentTaskTotalItems,
            bool progressReportedAsPercentComplete,
            float percentCompleteThisTask)
        {
            if (progressReportedAsPercentComplete)
            {
                // Convert % complete for this step into a pseudo item count by multiplying by 100
                currentTaskItemsProcessed = (int)Math.Round(percentCompleteThisTask * 100, 0);
                currentTaskTotalItems = 100 * 100;
            }

            var nextProgress = 100;

            // Find the % progress value for step following the current step
            foreach (var item in processingSteps)
            {
                if (item.Value > currentProgress && item.Value < nextProgress)
                    nextProgress = item.Value;
            }

            return ComputeIncrementalProgress(currentProgress, nextProgress,
                currentTaskItemsProcessed, currentTaskTotalItems);
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileToSkip(Dataset + AnalysisResources.DOT_MZML_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory();
        }

        private bool ExamineConsoleItemNumberOrProgress(
            string dataLine,
            ref float lastItemNumberOrProgress,
            float itemNumberOrProgress,
            ref float itemNumberOrProgressOutputThreshold,
            int outputThresholdIncrement,
            out string lastProcessingLine)
        {
            if (itemNumberOrProgress < lastItemNumberOrProgress)
            {
                // We have entered a new processing mode; reset the threshold
                itemNumberOrProgressOutputThreshold = 0;
            }

            lastItemNumberOrProgress = itemNumberOrProgress;

            if (itemNumberOrProgress < itemNumberOrProgressOutputThreshold)
            {
                lastProcessingLine = dataLine;
                return false;
            }

            // Write out this line and bump up itemNumberOutputThreshold by the given increment
            itemNumberOrProgressOutputThreshold += outputThresholdIncrement;
            lastProcessingLine = string.Empty;
            return true;
        }

        /// <summary>
        /// Returns a dictionary mapping parameter names to argument names
        /// </summary>
        /// <param name="useSeparateErrorTolerances"></param>
        private Dictionary<string, string> GetTopPICParameterNames(bool useSeparateErrorTolerances)
        {
            var paramToArgMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                {"MaxShift", "max-shift"},
                {"MinShift", "min-shift"},
                {"NumShift", "num-shift"},
                {"SpectrumCutoffType", "spectrum-cutoff-type"},
                {"SpectrumCutoffValue", "spectrum-cutoff-value"},
                {"ProteoformCutoffType", "proteoform-cutoff-type"},
                {"ProteoformCutoffValue", "proteoform-cutoff-value"},
                {"Decoy", "decoy"},
                {"NTerminalProteinForms", "n-terminal-form"},
                {"KeepTempFiles", "keep-temp-files"},
                {"DisableHtmlOutput", "skip-html-folder"}
            };

            if (useSeparateErrorTolerances)
            {
                // TopPIC 1.3 renamed the error tolerance parameter to --mass-error-tolerance
                paramToArgMapping.Add("ErrorTolerance", "mass-error-tolerance");
                paramToArgMapping.Add("ProteoformErrorTolerance", "proteoform-error-tolerance");
            }
            else
            {
                // TopPIC 1.2 and early used --error-tolerance
                paramToArgMapping.Add("ErrorTolerance", "error-tolerance");
            }

            return paramToArgMapping;
        }

        /// <summary>
        /// Parse the TopPIC console output file to determine the TopPIC version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // ReSharper disable CommentTypo

            // Example Console output for version 1.7 and later

            // toppic.exe --mass-error-tolerance 15 --proteoform-error-tolerance 0.8 --max-shift 500 --min-shift -500 --num-shift 1 --spectrum-cutoff-type FDR --spectrum-cutoff-value 0.01 --proteoform-cutoff-type FDR --proteoform-cutoff-value 0.01 --activation=FILE --thread-number 3 --decoy --n-terminal-form NONE,NME,NME_ACETYLATION,M_ACETYLATION --variable-ptm-num 2 --local-ptm-file-name C:\DMS_WorkDir\TopPIC_Dynamic_Mods.txt C:\DMS_Temp_Org\ID_008379_7A4C32B7.fasta DatasetName_ms2.msalign
            // --------------------------------------------------------------------------------
            // Total thread number: 16
            // Total memory: 31.69 GiB
            // Available memory: 29.69 GiB
            //
            // TopPIC 1.7.0
            // ********************** Parameters **********************
            // Protein database file:                         C:\DMS_Temp_Org\ID_008379_7A4C32B7.fasta
            // Spectrum file:                                 MRC5_229E_16h_5_28Nov23_Aragorn_BMEB2_21-12-03_ms2.msalign
            // Number of combined spectra:                    1
            // Fragmentation method:                          FILE
            // Search type:                                   TARGET+DECOY
            // Allowed N-terminal forms:                      NONE,NME,NME_ACETYLATION,M_ACETYLATION
            // Maximum number of variable modifications:      3
            // Use approximate spectra in protein filtering:  false
            // Variable modifications file name:              C:\DMS_WorkDir\TopPIC_Dynamic_Mods.txt
            // Variable modifications BEGIN
            // Acetyl                                         42.010565 K
            // CTrmAmid                                       -0.984016 ARNDCEQGHILKMFPSTWYV
            // Carbamyl                                       43.005814 K
            // Carbamyl_N                                     43.005814 ARNDCEQGHILKMFPSTWYV
            // Deamide                                        0.984016 QN
            // Dimethyl                                       28.031300 KR
            // IronAdduct                                     52.911464 DE
            // Methyl                                         14.015650 KR
            // Phosph                                         79.966331 STY
            // NH3_Loss                                       -17.026549 Q
            // Plus1Oxy                                       15.994915 CMW
            // Plus2Oxy                                       31.989829 CMW
            // Trimethyl                                      42.046950 KR
            // Nethylmaleimide                                125.047679 C
            // Glutathione                                    305.068156 C
            // Variable modifications END
            // Maximum number of unexpected modifications:    1
            // Maximum mass shift of modifications:           500 Da
            // Minimum mass shift of modifications:           -500 Da
            // Spectrum-level cutoff type:                    FDR
            // Spectrum-level cutoff value:                   0.01
            // Proteoform-level cutoff type:                  FDR
            // Proteoform-level cutoff value:                 0.01
            // Error tolerance for matching masses:           15 ppm
            // Error tolerance for identifying PrSM clusters: 0.8 Da
            // Use TopFD feature file:                        True
            // E-value computation:                           Generating function
            // Localization with MIScore:                     False
            // Thread number:                                 3
            // Executable file directory:                     C:\DMS_Programs\TopPIC
            // Start time:                                    Thu Dec 21 13:53:12 2023
            // Version:                                       1.7.0
            // ********************** Parameters **********************
            // Zero unexpected shift filtering - started.

            // Example Console output for version 1.5.4 and earlier

            // toppic.exe --mass-error-tolerance 15 --proteoform-error-tolerance 0.8 --max-shift 500 --min-shift -500 --num-shift 1 --spectrum-cutoff-type FDR --spectrum-cutoff-value 0.01 --proteoform-cutoff-type FDR --proteoform-cutoff-value 0.01 --activation=FILE --thread-number 14 --decoy --n-terminal-form NONE,NME,NME_ACETYLATION,M_ACETYLATION --mod-file-name E:\DMS_WorkDir5\TopPIC_Dynamic_Mods.txt E:\DMS_Temp_Org\ID_008379_7A4C32B7.fasta DatasetName_ms2.msalign
            // --------------------------------------------------------------------------------
            // TopPIC 1.5.4
            // ********************** Parameters **********************
            // Protein database file:                      	E:\DMS_Temp_Org\ID_008379_7A4C32B7.fasta
            // Spectrum file:                              	MRC5_229E_16h_5_28Nov23_Aragorn_BMEB2_21-12-03_ms2.msalign
            // Number of combined spectra:                 	1
            // Fragmentation method:                       	FILE
            // Search type:                                	TARGET+DECOY
            // Use TopFD feature file:                     	True
            // Maximum number of unexpected modifications: 	1
            // Error tolerance for matching masses:        	15 ppm
            // Error tolerance for identifying PrSM clusters: 	0.8 Da
            // Spectrum-level cutoff type:                 	FDR
            // Spectrum-level cutoff value:                	0.01
            // Proteoform-level cutoff type:               	FDR
            // Proteoform-level cutoff value:              	0.01
            // Allowed N-terminal forms:                   	NONE,NME,NME_ACETYLATION,M_ACETYLATION
            // Maximum mass shift of modifications:        	500 Da
            // Minimum mass shift of modifications:        	-500 Da
            // Thread number:                              	14
            // E-value computation:                        	Generating function
            // Common modification file name:              	E:\DMS_WorkDir5\TopPIC_Dynamic_Mods.txt
            // PTMs for MIScore BEGIN
            // Acetyl                                      	42.010565	K
            // CTrmAmid                                    	-0.984016	ARNDCEQGHILKMFPSTWYV
            // Carbamyl                                    	43.005814	K
            // Carbamyl_N                                  	43.005814	ARNDCEQGHILKMFPSTWYV
            // Deamide                                     	0.984016	QN
            // Dimethyl                                    	28.031300	KR
            // IronAdduct                                  	52.911464	DE
            // Methyl                                      	14.015650	KR
            // Phosph                                      	79.966331	STY
            // NH3_Loss                                    	-17.026549	Q
            // Plus1Oxy                                    	15.994915	CMW
            // Plus2Oxy                                    	31.989829	CMW
            // Trimethyl                                   	42.046950	KR
            // Nethylmaleimide                             	125.047679	C
            // Glutathione                                 	305.068156	C
            // PTMs for MIScore END
            // MIScore threshold:                          	0.15
            // Executable file directory:                  	C:\DMS_Programs\TopPIC
            // Start time:                                 	Thu Nov 30 10:20:53 2023
            // Version:                                    	1.5.4
            // ********************** Parameters **********************
            // Non PTM filtering - started.
            // Non PTM filtering - block 1 out of 3 started.
            // Non PTM filtering - processing 1504 of 1504 spectra.
            // Non PTM filtering - block 1 finished.
            // Non PTM filtering - block 2 out of 3 started.
            // Non PTM filtering - processing 1504 of 1504 spectra.
            // Non PTM filtering - block 2 finished.
            // Non PTM filtering - block 3 out of 3 started.
            // Non PTM filtering - processing 1504 of 1504 spectra.
            // Non PTM filtering - block 3 finished.
            // Non PTM filtering - combining blocks started.
            // Non PTM filtering - combining blocks finished.
            // Non PTM filtering - finished.
            // Non PTM search - started.
            // Non PTM search - processing 1504 of 1504 spectra.
            // Non PTM search - finished.
            // One PTM filtering - started.
            // One PTM filtering - block 1 out of 3 started.
            // One PTM filtering - processing 1504 of 1504 spectra.
            // One PTM filtering - block 1 finished.
            // ...
            // One PTM filtering - finished.
            // One PTM search - started.
            // One PTM search - processing 1504 of 1504 spectra.
            // One PTM search - finished.
            // Diagonal PTM filtering - started.
            // Diagonal filtering - block 1 out of 3 started.
            // ...
            // Diagonal filtering - finished.
            // Two PTM search - started.
            // PTM search - processing 1504 of 1504 spectra.
            // Two PTM search - finished.
            // Combining PRSMs - started.
            // Combining PRSMs - finished.
            // E-value computation - started.
            // E-value computation - processing 1504 of 1504 spectra.
            // E-value computation - finished.
            // Finding PrSM clusters - started.
            // Finding PrSM clusters - finished.
            // Top PRSM selecting - started
            // Top PRSM selecting - finished.
            // FDR computation - started.
            // FDR computation - finished.
            // PrSM filtering by EVALUE - started.
            // PrSM filtering by EVALUE - finished.
            // Outputting PrSM table - started.
            // Outputting PrSM table - finished.
            // Generating PRSM xml files - started.
            // Generating xml files - processing 676 PrSMs.
            // Generating xml files - preprocessing 466 Proteoforms.
            // Generating xml files - processing 466 Proteoforms.
            // Generating xml files - preprocessing 110 Proteins.
            // Generating xml files - processing 110 Proteins.
            // Generating PRSM xml files - finished.
            // Converting PRSM xml files to html files - started.
            // Converting xml files to html files - processing 1253 of 1253 files.
            // Converting PRSM xml files to html files - finished.
            // PrSM filtering by EVALUE - started.
            // PrSM filtering by EVALUE - finished.
            // Selecting top PrSMs for proteoforms - started.
            // Selecting top PrSMs for proteoforms - finished.
            // Outputting proteoform table - started.
            // Outputting proteoform table - finished.
            // Generating proteoform xml files - started.
            // Generating xml files - processing 676 PrSMs.
            // ...
            // Generating proteoform xml files - finished.
            // Converting proteoform xml files to html files - started.
            // Converting xml files to html files - processing 1253 of 1253 files.
            // Converting proteoform xml files to html files - finished.
            // Deleting temporary files - started.
            // Deleting temporary files - finished.
            // TopPIC finished.

            // TopPIC version 1.3.1 reports percent complete values in some sections; for example:
            // Non PTM filtering - started.
            // Non PTM filtering - processing 0.01%.
            // Non PTM filtering - processing 9.02%.
            // Non PTM filtering - processing 25%.

            // ReSharper restore CommentTypo

            var processingSteps = new SortedList<string, int>
            {
                {"Non PTM filtering", 0},                   // v1.5
                {"Zero unexpected shift filtering", 0},     // v1.7
                {"Non PTM search", 10},                     // v1.5
                {"Zero unexpected shift search", 10},       // v1.7
                {"Variable PTM filtering", 18},             // v1.7
                {"Variable PTM search", 20},                // v1.7
                {"One PTM filtering", 30},                  // v1.5
                {"One unexpected shift filtering", 30},     // v1.7
                {"One PTM search", 32},                     // v1.5
                {"One unexpected shift search", 32},        // v1.7
                {"Diagonal filtering", 45},
                {"Two PTM search", 55},                     // v1.5
                {"Two unexpected shift search", 55},        // v1.7
                {"Combining PRSMs", 70},
                {"Merging PRSMs", 70},
                // The space after "computation" in "E-value computation " is important to avoid matching "E-value computation:" in the parameters block
                {"E-value computation ", 75},
                {"Finding PrSM clusters", 89},
                {"Generating PrSM XML files", 90},
                {"Converting PrSM XML files to HTML files", 93},
                {"Converting PrSM XML files to JSON files", 93},
                {"Generating proteoform XML files", 95},
                {"Converting proteoform XML files to HTML files", 98},
                {"Deleting temporary files", 99},
                {"TopPIC finished", 100}
            };

            // RegEx to match lines like:
            // E-value computation - processing 560 of 2782 spectra.
            var incrementalProgressMatcher = new Regex(@"processing (?<Item>\d+) of (?<Total>\d+) [a-z]+",
                                                       RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // RegEx to match lines like:
            // Generating xml files - processing 398 PrSMs.
            var undefinedProgressMatcher = new Regex(@"\bprocessing (?<Item>\d+) [a-z ]+\.",
                                                       RegexOptions.Compiled | RegexOptions.IgnoreCase);
            // RegEx to match lines like:
            // Non PTM filtering - processing 9.03%.
            var percentCompleteMatcher = new Regex("\bprocessing (?<Progress>[0-9.]+)%", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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
                var currentTaskItemsProcessed = 0;
                var currentTaskTotalItems = 0;
                var undefinedProgress = false;

                float percentCompleteThisTask = 0;
                var progressReportedAsPercentComplete = false;

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var linesRead = 0;
                var msAlignFileNumber = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var dataLineLCase = dataLine.ToLower();

                    if (linesRead <= 3 || linesRead <= 8 && string.IsNullOrEmpty(mTopPICVersionText))
                    {
                        // The first line has the path to the TopPIC executable and the command line arguments
                        // The second line is dashes
                        // The third line has the TopPIC version
                        if (string.IsNullOrEmpty(mTopPICVersionText) &&
                            dataLine.IndexOf("TopPIC", StringComparison.OrdinalIgnoreCase) == 0 &&
                            dataLine.IndexOf(TOPPIC_EXE_NAME, StringComparison.OrdinalIgnoreCase) < 0)
                        {
                            if (mDebugLevel >= 2)
                            {
                                LogDebug("TopPIC version: " + dataLine);
                            }

                            mTopPICVersionText = dataLine;

                            var versionMatcher = new Regex(@"(?<Major>\d+)\.(?<Minor>\d+)\.(?<Build>\d+)", RegexOptions.Compiled);
                            var match = versionMatcher.Match(dataLine);

                            if (!match.Success)
                            {
                                continue;
                            }

                            mTopPICVersion = new Version(match.Value);

                            // ReSharper disable once InvertIf
                            if (mTopPICVersion.Equals(new Version(1, 4, 13)))
                            {
                                // Examine the date of the executable
                                // The beta version released 2021-08-20 has updated code, but the version is unchanged

                                var exeInfo = new FileInfo(mTopPICProgLoc);

                                if (exeInfo.LastWriteTime > new DateTime(2021, 8, 19))
                                {
                                    mTopPICVersion = new Version(1, 4, 13, 1);
                                    mTopPICVersionText = "TopPIC 1.4.13.1 beta";
                                }
                            }
                        }
                    }
                    else
                    {
                        foreach (var processingStep in processingSteps)
                        {
                            if (!dataLine.StartsWith(processingStep.Key, StringComparison.OrdinalIgnoreCase))
                                continue;

                            // This line will appear once for each .msalign input file that we are processing (FAIMS datasets can have multiple .msalign files)

                            if (dataLine.StartsWith("Non PTM filtering - started"))
                                msAlignFileNumber++;

                            currentProgress = processingStep.Value;
                        }

                        if (dataLineLCase.Contains("error") &&
                            !dataLineLCase.Contains("error tolerance:") &&
                            !dataLineLCase.Contains("error tolerance for ") &&
                            string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                        {
                            mConsoleOutputErrorMsg = "Error running TopPIC: " + dataLine;
                        }

                        var percentCompleteMatch = percentCompleteMatcher.Match(dataLine);

                        if (percentCompleteMatch.Success)
                        {
                            percentCompleteThisTask = float.Parse(percentCompleteMatch.Groups["Progress"].Value);
                            progressReportedAsPercentComplete = true;
                            undefinedProgress = false;
                        }
                        else
                        {
                            progressReportedAsPercentComplete = false;

                            var progressMatch = incrementalProgressMatcher.Match(dataLine);

                            if (progressMatch.Success)
                            {
                                if (int.TryParse(progressMatch.Groups["Item"].Value, out var itemValue))
                                    currentTaskItemsProcessed = itemValue;

                                if (int.TryParse(progressMatch.Groups["Total"].Value, out var totalValue))
                                    currentTaskTotalItems = totalValue;

                                undefinedProgress = false;
                            }

                            var undefinedProgressMatch = undefinedProgressMatcher.Match(dataLine);

                            if (undefinedProgressMatch.Success)
                            {
                                undefinedProgress = true;
                            }
                        }
                    }
                }

                float effectiveProgress;

                if (!undefinedProgress && currentTaskItemsProcessed > 0 && currentTaskTotalItems > 0)
                {
                    effectiveProgress = ComputeOverallProgress(
                        processingSteps, currentProgress,
                        currentTaskItemsProcessed, currentTaskTotalItems,
                        progressReportedAsPercentComplete, percentCompleteThisTask);
                }
                else
                {
                    effectiveProgress = currentProgress;
                }

                if (mMsAlignFileCount <= 1 || msAlignFileNumber == 0)
                {
                    mProgress = effectiveProgress;
                }
                else
                {
                    mProgress = ComputeIncrementalProgress(msAlignFileNumber, mMsAlignFileCount, effectiveProgress);
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate(string.Format(
                        "Error parsing the TopPIC console output file ({0}): {1}",
                        consoleOutputFilePath, ex.Message));
                }
            }
        }

        /// <summary>
        /// Validate the static or dynamic mods defined in modList
        /// If valid mods are defined, write them to a text file and update cmdLineOptions
        /// </summary>
        /// <param name="cmdLineArguments">Command line arguments to pass to TopPIC</param>
        /// <param name="modList">List of static or dynamic mods</param>
        /// <param name="modDescription">Either "static" or "dynamic"</param>
        /// <param name="modsFileName">Filename that mods are written to</param>
        /// <param name="modArgumentSwitch">Argument name to append to cmdLineOptions along with the mod file name</param>
        /// <returns>True if success, false if an error</returns>
        private bool ParseTopPICModifications(
            StringBuilder cmdLineArguments,
            IReadOnlyCollection<string> modList,
            string modDescription,
            string modsFileName,
            string modArgumentSwitch)
        {
            try
            {
                var validatedMods = ValidateTopPICMods(modList, out var invalidMods);

                if (validatedMods.Count != modList.Count)
                {
                    LogError("One or more {0} mods failed validation: {1}", modDescription, string.Join(", ", invalidMods));
                    return false;
                }

                if (validatedMods.Count > 0)
                {
                    var modsFilePath = Path.Combine(mWorkDir, modsFileName);
                    var success = WriteModsFile(modsFilePath, validatedMods);

                    if (!success)
                        return false;

                    // Append --local-ptm-file-name ModsFilePath
                    // Prior to TopPIC v1.7 the argument name was "--mod-file-name", and even older versions used "--fixed-mod"

                    cmdLineArguments.AppendFormat(" --{0} {1} ", modArgumentSwitch, modsFilePath);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError(string.Format("Exception creating {0} mods file for TopPIC", modDescription), ex);
                return false;
            }
        }

        /// <summary>
        /// Read the TopPIC options file and convert the options to command line switches
        /// </summary>
        /// <param name="fastaFileIsDecoy">The plugin will set this to true if the FASTA file is a forward+reverse FASTA file</param>
        /// <param name="cmdLineArguments">Output: TopPIC command line arguments</param>
        /// <param name="htmlOutputDisabled">Output: True if the parameter file has DisableHtmlOutput=True</param>
        /// <returns>Options string if success; empty string if an error</returns>
        public CloseOutType ParseTopPICParameterFile(bool fastaFileIsDecoy, out StringBuilder cmdLineArguments, out bool htmlOutputDisabled)
        {
            const string STATIC_MODS_FILE_NAME = "TopPIC_Static_Mods.txt";
            const string DYNAMIC_MODS_FILE_NAME = "TopPIC_Dynamic_Mods.txt";

            cmdLineArguments = new StringBuilder();

            var parameterFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);

            var result = LoadSettingsFromKeyValueParameterFile("TopPIC", parameterFileName, out var paramFileEntries, out var paramFileReader);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                htmlOutputDisabled = false;
                return result;
            }

            var workDirectory = new DirectoryInfo(mWorkDir);

            // If file DatasetName_ms2.feature exists, assume that we are using TopPIC 1.3 and are thus using separate error tolerances
            // FAIMS datasets will have files named DatasetName_0_ms2.feature, DatasetName_1_ms2.feature, etc.
            var ms2FeatureFiles = workDirectory.GetFiles(string.Format("{0}*_ms2{1}", Dataset, AnalysisResourcesTopPIC.TOPFD_FEATURE_FILE_SUFFIX));

            var useSeparateErrorTolerances = ms2FeatureFiles.Length > 0;

            // Obtain the dictionary that maps parameter names to argument names
            var paramToArgMapping = GetTopPICParameterNames(useSeparateErrorTolerances);

            var paramNamesToSkip = new SortedSet<string>(StringComparer.OrdinalIgnoreCase) {
                "NumMods",
                "StaticMod",
                "DynamicMod",
                "NTerminalProteinForms",
                "Decoy",
                "KeepTempFiles",
                "DisableHtmlOutput"
            };

            cmdLineArguments.Append(paramFileReader.ConvertParamsToArgs(paramFileEntries, paramToArgMapping, paramNamesToSkip, "--"));

            htmlOutputDisabled = paramFileReader.ParamIsEnabled(paramFileEntries, "DisableHtmlOutput");

            if (cmdLineArguments.Length == 0)
            {
                mMessage = paramFileReader.ErrorMessage;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (htmlOutputDisabled)
            {
                var htmlDirectory = new DirectoryInfo(Path.Combine(mWorkDir, mDatasetName + "_html"));

                if (htmlDirectory.Exists && htmlDirectory.GetFileSystemInfos().Length == 0)
                {
                    // Remove the empty directory
                    htmlDirectory.Delete();
                }
            }

            // ReSharper disable CommentTypo

            // Instruct TopPIC to use the fragmentation method info tracked in the .mzML file
            // Other options for activation are CID, HCDCID, ETDCID, or UVPDCID
            cmdLineArguments.Append(" --activation=FILE");

            // ReSharper restore CommentTypo

            if (mTopPICVersion >= new Version(1, 4))
            {
                // Specify the number of threads to use
                // Allow TopPIC to use 88% of the physical cores
                var coreCount = Global.GetCoreCount();
                var threadsToUse = (int)Math.Floor(coreCount * 0.88);

                // Additionally, assume that each thread will use 3 GB of memory
                // Adjust the thread count lower if insufficient free memory
                var freeMemoryGB = Global.GetFreeMemoryMB() / 1024;

                while (threadsToUse > 1 && threadsToUse * 3 > freeMemoryGB)
                {
                    threadsToUse--;
                }

                LogMessage("The system has {0} cores and {1:F1} GB of free memory; TopPIC will use {2} threads", coreCount, freeMemoryGB, threadsToUse);

                cmdLineArguments.AppendFormat(" --thread-number {0}", threadsToUse);

                // Note that TopPIC will display a warning when it starts if it thinks the number of threads selected is too high,
                // given the amount of free system memory, for example:

                // WARNING: Based on the available memory size, up to 6 threads can be used.
                // Please set the thread number to 6 or the program may crash.
            }

            // Arguments referenced in the following for loop are appended as --decoy or --keep-temp-files and not as "--decoy true" or "--keep-temp-files true"
            // Append these if set to true in the parameter file

            foreach (var paramName in new List<string> { "Decoy", "KeepTempFiles", "DisableHtmlOutput" })
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
                    //   Parameter to argument mapping dictionary does not have Decoy
                    //   Parameter to argument mapping dictionary does not have KeepTempFiles
                    //   Parameter to argument mapping dictionary does not have DisableHtmlOutput

                    LogError(string.Format("Parameter to argument mapping dictionary does not have {0}", paramName));
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            var staticMods = new List<string>();
            var dynamicMods = new List<string>();

            // By default, allow up to 3 dynamic mods per peptide
            var maxDynamicMods = 3;

            try
            {
                foreach (var kvSetting in paramFileEntries)
                {
                    var paramValue = kvSetting.Value;

                    if (Global.IsMatch(kvSetting.Key, "NumMods"))
                    {
                        if (!string.IsNullOrWhiteSpace(paramValue) && int.TryParse(paramValue, out var numMods))
                        {
                            maxDynamicMods = numMods;
                        }
                    }
                    else if (Global.IsMatch(kvSetting.Key, "StaticMod"))
                    {
                        if (!string.IsNullOrWhiteSpace(paramValue) && !Global.IsMatch(paramValue, "none"))
                        {
                            staticMods.Add(paramValue);
                        }
                    }
                    else if (Global.IsMatch(kvSetting.Key, "DynamicMod"))
                    {
                        if (!string.IsNullOrWhiteSpace(paramValue) && !Global.IsMatch(paramValue, "none") && !Global.IsMatch(paramValue, "defaults"))
                        {
                            dynamicMods.Add(paramValue);
                        }
                    }
                    else if (Global.IsMatch(kvSetting.Key, "NTerminalProteinForms"))
                    {
                        if (string.IsNullOrWhiteSpace(paramValue))
                            continue;

                        // Assure the N-terminal protein forms list has no spaces
                        if (paramToArgMapping.TryGetValue(kvSetting.Key, out var argumentName))
                        {
                            cmdLineArguments.AppendFormat(" --{0} {1}", argumentName, kvSetting.Value.Replace(" ", string.Empty));
                        }
                        else
                        {
                            LogError("Parameter to argument mapping dictionary does not have NTerminalProteinForms");
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception extracting dynamic and static mod information from the TopPIC parameter file", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Create the static and dynamic modification file(s) if any static or dynamic mods are defined
            // Will also update cmdLineOptions to have --fixed-mod and/or --local-ptm-file-name

            // Prior to v1.7.x we used "--mod-file-name" instead of "--local-ptm-file-name"
            // When this method was first updated to v1.7, it used "--variable-ptm-file-name" for the mod file name, but the correct argument name to use is "--local-ptm-file-name"

            if (!ParseTopPICModifications(cmdLineArguments, staticMods, "static", STATIC_MODS_FILE_NAME, "fixed-mod"))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            string variableModsArgName;

            if (mTopPICVersion >= new Version(1, 7))
            {
                cmdLineArguments.AppendFormat(" --variable-ptm-num {0}", maxDynamicMods);
                variableModsArgName = "local-ptm-file-name";
            }
            else
            {
                variableModsArgName = "mod-file-name";
            }

            if (!ParseTopPICModifications(cmdLineArguments, dynamicMods, "dynamic", DYNAMIC_MODS_FILE_NAME, variableModsArgName))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // ReSharper disable once InvertIf
            if (paramFileReader.ParamIsEnabled(paramFileEntries, "Decoy") && fastaFileIsDecoy)
            {
                // TopPIC should be run with a forward=only protein collection; allow TopPIC to add the decoy proteins
                LogError("Parameter file / decoy protein collection conflict: do not use a decoy protein collection " +
                         "when using a parameter file with setting Decoy=True");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType StartTopPIC(bool fastaFileIsDecoy, string progLoc)
        {
            LogMessage("Running TopPIC");

            // Set up and execute a program runner to run TopPIC
            // By default uses just one core; limit the number of cores to 4 with "--thread-number 4"

            var result = ParseTopPICParameterFile(fastaFileIsDecoy, out var cmdLineArguments, out var htmlOutputDisabled);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            var workingDirectory = new DirectoryInfo(mWorkDir);

            var arguments = new StringBuilder();

            arguments.AppendFormat("{0} {1}", cmdLineArguments.ToString().Trim(), mValidatedFASTAFilePath);

            // Append the .msalign file(s)

            var msAlignFiles = workingDirectory.GetFiles(string.Format("{0}*{1}", mDatasetName, AnalysisResourcesTopPIC.MSALIGN_FILE_SUFFIX)).ToList();

            foreach (var msAlignFile in msAlignFiles)
            {
                arguments.AppendFormat(" {0}", msAlignFile.Name);
            }

            LogDebug(progLoc + " " + arguments);

            mMsAlignFileCount = msAlignFiles.Count;

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = Path.Combine(mWorkDir, TOPPIC_CONSOLE_OUTPUT)
            };

            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mProgress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var processingSuccess = mCmdRunner.RunProgram(progLoc, arguments.ToString(), "TopPIC", true);

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mTopPICVersionText))
                {
                    ParseConsoleOutputFile(Path.Combine(mWorkDir, TOPPIC_CONSOLE_OUTPUT));
                }

                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!processingSuccess)
            {
                LogError("Error running TopPIC");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("TopPIC returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to TopPIC failed (but exit code is 0)");
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Validate the results files and zip the HTML subdirectories
            // The HTML directories will not exist if the parameter file has "DisableHtmlOutput=True"
            var processingError = !ValidateAndZipResults(msAlignFiles, htmlOutputDisabled, out var noValidResults);

            if (processingError)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            mStatusTools.UpdateAndWrite(mProgress);

            if (mDebugLevel >= 3)
            {
                LogDebug("TopPIC Search Complete");
            }

            return noValidResults ? CloseOutType.CLOSEOUT_NO_DATA : CloseOutType.CLOSEOUT_SUCCESS;
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

            var toolVersionInfo = mTopPICVersionText;

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(mTopPICProgLoc)
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

        /// <summary>
        /// Reads the console output file and removes the majority of the "Processing" messages
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void TrimConsoleOutputFile(string consoleOutputFilePath)
        {
            // This RegEx matches lines of the form:

            // Zero PTM filtering - processing 100 of 4404 spectra.
            // One PTM search - processing 100 of 4404 spectra.
            // E-value computation - processing 100 of 4404 spectra.
            // Generating xml files - processing 100 PrSMs.
            // Generating xml files - processing 100 Proteoforms.
            // Generating xml files - processing 100 Proteins.
            // Converting xml files to html files - processing 100 of 2833 files.

            var extractItemWithCount = new Regex(@"processing +(?<Item>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // This RegEx matches lines of the form:

            // Non PTM filtering - processing 0.122%.
            // Non PTM filtering - processing 5.6%.

            var extractItemWithPercentComplete = new Regex("processing +(?<Progress>[0-9.]+)%", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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

                var trimmedFilePath = consoleOutputFilePath + ".trimmed";

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(trimmedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    float progressOutputThreshold = 0;
                    float itemNumberOutputThreshold = 0;

                    float lastProgress = 0;
                    float lastItemNumber = 0;
                    var lastProcessingLine = string.Empty;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            // Skip blank lines
                            continue;
                        }

                        var keepLine = true;
                        var regexMatched = false;

                        var percentCompleteMatch = extractItemWithPercentComplete.Match(dataLine);

                        if (percentCompleteMatch.Success)
                        {
                            regexMatched = true;

                            var percentComplete = float.Parse(percentCompleteMatch.Groups["Progress"].Value);

                            keepLine = ExamineConsoleItemNumberOrProgress(
                                dataLine,
                                ref lastProgress,
                                percentComplete,
                                ref progressOutputThreshold,
                                20,
                                out lastProcessingLine);
                        }
                        else
                        {
                            var match = extractItemWithCount.Match(dataLine);

                            if (match.Success)
                            {
                                regexMatched = true;

                                var itemNumber = int.Parse(match.Groups["Item"].Value);

                                keepLine = ExamineConsoleItemNumberOrProgress(
                                    dataLine,
                                    ref lastItemNumber,
                                    itemNumber,
                                    ref itemNumberOutputThreshold,
                                    250,
                                    out lastProcessingLine);
                            }
                        }

                        if (!regexMatched && !string.IsNullOrWhiteSpace(lastProcessingLine))
                        {
                            writer.WriteLine(lastProcessingLine);
                            lastProcessingLine = string.Empty;
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

        /// <summary>
        /// Validate the results files and zip the html subdirectories
        /// </summary>
        /// <remarks>
        /// TopPIC 1.1 created tab-delimited text files ending with "_ms2.OUTPUT_TABLE" and "_ms2.FORM_OUTPUT_TABLE"
        /// TopPIC 1.2 creates .csv files ending with "_ms2_toppic_prsm.csv" and "_ms2_toppic_proteoform.csv"
        /// </remarks>
        /// <param name="msAlignFiles">List of .msalign files</param>
        /// <param name="htmlOutputDisabled">True if the parameter file has DisableHtmlOutput=True</param>
        /// <param name="noValidResults">Output: true if no valid results were found</param>
        /// <returns>True if success, false if an error</returns>
        private bool ValidateAndZipResults(List<FileInfo> msAlignFiles, bool htmlOutputDisabled, out bool noValidResults)
        {
            noValidResults = false;

            try
            {
                // This list tracks the original results file names for the TopPIC output files
                var resultFileNames = new List<TopPICResultFileInfo>
                {
                    // TopPIC output file names prior to November 2018 (version 1.1.2)
                    new(mDatasetName, PRSM_TSV_OUTPUT_TABLE_NAME_SUFFIX_ORIGINAL, PROTEOFORM_TSV_OUTPUT_TABLE_NAME_SUFFIX_ORIGINAL),

                    // TopPIC output file names used between November 2018 and January 2020 (versions 1.2.2, 1.2.3, and 1.3.1)
                    new(mDatasetName, PRSM_CSV_RESULT_TABLE_NAME_SUFFIX_ORIGINAL, PROTEOFORM_CSV_RESULT_TABLE_NAME_SUFFIX_ORIGINAL, true)
                };

                int expectedPrsmResults;
                var baseNames = new List<string>();

                if (msAlignFiles.Count <= 1)
                {
                    // TopPIC output file names used in 2021 (starting with version 1.4.4)
                    // Dataset_ms2_toppic_prsm.tsv and Dataset_ms2_toppic_proteoform.tsv

                    // Starting with TopFD v1.7, if the .mzML file is detected to have FAIMS data, the.msalign file will include the voltage level, for example:
                    // T1D_TD_BC228-4_KO_M_4_Aragorn_02Apr21_21-02-01_-35_ms2.msalign
                    // When this is the case, the base file name needs to include the voltage value

                    string baseName;

                    if (msAlignFiles.Count == 0 ||
                        string.Equals(msAlignFiles[0].Name, mDatasetName + AnalysisResourcesTopPIC.MSALIGN_FILE_SUFFIX, StringComparison.OrdinalIgnoreCase))
                    {
                        baseName = mDatasetName;
                    }
                    else
                    {
                        baseName = msAlignFiles[0].Name.Substring(0, msAlignFiles[0].Name.Length - AnalysisResourcesTopPIC.MSALIGN_FILE_SUFFIX.Length);
                    }

                    resultFileNames.Add(new TopPICResultFileInfo(baseName, PRSM_TSV_RESULT_TABLE_NAME_SUFFIX_ORIGINAL, PROTEOFORM_TSV_RESULT_TABLE_NAME_SUFFIX_ORIGINAL));

                    expectedPrsmResults = 1;
                    baseNames.Add(baseName);
                }
                else
                {
                    foreach (var msAlignFile in msAlignFiles)
                    {
                        // Determine the base name by removing the extension and removing "_ms2"
                        var nameWithoutExtension = Path.GetFileNameWithoutExtension(msAlignFile.Name);

                        string baseName;

                        if (nameWithoutExtension.EndsWith("_ms2", StringComparison.OrdinalIgnoreCase))
                        {
                            baseName = nameWithoutExtension.Substring(0, nameWithoutExtension.Length - 4);
                        }
                        else
                        {
                            LogWarning("MSAlign filename does not end with _ms2; this is unexpected: " + nameWithoutExtension);
                            baseName = nameWithoutExtension;
                        }

                        resultFileNames.Add(new TopPICResultFileInfo(baseName, PRSM_TSV_RESULT_TABLE_NAME_SUFFIX_ORIGINAL, PROTEOFORM_TSV_RESULT_TABLE_NAME_SUFFIX_ORIGINAL));
                        baseNames.Add(baseName);
                    }

                    expectedPrsmResults = msAlignFiles.Count;
                }

                var prsmResultsFound = 0;

                var validPrsmResults = 0;
                var validProteoformResults = 0;

                foreach (var resultFileInfo in resultFileNames)
                {
                    var sourcePrsmFile = new FileInfo(Path.Combine(mWorkDir, resultFileInfo.BaseName + resultFileInfo.PrsmFileSuffix));

                    if (!sourcePrsmFile.Exists)
                        continue;

                    var sourcePrsmSingleFile = AppendSuffixToBaseName(sourcePrsmFile, "_single");

                    var sourceProteoformFile = new FileInfo(Path.Combine(mWorkDir, resultFileInfo.BaseName + resultFileInfo.ProteoformFileSuffix));

                    var sourceProteoformSingleFile = AppendSuffixToBaseName(sourceProteoformFile, "_single");

                    prsmResultsFound++;

                    if (!sourceProteoformFile.Exists)
                    {
                        LogError("TopPIC Prsm results file exists ({0}) but the proteoform results file is missing ({1})", sourcePrsmFile.Name, sourceProteoformFile.Name);
                        break;
                    }

                    var targetPrsmFile = new FileInfo(Path.Combine(mWorkDir, resultFileInfo.BaseName + PRSM_RESULT_TABLE_NAME_SUFFIX_FINAL));
                    var targetProteoformFile = new FileInfo(Path.Combine(mWorkDir, resultFileInfo.BaseName + PROTEOFORM_RESULT_TABLE_NAME_SUFFIX_FINAL));

                    // Create file Dataset_TopPIC_PrSMs.txt
                    // In addition, extract the **** Parameters **** block from the start of the PRSM results file and save to TopPIC_RuntimeParameters.txt

                    if (ValidateResultTableFile(sourcePrsmFile, targetPrsmFile, true, resultFileInfo.IsCsvDelimited))
                    {
                        validPrsmResults++;

                        // Also process the "_single" file if it exists
                        // For example Dataset_ms2_toppic_prsm_single.tsv is based on Dataset_ms2_toppic_prsm.tsv but only lists the first protein for each proteoform

                        if (sourcePrsmSingleFile.Exists)
                        {
                            var targetPrsmSingleFile = AppendSuffixToBaseName(targetPrsmFile, "_single");
                            ValidateResultTableFile(sourcePrsmSingleFile, targetPrsmSingleFile, false, resultFileInfo.IsCsvDelimited);
                        }
                    }

                    // Create file Dataset_TopPIC_Proteoforms.txt
                    if (ValidateResultTableFile(sourceProteoformFile, targetProteoformFile, false, resultFileInfo.IsCsvDelimited))
                    {
                        validProteoformResults++;

                        // Also process the "_single" file if it exists
                        // For example Dataset_ms2_toppic_proteoform_single.tsv is based on Dataset_ms2_toppic_proteoform.tsv but only lists the first protein for each proteoform

                        if (sourceProteoformSingleFile.Exists)
                        {
                            var targetProteoformSingleFile = AppendSuffixToBaseName(targetProteoformFile, "_single");
                            ValidateResultTableFile(sourceProteoformSingleFile, targetProteoformSingleFile, false, resultFileInfo.IsCsvDelimited);
                        }
                    }
                }

                var validResults = true;

                if (validPrsmResults < expectedPrsmResults)
                {
                    validResults = false;

                    if (prsmResultsFound == 0)
                    {
                        // TopPIC Prsm results file not found
                        LogError(string.Format(
                            "TopPIC Prsm results {0} not found",
                            expectedPrsmResults > 1 ? "files" : "file"));
                    }
                    else if (expectedPrsmResults == 1)
                    {
                        LogError("TopPIC Prsm results file is not valid");
                    }
                    else if (validPrsmResults > 0)
                    {
                        LogError("{0} / {1} TopPIC Prsm results files were not valid", expectedPrsmResults - validPrsmResults, expectedPrsmResults);
                    }
                    else
                    {
                        LogError("None of the TopPIC Prsm results files were valid");
                    }
                }

                if (validResults && validProteoformResults < expectedPrsmResults)
                {
                    validResults = false;

                    if (expectedPrsmResults == 1)
                    {
                        LogError("TopPIC Proteoform results file not found or not valid");
                    }
                    else if (validPrsmResults > 0)
                    {
                        LogError("{0} / {1} TopPIC Proteoform results files were not valid", expectedPrsmResults - validProteoformResults, expectedPrsmResults);
                    }
                    else
                    {
                        LogError("None of the TopPIC Proteoform results files were valid");
                    }
                }

                // Add numerous temp files to skip
                // These are created when the parameter file has KeepTempFiles=True
                // Example names:
                //   DatasetName_ms2.toppic_one_filter_prefix
                //   DatasetName_ms2.toppic_two_ptm_complete
                //   DatasetName_ms2.toppic_zero_ptm_internal

                var tempFileShiftNames = new List<string> {
                    "zero",
                    "one",
                    "two"
                };

                var tempFileSuffixes = new List<string>
                {
                    "filter_complete",
                    "filter_internal",
                    "filter_prefix",
                    "filter_suffix",
                    "ptm_complete",
                    "ptm_internal",
                    "ptm_prefix",
                    "ptm_suffix"
                };

                foreach (var shiftName in tempFileShiftNames)
                {
                    foreach (var suffixName in tempFileSuffixes)
                    {
                        var fileNameToSkip = string.Format("{0}_ms2.toppic_{1}_{2}", Dataset, shiftName, suffixName);
                        mJobParams.AddResultFileToSkip(fileNameToSkip);
                    }
                }

                // Zip the Html directory (or directories)
                // TopPIC 1.2 (November 2018) created Html directories that include the text _ms2_toppic
                // TopPIC 1.3 (January 2020) and newer create just one _html directory, named DatasetName_html
                // If there are multiple msalign files, there will be multiple html directories
                // Earlier versions do not have _ms2_toppic
                var directorySuffixesToCompress = new SortedSet<string>(StringComparer.OrdinalIgnoreCase) {
                    "_html",
                    "_ms2_toppic_prsm_cutoff_html",
                    "_ms2_toppic_proteoform_cutoff_html",
                    "_ms2_prsm_cutoff_html",
                    "_ms2_proteoform_cutoff_html"};

                if (baseNames.Count > 0)
                {
                    foreach (var baseName in baseNames)
                    {
                        // baseName should be of the form DatasetName_0
                        // Remove the dataset name from baseName then add to directoriesToCompress

                        directorySuffixesToCompress.Add(string.Format("{0}_html", baseName.Substring(mDatasetName.Length)));
                    }
                }

                var directoriesZipped = 0;

                // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator

                foreach (var directorySuffix in directorySuffixesToCompress)
                {
                    var success = ZipTopPICResultsDirectory(directorySuffix, htmlOutputDisabled);

                    if (success)
                        directoriesZipped++;
                }

                if (directoriesZipped >= 1 || htmlOutputDisabled)
                {
                    return validResults;
                }

                LogError(string.Format(
                    "Expected TopPIC html directories were not found; checked for suffixes {0}",
                    string.Join(", ", directorySuffixesToCompress)));

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateAndZipResults", ex);
                return false;
            }
        }

        private bool ValidateFastaFile(out bool fastaFileIsDecoy)
        {
            fastaFileIsDecoy = false;

            // Define the path to the FASTA file
            var localOrgDbFolder = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);

            // Note that job parameter "GeneratedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
            mValidatedFASTAFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

            var fastaFile = new FileInfo(mValidatedFASTAFilePath);

            if (!fastaFile.Exists)
            {
                LogError("FASTA file not found: " + fastaFile.Name, "FASTA file not found: " + fastaFile.FullName);
                return false;
            }

            var proteinOptions = mJobParams.GetParam("ProteinOptions");

            if (!string.IsNullOrEmpty(proteinOptions) && proteinOptions.IndexOf("seq_direction=decoy", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                fastaFileIsDecoy = true;
            }

            return true;
        }

        private bool ValidateResultTableFile(FileSystemInfo sourceFile, FileSystemInfo targetFile, bool saveParameterFile, bool useCsvReader)
        {
            var parametersHeaderMatcher = new Regex(@"\*+ Parameters \*+", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            try
            {
                var validFile = false;
                var foundParamHeaderA = false;
                var foundParamHeaderB = false;

                var parameterInfo = new List<string>();

                if (!sourceFile.Exists)
                {
                    if (mDebugLevel >= 2)
                    {
                        LogWarning("TopPIC results file not found: " + sourceFile.FullName);
                    }
                    return false;
                }

                if (mDebugLevel >= 2)
                {
                    LogMessage("Validating that TopPIC results file {0} is not empty", sourceFile.Name);
                }

                // This RegEx is used to remove double quotes from the start and end of a column value
                var quoteMatcher = new Regex("\"(?<Data>.*)\"", RegexOptions.Compiled);

                CsvReader csvReader = null;
                var currentLineNumber = 0;

                // Open the input file and output file
                // The output file will not include the Parameters block before the header line of the data block

                // Note that the csvParser is instantiated with the StreamReader and thus
                // reader.EndOfStream is no longer reliable after the CsvParser has been instantiated

                using (var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(targetFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    while (true)
                    {
                        if (!foundParamHeaderB)
                        {
                            if (reader.EndOfStream)
                                break;

                            currentLineNumber++;
                            var paramBlockLine = reader.ReadLine();

                            if (string.IsNullOrEmpty(paramBlockLine))
                                continue;

                            // Look for the parameters header: ********************** Parameters **********************
                            var match = parametersHeaderMatcher.Match(paramBlockLine);

                            if (match.Success)
                            {
                                if (!foundParamHeaderA)
                                {
                                    foundParamHeaderA = true;
                                }
                                else
                                {
                                    foundParamHeaderB = true;

                                    if (parameterInfo.Count > 0)
                                    {
                                        // This is second instance of the parameters header
                                        // Optionally write the parameter file
                                        if (saveParameterFile)
                                        {
                                            WriteParametersToDisk(parameterInfo, useCsvReader);
                                        }
                                    }

                                    if (useCsvReader)
                                    {
                                        // Instantiate the CSV Reader
                                        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                                        {
                                            Delimiter = ","
                                        };

                                        csvReader = new CsvReader(reader, config);
                                    }
                                }
                            }
                            else
                            {
                                parameterInfo.Add(paramBlockLine);
                            }

                            continue;
                        }

                        string dataLine;

                        if (useCsvReader)
                        {
                            try
                            {
                                currentLineNumber++;

                                if (!csvReader.Read())
                                {
                                    break;
                                }

                                var rowData = new List<string>();

                                for (var columnIndex = 0; columnIndex < csvReader.Parser.Count; columnIndex++)
                                {
                                    if (!csvReader.TryGetField<string>(columnIndex, out var fieldValue))
                                    {
                                        break;
                                    }

                                    rowData.Add(fieldValue);
                                }

                                dataLine = string.Join("\t", rowData);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Csv reader raised exception reading line {0}: {1}", currentLineNumber, ex.Message);
                                break;
                            }
                        }
                        else
                        {
                            if (reader.EndOfStream)
                                break;

                            currentLineNumber++;
                            dataLine = reader.ReadLine();
                        }

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        // Starting with TopPIC v1.4.4, data in the protein description and proteoform columns are surrounded by double-quotes
                        // These double quotes are not necessary for a tab-delimited file
                        // In contrast, TopPIC 1.5 does not add the double quotes
                        // Remove the double quotes if present

                        var lineParts = dataLine.Split('\t');

                        for (var i = 0; i < lineParts.Length; i++)
                        {
                            var match = quoteMatcher.Match(lineParts[i]);

                            if (!match.Success)
                                continue;

                            lineParts[i] = match.Groups["Data"].Value;
                        }

                        writer.WriteLine(string.Join("\t", lineParts));

                        if (validFile)
                            continue;

                        var dataColumns = dataLine.Split('\t');

                        if (dataColumns.Length > 1)
                        {
                            // Look for an integer in the second column representing "Prsm ID" (the first column has the data file name)
                            if (int.TryParse(dataColumns[1], out _))
                            {
                                // Integer found; line is valid
                                validFile = true;
                            }
                        }
                    }
                }

                mJobParams.AddResultFileToSkip(sourceFile.Name);

                if (validFile)
                    return true;

                if (!foundParamHeaderB)
                {
                    LogError("TopPIC results file is empty: " + sourceFile.Name);
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateResultTableFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Validates the modification definition text
        /// </summary>
        /// <remarks>A valid modification definition contains 5 parts and doesn't contain any whitespace</remarks>
        /// <param name="modificationDefinition">Modification definition</param>
        /// <param name="modClean">Cleaned-up modification definition (output param)</param>
        /// <returns>True if valid; false if invalid</returns>
        private bool ValidateMod(string modificationDefinition, out string modClean)
        {
            modClean = string.Empty;

            var poundIndex = modificationDefinition.IndexOf('#');

            string mod;

            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (poundIndex > 0)
            {
                // comment = mod.Substring(poundIndex);
                mod = modificationDefinition.Substring(0, poundIndex - 1).Trim();
            }
            else
            {
                mod = modificationDefinition.Trim();
            }

            var splitMod = mod.Split(',');

            if (splitMod.Length < 5)
            {
                // Invalid mod definition; must have 5 sections
                LogError("Invalid modification string; must have 5 sections: " + mod);
                return false;
            }

            // Make sure mod does not have both * and any
            if (splitMod[1].Trim() == "*" && splitMod[3].ToLower().Trim() == "any")
            {
                LogError("Modification cannot contain both * and any: " + mod);
                return false;
            }

            // Make sure the Unimod ID is a positive integer or -1
            if (!int.TryParse(splitMod[4], out var unimodId))
            {
                LogError("UnimodID must be an integer: " + splitMod[4]);
                return false;
            }

            if (unimodId < 1 && unimodId != -1)
            {
                LogError(string.Format("Changing UnimodID from {0} to -1", splitMod[4]));
                splitMod[4] = "-1";
            }

            // Reconstruct the mod definition, making sure there is no whitespace
            modClean = splitMod[0].Trim();

            for (var index = 1; index <= splitMod.Length - 1; index++)
            {
                modClean += "," + splitMod[index].Trim();
            }

            return true;
        }

        private List<string> ValidateTopPICMods(IEnumerable<string> modList, out List<string> invalidMods)
        {
            var validatedMods = new List<string>();
            invalidMods = new List<string>();

            foreach (var modEntry in modList)
            {
                if (ValidateMod(modEntry, out var modClean))
                {
                    validatedMods.Add(modClean);
                }
                else
                {
                    invalidMods.Add(modEntry);
                }
            }

            return validatedMods;
        }

        private bool WriteModsFile(string modsFilePath, IEnumerable<string> validatedMods)
        {
            try
            {
                using var writer = new StreamWriter(new FileStream(modsFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                foreach (var modItem in validatedMods)
                {
                    writer.WriteLine(modItem);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError(string.Format("Exception creating mods file {0} for TopPIC", Path.GetFileName(modsFilePath)), ex);
                return false;
            }
        }

        private void WriteParametersToDisk(IEnumerable<string> parameterInfo, bool csvBasedParams)
        {
            try
            {
                var runtimeParamsPath = Path.Combine(mWorkDir, "TopPIC_RuntimeParameters.txt");
                using var writer = new StreamWriter(new FileStream(runtimeParamsPath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                foreach (var parameter in parameterInfo)
                {
                    if (!csvBasedParams)
                    {
                        writer.WriteLine(parameter);
                        continue;
                    }

                    // Parameter lines are of the form "Error tolerance:,15 ppm"
                    // Replace the comma with spaces
                    var paramParts = parameter.Split([','], 2);

                    if (paramParts.Length <= 1)
                    {
                        writer.WriteLine(parameter);
                        continue;
                    }

                    writer.WriteLine("{0,-46}\t{1}", paramParts[0], paramParts[1]);
                }
            }
            catch (Exception ex)
            {
                LogError("Exception saving the parameters file for TopPIC (after TopPIC finished running)", ex);
            }
        }

        private bool ZipTopPICResultsDirectory(string directorySuffix, bool htmlOutputDisabled)
        {
            try
            {
                var zipFilePath = Path.Combine(mWorkDir, mDatasetName + directorySuffix + ".zip");

                var sourceDirectory = new DirectoryInfo(Path.Combine(mWorkDir, mDatasetName + directorySuffix));

                if (!sourceDirectory.Exists)
                {
                    return htmlOutputDisabled && directorySuffix.Equals("_html");
                }

                // Confirm that the directory has one or more files or subdirectories
                if (sourceDirectory.GetFileSystemInfos().Length == 0)
                {
                    if (htmlOutputDisabled && directorySuffix.Equals("_html"))
                        return true;

                    if (mDebugLevel >= 1)
                    {
                        LogWarning("TopPIC results directory is empty; nothing to zip: " + sourceDirectory.Name);
                    }

                    return false;
                }

                var existingZipFile = new FileInfo(zipFilePath);

                if (existingZipFile.Exists)
                {
                    existingZipFile.Delete();
                }

                if (mDebugLevel >= 1)
                {
                    var logMessage = "Zipping directory " + sourceDirectory.FullName;

                    if (mDebugLevel >= 2)
                    {
                        logMessage += ": " + zipFilePath;
                    }
                    LogMessage(logMessage);
                }

                mZipTools.ZipDirectory(sourceDirectory.FullName, zipFilePath);

                return true;
            }
            catch (Exception ex)
            {
                LogError(string.Format("Exception compressing the {0} directory created by TopPIC", directorySuffix), ex);
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

            ParseConsoleOutputFile(Path.Combine(mWorkDir, TOPPIC_CONSOLE_OUTPUT));

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mTopPICVersionText))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("TopPIC");
        }
    }
}
