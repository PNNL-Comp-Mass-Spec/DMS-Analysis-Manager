//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/10/2014
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISM;

namespace AnalysisManagerMSPathFinderPlugin
{
    /// <summary>
    /// Class for running MSPathFinder analysis of top-down data
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerMSPathFinder : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: Acetyl, Dehydro, FASTA, Frag, Ic, Parm, Samwise, tda

        // ReSharper restore CommentTypo

        private const string MSPATHFINDER_CONSOLE_OUTPUT = "MSPathFinder_ConsoleOutput.txt";
        private const int PROGRESS_PCT_STARTING = 1;
        private const int PROGRESS_PCT_GENERATING_SEQUENCE_TAGS = 2;
        private const int PROGRESS_PCT_TAG_BASED_SEARCHING_TARGET_DB = 3;
        private const int PROGRESS_PCT_SEARCHING_TARGET_DB = 7;
        private const int PROGRESS_PCT_CALCULATING_TARGET_EVALUES = 40;
        private const int PROGRESS_PCT_TAG_BASED_SEARCHING_DECOY_DB = 50;
        private const int PROGRESS_PCT_SEARCHING_DECOY_DB = 54;
        private const int PROGRESS_PCT_CALCULATING_DECOY_EVALUES = 85;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private enum MSPathFinderSearchStage
        {
            Start = 0,
            GeneratingSequenceTags = 1,
            TagBasedSearchingTargetDB = 2,
            SearchingTargetDB = 3,
            CalculatingEValuesForTargetSpectra = 4,
            TagBasedSearchingDecoyDB = 5,
            SearchingDecoyDB = 6,
            CalculatingEValuesForDecoySpectra = 7,
            Complete = 8
        }

        private string mConsoleOutputErrorMsg;
        private int mFilteredPromexFeatures;
        private int mUnfilteredPromexFeatures;

        private RunDosProgram mCmdRunner;

        /// <summary>
        /// Runs MSPathFinder
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
                    LogDebug("AnalysisToolRunnerMSPathFinder.RunTool(): Enter");
                }

                // Determine the path to the MSPathFinder program (Top-down version)
                var progLoc = DetermineProgramLocation("MSPathFinderProgLoc", "MSPathFinderT.exe");

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the MSPathFinder version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining MSPathFinder version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!InitializeFastaFile(out var fastaFileIsDecoy))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run MSPathFinder
                var processingSuccess = StartMSPathFinder(progLoc, fastaFileIsDecoy, out var tdaEnabled);

                if (processingSuccess)
                {
                    // Look for the results file

                    FileInfo resultsFile;

                    if (tdaEnabled)
                    {
                        resultsFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_IcTda.tsv"));
                    }
                    else
                    {
                        resultsFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_IcTarget.tsv"));
                    }

                    if (resultsFile.Exists)
                    {
                        var postProcessSuccess = PostProcessMSPathFinderResults();

                        if (!postProcessSuccess)
                        {
                            if (string.IsNullOrEmpty(mMessage))
                            {
                                mMessage = "Unknown error post-processing the MSPathFinder results";
                            }
                            processingSuccess = false;
                        }
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(mMessage))
                        {
                            mMessage = "MSPathFinder results file not found: " + resultsFile.Name;
                            processingSuccess = false;
                        }
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

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in MSPathFinderPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileToSkip(Dataset + ".mzXML");

            base.CopyFailedResultsToArchiveDirectory();
        }

        private bool InitializeFastaFile(out bool fastaFileIsDecoy)
        {
            fastaFileIsDecoy = false;

            // Define the path to the FASTA file
            var localOrgDbFolder = mMgrParams.GetParam("OrgDbDir");
            var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName"));

            var fastaFile = new FileInfo(fastaFilePath);

            if (!fastaFile.Exists)
            {
                // FASTA file not found
                LogError("FASTA file not found: " + fastaFile.Name, "FASTA file not found: " + fastaFile.FullName);
                return false;
            }

            var proteinOptions = mJobParams.GetParam("ProteinOptions");

            if (!string.IsNullOrEmpty(proteinOptions))
            {
                if (proteinOptions.IndexOf("seq_direction=decoy", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    fastaFileIsDecoy = true;
                }
            }

            return true;
        }

        private bool LineStartsWith(string dataLine, string matchString)
        {
            return dataLine.StartsWith(matchString, StringComparison.OrdinalIgnoreCase);
        }

        private readonly Regex mPromexFeatureStats = new(@"ProMex[^\d]+(\d+)/(\d+) features loaded", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private readonly Regex mCheckProgress = new("([0-9.]+)% complete", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private readonly Regex mProcessingProteins = new(@"(\d+) proteins done", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the MSPathFinder console output file to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example Console output

            // ReSharper disable CommentTypo

            // MSPathFinderT 0.93 (June 29, 2015)
            // SpectrumFilePath: E:\DMS_WorkDir\NCR_2A_G_27Jun15_Samwise_15-05-04.pbf
            // DatabaseFilePath: c:\DMS_Temp_Org\ID_004973_9BA6912F_Excerpt.fasta
            // FeatureFilePath:  E:\DMS_WorkDir\NCR_2A_G_27Jun15_Samwise_15-05-04.ms1ft
            // OutputDir:        E:\DMS_WorkDir
            // SearchMode: 1
            // Tda: Target+Decoy
            // PrecursorIonTolerancePpm: 10
            // ProductIonTolerancePpm: 10
            // MinSequenceLength: 21
            // MaxSequenceLength: 300
            // MinPrecursorIonCharge: 2
            // MaxPrecursorIonCharge: 30
            // MinProductIonCharge: 1
            // MaxProductIonCharge: 15
            // MinSequenceMass: 3000
            // MaxSequenceMass: 50000
            // MinFeatureProbability: 0.1
            // MaxDynamicModificationsPerSequence: 4
            // Modifications:
            // C(0) H(0) N(0) O(1) S(0),M,opt,Everywhere,Oxidation
            // C(0) H(-1) N(0) O(0) S(0),C,opt,Everywhere,Dehydro
            // C(2) H(2) N(0) O(1) S(0),*,opt,ProteinNTerm,Acetyl
            // Loading MS1 features from E:\DMS_WorkDir\NCR_2A_G_27Jun15_Samwise_15-05-04.ms1ft.
            // Reading raw file...Elapsed Time: 0.0304 sec
            // Reading ProMex results...332/354 features loaded...Elapsed Time: 5.0866 sec
            // Generating sequence tags for MS/MS spectra...
            // Number of spectra: 6360
            // Processing, 0 spectra done, 0.0% complete, 0.1 sec elapsed
            // Processing, 1863 spectra done, 29.3% complete, 15.1 sec elapsed
            // Processing, 3123 spectra done, 49.1% complete, 30.1 sec elapsed
            // Processing, 3917 spectra done, 61.6% complete, 45.1 sec elapsed
            // Processing, 4718 spectra done, 74.2% complete, 60.2 sec elapsed
            // Processing, 5545 spectra done, 87.2% complete, 75.2 sec elapsed
            // Generated sequence tags: 1345048
            // Elapsed Time: 87.9 sec
            // Reading the target database...Elapsed Time: 0.0 sec
            // Tag-based searching the target database
            // Processing, 0 spectra done, 0.0% complete, 0.0 sec elapsed
            // Processing, 1424 spectra done, 22.4% complete, 15.1 sec elapsed
            // Processing, 1550 spectra done, 24.4% complete, 30.2 sec elapsed
            // ...
            // Processing, 4703 spectra done, 73.9% complete, 1817.6 sec elapsed
            // Processing, 5807 spectra done, 91.3% complete, 2117.9 sec elapsed
            // Target database tag-based search elapsed Time: 2147.0 sec
            // Searching the target database
            // Estimated proteins: 3421782
            // Processing, 0 proteins done, 0.0% complete, 0.0 sec elapsed
            // Processing, 4331 proteins done, 0.1% complete, 15.0 sec elapsed
            // Processing, 7092 proteins done, 0.2% complete, 30.0 sec elapsed
            // ...
            // Processing, 3316784 proteins done, 96.9% complete, 12901.9 sec elapsed
            // Processing, 3398701 proteins done, 99.3% complete, 13201.9 sec elapsed
            // Target database search elapsed Time: 13275.2 sec
            // Calculating spectral E-values for target-spectrum matches
            // Estimated matched proteins: 8059
            // Processing, 0 proteins done, 0.0% complete, 0.2 sec elapsed
            // Processing, 110 proteins done, 1.4% complete, 15.2 sec elapsed
            // Processing, 231 proteins done, 2.9% complete, 30.2 sec elapsed
            // ...
            // Processing, 7222 proteins done, 89.6% complete, 1128.6 sec elapsed
            // Processing, 7513 proteins done, 93.2% complete, 1188.6 sec elapsed
            // Target-spectrum match E-value calculation elapsed Time: 1429.6 sec
            // Reading the decoy database...Elapsed Time: 0.0 sec
            // Tag-based searching the decoy database
            // Processing, 0 spectra done, 0.0% complete, 0.0 sec elapsed
            // Processing, 1544 spectra done, 24.3% complete, 27.0 sec elapsed
            // Processing, 1618 spectra done, 25.4% complete, 42.4 sec elapsed
            // ...
            // Processing, 4150 spectra done, 65.3% complete, 1465.5 sec elapsed
            // Processing, 5351 spectra done, 84.1% complete, 1765.6 sec elapsed
            // Target database tag-based search elapsed Time: 1990.2 sec
            // Searching the decoy database
            // Estimated proteins: 3421782
            // Processing, 0 proteins done, 0.0% complete, 0.0 sec elapsed
            // Processing, 4045 proteins done, 0.1% complete, 15.0 sec elapsed
            // ...
            // Processing, 3341406 proteins done, 97.7% complete, 16795.6 sec elapsed
            // Processing, 3411145 proteins done, 99.7% complete, 17095.6 sec elapsed
            // Decoy database search elapsed Time: 17143.6 sec
            // Calculating spectral E-values for decoy-spectrum matches
            // Estimated matched proteins: 9708
            // Processing, 0 proteins done, 0.0% complete, 0.2 sec elapsed
            // Processing, 100 proteins done, 1.0% complete, 15.2 sec elapsed
            // ...
            // Processing, 7277 proteins done, 75.0% complete, 1188.8 sec elapsed
            // Processing, 8764 proteins done, 90.3% complete, 1488.8 sec elapsed
            // Decoy-spectrum match E-value calculation elapsed Time: 1849.9 sec
            // Done.
            // Total elapsed time for search: 37962.4 sec (632.71 min)

            // ReSharper restore CommentTypo

            const string EXCEPTION_FLAG = "Exception while processing:";
            const string ERROR_PROCESSING_FLAG = "Error processing";

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

                // progressComplete values are between 0 and 100
                // MSPathFinder reports % complete values numerous times for each section
                // We keep track of the section using currentStage
                float progressCompleteCurrentStage = 0;
                var currentStage = MSPathFinderSearchStage.Start;

                var percentCompleteFound = false;

                // This array holds the % complete values at the start of each stage
                var percentCompleteLevels = new float[(int) MSPathFinderSearchStage.Complete + 1];

                percentCompleteLevels[(int) MSPathFinderSearchStage.Start] = 0;
                percentCompleteLevels[(int) MSPathFinderSearchStage.GeneratingSequenceTags] = PROGRESS_PCT_GENERATING_SEQUENCE_TAGS;
                percentCompleteLevels[(int) MSPathFinderSearchStage.TagBasedSearchingTargetDB] = PROGRESS_PCT_TAG_BASED_SEARCHING_TARGET_DB;
                percentCompleteLevels[(int) MSPathFinderSearchStage.SearchingTargetDB] = PROGRESS_PCT_SEARCHING_TARGET_DB;
                percentCompleteLevels[(int) MSPathFinderSearchStage.CalculatingEValuesForTargetSpectra] = PROGRESS_PCT_CALCULATING_TARGET_EVALUES;
                percentCompleteLevels[(int) MSPathFinderSearchStage.TagBasedSearchingDecoyDB] = PROGRESS_PCT_TAG_BASED_SEARCHING_DECOY_DB;
                percentCompleteLevels[(int) MSPathFinderSearchStage.SearchingDecoyDB] = PROGRESS_PCT_SEARCHING_DECOY_DB;
                percentCompleteLevels[(int) MSPathFinderSearchStage.CalculatingEValuesForDecoySpectra] = PROGRESS_PCT_CALCULATING_DECOY_EVALUES;
                percentCompleteLevels[(int) MSPathFinderSearchStage.Complete] = PROGRESS_PCT_COMPLETE;

                var unfilteredFeatures = 0;

                var targetProteinsSearched = 0;
                var decoyProteinsSearched = 0;

                var searchingDecoyDB = false;
                mConsoleOutputErrorMsg = string.Empty;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            continue;
                        }

                        var dataLineLCase = dataLine.ToLower();

                        if (dataLineLCase.StartsWith(EXCEPTION_FLAG.ToLower()) || dataLineLCase.Contains("unhandled exception"))
                        {
                            // Exception while processing

                            var exceptionMessage = dataLine.Substring(EXCEPTION_FLAG.Length).TrimStart();

                            mConsoleOutputErrorMsg = "Error running MSPathFinder: " + exceptionMessage;
                            break;
                        }

                        if (dataLineLCase.StartsWith(ERROR_PROCESSING_FLAG.ToLower()))
                        {
                            // Error processing FileName.msf1lt: Error details;

                            string errorMessage;

                            var colonIndex = dataLine.IndexOf(':');

                            if (colonIndex > 0)
                            {
                                errorMessage = dataLine.Substring(colonIndex + 1).Trim();
                            }
                            else
                            {
                                errorMessage = dataLine;
                            }

                            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                if (errorMessage.Contains("No results found"))
                                {
                                    mConsoleOutputErrorMsg = errorMessage;
                                }
                                else
                                {
                                    mConsoleOutputErrorMsg = "Error running MSPathFinder: " + errorMessage;
                                }

                                continue;
                            }

                            mConsoleOutputErrorMsg += "; " + errorMessage;
                            continue;
                        }

                        if (LineStartsWith(dataLine, "Generating sequence tags for MS/MS spectra"))
                        {
                            currentStage = MSPathFinderSearchStage.GeneratingSequenceTags;
                        }
                        else if (LineStartsWith(dataLine, "Reading the target database") ||
                                 LineStartsWith(dataLine, "tag-based searching the target database"))
                        {
                            currentStage = MSPathFinderSearchStage.TagBasedSearchingTargetDB;
                        }
                        else if (LineStartsWith(dataLine, "Searching the target database"))
                        {
                            currentStage = MSPathFinderSearchStage.SearchingTargetDB;
                        }
                        else if (LineStartsWith(dataLine, "Calculating spectral E-values for target-spectrum matches"))
                        {
                            currentStage = MSPathFinderSearchStage.CalculatingEValuesForTargetSpectra;
                        }
                        else if (LineStartsWith(dataLine, "Reading the decoy database") ||
                                 LineStartsWith(dataLine, "Tag-based searching the decoy database"))
                        {
                            currentStage = MSPathFinderSearchStage.TagBasedSearchingDecoyDB;
                            searchingDecoyDB = true;
                            continue;
                        }
                        else if (LineStartsWith(dataLine, "Searching the decoy database"))
                        {
                            currentStage = MSPathFinderSearchStage.SearchingDecoyDB;
                            searchingDecoyDB = true;
                            continue;
                        }
                        else if (LineStartsWith(dataLine, "Calculating spectral E-values for decoy-spectrum matches"))
                        {
                            currentStage = MSPathFinderSearchStage.CalculatingEValuesForDecoySpectra;
                        }

                        var progressMatch = mCheckProgress.Match(dataLine);

                        if (progressMatch.Success)
                        {
                            if (float.TryParse(progressMatch.Groups[1].ToString(), out var progressValue))
                            {
                                progressCompleteCurrentStage = progressValue;
                                percentCompleteFound = true;
                            }
                            continue;
                        }

                        if (percentCompleteFound)
                        {
                            // No need to manually compute the % complete
                            continue;
                        }

                        if (unfilteredFeatures == 0)
                        {
                            var promexResultsMatch = mPromexFeatureStats.Match(dataLine);

                            if (promexResultsMatch.Success)
                            {
                                if (int.TryParse(promexResultsMatch.Groups[1].ToString(), out var filteredFeatures))
                                {
                                    mFilteredPromexFeatures = filteredFeatures;
                                }
                                if (int.TryParse(promexResultsMatch.Groups[2].ToString(), out unfilteredFeatures))
                                {
                                    mUnfilteredPromexFeatures = unfilteredFeatures;
                                }
                            }
                        }

                        var proteinSearchedMatch = mProcessingProteins.Match(dataLine);

                        if (proteinSearchedMatch.Success)
                        {
                            if (int.TryParse(proteinSearchedMatch.Groups[1].ToString(), out var proteinsSearched))
                            {
                                if (searchingDecoyDB)
                                {
                                    decoyProteinsSearched = Math.Max(decoyProteinsSearched, proteinsSearched);
                                }
                                else
                                {
                                    targetProteinsSearched = Math.Max(targetProteinsSearched, proteinsSearched);
                                }
                            }
                        }
                    }
                }

                float progressComplete = 0;

                if (percentCompleteFound)
                {
                    // Numeric % complete values were found

                    var progressCompleteAtStart = percentCompleteLevels[(int) currentStage];
                    var progressCompleteAtEnd = percentCompleteLevels[(int) currentStage + 1];

                    progressComplete = ComputeIncrementalProgress(progressCompleteAtStart, progressCompleteAtEnd, progressCompleteCurrentStage);
                }
                else if (searchingDecoyDB)
                {
                    // Numeric % complete values were not found, but we did encounter "Searching the decoy database"
                    // so we can thus now compute % complete based on the number of proteins searched
                    progressComplete = ComputeIncrementalProgress(PROGRESS_PCT_SEARCHING_DECOY_DB, PROGRESS_PCT_COMPLETE, decoyProteinsSearched, targetProteinsSearched);
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

        /// <summary>
        /// Read the MSPathFinder parameter file and validate the static and dynamic mods
        /// </summary>
        /// <param name="fastaFileIsDecoy">True if the FASTA file has had forward and reverse index files created</param>
        /// <param name="parameterFileName">Output: MSPathFinder parameter file name</param>
        /// <param name="tdaEnabled"></param>
        /// <returns>Options string if success; empty string if an error</returns>
        public CloseOutType ParseMSPathFinderParameterFile(bool fastaFileIsDecoy, out string parameterFileName, out bool tdaEnabled)
        {
            tdaEnabled = false;

            parameterFileName = mJobParams.GetParam("ParamFileName");

            // Although ParseKeyValueParameterFile checks for paramFileName being an empty string,
            // we check for it here since the name comes from the settings file, so we want to customize the error message
            if (string.IsNullOrWhiteSpace(parameterFileName))
            {
                LogError("MSPathFinder parameter file not defined in the job settings (param name ParamFileName)");
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            var result = LoadSettingsFromKeyValueParameterFile("MSPathFinder", parameterFileName, out var paramFileEntries, out var paramFileReader);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            tdaEnabled = paramFileReader.ParamIsEnabled(paramFileEntries, "TDA");

            // ReSharper disable once InvertIf
            if (tdaEnabled)
            {
                // MSPathFinder should be run with a forward=only protein collection; allow MSPathFinder to add the decoy proteins
                if (fastaFileIsDecoy)
                {
                    LogError("Parameter file / decoy protein collection conflict: do not use a decoy protein collection " +
                             "when using a parameter file with setting TDA=1");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Validates that the modification definition text
        /// </summary>
        /// <remarks>Valid modification definition contains 5 parts and doesn't contain any whitespace</remarks>
        /// <param name="mod">Modification definition</param>
        /// <param name="modClean">Cleaned-up modification definition (output param)</param>
        /// <returns>True if valid; false if invalid</returns>
        [Obsolete("No longer used")]
        private bool ParseMSPathFinderValidateMod(string mod, out string modClean)
        {
            var comment = string.Empty;

            modClean = string.Empty;

            var poundIndex = mod.IndexOf('#');

            if (poundIndex > 0)
            {
                comment = mod.Substring(poundIndex);
                mod = mod.Substring(0, poundIndex - 1).Trim();
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

            // Reconstruct the mod definition, making sure there is no whitespace
            modClean = splitMod[0].Trim();

            for (var index = 1; index <= splitMod.Length - 1; index++)
            {
                modClean += "," + splitMod[index].Trim();
            }

            if (!string.IsNullOrWhiteSpace(comment))
            {
                modClean += "     " + comment;
            }

            return true;
        }

        private bool PostProcessMSPathFinderResults()
        {
            // Move the output files into a subdirectory so that we can zip them
            string compressDirPath;

            try
            {
                var workDirInfo = new DirectoryInfo(mWorkDir);

                // Make sure MSPathFinder has released the file handles
                AppUtils.GarbageCollectNow();

                var compressDirInfo = new DirectoryInfo(Path.Combine(mWorkDir, "TempCompress"));

                if (compressDirInfo.Exists)
                {
                    foreach (var fileToDelete in compressDirInfo.GetFiles())
                    {
                        fileToDelete.Delete();
                    }
                }
                else
                {
                    compressDirInfo.Create();
                }

                var resultFiles = workDirInfo.GetFiles(mDatasetName + "*_Ic*.tsv").ToList();

                if (resultFiles.Count == 0)
                {
                    mMessage = "Did not find any _Ic*.tsv files";
                    return false;
                }

                foreach (var fileToMove in resultFiles)
                {
                    var targetFilePath = Path.Combine(compressDirInfo.FullName, fileToMove.Name);
                    fileToMove.MoveTo(targetFilePath);
                }

                compressDirPath = compressDirInfo.FullName;
            }
            catch (Exception ex)
            {
                LogError("Exception preparing the MSPathFinder results for zipping: " + ex.Message);
                return false;
            }

            try
            {
                mZipTools.DebugLevel = mDebugLevel;

                var resultsZipFilePath = Path.Combine(mWorkDir, mDatasetName + "_IcTsv.zip");
                var success = mZipTools.ZipDirectory(compressDirPath, resultsZipFilePath);

                if (!success)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = mZipTools.Message;

                        if (string.IsNullOrEmpty(mMessage))
                        {
                            mMessage = "Unknown error zipping the MSPathFinder results";
                        }
                    }
                }

                return success;
            }
            catch (Exception ex)
            {
                LogError("Exception zipping the MSPathFinder results: " + ex.Message);
                return false;
            }
        }

        private bool StartMSPathFinder(string progLoc, bool fastaFileIsDecoy, out bool tdaEnabled)
        {
            mConsoleOutputErrorMsg = string.Empty;

            // Read the MSPathFinder Parameter File
            // The parameter file name specifies the mass modifications to consider, plus also the analysis parameters

            var result = ParseMSPathFinderParameterFile(fastaFileIsDecoy, out var parameterFileName, out tdaEnabled);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return false;
            }

            var pbfFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_PBF_EXTENSION);
            var featureFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_MS1FT_EXTENSION);

            // Define the path to the FASTA file
            var localOrgDbFolder = mMgrParams.GetParam("OrgDbDir");
            var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName"));

            var paramFilePath = Path.Combine(mWorkDir, parameterFileName);

            LogMessage("Running MSPathFinder");

            // Set up and execute a program runner to run MSPathFinder

            var arguments = " -s " + pbfFilePath +
                            " -feature " + featureFilePath +
                            " -d " + fastaFilePath +
                            " -o " + mWorkDir +
                            " -ParamFile " + paramFilePath;

            if (mDebugLevel >= 1)
            {
                LogDebug(progLoc + " " + arguments);
            }

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, MSPATHFINDER_CONSOLE_OUTPUT);

            mProgress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var success = mCmdRunner.RunProgram(progLoc, arguments, "MSPathFinder", true);

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
                string msg;

                if (mConsoleOutputErrorMsg.Contains("No results found"))
                {
                    msg = string.Format("{0}{1}",
                        mConsoleOutputErrorMsg,
                        mUnfilteredPromexFeatures == 0 ? string.Empty :
                            string.Format("; loaded {0}/{1} ProMex features", mFilteredPromexFeatures, mUnfilteredPromexFeatures));
                }
                else
                {
                    msg = "Error running MSPathFinder";
                }

                LogError(msg);

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("MSPathFinder returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to MSPathFinder failed (but exit code is 0)");
                }

                return false;
            }

            mProgress = PROGRESS_PCT_COMPLETE;
            mStatusTools.UpdateAndWrite(mProgress);

            if (mDebugLevel >= 3)
            {
                LogDebug("MSPathFinder Search Complete");
            }

            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDLLs = new List<string>
            {
                "InformedProteomics.Backend.dll",
                "InformedProteomics.TopDown.dll"
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

            // Parse the console output file every 30 seconds
            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(mWorkDir, MSPATHFINDER_CONSOLE_OUTPUT));

                UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

                LogProgress("MSPathFinder");
            }
        }
    }
}
