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
using PRISM;

namespace AnalysisManagerMSPathFinderPlugin
{
    /// <summary>
    /// Class for running MSPathFinder analysis of top down data
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsAnalysisToolRunnerMSPathFinder : clsAnalysisToolRunnerBase
    {

        #region "Constants and Enums"

        private const string MSPATHFINDER_CONSOLE_OUTPUT = "MSPathFinder_ConsoleOutput.txt";
        private const float PROGRESS_PCT_STARTING = 1;
        private const float PROGRESS_PCT_GENERATING_SEQUENCE_TAGS = 2;
        private const float PROGRESS_PCT_TAG_BASED_SEARCHING_TARGET_DB = 3;
        private const float PROGRESS_PCT_SEARCHING_TARGET_DB = 7;
        private const int PROGRESS_PCT_CALCULATING_TARGET_EVALUES = 40;
        private const float PROGRESS_PCT_TAG_BASED_SEARCHING_DECOY_DB = 50;
        private const float PROGRESS_PCT_SEARCHING_DECOY_DB = 54;
        private const float PROGRESS_PCT_CALCULATING_DECOY_EVALUES = 85;
        private const float PROGRESS_PCT_COMPLETE = 99;

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

        #endregion

        #region "Module Variables"

        private string mConsoleOutputErrorMsg;
        private int mFilteredPromexFeatures;
        private int mUnfilteredPromexFeatures;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

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
                    LogDebug("clsAnalysisToolRunnerMSPathFinder.RunTool(): Enter");
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
                ProgRunner.GarbageCollectNow();

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
                mMessage = "Error in MSPathFinderPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveFolder()
        {
            mJobParams.AddResultFileToSkip(Dataset + ".mzXML");

            base.CopyFailedResultsToArchiveFolder();
        }

        private Dictionary<string, string> GetMSPathFinderParameterNames()
        {
            var dctParamNames = new Dictionary<string, string>(25, StringComparer.OrdinalIgnoreCase)
            {
                {"PMTolerance", "t"},
                {"FragTolerance", "f"},
                {"SearchMode", "m"},
                {"ActivationMethod", "act"},
                {"TDA", "tda"},
                {"minLength", "minLength"},
                {"maxLength", "maxLength"},
                {"minCharge", "minCharge"},
                {"maxCharge", "maxCharge"},
                {"minFragCharge", "minFragCharge"},
                {"maxFragCharge", "maxFragCharge"},
                {"minMass", "minMass"},
                {"maxMass", "maxMass"},
                {"tagSearch", "tagSearch"}
            };

            // The following are special cases;
            // do not add to dctParamNames
            //   NumMods
            //   StaticMod
            //   DynamicMod

            return dctParamNames;
        }

        private bool InitializeFastaFile(out bool fastaFileIsDecoy)
        {
            fastaFileIsDecoy = false;

            // Define the path to the fasta file
            var localOrgDbFolder = mMgrParams.GetParam("OrgDbDir");
            var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam("PeptideSearch", "generatedFastaName"));

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
                if (proteinOptions.ToLower().Contains("seq_direction=decoy"))
                {
                    fastaFileIsDecoy = true;
                }
            }

            return true;
        }

        private bool LineStartsWith(string dataLine, string matchString)
        {
            return dataLine.ToLower().StartsWith(matchString.ToLower());
        }

        private readonly Regex mPromexFeatureStats = new Regex(@"ProMex[^\d]+(\d+)/(\d+) features loaded", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private readonly Regex mCheckProgress = new Regex(@"([0-9.]+)% complete", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private readonly Regex mProcessingProteins = new Regex(@"(\d+) proteins done", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the MSPathFinder console output file to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example Console output

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
            // Getting MS1 features from E:\DMS_WorkDir\NCR_2A_G_27Jun15_Samwise_15-05-04.ms1ft.
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
        /// Parses the static and dynamic modification information to create the MSPathFinder Mods file
        /// </summary>
        /// <param name="cmdLineOptions">String builder of command line arguments to pass to MSPathFinder</param>
        /// <param name="numMods">Max Number of Modifications per peptide</param>
        /// <param name="staticMods">List of Static Mods</param>
        /// <param name="dynamicMods">List of Dynamic Mods</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        private bool ParseMSPathFinderModifications(ref string cmdLineOptions, int numMods, IReadOnlyCollection<string> staticMods,
            IReadOnlyCollection<string> dynamicMods)
        {
            const string MOD_FILE_NAME = "MSPathFinder_Mods.txt";
            bool success;

            try
            {
                var modFilePath = Path.Combine(mWorkDir, MOD_FILE_NAME);

                cmdLineOptions += " -mod " + modFilePath;

                using (var modFileWriter = new StreamWriter(new FileStream(modFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    modFileWriter.WriteLine("# This file is used to specify modifications for MSPathFinder");
                    modFileWriter.WriteLine("");
                    modFileWriter.WriteLine("# Max Number of Modifications per peptide");
                    modFileWriter.WriteLine("NumMods=" + numMods);

                    modFileWriter.WriteLine("");
                    modFileWriter.WriteLine("# Static mods");
                    if (staticMods.Count == 0)
                    {
                        modFileWriter.WriteLine("# None");
                    }
                    else
                    {
                        foreach (var staticMod in staticMods)
                        {

                            if (ParseMSPathFinderValidateMod(staticMod, out var modClean))
                            {
                                if (modClean.Contains(",opt,"))
                                {
                                    // Static (fixed) mod is listed as dynamic
                                    // Abort the analysis since the parameter file is misleading and needs to be fixed
                                    var errMsg =
                                        "Static mod definition contains ',opt,'; update the param file to have ',fix,' or change to 'DynamicMod='";
                                    LogError(errMsg, errMsg + "; " + staticMod);
                                    return false;
                                }
                                modFileWriter.WriteLine(modClean);
                            }
                            else
                            {
                                return false;
                            }
                        }
                    }

                    modFileWriter.WriteLine("");
                    modFileWriter.WriteLine("# Dynamic mods");
                    if (dynamicMods.Count == 0)
                    {
                        modFileWriter.WriteLine("# None");
                    }
                    else
                    {
                        foreach (var dynamicMod in dynamicMods)
                        {

                            if (ParseMSPathFinderValidateMod(dynamicMod, out var modClean))
                            {
                                if (modClean.Contains(",fix,"))
                                {
                                    // Dynamic (optional) mod is listed as static
                                    // Abort the analysis since the parameter file is misleading and needs to be fixed
                                    var errMsg =
                                        "Dynamic mod definition contains ',fix,'; update the param file to have ',opt,' or change to 'StaticMod='";
                                    LogError(errMsg, errMsg + "; " + dynamicMod);
                                    return false;
                                }
                                modFileWriter.WriteLine(modClean);
                            }
                            else
                            {
                                return false;
                            }
                        }
                    }
                }

                success = true;

            }
            catch (Exception ex)
            {
                LogError("Exception creating MSPathFinder Mods file", ex);
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Read the MSPathFinder options file and convert the options to command line switches
        /// </summary>
        /// <param name="fastaFileIsDecoy">True if the fasta file has had forward and reverse index files created</param>
        /// <param name="cmdLineOptions">Output: MSPathFinder command line arguments</param>
        /// <param name="tdaEnabled"></param>
        /// <returns>Options string if success; empty string if an error</returns>
        /// <remarks></remarks>
        public CloseOutType ParseMSPathFinderParameterFile(bool fastaFileIsDecoy, out string cmdLineOptions, out bool tdaEnabled)
        {

            cmdLineOptions = string.Empty;
            tdaEnabled = false;

            var paramFileName = mJobParams.GetParam("ParmFileName");

            var paramFileReader = new clsKeyValueParamFileReader("MSPathFinder", mWorkDir, paramFileName);
            RegisterEvents(paramFileReader);

            var eResult = paramFileReader.ParseKeyValueParameterFile(out var paramFileEntries);
            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                mMessage = paramFileReader.ErrorMessage;
                return eResult;
            }

            // Obtain the dictionary that maps parameter names to argument names
            var paramToArgMapping = GetMSPathFinderParameterNames();
            var paramNamesToSkip = new SortedSet<string>(StringComparer.OrdinalIgnoreCase) {
                "NumMods",
                "StaticMod",
                "DynamicMod"
            };

            cmdLineOptions = paramFileReader.ConvertParamsToArgs(paramFileEntries, paramToArgMapping, paramNamesToSkip, "-");
            if (string.IsNullOrWhiteSpace(cmdLineOptions))
            {
                mMessage = paramFileReader.ErrorMessage;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var numMods = 0;
            var staticMods = new List<string>();
            var dynamicMods = new List<string>();

            try
            {
                foreach (var kvSetting in paramFileEntries)
                {
                    var paramValue = kvSetting.Value;

                    if (clsGlobal.IsMatch(kvSetting.Key, "NumMods"))
                    {
                        if (int.TryParse(paramValue, out var intValue))
                        {
                            numMods = intValue;
                        }
                        else
                        {
                            var errMsg = "Invalid value for NumMods in the MSPathFinder parameter file";
                            LogError(errMsg, errMsg + ": " + kvSetting.Key + "=" + kvSetting.Value);
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                    else if (clsGlobal.IsMatch(kvSetting.Key, "StaticMod"))
                    {
                        if (!string.IsNullOrWhiteSpace(paramValue) && !clsGlobal.IsMatch(paramValue, "none"))
                        {
                            staticMods.Add(paramValue);
                        }
                    }
                    else if (clsGlobal.IsMatch(kvSetting.Key, "DynamicMod"))
                    {
                        if (!string.IsNullOrWhiteSpace(paramValue) && !clsGlobal.IsMatch(paramValue, "none"))
                        {
                            dynamicMods.Add(paramValue);
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                mMessage = "Exception extracting dynamic and static mod information from the TopPIC parameter file";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Create the modification file and append the -mod switch
            if (!ParseMSPathFinderModifications(ref cmdLineOptions, numMods, staticMods, dynamicMods))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // ReSharper disable once InvertIf
            if (paramToArgMapping.ContainsKey("-tda 1"))
            {
                tdaEnabled = true;

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
        /// <param name="mod">Modification definition</param>
        /// <param name="modClean">Cleaned-up modification definition (output param)</param>
        /// <returns>True if valid; false if invalid</returns>
        /// <remarks>Valid modification definition contains 5 parts and doesn't contain any whitespace</remarks>
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
                // As of August 12, 2011, the comment cannot contain a comma
                // Sangtae Kim has promised to fix this, but for now, we'll replace commas with semicolons
                comment = comment.Replace(",", ";");
                modClean += "     " + comment;
            }

            return true;
        }

        private bool PostProcessMSPathFinderResults()
        {
            // Move the output files into a subfolder so that we can zip them
            string compressDirPath;

            try
            {
                var workDirInfo = new DirectoryInfo(mWorkDir);

                // Make sure MSPathFinder has released the file handles
                ProgRunner.GarbageCollectNow();

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
                mDotNetZipTools.DebugLevel = mDebugLevel;

                var resultsZipFilePath = Path.Combine(mWorkDir, mDatasetName + "_IcTsv.zip");
                var success = mDotNetZipTools.ZipDirectory(compressDirPath, resultsZipFilePath);

                if (!success)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = mDotNetZipTools.Message;
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

            var eResult = ParseMSPathFinderParameterFile(fastaFileIsDecoy, out var cmdLineOptions, out tdaEnabled);

            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return false;
            }

            if (string.IsNullOrEmpty(cmdLineOptions))
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Problem parsing MSPathFinder parameter file";
                }
                return false;
            }

            var pbfFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_PBF_EXTENSION);
            var featureFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_MS1FT_EXTENSION);

            // Define the path to the fasta file
            var localOrgDbFolder = mMgrParams.GetParam("OrgDbDir");
            var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam("PeptideSearch", "generatedFastaName"));

            LogMessage("Running MSPathFinder");

            // Set up and execute a program runner to run MSPathFinder

            var cmdStr = " -s " + pbfFilePath +
                         " -feature " + featureFilePath +
                         " -d " + fastaFilePath +
                         " -o " + mWorkDir +
                         " " + cmdLineOptions;

            if (mDebugLevel >= 1)
            {
                LogDebug(progLoc + " " + cmdStr);
            }

            mCmdRunner = new clsRunDosProgram(mWorkDir, mDebugLevel);
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
            var success = mCmdRunner.RunProgram(progLoc, cmdStr, "MSPathFinder", true);

            if (!mCmdRunner.WriteConsoleOutputToFile)
            {
                // Write the console output to a text file
                clsGlobal.IdleLoop(0.25);

                using (var writer = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine(mCmdRunner.CachedConsoleOutput);
                }
            }

            // Parse the console output file one more time to check for errors
            clsGlobal.IdleLoop(0.25);
            ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!success)
            {
                var msg = "Error running MSPathFinder";

                if (mConsoleOutputErrorMsg.Contains("No results found"))
                {
                    msg = mConsoleOutputErrorMsg;

                    if (mUnfilteredPromexFeatures > 0)
                    {
                        msg += "; loaded " + mFilteredPromexFeatures + "/" + mUnfilteredPromexFeatures + " ProMex features";
                    }
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
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDLLs = new List<string>
            {
                "InformedProteomics.Backend.dll",
                "InformedProteomics.TopDown.dll"
            };

            return StoreDotNETToolVersionInfo(progLoc, additionalDLLs);

        }

        #endregion

        #region "Event Handlers"

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
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

        #endregion

    }
}
