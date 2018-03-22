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
using System.Text;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerMSPathFinderPlugin
{
    /// <summary>
    /// Class for running MSPathFinder analysis of top down data
    /// </summary>
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
        private int m_filteredPromexFeatures;
        private int m_unfilteredPromexFeatures;

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

                if (m_DebugLevel > 4)
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
                    m_message = "Error determining MSPathFinder version";
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

                    FileInfo fiResultsFile;

                    if (tdaEnabled)
                    {
                        fiResultsFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "_IcTda.tsv"));
                    }
                    else
                    {
                        fiResultsFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "_IcTarget.tsv"));
                    }

                    if (fiResultsFile.Exists)
                    {
                        var postProcessSuccess = PostProcessMSPathFinderResults();
                        if (!postProcessSuccess)
                        {
                            if (string.IsNullOrEmpty(m_message))
                            {
                                m_message = "Unknown error post-processing the MSPathFinder results";
                            }
                            processingSuccess = false;
                        }
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            m_message = "MSPathFinder results file not found: " + fiResultsFile.Name;
                            processingSuccess = false;
                        }
                    }
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                clsProgRunner.GarbageCollectNow();

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
                m_message = "Error in MSPathFinderPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveFolder()
        {
            m_jobParams.AddResultFileToSkip(Dataset + ".mzXML");

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
            var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
            var fastaFilePath = Path.Combine(localOrgDbFolder, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"));

            var fiFastaFile = new FileInfo(fastaFilePath);

            if (!fiFastaFile.Exists)
            {
                // Fasta file not found
                LogError("Fasta file not found: " + fiFastaFile.Name, "Fasta file not found: " + fiFastaFile.FullName);
                return false;
            }

            var strProteinOptions = m_jobParams.GetParam("ProteinOptions");
            if (!string.IsNullOrEmpty(strProteinOptions))
            {
                if (strProteinOptions.ToLower().Contains("seq_direction=decoy"))
                {
                    fastaFileIsDecoy = true;
                }
            }

            return true;
        }

        private bool LineStartsWith(string strLineIn, string matchString)
        {
            return strLineIn.ToLower().StartsWith(matchString.ToLower());
        }

        private readonly Regex rePromexFeatureStats = new Regex(@"ProMex[^\d]+(\d+)/(\d+) features loaded", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private readonly Regex reCheckProgress = new Regex(@"([0-9.]+)% complete", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private readonly Regex reProcessingProteins = new Regex(@"(\d+) proteins done", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the MSPathFinder console output file to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
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

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                        {
                            continue;
                        }

                        var strLineInLCase = strLineIn.ToLower();

                        if (strLineInLCase.StartsWith(EXCEPTION_FLAG.ToLower()) || strLineInLCase.Contains("unhandled exception"))
                        {
                            // Exception while processing

                            var exceptionMessage = strLineIn.Substring(EXCEPTION_FLAG.Length).TrimStart();

                            mConsoleOutputErrorMsg = "Error running MSPathFinder: " + exceptionMessage;
                            break;
                        }

                        if (strLineInLCase.StartsWith(ERROR_PROCESSING_FLAG.ToLower()))
                        {
                            // Error processing FileName.msf1lt: Error details;

                            string errorMessage;

                            var colonIndex = strLineIn.IndexOf(':');
                            if (colonIndex > 0)
                            {
                                errorMessage = strLineIn.Substring(colonIndex + 1).Trim();
                            }
                            else
                            {
                                errorMessage = strLineIn;
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

                        if (LineStartsWith(strLineIn, "Generating sequence tags for MS/MS spectra"))
                        {
                            currentStage = MSPathFinderSearchStage.GeneratingSequenceTags;
                        }
                        else if (LineStartsWith(strLineIn, "Reading the target database") ||
                                 LineStartsWith(strLineIn, "tag-based searching the target database"))
                        {
                            currentStage = MSPathFinderSearchStage.TagBasedSearchingTargetDB;
                        }
                        else if (LineStartsWith(strLineIn, "Searching the target database"))
                        {
                            currentStage = MSPathFinderSearchStage.SearchingTargetDB;
                        }
                        else if (LineStartsWith(strLineIn, "Calculating spectral E-values for target-spectrum matches"))
                        {
                            currentStage = MSPathFinderSearchStage.CalculatingEValuesForTargetSpectra;
                        }
                        else if (LineStartsWith(strLineIn, "Reading the decoy database") ||
                                 LineStartsWith(strLineIn, "Tag-based searching the decoy database"))
                        {
                            currentStage = MSPathFinderSearchStage.TagBasedSearchingDecoyDB;
                            searchingDecoyDB = true;
                            continue;
                        }
                        else if (LineStartsWith(strLineIn, "Searching the decoy database"))
                        {
                            currentStage = MSPathFinderSearchStage.SearchingDecoyDB;
                            searchingDecoyDB = true;
                            continue;
                        }
                        else if (LineStartsWith(strLineIn, "Calculating spectral E-values for decoy-spectrum matches"))
                        {
                            currentStage = MSPathFinderSearchStage.CalculatingEValuesForDecoySpectra;
                        }

                        var oProgressMatch = reCheckProgress.Match(strLineIn);
                        if (oProgressMatch.Success)
                        {
                            if (float.TryParse(oProgressMatch.Groups[1].ToString(), out var progressValue))
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
                            var oPromexResults = rePromexFeatureStats.Match(strLineIn);
                            if (oPromexResults.Success)
                            {
                                if (int.TryParse(oPromexResults.Groups[1].ToString(), out var filteredFeatures))
                                {
                                    m_filteredPromexFeatures = filteredFeatures;
                                }
                                if (int.TryParse(oPromexResults.Groups[2].ToString(), out unfilteredFeatures))
                                {
                                    m_unfilteredPromexFeatures = unfilteredFeatures;
                                }
                            }
                        }

                        var oProteinSerchedMatch = reProcessingProteins.Match(strLineIn);
                        if (oProteinSerchedMatch.Success)
                        {
                            if (int.TryParse(oProteinSerchedMatch.Groups[1].ToString(), out var proteinsSearched))
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

                if (m_progress < progressComplete)
                {
                    m_progress = progressComplete;
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
        /// Parses the static and dynamic modification information to create the MSPathFinder Mods file
        /// </summary>
        /// <param name="strParameterFilePath">Full path to the MSPathFinder parameter file; will create file MSPathFinder_Mods.txt in the same folder</param>
        /// <param name="sbOptions">String builder of command line arguments to pass to MSPathFinder</param>
        /// <param name="intNumMods">Max Number of Modifications per peptide</param>
        /// <param name="lstStaticMods">List of Static Mods</param>
        /// <param name="lstDynamicMods">List of Dynamic Mods</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        private bool ParseMSPathFinderModifications(string strParameterFilePath, StringBuilder sbOptions, int intNumMods, IReadOnlyCollection<string> lstStaticMods,
            IReadOnlyCollection<string> lstDynamicMods)
        {
            const string MOD_FILE_NAME = "MSPathFinder_Mods.txt";
            bool blnSuccess;

            try
            {
                var fiParameterFile = new FileInfo(strParameterFilePath);

                if (string.IsNullOrWhiteSpace(fiParameterFile.DirectoryName))
                {
                    LogError("Cannot determine the parent directory of " + fiParameterFile.FullName);
                    return false;
                }

                var strModFilePath = Path.Combine(fiParameterFile.DirectoryName, MOD_FILE_NAME);

                sbOptions.Append(" -mod " + strModFilePath);

                using (var swModFile = new StreamWriter(new FileStream(strModFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swModFile.WriteLine("# This file is used to specify modifications for MSPathFinder");
                    swModFile.WriteLine("");
                    swModFile.WriteLine("# Max Number of Modifications per peptide");
                    swModFile.WriteLine("NumMods=" + intNumMods);

                    swModFile.WriteLine("");
                    swModFile.WriteLine("# Static mods");
                    if (lstStaticMods.Count == 0)
                    {
                        swModFile.WriteLine("# None");
                    }
                    else
                    {
                        foreach (var strStaticMod in lstStaticMods)
                        {

                            if (ParseMSPathFinderValidateMod(strStaticMod, out var strModClean))
                            {
                                if (strModClean.Contains(",opt,"))
                                {
                                    // Static (fixed) mod is listed as dynamic
                                    // Abort the analysis since the parameter file is misleading and needs to be fixed
                                    var errMsg =
                                        "Static mod definition contains ',opt,'; update the param file to have ',fix,' or change to 'DynamicMod='";
                                    LogError(errMsg, errMsg + "; " + strStaticMod);
                                    return false;
                                }
                                swModFile.WriteLine(strModClean);
                            }
                            else
                            {
                                return false;
                            }
                        }
                    }

                    swModFile.WriteLine("");
                    swModFile.WriteLine("# Dynamic mods");
                    if (lstDynamicMods.Count == 0)
                    {
                        swModFile.WriteLine("# None");
                    }
                    else
                    {
                        foreach (var strDynamicMod in lstDynamicMods)
                        {

                            if (ParseMSPathFinderValidateMod(strDynamicMod, out var strModClean))
                            {
                                if (strModClean.Contains(",fix,"))
                                {
                                    // Dynamic (optional) mod is listed as static
                                    // Abort the analysis since the parameter file is misleading and needs to be fixed
                                    var errMsg =
                                        "Dynamic mod definition contains ',fix,'; update the param file to have ',opt,' or change to 'StaticMod='";
                                    LogError(errMsg, errMsg + "; " + strDynamicMod);
                                    return false;
                                }
                                swModFile.WriteLine(strModClean);
                            }
                            else
                            {
                                return false;
                            }
                        }
                    }
                }

                blnSuccess = true;

            }
            catch (Exception ex)
            {
                LogError("Exception creating MSPathFinder Mods file", ex);
                blnSuccess = false;
            }

            return blnSuccess;
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
            var intNumMods = 0;
            var lstStaticMods = new List<string>();
            var lstDynamicMods = new List<string>();

            cmdLineOptions = string.Empty;
            tdaEnabled = false;

            var strParameterFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"));

            if (!File.Exists(strParameterFilePath))
            {
                LogError("Parameter file not found", "Parameter file not found: " + strParameterFilePath);
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            var sbOptions = new StringBuilder(500);

            try
            {
                // Initialize the Param Name dictionary
                var dctParamNames = GetMSPathFinderParameterNames();

                using (var srParamFile = new StreamReader(new FileStream(strParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srParamFile.EndOfStream)
                    {
                        var strLineIn = srParamFile.ReadLine();

                        var kvSetting = clsGlobal.GetKeyValueSetting(strLineIn);

                        if (!string.IsNullOrWhiteSpace(kvSetting.Key))
                        {
                            var strValue = kvSetting.Value;


                            // Check whether kvSetting.key is one of the standard keys defined in dctParamNames
                            if (dctParamNames.TryGetValue(kvSetting.Key, out var strArgumentSwitch))
                            {
                                sbOptions.Append(" -" + strArgumentSwitch + " " + strValue);

                            }
                            else if (clsGlobal.IsMatch(kvSetting.Key, "NumMods"))
                            {
                                if (int.TryParse(strValue, out var intValue))
                                {
                                    intNumMods = intValue;
                                }
                                else
                                {
                                    var errMsg = "Invalid value for NumMods in MSPathFinder parameter file";
                                    LogError(errMsg, errMsg + ": " + strLineIn);
                                    srParamFile.Close();
                                    return CloseOutType.CLOSEOUT_FAILED;
                                }
                            }
                            else if (clsGlobal.IsMatch(kvSetting.Key, "StaticMod"))
                            {
                                if (!string.IsNullOrWhiteSpace(strValue) && !clsGlobal.IsMatch(strValue, "none"))
                                {
                                    lstStaticMods.Add(strValue);
                                }
                            }
                            else if (clsGlobal.IsMatch(kvSetting.Key, "DynamicMod"))
                            {
                                if (!string.IsNullOrWhiteSpace(strValue) && !clsGlobal.IsMatch(strValue, "none"))
                                {
                                    lstDynamicMods.Add(strValue);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception reading MSPathFinder parameter file";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Create the modification file and append the -mod switch
            if (!ParseMSPathFinderModifications(strParameterFilePath, sbOptions, intNumMods, lstStaticMods, lstDynamicMods))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            cmdLineOptions = sbOptions.ToString();

            if (cmdLineOptions.Contains("-tda 1"))
            {
                tdaEnabled = true;
                // Make sure the .Fasta file is not a Decoy fasta
                if (fastaFileIsDecoy)
                {
                    LogError(
                        "Parameter file / decoy protein collection conflict: do not use a decoy protein collection when using a target/decoy parameter file (which has setting TDA=1)");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Validates that the modification definition text
        /// </summary>
        /// <param name="strMod">Modification definition</param>
        /// <param name="strModClean">Cleaned-up modification definition (output param)</param>
        /// <returns>True if valid; false if invalid</returns>
        /// <remarks>Valid modification definition contains 5 parts and doesn't contain any whitespace</remarks>
        private bool ParseMSPathFinderValidateMod(string strMod, out string strModClean)
        {
            var strComment = string.Empty;

            strModClean = string.Empty;

            var intPoundIndex = strMod.IndexOf('#');
            if (intPoundIndex > 0)
            {
                strComment = strMod.Substring(intPoundIndex);
                strMod = strMod.Substring(0, intPoundIndex - 1).Trim();
            }

            var strSplitMod = strMod.Split(',');

            if (strSplitMod.Length < 5)
            {
                // Invalid mod definition; must have 5 sections
                LogError("Invalid modification string; must have 5 sections: " + strMod);
                return false;
            }

            // Make sure mod does not have both * and any
            if (strSplitMod[1].Trim() == "*" && strSplitMod[3].ToLower().Trim() == "any")
            {
                LogError("Modification cannot contain both * and any: " + strMod);
                return false;
            }

            // Reconstruct the mod definition, making sure there is no whitespace
            strModClean = strSplitMod[0].Trim();
            for (var intIndex = 1; intIndex <= strSplitMod.Length - 1; intIndex++)
            {
                strModClean += "," + strSplitMod[intIndex].Trim();
            }

            if (!string.IsNullOrWhiteSpace(strComment))
            {
                // As of August 12, 2011, the comment cannot contain a comma
                // Sangtae Kim has promised to fix this, but for now, we'll replace commas with semicolons
                strComment = strComment.Replace(",", ";");
                strModClean += "     " + strComment;
            }

            return true;
        }

        private bool PostProcessMSPathFinderResults()
        {
            // Move the output files into a subfolder so that we can zip them
            string compressDirPath;

            try
            {
                var diWorkDir = new DirectoryInfo(m_WorkDir);

                // Make sure MSPathFinder has released the file handles
                clsProgRunner.GarbageCollectNow();

                var diCompressDir = new DirectoryInfo(Path.Combine(m_WorkDir, "TempCompress"));
                if (diCompressDir.Exists)
                {
                    foreach (var fiFile in diCompressDir.GetFiles())
                    {
                        fiFile.Delete();
                    }
                }
                else
                {
                    diCompressDir.Create();
                }

                var fiResultFiles = diWorkDir.GetFiles(m_Dataset + "*_Ic*.tsv").ToList();

                if (fiResultFiles.Count == 0)
                {
                    m_message = "Did not find any _Ic*.tsv files";
                    return false;
                }

                foreach (var fiFile in fiResultFiles)
                {
                    var targetFilePath = Path.Combine(diCompressDir.FullName, fiFile.Name);
                    fiFile.MoveTo(targetFilePath);
                }

                compressDirPath = diCompressDir.FullName;
            }
            catch (Exception ex)
            {
                LogError("Exception preparing the MSPathFinder results for zipping: " + ex.Message);
                return false;
            }

            try
            {
                m_DotNetZipTools.DebugLevel = m_DebugLevel;

                var resultsZipFilePath = Path.Combine(m_WorkDir, m_Dataset + "_IcTsv.zip");
                var blnSuccess = m_DotNetZipTools.ZipDirectory(compressDirPath, resultsZipFilePath);

                if (!blnSuccess)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = m_DotNetZipTools.Message;
                        if (string.IsNullOrEmpty(m_message))
                        {
                            m_message = "Unknown error zipping the MSPathFinder results";
                        }
                    }
                }

                return blnSuccess;
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
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Problem parsing MSPathFinder parameter file";
                }
                return false;
            }

            var pbfFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_PBF_EXTENSION);
            var featureFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_MS1FT_EXTENSION);

            // Define the path to the fasta file
            var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
            var fastaFilePath = Path.Combine(localOrgDbFolder, m_jobParams.GetParam("PeptideSearch", "generatedFastaName"));

            LogMessage("Running MSPathFinder");

            // Set up and execute a program runner to run MSPathFinder

            var cmdStr = " -s " + pbfFilePath;
            cmdStr += " -feature " + featureFilePath;
            cmdStr += " -d " + fastaFilePath;
            cmdStr += " -o " + m_WorkDir;
            cmdStr += " " + cmdLineOptions;

            if (m_DebugLevel >= 1)
            {
                LogDebug(progLoc + " " + cmdStr);
            }

            mCmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, MSPATHFINDER_CONSOLE_OUTPUT);

            m_progress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var success = mCmdRunner.RunProgram(progLoc, cmdStr, "MSPathFinder", true);

            if (!mCmdRunner.WriteConsoleOutputToFile)
            {
                // Write the console output to a text file
                Thread.Sleep(250);

                var swConsoleOutputfile =
                    new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));
                swConsoleOutputfile.WriteLine(mCmdRunner.CachedConsoleOutput);
                swConsoleOutputfile.Close();
            }

            // Parse the console output file one more time to check for errors
            Thread.Sleep(250);
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

                    if (m_unfilteredPromexFeatures > 0)
                    {
                        msg += "; loaded " + m_filteredPromexFeatures + "/" + m_unfilteredPromexFeatures + " ProMex features";
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

            m_progress = PROGRESS_PCT_COMPLETE;
            m_StatusTools.UpdateAndWrite(m_progress);
            if (m_DebugLevel >= 3)
            {
                LogDebug("MSPathFinder Search Complete");
            }

            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string strProgLoc)
        {
            var additionalDlls = new List<string>
            {
                "InformedProteomics.Backend.dll",
                "InformedProteomics.TopDown.dll"
            };

            return StoreDotNETToolVersionInfo(strProgLoc, additionalDlls);

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

            // Parse the console output file every 30 seconds
            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE)
            {
                dtLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(m_WorkDir, MSPATHFINDER_CONSOLE_OUTPUT));

                UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

                LogProgress("MSPathFinder");
            }
        }

        #endregion

    }
}
