//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2008, Battelle Memorial Institute
// Created 10/10/2008
//
//*********************************************************************************************************

using AnalysisManagerBase;
using AnalysisManagerMSGFDBPlugIn;
using MSGFResultsSummarizer;
using PHRPReader;
using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace AnalysisManagerExtractionPlugin
{
    /// <summary>
    /// Primary class for controlling data extraction
    /// </summary>
    /// <remarks></remarks>
    public class clsExtractToolRunner : clsAnalysisToolRunnerBase
    {
        #region "Constants"

        public const float PROGRESS_EXTRACTION_START = 3;

        /// <summary>
        /// SEQUEST jobs have an extract extraction step, where progress will be between 0% and 33% complete
        /// MSGFPlus_SplitFasta jobs will call ConvertMZIDToTSV for each .mzid file, during which progress will update from 3% to 33%
        /// </summary>
        private const float PROGRESS_EXTRACTION_DONE = 33;

        /// <summary>
        /// For all tools, progress will be between 33% and 66% complete while PHRP is running
        /// For SEQUEST, we also run Peptide Prophet, during which progress will be between 66% and 100%
        /// For SplitFasta MS-GF+ jobs, we merge .mzid files, during which progress will be between 66% and 100%
        /// </summary>
        private const float PROGRESS_PHRP_DONE = 66;

        private const float PROGRESS_PEPTIDE_PROPHET_OR_MZID_MERGE_DONE = 90;

        private const float PROGRESS_COMPLETE = 100;

        public const string INSPECT_UNFILTERED_RESULTS_FILE_SUFFIX = "_inspect_unfiltered.txt";

        private const string MODa_JAR_NAME = "moda.jar";
        private const string MODa_FILTER_JAR_NAME = "anal_moda.jar";

        private const string MODPlus_JAR_NAME = "modp_pnnl.jar";
        private const string MODPlus_FILTER_JAR_NAME = "tda_plus.jar";

        #endregion

        #region "Module variables"

        private MSGFPlusUtils mMSGFPlusUtils;
        private bool mMSGFPlusUtilsError;

        private string mGeneratedFastaFilePath;

        private string mMzidMergerConsoleOutputFilePath;
        #endregion

        #region "Methods"

        /// <summary>
        /// Runs the data extraction tool(s)
        /// </summary>
        /// <returns>CloseOutType representing success or failure</returns>
        public override CloseOutType RunTool()
        {

            var currentAction = "preparing for extraction";

            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerDeconPeakDetector.RunTool(): Enter");
                }

                // Store the AnalysisManager version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false for Data Extraction");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

                // Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
                // However, if job parameter SkipProteinMods is True, the Fasta file will not have been retrieved
                var fastaFileName = mJobParams.GetParam("PeptideSearch", "generatedFastaName");
                if (string.IsNullOrWhiteSpace(fastaFileName))
                {
                    mGeneratedFastaFilePath = string.Empty;
                }
                else
                {
                    mGeneratedFastaFilePath = Path.Combine(orgDbDirectoryPath, fastaFileName);
                }

                CloseOutType result;
                var processingSuccess = true;

                switch (mJobParams.GetParam("ResultType"))
                {
                    case clsAnalysisResources.RESULT_TYPE_SEQUEST:
                        // Run the Peptide Extractor DLL
                        currentAction = "running peptide extraction for SEQUEST";
                        result = PerformPeptideExtraction();

                        // Check for no data first. If no data, exit but still copy results to server
                        if (result == CloseOutType.CLOSEOUT_NO_DATA)
                        {
                            break;
                        }

                        // Run PHRP
                        if (result == CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            mProgress = PROGRESS_EXTRACTION_DONE;     // 33% done
                            UpdateStatusRunning(mProgress);

                            currentAction = "running peptide hits result processor for SEQUEST";
                            result = RunPhrpForSEQUEST();
                        }

                        if (result == CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            mProgress = PROGRESS_PHRP_DONE;   // 66% done
                            UpdateStatusRunning(mProgress);
                            currentAction = "running peptide prophet for SEQUEST";
                            RunPeptideProphet();
                        }
                        break;

                    case clsAnalysisResources.RESULT_TYPE_XTANDEM:
                        // Run PHRP
                        currentAction = "running peptide hits result processor for X!Tandem";
                        result = RunPhrpForXTandem();
                        break;

                    case clsAnalysisResources.RESULT_TYPE_INSPECT:
                        // Run PHRP
                        currentAction = "running peptide hits result processor for Inspect";
                        result = RunPhrpForInSpecT();
                        break;

                    case clsAnalysisResources.RESULT_TYPE_MSGFPLUS:
                        // Run PHRP
                        currentAction = "running peptide hits result processor for MS-GF+";
                        result = RunPhrpForMSGFPlus();

                        var splitFastaEnabled = mJobParams.GetJobParameter("SplitFasta", false);
                        if (result == CloseOutType.CLOSEOUT_SUCCESS && splitFastaEnabled)
                        {
                            result = RunMzidMerger(Dataset + "_msgfplus_Part*.mzid", Dataset + "_msgfplus.mzid");
                        }
                        break;

                    case clsAnalysisResources.RESULT_TYPE_MSALIGN:
                        // Run PHRP
                        currentAction = "running peptide hits result processor for MSAlign";
                        result = RunPhrpForMSAlign();
                        break;

                    case clsAnalysisResources.RESULT_TYPE_MODA:
                        // Convert the MODa results to a tab-delimited file; do not filter out the reversed-hit proteins
                        result = ConvertMODaResultsToTxt(out var filteredMODaResultsFilePath, true);
                        if (result != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                            break;
                        }

                        // Run PHRP
                        currentAction = "running peptide hits result processor for MODa";
                        result = RunPhrpForMODa(filteredMODaResultsFilePath);
                        if (result != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                        }

                        // Convert the MODa results to a tab-delimited file, filter by FDR (and filter out the reverse-hit proteins)
                        result = ConvertMODaResultsToTxt(out filteredMODaResultsFilePath, false);
                        if (result != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                        }
                        break;

                    case clsAnalysisResources.RESULT_TYPE_MODPLUS:
                        // Convert the MODPlus results to a tab-delimited file; do not filter out the reversed-hit proteins
                        result = ConvertMODPlusResultsToTxt(out var filteredMODPlusResultsFilePath, true);
                        if (result != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                            break;
                        }

                        // Run PHRP
                        currentAction = "running peptide hits result processor for MODPlus";
                        result = RunPhrpForMODPlus(filteredMODPlusResultsFilePath);
                        if (result != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                        }

                        // Convert the MODPlus results to a tab-delimited file, filter by FDR (and filter out the reverse-hit proteins)
                        result = ConvertMODPlusResultsToTxt(out filteredMODPlusResultsFilePath, false);
                        if (result != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                        }
                        break;

                    case clsAnalysisResources.RESULT_TYPE_MSPATHFINDER:
                        // Run PHRP
                        currentAction = "running peptide hits result processor for MSPathFinder";
                        result = RunPHRPForMSPathFinder();
                        break;

                    case clsAnalysisResources.RESULT_TYPE_TOPPIC:
                        // Run PHRP
                        currentAction = "running peptide hits result processor for TopPIC";
                        result = RunPHRPForTopPIC();
                        break;

                    default:
                        // Should never get here - invalid result type specified
                        LogError("Invalid ResultType specified: " + mJobParams.GetParam("ResultType"));
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                if (result == CloseOutType.CLOSEOUT_NO_DATA)
                {
                    // Make sure mMessage has text; this will appear in the Completion_Message column in the database
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        // Storing "No results above threshold" in mMessage will result in the job being assigned state No Export (14) in DMS
                        // See stored procedure UpdateJobState
                        mMessage = NO_RESULTS_ABOVE_THRESHOLD;
                    }
                }

                // Possibly run AScore
                var runAscore = mJobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, clsAnalysisResourcesExtraction.JOB_PARAM_RUN_ASCORE, false);
                if (runAscore)
                {
                    LogMessage("TODO: Run AScore");
                    // result = RunAscore());
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS && result != CloseOutType.CLOSEOUT_NO_DATA)
                {
                    LogError("Error " + currentAction);
                    processingSuccess = false;
                }
                else
                {
                    mProgress = PROGRESS_COMPLETE;
                    UpdateStatusRunning(mProgress);
                    mJobParams.AddResultFileToSkip(clsPepHitResultsProcWrapper.PHRP_LOG_FILE_NAME);
                }

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the results directory,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var copySuccess = CopyResultsToTransferDirectory();

                if (!copySuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                // Everything succeeded; now delete the _msgfplus.tsv file from the server

                // For SplitFasta files there will be multiple tsv files to delete,
                // plus the individual ConsoleOutput.txt files (all tracked with mJobParams.ServerFilesToDelete)

                RemoveNonResultServerFiles();

                return result;
            }
            catch (Exception ex)
            {
                LogError("Exception running extraction tool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Convert the MODa output file to a tab-delimited text file
        /// </summary>
        /// <param name="filteredMODaResultsFilePath">Output parameter: path to the filtered results file</param>
        /// <param name="keepAllResults"></param>
        /// <returns>The path to the .txt file if successful; empty string if an error</returns>
        /// <remarks></remarks>
        private CloseOutType ConvertMODaResultsToTxt(out string filteredMODaResultsFilePath, bool keepAllResults)
        {
            var fdrThreshold = mJobParams.GetJobParameter("MODaFDRThreshold", 0.05f);
            var decoyPrefix = mJobParams.GetJobParameter("MODaDecoyPrefix", "Reversed_");

            const bool isModPlus = false;

            return ConvertMODaOrMODPlusResultsToTxt(fdrThreshold, decoyPrefix, isModPlus, out filteredMODaResultsFilePath, keepAllResults);
        }

        private CloseOutType ConvertMODPlusResultsToTxt(out string filteredMODPlusResultsFilePath, bool keepAllResults)
        {
            var fdrThreshold = mJobParams.GetJobParameter("MODPlusDecoyFilterFDR", 0.05f);
            var decoyPrefix = mJobParams.GetJobParameter("MODPlusDecoyPrefix", "Reversed_");

            const bool isModPlus = true;

            return ConvertMODaOrMODPlusResultsToTxt(fdrThreshold, decoyPrefix, isModPlus, out filteredMODPlusResultsFilePath, keepAllResults);
        }

        private CloseOutType ConvertMODaOrMODPlusResultsToTxt(float fdrThreshold, string decoyPrefixJobParam, bool isModPlus,
            out string filteredResultsFilePath, bool keepAllResults)
        {
            filteredResultsFilePath = string.Empty;

            try
            {
                string toolName;
                string fileNameSuffix;
                string modxProgJarName;
                string modxFilterJarName;

                if (isModPlus)
                {
                    toolName = "MODPlus";
                    fileNameSuffix = "_modp.txt";
                    modxProgJarName = MODPlus_JAR_NAME;
                    modxFilterJarName = MODPlus_FILTER_JAR_NAME;
                }
                else
                {
                    toolName = "MODa";
                    fileNameSuffix = "_moda.txt";
                    modxProgJarName = MODa_JAR_NAME;
                    modxFilterJarName = MODa_FILTER_JAR_NAME;
                }

                if (keepAllResults || string.IsNullOrWhiteSpace(mGeneratedFastaFilePath))
                {
                    // Use a fake decoy prefix so that all results will be kept (the top hit for each scan that anal_moda/tda_plus decides to keep)
                    decoyPrefixJobParam = "ABC123XYZ_";

                    if (!keepAllResults)
                    {
                        LogWarning("FASTA file not defined, cannot verify the decoy prefix; will instead include the top hit for each scan, regardless of protein");
                    }
                }
                else
                {
                    const int MINIMUM_PERCENT_DECOY = 25;
                    var fastaFile = new FileInfo(mGeneratedFastaFilePath);

                    if (mDebugLevel >= 1)
                    {
                        LogMessage("Verifying the decoy prefix in " + fastaFile.Name);
                    }

                    // Determine the most common decoy prefix in the Fasta file
                    var decoyPrefixes = clsAnalysisResources.GetDefaultDecoyPrefixes();
                    var bestPrefix = new KeyValuePair<double, string>(0, string.Empty);

                    foreach (var decoyPrefix in decoyPrefixes)
                    {
                        var fractionDecoy = clsAnalysisResources.GetDecoyFastaCompositionStats(fastaFile, decoyPrefix, out var _);

                        if (fractionDecoy * 100 >= MINIMUM_PERCENT_DECOY)
                        {
                            if (fractionDecoy > bestPrefix.Key)
                            {
                                bestPrefix = new KeyValuePair<double, string>(fractionDecoy, decoyPrefix);
                            }
                        }
                    }

                    if (!string.IsNullOrWhiteSpace(bestPrefix.Value) && bestPrefix.Value != decoyPrefixJobParam)
                    {
                        var msg = string.Format("Using decoy prefix {0} instead of {1} as defined by job parameter MODPlusDecoyPrefix " +
                                                "because {2:F1}% of the proteins start with {0}",
                                                bestPrefix.Value, decoyPrefixJobParam, bestPrefix.Key * 100);

                        LogWarning(msg, true);

                        decoyPrefixJobParam = bestPrefix.Value;
                    }
                }

                var paramFileName = mJobParams.GetParam("ParmFileName");
                var paramFilePath = Path.Combine(mWorkDir, paramFileName);

                var resultsFilePath = Path.Combine(mWorkDir, mDatasetName + fileNameSuffix);

                if (Math.Abs(fdrThreshold) < float.Epsilon)
                {
                    fdrThreshold = 0.05f;
                }
                else if (fdrThreshold > 1)
                {
                    fdrThreshold = 1;
                }

                if (mDebugLevel >= 2)
                {
                    LogMessage("Filtering MODa/MODPlus Results with FDR threshold " + fdrThreshold.ToString("0.00"));
                }

                const int javaMemorySize = 1000;

                // javaProgLoc will typically be "C:\Program Files\Java\jre8\bin\java.exe"
                var javaProgLoc = GetJavaProgLoc();
                if (string.IsNullOrWhiteSpace(javaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to the MODa or MODPlus program
                var modxProgLoc = DetermineProgramLocation(toolName + "ProgLoc", modxProgJarName);

                var modXProgram = new FileInfo(modxProgLoc);

                if (string.IsNullOrWhiteSpace(modXProgram.DirectoryName))
                {
                    LogError("Cannot determine the parent directory of " + modXProgram.FullName);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Set up and execute a program runner to run anal_moda.jar or tda_plus.jar
                var arguments = " -Xmx" + javaMemorySize + "M" +
                                " -jar " + Path.Combine(modXProgram.DirectoryName, modxFilterJarName) +
                                " -i " + resultsFilePath;

                if (!isModPlus)
                {
                    // Processing MODa data; include the parameter file
                    arguments += " -p " + paramFilePath;
                }

                arguments += " -fdr " + fdrThreshold;
                arguments += " -d " + decoyPrefixJobParam;

                // Example command line:
                // "C:\Program Files\Java\jre8\bin\java.exe" -Xmx1000M -jar C:\DMS_Programs\MODa\anal_moda.jar
                //   -i "E:\DMS_WorkDir3\QC_Shew_13_04_pt1_1_2_45min_14Nov13_Leopard_13-05-21_moda.txt"
                //   -p "E:\DMS_WorkDir3\MODa_PartTryp_Par20ppm_Frag0pt6Da" -fdr 0.05 -d XXX_
                // "C:\Program Files\Java\jre8\bin\java.exe" -Xmx1000M -jar C:\DMS_Programs\MODPlus\tda_plus.jar
                //   -i "E:\DMS_WorkDir3\QC_Shew_13_04_pt1_1_2_45min_14Nov13_Leopard_13-05-21_modp.txt"
                //   -fdr 0.05 -d Reversed_

                LogDebug(javaProgLoc + " " + arguments);

                var progRunner = new clsRunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = false,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(mWorkDir, toolName + "_Filter_ConsoleOutput.txt")
                };
                RegisterEvents(progRunner);

                var success = progRunner.RunProgram(javaProgLoc, arguments, toolName + "_Filter", true);

                if (!success)
                {
                    LogError("Error parsing and filtering " + toolName + " results");

                    if (progRunner.ExitCode != 0)
                    {
                        LogWarning(
                            modxFilterJarName + " returned a non-zero exit code: " + progRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to " + modxFilterJarName + " failed (but exit code is 0)");
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mJobParams.AddResultFileToSkip(Path.GetFileName(progRunner.ConsoleOutputFilePath));

                // Confirm that the results file was created
                var filteredResultsFile = new FileInfo(Path.ChangeExtension(resultsFilePath, ".id.txt"));

                if (!filteredResultsFile.Exists)
                {
                    LogError("Filtered " + toolName + " results file not found: " + filteredResultsFile.Name);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                filteredResultsFilePath = filteredResultsFile.FullName;
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in ConvertMODaOrMODPlusResultsToTxt", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Convert the .mzid file created by MS-GF+ to a .tsv file
        /// </summary>
        /// <param name="suffixToAdd">Suffix to add when parsing files created by Parallel MS-GF+</param>
        /// <returns>The path to the .tsv file if successful; empty string if an error</returns>
        /// <remarks></remarks>
        private string ConvertMZIDToTSV(string suffixToAdd)
        {
            try
            {
                var mzidFileName = mDatasetName + "_msgfplus" + suffixToAdd + ".mzid";
                if (!File.Exists(Path.Combine(mWorkDir, mzidFileName)))
                {
                    var mzidFileNameAlternate = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(mzidFileName, "Dataset_msgfdb.txt");
                    if (File.Exists(Path.Combine(mWorkDir, mzidFileNameAlternate)))
                    {
                        mzidFileName = mzidFileNameAlternate;
                    }
                    else
                    {
                        LogError(mzidFileName + " file not found");
                        return string.Empty;
                    }
                }

                // Determine the path to the MzidToTsvConverter
                var mzidToTsvConverterProgLoc = DetermineProgramLocation("MzidToTsvConverterProgLoc", "MzidToTsvConverter.exe");

                if (string.IsNullOrWhiteSpace(mzidToTsvConverterProgLoc))
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        LogError("Parameter 'MzidToTsvConverter' not defined for this manager");
                    }
                    return string.Empty;
                }

                Console.WriteLine();

                // Initialize mMSGFPlusUtils
                mMSGFPlusUtils = new MSGFPlusUtils(mMgrParams, mJobParams, mWorkDir, mDebugLevel);
                RegisterEvents(mMSGFPlusUtils);

                // Attach an additional handler for the ErrorEvent
                // This additional handler sets mMSGFPlusUtilsError to true
                mMSGFPlusUtils.ErrorEvent += MSGFPlusUtils_ErrorEvent;

                mMSGFPlusUtilsError = false;

                var tsvFilePath = mMSGFPlusUtils.ConvertMZIDToTSV(mzidToTsvConverterProgLoc, mDatasetName, mzidFileName);

                if (mMSGFPlusUtilsError)
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        LogError("mMSGFPlusUtilsError is True after call to ConvertMZIDToTSV");
                    }
                    return string.Empty;
                }

                if (!string.IsNullOrWhiteSpace(tsvFilePath))
                {
                    // File successfully created

                    if (string.IsNullOrWhiteSpace(suffixToAdd))
                        return tsvFilePath;

                    var tsvFile = new FileInfo(tsvFilePath);

                    if (string.IsNullOrWhiteSpace(tsvFile.DirectoryName))
                    {
                        LogError("Cannot determine the parent directory of " + tsvFile.FullName);
                        return string.Empty;
                    }

                    var newTSVPath = Path.Combine(tsvFile.DirectoryName,
                                                  Path.GetFileNameWithoutExtension(tsvFilePath) + suffixToAdd + ".tsv");

                    tsvFile.MoveTo(newTSVPath);
                    return newTSVPath;
                }

                if (string.IsNullOrWhiteSpace(mMessage))
                {
                    LogError("Error calling mMSGFPlusUtils.ConvertMZIDToTSV; path not returned");
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in ConvertMZIDToTSV", ex);
            }

            return string.Empty;
        }

        /// <summary>
        /// Create the Peptide to Protein map file for the given MS-GF+ results file
        /// </summary>
        /// <param name="resultsFileName"></param>
        /// <returns></returns>
        private CloseOutType CreateMSGFPlusResultsProteinToPeptideMappingFile(string resultsFileName)
        {
            LogMessage("Creating the missing _PepToProtMap.txt file");

            var localOrgDbDir = mMgrParams.GetParam("OrgDbDir");
            if (mMSGFPlusUtils == null)
            {
                mMSGFPlusUtils = new MSGFPlusUtils(mMgrParams, mJobParams, mWorkDir, mDebugLevel);
                RegisterEvents(mMSGFPlusUtils);

                // Attach an additional handler for the ErrorEvent
                // This additional handler sets mMSGFPlusUtilsError to true
                mMSGFPlusUtils.ErrorEvent += MSGFPlusUtils_ErrorEvent;
            }

            mMSGFPlusUtilsError = false;

            // Assume this is true
            const bool resultsIncludeAutoAddedDecoyPeptides = true;

            var result = mMSGFPlusUtils.CreatePeptideToProteinMapping(resultsFileName, resultsIncludeAutoAddedDecoyPeptides, localOrgDbDir);

            if (result != CloseOutType.CLOSEOUT_SUCCESS && result != CloseOutType.CLOSEOUT_NO_DATA)
            {
                return result;
            }

            if (mMSGFPlusUtilsError)
            {
                if (string.IsNullOrWhiteSpace(mMessage))
                {
                    LogError("mMSGFPlusUtilsError is True after call to CreatePeptideToProteinMapping");
                }

                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType ParallelMSGFPlusMergeTSVFiles(int numberOfClonedSteps, int numberOfHitsPerScanToKeep,
            out SortedSet<string> filterPassingPeptides)
        {
            filterPassingPeptides = new SortedSet<string>();

            try
            {
                var mergedFilePath = Path.Combine(mWorkDir, mDatasetName + "_msgfplus.tsv");

                // Keys in this dictionary are column names, values are the 0-based column index
                var headerMapping = new Dictionary<string, int>();

                // This dictionary keeps track of the top hit(s) for each scan/charge combo
                // Keys are scan_charge
                // Values are the clsMSGFPlusPSMs class, which keeps track of the top numberOfHitsPerScanToKeep hits for each scan/charge combo
                var dctScanChargeTopHits = new Dictionary<string, clsMSGFPlusPSMs>();

                // This dictionary keeps track of the best score (lowest SpecEValue) for each scan/charge combo
                // Keys are scan_charge
                // Values the lowest SpecEValue for the scan/charge
                var dctScanChargeBestScore = new Dictionary<string, double>();

                long totalLinesProcessed = 0;
                var warningsLogged = 0;

                using (var mergedFileWriter = new StreamWriter(new FileStream(mergedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    for (var iteration = 1; iteration <= numberOfClonedSteps; iteration++)
                    {
                        var sourceFilePath = Path.Combine(mWorkDir, mDatasetName + "_msgfplus_Part" + iteration + ".tsv");
                        var linesRead = 0;

                        if (mDebugLevel >= 2)
                        {
                            LogDebug("Caching data from " + sourceFilePath);
                        }

                        using (var reader = new StreamReader(new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                        {
                            while (!reader.EndOfStream)
                            {
                                var dataLine = reader.ReadLine();
                                if (string.IsNullOrWhiteSpace(dataLine))
                                    continue;

                                linesRead += 1;
                                totalLinesProcessed += 1;

                                if (linesRead == 1)
                                {
                                    if (iteration != 1)
                                        continue;

                                    // Write the header line
                                    mergedFileWriter.WriteLine(dataLine);

                                    const bool IS_CASE_SENSITIVE = false;
                                    var headerNames = new List<string>
                                    {
                                        "ScanNum",
                                        "Charge",
                                        "Peptide",
                                        "Protein",
                                        "SpecEValue"
                                    };
                                    headerMapping = clsGlobal.ParseHeaderLine(dataLine, headerNames, IS_CASE_SENSITIVE);

                                    foreach (var headerName in headerNames)
                                    {
                                        if (headerMapping[headerName] < 0)
                                        {
                                            LogError(string.Format("Header {0} not found in {1}; unable to merge the MS-GF+ .tsv files",
                                                                   headerName, Path.GetFileName(sourceFilePath)));
                                            return CloseOutType.CLOSEOUT_FAILED;
                                        }
                                    }
                                }
                                else
                                {
                                    var splitLine = dataLine.Split('\t');

                                    var scanNumber = splitLine[headerMapping["ScanNum"]];
                                    var chargeState = splitLine[headerMapping["Charge"]];

                                    int.TryParse(scanNumber, out var scanNumberValue);
                                    int.TryParse(chargeState, out var chargeStateValue);

                                    var scanChargeCombo = scanNumber + "_" + chargeState;
                                    var peptide = splitLine[headerMapping["Peptide"]];
                                    var protein = splitLine[headerMapping["Protein"]];
                                    var specEValueText = splitLine[headerMapping["SpecEValue"]];

                                    if (!double.TryParse(specEValueText, out var specEValue))
                                    {
                                        if (warningsLogged < 10)
                                        {
                                            LogWarning("SpecEValue was not numeric: " + specEValueText + " in " + dataLine);
                                            warningsLogged += 1;

                                            if (warningsLogged >= 10)
                                            {
                                                LogWarning("Additional warnings will not be logged");
                                            }
                                        }

                                        continue;
                                    }

                                    var udtPSM = new clsMSGFPlusPSMs.udtPSMType
                                    {
                                        Peptide = peptide,
                                        SpecEValue = specEValue,
                                        DataLine = dataLine
                                    };

                                    if (dctScanChargeTopHits.TryGetValue(scanChargeCombo, out var hitsForScan))
                                    {
                                        // Possibly store this value

                                        var passesFilter = hitsForScan.AddPSM(udtPSM, protein);

                                        if (passesFilter && specEValue < dctScanChargeBestScore[scanChargeCombo])
                                        {
                                            dctScanChargeBestScore[scanChargeCombo] = specEValue;
                                        }
                                    }
                                    else
                                    {
                                        // New entry for this scan/charge combo
                                        hitsForScan = new clsMSGFPlusPSMs(scanNumberValue, chargeStateValue, numberOfHitsPerScanToKeep);
                                        hitsForScan.AddPSM(udtPSM, protein);

                                        dctScanChargeTopHits.Add(scanChargeCombo, hitsForScan);
                                        dctScanChargeBestScore.Add(scanChargeCombo, specEValue);
                                    }
                                }
                            }
                        }
                    }

                    if (mDebugLevel >= 2)
                    {
                        LogDebug("Sorting results for " + dctScanChargeBestScore.Count + " lines of scan/charge combos");
                    }

                    // Sort the data, then write to disk
                    var scansByScore = from item in dctScanChargeBestScore orderby item.Value select item.Key;
                    var filterPassingPSMCount = 0;

                    foreach (var scanChargeCombo in scansByScore)
                    {
                        var hitsForScan = dctScanChargeTopHits[scanChargeCombo];
                        var lastPeptide = string.Empty;

                        foreach (var psm in hitsForScan.PSMs)
                        {
                            mergedFileWriter.WriteLine(psm.DataLine);

                            if (!filterPassingPeptides.Contains(psm.Peptide))
                            {
                                filterPassingPeptides.Add(psm.Peptide);
                            }

                            if (!string.Equals(psm.Peptide, lastPeptide))
                            {
                                filterPassingPSMCount += 1;
                                lastPeptide = psm.Peptide;
                            }
                        }
                    }

                    if (mDebugLevel >= 1)
                    {
                        LogMessage(
                            "Read " + totalLinesProcessed + " data lines from " + numberOfClonedSteps + " MS-GF+ .tsv files; wrote " +
                            filterPassingPSMCount + " PSMs to the merged file");
                    }
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in ParallelMSGFPlusMergeTSVFiles", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType ParallelMSGFPlusMergePepToProtMapFiles(int numberOfClonedSteps, ICollection<string> filterPassingPeptides)
        {
            try
            {
                var mergedFilePath = Path.Combine(mWorkDir, mDatasetName + "_msgfplus_PepToProtMap.txt");

                var tempFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_msgfplus_PepToProtMap.tmp"));
                mJobParams.AddResultFileToSkip(tempFile.Name);

                long totalLinesProcessed = 0;
                long totalLinesToWrite = 0;

                var pepProtMappingWritten = new SortedSet<string>();

                var lastPeptideFull = string.Empty;
                var addCurrentPeptide = false;

                using (var writer = new StreamWriter(new FileStream(tempFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    for (var iteration = 1; iteration <= numberOfClonedSteps; iteration++)
                    {
                        var sourceFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_msgfplus_Part" + iteration + "_PepToProtMap.txt"));
                        if (!sourceFile.Exists)
                        {
                            LogWarning("Peptide to protein map file not found; cannot merge: " + sourceFile.FullName);
                            continue;
                        }

                        var linesRead = 0;

                        if (mDebugLevel >= 2)
                        {
                            LogDebug("Caching data from " + sourceFile.FullName);
                        }

                        using (var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                        {
                            while (!reader.EndOfStream)
                            {
                                var dataLine = reader.ReadLine();
                                if (string.IsNullOrWhiteSpace(dataLine))
                                    continue;

                                linesRead += 1;
                                totalLinesProcessed += 1;

                                if (linesRead == 1 && iteration == 1)
                                {
                                    // Write the header line
                                    writer.WriteLine(dataLine);
                                    continue;
                                }

                                var charIndex = dataLine.IndexOf('\t');
                                if (charIndex <= 0)
                                {
                                    continue;
                                }

                                var peptideFull = dataLine.Substring(0, charIndex);
                                var peptide = clsMSGFPlusPSMs.RemovePrefixAndSuffix(peptideFull);

                                if (string.Equals(lastPeptideFull, peptideFull) || filterPassingPeptides.Contains(peptide))
                                {
                                    if (!string.Equals(lastPeptideFull, peptideFull))
                                    {
                                        // Done processing the last peptide; we can now update pepProtMappingWritten to True for this peptide
                                        // to prevent it from getting added to the merged file again in the future

                                        if (!string.IsNullOrWhiteSpace(lastPeptideFull))
                                        {
                                            if (!pepProtMappingWritten.Contains(lastPeptideFull))
                                            {
                                                pepProtMappingWritten.Add(lastPeptideFull);
                                            }
                                        }

                                        lastPeptideFull = string.Copy(peptideFull);
                                        addCurrentPeptide = !pepProtMappingWritten.Contains(peptideFull);
                                    }

                                    // Add this peptide if we didn't already add it during a previous iteration
                                    if (addCurrentPeptide)
                                    {
                                        writer.WriteLine(dataLine);
                                        totalLinesToWrite += 1;
                                    }
                                }
                            }
                        }
                    }
                }

                if (mDebugLevel >= 1)
                {
                    LogMessage(
                        "Read " + totalLinesProcessed + " data lines from " + numberOfClonedSteps + " _PepToProtMap files; now sorting the " +
                        totalLinesToWrite + " merged peptides using FlexibleFileSortUtility.dll");
                }

                var success = SortTextFile(tempFile.FullName, mergedFilePath, true);

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in ParallelMSGFPlusMergePepToProtMapFiles", ex);

                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void ParseMzidMergerConsoleOutputFile()
        {
            try
            {
                var reFileCount = new Regex(@"Input files: \(count: (?<FileCount>\d+)\)");
                var reMerging = new Regex(@"Merging file (?<MergeFile>\d+)");

                var totalFiles = 0;
                var filesMerged = 0;

                float progressSubtask = 0;

                if (!File.Exists(mMzidMergerConsoleOutputFilePath))
                    return;

                using (var reader = new StreamReader(new FileStream(mMzidMergerConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            continue;
                        }

                        var matchFileCount = reFileCount.Match(dataLine);
                        if (matchFileCount.Success)
                        {
                            totalFiles = int.Parse(matchFileCount.Groups["FileCount"].Value);
                            continue;
                        }

                        var matchMerging = reMerging.Match(dataLine);
                        if (matchMerging.Success)
                        {
                            var fileNumber = int.Parse(matchMerging.Groups["MergeFile"].Value);
                            if (fileNumber > filesMerged)
                                filesMerged = fileNumber;


                            if (totalFiles > 0)
                            {
                                progressSubtask = ComputeIncrementalProgress(0, 75, filesMerged / (float)totalFiles * 100);
                            }
                            continue;
                        }

                        if (dataLine.StartsWith("Repopulating the sequence collection"))
                        {
                            progressSubtask = 85;
                            continue;
                        }

                        if (dataLine.StartsWith("Writing merged file"))
                        {
                            progressSubtask = 95;
                            continue;
                        }

                        if (dataLine.StartsWith("Total time to merge"))
                        {
                            progressSubtask = 100;
                        }
                    }
                }

                var progressOverall = ComputeIncrementalProgress(PROGRESS_PHRP_DONE, PROGRESS_PEPTIDE_PROPHET_OR_MZID_MERGE_DONE, progressSubtask);

                if (progressOverall > mProgress)
                {
                    mProgress = progressOverall;
                    UpdateStatusRunning(mProgress);
                }
            }
            catch (Exception ex)
            {
                LogWarning("Error parsing MzidMerger console output file: " + ex.Message);
            }
        }

        /// <summary>
        /// Perform peptide hit extraction for SEQUEST data
        /// </summary>
        /// <returns>CloseOutType representing success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType PerformPeptideExtraction()
        {
            var pepExtractTool = new clsPeptideExtractWrapper(mMgrParams, mJobParams, ref mStatusTools);
            RegisterEvents(pepExtractTool);

            // Run the extractor
            if (mDebugLevel > 3)
            {
                LogDebug("clsExtractToolRunner.PerformPeptideExtraction(); Starting peptide extraction");
            }

            try
            {
                var result = pepExtractTool.PerformExtraction();

                if (result != CloseOutType.CLOSEOUT_SUCCESS && result != CloseOutType.CLOSEOUT_NO_DATA)
                {
                    // Log error and return result calling routine handles the error appropriately
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        LogError("Error encountered during extraction");
                    }
                    else
                    {
                        LogErrorNoMessageUpdate("Error encountered during extraction");
                    }

                    return result;
                }

                // If there was a _syn.txt file created, but it contains no data, we want to clean up and exit
                if (result == CloseOutType.CLOSEOUT_NO_DATA)
                {
                    // Storing "No results above threshold" in mMessage will result in the job being assigned state No Export (14) in DMS
                    // See stored procedure UpdateJobState
                    LogError(NO_RESULTS_ABOVE_THRESHOLD);
                    return result;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception running extraction tool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private void RegisterPHRPEvents(EventNotifier phrp)
        {
            RegisterEvents(phrp);

            // Handle progress events with PHRP_ProgressChanged
            phrp.ProgressUpdate -= ProgressUpdateHandler;
            phrp.ProgressUpdate += PHRP_ProgressChanged;
            phrp.SkipConsoleWriteIfNoProgressListener = true;

        }

        private CloseOutType RunMzidMerger(string mzidFilenameMatchSpec, string combinedMzidFileName)
        {

            try
            {

                if (string.IsNullOrWhiteSpace(mzidFilenameMatchSpec))
                {
                    LogError("mzidFilenameMatchSpec is empty; unable to run the MzidMerger");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Make sure the .mzid files exist
                var workDir = new DirectoryInfo(mWorkDir);
                var mzidFiles = workDir.GetFiles(mzidFilenameMatchSpec);

                if (mzidFiles.Length == 0)
                {
                    LogError(string.Format(
                                 "Working directory does not have any files matching {0}; unable to run the MzidMerger", mzidFilenameMatchSpec));
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                mMzidMergerConsoleOutputFilePath = Path.Combine(mWorkDir, "MzidMergerOutput.txt");

                var progLoc = DetermineProgramLocation("MzidMergerProgLoc", "MzidMerger.exe");

                // Verify that program file exists
                if (!File.Exists(progLoc))
                {
                    // The error has already been logged (and mMessage was updated)
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Set up and execute a program runner to run the MzidMerger

                var arguments = " -inDir " + PossiblyQuotePath(mWorkDir) +
                                " -filter " + PossiblyQuotePath(mzidFilenameMatchSpec) +
                                " -keepOnlyBestResults" +
                                " -out " + PossiblyQuotePath(combinedMzidFileName);

                if (mDebugLevel >= 1)
                {
                    LogDebug(progLoc + " " + arguments);
                }

                var cmdRunner = new clsRunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = mMzidMergerConsoleOutputFilePath
                };
                RegisterEvents(cmdRunner);
                cmdRunner.LoopWaiting += MzidMerger_LoopWaiting;

                // Abort MzidMerger if it runs for over 36 hours (this generally indicates that it's stuck)
                const int maxRuntimeSeconds = 36 * 60 * 60;
                var success = cmdRunner.RunProgram(progLoc, arguments, "MzidMerger", true, maxRuntimeSeconds);

                if (!success)
                {
                    LogError("Error running MzidMerger");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (cmdRunner.ExitCode != 0)
                {
                    LogError("MzidMerger runner returned a non-zero error code: " + cmdRunner.ExitCode);

                    // Parse the console output file for any lines that start with "ERROR:"

                    var consoleOutputFile = new FileInfo(mMzidMergerConsoleOutputFilePath);
                    var errorMessageFound = false;

                    if (consoleOutputFile.Exists)
                    {
                        using (var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                        {
                            while (!reader.EndOfStream)
                            {
                                var lineIn = reader.ReadLine();
                                if (string.IsNullOrWhiteSpace(lineIn))
                                    continue;

                                if (!lineIn.ToUpper().StartsWith("ERROR:"))
                                    continue;

                                LogError(lineIn);
                                errorMessageFound = true;
                            }
                        }
                    }

                    if (!errorMessageFound)
                    {
                        LogError("MzidMerger returned a non-zero exit code; unknown error");
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Make sure the key MzidMerger result file was created

                var combinedMzidFile = new FileInfo(Path.Combine(mWorkDir, combinedMzidFileName));

                if (!combinedMzidFile.Exists)
                {
                    LogError("Combined .mzid file not found: " + combinedMzidFileName);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // GZip the combined file
                var combinedMzidGzFile = GZipFile(combinedMzidFile, true);
                if (combinedMzidGzFile == null || !combinedMzidGzFile.Exists)
                {
                    // The error has already been logged
                    return CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE;
                }

                mJobParams.AddResultFileToSkip(mMzidMergerConsoleOutputFilePath);

                if (mDebugLevel >= 3)
                {
                    LogDebug("MzidMerger complete");
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Exception while running the MzidMerger: " + ex.Message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Runs PeptideHitsResultsProcessor on SEQUEST output
        /// </summary>
        /// <returns>CloseOutType representing success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType RunPhrpForSEQUEST()
        {
            CloseOutType result;
            string synFilePath;

            var phrp = new clsPepHitResultsProcWrapper(mMgrParams, mJobParams);
            RegisterPHRPEvents(phrp);

            // Run the processor
            if (mDebugLevel > 3)
            {
                LogDebug("clsExtractToolRunner.RunPhrpForSEQUEST(); Starting PHRP");
            }
            try
            {
                var targetFilePath = Path.Combine(mWorkDir, mDatasetName + "_syn.txt");
                synFilePath = string.Copy(targetFilePath);

                result = phrp.ExtractDataFromResults(targetFilePath, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_SEQUEST);

            }
            catch (Exception ex)
            {
                LogError("Exception running PHRP", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (result == CloseOutType.CLOSEOUT_NO_DATA)
            {
                // Message has already been logged
                return result;
            }

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                var msg = "Error running PHRP";
                if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                    msg += "; " + phrp.ErrMsg;
                LogWarning(msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Validate that the mass errors are within tolerance
            var paramFileName = mJobParams.GetParam("ParmFileName");
            if (!ValidatePHRPResultMassErrors(synFilePath, clsPHRPReader.ePeptideHitResultType.Sequest, paramFileName))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPhrpForXTandem()
        {
            string synFilePath;

            var phrp = new clsPepHitResultsProcWrapper(mMgrParams, mJobParams);
            RegisterPHRPEvents(phrp);

            // Run the processor
            if (mDebugLevel > 2)
            {
                LogDebug("clsExtractToolRunner.RunPhrpForXTandem(); Starting PHRP");
            }

            try
            {
                var targetFilePath = Path.Combine(mWorkDir, mDatasetName + "_xt.xml");
                synFilePath = Path.Combine(mWorkDir, mDatasetName + "_xt.txt");

                var result = phrp.ExtractDataFromResults(targetFilePath, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_XTANDEM);

                if (result == CloseOutType.CLOSEOUT_NO_DATA)
                {
                    // Message has already been logged
                    return result;
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    var msg = "Error running PHRP";
                    if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                        msg += "; " + phrp.ErrMsg;
                    LogWarning(msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception running PHRP", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Validate that the mass errors are within tolerance
            // Use input.xml for the X!Tandem parameter file
            if (!ValidatePHRPResultMassErrors(synFilePath, clsPHRPReader.ePeptideHitResultType.XTandem, "input.xml"))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPhrpForMSAlign()
        {
            string synFilePath;

            var phrp = new clsPepHitResultsProcWrapper(mMgrParams, mJobParams);
            RegisterPHRPEvents(phrp);

            // Run the processor
            if (mDebugLevel > 3)
            {
                LogDebug("clsExtractToolRunner.RunPhrpForMSAlign(); Starting PHRP");
            }

            try
            {
                // Create the Synopsis file using the _MSAlign_ResultTable.txt file
                var targetFilePath = Path.Combine(mWorkDir, mDatasetName + "_MSAlign_ResultTable.txt");
                synFilePath = Path.Combine(mWorkDir, mDatasetName + "_msalign_syn.txt");

                var result = phrp.ExtractDataFromResults(targetFilePath, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MSALIGN);

                if (result == CloseOutType.CLOSEOUT_NO_DATA)
                {
                    // Message has already been logged
                    return result;
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    var msg = "Error running PHRP";
                    if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                        msg += "; " + phrp.ErrMsg;
                    LogWarning(msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception running PHRP", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Summarize the number of PSMs in _msalign_syn.txt
            // ReSharper disable once UseImplicitlyTypedVariableEvident
            const clsPHRPReader.ePeptideHitResultType resultType = clsPHRPReader.ePeptideHitResultType.MSAlign;

            var summarizer = new clsMSGFResultsSummarizer(resultType, mDatasetName, mJob, mWorkDir);
            RegisterEvents(summarizer);

            // Monitor events for "permission was denied"
            summarizer.ErrorEvent += MSGFResultsSummarizer_ErrorHandler;

            summarizer.MSGFThreshold = clsMSGFResultsSummarizer.DEFAULT_MSGF_THRESHOLD;

            summarizer.ContactDatabase = true;
            summarizer.PostJobPSMResultsToDB = false;
            summarizer.SaveResultsToTextFile = false;
            summarizer.DatasetName = mDatasetName;

            var success = summarizer.ProcessMSGFResults();

            if (!success)
            {
                if (string.IsNullOrWhiteSpace(summarizer.ErrorMessage))
                {
                    LogError("Error summarizing the PSMs using clsMSGFResultsSummarizer");
                }
                else
                {
                    LogError("Error summarizing the PSMs: " + summarizer.ErrorMessage);
                }

                LogError("RunPhrpForMSAlign: " + mMessage);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Validate that the mass errors are within tolerance
            var paramFileName = mJobParams.GetParam("ParmFileName");
            if (!ValidatePHRPResultMassErrors(synFilePath, clsPHRPReader.ePeptideHitResultType.MSAlign, paramFileName))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPhrpForMODa(string filteredMODaResultsFilePath)
        {
            var currentStep = "Initializing";

            try
            {
                var phrp = new clsPepHitResultsProcWrapper(mMgrParams, mJobParams);
                RegisterPHRPEvents(phrp);

                // Run the processor
                if (mDebugLevel > 3)
                {
                    LogDebug("clsExtractToolRunner.RunPhrpForMODa(); Starting PHRP");
                }

                try
                {
                    // The goal:
                    //   Create the _syn.txt files from the _moda.id.txt file

                    currentStep = "Looking for the results file";

                    if (!File.Exists(filteredMODaResultsFilePath))
                    {
                        LogError("Filtered MODa results file not found: " + Path.GetFileName(filteredMODaResultsFilePath));
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    currentStep = "Running PHRP";

                    // Create the Synopsis and First Hits files using the _moda.id.txt file
                    const bool CreateFirstHitsFile = true;
                    const bool CreateSynopsisFile = true;

                    var result = phrp.ExtractDataFromResults(filteredMODaResultsFilePath, CreateFirstHitsFile, CreateSynopsisFile,
                        mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MODA);

                    if (result == CloseOutType.CLOSEOUT_NO_DATA)
                    {
                        // Message has already been logged
                        return result;
                    }

                    if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        var msg = "Error running PHRP";
                        if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                            msg += "; " + phrp.ErrMsg;
                        LogWarning(msg);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    currentStep = "Verifying results exist";

                    // Skip the _moda.id.txt file
                    mJobParams.AddResultFileToSkip(filteredMODaResultsFilePath);
                }
                catch (Exception ex)
                {
                    LogError("Exception running PHRP", ex);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Could validate that the mass errors are within tolerance
                // var paramFileName = mJobParams.GetParam("ParmFileName");
                // if (!ValidatePHRPResultMassErrors(synFilePath, clsPHRPReader.ePeptideHitResultType.MODa, paramFileName))
                //     return CloseOutType.CLOSEOUT_FAILED;
                // else
                //     return CloseOutType.CLOSEOUT_SUCCESS;

            }
            catch (Exception ex)
            {
                LogError("Error in RunPhrpForMODa at step " + currentStep, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPhrpForMODPlus(string filteredMODPlusResultsFilePath)
        {
            var currentStep = "Initializing";

            try
            {
                var phrp = new clsPepHitResultsProcWrapper(mMgrParams, mJobParams);
                RegisterPHRPEvents(phrp);

                // Run the processor
                if (mDebugLevel > 3)
                {
                    LogDebug("clsExtractToolRunner.RunPhrpForMODPlus(); Starting PHRP");
                }

                try
                {
                    // The goal:
                    //   Create the _syn.txt files from the _modp.id.txt file

                    currentStep = "Looking for the results file";

                    if (!File.Exists(filteredMODPlusResultsFilePath))
                    {
                        LogError("Filtered MODPlus results file not found: " + Path.GetFileName(filteredMODPlusResultsFilePath));
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    currentStep = "Running PHRP";

                    // Create the Synopsis file using the _modp.id.txt file
                    const bool CreateFirstHitsFile = false;
                    const bool CreateSynopsisFile = true;

                    var result = phrp.ExtractDataFromResults(filteredMODPlusResultsFilePath, CreateFirstHitsFile, CreateSynopsisFile,
                        mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MODPLUS);

                    if (result == CloseOutType.CLOSEOUT_NO_DATA)
                    {
                        // Message has already been logged
                        return result;
                    }

                    if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        var msg = "Error running PHRP";
                        if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                            msg += "; " + phrp.ErrMsg;
                        LogWarning(msg);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                }
                catch (Exception ex)
                {
                    LogError("Exception running PHRP", ex);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Could validate that the mass errors are within tolerance
                // var paramFileName = mJobParams.GetParam("ParmFileName");
                // if (!ValidatePHRPResultMassErrors(synFilePath, clsPHRPReader.ePeptideHitResultType.MODPlus, paramFileName))
                //     return CloseOutType.CLOSEOUT_FAILED;
                // else
                //     return CloseOutType.CLOSEOUT_SUCCESS;

            }
            catch (Exception ex)
            {
                LogError("Error in RunPhrpForMODPlus at step " + currentStep, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPhrpForMSGFPlus()
        {
            var currentStep = "Initializing";

            try
            {
                var skipPHRP = mJobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION,
                                                           clsAnalysisResourcesExtraction.JOB_PARAM_SKIP_PHRP, false);

                if (skipPHRP)
                {
                    LogMessage("Skipping PHRP since the results directory already has up-to-date PHRP files");

                    // Zip the MSGFPlus_ConsoleOutput files (if they exist; they probably don't)
                    ZipConsoleOutputFiles(false);
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                var phrp = new clsPepHitResultsProcWrapper(mMgrParams, mJobParams);
                RegisterPHRPEvents(phrp);

                // Run the processor
                if (mDebugLevel > 3)
                {
                    LogDebug("clsExtractToolRunner.RunPhrpForMSGFPlus(); Starting PHRP");
                }

                mProgress = PROGRESS_EXTRACTION_START;
                UpdateStatusRunning(mProgress);

                string synFilePath;
                try
                {
                    // The goal:
                    //   Create the _fht.txt and _syn.txt files from the _msgfplus.txt file (which should already have been unzipped from the _msgfplus.zip file)
                    //   or from the _msgfplus.tsv file

                    currentStep = "Determining results file type based on the results file name";

                    var splitFastaEnabled = mJobParams.GetJobParameter("SplitFasta", false);
                    var numberOfClonedSteps = 1;
                    var pepToProtMapCount = 0;

                    var skipWarned = false;

                    var targetFilePath = Path.Combine(mWorkDir, mDatasetName + "_msgfplus.txt");
                    CloseOutType result;
                    if (!File.Exists(targetFilePath))
                    {
                        // Processing MS-GF+ results, work with .tsv files

                        if (splitFastaEnabled)
                        {
                            numberOfClonedSteps = mJobParams.GetJobParameter("NumberOfClonedSteps", 0);
                        }

                        var skipProteinMods = mJobParams.GetJobParameter("SkipProteinMods", false);

                        for (var iteration = 1; iteration <= numberOfClonedSteps; iteration++)
                        {

                            currentStep = "Verifying that .tsv files exist; iteration " + iteration;

                            string suffixToAdd;

                            if (splitFastaEnabled)
                            {
                                suffixToAdd = "_Part" + iteration;
                            }
                            else
                            {
                                suffixToAdd = string.Empty;
                            }

                            var toolNameTag = "_msgfplus";
                            targetFilePath = Path.Combine(mWorkDir, mDatasetName + toolNameTag + suffixToAdd + ".tsv");

                            if (!File.Exists(targetFilePath))
                            {
                                var targetFilePathAlt = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(targetFilePath, "Dataset_msgfdb.txt");
                                if (File.Exists(targetFilePathAlt))
                                {
                                    targetFilePath = targetFilePathAlt;
                                    toolNameTag = "_msgfdb";
                                }
                            }

                            if (!File.Exists(targetFilePath))
                            {
                                // Need to create the .tsv file
                                currentStep = "Creating .tsv file " + targetFilePath;

                                targetFilePath = ConvertMZIDToTSV(suffixToAdd);
                                if (string.IsNullOrWhiteSpace(targetFilePath))
                                {
                                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                                }
                            }

                            var pepToProtMapFilePath = Path.Combine(mWorkDir, mDatasetName + toolNameTag + suffixToAdd + "_PepToProtMap.txt");

                            var subTaskProgress = iteration / (float)numberOfClonedSteps * 100;
                            mProgress = ComputeIncrementalProgress(3, PROGRESS_EXTRACTION_DONE, subTaskProgress);
                            UpdateStatusRunning(mProgress);

                            if (File.Exists(pepToProtMapFilePath))
                            {
                                pepToProtMapCount++;
                                continue;
                            }

                            var skipPeptideToProteinMapping = mJobParams.GetJobParameter("SkipPeptideToProteinMapping", false);

                            if (skipPeptideToProteinMapping)
                            {
                                if (skipWarned)
                                    continue;

                                LogMessage("Skipping PeptideToProteinMapping since job parameter SkipPeptideToProteinMapping is True");
                                skipWarned = true;
                            }
                            else if (string.IsNullOrWhiteSpace(mGeneratedFastaFilePath))
                            {
                                if (skipWarned)
                                    continue;

                                if (skipProteinMods)
                                {
                                    LogWarning("Skipping PeptideToProteinMapping since the FASTA file is not defined; " +
                                               "this is the case because job parameter SkipProteinMods is true", true);
                                    skipWarned = true;
                                }
                                else
                                {
                                    LogError("Skipping PeptideToProteinMapping since the FASTA file is not defined; " +
                                             "job parameter SkipProteinMods is false, so this indicates a problem");
                                    skipWarned = true;
                                }
                            }
                            else
                            {
                                result = CreateMSGFPlusResultsProteinToPeptideMappingFile(targetFilePath);
                                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                                {
                                    return result;
                                }
                                pepToProtMapCount++;
                            }

                        }

                        if (splitFastaEnabled)
                        {
                            currentStep = "Merging Parallel MS-GF+ results";

                            var numberOfHitsPerScanToKeep = mJobParams.GetJobParameter("MergeResultsToKeepPerScan", 2);
                            if (numberOfHitsPerScanToKeep < 1)
                                numberOfHitsPerScanToKeep = 1;

                            // Merge the TSV files (keeping the top scoring hit (or hits) for each scan)
                            // Keys in filterPassingPeptides are peptide sequences; values indicate whether the peptide (and its associated proteins) has been written to the merged _PepToProtMap.txt file

                            currentStep = "Merging the TSV files";
                            result = ParallelMSGFPlusMergeTSVFiles(numberOfClonedSteps, numberOfHitsPerScanToKeep, out var filterPassingPeptides);

                            if (result != CloseOutType.CLOSEOUT_SUCCESS)
                            {
                                return result;
                            }

                            if (pepToProtMapCount > 0)
                            {
                                // Merge the _PepToProtMap files (making sure we don't have any duplicates, and only keeping peptides that passed the filters)
                                currentStep = "Merging the _PepToProtMap files";
                                result = ParallelMSGFPlusMergePepToProtMapFiles(numberOfClonedSteps, filterPassingPeptides);

                                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                                {
                                    return result;
                                }
                            }
                            targetFilePath = Path.Combine(mWorkDir, mDatasetName + "_msgfplus.tsv");
                        }
                    }

                    synFilePath = Path.Combine(mWorkDir, mDatasetName + "_msgfplus_syn.txt");

                    // Create the Synopsis and First Hits files using the _msgfplus.txt file
                    const bool createMSGFPlusFirstHitsFile = true;
                    const bool createMSGFPlusSynopsisFile = true;

                    result = phrp.ExtractDataFromResults(targetFilePath, createMSGFPlusFirstHitsFile, createMSGFPlusSynopsisFile,
                        mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MSGFPLUS);

                    if (result == CloseOutType.CLOSEOUT_NO_DATA)
                    {
                        // Message has already been logged
                        return result;
                    }

                    if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        var msg = "Error running PHRP";
                        if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                            msg += "; " + phrp.ErrMsg;
                        LogWarning(msg);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (splitFastaEnabled)
                    {
                        // Zip the MSGFPlus_ConsoleOutput files (if they exist)
                        ZipConsoleOutputFiles();
                    }
                    else
                    {
                        try
                        {
                            // Delete the _msgfplus.txt or _msgfplus.tsv file
                            File.Delete(targetFilePath);
                        }
                        catch (Exception)
                        {
                            // Ignore errors here
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogError("Exception running PHRP", ex);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Validate that the mass errors are within tolerance
                var paramFileName = mJobParams.GetParam("ParmFileName");
                if (!ValidatePHRPResultMassErrors(synFilePath, clsPHRPReader.ePeptideHitResultType.MSGFPlus, paramFileName))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in RunPhrpForMSGFPlus at step " + currentStep, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType RunPHRPForMSPathFinder()
        {
            var currentStep = "Initializing";

            try
            {
                var phrp = new clsPepHitResultsProcWrapper(mMgrParams, mJobParams);
                RegisterPHRPEvents(phrp);

                // Run the processor
                if (mDebugLevel > 3)
                {
                    LogDebug("clsExtractToolRunner.RunPhrpForMSPathFinder(); Starting PHRP");
                }

                var synFilePath = Path.Combine(mWorkDir, mDatasetName + "_mspath_syn.txt");

                try
                {
                    // The goal:
                    //   Create the _syn.txt files from the _IcTda.tsv file

                    currentStep = "Looking for the results file";

                    var msPathFinderResultsFilePath = Path.Combine(mWorkDir, mDatasetName + "_IcTDA.tsv");
                    if (!File.Exists(msPathFinderResultsFilePath))
                    {
                        LogError("MSPathFinder results file not found: " + Path.GetFileName(msPathFinderResultsFilePath));
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    currentStep = "Running PHRP";

                    // Create the Synopsis file using the _IcTDA.tsv file
                    const bool CreateFirstHitsFile = false;
                    const bool CreateSynopsisFile = true;

                    var result = phrp.ExtractDataFromResults(msPathFinderResultsFilePath, CreateFirstHitsFile, CreateSynopsisFile,
                        mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MSPATHFINDER);

                    if (result == CloseOutType.CLOSEOUT_NO_DATA)
                    {
                        // Message has already been logged
                        return result;
                    }

                    if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        var msg = "Error running PHRP";
                        if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                            msg += "; " + phrp.ErrMsg;
                        LogWarning(msg);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                catch (Exception ex)
                {
                    LogError("Exception running PHRP", ex);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Validate that the mass errors are within tolerance
                var paramFileName = mJobParams.GetParam("ParmFileName");
                if (!ValidatePHRPResultMassErrors(synFilePath, clsPHRPReader.ePeptideHitResultType.MSPathFinder, paramFileName))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in RunPhrpForMSPathFinder at step " + currentStep, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType RunPHRPForTopPIC()
        {
            var currentStep = "Initializing";

            try
            {
                var phrp = new clsPepHitResultsProcWrapper(mMgrParams, mJobParams);
                RegisterPHRPEvents(phrp);

                // Run the processor
                if (mDebugLevel > 3)
                {
                    LogDebug("clsExtractToolRunner.RunPHRPForTopPIC(); Starting PHRP");
                }

                var synFilePath = Path.Combine(mWorkDir, mDatasetName + "_toppic_syn.txt");

                try
                {
                    // The goal:
                    //   Create the _syn.txt files from the _TopPIC_PrSMs.txt file

                    currentStep = "Looking for the results file";

                    var topPICResultsFilePath = Path.Combine(mWorkDir, mDatasetName + "_TopPIC_PrSMs.txt");
                    if (!File.Exists(topPICResultsFilePath))
                    {
                        LogError("TopPIC results file not found: " + Path.GetFileName(topPICResultsFilePath));
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    currentStep = "Running PHRP";

                    // Create the Synopsis file using the _TopPIC_PrSMs.txt file
                    const bool CreateFirstHitsFile = false;
                    const bool CreateSynopsisFile = true;

                    var result = phrp.ExtractDataFromResults(topPICResultsFilePath, CreateFirstHitsFile, CreateSynopsisFile,
                        mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_TOPPIC);

                    if (result == CloseOutType.CLOSEOUT_NO_DATA)
                    {
                        // Message has already been logged
                        return result;
                    }

                    if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        var msg = "Error running PHRP";
                        if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                            msg += "; " + phrp.ErrMsg;
                        LogWarning(msg);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                catch (Exception ex)
                {
                    LogError("Exception running PHRP", ex);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Validate that the mass errors are within tolerance
                var paramFileName = mJobParams.GetParam("ParmFileName");
                if (!ValidatePHRPResultMassErrors(synFilePath, clsPHRPReader.ePeptideHitResultType.TopPIC, paramFileName))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in RunPHRPForTopPIC at step " + currentStep, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void ZipConsoleOutputFiles(bool warnFilesNotFound = true)
        {
            var workingDirectory = new DirectoryInfo(mWorkDir);
            var consoleOutputFiles = new List<string>();

            var consoleOutputFilesDir = new DirectoryInfo(Path.Combine(workingDirectory.FullName, "ConsoleOutputFiles"));
            consoleOutputFilesDir.Create();

            foreach (var consoleOutputFile in workingDirectory.GetFiles("MSGFPlus_ConsoleOutput_Part*.txt"))
            {
                var targetPath = Path.Combine(consoleOutputFilesDir.FullName, consoleOutputFile.Name);
                consoleOutputFile.MoveTo(targetPath);
                consoleOutputFiles.Add(consoleOutputFile.Name);
            }

            if (consoleOutputFiles.Count == 0)
            {
                if (warnFilesNotFound)
                {
                    LogWarning("MS-GF+ console output files not found");
                }

                consoleOutputFilesDir.Refresh();
                if (consoleOutputFilesDir.GetFileSystemInfos().Length == 0)
                {
                    consoleOutputFilesDir.Delete();
                }

                return;
            }

            var zippedConsoleOutputFilePath = Path.Combine(workingDirectory.FullName, "MSGFPlus_ConsoleOutput_Files.zip");
            if (!mDotNetZipTools.ZipDirectory(consoleOutputFilesDir.FullName, zippedConsoleOutputFilePath))
            {
                LogError("Problem zipping the console output file; will not delete the separate copies from the transfer directory");
                return;
            }

            var transferDirPath = GetTransferFolderPath();

            if (string.IsNullOrWhiteSpace(transferDirPath))
            {
                // Error has already been logged
                return;
            }

            if (string.IsNullOrWhiteSpace(mResultsDirectoryName))
            {
                // Ignore error; will be logged in function
                return;
            }

            foreach (var consoleOutputFile in consoleOutputFiles)
            {
                var targetPath = Path.Combine(transferDirPath, mDatasetName, mResultsDirectoryName, consoleOutputFile);
                mJobParams.AddServerFileToDelete(targetPath);
            }
        }

        private CloseOutType RunPhrpForInSpecT()
        {
            string synFilePath;

            var phrp = new clsPepHitResultsProcWrapper(mMgrParams, mJobParams);
            RegisterPHRPEvents(phrp);

            // Run the processor
            if (mDebugLevel > 3)
            {
                LogDebug("clsExtractToolRunner.RunPhrpForInSpecT(); Starting PHRP");
            }

            try
            {
                // The goal:
                //   Get the _fht.txt and _FScore_fht.txt files from the _inspect.txt file in the _inspect_fht.zip file
                //   Get the other files from the _inspect.txt file in the_inspect.zip file

                // Extract _inspect.txt from the _inspect_fht.zip file
                var targetFilePath = Path.Combine(mWorkDir, mDatasetName + "_inspect_fht.zip");
                var success = UnzipFile(targetFilePath);

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Create the First Hits files using the _inspect.txt file
                var createInspectFirstHitsFile = true;
                var createInspectSynopsisFile = false;
                targetFilePath = Path.Combine(mWorkDir, mDatasetName + "_inspect.txt");

                // ReSharper disable ConditionIsAlwaysTrueOrFalse
                var result = phrp.ExtractDataFromResults(targetFilePath, createInspectFirstHitsFile, createInspectSynopsisFile,
                    mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_INSPECT);
                // ReSharper enable ConditionIsAlwaysTrueOrFalse

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    var msg = "Error running PHRP";
                    if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                        msg += "; " + phrp.ErrMsg;
                    LogWarning(msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Delete the _inspect.txt file
                File.Delete(targetFilePath);

                // Extract _inspect.txt from the _inspect.zip file
                targetFilePath = Path.Combine(mWorkDir, mDatasetName + "_inspect.zip");
                success = UnzipFile(targetFilePath);

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Create the Synopsis files using the _inspect.txt file
                createInspectFirstHitsFile = false;
                createInspectSynopsisFile = true;
                targetFilePath = Path.Combine(mWorkDir, mDatasetName + "_inspect.txt");
                synFilePath = Path.Combine(mWorkDir, mDatasetName + "_inspect_syn.txt");

                // ReSharper disable ConditionIsAlwaysTrueOrFalse
                result = phrp.ExtractDataFromResults(targetFilePath, createInspectFirstHitsFile, createInspectSynopsisFile,
                    mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_INSPECT);
                // ReSharper restore ConditionIsAlwaysTrueOrFalse

                if (result == CloseOutType.CLOSEOUT_NO_DATA)
                {
                    // Message has already been logged
                    return result;
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    var msg = "Error running PHRP";
                    if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                        msg += "; " + phrp.ErrMsg;
                    LogWarning(msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                try
                {
                    // Delete the _inspect.txt file
                    File.Delete(targetFilePath);
                }
                catch (Exception)
                {
                    // Ignore errors here
                }
            }
            catch (Exception ex)
            {
                LogError("Exception running PHRP", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Validate that the mass errors are within tolerance
            var paramFileName = mJobParams.GetParam("ParmFileName");
            if (!ValidatePHRPResultMassErrors(synFilePath, clsPHRPReader.ePeptideHitResultType.Inspect, paramFileName))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPeptideProphet()
        {
            const int SYN_FILE_MAX_SIZE_MB = 200;
            const string PEP_PROPHET_RESULT_FILE_SUFFIX = "_PepProphet.txt";

            string pepProphetOutputFilePath;

            var result = CloseOutType.CLOSEOUT_SUCCESS;

            bool success;

            var ignorePeptideProphetErrors = mJobParams.GetJobParameter("IgnorePeptideProphetErrors", false);

            var progLoc = mMgrParams.GetParam("PeptideProphetRunnerProgLoc");

            // verify that program file exists
            if (!File.Exists(progLoc))
            {
                if (progLoc.Length == 0)
                {
                    LogError("Manager parameter PeptideProphetRunnerProgLoc is not defined in the Manager Control DB");
                }
                else
                {
                    LogError("Cannot find PeptideProphetRunner program file: " + progLoc);
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var peptideProphet = new clsPeptideProphetWrapper(progLoc);
            RegisterEvents(peptideProphet);
            peptideProphet.PeptideProphetRunning += PeptideProphet_PeptideProphetRunning;

            if (mDebugLevel >= 3)
            {
                LogDebug("clsExtractToolRunner.RunPeptideProphet(); Starting Peptide Prophet");
            }

            var synFilePath = Path.Combine(mWorkDir, mDatasetName + "_syn.txt");

            // Check to see if the synopsis file exists
            var synopsisFile = new FileInfo(synFilePath);
            if (!synopsisFile.Exists)
            {
                LogError("clsExtractToolRunner.RunPeptideProphet(); Syn file " + synFilePath + " not found; unable to run peptide prophet");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            List<string> splitFileList;

            // Check the size of the Syn file
            // If it is too large, we will need to break it up into multiple parts, process each part separately, and then combine the results
            var parentSynFileSizeMB = clsGlobal.BytesToMB(synopsisFile.Length);
            if (parentSynFileSizeMB <= SYN_FILE_MAX_SIZE_MB)
            {
                splitFileList = new List<string>
                {
                    synopsisFile.FullName
                };
            }
            else
            {
                if (mDebugLevel >= 2)
                {
                    LogDebug(
                        "Synopsis file is " + parentSynFileSizeMB.ToString("0.0") +
                        " MB, which is larger than the maximum size for peptide prophet (" + SYN_FILE_MAX_SIZE_MB +
                        " MB); splitting into multiple sections");
                }

                // File is too large; split it into multiple chunks
                success = SplitFileRoundRobin(synopsisFile.FullName, SYN_FILE_MAX_SIZE_MB * 1024 * 1024, true, out splitFileList);

                if (success)
                {
                    if (mDebugLevel >= 3)
                    {
                        LogDebug("Synopsis file was split into " + splitFileList.Count + " sections by SplitFileRoundRobin");
                    }
                }
                else
                {
                    var msg = "Error splitting synopsis file that is over " + SYN_FILE_MAX_SIZE_MB + " MB in size";

                    if (ignorePeptideProphetErrors)
                    {
                        msg += "; Ignoring the error since 'IgnorePeptideProphetErrors' = True";
                        LogWarning(msg);
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }

                    LogError(msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Setup Peptide Prophet and run for each file in fileList
            foreach (var splitFilePath in splitFileList)
            {
                peptideProphet.InputFile = splitFilePath;
                peptideProphet.Enzyme = "tryptic";
                peptideProphet.OutputFolderPath = mWorkDir;
                peptideProphet.DebugLevel = mDebugLevel;

                var splitSynFile = new FileInfo(splitFilePath);
                var synFileNameAndSize = string.Format("{0} (file size = {1:F2} MB", splitSynFile.Name, clsGlobal.BytesToMB(splitSynFile.Length));
                if (splitFileList.Count > 1)
                {
                    synFileNameAndSize += "; parent syn file is " + parentSynFileSizeMB.ToString("0.00") + " MB)";
                }
                else
                {
                    synFileNameAndSize += ")";
                }

                if (mDebugLevel >= 1)
                {
                    LogDebug("Running peptide prophet on file " + synFileNameAndSize);
                }

                result = peptideProphet.CallPeptideProphet();

                if (result == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Make sure the Peptide Prophet output file was actually created
                    pepProphetOutputFilePath = Path.Combine(peptideProphet.OutputFolderPath,
                                                            Path.GetFileNameWithoutExtension(splitFilePath) + PEP_PROPHET_RESULT_FILE_SUFFIX);

                    if (mDebugLevel >= 3)
                    {
                        LogDebug("Peptide prophet processing complete; checking for file " + pepProphetOutputFilePath);
                    }

                    if (!File.Exists(pepProphetOutputFilePath))
                    {
                        LogError("clsExtractToolRunner.RunPeptideProphet(); Peptide Prophet output file not found for synopsis file " +
                                 synFileNameAndSize);

                        if (ignorePeptideProphetErrors)
                        {
                            LogWarning("Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' = True");
                        }
                        else
                        {
                            LogWarning(
                                "To ignore this error, update this job to use a settings file that has 'IgnorePeptideProphetErrors' set to True");
                            result = CloseOutType.CLOSEOUT_FAILED;
                            break;
                        }
                    }
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        LogError("Error running Peptide Prophet: " + peptideProphet.ErrMsg);
                        LogWarning("Input file: " + synFileNameAndSize);
                    }
                    else
                    {
                        LogErrorNoMessageUpdate("Error running Peptide Prophet on file " + synFileNameAndSize + ": " + peptideProphet.ErrMsg);
                    }

                    if (ignorePeptideProphetErrors)
                    {
                        LogWarning("Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' = True");
                    }
                    else
                    {
                        result = CloseOutType.CLOSEOUT_FAILED;
                        break;
                    }
                }
            }

            if (result != CloseOutType.CLOSEOUT_SUCCESS && !ignorePeptideProphetErrors)
                return result;

            if (splitFileList.Count <= 1)
                return result;

            // Delete each of the temporary synopsis files
            DeleteTemporaryFiles(splitFileList);

            // We now need to recombine the peptide prophet result files

            // Update fileList() to have the peptide prophet result file names
            var baseName = Path.Combine(peptideProphet.OutputFolderPath, Path.GetFileNameWithoutExtension(synFilePath));

            for (var fileIndex = 0; fileIndex <= splitFileList.Count - 1; fileIndex++)
            {
                var splitFile = baseName + "_part" + (fileIndex + 1) + PEP_PROPHET_RESULT_FILE_SUFFIX;

                // Add this file to the global delete list
                mJobParams.AddResultFileToSkip(splitFile);
            }

            // Define the final peptide prophet output file name
            pepProphetOutputFilePath = baseName + PEP_PROPHET_RESULT_FILE_SUFFIX;

            if (mDebugLevel >= 2)
            {
                LogDebug(
                    "Combining " + splitFileList.Count + " separate Peptide Prophet result files to create " +
                    Path.GetFileName(pepProphetOutputFilePath));
            }

            success = InterleaveFiles(splitFileList, pepProphetOutputFilePath, true);

            // Delete each of the temporary peptide prophet result files
            DeleteTemporaryFiles(splitFileList);

            if (success)
            {
                result = CloseOutType.CLOSEOUT_SUCCESS;
            }
            else
            {
                var msg = "Error interleaving the peptide prophet result files (FileCount=" + splitFileList.Count + ")";
                if (ignorePeptideProphetErrors)
                {
                    msg += "; Ignoring the error since 'IgnorePeptideProphetErrors' = True";
                    LogWarning(msg);
                    result = CloseOutType.CLOSEOUT_SUCCESS;
                }
                else
                {
                    LogError(msg);
                    result = CloseOutType.CLOSEOUT_FAILED;
                }
            }

            return result;
        }

        /// <summary>
        /// Deletes each file in splitFileList
        /// </summary>
        /// <param name="splitFileList">Full paths to files to delete</param>
        /// <remarks></remarks>
        private void DeleteTemporaryFiles(IReadOnlyCollection<string> splitFileList)
        {
            PRISM.ProgRunner.GarbageCollectNow();

            // Delete each file in fileList
            foreach (var splitFile in splitFileList)
            {
                if (mDebugLevel >= 5)
                {
                    LogDebug("Deleting file " + splitFile);
                }

                try
                {
                    File.Delete(splitFile);
                }
                catch (Exception ex)
                {
                    LogError("Error deleting file " + Path.GetFileName(splitFile) + ": " + ex.Message, ex);
                }
            }
        }

        /// <summary>
        /// Reads each file in fileList line by line, writing the lines to combinedFilePath
        /// Can also check for a header line on the first line; if a header line is found in the first file,
        /// then the header is also written to the combined file
        /// </summary>
        /// <param name="fileList">Files to combine</param>
        /// <param name="combinedFilePath">File to create</param>
        /// <param name="lookForHeaderLine">When true, looks for a header line by checking if the first column contains a number</param>
        /// <returns>True if success; false if failure</returns>
        /// <remarks></remarks>
        private bool InterleaveFiles(IReadOnlyList<string> fileList, string combinedFilePath, bool lookForHeaderLine)
        {

            try
            {
                if (fileList == null || fileList.Count == 0)
                {
                    // Nothing to do
                    return false;
                }

                var fileCount = fileList.Count;
                var fileReaders = new StreamReader[fileCount];
                var linesRead = new int[fileCount];

                // Open each of the input files
                for (var fileIndex = 0; fileIndex <= fileCount - 1; fileIndex++)
                {
                    if (File.Exists(fileList[fileIndex]))
                    {
                        fileReaders[fileIndex] = new StreamReader(new FileStream(fileList[fileIndex], FileMode.Open, FileAccess.Read, FileShare.Read));
                    }
                    else
                    {
                        // File not found; unable to continue
                        LogError("Source peptide prophet file not found, unable to continue: " + fileList[fileIndex]);
                        return false;
                    }
                }

                // Create the output file

                using (var writer = new StreamWriter(new FileStream(combinedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var totalLinesRead = 0;
                    var continueReading = true;

                    while (continueReading)
                    {
                        var totalLinesReadSaved = totalLinesRead;

                        for (var fileIndex = 0; fileIndex <= fileCount - 1; fileIndex++)
                        {
                            if (fileReaders[fileIndex].EndOfStream)
                                continue;

                            var lineIn = fileReaders[fileIndex].ReadLine();

                            linesRead[fileIndex] += 1;
                            totalLinesRead += 1;

                            if (lineIn == null)
                                continue;

                            var processLine = true;

                            if (linesRead[fileIndex] == 1 && lookForHeaderLine && lineIn.Length > 0)
                            {
                                // check for a header line
                                var splitLine = lineIn.Split(new[] { '\t' }, 2);

                                if (splitLine.Length > 0 && !double.TryParse(splitLine[0], out _))
                                {
                                    // first column does not contain a number; this must be a header line
                                    // write the header to the output file (provided fileIndex == 0)
                                    if (fileIndex == 0)
                                    {
                                        writer.WriteLine(lineIn);
                                    }
                                    processLine = false;
                                }
                            }

                            if (processLine)
                            {
                                writer.WriteLine(lineIn);
                            }
                        }

                        if (totalLinesRead == totalLinesReadSaved)
                        {
                            continueReading = false;
                        }
                    }

                    // Close the input files
                    for (var fileIndex = 0; fileIndex <= fileCount - 1; fileIndex++)
                    {
                        fileReaders[fileIndex].Dispose();
                    }

                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in clsExtractToolRunner.InterleaveFiles", ex);
                return false;
            }

        }

        /// <summary>
        /// Reads srcFilePath line-by-line and splits into multiple files such that none of the output
        /// files has length greater than maxSizeBytes. Can also check for a header line on the first line;
        /// if a header line is found, all of the split files will be assigned the same header line
        /// </summary>
        /// <param name="srcFilePath">FilePath to parse</param>
        /// <param name="maxSizeBytes">Maximum size of each file</param>
        /// <param name="lookForHeaderLine">When true, looks for a header line by checking if the first column contains a number</param>
        /// <param name="splitFileList">Output array listing the full paths to the split files that were created</param>
        /// <returns>True if success, false if failure</returns>
        /// <remarks></remarks>
        private bool SplitFileRoundRobin(string srcFilePath, long maxSizeBytes, bool lookForHeaderLine, out List<string> splitFileList)
        {
            bool success;
            splitFileList = new List<string>();

            try
            {
                var sourceFile = new FileInfo(srcFilePath);
                if (!sourceFile.Exists)
                {
                    LogError("File not found: " + sourceFile.FullName);
                    return false;
                }

                if (sourceFile.Length <= maxSizeBytes)
                {
                    // File is already less than the limit
                    splitFileList.Add(sourceFile.FullName);

                    return true;
                }

                if (string.IsNullOrWhiteSpace(sourceFile.DirectoryName))
                {
                    LogError("Cannot determine the parent directory of " + sourceFile.FullName);
                    return false;
                }

                // Determine the number of parts to split the file into
                var splitFileCount = (int)Math.Ceiling(sourceFile.Length / (float)maxSizeBytes);

                if (splitFileCount < 2)
                {
                    // This code should never be reached; we'll set splitFileCount to 2
                    splitFileCount = 2;
                }

                // Open the input file
                using (var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    // Create each of the output files
                    var outputFileWriters = new StreamWriter[splitFileCount];

                    var baseName = Path.Combine(sourceFile.DirectoryName, Path.GetFileNameWithoutExtension(sourceFile.Name));

                    for (var fileIndex = 0; fileIndex <= splitFileCount - 1; fileIndex++)
                    {
                        splitFileList[fileIndex] = baseName + "_part" + (fileIndex + 1) + Path.GetExtension(sourceFile.Name);
                        outputFileWriters[fileIndex] =
                            new StreamWriter(new FileStream(splitFileList[fileIndex], FileMode.Create, FileAccess.Write, FileShare.Read));
                    }

                    var linesRead = 0;
                    var targetFileIndex = 0;

                    while (!reader.EndOfStream)
                    {
                        var lineIn = reader.ReadLine();
                        linesRead += 1;

                        if (lineIn == null)
                            continue;

                        var processLine = true;

                        if (linesRead == 1 && lookForHeaderLine && lineIn.Length > 0)
                        {
                            // Check for a header line
                            var splitLine = lineIn.Split(new[] { '\t' }, 2);

                            if (splitLine.Length > 0 && !double.TryParse(splitLine[0], out _))
                            {
                                // First column does not contain a number; this must be a header line
                                // Write the header to each output file
                                for (var fileIndex = 0; fileIndex <= splitFileCount - 1; fileIndex++)
                                {
                                    outputFileWriters[fileIndex].WriteLine(lineIn);
                                }
                                processLine = false;
                            }
                        }

                        if (processLine)
                        {
                            outputFileWriters[targetFileIndex].WriteLine(lineIn);
                            targetFileIndex += 1;
                            if (targetFileIndex == splitFileCount)
                                targetFileIndex = 0;
                        }
                    }


                    // Close the output files
                    for (var fileIndex = 0; fileIndex <= splitFileCount - 1; fileIndex++)
                    {
                        outputFileWriters[fileIndex].Flush();
                        outputFileWriters[fileIndex].Dispose();
                    }

                }

                success = true;
            }
            catch (Exception ex)
            {
                LogError("Exception in clsExtractToolRunner.SplitFileRoundRobin", ex);
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            var toolVersionInfo = string.Empty;
            var toolFiles = new List<FileInfo>();

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of the PeptideHitResultsProcessor

            try
            {
                var progLoc = mMgrParams.GetParam("PHRPProgLoc");
                var phrpProgDir = new DirectoryInfo(progLoc);

                // verify that program file exists
                if (!phrpProgDir.Exists)
                {
                    LogError("PHRP directory not found at " + progLoc);
                    return false;
                }

                var phrpDLL = new FileInfo(Path.Combine(phrpProgDir.FullName, "PeptideHitResultsProcessor.dll"));
                if (!phrpDLL.Exists)
                {
                    LogError("PHRP DLL not found: " + phrpDLL.FullName);
                    return false;
                }

                toolFiles.Add(phrpDLL);
                mToolVersionUtilities.StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, phrpDLL.FullName);
            }
            catch (Exception ex)
            {
                LogError("Exception determining Assembly info for the PeptideHitResultsProcessor: " + ex.Message, ex);
                return false;
            }

            var splitFastaEnabled = mJobParams.GetJobParameter("SplitFasta", false);
            if (splitFastaEnabled)
            {
                // Lookup the version of the MzidMerger

                try
                {
                    var progLoc = mMgrParams.GetParam("MzidMergerProgLoc");
                    var mzidMergerDir = new DirectoryInfo(progLoc);

                    // verify that program file exists
                    if (!mzidMergerDir.Exists)
                    {
                        LogError("MzidMerger directory not found at " + progLoc);
                        return false;
                    }

                    var mzidMerger = new FileInfo(Path.Combine(mzidMergerDir.FullName, "MzidMerger.exe"));
                    if (!mzidMerger.Exists)
                    {
                        LogError("MzidMerger not found: " + mzidMerger.FullName);
                        return false;
                    }

                    toolFiles.Add(mzidMerger);
                    mToolVersionUtilities.StoreToolVersionInfoOneFile64Bit(ref toolVersionInfo, mzidMerger.FullName);
                }
                catch (Exception ex)
                {
                    LogError("Exception determining Assembly info for the PeptideHitResultsProcessor: " + ex.Message, ex);
                    return false;
                }
            }

            if (mJobParams.GetParam("ResultType") == clsAnalysisResources.RESULT_TYPE_SEQUEST)
            {
                // SEQUEST result type

                // Lookup the version of the PeptideFileExtractor
                if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "PeptideFileExtractor"))
                {
                    return false;
                }

                // Lookup the version of the PeptideProphetRunner

                var peptideProphetRunnerLoc = mMgrParams.GetParam("PeptideProphetRunnerProgLoc");
                var peptideProphetRunner = new FileInfo(peptideProphetRunnerLoc);

                if (peptideProphetRunner.Exists)
                {
                    toolFiles.Add(peptideProphetRunner);

                    // Lookup the version of the PeptideProphetRunner
                    var success = mToolVersionUtilities.StoreToolVersionInfoOneFile32Bit(ref toolVersionInfo, peptideProphetRunner.FullName);
                    if (!success)
                        return false;

                    if (!string.IsNullOrWhiteSpace(peptideProphetRunner.DirectoryName))
                    {
                        // Lookup the version of the PeptideProphetLibrary
                        var pepProphetDLL = new FileInfo(Path.Combine(peptideProphetRunner.DirectoryName, "PeptideProphetLibrary.dll"));
                        toolFiles.Add(pepProphetDLL);
                    }
                }
            }

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message, ex);
                return false;
            }
        }

        private bool ValidatePHRPResultMassErrors(string inputFilePath, clsPHRPReader.ePeptideHitResultType resultType,
            string searchEngineParamFileName)
        {
            bool success;

            try
            {
                var oValidator = new clsPHRPMassErrorValidator(mDebugLevel);
                RegisterEvents(oValidator);

                var paramFilePath = Path.Combine(mWorkDir, searchEngineParamFileName);

                success = oValidator.ValidatePHRPResultMassErrors(inputFilePath, resultType, paramFilePath);
                if (!success)
                {
                    var toolName = mJobParams.GetJobParameter("ToolName", "");

                    if (toolName.ToLower().StartsWith("inspect"))
                    {
                        // Ignore this error for inspect if running an unrestricted search
                        var paramFileName = mJobParams.GetJobParameter("ParmFileName", "");

                        // ReSharper disable once StringLiteralTypo
                        if (paramFileName.IndexOf("Unrestrictive", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            success = true;
                        }
                    }

                    if (!success)
                    {
                        if (string.IsNullOrWhiteSpace(oValidator.ErrorMessage))
                        {
                            LogError("ValidatePHRPResultMassErrors returned false");
                        }
                        else
                        {
                            LogError(oValidator.ErrorMessage);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception calling ValidatePHRPResultMassErrors", ex);
                success = false;
            }

            return success;
        }

        #endregion

        #region "Event handlers"

        private DateTime mLastPepProphetStatusLog = DateTime.MinValue;

        private void PeptideProphet_PeptideProphetRunning(string pepProphetStatus, float percentComplete)
        {
            const int PEP_PROPHET_DETAILED_LOG_INTERVAL_SECONDS = 60;
            mProgress = ComputeIncrementalProgress(PROGRESS_PHRP_DONE, PROGRESS_PEPTIDE_PROPHET_OR_MZID_MERGE_DONE, percentComplete);

            mStatusTools.UpdateAndWrite(mProgress);

            if (mDebugLevel < 3 || DateTime.UtcNow.Subtract(mLastPepProphetStatusLog).TotalSeconds < PEP_PROPHET_DETAILED_LOG_INTERVAL_SECONDS)
                return;

            mLastPepProphetStatusLog = DateTime.UtcNow;

            // Note that LogProgress uses mProgress
            LogProgress("Peptide prophet");

            if (!string.IsNullOrWhiteSpace(pepProphetStatus))
                LogDebug(pepProphetStatus);
        }

        private DateTime mLastPHRPStatusLog = DateTime.MinValue;

        private void PHRP_ProgressChanged(string taskDescription, float percentComplete)
        {
            const int PHRP_LOG_INTERVAL_SECONDS = 180;
            const int PHRP_DETAILED_LOG_INTERVAL_SECONDS = 20;

            ComputeIncrementalProgress(PROGRESS_EXTRACTION_DONE, PROGRESS_PHRP_DONE, percentComplete);
            mStatusTools.UpdateAndWrite(mProgress);

            if (mDebugLevel < 1)
                return;

            if (DateTime.UtcNow.Subtract(mLastPHRPStatusLog).TotalSeconds >= PHRP_DETAILED_LOG_INTERVAL_SECONDS && mDebugLevel >= 3 ||
                DateTime.UtcNow.Subtract(mLastPHRPStatusLog).TotalSeconds >= PHRP_LOG_INTERVAL_SECONDS)
            {
                mLastPHRPStatusLog = DateTime.UtcNow;
                LogDebug("Running PHRP: " + taskDescription + "; " + percentComplete + "% complete");
            }
        }

        private void MSGFPlusUtils_ErrorEvent(string errorMsg, Exception ex)
        {
            mMSGFPlusUtilsError = true;
        }

        /// <summary>
        /// Event handler for the MSGResultsSummarizer
        /// </summary>
        /// <param name="errorMessage"></param>
        /// <param name="ex"></param>
        private void MSGFResultsSummarizer_ErrorHandler(string errorMessage, Exception ex)
        {
            if (Message.ToLower().Contains("permission was denied"))
            {
                LogErrorToDatabase(errorMessage);
            }

        }

        private DateTime mLastMzidMergerUpdate = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event for MzidMerger
        /// </summary>
        /// <remarks></remarks>
        private void MzidMerger_LoopWaiting()
        {
            // Update the status by parsing the PHRP console output file every 20 seconds
            if (DateTime.UtcNow.Subtract(mLastMzidMergerUpdate).TotalSeconds >= 20)
            {
                mLastMzidMergerUpdate = DateTime.UtcNow;
                ParseMzidMergerConsoleOutputFile();
            }
        }

        #endregion
    }
}
