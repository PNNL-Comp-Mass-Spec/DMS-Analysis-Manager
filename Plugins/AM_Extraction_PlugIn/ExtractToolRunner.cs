//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2008, Battelle Memorial Institute
// Created 10/10/2008
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerMSGFDBPlugIn;
using MSGFResultsSummarizer;
using PHRPReader;
using PRISM;
using PRISM.Logging;
using PRISMDatabaseUtils;

namespace AnalysisManagerExtractionPlugin
{
    /// <summary>
    /// Primary class for controlling data extraction
    /// </summary>
    public class ExtractToolRunner : AnalysisToolRunnerBase
    {
        // ReSharper disable once CommentTypo
        // Ignore Spelling: ascore, fdr, moda, MODa, modp, msgfdb, mspath, nal
        // Ignore Spelling: parm, Phrp, Prot, tda, toppic, tryptic, tsv, Txt, Utils, xmx

        public const int PROGRESS_EXTRACTION_START = 3;

        /// <summary>
        /// SEQUEST jobs have an extract extraction step, where progress will be between 0% and 33% complete
        /// MSGFPlus_SplitFasta jobs will call ConvertMZIDToTSV for each .mzid file, during which progress will update from 3% to 33%
        /// </summary>
        private const int PROGRESS_EXTRACTION_DONE = 33;

        /// <summary>
        /// For all tools, progress will be between 33% and 66% complete while PHRP is running
        /// For SEQUEST, we also run Peptide Prophet, during which progress will be between 66% and 100%
        /// For SplitFasta MS-GF+ jobs, we merge .mzid files, during which progress will be between 66% and 100%
        /// </summary>
        private const int PROGRESS_PHRP_DONE = 66;

        private const int PROGRESS_PEPTIDE_PROPHET_OR_MZID_MERGE_DONE = 90;

        private const int PROGRESS_COMPLETE = 100;

        private const string MODa_JAR_NAME = "moda.jar";
        private const string MODa_FILTER_JAR_NAME = "anal_moda.jar";

        private const string MODPlus_JAR_NAME = "modp_pnnl.jar";
        private const string MODPlus_FILTER_JAR_NAME = "tda_plus.jar";

        private MSGFPlusUtils mMSGFPlusUtils;
        private bool mMSGFPlusUtilsError;

        private string mGeneratedFastaFilePath;

        private string mMzidMergerConsoleOutputFilePath;

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
                    LogDebug("AnalysisToolRunnerDeconPeakDetector.RunTool(): Enter");
                }

                // Store the AnalysisManager version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false for Data Extraction");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

                // Note that job parameter "GeneratedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
                // However, if job parameter SkipProteinMods is true, the FASTA file will not have been retrieved
                var fastaFileName = mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName");

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

                var resultTypeName = AnalysisResources.GetResultType(mJobParams);

                switch (resultTypeName)
                {
                    case AnalysisResources.RESULT_TYPE_DIANN:
                        // Run PHRP
                        currentAction = "running peptide hits result processor for DIA-NN";
                        result = RunPhrpForDiaNN();
                        break;

                    case AnalysisResources.RESULT_TYPE_INSPECT:
                        // Run PHRP
                        currentAction = "running peptide hits result processor for Inspect";
                        result = RunPhrpForInSpecT();
                        break;

                    case AnalysisResources.RESULT_TYPE_MAXQUANT:
                        // Run PHRP
                        currentAction = "running peptide hits result processor for MaxQuant";
                        result = RunPHRPForMaxQuant();
                        break;

                    case AnalysisResources.RESULT_TYPE_MODA:
                        // Convert the MODa results to a tab-delimited file; do not filter out the reversed-hit proteins
                        result = ConvertMODaResultsToTxt(out var filteredMODaResultsFileName, true);

                        if (result != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                            break;
                        }

                        // Run PHRP
                        currentAction = "running peptide hits result processor for MODa";
                        result = RunPhrpForMODa(filteredMODaResultsFileName);

                        if (result != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                        }

                        // Convert the MODa results to a tab-delimited file, filter by FDR (and filter out the reverse-hit proteins)
                        result = ConvertMODaResultsToTxt(out _, false);

                        if (result != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                        }
                        break;

                    case AnalysisResources.RESULT_TYPE_MODPLUS:
                        // Convert the MODPlus results to a tab-delimited file; do not filter out the reversed-hit proteins
                        result = ConvertMODPlusResultsToTxt(out var filteredMODPlusResultsFileName, true);

                        if (result != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                            break;
                        }

                        // Run PHRP
                        currentAction = "running peptide hits result processor for MODPlus";
                        result = RunPhrpForMODPlus(filteredMODPlusResultsFileName);

                        if (result != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                        }

                        // Convert the MODPlus results to a tab-delimited file, filter by FDR (and filter out the reverse-hit proteins)
                        result = ConvertMODPlusResultsToTxt(out _, false);

                        if (result != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                        }
                        break;

                    case AnalysisResources.RESULT_TYPE_MSALIGN:
                        // Run PHRP
                        currentAction = "running peptide hits result processor for MSAlign";
                        result = RunPhrpForMSAlign();
                        break;

                    case AnalysisResources.RESULT_TYPE_MSFRAGGER:
                        // Run PHRP
                        var scriptName = mJobParams.GetParam("ToolName");

                        // Example values for currentAction:
                        //  "running peptide hits result processor for MSFragger"
                        //  "running peptide hits result processor for FragPipe"
                        //  "running peptide hits result processor for FragPipe_DataPkg"
                        currentAction = string.Format("running peptide hits result processor for {0}", scriptName);
                        result = RunPhrpForMSFragger();
                        break;

                    case AnalysisResources.RESULT_TYPE_MSGFPLUS:
                        // Run PHRP

                        // Note that this plugin does not summarize the number of PSMs for MS-GF+ jobs
                        // That task is performed by method SummarizeMSGFResults in the MSGF plugin (project AnalysisManagerMSGFPlugin)

                        currentAction = "running peptide hits result processor for MS-GF+";
                        result = RunPhrpForMSGFPlus();

                        var splitFastaEnabled = mJobParams.GetJobParameter("SplitFasta", false);

                        if (result == CloseOutType.CLOSEOUT_SUCCESS && splitFastaEnabled)
                        {
                            result = RunMzidMerger(Dataset + "_msgfplus_Part*.mzid", Dataset + "_msgfplus.mzid");
                        }
                        break;

                    case AnalysisResources.RESULT_TYPE_MSPATHFINDER:
                        // Run PHRP
                        currentAction = "running peptide hits result processor for MSPathFinder";
                        result = RunPHRPForMSPathFinder();
                        break;

                    case AnalysisResources.RESULT_TYPE_SEQUEST:
                        // Run the SEQUEST Results Processor DLL
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
                            mProgress = PROGRESS_EXTRACTION_DONE; // 33% done
                            UpdateStatusRunning(mProgress);

                            currentAction = "running peptide hits result processor for SEQUEST";
                            result = RunPhrpForSEQUEST();
                        }

                        if (result == CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            mProgress = PROGRESS_PHRP_DONE; // 66% done
                            UpdateStatusRunning(mProgress);
                            currentAction = "running peptide prophet for SEQUEST";
                            RunPeptideProphet();
                        }
                        break;

                    case AnalysisResources.RESULT_TYPE_TOPPIC:
                        // Run PHRP
                        currentAction = "running peptide hits result processor for TopPIC";
                        result = RunPHRPForTopPIC();
                        break;

                    case AnalysisResources.RESULT_TYPE_XTANDEM:
                        // Run PHRP
                        currentAction = "running peptide hits result processor for X!Tandem";
                        result = RunPhrpForXTandem();
                        break;

                    default:
                        // Should never get here - invalid result type specified
                        LogError("Invalid ResultType specified: " + resultTypeName);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                if (result == CloseOutType.CLOSEOUT_NO_DATA)
                {
                    // Make sure mMessage has text; this will appear in the Completion_Message column in the database
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        // Storing "No results above threshold" in mMessage will result in the job being assigned state No Export (14) in DMS
                        // See procedure update_job_state
                        mMessage = NO_RESULTS_ABOVE_THRESHOLD;
                    }
                }

                // Possibly run AScore
                var runAscore = mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, AnalysisResourcesExtraction.JOB_PARAM_RUN_ASCORE, false);

                if (runAscore)
                {
                    LogMessage("TODO: Run AScore");
                    // result = RunAscore());
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS && result != CloseOutType.CLOSEOUT_NO_DATA)
                {
                    // Example error message: Error running peptide hits result processor for FragPipe_DataPkg; result code: 1
                    LogError("Error {0}; result code: {1}", currentAction, result);
                    processingSuccess = false;
                }
                else
                {
                    mProgress = PROGRESS_COMPLETE;
                    UpdateStatusRunning(mProgress);
                    mJobParams.AddResultFileToSkip(PepHitResultsProcWrapper.PHRP_LOG_FILE_NAME);
                }

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
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
        /// Pre-scan the DIA-NN report file to determine the Run names (which should be the dataset names);
        /// Look for the longest common text in the names and construct a map of full name to shortened name
        /// </summary>
        /// <param name="reportFile">DIA-NN report.tsv file</param>
        /// <param name="baseNameByDatasetName">Output: Dictionary where keys are dataset names and values are abbreviated names</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ConstructDatasetNameMap(FileSystemInfo reportFile, out Dictionary<string, string> baseNameByDatasetName)
        {
            try
            {
                var reader = new StreamReader(new FileStream(reportFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var linesRead = 0;
                var datasetNames = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    linesRead++;

                    var lineParts = dataLine.Split('\t');

                    if (linesRead == 1)
                    {
                        if (!ValidateDiannReportFileHeaderLine(reportFile, lineParts))
                        {
                            baseNameByDatasetName = new Dictionary<string, string>();
                            return false;
                        }

                        continue;
                    }

                    // Add the dataset name, if not yet present
                    datasetNames.Add(lineParts[1]);
                }

                // Assure that the reader is closed (so that method UpdateDiannReportFile can delete the old version of the report.tsv file)
                reader.Close();

                baseNameByDatasetName = DatasetNameMapUtility.GetDatasetNameMap(datasetNames, out _, out var warnings);

                foreach (var warning in warnings)
                {
                    LogWarning("{0} (called from AnalysisToolRunnerDiaNN.ConstructDatasetNameMap)", warning.Replace("\n", "; "));
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in ConstructDatasetNameMap", ex);
                baseNameByDatasetName = new Dictionary<string, string>();
                return false;
            }
        }

        /// <summary>
        /// Convert the MODa output file to a tab-delimited text file
        /// </summary>
        /// <param name="filteredMODaResultsFileName">Output: name of the filtered results file (in the working directory)</param>
        /// <param name="keepAllResults">If true, keep all results</param>
        /// <returns>CloseOutType representing success or failure</returns>
        private CloseOutType ConvertMODaResultsToTxt(out string filteredMODaResultsFileName, bool keepAllResults)
        {
            var fdrThreshold = mJobParams.GetJobParameter("MODaFDRThreshold", 0.05f);
            var decoyPrefix = mJobParams.GetJobParameter("MODaDecoyPrefix", "Reversed_");

            const bool isModPlus = false;

            return ConvertMODaOrMODPlusResultsToTxt(fdrThreshold, decoyPrefix, isModPlus, out filteredMODaResultsFileName, keepAllResults);
        }

        private CloseOutType ConvertMODPlusResultsToTxt(out string filteredMODPlusResultsFileName, bool keepAllResults)
        {
            var fdrThreshold = mJobParams.GetJobParameter("MODPlusDecoyFilterFDR", 0.05f);
            var decoyPrefix = mJobParams.GetJobParameter("MODPlusDecoyPrefix", "Reversed_");

            const bool isModPlus = true;

            return ConvertMODaOrMODPlusResultsToTxt(fdrThreshold, decoyPrefix, isModPlus, out filteredMODPlusResultsFileName, keepAllResults);
        }

        private CloseOutType ConvertMODaOrMODPlusResultsToTxt(
            float fdrThreshold, string decoyPrefixJobParam, bool isModPlus,
            out string filteredResultsFileName, bool keepAllResults)
        {
            filteredResultsFileName = string.Empty;

            try
            {
                string toolName;
                string fileNameSuffix;

                // ReSharper disable IdentifierTypo

                string modxProgJarName;
                string modxFilterJarName;

                // ReSharper restore IdentifierTypo

                if (isModPlus)
                {
                    toolName = "MODPlus";

                    // ReSharper disable once StringLiteralTypo
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

                    // Determine the most common decoy prefix in the FASTA file
                    var decoyPrefixes = AnalysisResources.GetDefaultDecoyPrefixes();
                    var bestPrefix = new KeyValuePair<double, string>(0, string.Empty);

                    foreach (var decoyPrefix in decoyPrefixes)
                    {
                        var fractionDecoy = AnalysisResources.GetDecoyFastaCompositionStats(fastaFile, decoyPrefix, out _);

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
                        LogWarning(string.Format(
                            "Using decoy prefix {0} instead of {1} as defined by job parameter MODPlusDecoyPrefix because {2:F1}% of the proteins start with {0}",
                            bestPrefix.Value, decoyPrefixJobParam, bestPrefix.Key * 100),
                            true);

                        decoyPrefixJobParam = bestPrefix.Value;
                    }
                }

                var paramFileName = mJobParams.GetParam("ParamFileName");
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

                // javaProgLoc will typically be "C:\DMS_Programs\Java\jre11\bin\java.exe"
                var javaProgLoc = GetJavaProgLoc();

                if (string.IsNullOrWhiteSpace(javaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to the MODa or MODPlus program

                // ReSharper disable once IdentifierTypo
                var modxProgLoc = DetermineProgramLocation(toolName + "ProgLoc", modxProgJarName);

                var modXProgram = new FileInfo(modxProgLoc);

                if (string.IsNullOrWhiteSpace(modXProgram.DirectoryName))
                {
                    LogError("Cannot determine the parent directory of " + modXProgram.FullName);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Set up and execute a program runner to run anal_moda.jar or tda_plus.jar
                var arguments =
                    " -Xmx" + javaMemorySize + "M" +
                    " -jar " + Global.PossiblyQuotePath(Path.Combine(modXProgram.DirectoryName, modxFilterJarName)) +
                    " -i " + resultsFilePath;

                if (!isModPlus)
                {
                    // Processing MODa data; include the parameter file
                    arguments += " -p " + paramFilePath;
                }

                arguments += " -fdr " + fdrThreshold;
                arguments += " -d " + decoyPrefixJobParam;

                // ReSharper disable once CommentTypo

                // Example command line:
                // "C:\DMS_Programs\Java\jre11\bin\java.exe" -Xmx1000M -jar C:\DMS_Programs\MODa\anal_moda.jar
                //   -i "E:\DMS_WorkDir3\QC_Shew_13_04_pt1_1_2_45min_14Nov13_Leopard_13-05-21_moda.txt"
                //   -p "E:\DMS_WorkDir3\MODa_PartTryp_Par20ppm_Frag0pt6Da" -fdr 0.05 -d XXX_
                // "C:\DMS_Programs\Java\jre11\bin\java.exe" -Xmx1000M -jar C:\DMS_Programs\MODPlus\tda_plus.jar
                //   -i "E:\DMS_WorkDir3\QC_Shew_13_04_pt1_1_2_45min_14Nov13_Leopard_13-05-21_modp.txt"
                //   -fdr 0.05 -d Reversed_

                LogDebug(javaProgLoc + " " + arguments);

                var progRunner = new RunDosProgram(mWorkDir, mDebugLevel)
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

                filteredResultsFileName = filteredResultsFile.Name;
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
        private string ConvertMZIDToTSV(string suffixToAdd)
        {
            try
            {
                var mzidFileName = mDatasetName + "_msgfplus" + suffixToAdd + ".mzid";

                if (!File.Exists(Path.Combine(mWorkDir, mzidFileName)))
                {
                    var mzidFileNameAlternate = ReaderFactory.AutoSwitchToLegacyMSGFDBIfRequired(mzidFileName, "Dataset_msgfdb.txt");

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
                        LogError("mMSGFPlusUtilsError is true after call to ConvertMZIDToTSV");
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
                LogError("Error in ConvertMZIDToTSV", ex);
            }

            return string.Empty;
        }

        /// <summary>
        /// Create the Peptide to Protein map file for the given MS-GF+ results file
        /// </summary>
        /// <param name="resultsFileName">Results file name</param>
        /// <returns>CloseOutType representing success or failure</returns>
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
                    LogError("mMSGFPlusUtilsError is true after call to CreatePeptideToProteinMapping");
                }

                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType ParallelMSGFPlusMergeTSVFiles(int numberOfClonedSteps, int numberOfHitsPerScanToKeep, out SortedSet<string> filterPassingPeptides)
        {
            filterPassingPeptides = new SortedSet<string>();

            try
            {
                var mergedFilePath = Path.Combine(mWorkDir, mDatasetName + "_msgfplus.tsv");

                // Keys in this dictionary are column names, values are the 0-based column index
                var columnMap = new Dictionary<string, int>();

                // This dictionary keeps track of the top hit(s) for each scan/charge combo
                // Keys are scan_charge
                // Values are the MSGFPlusPSMs class, which keeps track of the top numberOfHitsPerScanToKeep hits for each scan/charge combo
                var dictionary = new Dictionary<string, MSGFPlusPSMs>();

                // This dictionary keeps track of the best score (lowest SpecEValue) for each scan/charge combo
                // Keys are scan_charge
                // Values the lowest SpecEValue for the scan/charge
                var scanChargeBestScore = new Dictionary<string, double>();

                long totalLinesProcessed = 0;
                var warningsLogged = 0;

                using var mergedFileWriter = new StreamWriter(new FileStream(mergedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                for (var iteration = 1; iteration <= numberOfClonedSteps; iteration++)
                {
                    var sourceFilePath = Path.Combine(mWorkDir, mDatasetName + "_msgfplus_Part" + iteration + ".tsv");
                    var linesRead = 0;

                    if (mDebugLevel >= 2)
                    {
                        LogDebug("Caching data from " + sourceFilePath);
                    }

                    using var reader = new StreamReader(new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        linesRead++;
                        totalLinesProcessed++;

                        if (linesRead == 1)
                        {
                            if (iteration != 1)
                                continue;

                            // Write the header line
                            mergedFileWriter.WriteLine(dataLine);

                            var headerNames = new List<string>
                            {
                                "ScanNum",
                                "Charge",
                                "Peptide",
                                "Protein",
                                "SpecEValue"
                            };

                            Global.ParseHeaderLine(columnMap, dataLine, headerNames);

                            foreach (var headerName in headerNames)
                            {
                                if (columnMap[headerName] < 0)
                                {
                                    LogError("Header {0} not found in {1}; unable to merge the MS-GF+ .tsv files", headerName, Path.GetFileName(sourceFilePath));
                                    return CloseOutType.CLOSEOUT_FAILED;
                                }
                            }
                        }
                        else
                        {
                            var splitLine = dataLine.Split('\t');

                            var scanNumber = DataTableUtils.GetColumnValue(splitLine, columnMap, "ScanNum", 0);
                            var chargeState = DataTableUtils.GetColumnValue(splitLine, columnMap, "Charge", 0);

                            var scanChargeCombo = scanNumber + "_" + chargeState;
                            var peptide = DataTableUtils.GetColumnValue(splitLine, columnMap, "Peptide");
                            var protein = DataTableUtils.GetColumnValue(splitLine, columnMap, "Protein");
                            var specEValueText = DataTableUtils.GetColumnValue(splitLine, columnMap, "SpecEValue");

                            if (!double.TryParse(specEValueText, out var specEValue))
                            {
                                if (warningsLogged < 10)
                                {
                                    LogWarning("SpecEValue was not numeric: " + specEValueText + " in " + dataLine);
                                    warningsLogged++;

                                    if (warningsLogged >= 10)
                                    {
                                        LogWarning("Additional warnings will not be logged");
                                    }
                                }

                                continue;
                            }

                            var psm = new MSGFPlusPSMs.PSMInfo
                            {
                                Peptide = peptide,
                                SpecEValue = specEValue,
                                DataLine = dataLine
                            };

                            if (dictionary.TryGetValue(scanChargeCombo, out var hitsForScan))
                            {
                                // Possibly store this value

                                var passesFilter = hitsForScan.AddPSM(psm, protein);

                                if (passesFilter && specEValue < scanChargeBestScore[scanChargeCombo])
                                {
                                    scanChargeBestScore[scanChargeCombo] = specEValue;
                                }
                            }
                            else
                            {
                                // New entry for this scan/charge combo
                                hitsForScan = new MSGFPlusPSMs(scanNumber, chargeState, numberOfHitsPerScanToKeep);
                                hitsForScan.AddPSM(psm, protein);

                                dictionary.Add(scanChargeCombo, hitsForScan);
                                scanChargeBestScore.Add(scanChargeCombo, specEValue);
                            }
                        }
                    }
                }

                if (mDebugLevel >= 2)
                {
                    LogDebug("Sorting results for " + scanChargeBestScore.Count + " lines of scan/charge combos");
                }

                // Sort the data, then write to disk
                var scansByScore = from item in scanChargeBestScore orderby item.Value select item.Key;
                var filterPassingPSMCount = 0;

                foreach (var scanChargeCombo in scansByScore)
                {
                    var hitsForScan = dictionary[scanChargeCombo];
                    var lastPeptide = string.Empty;

                    foreach (var psm in hitsForScan.PSMs)
                    {
                        mergedFileWriter.WriteLine(psm.DataLine);

                        // Add the PSM, if not yet present
                        filterPassingPeptides.Add(psm.Peptide);

                        if (!string.Equals(psm.Peptide, lastPeptide))
                        {
                            filterPassingPSMCount++;
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

                        using var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));

                        while (!reader.EndOfStream)
                        {
                            var dataLine = reader.ReadLine();

                            if (string.IsNullOrWhiteSpace(dataLine))
                                continue;

                            linesRead++;
                            totalLinesProcessed++;

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
                            var peptide = MSGFPlusPSMs.RemovePrefixAndSuffix(peptideFull);

                            if (string.Equals(lastPeptideFull, peptideFull) || filterPassingPeptides.Contains(peptide))
                            {
                                if (!string.Equals(lastPeptideFull, peptideFull))
                                {
                                    // Done processing the last peptide; we can now update pepProtMappingWritten to true for this peptide
                                    // to prevent it from getting added to the merged file again in the future

                                    if (!string.IsNullOrWhiteSpace(lastPeptideFull))
                                    {
                                        // Add the peptide, if not yet present
                                        pepProtMappingWritten.Add(lastPeptideFull);
                                    }

                                    lastPeptideFull = peptideFull;
                                    addCurrentPeptide = !pepProtMappingWritten.Contains(peptideFull);
                                }

                                // Add this peptide if we didn't already add it during a previous iteration
                                if (addCurrentPeptide)
                                {
                                    writer.WriteLine(dataLine);
                                    totalLinesToWrite++;
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

                using var reader = new StreamReader(new FileStream(mMzidMergerConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

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
        private CloseOutType PerformPeptideExtraction()
        {
            var pepExtractTool = new PeptideExtractWrapper(mMgrParams, mJobParams, ref mStatusTools);
            RegisterEvents(pepExtractTool);

            // Run the extractor
            if (mDebugLevel > 3)
            {
                LogDebug("ExtractToolRunner.PerformPeptideExtraction(); Starting peptide extraction");
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
                    // See procedure update_job_state
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

        private void RegisterPHRPEvents(IEventNotifier phrp)
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

                var arguments =
                    " -inDir " + PossiblyQuotePath(mWorkDir) +
                    " -filter " + PossiblyQuotePath(mzidFilenameMatchSpec) +
                    " -keepOnlyBestResults" +
                    " -out " + PossiblyQuotePath(combinedMzidFileName);

                if (mDebugLevel >= 1)
                {
                    LogDebug(progLoc + " " + arguments);
                }

                var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
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
                        using var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                        while (!reader.EndOfStream)
                        {
                            var lineIn = reader.ReadLine();

                            if (string.IsNullOrWhiteSpace(lineIn))
                                continue;

                            if (!lineIn.StartsWith("ERROR:", StringComparison.OrdinalIgnoreCase))
                                continue;

                            LogError(lineIn);
                            errorMessageFound = true;
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

        private CloseOutType RunPhrpForDiaNN()
        {
            var reportParquetFile = GetDiannResultsFilePath("report.parquet");
            var reportTsvFile = GetDiannResultsFilePath("report.tsv");

            FileInfo inputFile;

            if (reportParquetFile.Exists)
            {
                inputFile = reportParquetFile;
            }
            else if (reportTsvFile.Exists)
            {
                inputFile = reportTsvFile;

                // Edit the report file to remove duplicate .mzML names and shorten dataset names
                var reportUpdated = UpdateDiannReportFile(inputFile);

                if (!reportUpdated)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            else
            {
                LogError("Could not find the DIA-NN report file ({0} or {1})", reportParquetFile.Name, reportTsvFile.Name);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var synopsisFileName = mDatasetName + "_diann_syn.txt";

            var result = RunPHRPWork(
                "DIA-NN",
                inputFile.Name,
                PeptideHitResultTypes.DiaNN,
                synopsisFileName,
                false,
                true,
                string.Empty,
                out var synopsisFileNameFromPHRP);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
                return result;

            // Summarize the number of PSMs in the synopsis file
            // This is done by this class since the DiaNN script does not have an MSGF job step

            const double thresholdForMSGFSpecEValueOrPEP = ResultsSummarizer.DEFAULT_POSTERIOR_ERROR_PROBABILITY_THRESHOLD;

            return SummarizePSMs(PeptideHitResultTypes.DiaNN, synopsisFileNameFromPHRP, thresholdForMSGFSpecEValueOrPEP);
        }

        private CloseOutType RunPhrpForInSpecT()
        {
            try
            {
                // Part 1
                // Create the First Hits file

                // Extract _inspect.txt from the _inspect_fht.zip file
                var fhtZipFilePath = Path.Combine(mWorkDir, mDatasetName + "_inspect_fht.zip");
                var successUnzipFht = UnzipFile(fhtZipFilePath);

                if (!successUnzipFht)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var inputFileName = mDatasetName + "_inspect.txt";

                RunPHRPWork(
                    "Inspect",
                    inputFileName,
                    PeptideHitResultTypes.Inspect,
                    string.Empty,
                    true,
                    false);

                try
                {
                    // Delete the _inspect.txt file
                    File.Delete(Path.Combine(mWorkDir, inputFileName));
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                // Part 2
                // Create the Synopsis file

                // Extract _inspect.txt from the _inspect.zip file
                var synZipFilePath = Path.Combine(mWorkDir, mDatasetName + "_inspect.zip");
                var successUnzipSyn = UnzipFile(synZipFilePath);

                if (!successUnzipSyn)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var synFileName = mDatasetName + "_inspect_syn.txt";

                var resultSyn = RunPHRPWork(
                    "Inspect",
                    inputFileName,
                    PeptideHitResultTypes.Inspect,
                    synFileName,
                    false,
                    true);

                try
                {
                    // Delete the _inspect.txt file
                    File.Delete(Path.Combine(mWorkDir, inputFileName));
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                if (resultSyn == CloseOutType.CLOSEOUT_NO_DATA)
                {
                    // Message has already been logged
                    return resultSyn;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception running PHRP for Inspect", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPHRPForMaxQuant()
        {
            const string inputFileName = "msms.txt";
            const int numericDataColIndex = 1;

            var msmsFileHasData = AnalysisResources.ValidateFileHasData(Path.Combine(mWorkDir, inputFileName), "MaxQuant msms.txt", out var errorMessage, numericDataColIndex);

            if (!msmsFileHasData)
            {
                mMessage = errorMessage;
                LogWarning(errorMessage);
                return CloseOutType.CLOSEOUT_NO_DATA;
            }

            var synopsisFileName = mDatasetName + "_maxq_syn.txt";

            var result = RunPHRPWork(
                "MaxQuant",
                inputFileName,
                PeptideHitResultTypes.MaxQuant,
                synopsisFileName,
                false,
                true,
                string.Empty,
                out var synopsisFileNameFromPHRP);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
                return result;

            // Summarize the number of PSMs in the synopsis file
            // This is done by this class since the MaxQuant script does not have an MSGF job step

            const double thresholdForMSGFSpecEValueOrPEP = ResultsSummarizer.DEFAULT_POSTERIOR_ERROR_PROBABILITY_THRESHOLD;

            return SummarizePSMs(PeptideHitResultTypes.MaxQuant, synopsisFileNameFromPHRP, thresholdForMSGFSpecEValueOrPEP);
        }

        private CloseOutType RunPhrpForMODa(string filteredMODaResultsFileName)
        {
            return RunPHRPWork(
                "MODa",
                filteredMODaResultsFileName,
                PeptideHitResultTypes.MODa,
                string.Empty,
                true,
                true);
        }

        private CloseOutType RunPhrpForMODPlus(string filteredMODPlusResultsFileName)
        {
            return RunPHRPWork(
                "MODPlus",
                filteredMODPlusResultsFileName,
                PeptideHitResultTypes.MODPlus,
                string.Empty,
                false,
                true);
        }

        private CloseOutType RunPhrpForMSAlign()
        {
            var inputFileName = mDatasetName + "_MSAlign_ResultTable.txt";
            var synopsisFileName = mDatasetName + "_msalign_syn.txt";

            var result = RunPHRPWork(
                "MSAlign",
                inputFileName,
                PeptideHitResultTypes.MSAlign,
                synopsisFileName,
                false,
                true,
                string.Empty,
                out var synopsisFileNameFromPHRP);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
                return result;

            // Summarize the number of PSMs in the synopsis file
            // This is done by this class since the MSAlign script does not have an MSGF job step

            const double thresholdForMSGFSpecEValueOrPEP = ResultsSummarizer.DEFAULT_MSGF_SPEC_EVALUE_THRESHOLD;

            return SummarizePSMs(PeptideHitResultTypes.MSAlign, synopsisFileNameFromPHRP, thresholdForMSGFSpecEValueOrPEP);
        }

        private CloseOutType RunPhrpForMSFragger()
        {
            if (!Global.IsMatch(mDatasetName, AnalysisResources.AGGREGATION_JOB_DATASET) || AnalysisResources.IsDataPackageDataset(mDatasetName))
            {
                return RunPhrpForMSFragger(mDatasetName, mDatasetName + AnalysisResourcesExtraction.PSM_FILE_SUFFIX, true, out _, out _);
            }

            var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

            // The constructor for DataPackageInfo reads data package metadata from packed job parameters, which were created by the resource class
            var dataPackageInfo = new DataPackageInfo(dataPackageID, this);
            RegisterEvents(dataPackageInfo);

            var dataPackageDatasets = dataPackageInfo.GetDataPackageDatasets();

            var datasetIDsByExperimentGroup = DataPackageInfoLoader.GetDataPackageDatasetsByExperimentGroup(dataPackageDatasets);

            var aggregationPsmTsv = new FileInfo(Path.Combine(mWorkDir, AnalysisResources.AGGREGATION_JOB_DATASET + AnalysisResourcesExtraction.PSM_FILE_SUFFIX));

            if (aggregationPsmTsv.Exists)
            {
                return RunPhrpForMSFragger(mDatasetName, aggregationPsmTsv.Name, true, out _, out _);
            }

            if (datasetIDsByExperimentGroup.Count <= 1)
            {
                LogError("Data package experiment group count is one, but file Aggregation_psm.tsv file does not exist; unable to proceed with extraction");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Multiple experiment groups
            // Run PHRP on each _psm.tsv file (or each Dataset.tsv file)
            // Keep track of overall PSM results by merging in the PSM results from each experiment group

            // In order to accurately determine unique peptide and protein counts, we need to create a combined _psm.tsv file,
            // then call PHRP using the combined file (named Combined_Results_AllExperimentGroups_psm.tsv)

            const string COMBINED_TSV_BASE_NAME = "Combined_Results_AllExperimentGroups";

            var combinedPsmTsvFilePath = Path.Combine(mWorkDir, COMBINED_TSV_BASE_NAME + AnalysisResourcesExtraction.PSM_FILE_SUFFIX);

            var synopsisFileNames = new List<string>();
            var psmResultsOverall = new PSMResults();
            var resultOverall = CloseOutType.CLOSEOUT_SUCCESS;

            using (var combinedPsmTsvWriter = new StreamWriter(new FileStream(combinedPsmTsvFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
            {
                var groupsProcessed = 0;

                foreach (var experimentGroup in datasetIDsByExperimentGroup.Keys)
                {
                    var inputFileName = experimentGroup + AnalysisResourcesExtraction.PSM_FILE_SUFFIX;

                    var experimentGroupResult = RunPhrpForMSFragger(
                        experimentGroup,
                        inputFileName,
                        false,
                        out var synopsisFileNamesFromPHRP,
                        out var psmResults);

                    if (experimentGroupResult == CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        synopsisFileNames.AddRange(synopsisFileNamesFromPHRP);
                    }
                    else
                    {
                        resultOverall = experimentGroupResult;
                    }

                    groupsProcessed++;

                    var linesRead = 0;

                    using (var tsvReader = new StreamReader(new FileStream(Path.Combine(mWorkDir, inputFileName), FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        while (!tsvReader.EndOfStream)
                        {
                            var dataLine = tsvReader.ReadLine();

                            if (string.IsNullOrWhiteSpace(dataLine))
                                continue;

                            linesRead++;

                            if (linesRead == 1 && groupsProcessed > 1)
                            {
                                // Skip the header line since this is not the first _psm.tsv file
                                continue;
                            }

                            combinedPsmTsvWriter.WriteLine(dataLine);
                        }
                    }

                    // Delay 1.5 seconds to assure that the synopsis file for each experiment group has a different timestamp
                    Global.IdleLoop(1.5);

                    if (groupsProcessed == 1)
                    {
                        psmResultsOverall = psmResults;
                        continue;
                    }

                    psmResultsOverall.AppendResults(psmResults);
                }
            }

            if (resultOverall != CloseOutType.CLOSEOUT_SUCCESS)
                return resultOverall;

            // Process combinedPsmTsvFilePath
            var combinedTsvResult = RunPhrpForMSFragger(
                COMBINED_TSV_BASE_NAME,
                Path.GetFileName(combinedPsmTsvFilePath),
                false,
                out _,
                out var combinedTsvPsmResults);

            if (combinedTsvResult == CloseOutType.CLOSEOUT_SUCCESS)
            {
                psmResultsOverall.UniquePeptides = combinedTsvPsmResults.UniquePeptides;
                psmResultsOverall.UniqueProteins = combinedTsvPsmResults.UniqueProteins;
                psmResultsOverall.UniquePeptidesFDRFilter = combinedTsvPsmResults.UniquePeptidesFDRFilter;
                psmResultsOverall.UniqueProteinsFDRFilter = combinedTsvPsmResults.UniqueProteinsFDRFilter;
                psmResultsOverall.UniquePhosphopeptideCountFDR = combinedTsvPsmResults.UniquePhosphopeptideCountFDR;
                psmResultsOverall.UniquePhosphopeptidesCTermK = combinedTsvPsmResults.UniquePhosphopeptidesCTermK;
                psmResultsOverall.UniquePhosphopeptidesCTermR = combinedTsvPsmResults.UniquePhosphopeptidesCTermR;

                psmResultsOverall.TrypticPeptides = combinedTsvPsmResults.TrypticPeptides;
                psmResultsOverall.KeratinPeptides = combinedTsvPsmResults.KeratinPeptides;
                psmResultsOverall.TrypsinPeptides = combinedTsvPsmResults.TrypsinPeptides;
                psmResultsOverall.UniqueAcetylPeptidesFDR = combinedTsvPsmResults.UniqueAcetylPeptidesFDR;
                psmResultsOverall.UniqueUbiquitinPeptidesFDR = combinedTsvPsmResults.UniqueUbiquitinPeptidesFDR;
                psmResultsOverall.UniquePeptides = combinedTsvPsmResults.UniquePeptides;

                // Method AppendResults used UpdatePercent() to estimate the following percentages
                // Update them using the accurate results from the combined _psm.tsv file
                psmResultsOverall.PercentMSnScansNoPSM = combinedTsvPsmResults.PercentMSnScansNoPSM;
                psmResultsOverall.MissedCleavageRatio = combinedTsvPsmResults.MissedCleavageRatio;
                psmResultsOverall.MissedCleavageRatioPhospho = combinedTsvPsmResults.MissedCleavageRatioPhospho;
                psmResultsOverall.PercentPSMsMissingNTermReporterIon = combinedTsvPsmResults.PercentPSMsMissingNTermReporterIon;
                psmResultsOverall.PercentPSMsMissingReporterIon = combinedTsvPsmResults.PercentPSMsMissingReporterIon;

                mJobParams.AddResultFileToSkip(combinedPsmTsvFilePath);

                var workDirInfo = new DirectoryInfo(mWorkDir);

                foreach (var phrpFile in workDirInfo.GetFileSystemInfos(COMBINED_TSV_BASE_NAME + "*"))
                {
                    mJobParams.AddResultFileToSkip(phrpFile.Name);
                }
            }

            var summarizer = GetPsmResultsSummarizer(PeptideHitResultTypes.MSFragger);

            var psmResultsPosted = summarizer.PostJobPSMResults(mJob, psmResultsOverall);

            LogDebug("PostJobPSMResults returned " + psmResultsPosted);

            if (datasetIDsByExperimentGroup.Keys.Count <= 3)
                return resultOverall;

            // Zip the PHRP result files to create Dataset_syn_txt.zip

            var filesToZip = new List<FileInfo>();
            var workingDirectory = new DirectoryInfo(mWorkDir);

            // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
            foreach (var synopsisFile in synopsisFileNames)
            {
                var searchPattern = string.Format("{0}*.txt", Path.GetFileNameWithoutExtension(synopsisFile));

                var filesToAppend = workingDirectory.GetFiles(searchPattern);
                filesToZip.AddRange(filesToAppend);
            }

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var pepProtMapFile in workingDirectory.GetFiles("*_msfragger_PepToProtMapMTS.txt"))
            {
                if (pepProtMapFile.Name.StartsWith(COMBINED_TSV_BASE_NAME))
                    continue;

                filesToZip.Add(pepProtMapFile);
            }

            // Zip the files to create Dataset_syn_txt.zip
            var zipSuccess = ZipFiles("PHRP _syn.txt files", filesToZip, "Dataset_syn_txt.zip");

            if (!zipSuccess)
                resultOverall = CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE;

            return resultOverall;
        }

        /// <summary>
        /// Run PHRP for MSFragger (or FragPipe)
        /// </summary>
        /// <param name="baseDatasetName">Base dataset name</param>
        /// <param name="inputFileName">Input file name, e.g. Dataset_psm.tsv</param>
        /// <param name="postJobPSMResultsToDB">When true, store the PSM results in the database</param>
        /// <param name="synopsisFileNamesFromPHRP">Output: PHRP synopsis file names</param>
        /// <param name="psmResults">Output: PSM results</param>
        /// <returns>True if successful, false if an error</returns>
        private CloseOutType RunPhrpForMSFragger(
            string baseDatasetName,
            string inputFileName,
            bool postJobPSMResultsToDB,
            out List<string> synopsisFileNamesFromPHRP,
            out PSMResults psmResults)
        {
            synopsisFileNamesFromPHRP = new List<string>();
            psmResults = new PSMResults();

            var synopsisFileName = baseDatasetName + "_msfragger_syn.txt";

            var peptideSearchResultsFilePSM = new FileInfo(Path.Combine(mWorkDir, inputFileName));
            var peptideSearchResultsFileNoPSM = new FileInfo(Path.Combine(mWorkDir, inputFileName.Replace(AnalysisResourcesExtraction.PSM_FILE_SUFFIX, ".tsv")));
            FileInfo peptideSearchResultsFile;

            var inputFiles = new List<FileInfo>();

            if (peptideSearchResultsFilePSM.Exists)
            {
                inputFiles.Add(peptideSearchResultsFilePSM);
                peptideSearchResultsFile = peptideSearchResultsFilePSM;
            }
            else if (peptideSearchResultsFileNoPSM.Exists)
            {
                inputFiles.Add(peptideSearchResultsFileNoPSM);
                peptideSearchResultsFile = peptideSearchResultsFileNoPSM;
            }
            else
            {
                // Run PHRP on each _psm.tsv file in the work directory
                var workingDirectory = new DirectoryInfo(mWorkDir);

                // Use search pattern *_psm.tsv
                inputFiles.AddRange(workingDirectory.GetFiles(string.Format("*{0}", AnalysisResourcesExtraction.PSM_FILE_SUFFIX)));

                if (inputFiles.Count == 0)
                {
                    LogError("Did not find any _psm.tsv files in the working directory; cannot run PHRP");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Yes, this file does not exist, but the variable needs to have a value
                peptideSearchResultsFile = peptideSearchResultsFilePSM;
            }

            var successCount = 0;
            var successOverall = CloseOutType.CLOSEOUT_SUCCESS;

            foreach (var inputFile in inputFiles)
            {
                if (!peptideSearchResultsFile.Exists)
                {
                    // Override baseDatasetName
                    baseDatasetName = inputFile.Name.Substring(0, inputFile.Name.Length - AnalysisResourcesExtraction.PSM_FILE_SUFFIX.Length);
                }

                var result = RunPHRPWork(
                    "MSFragger",
                    inputFile.Name,
                    PeptideHitResultTypes.MSFragger,
                    synopsisFileName,
                    false,
                    true,
                    baseDatasetName,
                    out var synopsisFileNameFromPHRP);

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (result != CloseOutType.CLOSEOUT_NO_DATA)
                    {
                        successOverall = result;
                    }

                    continue;
                }

                synopsisFileNamesFromPHRP.Add(synopsisFileNameFromPHRP);

                // Summarize the number of PSMs in the synopsis file
                // This is done by this class since the MSFragger and FragPipe scripts do not have an MSGF job step

                const double thresholdForMSGFSpecEValueOrPEP = ResultsSummarizer.DEFAULT_MSGF_SPEC_EVALUE_THRESHOLD;

                var summarizeResult = SummarizePSMs(PeptideHitResultTypes.MSFragger, synopsisFileNameFromPHRP, thresholdForMSGFSpecEValueOrPEP, postJobPSMResultsToDB, out var psmResultsToAdd);

                if (summarizeResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    successOverall = summarizeResult;
                    continue;
                }

                successCount++;

                if (successCount == 1)
                    psmResults = psmResultsToAdd;
                else
                    psmResults.AppendResults(psmResultsToAdd);
            }

            return successCount == inputFiles.Count
                ? CloseOutType.CLOSEOUT_SUCCESS
                : successOverall;
        }

        private CloseOutType RunPhrpForMSGFPlus()
        {
            var currentStep = "Initializing";

            try
            {
                var skipPHRP = mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, AnalysisResourcesExtraction.JOB_PARAM_SKIP_PHRP, false);

                if (skipPHRP)
                {
                    LogMessage("Skipping PHRP since the results directory already has up-to-date PHRP files");

                    // Zip the MSGFPlus_ConsoleOutput files (if they exist; they probably don't)
                    ZipConsoleOutputFiles(false);
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                var phrp = new PepHitResultsProcWrapper(mMgrParams, mJobParams);
                RegisterPHRPEvents(phrp);

                // Run the processor
                if (mDebugLevel > 3)
                {
                    LogDebug("ExtractToolRunner.RunPhrpForMSGFPlus(); Starting PHRP");
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
                                var targetFilePathAlt = ReaderFactory.AutoSwitchToLegacyMSGFDBIfRequired(targetFilePath, "Dataset_msgfdb.txt");

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

                                LogMessage("Skipping PeptideToProteinMapping since job parameter SkipPeptideToProteinMapping is true");
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

                    result = phrp.ExtractDataFromResults(
                        targetFilePath, createMSGFPlusFirstHitsFile, createMSGFPlusSynopsisFile,
                        mGeneratedFastaFilePath, PeptideHitResultTypes.MSGFPlus, string.Empty);

                    if (result == CloseOutType.CLOSEOUT_NO_DATA)
                    {
                        // Message has already been logged
                        return result;
                    }

                    if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        LogWarning(
                            "Error running PHRP{0}",
                            string.IsNullOrWhiteSpace(phrp.ErrorMessage) ? string.Empty : "; " + phrp.ErrorMessage);

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
                var paramFileName = mJobParams.GetParam("ParamFileName");

                if (!ValidatePHRPResultMassErrors(synFilePath, PeptideHitResultTypes.MSGFPlus, paramFileName))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // This plugin does not summarize the number of PSMs for MS-GF+ jobs
                // That task is performed by method PostProcessMSGFResults in the MSGF plugin (project AnalysisManagerMSGFPlugin), calling method SummarizeMSGFResults

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
            var inputFileName = mDatasetName + "_IcTDA.tsv";

            // ReSharper disable once StringLiteralTypo
            var synopsisFileName = mDatasetName + "_mspath_syn.txt";

            return RunPHRPWork(
                "MSPathFinder",
                inputFileName,
                PeptideHitResultTypes.MSPathFinder,
                synopsisFileName,
                false,
                true);
        }

        /// <summary>
        /// Runs PeptideHitsResultsProcessor on SEQUEST output
        /// </summary>
        /// <returns>CloseOutType representing success or failure</returns>
        private CloseOutType RunPhrpForSEQUEST()
        {
            var inputFileName = mDatasetName + "_syn.txt";

            // Note that for SEQUEST, the synopsis file is the input file
            return RunPHRPWork(
                "SEQUEST",
                inputFileName,
                PeptideHitResultTypes.Sequest,
                inputFileName,
                true,
                true);
        }

        private CloseOutType RunPHRPForTopPIC()
        {
            const string SYNOPSIS_FILE_SUFFIX = "_toppic_syn.txt";

            var filesToFind = mDatasetName + "*_TopPIC_PrSMs.txt";

            var workingDirectory = new DirectoryInfo(mWorkDir);
            var prsmFiles = workingDirectory.GetFiles(filesToFind).ToList();

            var successCodes = new List<CloseOutType>();

            foreach (var inputFile in prsmFiles)
            {
                var matchIndex = inputFile.Name.LastIndexOf("_TopPIC_PrSMs", StringComparison.OrdinalIgnoreCase);

                var synopsisFileName = matchIndex > 0
                    ? inputFile.Name.Substring(0, matchIndex) + SYNOPSIS_FILE_SUFFIX
                    : mDatasetName + SYNOPSIS_FILE_SUFFIX;

                var returnCode = RunPHRPWork(
                    "TopPIC",
                    inputFile.Name,
                    PeptideHitResultTypes.TopPIC,
                    synopsisFileName,
                    true,
                    true);

                successCodes.Add(returnCode);
            }

            if (successCodes.Count == 0)
            {
                LogError("Did not find any TopPIC_PrSMs.txt files in the working directory");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (successCodes.Count < prsmFiles.Count)
            {
                LogError("Only processed {0} / {1} TopPIC_PrSMs.txt files in the working directory", successCodes.Count, prsmFiles.Count);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
            foreach (var returnCode in successCodes)
            {
                if (returnCode != CloseOutType.CLOSEOUT_SUCCESS)
                    return returnCode;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPhrpForXTandem()
        {
            var inputFileName = mDatasetName + "_xt.xml";
            var synopsisFileName = mDatasetName + "_xt.txt";

            return RunPHRPWork(
                "X!Tandem",
                inputFileName,
                PeptideHitResultTypes.XTandem,
                synopsisFileName,
                true,
                true);
        }

        /// <summary>
        /// Run the Peptide Hit Results Processor
        /// </summary>
        /// <param name="toolName">Tool name</param>
        /// <param name="inputFileName">Input file name</param>
        /// <param name="resultType">Peptide hit results type enum</param>
        /// <param name="synopsisFileName">If defined, calls ValidatePHRPResultMassErrors() to validate mass errors in the synopsis file</param>
        /// <param name="createFirstHitsFile">If true, create the first hits file</param>
        /// <param name="createSynopsisFile">If true, create the synopsis file</param>
        /// <returns>CloseOutType representing success or failure</returns>
        private CloseOutType RunPHRPWork(
            string toolName,
            string inputFileName,
            PeptideHitResultTypes resultType,
            string synopsisFileName,
            bool createFirstHitsFile,
            bool createSynopsisFile)
        {
            return RunPHRPWork(
                toolName, inputFileName, resultType, synopsisFileName,
                createFirstHitsFile, createSynopsisFile,
                string.Empty, out _);
        }

        /// <summary>
        /// Run the Peptide Hit Results Processor
        /// </summary>
        /// <remarks>
        /// Note that for data package based MSFragger or FragPipe jobs that have multiple experiment groups, this method is called once for each experiment group
        /// </remarks>
        /// <param name="toolName">Tool name</param>
        /// <param name="inputFileName">Input file name</param>
        /// <param name="resultType">Peptide hit results type enum</param>
        /// <param name="synopsisFileName">If defined, calls ValidatePHRPResultMassErrors() to validate mass errors in the synopsis file</param>
        /// <param name="createFirstHitsFile">If true, create the first hits file</param>
        /// <param name="createSynopsisFile">If true, create the synopsis file</param>
        /// <param name="outputFileBaseName">Output file base name</param>
        /// <param name="synopsisFileNameFromPHRP">Output: synopsis file name, from PHRP</param>
        /// <returns>CloseOutType representing success or failure</returns>
        private CloseOutType RunPHRPWork(
            string toolName,
            string inputFileName,
            PeptideHitResultTypes resultType,
            string synopsisFileName,
            bool createFirstHitsFile,
            bool createSynopsisFile,
            string outputFileBaseName,
            out string synopsisFileNameFromPHRP)
        {
            var currentStep = "Initializing";

            try
            {
                var phrp = new PepHitResultsProcWrapper(mMgrParams, mJobParams);
                RegisterPHRPEvents(phrp);

                // Run the processor
                if (mDebugLevel > 3)
                {
                    LogDebug("ExtractToolRunner.RunPHRPWork(); Starting PHRP");
                }

                try
                {
                    // The goal:
                    //   Create the _syn.txt file from the input file

                    currentStep = "Looking for the results file";

                    var peptideSearchResultsFilePath = Path.Combine(mWorkDir, inputFileName);

                    if (!File.Exists(peptideSearchResultsFilePath))
                    {
                        LogError("{0} results file not found: {1}", toolName, Path.GetFileName(peptideSearchResultsFilePath));
                        synopsisFileNameFromPHRP = string.Empty;
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    currentStep = "Running PHRP";

                    // Create the Synopsis (and optionally the first hits file) using the input file

                    var result = phrp.ExtractDataFromResults(
                        peptideSearchResultsFilePath, createFirstHitsFile, createSynopsisFile,
                        mGeneratedFastaFilePath, resultType, outputFileBaseName);

                    if (result == CloseOutType.CLOSEOUT_NO_DATA)
                    {
                        // Message has already been logged
                        synopsisFileNameFromPHRP = string.Empty;
                        return result;
                    }

                    if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        LogWarning(
                            "Error running PHRP{0}",
                            string.IsNullOrWhiteSpace(phrp.ErrorMessage) ? string.Empty : "; " + phrp.ErrorMessage);

                        synopsisFileNameFromPHRP = string.Empty;
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (!string.IsNullOrWhiteSpace(phrp.WarningMessage))
                    {
                        mEvalMessage = Global.AppendToComment(mEvalMessage, phrp.WarningMessage);
                    }
                }
                catch (Exception ex)
                {
                    LogError("Exception running PHRP", ex);
                    synopsisFileNameFromPHRP = string.Empty;
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (string.IsNullOrWhiteSpace(synopsisFileName))
                {
                    // Skip validating mass errors
                    synopsisFileNameFromPHRP = string.Empty;
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                if (Global.IsMatch(mDatasetName, AnalysisResources.AGGREGATION_JOB_DATASET) || AnalysisResources.IsDataPackageDataset(mDatasetName))
                {
                    // PHRP auto-named the synopsis file based on the datasets in this data package
                    // Auto-find the file

                    string errorMessage;

                    List<FileInfo> synopsisFiles;

                    // ReSharper disable once SwitchStatementHandlesSomeKnownEnumValuesWithDefault
                    switch (resultType)
                    {
                        case PeptideHitResultTypes.MaxQuant:
                            synopsisFiles = FileSearch.FindMaxQuantSynopsisFiles(mWorkDir, out errorMessage);
                            break;

                        case PeptideHitResultTypes.MSFragger:
                            synopsisFiles = FileSearch.FindMSFraggerSynopsisFiles(mWorkDir, out errorMessage);
                            break;

                        case PeptideHitResultTypes.DiaNN:
                            synopsisFiles = FileSearch.FindDiaNNSynopsisFiles(mWorkDir, out errorMessage);
                            break;

                        default:
                            LogError("Cannot validate mass errors for this aggregation job, unsupported result type: " + resultType);
                            synopsisFileNameFromPHRP = string.Empty;
                            return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (!string.IsNullOrWhiteSpace(errorMessage))
                    {
                        LogError(errorMessage);
                        synopsisFileNameFromPHRP = string.Empty;
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    switch (synopsisFiles.Count)
                    {
                        case 0:
                            LogError("PHRP did not create a synopsis file for this aggregation job");
                            synopsisFileNameFromPHRP = string.Empty;
                            return CloseOutType.CLOSEOUT_FAILED;

                        case 1:
                            synopsisFileNameFromPHRP = synopsisFiles[0].Name;
                            break;

                        default:
                            // For data package based MSFragger jobs that have multiple experiment groups, this method is called once for each experiment group
                            // Find the newest synopsis file
                            synopsisFileNameFromPHRP = (from item in synopsisFiles orderby item.LastWriteTime descending select item.Name).First();
                            break;
                    }
                }
                else
                {
                    synopsisFileNameFromPHRP = synopsisFileName;
                }

                // Validate that the mass errors are within tolerance
                var parameterFileName = resultType == PeptideHitResultTypes.XTandem
                    ? "input.xml"
                    : mJobParams.GetParam("ParamFileName");

                var synopsisFilePath = Path.Combine(mWorkDir, synopsisFileNameFromPHRP);

                if (!File.Exists(synopsisFilePath))
                {
                    LogError(string.Format("Synopsis file not found: {0}", synopsisFilePath));
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!ValidatePHRPResultMassErrors(synopsisFilePath, resultType, parameterFileName))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in RunPHRPWork at step " + currentStep, ex);
                synopsisFileNameFromPHRP = string.Empty;
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

            if (!mZipTools.ZipDirectory(consoleOutputFilesDir.FullName, zippedConsoleOutputFilePath))
            {
                LogError("Problem zipping the console output file; will not delete the separate copies from the transfer directory");
                return;
            }

            var transferDirPath = GetTransferDirectoryPath();

            if (string.IsNullOrWhiteSpace(transferDirPath))
            {
                // Error has already been logged
                return;
            }

            if (string.IsNullOrWhiteSpace(mResultsDirectoryName))
            {
                // Ignore error; will be logged in method
                return;
            }

            foreach (var consoleOutputFile in consoleOutputFiles)
            {
                var targetPath = Path.Combine(transferDirPath, mDatasetName, mResultsDirectoryName, consoleOutputFile);
                mJobParams.AddServerFileToDelete(targetPath);
            }
        }

        /// <summary>
        /// Store the list of files in a zip file (overwriting any existing zip file),
        /// then call AddResultFileToSkip() for each file
        /// </summary>
        /// <param name="fileListDescription">File list description</param>
        /// <param name="filesToZip">List of files to zip</param>
        /// <param name="zipFileName">Zip file name</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ZipFiles(string fileListDescription, IReadOnlyList<FileInfo> filesToZip, string zipFileName)
        {
            var zipFilePath = Path.Combine(mWorkDir, zipFileName);

            var success = mZipTools.ZipFiles(filesToZip, zipFilePath);

            if (success)
            {
                foreach (var item in filesToZip)
                {
                    mJobParams.AddResultFileToSkip(item.Name);
                }
            }
            else
            {
                LogError("Error zipping " + fileListDescription + " to create " + zipFileName);
            }

            return success;
        }

        private void RunPeptideProphet()
        {
            const int SYN_FILE_MAX_SIZE_MB = 200;
            const string PEP_PROPHET_RESULT_FILE_SUFFIX = "_PepProphet.txt";

            string pepProphetOutputFilePath;

            var result = CloseOutType.CLOSEOUT_SUCCESS;

            bool success;

            // This job parameter is defined in select settings files for analysis jobs
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

                return;
            }

            var peptideProphet = new PeptideProphetWrapper(progLoc);
            RegisterEvents(peptideProphet);
            peptideProphet.PeptideProphetRunning += PeptideProphet_PeptideProphetRunning;

            if (mDebugLevel >= 3)
            {
                LogDebug("ExtractToolRunner.RunPeptideProphet(); Starting Peptide Prophet");
            }

            var synFilePath = Path.Combine(mWorkDir, mDatasetName + "_syn.txt");

            // Check to see if the synopsis file exists
            var synopsisFile = new FileInfo(synFilePath);

            if (!synopsisFile.Exists)
            {
                LogError("ExtractToolRunner.RunPeptideProphet(); Syn file " + synFilePath + " not found; unable to run peptide prophet");
                return;
            }

            List<string> splitFileList;

            // Check the size of the Syn file
            // If it is too large, we will need to break it up into multiple parts, process each part separately, and then combine the results
            var parentSynFileSizeMB = Global.BytesToMB(synopsisFile.Length);

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
                    var splitErrorMessage = string.Format("Error splitting synopsis file that is over {0} MB in size", SYN_FILE_MAX_SIZE_MB);

                    if (ignorePeptideProphetErrors)
                    {
                        LogWarning(splitErrorMessage + "; Ignoring the error since 'IgnorePeptideProphetErrors' is true");
                        return;
                    }

                    LogError(splitErrorMessage);
                    return;
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
                var synFileNameAndSize = string.Format("{0} (file size = {1:F2} MB", splitSynFile.Name, Global.BytesToMB(splitSynFile.Length));

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

                // ReSharper disable CommentTypo

                // Note that the PeptideProphet DLL compiled in 2021 requires .NET 4.8 and the
                // Microsoft Visual C++ 2015-2019 Redistributable (x86) (files msvcp140.dll and msvcr140)

                // ReSharper restore CommentTypo

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
                        LogError("ExtractToolRunner.RunPeptideProphet(); Peptide Prophet output file not found for synopsis file " +
                                 synFileNameAndSize);

                        if (ignorePeptideProphetErrors)
                        {
                            LogWarning("Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' is true");
                        }
                        else
                        {
                            LogWarning(
                                "To ignore this error, update this job to use a settings file that has 'IgnorePeptideProphetErrors' set to true");
                            result = CloseOutType.CLOSEOUT_FAILED;
                            break;
                        }
                    }
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        LogError("Error running Peptide Prophet: " + peptideProphet.ErrorMessage);
                        LogWarning("Input file: " + synFileNameAndSize);
                    }
                    else
                    {
                        LogErrorNoMessageUpdate("Error running Peptide Prophet on file " + synFileNameAndSize + ": " + peptideProphet.ErrorMessage);
                    }

                    if (ignorePeptideProphetErrors)
                    {
                        LogWarning("Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' is true");
                    }
                    else
                    {
                        result = CloseOutType.CLOSEOUT_FAILED;
                        break;
                    }
                }
            }

            if (result != CloseOutType.CLOSEOUT_SUCCESS && !ignorePeptideProphetErrors)
                return;

            if (splitFileList.Count <= 1)
                return;

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
                return;
            }

            var msg = string.Format("Error interleaving the peptide prophet result files (FileCount={0})", splitFileList.Count);

            if (ignorePeptideProphetErrors)
            {
                LogWarning(msg + "; Ignoring the error since 'IgnorePeptideProphetErrors' is true");
            }
            else
            {
                LogError(msg);
            }
        }

        /// <summary>
        /// Deletes each file in splitFileList
        /// </summary>
        /// <param name="splitFileList">Full paths to files to delete</param>
        private void DeleteTemporaryFiles(IEnumerable<string> splitFileList)
        {
            AppUtils.GarbageCollectNow();

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

                using var writer = new StreamWriter(new FileStream(combinedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

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

                        linesRead[fileIndex]++;
                        totalLinesRead++;

                        if (lineIn == null)
                            continue;

                        var processLine = true;

                        if (linesRead[fileIndex] == 1 && lookForHeaderLine && lineIn.Length > 0)
                        {
                            // check for a header line
                            var splitLine = lineIn.Split(['\t'], 2);

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

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in ExtractToolRunner.InterleaveFiles", ex);
                return false;
            }
        }

        /// <summary>
        /// Reads sourceFilePath line-by-line and splits into multiple files such that none of the output
        /// files has length greater than maxSizeBytes. Can also check for a header line on the first line;
        /// if a header line is found, each of the split files will be assigned the same header line
        /// </summary>
        /// <param name="sourceFilePath">FilePath to parse</param>
        /// <param name="maxSizeBytes">Maximum size of each file</param>
        /// <param name="lookForHeaderLine">When true, looks for a header line by checking if the first column contains a number</param>
        /// <param name="splitFileList">Output array listing the full paths to the split files that were created</param>
        /// <returns>True if success, false if failure</returns>
        private bool SplitFileRoundRobin(string sourceFilePath, long maxSizeBytes, bool lookForHeaderLine, out List<string> splitFileList)
        {
            splitFileList = new List<string>();

            try
            {
                var sourceFile = new FileInfo(sourceFilePath);

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
                using var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));

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
                    linesRead++;

                    if (lineIn == null)
                        continue;

                    var processLine = true;

                    if (linesRead == 1 && lookForHeaderLine && lineIn.Length > 0)
                    {
                        // Check for a header line
                        var splitLine = lineIn.Split(['\t'], 2);

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
                        targetFileIndex++;

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

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in ExtractToolRunner.SplitFileRoundRobin", ex);
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            var toolVersionInfo = string.Empty;
            var toolFiles = new List<FileInfo>();

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of AnalysisManager Extraction Plugin
            if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "AnalysisManagerExtractionPlugin"))
            {
                return false;
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

            var resultTypeName = AnalysisResources.GetResultType(mJobParams);

            if (resultTypeName.Equals(AnalysisResources.RESULT_TYPE_SEQUEST))
            {
                // SEQUEST result type

                // Lookup the version of SequestResultsProcessor.dll
                if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "SequestResultsProcessor"))
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
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Error calling SetStepTaskToolVersion: " + ex.Message, ex);
                return false;
            }
        }

        private ResultsSummarizer GetPsmResultsSummarizer(PeptideHitResultTypes resultType)
        {
            // SQL Server: Data Source=Gigasax;Initial Catalog=DMS5
            // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
            var connectionString = mMgrParams.GetParam("ConnectionString");

            var summarizer = new ResultsSummarizer(
                resultType, mDatasetName, mJob,
                mWorkDir,
                connectionString,
                mDebugLevel,
                TraceMode);

            RegisterEvents(summarizer);

            // Monitor events for "permission was denied"
            UnregisterEventHandler(summarizer, BaseLogger.LogLevels.ERROR);
            summarizer.ErrorEvent += MSGFResultsSummarizer_ErrorHandler;

            summarizer.ContactDatabase = true;
            summarizer.SaveResultsToTextFile = false;
            summarizer.DatasetName = mDatasetName;

            return summarizer;
        }

        private CloseOutType SummarizePSMs(PeptideHitResultTypes resultType, string synopsisFileNameFromPHRP, double thresholdForMsgfOrSpecEValueOrPEP)
        {
            return SummarizePSMs(resultType, synopsisFileNameFromPHRP, thresholdForMsgfOrSpecEValueOrPEP, true, out _);
        }

        /// <summary>
        /// Examine the synopsis (and optionally first hits) files to summarize PSMs
        /// </summary>
        /// <param name="resultType">Result type</param>
        /// <param name="synopsisFileNameFromPHRP">Synopsis file name, as reported by PHRP</param>
        /// <param name="thresholdForMSGFSpecEValueOrPEP">Threshold to use when computing the MSGF SpecEvalue stats (DIA-NN and MaxQuant use PEP instead of SpecEValue)</param>
        /// <param name="postJobPSMResultsToDB">When true, post the PSM results for this job to the database</param>
        /// <param name="psmResults">Output: PSM Results</param>
        /// <returns>CloseOutType representing success or failure</returns>
        private CloseOutType SummarizePSMs(
            PeptideHitResultTypes resultType,
            string synopsisFileNameFromPHRP,
            double thresholdForMSGFSpecEValueOrPEP,
            bool postJobPSMResultsToDB,
            out PSMResults psmResults)
        {
            var summarizer = GetPsmResultsSummarizer(resultType);
            summarizer.PostJobPSMResultsToDB = postJobPSMResultsToDB;
            summarizer.MSGFSpecEValueOrPEPThreshold = thresholdForMSGFSpecEValueOrPEP;

            if (resultType is PeptideHitResultTypes.MaxQuant or PeptideHitResultTypes.DiaNN)
            {
                summarizer.MSGFSpecEValueOrPEPThreshold = ResultsSummarizer.DEFAULT_POSTERIOR_ERROR_PROBABILITY_THRESHOLD;
            }

            var success = summarizer.ProcessPSMResults(synopsisFileNameFromPHRP);

            psmResults = summarizer.GetPsmResults();

            if (success)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            if (string.IsNullOrWhiteSpace(summarizer.ErrorMessage))
            {
                LogError("Error summarizing the PSMs using MSGFResultsSummarizer");
            }
            else
            {
                LogError("Error summarizing the PSMs: " + summarizer.ErrorMessage);
            }

            LogError("SummarizePSMs: " + mMessage);
            return CloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Update the DIA-NN report.tsv file to remove duplicate .mzML entries and possibly shorten the Run names
        /// </summary>
        /// <param name="reportFile">DIA-NN report.tsv file</param>
        private bool UpdateDiannReportFile(FileSystemInfo reportFile)
        {
            try
            {
                var filePathOld = reportFile.FullName + ".old";

                // Rename the report.tsv file (use a new FileInfo instance so that the path in reportFile remains unchanged)
                var sourceReportFile = new FileInfo(reportFile.FullName);

                LogDebug("Renaming {0} to {1}", sourceReportFile.FullName, filePathOld);

                sourceReportFile.MoveTo(filePathOld);

                mJobParams.AddResultFileToSkip(sourceReportFile.Name);

                // Pre-scan the report file to determine the Run names (which should be the dataset names);
                // Look for the longest common text in the names and construct a map of full name to shortened name
                if (!ConstructDatasetNameMap(sourceReportFile, out var baseNameByDatasetName))
                    return false;

                var updatedFile = new FileInfo(reportFile.FullName);

                // This sorted set tracks the dataset file names in the File.Name column (typically .mzML files)
                var datasetFiles = new SortedSet<string>();

                using (var reader = new StreamReader(new FileStream(sourceReportFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(updatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var linesRead = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        linesRead++;

                        var lineParts = dataLine.Split('\t');

                        if (linesRead == 1)
                        {
                            if (!ValidateDiannReportFileHeaderLine(sourceReportFile, lineParts))
                                return false;

                            writer.WriteLine(dataLine);
                            continue;
                        }

                        // ReSharper disable once CanSimplifySetAddingWithSingleCall

                        // Store an empty string in the first column if the .mzML file has already been listed once
                        if (datasetFiles.Contains(lineParts[0]))
                        {
                            lineParts[0] = string.Empty;
                        }
                        else
                        {
                            datasetFiles.Add(lineParts[0]);
                        }

                        // Possibly shorten the dataset name in the second column

                        if (baseNameByDatasetName.TryGetValue(lineParts[1], out var datasetNameToUse))
                            lineParts[1] = datasetNameToUse;

                        writer.WriteLine(string.Join("\t", lineParts));
                    }
                }

                AppUtils.SleepMilliseconds(100);

                LogMessage("Created updated report.tsv file; deleting {0}", sourceReportFile.FullName);

                try
                {
                    sourceReportFile.Delete();
                }
                catch (Exception ex)
                {
                    LogWarning("Unable to delete the old DIA-NN report.tsv file: {0}", ex.Message);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in UpdateDiannReportFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Validate the header names in headerNames
        /// </summary>
        /// <param name="reportFile">DIA-NN report.tsv file</param>
        /// <param name="headerNames">List of header names</param>
        /// <returns>True if the first two columns are 'File.Name' and 'Run', otherwise false</returns>
        private bool ValidateDiannReportFileHeaderLine(FileSystemInfo reportFile, IReadOnlyList<string> headerNames)
        {
            // Validate the header line
            if (headerNames.Count < 3)
            {
                LogError("File {0} has {1} columns in the header line; expecting over 50 columns", reportFile.Name, headerNames.Count);

                return false;
            }

            if (!headerNames[0].Equals("File.Name", StringComparison.OrdinalIgnoreCase))
            {
                LogWarning(string.Format(
                    "The first column in file {0} is '{1}', differing from the expected value: File.Name",
                    reportFile.Name, headerNames[0]), true);
            }

            if (!headerNames[1].Equals("Run", StringComparison.OrdinalIgnoreCase))
            {
                LogWarning(string.Format(
                    "The second column in file {0} is '{1}', differing from the expected value: Run",
                    reportFile.Name, headerNames[1]), true);
            }

            return true;
        }

        private bool ValidatePHRPResultMassErrors(
            string inputFilePath,
            PeptideHitResultTypes resultType,
            string searchEngineParamFileName)
        {
            try
            {
                var massErrorValidator = new PHRPMassErrorValidator(mDebugLevel);
                RegisterEvents(massErrorValidator);

                var paramFilePath = Path.Combine(mWorkDir, searchEngineParamFileName);

                // The "ToolName" parameter holds the pipeline script name
                var toolName = mJobParams.GetJobParameter("ToolName", string.Empty);

                // The default error threshold is 5%
                // Use 10% for MaxQuant
                // For small FASTA files, this percent may need to be even higher

                if (resultType == PeptideHitResultTypes.MaxQuant || toolName.StartsWith("MaxQuant"))
                {
                    massErrorValidator.ErrorThresholdPercent = 10;
                }

                var success = massErrorValidator.ValidatePHRPResultMassErrors(inputFilePath, resultType, paramFilePath);

                if (success)
                    return true;

                // This job parameter is defined in select settings files for analysis jobs
                var ignoreValidationErrors = mJobParams.GetJobParameter("IgnoreMassErrorValidationErrors", false);

                if (ignoreValidationErrors)
                {
                    LogWarning("Ignoring mass error validation failure since 'IgnoreMassErrorValidationErrors' is true");
                    return true;
                }

                if (toolName.StartsWith("inspect", StringComparison.OrdinalIgnoreCase))
                {
                    // Ignore this error for inspect if running an unrestricted search
                    var paramFileName = mJobParams.GetJobParameter("ParamFileName", "");

                    // ReSharper disable once StringLiteralTypo
                    if (paramFileName.IndexOf("Unrestrictive", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        return true;
                    }
                }

                if (string.IsNullOrWhiteSpace(massErrorValidator.ErrorMessage))
                {
                    LogError("ValidatePHRPResultMassErrors returned false");
                }
                else
                {
                    LogError(massErrorValidator.ErrorMessage);
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error calling ValidatePHRPResultMassErrors", ex);
                return false;
            }
        }

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
                LogDebug("Running PHRP: {0}; {1}% complete", taskDescription, percentComplete);
            }
        }

        private void MSGFPlusUtils_ErrorEvent(string errorMsg, Exception ex)
        {
            mMSGFPlusUtilsError = true;
        }

        /// <summary>
        /// Event handler for the MSGResultsSummarizer
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        /// <param name="ex">Exception</param>
        private void MSGFResultsSummarizer_ErrorHandler(string errorMessage, Exception ex)
        {
            if (errorMessage.IndexOf("permission was denied", StringComparison.OrdinalIgnoreCase) >= 0 ||
                errorMessage.IndexOf("permission denied", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                LogErrorToDatabase(errorMessage);
            }
            else
            {
                LogError(errorMessage, ex);
            }
        }

        private DateTime mLastMzidMergerUpdate = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event for MzidMerger
        /// </summary>
        private void MzidMerger_LoopWaiting()
        {
            // Update the status by parsing the PHRP console output file every 20 seconds
            if (DateTime.UtcNow.Subtract(mLastMzidMergerUpdate).TotalSeconds >= 20)
            {
                mLastMzidMergerUpdate = DateTime.UtcNow;
                ParseMzidMergerConsoleOutputFile();
            }
        }
    }
}
