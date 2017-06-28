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
using System.Threading;
using AnalysisManagerBase;
using AnalysisManagerMSGFDBPlugIn;
using MSGFResultsSummarizer;
using PHRPReader;

namespace AnalysisManagerExtractionPlugin
{
    /// <summary>
    /// Primary class for controlling data extraction
    /// </summary>
    /// <remarks></remarks>
    public class clsExtractToolRunner : clsAnalysisToolRunnerBase
    {
        #region "Constants"

        private const float SEQUEST_PROGRESS_EXTRACTION_DONE = 33;
        private const float SEQUEST_PROGRESS_PHRP_DONE = 66;
        private const float SEQUEST_PROGRESS_PEPPROPHET_DONE = 100;

        public const string INSPECT_UNFILTERED_RESULTS_FILE_SUFFIX = "_inspect_unfiltered.txt";

        private const string MODa_JAR_NAME = "moda.jar";
        private const string MODa_FILTER_JAR_NAME = "anal_moda.jar";

        private const string MODPlus_JAR_NAME = "modp_pnnl.jar";
        private const string MODPlus_FILTER_JAR_NAME = "tda_plus.jar";

        #endregion

        #region "Module variables"

        private clsPeptideProphetWrapper m_PeptideProphet;

        private MSGFPlusUtils mMSGFPlusUtils;
        private bool mMSGFPlusUtilsError;

        private string mGeneratedFastaFilePath;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs the data extraction tool(s)
        /// </summary>
        /// <returns>CloseOutType representing success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {

            var strCurrentAction = "preparing for extraction";

            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerDeconPeakDetector.RunTool(): Enter");
                }

                // Store the AnalysisManager version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false for Data Extraction");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var orgDbDir = m_mgrParams.GetParam("orgdbdir");

                // Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
                // However, if job parameter SkipProteinMods is True, the Fasta file will not have been retrieved
                var fastaFileName = m_jobParams.GetParam("PeptideSearch", "generatedFastaName");
                if (string.IsNullOrEmpty(fastaFileName))
                {
                    mGeneratedFastaFilePath = string.Empty;
                }
                else
                {
                    mGeneratedFastaFilePath = Path.Combine(orgDbDir, fastaFileName);
                }

                CloseOutType eResult;
                var processingSuccess = true;

                switch (m_jobParams.GetParam("ResultType"))
                {
                    case clsAnalysisResources.RESULT_TYPE_SEQUEST:   // Sequest result type
                        // Run Ken's Peptide Extractor DLL
                        strCurrentAction = "running peptide extraction for Sequest";
                        eResult = PerformPeptideExtraction();
                        // Check for no data first. If no data, then exit but still copy results to server
                        if (eResult == CloseOutType.CLOSEOUT_NO_DATA)
                        {
                            break;
                        }

                        // Run PHRP
                        if (eResult == CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            m_progress = SEQUEST_PROGRESS_EXTRACTION_DONE;     // 33% done
                            UpdateStatusRunning(m_progress);

                            strCurrentAction = "running peptide hits result processor for Sequest";
                            eResult = RunPhrpForSequest();
                        }

                        if (eResult == CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            m_progress = SEQUEST_PROGRESS_PHRP_DONE;   // 66% done
                            UpdateStatusRunning(m_progress);
                            strCurrentAction = "running peptide prophet for Sequest";
                            RunPeptideProphet();
                        }

                        break;
                    case clsAnalysisResources.RESULT_TYPE_XTANDEM:
                        // Run PHRP
                        strCurrentAction = "running peptide hits result processor for X!Tandem";
                        eResult = RunPhrpForXTandem();
                        break;
                    case clsAnalysisResources.RESULT_TYPE_INSPECT:
                        // Run PHRP
                        strCurrentAction = "running peptide hits result processor for Inspect";
                        eResult = RunPhrpForInSpecT();
                        break;
                    case clsAnalysisResources.RESULT_TYPE_MSGFPLUS:
                        // Run PHRP
                        strCurrentAction = "running peptide hits result processor for MSGF+";
                        eResult = RunPhrpForMSGFPlus();
                        break;
                    case clsAnalysisResources.RESULT_TYPE_MSALIGN:
                        // Run PHRP
                        strCurrentAction = "running peptide hits result processor for MSAlign";
                        eResult = RunPhrpForMSAlign();
                        break;
                    case clsAnalysisResources.RESULT_TYPE_MODA:
                        // Convert the MODa results to a tab-delimited file; do not filter out the reversed-hit proteins
                        eResult = ConvertMODaResultsToTxt(out var strFilteredMODaResultsFilePath, true);
                        if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                            break;
                        }

                        // Run PHRP
                        strCurrentAction = "running peptide hits result processor for MODa";
                        eResult = RunPhrpForMODa(strFilteredMODaResultsFilePath);
                        if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                        }

                        // Convert the MODa results to a tab-delimited file, filter by FDR (and filter out the reverse-hit proteins)
                        eResult = ConvertMODaResultsToTxt(out strFilteredMODaResultsFilePath, false);
                        if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                        }

                        break;
                    case clsAnalysisResources.RESULT_TYPE_MODPLUS:
                        // Convert the MODPlus results to a tab-delimited file; do not filter out the reversed-hit proteins
                        eResult = ConvertMODPlusResultsToTxt(out var strFilteredMODPlusResultsFilePath, true);
                        if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                            break;
                        }

                        // Run PHRP
                        strCurrentAction = "running peptide hits result processor for MODPlus";
                        eResult = RunPhrpForMODPlus(strFilteredMODPlusResultsFilePath);
                        if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                        }

                        // Convert the MODa results to a tab-delimited file, filter by FDR (and filter out the reverse-hit proteins)
                        eResult = ConvertMODPlusResultsToTxt(out strFilteredMODPlusResultsFilePath, false);
                        if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            processingSuccess = false;
                        }

                        break;
                    case clsAnalysisResources.RESULT_TYPE_MSPATHFINDER:
                        // Run PHRP
                        strCurrentAction = "running peptide hits result processor for MSPathFinder";
                        eResult = RunPHRPForMSPathFinder();
                        break;
                    default:
                        // Should never get here - invalid result type specified
                        LogError("Invalid ResultType specified: " + m_jobParams.GetParam("ResultType"));
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                if (eResult != CloseOutType.CLOSEOUT_SUCCESS && eResult != CloseOutType.CLOSEOUT_NO_DATA)
                {
                    LogError("Error " + strCurrentAction);
                    processingSuccess = false;
                }
                else
                {
                    m_progress = 100;
                    UpdateStatusRunning(m_progress);
                    m_jobParams.AddResultFileToSkip(clsPepHitResultsProcWrapper.PHRP_LOG_FILE_NAME);
                }

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;


                // Add the current job data to the summary file
                UpdateSummaryFile();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var copySuccess = CopyResultsToTransferDirectory();

                if (!copySuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                // Everything succeeded; now delete the _msgfplus.tsv file from the server
                // For SplitFasta files there will be multiple tsv files to delete, plus the individual ConsoleOutput.txt files (all tracked with m_jobParams.ServerFilesToDelete)
                RemoveNonResultServerFiles();

                return CloseOutType.CLOSEOUT_SUCCESS;
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
        /// <param name="strFilteredMODaResultsFilePath">Output parameter: path to the filtered results file</param>
        /// <param name="keepAllResults"></param>
        /// <returns>The path to the .txt file if successful; empty string if an error</returns>
        /// <remarks></remarks>
        private CloseOutType ConvertMODaResultsToTxt(out string strFilteredMODaResultsFilePath, bool keepAllResults)
        {
            var fdrThreshold = m_jobParams.GetJobParameter("MODaFDRThreshold", 0.05f);
            var decoyPrefix = m_jobParams.GetJobParameter("MODaDecoyPrefix", "Reversed_");

            const bool isModPlus = false;

            return ConvertMODaOrMODPlusResultsToTxt(fdrThreshold, decoyPrefix, isModPlus, out strFilteredMODaResultsFilePath, keepAllResults);
        }

        private CloseOutType ConvertMODPlusResultsToTxt(out string strFilteredMODPlusResultsFilePath, bool keepAllResults)
        {
            var fdrThreshold = m_jobParams.GetJobParameter("MODPlusDecoyFilterFDR", 0.05f);
            var decoyPrefix = m_jobParams.GetJobParameter("MODPlusDecoyPrefix", "Reversed_");

            const bool isModPlus = true;

            return ConvertMODaOrMODPlusResultsToTxt(fdrThreshold, decoyPrefix, isModPlus, out strFilteredMODPlusResultsFilePath, keepAllResults);
        }

        private CloseOutType ConvertMODaOrMODPlusResultsToTxt(float fdrThreshold, string decoyPrefixJobParam, bool isModPlus,
            out string strFilteredResultsFilePath, bool keepAllResults)
        {
            strFilteredResultsFilePath = string.Empty;

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

                if (keepAllResults)
                {
                    // Use a fake decoy prefix so that all results will be kept (the top hit for each scan that anal_moda/tda_plus decides to keep)
                    decoyPrefixJobParam = "ABC123XYZ_";
                }
                else
                {
                    const int MINIMUM_PERCENT_DECOY = 25;
                    var fiFastaFile = new FileInfo(mGeneratedFastaFilePath);

                    if (m_DebugLevel >= 1)
                    {
                        LogMessage("Verifying the decoy prefix in " + fiFastaFile.Name);
                    }

                    // Determine the most common decoy prefix in the Fasta file
                    var decoyPrefixes = clsAnalysisResources.GetDefaultDecoyPrefixes();
                    var bestPrefix = new KeyValuePair<double, string>(0, string.Empty);

                    foreach (var decoyPrefix in decoyPrefixes)
                    {
                        int proteinCount;
                        var fractionDecoy = clsAnalysisResources.GetDecoyFastaCompositionStats(fiFastaFile, decoyPrefix, out proteinCount);

                        if (fractionDecoy * 100 >= MINIMUM_PERCENT_DECOY)
                        {
                            if (fractionDecoy > bestPrefix.Key)
                            {
                                bestPrefix = new KeyValuePair<double, string>(fractionDecoy, decoyPrefix);
                            }
                        }
                    }

                    if (!string.IsNullOrEmpty(bestPrefix.Value) && bestPrefix.Value != decoyPrefixJobParam)
                    {
                        m_EvalMessage = "Using decoy prefix " + bestPrefix.Value + " instead of " + decoyPrefixJobParam +
                                        " as defined by job parameter MODPlusDecoyPrefix because " + (bestPrefix.Key * 100).ToString("0") +
                                        "% of the proteins start with " + bestPrefix.Value;

                        LogMessage(m_EvalMessage);

                        decoyPrefixJobParam = bestPrefix.Value;
                    }
                }

                var paramFileName = m_jobParams.GetParam("ParmFileName");
                var paramFilePath = Path.Combine(m_WorkDir, paramFileName);

                var resultsFilePath = Path.Combine(m_WorkDir, m_Dataset + fileNameSuffix);

                if (Math.Abs(fdrThreshold) < float.Epsilon)
                {
                    fdrThreshold = 0.05f;
                }
                else if (fdrThreshold > 1)
                {
                    fdrThreshold = 1;
                }

                if (m_DebugLevel >= 2)
                {
                    LogMessage("Filtering MODa/MODPlus Results with FDR threshold " + fdrThreshold.ToString("0.00"));
                }

                const int javaMemorySize = 1000;

                // javaProgLoc will typically be "C:\Program Files\Java\jre8\bin\java.exe"
                var javaProgLoc = GetJavaProgLoc();
                if (string.IsNullOrEmpty(javaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to the MODa or MODPlus program
                var modxProgLoc = DetermineProgramLocation(toolName + "ProgLoc", modxProgJarName);

                var fiMODx = new FileInfo(modxProgLoc);

                if (string.IsNullOrWhiteSpace(fiMODx.DirectoryName))
                {
                    LogError("Cannot determine the parent directory of " + fiMODx.FullName);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Set up and execute a program runner to run anal_moda.jar or tda_plus.jar
                var cmdStr = " -Xmx" + javaMemorySize + "M -jar " + Path.Combine(fiMODx.DirectoryName, modxFilterJarName);
                cmdStr += " -i " + resultsFilePath;

                if (!isModPlus)
                {
                    // Processing MODa data; include the parameter file
                    cmdStr += " -p " + paramFilePath;
                }

                cmdStr += " -fdr " + fdrThreshold;
                cmdStr += " -d " + decoyPrefixJobParam;

                // Example command line:
                // "C:\Program Files\Java\jre8\bin\java.exe" -Xmx1000M -jar C:\DMS_Programs\MODa\anal_moda.jar
                //   -i "E:\DMS_WorkDir3\QC_Shew_13_04_pt1_1_2_45min_14Nov13_Leopard_13-05-21_moda.txt"
                //   -p "E:\DMS_WorkDir3\MODa_PartTryp_Par20ppm_Frag0pt6Da" -fdr 0.05 -d XXX_
                // "C:\Program Files\Java\jre8\bin\java.exe" -Xmx1000M -jar C:\DMS_Programs\MODPlus\tda_plus.jar
                //   -i "E:\DMS_WorkDir3\QC_Shew_13_04_pt1_1_2_45min_14Nov13_Leopard_13-05-21_modp.txt"
                //   -fdr 0.05 -d Reversed_

                LogDebug(javaProgLoc + " " + cmdStr);

                var progRunner = new clsRunDosProgram(m_WorkDir)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = false,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(m_WorkDir, toolName + "_Filter_ConsoleOutput.txt")
                };
                RegisterEvents(progRunner);

                var blnSuccess = progRunner.RunProgram(javaProgLoc, cmdStr, toolName + "_Filter", true);

                if (!blnSuccess)
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

                m_jobParams.AddResultFileToSkip(Path.GetFileName(progRunner.ConsoleOutputFilePath));

                // Confirm that the results file was created
                var fiFilteredResultsFilePath = new FileInfo(Path.ChangeExtension(resultsFilePath, ".id.txt"));

                if (!fiFilteredResultsFilePath.Exists)
                {
                    LogError("Filtered " + toolName + " results file not found: " + fiFilteredResultsFilePath.Name);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                strFilteredResultsFilePath = fiFilteredResultsFilePath.FullName;
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in ConvertMODaOrMODPlusResultsToTxt", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Convert the .mzid file created by MSGF+ to a .tsv file
        /// </summary>
        /// <param name="suffixToAdd">Suffix to add when parsing files created by Parallel MSGF+</param>
        /// <returns>The path to the .tsv file if successful; empty string if an error</returns>
        /// <remarks></remarks>
        private string ConvertMZIDToTSV(string suffixToAdd)
        {
            try
            {
                var strMZIDFileName = m_Dataset + "_msgfplus" + suffixToAdd + ".mzid";
                if (!File.Exists(Path.Combine(m_WorkDir, strMZIDFileName)))
                {
                    var strMZIDFileNameAlternate = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(strMZIDFileName, "Dataset_msgfdb.txt");
                    if (File.Exists(Path.Combine(m_WorkDir, strMZIDFileNameAlternate)))
                    {
                        strMZIDFileName = strMZIDFileNameAlternate;
                    }
                    else
                    {
                        LogError(strMZIDFileName + " file not found");
                        return string.Empty;
                    }
                }

                // Determine the path to the MzidToTsvConverter
                var mzidToTsvConverterProgLoc = DetermineProgramLocation("MzidToTsvConverterProgLoc", "MzidToTsvConverter.exe");

                if (string.IsNullOrEmpty(mzidToTsvConverterProgLoc))
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("Parameter 'MzidToTsvConverter' not defined for this manager");
                    }
                    return string.Empty;
                }

                // Initialize mMSGFPlusUtils
                mMSGFPlusUtils = new MSGFPlusUtils(m_mgrParams, m_jobParams, m_WorkDir, m_DebugLevel);
                RegisterEvents(mMSGFPlusUtils);

                // Attach an additional handler for the ErrorEvent
                // This additional handler sets mMSGFPlusUtilsError to true
                mMSGFPlusUtils.ErrorEvent += MSGFPlusUtils_ErrorEvent;

                mMSGFPlusUtilsError = false;

                var strTSVFilePath = mMSGFPlusUtils.ConvertMZIDToTSV(mzidToTsvConverterProgLoc, m_Dataset, strMZIDFileName);

                if (mMSGFPlusUtilsError)
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                    {
                        LogError("mMSGFPlusUtilsError is True after call to ConvertMZIDToTSV");
                    }
                    return string.Empty;
                }

                if (!string.IsNullOrEmpty(strTSVFilePath))
                {
                    // File successfully created

                    if (!string.IsNullOrEmpty(suffixToAdd))
                    {
                        var fiTSVFile = new FileInfo(strTSVFilePath);

                        if (string.IsNullOrWhiteSpace(fiTSVFile.DirectoryName))
                        {
                            LogError("Cannot determine the parent directory of " + fiTSVFile.FullName);
                            return string.Empty;
                        }

                        var newTSVPath = Path.Combine(fiTSVFile.DirectoryName,
                            Path.GetFileNameWithoutExtension(strTSVFilePath) + suffixToAdd + ".tsv");

                        fiTSVFile.MoveTo(newTSVPath);
                    }

                    return strTSVFilePath;
                }

                if (string.IsNullOrEmpty(m_message))
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
        /// Create the Peptide to Protein map file for the given MSGF+ results file
        /// </summary>
        /// <param name="resultsFileName"></param>
        /// <returns></returns>
        private CloseOutType CreateMSGFPlusResultsProteinToPeptideMappingFile(string resultsFileName)
        {
            LogMessage("Creating the missing _PepToProtMap.txt file");

            var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
            if (mMSGFPlusUtils == null)
            {
                mMSGFPlusUtils = new MSGFPlusUtils(m_mgrParams, m_jobParams, m_WorkDir, m_DebugLevel);
                RegisterEvents(mMSGFPlusUtils);

                // Attach an additional handler for the ErrorEvent
                // This additional handler sets mMSGFPlusUtilsError to true
                mMSGFPlusUtils.ErrorEvent += MSGFPlusUtils_ErrorEvent;
            }

            mMSGFPlusUtilsError = false;

            // Assume this is true
            var resultsIncludeAutoAddedDecoyPeptides = true;

            var result = mMSGFPlusUtils.CreatePeptideToProteinMapping(resultsFileName, resultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder);

            if (result != CloseOutType.CLOSEOUT_SUCCESS && result != CloseOutType.CLOSEOUT_NO_DATA)
            {
                return result;
            }

            if (mMSGFPlusUtilsError)
            {
                if (string.IsNullOrWhiteSpace(m_message))
                {
                    LogError("mMSGFPlusUtilsError is True after call to CreatePeptideToProteinMapping");
                }

                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType ParallelMSGFPlusMergeTSVFiles(int numberOfClonedSteps, int numberOfHitsPerScanToKeep,
            out SortedSet<string> lstFilterPassingPeptides)
        {
            lstFilterPassingPeptides = new SortedSet<string>();

            try
            {
                var mergedFilePath = Path.Combine(m_WorkDir, m_Dataset + "_msgfplus.tsv");

                // Keys in this dictionary are column names, values are the 0-based column index
                var dctHeaderMapping = new Dictionary<string, int>();

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

                using (var swMergedFile = new StreamWriter(new FileStream(mergedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // ReSharper disable once UseImplicitlyTypedVariableEvident

                    for (var iteration = 1; iteration <= numberOfClonedSteps; iteration++)
                    {
                        var sourceFilePath = Path.Combine(m_WorkDir, m_Dataset + "_msgfplus_Part" + iteration + ".tsv");
                        var linesRead = 0;

                        if (m_DebugLevel >= 2)
                        {
                            LogDebug("Caching data from " + sourceFilePath);
                        }

                        using (var srSourceFile = new StreamReader(new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                        {
                            while (!srSourceFile.EndOfStream)
                            {
                                var strLineIn = srSourceFile.ReadLine();
                                if (string.IsNullOrWhiteSpace(strLineIn))
                                    continue;

                                linesRead += 1;
                                totalLinesProcessed += 1;

                                if (linesRead == 1)
                                {
                                    if (iteration != 1)
                                        continue;

                                    // Write the header line
                                    swMergedFile.WriteLine(strLineIn);

                                    const bool IS_CASE_SENSITIVE = false;
                                    var lstHeaderNames = new List<string>
                                    {
                                        "ScanNum",
                                        "Charge",
                                        "Peptide",
                                        "Protein",
                                        "SpecEValue"
                                    };
                                    dctHeaderMapping = clsGlobal.ParseHeaderLine(strLineIn, lstHeaderNames, IS_CASE_SENSITIVE);

                                    foreach (var headerName in lstHeaderNames)
                                    {
                                        if (dctHeaderMapping[headerName] < 0)
                                        {
                                            LogError("Header " + headerName + " not found in " + Path.GetFileName(sourceFilePath) +
                                                     "; unable to merge the MSGF+ .tsv files");
                                            return CloseOutType.CLOSEOUT_FAILED;
                                        }
                                    }
                                }
                                else
                                {
                                    var splitLine = strLineIn.Split('\t');

                                    var scanNumber = splitLine[dctHeaderMapping["ScanNum"]];
                                    var chargeState = splitLine[dctHeaderMapping["Charge"]];

                                    int.TryParse(scanNumber, out var scanNumberValue);
                                    int.TryParse(chargeState, out var chargeStateValue);

                                    var scanChargeCombo = scanNumber + "_" + chargeState;
                                    var peptide = splitLine[dctHeaderMapping["Peptide"]];
                                    var protein = splitLine[dctHeaderMapping["Protein"]];
                                    var specEValueText = splitLine[dctHeaderMapping["SpecEValue"]];

                                    if (!double.TryParse(specEValueText, out var specEValue))
                                    {
                                        if (warningsLogged < 10)
                                        {
                                            LogWarning("SpecEValue was not numeric: " + specEValueText + " in " + strLineIn);
                                            warningsLogged += 1;

                                            if (warningsLogged >= 10)
                                            {
                                                LogWarning("Additional warnings will not be logged");
                                            }
                                        }

                                        continue;
                                    }

                                    clsMSGFPlusPSMs hitsForScan;

                                    var udtPSM = new clsMSGFPlusPSMs.udtPSMType
                                    {
                                        Peptide = peptide,
                                        SpecEValue = specEValue,
                                        DataLine = strLineIn
                                    };

                                    if (dctScanChargeTopHits.TryGetValue(scanChargeCombo, out hitsForScan))
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

                    if (m_DebugLevel >= 2)
                    {
                        LogDebug("Sorting results for " + dctScanChargeBestScore.Count + " lines of scan/charge combos");
                    }

                    // Sort the data, then write to disk
                    var lstScansByScore = from item in dctScanChargeBestScore orderby item.Value select item.Key;
                    var filterPassingPSMCount = 0;

                    foreach (var scanChargeCombo in lstScansByScore)
                    {
                        var hitsForScan = dctScanChargeTopHits[scanChargeCombo];
                        var lastPeptide = string.Empty;

                        foreach (var psm in hitsForScan.PSMs)
                        {
                            swMergedFile.WriteLine(psm.DataLine);

                            if (!lstFilterPassingPeptides.Contains(psm.Peptide))
                            {
                                lstFilterPassingPeptides.Add(psm.Peptide);
                            }

                            if (!string.Equals(psm.Peptide, lastPeptide))
                            {
                                filterPassingPSMCount += 1;
                                lastPeptide = psm.Peptide;
                            }
                        }
                    }

                    if (m_DebugLevel >= 1)
                    {
                        LogMessage(
                            "Read " + totalLinesProcessed + " data lines from " + numberOfClonedSteps + " MSGF+ .tsv files; wrote " +
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

        private CloseOutType ParallelMSGFPlusMergePepToProtMapFiles(int numberOfClonedSteps, ICollection<string> lstFilterPassingPeptides)
        {
            try
            {
                var mergedFilePath = Path.Combine(m_WorkDir, m_Dataset + "_msgfplus_PepToProtMap.txt");

                var fiTempFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "_msgfplus_PepToProtMap.tmp"));
                m_jobParams.AddResultFileToSkip(fiTempFile.Name);

                long totalLinesProcessed = 0;
                long totalLinesToWrite = 0;

                var lstPepProtMappingWritten = new SortedSet<string>();

                var lastPeptideFull = string.Empty;
                var addCurrentPeptide = false;

                using (var swTempFile = new StreamWriter(new FileStream(fiTempFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // ReSharper disable once UseImplicitlyTypedVariableEvident

                    for (var iteration = 1; iteration <= numberOfClonedSteps; iteration++)
                    {
                        var sourceFilePath = Path.Combine(m_WorkDir, m_Dataset + "_msgfplus_Part" + iteration + "_PepToProtMap.txt");
                        var linesRead = 0;

                        if (m_DebugLevel >= 2)
                        {
                            LogDebug("Caching data from " + sourceFilePath);
                        }

                        using (var srSourceFile = new StreamReader(new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                        {
                            while (!srSourceFile.EndOfStream)
                            {
                                var strLineIn = srSourceFile.ReadLine();
                                if (string.IsNullOrWhiteSpace(strLineIn))
                                    continue;

                                linesRead += 1;
                                totalLinesProcessed += 1;

                                if (linesRead == 1 && iteration == 1)
                                {
                                    // Write the header line
                                    swTempFile.WriteLine(strLineIn);
                                    continue;
                                }

                                var charIndex = strLineIn.IndexOf('\t');
                                if (charIndex <= 0)
                                {
                                    continue;
                                }

                                var peptideFull = strLineIn.Substring(0, charIndex);
                                var peptide = clsMSGFPlusPSMs.RemovePrefixAndSuffix(peptideFull);

                                if (string.Equals(lastPeptideFull, peptideFull) || lstFilterPassingPeptides.Contains(peptide))
                                {
                                    if (!string.Equals(lastPeptideFull, peptideFull))
                                    {
                                        // Done processing the last peptide; we can now update lstPepProtMappingWritten to True for this peptide
                                        // to prevent it from getting added to the merged file again in the future

                                        if (!string.IsNullOrEmpty(lastPeptideFull))
                                        {
                                            if (!lstPepProtMappingWritten.Contains(lastPeptideFull))
                                            {
                                                lstPepProtMappingWritten.Add(lastPeptideFull);
                                            }
                                        }

                                        lastPeptideFull = string.Copy(peptideFull);
                                        addCurrentPeptide = !lstPepProtMappingWritten.Contains(peptideFull);
                                    }

                                    // Add this peptide if we didn't already add it during a previous iteration
                                    if (addCurrentPeptide)
                                    {
                                        swTempFile.WriteLine(strLineIn);
                                        totalLinesToWrite += 1;
                                    }
                                }
                            }
                        }
                    }
                }

                if (m_DebugLevel >= 1)
                {
                    LogMessage(
                        "Read " + totalLinesProcessed + " data lines from " + numberOfClonedSteps + " _PepToProtMap files; now sorting the " +
                        totalLinesToWrite + " merged peptides using FlexibleFileSortUtility.dll");
                }

                Thread.Sleep(250);

                var success = SortTextFile(fiTempFile.FullName, mergedFilePath, true);

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

        /// <summary>
        /// Perform peptide hit extraction for Sequest data
        /// </summary>
        /// <returns>CloseOutType representing success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType PerformPeptideExtraction()
        {
            var pepExtractTool = new clsPeptideExtractWrapper(m_mgrParams, m_jobParams, ref m_StatusTools);

            // Run the extractor
            if (m_DebugLevel > 3)
            {
                LogDebug("clsExtractToolRunner.PerformPeptideExtraction(); Starting peptide extraction");
            }

            try
            {
                var eResult = pepExtractTool.PerformExtraction();

                if ((eResult != CloseOutType.CLOSEOUT_SUCCESS) && (eResult != CloseOutType.CLOSEOUT_NO_DATA))
                {
                    // Log error and return result calling routine handles the error appropriately
                    LogError("Error encountered during extraction");
                    return eResult;
                }

                // If there was a _syn.txt file created, but it contains no data, then we want to clean up and exit
                if (eResult == CloseOutType.CLOSEOUT_NO_DATA)
                {
                    // Log error and return result calling routine handles the error appropriately
                    LogError("No results above threshold");
                    return eResult;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception running extraction tool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Runs PeptideHitsResultsProcessor on Sequest output
        /// </summary>
        /// <returns>CloseOutType representing success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType RunPhrpForSequest()
        {
            string msg;
            CloseOutType eResult;
            string strSynFilePath;

            var phrp = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
            RegisterEvents(phrp);

            phrp.ProgressUpdate -= ProgressUpdateHandler;
            phrp.ProgressChanged += PHRP_ProgressChanged;

            // Run the processor
            if (m_DebugLevel > 3)
            {
                msg = "clsExtractToolRunner.RunPhrpForSequest(); Starting PHRP";
                LogDebug(msg);
            }
            try
            {
                var strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_syn.txt");
                strSynFilePath = string.Copy(strTargetFilePath);

                eResult = phrp.ExtractDataFromResults(strTargetFilePath, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_SEQUEST);
            }
            catch (Exception ex)
            {
                LogError("Exception running PHRP", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                msg = "Error running PHRP";
                if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                    msg += "; " + phrp.ErrMsg;
                LogDebug(msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Validate that the mass errors are within tolerance
            var strParamFileName = m_jobParams.GetParam("ParmFileName");
            if (!ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.Sequest, strParamFileName))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPhrpForXTandem()
        {
            string msg;
            string strSynFilePath;

            var phrp = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
            RegisterEvents(phrp);

            phrp.ProgressUpdate -= ProgressUpdateHandler;
            phrp.ProgressChanged += PHRP_ProgressChanged;

            // Run the processor
            if (m_DebugLevel > 2)
            {
                msg = "clsExtractToolRunner.RunPhrpForXTandem(); Starting PHRP";
                LogDebug(msg);
            }

            try
            {
                var strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_xt.xml");
                strSynFilePath = Path.Combine(m_WorkDir, m_Dataset + "_xt.txt");

                var eResult = phrp.ExtractDataFromResults(strTargetFilePath, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_XTANDEM);

                if ((eResult != CloseOutType.CLOSEOUT_SUCCESS))
                {
                    msg = "Error running PHRP";
                    if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                        msg += "; " + phrp.ErrMsg;
                    LogDebug(msg);
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
            if (!ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.XTandem, "input.xml"))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPhrpForMSAlign()
        {
            string strSynFilePath;

            var phrp = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
            RegisterEvents(phrp);

            phrp.ProgressUpdate -= ProgressUpdateHandler;
            phrp.ProgressChanged += PHRP_ProgressChanged;

            // Run the processor
            if (m_DebugLevel > 3)
            {
                LogDebug("clsExtractToolRunner.RunPhrpForMSAlign(); Starting PHRP");
            }

            try
            {
                // Create the Synopsis file using the _MSAlign_ResultTable.txt file
                var strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_MSAlign_ResultTable.txt");
                strSynFilePath = Path.Combine(m_WorkDir, m_Dataset + "_msalign_syn.txt");

                var eResult = phrp.ExtractDataFromResults(strTargetFilePath, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MSALIGN);

                if ((eResult != CloseOutType.CLOSEOUT_SUCCESS))
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
            const clsPHRPReader.ePeptideHitResultType eResultType = clsPHRPReader.ePeptideHitResultType.MSAlign;

            var objSummarizer = new clsMSGFResultsSummarizer(eResultType, m_Dataset, m_JobNum, m_WorkDir);
            RegisterEvents(objSummarizer);
            objSummarizer.ErrorEvent += MSGFResultsSummarizer_ErrorHandler;

            objSummarizer.MSGFThreshold = clsMSGFResultsSummarizer.DEFAULT_MSGF_THRESHOLD;

            objSummarizer.ContactDatabase = true;
            objSummarizer.PostJobPSMResultsToDB = false;
            objSummarizer.SaveResultsToTextFile = false;
            objSummarizer.DatasetName = m_Dataset;

            var blnSuccess = objSummarizer.ProcessMSGFResults();

            if (!blnSuccess)
            {
                if (string.IsNullOrEmpty(objSummarizer.ErrorMessage))
                {
                    LogError("Error summarizing the PSMs using clsMSGFResultsSummarizer");
                }
                else
                {
                    LogError("Error summarizing the PSMs: " + objSummarizer.ErrorMessage);
                }

                LogError("RunPhrpForMSAlign: " + m_message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Validate that the mass errors are within tolerance
            var strParamFileName = m_jobParams.GetParam("ParmFileName");
            if (!ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.MSAlign, strParamFileName))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPhrpForMODa(string strFilteredMODaResultsFilePath)
        {
            var currentStep = "Initializing";

            try
            {
                var phrp = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
                RegisterEvents(phrp);

                phrp.ProgressUpdate -= ProgressUpdateHandler;
                phrp.ProgressChanged += PHRP_ProgressChanged;

                // Run the processor
                if (m_DebugLevel > 3)
                {
                    LogDebug("clsExtractToolRunner.RunPhrpForMODa(); Starting PHRP");
                }

                try
                {
                    // The goal:
                    //   Create the _syn.txt files from the _moda.id.txt file

                    currentStep = "Looking for the results file";

                    if (!File.Exists(strFilteredMODaResultsFilePath))
                    {
                        LogError("Filtered MODa results file not found: " + Path.GetFileName(strFilteredMODaResultsFilePath));
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    var strSynFilePath = Path.Combine(m_WorkDir, m_Dataset + "_moda_syn.txt");

                    currentStep = "Running PHRP";

                    // Create the Synopsis and First Hits files using the _moda.id.txt file
                    const bool CreateFirstHitsFile = true;
                    const bool CreateSynopsisFile = true;

                    var eResult = phrp.ExtractDataFromResults(strFilteredMODaResultsFilePath, CreateFirstHitsFile, CreateSynopsisFile,
                        mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MODA);

                    if ((eResult != CloseOutType.CLOSEOUT_SUCCESS))
                    {
                        var msg = "Error running PHRP";
                        if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                            msg += "; " + phrp.ErrMsg;
                        LogWarning(msg);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    currentStep = "Verifying results exist";

                    // Confirm that the synopsis file was made
                    if (!File.Exists(strSynFilePath))
                    {
                        LogError("Synopsis file not found: " + Path.GetFileName(strSynFilePath));
                        return CloseOutType.CLOSEOUT_NO_DATA;
                    }

                    // Skip the _moda.id.txt file
                    m_jobParams.AddResultFileToSkip(strFilteredMODaResultsFilePath);
                }
                catch (Exception ex)
                {
                    LogError("Exception running PHRP", ex);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Could validate that the mass errors are within tolerance
                // var paramFileName = m_jobParams.GetParam("ParmFileName");
                // if (!ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.MODa, paramFileName))
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
                var phrp = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
                RegisterEvents(phrp);

                phrp.ProgressUpdate -= ProgressUpdateHandler;
                phrp.ProgressChanged += PHRP_ProgressChanged;

                // Run the processor
                if (m_DebugLevel > 3)
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

                    var strSynFilePath = Path.Combine(m_WorkDir, m_Dataset + "_modp_syn.txt");

                    currentStep = "Running PHRP";

                    // Create the Synopsis file using the _modp.id.txt file
                    const bool CreateFirstHitsFile = false;
                    const bool CreateSynopsisFile = true;

                    var eResult = phrp.ExtractDataFromResults(filteredMODPlusResultsFilePath, CreateFirstHitsFile, CreateSynopsisFile,
                        mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MODPLUS);

                    if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        var msg = "Error running PHRP";
                        if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                            msg += "; " + phrp.ErrMsg;
                        LogWarning(msg);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    currentStep = "Verifying results exist";

                    // Confirm that the synopsis file was made
                    if (!File.Exists(strSynFilePath))
                    {
                        LogError("Synopsis file not found: " + Path.GetFileName(strSynFilePath));
                        return CloseOutType.CLOSEOUT_NO_DATA;
                    }
                }
                catch (Exception ex)
                {
                    LogError("Exception running PHRP", ex);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Could validate that the mass errors are within tolerance
                // var paramFileName = m_jobParams.GetParam("ParmFileName");
                // if (!ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.MODPlus, paramFileName))
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
                var phrp = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
                RegisterEvents(phrp);

                phrp.ProgressUpdate -= ProgressUpdateHandler;
                phrp.ProgressChanged += PHRP_ProgressChanged;

                // Run the processor
                if (m_DebugLevel > 3)
                {
                    LogDebug("clsExtractToolRunner.RunPhrpForMSGFPlus(); Starting PHRP");
                }

                string strSynFilePath;
                try
                {
                    // The goal:
                    //   Create the _fht.txt and _syn.txt files from the _msgfplus.txt file (which should already have been unzipped from the _msgfplus.zip file)
                    //   or from the _msgfplus.tsv file

                    currentStep = "Determining results file type based on the results file name";

                    var splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", false);
                    var numberOfClonedSteps = 1;

                    var strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_msgfplus.txt");
                    CloseOutType eResult;
                    if (!File.Exists(strTargetFilePath))
                    {
                        // Processing MSGF+ results, work with .tsv files

                        if (splitFastaEnabled)
                        {
                            numberOfClonedSteps = m_jobParams.GetJobParameter("NumberOfClonedSteps", 0);
                        }

                        // ReSharper disable once UseImplicitlyTypedVariableEvident
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
                            strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + toolNameTag + suffixToAdd + ".tsv");

                            if (!File.Exists(strTargetFilePath))
                            {
                                var strTargetFilePathAlt = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(strTargetFilePath, "Dataset_msgfdb.txt");
                                if (File.Exists(strTargetFilePathAlt))
                                {
                                    strTargetFilePath = strTargetFilePathAlt;
                                    toolNameTag = "_msgfdb";
                                }
                            }

                            if (!File.Exists(strTargetFilePath))
                            {
                                // Need to create the .tsv file
                                currentStep = "Creating .tsv file " + strTargetFilePath;

                                strTargetFilePath = ConvertMZIDToTSV(suffixToAdd);
                                if (string.IsNullOrEmpty(strTargetFilePath))
                                {
                                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                                }
                            }

                            var strPeptoProtMapFilePath = Path.Combine(m_WorkDir, m_Dataset + toolNameTag + suffixToAdd + "_PepToProtMap.txt");

                            if (!File.Exists(strPeptoProtMapFilePath))
                            {
                                var skipPeptideToProteinMapping = m_jobParams.GetJobParameter("SkipPeptideToProteinMapping", false);

                                if (skipPeptideToProteinMapping)
                                {
                                    LogMessage("Skipping PeptideToProteinMapping since job parameter SkipPeptideToProteinMapping is True");
                                }
                                else
                                {
                                    eResult = CreateMSGFPlusResultsProteinToPeptideMappingFile(strTargetFilePath);
                                    if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                                    {
                                        return eResult;
                                    }
                                }
                            }
                        }

                        if (splitFastaEnabled)
                        {
                            currentStep = "Merging Parallel MSGF+ results";

                            // Keys in this dictionary are peptide sequences; values indicate whether the peptide (and its associated proteins) has been written to the merged _PepToProtMap.txt file
                            SortedSet<string> lstFilterPassingPeptides;

                            var numberOfHitsPerScanToKeep = m_jobParams.GetJobParameter("MergeResultsToKeepPerScan", 2);
                            if (numberOfHitsPerScanToKeep < 1)
                                numberOfHitsPerScanToKeep = 1;

                            // Merge the TSV files (keeping the top scoring hit (or hits) for each scan)
                            currentStep = "Merging the TSV files";
                            eResult = ParallelMSGFPlusMergeTSVFiles(numberOfClonedSteps, numberOfHitsPerScanToKeep, out lstFilterPassingPeptides);

                            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                            {
                                return eResult;
                            }

                            // Merge the _PepToProtMap files (making sure we don't have any duplicates, and only keeping peptides that passed the filters)
                            currentStep = "Merging the _PepToProtMap files";
                            eResult = ParallelMSGFPlusMergePepToProtMapFiles(numberOfClonedSteps, lstFilterPassingPeptides);

                            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                            {
                                return eResult;
                            }

                            strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_msgfplus.tsv");
                        }
                    }

                    strSynFilePath = Path.Combine(m_WorkDir, m_Dataset + "_msgfplus_syn.txt");

                    // Create the Synopsis and First Hits files using the _msgfplus.txt file
                    var createMSGFPlusFirstHitsFile = true;
                    var createMSGFPlusSynopsisFile = true;

                    eResult = phrp.ExtractDataFromResults(strTargetFilePath, createMSGFPlusFirstHitsFile, createMSGFPlusSynopsisFile,
                        mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MSGFPLUS);

                    if ((eResult != CloseOutType.CLOSEOUT_SUCCESS))
                    {
                        var msg = "Error running PHRP";
                        if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                            msg += "; " + phrp.ErrMsg;
                        LogWarning(msg);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (splitFastaEnabled)
                    {
                        // Zip the MSGFPlus_ConsoleOutput files
                        ZipConsoleOutputFiles();
                    }
                    else
                    {
                        try
                        {
                            // Delete the _msgfplus.txt or _msgfplus.tsv file
                            File.Delete(strTargetFilePath);
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
                var strParamFileName = m_jobParams.GetParam("ParmFileName");
                if (!ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.MSGFDB, strParamFileName))
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
                var phrp = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
                RegisterEvents(phrp);

                phrp.ProgressUpdate -= ProgressUpdateHandler;
                phrp.ProgressChanged += PHRP_ProgressChanged;

                // Run the processor
                if (m_DebugLevel > 3)
                {
                    LogDebug("clsExtractToolRunner.RunPhrpForMODa(); Starting PHRP");
                }

                var strSynFilePath = Path.Combine(m_WorkDir, m_Dataset + "_mspath_syn.txt");

                try
                {
                    // The goal:
                    //   Create the _syn.txt files from the _IcTda.tsv file

                    currentStep = "Looking for the results file";

                    var msPathFinderResultsFilePath = Path.Combine(m_WorkDir, m_Dataset + "_IcTDA.tsv");
                    if (!File.Exists(msPathFinderResultsFilePath))
                    {
                        LogError("MSPathFinder results file not found: " + Path.GetFileName(msPathFinderResultsFilePath));
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    currentStep = "Running PHRP";

                    // Create the Synopsis file using the _IcTDA.tsv file
                    const bool CreateFirstHitsFile = false;
                    const bool CreateSynopsisFile = true;

                    var eResult = phrp.ExtractDataFromResults(msPathFinderResultsFilePath, CreateFirstHitsFile, CreateSynopsisFile,
                        mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MSPATHFINDER);

                    if ((eResult != CloseOutType.CLOSEOUT_SUCCESS))
                    {
                        var msg = "Error running PHRP";
                        if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                            msg += "; " + phrp.ErrMsg;
                        LogWarning(msg);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    currentStep = "Verifying results exist";

                    // Confirm that the synopsis file was made
                    if (!File.Exists(strSynFilePath))
                    {
                        LogError("Synopsis file not found: " + Path.GetFileName(strSynFilePath));
                        return CloseOutType.CLOSEOUT_NO_DATA;
                    }
                }
                catch (Exception ex)
                {
                    LogError("Exception running PHRP", ex);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Validate that the mass errors are within tolerance
                var strParamFileName = m_jobParams.GetParam("ParmFileName");
                if (!ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.MSPathFinder, strParamFileName))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in RunPhrpForMODa at step " + currentStep, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void ZipConsoleOutputFiles()
        {
            var diWorkingDirectory = new DirectoryInfo(m_WorkDir);
            var consoleOutputFiles = new List<string>();

            var diConsoleOutputFiles = new DirectoryInfo(Path.Combine(diWorkingDirectory.FullName, "ConsoleOutputFiles"));
            diConsoleOutputFiles.Create();

            foreach (var fiFile in diWorkingDirectory.GetFiles("MSGFPlus_ConsoleOutput_Part*.txt"))
            {
                var targetPath = Path.Combine(diConsoleOutputFiles.FullName, fiFile.Name);
                fiFile.MoveTo(targetPath);
                consoleOutputFiles.Add(fiFile.Name);
            }

            if (consoleOutputFiles.Count == 0)
            {
                LogError("MSGF+ Console output files not found");
                return;
            }

            var zippedConsoleOutputFilePath = Path.Combine(diWorkingDirectory.FullName, "MSGFPlus_ConsoleOutput_Files.zip");
            if (!m_IonicZipTools.ZipDirectory(diConsoleOutputFiles.FullName, zippedConsoleOutputFilePath))
            {
                LogError("Problem zipping the ConsoleOutput files; will not delete the separate copies from the transfer folder");
                return;
            }

            var transferFolderPath = GetTransferFolderPath();

            if (string.IsNullOrEmpty(transferFolderPath))
            {
                // Error has already been logged
                return;
            }

            if (string.IsNullOrEmpty(m_ResFolderName))
            {
                // Ignore error; will be logged in function
                return;
            }

            foreach (var consoleOutputfile in consoleOutputFiles)
            {
                var targetPath = Path.Combine(transferFolderPath, m_Dataset, m_ResFolderName, consoleOutputfile);
                m_jobParams.AddServerFileToDelete(targetPath);
            }
        }

        private CloseOutType RunPhrpForInSpecT()
        {
            string strSynFilePath;

            var phrp = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
            RegisterEvents(phrp);

            phrp.ProgressUpdate -= ProgressUpdateHandler;
            phrp.ProgressChanged += PHRP_ProgressChanged;

            // Run the processor
            if (m_DebugLevel > 3)
            {
                LogDebug("clsExtractToolRunner.RunPhrpForInSpecT(); Starting PHRP");
            }

            try
            {
                // The goal:
                //   Get the _fht.txt and _FScore_fht.txt files from the _inspect.txt file in the _inspect_fht.zip file
                //   Get the other files from the _inspect.txt file in the_inspect.zip file

                // Extract _inspect.txt from the _inspect_fht.zip file
                var strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_inspect_fht.zip");
                var blnSuccess = UnzipFile(strTargetFilePath);

                if (!blnSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Create the First Hits files using the _inspect.txt file
                var createInspectFirstHitsFile = true;
                var createInspectSynopsisFile = false;
                strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_inspect.txt");
                var eResult = phrp.ExtractDataFromResults(strTargetFilePath, createInspectFirstHitsFile, createInspectSynopsisFile,
                    mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_INSPECT);

                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    var msg = "Error running PHRP";
                    if (!string.IsNullOrWhiteSpace(phrp.ErrMsg))
                        msg += "; " + phrp.ErrMsg;
                    LogWarning(msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Delete the _inspect.txt file
                File.Delete(strTargetFilePath);

                Thread.Sleep(250);

                // Extract _inspect.txt from the _inspect.zip file
                strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_inspect.zip");
                blnSuccess = UnzipFile(strTargetFilePath);

                if (!blnSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Create the Synopsis files using the _inspect.txt file
                createInspectFirstHitsFile = false;
                createInspectSynopsisFile = true;
                strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_inspect.txt");
                strSynFilePath = Path.Combine(m_WorkDir, m_Dataset + "_inspect_syn.txt");

                eResult = phrp.ExtractDataFromResults(strTargetFilePath, createInspectFirstHitsFile, createInspectSynopsisFile,
                    mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_INSPECT);

                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
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
                    File.Delete(strTargetFilePath);
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
            var strParamFileName = m_jobParams.GetParam("ParmFileName");
            if (!ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.Inspect, strParamFileName))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPeptideProphet()
        {
            const int SYN_FILE_MAX_SIZE_MB = 200;
            const string PEPPROPHET_RESULT_FILE_SUFFIX = "_PepProphet.txt";

            string strPepProphetOutputFilePath;

            var eResult = CloseOutType.CLOSEOUT_SUCCESS;

            bool blnSuccess;

            var blnIgnorePeptideProphetErrors = m_jobParams.GetJobParameter("IgnorePeptideProphetErrors", false);

            var progLoc = m_mgrParams.GetParam("PeptideProphetRunnerProgLoc");

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

            m_PeptideProphet = new clsPeptideProphetWrapper(progLoc);
            m_PeptideProphet.PeptideProphetRunning += m_PeptideProphet_PeptideProphetRunning;

            if (m_DebugLevel >= 3)
            {
                LogDebug("clsExtractToolRunner.RunPeptideProphet(); Starting Peptide Prophet");
            }

            var SynFile = Path.Combine(m_WorkDir, m_Dataset + "_syn.txt");

            // Check to see if Syn file exists
            var fiSynFile = new FileInfo(SynFile);
            if (!fiSynFile.Exists)
            {
                LogError("clsExtractToolRunner.RunPeptideProphet(); Syn file " + SynFile + " not found; unable to run peptide prophet");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            List<string> splitFileList;

            // Check the size of the Syn file
            // If it is too large, then we will need to break it up into multiple parts, process each part separately, and then combine the results
            var sngParentSynFileSizeMB = (float)(fiSynFile.Length / 1024.0 / 1024.0);
            if (sngParentSynFileSizeMB <= SYN_FILE_MAX_SIZE_MB)
            {
                splitFileList = new List<string> {
                    fiSynFile.FullName
                };
            }
            else
            {
                if (m_DebugLevel >= 2)
                {
                    LogDebug(
                        "Synopsis file is " + sngParentSynFileSizeMB.ToString("0.0") +
                        " MB, which is larger than the maximum size for peptide prophet (" + SYN_FILE_MAX_SIZE_MB +
                        " MB); splitting into multiple sections");
                }

                // File is too large; split it into multiple chunks
                blnSuccess = SplitFileRoundRobin(fiSynFile.FullName, SYN_FILE_MAX_SIZE_MB * 1024 * 1024, true, out splitFileList);

                if (blnSuccess)
                {
                    if (m_DebugLevel >= 3)
                    {
                        LogDebug("Synopsis file was split into " + splitFileList.Count + " sections by SplitFileRoundRobin");
                    }
                }
                else
                {
                    var msg = "Error splitting synopsis file that is over " + SYN_FILE_MAX_SIZE_MB + " MB in size";

                    if (blnIgnorePeptideProphetErrors)
                    {
                        msg += "; Ignoring the error since 'IgnorePeptideProphetErrors' = True";
                        LogWarning(msg);
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }

                    LogError(msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Setup Peptide Prophet and run for each file in strFileList
            foreach (var splitFile in splitFileList)
            {
                m_PeptideProphet.InputFile = splitFile;
                m_PeptideProphet.Enzyme = "tryptic";
                m_PeptideProphet.OutputFolderPath = m_WorkDir;
                m_PeptideProphet.DebugLevel = m_DebugLevel;

                fiSynFile = new FileInfo(splitFile);
                var strSynFileNameAndSize = fiSynFile.Name + " (file size = " + (fiSynFile.Length / 1024.0 / 1024.0).ToString("0.00") + " MB";
                if (splitFileList.Count > 1)
                {
                    strSynFileNameAndSize += "; parent syn file is " + sngParentSynFileSizeMB.ToString("0.00") + " MB)";
                }
                else
                {
                    strSynFileNameAndSize += ")";
                }

                if (m_DebugLevel >= 1)
                {
                    LogDebug("Running peptide prophet on file " + strSynFileNameAndSize);
                }

                eResult = m_PeptideProphet.CallPeptideProphet();

                if (eResult == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Make sure the Peptide Prophet output file was actually created
                    strPepProphetOutputFilePath = Path.Combine(m_PeptideProphet.OutputFolderPath,
                        Path.GetFileNameWithoutExtension(splitFile) + PEPPROPHET_RESULT_FILE_SUFFIX);

                    if (m_DebugLevel >= 3)
                    {
                        LogDebug("Peptide prophet processing complete; checking for file " + strPepProphetOutputFilePath);
                    }

                    if (!File.Exists(strPepProphetOutputFilePath))
                    {
                        LogError("clsExtractToolRunner.RunPeptideProphet(); Peptide Prophet output file not found for synopsis file " +
                                 strSynFileNameAndSize);

                        if (!string.IsNullOrEmpty(m_PeptideProphet.ErrMsg))
                        {
                            LogError(m_PeptideProphet.ErrMsg);
                        }

                        if (blnIgnorePeptideProphetErrors)
                        {
                            LogWarning("Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' = True");
                        }
                        else
                        {
                            LogWarning("To ignore this error, update this job to use a settings file that has 'IgnorePeptideProphetErrors' set to True");
                            eResult = CloseOutType.CLOSEOUT_FAILED;
                            break;
                        }
                    }
                }
                else
                {
                    LogError("clsExtractToolRunner.RunPeptideProphet(); Error running Peptide Prophet on file " + strSynFileNameAndSize + ": " + m_PeptideProphet.ErrMsg);

                    if (blnIgnorePeptideProphetErrors)
                    {
                        LogWarning("Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' = True");
                    }
                    else
                    {
                        eResult = CloseOutType.CLOSEOUT_FAILED;
                        break;
                    }
                }
            }

            if (eResult == CloseOutType.CLOSEOUT_SUCCESS || blnIgnorePeptideProphetErrors)
            {
                if (splitFileList.Count > 1)
                {
                    // Delete each of the temporary synopsis files
                    DeleteTemporaryFiles(splitFileList);

                    // We now need to recombine the peptide prophet result files

                    // Update strFileList() to have the peptide prophet result file names
                    var strBaseName = Path.Combine(m_PeptideProphet.OutputFolderPath, Path.GetFileNameWithoutExtension(SynFile));

                    for (var intFileIndex = 0; intFileIndex <= splitFileList.Count - 1; intFileIndex++)
                    {
                        var splitFile = strBaseName + "_part" + (intFileIndex + 1) + PEPPROPHET_RESULT_FILE_SUFFIX;

                        // Add this file to the global delete list
                        m_jobParams.AddResultFileToSkip(splitFile);
                    }

                    // Define the final peptide prophet output file name
                    strPepProphetOutputFilePath = strBaseName + PEPPROPHET_RESULT_FILE_SUFFIX;

                    if (m_DebugLevel >= 2)
                    {
                        LogDebug(
                            "Combining " + splitFileList.Count + " separate Peptide Prophet result files to create " +
                            Path.GetFileName(strPepProphetOutputFilePath));
                    }

                    blnSuccess = InterleaveFiles(splitFileList, strPepProphetOutputFilePath, true);

                    // Delete each of the temporary peptide prophet result files
                    DeleteTemporaryFiles(splitFileList);

                    if (blnSuccess)
                    {
                        eResult = CloseOutType.CLOSEOUT_SUCCESS;
                    }
                    else
                    {
                        var msg = "Error interleaving the peptide prophet result files (FileCount=" + splitFileList.Count + ")";
                        if (blnIgnorePeptideProphetErrors)
                        {
                            msg += "; Ignoring the error since 'IgnorePeptideProphetErrors' = True";
                            LogWarning(msg);
                            eResult = CloseOutType.CLOSEOUT_SUCCESS;
                        }
                        else
                        {
                            LogError(msg);
                            eResult = CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                }
            }

            return eResult;
        }

        /// <summary>
        /// Deletes each file in splitFileList
        /// </summary>
        /// <param name="splitFileList">Full paths to files to delete</param>
        /// <remarks></remarks>
        private void DeleteTemporaryFiles(IReadOnlyCollection<string> splitFileList)
        {
            // Delay for 1 second
            Thread.Sleep(1000);
            PRISM.clsProgRunner.GarbageCollectNow();

            // Delete each file in strFileList
            foreach (var splitFile in splitFileList)
            {
                if (m_DebugLevel >= 5)
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
        /// Reads each file in fileList line by line, writing the lines to strCombinedFilePath
        /// Can also check for a header line on the first line; if a header line is found in the first file,
        /// then the header is also written to the combined file
        /// </summary>
        /// <param name="fileList">Files to combine</param>
        /// <param name="strCombinedFilePath">File to create</param>
        /// <param name="blnLookForHeaderLine">When true, then looks for a header line by checking if the first column contains a number</param>
        /// <returns>True if success; false if failure</returns>
        /// <remarks></remarks>
        private bool InterleaveFiles(IReadOnlyList<string> fileList, string strCombinedFilePath, bool blnLookForHeaderLine)
        {


            try
            {
                if (fileList == null || fileList.Count == 0)
                {
                    // Nothing to do
                    return false;
                }

                var intFileCount = fileList.Count;
                var srInFiles = new StreamReader[intFileCount];
                var intLinesRead = new int[intFileCount];

                // Open each of the input files
                for (var intIndex = 0; intIndex <= intFileCount - 1; intIndex++)
                {
                    if (File.Exists(fileList[intIndex]))
                    {
                        srInFiles[intIndex] = new StreamReader(new FileStream(fileList[intIndex], FileMode.Open, FileAccess.Read, FileShare.Read));
                    }
                    else
                    {
                        // File not found; unable to continue
                        LogError("Source peptide prophet file not found, unable to continue: " + fileList[intIndex]);
                        return false;
                    }
                }

                // Create the output file

                using (var swOutFile = new StreamWriter(new FileStream(strCombinedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var intTotalLinesRead = 0;
                    var blnContinueReading = true;

                    while (blnContinueReading)
                    {
                        var intTotalLinesReadSaved = intTotalLinesRead;

                        for (var intFileIndex = 0; intFileIndex <= intFileCount - 1; intFileIndex++)
                        {
                            if (srInFiles[intFileIndex].EndOfStream)
                                continue;

                            var strLineIn = srInFiles[intFileIndex].ReadLine();

                            intLinesRead[intFileIndex] += 1;
                            intTotalLinesRead += 1;

                            if (strLineIn == null)
                                continue;

                            var blnProcessLine = true;

                            if (intLinesRead[intFileIndex] == 1 && blnLookForHeaderLine && strLineIn.Length > 0)
                            {
                                // check for a header line
                                var strSplitLine = strLineIn.Split(new[] {'\t'}, 2);

                                double temp;
                                if (strSplitLine.Length > 0 && !double.TryParse(strSplitLine[0], out temp))
                                {
                                    // first column does not contain a number; this must be a header line
                                    // write the header to the output file (provided intfileindex=0)
                                    if (intFileIndex == 0)
                                    {
                                        swOutFile.WriteLine(strLineIn);
                                    }
                                    blnProcessLine = false;
                                }
                            }

                            if (blnProcessLine)
                            {
                                swOutFile.WriteLine(strLineIn);
                            }
                        }

                        if (intTotalLinesRead == intTotalLinesReadSaved)
                        {
                            blnContinueReading = false;
                        }
                    }

                    // Close the input files
                    for (var intIndex = 0; intIndex <= intFileCount - 1; intIndex++)
                    {
                        srInFiles[intIndex].Dispose();
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
        /// Reads strSrcFilePath line-by-line and splits into multiple files such that none of the output
        /// files has length greater than lngMaxSizeBytes. Can also check for a header line on the first line;
        /// if a header line is found, then all of the split files will be assigned the same header line
        /// </summary>
        /// <param name="strSrcFilePath">FilePath to parse</param>
        /// <param name="lngMaxSizeBytes">Maximum size of each file</param>
        /// <param name="blnLookForHeaderLine">When true, then looks for a header line by checking if the first column contains a number</param>
        /// <param name="splitFileList">Output array listing the full paths to the split files that were created</param>
        /// <returns>True if success, false if failure</returns>
        /// <remarks></remarks>
        private bool SplitFileRoundRobin(string strSrcFilePath, long lngMaxSizeBytes, bool blnLookForHeaderLine, out List<string> splitFileList)
        {
            bool blnSuccess;
            splitFileList = new List<string>();

            try
            {
                var fiFileInfo = new FileInfo(strSrcFilePath);
                if (!fiFileInfo.Exists)
                {
                    LogError("File not found: " + fiFileInfo.FullName);
                    return false;
                }

                if (fiFileInfo.Length <= lngMaxSizeBytes)
                {
                    // File is already less than the limit
                    splitFileList.Add(fiFileInfo.FullName);

                    return true;
                }

                if (string.IsNullOrWhiteSpace(fiFileInfo.DirectoryName))
                {
                    LogError("Cannot determine the parent directory of " + fiFileInfo.FullName);
                    return false;
                }

                // Determine the number of parts to split the file into
                var intSplitCount = (int)Math.Ceiling(fiFileInfo.Length / (float)lngMaxSizeBytes);

                if (intSplitCount < 2)
                {
                    // This code should never be reached; we'll set intSplitCount to 2
                    intSplitCount = 2;
                }

                // Open the input file
                using (var srInFile = new StreamReader(new FileStream(fiFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    // Create each of the output files
                    var swOutFiles = new StreamWriter[intSplitCount];

                    var strBaseName = Path.Combine(fiFileInfo.DirectoryName, Path.GetFileNameWithoutExtension(fiFileInfo.Name));

                    for (var intIndex = 0; intIndex <= intSplitCount - 1; intIndex++)
                    {
                        splitFileList[intIndex] = strBaseName + "_part" + (intIndex + 1) + Path.GetExtension(fiFileInfo.Name);
                        swOutFiles[intIndex] =
                            new StreamWriter(new FileStream(splitFileList[intIndex], FileMode.Create, FileAccess.Write, FileShare.Read));
                    }

                    var intLinesRead = 0;
                    var intTargetFileIndex = 0;

                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();
                        intLinesRead += 1;

                        if (strLineIn == null)
                            continue;

                        var blnProcessLine = true;

                        if (intLinesRead == 1 && blnLookForHeaderLine && strLineIn.Length > 0)
                        {
                            // Check for a header line
                            var strSplitLine = strLineIn.Split(new[] {'\t'}, 2);

                            double temp;
                            if (strSplitLine.Length > 0 && !double.TryParse(strSplitLine[0], out temp))
                            {
                                // First column does not contain a number; this must be a header line
                                // Write the header to each output file
                                for (var intIndex = 0; intIndex <= intSplitCount - 1; intIndex++)
                                {
                                    swOutFiles[intIndex].WriteLine(strLineIn);
                                }
                                blnProcessLine = false;
                            }
                        }

                        if (blnProcessLine)
                        {
                            swOutFiles[intTargetFileIndex].WriteLine(strLineIn);
                            intTargetFileIndex += 1;
                            if (intTargetFileIndex == intSplitCount)
                                intTargetFileIndex = 0;
                        }
                    }


                    // Close the output files
                    for (var intIndex = 0; intIndex <= intSplitCount - 1; intIndex++)
                    {
                        swOutFiles[intIndex].Flush();
                        swOutFiles[intIndex].Dispose();
                    }

                }

                blnSuccess = true;
            }
            catch (Exception ex)
            {
                LogError("Exception in clsExtractToolRunner.SplitFileRoundRobin", ex);
                blnSuccess = false;
            }

            return blnSuccess;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of the PeptideHitResultsProcessor

            try
            {
                var progLoc = m_mgrParams.GetParam("PHRPProgLoc");
                var diPHRP = new DirectoryInfo(progLoc);

                // verify that program file exists
                if (diPHRP.Exists)
                {
                    StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, Path.Combine(diPHRP.FullName, "PeptideHitResultsProcessor.dll"));
                }
                else
                {
                    LogError("PHRP folder not found at " + progLoc);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception determining Assembly info for the PeptideHitResultsProcessor: " + ex.Message, ex);
                return false;
            }

            if (m_jobParams.GetParam("ResultType") == clsAnalysisResources.RESULT_TYPE_SEQUEST)
            {
                // Sequest result type

                // Lookup the version of the PeptideFileExtractor
                if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "PeptideFileExtractor"))
                {
                    return false;
                }

                // Lookup the version of the PeptideProphetRunner

                var strPeptideProphetRunnerLoc = m_mgrParams.GetParam("PeptideProphetRunnerProgLoc");
                var ioPeptideProphetRunner = new FileInfo(strPeptideProphetRunnerLoc);

                if (ioPeptideProphetRunner.Exists)
                {
                    // Lookup the version of the PeptideProphetRunner
                    var blnSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, ioPeptideProphetRunner.FullName);
                    if (!blnSuccess)
                        return false;

                    if (!string.IsNullOrWhiteSpace(ioPeptideProphetRunner.DirectoryName))
                    {
                        // Lookup the version of the PeptideProphetLibrary
                        var dllPath = Path.Combine(ioPeptideProphetRunner.DirectoryName, "PeptideProphetLibrary.dll");
                        blnSuccess = StoreToolVersionInfoOneFile32Bit(ref strToolVersionInfo, dllPath);
                    }

                    if (!blnSuccess)
                        return false;
                }
            }

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message, ex);
                return false;
            }
        }

        private bool ValidatePHRPResultMassErrors(string strInputFilePath, clsPHRPReader.ePeptideHitResultType eResultType,
            string strSearchEngineParamFileName)
        {
            bool blnSuccess;

            try
            {
                var oValidator = new clsPHRPMassErrorValidator(m_DebugLevel);
                RegisterEvents(oValidator);

                var paramFilePath = Path.Combine(m_WorkDir, strSearchEngineParamFileName);

                blnSuccess = oValidator.ValidatePHRPResultMassErrors(strInputFilePath, eResultType, paramFilePath);
                if (!blnSuccess)
                {
                    var toolName = m_jobParams.GetJobParameter("ToolName", "");

                    if (toolName.ToLower().StartsWith("inspect"))
                    {
                        // Ignore this error for inspect if running an unrestricted search
                        var paramFileName = m_jobParams.GetJobParameter("ParmFileName", "");
                        if (paramFileName.IndexOf("Unrestrictive", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            blnSuccess = true;
                        }
                    }

                    if (!blnSuccess)
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
                blnSuccess = false;
            }

            return blnSuccess;
        }

        #endregion

        #region "Event handlers"

        private DateTime dtLastPepProphetStatusLog = DateTime.MinValue;

        private void m_PeptideProphet_PeptideProphetRunning(string PepProphetStatus, float PercentComplete)
        {
            const int PEPPROPHET_DETAILED_LOG_INTERVAL_SECONDS = 60;
            m_progress = SEQUEST_PROGRESS_PHRP_DONE + (float)(PercentComplete / 3.0);
            m_StatusTools.UpdateAndWrite(m_progress);

            if (m_DebugLevel >= 4)
            {
                if (DateTime.UtcNow.Subtract(dtLastPepProphetStatusLog).TotalSeconds >= PEPPROPHET_DETAILED_LOG_INTERVAL_SECONDS)
                {
                    dtLastPepProphetStatusLog = DateTime.UtcNow;
                    LogDebug("Running peptide prophet: " + PepProphetStatus + "; " + PercentComplete + "% complete");
                }
            }
        }

        private DateTime dtLastPHRPStatusLog = DateTime.MinValue;

        private void PHRP_ProgressChanged(string taskDescription, float percentComplete)
        {
            const int PHRP_LOG_INTERVAL_SECONDS = 180;
            const int PHRP_DETAILED_LOG_INTERVAL_SECONDS = 20;

            m_progress = SEQUEST_PROGRESS_EXTRACTION_DONE + (float)(percentComplete / 3.0);
            m_StatusTools.UpdateAndWrite(m_progress);

            if (m_DebugLevel >= 1)
            {
                if (DateTime.UtcNow.Subtract(dtLastPHRPStatusLog).TotalSeconds >= PHRP_DETAILED_LOG_INTERVAL_SECONDS && m_DebugLevel >= 3 ||
                    DateTime.UtcNow.Subtract(dtLastPHRPStatusLog).TotalSeconds >= PHRP_LOG_INTERVAL_SECONDS)
                {
                    dtLastPHRPStatusLog = DateTime.UtcNow;
                    LogDebug("Running PHRP: " + taskDescription + "; " + percentComplete + "% complete");
                }
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

        #endregion
    }
}
