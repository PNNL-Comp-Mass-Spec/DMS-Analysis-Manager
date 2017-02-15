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

        protected const float SEQUEST_PROGRESS_EXTRACTION_DONE = 33;
        protected const float SEQUEST_PROGRESS_PHRP_DONE = 66;
        protected const float SEQUEST_PROGRESS_PEPPROPHET_DONE = 100;

        public const string INSPECT_UNFILTERED_RESULTS_FILE_SUFFIX = "_inspect_unfiltered.txt";

        protected const string MODa_JAR_NAME = "moda.jar";
        protected const string MODa_FILTER_JAR_NAME = "anal_moda.jar";

        protected const string MODPlus_JAR_NAME = "modp_pnnl.jar";
        protected const string MODPlus_FILTER_JAR_NAME = "tda_plus.jar";

        #endregion

        #region "Module variables"

        protected clsPeptideProphetWrapper m_PeptideProphet;
        protected clsPepHitResultsProcWrapper m_PHRP;

        protected clsMSGFDBUtils mMSGFDBUtils;
        protected bool mMSGFDBUtilsError;

        protected string mGeneratedFastaFilePath;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs the data extraction tool(s)
        /// </summary>
        /// <returns>CloseOutType representing success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            string msg = null;

            var strCurrentAction = "preparing for extraction";
            bool blnProcessingError = false;

            try
            {
                //Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "clsAnalysisToolRunnerDeconPeakDetector.RunTool(): Enter");
                }

                // Store the AnalysisManager version info in the database
                if (!StoreToolVersionInfo())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Aborting since StoreToolVersionInfo returned false");
                    LogError("Error determining version of Data Extraction tools");
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
                switch (m_jobParams.GetParam("ResultType"))
                {
                    case clsAnalysisResources.RESULT_TYPE_SEQUEST:   //Sequest result type
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
                        string strFilteredMODaResultsFilePath = string.Empty;
                        eResult = ConvertMODaResultsToTxt(out strFilteredMODaResultsFilePath, true);
                        if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            blnProcessingError = true;
                            break;
                        }

                        // Run PHRP
                        strCurrentAction = "running peptide hits result processor for MODa";
                        eResult = RunPhrpForMODa(strFilteredMODaResultsFilePath);
                        if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            blnProcessingError = true;
                        }

                        // Convert the MODa results to a tab-delimited file, filter by FDR (and filter out the reverse-hit proteins)
                        eResult = ConvertMODaResultsToTxt(out strFilteredMODaResultsFilePath, false);
                        if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            blnProcessingError = true;
                            break;
                        }

                        break;
                    case clsAnalysisResources.RESULT_TYPE_MODPLUS:
                        // Convert the MODPlus results to a tab-delimited file; do not filter out the reversed-hit proteins
                        string strFilteredMODPlusResultsFilePath = string.Empty;
                        eResult = ConvertMODPlusResultsToTxt(out strFilteredMODPlusResultsFilePath, true);
                        if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            blnProcessingError = true;
                            break;
                        }

                        // Run PHRP
                        strCurrentAction = "running peptide hits result processor for MODPlus";
                        eResult = RunPhrpForMODPlus(strFilteredMODPlusResultsFilePath);
                        if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            blnProcessingError = true;
                        }

                        // Convert the MODa results to a tab-delimited file, filter by FDR (and filter out the reverse-hit proteins)
                        eResult = ConvertMODPlusResultsToTxt(out strFilteredMODPlusResultsFilePath, false);
                        if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            blnProcessingError = true;
                            break;
                        }

                        break;
                    case clsAnalysisResources.RESULT_TYPE_MSPATHFINDER:
                        // Run PHRP
                        strCurrentAction = "running peptide hits result processor for MSPathFinder";
                        eResult = RunPHRPForMSPathFinder();
                        break;
                    default:
                        // Should never get here - invalid result type specified
                        msg = "Invalid ResultType specified: " + m_jobParams.GetParam("ResultType");
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsExtractToolRunner.RunTool(); " + msg);
                        m_message = clsGlobal.AppendToComment(m_message, msg);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                if (eResult != CloseOutType.CLOSEOUT_SUCCESS & eResult != CloseOutType.CLOSEOUT_NO_DATA)
                {
                    msg = "Error " + strCurrentAction;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "clsExtractToolRunner.RunTool(); " + msg);
                    m_message = clsGlobal.AppendToComment(m_message, msg);
                    blnProcessingError = true;
                }
                else
                {
                    m_progress = 100;
                    UpdateStatusRunning(m_progress);
                    m_jobParams.AddResultFileToSkip(clsPepHitResultsProcWrapper.PHRP_LOG_FILE_NAME);
                }

                // Stop the job timer
                m_StopTime = System.DateTime.UtcNow;

                CloseOutType eReturnCode = CloseOutType.CLOSEOUT_SUCCESS;

                if (blnProcessingError)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the Result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    eReturnCode = CloseOutType.CLOSEOUT_FAILED;
                }

                // Add the current job data to the summary file
                if (!UpdateSummaryFile())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                        "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                }

                eResult = MakeResultsFolder();
                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // MakeResultsFolder handles posting to local log, so set database error message and exit
                    LogError("Error making results folder");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                eResult = MoveResultFiles();
                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // MoveResultFiles moves the Result files to the Result folder
                    LogError("Error moving files into results folder");
                    eReturnCode = CloseOutType.CLOSEOUT_FAILED;
                }

                if (blnProcessingError | eReturnCode == CloseOutType.CLOSEOUT_FAILED)
                {
                    // Try to save whatever files were moved into the results folder
                    var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
                    objAnalysisResults.CopyFailedResultsToArchiveFolder(Path.Combine(m_WorkDir, m_ResFolderName));

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                eResult = CopyResultsFolderToServer();
                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return eResult;
                }

                // Everything succeeded; now delete the _msgfplus.tsv file from the server
                // For SplitFasta files there will be multiple tsv files to delete, plus the individual ConsoleOutput.txt files (all tracked with m_jobParams.ServerFilesToDelete)
                RemoveNonResultServerFiles();
            }
            catch (Exception ex)
            {
                msg = "Exception running extraction tool: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                m_message = clsGlobal.AppendToComment(m_message, "Exception running extraction tool");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // If we got to here, everything worked so exit happily
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Convert the MODa output file to a tab-delimited text file
        /// </summary>
        /// <param name="strFilteredMODaResultsFilePath">Output parameter: path to the filtered results file</param>
        /// <returns>The path to the .txt file if successful; empty string if an error</returns>
        /// <remarks></remarks>
        protected CloseOutType ConvertMODaResultsToTxt(out string strFilteredMODaResultsFilePath, bool keepAllResults)
        {
            var fdrThreshold = m_jobParams.GetJobParameter("MODaFDRThreshold", 0.05f);
            var decoyPrefix = m_jobParams.GetJobParameter("MODaDecoyPrefix", "Reversed_");

            const bool isModPlus = false;

            return ConvertMODaOrMODPlusResultsToTxt(fdrThreshold, decoyPrefix, isModPlus, out strFilteredMODaResultsFilePath, keepAllResults);
        }

        protected CloseOutType ConvertMODPlusResultsToTxt(out string strFilteredMODPlusResultsFilePath, bool keepAllResults)
        {
            var fdrThreshold = m_jobParams.GetJobParameter("MODPlusDecoyFilterFDR", 0.05f);
            var decoyPrefix = m_jobParams.GetJobParameter("MODPlusDecoyPrefix", "Reversed_");

            const bool isModPlus = true;

            return ConvertMODaOrMODPlusResultsToTxt(fdrThreshold, decoyPrefix, isModPlus, out strFilteredMODPlusResultsFilePath, keepAllResults);
        }

        protected CloseOutType ConvertMODaOrMODPlusResultsToTxt(float fdrThreshold, string decoyPrefixJobParam, bool isModPlus,
            out string strFilteredResultsFilePath, bool keepAllResults)
        {
            strFilteredResultsFilePath = string.Empty;

            try
            {
                string toolName = null;
                string fileNameSuffix = null;
                string modxProgJarName = null;
                string modxFilterJarName = null;

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
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                            "Verifying the decoy prefix in " + fiFastaFile.Name);
                    }

                    // Determine the most common decoy prefix in the Fasta file
                    var decoyPrefixes = clsAnalysisResources.GetDefaultDecoyPrefixes();
                    var bestPrefix = new KeyValuePair<double, string>(0, string.Empty);

                    foreach (var decoyPrefix in decoyPrefixes)
                    {
                        int proteinCount = 0;
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

                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, m_EvalMessage);

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
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                        "Filtering MODa/MODPlus Results with FDR threshold " + fdrThreshold.ToString("0.00"));
                }

                const int javaMemorySize = 1000;

                // JavaProgLoc will typically be "C:\Program Files\Java\jre8\bin\java.exe"
                var JavaProgLoc = GetJavaProgLoc();
                if (string.IsNullOrEmpty(JavaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to the MODa or MODPlus program
                var modxProgLoc = DetermineProgramLocation(toolName, toolName + "ProgLoc", modxProgJarName);

                var fiMODx = new FileInfo(modxProgLoc);

                //Set up and execute a program runner to run anal_moda.jar or tda_plus.jar
                var cmdStr = " -Xmx" + javaMemorySize.ToString() + "M -jar " + Path.Combine(fiMODx.DirectoryName, modxFilterJarName);
                cmdStr += " -i " + resultsFilePath;

                if (!isModPlus)
                {
                    // Processing MODa data; include the parameter file
                    cmdStr += " -p " + paramFilePath;
                }

                cmdStr += " -fdr " + fdrThreshold;
                cmdStr += " -d " + decoyPrefixJobParam;

                // Example command line:
                // "C:\Program Files\Java\jre8\bin\java.exe" -Xmx1000M -jar C:\DMS_Programs\MODa\anal_moda.jar -i "E:\DMS_WorkDir3\QC_Shew_13_04_pt1_1_2_45min_14Nov13_Leopard_13-05-21_moda.txt" -p "E:\DMS_WorkDir3\MODa_PartTryp_Par20ppm_Frag0pt6Da" -fdr 0.05 -d XXX_
                // "C:\Program Files\Java\jre8\bin\java.exe" -Xmx1000M -jar C:\DMS_Programs\MODPlus\tda_plus.jar -i "E:\DMS_WorkDir3\QC_Shew_13_04_pt1_1_2_45min_14Nov13_Leopard_13-05-21_modp.txt" -fdr 0.05 -d Reversed_
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, JavaProgLoc + " " + cmdStr);

                var progRunner = new clsRunDosProgram(m_WorkDir)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = false,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(m_WorkDir, toolName + "_Filter_ConsoleOutput.txt")
                };
                RegisterEvents(progRunner);

                var blnSuccess = progRunner.RunProgram(JavaProgLoc, cmdStr, toolName + "_Filter", true);

                if (!blnSuccess)
                {
                    var msg = "Error parsing and filtering " + toolName + " results";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg + ", job " + m_JobNum);
                    m_message = clsGlobal.AppendToComment(m_message, msg);

                    if (progRunner.ExitCode != 0)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            modxFilterJarName + " returned a non-zero exit code: " + progRunner.ExitCode.ToString());
                    }
                    else
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            "Call to " + modxFilterJarName + " failed (but exit code is 0)");
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
        protected string ConvertMZIDToTSV(string suffixToAdd)
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
                var mzidToTsvConverterProgLoc = DetermineProgramLocation("MzidToTsvConverter", "MzidToTsvConverterProgLoc", "MzidToTsvConverter.exe");

                if (string.IsNullOrEmpty(mzidToTsvConverterProgLoc))
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("Parameter 'MzidToTsvConverter' not defined for this manager");
                    }
                    return string.Empty;
                }

                // Initialize mMSGFDBUtils
                mMSGFDBUtils = new clsMSGFDBUtils(m_mgrParams, m_jobParams, m_JobNum, m_WorkDir, m_DebugLevel, msgfPlus: true);
                RegisterEvents(mMSGFDBUtils);

                // Attach an additional handler for the ErrorEvent
                // This additional handler sets mMSGFDBUtilsError to true
                mMSGFDBUtils.ErrorEvent += mMSGFDBUtils_ErrorEvent;

                mMSGFDBUtilsError = false;

                var strTSVFilePath = mMSGFDBUtils.ConvertMZIDToTSV(mzidToTsvConverterProgLoc, m_Dataset, strMZIDFileName);

                if (mMSGFDBUtilsError)
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                    {
                        LogError("mMSGFDBUtilsError is True after call to ConvertMZIDToTSV");
                    }
                    return string.Empty;
                }

                if (!string.IsNullOrEmpty(strTSVFilePath))
                {
                    // File successfully created

                    if (!string.IsNullOrEmpty(suffixToAdd))
                    {
                        var fiTSVFile = new FileInfo(strTSVFilePath);
                        var newTSVPath = Path.Combine(fiTSVFile.Directory.FullName,
                            Path.GetFileNameWithoutExtension(strTSVFilePath) + suffixToAdd + ".tsv");
                        fiTSVFile.MoveTo(newTSVPath);
                    }

                    return strTSVFilePath;
                }

                if (string.IsNullOrEmpty(m_message))
                {
                    LogError("Error calling mMSGFDBUtils.ConvertMZIDToTSV; path not returned");
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
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Creating the missing _PepToProtMap.txt file");

            var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
            if (mMSGFDBUtils == null)
            {
                mMSGFDBUtils = new clsMSGFDBUtils(m_mgrParams, m_jobParams, m_JobNum.ToString(), m_WorkDir, m_DebugLevel, msgfPlus: true);
                RegisterEvents(mMSGFDBUtils);

                // Attach an additional handler for the ErrorEvent
                // This additional handler sets mMSGFDBUtilsError to true
                mMSGFDBUtils.ErrorEvent += mMSGFDBUtils_ErrorEvent;
            }

            mMSGFDBUtilsError = false;

            // Assume this is true
            var resultsIncludeAutoAddedDecoyPeptides = true;

            var result = mMSGFDBUtils.CreatePeptideToProteinMapping(resultsFileName, resultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder);

            if (result != CloseOutType.CLOSEOUT_SUCCESS & result != CloseOutType.CLOSEOUT_NO_DATA)
            {
                return result;
            }

            if (mMSGFDBUtilsError)
            {
                if (string.IsNullOrWhiteSpace(m_message))
                {
                    LogError("mMSGFDBUtilsError is True after call to CreatePeptideToProteinMapping");
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

                    for (int iteration = 1; iteration <= numberOfClonedSteps; iteration++)
                    {
                        var sourceFilePath = Path.Combine(m_WorkDir, m_Dataset + "_msgfplus_Part" + iteration + ".tsv");
                        var linesRead = 0;

                        if (m_DebugLevel >= 2)
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Caching data from " + sourceFilePath);
                        }

                        using (var srSourceFile = new StreamReader(new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                        {
                            while (!srSourceFile.EndOfStream)
                            {
                                var strLineIn = srSourceFile.ReadLine();
                                linesRead += 1;
                                totalLinesProcessed += 1;

                                if (linesRead == 1)
                                {
                                    if (iteration == 1)
                                    {
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
                                }
                                else
                                {
                                    var splitLine = strLineIn.Split('\t');

                                    var scanNumber = splitLine[dctHeaderMapping["ScanNum"]];
                                    var chargeState = splitLine[dctHeaderMapping["Charge"]];

                                    int scanNumberValue = 0;
                                    int chargeStateValue = 0;
                                    int.TryParse(scanNumber, out scanNumberValue);
                                    int.TryParse(chargeState, out chargeStateValue);

                                    var scanChargeCombo = scanNumber + "_" + chargeState;
                                    var peptide = splitLine[dctHeaderMapping["Peptide"]];
                                    var protein = splitLine[dctHeaderMapping["Protein"]];
                                    var specEValueText = splitLine[dctHeaderMapping["SpecEValue"]];

                                    double specEValue = 0;
                                    if (!double.TryParse(specEValueText, out specEValue))
                                    {
                                        if (warningsLogged < 10)
                                        {
                                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                                "SpecEValue was not numeric: " + specEValueText + " in " + strLineIn);
                                            warningsLogged += 1;

                                            if (warningsLogged >= 10)
                                            {
                                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                                    "Additional warnings will not be logged");
                                            }
                                        }

                                        continue;
                                    }

                                    clsMSGFPlusPSMs hitsForScan = null;

                                    clsMSGFPlusPSMs.udtPSMType udtPSM = new clsMSGFPlusPSMs.udtPSMType();
                                    udtPSM.Peptide = peptide;
                                    udtPSM.SpecEValue = specEValue;
                                    udtPSM.DataLine = strLineIn;

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
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "Sorting results for " + dctScanChargeBestScore.Count + " lines of scan/charge combos");
                    }

                    // Sort the data, then write to disk
                    var lstScansByScore = from item in dctScanChargeBestScore orderby item.Value select item.Key;
                    var filterPassingPSMCount = 0;

                    foreach (var scanChargeCombo in lstScansByScore)
                    {
                        var hitsForScan = dctScanChargeTopHits[scanChargeCombo];
                        string lastPeptide = string.Empty;

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
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
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

        private CloseOutType ParallelMSGFPlusMergePepToProtMapFiles(int numberOfClonedSteps, SortedSet<string> lstFilterPassingPeptides)
        {
            try
            {
                var mergedFilePath = Path.Combine(m_WorkDir, m_Dataset + "_msgfplus_PepToProtMap.txt");

                var fiTempFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "_msgfplus_PepToProtMap.tmp"));
                m_jobParams.AddResultFileToSkip(fiTempFile.Name);

                long totalLinesProcessed = 0;
                long totalLinesToWrite = 0;

                var lstPepProtMappingWritten = new SortedSet<string>();

                string lastPeptideFull = string.Empty;
                var addCurrentPeptide = false;

                using (var swTempFile = new StreamWriter(new FileStream(fiTempFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // ReSharper disable once UseImplicitlyTypedVariableEvident

                    for (int iteration = 1; iteration <= numberOfClonedSteps; iteration++)
                    {
                        var sourceFilePath = Path.Combine(m_WorkDir, m_Dataset + "_msgfplus_Part" + iteration + "_PepToProtMap.txt");
                        var linesRead = 0;

                        if (m_DebugLevel >= 2)
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Caching data from " + sourceFilePath);
                        }

                        using (var srSourceFile = new StreamReader(new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                        {
                            while (!srSourceFile.EndOfStream)
                            {
                                var strLineIn = srSourceFile.ReadLine();
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
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
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
            string msg = null;
            clsPeptideExtractWrapper pepExtractTool = new clsPeptideExtractWrapper(m_mgrParams, m_jobParams, ref m_StatusTools);

            //Run the extractor
            if (m_DebugLevel > 3)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                    "clsExtractToolRunner.PerformPeptideExtraction(); Starting peptide extraction");
            }

            try
            {
                var eResult = pepExtractTool.PerformExtraction();

                if ((eResult != CloseOutType.CLOSEOUT_SUCCESS) & (eResult != CloseOutType.CLOSEOUT_NO_DATA))
                {
                    //log error and return result calling routine handles the error appropriately
                    msg = "Error encountered during extraction";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "clsExtractToolRunner.PerformPeptideExtraction(); " + msg);
                    m_message = clsGlobal.AppendToComment(m_message, msg);
                    return eResult;
                }

                //If there was a _syn.txt file created, but it contains no data, then we want to clean up and exit
                if (eResult == CloseOutType.CLOSEOUT_NO_DATA)
                {
                    //log error and return result calling routine handles the error appropriately
                    LogError("No results above threshold");
                    return eResult;
                }
            }
            catch (Exception ex)
            {
                msg = "clsExtractToolRunner.PerformPeptideExtraction(); Exception running extraction tool: " + ex.Message + "; " +
                      clsGlobal.GetExceptionStackTrace(ex);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                m_message = clsGlobal.AppendToComment(m_message, "Exception running extraction tool");
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
            string msg = null;
            CloseOutType eResult;
            string strSynFilePath = null;

            m_PHRP = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
            m_PHRP.ProgressChanged += m_PHRP_ProgressChanged;

            // Run the processor
            if (m_DebugLevel > 3)
            {
                msg = "clsExtractToolRunner.RunPhrpForSequest(); Starting PHRP";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
            }
            try
            {
                string strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_syn.txt");
                strSynFilePath = string.Copy(strTargetFilePath);

                eResult = m_PHRP.ExtractDataFromResults(strTargetFilePath, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_SEQUEST);
            }
            catch (Exception ex)
            {
                msg = "clsExtractToolRunner.RunPhrpForSequest(); Exception running PHRP: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                m_message = clsGlobal.AppendToComment(m_message, "Exception running PHRP");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if ((eResult != CloseOutType.CLOSEOUT_SUCCESS))
            {
                msg = "Error running PHRP";
                if (!string.IsNullOrWhiteSpace(m_PHRP.ErrMsg))
                    msg += "; " + m_PHRP.ErrMsg;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Validate that the mass errors are within tolerance
            string strParamFileName = m_jobParams.GetParam("ParmFileName");
            if (!ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.Sequest, strParamFileName))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }
            else
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
        }

        private CloseOutType RunPhrpForXTandem()
        {
            string msg = null;
            string strSynFilePath = null;

            m_PHRP = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
            m_PHRP.ProgressChanged += m_PHRP_ProgressChanged;

            // Run the processor
            if (m_DebugLevel > 2)
            {
                msg = "clsExtractToolRunner.RunPhrpForXTandem(); Starting PHRP";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
            }

            try
            {
                string strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_xt.xml");
                strSynFilePath = Path.Combine(m_WorkDir, m_Dataset + "_xt.txt");

                var eResult = m_PHRP.ExtractDataFromResults(strTargetFilePath, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_XTANDEM);

                if ((eResult != CloseOutType.CLOSEOUT_SUCCESS))
                {
                    msg = "Error running PHRP";
                    if (!string.IsNullOrWhiteSpace(m_PHRP.ErrMsg))
                        msg += "; " + m_PHRP.ErrMsg;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                msg = "clsExtractToolRunner.RunPhrpForXTandem(); Exception running PHRP: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                m_message = clsGlobal.AppendToComment(m_message, "Exception running PHRP");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Validate that the mass errors are within tolerance
            // Use input.xml for the X!Tandem parameter file
            if (!ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.XTandem, "input.xml"))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }
            else
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
        }

        private CloseOutType RunPhrpForMSAlign()
        {
            string msg = null;

            string strTargetFilePath = null;
            string strSynFilePath = null;

            m_PHRP = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
            m_PHRP.ProgressChanged += m_PHRP_ProgressChanged;

            // Run the processor
            if (m_DebugLevel > 3)
            {
                msg = "clsExtractToolRunner.RunPhrpForMSAlign(); Starting PHRP";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
            }

            try
            {
                // Create the Synopsis file using the _MSAlign_ResultTable.txt file
                strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_MSAlign_ResultTable.txt");
                strSynFilePath = Path.Combine(m_WorkDir, m_Dataset + "_msalign_syn.txt");

                var eResult = m_PHRP.ExtractDataFromResults(strTargetFilePath, mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MSALIGN);

                if ((eResult != CloseOutType.CLOSEOUT_SUCCESS))
                {
                    msg = "Error running PHRP";
                    if (!string.IsNullOrWhiteSpace(m_PHRP.ErrMsg))
                        msg += "; " + m_PHRP.ErrMsg;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                msg = "clsExtractToolRunner.RunPhrpForMSAlign(); Exception running PHRP: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                m_message = clsGlobal.AppendToComment(m_message, "Exception running PHRP");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Summarize the number of PSMs in _msalign_syn.txt
            // ReSharper disable once UseImplicitlyTypedVariableEvident
            const clsPHRPReader.ePeptideHitResultType eResultType = clsPHRPReader.ePeptideHitResultType.MSAlign;
            var job = 0;
            bool blnPostResultsToDB = false;

            if (int.TryParse(m_JobNum, out job))
            {
                blnPostResultsToDB = true;
            }
            else
            {
                blnPostResultsToDB = false;
                msg = "Job number is not numeric: " + m_JobNum + "; will not be able to post PSM results to the database";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
            }

            var objSummarizer = new clsMSGFResultsSummarizer(eResultType, m_Dataset, job, m_WorkDir);
            objSummarizer.ErrorEvent += MSGFResultsSummarizer_ErrorHandler;

            objSummarizer.MSGFThreshold = clsMSGFResultsSummarizer.DEFAULT_MSGF_THRESHOLD;

            objSummarizer.ContactDatabase = true;
            objSummarizer.PostJobPSMResultsToDB = blnPostResultsToDB;
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

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "RunPhrpForMSAlign: " + m_message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Validate that the mass errors are within tolerance
            string strParamFileName = m_jobParams.GetParam("ParmFileName");
            if (!ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.MSAlign, strParamFileName))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }
            else
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
        }

        private CloseOutType RunPhrpForMODa(string strFilteredMODaResultsFilePath)
        {
            var currentStep = "Initializing";

            string msg = null;

            try
            {
                m_PHRP = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
                m_PHRP.ProgressChanged += m_PHRP_ProgressChanged;

                // Run the processor
                if (m_DebugLevel > 3)
                {
                    msg = "clsExtractToolRunner.RunPhrpForMODa(); Starting PHRP";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
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

                    var eResult = m_PHRP.ExtractDataFromResults(strFilteredMODaResultsFilePath, CreateFirstHitsFile, CreateSynopsisFile,
                        mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MODA);

                    if ((eResult != CloseOutType.CLOSEOUT_SUCCESS))
                    {
                        msg = "Error running PHRP";
                        if (!string.IsNullOrWhiteSpace(m_PHRP.ErrMsg))
                            msg += "; " + m_PHRP.ErrMsg;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
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
                    msg = "clsExtractToolRunner.RunPhrpForMODa(); Exception running PHRP: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    m_message = clsGlobal.AppendToComment(m_message, "Exception running PHRP");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                //' Validate that the mass errors are within tolerance
                //Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
                //If Not ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.MODa, strParamFileName) Then
                //	Return CloseOutType.CLOSEOUT_FAILED
                //Else
                //	Return CloseOutType.CLOSEOUT_SUCCESS
                //End If
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in RunPhrpForMODa at step " + currentStep, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPhrpForMODPlus(string filteredMODPlusResultsFilePath)
        {
            var currentStep = "Initializing";

            string msg = null;

            try
            {
                m_PHRP = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
                m_PHRP.ProgressChanged += m_PHRP_ProgressChanged;

                // Run the processor
                if (m_DebugLevel > 3)
                {
                    msg = "clsExtractToolRunner.RunPhrpForMODPlus(); Starting PHRP";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
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

                    var eResult = m_PHRP.ExtractDataFromResults(filteredMODPlusResultsFilePath, CreateFirstHitsFile, CreateSynopsisFile,
                        mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MODPLUS);

                    if ((eResult != CloseOutType.CLOSEOUT_SUCCESS))
                    {
                        msg = "Error running PHRP";
                        if (!string.IsNullOrWhiteSpace(m_PHRP.ErrMsg))
                            msg += "; " + m_PHRP.ErrMsg;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
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
                    msg = "clsExtractToolRunner.RunPhrpForMODPlus(); Exception running PHRP: " + ex.Message + "; " +
                          clsGlobal.GetExceptionStackTrace(ex);
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    m_message = clsGlobal.AppendToComment(m_message, "Exception running PHRP");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                //' Validate that the mass errors are within tolerance
                //Dim strParamFileName As String = m_jobParams.GetParam("ParmFileName")
                //If Not ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.MODPlus, strParamFileName) Then
                //	Return CloseOutType.CLOSEOUT_FAILED
                //Else
                //	Return CloseOutType.CLOSEOUT_SUCCESS
                //End If
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in RunPhrpForMODPlus at step " + currentStep, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType RunPhrpForMSGFPlus()
        {
            var currentStep = "Initializing";

            string msg = null;

            string strTargetFilePath = null;
            string strSynFilePath = null;

            try
            {
                m_PHRP = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
                m_PHRP.ProgressChanged += m_PHRP_ProgressChanged;

                // Run the processor
                if (m_DebugLevel > 3)
                {
                    msg = "clsExtractToolRunner.RunPhrpForMSGFPlus(); Starting PHRP";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                }

                try
                {
                    // The goal:
                    //   Create the _fht.txt and _syn.txt files from the _msgfplus.txt file (which should already have been unzipped from the _msgfplus.zip file)
                    //   or from the _msgfplus.tsv file

                    currentStep = "Determining results file type based on the results file name";

                    var splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", false);
                    var numberOfClonedSteps = 1;

                    strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_msgfplus.txt");
                    CloseOutType eResult;
                    if (!File.Exists(strTargetFilePath))
                    {
                        // Processing MSGF+ results, work with .tsv files

                        if (splitFastaEnabled)
                        {
                            numberOfClonedSteps = m_jobParams.GetJobParameter("NumberOfClonedSteps", 0);
                        }

                        // ReSharper disable once UseImplicitlyTypedVariableEvident
                        for (int iteration = 1; iteration <= numberOfClonedSteps; iteration++)
                        {
                            currentStep = "Verifying that .tsv files exist; iteration " + iteration;

                            string suffixToAdd = null;

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
                                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO,
                                        "Skipping PeptideToProteinMapping since job parameter SkipPeptideToProteinMapping is True");
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
                            SortedSet<string> lstFilterPassingPeptides = null;

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

                    eResult = m_PHRP.ExtractDataFromResults(strTargetFilePath, createMSGFPlusFirstHitsFile, createMSGFPlusSynopsisFile,
                        mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MSGFPLUS);

                    if ((eResult != CloseOutType.CLOSEOUT_SUCCESS))
                    {
                        msg = "Error running PHRP";
                        if (!string.IsNullOrWhiteSpace(m_PHRP.ErrMsg))
                            msg += "; " + m_PHRP.ErrMsg;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
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
                    msg = "clsExtractToolRunner.RunPhrpForMSGFPlus(); Exception running PHRP: " + ex.Message + "; " +
                          clsGlobal.GetExceptionStackTrace(ex);
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    m_message = clsGlobal.AppendToComment(m_message, "Exception running PHRP");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Validate that the mass errors are within tolerance
                string strParamFileName = m_jobParams.GetParam("ParmFileName");
                if (!ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.MSGFDB, strParamFileName))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Error in RunPhrpForMSGFPlus at step " + currentStep, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType RunPHRPForMSPathFinder()
        {
            var currentStep = "Initializing";

            string msg = null;

            try
            {
                m_PHRP = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
                m_PHRP.ProgressChanged += m_PHRP_ProgressChanged;

                // Run the processor
                if (m_DebugLevel > 3)
                {
                    msg = "clsExtractToolRunner.RunPhrpForMODa(); Starting PHRP";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
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

                    var eResult = m_PHRP.ExtractDataFromResults(msPathFinderResultsFilePath, CreateFirstHitsFile, CreateSynopsisFile,
                        mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_MSPATHFINDER);

                    if ((eResult != CloseOutType.CLOSEOUT_SUCCESS))
                    {
                        msg = "Error running PHRP";
                        if (!string.IsNullOrWhiteSpace(m_PHRP.ErrMsg))
                            msg += "; " + m_PHRP.ErrMsg;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
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
                    msg = "clsExtractToolRunner.RunPhrpForMODa(); Exception running PHRP: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                    m_message = clsGlobal.AppendToComment(m_message, "Exception running PHRP");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Validate that the mass errors are within tolerance
                string strParamFileName = m_jobParams.GetParam("ParmFileName");
                if (!ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.MSPathFinder, strParamFileName))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error in RunPhrpForMODa at step " + currentStep, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void ZipConsoleOutputFiles()
        {
            var diWorkingDirectory = new DirectoryInfo(m_WorkDir);
            var consoleOutputFiles = new List<string>();

            var diConsoleOutputFiles = new DirectoryInfo(Path.Combine(diWorkingDirectory.FullName, "ConsoleOutputFiles"));
            diConsoleOutputFiles.Create();

            foreach (FileInfo fiFile in diWorkingDirectory.GetFiles("MSGFPlus_ConsoleOutput_Part*.txt"))
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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Problem zipping the ConsoleOutput files; will not delete the separate copies from the transfer folder");
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
            string msg = null;

            bool CreateInspectFirstHitsFile = false;
            bool CreateInspectSynopsisFile = false;

            string strTargetFilePath = null;
            string strSynFilePath = null;

            bool blnSuccess = false;

            m_PHRP = new clsPepHitResultsProcWrapper(m_mgrParams, m_jobParams);
            m_PHRP.ProgressChanged += m_PHRP_ProgressChanged;

            // Run the processor
            if (m_DebugLevel > 3)
            {
                msg = "clsExtractToolRunner.RunPhrpForInSpecT(); Starting PHRP";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
            }

            try
            {
                // The goal:
                //   Get the _fht.txt and _FScore_fht.txt files from the _inspect.txt file in the _inspect_fht.zip file
                //   Get the other files from the _inspect.txt file in the_inspect.zip file

                // Extract _inspect.txt from the _inspect_fht.zip file
                strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_inspect_fht.zip");
                blnSuccess = base.UnzipFile(strTargetFilePath);

                if (!blnSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Create the First Hits files using the _inspect.txt file
                CreateInspectFirstHitsFile = true;
                CreateInspectSynopsisFile = false;
                strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_inspect.txt");
                var eResult = m_PHRP.ExtractDataFromResults(strTargetFilePath, CreateInspectFirstHitsFile, CreateInspectSynopsisFile,
                    mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_INSPECT);

                if ((eResult != CloseOutType.CLOSEOUT_SUCCESS))
                {
                    msg = "Error running PHRP";
                    if (!string.IsNullOrWhiteSpace(m_PHRP.ErrMsg))
                        msg += "; " + m_PHRP.ErrMsg;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Delete the _inspect.txt file
                File.Delete(strTargetFilePath);

                Thread.Sleep(250);

                // Extract _inspect.txt from the _inspect.zip file
                strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_inspect.zip");
                blnSuccess = base.UnzipFile(strTargetFilePath);

                if (!blnSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Create the Synopsis files using the _inspect.txt file
                CreateInspectFirstHitsFile = false;
                CreateInspectSynopsisFile = true;
                strTargetFilePath = Path.Combine(m_WorkDir, m_Dataset + "_inspect.txt");
                strSynFilePath = Path.Combine(m_WorkDir, m_Dataset + "_inspect_syn.txt");

                eResult = m_PHRP.ExtractDataFromResults(strTargetFilePath, CreateInspectFirstHitsFile, CreateInspectSynopsisFile,
                    mGeneratedFastaFilePath, clsAnalysisResources.RESULT_TYPE_INSPECT);

                if ((eResult != CloseOutType.CLOSEOUT_SUCCESS))
                {
                    msg = "Error running PHRP";
                    if (!string.IsNullOrWhiteSpace(m_PHRP.ErrMsg))
                        msg += "; " + m_PHRP.ErrMsg;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
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
                msg = "clsExtractToolRunner.RunPhrpForInSpecT(); Exception running PHRP: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                m_message = clsGlobal.AppendToComment(m_message, "Exception running PHRP");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Validate that the mass errors are within tolerance
            string strParamFileName = m_jobParams.GetParam("ParmFileName");
            if (!ValidatePHRPResultMassErrors(strSynFilePath, clsPHRPReader.ePeptideHitResultType.Inspect, strParamFileName))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }
            else
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
        }

        protected CloseOutType RunPeptideProphet()
        {
            const int SYN_FILE_MAX_SIZE_MB = 200;
            const string PEPPROPHET_RESULT_FILE_SUFFIX = "_PepProphet.txt";

            string msg = null;

            string SynFile = null;
            string[] strFileList = null;
            string strBaseName = null;
            string strSynFileNameAndSize = null;

            string strPepProphetOutputFilePath = null;

            CloseOutType eResult = CloseOutType.CLOSEOUT_SUCCESS;
            bool blnIgnorePeptideProphetErrors = false;

            int intFileIndex = 0;
            float sngParentSynFileSizeMB = 0;
            bool blnSuccess = false;

            blnIgnorePeptideProphetErrors = m_jobParams.GetJobParameter("IgnorePeptideProphetErrors", false);

            string progLoc = m_mgrParams.GetParam("PeptideProphetRunnerProgLoc");

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
                msg = "clsExtractToolRunner.RunPeptideProphet(); Starting Peptide Prophet";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
            }

            SynFile = Path.Combine(m_WorkDir, m_Dataset + "_syn.txt");

            //Check to see if Syn file exists
            var fiSynFile = new FileInfo(SynFile);
            if (!fiSynFile.Exists)
            {
                msg = "clsExtractToolRunner.RunPeptideProphet(); Syn file " + SynFile + " not found; unable to run peptide prophet";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Check the size of the Syn file
            // If it is too large, then we will need to break it up into multiple parts, process each part separately, and then combine the results
            sngParentSynFileSizeMB = (float)(fiSynFile.Length / 1024.0 / 1024.0);
            if (sngParentSynFileSizeMB <= SYN_FILE_MAX_SIZE_MB)
            {
                strFileList = new string[1];
                strFileList[0] = fiSynFile.FullName;
            }
            else
            {
                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "Synopsis file is " + sngParentSynFileSizeMB.ToString("0.0") +
                        " MB, which is larger than the maximum size for peptide prophet (" + SYN_FILE_MAX_SIZE_MB +
                        " MB); splitting into multiple sections");
                }

                // File is too large; split it into multiple chunks
                strFileList = new string[1];
                blnSuccess = SplitFileRoundRobin(fiSynFile.FullName, SYN_FILE_MAX_SIZE_MB * 1024 * 1024, true, ref strFileList);

                if (blnSuccess)
                {
                    if (m_DebugLevel >= 3)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "Synopsis file was split into " + strFileList.Length + " sections by SplitFileRoundRobin");
                    }
                }
                else
                {
                    msg = "Error splitting synopsis file that is over " + SYN_FILE_MAX_SIZE_MB + " MB in size";

                    if (blnIgnorePeptideProphetErrors)
                    {
                        msg += "; Ignoring the error since 'IgnorePeptideProphetErrors' = True";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }
                    else
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            //Setup Peptide Prophet and run for each file in strFileList
            for (intFileIndex = 0; intFileIndex <= strFileList.Length - 1; intFileIndex++)
            {
                m_PeptideProphet.InputFile = strFileList[intFileIndex];
                m_PeptideProphet.Enzyme = "tryptic";
                m_PeptideProphet.OutputFolderPath = m_WorkDir;
                m_PeptideProphet.DebugLevel = m_DebugLevel;

                fiSynFile = new FileInfo(strFileList[intFileIndex]);
                strSynFileNameAndSize = fiSynFile.Name + " (file size = " + (fiSynFile.Length / 1024.0 / 1024.0).ToString("0.00") + " MB";
                if (strFileList.Length > 1)
                {
                    strSynFileNameAndSize += "; parent syn file is " + sngParentSynFileSizeMB.ToString("0.00") + " MB)";
                }
                else
                {
                    strSynFileNameAndSize += ")";
                }

                if (m_DebugLevel >= 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "Running peptide prophet on file " + strSynFileNameAndSize);
                }

                eResult = m_PeptideProphet.CallPeptideProphet();

                if (eResult == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Make sure the Peptide Prophet output file was actually created
                    strPepProphetOutputFilePath = Path.Combine(m_PeptideProphet.OutputFolderPath,
                        Path.GetFileNameWithoutExtension(strFileList[intFileIndex]) + PEPPROPHET_RESULT_FILE_SUFFIX);

                    if (m_DebugLevel >= 3)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "Peptide prophet processing complete; checking for file " + strPepProphetOutputFilePath);
                    }

                    if (!File.Exists(strPepProphetOutputFilePath))
                    {
                        msg = "clsExtractToolRunner.RunPeptideProphet(); Peptide Prophet output file not found for synopsis file " + strSynFileNameAndSize;
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

                        msg = m_PeptideProphet.ErrMsg;
                        if (msg.Length > 0)
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                        }

                        if (blnIgnorePeptideProphetErrors)
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                "Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' = True");
                        }
                        else
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                "To ignore this error, update this job to use a settings file that has 'IgnorePeptideProphetErrors' set to True");
                            eResult = CloseOutType.CLOSEOUT_FAILED;
                            break;
                        }
                    }
                }
                else
                {
                    msg = "clsExtractToolRunner.RunPeptideProphet(); Error running Peptide Prophet on file " + strSynFileNameAndSize + ": " + m_PeptideProphet.ErrMsg;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

                    if (blnIgnorePeptideProphetErrors)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' = True");
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
                if (strFileList.Length > 1)
                {
                    // Delete each of the temporary synopsis files
                    DeleteTemporaryFiles(strFileList);

                    // We now need to recombine the peptide prophet result files

                    // Update strFileList() to have the peptide prophet result file names
                    strBaseName = Path.Combine(m_PeptideProphet.OutputFolderPath, Path.GetFileNameWithoutExtension(SynFile));

                    for (intFileIndex = 0; intFileIndex <= strFileList.Length - 1; intFileIndex++)
                    {
                        strFileList[intFileIndex] = strBaseName + "_part" + (intFileIndex + 1).ToString() + PEPPROPHET_RESULT_FILE_SUFFIX;

                        // Add this file to the global delete list
                        m_jobParams.AddResultFileToSkip(strFileList[intFileIndex]);
                    }

                    // Define the final peptide prophet output file name
                    strPepProphetOutputFilePath = strBaseName + PEPPROPHET_RESULT_FILE_SUFFIX;

                    if (m_DebugLevel >= 2)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "Combining " + strFileList.Length + " separate Peptide Prophet result files to create " +
                            Path.GetFileName(strPepProphetOutputFilePath));
                    }

                    blnSuccess = InterleaveFiles(ref strFileList, strPepProphetOutputFilePath, true);

                    // Delete each of the temporary peptide prophet result files
                    DeleteTemporaryFiles(strFileList);

                    if (blnSuccess)
                    {
                        eResult = CloseOutType.CLOSEOUT_SUCCESS;
                    }
                    else
                    {
                        msg = "Error interleaving the peptide prophet result files (FileCount=" + strFileList.Length + ")";
                        if (blnIgnorePeptideProphetErrors)
                        {
                            msg += "; Ignoring the error since 'IgnorePeptideProphetErrors' = True";
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, msg);
                            eResult = CloseOutType.CLOSEOUT_SUCCESS;
                        }
                        else
                        {
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                            eResult = CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                }
            }

            return eResult;
        }

        /// <summary>
        /// Deletes each file in strFileList()
        /// </summary>
        /// <param name="strFileList">Full paths to files to delete</param>
        /// <remarks></remarks>
        private void DeleteTemporaryFiles(string[] strFileList)
        {
            int intFileIndex = 0;

            Thread.Sleep(1000);                       //Delay for 1 second
            PRISM.Processes.clsProgRunner.GarbageCollectNow();

            // Delete each file in strFileList
            for (intFileIndex = 0; intFileIndex <= strFileList.Length - 1; intFileIndex++)
            {
                if (m_DebugLevel >= 5)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Deleting file " + strFileList[intFileIndex]);
                }
                try
                {
                    File.Delete(strFileList[intFileIndex]);
                }
                catch (Exception ex)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Error deleting file " + Path.GetFileName(strFileList[intFileIndex]) + ": " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Reads each file in strFileList() line by line, writing the lines to strCombinedFilePath
        /// Can also check for a header line on the first line; if a header line is found in the first file,
        /// then the header is also written to the combined file
        /// </summary>
        /// <param name="strFileList">Files to combine</param>
        /// <param name="strCombinedFilePath">File to create</param>
        /// <param name="blnLookForHeaderLine">When true, then looks for a header line by checking if the first column contains a number</param>
        /// <returns>True if success; false if failure</returns>
        /// <remarks></remarks>
        protected bool InterleaveFiles(ref string[] strFileList, string strCombinedFilePath, bool blnLookForHeaderLine)
        {
            string msg = null;

            int intFileCount = 0;
            StreamReader[] srInFiles = null;

            string strLineIn = null;
            string[] strSplitLine = null;

            int intFileIndex = 0;
            int[] intLinesRead = null;
            int intTotalLinesRead = 0;

            int intTotalLinesReadSaved = 0;

            bool blnContinueReading = false;
            bool blnProcessLine = false;
            bool blnSuccess = false;

            try
            {
                if (strFileList == null || strFileList.Length == 0)
                {
                    // Nothing to do
                    return false;
                }

                intFileCount = strFileList.Length;
                srInFiles = new StreamReader[intFileCount];
                intLinesRead = new int[intFileCount];

                // Open each of the input files
                for (var intIndex = 0; intIndex <= intFileCount - 1; intIndex++)
                {
                    if (File.Exists(strFileList[intIndex]))
                    {
                        srInFiles[intIndex] = new StreamReader(new FileStream(strFileList[intIndex], FileMode.Open, FileAccess.Read, FileShare.Read));
                    }
                    else
                    {
                        // File not found; unable to continue
                        msg = "Source peptide prophet file not found, unable to continue: " + strFileList[intIndex];
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                        return false;
                    }
                }

                // Create the output file

                var swOutFile = new StreamWriter(new FileStream(strCombinedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                intTotalLinesRead = 0;
                blnContinueReading = true;

                while (blnContinueReading)
                {
                    intTotalLinesReadSaved = intTotalLinesRead;

                    for (intFileIndex = 0; intFileIndex <= intFileCount - 1; intFileIndex++)
                    {
                        if (srInFiles[intFileIndex].EndOfStream)
                            continue;

                        strLineIn = srInFiles[intFileIndex].ReadLine();

                        intLinesRead[intFileIndex] += 1;
                        intTotalLinesRead += 1;

                        if (strLineIn == null)
                            continue;

                        blnProcessLine = true;

                        if (intLinesRead[intFileIndex] == 1 && blnLookForHeaderLine && strLineIn.Length > 0)
                        {
                            // check for a header line
                            strSplitLine = strLineIn.Split(new char[] { '\t' }, 2);

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
                    srInFiles[intIndex].Close();
                }

                // Close the output file
                swOutFile.Close();

                blnSuccess = true;
            }
            catch (Exception ex)
            {
                msg = "Exception in clsExtractToolRunner.InterleaveFiles: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                m_message = clsGlobal.AppendToComment(m_message, "Exception in InterleaveFiles");
                blnSuccess = false;
            }

            return blnSuccess;
        }

        /// <summary>
        /// Reads strSrcFilePath line-by-line and splits into multiple files such that none of the output
        /// files has length greater than lngMaxSizeBytes. Can also check for a header line on the first line;
        /// if a header line is found, then all of the split files will be assigned the same header line
        /// </summary>
        /// <param name="strSrcFilePath">FilePath to parse</param>
        /// <param name="lngMaxSizeBytes">Maximum size of each file</param>
        /// <param name="blnLookForHeaderLine">When true, then looks for a header line by checking if the first column contains a number</param>
        /// <param name="strSplitFileList">Output array listing the full paths to the split files that were created</param>
        /// <returns>True if success, false if failure</returns>
        /// <remarks></remarks>
        private bool SplitFileRoundRobin(string strSrcFilePath, long lngMaxSizeBytes, bool blnLookForHeaderLine, ref string[] strSplitFileList)
        {
            string strBaseName = null;

            int intLinesRead = 0;
            int intTargetFileIndex = 0;

            string msg = null;
            string strLineIn = null;
            string[] strSplitLine = null;

            StreamWriter[] swOutFiles = null;

            int intSplitCount = 0;

            bool blnProcessLine = false;
            bool blnSuccess = false;

            try
            {
                var fiFileInfo = new FileInfo(strSrcFilePath);
                if (!fiFileInfo.Exists)
                    return false;

                if (fiFileInfo.Length <= lngMaxSizeBytes)
                {
                    // File is already less than the limit
                    strSplitFileList = new string[1];
                    strSplitFileList[0] = fiFileInfo.FullName;

                    blnSuccess = true;
                }
                else
                {
                    // Determine the number of parts to split the file into
                    intSplitCount = (int)Math.Ceiling(fiFileInfo.Length / (float)lngMaxSizeBytes);

                    if (intSplitCount < 2)
                    {
                        // This code should never be reached; we'll set intSplitCount to 2
                        intSplitCount = 2;
                    }

                    // Open the input file
                    var srInFile = new StreamReader(new FileStream(fiFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));

                    // Create each of the output files
                    strSplitFileList = new string[intSplitCount];
                    swOutFiles = new StreamWriter[intSplitCount];

                    strBaseName = Path.Combine(fiFileInfo.DirectoryName, Path.GetFileNameWithoutExtension(fiFileInfo.Name));

                    for (var intIndex = 0; intIndex <= intSplitCount - 1; intIndex++)
                    {
                        strSplitFileList[intIndex] = strBaseName + "_part" + (intIndex + 1).ToString() + Path.GetExtension(fiFileInfo.Name);
                        swOutFiles[intIndex] = new StreamWriter(new FileStream(strSplitFileList[intIndex], FileMode.Create, FileAccess.Write, FileShare.Read));
                    }

                    intLinesRead = 0;
                    intTargetFileIndex = 0;

                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();
                        intLinesRead += 1;

                        if (strLineIn == null)
                            continue;

                        blnProcessLine = true;

                        if (intLinesRead == 1 && blnLookForHeaderLine && strLineIn.Length > 0)
                        {
                            // Check for a header line
                            strSplitLine = strLineIn.Split(new char[] { '\t' }, 2);

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

                    // Close the input file
                    srInFile.Close();

                    // Close the output files
                    for (var intIndex = 0; intIndex <= intSplitCount - 1; intIndex++)
                    {
                        swOutFiles[intIndex].Close();
                    }

                    blnSuccess = true;
                }
            }
            catch (Exception ex)
            {
                msg = "Exception in clsExtractToolRunner.SplitFileRoundRobin: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                m_message = clsGlobal.AppendToComment(m_message, "Exception in SplitFileRoundRobin");
                blnSuccess = false;
            }

            return blnSuccess;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo()
        {
            string strToolVersionInfo = string.Empty;
            bool blnSuccess = false;

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            // Lookup the version of the PeptideHitResultsProcessor

            try
            {
                string progLoc = m_mgrParams.GetParam("PHRPProgLoc");
                var diPHRP = new DirectoryInfo(progLoc);

                // verify that program file exists
                if (diPHRP.Exists)
                {
                    base.StoreToolVersionInfoOneFile64Bit(ref strToolVersionInfo, Path.Combine(diPHRP.FullName, "PeptideHitResultsProcessor.dll"));
                }
                else
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "PHRP folder not found at " + progLoc);
                    return false;
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception determining Assembly info for the PeptideHitResultsProcessor: " + ex.Message);
                return false;
            }

            if (m_jobParams.GetParam("ResultType") == clsAnalysisResources.RESULT_TYPE_SEQUEST)
            {
                //Sequest result type

                // Lookup the version of the PeptideFileExtractor
                if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "PeptideFileExtractor"))
                {
                    return false;
                }

                // Lookup the version of the PeptideProphetRunner

                string strPeptideProphetRunnerLoc = m_mgrParams.GetParam("PeptideProphetRunnerProgLoc");
                var ioPeptideProphetRunner = new FileInfo(strPeptideProphetRunnerLoc);

                if (ioPeptideProphetRunner.Exists)
                {
                    // Lookup the version of the PeptideProphetRunner
                    blnSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, ioPeptideProphetRunner.FullName);
                    if (!blnSuccess)
                        return false;

                    // Lookup the version of the PeptideProphetLibrary
                    blnSuccess = base.StoreToolVersionInfoOneFile32Bit(ref strToolVersionInfo,
                        Path.Combine(ioPeptideProphetRunner.DirectoryName, "PeptideProphetLibrary.dll"));
                    if (!blnSuccess)
                        return false;
                }
            }

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), blnSaveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        protected bool ValidatePHRPResultMassErrors(string strInputFilePath, clsPHRPReader.ePeptideHitResultType eResultType,
            string strSearchEngineParamFileName)
        {
            bool blnSuccess = false;

            try
            {
                var oValidator = new clsPHRPMassErrorValidator(m_DebugLevel);
                var paramFilePath = Path.Combine(m_WorkDir, strSearchEngineParamFileName);

                blnSuccess = oValidator.ValidatePHRPResultMassErrors(strInputFilePath, eResultType, paramFilePath);
                if (!blnSuccess)
                {
                    string toolName = m_jobParams.GetJobParameter("ToolName", "");

                    if (toolName.ToLower().StartsWith("inspect"))
                    {
                        // Ignore this error for inspect if running an unrestricted search
                        string paramFileName = m_jobParams.GetJobParameter("ParmFileName", "");
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
                if (System.DateTime.UtcNow.Subtract(dtLastPepProphetStatusLog).TotalSeconds >= PEPPROPHET_DETAILED_LOG_INTERVAL_SECONDS)
                {
                    dtLastPepProphetStatusLog = System.DateTime.UtcNow;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "Running peptide prophet: " + PepProphetStatus + "; " + PercentComplete + "% complete");
                }
            }
        }

        private DateTime dtLastPHRPStatusLog = DateTime.MinValue;

        private void m_PHRP_ProgressChanged(string taskDescription, float percentComplete)
        {
            const int PHRP_LOG_INTERVAL_SECONDS = 180;
            const int PHRP_DETAILED_LOG_INTERVAL_SECONDS = 20;

            m_progress = SEQUEST_PROGRESS_EXTRACTION_DONE + (float)(percentComplete / 3.0);
            m_StatusTools.UpdateAndWrite(m_progress);

            if (m_DebugLevel >= 1)
            {
                if (System.DateTime.UtcNow.Subtract(dtLastPHRPStatusLog).TotalSeconds >= PHRP_DETAILED_LOG_INTERVAL_SECONDS & m_DebugLevel >= 3 ||
                    System.DateTime.UtcNow.Subtract(dtLastPHRPStatusLog).TotalSeconds >= PHRP_LOG_INTERVAL_SECONDS)
                {
                    dtLastPHRPStatusLog = System.DateTime.UtcNow;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "Running PHRP: " + taskDescription + "; " + percentComplete + "% complete");
                }
            }
        }

        private void mMSGFDBUtils_ErrorEvent(string errorMsg, Exception ex)
        {
            mMSGFDBUtilsError = true;
        }

        /// <summary>
        /// Event handler for the MSGResultsSummarizer
        /// </summary>
        /// <param name="errorMessage"></param>
        private void MSGFResultsSummarizer_ErrorHandler(string errorMessage)
        {
            if (Message.ToLower().Contains("permission was denied"))
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, errorMessage);
            }
            else
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage);
            }
        }

        #endregion
    }
}
