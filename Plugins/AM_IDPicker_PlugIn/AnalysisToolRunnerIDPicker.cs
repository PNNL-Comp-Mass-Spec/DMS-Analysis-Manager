using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PHRPReader;

namespace AnalysisManagerIDPickerPlugIn
{
    /// <summary>
    /// Class for running IDPicker
    /// </summary>
    public class AnalysisToolRunnerIDPicker : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: app, cmd, idp, IDPicker, MODa, msgfspecprob, parm, prepend, Qonvert, xxx

        // ReSharper restore CommentTypo

        /// <summary>
        /// If true, always skip IDPicker
        /// </summary>
        public const bool ALWAYS_SKIP_IDPICKER = true;

        private const string PEPXML_CONSOLE_OUTPUT = "PepXML_ConsoleOutput.txt";

        private const string IPD_Qonvert_CONSOLE_OUTPUT = "IDPicker_Qonvert_ConsoleOutput.txt";
        private const string IPD_Assemble_CONSOLE_OUTPUT = "IDPicker_Assemble_ConsoleOutput.txt";
        private const string IPD_Report_CONSOLE_OUTPUT = "IDPicker_Report_ConsoleOutput.txt";

        private const string IDPicker_Qonvert = "idpQonvert.exe";
        private const string IDPicker_Assemble = "idpAssemble.exe";
        private const string IDPicker_Report = "idpReport.exe";

        private const string ASSEMBLE_GROUPING_FILENAME = "Assemble.txt";
        private const string ASSEMBLE_OUTPUT_FILENAME = "IDPicker_AssembledResults.xml";

        private const string MSGFDB_DECOY_PROTEIN_PREFIX = "REV_";
        private const string MSGFPLUS_DECOY_PROTEIN_PREFIX = "XXX_";

        private const string PEPTIDE_LIST_TO_XML_EXE = "PeptideListToXML.exe";

        private const int PROGRESS_PCT_IDPicker_STARTING = 1;
        private const int PROGRESS_PCT_IDPicker_SEARCHING_FOR_FILES = 5;
        private const int PROGRESS_PCT_IDPicker_CREATING_PEPXML_FILE = 10;
        private const int PROGRESS_PCT_IDPicker_RUNNING_IDPQonvert = 20;
        private const int PROGRESS_PCT_IDPicker_RUNNING_IDPAssemble = 60;
        private const int PROGRESS_PCT_IDPicker_RUNNING_IDPReport = 70;
        private const int PROGRESS_PCT_IDPicker_COMPLETE = 95;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private string mIDPickerProgramDirectory = string.Empty;
        private string mIDPickerParamFileNameLocal = string.Empty;

        private string mPeptideListToXMLExePath = string.Empty;
        private string mPepXMLFilePath = string.Empty;
        private string mIdpXMLFilePath = string.Empty;
        private string mIdpAssembleFilePath = string.Empty;

        private Dictionary<string, string> mIDPickerOptions;

        // This variable holds the name of the program that is currently running via CmdRunner
        private string mCmdRunnerDescription = string.Empty;

        // This list tracks the error messages reported by CmdRunner
        private ConcurrentBag<string> mCmdRunnerErrors;

        // This list tracks error message text that we look for when considering if an error message should be ignored
        private ConcurrentBag<string> mCmdRunnerErrorsToIgnore;

        // This list tracks files that we want to include in the zipped up IDPicker report directory
        private List<string> mFilenamesToAddToReportDirectory;

        private bool mBatchFilesMoved;

        /// <summary>
        /// Runs PepXML converter and IDPicker tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            // As of 2015-01-21, we are now always skipping IDPicker (and thus simply creating the .pepXML file)
            var skipIDPicker = ALWAYS_SKIP_IDPICKER;

            var processingSuccess = true;

            mIDPickerOptions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            mCmdRunnerErrors = new ConcurrentBag<string>();
            mCmdRunnerErrorsToIgnore = new ConcurrentBag<string>();
            mFilenamesToAddToReportDirectory = new List<string>();

            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerIDPicker.RunTool(): Enter");
                }

                mProgress = PROGRESS_PCT_IDPicker_SEARCHING_FOR_FILES;

                // Determine the path to the IDPicker program (idpQonvert); directory will also contain idpAssemble.exe and idpReport.exe
                var progLocQonvert = string.Empty;

                // ReSharper disable ConditionIsAlwaysTrueOrFalse
                if (!skipIDPicker)
                {
                    progLocQonvert = DetermineProgramLocation("IDPickerProgLoc", IDPicker_Qonvert);

                    if (string.IsNullOrWhiteSpace(progLocQonvert))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                // Determine the result type
                var resultType = AnalysisResources.GetResultType(mJobParams);

                var phrpResultType = ReaderFactory.GetPeptideHitResultType(resultType);

                if (phrpResultType == PeptideHitResultTypes.Unknown)
                {
                    LogError("Invalid tool result type (not supported by IDPicker): " + resultType);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Define the path to the synopsis file

                GetEffectiveSynopsisFileName(phrpResultType, out var synopsisFileName, out var phrpBaseName);

                var synFilePath = Path.Combine(mWorkDir, synopsisFileName);

                if (!File.Exists(synFilePath))
                {
                    var alternateFilePath = ReaderFactory.AutoSwitchToLegacyMSGFDBIfRequired(synFilePath, "Dataset_msgfdb.txt");

                    if (File.Exists(alternateFilePath))
                    {
                        synFilePath = alternateFilePath;
                    }
                }

                if (!AnalysisResources.ValidateFileHasData(synFilePath, "Synopsis", out var errorMessage))
                {
                    // The synopsis file is empty
                    LogError(errorMessage);
                    return CloseOutType.CLOSEOUT_NO_DATA;
                }

                // Define the path to the FASTA file
                var orgDbDir = mMgrParams.GetParam("OrgDbDir");
                var fastaFilePath = Path.Combine(orgDbDir, mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName"));

                var fastaFile = new FileInfo(fastaFilePath);

                if (!skipIDPicker && !fastaFile.Exists)
                {
                    // FASTA file not found
                    LogError("FASTA file not found: " + fastaFile.FullName);
                    mMessage = "FASTA file not found: " + fastaFile.Name;
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                var splitFasta = mJobParams.GetJobParameter("SplitFasta", false);

                if (!skipIDPicker && splitFasta)
                {
                    skipIDPicker = true;
                    LogWarning("SplitFasta jobs typically have FASTA files too large for IDPQonvert; skipping IDPicker", true);
                }

                // Store the version of IDPicker and PeptideListToXML in the database
                // Alternatively, if skipIDPicker is true, just store the version of PeptideListToXML

                // this method updates mPeptideListToXMLExePath and mIDPickerProgramDirectory
                if (!StoreToolVersionInfo(progLocQonvert, skipIDPicker))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining IDPicker version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Create the PepXML file
                var pepXmlSuccess = CreatePepXMLFile(fastaFile.FullName, synFilePath, phrpResultType, phrpBaseName);

                if (!pepXmlSuccess)
                {
                    LogError("Error creating PepXML file for job " + mJob);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (skipIDPicker)
                {
                    // Don't keep this file since we're skipping IDPicker
                    mJobParams.AddResultFileToSkip("Tool_Version_Info_IDPicker.txt");

                    var paramFileNameLocal = mJobParams.GetParam(AnalysisResourcesIDPicker.IDPICKER_PARAM_FILENAME_LOCAL);

                    if (string.IsNullOrEmpty(paramFileNameLocal))
                    {
                        mJobParams.AddResultFileToSkip(AnalysisResourcesIDPicker.DEFAULT_IDPICKER_PARAM_FILE_NAME);
                    }
                    else
                    {
                        mJobParams.AddResultFileToSkip(paramFileNameLocal);
                    }
                }
                else
                {
                    var idPickerSuccess = RunIDPickerWrapper(phrpResultType, phrpBaseName, synFilePath, fastaFile.FullName, out var processingError, out var criticalError);

                    if (criticalError)
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (!idPickerSuccess || processingError)
                        processingSuccess = false;
                }

                if (processingSuccess)
                {
                    // Zip the PepXML file
                    var zipSuccess = ZipPepXMLFile(phrpBaseName);

                    if (!zipSuccess)
                        processingSuccess = false;
                }

                mJobParams.AddResultFileExtensionToSkip(".bat");

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, return CloseOutType.CLOSEOUT_FAILED

                    mJobParams.RemoveResultFileToSkip(ASSEMBLE_GROUPING_FILENAME);
                    mJobParams.RemoveResultFileToSkip(ASSEMBLE_OUTPUT_FILENAME);

                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var directoryCreated = MakeResultsDirectory();

                if (!directoryCreated)
                {
                    // MakeResultsDirectory handles posting to local log, so set database error message and exit
                    mMessage = "Error making results directory";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!skipIDPicker)
                {
                    var moveResult = MoveFilesIntoIDPickerSubdirectory();

                    if (moveResult != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        // Note that MoveResultFiles should have already called AnalysisResults.CopyFailedResultsToArchiveDirectory
                        mMessage = "Error moving files into IDPicker subdirectory";
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                // ReSharper restore ConditionIsAlwaysTrueOrFalse

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Exception in IDPickerPlugin->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool RunIDPickerWrapper(
            PeptideHitResultTypes phrpResultType, string phrpBaseName,
            string synFilePath, string fastaFilePath,
            out bool processingError, out bool criticalError)
        {
            bool success;
            processingError = false;
            criticalError = false;

            // Determine the prefix used by decoy proteins
            var decoyPrefix = string.Empty;

            if (phrpResultType == PeptideHitResultTypes.MSGFPlus)
            {
                // If we run MS-GF+ with target/decoy mode and showDecoy=1, the _syn.txt file will have decoy proteins that start with REV_ or XXX_
                // Check for this
                success = LookForDecoyProteinsInMSGFPlusResults(synFilePath, phrpResultType, ref decoyPrefix);

                if (!success)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("Error looking for decoy proteins in the MS-GF+ synopsis file");
                    }
                    criticalError = true;
                    return false;
                }
            }

            if (string.IsNullOrEmpty(decoyPrefix))
            {
                // Look for decoy proteins in the FASTA file
                success = DetermineDecoyProteinPrefix(fastaFilePath, out decoyPrefix);

                if (!success)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("Error looking for decoy proteins in the FASTA file");
                    }
                    criticalError = true;
                    return false;
                }
            }

            if (string.IsNullOrEmpty(decoyPrefix))
            {
                LogWarning("No decoy proteins; skipping IDPicker", true);
                return true;
            }

            // Load the IDPicker options
            success = LoadIDPickerOptions();

            if (!success)
            {
                processingError = true;
                return false;
            }

            // Convert the search scores in the pepXML file to q-values
            success = RunQonvert(fastaFilePath, decoyPrefix, phrpResultType, phrpBaseName);

            if (!success)
            {
                processingError = true;
                return false;
            }

            // Organizes the search results into a hierarchy
            success = RunAssemble(phrpBaseName);

            if (!success)
            {
                processingError = true;
                return false;
            }

            // Apply parsimony in protein assembly and generate reports
            success = RunReport();

            if (!success)
            {
                processingError = true;
                return false;
            }

            return true;
        }

        /// <summary>
        /// Append a new command line argument (appends using valueIfMissing if not defined in mIDPickerOptions)
        /// </summary>
        /// <param name="cmdArgs">Current arguments</param>
        /// <param name="argumentName">Argument Name</param>
        /// <param name="valueIfMissing">Value to append if not defined in mIDPickerOptions</param>
        /// <returns>The new argument list</returns>
        private string AppendArgument(string cmdArgs, string argumentName, string valueIfMissing)
        {
            const bool appendIfMissing = true;
            return AppendArgument(cmdArgs, argumentName, argumentName, valueIfMissing, appendIfMissing);
        }

        /// <summary>
        /// Append a new command line argument (appends using valueIfMissing if not defined in mIDPickerOptions)
        /// </summary>
        /// <param name="arguments">Current arguments</param>
        /// <param name="optionName">Key name to lookup in mIDPickerOptions</param>
        /// <param name="argumentName">Argument Name</param>
        /// <param name="valueIfMissing">Value to append if not defined in mIDPickerOptions</param>
        /// <returns>The new argument list</returns>
        private string AppendArgument(string arguments, string optionName, string argumentName, string valueIfMissing)
        {
            const bool appendIfMissing = true;
            return AppendArgument(arguments, optionName, argumentName, valueIfMissing, appendIfMissing);
        }

        /// <summary>
        /// Append a new command line argument
        /// </summary>
        /// <param name="arguments">Current arguments</param>
        /// <param name="optionName">Key name to lookup in mIDPickerOptions</param>
        /// <param name="argumentName">Argument Name</param>
        /// <param name="valueIfMissing">Value to append if not defined in mIDPickerOptions</param>
        /// <param name="appendIfMissing">If true, append the argument using valueIfMissing if not found in mIDPickerOptions; if false, and not found, does not append the argument</param>
        /// <returns>The new argument list</returns>
        private string AppendArgument(string arguments, string optionName, string argumentName, string valueIfMissing, bool appendIfMissing)
        {
            bool isMissing;
            bool appendParam;

            if (mIDPickerOptions.TryGetValue(optionName, out var value))
            {
                isMissing = false;
            }
            else
            {
                isMissing = true;
                value = valueIfMissing;
            }

            if (isMissing)
            {
                appendParam = appendIfMissing;
            }
            else
            {
                appendParam = true;
            }

            if (string.IsNullOrEmpty(arguments))
            {
                arguments = string.Empty;
            }

            if (appendParam)
            {
                return arguments + " -" + argumentName + " " + PossiblyQuotePath(value);
            }

            return arguments;
        }

        private bool CreateAssembleFile(string assembleFilePath, string phrpBaseName)
        {
            try
            {
                // Prepend dataset name with PNNL/
                // Also make sure it doesn't contain any spaces
                var datasetLabel = "PNNL/" + phrpBaseName.Replace(" ", "_");

                // Create the Assemble.txt file
                using var writer = new StreamWriter(new FileStream(assembleFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine(datasetLabel + " " + Path.GetFileName(mIdpXMLFilePath));
            }
            catch (Exception ex)
            {
                LogError("Exception in IDPickerPlugin->CreateAssembleFile", ex);
                return false;
            }

            return true;
        }

        private void ClearConcurrentBag(ref ConcurrentBag<string> bag)
        {
            while (!bag.IsEmpty)
            {
                bag.TryTake(out _);
            }
        }

        /// <summary>
        /// Copies a file into directory reportDirectoryPath then adds it to mJobParams.AddResultFileToSkip
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="reportDirectoryPath"></param>
        private void CopyFileIntoReportDirectory(string fileName, string reportDirectoryPath)
        {
            try
            {
                var sourceFile = new FileInfo(Path.Combine(mWorkDir, fileName));

                if (sourceFile.Exists)
                {
                    sourceFile.CopyTo(Path.Combine(reportDirectoryPath, fileName), true);
                    mJobParams.AddResultFileToSkip(fileName);
                }
            }
            catch (Exception ex)
            {
                LogWarning("Error copying ConsoleOutput file into the IDPicker Report Directory: " + ex.Message);
            }
        }

        private bool CreatePepXMLFile(string fastaFilePath, string synFilePath, PeptideHitResultTypes phrpResultType, string phrpBaseName)
        {
            // PepXML file creation should generally be done in less than 10 minutes
            // However, for huge FASTA files, conversion could take several hours
            const int maxRuntimeMinutes = 480;

            bool success;

            try
            {
                // Set up and execute a program runner to run PeptideListToXML
                var paramFileName = mJobParams.GetParam("ParamFileName");

                mPepXMLFilePath = Path.Combine(mWorkDir, phrpBaseName + ".pepXML");
                var hitsPerSpectrum = mJobParams.GetJobParameter("PepXMLHitsPerSpectrum", 3);

                var arguments = PossiblyQuotePath(synFilePath) +
                                " /E:" + PossiblyQuotePath(paramFileName) +
                                " /F:" + PossiblyQuotePath(fastaFilePath) +
                                " /H:" + hitsPerSpectrum;

                if (phrpResultType is PeptideHitResultTypes.MODa or PeptideHitResultTypes.MODPlus or PeptideHitResultTypes.MaxQuant)
                {
                    // For MODa and MODPlus, the SpecProb values listed in the _syn_MSGF.txt file are not true spectral probabilities
                    //   Instead, they're just 1 - Probability  (where Probability is a value between 0 and 1 assigned by MODa)
                    //   Therefore, don't include them in the PepXML file
                    // MaxQuant results don't have a _msgf.txt file
                    arguments += " /NoMSGF";
                }

                if (mJobParams.GetJobParameter("PepXMLNoScanStats", false))
                {
                    arguments += " /NoScanStats";
                }

                ClearConcurrentBag(ref mCmdRunnerErrorsToIgnore);

                mProgress = PROGRESS_PCT_IDPicker_CREATING_PEPXML_FILE;

                success = RunProgramWork("PeptideListToXML", mPeptideListToXMLExePath, arguments, PEPXML_CONSOLE_OUTPUT, false, maxRuntimeMinutes);

                if (success)
                {
                    // Make sure the .pepXML file was created
                    if (!File.Exists(mPepXMLFilePath))
                    {
                        LogError("Error creating PepXML file, job " + mJob);
                        success = false;
                    }
                    else
                    {
                        mJobParams.AddResultFileToSkip(PEPXML_CONSOLE_OUTPUT);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in IDPickerPlugin->CreatePepXMLFile", ex);
                return false;
            }

            return success;
        }

        /// <summary>
        /// Determine the prefix used by to denote decoy (reversed) proteins
        /// </summary>
        /// <param name="fastaFilePath"></param>
        /// <param name="decoyPrefix"></param>
        /// <returns>True if success, false if an error</returns>
        private bool DetermineDecoyProteinPrefix(string fastaFilePath, out string decoyPrefix)
        {
            decoyPrefix = string.Empty;

            try
            {
                if (mDebugLevel >= 3)
                {
                    LogDebug("Looking for decoy proteins in the FASTA file");
                }

                var reversedProteinPrefixes = new SortedSet<string>
                {
                    "reversed_",                                   // MTS reversed proteins                 // reversed[_]%'
                    "scrambled_",                                  // MTS scrambled proteins                // scrambled[_]%'
                    "xxx.",                                        // Inspect reversed/scrambled proteins   // xxx.%'
                    MSGFDB_DECOY_PROTEIN_PREFIX.ToLower(),         // MSGFDB reversed proteins              // rev[_]%'
                    MSGFPLUS_DECOY_PROTEIN_PREFIX.ToLower()        // MS-GF+ reversed proteins              // xxx[_]%'
                };

                // Note that X!Tandem decoy proteins end with ":reversed"
                // IDPicker doesn't support decoy protein name suffixes, only prefixes

                var prefixStats = new Dictionary<string, int>();

                var fastaFileReader = new ProteinFileReader.FastaFileReader();

                if (!fastaFileReader.OpenFile(fastaFilePath))
                {
                    LogError("Error reading FASTA file with ProteinFileReader");
                    return false;
                }

                while (fastaFileReader.ReadNextProteinEntry())
                {
                    var protein = fastaFileReader.ProteinName;

                    foreach (var prefix in reversedProteinPrefixes)
                    {
                        if (!protein.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                            continue;

                        var proteinPrefix = protein.Substring(0, prefix.Length);

                        if (prefixStats.TryGetValue(proteinPrefix, out var count))
                        {
                            prefixStats[proteinPrefix] = count + 1;
                        }
                        else
                        {
                            prefixStats.Add(proteinPrefix, 1);
                        }
                    }
                }

                fastaFileReader.CloseFile();

                if (prefixStats.Count == 1)
                {
                    decoyPrefix = prefixStats.First().Key;
                }
                else if (prefixStats.Count > 1)
                {
                    // Find the prefix (key) in prefixStats with the highest occurrence count
                    var maxCount = -1;

                    foreach (var kvEntry in prefixStats)
                    {
                        if (kvEntry.Value > maxCount)
                        {
                            maxCount = kvEntry.Value;
                            decoyPrefix = kvEntry.Key;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in IDPickerPlugin->DetermineDecoyProteinPrefix", ex);
                return false;
            }

            return true;
        }

        private void GetEffectiveSynopsisFileName(PeptideHitResultTypes phrpResultType, out string synopsisFileName, out string phrpBaseName)
        {
            var aggregationJobSynopsisFileName = mJobParams.GetJobParameter(AnalysisResourcesIDPicker.JOB_PARAM_AGGREGATION_JOB_SYNOPSIS_FILE, string.Empty);
            var aggregationJobPhrpBaseName = mJobParams.GetJobParameter(AnalysisResourcesIDPicker.JOB_PARAM_AGGREGATION_JOB_PHRP_BASE_NAME, string.Empty);

            if (string.IsNullOrWhiteSpace(aggregationJobSynopsisFileName))
            {
                synopsisFileName = ReaderFactory.GetPHRPSynopsisFileName(phrpResultType, mDatasetName);
                phrpBaseName = mDatasetName;
                return;
            }

            synopsisFileName = aggregationJobSynopsisFileName;
            phrpBaseName = aggregationJobPhrpBaseName;
        }

        private bool IgnoreError(string errorMessage)
        {
            var ignore = false;

            foreach (var ignoreText in mCmdRunnerErrorsToIgnore)
            {
                if (errorMessage.Contains(ignoreText))
                {
                    ignore = true;
                    break;
                }
            }

            return ignore;
        }

        private bool LoadIDPickerOptions()
        {
            try
            {
                mIDPickerParamFileNameLocal = mJobParams.GetParam(AnalysisResourcesIDPicker.IDPICKER_PARAM_FILENAME_LOCAL);

                if (string.IsNullOrEmpty(mIDPickerParamFileNameLocal))
                {
                    LogError("IDPicker parameter file not defined");
                    return false;
                }

                var parameterFilePath = Path.Combine(mWorkDir, mIDPickerParamFileNameLocal);

                using var reader = new StreamReader(new FileStream(parameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    var trimmedLine = dataLine.Trim();

                    if (trimmedLine.StartsWith("#") || !trimmedLine.Contains('='))
                    {
                        continue;
                    }

                    var key = string.Empty;
                    var value = string.Empty;

                    var charIndex = trimmedLine.IndexOf('=');

                    if (charIndex > 0)
                    {
                        key = trimmedLine.Substring(0, charIndex).Trim();

                        if (charIndex < trimmedLine.Length - 1)
                        {
                            value = trimmedLine.Substring(charIndex + 1).Trim();
                        }
                        else
                        {
                            value = string.Empty;
                        }
                    }

                    charIndex = value.IndexOf('#');

                    if (charIndex >= 0)
                    {
                        value = value.Substring(0, charIndex);
                    }

                    if (!string.IsNullOrWhiteSpace(key))
                    {
                        if (mIDPickerOptions.ContainsKey(key))
                        {
                            LogWarning("Ignoring duplicate parameter file option '" + key + "' in file " + mIDPickerParamFileNameLocal);
                        }
                        else
                        {
                            mIDPickerOptions.Add(key, value.Trim());
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in IDPickerPlugin->LoadIDPickerOptions", ex);
                return false;
            }

            return true;
        }

        private bool LookForDecoyProteinsInMSGFPlusResults(string synFilePath, PeptideHitResultTypes resultType, ref string decoyPrefix)
        {
            try
            {
                decoyPrefix = string.Empty;
                var prefixesToCheck = new List<string> {
                    MSGFDB_DECOY_PROTEIN_PREFIX.ToUpper(),
                    MSGFPLUS_DECOY_PROTEIN_PREFIX.ToUpper()
                };

                if (mDebugLevel >= 3)
                {
                    LogDebug("Looking for decoy proteins in the MS-GF+ synopsis file");
                }

                using var reader = new ReaderFactory(synFilePath, resultType, false, false, false);
                RegisterEvents(reader);

                while (reader.MoveNext())
                {
                    var found = false;

                    foreach (var prefixToCheck in prefixesToCheck)
                    {
                        if (reader.CurrentPSM.ProteinFirst.ToUpper().StartsWith(prefixToCheck))
                        {
                            decoyPrefix = reader.CurrentPSM.ProteinFirst.Substring(0, prefixToCheck.Length);

                            if (mDebugLevel >= 4)
                            {
                                LogDebug("Decoy protein prefix found: " + decoyPrefix);
                            }

                            found = true;
                            break;
                        }
                    }
                    if (found)
                    {
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in IDPickerPlugin->LookForDecoyProteinsInMSGFPlusResults", ex);
                return false;
            }

            return true;
        }

        private CloseOutType MoveFilesIntoIDPickerSubdirectory()
        {
            var errorEncountered = false;

            try
            {
                var resultsDirectoryNamePath = Path.Combine(mWorkDir, mResultsDirectoryName);

                var sourceDirectory = new DirectoryInfo(resultsDirectoryNamePath);
                var targetDirectory = sourceDirectory.CreateSubdirectory("IDPicker");

                var fileSpecs = new List<string>();
                var filesToMove = new List<FileInfo>();

                fileSpecs.Add("*.idpXML");
                fileSpecs.Add("IDPicker*.*");
                fileSpecs.Add("Tool_Version_Info_IDPicker.txt");
                fileSpecs.Add(mIDPickerParamFileNameLocal);

                if (!mBatchFilesMoved)
                {
                    fileSpecs.Add("Run*.bat");
                }

                foreach (var fileSpec in fileSpecs)
                {
                    filesToMove.AddRange(sourceDirectory.GetFiles(fileSpec));
                }

                foreach (var fileToMove in filesToMove)
                {
                    var attempts = 0;
                    var success = false;

                    do
                    {
                        try
                        {
                            // Note that the file may have been moved already; confirm that it still exists
                            fileToMove.Refresh();

                            if (fileToMove.Exists)
                            {
                                fileToMove.MoveTo(Path.Combine(targetDirectory.FullName, fileToMove.Name));
                            }
                            success = true;
                        }
                        catch (Exception)
                        {
                            attempts++;
                            Global.IdleLoop(2);
                        }
                    } while (!success && attempts <= 3);

                    if (!success)
                    {
                        errorEncountered = true;
                        LogError("Unable to move " + fileToMove.Name + " into the IDPicker subdirectory; tried " + (attempts - 1) + " times");
                    }
                }
            }
            catch (Exception)
            {
                errorEncountered = true;
            }

            if (errorEncountered)
            {
                // Try to save whatever files were moved into the results directory
                var analysisResults = new AnalysisResults(mMgrParams, mJobParams);
                analysisResults.CopyFailedResultsToArchiveDirectory(Path.Combine(mWorkDir, mResultsDirectoryName));

                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private void ParseConsoleOutputFileForErrors(string consoleOutputFilePath)
        {
            var unhandledException = false;
            var exceptionText = string.Empty;

            try
            {
                if (!File.Exists(consoleOutputFilePath))
                    return;

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrEmpty(dataLine))
                        continue;

                    if (unhandledException)
                    {
                        if (string.IsNullOrEmpty(exceptionText))
                        {
                            exceptionText = dataLine;
                        }
                        else
                        {
                            exceptionText = ";" + dataLine;
                        }
                    }
                    else if (dataLine.StartsWith("Error:"))
                    {
                        if (!IgnoreError(dataLine))
                        {
                            mCmdRunnerErrors.Add(dataLine);
                        }
                    }
                    else if (dataLine.StartsWith("Unhandled Exception"))
                    {
                        mCmdRunnerErrors.Add(dataLine);
                        unhandledException = true;
                    }
                }

                if (!string.IsNullOrEmpty(exceptionText))
                {
                    mCmdRunnerErrors.Add(exceptionText);
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in ParseConsoleOutputFileForErrors: " + ex.Message);
            }
        }

        /// <summary>
        /// Run idpAssemble to organizes the search results into a hierarchy
        /// </summary>
        /// <param name="phrpBaseName"></param>
        /// <returns>True if successful, false if an error</returns>
        private bool RunAssemble(string phrpBaseName)
        {
            const int maxRuntimeMinutes = 90;

            // Create the Assemble.txt file
            // Since we're only processing one dataset, the file will only have one line
            var assembleFilePath = Path.Combine(mWorkDir, ASSEMBLE_GROUPING_FILENAME);

            var success = CreateAssembleFile(assembleFilePath, phrpBaseName);

            if (!success)
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    LogError("Error running idpAssemble");
                }
                return false;
            }

            // Define the errors that we can ignore
            ClearConcurrentBag(ref mCmdRunnerErrorsToIgnore);
            mCmdRunnerErrorsToIgnore.Add("protein database filename should be the same in all input files");
            mCmdRunnerErrorsToIgnore.Add("Could not find the default configuration file");

            // Define the path to the .Exe
            var progLoc = Path.Combine(mIDPickerProgramDirectory, IDPicker_Assemble);

            // Build the command string, for example:
            //  Assemble.xml -MaxFDR 0.1 -b Assemble.txt

            var arguments = ASSEMBLE_OUTPUT_FILENAME;
            arguments = AppendArgument(arguments, "AssemblyMaxFDR", "MaxFDR", "0.1");
            arguments += " -b Assemble.txt -dump";

            mProgress = PROGRESS_PCT_IDPicker_RUNNING_IDPAssemble;

            success = RunProgramWork("IDPAssemble", progLoc, arguments, IPD_Assemble_CONSOLE_OUTPUT, true, maxRuntimeMinutes);

            mIdpAssembleFilePath = Path.Combine(mWorkDir, ASSEMBLE_OUTPUT_FILENAME);

            if (success)
            {
                // Make sure the output file was created
                if (!File.Exists(mIdpAssembleFilePath))
                {
                    LogError("IDPicker Assemble results file not found: " + mIdpAssembleFilePath);
                    success = false;
                }
                else
                {
                    // ReSharper disable once GrammarMistakeInComment
                    // Do not keep the assemble input or output files
                    mJobParams.AddResultFileToSkip(ASSEMBLE_GROUPING_FILENAME);
                    mJobParams.AddResultFileToSkip(ASSEMBLE_OUTPUT_FILENAME);
                }
            }

            return success;
        }

        /// <summary>
        /// Run idpQonvert to convert the search scores in the pepXML file to q-values
        /// </summary>
        /// <param name="fastaFilePath"></param>
        /// <param name="decoyPrefix"></param>
        /// <param name="phrpResultType"></param>
        /// <param name="phrpBaseName"></param>
        private bool RunQonvert(string fastaFilePath, string decoyPrefix, PeptideHitResultTypes phrpResultType, string phrpBaseName)
        {
            const int maxRuntimeMinutes = 90;

            // Define the errors that we can ignore
            ClearConcurrentBag(ref mCmdRunnerErrorsToIgnore);
            mCmdRunnerErrorsToIgnore.Add("could not find the default configuration file");
            mCmdRunnerErrorsToIgnore.Add("could not find the default residue masses file");

            // Define the path to the .Exe
            var progLoc = Path.Combine(mIDPickerProgramDirectory, IDPicker_Qonvert);

            // Possibly override some options
            if (phrpResultType == PeptideHitResultTypes.MODa || phrpResultType == PeptideHitResultTypes.MODPlus)
            {
                // Higher MODa probability scores are better
                mIDPickerOptions["SearchScoreWeights"] = "Probability 1";
                mIDPickerOptions["NormalizedSearchScores"] = "Probability";
            }

            // Build the command string, for example:
            //   -MaxFDR 0.1 -ProteinDatabase c:\DMS_Temp_Org\ID_002339_125D2B84.fasta
            //   -SearchScoreWeights "msgfspecprob -1" -OptimizeScoreWeights 1
            //   -NormalizedSearchScores msgfspecprob -DecoyPrefix Reversed_
            //   -dump QC_Shew_11_06_pt5_3_13Feb12_Doc_11-12-07.pepXML
            var arguments = string.Empty;

            arguments = AppendArgument(arguments, "QonvertMaxFDR", "MaxFDR", "0.1");
            arguments += " -ProteinDatabase " + PossiblyQuotePath(fastaFilePath);
            arguments = AppendArgument(arguments, "SearchScoreWeights", "msgfspecprob -1");
            arguments = AppendArgument(arguments, "OptimizeScoreWeights", "1");
            arguments = AppendArgument(arguments, "NormalizedSearchScores", "msgfspecprob");

            arguments += " -DecoyPrefix " + PossiblyQuotePath(decoyPrefix);
            arguments += " -dump";              // This tells IDPQonvert to display the processing options that the program is using
            arguments += " " + mPepXMLFilePath;

            mProgress = PROGRESS_PCT_IDPicker_RUNNING_IDPQonvert;

            var success = RunProgramWork("IDPQonvert", progLoc, arguments, IPD_Qonvert_CONSOLE_OUTPUT, true, maxRuntimeMinutes);

            mIdpXMLFilePath = Path.Combine(mWorkDir, phrpBaseName + ".idpXML");

            if (success)
            {
                // Make sure the output file was created
                if (!File.Exists(mIdpXMLFilePath))
                {
                    LogError("IDPicker Qonvert results file not found: " + mIdpXMLFilePath);
                    success = false;
                }
            }

            return success;
        }

        /// <summary>
        /// Run idpReport to apply parsimony in protein assembly and generate reports
        /// </summary>
        private bool RunReport()
        {
            const int maxRuntimeMinutes = 60;

            const string outputDirectoryName = "IDPicker";

            // Define the errors that we can ignore
            ClearConcurrentBag(ref mCmdRunnerErrorsToIgnore);
            mCmdRunnerErrorsToIgnore.Add("protein database filename should be the same in all input files");
            mCmdRunnerErrorsToIgnore.Add("Could not find the default configuration file");

            // Define the path to the .Exe
            var progLoc = Path.Combine(mIDPickerProgramDirectory, IDPicker_Report);

            // Build the command string, for example:
            //  report Assemble.xml -MaxFDR 0.05 -MinDistinctPeptides 2 -MinAdditionalPeptides 2 -ModsAreDistinctByDefault true -MaxAmbiguousIds 2 -MinSpectraPerProtein 2 -OutputTextReport true

            var arguments = outputDirectoryName + " " + mIdpAssembleFilePath;
            arguments = AppendArgument(arguments, "ReportMaxFDR", "MaxFDR", "0.05");
            arguments = AppendArgument(arguments, "MinDistinctPeptides", "2");
            arguments = AppendArgument(arguments, "MinAdditionalPeptides", "2");
            arguments = AppendArgument(arguments, "ModsAreDistinctByDefault", "true");
            arguments = AppendArgument(arguments, "MaxAmbiguousIds", "2");
            arguments = AppendArgument(arguments, "MinSpectraPerProtein", "2");

            arguments += " -OutputTextReport true -dump";

            mProgress = PROGRESS_PCT_IDPicker_RUNNING_IDPReport;

            var success = RunProgramWork("IDPReport", progLoc, arguments, IPD_Report_CONSOLE_OUTPUT, true, maxRuntimeMinutes);

            if (success)
            {
                var reportDirectory = new DirectoryInfo(Path.Combine(mWorkDir, outputDirectoryName));

                // Make sure the output directory was created
                if (!reportDirectory.Exists)
                {
                    LogError("IDPicker report directory file not found: " + reportDirectory.FullName);
                    success = false;
                }

                if (success)
                {
                    var tsvFileFound = false;

                    // Move the .tsv files from the Report Directory up one level
                    foreach (var tsvFile in reportDirectory.GetFiles("*.tsv"))
                    {
                        tsvFile.MoveTo(Path.Combine(mWorkDir, tsvFile.Name));
                        tsvFileFound = true;
                    }

                    if (!tsvFileFound)
                    {
                        LogError("IDPicker report directory does not contain any TSV files: " + reportDirectory.FullName);
                        success = false;
                    }
                }

                if (success)
                {
                    // Copy the ConsoleOutput and RunProgram batch files into the Report Directory (and add them to the files to Skip)
                    // mFilenamesToAddToReportDirectory will already contain the batch file names

                    mFilenamesToAddToReportDirectory.Add(IPD_Qonvert_CONSOLE_OUTPUT);
                    mFilenamesToAddToReportDirectory.Add(IPD_Assemble_CONSOLE_OUTPUT);
                    mFilenamesToAddToReportDirectory.Add(IPD_Report_CONSOLE_OUTPUT);

                    foreach (var fileToAdd in mFilenamesToAddToReportDirectory)
                    {
                        CopyFileIntoReportDirectory(fileToAdd, reportDirectory.FullName);
                    }

                    mBatchFilesMoved = true;

                    // Zip the Report Directory
                    var zippedResultsFilePath = Path.Combine(mWorkDir, "IDPicker_HTML_Results.zip");
                    mZipTools.DebugLevel = mDebugLevel;
                    success = mZipTools.ZipDirectory(reportDirectory.FullName, zippedResultsFilePath, true);

                    if (!success && mZipTools.Message.IndexOf("OutOfMemoryException", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        mNeedToAbortProcessing = true;
                    }
                }
            }
            else
            {
                // Check whether mCmdRunnerErrors contains a known error message
                foreach (var error in mCmdRunnerErrors)
                {
                    if (error.Contains("no spectra in workspace"))
                    {
                        // Every protein was filtered out; we'll treat this as a successful completion of IDPicker
                        mMessage = string.Empty;
                        mEvalMessage = "IDPicker Report filtered out all of the proteins";
                        LogWarning(mEvalMessage + "; this indicates there are not enough filter-passing peptides.");
                        success = true;
                        break;
                    }
                }
            }

            return success;
        }

        /// <summary>
        /// Run IDPicker
        /// </summary>
        /// <param name="programDescription"></param>
        /// <param name="exePath"></param>
        /// <param name="arguments"></param>
        /// <param name="consoleOutputFileName">If empty, does not create a console output file</param>
        /// <param name="captureConsoleOutputViaDosRedirection"></param>
        /// <param name="maxRuntimeMinutes"></param>
        private bool RunProgramWork(string programDescription, string exePath, string arguments, string consoleOutputFileName,
            bool captureConsoleOutputViaDosRedirection, int maxRuntimeMinutes)
        {
            if (mDebugLevel >= 1)
            {
                LogMessage(exePath + " " + arguments.TrimStart(' '));
            }

            mCmdRunnerDescription = programDescription;
            ClearConcurrentBag(ref mCmdRunnerErrors);

            var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(cmdRunner);
            cmdRunner.ErrorEvent += CmdRunner_ConsoleErrorEvent;
            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;
            cmdRunner.Timeout += CmdRunner_Timeout;

            if (captureConsoleOutputViaDosRedirection)
            {
                // Create a batch file to run the command
                // Capture the console output (including output to the error stream) via redirection symbols:
                //    exePath arguments > ConsoleOutputFile.txt 2>&1

                var exePathOriginal = exePath;
                var argumentsOriginal = arguments;

                programDescription = programDescription.Replace(" ", "_");

                var batchFileName = "Run_" + programDescription + ".bat";
                mFilenamesToAddToReportDirectory.Add(batchFileName);

                // Update the Exe path to point to the RunProgram batch file; update arguments to be empty
                exePath = Path.Combine(mWorkDir, batchFileName);
                arguments = string.Empty;

                if (string.IsNullOrEmpty(consoleOutputFileName))
                {
                    consoleOutputFileName = programDescription + "_Console_Output.txt";
                }

                // Create the batch file
                using var writer = new StreamWriter(new FileStream(exePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine(exePathOriginal + " " + argumentsOriginal + " > " + consoleOutputFileName + " 2>&1");
            }

            if (captureConsoleOutputViaDosRedirection || string.IsNullOrEmpty(consoleOutputFileName))
            {
                cmdRunner.CreateNoWindow = false;
                cmdRunner.EchoOutputToConsole = false;
                cmdRunner.CacheStandardOutput = false;
                cmdRunner.WriteConsoleOutputToFile = false;
            }
            else
            {
                cmdRunner.CreateNoWindow = true;
                cmdRunner.EchoOutputToConsole = true;
                cmdRunner.CacheStandardOutput = false;
                cmdRunner.WriteConsoleOutputToFile = true;
                cmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, consoleOutputFileName);
            }

            var maxRuntimeSeconds = maxRuntimeMinutes * 60;

            var success = cmdRunner.RunProgram(exePath, arguments, programDescription, true, maxRuntimeSeconds);

            if (mCmdRunnerErrors.Count == 0 && !string.IsNullOrEmpty(cmdRunner.CachedConsoleError))
            {
                LogWarning("Cached console error is not empty, but mCmdRunnerErrors is empty; need to add code to parse CmdRunner.CachedConsoleError");
            }

            if (captureConsoleOutputViaDosRedirection)
            {
                ParseConsoleOutputFileForErrors(Path.Combine(mWorkDir, consoleOutputFileName));
            }
            else if (mCmdRunnerErrors.Count > 0)
            {
                // Append the error messages to the log
                // Note that ProgRunner will have already included them in the ConsoleOutput.txt file
                foreach (var error in mCmdRunnerErrors)
                {
                    if (!error.StartsWith("warning", StringComparison.OrdinalIgnoreCase))
                    {
                        LogError("... " + error);
                    }
                }
            }

            if (!success)
            {
                mMessage = "Error running " + programDescription;

                if (mCmdRunnerErrors.Count > 0)
                {
                    mMessage += ": " + mCmdRunnerErrors.First();
                }

                LogError(mMessage);

                if (cmdRunner.ExitCode != 0)
                {
                    LogWarning(programDescription + " returned a non-zero exit code: " + cmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to " + programDescription + " failed (but exit code is 0)");
                }
            }
            else
            {
                mStatusTools.UpdateAndWrite(mProgress);

                if (mDebugLevel >= 3)
                {
                    LogDebug(programDescription + " Complete");
                }
            }

            return success;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string idPickerProgLoc, bool skipIDPicker)
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // We will store paths to key files in toolFiles
            var toolFiles = new List<FileInfo>();

            // Determine the path to the PeptideListToXML.exe
            mPeptideListToXMLExePath = DetermineProgramLocation("PeptideListToXMLProgLoc", PEPTIDE_LIST_TO_XML_EXE);

            if (skipIDPicker)
            {
                // Only store the version of PeptideListToXML.exe in the database
                StoreToolVersionInfoOneFile(ref toolVersionInfo, mPeptideListToXMLExePath);
                toolFiles.Add(new FileInfo(mPeptideListToXMLExePath));
            }
            else
            {
                var idPickerProgram = new FileInfo(idPickerProgLoc);

                if (!idPickerProgram.Exists)
                {
                    try
                    {
                        toolVersionInfo = "Unknown";
                        return SetStepTaskToolVersion(toolVersionInfo, new List<FileInfo>());
                    }
                    catch (Exception ex)
                    {
                        LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                        return false;
                    }
                }

                if (idPickerProgram.Directory == null)
                {
                    LogError("Cannot determine the parent directory of " + idPickerProgram.FullName);
                    return false;
                }

                mIDPickerProgramDirectory = idPickerProgram.Directory.FullName;

                // Lookup the version of idpAssemble.exe (which is a .NET app; cannot use idpQonvert.exe since it is a C++ app)
                var idpAssembleExePath = Path.Combine(mIDPickerProgramDirectory, IDPicker_Assemble);
                mToolVersionUtilities.StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, idpAssembleExePath);
                toolFiles.Add(new FileInfo(idpAssembleExePath));

                // Lookup the version of idpReport.exe
                var idpReportExePath = Path.Combine(mIDPickerProgramDirectory, IDPicker_Report);
                mToolVersionUtilities.StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, idpReportExePath);
                toolFiles.Add(new FileInfo(idpReportExePath));

                // Also include idpQonvert.exe in toolFiles (version determination does not work)
                var idpQonvertExePath = Path.Combine(mIDPickerProgramDirectory, IDPicker_Qonvert);
                toolFiles.Add(new FileInfo(idpQonvertExePath));

                // Lookup the version of PeptideListToXML.exe
                StoreToolVersionInfoOneFile(ref toolVersionInfo, mPeptideListToXMLExePath);
                toolFiles.Add(new FileInfo(mPeptideListToXMLExePath));
            }

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        private bool ZipPepXMLFile(string phrpBaseName)
        {
            try
            {
                var zippedPepXMLFilePath = Path.Combine(mWorkDir, phrpBaseName + "_pepXML.zip");

                if (!ZipFile(mPepXMLFilePath, false, zippedPepXMLFilePath))
                {
                    LogError("Error zipping PepXML file");
                    return false;
                }

                // Add the .pepXML file to .FilesToDelete since we only want to keep the Zipped version
                mJobParams.AddResultFileToSkip(Path.GetFileName(mPepXMLFilePath));
            }
            catch (Exception ex)
            {
                LogError("Exception zipping PepXML output file", ex);
                return false;
            }

            return true;
        }

        private void CmdRunner_ConsoleErrorEvent(string newText, Exception ex)
        {
            if (mCmdRunnerErrors == null)
                return;

            // Split NewText on newline characters
            var newLineChars = new[] { '\r', '\n' };

            foreach (var item in newText.Split(newLineChars, StringSplitOptions.RemoveEmptyEntries))
            {
                var item2 = item.Trim(newLineChars);

                if (string.IsNullOrEmpty(item2))
                {
                    continue;
                }

                // Confirm that item does not contain any text in mCmdRunnerErrorsToIgnore
                if (!IgnoreError(item2))
                {
                    mCmdRunnerErrors.Add(item2);
                }
            }
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            LogProgress("IDPicker");
        }

        private void CmdRunner_Timeout()
        {
            if (mDebugLevel >= 2)
            {
                LogError("Aborted " + mCmdRunnerDescription);
            }
        }
    }
}
