using AnalysisManagerBase;
using PHRPReader;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace AnalysisManagerIDPickerPlugIn
{
    /// <summary>
    /// Class for running IDPicker
    /// </summary>
    public class AnalysisToolRunnerIDPicker : AnalysisToolRunnerBase
    {
        // Ignore Spelling: cmd, idp, Qonvert, prepend, parm, MODa, xxx, msgfspecprob

        #region "Module Variables"

        /// <summary>
        /// If True, always skip IDPicker
        /// </summary>
        public const bool ALWAYS_SKIP_IDPICKER = true;

        private const string PEPXML_CONSOLE_OUTPUT = "PepXML_ConsoleOutput.txt";

        private const string IPD_Qonvert_CONSOLE_OUTPUT = "IDPicker_Qonvert_ConsoleOutput.txt";
        private const string IPD_Assemble_CONSOLE_OUTPUT = "IDPicker_Assemble_ConsoleOutput.txt";
        private const string IPD_Report_CONSOLE_OUTPUT = "IDPicker_Report_ConsoleOutput.txt";

        private const string IDPicker_Qonvert = "idpQonvert.exe";
        private const string IDPicker_Assemble = "idpAssemble.exe";
        private const string IDPicker_Report = "idpReport.exe";
        private const string IDPicker_GUI = "IdPickerGui.exe";

        private const string ASSEMBLE_GROUPING_FILENAME = "Assemble.txt";
        private const string ASSEMBLE_OUTPUT_FILENAME = "IDPicker_AssembledResults.xml";

        private const string MSGFDB_DECOY_PROTEIN_PREFIX = "REV_";
        private const string MSGFPLUS_DECOY_PROTEIN_PREFIX = "XXX_";

        private const string PEPTIDE_LIST_TO_XML_EXE = "PeptideListToXML.exe";

        private const float PROGRESS_PCT_IDPicker_STARTING = 1;
        private const float PROGRESS_PCT_IDPicker_SEARCHING_FOR_FILES = 5;
        private const float PROGRESS_PCT_IDPicker_CREATING_PEPXML_FILE = 10;
        private const float PROGRESS_PCT_IDPicker_RUNNING_IDPQonvert = 20;
        private const float PROGRESS_PCT_IDPicker_RUNNING_IDPAssemble = 60;
        private const float PROGRESS_PCT_IDPicker_RUNNING_IDPReport = 70;
        private const float PROGRESS_PCT_IDPicker_COMPLETE = 95;
        private const float PROGRESS_PCT_COMPLETE = 99;

        private string mIDPickerProgramFolder = string.Empty;
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

        // This list tracks error message text that we look for when considering whether or not to ignore an error message
        private ConcurrentBag<string> mCmdRunnerErrorsToIgnore;

        // This list tracks files that we want to include in the zipped up IDPicker report folder
        private List<string> mFilenamesToAddToReportFolder;

        private bool mBatchFilesMoved;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs PepXML converter and IDPicker tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            // As of January 21, 2015 we are now always skipping IDPicker (and thus simply creating the .pepXML file)
            var skipIDPicker = ALWAYS_SKIP_IDPICKER;

            var processingSuccess = true;

            mIDPickerOptions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            mCmdRunnerErrors = new ConcurrentBag<string>();
            mCmdRunnerErrorsToIgnore = new ConcurrentBag<string>();
            mFilenamesToAddToReportFolder = new List<string>();

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

                // Determine the path to the IDPicker program (idpQonvert); folder will also contain idpAssemble.exe and idpReport.exe
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
                var resultType = mJobParams.GetParam("ResultType");

                var ePHRPResultType = clsPHRPReader.GetPeptideHitResultType(resultType);
                if (ePHRPResultType == clsPHRPReader.PeptideHitResultTypes.Unknown)
                {
                    mMessage = "Invalid tool result type (not supported by IDPicker): " + resultType;
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Define the path to the synopsis file
                var synFilePath = Path.Combine(mWorkDir, clsPHRPReader.GetPHRPSynopsisFileName(ePHRPResultType, mDatasetName));
                if (!File.Exists(synFilePath))
                {
                    var alternateFilePath = clsPHRPReader.AutoSwitchToLegacyMSGFDBIfRequired(synFilePath, "Dataset_msgfdb.txt");
                    if (File.Exists(alternateFilePath))
                    {
                        synFilePath = alternateFilePath;
                    }
                }

                if (!AnalysisResources.ValidateFileHasData(synFilePath, "Synopsis file", out var errorMessage))
                {
                    // The synopsis file is empty
                    mMessage = errorMessage;
                    return CloseOutType.CLOSEOUT_NO_DATA;
                }

                // Define the path to the fasta file
                var orgDbDir = mMgrParams.GetParam("OrgDbDir");
                var fastaFilePath = Path.Combine(orgDbDir, mJobParams.GetParam("PeptideSearch", "generatedFastaName"));

                var fastaFile = new FileInfo(fastaFilePath);

                if (!skipIDPicker && !fastaFile.Exists)
                {
                    // Fasta file not found
                    mMessage = "Fasta file not found: " + fastaFile.Name;
                    LogError("Fasta file not found: " + fastaFile.FullName);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                var splitFasta = mJobParams.GetJobParameter("SplitFasta", false);

                if (!skipIDPicker && splitFasta)
                {
                    skipIDPicker = true;
                    LogWarning("SplitFasta jobs typically have fasta files too large for IDPQonvert; skipping IDPicker", true);
                }

                // Store the version of IDPicker and PeptideListToXML in the database
                // Alternatively, if skipIDPicker is true, just store the version of PeptideListToXML

                // This function updates mPeptideListToXMLExePath and mIDPickerProgramFolder
                if (!StoreToolVersionInfo(progLocQonvert, skipIDPicker))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining IDPicker version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Create the PepXML file
                var pepXmlSuccess = CreatePepXMLFile(fastaFile.FullName, synFilePath, ePHRPResultType);
                if (!pepXmlSuccess)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Error creating PepXML file";
                    }
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
                    var idPickerSuccess = RunIDPickerWrapper(ePHRPResultType, synFilePath, fastaFile.FullName, out var processingError, out var criticalError);

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
                    var zipSuccess = ZipPepXMLFile();
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
                PRISM.ProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, return CloseOutType.CLOSEOUT_FAILED

                    mJobParams.RemoveResultFileToSkip(ASSEMBLE_GROUPING_FILENAME);
                    mJobParams.RemoveResultFileToSkip(ASSEMBLE_OUTPUT_FILENAME);

                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var folderCreated = MakeResultsDirectory();
                if (!folderCreated)
                {
                    // MakeResultsDirectory handles posting to local log, so set database error message and exit
                    mMessage = "Error making results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!skipIDPicker)
                {
                    var moveResult = MoveFilesIntoIDPickerSubdirectory();
                    if (moveResult != CloseOutType.CLOSEOUT_SUCCESS)
                    {
                        // Note that MoveResultFiles should have already called AnalysisResults.CopyFailedResultsToArchiveFolder
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
                mMessage = "Exception in IDPickerPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool RunIDPickerWrapper(clsPHRPReader.PeptideHitResultTypes ePHRPResultType, string synFilePath, string fastaFilePath,
            out bool processingError, out bool criticalError)
        {
            bool success;
            processingError = false;
            criticalError = false;

            // Determine the prefix used by decoy proteins
            var decoyPrefix = string.Empty;

            if (ePHRPResultType == clsPHRPReader.PeptideHitResultTypes.MSGFPlus)
            {
                // If we run MSGF+ with target/decoy mode and showDecoy=1, the _syn.txt file will have decoy proteins that start with REV_ or XXX_
                // Check for this
                success = LookForDecoyProteinsInMSGFPlusResults(synFilePath, ePHRPResultType, ref decoyPrefix);
                if (!success)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Error looking for decoy proteins in the MSGF+ synopsis file";
                    }
                    criticalError = true;
                    return false;
                }
            }

            if (string.IsNullOrEmpty(decoyPrefix))
            {
                // Look for decoy proteins in the Fasta file
                success = DetermineDecoyProteinPrefix(fastaFilePath, out decoyPrefix);
                if (!success)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Error looking for decoy proteins in the Fasta file";
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
            success = RunQonvert(fastaFilePath, decoyPrefix, ePHRPResultType);
            if (!success)
            {
                processingError = true;
                return false;
            }

            // Organizes the search results into a hierarchy
            success = RunAssemble();
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
        /// <param name="appendIfMissing">If True, append the argument using valueIfMissing if not found in mIDPickerOptions; if false, and not found, does not append the argument</param>
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

        private bool CreateAssembleFile(string assembleFilePath)
        {
            try
            {
                // Prepend dataset name with PNNL/
                // Also make sure it doesn't contain any spaces
                var datasetLabel = "PNNL/" + mDatasetName.Replace(" ", "_");

                // Create the Assemble.txt file
                using var writer = new StreamWriter(new FileStream(assembleFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine(datasetLabel + " " + Path.GetFileName(mIdpXMLFilePath));
            }
            catch (Exception ex)
            {
                mMessage = "Exception in IDPickerPlugin->CreateAssembleFile";
                LogError(mMessage, ex);
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
        /// Copies a file into folder reportFolderPath then adds it to mJobParams.AddResultFileToSkip
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="reportFolderPath"></param>
        private void CopyFileIntoReportFolder(string fileName, string reportFolderPath)
        {
            try
            {
                var sourceFile = new FileInfo(Path.Combine(mWorkDir, fileName));

                if (sourceFile.Exists)
                {
                    sourceFile.CopyTo(Path.Combine(reportFolderPath, fileName), true);
                    mJobParams.AddResultFileToSkip(fileName);
                }
            }
            catch (Exception ex)
            {
                LogWarning("Error copying ConsoleOutput file into the IDPicker Report folder: " + ex.Message);
            }
        }

        private bool CreatePepXMLFile(string fastaFilePath, string synFilePath, clsPHRPReader.PeptideHitResultTypes ePHRPResultType)
        {
            // PepXML file creation should generally be done in less than 10 minutes
            // However, for huge fasta files, conversion could take several hours
            const int maxRuntimeMinutes = 480;

            bool success;

            try
            {
                // Set up and execute a program runner to run PeptideListToXML
                var paramFileName = mJobParams.GetParam("ParmFileName");

                mPepXMLFilePath = Path.Combine(mWorkDir, mDatasetName + ".pepXML");
                var iHitsPerSpectrum = mJobParams.GetJobParameter("PepXMLHitsPerSpectrum", 3);

                var arguments = PossiblyQuotePath(synFilePath) +
                                " /E:" + PossiblyQuotePath(paramFileName) +
                                " /F:" + PossiblyQuotePath(fastaFilePath) +
                                " /H:" + iHitsPerSpectrum;

                if (ePHRPResultType == clsPHRPReader.PeptideHitResultTypes.MODa || ePHRPResultType == clsPHRPReader.PeptideHitResultTypes.MODPlus)
                {
                    // The SpecProb values listed in the _syn_MSGF.txt file are not true spectral probabilities
                    // Instead, they're just 1 - Probability  (where Probability is a value between 0 and 1 assigned by MODa)
                    // Therefore, don't include them in the PepXML file
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
                        mMessage = "Error creating PepXML file";
                        LogError(mMessage + ", job " + mJob);
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
                mMessage = "Exception in IDPickerPlugin->CreatePepXMLFile";
                LogError(mMessage, ex);
                return false;
            }

            return success;
        }

        /// <summary>
        /// Determine the prefix used by to denote decoy (reversed) proteins
        /// </summary>
        /// <param name="fastaFilePath"></param>
        /// <param name="decoyPrefix"></param>
        /// <returns>True if success; false if an error</returns>
        private bool DetermineDecoyProteinPrefix(string fastaFilePath, out string decoyPrefix)
        {
            decoyPrefix = string.Empty;

            try
            {
                if (mDebugLevel >= 3)
                {
                    LogDebug("Looking for decoy proteins in the fasta file");
                }

                var reversedProteinPrefixes = new SortedSet<string>
                {
                    "reversed_",                                   // MTS reversed proteins                 // reversed[_]%'
                    "scrambled_",                                  // MTS scrambled proteins                // scrambled[_]%'
                    "xxx.",                                        // Inspect reversed/scrambled proteins   // xxx.%'
                    MSGFDB_DECOY_PROTEIN_PREFIX.ToLower(),         // MSGFDB reversed proteins              // rev[_]%'
                    MSGFPLUS_DECOY_PROTEIN_PREFIX.ToLower()        // MSGF+ reversed proteins               // xxx[_]%'
                };

                // Note that X!Tandem decoy proteins end with ":reversed"
                // IDPicker doesn't support decoy protein name suffixes, only prefixes

                var prefixStats = new Dictionary<string, int>();

                var fastaFileReader = new ProteinFileReader.FastaFileReader();

                if (!fastaFileReader.OpenFile(fastaFilePath))
                {
                    mMessage = "Error reading fasta file with ProteinFileReader";
                    return false;
                }

                while (fastaFileReader.ReadNextProteinEntry())
                {
                    var protein = fastaFileReader.ProteinName;

                    foreach (var prefix in reversedProteinPrefixes)
                    {
                        if (protein.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                        {
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
                mMessage = "Exception in IDPickerPlugin->DetermineDecoyProteinPrefix";
                LogError(mMessage, ex);
                return false;
            }

            return true;
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
                    mMessage = "IDPicker parameter file not defined";
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
                mMessage = "Exception in IDPickerPlugin->LoadIDPickerOptions";
                LogError(mMessage, ex);
                return false;
            }

            return true;
        }

        private bool LookForDecoyProteinsInMSGFPlusResults(string synFilePath, clsPHRPReader.PeptideHitResultTypes resultType, ref string decoyPrefix)
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
                    LogDebug("Looking for decoy proteins in the MSGF+ synopsis file");
                }

                using var reader = new clsPHRPReader(synFilePath, resultType, false, false, false);
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
                mMessage = "Exception in IDPickerPlugin->LookForDecoyProteinsInMSGFPlusResults";
                LogError(mMessage, ex);
                return false;
            }

            return true;
        }

        private CloseOutType MoveFilesIntoIDPickerSubdirectory()
        {
            var errorEncountered = false;

            try
            {
                var resFolderNamePath = Path.Combine(mWorkDir, mResultsDirectoryName);

                var sourceFolder = new DirectoryInfo(resFolderNamePath);
                var targetFolder = sourceFolder.CreateSubdirectory("IDPicker");

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
                    filesToMove.AddRange(sourceFolder.GetFiles(fileSpec));
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
                                fileToMove.MoveTo(Path.Combine(targetFolder.FullName, fileToMove.Name));
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
                if (File.Exists(consoleOutputFilePath))
                {
                    using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        while (!reader.EndOfStream)
                        {
                            var dataLine = reader.ReadLine();

                            if (string.IsNullOrEmpty(dataLine))
                                continue;

                            if (unhandledException)
                            {
                                if (string.IsNullOrEmpty(exceptionText))
                                {
                                    exceptionText = string.Copy(dataLine);
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
                    }

                    if (!string.IsNullOrEmpty(exceptionText))
                    {
                        mCmdRunnerErrors.Add(exceptionText);
                    }
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
        private bool RunAssemble()
        {
            const int maxRuntimeMinutes = 90;

            // Create the Assemble.txt file
            // Since we're only processing one dataset, the file will only have one line
            var assembleFilePath = Path.Combine(mWorkDir, ASSEMBLE_GROUPING_FILENAME);

            var success = CreateAssembleFile(assembleFilePath);
            if (!success)
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Error running idpAssemble";
                }
                return false;
            }

            // Define the errors that we can ignore
            ClearConcurrentBag(ref mCmdRunnerErrorsToIgnore);
            mCmdRunnerErrorsToIgnore.Add("protein database filename should be the same in all input files");
            mCmdRunnerErrorsToIgnore.Add("Could not find the default configuration file");

            // Define the path to the .Exe
            var progLoc = Path.Combine(mIDPickerProgramFolder, IDPicker_Assemble);

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
                    mMessage = "IDPicker Assemble results file not found";
                    LogError(mMessage + " at " + mIdpAssembleFilePath);
                    success = false;
                }
                else
                {
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
        /// <param name="ePHRPResultType"></param>
        private bool RunQonvert(string fastaFilePath, string decoyPrefix, clsPHRPReader.PeptideHitResultTypes ePHRPResultType)
        {
            const int maxRuntimeMinutes = 90;

            // Define the errors that we can ignore
            ClearConcurrentBag(ref mCmdRunnerErrorsToIgnore);
            mCmdRunnerErrorsToIgnore.Add("could not find the default configuration file");
            mCmdRunnerErrorsToIgnore.Add("could not find the default residue masses file");

            // Define the path to the .Exe
            var progLoc = Path.Combine(mIDPickerProgramFolder, IDPicker_Qonvert);

            // Possibly override some options
            if (ePHRPResultType == clsPHRPReader.PeptideHitResultTypes.MODa || ePHRPResultType == clsPHRPReader.PeptideHitResultTypes.MODPlus)
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

            mIdpXMLFilePath = Path.Combine(mWorkDir, mDatasetName + ".idpXML");

            if (success)
            {
                // Make sure the output file was created
                if (!File.Exists(mIdpXMLFilePath))
                {
                    mMessage = "IDPicker Qonvert results file not found";
                    LogError(mMessage + " at " + mIdpXMLFilePath);
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

            const string outputFolderName = "IDPicker";

            // Define the errors that we can ignore
            ClearConcurrentBag(ref mCmdRunnerErrorsToIgnore);
            mCmdRunnerErrorsToIgnore.Add("protein database filename should be the same in all input files");
            mCmdRunnerErrorsToIgnore.Add("Could not find the default configuration file");

            // Define the path to the .Exe
            var progLoc = Path.Combine(mIDPickerProgramFolder, IDPicker_Report);

            // Build the command string, for example:
            //  report Assemble.xml -MaxFDR 0.05 -MinDistinctPeptides 2 -MinAdditionalPeptides 2 -ModsAreDistinctByDefault true -MaxAmbiguousIds 2 -MinSpectraPerProtein 2 -OutputTextReport true

            var arguments = outputFolderName + " " + mIdpAssembleFilePath;
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
                var reportFolder = new DirectoryInfo(Path.Combine(mWorkDir, outputFolderName));

                // Make sure the output folder was created
                if (!reportFolder.Exists)
                {
                    mMessage = "IDPicker report folder file not found";
                    LogError(mMessage + " at " + reportFolder.FullName);
                    success = false;
                }

                if (success)
                {
                    var tsvFileFound = false;

                    // Move the .tsv files from the Report folder up one level
                    foreach (var tsvFile in reportFolder.GetFiles("*.tsv"))
                    {
                        tsvFile.MoveTo(Path.Combine(mWorkDir, tsvFile.Name));
                        tsvFileFound = true;
                    }

                    if (!tsvFileFound)
                    {
                        mMessage = "IDPicker report folder does not contain any TSV files";
                        LogError(mMessage + "; " + reportFolder.FullName);
                        success = false;
                    }
                }

                if (success)
                {
                    // Copy the ConsoleOutput and RunProgram batch files into the Report folder (and add them to the files to Skip)
                    // mFilenamesToAddToReportFolder will already contain the batch file names

                    mFilenamesToAddToReportFolder.Add(IPD_Qonvert_CONSOLE_OUTPUT);
                    mFilenamesToAddToReportFolder.Add(IPD_Assemble_CONSOLE_OUTPUT);
                    mFilenamesToAddToReportFolder.Add(IPD_Report_CONSOLE_OUTPUT);

                    foreach (var fileToAdd in mFilenamesToAddToReportFolder)
                    {
                        CopyFileIntoReportFolder(fileToAdd, reportFolder.FullName);
                    }

                    mBatchFilesMoved = true;

                    // Zip the report folder
                    var zippedResultsFilePath = Path.Combine(mWorkDir, "IDPicker_HTML_Results.zip");
                    mDotNetZipTools.DebugLevel = mDebugLevel;
                    success = mDotNetZipTools.ZipDirectory(reportFolder.FullName, zippedResultsFilePath, true);

                    if (!success && mDotNetZipTools.Message.IndexOf("OutOfMemoryException", StringComparison.OrdinalIgnoreCase) >= 0)
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
                        // All of the proteins were filtered out; we'll treat this as a successful completion of IDPicker
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

            mCmdRunnerDescription = string.Copy(programDescription);
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

                var exePathOriginal = string.Copy(exePath);
                var argumentsOriginal = string.Copy(arguments);

                programDescription = programDescription.Replace(" ", "_");

                var batchFileName = "Run_" + programDescription + ".bat";
                mFilenamesToAddToReportFolder.Add(batchFileName);

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

                mIDPickerProgramFolder = idPickerProgram.Directory.FullName;

                // Lookup the version of idpAssemble.exe (which is a .NET app; cannot use idpQonvert.exe since it is a C++ app)
                var idpAssembleExePath = Path.Combine(mIDPickerProgramFolder, IDPicker_Assemble);
                mToolVersionUtilities.StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, idpAssembleExePath);
                toolFiles.Add(new FileInfo(idpAssembleExePath));

                // Lookup the version of idpReport.exe
                var idpReportExePath = Path.Combine(mIDPickerProgramFolder, IDPicker_Report);
                mToolVersionUtilities.StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, idpReportExePath);
                toolFiles.Add(new FileInfo(idpReportExePath));

                // Also include idpQonvert.exe in toolFiles (version determination does not work)
                var idpQonvertExePath = Path.Combine(mIDPickerProgramFolder, IDPicker_Qonvert);
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

        private bool ZipPepXMLFile()
        {
            try
            {
                var zippedPepXMLFilePath = Path.Combine(mWorkDir, mDatasetName + "_pepXML.zip");

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

        #endregion

        #region "Event Handlers"

        private void CmdRunner_ConsoleErrorEvent(string NewText, Exception ex)
        {
            if (mCmdRunnerErrors == null)
                return;

            // Split NewText on newline characters
            var newLineChars = new[] { '\r', '\n' };

            var splitLine = NewText.Split(newLineChars, StringSplitOptions.RemoveEmptyEntries);

            foreach (var item in splitLine)
            {
                var item2 = item.Trim(newLineChars);

                if (!string.IsNullOrEmpty(item2))
                {
                    // Confirm that item does not contain any text in mCmdRunnerErrorsToIgnore
                    if (!IgnoreError(item2))
                    {
                        mCmdRunnerErrors.Add(item2);
                    }
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

        #endregion
    }
}