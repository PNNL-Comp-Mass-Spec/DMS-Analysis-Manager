//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 07/11/2007
//
// Program converted from original version written by J.D. Sandoval, PNNL.
// Conversion performed as part of upgrade to VB.Net 2005, modification for use with manager and broker databases
//
//*********************************************************************************************************

using AnalysisManagerBase;
using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PeptideHitResultsProcessor.Processor;
using PHRPReader;

namespace AnalysisManagerExtractionPlugin
{
    /// <summary>
    /// Calls PeptideHitResultsProcRunner.exe
    /// </summary>
    public class PepHitResultsProcWrapper : EventNotifier
    {
        // Ignore Spelling: FASTA, MODa, Parm, PHRP, Pvalue, PepToProt

        public const string PHRP_LOG_FILE_NAME = "PHRP_LogFile.txt";

        private readonly int mDebugLevel;
        private readonly IMgrParams mMgrParams;
        private readonly IJobParams mJobParams;

        private int mProgress;
        private string mErrorMessage = string.Empty;
        private string mPHRPConsoleOutputFilePath;
        private string mWarningMessage = string.Empty;

        public string ErrorMessage => mErrorMessage ?? string.Empty;

        public string WarningMessage => mWarningMessage ?? string.Empty;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">IMgrParams object containing manager settings</param>
        /// <param name="jobParams">IJobParams object containing job parameters</param>
        public PepHitResultsProcWrapper(IMgrParams mgrParams, IJobParams jobParams)
        {
            mMgrParams = mgrParams;
            mJobParams = jobParams;
            mDebugLevel = mMgrParams.GetParam("DebugLevel", 1);
        }

        /// <summary>
        /// Converts MSFragger, MaxQuant, MS-GF+, MSAlign, MODa, MODPlus, X!Tandem, etc. output file to a flat file
        /// </summary>
        /// <param name="peptideSearchResultsFilePath">Peptide search results file path</param>
        /// <param name="createFirstHitsFile">If true, create the first hits file</param>
        /// <param name="createSynopsisFile">If true, create the synopsis file</param>
        /// <param name="fastaFilePath">FASTA file path</param>
        /// <param name="resultType">PHRP result type enum</param>
        /// <param name="outputFileBaseName">Base name to use for output files; PHRP will auto-determine the name if this is an empty string</param>
        /// <returns>Enum indicating success or failure</returns>
        public CloseOutType ExtractDataFromResults(
            string peptideSearchResultsFilePath,
            bool createFirstHitsFile,
            bool createSynopsisFile,
            string fastaFilePath,
            PeptideHitResultTypes resultType,
            string outputFileBaseName = "")
        {
            var paramFileName = mJobParams.GetParam("ParamFileName");

            try
            {
                if (!createFirstHitsFile && !createSynopsisFile)
                {
                    ReportError("Must create either a first hits file or a synopsis file (or both); cannot run PHRP");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                mProgress = 0;
                mErrorMessage = string.Empty;
                mWarningMessage = string.Empty;

                if (string.IsNullOrWhiteSpace(peptideSearchResultsFilePath))
                {
                    ReportError("peptideSearchResultsFileName is empty; unable to run PHRP");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Define the modification definitions file name
                var modDefsFileName = Path.GetFileNameWithoutExtension(paramFileName) + AnalysisResourcesExtraction.MOD_DEFS_FILE_SUFFIX;

                var psmResultsFile = new FileInfo(peptideSearchResultsFilePath);

                if (psmResultsFile.Directory == null)
                {
                    ReportError("Unable to determine the parent directory of the PSM results file: " + peptideSearchResultsFilePath);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // The ToolName job parameter holds the name of the job script we are executing
                var scriptName = mJobParams.GetParam("ToolName");
                bool ignorePeptideToProteinMapErrors;

                if (scriptName.StartsWith("MSPathFinder", StringComparison.OrdinalIgnoreCase))
                {
                    OnStatusEvent("Ignoring peptide to protein mapping errors since this is an MSPathFinder job");
                    ignorePeptideToProteinMapErrors = true;
                }
                else
                {
                    ignorePeptideToProteinMapErrors = mJobParams.GetJobParameter("IgnorePeptideToProteinMapError", false);

                    if (ignorePeptideToProteinMapErrors)
                    {
                        OnStatusEvent("Ignoring peptide to protein mapping errors since job parameter IgnorePeptideToProteinMapError is true");
                    }
                }

                mPHRPConsoleOutputFilePath = Path.Combine(psmResultsFile.Directory.FullName, "PHRPOutput.txt");

                var progLoc = mMgrParams.GetParam("PHRPProgLoc");
                progLoc = Path.Combine(progLoc, "PeptideHitResultsProcRunner.exe");

                // Verify that program file exists
                if (!File.Exists(progLoc))
                {
                    ReportError("PHRP not found at " + progLoc);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Set up and execute a program runner to run the PHRP
                // Note:
                //   /SynPvalue is only used when processing Inspect files
                //   /SynProb is only used for MODa and MODPlus results
                var arguments =
                    psmResultsFile.FullName +
                    " /O:" + psmResultsFile.DirectoryName +
                    " /M:" + modDefsFileName +
                    " /T:" + AnalysisResourcesExtraction.MASS_CORRECTION_TAGS_FILENAME +
                    " /N:" + paramFileName +
                    " /SynPvalue:0.2" +
                    " /SynProb:0.05" +
                    " /L:" + Path.Combine(psmResultsFile.Directory.FullName, PHRP_LOG_FILE_NAME);

                var skipProteinMods = mJobParams.GetJobParameter("SkipProteinMods", false);

                if (!skipProteinMods)
                {
                    arguments += " /ProteinMods";
                }

                if (!string.IsNullOrWhiteSpace(fastaFilePath))
                {
                    // Note that FastaFilePath will likely be empty if job parameter SkipProteinMods is true
                    arguments += " /F:" + AnalysisToolRunnerBase.PossiblyQuotePath(fastaFilePath);
                }

                // Note that PHRP assumes /FHT=True and /Syn=True by default
                // Thus, we only need to use these switches if either of these should be false
                if (!createFirstHitsFile || !createSynopsisFile)
                {
                    arguments += " /FHT:" + createFirstHitsFile;
                    arguments += " /Syn:" + createSynopsisFile;
                }

                // PHRP defaults to use /MSGFPlusSpecEValue:5E-7  and  /MSGFPlusEValue:0.75
                // Adjust these if defined in the job parameters
                var msgfPlusSpecEValue = mJobParams.GetJobParameter("MSGFPlusSpecEValue", "");
                var msgfPlusEValue = mJobParams.GetJobParameter("MSGFPlusEValue", "");

                if (!string.IsNullOrWhiteSpace(msgfPlusSpecEValue))
                {
                    arguments += " /MSGFPlusSpecEValue:" + msgfPlusSpecEValue;
                }

                if (!string.IsNullOrWhiteSpace(msgfPlusEValue))
                {
                    arguments += " /MSGFPlusEValue:" + msgfPlusEValue;
                }

                if (ignorePeptideToProteinMapErrors)
                {
                    arguments += " /IgnorePepToProtMapErrors";
                }

                if (!string.IsNullOrWhiteSpace(outputFileBaseName))
                {
                    arguments += " /OutputFileBaseName:" + Global.ReplaceInvalidPathChars(outputFileBaseName);
                }

                // Starting in August 2024, the default connection string in PeptideHitResultsProcRunner.exe is "Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms"
                // Only add connection string argument /DB if the DMS connection string is not prismdb2
                var dmsConnectionString = mMgrParams.GetParam("ConnectionString");

                if (dmsConnectionString.IndexOf("prismdb2", StringComparison.OrdinalIgnoreCase) < 0)
                {
                    arguments += string.Format(@" /DB:""{0}""", dmsConnectionString);
                }

                if (mDebugLevel >= 1)
                {
                    OnDebugEvent(progLoc + " " + arguments);
                }

                var cmdRunner = new RunDosProgram(psmResultsFile.Directory.FullName, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = mPHRPConsoleOutputFilePath
                };
                RegisterEvents(cmdRunner);
                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                // Abort PHRP if it runs for over 720 minutes (this generally indicates that it's stuck)
                const int maxRuntimeSeconds = 720 * 60;
                var success = cmdRunner.RunProgram(progLoc, arguments, "PHRP", true, maxRuntimeSeconds);

                if (!success)
                {
                    ReportError("Error running PHRP");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (cmdRunner.ExitCode != 0)
                {
                    ReportError("PHRP runner returned a non-zero error code: " + cmdRunner.ExitCode);

                    // Parse the console output file for any lines that contain "Error"
                    // Append them to mErrMsg

                    var errorMessageFound = ReadErrorMessagesFromConsoleOutputFile(mPHRPConsoleOutputFilePath, out var errorMessage);

                    if (errorMessageFound)
                    {
                        mErrorMessage = Global.AppendToComment(mErrorMessage, errorMessage);
                    }
                    else
                    {
                        mErrorMessage = Global.AppendToComment(mErrorMessage, "Unknown PHRP error message");
                        OnWarningEvent("Unknown PHRP error message");
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Make sure the key PHRP result files were created
                var filesToCheck = new List<string>();

                string fileDescription;

                if (createFirstHitsFile && !createSynopsisFile)
                {
                    // We're processing Inspect data, and PHRP simply created the _fht.txt file
                    // Thus, only look for the first-hits file
                    filesToCheck.Add("_fht.txt");
                    fileDescription = "first hits";
                }
                else
                {
                    filesToCheck.Add(resultType == PeptideHitResultTypes.XTandem ? "_xt.txt" : "_syn.txt");

                    fileDescription = "synopsis";
                }

                // Check for an empty first hits or synopsis file
                var validationResult = ValidatePrimaryResultsFile(psmResultsFile, filesToCheck[0], fileDescription);

                if (validationResult != CloseOutType.CLOSEOUT_SUCCESS && validationResult != CloseOutType.CLOSEOUT_NO_DATA)
                    return validationResult;

                if (createSynopsisFile)
                {
                    // Add additional files to find
                    filesToCheck.Add("_ResultToSeqMap.txt");
                    filesToCheck.Add("_SeqInfo.txt");
                    filesToCheck.Add("_SeqToProteinMap.txt");
                    filesToCheck.Add("_ModSummary.txt");
                    filesToCheck.Add("_ModDetails.txt");

                    if (!skipProteinMods && validationResult != CloseOutType.CLOSEOUT_NO_DATA)
                    {
                        var lookForProteinModsFile = true;

                        if (ReadWarningMessagesFromConsoleOutputFile(mPHRPConsoleOutputFilePath, out var warningMessages))
                        {
                            // Look for warning "Skipping creation of the ProteinMods file"
                            foreach (var message in warningMessages)
                            {
                                if (message.IndexOf(PHRPBaseClass.WARNING_MESSAGE_SKIPPING_PROTEIN_MODS_FILE_CREATION, StringComparison.OrdinalIgnoreCase) >= 0)
                                {
                                    mWarningMessage = message;
                                    lookForProteinModsFile = false;
                                }
                            }
                        }

                        if (lookForProteinModsFile)
                        {
                            if (!string.IsNullOrWhiteSpace(fastaFilePath))
                            {
                                if (PHRPBaseClass.ValidateProteinFastaFile(fastaFilePath, out _, out _))
                                {
                                    filesToCheck.Add("_ProteinMods.txt");
                                }
                            }
                            else if (resultType == PeptideHitResultTypes.MSGFPlus)
                            {
                                filesToCheck.Add("_ProteinMods.txt");
                            }
                        }
                    }
                }

                foreach (var fileName in filesToCheck)
                {
                    if (psmResultsFile.Directory.GetFiles("*" + fileName).Length == 0)
                    {
                        ReportError("PHRP results file not found: " + fileName);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                // Skip the PHRP console output file since we didn't encounter any errors
                mJobParams.AddResultFileToSkip(mPHRPConsoleOutputFilePath);

                if (mDebugLevel >= 3)
                {
                    OnDebugEvent("Peptide hit results processor complete");
                }

                return validationResult;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception while running the peptide hit results processor: " + ex.Message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private readonly Regex mProcessingMatcher = new(@"Processing: (\d+)");
        private readonly Regex mProcessingPhrpMatcher = new(@"^([0-9.]+)\% complete");

        private void ParsePHRPConsoleOutputFile()
        {
            const int CREATING_FHT = 0;
            const int CREATING_SYN = 10;
            const int CREATING_PHRP_FILES = 20;
            const int PHRP_COMPLETE = 100;

            try
            {
                var currentTaskProgressAtStart = CREATING_FHT;
                var currentTaskProgressAtEnd = CREATING_SYN;

                float progressSubtask = 0;

                if (!File.Exists(mPHRPConsoleOutputFilePath)) return;

                using var reader = new StreamReader(new FileStream(mPHRPConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var lineIn = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(lineIn))
                    {
                        continue;
                    }

                    if (lineIn.StartsWith("Creating the FHT file"))
                    {
                        currentTaskProgressAtStart = CREATING_FHT;
                        currentTaskProgressAtEnd = CREATING_SYN;
                        progressSubtask = 0;
                    }
                    else if (lineIn.StartsWith("Creating the SYN file"))
                    {
                        currentTaskProgressAtStart = CREATING_SYN;
                        currentTaskProgressAtEnd = CREATING_PHRP_FILES;
                        progressSubtask = 0;
                    }
                    else if (lineIn.StartsWith("Creating the PHRP files"))
                    {
                        currentTaskProgressAtStart = CREATING_PHRP_FILES;
                        currentTaskProgressAtEnd = PHRP_COMPLETE;
                        progressSubtask = 0;
                    }

                    Match reMatch;

                    if (currentTaskProgressAtStart < CREATING_PHRP_FILES)
                    {
                        reMatch = mProcessingMatcher.Match(lineIn);

                        if (reMatch.Success)
                        {
                            float.TryParse(reMatch.Groups[1].Value, out progressSubtask);
                        }
                    }
                    else
                    {
                        reMatch = mProcessingPhrpMatcher.Match(lineIn);

                        if (reMatch.Success)
                        {
                            float.TryParse(reMatch.Groups[1].Value, out progressSubtask);
                        }
                    }
                }

                var progressOverall = AnalysisToolRunnerBase.ComputeIncrementalProgress(currentTaskProgressAtStart, currentTaskProgressAtEnd, progressSubtask);

                if (progressOverall > mProgress)
                {
                    mProgress = (int)progressOverall;
                    OnProgressUpdate("Running PHRP", mProgress);
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error parsing PHRP console output file", ex);
            }
        }

        private bool ReadErrorMessagesFromConsoleOutputFile(string phrpConsoleOutputFilePath, out string errorMessage)
        {
            var consoleOutputFile = new FileInfo(phrpConsoleOutputFilePath);

            if (!consoleOutputFile.Exists)
            {
                errorMessage = string.Empty;
                return false;
            }

            using var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            var errorMessageData = new StringBuilder();

            while (!reader.EndOfStream)
            {
                var lineIn = reader.ReadLine();

                if (string.IsNullOrWhiteSpace(lineIn))
                    continue;

                if (lineIn.IndexOf("error", StringComparison.OrdinalIgnoreCase) < 0)
                    continue;

                if (errorMessageData.Length > 0)
                    errorMessageData.Append("; ");

                errorMessageData.Append(lineIn);

                OnWarningEvent(lineIn);
            }

            if (errorMessageData.Length == 0)
            {
                errorMessage = string.Empty;
                return false;
            }

            errorMessage = errorMessageData.ToString();
            return true;
        }

        private bool ReadWarningMessagesFromConsoleOutputFile(string phrpConsoleOutputFilePath, out SortedSet<string> warningMessages)
        {
            warningMessages = new SortedSet<string>();

            var consoleOutputFile = new FileInfo(phrpConsoleOutputFilePath);

            if (!consoleOutputFile.Exists)
            {
                return false;
            }

            using var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            while (!reader.EndOfStream)
            {
                var lineIn = reader.ReadLine();

                if (string.IsNullOrWhiteSpace(lineIn))
                    continue;

                if (!lineIn.StartsWith("Warning:", StringComparison.OrdinalIgnoreCase))
                    continue;

                warningMessages.Add(lineIn);
            }

            return warningMessages.Count > 0;
        }

        private void ReportError(string message)
        {
            mErrorMessage = message;
            OnErrorEvent(mErrorMessage);
        }

        private CloseOutType ValidatePrimaryResultsFile(FileInfo psmResultsFile, string fileSuffix, string fileDescription)
        {
            if (psmResultsFile?.Directory == null)
            {
                OnWarningEvent("psmResultsFile.Directory is null; cannot validate existence of the " + fileSuffix + " file");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            var files = psmResultsFile.Directory.GetFiles("*" + fileSuffix).ToList();

            if (files.Count == 0)
            {
                ReportError("PHRP did not create a " + fileDescription + " file");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var synopsisFileHasData = AnalysisResources.ValidateFileHasData(files.First().FullName, "PHRP " + fileDescription, out var errorMessage);

            if (!synopsisFileHasData)
            {
                mErrorMessage = errorMessage;
                OnWarningEvent(errorMessage);
                return CloseOutType.CLOSEOUT_NO_DATA;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private DateTime mLastStatusUpdate = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            // Update the status by parsing the PHRP console output file every 20 seconds
            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds >= 20)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                ParsePHRPConsoleOutputFile();
            }
        }
    }
}
