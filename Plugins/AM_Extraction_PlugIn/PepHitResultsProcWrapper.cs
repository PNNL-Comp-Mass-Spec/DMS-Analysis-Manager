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
using System.Text.RegularExpressions;

namespace AnalysisManagerExtractionPlugin
{
    /// <summary>
    /// Calls PeptideHitResultsProcRunner.exe
    /// </summary>
    public class PepHitResultsProcWrapper : EventNotifier
    {
        // Ignore Spelling: MODa, Parm, Pvalue, PepToProt

        #region "Constants"

        public const string PHRP_LOG_FILE_NAME = "PHRP_LogFile.txt";

        #endregion

        #region "Module Variables"

        private readonly int mDebugLevel;
        private readonly IMgrParams mMgrParams;
        private readonly IJobParams mJobParams;

        private int mProgress;
        private string mErrMsg = string.Empty;
        private string mPHRPConsoleOutputFilePath;

        #endregion

        #region "Properties"

        public string ErrMsg => mErrMsg ?? string.Empty;

        #endregion

        #region "Methods"

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
        /// Converts SEQUEST, X!Tandem, Inspect, MS-GF+, MSAlign, MODa, or MODPlus output file to a flat file
        /// </summary>
        /// <param name="peptideSearchResultsFileName"></param>
        /// <param name="fastaFilePath"></param>
        /// <param name="resultType"></param>
        /// <returns>Enum indicating success or failure</returns>
        public CloseOutType ExtractDataFromResults(
            string peptideSearchResultsFileName,
            string fastaFilePath,
            string resultType)
        {
            //  Let the DLL auto-determines the input filename, based on the dataset name
            return ExtractDataFromResults(peptideSearchResultsFileName, true, true, fastaFilePath, resultType);
        }

        /// <summary>
        /// Converts SEQUEST, X!Tandem, Inspect, MS-GF+, MSAlign, MODa, or MODPlus output file to a flat file
        /// </summary>
        /// <param name="peptideSearchResultsFileName"></param>
        /// <param name="createFirstHitsFile"></param>
        /// <param name="createSynopsisFile"></param>
        /// <param name="fastaFilePath"></param>
        /// <param name="resultType"></param>
        /// <returns>Enum indicating success or failure</returns>
        public CloseOutType ExtractDataFromResults(
            string peptideSearchResultsFileName,
            bool createFirstHitsFile,
            bool createSynopsisFile,
            string fastaFilePath,
            string resultType)
        {
            var paramFileName = mJobParams.GetParam("ParmFileName");

            try
            {
                if (!createFirstHitsFile && !createSynopsisFile)
                {
                    ReportError("Must create either a first hits file or a synopsis file (or both); cannot run PHRP");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                mProgress = 0;
                mErrMsg = string.Empty;

                if (string.IsNullOrWhiteSpace(peptideSearchResultsFileName))
                {
                    ReportError("peptideSearchResultsFileName is empty; unable to run PHRP");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Define the modification definitions file name
                var modDefsFileName = Path.GetFileNameWithoutExtension(paramFileName) + AnalysisResourcesExtraction.MOD_DEFS_FILE_SUFFIX;

                var psmResultsFile = new FileInfo(peptideSearchResultsFileName);
                if (psmResultsFile.Directory == null)
                {
                    ReportError("Unable to determine the parent directory of PeptideSearchResultsFileName: " + peptideSearchResultsFileName);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

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
                var arguments = psmResultsFile.FullName +
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

                    var consoleOutputFile = new FileInfo(mPHRPConsoleOutputFilePath);
                    var errorMessageFound = false;

                    if (consoleOutputFile.Exists)
                    {
                        using var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                        while (!reader.EndOfStream)
                        {
                            var lineIn = reader.ReadLine();
                            if (string.IsNullOrWhiteSpace(lineIn))
                                continue;

                            if (lineIn.IndexOf("error", StringComparison.OrdinalIgnoreCase) < 0)
                                continue;

                            mErrMsg += "; " + lineIn;
                            OnWarningEvent(lineIn);

                            errorMessageFound = true;
                        }
                    }

                    if (!errorMessageFound)
                    {
                        mErrMsg += "; Unknown error message";
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
                    filesToCheck.Add("_syn.txt");
                    fileDescription = "synopsis";
                }

                // Check for an empty first hits or synopsis file
                var validationResult = ValidatePrimaryResultsFile(psmResultsFile, filesToCheck.First(), fileDescription);

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
                        if (!string.IsNullOrWhiteSpace(fastaFilePath))
                        {
                            if (PeptideHitResultsProcessor.clsPHRPBaseClass.ValidateProteinFastaFile(fastaFilePath, out _))
                            {
                                filesToCheck.Add("_ProteinMods.txt");
                            }
                        }
                        else if (resultType == AnalysisResources.RESULT_TYPE_MSGFPLUS)
                        {
                            filesToCheck.Add("_ProteinMods.txt");
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

                // Delete the PHRP console output file, since we didn't encounter any errors and the file is typically not useful
                try
                {
                    File.Delete(mPHRPConsoleOutputFilePath);
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

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

        private readonly Regex reProcessing = new(@"Processing: (\d+)");
        private readonly Regex reProcessingPHRP = new(@"^([0-9.]+)\% complete");

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

                using (var reader = new StreamReader(new FileStream(mPHRPConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
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
                            reMatch = reProcessing.Match(lineIn);
                            if (reMatch.Success)
                            {
                                float.TryParse(reMatch.Groups[1].Value, out progressSubtask);
                            }
                        }
                        else
                        {
                            reMatch = reProcessingPHRP.Match(lineIn);
                            if (reMatch.Success)
                            {
                                float.TryParse(reMatch.Groups[1].Value, out progressSubtask);
                            }
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

        private void ReportError(string message)
        {
            mErrMsg = message;
            OnErrorEvent(mErrMsg);
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

            var synopsisFileHasData = AnalysisResources.ValidateFileHasData(files.First().FullName, "PHRP " + fileDescription + " file", out var errorMessage);
            if (!synopsisFileHasData)
            {
                mErrMsg = errorMessage;
                OnWarningEvent(errorMessage);
                return CloseOutType.CLOSEOUT_NO_DATA;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        #endregion

        #region "Event Handlers"

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

        #endregion
    }
}
