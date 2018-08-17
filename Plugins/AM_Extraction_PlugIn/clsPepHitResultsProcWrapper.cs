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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerExtractionPlugin
{
    /// <summary>
    /// Calls PeptideHitResultsProcRunner.exe
    /// </summary>
    /// <remarks></remarks>
    public class clsPepHitResultsProcWrapper : clsEventNotifier
    {
        #region "Constants"

        public const string PHRP_LOG_FILE_NAME = "PHRP_LogFile.txt";

        #endregion

        #region "Module Variables"

        private readonly int m_DebugLevel;
        private readonly IMgrParams m_MgrParams;
        private readonly IJobParams m_JobParams;

        private int m_Progress;
        private string m_ErrMsg = string.Empty;
        private string m_PHRPConsoleOutputFilePath;

        #endregion

        #region "Properties"

        public string ErrMsg
        {
            get
            {
                if (m_ErrMsg == null)
                {
                    return string.Empty;
                }

                return m_ErrMsg;
            }
        }

        #endregion

        #region "Events"

        public delegate void ProgressChangedEventHandler(string taskDescription, float percentComplete);

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">IMgrParams object containing manager settings</param>
        /// <param name="jobParams">IJobParams object containing job parameters</param>
        /// <remarks></remarks>
        public clsPepHitResultsProcWrapper(IMgrParams mgrParams, IJobParams jobParams)
        {
            m_MgrParams = mgrParams;
            m_JobParams = jobParams;
            m_DebugLevel = m_MgrParams.GetParam("debuglevel", 1);
        }

        /// <summary>
        /// Converts Sequest, X!Tandem, Inspect, MSGDB, or MSAlign output file to a flat file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType ExtractDataFromResults(
            string peptideSearchResultsFileName,
            string fastaFilePath,
            string resultType)
        {
            //  Let the DLL auto-determines the input filename, based on the dataset name
            return ExtractDataFromResults(peptideSearchResultsFileName, true, true, fastaFilePath, resultType);
        }

        /// <summary>
        /// Converts Sequest, X!Tandem, Inspect, MSGF+, MSAlign, MODa, or MODPlus output file to a flat file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType ExtractDataFromResults(
            string peptideSearchResultsFileName,
            bool createFirstHitsFile,
            bool createSynopsisFile,
            string fastaFilePath,
            string resultType)
        {
            var paramFileName = m_JobParams.GetParam("ParmFileName");

            try
            {
                if (!createFirstHitsFile && !createSynopsisFile)
                {
                    ReportError("Must create either a first hits file or a synopsis file (or both); cannot run PHRP");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                m_Progress = 0;
                m_ErrMsg = string.Empty;

                if (string.IsNullOrWhiteSpace(peptideSearchResultsFileName))
                {
                    ReportError("peptideSearchResultsFileName is empty; unable to run PHRP");
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Define the modification definitions file name
                var modDefsFileName = Path.GetFileNameWithoutExtension(paramFileName) + clsAnalysisResourcesExtraction.MOD_DEFS_FILE_SUFFIX;

                var psmResultsFile = new FileInfo(peptideSearchResultsFileName);
                if (psmResultsFile.Directory == null)
                {
                    ReportError("Unable to determine the parent directory of PeptideSearchResultsFileName: " + peptideSearchResultsFileName);
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                m_PHRPConsoleOutputFilePath = Path.Combine(psmResultsFile.Directory.FullName, "PHRPOutput.txt");

                var progLoc = m_MgrParams.GetParam("PHRPProgLoc");
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
                var cmdStr = psmResultsFile.FullName + " /O:" + psmResultsFile.DirectoryName + " /M:" + modDefsFileName + " /T:" +
                                clsAnalysisResourcesExtraction.MASS_CORRECTION_TAGS_FILENAME + " /N:" + paramFileName + " /SynPvalue:0.2 " +
                                " /SynProb:0.05 ";

                cmdStr += " /L:" + Path.Combine(psmResultsFile.Directory.FullName, PHRP_LOG_FILE_NAME);

                var skipProteinMods = m_JobParams.GetJobParameter("SkipProteinMods", false);
                if (!skipProteinMods)
                {
                    cmdStr += " /ProteinMods";
                }

                if (!string.IsNullOrWhiteSpace(fastaFilePath))
                {
                    // Note that FastaFilePath will likely be empty if job parameter SkipProteinMods is true
                    cmdStr += " /F:" + clsAnalysisToolRunnerBase.PossiblyQuotePath(fastaFilePath);
                }

                // Note that PHRP assumes /InsFHT=True and /InsSyn=True by default
                // Thus, we only need to use these switches if either of these should be false
                if (!createFirstHitsFile || !createSynopsisFile)
                {
                    cmdStr += " /InsFHT:" + createFirstHitsFile;
                    cmdStr += " /InsSyn:" + createSynopsisFile;
                }

                // PHRP defaults to use /MSGFPlusSpecEValue:5E-7  and  /MSGFPlusEValue:0.75
                // Adjust these if defined in the job parameters
                var msgfPlusSpecEValue = m_JobParams.GetJobParameter("MSGFPlusSpecEValue", "");
                var msgfPlusEValue = m_JobParams.GetJobParameter("MSGFPlusEValue", "");

                if (!string.IsNullOrWhiteSpace(msgfPlusSpecEValue))
                {
                    cmdStr += " /MSGFPlusSpecEValue:" + msgfPlusSpecEValue;
                }

                if (!string.IsNullOrWhiteSpace(msgfPlusEValue))
                {
                    cmdStr += " /MSGFPlusEValue:" + msgfPlusEValue;
                }

                if (m_DebugLevel >= 1)
                {
                    OnDebugEvent(progLoc + " " + cmdStr);
                }

                var cmdRunner = new clsRunDosProgram(psmResultsFile.Directory.FullName, m_DebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = m_PHRPConsoleOutputFilePath
                };
                RegisterEvents(cmdRunner);
                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                // Abort PHRP if it runs for over 720 minutes (this generally indicates that it's stuck)
                const int maxRuntimeSeconds = 720 * 60;
                var success = cmdRunner.RunProgram(progLoc, cmdStr, "PHRP", true, maxRuntimeSeconds);

                if (!success)
                {
                    ReportError("Error running PHRP");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (cmdRunner.ExitCode != 0)
                {
                    ReportError("PHRP runner returned a non-zero error code: " + cmdRunner.ExitCode);

                    // Parse the console output file for any lines that contain "Error"
                    // Append them to m_ErrMsg

                    var consoleOutputFile = new FileInfo(m_PHRPConsoleOutputFilePath);
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

                                if (!lineIn.ToLower().Contains("error"))
                                    continue;

                                m_ErrMsg += "; " + lineIn;
                                OnWarningEvent(lineIn);

                                errorMessageFound = true;
                            }
                        }
                    }

                    if (!errorMessageFound)
                    {
                        m_ErrMsg += "; Unknown error message";
                        OnWarningEvent("Unknown PHRP error message");
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Make sure the key PHRP result files were created
                var lstFilesToCheck = new List<string>();

                string fileDescription;

                if (createFirstHitsFile && !createSynopsisFile)
                {
                    // We're processing Inspect data, and PHRP simply created the _fht.txt file
                    // Thus, only look for the first-hits file
                    lstFilesToCheck.Add("_fht.txt");
                    fileDescription = "first hits";
                }
                else
                {
                    lstFilesToCheck.Add("_syn.txt");
                    fileDescription = "synopsis";
                }

                // Check for an empty first hits or synopsis file
                var validationResult = ValidatePrimaryResultsFile(psmResultsFile, lstFilesToCheck.First(), fileDescription);

                if (validationResult != CloseOutType.CLOSEOUT_SUCCESS && validationResult != CloseOutType.CLOSEOUT_NO_DATA)
                    return validationResult;

                if (createSynopsisFile)
                {
                    // Add additional files to find
                    lstFilesToCheck.Add("_ResultToSeqMap.txt");
                    lstFilesToCheck.Add("_SeqInfo.txt");
                    lstFilesToCheck.Add("_SeqToProteinMap.txt");
                    lstFilesToCheck.Add("_ModSummary.txt");
                    lstFilesToCheck.Add("_ModDetails.txt");

                    if (!skipProteinMods && validationResult != CloseOutType.CLOSEOUT_NO_DATA)
                    {
                        if (!string.IsNullOrWhiteSpace(fastaFilePath))
                        {
                            if (PeptideHitResultsProcessor.clsPHRPBaseClass.ValidateProteinFastaFile(fastaFilePath, out _))
                            {
                                lstFilesToCheck.Add("_ProteinMods.txt");
                            }
                        }
                        else if (resultType == clsAnalysisResources.RESULT_TYPE_MSGFPLUS)
                        {
                            lstFilesToCheck.Add("_ProteinMods.txt");
                        }
                    }
                }

                foreach (var fileName in lstFilesToCheck)
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
                    File.Delete(m_PHRPConsoleOutputFilePath);
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                if (m_DebugLevel >= 3)
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

        private readonly Regex reProcessing = new Regex(@"Processing: (\d+)");
        private readonly Regex reProcessingPHRP = new Regex(@"^([0-9.]+)\% complete");

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

                if (!File.Exists(m_PHRPConsoleOutputFilePath)) return;

                using (var srInFile = new StreamReader(new FileStream(m_PHRPConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var lineIn = srInFile.ReadLine();
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

                var progressOverall = clsAnalysisToolRunnerBase.ComputeIncrementalProgress(currentTaskProgressAtStart, currentTaskProgressAtEnd, progressSubtask);

                if (progressOverall > m_Progress)
                {
                    m_Progress = (int)progressOverall;
                    OnProgressUpdate("Running PHRP", m_Progress);
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error parsing PHRP console output file", ex);
            }
        }

        private void ReportError(string message)
        {
            m_ErrMsg = message;
            OnErrorEvent(m_ErrMsg);
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

            var synopsisFileHasData = clsAnalysisResources.ValidateFileHasData(files.First().FullName, "PHRP " + fileDescription + " file", out var errorMessage);
            if (!synopsisFileHasData)
            {
                m_ErrMsg = errorMessage;
                OnWarningEvent(errorMessage);
                return CloseOutType.CLOSEOUT_NO_DATA;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        #endregion

        #region "Event Handlers"

        private DateTime dtLastStatusUpdate = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            // Update the status by parsing the PHRP console output file every 20 seconds
            if (DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 20)
            {
                dtLastStatusUpdate = DateTime.UtcNow;
                ParsePHRPConsoleOutputFile();
            }
        }

        #endregion
    }
}
