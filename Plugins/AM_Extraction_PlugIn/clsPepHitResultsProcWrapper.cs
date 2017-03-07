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

        private readonly int m_DebugLevel = 0;
        private readonly IMgrParams m_MgrParams;
        private readonly IJobParams m_JobParams;

        private int m_Progress = 0;
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
                else
                {
                    return m_ErrMsg;
                }
            }
        }

        #endregion

        #region "Events"

        public event ProgressChangedEventHandler ProgressChanged;

        public delegate void ProgressChangedEventHandler(string taskDescription, float percentComplete);

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="MgrParams">IMgrParams object containing manager settings</param>
        /// <param name="JobParams">IJobParams object containing job parameters</param>
        /// <remarks></remarks>
        public clsPepHitResultsProcWrapper(IMgrParams MgrParams, IJobParams JobParams)
        {
            m_MgrParams = MgrParams;
            m_JobParams = JobParams;
            m_DebugLevel = m_MgrParams.GetParam("debuglevel", 1);
        }

        /// <summary>
        /// Converts Sequest, X!Tandem, Inspect, MSGDB, or MSAlign output file to a flat file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType ExtractDataFromResults(string peptideSearchResultsFileName, string fastaFilePath, string resultType)
        {
            //  Let the DLL auto-determines the input filename, based on the dataset name
            return ExtractDataFromResults(peptideSearchResultsFileName, true, true, fastaFilePath, resultType);
        }

        /// <summary>
        /// Converts Sequest, X!Tandem, Inspect, MSGF+, MSAlign, MODa, or MODPlus output file to a flat file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType ExtractDataFromResults(string peptideSearchResultsFileName, bool createFirstHitsFile, bool createSynopsisFile,
            string fastaFilePath, string resultType)
        {
            string ModDefsFileName = null;
            string ParamFileName = m_JobParams.GetParam("ParmFileName");

            string cmdStr = null;
            bool blnSuccess = false;

            try
            {
                m_Progress = 0;
                m_ErrMsg = string.Empty;

                if (string.IsNullOrWhiteSpace(peptideSearchResultsFileName))
                {
                    m_ErrMsg = "PeptideSearchResultsFileName is empty; unable to continue";
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                // Define the modification definitions file name
                ModDefsFileName = Path.GetFileNameWithoutExtension(ParamFileName) + clsAnalysisResourcesExtraction.MOD_DEFS_FILE_SUFFIX;

                var ioInputFile = new FileInfo(peptideSearchResultsFileName);
                m_PHRPConsoleOutputFilePath = Path.Combine(ioInputFile.DirectoryName, "PHRPOutput.txt");

                string progLoc = m_MgrParams.GetParam("PHRPProgLoc");
                progLoc = Path.Combine(progLoc, "PeptideHitResultsProcRunner.exe");

                // Verify that program file exists
                if (!File.Exists(progLoc))
                {
                    m_ErrMsg = "PHRP not found at " + progLoc;
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Set up and execute a program runner to run the PHRP
                // Note:
                //   /SynPvalue is only used when processing Inspect files
                //   /SynProb is only used for MODa and MODPlus results
                cmdStr = ioInputFile.FullName + " /O:" + ioInputFile.DirectoryName + " /M:" + ModDefsFileName + " /T:" +
                         clsAnalysisResourcesExtraction.MASS_CORRECTION_TAGS_FILENAME + " /N:" + ParamFileName + " /SynPvalue:0.2 " +
                         " /SynProb:0.05 ";

                cmdStr += " /L:" + Path.Combine(ioInputFile.DirectoryName, PHRP_LOG_FILE_NAME);

                var blnSkipProteinMods = m_JobParams.GetJobParameter("SkipProteinMods", false);
                if (!blnSkipProteinMods)
                {
                    cmdStr += " /ProteinMods";
                }

                if (!string.IsNullOrEmpty(fastaFilePath))
                {
                    // Note that FastaFilePath will likely be empty if job parameter SkipProteinMods is true
                    cmdStr += " /F:" + clsAnalysisToolRunnerBase.PossiblyQuotePath(fastaFilePath);
                }

                // Note that PHRP assumes /InsFHT=True and /InsSyn=True by default
                // Thus, we only need to use these switches if either of these should be false
                if (!createFirstHitsFile || !createSynopsisFile)
                {
                    cmdStr += " /InsFHT:" + createFirstHitsFile.ToString();
                    cmdStr += " /InsSyn:" + createSynopsisFile.ToString();
                }

                // PHRP defaults to use /MSGFPlusSpecEValue:5E-7  and  /MSGFPlusEValue:0.75
                // Adjust these if defined in the job parameters
                var msgfPlusSpecEValue = m_JobParams.GetJobParameter("MSGFPlusSpecEValue", "");
                var msgfPlusEValue = m_JobParams.GetJobParameter("MSGFPlusEValue", "");

                if (!string.IsNullOrEmpty(msgfPlusSpecEValue))
                {
                    cmdStr += " /MSGFPlusSpecEValue:" + msgfPlusSpecEValue;
                }

                if (!string.IsNullOrEmpty(msgfPlusEValue))
                {
                    cmdStr += " /MSGFPlusEValue:" + msgfPlusEValue;
                }

                if (m_DebugLevel >= 1)
                {
                    OnDebugEvent(progLoc + " " + cmdStr);
                }

                var cmdRunner = new clsRunDosProgram(ioInputFile.DirectoryName)
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
                const int intMaxRuntimeSeconds = 720 * 60;
                blnSuccess = cmdRunner.RunProgram(progLoc, cmdStr, "PHRP", true, intMaxRuntimeSeconds);

                if (!blnSuccess)
                {
                    m_ErrMsg = "Error running PHRP";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (cmdRunner.ExitCode != 0)
                {
                    m_ErrMsg = "PHRP runner returned a non-zero error code: " + cmdRunner.ExitCode.ToString();

                    // Parse the console output file for any lines that contain "Error"
                    // Append them to m_ErrMsg

                    var ioConsoleOutputFile = new FileInfo(m_PHRPConsoleOutputFilePath);
                    var blnErrorMessageFound = false;

                    if (ioConsoleOutputFile.Exists)
                    {
                        var srInFile = new StreamReader(new FileStream(ioConsoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                        while (!srInFile.EndOfStream)
                        {
                            string strLineIn = null;
                            strLineIn = srInFile.ReadLine();
                            if (!string.IsNullOrWhiteSpace(strLineIn))
                            {
                                if (strLineIn.ToLower().Contains("error"))
                                {
                                    m_ErrMsg += "; " + m_ErrMsg;
                                    blnErrorMessageFound = true;
                                }
                            }
                        }
                        srInFile.Close();
                    }

                    if (!blnErrorMessageFound)
                    {
                        m_ErrMsg += "; Unknown error message";
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    // Make sure the key PHRP result files were created
                    var lstFilesToCheck = new List<string>();

                    if (createFirstHitsFile & !createSynopsisFile)
                    {
                        // We're processing Inspect data, and PHRP simply created the _fht.txt file
                        // Thus, only look for the first-hits file
                        lstFilesToCheck.Add("_fht.txt");
                    }
                    else
                    {
                        lstFilesToCheck.Add("_ResultToSeqMap.txt");
                        lstFilesToCheck.Add("_SeqInfo.txt");
                        lstFilesToCheck.Add("_SeqToProteinMap.txt");
                        lstFilesToCheck.Add("_ModSummary.txt");
                        lstFilesToCheck.Add("_ModDetails.txt");

                        if (!blnSkipProteinMods)
                        {
                            if (!string.IsNullOrEmpty(fastaFilePath))
                            {
                                string strWarningMessage = string.Empty;

                                if (PeptideHitResultsProcessor.clsPHRPBaseClass.ValidateProteinFastaFile(fastaFilePath, out strWarningMessage))
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

                    foreach (string strFileName in lstFilesToCheck)
                    {
                        if (ioInputFile.Directory.GetFiles("*" + strFileName).Length == 0)
                        {
                            m_ErrMsg = "PHRP results file not found: " + strFileName;
                            OnErrorEvent(m_ErrMsg);
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                }

                // Delete strPHRPConsoleOutputFilePath, since we didn't encounter any errors and the file is typically not useful
                try
                {
                    File.Delete(m_PHRPConsoleOutputFilePath);
                }
                catch (Exception)
                {
                    // Ignore errors here
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception while running the peptide hit results processor: " + ex.Message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (m_DebugLevel >= 3)
            {
                OnDebugEvent("Peptide hit results processor complete");
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
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

                if (File.Exists(m_PHRPConsoleOutputFilePath))
                {
                    using (var srInFile = new StreamReader(new FileStream(m_PHRPConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        while (!srInFile.EndOfStream)
                        {
                            var strLineIn = srInFile.ReadLine();
                            if (string.IsNullOrWhiteSpace(strLineIn))
                            {
                                continue;
                            }

                            if (strLineIn.StartsWith("Creating the FHT file"))
                            {
                                currentTaskProgressAtStart = CREATING_FHT;
                                currentTaskProgressAtEnd = CREATING_SYN;
                                progressSubtask = 0;
                            }
                            else if (strLineIn.StartsWith("Creating the SYN file"))
                            {
                                currentTaskProgressAtStart = CREATING_SYN;
                                currentTaskProgressAtEnd = CREATING_PHRP_FILES;
                                progressSubtask = 0;
                            }
                            else if (strLineIn.StartsWith("Creating the PHRP files"))
                            {
                                currentTaskProgressAtStart = CREATING_PHRP_FILES;
                                currentTaskProgressAtEnd = PHRP_COMPLETE;
                                progressSubtask = 0;
                            }

                            Match reMatch;
                            if (currentTaskProgressAtStart < CREATING_PHRP_FILES)
                            {
                                reMatch = reProcessing.Match(strLineIn);
                                if (reMatch.Success)
                                {
                                    float.TryParse(reMatch.Groups[1].Value, out progressSubtask);
                                }
                            }
                            else
                            {
                                reMatch = reProcessingPHRP.Match(strLineIn);
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
                        if (ProgressChanged != null)
                        {
                            ProgressChanged("Running PHRP", m_Progress);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error parsing PHRP Console Output File", ex);
            }
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
            //Update the status by parsing the PHRP Console Output file every 20 seconds
            if (System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 20)
            {
                dtLastStatusUpdate = System.DateTime.UtcNow;
                ParsePHRPConsoleOutputFile();
            }
        }

        #endregion
    }
}
