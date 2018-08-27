//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerTopFDPlugIn
{
    /// <summary>
    /// Class for running TopFD analysis
    /// </summary>
    public class clsAnalysisToolRunnerTopFD : clsAnalysisToolRunnerBase
    {

        #region "Constants"

        /// <summary>
        /// .feature file created by TopFD
        /// </summary>
        /// <remarks>Tracks LC/MS features</remarks>
        public const string TOPFD_FEATURE_FILE_SUFFIX = ".feature";

        /// <summary>
        /// _ms2.msalign file created by TopFD
        /// </summary>
        private const string MSALIGN_FILE_SUFFIX = "_ms2.msalign";

        private const string TOPFD_CONSOLE_OUTPUT = "TopFD_ConsoleOutput.txt";
        private const string TOPFD_EXE_NAME = "topfd.exe";

        private const float PROGRESS_PCT_STARTING = 1;
        private const float PROGRESS_PCT_COMPLETE = 99;

        #endregion

        #region "Module Variables"

        private bool mToolVersionWritten;

        // Populate this with a tool version reported to the console
        private string mTopFDVersion;

        private string mTopFDProgLoc;
        private string mConsoleOutputErrorMsg;

        private readonly Regex reExtractPercentFinished = new Regex(@"(?<PercentComplete>\d+)% finished", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private DateTime mLastConsoleOutputParse;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs TopFD tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerTopFD.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to the TopFD program
                mTopFDProgLoc = DetermineProgramLocation("TopFDProgLoc", TOPFD_EXE_NAME);

                if (string.IsNullOrWhiteSpace(mTopFDProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the TopFD version info in the database after the first line is written to file TopFD_ConsoleOutput.txt
                mToolVersionWritten = false;
                mTopFDVersion = string.Empty;
                mConsoleOutputErrorMsg = string.Empty;

                var processingResult = StartTopFD(mTopFDProgLoc);

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                PRISM.clsProgRunner.GarbageCollectNow();

                // Trim the console output file to remove the majority of the % finished messages
                TrimConsoleOutputFile(Path.Combine(m_WorkDir, TOPFD_CONSOLE_OUTPUT));

                if (!clsAnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();
                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                return processingResult;

            }
            catch (Exception ex)
            {
                m_message = "Error in TopFDPlugin->RunTool: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Returns a dictionary mapping parameter names to argument names
        /// </summary>
        /// <returns></returns>
        private Dictionary<string, string> GetTopFDParameterNames()
        {
            var paramToArgMapping = new Dictionary<string, string>(25, StringComparer.OrdinalIgnoreCase)
            {
                {"MaxCharge", "max-charge"},
                {"MaxMass", "max-mass"},
                {"MzError", "mz-error"},
                {"SNRatio", "sn-ratio"},
                {"PrecursorWindow", "precursor-window"},
                {"MS1Missing", "missing-level-one"},
            };

            return paramToArgMapping;
        }

        /// <summary>
        /// Parse the TopFD console output file to determine the TopFD version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {

            // Example Console output (yes, TopFD misspells "running"):
            //
            // TopFD 1.1.2
            // Timestamp: Mon Aug 13 17:54:19 2018
            // ********************** Parameters **********************
            // Input file:                             QC_ShewIntact_1_2Aug18_HCD28_Aragorn_18-7-02.mzML
            // Data type:                              centroided
            // Maximum charge:                         30
            // Maximum monoisotopic mass:              100000 Dalton
            // Error tolerance:                        0.02 m/z
            // Signal/noise ratio:                     1
            // Precursor window size:                  3 m/z
            // ********************** Parameters **********************
            // Processing spectrum Scan_349...         3% finished.
            // Processing spectrum Scan_350...         3% finished.
            // Processing spectrum Scan_351...         3% finished.
            // Deconvolution finished.
            // Runing time: 51 seconds.
            // TopFD finished.

            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Parsing file " + consoleOutputFilePath);
                }

                short actualProgress = 0;

                mConsoleOutputErrorMsg = string.Empty;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var linesRead = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead += 1;

                        if (!string.IsNullOrWhiteSpace(dataLine))
                        {
                            if (linesRead <= 3)
                            {
                                // The first line has the TopFD executable name and the command line arguments
                                // The second line is dashes
                                // The third line has the TopFD version
                                if (string.IsNullOrEmpty(mTopFDVersion) &&
                                    dataLine.ToLower().StartsWith("topfd") &&
                                    !dataLine.ToLower().Contains(TOPFD_EXE_NAME.ToLower()))
                                {
                                    if (m_DebugLevel >= 2 && string.IsNullOrWhiteSpace(mTopFDVersion))
                                    {
                                        LogDebug("TopFD version: " + dataLine);
                                    }

                                    mTopFDVersion = string.Copy(dataLine);
                                }
                            }
                            else
                            {
                                // Update progress if the line starts with Processing spectrum
                                if (dataLine.StartsWith("Processing spectrum", StringComparison.OrdinalIgnoreCase))
                                {
                                    var match = reExtractPercentFinished.Match(dataLine);
                                    if (match.Success)
                                    {
                                        actualProgress = short.Parse(match.Groups["PercentComplete"].Value);
                                    }
                                }
                                else if (linesRead > 12 &&
                                         dataLine.ToLower().Contains("error") &&
                                         !dataLine.Contains("Error tolerance:") &&
                                         string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                {
                                    mConsoleOutputErrorMsg = "Error running TopFD: " + dataLine;
                                }
                            }
                        }
                    }
                }

                if (m_progress < actualProgress)
                {
                    m_progress = actualProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Read the TopFD options file and convert the options to command line switches
        /// </summary>
        /// <param name="cmdLineOptions">Output: TopFD command line arguments</param>
        /// <returns>Options string if success; empty string if an error</returns>
        /// <remarks></remarks>
        public CloseOutType ParseTopFDParameterFile(out string cmdLineOptions)
        {
            cmdLineOptions = string.Empty;

            var paramFileName = m_jobParams.GetParam("TopFD_ParamFile");

            // Although ParseKeyValueParameterFile checks for paramFileName being an empty string,
            // we check for it here since the name comes from the settings file, so we want to customize the error message
            if (string.IsNullOrWhiteSpace(paramFileName))
            {
                LogError("TopFD parameter file not defined in the job settings (param name TopFD_ParamFile)");
                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
            }

            var paramFileReader = new clsKeyValueParamFileReader("TopFD", m_WorkDir, paramFileName);
            RegisterEvents(paramFileReader);

            var eResult = paramFileReader.ParseKeyValueParameterFile(out var paramFileEntries);
            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                m_message = paramFileReader.ErrorMessage;
                return eResult;
            }

            // Obtain the dictionary that maps parameter names to argument names
            var paramToArgMapping = GetTopFDParameterNames();
            var paramNamesToSkip = new SortedSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "MS1Missing"
            };

            cmdLineOptions = paramFileReader.ConvertParamsToArgs(paramFileEntries, paramToArgMapping, paramNamesToSkip, "--");
            if (string.IsNullOrWhiteSpace(cmdLineOptions))
            {
                m_message = paramFileReader.ErrorMessage;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (paramFileReader.ParamIsEnabled(paramFileEntries, "MS1Missing"))
            {
                if (paramToArgMapping.TryGetValue("MS1Missing", out var argumentName))
                    cmdLineOptions += " --" + argumentName;
                else
                {
                    LogError("Parameter to argument mapping dictionary does not have MS1Missing");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType StartTopFD(string progLoc)
        {

            LogMessage("Running TopFD");

            var eResult = ParseTopFDParameterFile(out var cmdLineOptions);

            if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return eResult;
            }

            var cmdStr = cmdLineOptions +
                         Dataset + clsAnalysisResources.DOT_MZML_EXTENSION;

            LogDebug(progLoc + " " + cmdStr);

            mCmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, TOPFD_CONSOLE_OUTPUT);

            m_progress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var processingSuccess = mCmdRunner.RunProgram(progLoc, cmdStr, "TopFD", true);

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mTopFDVersion))
                {
                    ParseConsoleOutputFile(Path.Combine(m_WorkDir, TOPFD_CONSOLE_OUTPUT));
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!processingSuccess)
            {
                LogError("Error running TopFD");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("TopFD returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to TopFD failed (but exit code is 0)");
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make sure the output files were created and are not zero-bytes
            // If the input .mzML file only has MS spectra and no MS/MS spectra, the output files will be empty

            // Dictionary mapping a results file suffix to the full results file name
            var resultsFiles = new Dictionary<string, string>
            {
                {TOPFD_FEATURE_FILE_SUFFIX, m_Dataset + TOPFD_FEATURE_FILE_SUFFIX},
                {MSALIGN_FILE_SUFFIX, m_Dataset + MSALIGN_FILE_SUFFIX}
            };

            var validResultFiles = 0;

            foreach (var resultsFilePath in resultsFiles)
            {
                var resultsFile = new FileInfo(Path.Combine(m_WorkDir, resultsFilePath.Value));
                if (!resultsFile.Exists)
                {
                    LogError(string.Format("{0} file was not created by TopFD", resultsFilePath.Key));
                }
                else if (resultsFile.Length == 0)
                {
                    LogError(string.Format("{0} file created by TopFD is empty; " +
                                           "assure that the input .mzML file has MS/MS spectra", resultsFilePath.Key));
                }
                else
                {
                    validResultFiles++;
                }
            }

            if (validResultFiles < resultsFiles.Count)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            m_StatusTools.UpdateAndWrite(m_progress);
            if (m_DebugLevel >= 3)
            {
                LogDebug("TopFD analysis complete");
            }

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var toolVersionInfo = string.Copy(mTopFDVersion);

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new FileInfo(mTopFDProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        /// <summary>
        /// Reads the console output file and removes the majority of "Processing" messages
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void TrimConsoleOutputFile(string consoleOutputFilePath)
        {
            var reExtractScan = new Regex(@"Processing spectrum Scan_(?<Scan>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Trimming console output file at " + consoleOutputFilePath);
                }

                var mostRecentProgressLine = string.Empty;
                var mostRecentProgressLineWritten = string.Empty;

                var trimmedFilePath = consoleOutputFilePath + ".trimmed";

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(trimmedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var scanNumberOutputThreshold = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine(dataLine);
                            continue;
                        }

                        var keepLine = true;

                        var match = reExtractScan.Match(dataLine);
                        if (match.Success)
                        {
                            if (int.TryParse(match.Groups["Scan"].Value, out var scanNumber))
                            {
                                if (scanNumber < scanNumberOutputThreshold)
                                {
                                    keepLine = false;
                                }
                                else
                                {
                                    // Write out this line and bump up scanNumberOutputThreshold by 100
                                    scanNumberOutputThreshold += 100;
                                    mostRecentProgressLineWritten = string.Copy(dataLine);
                                }
                            }
                            mostRecentProgressLine = string.Copy(dataLine);
                        }
                        else if (dataLine.StartsWith("Deconvolution finished", StringComparison.OrdinalIgnoreCase))
                        {
                            // Possibly write out the most recent progress line
                            if (!clsGlobal.IsMatch(mostRecentProgressLine, mostRecentProgressLineWritten))
                            {
                                writer.WriteLine(mostRecentProgressLine);
                            }
                        }

                        if (keepLine)
                        {
                            writer.WriteLine(dataLine);
                        }
                    }
                }

                // Swap the files

                try
                {
                    File.Delete(consoleOutputFilePath);
                    File.Move(trimmedFilePath, consoleOutputFilePath);
                }
                catch (Exception ex)
                {
                    if (m_DebugLevel >= 1)
                    {
                        LogError("Error replacing original console output file (" + consoleOutputFilePath + ") with trimmed version", ex);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error trimming console output file (" + consoleOutputFilePath + ")", ex);
                }
            }
        }

        #endregion

        #region "Event Handlers"

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            if (!(DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE))
                return;

            mLastConsoleOutputParse = DateTime.UtcNow;

            ParseConsoleOutputFile(Path.Combine(m_WorkDir, TOPFD_CONSOLE_OUTPUT));

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mTopFDVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("TopFD");
        }

        #endregion
    }
}
