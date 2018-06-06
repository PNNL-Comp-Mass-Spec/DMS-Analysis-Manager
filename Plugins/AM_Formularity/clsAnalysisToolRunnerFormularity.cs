using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerFormularityPlugin
{
    /// <summary>
    /// Class for running Formularity
    /// </summary>
    public class clsAnalysisToolRunnerFormularity : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        protected const float PROGRESS_PCT_STARTING = 5;
        protected const float PROGRESS_PCT_COMPLETE = 99;

        protected const string FORMULARITY_CONSOLE_OUTPUT_FILE = "Formularity_ConsoleOutput.txt";

        #endregion

        #region "Module Variables"

        protected string mConsoleOutputFile;
        protected string mConsoleOutputErrorMsg;

        protected DateTime mLastConsoleOutputParse;
        protected DateTime mLastProgressWriteTime;

        #endregion

        #region "Methods"

        /// <summary>
        /// Processes data using Formularity
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
                    LogDebug("clsAnalysisToolRunnerFormularity.RunTool(): Enter");
                }

                // Initialize classwide variables
                mLastConsoleOutputParse = DateTime.UtcNow;
                mLastProgressWriteTime = DateTime.UtcNow;

                // Determine the path to Formularity
                var progLoc = DetermineProgramLocation("FormularityProgLoc", "CIA.exe");

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the Formularity.exe version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining Formularity version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Unzip the XML files
                var compressedXMLFiles = Path.Combine(m_WorkDir, m_Dataset + "_scans.zip");
                var unzipSuccess = UnzipFile(compressedXMLFiles, m_WorkDir);
                if (!unzipSuccess)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Unknown error extracting the XML spectra files";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Process the XML files using Formularity
                var processingSuccess = ProcessScansWithFormularity(progLoc, out var nothingToAlign);

                CloseOutType eReturnCode;

                if (nothingToAlign)
                {
                    eReturnCode = CloseOutType.CLOSEOUT_NO_DATA;
                }
                else if (!processingSuccess)
                {
                    eReturnCode = CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    // Look for the result files
                    eReturnCode = PostProcessResults(ref processingSuccess);
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Could use the following to create a summary file:
                // Add the current job data to the summary file
                // UpdateSummaryFile();

                // Make sure objects are released
                PRISM.clsProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // No need to keep the JobParameters file
                m_jobParams.AddResultFileToSkip("JobParameters_" + m_JobNum + ".xml");

                var success = CopyResultsToTransferDirectory();

                return success ? eReturnCode : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Error in FormularityPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveFolder()
        {

            try
            {
                var diWorkDir = new DirectoryInfo(m_WorkDir);

                foreach (var xmlFile in GetXmlSpectraFiles(diWorkDir, out _))
                    xmlFile.Delete();

            }
            catch (Exception)
            {
                // Ignore errors here
            }

            base.CopyFailedResultsToArchiveFolder();
        }

        private List<FileInfo> GetXmlSpectraFiles(DirectoryInfo diWorkDir, out string wildcardMatchSpec)
        {
            wildcardMatchSpec = m_Dataset + "_scan*.xml";
            var fiSpectraFiles = diWorkDir.GetFiles(wildcardMatchSpec).ToList();
            return fiSpectraFiles;
        }

        /// <summary>
        /// Parse the Formularity console output file to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <remarks>Not used at present</remarks>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            ParseConsoleOutputFile(consoleOutputFilePath, out _, out _, out _);
        }

        /// <summary>
        /// Parse the Formularity console output file to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        /// <param name="fileCountNoPeaks">Output: will be non-zero if Formularity reports "no data points found" for a given file</param>
        /// <param name="nothingToAlign">Output: set to true if Formularity reports "Nothing to align" (meaning non of the input files had peaks)</param>
        /// <param name="calibrationFailed">Output: set to true if calibration failed</param>
        /// <remarks>Not used at present</remarks>
        private void ParseConsoleOutputFile(string consoleOutputFilePath, out int fileCountNoPeaks, out bool nothingToAlign, out bool calibrationFailed)
        {
            // Example Console output
            //
            // Started.
            // Checked arguments.
            // Loaded parameters.
            // Reading database ..\..\..\..\Data\CIA_DB\PNNL_CIA_DB_1500_B.bin
            // Sorting 28,487,622 DB entries
            // Skipping check for duplicate formulas; database was previously validated
            // Loaded DB.
            // Opening F:\Documents\Projects\NikolaTolic\Formularity\Data\TestDataXML\Marco_AL1_Bot_23May18_p05_000001_scan1.xml
            // Opening F:\Documents\Projects\NikolaTolic\Formularity\Data\TestDataXML\Marco_AL3_Bot_23May18_p05_000001_scan1.xml
            // Aligning
            // Formula Finding
            // Writing results to F:\Documents\Projects\NikolaTolic\Formularity\Data\TestDataXML\Report.csv
            // Finished.

            fileCountNoPeaks = 0;
            nothingToAlign = false;
            calibrationFailed = false;

            try
            {

                var reErrorMessage = new Regex(@"Error:(?<ErrorMessage>.+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            continue;
                        }

                        // Check for "Warning: no data points found in FileName"
                        if (dataLine.StartsWith("Warning", StringComparison.OrdinalIgnoreCase) && dataLine.ToLower().Contains("no data points found"))
                        {
                            fileCountNoPeaks++;
                            continue;
                        }

                        // Check for "Error: Nothing to align; aborting"
                        if (dataLine.StartsWith("Error", StringComparison.OrdinalIgnoreCase) && dataLine.ToLower().Contains("nothing to align"))
                        {
                            nothingToAlign = true;
                            m_message = dataLine;
                            continue;
                        }

                        // Check for Calibration failed; using uncalibrated masses"
                        if (dataLine.StartsWith("Calibration failed", StringComparison.OrdinalIgnoreCase))
                        {
                            calibrationFailed = true;
                            continue;
                        }

                        // Look for generic errors
                        var reMatch = reErrorMessage.Match(dataLine);

                        if (reMatch.Success)
                        {
                            // Store this error message, plus any remaining console output lines
                            m_message = reMatch.Groups["ErrorMessage"].Value;
                            StoreConsoleErrorMessage(reader, dataLine);
                        }
                        else if (dataLine.ToLower().StartsWith("error "))
                        {
                            // Store this error message, plus any remaining console output lines
                            m_message = dataLine;
                            StoreConsoleErrorMessage(reader, dataLine);
                        }

                    }
                }


            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }

        }

        private CloseOutType PostProcessResults(ref bool processingSuccess)
        {

            try
            {
                var reportFile = new FileInfo(Path.Combine(m_WorkDir, "Report.csv"));

                if (!reportFile.Exists)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = string.Format("Formularity results not found ({0})", reportFile.Name);
                        processingSuccess = false;
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                else
                {
                    // Rename the report file to start with the dataset name
                    reportFile.MoveTo(Path.Combine(m_WorkDir, m_Dataset + "_Report.csv"));
                }

                var workDir = new DirectoryInfo(m_WorkDir);

                var logFiles = workDir.GetFiles("Report*.log");

                if (logFiles.Length > 0)
                {
                    // Rename the log file file to start with the dataset name
                    logFiles[0].MoveTo(Path.Combine(m_WorkDir, m_Dataset + "_Report.log"));
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error post processing results: " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }


        }
        private bool StartFormularity(
            string progLoc,
            string wildcardMatchSpec,
            string paramFilePath,
            string ciaDbPath,
            string calibrationPeaksFilePath,
            out int fileCountNoPeaks,
            out bool nothingToAlign)
        {

            // Set up and execute a program runner to run Formularity

            var cmdStr = " cia " +
                         PossiblyQuotePath(wildcardMatchSpec) + " " +
                         PossiblyQuotePath(paramFilePath) + " " +
                         PossiblyQuotePath(ciaDbPath);

            if (!string.IsNullOrWhiteSpace(calibrationPeaksFilePath))
            {
                cmdStr += " " + PossiblyQuotePath(calibrationPeaksFilePath);
            }

            if (m_DebugLevel >= 1)
            {
                LogDebug(progLoc + " " + cmdStr);
            }

            mConsoleOutputFile = Path.Combine(m_WorkDir, FORMULARITY_CONSOLE_OUTPUT_FILE);

            var cmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = mConsoleOutputFile
            };
            RegisterEvents(cmdRunner);

            cmdRunner.LoopWaiting += cmdRunner_LoopWaiting;

            var success = cmdRunner.RunProgram(progLoc, cmdStr, "Formularity", true);

            if (!cmdRunner.WriteConsoleOutputToFile && cmdRunner.CachedConsoleOutput.Length > 0)
            {
                // Write the console output to a text file
                clsGlobal.IdleLoop(0.25);

                using (var swConsoleOutputfile = new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swConsoleOutputfile.WriteLine(cmdRunner.CachedConsoleOutput);
                }

            }

            // Parse the console output file to look for errors
            ParseConsoleOutputFile(mConsoleOutputFile, out fileCountNoPeaks, out nothingToAlign, out var calibrationFailed);

            if (calibrationFailed)
            {
                m_message = "Calibration failed; used uncalibrated masses";
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (success)
            {
                return true;
            }

            if (cmdRunner.ExitCode != 0)
            {
                LogWarning("Formularity returned a non-zero exit code: " + cmdRunner.ExitCode);
            }
            else
            {
                LogWarning("Formularity failed (but exit code is 0)");
            }

            return false;
        }

        private bool ProcessScansWithFormularity(string progLoc, out bool nothingToAlign)
        {

            nothingToAlign = false;

            try
            {

                mConsoleOutputErrorMsg = string.Empty;

                LogMessage("Processing data using Formularity");

                var paramFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_PARAMETER_FILE));

                if (!File.Exists(paramFilePath))
                {
                    LogError("Parameter file not found", "Parameter file not found: " + paramFilePath);
                    return false;
                }

                var orgDbDirectory = m_mgrParams.GetParam(clsAnalysisResources.MGR_PARAM_ORG_DB_DIR);
                var ciaDbPath = Path.Combine(orgDbDirectory, m_jobParams.GetParam("cia_db_name"));

                if (!File.Exists(ciaDbPath))
                {
                    LogError("CIA database not found", "CIA database not found: " + ciaDbPath);
                    return false;
                }

                var diWorkDir = new DirectoryInfo(m_WorkDir);
                var spectraFiles = GetXmlSpectraFiles(diWorkDir, out var wildcardMatchSpec);

                if (spectraFiles.Count == 0)
                {
                    m_message = "XML spectrum files not found matching " + wildcardMatchSpec;
                    return false;
                }

                foreach (var spectrumFile in spectraFiles)
                {
                    m_jobParams.AddResultFileToSkip(spectrumFile.Name);
                }

                var calibrationPeaksFileName = m_jobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "CalibrationPeaksFile", string.Empty);
                string calibrationPeaksFilePath;
                if (string.IsNullOrWhiteSpace(calibrationPeaksFileName))
                {
                    calibrationPeaksFilePath = string.Empty;
                }
                else
                {
                    calibrationPeaksFilePath = Path.Combine(m_WorkDir, calibrationPeaksFileName);
                }

                m_progress = PROGRESS_PCT_STARTING;

                var success = StartFormularity(progLoc, wildcardMatchSpec, paramFilePath, ciaDbPath, calibrationPeaksFilePath,
                                               out var fileCountNoPeaks, out nothingToAlign);

                if (!success)
                    return false;

                m_progress = PROGRESS_PCT_COMPLETE;
                m_StatusTools.UpdateAndWrite(m_progress);
                if (m_DebugLevel >= 3)
                {
                    LogDebug("Formularity processing Complete");
                }

                if (fileCountNoPeaks <= 0 && nothingToAlign == false)
                {
                    return true;
                }

                if (nothingToAlign || fileCountNoPeaks >= spectraFiles.Count)
                {
                    // None of the scans had peaks
                    m_message = "No peaks found";
                    if (spectraFiles.Count > 1)
                        m_EvalMessage = "None of the scans had peaks";
                    else
                        m_EvalMessage = "Scan did not have peaks";

                    if (!nothingToAlign)
                        nothingToAlign = true;

                    // Do not put the parameter file in the results directory
                    m_jobParams.AddResultFileToSkip(paramFilePath);
                }
                else
                {
                    // Some of the scans had no peaks
                    m_EvalMessage = fileCountNoPeaks + " / " + spectraFiles.Count + " scans had no peaks";
                }

                return true;

            }
            catch (Exception ex)
            {
                m_message = "Error in FormularityPlugin->ProcessScansWithFormularity";
                LogError(m_message, ex);
                return false;
            }

        }

        private void StoreConsoleErrorMessage(StreamReader reader, string dataLine)
        {
            mConsoleOutputErrorMsg = "Error running Formularity: " + dataLine;

            while (!reader.EndOfStream)
            {
                // Store the remaining console output lines
                dataLine = reader.ReadLine();

                if (!string.IsNullOrWhiteSpace(dataLine))
                {
                    mConsoleOutputErrorMsg += "; " + dataLine;
                }

            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDlls = new List<string> {
                "ArrayMath.dll",
                "FindChains.exe",
                "TestFSDBSearch.exe"
            };

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDlls);

            return success;
        }

        #endregion

        #region "Event Handlers"

        void cmdRunner_LoopWaiting()
        {

            // Synchronize the stored Debug level with the value stored in the database

            {
                UpdateStatusFile();

                // Parse the console output file every 15 seconds
                if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
                {
                    mLastConsoleOutputParse = DateTime.UtcNow;

                    ParseConsoleOutputFile(Path.Combine(m_WorkDir, mConsoleOutputFile));

                    LogProgress("Formularity");
                }

            }

        }

        #endregion
    }
}
