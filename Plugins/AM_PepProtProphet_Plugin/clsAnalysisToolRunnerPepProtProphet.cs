//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 08/07/2018
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;

namespace AnalysisManagerPepProtProphetPlugIn
{
    /// <summary>
    /// Class for running peptide prophet and protein prophet using Philosopher
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsAnalysisToolRunnerPepProtProphet : clsAnalysisToolRunnerBase
    {

        #region "Constants and Enums"

        private const string Philosopher_CONSOLE_OUTPUT = "Philosopher_ConsoleOutput.txt";
        private const string Philosopher_EXE_NAME = "philosopher_windows_amd64.exe";

        private const float PROGRESS_PCT_STARTING = 1;
        private const float PROGRESS_PCT_COMPLETE = 99;

        #endregion

        #region "Module Variables"

        private string mConsoleOutputFilePath;

        // Populate this with a tool version reported to the console
        private string mPhilosopherVersion;

        private string mPhilosopherProgLoc;
        private string mConsoleOutputErrorMsg;

        private DateTime mLastConsoleOutputParse;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs peptide and protein prophet using Philosopher
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

                if (mDebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerPepProtProphet.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to Philosopher
                mPhilosopherProgLoc = DetermineProgramLocation("PhilosopherProgLoc", Philosopher_EXE_NAME);

                if (string.IsNullOrWhiteSpace(mPhilosopherProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the Philosopher version info in the database after the first line is written to file Philosopher_ConsoleOutput.txt
                mPhilosopherVersion = string.Empty;
                mConsoleOutputErrorMsg = string.Empty;

                // Process the pepXML file using Philosopher
                var processingResult = StartPepProtProphet();

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                clsGlobal.IdleLoop(0.5);
                PRISM.ProgRunner.GarbageCollectNow();

                if (!clsAnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();
                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                return processingResult;

            }
            catch (Exception ex)
            {
                LogError("Error in PepProtProphetPlugin->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileToSkip(Dataset + clsAnalysisResources.DOT_MZML_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory();
        }

        private void ParseConsoleOutputFile()
        {
            const string BUILD_AND_VERSION = "Current Philosopher build and version";

            if (string.IsNullOrWhiteSpace(mConsoleOutputFilePath))
                return;

            // Example Console output
            //
            // INFO[18:17:06] Current Philosopher build and version         build=201904051529 version=20190405
            // WARN[18:17:08] There is a new version of Philosopher available for download: https://github.com/prvst/philosopher/releases

            // INFO[18:25:51] Executing Workspace 20190405
            // INFO[18:25:52] Creating workspace
            // INFO[18:25:52] Done

            var processingSteps = new SortedList<string, int>
            {
                {"Starting", 0},
                {"Current Philosopher build", 1},
                {"Executing Workspace", 2},
                {"Executing Database", 3},
                {"Executing PeptideProphet", 10},
                {"Executing ProteinProphet", 50},
                {"Computing degenerate peptides", 60},
                {"Computing probabilities", 70},
                {"Calculating sensitivity", 80},
                {"Executing Filter", 90},
                {"Executing Report", 95},
                {"Plotting mass distribution", 98},
            };

            // Peptide prophet iterations status:
            // Iterations: .........10.........20.....

            try
            {
                if (!File.Exists(mConsoleOutputFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + mConsoleOutputFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 4)
                {
                    LogDebug("Parsing file " + mConsoleOutputFilePath);
                }

                mConsoleOutputErrorMsg = string.Empty;
                var currentProgress = 0;

                using (var reader = new StreamReader(new FileStream(mConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var linesRead = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead += 1;

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (linesRead <= 5)
                        {
                            // The first line has the path to the Philosopher .exe file and the command line arguments
                            // The second line is dashes
                            // The third line will have the version when philosopher is run with the "version" switch

                            var versionTextStartIndex = dataLine.IndexOf(BUILD_AND_VERSION, StringComparison.OrdinalIgnoreCase);

                            if (string.IsNullOrEmpty(mPhilosopherVersion) &&
                                versionTextStartIndex >= 0)
                            {
                                if (mDebugLevel >= 2)
                                {
                                    LogDebug(dataLine);
                                }

                                mPhilosopherVersion = dataLine.Substring(versionTextStartIndex + BUILD_AND_VERSION.Length).Trim();
                            }
                        }
                        else
                        {
                            foreach (var processingStep in processingSteps)
                            {
                                if (dataLine.IndexOf(processingStep.Key, StringComparison.OrdinalIgnoreCase) < 0)
                                    continue;

                                currentProgress = processingStep.Value;
                            }

                            // Future:
                            /*
                            if (linesRead > 12 &&
                                dataLineLCase.Contains("error") &&
                                string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg = "Error running Philosopher: " + dataLine;
                            }
                            */
                        }
                    }
                }

                if (currentProgress > mProgress)
                {
                    mProgress = currentProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate("Error parsing console output file (" + mConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private CloseOutType StartPepProtProphet()
        {
            LogMessage("Running Philosopher");

            // Set up and execute a program runner to run Philosopher

            // We will call Philosopher several times
            // 1. Determine the Philosopher version
            // 2. Initialize the workspace
            // 3. Annotate the database (creates db.bin in the .meta subdirectory)
            // 4. Run Peptide Prophet
            // 5. Run Protein Prophet
            // 6. Filter results
            // 7. Generate the final report

            mProgress = PROGRESS_PCT_STARTING;
            ResetProgRunnerCpuUsage();

            mCmdRunner = new clsRunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = Path.Combine(mWorkDir, Philosopher_CONSOLE_OUTPUT)
            };
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mConsoleOutputFilePath = mCmdRunner.ConsoleOutputFilePath;

            var versionResult = GetPhilosopherVersion();
            if (versionResult != CloseOutType.CLOSEOUT_SUCCESS)
                return versionResult;

            var workspaceInitResult = InitializeWorkspace();
            if (workspaceInitResult != CloseOutType.CLOSEOUT_SUCCESS)
                return workspaceInitResult;

            var dbAnnotationResult = AnnotateDatabase(out var fastaFile);
            if (dbAnnotationResult != CloseOutType.CLOSEOUT_SUCCESS)
                return dbAnnotationResult;

            var peptideProphetResult = RunPeptideProphet(fastaFile, out var peptideProphetResults);
            if (peptideProphetResult != CloseOutType.CLOSEOUT_SUCCESS)
                return peptideProphetResult;

            var proteinProphetResult = RunProteinProphet(peptideProphetResults, out var proteinProphetResults);
            if (proteinProphetResult != CloseOutType.CLOSEOUT_SUCCESS)
                return proteinProphetResult;

            var filterResult = FilterResults(proteinProphetResults);
            if (filterResult != CloseOutType.CLOSEOUT_SUCCESS)
                return filterResult;

            var reportResult = GenerateFinalReport();
            if (reportResult != CloseOutType.CLOSEOUT_SUCCESS)
                return reportResult;

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType GetPhilosopherVersion()
        {
            try
            {
                var arguments = "version";

                LogDebug(mPhilosopherProgLoc + " " + arguments);

                var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

                HandlePhilosopherError(processingSuccess, mCmdRunner.ExitCode);
                if (!processingSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                ParseConsoleOutputFile();

                if (string.IsNullOrWhiteSpace(mPhilosopherVersion))
                {
                    LogError("Unable to determine the Philosopher version");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var toolVersionWritten = StoreToolVersionInfo();

                return toolVersionWritten ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error in PepProtProphetPlugIn->GetPhilosopherVersion", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType InitializeWorkspace()
        {
            try
            {
                var arguments = "workspace --init";

                LogDebug(mPhilosopherProgLoc + " " + arguments);

                var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

                HandlePhilosopherError(processingSuccess, mCmdRunner.ExitCode);
                if (!processingSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                ParseConsoleOutputFile();
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in PepProtProphetPlugIn->InitializeWorkspace", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType AnnotateDatabase(out FileInfo fastaFile)
        {
            try
            {
                // Define the path to the fasta file
                var localOrgDbFolder = mMgrParams.GetParam(clsAnalysisResources.MGR_PARAM_ORG_DB_DIR);

                // Note that job parameter "generatedFastaName" gets defined by clsAnalysisResources.RetrieveOrgDB
                var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam("PeptideSearch", clsAnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

                fastaFile = new FileInfo(fastaFilePath);

                var arguments = "database" +
                                " --annotate " + fastaFile.FullName +
                                " --prefix XXX_";

                LogDebug(mPhilosopherProgLoc + " " + arguments);

                var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

                HandlePhilosopherError(processingSuccess, mCmdRunner.ExitCode);
                if (!processingSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                ParseConsoleOutputFile();
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in PepProtProphetPlugIn->AnnotateDatabase", ex);
                fastaFile = null;
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }


        private CloseOutType RunPeptideProphet(FileSystemInfo fastaFile, out FileInfo peptideProphetResults)
        {
            peptideProphetResults = null;

            try
            {
                var pepXmlFile = new FileInfo(Path.Combine(mWorkDir, Dataset + ".pepXML"));
                if (!pepXmlFile.Exists)
                {
                    LogError("PepXML file not found: " + pepXmlFile.Name);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // ReSharper disable StringLiteralTypo
                var arguments = "peptideprophet " +
                                "--ppm " +
                                "--accmass " +
                                "--nonparam " +
                                "--expectscore " +
                                "--decoyprobs " +
                                "--decoy XXX_ " +
                                "--database " + fastaFile.FullName +
                                " " + pepXmlFile.FullName;
                // ReSharper restore StringLiteralTypo

                LogDebug(mPhilosopherProgLoc + " " + arguments);

                // Start the program and wait for it to finish
                // However, while it's running, LoopWaiting will get called via events
                var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

                HandlePhilosopherError(processingSuccess, mCmdRunner.ExitCode);
                if (!processingSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                ParseConsoleOutputFile();

                peptideProphetResults = new FileInfo(Path.Combine(mWorkDir, "interact-" + Dataset + ".pep.xml"));
                if (!peptideProphetResults.Exists)
                {
                    LogError("Peptide prophet results file not found: " + pepXmlFile.Name);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in PepProtProphetPlugIn->RunPeptideProphet", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType RunProteinProphet(FileSystemInfo peptideProphetResults, out FileInfo proteinProphetResults)
        {
            proteinProphetResults = null;

            try
            {
                // ReSharper disable StringLiteralTypo
                var arguments = "proteinprophet" +
                                " --maxppmdiff 2000000" +
                                " " + peptideProphetResults.FullName;
                // ReSharper restore StringLiteralTypo

                LogDebug(mPhilosopherProgLoc + " " + arguments);

                var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

                HandlePhilosopherError(processingSuccess, mCmdRunner.ExitCode);
                if (!processingSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                ParseConsoleOutputFile();

                proteinProphetResults = new FileInfo(Path.Combine(mWorkDir, "interact.prot.xml"));
                if (!proteinProphetResults.Exists)
                {
                    LogError("Protein prophet results file not found: " + proteinProphetResults.Name);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in PepProtProphetPlugIn->RunProteinProphet", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType FilterResults(FileSystemInfo proteinProphetResults)
        {
            try
            {
                // ReSharper disable StringLiteralTypo
                var arguments = "filter" +
                                " --sequential" +
                                " --razor" +
                                " --mapmods" +
                                " --prot 0.01" +
                                " --tag XXX_" +
                                " --pepxml " + mWorkDir + "" +
                                " --protxml " + proteinProphetResults.FullName;
                // ReSharper restore StringLiteralTypo

                LogDebug(mPhilosopherProgLoc + " " + arguments);

                var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

                HandlePhilosopherError(processingSuccess, mCmdRunner.ExitCode);
                if (!processingSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                ParseConsoleOutputFile();
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in PepProtProphetPlugIn->FilterResults", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType GenerateFinalReport()
        {
            try
            {
                var arguments = "report";

                LogDebug(mPhilosopherProgLoc + " " + arguments);

                var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

                HandlePhilosopherError(processingSuccess, mCmdRunner.ExitCode);
                if (!processingSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                ParseConsoleOutputFile();
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in PepProtProphetPlugIn->GenerateFinalReport", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void HandlePhilosopherError(bool processingSuccess, int exitCode)
        {

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (processingSuccess)
                return;

            LogError("Error running Philosopher");

            if (exitCode != 0)
            {
                LogWarning("Philosopher returned a non-zero exit code: " + exitCode);
            }
            else
            {
                LogWarning("Call to Philosopher failed (but exit code is 0)");
            }
        }

        private bool StoreToolVersionInfo()
        {
            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var toolVersionInfo = string.Copy(mPhilosopherVersion);

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new FileInfo(mPhilosopherProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
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

            ParseConsoleOutputFile();

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("Philosopher");
        }

        #endregion
    }
}
