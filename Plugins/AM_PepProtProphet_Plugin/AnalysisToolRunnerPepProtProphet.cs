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
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerMSFraggerPlugIn;

namespace AnalysisManagerPepProtProphetPlugIn
{
    /// <summary>
    /// Class for running peptide prophet and protein prophet using Philosopher
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerPepProtProphet : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: accmass, acetylation, clevel, cp, decoyprobs, degen, dev, dir, expectscore
        // Ignore Spelling: Flammagenitus, fragpipe, freequant, Insilicos, mapmods, masswidth, maxppmdiff
        // Ignore Spelling: nc, nocheck, nonparam, peptideprophet, pepxml, plex, ppm, protxml, ptw, prot, tmt, tol
        // Ignore Spelling: \fragpipe, \tools

        // ReSharper restore CommentTypo

        private const string PHILOSOPHER_CONSOLE_OUTPUT = "Philosopher_ConsoleOutput.txt";
        private const string PHILOSOPHER_CONSOLE_OUTPUT_COMBINED = "Philosopher_ConsoleOutput_Combined.txt";

        // ReSharper disable IdentifierTypo

        private const string ABACUS_PROPHET_CONSOLE_OUTPUT = "Abacus_ConsoleOutput.txt";
        private const string FREEQUANT_PROPHET_CONSOLE_OUTPUT = "FreeQuant_ConsoleOutput.txt";
        private const string LABELQUANT_PROPHET_CONSOLE_OUTPUT = "LabelQuant_ConsoleOutput.txt";
        private const string PEPTIDE_PROPHET_CONSOLE_OUTPUT = "PeptideProphet_ConsoleOutput.txt";
        private const string PROTEIN_PROPHET_CONSOLE_OUTPUT = "ProteinProphet_ConsoleOutput.txt";

        // ReSharper restore IdentifierTypo

        private const string PEPXML_EXTENSION = ".pepXML";

        private const string PHILOSOPHER_RELATIVE_PATH = @"fragpipe\tools\philosopher\philosopher.exe";

        private const string TEMP_PEP_PROPHET_DIR_SUFFIX = ".pepXML-temp";

        private const string TMT_INTEGRATOR_JAR_RELATIVE_PATH = @"fragpipe\tools\tmt-integrator-2.4.0.jar";

        private const string UNDEFINED_EXPERIMENT_GROUP = "__UNDEFINED_EXPERIMENT_GROUP__";

        public const float PROGRESS_PCT_INITIALIZING = 1;

        private enum PhilosopherToolType
        {
            Undefined = 0,
            WorkspaceManager = 1,
            PeptideProphet = 2,
            ProteinProphet = 3,
            AnnotateDatabase = 4,
            ResultsFilter = 5,
            FreeQuant = 6,
            LabelQuant = 7,
            GenerateReport = 8
        }

        private enum ProgressPercentValues
        {
            Undefined = 0,
            Initializing = 1,
            ProcessingStarted = 2,
            CrystalCComplete = 5,
            PeptideProphetComplete = 15,
            ProteinProphetComplete = 30,
            DBAnnotationComplete = 45,
            ResultsFilterComplete = 60,
            LabelQuantComplete = 75,
            ReportGenerated = 85,
            AbacusComplete = 87,
            IonQuantComplete = 90,
            TmtIntegratorComplete = 95,
            PtmShepherdComplete = 97,
            ProcessingComplete = 99
        }

        private bool mToolVersionWritten;

        private string mFastaFilePath;

        // Populate this with a tool version reported to the console
        private string mPhilosopherVersion;
        private string mTmtIntegratorVersion;

        private string mPhilosopherProgLoc;
        private string mTmtIntegratorProgLoc;

        private string mConsoleOutputErrorMsg;

        private PhilosopherToolType mCurrentPhilosopherTool;

        private DateTime mLastConsoleOutputParse;

        private RunDosProgram mCmdRunner;

        /// <summary>
        /// Runs peptide and protein prophet using Philosopher
        /// Optionally also runs other post-processing tools
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                mProgress = (int)ProgressPercentValues.Initializing;

                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerPepProtProphet.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;
                mFastaFilePath = string.Empty;

                // Determine the path to Philosopher
                mPhilosopherProgLoc = DetermineProgramLocation("MSFraggerProgLoc", PHILOSOPHER_RELATIVE_PATH);

                mTmtIntegratorProgLoc = DetermineProgramLocation("MSFraggerProgLoc", TMT_INTEGRATOR_JAR_RELATIVE_PATH);

                if (string.IsNullOrWhiteSpace(mPhilosopherProgLoc) || string.IsNullOrWhiteSpace(mTmtIntegratorProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the Philosopher version info in the database after the first line is written to file Philosopher_ConsoleOutput.txt
                mPhilosopherVersion = string.Empty;
                mTmtIntegratorVersion = string.Empty;

                mConsoleOutputErrorMsg = string.Empty;

                mCurrentPhilosopherTool = PhilosopherToolType.Undefined;

                if (!ValidateFastaFile())
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Process the pepXML files using Philosopher
                var processingResult = ExecuteWorkflow();

                mProgress = (int)ProgressPercentValues.ProcessingComplete;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                Global.IdleLoop(0.5);
                PRISM.ProgRunner.GarbageCollectNow();

                if (!AnalysisJob.SuccessOrNoData(processingResult))
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

        private CloseOutType ExecuteWorkflow()
        {
            try
            {
                var paramFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);
                var paramFilePath = Path.Combine(mWorkDir, paramFileName);

                var moveFilesSuccess = OrganizePepXmlFiles(
                    out var dataPackageInfo,
                    out var datasetIDsByExperiment,
                    out var experimentWorkingDirectories);

                if (moveFilesSuccess != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return moveFilesSuccess;
                }

                var datasetCount = datasetIDsByExperiment.Sum(item => item.Value.Count);

                var success = LoadMSFraggerOptions(datasetCount, paramFilePath, out var options);

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(mWorkDir, PHILOSOPHER_CONSOLE_OUTPUT)
                };
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                mProgress = (int)ProgressPercentValues.ProcessingStarted;

                if (options.OpenSearch)
                {
                    var crystalCSuccess = RunCrystalC(datasetIDsByExperiment);
                    if (!crystalCSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.CrystalCComplete;
                }

                // Run Peptide Prophet
                var peptideProphetSuccess = RunPeptideProphet(dataPackageInfo, datasetIDsByExperiment, experimentWorkingDirectories, options, out var peptideProphetPepXmlFiles);
                if (!peptideProphetSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.PeptideProphetComplete;

                // Run Protein Prophet
                var proteinProphetSuccess = RunProteinProphet(peptideProphetPepXmlFiles);
                if (!proteinProphetSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.ProteinProphetComplete;

                var dbAnnotateSuccess = RunDatabaseAnnotation(experimentWorkingDirectories);
                if (!dbAnnotateSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.DBAnnotationComplete;

                var filterSuccess = RunResultsFilter(experimentWorkingDirectories, options);
                if (!filterSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.ResultsFilterComplete;

                if (options.RunFreeQuant && !options.RunIonQuant)
                {
                    var freeQuantSuccess = RunFreeQuant();
                    if (!freeQuantSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                if (options.ReporterIonMode != ReporterIonModes.Disabled)
                {
                    var labelQuantSuccess = RunLabelQuant(options.ReporterIonMode);
                    if (!labelQuantSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.LabelQuantComplete;
                }

                var reportSuccess = RunReportGeneration();
                if (!reportSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.ReportGenerated;

                if (datasetCount > 1 && options.RunAbacus)
                {
                    var abacusSuccess = RunAbacus();
                    if (!abacusSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.AbacusComplete;
                }

                if (options.RunIonQuant)
                {
                    var ionQuantSuccess = RunIonQuant();
                    if (!ionQuantSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.IonQuantComplete;
                }

                if (options.ReporterIonMode != ReporterIonModes.Disabled)
                {
                    var tmtIntegratorSuccess = RunTmtIntegrator(options.ReporterIonMode);
                    if (!tmtIntegratorSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.TmtIntegratorComplete;
                }

                if (options.OpenSearch && options.RunPTMShepherd)
                {
                    var ptmShepherdSuccess = RunPTMShepherd();
                    if (!ptmShepherdSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.PtmShepherdComplete;
                }

                var zipSuccess = ZipPepXmlFiles(dataPackageInfo);

                return zipSuccess ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error in PostProcessMSFraggerResults", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileToSkip(Dataset + AnalysisResources.DOT_MZML_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory();
        }

        /// <summary>
        /// Delete temporary directories, ignoring errors
        /// </summary>
        /// <param name="directoriesToDelete"></param>
        private void DeleteTempDirectories(IEnumerable<DirectoryInfo> directoriesToDelete)
        {
            // Delete the workspace directories
            foreach (var directory in directoriesToDelete)
            {
                try
                {
                    directory.Delete();
                }
                catch (Exception ex)
                {
                    LogWarning(string.Format("Error deleting directory {0}: {1}", directory.FullName, ex.Message));
                }
            }
        }

        /// <summary>
        /// Examine the dynamic and static mods loaded from a MSFragger parameter file to determine the reporter ion mode
        /// </summary>
        /// <param name="paramFileEntries"></param>
        /// <param name="reporterIonMode"></param>
        /// <returns>True if success, false if an error</returns>
        private bool DetermineReporterIonMode(IEnumerable<KeyValuePair<string, string>> paramFileEntries, out ReporterIonModes reporterIonMode)
        {
            reporterIonMode = ReporterIonModes.Disabled;

            try
            {
                var staticNTermModMass = 0.0;
                var staticLysineModMass = 0.0;

                // Keys in this dictionary are modification masses; values are a list of the affected residues
                var variableModMasses = new Dictionary<double, List<string>>();

                foreach (var parameter in paramFileEntries)
                {
                    // ReSharper disable once StringLiteralTypo
                    if (parameter.Key.Equals("add_Nterm_peptide"))
                    {
                        if (!ParseModMass(parameter, out staticNTermModMass, out _))
                            return false;

                        continue;
                    }

                    if (parameter.Key.Equals("add_K_lysine"))
                    {
                        if (!ParseModMass(parameter, out staticLysineModMass, out _))
                            return false;

                        continue;
                    }

                    if (!parameter.Key.StartsWith("variable_mod"))
                    {
                        continue;
                    }

                    if (!ParseModMass(parameter, out var dynamicModMass, out var affectedResidues))
                        return false;

                    if (variableModMasses.TryGetValue(dynamicModMass, out var existingResidueList))
                    {
                        existingResidueList.AddRange(affectedResidues);
                        continue;
                    }

                    variableModMasses.Add(dynamicModMass, affectedResidues);
                }

                var staticNTermMode = GetReporterIonModeFromModMass(staticNTermModMass);
                var staticLysineMode = GetReporterIonModeFromModMass(staticLysineModMass);

                var dynamicModModes = new Dictionary<double, ReporterIonModes>();

                foreach (var item in variableModMasses)
                {
                    dynamicModModes.Add(item.Key, GetReporterIonModeFromModMass(item.Key));

                    // If necessary, we could examine the affected residues to override the auto-determined mode
                    // var affectedResidues = item.Value;
                }

                var reporterIonModeStats = new Dictionary<ReporterIonModes, int>();

                UpdateReporterIonModeStats(reporterIonModeStats, staticNTermMode);
                UpdateReporterIonModeStats(reporterIonModeStats, staticLysineMode);
                UpdateReporterIonModeStats(reporterIonModeStats, dynamicModModes.Values.ToList());

                var matchedReporterIonModes = new Dictionary<ReporterIonModes, int>();
                foreach (var item in reporterIonModeStats)
                {
                    if (item.Key != ReporterIonModes.Disabled && item.Value > 0)
                    {
                        matchedReporterIonModes.Add(item.Key, item.Value);
                    }
                }

                if (matchedReporterIonModes.Count == 0)
                {
                    reporterIonMode = ReporterIonModes.Disabled;
                    return true;
                }

                if (matchedReporterIonModes.Count == 1)
                {
                    reporterIonMode = matchedReporterIonModes.First().Key;
                    return true;
                }

                LogError("The MSFragger parameter file has more than one reporter ion mode defined: " + string.Join(", ", matchedReporterIonModes.Keys.ToList()));

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in DetermineReporterIonMode", ex);
                return false;
            }
        }

        /// <summary>
        /// Look for text in affectedResidueList
        /// For each match found, append to affectedResidues
        /// </summary>
        /// <param name="affectedResidueList"></param>
        /// <param name="residueMatcher"></param>
        /// <param name="affectedResidues"></param>
        /// <returns>Updated version of affectedResidueList with the matches removed</returns>
        private string ExtractMatches(string affectedResidueList, Regex residueMatcher, ICollection<string> affectedResidues)
        {
            if (string.IsNullOrWhiteSpace(affectedResidueList))
                return affectedResidueList;

            var matches = residueMatcher.Matches(affectedResidueList);

            if (matches.Count <= 0)
            {
                return affectedResidueList;
            }

            foreach (var match in matches)
            {
                affectedResidues.Add(match.ToString());
            }

            return residueMatcher.Replace(affectedResidueList, string.Empty);
        }

        private string GetCurrentPhilosopherToolDescription()
        {
            return mCurrentPhilosopherTool switch
            {
                PhilosopherToolType.Undefined => "Philosopher: Undefined",
                PhilosopherToolType.WorkspaceManager => "Philosopher: Workspace Manager",
                PhilosopherToolType.PeptideProphet => "Philosopher: Peptide Prophet",
                PhilosopherToolType.ProteinProphet => "Philosopher: Protein Prophet",
                PhilosopherToolType.AnnotateDatabase => "Philosopher: Annotate Database",
                PhilosopherToolType.ResultsFilter => "Philosopher: Results Filter",
                PhilosopherToolType.FreeQuant => "Philosopher: FreeQuant",
                PhilosopherToolType.LabelQuant => "Philosopher: LabelQuant",
                PhilosopherToolType.GenerateReport => "Philosopher: Generate Report",
                _ => throw new ArgumentOutOfRangeException()
            };
        }

        /// <summary>
        /// Get appropriate path of the working directory for the given experiment
        /// </summary>
        /// <param name="experimentName"></param>
        /// <param name="experimentCount"></param>
        /// <remarks>
        /// <para>If all of the datasets belong to the same experiment, return the job's working directory</para>
        /// <para>Otherwise, return a subdirectory below the working directory, based on the experiment's name</para>
        /// </remarks>
        private string GetExperimentWorkingDirectory(string experimentName, int experimentCount)
        {
            return experimentCount <= 1 ? mWorkDir : Path.Combine(mWorkDir, experimentName);
        }

        /// <summary>
        /// Group the datasets in dataPackageInfo by experiment name
        /// </summary>
        /// <param name="dataPackageInfo"></param>
        /// <remarks>Datasets that do not have an experiment group defined will be assigned to __UNDEFINED_EXPERIMENT_GROUP__</remarks>
        /// <returns>Dictionary where keys are experiment name and values are dataset ID</returns>
        public static SortedDictionary<string, SortedSet<int>> GetDataPackageDatasetsByExperiment(DataPackageInfo dataPackageInfo)
        {
            // Keys in this dictionary are Experiment Group name
            // Values are a list of dataset IDs
            var dataPackageDatasetsByExperiment = new SortedDictionary<string, SortedSet<int>>();

            foreach (var item in dataPackageInfo.Datasets)
            {
                var experimentGroup = dataPackageInfo.DatasetExperimentGroup[item.Key];
                var experimentGroupToUse = string.IsNullOrWhiteSpace(experimentGroup) ? UNDEFINED_EXPERIMENT_GROUP : experimentGroup;

                if (dataPackageDatasetsByExperiment.TryGetValue(experimentGroupToUse, out var matchedDatasetsForGroup))
                {
                    matchedDatasetsForGroup.Add(item.Key);
                    continue;
                }

                var datasetsForGroup = new SortedSet<int>
                {
                    item.Key
                };

                dataPackageDatasetsByExperiment.Add(experimentGroupToUse, datasetsForGroup);
            }

            return dataPackageDatasetsByExperiment;
        }

        private bool FindFragPipeLibDirectory(out DirectoryInfo libDirectory)
        {
            // ReSharper disable CommentTypo

            // mPhilosopherProgLoc has the path to philosopher.exe, for example
            // C:\DMS_Programs\MSFragger\fragpipe\tools\philosopher\philosopher.exe

            // Construct the path to the fragpipe lib directory, which should be at
            // C:\DMS_Programs\MSFragger\fragpipe\lib

            // ReSharper restore CommentTypo

            var philosopherProgram = new FileInfo(mPhilosopherProgLoc);

            if (philosopherProgram.Directory == null)
            {
                LogError("Unable to determine the parent directory of " + mPhilosopherProgLoc);
                libDirectory = null;
                return false;
            }

            var toolsDirectory = philosopherProgram.Directory.Parent;
            if (toolsDirectory == null)
            {
                LogError("Unable to determine the parent directory of " + philosopherProgram.Directory.FullName);
                libDirectory = null;
                return false;
            }

            var fragPipeDirectory = toolsDirectory.Parent;
            if (fragPipeDirectory == null)
            {
                LogError("Unable to determine the parent directory of " + toolsDirectory.FullName);
                libDirectory = null;
                return false;
            }

            libDirectory = new DirectoryInfo(Path.Combine(fragPipeDirectory.FullName, "lib"));
            if (libDirectory.Exists)
            {
                return true;
            }

            LogError("FragPipe lib directory not found: " + libDirectory.FullName);
            return false;
        }

        private bool GetParamValueDouble(KeyValuePair<string, string> parameter, out double value)
        {
            if (double.TryParse(parameter.Value, out value))
                return true;

            LogError(string.Format(
                "Parameter value in MSFragger parameter file is not numeric: {0} = {1}",
                parameter.Key, parameter.Value));

            return false;
        }

        private bool GetParamValueInt(KeyValuePair<string, string> parameter, out int value)
        {
            if (int.TryParse(parameter.Value, out value))
                return true;

            LogError(string.Format(
                "Parameter value in MSFragger parameter file is not numeric: {0} = {1}",
                parameter.Key, parameter.Value));

            return false;
        }

        private ReporterIonModes GetReporterIonModeFromModMass(double modMass)
        {
            if (Math.Abs(modMass - 304.207146) < 0.001)
                return ReporterIonModes.Tmt16;

            if (Math.Abs(modMass - 304.205353) < 0.001)
                return ReporterIonModes.Itraq8;

            if (Math.Abs(modMass - 144.102066) < 0.005)
                return ReporterIonModes.Itraq4;

            if (Math.Abs(modMass - 229.162933) < 0.005)
            {
                // 6-plex, 10-plex, and 11-plex TMT
                return ReporterIonModes.Tmt11;
            }

            return ReporterIonModes.Disabled;
        }

        /// <summary>
        /// Create the temporary directories used by Peptide Prophet
        /// </summary>
        /// <param name="dataPackageInfo"></param>
        /// <param name="datasetIDsByExperiment"></param>
        /// <param name="experimentWorkingDirectories"></param>
        /// <returns>Dictionary where keys are dataset names and values are DirectoryInfo instances</returns>
        private Dictionary<int, DirectoryInfo> InitializePeptideProphetWorkspaceDirectories(
            DataPackageInfo dataPackageInfo,
            Dictionary<string, List<int>> datasetIDsByExperiment,
            IReadOnlyDictionary<string, DirectoryInfo> experimentWorkingDirectories)
        {
            var workspaceDirectoryByDatasetId = new Dictionary<int, DirectoryInfo>();

            // Initialize the workspace directories for PeptideProphet (separate subdirectory for each dataset)
            foreach (var item in datasetIDsByExperiment)
            {
                var experimentDirectory = experimentWorkingDirectories[item.Key];

                // Create a separate temp directory for each dataset
                foreach (var datasetId in item.Value)
                {
                    var datasetName = dataPackageInfo.Datasets[datasetId];

                    var directoryName = string.Format("fragpipe-{0}{1}", datasetName, TEMP_PEP_PROPHET_DIR_SUFFIX);
                    var workingDirectory = new DirectoryInfo(Path.Combine(experimentDirectory.FullName, directoryName));

                    InitializePhilosopherWorkspaceWork(workingDirectory);

                    workspaceDirectoryByDatasetId.Add(datasetId, workingDirectory);
                }
            }

            return workspaceDirectoryByDatasetId;
        }

        /// <summary>
        /// Initialize the Philosopher workspace (creates a hidden directory named .meta)
        /// </summary>
        /// <param name="experimentNames"></param>
        /// <param name="experimentWorkingDirectories"></param>
        /// <remarks>Also creates a subdirectory for each experiment if experimentNames has more than one item</remarks>
        /// <returns>Success code</returns>
        private CloseOutType InitializePhilosopherWorkspace(
            IReadOnlyCollection<string> experimentNames,
            out Dictionary<string, DirectoryInfo> experimentWorkingDirectories)
        {
            experimentWorkingDirectories = new Dictionary<string, DirectoryInfo>();

            try
            {
                LogDebug("Initializing the Philosopher Workspace");

                mCurrentPhilosopherTool = PhilosopherToolType.WorkspaceManager;

                var workDirSuccess = InitializePhilosopherWorkspaceWork(new DirectoryInfo(mWorkDir), false);
                if (workDirSuccess != CloseOutType.CLOSEOUT_SUCCESS)
                    return workDirSuccess;

                var experimentCount = experimentNames.Count;

                foreach (var experimentName in experimentNames)
                {
                    var workingDirectoryPath = GetExperimentWorkingDirectory(experimentName, experimentCount);
                    var workingDirectory = new DirectoryInfo(workingDirectoryPath);

                    experimentWorkingDirectories.Add(experimentName, workingDirectory);

                    if (experimentCount == 1)
                        continue;

                    var success = InitializePhilosopherWorkspaceWork(workingDirectory);

                    if (success != CloseOutType.CLOSEOUT_SUCCESS)
                        return success;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in InitializePhilosopherWorkspace", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType InitializePhilosopherWorkspaceWork(DirectoryInfo directory, bool createDirectoryIfMissing = true)
        {
            try
            {
                if (!directory.Exists)
                {
                    if (!createDirectoryIfMissing)
                    {
                        LogError("Cannot initialize the Philosopher workspace; directory not found: " + directory.FullName);
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    directory.Create();
                }

                // ReSharper disable once StringLiteralTypo
                var arguments = "workspace --init --nocheck";

                // Run the workspace init command
                var success = RunPhilosopher(PhilosopherToolType.WorkspaceManager, arguments, "initialize the workspace", directory.FullName);

                if (string.IsNullOrWhiteSpace(mPhilosopherVersion))
                {
                    ParsePhilosopherConsoleOutputFile(Path.Combine(mWorkDir, PHILOSOPHER_CONSOLE_OUTPUT));
                }

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error in InitializePhilosopherWorkspaceWork", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Parse the MSFragger parameter file to determine certain processing options
        /// </summary>
        /// <param name="datasetCount"></param>
        /// <param name="paramFilePath"></param>
        /// <param name="options"></param>
        /// <remarks>Also looks for job parameters that can be used to enable/disable processing options</remarks>
        /// <returns>True if success, false if an error</returns>
        private bool LoadMSFraggerOptions(int datasetCount, string paramFilePath, out MSFraggerOptions options)
        {
            options = new MSFraggerOptions(mJobParams, datasetCount);

            try
            {
                var result = LoadSettingsFromKeyValueParameterFile("MSFragger", paramFilePath, out var paramFileEntries, out _, true);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    return false;

                var success = DetermineReporterIonMode(paramFileEntries, out var reporterIonMode);
                if (!success)
                    return false;

                options.ReporterIonMode = reporterIonMode;

                var precursorMassLower = 0.0;
                var precursorMassUpper = 0.0;
                var precursorMassUnits = 0;

                foreach (var parameter in paramFileEntries)
                {
                    if (parameter.Key.Equals("precursor_mass_lower"))
                    {
                        if (!GetParamValueDouble(parameter, out precursorMassLower))
                            return false;

                        continue;
                    }

                    if (parameter.Key.Equals("precursor_mass_upper"))
                    {
                        if (!GetParamValueDouble(parameter, out precursorMassUpper))
                            return false;

                        continue;
                    }

                    if (parameter.Key.Equals("precursor_mass_units"))
                    {
                        if (!GetParamValueInt(parameter, out precursorMassUnits))
                            return false;

                        continue;
                    }
                }


                if (precursorMassUnits > 0 && precursorMassLower < -25 && precursorMassUpper > 50)
                {
                    // Wide, Dalton-based tolerances
                    // Assume open search
                    options.OpenSearch = true;

                }
                else
                {
                    options.OpenSearch = false;
                }

                // javaProgLoc will typically be "C:\DMS_Programs\Java\jre8\bin\java.exe"
                options.JavaProgLoc = GetJavaProgLoc();

                return !string.IsNullOrWhiteSpace(options.JavaProgLoc);
            }
            catch (Exception ex)
            {
                LogError("Error in LoadMSFraggerOptions", ex);
                return false;
            }
        }

        private bool MoveResultsIntoSubdirectories(
            DataPackageInfo dataPackageInfo,
            IDictionary<string, List<int>> datasetIDsByExperiment,
            IReadOnlyDictionary<string, DirectoryInfo> experimentWorkingDirectories)
        {
            try
            {
                var dataPackageDatasetsByExperiment = GetDataPackageDatasetsByExperiment(dataPackageInfo);

                var experimentCount = dataPackageDatasetsByExperiment.Count;

                foreach (var item in dataPackageDatasetsByExperiment)
                {
                    var experimentName = item.Key;
                    var experimentWorkingDirectory = experimentWorkingDirectories[experimentName];

                    var datasetIDs = new List<int>();

                    foreach (var datasetId in item.Value)
                    {
                        var datasetName = dataPackageInfo.Datasets[datasetId];
                        datasetIDs.Add(datasetId);

                        if (experimentCount <= 1)
                            continue;

                        var sourceFile = new FileInfo(Path.Combine(mWorkDir, datasetName + PEPXML_EXTENSION));

                        var targetPath = Path.Combine(experimentWorkingDirectory.FullName, sourceFile.Name);

                        sourceFile.MoveTo(targetPath);
                    }

                    datasetIDsByExperiment.Add(experimentName, datasetIDs);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in MoveResultsIntoSubdirectories", ex);
                return false;
            }
        }

        private CloseOutType OrganizePepXmlFiles(
            out DataPackageInfo dataPackageInfo,
            out Dictionary<string, List<int>> datasetIDsByExperiment,
            out Dictionary<string, DirectoryInfo> experimentWorkingDirectories)
        {
            // Keys in this dictionary are experiment names, values are a list of Dataset IDs for each experiment
            datasetIDsByExperiment = new Dictionary<string, List<int>>();

            // Keys in this dictionary are experiment names, values are the working directory to use
            experimentWorkingDirectories = new Dictionary<string, DirectoryInfo>();

            // If this job applies to a single dataset, dataPackageID will be 0
            // We still need to create an instance of DataPackageInfo to retrieve the experiment name associated with the job's dataset
            var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

            dataPackageInfo = new DataPackageInfo(dataPackageID, this);
            RegisterEvents(dataPackageInfo);

            // Keys in this dictionary are experiment name; values are dataset ID
            var dataPackageDatasetsByExperiment = GetDataPackageDatasetsByExperiment(dataPackageInfo);

            // Initialize the Philosopher workspace (creates a hidden directory named .meta)
            // If Experiment Groups are defined, we also create a subdirectory for each experiment and initialize it

            var experimentNames = dataPackageDatasetsByExperiment.Keys.ToList();
            var initResult = InitializePhilosopherWorkspace(experimentNames, out experimentWorkingDirectories);
            if (initResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return initResult;
            }

            if (experimentNames.Count <= 1)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            // Move the pepXML files into the experiment group directories
            var moveSuccess = MoveResultsIntoSubdirectories(dataPackageInfo, datasetIDsByExperiment, experimentWorkingDirectories);

            return moveSuccess ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        private List<string> ParseAffectedResidueList(string affectedResidueList)
        {
            // This matches [^ or ]^ or [A
            var proteinTerminusMatcher = new Regex(@"[\[\]][A-Z\^]", RegexOptions.Compiled);

            // This matches nQ or nC or cK or n^
            var peptideTerminusMatcher = new Regex(@"[nc][A-Z\^]", RegexOptions.Compiled);

            // This matches single letter residue symbols or *
            var residueMatcher = new Regex(@"[A-Z\*]", RegexOptions.Compiled);

            var affectedResidues = new List<string>();

            var updatedList1 = ExtractMatches(affectedResidueList, proteinTerminusMatcher, affectedResidues);
            var updatedList2 = ExtractMatches(updatedList1, peptideTerminusMatcher, affectedResidues);
            var updatedList3 = ExtractMatches(updatedList2, residueMatcher, affectedResidues);

            if (!string.IsNullOrWhiteSpace(updatedList3))
            {
                affectedResidues.Add(updatedList3);
            }

            return affectedResidues;
        }

        /// <summary>
        /// Parse a static or dynamic mod parameter to determine the modification mass, and (if applicable) the affected residues
        /// </summary>
        /// <param name="parameter"></param>
        /// <param name="modMass"></param>
        /// <param name="affectedResidues"></param>
        /// <remarks>Assumes the calling method already removed any comment text (beginning with the # sign)</remarks>
        /// <returns>True if success, false if an error</returns>
        private bool ParseModMass(KeyValuePair<string, string> parameter, out double modMass, out List<string> affectedResidues)
        {
            // Example dynamic mods (format is Mass AffectedResidues MaxOccurrences):
            // variable_mod_01 = 15.994900 M 3        # Oxidized methionine
            // variable_mod_02 = 42.010600 [^ 1       # Acetylation protein N-term

            // Example static mods:
            // add_Nterm_peptide = 304.207146    # 16-plex TMT
            // add_K_lysine = 304.207146         # 16-plex TMT

            var spaceIndex = parameter.Value.IndexOf(' ');
            string parameterValue;

            if (spaceIndex < 0)
            {
                parameterValue = parameter.Value;
                affectedResidues = new List<string>();
            }
            else
            {
                parameterValue = parameter.Value.Substring(0, spaceIndex).Trim();
                var remainingValue = parameter.Value.Substring(spaceIndex + 1).Trim();

                var spaceIndex2 = remainingValue.IndexOf(' ');

                var affectedResidueList = spaceIndex2 > 0 ? parameter.Value.Substring(0, spaceIndex2).Trim() : string.Empty;

                affectedResidues = ParseAffectedResidueList(affectedResidueList);
            }

            if (double.TryParse(parameterValue, out modMass))
            {
                return true;
            }

            LogError(string.Format(
                "Modification mass in MSFragger parameter file is not numeric: {0} = {1}",
                parameter.Key, parameter.Value));

            return false;
        }

        /// <summary>
        /// Parse the Philosopher console output file to determine the Philosopher version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParsePhilosopherConsoleOutputFile(string consoleOutputFilePath)
        {
            // ReSharper disable CommentTypo

            // ----------------------------------------------------
            // Example Console output when initializing the workspace
            // ----------------------------------------------------

            // INFO[17:45:51] Executing Workspace  v3.4.13
            // INFO[17:45:51] Removing workspace
            // INFO[17:45:51] Done

            // ----------------------------------------------------
            // Example Console output when running Peptide Prophet
            // ----------------------------------------------------

            // INFO[11:01:05] Executing PeptideProphet  v3.4.13
            //  file 1: C:\DMS_WorkDir\QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.pepXML
            //  processed altogether 6982 results
            // INFO: Results written to file: C:\DMS_WorkDir\interact-QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.pep.xml
            // ...
            // INFO: Processing standard MixtureModel ...
            //  PeptideProphet  (TPP v5.2.1-dev Flammagenitus, Build 201906281613-exported (Windows_NT-x86_64)) AKeller@ISB
            // ...
            // INFO[11:01:25] Done

            // ----------------------------------------------------
            // Example Console output when running Protein Prophet
            // ----------------------------------------------------

            // INFO[11:05:08] Executing ProteinProphet  v3.4.13
            // ProteinProphet (C++) by Insilicos LLC and LabKey Software, after the original Perl by A. Keller (TPP v5.2.1-dev Flammagenitus, Build 201906281613-exported (Windows_NT-x86_64))
            //  (no FPKM) (using degen pep info)
            // Reading in C:/DMS_WorkDir/interact-QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.pep.xml...
            // ...
            // Finished.
            // INFO[11:05:12] Done

            // ----------------------------------------------------
            // Example Console output when running Filter
            // ----------------------------------------------------

            // INFO[11:07:13] Executing Filter  v3.4.13
            // INFO[11:07:13] Processing peptide identification files
            // ...
            // INFO[11:07:16] Saving
            // INFO[11:07:16] Done

            // ----------------------------------------------------
            // Example Console output when running FreeQuant
            // ----------------------------------------------------
            // ToDo: add functionality for this

            // ----------------------------------------------------
            // Example Console output when running LabelQuant
            // ----------------------------------------------------
            // ToDo: add functionality for this

            // ----------------------------------------------------
            // Example Console output when running Abacus
            // ----------------------------------------------------
            // ToDo: add functionality for this

            // ReSharper restore CommentTypo

            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + consoleOutputFilePath);
                    }

                    return;
                }

                var versionMatcher = new Regex(@"INFO.+Executing [^ ]+ +(?<Version>v[^ ]+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                if (mDebugLevel >= 4)
                {
                    LogDebug("Parsing file " + consoleOutputFilePath);
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate("Error parsing the Philosopher console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        [Obsolete("Old method")]
        private void ParseConsoleOutputFile()
        {
            const string BUILD_AND_VERSION = "Current Philosopher build and version";

            var mConsoleOutputFilePath = Path.Combine(mWorkDir, PHILOSOPHER_CONSOLE_OUTPUT);

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

        private bool RunAbacus()
        {
            // PhilosopherAbacus [Work dir: C:\FragPipe_Test2\Results]
            // C:\DMS_Programs\MSFragger\FragPipe\tools\philosopher\philosopher.exe abacus --razor --reprint --tag XXX_ --protein CHI_IXN CHI_DA

            return false;
        }

        private bool RunCrystalC(Dictionary<string, List<int>> datasetIDsByExperiment)
        {
            // Crystal-C [Work dir: C:\FragPipe_Test2\Results\CHI_DA]
            // java -Dbatmass.io.libs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.2\ext\thermo" -Xmx17G -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\original-crystalc-1.3.2.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.22.1.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\grppr-0.3.23.jar" crystalc.Run C:\FragPipe_Test2\Results\CHI_DA\crystalc-0-CHI_IXN_DA_31_Bane_06May21_20-11-16.pepXML.params C:\FragPipe_Test2\Results\CHI_DA\CHI_IXN_DA_31_Bane_06May21_20-11-16.pepXML
            // Crystal-C [Work dir: C:\FragPipe_Test2\Results\CHI_IXN]
            // java -Dbatmass.io.libs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.2\ext\thermo" -Xmx17G -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\original-crystalc-1.3.2.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.22.1.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\grppr-0.3.23.jar" crystalc.Run C:\FragPipe_Test2\Results\CHI_IXN\crystalc-1-CHI_IXN_DA_30_Bane_06May21_20-11-16.pepXML.params C:\FragPipe_Test2\Results\CHI_IXN\CHI_IXN_DA_30_Bane_06May21_20-11-16.pepXML
            // Crystal-C [Work dir: C:\FragPipe_Test2\Results\CHI_IXN]
            // java -Dbatmass.io.libs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.2\ext\thermo" -Xmx17G -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\original-crystalc-1.3.2.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.22.1.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\grppr-0.3.23.jar" crystalc.Run C:\FragPipe_Test2\Results\CHI_IXN\crystalc-2-CHI_IXN_DA_29_Bane_06May21_20-11-16.pepXML.params C:\FragPipe_Test2\Results\CHI_IXN\CHI_IXN_DA_29_Bane_06May21_20-11-16.pepXML

            return false;
        }

        /// <summary>
        /// Create a db.bin file in the .meta subdirectory of the working directory and in any experiment directories
        /// </summary>
        /// <param name="experimentWorkingDirectories"></param>
        /// <returns></returns>
        private bool RunDatabaseAnnotation(IReadOnlyDictionary<string, DirectoryInfo> experimentWorkingDirectories)
        {
            try
            {
                LogDebug("Annotating the FASTA file to create db.bin files", 2);

                // First process the working directory
                var workDirSuccess = RunDatabaseAnnotation(mWorkDir);
                if (!workDirSuccess)
                    return false;

                if (experimentWorkingDirectories.Count <= 1)
                    return true;

                // Next process each of the experiment directories
                var successCount = 0;

                foreach (var experimentDirectory in experimentWorkingDirectories.Values)
                {
                    var success = RunDatabaseAnnotation(experimentDirectory.FullName);

                    if (success)
                        successCount++;
                }

                return successCount == experimentWorkingDirectories.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in RunDatabaseAnnotation", ex);
                return false;
            }
        }

        /// <summary>
        /// Create a db.bin file in the .meta subdirectory of the given directory
        /// </summary>
        /// <param name="workingDirectoryPath"></param>
        private bool RunDatabaseAnnotation(string workingDirectoryPath)
        {
            var arguments = string.Format("database --annotate {0} --prefix XXX_", mFastaFilePath);

            return RunPhilosopher(PhilosopherToolType.AnnotateDatabase, arguments, "annotate the database", workingDirectoryPath);
        }

        private bool RunFreeQuant()
        {
            try
            {
                LogDebug("Running FreeQuant", 2);

                // ReSharper disable once StringLiteralTypo
                var arguments = @"freequant --ptw 0.4 --tol 10 --isolated --dir C:\DMS_WorkDir";

                var directoryPath = mWorkDir;

                var success = RunPhilosopher(PhilosopherToolType.FreeQuant, arguments, "annotate the database", directoryPath);

                return success;
            }
            catch (Exception ex)
            {
                LogError("Error in RunFreeQuant", ex);
                return false;
            }
        }

        private bool RunIonQuant()
        {
            try
            {
                LogDebug("Running IonQuant", 2);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in RunIonQuant", ex);
                return false;
            }
        }

        private bool RunLabelQuant(ReporterIonModes reporterIonMode)
        {
            try
            {
                LogDebug("Running LabelQuant", 2);

                var arguments = "...";
                var directoryPath = mWorkDir;

                var success = RunPhilosopher(PhilosopherToolType.LabelQuant, arguments, "annotate the database", directoryPath);

                return success;
            }
            catch (Exception ex)
            {
                LogError("Error in RunLabelQuant", ex);
                return false;
            }
        }

        private bool RunPeptideProphet(
            DataPackageInfo dataPackageInfo,
            Dictionary<string, List<int>> datasetIDsByExperiment,
            IReadOnlyDictionary<string, DirectoryInfo> experimentWorkingDirectories,
            MSFraggerOptions options,
            out List<FileInfo> peptideProphetPepXmlFiles)
        {
            try
            {
                LogDebug("Running peptide prophet", 2);

                if (options.OpenSearch)
                {
                    return RunPeptideProphetForOpenSearch(
                        dataPackageInfo,
                        datasetIDsByExperiment,
                        experimentWorkingDirectories,
                        options,
                        out peptideProphetPepXmlFiles);
                }

                // Keys in this dictionary are dataset names, values are DirectoryInfo instances
                var workspaceDirectoryByDatasetId = InitializePeptideProphetWorkspaceDirectories(
                    dataPackageInfo,
                    datasetIDsByExperiment,
                    experimentWorkingDirectories);

                // Run Peptide Prophet separately against each dataset

                foreach (var item in workspaceDirectoryByDatasetId)
                {
                    var datasetId = item.Key;
                    var datasetName = dataPackageInfo.Datasets[datasetId];
                    var workingDirectory = item.Value;

                    // ReSharper disable StringLiteralTypo

                    var arguments = string.Format(
                        @"peptideprophet --decoyprobs --ppm --accmass --nonparam --expectscore --decoy XXX_ --database {0} ..\{1}.pepXML",
                        mFastaFilePath, datasetName);

                    // ReSharper restore StringLiteralTypo

                    var success = RunPhilosopher(PhilosopherToolType.PeptideProphet, arguments, "run peptide prophet", workingDirectory.FullName);
                    if (!success)
                    {
                        peptideProphetPepXmlFiles = new List<FileInfo>();
                        return false;
                    }
                }

                DeleteTempDirectories(workspaceDirectoryByDatasetId.Values.ToList());

                return UpdateMsMsRunSummaryInPepXmlFiles(dataPackageInfo, workspaceDirectoryByDatasetId, options, out peptideProphetPepXmlFiles);
            }
            catch (Exception ex)
            {
                LogError("Error in RunPeptideProphet", ex);
                peptideProphetPepXmlFiles = new List<FileInfo>();
                return false;
            }
        }

        private bool RunPeptideProphetForOpenSearch(
            DataPackageInfo dataPackageInfo,
            Dictionary<string, List<int>> datasetIDsByExperiment,
            IReadOnlyDictionary<string, DirectoryInfo> experimentWorkingDirectories,
            MSFraggerOptions options,
            out List<FileInfo> peptideProphetPepXmlFiles)
        {
            try
            {
                // Run Peptide Prophet separately against each experiment

                foreach (var item in datasetIDsByExperiment)
                {
                    var experimentDirectory = experimentWorkingDirectories[item.Key];

                    // ReSharper disable StringLiteralTypo

                    var arguments = new StringBuilder();

                    arguments.AppendFormat(
                        "peptideprophet --nonparam --expectscore --decoyprobs --masswidth 1000.0 --clevel -2 --decoy XXX_ --database {0} --combine",
                        mFastaFilePath);

                    // ReSharper restore StringLiteralTypo

                    foreach (var datasetId in item.Value)
                    {
                        var datasetName = dataPackageInfo.Datasets[datasetId];
                        var pepXmlFilename = string.Format("{0}_c.pepXML", datasetName);

                        var pepXmlFile = new FileInfo(Path.Combine(experimentDirectory.FullName, pepXmlFilename));
                        if (!pepXmlFile.Exists)
                        {
                            LogError("Crystal-C .pepXML file not found: " + pepXmlFile.Name);
                            peptideProphetPepXmlFiles = new List<FileInfo>();
                            return false;
                        }

                        arguments.AppendFormat(" {0}", pepXmlFilename);
                    }

                    var success = RunPhilosopher(PhilosopherToolType.PeptideProphet, arguments.ToString(), "run peptide prophet", experimentDirectory.FullName);
                    if (!success)
                    {
                        peptideProphetPepXmlFiles = new List<FileInfo>();
                        return false;
                    }
                }

                return UpdateMsMsRunSummaryInCombinedPepXmlFiles(
                    dataPackageInfo,
                    datasetIDsByExperiment,
                    experimentWorkingDirectories,
                    options,
                    out peptideProphetPepXmlFiles);
            }
            catch (Exception ex)
            {
                LogError("Error in RunPeptideProphetCombined", ex);
                peptideProphetPepXmlFiles = new List<FileInfo>();
                return false;
            }
        }

        private bool RunPhilosopher(PhilosopherToolType toolType, string arguments, string currentTask, string workingDirectoryPath = "")
        {
            try
            {
                mCurrentPhilosopherTool = toolType;

                if (string.IsNullOrWhiteSpace(workingDirectoryPath))
                    mCmdRunner.WorkDir = mWorkDir;
                else
                    mCmdRunner.WorkDir = workingDirectoryPath;

                mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, PHILOSOPHER_CONSOLE_OUTPUT);

                // Run philosopher using the specified arguments

                LogDebug(mPhilosopherProgLoc + " " + arguments);

                // Start the program and wait for it to finish
                // However, while it's running, LoopWaiting will get called via events
                var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputErrorMsg);
                }

                UpdateCombinedPhilosopherConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

                if (!processingSuccess)
                {
                    LogError("Error running Philosopher to " + currentTask);

                    if (mCmdRunner.ExitCode != 0)
                    {
                        LogWarning("Philosopher returned a non-zero exit code: " + mCmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to Philosopher failed (but exit code is 0)");
                    }

                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in RunPhilosopher", ex);
                return false;
            }
        }

        private bool RunProteinProphet(IEnumerable<FileInfo> peptideProphetPepXmlFiles)
        {
            try
            {
                LogDebug("Running Protein Prophet", 2);

                var arguments = new StringBuilder();

                // ReSharper disable once StringLiteralTypo
                arguments.Append("--maxppmdiff 2000000 --output combined");

                foreach (var pepXmlFile in peptideProphetPepXmlFiles)
                {
                    arguments.AppendFormat(" {0}", pepXmlFile.FullName);
                }

                return RunPhilosopher(PhilosopherToolType.ProteinProphet, arguments.ToString(), "run protein prophet");
            }
            catch (Exception ex)
            {
                LogError("Error in RunProteinProphet", ex);
                return false;
            }
        }

        private bool RunPTMShepherd()
        {
            // PTMShepherd [Work dir: C:\FragPipe_Test2\Results]
            // java -Dbatmass.io.libs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.2\ext\thermo" -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\ptmshepherd-1.0.0.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.22.1.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\commons-math3-3.6.1.jar" edu.umich.andykong.ptmshepherd.PTMShepherd "C:\FragPipe_Test2\Results\shepherd.config"

            return false;
        }

        private bool RunReportGeneration()
        {
            try
            {
                LogDebug("Generating MSFragger report files", 2);

                var arguments = "...";
                var directoryPath = mWorkDir;

                var success = RunPhilosopher(PhilosopherToolType.GenerateReport, arguments, "generate report files", directoryPath);

                return success;
            }
            catch (Exception ex)
            {
                LogError("Error in RunReportGeneration", ex);
                return false;
            }
        }

        private bool RunResultsFilter(IReadOnlyDictionary<string, DirectoryInfo> experimentWorkingDirectories, MSFraggerOptions options)
        {
            try
            {
                LogDebug("Filtering MSFragger Results", 2);

                var successCount = 0;

                var arguments = new StringBuilder();

                foreach (var experimentDirectory in experimentWorkingDirectories.Values)
                {
                    arguments.Clear();

                    arguments.Append("filter --sequential --razor");

                    if (!options.MatchBetweenRuns)
                    {
                        arguments.Append(" --picked");
                    }

                    arguments.Append(" --prot 0.01");

                    if (options.OpenSearch)
                    {
                        // ReSharper disable once StringLiteralTypo
                        arguments.Append(" --mapmods");
                    }

                    arguments.Append("--tag XXX_");

                    arguments.AppendFormat(" --pepxml {0}", experimentDirectory.FullName);

                    arguments.AppendFormat(" --protxml {0}", Path.Combine(experimentDirectory.FullName, "combined.prot.xml"));

                    var success = RunPhilosopher(PhilosopherToolType.ResultsFilter, arguments.ToString(), "filter results", experimentDirectory.FullName);

                    if (success)
                        successCount++;
                }

                return successCount == experimentWorkingDirectories.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in RunResultsFilter", ex);
                return false;
            }
        }

        private bool RunTmtIntegrator(ReporterIonModes reporterIonMode)
        {
            try
            {
                LogDebug("Running TMT-Integrator", 2);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in RunTmtIntegrator", ex);
                return false;
            }
        }

        //[Obsolete("Old workflow")]
        //private CloseOutType StartPepProtProphet()
        //{
        //    LogMessage("Running Philosopher");

        //    // Set up and execute a program runner to run Philosopher

        //    // We will call Philosopher several times
        //    // 1. Determine the Philosopher version
        //    // 2. Initialize the workspace
        //    // 3. Annotate the database (creates db.bin in the .meta subdirectory)
        //    // 4. Run Peptide Prophet
        //    // 5. Run Protein Prophet
        //    // 6. Filter results
        //    // 7. Generate the final report

        //    mProgress = PROGRESS_PCT_STARTING;
        //    ResetProgRunnerCpuUsage();

        //    mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
        //    {
        //        CreateNoWindow = true,
        //        CacheStandardOutput = true,
        //        EchoOutputToConsole = true,
        //        WriteConsoleOutputToFile = true,
        //        ConsoleOutputFilePath = Path.Combine(mWorkDir, PHILOSOPHER_CONSOLE_OUTPUT)
        //    };
        //    RegisterEvents(mCmdRunner);
        //    mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

        //    mConsoleOutputFilePath = mCmdRunner.ConsoleOutputFilePath;

        //    var versionResult = GetPhilosopherVersion();
        //    if (versionResult != CloseOutType.CLOSEOUT_SUCCESS)
        //        return versionResult;

        //    var workspaceInitResult = InitializeWorkspace();
        //    if (workspaceInitResult != CloseOutType.CLOSEOUT_SUCCESS)
        //        return workspaceInitResult;

        //    var dbAnnotationResult = AnnotateDatabase(out var fastaFile);
        //    if (dbAnnotationResult != CloseOutType.CLOSEOUT_SUCCESS)
        //        return dbAnnotationResult;

        //    var peptideProphetResult = RunPeptideProphet(fastaFile, out var peptideProphetResults);
        //    if (peptideProphetResult != CloseOutType.CLOSEOUT_SUCCESS)
        //        return peptideProphetResult;

        //    var proteinProphetResult = RunProteinProphet(peptideProphetResults, out var proteinProphetResults);
        //    if (proteinProphetResult != CloseOutType.CLOSEOUT_SUCCESS)
        //        return proteinProphetResult;

        //    var filterResult = FilterResults(proteinProphetResults);
        //    if (filterResult != CloseOutType.CLOSEOUT_SUCCESS)
        //        return filterResult;

        //    var reportResult = GenerateFinalReport();
        //    if (reportResult != CloseOutType.CLOSEOUT_SUCCESS)
        //        return reportResult;

        //    return CloseOutType.CLOSEOUT_SUCCESS;
        //}

        //private CloseOutType GetPhilosopherVersion()
        //{
        //    try
        //    {
        //        var arguments = "version";

        //        LogDebug(mPhilosopherProgLoc + " " + arguments);

        //        var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

        //        HandlePhilosopherError(processingSuccess, mCmdRunner.ExitCode);
        //        if (!processingSuccess)
        //        {
        //            return CloseOutType.CLOSEOUT_FAILED;
        //        }

        //        ParseConsoleOutputFile();

        //        if (string.IsNullOrWhiteSpace(mPhilosopherVersion))
        //        {
        //            LogError("Unable to determine the Philosopher version");
        //            return CloseOutType.CLOSEOUT_FAILED;
        //        }

        //        var toolVersionWritten = StoreToolVersionInfo();

        //        return toolVersionWritten ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        //    }
        //    catch (Exception ex)
        //    {
        //        LogError("Error in PepProtProphetPlugIn->GetPhilosopherVersion", ex);
        //        return CloseOutType.CLOSEOUT_FAILED;
        //    }
        //}

        //private CloseOutType InitializeWorkspace()
        //{
        //    try
        //    {
        //        var arguments = "workspace --init";

        //        LogDebug(mPhilosopherProgLoc + " " + arguments);

        //        var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

        //        HandlePhilosopherError(processingSuccess, mCmdRunner.ExitCode);
        //        if (!processingSuccess)
        //        {
        //            return CloseOutType.CLOSEOUT_FAILED;
        //        }

        //        ParseConsoleOutputFile();
        //        return CloseOutType.CLOSEOUT_SUCCESS;
        //    }
        //    catch (Exception ex)
        //    {
        //        LogError("Error in PepProtProphetPlugIn->InitializeWorkspace", ex);
        //        return CloseOutType.CLOSEOUT_FAILED;
        //    }
        //}

        //private CloseOutType AnnotateDatabase(out FileInfo fastaFile)
        //{
        //    try
        //    {
        //        // Define the path to the fasta file
        //        var localOrgDbFolder = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);

        //        // Note that job parameter "generatedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
        //        var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam("PeptideSearch", AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

        //        fastaFile = new FileInfo(fastaFilePath);

        //        var arguments = "database" +
        //                        " --annotate " + fastaFile.FullName +
        //                        " --prefix XXX_";

        //        LogDebug(mPhilosopherProgLoc + " " + arguments);

        //        var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

        //        HandlePhilosopherError(processingSuccess, mCmdRunner.ExitCode);
        //        if (!processingSuccess)
        //        {
        //            return CloseOutType.CLOSEOUT_FAILED;
        //        }

        //        ParseConsoleOutputFile();
        //        return CloseOutType.CLOSEOUT_SUCCESS;
        //    }
        //    catch (Exception ex)
        //    {
        //        LogError("Error in PepProtProphetPlugIn->AnnotateDatabase", ex);
        //        fastaFile = null;
        //        return CloseOutType.CLOSEOUT_FAILED;
        //    }
        //}

        //private CloseOutType RunPeptideProphet(FileSystemInfo fastaFile, out FileInfo peptideProphetResults)
        //{
        //    peptideProphetResults = null;

        //    try
        //    {
        //        var pepXmlFile = new FileInfo(Path.Combine(mWorkDir, Dataset + ".pepXML"));
        //        if (!pepXmlFile.Exists)
        //        {
        //            LogError("PepXML file not found: " + pepXmlFile.Name);
        //            return CloseOutType.CLOSEOUT_FAILED;
        //        }

        //        // ReSharper disable StringLiteralTypo
        //        var arguments = "peptideprophet " +
        //                        "--ppm " +
        //                        "--accmass " +
        //                        "--nonparam " +
        //                        "--expectscore " +
        //                        "--decoyprobs " +
        //                        "--decoy XXX_ " +
        //                        "--database " + fastaFile.FullName +
        //                        " " + pepXmlFile.FullName;
        //        // ReSharper restore StringLiteralTypo

        //        LogDebug(mPhilosopherProgLoc + " " + arguments);

        //        // Start the program and wait for it to finish
        //        // However, while it's running, LoopWaiting will get called via events
        //        var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

        //        HandlePhilosopherError(processingSuccess, mCmdRunner.ExitCode);
        //        if (!processingSuccess)
        //        {
        //            return CloseOutType.CLOSEOUT_FAILED;
        //        }

        //        ParseConsoleOutputFile();

        //        peptideProphetResults = new FileInfo(Path.Combine(mWorkDir, "interact-" + Dataset + ".pep.xml"));
        //        if (!peptideProphetResults.Exists)
        //        {
        //            LogError("Peptide prophet results file not found: " + pepXmlFile.Name);
        //            return CloseOutType.CLOSEOUT_FAILED;
        //        }

        //        return CloseOutType.CLOSEOUT_SUCCESS;
        //    }
        //    catch (Exception ex)
        //    {
        //        LogError("Error in PepProtProphetPlugIn->RunPeptideProphet", ex);
        //        return CloseOutType.CLOSEOUT_FAILED;
        //    }
        //}

        //private CloseOutType RunProteinProphet(FileSystemInfo peptideProphetResults, out FileInfo proteinProphetResults)
        //{
        //    proteinProphetResults = null;

        //    try
        //    {
        //        // ReSharper disable StringLiteralTypo
        //        var arguments = "proteinprophet" +
        //                        " --maxppmdiff 2000000" +
        //                        " " + peptideProphetResults.FullName;
        //        // ReSharper restore StringLiteralTypo

        //        LogDebug(mPhilosopherProgLoc + " " + arguments);

        //        var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

        //        HandlePhilosopherError(processingSuccess, mCmdRunner.ExitCode);
        //        if (!processingSuccess)
        //        {
        //            return CloseOutType.CLOSEOUT_FAILED;
        //        }

        //        ParseConsoleOutputFile();

        //        proteinProphetResults = new FileInfo(Path.Combine(mWorkDir, "interact.prot.xml"));
        //        if (!proteinProphetResults.Exists)
        //        {
        //            LogError("Protein prophet results file not found: " + proteinProphetResults.Name);
        //            return CloseOutType.CLOSEOUT_FAILED;
        //        }

        //        return CloseOutType.CLOSEOUT_SUCCESS;
        //    }
        //    catch (Exception ex)
        //    {
        //        LogError("Error in PepProtProphetPlugIn->RunProteinProphet", ex);
        //        return CloseOutType.CLOSEOUT_FAILED;
        //    }
        //}

        //private CloseOutType FilterResults(FileSystemInfo proteinProphetResults)
        //{
        //    try
        //    {
        //        // ReSharper disable StringLiteralTypo
        //        var arguments = "filter" +
        //                        " --sequential" +
        //                        " --razor" +
        //                        " --mapmods" +
        //                        " --prot 0.01" +
        //                        " --tag XXX_" +
        //                        " --pepxml " + mWorkDir + "" +
        //                        " --protxml " + proteinProphetResults.FullName;
        //        // ReSharper restore StringLiteralTypo

        //        LogDebug(mPhilosopherProgLoc + " " + arguments);

        //        var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

        //        HandlePhilosopherError(processingSuccess, mCmdRunner.ExitCode);
        //        if (!processingSuccess)
        //        {
        //            return CloseOutType.CLOSEOUT_FAILED;
        //        }

        //        ParseConsoleOutputFile();
        //        return CloseOutType.CLOSEOUT_SUCCESS;
        //    }
        //    catch (Exception ex)
        //    {
        //        LogError("Error in PepProtProphetPlugIn->FilterResults", ex);
        //        return CloseOutType.CLOSEOUT_FAILED;
        //    }
        //}

        //private CloseOutType GenerateFinalReport()
        //{
        //    try
        //    {
        //        var arguments = "report";

        //        LogDebug(mPhilosopherProgLoc + " " + arguments);

        //        var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

        //        HandlePhilosopherError(processingSuccess, mCmdRunner.ExitCode);
        //        if (!processingSuccess)
        //        {
        //            return CloseOutType.CLOSEOUT_FAILED;
        //        }

        //        ParseConsoleOutputFile();
        //        return CloseOutType.CLOSEOUT_SUCCESS;
        //    }
        //    catch (Exception ex)
        //    {
        //        LogError("Error in PepProtProphetPlugIn->GenerateFinalReport", ex);
        //        return CloseOutType.CLOSEOUT_FAILED;
        //    }
        //}

        private bool StoreToolVersionInfo()
        {
            LogDebug("Determining tool version info", mDebugLevel);

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(mPhilosopherProgLoc),
                new(mTmtIntegratorProgLoc),
            };

            try
            {
                return SetStepTaskToolVersion(mPhilosopherVersion, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private void UpdateCombinedPhilosopherConsoleOutputFile(string consoleOutputFilepath)
        {
            try
            {
                var consoleOutputFile = new FileInfo(consoleOutputFilepath);
                if (!consoleOutputFile.Exists)
                {
                    LogWarning("UpdateCombinedPhilosopherConsoleOutput: ConsoleOutput file not found: " + consoleOutputFilepath);
                    return;
                }

                var combinedFilePath = Path.Combine(mWorkDir, PHILOSOPHER_CONSOLE_OUTPUT_COMBINED);

                using var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
                using var writer = new StreamWriter(new FileStream(combinedFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite));

                writer.WriteLine();

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    if (string.IsNullOrWhiteSpace(dataLine))
                        writer.WriteLine();
                    else
                        writer.WriteLine(dataLine);
                }
            }
            catch (Exception ex)
            {
                LogError("Error in UpdateCombinedPhilosopherConsoleOutputFile", ex);
            }
        }

        /// <summary>
        /// Update the msms_run_summary element in pepXML files created by Peptide Prophet to adjust the path to the parent .mzML files
        /// </summary>
        /// <param name="dataPackageInfo"></param>
        /// <param name="datasetIDsByExperiment"></param>
        /// <param name="experimentWorkingDirectories"></param>
        /// <param name="options"></param>
        /// <param name="peptideProphetPepXmlFiles">Output: list of the .pepXML files created by peptide prophet</param>
        /// <remarks>
        /// This method is called when peptide prophet was run against groups of dataset (<seealso cref="UpdateMsMsRunSummaryInPepXmlFiles"/>)</remarks>
        /// <returns>True if success, false if an error</returns>
        private bool UpdateMsMsRunSummaryInCombinedPepXmlFiles(
            DataPackageInfo dataPackageInfo,
            Dictionary<string, List<int>> datasetIDsByExperiment,
            IReadOnlyDictionary<string, DirectoryInfo> experimentWorkingDirectories,
            MSFraggerOptions options,
            out List<FileInfo> peptideProphetPepXmlFiles)
        {
            peptideProphetPepXmlFiles = new List<FileInfo>();

            try
            {
                var successCount = 0;

                if (!FindFragPipeLibDirectory(out var libDirectory))
                    return false;

                var arguments = new StringBuilder();

                foreach (var item in datasetIDsByExperiment)
                {
                    var experimentDirectory = experimentWorkingDirectories[item.Key];

                    var pepXmlFile = new FileInfo(Path.Combine(experimentDirectory.FullName, "interact.pep.xml"));

                    if (!pepXmlFile.Exists)
                    {
                        LogError("Peptide prophet results file not found: " + pepXmlFile.FullName);
                        continue;
                    }

                    peptideProphetPepXmlFiles.Add(pepXmlFile);

                    arguments.Clear();

                    // ReSharper disable once StringLiteralTypo
                    arguments.AppendFormat("-cp {0}/* com.dmtavt.fragpipe.util.RewritePepxml {1}", libDirectory.FullName, pepXmlFile.FullName);

                    foreach (var datasetId in item.Value)
                    {
                        var datasetFile = new FileInfo(Path.Combine(mWorkDir, dataPackageInfo.DatasetFiles[datasetId]));
                        if (!datasetFile.Extension.Equals(AnalysisResources.DOT_MZML_EXTENSION, StringComparison.OrdinalIgnoreCase))
                        {
                            LogError(string.Format("The extension for dataset file {0} is not .mzML; this is unexpected", datasetFile.Name));
                            continue;
                        }

                        arguments.AppendFormat(" {0}", datasetFile.FullName);
                    }

                    // ReSharper disable CommentTypo

                    // Example command:
                    // C:\DMS_Programs\Java\jre8\bin\java.exe -cp C:\DMS_Programs\MSFragger\fragpipe\lib/* com.dmtavt.fragpipe.util.RewritePepxml C:\DMS_WorkDir\CHI_IXN\interact.pep.xml C:\DMS_WorkDir\CHI_IXN_DA_30_Bane_06May21_20-11-16.mzML C:\FragPipe_Test2\CHI_IXN_DA_29_Bane_06May21_20-11-16.mzML

                    // ReSharper enable CommentTypo

                    mCmdRunner.WorkDir = experimentDirectory.FullName;

                    LogDebug(options.JavaProgLoc + " " + arguments);

                    var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments.ToString(), "Java", true);

                    if (!processingSuccess)
                    {
                        if (mCmdRunner.ExitCode != 0)
                        {
                            LogWarning("Java returned a non-zero exit code: " + mCmdRunner.ExitCode);
                        }
                        else
                        {
                            LogWarning("Call to Java failed (but exit code is 0)");
                        }

                        continue;
                    }

                    successCount++;
                }

                return successCount == datasetIDsByExperiment.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in UpdateMsMsRunSummaryInCombinedPepXmlFiles", ex);
                return false;
            }
        }
        /// <summary>
        /// Update the msms_run_summary element in pepXML files created by Peptide Prophet to adjust the path to the parent .mzML file
        /// </summary>
        /// <param name="dataPackageInfo"></param>
        /// <param name="workspaceDirectoryByDatasetId"></param>
        /// <param name="options"></param>
        /// <param name="peptideProphetPepXmlFiles">Output: list of the .pepXML files created by peptide prophet</param>
        /// <remarks>
        /// This method is called when Peptide Prophet was run separately against each dataset (<seealso cref="UpdateMsMsRunSummaryInCombinedPepXmlFiles"/>)
        /// </remarks>
        /// <returns>True if success, false if an error</returns>
        private bool UpdateMsMsRunSummaryInPepXmlFiles(
            DataPackageInfo dataPackageInfo,
            Dictionary<int, DirectoryInfo> workspaceDirectoryByDatasetId,
            MSFraggerOptions options,
            out List<FileInfo> peptideProphetPepXmlFiles)
        {
            // This method updates the .pep.xml files created by PeptideProphet to remove the experiment name and forward slash from the <msms_run_summary> element

            // For example, changing from:
            // <msms_run_summary base_name="C:\FragPipe_Test2\Experiment1/Dataset_20-11-16" raw_data_type="mzML" comment="This pepXML was from calibrated spectra." raw_data="mzML">

            // To:
            // <msms_run_summary base_name="C:\FragPipe_Test2\Dataset_20-11-16" raw_data_type="mzML" raw_data="mzML">

            peptideProphetPepXmlFiles = new List<FileInfo>();

            try
            {
                var successCount = 0;

                if (!FindFragPipeLibDirectory(out var libDirectory))
                    return false;

                foreach (var item in workspaceDirectoryByDatasetId)
                {
                    var datasetId = item.Key;
                    var datasetName = dataPackageInfo.Datasets[datasetId];
                    var workingDirectory = item.Value;

                    if (workingDirectory.Parent == null)
                    {
                        LogError("Unable to determine the parent directory of " + workingDirectory.FullName);
                        continue;
                    }

                    var pepXmlFile = new FileInfo(Path.Combine(
                        workingDirectory.Parent.FullName,
                        string.Format("interact-{0}.pep.xml", datasetName)));

                    if (!pepXmlFile.Exists)
                    {
                        LogError("Peptide prophet results file not found: " + pepXmlFile.FullName);
                        continue;
                    }

                    peptideProphetPepXmlFiles.Add(pepXmlFile);

                    var datasetFile = new FileInfo(Path.Combine(mWorkDir, dataPackageInfo.DatasetFiles[datasetId]));
                    if (!datasetFile.Extension.Equals(AnalysisResources.DOT_MZML_EXTENSION, StringComparison.OrdinalIgnoreCase))
                    {
                        LogError(string.Format("The extension for dataset file {0} is not .mzML; this is unexpected", datasetFile.Name));
                        continue;
                    }

                    // ReSharper disable CommentTypo

                    // Example command:
                    // C:\DMS_Programs\Java\jre8\bin\java.exe -cp C:\DMS_Programs\MSFragger\fragpipe\lib/* com.dmtavt.fragpipe.util.RewritePepxml C:\DMS_WorkDir\interact-QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.pep.xml C:\DMS_WorkDir\QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.mzML

                    // ReSharper restore CommentTypo

                    // ReSharper disable once StringLiteralTypo

                    var arguments = string.Format(
                        "-cp {0}/* com.dmtavt.fragpipe.util.RewritePepxml {1} {2}",
                        libDirectory.FullName, pepXmlFile.FullName, datasetFile.FullName);

                    mCmdRunner.WorkDir = workingDirectory.FullName;

                    LogDebug(options.JavaProgLoc + " " + arguments);

                    var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments, "Java", true);

                    if (!processingSuccess)
                    {
                        if (mCmdRunner.ExitCode != 0)
                        {
                            LogWarning("Java returned a non-zero exit code: " + mCmdRunner.ExitCode);
                        }
                        else
                        {
                            LogWarning("Call to Java failed (but exit code is 0)");
                        }

                        continue;
                    }

                    successCount++;
                }

                return successCount == workspaceDirectoryByDatasetId.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in UpdateMsMsRunSummaryInPepXmlFiles", ex);
                return false;
            }
        }

        private void UpdateReporterIonModeStats(IDictionary<ReporterIonModes, int> reporterIonModeStats, ReporterIonModes reporterIonMode)
        {
            UpdateReporterIonModeStats(reporterIonModeStats, new List<ReporterIonModes> { reporterIonMode });
        }

        private void UpdateReporterIonModeStats(IDictionary<ReporterIonModes, int> reporterIonModeStats, IEnumerable<ReporterIonModes> reporterIonModeList)
        {
            foreach (var reporterIonMode in reporterIonModeList)
            {
                if (reporterIonModeStats.TryGetValue(reporterIonMode, out var currentCount))
                {
                    reporterIonModeStats[reporterIonMode] = currentCount + 1;
                }
                else
                {
                    reporterIonModeStats.Add(reporterIonMode, 1);
                }
            }
        }

        private bool ValidateFastaFile()
        {
            // Define the path to the fasta file
            var localOrgDbFolder = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);

            // Note that job parameter "generatedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
            var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam("PeptideSearch", AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

            var fastaFile = new FileInfo(fastaFilePath);

            if (fastaFile.Exists)
            {
                mFastaFilePath = fastaFilePath;
                return true;
            }

            // Fasta file not found
            LogError("Fasta file not found: " + fastaFile.Name, "Fasta file not found: " + fastaFile.FullName);
            return false;
        }

        private bool ZipPepXmlFiles(DataPackageInfo dataPackageInfo)
        {
            try
            {
                // Zip each .pepXML file
                var successCount = 0;

                foreach (var dataset in dataPackageInfo.Datasets)
                {
                    var pepXmlFile = new FileInfo(Path.Combine(mWorkDir, dataset.Value + PEPXML_EXTENSION));

                    var zipSuccess = AnalysisToolRunnerMSFragger.ZipPepXmlFile(this, dataset.Value, pepXmlFile);
                    if (!zipSuccess)
                    {
                        continue;
                    }

                    successCount++;
                }

                return successCount == dataPackageInfo.Datasets.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in ZipPepXmlFiles", ex);
                return false;
            }
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            if (!(DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE))
                return;

            mLastConsoleOutputParse = DateTime.UtcNow;

            ParsePhilosopherConsoleOutputFile(Path.Combine(mWorkDir, PHILOSOPHER_CONSOLE_OUTPUT));

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mPhilosopherVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("Philosopher");
        }
    }
}
