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

        // Ignore Spell: Acetylation, Batmass, Da, degen, deisotoping, dev, dir, Flammagenitus, fragpipe, freequantInsilicos
        // Ignore Spell: nocheck, Nterm, pepindex, plex, postprocessing, ptw, timsdata, tmt, tol, Xmx

        // Ignore Spell: peptideprophet, decoyprobs, ppm, accmass, nonparam, expectscore

        // ReSharper restore CommentTypo

        // ReSharper disable IdentifierTypo


        private const string PHILOSOPHER_CONSOLE_OUTPUT = "Philosopher_ConsoleOutput.txt";
        private const string PHILOSOPHER_CONSOLE_OUTPUT_COMBINED = "Philosopher_ConsoleOutput_Combined.txt";

        private const string ABACUS_PROPHET_CONSOLE_OUTPUT = "Abacus_ConsoleOutput.txt";
        private const string FREEQUANT_PROPHET_CONSOLE_OUTPUT = "FreeQuant_ConsoleOutput.txt";
        private const string LABELQUANT_PROPHET_CONSOLE_OUTPUT = "LabelQuant_ConsoleOutput.txt";
        private const string PEPTIDE_PROPHET_CONSOLE_OUTPUT = "PeptideProphet_ConsoleOutput.txt";
        private const string PROTEIN_PROPHET_CONSOLE_OUTPUT = "ProteinProphet_ConsoleOutput.txt";

        private const string PEPXML_EXTENSION = ".pepXML";

        private const string PHILOSOPHER_RELATIVE_PATH = @"fragpipe\tools\philosopher\philosopher.exe";

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
            StartingPeptideProphet = 2,
            PeptideProphetComplete = 15,
            ProteinProphetComplete = 30,
            DBAnnotationComplete = 50,
            ResultsFilterComplete = 65,
            LabelQuantComplete = 80,
            ReportGenerated = 90,
            IonQuantComplete = 93,
            TmtIntegratorComplete = 96,
            ProcessingComplete = 99
        }

        private enum ReporterIonMode
        {
            Disabled = 0,
            Itraq4 = 1,
            Itraq8 = 2,
            Tmt6 = 3,
            Tmt10 = 4,
            Tmt11 = 5,
            Tmt16 = 6
        }

        private bool mToolVersionWritten;

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
                var paramFilePath= Path.Combine(mWorkDir, paramFileName);

                var success = DetermineReporterIonMode(paramFilePath, out var reporterIonMode);

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var moveFilesSuccess = OrganizePepXmlFiles(out var dataPackageInfo, out var datasetsByExperiment);
                if (moveFilesSuccess != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return moveFilesSuccess;
                }

                // Run Peptide Prophet
                var peptideProphetSuccess = RunPeptideProphet(datasetsByExperiment);
                if (!peptideProphetSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.PeptideProphetComplete;

                // Run Protein Prophet
                var proteinProphetSuccess = RunProteinProphet(dataPackageInfo);
                if (!proteinProphetSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.ProteinProphetComplete;

                var dbAnnotateSuccess = RunDatabaseAnnotation();
                if (!dbAnnotateSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.DBAnnotationComplete;

                var filterSuccess = RunResultsFilter();
                if (!filterSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.ResultsFilterComplete;

                var runFreeQuant = mJobParams.GetJobParameter("RunFreeQuant", false);
                if (runFreeQuant)
                {
                    var freeQuantSuccess = RunFreeQuant();
                    if (!freeQuantSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                if (reporterIonMode != ReporterIonMode.Disabled)
                {
                    var labelQuantSuccess = RunLabelQuant(reporterIonMode);
                    if (!labelQuantSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.LabelQuantComplete;
                }

                var reportSuccess = RunReportGeneration();
                if (!reportSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.ReportGenerated;


                var runIonQuant = mJobParams.GetJobParameter("RunIonQuant", false);
                if (runIonQuant)
                {
                    var ionQuantSuccess = RunIonQuant();
                    if (!ionQuantSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.IonQuantComplete;
                }

                if (reporterIonMode != ReporterIonMode.Disabled)
                {
                    var tmtIntegratorSuccess = RunTmtIntegrator(reporterIonMode);
                    if (!tmtIntegratorSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.TmtIntegratorComplete;
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
        /// Examine the dynamic and static mods in a MSFragger parameter file to determine the reporter ion mode
        /// </summary>
        /// <param name="paramFilePath"></param>
        /// <param name="reporterIonMode"></param>
        /// <returns>True if success, false if an error</returns>
        private bool DetermineReporterIonMode(string paramFilePath, out ReporterIonMode reporterIonMode)
        {
            reporterIonMode = ReporterIonMode.Disabled;

            try
            {
                var staticNTermModMass = 0.0;
                var staticLysineModMass = 0.0;

                // Keys in this dictionary are modification masses; values are a list of the affected residues
                var variableModMasses = new Dictionary<double, List<string>>();

                var result = LoadSettingsFromKeyValueParameterFile("MSFragger", paramFilePath, out var paramFileEntries, out _, true);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    return false;

                foreach (var parameter in paramFileEntries)
                {
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

                var dynamicModModes = new Dictionary<double, ReporterIonMode>();

                foreach (var item in variableModMasses)
                {
                    dynamicModModes.Add(item.Key, GetReporterIonModeFromModMass(item.Key));

                    // If necessary, we could examine the affected residues to override the auto-determined mode
                    // var affectedResidues = item.Value;
                }

                var reporterIonModeStats = new Dictionary<ReporterIonMode, int>();

                UpdateReporterIonModeStats(reporterIonModeStats, staticNTermMode);
                UpdateReporterIonModeStats(reporterIonModeStats, staticLysineMode);
                UpdateReporterIonModeStats(reporterIonModeStats, dynamicModModes.Values.ToList());

                var matchedReporterIonModes = new Dictionary<ReporterIonMode, int>();
                foreach (var item in reporterIonModeStats)
                {
                    if (item.Key != ReporterIonMode.Disabled && item.Value > 0)
                    {
                        matchedReporterIonModes.Add(item.Key, item.Value);
                    }
                }

                if (matchedReporterIonModes.Count == 0)
                {
                    reporterIonMode = ReporterIonMode.Disabled;
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

        private ReporterIonMode GetReporterIonModeFromModMass(double modMass)
        {
            if (Math.Abs(modMass - 304.207146) < 0.001)
                return ReporterIonMode.Tmt16;

            if (Math.Abs(modMass - 304.205353) < 0.001)
                return ReporterIonMode.Itraq8;

            if (Math.Abs(modMass - 144.102066) < 0.005)
                return ReporterIonMode.Itraq4;

            if (Math.Abs(modMass - 229.162933) < 0.005)
            {
                // 6-plex, 10-plex, and 11-plex TMT
                return ReporterIonMode.Tmt11;
            }

            return ReporterIonMode.Disabled;
        }

        /// <summary>
        /// Initialize the Philosopher workspace (creates a hidden directory named .meta)
        /// </summary>
        /// <param name="experimentNames"></param>
        /// <remarks>Also creates a subdirectory for each experiment if experimentNames has more than one item</remarks>
        /// <returns>Success code</returns>
        private CloseOutType InitializePhilosopherWorkspace(IReadOnlyCollection<string> experimentNames)
        {
            try
            {
                LogMessage("Initializing the Philosopher Workspace");

                mCurrentPhilosopherTool = PhilosopherToolType.WorkspaceManager;

                var workDirSuccess = InitializePhilosopherWorkspaceWork(mWorkDir, false);
                if (workDirSuccess != CloseOutType.CLOSEOUT_SUCCESS)
                    return workDirSuccess;

                if (experimentNames.Count <= 1)
                {
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                foreach (var experimentName in experimentNames)
                {
                    var success = InitializePhilosopherWorkspaceWork(Path.Combine(mWorkDir, experimentName));

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

        private CloseOutType InitializePhilosopherWorkspaceWork(string directoryPath, bool createDirectoryIfMissing = true)
        {
            try
            {
                var directory = new DirectoryInfo(directoryPath);
                if (!directory.Exists)
                {
                    if (!createDirectoryIfMissing)
                    {
                        LogError("Cannot initialize the Philosopher workspace; directory not found: " + directoryPath);
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    directory.Create();
                }

                // ReSharper disable once StringLiteralTypo
                var arguments = "workspace --init --nocheck";

                // Run the workspace init command
                var success = RunPhilosopher(PhilosopherToolType.WorkspaceManager, arguments, "initialize the workspace", directoryPath);

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

        private bool MoveResultsIntoSubdirectories(DataPackageInfo dataPackageInfo, IDictionary<string, List<string>> datasetsByExperiment)
        {
            try
            {
                foreach (var item in AnalysisToolRunnerMSFragger.GetDataPackageDatasetsByExperiment(dataPackageInfo))
                {
                    var experimentName = item.Key;
                    var datasetNames = new List<string>();

                    foreach (var datasetId in item.Value)
                    {
                        var datasetName = dataPackageInfo.Datasets[datasetId];
                        datasetNames.Add(datasetName);

                        var sourceFile = new FileInfo(Path.Combine(mWorkDir, datasetName + PEPXML_EXTENSION));

                        var targetPath = Path.Combine(mWorkDir, experimentName, sourceFile.Name);

                        sourceFile.MoveTo(targetPath);
                    }

                    datasetsByExperiment.Add(experimentName, datasetNames);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in MoveResultsIntoSubdirectories", ex);
                return false;
            }
        }

        private CloseOutType OrganizePepXmlFiles(out DataPackageInfo dataPackageInfo, out Dictionary<string, List<string>> datasetsByExperiment)
        {
            // Keys in this dictionary are experiment names, values are a list of Dataset Names for each experiment
            datasetsByExperiment = new Dictionary<string, List<string>>();

            // If this job applies to a single dataset, dataPackageID will be 0
            // We still need to create an instance of DataPackageInfo to retrieve the experiment name associated with the job's dataset
            var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

            dataPackageInfo = new DataPackageInfo(dataPackageID, this);
            RegisterEvents(dataPackageInfo);

            // Keys in this dictionary are experiment name; values are dataset ID
            var dataPackageDatasetsByExperiment = AnalysisToolRunnerMSFragger.GetDataPackageDatasetsByExperiment(dataPackageInfo);

            // Initialize the Philosopher workspace (creates a hidden directory named .meta)
            // If Experiment Groups are defined, we also create a subdirectory for each experiment and initialize it

            var experimentNames = dataPackageDatasetsByExperiment.Keys.ToList();
            var initResult = InitializePhilosopherWorkspace(experimentNames);
            if (initResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return initResult;
            }

            if (dataPackageInfo.Datasets.Count > 1)
            {
                // Move the pepXML files into the experiment group directories
                var moveSuccess = MoveResultsIntoSubdirectories(dataPackageInfo, datasetsByExperiment);
                if (!moveSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
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


        private bool RunDatabaseAnnotation()
        {
            try
            {
                mCurrentPhilosopherTool = PhilosopherToolType.AnnotateDatabase;
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in RunDatabaseAnnotation", ex);
                return false;
            }
        }

        private bool RunFreeQuant()
        {
            try
            {
                mCurrentPhilosopherTool = PhilosopherToolType.FreeQuant;

                // Command line:
                // philosopher.exe freequant --ptw 0.4 --tol 10 --isolated --dir C:\DMS_WorkDir

                return true;
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

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in RunIonQuant", ex);
                return false;
            }
        }

        private bool RunLabelQuant(ReporterIonMode reporterIonMode)
        {
            try
            {
                mCurrentPhilosopherTool = PhilosopherToolType.LabelQuant;
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in RunLabelQuant", ex);
                return false;
            }
        }

        private bool RunPeptideProphet(Dictionary<string, List<string>> datasetsByExperiment)
        {
            try
            {
                // Keys in this dictionary are dataset names, values are DirectoryInfo instances
                var workspaceDirectoryByDataset = new Dictionary<string, DirectoryInfo>();

                // Initialize the workspace directories for PeptideProphet (separate subdirectory for each dataset)
                foreach (var item in datasetsByExperiment)
                {
                    var experimentDirectory = Path.Combine(mWorkDir, item.Key);

                    foreach (var datasetName in item.Value)
                    {
                        var directoryName = string.Format("fragpipe-{0}.pepXML-temp", datasetName);
                        var workingDirectory = new DirectoryInfo(Path.Combine(experimentDirectory, directoryName));

                        InitializePhilosopherWorkspaceWork(workingDirectory.FullName);

                        workspaceDirectoryByDataset.Add(datasetName, workingDirectory);
                    }
                }

                // Run peptide prophet for each dataset

                var fastaFilepath = @"C:\FragPipe_Test2\ID_006084_0D8B6467.revCat.fasta";

                foreach (var item in workspaceDirectoryByDataset)
                {
                    var datasetName = item.Key;
                    var workingDirectory = item.Value;

                    var arguments = string.Format(
                        @"peptideprophet --decoyprobs --ppm --accmass --nonparam --expectscore --decoy XXX_ --database {0} ..\{1}.pepXML",
                        fastaFilepath, datasetName);

                    var success = RunPhilosopher(PhilosopherToolType.PeptideProphet, arguments, "run peptide prophet", workingDirectory.FullName);
                    if (!success)
                        return false;

                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in RunPeptideProphet", ex);
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

        private bool RunProteinProphet(DataPackageInfo dataPackageInfo)
        {
            try
            {
                mCurrentPhilosopherTool = PhilosopherToolType.ProteinProphet;
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in RunProteinProphet", ex);
                return false;
            }
        }

        private bool RunReportGeneration()
        {
            try
            {
                mCurrentPhilosopherTool = PhilosopherToolType.GenerateReport;
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in RunReportGeneration", ex);
                return false;
            }
        }

        private bool RunResultsFilter()
        {
            try
            {
                mCurrentPhilosopherTool = PhilosopherToolType.ResultsFilter;
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in RunResultsFilter", ex);
                return false;
            }
        }

        private bool RunTmtIntegrator(ReporterIonMode reporterIonMode)
        {
            try
            {
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
            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

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

        private void UpdateReporterIonModeStats(IDictionary<ReporterIonMode, int> reporterIonModeStats, ReporterIonMode reporterIonMode)
        {
            UpdateReporterIonModeStats(reporterIonModeStats, new List<ReporterIonMode> { reporterIonMode });
        }

        private void UpdateReporterIonModeStats(IDictionary<ReporterIonMode, int> reporterIonModeStats, IEnumerable<ReporterIonMode> reporterIonModeList)
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
