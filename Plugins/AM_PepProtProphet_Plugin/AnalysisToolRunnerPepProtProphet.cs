//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 04/19/2019
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerMSFraggerPlugIn;
using PRISMDatabaseUtils;

namespace AnalysisManagerPepProtProphetPlugIn
{
    /// <summary>
    /// Class for running PeptideProphet, ProteinProphet, and other tools for post-processing MSFragger results
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerPepProtProphet : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: accmass, acetyl, acetylation, annot, antivirus, batmass-io, bruker, ccee, clevel, contam, cp, crystalc, decoyprobs, dir, expectscore
        // Ignore Spelling: fasta, filelist, fragger, fragpipe, freequant, glycan, glyco, glycosylation, groupby, iprophet, itraq, java, javacpp, jfreechart, labelquant, labelling, linux, locprob
        // Ignore Spelling: mapmods, masswidth, maxlfq, maxppmdiff, minprob, msbooster, msstats, multidir, nocheck, nonparam, nonsp, num, openblas, overlabelling, outlier
        // Ignore Spelling: peptideprophet, pepxml, phospho, phosphorylation, plex, ppm, proteinprophet, protxml, psm, psms, --ptw, prot, Quant
        // Ignore Spelling: razorbin, sp, specdir, tdc, tmt, tmtintegrator, --tol, unimod, Xmx
        // Ignore Spelling: \batmass, \bruker, \fragpipe, \grppr, \ionquant, \ptmshepherd, \thermo, \tools

        // IonQuant command line arguments
        // Ignore Spelling: ionmobility, mbr, proteinquant, requantify, mztol, imtol, rttol, mbrmincorr, mbrrttol, mbrimtol, mbrtoprun
        // Ignore Spelling: ionfdr, proteinfdr, peptidefdr, minisotopes, minscans, writeindex, --tp, minfreq, minexps

        // ReSharper restore CommentTypo

        private const string JAVA_CONSOLE_OUTPUT = "Java_ConsoleOutput.txt";
        private const string JAVA_CONSOLE_OUTPUT_COMBINED = "Java_ConsoleOutput_Combined.txt";

        private const string PERCOLATOR_CONSOLE_OUTPUT = "Percolator_ConsoleOutput.txt";
        private const string PERCOLATOR_CONSOLE_OUTPUT_COMBINED = "Percolator_ConsoleOutput_Combined.txt";

        private const string PHILOSOPHER_CONSOLE_OUTPUT = "Philosopher_ConsoleOutput.txt";
        private const string PHILOSOPHER_CONSOLE_OUTPUT_COMBINED = "Philosopher_ConsoleOutput_Combined.txt";

        private const string PTM_PROPHET_CONSOLE_OUTPUT = "PTMProphet_ConsoleOutput.txt";
        private const string PTM_PROPHET_CONSOLE_OUTPUT_COMBINED = "PTMProphet_ConsoleOutput_Combined.txt";

        private const string PTM_SHEPHERD_CONSOLE_OUTPUT = "PTMShepherd_ConsoleOutput.txt";

        /// <summary>
        /// Reserve 16 GB when running Crystal-C with Java
        /// </summary>
        public const int CRYSTALC_MEMORY_SIZE_GB = 16;

        /// <summary>
        /// Interval, in milliseconds, for monitoring programs run via <see cref="mCmdRunner"/>
        /// </summary>
        private const int DEFAULT_MONITOR_INTERVAL_MSEC = 2000;

        /// <summary>
        /// Reserve 16 GB when running IonQuant with Java
        /// </summary>
        public const int ION_QUANT_MEMORY_SIZE_GB = 16;

        /// <summary>
        /// Reserve 16 GB when running MSBooster with Java
        /// </summary>
        public const int MSBOOSTER_MEMORY_SIZE_GB = 16;

        /// <summary>
        /// Reserve 11 GB when running TMT-Integrator
        /// </summary>
        public const int TMT_INTEGRATOR_MEMORY_SIZE_GB = 11;

        /// <summary>
        /// Extension for peptide XML files
        /// </summary>
        public const string PEPXML_EXTENSION = ".pepXML";

        /// <summary>
        /// Extension for pin files (tab-delimited text files created by MSFragger)
        /// </summary>
        public const string PIN_EXTENSION = ".pin";

        private const string PROTEIN_PROPHET_RESULTS_FILE = "combined.prot.xml";

        private const string TEMP_PEP_PROPHET_DIR_SUFFIX = ".pepXML-temp";

        private const string META_SUBDIRECTORY = ".meta";

        public const float PROGRESS_PCT_INITIALIZING = 1;

        private const string TMT_REPORT_DIRECTORY = "tmt-report";

        public const string ZIPPED_QUANT_CSV_FILES = "Dataset_quant_csv.zip";

        /// <summary>
        /// Philosopher tool type
        /// </summary>
        internal enum PhilosopherToolType
        {
            Undefined = 0,
            ShowVersion = 1,
            WorkspaceManager = 2,
            PeptideProphet = 3,
            ProteinProphet = 4,
            AnnotateDatabase = 5,
            ResultsFilter = 6,
            FreeQuant = 7,
            LabelQuant = 8,
            GenerateReport = 9,
            IProphet = 10,
            Abacus = 11
        }

        /// <summary>
        /// Progress percent values
        /// </summary>
        private enum ProgressPercentValues
        {
            Undefined = 0,
            Initializing = 1,
            ProcessingStarted = 2,
            CrystalCComplete = 5,
            MSBoosterComplete = 10,
            PeptideProphetOrPercolatorComplete = 15,
            PTMProphetComplete = 18,
            ProteinProphetComplete = 30,
            DBAnnotationComplete = 45,
            ResultsFilterComplete = 60,
            FreeQuantOrLabelQuantComplete = 75,
            ReportGenerated = 85,
            IProphetComplete = 86,
            // Deprecated: AbacusComplete = 87,
            IonQuantComplete = 90,
            TmtIntegratorComplete = 93,
            PtmShepherdComplete = 95,
            ReportFilesUpdated = 97,
            ProcessingComplete = 99
        }

        /// <summary>
        /// Command runner modes
        /// </summary>
        internal enum CmdRunnerModes
        {
            Undefined = 0,
            Philosopher = 1,
            CrystalC = 2,
            Percolator = 3,
            PercolatorOutputToPepXml = 4,
            TmtIntegrator = 5,
            PtmProphet = 6,
            PtmShepherd = 7,
            RewritePepXml = 8,
            IonQuant = 9,
            // ReSharper disable once InconsistentNaming
            MSBooster = 10
        }

        private string mFastaFilePath;

        private string mDiaNNProgLoc;
        private string mPercolatorProgLoc;
        private string mPhilosopherProgLoc;
        private string mPtmProphetProgLoc;
        private string mTmtIntegratorProgLoc;

        private ConsoleOutputFileParser mConsoleOutputFileParser;

        /// <summary>
        /// This is set to false before each call to mCmdRunner.RunProgram
        /// It is set to true by ParseConsoleOutputFile
        /// </summary>
        public bool mConsoleOutputFileParsed;

        private PhilosopherToolType mCurrentPhilosopherTool;

        private DateTime mLastConsoleOutputParse;

        private RunDosProgram mCmdRunner;

        private CmdRunnerModes mCmdRunnerMode;

        /// <summary>
        /// Dictionary of experiment group working directories
        /// </summary>
        /// <remarks>
        /// Keys are experiment group name, values are the corresponding working directory
        /// </remarks>
        private Dictionary<string, DirectoryInfo> mExperimentGroupWorkingDirectories;

        private DirectoryInfo mWorkingDirectory;

        /// <summary>
        /// Constructor
        /// </summary>
        public AnalysisToolRunnerPepProtProphet()
        {
            mProgress = (int)ProgressPercentValues.Undefined;
        }

        /// <summary>
        /// Runs peptide and ProteinProphet using Philosopher
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

                mExperimentGroupWorkingDirectories = new Dictionary<string, DirectoryInfo>();

                mWorkingDirectory = new DirectoryInfo(mWorkDir);

                // Determine the path to DiaNN, Percolator, Philosopher, and TMT Integrator

                mDiaNNProgLoc = DetermineProgramLocation("MSFraggerProgLoc", FragPipeLibFinder.DIANN_RELATIVE_PATH);

                mPercolatorProgLoc = DetermineProgramLocation("MSFraggerProgLoc", FragPipeLibFinder.PERCOLATOR_RELATIVE_PATH);

                mPhilosopherProgLoc = DetermineProgramLocation("MSFraggerProgLoc", FragPipeLibFinder.PHILOSOPHER_RELATIVE_PATH);

                mPtmProphetProgLoc = DetermineProgramLocation("MSFraggerProgLoc", FragPipeLibFinder.PTM_PROPHET_RELATIVE_PATH);

                mTmtIntegratorProgLoc = DetermineProgramLocation("MSFraggerProgLoc", FragPipeLibFinder.TMT_INTEGRATOR_JAR_RELATIVE_PATH);

                if (string.IsNullOrWhiteSpace(mDiaNNProgLoc) ||
                    string.IsNullOrWhiteSpace(mPercolatorProgLoc) ||
                    string.IsNullOrWhiteSpace(mPhilosopherProgLoc) ||
                    string.IsNullOrWhiteSpace(mTmtIntegratorProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mConsoleOutputFileParser = new ConsoleOutputFileParser(mDebugLevel);
                RegisterEvents(mConsoleOutputFileParser);

                mConsoleOutputFileParser.ErrorNoMessageUpdateEvent += ConsoleOutputFileParser_ErrorNoMessageUpdateEvent;

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
                mCmdRunnerMode = CmdRunnerModes.Undefined;

                // Make sure objects are released
                Global.IdleLoop(0.5);
                PRISM.AppUtils.GarbageCollectNow();

                if (!AnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Skip these console output files since we created combined console output files
                mJobParams.AddResultFileToSkip(JAVA_CONSOLE_OUTPUT);
                mJobParams.AddResultFileToSkip(PERCOLATOR_CONSOLE_OUTPUT);
                mJobParams.AddResultFileToSkip(PHILOSOPHER_CONSOLE_OUTPUT);
                mJobParams.AddResultFileToSkip(PTM_PROPHET_CONSOLE_OUTPUT);

                // Skip the filtered FASTA file, created when method RunReportGeneration is called
                mJobParams.AddResultFileToSkip("protein.fas");

                // Also skip these files (since they're small and do not contain useful info)
                mJobParams.AddResultFileToSkip("reprint.int.tsv");
                mJobParams.AddResultFileToSkip("reprint.spc.tsv");

                var subdirectoriesToSkip = new SortedSet<string>
                {
                    Path.Combine(mWorkingDirectory.FullName, META_SUBDIRECTORY),
                    Path.Combine(mWorkingDirectory.FullName, "MSBooster_RTplots")
                };

                if (mExperimentGroupWorkingDirectories.Count > 1)
                {
                    foreach (var experimentGroupDirectory in mExperimentGroupWorkingDirectories)
                    {
                        subdirectoriesToSkip.Add(experimentGroupDirectory.Value.FullName);
                    }
                }

                var success = CopyResultsToTransferDirectory(true, subdirectoriesToSkip);

                // ReSharper disable once ConvertIfStatementToReturnStatement
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

        private void AppendModificationMasses(ISet<double> modificationMasses, Dictionary<string, SortedSet<double>> modificationInfo)
        {
            foreach (var residueOrLocation in modificationInfo)
            {
                foreach (var modificationMass in residueOrLocation.Value)
                {
                    // Add the modification mass if not present
                    modificationMasses.Add(modificationMass);
                }
            }
        }

        private bool DeterminePhilosopherVersion()
        {
            const PhilosopherToolType toolType = PhilosopherToolType.ShowVersion;

            var success = RunPhilosopher(toolType, "version", "get the version");

            if (!success)
                return false;

            if (string.IsNullOrWhiteSpace(mConsoleOutputFileParser.PhilosopherVersion))
            {
                mConsoleOutputFileParser.ParsePhilosopherConsoleOutputFile(Path.Combine(mWorkingDirectory.FullName, PHILOSOPHER_CONSOLE_OUTPUT), toolType);
            }

            if (!string.IsNullOrWhiteSpace(mConsoleOutputFileParser.PhilosopherVersion))
            {
                return StoreToolVersionInfo();
            }

            LogError("Unable to determine the version of Philosopher");
            return false;
        }

        private CloseOutType ExecuteWorkflow()
        {
            try
            {
                mCmdRunner = new RunDosProgram(mWorkingDirectory.FullName, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(mWorkingDirectory.FullName, PHILOSOPHER_CONSOLE_OUTPUT),
                    MonitorInterval = DEFAULT_MONITOR_INTERVAL_MSEC
                };
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                var philosopherExe = new FileInfo(mPhilosopherProgLoc);

                // Determine the version of Philosopher
                var versionSuccess = DeterminePhilosopherVersion();

                if (!versionSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var moveFilesSuccess = OrganizePepXmlAndPinFiles(
                    out var dataPackageInfo,
                    out var datasetIDsByExperimentGroup,
                    out var workingDirectoryPadWidth);

                if (moveFilesSuccess != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return moveFilesSuccess;
                }

                var paramFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);
                var paramFilePath = Path.Combine(mWorkingDirectory.FullName, paramFileName);

                var datasetCount = datasetIDsByExperimentGroup.Sum(item => item.Value.Count);

                var optionsLoaded = LoadMSFraggerOptions(philosopherExe, datasetCount, paramFilePath, out var options);

                if (!optionsLoaded)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (options.ReporterIonMode == ReporterIonModes.Tmt16)
                {
                    // Switch to 18-plex TMT if any of the datasets associated with this job has TMT18 labeling defined for the experiment in DMS
                    var success = UpdateReporterIonModeIfRequired(options);

                    if (!success)
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                var phosphoSearch = PhosphoModDefined(options.FraggerOptions.VariableModifications);

                if (options.OpenSearch || options.FraggerOptions.ReporterIonMode != ReporterIonModes.Disabled && phosphoSearch)
                {
                    var runPTMProphetParam = mJobParams.GetJobParameter("RunPTMProphet", string.Empty);

                    if (options.FraggerOptions.IsUndefinedOrAuto(runPTMProphetParam))
                    {
                        options.RunPTMProphet = true;
                    }
                }

                options.WorkingDirectoryPadWidth = workingDirectoryPadWidth;

                mProgress = (int)ProgressPercentValues.ProcessingStarted;

                if (mExperimentGroupWorkingDirectories.Count > 1)
                {
                    // FragPipe v20 creates file experiment_annotation.tsv, listing .mzML files and the sample (aka experiment group) that each is associated with
                    // It is unknown which tool uses the experiment_annotation.tsv file, but it doesn't hurt to create it

                    CreateExperimentAnnotationFile(dataPackageInfo, datasetIDsByExperimentGroup);
                }

                if (options.OpenSearch)
                {
                    var crystalCSuccess = RunCrystalC(dataPackageInfo, datasetIDsByExperimentGroup, options);

                    if (!crystalCSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.CrystalCComplete;
                }

                bool psmValidationSuccess;

                // This tracks the files created by either Peptide Prophet or Percolator
                // Example filename: interact-DatasetName.pep.xml
                List<FileInfo> peptideProphetPepXmlFiles;

                var databaseSplitCount = mJobParams.GetJobParameter("MSFragger", "DatabaseSplitCount", 1);

                if (databaseSplitCount > 1 && options.MS1ValidationMode == MS1ValidationModes.Percolator)
                {
                    // Split FASTA search
                    // Cannot run Percolator since we don't have .pin files
                    options.FraggerOptions.MS1ValidationMode = MS1ValidationModes.PeptideProphet;

                    if (options.RunMSBooster)
                    {
                        LogWarning("Disabling MSBooster since the FASTA file was split into multiple parts and we're thus using Peptide Prophet");
                        options.RunMSBooster = false;
                    }
                }

                if (options.RunMSBooster)
                {
                    if (options.ReporterIonMode != ReporterIonModes.Disabled)
                    {
                        if (options.ReporterIonMode is ReporterIonModes.Tmt6 or ReporterIonModes.Tmt10 or ReporterIonModes.Tmt11)
                        {
                            LogMessage("Running MSBooster since it supports reporter ion mode {0}", options.ReporterIonMode);
                        }
                        else
                        {
                            LogMessage("Disabling MSBooster since the reporter ion mode is {0}", options.ReporterIonMode);
                            options.RunMSBooster = false;
                        }
                    }
                    else if (options.FraggerOptions.OpenSearch)
                    {
                        LogMessage("Disabling MSBooster since running an open search");
                        options.RunMSBooster = false;
                    }

                    if (options.RunMSBooster)
                    {
                        // Run DIA-NN (aka MSBooster)
                        var msBoosterSuccess = RunMSBooster(dataPackageInfo, datasetIDsByExperimentGroup, options, paramFilePath);

                        if (!msBoosterSuccess)
                            return CloseOutType.CLOSEOUT_FAILED;

                        mProgress = (int)ProgressPercentValues.MSBoosterComplete;

                        // Move the retention time plot files to the working directory
                        MoveRetentionTimePlotFiles();
                    }
                }

                if (options.MS1ValidationMode == MS1ValidationModes.PeptideProphet)
                {
                    // Run PeptideProphet
                    psmValidationSuccess = RunPeptideProphet(
                        dataPackageInfo,
                        datasetIDsByExperimentGroup,
                        options,
                        out peptideProphetPepXmlFiles);
                }
                else if (options.MS1ValidationMode == MS1ValidationModes.Percolator)
                {
                    // Run Percolator
                    psmValidationSuccess = RunPercolator(
                        dataPackageInfo,
                        datasetIDsByExperimentGroup,
                        options,
                        out peptideProphetPepXmlFiles);
                }
                else
                {
                    peptideProphetPepXmlFiles = new List<FileInfo>();
                    psmValidationSuccess = true;
                }

                if (!psmValidationSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.PeptideProphetOrPercolatorComplete;

                if (options.RunPTMProphet)
                {
                    // By default, PTM Prophet is run when MSFragger searched for iTRAQ or TMT labeled peptides and also looked for phospho STY
                    // Additionally, PTM Prophet is used when performing an Open Search
                    // However, if job parameter RunPTMProphet is false, options.RunPTMProphet will be false

                    var ptmProphetSuccess = RunPTMProphet(peptideProphetPepXmlFiles, options);

                    if (!ptmProphetSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.PTMProphetComplete;
                }

                bool usedProteinProphet;

                if (peptideProphetPepXmlFiles.Count > 0 && options.RunProteinProphet)
                {
                    // Run ProteinProphet
                    var proteinProphetSuccess = RunProteinProphet(peptideProphetPepXmlFiles, options);

                    if (!proteinProphetSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.ProteinProphetComplete;

                    usedProteinProphet = true;
                }
                else
                {
                    usedProteinProphet = false;
                }

                var dbAnnotateSuccess = RunDatabaseAnnotation(options);

                if (!dbAnnotateSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.DBAnnotationComplete;

                if (peptideProphetPepXmlFiles.Count > 0)
                {
                    var filterSuccess = RunResultsFilter(options, usedProteinProphet);

                    if (!filterSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                mProgress = (int)ProgressPercentValues.ResultsFilterComplete;

                if (options.ReporterIonMode != ReporterIonModes.Disabled)
                {
                    LogMessage("Based on static and/or dynamic mods, the reporter ion mode is {0}", options.ReporterIonMode);
                }

                var ms1QuantDisabled = mJobParams.GetJobParameter("MS1QuantDisabled", false);

                if (!ms1QuantDisabled && (options.ReporterIonMode != ReporterIonModes.Disabled || options.RunFreeQuant && !options.RunIonQuant))
                {
                    // Always run FreeQuant when we have reporter ions
                    // If no reporter ions, either run FreeQuant or run IonQuant

                    var freeQuantSuccess = RunFreeQuant(options);

                    if (!freeQuantSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.FreeQuantOrLabelQuantComplete;
                }

                if (options.ReporterIonMode != ReporterIonModes.Disabled)
                {
                    if (!options.RunLabelQuant)
                    {
                        LogMessage("Skipped LabelQuant and TMT-Integrator since job parameter RunLabelQuant is false");
                        UpdateStatusMessage("Skipped LabelQuant and TMT-Integrator");
                    }
                    else
                    {
                        var labelQuantSuccess = RunLabelQuant(options);

                        if (!labelQuantSuccess)
                            return CloseOutType.CLOSEOUT_FAILED;

                        mProgress = (int)ProgressPercentValues.FreeQuantOrLabelQuantComplete;
                    }
                }

                bool reportFilesCreated;

                if (peptideProphetPepXmlFiles.Count == 0)
                {
                    reportFilesCreated = false;
                }
                else
                {
                    var reportSuccess = RunReportGeneration(options);

                    if (!reportSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    reportFilesCreated = true;
                }

                mProgress = (int)ProgressPercentValues.ReportGenerated;

                if (mExperimentGroupWorkingDirectories.Count <= 1)
                {
                    if (dataPackageInfo.DataPackageID > 0 && (options.RunIProphet || options.RunAbacus))
                    {
                        string skipList;

                        // ReSharper disable once ConvertIfStatementToSwitchStatement
                        // ReSharper disable once ConvertIfStatementToSwitchExpression
                        if (options.RunIProphet && options.RunAbacus)
                            skipList = "iProphet and Abacus";
                        else if (options.RunIProphet && !options.RunAbacus)
                            skipList = "iProphet";
                        else
                            skipList = "Abacus";

                        // Skipping iProphet and Abacus since data package 1234 does not contain two or more experiment group names; see https://prismwiki.pnl.gov/wiki/MSFragger_Experiment_Groups
                        // Skipping iProphet since data package ...
                        // Skipping Abacus since data package ...

                        var msg = string.Format(
                            "Skipping {0} since data package {1} does not contain two or more experiment group names; see {2}",
                            skipList,
                            dataPackageInfo.DataPackageID,
                            "https://prismwiki.pnl.gov/wiki/MSFragger_Experiment_Groups");

                        LogMessage(msg);

                        UpdateStatusMessage(msg);
                    }
                }
                else
                {
                    if (options.RunIProphet)
                    {
                        // Note that FragPipe v20 does not run iProphet when "Generate peptide-level summary" is checked; this may or may not be correct behavior

                        var iProphetSuccess = RunIProphet(dataPackageInfo, datasetIDsByExperimentGroup, options);

                        if (!iProphetSuccess)
                            return CloseOutType.CLOSEOUT_FAILED;

                        mProgress = (int)ProgressPercentValues.IProphetComplete;
                    }

                    // FragPipe v20 does not appear to run Abacus, so the following is commented out

                    // Only run Abacus if Protein Prophet was used
                    // if (options.RunAbacus && options.RunProteinProphet)
                    // {
                    //     var abacusSuccess = RunAbacus(options);

                    //     if (!abacusSuccess) {
                    //         return CloseOutType.CLOSEOUT_FAILED;
                    //     }

                    //     mProgress = (int)ProgressPercentValues.AbacusComplete;
                    // }
                }

                if (options.RunIonQuant && !ms1QuantDisabled)
                {
                    var ionQuantSuccess = RunIonQuant(datasetIDsByExperimentGroup, options);

                    if (!ionQuantSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.IonQuantComplete;
                }

                // Only run TMT-Integrator if Protein Prophet was used, since TMT-Integrator expects that file combined.prot.xml exists
                if (options.RunLabelQuant && options.ReporterIonMode != ReporterIonModes.Disabled && options.RunProteinProphet)
                {
                    var tmtIntegratorSuccess = RunTmtIntegrator(options);

                    if (!tmtIntegratorSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.TmtIntegratorComplete;
                }

                if (options.OpenSearch && options.RunPTMShepherd)
                {
                    var ptmShepherdSuccess = RunPTMShepherd(options);

                    if (!ptmShepherdSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.PtmShepherdComplete;
                }

                if (reportFilesCreated)
                {
                    var reportFilesUpdated = UpdatePhilosopherReportFiles(usedProteinProphet);

                    if (!reportFilesUpdated)
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                mProgress = (int)ProgressPercentValues.ReportFilesUpdated;

                var moveSuccess = MoveResultsOutOfSubdirectories(dataPackageInfo, datasetIDsByExperimentGroup);

                if (!moveSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                var zipSuccessPepXml = ZipPepXmlAndPinFiles(dataPackageInfo, options);

                if (!zipSuccessPepXml)
                    return CloseOutType.CLOSEOUT_FAILED;

                // Zip the _ion.tsv, _peptide.tsv, and _protein.tsv files created for each experiment group,
                // but only if there are more than three experiment groups
                var zipSuccessPsmTsv = ZipPsmTsvFiles(usedProteinProphet);

                return zipSuccessPsmTsv ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error in ExecuteWorkflow", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Convert the output from Percolator to .pep.xml
        /// </summary>
        /// <param name="fragPipeLibDirectory">Frag pipe library directory</param>
        /// <param name="experimentGroupDirectory">Experiment group directory</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="options">Processing options</param>
        /// <param name="pepXmlFiles">.pepXML files</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ConvertPercolatorOutputToPepXML(
            FileSystemInfo fragPipeLibDirectory,
            DirectoryInfo experimentGroupDirectory,
            string datasetName,
            FragPipeOptions options,
            out List<FileInfo> pepXmlFiles)
        {
            try
            {
                // ReSharper disable StringLiteralTypo
                // ReSharper disable CommentTypo

                // Percolator: Convert to pepxml
                // Example command line:
                // java -cp C:\DMS_Programs\MSFragger\fragpipe\lib/* com.dmtavt.fragpipe.tools.percolator.PercolatorOutputToPepXML DatasetName.pin DatasetName DatasetName_percolator_target_psms.tsv DatasetName_percolator_decoy_psms.tsv interact-DatasetName DDA 0.5 C:\DMS_WorkDir\DatasetName.mzML

                // ReSharper restore CommentTypo

                var targetPsmFile = GetPercolatorFileName(datasetName, false);
                var decoyPsmFile = GetPercolatorFileName(datasetName, true);
                var pinFile = string.Format("{0}.pin", datasetName);
                var mzMLFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, string.Format("{0}{1}", datasetName, AnalysisResources.DOT_MZML_EXTENSION)));

                string dataMode;
                string probabilityThreshold;

                if (options.FraggerOptions.DIASearchEnabled)
                {
                    dataMode = "DIA";
                    probabilityThreshold = "0.5";
                }
                else
                {
                    dataMode = "DDA";
                    probabilityThreshold = "0.5";
                }

                var arguments = string.Format(
                    "-cp {0}/* com.dmtavt.fragpipe.tools.percolator.PercolatorOutputToPepXML " +
                    "{1} " +            // DatasetName.pin
                    "{2} " +            // DatasetName
                    "{3} " +            // DatasetName_percolator_target_psms.tsv
                    "{4} " +            // DatasetName_percolator_decoy_psms.tsv
                    "interact-{5} " +   // interact-DatasetName
                    "{6} " +            // DDA or DIA
                    "{7} " +            // Minimum probability threshold
                    "{8}",              // C:\DMS_WorkDir\DatasetName.mzML
                    fragPipeLibDirectory.FullName,
                    pinFile,
                    datasetName,
                    targetPsmFile,
                    decoyPsmFile,
                    datasetName,
                    dataMode,
                    probabilityThreshold,
                    mzMLFile.FullName);

                // ReSharper restore StringLiteralTypo

                InitializeCommandRunner(
                    experimentGroupDirectory,
                    Path.Combine(mWorkingDirectory.FullName, JAVA_CONSOLE_OUTPUT),
                    CmdRunnerModes.PercolatorOutputToPepXml,
                    500);

                LogCommandToExecute(experimentGroupDirectory, options.JavaProgLoc, arguments, options.WorkingDirectoryPadWidth);

                // Start the program and wait for it to finish
                // However, while it's running, LoopWaiting will get called via events
                var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments, "Java", true);

                // Note that fragpipe.tools.percolator.PercolatorOutputToPepXML writes various status messages to the error stream
                // The following messages are not actually errors:

                // Console error: Iteration 7:     Estimated 6315 PSMs with q<0.01
                // Learned normalized SVM weights for the 3 cross-validation splits:
                // Split1   Split2  Split3 FeatureName
                // 0.0000   0.0000  0.0000 rank
                // 0.4576  -0.6424 -0.5811 abs_ppm
                // 0.0837  -0.2854 -0.1592 isotope_errors
                // 0.5453  -0.3633 -0.3602 log10_evalue
                // 0.0867  -0.1044 -0.5247 hyperscore
                // 1.0133   0.9964  1.1500 delta_hyperscore
                // 0.7742   0.5430  1.6079 matched_ion_num
                // ...
                // Found 6154 test set PSMs with q<0.01.
                // Selected best-scoring PSM per scan+expMass (target-decoy competition): 19345 target PSMs and 11905 decoy PSMs.
                // Calculating q values.
                // Final list yields 6170 target PSMs with q<0.01.
                // Calculating posterior error probabilities (PEPs).
                // Processing took 3.6920 cpu seconds or 4 seconds wall clock time.

                if (!mConsoleOutputFileParsed)
                {
                    ParseConsoleOutputFile();
                }

                if (!string.IsNullOrEmpty(mConsoleOutputFileParser.ConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputFileParser.ConsoleOutputErrorMsg);
                }

                var currentStep = "PercolatorOutputToPepXML for " + datasetName;
                UpdateCombinedJavaConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, currentStep);

                var pepXmlFile = new FileInfo(
                    Path.Combine(experimentGroupDirectory.FullName, string.Format("interact-{0}{1}.pep.xml",
                        datasetName,
                        options.FraggerOptions.DIASearchEnabled ? "_rank1" : string.Empty
                        )));

                if (processingSuccess)
                {
                    pepXmlFiles = new List<FileInfo>();

                    // Verify that Percolator created the .pep.xml file
                    if (!pepXmlFile.Exists)
                    {
                        LogError("PercolatorOutputToPepXML did not create file " + pepXmlFile.Name);
                        pepXmlFiles.Add(pepXmlFile);
                        return false;
                    }

                    // With DDA results there will be only one interact .pep.xml file
                    // For DIA there will be several
                    // This for loop works for both, populating the pepXmlFiles output parameter

                    foreach (var item in experimentGroupDirectory.GetFiles(string.Format("interact-{0}*.pep.xml", datasetName)))
                    {
                        pepXmlFiles.Add(item);
                        mJobParams.AddResultFileToSkip(item.Name);
                    }

                    return true;
                }

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("Java returned a non-zero exit code while calling PercolatorOutputToPepXML: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to Java failed while calling PercolatorOutputToPepXML on interact.pep.xml (but exit code is 0)");
                }

                pepXmlFiles = new List<FileInfo> { pepXmlFile };

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in ConvertPercolatorOutputToPepXML", ex);

                pepXmlFiles = new List<FileInfo>
                {
                    new (string.Format("interact-{0}.pep.xml", datasetName))
                };

                return false;
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

        private bool CreateCrystalCParamFile(FileSystemInfo experimentGroupDirectory, string datasetName, out FileInfo crystalcParamFile)
        {
            // Future: Possibly customize this
            const int CRYSTALC_THREAD_COUNT = 4;

            try
            {
                var paramFileName = string.Format("crystalc-0-{0}.pepXML.params", datasetName);
                crystalcParamFile = new FileInfo(Path.Combine(experimentGroupDirectory.FullName, paramFileName));

                using var writer = new StreamWriter(new FileStream(crystalcParamFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine("# Crystal-C (Version: 2019.08)");
                writer.WriteLine();
                writer.WriteLine("thread = {0}", CRYSTALC_THREAD_COUNT);
                writer.WriteLine("fasta = {0}", mFastaFilePath);
                writer.WriteLine("raw_file_location = {0}", mWorkingDirectory.FullName);
                writer.WriteLine("raw_file_extension = mzML");
                writer.WriteLine("output_location = {0}", experimentGroupDirectory.FullName);
                writer.WriteLine();
                writer.WriteLine("precursor_charge = 1 6             # precursor charge range for detecting chimeric spectra");
                writer.WriteLine("isotope_number = 3                 # number of theoretical isotope peaks");
                writer.WriteLine("precursor_mass = 20                # precursor mass tolerance (unit: ppm)");
                writer.WriteLine("precursor_isolation_window = 0.7   # precursor isolation window");
                writer.WriteLine("correct_isotope_error = false      # correct isotope error by updating precursor neutral mass with the monoisotopic mass");
                writer.WriteLine();

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in CreateCrystalCParamFile", ex);
                crystalcParamFile = new FileInfo("NonExistentFile.params");
                return false;
            }
        }

        // ReSharper disable once CommentTypo

        /// <summary>
        /// Create the file listing each dataset's .mzML file and the associated experiment group name
        /// </summary>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="datasetIDsByExperimentGroup">Keys are experiment group name, values are lists of dataset IDs</param>
        private void CreateExperimentAnnotationFile(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup
            )
        {
            var experimentAnnotationFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "experiment_annotation.tsv"));

            using var writer = new StreamWriter(new FileStream(experimentAnnotationFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

            // Initially populate this list with the header names
            // Later, use it for the values to write for each dataset
            var dataValues = new List<string>
            {
                "file",
                "sample",
                "sample_name",
                "condition",
                "replicate"
            };

            // Header line
            writer.WriteLine(string.Join("\t", dataValues));

            foreach (var item in datasetIDsByExperimentGroup)
            {
                var experimentGroupName = item.Key;

                // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                foreach (var datasetId in item.Value)
                {
                    var datasetName = dataPackageInfo.Datasets[datasetId];
                    var mzMLFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, string.Format("{0}{1}", datasetName, AnalysisResources.DOT_MZML_EXTENSION)));

                    dataValues.Clear();
                    dataValues.Add(mzMLFile.FullName);
                    dataValues.Add(experimentGroupName);
                    dataValues.Add(experimentGroupName);
                    dataValues.Add(experimentGroupName);
                    dataValues.Add("1");

                    writer.WriteLine(string.Join("\t", dataValues));
                }
            }
        }

        /// <summary>
        /// Create the file listing the psm.tsv files for IonQuant to process
        /// </summary>
        /// <param name="datasetIDsByExperimentGroup">Keys are experiment group name, values are lists of dataset IDs</param>
        /// <param name="datasetCount">Dataset count</param>
        /// <returns>File list file</returns>
        private FileInfo CreateIonQuantFileListFile(
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            out int datasetCount
            )
        {
            datasetCount = 0;

            var fileListFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "filelist_ionquant.txt"));

            using var writer = new StreamWriter(new FileStream(fileListFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

            // Header line
            writer.WriteLine("flag{0}value", '\t');

            if (mExperimentGroupWorkingDirectories.Count <= 1)
            {
                writer.WriteLine("--psm\tpsm.tsv");
            }
            else
            {
                foreach (var experimentGroupWorkingDirectory in mExperimentGroupWorkingDirectories.Values)
                {
                    writer.WriteLine("--psm\t{0}", Path.Combine(experimentGroupWorkingDirectory.Name, "psm.tsv"));
                }
            }

            // ReSharper disable once StringLiteralTypo
            writer.WriteLine("--specdir\t{0}", mWorkingDirectory.FullName);

            // Prior to FragPipe v20, we also added --pepxml lines, as shown below

            // The "--pepxml" lines referenced experiment group's .pepXML file
            // FragPipe v20 included the "--pepxml" lines in the IonQuant file list file, but v21 does not, since IonQuant v1.10.12 reports an error if the --pepxml lines are included

            // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
            foreach (var item in datasetIDsByExperimentGroup)
            {
                datasetCount += item.Value.Count;

                // The following was deprecated with FragPipe v21 and is thus commented out

                /*

                var experimentGroupName = item.Key;
                var experimentWorkingDirectory = mExperimentGroupWorkingDirectories[experimentGroupName];

                foreach (var datasetId in item.Value)
                {
                    var datasetName = dataPackageInfo.Datasets[datasetId];

                    if (mExperimentGroupWorkingDirectories.Count <= 1)
                    {
                        writer.WriteLine("--pepxml\t{0}", datasetName + ".pepXML");
                    }
                    else
                    {
                        writer.WriteLine("--pepxml\t{0}", Path.Combine(experimentWorkingDirectory.Name, datasetName + ".pepXML"));
                    }
                }

                */
            }

            return fileListFile;
        }

        /// <summary>
        /// Create a text file listing modification masses (both static and dynamic modifications)
        /// </summary>
        /// <param name="options">Processing options</param>
        /// <returns>Mod masses file</returns>
        private FileInfo CreateIonQuantModMassesFile(FragPipeOptions options)
        {
            // Construct a unique list of modification masses
            var modificationMasses = new SortedSet<double>();

            AppendModificationMasses(modificationMasses, options.FraggerOptions.StaticModifications);
            AppendModificationMasses(modificationMasses, options.FraggerOptions.VariableModifications);

            var modMassesFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "modmasses_ionquant.txt"));

            using var writer = new StreamWriter(new FileStream(modMassesFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

            // Write the static and dynamic mod masses to the file
            foreach (var modMass in modificationMasses)
            {
                writer.WriteLine(modMass);
            }

            // ReSharper disable CommentTypo

            // Example message:
            // Created file modmasses_ionquant.txt; mass list: 15.9949, 42.0106, 57.02146

            if (modificationMasses.Count == 0)
                LogMessage("Created empty file {0} for IonQuant since there are no static or dynamic modifications", modMassesFile.Name);
            else
                LogMessage("Created file {0} for IonQuant; mass list: {1}", modMassesFile.Name, string.Join(", ", modificationMasses));

            // ReSharper restore CommentTypo

            return modMassesFile;
        }

        /// <summary>
        /// Create the MSBooster parameter file
        /// </summary>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="datasetIDsByExperimentGroup">Keys are experiment group name, values are lists of dataset IDs</param>
        /// <param name="paramFilePath">Parameter file path</param>
        /// <param name="msBoosterParamFile">MSBooster parameter file</param>
        /// <returns>True if successful, false if error</returns>
        private bool CreateMSBoosterParamFile(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            string paramFilePath,
            out FileInfo msBoosterParamFile)
        {
            // Future: Possibly customize this
            const int MSBOOSTER_THREAD_COUNT = 4;

            try
            {
                // This list tracks the .pin file for each dataset (as a relative path)
                // GroupA\Dataset1.pin
                // GroupB\Dataset2.pin
                // GroupB\Dataset3.pin
                var pinFiles = new List<string>();

                // ReSharper disable ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator

                foreach (var item in datasetIDsByExperimentGroup)
                {
                    var experimentGroupName = item.Key;

                    foreach (var datasetId in item.Value)
                    {
                        var datasetName = dataPackageInfo.Datasets[datasetId];

                        if (datasetIDsByExperimentGroup.Count == 1)
                            pinFiles.Add(datasetName + ".pin");
                        else
                            pinFiles.Add(Path.Combine(experimentGroupName, datasetName + ".pin"));
                    }
                }

                // ReSharper restore ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator

                msBoosterParamFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "msbooster_params.txt"));

                using var writer = new StreamWriter(new FileStream(msBoosterParamFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine("useDetect = false");
                writer.WriteLine("numThreads = {0}", MSBOOSTER_THREAD_COUNT);
                writer.WriteLine("DiaNN = {0}", mDiaNNProgLoc);
                writer.WriteLine("renamePin = 1");
                writer.WriteLine("useRT = true");
                writer.WriteLine("useSpectra = true");
                writer.WriteLine("fragger = {0}", paramFilePath);

                // ReSharper disable once StringLiteralTypo
                writer.WriteLine("deletePreds = false");

                writer.WriteLine("mzmlDirectory = {0}", mWorkingDirectory.FullName);
                writer.WriteLine("pinPepXMLDirectory = {0}", string.Join(" ", pinFiles));
                writer.WriteLine("useMultipleCorrelatedFeatures = false");

                writer.WriteLine();

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in CreateMSBoosterParamFile", ex);
                msBoosterParamFile = new FileInfo("NonExistentFile.params");
                return false;
            }
        }

        private FileInfo CreateReporterIonAliasNameFile(ReporterIonModes reporterIonMode, FileInfo aliasNameFile, string sampleNamePrefix)
        {
            try
            {
                using var writer = new StreamWriter(new FileStream(aliasNameFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                var reporterIonNames = GetReporterIonNames(reporterIonMode);

                // Example output (space-delimited):
                // 126 sample-01
                // 127N sample-02
                // 127C sample-03
                // 128N sample-04

                // If sampleNamePrefix is not an empty string, it will be included before each sample name, e.g.
                // 126 QC_Shew_20_01_R01_sample-01
                // 127N QC_Shew_20_01_R01_sample-02
                // 127C QC_Shew_20_01_R01_sample-03
                // 128N QC_Shew_20_01_R01_sample-04

                if (string.IsNullOrWhiteSpace(sampleNamePrefix))
                {
                    sampleNamePrefix = string.Empty;
                }
                else if (!sampleNamePrefix.EndsWith("_"))
                {
                    sampleNamePrefix += "_";
                }

                var sampleNumber = 0;

                foreach (var reporterIon in reporterIonNames)
                {
                    sampleNumber++;
                    writer.WriteLine("{0} {1}sample-{2:D2}", reporterIon, sampleNamePrefix, sampleNumber);
                }

                aliasNameFile.Refresh();
                return aliasNameFile;
            }
            catch (Exception ex)
            {
                LogError("Error in CreateReporterIonAliasNameFile", ex);
                return null;
            }
        }

        /// <summary>
        /// Delete temporary directories, ignoring errors
        /// </summary>
        /// <param name="directoriesToDelete">Directories to delete</param>
        private void DeleteTempDirectories(IEnumerable<DirectoryInfo> directoriesToDelete)
        {
            // Delete the workspace directories
            foreach (var directory in directoriesToDelete)
            {
                try
                {
                    directory.Delete(true);
                }
                catch (Exception ex)
                {
                    LogWarning("Error deleting directory {0}: {1}", directory.FullName, ex.Message);
                }
            }
        }

        /// <summary>
        /// Obtain a dictionary mapping the experiment group names to abbreviated versions, assuring that each abbreviation is unique
        /// </summary>
        /// <remarks>If there is only one experiment group, the dictionary will have an empty string for the abbreviated name</remarks>
        /// <param name="experimentGroupNames">Experiment group name</param>
        /// <returns>Dictionary mapping experiment group name to abbreviated name</returns>
        private Dictionary<string, string> GetAbbreviatedExperimentGroupNames(IReadOnlyList<string> experimentGroupNames)
        {
            var experimentGroupNameMap = new Dictionary<string, string>();

            switch (experimentGroupNames.Count)
            {
                case 0:
                    return experimentGroupNameMap;

                case 1:
                    experimentGroupNameMap.Add(experimentGroupNames[0], string.Empty);
                    return experimentGroupNameMap;
            }

            var uniquePrefixTool = new ShortestUniquePrefix();

            // Keys in this dictionary are experiment group names
            // Values are the abbreviated name to use
            return uniquePrefixTool.GetShortestUniquePrefix(experimentGroupNames, true);
        }

        internal static string GetCurrentPhilosopherToolDescription(PhilosopherToolType currentTool)
        {
            return currentTool switch
            {
                PhilosopherToolType.Undefined => "Undefined",
                PhilosopherToolType.ShowVersion => "Get Version",
                PhilosopherToolType.WorkspaceManager => "Workspace Manager",
                PhilosopherToolType.PeptideProphet => "PeptideProphet",
                PhilosopherToolType.ProteinProphet => "ProteinProphet",
                PhilosopherToolType.AnnotateDatabase => "Annotate Database",
                PhilosopherToolType.ResultsFilter => "Results Filter",
                PhilosopherToolType.FreeQuant => "FreeQuant",
                PhilosopherToolType.LabelQuant => "LabelQuant",
                PhilosopherToolType.GenerateReport => "Generate Report",
                PhilosopherToolType.IProphet => "iProphet",
                PhilosopherToolType.Abacus => "Abacus",
                _ => throw new ArgumentOutOfRangeException()
            };
        }

        /// <summary>
        /// Get appropriate path of the working directory for the given experiment
        /// </summary>
        /// <remarks>
        /// <para>If all the datasets belong to the same experiment, return the job's working directory</para>
        /// <para>Otherwise, return a subdirectory below the working directory, based on the experiment's name</para>
        /// </remarks>
        /// <param name="experimentGroupName">Experiment group name</param>
        /// <param name="experimentGroupCount">Experiment group count</param>
        private DirectoryInfo GetExperimentGroupWorkingDirectory(string experimentGroupName, int experimentGroupCount)
        {
            if (experimentGroupCount <= 1)
                return mWorkingDirectory;

            var cleanName = Global.ReplaceInvalidPathChars(experimentGroupName);

            return new DirectoryInfo(Path.Combine(mWorkingDirectory.FullName, cleanName));
        }

        private float GetJobParameterOrDefault(string sectionName, string parameterName, float valueIfMissing, out bool jobParameterExists)
        {
            if (!mJobParams.TryGetParam(sectionName, parameterName, out _, false))
            {
                jobParameterExists = false;
                return valueIfMissing;
            }

            jobParameterExists = true;
            return mJobParams.GetJobParameter(sectionName, parameterName, valueIfMissing);
        }

        private int GetLongestWorkingDirectoryName()
        {
            return GetLongestWorkingDirectoryName(mExperimentGroupWorkingDirectories.Values.ToList());
        }

        private static int GetLongestWorkingDirectoryName(IReadOnlyCollection<DirectoryInfo> workingDirectories)
        {
            return workingDirectories.Count == 0
                ? 0
                : workingDirectories.Max(item => item.FullName.Length);
        }

        /// <summary>
        /// Get the interval, in milliseconds, for monitoring programs run via <see cref="mCmdRunner"/>
        /// </summary>
        /// <remarks>
        /// Monitor every 500 msec if determining the version, managing the workspace, or generating the report
        /// Otherwise, monitor every 2000 msec
        /// </remarks>
        /// <param name="toolType">Tool type</param>
        private int GetMonitoringInterval(PhilosopherToolType toolType)
        {
            return toolType is PhilosopherToolType.ShowVersion or PhilosopherToolType.WorkspaceManager or PhilosopherToolType.GenerateReport
                ? 500
                : DEFAULT_MONITOR_INTERVAL_MSEC;
        }

        /// <summary>
        /// Get a file named DatasetName_percolator_target_psms.tsv or DatasetName_percolator_decoy_psms.tsv
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="isDecoy">True if the file should be named decoy_psms.tsv</param>
        private string GetPercolatorFileName(string datasetName, bool isDecoy)
        {
            // ReSharper disable once StringLiteralTypo
            return string.Format("{0}_percolator_{1}_psms.tsv", datasetName, isDecoy ? "decoy" : "target");
        }

        private byte GetReporterIonChannelCount(ReporterIonModes reporterIonMode)
        {
            return reporterIonMode switch
            {
                ReporterIonModes.Itraq4 => 4,
                ReporterIonModes.Itraq8 => 8,
                ReporterIonModes.Tmt6 => 6,
                ReporterIonModes.Tmt10 => 10,
                ReporterIonModes.Tmt11 => 11,
                ReporterIonModes.Tmt16 => 16,
                ReporterIonModes.Tmt18 => 18,
                ReporterIonModes.Disabled => 0,
                _ => throw new ArgumentOutOfRangeException()
            };
        }

        private IEnumerable GetReporterIonNames(ReporterIonModes reporterIonMode)
        {
            var reporterIonNames = new List<string>();

            switch (reporterIonMode)
            {
                case ReporterIonModes.Tmt6:
                    reporterIonNames.Add("126");
                    reporterIonNames.Add("127N");
                    reporterIonNames.Add("128C");
                    reporterIonNames.Add("129N");
                    reporterIonNames.Add("130C");
                    reporterIonNames.Add("131");
                    return reporterIonNames;

                case ReporterIonModes.Tmt10 or ReporterIonModes.Tmt11 or ReporterIonModes.Tmt16 or ReporterIonModes.Tmt18:
                    reporterIonNames.Add("126");
                    reporterIonNames.Add("127N");
                    reporterIonNames.Add("127C");
                    reporterIonNames.Add("128N");
                    reporterIonNames.Add("128C");
                    reporterIonNames.Add("129N");
                    reporterIonNames.Add("129C");
                    reporterIonNames.Add("130N");
                    reporterIonNames.Add("130C");
                    reporterIonNames.Add("131N");

                    if (reporterIonMode == ReporterIonModes.Tmt10)
                        return reporterIonNames;

                    // TMT 11, TMT 16, and TMT 18
                    reporterIonNames.Add("131C");

                    if (reporterIonMode == ReporterIonModes.Tmt11)
                        return reporterIonNames;

                    // TMT 16 and TMT 18
                    reporterIonNames.Add("132N");
                    reporterIonNames.Add("132C");
                    reporterIonNames.Add("133N");
                    reporterIonNames.Add("133C");
                    reporterIonNames.Add("134N");

                    if (reporterIonMode == ReporterIonModes.Tmt16)
                        return reporterIonNames;

                    // TMT 18
                    reporterIonNames.Add("134C");
                    reporterIonNames.Add("135N");

                    return reporterIonNames;
            }

            if (reporterIonMode != ReporterIonModes.Itraq4 && reporterIonMode != ReporterIonModes.Itraq8)
            {
                LogWarning("Unrecognized reporter ion mode in GetReporterIonNames: " + reporterIonMode);
                return reporterIonNames;
            }

            if (reporterIonMode == ReporterIonModes.Itraq8)
            {
                // 8-plex iTRAQ
                reporterIonNames.Add("113");
            }

            if (reporterIonMode is ReporterIonModes.Itraq4 or ReporterIonModes.Itraq8)
            {
                // 4-plex and 8-plex iTRAQ
                reporterIonNames.Add("114");
                reporterIonNames.Add("115");
                reporterIonNames.Add("116");
                reporterIonNames.Add("117");
            }

            if (reporterIonMode != ReporterIonModes.Itraq8)
            {
                return reporterIonNames;
            }

            // 8-plex iTRAQ
            reporterIonNames.Add("118");
            reporterIonNames.Add("119");
            reporterIonNames.Add("121");

            return reporterIonNames;
        }

        /// <summary>
        /// Initialize the runtime values for the command runner
        /// </summary>
        /// <remarks>This is called before starting each new external process</remarks>
        /// <param name="workingDirectory">Working directory</param>
        /// <param name="consoleOutputFilePath">Console output file</param>
        /// <param name="cmdRunnerMode">Command runner mode</param>
        /// <param name="monitorInterval">Monitoring interval, in milliseconds</param>
        private void InitializeCommandRunner(
            FileSystemInfo workingDirectory,
            string consoleOutputFilePath,
            CmdRunnerModes cmdRunnerMode,
            int monitorInterval = DEFAULT_MONITOR_INTERVAL_MSEC)
        {
            mConsoleOutputFileParsed = false;

            mCmdRunner.WorkDir = workingDirectory.FullName;
            mCmdRunner.ConsoleOutputFilePath = consoleOutputFilePath;
            mCmdRunner.MonitorInterval = monitorInterval;
            mCmdRunnerMode = cmdRunnerMode;
        }

        /// <summary>
        /// Create the temporary directories used by PeptideProphet
        /// </summary>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="datasetIDsByExperimentGroup">Keys are experiment group name, values are lists of dataset IDs</param>
        /// <returns>Dictionary where keys are dataset names and values are DirectoryInfo instances</returns>
        private Dictionary<int, DirectoryInfo> InitializePeptideProphetWorkspaceDirectories(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup)
        {
            var workspaceDirectoryByDatasetId = new Dictionary<int, DirectoryInfo>();

            // Populate a dictionary with the working directories to create
            foreach (var item in datasetIDsByExperimentGroup)
            {
                var experimentGroupName = item.Key;
                var experimentGroupDirectory = mExperimentGroupWorkingDirectories[experimentGroupName];

                // Create a separate temp directory for each dataset
                foreach (var datasetId in item.Value)
                {
                    var datasetName = dataPackageInfo.Datasets[datasetId];

                    var directoryName = string.Format("fragpipe-{0}{1}", datasetName, TEMP_PEP_PROPHET_DIR_SUFFIX);
                    var workingDirectory = new DirectoryInfo(Path.Combine(experimentGroupDirectory.FullName, directoryName));

                    workspaceDirectoryByDatasetId.Add(datasetId, workingDirectory);
                }
            }

            var workingDirectoryPadWidth = workspaceDirectoryByDatasetId.Values.Max(item => item.FullName.Length);

            // Initialize the workspace directories for PeptideProphet (separate subdirectory for each dataset)
            foreach (var workingDirectory in workspaceDirectoryByDatasetId.Values)
            {
                InitializePhilosopherWorkspaceWork(workingDirectory, workingDirectoryPadWidth);
            }

            return workspaceDirectoryByDatasetId;
        }

        /// <summary>
        /// Initialize the Philosopher workspace (creates a hidden directory named .meta)
        /// </summary>
        /// <remarks>Also creates a subdirectory for each experiment group if experimentGroupNames has more than one item</remarks>
        /// <param name="experimentGroupNames">Experiment group names</param>
        /// <param name="workingDirectoryPadWidth">Longest directory path in mExperimentGroupWorkingDirectories</param>
        /// <returns>Success code</returns>
        private CloseOutType InitializePhilosopherWorkspace(
            SortedSet<string> experimentGroupNames,
            out int workingDirectoryPadWidth)
        {
            try
            {
                LogDebug("Initializing the Philosopher Workspace");

                mCurrentPhilosopherTool = PhilosopherToolType.WorkspaceManager;

                var experimentCount = experimentGroupNames.Count;

                // Populate a dictionary with experiment group names and corresponding working directories
                foreach (var experimentGroupName in experimentGroupNames)
                {
                    var workingDirectory = GetExperimentGroupWorkingDirectory(experimentGroupName, experimentCount);

                    mExperimentGroupWorkingDirectories.Add(experimentGroupName, workingDirectory);
                }

                workingDirectoryPadWidth = GetLongestWorkingDirectoryName();

                // Initialize the workspace in the primary working directory
                var workDirSuccess = InitializePhilosopherWorkspaceWork(mWorkingDirectory, workingDirectoryPadWidth, false);

                if (workDirSuccess != CloseOutType.CLOSEOUT_SUCCESS)
                    return workDirSuccess;

                if (experimentCount <= 1)
                    return CloseOutType.CLOSEOUT_SUCCESS;

                // Since we have multiple experiment groups, initialize the workspace for each one
                foreach (var experimentGroupDirectory in mExperimentGroupWorkingDirectories.Values)
                {
                    var success = InitializePhilosopherWorkspaceWork(experimentGroupDirectory, workingDirectoryPadWidth);

                    if (success != CloseOutType.CLOSEOUT_SUCCESS)
                        return success;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in InitializePhilosopherWorkspace", ex);
                workingDirectoryPadWidth = 0;
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType InitializePhilosopherWorkspaceWork(
            DirectoryInfo targetDirectory,
            int workingDirectoryPadWidth,
            bool createDirectoryIfMissing = true)
        {
            try
            {
                const PhilosopherToolType toolType = PhilosopherToolType.WorkspaceManager;

                if (!targetDirectory.Exists)
                {
                    if (!createDirectoryIfMissing)
                    {
                        LogError("Cannot initialize the Philosopher workspace; directory not found: " + targetDirectory.FullName);
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }

                    targetDirectory.Create();
                }

                // ReSharper disable once CommentTypo

                // FragPipe uses the --temp parameter to define the temporary directory; the analysis manager does not include "--temp"
                // philosopher.exe workspace --init --nocheck --temp C:\Users\D3L243\AppData\Local\Temp\c38b98c8-a78b-40ad-bcf5-57e81b91af61

                // ReSharper disable once StringLiteralTypo
                var arguments = "workspace --init --nocheck";

                // Run the workspace init command
                var success = RunPhilosopher(
                    toolType,
                    arguments,
                    "initialize the workspace",
                    targetDirectory,
                    workingDirectoryPadWidth);

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
        /// <remarks>Also looks for job parameters that can be used to enable/disable processing options</remarks>
        /// <param name="philosopherExe">Path to philosopher</param>
        /// <param name="datasetCount">Dataset count</param>
        /// <param name="paramFilePath">Parameter file path</param>
        /// <param name="options">Output: instance of the MSFragger options class</param>
        /// <returns>True if successful, false if error</returns>
        private bool LoadMSFraggerOptions(FileInfo philosopherExe, int datasetCount, string paramFilePath, out FragPipeOptions options)
        {
            options = new FragPipeOptions(mJobParams, philosopherExe, datasetCount);
            RegisterEvents(options);

            try
            {
                // javaProgLoc will typically be "C:\DMS_Programs\Java\jre8\bin\java.exe"
                options.JavaProgLoc = GetJavaProgLoc();

                if (string.IsNullOrWhiteSpace(options.JavaProgLoc))
                {
                    // The error has already been logged
                    return false;
                }

                options.LoadMSFraggerOptions(paramFilePath);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in LoadMSFraggerOptions", ex);
                return false;
            }
        }

        private void LogCommandToExecute(FileSystemInfo workingDirectory, string exePath, string arguments, int workingDirectoryPadWidth = 0)
        {
            LogDebug("[{0}] {1} {2}", workingDirectory.FullName.PadRight(workingDirectoryPadWidth), exePath, arguments);
        }

        private bool MoveFile(string sourceDirectoryPath, string fileName, string targetDirectoryPath)
        {
            try
            {
                var sourceFile = new FileInfo(Path.Combine(sourceDirectoryPath, fileName));

                var targetPath = Path.Combine(targetDirectoryPath, sourceFile.Name);

                sourceFile.MoveTo(targetPath);

                return true;
            }
            catch (Exception ex)
            {
                LogError(string.Format("Error in MoveFile for {0}", fileName), ex);
                return false;
            }
        }

        /// <summary>
        /// Move results into subdirectories, but only if datasetIDsByExperimentGroup has more than one experiment group
        /// </summary>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="datasetIDsByExperimentGroup">Keys are experiment group name, values are lists of dataset IDs</param>
        private bool MoveResultsIntoSubdirectories(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup)
        {
            return MoveResultsToFromSubdirectories(dataPackageInfo, datasetIDsByExperimentGroup, true);
        }

        /// <summary>
        /// Move results out of subdirectories, but only if datasetIDsByExperimentGroup has more than one experiment group
        /// </summary>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="datasetIDsByExperimentGroup">Keys are experiment group name, values are lists of dataset IDs</param>
        private bool MoveResultsOutOfSubdirectories(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup)
        {
            return MoveResultsToFromSubdirectories(dataPackageInfo, datasetIDsByExperimentGroup, false);
        }

        /// <summary>
        /// Move results into or out of subdirectories, but only if datasetIDsByExperimentGroup has more than one experiment group
        /// </summary>
        /// <remarks>
        /// If datasetIDsByExperimentGroup only has one item, no files are moved
        /// </remarks>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="datasetIDsByExperimentGroup">Keys are experiment group name, values are lists of dataset IDs</param>
        /// <param name="sourceIsWorkDirectory">
        /// When true, the source directory is the working directory
        /// When false, the working directory is the target directory
        /// </param>
        private bool MoveResultsToFromSubdirectories(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            bool sourceIsWorkDirectory)
        {
            try
            {
                if (datasetIDsByExperimentGroup.Count <= 1)
                {
                    // Nothing to do
                    return true;
                }

                var databaseSplitCount = mJobParams.GetJobParameter("MSFragger", "DatabaseSplitCount", 1);

                foreach (var item in datasetIDsByExperimentGroup)
                {
                    var experimentGroupName = item.Key;
                    var experimentWorkingDirectory = mExperimentGroupWorkingDirectories[experimentGroupName];

                    // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                    foreach (var datasetId in item.Value)
                    {
                        var datasetName = dataPackageInfo.Datasets[datasetId];

                        string sourceDirectoryPath;
                        string targetDirectoryPath;

                        if (sourceIsWorkDirectory)
                        {
                            sourceDirectoryPath = mWorkingDirectory.FullName;
                            targetDirectoryPath = experimentWorkingDirectory.FullName;
                        }
                        else
                        {
                            sourceDirectoryPath = experimentWorkingDirectory.FullName;
                            targetDirectoryPath = mWorkingDirectory.FullName;
                        }

                        var pepXmlSuccess = MoveFile(sourceDirectoryPath, datasetName + PEPXML_EXTENSION, targetDirectoryPath);

                        if (!pepXmlSuccess)
                            return false;

                        if (databaseSplitCount > 1)
                        {
                            // .pin files are not created for split FASTA MSFragger searches
                            continue;
                        }

                        var pinSuccess = MoveFile(sourceDirectoryPath, datasetName + PIN_EXTENSION, targetDirectoryPath);

                        if (!pinSuccess)
                            return false;

                        var sourceDirectory = new DirectoryInfo(sourceDirectoryPath);
                        var msstatsFiles = sourceDirectory.GetFiles("*msstats.csv");

                        if (msstatsFiles.Length == 0)
                            continue;

                        foreach (var msstatsFile in msstatsFiles)
                        {
                            var msstatsSuccess = MoveFile(sourceDirectoryPath, msstatsFile.Name, targetDirectoryPath);

                            if (!msstatsSuccess)
                                return false;
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in MoveResultsIntoSubdirectories", ex);
                return false;
            }
        }

        // ReSharper disable once CommentTypo

        /// <summary>
        /// Move the retention time .png files to the working directory, renaming to end with _RTplot.png
        /// </summary>
        /// <remarks>
        /// The plots show observed vs. predicted retention time (aka elution time)
        /// </remarks>
        private void MoveRetentionTimePlotFiles()
        {
            try
            {
                foreach (var plotFile in mWorkingDirectory.GetFiles("*.png", SearchOption.AllDirectories))
                {
                    if (plotFile.Directory == null)
                    {
                        plotFile.MoveTo(Path.Combine(mWorkingDirectory.FullName, plotFile.Name));
                        continue;
                    }

                    if (plotFile.Directory.FullName.Equals(mWorkingDirectory.FullName))
                        continue;

                    // ReSharper disable once StringLiteralTypo

                    var targetFileName = plotFile.Directory.Name.Equals("RTPlots", StringComparison.OrdinalIgnoreCase)
                        ? string.Format("{0}_RTplot.png", Path.GetFileNameWithoutExtension(plotFile.Name))
                        : plotFile.Name;

                    plotFile.MoveTo(Path.Combine(mWorkingDirectory.FullName, targetFileName));
                }
            }
            catch (Exception ex)
            {
                LogError("Error in MoveRetentionTimePlotFiles", ex);
            }
        }

        /// <summary>
        /// Organize .pepXML and .pin files and populate several dictionaries
        /// </summary>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="datasetIDsByExperimentGroup">
        /// Keys in this dictionary are experiment group names, values are a list of Dataset IDs for each experiment group
        /// If experiment group names are not defined in the data package, this dictionary will have a single entry named __UNDEFINED_EXPERIMENT_GROUP__
        /// </param>
        /// <param name="workingDirectoryPadWidth">Longest directory path in mExperimentGroupWorkingDirectories</param>
        /// <returns>Result code</returns>
        private CloseOutType OrganizePepXmlAndPinFiles(
            out DataPackageInfo dataPackageInfo,
            out SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            out int workingDirectoryPadWidth)
        {
            // Keys in this dictionary are experiment group names, values are the working directory to use
            mExperimentGroupWorkingDirectories.Clear();

            // If this job applies to a single dataset, dataPackageID will be 0
            // We still need to create an instance of DataPackageInfo to retrieve the experiment name associated with the job's dataset
            var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

            // The constructor for DataPackageInfo reads data package metadata from packed job parameters, which were created by the resource class
            dataPackageInfo = new DataPackageInfo(dataPackageID, this);
            RegisterEvents(dataPackageInfo);

            var dataPackageDatasets = dataPackageInfo.GetDataPackageDatasets();

            datasetIDsByExperimentGroup = DataPackageInfoLoader.GetDataPackageDatasetsByExperimentGroup(dataPackageDatasets);

            // ProteinProphet needs the FASTA file data, and it uses the information in the .pepXML file to find the FASTA file
            // However, when MSFragger was run, the FASTA file was copied to the working directory (so that MSFragger can index it)
            // and thus the .pepXML files have the wrong path to the FASTA file

            // Update the FASTA file paths in the .pepXML files
            var success = UpdateFASTAPathInPepXMLFiles();

            if (!success)
            {
                workingDirectoryPadWidth = 64;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Initialize the Philosopher workspace (creates a hidden directory named .meta)
            // If Experiment Groups are defined, we also create a subdirectory for each experiment group and initialize it

            var experimentGroupNames = new SortedSet<string>();

            foreach (var item in datasetIDsByExperimentGroup.Keys)
            {
                experimentGroupNames.Add(item);
            }

            var initResult = InitializePhilosopherWorkspace(
                experimentGroupNames,
                out workingDirectoryPadWidth);

            if (initResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return initResult;
            }

            if (datasetIDsByExperimentGroup.Count <= 1)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            // Since we have multiple experiment groups, move the pepXML and .pin files into subdirectories
            var moveSuccess = MoveResultsIntoSubdirectories(dataPackageInfo, datasetIDsByExperimentGroup);

            return moveSuccess ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        private void ParseConsoleOutputFile()
        {
            mLastConsoleOutputParse = DateTime.UtcNow;

            switch (mCmdRunnerMode)
            {
                case CmdRunnerModes.CrystalC:
                case CmdRunnerModes.IonQuant:
                case CmdRunnerModes.MSBooster:
                case CmdRunnerModes.PercolatorOutputToPepXml:
                case CmdRunnerModes.TmtIntegrator:
                case CmdRunnerModes.PtmShepherd:
                case CmdRunnerModes.RewritePepXml:
                    mConsoleOutputFileParser.ParseJavaConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, mCmdRunnerMode);
                    break;

                case CmdRunnerModes.Percolator:
                    mConsoleOutputFileParser.ParsePercolatorConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);
                    break;

                case CmdRunnerModes.Philosopher:
                    // Includes DB Annotation, FreeQuant, iProphet, LabelQuant, Peptide Prophet, Protein Prophet, Report Generation and Results Filter
                    mConsoleOutputFileParser.ParsePhilosopherConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, mCurrentPhilosopherTool);
                    break;

                case CmdRunnerModes.PtmProphet:
                    mConsoleOutputFileParser.ParsePTMProphetConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);
                    break;

                case CmdRunnerModes.Undefined:
                    break;

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        /// <summary>
        /// This method looks for phosphorylated S, T, or Y in the dynamic (variable) modifications
        /// </summary>
        /// <param name="variableModifications">Dictionary of variable (dynamic) modifications, by residue or position</param>
        /// <remarks>
        /// <para>Keys in variableModifications are single letter amino acid symbols and values are a list of modifications masses for the amino acid</para>
        /// <para>Keys can alternatively be symbols indicating N or C terminal peptide or protein (e.g., [^ for protein N-terminus or n^ for peptide N-terminus)</para>
        /// </remarks>
        /// <returns>True if phospho S, T, or Y is defined as a dynamic modification</returns>
        private static bool PhosphoModDefined(Dictionary<string, SortedSet<double>> variableModifications)
        {
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var modItem in variableModifications.Where(modItem => modItem.Key.Equals("S") || !modItem.Key.Equals("T") || modItem.Key.Equals("Y")))
            {
                if (modItem.Value.Any(modMass => Math.Abs(79.966331 - modMass) < 0.01))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Rename the TMT Integrator result files to end with _Group1.tsv
        /// </summary>
        /// <param name="groupNumber">Group number</param>
        /// <param name="resultFiles">TMT Integrator result files</param>
        /// <returns>List of the renamed files</returns>
        private List<FileInfo> RenameTmtIntegratorResultFiles(int groupNumber, List<FileInfo> resultFiles)
        {
            var renamedFiles = new List<FileInfo>();

            foreach (var resultFile in resultFiles)
            {
                var newFileName = string.Format("{0}_Group{1}{2}",
                    Path.GetFileNameWithoutExtension(resultFile.Name),
                    groupNumber,
                    Path.GetExtension(resultFile.Name));

                string newPath;

                if (resultFile.DirectoryName == null)
                {
                    LogWarning("Unable to determine the parent directory of result file {0}; will use the working directory instead", resultFile.FullName);
                    newPath = Path.Combine(mWorkDir, newFileName);
                }
                else
                {
                    newPath = Path.Combine(resultFile.DirectoryName, newFileName);
                }

                LogMessage("Renaming TMT Integrator result file {0} to {1}", resultFile.FullName, newFileName);

                try
                {
                    resultFile.MoveTo(newPath);
                    renamedFiles.Add(new FileInfo(newPath));
                }
                catch (Exception ex)
                {
                    LogError("Error renaming TMT Integrator result file to " + newFileName, ex);

                    if (resultFile.Exists)
                    {
                        LogMessage("Copying TMT Integrator result file from {0} to {1}", resultFile.FullName, newPath);
                        resultFile.CopyTo(newPath);
                        renamedFiles.Add(new FileInfo(newPath));
                    }
                    else
                    {
                        LogWarning("Result file not found ({0}); this indicates a programming error", resultFile.FullName);
                    }
                }
            }

            return renamedFiles;
        }

        // ReSharper disable once UnusedMember.Local

        [Obsolete("No longer used by FragPipe v19 or newer")]
        private bool RunAbacus(FragPipeOptions options)
        {
            try
            {
                LogDebug("Running Abacus", 2);

                // Example command line:
                // philosopher.exe abacus --picked --razor --reprint --tag XXX_ --protein --peptide ExperimentGroupA ExperimentGroupB

                var arguments = new StringBuilder();

                // When Match Between Runs or Open Search is not in use:
                // --picked --razor --reprint --tag XXX_ --protein

                // Otherwise, remove --picked, giving:
                // --razor --reprint --tag XXX_ --protein

                arguments.Append("abacus");

                if (!options.MatchBetweenRuns && !options.OpenSearch)
                {
                    arguments.Append(" --picked");
                }

                arguments.Append(" --razor --reprint --tag XXX_");

                var generatePeptideLevelSummary = options.FraggerOptions.GetParameterValueOrDefault("GeneratePeptideLevelSummary", true);
                var generateProteinLevelSummary = options.FraggerOptions.GetParameterValueOrDefault("GenerateProteinLevelSummary", true);

                // ReSharper disable once ConvertIfStatementToSwitchStatement
                if (generatePeptideLevelSummary && !generateProteinLevelSummary)
                {
                    arguments.Append(" --peptide");
                }
                else if (!generatePeptideLevelSummary && generateProteinLevelSummary)
                {
                    arguments.Append(" --protein");
                }
                else
                {
                    // Either both are true or both are false

                    if (!generatePeptideLevelSummary)
                    {
                        // Both are false; options.RunAbacus should be false and this method should not have even been called
                        // Log a warning, and use "--protein --peptide" anyway

                        LogWarning(
                            "Method RunAbacus was called when job parameters GeneratePeptideLevelSummary and GenerateProteinLevelSummary are both false; " +
                            "this indicates a logic bug");
                    }

                    arguments.Append(" --protein --peptide");
                }

                // Version 15 of FragPipe would append --labels if reporter ions were in use
                // This was disabled in version 16 but was re-enabled in v19 when the "--plex" argument was added

                if (options.ReporterIonMode != ReporterIonModes.Disabled)
                {
                    var plex = GetReporterIonChannelCount(options.ReporterIonMode);

                    arguments.Append(" --labels");
                    arguments.AppendFormat(" --plex {0}", plex);
                }

                // Append the experiment group working directory names
                foreach (var experimentGroupDirectory in mExperimentGroupWorkingDirectories.Values)
                {
                    arguments.AppendFormat(" {0}", experimentGroupDirectory.Name);
                }

                var success = RunPhilosopher(PhilosopherToolType.Abacus, arguments.ToString(), "run abacus");

                // Verify that the Abacus result files exists
                var outputFiles = new List<FileInfo>
                {
                    new(Path.Combine(mWorkingDirectory.FullName, "reprint.spc.tsv")),
                    new(Path.Combine(mWorkingDirectory.FullName, "reprint.int.tsv"))
                };

                if (generatePeptideLevelSummary)
                {
                    outputFiles.Add(new FileInfo(Path.Combine(mWorkingDirectory.FullName, "combined_peptide.tsv")));
                }

                if (generateProteinLevelSummary)
                {
                    outputFiles.Add(new FileInfo(Path.Combine(mWorkingDirectory.FullName, "combined_protein.tsv")));
                }

                var outputFilesExist = ValidateOutputFilesExist("Abacus", outputFiles);

                return success && outputFilesExist;
            }
            catch (Exception ex)
            {
                LogError("Error in RunAbacus", ex);
                return false;
            }
        }

        private bool RunCrystalC(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            FragPipeOptions options)
        {
            try
            {
                LogDebug("Running Crystal-C", 2);

                // ReSharper disable CommentTypo
                // ReSharper disable IdentifierTypo

                // Run Crystal-C for this dataset; example command line:
                // java -Dbatmass.io.libs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-4.1\ext\thermo" -Xmx17G -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\original-crystalc-1.4.2.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.28.12.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\grppr-0.3.23.jar" crystalc.Run C:\DMS_WorkDir\ExperimentGroup\crystalc-0-DatasetName.pepXML.params C:\DMS_WorkDir\ExperimentGroup\DatasetName.pepXML

                // Find the thermo lib directory
                if (!options.LibraryFinder.FindVendorLibDirectory("thermo", out var thermoLibDirectory))
                    return false;

                // Find the Crystal-C jar file
                if (!options.LibraryFinder.FindJarFileCrystalC(out var jarFileCrystalC))
                    return false;

                // Find the Batmass-IO jar file
                if (!options.LibraryFinder.FindJarFileBatmassIO(out var jarFileBatmassIO))
                    return false;

                // Find the Grppr jar file
                if (!options.LibraryFinder.FindJarFileGrppr(out var jarFileGrppr))
                    return false;

                // ReSharper restore CommentTypo
                // ReSharper restore IdentifierTypo

                var datasetCount = datasetIDsByExperimentGroup.Sum(item => item.Value.Count);
                var successCount = 0;
                var arguments = new StringBuilder();

                foreach (var item in datasetIDsByExperimentGroup)
                {
                    var experimentGroupName = item.Key;
                    var experimentGroupDirectory = mExperimentGroupWorkingDirectories[experimentGroupName];

                    foreach (var datasetId in item.Value)
                    {
                        var datasetName = dataPackageInfo.Datasets[datasetId];

                        var pepXmlFile = new FileInfo(Path.Combine(experimentGroupDirectory.FullName, string.Format("{0}.pepXML", datasetName)));

                        if (!pepXmlFile.Exists)
                        {
                            LogError("Cannot run Crystal-C since the PeptideProphet results file was not found: " + pepXmlFile.FullName);
                            return false;
                        }

                        // Create the Crystal-C parameter file for this dataset

                        if (!CreateCrystalCParamFile(experimentGroupDirectory, datasetName, out var crystalcParamFile))
                            return false;

                        arguments.Clear();

                        // ReSharper disable StringLiteralTypo
                        arguments.AppendFormat("-Dbatmass.io.libs.thermo.dir=\"{0}\" -Xmx{1}G -cp \"{2};{3};{4}\" crystalc.Run",
                            thermoLibDirectory.FullName, CRYSTALC_MEMORY_SIZE_GB, jarFileCrystalC.FullName, jarFileBatmassIO.FullName, jarFileGrppr.FullName);

                        arguments.AppendFormat(" {0} {1}", crystalcParamFile.FullName, pepXmlFile.FullName);

                        // ReSharper restore StringLiteralTypo

                        InitializeCommandRunner(
                            experimentGroupDirectory,
                            Path.Combine(mWorkingDirectory.FullName, JAVA_CONSOLE_OUTPUT),
                            CmdRunnerModes.CrystalC);

                        LogCommandToExecute(experimentGroupDirectory, options.JavaProgLoc, arguments.ToString(), options.WorkingDirectoryPadWidth);

                        var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments.ToString(), "Java", true);

                        if (!mConsoleOutputFileParsed)
                        {
                            ParseConsoleOutputFile();
                        }

                        if (!string.IsNullOrEmpty(mConsoleOutputFileParser.ConsoleOutputErrorMsg))
                        {
                            LogError(mConsoleOutputFileParser.ConsoleOutputErrorMsg);
                        }

                        var currentStep = "Crystal-C for " + pepXmlFile.Name;
                        UpdateCombinedJavaConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, currentStep);

                        if (!processingSuccess)
                        {
                            if (mCmdRunner.ExitCode != 0)
                            {
                                LogWarning("Java returned a non-zero exit code while running Crystal-C: " + mCmdRunner.ExitCode);
                            }
                            else
                            {
                                LogWarning("Call to Java failed while running Crystal-C (but exit code is 0)");
                            }

                            continue;
                        }

                        var newPepXmlFile = new FileInfo(Path.Combine(experimentGroupDirectory.FullName, string.Format("{0}_c.pepXML", datasetName)));

                        if (!newPepXmlFile.Exists)
                        {
                            LogError("Crystal-C results file not found: " + newPepXmlFile.Name);
                            return false;
                        }

                        successCount++;
                    }
                }

                return successCount == datasetCount;
            }
            catch (Exception ex)
            {
                LogError("Error in RunCrystalC", ex);
                return false;
            }
        }

        /// <summary>
        /// Create a db.bin file in the .meta subdirectory of the first working directory in mExperimentGroupWorkingDirectories
        /// </summary>
        /// <remarks>
        /// In FragPipe v19 and earlier, the .meta subdirectory was updated at both C:\DMS_WorkDir\.meta and in each experiment group's subdirectory
        /// With FragPipe v20 we only update the first experiment group's working directory (or only C:\DMS_WorkDir\.meta if there is only one experiment group)
        /// </remarks>
        /// <param name="options">Processing options</param>
        private bool RunDatabaseAnnotation(FragPipeOptions options)
        {
            try
            {
                LogDebug("Annotating the FASTA file to create db.bin files", 2);

                // ReSharper disable once CommentTypo

                /*

                // The following code can be used to replace a FASTA file with an alternative one, prior to indexing
                // This was used in February 2023 while diagnosing an indexing error with Philosopher
                // See GitHub issue https://github.com/Nesvilab/philosopher/issues/411

                // If this code is used, be sure to uncomment the "if (filesSwapped)" code in two places later in this method (after the call to RunDatabaseAnnotation)
                const string fastaFileName = "ID_008341_110ECBC1.fasta";

                var fastaFile = new FileInfo(mFastaFilePath);

                if (fastaFile.DirectoryName == null)
                {
                    LogError("Unable to determine the parent directory of " + mFastaFilePath);
                    return false;
                }

                var alternateFilePath = Path.Combine(fastaFile.DirectoryName, Path.GetFileNameWithoutExtension(fastaFile.Name) + "_fixed.fasta");
                var alternateFile = new FileInfo(alternateFilePath);

                var backupPath = mFastaFilePath + ".original";

                bool filesSwapped;

                if (fastaFile.Name.Equals(fastaFileName))
                {
                    if (!alternateFile.Exists)
                    {
                        LogError("Alternate FASTA file not found: " + alternateFile.FullName);
                        return false;
                    }

                    LogDebug("Replacing problematic FASTA file with alternate: " + alternateFile.FullName);

                    fastaFile.MoveTo(backupPath);
                    alternateFile.MoveTo(mFastaFilePath);

                    filesSwapped = true;
                }
                else
                {
                    filesSwapped = false;
                }

                */

                var workingDirectory = mExperimentGroupWorkingDirectories.Count <= 1
                    ? mWorkingDirectory
                    : mExperimentGroupWorkingDirectories.Values.First();

                var workDirSuccess = RunDatabaseAnnotation(workingDirectory, options.WorkingDirectoryPadWidth);

                if (workDirSuccess)
                {
                    /*

                    if (filesSwapped)
                    {
                        alternateFile.MoveTo(alternateFilePath);
                        fastaFile.MoveTo(mFastaFilePath);
                    }

                    */

                    return true;
                }

                // ReSharper disable CommentTypo

                // Philosopher makes several assumptions about protein names, which can lead to "index out of range" errors while indexing the FASTA file

                // For example, ">AT10A_MOUSE" is assumed to be Arabidopsis thaliana because it starts with "AT"
                // Also, ">ZP1_MOUSE" is assumed to be an NCBI protein because it starts with "ZP"

                // Protein lines like these lead to "index out of range" errors due to erroneous assumptions in the code
                // For more info, see GitHub issue https://github.com/Nesvilab/philosopher/issues/411

                // Use the following commands to manually index a FASTA file to confirm that protein names are supported:

                // cd \DMS_WorkDir1
                // C:\DMS_Programs\MSFragger\fragpipe\tools\philosopher\philosopher.exe workspace --init --nocheck
                // C:\DMS_Programs\MSFragger\fragpipe\tools\philosopher\philosopher.exe database --annotate c:\DMS_Temp_Org\ID_008341_110ECBC1.fasta --prefix XXX_

                // ReSharper restore CommentTypo

                // Look for "panic:" and "runtime error:" in the cached console output error messages
                if (mConsoleOutputFileParser.ConsoleOutputErrorMsg.Contains(ConsoleOutputFileParser.PHILOSOPHER_PANIC_ERROR) ||
                    mConsoleOutputFileParser.ConsoleOutputErrorMsg.Contains(ConsoleOutputFileParser.PHILOSOPHER_RUNTIME_ERROR))
                {
                    // Clear mMessage so that this text appears at the start of the message:
                    var existingMessage = mMessage;

                    mMessage = string.Empty;

                    LogError("Philosopher reported an error while indexing the FASTA file; " +
                             "it makes assumptions about protein names that can fail; " +
                             "rename any proteins that start with \"AT\" or \"ZP\" to instead start with \"_AT\" or \"_ZP\", then re-run MSFragger and Philosopher");

                    mMessage = Global.AppendToComment(mMessage, mConsoleOutputFileParser.ConsoleOutputErrorMsg);

                    if (!string.IsNullOrWhiteSpace(existingMessage))
                    {
                        mMessage = Global.AppendToComment(mMessage, existingMessage);
                    }
                }
                else
                {
                    mMessage = Global.AppendToComment("Philosopher reported an error while indexing the FASTA file", mMessage);
                    LogErrorNoMessageUpdate("Philosopher reported an error while indexing the FASTA file");
                }

                /*

                if (filesSwapped)
                {
                    alternateFile.MoveTo(alternateFilePath);
                    fastaFile.MoveTo(mFastaFilePath);
                }

                */

                return false;
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
        /// <param name="workingDirectory">Working directory</param>
        /// <param name="workingDirectoryPadWidth">Working directory pad width</param>
        private bool RunDatabaseAnnotation(DirectoryInfo workingDirectory, int workingDirectoryPadWidth)
        {
            // Note: If the FASTA file does not have Decoy proteins, use this command:
            // philosopher database --custom <file_name> --contam

            // Since our FASTA files have both forward and reverse sequences, we use "--annotate" and "--prefix"
            var arguments = string.Format("database --annotate {0} --prefix XXX_", mFastaFilePath);

            var success = RunPhilosopher(
                PhilosopherToolType.AnnotateDatabase,
                arguments,
                "annotate the database",
                workingDirectory,
                workingDirectoryPadWidth);

            if (!success)
                return false;

            // The database annotation command should have created db.bin in the .meta directory:
            // Verify that it was created

            var outputFile = new FileInfo(Path.Combine(workingDirectory.FullName, META_SUBDIRECTORY, "db.bin"));

            if (outputFile.Exists)
            {
                return true;
            }

            LogError("Database annotation file not found in the .meta directory: " + outputFile.Name);
            return false;
        }

        private bool RunFreeQuant(FragPipeOptions options)
        {
            try
            {
                LogDebug("Running FreeQuant", 2);

                // ReSharper disable CommentTypo

                // Run FreeQuant inside each experiment group working directory, referencing the job's working directory using --dir

                // Example command line:
                // C:\DMS_Programs\MSFragger\fragpipe\tools\philosopher\philosopher.exe freequant --ptw 0.4 --tol 10 --dir C:\DMS_WorkDir

                // ReSharper restore CommentTypo

                var successCount = 0;

                // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
                foreach (var experimentGroupDirectory in mExperimentGroupWorkingDirectories.Values)
                {
                    // ReSharper disable once StringLiteralTypo
                    var arguments = string.Format("freequant --ptw 0.4 --tol 10 --dir {0}", mWorkingDirectory.FullName);

                    var success = RunPhilosopher(
                        PhilosopherToolType.FreeQuant,
                        arguments,
                        "run FreeQuant",
                        experimentGroupDirectory,
                        options.WorkingDirectoryPadWidth);

                    if (!success)
                    {
                        continue;
                    }

                    // FreeQuant updates files in the .meta directory, so there is not a text file to look for to verify that FreeQuant completed successfully

                    successCount++;
                }

                return successCount == mExperimentGroupWorkingDirectories.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in RunFreeQuant", ex);
                return false;
            }
        }

        private bool RunIonQuant(
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            FragPipeOptions options)
        {
            try
            {
                LogDebug("Running IonQuant", 2);

                // ReSharper disable CommentTypo
                // ReSharper disable IdentifierTypo

                // Run IonQuant, example command line:
                // v16
                // java -Xmx4G -Dlibs.bruker.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.4\ext\bruker" -Dlibs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.4\ext\thermo" -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\ionquant-1.7.5.jar;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.23.4.jar" ionquant.IonQuant --threads 4 --ionmobility 0 --mbr 1 --proteinquant 2 --requantify 1 --mztol 10 --imtol 0.05 --rttol 0.4 --mbrmincorr 0 --mbrrttol 1 --mbrimtol 0.05 --mbrtoprun 100000 --ionfdr 0.01 --proteinfdr 1 --peptidefdr 1 --normalization 1 --minisotopes 2 --minscans 3 --writeindex 0 --tp 3 --minfreq 0.5 --minions 2 --minexps 1                --multidir . --filelist C:\DMS_WorkDir\Results\filelist_ionquant.txt

                // v17
                // java -Xmx4G -Dlibs.bruker.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.4\ext\bruker" -Dlibs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.4\ext\thermo" -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\ionquant-1.7.17.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\smile-core-2.6.0.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\smile-math-2.6.0.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\javacpp-presets-platform-1.5.6-bin\javacpp.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\javacpp-presets-platform-1.5.6-bin\javacpp-windows-x86_64.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\javacpp-presets-platform-1.5.6-bin\javacpp-linux-x86_64.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\javacpp-presets-platform-1.5.6-bin\openblas.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\javacpp-presets-platform-1.5.6-bin\openblas-windows-x86_64.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\javacpp-presets-platform-1.5.6-bin\openblas-linux-x86_64.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.23.6.jar" ionquant.IonQuant --threads 4 --ionmobility 0 --mbr 1 --maxlfq 1       --requantify 1 --mztol 10 --imtol 0.05 --rttol 0.4 --mbrmincorr 0 --mbrrttol 1 --mbrimtol 0.05 --mbrtoprun 100000 --ionfdr 0.01 --proteinfdr 1 --peptidefdr 1 --normalization 1 --minisotopes 2 --minscans 3 --writeindex 0 --tp 3 --minfreq 0.5 --minions 2 --minexps 1 --locprob 0.75 --multidir . --filelist C:\DMS_WorkDir\Results\filelist_ionquant.txt

                // v17, use wildcard for javacpp-presets
                // java -Xmx4G  -Dlibs.bruker.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.4\ext\bruker" -Dlibs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.4\ext\thermo" -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\ionquant-1.7.17.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\smile-core-2.6.0.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\smile-math-2.6.0.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\javacpp-presets-platform-1.5.6-bin\*;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.23.6.jar" ionquant.IonQuant --threads 4 --ionmobility 0 --mbr 1 --maxlfq 1       --requantify 1 --mztol 10 --imtol 0.05 --rttol 0.4 --mbrmincorr 0 --mbrrttol 1 --mbrimtol 0.05 --mbrtoprun 100000 --ionfdr 0.01 --proteinfdr 1 --peptidefdr 1 --normalization 1 --minisotopes 2 --minscans 3 --writeindex 0 --tp 3 --minfreq 0.5 --minions 2 --minexps 1 --locprob 0.75 --multidir . --filelist C:\DMS_WorkDir\Results\filelist_ionquant.txt

                // v18
                // java -Xmx10G -Dlibs.bruker.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.5\ext\bruker" -Dlibs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.5\ext\thermo" -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\jfreechart-1.5.3.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.25.5.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\IonQuant-1.8.0.jar"  ionquant.IonQuant --threads 4 --ionmobility 0 --minexps 1 --mbr 1 --maxlfq 1 --requantify 1 --mztol 10 --imtol 0.05 --rttol 0.4 --mbrmincorr 0 --mbrrttol 1 --mbrimtol 0.05 --mbrtoprun 100000 --ionfdr 0.01 --proteinfdr 1 --peptidefdr 1 --normalization 1 --minisotopes 2 --minscans 3 --writeindex 0 --tp 0 --minfreq 0 --minions 2 --locprob 0.75 --uniqueness 0 --multidir . --filelist C:\FragPipe_Test3\Results\filelist_ionquant.txt

                // v19.1
                // java -Xmx11G -Dlibs.bruker.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.8\ext\bruker" -Dlibs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.8\ext\thermo" -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\jfreechart-1.5.3.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.28.9.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\IonQuant-1.8.10.jar" ionquant.IonQuant --threads 4 --ionmobility 0 --minexps 1 --mbr 1 --maxlfq 1 --requantify 1 --mztol 10 --imtol 0.05 --rttol 0.4 --mbrmincorr 0 --mbrrttol 1 --mbrimtol 0.05 --mbrtoprun 10     --ionfdr 0.01 --proteinfdr 1 --peptidefdr 1 --normalization 1 --minisotopes 2 --minscans 3 --writeindex 0 --tp 0 --minfreq 0 --minions 2 --locprob 0.75 --uniqueness 0 --filelist C:\FragPipe_Test3\Results\filelist_ionquant.txt --modlist C:\FragPipe_Test3\Results\modmasses_ionquant.txt

                // v20
                // java -Xmx11G -Dlibs.bruker.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.8\ext\bruker" -Dlibs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.8\ext\thermo" -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\jfreechart-1.5.3.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.28.12.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\IonQuant-1.9.8.jar"   ionquant.IonQuant --threads 4 --ionmobility 0 --minexps 1 --mbr 1 --maxlfq 1 --requantify 1 --mztol 10 --imtol 0.05 --rttol 0.4 --mbrmincorr 0 --mbrrttol 1 --mbrimtol 0.05 --mbrtoprun 10     --ionfdr 0.01 --proteinfdr 1 --peptidefdr 1 --normalization 1 --minisotopes 2 --minscans 3 --writeindex 0 --tp 0 --minfreq 0 --minions 2 --locprob 0.75 --uniqueness 0 --filelist C:\FragPipe_Test3\Results\filelist_ionquant.txt --modlist C:\FragPipe_Test3\Results\modmasses_ionquant.txt
                // java -Xmx11G -Dlibs.bruker.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-4.0\ext\bruker" -Dlibs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-4.0\ext\thermo" -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\jfreechart-1.5.3.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.28.12.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\IonQuant-1.10.12.jar" ionquant.IonQuant --threads 4 --ionmobility 0 --minexps 1 --mbr 1 --maxlfq 1 --requantify 1 --mztol 10 --imtol 0.05 --rttol 0.4 --mbrmincorr 0 --mbrrttol 1 --mbrimtol 0.05 --mbrtoprun 10     --ionfdr 0.01 --proteinfdr 1 --peptidefdr 1 --normalization 1 --minisotopes 2 --minscans 3 --writeindex 0 --tp 0 --minfreq 0 --minions 2 --locprob 0.75 --uniqueness 0 --filelist C:\FragPipe_Test3\Results\filelist_ionquant.txt --modlist C:\FragPipe_Test3\Results\modmasses_ionquant.txt

                // v21
                // java -Xmx11G -Dlibs.bruker.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-4.1\ext\bruker" -Dlibs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-4.1\ext\thermo" -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\jfreechart-1.5.3.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.30.0.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\IonQuant-1.10.27.jar"  ionquant.IonQuant --threads 4 --perform-ms1quant 1 --perform-isoquant 0 --isotol 20.0 --isolevel 2 --isotype tmt10 --ionmobility 0 --site-reports 1 --minexps 1 --mbr 1 --maxlfq 1 --requantify 1 --mztol 10 --imtol 0.05 --rttol 0.4 --mbrmincorr 0 --mbrrttol 1 --mbrimtol 0.05 --mbrtoprun 10 --ionfdr 0.01 --proteinfdr 1 --peptidefdr 1 --normalization 1 --minisotopes 2 --minscans 3 --writeindex 0 --tp 0 --minfreq 0 --minions 2 --locprob 0.75 --uniqueness 0 --filelist C:\DMS_WorkDir\filelist_ionquant.txt --modlist C:\DMS_WorkDir\modmasses_ionquant.txt

                // Find the Bruker lib directory, typically C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-4.1\ext\bruker
                if (!options.LibraryFinder.FindVendorLibDirectory("bruker", out var brukerLibDirectory))
                    return false;

                // Find the Thermo lib directory, typically C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-4.1\ext\thermo
                if (!options.LibraryFinder.FindVendorLibDirectory("thermo", out var thermoLibDirectory))
                    return false;

                // Find the JFreeChart jar file
                // Find the JFreeChart jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\jfreechart-1.5.3.jar
                if (!options.LibraryFinder.FindJarFileJFreeChart(out var jarFileJFreeChart))
                    return false;

                // Find the IonQuant jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\IonQuant-1.10.27.jar
                if (!options.LibraryFinder.FindJarFileIonQuant(out var jarFileIonQuant))
                    return false;

                // Old, prior to v18:
                // Find the smile-core jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\smile-core-2.6.0.jar
                // if (!options.LibraryFinder.FindJarFileSmileCore(out var jarFileSmileCore))
                //     return false;

                // Old, prior to v18:
                // Find the smile-math jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\smile-math-2.6.0.jar
                // if (!options.LibraryFinder.FindJarFileSmileMath(out var jarFileSmileMath))
                //     return false;

                // Old, prior to v18:
                // Find the Java C++ presets directory, typically C:\DMS_Programs\MSFragger\fragpipe\tools\javacpp-presets-platform-1.5.6-bin\*
                // if (!options.LibraryFinder.FindCppPresetsPlatformDirectory(out var cppPresetsPlatformDirectory))
                //     return false;

                // Find the Batmass-IO jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.28.12.jar
                if (!options.LibraryFinder.FindJarFileBatmassIO(out var jarFileBatmassIO))
                    return false;

                // ReSharper restore IdentifierTypo
                // ReSharper restore CommentTypo

                // Future: Possibly customize this
                const int ION_QUANT_THREAD_COUNT = 4;

                var matchBetweenRunsFlag = options.MatchBetweenRuns ? 1 : 0;

                // ReSharper disable StringLiteralTypo

                var arguments = new StringBuilder();

                // ReSharper disable CommentTypo

                /*
                ** v17, use wildcard for javacpp-presets
                *
                arguments.AppendFormat(
                    "-Xmx{0}G -Dlibs.bruker.dir=\"{1}\" -Dlibs.thermo.dir=\"{2}\" -cp \"{3};{4};{5};{6}\\*;{7}\" ionquant.IonQuant",
                    ION_QUANT_MEMORY_SIZE_GB,
                    brukerLibDirectory.FullName,
                    thermoLibDirectory.FullName,
                    jarFileIonQuant.FullName,
                    jarFileSmileCore,
                    jarFileSmileMath,
                    cppPresetsPlatformDirectory,
                    jarFileBatmassIO.FullName);
                */

                // ReSharper restore CommentTypo

                // v18, v19, v20, and v21
                arguments.AppendFormat(
                   "-Xmx{0}G -Dlibs.bruker.dir=\"{1}\" -Dlibs.thermo.dir=\"{2}\" -cp \"{3};{4};{5}\" ionquant.IonQuant",
                   ION_QUANT_MEMORY_SIZE_GB,
                   brukerLibDirectory.FullName,
                   thermoLibDirectory.FullName,
                   jarFileJFreeChart.FullName,
                   jarFileBatmassIO.FullName,
                   jarFileIonQuant.FullName);

                arguments.AppendFormat(" --threads {0} --perform-ms1quant 1 --perform-isoquant 0 --isotol 20.0 --isolevel 2 --isotype tmt10", ION_QUANT_THREAD_COUNT);
                arguments.AppendFormat(" --ionmobility 0 --site-reports 1 --minexps 1 --mbr {0}", matchBetweenRunsFlag);

                // Feature detection m/z tolerance, in ppm
                var featureDetectionMZTolerance = mJobParams.GetJobParameter("FeatureDetectionMZTolerance", 10.0f);

                // Feature detection retention time tolerance, in minutes
                var featureDetectionRTTolerance = mJobParams.GetJobParameter("FeatureDetectionRTTolerance", 0.4f);

                // Minimum correlation between two runs for match between runs
                var mbrMinimumCorrelation = mJobParams.GetJobParameter("MbrMinimumCorrelation", 0f);

                // Match between runs retention time tolerance, in minutes
                var mbrRTTolerance = mJobParams.GetJobParameter("MbrRTTolerance", 1.0f);

                // Match between runs ion FDR
                var mbrIonFdr = mJobParams.GetJobParameter("MbrIonFdr", 0.01f);

                var mbrPeptideFdr = mJobParams.GetJobParameter("MbrPeptideFdr", 1.0f);

                var mbrProteinFdr = mJobParams.GetJobParameter("MbrProteinFdr", 1.0f);

                // When 1, normalize ion intensities among experiments; 0 to disable normalization
                var normalizeIonIntensities = mJobParams.GetJobParameter("NormalizeIonIntensities", 1);

                // Minimum ions required to quantify a protein
                var minIonsForProteinQuant = mJobParams.GetJobParameter("MinIonsForProteinQuant", 2);

                // ReSharper disable CommentTypo

                // v19 switched from "--mbrtoprun 100000" to "--mbrtoprun 10"

                // ReSharper restore CommentTypo

                arguments.AppendFormat(
                    " --maxlfq 1 --requantify 1 --mztol {0} --imtol 0.05 --rttol {1} --mbrmincorr {2} --mbrrttol {3} --mbrimtol 0.05 --mbrtoprun 10",
                    featureDetectionMZTolerance, featureDetectionRTTolerance,
                    mbrMinimumCorrelation, mbrRTTolerance);

                arguments.AppendFormat(
                    " --ionfdr {0} --proteinfdr {1} --peptidefdr {2} --normalization {3}",
                    mbrIonFdr, mbrProteinFdr, mbrPeptideFdr, normalizeIonIntensities);

                arguments.AppendFormat(
                    " --minisotopes 2 --minscans 3 --writeindex 0 --tp 0 --minfreq 0 --minions {0} --locprob 0.75 --uniqueness 0",
                    minIonsForProteinQuant);

                var datasetCount = 0;
                bool creatingCombinedFile;

                if (mExperimentGroupWorkingDirectories.Count <= 1 && !options.MatchBetweenRuns)
                {
                    arguments.AppendFormat(" --psm {0} --specdir {1}", "psm.tsv", mWorkingDirectory.FullName);

                    creatingCombinedFile = false;

                    // ReSharper disable ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator

                    // Prior to v18.0 we appended the .pepXML file names; that step is no longer necessary
                    // Instead, simply update datasetCount

                    foreach (var datasetIDs in datasetIDsByExperimentGroup.Values)
                    {
                        // for each (var datasetId in datasetIDs)
                        // {
                        //    var datasetName = dataPackageInfo.Datasets[datasetId];
                        //    arguments.AppendFormat(" {0}.pepXML", datasetName);
                        //    datasetCount++;
                        // }

                        datasetCount += datasetIDs.Count;
                    }

                    // ReSharper restore ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                }
                else
                {
                    // ReSharper disable CommentTypo

                    // Option 1: append each psm.tsv file and each .pepXML file

                    // for each (var experimentGroupWorkingDirectory in mExperimentGroupWorkingDirectories.Values)
                    // {
                    //    arguments.AppendFormat(@" --psm {0}\psm.tsv ", experimentGroupWorkingDirectory.Name);
                    // }
                    //
                    // arguments.AppendFormat(" --multidir . --specdir {0}", mWorkingDirectory.FullName);
                    //
                    // for each (var item in datasetIDsByExperimentGroup)
                    // {
                    //    var experimentGroupName = item.Key;
                    //    var experimentWorkingDirectory = mExperimentGroupWorkingDirectories[experimentGroupName];
                    //
                    //    for each (var datasetId in item.Value)
                    //    {
                    //        var datasetName = dataPackageInfo.Datasets[datasetId];
                    //
                    //        arguments.AppendFormat(@" {0}\{1}.pepXML ", experimentWorkingDirectory.Name, datasetName);
                    //    }
                    // }

                    // Option 2:
                    // Create a text file listing the psm.tsv and .pepXML files (thus reducing the length of the command line)

                    var fileListFile = CreateIonQuantFileListFile(datasetIDsByExperimentGroup, out datasetCount);

                    // In FragPipe v19 and earlier, if Match-between-runs was enabled, dry-run includes "--multidir ." in the argument list
                    // FragPipe v20 does not include "--multidir .", but testing reveals that it is required when "--mbr 1" is used
                    // FragPipe v20 does include "--multidir ." when using TMT, even if MatchBetweenRuns is not enabled
                    // Thus, always add "--multidir ." when there are multiple experiment group directories

                    // FragPipe v19 and earlier used this:
                    //if (options.MatchBetweenRuns)
                    // {
                    //     arguments.Append(" --multidir .");
                    // }

                    arguments.Append(" --multidir .");
                    arguments.AppendFormat(" --filelist {0}", fileListFile.FullName);

                    creatingCombinedFile = true;

                    mJobParams.AddResultFileToSkip(fileListFile.Name);

                    // ReSharper restore CommentTypo
                }

                // Create a file listing modification masses
                var modMassesFile = CreateIonQuantModMassesFile(options);

                arguments.AppendFormat(" --modlist {0}", modMassesFile.FullName);

                // ReSharper restore StringLiteralTypo

                InitializeCommandRunner(
                    mWorkingDirectory,
                    Path.Combine(mWorkingDirectory.FullName, JAVA_CONSOLE_OUTPUT),
                    CmdRunnerModes.IonQuant);

                LogCommandToExecute(mWorkingDirectory, options.JavaProgLoc, arguments.ToString());

                var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments.ToString(), "Java", true);

                // Note that warnings regarding SLF4J bindings can be ignored

                if (!mConsoleOutputFileParsed)
                {
                    ParseConsoleOutputFile();
                }

                if (!string.IsNullOrEmpty(mConsoleOutputFileParser.ConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputFileParser.ConsoleOutputErrorMsg);
                }

                const string currentStep = "IonQuant";
                UpdateCombinedJavaConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, currentStep);

                if (processingSuccess)
                {
                    // IonQuant-1.8.0  (included FragPipe v18) created one _quant.csv file for each dataset
                    // IonQuant-1.8.10 (included with FragPipe v19) does not create _quant.csv files
                    // IonQuant-1.9.8  (included with FragPipe v20) does not create _quant.csv files

                    // Check whether any _quant.csv files were created
                    var quantFiles = mWorkingDirectory.GetFiles("*_quant.csv", SearchOption.AllDirectories);

                    if (quantFiles.Length == 0)
                    {
                        // No _quant.csv files were found
                        // If IonQuant could not find enough features in common between datasets, this warning will appear in the console output (Java_ConsoleOutput_Combined.txt)
                        //   There are only 2 negative data points in training. We need at least 10

                        // Previously logged a warning, but continued processing
                        // No longer log a warning since FragPipe v19 and v20 do not create _quant.csv files

                        // LogWarning("IonQuant did not create any _quant.csv files, either because of not enough features in common, or because a newer version that does not create _quant.csv files");
                    }

                    // Confirm that the output files were created
                    var outputFiles = new List<FileInfo>();

                    if (creatingCombinedFile)
                    {
                        var combinedIonFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "combined_ion.tsv"));

                        if (!combinedIonFile.Exists && quantFiles.Length == 0)
                        {
                            LogMessage("IonQuant did not create file combined_ion.tsv; however, it did not create any _quant.csv files so this is expected");
                        }
                        else
                        {
                            outputFiles.Add(combinedIonFile);
                        }

                        outputFiles.Add(new FileInfo(Path.Combine(mWorkingDirectory.FullName, "combined_peptide.tsv")));
                        outputFiles.Add(new FileInfo(Path.Combine(mWorkingDirectory.FullName, "combined_protein.tsv")));
                    }
                    else
                    {
                        outputFiles.Add(new FileInfo(Path.Combine(mWorkingDirectory.FullName, "ion.tsv")));
                        outputFiles.Add(new FileInfo(Path.Combine(mWorkingDirectory.FullName, "peptide.tsv")));
                        outputFiles.Add(new FileInfo(Path.Combine(mWorkingDirectory.FullName, "protein.tsv")));
                    }

                    var outputFilesExist = ValidateOutputFilesExist("IonQuant", outputFiles);

                    if (options.MatchBetweenRuns)
                    {
                        // IonQuant 1.7 always created file mbr_ion.tsv
                        // IonQuant 1.8 only creates it if at least 10 data points are found in the training data
                        // Thus, only log a warning if mbr_ion.tsv is missing

                        var mbrIonFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "mbr_ion.tsv"));

                        if (!mbrIonFile.Exists)
                        {
                            // IonQuant results file not found: mbr_ion.tsv
                            LogWarning("IonQuant did not create file {0}; this indicates that insufficient training data could be found and match-between-runs could thus not be performed", mbrIonFile.Name);

                            mEvalMessage = Global.AppendToComment(mEvalMessage, "IonQuant did not create match-between-runs file mbr_ion.tsv");
                        }
                    }

                    try
                    {
                        LogDebug("Moving _quant.csv files created by IonQuant to the working directory");

                        foreach (var quantFile in quantFiles)
                        {
                            if (quantFile.Directory?.FullName.Equals(mWorkingDirectory.FullName) == true)
                                continue;

                            var targetPath = Path.Combine(mWorkingDirectory.FullName, quantFile.Name);
                            quantFile.MoveTo(targetPath);
                        }

                        if (quantFiles.Length > 3)
                        {
                            // Zip the _quant.csv files, creating Dataset_quant_csv.zip

                            var zipSuccess = ZipFiles("_quant .csv files", quantFiles, ZIPPED_QUANT_CSV_FILES);

                            if (!zipSuccess)
                                return false;
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError("Error moving IonQuant _quant.csv files to the working directory", ex);
                    }

                    try
                    {
                        LogDebug("Moving .png files created by IonQuant files to the working directory");

                        foreach (var pngFile in mWorkingDirectory.GetFiles("*.png", SearchOption.AllDirectories))
                        {
                            if (pngFile.Directory?.FullName.Equals(mWorkingDirectory.FullName) == true)
                                continue;

                            var targetPath = Path.Combine(mWorkingDirectory.FullName, pngFile.Name);

                            if (File.Exists(targetPath))
                            {
                                LogWarning(".png file already exists; not replacing {0} with {1}", targetPath, pngFile.FullName);
                                continue;
                            }

                            pngFile.MoveTo(targetPath);
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError("Error moving IonQuant .png files to the working directory", ex);
                    }

                    bool success;

                    if (creatingCombinedFile && mExperimentGroupWorkingDirectories.Count <= 1)
                    {
                        // IonQuant assumes that the experiment group name is the parent directory name
                        // This will be valid when multiple experiment groups are present, but is not valid if a single experiment group is present
                        // Edit the header line of the combined*.tsv files to replace the working directory name with either the dataset name or "Aggregation"
                        success = UpdateCombinedTsvFiles(datasetCount);
                    }
                    else
                    {
                        success = true;
                    }

                    return outputFilesExist && success;
                }

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("Java returned a non-zero exit code while running IonQuant: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to Java failed while runningIonQuant (but exit code is 0)");
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in RunIonQuant", ex);
                return false;
            }
        }

        private bool RunIProphet(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            FragPipeOptions options)
        {
            try
            {
                LogDebug("Running iProphet", 2);

                // ReSharper disable CommentTypo

                // Example command line:
                // philosopher.exe iprophet --decoy XXX_ --nonsp --output combined --threads 4 C:\DMS_WorkDir\Results\ExperimentGroupA\interact-Dataset1.pep.xml C:\DMS_WorkDir\Results\ExperimentGroupB\interact-Dataset2.pep.xml C:\DMS_WorkDir\Results\ExperimentGroupB\interact-Dataset3.pep.xml

                // ReSharper restore CommentTypo

                var arguments = new StringBuilder();

                // ReSharper disable StringLiteralTypo

                arguments.Append("iprophet --decoy XXX_ --nonsp --output combined --threads 4");

                // ReSharper restore StringLiteralTypo

                var generatePeptideLevelSummary = options.FraggerOptions.GetParameterValueOrDefault("GeneratePeptideLevelSummary", true);

                if (!generatePeptideLevelSummary)
                {
                    LogWarning("Method RunIProphet was called when job parameter GeneratePeptideLevelSummary is false; this indicates a logic bug");
                }

                // Append the .pep.xml file for each dataset in each experiment group

                // ReSharper disable ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator

                foreach (var item in datasetIDsByExperimentGroup)
                {
                    var experimentGroupName = item.Key;
                    var experimentWorkingDirectory = mExperimentGroupWorkingDirectories[experimentGroupName];

                    foreach (var datasetId in item.Value)
                    {
                        var datasetName = dataPackageInfo.Datasets[datasetId];

                        var pepXmlFile = new FileInfo(Path.Combine(experimentWorkingDirectory.FullName, string.Format("interact-{0}.pep.xml", datasetName)));
                        arguments.AppendFormat(" {0}", pepXmlFile.FullName);
                    }
                }

                // ReSharper restore ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator

                var success = RunPhilosopher(PhilosopherToolType.IProphet, arguments.ToString(), "run iProphet");

                // Verify that the iProphet result file exists
                var outputFiles = new List<FileInfo>
                {
                    new(Path.Combine(mWorkingDirectory.FullName, "combined.pep.xml")),
                };

                var outputFilesExist = ValidateOutputFilesExist("iProphet", outputFiles);

                return success && outputFilesExist;
            }
            catch (Exception ex)
            {
                LogError("Error in RunIProphet", ex);
                return false;
            }
        }

        /// <summary>
        /// Isobaric Quantification (LabelQuant)
        /// </summary>
        /// <remarks>
        /// Results will appear in the.tsv files created by the Report step (ion.tsv, peptide.tsv, protein.tsv, and psm.tsv),
        /// in columns corresponding to labels in the AliasNames.txt file (or experiment group specific alias name file)
        /// </remarks>
        /// <param name="options">Processing options</param>
        private bool RunLabelQuant(FragPipeOptions options)
        {
            try
            {
                LogDebug(string.Format("Running LabelQuant for Isobaric Quantification using {0} reporter ions", options.ReporterIonMode), 2);

                // ReSharper disable CommentTypo

                // Example command line:
                // C:\DMS_Programs\MSFragger\fragpipe\tools\philosopher\philosopher.exe labelquant --tol 20 --level 2 --minprob 0.900 --purity 0.500 --removelow 0.050 --plex 10 --annot C:\DMS_WorkDir\ExperimentGroupA_annotation.txt --brand tmt --dir C:\DMS_WorkDir

                // ReSharper restore CommentTypo

                var successCount = 0;

                var reporterIonType = options.ReporterIonMode switch
                {
                    ReporterIonModes.Itraq4 => "iTraq",
                    ReporterIonModes.Itraq8 => "iTraq",
                    ReporterIonModes.Tmt6 => "TMT",
                    ReporterIonModes.Tmt10 => "TMT",
                    ReporterIonModes.Tmt11 => "TMT",
                    ReporterIonModes.Tmt16 => "TMT",
                    ReporterIonModes.Tmt18 => "TMT",
                    _ => throw new ArgumentOutOfRangeException()
                };

                var plex = GetReporterIonChannelCount(options.ReporterIonMode);
                var autoGeneratedAliasNameFile = false;

                // If multiple experiment groups are defined, try to get shorter experiment group names, which will be used for the sample names in the AliasNames.txt files
                var experimentGroupAbbreviations = GetAbbreviatedExperimentGroupNames(mExperimentGroupWorkingDirectories.Keys.ToList());

                foreach (var experimentGroup in mExperimentGroupWorkingDirectories)
                {
                    FileInfo aliasFile;

                    var aliasNameSuffix = experimentGroup.Key.Equals(DataPackageInfoLoader.UNDEFINED_EXPERIMENT_GROUP)
                        ? string.Empty
                        : "_" + experimentGroup.Key;

                    var experimentSpecificAliasFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, string.Format("AliasNames{0}.txt", aliasNameSuffix)));
                    var genericAliasFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "AliasNames.txt"));
                    var genericAliasFile2 = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "AliasName.txt"));

                    if (experimentSpecificAliasFile.Exists)
                    {
                        aliasFile = experimentSpecificAliasFile;
                    }
                    else if (genericAliasFile.Exists)
                    {
                        aliasFile = genericAliasFile;
                    }
                    else if (genericAliasFile2.Exists)
                    {
                        aliasFile = genericAliasFile2;
                    }
                    else
                    {
                        LogMessage(
                            "{0} alias file not found; will auto-generate file {1} for use with LabelQuant",
                            reporterIonType, experimentSpecificAliasFile.Name);

                        string prefixToUse;

                        if (experimentGroupAbbreviations.TryGetValue(experimentGroup.Key, out var sampleNamePrefix))
                        {
                            prefixToUse = sampleNamePrefix;
                        }
                        else
                        {
                            LogWarning("Experiment group {0} was not found in the dictionary returned by GetAbbreviatedExperimentGroupNames", experimentGroup.Key);
                            prefixToUse = string.Empty;
                        }

                        aliasFile = CreateReporterIonAliasNameFile(options.ReporterIonMode, experimentSpecificAliasFile, prefixToUse);

                        if (!autoGeneratedAliasNameFile)
                        {
                            // This is the first auto-generated alias name file; copy it to the results directory
                            autoGeneratedAliasNameFile = true;
                        }
                        else
                        {
                            // Do not copy this auto-generated alias name file to the results directory
                            mJobParams.AddResultFileToSkip(experimentSpecificAliasFile.Name);
                        }

                        if (aliasFile == null)
                            return false;
                    }

                    // ReSharper disable StringLiteralTypo

                    var phosphoSearch = PhosphoModDefined(options.FraggerOptions.VariableModifications);

                    string minProb;
                    string purity;
                    string removeLow;

                    if (phosphoSearch)
                    {
                        minProb = "0.500";
                        purity = "0.500";
                        removeLow = "0.025";
                    }
                    else
                    {
                        minProb = "0.900";
                        purity = "0.500";
                        removeLow = "0.050";
                    }

                    var arguments = string.Format(
                        "labelquant --tol 20 --level 2 --minprob {0} --purity {1} --removelow {2} --plex {3} --annot {4} --brand {5} --dir {6}",
                        minProb,
                        purity,
                        removeLow,
                        plex,
                        aliasFile.FullName,
                        reporterIonType.ToLower(),
                        mWorkingDirectory.FullName);

                    // ReSharper restore StringLiteralTypo

                    var success = RunPhilosopher(
                        PhilosopherToolType.LabelQuant,
                        arguments,
                        "run LabelQuant",
                        experimentGroup.Value,
                        options.WorkingDirectoryPadWidth);

                    if (!success)
                    {
                        continue;
                    }

                    // LabelQuant updates files in the .meta directory, so there is not a text file to look for to verify that LabelQuant completed successfully

                    successCount++;
                }

                return successCount == mExperimentGroupWorkingDirectories.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in RunLabelQuant", ex);
                return false;
            }
        }

        private bool RunMSBooster(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            FragPipeOptions options,
            string paramFilePath)
        {
            try
            {
                LogDebug("Running MSBooster (DIA-NN)", 2);

                // ReSharper disable CommentTypo
                // ReSharper disable IdentifierTypo

                // Run MSBooster for this dataset; example command line:

                // v18
                // java -Xmx11G -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\msbooster-1.1.4.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\smile-core-2.6.0.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\smile-math-2.6.0.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.25.5.jar" Features.MainClass --paramsList C:\FragPipe_Test3\Results\msbooster_params.txt

                // v19.1
                // java -Xmx11G -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\msbooster-1.1.11.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.28.9.jar" Features.MainClass --paramsList C:\FragPipe_Test3\Results\msbooster_params.txt

                // v20
                // java -Xmx11G -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\msbooster-1.1.11.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.28.12.jar" Features.MainClass --paramsList C:\FragPipe_Test3\Results\msbooster_params.txt

                // v21.1
                // java -Xmx11G -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\msbooster-1.1.28.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.30.0.jar" Features.MainClass --paramsList C:\FragPipe_Test3\Results\msbooster_params.txt

                // Note that with FragPipe v19, MSBooster is calling DiaNN.exe with a TMT mod mass, even for parameter files that do not have TMT as a mod mass
                // This is "by design", as confirmed at https://github.com/Nesvilab/FragPipe/issues/1031
                // However, note that Dia-NN only supports TMT 6/10/11, not TMT 16 or TMT 18

                // Java console output shows these arguments being used on the command line for job 2163338 (using FragPipe v19)
                //   DiaNN.exe --lib D:\DMS_WorkDir4\51912_Lewis_nanoPOTS_Set1_Inner_Vascicular_Bundle_A1\spectraRT.tsv --predict --threads 4 --strip-unknown-mods --mod TMT,229.1629 --predict-n-frag 100

                // Contrast with job 2144860 (using FragPipe v18)
                //   DiaNN.exe --lib D:\DMS_WorkDir4\51912_Lewis_nanoPOTS_Set1_Inner_Vascicular_Bundle_A1\spectraRT.tsv --predict --threads 4 --strip-unknown-mods

                // Java console output for job 2163338 also shows this:
                //	 Modification TMT with mass delta 229.163 added to the list of recognised modifications for spectral library-based search
                //	 Deep learning predictor will predict 100 fragments
                //	 Cannot find a UniMod modification match for TMT: 73.0618 minimal mass discrepancy; using the original modificaiton name

                // These warnings can safely be ignored

                // Find the MSBooster jar file
                if (!options.LibraryFinder.FindJarFileMSBooster(out var jarFileMSBooster))
                    return false;

                /*
                // v18: Find the smile-core jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\smile-core-2.6.0.jar;

                if (!options.LibraryFinder.FindJarFileSmileCore(out var jarFileSmileCore))
                    return false;

                // v18: Find the smile-math jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\smile-math-2.6.0.jar;

                if (!options.LibraryFinder.FindJarFileSmileMath(out var jarFileSmileMath))
                    return false;
                */

                // Find the Batmass-IO jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.28.12.jar
                if (!options.LibraryFinder.FindJarFileBatmassIO(out var jarFileBatmassIO))
                    return false;

                // ReSharper restore CommentTypo
                // ReSharper restore IdentifierTypo

                // Create the MSBooster parameter file

                if (!CreateMSBoosterParamFile(dataPackageInfo, datasetIDsByExperimentGroup, paramFilePath, out var msBoosterParamFile))
                    return false;

                var arguments = new StringBuilder();

                // ReSharper disable StringLiteralTypo
                arguments.AppendFormat("-Xmx{0}G -cp \"{1};{2}\" Features.MainClass",
                    MSBOOSTER_MEMORY_SIZE_GB, jarFileMSBooster.FullName, jarFileBatmassIO.FullName);

                arguments.AppendFormat(" --paramsList {0}", msBoosterParamFile.FullName);

                InitializeCommandRunner(
                    mWorkingDirectory,
                    Path.Combine(mWorkingDirectory.FullName, JAVA_CONSOLE_OUTPUT),
                    CmdRunnerModes.MSBooster);

                LogCommandToExecute(mWorkingDirectory, options.JavaProgLoc, arguments.ToString(), options.WorkingDirectoryPadWidth);

                var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments.ToString(), "Java", true);

                if (!mConsoleOutputFileParsed)
                {
                    ParseConsoleOutputFile();
                }

                if (!string.IsNullOrEmpty(mConsoleOutputFileParser.ConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputFileParser.ConsoleOutputErrorMsg);
                }

                const string currentStep = "MSBooster";
                UpdateCombinedJavaConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, currentStep);

                if (!processingSuccess)
                {
                    if (mCmdRunner.ExitCode != 0)
                    {
                        LogWarning("Java returned a non-zero exit code while running MSBooster: " + mCmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to Java failed while running MSBooster (but exit code is 0)");
                    }
                }

                var newPinFiles = mWorkingDirectory.GetFiles("*_edited.pin", SearchOption.AllDirectories);

                if (newPinFiles.Length == 0)
                {
                    LogError("MSBooster did not create any _edited.pin files");
                    return false;
                }

                LogMessage(
                    "MSBooster created {0} _edited.pin {1}",
                    newPinFiles.Length,
                    Global.CheckPlural(newPinFiles.Length, "file", "files"));

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in RunMSBooster", ex);
                return false;
            }
        }

        private bool RunPeptideProphet(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            FragPipeOptions options,
            out List<FileInfo> peptideProphetPepXmlFiles)
        {
            try
            {
                LogDebug("Running PeptideProphet", 2);

                if (options.OpenSearch)
                {
                    return RunPeptideProphetForOpenSearch(
                        dataPackageInfo,
                        datasetIDsByExperimentGroup,
                        options,
                        out peptideProphetPepXmlFiles);
                }

                // Keys in this dictionary are dataset IDs, values are DirectoryInfo instances
                var workspaceDirectoryByDatasetId = InitializePeptideProphetWorkspaceDirectories(
                    dataPackageInfo,
                    datasetIDsByExperimentGroup);

                var workingDirectoryPadWidth = GetLongestWorkingDirectoryName(workspaceDirectoryByDatasetId.Values);

                // Run PeptideProphet separately against each dataset

                var interactFilesByDatasetID = new Dictionary<int, List<FileInfo>>();

                foreach (var dataset in workspaceDirectoryByDatasetId)
                {
                    var datasetId = dataset.Key;
                    var datasetName = dataPackageInfo.Datasets[datasetId];
                    var workingDirectory = dataset.Value;

                    if (workingDirectory.Parent == null)
                    {
                        LogError("Unable to determine the parent directory of " + workingDirectory.FullName);
                        peptideProphetPepXmlFiles = new List<FileInfo>();
                        return false;
                    }

                    var interactFiles = new List<FileInfo>();
                    interactFilesByDatasetID.Add(datasetId, interactFiles);

                    var pepXmlFiles = new List<string>();

                    if (options.FraggerOptions.DIASearchEnabled)
                    {
                        // Each dataset will have multiple .pepXML files; run Peptide Prophet separately on each one
                        foreach (var item in (workingDirectory.Parent.GetFiles(string.Format("{0}_rank*.pepXML", datasetName))))
                        {
                            pepXmlFiles.Add(string.Format(@"..\{0}", item.Name));
                        }
                    }
                    else
                    {
                        pepXmlFiles.Add(string.Format(@"..\{0}.pepXML", datasetName));
                    }

                    foreach (var pepXmlFile in pepXmlFiles)
                    {
                        // ReSharper disable StringLiteralTypo
                        var arguments = string.Format(
                            "peptideprophet --decoyprobs --ppm --accmass --nonparam --expectscore --decoy XXX_ --database {0} {1}",
                            mFastaFilePath, pepXmlFile);

                        // ReSharper restore StringLiteralTypo

                        var success = RunPhilosopher(
                            PhilosopherToolType.PeptideProphet,
                            arguments,
                            "run PeptideProphet",
                            workingDirectory,
                            workingDirectoryPadWidth);

                        if (!success)
                        {
                            peptideProphetPepXmlFiles = new List<FileInfo>();
                            return false;
                        }

                        // Verify that the PeptideProphet results file was created

                        var interactFileName = string.Format("interact-{0}.pep.xml", Path.GetFileNameWithoutExtension(pepXmlFile));
                        var interactFile = new FileInfo(Path.Combine(workingDirectory.Parent.FullName, interactFileName));

                        if (!interactFile.Exists)
                        {
                            LogError("PeptideProphet results file not found: " + interactFile.Name);
                            peptideProphetPepXmlFiles = new List<FileInfo>();
                            return false;
                        }

                        interactFiles.Add(interactFile);

                        mJobParams.AddResultFileToSkip(interactFile.Name);
                    }
                }

                DeleteTempDirectories(workspaceDirectoryByDatasetId.Values.ToList());

                return UpdateMsMsRunSummaryInPepXmlFiles(
                    dataPackageInfo,
                    workspaceDirectoryByDatasetId,
                    interactFilesByDatasetID,
                    options,
                    out peptideProphetPepXmlFiles);
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
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            FragPipeOptions options,
            out List<FileInfo> peptideProphetPepXmlFiles)
        {
            try
            {
                // Run PeptideProphet separately against each experiment group

                foreach (var item in datasetIDsByExperimentGroup)
                {
                    var experimentGroupName = item.Key;
                    var experimentGroupDirectory = mExperimentGroupWorkingDirectories[experimentGroupName];

                    // ReSharper disable StringLiteralTypo

                    var arguments = new StringBuilder();

                    arguments.AppendFormat(
                        "peptideprophet --nonparam --expectscore --decoyprobs --masswidth 1000.0 --clevel -2 --decoy XXX_ --database {0} --combine",
                        mFastaFilePath);

                    // ReSharper restore StringLiteralTypo

                    var crystalcPepXmlFiles = new List<FileInfo>();

                    foreach (var datasetId in item.Value)
                    {
                        var datasetName = dataPackageInfo.Datasets[datasetId];
                        var pepXmlFilename = string.Format("{0}_c.pepXML", datasetName);

                        var pepXmlFile = new FileInfo(Path.Combine(experimentGroupDirectory.FullName, pepXmlFilename));

                        if (!pepXmlFile.Exists)
                        {
                            LogError("Crystal-C .pepXML file not found: " + pepXmlFile.Name);
                            peptideProphetPepXmlFiles = new List<FileInfo>();
                            return false;
                        }

                        arguments.AppendFormat(" {0}", pepXmlFilename);
                        crystalcPepXmlFiles.Add(pepXmlFile);
                    }

                    var success = RunPhilosopher(
                        PhilosopherToolType.PeptideProphet,
                        arguments.ToString(),
                        "run PeptideProphet",
                        experimentGroupDirectory,
                        options.WorkingDirectoryPadWidth);

                    if (!success)
                    {
                        peptideProphetPepXmlFiles = new List<FileInfo>();
                        return false;
                    }

                    // Note that UpdateMsMsRunSummaryInCombinedPepXmlFiles will verify that PeptideProphet created .pep.xml files

                    foreach (var pepXmlFile in crystalcPepXmlFiles)
                    {
                        mJobParams.AddResultFileToSkip(pepXmlFile.Name);
                    }
                }

                return UpdateMsMsRunSummaryInCombinedPepXmlFiles(
                    dataPackageInfo,
                    datasetIDsByExperimentGroup,
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

        private bool RunPercolator(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            FragPipeOptions options,
            out List<FileInfo> peptideProphetPepXmlFiles)
        {
            peptideProphetPepXmlFiles = new List<FileInfo>();

            try
            {
                mJobParams.AddResultFileToSkip(JAVA_CONSOLE_OUTPUT);
                mJobParams.AddResultFileToSkip(PERCOLATOR_CONSOLE_OUTPUT);

                if (!options.LibraryFinder.FindFragPipeLibDirectory(out var fragPipeLibDirectory))
                    return false;

                var successCount = 0;

                // Run percolator on each dataset

                foreach (var item in datasetIDsByExperimentGroup)
                {
                    var experimentGroupName = item.Key;
                    var experimentGroupDirectory = mExperimentGroupWorkingDirectories[experimentGroupName];

                    LogDebug("Running Percolator in " + experimentGroupDirectory.FullName);

                    // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                    foreach (var datasetId in item.Value)
                    {
                        var datasetName = dataPackageInfo.Datasets[datasetId];

                        var percolatorSuccess = RunPercolatorOnDataset(
                            experimentGroupDirectory, datasetName,
                            options,
                            out var percolatorPsmFiles);

                        if (!percolatorSuccess)
                            continue;

                        var percolatorToPepXMLSuccess = ConvertPercolatorOutputToPepXML(
                            fragPipeLibDirectory,
                            experimentGroupDirectory,
                            datasetName,
                            options,
                            out var pepXmlFiles);

                        if (!percolatorToPepXMLSuccess)
                            continue;

                        peptideProphetPepXmlFiles.AddRange(pepXmlFiles);

                        // Delete the percolator PSM files, since we no longer need them

                        // Example names:
                        // DatasetName_percolator_target_psms.tsv or
                        // DatasetName_percolator_decoy_psms.tsv

                        foreach (var psmFile in percolatorPsmFiles)
                        {
                            try
                            {
                                if (psmFile.Exists)
                                    psmFile.Delete();
                            }
                            catch (Exception ex)
                            {
                                LogWarning("Error deleting {0}: {1}", psmFile.FullName, ex.Message);
                            }
                        }

                        successCount++;
                    }
                }

                return successCount == dataPackageInfo.Datasets.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in RunPercolator", ex);
                return false;
            }
        }

        private bool RunPercolatorOnDataset(
            FileSystemInfo experimentGroupDirectory,
            string datasetName,
            FragPipeOptions options,
            out List<FileInfo> percolatorPsmFiles)
        {
            // Future: possibly adjust this
            const int PERCOLATOR_THREAD_COUNT = 4;

            percolatorPsmFiles = new List<FileInfo>();

            try
            {
                LogDebug("Running Percolator on " + datasetName, 2);

                // ReSharper disable StringLiteralTypo
                // ReSharper disable CommentTypo

                // Example command line:
                // percolator_3_6_4\windows\percolator.exe --only-psms --no-terminate --post-processing-tdc --num-threads 4 --results-psms DatasetName_percolator_target_psms.tsv --decoy-results-psms DatasetName_percolator_decoy_psms.tsv --protein-decoy-pattern XXX_ DatasetName.pin

                // If MSBooster was used, it will have created _edited.pin files
                // percolator_3_6_4\windows\percolator.exe --only-psms --no-terminate --post-processing-tdc --num-threads 4 --results-psms DatasetName_percolator_target_psms.tsv --decoy-results-psms DatasetName_percolator_decoy_psms.tsv --protein-decoy-pattern XXX_ DatasetName_edited.pin

                var targetPsmFileName = GetPercolatorFileName(datasetName, false);
                var decoyPsmFileName = GetPercolatorFileName(datasetName, true);

                // The .pin file will be DatasetName_edited.pin if MSBooster was used
                // Otherwise, it is DatasetName.pin
                var pinFile = string.Format("{0}{1}", datasetName, options.RunMSBooster ? "_edited.pin" : ".pin");

                var arguments = string.Format(
                    "--only-psms --no-terminate --post-processing-tdc --num-threads {0} " +
                    "--results-psms {1} " +
                    "--decoy-results-psms {2} " +
                    "--protein-decoy-pattern XXX_ " +       // New for FragPipe v20, though percolator is smart enough to recognize XXX_ as a decoy protein prefix even if argument "--protein-decoy-pattern" is not used
                    "{3}",
                    PERCOLATOR_THREAD_COUNT,
                    targetPsmFileName,
                    decoyPsmFileName,
                    pinFile);

                // ReSharper restore CommentTypo
                // ReSharper restore StringLiteralTypo

                var targetPsmFile = new FileInfo(Path.Combine(experimentGroupDirectory.FullName, targetPsmFileName));
                var decoyPsmFile = new FileInfo(Path.Combine(experimentGroupDirectory.FullName, decoyPsmFileName));

                percolatorPsmFiles.Add(targetPsmFile);
                percolatorPsmFiles.Add(decoyPsmFile);

                InitializeCommandRunner(
                    experimentGroupDirectory,
                    Path.Combine(mWorkingDirectory.FullName, PERCOLATOR_CONSOLE_OUTPUT),
                    CmdRunnerModes.Percolator);

                // Percolator reports all of its messages via the console error stream
                // Instruct mCmdRunner to treat them as normal messages
                mCmdRunner.RaiseConsoleErrorEvents = false;

                LogCommandToExecute(experimentGroupDirectory, mPercolatorProgLoc, arguments, options.WorkingDirectoryPadWidth);

                // Start the program and wait for it to finish
                // However, while it's running, LoopWaiting will get called via events
                var processingSuccess = mCmdRunner.RunProgram(mPercolatorProgLoc, arguments, "Percolator", true);

                if (!mConsoleOutputFileParsed)
                {
                    ParseConsoleOutputFile();
                }

                if (!string.IsNullOrEmpty(mConsoleOutputFileParser.ConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputFileParser.ConsoleOutputErrorMsg);
                }

                UpdateCombinedPercolatorConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, datasetName);

                mCmdRunner.RaiseConsoleErrorEvents = true;

                // ReSharper disable once ConvertIfStatementToSwitchStatement
                if (processingSuccess &&
                    mConsoleOutputFileParser.ConsoleOutputErrorMsg.IndexOf("Error: no decoy PSMs were provided", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    // The error should have already been logged (and stored in mMessage)
                    return false;
                }

                if (processingSuccess)
                {
                    if (!targetPsmFile.Exists)
                    {
                        LogError("Percolator results file not found: " + targetPsmFile.Name);
                        return false;
                    }

                    if (!decoyPsmFile.Exists)
                    {
                        LogError("Percolator results file not found: " + decoyPsmFile.Name);
                        return false;
                    }

                    return true;
                }

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("Percolator returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to Percolator failed (but exit code is 0)");
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in RunPercolatorOnDataset", ex);
                mCmdRunner.RaiseConsoleErrorEvents = true;
                return false;
            }
        }

        /// <summary>
        /// Run philosopher using the specified arguments
        /// </summary>
        /// <param name="toolType">Tool type</param>
        /// <param name="arguments">Command line arguments</param>
        /// <param name="currentTask">Current task</param>
        /// <param name="workingDirectory">Optional, custom working directory; if null, will use mWorkingDirectory</param>
        /// <param name="workingDirectoryPadWidth">Working directory pad width</param>
        /// <returns>True if successful, false if error</returns>
        private bool RunPhilosopher(
            PhilosopherToolType toolType,
            string arguments,
            string currentTask,
            DirectoryInfo workingDirectory = null,
            int workingDirectoryPadWidth = 0)
        {
            try
            {
                mCurrentPhilosopherTool = toolType;

                workingDirectory ??= mWorkingDirectory;

                InitializeCommandRunner(
                    workingDirectory,
                    Path.Combine(mWorkingDirectory.FullName, PHILOSOPHER_CONSOLE_OUTPUT),
                    CmdRunnerModes.Philosopher,
                    GetMonitoringInterval(toolType));

                if (toolType == PhilosopherToolType.ShowVersion)
                    LogDebug(mPhilosopherProgLoc + " " + arguments);
                else
                    LogCommandToExecute(workingDirectory, mPhilosopherProgLoc, arguments, workingDirectoryPadWidth);

                if (toolType is PhilosopherToolType.PeptideProphet or PhilosopherToolType.ProteinProphet or PhilosopherToolType.IProphet)
                {
                    // PeptideProphet and ProteinProphet report numerous warnings via the console error stream
                    // iProphet reports status messages using the console error stream
                    // Instruct mCmdRunner to treat them as normal messages
                    mCmdRunner.RaiseConsoleErrorEvents = false;
                }

                // Start the program and wait for it to finish
                // However, while it's running, LoopWaiting will get called via events
                var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

                if (!mConsoleOutputFileParsed)
                {
                    ParseConsoleOutputFile();
                }

                if (!string.IsNullOrEmpty(mConsoleOutputFileParser.ConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputFileParser.ConsoleOutputErrorMsg);
                }

                var currentStep = GetCurrentPhilosopherToolDescription(toolType);
                UpdateCombinedPhilosopherConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, currentStep, toolType);

                if (toolType is PhilosopherToolType.PeptideProphet or PhilosopherToolType.ProteinProphet)
                {
                    mCmdRunner.RaiseConsoleErrorEvents = true;
                }

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
                mCmdRunner.RaiseConsoleErrorEvents = true;
                return false;
            }
        }

        /// <summary>
        /// Run ProteinProphet
        /// </summary>
        /// <param name="peptideProphetPepXmlFiles">List of .pep.xml files created by PeptideProphet</param>
        /// <param name="options">Processing options</param>
        private bool RunProteinProphet(ICollection<FileInfo> peptideProphetPepXmlFiles, FragPipeOptions options)
        {
            try
            {
                LogDebug("Running ProteinProphet", 2);

                // ReSharper disable CommentTypo
                // ReSharper disable StringLiteralTypo

                var arguments = new StringBuilder();

                // Closed search without TMT or iTRAQ; also, open search:
                // --maxppmdiff 2000000 --output combined

                // Closed search, with TMT or iTRAQ:
                // --maxppmdiff 2000000 --minprob 0.5 --output combined

                arguments.Append("proteinprophet --maxppmdiff 2000000");

                if (options.ReporterIonMode != ReporterIonModes.Disabled && !options.OpenSearch)
                {
                    arguments.Append(" --minprob 0.5");
                }

                arguments.Append(" --output combined");

                // ReSharper restore StringLiteralTypo
                // ReSharper restore CommentTypo

                if (peptideProphetPepXmlFiles.Count > 1)
                {
                    // Create a text file listing the .pep.xml files, one per line (thus reducing the length of the command line)
                    var fileListFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "filelist_proteinprophet.txt"));

                    using (var writer = new StreamWriter(new FileStream(fileListFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        foreach (var pepXmlFile in peptideProphetPepXmlFiles)
                        {
                            writer.WriteLine(pepXmlFile.FullName);
                        }
                    }

                    arguments.AppendFormat(" {0}", fileListFile.FullName);

                    mJobParams.AddResultFileToSkip(fileListFile.Name);
                }
                else
                {
                    // Simply append the .pep.xml file name
                    foreach (var pepXmlFile in peptideProphetPepXmlFiles)
                    {
                        arguments.AppendFormat(" {0}", pepXmlFile.FullName);
                    }
                }

                // ReSharper disable CommentTypo

                // Note that ProteinProphet creates a GUID-named subdirectory below the user's temp directory
                // Inside this directory, files batchcoverage.exe and DatabaseParser.exe are created
                // When ProteinProphet finishes, these files are deleted
                // Antivirus scanning processes sometimes lock these files, preventing their deletion, leading to errors like these:

                // ERRO[16:24:59] remove C:\Users\D3L243\AppData\Local\Temp\06c436c4-ccee-42bd-b2e7-cc9e23e14ab5\batchcoverage.exe: The process cannot access the file because it is being used by another process.
                // ERRO[16:25:43] remove C:\Users\D3L243\AppData\Local\Temp\06c436c4-ccee-42bd-b2e7-cc9e23e14ab5\DatabaseParser.exe: The process cannot access the file because it is being used by another process.

                // These errors can be safely ignored

                // ReSharper restore CommentTypo

                var success = RunPhilosopher(PhilosopherToolType.ProteinProphet, arguments.ToString(), "run ProteinProphet");

                // Look for the ProteinProphet results file
                var proteinGroupsFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, PROTEIN_PROPHET_RESULTS_FILE));

                if (!success)
                {
                    // ReSharper disable once CommentTypo

                    // If the ProteinProphet results file exists, but success is false, this is most likely because ProteinProphet was unable to delete either BatchCoverage.exe or DatabaseParser (as described above)
                    // Examine file Philosopher_ConsoleOutput.txt to look for errors of the form:
                    //  remove C:\Users\D3L243\AppData\Local\Temp\a7785d27-366e-4048-a23f-c1867454f9f0\batchcoverage.exe: The process cannot access the file because it is being used by another process.

                    var removeFileError = mConsoleOutputFileParser.FileHasRemoveFileError(Path.Combine(mWorkingDirectory.FullName, PHILOSOPHER_CONSOLE_OUTPUT));

                    if (removeFileError && proteinGroupsFile.Exists)
                    {
                        // Ignore the non-zero error code from Philosopher since Protein Prophet created combined.prot.xml
                    }
                    else
                    {
                        return false;
                    }
                }

                if (!proteinGroupsFile.Exists)
                {
                    LogError("ProteinProphet results file not found: " + proteinGroupsFile.Name);
                    return false;
                }

                // Zip the ProteinProphet results file, combined.prot.xml

                var zipFilePath = Path.Combine(mWorkingDirectory.FullName, "ProteinProphet_Protein_Groups.zip");

                var fileZipped = mZipTools.ZipFile(proteinGroupsFile.FullName, false, zipFilePath);

                if (!fileZipped)
                {
                    return false;
                }

                mJobParams.AddResultFileToSkip(PROTEIN_PROPHET_RESULTS_FILE);
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in RunProteinProphet", ex);
                return false;
            }
        }

        // ReSharper disable once InconsistentNaming

        /// <summary>
        /// Run PTM prophet on each .pep.xml file tracked by peptideProphetPepXmlFiles
        /// </summary>
        /// <param name="peptideProphetPepXmlFiles">Peptide prophet .pepXML files</param>
        /// <param name="options">Processing options</param>
        /// <returns>True if successful, false if error</returns>
        private bool RunPTMProphet(
            List<FileInfo> peptideProphetPepXmlFiles,
            FragPipeOptions options)
        {
            try
            {
                if (!File.Exists(mPtmProphetProgLoc))
                {
                    LogError("PTM Prophet program does not exist: ", mPtmProphetProgLoc);
                }

                var successCount = 0;

                // Run PTM Prophet on each dataset

                foreach (var pepXmlFile in peptideProphetPepXmlFiles)
                {
                    var success = RunPTMProphetOnDataset(
                        pepXmlFile,
                        options);

                    if (!success)
                        continue;

                    successCount++;
                }

                return successCount == peptideProphetPepXmlFiles.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in RunPTMProphet", ex);
                return false;
            }
        }

        // ReSharper disable once InconsistentNaming
        /// <summary>
        /// Run PTM prophet on the given .pep.xml file
        /// </summary>
        /// <param name="pepXmlFile">Peptide Prophet or Percolator results file, e.g. interact-DatasetName.pep.xml</param>
        /// <param name="options">Processing options</param>
        /// <returns>True if successful, false if an error</returns>
        private bool RunPTMProphetOnDataset(FileInfo pepXmlFile, FragPipeOptions options)
        {
            const string PEP_XML_FILE_SUFFIX = ".pep.xml";
            const string MOD_PEP_XML_FILE_SUFFIX = ".mod.pep.xml";

            try
            {
                LogDebug("Running PTM Prophet", 2);

                // ReSharper disable CommentTypo

                // Example command line:
                // C:\DMS_Programs\MSFragger\fragpipe\tools\PTMProphet\PTMProphetParser.exe KEEPOLD STATIC EM=1 NIONS=b STY:79.966331,M:15.9949 MINPROB=0.5 MAXTHREADS=1 interact-DatasetName.pep.xml interact-DatasetName.mod.pep.xml

                // ReSharper restore CommentTypo

                if (!pepXmlFile.Exists)
                {
                    // Peptide Prophet .pep.xml file not found
                    LogError("Cannot run PTM Prophet; Peptide Prophet {0} file not found: {1}", PEP_XML_FILE_SUFFIX, pepXmlFile.FullName);
                    return false;
                }

                if (!pepXmlFile.FullName.EndsWith(PEP_XML_FILE_SUFFIX))
                {
                    // Peptide Prophet result file does not end in '.pep.xml'
                    LogError("Cannot run PTM Prophet; Peptide Prophet result file does not end in '{0}': {1}", PEP_XML_FILE_SUFFIX, pepXmlFile.FullName);
                    return false;
                }

                // Define the PTM Prophet output file
                var modPepXmlFile = new FileInfo(pepXmlFile.FullName.Replace(PEP_XML_FILE_SUFFIX, MOD_PEP_XML_FILE_SUFFIX));

                // ReSharper disable StringLiteralTypo

                var arguments = string.Format(
                    "KEEPOLD STATIC EM=1 NIONS=b STY:79.966331,M:15.9949 MINPROB=0.5 MAXTHREADS=1 " +
                    "{0} " +
                    "{1}",
                    pepXmlFile.Name,
                    modPepXmlFile.Name);

                // ReSharper restore StringLiteralTypo

                InitializeCommandRunner(
                    pepXmlFile.Directory,
                    Path.Combine(mWorkingDirectory.FullName, PTM_PROPHET_CONSOLE_OUTPUT),
                    CmdRunnerModes.PtmProphet);

                // PTM Prophet reports all of its messages via the console error stream
                // Instruct mCmdRunner to treat them as normal messages
                mCmdRunner.RaiseConsoleErrorEvents = false;

                LogCommandToExecute(pepXmlFile.Directory, mPtmProphetProgLoc, arguments, options.WorkingDirectoryPadWidth);

                // Start the program and wait for it to finish
                // However, while it's running, LoopWaiting will get called via events
                var processingSuccess = mCmdRunner.RunProgram(mPtmProphetProgLoc, arguments, "PTMProphetParser", true);

                if (!mConsoleOutputFileParsed)
                {
                    ParseConsoleOutputFile();
                }

                if (!string.IsNullOrEmpty(mConsoleOutputFileParser.ConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputFileParser.ConsoleOutputErrorMsg);
                }

                var datasetName = pepXmlFile.Name.StartsWith("interact-", StringComparison.OrdinalIgnoreCase)
                    ? Path.GetFileNameWithoutExtension(pepXmlFile.Name).Substring("interact-".Length)
                    : pepXmlFile.Name;

                UpdateCombinedPTMProphetConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, datasetName);

                mCmdRunner.RaiseConsoleErrorEvents = true;

                if (processingSuccess)
                {
                    if (modPepXmlFile.Exists)
                        return true;

                    LogError("PTM Prophet results file not found: " + modPepXmlFile.Name);
                    return false;
                }

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("PTM Prophet returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to PTM Prophet failed (but exit code is 0)");
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in RunPTMProphetOnDataset", ex);
                mCmdRunner.RaiseConsoleErrorEvents = true;
                return false;
            }
        }

        // ReSharper disable once InconsistentNaming
        private bool RunPTMShepherd(FragPipeOptions options)
        {
            try
            {
                LogDebug("Running PTMShepherd", 2);

                // ReSharper disable CommentTypo
                // ReSharper disable StringLiteralTypo

                // Run PTMShepherd, example command line:
                // java -Dbatmass.io.libs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-4.1\ext\thermo" -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\ptmshepherd-1.2.6.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.28.12.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\commons-math3-3.6.1.jar" edu.umich.andykong.ptmshepherd.PTMShepherd "C:DMS_WorkDir\shepherd.config"

                // Find the thermo lib directory, typically C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-4.1\ext\thermo
                if (!options.LibraryFinder.FindVendorLibDirectory("thermo", out var thermoLibDirectory))
                    return false;

                // Find the PTM-Shepherd jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\ptmshepherd-1.2.6.jar
                if (!options.LibraryFinder.FindJarFilePtmShepherd(out var jarFilePtmShepherd))
                    return false;

                // Find the Batmass-IO jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.28.12.jar

                // ReSharper disable once IdentifierTypo
                if (!options.LibraryFinder.FindJarFileBatmassIO(out var jarFileBatmassIO))
                    return false;

                // Find the Commons-Math3 jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\commons-math3-3.6.1.jar
                if (!options.LibraryFinder.FindJarFileCommonsMath(out var jarFileCommonsMath))
                    return false;

                // ReSharper restore CommentTypo

                // Create the PTM-Shepherd config file
                var ptmShepherdConfigFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "shepherd.config"));

                using (var writer = new StreamWriter(new FileStream(ptmShepherdConfigFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine("database = " + mFastaFilePath);

                    foreach (var experimentGroup in mExperimentGroupWorkingDirectories)
                    {
                        // dataset = ExperimentGroupA C:\DMS_WorkDir\Results\ExperimentGroupA\psm.tsv WorkingDirectoryPath");

                        writer.WriteLine("dataset = {0} {1} {2}",
                            experimentGroup.Key,
                            Path.Combine(experimentGroup.Value.FullName, "psm.tsv"),
                            mWorkingDirectory.FullName);
                    }

                    writer.WriteLine();
                    writer.WriteLine("annotation-common = false");
                    writer.WriteLine("annotation-custom = false");
                    writer.WriteLine("annotation-glyco = false");
                    writer.WriteLine("annotation_file = unimod");
                    writer.WriteLine("annotation_tol = 0.01");
                    writer.WriteLine("cap_y_ions = ");
                    writer.WriteLine("compare_betweenRuns = false");
                    writer.WriteLine("diag_ions = ");
                    writer.WriteLine("glyco_mode = false");
                    writer.WriteLine("histo_bindivs = 5000");
                    writer.WriteLine("histo_normalizeTo = psms");
                    writer.WriteLine("histo_smoothbins = 2");
                    writer.WriteLine("iontype_a = 0");
                    writer.WriteLine("iontype_b = 1");
                    writer.WriteLine("iontype_c = 0");
                    writer.WriteLine("iontype_x = 0");
                    writer.WriteLine("iontype_y = 1");
                    writer.WriteLine("iontype_z = 0");
                    writer.WriteLine("isotope_error = 0");
                    writer.WriteLine("isotope_states = ");
                    writer.WriteLine("localization_allowed_res = all");
                    writer.WriteLine("localization_background = 4");
                    writer.WriteLine("mass_offsets = 0");
                    writer.WriteLine("normalization-psms = true");
                    writer.WriteLine("normalization-scans = false");
                    writer.WriteLine("output_extended = false");
                    writer.WriteLine("peakpicking_mass_units = 0");
                    writer.WriteLine("peakpicking_minPsm = 10");
                    writer.WriteLine("peakpicking_promRatio = 0.3");
                    writer.WriteLine("peakpicking_topN = 500");
                    writer.WriteLine("peakpicking_width = 0.002");
                    writer.WriteLine("precursor_mass_units = 0");
                    writer.WriteLine("precursor_tol = 0.01");
                    writer.WriteLine("remainder_masses = ");
                    writer.WriteLine("run-shepherd = true");
                    writer.WriteLine("spectra_condPeaks = 100");
                    writer.WriteLine("spectra_condRatio = 0.01");
                    writer.WriteLine("spectra_maxfragcharge = 2");
                    writer.WriteLine("spectra_ppmtol = 20");
                    writer.WriteLine("threads = 4");
                    writer.WriteLine("varmod_masses = ");
                }

                var arguments = string.Format("{0} -Dbatmass.io.libs.thermo.dir=\"{1}\" -cp \"{2};{3};{4}\" edu.umich.andykong.ptmshepherd.PTMShepherd {5}",
                    options.JavaProgLoc,
                    thermoLibDirectory.FullName,
                    jarFilePtmShepherd.FullName,
                    jarFileBatmassIO.FullName,
                    jarFileCommonsMath.FullName,
                    ptmShepherdConfigFile.FullName
                );

                // ReSharper restore StringLiteralTypo

                InitializeCommandRunner(
                    mWorkingDirectory,
                    Path.Combine(mWorkingDirectory.FullName, PTM_SHEPHERD_CONSOLE_OUTPUT),
                    CmdRunnerModes.PtmShepherd);

                LogCommandToExecute(mWorkingDirectory, options.JavaProgLoc, arguments);

                var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments, "Java", true);

                if (!mConsoleOutputFileParsed)
                {
                    ParseConsoleOutputFile();
                }

                if (!string.IsNullOrEmpty(mConsoleOutputFileParser.ConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputFileParser.ConsoleOutputErrorMsg);
                }

                if (processingSuccess)
                {
                    // ToDo: Verify that the PTM Shepherd results file was created
                    var outputFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "PTM_Shepherd_Results.txt"));

                    //if (!outputFile.Exists)
                    //{
                    //    LogError("IonQuant results file not found: " + outputFile.Name);
                    //    return false;
                    //}

                    return true;
                }

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("Java returned a non-zero exit code while running PTM-Shepherd: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to Java failed while running PTM-Shepherd (but exit code is 0)");
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in RunPTMShepherd", ex);
                return false;
            }
        }

        private bool RunReportGeneration(FragPipeOptions options)
        {
            try
            {
                LogDebug("Generating MSFragger report files", 2);

                // Generate a separate report for each experiment group
                var successCount = 0;

                // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
                // ReSharper disable once LoopCanBeConvertedToQuery
                foreach (var experimentGroupDirectory in mExperimentGroupWorkingDirectories.Values)
                {
                    // ReSharper disable CommentTypo

                    // Example command line:
                    // C:\DMS_Programs\MSFragger\fragpipe\tools\philosopher\philosopher.exe report

                    // ReSharper restore CommentTypo

                    var arguments = new StringBuilder();
                    arguments.Append("report");

                    if (mJobParams.GetJobParameter("Philosopher", "PhilosopherIncludeDecoys", false))
                    {
                        // Include decoy peptides and proteins in the reports
                        arguments.Append(" --decoys");
                    }

                    // ReSharper disable once StringLiteralTypo
                    var msStatsEnabled = mJobParams.GetJobParameter("Philosopher", "PhilosopherGenerateMSstats", false);

                    if (msStatsEnabled)
                    {
                        // ReSharper disable once CommentTypo

                        // Generate MSstats files
                        arguments.Append(" --msstats");
                    }

                    var success = RunPhilosopher(
                        PhilosopherToolType.GenerateReport,
                        arguments.ToString(),
                        "generate report files",
                        experimentGroupDirectory,
                        options.WorkingDirectoryPadWidth);

                    if (!success)
                    {
                        continue;
                    }

                    // Verify that report files were created
                    var outputFiles = new List<FileInfo>
                    {
                        new(Path.Combine(experimentGroupDirectory.FullName, "psm.tsv")),
                        new(Path.Combine(experimentGroupDirectory.FullName, "ion.tsv")),
                        new(Path.Combine(experimentGroupDirectory.FullName, "peptide.tsv"))
                    };

                    if (options.RunProteinProphet)
                    {
                        outputFiles.Add(new FileInfo(Path.Combine(experimentGroupDirectory.FullName, "protein.tsv")));
                    }

                    if (msStatsEnabled)
                    {
                        outputFiles.Add(new FileInfo(Path.Combine(experimentGroupDirectory.FullName, "msstats.csv")));
                    }

                    var outputFilesExist = ValidateOutputFilesExist("Philosopher report", outputFiles);

                    if (outputFilesExist)
                    {
                        successCount++;
                    }
                }

                return successCount == mExperimentGroupWorkingDirectories.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in RunReportGeneration", ex);
                return false;
            }
        }

        /// <summary>
        /// Use Philosopher to filter the data
        /// </summary>
        /// <remarks>
        /// This will create four binary files in the .meta subdirectory for each experiment group (or in the single .meta directory if just one experiment group)
        /// .meta\pro.bin
        /// .meta\psm.bin
        /// .meta\ion.bin
        /// .meta\pep.bin
        /// </remarks>
        /// <param name="options">Processing options</param>
        /// <param name="usedProteinProphet">True if protein prophet was used</param>
        private bool RunResultsFilter(FragPipeOptions options, bool usedProteinProphet)
        {
            try
            {
                LogDebug("Filtering MSFragger Results", 2);

                var successCount = 0;
                var iteration = 0;

                var arguments = new StringBuilder();

                // ReSharper disable CommentTypo

                // The first time we call philosopher.exe with the filter command, use argument --razor
                // That will create file .meta\razor.bin in the first experiment group's working directory
                // On subsequent calls, use --dbbin and --probin and reference the first working directory
                // For example: --dbbin C:\FragPipe_Test5\Results\Leaf --probin C:\FragPipe_Test5\Results\Leaf --razor--razor

                // Note: versions prior to Philosopher v5.0 only used --probin
                // The --dbbin argument is new for v5.0

                // In FragPipe v19 and earlier, we would use --razorbin and reference file razor.bin in the first experiment group's .meta directory
                // For example: --razorbin C:\DMS_WorkDir\Leaf\.meta\razor.bin

                // ReSharper restore CommentTypo

                var firstWorkingDirectoryPath = mExperimentGroupWorkingDirectories.Values.First().FullName;

                foreach (var experimentGroupDirectory in mExperimentGroupWorkingDirectories.Values)
                {
                    arguments.Clear();
                    iteration++;

                    // ReSharper disable CommentTypo

                    // Closed search, proteinprophet disabled
                    // filter --tag XXX_ --pepxml

                    // Closed search, without match between runs:
                    // filter --sequential --picked --prot 0.01 --tag XXX_

                    // Closed search, with match between runs enabled (FragPipe v20 used "--picked --prot 0.01" when MBR is enabled, but v21 does not use --picked):
                    // filter --sequential --prot 0.01 --tag XXX_

                    // Open search:
                    // filter --sequential --prot 0.01 --mapmods --tag XXX_

                    // ReSharper restore CommentTypo

                    arguments.Append("filter");

                    if (usedProteinProphet)
                    {
                        arguments.Append(" --sequential");
                    }

                    // FragPipe v20 included "--picked" when match between runs was enabled, but older versions and v21 do not include --picked

                    if (!options.MatchBetweenRuns && !options.OpenSearch)
                    {
                        arguments.Append(" --picked");
                    }

                    var psmLevelFDR = GetJobParameterOrDefault("Philosopher", "PhilosopherFDR_PSM", 0.01f, out var hasPSMLevelFDR);
                    var peptideLevelFDR = GetJobParameterOrDefault("Philosopher", "PhilosopherFDR_Peptide", 0.01f, out var hasPeptideLevelFDR);
                    var proteinLevelFDR = GetJobParameterOrDefault("Philosopher", "PhilosopherFDR_Protein", 0.01f, out var hasProteinLevelFDR);

                    if (hasPSMLevelFDR)
                    {
                        arguments.AppendFormat(" --psm {0:0.00}", psmLevelFDR);
                    }

                    if (hasPeptideLevelFDR)
                    {
                        arguments.AppendFormat(" --pep {0:0.00}", peptideLevelFDR);
                    }

                    if (usedProteinProphet || hasProteinLevelFDR)
                    {
                        arguments.AppendFormat(" --prot {0:0.00}", proteinLevelFDR);
                    }

                    if (options.OpenSearch)
                    {
                        // ReSharper disable once StringLiteralTypo
                        arguments.Append(" --mapmods");
                    }

                    arguments.Append(" --tag XXX_");

                    arguments.AppendFormat(" --pepxml {0}", experimentGroupDirectory.FullName);

                    if (usedProteinProphet)
                    {
                        // ReSharper disable StringLiteralTypo

                        arguments.AppendFormat(" --protxml {0}", Path.Combine(mWorkingDirectory.FullName, PROTEIN_PROPHET_RESULTS_FILE));

                        if (iteration == 1)
                        {
                            // This will create file razor.bin in the .meta directory of the first experiment group
                            arguments.AppendFormat(" --razor");
                        }
                        else
                        {
                            // ReSharper disable CommentTypo

                            // Use the existing razor.bin file
                            arguments.AppendFormat(" --dbbin {0} --probin {0} --razor", firstWorkingDirectoryPath);

                            // ReSharper restore CommentTypo
                        }

                        // ReSharper restore StringLiteralTypo
                    }

                    var success = RunPhilosopher(
                        PhilosopherToolType.ResultsFilter,
                        arguments.ToString(),
                        "filter results",
                        experimentGroupDirectory,
                        options.WorkingDirectoryPadWidth);

                    if (!success)
                    {
                        continue;
                    }

                    // The filter command should have created four .bin files in the .meta directory:
                    //   psm.bin, ion.bin, pep.bin, and pro.bin

                    // Verify that file psm.bin was created

                    var outputFile = new FileInfo(Path.Combine(experimentGroupDirectory.FullName, META_SUBDIRECTORY, "psm.bin"));

                    if (!outputFile.Exists)
                    {
                        LogError("Filtered results file not found in the .meta directory: " + outputFile.Name);
                        return false;
                    }

                    successCount++;
                }

                return successCount == mExperimentGroupWorkingDirectories.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in RunResultsFilter", ex);
                return false;
            }
        }

        private bool RunTmtIntegrator(FragPipeOptions options)
        {
            try
            {
                LogDebug("Running TMT-Integrator", 2);

                // ReSharper disable CommentTypo

                // Example command line:
                // java -Xmx14G -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\tmt-integrator-4.0.5.jar" TMTIntegrator C:\DMS_WorkDir\Results\tmt-integrator-conf.yml C:\DMS_WorkDir\Results\ExperimentGroupA\psm.tsv C:\DMS_WorkDir\Results\ExperimentGroupB\psm.tsv

                // ReSharper restore CommentTypo

                var plex = GetReporterIonChannelCount(options.ReporterIonMode);

                int memoryToReserveGB;

                var freeMemoryMB = Global.GetFreeMemoryMB();

                if (TMT_INTEGRATOR_MEMORY_SIZE_GB * 1024 < freeMemoryMB)
                {
                    memoryToReserveGB = TMT_INTEGRATOR_MEMORY_SIZE_GB;
                }
                else if (Global.RunningOnDeveloperComputer())
                {
                    memoryToReserveGB = (int)Math.Floor(freeMemoryMB * 0.9 / 1024);

                    LogWarning(
                        "We typically allocate {0} GB for TMT-Integrator, but system free memory is currently {1:F1} GB; " +
                        "adjusting value to {2} GB since running on a developer computer",
                        TMT_INTEGRATOR_MEMORY_SIZE_GB, freeMemoryMB / 1024, memoryToReserveGB);
                }
                else
                {
                    LogWarning(
                        "We typically allocate {0} GB for TMT-Integrator, but system free memory is currently {1:F1} GB; Java might end with an error",
                        TMT_INTEGRATOR_MEMORY_SIZE_GB, freeMemoryMB / 1024);

                    memoryToReserveGB = TMT_INTEGRATOR_MEMORY_SIZE_GB;
                }

                var arguments = new StringBuilder();
                arguments.AppendFormat("-Xmx{0}G -cp \"{1}\" TMTIntegrator", memoryToReserveGB, mTmtIntegratorProgLoc);

                // Create the TMT-Integrator config file
                var tmtIntegratorConfigFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "tmt-integrator-conf.yml"));

                var phosphoSearch = PhosphoModDefined(options.FraggerOptions.VariableModifications);

                // ReSharper disable once CommentTypo

                string groupBy;                 // Summarization level: 0=aggregation at the gene level, -1=generate reports at all levels, see below for others
                string minPepProb;              // Minimum PSM probability
                string minIntensityPercent;     // Minimum Intensity percent; remove low intensity PSMs based on summed TMT reporter ion intensity
                string minPurity;               // Ion purity score threshold
                string minSiteProbability;      // Site localization confidence threshold
                string ptmModTag;

                if (phosphoSearch)
                {
                    groupBy = "-1";
                    minPepProb = "0.5";
                    minIntensityPercent = "0.025";
                    minPurity = "0.5";
                    minSiteProbability = options.RunPTMProphet ? "0.75" : "-1";
                    ptmModTag = "S(79.9663),T(79.9663),Y(79.9663)";
                }
                else
                {
                    groupBy = "0";
                    minPepProb = "0.9";
                    minIntensityPercent = "0.05";
                    minPurity = "0.5";
                    minSiteProbability = "-1";
                    ptmModTag = "none";
                }

                using (var writer = new StreamWriter(new FileStream(tmtIntegratorConfigFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // ReSharper disable StringLiteralTypo

                    writer.WriteLine("tmtintegrator:");
                    writer.WriteLine("  add_Ref: 1                               # add an artificial reference channel if there is no reference channel or export raw abundance (-2: export raw abundance; -1: don't add the reference; 0: use summation as the reference; 1: use average as the reference; 2: use median as the reference)");
                    writer.WriteLine("  aggregation_method: 0                    # the aggregation method from the PSM level to the specified level (0: median, 1: weighted-ratio)");
                    writer.WriteLine("  allow_overlabel: true                    # allow PSMs with TMT on S (when overlabelling on S was allowed in the database search)");
                    writer.WriteLine("  allow_unlabeled: false                   # allow PSMs without TMT tag or acetylation on the peptide n-terminus ");
                    writer.WriteLine("  best_psm: true                           # keep the best PSM only (highest summed TMT intensity) among all redundant PSMs within the same LC-MS run");
                    writer.WriteLine("{0,-42} {1}", string.Format("  channel_num: {0}", plex), "# number of channels in the multiplex (e.g. 10, 11)");
                    writer.WriteLine("  glyco_qval: -1                           # (optional) filter modified PSMs to those with glycan q-value less than provided value. 0 <= value <= 1. Value of -1 or not specified ignores");   // Introduced with TMT-Integrator 3.3.3
                    writer.WriteLine("{0,-42} {1}", string.Format("  groupby: {0}", groupBy), "# level of data summarization(0: PSM aggregation to the gene level; 1: protein; 2: peptide sequence; 3: multiple PTM sites; 4: single PTM site; 5: multi-mass (for glycosylation); -1: generate reports at all levels)");
                    writer.WriteLine("  log2transformed: true                    # report ratio and abundance reports in the log2 scale");                                                                                           // Introduced with TMT-Integrator 4.0
                    writer.WriteLine("  max_pep_prob_thres: 0.9                  # the threshold for maximum peptide probability");
                    writer.WriteLine("  memory: 11                               # memory allocation, in GB");
                    writer.WriteLine("  min_ntt: 0                               # minimum allowed number of enzymatic termini");
                    writer.WriteLine("{0,-42} {1}", string.Format("  min_pep_prob: {0}", minPepProb), "# minimum PSM probability threshold (in addition to FDR-based filtering by Philosopher)");
                    writer.WriteLine("{0,-42} {1}", string.Format("  min_percent: {0}", minIntensityPercent), "# remove low intensity PSMs (e.g. value of 0.05 indicates removal of PSMs with the summed TMT reporter ions intensity in the lowest 5% of all PSMs)");
                    writer.WriteLine("{0,-42} {1}", string.Format("  min_purity: {0}", minPurity), "# ion purity score threshold");
                    writer.WriteLine("{0,-42} {1}", string.Format("  min_site_prob: {0}", minSiteProbability), "# site localization confidence threshold (-1: for Global; 0: as determined by the search engine; above 0 (e.g. 0.75): PTMProphet probability, to be used with phosphorylation only)");
                    writer.WriteLine("{0,-42} {1}", string.Format("  mod_tag: {0}", ptmModTag), "# PTM info for generation of PTM-specific reports (none: for Global data; S(79.9663),T(79.9663),Y(79.9663): for Phospho; K(42.0105): for K-Acetyl; M(15.9949): for M-Oxidation; N-glyco: for N-glycosylation; O-glyco: for O-glycosylation)");
                    writer.WriteLine("  ms1_int: true                            # use MS1 precursor ion intensity (if true) or MS2 reference intensity (if false) as part of the reference sample abundance estimation ");
                    writer.WriteLine("  outlier_removal: true                    # perform outlier removal");
                    writer.WriteLine("{0,-42} {1}", string.Format("  output: {0}", mWorkingDirectory.FullName), "# the location of output files");
                    writer.WriteLine("{0,-76} {1}", string.Format("  path: {0}", mTmtIntegratorProgLoc), "# path to TMT-Integrator jar");
                    writer.WriteLine("  prefix: XXX_                             # the prefix for decoy sequences");                                                                                                                 // Introduced with TMT-Integrator 4.0
                    writer.WriteLine("  print_RefInt: false                      # print individual reference sample abundance estimates for each multiplex in the final reports (in addition to the combined reference sample abundance estimate)");
                    writer.WriteLine("  prot_exclude: none                       # exclude proteins with specified tags at the beginning of the accession number (e.g. none: no exclusion; sp|,tr| : exclude protein with sp| or tr|)");
                    writer.WriteLine("  prot_norm: 1                             # normalization ( -1: generate reports with all normalization options; 0: None; 1: MC (median centering); 2: GN (median centering + variance scaling); 3: SL (sample loading); 4: IRS (internal reference scaling); 5: SL+IRS (sample loading and internal reference scaling))");
                    writer.WriteLine("{0,-76} {1}", string.Format("  protein_database: {0}", mFastaFilePath), "# protein fasta file");
                    writer.WriteLine("  psm_norm: false                          # perform additional retention time-based normalization at the PSM level");
                    writer.WriteLine("  ref_tag: Bridge                          # unique tag for identifying the reference channel (Bridge sample added to each multiplex)");
                    writer.WriteLine("  top3_pep: true                           # use top 3 most intense peptide ions as part of the reference sample abundance estimation");
                    writer.WriteLine("  unique_gene: 0                           # additional, gene-level uniqueness filter (0: allow all PSMs; 1: remove PSMs mapping to more than one GENE with evidence of expression in the dataset; 2:remove all PSMs mapping to more than one GENE in the fasta file)");
                    writer.WriteLine("  unique_pep: false                        # allow PSMs with unique peptides only (if true) or unique plus razor peptides (if false), as classified by Philosopher and defined in PSM.tsv files");
                    writer.WriteLine("  use_glycan_composition: false            # (optional) for multi-mass report, create index using glycan composition (from observed mod column) instead of mass");                             // Introduced with TMT-Integrator 3.3.3

                    // ReSharper restore StringLiteralTypo
                }

                arguments.AppendFormat(" {0}", tmtIntegratorConfigFile.Name);

                // Since the Windows command line cannot be over 8192 characters long, populate a list of string builders with the space separated list of input files
                // If there is a small number of datasets, there will only be one item in inputFileLists

                var inputFileLists = new List<StringBuilder>();

                const int maximumFileListLength = 7000;

                var currentFileList = new StringBuilder();

                foreach (var experimentGroup in mExperimentGroupWorkingDirectories)
                {
                    currentFileList.AppendFormat(" {0}", Path.Combine(experimentGroup.Value.FullName, "psm.tsv"));

                    if (currentFileList.Length <= maximumFileListLength)
                        continue;

                    inputFileLists.Add(currentFileList);

                    currentFileList = new StringBuilder();
                }

                if (currentFileList.Length > 0)
                {
                    inputFileLists.Add(currentFileList);
                }

                if (inputFileLists.Count == 0)
                {
                    LogError("List inputFileLists is empty; either mExperimentGroupWorkingDirectories is empty, or there is a programming bug in RunTmtIntegrator");
                    return false;
                }

                var groupNumber = 0;

                foreach (var inputFileList in inputFileLists)
                {
                    groupNumber++;

                    var success = RunTmtIntegratorWork(options.JavaProgLoc, arguments, inputFileList, out var resultFiles);

                    if (!success)
                        return false;

                    // When inputFileLists has more than one item, rename the TMT Integrator result files to avoid naming conflicts

                    var filesToMove = inputFileLists.Count == 1
                        ? resultFiles :
                        RenameTmtIntegratorResultFiles(groupNumber, resultFiles);

                    // Move the files into a subdirectory named "tmt-report"
                    var targetDirectory = new DirectoryInfo(Path.Combine(mWorkingDirectory.FullName, TMT_REPORT_DIRECTORY));

                    if (!targetDirectory.Exists)
                        targetDirectory.Create();

                    foreach (var tsvFile in filesToMove)
                    {
                        var newPath = Path.Combine(targetDirectory.FullName, tsvFile.Name);
                        tsvFile.MoveTo(newPath);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in RunTmtIntegrator", ex);
                return false;
            }
        }

        private bool RunTmtIntegratorWork(
            string javaProgLoc,
            StringBuilder arguments,
            StringBuilder inputFileList,
            out List<FileInfo> resultFiles)
        {
            // inputFileList should start with a space
            var argumentsWithFiles = string.Format("{0}{1}", arguments, inputFileList);

            InitializeCommandRunner(
                mWorkingDirectory,
                Path.Combine(mWorkingDirectory.FullName, JAVA_CONSOLE_OUTPUT),
                CmdRunnerModes.TmtIntegrator,
                500);

            LogCommandToExecute(mWorkingDirectory, javaProgLoc, argumentsWithFiles);

            var processingSuccess = mCmdRunner.RunProgram(javaProgLoc, argumentsWithFiles, "Java", true);

            if (!mConsoleOutputFileParsed)
            {
                ParseConsoleOutputFile();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputFileParser.ConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputFileParser.ConsoleOutputErrorMsg);
            }

            const string currentStep = "TMT-Integrator";
            UpdateCombinedJavaConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, currentStep);

            resultFiles = new List<FileInfo>();

            if (processingSuccess)
            {
                // ReSharper disable CommentTypo
                // These two files are created when tmt-integrator-conf.yml has "groupby: 0"
                // If "groupby: -1" is used, additional _MD.tsv files will be created

                var abundanceFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "abundance_gene_MD.tsv"));
                var ratioFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "ratio_gene_MD.tsv"));

                // ReSharper restore CommentTypo

                if (abundanceFile.Exists)
                {
                    resultFiles.Add(abundanceFile);
                }
                else
                {
                    LogError("TMT Integrator abundance file not found: " + abundanceFile.Name);
                    return false;
                }

                if (ratioFile.Exists)
                {
                    resultFiles.Add(ratioFile);
                }
                else
                {
                    LogError("TMT Integrator ratio file not found: " + ratioFile.Name);
                    return false;
                }

                // Look for additional _MD.tsv files

                // ReSharper disable once LoopCanBeConvertedToQuery
                foreach (var tsvFile in mWorkingDirectory.GetFiles("*_MD.tsv"))
                {
                    if (tsvFile.Name.Equals(abundanceFile.Name) || tsvFile.Name.Equals(ratioFile.Name))
                        continue;

                    resultFiles.Add(tsvFile);
                }

                return true;
            }

            if (mCmdRunner.ExitCode != 0)
            {
                LogWarning("Java returned a non-zero exit code while calling tmt-integrator: " + mCmdRunner.ExitCode);
            }
            else
            {
                LogWarning("Call to Java failed while calling tmt-integrator (but exit code is 0)");
            }

            return false;
        }

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
                // Version will be of the form
                // Philosopher v4.1.0; philosopher.exe: 2021-11-02 05:18:30 PM; tmt-integrator-3.2.1.jar: 2021-11-17 02:01:34 PM
                return SetStepTaskToolVersion(mConsoleOutputFileParser.PhilosopherVersion, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Error calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private void UpdateCombinedConsoleOutputFile(
            string consoleOutputFilepath,
            string combinedFileName,
            string currentStep,
            PhilosopherToolType currentTool = PhilosopherToolType.Undefined)
        {
            try
            {
                var consoleOutputFile = new FileInfo(consoleOutputFilepath);

                if (!consoleOutputFile.Exists)
                {
                    LogWarning("UpdateCombinedPhilosopherConsoleOutput: ConsoleOutput file not found: " + consoleOutputFilepath);
                    return;
                }

                var combinedFilePath = Path.Combine(mWorkingDirectory.FullName, combinedFileName);

                using var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
                using var writer = new StreamWriter(new FileStream(combinedFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite));

                if (currentTool != PhilosopherToolType.ShowVersion)
                {
                    writer.WriteLine();
                    writer.WriteLine();
                    writer.WriteLine("### {0} ###", currentStep);
                    writer.WriteLine();
                }

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        writer.WriteLine();
                        continue;
                    }

                    if (currentTool == PhilosopherToolType.Undefined)
                    {
                        writer.WriteLine(dataLine);
                        continue;
                    }

                    // Remove any color tags
                    var cleanedLine = mConsoleOutputFileParser.ColorTagMatcher.Replace(dataLine, string.Empty);
                    writer.WriteLine(cleanedLine);
                }
            }
            catch (Exception ex)
            {
                LogError("Error in UpdateCombinedConsoleOutputFile for " + combinedFileName, ex);
            }
        }

        private void UpdateCombinedJavaConsoleOutputFile(string consoleOutputFilepath, string currentStep)
        {
            UpdateCombinedConsoleOutputFile(consoleOutputFilepath, JAVA_CONSOLE_OUTPUT_COMBINED, currentStep);
        }

        private void UpdateCombinedPercolatorConsoleOutputFile(string consoleOutputFilepath, string currentStep)
        {
            UpdateCombinedConsoleOutputFile(consoleOutputFilepath, PERCOLATOR_CONSOLE_OUTPUT_COMBINED, currentStep);
        }

        private void UpdateCombinedPhilosopherConsoleOutputFile(string consoleOutputFilepath, string currentStep, PhilosopherToolType toolType)
        {
            UpdateCombinedConsoleOutputFile(consoleOutputFilepath, PHILOSOPHER_CONSOLE_OUTPUT_COMBINED, currentStep, toolType);
        }

        private void UpdateCombinedPTMProphetConsoleOutputFile(string consoleOutputFilepath, string currentStep)
        {
            UpdateCombinedConsoleOutputFile(consoleOutputFilepath, PTM_PROPHET_CONSOLE_OUTPUT_COMBINED, currentStep);
        }

        /// <summary>
        /// Edit the header line of the combined .tsv files to replace the working directory name with either the dataset name or "Aggregation"
        /// </summary>
        /// <param name="datasetCount">Dataset count</param>
        /// <returns>True if successful, false if an error</returns>
        private bool UpdateCombinedTsvFiles(int datasetCount)
        {
            var filesToUpdate = new List<string>
            {
                "combined_ion.tsv",
                "combined_modified_peptide.tsv",
                "combined_peptide.tsv",
                "combined_protein.tsv"
            };

            var successCount = 0;

            foreach (var tsvFile in filesToUpdate)
            {
                var success = UpdateCombinedTsvFileHeaders(new FileInfo(Path.Combine(mWorkingDirectory.FullName, tsvFile)), datasetCount);

                if (success)
                    successCount++;
            }

            return successCount == filesToUpdate.Count;
        }

        private bool UpdateCombinedTsvFileHeaders(FileSystemInfo tsvFile, int datasetCount)
        {
            try
            {
                if (!tsvFile.Exists)
                {
                    LogWarning("File not found; cannot update: " + tsvFile.FullName);
                }

                var temporaryFile = new FileInfo(tsvFile.FullName + ".updatedHeaders");

                using (var reader = new StreamReader(new FileStream(tsvFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(temporaryFile.FullName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)))
                {
                    var headerParsed = false;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (headerParsed)
                        {
                            writer.WriteLine(dataLine);
                            continue;
                        }

                        var updatedHeaders = new List<string>();

                        foreach (var header in dataLine.Split('\t'))
                        {
                            if (!header.StartsWith(mWorkingDirectory.Name))
                            {
                                updatedHeaders.Add(header);
                                continue;
                            }

                            var datasetName = datasetCount > 1 ? "Aggregation" : mDatasetName;
                            updatedHeaders.Add(header.Replace(mWorkingDirectory.Name, datasetName));
                        }

                        writer.WriteLine(string.Join("\t", updatedHeaders));

                        headerParsed = true;
                    }
                }

                var finalFilePath = tsvFile.FullName;

                // Replace the source file
                tsvFile.Delete();

                temporaryFile.MoveTo(finalFilePath);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in UpdateCombinedTsvFileHeaders for " + tsvFile.FullName, ex);
                return false;
            }
        }

        /// <summary>
        /// Replace the FASTA file path in the data line with mFastaFilePath
        /// </summary>
        /// <param name="dataLine">Data line</param>
        /// <param name="match">Regex match</param>
        /// <param name="matchPattern">Match pattern</param>
        /// <returns>Updated line if a successful RegEx match, otherwise logs a warning and returns the original line</returns>
        private string UpdateFASTAFilePath(string dataLine, Match match, string matchPattern)
        {
            if (match.Success)
            {
                var fastaFilePath = match.Groups["FilePath"].Value;
                return dataLine.Replace(fastaFilePath, mFastaFilePath);
            }

            LogWarning("RegEx search for {0} failed in line: {1}", matchPattern, dataLine);
            return dataLine;
        }

        /// <summary>
        /// Update FASTA file paths in the .pepXML files
        /// </summary>
        /// <remarks>
        /// This is required because ProteinProphet needs the FASTA file data, and it uses the information in the .pepXML file to find the FASTA file
        /// </remarks>
        private bool UpdateFASTAPathInPepXMLFiles()
        {
            try
            {
                if (string.IsNullOrWhiteSpace(mFastaFilePath))
                {
                    LogError("Cannot update FASTA file paths in .pepXML files because variable mFastaFilePath is empty in AnalysisToolRunnerPepProtProphet");
                    return false;
                }

                // This extracts the FASTA file path from lines of the form
                // <search_database local_path="D:\DMS_WorkDir1\ID_008098_4E97840F.fasta" type="AA"/>
                var localPathMatcher = new Regex("local_path *= *\"(?<FilePath>[^\"]+)\"", RegexOptions.Compiled);

                var databaseNameMatcher = new Regex("value *= *\"(?<FilePath>[^\"]+)\"", RegexOptions.Compiled);

                foreach (var pepXmlFile in mWorkingDirectory.GetFileSystemInfos(string.Format("*{0}", PEPXML_EXTENSION)))
                {
                    var finalFilePath = pepXmlFile.FullName;
                    var tempFile = new FileInfo(pepXmlFile.FullName + ".tmp");

                    LogDebug("Updating FASTA file paths in file " + pepXmlFile.Name);

                    // Once this reaches 2, simply write out the remaining data lines
                    var matchesFound = 0;

                    using (var reader = new StreamReader(new FileStream(pepXmlFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                    using (var writer = new StreamWriter(new FileStream(tempFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        while (!reader.EndOfStream)
                        {
                            var dataLine = reader.ReadLine();

                            if (string.IsNullOrWhiteSpace(dataLine))
                            {
                                writer.WriteLine();
                                continue;
                            }

                            if (matchesFound >= 2)
                            {
                                writer.WriteLine(dataLine);
                                continue;
                            }

                            string lineToWrite;

                            if (dataLine.Contains("search_database") && dataLine.Contains("local_path"))
                            {
                                var match = localPathMatcher.Match(dataLine);
                                lineToWrite = UpdateFASTAFilePath(dataLine, match, "local_path=\"FilePath.fasta\"");
                                matchesFound++;
                            }
                            else if (dataLine.Contains("database_name") && dataLine.Contains("value"))
                            {
                                var match = databaseNameMatcher.Match(dataLine);
                                lineToWrite = UpdateFASTAFilePath(dataLine, match, "value=\"FilePath.fasta\"");
                                matchesFound++;
                            }
                            else
                            {
                                lineToWrite = dataLine;
                            }

                            writer.WriteLine(lineToWrite);
                        }
                    }

                    // Replace the original file
                    pepXmlFile.Delete();
                    tempFile.MoveTo(finalFilePath);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in UpdateFASTAPathInPepXMLFiles", ex);
                return false;
            }
        }

        /// <summary>
        /// Update the msms_run_summary element in pepXML files created by PeptideProphet to adjust the path to the parent .mzML files
        /// </summary>
        /// <remarks>
        /// This method is called when PeptideProphet was run against a group of datasets, creating an interact.pep.xml file for each experiment group
        /// (<seealso cref="UpdateMsMsRunSummaryInPepXmlFiles"/>)
        /// </remarks>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="datasetIDsByExperimentGroup">Keys are experiment group name, values are lists of dataset IDs</param>
        /// <param name="options">Processing options</param>
        /// <param name="peptideProphetPepXmlFiles">Output: list of the .pepXML files created by PeptideProphet</param>
        /// <returns>True if successful, false if error</returns>
        private bool UpdateMsMsRunSummaryInCombinedPepXmlFiles(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            FragPipeOptions options,
            out List<FileInfo> peptideProphetPepXmlFiles)
        {
            peptideProphetPepXmlFiles = new List<FileInfo>();

            try
            {
                LogDebug("Rewriting .pepXML files", 2);

                // ReSharper disable CommentTypo

                // Example command:
                // C:\DMS_Programs\Java\jre8\bin\java.exe -cp C:\DMS_Programs\MSFragger\fragpipe\lib/* com.dmtavt.fragpipe.util.RewritePepxml C:\DMS_WorkDir\CHI_IXN\interact.pep.xml C:\DMS_WorkDir\Dataset1.mzML C:\DMS_WorkDir\Dataset2.mzML

                // ReSharper enable CommentTypo

                if (!options.LibraryFinder.FindFragPipeLibDirectory(out var libDirectory))
                    return false;

                var successCount = 0;
                var arguments = new StringBuilder();

                foreach (var item in datasetIDsByExperimentGroup)
                {
                    var experimentGroupName = item.Key;
                    var experimentGroupDirectory = mExperimentGroupWorkingDirectories[experimentGroupName];

                    var pepXmlFile = new FileInfo(Path.Combine(experimentGroupDirectory.FullName, "interact.pep.xml"));

                    if (!pepXmlFile.Exists)
                    {
                        LogError("PeptideProphet results file not found: " + pepXmlFile.FullName);
                        continue;
                    }

                    peptideProphetPepXmlFiles.Add(pepXmlFile);

                    var currentStep = string.Format(@"RewritePepxml for {0}\interact.pep.xml", experimentGroupDirectory.Name);

                    arguments.Clear();

                    // ReSharper disable once StringLiteralTypo
                    arguments.AppendFormat("-cp {0}/* com.dmtavt.fragpipe.util.RewritePepxml {1}", libDirectory.FullName, pepXmlFile.FullName);

                    // Append the .mzML files

                    // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                    foreach (var datasetId in item.Value)
                    {
                        var datasetFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, dataPackageInfo.DatasetFiles[datasetId]));

                        if (!datasetFile.Extension.Equals(AnalysisResources.DOT_MZML_EXTENSION, StringComparison.OrdinalIgnoreCase))
                        {
                            LogError(string.Format("The extension for dataset file {0} is not .mzML; this is unexpected", datasetFile.Name));
                            continue;
                        }

                        arguments.AppendFormat(" {0}", datasetFile.FullName);
                    }

                    InitializeCommandRunner(
                        experimentGroupDirectory,
                        Path.Combine(mWorkingDirectory.FullName, JAVA_CONSOLE_OUTPUT),
                        CmdRunnerModes.RewritePepXml,
                        500);

                    LogCommandToExecute(experimentGroupDirectory, options.JavaProgLoc, arguments.ToString(), options.WorkingDirectoryPadWidth);

                    var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments.ToString(), "Java", true);

                    if (!mConsoleOutputFileParsed)
                    {
                        ParseConsoleOutputFile();
                    }

                    if (!string.IsNullOrEmpty(mConsoleOutputFileParser.ConsoleOutputErrorMsg))
                    {
                        LogError(mConsoleOutputFileParser.ConsoleOutputErrorMsg);
                    }

                    UpdateCombinedJavaConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, currentStep);

                    if (!processingSuccess)
                    {
                        if (mCmdRunner.ExitCode != 0)
                        {
                            LogWarning("Java returned a non-zero exit code while calling RewritePepxml on interact.pep.xml: " + mCmdRunner.ExitCode);
                        }
                        else
                        {
                            LogWarning("Call to Java failed while calling RewritePepxml on interact.pep.xml (but exit code is 0)");
                        }

                        continue;
                    }

                    successCount++;
                }

                return successCount == datasetIDsByExperimentGroup.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in UpdateMsMsRunSummaryInCombinedPepXmlFiles", ex);
                return false;
            }
        }

        /// <summary>
        /// Update the msms_run_summary element in pepXML files created by PeptideProphet to adjust the path to the parent .mzML file
        /// </summary>
        /// <remarks>
        /// <para>
        /// This method is called when PeptideProphet was run separately against each dataset (<seealso cref="UpdateMsMsRunSummaryInCombinedPepXmlFiles"/>)
        /// </para>
        /// <para>
        /// This corresponds to FragPipe step "Rewrite pepxml"
        /// </para>
        /// </remarks>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="workspaceDirectoryByDatasetId">Keys are dataset IDs, values are DirectoryInfo instances</param>
        /// <param name="interactFilesByDatasetID">Keys are dataset IDs, values are interact files</param>
        /// <param name="options">Processing options</param>
        /// <param name="peptideProphetPepXmlFiles">Output: list of the .pep.xml files created by PeptideProphet</param>
        /// <returns>True if successful, false if error</returns>
        private bool UpdateMsMsRunSummaryInPepXmlFiles(
            DataPackageInfo dataPackageInfo,
            Dictionary<int, DirectoryInfo> workspaceDirectoryByDatasetId,
            IReadOnlyDictionary<int, List<FileInfo>> interactFilesByDatasetID,
            FragPipeOptions options,
            out List<FileInfo> peptideProphetPepXmlFiles)
        {
            // This method updates the .pep.xml files created by PeptideProphet to remove the experiment group name and forward slash from the <msms_run_summary> element

            // For example, changing from:
            // <msms_run_summary base_name="C:\DMS_WorkDir\Experiment1/Dataset1" raw_data_type="mzML" comment="This pepXML was from calibrated spectra." raw_data="mzML">

            // To:
            // <msms_run_summary base_name="C:\DMS_WorkDir\Dataset1" raw_data_type="mzML" raw_data="mzML">

            peptideProphetPepXmlFiles = new List<FileInfo>();

            try
            {
                LogDebug("Rewriting .pepXML files", 2);

                // Example command line:
                // C:\DMS_Programs\Java\jre8\bin\java.exe -cp C:\DMS_Programs\MSFragger\fragpipe\lib/* com.dmtavt.fragpipe.util.RewritePepxml C:\DMS_WorkDir\interact-QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.pep.xml C:\DMS_WorkDir\QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.mzML

                if (!options.LibraryFinder.FindFragPipeLibDirectory(out var libDirectory))
                    return false;

                var successCount = 0;

                foreach (var item in workspaceDirectoryByDatasetId)
                {
                    var datasetId = item.Key;
                    var workingDirectory = item.Value;

                    if (workingDirectory.Parent == null)
                    {
                        LogError("Unable to determine the parent directory of " + workingDirectory.FullName);
                        continue;
                    }

                    if (!interactFilesByDatasetID.TryGetValue(datasetId, out var interactFiles))
                    {
                        LogError("Dataset ID {0} not found in dictionary interactFilesByDatasetID", datasetId);
                        continue;
                    }

                    var successCount2 = 0;

                    foreach (var interactFile in interactFiles)
                    {
                        peptideProphetPepXmlFiles.Add(interactFile);

                        var currentStep = string.Format(@"RewritePepxml for {0}\{1}", workingDirectory.Parent.Name, interactFile.Name);

                        var datasetFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, dataPackageInfo.DatasetFiles[datasetId]));

                        if (!datasetFile.Extension.Equals(AnalysisResources.DOT_MZML_EXTENSION, StringComparison.OrdinalIgnoreCase))
                        {
                            LogError(string.Format("The extension for dataset file {0} is not .mzML; this is unexpected", datasetFile.Name));
                            continue;
                        }

                        // ReSharper disable once StringLiteralTypo

                        var arguments = string.Format(
                            "-cp {0}/* com.dmtavt.fragpipe.util.RewritePepxml {1} {2}",
                            libDirectory.FullName, interactFile.FullName, datasetFile.FullName);

                        InitializeCommandRunner(
                            workingDirectory.Parent,
                            Path.Combine(mWorkingDirectory.FullName, JAVA_CONSOLE_OUTPUT),
                            CmdRunnerModes.RewritePepXml,
                            500);

                        LogCommandToExecute(workingDirectory.Parent, options.JavaProgLoc, arguments, options.WorkingDirectoryPadWidth);

                        var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments, "Java", true);

                        if (!mConsoleOutputFileParsed)
                        {
                            ParseConsoleOutputFile();
                        }

                        if (!string.IsNullOrEmpty(mConsoleOutputFileParser.ConsoleOutputErrorMsg))
                        {
                            LogError(mConsoleOutputFileParser.ConsoleOutputErrorMsg);
                        }

                        UpdateCombinedJavaConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, currentStep);

                        if (!processingSuccess)
                        {
                            if (mCmdRunner.ExitCode != 0)
                            {
                                LogWarning("Java returned a non-zero exit code while calling RewritePepxml on interact-Dataset.pep.xml: " +
                                           mCmdRunner.ExitCode);
                            }
                            else
                            {
                                LogWarning("Call to Java failed while calling RewritePepxml on interact-Dataset.pep.xml (but exit code is 0)");
                            }

                            continue;
                        }

                        successCount2++;
                    }

                    if (successCount2 == interactFiles.Count)
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

        /// <summary>
        /// Rename and update the report files created by Philosopher
        /// </summary>
        /// <remarks>
        /// <para>
        /// Updates files ion.tsv, peptide.tsv, protein.tsv, and psm.tsv in each experiment group working directory,
        /// updating the strings in columns Spectrum, Spectrum File, and Protein ID
        /// </para>
        /// <para>
        /// If experiment group working directories are present, will move the updated files to the main working directory
        /// </para>
        /// </remarks>
        /// <param name="usedProteinProphet">True if Protein Prophet was used</param>
        /// <returns>True if successful, false if an error</returns>
        private bool UpdatePhilosopherReportFiles(bool usedProteinProphet)
        {
            var processor = new PhilosopherResultsUpdater(mDatasetName, mWorkingDirectory);
            RegisterEvents(processor);

            var success = processor.UpdatePhilosopherReportFiles(mExperimentGroupWorkingDirectories, usedProteinProphet, out var totalPeptideCount);

            if (totalPeptideCount > 0)
            {
                return success;
            }

            var warningMessage = string.Format("No peptides were confidently identified ({0})",
                mExperimentGroupWorkingDirectories.Count > 1
                    ? "the peptide.tsv files are all empty"
                    : "the peptide.tsv file is empty");

            LogWarning(warningMessage, true);

            return success;
        }

        /// <summary>
        /// Update the .pin file to remove the spectrum ID from the first column (though it is retained for the first 10 PSMs)
        /// </summary>
        /// <remarks>
        /// <para>
        /// This is done to reduce the file size, since the dataset name can be inferred from the filename,
        /// and since the scan number and charge are already listed in other columns
        /// </para>
        /// <para>
        /// Example spectrum ID that is removed:
        /// SampleName_06May21_20-11-16.501.501.2_1
        /// </para>
        /// </remarks>
        /// <param name="sourcePinFile">Source .pin file</param>
        /// <returns>True if successful, false if an error</returns>
        private bool UpdatePinFileStripDataset(FileInfo sourcePinFile)
        {
            const string TRASH_EXTENSION = ".trash2";

            mJobParams.AddResultFileExtensionToSkip(TRASH_EXTENSION);

            try
            {
                if (!sourcePinFile.Exists)
                {
                    LogError("File not found: " + sourcePinFile.FullName);
                    return false;
                }

                var updatedPinFile = new FileInfo(sourcePinFile.FullName + ".updated");

                using (var reader = new StreamReader(new FileStream(sourcePinFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(updatedPinFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    var linesRead = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead++;

                        if (linesRead <= 11 || dataLine == null)
                        {
                            // This is the header line, the first 10 results, or an empty line
                            writer.WriteLine(dataLine);
                            continue;
                        }

                        var lineParts = dataLine.Split('\t');

                        if (lineParts.Length > 1)
                            lineParts[0] = string.Empty;

                        writer.WriteLine(string.Join("\t", lineParts));
                    }
                }

                var finalPath = sourcePinFile.FullName;
                sourcePinFile.MoveTo(sourcePinFile.FullName + TRASH_EXTENSION);

                updatedPinFile.MoveTo(finalPath);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in ValidatePINFile for " + sourcePinFile.Name, ex);
                return false;
            }
        }

        private bool UpdateReporterIonModeIfRequired(FragPipeOptions options)
        {
            try
            {
                // Keys in this dictionary are dataset IDs, values are experiment names
                var experimentNames = ExtractPackedJobParameterList(AnalysisResourcesPepProtProphet.JOB_PARAM_DICTIONARY_EXPERIMENTS_BY_DATASET_ID);

                if (experimentNames.Count == 0)
                {
                    LogWarning("Packed job parameter {0} is missing or empty; this is unexpected and likely a bug",
                        AnalysisResourcesPepProtProphet.JOB_PARAM_DICTIONARY_EXPERIMENTS_BY_DATASET_ID);

                    var experiment = mJobParams.GetJobParameter("Experiment", string.Empty);

                    experimentNames.Add(experiment);
                }

                // Keys in this dictionary are experiment names, values are quoted experiment names
                var quotedExperimentNames = new Dictionary<string, string>();

                // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                foreach (var experiment in experimentNames)
                {
                    if (quotedExperimentNames.ContainsKey(experiment))
                        continue;

                    quotedExperimentNames.Add(experiment, string.Format("'{0}'", experiment));
                }

                // Gigasax.DMS5
                var dmsConnectionString = mMgrParams.GetParam("ConnectionString");

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(dmsConnectionString, mMgrName);

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
                RegisterEvents(dbTools);

                var sqlStr = new StringBuilder();

                sqlStr.Append("SELECT labelling, COUNT(*) AS experiments ");
                sqlStr.Append("FROM V_Experiment_Export ");
                sqlStr.AppendFormat("WHERE experiment IN ({0}) ", string.Join(",", quotedExperimentNames.Values));
                sqlStr.Append("GROUP BY labelling");

                var success = dbTools.GetQueryResultsDataTable(sqlStr.ToString(), out var resultSet);

                if (!success)
                {
                    LogError("Error querying V_Experiment_Export in UpdateReporterIonModeIfRequired");
                    return false;
                }

                var experimentLabels = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                foreach (DataRow curRow in resultSet.Rows)
                {
                    var labelName = curRow[0].CastDBVal<string>();

                    if (experimentLabels.TryGetValue(labelName, out var experimentCount))
                    {
                        experimentLabels[labelName] = experimentCount + 1;
                        continue;
                    }

                    experimentLabels.Add(labelName, 1);
                }

                if (experimentLabels.ContainsKey("TMT18"))
                {
                    LogMessage("Changing the reporter ion mode from {0} to {1} based on experiment labelling info",
                        options.ReporterIonMode,
                        ReporterIonModes.Tmt18);

                    options.ReporterIonMode = ReporterIonModes.Tmt18;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in UpdateReporterIonModeIfRequired", ex);
                return false;
            }
        }

        private bool ValidateFastaFile()
        {
            // Define the path to the FASTA file
            var localOrgDbFolder = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);

            // Note that job parameter "GeneratedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
            var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

            var fastaFile = new FileInfo(fastaFilePath);

            if (fastaFile.Exists)
            {
                mFastaFilePath = fastaFilePath;
                return true;
            }

            // FASTA file not found
            LogError("FASTA file not found: " + fastaFile.Name, "FASTA file not found: " + fastaFile.FullName);
            return false;
        }

        /// <summary>
        /// Verify that each output file exists
        /// </summary>
        /// <param name="toolName">Tool name</param>
        /// <param name="outputFiles">Output files</param>
        /// <returns>True if all the files are found, false if any are missing</returns>
        private bool ValidateOutputFilesExist(string toolName, IEnumerable<FileInfo> outputFiles)
        {
            var missingFiles = (from outputFile in outputFiles where !outputFile.Exists select outputFile.Name).ToList();

            if (missingFiles.Count == 0)
                return true;

            // Example error messages:

            // Abacus results file not found: reprint.spc.tsv
            // IonQuant results file not found: combined_ion.tsv
            // IonQuant results files not found: combined_peptide.tsv, combined_protein.tsv
            // iProphet results file not found: combined.pep.xml
            // Philosopher report file not found: psm.tsv
            // Philosopher report results file not found: protein.tsv

            LogError("{0} results file{1} not found: {2}",
                toolName,
                missingFiles.Count > 1 ? "s" : string.Empty,
                string.Join(", ", missingFiles));

            return false;
        }

        /// <summary>
        /// Store the list of files in a zip file (overwriting any existing zip file),
        /// then call AddResultFileToSkip() for each file
        /// </summary>
        /// <param name="fileListDescription">File list description</param>
        /// <param name="filesToZip">Files to zip</param>
        /// <param name="zipFileName">Zip file name</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ZipFiles(string fileListDescription, IReadOnlyList<FileInfo> filesToZip, string zipFileName)
        {
            var zipFilePath = Path.Combine(mWorkingDirectory.FullName, zipFileName);

            var success = mZipTools.ZipFiles(filesToZip, zipFilePath);

            if (success)
            {
                foreach (var item in filesToZip)
                {
                    mJobParams.AddResultFileToSkip(item.Name);
                }
            }
            else
            {
                LogError("Error zipping " + fileListDescription + " to create " + zipFileName);
            }

            return success;
        }

        /// <summary>
        /// Zip each .pepXML file
        /// Also store the .pin files in the zip files
        /// </summary>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="options">Processing options</param>
        /// <returns>True if successful, false if error</returns>
        private bool ZipPepXmlAndPinFiles(DataPackageInfo dataPackageInfo, FragPipeOptions options)
        {
            try
            {
                var successCount = 0;

                foreach (var dataset in dataPackageInfo.Datasets)
                {
                    var datasetName = dataset.Value;

                    var pepXmlFiles = AnalysisToolRunnerMSFragger.FindDatasetPinFileAndPepXmlFiles(
                        mWorkingDirectory, options.FraggerOptions.DIASearchEnabled, datasetName, out var pinFile);

                    if (pepXmlFiles.Count == 0)
                    {
                        LogError(string.Format("Cannot zip the .pepXML file for dataset {0} since not found in the working directory", datasetName));
                        continue;
                    }

                    bool pinFileUpdated;

                    // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                    if (pinFile.Exists)
                    {
                        pinFileUpdated = UpdatePinFileStripDataset(pinFile);
                    }
                    else
                    {
                        // The .pin file will not exist for split FASTA searches; this is expected
                        pinFileUpdated = true;
                    }

                    var zipSuccess = AnalysisToolRunnerMSFragger.ZipPepXmlAndPinFiles(this, dataPackageInfo, datasetName, pepXmlFiles, pinFile.Exists);

                    if (!zipSuccess || !pinFileUpdated)
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
        /// Zip the _ion.tsv, _peptide.tsv, and _protein.tsv files created for each experiment group,
        /// but only if there are more than three experiment groups
        /// </summary>
        /// <param name="usedProteinProphet">True if Protein Prophet was used</param>
        /// <returns>True if successful, false if error</returns>
        private bool ZipPsmTsvFiles(bool usedProteinProphet)
        {
            try
            {
                if (mExperimentGroupWorkingDirectories.Count <= 3)
                    return true;

                var validExperimentGroupCount = 0;
                var filesToZip = new List<FileInfo>();

                foreach (var experimentGroupDirectory in mExperimentGroupWorkingDirectories.Values)
                {
                    var experimentGroup = experimentGroupDirectory.Name;

                    var ionFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, experimentGroup + "_ion.tsv"));
                    var peptideFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, experimentGroup + "_peptide.tsv"));
                    var proteinFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, experimentGroup + "_protein.tsv"));
                    var psmFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, experimentGroup + "_psm.tsv"));

                    if (ionFile.Exists)
                        filesToZip.Add(ionFile);
                    else
                        LogError("File not found: " + ionFile.Name);

                    if (peptideFile.Exists)
                        filesToZip.Add(peptideFile);
                    else
                        LogError("File not found: " + peptideFile.Name);

                    if (proteinFile.Exists)
                        filesToZip.Add(proteinFile);
                    else if (usedProteinProphet)
                        LogError("File not found: " + proteinFile.Name);

                    if (psmFile.Exists)
                        filesToZip.Add(psmFile);
                    else
                        LogError("File not found: " + psmFile.Name);

                    if (ionFile.Exists && peptideFile.Exists && psmFile.Exists && (proteinFile.Exists || !usedProteinProphet))
                    {
                        validExperimentGroupCount++;
                    }
                }

                // Zip the files to create Dataset_PSM_tsv.zip
                var zipSuccess = ZipFiles("PSM .tsv files", filesToZip, AnalysisResources.ZIPPED_MSFRAGGER_PSM_TSV_FILES);

                return zipSuccess && validExperimentGroupCount == mExperimentGroupWorkingDirectories.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in ZipPsmTsvFiles", ex);
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

            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds < SECONDS_BETWEEN_UPDATE)
                return;

            ParseConsoleOutputFile();

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress(mCmdRunnerMode.ToString());
        }

        private void ConsoleOutputFileParser_ErrorNoMessageUpdateEvent(string message)
        {
            LogErrorNoMessageUpdate(message);
        }
    }
}
