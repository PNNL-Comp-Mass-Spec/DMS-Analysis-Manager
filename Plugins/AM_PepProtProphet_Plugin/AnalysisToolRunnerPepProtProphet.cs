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
using System.IO;
using System.Linq;
using System.Text;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.FileAndDirectoryTools;
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

        // Ignore Spelling: accmass, annot, antivirus, batmass-io, bruker, ccee, clevel, cp, crystalc, decoyprobs, dir, expectscore
        // Ignore Spelling: fasta, filelist, fragpipe, freequant, glyco, groupby, itraq, java, labelquant,
        // Ignore Spelling: mapmods, masswidth, maxppmdiff, minprob, multidir
        // Ignore Spelling: nocheck, nonparam, num, peptideprophet, pepxml, plex, ppm, proteinprophet, protxml, psm, psms, --ptw, prot
        // Ignore Spelling: razorbin, specdir, tdc, tmt, tmtintegrator, --tol, unimod, Xmx
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

        private const string PTM_SHEPHERD_CONSOLE_OUTPUT = "PTMShepherd_ConsoleOutput.txt";

        /// <summary>
        /// Reserve 14 GB when running Crystal-C with Java
        /// </summary>
        public const int CRYSTALC_MEMORY_SIZE_GB = 14;

        /// <summary>
        /// Reserve 4 GB when running IonQuant with Java
        /// </summary>
        public const int ION_QUANT_MEMORY_SIZE_GB = 4;

        /// <summary>
        /// Reserve 14 GB when running TMT-Integrator
        /// </summary>
        public const int TMT_INTEGRATOR_MEMORY_SIZE_GB = 14;

        /// <summary>
        /// Extension for peptide XML files
        /// </summary>
        public const string PEPXML_EXTENSION = ".pepXML";

        /// <summary>
        /// Extension for pin files (tab-delimited text files created by MSFragger)
        /// </summary>
        public const string PIN_EXTENSION = ".pin";

        private const string TEMP_PEP_PROPHET_DIR_SUFFIX = ".pepXML-temp";

        private const string TMT_INTEGRATOR_JAR_RELATIVE_PATH = @"fragpipe\tools\tmt-integrator-3.0.0.jar";

        private const string UNDEFINED_EXPERIMENT_GROUP = "__UNDEFINED_EXPERIMENT_GROUP__";

        public const float PROGRESS_PCT_INITIALIZING = 1;

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
            Abacus = 10
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
            PeptideProphetOrPercolatorComplete = 15,
            ProteinProphetComplete = 30,
            DBAnnotationComplete = 45,
            ResultsFilterComplete = 60,
            FreeQuantOrLabelQuantComplete = 75,
            ReportGenerated = 85,
            AbacusComplete = 87,
            IonQuantComplete = 90,
            TmtIntegratorComplete = 95,
            PtmShepherdComplete = 97,
            ProcessingComplete = 99
        }

        /// <summary>
        /// Command runner modes
        /// </summary>
        public enum CmdRunnerModes
        {
            Undefined = 0,
            Philosopher = 1,
            CrystalC = 2,
            Percolator = 3,
            PercolatorOutputToPepXml = 4,
            PtmShepherd = 5,
            RewritePepXml = 6,
            IonQuant = 7
        }

        private string mFastaFilePath;

        private string mPercolatorProgLoc;
        private string mPhilosopherProgLoc;
        private string mTmtIntegratorProgLoc;

        private ConsoleOutputFileParser mConsoleOutputFileParser;

        private PhilosopherToolType mCurrentPhilosopherTool;

        private DateTime mLastConsoleOutputParse;

        private RunDosProgram mCmdRunner;

        private CmdRunnerModes mCmdRunnerMode;

        /// <summary>
        /// Constructor
        /// </summary>
        public AnalysisToolRunnerPepProtProphet()
        {
            mProgress = (int)ProgressPercentValues.Undefined;
        }

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

                // Determine the path to Percolator, Philosopher, and TMT Integrator

                mPercolatorProgLoc = DetermineProgramLocation("MSFraggerProgLoc", FragPipeLibFinder.PERCOLATOR_RELATIVE_PATH);

                mPhilosopherProgLoc = DetermineProgramLocation("MSFraggerProgLoc", FragPipeLibFinder.PHILOSOPHER_RELATIVE_PATH);

                mTmtIntegratorProgLoc = DetermineProgramLocation("MSFraggerProgLoc", TMT_INTEGRATOR_JAR_RELATIVE_PATH);

                if (string.IsNullOrWhiteSpace(mPhilosopherProgLoc) || string.IsNullOrWhiteSpace(mTmtIntegratorProgLoc))
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

        private bool DeterminePhilosopherVersion()
        {
            const PhilosopherToolType toolType = PhilosopherToolType.ShowVersion;

            var success = RunPhilosopher(toolType, "version", "get the version");
            if (!success)
                return false;

            if (string.IsNullOrWhiteSpace(mConsoleOutputFileParser.PhilosopherVersion))
            {
                mConsoleOutputFileParser.ParsePhilosopherConsoleOutputFile(Path.Combine(mWorkDir, PHILOSOPHER_CONSOLE_OUTPUT), toolType);
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
                var paramFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);
                var paramFilePath = Path.Combine(mWorkDir, paramFileName);

                var philosopherExe = new FileInfo(mPhilosopherProgLoc);

                // Determine the version of Philosopher
                var versionSuccess = DeterminePhilosopherVersion();

                if (!versionSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                var moveFilesSuccess = OrganizePepXmlAndPinFiles(
                    out var dataPackageInfo,
                    out var datasetIDsByExperimentGroup,
                    out var experimentGroupWorkingDirectories);

                if (moveFilesSuccess != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return moveFilesSuccess;
                }

                var datasetCount = datasetIDsByExperimentGroup.Sum(item => item.Value.Count);

                var optionsLoaded = LoadMSFraggerOptions(philosopherExe, datasetCount, paramFilePath, out var options);

                if (!optionsLoaded)
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
                    var crystalCSuccess = RunCrystalC(dataPackageInfo, datasetIDsByExperimentGroup, experimentGroupWorkingDirectories, options);
                    if (!crystalCSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.CrystalCComplete;
                }

                bool psmValidationSuccess;
                List<FileInfo> peptideProphetPepXmlFiles;

                if (options.MS1ValidationMode == MS1ValidationModes.PeptideProphet)
                {
                    // Run Peptide Prophet
                    psmValidationSuccess = RunPeptideProphet(
                        dataPackageInfo,
                        datasetIDsByExperimentGroup,
                        experimentGroupWorkingDirectories,
                        options,
                        out peptideProphetPepXmlFiles);
                }
                else if (options.MS1ValidationMode == MS1ValidationModes.Percolator)
                {
                    // Run Percolator
                    psmValidationSuccess = RunPercolator(
                        dataPackageInfo,
                        datasetIDsByExperimentGroup,
                        experimentGroupWorkingDirectories,
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

                if (options.OpenSearch)
                {
                    // ToDo: Possibly run PTM Prophet
                }

                bool usedProteinProphet;

                if (peptideProphetPepXmlFiles.Count > 0)
                {
                    // Run Protein Prophet
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

                var dbAnnotateSuccess = RunDatabaseAnnotation(experimentGroupWorkingDirectories);
                if (!dbAnnotateSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.DBAnnotationComplete;

                var filterSuccess = RunResultsFilter(experimentGroupWorkingDirectories, options, usedProteinProphet);
                if (!filterSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.ResultsFilterComplete;

                if (options.ReporterIonMode != ReporterIonModes.Disabled || options.RunFreeQuant && !options.RunIonQuant)
                {
                    // Always run FreeQuant when we have reporter ions
                    // If no reporter ions, either run FreeQuant or run IonQuant

                    var freeQuantSuccess = RunFreeQuant(experimentGroupWorkingDirectories);
                    if (!freeQuantSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.FreeQuantOrLabelQuantComplete;
                }

                if (options.ReporterIonMode != ReporterIonModes.Disabled)
                {
                    var labelQuantSuccess = RunLabelQuant(experimentGroupWorkingDirectories, options);
                    if (!labelQuantSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.FreeQuantOrLabelQuantComplete;
                }

                var reportSuccess = RunReportGeneration(experimentGroupWorkingDirectories);
                if (!reportSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                mProgress = (int)ProgressPercentValues.ReportGenerated;

                if (experimentGroupWorkingDirectories.Count > 1 && options.RunAbacus)
                {
                    var abacusSuccess = RunAbacus(experimentGroupWorkingDirectories, options);
                    if (!abacusSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.AbacusComplete;
                }

                if (options.RunIonQuant)
                {
                    var ionQuantSuccess = RunIonQuant(dataPackageInfo, datasetIDsByExperimentGroup, experimentGroupWorkingDirectories, options);
                    if (!ionQuantSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.IonQuantComplete;
                }

                if (options.ReporterIonMode != ReporterIonModes.Disabled)
                {
                    var tmtIntegratorSuccess = RunTmtIntegrator(experimentGroupWorkingDirectories, options);
                    if (!tmtIntegratorSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.TmtIntegratorComplete;
                }

                if (options.OpenSearch && options.RunPTMShepherd)
                {
                    var ptmShepherdSuccess = RunPTMShepherd(experimentGroupWorkingDirectories, options);
                    if (!ptmShepherdSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.PtmShepherdComplete;
                }

                var moveSuccess = MoveResultsOutOfSubdirectories(dataPackageInfo, datasetIDsByExperimentGroup, experimentGroupWorkingDirectories);
                if (!moveSuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                var zipSuccess = ZipPepXmlAndPinFiles(dataPackageInfo);

                return zipSuccess ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
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
        /// <param name="options"></param>
        /// <param name="fragPipeLibDirectory"></param>
        /// <param name="experimentGroupDirectory"></param>
        /// <param name="datasetName"></param>
        /// <returns>True if successful, false if an error</returns>
        private bool ConvertPercolatorOutputToPepXML(
            FragPipeOptions options,
            FileSystemInfo fragPipeLibDirectory,
            FileSystemInfo experimentGroupDirectory, string datasetName)
        {
            try
            {
                // ReSharper disable StringLiteralTypo
                // ReSharper disable CommentTypo

                // Example command line:
                // java -cp C:\DMS_Programs\MSFragger\fragpipe\lib/* com.dmtavt.fragpipe.tools.percolator.PercolatorOutputToPepXML DatasetName.pin DatasetName DatasetName_percolator_target_psms.tsv DatasetName_percolator_decoy_psms.tsv interact-DatasetName DDA

                var targetPsmFile = GetPercolatorFileName(datasetName, false);
                var decoyPsmFile = GetPercolatorFileName(datasetName, true);
                var pinFile = string.Format("{0}.pin", datasetName);

                var arguments = string.Format(
                    "-cp {0}/* com.dmtavt.fragpipe.tools.percolator.PercolatorOutputToPepXML " +
                    "{1} " +               // DatasetName.pin
                    "{2} " +               // DatasetName
                    "{3} " +               // DatasetName_percolator_target_psms.tsv
                    "{4} " +               // DatasetName_percolator_decoy_psms.tsv
                    "interact-{1} " +      // interact-DatasetName
                    "DDA",
                    fragPipeLibDirectory.FullName,
                    pinFile,
                    datasetName,
                    targetPsmFile,
                    decoyPsmFile);

                // ReSharper restore CommentTypo
                // ReSharper restore StringLiteralTypo

                mCmdRunner.WorkDir = experimentGroupDirectory.FullName;
                mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, JAVA_CONSOLE_OUTPUT);
                mCmdRunnerMode = CmdRunnerModes.PercolatorOutputToPepXml;

                LogDebug(options.JavaProgLoc + " " + arguments);

                // Start the program and wait for it to finish
                // However, while it's running, LoopWaiting will get called via events
                var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments, "Java", true);

                var currentStep = "PercolatorOutputToPepXML for " + datasetName;
                UpdateCombinedJavaConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, currentStep);

                if (processingSuccess)
                {
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

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in ConvertPercolatorOutputToPepXML", ex);
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
                writer.WriteLine("raw_file_location = {0}", mWorkDir);
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

        private FileInfo CreateReporterIonAliasNameFile(ReporterIonModes reporterIonMode, FileInfo aliasNameFile)
        {
            try
            {
                using var writer = new StreamWriter(new FileStream(aliasNameFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                var reporterIonNames = GetReporterIonNames(reporterIonMode);

                // Example output:
                // 126 sample-01
                // 127N sample-02
                // 127C sample-03
                // 128N sample-04

                var sampleNumber = 0;
                foreach (var reporterIon in reporterIonNames)
                {
                    sampleNumber++;
                    writer.WriteLine("{0} sample-{1:D2}", reporterIon, sampleNumber);
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

        internal static string GetCurrentPhilosopherToolDescription(PhilosopherToolType currentTool)
        {
            return currentTool switch
            {
                PhilosopherToolType.Undefined => "Undefined",
                PhilosopherToolType.ShowVersion => "Get Version",
                PhilosopherToolType.WorkspaceManager => "Workspace Manager",
                PhilosopherToolType.PeptideProphet => "Peptide Prophet",
                PhilosopherToolType.ProteinProphet => "Protein Prophet",
                PhilosopherToolType.AnnotateDatabase => "Annotate Database",
                PhilosopherToolType.ResultsFilter => "Results Filter",
                PhilosopherToolType.FreeQuant => "FreeQuant",
                PhilosopherToolType.LabelQuant => "LabelQuant",
                PhilosopherToolType.GenerateReport => "Generate Report",
                _ => throw new ArgumentOutOfRangeException()
            };
        }

        /// <summary>
        /// Get appropriate path of the working directory for the given experiment
        /// </summary>
        /// <remarks>
        /// <para>If all of the datasets belong to the same experiment, return the job's working directory</para>
        /// <para>Otherwise, return a subdirectory below the working directory, based on the experiment's name</para>
        /// </remarks>
        /// <param name="experimentGroupName"></param>
        /// <param name="experimentGroupCount"></param>
        private string GetExperimentGroupWorkingDirectory(string experimentGroupName, int experimentGroupCount)
        {
            return experimentGroupCount <= 1 ? mWorkDir : Path.Combine(mWorkDir, experimentGroupName);
        }

        /// <summary>
        /// Group the datasets in dataPackageInfo by experiment group name
        /// </summary>
        /// <remarks>Datasets that do not have an experiment group defined will be assigned to __UNDEFINED_EXPERIMENT_GROUP__</remarks>
        /// <param name="dataPackageInfo"></param>
        /// <returns>Dictionary where keys are experiment group name and values are dataset ID</returns>
        public static SortedDictionary<string, SortedSet<int>> GetDataPackageDatasetsByExperimentGroup(DataPackageInfo dataPackageInfo)
        {
            // Keys in this dictionary are Experiment Group name
            // Values are a list of dataset IDs
            var datasetIDsByExperimentGroup = new SortedDictionary<string, SortedSet<int>>();

            foreach (var item in dataPackageInfo.Datasets)
            {
                var datasetId = item.Key;

                var experimentGroup = dataPackageInfo.DatasetExperimentGroup[datasetId];

                if (string.IsNullOrWhiteSpace(experimentGroup) && dataPackageInfo.Datasets.Count == 1)
                {
                    var experimentName = dataPackageInfo.Experiments[datasetId];

                    var singleDatasetGroup = new SortedSet<int>
                    {
                        datasetId
                    };

                    datasetIDsByExperimentGroup.Add(experimentName, singleDatasetGroup);
                    continue;
                }

                var experimentGroupToUse = string.IsNullOrWhiteSpace(experimentGroup) ? UNDEFINED_EXPERIMENT_GROUP : experimentGroup;

                if (datasetIDsByExperimentGroup.TryGetValue(experimentGroupToUse, out var matchedDatasetsForGroup))
                {
                    matchedDatasetsForGroup.Add(datasetId);
                    continue;
                }

                var datasetsForGroup = new SortedSet<int>
                {
                    datasetId
                };

                datasetIDsByExperimentGroup.Add(experimentGroupToUse, datasetsForGroup);
            }

            return datasetIDsByExperimentGroup;
        }

        /// <summary>
        /// Get a file named DatasetName_percolator_target_psms.tsv or DatasetName_percolator_decoy_psms.tsv
        /// </summary>
        /// <param name="datasetName"></param>
        /// <param name="isDecoy"></param>
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

                case ReporterIonModes.Tmt10 or ReporterIonModes.Tmt11 or ReporterIonModes.Tmt16:
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

                    // TMT 11 and TMT 16
                    reporterIonNames.Add("131C");

                    if (reporterIonMode == ReporterIonModes.Tmt11)
                        return reporterIonNames;

                    // TMT 16
                    reporterIonNames.Add("131C");
                    reporterIonNames.Add("132N");
                    reporterIonNames.Add("132C");
                    reporterIonNames.Add("133N");
                    reporterIonNames.Add("133C");
                    reporterIonNames.Add("134N");

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
        /// Create the temporary directories used by Peptide Prophet
        /// </summary>
        /// <param name="dataPackageInfo"></param>
        /// <param name="datasetIDsByExperimentGroup"></param>
        /// <param name="experimentGroupWorkingDirectories">Keys are experiment group name, values are the corresponding working directory</param>
        /// <returns>Dictionary where keys are dataset names and values are DirectoryInfo instances</returns>
        private Dictionary<int, DirectoryInfo> InitializePeptideProphetWorkspaceDirectories(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories)
        {
            var workspaceDirectoryByDatasetId = new Dictionary<int, DirectoryInfo>();

            // Initialize the workspace directories for PeptideProphet (separate subdirectory for each dataset)
            foreach (var item in datasetIDsByExperimentGroup)
            {
                var experimentGroupDirectory = experimentGroupWorkingDirectories[item.Key];

                // Create a separate temp directory for each dataset
                foreach (var datasetId in item.Value)
                {
                    var datasetName = dataPackageInfo.Datasets[datasetId];

                    var directoryName = string.Format("fragpipe-{0}{1}", datasetName, TEMP_PEP_PROPHET_DIR_SUFFIX);
                    var workingDirectory = new DirectoryInfo(Path.Combine(experimentGroupDirectory.FullName, directoryName));

                    InitializePhilosopherWorkspaceWork(workingDirectory);

                    workspaceDirectoryByDatasetId.Add(datasetId, workingDirectory);
                }
            }

            return workspaceDirectoryByDatasetId;
        }

        /// <summary>
        /// Initialize the Philosopher workspace (creates a hidden directory named .meta)
        /// </summary>
        /// <remarks>Also creates a subdirectory for each experiment group if experimentGroupNames has more than one item</remarks>
        /// <param name="experimentGroupNames"></param>
        /// <param name="experimentGroupWorkingDirectories">Keys are experiment group name, values are the corresponding working directory</param>
        /// <returns>Success code</returns>
        private CloseOutType InitializePhilosopherWorkspace(
            SortedSet<string> experimentGroupNames,
            out Dictionary<string, DirectoryInfo> experimentGroupWorkingDirectories)
        {
            experimentGroupWorkingDirectories = new Dictionary<string, DirectoryInfo>();

            try
            {
                LogDebug("Initializing the Philosopher Workspace");

                mCurrentPhilosopherTool = PhilosopherToolType.WorkspaceManager;

                var workDirSuccess = InitializePhilosopherWorkspaceWork(new DirectoryInfo(mWorkDir), false);
                if (workDirSuccess != CloseOutType.CLOSEOUT_SUCCESS)
                    return workDirSuccess;

                var experimentCount = experimentGroupNames.Count;

                foreach (var experimentGroupName in experimentGroupNames)
                {
                    var workingDirectoryPath = GetExperimentGroupWorkingDirectory(experimentGroupName, experimentCount);
                    var workingDirectory = new DirectoryInfo(workingDirectoryPath);

                    experimentGroupWorkingDirectories.Add(experimentGroupName, workingDirectory);

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
                const PhilosopherToolType toolType = PhilosopherToolType.WorkspaceManager;

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
                var success = RunPhilosopher(toolType, arguments, "initialize the workspace", directory.FullName);

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
        /// <param name="philosopherExe"></param>
        /// <param name="datasetCount"></param>
        /// <param name="paramFilePath"></param>
        /// <param name="options">Output: instance of the MSFragger options class</param>
        /// <returns>True if success, false if an error</returns>
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
        /// <param name="dataPackageInfo"></param>
        /// <param name="datasetIDsByExperimentGroup">Keys are experiment group name, values are lists of dataset IDs</param>
        /// <param name="experimentGroupWorkingDirectories">Keys are experiment group name, values are the corresponding working directory</param>
        private bool MoveResultsIntoSubdirectories(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories)
        {
            return MoveResultsToFromSubdirectories(dataPackageInfo, datasetIDsByExperimentGroup, experimentGroupWorkingDirectories, true);
        }

        /// <summary>
        /// Move results out of subdirectories, but only if datasetIDsByExperimentGroup has more than one experiment group
        /// </summary>
        /// <param name="dataPackageInfo"></param>
        /// <param name="datasetIDsByExperimentGroup">Keys are experiment group name, values are lists of dataset IDs</param>
        /// <param name="experimentGroupWorkingDirectories">Keys are experiment group name, values are the corresponding working directory</param>
        private bool MoveResultsOutOfSubdirectories(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories)
        {
            return MoveResultsToFromSubdirectories(dataPackageInfo, datasetIDsByExperimentGroup, experimentGroupWorkingDirectories, false);
        }

        /// <summary>
        /// Move results into or out of subdirectories, but only if datasetIDsByExperimentGroup has more than one experiment group
        /// </summary>
        /// <remarks>
        /// If datasetIDsByExperimentGroup only has one item, no files are moved
        /// </remarks>
        /// <param name="dataPackageInfo"></param>
        /// <param name="datasetIDsByExperimentGroup">Keys are experiment group name, values are lists of dataset IDs</param>
        /// <param name="experimentGroupWorkingDirectories">Keys are experiment group name, values are the corresponding working directory</param>
        /// <param name="sourceIsWorkDirectory">
        /// When true, the source directory is the working directory
        /// When false, the working directory is the target directory
        /// </param>
        private bool MoveResultsToFromSubdirectories(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories,
            bool sourceIsWorkDirectory)
        {
            try
            {
                if (datasetIDsByExperimentGroup.Count <= 1)
                {
                    // Nothing to do
                    return true;
                }

                foreach (var item in datasetIDsByExperimentGroup)
                {
                    var experimentGroupName = item.Key;
                    var experimentWorkingDirectory = experimentGroupWorkingDirectories[experimentGroupName];

                    foreach (var datasetId in item.Value)
                    {
                        var datasetName = dataPackageInfo.Datasets[datasetId];

                        string sourceDirectoryPath;
                        string targetDirectoryPath;

                        if (sourceIsWorkDirectory)
                        {
                            sourceDirectoryPath = mWorkDir;
                            targetDirectoryPath = experimentWorkingDirectory.FullName;
                        }
                        else
                        {
                            sourceDirectoryPath = experimentWorkingDirectory.FullName;
                            targetDirectoryPath = mWorkDir;
                        }

                        var pepXmlSuccess = MoveFile(sourceDirectoryPath, datasetName + PEPXML_EXTENSION, targetDirectoryPath);
                        var pinSuccess = MoveFile(sourceDirectoryPath, datasetName + PIN_EXTENSION, targetDirectoryPath);

                        if (!pepXmlSuccess || !pinSuccess)
                            return false;
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

        /// <summary>
        /// Organize .pepXML and .pin files and populate several dictionaries
        /// </summary>
        /// <param name="dataPackageInfo"></param>
        /// <param name="datasetIDsByExperimentGroup">
        /// Keys in this dictionary are experiment group names, values are a list of Dataset IDs for each experiment group
        /// If experiment group names are not defined in the data package, this dictionary will have a single entry named __UNDEFINED_EXPERIMENT_GROUP__
        /// </param>
        /// <param name="experimentGroupWorkingDirectories">Keys are experiment group name, values are the corresponding working directory</param>
        private CloseOutType OrganizePepXmlAndPinFiles(
            out DataPackageInfo dataPackageInfo,
            out SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            out Dictionary<string, DirectoryInfo> experimentGroupWorkingDirectories)
        {
            // Keys in this dictionary are experiment group names, values are the working directory to use
            experimentGroupWorkingDirectories = new Dictionary<string, DirectoryInfo>();

            // If this job applies to a single dataset, dataPackageID will be 0
            // We still need to create an instance of DataPackageInfo to retrieve the experiment name associated with the job's dataset
            var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

            dataPackageInfo = new DataPackageInfo(dataPackageID, this);
            RegisterEvents(dataPackageInfo);

            // Keys in this dictionary are experiment group name; values are a list of dataset IDs
            // If a dataset does not have an experiment group name, it will be assigned to __UNDEFINED_EXPERIMENT_GROUP__
            datasetIDsByExperimentGroup = GetDataPackageDatasetsByExperimentGroup(dataPackageInfo);

            // Initialize the Philosopher workspace (creates a hidden directory named .meta)
            // If Experiment Groups are defined, we also create a subdirectory for each experiment group and initialize it

            var experimentGroupNames = new SortedSet<string>();
            foreach (var item in datasetIDsByExperimentGroup.Keys)
            {
                experimentGroupNames.Add(item);
            }

            var initResult = InitializePhilosopherWorkspace(experimentGroupNames, out experimentGroupWorkingDirectories);
            if (initResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return initResult;
            }

            if (datasetIDsByExperimentGroup.Count <= 1)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            // Since we have multiple experiment groups, move the pepXML and .pin files into subdirectories
            var moveSuccess = MoveResultsIntoSubdirectories(dataPackageInfo, datasetIDsByExperimentGroup, experimentGroupWorkingDirectories);

            return moveSuccess ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        private bool RunAbacus(IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories, FragPipeOptions options)
        {
            try
            {
                LogDebug("Running Abacus", 2);

                // Example command line:
                // philosopher.exe abacus --razor --picked --reprint --tag XXX_ --protein ExperimentGroupA ExperimentGroupB

                var arguments = new StringBuilder();

                // When Match Between Runs or Open Search is not in use:
                // --razor --picked --reprint --tag XXX_ --protein

                // Otherwise, remove --picked, giving:
                // --razor --reprint --tag XXX_ --protein

                arguments.Append("abacus --razor");

                if (!options.MatchBetweenRuns && !options.OpenSearch)
                {
                    arguments.Append(" --picked");
                }

                arguments.Append(" --reprint --tag XXX_");

                // Version 15 of FragPipe would append --labels if reporter ions were in use
                // This has been disabled in version 16

                // if (options.ReporterIonMode != ReporterIonModes.Disabled)
                // {
                //     arguments.Append(" --labels");
                // }

                // Append the experiment group working directory names

                arguments.Append(" --protein");

                foreach (var experimentGroupDirectory in experimentGroupWorkingDirectories.Values)
                {
                    arguments.AppendFormat(" {0}", experimentGroupDirectory.Name);
                }

                return RunPhilosopher(PhilosopherToolType.Abacus, arguments.ToString(), "run abacus");
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
            IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories,
            FragPipeOptions options)
        {
            try
            {
                LogDebug("Running Crystal-C", 2);

                // ReSharper disable CommentTypo
                // ReSharper disable IdentifierTypo

                // Run Crystal-C for this dataset; example command line:
                // java -Dbatmass.io.libs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.3\ext\thermo" -Xmx17G -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\original-crystalc-1.4.2.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.23.4.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\grppr-0.3.23.jar" crystalc.Run C:\DMS_WorkDir\ExperimentGroup\crystalc-0-DatasetName.pepXML.params C:\DMS_WorkDir\ExperimentGroup\DatasetName.pepXML

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
                    var experimentGroupDirectory = experimentGroupWorkingDirectories[item.Key];

                    foreach (var datasetId in item.Value)
                    {
                        var datasetName = dataPackageInfo.Datasets[datasetId];

                        var pepXmlFile = new FileInfo(Path.Combine(experimentGroupDirectory.FullName, string.Format("{0}.pepXML", datasetName)));

                        if (!pepXmlFile.Exists)
                        {
                            LogError("Peptide prophet results file not found: " + pepXmlFile.FullName);
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

                        mCmdRunner.WorkDir = experimentGroupDirectory.FullName;
                        mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, JAVA_CONSOLE_OUTPUT);
                        mCmdRunnerMode = CmdRunnerModes.CrystalC;

                        LogDebug(options.JavaProgLoc + " " + arguments);

                        var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments.ToString(), "Java", true);

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
        /// Create a db.bin file in the .meta subdirectory of the working directory and in any experiment directories
        /// </summary>
        /// <param name="experimentGroupWorkingDirectories">Keys are experiment group name, values are the corresponding working directory</param>
        private bool RunDatabaseAnnotation(IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories)
        {
            try
            {
                LogDebug("Annotating the FASTA file to create db.bin files", 2);

                // First process the working directory
                var workDirSuccess = RunDatabaseAnnotation(mWorkDir);
                if (!workDirSuccess)
                    return false;

                if (experimentGroupWorkingDirectories.Count <= 1)
                    return true;

                // Next process each of the experiment directories
                var successCount = 0;

                foreach (var experimentGroupDirectory in experimentGroupWorkingDirectories.Values)
                {
                    var success = RunDatabaseAnnotation(experimentGroupDirectory.FullName);

                    if (success)
                        successCount++;
                }

                return successCount == experimentGroupWorkingDirectories.Count;
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

        private bool RunFreeQuant(Dictionary<string, DirectoryInfo> experimentGroupWorkingDirectories)
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
                foreach (var experimentGroupDirectory in experimentGroupWorkingDirectories.Values)
                {
                    // ReSharper disable once StringLiteralTypo
                    var arguments = string.Format("freequant --ptw 0.4 --tol 10 --dir {0}", mWorkDir);

                    var success = RunPhilosopher(PhilosopherToolType.FreeQuant, arguments, "run FreeQuant", experimentGroupDirectory.FullName);

                    if (success)
                        successCount++;
                }

                return successCount == experimentGroupWorkingDirectories.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in RunFreeQuant", ex);
                return false;
            }
        }

        private bool RunIonQuant(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories,
            FragPipeOptions options)
        {
            try
            {
                LogDebug("Running IonQuant", 2);

                // ReSharper disable CommentTypo
                // ReSharper disable IdentifierTypo

                // Run IonQuant, example command line:
                // java -Xmx4G -Dlibs.bruker.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.3\ext\bruker" -Dlibs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.3\ext\thermo" -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\ionquant-1.7.5.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.23.4.jar" ionquant.IonQuant --threads 4 --ionmobility 0 --mbr 1 --proteinquant 2 --requantify 1 --mztol 10 --imtol 0.05 --rttol 0.4 --mbrmincorr 0 --mbrrttol 1 --mbrimtol 0.05 --mbrtoprun 100000 --ionfdr 0.01 --proteinfdr 1 --peptidefdr 1 --normalization 1 --minisotopes 2 --minscans 3 --writeindex 0 --tp 3 --minfreq 0.5 --minions 2 --minexps 1 --multidir . --filelist C:\DMS_WorkDir\Results\filelist_ionquant.txt

                // Find the Bruker lib directory, typically C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.3\ext\bruker
                if (!options.LibraryFinder.FindVendorLibDirectory("bruker", out var brukerLibDirectory))
                    return false;

                // Find the Thermo lib directory, typically C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.3\ext\thermo
                if (!options.LibraryFinder.FindVendorLibDirectory("thermo", out var thermoLibDirectory))
                    return false;

                // Find the IonQuant jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\ionquant-1.5.5.jar
                if (!options.LibraryFinder.FindJarFileIonQuant(out var jarFileIonQuant))
                    return false;

                // Find the Batmass-IO jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.23.4.jar
                if (!options.LibraryFinder.FindJarFileBatmassIO(out var jarFileBatmassIO))
                    return false;

                // ReSharper restore IdentifierTypo
                // ReSharper restore CommentTypo

                // Future: Possibly customize this
                const int ION_QUANT_THREAD_COUNT = 4;

                var matchBetweenRunsFlag = options.MatchBetweenRuns ? 1 : 0;

                // ReSharper disable StringLiteralTypo

                var arguments = new StringBuilder();

                arguments.AppendFormat(
                    "{0} -Xmx{1}G -Dlibs.bruker.dir=\"{2}\" -Dlibs.thermo.dir=\"{3}\" -cp \"{4};{5}\" ionquant.IonQuant",
                    options.JavaProgLoc,
                    ION_QUANT_MEMORY_SIZE_GB,
                    brukerLibDirectory.FullName,
                    thermoLibDirectory.FullName,
                    jarFileIonQuant.FullName,
                    jarFileBatmassIO.FullName);

                arguments.AppendFormat(" --threads {0} --ionmobility 0 --mbr {1}", ION_QUANT_THREAD_COUNT, matchBetweenRunsFlag);

                arguments.Append(" --proteinquant 2 --requantify 1 --mztol 10 --imtol 0.05 --rttol 0.4 --mbrmincorr 0 --mbrrttol 1 --mbrimtol 0.05 --mbrtoprun 100000");
                arguments.Append(" --ionfdr 0.01 --proteinfdr 1 --peptidefdr 1 --normalization 1");
                arguments.Append(" --minisotopes 2 --minscans 3 --writeindex 0 --tp 3 --minfreq 0.5 --minions 2 --minexps 1");

                if (experimentGroupWorkingDirectories.Count <= 1)
                {
                    arguments.AppendFormat(" --psm {0} --specdir {1}", "psm.tsv", mWorkDir);

                    foreach (var datasetIDs in datasetIDsByExperimentGroup.Values)
                    {
                        foreach (var datasetId in datasetIDs)
                        {
                            var datasetName = dataPackageInfo.Datasets[datasetId];

                            arguments.AppendFormat(" {0}.pepXML", datasetName);
                        }
                    }
                }
                else
                {
                    // ReSharper disable CommentTypo

                    // Option 1: append each psm.tsv file and each .pepXML file
                    //
                    // for each (var experimentGroupWorkingDirectory in experimentGroupWorkingDirectories.Values)
                    // {
                    //    arguments.AppendFormat(@" --psm {0}\psm.tsv ", experimentGroupWorkingDirectory.Name);
                    // }
                    //
                    // arguments.AppendFormat(" --multidir . --specdir {0}", mWorkDir);
                    //
                    // for each (var item in datasetIDsByExperimentGroup)
                    // {
                    //    var experimentGroupName = item.Key;
                    //    var experimentWorkingDirectory = experimentGroupWorkingDirectories[experimentGroupName];
                    //
                    //    for each (var datasetId in item.Value)
                    //    {
                    //        var datasetName = dataPackageInfo.Datasets[datasetId];
                    //
                    //        arguments.AppendFormat(@" {0}\{1}.pepXML ", experimentWorkingDirectory.Name, datasetName);
                    //    }
                    // }

                    // ReSharper restore CommentTypo

                    // Option 2:
                    // Create a text file listing the psm.tsv and .pepXML files (thus reducing the length of the command line)

                    var fileListFile = new FileInfo(Path.Combine(mWorkDir, "filelist_ionquant.txt"));

                    using (var writer = new StreamWriter(new FileStream(fileListFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        // Header line
                        writer.WriteLine("flag{0}value", '\t');

                        foreach (var experimentGroupWorkingDirectory in experimentGroupWorkingDirectories.Values)
                        {
                            writer.WriteLine(@"--psm{0}{1}\psm.tsv", '\t', experimentGroupWorkingDirectory.Name);
                        }

                        writer.WriteLine("--specdir{0}{1}", '\t', mWorkDir);

                        foreach (var item in datasetIDsByExperimentGroup)
                        {
                            var experimentGroupName = item.Key;
                            var experimentWorkingDirectory = experimentGroupWorkingDirectories[experimentGroupName];

                            foreach (var datasetId in item.Value)
                            {
                                var datasetName = dataPackageInfo.Datasets[datasetId];

                                writer.WriteLine(@"--pepxml{0}{1}\{2}.pepXML", '\t', experimentWorkingDirectory.Name, datasetName);
                            }
                        }
                    }

                    arguments.AppendFormat("--multidir . --filelist {0}", fileListFile.FullName);
                }

                // ReSharper restore StringLiteralTypo

                mCmdRunner.WorkDir = mWorkDir;
                mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, JAVA_CONSOLE_OUTPUT);
                mCmdRunnerMode = CmdRunnerModes.IonQuant;

                LogDebug(options.JavaProgLoc + " " + arguments);

                var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments.ToString(), "Java", true);

                var currentStep = "IonQuant";
                UpdateCombinedJavaConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, currentStep);

                if (processingSuccess)
                {
                    return true;
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

        /// <summary>
        /// Isobaric Quantification (LabelQuant)
        /// </summary>
        /// <remarks>
        /// Results will appear in the.tsv files created by the Report step (ion.tsv, peptide.tsv, protein.tsv, and psm.tsv),
        /// in columns corresponding to labels in the AliasNames.txt file (or experiment group specific alias name file)
        /// </remarks>
        /// <param name="experimentGroupWorkingDirectories">Keys are experiment group name, values are the corresponding working directory</param>
        /// <param name="options"></param>
        private bool RunLabelQuant(IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories, FragPipeOptions options)
        {
            try
            {
                LogDebug(string.Format("Running LabelQuant for Isobaric Quantification using {0} reporter ions", options.ReporterIonMode), 2);

                // ReSharper disable CommentTypo

                // Example command line:
                // C:\DMS_Programs\MSFragger\fragpipe\tools\philosopher\philosopher.exe labelquant --tol 20 --level 2 --plex 10 --annot C:\DMS_WorkDir\ExperimentGroupA_annotation.txt --brand tmt --dir C:\DMS_WorkDir

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
                    _ => throw new ArgumentOutOfRangeException()
                };

                var plex = GetReporterIonChannelCount(options.ReporterIonMode);

                foreach (var experimentGroup in experimentGroupWorkingDirectories)
                {
                    FileInfo aliasFile;

                    var experimentSpecificAliasFile = new FileInfo(Path.Combine(mWorkDir, string.Format("AliasNames_{0}.txt", experimentGroup.Key)));
                    var genericAliasFile = new FileInfo(Path.Combine(mWorkDir, "AliasNames.txt"));
                    var genericAliasFile2 = new FileInfo(Path.Combine(mWorkDir, "AliasName.txt"));

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
                        LogMessage(string.Format(
                            "{0} alias file not found; will auto-generate file {1} for use with LabelQuant",
                            reporterIonType, experimentSpecificAliasFile.Name));

                        aliasFile = CreateReporterIonAliasNameFile(options.ReporterIonMode, experimentSpecificAliasFile);

                        if (aliasFile == null)
                            return false;
                    }

                    // ReSharper disable StringLiteralTypo

                    var arguments = string.Format(
                        "labelquant --tol 20 --level 2 --plex {0} --annot {1} --brand {2} --dir {3}",
                        plex,
                        aliasFile.FullName,
                        reporterIonType.ToLower(),
                        mWorkDir);

                    // ReSharper restore StringLiteralTypo

                    var success = RunPhilosopher(
                        PhilosopherToolType.LabelQuant,
                        arguments,
                        "run LabelQuant",
                        experimentGroup.Value.FullName);

                    if (success)
                        successCount++;
                }

                return successCount == experimentGroupWorkingDirectories.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in RunLabelQuant", ex);
                return false;
            }
        }

        private bool RunPeptideProphet(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories,
            FragPipeOptions options,
            out List<FileInfo> peptideProphetPepXmlFiles)
        {
            try
            {
                LogDebug("Running peptide prophet", 2);

                if (options.OpenSearch)
                {
                    return RunPeptideProphetForOpenSearch(
                        dataPackageInfo,
                        datasetIDsByExperimentGroup,
                        experimentGroupWorkingDirectories,
                        options,
                        out peptideProphetPepXmlFiles);
                }

                // Keys in this dictionary are dataset names, values are DirectoryInfo instances
                var workspaceDirectoryByDatasetId = InitializePeptideProphetWorkspaceDirectories(
                    dataPackageInfo,
                    datasetIDsByExperimentGroup,
                    experimentGroupWorkingDirectories);

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
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories,
            FragPipeOptions options,
            out List<FileInfo> peptideProphetPepXmlFiles)
        {
            try
            {
                // Run Peptide Prophet separately against each experiment group

                foreach (var item in datasetIDsByExperimentGroup)
                {
                    var experimentGroupDirectory = experimentGroupWorkingDirectories[item.Key];

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

                        var pepXmlFile = new FileInfo(Path.Combine(experimentGroupDirectory.FullName, pepXmlFilename));
                        if (!pepXmlFile.Exists)
                        {
                            LogError("Crystal-C .pepXML file not found: " + pepXmlFile.Name);
                            peptideProphetPepXmlFiles = new List<FileInfo>();
                            return false;
                        }

                        arguments.AppendFormat(" {0}", pepXmlFilename);
                    }

                    var success = RunPhilosopher(PhilosopherToolType.PeptideProphet, arguments.ToString(), "run peptide prophet", experimentGroupDirectory.FullName);

                    if (!success)
                    {
                        peptideProphetPepXmlFiles = new List<FileInfo>();
                        return false;
                    }
                }

                return UpdateMsMsRunSummaryInCombinedPepXmlFiles(
                    dataPackageInfo,
                    datasetIDsByExperimentGroup,
                    experimentGroupWorkingDirectories,
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
            IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories,
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
                    var experimentGroupDirectory = experimentGroupWorkingDirectories[item.Key];

                    LogDebug("Running Percolator in " + experimentGroupDirectory.FullName);

                    foreach (var datasetId in item.Value)
                    {
                        var datasetName = dataPackageInfo.Datasets[datasetId];

                        var percolatorSuccess = RunPercolatorOnDataset(experimentGroupDirectory, datasetName, out var percolatorPsmFiles);
                        if (!percolatorSuccess)
                            continue;

                        var percolatorToPepXMLSuccess = ConvertPercolatorOutputToPepXML(options, fragPipeLibDirectory, experimentGroupDirectory, datasetName);
                        if (!percolatorToPepXMLSuccess)
                            continue;

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
                                LogWarning(string.Format("Error deleting {0}: {1}", psmFile.FullName, ex.Message));
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

        private bool RunPercolatorOnDataset(FileSystemInfo experimentGroupDirectory, string datasetName, out List<FileInfo> percolatorPsmFiles)
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
                // percolator-v3-05.exe --only-psms --no-terminate --post-processing-tdc --num-threads 4 --results-psms DatasetName_percolator_target_psms.tsv --decoy-results-psms DatasetName_percolator_decoy_psms.tsv DatasetName.pin

                var targetPsmFile = GetPercolatorFileName(datasetName, false);
                var decoyPsmFile = GetPercolatorFileName(datasetName, true);
                var pinFile = string.Format("{0}.pin", datasetName);

                var arguments = string.Format(
                    "--only-psms --no-terminate --post-processing-tdc --num-threads {0} " +
                    "--results-psms {1} " +
                    "--decoy-results-psms {2} " +
                    "{3}",
                    PERCOLATOR_THREAD_COUNT,
                    targetPsmFile,
                    decoyPsmFile,
                    pinFile);

                // ReSharper restore CommentTypo
                // ReSharper restore StringLiteralTypo

                percolatorPsmFiles.Add(new FileInfo(Path.Combine(mCmdRunner.WorkDir, targetPsmFile)));
                percolatorPsmFiles.Add(new FileInfo(Path.Combine(mCmdRunner.WorkDir, decoyPsmFile)));

                mCmdRunner.WorkDir = experimentGroupDirectory.FullName;
                mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, PERCOLATOR_CONSOLE_OUTPUT);
                mCmdRunnerMode = CmdRunnerModes.Percolator;

                LogDebug(mPercolatorProgLoc + " " + arguments);

                // Start the program and wait for it to finish
                // However, while it's running, LoopWaiting will get called via events
                var processingSuccess = mCmdRunner.RunProgram(mPercolatorProgLoc, arguments, "Percolator", true);

                if (!string.IsNullOrEmpty(mConsoleOutputFileParser.ConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputFileParser.ConsoleOutputErrorMsg);
                }

                UpdateCombinedPercolatorConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, datasetName);

                if (processingSuccess)
                {
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
                return false;
            }
        }

        /// <summary>
        /// Run philosopher using the specified arguments
        /// </summary>
        /// <param name="toolType"></param>
        /// <param name="arguments"></param>
        /// <param name="currentTask"></param>
        /// <param name="workingDirectoryPath"></param>
        /// <returns>True if successful, false if error</returns>
        private bool RunPhilosopher(PhilosopherToolType toolType, string arguments, string currentTask, string workingDirectoryPath = "")
        {
            try
            {
                mCurrentPhilosopherTool = toolType;

                mCmdRunner.WorkDir = string.IsNullOrWhiteSpace(workingDirectoryPath) ? mWorkDir : workingDirectoryPath;
                mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, PHILOSOPHER_CONSOLE_OUTPUT);
                mCmdRunnerMode = CmdRunnerModes.Philosopher;

                LogDebug(mPhilosopherProgLoc + " " + arguments);

                // Start the program and wait for it to finish
                // However, while it's running, LoopWaiting will get called via events
                var processingSuccess = mCmdRunner.RunProgram(mPhilosopherProgLoc, arguments, "Philosopher", true);

                if (!string.IsNullOrEmpty(mConsoleOutputFileParser.ConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputFileParser.ConsoleOutputErrorMsg);
                }

                var currentStep = GetCurrentPhilosopherToolDescription();
                UpdateCombinedPhilosopherConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, currentStep);

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

        /// <summary>
        /// Run protein prophet
        /// </summary>
        /// <param name="peptideProphetPepXmlFiles">List of .pep.xml files created by peptide prophet</param>
        /// <param name="options"></param>
        private bool RunProteinProphet(ICollection<FileInfo> peptideProphetPepXmlFiles, FragPipeOptions options)
        {
            try
            {
                LogDebug("Running Protein Prophet", 2);

                // ReSharper disable CommentTypo
                // ReSharper disable StringLiteralTypo

                var arguments = new StringBuilder();

                // Closed search without TMT or iTRAQ; also, open search:
                // --maxppmdiff 2000000 --output combined

                // Closed search, with TMT or iTRAQ:
                // --maxppmdiff 2000000 --minprob 0.9 --output combined

                arguments.Append("--maxppmdiff 2000000");

                if (options.ReporterIonMode != ReporterIonModes.Disabled && !options.OpenSearch)
                {
                    arguments.Append(" --minprob 0.9");
                }

                arguments.Append(" --output combined");

                // ReSharper restore StringLiteralTypo
                // ReSharper restore CommentTypo

                if (peptideProphetPepXmlFiles.Count > 1)
                {
                    // Create a text file listing the .pep.xml files, one per line (thus reducing the length of the command line)
                    var fileListFile = new FileInfo(Path.Combine(mWorkDir, "filelist_proteinprophet.txt"));

                    using (var writer = new StreamWriter(new FileStream(fileListFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        foreach (var pepXmlFile in peptideProphetPepXmlFiles)
                        {
                            writer.WriteLine(pepXmlFile.FullName);
                        }
                    }

                    arguments.AppendFormat(" {0}", fileListFile.FullName);
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

                // Note that Protein Prophet creates a subdirectory named 6c436c4-ccee-42bd-b2e7-cc9e23e14ab5 below the user's temp directory
                // Inside this directory, files batchcoverage.exe and DatabaseParser.exe are created
                // When Protein Prophet finishes, these files are deleted
                // Antivirus scanning processes sometimes lock these files, preventing their deletion, leading to errors like these:

                // ERRO[16:24:59] remove C:\Users\D3L243\AppData\Local\Temp\06c436c4-ccee-42bd-b2e7-cc9e23e14ab5\batchcoverage.exe: The process cannot access the file because it is being used by another process.
                // ERRO[16:25:43] remove C:\Users\D3L243\AppData\Local\Temp\06c436c4-ccee-42bd-b2e7-cc9e23e14ab5\DatabaseParser.exe: The process cannot access the file because it is being used by another process.

                // These errors can be safely ignored

                // ReSharper restore CommentTypo

                return RunPhilosopher(PhilosopherToolType.ProteinProphet, arguments.ToString(), "run protein prophet");
            }
            catch (Exception ex)
            {
                LogError("Error in RunProteinProphet", ex);
                return false;
            }
        }

        private bool RunPTMShepherd(IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories, FragPipeOptions options)
        {
            try
            {
                LogDebug("Running PTMShepherd", 2);

                // ReSharper disable CommentTypo
                // ReSharper disable StringLiteralTypo

                // Run PTMShepherd, example command line:
                // java -Dbatmass.io.libs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.3\ext\thermo" -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\ptmshepherd-1.0.0.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.23.4.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\commons-math3-3.6.1.jar" edu.umich.andykong.ptmshepherd.PTMShepherd "C:DMS_WorkDir\shepherd.config"

                // Find the thermo lib directory, typically C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.3\ext\thermo
                if (!options.LibraryFinder.FindVendorLibDirectory("thermo", out var thermoLibDirectory))
                    return false;

                // Find the PTM-Shepherd jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\ptmshepherd-1.0.0.jar
                if (!options.LibraryFinder.FindJarFileBatmassIO(out var jarFilePtmShepherd))
                    return false;

                // Find the Batmass-IO jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.23.4.jar

                // ReSharper disable once IdentifierTypo
                if (!options.LibraryFinder.FindJarFileBatmassIO(out var jarFileBatmassIO))
                    return false;

                // Find the Commons-Math3 jar file, typically C:\DMS_Programs\MSFragger\fragpipe\tools\commons-math3-3.6.1.jar
                if (!options.LibraryFinder.FindJarFileCommonsMath(out var jarFileCommonsMath))
                    return false;

                // Create the PTM-Shepherd config file
                var ptmShepherdConfigFile = new FileInfo(Path.Combine(mWorkDir, "shepherd.config"));

                using (var writer = new StreamWriter(new FileStream(ptmShepherdConfigFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine("database = " + mFastaFilePath);

                    foreach (var experimentGroup in experimentGroupWorkingDirectories)
                    {
                        // dataset = ExperimentGroupA C:\FragPipe_Test3\Results\ExperimentGroupA\psm.tsv WorkingDirectoryPath");

                        writer.WriteLine("dataset = {0} {1} {2}",
                            experimentGroup.Key,
                            Path.Combine(experimentGroup.Value.FullName, "psm.tsv"),
                            mWorkDir);
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
                // ReSharper restore CommentTypo

                mCmdRunner.WorkDir = mWorkDir;
                mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, PTM_SHEPHERD_CONSOLE_OUTPUT);
                mCmdRunnerMode = CmdRunnerModes.PtmShepherd;

                LogDebug(options.JavaProgLoc + " " + arguments);

                var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments, "Java", true);

                if (processingSuccess)
                {
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

        private bool RunReportGeneration(IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories)
        {
            try
            {
                LogDebug("Generating MSFragger report files", 2);

                // Generate a separate report for each experiment group
                var successCount = 0;

                // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
                foreach (var experimentGroupDirectory in experimentGroupWorkingDirectories.Values)
                {
                    // ReSharper disable once ConvertToConstant.Local
                    var arguments = "philosopher.exe report";

                    var success = RunPhilosopher(PhilosopherToolType.GenerateReport, arguments, "generate report files", experimentGroupDirectory.FullName);

                    if (success)
                        successCount++;
                }

                return successCount == experimentGroupWorkingDirectories.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in RunReportGeneration", ex);
                return false;
            }
        }

        private bool RunResultsFilter(
            IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories,
            FragPipeOptions options,
            bool usedProteinProphet)
        {
            try
            {
                LogDebug("Filtering MSFragger Results", 2);

                var successCount = 0;

                var arguments = new StringBuilder();

                // Note that each time we call philosopher.exe with the filter command, we use the same, shared razor.bin file
                var razorBinFilePath = Path.Combine(experimentGroupWorkingDirectories.Values.First().FullName, @".meta\razor.bin");

                foreach (var experimentGroupDirectory in experimentGroupWorkingDirectories.Values)
                {
                    arguments.Clear();

                    // ReSharper disable CommentTypo

                    // Closed search, without match between runs:
                    // --sequential --razor --picked --prot 0.01 --tag XXX_

                    // Closed search, with match between runs enabled:
                    // --sequential --razor --prot 0.01 --tag XXX_

                    // Open search:
                    // --sequential --razor --prot 0.01 --mapmods --tag XXX_

                    // ReSharper restore CommentTypo

                    arguments.Append("filter --sequential --razor");

                    if (!options.MatchBetweenRuns && !options.OpenSearch)
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

                    arguments.AppendFormat(" --pepxml {0}", experimentGroupDirectory.FullName);

                    if (usedProteinProphet)
                    {
                        // ReSharper disable StringLiteralTypo

                        arguments.AppendFormat(" --protxml {0}", Path.Combine(experimentGroupDirectory.FullName, "combined.prot.xml"));

                        // Each invocation of filter uses the same razor.bin file
                        arguments.AppendFormat(" --razorbin {0}", razorBinFilePath);

                        // ReSharper restore StringLiteralTypo
                    }

                    var success = RunPhilosopher(PhilosopherToolType.ResultsFilter, arguments.ToString(), "filter results", experimentGroupDirectory.FullName);

                    if (success)
                        successCount++;
                }

                return successCount == experimentGroupWorkingDirectories.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in RunResultsFilter", ex);
                return false;
            }
        }

        private bool RunTmtIntegrator(IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories, FragPipeOptions options)
        {
            try
            {
                LogDebug("Running TMT-Integrator", 2);

                // ReSharper disable CommentTypo

                // Example command line:
                // java -Xmx14G -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\tmt-integrator-3.0.0.jar" TMTIntegrator C:\DMS_WorkDir\Results\tmt-integrator-conf.yml C:\DMS_WorkDir\Results\ExperimentGroupA\psm.tsv C:\DMS_WorkDir\Results\ExperimentGroupB\psm.tsv

                // ReSharper restore CommentTypo

                var plex = GetReporterIonChannelCount(options.ReporterIonMode);

                var arguments = new StringBuilder();
                arguments.AppendFormat("-Xmx{0}G -cp \"{1}\" TMTIntegrator", TMT_INTEGRATOR_MEMORY_SIZE_GB, mTmtIntegratorProgLoc);

                // Create the TMT-Integrator config file
                var tmtIntegratorConfigFile = new FileInfo(Path.Combine(mWorkDir, "tmt-integrator-conf.yml"));

                using (var writer = new StreamWriter(new FileStream(tmtIntegratorConfigFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // ReSharper disable StringLiteralTypo

                    writer.WriteLine("tmtintegrator:");
                    writer.WriteLine("  min_site_prob: -1");
                    writer.WriteLine("  mod_tag: none");
                    writer.WriteLine("  min_purity: 0.5");
                    writer.WriteLine("  memory: 16");
                    writer.WriteLine("  ref_tag: Bridge");
                    writer.WriteLine("  max_pep_prob_thres: 0.9");
                    writer.WriteLine("  unique_gene: 0");
                    writer.WriteLine("  min_pep_prob: 0.9");
                    writer.WriteLine("  psm_norm: false");
                    writer.WriteLine("  outlier_removal: true");
                    writer.WriteLine("  output: {0}", Path.Combine(mWorkDir, "tmt-report"));
                    writer.WriteLine("  path: {0}", mTmtIntegratorProgLoc);
                    writer.WriteLine("  channel_num: {0}", plex);
                    writer.WriteLine("  ms1_int: true");
                    writer.WriteLine("  add_Ref: 1");
                    writer.WriteLine("  min_percent: 0.05");
                    writer.WriteLine("  protein_database: {0}", mFastaFilePath);
                    writer.WriteLine("  groupby: 0");
                    writer.WriteLine("  top3_pep: true");
                    writer.WriteLine("  min_ntt: 0");
                    writer.WriteLine("  aggregation_method: 0");
                    writer.WriteLine("  allow_overlabel: true");
                    writer.WriteLine("  allow_unlabeled: false");
                    writer.WriteLine("  print_RefInt: false");
                    writer.WriteLine("  prot_exclude: none");
                    writer.WriteLine("  unique_pep: false");
                    writer.WriteLine("  best_psm: true");
                    writer.WriteLine("  prot_norm: 1");

                    // ReSharper restore StringLiteralTypo
                }

                foreach (var experimentGroup in experimentGroupWorkingDirectories)
                {
                    mCmdRunner.WorkDir = experimentGroup.Value.FullName;
                    mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, JAVA_CONSOLE_OUTPUT);
                    mCmdRunnerMode = CmdRunnerModes.PercolatorOutputToPepXml;

                    arguments.AppendFormat(" {0}", Path.Combine(experimentGroup.Value.FullName, "psm.tsv"));
                }

                LogDebug(options.JavaProgLoc + " " + arguments);

                var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments.ToString(), "Java", true);

                var currentStep = "TMT-Integrator";
                UpdateCombinedJavaConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, currentStep);

                if (processingSuccess)
                {
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

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error in RunTmtIntegrator", ex);
                return false;
            }
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
                return SetStepTaskToolVersion(mConsoleOutputFileParser.PhilosopherVersion, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private void UpdateCombinedConsoleOutputFile(string consoleOutputFilepath, string combinedFileName, string currentStep)
        {
            try
            {
                var consoleOutputFile = new FileInfo(consoleOutputFilepath);
                if (!consoleOutputFile.Exists)
                {
                    LogWarning("UpdateCombinedPhilosopherConsoleOutput: ConsoleOutput file not found: " + consoleOutputFilepath);
                    return;
                }

                var combinedFilePath = Path.Combine(mWorkDir, combinedFileName);

                using var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
                using var writer = new StreamWriter(new FileStream(combinedFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite));

                if (currentTool != PhilosopherToolType.ShowVersion)
                {
                    writer.WriteLine();
                    writer.WriteLine();
                    writer.WriteLine("### {0} ### ", currentStep);
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

        private void UpdateCombinedPhilosopherConsoleOutputFile(string consoleOutputFilepath, string currentStep)
        {
            UpdateCombinedConsoleOutputFile(consoleOutputFilepath, PHILOSOPHER_CONSOLE_OUTPUT_COMBINED, currentStep);
        }

        /// <summary>
        /// Update the msms_run_summary element in pepXML files created by Peptide Prophet to adjust the path to the parent .mzML files
        /// </summary>
        /// <remarks>
        /// This method is called when peptide prophet was run against a group of datasets, creating an interact.pep.xml file for each experiment group
        /// (<seealso cref="UpdateMsMsRunSummaryInPepXmlFiles"/>)
        /// </remarks>
        /// <param name="dataPackageInfo"></param>
        /// <param name="datasetIDsByExperimentGroup"></param>
        /// <param name="experimentGroupWorkingDirectories">Keys are experiment group name, values are the corresponding working directory</param>
        /// <param name="options"></param>
        /// <param name="peptideProphetPepXmlFiles">Output: list of the .pepXML files created by peptide prophet</param>
        /// <returns>True if success, false if an error</returns>
        private bool UpdateMsMsRunSummaryInCombinedPepXmlFiles(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories,
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
                    var experimentGroupDirectory = experimentGroupWorkingDirectories[item.Key];

                    var pepXmlFile = new FileInfo(Path.Combine(experimentGroupDirectory.FullName, "interact.pep.xml"));

                    if (!pepXmlFile.Exists)
                    {
                        LogError("Peptide prophet results file not found: " + pepXmlFile.FullName);
                        continue;
                    }

                    peptideProphetPepXmlFiles.Add(pepXmlFile);

                    var currentStep = string.Format(@"RewritePepxml for {0}\interact.pep.xml", experimentGroupDirectory.Name);

                    arguments.Clear();

                    // ReSharper disable once StringLiteralTypo
                    arguments.AppendFormat("-cp {0}/* com.dmtavt.fragpipe.util.RewritePepxml {1}", libDirectory.FullName, pepXmlFile.FullName);

                    // Append the .mzML files
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

                    mCmdRunner.WorkDir = experimentGroupDirectory.FullName;
                    mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, JAVA_CONSOLE_OUTPUT);
                    mCmdRunnerMode = CmdRunnerModes.RewritePepXml;

                    LogDebug(options.JavaProgLoc + " " + arguments);

                    var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments.ToString(), "Java", true);

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
        /// Update the msms_run_summary element in pepXML files created by Peptide Prophet to adjust the path to the parent .mzML file
        /// </summary>
        /// <remarks>
        /// <para>
        /// This method is called when Peptide Prophet was run separately against each dataset (<seealso cref="UpdateMsMsRunSummaryInCombinedPepXmlFiles"/>)
        /// </para>
        /// <para>
        /// This corresponds to FragPipe step "Rewrite pepxml"
        /// </para>
        /// </remarks>
        /// <param name="dataPackageInfo"></param>
        /// <param name="workspaceDirectoryByDatasetId"></param>
        /// <param name="options"></param>
        /// <param name="peptideProphetPepXmlFiles">Output: list of the .pepXML files created by peptide prophet</param>
        /// <returns>True if success, false if an error</returns>
        private bool UpdateMsMsRunSummaryInPepXmlFiles(
            DataPackageInfo dataPackageInfo,
            Dictionary<int, DirectoryInfo> workspaceDirectoryByDatasetId,
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

                    var currentStep = string.Format(@"RewritePepxml for {0}\{1}", workingDirectory.Parent.Name, pepXmlFile.Name);

                    var datasetFile = new FileInfo(Path.Combine(mWorkDir, dataPackageInfo.DatasetFiles[datasetId]));
                    if (!datasetFile.Extension.Equals(AnalysisResources.DOT_MZML_EXTENSION, StringComparison.OrdinalIgnoreCase))
                    {
                        LogError(string.Format("The extension for dataset file {0} is not .mzML; this is unexpected", datasetFile.Name));
                        continue;
                    }

                    // ReSharper disable once StringLiteralTypo

                    var arguments = string.Format(
                        "-cp {0}/* com.dmtavt.fragpipe.util.RewritePepxml {1} {2}",
                        libDirectory.FullName, pepXmlFile.FullName, datasetFile.FullName);

                    mCmdRunner.WorkDir = workingDirectory.FullName;
                    mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, JAVA_CONSOLE_OUTPUT);
                    mCmdRunnerMode = CmdRunnerModes.RewritePepXml;

                    LogDebug(options.JavaProgLoc + " " + arguments);

                    var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments, "Java", true);

                    UpdateCombinedJavaConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, currentStep);

                    if (!processingSuccess)
                    {
                        if (mCmdRunner.ExitCode != 0)
                        {
                            LogWarning("Java returned a non-zero exit code while calling RewritePepxml on interact-Dataset.pep.xml: " + mCmdRunner.ExitCode);
                        }
                        else
                        {
                            LogWarning("Call to Java failed while calling RewritePepxml on interact-Dataset.pep.xml (but exit code is 0)");
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
        private bool ValidateFastaFile()
        {
            // Define the path to the FASTA file
            var localOrgDbFolder = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);

            // Note that job parameter "generatedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
            var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam("PeptideSearch", AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

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
        /// Zip each .pepXML file
        /// Also store the .pin files in the zip files
        /// </summary>
        /// <param name="dataPackageInfo"></param>
        /// <returns>True if success, false if an error</returns>
        private bool ZipPepXmlAndPinFiles(DataPackageInfo dataPackageInfo)
        {
            try
            {
                var successCount = 0;

                foreach (var dataset in dataPackageInfo.Datasets)
                {
                    var datasetName = dataset.Value;

                    var pepXmlFile = new FileInfo(Path.Combine(mWorkDir, datasetName + PEPXML_EXTENSION));
                    var pinFile = new FileInfo(Path.Combine(mWorkDir, datasetName + PIN_EXTENSION));

                    var pinFileUpdated = UpdatePinFileStripDataset(pinFile);

                    var zipSuccess = AnalysisToolRunnerMSFragger.ZipPepXmlAndPinFiles(this, datasetName, pepXmlFile);
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
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            if (!(DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE))
                return;

            mLastConsoleOutputParse = DateTime.UtcNow;

            switch (mCmdRunnerMode)
            {
                case CmdRunnerModes.CrystalC:
                case CmdRunnerModes.IonQuant:
                case CmdRunnerModes.PercolatorOutputToPepXml:
                case CmdRunnerModes.PtmShepherd:
                case CmdRunnerModes.RewritePepXml:
                    mConsoleOutputFileParser.ParseJavaConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, mCmdRunnerMode);
                    break;

                case CmdRunnerModes.Percolator:
                    mConsoleOutputFileParser.ParsePercolatorConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);
                    break;

                case CmdRunnerModes.Philosopher:
                    mConsoleOutputFileParser.ParsePhilosopherConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath, mCurrentPhilosopherTool);
                    break;

                case CmdRunnerModes.Undefined:
                    break;

                default:
                    throw new ArgumentOutOfRangeException();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress(mCmdRunnerMode.ToString());
        }

        private void ConsoleOutputFileParser_ErrorNoMessageUpdateEvent(string message)
        {
            LogErrorNoMessageUpdate(message);
        }
    }
}
