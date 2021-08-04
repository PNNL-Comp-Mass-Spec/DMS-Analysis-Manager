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

        // Ignore Spelling: accmass, annot, antivirus, batmass-io, bruker, ccee, clevel, cp, crystalc, decoyprobs, degen, dev, dir, expectscore
        // Ignore Spelling: Flammagenitus, fragpipe, freequant, Insilicos, itraq, java, labelquant, mapmods, masswidth, maxppmdiff, minprob, multidir
        // Ignore Spelling: nocheck, nonparam, num, peptideprophet, pepxml, plex, ppm, protxml, psm, psms, --ptw, prot
        // Ignore Spelling: razorbin, specdir, tdc, tmt, --tol, Xmx
        // Ignore Spelling: \batmass, \bruker, \fragpipe, \grppr, \ionquant, \ptmshepherd, \thermo, \tools

        // IonQuant command line arguments
        // Ignore Spelling: ionmobility, mbr, proteinquant, requantify, mztol, imtol, rttol, mbrmincorr, mbrrttol, mbrimtol, mbrtoprun
        // Ignore Spelling: ionfdr, proteinfdr, peptidefdr, minisotopes, minscans, writeindex, --tp, minfreq, minexps

        // ReSharper restore CommentTypo

        private const string JAVA_CONSOLE_OUTPUT = "Java_ConsoleOutput.txt";

        private const string PHILOSOPHER_CONSOLE_OUTPUT = "Philosopher_ConsoleOutput.txt";
        private const string PHILOSOPHER_CONSOLE_OUTPUT_COMBINED = "Philosopher_ConsoleOutput_Combined.txt";

        private const string PERCOLATOR_CONSOLE_OUTPUT = "Percolator_ConsoleOutput.txt";
        private const string PERCOLATOR_CONSOLE_OUTPUT_COMBINED = "Percolator_ConsoleOutput_Combined.txt";

        // ReSharper disable IdentifierTypo

        private const string ABACUS_PROPHET_CONSOLE_OUTPUT = "Abacus_ConsoleOutput.txt";
        private const string FREEQUANT_PROPHET_CONSOLE_OUTPUT = "FreeQuant_ConsoleOutput.txt";
        private const string LABELQUANT_PROPHET_CONSOLE_OUTPUT = "LabelQuant_ConsoleOutput.txt";
        private const string PEPTIDE_PROPHET_CONSOLE_OUTPUT = "PeptideProphet_ConsoleOutput.txt";
        private const string PROTEIN_PROPHET_CONSOLE_OUTPUT = "ProteinProphet_ConsoleOutput.txt";

        /// <summary>
        /// Reserve 15 GB when running Crystal-C with Java
        /// </summary>
        public const int CRYSTALC_MEMORY_SIZE_GB = 15;

        /// <summary>
        /// Reserve 15 GB when running IonQuant with Java
        /// </summary>
        public const int ION_QUANT_MEMORY_SIZE_GB = 15;

        // ReSharper restore IdentifierTypo

        private const string PEPXML_EXTENSION = ".pepXML";

        private const string TEMP_PEP_PROPHET_DIR_SUFFIX = ".pepXML-temp";

        private const string TMT_INTEGRATOR_JAR_RELATIVE_PATH = @"fragpipe\tools\tmt-integrator-3.0.0.jar";

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
            GenerateReport = 8,
            Abacus = 9
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

        private string mPercolatorProgLoc;
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

                // Determine the path to Percolator, Philosopher, and TMT Integrator

                mPercolatorProgLoc = DetermineProgramLocation("MSFraggerProgLoc", FragPipeLibFinder.PERCOLATOR_RELATIVE_PATH);

                mPhilosopherProgLoc = DetermineProgramLocation("MSFraggerProgLoc", FragPipeLibFinder.PHILOSOPHER_RELATIVE_PATH);

                mTmtIntegratorProgLoc = DetermineProgramLocation("MSFraggerProgLoc", TMT_INTEGRATOR_JAR_RELATIVE_PATH);

                if (string.IsNullOrWhiteSpace(mPhilosopherProgLoc) || string.IsNullOrWhiteSpace(mTmtIntegratorProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the Philosopher version info in the database after the first line is written to file Philosopher_ConsoleOutput.txt
                mPhilosopherVersion = string.Empty;

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

                var philosopherExe = new FileInfo(mPhilosopherProgLoc);

                var moveFilesSuccess = OrganizePepXmlFiles(
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

                mProgress = (int)ProgressPercentValues.PeptideProphetComplete;

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
                }

                if (options.ReporterIonMode != ReporterIonModes.Disabled)
                {
                    var labelQuantSuccess = RunLabelQuant(experimentGroupWorkingDirectories, options);
                    if (!labelQuantSuccess)
                        return CloseOutType.CLOSEOUT_FAILED;

                    mProgress = (int)ProgressPercentValues.LabelQuantComplete;
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
                    var tmtIntegratorSuccess = RunTmtIntegrator(options.ReporterIonMode);
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

                var zipSuccess = ZipPepXmlFiles(dataPackageInfo);

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
            MSFraggerOptions options,
            FileSystemInfo fragPipeLibDirectory,
            FileSystemInfo experimentGroupDirectory, string datasetName)
        {
            mCmdRunner.WorkDir = experimentGroupDirectory.FullName;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, JAVA_CONSOLE_OUTPUT);

            // ReSharper disable StringLiteralTypo

            var arguments = string.Format(
                "-cp {0}/* com.dmtavt.fragpipe.tools.percolator.PercolatorOutputToPepXML " +
                "{1}.pin " +
                "{1} " +
                "{1}_percolator_target_psms.tsv " +
                "{1}_percolator_decoy_psms.tsv " +
                "interact-{0} " +
                "DDA",
                fragPipeLibDirectory.FullName,
                datasetName);

            // ReSharper restore StringLiteralTypo

            LogDebug(options.JavaProgLoc + " " + arguments);

            var processingSuccess = mCmdRunner.RunProgram(mPercolatorProgLoc, arguments, "Java", true);

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

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileToSkip(Dataset + AnalysisResources.DOT_MZML_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory();
        }

        private bool CreateCrystalcParamFile(DirectoryInfo experimentGroupDirectory, string datasetName, out FileInfo fileInfo)
        {
            throw new NotImplementedException();
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
        /// <param name="experimentGroupName"></param>
        /// <param name="experimentGroupCount"></param>
        /// <remarks>
        /// <para>If all of the datasets belong to the same experiment, return the job's working directory</para>
        /// <para>Otherwise, return a subdirectory below the working directory, based on the experiment's name</para>
        /// </remarks>
        private string GetExperimentGroupWorkingDirectory(string experimentGroupName, int experimentGroupCount)
        {
            return experimentGroupCount <= 1 ? mWorkDir : Path.Combine(mWorkDir, experimentGroupName);
        }

        /// <summary>
        /// Group the datasets in dataPackageInfo by experiment group name
        /// </summary>
        /// <param name="dataPackageInfo"></param>
        /// <remarks>Datasets that do not have an experiment group defined will be assigned to __UNDEFINED_EXPERIMENT_GROUP__</remarks>
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
        /// Create the temporary directories used by Peptide Prophet
        /// </summary>
        /// <param name="dataPackageInfo"></param>
        /// <param name="datasetIDsByExperimentGroup"></param>
        /// <param name="experimentGroupWorkingDirectories"></param>
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
        /// <param name="experimentGroupNames"></param>
        /// <param name="experimentGroupWorkingDirectories"></param>
        /// <remarks>Also creates a subdirectory for each experiment if experimentGroupNames has more than one item</remarks>
        /// <returns>Success code</returns>
        private CloseOutType InitializePhilosopherWorkspace(
            IReadOnlyCollection<string> experimentGroupNames,
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
        /// <param name="philosopherExe"></param>
        /// <param name="datasetCount"></param>
        /// <param name="paramFilePath"></param>
        /// <param name="options">Output: instance of the MSFragger options class</param>
        /// <remarks>Also looks for job parameters that can be used to enable/disable processing options</remarks>
        /// <returns>True if success, false if an error</returns>
        private bool LoadMSFraggerOptions(FileInfo philosopherExe, int datasetCount, string paramFilePath, out MSFraggerOptions options)
        {
            options = new MSFraggerOptions(mJobParams, philosopherExe, datasetCount);
            RegisterEvents(options);

            try
            {
                // javaProgLoc will typically be "C:\DMS_Programs\Java\jre8\bin\java.exe"
                options.JavaProgLoc = GetJavaProgLoc();

                options.LoadMSFraggerOptions(paramFilePath);
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in LoadMSFraggerOptions", ex);
                return false;
            }
        }

        /// <summary>
        /// Move results into subdirectories, but only if datasetIDsByExperimentGroup has more than one experiment group
        /// </summary>
        /// <param name="dataPackageInfo"></param>
        /// <param name="datasetIDsByExperimentGroup">Keys are experiment group name, values are lists of dataset IDs</param>
        /// <param name="experimentGroupWorkingDirectories"></param>
        private bool MoveResultsIntoSubdirectories(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories)
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

                        var sourceFile = new FileInfo(Path.Combine(mWorkDir, datasetName + PEPXML_EXTENSION));

                        var targetPath = Path.Combine(experimentWorkingDirectory.FullName, sourceFile.Name);

                        sourceFile.MoveTo(targetPath);
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
        /// Organize .pepXML files and populate several dictionaries
        /// </summary>
        /// <param name="dataPackageInfo"></param>
        /// <param name="datasetIDsByExperimentGroup">
        /// Keys in this dictionary are experiment group names, values are a list of Dataset IDs for each experiment
        /// If experiment group names are not defined in the data package, this dictionary will have a single entry named __UNDEFINED_EXPERIMENT_GROUP__
        /// </param>
        /// <param name="experimentGroupWorkingDirectories"></param>
        private CloseOutType OrganizePepXmlFiles(
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
            // If Experiment Groups are defined, we also create a subdirectory for each experiment and initialize it

            var experimentGroupNames = datasetIDsByExperimentGroup.Keys.ToList();

            var initResult = InitializePhilosopherWorkspace(experimentGroupNames, out experimentGroupWorkingDirectories);
            if (initResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return initResult;
            }

            if (datasetIDsByExperimentGroup.Count <= 1)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            // Since we have multiple experiment groups, move the pepXML files into subdirectories
            var moveSuccess = MoveResultsIntoSubdirectories(dataPackageInfo, datasetIDsByExperimentGroup, experimentGroupWorkingDirectories);

            return moveSuccess ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
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

        private bool RunAbacus(IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories, MSFraggerOptions options)
        {
            try
            {
                LogDebug("Running Abacus", 2);

                mCmdRunner.WorkDir = mWorkDir;
                mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, "Abacus_ConsoleOutput.txt");

                var arguments = new StringBuilder();


                // When Match Between Runs or Open Search is not in use:
                // --razor --picked --reprint --tag XXX_ --protein

                // Otherwise, exclude --picked, giving:
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

                return RunPhilosopher(PhilosopherToolType.Abacus, arguments.ToString(), "running abacus");
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
            MSFraggerOptions options)
        {
            try
            {
                LogDebug("Running Crystal-C", 2);

                // ReSharper disable CommentTypo
                // ReSharper disable IdentifierTypo

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

                // ReSharper restore IdentifierTypo
                // ReSharper restore CommentTypo

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

                        // ReSharper disable once IdentifierTypo
                        if (!CreateCrystalcParamFile(experimentGroupDirectory, datasetName, out var crystalcParamFile))
                            return false;

                        // ReSharper disable CommentTypo

                        // Run Crystal-C for this dataset; example command line:
                        // java -Dbatmass.io.libs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.2\ext\thermo" -Xmx17G -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\original-crystalc-1.3.2.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.23.4.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\grppr-0.3.23.jar" crystalc.Run C:\DMS_WorkDir\ExperimentGroup\crystalc-0-DatasetName.pepXML.params C:\DMS_WorkDir\ExperimentGroup\DatasetName.pepXML

                        // ReSharper restore CommentTypo

                        arguments.Clear();

                        // ReSharper disable StringLiteralTypo
                        arguments.AppendFormat("-Dbatmass.io.libs.thermo.dir=\"{0}\" -Xmx{1}G -cp \"{2};{3};{4}\" crystalc.Run",
                            thermoLibDirectory.FullName, CRYSTALC_MEMORY_SIZE_GB, jarFileCrystalC.FullName, jarFileBatmassIO.FullName, jarFileGrppr.FullName);

                        arguments.AppendFormat(" {0} {1}", crystalcParamFile.FullName, pepXmlFile.FullName);

                        // ReSharper restore StringLiteralTypo

                        mCmdRunner.WorkDir = experimentGroupDirectory.FullName;
                        mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, "CrystalC_ConsoleOutput.txt");

                        LogDebug(options.JavaProgLoc + " " + arguments);

                        var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments.ToString(), "Java", true);

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
        /// <param name="experimentGroupWorkingDirectories"></param>
        /// <returns></returns>
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

                // Run FreeQuant inside each experiment group working directory, referencing the job's working directory using --dir
                var successCount = 0;

                // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
                foreach (var experimentGroupDirectory in experimentGroupWorkingDirectories.Values)
                {
                    // ReSharper disable once StringLiteralTypo
                    var arguments = string.Format("freequant --ptw 0.4 --tol 10 --isolated --dir {0}", mWorkDir);

                    var success = RunPhilosopher(PhilosopherToolType.FreeQuant, arguments, "annotate the database", experimentGroupDirectory.FullName);

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
            MSFraggerOptions options)
        {
            try
            {
                LogDebug("Running IonQuant", 2);

                // ReSharper disable CommentTypo
                // ReSharper disable IdentifierTypo

                // Find the Bruker lib directory, typically C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.2\ext\bruker
                if (!options.LibraryFinder.FindVendorLibDirectory("bruker", out var brukerLibDirectory))
                    return false;

                // Find the Thermo lib directory, typically C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.2\ext\thermo
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

                // ToDo: Customize this
                var threadCount = 4;

                int matchBetweenRunsFlag;
                if (options.MatchBetweenRuns)
                    matchBetweenRunsFlag = 1;
                else
                    matchBetweenRunsFlag = 0;

                // ReSharper disable StringLiteralTypo

                var arguments = new StringBuilder();

                // java -Xmx4G -Dlibs.bruker.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.3\ext\bruker" -Dlibs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.3\ext\thermo" -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\ionquant-1.7.5.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.23.4.jar" ionquant.IonQuant --threads 4 --ionmobility 0 --mbr 1 --proteinquant 2 --requantify 1 --mztol 10 --imtol 0.05 --rttol 0.4 --mbrmincorr 0 --mbrrttol 1 --mbrimtol 0.05 --mbrtoprun 100000 --ionfdr 0.01 --proteinfdr 1 --peptidefdr 1 --normalization 1 --minisotopes 2 --minscans 3 --writeindex 0 --tp 3 --minfreq 0.5 --minions 2 --minexps 1 --multidir . --filelist C:\FragPipe_Test3\Results\filelist_ionquant.txt

                arguments.AppendFormat(
                    "{0} -Xmx{1}G -Dlibs.bruker.dir=\"{2}\" -Dlibs.thermo.dir=\"{3}\" -cp \"{4};{5}\" ionquant.IonQuant",
                    options.JavaProgLoc,
                    ION_QUANT_MEMORY_SIZE_GB,
                    brukerLibDirectory.FullName,
                    thermoLibDirectory.FullName,
                    jarFileIonQuant.FullName,
                    jarFileBatmassIO.FullName);

                arguments.AppendFormat(" --threads {0} --ionmobility 0 --mbr {1}", threadCount, matchBetweenRunsFlag);

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
                    foreach (var experimentGroupWorkingDirectory in experimentGroupWorkingDirectories.Values)
                    {
                        arguments.AppendFormat(@" --psm {0}\psm.tsv ", experimentGroupWorkingDirectory.Name);
                    }

                    // ToDo: Switch to using filelist_ionquant.txt

                    //  --multidir . --filelist C:\FragPipe_Test3\Results\filelist_ionquant.txt

                    arguments.AppendFormat(" --multidir . --specdir {0}", mWorkDir);

                    foreach (var item in datasetIDsByExperimentGroup)
                    {
                        var experimentGroupName = item.Key;
                        var experimentWorkingDirectory = experimentGroupWorkingDirectories[experimentGroupName];

                        foreach (var datasetId in item.Value)
                        {
                            var datasetName = dataPackageInfo.Datasets[datasetId];

                            arguments.AppendFormat(@" {0}\{1}.pepXML ", experimentWorkingDirectory.Name, datasetName);
                        }
                    }
                }

                // ReSharper restore StringLiteralTypo

                return true;
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
        /// <param name="experimentGroupWorkingDirectories"></param>
        /// <param name="options"></param>
        /// <remarks>
        /// Results will appear in the.tsv files created by the Report step (ion.tsv, peptide.tsv, protein.tsv, and psm.tsv),
        /// in columns corresponding to labels in the AliasNames.txt file
        /// </remarks>
        private bool RunLabelQuant(Dictionary<string, DirectoryInfo> experimentGroupWorkingDirectories, MSFraggerOptions options)
        {
            try
            {
                LogDebug(string.Format("Running LabelQuant for Isobaric Quantification using {0} reporter ions", options.ReporterIonMode), 2);

                var successCount = 0;

                var reporterIonType = options.ReporterIonMode switch
                {
                    ReporterIonModes.Itraq4 => "itraq",
                    ReporterIonModes.Itraq8 => "itraq",
                    ReporterIonModes.Tmt6 => "tmt",
                    ReporterIonModes.Tmt10 => "tmt",
                    ReporterIonModes.Tmt11 => "tmt",
                    ReporterIonModes.Tmt16 => "tmt",
                    _ => throw new ArgumentOutOfRangeException()
                };

                var plex = options.ReporterIonMode switch
                {
                    ReporterIonModes.Itraq4 => 4,
                    ReporterIonModes.Itraq8 => 8,
                    ReporterIonModes.Tmt6 => 6,
                    ReporterIonModes.Tmt10 => 10,
                    ReporterIonModes.Tmt11 => 11,
                    ReporterIonModes.Tmt16 => 16,
                    _ => throw new ArgumentOutOfRangeException()
                };

                foreach (var item in experimentGroupWorkingDirectories)
                {
                    var experimentGroup = item.Key;
                    var aliasFile = new FileInfo(Path.Combine(mWorkDir, string.Format("AliasNames_{0}.txt", experimentGroup)));

                    // ReSharper disable StringLiteralTypo

                    var arguments = string.Format(
                        "labelquant --tol 20 --level 2 --plex {0} --annot {1} --brand {2} --dir {3}",
                        plex,
                        aliasFile.FullName,
                        reporterIonType,
                        mWorkDir);

                    // ReSharper restore StringLiteralTypo

                    var success = RunPhilosopher(PhilosopherToolType.LabelQuant, arguments, "annotate the database", item.Value.FullName);

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
            MSFraggerOptions options,
            out List<FileInfo> peptideProphetPepXmlFiles)
        {
            try
            {
                // Run Peptide Prophet separately against each experiment

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
            MSFraggerOptions options,
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

                        var percolatorSuccess = RunPercolator(experimentGroupDirectory, datasetName, out var percolatorPsmFiles);
                        if (!percolatorSuccess)
                            continue;

                        var percolatorToPepXMLSuccess = ConvertPercolatorOutputToPepXML(options, fragPipeLibDirectory, experimentGroupDirectory, datasetName);
                        if (!percolatorToPepXMLSuccess)
                            continue;

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

        private bool RunPercolator(FileSystemInfo experimentGroupDirectory, string datasetName, out List<FileInfo> percolatorPsmFiles)
        {
            mCmdRunner.WorkDir = experimentGroupDirectory.FullName;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, PERCOLATOR_CONSOLE_OUTPUT);

            // ReSharper disable StringLiteralTypo

            var targetPsmFile = string.Format("{0}_percolator_target_psms.tsv", datasetName);
            var decoyPsmFile = string.Format("{0}_percolator_decoy_psms.tsv", datasetName);

            var arguments = string.Format(
                "--only-psms --no-terminate --post-processing-tdc --num-threads 4 " +
                "--results-psms {1} " +
                "--decoy-results-psms {2} " +
                "{0}.pin",
                datasetName,
                targetPsmFile,
                decoyPsmFile);

            // ReSharper restore StringLiteralTypo

            percolatorPsmFiles = new List<FileInfo>
            {
                new(Path.Combine(mCmdRunner.WorkDir, targetPsmFile)),
                new(Path.Combine(mCmdRunner.WorkDir, decoyPsmFile))
            };

            LogDebug(mPercolatorProgLoc + " " + arguments);

            var processingSuccess = mCmdRunner.RunProgram(mPercolatorProgLoc, arguments, "Percolator", true);

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

        /// <summary>
        /// Run protein prophet
        /// </summary>
        /// <param name="peptideProphetPepXmlFiles">List of .pep.xml files created by peptide prophet</param>
        /// <param name="options"></param>
        private bool RunProteinProphet(ICollection<FileInfo> peptideProphetPepXmlFiles, MSFraggerOptions options)
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

        private bool RunPTMShepherd(MSFraggerOptions options)
        {
            try
            {
                // ReSharper disable CommentTypo
                // ReSharper disable StringLiteralTypo

                // Find the thermo lib directory, typically C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.2\ext\thermo
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
                    writer.WriteLine("TBD");
                    writer.WriteLine("TBD");
                }

                // Run PTMShepherd, example command line:
                // java -Dbatmass.io.libs.thermo.dir="C:\DMS_Programs\MSFragger\fragpipe\tools\MSFragger-3.2\ext\thermo" -cp "C:\DMS_Programs\MSFragger\fragpipe\tools\ptmshepherd-1.0.0.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\batmass-io-1.23.4.jar;C:\DMS_Programs\MSFragger\fragpipe\tools\commons-math3-3.6.1.jar" edu.umich.andykong.ptmshepherd.PTMShepherd "C:DMS_WorkDir\shepherd.config"

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
                mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, "PTMShepherd_ConsoleOutput.txt");

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
            MSFraggerOptions options,
            bool usedProteinProphet)
        {
            try
            {
                LogDebug("Filtering MSFragger Results", 2);

                var successCount = 0;

                var arguments = new StringBuilder();

                var razorBinFilePath = Path.Combine(experimentGroupWorkingDirectories.Values.First().FullName, @".meta\razor.bin");

                foreach (var experimentGroupDirectory in experimentGroupWorkingDirectories.Values)
                {
                    arguments.Clear();

                    // Closed search, without match between runs:
                    // --sequential --razor --picked --prot 0.01

                    // Closed search, with match between runs enabled:
                    // --sequential --razor --prot 0.01

                    // Open search:
                    // --sequential --razor --prot 0.01 --mapmods

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
        /// <param name="datasetIDsByExperimentGroup"></param>
        /// <param name="experimentGroupWorkingDirectories"></param>
        /// <param name="options"></param>
        /// <param name="peptideProphetPepXmlFiles">Output: list of the .pepXML files created by peptide prophet</param>
        /// <remarks>
        /// This method is called when peptide prophet was run against groups of dataset (<seealso cref="UpdateMsMsRunSummaryInPepXmlFiles"/>)</remarks>
        /// <returns>True if success, false if an error</returns>
        private bool UpdateMsMsRunSummaryInCombinedPepXmlFiles(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            IReadOnlyDictionary<string, DirectoryInfo> experimentGroupWorkingDirectories,
            MSFraggerOptions options,
            out List<FileInfo> peptideProphetPepXmlFiles)
        {
            peptideProphetPepXmlFiles = new List<FileInfo>();

            try
            {
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
                    // C:\DMS_Programs\Java\jre8\bin\java.exe -cp C:\DMS_Programs\MSFragger\fragpipe\lib/* com.dmtavt.fragpipe.util.RewritePepxml C:\DMS_WorkDir\CHI_IXN\interact.pep.xml C:\DMS_WorkDir\Dataset1.mzML C:\DMS_WorkDir\Dataset2.mzML

                    // ReSharper enable CommentTypo

                    mCmdRunner.WorkDir = experimentGroupDirectory.FullName;
                    mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, "RewritePepXml_ConsoleOutput.txt");

                    LogDebug(options.JavaProgLoc + " " + arguments);

                    var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments.ToString(), "Java", true);

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
            // This method updates the .pep.xml files created by PeptideProphet to remove the experiment group name and forward slash from the <msms_run_summary> element

            // For example, changing from:
            // <msms_run_summary base_name="C:\DMS_WorkDir\Experiment1/Dataset1" raw_data_type="mzML" comment="This pepXML was from calibrated spectra." raw_data="mzML">

            // To:
            // <msms_run_summary base_name="C:\DMS_WorkDir\Dataset1" raw_data_type="mzML" raw_data="mzML">

            peptideProphetPepXmlFiles = new List<FileInfo>();

            try
            {
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
                    mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, "RewritePepXml_ConsoleOutput.txt");

                    LogDebug(options.JavaProgLoc + " " + arguments);

                    var processingSuccess = mCmdRunner.RunProgram(options.JavaProgLoc, arguments, "Java", true);

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
