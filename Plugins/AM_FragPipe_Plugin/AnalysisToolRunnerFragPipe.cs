﻿//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 04/19/2019
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerFragPipePlugin;
using AnalysisManagerMSFraggerPlugIn;
using AnalysisManagerPepProtProphetPlugIn;
using PRISM;
using PRISM.AppSettings;
using PRISM.Logging;
using PRISMDatabaseUtils;

namespace AnalysisManagerFragPipePlugIn
{
    /// <summary>
    /// Class for running FragPipe
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerFragPipe : AnalysisToolRunnerBase
    {
        // Ignore Spelling: dia, frag

        public const string ANNOTATION_FILE_SUFFIX = "_annotation.txt";

        private const string FRAGPIPE_INSTANCE_DIRECTORY = "FragPipe_v23.0";

        private const string FRAGPIPE_BATCH_FILE_PATH = FRAGPIPE_INSTANCE_DIRECTORY + @"\bin\fragpipe.bat";

        private const string FRAGPIPE_TOOLS_DIRECTORY_PATH = FRAGPIPE_INSTANCE_DIRECTORY + @"\tools";

        private const string FRAGPIPE_DIANN_FILE_PATH = FRAGPIPE_INSTANCE_DIRECTORY + @"\tools\diann\1.8.2_beta_8\windows\DiaNN.exe";

        private const string FRAGPIPE_CONSOLE_OUTPUT = "FragPipe_ConsoleOutput.txt";

        private const string FRAGPIPE_ERROR_INSUFFICIENT_MEMORY_ALLOCATED = "Not enough memory allocated to MSFragger";

        private const string FRAGPIPE_ERROR_INSUFFICIENT_MEMORY_FOR_JAVA = "insufficient memory for the Java Runtime Environment";

        private const string FRAGPIPE_ERROR_OUT_OF_MEMORY = "java.lang.OutOfMemoryError";

        private const string PEPXML_EXTENSION = ".pepXML";

        private const string PIN_EXTENSION = ".pin";

        internal const float PROGRESS_PCT_INITIALIZING = 1;

        private const string PROTEIN_PROPHET_RESULTS_FILE = "combined.prot.xml";

        private enum ProgressPercentValues
        {
            Initializing = 0,
            StartingFragPipe = 1,
            FragPipeComplete = 95,
            ProcessingComplete = 99
        }

        private RunDosProgram mCmdRunner;

        private string mConsoleOutputErrorMsg;

        private int mDatasetCount;

        /// <summary>
        /// Dictionary of experiment group working directories
        /// </summary>
        /// <remarks>
        /// Keys are experiment group name, values are the corresponding working directory
        /// </remarks>
        private Dictionary<string, DirectoryInfo> mExperimentGroupWorkingDirectories;

        // Populate this with a tool version reported to the console
        private string mFragPipeVersion;

        /// <summary>
        /// Path to fragpipe.bat, e.g. C:\DMS_Programs\FragPipe\FragPipe_v23.0\bin\fragpipe.bat
        /// </summary>
        private string mFragPipeProgLoc;

        private DateTime mLastConsoleOutputParse;

        private FastaFileUtilities mFastaUtils;

        private bool mToolVersionWritten;

        private bool mWarnedInvalidDatasetCount;

        private DirectoryInfo mWorkingDirectory;

        private static ZipFileTools mZipTool;

        /// <summary>
        /// Runs FragPipe tool
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
                    LogDebug("AnalysisToolRunnerFragPipe.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                mExperimentGroupWorkingDirectories = new Dictionary<string, DirectoryInfo>(StringComparer.OrdinalIgnoreCase);

                mWorkingDirectory = new DirectoryInfo(mWorkDir);

                var fileCopyUtils = new FileCopyUtilities(mFileTools, mMyEMSLUtilities, mDebugLevel);
                RegisterEvents(fileCopyUtils);

                mFastaUtils = new FastaFileUtilities(fileCopyUtils, mMgrParams, mJobParams);
                RegisterEvents(mFastaUtils);
                UnregisterEventHandler(mFastaUtils, BaseLogger.LogLevels.ERROR);

                mFastaUtils.ErrorEvent += FastaUtilsErrorEventHandler;

                if (!mFastaUtils.DefineConnectionStrings())
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to the FragPipe batch file, e.g. C:\DMS_Programs\FragPipe\FragPipe_v23.0\bin\fragpipe.bat

                mFragPipeProgLoc = DetermineProgramLocation("FragPipeProgLoc", FRAGPIPE_BATCH_FILE_PATH);

                if (string.IsNullOrWhiteSpace(mFragPipeProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the FragPipe version info in the database after the first line is written to file FragPipe_ConsoleOutput.txt
                mToolVersionWritten = false;
                mFragPipeVersion = string.Empty;

                mConsoleOutputErrorMsg = string.Empty;

                if (!ValidateFastaFile(out var fastaFile))
                {
                    // Abort processing
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var organizeResult = OrganizeFilesForFragPipe(out var dataPackageInfo, out var datasetIDsByExperimentGroup);

                if (organizeResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return organizeResult;
                }

                // Process the mzML files using FragPipe
                var processingResult = StartFragPipe(
                    fastaFile, dataPackageInfo, datasetIDsByExperimentGroup,
                    out var diaSearchEnabled,
                    out var databaseSplitCount,
                    out var annotationFilesToSkip);

                mProgress = (int)ProgressPercentValues.ProcessingComplete;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                Global.IdleLoop(0.5);
                AppUtils.GarbageCollectNow();

                var postProcessResult = PostProcessResults(dataPackageInfo, datasetIDsByExperimentGroup, diaSearchEnabled, databaseSplitCount);

                if (!AnalysisJob.SuccessOrNoData(processingResult) || !AnalysisJob.SuccessOrNoData(postProcessResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory(true);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                try
                {
                    foreach (var annotationFile in annotationFilesToSkip)
                    {
                        annotationFile.Delete();
                    }
                }
                catch (Exception ex)
                {
                    LogWarning("Error deleting duplicate annotation file defined in annotationFilesToSkip: {0}", ex.Message);
                }

                var subdirectoriesToSkip = new SortedSet<string>();
                var success = CopyResultsToTransferDirectory(true, subdirectoriesToSkip);

                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                return processingResult;
            }
            catch (Exception ex)
            {
                LogError("Error in FragPipePlugin->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory(bool includeSubdirectories = false)
        {
            mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_MZML_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory(includeSubdirectories);
        }

        /// <summary>
        /// Search for files matching the pattern in the given directory and all subdirectories
        /// If one or more files is found, at the newest one to toolFiles
        /// </summary>
        /// <param name="toolFiles">List of files to include in the version info string</param>
        /// <param name="parentDirectory">Parent directory</param>
        /// <param name="searchPattern">Search pattern, e.g. "MSFragger-*.jar"</param>
        private void AddNewestMatchingFile(ICollection<FileInfo> toolFiles, DirectoryInfo parentDirectory, string searchPattern)
        {
            var matchingFiles = parentDirectory.GetFiles(searchPattern, SearchOption.AllDirectories);

            if (matchingFiles.Length == 0)
            {
                LogWarning("Could not find a match for {0} in {1}", searchPattern, parentDirectory.FullName);
                return;
            }

            toolFiles.Add(GetNewestFile(matchingFiles));
        }

        /// <summary>
        /// Create the manifest file, which lists the input .mzML files, the experiment for each (optional), and the data type for each
        /// </summary>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="datasetIDsByExperimentGroup">
        /// Keys in this dictionary are experiment group names, values are a list of Dataset IDs for each experiment group
        /// If experiment group names are not defined in the data package, this dictionary will have a single entry named __UNDEFINED_EXPERIMENT_GROUP__
        /// However, if there is only one dataset in dataPackageInfo, the experiment name of the dataset will be used
        /// </param>
        /// <param name="manifestFilePath">Output: manifest file path</param>
        /// <param name="diaSearchEnabled">Output: true if the data type is DIA, DIA-Quant, or DIA-Lib</param>
        private bool CreateManifestFile(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            out string manifestFilePath,
            out bool diaSearchEnabled)
        {
            diaSearchEnabled = false;

            try
            {
                if (dataPackageInfo.DatasetFiles.Count == 0)
                {
                    LogError("DatasetFiles list in dataPackageInfo is empty");
                    manifestFilePath = string.Empty;
                    return false;
                }

                var datasetFileExtension = string.Empty;

                foreach (var item in dataPackageInfo.DatasetFiles)
                {
                    if (string.IsNullOrEmpty(datasetFileExtension))
                    {
                        datasetFileExtension = Path.GetExtension(item.Value);
                        continue;
                    }

                    var fileExtension = Path.GetExtension(item.Value);

                    if (fileExtension.Equals(datasetFileExtension, StringComparison.OrdinalIgnoreCase))
                        continue;

                    LogError("Files in dataPackageInfo.DatasetFiles do not all have the same file extension; expecting {0} but found {1}",
                        datasetFileExtension, fileExtension);

                    manifestFilePath = string.Empty;
                    return false;
                }

                manifestFilePath = Path.Combine(mWorkDir, "datasets.fp-manifest");

                // FragPipe creates the following file, which is the same as datasets.fp-manifest but with a different text encoding; skip it
                mJobParams.AddResultFileToSkip("fragpipe-files.fp-manifest");

                using var writer = new StreamWriter(new FileStream(manifestFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                var outputDirectoryPaths = new SortedSet<string>();

                foreach (var experimentGroup in datasetIDsByExperimentGroup)
                {
                    var experimentGroupName = experimentGroup.Key;
                    var experimentWorkingDirectory = mExperimentGroupWorkingDirectories[experimentGroupName];

                    // Prior to July 2025, if there was only one experiment group, we left the .mzML files in the working directory (since method MoveDatasetsIntoSubdirectories was not called)
                    // However, if isobaric quantitation using TMT Integrator and Philosopher is enabled, the .mzML files must be in the same directory as the _annotation.txt file
                    // Thus, starting in July 2025, the .mzML files are always moved into the experiment group directory, even if there is only one experiment group directory

                    var datasetFileDirectory = experimentWorkingDirectory.FullName;

                    foreach (var datasetID in experimentGroup.Value)
                    {
                        var datasetFile = dataPackageInfo.DatasetFiles[datasetID];
                        var datasetFilePath = Path.Combine(datasetFileDirectory, datasetFile);

                        outputDirectoryPaths.Add(Path.Combine(mWorkDir, experimentGroup.Key));

                        string dataType;

                        // Data types supported by FragPipe: DDA, DDA+, DIA, DIA-Quant, DIA-Lib
                        // This method chooses either DDA or DIA, depending on the dataset type

                        if (dataPackageInfo.DatasetTypes.TryGetValue(datasetID, out var datasetType))
                        {
                            if (datasetType.Contains("DIA"))
                            {
                                diaSearchEnabled = true;
                                dataType = "DIA";
                            }
                            else
                            {
                                dataType = "DDA";
                            }
                        }
                        else
                        {
                            dataType = "DDA";
                        }

                        // ReSharper disable once IdentifierTypo
                        const string BIOREPLICATE = "";

                        writer.WriteLine("{0}\t{1}\t{2}\t{3}", datasetFilePath, experimentGroup.Key, BIOREPLICATE, dataType);
                    }
                }

                // Make sure each of the output directories exists
                foreach (var outputDirectoryPath in outputDirectoryPaths)
                {
                    var outputDirectory = new DirectoryInfo(outputDirectoryPath);
                    if (!outputDirectory.Exists)
                        outputDirectory.Create();
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in CreateManifestFile", ex);
                manifestFilePath = string.Empty;
                return false;
            }
        }

        private FileInfo CreateReporterIonAnnotationFile(ReporterIonInfo.ReporterIonModes reporterIonMode, FileInfo aliasNameFile, string sampleNamePrefix)
        {
            try
            {
                var reporterIonNames = ReporterIonInfo.GetReporterIonNames(reporterIonMode);

                if (reporterIonNames.Count == 0)
                {
                    LogWarning("Unrecognized reporter ion mode in CreateReporterIonAnnotationFile: " + reporterIonMode);
                }

                return ReporterIonInfo.CreateReporterIonAnnotationFile(reporterIonMode, aliasNameFile, sampleNamePrefix);
            }
            catch (Exception ex)
            {
                LogError("Error in CreateReporterIonAnnotationFile", ex);
                return null;
            }
        }

        /// <summary>
        /// Create annotation.txt files that define alias names for reporter ions
        /// </summary>
        /// <param name="options">Processing options</param>
        /// <param name="annotationFilesToSkip">Output: list of annotation files to not copy to the transfer directory</param>
        private bool CreateReporterIonAnnotationFiles(FragPipeOptions options, out List<FileInfo> annotationFilesToSkip)
        {
            annotationFilesToSkip = new List<FileInfo>();

            try
            {
                if (options.ReporterIonMode == ReporterIonInfo.ReporterIonModes.Disabled)
                {
                    // Nothing to do
                    return true;
                }

                LogMessage("Creating annotation.txt files that define alias names for reporter ions");

                var successCount = 0;

                var reporterIonType = options.ReporterIonMode switch
                {
                    ReporterIonInfo.ReporterIonModes.Itraq4 => "iTraq",
                    ReporterIonInfo.ReporterIonModes.Itraq8 => "iTraq",
                    ReporterIonInfo.ReporterIonModes.Tmt6 => "TMT",
                    ReporterIonInfo.ReporterIonModes.Tmt10 => "TMT",
                    ReporterIonInfo.ReporterIonModes.Tmt11 => "TMT",
                    ReporterIonInfo.ReporterIonModes.Tmt16 => "TMT",
                    ReporterIonInfo.ReporterIonModes.Tmt18 => "TMT",
                    _ => throw new ArgumentOutOfRangeException()
                };

                var groupNumber = 0;

                // If multiple experiment groups are defined, try to get shorter experiment group names, which will be used for the sample names in the AliasNames.txt files
                var experimentGroupAbbreviations = ReporterIonInfo.GetAbbreviatedExperimentGroupNames(mExperimentGroupWorkingDirectories.Keys.ToList());

                foreach (var experimentGroup in mExperimentGroupWorkingDirectories)
                {
                    groupNumber++;

                    var aliasNamePrefix = experimentGroup.Key.Equals(DataPackageInfoLoader.UNDEFINED_EXPERIMENT_GROUP)
                        ? string.Format("annotation_{0}", groupNumber)
                        : experimentGroup.Key;

                    var experimentSpecificAliasFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, string.Format("{0}{1}", aliasNamePrefix, ANNOTATION_FILE_SUFFIX)));
                    var genericAliasFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "AliasNames.txt"));
                    var genericAliasFile2 = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "AliasName.txt"));

                    var targetFile = new FileInfo(Path.Combine(experimentGroup.Value.FullName, experimentSpecificAliasFile.Name));

                    var useExpGroupAnnotationFiles = mJobParams.GetJobParameter("UseExpGroupAnnotationFiles", false);

                    if (useExpGroupAnnotationFiles && !experimentSpecificAliasFile.Exists)
                    {
                        LogError(string.Format("The settings file for this job has UseExpGroupAnnotationFiles=true, but file {0} was not found", experimentSpecificAliasFile.Name));
                        return false;
                    }

                    if (experimentSpecificAliasFile.Exists || genericAliasFile.Exists || genericAliasFile2.Exists)
                    {
                        FileInfo sourceAnnotationFile;

                        if (experimentSpecificAliasFile.Exists)
                        {
                            // Copy the file into the experiment group working directory
                            sourceAnnotationFile = experimentSpecificAliasFile;
                            annotationFilesToSkip.Add(experimentSpecificAliasFile);
                        }
                        else if (genericAliasFile.Exists)
                        {
                            // Copy the file into the experiment group working directory, renaming it to end with _annotation.txt (as required by FragPipe)
                            sourceAnnotationFile = genericAliasFile;
                        }
                        else if (genericAliasFile2.Exists)
                        {
                            // Copy the file into the experiment group working directory, renaming it to end with _annotation.txt (as required by FragPipe)
                            sourceAnnotationFile = genericAliasFile2;
                        }
                        else
                        {
                            throw new Exception("If statement logic error in CreateReporterIonAnnotationFiles when determining the source and target annotation.txt files");
                        }

                        sourceAnnotationFile.CopyTo(targetFile.FullName, true);
                        successCount++;
                        continue;
                    }

                    LogMessage(
                        "{0} alias file not found; will auto-generate file {1} for use by TMT Integrator",
                        reporterIonType, targetFile.Name);

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

                    var annotationFile = CreateReporterIonAnnotationFile(options.ReporterIonMode, targetFile, prefixToUse);

                    if (annotationFile == null)
                        return false;

                    successCount++;
                }

                return successCount == mExperimentGroupWorkingDirectories.Count;
            }
            catch (Exception ex)
            {
                LogError("Error in CreateReporterIonAnnotationFiles", ex);
                return false;
            }
        }

        /// <summary>
        /// Determine the path to the FragPipe tools directory
        /// </summary>
        /// <param name="toolsDirectory">Output: path to the tools directory below the FragPipe instance directory (FRAGPIPE_INSTANCE_DIRECTORY), e.g. C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools</param>
        /// <param name="fragPipeProgLoc">Output: path to the FragPipe directory below DMS_Programs, e.g. C:\DMS_Programs\FragPipe</param>
        /// <returns>True if the directory was found, false if missing or an error</returns>
        private bool DetermineFragPipeToolLocations(out DirectoryInfo toolsDirectory, out string fragPipeProgLoc)
        {
            try
            {
                // Manager parameter "FragPipeProgLoc" should be "C:\DMS_Programs\FragPipe"
                fragPipeProgLoc = mMgrParams.GetParam("FragPipeProgLoc");

                if (!Directory.Exists(fragPipeProgLoc))
                {
                    if (fragPipeProgLoc.Length == 0)
                    {
                        LogError("Parameter 'FragPipeProgLoc' not defined for this manager", true);
                    }
                    else
                    {
                        LogError("Cannot find the FragPipe directory: " + fragPipeProgLoc, true);
                    }

                    toolsDirectory = null;
                    return false;
                }

                toolsDirectory = new DirectoryInfo(Path.Combine(fragPipeProgLoc, FRAGPIPE_TOOLS_DIRECTORY_PATH));

                if (toolsDirectory.Exists)
                    return true;

                LogError("Cannot find the FragPipe tools directory: " + toolsDirectory.FullName, true);
                return false;
            }
            catch (Exception ex)
            {
                LogError("Error determining FragPipe tool locations", ex);
                toolsDirectory = null;
                fragPipeProgLoc = string.Empty;
                return false;
            }
        }

        /// <summary>
        /// Determine the path to the FragPipe tools directory, DiaNN.exe, and Python.exe
        /// </summary>
        /// <param name="fragPipePaths">Output: FragPipe program paths (tools directory, DiaNN.exe and python.exe)</param>
        /// <returns>True if the directory was found, false if missing or an error</returns>
        private bool DetermineFragPipeToolLocations(out FragPipeProgramPaths fragPipePaths)
        {
            try
            {
                if (!DetermineFragPipeToolLocations(out var toolsDirectory, out var fragPipeProgLoc))
                {
                    fragPipePaths = new FragPipeProgramPaths(toolsDirectory);
                    return false;
                }

                fragPipePaths = new FragPipeProgramPaths(toolsDirectory)
                {
                    DiannExe = new FileInfo(Path.Combine(fragPipeProgLoc, FRAGPIPE_DIANN_FILE_PATH))
                };

                if (!fragPipePaths.DiannExe.Exists)
                {
                    LogError("Cannot find the DiaNN executable: " + fragPipePaths.DiannExe.FullName, true);
                    return false;
                }

                // Verify that Python.exe exists
                // Prior to May 2025, used Python3ProgLoc, which is "C:\Python3"
                // FragPipe v23 ships with Python 3.11.11, which is tracked by manager parameter FragPipePython3ProgLoc, with value C:\DMS_Programs\FragPipe\FragPipe_v23.0\python
                var pythonProgLoc = mMgrParams.GetParam("FragPipePython3ProgLoc");

                if (!Directory.Exists(pythonProgLoc))
                {
                    if (pythonProgLoc.Length == 0)
                    {
                        LogError("Parameter 'FragPipePython3ProgLoc' not defined for this manager", true);
                    }
                    else
                    {
                        LogError("The Python directory does not exist: " + pythonProgLoc, true);
                    }

                    return false;
                }

                fragPipePaths.PythonExe = new FileInfo(Path.Combine(pythonProgLoc, "python.exe"));

                if (!fragPipePaths.PythonExe.Exists)
                {
                    LogError("Python executable not found at: " + fragPipePaths.PythonExe.FullName, true);
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error determining FragPipe tool locations", ex);
                fragPipePaths = new FragPipeProgramPaths(null);
                return false;
            }
        }

        /// <summary>
        /// Replace colons and backslashes in the given path with \: and \\
        /// </summary>
        /// <param name="fileOrDirectoryPath">Path to escape</param>
        /// <returns>Escaped path</returns>
        private static string EscapeFragPipeWorkFlowParameterPath(string fileOrDirectoryPath)
        {
            return fileOrDirectoryPath.Replace(@"\", @"\\").Replace(":", @"\:");
        }

        private static void FindDatasetPinFiles(string sourceDirectoryPath, string datasetName, out FileInfo pinFile, out FileInfo pinFileEdited)
        {
            pinFile = new FileInfo(Path.Combine(sourceDirectoryPath, string.Format("{0}{1}", datasetName, PIN_EXTENSION)));
            pinFileEdited = new FileInfo(Path.Combine(sourceDirectoryPath, string.Format("{0}_edited{1}", datasetName, PIN_EXTENSION)));
        }

        /// <summary>
        /// Get appropriate path of the working directory for the given experiment
        /// </summary>
        /// <remarks>
        /// <para>If all the datasets belong to the same experiment, return the job's working directory</para>
        /// <para>Otherwise, return a subdirectory below the working directory, based on the experiment's name</para>
        /// </remarks>
        /// <param name="experimentGroupName">Experiment group name</param>
        private DirectoryInfo GetExperimentGroupWorkingDirectory(string experimentGroupName)
        {
            var cleanName = Global.ReplaceInvalidPathChars(experimentGroupName);

            return new DirectoryInfo(Path.Combine(mWorkingDirectory.FullName, cleanName));
        }

        private DirectoryInfo GetExperimentGroupWorkingDirectoryToUse(
            DataPackageInfo dataPackageInfo,
            KeyValuePair<string, SortedSet<int>> experimentGroup,
            DirectoryInfo experimentGroupWorkingDirectory,
            bool diaSearchEnabled)
        {
            if (AnalysisToolRunnerMSFragger.ExperimentGroupWorkingDirectoryHasResults(experimentGroupWorkingDirectory))
            {
                return experimentGroupWorkingDirectory;
            }

            foreach (var datasetID in experimentGroup.Value)
            {
                var datasetName = dataPackageInfo.Datasets[datasetID];
                var pepXmlFiles = FindDatasetPinFileAndPepXmlFiles(mWorkingDirectory, diaSearchEnabled, datasetName, out var pinFile);

                if (pepXmlFiles.Count > 0)
                    return mWorkingDirectory;

                if (pinFile.Exists)
                    return mWorkingDirectory;
            }

            return experimentGroupWorkingDirectory;
        }

        private static List<FileInfo> FindDatasetPinFileAndPepXmlFiles(
            DirectoryInfo workingDirectory,
            bool diaSearchEnabled,
            string datasetName,
            out FileInfo pinFile)
        {
            var pepXmlFiles = new List<FileInfo>();

            FindDatasetPinFiles(workingDirectory.FullName, datasetName, out var pinFileUnedited, out var pinFileEdited);

            pinFile = pinFileEdited.Exists ? pinFileEdited : pinFileUnedited;

            if (diaSearchEnabled)
            {
                // Look for files matching DatasetName_rank*.pepXML
                // For example:
                //   QC_Dataset_rank1.pepXML
                //   QC_Dataset_rank2.pepXML

                var searchPattern = string.Format("{0}_rank*{1}", datasetName, PEPXML_EXTENSION);

                pepXmlFiles.AddRange(workingDirectory.GetFiles(searchPattern));

                return pepXmlFiles.Count > 0 ? pepXmlFiles : new List<FileInfo>();
            }

            pepXmlFiles.Add(new FileInfo(Path.Combine(workingDirectory.FullName, datasetName + PEPXML_EXTENSION)));

            return pepXmlFiles[0].Exists ? pepXmlFiles : new List<FileInfo>();
        }

        /// <summary>
        /// Given a linked list of progress values (which should have been populated in ascending order), find the next progress value
        /// </summary>
        /// <param name="progressValues">List of progress values</param>
        /// <param name="currentProgress">Current progress</param>
        /// <returns>Next progress value, or 100 if either the current value is not found, or the next value is not defined</returns>
        private static int GetNextProgressValue(LinkedList<int> progressValues, int currentProgress)
        {
            var currentNode = progressValues.Find(currentProgress);

            if (currentNode?.Next == null)
                return 100;

            return currentNode.Next.Value;
        }

        /// <summary>
        /// Determine the number of threads to use for FragPipe
        /// </summary>
        private int GetNumThreadsToUse()
        {
            var coreCount = Global.GetCoreCount();

            if (coreCount > 4)
            {
                return coreCount - 1;
            }

            return coreCount;
        }

        private string GetComment(KeyValueParamFileLine setting, string defaultComment)
        {
            return string.IsNullOrWhiteSpace(setting.Comment)
                ? defaultComment
                : setting.Comment;
        }

        private static FileInfo GetNewestFile(IEnumerable<FileInfo> files)
        {
            return (from file in files orderby file.LastWriteTimeUtc descending select file).FirstOrDefault();
        }

        private static Regex GetRegEx(string matchPattern, bool ignoreCase = true)
        {
            var options = ignoreCase ? RegexOptions.Compiled | RegexOptions.IgnoreCase : RegexOptions.Compiled;
            return new Regex(matchPattern, options);
        }

        private void LookForErrorsInFragPipeConsoleOutputFile(string consoleOutputFilePath)
        {
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

                if (mDebugLevel >= 4)
                {
                    LogDebug("Looking for errors in file " + consoleOutputFilePath);
                }

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var trimmedLine = dataLine.Trim();

                    // Check for "Not enough memory allocated to MSFragger"
                    if (trimmedLine.StartsWith(FRAGPIPE_ERROR_INSUFFICIENT_MEMORY_ALLOCATED, StringComparison.OrdinalIgnoreCase))
                    {
                        LogError("Error running FragPipe: " + trimmedLine);
                        return;
                    }

                    // Check for "There is insufficient memory for the Java Runtime Environment to continue."
                    var startIndexA = trimmedLine.IndexOf(FRAGPIPE_ERROR_OUT_OF_MEMORY, StringComparison.OrdinalIgnoreCase);

                    // Check for "java.lang.OutOfMemoryError"
                    var startIndexB = trimmedLine.IndexOf(FRAGPIPE_ERROR_INSUFFICIENT_MEMORY_FOR_JAVA, StringComparison.OrdinalIgnoreCase);

                    if (startIndexA >= 0)
                    {
                        LogError("Error running FragPipe: " + trimmedLine.Substring(startIndexA));
                        return;
                    }

                    // ReSharper disable once InvertIf
                    if (startIndexB >= 0)
                    {
                        LogError("Error running FragPipe: " + trimmedLine.Substring(startIndexB));
                        return;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error in LookForErrorsInFragPipeConsoleOutputFile", ex);
            }
        }

        private bool MoveDatasetsIntoSubdirectories(DataPackageInfo dataPackageInfo, SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup)
        {
            try
            {
                foreach (var item in datasetIDsByExperimentGroup)
                {
                    var experimentGroupName = item.Key;
                    var experimentWorkingDirectory = mExperimentGroupWorkingDirectories[experimentGroupName];

                    // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                    foreach (var datasetId in item.Value)
                    {
                        var datasetFile = dataPackageInfo.DatasetFiles[datasetId];

                        var sourceDirectoryPath = mWorkingDirectory.FullName;
                        var targetDirectoryPath = experimentWorkingDirectory.FullName;

                        var success = MoveFile(sourceDirectoryPath, datasetFile, targetDirectoryPath);

                        if (!success)
                            return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in MoveDatasetsIntoSubdirectories", ex);
                return false;
            }
        }

        private bool MoveFile(string sourceDirectoryPath, string fileName, string targetDirectoryPath)
        {
            return MoveFile(sourceDirectoryPath, fileName, targetDirectoryPath, fileName);
        }

        private bool MoveFile(string sourceDirectoryPath, string sourceFileName, string targetDirectoryPath, string targetFileName)
        {
            try
            {
                var sourceFile = new FileInfo(Path.Combine(sourceDirectoryPath, sourceFileName));

                var targetPath = Path.Combine(targetDirectoryPath, targetFileName);

                sourceFile.MoveTo(targetPath);

                return true;
            }
            catch (Exception ex)
            {
                LogError(
                    string.Format("Error in MoveFile, moving file {0} from {1} to {2}{3}",
                        sourceFileName,
                        sourceDirectoryPath,
                        targetDirectoryPath,
                        sourceFileName.Equals(targetFileName) ? string.Empty : string.Format(", renaming to {0}", targetFileName)),
                    ex);

                return false;
            }
        }

        // ReSharper disable once CommentTypo

        /// <summary>
        /// Move the retention time .png files into each experiment group's working directory, renaming to end with _RTplot.png
        /// Move the score histogram .png files into each experiment group's working directory, removing "_edited" from the name
        /// </summary>
        private void MovePlotFiles()
        {
            try
            {
                foreach (var experimentGroupDirectory in mExperimentGroupWorkingDirectories.Values)
                {
                    foreach (var plotFile in experimentGroupDirectory.GetFiles("*.png", SearchOption.AllDirectories))
                    {
                        if (plotFile.Directory == null)
                        {
                            plotFile.MoveTo(Path.Combine(experimentGroupDirectory.FullName, plotFile.Name));
                            continue;
                        }

                        if (plotFile.Directory.FullName.Equals(experimentGroupDirectory.FullName))
                            continue;

                        string targetFileName;

                        if (plotFile.Directory.Name.Equals("RT_calibration_curves", StringComparison.OrdinalIgnoreCase) ||
                            plotFile.Directory.Name.Equals("RTPlots", StringComparison.OrdinalIgnoreCase))
                        {
                            // Plots in this directory show observed vs. predicted retention time (aka elution time)

                            // ReSharper disable once StringLiteralTypo
                            targetFileName = string.Format("{0}_RTplot.png", Path.GetFileNameWithoutExtension(plotFile.Name));
                        }
                        else if (plotFile.Directory.Name.Equals("score_histograms", StringComparison.OrdinalIgnoreCase))
                        {
                            targetFileName = plotFile.Name.Replace("_edited_", "_");
                        }
                        else
                        {
                            targetFileName = plotFile.Name;
                        }

                        plotFile.MoveTo(Path.Combine(experimentGroupDirectory.FullName, targetFileName));
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error in MovePlotFiles", ex);
            }
        }

        private bool MoveResultsIntoDirectory(DirectoryInfo sourceDirectory, FileSystemInfo targetDirectory, bool recurse)
        {
            try
            {
                foreach (var sourceFile in sourceDirectory.GetFiles())
                {
                    var targetFile = new FileInfo(Path.Combine(targetDirectory.FullName, sourceFile.Name));

                    bool success;

                    if (!targetFile.Exists)
                    {
                        success = MoveFile(sourceDirectory.FullName, sourceFile.Name, targetDirectory.FullName);
                    }
                    else if (targetFile.Name.EndsWith(ANNOTATION_FILE_SUFFIX))
                    {
                        LogDebug("MoveResultsIntoDirectory: replacing {0} with {1}", targetFile.FullName, sourceFile.FullName);

                        // Replace the target file with the source file
                        targetFile.Delete();

                        success = MoveFile(sourceDirectory.FullName, sourceFile.Name, targetDirectory.FullName);
                    }
                    else
                    {
                        // Append the source directory name to the file
                        var alternateName = string.Format("{0}_{1}{2}",
                            Path.GetFileNameWithoutExtension(sourceFile.Name),
                            sourceDirectory.Name,
                            Path.GetExtension(sourceFile.Name));

                        var alternateTargetFile = new FileInfo(Path.Combine(targetDirectory.FullName, alternateName));

                        if (alternateTargetFile.Exists)
                        {
                            LogError("Cannot move file from {0} since the target file exists ({1}), and the alternate target file also exists ({2}",
                                sourceDirectory.FullName,
                                targetFile.FullName,
                                alternateName);

                            return false;
                        }

                        success = MoveFile(sourceDirectory.FullName, sourceFile.Name, targetDirectory.FullName, alternateTargetFile.Name);
                    }

                    if (!success)
                        return false;
                }

                if (!recurse)
                    return true;

                // Also move files in subdirectories
                foreach (var subdirectory in sourceDirectory.GetDirectories())
                {
                    if (subdirectory.GetFileSystemInfos().Length == 0)
                        continue;

                    var targetSubdirectory = new DirectoryInfo(Path.Combine(targetDirectory.FullName, subdirectory.Name));

                    if (!targetSubdirectory.Exists)
                    {
                        targetSubdirectory.Create();
                    }

                    var filesMoved = MoveResultsIntoDirectory(subdirectory, targetSubdirectory, true);

                    if (!filesMoved)
                        return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in MoveResultsIntoWorkingDirectory", ex);
                return false;
            }
        }

        /// <summary>
        /// Organize .mzML files and populate several dictionaries
        /// </summary>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="datasetIDsByExperimentGroup">
        /// Keys in this dictionary are experiment group names, values are a list of Dataset IDs for each experiment group
        /// If experiment group names are not defined in the data package, this dictionary will have a single entry named __UNDEFINED_EXPERIMENT_GROUP__
        /// However, if there is only one dataset in dataPackageInfo, the experiment name of the dataset will be used
        /// </param>
        /// <returns>Result code</returns>
        private CloseOutType OrganizeDatasetFiles(
            out DataPackageInfo dataPackageInfo,
            out SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup)
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

            // If Experiment Groups are defined, create a subdirectory for each experiment group
            // If using TMT, put a separate annotation.txt file in each subdirectory (the filename must end with "annotation.txt")

            var experimentGroupNames = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var item in datasetIDsByExperimentGroup.Keys)
            {
                experimentGroupNames.Add(item);
            }

            // Populate a dictionary with experiment group names and corresponding working directories
            foreach (var experimentGroupName in experimentGroupNames)
            {
                var workingDirectory = GetExperimentGroupWorkingDirectory(experimentGroupName);

                mExperimentGroupWorkingDirectories.Add(experimentGroupName, workingDirectory);
            }

            // Create a subdirectory for each experiment group
            foreach (var experimentGroupDirectory in mExperimentGroupWorkingDirectories.Values)
            {
                if (!experimentGroupDirectory.Exists)
                {
                    experimentGroupDirectory.Create();
                }
            }

            // Prior to July 2025, if there was only one experiment group, we left the .mzML files in the working directory
            // However, if isobaric quantitation using TMT Integrator and Philosopher is enabled, the .mzML files must be in the same directory as the _annotation.txt file
            // Thus, starting in July 2025, the .mzML files are always moved into the experiment group directory, even if there is only one experiment group directory

            // Since we have multiple experiment groups, move the .mzML files into subdirectories
            var moveSuccess = MoveDatasetsIntoSubdirectories(dataPackageInfo, datasetIDsByExperimentGroup);

            return moveSuccess ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        private CloseOutType OrganizeFilesForFragPipe(
            out DataPackageInfo dataPackageInfo,
            out SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup)
        {
            LogMessage("Preparing to run FragPipe");

            var moveFilesSuccess = OrganizeDatasetFiles(
                out dataPackageInfo,
                out datasetIDsByExperimentGroup);

            if (moveFilesSuccess != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return moveFilesSuccess;
            }

            if (dataPackageInfo.DatasetFiles.Count == 0)
            {
                LogError("No datasets were found (dataPackageInfo.DatasetFiles is empty after calling OrganizeDatasetFiles)");
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Parse the FragPipe console output file to determine the FragPipe version and to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath">Console output file path</param>
        private void ParseFragPipeConsoleOutputFile(string consoleOutputFilePath)
        {
            // ReSharper disable CommentTypo

            // ----------------------------------------------------
            // Example Console output
            // ----------------------------------------------------

            // System OS: Windows 10, Architecture: AMD64
            // Java Info: 11.0.24, OpenJDK 64-Bit Server VM, Eclipse Adoptium
            // .NET Core Info: N/A
            //
            // Version info:
            // FragPipe version 23.0
            // DIA-Umpire version 2.3.2
            // diaTracer version 1.2.5
            // MSFragger version 4.2
            // Crystal-C version 1.5.8
            // MSBooster version 1.3.9
            // Percolator version 3.7.1
            // Philosopher version 5.1.1
            // PTM-Shepherd version 3.0.1
            // IonQuant version 1.11.9
            // TMT-Integrator version 6.1.1
            // EasyPQP version 0.1.52
            // DIA-NN version 1.8.2 beta 8
            // Skyline version N/A
            // Pandas version 2.2.3
            // Numpy version 1.26.4
            //
            // LCMS files:
            //   Experiment/Group: NYBB_30_P01_P
            //   (if "spectral library generation" is enabled, all files will be analyzed together)
            //   - D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML	DDA
            //   - D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML	DDA
            //   - D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML	DDA
            //   Experiment/Group: NYBB_30_P02_P
            //   (if "spectral library generation" is enabled, all files will be analyzed together)
            //   - D:\DMS_WorkDir1\NYBB_30_P02_P\NYBB_30_P02_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML	DDA
            //   - D:\DMS_WorkDir1\NYBB_30_P02_P\NYBB_30_P02_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML	DDA
            //   - D:\DMS_WorkDir1\NYBB_30_P02_P\NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML	DDA
            //
            // 50 commands to execute:
            // CheckCentroid
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\jre\bin\java.exe -Xmx60G -cp C:\DMS_Programs\FragPipe\FragPipe_v23.0\lib\fragpipe-23.0.jar;C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\batmass-io-1.35.1.jar org.nesvilab.fragpipe.util.CheckCentroid D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML 23
            // WorkspaceCleanInit [Work dir: D:\DMS_WorkDir1\NYBB_30_P02_P]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\Philosopher\philosopher-v5.1.1.exe workspace --clean --nocheck
            // ...
            // MSFragger [Work dir: D:\DMS_WorkDir1]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\jre\bin\java.exe -jar -Dfile.encoding=UTF-8 -Xmx60G C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\MSFragger-4.3\MSFragger-4.3.jar D:\DMS_WorkDir1\fragger.params D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML D:\DMS_WorkDir1\NYBB_30_P02_P\NYBB_30_P02_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML D:\DMS_WorkDir1\NYBB_30_P02_P\NYBB_30_P02_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML D:\DMS_WorkDir1\NYBB_30_P02_P\NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML
            // Percolator [Work dir: D:\DMS_WorkDir1\NYBB_30_P01_P]
            // ...
            // Percolator: Convert to pepxml [Work dir: D:\DMS_WorkDir1\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\jre\bin\java.exe -cp C:\DMS_Programs\FragPipe\FragPipe_v23.0\lib/* org.nesvilab.fragpipe.tools.percolator.PercolatorOutputToPepXML NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.pin NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19 NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19_percolator_target_psms.tsv NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19_percolator_decoy_psms.tsv interact-NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19 DDA 0.5 D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML
            // Percolator delete temp
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\jre\bin\java.exe -cp C:\DMS_Programs\FragPipe\FragPipe_v23.0\lib\fragpipe-23.0.jar org.nesvilab.utils.FileDelete D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19_percolator_target_psms.tsv
            // ...
            // PTMProphet [Work dir: D:\DMS_WorkDir1\NYBB_30_P02_P]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\PTMProphet\PTMProphetParser.exe NOSTACK KEEPOLD STATIC FRAGPPMTOL=10 EM=1 NIONS=b M:15.9949,n:42.010567,STY:79.96633 MINPROB=0.5 MAXTHREADS=1 interact-NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.pep.xml interact-NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mod.pep.xml
            // ...
            // ProteinProphet [Work dir: D:\DMS_WorkDir1]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\Philosopher\philosopher-v5.1.1.exe proteinprophet --maxppmdiff 2000000 --minprob 0.5 --output combined D:\DMS_WorkDir1\filelist_proteinprophet.txt
            // PhilosopherDbAnnotate [Work dir: D:\DMS_WorkDir1\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\Philosopher\philosopher-v5.1.1.exe database --annotate D:\DMS_WorkDir1\ID_008380_0E5568A3.fasta --prefix XXX_
            // PhilosopherFilter [Work dir: D:\DMS_WorkDir1\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\Philosopher\philosopher-v5.1.1.exe filter --sequential --picked --prot 0.01 --tag XXX_ --pepxml D:\DMS_WorkDir1\NYBB_30_P01_P --protxml D:\DMS_WorkDir1\combined.prot.xml --razor
            // PhilosopherFilter [Work dir: D:\DMS_WorkDir1\NYBB_30_P02_P]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\Philosopher\philosopher-v5.1.1.exe filter --sequential --picked --prot 0.01 --tag XXX_ --pepxml D:\DMS_WorkDir1\NYBB_30_P02_P --dbbin D:\DMS_WorkDir1\NYBB_30_P01_P --protxml D:\DMS_WorkDir1\combined.prot.xml --probin D:\DMS_WorkDir1\NYBB_30_P01_P --razor
            // PhilosopherReport [Work dir: D:\DMS_WorkDir1\NYBB_30_P02_P]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\Philosopher\philosopher-v5.1.1.exe report
            // PhilosopherReport [Work dir: D:\DMS_WorkDir1\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\Philosopher\philosopher-v5.1.1.exe report
            // WorkspaceClean [Work dir: D:\DMS_WorkDir1\NYBB_30_P02_P]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\Philosopher\philosopher-v5.1.1.exe workspace --clean --nocheck
            // ...
            // IonQuant [Work dir: D:\DMS_WorkDir1]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\jre\bin\java.exe -Djava.awt.headless=true -Xmx60G -Dlibs.bruker.dir=C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\MSFragger-4.3\ext\bruker -Dlibs.thermo.dir=C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\MSFragger-4.3\ext\thermo -cp C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\jfreechart-1.5.3.jar;C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\IonQuant-1.11.11.jar ionquant.IonQuant --threads 23 --perform-ms1quant 1 --perform-isoquant 0 --isotol 20.0 --isolevel 2 --isotype tmt10 --ionmobility 0 --site-reports 0 --msstats 0 --minexps 1 --mbr 0 --maxlfq 0 --requantify 0 --mztol 10 --imtol 0.05 --rttol 1 --normalization 0 --minisotopes 1 --minscans 1 --writeindex 0 --tp 0 --minfreq 0 --minions 1 --locprob 0 --uniqueness 0 --multidir . --filelist D:\DMS_WorkDir1\filelist_ionquant.txt --modlist D:\DMS_WorkDir1\modmasses_ionquant.txt
            // IonQuant [Work dir: D:\DMS_WorkDir1]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\jre\bin\java.exe -Djava.awt.headless=true -Xmx60G -Dlibs.bruker.dir=C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\MSFragger-4.3\ext\bruker -Dlibs.thermo.dir=C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\MSFragger-4.3\ext\thermo -cp C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\jfreechart-1.5.3.jar;C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\IonQuant-1.11.11.jar ionquant.IonQuant --threads 23 --perform-ms1quant 0 --perform-isoquant 1 --isotol 20.0 --isolevel 2 --isotype TMT-16 --ionmobility 0 --site-reports 0 --msstats 0 --annotation D:\DMS_WorkDir1\NYBB_30_P01_P\psm.tsv=D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_annotation.txt --annotation D:\DMS_WorkDir1\NYBB_30_P02_P\psm.tsv=D:\DMS_WorkDir1\NYBB_30_P02_P\NYBB_30_P02_P_annotation.txt --minexps 1 --mbr 0 --maxlfq 0 --requantify 0 --mztol 10 --imtol 0.05 --rttol 1 --normalization 0 --minisotopes 1 --minscans 1 --writeindex 0 --tp 0 --minfreq 0 --minions 1 --locprob 0 --uniqueness 0 --multidir . --filelist D:\DMS_WorkDir1\filelist_ionquant.txt --modlist D:\DMS_WorkDir1\modmasses_ionquant.txt
            // TmtIntegrator [Work dir: D:\DMS_WorkDir1]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\jre\bin\java.exe -Xmx60G -jar C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\TMT-Integrator-6.1.1.jar D:\DMS_WorkDir1\tmt-integrator-conf.yml D:\DMS_WorkDir1\NYBB_30_P01_P\psm.tsv D:\DMS_WorkDir1\NYBB_30_P02_P\psm.tsv
            // ~~~~~~~~~~~~~~~~~~~~~~
            //
            // Execution order:
            //
            //     Cmd: [START], Work dir: [D:\DMS_WorkDir1]
            //     Cmd: [CheckCentroid], Work dir: [D:\DMS_WorkDir1]
            //     Cmd: [WorkspaceCleanInit], Work dir: [D:\DMS_WorkDir1\NYBB_30_P02_P]
            //     Cmd: [WorkspaceCleanInit], Work dir: [D:\DMS_WorkDir1\NYBB_30_P01_P]
            //     Cmd: [WorkspaceCleanInit], Work dir: [D:\DMS_WorkDir1]
            //     Cmd: [MSFragger], Work dir: [D:\DMS_WorkDir1]
            //     Cmd: [Percolator], Work dir: [D:\DMS_WorkDir1]
            //     Cmd: [PTMProphet], Work dir: [D:\DMS_WorkDir1]
            //     Cmd: [ProteinProphet], Work dir: [D:\DMS_WorkDir1]
            //     Cmd: [PhilosopherDbAnnotate], Work dir: [D:\DMS_WorkDir1]
            //     Cmd: [PhilosopherFilter], Work dir: [D:\DMS_WorkDir1]
            //     Cmd: [PhilosopherReport], Work dir: [D:\DMS_WorkDir1]
            //     Cmd: [WorkspaceClean], Work dir: [D:\DMS_WorkDir1\NYBB_30_P02_P]
            //     Cmd: [WorkspaceClean], Work dir: [D:\DMS_WorkDir1\NYBB_30_P01_P]
            //     Cmd: [WorkspaceClean], Work dir: [D:\DMS_WorkDir1]
            //     Cmd: [IonQuant], Work dir: [D:\DMS_WorkDir1]
            //     Cmd: [IonQuant], Work dir: [D:\DMS_WorkDir1]
            //     Cmd: [TmtIntegrator], Work dir: [D:\DMS_WorkDir1]
            //
            // ~~~~~~~~~~~~~~~~~~~~~~
            //
            // ~~~~~~Sample of D:\DMS_WorkDir1\ID_008380_0E5568A3.fasta~~~~~~~
            // >Contaminant_ALBU_BOVIN SERUM ALBUMIN PRECURSOR. (sp|P02769|ALBU_BOVIN, gi|113574)
            // >XXX_sp|O43741|AAKB2_HUMAN 5'-AMP-activated protein kinase subunit beta-2 OS=Homo sapiens OX=9606 GN=PRKAB2 PE=1 SV=1
            // >XXX_sp|P0DPD6|ECE2_HUMAN Endothelin-converting enzyme 2 OS=Homo sapiens OX=9606 GN=ECE2 PE=1 SV=1
            // ...
            // >XXX_sp|Q9Y6X6|MYO16_HUMAN Unconventional myosin-XVI OS=Homo sapiens OX=9606 GN=MYO16 PE=1 SV=3
            // >sp|O43776|SYNC_HUMAN Asparagine--tRNA ligase, cytoplasmic OS=Homo sapiens OX=9606 GN=NARS1 PE=1 SV=1
            // >sp|P0DPI2|GAL3A_HUMAN Glutamine amidotransferase-like class 1 domain-containing protein 3, mitochondrial OS=Homo sapiens OX=9606 GN=GATD3 PE=1 SV=1
            // ...
            // >sp|Q9NVI1|FANCI_HUMAN Fanconi anemia group I protein OS=Homo sapiens OX=9606 GN=FANCI PE=1 SV=4
            // >sp|S4R3Y5|HMN11_HUMAN Humanin-like 11 OS=Homo sapiens OX=9606 GN=MTRNR2L11 PE=3 SV=1
            // ~~~~~~~~~~~~~~~~~~~~~~
            //
            // ~~~~~~annotation files~~~~~~~
            // D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_annotation.txt:
            // 126 NYBB_30_P01_sample-01
            // 127N NYBB_30_P01_sample-02
            // 127C NYBB_30_P01_sample-03
            // 128N NYBB_30_P01_sample-04
            // 128C NYBB_30_P01_sample-05
            // 129N NYBB_30_P01_sample-06
            // 129C NYBB_30_P01_sample-07
            // 130N NYBB_30_P01_sample-08
            // 130C NYBB_30_P01_sample-09
            // 131N NYBB_30_P01_sample-10
            // 131C NYBB_30_P01_sample-11
            // 132N NYBB_30_P01_sample-12
            // 132C NYBB_30_P01_sample-13
            // 133N NYBB_30_P01_sample-14
            // 133C NYBB_30_P01_sample-15
            // 134N NYBB_30_P01_sample-16
            // D:\DMS_WorkDir1\NYBB_30_P02_P\NYBB_30_P02_P_annotation.txt:
            // 126 NYBB_30_P02_sample-01
            // 127N NYBB_30_P02_sample-02
            // ...
            // 133C NYBB_30_P02_sample-15
            // 134N NYBB_30_P02_sample-16
            // ~~~~~~~~~~~~~~~~~~~~~~
            //
            // ~~~~~~~~~ fragpipe.config ~~~~~~~~~
            // # FragPipe v23.0ui state cache
            //
            // ...
            //
            // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
            // CheckCentroid
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\jre\bin\java.exe -Xmx60G -cp C:\DMS_Programs\FragPipe\FragPipe_v23.0\lib\fragpipe-23.0.jar;C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\batmass-io-1.35.1.jar org.nesvilab.fragpipe.util.CheckCentroid D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML 23
            // Done in 2.8 s.
            // Process 'CheckCentroid' finished, exit code: 0
            // ...
            // MSFragger [Work dir: D:\DMS_WorkDir1]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\jre\bin\java.exe -jar -Dfile.encoding=UTF-8 -Xmx60G C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\MSFragger-4.3\MSFragger-4.3.jar D:\DMS_WorkDir1\fragger.params D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML D:\DMS_WorkDir1\NYBB_30_P02_P\NYBB_30_P02_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML D:\DMS_WorkDir1\NYBB_30_P02_P\NYBB_30_P02_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML D:\DMS_WorkDir1\NYBB_30_P02_P\NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML
            // MSFragger version MSFragger-4.2
            // Batmass-IO version 1.35.1
            // timsdata library version timsdata-2-21-0-4
            // (c) University of Michigan
            // RawFileReader reading tool. Copyright (c) 2016 by Thermo Fisher Scientific, Inc. All rights reserved.
            // timdTOF .d reading tool. Copyright (c) 2022 by Bruker Daltonics GmbH & Co. KG. All rights reserved.
            // System OS: Windows 10, Architecture: AMD64
            // Java Info: 17.0.10, OpenJDK 64-Bit Server VM, Eclipse Adoptium
            // JVM started with 60 GB memory
            // Checking database...
            // Checking spectral files...
            // NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML: Scans = 45846; MS2 ITMS = false; MS2 FTMS = true; MS2 ASTMS = false; MS1 ITMS = false; Isolation sizes = [0.7]; Instrument = Orbitrap Fusion Lumos
            // NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML: Scans = 45900; MS2 ITMS = false; MS2 FTMS = true; MS2 ASTMS = false; MS1 ITMS = false; Isolation sizes = [0.7]; Instrument = Orbitrap Fusion Lumos
            // NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML: Scans = 46689; MS2 ITMS = false; MS2 FTMS = true; MS2 ASTMS = false; MS1 ITMS = false; Isolation sizes = [0.7]; Instrument = Orbitrap Fusion Lumos
            // NYBB_30_P02_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML: Scans = 46797; MS2 ITMS = false; MS2 FTMS = true; MS2 ASTMS = false; MS1 ITMS = false; Isolation sizes = [0.7]; Instrument = Orbitrap Fusion Lumos
            // NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML: Scans = 46862; MS2 ITMS = false; MS2 FTMS = true; MS2 ASTMS = false; MS1 ITMS = false; Isolation sizes = [0.7]; Instrument = Orbitrap Fusion Lumos
            // NYBB_30_P02_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML: Scans = 49119; MS2 ITMS = false; MS2 FTMS = true; MS2 ASTMS = false; MS1 ITMS = false; Isolation sizes = [0.7]; Instrument = Orbitrap Fusion Lumos
            // ***********************************FIRST SEARCH************************************
            // Parameters:
            // ...
            // Number of unique peptides
            // 	of length 7: 287673
            // 	of length 8: 278840
            // 	...
            // 	of length 49: 2420
            // 	of length 50: 1475
            // In total 5004351 peptides.
            // Generated 84825409 modified peptides.
            // Number of peptides with more than 5000 modification patterns: 0
            // Selected fragment index width 0.10 Da.
            // 4755721800 fragments to be searched in 1 slices (44.29 GB total)
            // Operating on slice 1 of 1:
            // 	Fragment index slice generated in 37.04 s
            // 	001. NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML 2.5 s | deisotoping 1.6 s
            // 		[progress: 45846/45846 (100%) - 7742 spectra/s] 5.9s
            // 	002. NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML 1.7 s | deisotoping 0.2 s
            // 		[progress: 45900/45900 (100%) - 26109 spectra/s] 1.8s
            // 	003. NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML 1.7 s | deisotoping 0.2 s
            // 		[progress: 46689/46689 (100%) - 21348 spectra/s] 2.2s
            // 	004. NYBB_30_P02_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML 1.6 s | deisotoping 0.3 s
            // 		[progress: 46797/46797 (100%) - 25187 spectra/s] 1.9s
            // 	005. NYBB_30_P02_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML 1.9 s | deisotoping 0.5 s
            // 		[progress: 49119/49119 (100%) - 21393 spectra/s] 2.3s
            // 	006. NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML 1.6 s | deisotoping 0.3 s
            // 		[progress: 46862/46862 (100%) - 22551 spectra/s] 2.1s
            // postprocessing NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML 0.3 s
            // postprocessing NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML 0.2 s
            // postprocessing NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML 0.2 s
            // postprocessing NYBB_30_P02_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML 0.2 s
            // postprocessing NYBB_30_P02_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML 0.4 s
            // postprocessing NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML 0.3 s
            // ***************************FIRST SEARCH DONE IN 1.608 MIN**************************
            //
            // *********************MASS CALIBRATION AND PARAMETER OPTIMIZATION*******************
            // -----|---------------|---------------|---------------|---------------
            //      |  MS1   (Old)  |  MS1   (New)  |  MS2   (Old)  |  MS2   (New)
            // -----|---------------|---------------|---------------|---------------
            //  Run |  Median  MAD  |  Median  MAD  |  Median  MAD  |  Median  MAD
            //  001 |   3.56   1.68 |   0.06   0.97 |   1.37   1.53 |  -0.12   1.09
            //  002 |   3.53   1.46 |   0.07   1.00 |   1.63   1.48 |  -0.13   1.05
            //  003 |   3.69   1.38 |  -0.05   0.90 |   1.56   1.49 |  -0.09   1.03
            //  004 |   3.22   1.55 |   0.05   1.01 |   1.62   1.46 |  -0.11   1.04
            //  005 |   2.75   1.59 |   0.14   1.01 |   2.06   1.51 |  -0.08   1.08
            //  006 |   3.54   1.68 |   0.05   1.09 |   2.24   1.52 |  -0.06   1.05
            // -----|---------------|---------------|---------------|---------------
            // Finding the optimal parameters:
            // -------|-------|-------|-------|-------|-------|-------
            //   MS2  |    7  |   10  |   15  |   20  |   25  |   30
            // -------|-------|-------|-------|-------|-------|-------
            //  Count |   7034|   7017| skip rest
            // -------|-------|-------|-------|-------|-------|-------
            // -------|-------|-------|-------|-------
            //  Peaks | 300_0 | 200_0 | 150_1 | 100_1
            // -------|-------|-------|-------|-------
            //  Count |   7682|   7560| skip rest
            // -------|-------|-------|-------|-------
            // -------|-------
            //  Int.  |    1
            // -------|-------
            //  Count |   7483
            // -------|-------
            // -------|-------
            //  Rm P. |    0
            // -------|-------
            //  Count |   7679
            // -------|-------
            // New fragment_mass_tolerance = 7.000000 PPM
            // New use_topN_peaks = 300
            // New minimum_ratio = 0.000000
            // New intensity_transform = 0
            // New remove_precursor_peak = 1
            // ************MASS CALIBRATION AND PARAMETER OPTIMIZATION DONE IN 4.987 MIN*********
            //
            // ************************************MAIN SEARCH************************************
            // Checking database...
            // Parameters:
            // num_threads = 23
            // ...
            // Number of unique peptides
            // 	of length 7: 287673
            // 	of length 8: 278840
            // 	...
            // 	of length 49: 2420
            // 	of length 50: 1475
            // In total 5004351 peptides.
            // Generated 145075941 modified peptides.
            // Number of peptides with more than 5000 modification patterns: 28
            // Selected fragment index width 0.03 Da.
            // 8734427126 fragments to be searched in 2 slices (81.35 GB total)
            // Operating on slice 1 of 2:
            // 	Fragment index slice generated in 36.15 s
            // 	001. NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 0.5 s
            // 		[progress: 45846/45846 (100%) - 15530 spectra/s] 3.0s
            // 	002. NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 0.4 s
            // 		[progress: 45900/45900 (100%) - 12718 spectra/s] 3.6s
            // 	003. NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 0.3 s
            // 		[progress: 46689/46689 (100%) - 13764 spectra/s] 3.4s
            // 	004. NYBB_30_P02_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 0.3 s
            // 		[progress: 46797/46797 (100%) - 11563 spectra/s] 4.0s
            // 	005. NYBB_30_P02_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 0.4 s
            // 		[progress: 49119/49119 (100%) - 8796 spectra/s] 5.6s
            // 	006. NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 1.0 s
            // 		[progress: 46862/46862 (100%) - 9958 spectra/s] 4.7s
            // Operating on slice 2 of 2:
            // 	Fragment index slice generated in 30.04 s
            // 	001. NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 0.3 s
            // 		[progress: 45846/45846 (100%) - 29945 spectra/s] 1.5s
            // 	002. NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 0.3 s
            // 		[progress: 45900/45900 (100%) - 29273 spectra/s] 1.6s
            // 	003. NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 0.3 s
            // 		[progress: 46689/46689 (100%) - 35478 spectra/s] 1.3s
            // 	004. NYBB_30_P02_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 0.3 s
            // 		[progress: 46797/46797 (100%) - 23779 spectra/s] 2.0s
            // 	005. NYBB_30_P02_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 0.4 s
            // 		[progress: 49119/49119 (100%) - 21393 spectra/s] 2.3s
            // 	006. NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 0.4 s
            // 		[progress: 46862/46862 (100%) - 25208 spectra/s] 1.9s
            // postprocessing NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 4.5 s
            // postprocessing NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 4.4 s
            // postprocessing NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 3.7 s
            // postprocessing NYBB_30_P02_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 4.0 s
            // postprocessing NYBB_30_P02_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 4.7 s
            // postprocessing NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzBIN_calibrated 4.1 s
            // ***************************MAIN SEARCH DONE IN 2.606 MIN***************************
            //
            // *******************************TOTAL TIME 9.200 MIN********************************
            // Process 'MSFragger' finished, exit code: 0
            // Percolator [Work dir: D:\DMS_WorkDir1\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\percolator_3_7_1\windows\percolator.exe --only-psms --no-terminate --post-processing-tdc --num-threads 23 --results-psms NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19_percolator_target_psms.tsv --decoy-results-psms NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19_percolator_decoy_psms.tsv --protein-decoy-pattern XXX_ NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.pin
            // Percolator version 3.07.1, Build Date Jun 20 2024 13:21:08
            // ...
            // Calculating q values.
            // Final list yields 8454 target PSMs with q<0.01.
            // Calculating posterior error probabilities (PEPs).
            // Processing took 9.2150 cpu seconds or 9 seconds wall clock time.
            // Process 'Percolator' finished, exit code: 0
            // ...
            // Percolator: Convert to pepxml [Work dir: D:\DMS_WorkDir1\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\jre\bin\java.exe -cp C:\DMS_Programs\FragPipe\FragPipe_v23.0\lib/* org.nesvilab.fragpipe.tools.percolator.PercolatorOutputToPepXML NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.pin NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19 NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19_percolator_target_psms.tsv NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19_percolator_decoy_psms.tsv interact-NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19 DDA 0.5 D:\DMS_WorkDir1\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML
            // ...
            // PTMProphet [Work dir: D:\DMS_WorkDir1\NYBB_30_P02_P]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\PTMProphet\PTMProphetParser.exe NOSTACK KEEPOLD STATIC FRAGPPMTOL=10 EM=1 NIONS=b M:15.9949,n:42.010567,STY:79.96633 MINPROB=0.5 MAXTHREADS=1 interact-NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.pep.xml interact-NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mod.pep.xml
            // PTMProphet [Work dir: D:\DMS_WorkDir1\NYBB_30_P02_P]
            // ...
            // [INFO:] Writing file interact-NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mod.pep.xml ...
            // [INFO:] done ...
            // Process 'PTMProphet' finished, exit code: 0
            // [INFO:] Using statically set 10 PPM tolerance ...
            // ...
            // ProteinProphet [Work dir: D:\DMS_WorkDir1]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\Philosopher\philosopher-v5.1.1.exe proteinprophet --maxppmdiff 2000000 --minprob 0.5 --output combined D:\DMS_WorkDir1\filelist_proteinprophet.txt
            // time="20:54:27" level=info msg="Executing ProteinProphet  v5.1.1"
            // ProteinProphet (C++) by Insilicos LLC and LabKey Software, after the original Perl by A. Keller (TPP v6.0.0-rc15 Noctilucent, Build 202105101442-exported (Windows_NT-x86_64))
            //  (no FPKM) (no groups) (using degen pep info)
            // ...
            // Process 'ProteinProphet' finished, exit code: 0
            // PhilosopherDbAnnotate [Work dir: D:\DMS_WorkDir1\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\Philosopher\philosopher-v5.1.1.exe database --annotate D:\DMS_WorkDir1\ID_008380_0E5568A3.fasta --prefix XXX_
            // time="20:54:37" level=info msg="Executing Database  v5.1.1"
            // time="20:54:37" level=info msg="Annotating the database"
            // time="20:54:38" level=info msg=Done
            // Process 'PhilosopherDbAnnotate' finished, exit code: 0
            // PhilosopherFilter [Work dir: D:\DMS_WorkDir1\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\Philosopher\philosopher-v5.1.1.exe filter --sequential --picked --prot 0.01 --tag XXX_ --pepxml D:\DMS_WorkDir1\NYBB_30_P01_P --protxml D:\DMS_WorkDir1\combined.prot.xml --razor
            // ...
            // Process 'PhilosopherFilter' finished, exit code: 0
            // PhilosopherReport [Work dir: D:\DMS_WorkDir1\NYBB_30_P02_P]
            // ...
            // IonQuant [Work dir: D:\DMS_WorkDir1]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\jre\bin\java.exe -Djava.awt.headless=true -Xmx60G -Dlibs.bruker.dir=C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\MSFragger-4.3\ext\bruker -Dlibs.thermo.dir=C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\MSFragger-4.3\ext\thermo -cp C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\jfreechart-1.5.3.jar;C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\IonQuant-1.11.11.jar ionquant.IonQuant --threads 23 --perform-ms1quant 1 --perform-isoquant 0 --isotol 20.0 --isolevel 2 --isotype tmt10 --ionmobility 0 --site-reports 0 --msstats 0 --minexps 1 --mbr 0 --maxlfq 0 --requantify 0 --mztol 10 --imtol 0.05 --rttol 1 --normalization 0 --minisotopes 1 --minscans 1 --writeindex 0 --tp 0 --minfreq 0 --minions 1 --locprob 0 --uniqueness 0 --multidir . --filelist D:\DMS_WorkDir1\filelist_ionquant.txt --modlist D:\DMS_WorkDir1\modmasses_ionquant.txt
            // IonQuant version IonQuant-1.11.11
            // Batmass-IO version 1.35.1
            // timsdata library version timsdata-2-21-0-4
            // ...
            // Process 'IonQuant' finished, exit code: 0
            // TmtIntegrator [Work dir: D:\DMS_WorkDir1]
            // C:\DMS_Programs\FragPipe\FragPipe_v23.0\jre\bin\java.exe -Xmx60G -jar C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\TMT-Integrator-6.1.1.jar D:\DMS_WorkDir1\tmt-integrator-conf.yml D:\DMS_WorkDir1\NYBB_30_P01_P\psm.tsv D:\DMS_WorkDir1\NYBB_30_P02_P\psm.tsv
            // TMT Integrator 6.1.1
            // ...
            // Process 'TmtIntegrator' finished, exit code: 0
            //
            // Please cite:
            // ...
            //
            // Task Runtimes:
            //   CheckCentroid: 0.05 minutes
            //   WorkspaceCleanInit: 0.03 minutes
            //   MSFragger: 9.22 minutes
            //   Percolator: 0.71 minutes
            //   Percolator: Convert to pepxml: 0.23 minutes
            //   Percolator delete temp: 0.05 minutes
            //   PTMProphet: 5.47 minutes
            //   ProteinProphet: 0.16 minutes
            //   PhilosopherDbAnnotate: 0.01 minutes
            //   PhilosopherFilter: 0.34 minutes
            //   PhilosopherReport: 0.05 minutes
            //   WorkspaceClean: 0.01 minutes
            //   IonQuant: 1.01 minutes
            //   TmtIntegrator: 0.08 minutes
            //   Finalizer Task: 0.01 minutes
            //
            // =============================================================ALL JOBS DONE IN 17.5 MINUTES=============================================================

            // ReSharper restore CommentTypo

            const int CHECK_CENTROID = (int)ProgressPercentValues.StartingFragPipe + 1;

            const int FIRST_SEARCH_START = (int)ProgressPercentValues.StartingFragPipe + 3;
            const int FIRST_SEARCH_DONE = 24;

            const int MAIN_SEARCH_START = 30;
            const int MAIN_SEARCH_DONE = 50;

            const int FRAG_PIPE_COMPLETE = (int)ProgressPercentValues.FragPipeComplete;

            var processingSteps = new SortedList<int, Regex>
            {
                { CHECK_CENTROID          , GetRegEx("^CheckCentroid") },
                { CHECK_CENTROID + 1      , GetRegEx("^Checking spectral files") },
                { FIRST_SEARCH_START      , GetRegEx(@"^\*+FIRST SEARCH\*+") },
                { FIRST_SEARCH_DONE       , GetRegEx(@"^\*+FIRST SEARCH DONE") },
                { FIRST_SEARCH_DONE + 1   , GetRegEx(@"^\*+MASS CALIBRATION AND PARAMETER OPTIMIZATION\*+") },
                { MAIN_SEARCH_START       , GetRegEx(@"^\*+MAIN SEARCH\*+") },
                { MAIN_SEARCH_DONE        , GetRegEx(@"^\*+MAIN SEARCH DONE") },
                { MAIN_SEARCH_DONE + 1    , GetRegEx("^MSBooster[: ]") },
                { 53                      , GetRegEx("^Percolator[: ]") },
                { 55                      , GetRegEx("^PTMProphet[: ]") },
                { 60                      , GetRegEx("^ProteinProphet[: ]") },
                { 70                      , GetRegEx("^PhilosopherDbAnnotate[: ]") },
                { 75                      , GetRegEx("^PhilosopherFilter[: ]") },
                { 80                      , GetRegEx("^PhilosopherReport[: ]") },
                { 85                      , GetRegEx("^IonQuant[: ]") },
                { 90                      , GetRegEx("^TmtIntegrator[: ]") },
                { FRAG_PIPE_COMPLETE      , GetRegEx("^Please cite[: ]") }
            };

            var slabProgressRanges = new Dictionary<int, int>
            {
                {FIRST_SEARCH_START, FIRST_SEARCH_DONE}, // First Search progress range
                {MAIN_SEARCH_START, MAIN_SEARCH_DONE}    // Main Search progress range
            };

            // Use a linked list to keep track of the progress values
            // This makes lookup of the next progress value easier
            var progressValues = new LinkedList<int>();

            foreach (var item in (from progressValue in processingSteps.Keys orderby progressValue select progressValue))
            {
                progressValues.AddLast(item);
            }

            progressValues.AddLast(100);

            // RegEx to match lines like:
            //  001. Sample_Bane_06May21_20-11-16.mzML 1.0 s | deisotoping 0.6 s
            //	001. QC_Shew_20_01_R01_Bane_10Feb21_20-11-16.mzBIN_calibrated 0.1 s
            var datasetMatcher = new Regex(@"^[\t ]+(?<DatasetNumber>\d+)\. .+\.(mzML|mzBIN)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // RegEx to match lines like:
            // Operating on slice 1 of 2: 4463ms
            var sliceMatcher = new Regex(@"Operating on slice (?<Current>\d+) of (?<Total>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // RegEx to match lines like:
            // DatasetName.mzML 7042ms [progress: 29940/50420 (59.38%) - 5945.19 spectra/s]
            var progressMatcher = new Regex(@"progress: \d+/\d+ \((?<PercentComplete>[0-9.]+)%\)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var splitFastaMatcher = new Regex(@"^[\t ]*(?<Action>STARTED|DONE): slice (?<CurrentSplitFile>\d+) of (?<TotalSplitFiles>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            var linesRead = 0;

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

                if (mDebugLevel >= 4)
                {
                    LogDebug("Parsing file " + consoleOutputFilePath);
                }

                mConsoleOutputErrorMsg = string.Empty;
                var currentProgress = 0;

                var currentSlice = 0;
                var totalSlices = 0;

                var currentSplitFastaFile = 0;
                var splitFastaFileCount = 0;

                var currentDatasetId = 0;
                float datasetProgress = 0;

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                var executionOrderFound = false;
                var fragPipeConfigFound = false;
                var versionFound = false;
                var extractVersionInfo = string.IsNullOrEmpty(mFragPipeVersion);

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var trimmedLine = dataLine.Trim();

                    if (!versionFound)
                    {
                        if (trimmedLine.Trim().StartsWith("FragPipe version", StringComparison.OrdinalIgnoreCase))
                        {
                            versionFound = true;
                        }

                        if (versionFound && extractVersionInfo)
                        {
                            // Determine the versions of FragPipe, MSFragger, IonQuant, Philosopher, etc.

                            LogDebug(trimmedLine, mDebugLevel);
                            mFragPipeVersion = trimmedLine;

                            // The next few lines should have version numbers for additional programs, including MSFragger, IonQuant, and Philosopher

                            while (versionFound && !reader.EndOfStream && linesRead < 20)
                            {
                                var currentLine = reader.ReadLine()?.Trim();
                                linesRead++;

                                if (string.IsNullOrWhiteSpace(currentLine) || !currentLine.Contains(" version"))
                                    continue;

                                mFragPipeVersion = mFragPipeVersion.Length > 0
                                    ? string.Format("{0}; {1}", mFragPipeVersion, currentLine)
                                    : currentLine;
                            }

                            continue;
                        }
                    }

                    // Check for "Not enough memory allocated to MSFragger"
                    if (trimmedLine.StartsWith(FRAGPIPE_ERROR_INSUFFICIENT_MEMORY_ALLOCATED))
                    {
                        // This is a critical error; do not attempt to compute % complete
                        mConsoleOutputErrorMsg = "Error running FragPipe: " + dataLine;
                        break;
                    }

                    if (!executionOrderFound && trimmedLine.StartsWith("Execution order", StringComparison.OrdinalIgnoreCase))
                    {
                        executionOrderFound = true;
                    }

                    if (!fragPipeConfigFound && trimmedLine.IndexOf("fragpipe.config", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        fragPipeConfigFound = true;
                    }

                    // Do not look for processing step keywords until after either "Execution order" is shown or the FragPipe config is shown
                    if (!(executionOrderFound || fragPipeConfigFound))
                    {
                        continue;
                    }

                    foreach (var processingStep in processingSteps)
                    {
                        if (!processingStep.Value.IsMatch(trimmedLine))
                            continue;

                        if (processingStep.Key > currentProgress)
                        {
                            currentProgress = processingStep.Key;
                        }
                        else
                        {
                            Console.WriteLine("Not changing progress from {0} to {1} for console output line {2}",
                                currentProgress, processingStep.Key, trimmedLine);
                        }

                        if (currentProgress == MAIN_SEARCH_START)
                        {
                            // Reset slice tracking variables
                            currentSlice = 0;
                            totalSlices = 0;

                            currentDatasetId = 0;
                            datasetProgress = 0;
                        }

                        break;
                    }

                    var splitFastaProgressMatch = splitFastaMatcher.Match(trimmedLine);

                    if (splitFastaProgressMatch.Success &&
                        splitFastaProgressMatch.Groups["Action"].Value.Equals("STARTED", StringComparison.OrdinalIgnoreCase))
                    {
                        currentSplitFastaFile = int.Parse(splitFastaProgressMatch.Groups["CurrentSplitFile"].Value);

                        if (splitFastaFileCount == 0)
                        {
                            splitFastaFileCount = int.Parse(splitFastaProgressMatch.Groups["TotalSplitFiles"].Value);
                        }
                    }

                    var sliceMatch = sliceMatcher.Match(trimmedLine);

                    if (sliceMatch.Success)
                    {
                        currentSlice = int.Parse(sliceMatch.Groups["Current"].Value);
                        totalSlices = int.Parse(sliceMatch.Groups["Total"].Value);
                    }
                    else if (currentSlice > 0)
                    {
                        var datasetMatch = datasetMatcher.Match(trimmedLine);

                        if (datasetMatch.Success)
                        {
                            currentDatasetId = int.Parse(datasetMatch.Groups["DatasetNumber"].Value);
                        }

                        var progressMatch = progressMatcher.Match(trimmedLine);

                        if (progressMatch.Success)
                        {
                            datasetProgress = float.Parse(progressMatch.Groups["PercentComplete"].Value);
                        }
                    }

                    // Check whether the line starts with the text error
                    // Future: possibly adjust this check

                    if (currentProgress > 1 &&
                        trimmedLine.StartsWith("error", StringComparison.OrdinalIgnoreCase) &&
                        string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                    {
                        mConsoleOutputErrorMsg = "Error running FragPipe: " + dataLine;
                    }
                }

                float effectiveProgressOverall;

                var processSlab = slabProgressRanges.Any(item => currentProgress >= item.Key && currentProgress < item.Value);

                if (processSlab && totalSlices > 0)
                {
                    float currentProgressOnSlice;
                    float nextProgressOnSlice;

                    if (currentDatasetId > 0 && currentDatasetId > mDatasetCount)
                    {
                        if (!mWarnedInvalidDatasetCount)
                        {
                            if (mDatasetCount == 0)
                            {
                                LogWarning(
                                    "mDatasetCount is 0 in ParseFragPipeConsoleOutputFile; this indicates a programming bug. " +
                                    "Auto-updating dataset count to " + currentDatasetId);
                            }
                            else
                            {
                                LogWarning("CurrentDatasetId is greater than mDatasetCount in ParseFragPipeConsoleOutputFile; this indicates a programming bug. " +
                                           "Auto-updating dataset count from {0} to {1}", mDatasetCount, currentDatasetId);
                            }

                            mWarnedInvalidDatasetCount = true;
                        }

                        mDatasetCount = currentDatasetId;
                    }

                    if (currentDatasetId == 0 || mDatasetCount == 0)
                    {
                        currentProgressOnSlice = 0;
                        nextProgressOnSlice = 100;
                    }
                    else
                    {
                        currentProgressOnSlice = (currentDatasetId - 1) * (100f / mDatasetCount);
                        nextProgressOnSlice = currentDatasetId * (100f / mDatasetCount);
                    }

                    // First compute the effective progress for this slice
                    var sliceProgress = ComputeIncrementalProgress(currentProgressOnSlice, nextProgressOnSlice, datasetProgress);

                    // Next compute the progress processing each of the slices (which as a group can be considered a "slab")
                    var currentProgressOnSlab = (currentSlice - 1) * (100f / totalSlices);
                    var nextProgressOnSlab = currentSlice * (100f / totalSlices);

                    var slabProgress = ComputeIncrementalProgress(currentProgressOnSlab, nextProgressOnSlab, sliceProgress);

                    // Now compute the effective overall progress

                    var nextProgress = GetNextProgressValue(progressValues, currentProgress);

                    effectiveProgressOverall = ComputeIncrementalProgress(currentProgress, nextProgress, slabProgress);
                }
                else
                {
                    effectiveProgressOverall = currentProgress;
                }

                if (float.IsNaN(effectiveProgressOverall))
                {
                    return;
                }

                if (currentSplitFastaFile > 0 && splitFastaFileCount > 0)
                {
                    // Compute overall progress as 25 plus a value between 0 and 25, where 25 is MAIN_SEARCH_DONE / 2.0

                    var currentProgressOnSplitFasta = (currentSplitFastaFile - 1) * (MAIN_SEARCH_DONE / 2f / splitFastaFileCount);
                    var nextProgressOnSplitFasta = currentSplitFastaFile * (MAIN_SEARCH_DONE / 2f / splitFastaFileCount);

                    mProgress = 25 + ComputeIncrementalProgress(currentProgressOnSplitFasta, nextProgressOnSplitFasta, 25);
                    return;
                }

                if (effectiveProgressOverall < mProgress && mProgress > (int)ProgressPercentValues.StartingFragPipe)
                {
                    // Show a message if the new progress value is less than the current progress value
                    // This could be an indication that the progress computation logic needs to be updated
                    ConsoleMsgUtils.ShowDebug("The new progress value is less than the existing progress value: {0:F2} vs. {1:F2}", effectiveProgressOverall, mProgress);
                }

                mProgress = effectiveProgressOverall;
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate(string.Format(
                        "Error parsing the FragPipe console output file at line {0} ({1}): {2}",
                        linesRead,
                        consoleOutputFilePath, ex.Message));
                }
            }
        }

        private CloseOutType PostProcessResults(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            bool diaSearchEnabled,
            int databaseSplitCount)
        {
            try
            {
                // Create a tab-delimited text file listing the files in the working directory and all subdirectories
                // This file will only be retained if there is a problem during post-processing
                var fileInfoFilePath = CreateWorkingDirectoryFileInfo("Before_Post_Process");

                // If the FragPipe workflow file has "msfragger.write_calibrated_mzml=true" or "msfragger.write_uncalibrated_mgf=true",
                // FragPipe creates large output files that we don't want to keep (even if post-processing fails)
                mJobParams.AddResultFileExtensionToSkip("_calibrated.mzML");
                mJobParams.AddResultFileExtensionToSkip("_uncalibrated.mgf");

                // Move the plot files into each experiment group working directory
                MovePlotFiles();

                // Zip the .pepXML file(s) and .pin file(s)
                var zipSuccessPepXml = ZipPepXmlFiles(dataPackageInfo, datasetIDsByExperimentGroup, diaSearchEnabled, databaseSplitCount);

                if (!zipSuccessPepXml)
                    return CloseOutType.CLOSEOUT_FAILED;

                // Zip the combined protein prophet groups file
                var zipSuccessProteinProphetResults = ZipProteinProphetResultsFile();

                if (!zipSuccessProteinProphetResults)
                    return CloseOutType.CLOSEOUT_FAILED;

                var psmTsvFiles = mWorkingDirectory.GetFiles("psm.tsv", SearchOption.AllDirectories);
                var proteinTsvFiles = mWorkingDirectory.GetFiles("protein.tsv", SearchOption.AllDirectories);

                if (psmTsvFiles.Length > 0)
                {
                    var usedProteinProphet = proteinTsvFiles.Length > 0;

                    // Rename and update the report files created by Philosopher
                    var tsvFilesUpdated = UpdatePhilosopherReportFiles(usedProteinProphet);

                    if (!tsvFilesUpdated)
                        return CloseOutType.CLOSEOUT_FAILED;

                    // Zip the _ion.tsv, _peptide.tsv, _protein.tsv, and _psm.tsv files created for each experiment group,
                    // but only if there are more than three experiment groups
                    var zipSuccessPsmTsv = ZipPsmTsvFiles(usedProteinProphet);

                    if (!zipSuccessPsmTsv)
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                if (datasetIDsByExperimentGroup.Count == 1)
                {
                    var experimentGroupName = datasetIDsByExperimentGroup.FirstOrDefault().Key;
                    var experimentWorkingDirectory = mExperimentGroupWorkingDirectories[experimentGroupName];

                    var filesMoved = MoveResultsIntoDirectory(experimentWorkingDirectory, mWorkingDirectory, true);

                    if (!filesMoved)
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                // Skip additional files, including interact-Dataset.pep.xml and protein.fas
                mJobParams.AddResultFileExtensionToSkip(".pep.xml");
                mJobParams.AddResultFileExtensionToSkip("protein.fas");

                // Skip the FragPipe log file since file FragPipe_ConsoleOutput.txt should include the log file text
                var fragPipeLogFileMatcher = new Regex(@"log_\d{4}-", RegexOptions.Compiled);

                foreach (var logFile in mWorkingDirectory.GetFiles("log_*.txt"))
                {
                    if (fragPipeLogFileMatcher.IsMatch(logFile.Name))
                    {
                        // This is a file of the form log_2024-11-01_11-38-19.txt; skip it
                        mJobParams.AddResultFileToSkip(logFile.Name);
                    }
                }

                // Since the post-processing succeeded, we no longer need the file info file that lists working directory files prior to post-processing
                mJobParams.AddResultFileToSkip(Path.GetFileName(fileInfoFilePath));

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in PostProcessResults", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType StartFragPipe(
            FileInfo fastaFile,
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            out bool diaSearchEnabled,
            out int databaseSplitCount,
            out List<FileInfo> annotationFilesToSkip)
        {
            annotationFilesToSkip = new List<FileInfo>();

            try
            {
                // Create the manifest file
                var manifestCreated = CreateManifestFile(dataPackageInfo, datasetIDsByExperimentGroup, out var manifestFilePath, out diaSearchEnabled);

                if (!manifestCreated)
                {
                    databaseSplitCount = 0;
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!DetermineFragPipeToolLocations(out var fragPipePaths))
                {
                    databaseSplitCount = 0;
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Customize the path to the FASTA file
                var workflowResultCode = UpdateFragPipeWorkflowFile(fragPipePaths, out var workflowFilePath, out databaseSplitCount);

                if (workflowResultCode != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return workflowResultCode;
                }

                if (string.IsNullOrWhiteSpace(workflowFilePath))
                {
                    LogError("FragPipe workflow file name returned by UpdateFragPipeWorkflowFile is empty");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mDatasetCount = dataPackageInfo.DatasetFiles.Count;

                var options = new FragPipeOptions(mJobParams, mDatasetCount);
                RegisterEvents(options);

                options.LoadFragPipeOptions(workflowFilePath);

                var reporterIonModeValidated = UpdateReporterIonModeIfRequired(options);

                if (!reporterIonModeValidated)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // If reporter ions are defined, create annotation.txt files
                var annotationFileSuccess = CreateReporterIonAnnotationFiles(options, out var filesToSkip);
                annotationFilesToSkip.AddRange(filesToSkip);

                if (!annotationFileSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mWarnedInvalidDatasetCount = false;

                LogMessage("Running FragPipe");
                mProgress = (int)ProgressPercentValues.StartingFragPipe;

                ResetProgRunnerCpuUsage();

                var fragPipeBatchFile = new FileInfo(mFragPipeProgLoc);

                if (!fragPipeBatchFile.Exists)
                {
                    LogError(string.Format("FragPipe batch file not found: {0}", mFragPipeProgLoc));
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (fragPipeBatchFile.Directory == null)
                {
                    LogError(string.Format("FragPipe batch file parent directory is null: {0}", mFragPipeProgLoc));
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var consoleOutputFilePath = Path.Combine(mWorkDir, FRAGPIPE_CONSOLE_OUTPUT);

                // When running FragPipe, CacheStandardOutput needs to be false,
                // otherwise the program runner will randomly lock up, preventing FragPipe from finishing

                mCmdRunner = new RunDosProgram(fragPipeBatchFile.Directory.FullName, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = false,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = consoleOutputFilePath
                };

                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                double fastaFileSizeMB;

                if (databaseSplitCount > 1)
                {
                    fastaFileSizeMB = fastaFile.Length / 1024.0 / 1024 / databaseSplitCount;
                }
                else
                {
                    fastaFileSizeMB = fastaFile.Length / 1024.0 / 1024;
                }

                // Set up and execute a program runner to run FragPipe
                var processingSuccess = StartFragPipe(fragPipeBatchFile, fragPipePaths, fastaFileSizeMB, manifestFilePath, workflowFilePath, options);

                mCmdRunner.FlushConsoleOutputFileNow(true);

                if (!mToolVersionWritten)
                {
                    if (string.IsNullOrWhiteSpace(mFragPipeVersion))
                    {
                        ParseFragPipeConsoleOutputFile(consoleOutputFilePath);
                    }
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                if (!processingSuccess && string.IsNullOrWhiteSpace(mConsoleOutputErrorMsg))
                {
                    // Parse the console output file one more time to check for a new error message
                    ParseFragPipeConsoleOutputFile(consoleOutputFilePath);
                }

                if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LookForErrorsInFragPipeConsoleOutputFile(consoleOutputFilePath);
                }
                else
                {
                    LogError(mConsoleOutputErrorMsg);
                }

                if (!processingSuccess)
                {
                    if (!mMessage.Contains("Error running FragPipe"))
                    {
                        LogError("Error running FragPipe");
                    }

                    if (mCmdRunner.ExitCode != 0)
                    {
                        LogWarning("FragPipe returned a non-zero exit code: " + mCmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to FragPipe failed (but exit code is 0)");
                    }

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mStatusTools.UpdateAndWrite(mProgress);
                LogDebug("FragPipe Search Complete", mDebugLevel);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in StartFragPipe", ex);
                diaSearchEnabled = false;
                databaseSplitCount = 0;
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool StartFragPipe(
            FileInfo fragPipeBatchFile,
            FragPipeProgramPaths fragPipePaths,
            double fastaFileSizeMB,
            string manifestFilePath,
            string workflowFilePath,
            FragPipeOptions options)
        {
            // Larger FASTA files need more memory
            // Additional memory is also required as the number of dynamic mods being considered increases

            // 10 GB of memory was not sufficient for a 26 MB FASTA file, but 15 GB worked when using 2 dynamic mods

            var dynamicModCount = options.GetDynamicModResidueCount();

            var fragPipeMemorySizeGB = AnalysisResourcesFragPipe.GetFragPipeMemorySizeToUse(
                mJobParams,
                fastaFileSizeMB,
                dynamicModCount,
                options.EnzymaticTerminiCount,
                out var fragPipeMemorySizeMBJobParam,
                out var fixedFragPipeMemorySizeJobParam);

            // Construct a message similar to either of these
            // Allocating 20 GB to FragPipe for a 22 MB FASTA file and 2 dynamic mods
            // Allocating 20 GB to FragPipe (as defined by FragPipeMemorySizeMB in the settings file) for a 22 MB FASTA file and 2 dynamic mods

            var dynamicModCountDescription = FragPipeOptions.GetDynamicModCountDescription(dynamicModCount);

            string settingsFileComment;

            if (fixedFragPipeMemorySizeJobParam)
            {
                settingsFileComment = " (as defined by FragPipeMemorySizeMB and FixedFragPipeMemorySize=true in the settings file)";
            }
            else if (fragPipeMemorySizeGB > fragPipeMemorySizeMBJobParam / 1024.0)
            {
                settingsFileComment = string.Empty;
            }
            else
            {
                settingsFileComment = " (as defined by FragPipeMemorySizeMB in the settings file)";
            }

            var memoryAllocationMessage = string.Format(
                "Allocating {0:N0} GB to FragPipe{1} for a {2:N0} MB FASTA file and {3}",
                fragPipeMemorySizeGB, settingsFileComment, fastaFileSizeMB, dynamicModCountDescription);

            LogMessage(memoryAllocationMessage);

            mEvalMessage = Global.AppendToComment(mEvalMessage, memoryAllocationMessage);

            if (Global.RunningOnDeveloperComputer())
            {
                var freeMemoryMB = Global.GetFreeMemoryMB();

                if (fragPipeMemorySizeGB * 1024 > freeMemoryMB * 0.9)
                {
                    var memorySizeToUse = (int)Math.Floor(freeMemoryMB * 0.9 / 1024);

                    ConsoleMsgUtils.ShowWarning(
                        "Decreasing FragPipe memory size from {0:N0} GB to {1:N0} GB since running on developer machine and not enough free memory",
                        fragPipeMemorySizeGB, memorySizeToUse);

                    fragPipeMemorySizeGB = memorySizeToUse;
                    ConsoleMsgUtils.SleepSeconds(2);
                }
            }

            var numThreadsToUse = GetNumThreadsToUse();

            // ReSharper disable CommentTypo

            // Example command line:
            // fragpipe.bat --headless --ram 0 --threads 15 --workflow C:\DMS_WorkDir\FragPipe_TMT16-phospho_2024-09-09.workflow --manifest C:\DMS_WorkDir\datasets.fp-manifest --workdir C:\DMS_WorkDir --config-tools-folder C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools --config-diann C:\DMS_Programs\FragPipe\FragPipe_v23.0\tools\diann\1.8.2_beta_8\win\DiaNN.exe --config-python C:\Python3\python.exe

            // ReSharper restore CommentTypo

            var arguments = new StringBuilder();

            // ReSharper disable once StringLiteralTypo
            arguments.AppendFormat("--headless --ram {0} --threads {1}", fragPipeMemorySizeGB, numThreadsToUse);

            arguments.AppendFormat(" --workflow {0}", workflowFilePath);

            arguments.AppendFormat(" --manifest {0}", manifestFilePath);

            arguments.AppendFormat(" --workdir {0}", mWorkDir);
            arguments.AppendFormat(" --config-tools-folder {0}", fragPipePaths.ToolsDirectory.FullName);
            arguments.AppendFormat(" --config-diann {0}", fragPipePaths.DiannExe.FullName);
            arguments.AppendFormat(" --config-python {0}", fragPipePaths.PythonExe.FullName);

            if (fragPipeBatchFile.Directory == null)
            {
                LogError(string.Format("Unable to determine the parent directory of the FragPipe batch file: {0}", fragPipeBatchFile.FullName));
                return false;
            }

            LogDebug("cd {0}", fragPipeBatchFile.Directory.FullName);
            LogDebug(fragPipeBatchFile.Name + " " + arguments);

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            return mCmdRunner.RunProgram(fragPipeBatchFile.FullName, arguments.ToString(), "FragPipe", true);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            LogDebug("Determining tool version info", mDebugLevel);

            // fragpipe.exe should be in the same directory as fragpipe.bat
            var fragPipeExePath = mFragPipeProgLoc.Replace(".bat", ".exe");

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(fragPipeExePath)
            };

            try
            {
                // ReSharper disable once InvertIf

                // Add MSFragger, IonQuant, Philosopher, and TMT-Integrator to toolFiles:
                if (DetermineFragPipeToolLocations(out var toolsDirectory, out _))
                {
                    AddNewestMatchingFile(toolFiles, toolsDirectory, "MSFragger-*.jar");
                    AddNewestMatchingFile(toolFiles, toolsDirectory, "IonQuant-*.jar");
                    AddNewestMatchingFile(toolFiles, toolsDirectory, "Philosopher-*.exe");
                    AddNewestMatchingFile(toolFiles, toolsDirectory, "tmt-integrator-*.jar");
                }

                return SetStepTaskToolVersion(mFragPipeVersion, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Error calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        /// <summary>
        /// Update the FASTA file name defined in the FragPipe workflow file
        /// </summary>
        /// <param name="fragPipePaths">FragPipe tools directory, path to DiaNN.exe and path to python.exe</param>
        /// <param name="workflowFilePath">Output: workflow file path</param>
        /// <param name="databaseSplitCount">Output: database split count</param>
        private CloseOutType UpdateFragPipeWorkflowFile(FragPipeProgramPaths fragPipePaths, out string workflowFilePath, out int databaseSplitCount)
        {
            const string FASTA_FILE_COMMENT = "FASTA File (should include decoy proteins)";
            const string FILE_FORMAT_COMMENT = "File format of output files; Percolator uses .pin files";

            const string REQUIRED_OUTPUT_FORMAT = "tsv_pepxml_pin";

            const string DATABASE_PATH_PARAMETER = "database.db-path";
            const string DIANN_LIBRARY_PARAMETER = "diann.library";
            const string OUTPUT_FORMAT_PARAMETER = "msfragger.output_format";
            const string SLICE_DB_PARAMETER = "msfragger.misc.slice-db";

            try
            {
                // In this dictionary, keys are FragPipe parameter names and values are the corresponding file or directory path as defined for the current analysis job
                var fragPipeConfigParameters = new Dictionary<string, string>
                {
                    { "fragpipe-config.bin-diann", EscapeFragPipeWorkFlowParameterPath(fragPipePaths.DiannExe.FullName) },
                    { "fragpipe-config.bin-python", EscapeFragPipeWorkFlowParameterPath(fragPipePaths.PythonExe.FullName) },
                    { "fragpipe-config.tools-folder", EscapeFragPipeWorkFlowParameterPath(fragPipePaths.ToolsDirectory.FullName) }
                };

                var workflowFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);

                var sourceFile = new FileInfo(Path.Combine(mWorkDir, workflowFileName));
                var targetFile = new FileInfo(sourceFile.FullName + ".original");

                if (targetFile.Exists)
                {
                    // The .original file already exists; we're likely debugging the code
                    targetFile.Delete();
                }

                sourceFile.MoveTo(sourceFile.FullName + ".original");
                mJobParams.AddResultFileToSkip(sourceFile.Name);

                var updatedFile = new FileInfo(Path.Combine(mWorkDir, workflowFileName));

                databaseSplitCount = mJobParams.GetJobParameter(
                    AnalysisResourcesFragPipe.DATABASE_SPLIT_COUNT_SECTION,
                    AnalysisResourcesFragPipe.DATABASE_SPLIT_COUNT_PARAM,
                    1);

                var diannSpectralLibraryPath = mJobParams.GetJobParameter(
                    AnalysisResourcesFragPipe.DIANN_LIBRARY_SECTION,
                    AnalysisResourcesFragPipe.DIANN_LIBRARY_PARAM,
                    string.Empty);

                FileInfo localDiannSpectralLibraryFile;

                if (string.IsNullOrWhiteSpace(diannSpectralLibraryPath))
                {
                    localDiannSpectralLibraryFile = new FileInfo("NoLibraryDefined");
                }
                else
                {
                    var remoteDiannSpectralLibraryFile = new FileInfo(diannSpectralLibraryPath);
                    localDiannSpectralLibraryFile = new FileInfo(Path.Combine(mWorkDir, remoteDiannSpectralLibraryFile.Name));
                }

                var dbSplitCountDefined = false;
                var diannLibraryDefined = false;
                var fastaFileDefined = false;
                var outputFormatDefined = false;

                using (var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(updatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    var lineNumber = 0;

                    var fastaFileComment = string.Format("# {0}", FASTA_FILE_COMMENT);
                    var outputFormatComment = string.Format("# {0}", FILE_FORMAT_COMMENT);

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        lineNumber++;

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine();
                            continue;
                        }

                        var trimmedLine = dataLine.Trim();

                        if (trimmedLine.Equals(fastaFileComment) || trimmedLine.Equals(outputFormatComment))
                        {
                            // Line contains either comment "# FASTA File (should include decoy proteins)" or comment "# File format of output files; Percolator uses .pin files"
                            // Skip the line, since that same text will be auto added by this method
                            continue;
                        }

                        if (trimmedLine.StartsWith(DATABASE_PATH_PARAMETER))
                        {
                            if (fastaFileDefined)
                                continue;

                            var setting = new KeyValueParamFileLine(lineNumber, dataLine, true);
                            var comment = GetComment(setting, FASTA_FILE_COMMENT);

                            // On Windows, the path to the FASTA file must use '\\' for directory separators, instead of '\'

                            var pathToUse = EscapeFragPipeWorkFlowParameterPath(mFastaUtils.LocalFASTAFilePath);

                            WriteWorkflowFileSetting(writer, setting.ParamName, pathToUse, comment);

                            fastaFileDefined = true;
                            continue;
                        }

                        if (trimmedLine.StartsWith(DIANN_LIBRARY_PARAMETER))
                        {
                            if (diannLibraryDefined)
                                continue;

                            if (string.IsNullOrWhiteSpace(diannSpectralLibraryPath))
                            {
                                writer.WriteLine("{0}=", DIANN_LIBRARY_PARAMETER);
                                diannLibraryDefined = true;
                                continue;
                            }

                            var setting = new KeyValueParamFileLine(lineNumber, dataLine, true);
                            var comment = GetComment(setting, string.Empty);

                            // On Windows, the path to the spectral library file must use '\\' for directory separators, instead of '\'

                            var pathToUse = EscapeFragPipeWorkFlowParameterPath(localDiannSpectralLibraryFile.FullName);

                            WriteWorkflowFileSetting(writer, setting.ParamName, pathToUse, comment);

                            diannLibraryDefined = true;
                            continue;
                        }

                        if (trimmedLine.StartsWith(OUTPUT_FORMAT_PARAMETER))
                        {
                            if (outputFormatDefined)
                                continue;

                            var setting = new KeyValueParamFileLine(lineNumber, dataLine, true);
                            var comment = GetComment(setting, FILE_FORMAT_COMMENT);

                            if (setting.ParamValue.Equals(REQUIRED_OUTPUT_FORMAT, StringComparison.OrdinalIgnoreCase))
                            {
                                WriteWorkflowFileSetting(writer, setting.ParamName, setting.ParamValue, comment);
                            }
                            else
                            {
                                // Auto-change the output format to tsv_pepxml_pin
                                WriteWorkflowFileSetting(writer, setting.ParamName, REQUIRED_OUTPUT_FORMAT, comment);

                                LogWarning("Auto-updated the MSFragger output format from {0} to {1} because Percolator requires .pin files", setting.ParamValue, REQUIRED_OUTPUT_FORMAT);
                            }

                            outputFormatDefined = true;
                            continue;
                        }

                        if (trimmedLine.StartsWith(SLICE_DB_PARAMETER))
                        {
                            if (dbSplitCountDefined)
                                continue;

                            writer.WriteLine("{0}={1}", SLICE_DB_PARAMETER, Math.Max(1, databaseSplitCount));

                            dbSplitCountDefined = true;
                            continue;
                        }

                        var commentedConfigParamWritten = false;

                        foreach (var param in fragPipeConfigParameters)
                        {
                            if (!trimmedLine.Contains(param.Key))
                                continue;

                            // Write a commented version of this parameter, using the file or directory path tracked in fragPipeConfigParameters
                            writer.WriteLine("# {0}={1}", param.Key, param.Value);
                            commentedConfigParamWritten = true;
                            break;
                        }

                        if (commentedConfigParamWritten)
                            continue;

                        writer.WriteLine(dataLine);
                    }

                    if (!fastaFileDefined)
                    {
                        WriteWorkflowFileSetting(writer, DATABASE_PATH_PARAMETER, mFastaUtils.LocalFASTAFilePath, FASTA_FILE_COMMENT);
                    }

                    if (!outputFormatDefined)
                    {
                        WriteWorkflowFileSetting(writer, OUTPUT_FORMAT_PARAMETER, REQUIRED_OUTPUT_FORMAT, FILE_FORMAT_COMMENT);
                    }

                    if (!diannLibraryDefined && !string.IsNullOrWhiteSpace(diannSpectralLibraryPath))
                    {
                        writer.WriteLine("{0}={1}", DIANN_LIBRARY_PARAMETER, EscapeFragPipeWorkFlowParameterPath(localDiannSpectralLibraryFile.FullName));
                    }
                }

                workflowFilePath = updatedFile.FullName;

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error updating the FragPipe workflow file", ex);
                workflowFilePath = string.Empty;
                databaseSplitCount = 1;
                return CloseOutType.CLOSEOUT_FAILED;
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

        private bool UpdateReporterIonModeIfRequired(FragPipeOptions options)
        {
            try
            {
                // Keys in this dictionary are dataset IDs, values are experiment names
                var experimentNames = ExtractPackedJobParameterList(AnalysisResourcesFragPipe.JOB_PARAM_DICTIONARY_EXPERIMENTS_BY_DATASET_ID);

                if (experimentNames.Count == 0)
                {
                    LogWarning("Packed job parameter {0} is missing or empty; this is unexpected and likely a bug",
                        AnalysisResourcesFragPipe.JOB_PARAM_DICTIONARY_EXPERIMENTS_BY_DATASET_ID);

                    var experiment = mJobParams.GetJobParameter("Experiment", string.Empty);

                    experimentNames.Add(experiment);
                }

                // SQL Server: Data Source=Gigasax;Initial Catalog=DMS5
                // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
                var dmsConnectionString = mMgrParams.GetParam("ConnectionString");

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(dmsConnectionString, mMgrName);

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
                RegisterEvents(dbTools);

                var success = ReporterIonInfo.GetReporterIonModeForExperiments(
                    dbTools, experimentNames, options.ReporterIonMode,
                    out var message, out var reporterIonModeToUse);

                if (!success)
                {
                    LogError(message);
                    return false;
                }

                if (options.ReporterIonMode == reporterIonModeToUse)
                    return true;

                LogMessage(message);
                options.ReporterIonMode = reporterIonModeToUse;

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in UpdateReporterIonModeIfRequired", ex);
                return false;
            }
        }

        private bool ValidateFastaFile(out FileInfo fastaFile)
        {
            return mFastaUtils.ValidateFragPipeFastaFile(out fastaFile);
        }

        private void WriteWorkflowFileSetting(TextWriter writer, string paramName, string paramValue, string comment)
        {
            if (!string.IsNullOrWhiteSpace(comment))
            {
                if (comment.Trim().StartsWith("#"))
                {
                    writer.WriteLine(comment.Trim());
                }
                else
                {
                    writer.WriteLine("# {0}", comment.Trim());
                }
            }

            writer.WriteLine("{0}={1}", paramName, paramValue);
        }

        /// <summary>
        /// Store the list of files in a zip file (overwriting any existing zip file), then call AddResultFileToSkip() for each file
        /// </summary>
        /// <param name="fileListDescription">File list description</param>
        /// <param name="filesToZip">Files to zip</param>
        /// <param name="zipFileName">Zip file name</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ZipFiles(string fileListDescription, IReadOnlyList<FileInfo> filesToZip, string zipFileName)
        {
            var zipFilePath = Path.Combine(mWorkingDirectory.FullName, zipFileName);

            var success = mZipTools.ZipFiles(filesToZip, zipFilePath);

            if (!success)
            {
                LogError("Error zipping " + fileListDescription + " to create " + zipFileName);
                return false;
            }

            foreach (var item in filesToZip)
            {
                mJobParams.AddResultFileToSkip(item.Name);
            }

            return true;
        }

        /// <summary>
        /// Zip the _ion.tsv, _peptide.tsv, _protein.tsv, and _psm.tsv files created for each experiment group,
        /// but only if there are more than three experiment groups
        /// </summary>
        /// <param name="usedProteinProphet">True if Protein Prophet was used</param>
        /// <returns>True if successful, false if error</returns>
        private bool ZipPsmTsvFiles(bool usedProteinProphet)
        {
            try
            {
                if (mExperimentGroupWorkingDirectories.Count <= 3)
                {
                    // There are three or fewer experiment groups; zipping is not required
                    return true;
                }

                var ionFiles = mWorkingDirectory.GetFiles("*_ion.tsv");
                var peptideFiles = mWorkingDirectory.GetFiles("*_peptide.tsv");
                var proteinFiles = mWorkingDirectory.GetFiles("*_protein.tsv");
                var psmFiles = mWorkingDirectory.GetFiles("*_psm.tsv");

                if (ionFiles.Length + peptideFiles.Length + proteinFiles.Length + psmFiles.Length <= 8)
                {
                    // There likely is just one of each type of TSV file; zipping is not required
                    return true;
                }

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
                    {
                        filesToZip.Add(ionFile);
                    }
                    else
                    {
                        LogError("File not found: " + ionFile.Name);
                    }

                    if (peptideFile.Exists)
                    {
                        filesToZip.Add(peptideFile);
                    }
                    else
                    {
                        LogError("File not found: " + peptideFile.Name);
                    }

                    if (proteinFile.Exists)
                    {
                        filesToZip.Add(proteinFile);
                    }
                    else if (usedProteinProphet)
                    {
                        LogWarning("File not found: " + proteinFile.Name);
                    }

                    if (psmFile.Exists)
                    {
                        filesToZip.Add(psmFile);
                    }
                    else
                    {
                        LogError("File not found: " + psmFile.Name);
                    }

                    if (ionFile.Exists && peptideFile.Exists && psmFile.Exists)
                    {
                        validExperimentGroupCount++;
                    }
                }

                if (validExperimentGroupCount <= 3)
                {
                    return true;
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
        /// Zip the .pepXML file(s) and .pin file(s) created by FragPipe
        /// </summary>
        /// <param name="dataPackageInfo">Data package info</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="pepXmlFiles">Typically this is a single .pepXML file, but for DIA searches, this is a set of .pepXML files</param>
        /// <param name="addPinFile">When true, add the .pin file to the first zipped .pepXML file</param>
        /// <returns>True if success, false if an error</returns>
        private bool ZipPepXmlAndPinFiles(
            DataPackageInfo dataPackageInfo,
            string datasetName,
            List<FileInfo> pepXmlFiles,
            bool addPinFile)
        {
            if (pepXmlFiles.Count == 0)
            {
                LogError("Empty file list sent to method ZipPepXmlAndPinFiles");
                return false;
            }

            var primaryPepXmlFile = new List<FileInfo>();
            var additionalPepXmlFiles = new List<FileInfo>();

            if (pepXmlFiles.Count == 1)
            {
                primaryPepXmlFile.Add(pepXmlFiles[0]);
            }
            else
            {
                // Determine the .pepXML file to store in zip file DatasetName_pepXML.zip
                // Look for the _rank1.pepXML file in pepXmlFiles
                // If not found, just use pepXmlFiles[0]

                foreach (var item in pepXmlFiles)
                {
                    if (primaryPepXmlFile.Count == 0 && item.Name.EndsWith("_rank1.pepXML", StringComparison.OrdinalIgnoreCase))
                        primaryPepXmlFile.Add(item);
                    else
                        additionalPepXmlFiles.Add(item);
                }

                if (primaryPepXmlFile.Count == 0)
                    primaryPepXmlFile.Add(pepXmlFiles[0]);
            }

            if (primaryPepXmlFile.Count == 0)
                return false;

            if (primaryPepXmlFile[0].Length == 0)
            {
                string optionalDatasetInfo;

                if (dataPackageInfo.Datasets.Count > 0)
                {
                    optionalDatasetInfo = " for dataset " + datasetName;
                }
                else
                {
                    optionalDatasetInfo = string.Empty;
                }

                // pepXML file created by FragPipe is empty for dataset
                LogError("pepXML file created by FragPipe is empty{0}", optionalDatasetInfo);
            }

            var success = ZipPepXmlAndPinFile(datasetName, primaryPepXmlFile[0], addPinFile);

            if (!success)
                return false;

            if (additionalPepXmlFiles.Count == 0)
                return true;

            var successCount = 0;

            foreach (var pepXmlFile in additionalPepXmlFiles)
            {
                var zipFileNameOverride = string.Format("{0}_pepXML.zip", Path.GetFileNameWithoutExtension(pepXmlFile.Name));

                var success2 = ZipPepXmlAndPinFile(datasetName, pepXmlFile, false, zipFileNameOverride);

                if (success2)
                    successCount++;
            }

            if (successCount == additionalPepXmlFiles.Count)
                return true;

            LogError("Zip failure for {0} / {1} .pepXML files created by FragPipe", additionalPepXmlFiles.Count - successCount, additionalPepXmlFiles.Count);
            return false;
        }

        // ReSharper disable once CommentTypo

        /// <summary>
        /// Zip the .pepXML file created by FragPipe, storing it in the working directory (not the experiment working directory)
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="pepXmlFile">.pepXML file</param>
        /// <param name="addPinFile">If true, add this dataset's .pin file to the .zip file</param>
        /// <param name="zipFileNameOverride">If an empty string, name the .zip file DatasetName_pepXML.zip; otherwise, use this name</param>
        /// <returns>True if success, false if an error</returns>
        private bool ZipPepXmlAndPinFile(string datasetName, FileInfo pepXmlFile, bool addPinFile, string zipFileNameOverride = "")
        {
            // Instantiate the ZipFileTools instance if it is null
            mZipTool ??= new ZipFileTools(DebugLevel, WorkingDirectory);

            var zipSuccess = ZipOutputFile(pepXmlFile, ".pepXML file");

            if (!zipSuccess)
            {
                return false;
            }

            // Rename the zipped file
            var zipFile = new FileInfo(Path.ChangeExtension(pepXmlFile.FullName, ".zip"));

            if (!zipFile.Exists)
            {
                LogError("Zipped pepXML file not found; cannot rename");
                return false;
            }

            if (string.IsNullOrWhiteSpace(zipFileNameOverride))
            {
                zipFileNameOverride = datasetName + "_pepXML.zip";
            }

            var newZipFilePath = Path.Combine(WorkingDirectory, zipFileNameOverride);

            var existingTargetFile = new FileInfo(newZipFilePath);

            if (existingTargetFile.Exists)
            {
                LogMessage("Replacing {0} with updated version", existingTargetFile.Name);
                existingTargetFile.Delete();
            }

            zipFile.MoveTo(newZipFilePath);

            if (!addPinFile)
                return true;

            if (pepXmlFile.Directory == null)
            {
                LogWarning("Parent directory of file {0} is null; cannot search for .pin files", pepXmlFile.Name);
                return true;
            }

            // Add the Dataset_edited.pin file to the zip file
            // If not found, but Dataset.pin exists, add it

            FindDatasetPinFiles(pepXmlFile.Directory.FullName, datasetName, out var pinFile, out var pinFileEdited);

            if (!pinFile.Exists && !pinFileEdited.Exists)
            {
                LogError(".pin file not found; cannot add: " + pinFile.Name);
                return false;
            }

            FileInfo pinFileToUse;

            if (pinFileEdited.Exists)
            {
                pinFileToUse = pinFileEdited;
                mJobParams.AddResultFileToSkip(pinFile.Name);
            }
            else
            {
                pinFileToUse = pinFile;
                mJobParams.AddResultFileToSkip(pinFileEdited.Name);
            }

            var success = mZipTool.AddToZipFile(zipFile.FullName, pinFileToUse);

            if (success)
            {
                return true;
            }

            LogError("Error adding {0} to {1}", pinFileToUse.Name, zipFile.FullName);
            return false;
        }

        private bool ZipPepXmlFiles(
            DataPackageInfo dataPackageInfo,
            SortedDictionary<string, SortedSet<int>> datasetIDsByExperimentGroup,
            bool diaSearchEnabled,
            int databaseSplitCount)
        {
            var datasetCount = 0;

            try
            {
                var successCount = 0;

                // Validate that FragPipe created a .pepXML file for each dataset
                // For DIA data, the program creates several .pepXML files

                // If databaseSplitCount is 1, there should also be a .tsv file and a .pin file for each dataset (though with DIA data there might only be a .pin file and not a .tsv file)
                // If databaseSplitCount is more than 1, we will create a .tsv file using the data in the .pepXML file

                // Zip each .pepXML file
                foreach (var experimentGroup in datasetIDsByExperimentGroup)
                {
                    var experimentGroupName = experimentGroup.Key;

                    // For DIA searches that use a spectral library, the results might have been created in the working directory
                    // and not in the experiment group working directory; check for this

                    var experimentWorkingDirectory = GetExperimentGroupWorkingDirectoryToUse(
                        dataPackageInfo,
                        experimentGroup,
                        mExperimentGroupWorkingDirectories[experimentGroupName],
                        diaSearchEnabled);

                    foreach (var datasetID in experimentGroup.Value)
                    {
                        var datasetName = dataPackageInfo.Datasets[datasetID];
                        datasetCount++;
                        Console.WriteLine();

                        string optionalDatasetInfo;

                        if (dataPackageInfo.Datasets.Count > 0)
                        {
                            optionalDatasetInfo = " for dataset " + datasetName;
                        }
                        else
                        {
                            optionalDatasetInfo = string.Empty;
                        }

                        var pepXmlFiles = FindDatasetPinFileAndPepXmlFiles(experimentWorkingDirectory, diaSearchEnabled, datasetName, out var pinFile);

                        if (pepXmlFiles.Count == 0)
                        {
                            if (mMessage.IndexOf(FRAGPIPE_ERROR_INSUFFICIENT_MEMORY_ALLOCATED, StringComparison.OrdinalIgnoreCase) >= 0 ||
                                mMessage.IndexOf(FRAGPIPE_ERROR_INSUFFICIENT_MEMORY_FOR_JAVA, StringComparison.OrdinalIgnoreCase) >= 0 ||
                                mMessage.IndexOf(FRAGPIPE_ERROR_OUT_OF_MEMORY, StringComparison.OrdinalIgnoreCase) >= 0)
                            {
                                // The error message already has "Not enough memory allocated to MSFragger", "insufficient memory for Java" or "java.lang.OutOfMemoryError"
                                // There is no need to mention a missing .pepXML file
                                return false;
                            }

                            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                            if (diaSearchEnabled)
                            {
                                // FragPipe did not create any .pepXML files
                                LogError(string.Format("FragPipe did not create any .pepXML files{0}", optionalDatasetInfo));
                            }
                            else
                            {
                                // FragPipe did not create a .pepXML file for dataset
                                LogError(string.Format("FragPipe did not create a .pepXML file{0}", optionalDatasetInfo));
                            }

                            // Treat this as a fatal error
                            return false;
                        }

                        var tsvFile = new FileInfo(Path.Combine(experimentWorkingDirectory.FullName, datasetName + ".tsv"));

                        var splitFastaSearch = databaseSplitCount > 1;

                        if (!diaSearchEnabled && !tsvFile.Exists && !splitFastaSearch)
                        {
                            LogError(string.Format("FragPipe did not create a .tsv file{0}", optionalDatasetInfo));

                            // ToDo: create a .tsv file using the .pepXML file
                        }

                        if (!pinFile.Exists && !splitFastaSearch)
                        {
                            LogError(string.Format("FragPipe did not create a .pin file{0}", optionalDatasetInfo));
                        }

                        var zipSuccessPepXml = ZipPepXmlAndPinFiles(dataPackageInfo, datasetName, pepXmlFiles, pinFile.Exists);

                        if (!zipSuccessPepXml)
                            continue;

                        successCount++;

                        if (successCount > 1)
                            continue;

                        mJobParams.AddResultFileExtensionToSkip(PEPXML_EXTENSION);
                        mJobParams.AddResultFileExtensionToSkip(PIN_EXTENSION);
                    }
                }

                if (datasetCount != dataPackageInfo.Datasets.Count)
                {
                    LogWarning("Dataset count differs from dataPackageInfo.Datasets.Count: {0} vs. {1}", datasetCount, dataPackageInfo.Datasets.Count);
                }

                return successCount == datasetCount;
            }
            catch (Exception ex)
            {
                LogError("Error in ZipPepXmlFiles", ex);
                return false;
            }
        }

        private bool ZipProteinProphetResultsFile()
        {
            try
            {
                // Look for the Protein Prophet results file
                var proteinGroupsFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, PROTEIN_PROPHET_RESULTS_FILE));

                if (!proteinGroupsFile.Exists)
                {
                    LogWarning("Protein prophet results file not found: {0}", proteinGroupsFile.FullName);
                    return true;
                }

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
                LogError("Error in ZipProteinProphetResultsFile", ex);
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

            mLastConsoleOutputParse = DateTime.UtcNow;

            ParseFragPipeConsoleOutputFile(Path.Combine(mWorkDir, FRAGPIPE_CONSOLE_OUTPUT));

            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mFragPipeVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("FragPipe");
        }

        private void FastaUtilsErrorEventHandler(string message, Exception ex)
        {
            LogError(message, ex, true);
        }
    }
}
