//*********************************************************************************************************
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
using AnalysisManagerMSFraggerPlugIn;
using AnalysisManagerPepProtProphetPlugIn;
using PRISM;
using PRISM.AppSettings;
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

        private const string ANNOTATION_FILE_SUFFIX = "_annotation.txt";

        private const string FRAGPIPE_INSTANCE_DIRECTORY = "fragpipe_v22.0";

        private const string FRAGPIPE_BATCH_FILE_PATH = FRAGPIPE_INSTANCE_DIRECTORY + @"\bin\fragpipe.bat";

        private const string FRAGPIPE_TOOLS_DIRECTORY_PATH = FRAGPIPE_INSTANCE_DIRECTORY + @"\tools";

        private const string FRAGPIPE_DIANN_FILE_PATH = FRAGPIPE_INSTANCE_DIRECTORY + @"\tools\diann\1.8.2_beta_8\win\DiaNN.exe";

        private const string FRAGPIPE_CONSOLE_OUTPUT = "FragPipe_ConsoleOutput.txt";

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
        /// Path to fragpipe.bat, e.g. C:\DMS_Programs\FragPipe\fragpipe_v22.0\bin\fragpipe.bat
        /// </summary>
        private string mFragPipeProgLoc;

        private string mLocalFASTAFilePath;

        private DateTime mLastConsoleOutputParse;

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

                mExperimentGroupWorkingDirectories = new Dictionary<string, DirectoryInfo>();

                mWorkingDirectory = new DirectoryInfo(mWorkDir);

                // Determine the path to FragPipe

                // ReSharper disable once CommentTypo
                // Construct the relative path to the FragPipe batch file, for example:
                // fragpipe_v22.0\bin\fragpipe.bat

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
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var organizeResult = OrganizeFilesForFragPipe(out var dataPackageInfo, out var datasetIDsByExperimentGroup);

                if (organizeResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return organizeResult;
                }

                // Process the mzML files using FragPipe
                var processingResult = StartFragPipe(fastaFile, dataPackageInfo, datasetIDsByExperimentGroup, out var diaSearchEnabled, out var databaseSplitCount);

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
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
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
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_MZML_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory();
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

                    // If there is only one experiment group, leave the .mzML files in the working directory
                    // If multiple experiment groups, method MoveDatasetsIntoSubdirectories will move the .mzML files into the experiment group subdirectory

                    var datasetFileDirectory = datasetIDsByExperimentGroup.Count == 1
                        ? mWorkDir
                        : experimentWorkingDirectory.FullName;

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
        private bool CreateReporterIonAnnotationFiles(FragPipeOptions options)
        {
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
                        ? string.Format("annotation_{0}_", groupNumber)
                        : experimentGroup.Key + "_";

                    var experimentSpecificAliasFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, string.Format("{0}{1}", aliasNamePrefix, ANNOTATION_FILE_SUFFIX)));
                    var genericAliasFile = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "AliasNames.txt"));
                    var genericAliasFile2 = new FileInfo(Path.Combine(mWorkingDirectory.FullName, "AliasName.txt"));

                    if (experimentSpecificAliasFile.Exists || genericAliasFile.Exists || genericAliasFile2.Exists)
                    {
                        FileInfo sourceAnnotationFile;

                        if (experimentSpecificAliasFile.Exists)
                        {
                            // Copy the file into the experiment group working directory
                            sourceAnnotationFile = experimentSpecificAliasFile;
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

                        var targetFile = new FileInfo(Path.Combine(experimentGroup.Value.FullName, experimentSpecificAliasFile.Name));

                        sourceAnnotationFile.CopyTo(targetFile.FullName);
                        continue;
                    }

                    LogMessage(
                        "{0} alias file not found; will auto-generate file {1} for use by TMT Integrator",
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

                    var annotationFile = CreateReporterIonAnnotationFile(options.ReporterIonMode, experimentSpecificAliasFile, prefixToUse);

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
        /// <param name="toolsDirectory">Output: path to the tools directory below the FragPipe instance directory (FRAGPIPE_INSTANCE_DIRECTORY)</param>
        /// <param name="fragPipeProgLoc">Output: path to the FragPipe directory below DMS_Programs</param>
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
        /// <param name="toolsDirectory">Output: path to the tools directory below the FragPipe instance directory (FRAGPIPE_INSTANCE_DIRECTORY)</param>
        /// <param name="diannExe">Output: path to DiaNN.exe</param>
        /// <param name="pythonExe">Output: path to python.exe</param>
        /// <returns>True if the directory was found, false if missing or an error</returns>
        private bool DetermineFragPipeToolLocations(out DirectoryInfo toolsDirectory, out FileInfo diannExe, out FileInfo pythonExe)
        {
            try
            {
                if (!DetermineFragPipeToolLocations(out toolsDirectory, out var fragPipeProgLoc))
                {
                    diannExe = null;
                    pythonExe = null;
                    return false;
                }

                diannExe = new FileInfo(Path.Combine(fragPipeProgLoc, FRAGPIPE_DIANN_FILE_PATH));

                if (!diannExe.Exists)
                {
                    LogError("Cannot find the DiaNN executable: " + diannExe.FullName, true);

                    pythonExe = null;
                    return false;
                }

                // Verify that Python.exe exists
                // Python3ProgLoc will be something like this: "C:\Python3"
                var pythonProgLoc = mMgrParams.GetParam("Python3ProgLoc");

                if (!Directory.Exists(pythonProgLoc))
                {
                    if (pythonProgLoc.Length == 0)
                    {
                        LogError("Parameter 'Python3ProgLoc' not defined for this manager", true);
                    }
                    else
                    {
                        LogError("The Python directory does not exist: " + pythonProgLoc, true);
                    }

                    pythonExe = null;
                    return false;
                }

                pythonExe = new FileInfo(Path.Combine(pythonProgLoc, "python.exe"));

                if (!pythonExe.Exists)
                {
                    LogError("Python executable not found at: " + pythonExe.FullName, true);
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error determining FragPipe tool locations", ex);
                toolsDirectory = null;
                diannExe = null;
                pythonExe = null;
                return false;
            }
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

                if (pepXmlFiles.Count > 0)
                    return pepXmlFiles;

                return new List<FileInfo>();
            }

            pepXmlFiles.Add(new FileInfo(Path.Combine(workingDirectory.FullName, datasetName + PEPXML_EXTENSION)));

            if (pepXmlFiles[0].Exists)
                return pepXmlFiles;

            return new List<FileInfo>();
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
                    else if(targetFile.Name.EndsWith(ANNOTATION_FILE_SUFFIX))
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

            var experimentGroupNames = new SortedSet<string>();

            foreach (var item in datasetIDsByExperimentGroup.Keys)
            {
                experimentGroupNames.Add(item);
            }

            var experimentCount = experimentGroupNames.Count;

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

            if (experimentCount <= 1)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

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

            // System OS: Windows 11, Architecture: AMD64
            // Java Info: 11.0.24, OpenJDK 64-Bit Server VM, Eclipse Adoptium
            // .NET Core Info: 6.0.425
            //
            // Version info:
            // FragPipe version 22.0
            // MSFragger version 4.1
            // IonQuant version 1.10.27
            // diaTracer version 1.1.5
            // Philosopher version 5.1.1
            //
            // LCMS files:
            //   Experiment/Group: NYBB_30_P01_P
            //   (if "spectral library generation" is enabled, all files will be analyzed together)
            //   - C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML   DDA
            //   - C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML   DDA
            //   - C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML   DDA
            //   Experiment/Group: NYBB_30_P02_P
            //   (if "spectral library generation" is enabled, all files will be analyzed together)
            //   - C:\DMS_WorkDir\NYBB_30_P02_P\NYBB_30_P02_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML   DDA
            //   - C:\DMS_WorkDir\NYBB_30_P02_P\NYBB_30_P02_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML   DDA
            //   - C:\DMS_WorkDir\NYBB_30_P02_P\NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML   DDA
            //
            // 68 commands to execute:
            // CheckCentroid
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -Xmx9G -cp C:\DMS_Programs\FragPipe\fragpipe_v22.0\lib\fragpipe-22.0.jar;C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\batmass-io-1.33.4.jar com.dmtavt.fragpipe.util.CheckCentroid C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML 15
            // WorkspaceCleanInit [Work dir: C:\DMS_WorkDir]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe workspace --clean --nocheck
            // ...
            // MSFragger [Work dir: C:\DMS_WorkDir]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -jar -Dfile.encoding=UTF-8 -Xmx9G C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\MSFragger-4.1\MSFragger-4.1.jar C:\DMS_WorkDir\fragger.params C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML C:\DMS_WorkDir\NYBB_30_P02_P\NYBB_30_P02_P_f07_20Jun23_Pippin_WBEH-23-05-19.mzML C:\DMS_WorkDir\NYBB_30_P02_P\NYBB_30_P02_P_f08_20Jun23_Pippin_WBEH-23-05-19.mzML C:\DMS_WorkDir\NYBB_30_P02_P\NYBB_30_P02_P_f09_20Jun23_Pippin_WBEH-23-05-19.mzML
            // MSFragger move pepxml
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -cp C:\DMS_Programs\FragPipe\fragpipe_v22.0\lib\fragpipe-22.0.jar;/C:/DMS_Programs/FragPipe/fragpipe_v22.0/lib/commons-io-2.15.1.jar com.github.chhh.utils.FileMove --no-err C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.pepXML C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.pepXML
            // MSFragger move tsv
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -cp C:\DMS_Programs\FragPipe\fragpipe_v22.0\lib\fragpipe-22.0.jar;/C:/DMS_Programs/FragPipe/fragpipe_v22.0/lib/commons-io-2.15.1.jar com.github.chhh.utils.FileMove --no-err C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.tsv C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.tsv
            // MSFragger move pin
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -cp C:\DMS_Programs\FragPipe\fragpipe_v22.0\lib\fragpipe-22.0.jar;/C:/DMS_Programs/FragPipe/fragpipe_v22.0/lib/commons-io-2.15.1.jar com.github.chhh.utils.FileMove --no-err C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.pin C:\DMS_WorkDir\NYBB_30_P01_P\NYBB_30_P01_P_f07_20Jun23_Pippin_WBEH-23-05-19.pin
            // Percolator [Work dir: C:\DMS_WorkDir\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\percolator_3_6_5\windows\percolator.exe --only-psms --no-terminate --post-processing-tdc --num-threads 15 --results-psms NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19_percolator_target_psms.tsv --decoy-results-psms NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19_percolator_decoy_psms.tsv --protein-decoy-pattern XXX_ NYBB_30_P01_P_f08_20Jun23_Pippin_WBEH-23-05-19.pin
            // ...
            // PTMProphet [Work dir: C:\DMS_WorkDir\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\PTMProphet\PTMProphetParser.exe NOSTACK KEEPOLD STATIC EM=1 NIONS=b STY:79.966331,M:15.9949 MINPROB=0.5 MAXTHREADS=1 interact-NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.pep.xml interact-NYBB_30_P01_P_f09_20Jun23_Pippin_WBEH-23-05-19.mod.pep.xml
            // ...
            // ProteinProphet [Work dir: C:\DMS_WorkDir]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe proteinprophet --maxppmdiff 2000000 --minprob 0.5 --output combined C:\DMS_WorkDir\filelist_proteinprophet.txt
            // PhilosopherDbAnnotate [Work dir: C:\DMS_WorkDir\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe database --annotate C:\FragPipe_TestTMT_Phospho_Multi_TwoGroups\ID_008380_0E5568A3.fasta --prefix XXX_
            // PhilosopherFilter [Work dir: C:\DMS_WorkDir\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe filter --sequential --picked --prot 0.01 --tag XXX_ --pepxml C:\DMS_WorkDir\NYBB_30_P01_P --protxml C:\DMS_WorkDir\combined.prot.xml --razor
            // PhilosopherFilter [Work dir: C:\DMS_WorkDir\NYBB_30_P02_P]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe filter --sequential --picked --prot 0.01 --tag XXX_ --pepxml C:\DMS_WorkDir\NYBB_30_P02_P --dbbin C:\DMS_WorkDir\NYBB_30_P01_P --protxml C:\DMS_WorkDir\combined.prot.xml --probin C:\DMS_WorkDir\NYBB_30_P01_P --razor
            // PhilosopherReport [Work dir: C:\DMS_WorkDir\NYBB_30_P01_P]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe report
            // PhilosopherReport [Work dir: C:\DMS_WorkDir\NYBB_30_P02_P]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe report
            // WorkspaceClean [Work dir: C:\DMS_WorkDir]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\Philosopher\philosopher-v5.1.1.exe workspace --clean --nocheck
            // IonQuant [Work dir: C:\DMS_WorkDir]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -Djava.awt.headless=true -Xmx9G -Dlibs.bruker.dir=C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\MSFragger-4.1\ext\bruker -Dlibs.thermo.dir=C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\MSFragger-4.1\ext\thermo -cp C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\jfreechart-1.5.3.jar;C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\batmass-io-1.33.4.jar;C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\IonQuant-1.10.27.jar ionquant.IonQuant --threads 15 --perform-ms1quant 1 --perform-isoquant 0 --isotol 20.0 --isolevel 2 --isotype tmt10 --ionmobility 0 --site-reports 0 --msstats 0 --minexps 1 --mbr 0 --maxlfq 0 --requantify 0 --mztol 10 --imtol 0.05 --rttol 1 --normalization 0 --minisotopes 1 --minscans 1 --writeindex 0 --tp 0 --minfreq 0 --minions 1 --locprob 0 --uniqueness 0 --multidir . --filelist C:\DMS_WorkDir\filelist_ionquant.txt --modlist C:\DMS_WorkDir\modmasses_ionquant.txt
            // IonQuant [Work dir: C:\DMS_WorkDir]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -Djava.awt.headless=true -Xmx9G -Dlibs.bruker.dir=C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\MSFragger-4.1\ext\bruker -Dlibs.thermo.dir=C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\MSFragger-4.1\ext\thermo -cp C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\jfreechart-1.5.3.jar;C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\batmass-io-1.33.4.jar;C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\IonQuant-1.10.27.jar ionquant.IonQuant --threads 15 --perform-ms1quant 0 --perform-isoquant 1 --isotol 20.0 --isolevel 2 --isotype TMT-16 --ionmobility 0 --site-reports 0 --msstats 0 --annotation C:\DMS_WorkDir\NYBB_30_P01_P\psm.tsv=C:\DMS_WorkDir\NYBB_30_P01_P\annotation_01_annotation.txt --annotation C:\DMS_WorkDir\NYBB_30_P02_P\psm.tsv=C:\DMS_WorkDir\NYBB_30_P02_P\annotation_02_annotation.txt --minexps 1 --mbr 0 --maxlfq 0 --requantify 0 --mztol 10 --imtol 0.05 --rttol 1 --normalization 0 --minisotopes 1 --minscans 1 --writeindex 0 --tp 0 --minfreq 0 --minions 1 --locprob 0 --uniqueness 0 --multidir . --filelist C:\DMS_WorkDir\filelist_ionquant.txt --modlist C:\DMS_WorkDir\modmasses_ionquant.txt
            // TmtIntegrator [Work dir: C:\DMS_WorkDir]
            // C:\DMS_Programs\FragPipe\fragpipe_v22.0\jre\bin\java.exe -Xmx9G -cp C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\tmt-integrator-5.0.9.jar tmtintegrator.TMTIntegrator C:\DMS_WorkDir\tmt-integrator-conf.yml C:\DMS_WorkDir\NYBB_30_P01_P\psm.tsv C:\DMS_WorkDir\NYBB_30_P02_P\psm.tsv
            // ~~~~~~~~~~~~~~~~~~~~~~
            //
            // Execution order:
            //
            //     Cmd: [START], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [CheckCentroid], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [WorkspaceCleanInit], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [WorkspaceCleanInit], Work dir: [C:\DMS_WorkDir\NYBB_30_P01_P]
            //     Cmd: [WorkspaceCleanInit], Work dir: [C:\DMS_WorkDir\NYBB_30_P02_P]
            //     Cmd: [MSFragger], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [Percolator], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [PTMProphet], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [ProteinProphet], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [PhilosopherDbAnnotate], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [PhilosopherFilter], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [PhilosopherReport], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [WorkspaceClean], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [WorkspaceClean], Work dir: [C:\DMS_WorkDir\NYBB_30_P01_P]
            //     Cmd: [WorkspaceClean], Work dir: [C:\DMS_WorkDir\NYBB_30_P02_P]
            //     Cmd: [IonQuant], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [IonQuant], Work dir: [C:\DMS_WorkDir]
            //     Cmd: [TmtIntegrator], Work dir: [C:\DMS_WorkDir]
            //
            // ~~~~~~~~~~~~~~~~~~~~~~
            //
            // ~~~~~~Sample of C:\FragPipe_TestTMT_Phospho_Multi_TwoGroups\ID_008380_0E5568A3.fasta~~~~~~~
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
            // C:\DMS_WorkDir\NYBB_30_P01_P\annotation_01_annotation.txt:
            // 126 sample-01
            // 127N sample-02
            // 127C sample-03
            // 128N sample-04
            // 128C sample-05
            // 129N sample-06
            // 129C sample-07
            // 130N sample-08
            // 130C sample-09
            // 131N sample-10
            // 131C sample-11
            // 132N sample-12
            // 132C sample-13
            // 133N sample-14
            // 133C sample-15
            // 134N sample-16
            // C:\DMS_WorkDir\NYBB_30_P02_P\annotation_02_annotation.txt:
            // 126 sample-01
            // 127N sample-02
            // ...
            // 133C sample-15
            // 134N sample-16
            // ~~~~~~~~~~~~~~~~~~~~~~
            //

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
                { CHECK_CENTROID + 1      , GetRegEx("^MSFragger[: ]") },
                { FIRST_SEARCH_START      , GetRegEx(@"^\*+FIRST SEARCH\*+") },
                { FIRST_SEARCH_DONE       , GetRegEx(@"^\*+FIRST SEARCH DONE") },
                { FIRST_SEARCH_DONE + 1   , GetRegEx(@"^\*+MASS CALIBRATION AND PARAMETER OPTIMIZATION\*+") },
                { MAIN_SEARCH_START       , GetRegEx(@"^\*+MAIN SEARCH\*+") },
                { MAIN_SEARCH_DONE        , GetRegEx(@"^\*+MAIN SEARCH DONE") },
                { MAIN_SEARCH_DONE + 1    , GetRegEx("^Percolator[: ]") },
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
                        if (dataLine.Trim().StartsWith("FragPipe version", StringComparison.OrdinalIgnoreCase))
                        {
                            versionFound = true;
                        }

                        if (versionFound && extractVersionInfo)
                        {
                            // Determine the versions of FragPipe, MSFragger, IonQuant, Philosopher, etc.

                            LogDebug(dataLine, mDebugLevel);
                            mFragPipeVersion = dataLine.Trim();

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
                        if (!processingStep.Value.IsMatch(dataLine))
                            continue;

                        currentProgress = processingStep.Key;

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

                    var splitFastaProgressMatch = splitFastaMatcher.Match(dataLine);

                    if (splitFastaProgressMatch.Success &&
                        splitFastaProgressMatch.Groups["Action"].Value.Equals("STARTED", StringComparison.OrdinalIgnoreCase))
                    {
                        currentSplitFastaFile = int.Parse(splitFastaProgressMatch.Groups["CurrentSplitFile"].Value);

                        if (splitFastaFileCount == 0)
                        {
                            splitFastaFileCount = int.Parse(splitFastaProgressMatch.Groups["TotalSplitFiles"].Value);
                        }
                    }

                    // Check whether the line starts with the text error
                    // Future: possibly adjust this check

                    if (currentProgress > 1 &&
                        dataLine.StartsWith("error", StringComparison.OrdinalIgnoreCase) &&
                        string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                    {
                        mConsoleOutputErrorMsg = "Error running MSFragger: " + dataLine;
                    }

                    var sliceMatch = sliceMatcher.Match(dataLine);

                    if (sliceMatch.Success)
                    {
                        currentSlice = int.Parse(sliceMatch.Groups["Current"].Value);
                        totalSlices = int.Parse(sliceMatch.Groups["Total"].Value);
                    }
                    else if (currentSlice > 0)
                    {
                        var datasetMatch = datasetMatcher.Match(dataLine);

                        if (datasetMatch.Success)
                        {
                            currentDatasetId = int.Parse(datasetMatch.Groups["DatasetNumber"].Value);
                        }

                        var progressMatch = progressMatcher.Match(dataLine);

                        if (progressMatch.Success)
                        {
                            datasetProgress = float.Parse(progressMatch.Groups["PercentComplete"].Value);
                        }
                    }

                    // Check whether the line starts with the text error
                    // Future: possibly adjust this check

                    if (currentProgress > 1 &&
                        dataLine.StartsWith("error", StringComparison.OrdinalIgnoreCase) &&
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

                // This file may have been created by MSFragger; ignore it
                mJobParams.AddResultFileExtensionToSkip("_uncalibrated.mgf");

                // Skip the FragPipe log file since file FragPipe_ConsoleOutput.txt should include the log file text
                var fragPipeLogFileMatcher = new Regex(@"log_\d{4}-", RegexOptions.Compiled);

                foreach (var logFile in mWorkingDirectory.GetFiles("log_ *.txt"))
                {
                    if (fragPipeLogFileMatcher.IsMatch(logFile.Name))
                    {
                        mJobParams.AddResultFileToSkip(logFile.Name);
                    }
                }

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
            out int databaseSplitCount)
        {
            try
            {
                // Create the manifest file
                var manifestCreated = CreateManifestFile(dataPackageInfo, datasetIDsByExperimentGroup, out var manifestFilePath, out diaSearchEnabled);

                if (!manifestCreated)
                {
                    databaseSplitCount = 0;
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Customize the path to the FASTA file
                var workflowResultCode = UpdateFragPipeWorkflowFile(out var workflowFilePath, out databaseSplitCount);

                if (workflowResultCode != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return workflowResultCode;
                }

                if (string.IsNullOrWhiteSpace(workflowFilePath))
                {
                    LogError("FragPipe workflow file name returned by UpdateFragPipeParameterFile is empty");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var datasetCount = dataPackageInfo.DatasetFiles.Count;

                var options = new FragPipeOptions(mJobParams, datasetCount);
                RegisterEvents(options);

                options.LoadFragPipeOptions(workflowFilePath);

                var reporterIonModeValidated = UpdateReporterIonModeIfRequired(options);

                if (!reporterIonModeValidated)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // If reporter ions are defined, create annotation.txt files
                var annotationFileSuccess = CreateReporterIonAnnotationFiles(options);

                if (!annotationFileSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mDatasetCount = dataPackageInfo.DatasetFiles.Count;
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

                // When running FragPipe, CacheStandardOutput needs to be false,
                // otherwise the program runner will randomly lock up, preventing FragPipe from finishing

                mCmdRunner = new RunDosProgram(fragPipeBatchFile.Directory.FullName, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = false,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(mWorkDir, FRAGPIPE_CONSOLE_OUTPUT)
                };

                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                var fastaFileSizeMB = fastaFile.Length / 1024.0 / 1024;

                // Set up and execute a program runner to run FragPipe
                var processingSuccess = StartFragPipe(fragPipeBatchFile, fastaFileSizeMB, manifestFilePath, workflowFilePath, options);

                if (!mToolVersionWritten)
                {
                    if (string.IsNullOrWhiteSpace(mFragPipeVersion))
                    {
                        ParseFragPipeConsoleOutputFile(Path.Combine(mWorkDir, FRAGPIPE_CONSOLE_OUTPUT));
                    }
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputErrorMsg);
                }

                if (!processingSuccess)
                {
                    LogError("Error running FragPipe");

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
            double fastaFileSizeMB,
            string manifestFilePath,
            string workflowFilePath,
            FragPipeOptions options)
        {
            // Larger FASTA files need more memory
            // Additional memory is also required as the number of dynamic mods being considered increases

            // 10 GB of memory was not sufficient for a 26 MB FASTA file, but 15 GB worked when using 2 dynamic mods

            var dynamicModCount = options.GetDynamicModResidueCount();

            var fragPipeMemorySizeGB = AnalysisResourcesFragPipe.GetFragPipeMemorySizeToUse(mJobParams, fastaFileSizeMB, dynamicModCount, out var fragPipeMemorySizeMB);

            if (fragPipeMemorySizeGB > fragPipeMemorySizeMB / 1024.0)
            {
                var dynamicModCountDescription = FragPipeOptions.GetDynamicModCountDescription(dynamicModCount);

                var msg = string.Format("Allocating {0:N0} GB to FragPipe for a {1:N0} MB FASTA file and {2}", fragPipeMemorySizeGB, fastaFileSizeMB, dynamicModCountDescription);
                LogMessage(msg);

                mEvalMessage = Global.AppendToComment(mEvalMessage, msg);
            }

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
            // fragpipe.bat --headless --ram 0 --threads 15 --workflow C:\DMS_WorkDir\FragPipe_TMT16-phospho_2024-09-09.workflow --manifest C:\DMS_WorkDir\datasets.fp-manifest --workdir C:\DMS_WorkDir --config-tools-folder C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools --config-diann C:\DMS_Programs\FragPipe\fragpipe_v22.0\tools\diann\1.8.2_beta_8\win\DiaNN.exe --config-python C:\Python3\python.exe

            // ReSharper restore CommentTypo

            if (!DetermineFragPipeToolLocations(out var toolsDirectory, out var diannExePath, out var pythonExePath))
            {
                return false;
            }

            var arguments = new StringBuilder();

            // ReSharper disable once StringLiteralTypo
            arguments.AppendFormat("--headless --ram {0} --threads {1}", fragPipeMemorySizeGB, numThreadsToUse);

            arguments.AppendFormat(" --workflow {0}", workflowFilePath);

            arguments.AppendFormat(" --manifest {0}", manifestFilePath);

            arguments.AppendFormat(" --workdir {0}", mWorkDir);
            arguments.AppendFormat(" --config-tools-folder {0}", toolsDirectory.FullName);
            arguments.AppendFormat(" --config-diann {0}", diannExePath.FullName);
            arguments.AppendFormat(" --config-python {0}", pythonExePath.FullName);

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
        /// <param name="workflowFilePath">Output: workflow file path</param>
        /// <param name="databaseSplitCount">Output: database split count</param>
        private CloseOutType UpdateFragPipeWorkflowFile(out string workflowFilePath, out int databaseSplitCount)
        {
            const string FASTA_FILE_COMMENT = "FASTA File (should include decoy proteins)";
            const string FILE_FORMAT_COMMENT = "File format of output files; Percolator uses .pin files";

            const string REQUIRED_OUTPUT_FORMAT = "tsv_pepxml_pin";

            const string DATABASE_PATH_PARAMETER = "database.db-path";
            const string OUTPUT_FORMAT_PARAMETER = "msfragger.output_format";
            const string SLICE_DB_PARAMETER = "msfragger.misc.slice-db";

            try
            {
                var workflowFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);
                var sourceFile = new FileInfo(Path.Combine(mWorkDir, workflowFileName));
                var updatedFile = new FileInfo(Path.Combine(mWorkDir, workflowFileName + ".new"));

                databaseSplitCount = mJobParams.GetJobParameter(
                    AnalysisResourcesFragPipe.DATABASE_SPLIT_COUNT_SECTION,
                    AnalysisResourcesFragPipe.DATABASE_SPLIT_COUNT_PARAM,
                    1);

                var fastaFileDefined = false;
                var outputFormatDefined = false;
                var dbSplitCountDefined = false;

                using (var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(updatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    var lineNumber = 0;

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

                        if (trimmedLine.StartsWith(DATABASE_PATH_PARAMETER))
                        {
                            if (fastaFileDefined)
                                continue;

                            var setting = new KeyValueParamFileLine(lineNumber, dataLine, true);
                            var comment = GetComment(setting, FASTA_FILE_COMMENT);

                            // On Windows, the path to the FASTA file must use '\\' for directory separators, instead of '\'

                            var pathToUse = mLocalFASTAFilePath.Replace(@"\", @"\\");

                            WriteWorkflowFileSetting(writer, DATABASE_PATH_PARAMETER, pathToUse, comment);

                            fastaFileDefined = true;
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
                                writer.WriteLine(dataLine);
                            }
                            else
                            {
                                // Auto-change the output format to tsv_pepxml_pin
                                WriteWorkflowFileSetting(writer, OUTPUT_FORMAT_PARAMETER, REQUIRED_OUTPUT_FORMAT, comment);

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

                        writer.WriteLine(dataLine);
                    }

                    if (!fastaFileDefined)
                    {
                        WriteWorkflowFileSetting(writer, DATABASE_PATH_PARAMETER, mLocalFASTAFilePath, FASTA_FILE_COMMENT);
                    }

                    if (!outputFormatDefined)
                    {
                        WriteWorkflowFileSetting(writer, OUTPUT_FORMAT_PARAMETER, REQUIRED_OUTPUT_FORMAT, FILE_FORMAT_COMMENT);
                    }
                }

                // Replace the original parameter file with the updated one
                sourceFile.Delete();
                updatedFile.MoveTo(Path.Combine(mWorkDir, workflowFileName));

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
            // Define the path to the FASTA file
            var localOrgDbFolder = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);

            // Note that job parameter "GeneratedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
            var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

            fastaFile = new FileInfo(fastaFilePath);

            if (!fastaFile.Exists)
            {
                // FASTA file not found
                LogError("FASTA file not found: " + fastaFile.Name, "FASTA file not found: " + fastaFile.FullName);
                return false;
            }

            var proteinCollectionList = mJobParams.GetParam("ProteinCollectionList");

            var fastaHasDecoys = ValidateFastaHasDecoyProteins(fastaFile);

            if (!fastaHasDecoys)
            {
                string warningMessage;

                if (string.IsNullOrWhiteSpace(proteinCollectionList) || proteinCollectionList.Equals("na", StringComparison.OrdinalIgnoreCase))
                {
                    warningMessage = "Using a legacy FASTA file that does not have decoy proteins; " +
                                     "this will lead to errors with Peptide Prophet or Percolator";
                }
                else
                {
                    warningMessage = "Protein options for this analysis job contain seq_direction=forward; " +
                                     "decoy proteins will not be used (which will lead to errors with Peptide Prophet or Percolator)";
                }

                // The FASTA file does not have decoy sequences
                // FragPipe will be unable to optimize parameters and Peptide Prophet will likely fail
                LogError(warningMessage, true);

                // Abort processing
                return false;
            }

            // Copy the FASTA file to the working directory
            // This is done because FragPipe indexes the file based on the dynamic and static mods,
            // and we want that index file to be in the working directory

            // ReSharper disable once CommentTypo

            // Example index file name: ID_007564_FEA6EC69.fasta.1.pepindex

            mLocalFASTAFilePath = Path.Combine(mWorkDir, fastaFile.Name);

            fastaFile.CopyTo(mLocalFASTAFilePath, true);

            // Add the FASTA file and the associated index files to the list of files to skip when copying results to the transfer directory
            mJobParams.AddResultFileToSkip(fastaFile.Name);

            // ReSharper disable once StringLiteralTypo
            mJobParams.AddResultFileExtensionToSkip(".pepindex");

            // This file was created by older versions of MSFragger, but has not been seen with MSFragger 4.1
            mJobParams.AddResultFileExtensionToSkip("peptide_idx_dict");

            return true;
        }

        private bool ValidateFastaHasDecoyProteins(FileInfo fastaFile)
        {
            const string DECOY_PREFIX = "XXX_";

            try
            {
                // If using a protein collection, could check for "seq_direction=decoy" in proteinOptions
                // But, we'll instead examine the actual protein names for both Protein Collection-based and Legacy FASTA-based jobs

                var forwardCount = 0;
                var decoyCount = 0;

                var reader = new ProteinFileReader.FastaFileReader(fastaFile.FullName);

                while (reader.ReadNextProteinEntry())
                {
                    if (reader.ProteinName.StartsWith(DECOY_PREFIX))
                        decoyCount++;
                    else
                        forwardCount++;
                }

                var fileSizeMB = fastaFile.Length / 1024.0 / 1024;

                if (decoyCount == 0)
                {
                    LogDebug("FASTA file {0} is {1:N1} MB and has {2:N0} forward proteins, but no decoy proteins", fastaFile.Name, fileSizeMB, forwardCount);
                    return false;
                }

                LogDebug("FASTA file {0} is {1:N1} MB and has {2:N0} forward proteins and {3:N0} decoy proteins", fastaFile.Name, fileSizeMB, forwardCount, decoyCount);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in ValidateFastaHasDecoyProteins", ex);
                return false;
            }
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

                // If databaseSplitCount is 1, there should also be a .tsv file and a .pin file for each dataset (though with DIA data there is only a .pin file, not a .tsv file)
                // If databaseSplitCount is more than 1, we will create a .tsv file using the data in the .pepXML file

                // Zip each .pepXML file
                foreach (var experimentGroup in datasetIDsByExperimentGroup)
                {
                    var experimentGroupName = experimentGroup.Key;
                    var experimentWorkingDirectory = mExperimentGroupWorkingDirectories[experimentGroupName];

                    foreach (var datasetID in experimentGroup.Value)
                    {
                        var datasetName = dataPackageInfo.Datasets[datasetID];
                        datasetCount++;

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
                            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                            if (diaSearchEnabled)
                            {
                                // FragPipe did not create any .pepXML files
                                LogError(string.Format("FragPipe did not create any .pepXML files{0}", optionalDatasetInfo));
                            }
                            else
                            {
                                // FragPipe did not create a .pepXML file
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
    }
}
