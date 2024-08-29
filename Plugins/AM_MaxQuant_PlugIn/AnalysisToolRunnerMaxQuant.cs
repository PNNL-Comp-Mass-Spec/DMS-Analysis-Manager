//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 03/05/2021
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.DataFileTools;

namespace AnalysisManagerMaxQuantPlugIn
{
    /// <summary>
    /// Class for running MaxQuant analysis
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerMaxQuant : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: \andromeda, andromeda, apar, apl, aplfiles, deisotoping, dotnet, dryrun, fasta
        // Ignore Spelling: proc, ptms, Quant, quantitation, resourcer, sdk, secpepFiles, txt

        // ReSharper restore CommentTypo

        internal const string MAXQUANT_EXE_NAME = @"bin\MaxQuantCmd.exe";

        /// <summary>
        /// Percent complete to report when the tool starts
        /// </summary>
        public const int PROGRESS_PCT_TOOL_RUNNER_STARTING = 1;

        private const int PROGRESS_PCT_COMPLETE = 99;

        internal const string PROTEIN_DESCRIPTION_REGEX = ">[^ ]+ +(.*)";

        internal const string PROTEIN_NAME_AND_DESCRIPTION_REGEX = ">(.*)";

        private string mMaxQuantProgLoc;

        private string mConsoleOutputErrorMsg;

        private string mLocalFASTAFilePath;

        private DateTime mLastConsoleOutputParse;

        private RunDosProgram mCmdRunner;

        private MaxQuantRuntimeOptions RuntimeOptions { get; } = new();

        /// <summary>
        /// Dictionary mapping step number to the task description
        /// </summary>
        /// <remarks>
        /// Keys are step number
        /// Values are step description
        /// </remarks>
        private SortedDictionary<int, string> StepToTaskMap { get; } = new();

        /// <summary>
        /// Runs MaxQuant tool
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
                    LogDebug("AnalysisToolRunnerMaxQuant.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;
                mConsoleOutputErrorMsg = string.Empty;

                StepToTaskMap.Clear();

                // Determine the path to MaxQuantCmd.exe
                mMaxQuantProgLoc = DetermineProgramLocation("MaxQuantProgLoc", MAXQUANT_EXE_NAME);

                if (string.IsNullOrWhiteSpace(mMaxQuantProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the MaxQuant version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining MaxQuant version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!ValidateFastaFile())
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // If this job applies to a single dataset, dataPackageID will be 0
                // We still need to create an instance of DataPackageInfo to retrieve the experiment name associated with the job's dataset
                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                // The constructor for DataPackageInfo reads data package metadata from packed job parameters, which were created by the resource class
                var dataPackageInfo = new DataPackageInfo(dataPackageID, this);
                RegisterEvents(dataPackageInfo);

                // Customize the path to the FASTA file, the number of threads to use, the dataset files, etc.
                // This will involve a dry-run of MaxQuant if startStepID values in the dmsSteps elements are "auto" instead of integers

                var paramFileUpdateResult = UpdateMaxQuantParameterFileMetadata(dataPackageInfo);

                if (paramFileUpdateResult != CloseOutType.CLOSEOUT_SUCCESS)
                    return paramFileUpdateResult;

                if (!RuntimeOptions.StepRangeValidated)
                {
                    LogError("Aborting since the MaxQuant step range was not validated");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // ReSharper disable CommentTypo

                // Fix paths in the following files:
                // - Andromeda peak list files metadata file (aplfiles)
                // - Andromeda secondary peptide search metadata file (secpepFiles)
                // - Andromeda parameter files (*.apar)

                // ReSharper restore CommentTypo

                var filePathUpdateResult = UpdateMaxQuantRuntimeFilePaths();

                if (filePathUpdateResult != CloseOutType.CLOSEOUT_SUCCESS)
                    return filePathUpdateResult;

                // Process one or more datasets using MaxQuant
                var processingResult = StartMaxQuant();

                var subdirectoriesToSkipTransfer = new SortedSet<string>();

                if (processingResult == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    processingResult = PostProcessMaxQuantResults(subdirectoriesToSkipTransfer);
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

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

                var success = CopyResultsToTransferDirectory(true, subdirectoriesToSkipTransfer);

                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                return processingResult;
            }
            catch (Exception ex)
            {
                LogError("Error in MaxQuantPlugin->RunTool", ex);
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

        private bool FindDirectoriesToSkipTransfer(DirectoryInfo workingDirectory, ISet<string> subdirectoriesToSkipTransfer)
        {
            subdirectoriesToSkipTransfer.Clear();

            try
            {
                foreach (var subdirectory in workingDirectory.GetDirectories("*", SearchOption.AllDirectories))
                {
                    if (subdirectory.Parent == null)
                    {
                        LogError("Unable to determine the parent directory of " + subdirectory.FullName);
                        return false;
                    }

                    if (subdirectory.Parent.FullName.Equals(workingDirectory.FullName) &&
                        (subdirectory.Name.Equals("proc") || subdirectory.Name.Equals("txt")))
                    {
                        // We want to copy this directory to the transfer directory
                        continue;
                    }

                    subdirectoriesToSkipTransfer.Add(subdirectory.FullName);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in MaxQuantPlugin->FindDirectoriesToSkipTransfer", ex);
                return false;
            }
        }

        private bool FindDirectoriesToSkipZipping(
            SubdirectoryFileCompressor subdirectoryCompressor,
            out List<DirectoryInfo> directoriesToSkipZipping)
        {
            directoriesToSkipZipping = new List<DirectoryInfo>();

            try
            {
                var procDirectory = new DirectoryInfo(Path.Combine(subdirectoryCompressor.WorkingDirectory.FullName, "proc"));

                if (procDirectory.Exists)
                {
                    directoriesToSkipZipping.Add(procDirectory);
                }

                var txtDirectory = new DirectoryInfo(Path.Combine(subdirectoryCompressor.WorkingDirectory.FullName, "txt"));

                if (txtDirectory.Exists)
                {
                    directoriesToSkipZipping.Add(txtDirectory);
                }

                foreach (var subdirectory in subdirectoryCompressor.WorkingDirectory.GetDirectories("*", SearchOption.AllDirectories))
                {
                    if (subdirectory.Parent == null)
                    {
                        LogError("Unable to determine the parent directory of " + subdirectory.FullName);
                        return false;
                    }

                    var isUnchanged = subdirectoryCompressor.UnchangedDirectories.Any(item => item.FullName.Equals(subdirectory.FullName));

                    if (isUnchanged)
                        directoriesToSkipZipping.Add(subdirectory);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in MaxQuantPlugin->FindDirectoriesToSkipZipping", ex);
                return false;
            }
        }

        internal bool GetDmsStepDetails(XElement item, out DmsStepInfo dmsStepInfo)
        {
            var success = GetDmsStepDetails(item, out dmsStepInfo, out var errorMessage);

            if (success)
                return true;

            LogError(errorMessage);

            return false;
        }

        /// <summary>
        /// Examine the attributes of the XML node to determine DMS step metadata
        /// </summary>
        /// <param name="item">XML node to parse</param>
        /// <param name="dmsStepInfo">Output: instance of DmsStepInfo</param>
        /// <param name="errorMessage">Output: error message</param>
        /// <returns>True if success, false if an error</returns>
        internal static bool GetDmsStepDetails(XElement item, out DmsStepInfo dmsStepInfo, out string errorMessage)
        {
            if (!Global.TryGetAttribute(item, "id", out var stepIdText))
            {
                errorMessage = "DMS step in the MaxQuant parameter file is missing the 'id' attribute";
                dmsStepInfo = new DmsStepInfo(0);
                return false;
            }

            if (!Global.TryGetAttribute(item, "tool", out var stepToolName))
            {
                errorMessage = "DMS step in the MaxQuant parameter file is missing the 'tool' attribute";
                dmsStepInfo = new DmsStepInfo(0);
                return false;
            }

            if (!Global.TryGetAttribute(item, "startStepName", out var startStepName))
            {
                errorMessage = "DMS step in the MaxQuant parameter file is missing the 'startStepName' attribute";
                dmsStepInfo = new DmsStepInfo(0);
                return false;
            }

            if (!Global.TryGetAttribute(item, "startStepID", out var startStepIDText))
            {
                errorMessage = "DMS step in the MaxQuant parameter file is missing the 'startStepID' attribute";
                dmsStepInfo = new DmsStepInfo(0);
                return false;
            }

            if (!int.TryParse(stepIdText, out var stepId))
            {
                errorMessage = string.Format("DMS step in the MaxQuant parameter file has a non-numeric step ID value of '{0}", stepIdText);
                dmsStepInfo = new DmsStepInfo(0);
                return false;
            }

            dmsStepInfo = new DmsStepInfo(stepId)
            {
                Tool = stepToolName,
                StartStepName = startStepName,
            };

            if (!startStepIDText.Equals("auto") && int.TryParse(startStepIDText, out var startStepID))
            {
                dmsStepInfo.StartStepID = startStepID;
            }

            errorMessage = string.Empty;
            return true;
        }

        /// <summary>
        /// Get the expected start and end values that MaxQuant processing will represent
        /// </summary>
        /// <param name="processingSteps">Keys are step name, values are the approximate percent complete at the start of the step (value between 0 and 100)</param>
        /// <param name="progressAtStart">Output: progress at start</param>
        /// <param name="progressAtEnd">Output: progress at end</param>
        private void GetIncrementalProgressRange(IReadOnlyDictionary<string, int> processingSteps, out float progressAtStart, out float progressAtEnd)
        {
            if (RuntimeOptions.DryRun)
            {
                progressAtStart = 0;
                progressAtEnd = 1;
                return;
            }

            if (processingSteps.TryGetValue(RuntimeOptions.StartStepName, out var startStepPercentComplete))
            {
                progressAtStart = startStepPercentComplete;
            }
            else
            {
                progressAtStart = 0;
            }

            if (processingSteps.TryGetValue(RuntimeOptions.NextDMSStepStartStepName, out var nextStartStepPercentComplete))
            {
                progressAtEnd = nextStartStepPercentComplete;
            }
            else
            {
                progressAtEnd = 100;
            }
        }

        /// <summary>
        /// Get a list of typical MaxQuant processing steps
        /// </summary>
        /// <remarks>
        /// Keys are step name, values are the approximate percent complete at the start of the step
        /// </remarks>
        private static SortedList<string, int> GetMaxQuantProcessingSteps()
        {
            return new SortedList<string, int>
            {
                { "Configuring", 1 },
                { "Feature detection", 2 },
                { "Deisotoping", 3 },
                { "MS/MS preparation", 4 },
                { "Combining apl files for first search", 5 },
                { "Preparing searches", 6 },
                { "MS/MS first search", 34 },
                { "Read search results for recalibration", 50 },
                { "Mass recalibration", 51 },
                { "Calculating masses", 54 },
                { "MS/MS preparation for main search", 54 },
                { "Combining apl files for main search", 56 },
                { "MS/MS main search", 56 },
                { "Preparing combined folder ", 70 },
                { "Correcting errors", 70 },
                { "Reading search engine results", 70 },
                { "Preparing reverse hits", 71 },
                { "Finish search engine results", 72 },
                { "Filter identifications (MS/MS)", 72 },
                { "Calculating PEP", 72 },
                { "Copying identifications", 73 },
                { "Applying FDR", 73 },
                { "Assembling second peptide MS/MS", 73 },
                { "Combining second peptide files", 74 },
                { "Second peptide search", 74 },
                { "Reading search engine results (SP)", 87 },
                { "Finish search engine results (SP)", 88 },
                { "Filtering identifications (SP)", 88 },
                { "Applying FDR (SP)", 89 },
                { "Re-quantification", 89 },
                { "Reporter quantification", 89 },
                { "Prepare protein assembly", 89 },
                { "Assembling proteins", 90 },
                { "Assembling unidentified peptides", 91 },
                { "Finish protein assembly", 91 },
                { "Updating identifications", 95 },
                { "Estimating complexity", 95 },
                { "Prepare writing tables ", 95 },
                { "Writing tables", 95 },
                { "Finish writing tables ", 99 }
            };
        }

        /// <summary>
        /// Determine the number of threads to use for MaxQuant
        /// </summary>
        /// <remarks>
        /// Allow MaxQuant to use 88% of the physical cores
        /// </remarks>
        private int GetNumThreadsToUse()
        {
            var coreCount = Global.GetCoreCount();
            return (int)Math.Floor(coreCount * 0.88);
        }

        private static void MoveFileOverwrite(FileInfo fileToMove, string newFilePath)
        {
            var targetFile = new FileInfo(newFilePath);

            if (targetFile.Exists)
                targetFile.Delete();

            fileToMove.MoveTo(newFilePath);
        }

        /// <summary>
        /// Move the #runningTimes.txt file in the proc directory to a proc directory below the working directory
        /// When moving, rename it to include the step tool name
        /// </summary>
        /// <param name="workingDirectory"></param>
        /// <param name="combinedDirectory"></param>
        private void MoveRunningTimeFiles(FileSystemInfo workingDirectory, FileSystemInfo combinedDirectory)
        {
            try
            {
                var procDirectory = new DirectoryInfo(Path.Combine(combinedDirectory.FullName, "proc"));
                var targetDirectory = new DirectoryInfo(Path.Combine(workingDirectory.FullName, "proc"));

                if (!procDirectory.Exists)
                    return;

                if (!targetDirectory.Exists)
                    targetDirectory.Create();

                foreach (var fileToMove in procDirectory.GetFiles("#runningTimes.txt"))
                {
                    var newFilePath = Path.Combine(targetDirectory.FullName, string.Format("#runningTimes_{0}.txt", StepToolName));
                    MoveFileOverwrite(fileToMove, newFilePath);
                    break;
                }
            }
            catch (Exception ex)
            {
                LogError("Error in MaxQuantPlugin->MoveRunningTimeFiles", ex);
            }
        }

        /// <summary>
        /// Move the txt directory to be just below the working directory
        /// </summary>
        /// <param name="workingDirectory"></param>
        /// <param name="combinedDirectory"></param>
        private bool MoveTxtDirectory(FileSystemInfo workingDirectory, FileSystemInfo combinedDirectory)
        {
            try
            {
                if (!combinedDirectory.Exists)
                {
                    LogError("The MaxQuant search should be complete, but the combined directory does not exist");
                    return false;
                }

                var txtDirectory = new DirectoryInfo(Path.Combine(combinedDirectory.FullName, "txt"));

                if (!txtDirectory.Exists)
                {
                    LogError("The MaxQuant search should be complete, but the txt directory does not exist in the combined directory");
                    return false;
                }

                var newTxtDirectory = new DirectoryInfo(Path.Combine(workingDirectory.FullName, "txt"));

                if (!newTxtDirectory.Exists)
                    newTxtDirectory.Create();

                foreach (var fileToMove in txtDirectory.GetFiles())
                {
                    var newFilePath = Path.Combine(newTxtDirectory.FullName, fileToMove.Name);
                    MoveFileOverwrite(fileToMove, newFilePath);
                }

                txtDirectory.Delete();
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in MaxQuantPlugin->MoveTxtDirectory", ex);
                return false;
            }
        }

        /// <summary>
        /// Post-process MaxQuant results
        /// </summary>
        /// <param name="subdirectoriesToSkipTransfer">Full paths to subdirectories that should not be copied to the remote server</param>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType PostProcessMaxQuantResults(ISet<string> subdirectoriesToSkipTransfer)
        {
            try
            {
                var workingDirectory = new DirectoryInfo(mWorkDir);

                // This dictionary tracks directories for which their subdirectories will be zipped separately
                // Keys are DirectoryInfo instances
                // Values are true if the directory's files should be zipped, or false if they should be left alone
                var directoriesToZipSubsSeparately = new Dictionary<DirectoryInfo, bool>();

                var combinedDirectory = new DirectoryInfo(Path.Combine(workingDirectory.FullName, "combined"));

                if (combinedDirectory.Exists)
                {
                    directoriesToZipSubsSeparately.Add(combinedDirectory, false);

                    // Move the #runningTimes.txt file in the proc directory to a proc directory below the working directory
                    MoveRunningTimeFiles(workingDirectory, combinedDirectory);
                }
                else
                {
                    LogWarning("Combined directory not found, indicating a problem running MaxQuant");
                }

                var maxquantSearchComplete = combinedDirectory.Exists && RuntimeOptions.EndStepNumber >= MaxQuantRuntimeOptions.MAX_STEP_NUMBER;

                var txtDirectoryMoveError = false;

                if (maxquantSearchComplete)
                {
                    // Move the txt directory to be just below the working directory
                    var txtMoveSuccess = MoveTxtDirectory(workingDirectory, combinedDirectory);

                    if (!txtMoveSuccess)
                    {
                        // This is a critical error
                        // Allow the FileCompressor to zip subdirectories, but return an error code from this method
                        maxquantSearchComplete = false;
                        txtDirectoryMoveError = true;
                    }
                }

                var subdirectoryCompressor = new SubdirectoryFileCompressor(workingDirectory, mDebugLevel);
                RegisterEvents(subdirectoryCompressor);

                // Examine subdirectory names to determine which ones should be zipped and copied to the transfer directory
                // Skip any that do not have any changed files

                // Also, if RuntimeOptions.EndStepNumber >= MaxQuantRuntimeOptions.MAX_STEP_NUMBER,
                // skip all except the txt directory below the combined subdirectory and the proc directory below the working directory

                var findUnchangedSuccess = subdirectoryCompressor.FindUnchangedDirectories();

                if (!findUnchangedSuccess)
                {
                    LogWarning("SubdirectoryFileCompressor->FindUnchangedDirectories returned false");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (maxquantSearchComplete)
                {
                    var success = FindDirectoriesToSkipTransfer(workingDirectory, subdirectoriesToSkipTransfer);

                    if (!success)
                        return CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    var success = FindDirectoriesToSkipZipping(subdirectoryCompressor, out var directoriesToSkipZipping);

                    if (!success)
                        return CloseOutType.CLOSEOUT_FAILED;

                    var successZipping = subdirectoryCompressor.ZipDirectories(directoriesToSkipZipping, directoriesToZipSubsSeparately);

                    if (!successZipping)
                        return CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE;
                }

                mJobParams.AddResultFileToSkip(SubdirectoryFileCompressor.WORKING_DIRECTORY_METADATA_FILE);

                if (combinedDirectory.Exists && !txtDirectoryMoveError)
                    return CloseOutType.CLOSEOUT_SUCCESS;

                return CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error in MaxQuantPlugin->PostProcessMaxQuantResults", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Parse the MaxQuant console output file to track the search progress
        /// </summary>
        private void ParseConsoleOutputFile()
        {
            // Example Console output

            // id      number of threads       job name
            // Configuring
            // Assemble run info
            // Finish run info
            // Testing fasta files
            // Testing raw files
            // Feature detection
            // Deisotoping
            // MS/MS preparation
            // Calculating peak properties
            // Combining apl files for first search
            // Preparing searches
            // MS/MS first search
            // etc.

            var processingSteps = GetMaxQuantProcessingSteps();

            var dryRunStepMatcher = new Regex(@"^(?<StepNumber>\d+)[ \t]+(?<TaskDescription>.+)", RegexOptions.Compiled);

            try
            {
                if (!File.Exists(RuntimeOptions.ConsoleOutputFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + RuntimeOptions.ConsoleOutputFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 4)
                {
                    LogDebug("Parsing file " + RuntimeOptions.ConsoleOutputFilePath);
                }

                mConsoleOutputErrorMsg = string.Empty;
                var currentProgress = 0;

                // Dictionary mapping step number to the task description
                // This is only populated if RuntimeOptions.DryRun is true
                var stepToTaskMap = new SortedDictionary<int, string>();

                GetIncrementalProgressRange(processingSteps, out var progressAtStart, out var progressAtEnd);

                var exceptionFound = false;

                using var reader = new StreamReader(new FileStream(RuntimeOptions.ConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    if (exceptionFound)
                    {
                        if (!dataLine.Trim().StartsWith("at "))
                        {
                            // Don't read any more lines (could be more exceptions)
                            break;
                        }

                        mConsoleOutputErrorMsg = Global.AppendToComment(mConsoleOutputErrorMsg, dataLine);
                        continue;
                    }

                    if (dataLine.StartsWith("Unhandled Exception:", StringComparison.OrdinalIgnoreCase))
                    {
                        mConsoleOutputErrorMsg = "Error running MaxQuant: " + dataLine;
                        exceptionFound = true;
                        continue;
                    }

                    if (RuntimeOptions.DryRun)
                    {
                        var match = dryRunStepMatcher.Match(dataLine);

                        if (match.Success)
                        {
                            var stepNumber = int.Parse(match.Groups["StepNumber"].Value);
                            var taskDescription = match.Groups["TaskDescription"].Value.Trim();

                            stepToTaskMap.Add(stepNumber, taskDescription);
                        }
                    }

                    foreach (var processingStep in processingSteps.Where(processingStep => dataLine.StartsWith(processingStep.Key, StringComparison.OrdinalIgnoreCase)))
                    {
                        currentProgress = processingStep.Value;
                    }
                }

                if (RuntimeOptions.DryRun)
                {
                    StepToTaskMap.Clear();

                    foreach (var item in stepToTaskMap.Where(item => !StepToTaskMap.ContainsKey(item.Key)))
                    {
                        StepToTaskMap.Add(item.Key, item.Value);
                    }

                    return;
                }

                var progressRange = progressAtEnd - progressAtStart;
                var currentProgressAdjusted = currentProgress - progressAtStart;

                if (progressRange > 0)
                {
                    mProgress = currentProgressAdjusted / progressRange * 100;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate(string.Format(
                        "Error parsing the MaxQuant console output file ({0}): {1}",
                        RuntimeOptions.ConsoleOutputFilePath, ex.Message));
                }
            }
        }

        /// <summary>
        /// Remove the child nodes of the given parent nodes
        /// </summary>
        /// <param name="doc"></param>
        /// <param name="parentNodeName"></param>
        /// <returns>Count removed</returns>
        private int RemoveNodeChildren(XContainer doc, string parentNodeName)
        {
            var childNodeCount = 0;

            foreach (var parent in doc.Elements("MaxQuantParams").Elements(parentNodeName))
            {
                childNodeCount = parent.Descendants().Count();
                parent.RemoveAll();
            }

            return childNodeCount;
        }

        /// <summary>
        /// Find and remove all children of the given parent node (assuming there is only one instance of the parent)
        /// Next, append new children nodes
        /// </summary>
        /// <param name="doc"></param>
        /// <param name="parentNodeName"></param>
        /// <param name="childNodes"></param>
        private static void ReplaceChildNodes(XContainer doc, string parentNodeName, IEnumerable<XElement> childNodes)
        {
            var parentNode = doc.Elements("MaxQuantParams").Elements(parentNodeName).First();
            parentNode.RemoveAll();

            foreach (var item in childNodes)
            {
                parentNode.Add(item);
            }
        }

        /// <summary>
        /// Run MaxQuant, using options define in RuntimeOptions
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType StartMaxQuant()
        {
            LogMessage("Running MaxQuant");

            if (string.IsNullOrWhiteSpace(RuntimeOptions.ParameterFilePath))
            {
                LogError("MaxQuant parameter file name returned by UpdateMaxQuantParameterFile is empty");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Set up and execute a program runner to run MaxQuant
            var cmdLineArguments = new List<string>();

            if (RuntimeOptions.DryRun)
            {
                RuntimeOptions.ConsoleOutputFilePath = Path.Combine(mWorkDir, "MaxQuant_ConsoleOutput.txt");
                cmdLineArguments.Add("--dryrun");
            }
            else if (RuntimeOptions.StepRangeDefined)
            {
                RuntimeOptions.ConsoleOutputFilePath = Path.Combine(mWorkDir, StepToolName + "_ConsoleOutput.txt");
                cmdLineArguments.Add("--partial-processing=" + RuntimeOptions.StartStepNumber);
                cmdLineArguments.Add("--partial-processing-end=" + RuntimeOptions.EndStepNumber);
            }

            cmdLineArguments.Add(RuntimeOptions.ParameterFilePath);

            var arguments = string.Join(" ", cmdLineArguments);

            LogDebug(mMaxQuantProgLoc + " " + arguments);

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = RuntimeOptions.ConsoleOutputFilePath
            };
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mProgress = PROGRESS_PCT_TOOL_RUNNER_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var processingSuccess = mCmdRunner.RunProgram(mMaxQuantProgLoc, arguments, "MaxQuant", true);

            // Parse the console output file one more time
            ParseConsoleOutputFile();

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!processingSuccess)
            {
                var dotNetDirectory = new DirectoryInfo(@"C:\Program Files\dotnet");

                var searchPath = Environment.GetEnvironmentVariable("Path");

                if (searchPath != null && dotNetDirectory.Exists && searchPath.IndexOf(@"C:\Program Files\dotnet", StringComparison.OrdinalIgnoreCase) < 0)
                {
                    LogError(@"Directory 'C:\Program Files\dotnet' exists but it is not present in the system path; the DMS Program Runner Service likely needs to be restarted");
                }

                if (mCmdRunner.CachedConsoleErrors.Contains(".NET Core 3.1"))
                {
                    LogError("Install 64-bit .NET Core SDK 3.1 (file dotnet-sdk-3.1.416-win-x64.exe) from https://dotnet.microsoft.com/en-us/download/dotnet/3.1");
                }
                else if (mCmdRunner.CachedConsoleErrors.Contains(".NET Core 2.1"))
                {
                    LogError("Install 64-bit .NET Core SDK 2.1 (file dotnet-sdk-2.1.813-win-x64.exe) from https://dotnet.microsoft.com/download/dotnet/2.1");
                }
                else if (mCmdRunner.CachedConsoleErrors.Contains(".NET Core"))
                {
                    LogError("Install the 64-bit .NET Core SDK for the required version of .NET Core");
                }

                LogError("Error running MaxQuant");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("MaxQuant returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to MaxQuant failed (but exit code is 0)");
                }

                // Optionally change this to true to aid with debugging
                // ReSharper disable once InvertIf
                if (false)
                {
#pragma warning disable CS0162 // Unreachable code detected
                    LogWarning("MaxQuant processing failed");
                    LogWarning("Sleeping for 5 minutes to allow for diagnosis");

                    var startTime = DateTime.UtcNow;

                    while (DateTime.UtcNow.Subtract(startTime).TotalMinutes < 5)
                    {
                        Console.Write(". ");
                        Global.IdleLoop(15);
                    }

                    Console.WriteLine();
#pragma warning restore CS0162 // Unreachable code detected
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }

            mStatusTools.UpdateAndWrite(mProgress);

            if (mDebugLevel >= 3)
            {
                if (RuntimeOptions.DryRun)
                    LogDebug("MaxQuant Dry Run Complete");
                else
                    LogDebug("MaxQuant Search Complete");
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var additionalDLLs = new List<string> {
                "MaxQuantTask.exe",
                "MaxQuantLib.dll",
                "MaxQuantLibS.dll",
                "MaxQuantPLib.dll"
            };

            var success = StoreDotNETToolVersionInfo(mMaxQuantProgLoc, additionalDLLs, true);

            if (!success)
                return false;

            if (!StepToolName.Equals("MaxqS1", StringComparison.OrdinalIgnoreCase))
                return true;

            try
            {
                // Rename Tool_Version_Info_MaxqS1.txt to Tool_Version_Info_MaxQuant.txt
                var sourceFile = new FileInfo(Path.Combine(mWorkDir, "Tool_Version_Info_MaxqS1.txt"));

                if (!sourceFile.Exists)
                {
                    LogWarning("MaxqS1 tool version info file not found: " + sourceFile.FullName);
                    return false;
                }

                var targetFile = new FileInfo(Path.Combine(mWorkDir, "Tool_Version_Info_MaxQuant.txt"));

                if (targetFile.Exists)
                    targetFile.Delete();

                sourceFile.MoveTo(targetFile.FullName);
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in StoreToolVersionInfo", ex);
                return false;
            }
        }

        /// <summary>
        /// Update the "fasta file path" entry in the specified Andromeda parameter files
        /// Also update other parameter files in the same directory
        /// </summary>
        /// <param name="workingDirectory"></param>
        /// <param name="andromedaParameterFiles">
        /// Parameter files listed in aplfiles
        /// These will be compared to entries in processedParameterFilePaths to see if any were missed
        /// </param>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType UpdateAndromedaParameterFiles(DirectoryInfo workingDirectory, IEnumerable<string> andromedaParameterFiles)
        {
            var localOrgDbDirectory = new DirectoryInfo(mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR));
            var generatedFastaFileName = mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME);

            var generatedFastaFilePath = Path.Combine(localOrgDbDirectory.FullName, generatedFastaFileName);

            // This tracks full paths of parameter files that have been checked and updated if necessary
            var processedParameterFilePaths = new SortedSet<string>();
            var countUpdated = 0;

            foreach (var parameterFile in workingDirectory.GetFiles("*.apar", SearchOption.AllDirectories))
            {
                processedParameterFilePaths.Add(parameterFile.FullName);
                var success = UpdateAndromedaParameterFile(localOrgDbDirectory, generatedFastaFilePath, parameterFile, out var fileUpdated);

                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                if (fileUpdated)
                    countUpdated++;
            }

            // See if any were missed
            foreach (var parameterFilePath in andromedaParameterFiles)
            {
                var parameterFile = new FileInfo(parameterFilePath);

                if (processedParameterFilePaths.Contains(parameterFile.FullName))
                    continue;

                if (!parameterFile.Exists)
                {
                    LogWarning("The aplfiles parameter file mentioned a file in an unexpected location, but the file does not exist: {0}", parameterFile.FullName);
                    continue;
                }

                LogWarning("The aplfiles parameter file mentioned a file in an unexpected location, processing now: {0}", parameterFile.FullName);

                processedParameterFilePaths.Add(parameterFile.FullName);

                var success = UpdateAndromedaParameterFile(localOrgDbDirectory, generatedFastaFilePath, parameterFile, out var fileUpdated);

                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                if (fileUpdated)
                    countUpdated++;
            }

            if (countUpdated == 0)
            {
                LogMessage(string.Format(
                    "Andromeda parameter files were already up-to-date; checked {0} files",
                    processedParameterFilePaths.Count));

                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            LogMessage("Updated the FASTA file path in {0} Andromeda parameter files; {1} were already up-to-date", countUpdated, processedParameterFilePaths.Count - countUpdated);

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Update the "fasta file path" entry in the Andromeda parameter file
        /// </summary>
        /// <param name="localOrgDbDirectory"></param>
        /// <param name="generatedFastaFilePath">Full path to the FASTA file generated by the resourcer for this job</param>
        /// <param name="parameterFile">Parameter file to examine</param>
        /// <param name="fileUpdated"></param>
        /// <returns>True if success, false if an error</returns>
        private bool UpdateAndromedaParameterFile(
            FileSystemInfo localOrgDbDirectory,
            string generatedFastaFilePath,
            FileSystemInfo parameterFile,
            out bool fileUpdated)
        {
            // Excerpt of the region we're searching for:

            // max missed cleavages=4
            // fasta file path="C:\DMS_Temp_Org\ID_003456_9B916A8B.fasta"
            // identifier parse rule=">([^ ]+)"
            // description parse rule=">([^ ]+) *(.*)"

            try
            {
                var parameterFilePath = parameterFile.FullName;

                var dataLines = new List<string>();
                var delimiter = new[] { '=' };
                var pathUpdated = false;

                using (var reader = new StreamReader(new FileStream(parameterFile.FullName, FileMode.Open, FileAccess.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        // Split on an equals sign
                        var lineParts = dataLine.Split(delimiter, 2).ToList();

                        if (lineParts.Count < 2)
                        {
                            dataLines.Add(dataLine);
                            continue;
                        }

                        if (!lineParts[0].Equals("fasta file path"))
                        {
                            dataLines.Add(dataLine);
                            continue;
                        }

                        // Examine the path to the FASTA file and update if necessary
                        var fastaFilePath = lineParts[1].Trim('"');

                        var fastaFileInfo = new FileInfo(fastaFilePath);

                        if (fastaFileInfo.Directory?.FullName.Equals(localOrgDbDirectory.FullName) == true)
                        {
                            dataLines.Add(dataLine);
                            continue;
                        }

                        var updatedPath = Path.Combine(localOrgDbDirectory.FullName, fastaFileInfo.Name);

                        if (!updatedPath.Equals(generatedFastaFilePath))
                        {
                            LogWarning("Mismatch between FASTA file path in andromeda parameter file {0} and the generated FASTA file: {1} vs. {2}", parameterFile.FullName, updatedPath, generatedFastaFilePath);
                        }

                        dataLines.Add(string.Format("fasta file path=\"{0}\"", updatedPath));
                        pathUpdated = true;
                    }
                }

                if (!pathUpdated)
                {
                    // No change was made
                    fileUpdated = false;
                    return true;
                }

                // Replace the original parameter file
                var updatedFile = new FileInfo(parameterFile.FullName + ".new");

                using (var writer = new StreamWriter(new FileStream(updatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    foreach (var item in dataLines)
                    {
                        writer.WriteLine(item);
                    }
                }

                parameterFile.Delete();
                updatedFile.MoveTo(parameterFilePath);

                fileUpdated = true;
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in UpdateAndromedaParameterFile", ex);
                fileUpdated = false;
                return false;
            }
        }

        /// <summary>
        /// Update paths in an Andromeda peak list metadata file
        /// </summary>
        /// <param name="workingDirectory"></param>
        /// <param name="metadataFile"></param>
        /// <param name="andromedaParameterFiles"></param>
        /// <returns>True if success, false if an error</returns>
        private bool UpdateAndromedaPeakListFile(
            FileSystemInfo workingDirectory,
            FileSystemInfo metadataFile,
            ISet<string> andromedaParameterFiles)
        {
            // ReSharper disable CommentTypo

            // Example file contents (tab-separated):

            // C:\DMS_WorkDir\combined\andromeda\allSpectra.CID.ITMS.iso_0.apl	C:\DMS_WorkDir\combined\andromeda\allSpectra.CID.ITMS.iso.apar
            // C:\DMS_WorkDir\combined\andromeda\allSpectra.CID.ITMS.iso_1.apl	C:\DMS_WorkDir\combined\andromeda\allSpectra.CID.ITMS.iso.apar
            // C:\DMS_WorkDir\combined\andromeda\allSpectra.CID.ITMS.iso_2.apl	C:\DMS_WorkDir\combined\andromeda\allSpectra.CID.ITMS.iso.apar

            // ReSharper restore CommentTypo

            // This RegEx will be applied separately to the left and right columns in the input file
            var directoryMatcher = new Regex(@"^(?<ParentPath>.+)(?<RelativePath>[\\/]combined[\\/].+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            // This list holds updated lines
            var metadataFileContents = new List<string>();

            var metadataFilePath = metadataFile.FullName;

            var linesRead = 0;
            var updateRequired = false;

            using (var reader = new StreamReader(new FileStream(metadataFile.FullName, FileMode.Open, FileAccess.ReadWrite)))
            {
                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    // Split on tabs
                    var lineParts = dataLine.Split('\t').ToList();

                    if (lineParts.Count < 2)
                    {
                        LogWarning("Line {0} in file {1} does not have a tab; this is unexpected: {2}", linesRead, metadataFile.FullName, dataLine);
                        continue;
                    }

                    // Look for the parent directory above the \combined\ directory in both columns
                    var aplMatch = directoryMatcher.Match(lineParts[0]);
                    var aprMatch = directoryMatcher.Match(lineParts[1]);

                    if (!aplMatch.Success)
                    {
                        LogWarning("Line {0} in file {1} did not contain a directory named combined in column 1; this is unexpected: {2}", linesRead, metadataFile.FullName, dataLine);
                        continue;
                    }

                    if (!aprMatch.Success)
                    {
                        LogWarning("Line {0} in file {1} did not contain a directory named combined in column 2; this is unexpected: {2}", linesRead, metadataFile.FullName, dataLine);
                        continue;
                    }

                    string updatedLine;
                    string updatedParameterFilePath;

                    if (aplMatch.Groups["ParentPath"].Value.Equals(workingDirectory.FullName))
                    {
                        // Drive letter and directory name are already correct
                        updatedLine = dataLine;
                        updatedParameterFilePath = lineParts[1];
                    }
                    else
                    {
                        var updatedAplFilePath = workingDirectory.FullName + aplMatch.Groups["RelativePath"];
                        updatedParameterFilePath = workingDirectory.FullName + aprMatch.Groups["RelativePath"];

                        updatedLine = string.Format("{0}\t{1}", updatedAplFilePath, updatedParameterFilePath);
                        updateRequired = true;
                    }

                    metadataFileContents.Add(updatedLine);

                    // Add the updated parameter file, if not yet in the sorted set
                    andromedaParameterFiles.Add(updatedParameterFilePath);
                }
            }

            if (!updateRequired)
            {
                LogMessage("Andromeda peak list file is already up-to-date: " + metadataFile.FullName);
                return true;
            }

            LogMessage("Updating paths in Andromeda peak list file: " + metadataFile.FullName);

            // Replace the original aplfiles file
            var updatedFile = new FileInfo(metadataFile.FullName + ".new");

            using (var writer = new StreamWriter(new FileStream(updatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
            {
                foreach (var metadataLine in metadataFileContents)
                {
                    writer.WriteLine(metadataLine);
                }
            }

            metadataFile.Delete();
            updatedFile.MoveTo(metadataFilePath);

            return true;
        }

        private CloseOutType UpdateMaxQuantParameterFileMetadata(DataPackageInfo dataPackageInfo)
        {
            try
            {
                var paramFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);
                var sourceFile = new FileInfo(Path.Combine(mWorkDir, paramFileName));
                var updatedFile = new FileInfo(Path.Combine(mWorkDir, paramFileName + "_CustomSettings.xml"));

                var numThreadsToUse = GetNumThreadsToUse();

                // Keys in this dictionary are step IDs
                // Values are step info
                var dmsSteps = new SortedDictionary<int, DmsStepInfo>();

                using (var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    // Note that XDocument supersedes XmlDocument and XPathDocument
                    // XDocument can often be easier to use since XDocument is LINQ-based

                    var doc = XDocument.Parse(reader.ReadToEnd());

                    var dmsStepNodes = doc.Elements("MaxQuantParams").Elements("dmsSteps").Elements("step").ToList();

                    if (dmsStepNodes.Count == 0)
                    {
                        if (Global.IsMatch(mDatasetName, AnalysisResources.AGGREGATION_JOB_DATASET))
                        {
                            LogError("MaxQuant parameter file does not have a dmsSteps section; this is required for data-package based MaxQuant tasks");
                            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                        }
                    }
                    else
                    {
                        // Get the DMS step info
                        foreach (var item in dmsStepNodes)
                        {
                            if (!GetDmsStepDetails(item, out var dmsStepInfo))
                            {
                                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                            }

                            dmsSteps.Add(dmsStepInfo.ID, dmsStepInfo);
                        }
                    }

                    var fastaFileNodes = doc.Elements("MaxQuantParams").Elements("fastaFiles").ToList();

                    var numThreadsNode = doc.Elements("MaxQuantParams").Elements("numThreads").ToList();

                    var filePathChildCount = RemoveNodeChildren(doc, "filePaths");
                    var experimentChildCount = RemoveNodeChildren(doc, "experiments");
                    var fractionChildCount = RemoveNodeChildren(doc, "fractions");

                    var parameterGroupNodes = doc.Elements("MaxQuantParams").Elements("parameterGroups").Elements("parameterGroup").ToList();

                    var nodesToValidate = new Dictionary<string, int>
                    {
                        {"fastaFiles", fastaFileNodes.Count},
                        {"numThreads", numThreadsNode.Count},
                        {"filePaths", filePathChildCount},
                        {"experiments", experimentChildCount},
                        {"fractions", fractionChildCount},
                        {"parameterGroups", parameterGroupNodes.Count}
                    };

                    if (!ValidateNodesPresent(nodesToValidate))
                    {
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }

                    // Update the thread count
                    numThreadsNode[0].Value = numThreadsToUse.ToString();

                    fastaFileNodes.Clear();

                    var proteinDescriptionParseRule = mJobParams.GetJobParameter(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResourcesMaxQuant.JOB_PARAM_PROTEIN_DESCRIPTION_PARSE_RULE, string.Empty);

                    var fastaFileInfoNode = new XElement("FastaFileInfo");
                    fastaFileInfoNode.Add(new XElement("fastaFilePath", mLocalFASTAFilePath));
                    fastaFileInfoNode.Add(new XElement("identifierParseRule", ">([^ ]+)"));

                    // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                    if (string.IsNullOrWhiteSpace(proteinDescriptionParseRule))
                    {
                        // This RegEx will match both the protein name and protein description
                        fastaFileInfoNode.Add(new XElement("descriptionParseRule", PROTEIN_NAME_AND_DESCRIPTION_REGEX));
                    }
                    else
                    {
                        // Use the RegEx used by the previous job step
                        fastaFileInfoNode.Add(new XElement("descriptionParseRule", proteinDescriptionParseRule));
                    }

                    fastaFileInfoNode.Add(new XElement("taxonomyParseRule", string.Empty));
                    fastaFileInfoNode.Add(new XElement("variationParseRule", @">[^\s]+\s+(.+)"));
                    fastaFileInfoNode.Add(new XElement("modificationParseRule", string.Empty));
                    fastaFileInfoNode.Add(new XElement("taxonomyId", -1));

                    fastaFileNodes.Add(fastaFileInfoNode);

                    var filePathNodes = new List<XElement>();
                    var experimentNodes = new List<XElement>();
                    var fractionNodes = new List<XElement>();
                    var ptmNodes = new List<XElement>();
                    var paramGroupIndexNodes = new List<XElement>();
                    var referenceChannelNodes = new List<XElement>();

                    // If the experiment ends in text similar to "_f01" or "_f22", assume this is a fractionated sample
                    var fractionMatcher = new Regex(@"_f(?<FractionNumber>\d+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                    // Check whether datasets in this data package have non-zero values for MaxQuant Parameter Group

                    // Parameter groups are most commonly used to group datasets when using label-free quantitation (LFQ)
                    // Datasets grouped together will be normalized together

                    // Parameter groups can also be used to define different search settings for different groups of datasets
                    // (different enzyme, different search tolerances)

                    // If any parameter group IDs are found, determine the group indices to use (auto adjusting from group 1 and group 2 to groupIndex 0 and groupIndex 1 if necessary)
                    // We will use this information to duplicate the first <parameterGroup></parameterGroup> section from the master parameter file to add the necessary number of <parameterGroup> sections (if the file already has the extras, use them)
                    // The groupIndex values are used when adding datasets
                    // See: https://dms2.pnl.gov/data_package_dataset/report/3887/-/-/-/-/-

                    // Keys in this dictionary are MaxQuant group index values (0-based)
                    // Values are a list of dataset IDs
                    var dataPackageDatasetsByParamGroup = new SortedDictionary<int, SortedSet<int>>();

                    foreach (var item in dataPackageInfo.Datasets)
                    {
                        var groupIndex = dataPackageInfo.DatasetMaxQuantParamGroup[item.Key];

                        if (dataPackageDatasetsByParamGroup.TryGetValue(groupIndex, out var matchedDatasetsForGroup))
                        {
                            matchedDatasetsForGroup.Add(item.Key);
                            continue;
                        }

                        var datasetsForGroup = new SortedSet<int>
                        {
                            item.Key
                        };

                        dataPackageDatasetsByParamGroup.Add(groupIndex, datasetsForGroup);
                    }

                    // This dictionary is used to guarantee that the group index values in the customized MaxQuant parameter file start at 0 and are contiguous
                    // Keys in this dictionary are the group index or number defined by the user in the data package
                    // Values are 0 for the first group, 1 for the second, 2 for the 3rd, etc.
                    var groupIndexOrNumberMap = new SortedDictionary<int, int>();

                    foreach (var groupIndexOrNumber in dataPackageDatasetsByParamGroup.Keys)
                    {
                        var zeroBasedGroupIndex = groupIndexOrNumberMap.Count;
                        groupIndexOrNumberMap.Add(groupIndexOrNumber, zeroBasedGroupIndex);
                    }

                    foreach (var paramGroupItem in dataPackageDatasetsByParamGroup)
                    {
                        var paramGroupIndex = groupIndexOrNumberMap[paramGroupItem.Key];

                        foreach (var datasetId in paramGroupItem.Value)
                        {
                            var datasetFileOrDirectoryName = dataPackageInfo.DatasetFiles[datasetId];
                            var experiment = dataPackageInfo.Experiments[datasetId];

                            var experimentGroup = dataPackageInfo.DatasetExperimentGroup[datasetId];

                            filePathNodes.Add(new XElement("string", Path.Combine(mWorkDir, datasetFileOrDirectoryName)));

                            // ReSharper disable once CommentTypo

                            // If Dataset Experiment Group is an empty string or an integer, use the dataset's experiment name as the experiment name in the parameter file
                            // Dataset Experiment Group is parsed from the data package comment field for datasets in a Data Package
                            experimentNodes.Add(new XElement("string",
                                string.IsNullOrWhiteSpace(experimentGroup) || int.TryParse(experimentGroup, out _)
                                    ? experiment
                                    : experimentGroup));

                            var match = fractionMatcher.Match(experiment);

                            var fractionNumberFromExperiment = match.Success ? match.Groups["FractionNumber"].Value : "32767";

                            var fractionNumberFromPackageComment = dataPackageInfo.DatasetMaxQuantFractionNumber[datasetId];

                            var fractionNumber = fractionNumberFromPackageComment > 0
                                ? fractionNumberFromPackageComment.ToString()
                                : fractionNumberFromExperiment;

                            fractionNodes.Add(new XElement("short", fractionNumber));

                            ptmNodes.Add(new XElement("boolean", "False"));
                            paramGroupIndexNodes.Add(new XElement("int", paramGroupIndex));
                            referenceChannelNodes.Add(new XElement("string", string.Empty));
                        }
                    }

                    var parameterGroupNodesUpdated = false;

                    if (dataPackageDatasetsByParamGroup.Count > 0)
                    {
                        // Remove any extra nodes (this likely won't be needed since most of the MaxQuant parameter files will have just one <parameterGroup> node)
                        while (parameterGroupNodes.Count > dataPackageDatasetsByParamGroup.Count)
                        {
                            parameterGroupNodes.RemoveAt(parameterGroupNodes.Count - 1);
                            parameterGroupNodesUpdated = true;
                        }

                        // Add additional <parameterGroup></parameterGroup> nodes, if necessary
                        while (parameterGroupNodes.Count < dataPackageDatasetsByParamGroup.Count)
                        {
                            parameterGroupNodes.Add(parameterGroupNodes[0]);
                            parameterGroupNodesUpdated = true;
                        }
                    }

                    // Replace the first FastaFileInfo node
                    // Remove any extra nodes
                    ReplaceChildNodes(doc, "fastaFiles", fastaFileNodes);

                    // Replace the file path
                    ReplaceChildNodes(doc, "filePaths", filePathNodes);

                    // Replace the experiment nodes
                    ReplaceChildNodes(doc, "experiments", experimentNodes);

                    // Replace the fraction nodes
                    ReplaceChildNodes(doc, "fractions", fractionNodes);

                    // Replace the PTM nodes
                    // ReSharper disable once StringLiteralTypo
                    ReplaceChildNodes(doc, "ptms", ptmNodes);

                    if (parameterGroupNodesUpdated)
                    {
                        // Replace the parameter group nodes
                        ReplaceChildNodes(doc, "parameterGroups", parameterGroupNodes);
                    }

                    // Replace the param group index nodes
                    ReplaceChildNodes(doc, "paramGroupIndices", paramGroupIndexNodes);

                    // Replace the reference channel nodes
                    ReplaceChildNodes(doc, "referenceChannel", referenceChannelNodes);

                    // Create the updated XML file
                    var settings = new XmlWriterSettings
                    {
                        Indent = true,
                        IndentChars = "   ",
                        OmitXmlDeclaration = false
                    };

                    using var outStream = new FileStream(updatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
                    using var writer = XmlWriter.Create(outStream, settings);

                    doc.Save(writer);
                }

                // Replace the original parameter file with the updated one
                sourceFile.Delete();
                updatedFile.MoveTo(Path.Combine(mWorkDir, paramFileName));

                RuntimeOptions.ParameterFilePath = updatedFile.FullName;

                // Determine the step range to use for the current step tool
                return ValidateStepRange(dmsSteps);
            }
            catch (Exception ex)
            {
                LogError("Error in UpdateMaxQuantParameterFileMetadata", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private CloseOutType UpdateMaxQuantParameterFileStartStepIDs(IReadOnlyDictionary<int, DmsStepInfo> dmsSteps)
        {
            var parameterFile = new FileInfo(RuntimeOptions.ParameterFilePath);
            var result = UpdateMaxQuantParameterFileStartStepIDs(mDatasetName, parameterFile, dmsSteps, out var errorMessage);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
                LogError(errorMessage);

            return result;
        }

        internal static CloseOutType UpdateMaxQuantParameterFileStartStepIDs(
            string datasetName,
            FileInfo parameterFileToUpdate,
            IReadOnlyDictionary<int, DmsStepInfo> dmsSteps,
            out string errorMessage)
        {
            errorMessage = string.Empty;

            try
            {
                var originalParameterFilePath = parameterFileToUpdate.FullName;
                var updatedFile = new FileInfo(parameterFileToUpdate.FullName + "_NewID.xml");

                using (var reader = new StreamReader(new FileStream(parameterFileToUpdate.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    // Note that XDocument supersedes XmlDocument and XPathDocument
                    // XDocument can often be easier to use since XDocument is LINQ-based

                    var doc = XDocument.Parse(reader.ReadToEnd());

                    var dmsStepNodes = doc.Elements("MaxQuantParams").Elements("dmsSteps").Elements("step").ToList();

                    if (dmsStepNodes.Count == 0)
                    {
                        if (!Global.IsMatch(datasetName, AnalysisResources.AGGREGATION_JOB_DATASET))
                            return CloseOutType.CLOSEOUT_SUCCESS;

                        errorMessage = "MaxQuant parameter file does not have a dmsSteps section; this is required for data-package based MaxQuant tasks";
                        {
                            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                        }
                    }

                    foreach (var stepNode in dmsStepNodes)
                    {
                        if (!Global.TryGetAttribute(stepNode, "id", out var stepIdText))
                        {
                            errorMessage = "DMS step in the MaxQuant parameter file is missing the 'id' attribute";
                            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                        }

                        if (!int.TryParse(stepIdText, out var stepId))
                        {
                            errorMessage = string.Format("DMS step in the MaxQuant parameter file has a non-numeric step ID value of '{0}", stepIdText);
                            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                        }

                        var stepMatched = false;

                        foreach (var dmsStep in dmsSteps)
                        {
                            if (dmsStep.Key != stepId)
                                continue;

                            var startStepIdAttribute = stepNode.Attribute("startStepID");

                            if (startStepIdAttribute == null)
                            {
                                errorMessage = string.Format("DMS step {0} in the MaxQuant parameter file is missing attribute startStepID", stepId);
                                return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                            }

                            startStepIdAttribute.Value = dmsStep.Value.StartStepID.ToString();
                            stepMatched = true;
                            break;
                        }

                        if (!stepMatched)
                        {
                            errorMessage = string.Format("DMS step {0} not found in the dmsSteps dictionary", stepId);
                            return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                        }
                    }

                    // Create the updated XML file
                    var settings = new XmlWriterSettings
                    {
                        Indent = true,
                        IndentChars = "   ",
                        OmitXmlDeclaration = false
                    };

                    using var outStream = new FileStream(updatedFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
                    using var writer = XmlWriter.Create(outStream, settings);

                    doc.Save(writer);
                }

                // Replace the original parameter file with the updated one
                parameterFileToUpdate.Delete();
                updatedFile.MoveTo(originalParameterFilePath);

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                errorMessage = "Error in UpdateMaxQuantParameterFileStartStepIDs: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        // ReSharper disable once CommentTypo
        /// <summary>
        /// Fix paths in the several Andromeda metadata files (aplfiles, secpepFiles, and the Andromeda parameter files)
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType UpdateMaxQuantRuntimeFilePaths()
        {
            try
            {
                var workingDirectory = new DirectoryInfo(mWorkDir);

                var andromedaDirectory = new DirectoryInfo(Path.Combine(workingDirectory.FullName, "combined", "andromeda"));

                if (!andromedaDirectory.Exists)
                {
                    // Nothing to update, meaning this is the first MaxQuant job step
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                var peakListMetadataFile = new FileInfo(Path.Combine(andromedaDirectory.FullName, "aplfiles"));
                var secondarySearchMetadataFile = new FileInfo(Path.Combine(andromedaDirectory.FullName, "secpepFiles"));

                if (!peakListMetadataFile.Exists)
                {
                    LogWarning("Andromeda directory exists but the Andromeda peak list file was not found; this is unexpected: " + peakListMetadataFile.FullName);
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                var metadataFiles = new List<FileInfo> {
                    peakListMetadataFile
                };

                if (secondarySearchMetadataFile.Exists)
                {
                    metadataFiles.Add(secondarySearchMetadataFile);
                }

                var andromedaParameterFiles = new SortedSet<string>();

                foreach (var metadataFile in metadataFiles)
                {
                    var success = UpdateAndromedaPeakListFile(workingDirectory, metadataFile, andromedaParameterFiles);

                    if (!success)
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                // Update the "fasta file path" entry in every Andromeda parameter file in the working directory or below
                return UpdateAndromedaParameterFiles(workingDirectory, andromedaParameterFiles);
            }
            catch (Exception ex)
            {
                LogError("Error in UpdateMaxQuantRuntimeFilePaths", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool ValidateFastaFile()
        {
            // Define the path to the FASTA file
            var localOrgDbDirectory = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);
            var generatedFastaFileName = mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME);

            // Note that job parameter "GeneratedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
            var fastaFilePath = Path.Combine(localOrgDbDirectory, generatedFastaFileName);

            var fastaFile = new FileInfo(fastaFilePath);

            if (!fastaFile.Exists)
            {
                // FASTA file not found
                LogError("FASTA file not found: " + fastaFile.Name, "FASTA file not found: " + fastaFile.FullName);
                return false;
            }

            var proteinOptions = mJobParams.GetParam("ProteinOptions");

            if (!string.IsNullOrEmpty(proteinOptions) && proteinOptions.IndexOf("seq_direction=decoy", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // The FASTA file has decoy sequences
                LogError("Protein options for this analysis job must contain seq_direction=forward, not seq_direction=decoy " +
                         "(since MaxQuant will auto-add decoy sequences)");
                return false;
            }

            mLocalFASTAFilePath = fastaFile.FullName;

            return true;
        }

        /// <summary>
        /// Validate that each list in the dictionary has at least one item
        /// </summary>
        /// <param name="nodesToValidate">Keys are a description of the item, values the number of child nodes of each parent</param>
        /// <returns>True if every item is valid, otherwise false</returns>
        private bool ValidateNodesPresent(Dictionary<string, int> nodesToValidate)
        {
            foreach (var item in nodesToValidate.Where(item => item.Value == 0))
            {
                LogError(string.Format("MaxQuant parameter file does not have a '{0}' element with one or more child nodes; this is required", item.Key));
                return false;
            }

            return true;
        }

        /// <summary>
        /// Validate the step range, updating RuntimeOptions.StartStepNumber and RuntimeOptions.EndStepNumber
        /// This will involve a dry-run of MaxQuant if startStepID values in the dmsSteps elements are "auto" instead of integers
        /// </summary>
        /// <param name="dmsSteps">Keys are step IDs, values are step info</param>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType ValidateStepRange(IReadOnlyDictionary<int, DmsStepInfo> dmsSteps)
        {
            const string FINISH_WRITING_TABLES = "Finish writing tables";

            RuntimeOptions.StepRangeValidated = false;

            try
            {
                if (dmsSteps.Count == 0)
                {
                    // All steps will be run
                    RuntimeOptions.StartStepNumber = 0;
                    RuntimeOptions.EndStepNumber = MaxQuantRuntimeOptions.MAX_STEP_NUMBER;
                    RuntimeOptions.StepRangeValidated = true;

                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                RuntimeOptions.DryRun = false;

                foreach (var dmsStep in dmsSteps.Where(item => !item.Value.StartStepID.HasValue))
                {
                    // In the MaxQuant parameter file, DMS step 1 has an undefined startStepID; running a dry run of MaxQuant to determine step IDs
                    LogMessage(string.Format(
                        "In the MaxQuant parameter file, DMS step {0} has an undefined startStepID; " +
                        "running a dry run of MaxQuant to determine step IDs", dmsStep.Key));

                    RuntimeOptions.DryRun = true;
                    break;
                }

                bool usedDryRun;

                if (RuntimeOptions.DryRun)
                {
                    var result = StartMaxQuant();
                    RuntimeOptions.DryRun = false;

                    usedDryRun = true;

                    if (result != CloseOutType.CLOSEOUT_SUCCESS)
                        return result;

                    foreach (var dmsStep in dmsSteps)
                    {
                        foreach (var stepID in StepToTaskMap.Keys.Where(stepID => StepToTaskMap[stepID].Equals(dmsStep.Value.StartStepName)))
                        {
                            dmsStep.Value.StartStepID = stepID;
                            break;
                        }
                    }

                    // Look for unresolved steps
                    var unresolvedStepCount = 0;

                    foreach (var dmsStep in dmsSteps.Where(dmsStep => !dmsStep.Value.StartStepID.HasValue))
                    {
                        LogError("In the MaxQuant parameter file, DMS step {0} has startStepName '{1}', " +
                                 "which did not match any of the task messages shown by MaxQuant during the dry run", dmsStep.Key, dmsStep.Value.StartStepName);

                        unresolvedStepCount++;
                    }

                    if (unresolvedStepCount > 0)
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    // Assure that the final step appeared in the DryRun console output
                    if (!StepToTaskMap.Values.Contains(FINISH_WRITING_TABLES))
                    {
                        LogError(string.Format("MaxQuant dry run did not include step '{0}'; aborting", FINISH_WRITING_TABLES));
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                else
                {
                    usedDryRun = false;
                }

                var dataSourceDescription = usedDryRun ? "console output from the MaxQuant dry run" : "MaxQuant parameter file";

                // Verify that StartStepID is now defined for each of the steps

                // Additionally, populate several properties in RuntimeOptions:
                //   StartStepNumber
                //   EndStepNumber
                //   StartStepName
                //   NextDMSStepStartStepName

                foreach (var dmsStep in dmsSteps)
                {
                    if (!dmsStep.Value.StartStepID.HasValue)
                    {
                        LogError("In the {0}, DMS step {1} (with startStepName '{2}') " +
                                 "does not have an integer defined for StartStepID, indicating an unresolved start step name", dataSourceDescription, dmsStep.Key, dmsStep.Value.StartStepName);

                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (!dmsStep.Value.Tool.Equals(StepToolName, StringComparison.OrdinalIgnoreCase))
                        continue;

                    RuntimeOptions.StartStepName = dmsStep.Value.StartStepName;
                    RuntimeOptions.StartStepNumber = dmsStep.Value.StartStepID.Value;

                    var nextStepID = dmsStep.Key + 1;

                    if (dmsSteps.TryGetValue(nextStepID, out var nextDmsStep))
                    {
                        if (!nextDmsStep.StartStepID.HasValue)
                        {
                            LogError("In the {0}, DMS step {1} (with startStepName '{2}') " +
                                     "does not have an integer defined for StartStepID, indicating an unresolved start step name", dataSourceDescription, nextStepID, nextDmsStep.StartStepName);

                            return CloseOutType.CLOSEOUT_FAILED;
                        }

                        RuntimeOptions.NextDMSStepStartStepName = nextDmsStep.StartStepName;
                        RuntimeOptions.EndStepNumber = nextDmsStep.StartStepID.Value - 1;
                    }
                    else
                    {
                        RuntimeOptions.NextDMSStepStartStepName = string.Empty;
                        RuntimeOptions.EndStepNumber = MaxQuantRuntimeOptions.MAX_STEP_NUMBER;
                    }

                    RuntimeOptions.StepRangeValidated = true;
                    break;
                }

                if (!RuntimeOptions.StepRangeValidated)
                {
                    LogError(string.Format(
                        "In the MaxQuant parameter file, none of the DMS steps matched the current step tool name: {0}",
                        StepToolName));

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!usedDryRun)
                {
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                // Update the parameter file to switch from startStepID="auto" to startStepID="1"
                return UpdateMaxQuantParameterFileStartStepIDs(dmsSteps);
            }
            catch (Exception ex)
            {
                LogError("Exception validating step ranges in the MaxQuant parameter file", ex);
                return CloseOutType.CLOSEOUT_FAILED;
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

            ParseConsoleOutputFile();

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("MaxQuant");
        }
    }
}
