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
using AnalysisManagerBase.FileAndDirectoryTools;

namespace AnalysisManagerMaxQuantPlugIn
{
    // Ignore Spelling: Quant, deisotoping, apl, dryrun, proc, txt

    /// <summary>
    /// Class for running MaxQuant analysis
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerMaxQuant : AnalysisToolRunnerBase
    {
        private const string MAXQUANT_EXE_NAME = @"bin\MaxQuantCmd.exe";

        /// <summary>
        /// Percent complete to report when the tool starts
        /// </summary>
        public const float PROGRESS_PCT_TOOL_RUNNER_STARTING = 5;

        private const float PROGRESS_PCT_COMPLETE = 99;

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

                var dataPackageInfo = new DataPackageInfo(dataPackageID, this);

                // Customize the path to the FASTA file, the number of threads to use, the dataset files, etc.
                // This will involve a dry-run of MaxQuant if startStepID values in the dmsSteps elements are "auto" instead of integers

                var result = UpdateMaxQuantParameterFileMetadata(dataPackageInfo);

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    return result;

                if (!RuntimeOptions.StepRangeValidated)
                {
                    LogError("Aborting since the MaxQuant step range was not validated");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Process one or more datasets using MaxQuant
                var processingResult = StartMaxQuant();

                if (processingResult == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    processingResult = PostProcessMaxQuantResults();
                }

                mProgress = PROGRESS_PCT_COMPLETE;

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

                var success = CopyResultsToTransferDirectory(true);
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

        private bool FindDirectoriesToSkip(
            SubdirectoryFileCompressor subdirectoryCompressor,
            out List<DirectoryInfo> directoriesToSkip)
        {
            directoriesToSkip = new List<DirectoryInfo>();

            try
            {
                foreach (var subdirectory in subdirectoryCompressor.WorkingDirectory.GetDirectories("*", SearchOption.AllDirectories))
                {
                    if (subdirectory.Parent == null)
                    {
                        LogError("Unable to determine the parent directory of " + subdirectory.FullName);
                        return false;
                    }

                    if (RuntimeOptions.EndStepNumber < MaxQuantRuntimeOptions.MAX_STEP_NUMBER)
                    {
                        var isUnchanged = subdirectoryCompressor.UnchangedDirectories.Any(item => item.FullName.Equals(subdirectory.FullName));

                        if (isUnchanged)
                            directoriesToSkip.Add(subdirectory);

                        continue;
                    }

                    // skip all except the txt and proc directories below the combined subdirectory
                    if (subdirectory.Parent.Name.Equals("combined", StringComparison.OrdinalIgnoreCase) &&
                        (subdirectory.Name.Equals("proc", StringComparison.OrdinalIgnoreCase) ||
                         subdirectory.Name.Equals("txt", StringComparison.OrdinalIgnoreCase)))
                    {
                        // Zip this directory
                        continue;
                    }

                    directoriesToSkip.Add(subdirectory);
                }

                mJobParams.AddResultFileToSkip(SubdirectoryFileCompressor.WORKING_DIRECTORY_METADATA_FILE);
                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in MaxQuantPlugin->FindDirectoriesToSkip", ex);
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
        /// <param name="processingSteps">Keys are step name, values are the approximate percent complete at the start of the step</param>
        /// <param name="progressAtStart">Output: progress at start</param>
        /// <param name="progressAtEnd">Output: progress at end</param>
        private void GetIncrementalProgressRange(SortedList<string, int> processingSteps, out float progressAtStart, out float progressAtEnd)
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
                {"Configuring", 0},
                {"Feature detection", 1},
                {"Deisotoping", 3},
                {"MS/MS preparation", 4},
                {"Combining apl files for first search", 5},
                {"Preparing searches", 6},
                {"MS/MS first search", 34},
                {"Read search results for recalibration", 50},
                {"Mass recalibration", 51},
                {"Calculating masses", 54},
                {"MS/MS preparation for main search", 54},
                {"Combining apl files for main search", 56},
                {"MS/MS main search", 56},
                {"Preparing combined folder ", 70},
                {"Correcting errors", 70},
                {"Reading search engine results", 70},
                {"Preparing reverse hits", 71},
                {"Finish search engine results", 72},
                {"Filter identifications (MS/MS)", 72},
                {"Calculating PEP", 72},
                {"Copying identifications", 73},
                {"Applying FDR", 73},
                {"Assembling second peptide MS/MS", 73},
                {"Combining second peptide files", 74},
                {"Second peptide search", 74},
                {"Reading search engine results (SP)", 87},
                {"Finish search engine results (SP)", 88},
                {"Filtering identifications (SP)", 88},
                {"Applying FDR (SP)", 89},
                {"Re-quantification", 89},
                {"Reporter quantification", 89},
                {"Prepare protein assembly", 89},
                {"Assembling proteins", 90},
                {"Assembling unidentified peptides", 91},
                {"Finish protein assembly", 91},
                {"Updating identifications", 95},
                {"Estimating complexity", 95},
                {"Prepare writing tables ", 95},
                {"Writing tables", 95},
                {"Finish writing tables ", 99}
            };
        }

        /// <summary>
        /// Determine the number of threads to use for MaxQuant
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

        private CloseOutType PostProcessMaxQuantResults()
        {
            try
            {
                var workingDirectory = new DirectoryInfo(mWorkDir);

                // Keys are DirectoryInfo instances
                // Values are true if the directory's files should be zipped, or false if they should be left alone
                var directoriesToZipSubsSeparately = new Dictionary<DirectoryInfo, bool>();

                var combinedDirectory = new DirectoryInfo(Path.Combine(workingDirectory.FullName, "combined"));
                if (combinedDirectory.Exists)
                {
                    directoriesToZipSubsSeparately.Add(combinedDirectory, false);

                    // Rename the #runningTimes.txt file in the proc directory
                    var procDirectory = new DirectoryInfo(Path.Combine(combinedDirectory.FullName, "proc"));
                    if (procDirectory.Exists)
                    {
                        foreach (var item in procDirectory.GetFiles("#runningTimes.txt"))
                        {
                            var updatedRunningTimesPath = Path.Combine(procDirectory.FullName, string.Format("#runningTimes_{0}.txt", StepToolName));
                            item.MoveTo(updatedRunningTimesPath);
                            break;
                        }
                    }
                }

                var subdirectoryCompressor = new SubdirectoryFileCompressor(workingDirectory, mDebugLevel);
                RegisterEvents(subdirectoryCompressor);

                // Examine subdirectory names to determine which ones should be zipped and copied to the transfer directory
                // Skip any that do not have any changed files

                // Also, if RuntimeOptions.EndStepNumber >= MaxQuantRuntimeOptions.MAX_STEP_NUMBER,
                // skip all except the txt and proc directories below the combined subdirectory

                var findUnchangedSuccess = subdirectoryCompressor.FindUnchangedDirectories();

                if (!findUnchangedSuccess)
                {
                    LogWarning("SubdirectoryFileCompressor->FindUnchangedDirectories returned false");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = FindDirectoriesToSkip(subdirectoryCompressor, out var directoriesToSkip);

                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                var successZipping = subdirectoryCompressor.ZipDirectories(directoriesToSkip, directoriesToZipSubsSeparately);
                if (!successZipping)
                    return CloseOutType.CLOSEOUT_ERROR_ZIPPING_FILE;

                return CloseOutType.CLOSEOUT_SUCCESS;
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
            //
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

                mProgress = PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.ComputeIncrementalProgress(progressAtStart, progressAtEnd, currentProgress);

                if (!RuntimeOptions.DryRun)
                    return;

                StepToTaskMap.Clear();

                foreach (var item in stepToTaskMap.Where(item => !StepToTaskMap.ContainsKey(item.Key)))
                {
                    StepToTaskMap.Add(item.Key, item.Value);
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate("Error parsing console output file (" + RuntimeOptions.ConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

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
                LogError("Error running MaxQuant");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("MaxQuant returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to MaxQuant failed (but exit code is 0)");
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

            var success = StoreDotNETToolVersionInfo(mMaxQuantProgLoc, additionalDLLs);
            return success;
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
                var dmsSteps = new Dictionary<int, DmsStepInfo>();

                using (var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    // Note that XDocument supersedes XmlDocument and XPathDocument
                    // XDocument can often be easier to use since XDocument is LINQ-based

                    var doc = XDocument.Parse(reader.ReadToEnd());

                    var dmsStepNodes = doc.Elements("MaxQuantParams").Elements("dmsSteps").Elements("step").ToList();

                    if (dmsStepNodes.Count == 0)
                    {
                        if (mDatasetName.Equals("Aggregation"))
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

                    var nodesToValidate = new Dictionary<string, int>
                    {
                        {"fastaFiles", fastaFileNodes.Count},
                        {"numThreads", numThreadsNode.Count},
                        {"filePaths", filePathChildCount},
                        {"experiments", experimentChildCount},
                        {"fractions", fractionChildCount}
                    };

                    if (!ValidateNodesPresent(nodesToValidate))
                    {
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }

                    // Update the thread count
                    numThreadsNode[0].Value = numThreadsToUse.ToString();

                    fastaFileNodes.Clear();

                    var fastaFileInfoNode = new XElement("FastaFileInfo");
                    fastaFileInfoNode.Add(new XElement("fastaFilePath", mLocalFASTAFilePath));
                    fastaFileInfoNode.Add(new XElement("identifierParseRule", ">([^ ]+)"));
                    fastaFileInfoNode.Add(new XElement("descriptionParseRule", ">([^ ]+) *(.*)"));
                    fastaFileInfoNode.Add(new XElement("taxonomyParseRule", string.Empty));
                    fastaFileInfoNode.Add(new XElement("variationParseRule", @">[^\s]+\s+(.+)"));
                    fastaFileInfoNode.Add(new XElement("modificationParseRule", string.Empty));
                    fastaFileInfoNode.Add(new XElement("taxonomyId", -1));

                    fastaFileNodes.Add(fastaFileInfoNode);

                    var filePathNodes = new List<XElement>();
                    var experimentNodes = new List<XElement>();
                    var fractionNodes = new List<XElement>();

                    // If the experiment ends in text similar to "_f22", assume this is a fractionated sample and this is fraction 22
                    var fractionMatcher = new Regex(@"_f(?<FractionNumber>\d+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                    foreach (var item in dataPackageInfo.Datasets)
                    {
                        var datasetId = item.Key;

                        var datasetFileOrDirectoryName = dataPackageInfo.DatasetFiles[datasetId];
                        var experiment = dataPackageInfo.Experiments[datasetId];

                        filePathNodes.Add(new XElement("string", Path.Combine(mWorkDir, datasetFileOrDirectoryName)));

                        experimentNodes.Add(new XElement("string", experiment));

                        var match = fractionMatcher.Match(experiment);

                        var fractionNumber = match.Success ? match.Groups["FractionNumber"].Value : "32767";
                        fractionNodes.Add(new XElement("short", fractionNumber));
                    }

                    // Replace the first FastaFileInfo node
                    // Remove any extra nodes
                    ReplaceChildNodes(doc, "fastaFiles", fastaFileNodes);

                    // Replace the file path
                    ReplaceChildNodes(doc, "filePaths", filePathNodes);

                    // Replace the experiment nodes
                    ReplaceChildNodes(doc, "experiments", experimentNodes);

                    // Replace the fraction number
                    ReplaceChildNodes(doc, "fractions", fractionNodes);

                    // Create the updated XML file
                    var settings = new XmlWriterSettings
                    {
                        Indent = true,
                        IndentChars = "   ",
                        OmitXmlDeclaration = true
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
                LogError("Exception in UpdateMaxQuantParameterFileMetadata", ex);
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
                        if (!datasetName.Equals("Aggregation"))
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
                errorMessage = "Exception in UpdateMaxQuantParameterFileStartStepIDs: " + ex.Message;
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool ValidateFastaFile()
        {
            // Define the path to the fasta file
            var localOrgDbFolder = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);

            // Note that job parameter "generatedFastaName" gets defined by AnalysisResources.RetrieveOrgDB
            var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam("PeptideSearch", AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME));

            var fastaFile = new FileInfo(fastaFilePath);

            if (!fastaFile.Exists)
            {
                // Fasta file not found
                LogError("Fasta file not found: " + fastaFile.Name, "Fasta file not found: " + fastaFile.FullName);
                return false;
            }

            var proteinOptions = mJobParams.GetParam("ProteinOptions");
            if (!string.IsNullOrEmpty(proteinOptions))
            {
                if (proteinOptions.IndexOf("seq_direction=decoy", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    // The FASTA file has decoy sequences
                    LogError("Protein options for this analysis job must contain seq_direction=forward, not seq_direction=decoy " +
                             "(since MaxQuant will auto-add decoy sequences)");
                    return false;
                }
            }

            mLocalFASTAFilePath = fastaFile.FullName;

            return true;
        }

        /// <summary>
        /// Validate that each list in the dictionary has at least one item
        /// </summary>
        /// <param name="nodesToValidate">Keys are a description of the item, values the number of child nodes of each parent</param>
        /// <returns>True all of the items are valid, otherwise false</returns>
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
                        LogError(string.Format(
                            "In the MaxQuant parameter file, DMS step {0} has startStepName '{1}', " +
                            "which did not match any of the tasks messages shown by MaxQuant during the dry run",
                            dmsStep.Key, dmsStep.Value.StartStepName));

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
                        LogError(string.Format(
                            "In the {0}, DMS step {1} (with startStepName '{2}') " +
                            "does not have an integer defined for StartStepID, indicating an unresolved start step name",
                            dataSourceDescription, dmsStep.Key, dmsStep.Value.StartStepName));

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
                            LogError(string.Format(
                                "In the {0}, DMS step {1} (with startStepName '{2}') " +
                                "does not have an integer defined for StartStepID, indicating an unresolved start step name",
                                dataSourceDescription, nextStepID, nextDmsStep.StartStepName));

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

        #region "Event Handlers"

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

            ParseConsoleOutputFile();

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("MaxQuant");
        }

        #endregion
    }
}
