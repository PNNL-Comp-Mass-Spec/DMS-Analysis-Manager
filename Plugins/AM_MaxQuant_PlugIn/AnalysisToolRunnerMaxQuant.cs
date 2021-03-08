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
using System.Xml;
using System.Xml.Linq;

namespace AnalysisManagerMaxQuantPlugIn
{
    // Ignore Spelling: Quant, deisotoping, apl

    /// <summary>
    /// Class for running MaxQuant analysis
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerMaxQuant : AnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        private const string MAXQUANT_CONSOLE_OUTPUT = "MaxQuant_ConsoleOutput.txt";

        private const string MAXQUANT_EXE_NAME = @"bin\MaxQuantCmd.exe";

        /// <summary>
        /// Percent complete to report when the tool starts
        /// </summary>
        public const float PROGRESS_PCT_TOOL_RUNNER_STARTING = 5;

        private const float PROGRESS_PCT_COMPLETE = 99;

        #endregion

        #region "Module Variables"

        private string mMaxQuantProgLoc;
        private string mConsoleOutputErrorMsg;

        private string mLocalFASTAFilePath;

        private DateTime mLastConsoleOutputParse;

        private RunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

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

                var result = UpdateMaxQuantParameterFile(
                    dataPackageInfo, out var parameterFilePath, out var startStepNumber, out var endStepNumber);

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    return result;

                // Process one or more datasets using MaxQuant
                var processingResult = StartMaxQuant(parameterFilePath, startStepNumber, endStepNumber);

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

                var success = CopyResultsToTransferDirectory();
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

        private bool GetDmsStepDetails(XElement item, out DmsStepInfo dmsStepInfo)
        {
            if (!TryGetAttribute(item, "id", out var stepIdText))
            {
                LogError("DMS step in the MaxQuant parameter file is missing the 'id' attribute");
                dmsStepInfo = new DmsStepInfo(0);
                return false;
            }

            if (!TryGetAttribute(item, "tool", out var stepToolName))
            {
                LogError("DMS step in the MaxQuant parameter file is missing the 'tool' attribute");
                dmsStepInfo = new DmsStepInfo(0);
                return false;
            }

            if (!TryGetAttribute(item, "startStepName", out var startStepName))
            {
                LogError("DMS step in the MaxQuant parameter file is missing the 'startStepName' attribute");
                dmsStepInfo = new DmsStepInfo(0);
                return false;
            }

            if (!TryGetAttribute(item, "startStepID", out var startStepIDText))
            {
                LogError("DMS step in the MaxQuant parameter file is missing the 'startStepID' attribute");
                dmsStepInfo = new DmsStepInfo(0);
                return false;
            }

            if (!int.TryParse(stepIdText, out var stepId))
            {
                LogError(string.Format("DMS step in the MaxQuant parameter file has a non-numeric step ID value of '{0}", stepIdText));
                dmsStepInfo = new DmsStepInfo(0);
                return false;
            }

            dmsStepInfo = new DmsStepInfo(stepId) {
                Tool = stepToolName,
                StartStepName = startStepName,
            };

            if (!startStepIDText.Equals("auto") && int.TryParse(startStepIDText, out var startStepID))
            {
                dmsStepInfo.StartStepID = startStepID;
            }

            return true;
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

        /// <summary>
        /// Parse the MaxQuant console output file to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example Console output
            //
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

            var processingSteps = new SortedList<string, int>
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

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    foreach (var processingStep in processingSteps.Where(processingStep => dataLine.StartsWith(processingStep.Key, StringComparison.OrdinalIgnoreCase)))
                    {
                        currentProgress = processingStep.Value;
                    }

                    if (dataLine.IndexOf("exception", StringComparison.OrdinalIgnoreCase) >= 0 &&
                        string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                    {
                        mConsoleOutputErrorMsg = "Error running MaxQuant: " + dataLine;
                    }
                }

                mProgress = currentProgress;
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private CloseOutType StartMaxQuant(string parameterFilePath, int startStepNumber, int endStepNumber)
        {
            LogMessage("Running MaxQuant");

            if (string.IsNullOrWhiteSpace(parameterFilePath))
            {
                LogError("MaxQuant parameter file name returned by UpdateMaxQuantParameterFile is empty");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Set up and execute a program runner to run MaxQuant
            var arguments = string.Format("--partial-processing={0} --partial-processing-end={1} {2}", startStepNumber, endStepNumber, parameterFilePath);

            LogDebug(mMaxQuantProgLoc + " " + arguments);

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = Path.Combine(mWorkDir, MAXQUANT_CONSOLE_OUTPUT)
            };
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mProgress = PROGRESS_PCT_TOOL_RUNNER_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var processingSuccess = mCmdRunner.RunProgram(mMaxQuantProgLoc, arguments, "MaxQuant", true);

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

            // Validate MaxQuant outputs ...

            mStatusTools.UpdateAndWrite(mProgress);
            if (mDebugLevel >= 3)
            {
                LogDebug("MaxQuant Search Complete");
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private CloseOutType UpdateMaxQuantParameterFile(
            DataPackageInfo dataPackageInfo,
            out string paramFilePath,
            out int startStepNumber,
            out int endStepNumber)
        {
            paramFilePath = string.Empty;
            startStepNumber = 0;
            endStepNumber = 9999;

            try
            {
                var paramFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);
                var sourceFile = new FileInfo(Path.Combine(mWorkDir, paramFileName));
                var updatedFile = new FileInfo(Path.Combine(mWorkDir, paramFileName + ".new"));

                var numThreadsToUse = GetNumThreadsToUse();
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

                    var filePathNodes = doc.Elements("MaxQuantParams").Elements("filePaths").ToList();

                    var experimentNodes = doc.Elements("MaxQuantParams").Elements("experiments").ToList();

                    var fractionNodes = doc.Elements("MaxQuantParams").Elements("fractions").ToList();

                    // Update the thread count
                    if (numThreadsNode.Count == 0)
                    {
                        LogError("MaxQuant parameter file does not have 'numThreads' element; this is required");
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }

                    if (fastaFileNodes.Count == 0)
                    {
                        LogError("MaxQuant parameter file does not have 'fastaFiles' element; this is required");
                        return CloseOutType.CLOSEOUT_NO_PARAM_FILE;
                    }

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

                    filePathNodes.Clear();
                    experimentNodes.Clear();

                    // If the experiment ends in _f22, assume this is a fractionated sample and this is fraction 22
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

                paramFilePath = updatedFile.FullName;

                // ToDo: Determine the step range to use for the current step tool
                // If the items in dmsSteps all have numeric values for StartStepID, make sure the step range is contiguous

                // Otherwise, run MaxQuant with DryRun then correlate with StartStepName values in dmsSteps
                // If we use DryRun, update the parameter file to switch from startStepID="auto" to startStepID="1"

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Exception updating the MaxQuant parameter file", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
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

        private static bool TryGetAttribute(XElement item, string attributeName, out string attributeValue)
        {
            if (!item.HasAttributes)
            {
                attributeValue = string.Empty;
                return false;
            }

            var attribute = item.Attribute(attributeName);

            if (attribute == null)
            {
                attributeValue = string.Empty;
                return false;
            }

            attributeValue = attribute.Value;
            return true;
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

        #endregion

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

            ParseConsoleOutputFile(Path.Combine(mWorkDir, MAXQUANT_CONSOLE_OUTPUT));

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("MaxQuant");
        }

        #endregion
    }
}
