using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerMSAlignQuantPlugIn
{
    /// <summary>
    /// Class for running TargetedQuant
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerMSAlignQuant : AnalysisToolRunnerBase
    {
        // Ignore Spelling: Acetylation, Da, Phosphorylation, Pyroglutomate, quant, quantitation, Workflow

        private const string TARGETED_QUANT_XML_FILE_NAME = "TargetedWorkflowParams.xml";
        private const string TARGETED_WORKFLOWS_CONSOLE_OUTPUT = "TargetedWorkflow_ConsoleOutput.txt";
        private const int PROGRESS_PCT_CREATING_PARAMETERS = 5;

        private const int PROGRESS_TARGETED_WORKFLOWS_STARTING = 10;
        private const int PROGRESS_TARGETED_WORKFLOWS_CREATING_XIC = 11;
        private const int PROGRESS_TARGETED_WORKFLOWS_XIC_CREATED = 15;
        private const int PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED = 15;
        private const int PROGRESS_TARGETED_WORKFLOWS_PROCESSING_COMPLETE = 95;

        private const int PROGRESS_PCT_COMPLETE = 99;

        private string mConsoleOutputErrorMsg;

        private string mTargetedWorkflowsProgLoc;
        private Dictionary<string, int> mConsoleOutputProgressMap;

        private RunDosProgram mCmdRunner;

        /// <summary>
        /// Runs TargetedWorkFlowConsole tool
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
                    LogDebug("AnalysisToolRunnerMSAlignQuant.RunTool(): Enter");
                }

                // Determine the path to the TargetedWorkflowConsole.exe program
                // Note that it is part of project DeconTools.Workflows
                mTargetedWorkflowsProgLoc = DetermineProgramLocation("TargetedWorkflowsProgLoc", "TargetedWorkflowConsole.exe");

                if (string.IsNullOrWhiteSpace(mTargetedWorkflowsProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the TargetedWorkflowsConsole version info in the database
                if (!StoreToolVersionInfo(mTargetedWorkflowsProgLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining TargetedWorkflowsConsole version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Create the TargetedWorkflowParams.xml file
                mProgress = PROGRESS_PCT_CREATING_PARAMETERS;

                var targetedQuantParamFilePaths = CreateTargetedQuantParamFiles();

                if (targetedQuantParamFilePaths.Count == 0)
                {
                    LogError("Aborting since CreateTargetedQuantParamFile returned an empty list");

                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Error creating " + TARGETED_QUANT_XML_FILE_NAME;
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var toolName = mJobParams.GetJobParameter("JobParameters", "ToolName", string.Empty);
                var runningTopPIC = toolName.StartsWith("TopPIC", StringComparison.OrdinalIgnoreCase);

                var successOverall = true;

                foreach (var item in targetedQuantParamFilePaths)
                {
                    var baseName = item.Key;
                    var targetedQuantParamFilePath = item.Value;

                    var processingSuccess = RunTargetedWorkflow(baseName, targetedQuantParamFilePath);

                    if (!processingSuccess)
                    {
                        if (runningTopPIC)
                        {
                            LogWarning("Ignoring Targeted Workflows error since this is a {0} job", toolName);
                        }
                        else
                        {
                            successOverall = false;
                        }
                    }
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                if (!successOverall)
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in MSAlignQuantPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Creates the targeted quant params XML file
        /// </summary>
        /// <returns>List of key value pairs where the key is the base name of the input file and the value is the XML parameter file with Targeted Workflow options</returns>
        private List<KeyValuePair<string, string>> CreateTargetedQuantParamFiles()
        {
            var targetedQuantParamFileInfo = new List<KeyValuePair<string, string>>();

            try
            {
                var psmResultFiles = mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, AnalysisResourcesMSAlignQuant.MSALIGN_QUANT_INPUT_FILE_NAME_PARAM, "");

                if (string.IsNullOrWhiteSpace(psmResultFiles))
                {
                    mMessage = NotifyMissingParameter(mJobParams, AnalysisJob.STEP_PARAMETERS_SECTION);
                    return targetedQuantParamFileInfo;
                }

                var psmResultFileNames = psmResultFiles.Split('\t').ToList();

                var fileNumber = 0;

                foreach (var psmResultsFileName in psmResultFileNames)
                {
                    // Optionally make a trimmed version of the PSM Results file for testing purposes

                    // var fullResultsPath = Path.Combine(mWorkDir, mSAlignResultTableName);
                    // var trimmedFilePath = Path.Combine(mWorkDir, Dataset + "_TrimmedResults.tmp");
                    //
                    // using (var reader = new StreamReader(new FileStream(fullResultsPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    // using (var writer = new StreamWriter(new FileStream(trimmedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    //{
                    //    var linesRead = 0;
                    //    while (!reader.EndOfStream && linesRead < 30)
                    //    {
                    //        writer.WriteLine(reader.ReadLine());
                    //        linesRead += 1;
                    //    }
                    //}
                    //
                    // // Replace the original file with the trimmed one
                    // Thread.Sleep(100);
                    // File.Delete(fullResultsPath);
                    // Thread.Sleep(100);
                    //
                    // File.Move(trimmedFilePath, fullResultsPath);

                    var workflowParamFileName = mJobParams.GetParam("MSAlignQuantParamFile");

                    if (string.IsNullOrEmpty(workflowParamFileName))
                    {
                        mMessage = NotifyMissingParameter(mJobParams, "MSAlignQuantParamFile");
                        return targetedQuantParamFileInfo;
                    }

                    fileNumber++;

                    // If psmResultFileNames only has a single file it will be named:
                    //   DatasetName_TopPIC_PrSMs

                    // If it has multiple files, they will be named:
                    //   DatasetName_0_TopPIC_PrSMs
                    //   DatasetName_1_TopPIC_PrSMs
                    //   DatasetName_2_TopPIC_PrSMs

                    string targetedQuantParamFileName;
                    string baseName;

                    if (psmResultFileNames.Count == 1)
                    {
                        targetedQuantParamFileName = TARGETED_QUANT_XML_FILE_NAME;
                        baseName = mDatasetName;
                    }
                    else
                    {
                        targetedQuantParamFileName = string.Format("{0}{1}.xml", Path.GetFileNameWithoutExtension(TARGETED_QUANT_XML_FILE_NAME), fileNumber);

                        baseName = psmResultsFileName.EndsWith("_TopPIC_PrSMs.txt", StringComparison.OrdinalIgnoreCase)
                            ? psmResultsFileName.Substring(0, psmResultsFileName.Length - "_TopPIC_PrSMs.txt".Length)
                            : string.Format("{0}_{1}", mDatasetName, fileNumber - 1);
                    }

                    var targetedQuantParamFilePath = Path.Combine(mWorkDir, targetedQuantParamFileName);

                    targetedQuantParamFileInfo.Add(new KeyValuePair<string, string>(baseName, targetedQuantParamFilePath));

                    using var targetedQuantXmlWriter = new XmlTextWriter(targetedQuantParamFilePath, Encoding.UTF8)
                    {
                        Formatting = Formatting.Indented,
                        Indentation = 4
                    };

                    targetedQuantXmlWriter.WriteStartDocument();
                    targetedQuantXmlWriter.WriteStartElement("WorkflowParameters");

                    WriteXMLSetting(targetedQuantXmlWriter, "CopyRawFileLocal", "false");
                    WriteXMLSetting(targetedQuantXmlWriter, "DeleteLocalDatasetAfterProcessing", "false");
                    WriteXMLSetting(targetedQuantXmlWriter, "FileContainingDatasetPaths", "");
                    WriteXMLSetting(targetedQuantXmlWriter, "FolderPathForCopiedRawDataset", "");
                    WriteXMLSetting(targetedQuantXmlWriter, "LoggingFolder", mWorkDir);
                    WriteXMLSetting(targetedQuantXmlWriter, "TargetsFilePath", Path.Combine(mWorkDir, psmResultsFileName));
                    WriteXMLSetting(targetedQuantXmlWriter, "TargetType", "LcmsFeature");
                    WriteXMLSetting(targetedQuantXmlWriter, "ResultsFolder", mWorkDir);
                    WriteXMLSetting(targetedQuantXmlWriter, "WorkflowParameterFile", Path.Combine(mWorkDir, workflowParamFileName));
                    WriteXMLSetting(targetedQuantXmlWriter, "WorkflowType", "TopDownTargetedWorkflowExecutor1");

                    targetedQuantXmlWriter.WriteEndElement(); // WorkflowParameters

                    targetedQuantXmlWriter.WriteEndDocument();
                }

                return targetedQuantParamFileInfo;
            }
            catch (Exception ex)
            {
                mMessage = "Exception creating " + TARGETED_QUANT_XML_FILE_NAME;
                LogError(mMessage + ": " + ex.Message);
                return targetedQuantParamFileInfo;
            }
        }

        // Example Console output:

        // ReSharper disable CommentTypo

        //   8/13/2012 2:29:48 PM    Started Processing....
        //   8/13/2012 2:29:48 PM    Dataset = E:\DMS_WorkDir2\Proteus_Peri_intact_ETD.raw
        //   8/13/2012 2:29:48 PM    Run initialized successfully.
        //   8/13/2012 2:29:48 PM    Creating extracted ion chromatogram (XIC) source data... takes 1-5 minutes.. only needs to be done once.
        //   8/13/2012 2:30:18 PM    Done creating XIC source data.
        //   8/13/2012 2:30:18 PM    Peak loading started...
        //   Peak importer progress (%) = 4
        //   Peak importer progress (%) = 27
        //   Peak importer progress (%) = 50
        //   Peak importer progress (%) = 73
        //   Peak importer progress (%) = 92
        //   8/13/2012 2:30:21 PM    Peak Loading complete.
        //   Proteus_Peri_intact_ETD NOT aligned.
        //   8/13/2012 2:30:21 PM    FYI - Run has NOT been mass aligned.
        //   8/13/2012 2:30:21 PM    Warning - Run has NOT been NET aligned.
        //   8/13/2012 2:30:21 PM    Processing...
        //   8/13/2012 2:31:27 PM    Percent complete = 3%   Target 100 of 3917
        //   8/13/2012 2:32:24 PM    Percent complete = 5%   Target 200 of 3917
        //   8/13/2012 2:33:20 PM    Percent complete = 8%   Target 300 of 3917
        //   ...
        //   8/13/2012 1:56:55 PM    ---- PROCESSING COMPLETE ---------------

        // ReSharper restore CommentTypo

        private readonly Regex reSubProgress = new("Percent complete = ([0-9.]+)", RegexOptions.Compiled);

        /// <summary>
        /// Parse the TargetedWorkflowsConsole console output file to track progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            try
            {
                if (mConsoleOutputProgressMap == null || mConsoleOutputProgressMap.Count == 0)
                {
                    mConsoleOutputProgressMap = new Dictionary<string, int>
                    {
                        {"Creating extracted ion chromatogram", PROGRESS_TARGETED_WORKFLOWS_CREATING_XIC},
                        {"Done creating XIC source data", PROGRESS_TARGETED_WORKFLOWS_XIC_CREATED},
                        {"Peak Loading complete", PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED},
                        {"---- PROCESSING COMPLETE ----", PROGRESS_TARGETED_WORKFLOWS_PROCESSING_COMPLETE}
                    };
                }

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

                double subProgressAddOn = 0;

                var effectiveProgress = PROGRESS_TARGETED_WORKFLOWS_STARTING;

                mConsoleOutputErrorMsg = string.Empty;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (!string.IsNullOrWhiteSpace(dataLine))
                        {
                            var dataLineLCase = dataLine.ToLower();

                            // Update progress if the line contains any one of the expected phrases
                            foreach (var item in mConsoleOutputProgressMap)
                            {
                                if (dataLine.Contains(item.Key))
                                {
                                    if (effectiveProgress < item.Value)
                                    {
                                        effectiveProgress = item.Value;
                                    }
                                }
                            }

                            if (effectiveProgress == PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED)
                            {
                                var match = reSubProgress.Match(dataLine);

                                if (match.Success)
                                {
                                    if (double.TryParse(match.Groups[1].Value, out subProgressAddOn))
                                    {
                                        subProgressAddOn /= 100;
                                    }
                                }
                            }

                            var charIndex = dataLineLCase.IndexOf("exception of type", StringComparison.Ordinal);

                            if (charIndex < 0)
                            {
                                charIndex = dataLineLCase.IndexOf("\terror", StringComparison.Ordinal);

                                if (charIndex > 0)
                                {
                                    charIndex++;
                                }
                                else if (dataLineLCase.StartsWith("error"))
                                {
                                    charIndex = 0;
                                }
                            }

                            if (charIndex >= 0)
                            {
                                // Error message found; update mMessage
                                var newError = dataLine.Substring(charIndex);

                                if (newError.Contains("all peptides contain unknown modifications"))
                                {
                                    newError = "Error: every peptide in the mass tags file had an unknown modification; " +
                                        "known mods are Acetylation (C2H2O, 42.01 Da), Phosphorylation (HPO3, 79.97 Da), " +
                                        "or Pyroglutomate (H3N1, 17.03 Da)";
                                }

                                mConsoleOutputErrorMsg = Global.AppendToComment(mConsoleOutputErrorMsg, newError);
                            }
                        }
                    }
                }

                float effectiveProgressOverall = effectiveProgress;

                // Bump up the effective progress if finding features in positive or negative data
                if (effectiveProgress == PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED)
                {
                    effectiveProgressOverall += (float)((PROGRESS_TARGETED_WORKFLOWS_PROCESSING_COMPLETE - PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED) * subProgressAddOn);
                }

                if (mProgress < effectiveProgressOverall)
                {
                    mProgress = effectiveProgressOverall;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private bool RunTargetedWorkflow(string baseName, string targetedQuantParamFilePath)
        {
            mConsoleOutputErrorMsg = string.Empty;

            LogMessage("Running TargetedWorkflowsConsole");

            // Set up and execute a program runner to run TargetedWorkflowsConsole
            var rawDataTypeName = mJobParams.GetParam("RawDataType");
            string arguments;

            switch (rawDataTypeName.ToLower())
            {
                case AnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES:
                    arguments = " " + PossiblyQuotePath(Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_RAW_EXTENSION));
                    break;
                case AnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                case AnalysisResources.RAW_DATA_TYPE_DOT_D_FOLDERS:
                    // Bruker_FT folders are actually .D folders
                    arguments = " " + PossiblyQuotePath(Path.Combine(mWorkDir, mDatasetName) + AnalysisResources.DOT_D_EXTENSION);
                    break;
                default:
                    mMessage = "Dataset type " + rawDataTypeName + " is not supported";
                    LogDebug(mMessage);
                    return false;
            }

            arguments += " " + PossiblyQuotePath(targetedQuantParamFilePath);

            if (mDebugLevel >= 1)
            {
                LogDebug(mTargetedWorkflowsProgLoc + arguments);
            }

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = true;
            mCmdRunner.EchoOutputToConsole = true;
            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, TARGETED_WORKFLOWS_CONSOLE_OUTPUT);

            mProgress = PROGRESS_TARGETED_WORKFLOWS_STARTING;

            var processingSuccess = mCmdRunner.RunProgram(mTargetedWorkflowsProgLoc, arguments, "TargetedWorkflowsConsole", true);

            if (!mCmdRunner.WriteConsoleOutputToFile)
            {
                // Write the console output to a text file
                Global.IdleLoop(0.25);

                using var writer = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine(mCmdRunner.CachedConsoleOutput);
            }

            // Parse the console output file one more time to check for errors
            Global.IdleLoop(0.25);
            ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (processingSuccess)
            {
                // Make sure that the quantitation output file was created
                var outputFileName = mDatasetName + "_quant.txt";

                var quantFile = new FileInfo(Path.Combine(mWorkDir, outputFileName));

                if (!quantFile.Exists)
                {
                    mMessage = "MSAlign_Quant result file not found (" + outputFileName + ")";
                    LogError(mMessage);
                    processingSuccess = false;
                }

                if (!mDatasetName.Equals(baseName))
                {
                    // Rename the file
                    var newName = Path.Combine(mWorkDir, baseName + "_quant.txt");
                    quantFile.MoveTo(newName);
                }
            }

            if (!processingSuccess)
            {
                const string msg = "Error running TargetedWorkflowsConsole";

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError(msg + "; " + mConsoleOutputErrorMsg);
                }
                else
                {
                    LogError(msg);
                }

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("TargetedWorkflowsConsole returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to TargetedWorkflowsConsole failed (but exit code is 0)");
                }
            }
            else
            {
                mProgress = PROGRESS_PCT_COMPLETE;
                mStatusTools.UpdateAndWrite(mProgress);

                if (mDebugLevel >= 3)
                {
                    LogDebug("TargetedWorkflowsConsole Quantitation Complete");
                }

                var consoleOutputFile = new FileInfo(Path.Combine(mWorkDir, TARGETED_WORKFLOWS_CONSOLE_OUTPUT));
                var deconWorkflowsLogFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_log.txt"));

                if (consoleOutputFile.Exists && deconWorkflowsLogFile.Exists && consoleOutputFile.Length > deconWorkflowsLogFile.Length)
                {
                    // Don't keep the _log.txt file since the Console_Output file has the same information
                    mJobParams.AddResultFileToSkip(deconWorkflowsLogFile.Name);
                }

                if (consoleOutputFile.Exists && !mDatasetName.Equals(baseName) && baseName.Length > mDatasetName.Length)
                {
                    var suffix = baseName.Substring(mDatasetName.Length);
                    var newName = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(TARGETED_WORKFLOWS_CONSOLE_OUTPUT) + suffix + ".txt");

                    consoleOutputFile.MoveTo(newName);
                }

                // Don't keep the _peaks.txt file since it can get quite large
                mJobParams.AddResultFileToSkip(mDatasetName + "_peaks.txt");
            }

            return processingSuccess;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string targetedWorkflowsConsoleProgLoc)
        {
            var additionalDLLs = new List<string>
            {
                "DeconTools.Backend.dll",
                "DeconTools.Workflows.dll"
            };

            var success = StoreDotNETToolVersionInfo(targetedWorkflowsConsoleProgLoc, additionalDLLs, true);

            return success;
        }

        public void WriteXMLSetting(XmlTextWriter writer, string settingName, string settingValue)
        {
            writer.WriteStartElement(settingName);
            writer.WriteValue(settingValue);
            writer.WriteEndElement();
        }

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(mWorkDir, TARGETED_WORKFLOWS_CONSOLE_OUTPUT));

                LogProgress("MSAlign Quant");
            }
        }
    }
}
