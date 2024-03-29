using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Xml;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerProSightQuantPlugIn
{
    /// <summary>
    /// Class for running TargetedQuant
    /// </summary>
    public class AnalysisToolRunnerProSightQuant : AnalysisToolRunnerBase
    {
        // Ignore Spelling: quant, quantitation

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
                    LogDebug("AnalysisToolRunnerProSightQuant.RunTool(): Enter");
                }

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (AnalysisResourcesProSightQuant.TOOL_DISABLED)
                {
                    // This tool is currently disabled, so just return Success
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

#pragma warning disable CS0162

                // Determine the path to the TargetedWorkflowConsole.exe program
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

                var targetedQuantParamFilePath = CreateTargetedQuantParamFile();

                if (string.IsNullOrEmpty(targetedQuantParamFilePath))
                {
                    LogError("Aborting since CreateTargetedQuantParamFile returned false");

                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Error creating " + TARGETED_QUANT_XML_FILE_NAME;
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mConsoleOutputErrorMsg = string.Empty;

                LogMessage("Running TargetedWorkflowsConsole");

                // Set up and execute a program runner to run TargetedWorkflowsConsole
                var rawDataType = mJobParams.GetParam("RawDataType");
                string arguments;

                switch (rawDataType.ToLower())
                {
                    case AnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES:
                        arguments = " " + PossiblyQuotePath(Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_RAW_EXTENSION));
                        break;
                    case AnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                        // Bruker_FT folders are actually .D folders
                        arguments = " " + PossiblyQuotePath(Path.Combine(mWorkDir, mDatasetName) + AnalysisResources.DOT_D_EXTENSION);
                        break;
                    default:
                        mMessage = "Dataset type " + rawDataType + " is not supported";
                        LogDebug(mMessage);
                        return CloseOutType.CLOSEOUT_FAILED;
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

                    if (!File.Exists(Path.Combine(mWorkDir, outputFileName)))
                    {
                        mMessage = "ProSight_Quant result file not found (" + outputFileName + ")";
                        processingSuccess = false;
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

                    var consoleOutputfile = new FileInfo(Path.Combine(mWorkDir, TARGETED_WORKFLOWS_CONSOLE_OUTPUT));
                    var deconWorkflowsLogFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_log.txt"));

                    if (consoleOutputfile.Exists && deconWorkflowsLogFile.Exists && consoleOutputfile.Length > deconWorkflowsLogFile.Length)
                    {
                        // Don't keep the _log.txt file since the Console_Output file has all of the same information
                        mJobParams.AddResultFileToSkip(deconWorkflowsLogFile.Name);
                    }

                    // Don't keep the _peaks.txt file since it can get quite large
                    mJobParams.AddResultFileToSkip(mDatasetName + "_peaks.txt");
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

#pragma warning restore CS0162
            }
            catch (Exception ex)
            {
                mMessage = "Exception in ProSightQuantPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Creates the targeted quant params XML file
        /// </summary>
        /// <returns>The full path to the file, if successful.  Otherwise, and empty string</returns>
        private string CreateTargetedQuantParamFile()
        {
            try
            {
                var targetedQuantParamFilePath = Path.Combine(mWorkDir, TARGETED_QUANT_XML_FILE_NAME);
                const string proSightPCResultsFile = AnalysisResourcesProSightQuant.PROSIGHT_PC_RESULT_FILE;

                var workflowParamFileName = mJobParams.GetParam("ProSightQuantParamFile");

                if (string.IsNullOrEmpty(workflowParamFileName))
                {
                    mMessage = NotifyMissingParameter(mJobParams, "ProSightQuantParamFile");
                    return string.Empty;
                }

                using var writer = new XmlTextWriter(targetedQuantParamFilePath, System.Text.Encoding.UTF8)
                {
                    Formatting = Formatting.Indented,
                    Indentation = 4
                };

                writer.WriteStartDocument();
                writer.WriteStartElement("WorkflowParameters");

                WriteXMLSetting(writer, "CopyRawFileLocal", "false");
                WriteXMLSetting(writer, "DeleteLocalDatasetAfterProcessing", "false");
                WriteXMLSetting(writer, "FileContainingDatasetPaths", "");
                WriteXMLSetting(writer, "FolderPathForCopiedRawDataset", "");
                WriteXMLSetting(writer, "LoggingFolder", mWorkDir);
                WriteXMLSetting(writer, "TargetsFilePath", Path.Combine(mWorkDir, proSightPCResultsFile));
                WriteXMLSetting(writer, "TargetType", "LcmsFeature");
                WriteXMLSetting(writer, "ResultsFolder", mWorkDir);
                WriteXMLSetting(writer, "WorkflowParameterFile", Path.Combine(mWorkDir, workflowParamFileName));
                WriteXMLSetting(writer, "WorkflowType", "TopDownTargetedWorkflowExecutor1");

                writer.WriteEndElement();    // WorkflowParameters

                writer.WriteEndDocument();

                return targetedQuantParamFilePath;
            }
            catch (Exception ex)
            {
                mMessage = "Exception creating " + TARGETED_QUANT_XML_FILE_NAME;
                LogError(mMessage + ": " + ex.Message);
                return string.Empty;
            }
        }

        // Example Console output:
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

                double subProgressAddon = 0;

                var effectiveProgress = PROGRESS_TARGETED_WORKFLOWS_STARTING;

                mConsoleOutputErrorMsg = string.Empty;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var linesRead = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        linesRead++;

                        if (!string.IsNullOrWhiteSpace(dataLine))
                        {
                            var dataLineLCase = dataLine.ToLower();

                            // Update progress if the line contains any one of the expected phrases
                            foreach (var item in mConsoleOutputProgressMap)
                            {
                                if (dataLine.IndexOf(item.Key, StringComparison.OrdinalIgnoreCase) >= 0)
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
                                    if (double.TryParse(match.Groups[1].Value, out subProgressAddon))
                                    {
                                        subProgressAddon /= 100;
                                    }
                                }
                            }

                            var charIndex = dataLineLCase.IndexOf("exception of type", StringComparison.OrdinalIgnoreCase);

                            if (charIndex < 0)
                            {
                                charIndex = dataLineLCase.IndexOf("\terror", StringComparison.OrdinalIgnoreCase);

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
                                mConsoleOutputErrorMsg = dataLine.Substring(charIndex);
                            }
                        }
                    }
                }

                float progressOverall = effectiveProgress;

                // Bump up the effective progress if finding features in positive or negative data
                if (effectiveProgress == PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED)
                {
                    progressOverall += (float)((PROGRESS_TARGETED_WORKFLOWS_PROCESSING_COMPLETE - PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED) * subProgressAddon);
                }

                if (mProgress < progressOverall)
                {
                    mProgress = progressOverall;
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

                LogProgress("ProSightQuant");
            }
        }
    }
}
