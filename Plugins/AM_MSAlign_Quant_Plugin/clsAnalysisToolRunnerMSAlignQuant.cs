using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using AnalysisManagerBase;

namespace AnalysisManagerMSAlignQuantPlugIn
{
    /// <summary>
    /// Class for running TargetedQuant
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsAnalysisToolRunnerMSAlignQuant : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

        protected const string TARGETED_QUANT_XML_FILE_NAME = "TargetedWorkflowParams.xml";
        protected const string TARGETED_WORKFLOWS_CONSOLE_OUTPUT = "TargetedWorkflow_ConsoleOutput.txt";
        protected const int PROGRESS_PCT_CREATING_PARAMETERS = 5;

        protected const int PROGRESS_TARGETED_WORKFLOWS_STARTING = 10;
        protected const int PROGRESS_TARGETED_WORKFLOWS_CREATING_XIC = 11;
        protected const int PROGRESS_TARGETED_WORKFLOWS_XIC_CREATED = 15;
        protected const int PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED = 15;
        protected const int PROGRESS_TARGETED_WORKFLOWS_PROCESSING_COMPLETE = 95;

        protected const int PROGRESS_TARGETED_WORKFLOWS_COMPLETE = 98;
        protected const int PROGRESS_PCT_COMPLETE = 99;

        protected string mConsoleOutputErrorMsg;

        protected string mTargetedWorkflowsProgLoc;
        protected Dictionary<string, int> mConsoleOutputProgressMap;

        protected clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

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

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerMSAlignQuant.RunTool(): Enter");
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
                    m_message = "Error determining TargetedWorkflowsConsole version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Create the TargetedWorkflowParams.xml file
                m_progress = PROGRESS_PCT_CREATING_PARAMETERS;

                var strTargetedQuantParamFilePath = CreateTargetedQuantParamFile();
                if (string.IsNullOrEmpty(strTargetedQuantParamFilePath))
                {
                    LogError("Aborting since CreateTargetedQuantParamFile returned false");
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Error creating " + TARGETED_QUANT_XML_FILE_NAME;
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mConsoleOutputErrorMsg = string.Empty;

                LogMessage("Running TargetedWorkflowsConsole");

                // Set up and execute a program runner to run TargetedWorkflowsConsole
                var strRawDataType = m_jobParams.GetParam("RawDataType");
                string cmdStr;

                switch (strRawDataType.ToLower())
                {
                    case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES:
                        cmdStr = " " + PossiblyQuotePath(Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_RAW_EXTENSION));
                        break;
                    case clsAnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                    case clsAnalysisResources.RAW_DATA_TYPE_DOT_D_FOLDERS:
                        // Bruker_FT folders are actually .D folders
                        cmdStr = " " + PossiblyQuotePath(Path.Combine(m_WorkDir, m_Dataset) + clsAnalysisResources.DOT_D_EXTENSION);
                        break;
                    default:
                        m_message = "Dataset type " + strRawDataType + " is not supported";
                        LogDebug(m_message);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                cmdStr += " " + PossiblyQuotePath(strTargetedQuantParamFilePath);

                if (m_DebugLevel >= 1)
                {
                    LogDebug(mTargetedWorkflowsProgLoc + cmdStr);
                }

                mCmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                mCmdRunner.CreateNoWindow = true;
                mCmdRunner.CacheStandardOutput = true;
                mCmdRunner.EchoOutputToConsole = true;
                mCmdRunner.WriteConsoleOutputToFile = true;
                mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, TARGETED_WORKFLOWS_CONSOLE_OUTPUT);

                m_progress = PROGRESS_TARGETED_WORKFLOWS_STARTING;

                var processingSuccess = mCmdRunner.RunProgram(mTargetedWorkflowsProgLoc, cmdStr, "TargetedWorkflowsConsole", true);

                if (!mCmdRunner.WriteConsoleOutputToFile)
                {
                    // Write the console output to a text file
                    clsGlobal.IdleLoop(0.25);

                    using (var swConsoleOutputFile = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        swConsoleOutputFile.WriteLine(mCmdRunner.CachedConsoleOutput);
                    }
                }

                // Parse the console output file one more time to check for errors
                clsGlobal.IdleLoop(0.25);
                ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputErrorMsg);
                }

                if (processingSuccess)
                {
                    // Make sure that the quantitation output file was created
                    var strOutputFileName = m_Dataset + "_quant.txt";

                    if (!File.Exists(Path.Combine(m_WorkDir, strOutputFileName)))
                    {
                        m_message = "MSAlign_Quant result file not found (" + strOutputFileName + ")";
                        LogError(m_message);
                        processingSuccess = false;
                    }
                }

                if (!processingSuccess)
                {
                    var msg = "Error running TargetedWorkflowsConsole";

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
                    m_progress = PROGRESS_PCT_COMPLETE;
                    m_StatusTools.UpdateAndWrite(m_progress);
                    if (m_DebugLevel >= 3)
                    {
                        LogDebug("TargetedWorkflowsConsole Quantitation Complete");
                    }

                    var consoleOutputFile = new FileInfo(Path.Combine(m_WorkDir, TARGETED_WORKFLOWS_CONSOLE_OUTPUT));
                    var deconWorkflowsLogFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "_log.txt"));

                    if (consoleOutputFile.Exists && deconWorkflowsLogFile.Exists && consoleOutputFile.Length > deconWorkflowsLogFile.Length)
                    {
                        // Don't keep the _log.txt file since the Console_Output file has all of the same information
                        m_jobParams.AddResultFileToSkip(deconWorkflowsLogFile.Name);
                    }

                    // Don't keep the _peaks.txt file since it can get quite large
                    m_jobParams.AddResultFileToSkip(m_Dataset + "_peaks.txt");
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.clsProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Exception in MSAlignQuantPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Creates the targeted quant params XML file
        /// </summary>
        /// <returns>The full path to the file, if successful.  Otherwise, and empty string</returns>
        /// <remarks></remarks>
        protected string CreateTargetedQuantParamFile()
        {
            string strTargetedQuantParamFilePath;

            try
            {
                strTargetedQuantParamFilePath = Path.Combine(m_WorkDir, TARGETED_QUANT_XML_FILE_NAME);
                var strMSAlignResultTableName = m_Dataset + clsAnalysisResourcesMSAlignQuant.MSALIGN_RESULT_TABLE_SUFFIX;

                // Optionally make a trimmed version of the ResultTable file for testing purposes

                // var strFullResultsPath = Path.Combine(m_WorkDir, strMSAlignResultTableName);
                // var strTrimmedFilePath = Path.Combine(m_WorkDir, Dataset + "_TrimmedResults.tmp");
                //
                // using (var srFullResults = new StreamReader(new FileStream(strFullResultsPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                // using (var swTrimmedResults = new StreamWriter(new FileStream(strTrimmedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                //{
                //    var linesRead = 0;
                //    while (!srFullResults.EndOfStream && linesRead < 30)
                //    {
                //        swTrimmedResults.WriteLine(srFullResults.ReadLine());
                //        linesRead += 1;
                //    }
                //}
                //
                // // Replace the original file with the trimmed one
                // Thread.Sleep(100);
                // File.Delete(strFullResultsPath);
                // Thread.Sleep(100);
                //
                // File.Move(strTrimmedFilePath, strFullResultsPath);

                var strWorkflowParamFileName = m_jobParams.GetParam("MSAlignQuantParamFile");
                if (string.IsNullOrEmpty(strWorkflowParamFileName))
                {
                    m_message = NotifyMissingParameter(m_jobParams, "MSAlignQuantParamFile");
                    return string.Empty;
                }

                using (var swTargetedQuantXMLFile = new XmlTextWriter(strTargetedQuantParamFilePath, Encoding.UTF8))
                {
                    swTargetedQuantXMLFile.Formatting = Formatting.Indented;
                    swTargetedQuantXMLFile.Indentation = 4;

                    swTargetedQuantXMLFile.WriteStartDocument();
                    swTargetedQuantXMLFile.WriteStartElement("WorkflowParameters");

                    WriteXMLSetting(swTargetedQuantXMLFile, "CopyRawFileLocal", "false");
                    WriteXMLSetting(swTargetedQuantXMLFile, "DeleteLocalDatasetAfterProcessing", "false");
                    WriteXMLSetting(swTargetedQuantXMLFile, "FileContainingDatasetPaths", "");
                    WriteXMLSetting(swTargetedQuantXMLFile, "FolderPathForCopiedRawDataset", "");
                    WriteXMLSetting(swTargetedQuantXMLFile, "LoggingFolder", m_WorkDir);
                    WriteXMLSetting(swTargetedQuantXMLFile, "TargetsFilePath", Path.Combine(m_WorkDir, strMSAlignResultTableName));
                    WriteXMLSetting(swTargetedQuantXMLFile, "TargetType", "LcmsFeature");
                    WriteXMLSetting(swTargetedQuantXMLFile, "ResultsFolder", m_WorkDir);
                    WriteXMLSetting(swTargetedQuantXMLFile, "WorkflowParameterFile", Path.Combine(m_WorkDir, strWorkflowParamFileName));
                    WriteXMLSetting(swTargetedQuantXMLFile, "WorkflowType", "TopDownTargetedWorkflowExecutor1");

                    swTargetedQuantXMLFile.WriteEndElement();    // WorkflowParameters

                    swTargetedQuantXMLFile.WriteEndDocument();
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception creating " + TARGETED_QUANT_XML_FILE_NAME;
                LogError(m_message + ": " + ex.Message);
                return string.Empty;
            }

            return strTargetedQuantParamFilePath;
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

        private readonly Regex reSubProgress = new Regex(@"Percent complete = ([0-9.]+)", RegexOptions.Compiled);

        /// <summary>
        /// Parse the TargetedWorkflowsConsole console output file to track progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        protected void ParseConsoleOutputFile(string strConsoleOutputFilePath)
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

                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Parsing file " + strConsoleOutputFilePath);
                }

                double subProgressAddOn = 0;

                var intEffectiveProgress = PROGRESS_TARGETED_WORKFLOWS_STARTING;

                mConsoleOutputErrorMsg = string.Empty;

                using (var reader = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var strLineIn = reader.ReadLine();

                        if (!string.IsNullOrWhiteSpace(strLineIn))
                        {
                            var strLineInLCase = strLineIn.ToLower();

                            // Update progress if the line contains any one of the expected phrases
                            foreach (var oItem in mConsoleOutputProgressMap)
                            {
                                if (strLineIn.Contains(oItem.Key))
                                {
                                    if (intEffectiveProgress < oItem.Value)
                                    {
                                        intEffectiveProgress = oItem.Value;
                                    }
                                }
                            }

                            if (intEffectiveProgress == PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED)
                            {
                                var oMatch = reSubProgress.Match(strLineIn);
                                if (oMatch.Success)
                                {
                                    if (double.TryParse(oMatch.Groups[1].Value, out subProgressAddOn))
                                    {
                                        subProgressAddOn /= 100;
                                    }
                                }
                            }

                            var intCharIndex = strLineInLCase.IndexOf("exception of type", StringComparison.Ordinal);
                            if (intCharIndex < 0)
                            {
                                intCharIndex = strLineInLCase.IndexOf("\t" + "error", StringComparison.Ordinal);

                                if (intCharIndex > 0)
                                {
                                    intCharIndex += 1;
                                }
                                else if (strLineInLCase.StartsWith("error"))
                                {
                                    intCharIndex = 0;
                                }
                            }

                            if (intCharIndex >= 0)
                            {
                                // Error message found; update m_message
                                var strNewError = strLineIn.Substring(intCharIndex);

                                if (strNewError.Contains("all peptides contain unknown modifications"))
                                {
                                    strNewError = "Error: every peptide in the mass tags file had an unknown modification; " +
                                        "known mods are Acetylation (C2H2O, 42.01 Da), Phosphorylation (HPO3, 79.97 Da), " +
                                        "or Pyroglutomate (H3N1, 17.03 Da)";
                                }

                                mConsoleOutputErrorMsg = clsGlobal.AppendToComment(mConsoleOutputErrorMsg, strNewError);
                            }
                        }
                    }
                }

                float sngEffectiveProgress = intEffectiveProgress;

                // Bump up the effective progress if finding features in positive or negative data
                if (intEffectiveProgress == PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED)
                {
                    sngEffectiveProgress += (float)((PROGRESS_TARGETED_WORKFLOWS_PROCESSING_COMPLETE - PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED) * subProgressAddOn);
                }

                if (m_progress < sngEffectiveProgress)
                {
                    m_progress = sngEffectiveProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string strTargetedWorkflowsConsoleProgLoc)
        {
            var additionalDLLs = new List<string>
            {
                "DeconTools.Backend.dll",
                "DeconTools.Workflows.dll"
            };

            var success = StoreDotNETToolVersionInfo(strTargetedWorkflowsConsoleProgLoc, additionalDLLs);

            return success;
        }

        public void WriteXMLSetting(XmlTextWriter swOutFile, string strSettingName, string strSettingValue)
        {
            swOutFile.WriteStartElement(strSettingName);
            swOutFile.WriteValue(strSettingValue);
            swOutFile.WriteEndElement();
        }

        #endregion

        #region "Event Handlers"

        private DateTime dtLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        protected void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15)
            {
                dtLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(m_WorkDir, TARGETED_WORKFLOWS_CONSOLE_OUTPUT));

                LogProgress("MSAlign Quant");
            }
        }

        #endregion
    }
}
