using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Xml;
using AnalysisManagerBase;

namespace AnalysisManagerProSightQuantPlugIn
{
    /// <summary>
    /// Class for running TargetedQuant
    /// </summary>
    public class clsAnalysisToolRunnerProSightQuant : clsAnalysisToolRunnerBase
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
        protected int mDatasetID = 0;

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
                    LogDebug("clsAnalysisToolRunnerProSightQuant.RunTool(): Enter");
                }

                if (clsAnalysisResourcesProSightQuant.TOOL_DISABLED)
                {
                    // This tool is currently disabled, so just return Success
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

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

                mCmdRunner = new clsRunDosProgram(m_WorkDir);
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
                    System.Threading.Thread.Sleep(250);

                    using (var swConsoleOutputfile = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        swConsoleOutputfile.WriteLine(mCmdRunner.CachedConsoleOutput);
                    }
                }

                // Parse the console output file one more time to check for errors
                System.Threading.Thread.Sleep(250);
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
                        m_message = "ProSight_Quant result file not found (" + strOutputFileName + ")";
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

                    var fiConsoleOutputfile = new FileInfo(Path.Combine(m_WorkDir, TARGETED_WORKFLOWS_CONSOLE_OUTPUT));
                    var fiDeconWorkflowsLogFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "_log.txt"));

                    if (fiConsoleOutputfile.Exists && fiDeconWorkflowsLogFile.Exists && fiConsoleOutputfile.Length > fiDeconWorkflowsLogFile.Length)
                    {
                        // Don't keep the _log.txt file since the Console_Output file has all of the same information
                        m_jobParams.AddResultFileToSkip(fiDeconWorkflowsLogFile.Name);
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
                System.Threading.Thread.Sleep(500);
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
                m_message = "Exception in ProSightQuantPlugin->RunTool";
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
            var strTargetedQuantParamFilePath = string.Empty;

            try
            {
                strTargetedQuantParamFilePath = Path.Combine(m_WorkDir, TARGETED_QUANT_XML_FILE_NAME);
                var strProSightPCResultsFile = clsAnalysisResourcesProSightQuant.PROSIGHT_PC_RESULT_FILE;

                var strWorkflowParamFileName = m_jobParams.GetParam("ProSightQuantParamFile");
                if (string.IsNullOrEmpty(strWorkflowParamFileName))
                {
                    m_message = NotifyMissingParameter(m_jobParams, "ProSightQuantParamFile");
                    return string.Empty;
                }

                using (var swTargetedQuantXMLFile = new XmlTextWriter(strTargetedQuantParamFilePath, System.Text.Encoding.UTF8))
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
                    WriteXMLSetting(swTargetedQuantXMLFile, "TargetsFilePath", Path.Combine(m_WorkDir, strProSightPCResultsFile));
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

                string strLineIn = null;
                string strLineInLCase = null;

                var intLinesRead = 0;
                var intCharIndex = 0;

                double dblSubProgressAddon = 0;

                var intEffectiveProgress = 0;
                intEffectiveProgress = PROGRESS_TARGETED_WORKFLOWS_STARTING;

                mConsoleOutputErrorMsg = string.Empty;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    intLinesRead = 0;
                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();
                        intLinesRead += 1;

                        if (!string.IsNullOrWhiteSpace(strLineIn))
                        {
                            strLineInLCase = strLineIn.ToLower();

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
                                    if (double.TryParse(oMatch.Groups[1].Value, out dblSubProgressAddon))
                                    {
                                        dblSubProgressAddon /= 100;
                                    }
                                }
                            }

                            intCharIndex = strLineInLCase.IndexOf("exception of type");
                            if (intCharIndex < 0)
                            {
                                intCharIndex = strLineInLCase.IndexOf("\t" + "error");

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
                                mConsoleOutputErrorMsg = strLineIn.Substring(intCharIndex);
                            }
                        }
                    }
                }

                float sngEffectiveProgress = intEffectiveProgress;

                // Bump up the effective progress if finding features in positive or negative data
                if (intEffectiveProgress == PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED)
                {
                    sngEffectiveProgress += (float)((PROGRESS_TARGETED_WORKFLOWS_PROCESSING_COMPLETE - PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED) * dblSubProgressAddon);
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
        protected bool StoreToolVersionInfo(string targetedWorkflowsConsoleProgLoc)
        {
            var additionalDLLs = new List<string>
            {
                "DeconTools.Backend.dll",
                "DeconTools.Workflows.dll"
            };

            var success = StoreDotNETToolVersionInfo(targetedWorkflowsConsoleProgLoc, additionalDLLs);

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

                LogProgress("ProSightQuant");
            }
        }

        #endregion
    }
}
