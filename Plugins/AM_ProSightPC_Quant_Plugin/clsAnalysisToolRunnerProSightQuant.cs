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
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            string CmdStr = null;

            CloseOutType result = CloseOutType.CLOSEOUT_SUCCESS;
            var blnProcessingError = false;

            bool blnSuccess = false;

            string strTargetedQuantParamFilePath = null;

            try
            {
                //Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "clsAnalysisToolRunnerProSightQuant.RunTool(): Enter");
                }

                if (clsAnalysisResourcesProSightQuant.TOOL_DISABLED)
                {
                    // This tool is currently disabled, so just return Success
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                // Determine the path to the TargetedWorkflowConsole.exe program
                mTargetedWorkflowsProgLoc = DetermineProgramLocation("MSAlign_Quant", "TargetedWorkflowsProgLoc", "TargetedWorkflowConsole.exe");

                if (string.IsNullOrWhiteSpace(mTargetedWorkflowsProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the TargetedWorkflowsConsole version info in the database
                if (!StoreToolVersionInfo(mTargetedWorkflowsProgLoc))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining TargetedWorkflowsConsole version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Create the TargetedWorkflowParams.xml file
                m_progress = PROGRESS_PCT_CREATING_PARAMETERS;

                strTargetedQuantParamFilePath = CreateTargetedQuantParamFile();
                if (string.IsNullOrEmpty(strTargetedQuantParamFilePath))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Aborting since CreateTargetedQuantParamFile returned false");
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Error creating " + TARGETED_QUANT_XML_FILE_NAME;
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mConsoleOutputErrorMsg = string.Empty;

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running TargetedWorkflowsConsole");

                // Set up and execute a program runner to run TargetedWorkflowsConsole
                string strRawDataType = m_jobParams.GetParam("RawDataType");

                switch (strRawDataType.ToLower())
                {
                    case clsAnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES:
                        CmdStr = " " + PossiblyQuotePath(Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_RAW_EXTENSION));
                        break;
                    case clsAnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                        // Bruker_FT folders are actually .D folders
                        CmdStr = " " + PossiblyQuotePath(Path.Combine(m_WorkDir, m_Dataset) + clsAnalysisResources.DOT_D_EXTENSION);
                        break;
                    default:
                        m_message = "Dataset type " + strRawDataType + " is not supported";
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message);
                        return CloseOutType.CLOSEOUT_FAILED;
                }

                CmdStr += " " + PossiblyQuotePath(strTargetedQuantParamFilePath);

                if (m_DebugLevel >= 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, mTargetedWorkflowsProgLoc + CmdStr);
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

                blnSuccess = mCmdRunner.RunProgram(mTargetedWorkflowsProgLoc, CmdStr, "TargetedWorkflowsConsole", true);

                if (!mCmdRunner.WriteConsoleOutputToFile)
                {
                    // Write the console output to a text file
                    System.Threading.Thread.Sleep(250);

                    using (StreamWriter swConsoleOutputfile = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        swConsoleOutputfile.WriteLine(mCmdRunner.CachedConsoleOutput);
                    }
                }

                // Parse the console output file one more time to check for errors
                System.Threading.Thread.Sleep(250);
                ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg);
                }

                if (blnSuccess)
                {
                    // Make sure that the quantitation output file was created
                    string strOutputFileName = m_Dataset + "_quant.txt";
                    if (!File.Exists(Path.Combine(m_WorkDir, strOutputFileName)))
                    {
                        m_message = "ProSight_Quant result file not found (" + strOutputFileName + ")";
                        blnSuccess = false;
                    }
                }

                if (!blnSuccess)
                {
                    string Msg = null;
                    Msg = "Error running TargetedWorkflowsConsole";

                    if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                    {
                        m_message = clsGlobal.AppendToComment(m_message, Msg + "; " + mConsoleOutputErrorMsg);
                    }
                    else
                    {
                        m_message = clsGlobal.AppendToComment(m_message, Msg);
                    }

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg + ", job " + m_JobNum);

                    if (mCmdRunner.ExitCode != 0)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            "TargetedWorkflowsConsole returned a non-zero exit code: " + mCmdRunner.ExitCode.ToString());
                    }
                    else
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            "Call to TargetedWorkflowsConsole failed (but exit code is 0)");
                    }

                    blnProcessingError = true;
                }
                else
                {
                    m_progress = PROGRESS_PCT_COMPLETE;
                    m_StatusTools.UpdateAndWrite(m_progress);
                    if (m_DebugLevel >= 3)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "TargetedWorkflowsConsole Quantitation Complete");
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

                //Stop the job timer
                m_StopTime = System.DateTime.UtcNow;

                //Add the current job data to the summary file
                if (!UpdateSummaryFile())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                        "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                }

                //Make sure objects are released
                System.Threading.Thread.Sleep(500);         // 1 second delay
                PRISM.Processes.clsProgRunner.GarbageCollectNow();

                if (blnProcessingError | result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                result = MakeResultsFolder();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    //MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                result = MoveResultFiles();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    m_message = "Error moving files into results folder";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                result = CopyResultsFolderToServer();
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return result;
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception in ProSightQuantPlugin->RunTool";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS; //No failures so everything must have succeeded
        }

        protected void CopyFailedResultsToArchiveFolder()
        {
            string strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrWhiteSpace(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory
            string strFolderPathToArchive = null;
            strFolderPathToArchive = string.Copy(m_WorkDir);

            // Make the results folder
            var result = MakeResultsFolder();
            if (result == CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Move the result files into the result folder
                result = MoveResultFiles();
                if (result == CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Move was a success; update strFolderPathToArchive
                    strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName);
                }
            }

            // Copy the results folder to the Archive folder
            var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
            objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive);
        }

        /// <summary>
        /// Creates the targeted quant params XML file
        /// </summary>
        /// <returns>The full path to the file, if successful.  Otherwise, and empty string</returns>
        /// <remarks></remarks>
        protected string CreateTargetedQuantParamFile()
        {
            string strTargetedQuantParamFilePath = string.Empty;
            string strProSightPCResultsFile = null;
            string strWorkflowParamFileName = null;

            try
            {
                strTargetedQuantParamFilePath = Path.Combine(m_WorkDir, TARGETED_QUANT_XML_FILE_NAME);
                strProSightPCResultsFile = clsAnalysisResourcesProSightQuant.PROSIGHT_PC_RESULT_FILE;

                strWorkflowParamFileName = m_jobParams.GetParam("ProSightQuantParamFile");
                if (string.IsNullOrEmpty(strWorkflowParamFileName))
                {
                    m_message = clsAnalysisToolRunnerBase.NotifyMissingParameter(m_jobParams, "ProSightQuantParamFile");
                    return string.Empty;
                }

                using (System.Xml.XmlTextWriter swTargetedQuantXMLFile = new System.Xml.XmlTextWriter(strTargetedQuantParamFilePath, System.Text.Encoding.UTF8))
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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
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
        private Regex reSubProgress = new Regex(@"Percent complete = ([0-9.]+)", RegexOptions.Compiled);

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
                    mConsoleOutputProgressMap = new Dictionary<string, int>();

                    mConsoleOutputProgressMap.Add("Creating extracted ion chromatogram", PROGRESS_TARGETED_WORKFLOWS_CREATING_XIC);
                    mConsoleOutputProgressMap.Add("Done creating XIC source data", PROGRESS_TARGETED_WORKFLOWS_XIC_CREATED);
                    mConsoleOutputProgressMap.Add("Peak Loading complete", PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED);
                    mConsoleOutputProgressMap.Add("---- PROCESSING COMPLETE ----", PROGRESS_TARGETED_WORKFLOWS_PROCESSING_COMPLETE);
                }

                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                            "Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " + strConsoleOutputFilePath);
                }

                string strLineIn = null;
                string strLineInLCase = null;

                int intLinesRead = 0;
                int intCharIndex = 0;

                double dblSubProgressAddon = 0;

                int intEffectiveProgress = 0;
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
                            foreach (KeyValuePair<string, int> oItem in mConsoleOutputProgressMap)
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
                    sngEffectiveProgress += Convert.ToSingle((PROGRESS_TARGETED_WORKFLOWS_PROCESSING_COMPLETE - PROGRESS_TARGETED_WORKFLOWS_PEAKS_LOADED) * dblSubProgressAddon);
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
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string strTargetedWorkflowsConsoleProgLoc)
        {
            string strToolVersionInfo = string.Empty;
            bool blnSuccess = false;

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            var ioTargetedWorkflowsConsole = new FileInfo(strTargetedWorkflowsConsoleProgLoc);
            if (!ioTargetedWorkflowsConsole.Exists)
            {
                try
                {
                    strToolVersionInfo = "Unknown";
                    return base.SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>());
                }
                catch (Exception ex)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Exception calling SetStepTaskToolVersion: " + ex.Message);
                    return false;
                }

                return false;
            }

            // Lookup the version of the TargetedWorkflowsConsole application
            blnSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, ioTargetedWorkflowsConsole.FullName);
            if (!blnSuccess)
                return false;

            // Store paths to key DLLs in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(ioTargetedWorkflowsConsole);

            ioToolFiles.Add(new FileInfo(Path.Combine(ioTargetedWorkflowsConsole.DirectoryName, "DeconTools.Backend.dll")));
            ioToolFiles.Add(new FileInfo(Path.Combine(ioTargetedWorkflowsConsole.DirectoryName, "DeconTools.Workflows.dll")));

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        public void WriteXMLSetting(System.Xml.XmlTextWriter swOutFile, string strSettingName, string strSettingValue)
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

            if (System.DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15)
            {
                dtLastConsoleOutputParse = System.DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(m_WorkDir, TARGETED_WORKFLOWS_CONSOLE_OUTPUT));

                LogProgress("ProSightQuant");
            }
        }

        #endregion
    }
}
