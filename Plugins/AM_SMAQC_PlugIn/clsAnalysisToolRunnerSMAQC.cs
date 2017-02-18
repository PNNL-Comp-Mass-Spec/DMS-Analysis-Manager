using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerSMAQCPlugIn
{
    /// <summary>
    /// Class for running SMAQC
    /// </summary>
    public class clsAnalysisToolRunnerSMAQC : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

        private const string SMAQC_CONSOLE_OUTPUT = "SMAQC_ConsoleOutput.txt";
        private const float PROGRESS_PCT_SMAQC_STARTING = 1;
        private const float PROGRESS_PCT_SMAQC_SEARCHING_FOR_FILES = 5;
        private const float PROGRESS_PCT_SMAQC_POPULATING_DB_TEMP_TABLES = 10;
        private const float PROGRESS_PCT_SMAQC_RUNNING_MEASUREMENTS = 15;
        private const float PROGRESS_PCT_SMAQC_SAVING_RESULTS = 88;
        private const float PROGRESS_PCT_SMAQC_COMPLETE = 90;
        private const float PROGRESS_PCT_RUNNING_LLRC = 95;
        private const float PROGRESS_PCT_COMPLETE = 99;

        private const string STORE_SMAQC_RESULTS_SP_NAME = "StoreSMAQCResults";

        internal const bool LLRC_ENABLED = false;

        private string mConsoleOutputErrorMsg;
        private int mDatasetID = 0;

        #endregion

        private clsRunDosProgram mCmdRunner;

        #region "Methods"

        /// <summary>
        /// Runs SMAQC tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            CloseOutType result = CloseOutType.CLOSEOUT_SUCCESS;
            var blnProcessingError = false;

            bool blnSuccess = false;

            try
            {
                //Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerSMAQC.RunTool(): Enter");
                }

                // Determine the path to the SMAQC program
                string progLoc = null;
                progLoc = DetermineProgramLocation("SMAQC", "SMAQCProgLoc", "SMAQC.exe");

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the SMAQC version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                        "Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining SMAQC version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mConsoleOutputErrorMsg = string.Empty;

                // The parameter file name specifies the name of the .XML file listing the Measurements to run
                var strParameterFileName = m_jobParams.GetParam("parmFileName");
                var strParameterFilePath = Path.Combine(m_WorkDir, strParameterFileName);

                // Lookup the InstrumentID for this dataset
                var InstrumentID = 0;
                if (!LookupInstrumentIDFromDB(ref InstrumentID))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var resultsFilePath = Path.Combine(m_WorkDir, m_Dataset + "_SMAQC.txt");

                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running SMAQC");

                //Set up and execute a program runner to run SMAQC
                var CmdStr = PossiblyQuotePath(m_WorkDir);                       // Path to folder containing input files
                CmdStr += " /O:" + PossiblyQuotePath(resultsFilePath);           // Text file to write the results to
                CmdStr += " /DB:" + PossiblyQuotePath(m_WorkDir);                // Folder where SQLite DB will be created
                CmdStr += " /I:" + InstrumentID.ToString();                      // Instrument ID
                CmdStr += " /M:" + PossiblyQuotePath(strParameterFilePath);      // Path to XML file specifying measurements to run

                m_jobParams.AddResultFileToSkip("SMAQC.s3db");                   // Don't keep the SQLite DB

                if (m_DebugLevel >= 1)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc + " " + CmdStr);
                }

                mCmdRunner = new clsRunDosProgram(m_WorkDir)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = true,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(m_WorkDir, SMAQC_CONSOLE_OUTPUT)
                };
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                // We will delete the console output file later since it has the same content as the log file
                m_jobParams.AddResultFileToSkip(SMAQC_CONSOLE_OUTPUT);

                m_progress = PROGRESS_PCT_SMAQC_STARTING;

                blnSuccess = mCmdRunner.RunProgram(progLoc, CmdStr, "SMAQC", true);

                if (!mCmdRunner.WriteConsoleOutputToFile)
                {
                    // Write the console output to a text file
                    Thread.Sleep(250);

                    var swConsoleOutputfile = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));
                    swConsoleOutputfile.WriteLine(mCmdRunner.CachedConsoleOutput);
                    swConsoleOutputfile.Close();
                }

                // Parse the console output file one more time to check for errors
                Thread.Sleep(250);
                ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mConsoleOutputErrorMsg);
                }

                if (!blnSuccess)
                {
                    string Msg = null;
                    Msg = "Error running SMAQC";
                    m_message = clsGlobal.AppendToComment(m_message, Msg);

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Msg + ", job " + m_JobNum);

                    if (mCmdRunner.ExitCode != 0)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                            "SMAQC returned a non-zero exit code: " + mCmdRunner.ExitCode.ToString());
                    }
                    else
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Call to SMAQC failed (but exit code is 0)");
                    }

                    blnProcessingError = true;
                }
                else
                {
                    m_progress = PROGRESS_PCT_SMAQC_COMPLETE;
                    m_StatusTools.UpdateAndWrite(m_progress);
                    if (m_DebugLevel >= 3)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "SMAQC Search Complete");
                    }
                }

                if (!blnProcessingError)
                {
                    // Parse the results file and post to the database
                    if (!ReadAndStoreSMAQCResults(resultsFilePath))
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            m_message = "Error parsing SMAQC results";
                        }
                        blnProcessingError = true;
                    }
                }

                // In use from June 2013 through November 12, 2015
                //
                //if (!blnProcessingError)
                //{
                //    var blnSuccessLLRC = ComputeLLRC();

                //    if (!blnSuccessLLRC)
                //    {
                //        // Do not treat this as a fatal error
                //        if (string.IsNullOrEmpty(m_EvalMessage))
                //            m_EvalMessage = "Unknown error computing QCDM using LLRC";
                //    }
                //}

                // Rename the SMAQC log file to remove the datestamp
                var logFilePath = RenameSMAQCLogFile();

                // Don't move the AnalysisSummary.txt file to the results folder; it doesn't have any useful information
                m_jobParams.AddResultFileToSkip("SMAQC_AnalysisSummary.txt");

                // Don't move the parameter file to the results folder, since it's not very informative
                m_jobParams.AddResultFileToSkip(strParameterFileName);

                m_progress = PROGRESS_PCT_COMPLETE;

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;

                //Add the current job data to the summary file
                if (!UpdateSummaryFile())
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                        "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                }

                //Make sure objects are released
                Thread.Sleep(500);
                // 1 second delay
                PRISM.clsProgRunner.GarbageCollectNow();

                if (blnProcessingError | result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_jobParams.AddResultFileToSkip(logFilePath);

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
                m_message = "Exception in SMAQCPlugin->RunTool";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
            //No failures so everything must have succeeded
        }

        /// <summary>
        /// Use the SMAQC metrics to compute the LLRC score
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// QCDM_2013.09.27.zip requires R 2.x
        /// We updated to R 3.x in November 2015 and have thus deprecated this method</remarks>
        [Obsolete("No longer used")]
        private bool ComputeLLRC()
        {
            bool blnSuccess = false;

            var lstDatasetIDs = new List<int>();

            var intDatasetID = m_jobParams.GetJobParameter("DatasetID", -1);

            if (!LLRC_ENABLED)
                throw new Exception("LLRC is disabled -- do not call this function");

            if (intDatasetID < 0)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Job parameter DatasetID is missing; cannot compute LLRC");
                return false;
            }

            lstDatasetIDs.Add(intDatasetID);

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "Running LLRC to compute QCDM");

            var oLLRC = new LLRC.LLRCWrapper();
            oLLRC.PostToDB = true;
            oLLRC.WorkingDirectory = m_WorkDir;

            // Add result files to skip
            m_jobParams.AddResultFileExtensionToSkip(".Rdata");
            m_jobParams.AddResultFileExtensionToSkip(".Rout");
            m_jobParams.AddResultFileExtensionToSkip(".r");
            m_jobParams.AddResultFileExtensionToSkip(".bat");
            m_jobParams.AddResultFileToSkip("data.csv");
            m_jobParams.AddResultFileToSkip("TestingDataset.csv");

            blnSuccess = oLLRC.ProcessDatasets(lstDatasetIDs);

            if (!blnSuccess)
            {
                m_EvalMessage = "Error running LLRC: " + oLLRC.ErrorMessage;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage);
            }
            else if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, "LLRC Succeeded");
            }

            return blnSuccess;
        }

        /// <summary>
        /// Looks up the InstrumentID for the dataset associated with this job
        /// </summary>
        /// <param name="InstrumentID">Output parameter</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool LookupInstrumentIDFromDB(ref int InstrumentID)
        {
            short RetryCount = 3;

            string strDatasetID = m_jobParams.GetParam("DatasetID");
            mDatasetID = 0;

            if (string.IsNullOrWhiteSpace(strDatasetID))
            {
                m_message = "DatasetID not defined";
                return false;
            }
            else if (!int.TryParse(strDatasetID, out mDatasetID))
            {
                m_message = "DatasetID is not numeric: " + strDatasetID;
                return false;
            }

            string ConnectionString = m_mgrParams.GetParam("connectionstring");
            bool blnSuccess = false;

            string SqlStr = "SELECT Instrument_ID " + "FROM V_Dataset_Instrument_List_Report " + "WHERE ID = " + strDatasetID;

            InstrumentID = 0;

            //Get a table to hold the results of the query
            while (RetryCount > 0)
            {
                try
                {
                    using (var Cn = new SqlConnection(ConnectionString))
                    {
                        var dbCmd = new SqlCommand(SqlStr, Cn);

                        Cn.Open();

                        var objResult = dbCmd.ExecuteScalar();

                        if ((objResult != null))
                        {
                            InstrumentID = (Int32) objResult;
                            blnSuccess = true;
                        }
                    }
                    //Cn
                    break;
                }
                catch (Exception ex)
                {
                    RetryCount -= 1;
                    m_message = "clsAnalysisToolRunnerSMAQC.LookupInstrumentIDFromDB; Exception obtaining InstrumentID from the database: " +
                                ex.Message + "; ConnectionString: " + ConnectionString;
                    m_message += ", RetryCount = " + RetryCount.ToString();
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                    Thread.Sleep(5000);             //Delay for 5 second before trying again
                }
            }

            //If loop exited due to errors, return false
            if (RetryCount < 1)
            {
                m_message = "clsAnalysisToolRunnerSMAQC.LookupInstrumentIDFromDB; Excessive failures obtaining InstrumentID from the database";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                return false;
            }
            else
            {
                if (!blnSuccess)
                {
                    m_message = "Error obtaining InstrumentID for dataset " + mDatasetID;
                }
                return blnSuccess;
            }
        }

        private void CopyFailedResultsToArchiveFolder()
        {
            string strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrWhiteSpace(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            m_jobParams.RemoveResultFileToSkip(SMAQC_CONSOLE_OUTPUT);

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

        private bool ConvertResultsToXML(ref List<KeyValuePair<string, string>> lstResults, ref string strXMLResults)
        {
            // XML will look like:

            // <?xml version="1.0" encoding="utf-8" standalone="yes"?>
            // <SMAQC_Results>
            //   <Dataset>QC_Shew_13_04_2e_45min_pt1_26Jun13_Leopard_13-06-32_g</Dataset>
            //   <Job>958305</Job>
            //   <PSM_Source_Job>958303</PSM_Source_Job>
            //   <Measurements>
            //     <Measurement Name="C_1A">0.002028</Measurement>
            //     <Measurement Name="C_1B">0.00583</Measurement>
            //     <Measurement Name="C_2A">23.5009</Measurement>
            //     <Measurement Name="C_3B">25.99</Measurement>
            //     <Measurement Name="C_4A">23.28</Measurement>
            //     <Measurement Name="C_4B">26.8</Measurement>
            //     <Measurement Name="C_4C">27.18</Measurement>
            //   </Measurements>
            // </SMAQC_Results>

            StringBuilder sbXML = new StringBuilder();
            strXMLResults = string.Empty;

            string strPSMSourceJob = m_jobParams.GetParam("SourceJob");

            try
            {
                sbXML.Append("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");
                sbXML.Append("<SMAQC_Results>");

                sbXML.Append("<Dataset>" + m_Dataset + "</Dataset>");
                sbXML.Append("<Job>" + m_JobNum + "</Job>");
                sbXML.Append("<PSM_Source_Job>" + strPSMSourceJob + "</PSM_Source_Job>");

                sbXML.Append("<Measurements>");

                foreach (var kvResult in lstResults)
                {
                    sbXML.Append("<Measurement Name=\"" + kvResult.Key + "\">" + kvResult.Value + "</Measurement>");
                }

                sbXML.Append("</Measurements>");
                sbXML.Append("</SMAQC_Results>");

                strXMLResults = sbXML.ToString();
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Error converting SMAQC results to XML: " + ex.Message);
                m_message = "Error converting SMAQC results to XML";
                return false;
            }

            return true;
        }

        /// <summary>
        /// Extract the results from a SMAQC results file
        /// </summary>
        /// <param name="ResultsFilePath"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private List<KeyValuePair<string, string>> LoadSMAQCResults(string ResultsFilePath)
        {
            // Typical file contents:

            // Results from Scan ID: 10
            // Instrument ID: 1
            // Scan Date: 2011-12-06 19:03:51
            // [Data]
            // Dataset, Measurement Name, Measurement Value
            // QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45, C_1A, 0.002028
            // QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45, C_1B, 0.00583
            // QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45, C_2A, 23.5009
            // QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45, C_3B, 25.99
            // QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45, C_4A, 23.28
            // QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45, C_4B, 26.8
            // QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45, C_4C, 27.18

            // The measurments are returned via this list
            var lstResults = new List<KeyValuePair<string, string>>();

            if (!File.Exists(ResultsFilePath))
            {
                m_message = "SMAQC Results file not found";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message + ": " + ResultsFilePath);
                return lstResults;
            }

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing SMAQC Results file " + ResultsFilePath);
            }

            string strLineIn = null;
            string[] strSplitLine = null;

            bool blnMeasurementsFound = false;
            bool blnHeadersFound = false;

            using (var srInFile = new StreamReader(new FileStream(ResultsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                while (!srInFile.EndOfStream)
                {
                    strLineIn = srInFile.ReadLine();

                    if (string.IsNullOrWhiteSpace(strLineIn))
                        continue;

                    if (!blnMeasurementsFound)
                    {
                        if (strLineIn.StartsWith("[Data]"))
                        {
                            blnMeasurementsFound = true;
                        }
                    }
                    else if (!blnHeadersFound)
                    {
                        if (strLineIn.StartsWith("Dataset"))
                        {
                            blnHeadersFound = true;
                        }
                    }
                    else
                    {
                        // This is a measurement result line
                        strSplitLine = strLineIn.Split(',');

                        if ((strSplitLine != null) && strSplitLine.Length >= 3)
                        {
                            lstResults.Add(new KeyValuePair<string, string>(strSplitLine[1].Trim(), strSplitLine[2].Trim()));
                        }
                    }
                }
            }

            return lstResults;
        }

        // Example Console output:
        //
        // 2/13/2012 07:15:41 PM - [Version Info]
        // 2/13/2012 07:15:41 PM - Loading Assemblies
        // 2/13/2012 07:15:41 PM - mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089
        // 2/13/2012 07:15:41 PM - SMAQC, Version=1.0.4423.30421, Culture=neutral, PublicKeyToken=null
        // 2/13/2012 07:15:41 PM - [System Information]
        // 2/13/2012 07:15:41 PM - OS Version: Microsoft Windows NT 6.1.7601 Service Pack 1
        // 2/13/2012 07:15:41 PM - Processor Count: 4
        // 2/13/2012 07:15:41 PM - Operating System Type: 64-Bit OS
        // 2/13/2012 07:15:41 PM - Page Size: 4096
        // 2/13/2012 07:15:41 PM - [LogStart]
        // 2/13/2012 07:15:41 PM - -----------------------------------------------------
        // 2/13/2012 07:15:41 PM - SMAQC Version 1.02 [BUILD DATE: Feb 10, 2012]
        // 2/13/2012 07:15:42 PM - Searching for Text Files!...
        // 2/13/2012 07:15:42 PM - Parsing and Inserting Data into DB Temp Tables!...
        // 2/13/2012 07:15:45 PM - Now running Measurements on QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45!
        // 2/13/2012 07:15:47 PM - Saving Scan Results!...
        // 2/13/2012 07:15:47 PM - Scan output has been saved to E:\DMS_WorkDir\QC_Shew_10_07_pt5_1_21Sep10_Earth_10-07-45_SMAQC.txt
        // 2/13/2012 07:15:47 PM - SMAQC analysis complete

        // This RegEx matches lines in the form:
        // 2/13/2012 07:15:42 PM - Searching for Text Files!...
        private Regex reMatchTimeStamp = new Regex(@"^\d+/\d+/\d+ \d+:\d+:\d+ [AP]M - ", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the SMAQC console output file to track progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            try
            {
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

                float sngEffectiveProgress = 0;
                sngEffectiveProgress = PROGRESS_PCT_SMAQC_STARTING;

                mConsoleOutputErrorMsg = string.Empty;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var intLinesRead = 0;
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();
                        intLinesRead += 1;

                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        // Remove the timestamp from the start of the line (if present)
                        var reMatch = reMatchTimeStamp.Match(strLineIn);
                        if (reMatch.Success)
                        {
                            strLineIn = strLineIn.Substring(reMatch.Length);
                        }

                        // Update progress if the line starts with one of the expected phrases
                        if (strLineIn.StartsWith("Searching for Text Files"))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_SMAQC_SEARCHING_FOR_FILES)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_SMAQC_SEARCHING_FOR_FILES;
                            }
                        }
                        else if (strLineIn.StartsWith("Parsing and Inserting Data"))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_SMAQC_POPULATING_DB_TEMP_TABLES)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_SMAQC_POPULATING_DB_TEMP_TABLES;
                            }
                        }
                        else if (strLineIn.StartsWith("Now running Measurements"))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_SMAQC_RUNNING_MEASUREMENTS)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_SMAQC_RUNNING_MEASUREMENTS;
                            }
                        }
                        else if (strLineIn.StartsWith("Saving Scan Results"))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_SMAQC_SAVING_RESULTS)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_SMAQC_SAVING_RESULTS;
                            }
                        }
                        else if (strLineIn.StartsWith("Scan output has been saved"))
                        {
                            // Ignore this line
                        }
                        else if (strLineIn.StartsWith("SMAQC analysis complete"))
                        {
                            // Ignore this line
                        }
                        else if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                        {
                            if (strLineIn.ToLower().Contains("error"))
                            {
                                mConsoleOutputErrorMsg += "; " + strLineIn;
                            }
                        }
                    }
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

        private string ParseSMAQCParameterFile(string strParameterFilePath)
        {
            // If necessary, could parse a parameter file and convert the options in the parameter file to command line arguments
            // As an example, see function ParseMSGFPlusParameterFile in the AnalysisManagerMSGFDBPlugIn project
            return string.Empty;
        }

        private bool PostSMAQCResultsToDB(string strXMLResults)
        {
            // Note that mDatasetID gets populated by LookupInstrumentIDFromDB

            return PostSMAQCResultsToDB(mDatasetID, strXMLResults);
        }

        private bool PostSMAQCResultsToDB(int intDatasetID, string strXMLResults)
        {
            const int MAX_RETRY_COUNT = 3;

            int intStartIndex = 0;

            string strXMLResultsClean = null;

            bool blnSuccess = false;

            try
            {
                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                        "Posting SMAQC Results to the database (using Dataset ID " + intDatasetID.ToString() + ")");
                }

                // We need to remove the encoding line from strXMLResults before posting to the DB
                // This line will look like this:
                //   <?xml version="1.0" encoding="utf-8" standalone="yes"?>

                intStartIndex = strXMLResults.IndexOf("?>", StringComparison.Ordinal);
                if (intStartIndex > 0)
                {
                    strXMLResultsClean = strXMLResults.Substring(intStartIndex + 2).Trim();
                }
                else
                {
                    strXMLResultsClean = strXMLResults;
                }

                // Call stored procedure StoreSMAQCResults in DMS5

                var objCommand = new SqlCommand();

                objCommand.CommandType = CommandType.StoredProcedure;
                objCommand.CommandText = STORE_SMAQC_RESULTS_SP_NAME;

                objCommand.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                objCommand.Parameters.Add(new SqlParameter("@DatasetID", SqlDbType.Int)).Value = intDatasetID;
                objCommand.Parameters.Add(new SqlParameter("@ResultsXML", SqlDbType.Xml)).Value = strXMLResultsClean;

                var objAnalysisTask = new clsAnalysisJob(m_mgrParams, m_DebugLevel);

                //Execute the SP (retry the call up to 4 times)
                var ResCode = objAnalysisTask.DMSProcedureExecutor.ExecuteSP(objCommand, MAX_RETRY_COUNT);

                if (ResCode == 0)
                {
                    blnSuccess = true;
                }
                else
                {
                    m_message = "Error storing SMAQC Results in database, " + STORE_SMAQC_RESULTS_SP_NAME + " returned " + ResCode.ToString();
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                    blnSuccess = false;
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception storing SMAQC Results in database";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
                blnSuccess = false;
            }

            return blnSuccess;
        }

        /// <summary>
        /// Read the SMAQC results files, convert to XML, and post to DMS
        /// </summary>
        /// <param name="ResultsFilePath">Path to the SMAQC results file</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool ReadAndStoreSMAQCResults(string ResultsFilePath)
        {
            bool blnSuccess = false;

            try
            {
                var lstResults = LoadSMAQCResults(ResultsFilePath);

                if (lstResults.Count == 0)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "No SMAQC results were found";
                    }
                }
                else
                {
                    // Convert the results to XML format
                    string strXMLResults = string.Empty;

                    blnSuccess = ConvertResultsToXML(ref lstResults, ref strXMLResults);

                    if (blnSuccess)
                    {
                        // Store the results in the database
                        blnSuccess = PostSMAQCResultsToDB(strXMLResults);
                    }
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception parsing SMAQC results";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                    "Exception parsing SMAQC results and posting to the database", ex);
                blnSuccess = false;
            }

            return blnSuccess;
        }

        /// <summary>
        /// Renames the SMAQC log file
        /// </summary>
        /// <returns>The full path to the renamed log file, or an empty string if the log file was not found</returns>
        /// <remarks></remarks>
        private string RenameSMAQCLogFile()
        {
            try
            {
                var diWorkDir = new DirectoryInfo(m_WorkDir);

                var fiFiles = diWorkDir.GetFiles("SMAQC-log*.txt");

                if ((fiFiles != null) && fiFiles.Length > 0)
                {
                    // There should only be one file; just parse fiFiles[0]
                    var strLogFilePathNew = Path.Combine(m_WorkDir, "SMAQC_log.txt");

                    if (File.Exists(strLogFilePathNew))
                    {
                        File.Delete(strLogFilePathNew);
                    }

                    fiFiles[0].MoveTo(strLogFilePathNew);

                    return strLogFilePathNew;
                }
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception renaming SMAQC log file", ex);
            }

            return string.Empty;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string strSMAQCProgLoc)
        {
            string strToolVersionInfo = string.Empty;
            bool blnSuccess = false;

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            var ioSMAQC = new FileInfo(strSMAQCProgLoc);
            if (!ioSMAQC.Exists)
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

            // Lookup the version of the SMAQC application
            blnSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, ioSMAQC.FullName);
            if (!blnSuccess)
                return false;

            if (LLRC_ENABLED)
            {
                // Lookup the version of LLRC
                blnSuccess = base.StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "LLRC");
                if (!blnSuccess)
                    return false;
            }

            // Store paths to key DLLs in ioToolFiles
            var ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(ioSMAQC);

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

        #endregion

        #region "Event Handlers"

        private DateTime dtLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15)
            {
                dtLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(m_WorkDir, SMAQC_CONSOLE_OUTPUT));

                LogProgress("SMAQC");
            }
        }

        #endregion
    }
}