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
        private int mDatasetID;

        #endregion

        private clsRunDosProgram mCmdRunner;

        #region "Methods"

        /// <summary>
        /// Runs SMAQC tool
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
                    LogDebug("clsAnalysisToolRunnerSMAQC.RunTool(): Enter");
                }

                // Determine the path to the SMAQC program
                var progLoc = DetermineProgramLocation("SMAQCProgLoc", "SMAQC.exe");

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the SMAQC version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
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

                LogMessage("Running SMAQC");

                // Set up and execute a program runner to run SMAQC
                var cmdStr = PossiblyQuotePath(m_WorkDir);                       // Path to folder containing input files
                cmdStr += " /O:" + PossiblyQuotePath(resultsFilePath);           // Text file to write the results to
                cmdStr += " /DB:" + PossiblyQuotePath(m_WorkDir);                // Folder where SQLite DB will be created
                cmdStr += " /I:" + InstrumentID;                      // Instrument ID
                cmdStr += " /M:" + PossiblyQuotePath(strParameterFilePath);      // Path to XML file specifying measurements to run

                m_jobParams.AddResultFileToSkip("SMAQC.s3db");                   // Don't keep the SQLite DB

                if (m_DebugLevel >= 1)
                {
                    LogDebug(progLoc + " " + cmdStr);
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

                var processingSuccess = mCmdRunner.RunProgram(progLoc, cmdStr, "SMAQC", true);

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
                    LogError(mConsoleOutputErrorMsg);
                }

                if (!processingSuccess)
                {
                    LogError("Error running SMAQC");

                    if (mCmdRunner.ExitCode != 0)
                    {
                        LogWarning("SMAQC returned a non-zero exit code: " + mCmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to SMAQC failed (but exit code is 0)");
                    }
                }
                else
                {
                    m_progress = PROGRESS_PCT_SMAQC_COMPLETE;
                    m_StatusTools.UpdateAndWrite(m_progress);
                    if (m_DebugLevel >= 3)
                    {
                        LogDebug("SMAQC Search Complete");
                    }
                }

                if (processingSuccess)
                {
                    // Parse the results file and post to the database
                    if (!ReadAndStoreSMAQCResults(resultsFilePath))
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            m_message = "Error parsing SMAQC results";
                        }
                        processingSuccess = false;
                    }
                }

                // In use from June 2013 through November 12, 2015
                //
                // if (processingSuccess)
                // {
                //    var blnSuccessLLRC = ComputeLLRC();

                //    if (!blnSuccessLLRC)
                //    {
                //        // Do not treat this as a fatal error
                //        if (string.IsNullOrEmpty(m_EvalMessage))
                //            m_EvalMessage = "Unknown error computing QCDM using LLRC";
                //    }
                // }

                // Rename the SMAQC log file to remove the datestamp
                var logFilePath = RenameSMAQCLogFile();

                // Don't move the AnalysisSummary.txt file to the results folder; it doesn't have any useful information
                m_jobParams.AddResultFileToSkip("SMAQC_AnalysisSummary.txt");

                // Don't move the parameter file to the results folder, since it's not very informative
                m_jobParams.AddResultFileToSkip(strParameterFileName);

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                Thread.Sleep(500);
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
                m_message = "Exception in SMAQCPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

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
            bool blnSuccess;

            List<int> lstDatasetIDs;

            var intDatasetID = m_jobParams.GetJobParameter("DatasetID", -1);

            if (!LLRC_ENABLED)
                throw new Exception("LLRC is disabled -- do not call this function");

            if (intDatasetID < 0)
            {
                LogError("Job parameter DatasetID is missing; cannot compute LLRC");
                return false;
            }

            lstDatasetIDs.Add(intDatasetID);

            LogMessage("Running LLRC to compute QCDM");

            var oLLRC = new LLRC.LLRCWrapper
            {
                PostToDB = true,
                WorkingDirectory = m_WorkDir
            };

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
                LogWarning(m_EvalMessage);
            }
            else if (m_DebugLevel >= 2)
            {
                LogMessage("LLRC Succeeded");
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

            var strDatasetID = m_jobParams.GetParam("DatasetID");
            mDatasetID = 0;

            if (string.IsNullOrWhiteSpace(strDatasetID))
            {
                m_message = "DatasetID not defined";
                return false;
            }

            if (!int.TryParse(strDatasetID, out mDatasetID))
            {
                m_message = "DatasetID is not numeric: " + strDatasetID;
                return false;
            }

            var ConnectionString = m_mgrParams.GetParam("connectionstring");
            var blnSuccess = false;

            var SqlStr = "SELECT Instrument_ID " + "FROM V_Dataset_Instrument_List_Report " + "WHERE ID = " + strDatasetID;

            InstrumentID = 0;

            // Get a table to hold the results of the query
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
                    break;
                }
                catch (Exception ex)
                {
                    RetryCount -= 1;
                    m_message = "clsAnalysisToolRunnerSMAQC.LookupInstrumentIDFromDB; Exception obtaining InstrumentID from the database: " +
                                ex.Message + "; ConnectionString: " + ConnectionString;
                    m_message += ", RetryCount = " + RetryCount;
                    LogError(m_message);

                    // Delay for 5 second before trying again
                    Thread.Sleep(5000);
                }
            }

            // If loop exited due to errors, return false
            if (RetryCount < 1)
            {
                m_message = "clsAnalysisToolRunnerSMAQC.LookupInstrumentIDFromDB; Excessive failures obtaining InstrumentID from the database";
                LogError(m_message);
                return false;
            }

            if (!blnSuccess)
            {
                m_message = "Error obtaining InstrumentID for dataset " + mDatasetID;
            }

            return blnSuccess;
        }

        private bool ConvertResultsToXML(ref List<KeyValuePair<string, string>> lstResults, out string strXMLResults)
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

            var sbXML = new StringBuilder();
            strXMLResults = string.Empty;

            var strPSMSourceJob = m_jobParams.GetParam("SourceJob");

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
                LogError("Error converting SMAQC results to XML", ex);
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
                LogDebug(m_message + ": " + ResultsFilePath);
                return lstResults;
            }

            if (m_DebugLevel >= 2)
            {
                LogDebug("Parsing SMAQC Results file " + ResultsFilePath);
            }

            var blnMeasurementsFound = false;
            var blnHeadersFound = false;

            using (var srInFile = new StreamReader(new FileStream(ResultsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                while (!srInFile.EndOfStream)
                {
                    var strLineIn = srInFile.ReadLine();

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
                        var strSplitLine = strLineIn.Split(',');

                        if (strSplitLine.Length >= 3)
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
        private readonly Regex reMatchTimeStamp = new Regex(@"^\d+/\d+/\d+ \d+:\d+:\d+ [AP]M - ", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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
                        LogDebug("Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("Parsing file " + strConsoleOutputFilePath);
                }

                var sngEffectiveProgress = PROGRESS_PCT_SMAQC_STARTING;

                mConsoleOutputErrorMsg = string.Empty;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        // Remove the timestamp from the start of the line (if present)
                        var reMatch = reMatchTimeStamp.Match(strLineIn);
                        if (reMatch.Success)
                        {
                            strLineIn = strLineIn.Substring(reMatch.Length);
                        }

                        // Update progress if the line starts with one of the expected phrases
                        if (strLineIn.StartsWith("Searching for Text Files", StringComparison.InvariantCultureIgnoreCase))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_SMAQC_SEARCHING_FOR_FILES)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_SMAQC_SEARCHING_FOR_FILES;
                            }
                        }
                        else if (strLineIn.StartsWith("Parsing and Inserting Data", StringComparison.InvariantCultureIgnoreCase))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_SMAQC_POPULATING_DB_TEMP_TABLES)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_SMAQC_POPULATING_DB_TEMP_TABLES;
                            }
                        }
                        else if (strLineIn.StartsWith("Now running Measurements", StringComparison.InvariantCultureIgnoreCase))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_SMAQC_RUNNING_MEASUREMENTS)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_SMAQC_RUNNING_MEASUREMENTS;
                            }
                        }
                        else if (strLineIn.StartsWith("Saving Scan Results", StringComparison.InvariantCultureIgnoreCase))
                        {
                            if (sngEffectiveProgress < PROGRESS_PCT_SMAQC_SAVING_RESULTS)
                            {
                                sngEffectiveProgress = PROGRESS_PCT_SMAQC_SAVING_RESULTS;
                            }
                        }
                        else if (strLineIn.StartsWith("Scan output has been saved", StringComparison.InvariantCultureIgnoreCase))
                        {
                            // Ignore this line
                        }
                        else if (strLineIn.StartsWith("SMAQC analysis complete", StringComparison.InvariantCultureIgnoreCase))
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
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + ")", ex);
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

            bool blnSuccess;

            try
            {
                if (m_DebugLevel >= 2)
                {
                    LogDebug("Posting SMAQC Results to the database (using Dataset ID " + intDatasetID + ")");
                }

                // We need to remove the encoding line from strXMLResults before posting to the DB
                // This line will look like this:
                //   <?xml version="1.0" encoding="utf-8" standalone="yes"?>

                var intStartIndex = strXMLResults.IndexOf("?>", StringComparison.Ordinal);
                string strXMLResultsClean;
                if (intStartIndex > 0)
                {
                    strXMLResultsClean = strXMLResults.Substring(intStartIndex + 2).Trim();
                }
                else
                {
                    strXMLResultsClean = strXMLResults;
                }

                // Call stored procedure StoreSMAQCResults in DMS5

                var objCommand = new SqlCommand
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandText = STORE_SMAQC_RESULTS_SP_NAME
                };

                objCommand.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                objCommand.Parameters.Add(new SqlParameter("@DatasetID", SqlDbType.Int)).Value = intDatasetID;
                objCommand.Parameters.Add(new SqlParameter("@ResultsXML", SqlDbType.Xml)).Value = strXMLResultsClean;

                var objAnalysisTask = new clsAnalysisJob(m_mgrParams, m_DebugLevel);

                // Execute the SP (retry the call up to 4 times)
                var ResCode = objAnalysisTask.DMSProcedureExecutor.ExecuteSP(objCommand, MAX_RETRY_COUNT);

                if (ResCode == 0)
                {
                    blnSuccess = true;
                }
                else
                {
                    m_message = "Error storing SMAQC Results in database, " + STORE_SMAQC_RESULTS_SP_NAME + " returned " + ResCode;
                    LogError(m_message);
                    blnSuccess = false;
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception storing SMAQC Results in database";
                LogError(m_message, ex);
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
            var blnSuccess = false;

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
                    blnSuccess = ConvertResultsToXML(ref lstResults, out var xmlResults);

                    if (blnSuccess)
                    {
                        // Store the results in the database
                        blnSuccess = PostSMAQCResultsToDB(xmlResults);
                    }
                }
            }
            catch (Exception ex)
            {
                m_message = "Exception parsing SMAQC results";
                LogError("Exception parsing SMAQC results and posting to the database", ex);
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

                if (fiFiles.Length > 0)
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
                LogError("Exception renaming SMAQC log file", ex);
            }

            return string.Empty;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDLLs = new List<string>();
            if (LLRC_ENABLED)
            {
                additionalDLLs.Add("LLRC.dll");
            }

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDLLs);

            return success;
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