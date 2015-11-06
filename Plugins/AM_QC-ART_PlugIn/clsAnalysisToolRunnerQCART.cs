/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 10/05/2015                                           **
**                                                              **
*****************************************************************/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml;
using AnalysisManagerBase;
using PRISM.DataBase;
using PRISM.Files;

namespace AnalysisManagerQCARTPlugin
{
    public class clsAnalysisToolRunnerQCART : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        protected const float PROGRESS_PCT_STARTING = 5;
        protected const float PROGRESS_PCT_COMPLETE = 99;

        protected const string QCART_CONSOLE_OUTPUT_BASE = "QCART_ConsoleOutput";
        protected const string STORE_QCART_RESULTS = "StoreQCARTResults";

        #endregion

        #region "Module Variables"

        protected string mCurrentConsoleOutputFile;
        protected string mConsoleOutputErrorMsg;
        protected bool mNoPeaksFound;

        protected DateTime mLastConsoleOutputParse;
        protected DateTime mLastProgressWriteTime;

        protected int mTotalSpectra;
        protected int mSpectraProcessed;

        #endregion

        #region "Methods"

        /// <summary>
        /// Processes data using QC-ART
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override IJobParams.CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    DeleteLockFileIfRequired();
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsAnalysisToolRunnerQCART.RunTool(): Enter");
                }

                // Initialize classwide variables
                mLastConsoleOutputParse = DateTime.UtcNow;
                mLastProgressWriteTime = DateTime.UtcNow;

                mTotalSpectra = 0;
                mSpectraProcessed = 0;

                // Determine the path to R
                var rProgLoc = DetermineProgramLocation("QCART", "QCARTProgLoc", "QCART.exe");

                if (string.IsNullOrWhiteSpace(rProgLoc))
                {
                    DeleteLockFileIfRequired();
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the QCART.exe version info in the database
                if (!StoreToolVersionInfo(rProgLoc))
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining R version";
                    DeleteLockFileIfRequired();
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                var existingBaselineResultsFileName = m_jobParams.GetJobParameter(clsAnalysisResourcesQCART.JOB_PARAMETER_QCART_BASELINE_RESULTS_FILENAME, string.Empty);

                // Retrieve the baseline dataset names and corresponding Masic job numbers
                var datasetNamesAndJobs = GetPackedDatasetNamesAndJobs();
                if (datasetNamesAndJobs.Count == 0)
                {
                    LogError("Baseline dataset names/jobs parameter was empty; this is unexpected");
                    DeleteLockFileIfRequired();
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                var success = ProcessDatasetWithQCART(rProgLoc, existingBaselineResultsFileName, datasetNamesAndJobs);

                var eReturnCode = IJobParams.CloseOutType.CLOSEOUT_SUCCESS;

                if (!success)
                {
                    eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    // Look for the result files
                    success = PostProcessResults(datasetNamesAndJobs, existingBaselineResultsFileName);
                    if (!success)
                        eReturnCode = IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                DeleteLockFileIfRequired();

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Could use the following to create a summary file:
                // Add the current job data to the summary file
                // if (!UpdateSummaryFile())
                // {
                //    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Error creating summary file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
                // }

                // Make sure objects are released
                Thread.Sleep(500);
                PRISM.Processes.clsProgRunner.GarbageCollectNow();

                if (!success)
                {
                    // Move the source files and any results to the Failed Job folder
                    // Useful for debugging problems
                    CopyFailedResultsToArchiveFolder();
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                // No need to keep the JobParameters file
                m_jobParams.AddResultFileToSkip("JobParameters_" + m_JobNum + ".xml");

                var result = MakeResultsFolder();
                if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // MakeResultsFolder handles posting to local log, so set database error message and exit
                    m_message = "Error making results folder";
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                result = MoveResultFiles();
                if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that MoveResultFiles should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    m_message = "Error moving files into results folder";
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                result = CopyResultsFolderToServer();
                if (result != IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Note that CopyResultsFolderToServer should have already called clsAnalysisResults.CopyFailedResultsToArchiveFolder
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                return eReturnCode;
            }
            catch (Exception ex)
            {
                m_message = "Error in QCARTPlugin->RunTool";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Append a new element and value to the Xml
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="elementName"></param>
        /// <param name="value"></param>
        private void AppendXmlElementWithValue(XmlWriter writer, string elementName, string value)
        {
            writer.WriteStartElement(elementName);
            writer.WriteValue(value);
            writer.WriteEndElement();
        }


        /// <summary>
        /// Converts the QCDM to xml to be used by database
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="masicJob">Masic job for the dataset</param>
        /// <param name="qcartValue">QC-ART value</param>
        /// <returns></returns>
        private string ConstructXmlForDbPosting(string datasetName, int masicJob, float qcartValue)
        {

            try
            {

                var sb = new StringBuilder();
                var settings = new XmlWriterSettings
                {
                    OmitXmlDeclaration = true,
                    Indent = true
                };

                using (var writer = XmlWriter.Create(sb, settings))
                {
                    writer.WriteStartElement("QCART_Results");
                    AppendXmlElementWithValue(writer, "Dataset", datasetName);
                    AppendXmlElementWithValue(writer, "MASIC_Job", masicJob.ToString());

                    writer.WriteStartElement("Measurements");
                    writer.WriteStartElement("Measurement");
                    writer.WriteAttributeString("Name", "QCART");
                    writer.WriteValue(qcartValue.ToString("0.0000"));
                    writer.WriteEndElement(); // Measurement
                    writer.WriteEndElement(); // Measurements

                    writer.WriteEndElement(); // QCART_Results
                }

                return sb.ToString();

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error converting Quameter results to XML; details:");
                Console.WriteLine(ex);
                return string.Empty;
            }

        }

        protected void CopyFailedResultsToArchiveFolder()
        {
            var strFailedResultsFolderPath = m_mgrParams.GetParam("FailedResultsFolderPath");
            if (string.IsNullOrWhiteSpace(strFailedResultsFolderPath))
                strFailedResultsFolderPath = "??Not Defined??";

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Processing interrupted; copying results to archive folder: " + strFailedResultsFolderPath);

            // Bump up the debug level if less than 2
            if (m_DebugLevel < 2)
                m_DebugLevel = 2;

            // Try to save whatever files are in the work directory (however, delete the XML files first)
            var strFolderPathToArchive = string.Copy(m_WorkDir);

            // Make the results folder
            var result = MakeResultsFolder();
            if (result == IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Move the result files into the result folder
                result = MoveResultFiles();
                if (result == IJobParams.CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Move was a success; update strFolderPathToArchive
                    strFolderPathToArchive = Path.Combine(m_WorkDir, m_ResFolderName);
                }
            }

            // Copy the results folder to the Archive folder
            var objAnalysisResults = new clsAnalysisResults(m_mgrParams, m_jobParams);
            objAnalysisResults.CopyFailedResultsToArchiveFolder(strFolderPathToArchive);

        }

        private bool CreateBaselineMetricsMetadataFile(Dictionary<string, int> datasetNamesAndJobs)
        {
            try
            {
                var cacheFolderPath = m_jobParams.GetJobParameter(clsAnalysisResourcesQCART.JOB_PARAMETER_QCART_BASELINE_RESULTS_CACHE_FOLDER, string.Empty);

                var baselineMetadataFileName = m_jobParams.GetJobParameter(clsAnalysisResourcesQCART.JOB_PARAMETER_QCART_BASELINE_METADATA_FILENAME, string.Empty);
                var baselineMetadataPathLocal = Path.Combine(m_WorkDir, baselineMetadataFileName);
                var baselineMetadataPathRemote = Path.Combine(cacheFolderPath, baselineMetadataFileName);

                var baselineResultsDataFileName = baselineMetadataFileName + "_" +
                                                  DateTime.Now.ToString("yyyy-MM-dd_hhmm") + ".csv";
                var baselineResultsPathLocal = Path.Combine(m_WorkDir, baselineResultsDataFileName);
                var baselineResultsPathRemote = Path.Combine(cacheFolderPath, baselineResultsDataFileName);

                m_jobParams.AddResultFileToSkip(baselineResultsPathLocal);

                var projectName = m_jobParams.GetJobParameter(clsAnalysisResourcesQCART.JOB_PARAMETER_QCART_PROJECT_NAME, string.Empty);

                // Create the new metadata file
                var baselineResultsTimestamp = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt");

                using (var writer = new XmlTextWriter(new FileStream(baselineMetadataPathLocal, FileMode.Create, FileAccess.Write, FileShare.Read), Encoding.UTF8))
                {
                    writer.Formatting = Formatting.Indented;
                    writer.Indentation = 4;
                    writer.IndentChar = '\t';

                    writer.WriteStartElement("Parameters");
                    
                    writer.WriteStartElement("Metadata");
                    AppendXmlElementWithValue(writer, "Project", projectName);
                    writer.WriteEndElement();

                    writer.WriteStartElement("BaselineList");
                    
                    var query = (from item in datasetNamesAndJobs orderby item.Key select item);
                    foreach (var item in query)
                    {
                        writer.WriteStartElement("BaselineDataset");
                        AppendXmlElementWithValue(writer, "MasicJob", item.Value.ToString());
                        AppendXmlElementWithValue(writer, "Dataset", item.Key);
                        writer.WriteEndElement();
                    }
                    writer.WriteEndElement();

                    writer.WriteStartElement("Results");
                    AppendXmlElementWithValue(writer, "Timestamp", baselineResultsTimestamp);
                    AppendXmlElementWithValue(writer, "BaselineResultsDataFile", baselineResultsDataFileName);
                    writer.WriteEndElement();

                    writer.WriteEndElement(); // </Parameters>
                }

                try 
                {
                    // Copy the metadata file to the remote path
                    m_FileTools.CopyFile(baselineMetadataPathLocal, baselineMetadataPathRemote);
                }
                catch (Exception ex)
                {
                    m_message = "Exception copying the baseline metadata file to the cache folder";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                         m_message + ": " + ex.Message);
                    return false;
                }

                try
                {
                    // Copy the baseline results file to the remote path
                    m_FileTools.CopyFile(baselineResultsPathLocal, baselineResultsPathRemote);
                }
                catch (Exception ex)
                {
                    m_message = "Exception copying the baseline results file to the cache folder";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                         m_message + ": " + ex.Message);
                    return false;
                }

            
                return true;
            }
            catch (Exception ex)
            {
                m_message = "Exception creating the baseline metrics metadata file";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG,
                                     m_message + ": " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Delete the baseline metadata lock file if it exists
        /// </summary>
        private void DeleteLockFileIfRequired()
        {
            var lockFilePath = m_jobParams.GetJobParameter(clsAnalysisResourcesQCART.JOB_PARAMETER_QCART_BASELINE_METADATA_LOCKFILE, string.Empty);

            if (string.IsNullOrWhiteSpace(lockFilePath))
                return;

            try
            {
                var fiLockFile = new FileInfo(lockFilePath);
                if (fiLockFile.Exists)
                    fiLockFile.Delete();
            }
            catch
            {
                // Ignore errors here
            }

        }

        /// <summary>
        /// Retrieve the baseline dataset names and corresponding Masic jobs that were stored by clsAnalysisResourcesQCART
        /// </summary>
        /// <returns>Dictionary of dataset names and Masic jobs</returns>
        private Dictionary<string, int> GetPackedDatasetNamesAndJobs()
        {

            var datasetNamesAndJobsText = ExtractPackedJobParameterDictionary(clsAnalysisResourcesQCART.JOB_PARAMETER_QCART_BASELINE_DATASET_NAMES_AND_JOBS);

            var datasetNamesAndJobs = new Dictionary<string, int>(datasetNamesAndJobsText.Count);

            foreach (var item in datasetNamesAndJobsText)
            {
                int masicJob;
                if (int.TryParse(item.Value, out masicJob))
                {
                    datasetNamesAndJobs.Add(item.Key, masicJob);
                }

            }

            return datasetNamesAndJobs;

        }

        private bool LoadQCARTResults(FileInfo fiResults, out float qcartValue)
        {
            qcartValue = 0;

            try
            {

                m_message = "LoadQCARTResults not implemented!";

                throw new NotImplementedException();
            }
            catch (Exception ex)
            {
                m_message = "Exception loading QC-Art results";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message + ": " + ex.Message);
                return false;              
            }
        }

        /// <summary>
        /// Parse the QCART console output file to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks>Not used at present</remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            // Example Console output
            //
            // ...
            // 

            try
            {

                // Nothing to do

            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }

        }

        private bool PostProcessResults(Dictionary<string, int> datasetNamesAndJobs, string existingBaselineResultsFileName)
        {
            try
            {
                var diWorkDir = new DirectoryInfo(m_WorkDir);
                var fiResultsFiles = diWorkDir.GetFiles("QCART_results.csv").ToList();

                if (fiResultsFiles.Count == 0)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "QCART results not found";
                    }
                    return false;
                }

                float qcartValue;

                var success = LoadQCARTResults(fiResultsFiles.FirstOrDefault(), out qcartValue);
                if (!success)
                {
                    return false;
                }

                success = StoreResultsInDb(datasetNamesAndJobs, qcartValue);

                if (success && string.IsNullOrWhiteSpace(existingBaselineResultsFileName))
                {
                    CreateBaselineMetricsMetadataFile(datasetNamesAndJobs);
                }

                return success;

            }
            catch (Exception ex)
            {
                m_message = "Exception post processing results";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message + ": " + ex.Message);
                return false;              
            }
        }
  
        private bool ProcessDatasetWithQCART(
            string rProgLoc,
            string existingBaselineResultsFileName,
            Dictionary<string, int> datasetNamesAndJobs)
        {
            // Set up and execute a program runner to run QC-ART
            // We create a batch file that calls R using a custom R script

            var cmdStr = string.Empty;
            // cmdStr += " " + PossiblyQuotePath(spectrumFile.FullName);

            /*
            if (m_DebugLevel >= 1)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, progLoc + " " + cmdStr);
            }

            scanNumber = filesProcessed;

            var reMatch = reGetScanNumber.Match(spectrumFile.Name);
            if (reMatch.Success)
            {
                int.TryParse(reMatch.Groups[1].Value, out scanNumber);
            }

            mCurrentConsoleOutputFile = Path.Combine(m_WorkDir, QCART_CONSOLE_OUTPUT_BASE + scanNumber + ".txt");

            var cmdRunner = new clsRunDosProgram(m_WorkDir)
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = false
                // ConsoleOutputFilePath = mCurrentConsoleOutputFile
            };

            cmdRunner.LoopWaiting += cmdRunner_LoopWaiting;

            var subTaskProgress = filesProcessed / (float)mTotalSpectra * 100;

            m_progress = ComputeIncrementalProgress(PROGRESS_PCT_STARTING, PROGRESS_PCT_COMPLETE, subTaskProgress);

            var success = cmdRunner.RunProgram(progLoc, cmdStr, "QCART", true);

            if (!cmdRunner.WriteConsoleOutputToFile && cmdRunner.CachedConsoleOutput.Length > 0)
            {
                // Write the console output to a text file
                Thread.Sleep(250);

                var swConsoleOutputfile =
                    new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create,
                                                    FileAccess.Write, FileShare.Read));
                swConsoleOutputfile.WriteLine(cmdRunner.CachedConsoleOutput);
                swConsoleOutputfile.Close();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     mConsoleOutputErrorMsg);
            }

            // Parse the QCART_summary file to look for errors
            Thread.Sleep(250);
            var fiLogSummaryFile = new FileInfo(Path.Combine(m_WorkDir, "QCART_summary.txt"));
            if (!fiLogSummaryFile.Exists)
            {
                // Summary file not created
                // Look for a log file in folder C:\Users\d3l243\AppData\Local\
                var alternateLogPath = Path.Combine(@"C:\Users", GetUsername(), @"AppData\Local\QCART.log");

                var fiAlternateLogFile = new FileInfo(alternateLogPath);
                if (fiAlternateLogFile.Exists)
                    fiAlternateLogFile.CopyTo(fiLogSummaryFile.FullName, true);
            }

            ParseConsoleOutputFile(fiLogSummaryFile.FullName);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                     mConsoleOutputErrorMsg);
            }

            if (success)
            {
                return true;
            }

            var msg = "Error processing scan " + scanNumber + " using QCART";
            m_message = clsGlobal.AppendToComment(m_message, msg);

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                 msg + ", job " + m_JobNum);

            if (cmdRunner.ExitCode != 0)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                     "QCART returned a non-zero exit code: " + cmdRunner.ExitCode);
            }
            else
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                                     "QCART failed (but exit code is 0)");
            }
            */

            return false;
        }


        private void StoreConsoleErrorMessage(StreamReader srInFile, string strLineIn)
        {
            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                mConsoleOutputErrorMsg = "Error running QCART:";
            }
            mConsoleOutputErrorMsg += "; " + strLineIn;

            while (!srInFile.EndOfStream)
            {
                // Store the remaining console output lines
                strLineIn = srInFile.ReadLine();

                if (!string.IsNullOrWhiteSpace(strLineIn) && !strLineIn.StartsWith("========"))
                {
                    mConsoleOutputErrorMsg += "; " + strLineIn;
                }

            }
        }

        private bool StoreResultsInDb(Dictionary<string, int> datasetNamesAndJobs, float qcartValue)
        {
              
            try
            {
               
                var query = (from item in datasetNamesAndJobs where string.Equals(item.Key, m_Dataset, StringComparison.CurrentCultureIgnoreCase) select item.Value).ToList();
                if (query.Count == 0)
                {
                    LogError("Dataset " + m_Dataset + " not found in the cached dataset names and jobs");
                    return false;
                }

                var xmlData = ConstructXmlForDbPosting(m_Dataset, query.First(), qcartValue);

                // Gigasax.DMS5
                var connectionString = m_mgrParams.GetParam("connectionstring");
                var datasetID = m_jobParams.GetJobParameter("JobParameters", "DatasetID", 0);


                // Call stored procedure StoreQCARTResults
                // Retry up to 3 times

                var objCommand = new System.Data.SqlClient.SqlCommand();

                {
                    objCommand.CommandType = System.Data.CommandType.StoredProcedure;
                    objCommand.CommandText = STORE_QCART_RESULTS;

                    objCommand.Parameters.Add(new System.Data.SqlClient.SqlParameter("@Return", System.Data.SqlDbType.Int));
                    objCommand.Parameters["@Return"].Direction = System.Data.ParameterDirection.ReturnValue;

                    objCommand.Parameters.Add(new System.Data.SqlClient.SqlParameter("@DatasetID", System.Data.SqlDbType.Int));
                    objCommand.Parameters["@DatasetID"].Direction = System.Data.ParameterDirection.Input;
                    objCommand.Parameters["@DatasetID"].Value = datasetID;

                    objCommand.Parameters.Add(new System.Data.SqlClient.SqlParameter("@ResultsXML", System.Data.SqlDbType.Xml));
                    objCommand.Parameters["@ResultsXML"].Direction = System.Data.ParameterDirection.Input;
                    objCommand.Parameters["@ResultsXML"].Value = xmlData;
                }

                var executor = new clsExecuteDatabaseSP(connectionString);

                var returnCode = executor.ExecuteSP(objCommand, 3);

                if (returnCode == 0)
                {
                    return true;
                }

                LogError("Error storing the QC-ART result in the database");
                return false;

            }
            catch (Exception ex)
            {
                m_message = "Exception storing QCART Results in database";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, m_message + ": " + ex.Message);
                return false;
            }

        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string strProgLoc)
        {

            var strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Determining tool version info");
            }

            var fiProgram = new FileInfo(strProgLoc);
            if (!fiProgram.Exists)
            {
                try
                {
                    strToolVersionInfo = "Unknown";
                    return base.SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), blnSaveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion", ex);
                    return false;
                }

            }

            // Lookup the version of the .NET program
            StoreToolVersionInfoOneFile(ref strToolVersionInfo, fiProgram.FullName);

            // Store paths to key DLLs in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                fiProgram
            };

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception calling SetStepTaskToolVersion", ex);
                return false;
            }

        }

        private bool UpdateParameterFile(string paramFilePath, string targetsFileName)
        {
            try
            {
                var fiParamFile = new FileInfo(paramFilePath);
                if (fiParamFile.DirectoryName == null)
                {
                    LogError("Directory for parameter file found to be null in UpdateParameterFile");
                    return false;
                }

                if (string.IsNullOrWhiteSpace(targetsFileName))
                {
                    // Leave the parameter file unchanged
                    m_EvalMessage = "Warning: targets file was empty; parameter file used as-is";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, m_EvalMessage);
                    return true;
                }

                var fiParamFileNew = new FileInfo(fiParamFile.FullName + ".new");

                using (var reader = new StreamReader(new FileStream(fiParamFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var writer = new StreamWriter(new FileStream(fiParamFileNew.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine();
                            continue;
                        }

                        if (dataLine.Trim().ToLower().StartsWith("param_dm_target_file"))
                        {
                            writer.WriteLine("param_dm_target_file=" + Path.Combine(m_WorkDir, targetsFileName));
                        }
                        else
                        {
                            writer.WriteLine(dataLine);
                        }
                    }
                }

                fiParamFile.MoveTo(Path.Combine(fiParamFile.DirectoryName, fiParamFile.Name + ".old"));
                Thread.Sleep(100);

                fiParamFileNew.MoveTo(paramFilePath);

                // Skip the old parameter file
                m_jobParams.AddResultFileToSkip(fiParamFile.Name);
                return true;
            }
            catch (Exception ex)
            {
                m_message = "Error in QCARTPlugin->UpdateParameterFile";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
                return false;
            }
        }

        #endregion

        #region "Event Handlers"

        void cmdRunner_LoopWaiting()
        {

            // Synchronize the stored Debug level with the value stored in the database

            {
                UpdateStatusFile();

                // Parse the console output file every 15 seconds
                if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
                {
                    mLastConsoleOutputParse = DateTime.UtcNow;

                    ParseConsoleOutputFile(Path.Combine(m_WorkDir, mCurrentConsoleOutputFile));

                    LogProgress("QCART");
                }

            }

        }

        #endregion
    }
}
