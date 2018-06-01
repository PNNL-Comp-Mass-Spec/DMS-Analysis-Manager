/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 11/02/2015                                           **
**                                                              **
*****************************************************************/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerQCARTPlugin
{
    /// <summary>
    /// Class for running QC-ART
    /// </summary>
    public class clsAnalysisToolRunnerQCART : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        private const float PROGRESS_PCT_STARTING = 5;
        private const float PROGRESS_PCT_COMPLETE = 99;

        // private const string QCART_CONSOLE_OUTPUT = "QCART_ConsoleOutput.txt";

        private const string STORE_QCART_RESULTS = "StoreQCARTResults";

        #endregion

        #region "Module Variables"

        // private string mConsoleOutputFile;
        // private string mConsoleOutputErrorMsg;

        // private DateTime mLastConsoleOutputParse;
        // private DateTime mLastProgressWriteTime;

        #endregion

        #region "Methods"

        /// <summary>
        /// Processes data using QC-ART
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    DeleteLockFileIfRequired();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerQCART.RunTool(): Enter");
                }

                // Initialize classwide variables
                // (these are not used by this plugin)
                // mLastConsoleOutputParse = DateTime.UtcNow;
                // mLastProgressWriteTime = DateTime.UtcNow;

                // Determine the path to R
                var rProgLocFromRegistry = GetRPathFromWindowsRegistry();
                if (string.IsNullOrEmpty(rProgLocFromRegistry))
                {
                    DeleteLockFileIfRequired();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (!Directory.Exists(rProgLocFromRegistry))
                {
                    m_message = "R folder not found (path determined from the Windows Registry)";
                    LogError(m_message + " at " + rProgLocFromRegistry);
                    DeleteLockFileIfRequired();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var rProgLoc = Path.Combine(rProgLocFromRegistry, "R.exe");

                // Store the R.exe version info in the database
                if (!StoreToolVersionInfo(rProgLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining R version";
                    DeleteLockFileIfRequired();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Retrieve the baseline dataset names and corresponding Masic job numbers
                var datasetNamesAndJobs = GetPackedDatasetNamesAndJobs();
                if (datasetNamesAndJobs.Count == 0)
                {
                    LogError("Baseline dataset names/jobs parameter was empty; this is unexpected");
                    DeleteLockFileIfRequired();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var processingSuccess = ProcessDatasetWithQCART(rProgLoc);

                if (processingSuccess)
                {
                    // Look for the result files
                    var postProcessSuccess = PostProcessResults(datasetNamesAndJobs);
                    if (!postProcessSuccess)
                        processingSuccess = false;
                }

                DeleteLockFileIfRequired();

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Could use the following to create a summary file:
                // Add the current job data to the summary file
                // UpdateSummaryFile();

                // Make sure objects are released
                clsProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // No need to keep several files; exclude them now
                m_jobParams.AddResultFileToSkip(clsAnalysisResourcesQCART.SMAQC_DATA_FILE_NAME);
                m_jobParams.AddResultFileToSkip(clsAnalysisResourcesQCART.QCART_PROCESSING_SCRIPT_NAME);
                m_jobParams.AddResultFileToSkip(clsAnalysisResourcesQCART.NEW_BASELINE_DATASETS_METADATA_FILE);
                m_jobParams.AddResultFileToSkip(m_jobParams.GetParam(clsAnalysisResources.JOB_PARAM_PARAMETER_FILE));

                // Skip the .Rout file
                m_jobParams.AddResultFileToSkip(clsAnalysisResourcesQCART.QCART_PROCESSING_SCRIPT_NAME + "out");

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Error in QCARTPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
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
        private string ConstructXmlForDbPosting(string datasetName, int masicJob, double qcartValue)
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
                    writer.WriteValue(qcartValue.ToString("0.000000"));
                    writer.WriteEndElement(); // Measurement
                    writer.WriteEndElement(); // Measurements

                    writer.WriteEndElement(); // QCART_Results
                }

                return sb.ToString();

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error converting QC-ART results to XML; details:");
                Console.WriteLine(ex);
                return string.Empty;
            }

        }

        private bool CreateBaselineMetricsMetadataFile(
            Dictionary<string, int> datasetNamesAndJobs,
            FileSystemInfo fiNewBaselineData)
        {
            try
            {
                var cacheFolderPath = m_jobParams.GetJobParameter(clsAnalysisResourcesQCART.JOB_PARAMETER_QCART_BASELINE_RESULTS_CACHE_FOLDER, string.Empty);

                var baselineMetadataFileName = m_jobParams.GetJobParameter(clsAnalysisResourcesQCART.JOB_PARAMETER_QCART_BASELINE_METADATA_FILENAME, string.Empty);
                var baselineMetadataPathLocal = Path.Combine(m_WorkDir, baselineMetadataFileName);
                var baselineMetadataPathRemote = Path.Combine(cacheFolderPath, baselineMetadataFileName);

                var dtCurrentTime = DateTime.Now;

                var baselineDataCacheName = baselineMetadataFileName + "_" + dtCurrentTime.ToString("yyyy-MM-dd_hhmm") + ".csv";

                var baselineDataPathRemote = Path.Combine(cacheFolderPath, baselineDataCacheName);

                m_jobParams.AddResultFileToSkip(fiNewBaselineData.Name);

                var projectName = m_jobParams.GetJobParameter(clsAnalysisResourcesQCART.JOB_PARAMETER_QCART_PROJECT_NAME, string.Empty);

                // Create the new metadata file
                var baselineResultsTimestamp = dtCurrentTime.ToString("yyyy-MM-dd hh:mm:ss tt");

                using (var writer = new XmlTextWriter(new FileStream(baselineMetadataPathLocal, FileMode.Create, FileAccess.Write, FileShare.Read), Encoding.UTF8))
                {
                    writer.Formatting = Formatting.Indented;
                    writer.Indentation = 1;
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
                    AppendXmlElementWithValue(writer, "BaselineDataCacheFile", baselineDataCacheName);
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
                    LogError(m_message + ": " + ex.Message);
                    return false;
                }

                try
                {
                    // Copy the baseline data cache file to the remote path
                    m_FileTools.CopyFile(fiNewBaselineData.FullName, baselineDataPathRemote);
                }
                catch (Exception ex)
                {
                    m_message = "Exception copying the baseline data cache file to the cache folder";
                    LogError(m_message + ": " + ex.Message);
                    return false;
                }

                m_jobParams.AddResultFileToSkip(baselineMetadataFileName);

                return true;
            }
            catch (Exception ex)
            {
                m_message = "Exception creating the baseline metrics metadata file";
                LogError(m_message + ": " + ex.Message);
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
                if (int.TryParse(item.Value, out var masicJob))
                {
                    datasetNamesAndJobs.Add(item.Key, masicJob);
                }

            }

            return datasetNamesAndJobs;

        }

        private bool LoadQCARTResults(FileSystemInfo fiResults, out double qcartValue)
        {
            qcartValue = 0;
            var validData = false;
            try
            {
                using (var reader = new StreamReader(new FileStream(fiResults.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var resultsLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(resultsLine))
                        {
                            continue;
                        }

                        var dataCols = resultsLine.Split('\t').ToList();
                        if (dataCols.Count < 2)
                        {
                            LogError("QC-ART results file has fewer than 2 columns");
                            continue;
                        }

                        var datasetName = dataCols[0];
                        if (!clsGlobal.IsMatch(m_Dataset, datasetName))
                        {
                            LogError("QC-ART results file has results for an unexpected dataset: " + datasetName);
                            continue;
                        }

                        if (double.TryParse(dataCols[1], out qcartValue))
                        {
                            validData = true;
                            break;
                        }
                    }
                }

                if (validData)
                {
                    m_message = string.Empty;
                    return true;
                }

                LogError("QC-ART results file is not in the expected format");
                return false;
            }
            catch (Exception ex)
            {
                m_message = "Exception loading QC-ART results";
                LogError(m_message + ": " + ex.Message);
                return false;
            }
        }

        /*
         * Not used by this plugin
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
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }

        }
        */

        /// <summary>
        /// Read the QC-ART results file and store the score in the database
        /// If we did not use an existing baseline file, creates a new baseline metadata file
        /// and copies the metadata file plus the
        /// </summary>
        /// <param name="datasetNamesAndJobs"></param>
        /// <returns></returns>
        private bool PostProcessResults(Dictionary<string, int> datasetNamesAndJobs)
        {
            try
            {
                // Load the QC-ART results for the target dataset
                var fiResultsFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResourcesQCART.QCART_RESULTS_FILE_SUFFIX));

                if (!fiResultsFile.Exists)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("QC-ART results not found: " + fiResultsFile.Name);
                    }
                    return false;
                }

                var success = LoadQCARTResults(fiResultsFile, out var qcartValue);
                if (!success)
                {
                    return false;
                }

                success = StoreResultsInDB(qcartValue);
                if (!success)
                {
                    return false;
                }

                // Determine whether or not we used existing an baseline results file
                var existingBaselineResultsFileName = m_jobParams.GetJobParameter(clsAnalysisResourcesQCART.JOB_PARAMETER_QCART_BASELINE_RESULTS_FILENAME, string.Empty);

                if (!string.IsNullOrWhiteSpace(existingBaselineResultsFileName))
                {
                    // Existing results used
                    return true;
                }

                var fiNewBaselineData = new FileInfo(Path.Combine(m_WorkDir, clsAnalysisResourcesQCART.NEW_BASELINE_DATASETS_CACHE_FILE));
                if (!fiNewBaselineData.Exists)
                {
                    m_message = "QC-ART Processing error: new baseline data file not found";
                    LogError(m_message + ": " + fiNewBaselineData.Name);
                }

                success = CreateBaselineMetricsMetadataFile(datasetNamesAndJobs, fiNewBaselineData);
                if (success)
                    m_jobParams.AddResultFileToSkip(fiNewBaselineData.Name);

                return success;

            }
            catch (Exception ex)
            {
                m_message = "Exception post processing results";
                LogError(m_message + ": " + ex.Message);
                return false;
            }
        }

        private bool ProcessDatasetWithQCART(string rProgLoc)
        {
            // Set up and execute a program runner to run QC-ART
            // We call R.exe passing it the path to the R script file customized by clsAnalysisResourcesQCART
            // R will create a text file with the same name as the rScriptPath but with extension .Rout
            var rScriptPath = Path.Combine(m_WorkDir, clsAnalysisResourcesQCART.QCART_PROCESSING_SCRIPT_NAME);

            var cmdStr = "CMD BATCH --vanilla --slave " + PossiblyQuotePath(rScriptPath);

            if (m_DebugLevel >= 1)
            {
                LogDebug(rProgLoc + " " + cmdStr);
            }

            // Not used by this plugin
            // mConsoleOutputFile = Path.Combine(m_WorkDir, QCART_CONSOLE_OUTPUT);

            var cmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = false
                // ConsoleOutputFilePath = mCurrentConsoleOutputFile
            };
            RegisterEvents(cmdRunner);
            cmdRunner.LoopWaiting += cmdRunner_LoopWaiting;

            m_progress = PROGRESS_PCT_STARTING;

            var success = cmdRunner.RunProgram(rProgLoc, cmdStr, "QCART", true);

            /*
             * This plugin does not use ConsoleOutput files
             *
            if (!cmdRunner.WriteConsoleOutputToFile && cmdRunner.CachedConsoleOutput.Length > 0)
            {
                // Write the console output to a text file
                clsGlobal.IdleLoop(0.25);

                using (var swConsoleOutputfile = new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swConsoleOutputfile.WriteLine(cmdRunner.CachedConsoleOutput);
                }
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            // Parse the QCART_summary file to look for errors
            Thread.Sleep(250);
            var fiLogSummaryFile = new FileInfo(Path.Combine(m_WorkDir, "QCART_summary.txt"));

            ParseConsoleOutputFile(fiLogSummaryFile.FullName);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }
            */

            if (success)
            {
                return true;
            }

            m_message = "Error running QC-ART";

            LogError(m_message + ", job " + m_JobNum);

            if (cmdRunner.ExitCode != 0)
            {
                LogWarning("R.exe returned a non-zero exit code: " + cmdRunner.ExitCode);
            }
            else
            {
                LogWarning("R.exe failed (but exit code is 0)");
            }

            return false;
        }

        /*
         * Not used by this plugin
         *
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
        */

        /// <summary>
        /// Store the QC-ART score in the database
        /// </summary>
        /// <param name="qcartValue"></param>
        /// <returns></returns>
        private bool StoreResultsInDB(double qcartValue)
        {

            try
            {

                var targetDatasetMasicJob = m_jobParams.GetJobParameter("SourceJob2", 0);

                var xmlData = ConstructXmlForDbPosting(m_Dataset, targetDatasetMasicJob, qcartValue);

                // Gigasax.DMS5
                var connectionString = m_mgrParams.GetParam("connectionstring");
                var datasetID = m_jobParams.GetJobParameter(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetID", 0);

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
                LogError(m_message + ": " + ex.Message);
                return false;
            }

        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string rProgLoc)
        {

            var toolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var fiProgram = new FileInfo(rProgLoc);
            if (!fiProgram.Exists)
            {
                try
                {
                    toolVersionInfo = "Unknown";
                    return SetStepTaskToolVersion(toolVersionInfo, new List<FileInfo>(), saveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion", ex);
                    return false;
                }

            }

            StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, fiProgram.FullName);

            // Store paths to key DLLs in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                fiProgram
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, ioToolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
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

                /*
                 * This plugin does not use ConsoleOutput files
                 *
                // Parse the console output file every 15 seconds
                if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
                {
                    mLastConsoleOutputParse = DateTime.UtcNow;

                    ParseConsoleOutputFile(Path.Combine(m_WorkDir, mConsoleOutputFile));

                    LogProgress("QCART");
                }
                */
            }

        }

        #endregion
    }
}
