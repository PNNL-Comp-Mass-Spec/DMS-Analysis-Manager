/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 11/02/2015                                           **
**                                                              **
*****************************************************************/

using AnalysisManagerBase;
using PRISM;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISMDatabaseUtils;

namespace AnalysisManagerQCARTPlugin
{
    /// <summary>
    /// Class for running QC-ART
    /// </summary>
    public class AnalysisToolRunnerQCART : AnalysisToolRunnerBase
    {
        // Ignore Spelling: yyyy-MM-dd hh:mm:ss tt

        private const int PROGRESS_PCT_STARTING = 5;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private const string STORE_QCART_RESULTS = "store_qcart_results";

        // private string mConsoleOutputFile;
        // private string mConsoleOutputErrorMsg;

        // private DateTime mLastConsoleOutputParse;
        // private DateTime mLastProgressWriteTime;

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

                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerQCART.RunTool(): Enter");
                }

                // Initialize class wide variables
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
                    mMessage = "R folder not found (path determined from the Windows Registry)";
                    LogError(mMessage + " at " + rProgLocFromRegistry);
                    DeleteLockFileIfRequired();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var rProgLoc = Path.Combine(rProgLocFromRegistry, "R.exe");

                // Store the R.exe version info in the database
                if (!StoreToolVersionInfo(rProgLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining R version";
                    DeleteLockFileIfRequired();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Retrieve the baseline dataset names and corresponding MASIC job numbers
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

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Could use the following to create a summary file:
                // Add the current job data to the summary file
                // UpdateSummaryFile();

                // Make sure objects are released
                AppUtils.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // No need to keep several files; exclude them now
                mJobParams.AddResultFileToSkip(AnalysisResourcesQCART.SMAQC_DATA_FILE_NAME);
                mJobParams.AddResultFileToSkip(AnalysisResourcesQCART.QCART_PROCESSING_SCRIPT_NAME);
                mJobParams.AddResultFileToSkip(AnalysisResourcesQCART.NEW_BASELINE_DATASETS_METADATA_FILE);
                mJobParams.AddResultFileToSkip(mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE));

                // Skip the .Rout file
                mJobParams.AddResultFileToSkip(AnalysisResourcesQCART.QCART_PROCESSING_SCRIPT_NAME + "out");

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in QCARTPlugin->RunTool";
                LogError(mMessage, ex);
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

        // ReSharper disable once CommentTypo

        /// <summary>
        /// Converts the QCDM to xml to be used by database
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="masicJob">MASIC job for the dataset</param>
        /// <param name="qcartValue">QC-ART value</param>
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

                using var writer = XmlWriter.Create(sb, settings);

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
            FileSystemInfo newBaselineData)
        {
            try
            {
                var cacheFolderPath = mJobParams.GetJobParameter(AnalysisResourcesQCART.JOB_PARAMETER_QCART_BASELINE_RESULTS_CACHE_FOLDER, string.Empty);

                var baselineMetadataFileName = mJobParams.GetJobParameter(AnalysisResourcesQCART.JOB_PARAMETER_QCART_BASELINE_METADATA_FILENAME, string.Empty);
                var baselineMetadataPathLocal = Path.Combine(mWorkDir, baselineMetadataFileName);
                var baselineMetadataPathRemote = Path.Combine(cacheFolderPath, baselineMetadataFileName);

                var currentTime = DateTime.Now;

                var baselineDataCacheName = baselineMetadataFileName + "_" + currentTime.ToString("yyyy-MM-dd_hhmm") + ".csv";

                var baselineDataPathRemote = Path.Combine(cacheFolderPath, baselineDataCacheName);

                mJobParams.AddResultFileToSkip(newBaselineData.Name);

                var projectName = mJobParams.GetJobParameter(AnalysisResourcesQCART.JOB_PARAMETER_QCART_PROJECT_NAME, string.Empty);

                // Create the new metadata file
                var baselineResultsTimestamp = currentTime.ToString("yyyy-MM-dd hh:mm:ss tt");

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
                    mFileTools.CopyFile(baselineMetadataPathLocal, baselineMetadataPathRemote);
                }
                catch (Exception ex)
                {
                    mMessage = "Exception copying the baseline metadata file to the cache folder";
                    LogError(mMessage + ": " + ex.Message);
                    return false;
                }

                try
                {
                    // Copy the baseline data cache file to the remote path
                    mFileTools.CopyFile(newBaselineData.FullName, baselineDataPathRemote);
                }
                catch (Exception ex)
                {
                    mMessage = "Exception copying the baseline data cache file to the cache folder";
                    LogError(mMessage + ": " + ex.Message);
                    return false;
                }

                mJobParams.AddResultFileToSkip(baselineMetadataFileName);

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Exception creating the baseline metrics metadata file";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Delete the baseline metadata lock file if it exists
        /// </summary>
        private void DeleteLockFileIfRequired()
        {
            var lockFilePath = mJobParams.GetJobParameter(AnalysisResourcesQCART.JOB_PARAMETER_QCART_BASELINE_METADATA_LOCKFILE, string.Empty);

            if (string.IsNullOrWhiteSpace(lockFilePath))
                return;

            try
            {
                var lockFile = new FileInfo(lockFilePath);

                if (lockFile.Exists)
                    lockFile.Delete();
            }
            catch
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Retrieve the baseline dataset names and corresponding MASIC jobs that were stored by AnalysisResourcesQCART
        /// </summary>
        /// <returns>Dictionary of dataset names and MASIC jobs</returns>
        private Dictionary<string, int> GetPackedDatasetNamesAndJobs()
        {
            var datasetNamesAndJobsText = ExtractPackedJobParameterDictionary(AnalysisResourcesQCART.JOB_PARAMETER_QCART_BASELINE_DATASET_NAMES_AND_JOBS);

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

        private bool LoadQCARTResults(FileSystemInfo results, out double qcartValue)
        {
            qcartValue = 0;
            var validData = false;
            try
            {
                using (var reader = new StreamReader(new FileStream(results.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
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

                        if (!Global.IsMatch(mDatasetName, datasetName))
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
                    mMessage = string.Empty;
                    return true;
                }

                LogError("QC-ART results file is not in the expected format");
                return false;
            }
            catch (Exception ex)
            {
                mMessage = "Exception loading QC-ART results";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }
        }

        /*
         * Not used by this plugin
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
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
                if (mDebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
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
        private bool PostProcessResults(Dictionary<string, int> datasetNamesAndJobs)
        {
            try
            {
                // Load the QC-ART results for the target dataset
                var resultsFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + AnalysisResourcesQCART.QCART_RESULTS_FILE_SUFFIX));

                if (!resultsFile.Exists)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("QC-ART results not found: " + resultsFile.Name);
                    }
                    return false;
                }

                var success = LoadQCARTResults(resultsFile, out var qcartValue);

                if (!success)
                {
                    return false;
                }

                success = StoreResultsInDB(qcartValue);

                if (!success)
                {
                    return false;
                }

                // Determine whether we used an existing baseline results file
                var existingBaselineResultsFileName = mJobParams.GetJobParameter(AnalysisResourcesQCART.JOB_PARAMETER_QCART_BASELINE_RESULTS_FILENAME, string.Empty);

                if (!string.IsNullOrWhiteSpace(existingBaselineResultsFileName))
                {
                    // Existing results used
                    return true;
                }

                var newBaselineData = new FileInfo(Path.Combine(mWorkDir, AnalysisResourcesQCART.NEW_BASELINE_DATASETS_CACHE_FILE));

                if (!newBaselineData.Exists)
                {
                    mMessage = "QC-ART Processing error: new baseline data file not found";
                    LogError(mMessage + ": " + newBaselineData.Name);
                }

                success = CreateBaselineMetricsMetadataFile(datasetNamesAndJobs, newBaselineData);

                if (success)
                    mJobParams.AddResultFileToSkip(newBaselineData.Name);

                return success;
            }
            catch (Exception ex)
            {
                mMessage = "Exception post-processing results";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }
        }

        private bool ProcessDatasetWithQCART(string rProgLoc)
        {
            // Set up and execute a program runner to run QC-ART
            // We call R.exe passing it the path to the R script file customized by AnalysisResourcesQCART
            // R will create a text file with the same name as the rScriptPath but with extension .Rout
            var rScriptPath = Path.Combine(mWorkDir, AnalysisResourcesQCART.QCART_PROCESSING_SCRIPT_NAME);

            var arguments = "CMD BATCH" +
                            " --vanilla" +
                            " --slave " + PossiblyQuotePath(rScriptPath);

            if (mDebugLevel >= 1)
            {
                LogDebug(rProgLoc + " " + arguments);
            }

            // Not used by this plugin
            // mConsoleOutputFile = Path.Combine(mWorkDir, QCART_CONSOLE_OUTPUT);

            var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = false
                // ConsoleOutputFilePath = mCurrentConsoleOutputFile
            };
            RegisterEvents(cmdRunner);
            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mProgress = PROGRESS_PCT_STARTING;

            var success = cmdRunner.RunProgram(rProgLoc, arguments, "QCART", true);

            /*
             * This plugin does not use ConsoleOutput files
             *
            if (!cmdRunner.WriteConsoleOutputToFile && cmdRunner.CachedConsoleOutput.Length > 0)
            {
                // Write the console output to a text file
                Global.IdleLoop(0.25);

                using (var writer = new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine(cmdRunner.CachedConsoleOutput);
                }
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            // Parse the QCART_summary file to look for errors
            Thread.Sleep(250);
            var logSummaryFile = new FileInfo(Path.Combine(mWorkDir, "QCART_summary.txt"));

            ParseConsoleOutputFile(logSummaryFile.FullName);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }
            */

            if (success)
            {
                return true;
            }

            mMessage = "Error running QC-ART";

            LogError(mMessage + ", job " + mJob);

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
        private void StoreConsoleErrorMessage(StreamReader inFile, string lineIn)
        {
            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                mConsoleOutputErrorMsg = "Error running QCART:";
            }
            mConsoleOutputErrorMsg += "; " + lineIn;

            while (!inFile.EndOfStream)
            {
                // Store the remaining console output lines
                lineIn = inFile.ReadLine();

                if (!string.IsNullOrWhiteSpace(lineIn) && !lineIn.StartsWith("========"))
                {
                    mConsoleOutputErrorMsg += "; " + lineIn;
                }

            }
        }
        */

        /// <summary>
        /// Store the QC-ART score in the database
        /// </summary>
        /// <param name="qcartValue"></param>
        private bool StoreResultsInDB(double qcartValue)
        {
            try
            {
                var targetDatasetMasicJob = mJobParams.GetJobParameter("SourceJob2", 0);

                var xmlData = ConstructXmlForDbPosting(mDatasetName, targetDatasetMasicJob, qcartValue);

                // SQL Server: Data Source=Gigasax;Initial Catalog=DMS5
                // PostgreSQL: Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms
                var connectionString = mMgrParams.GetParam("ConnectionString");

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrName);

                var datasetID = mJobParams.GetJobParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "DatasetID", 0);

                // Call stored procedure store_qcart_results
                // Retry up to 3 times

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
                RegisterEvents(dbTools);

                var cmd = dbTools.CreateCommand(STORE_QCART_RESULTS, CommandType.StoredProcedure);

                // Define parameter for procedure's return value
                // If querying a Postgres DB, dbTools will auto-change "@return" to "_returnCode"
                var returnParam = dbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);

                dbTools.AddTypedParameter(cmd, "@datasetID", SqlType.Int, value: datasetID);
                dbTools.AddParameter(cmd, "@resultsXML", SqlType.XML).Value = xmlData;

                var resCode = dbTools.ExecuteSP(cmd);

                var returnCode = DBToolsBase.GetReturnCode(returnParam);

                if (resCode == 0 && returnCode == 0)
                {
                    return true;
                }

                if (resCode != 0 && returnCode == 0)
                {
                    LogError(
                        "ExecuteSP() reported result code {0} storing the QC-ART result in database using {1}",
                        resCode, STORE_QCART_RESULTS);
                }
                else
                {
                    LogError(
                        "Error storing the QC-ART result in database, {0} returned {1}",
                        STORE_QCART_RESULTS, returnParam.Value.CastDBVal<string>());
                }

                return false;
            }
            catch (Exception ex)
            {
                mMessage = "Exception storing QCART Results in database";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string rProgLoc)
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var program = new FileInfo(rProgLoc);

            if (!program.Exists)
            {
                try
                {
                    toolVersionInfo = "Unknown";
                    return SetStepTaskToolVersion(toolVersionInfo, new List<FileInfo>(), false);
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion", ex);
                    return false;
                }
            }

            mToolVersionUtilities.StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, program.FullName);

            // Store paths to key DLLs in toolFiles
            var toolFiles = new List<FileInfo>
            {
                program
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private void CmdRunner_LoopWaiting()
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

                    ParseConsoleOutputFile(Path.Combine(mWorkDir, mConsoleOutputFile));

                    LogProgress("QCART");
                }
                */
            }
        }
    }
}
