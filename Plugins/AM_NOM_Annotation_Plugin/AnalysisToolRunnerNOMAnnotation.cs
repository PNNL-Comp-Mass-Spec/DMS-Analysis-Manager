//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2026, Battelle Memorial Institute
// Created 04/02/2026
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;
using Newtonsoft.Json;
using PRISM;
using PRISMDatabaseUtils;
using pwiz.ProteowizardWrapper;

namespace AnalysisManagerNOMAnnotationPlugin
{
    /// <summary>
    /// Class for annotating natural organic matter features
    /// </summary>
    public class AnalysisToolRunnerNOMAnnotation : AnalysisToolRunnerBase
    {
        // Ignore Spelling:

        /// <summary>
        /// Procedure used to store the natural organic matter stats in the database
        /// </summary>
        private const string UPDATE_NOM_STATS_PROCEDURE = "update_dataset_nom_stats_xml";

        private const string NOM_ANNOTATION_CONSOLE_OUTPUT_FILE = "NOM_Annotation_ConsoleOutput.txt";

        private const string NOM_ANNOTATION_PYTHON_SCRIPT = "smaqc_nom_mass_spec_metrics.py";

        /// <summary>
        /// Progress percent value when preparing to annotate NOM features
        /// </summary>
        public const int PROGRESS_PCT_INITIALIZING = 4;

        private const int PROGRESS_PCT_ANNOTATING_NOM_FEATURES = 5;
        private const int PROGRESS_PCT_FINISHED_ANNOTATION = 90;
        private const int PROGRESS_PCT_STORING_RESULTS_IN_DB = 95;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private CancellationTokenSource mCancellationToken;
        private DateTime mGetScanTimesStartTime;

        /// <summary>
        /// Maximum time to wait for msDataFileReader.GetScanTimesAndMsLevels() to finish determining scan times and MS levels
        /// </summary>
        /// <remarks>Prior to August 2024 this was set to 90 seconds. It was increased to 900 seconds to handle timsTOF SCP datasets</remarks>
        private int mGetScanTimesMaxWaitTimeSeconds;
        private bool mGetScanTimesAutoAborted;

        private DateTime mLastScanLoadingDebugProgressTime;
        private DateTime mLastScanLoadingStatusProgressTime;
        private bool mReportedTotalSpectraToExamine;

        private string mConsoleOutputFile;
        private string mConsoleOutputErrorMsg;

        private DateTime mLastConsoleOutputParse;

        /// <summary>
        /// Identify natural organic matter features in mass spectra
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
                    LogDebug("AnalysisToolRunnerNOMAnnotation.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                // Determine the path to the NOM annotation python script
                var pythonScriptPath = DetermineProgramLocation("NOMAnnotationProgLoc", NOM_ANNOTATION_PYTHON_SCRIPT);

                if (string.IsNullOrWhiteSpace(pythonScriptPath))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var pythonScriptFile = new FileInfo(pythonScriptPath);

                if (!pythonScriptFile.Exists)
                {
                    LogError("NOM annotation script file not found: " + pythonScriptFile.FullName);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the smaqc_nom_mass_spec_metrics.py version info in the database
                if (!StoreToolVersionInfo(pythonScriptFile.FullName))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining smaqc_nom_mass_spec_metrics.py version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Process the data using the NOM metrics script
                var processingSuccess = ComputeNOMMetricsForDatasets(pythonScriptFile, out var resultFiles, out var datasetsByID);

                CloseOutType eReturnCode;

                if (!processingSuccess)
                {
                    eReturnCode = CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    // Store the results in the database
                    eReturnCode = PostProcessResults(resultFiles, datasetsByID);
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Could use the following to create a summary file:
                // Add the current job data to the summary file
                // UpdateSummaryFile();


                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();

                return success ? eReturnCode : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error in NOMAnnotationPlugin->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Annotate natural organic matter features using Python script smaqc_nom_mass_spec_metrics.py
        /// </summary>
        /// <param name="pythonScriptFile">Python script file</param>
        /// <param name="resultFiles">Output: dictionary where keys are dataset ID and values are the JSON result files for the dataset (one per scan)</param>
        /// <param name="datasetsByID">Output: dictionary where keys are dataset ID and values are dataset name</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ComputeNOMMetricsForDatasets(
            FileInfo pythonScriptFile,
            out Dictionary<int, List<FileInfo>> resultFiles,
            out Dictionary<int, string> datasetsByID)
        {
            resultFiles = new Dictionary<int, List<FileInfo>>();

            try
            {
                mConsoleOutputErrorMsg = string.Empty;

                LogMessage("Annotating natural organic matter features");

                var referenceMassFileName = mJobParams.GetJobParameter(
                    AnalysisJob.STEP_PARAMETERS_SECTION,
                    AnalysisResourcesNOMAnnotation.JOB_PARAM_REFERENCE_MASS_FILE,
                    string.Empty);

                string referenceMassFilePath;

                if (string.IsNullOrWhiteSpace(referenceMassFileName))
                {
                    referenceMassFilePath = string.Empty;
                }
                else
                {
                    referenceMassFilePath = Path.Combine(mWorkDir, referenceMassFileName);

                    if (!File.Exists(referenceMassFilePath))
                    {
                        LogError("Reference mass file not found: " + referenceMassFileName, "Reference mass file not found: " + referenceMassFilePath);
                        datasetsByID = new Dictionary<int, string>();
                        return false;
                    }
                }

                var pythonExePath = GetPythonProgLoc(mMgrParams, out var errorMessage);

                if (string.IsNullOrWhiteSpace(pythonExePath))
                {
                    LogError(errorMessage);
                    datasetsByID = new Dictionary<int, string>();
                    return false;
                }

                var pythonExe = new FileInfo(pythonExePath);

                // If this job applies to a single dataset, dataPackageID will be 0
                // We still need to create an instance of DataPackageInfo to retrieve the experiment name associated with the job's dataset
                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", 0);

                // The constructor for DataPackageInfo reads data package metadata from packed job parameters, which were created by the resource class
                var dataPackageInfo = new DataPackageInfo(dataPackageID, this);
                RegisterEvents(dataPackageInfo);

                if (dataPackageInfo.DatasetFiles.Count == 0)
                {
                    LogError("No datasets were found (dataPackageInfo.DatasetFiles is empty)");
                    datasetsByID = new Dictionary<int, string>();
                    return false;
                }

                mProgress = PROGRESS_PCT_ANNOTATING_NOM_FEATURES;

                var datasetsProcessed = 0;
                var success = true;

                datasetsByID = dataPackageInfo.Datasets;

                foreach (var dataset in datasetsByID)
                {
                    var datasetID = dataset.Key;
                    var datasetName = dataset.Value;

                    var datasetResultFiles = new List<FileInfo>();
                    resultFiles.Add(datasetID, datasetResultFiles);

                    var datasetFileOrDirectory = dataPackageInfo.DatasetFiles[datasetID];
                    var datasetFileType = dataPackageInfo.DatasetFileTypes[datasetID];
                    var datasetRawDataTypeName = dataPackageInfo.DatasetRawDataTypeNames[datasetID];

                    // Find the analysis.baf or analysis.tdf file for the current dataset

                    switch (datasetRawDataTypeName)
                    {
                        case AnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                        case AnalysisResources.RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER:
                        case AnalysisResources.RAW_DATA_TYPE_BRUKER_TOF_TDF_FOLDER:

                            FileInfo analysisBafFile;
                            FileInfo analysisTdfFile;

                            if (datasetFileType.Equals("File"))
                            {
                                analysisBafFile = new FileInfo(Path.Combine(mWorkDir, datasetFileOrDirectory));
                                analysisTdfFile = new FileInfo(Path.Combine(mWorkDir, datasetFileOrDirectory));
                            }
                            else
                            {
                                analysisBafFile = new FileInfo(Path.Combine(mWorkDir, datasetFileOrDirectory, "analysis.baf"));
                                analysisTdfFile = new FileInfo(Path.Combine(mWorkDir, datasetFileOrDirectory, "analysis.tdf"));
                            }

                            bool successCurrentDataset;

                            if (analysisBafFile.Exists)
                            {
                                successCurrentDataset = ComputeNOMMetricsForOneDataset(pythonExe, pythonScriptFile, referenceMassFilePath, analysisBafFile, datasetResultFiles);
                            }
                            else if (analysisTdfFile.Exists)
                            {
                                successCurrentDataset = ComputeNOMMetricsForOneDataset(pythonExe, pythonScriptFile, referenceMassFilePath, analysisTdfFile, datasetResultFiles);
                            }
                            else
                            {
                                LogError("Could not find the analysis.baf or analysis.tdf file for dataset " + datasetName);
                                successCurrentDataset = false;
                            }

                            if (!successCurrentDataset)
                                success = false;

                            break;

                        default:
                            LogError("Cannot annotate NOM features since unsupported raw data type: " + datasetRawDataTypeName);
                            success = false;
                            break;
                    }

                    datasetsProcessed++;
                    mProgress = ComputeIncrementalProgress(PROGRESS_PCT_ANNOTATING_NOM_FEATURES, PROGRESS_PCT_FINISHED_ANNOTATION, datasetsProcessed, datasetsByID.Count);
                }

                if (!success)
                    return false;

                mProgress = PROGRESS_PCT_FINISHED_ANNOTATION;
                mStatusTools.UpdateAndWrite(mProgress);

                if (mDebugLevel >= 3)
                {
                    LogDebug("NOM annotation processing complete");
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error finding dataset files in which to annotate natural organic matter features", ex);
                datasetsByID = new Dictionary<int, string>();
                return false;
            }
        }

        /// <summary>
        /// Call Python script smaqc_nom_mass_spec_metrics.py to annotate natural organic matter features in the given dataset
        /// </summary>
        /// <param name="pythonExe">Python .exe</param>
        /// <param name="pythonScriptFile">Python script fil</param>
        /// <param name="referenceMassFilePath">Reference mass file path</param>
        /// <param name="datasetFile">Dataset file (analysis.baf or analysis.tdf)</param>
        /// <param name="datasetResultFiles">List of JSON result files for the current dataset (one per scan)</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ComputeNOMMetricsForOneDataset(
            FileSystemInfo pythonExe,
            FileInfo pythonScriptFile,
            string referenceMassFilePath,
            FileInfo datasetFile,
            ICollection<FileInfo> datasetResultFiles)
        {
            try
            {
                if (datasetFile.Length > 1024 * 1024 * 1024)
                {
                    LogWarning("{0} file is over 1 GB; ProteoWizard typically cannot handle .baf files this large", datasetFile.FullName);
                }

                // Open the analysis.baf (or analysis.tdf or extension.baf) file using the ProteoWizardWrapper
                LogDebug("Using ProteoWizard to read spectra from {0}", datasetFile.FullName);

                var msDataFileReader = new MSDataFileReader(datasetFile.FullName, requireVendorCentroidedMS1: true, requireVendorCentroidedMS2: true);

                bool success;

                // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                if (msDataFileReader.SpectrumCount == 0)
                {
                    success = false;
                }
                else
                {
                    success = ComputeNOMMetricsForOneDataset(pythonExe, pythonScriptFile, referenceMassFilePath, msDataFileReader, datasetResultFiles);
                }

                msDataFileReader.Dispose();
                AppUtils.GarbageCollectNow();

                return success;
            }
            catch (Exception ex)
            {
                LogError(string.Format("Error opening dataset file {0} using ProteoWizard", datasetFile.FullName), ex);
                return false;
            }
        }

        private bool ComputeNOMMetricsForOneDataset(
            FileSystemInfo pythonExe,
            FileInfo pythonScriptFile,
            string referenceMassFilePath,
            MSDataFileReader msDataFileReader,
            ICollection<FileInfo> datasetResultFiles)
        {
            Console.WriteLine();
            LogDebug("Obtaining scan times and MSLevels (this could take several minutes)");
            var success = true;

            mCancellationToken = new CancellationTokenSource();
            var minScanIndexWithoutScanTimes = int.MaxValue;

            var scanTimes = Array.Empty<double>();
            var msLevels = Array.Empty<byte>();

            mGetScanTimesStartTime = DateTime.UtcNow;
            mGetScanTimesMaxWaitTimeSeconds = 900;
            mGetScanTimesAutoAborted = false;

            var attemptNumber = 1;

            while (true)
            {
                var useAlternateMethod = attemptNumber > 1;

                try
                {
                    msDataFileReader.GetScanTimesAndMsLevels(
                        mCancellationToken.Token, out scanTimes, out msLevels, MonitorScanTimeLoadingProgress, useAlternateMethod);

                    break;
                }
                catch (OperationCanceledException)
                {
                    // mCancellationToken.Cancel was called in MonitorScanTimeLoadingProgress

                    // Determine the scan index where GetScanTimesAndMsLevels exited the for loop
                    for (var spectrumIndex = 0; spectrumIndex < scanTimes.Length; spectrumIndex++)
                    {
                        if (msLevels[spectrumIndex] > 0) continue;

                        minScanIndexWithoutScanTimes = spectrumIndex;
                        break;
                    }

                    if (!mGetScanTimesAutoAborted && minScanIndexWithoutScanTimes < int.MaxValue)
                    {
                        // Manually aborted; shrink the arrays to reflect the amount of data that was actually loaded
                        Array.Resize(ref scanTimes, minScanIndexWithoutScanTimes);
                        Array.Resize(ref msLevels, minScanIndexWithoutScanTimes);
                    }

                    break;
                }
                catch (Exception ex)
                {
                    const string baseMessage = "Exception calling msDataFileReader.GetScanTimesAndMsLevels";
                    var alternateMethodFlag = useAlternateMethod ? " (useAlternateMethod = true)" : string.Empty;

                    LogWarning("{0}{1}: {2}", baseMessage, alternateMethodFlag, ex.Message);
                    attemptNumber++;

                    if (attemptNumber > 2)
                        throw new Exception(baseMessage, ex);
                }
            }

            var spectrumCount = scanTimes.Length;

            Console.WriteLine();
            LogDebug("Reading spectra");

            var lastStatusProgressTime = DateTime.UtcNow;

            for (var spectrumIndex = 0; spectrumIndex < spectrumCount; spectrumIndex++)
            {
                var scanNumber = spectrumIndex + 1;

                try
                {
                    // Obtain the raw mass spectrum
                    var msDataSpectrum = msDataFileReader.GetSpectrum(spectrumIndex);

                    if (spectrumIndex >= minScanIndexWithoutScanTimes && msDataSpectrum.Level is >= byte.MinValue and <= byte.MaxValue)
                    {
                        msLevels[spectrumIndex] = (byte)msDataSpectrum.Level;
                    }

                    if (int.TryParse(msDataSpectrum.Id, out var actualScanNumber))
                    {
                        scanNumber = actualScanNumber;
                    }

                    // Create file massSpectrumFile using msDataSpectrum.Mzs and msDataSpectrum.Intensities (which should both have msDataSpectrum.Mzs.Length items)
                    var massSpectrumFile = new FileInfo(string.Format("Scan_{0}.txt", scanNumber));
                    mJobParams.AddResultFileToSkip(massSpectrumFile.Name);

                    LogDebug("Writing spectral data file file " + massSpectrumFile.FullName);

                    using (var writer = new StreamWriter(new FileStream(massSpectrumFile.FullName, FileMode.Create, FileAccess.ReadWrite, FileShare.Read)))
                    {
                        for (var i = 0; i < msDataSpectrum.Mzs.Length; i++)
                        {
                            writer.WriteLine("{0}\t{1}", msDataSpectrum.Mzs[i], msDataSpectrum.Intensities[i]);
                        }
                    }

                    var successCurrent = ComputeNOMMetricsForSingleScan(pythonExe, pythonScriptFile, massSpectrumFile, referenceMassFilePath, datasetResultFiles);

                    if (!successCurrent)
                        success = false;

                }
                catch (Exception ex)
                {
                    LogError(string.Format("Error calling ComputeNOMMetricsForSingleScan for spectrum index {0} (scan {1})", spectrumIndex, scanNumber), ex);
                    success = false;
                }

                if (!(DateTime.UtcNow.Subtract(lastStatusProgressTime).TotalMinutes >= 5))
                    continue;

                LogDebug(
                    "Reading spectra, loaded {0:N0} / {1:N0} spectra",
                    scanNumber, spectrumCount);

                lastStatusProgressTime = DateTime.UtcNow;
            }

            return success;
        }

        private bool ComputeNOMMetricsForSingleScan(
            FileSystemInfo pythonExe,
            FileInfo pythonScriptFile,
            FileInfo massSpectrumFile,
            string referenceMassFilePath,
            ICollection<FileInfo> resultFiles
            )
        {
            // Set up and execute a program runner to compute the NOM metrics

            if (pythonScriptFile.DirectoryName is null)
            {
                LogError("Unable to determine the parent directory of the Python script file: " + pythonScriptFile.FullName);
                return false;
            }

            if (massSpectrumFile.DirectoryName is null)
            {
                LogError("Unable to determine the parent directory of the mass spectrum file: " + massSpectrumFile.FullName);
                return false;
            }

            var formulaTableJsonFile = new FileInfo(Path.Combine(pythonScriptFile.DirectoryName, "master_formula_table.json"));

            if (!formulaTableJsonFile.Exists)
            {
                LogError("Unable to determine the parent directory of the mass spectrum file: " + massSpectrumFile.FullName);
                return false;
            }

            var resultFileName = Path.GetFileNameWithoutExtension(massSpectrumFile.Name) + "_results.json";

            var resultFile = new FileInfo(Path.Combine(massSpectrumFile.DirectoryName, resultFileName));
            resultFiles.Add(resultFile);

            var arguments = new StringBuilder();

            arguments.AppendFormat(" --input {0}", massSpectrumFile.FullName);
            arguments.AppendFormat(" --output {0}", resultFile.FullName);

            if (!string.IsNullOrWhiteSpace(referenceMassFilePath))
            {
                // ReSharper disable once StringLiteralTypo
                arguments.AppendFormat(" --ref-masslist {0}", referenceMassFilePath);
            }

            arguments.AppendFormat(" --formula-table {0}", formulaTableJsonFile.FullName);
            // arguments.Append(" --ppm-tolerance 1.0");

            // Create a batch file to run the command
            // Capture the console output (including output to the error stream) via redirection symbols:
            //    exePath arguments > ConsoleOutputFile.txt 2>&1

            var batchFilePath = Path.Combine(mWorkDir, "Run_NOM_Annotation.bat");
            mJobParams.AddResultFileToSkip(Path.GetFileName(batchFilePath));

            var batchFileCmdLine = pythonExe.FullName + " " + pythonScriptFile.FullName + " " + arguments + " > " + NOM_ANNOTATION_CONSOLE_OUTPUT_FILE + " 2>&1";

            LogDebug("Creating batch file at " + batchFilePath);

            // Create the batch file
            using (var writer = new StreamWriter(new FileStream(batchFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                if (mDebugLevel >= 1)
                {
                    LogDebug(batchFileCmdLine);
                }

                writer.WriteLine(batchFileCmdLine);
            }

            mConsoleOutputFile = Path.Combine(mWorkDir, NOM_ANNOTATION_CONSOLE_OUTPUT_FILE);

            var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = false,
                CacheStandardOutput = false,
                EchoOutputToConsole = false,
                WriteConsoleOutputToFile = false
            };

            RegisterEvents(cmdRunner);

            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            var success = cmdRunner.RunProgram(batchFilePath, string.Empty, "NOM Annotation", true);

            if (!cmdRunner.WriteConsoleOutputToFile && cmdRunner.CachedConsoleOutput.Length > 0)
            {
                // Write the console output to a text file
                Global.IdleLoop(0.25);

                using var writer = new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine(cmdRunner.CachedConsoleOutput);
            }

            // Parse the console output file to look for errors
            ParseConsoleOutputFile(mConsoleOutputFile);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (success)
            {
                return true;
            }

            if (cmdRunner.ExitCode != 0)
            {
                LogWarning("The NOM annotation script returned a non-zero exit code: " + cmdRunner.ExitCode);
            }
            else
            {
                LogWarning("NOM annotation failed (but exit code is 0)");
            }

            return false;
        }

        private void MonitorScanTimeLoadingProgress(int scansLoaded, int totalScans)
        {
            if (DateTime.UtcNow.Subtract(mLastScanLoadingDebugProgressTime).TotalSeconds < 30)
                return;

            // If the call to msDataFileReader.GetScanTimesAndMsLevels() takes too long,
            // abort the process and instead get the ScanTimes and MSLevels via msDataFileReader.GetSpectrum()
            if (mGetScanTimesMaxWaitTimeSeconds > 0 &&
                DateTime.UtcNow.Subtract(mGetScanTimesStartTime).TotalSeconds >= mGetScanTimesMaxWaitTimeSeconds)
            {
                mGetScanTimesAutoAborted = true;
                mCancellationToken.Cancel();
            }

            mLastScanLoadingDebugProgressTime = DateTime.UtcNow;

            if (totalScans == 0)
                return;

            if (!mReportedTotalSpectraToExamine)
            {
                LogDebug(" ... {0:N0} total spectra to examine", totalScans);
                mReportedTotalSpectraToExamine = true;
            }

            if (DateTime.UtcNow.Subtract(mLastScanLoadingStatusProgressTime).TotalMinutes > 5)
            {
                LogDebug("Obtaining scan times and MSLevels, examined {0:N0} / {1:N0} spectra", scansLoaded, totalScans);
                mLastScanLoadingStatusProgressTime = DateTime.UtcNow;
            }
        }

        /// <summary>
        /// Parse the NOM Annotation console output file to track the search progress
        /// </summary>
        /// <remarks>Not used at present</remarks>
        /// <param name="consoleOutputFilePath">Console output file path</param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example Console output

            // ...

            try
            {
                var reErrorMessage = new Regex("Error:(?<ErrorMessage>.+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    // Look for generic errors
                    var reMatch = reErrorMessage.Match(dataLine);

                    if (reMatch.Success)
                    {
                        // Store this error message, plus any remaining console output lines
                        mMessage = reMatch.Groups["ErrorMessage"].Value;
                        StoreConsoleErrorMessage(reader, dataLine);
                    }
                    else if (dataLine.StartsWith("error ", StringComparison.OrdinalIgnoreCase))
                    {
                        // Store this error message, plus any remaining console output lines
                        mMessage = dataLine;
                        StoreConsoleErrorMessage(reader, dataLine);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogErrorNoMessageUpdate("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Read NOM metrics from a JSON file, convert to XML, and store in the database
        /// </summary>
        /// <param name="datasetID">Dataset ID</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="nomStatsJSONFile">NOM stats .json file</param>
        /// <returns>True if successful, false if an error</returns>
        private bool PostDatasetNOMStatsXml(int datasetID, string datasetName, FileSystemInfo nomStatsJSONFile)
        {
            try
            {
                // Convert JSON to XML
                var jsonText = File.ReadAllText(nomStatsJSONFile.FullName);
                var jsonInfo = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonText);

                var xmlSettings = new XmlWriterSettings
                {
                    CheckCharacters = true,
                    Indent = true,
                    IndentChars = "  ",
                    Encoding = Encoding.UTF8,
                    CloseOutput = false        // Do not close output automatically so that the MemoryStream can be read after the XmlWriter has been closed
                };

                // Cache the XML using a MemoryStream.  Here, the stream encoding is set by the XmlWriter
                // and so you see the attribute encoding="UTF-8" in the opening XML declaration encoding
                // (since we used xmlSettings.Encoding = Encoding.UTF8)
                //
                var memStream = new MemoryStream();
                var writer = XmlWriter.Create(memStream, xmlSettings);

                writer.WriteStartDocument(true);

                // Write the beginning of the "Root" element.
                writer.WriteStartElement("NOMStats");

                writer.WriteStartElement("Dataset");

                if (datasetID > 0)
                {
                    writer.WriteAttributeString("DatasetID", datasetID.ToString());
                }
                writer.WriteString(datasetName);
                writer.WriteEndElement();       // Dataset EndElement


                writer.WriteStartElement("metrics");

                foreach (var entry in jsonInfo)
                {
                    if (!entry.Key.Equals("metrics"))
                    {
                        continue;
                    }

                    // ToDo: debug this
                    foreach (var metric in (Dictionary<string, object>)entry.Value)
                    {
                        writer.WriteElementString(metric.Key, metric.Value.ToString());
                    }
                }

                writer.WriteEndElement();       // metrics

                writer.WriteEndElement();  // End the "Root" element (NOMStats)

                writer.WriteEndDocument(); // End the document

                writer.Close();

                // Now Rewind the memory stream and output as a string
                memStream.Position = 0;
                var reader = new StreamReader(memStream);

                // Return the XML as text
                var nomStatsXML = reader.ReadToEnd();

                return PostDatasetNOMStatsXml(datasetID, nomStatsXML);
            }
            catch (Exception ex)
            {
                LogError("Error converting NOM stats from JSON to XML", ex);
                return false;
            }
        }

        private bool PostDatasetNOMStatsXml(int datasetID, string nomStatsXML)
        {
            var postCount = 0;

            // This connection string points to the DMS database on prismdb2
            var connectionString = mMgrParams.GetParam("ConnectionString");

            var applicationName = string.Format("{0}_DatasetInfo", mMgrParams.ManagerName);

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, applicationName);

            var successPosting = false;

            while (postCount <= 2)
            {
                successPosting = PostDatasetNOMStatsXml(datasetID, nomStatsXML, connectionStringToUse, UPDATE_NOM_STATS_PROCEDURE);

                if (successPosting)
                {
                    break;
                }

                // If the error message contains the text "timeout expired", try again, up to 2 times
                if (mMessage.IndexOf("timeout expired", StringComparison.OrdinalIgnoreCase) < 0)
                {
                    break;
                }

                Thread.Sleep(1500);
                postCount++;
            }

            if (successPosting)
            {
                return true;
            }

            // Either a non-zero error code was returned, or an error event was received
            LogError("Error posting natural organic matter stats XML for dataset ID {0}", datasetID);
            return false;
        }

        /// <summary>
        /// Post the dataset info in nomStatsXML to the database, using the specified connection string and procedure
        /// This version assumes the procedure takes DatasetID as the first parameter
        /// </summary>
        /// <param name="datasetID">Dataset ID to send to the procedure</param>
        /// <param name="nomStatsXML">Natural organic matter stats XML</param>
        /// <param name="connectionString">Database connection string</param>
        /// <param name="procedureName">Procedure</param>
        /// <returns>True if success; false if failure</returns>
        private bool PostDatasetNOMStatsXml(int datasetID, string nomStatsXML, string connectionString, string procedureName)
        {
            try
            {
                LogMessage("  Posting NOM Stats XML to the database (using Dataset ID " + datasetID + ")");

                // We need to remove the encoding line from datasetInfoXML before posting to the DB
                // This line will look like this:
                //   <?xml version="1.0" encoding="utf-16" standalone="yes"?>

                var startIndex = nomStatsXML.IndexOf("?>", StringComparison.Ordinal);

                string nomStatsXMLClean;

                if (startIndex > 0)
                {
                    nomStatsXMLClean = nomStatsXML.Substring(startIndex + 2).Trim();
                }
                else
                {
                    nomStatsXMLClean = nomStatsXML;
                }

                // Call procedure procedureName using connection string connectionString

                if (string.IsNullOrEmpty(connectionString))
                {
                    LogError("Connection string not defined; unable to post the NOM stats to the database");
                    return false;
                }

                if (string.IsNullOrEmpty(procedureName))
                {
                    procedureName = "cache_dataset_nom_stats_xml";
                }

                var applicationName = "MSFileInfoScanner_DatasetID_" + datasetID;

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, applicationName);

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse);
                RegisterEvents(dbTools);

                var cmd = dbTools.CreateCommand(procedureName, CommandType.StoredProcedure);

                var returnParam = dbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);
                dbTools.AddParameter(cmd, "@datasetID", SqlType.Int).Value = datasetID;
                dbTools.AddParameter(cmd, "@nomStatsXML", SqlType.XML).Value = nomStatsXMLClean;

                var result = dbTools.ExecuteSP(cmd);

                if (result == DbUtilsConstants.RET_VAL_OK)
                {
                    // No errors
                    return true;
                }

                LogError("Error calling procedure to store NOM Stats XML, return code = " + returnParam.Value.CastDBVal<string>());
                return false;
            }
            catch (Exception ex)
            {
                LogError("Error calling procedure to store NOM Stats XML", ex);
                return false;
            }
        }

        /// <summary>
        /// Store the NOM Stats results in the database for each dataset in resultFiles (there typically should only be one since this job should not be data-package based)
        /// </summary>
        /// <param name="resultFiles">Dictionary where keys are dataset ID and values are the JSON result files for the dataset (one per scan)</param>
        /// <param name="datasetsByID">Output: dictionary where keys are dataset ID and values are dataset name</param>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType PostProcessResults(Dictionary<int, List<FileInfo>> resultFiles, IReadOnlyDictionary<int, string> datasetsByID)
        {
            try
            {
                if (resultFiles.Count == 0)
                {
                    LogError("No datasets were found; cannot send NOM Stats to the database");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = true;
                mProgress = PROGRESS_PCT_STORING_RESULTS_IN_DB;

                foreach (var entry in resultFiles)
                {
                    var datasetID = entry.Key;
                    var datasetName = datasetsByID[datasetID];

                    // ToDo: verify that calling LogError here updates mMessage (it should, due to how function overloading works)
                    if (entry.Value.Count == 0)
                    {
                        LogError("No result files were found for dataset ID {0}; cannot send NOM Stats to the database", datasetID);
                        success = false;
                        continue;
                    }

                    bool successCurrent;

                    // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                    if (entry.Value.Count > 1)
                    {
                        // The dataset had multiple scans; only store the results for the first scan
                        // Future ToDo: compute median values of the metrics
                        successCurrent = PostDatasetNOMStatsXml(datasetID, datasetName, entry.Value[0]);
                    }
                    else
                    {
                        successCurrent = PostDatasetNOMStatsXml(datasetID, datasetName, entry.Value[0]);
                    }

                    if (!successCurrent)
                        success = false;
                }

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error post-processing results", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private void StoreConsoleErrorMessage(StreamReader reader, string dataLine)
        {
            mConsoleOutputErrorMsg = "Error running Formularity: " + dataLine;

            while (!reader.EndOfStream)
            {
                // Store the remaining console output lines
                dataLine = reader.ReadLine();

                if (!string.IsNullOrWhiteSpace(dataLine))
                {
                    mConsoleOutputErrorMsg += "; " + dataLine;
                }
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string pythonScriptPath)
        {
            var toolVersionInfo = string.Empty;

            return mToolVersionUtilities.StoreToolVersionInfoPythonScript(ref toolVersionInfo, pythonScriptPath);
        }

        private void CmdRunner_LoopWaiting()
        {
            // Synchronize the stored Debug level with the value stored in the database

            {
                UpdateStatusFile();

                // Parse the console output file every 15 seconds
                if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
                {
                    mLastConsoleOutputParse = DateTime.UtcNow;

                    ParseConsoleOutputFile(Path.Combine(mWorkDir, mConsoleOutputFile));

                    LogProgress("Formularity");
                }
            }
        }
    }
}
