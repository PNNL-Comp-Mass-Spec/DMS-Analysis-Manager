//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 05/29/2014
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISM;
using PRISM.Logging;
using PRISMDatabaseUtils;
using ThermoRawFileReader;

namespace AnalysisManagerGlyQIQPlugin
{
    /// <summary>
    /// Class for running the GlyQ-IQ
    /// </summary>
    public class AnalysisToolRunnerGlyQIQ : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: glycan, glycans, isotpe, Lc, Pre, Procssing

        // ReSharper restore CommentTypo

        private const int PROGRESS_PCT_STARTING = 1;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private const string STORE_JOB_PSM_RESULTS_SP_NAME = "StoreJobPSMStats";

        private struct PSMStats
        {
            public int TotalPSMs;
            public int UniquePeptideCount;
            public int UniqueProteinCount;

            public void Clear()
            {
                TotalPSMs = 0;
                UniquePeptideCount = 0;
                UniqueProteinCount = 0;
            }
        }

        private int mCoreCount;

        private int mSpectraSearched;

        /// <summary>
        /// Dictionary of GlyQIqRunner instances
        /// </summary>
        /// <remarks>Key is core number (1 through NumCores), value is the instance</remarks>
        private Dictionary<int, GlyQIqRunner> mGlyQRunners;

        private XRawFileIO mThermoFileReader;

        private IDBTools mStoredProcedureExecutor;

        /// <summary>
        /// Runs GlyQ-IQ
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
                    LogDebug("AnalysisToolRunnerGlyQIQ.RunTool(): Enter");
                }

                // Determine the path to the IQGlyQ program
                var progLoc = DetermineProgramLocation("GlyQIQProgLoc", "IQGlyQ_Console.exe");

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the GlyQ-IQ version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");

                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Error determining GlyQ-IQ version";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run GlyQ-IQ
                var success = RunGlyQIQ();

                if (success)
                {
                    success = CombineResultFiles();
                }

                // Zip up the settings files and batch files so we have a record of them
                PackageResults();

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                if (!success)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var copySuccess = CopyResultsToTransferDirectory();

                if (!copySuccess)
                    return CloseOutType.CLOSEOUT_FAILED;

                // It is now safe to delete the _peaks.txt file that is in the transfer folder
                if (mDebugLevel >= 1)
                {
                    LogDebug("Deleting the _peaks.txt file from the Results Transfer folder");
                }

                RemoveNonResultServerFiles();

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Error in GlyQIQ->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool CombineResultFiles()
        {
            var reFutureTarget = new Regex(@"\tFutureTarget\t", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            try
            {
                // Combine the results files
                var resultsFolder = new DirectoryInfo(Path.Combine(mWorkDir, "Results_" + mDatasetName));

                if (!resultsFolder.Exists)
                {
                    mMessage = "Results folder not found: " + resultsFolder.FullName;
                    LogError(mMessage);
                    return false;
                }

                var unfilteredResults = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_iqResults_Unfiltered.txt"));
                var filteredResults = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_iqResults.txt"));

                using (var writerUnfiltered = new StreamWriter(new FileStream(unfilteredResults.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                using (var writerFiltered = new StreamWriter(new FileStream(filteredResults.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    for (var core = 1; core <= mCoreCount; core++)
                    {
                        var resultFile = new FileInfo(Path.Combine(resultsFolder.FullName, mDatasetName + "_iqResults_" + core + ".txt"));

                        if (!resultFile.Exists)
                        {
                            if (string.IsNullOrEmpty(mMessage))
                            {
                                mMessage = "Result file not found: " + resultFile.Name;
                            }
                            LogError("Result file not found: " + resultFile.FullName);
                            continue;
                        }

                        using var reader = new StreamReader(new FileStream(resultFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                        var linesRead = 0;

                        while (!reader.EndOfStream)
                        {
                            var dataLine = reader.ReadLine();
                            linesRead++;

                            if (linesRead == 1 && core > 1)
                            {
                                // This is the header line from a core 2 or later file
                                // Skip it
                                continue;
                            }

                            writerUnfiltered.WriteLine(dataLine);

                            // Write lines that do not contain "FutureTarget" to the _iqResults.txt file
                            if (string.IsNullOrEmpty(dataLine) || !reFutureTarget.IsMatch(dataLine))
                            {
                                writerFiltered.WriteLine(dataLine);
                            }
                        }
                    }
                }

                // Zip the unfiltered results
                ZipFile(unfilteredResults.FullName, true);

                // Parse the filtered results to count the number of identified glycans
                var success = ExamineFilteredResults(filteredResults);

                return success;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in CombineResultFiles: " + ex.Message;
                LogError(mMessage);
                return false;
            }
        }

        private int CountMsMsSpectra(string rawFilePath)
        {
            try
            {
                if (mDebugLevel >= 1)
                {
                    LogDebug("Counting the number of MS/MS spectra in " + Path.GetFileName(rawFilePath));
                }

                mThermoFileReader = new XRawFileIO();
                RegisterEvents(mThermoFileReader);

                if (!mThermoFileReader.OpenRawFile(rawFilePath))
                {
                    mMessage = "Error opening the Thermo Raw file to count the MS/MS spectra";
                    return 0;
                }

                var scanCount = mThermoFileReader.GetNumScans();

                var ms1ScanCount = 0;
                var ms2ScanCount = 0;

                for (var scan = 1; scan <= scanCount; scan++)
                {
                    if (mThermoFileReader.GetScanInfo(scan, out var scanInfo))
                    {
                        if (scanInfo.MSLevel > 1)
                        {
                            ms2ScanCount++;
                        }
                        else
                        {
                            ms1ScanCount++;
                        }
                    }
                }

                mThermoFileReader.CloseRawFile();

                if (mDebugLevel >= 1)
                {
                    LogDebug(" ... MS1 spectra: " + ms1ScanCount);
                    LogDebug(" ... MS2 spectra: " + ms2ScanCount);
                }

                if (ms2ScanCount > 0)
                {
                    return ms2ScanCount;
                }

                return ms1ScanCount;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in CountMsMsSpectra: " + ex.Message;
                LogError(mMessage);
                return 0;
            }
        }

        private bool ExamineFilteredResults(FileInfo resultsFile)
        {
            return ExamineFilteredResults(resultsFile, mJob, string.Empty);
        }

        /// <summary>
        /// Examine the GlyQ-IQ results in the given file to count the number of PSMs and unique number of glycans
        /// Post the results to DMS using jobNumber
        /// </summary>
        /// <remarks>If dmsConnectionStringOverride is empty then PostJobResults will use the Manager Parameters (mMgrParams)</remarks>
        /// <param name="resultsFile"></param>
        /// <param name="jobNumber"></param>
        /// <param name="dmsConnectionStringOverride">Optional: DMS5 connection string</param>
        public bool ExamineFilteredResults(FileInfo resultsFile, int jobNumber, string dmsConnectionStringOverride)
        {
            try
            {
                var headerSkipped = false;

                var totalPSMs = 0;
                var uniqueCodeFormulaCombos = new SortedSet<string>();
                var uniqueCodes = new SortedSet<string>();

                using (var reader = new StreamReader(new FileStream(resultsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var dataCols = dataLine.Split('\t');

                        if (dataCols.Length < 3)
                        {
                            continue;
                        }

                        var compoundCode = dataCols[1];
                        var empiricalFormula = dataCols[2];

                        if (!headerSkipped)
                        {
                            if (!string.Equals(compoundCode, "Code", StringComparison.OrdinalIgnoreCase))
                            {
                                mMessage = "2nd column in the glycan result file is not Code";
                                return false;
                            }

                            if (!string.Equals(empiricalFormula, "EmpiricalFormula", StringComparison.OrdinalIgnoreCase))
                            {
                                mMessage = "3rd column in the glycan result file is not EmpiricalFormula";
                                return false;
                            }

                            headerSkipped = true;
                            continue;
                        }

                        var codePlusFormula = compoundCode + "_" + empiricalFormula;

                        if (!uniqueCodeFormulaCombos.Contains(codePlusFormula))
                        {
                            uniqueCodeFormulaCombos.Add(codePlusFormula);
                        }

                        if (!uniqueCodes.Contains(compoundCode))
                        {
                            uniqueCodes.Add(compoundCode);
                        }

                        totalPSMs++;
                    }
                }

                var psmstats = new PSMStats();
                psmstats.Clear();
                psmstats.TotalPSMs = totalPSMs;
                psmstats.UniquePeptideCount = uniqueCodeFormulaCombos.Count;
                psmstats.UniqueProteinCount = uniqueCodes.Count;

                // Store the results in the database
                PostJobResults(jobNumber, psmstats, dmsConnectionStringOverride);

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in ExamineFilteredResults";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }
        }

        private void PackageResults()
        {
            var tempZipDirectory = new DirectoryInfo(Path.Combine(mWorkDir, "FilesToZip"));

            try
            {
                if (!tempZipDirectory.Exists)
                {
                    tempZipDirectory.Create();
                }

                // Move the batch files and console output files into the FilesToZip folder
                var workingDirectory = new DirectoryInfo(mWorkDir);
                var filesToMove = new List<FileInfo>();

                var batchFiles = workingDirectory.GetFiles("*.bat");
                filesToMove.AddRange(batchFiles);

                // We don't keep the entire ConsoleOutput file
                // Instead, just keep a trimmed version of the original, removing extraneous log messages
                foreach (var consoleOutputFile in workingDirectory.GetFiles(GlyQIqRunner.GLYQ_IQ_CONSOLE_OUTPUT_PREFIX + "*.txt"))
                {
                    PruneConsoleOutputFiles(consoleOutputFile, tempZipDirectory);
                }

                foreach (var file in filesToMove)
                {
                    file.MoveTo(Path.Combine(tempZipDirectory.FullName, file.Name));
                }

                // Move selected files from the first WorkingParameters folder

                // We just need to copy files from the first core's WorkingParameters folder
                var sourceWorkingParametersDirectory = new DirectoryInfo(Path.Combine(mWorkDir, "WorkingParametersCore1"));

                var targetWorkingParametersDirectory = new DirectoryInfo(Path.Combine(tempZipDirectory.FullName, "WorkingParameters"));

                if (!targetWorkingParametersDirectory.Exists)
                {
                    targetWorkingParametersDirectory.Create();
                }

                var iqParamFileName = mJobParams.GetJobParameter("ParamFileName", "");

                foreach (var file in sourceWorkingParametersDirectory.GetFiles())
                {
                    var moveFile = false;

                    if (string.Equals(file.Name, iqParamFileName, StringComparison.OrdinalIgnoreCase))
                    {
                        moveFile = true;
                    }
                    else if (file.Name.StartsWith(AnalysisResourcesGlyQIQ.GLYQIQ_PARAMS_FILE_PREFIX))
                    {
                        moveFile = true;
                    }
                    else if (file.Name.StartsWith(AnalysisResourcesGlyQIQ.ALIGNMENT_PARAMETERS_FILENAME))
                    {
                        moveFile = true;
                    }
                    else if (file.Name.StartsWith(AnalysisResourcesGlyQIQ.EXECUTOR_PARAMETERS_FILE))
                    {
                        moveFile = true;
                    }

                    if (moveFile)
                    {
                        file.MoveTo(Path.Combine(targetWorkingParametersDirectory.FullName, file.Name));
                    }
                }

                var zipFilePath = Path.Combine(mWorkDir, "GlyQIq_Automation_Files.zip");

                mZipTools.ZipDirectory(tempZipDirectory.FullName, zipFilePath);
            }
            catch (Exception ex)
            {
                mMessage = "Exception creating GlyQIq_Automation_Files.zip";
                LogError(mMessage + ": " + ex.Message);
                return;
            }

            try
            {
                // Clear the TempZipFolder
                Global.IdleLoop(0.25);
                tempZipDirectory.Delete(true);
                tempZipDirectory.Create();
            }
            catch (Exception)
            {
                // This error can be safely ignored
            }
        }

        private bool PostJobResults(int jobNumber, PSMStats psmstats, string dmsConnectionStringOverride)
        {
            const int MAX_RETRY_COUNT = 3;

            try
            {
                // Call stored procedure StoreJobPSMStats in DMS5

                if (mStoredProcedureExecutor == null || !string.IsNullOrWhiteSpace(dmsConnectionStringOverride))
                {
                    string connectionString;

                    if (string.IsNullOrWhiteSpace(dmsConnectionStringOverride))
                    {
                        if (mMgrParams == null)
                        {
                            throw new Exception("mMgrParams object has not been initialized");
                        }

                        // Gigasax.DMS5
                        connectionString = mMgrParams.GetParam("ConnectionString");
                    }
                    else
                    {
                        connectionString = dmsConnectionStringOverride;
                    }

                    var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrName);

                    mStoredProcedureExecutor = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
                    RegisterEvents(mStoredProcedureExecutor);

                    UnregisterEventHandler((EventNotifier)mStoredProcedureExecutor, BaseLogger.LogLevels.ERROR);
                    mStoredProcedureExecutor.ErrorEvent += ExecuteSP_DBErrorEvent;
                }

                var dbTools = mStoredProcedureExecutor;
                var cmd = dbTools.CreateCommand(STORE_JOB_PSM_RESULTS_SP_NAME, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);
                dbTools.AddTypedParameter(cmd, "@Job", SqlType.Int, value: jobNumber);
                dbTools.AddTypedParameter(cmd, "@MSGFThreshold", SqlType.Float, value: 1);
                dbTools.AddTypedParameter(cmd, "@FDRThreshold", SqlType.Float, value: 0.25);
                dbTools.AddTypedParameter(cmd, "@SpectraSearched", SqlType.Int, value: mSpectraSearched);
                dbTools.AddTypedParameter(cmd, "@TotalPSMs", SqlType.Int, value: psmstats.TotalPSMs);
                dbTools.AddTypedParameter(cmd, "@UniquePeptides", SqlType.Int, value: psmstats.UniquePeptideCount);
                dbTools.AddTypedParameter(cmd, "@UniqueProteins", SqlType.Int, value: psmstats.UniqueProteinCount);
                dbTools.AddTypedParameter(cmd, "@TotalPSMsFDRFilter", SqlType.Int, value: psmstats.TotalPSMs);
                dbTools.AddTypedParameter(cmd, "@UniquePeptidesFDRFilter", SqlType.Int, value: psmstats.UniquePeptideCount);
                dbTools.AddTypedParameter(cmd, "@UniqueProteinsFDRFilter", SqlType.Int, value: psmstats.UniqueProteinCount);
                dbTools.AddTypedParameter(cmd, "@MSGFThresholdIsEValue", SqlType.TinyInt, value: 0);

                // Execute the SP (retry the call up to 3 times)
                var resCode = mStoredProcedureExecutor.ExecuteSP(cmd, MAX_RETRY_COUNT);

                if (resCode == 0)
                {
                    return true;
                }

                const string msg = "Error storing PSM Results in database";
                LogError(msg, msg + ", " + STORE_JOB_PSM_RESULTS_SP_NAME + " returned " + resCode);

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception storing PSM Results in database: " + ex.Message);
                return false;
            }
        }

        private void PruneConsoleOutputFiles(FileInfo consoleOutputFile, DirectoryInfo targetFolder)
        {
            if (consoleOutputFile.Directory == null)
                return;

            if (consoleOutputFile.Directory.FullName == targetFolder.FullName)
            {
                throw new Exception("The Source console output file cannot reside in the Target Folder: " + consoleOutputFile.FullName + " vs. " + targetFolder.FullName);
            }

            try
            {
                var linesToPrune = new List<string>
                {
                    "LC Peaks To Analyze:",
                    "Best:",
                    "Next Lc peak",
                    "Next Peak Quality, we have",
                    // ReSharper disable StringLiteralTypo
                    "No isotpe profile was found using the IterativelyFindMSFeature",
                    "Peak Finished Procssing",
                    "PostProccessing info adding",
                    // ReSharper restore StringLiteralTypo
                    "Pre MS Processor... Press Key",
                    "the old scan Range is",
                    "The time is ",
                    "BreakOut",
                    "Loading",
                    "      Fit Seed",
                    "       LM Worked "
                };

                var reNumericLine = new Regex("^[0-9.]+$", RegexOptions.Compiled);

                var consoleOutputFilePruned = Path.Combine(targetFolder.FullName, consoleOutputFile.Name);

                using (var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(consoleOutputFilePruned, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine(dataLine);
                            continue;
                        }

                        if (dataLine.StartsWith("start post run"))
                        {
                            // Ignore everything after this point
                            break;
                        }

                        var skipLine = linesToPrune.Any(textToFind => dataLine.StartsWith(textToFind));

                        if (skipLine)
                            continue;

                        if (reNumericLine.IsMatch(dataLine))
                        {
                            // Skip this line
                            continue;
                        }

                        writer.WriteLine(dataLine);
                    }
                }

                // Make sure that we don't keep the original, non-pruned file
                // The pruned file was created in targetFolder and will get included in GlyQIq_Automation_Files.zip
                mJobParams.AddResultFileToSkip(consoleOutputFile.Name);
            }
            catch (Exception ex)
            {
                LogError("Exception in PruneConsoleOutputFiles: " + ex.Message);
            }
        }

        private bool RunGlyQIQ()
        {
            var currentTask = "Initializing";

            try
            {
                mCoreCount = mJobParams.GetJobParameter(AnalysisResourcesGlyQIQ.JOB_PARAM_ACTUAL_CORE_COUNT, 0);

                if (mCoreCount < 1)
                {
                    mMessage = "Core count reported by " + AnalysisResourcesGlyQIQ.JOB_PARAM_ACTUAL_CORE_COUNT + " is 0; unable to continue";
                    return false;
                }

                var rawDataTypeName = mJobParams.GetParam("RawDataType");
                var rawDataType = AnalysisResources.GetRawDataType(rawDataTypeName);

                if (rawDataType == AnalysisResources.RawDataTypeConstants.ThermoRawFile)
                {
                    mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_RAW_EXTENSION);
                }
                else
                {
                    mMessage = "GlyQ-IQ presently only supports Thermo .Raw files";
                    return false;
                }

                // Determine the number of MS/MS spectra in the .Raw file (required for PostJobResults)
                var rawFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_RAW_EXTENSION);
                mSpectraSearched = CountMsMsSpectra(rawFilePath);

                // Set up and execute a program runner to run each batch file that launches GlyQ-IQ

                mProgress = PROGRESS_PCT_STARTING;

                mGlyQRunners = new Dictionary<int, GlyQIqRunner>();
                // var threads = new List<Thread>();

                for (var core = 1; core <= mCoreCount; core++)
                {
                    var batchFilePath = Path.Combine(mWorkDir, AnalysisResourcesGlyQIQ.START_PROGRAM_BATCH_FILE_PREFIX + core + ".bat");

                    currentTask = "Launching GlyQ-IQ, core " + core;
                    LogDebug(currentTask + ": " + batchFilePath);

                    var glyQRunner = new GlyQIqRunner(mWorkDir, core, batchFilePath);
                    glyQRunner.CmdRunnerWaiting += CmdRunner_LoopWaiting;
                    mGlyQRunners.Add(core, glyQRunner);

                    var newThread = new System.Threading.Thread(glyQRunner.StartAnalysis) {
                        Priority = System.Threading.ThreadPriority.BelowNormal
                    };

                    newThread.Start();
                    // threads.Add(newThread);
                }

                // Wait for all of the threads to exit
                // Run for a maximum of 14 days

                currentTask = "Waiting for all of the threads to exit";

                var startTime = DateTime.UtcNow;
                var completedCores = new SortedSet<int>();

                while (true)
                {
                    // Poll the status of each of the threads

                    var stepsComplete = 0;
                    double progressSum = 0;

                    foreach (var glyQRunner in mGlyQRunners)
                    {
                        var eStatus = glyQRunner.Value.Status;

                        if (eStatus >= GlyQIqRunner.GlyQIqRunnerStatusCodes.Success)
                        {
                            // Analysis completed (or failed)
                            stepsComplete++;

                            if (!completedCores.Contains(glyQRunner.Key))
                            {
                                completedCores.Add(glyQRunner.Key);
                                LogDebug("GlyQ-IQ processing core " + glyQRunner.Key + " is now complete");
                            }
                        }

                        progressSum += glyQRunner.Value.Progress;
                    }

                    var subTaskProgress = (float)(progressSum / mGlyQRunners.Count);
                    var updatedProgress = ComputeIncrementalProgress(PROGRESS_PCT_STARTING, PROGRESS_PCT_COMPLETE, subTaskProgress);

                    if (updatedProgress > mProgress)
                    {
                        // This progress will get written to the status file and sent to the messaging queue by UpdateStatusFile()
                        mProgress = updatedProgress;
                    }

                    if (stepsComplete >= mGlyQRunners.Count)
                    {
                        // All threads are done
                        break;
                    }

                    Global.IdleLoop(2);

                    if (DateTime.UtcNow.Subtract(startTime).TotalDays > 14)
                    {
                        mMessage = "GlyQ-IQ ran for over 14 days; aborting";

                        foreach (var glyQRunner in mGlyQRunners)
                        {
                            glyQRunner.Value.AbortProcessingNow();
                        }

                        return false;
                    }
                }

                var success = true;
                var exitCode = 0;

                currentTask = "Looking for console output error messages";

                // Look for any console output error messages
                // Note that ProgRunner will have already included them in the ConsoleOutput.txt file

                foreach (var glyQRunner in mGlyQRunners)
                {
                    var progRunner = glyQRunner.Value.ProgramRunner;

                    if (progRunner == null)
                        continue;

                    foreach (var cachedError in progRunner.CachedConsoleErrors)
                    {
                        LogError("Core " + glyQRunner.Key + ": " + cachedError);
                        success = false;
                    }

                    if (progRunner.ExitCode != 0 && exitCode == 0)
                    {
                        exitCode = progRunner.ExitCode;
                    }
                }

                if (!success)
                {
                    LogError("Error running GlyQ-IQ");

                    if (exitCode != 0)
                    {
                        LogWarning("GlyQ-IQ returned a non-zero exit code: " + exitCode);
                    }
                    else
                    {
                        LogWarning("Call to GlyQ-IQ failed (but exit code is 0)");
                    }

                    return false;
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                mStatusTools.UpdateAndWrite(mProgress);

                if (mDebugLevel >= 3)
                {
                    LogDebug("GlyQ-IQ Analysis Complete");
                }

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Error in RunGlyQIQ while " + currentTask;
                LogError(mMessage + ": " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDLLs = new List<string>
            {
                "IQGlyQ.dll",
                "IQ2_x64.dll",
                "Run64.dll"
            };

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDLLs, true);

            return success;
        }

        private void ExecuteSP_DBErrorEvent(string errorMessage, Exception ex)
        {
            if (Message.IndexOf("permission was denied", StringComparison.OrdinalIgnoreCase) >= 0 ||
                Message.IndexOf("permission denied", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                LogErrorToDatabase(Message);
            }
            else
            {
                LogError(errorMessage, ex);
            }
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile(mProgress);

            LogProgress("GlyQIQ");
        }
    }
}
