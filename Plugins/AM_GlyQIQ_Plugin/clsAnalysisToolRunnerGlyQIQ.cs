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
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using ThermoRawFileReader;

namespace AnalysisManagerGlyQIQPlugin
{
    /// <summary>
    /// Class for running the GlyQ-IQ
    /// </summary>
    public class clsAnalysisToolRunnerGlyQIQ : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        protected const float PROGRESS_PCT_STARTING = 1;
        protected const float PROGRESS_PCT_COMPLETE = 99;

        protected const string STORE_JOB_PSM_RESULTS_SP_NAME = "StoreJobPSMStats";

        #endregion

        #region "Structures"

        protected struct udtPSMStatsType
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

        #endregion

        #region "Module Variables"

        protected int mCoreCount;

        protected int mSpectraSearched;

        /// <summary>
        /// Dictionary of GlyQIqRunner instances
        /// </summary>
        /// <remarks>Key is core number (1 through NumCores), value is the instance</remarks>
        protected Dictionary<int, clsGlyQIqRunner> mGlyQRunners;

        private XRawFileIO mThermoFileReader;

        private PRISM.ExecuteDatabaseSP mStoredProcedureExecutor;

        #endregion

        #region "Methods"

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
                    LogDebug("clsAnalysisToolRunnerGlyQIQ.RunTool(): Enter");
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
                var blnSuccess = RunGlyQIQ();

                if (blnSuccess)
                {
                    blnSuccess = CombineResultFiles();
                }

                // Zip up the settings files and batch files so we have a record of them
                PackageResults();

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.ProgRunner.GarbageCollectNow();

                if (!blnSuccess)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();

                if (!success)
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
                var diResultsFolder = new DirectoryInfo(Path.Combine(mWorkDir, "Results_" + mDatasetName));
                if (!diResultsFolder.Exists)
                {
                    mMessage = "Results folder not found: " + diResultsFolder.FullName;
                    LogError(mMessage);
                    return false;
                }

                var fiUnfilteredResults = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_iqResults_Unfiltered.txt"));
                var fiFilteredResults = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_iqResults.txt"));

                using (var writerUnfiltered = new StreamWriter(new FileStream(fiUnfilteredResults.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                using (var writerFiltered = new StreamWriter(new FileStream(fiFilteredResults.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    for (var core = 1; core <= mCoreCount; core++)
                    {
                        var fiResultFile = new FileInfo(Path.Combine(diResultsFolder.FullName, mDatasetName + "_iqResults_" + core + ".txt"));

                        if (!fiResultFile.Exists)
                        {
                            if (string.IsNullOrEmpty(mMessage))
                            {
                                mMessage = "Result file not found: " + fiResultFile.Name;
                            }
                            LogError("Result file not found: " + fiResultFile.FullName);
                            continue;
                        }

                        var linesRead = 0;
                        using (var reader = new StreamReader(new FileStream(fiResultFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                        {
                            while (!reader.EndOfStream)
                            {
                                var dataLine = reader.ReadLine();
                                linesRead += 1;

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
                }

                // Zip the unfiltered results
                ZipFile(fiUnfilteredResults.FullName, true);

                // Parse the filtered results to count the number of identified glycans
                var blnSuccess = ExamineFilteredResults(fiFilteredResults);

                return blnSuccess;
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

                    if (mThermoFileReader.GetScanInfo(scan, out clsScanInfo scanInfo))
                    {
                        if (scanInfo.MSLevel > 1)
                        {
                            ms2ScanCount += 1;
                        }
                        else
                        {
                            ms1ScanCount += 1;
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

        protected bool ExamineFilteredResults(FileInfo fiResultsFile)
        {
            return ExamineFilteredResults(fiResultsFile, mJob, string.Empty);
        }

        /// <summary>
        /// Examine the GlyQ-IQ results in the given file to count the number of PSMs and unique number of glycans
        /// Post the results to DMS using jobNumber
        /// </summary>
        /// <param name="fiResultsFile"></param>
        /// <param name="jobNumber"></param>
        /// <param name="dmsConnectionStringOverride">Optional: DMS5 connection string</param>
        /// <returns></returns>
        /// <remarks>If dmsConnectionStringOverride is empty then PostJobResults will use the Manager Parameters (mMgrParams)</remarks>
        public bool ExamineFilteredResults(FileInfo fiResultsFile, int jobNumber, string dmsConnectionStringOverride)
        {
            try
            {
                var headerSkipped = false;

                var totalPSMs = 0;
                var uniqueCodeFormulaCombos = new SortedSet<string>();
                var uniqueCodes = new SortedSet<string>();

                using (var reader = new StreamReader(new FileStream(fiResultsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
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

                        totalPSMs += 1;
                    }
                }

                var udtPSMStats = new udtPSMStatsType();
                udtPSMStats.Clear();
                udtPSMStats.TotalPSMs = totalPSMs;
                udtPSMStats.UniquePeptideCount = uniqueCodeFormulaCombos.Count;
                udtPSMStats.UniqueProteinCount = uniqueCodes.Count;

                // Store the results in the database
                PostJobResults(jobNumber, udtPSMStats, dmsConnectionStringOverride);

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
            var diTempZipFolder = new DirectoryInfo(Path.Combine(mWorkDir, "FilesToZip"));

            try
            {
                if (!diTempZipFolder.Exists)
                {
                    diTempZipFolder.Create();
                }

                // Move the batch files and console output files into the FilesToZip folder
                var diWorkDir = new DirectoryInfo(mWorkDir);
                var lstFilesToMove = new List<FileInfo>();

                var lstFiles = diWorkDir.GetFiles("*.bat");
                lstFilesToMove.AddRange(lstFiles);

                // We don't keep the entire ConsoleOutput file
                // Instead, just keep a trimmed version of the original, removing extraneous log messages
                foreach (var fiConsoleOutputFile in diWorkDir.GetFiles(clsGlyQIqRunner.GLYQ_IQ_CONSOLE_OUTPUT_PREFIX + "*.txt"))
                {
                    PruneConsoleOutputFiles(fiConsoleOutputFile, diTempZipFolder);
                }

                lstFilesToMove.AddRange(lstFiles);

                foreach (var fiFile in lstFilesToMove)
                {
                    fiFile.MoveTo(Path.Combine(diTempZipFolder.FullName, fiFile.Name));
                }

                // Move selected files from the first WorkingParameters folder

                // We just need to copy files from the first core's WorkingParameters folder
                var diWorkingParamsSource = new DirectoryInfo(Path.Combine(mWorkDir, "WorkingParametersCore1"));

                var diWorkingParamsTarget = new DirectoryInfo(Path.Combine(diTempZipFolder.FullName, "WorkingParameters"));
                if (!diWorkingParamsTarget.Exists)
                {
                    diWorkingParamsTarget.Create();
                }

                var iqParamFileName = mJobParams.GetJobParameter("ParmFileName", "");
                foreach (var fiFile in diWorkingParamsSource.GetFiles())
                {
                    var moveFile = false;

                    if (string.Equals(fiFile.Name, iqParamFileName, StringComparison.OrdinalIgnoreCase))
                    {
                        moveFile = true;
                    }
                    else if (fiFile.Name.StartsWith(clsAnalysisResourcesGlyQIQ.GLYQIQ_PARAMS_FILE_PREFIX))
                    {
                        moveFile = true;
                    }
                    else if (fiFile.Name.StartsWith(clsAnalysisResourcesGlyQIQ.ALIGNMENT_PARAMETERS_FILENAME))
                    {
                        moveFile = true;
                    }
                    else if (fiFile.Name.StartsWith(clsAnalysisResourcesGlyQIQ.EXECUTOR_PARAMETERS_FILE))
                    {
                        moveFile = true;
                    }

                    if (moveFile)
                    {
                        fiFile.MoveTo(Path.Combine(diWorkingParamsTarget.FullName, fiFile.Name));
                    }
                }

                var strZipFilePath = Path.Combine(mWorkDir, "GlyQIq_Automation_Files.zip");

                mDotNetZipTools.ZipDirectory(diTempZipFolder.FullName, strZipFilePath);
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
                clsGlobal.IdleLoop(0.25);
                diTempZipFolder.Delete(true);
                diTempZipFolder.Create();
            }
            catch (Exception)
            {
                // This error can be safely ignored
            }

        }

        protected bool PostJobResults(int jobNumber, udtPSMStatsType udtPSMStats, string dmsConnectionStringOverride)
        {
            const int MAX_RETRY_COUNT = 3;

            try
            {
                // Call stored procedure StoreJobPSMStats in DMS5

                var objCommand = new SqlCommand
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandText = STORE_JOB_PSM_RESULTS_SP_NAME
                };

                objCommand.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int));
                objCommand.Parameters["@Return"].Direction = ParameterDirection.ReturnValue;

                objCommand.Parameters.Add(new SqlParameter("@Job", SqlDbType.Int));
                objCommand.Parameters["@Job"].Direction = ParameterDirection.Input;
                objCommand.Parameters["@Job"].Value = jobNumber;

                objCommand.Parameters.Add(new SqlParameter("@MSGFThreshold", SqlDbType.Float));
                objCommand.Parameters["@MSGFThreshold"].Direction = ParameterDirection.Input;

                objCommand.Parameters["@MSGFThreshold"].Value = 1;

                objCommand.Parameters.Add(new SqlParameter("@FDRThreshold", SqlDbType.Float));
                objCommand.Parameters["@FDRThreshold"].Direction = ParameterDirection.Input;
                objCommand.Parameters["@FDRThreshold"].Value = 0.25;

                objCommand.Parameters.Add(new SqlParameter("@SpectraSearched", SqlDbType.Int));
                objCommand.Parameters["@SpectraSearched"].Direction = ParameterDirection.Input;
                objCommand.Parameters["@SpectraSearched"].Value = mSpectraSearched;

                objCommand.Parameters.Add(new SqlParameter("@TotalPSMs", SqlDbType.Int));
                objCommand.Parameters["@TotalPSMs"].Direction = ParameterDirection.Input;
                objCommand.Parameters["@TotalPSMs"].Value = udtPSMStats.TotalPSMs;

                objCommand.Parameters.Add(new SqlParameter("@UniquePeptides", SqlDbType.Int));
                objCommand.Parameters["@UniquePeptides"].Direction = ParameterDirection.Input;
                objCommand.Parameters["@UniquePeptides"].Value = udtPSMStats.UniquePeptideCount;

                objCommand.Parameters.Add(new SqlParameter("@UniqueProteins", SqlDbType.Int));
                objCommand.Parameters["@UniqueProteins"].Direction = ParameterDirection.Input;
                objCommand.Parameters["@UniqueProteins"].Value = udtPSMStats.UniqueProteinCount;

                objCommand.Parameters.Add(new SqlParameter("@TotalPSMsFDRFilter", SqlDbType.Int));
                objCommand.Parameters["@TotalPSMsFDRFilter"].Direction = ParameterDirection.Input;
                objCommand.Parameters["@TotalPSMsFDRFilter"].Value = udtPSMStats.TotalPSMs;

                objCommand.Parameters.Add(new SqlParameter("@UniquePeptidesFDRFilter", SqlDbType.Int));
                objCommand.Parameters["@UniquePeptidesFDRFilter"].Direction = ParameterDirection.Input;
                objCommand.Parameters["@UniquePeptidesFDRFilter"].Value = udtPSMStats.UniquePeptideCount;

                objCommand.Parameters.Add(new SqlParameter("@UniqueProteinsFDRFilter", SqlDbType.Int));
                objCommand.Parameters["@UniqueProteinsFDRFilter"].Direction = ParameterDirection.Input;
                objCommand.Parameters["@UniqueProteinsFDRFilter"].Value = udtPSMStats.UniqueProteinCount;

                objCommand.Parameters.Add(new SqlParameter("@MSGFThresholdIsEValue", SqlDbType.TinyInt));
                objCommand.Parameters["@MSGFThresholdIsEValue"].Direction = ParameterDirection.Input;

                objCommand.Parameters["@MSGFThresholdIsEValue"].Value = 0;

                if (mStoredProcedureExecutor == null || !string.IsNullOrWhiteSpace(dmsConnectionStringOverride))
                {
                    string strConnectionString;

                    if (string.IsNullOrWhiteSpace(dmsConnectionStringOverride))
                    {
                        if (mMgrParams == null)
                        {
                            throw new Exception("mMgrParams object has not been initialized");
                        }

                        // Gigasax.DMS5
                        strConnectionString = mMgrParams.GetParam("ConnectionString");
                    }
                    else
                    {
                        strConnectionString = dmsConnectionStringOverride;
                    }

                    mStoredProcedureExecutor = new PRISM.ExecuteDatabaseSP(strConnectionString);
                    mStoredProcedureExecutor.DebugEvent += ExecuteSP_DebugEvent;
                    mStoredProcedureExecutor.ErrorEvent += ExecuteSP_DBErrorEvent;
                }

                // Execute the SP (retry the call up to 3 times)
                var resCode = mStoredProcedureExecutor.ExecuteSP(objCommand, MAX_RETRY_COUNT, out _);

                if (resCode == 0)
                {
                    return true;
                }

                var msg = "Error storing PSM Results in database";
                LogError(msg, msg + ", " + STORE_JOB_PSM_RESULTS_SP_NAME + " returned " + resCode);

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception storing PSM Results in database: " + ex.Message);
                return false;
            }

        }

        protected void PruneConsoleOutputFiles(FileInfo fiConsoleOutputFile, DirectoryInfo diTargetFolder)
        {
            if (fiConsoleOutputFile.Directory == null)
                return;

            if (fiConsoleOutputFile.Directory.FullName == diTargetFolder.FullName)
            {
                throw new Exception("The Source console output file cannot reside in the Target Folder: " + fiConsoleOutputFile.FullName + " vs. " + diTargetFolder.FullName);
            }

            try
            {
                var lstLinesToPrune = new List<string>
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

                var reNumericLine = new Regex(@"^[0-9.]+$", RegexOptions.Compiled);

                var consoleOutputFilePruned = Path.Combine(diTargetFolder.FullName, fiConsoleOutputFile.Name);

                using (var reader = new StreamReader(new FileStream(fiConsoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
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

                        var skipLine = lstLinesToPrune.Any(textToFind => dataLine.StartsWith(textToFind));

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
                // The pruned file was created in diTargetFolder and will get included in GlyQIq_Automation_Files.zip
                //
                mJobParams.AddResultFileToSkip(fiConsoleOutputFile.Name);
            }
            catch (Exception ex)
            {
                LogError("Exception in PruneConsoleOutputFiles: " + ex.Message);
            }
        }

        protected bool RunGlyQIQ()
        {
            var currentTask = "Initializing";

            try
            {
                mCoreCount = mJobParams.GetJobParameter(clsAnalysisResourcesGlyQIQ.JOB_PARAM_ACTUAL_CORE_COUNT, 0);
                if (mCoreCount < 1)
                {
                    mMessage = "Core count reported by " + clsAnalysisResourcesGlyQIQ.JOB_PARAM_ACTUAL_CORE_COUNT + " is 0; unable to continue";
                    return false;
                }

                var rawDataType = mJobParams.GetParam("RawDataType");
                var eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType);

                if (eRawDataType == clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile)
                {
                    mJobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION);
                }
                else
                {
                    mMessage = "GlyQ-IQ presently only supports Thermo .Raw files";
                    return false;
                }

                // Determine the number of MS/MS spectra in the .Raw file (required for PostJobResults)
                var rawFilePath = Path.Combine(mWorkDir, mDatasetName + clsAnalysisResources.DOT_RAW_EXTENSION);
                mSpectraSearched = CountMsMsSpectra(rawFilePath);

                // Set up and execute a program runner to run each batch file that launches GlyQ-IQ

                mProgress = PROGRESS_PCT_STARTING;

                mGlyQRunners = new Dictionary<int, clsGlyQIqRunner>();
                // var lstThreads = new List<Thread>();

                for (var core = 1; core <= mCoreCount; core++)
                {
                    var batchFilePath = Path.Combine(mWorkDir, clsAnalysisResourcesGlyQIQ.START_PROGRAM_BATCH_FILE_PREFIX + core + ".bat");

                    currentTask = "Launching GlyQ-IQ, core " + core;
                    LogDebug(currentTask + ": " + batchFilePath);

                    var glyQRunner = new clsGlyQIqRunner(mWorkDir, core, batchFilePath);
                    glyQRunner.CmdRunnerWaiting += CmdRunner_LoopWaiting;
                    mGlyQRunners.Add(core, glyQRunner);

                    var newThread = new System.Threading.Thread(glyQRunner.StartAnalysis) {
                        Priority = System.Threading.ThreadPriority.BelowNormal
                    };

                    newThread.Start();
                    // lstThreads.Add(newThread);
                }

                // Wait for all of the threads to exit
                // Run for a maximum of 14 days

                currentTask = "Waiting for all of the threads to exit";

                var dtStartTime = DateTime.UtcNow;
                var completedCores = new SortedSet<int>();

                while (true)
                {
                    // Poll the status of each of the threads

                    var stepsComplete = 0;
                    double progressSum = 0;

                    foreach (var glyQRunner in mGlyQRunners)
                    {
                        var eStatus = glyQRunner.Value.Status;
                        if (eStatus >= clsGlyQIqRunner.GlyQIqRunnerStatusCodes.Success)
                        {
                            // Analysis completed (or failed)
                            stepsComplete += 1;

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

                    clsGlobal.IdleLoop(2);

                    if (DateTime.UtcNow.Subtract(dtStartTime).TotalDays > 14)
                    {
                        mMessage = "GlyQ-IQ ran for over 14 days; aborting";

                        foreach (var glyQRunner in mGlyQRunners)
                        {
                            glyQRunner.Value.AbortProcessingNow();
                        }

                        return false;
                    }
                }

                var blnSuccess = true;
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
                        blnSuccess = false;
                    }

                    if (progRunner.ExitCode != 0 && exitCode == 0)
                    {
                        exitCode = progRunner.ExitCode;
                    }
                }

                if (!blnSuccess)
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
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string progLoc)
        {

            var additionalDLLs = new List<string>
            {
                "IQGlyQ.dll",
                "IQ2_x64.dll",
                "Run64.dll"
            };

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDLLs);

            return success;
        }

        #endregion

        #region "Event Handlers"

        private void ExecuteSP_DebugEvent(string errorMessage)
        {
            LogDebug("StoredProcedureExecutor: " + errorMessage);
        }

        private void ExecuteSP_DBErrorEvent(string errorMessage, Exception ex)
        {
            LogError("StoredProcedureExecutor: " + errorMessage);

            if (Message.ToLower().Contains("permission was denied"))
            {
                LogErrorToDatabase(Message);
            }
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile(mProgress);

            LogProgress("GlyQIQ");
        }

        #endregion
    }
}
