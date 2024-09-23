//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

using AnalysisManagerBase;
using PRISM;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;
using System.Xml;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISMDatabaseUtils;

namespace AnalysisManagerMasicPlugin
{
    /// <summary>
    /// Class for performing MASIC analysis
    /// </summary>
    public abstract class AnalysisToolRunnerMASICBase : AnalysisToolRunnerBase
    {
        // ReSharper disable once CommentTypo
        // Ignore Spelling: Az, Glc, labelling, loc, MASIC, Perf, prog, Traq

        private const string MASIC_STATUS_FILE_PREFIX = "MasicStatus_";

        private const string STORE_REPORTER_ION_OBS_STATS_SP_NAME = "store_reporter_ion_obs_stats";

        private const string SICS_XML_FILE_SUFFIX = "_SICs.xml";

        // Job running status variable
        private bool mJobRunning;

        protected string mErrorMessage = string.Empty;

        private string mProcessStep = string.Empty;
        private string mMASICStatusFileName = string.Empty;

        private bool mRemovedOldMasicStatusFiles;

        private string mReporterIonName = string.Empty;
        private int mReporterIonObservationRateTopNPct;

        private void ExtractErrorsFromMASICLogFile(FileSystemInfo logFile, out int errorCount)
        {
            // Read the most recent MASIC_Log file and look for any lines with the text "Error"

            errorCount = 0;

            try
            {
                if (!logFile.Exists)
                    return;

                var errorMatcher = new Regex(@"\berror\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                using var reader = new StreamReader(new FileStream(logFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                errorCount = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrEmpty(dataLine))
                        continue;

                    if (!errorMatcher.IsMatch(dataLine))
                        continue;

                    if (errorCount == 0)
                    {
                        LogError("Errors found in the MASIC log file");

                        if (string.IsNullOrWhiteSpace(mErrorMessage))
                        {
                            // Store the first error in mErrorMessage
                            mErrorMessage = dataLine;
                        }
                    }

                    if (errorCount <= 10)
                        LogWarning(" ... " + dataLine);

                    errorCount++;
                }

                if (errorCount > 10)
                {
                    LogWarning(" ... {0} total errors", errorCount);
                }
            }
            catch (Exception ex)
            {
                if (string.IsNullOrWhiteSpace(mErrorMessage))
                {
                    mErrorMessage = string.Format("Error reading MASIC log file ({0}): {1}", logFile.Name, ex.Message);
                }

                LogError("Error reading MASIC log file at '" + logFile.FullName + "'; " + ex.Message, ex);
                errorCount++;
            }
        }

        /// <summary>
        /// Primary entry point for running this tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
        public override CloseOutType RunTool()
        {
            // Call base class for initial setup
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the MASIC version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                mMessage = "Error determining MASIC version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Start the job timer
            mStartTime = DateTime.UtcNow;
            mMessage = string.Empty;

            LogMessage("Calling MASIC to create the SIC files, job " + mJob);

            CloseOutType processingResult;

            try
            {
                // Note that RunMASIC will populate the File Path variables, then will call
                //  StartMASICAndWait() and WaitForJobToFinish(), which are in this class
                processingResult = RunMASIC();
            }
            catch (Exception ex)
            {
                LogError("AnalysisToolRunnerMASICBase.RunTool(), Exception calling MASIC to create the SIC files, " + ex.Message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            mProgress = 100;
            UpdateStatusFile();

            // Run the cleanup routine from the base class
            var postProcessingResult = PerfPostAnalysisTasks();

            if (processingResult != CloseOutType.CLOSEOUT_SUCCESS || postProcessingResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Something went wrong
                // In order to help diagnose things, move the output files into the results directory,
                // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                CopyFailedResultsToArchiveDirectory();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make the results folder
            if (mDebugLevel > 3)
            {
                LogDebug("AnalysisToolRunnerMASICBase.RunTool(), Making results folder");
            }

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        protected CloseOutType StartMASICAndWait(string inputFilePath, string outputFolderPath, string parameterFilePath)
        {
            // Note that this method is normally called by RunMasic() in the subclass

            var masicExePath = string.Empty;

            mErrorMessage = string.Empty;
            mProcessStep = "NewTask";

            try
            {
                mMASICStatusFileName = string.Format("{0}{1}.xml", MASIC_STATUS_FILE_PREFIX, mMgrName);
            }
            catch (Exception)
            {
                mMASICStatusFileName = string.Format("{0}{1}.xml", MASIC_STATUS_FILE_PREFIX, "Undefined");
            }

            // Make sure the MASIC executable file exists (MASIC_Console.exe)
            try
            {
                // This manager parameter is the full path to the MASIC .exe
                masicExePath = mMgrParams.GetParam("MasicProgLoc");

                if (!File.Exists(masicExePath))
                {
                    LogError("AnalysisToolRunnerMASICBase.StartMASICAndWait(); MASIC not found at: " + masicExePath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                LogError("AnalysisToolRunnerMASICBase.StartMASICAndWait(); Error looking for MASIC_Console.exe at " + masicExePath, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Call MASIC using the Program Runner class

            var logFile = new FileInfo(Path.Combine(mWorkDir, "MASIC_Log_Job" + mJob + ".txt"));

            // Define the parameters to send to MASIC_Console.exe
            var argumentList = new List<string>
            {
                "/I:" + inputFilePath,
                "/O:" + outputFolderPath,
                "/P:" + parameterFilePath,
                "/SF:" + mMASICStatusFileName,
                "/L:" + PathUtils.PossiblyQuotePath(logFile.FullName)
            };

            var arguments = string.Join(" ", argumentList);

            if (mDebugLevel >= 1)
            {
                LogDebug(masicExePath + " " + arguments);
            }

            var masicProgRunner = new ProgRunner
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = false,
                Name = "MASIC",
                Program = masicExePath,
                Arguments = arguments,
                WorkDir = mWorkDir
            };

            ResetProgRunnerCpuUsage();

            masicProgRunner.StartAndMonitorProgram();

            // Wait for the job to complete
            var success = WaitForJobToFinish(masicProgRunner);

            // Delay for 3 seconds to make sure program exits
            Global.IdleLoop(3);

            // Read the most recent MASIC_Log file and look for any lines with the text "Error"
            ExtractErrorsFromMASICLogFile(logFile, out var errorCount);

            // Verify MASIC exited due to job completion
            if (success && errorCount == 0)
            {
                mJobParams.AddResultFileToSkip(logFile.Name);
            }
            else
            {
                if (mDebugLevel > 1)
                {
                    LogError("WaitForJobToFinish returned false");
                }

                if (!string.IsNullOrEmpty(mErrorMessage))
                {
                    LogError("AnalysisToolRunnerMASICBase.StartMASICAndWait(); MASIC Error message: " + mErrorMessage);

                    if (string.IsNullOrEmpty(mMessage))
                        mMessage = mErrorMessage;
                }
                else
                {
                    LogError("AnalysisToolRunnerMASICBase.StartMASICAndWait(); MASIC Error message is blank");

                    if (string.IsNullOrEmpty(mMessage))
                        mMessage = "Unknown error running MASIC";
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (mDebugLevel > 0)
            {
                LogDebug("AnalysisToolRunnerMASICBase.StartMASICAndWait(); mProcessStep=" + mProcessStep);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        protected abstract CloseOutType RunMASIC();

        protected abstract CloseOutType DeleteDataFile();

        protected virtual void CalculateNewStatus(string masicProgLoc)
        {
            // Calculates status information for progress file
            // Does this by reading the MasicStatus.xml file

            var progress = string.Empty;

            try
            {
                var masicExe = new FileInfo(masicProgLoc);

                if (masicExe.DirectoryName == null)
                    return;

                var statusFile = new FileInfo(Path.Combine(masicExe.DirectoryName, mMASICStatusFileName));

                if (!statusFile.Exists)
                    return;

                if (!mRemovedOldMasicStatusFiles)
                {
                    // Remove any MasicStatus files that were created over 6 months ago
                    RemoveOldMasicStatusFiles(masicExe.DirectoryName);
                    mRemovedOldMasicStatusFiles = true;
                }

                using var reader = new XmlTextReader(new FileStream(statusFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    WhitespaceHandling = WhitespaceHandling.None
                };

                while (reader.Read())
                {
                    if (reader.NodeType != XmlNodeType.Element)
                        continue;

                    switch (reader.Name)
                    {
                        case "ProcessingStep":
                            if (!reader.IsEmptyElement)
                            {
                                if (reader.Read())
                                    mProcessStep = reader.Value;
                            }
                            break;
                        case "Progress":
                            if (!reader.IsEmptyElement)
                            {
                                if (reader.Read())
                                    progress = reader.Value;
                            }
                            break;
                        case "Error":
                            if (!reader.IsEmptyElement)
                            {
                                if (reader.Read())
                                    mErrorMessage = reader.Value;
                            }
                            break;
                    }
                }

                if (string.IsNullOrEmpty(progress))
                    return;

                try
                {
                    mProgress = float.Parse(progress);
                }
                catch (Exception)
                {
                    // Ignore errors
                }
            }
            catch (Exception)
            {
                // Ignore errors
            }
        }

        private int GetColumnIndex(string headerLine, string columnName, int indexIfMissing)
        {
            var columnNames = headerLine.Split('\t');

            for (var i = 0; i < columnNames.Length; i++)
            {
                if (columnNames[i].Equals(columnName, StringComparison.OrdinalIgnoreCase))
                    return i;
            }

            LogWarning("Header line does not contain column '{0}'; will presume the data is in column {1}", columnName, indexIfMissing + 1);

            return indexIfMissing;
        }

        /// <summary>
        /// Get the DMS-compatible reporter ion name from the MASIC reporter ion mass mode
        /// </summary>
        /// <remarks>MASIC mass modes: https://github.com/PNNL-Comp-Mass-Spec/MASIC/blob/master/Data/ReporterIons.cs#L46
        /// </remarks>
        /// <param name="reporterIonMassMode">MASIC reporter ion mass mode</param>
        private static string GetReporterIonNameFromMassMode(int reporterIonMassMode)
        {
            return reporterIonMassMode switch
            {
                1 => "iTRAQ",     // ITraqFourMZ
                3 => "TMT2",      // TMTTwoMZ
                4 => "TMT6",      // TMTSixMZ
                5 => "iTRAQ8",    // ITraqEightMZHighRes
                6 => "iTRAQ8",    // ITraqEightMZLowRes
                10 => "TMT10",    // TMTTenMZ
                11 => "PCGalNAz", // OGlcNAc
                16 => "TMT11",    // TMTElevenMZ
                18 => "TMT16",    // TMTSixteenMZ
                20 => "TMT18",    // TMTEighteenMZ
                21 => "TMT32",    // TMT32MZ
                22 => "TMT35",    // TMT35MZ
                _ => "ReporterIonMassMode_" + reporterIonMassMode + "__NeedToUpdateAnalysisManagerPlugin"
            };
        }

        protected virtual CloseOutType PerfPostAnalysisTasks()
        {
            // Stop the job timer
            mStopTime = DateTime.UtcNow;

            // Get rid of raw data file
            var stepResult = DeleteDataFile();

            if (stepResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return stepResult;
            }

            // Zip the _SICs.XML file (if it exists; it won't if SkipSICProcessing is true in the parameter file)
            var foundFiles = Directory.GetFiles(mWorkDir, "*" + SICS_XML_FILE_SUFFIX);

            if (foundFiles.Length > 0)
            {
                // Setup zipper

                var zipFileName = mDatasetName + "_SICs.zip";

                if (!ZipFile(foundFiles[0], true, Path.Combine(mWorkDir, zipFileName)))
                {
                    LogErrorToDatabase("Error zipping " + Path.GetFileName(foundFiles[0]) + ", job " + mJob);
                    UpdateStatusMessage("Error zipping " + SICS_XML_FILE_SUFFIX + " file");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            // Add all the extensions of the files to delete after run
            mJobParams.AddResultFileExtensionToSkip(SICS_XML_FILE_SUFFIX); // Unzipped, concatenated DTA

            // If a _RepIonObsRate.txt file was created, read the data and push into DMS
            var success = StoreReporterIonObservationRateStats();

            if (!success)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Add the current job data to the summary file
            UpdateSummaryFile();

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool ReadReporterIonIntensityStatsFile(
            FileSystemInfo intensityStatsFile,
            out List<int> medianIntensitiesTopNPct
            )
        {
            medianIntensitiesTopNPct = new List<int>();

            try
            {
                LogDebug("Reading " + intensityStatsFile.FullName);

                using var reader = new StreamReader(new FileStream(intensityStatsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                if (reader.EndOfStream)
                {
                    LogError("Reporter ion intensity stats file is empty: " + intensityStatsFile.FullName);
                    return false;
                }

                // Validate the header line
                var headerLine = reader.ReadLine();

                var medianColumnIndex = GetColumnIndex(headerLine, "Median_Top80Pct", 2);

                var channel = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    // Columns:
                    // Reporter_Ion    NonZeroCount_Top80Pct    Median_Top80Pct    InterQuartileRange_Top80Pct    LowerWhisker_Top80Pct    etc.
                    var lineParts = dataLine.Split('\t');

                    if (lineParts.Length > 0 && lineParts[0].StartsWith("Reporter_Ion", StringComparison.OrdinalIgnoreCase))
                    {
                        // The _RepIonStats.txt file has two tables of intensity stats
                        // We have reached the second table
                        break;
                    }

                    channel++;

                    if (lineParts.Length < medianColumnIndex + 1)
                    {
                        LogError("Channel {0} in the reporter ion intensity stats file has fewer than three columns; corrupt file: {1}", channel, intensityStatsFile.FullName);
                        return false;
                    }

                    if (!int.TryParse(lineParts[medianColumnIndex], out var medianTopNPct))
                    {
                        LogError("Channel {0} in the reporter ion intensity stats file has a non-integer Median_Top80Pct value: {1}", channel, lineParts[medianColumnIndex]);
                        return false;
                    }

                    medianIntensitiesTopNPct.Add(medianTopNPct);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error reading the _RepIonStats.txt file", ex);
                return false;
            }
        }

        private bool ReadReporterIonObservationRateFile(
            FileSystemInfo observationRateFile,
            out List<double> observationStatsTopNPct)
        {
            observationStatsTopNPct = new List<double>();

            try
            {
                LogDebug("Reading " + observationRateFile.FullName);

                using var reader = new StreamReader(new FileStream(observationRateFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                if (reader.EndOfStream)
                {
                    LogError("Reporter ion observation rate file is empty: " + observationRateFile.FullName);
                    return false;
                }

                // Validate the header line
                var headerLine = reader.ReadLine();

                var obsRateColumnIndex = GetColumnIndex(headerLine, "Observation_Rate_Top80Pct", 2);

                var channel = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    // Columns:
                    // Reporter_Ion     Observation_Rate     Observation_Rate_Top80Pct
                    var lineParts = dataLine.Split('\t');

                    channel++;

                    if (lineParts.Length < obsRateColumnIndex + 1)
                    {
                        LogError("Channel {0} in the reporter ion observation rate file has fewer than three columns; corrupt file: {1}", channel, observationRateFile.FullName);
                        return false;
                    }

                    if (!double.TryParse(lineParts[obsRateColumnIndex], out var observationRateTopNPct))
                    {
                        LogError("Channel {0} in the reporter ion observation rate file has a non-numeric Observation_Rate_Top80Pct value: {1}", channel, lineParts[obsRateColumnIndex]);
                        return false;
                    }

                    observationStatsTopNPct.Add(observationRateTopNPct);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error reading the _RepIonObsRate.txt file", ex);
                return false;
            }
        }

        /// <summary>
        /// Remove any MasicStatus files that were created over 6 months ago
        /// </summary>
        /// <remarks>
        /// For example, if the DMS_Programs directory was copied from one server to another server,
        /// MasicStatus files from the old server may still be in the MASIC directory
        /// </remarks>
        /// <param name="masicDirectoryPath"></param>
        private void RemoveOldMasicStatusFiles(string masicDirectoryPath)
        {
            try
            {
                var masicDirectory = new DirectoryInfo(masicDirectoryPath);

                foreach (var item in masicDirectory.GetFiles(string.Format("{0}*.xml", MASIC_STATUS_FILE_PREFIX)))
                {
                    try
                    {
                        if (DateTime.UtcNow.Subtract(item.LastWriteTimeUtc).TotalDays < 180)
                            continue;

                        LogMessage("Removing old MASIC status file: {0} (last modified {1:yyyy-MM-dd})", item.FullName, item.LastWriteTime);

                        item.Delete();
                    }
                    catch (Exception ex)
                    {
                        LogErrorToDatabase(string.Format(
                            "Error deleting old MASIC status file at {0}: {1}",
                            item.FullName, ex.Message));
                    }
                }
            }
            catch (Exception ex)
            {
                LogErrorToDatabase(string.Format(
                    "Error deleting old MASIC status files in {0}: {1}",
                    masicDirectoryPath ?? "?Undefined?", ex.Message));
            }
        }

        private bool StoreReporterIonObservationRateStats()
        {
            try
            {
                var observationRateFile = new FileInfo(Path.Combine(mWorkDir, Dataset + "_RepIonObsRate.txt"));

                if (!observationRateFile.Exists)
                    return true;

                var intensityStatsFile = new FileInfo(Path.Combine(mWorkDir, Dataset + "_RepIonStats.txt"));

                var obsRatesLoaded = ReadReporterIonObservationRateFile(
                    observationRateFile,
                    out var observationStatsTopNPct);

                var intensityStatsLoaded = ReadReporterIonIntensityStatsFile(
                    intensityStatsFile,
                    out var medianIntensitiesTopNPct);

                if (!obsRatesLoaded || !intensityStatsLoaded)
                    return false;

                var analysisTask = new AnalysisJob(mMgrParams, mDebugLevel);
                var dbTools = analysisTask.DMSProcedureExecutor;

                // Call stored procedure store_reporter_ion_obs_stats in the DMS database
                // Data is stored in tables T_Reporter_Ion_Observation_Rates and T_Reporter_Ion_Observation_Rates_Addnl
                var sqlCmd = dbTools.CreateCommand(STORE_REPORTER_ION_OBS_STATS_SP_NAME, CommandType.StoredProcedure);

                // ReSharper disable once CommentTypo
                // Note that reporterIonName must match a Label in T_Sample_Labelling_Reporter_Ions
                if (string.IsNullOrWhiteSpace(mReporterIonName))
                {
                    LogError("Reporter ion name is empty for job {0}; " +
                             "cannot store reporter ion observation stats in the database", mJob);

                    return false;
                }

                // Define parameter for procedure's return value
                // If querying a Postgres DB, dbTools will auto-change "@return" to "_returnCode"
                var returnParam = dbTools.AddParameter(sqlCmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);

                dbTools.AddTypedParameter(sqlCmd, "@job", SqlType.Int, value: mJob);
                dbTools.AddParameter(sqlCmd, "@reporterIon", SqlType.VarChar, 64).Value = mReporterIonName;
                dbTools.AddParameter(sqlCmd, "@topNPct", SqlType.Int).Value = mReporterIonObservationRateTopNPct;
                dbTools.AddParameter(sqlCmd, "@observationStatsTopNPct", SqlType.VarChar, 4000).Value = string.Join(",", observationStatsTopNPct);
                dbTools.AddParameter(sqlCmd, "@medianIntensitiesTopNPct", SqlType.VarChar, 4000).Value = string.Join(",", medianIntensitiesTopNPct);
                var messageParam = dbTools.AddTypedParameter(sqlCmd, "@message", SqlType.VarChar, 255, string.Empty, ParameterDirection.InputOutput);

                if (dbTools.DbServerType == DbServerTypes.PostgreSQL)
                {
                    dbTools.AddTypedParameter(sqlCmd, "@infoOnly", SqlType.Boolean, value: false);
                }
                else
                {
                    dbTools.AddTypedParameter(sqlCmd, "@infoOnly", SqlType.TinyInt, value: 0);
                }

                // Call the procedure (retry the call, up to 3 times)
                var resCode = dbTools.ExecuteSP(sqlCmd);

                var returnCode = DBToolsBase.GetReturnCode(returnParam);

                if (resCode == 0 && returnCode == 0)
                {
                    return true;
                }

                if (resCode != 0 && returnCode == 0)
                {
                    LogError("ExecuteSP() reported result code {0} storing reporter ion observation stats and median intensities in the database using {1}",
                        resCode, STORE_REPORTER_ION_OBS_STATS_SP_NAME);

                    return false;
                }

                var message = messageParam.Value.CastDBVal<string>();
                var errorMessage = string.IsNullOrWhiteSpace(message) ? "No error message" : message;

                LogError(
                    "Error storing reporter ion observation stats and median intensities in the database, {0} returned {1}: {2}",
                    STORE_REPORTER_ION_OBS_STATS_SP_NAME, returnParam.Value.CastDBVal<string>(), errorMessage);

                return false;
            }
            catch (Exception ex)
            {
                LogError("Error posting reporter ion observation rate stats", ex);
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            var masicExecutablePath = mMgrParams.GetParam("MasicProgLoc");
            var success = StoreDotNETToolVersionInfo(masicExecutablePath, false);

            return success;
        }

        /// <summary>
        /// Validate that required options are defined in the MASIC parameter file
        /// </summary>
        /// <remarks>Also reads ReporterIonMassMode and ReporterIonObservationRateTopNPct</remarks>
        /// <param name="parameterFilePath"></param>
        protected bool ValidateParameterFile(string parameterFilePath)
        {
            if (string.IsNullOrWhiteSpace(parameterFilePath))
            {
                LogWarning("The MASIC Parameter File path is empty; nothing to validate");
                return true;
            }

            LogDebug("Reading options in MASIC parameter file: " + Path.GetFileName(parameterFilePath));

            var masicSettings = new XmlSettingsFileAccessor();

            if (!masicSettings.LoadSettings(parameterFilePath))
            {
                LogError("Error loading parameter file " + parameterFilePath);
                return false;
            }

            if (!masicSettings.SectionPresent("MasicExportOptions"))
            {
                LogWarning("MasicExportOptions section not found in " + parameterFilePath);
                masicSettings.SetParam("MasicExportOptions", "IncludeHeaders", "True");
                masicSettings.SaveSettings();
                return true;
            }

            var includeHeaders = masicSettings.GetParam("MasicExportOptions", "IncludeHeaders", false, out _);

            if (!includeHeaders)
            {
                // File needs to be updated
                masicSettings.SetParam("MasicExportOptions", "IncludeHeaders", "True");
                masicSettings.SaveSettings();
            }

            if (masicSettings.SectionPresent("MasicExportOptions"))
            {
                var reporterIonMassMode = masicSettings.GetParam("MasicExportOptions", "ReporterIonMassMode", 0);

                mReporterIonName = GetReporterIonNameFromMassMode(reporterIonMassMode);
            }

            if (masicSettings.SectionPresent("PlotOptions"))
            {
                mReporterIonObservationRateTopNPct = masicSettings.GetParam("PlotOptions", "ReporterIonObservationRateTopNPct", 0);
            }

            return true;
        }

        private bool WaitForJobToFinish(ProgRunner masicProgRunner)
        {
            const int MAX_RUNTIME_HOURS = 24;
            const int SECONDS_BETWEEN_UPDATE = 30;

            var sicsXMLFileExists = false;
            var startTime = DateTime.UtcNow;
            var abortedProgram = false;

            // Wait for completion
            mJobRunning = true;

            while (mJobRunning)
            {
                // Wait for 30 seconds
                Global.IdleLoop(SECONDS_BETWEEN_UPDATE);

                if (masicProgRunner.State == ProgRunner.States.NotMonitoring)
                {
                    mJobRunning = false;
                }
                else
                {
                    // Update the status
                    CalculateNewStatus(masicProgRunner.Program);
                    UpdateStatusFile();

                    var processID = 0;

                    try
                    {
                        // Note that the call to GetCoreUsage() will take at least 1 second
                        processID = masicProgRunner.PID;
                        var coreUsage = Global.ProcessInfo.GetCoreUsageByProcessID(processID);

                        UpdateProgRunnerCpuUsage(masicProgRunner.PID, coreUsage, SECONDS_BETWEEN_UPDATE);
                    }
                    catch (Exception ex)
                    {
                        // Sometimes we get exception "Performance counter not found for processID 4896" if the process ends before we can check its core usage
                        // Log a warning since this is not a fatal error
                        LogWarning("Exception getting core usage for MASIC, process ID " + processID + ": " + ex.Message);
                    }

                    LogProgress("MASIC");
                }

                if (DateTime.UtcNow.Subtract(startTime).TotalHours >= MAX_RUNTIME_HOURS)
                {
                    // Abort processing
                    masicProgRunner.StopMonitoringProgram(kill: true);
                    abortedProgram = true;
                }
            }

            if (mDebugLevel > 0)
            {
                LogDebug("AnalysisToolRunnerMASICBase.WaitForJobToFinish(); MASIC process has ended");
            }

            if (abortedProgram)
            {
                mErrorMessage = "Aborted MASIC processing since over " + MAX_RUNTIME_HOURS + " hours have elapsed";
                LogError("AnalysisToolRunnerMASICBase.WaitForJobToFinish(); " + mErrorMessage);
                return false;
            }

            if ((int)masicProgRunner.State == 10)
            {
                LogError("AnalysisToolRunnerMASICBase.WaitForJobToFinish(); masicProgRunner.State = 10");
                return false;
            }

            if (masicProgRunner.ExitCode == 0)
                return true;

            LogError("AnalysisToolRunnerMASICBase.WaitForJobToFinish(); masicProgRunner.ExitCode is nonzero: " + masicProgRunner.ExitCode);

            // See if a _SICs.XML file was created
            if (Directory.GetFiles(mWorkDir, "*" + SICS_XML_FILE_SUFFIX).Length > 0)
            {
                sicsXMLFileExists = true;
            }

            if (masicProgRunner.ExitCode != 32)
                return false;

            // Error code 32 is "FindSICPeaksError"
            // As long as the _SICs.xml file was created, we can safely ignore this error
            if (sicsXMLFileExists)
            {
                LogWarning(
                    "AnalysisToolRunnerMASICBase.WaitForJobToFinish(); " + SICS_XML_FILE_SUFFIX +
                    " file found, so ignoring non-zero exit code");
                return true;
            }

            LogError("AnalysisToolRunnerMASICBase.WaitForJobToFinish(); " + SICS_XML_FILE_SUFFIX + " file not found");
            return false;
        }
    }
}
