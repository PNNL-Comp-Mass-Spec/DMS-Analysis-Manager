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
using System.Xml;
using PRISMDatabaseUtils;

namespace AnalysisManagerMasicPlugin
{
    /// <summary>
    /// Class for performing MASIC analysis
    /// </summary>
    public abstract class clsAnalysisToolRunnerMASICBase : clsAnalysisToolRunnerBase
    {

        #region "Module variables"

        private const string STORE_REPORTER_ION_OBS_STATS_SP_NAME = "StoreReporterIonObsStats";
        private const string SICS_XML_FILE_SUFFIX = "_SICs.xml";

        // Job running status variable
        private bool mJobRunning;

        protected string mErrorMessage = string.Empty;

        private string mProcessStep = string.Empty;
        private string mMASICStatusFileName = string.Empty;

        private string mReporterIonName = string.Empty;
        private int mReporterIonObservationRateTopNPct;

        #endregion

        #region "Methods"

        private void ExtractErrorsFromMASICLogFile(FileSystemInfo logFile)
        {
            // Read the most recent MASIC_Log file and look for any lines with the text "Error"

            try
            {
                if (!logFile.Exists)
                    return;

                using (var reader = new StreamReader(new FileStream(logFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    var errorCount = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                            continue;

                        if (dataLine.ToLower().Contains("error"))
                        {
                            if (errorCount == 0)
                            {
                                LogError("Errors found in the MASIC Log File");
                            }

                            if (errorCount <= 10)
                                LogWarning(" ... " + dataLine);

                            errorCount += 1;
                        }
                    }

                    if (errorCount > 10)
                    {
                        LogWarning(string.Format(" ... {0} total errors", errorCount));
                    }
                }

            }
            catch (Exception ex)
            {
                LogError("Error reading MASIC Log File at '" + logFile.FullName + "'; " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Primary entry point for running this tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
        public override CloseOutType RunTool()
        {
            // Call base class for initial setup
            base.RunTool();

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

            // Make the SIC's
            LogMessage("Calling MASIC to create the SIC files, job " + mJob);
            try
            {
                // Note that RunMASIC will populate the File Path variables, then will call
                //  StartMASICAndWait() and WaitForJobToFinish(), which are in this class
                var eProcessingResult = RunMASIC();
                if (eProcessingResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return eProcessingResult;
                }
            }
            catch (Exception ex)
            {
                LogError("clsAnalysisToolRunnerMASICBase.RunTool(), Exception calling MASIC to create the SIC files, " + ex.Message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            mProgress = 100;
            UpdateStatusFile();

            // Run the cleanup routine from the base class
            var postProcessingResult = PerfPostAnalysisTasks();
            if (postProcessingResult != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make the results folder
            if (mDebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerMASICBase.RunTool(), Making results folder");
            }

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

        }

        protected CloseOutType StartMASICAndWait(string inputFilePath, string outputFolderPath, string parameterFilePath)
        {
            // Note that this function is normally called by RunMasic() in the subclass

            var masicExePath = string.Empty;

            mErrorMessage = string.Empty;
            mProcessStep = "NewTask";

            try
            {
                mMASICStatusFileName = "MasicStatus_" + mMgrName + ".xml";
            }
            catch (Exception)
            {
                mMASICStatusFileName = "MasicStatus.xml";
            }

            // Make sure the MASIC executable file exists (MASIC_Console.exe)
            try
            {
                masicExePath = mMgrParams.GetParam("MasicProgLoc");
                if (!File.Exists(masicExePath))
                {
                    LogError("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); MASIC not found at: " + masicExePath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                LogError("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Error looking for MASIC_Console.exe at " + masicExePath, ex);
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
            clsGlobal.IdleLoop(3);

            // Read the most recent MASIC_Log file and look for any lines with the text "Error"
            ExtractErrorsFromMASICLogFile(logFile);

            // Verify MASIC exited due to job completion
            if (success)
            {
                mJobParams.AddResultFileToSkip(logFile.Name);
            }
            else
            {
                if (mDebugLevel > 1)
                {
                    LogError("WaitForJobToFinish returned False");
                }

                if (!string.IsNullOrEmpty(mErrorMessage))
                {
                    LogError("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Masic Error message: " + mErrorMessage);
                    if (string.IsNullOrEmpty(mMessage))
                        mMessage = mErrorMessage;
                }
                else
                {
                    LogError("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); Masic Error message is blank");
                    if (string.IsNullOrEmpty(mMessage))
                        mMessage = "Unknown error running MASIC";
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (mDebugLevel > 0)
            {
                LogDebug("clsAnalysisToolRunnerMASICBase.StartMASICAndWait(); mProcessStep=" + mProcessStep);
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

                using (var fsInFile = new FileStream(statusFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    using (var reader = new XmlTextReader(fsInFile))
                    {
                        reader.WhitespaceHandling = WhitespaceHandling.None;

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

            // Zip the _SICs.XML file (if it exists; it won't if SkipSICProcessing = True in the parameter file)
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

        private bool ReadReporterIonObservationRateFile(
            FileSystemInfo observationRateFile,
            out List<double> observationStatsAll,
            out List<double> observationStatsTopNPct)
        {
            observationStatsAll = new List<double>();
            observationStatsTopNPct = new List<double>();

            try
            {
                LogDebug("Reading " + observationRateFile.FullName);

                using (var reader = new StreamReader(new FileStream(observationRateFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    if (reader.EndOfStream)
                    {
                        LogError("Reporter ion observation rate file is empty: " + observationRateFile.FullName);
                        return false;
                    }

                    // Skip the header line
                    reader.ReadLine();

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

                        if (lineParts.Length < 3)
                        {
                            LogError(string.Format(
                                "Channel {0} in the reporter ion observation rate file has fewer than three columns; corrupt file: {1}",
                                channel, observationRateFile.FullName));
                            return false;
                        }

                        if (!double.TryParse(lineParts[1], out var observationRateAll))
                        {
                            LogError(string.Format(
                                "Channel {0} in the reporter ion observation rate file has a non-numeric Observation_Rate value: {1}",
                                channel, lineParts[1]));
                            return false;
                        }

                        if (!double.TryParse(lineParts[2], out var observationRateTopNPct))
                        {
                            LogError(string.Format(
                                "Channel {0} in the reporter ion observation rate file has a non-numeric Observation_Rate_Top80Pct value: {1}",
                                channel, lineParts[2]));
                            return false;
                        }

                        observationStatsAll.Add(observationRateAll);
                        observationStatsTopNPct.Add(observationRateTopNPct);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error reading the _RepIonObsRate.txt file", ex);
                return false;
            }
        }

        private bool StoreReporterIonObservationRateStats()
        {
            const int MAX_RETRY_COUNT = 3;

            try
            {
                var observationRateFile = new FileInfo(Path.Combine(mWorkDir, Dataset + "_RepIonObsRate.txt"));
                if (!observationRateFile.Exists)
                    return true;

                var dataLoaded = ReadReporterIonObservationRateFile(
                    observationRateFile,
                    out var observationStatsAll,
                    out var observationStatsTopNPct);

                if (!dataLoaded)
                    return false;

                var analysisTask = new clsAnalysisJob(mMgrParams, mDebugLevel);
                var dbTools = analysisTask.DMSProcedureExecutor;

                // Call stored procedure StoreDTARefMassErrorStats in DMS5
                // Data is stored in table T_Dataset_QC
                var sqlCmd = dbTools.CreateCommand(STORE_REPORTER_ION_OBS_STATS_SP_NAME, CommandType.StoredProcedure);

                // ReSharper disable once CommentTypo
                // Note that reporterIonName must match a Label in T_Sample_Labelling_Reporter_Ions
                if (string.IsNullOrWhiteSpace(mReporterIonName))
                {
                    var parameterFileName = mJobParams.GetParam("parmFileName");
                    if (string.IsNullOrWhiteSpace(parameterFileName))
                    {
                        LogError(string.Format(
                            "The parameter file name is empty for job {0}; " +
                            "cannot store reporter ion observation stats in the database",
                            mJob));

                        return false;
                    }

                    LogError(string.Format(
                        "Reporter ion name is empty for job {0}; " +
                        "cannot store reporter ion observation stats in the database",
                        mJob));

                    return false;
                }

                dbTools.AddParameter(sqlCmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);
                dbTools.AddTypedParameter(sqlCmd, "@job", SqlType.Int, value: mJob);
                dbTools.AddParameter(sqlCmd, "@reporterIon", SqlType.VarChar, 64).Value = mReporterIonName;
                dbTools.AddParameter(sqlCmd, "@topNPct", SqlType.Int).Value = mReporterIonObservationRateTopNPct;
                dbTools.AddParameter(sqlCmd, "@observationStatsAll", SqlType.VarChar, 4000).Value = string.Join(",", observationStatsAll);
                dbTools.AddParameter(sqlCmd, "@observationStatsTopNPct", SqlType.VarChar, 4000).Value = string.Join(",", observationStatsTopNPct);
                dbTools.AddTypedParameter(sqlCmd, "@message", SqlType.VarChar, 255, ParameterDirection.InputOutput);
                dbTools.AddTypedParameter(sqlCmd, "@infoOnly", SqlType.TinyInt, value: 0);

                // Execute the SP (retry the call up to 3 times)
                var resCode = dbTools.ExecuteSP(sqlCmd, MAX_RETRY_COUNT);

                if (resCode == 0)
                {
                    return true;
                }

                LogError(string.Format(
                    "Error storing reporter ion observation stats in the database, {0} returned {1}",
                    STORE_REPORTER_ION_OBS_STATS_SP_NAME, resCode));

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
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            var masicExecutablePath = mMgrParams.GetParam("MasicProgLoc");
            var success = StoreDotNETToolVersionInfo(masicExecutablePath);

            return success;
        }

        /// <summary>
        /// Validate that required options are defined in the MASIC parameter file
        /// </summary>
        /// <param name="parameterFilePath"></param>
        /// <remarks>Also reads ReporterIonMassMode and ReporterIonObservationRateTopNPct</remarks>
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

                switch (reporterIonMassMode)
                {
                   case 1:
                        // ITraqFourMZ
                        mReporterIonName = "iTRAQ";
                        break;

                    case 3:
                        // TMTTwoMZ
                        mReporterIonName = "TMT2";
                        break;

                    case 4:
                        // TMTSixMZ
                        mReporterIonName = "TMT6";
                        break;

                    case 5:
                        // ITraqEightMZHighRes
                        mReporterIonName = "iTRAQ8";
                        break;

                    case 6:
                        // ITraqEightMZLowRes
                        mReporterIonName = "iTRAQ8";
                        break;

                    case 10:
                        // TMTTenMZ
                        mReporterIonName = "TMT10";
                        break;

                    case 16:
                        // TMTElevenMZ
                        mReporterIonName = "TMT11";
                        break;

                    case 18:
                        // TMTSixteenMZ
                        mReporterIonName = "TMT16";
                        break;

                    default:
                        mReporterIonName = "ReporterIonMassMode_" + reporterIonMassMode;
                        break;

                }
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
                clsGlobal.IdleLoop(SECONDS_BETWEEN_UPDATE);

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
                        var coreUsage = clsGlobal.ProcessInfo.GetCoreUsageByProcessID(processID);

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
                LogDebug("clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); MASIC process has ended");
            }

            if (abortedProgram)
            {
                mErrorMessage = "Aborted MASIC processing since over " + MAX_RUNTIME_HOURS + " hours have elapsed";
                LogError("clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); " + mErrorMessage);
                return false;
            }

            if ((int)masicProgRunner.State == 10)
            {
                LogError("clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); masicProgRunner.State = 10");
                return false;
            }

            if (masicProgRunner.ExitCode == 0)
                return true;

            LogError("clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); masicProgRunner.ExitCode is nonzero: " + masicProgRunner.ExitCode);

            // See if a _SICs.XML file was created
            if (Directory.GetFiles(mWorkDir, "*" + SICS_XML_FILE_SUFFIX).Length > 0)
            {
                sicsXMLFileExists = true;
            }

            if (masicProgRunner.ExitCode != 32)
                return false;

            // FindSICPeaksError
            // As long as the _SICs.xml file was created, we can safely ignore this error
            if (sicsXMLFileExists)
            {
                LogWarning(
                    "clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); " + SICS_XML_FILE_SUFFIX +
                    " file found, so ignoring non-zero exit code");
                return true;
            }

            LogError("clsAnalysisToolRunnerMASICBase.WaitForJobToFinish(); " + SICS_XML_FILE_SUFFIX + " file not found");
            return false;

            // Return False for any other exit codes
        }

        #endregion
    }
}
