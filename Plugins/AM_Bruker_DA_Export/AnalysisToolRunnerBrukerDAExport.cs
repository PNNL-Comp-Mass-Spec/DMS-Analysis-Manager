/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 04/29/2015                                           **
**                                                              **
*****************************************************************/

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerBrukerDAExportPlugin
{
    /// <summary>
    /// Class for running DA Export
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerBrukerDAExport : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: Bruker, Bruker's, Daltonik

        // ReSharper restore CommentTypo

        private const int PROGRESS_PCT_STARTING = 5;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private const string DATA_EXPORT_CONSOLE_OUTPUT = "SpectraExport_ConsoleOutput.txt";

        private const string BRUKER_SPECTRA_EXPORT_METHOD_CONTAINER_DIR = "BrukerSpectraExportMethodDir";
        private const string BRUKER_SPECTRA_EXPORT_METHOD_PARAM = "BrukerSpectraExportMethod";

        private string mConsoleOutputErrorMsg;

        private bool mMaxRuntimeReached;

        private DateTime mLastConsoleOutputParse;
        private DateTime mLastProgressWriteTime;

        /// <summary>
        /// Exports spectra using Bruker Data Analysis
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
                    LogDebug("AnalysisToolRunnerBrukerDAExport.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;
                mLastProgressWriteTime = DateTime.UtcNow;

                // Determine the version of the Bruker DataAnalysis.exe program
                var progLoc = FindDataAnalysisProgram();

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the DataAnalysis.exe version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining Bruker DataAnalysis version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var exportScriptName = mJobParams.GetJobParameter("BrukerSpectraExportScriptFile", string.Empty);

                if (string.IsNullOrEmpty(exportScriptName))
                {
                    LogError("BrukerSpectraExportScriptFile parameter is empty");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                var scriptPath = Path.Combine(mWorkDir, exportScriptName);

                // Run the export script to create XML files of the mass spectra in the data file
                var exportSuccess = ExportSpectraUsingScript(scriptPath);

                if (exportSuccess)
                {
                    // Look for the at least one exported mass spectrum

                    var resultsFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_scan1.xml"));

                    if (resultsFile.Exists)
                    {
                        var postProcessSuccess = PostProcessExportedSpectra();

                        if (!postProcessSuccess)
                        {
                            if (string.IsNullOrEmpty(mMessage))
                            {
                                mMessage = "Unknown error post-processing the exported spectra";
                            }
                            exportSuccess = false;
                        }
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(mMessage))
                        {
                            mMessage = "No spectra were exported";
                            exportSuccess = false;
                        }
                    }
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Could use the following to create a summary file:
                // Add the current job data to the summary file
                // UpdateSummaryFile();

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                if (!exportSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in BrukerDAExportPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private int EstimateMaxRuntime(string dataFolderPath)
        {
            // Define the maximum runtime (in seconds)
            // We start with a minimum value of 10 minutes
            var maxRuntimeSeconds = 10 * 60;

            // We then estimate the number of scans using the size of the analysis.baf file as a guide
            // Rough estimates show there are each scan occupies 7 MB of disk space
            const int MB_PER_SCAN = 7;
            const int SECONDS_PER_SCAN = 20;

            double datasetSizeMB;

            var dataFolder = new DirectoryInfo(dataFolderPath);
            var bafFile = dataFolder.GetFiles("analysis.baf", SearchOption.AllDirectories).ToList();

            if (bafFile.Count > 0)
            {
                datasetSizeMB = Global.BytesToMB(bafFile[0].Length);
            }
            else
            {
                var fileTools = new PRISM.FileTools();
                RegisterEvents(fileTools);

                datasetSizeMB = Global.BytesToMB(fileTools.GetDirectorySize(dataFolderPath));
            }

            var scanCountEstimate = (int)Math.Round(datasetSizeMB / MB_PER_SCAN, 0);

            if (scanCountEstimate > 1)
            {
                maxRuntimeSeconds += scanCountEstimate * SECONDS_PER_SCAN;
            }

            // Cap the maximum runtime at 24 hours
            if (maxRuntimeSeconds > 86400)
                maxRuntimeSeconds = 86400;

            return maxRuntimeSeconds;
        }

        private bool ExportSpectraUsingScript(string scriptPath)
        {
            try
            {
                mConsoleOutputErrorMsg = string.Empty;

                var rawDataTypeName = mJobParams.GetParam("RawDataType");
                string dataFolderPath;

                switch (rawDataTypeName.ToLower())
                {
                    case AnalysisResources.RAW_DATA_TYPE_DOT_D_FOLDERS:
                    case AnalysisResources.RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER:
                    case AnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                        dataFolderPath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_D_EXTENSION);
                        break;
                    default:
                        mMessage = "Dataset type " + rawDataTypeName + " is not supported";
                        LogWarning("ExportSpectraUsingScript: " + mMessage);
                        return false;
                }

                var outputPathBase = Path.Combine(mWorkDir, mDatasetName + "_scan");

                var methodName = mJobParams.GetParam(BRUKER_SPECTRA_EXPORT_METHOD_PARAM);
                string methodOverridePath;

                // ToDo: Remove this after we get method loading working, or we switch to MSConvert
                const bool IGNORE_BRUKER_SPECTRA_EXPORT_METHOD = true;

                if (string.IsNullOrWhiteSpace(methodName) || IGNORE_BRUKER_SPECTRA_EXPORT_METHOD)
                {
                    methodOverridePath = string.Empty;
                }
                else
                {
                    // Determine the directory that contains method directories (.m directories) that we can use with the Bruker DataAnalysis program
                    var methodsDirPath = mMgrParams.GetParam(BRUKER_SPECTRA_EXPORT_METHOD_CONTAINER_DIR, @"C:\DMS_Programs\Bruker_DataAnalysis");
                    var methodsDir = new DirectoryInfo(methodsDirPath);

                    if (!methodsDir.Exists)
                    {
                        LogError(
                            "Bruker spectra export methods directory not found (see manager parameter {0}): {1}",
                            BRUKER_SPECTRA_EXPORT_METHOD_CONTAINER_DIR, methodsDirPath);

                        return false;
                    }

                    methodOverridePath = Path.Combine(methodsDir.FullName, methodName);

                    if (!Directory.Exists(methodOverridePath))
                    {
                        LogError(
                            "Bruker spectra export method directory not found (see parameter {0} in the settings file for this job): {1}",
                            BRUKER_SPECTRA_EXPORT_METHOD_PARAM, methodOverridePath);

                        return false;
                    }
                }

                LogMessage("Exporting spectra using Bruker DataAnalysis");

                // Set up and execute a program runner to run the export script

                const string progLoc = @"C:\Windows\System32\cscript.exe";

                var arguments = PossiblyQuotePath(scriptPath) + " " +
                                PossiblyQuotePath(dataFolderPath) + " " +
                                PossiblyQuotePath(outputPathBase) + " " +
                                PossiblyQuotePath(methodOverridePath);

                if (mDebugLevel >= 1)
                {
                    LogDebug(progLoc + " " + arguments.Trim());
                }

                var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = false,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(mWorkDir, DATA_EXPORT_CONSOLE_OUTPUT)
                };

                RegisterEvents(cmdRunner);

                cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;
                cmdRunner.Timeout += CmdRunner_Timeout;
                mProgress = PROGRESS_PCT_STARTING;

                var maxRuntimeSeconds = EstimateMaxRuntime(dataFolderPath);
                mMaxRuntimeReached = false;

                var success = cmdRunner.RunProgram(progLoc, arguments.Trim(), "DataExport", true, maxRuntimeSeconds);

                if (!cmdRunner.WriteConsoleOutputToFile)
                {
                    // Write the console output to a text file
                    Global.IdleLoop(0.25);

                    using var writer = new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                    writer.WriteLine(cmdRunner.CachedConsoleOutput);
                }

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputErrorMsg);
                }

                // Parse the console output file one more time to check for errors
                Global.IdleLoop(0.25);
                ParseConsoleOutputFile(cmdRunner.ConsoleOutputFilePath);

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputErrorMsg);
                    success = false;
                }

                if (!success || mMaxRuntimeReached)
                {
                    var msg = "Error exporting Bruker data using DataAnalysis.exe";

                    if (mMaxRuntimeReached)
                    {
                        msg += "; program aborted because runtime exceeded " + (maxRuntimeSeconds / 60.0).ToString("0") + " minutes";
                    }

                    if (!string.IsNullOrWhiteSpace(mConsoleOutputErrorMsg) && mConsoleOutputErrorMsg.Contains("ActiveX component"))
                    {
                        msg += "; ActiveX component error -- is Bruker DataAnalysis installed or has a license expired?";
                    }

                    LogError(msg);

                    if (cmdRunner.ExitCode != 0)
                    {
                        LogWarning("Export script returned a non-zero exit code: " + cmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Export failed (but exit code is 0)");
                    }

                    return false;
                }

                // Add some files to skip
                mJobParams.AddResultFileToSkip(cmdRunner.ConsoleOutputFilePath);
                mJobParams.AddResultFileToSkip(scriptPath);
                mJobParams.AddResultFileToSkip("JobParameters_" + mJob + ".xml");

                mProgress = PROGRESS_PCT_COMPLETE;
                mStatusTools.UpdateAndWrite(mProgress);

                if (mDebugLevel >= 3)
                {
                    LogDebug("Bruker spectrum export Complete");
                }

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Error in BrukerDAExportPlugin->ExportSpectraUsingScript";
                LogError(mMessage, ex);
                return false;
            }
        }

        /// <summary>
        /// Determine the location of Bruker's DataAnalysis.exe program
        /// </summary>
        private string FindDataAnalysisProgram()
        {
            try
            {
                var brukerDaltonikDir = new DirectoryInfo(@"C:\Program Files (x86)\Bruker Daltonik\");

                if (!brukerDaltonikDir.Exists)
                {
                    brukerDaltonikDir = new DirectoryInfo(@"C:\Program Files\Bruker Daltonik\");
                }

                if (!brukerDaltonikDir.Exists)
                {
                    LogError(@"Bruker Daltonik folder not found in C:\Program Files (x86) or C:\Program Files");
                    return string.Empty;
                }

                var matchingFiles = brukerDaltonikDir.GetFiles("DataAnalysis.exe", SearchOption.AllDirectories).ToList();

                if (matchingFiles.Count == 0)
                {
                    LogError("DataAnalysis.exe not found in the Bruker Daltonik folder at " + brukerDaltonikDir.FullName);
                    return string.Empty;
                }

                return matchingFiles[0].FullName;
            }
            catch (Exception ex)
            {
                mMessage = "Error in BrukerDAExportPlugin->FindDataAnalysisProgram";
                LogError(mMessage, ex);
                return string.Empty;
            }
        }

        private List<FileInfo> GetXMLSpectraFiles(DirectoryInfo workingDirectory)
        {
            var spectraFiles = workingDirectory.GetFiles(mDatasetName + "_scan*.xml").ToList();
            return spectraFiles;
        }

        /// <summary>
        /// Parse the Spectrum Export console output file to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // ReSharper disable CommentTypo

            // Example Console output

            // Microsoft (R) Windows Script Host Version 5.8
            // Copyright (C) Microsoft Corporation. All rights reserved.
            //
            // Output file base: C:\Data\2014_05_09_Kaplan_Far_Neg_000001_scan
            //
            // Scans to export = 1
            // Scan 1, mass range 98.278 to 1199.996
            // ... create C:\Data\2014_05_09_Kaplan_Far_Neg_000001_scan1.xml
            //
            // Scan count exported = 1

            // ReSharper restore CommentTypo

            try
            {
                var reTotalScans = new Regex(@"Scans to export = (\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var reCurrentScan = new Regex(@"Scan (\d+), mass range", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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

                // Value between 0 and 100
                var progressComplete = mProgress;
                var totalScans = 0;
                var currentScan = 0;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            continue;
                        }

                        if (dataLine.StartsWith("error occurred", StringComparison.OrdinalIgnoreCase))
                        {
                            StoreConsoleErrorMessage(reader, dataLine);
                        }

                        var reMatch = reTotalScans.Match(dataLine);

                        if (reMatch.Success)
                        {
                            int.TryParse(reMatch.Groups[1].Value, out totalScans);
                        }
                        else
                        {
                            reMatch = reCurrentScan.Match(dataLine);

                            if (reMatch.Success)
                            {
                                int.TryParse(reMatch.Groups[1].Value, out currentScan);
                            }
                        }
                    }
                }

                if (totalScans > 0)
                {
                    progressComplete = currentScan / (float)totalScans * 100;
                }

                if (mProgress < progressComplete || DateTime.UtcNow.Subtract(mLastProgressWriteTime).TotalMinutes >= 60)
                {
                    mProgress = progressComplete;

                    if (mDebugLevel >= 3 || DateTime.UtcNow.Subtract(mLastProgressWriteTime).TotalMinutes >= 20)
                    {
                        mLastProgressWriteTime = DateTime.UtcNow;
                        LogDebug(" ... " + mProgress.ToString("0") + "% complete");
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogWarning("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private void StoreConsoleErrorMessage(StreamReader reader, string firstDataLine)
        {
            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                mConsoleOutputErrorMsg = "Error exporting spectra:";
            }
            mConsoleOutputErrorMsg += " " + firstDataLine;

            while (!reader.EndOfStream)
            {
                // Store the remaining console output lines
                var dataLine = reader.ReadLine();

                if (!string.IsNullOrWhiteSpace(dataLine) && !dataLine.StartsWith("========"))
                {
                    mConsoleOutputErrorMsg += "; " + dataLine;
                }
            }
        }

        private bool PostProcessExportedSpectra()
        {
            try
            {
                var workingDirectory = new DirectoryInfo(mWorkDir);
                var spectraFiles = GetXMLSpectraFiles(workingDirectory);

                if (spectraFiles.Count == 0)
                {
                    LogError("No exported spectra files were found in PostProcessExportedSpectra");
                    return false;
                }

                var subDir = workingDirectory.CreateSubdirectory("FilesToZip");

                PRISM.AppUtils.GarbageCollectNow();

                foreach (var fileToMove in spectraFiles)
                {
                    fileToMove.MoveTo(Path.Combine(subDir.FullName, fileToMove.Name));
                }

                var success = mZipTools.ZipDirectory(subDir.FullName, Path.Combine(mWorkDir, mDatasetName + "_scans.zip"));

                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(mMessage))
                    {
                        LogError("Unknown error zipping the XML spectrum files in PostProcessExportedSpectra");
                    }

                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Error in BrukerDAExportPlugin->PostProcessExportedSpectra";
                LogError(mMessage, ex);
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var program = new FileInfo(progLoc);

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

            // Lookup the version of the DataAnalysis program
            mToolVersionUtilities.StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, program.FullName);

            // Store paths to key DLLs in toolFiles
            var toolFiles = new List<FileInfo>
            {
                program
            };

            if (program.Directory != null)
            {
                toolFiles.Add(new FileInfo(Path.Combine(program.Directory.FullName, "AnalysisCore.dll")));
            }

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
            {
                // Parse the console output file every 15 seconds
                if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
                {
                    mLastConsoleOutputParse = DateTime.UtcNow;

                    ParseConsoleOutputFile(Path.Combine(mWorkDir, DATA_EXPORT_CONSOLE_OUTPUT));
                }

                UpdateStatusFile();

                LogProgress("BrukerDAExport");
            }
        }

        private void CmdRunner_Timeout()
        {
            mMaxRuntimeReached = true;
        }
    }
}
