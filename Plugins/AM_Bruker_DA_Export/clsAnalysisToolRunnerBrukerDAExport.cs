/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 04/29/2015                                           **
**                                                              **
*****************************************************************/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerBrukerDAExportPlugin
{
    /// <summary>
    /// Class for running DA Export
    /// </summary>
    public class clsAnalysisToolRunnerBrukerDAExport : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        private const float PROGRESS_PCT_STARTING = 5;
        private const float PROGRESS_PCT_COMPLETE = 99;

        private const string DATAEXPORT_CONSOLE_OUTPUT = "SpectraExport_ConsoleOutput.txt";

        #endregion

        #region "Module Variables"

        private string mConsoleOutputErrorMsg;

        private bool mMaxRuntimeReached;

        private DateTime mLastConsoleOutputParse;
        private DateTime mLastProgressWriteTime;

        #endregion

        #region "Methods"

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

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerBrukerDAExport.RunTool(): Enter");
                }

                // Initialize classwide variables
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
                    m_message = "Error determining Bruker DataAnalysis version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var exportScriptName = m_jobParams.GetJobParameter("BrukerSpectraExportScriptFile", string.Empty);
                if (string.IsNullOrEmpty(exportScriptName))
                {
                    LogError("BrukerSpectraExportScriptFile parameter is empty");
                    return CloseOutType.CLOSEOUT_FAILED;
                }
                var scriptPath = Path.Combine(m_WorkDir, exportScriptName);

                // Run the export script to create XML files of the mass spectra in the data file
                var exportSuccess = ExportSpectraUsingScript(scriptPath);

                if (exportSuccess)
                {
                    // Look for the at least one exported mass spectrum

                    var fiResultsFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "_scan1.xml"));

                    if (fiResultsFile.Exists)
                    {
                        var postProcessSuccess = PostProcessExportedSpectra();
                        if (!postProcessSuccess)
                        {
                            if (string.IsNullOrEmpty(m_message))
                            {
                                m_message = "Unknown error post-processing the exported spectra";
                            }
                            exportSuccess = false;
                        }

                    }
                    else
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            m_message = "No spectra were exported";
                            exportSuccess = false;
                        }
                    }
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Could use the following to create a summary file:
                // Add the current job data to the summary file
                // UpdateSummaryFile();

                // Make sure objects are released
                PRISM.clsProgRunner.GarbageCollectNow();

                if (!exportSuccess)
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
                m_message = "Error in BrukerDAExportPlugin->RunTool";
                LogError(m_message, ex);
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

            var diDataFolder = new DirectoryInfo(dataFolderPath);
            var fiBafFile = diDataFolder.GetFiles("analysis.baf", SearchOption.AllDirectories).ToList();
            if (fiBafFile.Count > 0)
            {
                datasetSizeMB = fiBafFile[0].Length / 1024.0 / 1024;
            }
            else
            {
                var fileTools = new PRISM.clsFileTools();
                RegisterEvents(fileTools);

                datasetSizeMB = fileTools.GetDirectorySize(dataFolderPath) / 1024.0 / 1024;
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

                var strRawDataType = m_jobParams.GetParam("RawDataType");
                string dataFolderPath;

                switch (strRawDataType.ToLower())
                {
                    case clsAnalysisResources.RAW_DATA_TYPE_DOT_D_FOLDERS:
                    case clsAnalysisResources.RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER:
                    case clsAnalysisResources.RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                        dataFolderPath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_D_EXTENSION);
                        break;
                    default:
                        m_message = "Dataset type " + strRawDataType + " is not supported";
                        LogWarning("ExportSpectraUsingScript: " + m_message);
                        return false;
                }

                var outputPathBase = Path.Combine(m_WorkDir, m_Dataset + "_scan");

                LogMessage("Exporting spectra using Bruker DataAnalysis");

                // Set up and execute a program runner to run the export script

                const string progLoc = @"C:\Windows\System32\cscript.exe";

                var cmdStr = string.Empty;
                cmdStr += " " + PossiblyQuotePath(scriptPath);
                cmdStr += " " + PossiblyQuotePath(dataFolderPath);
                cmdStr += " " + PossiblyQuotePath(outputPathBase);

                // Could override the default method using this:
                // cmdStr += " " + PossiblyQuotePath(methodOverridePath);

                if (m_DebugLevel >= 1)
                {
                    LogDebug(progLoc + " " + cmdStr);
                }

                var cmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel)
                {
                    CreateNoWindow = true,
                    CacheStandardOutput = false,
                    EchoOutputToConsole = true,
                    WriteConsoleOutputToFile = true,
                    ConsoleOutputFilePath = Path.Combine(m_WorkDir, DATAEXPORT_CONSOLE_OUTPUT)
                };
                RegisterEvents(cmdRunner);

                cmdRunner.LoopWaiting += cmdRunner_LoopWaiting;
                cmdRunner.Timeout += cmdRunner_Timeout;
                m_progress = PROGRESS_PCT_STARTING;

                var maxRuntimeSeconds = EstimateMaxRuntime(dataFolderPath);
                mMaxRuntimeReached = false;

                var success = cmdRunner.RunProgram(progLoc, cmdStr, "DataExport", true, maxRuntimeSeconds);

                if (!cmdRunner.WriteConsoleOutputToFile)
                {
                    // Write the console output to a text file
                    Thread.Sleep(250);

                    var swConsoleOutputfile = new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));
                    swConsoleOutputfile.WriteLine(cmdRunner.CachedConsoleOutput);
                    swConsoleOutputfile.Close();
                }

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputErrorMsg);
                }

                // Parse the console output file one more time to check for errors
                Thread.Sleep(250);
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
                m_jobParams.AddResultFileToSkip(cmdRunner.ConsoleOutputFilePath);
                m_jobParams.AddResultFileToSkip(scriptPath);
                m_jobParams.AddResultFileToSkip("JobParameters_" + m_JobNum + ".xml");

                m_progress = PROGRESS_PCT_COMPLETE;
                m_StatusTools.UpdateAndWrite(m_progress);
                if (m_DebugLevel >= 3)
                {
                    LogDebug("Bruker spectrum export Complete");
                }

                return true;

            }
            catch (Exception ex)
            {
                m_message = "Error in BrukerDAExportPlugin->ExportSpectraUsingScript";
                LogError(m_message, ex);
                return false;
            }

        }

        /// <summary>
        /// Determine the location of Bruker's DataAnalysis.exe program
        /// </summary>
        /// <returns></returns>
        protected string FindDataAnalysisProgram()
        {
            try
            {
                var diBrukerDaltonik = new DirectoryInfo(@"C:\Program Files (x86)\Bruker Daltonik\");

                if (!diBrukerDaltonik.Exists)
                {
                    diBrukerDaltonik = new DirectoryInfo(@"C:\Program Files\Bruker Daltonik\");
                }

                if (!diBrukerDaltonik.Exists)
                {
                    LogError(@"Bruker Daltonik folder not found in C:\Program Files (x86) or C:\Program Files");
                    return string.Empty;
                }

                var fiFiles = diBrukerDaltonik.GetFiles("DataAnalysis.exe", SearchOption.AllDirectories).ToList();
                if (fiFiles.Count == 0)
                {
                    LogError(@"DataAnalysis.exe not found in the Bruker Daltonik folder at " + diBrukerDaltonik.FullName);
                    return string.Empty;
                }

                return fiFiles[0].FullName;

            }
            catch (Exception ex)
            {
                m_message = "Error in BrukerDAExportPlugin->FindDataAnlaysisProgram";
                LogError(m_message, ex);
                return string.Empty;
            }

        }

        private List<FileInfo> GetXMLSpectraFiles(DirectoryInfo diWorkDir)
        {
            var fiSpectraFiles = diWorkDir.GetFiles(m_Dataset + "_scan*.xml").ToList();
            return fiSpectraFiles;
        }

        /// <summary>
        /// Parse the Spectrum Export console output file to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            // Example Console output
            //
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

            try
            {
                var reTotalScans = new Regex(@"Scans to export = (\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
                var reCurrentScan = new Regex(@"Scan (\d+), mass range", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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

                // Value between 0 and 100
                var progressComplete = m_progress;
                var totalScans = 0;
                var currentScan = 0;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                        {
                            continue;
                        }

                        if (strLineIn.ToLower().StartsWith("error occurred"))
                        {
                            StoreConsoleErrorMessage(srInFile, strLineIn);
                        }

                        var reMatch = reTotalScans.Match(strLineIn);
                        if (reMatch.Success)
                        {
                            int.TryParse(reMatch.Groups[1].Value, out totalScans);
                        }
                        else
                        {
                            reMatch = reCurrentScan.Match(strLineIn);
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

                if (m_progress < progressComplete || DateTime.UtcNow.Subtract(mLastProgressWriteTime).TotalMinutes >= 60)
                {
                    m_progress = progressComplete;

                    if (m_DebugLevel >= 3 || DateTime.UtcNow.Subtract(mLastProgressWriteTime).TotalMinutes >= 20)
                    {
                        mLastProgressWriteTime = DateTime.UtcNow;
                        LogDebug(" ... " + m_progress.ToString("0") + "% complete");
                    }
                }

            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogWarning("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }

        }

        private void StoreConsoleErrorMessage(StreamReader srInFile, string strLineIn)
        {
            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                mConsoleOutputErrorMsg = "Error exporting spectra:";
            }
            mConsoleOutputErrorMsg += " " + strLineIn;

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

        private bool PostProcessExportedSpectra()
        {
            try
            {
                var diWorkDir = new DirectoryInfo(m_WorkDir);
                var fiSpectraFiles = GetXMLSpectraFiles(diWorkDir);

                if (fiSpectraFiles.Count == 0)
                {
                    LogError("No exported spectra files were found in PostProcessExportedSpectra");
                    return false;
                }

                var diSubDir = diWorkDir.CreateSubdirectory("FilesToZip");

                PRISM.clsProgRunner.GarbageCollectNow();

                foreach (var file in fiSpectraFiles)
                {
                    file.MoveTo(Path.Combine(diSubDir.FullName, file.Name));
                }

                var success = m_DotNetZipTools.ZipDirectory(diSubDir.FullName, Path.Combine(m_WorkDir, m_Dataset + "_scans.zip"));
                if (!success)
                {
                    if (string.IsNullOrWhiteSpace(m_message))
                    {
                        LogError("Unknown error zipping the XML spectrum files in PostProcessExportedSpectra");
                    }

                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                m_message = "Error in BrukerDAExportPlugin->PostProcessExportedSpectra";
                LogError(m_message, ex);
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
                LogDebug("Determining tool version info");
            }

            var fiProgram = new FileInfo(strProgLoc);
            if (!fiProgram.Exists)
            {
                try
                {
                    strToolVersionInfo = "Unknown";
                    return SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), saveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion", ex);
                    return false;
                }

            }

            // Lookup the version of the DataAnalysis program
            StoreToolVersionInfoViaSystemDiagnostics(ref strToolVersionInfo, fiProgram.FullName);

            // Store paths to key DLLs in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                fiProgram
            };

            if (fiProgram.Directory != null)
            {
                ioToolFiles.Add(new FileInfo(Path.Combine(fiProgram.Directory.FullName, "AnalysisCore.dll")));
            }

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, saveToolVersionTextFile: false);
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

            {

                // Parse the console output file every 15 seconds
                if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
                {
                    mLastConsoleOutputParse = DateTime.UtcNow;

                    ParseConsoleOutputFile(Path.Combine(m_WorkDir, DATAEXPORT_CONSOLE_OUTPUT));

                }

                UpdateStatusFile();

                LogProgress("BrukerDAExport");
            }
        }

        void cmdRunner_Timeout()
        {
            mMaxRuntimeReached = true;
        }

        #endregion

    }
}
