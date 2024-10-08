//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
//
//*********************************************************************************************************

using AnalysisManagerBase;
using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerDtaRefineryPlugIn
{
    /// <summary>
    /// Class for running DTA_Refinery analysis
    /// </summary>
    public class AnalysisToolRunnerDtaRefinery : AnalysisToolRunnerBase
    {
        // Ignore Spelling: DTA, Orbitrap

        private const int PROGRESS_PCT_DTA_REFINERY_RUNNING = 5;

        private bool mXTandemHasFinished;

        private RunDosProgram mCmdRunner;

        /// <summary>
        /// Runs DTA_Refinery tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            CloseOutType result;
            var orgDBName = mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName");
            var localOrgDBFolder = mMgrParams.GetParam("OrgDbDir");

            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (mDebugLevel > 4)
            {
                LogDebug("AnalysisToolRunnerDtaRefinery.RunTool(): Enter");
            }

            // Store the DTARefinery and X!Tandem version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                mMessage = "Error determining DTA Refinery version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Make sure the _DTA.txt file is valid
            if (!ValidateCDTAFile())
            {
                return CloseOutType.CLOSEOUT_NO_DTA_FILES;
            }

            if (mDebugLevel >= 2)
            {
                LogMessage("Running DTA_Refinery");
            }

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = false,
                EchoOutputToConsole = false,
                CacheStandardOutput = false,
                WriteConsoleOutputToFile = false
            };

            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            // verify that program file exists
            // DTARefineryLoc will be something like this: "c:\dms_programs\DTARefinery\dta_refinery.py"
            var progLoc = mMgrParams.GetParam("DTARefineryLoc");

            if (!File.Exists(progLoc))
            {
                if (progLoc.Length == 0)
                    LogError("Parameter 'DTARefineryLoc' not defined for this manager");
                else
                    LogError("Cannot find DTA_Refinery program file: " + progLoc);

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Verify that Python.exe exists
            // Python3ProgLoc will be something like this: "C:\Python3"
            var pythonProgLoc = mMgrParams.GetParam("Python3ProgLoc");

            if (!Directory.Exists(pythonProgLoc))
            {
                if (pythonProgLoc.Length == 0)
                    LogError("Parameter 'Python3ProgLoc' not defined for this manager");
                else
                    LogError("Cannot find python in directory: " + pythonProgLoc);

                return CloseOutType.CLOSEOUT_FAILED;
            }

            var pythonExe = new FileInfo(Path.Combine(pythonProgLoc, "python.exe"));

            if (!pythonExe.Exists)
            {
                LogError("Python executable not found at: " + pythonExe.FullName);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var arguments =
                Path.Combine(mWorkDir, mJobParams.GetParam("DTARefineryXMLFile")) + " " +
                Path.Combine(mWorkDir, mDatasetName + "_dta.txt") + " " +
                Path.Combine(localOrgDBFolder, orgDBName);

            // Create a batch file to run the command
            // Capture the console output (including output to the error stream) via redirection symbols:
            //    exePath arguments > ConsoleOutputFile.txt 2>&1

            var batchFilePath = Path.Combine(mWorkDir, "Run_DTARefinery.bat");
            const string consoleOutputFileName = "DTARefinery_Console_Output.txt";
            mJobParams.AddResultFileToSkip(Path.GetFileName(batchFilePath));

            var batchFileCmdLine = pythonExe + " " + progLoc + " " + arguments + " > " + consoleOutputFileName + " 2>&1";

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

            mProgress = PROGRESS_PCT_DTA_REFINERY_RUNNING;
            ResetProgRunnerCpuUsage();
            mXTandemHasFinished = false;

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var processingSuccess = mCmdRunner.RunProgram(batchFilePath, string.Empty, "DTARefinery", true);

            if (!processingSuccess)
            {
                Global.IdleLoop(0.5);

                // Open DTARefinery_Console_Output.txt and look for the last line with the text "error"
                var consoleOutputFile = new FileInfo(Path.Combine(mWorkDir, consoleOutputFileName));
                var consoleOutputErrorMessage = string.Empty;

                if (consoleOutputFile.Exists)
                {
                    using var consoleOutputReader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                    while (!consoleOutputReader.EndOfStream)
                    {
                        var dataLine = consoleOutputReader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            continue;
                        }

                        if (dataLine.IndexOf("error", StringComparison.InvariantCultureIgnoreCase) >= 0)
                        {
                            consoleOutputErrorMessage = dataLine;
                        }
                    }
                }

                mMessage = string.Empty;

                LogError(string.Format(
                    "Error running DTARefinery{0}",
                    string.IsNullOrWhiteSpace(consoleOutputErrorMessage)
                        ? string.Empty
                        : ": " + string.IsNullOrWhiteSpace(consoleOutputErrorMessage)));

                ValidateDTARefineryLogFile();

                // Move the source files and any results to the Failed Job folder
                // Useful for debugging DTA_Refinery problems
                CopyFailedResultsToArchiveDirectory();

                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Stop the job timer
            mStopTime = DateTime.UtcNow;

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Make sure objects are released
            Global.IdleLoop(0.5);
            AppUtils.GarbageCollectNow();

            if (!ValidateDTARefineryLogFile())
            {
                result = CloseOutType.CLOSEOUT_NO_DATA;
            }
            else
            {
                var massErrorExtractor = new DtaRefLogMassErrorExtractor(mMgrParams, mWorkDir, mDebugLevel, postResultsToDB: true);
                RegisterEvents(massErrorExtractor);

                var datasetID = mJobParams.GetJobParameter("DatasetID", 0);

                var massErrorsExtracted = massErrorExtractor.ParseDTARefineryLogFile(mDatasetName, datasetID, mJob);

                if (!massErrorsExtracted)
                {
                    mMessage = "Error parsing DTA refinery log file to extract mass error stats";
                    LogErrorToDatabase(mMessage + ", job " + mJob);
                }

                // Zip the output file
                result = ZipMainOutputFile();
            }

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Move the source files and any results to the Failed Job folder
                // Useful for debugging DTA_Refinery problems
                CopyFailedResultsToArchiveDirectory();
                return result;
            }

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileToSkip(Dataset + "_dta.zip");
            mJobParams.AddResultFileToSkip(Dataset + "_dta.txt");

            base.CopyFailedResultsToArchiveDirectory();
        }

        /// <summary>
        /// Parses the _DTARefineryLog.txt file to check for a message regarding X!Tandem being finished
        /// </summary>
        /// <returns>True if finished, false if not</returns>
        private bool IsXTandemFinished()
        {
            try
            {
                var sourceFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_dta_DtaRefineryLog.txt"));

                if (!sourceFile.Exists)
                {
                    LogDebug("DTA_Refinery log file not found by IsXTandemFinished: " + sourceFile.Name, 10);
                    return false;
                }

                var tmpFilePath = sourceFile.FullName + ".tmp";
                sourceFile.CopyTo(tmpFilePath, true);
                mJobParams.AddResultFileToSkip(tmpFilePath);
                Global.IdleLoop(0.1);

                using var reader = new StreamReader(new FileStream(tmpFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (dataLine != null && dataLine.Contains("finished x!tandem"))
                    {
                        LogMessage("X!Tandem has finished searching and now DTA_Refinery is running (parsed " + sourceFile.Name + ")");
                        return true;
                    }
                }

                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception in IsXTandemFinished: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo>();
            var dtaRefineryFileInfo = new FileInfo(mMgrParams.GetParam("DTARefineryLoc"));

            if (dtaRefineryFileInfo.Exists)
            {
                toolFiles.Add(dtaRefineryFileInfo);

                if (dtaRefineryFileInfo.DirectoryName == null)
                {
                    LogError("Unable to determine the parent directory of " + dtaRefineryFileInfo.FullName);
                }
                else
                {
                    var xTandemModuleLoc = Path.Combine(dtaRefineryFileInfo.DirectoryName, @"aux_xtandem_module\tandem_5digit_precision.exe");
                    toolFiles.Add(new FileInfo(xTandemModuleLoc));
                }
            }
            else
            {
                LogError("DTARefinery not found: " + dtaRefineryFileInfo.FullName);
                return false;
            }

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message, ex);
                return false;
            }
        }

        /// <summary>
        /// Parses the _DTARefineryLog.txt file to look for errors
        /// </summary>
        /// <returns>True if no errors, false if a problem</returns>
        private bool ValidateDTARefineryLogFile()
        {
            try
            {
                var sourceFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_dta_DtaRefineryLog.txt"));

                if (!sourceFile.Exists)
                {
                    mMessage = string.Empty;
                    LogError("DtaRefinery Log file not found (" + sourceFile.Name + ")");
                    return false;
                }

                using var reader = new StreamReader(new FileStream(sourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (dataLine == null || !dataLine.StartsWith("number of spectra identified less than 2", StringComparison.InvariantCultureIgnoreCase))
                        continue;

                    if (!reader.EndOfStream)
                    {
                        dataLine = reader.ReadLine();

                        if (dataLine != null && dataLine.StartsWith("stop processing", StringComparison.InvariantCultureIgnoreCase))
                        {
                            mMessage = string.Empty;
                            LogError("X!Tandem identified fewer than 2 peptides; unable to use DTARefinery with this dataset");
                            return false;
                        }
                    }

                    LogWarning("Encountered message 'number of spectra identified less than 2' but did not find 'stop processing' on the next line; " +
                               "DTARefinery likely did not complete properly");
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateDTARefineryLogFile: " + ex.Message, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Zips concatenated XML output file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType ZipMainOutputFile()
        {
            // Do we want to zip these output files?  Yes, we keep them all
            // * _dta_DtaRefineryLog.txt
            // * _dta_SETTINGS.xml
            // * _FIXED_dta.txt
            // * _HIST.png
            // * _HIST.txt
            // * scan number: _scanNum.png
            // * m/z: _mz.png
            // * log10 of ion intensity in the ICR/Orbitrap cell: _logTrappedIonInt.png
            // * total ion current in the ICR/Orbitrap cell: _trappedIonsTIC.png

            // Delete the original DTA files
            var targetFileName = "??";

            try
            {
                var workDirectory = new DirectoryInfo(mWorkDir);
                var dtaFiles = workDirectory.GetFiles("*_dta.*");

                foreach (var dtaFile in dtaFiles)
                {
                    if (!dtaFile.Name.EndsWith("_FIXED_dta.txt", StringComparison.InvariantCultureIgnoreCase))
                    {
                        targetFileName = dtaFile.Name;
                        dtaFile.Attributes &= ~FileAttributes.ReadOnly;
                        dtaFile.Delete();
                    }
                }
            }
            catch (Exception ex)
            {
                LogError(string.Format("Error deleting file {0}: {1}", targetFileName, ex.Message), ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            try
            {
                var fixedDTAFilePath = Path.Combine(mWorkDir, mDatasetName + "_FIXED_dta.txt");
                var fixedDtaFile = new FileInfo(fixedDTAFilePath);

                if (!fixedDtaFile.Exists)
                {
                    LogError("DTARefinery output file not found: " + fixedDtaFile.Name);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                fixedDtaFile.MoveTo(Path.Combine(mWorkDir, mDatasetName + "_dta.txt"));

                try
                {
                    if (!ZipFile(fixedDtaFile.FullName, true))
                    {
                        LogError("Error zipping DTARefinery output file: " + fixedDtaFile.FullName);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                catch (Exception ex)
                {
                    LogError("Error zipping DTARefinery output file: " + ex.Message, ex);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                LogError("Error renaming DTARefinery output file: " + ex.Message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private DateTime mLastCpuUsageCheck = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            const string DTA_REFINERY_PROCESS_NAME = "dta_refinery";
            const string XTANDEM_PROCESS_NAME = "tandem_5digit_precision";
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            // Push a new core usage value into the queue every 30 seconds
            if (DateTime.UtcNow.Subtract(mLastCpuUsageCheck).TotalSeconds >= SECONDS_BETWEEN_UPDATE)
            {
                mLastCpuUsageCheck = DateTime.UtcNow;

                if (!mXTandemHasFinished)
                {
                    mXTandemHasFinished = IsXTandemFinished();
                }

                if (mXTandemHasFinished)
                {
                    // Determine the CPU usage of DTA_Refinery
                    UpdateCpuUsageByProcessName(DTA_REFINERY_PROCESS_NAME, SECONDS_BETWEEN_UPDATE, mCmdRunner.ProcessID);
                }
                else
                {
                    // Determine the CPU usage of X!Tandem
                    UpdateCpuUsageByProcessName(XTANDEM_PROCESS_NAME, SECONDS_BETWEEN_UPDATE, mCmdRunner.ProcessID);
                }

                LogProgress("DtaRefinery");
            }
        }
    }
}
