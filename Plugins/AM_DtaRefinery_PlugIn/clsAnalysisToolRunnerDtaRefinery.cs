//*********************************************************************************************************
// Written by Matt Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerDtaRefineryPlugIn
{
    /// <summary>
    /// Class for running DTA_Refinery analysis
    /// </summary>
    public class clsAnalysisToolRunnerDtaRefinery : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

        private const float PROGRESS_PCT_DTA_REFINERY_RUNNING = 5;

        private bool mXTandemHasFinished;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs DTA_Refinery tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            CloseOutType result;
            var orgDBName = mJobParams.GetParam("PeptideSearch", "generatedFastaName");
            var localOrgDBFolder = mMgrParams.GetParam("OrgDbDir");

            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (mDebugLevel > 4)
            {
                LogDebug("clsAnalysisToolRunnerDtaRefinery.RunTool(): Enter");
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

            mCmdRunner = new clsRunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = false,
                EchoOutputToConsole = false,
                CacheStandardOutput = false,
                WriteConsoleOutputToFile = false
            };

            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            // verify that program file exists
            // DTARefineryLoc will be something like this: "c:\dms_programs\DTARefinery\dta_refinery.exe"
            var progLoc = mMgrParams.GetParam("DTARefineryLoc");
            if (!File.Exists(progLoc))
            {
                if (progLoc.Length == 0)
                    progLoc = "Parameter 'DTARefineryLoc' not defined for this manager";
                LogError("Cannot find DTA_Refinery program file: " + progLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var arguments =
                Path.Combine(mWorkDir, mJobParams.GetParam("DTARefineryXMLFile")) + " " +
                Path.Combine(mWorkDir, mDatasetName + "_dta.txt") + " " +
                Path.Combine(localOrgDBFolder, orgDBName);

            // Create a batch file to run the command
            // Capture the console output (including output to the error stream) via redirection symbols:
            //    exePath arguments > ConsoleOutputFile.txt 2>&1

            var strBatchFilePath = Path.Combine(mWorkDir, "Run_DTARefinery.bat");
            var strConsoleOutputFileName = "DTARefinery_Console_Output.txt";
            mJobParams.AddResultFileToSkip(Path.GetFileName(strBatchFilePath));

            var strBatchFileCmdLine = progLoc + " " + arguments + " > " + strConsoleOutputFileName + " 2>&1";

            // Create the batch file
            using (var writer = new StreamWriter(new FileStream(strBatchFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                if (mDebugLevel >= 1)
                {
                    LogDebug(strBatchFileCmdLine);
                }

                writer.WriteLine(strBatchFileCmdLine);
            }

            mProgress = PROGRESS_PCT_DTA_REFINERY_RUNNING;
            ResetProgRunnerCpuUsage();
            mXTandemHasFinished = false;

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var processingSuccess = mCmdRunner.RunProgram(strBatchFilePath, string.Empty, "DTARefinery", true);

            if (!processingSuccess)
            {
                clsGlobal.IdleLoop(0.5);

                // Open DTARefinery_Console_Output.txt and look for the last line with the text "error"
                var fiConsoleOutputFile = new FileInfo(Path.Combine(mWorkDir, strConsoleOutputFileName));
                var consoleOutputErrorMessage = string.Empty;

                if (fiConsoleOutputFile.Exists)
                {
                    using (var consoleOutputReader = new StreamReader(new FileStream(fiConsoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        while (!consoleOutputReader.EndOfStream)
                        {
                            var dataLine = consoleOutputReader.ReadLine();
                            if (string.IsNullOrWhiteSpace(dataLine))
                            {
                                continue;
                            }

                            if (dataLine.IndexOf("error", StringComparison.InvariantCultureIgnoreCase) >= 0)
                            {
                                consoleOutputErrorMessage = string.Copy(dataLine);
                            }
                        }
                    }
                }

                var msg = "Error running DTARefinery";
                if (!string.IsNullOrWhiteSpace(consoleOutputErrorMessage))
                {
                    msg += ": " + consoleOutputErrorMessage;
                }

                mMessage = string.Empty;
                LogError(msg);

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
            clsGlobal.IdleLoop(0.5);
            ProgRunner.GarbageCollectNow();

            if (!ValidateDTARefineryLogFile())
            {
                result = CloseOutType.CLOSEOUT_NO_DATA;
            }
            else
            {
                var oMassErrorExtractor = new clsDtaRefLogMassErrorExtractor(mMgrParams, mWorkDir, mDebugLevel, blnPostResultsToDB: true);
                RegisterEvents(oMassErrorExtractor);

                var intDatasetID = mJobParams.GetJobParameter("DatasetID", 0);

                var blnSuccess = oMassErrorExtractor.ParseDTARefineryLogFile(mDatasetName, intDatasetID, mJob);

                if (!blnSuccess)
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
        /// <remarks></remarks>
        private bool IsXTandemFinished()
        {
            try
            {
                var fiSourceFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_dta_DtaRefineryLog.txt"));
                if (!fiSourceFile.Exists)
                {
                    LogDebug("DTA_Refinery log file not found by IsXTandemFinished: " + fiSourceFile.Name, 10);
                    return false;
                }

                var tmpFilePath = fiSourceFile.FullName + ".tmp";
                fiSourceFile.CopyTo(tmpFilePath, true);
                mJobParams.AddResultFileToSkip(tmpFilePath);
                clsGlobal.IdleLoop(0.1);

                using (var reader = new StreamReader(new FileStream(tmpFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (dataLine != null && dataLine.Contains("finished x!tandem"))
                        {
                            LogMessage("X!Tandem has finished searching and now DTA_Refinery is running (parsed " + fiSourceFile.Name + ")");
                            return true;
                        }
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
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            var strToolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo>();
            var ioDtaRefineryFileInfo = new FileInfo(mMgrParams.GetParam("DTARefineryLoc"));

            if (ioDtaRefineryFileInfo.Exists)
            {
                toolFiles.Add(ioDtaRefineryFileInfo);

                if (ioDtaRefineryFileInfo.DirectoryName == null)
                {
                    LogError("Unable to determine the parent directory of " + ioDtaRefineryFileInfo.FullName);
                }
                else
                {
                    var strXTandemModuleLoc = Path.Combine(ioDtaRefineryFileInfo.DirectoryName, @"aux_xtandem_module\tandem_5digit_precision.exe");
                    toolFiles.Add(new FileInfo(strXTandemModuleLoc));
                }
            }
            else
            {
                LogError("DTARefinery not found: " + ioDtaRefineryFileInfo.FullName);
                return false;
            }

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles, saveToolVersionTextFile: false);
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
        /// <remarks></remarks>
        private bool ValidateDTARefineryLogFile()
        {
            try
            {
                var fiSourceFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_dta_DtaRefineryLog.txt"));
                if (!fiSourceFile.Exists)
                {
                    mMessage = string.Empty;
                    LogError("DtaRefinery Log file not found (" + fiSourceFile.Name + ")");
                    return false;
                }

                using (var reader = new StreamReader(new FileStream(fiSourceFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
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
        /// <remarks></remarks>
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
            var targetFile = "??";

            try
            {
                var ioWorkDirectory = new DirectoryInfo(mWorkDir);
                var ioFiles = ioWorkDirectory.GetFiles("*_dta.*");

                foreach (var ioFile in ioFiles)
                {
                    if (!ioFile.Name.EndsWith("_FIXED_dta.txt", StringComparison.InvariantCultureIgnoreCase))
                    {
                        targetFile = ioFile.Name;
                        ioFile.Attributes = ioFile.Attributes & ~FileAttributes.ReadOnly;
                        ioFile.Delete();
                    }
                }
            }
            catch (Exception ex)
            {
                LogError(string.Format("Error deleting file {0}: {1}", targetFile, ex.Message), ex);
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
        /// <remarks></remarks>
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

        #endregion
    }
}
