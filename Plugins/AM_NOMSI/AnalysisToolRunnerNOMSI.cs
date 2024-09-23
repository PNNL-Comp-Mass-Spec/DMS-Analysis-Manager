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

namespace AnalysisManagerNOMSIPlugin
{
    /// <summary>
    /// Class for running NOMSI
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerNOMSI : AnalysisToolRunnerBase
    {
        private const int PROGRESS_PCT_STARTING = 5;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private const string NOMSI_CONSOLE_OUTPUT_BASE = "NOMSI_ConsoleOutput_scan";
        private const string COMPRESSED_NOMSI_RESULTS_BASE = "NOMSI_Results_scan";

        private string mCurrentConsoleOutputFile;
        private string mConsoleOutputErrorMsg;
        private bool mNoPeaksFound;

        private DateTime mLastConsoleOutputParse;

        private int mTotalSpectra;

        /// <summary>
        /// Processes data using NOMSI
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
                    LogDebug("AnalysisToolRunnerNOMSI.RunTool(): Enter");
                }

                // Initialize class wide variables
                mLastConsoleOutputParse = DateTime.UtcNow;

                mTotalSpectra = 0;

                // Determine the path to NOMSI
                var progLoc = DetermineProgramLocation("NOMSIProgLoc", "NOMSI.exe");

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the NOMSI.exe version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining NOMSI version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Unzip the XML files
                var compressedXMLFiles = Path.Combine(mWorkDir, mDatasetName + "_scans.zip");
                var unzipSuccess = UnzipFile(compressedXMLFiles, mWorkDir);

                if (!unzipSuccess)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Unknown error extracting the XML spectra files";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Process the XML files using NOMSI
                var processingSuccess = ProcessScansWithNOMSI(progLoc, out var noPeaksFound);

                var eReturnCode = CloseOutType.CLOSEOUT_SUCCESS;

                if (noPeaksFound)
                {
                    eReturnCode = CloseOutType.CLOSEOUT_NO_DATA;
                }
                else if (!processingSuccess)
                {
                    eReturnCode = CloseOutType.CLOSEOUT_FAILED;
                }
                else
                {
                    // Look for the result files

                    var workingDirectory = new DirectoryInfo(mWorkDir);
                    var resultsFiles = workingDirectory.GetFiles("Distributions.zip").ToList();

                    if (resultsFiles.Count == 0)
                    {
                        resultsFiles = workingDirectory.GetFiles(COMPRESSED_NOMSI_RESULTS_BASE + "*.zip").ToList();
                    }

                    if (resultsFiles.Count == 0)
                    {
                        if (string.IsNullOrEmpty(mMessage))
                        {
                            mMessage = "NOMSI results not found";
                            processingSuccess = false;
                            eReturnCode = CloseOutType.CLOSEOUT_FAILED;
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

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, move the output files into the results directory,
                    // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // No need to keep the JobParameters file
                mJobParams.AddResultFileToSkip("JobParameters_" + mJob + ".xml");

                var success = CopyResultsToTransferDirectory();

                return success ? eReturnCode : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in NOMSIPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            try
            {
                var workingDirectory = new DirectoryInfo(mWorkDir);
                var spectraFiles = GetXMLSpectraFiles(workingDirectory);

                foreach (var file in spectraFiles)
                {
                    file.Delete();
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            base.CopyFailedResultsToArchiveDirectory();
        }

        private string GetUsername()
        {
            var currentUser = System.Security.Principal.WindowsIdentity.GetCurrent();
            var userName = currentUser.Name;
            var lastSlashIndex = userName.LastIndexOf('\\');

            if (lastSlashIndex >= 0)
                return userName.Substring(lastSlashIndex + 1);

            return userName;
        }

        private List<FileInfo> GetXMLSpectraFiles(DirectoryInfo workingDirectory)
        {
            return workingDirectory.GetFiles(mDatasetName + "_scan*.xml").ToList();
        }

        private int MoveWorkDirFiles(FileSystemInfo zipWork, string fileMask)
        {
            var workingDirectory = new DirectoryInfo(mWorkDir);
            var filesToMove = workingDirectory.GetFiles(fileMask).ToList();

            foreach (var sourceFile in filesToMove)
            {
                sourceFile.MoveTo(Path.Combine(zipWork.FullName, sourceFile.Name));
            }

            return filesToMove.Count;
        }

        /// <summary>
        /// Parse the NOMSI console output file to track the search progress
        /// </summary>
        /// <remarks>Not used at present</remarks>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example Console output

            // start=5/4/2015 2:16:45 PM
            // dataset_path=E:\DMS_WorkDir\2015_04_05_ESIL_Pos_ESIL_HighMass_TOF1p4_000001_scan1.xml
            // param_file_path=E:\DMS_WorkDir\NOMSI_DI_Diagnostics_Targets_Pairs_2015-04-30.param
            // 	err	[Only one peak found; cannot run diagnostics]
            // diagnostics=failure
            // summary=success
            // end=5/4/2015 2:16:45 PM

            try
            {
                var reErrorMessage = new Regex(@"err\t\[(.+)\]", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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

                    if (dataLine.StartsWith("error ", StringComparison.OrdinalIgnoreCase))
                    {
                        StoreConsoleErrorMessage(reader, dataLine);
                    }

                    var reMatch = reErrorMessage.Match(dataLine);

                    if (reMatch.Success)
                    {
                        var errorMessage = reMatch.Groups[1].Value;

                        if (errorMessage.Contains("No peaks found") ||
                            errorMessage.Contains("Only one peak found"))
                        {
                            mNoPeaksFound = true;
                        }
                        else
                        {
                            mConsoleOutputErrorMsg = Global.AppendToComment(mConsoleOutputErrorMsg, errorMessage);
                        }
                    }
                    else if (dataLine.Contains("No peaks found") ||
                             dataLine.Contains("Only one peak found"))
                    {
                        mNoPeaksFound = true;
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

        private void PostProcessResultsOneScan(DirectoryInfo zipWork, int scanCount, int scanNumber)
        {
            zipWork.Refresh();

            if (zipWork.Exists)
            {
                foreach (var fileToRemove in zipWork.GetFiles("*").ToList())
                {
                    fileToRemove.Delete();
                }
            }
            else
            {
                zipWork.Create();
            }

            var filesMatched = 0;

            if (scanCount == 1)
            {
                // Skip the console output file and NOMSI summary file
                mJobParams.AddResultFileToSkip(mCurrentConsoleOutputFile);
                mJobParams.AddResultFileToSkip("nomsi_summary.txt");

                // Combine the distribution files into a single .zip file
                filesMatched += MoveWorkDirFiles(zipWork, "distribution*.txt");

                if (filesMatched > 0)
                    mZipTools.ZipDirectory(zipWork.FullName, Path.Combine(mWorkDir, "Distributions.zip"));
            }
            else
            {
                // Combine the distribution files, the dm_ files, and the console output file into a zip file
                // Example name: NOMSI_Results_scan1.zip

                filesMatched += MoveWorkDirFiles(zipWork, "distribution*.txt");
                filesMatched += MoveWorkDirFiles(zipWork, "dm_pairs*.txt");
                filesMatched += MoveWorkDirFiles(zipWork, "dm_stats*.txt");
                filesMatched += MoveWorkDirFiles(zipWork, "NOMSI_ConsoleOutput_scan*.txt");

                if (filesMatched > 0)
                    mZipTools.ZipDirectory(zipWork.FullName, Path.Combine(mWorkDir, COMPRESSED_NOMSI_RESULTS_BASE + scanNumber + ".zip"));
            }
        }

        private bool ProcessOneFileWithNOMSI(
            string progLoc,
            FileSystemInfo spectrumFile,
            string paramFilePath,
            int filesProcessed,
            out int scanNumber)
        {
            // Set up and execute a program runner to run NOMSI

            var reGetScanNumber = new Regex(@"scan(\d+).xml", RegexOptions.IgnoreCase | RegexOptions.Compiled);

            var arguments = PossiblyQuotePath(spectrumFile.FullName) + " " +
                            PossiblyQuotePath(paramFilePath);

            if (mDebugLevel >= 1)
            {
                LogDebug(progLoc + " " + arguments);
            }

            scanNumber = filesProcessed;

            var reMatch = reGetScanNumber.Match(spectrumFile.Name);

            if (reMatch.Success)
            {
                int.TryParse(reMatch.Groups[1].Value, out scanNumber);
            }

            mCurrentConsoleOutputFile = Path.Combine(mWorkDir, NOMSI_CONSOLE_OUTPUT_BASE + scanNumber + ".txt");

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

            var subTaskProgress = filesProcessed / (float)mTotalSpectra * 100;

            mProgress = ComputeIncrementalProgress(PROGRESS_PCT_STARTING, PROGRESS_PCT_COMPLETE, subTaskProgress);

            var success = cmdRunner.RunProgram(progLoc, arguments, "NOMSI", true);

            if (!cmdRunner.WriteConsoleOutputToFile && cmdRunner.CachedConsoleOutput.Length > 0)
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

            // Parse the nomsi_summary file to look for errors
            Global.IdleLoop(0.25);
            var logSummaryFile = new FileInfo(Path.Combine(mWorkDir, "nomsi_summary.txt"));

            if (!logSummaryFile.Exists)
            {
                // Summary file not created
                // Look for a log file in folder C:\Users\d3l243\AppData\Local\
                var alternateLogPath = Path.Combine(@"C:\Users", GetUsername(), @"AppData\Local\NOMSI.log");

                var alternateLogFile = new FileInfo(alternateLogPath);

                if (alternateLogFile.Exists)
                    alternateLogFile.CopyTo(logSummaryFile.FullName, true);
            }

            ParseConsoleOutputFile(logSummaryFile.FullName);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (success)
            {
                return true;
            }

            LogError("Error processing scan " + scanNumber + " using NOMSI");

            if (cmdRunner.ExitCode != 0)
            {
                LogWarning("NOMSI returned a non-zero exit code: " + cmdRunner.ExitCode);
            }
            else
            {
                LogWarning("NOMSI failed (but exit code is 0)");
            }

            return false;
        }

        private bool ProcessScansWithNOMSI(string progLoc, out bool noPeaksFound)
        {
            noPeaksFound = false;

            try
            {
                mConsoleOutputErrorMsg = string.Empty;

                LogMessage("Processing data using NOMSI");

                var paramFilePath = Path.Combine(mWorkDir, mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE));

                if (!File.Exists(paramFilePath))
                {
                    LogError("Parameter file not found", "Parameter file not found: " + paramFilePath);
                    return false;
                }

                var targetsFileName = mJobParams.GetParam("dm_target_file");

                // Update the parameter file to use the targets file specified by the settings file
                var success = UpdateParameterFile(paramFilePath, targetsFileName);

                if (!success)
                    return false;

                var workingDirectory = new DirectoryInfo(mWorkDir);
                var spectraFiles = GetXMLSpectraFiles(workingDirectory);

                mTotalSpectra = spectraFiles.Count;

                if (mTotalSpectra == 0)
                {
                    mMessage = "XML spectrum files not found";
                    return false;
                }

                mProgress = PROGRESS_PCT_STARTING;

                var zipWork = new DirectoryInfo(Path.Combine(mWorkDir, "ScanResultsZipWork"));

                var filesProcessed = 0;
                var fileCountNoPeaks = 0;

                foreach (var spectrumFile in spectraFiles)
                {
                    mNoPeaksFound = false;

                    success = ProcessOneFileWithNOMSI(progLoc, spectrumFile, paramFilePath, filesProcessed, out var scanNumber);

                    if (!success)
                        return false;

                    mJobParams.AddResultFileToSkip(spectrumFile.Name);

                    PostProcessResultsOneScan(zipWork, spectraFiles.Count, scanNumber);

                    if (mNoPeaksFound)
                        fileCountNoPeaks++;

                    filesProcessed++;
                }

                mJobParams.AddResultFileToSkip(targetsFileName);

                mProgress = PROGRESS_PCT_COMPLETE;
                mStatusTools.UpdateAndWrite(mProgress);

                if (mDebugLevel >= 3)
                {
                    LogDebug("NOMSI processing Complete");
                }

                if (fileCountNoPeaks <= 0)
                {
                    return true;
                }

                if (filesProcessed == 1 || fileCountNoPeaks >= filesProcessed)
                {
                    // None of the scans had peaks
                    mMessage = "No peaks found";

                    if (filesProcessed > 1)
                        mEvalMessage = "None of the scans had peaks";
                    else
                        mEvalMessage = "Scan did not have peaks";

                    noPeaksFound = true;

                    // Do not put the parameter file in the results directory
                    mJobParams.AddResultFileToSkip(paramFilePath);
                }
                else
                {
                    // Some of the scans had no peaks
                    mEvalMessage = fileCountNoPeaks + " / " + filesProcessed + " scans had no peaks";
                }

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Error in NOMSIPlugin->StartNOMSI";
                LogError(mMessage, ex);
                return false;
            }
        }

        private void StoreConsoleErrorMessage(StreamReader reader, string firstDataLine)
        {
            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                mConsoleOutputErrorMsg = "Error running NOMSI:";
            }
            mConsoleOutputErrorMsg += "; " + firstDataLine;

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

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var success = StoreDotNETToolVersionInfo(progLoc, true);

            return success;
        }

        private bool UpdateParameterFile(string paramFilePath, string targetsFileName)
        {
            try
            {
                var paramFile = new FileInfo(paramFilePath);

                if (paramFile.DirectoryName == null)
                {
                    LogError("Directory for parameter file found to be null in UpdateParameterFile");
                    return false;
                }

                if (string.IsNullOrWhiteSpace(targetsFileName))
                {
                    // Leave the parameter file unchanged
                    LogWarning("Warning: targets file was empty; parameter file used as-is", true);
                    return true;
                }

                var paramFileNew = new FileInfo(paramFile.FullName + ".new");

                using (var reader = new StreamReader(new FileStream(paramFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                using (var writer = new StreamWriter(new FileStream(paramFileNew.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            writer.WriteLine();
                            continue;
                        }

                        if (dataLine.Trim().StartsWith("param_dm_target_file", StringComparison.OrdinalIgnoreCase))
                        {
                            writer.WriteLine("param_dm_target_file=" + Path.Combine(mWorkDir, targetsFileName));
                        }
                        else
                        {
                            writer.WriteLine(dataLine);
                        }
                    }
                }

                paramFile.MoveTo(Path.Combine(paramFile.DirectoryName, paramFile.Name + ".old"));

                paramFileNew.MoveTo(paramFilePath);

                // Skip the old parameter file
                mJobParams.AddResultFileToSkip(paramFile.Name);
                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Error in NOMSIPlugin->UpdateParameterFile";
                LogError(mMessage, ex);
                return false;
            }
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

                    ParseConsoleOutputFile(Path.Combine(mWorkDir, mCurrentConsoleOutputFile));

                    LogProgress("NOMSI");
                }
            }
        }
    }
}
