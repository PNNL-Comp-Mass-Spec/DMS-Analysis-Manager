using AnalysisManagerBase;
using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerLipidMapSearchPlugIn
{
    /// <summary>
    /// Class for running LipidMapSearch
    /// </summary>
    public class AnalysisToolRunnerLipidMapSearch : AnalysisToolRunnerBase
    {
        // Ignore Spelling: hashcheck, pn, rn, rp, yyyy-MM-dd

        private const string LIPID_MAPS_DB_FILENAME_PREFIX = "LipidMapsDB_";
        private const int LIPID_MAPS_STALE_DB_AGE_DAYS = 5;

        private const string LIPID_TOOLS_RESULT_FILE_PREFIX = "LipidMap_";
        private const string LIPID_TOOLS_CONSOLE_OUTPUT = "LipidTools_ConsoleOutput.txt";

        private const int PROGRESS_PCT_UPDATING_LIPID_MAPS_DATABASE = 5;
        private const int PROGRESS_PCT_LIPID_TOOLS_STARTING = 10;

        private const int PROGRESS_PCT_LIPID_TOOLS_READING_DATABASE = 11;
        private const int PROGRESS_PCT_LIPID_TOOLS_READING_POSITIVE_DATA = 12;
        private const int PROGRESS_PCT_LIPID_TOOLS_READING_NEGATIVE_DATA = 13;
        private const int PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES = 15;
        private const int PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES = 50;
        private const int PROGRESS_PCT_LIPID_TOOLS_ALIGNING_FEATURES = 90;
        private const int PROGRESS_PCT_LIPID_TOOLS_MATCHING_TO_DB = 92;
        private const int PROGRESS_PCT_LIPID_TOOLS_WRITING_RESULTS = 94;
        private const int PROGRESS_PCT_LIPID_TOOLS_WRITING_QC_DATA = 96;

        private const int PROGRESS_PCT_LIPID_TOOLS_COMPLETE = 98;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private string mConsoleOutputErrorMsg;

        private string mLipidToolsProgLoc;
        private Dictionary<string, int> mConsoleOutputProgressMap;

        private string mLipidMapsDBFilename = string.Empty;

        private RunDosProgram mCmdRunner;

        /// <summary>
        /// Runs LipidMapSearch tool
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
                    LogDebug("AnalysisToolRunnerLipidMapSearch.RunTool(): Enter");
                }

                // Determine the path to the LipidTools program
                mLipidToolsProgLoc = DetermineProgramLocation("LipidToolsProgLoc", "LipidTools.exe");

                if (string.IsNullOrWhiteSpace(mLipidToolsProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the LipidTools version info in the database
                if (!StoreToolVersionInfo(mLipidToolsProgLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining LipidTools version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Obtain the LipidMaps.txt database
                mProgress = PROGRESS_PCT_UPDATING_LIPID_MAPS_DATABASE;

                if (!GetLipidMapsDatabase())
                {
                    LogError("Aborting since GetLipidMapsDatabase returned false");

                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Error obtaining the LipidMaps database";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mJobParams.AddResultFileToSkip(mLipidMapsDBFilename);               // Don't keep the Lipid Maps Database since we keep the permanent copy on Gigasax

                mConsoleOutputErrorMsg = string.Empty;

                // The parameter file name specifies the values to pass to LipidTools.exe at the command line
                var parameterFileName = mJobParams.GetParam("ParamFileName");
                var parameterFilePath = Path.Combine(mWorkDir, parameterFileName);

                LogMessage("Running LipidTools");

                // Set up and execute a program runner to run LipidTools
                var arguments = " -db " + PossiblyQuotePath(Path.Combine(mWorkDir, mLipidMapsDBFilename)) +
                                " -NoDBUpdate" +
                                " -rp " + PossiblyQuotePath(Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_RAW_EXTENSION));   // Positive-mode .Raw file

                var filePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResourcesLipidMapSearch.DECON_TOOLS_PEAKS_FILE_SUFFIX);

                if (File.Exists(filePath))
                {
                    arguments += " -pp " + PossiblyQuotePath(filePath);                  // Positive-mode peaks.txt file
                }

                var dataset2 = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "SourceJob2Dataset");

                if (!string.IsNullOrEmpty(dataset2))
                {
                    arguments += " -rn " + PossiblyQuotePath(Path.Combine(mWorkDir, dataset2 + AnalysisResources.DOT_RAW_EXTENSION)); // Negative-mode .Raw file

                    filePath = Path.Combine(mWorkDir, dataset2 + AnalysisResourcesLipidMapSearch.DECON_TOOLS_PEAKS_FILE_SUFFIX);

                    if (File.Exists(filePath))
                    {
                        arguments += " -pn " + PossiblyQuotePath(filePath);                  // Negative-mode peaks.txt file
                    }
                }

                // Append the remaining parameters
                arguments += ParseLipidMapSearchParameterFile(parameterFilePath);

                arguments += " -o " + PossiblyQuotePath(Path.Combine(mWorkDir, LIPID_TOOLS_RESULT_FILE_PREFIX));            // Folder and prefix text for output files

                if (mDebugLevel >= 1)
                {
                    LogDebug(mLipidToolsProgLoc + arguments);
                }

                mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                mCmdRunner.CreateNoWindow = true;
                mCmdRunner.CacheStandardOutput = true;
                mCmdRunner.EchoOutputToConsole = true;
                mCmdRunner.WriteConsoleOutputToFile = true;
                mCmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, LIPID_TOOLS_CONSOLE_OUTPUT);

                mProgress = PROGRESS_PCT_LIPID_TOOLS_STARTING;

                var processingSuccess = mCmdRunner.RunProgram(mLipidToolsProgLoc, arguments, "LipidTools", true);

                if (!mCmdRunner.WriteConsoleOutputToFile)
                {
                    // Write the console output to a text file
                    Global.IdleLoop(0.25);

                    using var writer = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                    writer.WriteLine(mCmdRunner.CachedConsoleOutput);
                }

                // Parse the console output file one more time to check for errors
                Global.IdleLoop(0.25);
                ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputErrorMsg);
                }

                var sha1Hash = HashUtilities.ComputeFileHashSha1(Path.Combine(mWorkDir, mLipidMapsDBFilename));

                // Append a line to the console output file listing the name of the LipidMapsDB that we used
                using (var writer = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Append, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine("LipidMapsDB Name: " + mLipidMapsDBFilename);
                    writer.WriteLine("LipidMapsDB Hash: " + sha1Hash);
                }

                // Update the evaluation message to include the lipid maps DB filename
                // This message will appear in Evaluation_Message column of T_Job_Steps
                mEvalMessage = mLipidMapsDBFilename;

                if (!processingSuccess)
                {
                    LogError("Error running LipidTools");

                    if (mCmdRunner.ExitCode != 0)
                    {
                        LogWarning("LipidTools returned a non-zero exit code: " + mCmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to LipidTools failed (but exit code is 0)");
                    }
                }
                else
                {
                    mProgress = PROGRESS_PCT_LIPID_TOOLS_COMPLETE;
                    mStatusTools.UpdateAndWrite(mProgress);

                    if (mDebugLevel >= 3)
                    {
                        LogDebug("LipidTools Search Complete");
                    }
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                // Zip up the text files that contain the data behind the plots
                // In addition, rename file LipidMap_results.xlsx
                var postProcessSuccess = PostProcessLipidToolsResults();

                if (!postProcessSuccess)
                {
                    processingSuccess = false;
                }

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in LipidMapSearchPlugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Downloads the latest version of the LipidMaps database
        /// </summary>
        /// <remarks>
        /// If the newly downloaded LipidMaps.txt file has a hash that matches the computed hash for newestLipidMapsDBFileName,
        /// then we update the time stamp on the HashCheckFile instead of copying the downloaded data back to the server
        /// </remarks>
        /// <param name="lipidMapsDBFolder">the directory to store the Lipid Maps DB file</param>
        /// <param name="newestLipidMapsDBFileName">The name of the newest Lipid Maps DB in the Lipid Maps DB folder</param>
        /// <returns>The filename of the latest version of the database</returns>
        private string DownloadNewLipidMapsDB(DirectoryInfo lipidMapsDBFolder, string newestLipidMapsDBFileName)
        {
            var lockFileFound = false;
            var lockFilePath = string.Empty;

            var hashCheckFilePath = string.Empty;
            var sha1HashNewestLipidMapsDBFile = string.Empty;

            // Look for a recent .lock file

            foreach (var lockFile in lipidMapsDBFolder.GetFiles("*" + Global.LOCK_FILE_EXTENSION))
            {
                if (DateTime.UtcNow.Subtract(lockFile.LastWriteTimeUtc).TotalHours < 2)
                {
                    lockFileFound = true;
                    lockFilePath = lockFile.FullName;
                    break;
                }

                // Lock file has aged; delete it
                lockFile.Delete();
            }

            if (lockFileFound)
            {
                var dataFilePath = lockFilePath.Substring(0, lockFilePath.Length - Global.LOCK_FILE_EXTENSION.Length);
                AnalysisResources.CheckForLockFile(dataFilePath, "LipidMapsDB", mStatusTools);

                newestLipidMapsDBFileName = FindNewestLipidMapsDB(lipidMapsDBFolder, out var lipidMapsDBFileTime);

                if (!string.IsNullOrEmpty(newestLipidMapsDBFileName))
                {
                    if (DateTime.UtcNow.Subtract(lipidMapsDBFileTime).TotalDays < LIPID_MAPS_STALE_DB_AGE_DAYS)
                    {
                        // File is now up-to-date
                        return newestLipidMapsDBFileName;
                    }
                }
            }

            if (!string.IsNullOrEmpty(newestLipidMapsDBFileName))
            {
                // Read the hash value stored in the hashcheck file for newestLipidMapsDBFileName
                hashCheckFilePath = GetHashCheckFilePath(lipidMapsDBFolder.FullName, newestLipidMapsDBFileName);

                if (File.Exists(hashCheckFilePath))
                {
                    using (var reader = new StreamReader(new FileStream(hashCheckFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                    {
                        sha1HashNewestLipidMapsDBFile = reader.ReadLine();
                    }

                    if (string.IsNullOrEmpty(sha1HashNewestLipidMapsDBFile))
                        sha1HashNewestLipidMapsDBFile = string.Empty;
                }
            }

            // Call the LipidTools.exe program to obtain the latest database

            var timeStamp = DateTime.Now.ToString("yyyy-MM-dd");
            var newLipidMapsDBFilePath = Path.Combine(lipidMapsDBFolder.FullName, LIPID_MAPS_DB_FILENAME_PREFIX + timeStamp);

            // Create a new lock file
            AnalysisResources.CreateLockFile(newLipidMapsDBFilePath, "Downloading LipidMaps.txt file via " + mMgrName);

            // Call the LipidTools program to obtain the latest database from http://www.lipidmaps.org/
            var lipidMapsDBFileLocal = Path.Combine(mWorkDir, LIPID_MAPS_DB_FILENAME_PREFIX + timeStamp + ".txt");

            LogMessage("Downloading latest LipidMaps database");

            var arguments = " -UpdateDBOnly" +
                            " -db " + PossiblyQuotePath(lipidMapsDBFileLocal);

            if (mDebugLevel >= 1)
            {
                LogDebug(mLipidToolsProgLoc + arguments);
            }

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;
            mCmdRunner.WriteConsoleOutputToFile = false;

            var success = mCmdRunner.RunProgram(mLipidToolsProgLoc, arguments, "LipidTools", true);

            if (!success)
            {
                mMessage = "Error downloading the latest LipidMaps DB using LipidTools";
                LogError(mMessage);

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("LipidTools returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to LipidTools failed (but exit code is 0)");
                }

                return string.Empty;
            }

            // Compute the Sha1 hash value of the newly downloaded file
            var sha1HashNew = PRISM.HashUtilities.ComputeFileHashSha1(lipidMapsDBFileLocal);

            if (!string.IsNullOrEmpty(sha1HashNewestLipidMapsDBFile) && sha1HashNew == sha1HashNewestLipidMapsDBFile)
            {
                // The hashes match; we'll update the timestamp of the hashcheck file below
                if (mDebugLevel >= 1)
                {
                    LogMessage(
                        "Hash code of the newly downloaded database matches the hash for " + newestLipidMapsDBFileName + ": " +
                        sha1HashNewestLipidMapsDBFile);
                }

                if (Path.GetFileName(lipidMapsDBFileLocal) != newestLipidMapsDBFileName)
                {
                    // Rename the newly downloaded file to newestLipidMapsDBFileName
                    Global.IdleLoop(0.25);

                    if (string.IsNullOrWhiteSpace(newestLipidMapsDBFileName))
                        throw new Exception("newestLipidMapsDBFileName is null in DownloadNewLipidMapsDB");

                    File.Move(lipidMapsDBFileLocal, Path.Combine(mWorkDir, newestLipidMapsDBFileName));
                }
            }
            else
            {
                // Copy the new file up to the server

                newestLipidMapsDBFileName = Path.GetFileName(lipidMapsDBFileLocal);

                var copyAttempts = 0;

                while (copyAttempts <= 2)
                {
                    var lipidMapsDBFileTarget = lipidMapsDBFolder.FullName + " plus " + newestLipidMapsDBFileName;

                    try
                    {
                        copyAttempts++;
                        lipidMapsDBFileTarget = Path.Combine(lipidMapsDBFolder.FullName, newestLipidMapsDBFileName);
                        File.Copy(lipidMapsDBFileLocal, lipidMapsDBFileTarget);
                        break;
                    }
                    catch (Exception ex)
                    {
                        LogError("Exception copying Lipid Maps DB to server; attempt=" + copyAttempts + ": " + ex.Message);
                        LogDebug("Source path: " + lipidMapsDBFileLocal);
                        LogDebug("Target path: " + lipidMapsDBFileTarget);
                        // Wait 5 seconds, then try again
                        Global.IdleLoop(5);
                    }
                }

                hashCheckFilePath = GetHashCheckFilePath(lipidMapsDBFolder.FullName, newestLipidMapsDBFileName);
            }

            // Update the hash-check file (do this regardless of whether or not the newly downloaded file matched the most recent one)
            using (var writer = new StreamWriter(new FileStream(hashCheckFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                writer.WriteLine(sha1HashNew);
            }

            Global.DeleteLockFile(newLipidMapsDBFilePath);

            return newestLipidMapsDBFileName;
        }

        private string FindNewestLipidMapsDB(DirectoryInfo lipidMapsDBFolder, out DateTime lipidMapsDBFileTime)
        {
            var newestLipidMapsDBFileName = string.Empty;

            lipidMapsDBFileTime = DateTime.MinValue;

            foreach (var dbFile in lipidMapsDBFolder.GetFiles(LIPID_MAPS_DB_FILENAME_PREFIX + "*.txt"))
            {
                if (dbFile.LastWriteTimeUtc > lipidMapsDBFileTime)
                {
                    lipidMapsDBFileTime = dbFile.LastWriteTimeUtc;
                    newestLipidMapsDBFileName = dbFile.Name;
                }
            }

            if (!string.IsNullOrEmpty(newestLipidMapsDBFileName))
            {
                // Now look for a .hashcheck file for this LipidMapsDB.txt file
                var hashCheckFile = new FileInfo(GetHashCheckFilePath(lipidMapsDBFolder.FullName, newestLipidMapsDBFileName));

                if (hashCheckFile.Exists)
                {
                    // Update the Lipid Maps DB file time
                    if (lipidMapsDBFileTime < hashCheckFile.LastWriteTimeUtc)
                    {
                        lipidMapsDBFileTime = hashCheckFile.LastWriteTimeUtc;
                    }
                }
            }

            return newestLipidMapsDBFileName;
        }

        private string GetHashCheckFilePath(string lipidMapsDBFolderPath, string newestLipidMapsDBFileName)
        {
            return Path.Combine(lipidMapsDBFolderPath, Path.GetFileNameWithoutExtension(newestLipidMapsDBFileName) + ".hashcheck");
        }

        private bool GetLipidMapsDatabase()
        {
            var updateDB = false;

            try
            {
                var paramFileFolderPath = mJobParams.GetJobParameter("ParamFileStoragePath", string.Empty);

                if (string.IsNullOrEmpty(paramFileFolderPath))
                {
                    mMessage = "Parameter 'ParamFileStoragePath' is empty";
                    LogError(mMessage + "; unable to get the LipidMaps database");
                    return false;
                }

                var lipidMapsDBFolder = new DirectoryInfo(Path.Combine(paramFileFolderPath, "LipidMapsDB"));

                if (!lipidMapsDBFolder.Exists)
                {
                    mMessage = "LipidMaps database folder not found";
                    LogError(mMessage + ": " + lipidMapsDBFolder.FullName);
                    return false;
                }

                // Find the newest date-stamped file
                var newestLipidMapsDBFileName = FindNewestLipidMapsDB(lipidMapsDBFolder, out var lipidMapsDbFileTime);

                if (string.IsNullOrEmpty(newestLipidMapsDBFileName))
                {
                    updateDB = true;
                }
                else if (DateTime.UtcNow.Subtract(lipidMapsDbFileTime).TotalDays > LIPID_MAPS_STALE_DB_AGE_DAYS)
                {
                    updateDB = true;
                }

                if (updateDB)
                {
                    var downloadAttempts = 0;

                    while (downloadAttempts <= 2)
                    {
                        try
                        {
                            downloadAttempts++;
                            newestLipidMapsDBFileName = DownloadNewLipidMapsDB(lipidMapsDBFolder, newestLipidMapsDBFileName);
                            break;
                        }
                        catch (Exception ex)
                        {
                            LogError("Exception downloading Lipid Maps DB; attempt=" + downloadAttempts + ": " + ex.Message);
                            // Wait 5 seconds, then try again
                            Global.IdleLoop(5);
                        }
                    }
                }

                if (string.IsNullOrEmpty(newestLipidMapsDBFileName))
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Unable to determine the LipidMapsDB file to copy locally";
                    }
                    return false;
                }

                // File is now up-to-date; copy locally (if not already in the work directory)
                mLipidMapsDBFilename = newestLipidMapsDBFileName;
                var sourceFilePath = Path.Combine(lipidMapsDBFolder.FullName, newestLipidMapsDBFileName);
                var targetFilePath = Path.Combine(mWorkDir, newestLipidMapsDBFileName);

                if (!File.Exists(targetFilePath))
                {
                    if (mDebugLevel >= 1)
                    {
                        LogMessage("Copying lipid Maps DB locally: " + sourceFilePath);
                    }
                    File.Copy(sourceFilePath, targetFilePath);
                }
            }
            catch (Exception ex)
            {
                LogError("Exception obtaining lipid Maps DB: " + ex.Message);
                return false;
            }

            return true;
        }

        private Dictionary<string, string> GetLipidMapsParameterNames()
        {
            var paramNames = new Dictionary<string, string>(25, StringComparer.OrdinalIgnoreCase)
            {
                {"AlignmentToleranceNET", "an"},
                {"AlignmentToleranceMassPPM", "am"},
                {"DBMatchToleranceMassPPM", "mm"},
                {"DBMatchToleranceMzPpmCID", "ct"},
                {"DBMatchToleranceMzPpmHCD", "ht"}
            };

            return paramNames;
        }

        // Example Console output:
        //   Reading local Lipid Maps database...Done.
        //   Reading positive data...Done.
        //   Reading negative data...Done.
        //   Finding features (positive)...200 / 4778
        //   400 / 4778
        //   ...
        //   4600 / 4778
        //   Done (1048 found).
        //   Finding features (negative)...200 / 4558
        //   400 / 4558
        //   ...
        //   4400 / 4558
        //   Done (900 found).
        //   Aligning features...Done (221 alignments).
        //   Matching to Lipid Maps database...Done (2041 matches).
        //   Writing results...Done.
        //   Writing QC data...Done.
        //   Saving QC images...Done.

        // ReSharper disable once UseImplicitlyTypedVariableEvident
        private readonly Regex reSubProgress = new(@"^(\d+) / (\d+)", RegexOptions.Compiled);

        /// <summary>
        /// Parse the LipidTools console output file to track progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            try
            {
                if (mConsoleOutputProgressMap == null || mConsoleOutputProgressMap.Count == 0)
                {
                    mConsoleOutputProgressMap = new Dictionary<string, int>
                    {
                        {"Reading local Lipid Maps database", PROGRESS_PCT_LIPID_TOOLS_READING_DATABASE},
                        {"Reading positive data", PROGRESS_PCT_LIPID_TOOLS_READING_POSITIVE_DATA},
                        {"Reading negative data", PROGRESS_PCT_LIPID_TOOLS_READING_NEGATIVE_DATA},
                        {"Finding features (positive)", PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES},
                        {"Finding features (negative)", PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES},
                        {"Aligning features", PROGRESS_PCT_LIPID_TOOLS_ALIGNING_FEATURES},
                        {"Matching to Lipid Maps database", PROGRESS_PCT_LIPID_TOOLS_MATCHING_TO_DB},
                        {"Writing results", PROGRESS_PCT_LIPID_TOOLS_WRITING_RESULTS},
                        {"Writing QC data", PROGRESS_PCT_LIPID_TOOLS_WRITING_QC_DATA}
                    };
                }

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

                double subProgressAddon = 0;
                var effectiveProgress = PROGRESS_PCT_LIPID_TOOLS_STARTING;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        // Update progress if the line starts with one of the expected phrases
                        foreach (var item in mConsoleOutputProgressMap)
                        {
                            if (dataLine.StartsWith(item.Key))
                            {
                                if (effectiveProgress < item.Value)
                                {
                                    effectiveProgress = item.Value;
                                }
                            }
                        }

                        if (effectiveProgress == PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES ||
                            effectiveProgress == PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES)
                        {
                            var match = reSubProgress.Match(dataLine);

                            if (match.Success)
                            {
                                if (int.TryParse(match.Groups[1].Value, out var subProgressCount))
                                {
                                    if (int.TryParse(match.Groups[2].Value, out var subProgressCountTotal))
                                    {
                                        subProgressAddon = subProgressCount / (double)subProgressCountTotal;
                                    }
                                }
                            }
                        }
                    }
                }

                float progressOverall = effectiveProgress;

                // Bump up the effective progress if finding features in positive or negative data
                if (effectiveProgress == PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES)
                {
                    progressOverall += (float)((PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES - PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES) * subProgressAddon);
                }
                else if (effectiveProgress == PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES)
                {
                    progressOverall += (float)((PROGRESS_PCT_LIPID_TOOLS_ALIGNING_FEATURES - PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES) * subProgressAddon);
                }

                if (mProgress < progressOverall)
                {
                    mProgress = progressOverall;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Read the LipidMapSearch options file and convert the options to command line switches
        /// </summary>
        /// <param name="parameterFilePath">Path to the LipidMapSearch Parameter File</param>
        /// <returns>Options string if success; empty string if an error</returns>
        private string ParseLipidMapSearchParameterFile(string parameterFilePath)
        {
            var options = new StringBuilder(500);

            try
            {
                // Initialize the Param Name dictionary
                var paramNames = GetLipidMapsParameterNames();

                using var reader = new StreamReader(new FileStream(parameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    var key = string.Empty;
                    var value = string.Empty;

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    var dataLineTrimmed = dataLine.Trim();

                    if (!dataLineTrimmed.StartsWith("#") && dataLineTrimmed.Contains("="))
                    {
                        var charIndex = dataLineTrimmed.IndexOf('=');

                        if (charIndex > 0)
                        {
                            key = dataLineTrimmed.Substring(0, charIndex).Trim();

                            if (charIndex < dataLineTrimmed.Length - 1)
                            {
                                value = dataLineTrimmed.Substring(charIndex + 1).Trim();
                            }
                            else
                            {
                                value = string.Empty;
                            }
                        }
                    }

                    if (string.IsNullOrWhiteSpace(key))
                        continue;

                    // Check whether key is one of the standard keys defined in paramNames
                    if (paramNames.TryGetValue(key, out var argumentSwitch))
                    {
                        options.AppendFormat(" -{0} {1}", argumentSwitch, value);
                    }
                    else if (string.Equals(key, "adducts", StringComparison.OrdinalIgnoreCase))
                    {
                        options.AppendFormat(" -adducts \"{0}\"", value);
                    }
                    else if (string.Equals(key, "NoScanGroups", StringComparison.OrdinalIgnoreCase))
                    {
                        if (bool.TryParse(value, out var booleanValue))
                        {
                            if (booleanValue)
                            {
                                options.Append(" -NoScanGroups");
                            }
                        }
                    }
                    else
                    {
                        // Ignore the option
                        LogWarning("Unrecognized setting in the LipidMaps parameter file: " + key);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception reading LipidMaps parameter file: " + ex.Message);
                return string.Empty;
            }

            return options.ToString();
        }

        private bool PostProcessLipidToolsResults()
        {
            try
            {
                // Create the PlotData folder and move the plot data text files into that folder
                var folderToZip = Path.Combine(mWorkDir, "PlotData");
                Directory.CreateDirectory(folderToZip);

                var filesToMove = new List<string>
                {
                    LIPID_TOOLS_RESULT_FILE_PREFIX + "AlignMassError.txt",
                    LIPID_TOOLS_RESULT_FILE_PREFIX + "AlignNETError.txt",
                    LIPID_TOOLS_RESULT_FILE_PREFIX + "MatchMassError.txt",
                    LIPID_TOOLS_RESULT_FILE_PREFIX + "Tiers.txt",
                    LIPID_TOOLS_RESULT_FILE_PREFIX + "MassErrorComparison.txt"
                };

                foreach (var fileName in filesToMove)
                {
                    var sourceFile = new FileInfo(Path.Combine(mWorkDir, fileName));

                    if (sourceFile.Exists)
                    {
                        sourceFile.MoveTo(Path.Combine(folderToZip, fileName));
                    }
                }

                // Zip up the files in the PlotData folder
                var zipTools = new ZipFileTools(mDebugLevel, mWorkDir);

                zipTools.ZipDirectory(folderToZip, Path.Combine(mWorkDir, "LipidMap_PlotData.zip"));
            }
            catch (Exception ex)
            {
                mMessage = "Exception zipping the plot data text files";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }

            try
            {
                var excelFile = new FileInfo(Path.Combine(mWorkDir, LIPID_TOOLS_RESULT_FILE_PREFIX + "results.xlsx"));

                if (!excelFile.Exists)
                {
                    mMessage = "Excel results file not found";
                    LogError(mMessage + ": " + excelFile.Name);
                    return false;
                }

                excelFile.MoveTo(Path.Combine(mWorkDir, LIPID_TOOLS_RESULT_FILE_PREFIX + "results_" + mDatasetName + ".xlsx"));
            }
            catch (Exception ex)
            {
                mMessage = "Exception renaming Excel results file";
                LogError(mMessage + ": " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var success = StoreDotNETToolVersionInfo(progLoc, true);

            return success;
        }

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(mWorkDir, LIPID_TOOLS_CONSOLE_OUTPUT));

                LogProgress("LipidMapSearch");
            }
        }
    }
}
