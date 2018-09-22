using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerLipidMapSearchPlugIn
{
    /// <summary>
    /// Class for running LipidMapSearch
    /// </summary>
    public class clsAnalysisToolRunnerLipidMapSearch : clsAnalysisToolRunnerBase
    {
        #region "Module Variables"

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

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

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

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerLipidMapSearch.RunTool(): Enter");
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
                    m_message = "Error determining LipidTools version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Obtain the LipidMaps.txt database
                m_progress = PROGRESS_PCT_UPDATING_LIPID_MAPS_DATABASE;

                if (!GetLipidMapsDatabase())
                {
                    LogError("Aborting since GetLipidMapsDatabase returned false");
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Error obtaining the LipidMaps database";
                    }
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                m_jobParams.AddResultFileToSkip(mLipidMapsDBFilename);               // Don't keep the Lipid Maps Database since we keep the permanent copy on Gigasax

                mConsoleOutputErrorMsg = string.Empty;

                // The parameter file name specifies the values to pass to LipidTools.exe at the command line
                var strParameterFileName = m_jobParams.GetParam("parmFileName");
                var strParameterFilePath = Path.Combine(m_WorkDir, strParameterFileName);

                LogMessage("Running LipidTools");

                // Set up and execute a program runner to run LipidTools
                var cmdStr = " -db " + PossiblyQuotePath(Path.Combine(m_WorkDir, mLipidMapsDBFilename)) + " -NoDBUpdate";
                cmdStr += " -rp " + PossiblyQuotePath(Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_RAW_EXTENSION));   // Positive-mode .Raw file

                var strFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResourcesLipidMapSearch.DECONTOOLS_PEAKS_FILE_SUFFIX);
                if (File.Exists(strFilePath))
                {
                    cmdStr += " -pp " + PossiblyQuotePath(strFilePath);                  // Positive-mode peaks.txt file
                }

                var strDataset2 = m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "SourceJob2Dataset");
                if (!string.IsNullOrEmpty(strDataset2))
                {
                    cmdStr += " -rn " + PossiblyQuotePath(Path.Combine(m_WorkDir, strDataset2 + clsAnalysisResources.DOT_RAW_EXTENSION)); // Negative-mode .Raw file

                    strFilePath = Path.Combine(m_WorkDir, strDataset2 + clsAnalysisResourcesLipidMapSearch.DECONTOOLS_PEAKS_FILE_SUFFIX);
                    if (File.Exists(strFilePath))
                    {
                        cmdStr += " -pn " + PossiblyQuotePath(strFilePath);                  // Negative-mode peaks.txt file
                    }
                }

                // Append the remaining parameters
                cmdStr += ParseLipidMapSearchParameterFile(strParameterFilePath);

                cmdStr += " -o " + PossiblyQuotePath(Path.Combine(m_WorkDir, LIPID_TOOLS_RESULT_FILE_PREFIX));            // Folder and prefix text for output files

                if (m_DebugLevel >= 1)
                {
                    LogDebug(mLipidToolsProgLoc + cmdStr);
                }

                mCmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
                RegisterEvents(mCmdRunner);
                mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

                mCmdRunner.CreateNoWindow = true;
                mCmdRunner.CacheStandardOutput = true;
                mCmdRunner.EchoOutputToConsole = true;
                mCmdRunner.WriteConsoleOutputToFile = true;
                mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, LIPID_TOOLS_CONSOLE_OUTPUT);

                m_progress = PROGRESS_PCT_LIPID_TOOLS_STARTING;

                var processingSuccess = mCmdRunner.RunProgram(mLipidToolsProgLoc, cmdStr, "LipidTools", true);

                if (!mCmdRunner.WriteConsoleOutputToFile)
                {
                    // Write the console output to a text file
                    clsGlobal.IdleLoop(0.25);

                    using (var swConsoleOutputfile = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        swConsoleOutputfile.WriteLine(mCmdRunner.CachedConsoleOutput);
                    }
                }

                // Parse the console output file one more time to check for errors
                clsGlobal.IdleLoop(0.25);
                ParseConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

                if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                {
                    LogError(mConsoleOutputErrorMsg);
                }

                var sha1Hash = HashUtilities.ComputeFileHashSha1(Path.Combine(m_WorkDir, mLipidMapsDBFilename));

                // Append a line to the console output file listing the name of the LipidMapsDB that we used
                using (var swConsoleOutputFile = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Append, FileAccess.Write, FileShare.Read)))
                {
                    swConsoleOutputFile.WriteLine("LipidMapsDB Name: " + mLipidMapsDBFilename);
                    swConsoleOutputFile.WriteLine("LipidMapsDB Hash: " + sha1Hash);
                }

                // Update the evaluation message to include the lipid maps DB filename
                // This message will appear in Evaluation_Message column of T_Job_Steps
                m_EvalMessage = string.Copy(mLipidMapsDBFilename);

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
                    m_progress = PROGRESS_PCT_LIPID_TOOLS_COMPLETE;
                    m_StatusTools.UpdateAndWrite(m_progress);
                    if (m_DebugLevel >= 3)
                    {
                        LogDebug("LipidTools Search Complete");
                    }
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.ProgRunner.GarbageCollectNow();

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
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Exception in LipidMapSearchPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Downloads the latest version of the LipidMaps database
        /// </summary>
        /// <param name="diLipidMapsDBFolder">the directory to store the Lipid Maps DB file</param>
        /// <param name="strNewestLipidMapsDBFileName">The name of the newest Lipid Maps DB in the Lipid Maps DB folder</param>
        /// <returns>The filename of the latest version of the database</returns>
        /// <remarks>
        /// If the newly downloaded LipidMaps.txt file has a hash that matches the computed hash for strNewestLipidMapsDBFileName,
        /// then we update the time stamp on the HashCheckFile instead of copying the downloaded data back to the server
        /// </remarks>
        private string DownloadNewLipidMapsDB(DirectoryInfo diLipidMapsDBFolder, string strNewestLipidMapsDBFileName)
        {
            var lockFileFound = false;
            var strLockFilePath = string.Empty;

            var strHashCheckFilePath = string.Empty;
            var sha1HashNewestLipidMapsDBFile = string.Empty;

            // Look for a recent .lock file

            foreach (var lockFile in diLipidMapsDBFolder.GetFiles("*" + clsGlobal.LOCK_FILE_EXTENSION))
            {
                if (DateTime.UtcNow.Subtract(lockFile.LastWriteTimeUtc).TotalHours < 2)
                {
                    lockFileFound = true;
                    strLockFilePath = lockFile.FullName;
                    break;
                }

                // Lock file has aged; delete it
                lockFile.Delete();
            }

            if (lockFileFound)
            {
                var dataFilePath = strLockFilePath.Substring(0, strLockFilePath.Length - clsGlobal.LOCK_FILE_EXTENSION.Length);
                clsAnalysisResources.CheckForLockFile(dataFilePath, "LipidMapsDB", m_StatusTools);

                strNewestLipidMapsDBFileName = FindNewestLipidMapsDB(diLipidMapsDBFolder, out var dtLipidMapsDBFileTime);

                if (!string.IsNullOrEmpty(strNewestLipidMapsDBFileName))
                {
                    if (DateTime.UtcNow.Subtract(dtLipidMapsDBFileTime).TotalDays < LIPID_MAPS_STALE_DB_AGE_DAYS)
                    {
                        // File is now up-to-date
                        return strNewestLipidMapsDBFileName;
                    }
                }
            }

            if (!string.IsNullOrEmpty(strNewestLipidMapsDBFileName))
            {
                // Read the hash value stored in the hashcheck file for strNewestLipidMapsDBFileName
                strHashCheckFilePath = GetHashCheckFilePath(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName);

                if (File.Exists(strHashCheckFilePath))
                {
                    using (var srInFile = new StreamReader(new FileStream(strHashCheckFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                    {
                        sha1HashNewestLipidMapsDBFile = srInFile.ReadLine();
                    }

                    if (string.IsNullOrEmpty(sha1HashNewestLipidMapsDBFile))
                        sha1HashNewestLipidMapsDBFile = string.Empty;
                }
            }

            // Call the LipidTools.exe program to obtain the latest database

            var strTimeStamp = DateTime.Now.ToString("yyyy-MM-dd");
            var newLipidMapsDBFilePath = Path.Combine(diLipidMapsDBFolder.FullName, LIPID_MAPS_DB_FILENAME_PREFIX + strTimeStamp);

            // Create a new lock file
            clsAnalysisResources.CreateLockFile(newLipidMapsDBFilePath, "Downloading LipidMaps.txt file via " + m_MachName);

            // Call the LipidTools program to obtain the latest database from http://www.lipidmaps.org/
            var strLipidMapsDBFileLocal = Path.Combine(m_WorkDir, LIPID_MAPS_DB_FILENAME_PREFIX + strTimeStamp + ".txt");

            LogMessage("Downloading latest LipidMaps database");

            var cmdStr = " -UpdateDBOnly -db " + PossiblyQuotePath(strLipidMapsDBFileLocal);

            if (m_DebugLevel >= 1)
            {
                LogDebug(mLipidToolsProgLoc + cmdStr);
            }

            mCmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;
            mCmdRunner.WriteConsoleOutputToFile = false;

            var blnSuccess = mCmdRunner.RunProgram(mLipidToolsProgLoc, cmdStr, "LipidTools", true);

            if (!blnSuccess)
            {
                m_message = "Error downloading the latest LipidMaps DB using LipidTools";
                LogError(m_message);

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
            var sha1HashNew = PRISM.HashUtilities.ComputeFileHashSha1(strLipidMapsDBFileLocal);

            if (!string.IsNullOrEmpty(sha1HashNewestLipidMapsDBFile) && sha1HashNew == sha1HashNewestLipidMapsDBFile)
            {
                // The hashes match; we'll update the timestamp of the hashcheck file below
                if (m_DebugLevel >= 1)
                {
                    LogMessage(
                        "Hash code of the newly downloaded database matches the hash for " + strNewestLipidMapsDBFileName + ": " +
                        sha1HashNewestLipidMapsDBFile);
                }

                if (Path.GetFileName(strLipidMapsDBFileLocal) != strNewestLipidMapsDBFileName)
                {
                    // Rename the newly downloaded file to strNewestLipidMapsDBFileName
                    clsGlobal.IdleLoop(0.25);
                    if (string.IsNullOrWhiteSpace(strNewestLipidMapsDBFileName))
                        throw new Exception("strNewestLipidMapsDBFileName is null in DownloadNewLipidMapsDB");

                    File.Move(strLipidMapsDBFileLocal, Path.Combine(m_WorkDir, strNewestLipidMapsDBFileName));
                }
            }
            else
            {
                // Copy the new file up to the server

                strNewestLipidMapsDBFileName = Path.GetFileName(strLipidMapsDBFileLocal);

                var intCopyAttempts = 0;

                while (intCopyAttempts <= 2)
                {
                    var strLipidMapsDBFileTarget = diLipidMapsDBFolder.FullName + " plus " + strNewestLipidMapsDBFileName;

                    try
                    {
                        intCopyAttempts += 1;
                        strLipidMapsDBFileTarget = Path.Combine(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName);
                        File.Copy(strLipidMapsDBFileLocal, strLipidMapsDBFileTarget);
                        break;
                    }
                    catch (Exception ex)
                    {
                        LogError("Exception copying Lipid Maps DB to server; attempt=" + intCopyAttempts + ": " + ex.Message);
                        LogDebug("Source path: " + strLipidMapsDBFileLocal);
                        LogDebug("Target path: " + strLipidMapsDBFileTarget);
                        // Wait 5 seconds, then try again
                        clsGlobal.IdleLoop(5);
                    }
                }

                strHashCheckFilePath = GetHashCheckFilePath(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName);
            }

            // Update the hash-check file (do this regardless of whether or not the newly downloaded file matched the most recent one)
            using (var swOutFile = new StreamWriter(new FileStream(strHashCheckFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                swOutFile.WriteLine(sha1HashNew);
            }

            clsGlobal.DeleteLockFile(newLipidMapsDBFilePath);

            return strNewestLipidMapsDBFileName;
        }

        private string FindNewestLipidMapsDB(DirectoryInfo diLipidMapsDBFolder, out DateTime dtLipidMapsDBFileTime)
        {
            var strNewestLipidMapsDBFileName = string.Empty;

            dtLipidMapsDBFileTime = DateTime.MinValue;

            foreach (var dbFile in diLipidMapsDBFolder.GetFiles(LIPID_MAPS_DB_FILENAME_PREFIX + "*.txt"))
            {
                if (dbFile.LastWriteTimeUtc > dtLipidMapsDBFileTime)
                {
                    dtLipidMapsDBFileTime = dbFile.LastWriteTimeUtc;
                    strNewestLipidMapsDBFileName = dbFile.Name;
                }
            }

            if (!string.IsNullOrEmpty(strNewestLipidMapsDBFileName))
            {
                // Now look for a .hashcheck file for this LipidMapsDB.txt file
                var fiHashCheckFile = new FileInfo(GetHashCheckFilePath(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName));

                if (fiHashCheckFile.Exists)
                {
                    // Update the Lipid Maps DB file time
                    if (dtLipidMapsDBFileTime < fiHashCheckFile.LastWriteTimeUtc)
                    {
                        dtLipidMapsDBFileTime = fiHashCheckFile.LastWriteTimeUtc;
                    }
                }
            }

            return strNewestLipidMapsDBFileName;
        }

        private string GetHashCheckFilePath(string strLipidMapsDBFolderPath, string strNewestLipidMapsDBFileName)
        {
            return Path.Combine(strLipidMapsDBFolderPath, Path.GetFileNameWithoutExtension(strNewestLipidMapsDBFileName) + ".hashcheck");
        }

        private bool GetLipidMapsDatabase()
        {
            var blnUpdateDB = false;

            try
            {
                var strParamFileFolderPath = m_jobParams.GetJobParameter("ParmFileStoragePath", string.Empty);

                if (string.IsNullOrEmpty(strParamFileFolderPath))
                {
                    m_message = "Parameter 'ParmFileStoragePath' is empty";
                    LogError(m_message + "; unable to get the LipidMaps database");
                    return false;
                }

                var diLipidMapsDBFolder = new DirectoryInfo(Path.Combine(strParamFileFolderPath, "LipidMapsDB"));

                if (!diLipidMapsDBFolder.Exists)
                {
                    m_message = "LipidMaps database folder not found";
                    LogError(m_message + ": " + diLipidMapsDBFolder.FullName);
                    return false;
                }

                // Find the newest date-stamped file
                var strNewestLipidMapsDBFileName = FindNewestLipidMapsDB(diLipidMapsDBFolder, out var dtLipidMapsDbFileTime);

                if (string.IsNullOrEmpty(strNewestLipidMapsDBFileName))
                {
                    blnUpdateDB = true;
                }
                else if (DateTime.UtcNow.Subtract(dtLipidMapsDbFileTime).TotalDays > LIPID_MAPS_STALE_DB_AGE_DAYS)
                {
                    blnUpdateDB = true;
                }

                if (blnUpdateDB)
                {
                    var intDownloadAttempts = 0;

                    while (intDownloadAttempts <= 2)
                    {
                        try
                        {
                            intDownloadAttempts += 1;
                            strNewestLipidMapsDBFileName = DownloadNewLipidMapsDB(diLipidMapsDBFolder, strNewestLipidMapsDBFileName);
                            break;
                        }
                        catch (Exception ex)
                        {
                            LogError("Exception downloading Lipid Maps DB; attempt=" + intDownloadAttempts + ": " + ex.Message);
                            // Wait 5 seconds, then try again
                            clsGlobal.IdleLoop(5);
                        }
                    }
                }

                if (string.IsNullOrEmpty(strNewestLipidMapsDBFileName))
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Unable to determine the LipidMapsDB file to copy locally";
                    }
                    return false;
                }

                // File is now up-to-date; copy locally (if not already in the work dir)
                mLipidMapsDBFilename = string.Copy(strNewestLipidMapsDBFileName);
                var strSourceFilePath = Path.Combine(diLipidMapsDBFolder.FullName, strNewestLipidMapsDBFileName);
                var strTargetFilePath = Path.Combine(m_WorkDir, strNewestLipidMapsDBFileName);

                if (!File.Exists(strTargetFilePath))
                {
                    if (m_DebugLevel >= 1)
                    {
                        LogMessage("Copying lipid Maps DB locally: " + strSourceFilePath);
                    }
                    File.Copy(strSourceFilePath, strTargetFilePath);
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
            var dctParamNames = new Dictionary<string, string>(25, StringComparer.OrdinalIgnoreCase)
            {
                {"AlignmentToleranceNET", "an"},
                {"AlignmentToleranceMassPPM", "am"},
                {"DBMatchToleranceMassPPM", "mm"},
                {"DBMatchToleranceMzPpmCID", "ct"},
                {"DBMatchToleranceMzPpmHCD", "ht"}
            };


            return dctParamNames;
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
        private readonly Regex reSubProgress = new Regex(@"^(\d+) / (\d+)", RegexOptions.Compiled);

        /// <summary>
        /// Parse the LipidTools console output file to track progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
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

                double dblSubProgressAddon = 0;
                var intEffectiveProgress = PROGRESS_PCT_LIPID_TOOLS_STARTING;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                            continue;

                        // Update progress if the line starts with one of the expected phrases
                        foreach (var oItem in mConsoleOutputProgressMap)
                        {
                            if (strLineIn.StartsWith(oItem.Key))
                            {
                                if (intEffectiveProgress < oItem.Value)
                                {
                                    intEffectiveProgress = oItem.Value;
                                }
                            }
                        }

                        if (intEffectiveProgress == PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES ||
                            intEffectiveProgress == PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES)
                        {
                            var oMatch = reSubProgress.Match(strLineIn);
                            if (oMatch.Success)
                            {
                                if (int.TryParse(oMatch.Groups[1].Value, out var intSubProgressCount))
                                {
                                    if (int.TryParse(oMatch.Groups[2].Value, out var intSubProgressCountTotal))
                                    {
                                        dblSubProgressAddon = intSubProgressCount / (double)intSubProgressCountTotal;
                                    }
                                }
                            }
                        }
                    }
                }

                float sngEffectiveProgress = intEffectiveProgress;

                // Bump up the effective progress if finding features in positive or negative data
                if (intEffectiveProgress == PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES)
                {
                    sngEffectiveProgress += (float)((PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES - PROGRESS_PCT_LIPID_TOOLS_FINDING_POSITIVE_FEATURES) * dblSubProgressAddon);
                }
                else if (intEffectiveProgress == PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES)
                {
                    sngEffectiveProgress += (float)((PROGRESS_PCT_LIPID_TOOLS_ALIGNING_FEATURES - PROGRESS_PCT_LIPID_TOOLS_FINDING_NEGATIVE_FEATURES) * dblSubProgressAddon);
                }

                if (m_progress < sngEffectiveProgress)
                {
                    m_progress = sngEffectiveProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Read the LipidMapSearch options file and convert the options to command line switches
        /// </summary>
        /// <param name="strParameterFilePath">Path to the LipidMapSearch Parameter File</param>
        /// <returns>Options string if success; empty string if an error</returns>
        /// <remarks></remarks>
        private string ParseLipidMapSearchParameterFile(string strParameterFilePath)
        {
            var sbOptions = new StringBuilder(500);

            try
            {
                // Initialize the Param Name dictionary
                var dctParamNames = GetLipidMapsParameterNames();

                using (var srParamFile = new StreamReader(new FileStream(strParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srParamFile.EndOfStream)
                    {
                        var strLineIn = srParamFile.ReadLine();
                        var strKey = string.Empty;
                        var strValue = string.Empty;

                        if (string.IsNullOrWhiteSpace(strLineIn))
                        {
                            continue;
                        }

                        strLineIn = strLineIn.Trim();

                        if (!strLineIn.StartsWith("#") && strLineIn.Contains("="))
                        {
                            var intCharIndex = strLineIn.IndexOf('=');
                            if (intCharIndex > 0)
                            {
                                strKey = strLineIn.Substring(0, intCharIndex).Trim();
                                if (intCharIndex < strLineIn.Length - 1)
                                {
                                    strValue = strLineIn.Substring(intCharIndex + 1).Trim();
                                }
                                else
                                {
                                    strValue = string.Empty;
                                }
                            }
                        }

                        if (string.IsNullOrWhiteSpace(strKey))
                            continue;

                        // Check whether strKey is one of the standard keys defined in dctParamNames
                        if (dctParamNames.TryGetValue(strKey, out var strArgumentSwitch))
                        {
                            sbOptions.Append(" -" + strArgumentSwitch + " " + strValue);
                        }
                        else if (strKey.ToLower() == "adducts")
                        {
                            sbOptions.Append(" -adducts " + "\"" + strValue + "\"");
                        }
                        else if (strKey.ToLower() == "noscangroups")
                        {
                            if (bool.TryParse(strValue, out var blnValue))
                            {
                                if (blnValue)
                                {
                                    sbOptions.Append(" -NoScanGroups");
                                }
                            }
                        }
                        else
                        {
                            // Ignore the option
                            LogWarning("Unrecognized setting in the LipidMaps parameter file: " + strKey);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception reading LipidMaps parameter file: " + ex.Message);
                return string.Empty;
            }

            return sbOptions.ToString();
        }

        private bool PostProcessLipidToolsResults()
        {
            try
            {
                // Create the PlotData folder and move the plot data text files into that folder
                var strFolderToZip = Path.Combine(m_WorkDir, "PlotData");
                Directory.CreateDirectory(strFolderToZip);

                var lstFilesToMove = new List<string>
                {
                    LIPID_TOOLS_RESULT_FILE_PREFIX + "AlignMassError.txt",
                    LIPID_TOOLS_RESULT_FILE_PREFIX + "AlignNETError.txt",
                    LIPID_TOOLS_RESULT_FILE_PREFIX + "MatchMassError.txt",
                    LIPID_TOOLS_RESULT_FILE_PREFIX + "Tiers.txt",
                    LIPID_TOOLS_RESULT_FILE_PREFIX + "MassErrorComparison.txt"
                };

                foreach (var strFileName in lstFilesToMove)
                {
                    var fiSourceFile = new FileInfo(Path.Combine(m_WorkDir, strFileName));

                    if (fiSourceFile.Exists)
                    {
                        fiSourceFile.MoveTo(Path.Combine(strFolderToZip, strFileName));
                    }
                }

                // Zip up the files in the PlotData folder
                var dotNetZipTools = new clsDotNetZipTools(m_DebugLevel, m_WorkDir);

                dotNetZipTools.ZipDirectory(strFolderToZip, Path.Combine(m_WorkDir, "LipidMap_PlotData.zip"));
            }
            catch (Exception ex)
            {
                m_message = "Exception zipping the plot data text files";
                LogError(m_message + ": " + ex.Message);
                return false;
            }

            try
            {
                var fiExcelFile = new FileInfo(Path.Combine(m_WorkDir, LIPID_TOOLS_RESULT_FILE_PREFIX + "results.xlsx"));

                if (!fiExcelFile.Exists)
                {
                    m_message = "Excel results file not found";
                    LogError(m_message + ": " + fiExcelFile.Name);
                    return false;
                }

                fiExcelFile.MoveTo(Path.Combine(m_WorkDir, LIPID_TOOLS_RESULT_FILE_PREFIX + "results_" + m_Dataset + ".xlsx"));
            }
            catch (Exception ex)
            {
                m_message = "Exception renaming Excel results file";
                LogError(m_message + ": " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var success = StoreDotNETToolVersionInfo(progLoc);

            return success;
        }

        #endregion

        #region "Event Handlers"

        private DateTime dtLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15)
            {
                dtLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(m_WorkDir, LIPID_TOOLS_CONSOLE_OUTPUT));

                LogProgress("LipidMapSearch");
            }
        }

        #endregion
    }
}
