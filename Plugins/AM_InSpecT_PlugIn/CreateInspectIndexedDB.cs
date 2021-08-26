//*********************************************************************************************************
// Written by John Sandoval for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 01/29/2009
//
//*********************************************************************************************************

using AnalysisManagerBase;
using PRISM.Logging;
using System;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerInSpecTPlugIn
{
    public class CreateInspectIndexedDB
    {
        /// <summary>
        /// Convert FASTA file to indexed DB files
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public CloseOutType CreateIndexedDbFiles(ref IMgrParams mgrParams, ref IJobParams jobParams, int debugLevel, int jobNum, string inspectDir, string orgDbDir)
        {
            const float MAX_WAITTIME_HOURS = 1.0f;
            const float MAX_WAITTIME_PREVENT_REPEATS = 2.0f;

            const string PREPDB_SCRIPT = "PrepDB.py";
            const string SHUFFLEDB_SCRIPT = "ShuffleDB_Seed.py";

            var maxWaitTimeHours = MAX_WAITTIME_HOURS;

            try
            {
                if (debugLevel > 4)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                        "CreateInspectIndexedDB.CreateIndexedDbFiles(): Enter");
                }

                var randomNumberSeed = jobParams.GetJobParameter("InspectShuffleDBSeed", 1000);
                var shuffleDBPreventRepeats = jobParams.GetJobParameter("InspectPreventShuffleDBRepeats", false);

                if (shuffleDBPreventRepeats)
                {
                    maxWaitTimeHours = MAX_WAITTIME_PREVENT_REPEATS;
                }

                var dbfileNameInput = Path.Combine(orgDbDir, jobParams.GetParam("PeptideSearch", "generatedFastaName"));
                var useShuffledDB = jobParams.GetJobParameter("InspectUsesShuffledDB", false);

                var outputNameBase = Path.GetFileNameWithoutExtension(dbfileNameInput);
                var dbTrieFilenameBeforeShuffle = Path.Combine(orgDbDir, outputNameBase + ".trie");

                if (useShuffledDB)
                {
                    // Will create the .trie file using PrepDB.py, then shuffle it using shuffleDB.py
                    // The Pvalue.py script does much better at computing p-values if a decoy search is performed (i.e. shuffleDB.py is used)
                    // Note that shuffleDB will add a prefix of XXX to the shuffled protein names
                    outputNameBase += "_shuffle";
                }

                var dbLockFilename = Path.Combine(orgDbDir, outputNameBase + "_trie.lock");
                var dbTrieFilename = Path.Combine(orgDbDir, outputNameBase + ".trie");

                var pythonProgLoc = mgrParams.GetParam("PythonProgLoc");

                var prepDB = new RunDosProgram(inspectDir + Path.DirectorySeparatorChar, debugLevel);
                prepDB.ErrorEvent += CmdRunner_ErrorEvent;

                // Check to see if another Analysis Manager is already creating the indexed db files
                if (File.Exists(dbLockFilename))
                {
                    if (debugLevel >= 1)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                            "Lock file found: " + dbLockFilename + "; waiting for file to be removed by other manager generating .trie file " +
                            Path.GetFileName(dbTrieFilename));
                    }

                    // Lock file found; wait up to maxWaitTimeHours hours
                    var lockFileInfo = new FileInfo(dbLockFilename);
                    var createTime = lockFileInfo.CreationTimeUtc;
                    var currentTime = DateTime.UtcNow;
                    var durationTime = currentTime - createTime;
                    while (File.Exists(dbLockFilename) && durationTime.Hours < maxWaitTimeHours)
                    {
                        // Sleep for 2 seconds
                        Global.IdleLoop(2);

                        // Update the current time and elapsed duration
                        currentTime = DateTime.UtcNow;
                        durationTime = currentTime - createTime;
                    }

                    // If the duration time has exceeded maxWaitTimeHours, delete the lock file and try again with this manager
                    if (durationTime.Hours > maxWaitTimeHours)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                            "Waited over " + maxWaitTimeHours.ToString("0.0") + " hour(s) for lock file: " + dbLockFilename +
                            " to be deleted, but it is still present; deleting the file now and continuing");
                        if (File.Exists(dbLockFilename))
                        {
                            File.Delete(dbLockFilename);
                        }
                    }
                }

                // If lock file existed, the index files should now be created
                // Check for one of the index files in case this is the first time or there was a problem with
                // another manager creating it.
                if (!File.Exists(dbTrieFilename))
                {
                    // Try to create the index files for FASTA file dbfileNameInput

                    // Verify that python program file exists
                    if (!File.Exists(pythonProgLoc))
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                            "Cannot find python.exe program file: " + pythonProgLoc);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    // Verify that the PrepDB python script exists
                    var prebDBScriptPath = Path.Combine(inspectDir, PREPDB_SCRIPT);
                    if (!File.Exists(prebDBScriptPath))
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                            "Cannot find PrepDB script: " + prebDBScriptPath);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    // Verify that the ShuffleDB python script exists
                    var ShuffleDBScriptPath = Path.Combine(inspectDir, SHUFFLEDB_SCRIPT);
                    if (useShuffledDB)
                    {
                        if (!File.Exists(ShuffleDBScriptPath))
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                                "Cannot find ShuffleDB script: " + ShuffleDBScriptPath);
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                    }

                    if (debugLevel >= 3)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Creating lock file: " + dbLockFilename);
                    }

                    // Create lock file
                    var success = CreateLockFile(dbLockFilename);
                    if (!success)
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (debugLevel >= 2)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                            "Creating indexed database file: " + dbTrieFilenameBeforeShuffle);
                    }

                    // Set up and execute a program runner to run PrepDB.py
                    var arguments = " " + prebDBScriptPath + " FASTA " + dbfileNameInput;
                    if (debugLevel >= 1)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, pythonProgLoc + " " + arguments);
                    }

                    if (!prepDB.RunProgram(pythonProgLoc, arguments, "PrepDB", true))
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogDb, BaseLogger.LogLevels.ERROR,
                            "Error running " + PREPDB_SCRIPT + " for " + dbfileNameInput + " : " + jobNum);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (debugLevel >= 1)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                                          "Created .trie file for " + dbfileNameInput);
                    }

                    if (useShuffledDB)
                    {
                        // Set up and execute a program runner to run ShuffleDB_seed.py
                        var shuffleDB = new RunDosProgram(inspectDir + Path.DirectorySeparatorChar, debugLevel);
                        shuffleDB.ErrorEvent += CmdRunner_ErrorEvent;

                        arguments = " " + ShuffleDBScriptPath + " -r " + dbTrieFilenameBeforeShuffle + " -w " + dbTrieFilename;

                        if (shuffleDBPreventRepeats)
                        {
                            arguments += " -p";
                        }

                        if (randomNumberSeed != 0)
                        {
                            arguments += " -d " + randomNumberSeed;
                        }

                        if (debugLevel >= 1)
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, pythonProgLoc + " " + arguments);
                        }

                        if (!shuffleDB.RunProgram(pythonProgLoc, arguments, "ShuffleDB", true))
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogDb, BaseLogger.LogLevels.ERROR,
                                "Error running " + SHUFFLEDB_SCRIPT + " for " + dbTrieFilenameBeforeShuffle + " : " + jobNum);
                            return CloseOutType.CLOSEOUT_FAILED;
                        }

                        if (debugLevel >= 1)
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                                              "Shuffled .trie file created: " + dbTrieFilename);
                        }
                    }

                    if (debugLevel >= 3)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Deleting lock file: " + dbLockFilename);
                    }

                    // Delete the lock file
                    if (File.Exists(dbLockFilename))
                    {
                        File.Delete(dbLockFilename);
                    }
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                    "CreateInspectIndexedDB.CreateIndexedDbFiles, An exception has occurred: " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Creates a lock file
        /// </summary>
        /// <returns>True if success; false if failure</returns>
        private bool CreateLockFile(string lockFilePath)
        {
            try
            {
                using var writer = new StreamWriter(lockFilePath);

                // Add Date and time to the file.
                writer.WriteLine(DateTime.Now);
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                    "CreateInspectIndexedDB.CreateLockFile, Error creating lock file: " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Event handler for event CmdRunner.ErrorEvent
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex"></param>
        private void CmdRunner_ErrorEvent(string message, Exception ex)
        {
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, message);
        }
    }
}
