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

namespace AnalysisManagerInSpecTPlugIn
{
    public class clsCreateInspectIndexedDB
    {
        /// <summary>
        /// Convert .Fasta file to indexed DB files
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType CreateIndexedDbFiles(ref IMgrParams mgrParams, ref IJobParams jobParams, int DebugLevel, int JobNum, string InspectDir, string OrgDbDir)
        {
            const float MAX_WAITTIME_HOURS = 1.0f;
            const float MAX_WAITTIME_PREVENT_REPEATS = 2.0f;

            const string PREPDB_SCRIPT = "PrepDB.py";
            const string SHUFFLEDB_SCRIPT = "ShuffleDB_Seed.py";

            var sngMaxWaitTimeHours = MAX_WAITTIME_HOURS;

            try
            {
                if (DebugLevel > 4)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                        "clsCreateInspectIndexedDB.CreateIndexedDbFiles(): Enter");
                }

                var intRandomNumberSeed = jobParams.GetJobParameter("InspectShuffleDBSeed", 1000);
                var blnShuffleDBPreventRepeats = jobParams.GetJobParameter("InspectPreventShuffleDBRepeats", false);

                if (blnShuffleDBPreventRepeats)
                {
                    sngMaxWaitTimeHours = MAX_WAITTIME_PREVENT_REPEATS;
                }

                var strDBFileNameInput = Path.Combine(OrgDbDir, jobParams.GetParam("PeptideSearch", "generatedFastaName"));
                var blnUseShuffledDB = jobParams.GetJobParameter("InspectUsesShuffledDB", false);

                var strOutputNameBase = Path.GetFileNameWithoutExtension(strDBFileNameInput);
                var dbTrieFilenameBeforeShuffle = Path.Combine(OrgDbDir, strOutputNameBase + ".trie");

                if (blnUseShuffledDB)
                {
                    // Will create the .trie file using PrepDB.py, then shuffle it using shuffleDB.py
                    // The Pvalue.py script does much better at computing p-values if a decoy search is performed (i.e. shuffleDB.py is used)
                    // Note that shuffleDB will add a prefix of XXX to the shuffled protein names
                    strOutputNameBase += "_shuffle";
                }

                var dbLockFilename = Path.Combine(OrgDbDir, strOutputNameBase + "_trie.lock");
                var dbTrieFilename = Path.Combine(OrgDbDir, strOutputNameBase + ".trie");

                var pythonProgLoc = mgrParams.GetParam("pythonprogloc");

                var objPrepDB = new clsRunDosProgram(InspectDir + Path.DirectorySeparatorChar, DebugLevel);
                objPrepDB.ErrorEvent += CmdRunner_ErrorEvent;

                // Check to see if another Analysis Manager is already creating the indexed db files
                if (File.Exists(dbLockFilename))
                {
                    if (DebugLevel >= 1)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                            "Lock file found: " + dbLockFilename + "; waiting for file to be removed by other manager generating .trie file " +
                            Path.GetFileName(dbTrieFilename));
                    }

                    // Lock file found; wait up to sngMaxWaitTimeHours hours
                    var fi = new FileInfo(dbLockFilename);
                    var createTime = fi.CreationTimeUtc;
                    var currentTime = DateTime.UtcNow;
                    var durationTime = currentTime - createTime;
                    while (File.Exists(dbLockFilename) && durationTime.Hours < sngMaxWaitTimeHours)
                    {
                        // Sleep for 2 seconds
                        clsGlobal.IdleLoop(2);

                        // Update the current time and elapsed duration
                        currentTime = DateTime.UtcNow;
                        durationTime = currentTime - createTime;
                    }

                    // If the duration time has exceeded sngMaxWaitTimeHours, delete the lock file and try again with this manager
                    if (durationTime.Hours > sngMaxWaitTimeHours)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.WARN,
                            "Waited over " + sngMaxWaitTimeHours.ToString("0.0") + " hour(s) for lock file: " + dbLockFilename +
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
                    // Try to create the index files for fasta file strDBFileNameInput

                    // Verify that python program file exists
                    if (!File.Exists(pythonProgLoc))
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                            "Cannot find python.exe program file: " + pythonProgLoc);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    // Verify that the PrepDB python script exists
                    var PrebDBScriptPath = Path.Combine(InspectDir, PREPDB_SCRIPT);
                    if (!File.Exists(PrebDBScriptPath))
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                            "Cannot find PrepDB script: " + PrebDBScriptPath);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    // Verify that the ShuffleDB python script exists
                    var ShuffleDBScriptPath = Path.Combine(InspectDir, SHUFFLEDB_SCRIPT);
                    if (blnUseShuffledDB)
                    {
                        if (!File.Exists(ShuffleDBScriptPath))
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                                "Cannot find ShuffleDB script: " + ShuffleDBScriptPath);
                            return CloseOutType.CLOSEOUT_FAILED;
                        }
                    }

                    if (DebugLevel >= 3)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Creating lock file: " + dbLockFilename);
                    }

                    // Create lock file
                    var bSuccess = CreateLockFile(dbLockFilename);
                    if (!bSuccess)
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (DebugLevel >= 2)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                            "Creating indexed database file: " + dbTrieFilenameBeforeShuffle);
                    }

                    // Set up and execute a program runner to run PrepDB.py
                    var arguments = " " + PrebDBScriptPath + " FASTA " + strDBFileNameInput;
                    if (DebugLevel >= 1)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, pythonProgLoc + " " + arguments);
                    }

                    if (!objPrepDB.RunProgram(pythonProgLoc, arguments, "PrepDB", true))
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogDb, BaseLogger.LogLevels.ERROR,
                            "Error running " + PREPDB_SCRIPT + " for " + strDBFileNameInput + " : " + JobNum);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    if (DebugLevel >= 1)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                                          "Created .trie file for " + strDBFileNameInput);
                    }

                    if (blnUseShuffledDB)
                    {
                        // Set up and execute a program runner to run ShuffleDB_seed.py
                        var objShuffleDB = new clsRunDosProgram(InspectDir + Path.DirectorySeparatorChar, DebugLevel);
                        objShuffleDB.ErrorEvent += CmdRunner_ErrorEvent;

                        arguments = " " + ShuffleDBScriptPath + " -r " + dbTrieFilenameBeforeShuffle + " -w " + dbTrieFilename;

                        if (blnShuffleDBPreventRepeats)
                        {
                            arguments += " -p";
                        }

                        if (intRandomNumberSeed != 0)
                        {
                            arguments += " -d " + intRandomNumberSeed;
                        }

                        if (DebugLevel >= 1)
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, pythonProgLoc + " " + arguments);
                        }

                        if (!objShuffleDB.RunProgram(pythonProgLoc, arguments, "ShuffleDB", true))
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogDb, BaseLogger.LogLevels.ERROR,
                                "Error running " + SHUFFLEDB_SCRIPT + " for " + dbTrieFilenameBeforeShuffle + " : " + JobNum);
                            return CloseOutType.CLOSEOUT_FAILED;
                        }

                        if (DebugLevel >= 1)
                        {
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG,
                                              "Shuffled .trie file created: " + dbTrieFilename);
                        }
                    }

                    if (DebugLevel >= 3)
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
                    "clsCreateInspectIndexedDB.CreateIndexedDbFiles, An exception has occurred: " + ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Creates a lock file
        /// </summary>
        /// <returns>True if success; false if failure</returns>
        protected bool CreateLockFile(string strLockFilePath)
        {
            try
            {
                using (var writer = new StreamWriter(strLockFilePath))
                {
                    // Add Date and time to the file.
                    writer.WriteLine(DateTime.Now);
                }
            }
            catch (Exception ex)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                    "clsCreateInspectIndexedDB.CreateLockFile, Error creating lock file: " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Event handler for event CmdRunner.ErrorEvent
        /// </summary>
        /// <param name="strMessage"></param>
        /// <param name="ex"></param>
        private void CmdRunner_ErrorEvent(string strMessage, Exception ex)
        {
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, strMessage);
        }
    }
}
