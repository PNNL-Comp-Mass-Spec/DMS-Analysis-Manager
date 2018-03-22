/*****************************************************************
** Written by Matthew Monroe for the US Department of Energy    **
** Pacific Northwest National Laboratory, Richland, WA          **
** Created 08/08/2017                                           **
**                                                              **
*****************************************************************/

using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;

namespace AnalysisManagerUIMFtoMassHunterPlugin
{
    /// <summary>
    /// Class for running the UIMF to MassHunter converter
    /// </summary>
    public class clsAnalysisToolRunnerUIMFtoMassHunter : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        private const float PROGRESS_PCT_STARTING = 5;
        private const float PROGRESS_PCT_COMPLETE = 99;

        private const string UIMF_CONVERTER_CONSOLE_OUTPUT = "UIMFtoMassHunter_ConsoleOutput.txt";

        #endregion

        #region "Module Variables"

        private string mConsoleOutputFile;
        private string mConsoleOutputErrorMsg;

        private string mUIMFConverterProgLoc;

        private DateTime mLastConsoleOutputParse;
        private DateTime mLastProgressWriteTime;

        #endregion

        #region "Methods"

        /// <summary>
        /// Converts .UIMF files to Agilent .D folders
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
                    LogDebug("clsAnalysisToolRunnerUIMFtoMassHunter.RunTool(): Enter");
                }

                // Determine the path to the UimfToMassHunter program
                mUIMFConverterProgLoc = DetermineProgramLocation("UimfToMassHunterProgLoc", "UimfToMassHunter.exe");

                if (string.IsNullOrWhiteSpace(mUIMFConverterProgLoc))
                {
                    // Error has already been logged
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the UimfToMassHunter version info in the database
                if (!StoreToolVersionInfo(mUIMFConverterProgLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    m_message = "Error determining UIMFtoMassHunter version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Initialize classwide variables
                mLastConsoleOutputParse = DateTime.UtcNow;
                mLastProgressWriteTime = DateTime.UtcNow;

                var processingSuccess = ConvertToAgilentDotD();

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Could use the following to create a summary file:
                // Add the current job data to the summary file
                // UpdateSummaryFile();

                // Make sure objects are released
                PRISM.clsProgRunner.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // No need to keep several files; exclude them now
                m_jobParams.AddResultFileToSkip(m_jobParams.GetParam("ParmFileName"));

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Error in UIMFtoMassHunterPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        private bool ConvertToAgilentDotD()
        {
            // Set up and execute a program runner to run the Metabolite Detector

            // Valid dataset type
            var uimfFileName = m_Dataset + ".uimf";
            var uimfFilePath = clsAnalysisResources.ResolveStoragePath(m_WorkDir, uimfFileName);

            if (string.IsNullOrWhiteSpace(uimfFilePath))
            {
                LogError("Cannot convert; UIMF file not found in " + m_WorkDir);
                return false;
            }

            var uimfFile = new FileInfo(uimfFilePath);
            if (!uimfFile.Exists)
            {
                LogError("UIMF file not found: " + uimfFile.FullName);
                return false;
            }

            var cmdStr = "UimfToMassHunter.exe " + clsGlobal.PossiblyQuotePath(uimfFile.FullName);

            if (m_DebugLevel >= 1)
            {
                LogDebug(cmdStr);
            }

            mConsoleOutputFile = Path.Combine(m_WorkDir, UIMF_CONVERTER_CONSOLE_OUTPUT);

            var cmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = false,
                ConsoleOutputFilePath = mConsoleOutputFile
            };
            RegisterEvents(cmdRunner);
            cmdRunner.LoopWaiting += cmdRunner_LoopWaiting;

            m_progress = PROGRESS_PCT_STARTING;

            var success = cmdRunner.RunProgram(mUIMFConverterProgLoc, cmdStr, "UIMFtoMassHunter", true);

            if (!cmdRunner.WriteConsoleOutputToFile && cmdRunner.CachedConsoleOutput.Length > 0)
            {
                // Write the console output to a text file
                clsGlobal.IdleLoop(0.25);

                var swConsoleOutputfile = new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));
                swConsoleOutputfile.WriteLine(cmdRunner.CachedConsoleOutput);
                swConsoleOutputfile.Close();
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            clsGlobal.IdleLoop(0.25);

            // Parse the ConsoleOutput file to look for errors
            ParseConsoleOutputFile(mConsoleOutputFile);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (success)
            {
                return true;
            }

            m_message = "Error running UIMFtoMassHunter";

            LogError(m_message + ", job " + m_JobNum);

            if (cmdRunner.ExitCode != 0)
            {
                LogWarning("UIMFtoMassHunter returned a non-zero exit code: " + cmdRunner.ExitCode);
            }
            else
            {
                LogWarning("UIMFtoMassHunter failed (but exit code is 0)");
            }

            return false;
        }

        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            // Example Console output
            //
            // ...
            //

            try
            {
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

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                        {
                            continue;
                        }

                        if (strLineIn.StartsWith("error ", StringComparison.OrdinalIgnoreCase))
                        {
                            StoreConsoleErrorMessage(srInFile, strLineIn);
                        }
                    }
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

        private void StoreConsoleErrorMessage(StreamReader srInFile, string strLineIn)
        {
            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                mConsoleOutputErrorMsg = "Error running UIMFtoMassHunter:";
            }
            mConsoleOutputErrorMsg += "; " + strLineIn;

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

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string progLoc)
        {

            var additionalDLLs = new List<string>
            {
                "UIMFLibrary.dll"
            };

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDLLs);

            return success;
        }
        #endregion

        #region "Event Handlers"

        void cmdRunner_LoopWaiting()
        {

            // Synchronize the stored Debug level with the value stored in the database

            {
                UpdateStatusFile();

                // Parse the console output file every 15 seconds
                if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
                {
                    mLastConsoleOutputParse = DateTime.UtcNow;

                    ParseConsoleOutputFile(Path.Combine(m_WorkDir, mConsoleOutputFile));

                    LogProgress("UIMFtoMassHunter");
                }
            }

        }

        #endregion
    }
}
