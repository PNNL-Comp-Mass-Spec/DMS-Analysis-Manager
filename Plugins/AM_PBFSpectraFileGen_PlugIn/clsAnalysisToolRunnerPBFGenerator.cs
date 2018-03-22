//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/16/2014
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using AnalysisManagerBase;
using System.IO;

namespace AnalysisManagerPBFGenerator
{
    /// <summary>
    /// Class for creation PBF (PNNL Binary Format) files using PBFGen
    /// </summary>
    public class clsAnalysisToolRunnerPBFGenerator : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        protected const string PBF_GEN_CONSOLE_OUTPUT = "PBFGen_ConsoleOutput.txt";
        protected const float PROGRESS_PCT_STARTING = 1;

        protected const float PROGRESS_PCT_COMPLETE = 99;

        #endregion

        #region "Module Variables"

        protected string mConsoleOutputErrorMsg;
        private long mInstrumentFileSizeBytes;
        protected string mResultsFilePath;

        protected string mPbfFormatVersion;

        protected DirectoryInfo mMSXmlCacheFolder;

        #endregion

        #region "Methods"

        /// <summary>
        /// Generates a PBF file for the dataset
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
                    LogDebug("clsAnalysisToolRunnerPBFGenerator.RunTool(): Enter");
                }

                // Determine the path to the PbfGen program
                var progLoc = DetermineProgramLocation("PbfGenProgLoc", "PbfGen.exe");

                if (string.IsNullOrWhiteSpace(progLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Store the PBFGen version info in the database
                if (!StoreToolVersionInfo(progLoc))
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    LogError("Error determining PBFGen version");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var msXMLCacheFolderPath = m_mgrParams.GetParam("MSXMLCacheFolderPath", string.Empty);
                mMSXmlCacheFolder = new DirectoryInfo(msXMLCacheFolderPath);

                if (!mMSXmlCacheFolder.Exists)
                {
                    LogError("MSXmlCache folder not found: " + msXMLCacheFolderPath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Create the PBF file
                var processingSuccess = StartPBFFileCreation(progLoc);

                if (processingSuccess)
                {
                    // Look for the results file

                    var fiResultsFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_PBF_EXTENSION));

                    if (fiResultsFile.Exists)
                    {
                        // Success; validate mPbfFormatVersion
                        if (string.IsNullOrEmpty(mPbfFormatVersion))
                            mPbfFormatVersion = string.Empty;

                        var knownVersion = true;

                        switch (mPbfFormatVersion)
                        {
                            case "150601":
                                // This version is created by Pbf_Gen.exe v1.0.5311
                                // Make sure the output folder starts with PBF_Gen_1_191
                                // (which will be the case if the settings file has <item key="PbfFormatVersion" value="110569"/>)
                                if (!m_ResFolderName.StartsWith("PBF_Gen_1_191"))
                                {
                                    processingSuccess = false;
                                }
                                break;
                            case "150604":
                                // This version is created by Pbf_Gen.exe v1.0.5367
                                // Make sure the output folder starts with PBF_Gen_1_193
                                // (which will be the case if the settings file has <item key="PbfFormatVersion" value="150604"/>)
                                if (!m_ResFolderName.StartsWith("PBF_Gen_1_193"))
                                {
                                    processingSuccess = false;
                                }
                                break;
                            case "150605":
                                // This version is created by Pbf_Gen.exe v1.0.6526
                                // Make sure the output folder starts with PBF_Gen_1_214
                                // (which will be the case if the settings file has <item key="PbfFormatVersion" value="150605"/>)
                                if (!m_ResFolderName.StartsWith("PBF_Gen_1_214"))
                                {
                                    processingSuccess = false;
                                }
                                break;

                            case "150608":
                                // This version is created by Pbf_Gen.exe v1.0.5714
                                // Make sure the output folder starts with PBF_Gen_1_243
                                // (which will be the case if the settings file has <item key="PbfFormatVersion" value="150608"/>)
                                if (!m_ResFolderName.StartsWith("PBF_Gen_1_243"))
                                {
                                    processingSuccess = false;
                                }
                                break;

                            default:
                                processingSuccess = false;
                                knownVersion = false;
                                break;
                        }

                        if (!processingSuccess)
                        {
                            if (knownVersion)
                            {
                                LogError("Unrecognized PbfFormatVersion.  Either create a new Settings file with PbfFormatVersion " + mPbfFormatVersion +
                                         " or update the version listed in the current, default settings file;" +
                                         " next, delete the job from the DMS_Pipeline database then update the job to use the new settings file (or reset the job)");
                            }
                            else
                            {
                                LogError("Unrecognized PbfFormatVersion. Update file clsAnalysisToolRunnerPBFGenerator.cs in the PBFSpectraFileGen Plugin " +
                                         "of the Analysis Manager to add version " + mPbfFormatVersion + "; next, reset the failed job step");
                            }

                        }
                        else
                        {
                            // Copy the .pbf file to the MSXML cache
                            var remoteCachefilePath = CopyFileToServerCache(mMSXmlCacheFolder.FullName, fiResultsFile.FullName, purgeOldFilesIfNeeded: true);

                            if (string.IsNullOrEmpty(remoteCachefilePath))
                            {
                                if (string.IsNullOrEmpty(m_message))
                                {
                                    LogError("CopyFileToServerCache returned false for " + fiResultsFile.Name);
                                }
                                processingSuccess = false;
                            }

                            // Create the _CacheInfo.txt file
                            var cacheInfoFilePath = fiResultsFile.FullName + "_CacheInfo.txt";
                            using (var swOutFile = new StreamWriter(new FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                            {
                                swOutFile.WriteLine(remoteCachefilePath);
                            }

                            m_jobParams.AddResultFileToSkip(fiResultsFile.Name);
                        }
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            LogError("PBF_Gen results file not found: " + fiResultsFile.Name);
                            processingSuccess = false;
                        }
                    }
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

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

                m_jobParams.AddResultFileExtensionToSkip("_ConsoleOutput.txt");

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

            }
            catch (Exception ex)
            {
                m_message = "Error in clsAnalysisToolRunnerPBFGenerator->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveFolder()
        {
            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_PBF_EXTENSION);
            m_jobParams.AddResultFileExtensionToSkip(clsAnalysisResources.DOT_RAW_EXTENSION);

            base.CopyFailedResultsToArchiveFolder();
        }

        /// <summary>
        /// Computes a crude estimate of % complete based on the input dataset file size and the file size of the result file
        /// This will always vastly underestimate the progress since the PBF file is always smaller than the .raw file
        /// Furthermore, it looks like all of the data in the .raw file is cached in memory and the .PBF file is not created until the very end
        ///  and thus this progress estimation is useless
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        protected float EstimatePBFProgress()
        {
            try
            {
                var fiResults = new FileInfo(mResultsFilePath);

                if (fiResults.Exists && mInstrumentFileSizeBytes > 0)
                {
                    var percentComplete = fiResults.Length / (float)mInstrumentFileSizeBytes * 100;
                    return percentComplete;
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            return 0;
        }

        /// <summary>
        /// Parse the PBFGen console output file to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            // Example Console output
            //
            // Creating E:\DMS_WorkDir\Synocho_L2_1.pbf from E:\DMS_WorkDir\Synocho_L2_1.raw
            // PbfFormatVersion: 150601

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

                mConsoleOutputErrorMsg = string.Empty;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (!string.IsNullOrWhiteSpace(strLineIn))
                        {
                            var strLineInLCase = strLineIn.ToLower();

                            if (strLineInLCase.StartsWith("error:") || strLineInLCase.Contains("unhandled exception"))
                            {
                                if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                {
                                    mConsoleOutputErrorMsg = "Error running PBFGen:";
                                }
                                mConsoleOutputErrorMsg += "; " + strLineIn;
                                continue;
                            }

                            if (strLineIn.StartsWith("PbfFormatVersion:"))
                            {
                                // Parse out the version number
                                var strVersion = strLineIn.Substring("PbfFormatVersion:".Length).Trim();

                                mPbfFormatVersion = strVersion;
                            }
                        }
                    }
                }

                var progressComplete = EstimatePBFProgress();

                if (m_progress < progressComplete)
                {
                    m_progress = progressComplete;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + ")", ex);
                }
            }
        }

        protected bool StartPBFFileCreation(string progLoc)
        {
            mConsoleOutputErrorMsg = string.Empty;

            var rawDataType = m_jobParams.GetJobParameter("RawDataType", "");
            var eRawDataType = clsAnalysisResources.GetRawDataType(rawDataType);

            if (eRawDataType != clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile)
            {
                LogError("PBF generation presently only supports Thermo .Raw files");
                return false;
            }

            var rawFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_RAW_EXTENSION);

            // Cache the size of the instrument data file
            var fiInstrumentFile = new FileInfo(rawFilePath);
            if (!fiInstrumentFile.Exists)
            {
                LogError("Instrument data not found: " + rawFilePath);
                return false;
            }

            mInstrumentFileSizeBytes = fiInstrumentFile.Length;
            mPbfFormatVersion = string.Empty;

            // Cache the full path to the expected output file
            mResultsFilePath = Path.Combine(m_WorkDir, m_Dataset + clsAnalysisResources.DOT_PBF_EXTENSION);

            LogMessage("Running PBFGen to create the PBF file");

            // Set up and execute a program runner to run PBFGen
            var cmdStr = " -s " + rawFilePath;

            // cmdStr += " -o " + m_WorkDir

            if (m_DebugLevel >= 1)
            {
                LogDebug(progLoc + cmdStr);
            }

            var cmdRunner = new clsRunDosProgram(m_WorkDir, m_DebugLevel);
            RegisterEvents(cmdRunner);
            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            cmdRunner.CreateNoWindow = true;
            cmdRunner.CacheStandardOutput = false;
            cmdRunner.EchoOutputToConsole = true;

            cmdRunner.WriteConsoleOutputToFile = true;
            cmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, PBF_GEN_CONSOLE_OUTPUT);

            m_progress = PROGRESS_PCT_STARTING;

            var success = cmdRunner.RunProgram(progLoc, cmdStr, "PbfGen", true);

            if (!cmdRunner.WriteConsoleOutputToFile)
            {
                // Write the console output to a text file
                clsGlobal.IdleLoop(0.25);

                using (var swConsoleOutputfile = new StreamWriter(new FileStream(cmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swConsoleOutputfile.WriteLine(cmdRunner.CachedConsoleOutput);
                }
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            // Parse the console output file one more time to check for errors and to update mPbfFormatVersion
            clsGlobal.IdleLoop(0.25);
            ParseConsoleOutputFile(cmdRunner.ConsoleOutputFilePath);

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
            }

            if (!success)
            {
                LogError("Error running PBFGen to create a PBF file");

                if (cmdRunner.ExitCode != 0)
                {
                    LogWarning("PBFGen returned a non-zero exit code: " + cmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to PBFGen failed (but exit code is 0)");
                }

                return false;
            }

            m_progress = PROGRESS_PCT_COMPLETE;
            m_StatusTools.UpdateAndWrite(m_progress);
            if (m_DebugLevel >= 3)
            {
                LogDebug("PBF Generation Complete");
            }

            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        protected bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDLLs = new List<string>
            {
                "InformedProteomics.Backend.dll"
            };

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDLLs);

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

            // Parse the console output file and estimate progress every 15 seconds
            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= 15)
            {
                dtLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(m_WorkDir, PBF_GEN_CONSOLE_OUTPUT));

                LogProgress("PBFGenerator");
            }
        }

        #endregion
    }
}
