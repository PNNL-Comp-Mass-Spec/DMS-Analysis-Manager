//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/16/2014
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerPBFGenerator
{
    /// <summary>
    /// Class for creation PBF (PNNL Binary Format) files using PBFGen
    /// </summary>
    public class AnalysisToolRunnerPBFGenerator : AnalysisToolRunnerBase
    {
        private const string PBF_GEN_CONSOLE_OUTPUT = "PBFGen_ConsoleOutput.txt";

        private const int PROGRESS_PCT_STARTING = 1;
        private const int PROGRESS_PCT_COMPLETE = 99;

        private string mConsoleOutputErrorMsg;
        private long mInstrumentFileSizeBytes;
        private string mResultsFilePath;

        private string mPbfFormatVersion;

        private DirectoryInfo mMSXmlCacheFolder;

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

                if (mDebugLevel > 4)
                {
                    LogDebug("AnalysisToolRunnerPBFGenerator.RunTool(): Enter");
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

                var msXMLCacheFolderPath = mMgrParams.GetParam("MSXMLCacheFolderPath", string.Empty);
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

                    var resultsFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_PBF_EXTENSION));

                    if (resultsFile.Exists)
                    {
                        // Success; validate mPbfFormatVersion
                        if (string.IsNullOrEmpty(mPbfFormatVersion))
                            mPbfFormatVersion = string.Empty;

                        var knownVersion = true;
                        string expectedResultsDirectoryPrefix;

                        switch (mPbfFormatVersion)
                        {
                            case "150601":
                                // This version is created by Pbf_Gen.exe v1.0.5311
                                // Make sure the output folder starts with PBF_Gen_1_191
                                // (which will be the case if the settings file has <item key="PbfFormatVersion" value="110569"/>)

                                expectedResultsDirectoryPrefix = "PBF_Gen_1_191";

                                if (!mResultsDirectoryName.StartsWith(expectedResultsDirectoryPrefix))
                                {
                                    processingSuccess = false;
                                }
                                break;

                            case "150604":
                                // This version is created by Pbf_Gen.exe v1.0.5367
                                // Make sure the output folder starts with PBF_Gen_1_193
                                // (which will be the case if the settings file has <item key="PbfFormatVersion" value="150604"/>)
                                expectedResultsDirectoryPrefix = "PBF_Gen_1_193";

                                if (!mResultsDirectoryName.StartsWith(expectedResultsDirectoryPrefix))
                                {
                                    processingSuccess = false;
                                }
                                break;

                            case "150605":
                                // This version is created by Pbf_Gen.exe v1.0.6526
                                // Make sure the output folder starts with PBF_Gen_1_214
                                // (which will be the case if the settings file has <item key="PbfFormatVersion" value="150605"/>)
                                expectedResultsDirectoryPrefix = "PBF_Gen_1_214";

                                if (!mResultsDirectoryName.StartsWith(expectedResultsDirectoryPrefix))
                                {
                                    processingSuccess = false;
                                }
                                break;

                            case "150608":
                                // This version is created by Pbf_Gen.exe v1.0.5714
                                // Make sure the output folder starts with PBF_Gen_1_243
                                // (which will be the case if the settings file has <item key="PbfFormatVersion" value="150608"/>)
                                expectedResultsDirectoryPrefix = "PBF_Gen_1_243";

                                if (!mResultsDirectoryName.StartsWith(expectedResultsDirectoryPrefix))
                                {
                                    processingSuccess = false;
                                }
                                break;

                            default:
                                expectedResultsDirectoryPrefix = "?undefined?";

                                processingSuccess = false;
                                knownVersion = false;
                                break;
                        }

                        if (!processingSuccess)
                        {
                            if (knownVersion)
                            {
                                LogError("Unrecognized results directory prefix (starts with {0} instead of {1}). " +
                                         "Either create a new Settings file with PbfFormatVersion {2} or update the version listed in the current, default settings file; " +
                                         "next, delete the job from the DMS_Pipeline database then update the job to use the new settings file (or reset the job)", mResultsDirectoryName, expectedResultsDirectoryPrefix, mPbfFormatVersion);
                            }
                            else
                            {
                                LogError(string.Format(
                                    "Unrecognized PbfFormatVersion. " +
                                    "Update file AnalysisToolRunnerPBFGenerator.cs in the PBFSpectraFileGen Plugin of the Analysis Manager to add version {0}; " +
                                    "next, reset the failed job step",
                                    mPbfFormatVersion));
                            }
                        }
                        else
                        {
                            // Copy the .pbf file to the MSXML cache
                            var remoteCacheFilePath = CopyFileToServerCache(mMSXmlCacheFolder.FullName, resultsFile.FullName, purgeOldFilesIfNeeded: true);

                            if (string.IsNullOrEmpty(remoteCacheFilePath))
                            {
                                if (string.IsNullOrEmpty(mMessage))
                                {
                                    LogError("CopyFileToServerCache returned false for " + resultsFile.Name);
                                }
                                processingSuccess = false;
                            }

                            // Create the _CacheInfo.txt file
                            var cacheInfoFilePath = resultsFile.FullName + "_CacheInfo.txt";
                            using (var writer = new StreamWriter(new FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                            {
                                writer.WriteLine(remoteCacheFilePath);
                            }

                            mJobParams.AddResultFileToSkip(resultsFile.Name);
                        }
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(mMessage))
                        {
                            LogError("PBF_Gen results file not found: " + resultsFile.Name);
                            processingSuccess = false;
                        }
                    }
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                PRISM.AppUtils.GarbageCollectNow();

                if (!processingSuccess)
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                mJobParams.AddResultFileExtensionToSkip("_ConsoleOutput.txt");

                var success = CopyResultsToTransferDirectory();

                return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                mMessage = "Error in AnalysisToolRunnerPBFGenerator->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_PBF_EXTENSION);
            mJobParams.AddResultFileExtensionToSkip(AnalysisResources.DOT_RAW_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory();
        }

        // ReSharper disable once GrammarMistakeInComment

        /// <summary>
        /// Computes a crude estimate of % complete based on the input dataset file size and the file size of the result file
        /// This will always vastly underestimate the progress since the PBF file is always smaller than the .raw file
        /// Furthermore, it looks like all of the data in the .raw file is cached in memory and the .PBF file is not created until the very end
        /// and thus this progress estimation is useless
        /// </summary>
        private float EstimatePBFProgress()
        {
            try
            {
                var results = new FileInfo(mResultsFilePath);

                if (results.Exists && mInstrumentFileSizeBytes > 0)
                {
                    var percentComplete = results.Length / (float)mInstrumentFileSizeBytes * 100;
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
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example Console output

            // Creating E:\DMS_WorkDir\Synocho_L2_1.pbf from E:\DMS_WorkDir\Synocho_L2_1.raw
            // PbfFormatVersion: 150601

            try
            {
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

                mConsoleOutputErrorMsg = string.Empty;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (!string.IsNullOrWhiteSpace(dataLine))
                        {
                            var dataLineLCase = dataLine.ToLower();

                            if (dataLineLCase.StartsWith("error:") || dataLineLCase.Contains("unhandled exception"))
                            {
                                if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                {
                                    mConsoleOutputErrorMsg = "Error running PBFGen:";
                                }
                                mConsoleOutputErrorMsg += "; " + dataLine;
                                continue;
                            }

                            if (dataLineLCase.StartsWith("PbfFormatVersion:".ToLower()))
                            {
                                // Parse out the version number
                                mPbfFormatVersion = dataLine.Substring("PbfFormatVersion:".Length).Trim();
                            }
                        }
                    }
                }

                var progressComplete = EstimatePBFProgress();

                if (mProgress < progressComplete)
                {
                    mProgress = progressComplete;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + consoleOutputFilePath + ")", ex);
                }
            }
        }

        private bool StartPBFFileCreation(string progLoc)
        {
            mConsoleOutputErrorMsg = string.Empty;

            var rawDataTypeName = mJobParams.GetJobParameter("RawDataType", "");
            var rawDataType = AnalysisResources.GetRawDataType(rawDataTypeName);

            if (rawDataType != AnalysisResources.RawDataTypeConstants.ThermoRawFile)
            {
                LogError("PBF generation presently only supports Thermo .Raw files");
                return false;
            }

            var rawFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_RAW_EXTENSION);

            // Cache the size of the instrument data file
            var instrumentFile = new FileInfo(rawFilePath);

            if (!instrumentFile.Exists)
            {
                LogError("Instrument data not found: " + rawFilePath);
                return false;
            }

            mInstrumentFileSizeBytes = instrumentFile.Length;
            mPbfFormatVersion = string.Empty;

            // Cache the full path to the expected output file
            mResultsFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_PBF_EXTENSION);

            LogMessage("Running PBFGen to create the PBF file");

            // Set up and execute a program runner to run PBFGen
            var arguments = " -s " + rawFilePath;

            // arguments += " -o " + mWorkDir

            if (mDebugLevel >= 1)
            {
                LogDebug(progLoc + arguments);
            }

            var cmdRunner = new RunDosProgram(mWorkDir, mDebugLevel);
            RegisterEvents(cmdRunner);
            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            cmdRunner.CreateNoWindow = true;
            cmdRunner.CacheStandardOutput = false;
            cmdRunner.EchoOutputToConsole = true;

            cmdRunner.WriteConsoleOutputToFile = true;
            cmdRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, PBF_GEN_CONSOLE_OUTPUT);

            mProgress = PROGRESS_PCT_STARTING;

            var success = cmdRunner.RunProgram(progLoc, arguments, "PbfGen", true);

            if (!cmdRunner.WriteConsoleOutputToFile)
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

            // Parse the console output file one more time to check for errors and to update mPbfFormatVersion
            Global.IdleLoop(0.25);
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

            mProgress = PROGRESS_PCT_COMPLETE;
            mStatusTools.UpdateAndWrite(mProgress);

            if (mDebugLevel >= 3)
            {
                LogDebug("PBF Generation Complete");
            }

            return true;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string progLoc)
        {
            var additionalDLLs = new List<string>
            {
                "InformedProteomics.Backend.dll"
            };

            var success = StoreDotNETToolVersionInfo(progLoc, additionalDLLs, true);

            return success;
        }

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            UpdateStatusFile();

            // Parse the console output file and estimate progress every 15 seconds
            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(Path.Combine(mWorkDir, PBF_GEN_CONSOLE_OUTPUT));

                LogProgress("PBFGenerator");
            }
        }
    }
}
