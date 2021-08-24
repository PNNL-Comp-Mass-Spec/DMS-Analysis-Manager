﻿//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/29/2011
//
//*********************************************************************************************************

using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.OfflineJobs;

namespace AnalysisManagerMSGFDBPlugIn
{
    /// <summary>
    /// Class for running MS-GF+ analysis
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerMSGFDB : AnalysisToolRunnerBase
    {
        // Ignore Spelling: centroided, conf, Gzip, mzid, mzml, mzxml, Utils, Xmx

        private enum InputFileFormatTypes
        {
            Unknown = 0,
            CDTA = 1,
            MzXML = 2,
            MzML = 3,
            MGF = 4
        }

        private readonly List<string> mResultFilesToSkipIfNoError = new();

        private bool mToolVersionWritten;

        /// <summary>
        /// Path to MSGFPlus.jar
        /// </summary>
        private string mMSGFPlusProgLoc;

        /// <summary>
        /// This will be set to True if the parameter file has TDA=1, meaning MS-GF+ auto-added decoy proteins to its list of candidate proteins
        /// When TDA is 1, the FASTA must only contain normal (forward) protein sequences
        /// </summary>
        private bool mResultsIncludeAutoAddedDecoyPeptides;

        private string mWorkingDirectoryInUse;

        private bool mMSGFPlusComplete;
        private DateTime mMSGFPlusCompletionTime;
        private double mMSGFPlusRunTimeMinutes;

        private MSGFPlusUtils mMSGFPlusUtils;

        private RunDosProgram mCmdRunner;

        /// <summary>
        /// Runs MS-GF+ tool (aka MSGF+)
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
                    LogDebug("AnalysisToolRunnerMSGFDB.RunTool(): Enter");
                }

                // Verify that program files exist

                // javaProgLoc will typically be "C:\DMS_Programs\Java\jre8\bin\java.exe"
                var javaProgLoc = GetJavaProgLoc();
                if (string.IsNullOrEmpty(javaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run MS-GF+ (includes indexing the FASTA file)

                var processingResult = RunMSGFPlus(javaProgLoc, out var mzidResultsFile, out var processingError, out var tooManySkippedSpectra);
                if (processingResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Unknown error running MS-GF+";
                    }

                    // If the MSGFPlus_ConsoleOutput.txt file or the .mzid file exist, we want to move them to the failed results folder
                    mzidResultsFile.Refresh();

                    DirectoryInfo workingDirectory;

                    if (string.IsNullOrEmpty(mWorkingDirectoryInUse))
                    {
                        workingDirectory = new DirectoryInfo(mWorkDir);
                    }
                    else
                    {
                        workingDirectory = new DirectoryInfo(mWorkingDirectoryInUse);
                    }

                    var consoleOutputFile = workingDirectory.GetFiles(MSGFPlusUtils.MSGFPLUS_CONSOLE_OUTPUT_FILE);

                    if (!mzidResultsFile.Exists && consoleOutputFile.Length == 0)
                    {
                        return processingResult;
                    }
                }

                // Look for the .mzid file
                // If it exists, call PostProcessMSGFPlusResults even if processingError is true

                mzidResultsFile.Refresh();
                if (mzidResultsFile.Exists)
                {
                    // Look for a "dirty" mzid file
                    var dirtyResultsFilename = Path.GetFileNameWithoutExtension(mzidResultsFile.Name) + "_dirty.gz";

                    bool dirtyFileExists;
                    if (mzidResultsFile.Directory == null)
                    {
                        dirtyFileExists = false;
                    }
                    else
                    {
                        var dirtyResultFile = new FileInfo(Path.Combine(mzidResultsFile.Directory.FullName, dirtyResultsFilename));
                        dirtyFileExists = dirtyResultFile.Exists;
                    }

                    if (dirtyFileExists)
                    {
                        mMessage = "MS-GF+ _dirty.gz file found; this indicates a processing error";
                        processingError = true;
                    }
                    else
                    {
                        var postProcessingResult = PostProcessMSGFPlusResults(mzidResultsFile.Name);
                        if (postProcessingResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            if (string.IsNullOrEmpty(mMessage))
                            {
                                mMessage = "Unknown error post-processing the MS-GF+ results";
                            }

                            processingError = true;
                            if (processingResult == CloseOutType.CLOSEOUT_SUCCESS)
                                processingResult = postProcessingResult;
                        }
                    }
                }
                else
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "MS-GF+ results file not found: " + mzidResultsFile.Name;
                        processingError = true;
                    }
                }

                if (!mMSGFPlusComplete)
                {
                    processingError = true;
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("MS-GF+ did not reach completion");
                    }
                }

                if (!string.IsNullOrWhiteSpace(mMSGFPlusUtils.EnzymeDefinitionFilePath))
                {
                    // Move the enzymes.txt file into the working directory
                    var enzymesFile = new FileInfo(mMSGFPlusUtils.EnzymeDefinitionFilePath);
                    var newEnzymesFile = new FileInfo(Path.Combine(mWorkingDirectoryInUse, enzymesFile.Name));
                    if (!string.Equals(enzymesFile.FullName, newEnzymesFile.FullName))
                    {
                        if (newEnzymesFile.Exists && enzymesFile.Exists)
                            newEnzymesFile.Delete();

                        if (enzymesFile.Exists)
                            enzymesFile.MoveTo(newEnzymesFile.FullName);
                    }
                }

                mProgress = MSGFPlusUtils.PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                // Make sure objects are released
                Global.IdleLoop(0.5);
                PRISM.ProgRunner.GarbageCollectNow();

                if (processingError || !AnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveDirectory();
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var success = CopyResultsToTransferDirectory();
                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                if (tooManySkippedSpectra)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                return processingResult;
            }
            catch (Exception ex)
            {
                LogError("Error in MSGFDbPlugin->RunTool: " + ex.Message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Index the FASTA file (if needed) then run MS-GF+
        /// </summary>
        /// <param name="javaProgLoc"></param>
        /// <param name="mzidResultsFile">MS-GF+ results file</param>
        /// <param name="processingError"></param>
        /// <param name="tooManySkippedSpectra"></param>
        private CloseOutType RunMSGFPlus(
            string javaProgLoc,
            out FileInfo mzidResultsFile,
            out bool processingError,
            out bool tooManySkippedSpectra)
        {
            mzidResultsFile = new FileInfo(Path.Combine(mWorkDir, Dataset + "_msgfplus.mzid"));

            var splitFastaEnabled = mJobParams.GetJobParameter("SplitFasta", false);
            if (splitFastaEnabled)
            {
                var iteration = AnalysisResources.GetSplitFastaIteration(mJobParams, out var errorMessage);
                if (!string.IsNullOrWhiteSpace(errorMessage))
                    mMessage = errorMessage;

                var splitFastaMzidFile = new FileInfo(
                    Path.Combine(mWorkDir, Dataset + "_msgfplus_Part" + iteration + ".mzid"));

                if (!mzidResultsFile.Exists && splitFastaMzidFile.Exists)
                {
                    splitFastaMzidFile.MoveTo(mzidResultsFile.FullName);
                    mzidResultsFile.Refresh();
                }
            }

            processingError = false;
            tooManySkippedSpectra = false;

            // Determine the path to MS-GF+
            // Manager parameter MSGFPlusProgLoc will either come from the Manager Control database,
            // or on Linux from file ManagerSettingsLocal.xml
            mMSGFPlusProgLoc = DetermineProgramLocation("MSGFPlusProgLoc", MSGFPlusUtils.MSGFPLUS_JAR_NAME);

            if (string.IsNullOrWhiteSpace(mMSGFPlusProgLoc))
            {
                // Returning CLOSEOUT_FAILED will cause the plugin to immediately exit; results and console output files will not be saved
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Note: we will store the MS-GF+ version info in the database after the first line is written to file MSGFPlus_ConsoleOutput.txt
            mToolVersionWritten = false;

            mMSGFPlusComplete = false;

            mResultFilesToSkipIfNoError.Clear();

            var result = DetermineInputFileFormat(true, out var eInputFileFormat, out var assumedScanType, out var scanTypeFilePath);
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Immediately exit the plugin; results and console output files will not be saved
                return result;
            }

            // Initialize mMSGFPlusUtils
            mMSGFPlusUtils = new MSGFPlusUtils(mMgrParams, mJobParams, mWorkDir, mDebugLevel);
            RegisterEvents(mMSGFPlusUtils);

            mMSGFPlusUtils.IgnorePreviousErrorEvent += MSGFPlusUtils_IgnorePreviousErrorEvent;

            // Get the FASTA file and index it if necessary
            // Passing in the path to the parameter file so we can look for TDA=0 when using large .Fasta files
            var parameterFilePath = Path.Combine(mWorkDir, mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE));
            var javaExePath = javaProgLoc;
            var msgfPlusJarFilePath = mMSGFPlusProgLoc;

            result = mMSGFPlusUtils.InitializeFastaFile(
                javaExePath, msgfPlusJarFilePath,
                out var fastaFileSizeKB, out var fastaFileIsDecoy,
                out var fastaFilePath, parameterFilePath);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                if (string.IsNullOrWhiteSpace(mMessage) && !string.IsNullOrWhiteSpace(mMSGFPlusUtils.ErrorMessage))
                {
                    mMessage = mMSGFPlusUtils.ErrorMessage;
                }

                // Immediately exit the plugin; results and console output files will not be saved
                return result;
            }

            var instrumentGroup = mJobParams.GetJobParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "InstrumentGroup", string.Empty);

            // Read the MSGFPlus Parameter File and optionally create a new one with customized parameters
            // paramFile will contain the path to either the original parameter file or the customized one
            result = mMSGFPlusUtils.ParseMSGFPlusParameterFile(
                fastaFileIsDecoy, assumedScanType, scanTypeFilePath,
                instrumentGroup, parameterFilePath,
                out var sourceParamFile, out var finalParamFile);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                if (string.IsNullOrWhiteSpace(mMessage) && !string.IsNullOrWhiteSpace(mMSGFPlusUtils.ErrorMessage))
                {
                    mMessage = mMSGFPlusUtils.ErrorMessage;
                }

                // Immediately exit the plugin; results and console output files will not be saved
                return result;
            }

            if (finalParamFile == null)
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Problem parsing MS-GF+ parameter file";
                }
                // Immediately exit the plugin; results and console output files will not be saved
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!string.Equals(sourceParamFile.FullName, finalParamFile.FullName))
            {
                // Do not copy the original parameter file to the results directory
                // Example name to skip: MSGFPlus_Tryp_MetOx_20ppmParTol.original
                AddResultFileToSkipIfNoError(sourceParamFile.Name);
            }

            // This will be set to True if the parameter file contains both TDA=1 and showDecoy=1
            mResultsIncludeAutoAddedDecoyPeptides = mMSGFPlusUtils.ResultsIncludeAutoAddedDecoyPeptides;

            LogMessage("Running MS-GF+");

            string inputFileName;
            string inputFileDescription;

            switch (eInputFileFormat)
            {
                case InputFileFormatTypes.CDTA:
                    inputFileName = Dataset + AnalysisResources.CDTA_EXTENSION;
                    inputFileDescription = "CDTA (_dta.txt) file";
                    break;

                case InputFileFormatTypes.MGF:
                    inputFileName = Dataset + AnalysisResources.DOT_MGF_EXTENSION;
                    inputFileDescription = ".mgf file";
                    break;

                case InputFileFormatTypes.MzML:
                    inputFileName = Dataset + AnalysisResources.DOT_MZML_EXTENSION;
                    inputFileDescription = ".mzML file";
                    break;

                case InputFileFormatTypes.MzXML:
                    inputFileName = Dataset + AnalysisResources.DOT_MZXML_EXTENSION;
                    inputFileDescription = ".mzXML file";
                    break;

                default:
                    LogError("Unsupported InputFileFormat: " + eInputFileFormat);
                    // Immediately exit the plugin; results and console output files will not be saved
                    return CloseOutType.CLOSEOUT_FAILED;
            }

            var inputFile = new FileInfo(Path.Combine(mWorkDir, inputFileName));

            // If an MS-GF+ analysis crashes with an "out-of-memory" error, we need to reserve more memory for Java.
            // The amount of memory required depends on both the FASTA file size and the size of the input data file (_dta.txt or .mzML)
            //   since data from all spectra are cached in memory.
            // Customize this on a per-job basis using the MSGFDBJavaMemorySize setting in the settings file
            // (job 611216 succeeded with a value of 5000)

            // Prior to January 2016, MS-GF+ used 4 to 7 threads, and if MSGFDBJavaMemorySize was too small,
            // we ran the risk of one thread crashing and the results files missing the search results for the spectra assigned to that thread
            // For large _dta.txt files, 2000 MB of memory could easily be small enough to result in crashing threads
            // Consequently, the default is now 4000 MB
            //
            // Furthermore, the 2016-Jan-21 release uses 128 search tasks (or 10 tasks per thread if over 12 threads),
            // executing the tasks via a pool, meaning the memory overhead of each thread is lower vs. previous versions that
            // had large numbers of tasks on a small, finite number of threads

            // Setting MSGFDBJavaMemorySize is stored in the settings file for this job

            var javaMemorySizeMB = mJobParams.GetJobParameter("MSGFDBJavaMemorySize", 4000);
            if (javaMemorySizeMB < 512)
                javaMemorySizeMB = 512;

            // Possibly increase the Java memory size based on the size of the FASTA file
            var fastaBasedMinimumJavaMemoryMB = 7.5 * fastaFileSizeKB / 1024.0 + 1000;

            // Possibly increase the Java memory size based on the size of the spectrum file
            var spectraBasedMinimumJavaMemoryMB = 3 * Global.BytesToMB(inputFile.Length) + 2250;

            int minimumJavaMemoryMB;
            string warningMsg;
            if (fastaBasedMinimumJavaMemoryMB > javaMemorySizeMB && fastaBasedMinimumJavaMemoryMB > spectraBasedMinimumJavaMemoryMB)
            {
                minimumJavaMemoryMB = (int)Math.Ceiling(fastaBasedMinimumJavaMemoryMB / 500.0) * 500;
                warningMsg = string.Format("Increasing Java memory size from {0:N0} MB to {1:N0} MB due to large FASTA file ({2:N0} MB)",
                    javaMemorySizeMB, minimumJavaMemoryMB, fastaFileSizeKB / 1024.0);
            }
            else if (spectraBasedMinimumJavaMemoryMB > javaMemorySizeMB)
            {
                minimumJavaMemoryMB = (int)Math.Ceiling(spectraBasedMinimumJavaMemoryMB / 500.0) * 500;
                warningMsg = string.Format("Increasing Java memory size from {0:N0} MB to {1:N0} MB due to large {2} ({3:N0} MB)",
                    javaMemorySizeMB, minimumJavaMemoryMB, inputFileDescription, Global.BytesToMB(inputFile.Length));
            }
            else
            {
                minimumJavaMemoryMB = javaMemorySizeMB;
                warningMsg = string.Empty;
            }

            if (javaMemorySizeMB < minimumJavaMemoryMB)
            {
                LogWarning(warningMsg);
                javaMemorySizeMB = minimumJavaMemoryMB;
            }

            // Set up and execute a program runner to run MS-GF+
            var arguments = " -Xmx" + javaMemorySizeMB + "M -jar " + msgfPlusJarFilePath;

            // Define the input file, output file, and FASTA file
            // It is safe to simply use the input file name since the working directory will be mWorkDir
            arguments += " -s " + inputFile.Name;
            arguments += " -o " + mzidResultsFile.Name;
            arguments += " -d " + PossiblyQuotePath(fastaFilePath);

            // Append the MS-GF+ parameter file name
            arguments += " -conf " + finalParamFile.Name;

            // Make sure the machine has enough free memory to run MSGFPlus
            var logFreeMemoryOnSuccess = (mDebugLevel >= 1);

            if (!AnalysisResources.ValidateFreeMemorySize(javaMemorySizeMB, "MS-GF+", logFreeMemoryOnSuccess))
            {
                mMessage = "Not enough free memory to run MS-GF+";
                // Immediately exit the plugin; results and console output files will not be saved
                return CloseOutType.CLOSEOUT_FAILED;
            }

            mWorkingDirectoryInUse = mWorkDir;

            bool validExistingResults;
            if (mzidResultsFile.Exists)
            {
                // Don't actually run MS-GF+ if the results file exists and ends in </MzIdentML>
                validExistingResults = MSGFPlusUtils.MSGFPlusResultsFileHasClosingTag(mzidResultsFile);
                if (validExistingResults)
                {
                    LogMessage(string.Format("Using existing MS-GF+ results: {0} created {1}",
                        mzidResultsFile.Name, mzidResultsFile.LastWriteTime.ToString(DATE_TIME_FORMAT)));
                }
                else
                {
                    LogMessage(string.Format("Deleting incomplete existing MS-GF+ results: {0} created {1}",
                        mzidResultsFile.Name, mzidResultsFile.LastWriteTime.ToString(DATE_TIME_FORMAT)));
                    mzidResultsFile.Delete();
                    mzidResultsFile.Refresh();
                }
            }
            else
            {
                validExistingResults = false;
            }

            var success = validExistingResults || StartMSGFPlusLocal(javaExePath, arguments);

            if (!success && string.IsNullOrEmpty(mMSGFPlusUtils.ConsoleOutputErrorMsg))
            {
                // Wait 2 seconds to give the log file a chance to finalize
                Global.IdleLoop(2);

                // Parse the console output file one more time in hopes of finding an error message
                ParseConsoleOutputFile(mWorkingDirectoryInUse);
            }

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mMSGFPlusUtils.MSGFPlusVersion))
                {
                    ParseConsoleOutputFile(mWorkingDirectoryInUse);
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mMSGFPlusUtils.ConsoleOutputErrorMsg))
            {
                LogMessage(mMSGFPlusUtils.ConsoleOutputErrorMsg, 1, true);
            }

            mzidResultsFile.Refresh();

            if (success)
            {
                if (!mMSGFPlusComplete)
                {
                    mMSGFPlusComplete = true;
                }
            }
            else
            {
                if (mMSGFPlusComplete)
                {
                    LogError("MS-GF+ log file reported it was complete, but aborted the ProgRunner since Java was frozen");
                }
                else
                {
                    LogError("Error running MS-GF+");
                }

                if (mMSGFPlusComplete)
                {
                    // Don't treat this as a fatal error
                    mEvalMessage = mMessage;
                    mMessage = string.Empty;
                }
                else
                {
                    processingError = true;
                }

                if (!mMSGFPlusComplete)
                {
                    if (mCmdRunner.ExitCode != 0)
                    {
                        LogWarning("MS-GF+ returned a non-zero exit code: " + mCmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to MS-GF+ failed (but exit code is 0)");
                    }
                }
            }

            if (mMSGFPlusComplete)
            {
                if (mMSGFPlusUtils.TaskCountCompleted < mMSGFPlusUtils.TaskCountTotal)
                {
                    var savedCountCompleted = mMSGFPlusUtils.TaskCountCompleted;

                    // MS-GF+ finished, but the log file doesn't report that all of the threads finished
                    // Wait 5 more seconds, then parse the log file again
                    // Keep checking and waiting for up to 45 seconds

                    LogWarning("MS-GF+ finished, but the log file reports " + mMSGFPlusUtils.TaskCountCompleted + " / " + mMSGFPlusUtils.TaskCountTotal +
                               " completed tasks");

                    var waitStartTime = DateTime.UtcNow;
                    while (DateTime.UtcNow.Subtract(waitStartTime).TotalSeconds < 45)
                    {
                        Global.IdleLoop(5);
                        mMSGFPlusUtils.ParseMSGFPlusConsoleOutputFile(mWorkingDirectoryInUse);

                        if (mMSGFPlusUtils.TaskCountCompleted == mMSGFPlusUtils.TaskCountTotal)
                        {
                            break;
                        }
                    }

                    if (mMSGFPlusUtils.TaskCountCompleted == mMSGFPlusUtils.TaskCountTotal)
                    {
                        LogMessage(string.Format(
                            "Re-parsing the MS-GF+ log file now indicates that all tasks finished (waited {0:0} seconds)",
                            DateTime.UtcNow.Subtract(waitStartTime).TotalSeconds));
                    }
                    else if (mMSGFPlusUtils.TaskCountCompleted > savedCountCompleted)
                    {
                        LogWarning(string.Format(
                            "Re-parsing the MS-GF+ log file now indicates that {0} tasks finished. " +
                            "That is an increase over the previous value but still not all {1} tasks",
                            mMSGFPlusUtils.TaskCountCompleted, mMSGFPlusUtils.TaskCountTotal));
                    }
                    else
                    {
                        LogWarning("Re-parsing the MS-GF+ log file indicated the same number of completed tasks");
                    }
                }

                if (mMSGFPlusUtils.TaskCountCompleted < mMSGFPlusUtils.TaskCountTotal)
                {
                    if (mMSGFPlusUtils.TaskCountCompleted == mMSGFPlusUtils.TaskCountTotal - 1)
                    {
                        // All but one of the tasks finished
                        LogWarning(
                            "MS-GF+ finished, but the logs indicate that one of the " + mMSGFPlusUtils.TaskCountTotal + " tasks did not complete; " +
                            "this could indicate an error", true);
                    }
                    else
                    {
                        // 2 or more tasks did not finish
                        mMSGFPlusComplete = false;
                        LogError("MS-GF+ finished, but the logs are incomplete, showing " + mMSGFPlusUtils.TaskCountCompleted + " / " +
                                 mMSGFPlusUtils.TaskCountTotal + " completed search tasks");

                        // Do not return CLOSEOUT_FAILED, as that causes the plugin to immediately exit; results and console output files would not be saved in that case
                        // Instead, set processingError to true
                        processingError = true;
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }
                }
            }
            else
            {
                if (mMSGFPlusUtils.TaskCountCompleted > 0)
                {
                    var msg = mMessage;
                    if (string.IsNullOrWhiteSpace(msg))
                    {
                        msg = "MS-GF+ processing failed";
                    }
                    msg += "; logs show " + mMSGFPlusUtils.TaskCountCompleted + " / " + mMSGFPlusUtils.TaskCountTotal + " completed search tasks";
                    LogError(msg);
                }

                // Do not return CLOSEOUT_FAILED, as that causes the plugin to immediately exit; results and console output files would not be saved in that case
                // Instead, set processingError to true
                processingError = true;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            mProgress = MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE;
            mStatusTools.UpdateAndWrite(mProgress);
            LogMessage("MS-GF+ Search Complete", 3);

            if (mMSGFPlusUtils.ContinuumSpectraSkipped > 0)
            {
                // See if any spectra were processed
                if (!mzidResultsFile.Exists)
                {
                    // Note that DMS stored procedure AutoResetFailedJobs looks for jobs with these phrases in the job comment
                    //   "None of the spectra are centroided; unable to process"
                    //   "skipped xx% of the spectra because they did not appear centroided"
                    //   "skip xx% of the spectra because they did not appear centroided"
                    //
                    // Failed jobs that are found to have this comment will have their settings files auto-updated and the job will auto-reset

                    LogError(AnalysisResources.SPECTRA_ARE_NOT_CENTROIDED + " with MS-GF+");
                    processingError = true;
                }
                else
                {
                    // Compute the fraction of all potential spectra that were skipped
                    // If over 20% of the spectra were skipped, and if the source spectra were not centroided,
                    //   then tooManySkippedSpectra will be set to True and the job step will be marked as failed

                    var spectraAreCentroided =
                        mJobParams.GetJobParameter("CentroidMSXML", false) ||
                        mJobParams.GetJobParameter("CentroidDTAs", false) ||
                        mJobParams.GetJobParameter("CentroidMGF", false);

                    var fractionSkipped = mMSGFPlusUtils.ContinuumSpectraSkipped /
                                             (double)(mMSGFPlusUtils.ContinuumSpectraSkipped + mMSGFPlusUtils.SpectraSearched);
                    var percentSkipped = (fractionSkipped * 100).ToString("0.0") + "%";

                    if (fractionSkipped > 0.2 && !spectraAreCentroided)
                    {
                        LogError("MS-GF+ skipped " + percentSkipped + " of the spectra because they did not appear centroided");
                        tooManySkippedSpectra = true;
                    }
                    else
                    {
                        LogWarning(
                            "MS-GF+ processed some of the spectra, but it skipped " +
                            mMSGFPlusUtils.ContinuumSpectraSkipped + " spectra that were not centroided " +
                            "(" + percentSkipped + " skipped)", true);
                    }
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool StartMSGFPlusLocal(string javaExePath, string arguments)
        {
            if (mDebugLevel >= 1)
            {
                LogMessage(javaExePath + " " + arguments);
            }

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = Path.Combine(mWorkDir, MSGFPlusUtils.MSGFPLUS_CONSOLE_OUTPUT_FILE)
            };
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mProgress = MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            return mCmdRunner.RunProgram(javaExePath, arguments, "MS-GF+", true);
        }

        private void AddResultFileToSkipIfNoError(string fileName)
        {
            mResultFilesToSkipIfNoError.Add(fileName);
        }

        /// <summary>
        /// Convert the .mzid file created by MS-GF+ to a .tsv file
        /// </summary>
        /// <param name="mzidFileName"></param>
        /// <returns>The name of the .tsv file if successful; empty string if an error</returns>
        private string ConvertMZIDToTSV(string mzidFileName)
        {
            // Determine the path to the MzidToTsvConverter
            // Manager parameter MzidToTsvConverterProgLoc will either come from the Manager Control database,
            // or on Linux from file ManagerSettingsLocal.xml
            var mzidToTsvConverterProgLoc = DetermineProgramLocation("MzidToTsvConverterProgLoc", "MzidToTsvConverter.exe");

            if (string.IsNullOrEmpty(mzidToTsvConverterProgLoc))
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    LogError("Parameter 'MzidToTsvConverter' not defined for this manager");
                }
                return string.Empty;
            }

            var tsvFilePath = mMSGFPlusUtils.ConvertMZIDToTSV(mzidToTsvConverterProgLoc, Dataset, mzidFileName);

            if (string.IsNullOrEmpty(tsvFilePath))
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    LogError("Error calling mMSGFPlusUtils.ConvertMZIDToTSV; path not returned");
                }
                return string.Empty;
            }

            var splitFastaEnabled = mJobParams.GetJobParameter("SplitFasta", false);

            if (splitFastaEnabled)
            {
                var tsvFileName = ParallelMSGFPlusRenameFile(Path.GetFileName(tsvFilePath));
                return tsvFileName;
            }

            return Path.GetFileName(tsvFilePath);
        }

        /// <summary>
        /// Copy failed results to the local archive folder
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            // Try to save whatever files are in the work directory (however, delete any spectral data files)

            mJobParams.AddResultFileToSkip(Dataset + AnalysisResources.CDTA_ZIPPED_EXTENSION);
            mJobParams.AddResultFileToSkip(Dataset + AnalysisResources.CDTA_EXTENSION);

            mJobParams.AddResultFileToSkip(Dataset + AnalysisResources.DOT_RAW_EXTENSION);
            mJobParams.AddResultFileToSkip(Dataset + AnalysisResources.DOT_MZML_EXTENSION);
            mJobParams.AddResultFileToSkip(Dataset + AnalysisResources.DOT_MGF_EXTENSION);

            base.CopyFailedResultsToArchiveDirectory();
        }

        /// <summary>
        /// Make the local results directory, move files into that directory, then copy the files to the transfer directory on the Proto-x server
        /// </summary>
        /// <returns>True if success, otherwise false</returns>
        public override bool CopyResultsToTransferDirectory(string transferDirectoryPathOverride = "")
        {
            foreach (var fileName in mResultFilesToSkipIfNoError)
            {
                mJobParams.AddResultFileToSkip(fileName);
            }

            return base.CopyResultsToTransferDirectory(transferDirectoryPathOverride);
        }

        private bool CreateScanTypeFile(out string scanTypeFilePath)
        {
            var scanTypeFileCreator = new ScanTypeFileCreator(mWorkDir, Dataset);

            scanTypeFilePath = string.Empty;

            if (scanTypeFileCreator.CreateScanTypeFile())
            {
                if (mDebugLevel >= 1)
                {
                    LogMessage("Created ScanType file: " + Path.GetFileName(scanTypeFileCreator.ScanTypeFilePath));
                }
                scanTypeFilePath = scanTypeFileCreator.ScanTypeFilePath;
                return true;
            }

            var errorMessage = "Error creating scan type file: " + scanTypeFileCreator.ErrorMessage;
            var detailedMessage = string.Empty;

            if (!string.IsNullOrEmpty(scanTypeFileCreator.ExceptionDetails))
            {
                detailedMessage += "; " + scanTypeFileCreator.ExceptionDetails;
            }

            LogError(errorMessage, detailedMessage);
            return false;
        }

        /// <summary>
        /// Determine the scan type and input file format
        /// Does not validate the _dta.txt file or create the _ScanType.txt file; simply populates the output parameters
        /// </summary>
        /// <param name="eInputFileFormat">Input file format enum</param>
        /// <param name="assumedScanType">Assumed scan type (only applicable for CDTA files)</param>
        private void DetermineInputFileFormat(out InputFileFormatTypes eInputFileFormat, out string assumedScanType)
        {
            DetermineInputFileFormat(false, out eInputFileFormat, out assumedScanType, out _);
        }

        /// <summary>
        /// Determine the scan type and input file format
        /// </summary>
        /// <param name="validateCdtaAndCreateScanTypeFile">
        /// When true, if the file format is CDTA, validate the _dta.txt file and create the ScanType file if required
        /// When false, simply populate the output variables
        /// </param>
        /// <param name="eInputFileFormat">Output: input file format enum</param>
        /// <param name="assumedScanType">
        /// Output: assumed scan type (only applicable for CDTA files)
        /// Comes from job param AssumedScanType, which is typically not defined, meaning a _ScanType.txt file needs to be created</param>
        /// <param name="scanTypeFilePath">Output: scan type file path (if one is created)</param>
        private CloseOutType DetermineInputFileFormat(
            bool validateCdtaAndCreateScanTypeFile,
            out InputFileFormatTypes eInputFileFormat,
            out string assumedScanType,
            out string scanTypeFilePath)
        {
            assumedScanType = string.Empty;

            // The ToolName job parameter holds the name of the job script we are executing
            var scriptName = mJobParams.GetParam("ToolName");
            scanTypeFilePath = string.Empty;

            if (scriptName.IndexOf("mzxml", StringComparison.OrdinalIgnoreCase) >= 0 ||
                scriptName.IndexOf("msgfplus_bruker", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                eInputFileFormat = InputFileFormatTypes.MzXML;
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            if (scriptName.IndexOf("mzml", StringComparison.OrdinalIgnoreCase) >= 0 ||
                scriptName.IndexOf("DeconMSn_MzRefinery", StringComparison.OrdinalIgnoreCase) > 0)
            {
                eInputFileFormat = InputFileFormatTypes.MzML;
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            assumedScanType = mJobParams.GetParam("AssumedScanType");

            var mgfFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_MGF_EXTENSION));
            if (mgfFile.Exists)
            {
                eInputFileFormat = InputFileFormatTypes.MGF;
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            eInputFileFormat = InputFileFormatTypes.CDTA;

            if (!validateCdtaAndCreateScanTypeFile)
                return CloseOutType.CLOSEOUT_SUCCESS;

            // Make sure the _DTA.txt file is valid
            if (!ValidateCDTAFile())
            {
                return CloseOutType.CLOSEOUT_NO_DTA_FILES;
            }

            if (string.IsNullOrWhiteSpace(assumedScanType))
            {
                // Create the ScanType file (lists scan type for each scan number)
                if (!CreateScanTypeFile(out scanTypeFilePath))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Generate an MS-GF+ results file name using the dataset name, the given suffix, and optionally some additional text
        /// </summary>
        /// <param name="fileNamePrefix">Prefix text (may be an empty string)</param>
        /// <param name="fileName"></param>
        /// <param name="fileNameSuffix">Suffix text (may be an empty string)</param>
        private string GenerateResultFileName(string fileNamePrefix, string fileName, string fileNameSuffix)
        {
            return fileNamePrefix + Path.GetFileNameWithoutExtension(fileName) +
                   fileNameSuffix + Path.GetExtension(fileName);
        }

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        private void MonitorProgress()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            // Parse the console output file every 30 seconds
            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds < SECONDS_BETWEEN_UPDATE)
            {
                return;
            }

            mLastConsoleOutputParse = DateTime.UtcNow;

            ParseConsoleOutputFile(mWorkingDirectoryInUse);
            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mMSGFPlusUtils.MSGFPlusVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("MS-GF+");

            if (mProgress < MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE)
                return;

            if (!mMSGFPlusComplete)
            {
                mMSGFPlusComplete = true;
                mMSGFPlusCompletionTime = DateTime.UtcNow;
                mMSGFPlusRunTimeMinutes = Math.Max(1, mCmdRunner?.RunTime.TotalMinutes ?? 1);
            }
            else
            {
                // Wait a minimum of 5 minutes for Java to finish
                // Wait longer for jobs that have been running longer
                var waitTimeMinutes = (int)Math.Ceiling(Math.Max(5, Math.Sqrt(mMSGFPlusRunTimeMinutes)));

                if (DateTime.UtcNow.Subtract(mMSGFPlusCompletionTime).TotalMinutes < waitTimeMinutes)
                    return;

                // MS-GF+ is finished but hasn't exited after 5 minutes (longer for long-running jobs)
                // If there is a large number results, we need to given MS-GF+ time to sort them prior to writing to disk
                // However, it is also possible that Java frozen and thus the process should be aborted

                var warningMessage = "MS-GF+ has been stuck at " +
                                     MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE.ToString("0") + "% complete for " + waitTimeMinutes + " minutes; " +
                                     "aborting since Java appears frozen";

                LogWarning(warningMessage);

                // Bump up mMSGFPlusCompletionTime by one hour
                // This will prevent this function from logging the above message every 30 seconds if the .abort command fails
                mMSGFPlusCompletionTime = mMSGFPlusCompletionTime.AddHours(1);
                mCmdRunner.AbortProgramNow();
            }
        }

        /// <summary>
        /// Renames the results file created by a Parallel MS-GF+ instance to have _Part##.mzid as a suffix
        /// </summary>
        /// <param name="resultsFileName"></param>
        /// <returns>The path to the new file if success, otherwise the original filename</returns>
        private string ParallelMSGFPlusRenameFile(string resultsFileName)
        {
            var filePathNew = "??";

            try
            {
                var resultsFile = new FileInfo(Path.Combine(mWorkDir, resultsFileName));

                var iteration = AnalysisResources.GetSplitFastaIteration(mJobParams, out var errorMessage);
                if (!string.IsNullOrWhiteSpace(errorMessage))
                    mMessage = errorMessage;

                var fileNameNew = Path.GetFileNameWithoutExtension(resultsFile.Name) + "_Part" + iteration + resultsFile.Extension;

                if (!resultsFile.Exists)
                    return resultsFileName;

                filePathNew = Path.Combine(mWorkDir, fileNameNew);

                if (File.Exists(filePathNew))
                    File.Delete(filePathNew);

                resultsFile.MoveTo(filePathNew);

                return fileNameNew;
            }
            catch (Exception ex)
            {
                LogError("Error renaming file " + resultsFileName + " to " + filePathNew, ex);
                return resultsFileName;
            }
        }

        /// <summary>
        /// Parse the MSGFPlus console output file to determine the MSGFPlus version and to track the search progress
        /// </summary>
        private void ParseConsoleOutputFile(string workingDirectory)
        {
            try
            {
                if (mMSGFPlusUtils == null)
                    return;

                var msgfPlusProgress = mMSGFPlusUtils.ParseMSGFPlusConsoleOutputFile(workingDirectory);
                if (msgfPlusProgress > 0)
                {
                    mProgress = msgfPlusProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogMessage("Error parsing console output file: " + ex.Message, 2, true);
                }
            }
        }

        /// <summary>
        /// Convert the .mzid file to a TSV file and create the PeptideToProtein map file (Dataset_msgfplus_PepToProtMap.txt)
        /// </summary>
        /// <remarks>Assumes that the calling function has verified that resultsFileName exists</remarks>
        /// <param name="resultsFileName"></param>
        /// <returns>True if success, false if an error</returns>
        private CloseOutType PostProcessMSGFPlusResults(string resultsFileName)
        {
            var currentTask = "Starting";

            try
            {
                var splitFastaEnabled = mJobParams.GetJobParameter("SplitFasta", false);

                if (splitFastaEnabled)
                {
                    currentTask = "Calling ParallelMSGFPlusRenameFile for " + resultsFileName;
                    resultsFileName = ParallelMSGFPlusRenameFile(resultsFileName);

                    currentTask = "Calling ParallelMSGFPlusRenameFile for MSGFPlus_ConsoleOutput.txt";
                    ParallelMSGFPlusRenameFile("MSGFPlus_ConsoleOutput.txt");
                }

                // Gzip the output file
                currentTask = "Zipping " + resultsFileName;
                var result = mMSGFPlusUtils.ZipOutputFile(this, resultsFileName);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                string msgfPlusResultsFileName;
                var extension = Path.GetExtension(resultsFileName);
                if (extension != null && string.Equals(extension, ".mzid", StringComparison.OrdinalIgnoreCase))
                {
                    // Convert the .mzid file to a .tsv file

                    currentTask = "Calling ConvertMZIDToTSV";
                    UpdateStatusRunning(MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_CONVERT_MZID_TO_TSV);
                    msgfPlusResultsFileName = ConvertMZIDToTSV(resultsFileName);

                    if (string.IsNullOrEmpty(msgfPlusResultsFileName))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                else
                {
                    msgfPlusResultsFileName = resultsFileName;
                }

                var skipPeptideToProteinMapping = mJobParams.GetJobParameter("SkipPeptideToProteinMapping", false);

                if (skipPeptideToProteinMapping)
                {
                    LogMessage("Skipping PeptideToProteinMapping since job parameter SkipPeptideToProteinMapping is True");
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                // Examine the MS-GF+ TSV file to see if it's empty
                using (var reader = new StreamReader(new FileStream(Path.Combine(mWorkDir, msgfPlusResultsFileName), FileMode.Open, FileAccess.Read,
                                                                    FileShare.ReadWrite)))
                {
                    var dataLines = 0;
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            continue;
                        }

                        dataLines++;
                        if (dataLines > 2)
                            break;
                    }

                    if (dataLines <= 1)
                    {
                        LogWarning("MS-GF+ did not identify any peptides (TSV file is empty)", true);
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }
                }

                // Create the Peptide to Protein map file, Dataset_msgfplus_PepToProtMap.txt

                UpdateStatusRunning(MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_MAPPING_PEPTIDES_TO_PROTEINS);

                var localOrgDbFolder = mMgrParams.GetParam(AnalysisResources.MGR_PARAM_ORG_DB_DIR);
                currentTask = "Calling CreatePeptideToProteinMapping";
                result = mMSGFPlusUtils.CreatePeptideToProteinMapping(msgfPlusResultsFileName, mResultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder);
                if (!AnalysisJob.SuccessOrNoData(result))
                {
                    return result;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in PostProcessMSGFPlusResults (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        public override CloseOutType PostProcessRemoteResults()
        {
            var result = base.PostProcessRemoteResults();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
                return result;

            // Read the MS-GF+ Console_Output file and look for "seconds elapsed", "minutes elapsed", and "hours elapsed" entries
            // If the elapsed time from the job status file is more than 10% shorter than MS-GF+ runtime, use the MS-GF+ runtime instead
            // This will be the case if the analysis manager crashes while MS-GF+ is running but MS-GF+ actually finishes and another manager uses the existing results
            // Also require the MS-GF+ progress to be over 95%
            try
            {
                if (mMSGFPlusUtils == null)
                {
                    // Initialize mMSGFPlusUtils
                    mMSGFPlusUtils = new MSGFPlusUtils(mMgrParams, mJobParams, mWorkDir, mDebugLevel);
                    RegisterEvents(mMSGFPlusUtils);
                }

                var msgfPlusProgress = mMSGFPlusUtils.ParseMSGFPlusConsoleOutputFile(mWorkDir);
                if (msgfPlusProgress < MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE - 5)
                {
                    LogWarning(string.Format(
                        "Progress from the MS-GF+ console output file is {0:F0}, " +
                        "which is much less than the expected value of {1:F0}; " +
                        "will not compare to the RemoteStart and RemoteFinish job parameters",
                        msgfPlusProgress, MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE));

                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                if (mMSGFPlusUtils.ElapsedTimeHours * 60 <= 1)
                {
                    LogWarning(string.Format(
                        "Processing time from the MS-GF+ console output file is {0:F1} minutes; " +
                        "will not compare to the RemoteStart and RemoteFinish job parameters",
                        mMSGFPlusUtils.ElapsedTimeHours * 60));

                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                var remoteStartText = mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, RemoteTransferUtility.STEP_PARAM_REMOTE_START, "");
                var remoteFinishText = mJobParams.GetJobParameter(AnalysisJob.STEP_PARAMETERS_SECTION, RemoteTransferUtility.STEP_PARAM_REMOTE_FINISH, "");

                if (string.IsNullOrWhiteSpace(remoteStartText) || string.IsNullOrWhiteSpace(remoteFinishText))
                    return CloseOutType.CLOSEOUT_SUCCESS;

                if (!DateTime.TryParse(remoteStartText, out var remoteStart) ||
                    !DateTime.TryParse(remoteFinishText, out var remoteFinish))
                {
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                var elapsedTimeHoursFromStatusFile = remoteFinish.Subtract(remoteStart).TotalHours;

                if (elapsedTimeHoursFromStatusFile > mMSGFPlusUtils.ElapsedTimeHours * 0.9)
                    return CloseOutType.CLOSEOUT_SUCCESS;

                LogMessage(string.Format(
                    "Updating the RemoteStart and RemoteFinish times based on the processing time reported in the MS-GF+ console output file; " +
                    "changing from {0:F1} hours to {1:F1} hours",
                    elapsedTimeHoursFromStatusFile, mMSGFPlusUtils.ElapsedTimeHours));

                var newRemoteStart = remoteFinish.AddHours(-mMSGFPlusUtils.ElapsedTimeHours);

                // Update the remote start time, using format code "{0:O}" to format as "2018-04-17T10:30:59.0000000"
                mJobParams.AddAdditionalParameter(AnalysisJob.STEP_PARAMETERS_SECTION,
                                                   RemoteTransferUtility.STEP_PARAM_REMOTE_START,
                                                   string.Format("{0:O}", newRemoteStart));

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error post-processing MS-GF+ results retrieved from the remote processor", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Retrieve MS-GF+ results that were run remotely
        /// </summary>
        /// <param name="transferUtility">Transfer utility</param>
        /// <param name="verifyCopied">Log warnings if any files are missing.  When false, logs debug messages instead</param>
        /// <param name="retrievedFilePaths">Local paths of retrieved files</param>
        /// <returns>True on success, otherwise false</returns>
        public override bool RetrieveRemoteResults(RemoteTransferUtility transferUtility, bool verifyCopied, out List<string> retrievedFilePaths)
        {
            try
            {
                var paramFileName = mJobParams.GetParam(AnalysisResources.JOB_PARAM_PARAMETER_FILE);
                var modDefsFile = new FileInfo(Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(paramFileName) + "_ModDefs.txt"));

                // Keys in this dictionary are file names, values are true if the file is required, or false if it's optional
                var filesToRetrieve = new Dictionary<string, bool>
                {
                    {"Mass_Correction_Tags.txt", true},
                    {modDefsFile.Name, true},
                    {MSGFPlusUtils.MOD_FILE_NAME, false},     // MSGFPlus_Mods.txt likely will not exist since we switched to using -conf in February 2019
                    {paramFileName, true},
                    {mToolVersionUtilities.ToolVersionInfoFile, true}
                };

                DetermineInputFileFormat(out var eInputFileFormat, out var assumedScanType);

                if (eInputFileFormat == InputFileFormatTypes.CDTA && string.IsNullOrWhiteSpace(assumedScanType))
                {
                    // The ScanType.txt file was created; retrieve it
                    filesToRetrieve.Add(Dataset + "_ScanType.txt", true);
                }

                string addOn;
                var splitFastaEnabled = mJobParams.GetJobParameter("SplitFasta", false);
                if (splitFastaEnabled)
                {
                    var iteration = AnalysisResources.GetSplitFastaIteration(mJobParams, out var errorMessage);
                    if (!string.IsNullOrWhiteSpace(errorMessage))
                        mMessage = errorMessage;

                    addOn = "_Part" + iteration;
                }
                else
                {
                    addOn = string.Empty;
                }

                filesToRetrieve.Add(Dataset + "_msgfplus" + addOn + ".mzid.gz", true);
                filesToRetrieve.Add(Dataset + "_msgfplus" + addOn + "_PepToProtMap.txt", true);

                var tsvResultsFile = GenerateResultFileName(Dataset, MSGFPlusUtils.MSGFPLUS_TSV_SUFFIX, addOn);
                var consoleOutputFile = GenerateResultFileName(string.Empty, MSGFPlusUtils.MSGFPLUS_CONSOLE_OUTPUT_FILE, addOn);

                filesToRetrieve.Add(tsvResultsFile, true);
                filesToRetrieve.Add(consoleOutputFile, true);

                var success = RetrieveRemoteResultFiles(transferUtility, filesToRetrieve, verifyCopied, out retrievedFilePaths);

                return success;
            }
            catch (Exception ex)
            {
                retrievedFilePaths = new List<string>();
                LogError("Error in RetrieveRemoteResults", ex);
                return false;
            }
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            LogMessage("Determining tool version info", 2);

            var toolVersionInfo = mMSGFPlusUtils.MSGFPlusVersion;

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(mMSGFPlusProgLoc)
            };

            try
            {
                // Need to pass saveToolVersionTextFile to True so that the ToolVersionInfo file gets created
                // The PeptideListToXML program uses that file when creating .pepXML files
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {
            MonitorProgress();
        }

        private void MSGFPlusUtils_IgnorePreviousErrorEvent(string messageToIgnore)
        {
            mMessage = mMessage.Replace(messageToIgnore, string.Empty).Trim(';', ' ');
        }
    }
}
