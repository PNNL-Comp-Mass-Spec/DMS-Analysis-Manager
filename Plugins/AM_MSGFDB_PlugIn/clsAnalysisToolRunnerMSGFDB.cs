//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 07/29/2011
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using AnalysisManagerBase;

namespace AnalysisManagerMSGFDBPlugIn
{
    /// <summary>
    /// Class for running MSGFDB or MSGF+ analysis
    /// </summary>
    public class clsAnalysisToolRunnerMSGFDB : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        private enum eInputFileFormatTypes
        {
            Unknown = 0,
            CDTA = 1,
            MzXML = 2,
            MzML = 3
        }

        #endregion

        #region "Module Variables"

        private bool mToolVersionWritten;

        // Path to MSGFPlus.jar
        private string mMSGFDbProgLoc;

        private bool mResultsIncludeAutoAddedDecoyPeptides;

        private string mWorkingDirectoryInUse;

        private bool mMSGFPlusComplete;
        private DateTime mMSGFPlusCompletionTime;

        private clsMSGFDBUtils mMSGFDBUtils;

        private clsRunDosProgram mCmdRunner;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs MSGFDB tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public override CloseOutType RunTool()
        {
            try
            {
                //Call base class for initial setup
                if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                if (m_DebugLevel > 4)
                {
                    LogDebug("clsAnalysisToolRunnerMSGFDB.RunTool(): Enter");
                }

                // Verify that program files exist

                // javaProgLoc will typically be "C:\Program Files\Java\jre8\bin\Java.exe"
                var javaProgLoc = GetJavaProgLoc();
                if (string.IsNullOrEmpty(javaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Run MSGF+ (includes indexing the fasta file)

                var processingResult = RunMSGFPlus(javaProgLoc, out var fiMSGFPlusResults, out var processingError, out var tooManySkippedSpectra);
                if (processingResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Unknown error running MSGF+";
                    }

                    // If the MSGFPlus_ConsoleOutput.txt file or the .mzid file exist, we want to move them to the failed results folder
                    fiMSGFPlusResults.Refresh();

                    DirectoryInfo diWorkingDirectory;

                    if (string.IsNullOrEmpty(mWorkingDirectoryInUse))
                    {
                        diWorkingDirectory = new DirectoryInfo(m_WorkDir);
                    }
                    else
                    {
                        diWorkingDirectory = new DirectoryInfo(mWorkingDirectoryInUse);
                    }

                    var fiConsoleOutputFile = diWorkingDirectory.GetFiles(clsMSGFDBUtils.MSGFPLUS_CONSOLE_OUTPUT_FILE);

                    if (!fiMSGFPlusResults.Exists && fiConsoleOutputFile.Length == 0)
                    {
                        return processingResult;
                    }
                }

                // Look for the .mzid file
                // If it exists, then call PostProcessMSGFDBResults even if processingError is true

                fiMSGFPlusResults.Refresh();
                if (fiMSGFPlusResults.Exists)
                {
                    // Look for a "dirty" mzid file
                    var dirtyResultsFilename = Path.GetFileNameWithoutExtension(fiMSGFPlusResults.Name) + "_dirty.gz";
                    var fiMSGFPlusDirtyResults = new FileInfo(Path.Combine(fiMSGFPlusResults.Directory.FullName, dirtyResultsFilename));

                    if (fiMSGFPlusDirtyResults.Exists)
                    {
                        m_message = "MSGF+ _dirty.gz file found; this indicates a processing error";
                        processingError = true;
                    }
                    else
                    {
                        var postProcessingResult = PostProcessMSGFDBResults(fiMSGFPlusResults.Name);
                        if (postProcessingResult != CloseOutType.CLOSEOUT_SUCCESS)
                        {
                            if (string.IsNullOrEmpty(m_message))
                            {
                                m_message = "Unknown error post-processing the MSGF+ results";
                            }

                            processingError = true;
                            if (processingResult == CloseOutType.CLOSEOUT_SUCCESS)
                                processingResult = postProcessingResult;
                        }
                    }
                }
                else
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "MSGF+ results file not found: " + fiMSGFPlusResults.Name;
                        processingError = true;
                    }
                }

                if (!mMSGFPlusComplete)
                {
                    processingError = true;
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("MSGF+ did not reach completion");
                    }
                }

                m_progress = clsMSGFDBUtils.PROGRESS_PCT_COMPLETE;

                //Stop the job timer
                m_StopTime = DateTime.UtcNow;

                //Add the current job data to the summary file
                UpdateSummaryFile();

                //Make sure objects are released
                Thread.Sleep(500);
                PRISM.clsProgRunner.GarbageCollectNow();

                if (processingError || !clsAnalysisJob.SuccessOrNoData(processingResult))
                {
                    // Something went wrong
                    // In order to help diagnose things, we will move whatever files were created into the result folder,
                    //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                    CopyFailedResultsToArchiveFolder();
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
        /// Index the Fasta file (if needed) then run MSGF+
        /// </summary>
        /// <param name="javaProgLoc"></param>
        /// <param name="fiMSGFPlusResults"></param>
        /// <param name="processingError"></param>
        /// <param name="tooManySkippedSpectra"></param>
        /// <returns></returns>
        private CloseOutType RunMSGFPlus(
            string javaProgLoc,
            out FileInfo fiMSGFPlusResults,
            out bool processingError,
            out bool tooManySkippedSpectra)
        {
            var msgfPlusJarfile = clsMSGFDBUtils.MSGFPLUS_JAR_NAME;

            fiMSGFPlusResults = new FileInfo(Path.Combine(m_WorkDir, Dataset + "_msgfplus.mzid"));

            processingError = false;
            tooManySkippedSpectra = false;

            // Determine the path to MSGF+
            // The manager parameter is MSGFDbProgLoc because originally the software was named MSGFDB (aka MS-GFDB)
            mMSGFDbProgLoc = DetermineProgramLocation("MSGFDbProgLoc", strMSGFJarfile);

            if (string.IsNullOrWhiteSpace(mMSGFDbProgLoc))
            {
                // Returning CLOSEOUT_FAILED will cause the plugin to immediately exit; results and console output files will not be saved
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Note: we will store the MSGF+ version info in the database after the first line is written to file MSGFPlus_ConsoleOutput.txt
            mToolVersionWritten = false;

            mMSGFPlusComplete = false;


            var result = DetermineAssumedScanType(out var assumedScanType, out var eInputFileFormat, out var scanTypeFilePath);
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Immediately exit the plugin; results and console output files will not be saved
                return result;
            }

            // Initialize mMSGFDBUtils
            mMSGFDBUtils = new clsMSGFDBUtils(m_mgrParams, m_jobParams, Job, m_WorkDir, m_DebugLevel, msgfPlus: true);
            RegisterEvents(mMSGFDBUtils);

            mMSGFDBUtils.IgnorePreviousErrorEvent += mMSGFDBUtils_IgnorePreviousErrorEvent;

            // Get the FASTA file and index it if necessary
            // Passing in the path to the parameter file so we can look for TDA=0 when using large .Fasta files
            var parameterFilePath = Path.Combine(m_WorkDir, m_jobParams.GetParam("parmFileName"));
            var javaExePath = string.Copy(javaProgLoc);
            var msgfdbJarFilePath = string.Copy(mMSGFDbProgLoc);


            result = mMSGFDBUtils.InitializeFastaFile(javaExePath, msgfdbJarFilePath, out var fastaFileSizeKB, out var fastaFileIsDecoy, out var fastaFilePath,
                                                      strParameterFilePath);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Immediately exit the plugin; results and console output files will not be saved
                return result;
            }

            var instrumentGroup = m_jobParams.GetJobParameter("JobParameters", "InstrumentGroup", string.Empty);

            // Read the MSGFDB Parameter File

            result = mMSGFPlusUtils.ParseMSGFPlusParameterFile(
                fastaFileSizeKB, fastaFileIsDecoy, assumedScanType, scanTypeFilePath,
                instrumentGroup, parameterFilePath, out var msgfPlusCmdLineOptions);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Immediately exit the plugin; results and console output files will not be saved
                return result;
            }

            if (string.IsNullOrEmpty(strMSGFDbCmdLineOptions))
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Problem parsing MSGF+ parameter file";
                }
                // Immediately exit the plugin; results and console output files will not be saved
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // This will be set to True if the parameter file contains both TDA=1 and showDecoy=1
            mResultsIncludeAutoAddedDecoyPeptides = mMSGFDBUtils.ResultsIncludeAutoAddedDecoyPeptides;

            LogMessage("Running MSGF+");

            // If an MSGFDB analysis crashes with an "out-of-memory" error, we need to reserve more memory for Java
            // The amount of memory required depends on both the fasta file size and the size of the input .mzML file, since data from all spectra are cached in memory
            // Customize this on a per-job basis using the MSGFDBJavaMemorySize setting in the settings file
            // (job 611216 succeeded with a value of 5000)

            // Prior to January 2016, MSGF+ used 4 to 7 threads, and if MSGFDBJavaMemorySize was too small,
            // we ran the risk of one thread crashing and the results files missing the search results for the spectra assigned to that thread
            // For large _dta.txt files, 2000 MB of memory could easily be small enough to result in crashing threads
            // Consequently, the default is now 4000 MB
            //
            // Furthermore, the 2016-Jan-21 release uses 128 search tasks (or 10 tasks per thread if over 12 threads),
            // executing the tasks via a pool, meaning the memory overhead of each thread is lower vs. previous versions that
            // had large numbers of tasks on a small, finite number of threads

            var javaMemorySize = m_jobParams.GetJobParameter("MSGFDBJavaMemorySize", 4000);
            if (javaMemorySize < 512)
                javaMemorySize = 512;

            // Set up and execute a program runner to run MSGFDB
            var cmdStr = " -Xmx" + javaMemorySize + "M -jar " + msgfdbJarFilePath;

            // Define the input file, output file, and fasta file
            switch (eInputFileFormat)
            {
                case eInputFileFormatTypes.CDTA:
                    cmdStr += " -s " + Dataset + "_dta.txt";
                    break;
                case eInputFileFormatTypes.MzML:
                    cmdStr += " -s " + Dataset + clsAnalysisResources.DOT_MZML_EXTENSION;
                    break;
                case eInputFileFormatTypes.MzXML:
                    cmdStr += " -s " + Dataset + clsAnalysisResources.DOT_MZXML_EXTENSION;
                    break;
                default:
                    LogError("Unsupported InputFileFormat: " + eInputFileFormat);
                    // Immediately exit the plugin; results and console output files will not be saved
                    return CloseOutType.CLOSEOUT_FAILED;
            }

            cmdStr += " -o " + fiMSGFPlusResults.Name;
            cmdStr += " -d " + PossiblyQuotePath(fastaFilePath);

            // Append the remaining options loaded from the parameter file
            cmdStr += " " + strMSGFDbCmdLineOptions;

            // Make sure the machine has enough free memory to run MSGFDB
            var logFreeMemoryOnSuccess = (m_DebugLevel >= 1);

            if (!clsAnalysisResources.ValidateFreeMemorySize(javaMemorySize, "MSGF+", logFreeMemoryOnSuccess))
            {
                m_message = "Not enough free memory to run MSGF+";
                // Immediately exit the plugin; results and console output files will not be saved
                return CloseOutType.CLOSEOUT_FAILED;
            }

            mWorkingDirectoryInUse = string.Copy(m_WorkDir);

            var success = StartMSGFPlusLocal(javaExePath, cmdStr);

            if (!success && string.IsNullOrEmpty(mMSGFPlusUtils.ConsoleOutputErrorMsg))
            {
                // Wait 2 seconds to give the log file a chance to finalize
                Thread.Sleep(2000);

                // Parse the console output file one more time in hopes of finding an error message
                ParseConsoleOutputFile(mWorkingDirectoryInUse);
            }

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mMSGFDBUtils.MSGFPlusVersion))
                {
                    ParseConsoleOutputFile(mWorkingDirectoryInUse);
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mMSGFDBUtils.ConsoleOutputErrorMsg))
            {
                LogMessage(mMSGFDBUtils.ConsoleOutputErrorMsg, 1, true);
            }

            fiMSGFPlusResults.Refresh();

            if (success)
            {
                if (!mMSGFPlusComplete)
                {
                    mMSGFPlusComplete = true;
                    mMSGFPlusCompletionTime = DateTime.UtcNow;
                }
            }
            else
            {
                if (mMSGFPlusComplete)
                {
                    LogError("MSGF+ log file reported it was complete, but aborted the ProgRunner since Java was frozen");
                }
                else
                {
                    LogError("Error running MSGF+");
                }

                if (mMSGFPlusComplete)
                {
                    // Don't treat this as a fatal error
                    m_EvalMessage = string.Copy(m_message);
                    m_message = string.Empty;
                }
                else
                {
                    processingError = true;
                }

                if (!mMSGFPlusComplete)
                {
                    if (mCmdRunner.ExitCode != 0)
                    {
                        LogWarning("MSGF+ returned a non-zero exit code: " + mCmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to MSGF+ failed (but exit code is 0)");
                    }
                }
            }

            if (mMSGFPlusComplete)
            {
                if (mMSGFDBUtils.TaskCountCompleted < mMSGFDBUtils.TaskCountTotal)
                {
                    var savedCountCompleted = mMSGFDBUtils.TaskCountCompleted;

                    // MSGF+ finished, but the log file doesn't report that all of the threads finished
                    // Wait 5 more seconds, then parse the log file again
                    // Keep checking and waiting for up to 45 seconds

                    LogWarning("MSGF+ finished, but the log file reports " + mMSGFDBUtils.TaskCountCompleted + " / " + mMSGFDBUtils.TaskCountTotal +
                               " completed tasks");

                    var waitStartTime = DateTime.UtcNow;
                    while (DateTime.UtcNow.Subtract(waitStartTime).TotalSeconds < 45)
                    {
                        Thread.Sleep(5000);
                        mMSGFDBUtils.ParseMSGFPlusConsoleOutputFile(mWorkingDirectoryInUse);

                        if (mMSGFDBUtils.TaskCountCompleted == mMSGFDBUtils.TaskCountTotal)
                        {
                            break;
                        }
                    }

                    if (mMSGFDBUtils.TaskCountCompleted == mMSGFDBUtils.TaskCountTotal)
                    {
                        LogMessage("Reparsing the MSGF+ log file now indicates that all tasks finished " + "(waited " +
                                   DateTime.UtcNow.Subtract(waitStartTime).TotalSeconds.ToString("0") + " seconds)");
                    }
                    else if (mMSGFDBUtils.TaskCountCompleted > savedCountCompleted)
                    {
                        LogWarning("Reparsing the MSGF+ log file now indicates that " + mMSGFDBUtils.TaskCountCompleted + " tasks finished. " +
                                   "That is an increase over the previous value but still not all " + mMSGFDBUtils.TaskCountTotal + " tasks");
                    }
                    else
                    {
                        LogWarning("Reparsing the MSGF+ log file indicated the same number of completed tasks");
                    }
                }

                if (mMSGFDBUtils.TaskCountCompleted < mMSGFDBUtils.TaskCountTotal)
                {
                    if (mMSGFDBUtils.TaskCountCompleted == mMSGFDBUtils.TaskCountTotal - 1)
                    {
                        // All but one of the tasks finished
                        LogWarning(
                            "MSGF+ finished, but the logs indicate that one of the " + mMSGFDBUtils.TaskCountTotal + " tasks did not complete; " +
                            "this could indicate an error", true);
                    }
                    else
                    {
                        // 2 or more tasks did not finish
                        mMSGFPlusComplete = false;
                        LogError("MSGF+ finished, but the logs are incomplete, showing " + mMSGFDBUtils.TaskCountCompleted + " / " +
                                 mMSGFDBUtils.TaskCountTotal + " completed search tasks");

                        // Do not return CLOSEOUT_FAILED, as that causes the plugin to immediately exit; results and console output files would not be saved in that case
                        // Instead, set processingError to true
                        processingError = true;
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }
                }
            }
            else
            {
                if (mMSGFDBUtils.TaskCountCompleted > 0)
                {
                    var msg = string.Copy(m_message);
                    if (string.IsNullOrWhiteSpace(msg))
                    {
                        msg = "MSGF+ processing failed";
                    }
                    msg += "; logs show " + mMSGFDBUtils.TaskCountCompleted + " / " + mMSGFDBUtils.TaskCountTotal + " completed search tasks";
                    LogError(msg);
                }

                // Do not return CLOSEOUT_FAILED, as that causes the plugin to immediately exit; results and console output files would not be saved in that case
                // Instead, set processingError to true
                processingError = true;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            m_progress = clsMSGFDBUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE;
            m_StatusTools.UpdateAndWrite(m_progress);
            LogMessage("MSGF+ Search Complete", 3);

            if (mMSGFDBUtils.ContinuumSpectraSkipped > 0)
            {
                // See if any spectra were processed
                if (!fiMSGFPlusResults.Exists)
                {
                    // Note that DMS stored procedure AutoResetFailedJobs looks for jobs with these phrases in the job comment
                    //   "None of the spectra are centroided; unable to process"
                    //   "skipped xx% of the spectra because they did not appear centroided"
                    //   "skip xx% of the spectra because they did not appear centroided"
                    //
                    // Failed jobs that are found to have this comment will have their settings files auto-updated and the job will auto-reset

                    LogError(clsAnalysisResources.SPECTRA_ARE_NOT_CENTROIDED + " with MSGF+");
                    processingError = true;
                }
                else
                {
                    // Compute the fraction of all potential spectra that were skipped
                    // If over 20% of the spectra were skipped, and if the source spectra were not centroided,
                    //   then tooManySkippedSpectra will be set to True and the job step will be marked as failed

                    var spectraAreCentroided =
                        m_jobParams.GetJobParameter("CentroidMSXML", false) ||
                        m_jobParams.GetJobParameter("CentroidDTAs", false) ||
                        m_jobParams.GetJobParameter("CentroidMGF", false);

                    var fractionSkipped = mMSGFPlusUtils.ContinuumSpectraSkipped /
                                             (double)(mMSGFPlusUtils.ContinuumSpectraSkipped + mMSGFPlusUtils.SpectraSearched);
                    var percentSkipped = (fractionSkipped * 100).ToString("0.0") + "%";

                    if (fractionSkipped > 0.2 && !spectraAreCentroided)
                    {
                        LogError("MSGF+ skipped " + percentSkipped + " of the spectra because they did not appear centroided");
                        tooManySkippedSpectra = true;
                    }
                    else
                    {
                        LogWarning(
                            "MSGF+ processed some of the spectra, but it skipped " +
                            mMSGFDBUtils.ContinuumSpectraSkipped + " spectra that were not centroided " +
                            "(" + strPercentSkipped + " skipped)", true);
                    }
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool StartMSGFPlusLocal(string javaExePath, string cmdStr)
        {
            if (m_DebugLevel >= 1)
            {
                LogMessage(javaExePath + " " + cmdStr);
            }

            mCmdRunner = new clsRunDosProgram(m_WorkDir)
            {
                CreateNoWindow = true,
                CacheStandardOutput = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = Path.Combine(m_WorkDir, clsMSGFDBUtils.MSGFPLUS_CONSOLE_OUTPUT_FILE)
            };
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            m_progress = clsMSGFDBUtils.PROGRESS_PCT_MSGFPLUS_STARTING;
            ResetProgRunnerCpuUsage();

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var success = mCmdRunner.RunProgram(javaExePath, cmdStr, "MSGF+", true);

            return success;
        }

        /// <summary>
        /// Convert the .mzid file created by MSGF+ to a .tsv file
        /// </summary>
        /// <param name="mzidFileName"></param>
        /// <returns>The name of the .tsv file if successful; empty string if an error</returns>
        /// <remarks></remarks>
        private string ConvertMZIDToTSV(string mzidFileName)
        {
            // Determine the path to the MzidToTsvConverter
            var mzidToTsvConverterProgLoc = DetermineProgramLocation("MzidToTsvConverterProgLoc", "MzidToTsvConverter.exe");

            if (string.IsNullOrEmpty(mzidToTsvConverterProgLoc))
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    LogError("Parameter 'MzidToTsvConverter' not defined for this manager");
                }
                return string.Empty;
            }

            var tsvFilePath = mMSGFPlusUtils.ConvertMZIDToTSV(mzidToTsvConverterProgLoc, Dataset, mzidFileName);

            if (string.IsNullOrEmpty(tsvFilePath))
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    LogError("Error calling mMSGFDBUtils.ConvertMZIDToTSV; path not returned");
                }
                return string.Empty;
            }

            var splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", false);

            if (splitFastaEnabled)
            {
                var tsvFileName = ParallelMSGFPlusRenameFile(Path.GetFileName(tsvFilePath));
                return tsvFileName;
            }

            return Path.GetFileName(tsvFilePath);

        }

        public override void CopyFailedResultsToArchiveFolder()
        {
            // Try to save whatever files are in the work directory (however, delete any spectral data files)

            m_jobParams.AddResultFileToSkip(Dataset + "_dta.zip");
            m_jobParams.AddResultFileToSkip(Dataset + "_dta.txt");

            m_jobParams.AddResultFileToSkip(Dataset + clsAnalysisResources.DOT_RAW_EXTENSION);
            m_jobParams.AddResultFileToSkip(Dataset + clsAnalysisResources.DOT_MZML_EXTENSION);
            m_jobParams.AddResultFileToSkip(Dataset + clsAnalysisResources.DOT_MGF_EXTENSION);

            base.CopyFailedResultsToArchiveFolder();
        }

        private bool CreateScanTypeFile(out string scanTypeFilePath)
        {
            var objScanTypeFileCreator = new clsScanTypeFileCreator(m_WorkDir, Dataset);

            scanTypeFilePath = string.Empty;

            if (objScanTypeFileCreator.CreateScanTypeFile())
            {
                if (m_DebugLevel >= 1)
                {
                    LogMessage("Created ScanType file: " + Path.GetFileName(objScanTypeFileCreator.ScanTypeFilePath));
                }
                scanTypeFilePath = objScanTypeFileCreator.ScanTypeFilePath;
                return true;
            }

            var errorMessage = "Error creating scan type file: " + objScanTypeFileCreator.ErrorMessage;
            var detailedMessage = string.Empty;

            if (!string.IsNullOrEmpty(objScanTypeFileCreator.ExceptionDetails))
            {
                detailedMessage += "; " + objScanTypeFileCreator.ExceptionDetails;
            }

            LogError(errorMessage, detailedMessage);
            return false;
        }

        private CloseOutType DetermineAssumedScanType(out string assumedScanType, out eInputFileFormatTypes eInputFileFormat,
                                                      out string scanTypeFilePath)
        {
            assumedScanType = string.Empty;

            var scriptNameLCase = m_jobParams.GetParam("ToolName").ToLower();
            scanTypeFilePath = string.Empty;

            if (scriptNameLCase.Contains("mzxml") || scriptNameLCase.Contains("msgfplus_bruker"))
            {
                eInputFileFormat = eInputFileFormatTypes.MzXML;
            }
            else if (scriptNameLCase.Contains("mzml"))
            {
                eInputFileFormat = eInputFileFormatTypes.MzML;
            }
            else
            {
                eInputFileFormat = eInputFileFormatTypes.CDTA;

                // Make sure the _DTA.txt file is valid
                if (!ValidateCDTAFile())
                {
                    return CloseOutType.CLOSEOUT_NO_DTA_FILES;
                }

                assumedScanType = m_jobParams.GetParam("AssumedScanType");

                if (string.IsNullOrWhiteSpace(assumedScanType))
                {
                    // Create the ScanType file (lists scan type for each scan number)
                    if (!CreateScanTypeFile(out scanTypeFilePath))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private DateTime dtLastConsoleOutputParse = DateTime.MinValue;

        private void MonitorProgress()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            UpdateStatusFile();

            // Parse the console output file every 30 seconds
            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds < SECONDS_BETWEEN_UPDATE)
            {
                return;
            }

            dtLastConsoleOutputParse = DateTime.UtcNow;

            ParseConsoleOutputFile(mWorkingDirectoryInUse);
            if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mMSGFDBUtils.MSGFPlusVersion))
            {
                mToolVersionWritten = StoreToolVersionInfo();
            }

            UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

            LogProgress("MSGF+");

            if (m_progress < clsMSGFDBUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE)
                return;

            if (!mMSGFPlusComplete)
            {
                mMSGFPlusComplete = true;
                mMSGFPlusCompletionTime = DateTime.UtcNow;
            }
            else
            {
                if (DateTime.UtcNow.Subtract(mMSGFPlusCompletionTime).TotalMinutes < 5)
                    return;

                // MSGF+ is stuck at 96% complete and has been that way for 5 minutes
                // Java is likely frozen and thus the process should be aborted

                var warningMessage = "MSGF+ has been stuck at " +
                                     clsMSGFDBUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE.ToString("0") + "% complete for 5 minutes; " +
                                     "aborting since Java appears frozen";

                LogWarning(warningMessage);

                // Bump up mMSGFPlusCompletionTime by one hour
                // This will prevent this function from logging the above message every 30 seconds if the .abort command fails
                mMSGFPlusCompletionTime = mMSGFPlusCompletionTime.AddHours(1);
                mCmdRunner.AbortProgramNow();
            }
        }

        /// <summary>
        /// Renames the results file created by a Parallel MSGF+ instance to have _Part##.mzid as a suffix
        /// </summary>
        /// <param name="resultsFileName"></param>
        /// <returns>The path to the new file if success, otherwise the original filename</returns>
        /// <remarks></remarks>
        private string ParallelMSGFPlusRenameFile(string resultsFileName)
        {
            var filePathNew = "??";

            try
            {
                var fiFile = new FileInfo(Path.Combine(m_WorkDir, resultsFileName));

                var iteration = clsAnalysisResources.GetSplitFastaIteration(m_jobParams, out m_message);

                var fileNameNew = Path.GetFileNameWithoutExtension(fiFile.Name) + "_Part" + iteration + fiFile.Extension;

                if (!fiFile.Exists)
                    return resultsFileName;

                filePathNew = Path.Combine(m_WorkDir, fileNameNew);
                fiFile.MoveTo(filePathNew);

                return fileNameNew;
            }
            catch (Exception ex)
            {
                LogError("Error renaming file " + resultsFileName + " to " + filePathNew, ex);
                return (resultsFileName);
            }
        }

        /// <summary>
        /// Parse the MSGFDB console output file to determine the MSGFDB version and to track the search progress
        /// </summary>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string workingDirectory)
        {
            float sngMSGFBProgress = 0;

            try
            {
                if ((mMSGFDBUtils != null))
                {
                    sngMSGFBProgress = mMSGFDBUtils.ParseMSGFPlusConsoleOutputFile(workingDirectory);
                }

                if (m_progress < sngMSGFBProgress)
                {
                    m_progress = sngMSGFBProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogMessage("Error parsing console output file: " + ex.Message, 2, true);
                }
            }
        }

        /// <summary>
        /// Convert the .mzid file to a TSV file and create the PeptideToProtein map file (Dataset_msgfplus_PepToProtMap.txt)
        /// </summary>
        /// <param name="resultsFileName"></param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Assumes that the calling function has verified that resultsFileName exists</remarks>
        private CloseOutType PostProcessMSGFDBResults(string resultsFileName)
        {
            var currentTask = "Starting";

            try
            {
                var splitFastaEnabled = m_jobParams.GetJobParameter("SplitFasta", false);

                if (splitFastaEnabled)
                {
                    currentTask = "Calling ParallelMSGFPlusRenameFile for " + resultsFileName;
                    resultsFileName = ParallelMSGFPlusRenameFile(resultsFileName);

                    currentTask = "Calling ParallelMSGFPlusRenameFile for MSGFPlus_ConsoleOutput.txt";
                    ParallelMSGFPlusRenameFile("MSGFPlus_ConsoleOutput.txt");
                }

                // Gzip the output file
                currentTask = "Zipping " + resultsFileName;
                var result = mMSGFDBUtils.ZipOutputFile(this, resultsFileName);
                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                string msgfPlusResultsFileName;
                var extension = Path.GetExtension(resultsFileName);
                if (extension != null && extension.ToLower() == ".mzid")
                {
                    // Convert the .mzid file to a .tsv file

                    currentTask = "Calling ConvertMZIDToTSV";
                    UpdateStatusRunning(clsMSGFDBUtils.PROGRESS_PCT_MSGFPLUS_CONVERT_MZID_TO_TSV);
                    msgfPlusResultsFileName = ConvertMZIDToTSV(resultsFileName);

                    if (string.IsNullOrEmpty(msgfPlusResultsFileName))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                else
                {
                    msgfPlusResultsFileName = string.Copy(resultsFileName);
                }

                var skipPeptideToProteinMapping = m_jobParams.GetJobParameter("SkipPeptideToProteinMapping", false);

                if (skipPeptideToProteinMapping)
                {
                    LogMessage("Skipping PeptideToProteinMapping since job parameter SkipPeptideToProteinMapping is True");
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                // Examine the MSGF+ TSV file to see if it's empty
                using (var reader = new StreamReader(new FileStream(Path.Combine(m_WorkDir, msgfPlusResultsFileName), FileMode.Open, FileAccess.Read,
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
                        dataLines += 1;
                        if (dataLines > 2)
                            break;
                    }

                    if (dataLines <= 1)
                    {
                        LogWarning("MSGF+ did not identify any peptides (TSV file is empty)", true);
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }
                }

                // Create the Peptide to Protein map file, Dataset_msgfplus_PepToProtMap.txt

                UpdateStatusRunning(clsMSGFDBUtils.PROGRESS_PCT_MSGFPLUS_MAPPING_PEPTIDES_TO_PROTEINS);

                var localOrgDbFolder = m_mgrParams.GetParam("orgdbdir");
                currentTask = "Calling CreatePeptideToProteinMapping";
                result = mMSGFDBUtils.CreatePeptideToProteinMapping(msgfPlusResultsFileName, mResultsIncludeAutoAddedDecoyPeptides, localOrgDbFolder);
                if (!clsAnalysisJob.SuccessOrNoData(result))
                {
                    return result;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error in PostProcessMSGFDBResults (CurrentTask = " + currentTask + ")", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Retrieve MSGF+ results that were run remotely
        /// </summary>
        /// <param name="transferUtility">Transfer utility</param>
        /// <param name="verifyCopied">Log warnings if any files are missing.  When false, logs debug messages instead</param>
        /// <param name="retrievedFilePaths">Local paths of retrieved files</param>
        /// <returns>True on success, otherwise false</returns>
        public override bool RetrieveRemoteResults(clsRemoteTransferUtility transferUtility, bool verifyCopied, out List<string> retrievedFilePaths)
        {
            try
            {
                var filesToRetrieve = new List<string> {
                    ToolVersionInfoFile,
                    Dataset + "_msgfplus.mzid.gz",
                    clsMSGFDBUtils.MSGFPLUS_CONSOLE_OUTPUT_FILE
                };

                var success = base.RetrieveRemoteResults(transferUtility, filesToRetrieve, verifyCopied, out retrievedFilePaths);
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
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            LogMessage("Determining tool version info", 2);

            var toolVersionInfo = string.Copy(mMSGFPlusUtils.MSGFPlusVersion);

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo> {
                new FileInfo(mMSGFDbProgLoc)
            };

            try
            {
                // Need to pass saveToolVersionTextFile to True so that the ToolVersionInfo file gets created
                // The PeptideListToXML program uses that file when creating .pepXML files
                return SetStepTaskToolVersion(toolVersionInfo, ioToolFiles, saveToolVersionTextFile: true);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        #endregion

        #region "Event Handlers"

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        /// <remarks></remarks>
        private void CmdRunner_LoopWaiting()
        {
            MonitorProgress();
        }

        private void mMSGFDBUtils_IgnorePreviousErrorEvent()
        {
            m_message = string.Empty;
        }

        #endregion
    }
}
