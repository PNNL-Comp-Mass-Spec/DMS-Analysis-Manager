//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 09/05/2014
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerMSGFDBPlugIn;
using MsMsDataFileReader;
using PRISM;
using PRISMDatabaseUtils;

namespace AnalysisManagerMzRefineryPlugIn
{
    /// <summary>
    /// Class for running Mz Refinery to recalibrate m/z values in a .mzXML or .mzML file
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class AnalysisToolRunnerMzRefinery : AnalysisToolRunnerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: Caulo, conf, cv, Cyano, Cyanothece, Dta, endian, identfile, indexedmzML,
        // Ignore Spelling: modplus, MSGFPlus, MzID, mzxml, outfile, Prepend, Utils, Xmx

        // ReSharper restore CommentTypo

        private const int PROGRESS_PCT_MzREFINERY_COMPLETE = 97;

        private const int PROGRESS_PCT_PLOTS_GENERATED = 98;

        private const int PROGRESS_PCT_COMPLETE = 99;

        private const string MZ_REFINERY_CONSOLE_OUTPUT = "MSConvert_MzRefinery_ConsoleOutput.txt";

        private const string ERROR_CHARTER_CONSOLE_OUTPUT_FILE = "PPMErrorCharter_ConsoleOutput.txt";

        public const string MSGFPLUS_MZID_SUFFIX = "_msgfplus.mzid";

        private enum MzRefinerProgRunnerMode
        {
            Unknown = 0,
            MSGFPlus = 1,
            MzRefiner = 2,
            PPMErrorCharter = 3
        }

        private readonly List<string> mResultFilesToSkipIfNoError = new();

        private bool mToolVersionWritten;

        private string mConsoleOutputErrorMsg;
        private string mMSGFPlusProgLoc;
        private string mMSConvertProgLoc;

        private string mPpmErrorCharterProgLoc;

        private MzRefinerProgRunnerMode mProgRunnerMode;
        private bool mMSGFPlusComplete;

        private DateTime mMSGFPlusCompletionTime;
        private double mMSGFPlusRunTimeMinutes;

        private bool mSkipMzRefinery;
        private bool mUnableToUseMzRefinery;

        private bool mForceGeneratePPMErrorPlots;

        private string mMzRefineryCorrectionMode;
        private int mMzRefinerGoodMS1Spectra;
        private int mMzRefinerGoodMS2FragmentIons;
        private double mMzRefinerSpecEValueThreshold;

        private MSGFPlusUtils mMSGFPlusUtils;

        private DirectoryInfo mMSXmlCacheFolder;

        /// <summary>
        /// Command runner for MS-GF+, MzRefinery (which uses MSConvert), and PPMErrorCharter
        /// </summary>
        /// <remarks>MzRefinerProgRunnerMode keeps track of the current ProgRunner and is used by MonitorProgress</remarks>
        private RunDosProgram mCmdRunner;

        /// <summary>
        /// Runs MS-GF+ then runs MSConvert with the MzRefiner filter
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
                    LogDebug("AnalysisToolRunnerMzRefinery.RunTool(): Enter");
                }

                // Initialize class-wide variables that will be updated later
                mMzRefineryCorrectionMode = string.Empty;
                mMzRefinerGoodMS1Spectra = 0;
                mMzRefinerGoodMS2FragmentIons = 0;
                mMzRefinerSpecEValueThreshold = 1E-10;

                mUnableToUseMzRefinery = false;
                mForceGeneratePPMErrorPlots = false;

                // Verify that program files exist

                // Determine the path to MSConvert
                // (as of March 10, 2015 the official release of ProteoWizard contains MSConvert.exe that supports the MzRefiner filter)
                mMSConvertProgLoc = DetermineProgramLocation("ProteoWizardDir", ToolVersionUtilities.MSCONVERT_EXE_NAME.ToLower());

                if (string.IsNullOrWhiteSpace(mMSConvertProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // Determine the path to the PPM error charter program
                mPpmErrorCharterProgLoc = DetermineProgramLocation("MzRefineryProgLoc", "PPMErrorCharter.exe");

                if (string.IsNullOrWhiteSpace(mPpmErrorCharterProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                // javaProgLoc will typically be "C:\Program Files\Java\jre11\bin\Java.exe"
                var javaProgLoc = GetJavaProgLoc();

                if (string.IsNullOrEmpty(javaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var msXMLCacheFolderPath = mMgrParams.GetParam("MSXMLCacheFolderPath", string.Empty);
                mMSXmlCacheFolder = new DirectoryInfo(msXMLCacheFolderPath);

                if (!mMSXmlCacheFolder.Exists)
                {
                    LogError("MSXmlCache folder not found: " + msXMLCacheFolderPath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var msXmlFileExtension = AnalysisResources.DOT_MZML_EXTENSION;

                var dtaGenerator = mJobParams.GetJobParameter("DtaGenerator", string.Empty);

                if (!string.IsNullOrWhiteSpace(dtaGenerator))
                {
                    // Update the .mzML file using parent ion details in the _dta.txt file
                    var successToMzML = UpdateMzMLUsingCDTA();

                    if (!successToMzML)
                    {
                        if (string.IsNullOrEmpty(mMessage))
                        {
                            LogError("Unknown error updating the .mzML with the _dta.txt file");
                        }
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                else
                {
                    var msXmlOutputType = mJobParams.GetJobParameter("MSXMLOutputType", string.Empty);

                    if (string.Equals(msXmlOutputType, "mzXML", StringComparison.OrdinalIgnoreCase))
                    {
                        msXmlFileExtension = AnalysisResources.DOT_MZXML_EXTENSION;
                    }
                }

                // Look for existing MS-GF+ results (which would have been retrieved by AnalysisResourcesMzRefinery)

                var msgfPlusResults = new FileInfo(Path.Combine(mWorkDir, mDatasetName + MSGFPLUS_MZID_SUFFIX));
                var skippedMSGFPlus = false;

                CloseOutType result;

                if (msgfPlusResults.Exists)
                {
                    result = CloseOutType.CLOSEOUT_SUCCESS;
                    skippedMSGFPlus = true;
                    mJobParams.AddResultFileToSkip(msgfPlusResults.Name);
                }
                else
                {
                    // Run MS-GF+ (includes indexing the FASTA file)
                    result = RunMSGFPlus(javaProgLoc, msXmlFileExtension, out msgfPlusResults);
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("Unknown error running MS-GF+ prior to running MzRefiner");
                    }
                    return result;
                }

                mCmdRunner = null;

                var processingError = false;

                var originalMSXmlFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + msXmlFileExtension));
                var fixedMSXmlFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_FIXED" + msXmlFileExtension));

                mJobParams.AddResultFileToSkip(originalMSXmlFile.Name);
                mJobParams.AddResultFileToSkip(fixedMSXmlFile.Name);

                if (mSkipMzRefinery)
                {
                    // Rename the original file to have the expected name of the fixed mzML file
                    // Required for PostProcessMzRefineryResults to work properly
                    originalMSXmlFile.MoveTo(fixedMSXmlFile.FullName);
                }
                else
                {
                    // Run MSConvert with the MzRefiner filter
                    var mzRefinerySuccess = StartMzRefinery(originalMSXmlFile, msgfPlusResults);

                    if (!mzRefinerySuccess)
                    {
                        processingError = true;
                    }
                    else
                    {
                        if (mMzRefineryCorrectionMode.StartsWith("Chose no shift"))
                        {
                            // No valid peak was found; a result file may not exist
                            fixedMSXmlFile.Refresh();

                            if (!fixedMSXmlFile.Exists)
                            {
                                // Rename the original file to have the expected name of the fixed mzML file
                                // Required for PostProcessMzRefineryResults to work properly
                                originalMSXmlFile.MoveTo(fixedMSXmlFile.FullName);
                            }
                        }
                    }
                }

                if (!processingError)
                {
                    // Look for the results file
                    fixedMSXmlFile.Refresh();

                    if (fixedMSXmlFile.Exists)
                    {
                        var postProcessSuccess = PostProcessMzRefineryResults(msgfPlusResults, fixedMSXmlFile, skippedMSGFPlus);

                        if (!postProcessSuccess)
                            processingError = true;
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(mMessage))
                        {
                            LogError("MzRefinery results file not found: " + fixedMSXmlFile.Name);
                        }
                        processingError = true;
                    }
                }

                if (mUnableToUseMzRefinery)
                {
                    msgfPlusResults.Refresh();

                    if (mForceGeneratePPMErrorPlots && msgfPlusResults.Exists)
                    {
                        try
                        {
                            if (fixedMSXmlFile.Exists)
                            {
                                StartPpmErrorCharter(msgfPlusResults, fixedMSXmlFile);
                            }
                            else if (originalMSXmlFile.Exists)
                            {
                                StartPpmErrorCharter(msgfPlusResults, originalMSXmlFile);
                            }
                            else
                            {
                                LogWarning("Unable to generate PPMError plots for debugging purposes; .mzML file not found");
                            }
                        }
                        catch (Exception ex)
                        {
                            // Treat this as a warning
                            LogWarning("Error generating PPMError plots for debugging purposes: " + ex.Message);
                        }
                    }

                    using var writer = new StreamWriter(new FileStream(
                        Path.Combine(mWorkDir, "NOTE - Orphan folder; safe to delete.txt"),
                        FileMode.Create, FileAccess.Write, FileShare.Read));

                    writer.WriteLine("This folder contains MS-GF+ results and the MzRefinery log file from a failed attempt at running MzRefinery for job " + mJob + ".");
                    writer.WriteLine("The files can be used to investigate the MzRefinery failure.");
                    writer.WriteLine("the directory can be safely deleted.");
                }

                mProgress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                mStopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                Global.IdleLoop(0.5);
                AppUtils.GarbageCollectNow();

                if (processingError)
                {
                    var msgfPlusResultsExist = false;

                    if (msgfPlusResults?.Exists == true)
                    {
                        // MS-GF+ succeeded but MzRefinery or PostProcessing failed
                        // We will mark the job as failed, but we want to move the MS-GF+ results into the transfer folder

                        if (skippedMSGFPlus)
                        {
                            msgfPlusResultsExist = true;
                        }
                        else
                        {
                            msgfPlusResultsExist = CompressMSGFPlusResults(msgfPlusResults);
                        }
                    }

                    if (!msgfPlusResultsExist)
                    {
                        // Move the source files and any results to the Failed Job folder
                        // Useful for debugging problems
                        CopyFailedResultsToArchiveDirectory(msXmlFileExtension);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }

                var success = CopyResultsToTransferDirectory();

                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                if (!processingError)
                    return CloseOutType.CLOSEOUT_SUCCESS;

                // If we get here, MS-GF+ succeeded, but MzRefinery or PostProcessing failed
                LogWarning("Processing failed; see results at " + mJobParams.GetParam(AnalysisResources.JOB_PARAM_TRANSFER_DIRECTORY_PATH));

                if (mUnableToUseMzRefinery)
                {
                    return CloseOutType.CLOSEOUT_UNABLE_TO_USE_MZ_REFINERY;
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Error in MzRefineryPlugin->RunTool", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Index the FASTA file (if needed) then run MS-GF+
        /// </summary>
        /// <param name="javaProgLoc">Path to Java</param>
        /// <param name="msXmlFileExtension">.mzXML or .mzML</param>
        /// <param name="msgfPlusResults">Output: MS-GF+ results file</param>
        private CloseOutType RunMSGFPlus(string javaProgLoc, string msXmlFileExtension, out FileInfo msgfPlusResults)
        {
            msgfPlusResults = null;

            // Determine the path to MS-GF+
            mMSGFPlusProgLoc = DetermineProgramLocation("MSGFPlusProgLoc", MSGFPlusUtils.MSGFPLUS_JAR_NAME);

            if (string.IsNullOrWhiteSpace(mMSGFPlusProgLoc))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Note: we will store the MS-GF+ version info in the database after the first line is written to file MSGFPlus_ConsoleOutput.txt
            mToolVersionWritten = false;

            mMSGFPlusComplete = false;

            mResultFilesToSkipIfNoError.Clear();

            // These two variables are required for the call to ParseMSGFPlusParameterFile
            // They are blank because the source file is a mzML file, and that file includes scan type information
            var scanTypeFilePath = string.Empty;
            var assumedScanType = string.Empty;

            // Initialize mMSGFPlusUtils
            mMSGFPlusUtils = new MSGFPlusUtils(mMgrParams, mJobParams, mWorkDir, mDebugLevel);
            RegisterEvents(mMSGFPlusUtils);

            mMSGFPlusUtils.IgnorePreviousErrorEvent += MSGFPlusUtils_IgnorePreviousErrorEvent;

            // Get the FASTA file and index it if necessary
            // Note: if the FASTA file is over 50 MB in size, only use the first 50 MB

            // Passing in the path to the parameter file so that we can look for TDA=0 when using large FASTA files
            var paramFilePath = Path.Combine(mWorkDir, mJobParams.GetJobParameter("MzRefParamFile", string.Empty));
            var javaExePath = javaProgLoc;
            var msgfplusJarFilePath = mMSGFPlusProgLoc;

            const int maxFastaFileSizeMB = 50;

            // Initialize the FASTA file; truncating it if it is over 50 MB in size
            var result = mMSGFPlusUtils.InitializeFastaFile(
                javaExePath, msgfplusJarFilePath,
                out _, out var fastaFileIsDecoy, out var fastaFilePath,
                paramFilePath, maxFastaFileSizeMB);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                mInsufficientFreeMemory = mMSGFPlusUtils.InsufficientFreeMemory;
                return result;
            }

            var instrumentGroup = mJobParams.GetJobParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "InstrumentGroup", string.Empty);

            // Read the MS-GF+ Parameter File

            var overrideParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var jobScript = mJobParams.GetJobParameter("ToolName", "");

            if (jobScript.StartsWith("modplus", StringComparison.OrdinalIgnoreCase))
            {
                if (fastaFileIsDecoy)
                {
                    overrideParams.Add(MSGFPlusUtils.MSGFPLUS_OPTION_TDA, "0");
                }
            }

            // Read the MSGFPlus Parameter File and optionally create a new one with customized parameters
            // paramFile will contain the path to either the original parameter file or the customized one

            result = mMSGFPlusUtils.ParseMSGFPlusParameterFile(
                fastaFileIsDecoy, assumedScanType, scanTypeFilePath,
                instrumentGroup, paramFilePath, overrideParams,
                out var sourceParamFile, out var finalParamFile);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            if (finalParamFile == null)
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    LogError("Problem parsing MzRef parameter file to extract MS-GF+ options");
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (!string.Equals(sourceParamFile.FullName, finalParamFile.FullName))
            {
                AddResultFileToSkipIfNoError(sourceParamFile.Name);
            }

            // Look for extra parameters specific to MZRefinery
            var success = ExtractMzRefinerOptionsFromParameterFile(sourceParamFile.FullName);

            if (!success)
            {
                LogError("Error extracting MzRefinery options from parameter file " + Path.GetFileName(paramFilePath));
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var resultsFileName = mDatasetName + MSGFPLUS_MZID_SUFFIX;
            msgfPlusResults = new FileInfo(Path.Combine(mWorkDir, resultsFileName));

            LogMessage("Running MS-GF+");

            // Setting MzRefMSGFPlusJavaMemorySize is stored in the settings file for this job

            // If an MS-GF+ analysis crashes with an "out-of-memory" error, we need to reserve more memory for Java
            // The amount of memory required depends on both the FASTA file size and the size of the input .mzML file, since data from all spectra are cached in memory
            // Customize this on a per-job basis using the MzRefMSGFPlusJavaMemorySize setting in the settings file
            var defaultJavaMemorySizeMB = mJobParams.GetJobParameter("MzRefMSGFPlusJavaMemorySize", 8192);

            if (defaultJavaMemorySizeMB < 4000)
                defaultJavaMemorySizeMB = 4000;

            var fastaFile = new FileInfo(fastaFilePath);
            var inputFileName = mDatasetName + msXmlFileExtension;
            var inputFile = new FileInfo(Path.Combine(mWorkDir, inputFileName));
            var inputFileDescription = string.Format("{0} file", msXmlFileExtension);

            var javaMemorySizeMB = AnalysisToolRunnerMSGFDB.GetMemoryRequiredForFASTA(defaultJavaMemorySizeMB, fastaFile, inputFile, inputFileDescription, out var warningMessage);

            if (!string.IsNullOrEmpty(warningMessage))
            {
                LogWarning(warningMessage);
            }

            // Set up and execute a program runner to run MS-GF+
            var arguments =
                " -Xmx" + javaMemorySizeMB + "M" +
                " -jar " + msgfplusJarFilePath +
                " -s " + inputFileName +
                " -o " + msgfPlusResults.Name +
                " -d " + PossiblyQuotePath(fastaFilePath) +
                " -conf " + finalParamFile.Name;

            // Make sure the machine has enough free memory to run MS-GF+
            var logFreeMemoryOnSuccess = mDebugLevel >= 1;

            if (!AnalysisResources.ValidateFreeMemorySize(javaMemorySizeMB, "MS-GF+", logFreeMemoryOnSuccess))
            {
                mInsufficientFreeMemory = true;

                LogError("Not enough free memory to run MS-GF+");
                return CloseOutType.CLOSEOUT_RESET_JOB_STEP;
            }

            success = StartMSGFPlus(javaExePath, "MS-GF+", arguments);

            if (!success && string.IsNullOrEmpty(mMSGFPlusUtils.ConsoleOutputErrorMsg))
            {
                // Parse the console output file one more time in hopes of finding an error message
                ParseMSGFPlusConsoleOutputFile(mWorkDir);
            }

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mMSGFPlusUtils.MSGFPlusVersion))
                {
                    ParseMSGFPlusConsoleOutputFile(mWorkDir);
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mMSGFPlusUtils.ConsoleOutputErrorMsg))
            {
                LogError(mMSGFPlusUtils.ConsoleOutputErrorMsg);
            }

            var processingError = false;

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
                string msg;

                if (mMSGFPlusComplete)
                {
                    msg = "MS-GF+ log file reported it was complete, but aborted the ProgRunner since Java was frozen";
                }
                else
                {
                    msg = "Error running MS-GF+";
                }

                LogError(msg);

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
                mProgress = MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE;
                mStatusTools.UpdateAndWrite(mProgress);

                if (mDebugLevel >= 3)
                {
                    LogDebug("MS-GF+ Search Complete");
                }
            }

            // Look for the .mzid file
            msgfPlusResults.Refresh();

            if (!msgfPlusResults.Exists)
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    LogError("MS-GF+ results file not found: " + resultsFileName);
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            mJobParams.AddResultFileToSkip(MSGFPlusUtils.MOD_FILE_NAME);

            if (processingError)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool StartMSGFPlus(string javaExePath, string searchEngineName, string arguments)
        {
            if (mDebugLevel >= 1)
            {
                LogDebug(javaExePath + " " + arguments);
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

            mProgRunnerMode = MzRefinerProgRunnerMode.MSGFPlus;

            // Start MS-GF+ and wait for it to exit
            var success = mCmdRunner.RunProgram(javaExePath, arguments, searchEngineName, true);

            mProgRunnerMode = MzRefinerProgRunnerMode.Unknown;

            return success;
        }

        private void AddResultFileToSkipIfNoError(string fileName)
        {
            mResultFilesToSkipIfNoError.Add(fileName);
        }

        private bool CompressMSGFPlusResults(FileSystemInfo msgfPlusResults)
        {
            try
            {
                // Compress the MS-GF+ .mzID file
                var success = mZipTools.GZipFile(msgfPlusResults.FullName, true);

                if (!success)
                {
                    LogError(mZipTools.Message);
                    return false;
                }

                mJobParams.AddResultFileToSkip(msgfPlusResults.Name);
                mJobParams.AddResultFileToKeep(msgfPlusResults.Name + AnalysisResources.DOT_GZ_EXTENSION);
            }
            catch (Exception ex)
            {
                LogError("Error compressing the .mzID file", ex);
                return false;
            }

            return true;
        }

        private void CopyFailedResultsToArchiveDirectory(string msXmlFileExtension)
        {
            try
            {
                var msXmlFiles = new DirectoryInfo(mWorkDir).GetFiles("*" + msXmlFileExtension);

                foreach (var fileToDelete in msXmlFiles)
                {
                    fileToDelete.Delete();
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }

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

        private bool ExtractMzRefinerOptionsFromParameterFile(string parameterFilePath)
        {
            mSkipMzRefinery = false;

            try
            {
                var paramFileReader = new PRISM.AppSettings.KeyValueParamFileReader("MzRefinery", parameterFilePath);
                RegisterEvents(paramFileReader);

                var success = paramFileReader.ParseKeyValueParameterFile(out var paramFileEntries);

                if (!success)
                {
                    LogError(paramFileReader.ErrorMessage);
                    return false;
                }

                mSkipMzRefinery = paramFileReader.ParamIsEnabled(paramFileEntries, "SkipMzRefinery");

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error in ExtractMzRefinerOptionsFromParameterFile", ex);
                return false;
            }
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

            if (mProgRunnerMode == MzRefinerProgRunnerMode.MSGFPlus)
            {
                ParseMSGFPlusConsoleOutputFile(mWorkDir);

                if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mMSGFPlusUtils.MSGFPlusVersion))
                {
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

                LogProgress("MS-GF+ for MzRefinery");

                if (mProgress < MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE)
                    return;

                var cmdRunnerRuntimeMinutes = Math.Max(1, mCmdRunner?.RunTime.TotalMinutes ?? 1);

                if (!mMSGFPlusComplete)
                {
                    mMSGFPlusComplete = true;
                    mMSGFPlusCompletionTime = DateTime.UtcNow;
                    mMSGFPlusRunTimeMinutes = cmdRunnerRuntimeMinutes;
                    return;
                }

                // A previous call to this method should have updated mMSGFPlusCompletionTime and mMSGFPlusRunTimeMinutes
                // Check, just to be sure, updating if necessary
                if (mMSGFPlusCompletionTime == DateTime.MinValue)
                    mMSGFPlusCompletionTime = DateTime.UtcNow;

                if (mMSGFPlusRunTimeMinutes < cmdRunnerRuntimeMinutes)
                    mMSGFPlusRunTimeMinutes = cmdRunnerRuntimeMinutes;

                // Wait a minimum of 5 minutes for Java to finish
                // Wait longer for jobs that have been running longer
                var waitTimeMinutes = (int)Math.Ceiling(Math.Max(5, Math.Sqrt(mMSGFPlusRunTimeMinutes)));

                if (DateTime.UtcNow.Subtract(mMSGFPlusCompletionTime).TotalMinutes < waitTimeMinutes)
                    return;

                // MS-GF+ is finished but hasn't exited after 5 minutes (longer for long-running jobs)
                // If there is a large number results, we need to given MS-GF+ time to sort them prior to writing to disk
                // However, it is also possible that Java frozen and thus the process should be aborted

                var warningMessage = string.Format(
                    "MS-GF+ has been stuck at {0}% complete for {1} minutes (after running for {2:F0} minutes); aborting since Java appears frozen",
                    MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE, waitTimeMinutes, mMSGFPlusRunTimeMinutes);

                LogWarning(warningMessage);

                // Bump up mMSGFPlusCompletionTime by one hour
                // This will prevent this method from logging the above message every 30 seconds if the .abort command fails
                mMSGFPlusCompletionTime = mMSGFPlusCompletionTime.AddHours(1);

                mCmdRunner.AbortProgramNow();
            }
            else if (mProgRunnerMode == MzRefinerProgRunnerMode.MzRefiner)
            {
                ParseMSConvertConsoleOutputFile(Path.Combine(mWorkDir, MZ_REFINERY_CONSOLE_OUTPUT));

                UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

                LogProgress("MzRefinery");
            }
            else
            {
                LogProgress("MzRefinery, unknown step: " + mProgRunnerMode);
            }
        }

        /// <summary>
        /// Parse the MS-GF+ console output file to determine the MS-GF+ version and to track the search progress
        /// </summary>
        private void ParseMSGFPlusConsoleOutputFile(string workingDirectory)
        {
            try
            {
                if (mMSGFPlusUtils != null)
                {
                    var msgfPlusProgress = mMSGFPlusUtils.ParseMSGFPlusConsoleOutputFile(workingDirectory);
                    UpdateProgress(msgfPlusProgress);
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error parsing MS-GF+ console output file: " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Parse the MSConvert console output file to look for errors from MzRefiner
        /// </summary>
        /// <param name="consoleOutputFilePath">Console output file path</param>
        private void ParseMSConvertConsoleOutputFile(string consoleOutputFilePath)
        {
            // Example console output

            // format: mzML
            //     m/z: Compression-None, 32-bit
            //     intensity: Compression-None, 32-bit
            //     rt: Compression-None, 32-bit
            // ByteOrder_LittleEndian
            //  indexed="true"
            // outputPath: .
            // extension: .mzML
            // contactFilename:
            //
            // spectrum list filters:
            //   mzRefiner E:\DMS_WorkDir\Dataset_msgfplus.mzid thresholdValue=-1e-10 thresholdStep=10 maxSteps=2
            //
            // chromatogram list filters:
            //
            // filenames:
            //   E:\DMS_WorkDir\Dataset.mzML
            //
            // processing file: E:\DMS_WorkDir\Dataset.mzML
            // writing output file: .\Dataset_FIXED.mzML

            // ReSharper disable CommentTypo

            // Example warnings (these may be out of date)

            // Sparse data file
            // Low number of good identifications found. Will not perform dependent shifts.
            //    Less than 500 (123) results after filtering.
            //    Filtered out 6830 identifications because of score.

            // Really sparse data file
            // Excluding file ".\mzmlRefineryData\Cyanothece_bad\Cyano_GC_07_10_25Aug09_Draco_09-05-02.mzid" from data set
            //    Less than 100 (16) results after filtering.
            //    Filtered out 4208 identifications because of score.
            //    Filtered out 0 identifications because of mass error.

            // No data passing the filters
            // Excluding file "C:\DMS_WorkDir1\Caulo_pY_Run5_msgfplus.mzid" from data set.
            //    Less than 100 (0) results after filtering.
            //    Filtered out 8 identifications because of score.
            //    Filtered out 0 identifications because of mass error.

            // ReSharper restore CommentTypo

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

                using var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    if (dataLine.StartsWith("error:", StringComparison.OrdinalIgnoreCase) ||
                        dataLine.IndexOf("unhandled exception", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                        {
                            mConsoleOutputErrorMsg = "Error running MzRefinery: " + dataLine;
                        }
                        else
                        {
                            mConsoleOutputErrorMsg += "; " + dataLine;
                        }
                    }
                    else if (dataLine.StartsWith("Low number of good identifications found"))
                    {
                        mEvalMessage = dataLine;
                        LogMessage("MzRefinery warning: " + dataLine);
                    }
                    else if (dataLine.StartsWith("Excluding file") && dataLine.EndsWith("from data set"))
                    {
                        LogError("Fewer than 100 matches after filtering; cannot use MzRefinery on this dataset");
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    LogError("Error parsing MzRefinery console output file (" + consoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Parse the mzRefinement.tsv created by MzRefiner
        /// </summary>
        /// <param name="msXmlFile">.mzML file</param>
        private void ParseMzRefinementStatsFile(FileSystemInfo msXmlFile)
        {
            // Example results from the .tsv file (reformatted from column-based to row-based data for readability)

            // ThresholdScore	MS-GF:SpecEValue
            // ThresholdValue	-1.7976931348623157e+308 <= MME <= 1e-010
            // Excluded (score)	33659
            // Excluded (mass error)	7
            // MS1 Included	4122
            // MS1 Shift method	scan time
            // MS1 Final stDev	6.07514
            // MS1 Tolerance for 99%	18.2254
            // MS1 Final MAD	1.03865
            // MS1 MAD Tolerance for 99%	4.61971
            // MS2 Included	115278
            // MS2 Shift method	scan time
            // MS2 Final stDev	3.27895
            // MS2 Tolerance for 99%	9.83686
            // MS2 Final MAD	0.727381
            // MS2 MAD Tolerance for 99%	3.23524

            try
            {
                var tsvFilePath = Path.ChangeExtension(msXmlFile.FullName, ".mzRefinement.tsv");

                if (!File.Exists(tsvFilePath))
                {
                    LogWarning("MzRefinery stats file not found: " + tsvFilePath);
                    return;
                }

                if (mDebugLevel >= 4)
                {
                    LogDebug("Parsing file " + tsvFilePath);
                }

                // This dictionary maps column name to column index
                var columnMap = new Dictionary<string, int>();

                var requiredColumns = new List<string> {
                    "ThresholdValue",
                    "Excluded (score)",
                    "Excluded (mass error)",
                    "MS1 Included",
                    "MS1 Shift method",
                    "MS1 Final stDev",
                    "MS1 Tolerance for 99%",
                    "MS2 Included",
                    "MS2 Shift method",
                    "MS2 Final stDev",
                    "MS2 Tolerance for 99%"
                };

                using (var reader = new StreamReader(new FileStream(tsvFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (columnMap.Count == 0)
                        {
                            Global.ParseHeaderLine(columnMap, dataLine, requiredColumns);
                            continue;
                        }

                        var dataColumns = dataLine.Split('\t');

                        var thresholdRange = DataTableUtils.GetColumnValue(dataColumns, columnMap, "ThresholdValue");
                        var countExcludedByScore = DataTableUtils.GetColumnValue(dataColumns, columnMap, "Excluded (score)", 0);
                        var countExcludedByMassError = DataTableUtils.GetColumnValue(dataColumns, columnMap, "Excluded (mass error)", 0);

                        var shiftMethodMS1 = DataTableUtils.GetColumnValue(dataColumns, columnMap, "MS1 Shift method");
                        var includedMS1 = DataTableUtils.GetColumnValue(dataColumns, columnMap, "MS1 Included", 0);
                        var finalStDevMS1 = DataTableUtils.GetColumnValue(dataColumns, columnMap, "MS1 Final stDev", 0.0);
                        var toleranceFor99PctMS1 = DataTableUtils.GetColumnValue(dataColumns, columnMap, "MS1 Tolerance for 99%", 0.0);

                        var shiftMethodMS2 = DataTableUtils.GetColumnValue(dataColumns, columnMap, "MS2 Shift method");
                        var includedMS2 = DataTableUtils.GetColumnValue(dataColumns, columnMap, "MS2 Included", 0);
                        var finalStDevMS2 = DataTableUtils.GetColumnValue(dataColumns, columnMap, "MS2 Final stDev", 0.0);
                        var toleranceFor99PctMS2 = DataTableUtils.GetColumnValue(dataColumns, columnMap, "MS2 Tolerance for 99%", 0.0);

                        var thresholdValueMatcher = new Regex("MME <= (?<Threshold>.+)");
                        var thresholdMatch = thresholdValueMatcher.Match(thresholdRange);

                        if (thresholdMatch.Success)
                        {
                            if (double.TryParse(thresholdMatch.Groups["Threshold"].Value, out var specEValueThreshold))
                            {
                                mMzRefinerSpecEValueThreshold = specEValueThreshold;
                            }
                            else
                            {
                                LogWarning("MZRefinery SpecEValue threshold not numeric in the .tsv file: " +
                                           thresholdMatch.Groups["Threshold"].Value);
                            }
                        }
                        else
                        {
                            LogWarning("MZRefinery SpecEValue threshold not numeric in the .tsv file: " +
                                       thresholdRange);
                        }

                        if (!string.IsNullOrWhiteSpace(shiftMethodMS1))
                        {
                            mMzRefineryCorrectionMode = shiftMethodMS1;
                        }
                        else if (!string.IsNullOrWhiteSpace(shiftMethodMS2))
                        {
                            mMzRefineryCorrectionMode = shiftMethodMS2;
                        }

                        mMzRefinerGoodMS1Spectra = includedMS1;
                        mMzRefinerGoodMS2FragmentIons = includedMS2;

                        var logMessage = string.Format("MzRefinery stats: included {0:#,##0} MS1 spectra and {1:#,##0} MS2 fragment ions; " +
                                                       "excluded {2:#,##0} by score and {3:#,##0} by mass error; " +
                                                       "tolerance for 99% of data was {4:F2} ppm for MS1 and {5:F2} pm for MS2; " +
                                                       "StDev was {6:F2} for MS1 and {7:F2} for MS2", includedMS1, includedMS2,
                                                       countExcludedByScore, countExcludedByMassError,
                                                       toleranceFor99PctMS1, toleranceFor99PctMS2,
                                                       finalStDevMS1, finalStDevMS2);

                        LogMessage(logMessage);
                        break;
                    }
                }

                if (!string.IsNullOrEmpty(mMzRefineryCorrectionMode))
                {
                    string correctionMode;

                    if (mMzRefineryCorrectionMode.Equals("global"))
                    {
                        correctionMode = "a global shift";
                    }
                    else
                    {
                        // Correction mode is either "scan time" or "m/z"
                        correctionMode = mMzRefineryCorrectionMode;
                    }

                    var logMessage = string.Format(
                        "MzRefinery shifted data using {0}, filtering on SpecEValue <= {1:0.###E+00}; used {2:#,##0} MS1 scans and {3:#,##0} MS2 fragment ions",
                        correctionMode, mMzRefinerSpecEValueThreshold, mMzRefinerGoodMS1Spectra, mMzRefinerGoodMS2FragmentIons
                    );

                    mEvalMessage = logMessage;

                    LogMessage(logMessage);
                }
            }
            catch (Exception ex)
            {
                LogError("Error parsing mzRefinement.tsv file: " + ex.Message, ex);
            }
        }

        private bool PostProcessMzRefineryResults(FileSystemInfo msgfPlusResults, FileInfo fixedMSXmlFile, bool skippedMSGFPlus)
        {
            var originalMsXmlFilePath = Path.Combine(mWorkDir, mDatasetName + fixedMSXmlFile.Extension);

            try
            {
                // Create the plots
                var success = StartPpmErrorCharter(msgfPlusResults, fixedMSXmlFile);

                if (!success)
                {
                    return false;
                }

                if (!mSkipMzRefinery)
                {
                    // Store the PPM Mass Errors in the database
                    success = StorePPMErrorStatsInDB();

                    if (!success)
                    {
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error creating PPM Error charters", ex);
                return false;
            }

            try
            {
                if (File.Exists(originalMsXmlFilePath))
                {
                    // Delete the original .mzML file
                    DeleteFileWithRetries(originalMsXmlFilePath, mDebugLevel, 2);
                }
            }
            catch (Exception ex)
            {
                LogError("Error replacing the original .mzML file with the updated version; cannot delete original", ex);
                return false;
            }

            try
            {
                // Rename the fixed mzML file
                fixedMSXmlFile.MoveTo(originalMsXmlFilePath);
            }
            catch (Exception ex)
            {
                LogError("Error replacing the original .mzML file with the updated version; cannot rename the fixed file", ex);
                return false;
            }

            try
            {
                // Compress the .mzXML or .mzML file
                var success = mZipTools.GZipFile(fixedMSXmlFile.FullName, true);

                if (!success)
                {
                    LogError(mZipTools.Message);
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError("Error compressing the fixed .mzXML/.mzML file", ex);
                return false;
            }

            try
            {
                var mzRefFileGzipped = new FileInfo(fixedMSXmlFile.FullName + AnalysisResources.DOT_GZ_EXTENSION);

                // Copy the .mzXML.gz or .mzML.gz file to the cache
                var remoteCacheFilePath = CopyFileToServerCache(mMSXmlCacheFolder.FullName, mzRefFileGzipped.FullName, purgeOldFilesIfNeeded: true);

                if (string.IsNullOrEmpty(remoteCacheFilePath))
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("CopyFileToServerCache returned false for " + mzRefFileGzipped.Name);
                    }
                    return false;
                }

                // Create the _CacheInfo.txt file
                var cacheInfoFilePath = mzRefFileGzipped.FullName + "_CacheInfo.txt";
                using (var writer = new StreamWriter(new FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    writer.WriteLine(remoteCacheFilePath);
                }

                mJobParams.AddResultFileToSkip(mzRefFileGzipped.Name);
            }
            catch (Exception ex)
            {
                LogError("Error copying the .mzML.gz file to the remote cache folder", ex);
                return false;
            }

            if (skippedMSGFPlus)
            {
                msgfPlusResults.Delete();
                return true;
            }

            // Compress the MS-GF+ .mzID file
            var gzipSuccess = CompressMSGFPlusResults(msgfPlusResults);

            return gzipSuccess;
        }

        /// <summary>
        /// Cache the parent ion m/z values and charges in a dictionary
        /// </summary>
        /// <param name="parentIonMZs">
        /// Dictionary where keys are scan numbers are values are a dictionary of parent ion charge and m/z for a given scan
        /// </param>
        private bool ReadParentIonMZsFromCDTA(out Dictionary<int, Dictionary<int, double>> parentIonMZs)
        {
            parentIonMZs = new Dictionary<int, Dictionary<int, double>>();

            try
            {
                var cdtaFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + AnalysisResources.CDTA_EXTENSION));

                if (!cdtaFile.Exists)
                {
                    LogError("_dta.txt file not found: " + cdtaFile.FullName);
                    return false;
                }

                LogMessage("Caching parent ion info using " + cdtaFile.Name);

                var reader = new clsDtaTextFileReader(true);

                var success = reader.OpenFile(cdtaFile.FullName);

                if (!success)
                {
                    LogError("Error opening the _dta.txt file: " + cdtaFile.Name);
                    return false;
                }

                while (reader.ReadNextSpectrum(out _, out var spectrumHeaderInfo))
                {
                    if (!parentIonMZs.TryGetValue(spectrumHeaderInfo.ScanNumberStart, out var parentIonsForScan))
                    {
                        parentIonsForScan = new Dictionary<int, double>();
                        parentIonMZs.Add(spectrumHeaderInfo.ScanNumberStart, parentIonsForScan);
                    }

                    foreach (var parentIonCharge in spectrumHeaderInfo.ParentIonCharges)
                    {
                        if (parentIonCharge > 0 && !parentIonsForScan.ContainsKey(parentIonCharge))
                        {
                            parentIonsForScan.Add(parentIonCharge, spectrumHeaderInfo.ParentIonMZ);
                        }
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error copying the .mzML.gz file to the remote cache folder", ex);
                return false;
            }
        }

        /// <summary>
        /// Start mzRefinery
        /// </summary>
        /// <param name="originalMSXmlFile">.mzML file</param>
        /// <param name="msgfPlusResults">.mzid file from MS-GF+</param>
        private bool StartMzRefinery(FileInfo originalMSXmlFile, FileSystemInfo msgfPlusResults)
        {
            mConsoleOutputErrorMsg = string.Empty;

            LogMessage("Running MzRefinery using MSConvert");

            if (originalMSXmlFile.DirectoryName == null)
            {
                LogError("Cannot determine the parent directory of the input file: " + originalMSXmlFile.FullName);
                return false;
            }
            // Set up and execute a program runner to run MSConvert
            // Provide the path to the .mzML file plus the --filter switch with the information required to run MzRefiner

            var outputFile = new FileInfo(Path.Combine(originalMSXmlFile.DirectoryName,
                Path.GetFileNameWithoutExtension(originalMSXmlFile.Name) + "_FIXED.mzML"));

            var arguments = originalMSXmlFile.FullName +
                            " --outfile " + outputFile.Name +
                            " --filter \"mzRefiner " + msgfPlusResults.FullName;

            // MzRefiner will perform a segmented correction if there are at least 500 matches; it will perform a global shift if between 100 and 500 matches
            // The data is initially filtered by MSGF SpecProb <= 1e-10
            // The reason that we prepend "1e-10" with a dash is to indicate a range of "-infinity to 1e-10"
            arguments += " thresholdValue=-1e-10";

            // If there are not 500 matches with 1e-10, the threshold value is multiplied by the thresholdStep value
            // This process is continued at most maxSteps times
            // Thus, using 10 and 2 means the thresholds that will be considered are 1e-10, 1e-9, and 1e-8
            arguments += " thresholdStep=10";
            arguments += " maxSteps=2\"";

            // These switches assure that the output file is a 32-bit mzML file
            arguments += " --32 --mzML";

            if (mDebugLevel >= 1)
            {
                LogDebug(mMSConvertProgLoc + " " + arguments);
            }

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = Path.Combine(mWorkDir, MZ_REFINERY_CONSOLE_OUTPUT)
            };

            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mProgress = MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE;

            mProgRunnerMode = MzRefinerProgRunnerMode.MzRefiner;

            // Start MSConvert and wait for it to exit
            var success = mCmdRunner.RunProgram(mMSConvertProgLoc, arguments, "MSConvert_MzRefinery", true);

            mProgRunnerMode = MzRefinerProgRunnerMode.Unknown;

            if (!mCmdRunner.WriteConsoleOutputToFile)
            {
                // Write the console output to a text file
                Global.IdleLoop(0.25);

                using var writer = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine(mCmdRunner.CachedConsoleOutput);
            }

            // Parse the console output file one more time to check for errors
            Global.IdleLoop(0.25);
            ParseMSConvertConsoleOutputFile(mCmdRunner.ConsoleOutputFilePath);

            // Parse the .mzRefinement.tsv file to update mMzRefineryCorrectionMode, mMzRefinerGoodDataPoints, and mMzRefinerSpecEValueThreshold
            // We will also extract out the final MS-GF:SpecEValue used for filtering the data
            ParseMzRefinementStatsFile(msgfPlusResults);

            if (!string.IsNullOrWhiteSpace(mCmdRunner.CachedConsoleErrors))
            {
                success = false;

                if (mCmdRunner.CachedConsoleErrors.Trim().Equals("[MSData::stringToPair] Bad format:"))
                {
                    // Ignore this error if a _FIXED.mzML file was created and it is of comparable size to the input file
                    outputFile.Refresh();

                    if (outputFile.Exists && outputFile.Length >= originalMSXmlFile.Length * 0.95)
                    {
                        LogWarning(
                            "Ignoring error '[MSData::stringToPair] Bad format' since the _FIXED.mzML was created " +
                            "and is similar in size to the input file ({0:F0} MB vs. {1:F0} MB)",
                            outputFile.Length / 1024.0 / 1024.0,
                            originalMSXmlFile.Length / 1024.0 / 1024.0);

                        success = true;
                    }
                }

                if (!success)
                {
                    // Append the error messages to the log
                    // Note that ProgRunner will have already included them in the ConsoleOutput.txt file

                    var consoleError = "Console error: " + mCmdRunner.CachedConsoleErrors.Replace(Environment.NewLine, "; ");

                    if (string.IsNullOrWhiteSpace(mConsoleOutputErrorMsg))
                    {
                        mConsoleOutputErrorMsg = consoleError;
                    }
                    else
                    {
                        LogError(consoleError);
                    }
                }
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);

                if (mConsoleOutputErrorMsg.Contains("No high-resolution data in input file"))
                {
                    mMessage = string.Empty;
                    LogError("No high-resolution data in input file; cannot use MzRefinery on this dataset");
                    mUnableToUseMzRefinery = true;
                    mForceGeneratePPMErrorPlots = false;
                }
                else if (mConsoleOutputErrorMsg.Contains("No significant peak (ppm error histogram) found"))
                {
                    mMessage = string.Empty;
                    LogError("Significant peak not found in the ppm error histogram; cannot use MzRefinery on this dataset");
                    mUnableToUseMzRefinery = true;
                    mForceGeneratePPMErrorPlots = true;
                }
                else
                {
                    // ReSharper disable once CommentTypo
                    // Look for a match to Less than 100 (0) values in identfile that pass the threshold

                    var thresholdWarning = new Regex("Less than 100 .+ values in identfile .+ pass the threshold");

                    if (thresholdWarning.IsMatch(mConsoleOutputErrorMsg))
                    {
                        mMessage = string.Empty;
                        LogError("Not enough confidently identified PSMs; cannot use MzRefinery on this dataset");
                        mUnableToUseMzRefinery = true;
                        mForceGeneratePPMErrorPlots = false;
                    }
                }
            }

            if (mUnableToUseMzRefinery)
                success = false;

            if (!success)
            {
                if (mUnableToUseMzRefinery)
                {
                    if (string.IsNullOrEmpty(mMessage))
                    {
                        LogError("MS-GF+ identified too few peptides; unable to use MzRefinery with this dataset");
                        mForceGeneratePPMErrorPlots = true;
                    }
                    else
                    {
                        LogErrorNoMessageUpdate("Unable to use MzRefinery with this dataset");
                    }
                }

                LogErrorNoMessageUpdate("Error running MSConvert/MzRefinery");

                if (!mUnableToUseMzRefinery)
                {
                    if (mCmdRunner.ExitCode != 0)
                    {
                        LogWarning("MSConvert/MzRefinery returned a non-zero exit code: " + mCmdRunner.ExitCode);
                    }
                    else
                    {
                        LogWarning("Call to MSConvert/MzRefinery failed (but exit code is 0)");
                    }
                }

                return false;
            }

            mProgress = PROGRESS_PCT_MzREFINERY_COMPLETE;
            mStatusTools.UpdateAndWrite(mProgress);

            if (mDebugLevel >= 3)
            {
                LogDebug("MzRefinery Complete");
            }

            return true;
        }

        private bool StartPpmErrorCharter(FileSystemInfo msgfPlusResults, FileSystemInfo fixedMSXmlFile)
        {
            LogMessage("Running PPMErrorCharter");

            // Set up and execute a program runner to run the PPMErrorCharter
            var arguments = " -I:" + msgfPlusResults.FullName +
                            " -EValue:" + mMzRefinerSpecEValueThreshold.ToString("0.###E+00") +
                            " -MzML:" + fixedMSXmlFile.FullName +
                            " -Python";

            if (mDebugLevel >= 1)
            {
                LogDebug(mPpmErrorCharterProgLoc + arguments);
            }

            mCmdRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = false,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = Path.Combine(mWorkDir, ERROR_CHARTER_CONSOLE_OUTPUT_FILE)
            };

            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mProgRunnerMode = MzRefinerProgRunnerMode.PPMErrorCharter;

            // Start the PPM Error Charter and wait for it to exit
            var success = mCmdRunner.RunProgram(mPpmErrorCharterProgLoc, arguments, "PPMErrorCharter", true);

            mProgRunnerMode = MzRefinerProgRunnerMode.Unknown;

            if (!success)
            {
                LogError("Error running PPMErrorCharter");

                if (mCmdRunner.ExitCode != 0)
                {
                    LogWarning("PPMErrorCharter returned a non-zero exit code: " + mCmdRunner.ExitCode);
                }
                else
                {
                    LogWarning("Call to PPMErrorCharter failed (but exit code is 0)");
                }

                return false;
            }

            // Make sure the plots were created
            var plotFiles = new List<FileInfo>
            {
                new(Path.Combine(mWorkDir, mDatasetName + "_MZRefinery_MassErrors.png")),
                new(Path.Combine(mWorkDir, mDatasetName + "_MZRefinery_Histograms.png"))
            };

            foreach (var plotFile in plotFiles)
            {
                if (!plotFile.Exists)
                {
                    LogError("PPMError plot not found: " + plotFile.Name);
                    return false;
                }
            }

            mProgress = PROGRESS_PCT_PLOTS_GENERATED;
            mStatusTools.UpdateAndWrite(mProgress);

            if (mDebugLevel >= 3)
            {
                LogDebug("PPMErrorCharter Complete");
            }

            return true;
        }

        private bool StorePPMErrorStatsInDB()
        {
            var massErrorExtractor = new MzRefineryMassErrorStatsExtractor(mMgrParams, mDebugLevel);

            var datasetID = mJobParams.GetJobParameter("DatasetID", 0);

            var consoleOutputFilePath = Path.Combine(mWorkDir, ERROR_CHARTER_CONSOLE_OUTPUT_FILE);
            var success = massErrorExtractor.ParsePPMErrorCharterOutput(mDatasetName, datasetID, mJob, consoleOutputFilePath);

            if (success)
            {
                mEvalMessage = Global.AppendToComment(mEvalMessage,
                    string.Format(
                        "Median mass error changed from {0:F2} ppm to {1:F2} ppm",
                        massErrorExtractor.MassErrorStats.MassErrorPPM,
                        massErrorExtractor.MassErrorStats.MassErrorPPMRefined));

                return true;
            }

            string errorMsg;

            if (string.IsNullOrEmpty(massErrorExtractor.ErrorMessage))
            {
                errorMsg = "Error parsing PMM Error Charter output to extract mass error stats";
            }
            else
            {
                errorMsg = massErrorExtractor.ErrorMessage;
            }

            LogErrorToDatabase(errorMsg + ", job " + mJob);
            mMessage = errorMsg;
            return false;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var toolVersionInfo = mMSGFPlusUtils.MSGFPlusVersion;

            // MSConvert
            var success = mToolVersionUtilities.GetMSConvertToolVersion(mMSConvertProgLoc, out var msConvertVersion);

            if (!success)
            {
                LogError(string.Format("Unable to determine the version of {0}", Path.GetFileName(mMSConvertProgLoc)), true);
            }
            else
            {
                toolVersionInfo = Global.AppendToComment(toolVersionInfo, msConvertVersion);
            }

            // Create file Tool_Version_Info_MSConvert.txt
            mToolVersionUtilities.SaveToolVersionInfoFile(mWorkDir, msConvertVersion, "MSConvert");

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo>
            {
                new(mMSGFPlusProgLoc),
                new(mMSConvertProgLoc),
                new(mPpmErrorCharterProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Error calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Update .mzML file using the parent ion m/z values listed in a _dta.txt file
        /// </summary>
        private bool UpdateMzMLUsingCDTA()
        {
            try
            {
                var success = ReadParentIonMZsFromCDTA(out var parentIonMZs);

                if (!success)
                    return false;

                var mzMLFilePath = Path.Combine(mWorkDir, mDatasetName + AnalysisResources.DOT_MZML_EXTENSION);
                var mzMLFile = new FileInfo(mzMLFilePath);
                var updatedMzMLFile = new FileInfo(Path.Combine(mWorkDir, mzMLFile.Name + ".new"));

                // Read the .mzML file using a StreamReader
                // Write a new .mzML file with corrected parent ion m/z values

                var reScanNumber = new Regex(@"scan=(?<Scan>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var reValueReplace = new Regex(@"value *= *""[0-9.-]+""", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var parentIonsNotMatched = 0;
                var parentIonsUpdated = 0;
                var parentIonsWithMultiChargeState = 0;

                var maxScanMS2 = parentIonMZs.Keys.Max();
                var lastProgress = DateTime.UtcNow;

                using (var reader = new StreamReader(new FileStream(mzMLFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(updatedMzMLFile.FullName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {
                    var updatedParentIonMz = string.Empty;
                    var updatedParentIonCharge = 0;

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrEmpty(dataLine))
                        {
                            continue;
                        }

                        var dataLineTrimmed = dataLine.Trim();

                        // ReSharper disable once StringLiteralTypo
                        if (dataLineTrimmed.StartsWith("<indexedmzML"))
                        {
                            // The new .mzML file will not be indexed; skip this line
                            continue;
                        }

                        if (dataLineTrimmed.StartsWith("</mzML>"))
                        {
                            // Write this line, then stop reading (since the new file is not indexed)
                            writer.WriteLine(dataLine);
                            break;
                        }

                        if (dataLineTrimmed.StartsWith("<spectrum index"))
                        {
                            // Parse out the scan number
                            var match = reScanNumber.Match(dataLine);

                            if (!match.Success)
                            {
                                LogError("<spectrum> entry does not have scan=, " + dataLine);
                                return false;
                            }

                            var scanNumber = int.Parse(match.Groups["Scan"].Value);

                            updatedParentIonMz = string.Empty;
                            updatedParentIonCharge = 0;

                            if (parentIonMZs.TryGetValue(scanNumber, out var parentIons))
                            {
                                // Only update the parent ion info if DeconMSn chose a single charge state
                                // Ignore scans that are "2+ or 3+"
                                if (parentIons.Count == 0)
                                {
                                    parentIonsNotMatched++;
                                }
                                else if (parentIons.Count == 1)
                                {
                                    parentIonsUpdated++;

                                    updatedParentIonCharge = parentIons.First().Key;
                                    updatedParentIonMz = StringUtilities.DblToString(parentIons.First().Value, 6);
                                }
                                else
                                {
                                    parentIonsWithMultiChargeState++;
                                }

                                if (DateTime.UtcNow.Subtract(lastProgress).TotalSeconds > 5)
                                {
                                    lastProgress = DateTime.UtcNow;
                                    var progress = scanNumber / (float)maxScanMS2 * 100;
                                    LogDebug("Updating parent ion m/z's in the mzML file, {0:F1}% complete", progress);
                                }
                            }
                        }

                        if (updatedParentIonCharge > 0)
                        {
                            var replaceValue = false;
                            var replacementContext = string.Empty;
                            var newValue = string.Empty;

                            if (dataLineTrimmed.StartsWith("<userParam"))
                            {
                                // Look for <userParam name="[Thermo Trailer Extra]Monoisotopic M/Z:" value="1013.7643443411453"
                                if (dataLine.IndexOf("[Thermo Trailer Extra]Monoisotopic M/Z", StringComparison.OrdinalIgnoreCase) > 0)
                                {
                                    replaceValue = true;
                                    replacementContext = "<userParam> [Thermo Trailer Extra]";
                                    newValue = updatedParentIonMz;
                                }
                            }
                            else if (dataLineTrimmed.StartsWith("<cvParam"))
                            {
                                // Look for <cvParam cvRef="MS" accession="MS:1000827" name="isolation window target m/z" value="1013.7643443411457"
                                if (dataLine.IndexOf("MS:1000827", StringComparison.OrdinalIgnoreCase) > 0)
                                {
                                    replaceValue = true;
                                    replacementContext = "<cvParam> MS:1000827";
                                    newValue = updatedParentIonMz;
                                }

                                // Look for <cvParam cvRef="MS" accession="MS:1000744" name="selected ion m/z" value="1013.7643443411457"
                                if (dataLine.IndexOf("MS:1000744", StringComparison.OrdinalIgnoreCase) > 0)
                                {
                                    replaceValue = true;
                                    replacementContext = "<cvParam> MS:1000744";
                                    newValue = updatedParentIonMz;
                                }

                                // Look for <cvParam cvRef="MS" accession="MS:1000041" name="charge state" value="2"/>
                                if (dataLine.IndexOf("MS:1000041", StringComparison.OrdinalIgnoreCase) > 0)
                                {
                                    replaceValue = true;
                                    replacementContext = "<cvParam> MS:1000041";
                                    newValue = updatedParentIonCharge.ToString();
                                }
                            }

                            if (replaceValue)
                            {
                                // Replace the m/z or charge
                                var match = reValueReplace.Match(dataLine);

                                if (!match.Success)
                                {
                                    LogError(replacementContext + " does not have value=, " + dataLine);
                                    return false;
                                }

                                var updatedLine = dataLine.Replace(match.Groups[0].Value, "value=\"" + newValue + "\"");

                                writer.WriteLine(updatedLine);
                                continue;
                            }
                        }

                        writer.WriteLine(dataLine);
                    }
                }

                LogMessage("Updated parent ion values in the mzML file. " +
                           "{0:#,##0} updated; " +
                           "{1:#,##0} skipped due to ambiguous charge; " +
                           "{2:#,##0} skipped since not in the _dta.txt file", parentIonsUpdated, parentIonsWithMultiChargeState, parentIonsNotMatched);

                try
                {
                    // Replace the original file with the updated file
                    mzMLFile.MoveTo(mzMLFile.FullName + ".old");
                    mJobParams.AddResultFileToSkip(mzMLFile.Name);

                    updatedMzMLFile.MoveTo(mzMLFilePath);
                }
                catch (Exception ex)
                {
                    LogError("Error replacing the original .mzML file with the updated file", ex);
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Error updating the .mzML using parent ion m/z values in the _dta.txt file", ex);
                return false;
            }
        }

        private void UpdateProgress(float progressCompleteOverall)
        {
            if (mProgress < progressCompleteOverall)
            {
                mProgress = progressCompleteOverall;
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
