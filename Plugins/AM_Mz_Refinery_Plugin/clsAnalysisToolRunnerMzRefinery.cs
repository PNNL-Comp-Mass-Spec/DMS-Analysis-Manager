//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 09/05/2014
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;

using AnalysisManagerBase;
using System.IO;
using AnalysisManagerMSGFDBPlugIn;
using System.Text.RegularExpressions;

namespace AnalysisManagerMzRefineryPlugIn
{
    /// <summary>
    /// Class for running Mz Refinery to recalibrate m/z values in a .mzXML or .mzML file
    /// </summary>
    public class clsAnalysisToolRunnerMzRefinery : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"
        private const float PROGRESS_PCT_STARTING = 1;
        private const float PROGRESS_PCT_MZREFINERY_COMPLETE = 97;
        private const float PROGRESS_PCT_PLOTS_GENERATED = 98;

        private const float PROGRESS_PCT_COMPLETE = 99;
        private const string MZ_REFINERY_CONSOLE_OUTPUT = "MSConvert_MzRefinery_ConsoleOutput.txt";

        private const string ERROR_CHARTER_CONSOLE_OUTPUT_FILE = "PPMErrorCharter_ConsoleOutput.txt";

        public const string MSGFPLUS_MZID_SUFFIX = "_msgfplus.mzid";
        private enum eMzRefinerProgRunnerMode
        {
            Unknown = 0,
            MSGFPlus = 1,
            MzRefiner = 2,
            PPMErrorCharter = 3
        }
        #endregion

        #region "Module Variables"

        private bool mToolVersionWritten;

        private string mConsoleOutputErrorMsg;
        private string mMSGFPlusProgLoc;
        private string mMSConvertProgLoc;

        private string mPpmErrorCharterProgLoc;

        private eMzRefinerProgRunnerMode mProgRunnerMode;
        private bool mMSGFPlusComplete;

        private DateTime mMSGFPlusCompletionTime;
        private bool mSkipMzRefinery;
        private bool m_UnableToUseMzRefinery;

        private bool m_ForceGeneratePPMErrorPlots;
        private string mMzRefineryCorrectionMode;
        private int mMzRefinerGoodDataPoints;

        private double mMzRefinerSpecEValueThreshold;

        private MSGFPlusUtils mMSGFPlusUtils;

        private DirectoryInfo mMSXmlCacheFolder;

        private clsRunDosProgram mCmdRunner;
        #endregion

        #region "Methods"

        /// <summary>
        /// Runs MSGF+ then runs MSConvert with the MzRefiner filter
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
                    LogDebug("clsAnalysisToolRunnerMzRefinery.RunTool(): Enter");
                }

                // Initialize class-wide variables that will be updated later
                mMzRefineryCorrectionMode = string.Empty;
                mMzRefinerGoodDataPoints = 0;
                mMzRefinerSpecEValueThreshold = 1E-10;

                m_UnableToUseMzRefinery = false;
                m_ForceGeneratePPMErrorPlots = false;

                // Verify that program files exist

                // Determine the path to MSConvert (as of March 10, 2015 the official release of Proteowizard contains MSConvert.exe that supports the MzRefiner filter)
                mMSConvertProgLoc = DetermineProgramLocation("ProteoWizardDir", "msconvert.exe");

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

                // javaProgLoc will typically be "C:\Program Files\Java\jre8\bin\Java.exe"
                var javaProgLoc = GetJavaProgLoc();
                if (string.IsNullOrEmpty(javaProgLoc))
                {
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var msXMLCacheFolderPath = m_mgrParams.GetParam("MSXMLCacheFolderPath", string.Empty);
                mMSXmlCacheFolder = new DirectoryInfo(msXMLCacheFolderPath);

                if (!mMSXmlCacheFolder.Exists)
                {
                    LogError("MSXmlCache folder not found: " + msXMLCacheFolderPath);
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var msXmlFileExtension = clsAnalysisResources.DOT_MZML_EXTENSION;

                var msXmlOutputType = m_jobParams.GetJobParameter("MSXMLOutputType", string.Empty);
                if (msXmlOutputType.ToLower() == "mzxml")
                {
                    msXmlFileExtension = clsAnalysisResources.DOT_MZXML_EXTENSION;
                }

                // Look for existing MSGF+ results (which would have been retrieved by clsAnalysisResourcesMzRefinery)

                var fiMSGFPlusResults = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + MSGFPLUS_MZID_SUFFIX));
                var skippedMSGFPlus = false;

                CloseOutType result;
                if (fiMSGFPlusResults.Exists)
                {
                    result = CloseOutType.CLOSEOUT_SUCCESS;
                    skippedMSGFPlus = true;
                    m_jobParams.AddResultFileToSkip(fiMSGFPlusResults.Name);
                }
                else
                {
                    // Run MSGF+ (includes indexing the fasta file)
                    result = RunMSGFPlus(javaProgLoc, msXmlFileExtension, out fiMSGFPlusResults);
                }

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        m_message = "Unknown error running MSGF+ prior to running MzRefiner";
                    }
                    return result;
                }

                mCmdRunner = null;

                var processingError = false;

                var fiOriginalMSXmlFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + msXmlFileExtension));
                var fiFixedMSXmlFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "_FIXED" + msXmlFileExtension));

                m_jobParams.AddResultFileToSkip(fiOriginalMSXmlFile.Name);
                m_jobParams.AddResultFileToSkip(fiFixedMSXmlFile.Name);

                if (mSkipMzRefinery)
                {
                    // Rename the original file to have the expected name of the fixed mzML file
                    // Required for PostProcessMzRefineryResults to work properly
                    fiOriginalMSXmlFile.MoveTo(fiFixedMSXmlFile.FullName);
                }
                else
                {
                    // Run MSConvert with the MzRefiner filter
                    var mzRefinerySuccess = StartMzRefinery(fiOriginalMSXmlFile, fiMSGFPlusResults);

                    if (!mzRefinerySuccess)
                    {
                        processingError = true;
                    }
                    else
                    {
                        if (mMzRefineryCorrectionMode.StartsWith("Chose no shift"))
                        {
                            // No valid peak was found; a result file may not exist
                            fiFixedMSXmlFile.Refresh();
                            if (!fiFixedMSXmlFile.Exists)
                            {
                                // Rename the original file to have the expected name of the fixed mzML file
                                // Required for PostProcessMzRefineryResults to work properly
                                fiOriginalMSXmlFile.MoveTo(fiFixedMSXmlFile.FullName);
                            }
                        }
                    }
                }

                if (!processingError)
                {
                    // Look for the results file
                    fiFixedMSXmlFile.Refresh();
                    if (fiFixedMSXmlFile.Exists)
                    {
                        var postProcessSuccess = PostProcessMzRefineryResults(fiMSGFPlusResults, fiFixedMSXmlFile);
                        if (!postProcessSuccess)
                            processingError = true;
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            m_message = "MzRefinery results file not found: " + fiFixedMSXmlFile.Name;
                        }
                        processingError = true;
                    }
                }

                if (m_UnableToUseMzRefinery)
                {
                    fiMSGFPlusResults.Refresh();
                    if (m_ForceGeneratePPMErrorPlots && fiMSGFPlusResults.Exists)
                    {
                        try
                        {
                            StartPpmErrorCharter(fiMSGFPlusResults);
                        }
                        catch (Exception ex)
                        {
                            // Treat this as a warning
                            LogWarning("Error generating PPMError plots for debugging purposes: " + ex.Message);
                        }
                    }

                    using (var swMessageFile = new StreamWriter(new FileStream(
                        Path.Combine(m_WorkDir, "NOTE - Orphan folder; safe to delete.txt"),
                        FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        swMessageFile.WriteLine("This folder contains MSGF+ results and the MzRefinery log file from a failed attempt at running MzRefinery for job " + m_JobNum + ".");
                        swMessageFile.WriteLine("The files can be used to investigate the MzRefinery failure.");
                        swMessageFile.WriteLine("The folder can be safely deleted.");
                    }
                }

                m_progress = PROGRESS_PCT_COMPLETE;

                // Stop the job timer
                m_StopTime = DateTime.UtcNow;

                // Add the current job data to the summary file
                UpdateSummaryFile();

                mCmdRunner = null;

                // Make sure objects are released
                System.Threading.Thread.Sleep(500);
                PRISM.clsProgRunner.GarbageCollectNow();

                if (processingError)
                {
                    var msgfPlusResultsExist = false;

                    if ((fiMSGFPlusResults != null) && fiMSGFPlusResults.Exists)
                    {
                        // MSGF+ succeeded but MzRefinery or PostProcessing failed
                        // We will mark the job as failed, but we want to move the MSGF+ results into the transfer folder

                        if (skippedMSGFPlus)
                        {
                            msgfPlusResultsExist = true;
                        }
                        else
                        {
                            msgfPlusResultsExist = CompressMSGFPlusResults(fiMSGFPlusResults);
                        }
                    }

                    if (!msgfPlusResultsExist)
                    {
                        // Move the source files and any results to the Failed Job folder
                        // Useful for debugging problems
                        CopyFailedResultsToArchiveFolder(msXmlFileExtension);
                        return CloseOutType.CLOSEOUT_FAILED;
                    }
                }


                var success = CopyResultsToTransferDirectory();

                if (!success)
                    return CloseOutType.CLOSEOUT_FAILED;

                if (!processingError)
                    return CloseOutType.CLOSEOUT_SUCCESS;

                // If we get here, MSGF+ succeeded, but MzRefinery or PostProcessing failed
                LogWarning("Processing failed; see results at " + m_jobParams.GetParam("transferFolderPath"));
                if (m_UnableToUseMzRefinery)
                {
                    return CloseOutType.CLOSEOUT_UNABLE_TO_USE_MZ_REFINERY;
                }

                return CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                m_message = "Error in MzRefineryPlugin->RunTool";
                LogError(m_message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

        }

        /// <summary>
        /// Index the Fasta file (if needed) then run MSGF+
        /// </summary>
        /// <param name="javaProgLoc">Path to Java</param>
        /// <param name="msXmlFileExtension">.mzXML or .mzML</param>
        /// <param name="fiMSGFPlusResults">Output: MSGF+ results file</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private CloseOutType RunMSGFPlus(string javaProgLoc, string msXmlFileExtension, out FileInfo fiMSGFPlusResults)
        {
            const string strMSGFJarfile = MSGFPlusUtils.MSGFPLUS_JAR_NAME;

            fiMSGFPlusResults = null;

            // Determine the path to MSGF+
            mMSGFPlusProgLoc = DetermineProgramLocation("MSGFPlusProgLoc", strMSGFJarfile);

            if (string.IsNullOrWhiteSpace(mMSGFPlusProgLoc))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Note: we will store the MSGF+ version info in the database after the first line is written to file MSGFPlus_ConsoleOutput.txt
            mToolVersionWritten = false;

            mMSGFPlusComplete = false;

            // These two variables are required for the call to ParseMSGFPlusParameterFile
            // They are blank because the source file is a mzML file, and that file includes scan type information
            var strScanTypeFilePath = string.Empty;
            var strAssumedScanType = string.Empty;

            // Initialize mMSGFPlusUtils
            mMSGFPlusUtils = new MSGFPlusUtils(m_mgrParams, m_jobParams, m_WorkDir, m_DebugLevel);
            RegisterEvents(mMSGFPlusUtils);

            mMSGFPlusUtils.IgnorePreviousErrorEvent += MSGFPlusUtils_IgnorePreviousErrorEvent;

            // Get the FASTA file and index it if necessary
            // Note: if the fasta file is over 50 MB in size, only use the first 50 MB

            // Passing in the path to the parameter file so we can look for TDA=0 when using large .Fasta files
            var strParameterFilePath = Path.Combine(m_WorkDir, m_jobParams.GetJobParameter("MzRefParamFile", string.Empty));
            var javaExePath = string.Copy(javaProgLoc);
            var msgfplusJarFilePath = string.Copy(mMSGFPlusProgLoc);

            const int maxFastaFileSizeMB = 50;

            // Initialize the fasta file; truncating it if it is over 50 MB in size
            var result = mMSGFPlusUtils.InitializeFastaFile(
                javaExePath, msgfplusJarFilePath,
                out var fastaFileSizeKB, out var fastaFileIsDecoy, out var fastaFilePath,
                strParameterFilePath, maxFastaFileSizeMB);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            var strInstrumentGroup = m_jobParams.GetJobParameter("JobParameters", "InstrumentGroup", string.Empty);

            // Read the MSGF+ Parameter File

            var overrideParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var jobScript = m_jobParams.GetJobParameter("ToolName", "");
            if (jobScript.ToLower().StartsWith("modplus"))
            {
                if (fastaFileIsDecoy)
                {
                    overrideParams.Add("TDA", "0");
                }
            }

            result = mMSGFPlusUtils.ParseMSGFPlusParameterFile(
                fastaFileSizeKB, fastaFileIsDecoy, strAssumedScanType, strScanTypeFilePath,
                strInstrumentGroup, strParameterFilePath, overrideParams, out var strMSGFPlusCmdLineOptions);

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            if (string.IsNullOrEmpty(strMSGFPlusCmdLineOptions))
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "Problem parsing MzRef parameter file to extract MGSF+ options";
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Look for extra parameters specific to MZRefinery
            var success = ExtractMzRefinerOptionsFromParameterFile(strParameterFilePath);
            if (!success)
            {
                m_message = "Error extracting MzRefinery options from parameter file " + Path.GetFileName(strParameterFilePath);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var resultsFileName = m_Dataset + MSGFPLUS_MZID_SUFFIX;
            fiMSGFPlusResults = new FileInfo(Path.Combine(m_WorkDir, resultsFileName));

            LogMessage("Running MSGF+");

            // If an MSGF+ analysis crashes with an "out-of-memory" error, we need to reserve more memory for Java
            // The amount of memory required depends on both the fasta file size and the size of the input .mzML file, since data from all spectra are cached in memory
            // Customize this on a per-job basis using the MSGFDBJavaMemorySize setting in the settings file
            var intJavaMemorySize = m_jobParams.GetJobParameter("MzRefMSGFPlusJavaMemorySize", 1500);
            if (intJavaMemorySize < 512)
                intJavaMemorySize = 512;

            // Set up and execute a program runner to run MSGF+
            var cmdStr = " -Xmx" + intJavaMemorySize + "M -jar " + msgfplusJarFilePath;

            // Define the input file, output file, and fasta file
            cmdStr += " -s " + m_Dataset + msXmlFileExtension;

            cmdStr += " -o " + fiMSGFPlusResults.Name;
            cmdStr += " -d " + PossiblyQuotePath(fastaFilePath);

            // Append the remaining options loaded from the parameter file
            cmdStr += " " + strMSGFPlusCmdLineOptions;

            // Make sure the machine has enough free memory to run MSGF+
            var blnLogFreeMemoryOnSuccess = !(m_DebugLevel < 1);

            if (!clsAnalysisResources.ValidateFreeMemorySize(intJavaMemorySize, "MSGF+", blnLogFreeMemoryOnSuccess))
            {
                m_message = "Not enough free memory to run MSGF+";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            success = StartMSGFPlus(javaExePath, "MSGF+", cmdStr);

            if (!success && string.IsNullOrEmpty(mMSGFPlusUtils.ConsoleOutputErrorMsg))
            {
                // Parse the console output file one more time in hopes of finding an error message
                ParseMSGFPlusConsoleOutputFile(m_WorkDir);
            }

            if (!mToolVersionWritten)
            {
                if (string.IsNullOrWhiteSpace(mMSGFPlusUtils.MSGFPlusVersion))
                {
                    ParseMSGFPlusConsoleOutputFile(m_WorkDir);
                }
                mToolVersionWritten = StoreToolVersionInfo();
            }

            if (!string.IsNullOrEmpty(mMSGFPlusUtils.ConsoleOutputErrorMsg))
            {
                LogError(mMSGFPlusUtils.ConsoleOutputErrorMsg);
            }

            var blnProcessingError = false;

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
                    msg = "MSGF+ log file reported it was complete, but aborted the ProgRunner since Java was frozen";
                }
                else
                {
                    msg = "Error running MSGF+";
                }

                LogError(msg);

                if (mMSGFPlusComplete)
                {
                    // Don't treat this as a fatal error
                    blnProcessingError = false;
                    m_EvalMessage = string.Copy(m_message);
                    m_message = string.Empty;
                }
                else
                {
                    blnProcessingError = true;
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
                m_progress = MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE;
                m_StatusTools.UpdateAndWrite(m_progress);
                if (m_DebugLevel >= 3)
                {
                    LogDebug("MSGF+ Search Complete");
                }
            }

            // Look for the .mzid file
            fiMSGFPlusResults.Refresh();

            if (!fiMSGFPlusResults.Exists)
            {
                if (string.IsNullOrEmpty(m_message))
                {
                    m_message = "MSGF+ results file not found: " + resultsFileName;
                }
                return CloseOutType.CLOSEOUT_FAILED;
            }

            m_jobParams.AddResultFileToSkip(MSGFPlusUtils.MOD_FILE_NAME);

            if (blnProcessingError)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool StartMSGFPlus(string javaExePath, string strSearchEngineName, string cmdStr)
        {
            if (m_DebugLevel >= 1)
            {
                LogDebug(javaExePath + " " + cmdStr);
            }

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = true;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, MSGFPlusUtils.MSGFPLUS_CONSOLE_OUTPUT_FILE);

            m_progress = MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_STARTING;

            mProgRunnerMode = eMzRefinerProgRunnerMode.MSGFPlus;

            // Start MSGF+ and wait for it to exit
            var success = mCmdRunner.RunProgram(javaExePath, cmdStr, strSearchEngineName, true);

            mProgRunnerMode = eMzRefinerProgRunnerMode.Unknown;

            return success;
        }

        private bool CompressMSGFPlusResults(FileInfo fiMSGFPlusResults)
        {
            try
            {
                // Compress the MSGF+ .mzID file
                var success = m_IonicZipTools.GZipFile(fiMSGFPlusResults.FullName, true);

                if (!success)
                {
                    m_message = m_IonicZipTools.Message;
                    return false;
                }

                m_jobParams.AddResultFileToSkip(fiMSGFPlusResults.Name);
                m_jobParams.AddResultFileToKeep(fiMSGFPlusResults.Name + clsAnalysisResources.DOT_GZ_EXTENSION);
            }
            catch (Exception ex)
            {
                m_message = "Error compressing the .mzID file";
                LogError(m_message, ex);
                return false;
            }

            return true;
        }

        private void CopyFailedResultsToArchiveFolder(string msXmlFileExtension)
        {

            try
            {
                var fiFiles = new DirectoryInfo(m_WorkDir).GetFiles("*" + msXmlFileExtension);
                foreach (var fiFileToDelete in fiFiles)
                {
                    fiFileToDelete.Delete();
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }

            base.CopyFailedResultsToArchiveFolder();

        }

        private bool ExtractMzRefinerOptionsFromParameterFile(string strParameterFilePath)
        {
            mSkipMzRefinery = false;

            try
            {
                using (var srParamFile = new StreamReader(new FileStream(strParameterFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srParamFile.EndOfStream)
                    {
                        var strLineIn = srParamFile.ReadLine();

                        var kvSetting = clsGlobal.GetKeyValueSetting(strLineIn);

                        if (!string.IsNullOrWhiteSpace(kvSetting.Key))
                        {
                            switch (kvSetting.Key)
                            {
                                case "SkipMzRefinery":
                                    var strValue = kvSetting.Value;
                                    if (bool.TryParse(strValue, out var value))
                                    {
                                        mSkipMzRefinery = value;
                                    }
                                    break;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error in ExtractMzRefinerOptionsFromParameterFile", ex);
                return false;
            }

            return true;
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

            if (mProgRunnerMode == eMzRefinerProgRunnerMode.MSGFPlus)
            {
                ParseMSGFPlusConsoleOutputFile(m_WorkDir);
                if (!mToolVersionWritten && !string.IsNullOrWhiteSpace(mMSGFPlusUtils.MSGFPlusVersion))
                {
                    mToolVersionWritten = StoreToolVersionInfo();
                }

                UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

                LogProgress("MSGF+ for MzRefinery");

                if (m_progress < MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE)
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
                                         MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE.ToString("0") + "% complete for 5 minutes; " +
                                         "aborting since Java appears frozen";
                    LogWarning(warningMessage);

                    // Bump up mMSGFPlusCompletionTime by one hour
                    // This will prevent this function from logging the above message every 30 seconds if the .abort command fails
                    mMSGFPlusCompletionTime = mMSGFPlusCompletionTime.AddHours(1);

                    mCmdRunner.AbortProgramNow();
                }
            }
            else if (mProgRunnerMode == eMzRefinerProgRunnerMode.MzRefiner)
            {
                ParseMSConvertConsoleOutputfile(Path.Combine(m_WorkDir, MZ_REFINERY_CONSOLE_OUTPUT));

                UpdateProgRunnerCpuUsage(mCmdRunner, SECONDS_BETWEEN_UPDATE);

                LogProgress("MzRefinery");
            }
            else
            {
                LogProgress("MzRefinery, unknown step: " + mProgRunnerMode);
            }
        }

        /// <summary>
        /// Parse the MSGF+ console output file to determine the MSGF+ version and to track the search progress
        /// </summary>
        /// <remarks></remarks>
        private void ParseMSGFPlusConsoleOutputFile(string workingDirectory)
        {
            try
            {
                if ((mMSGFPlusUtils != null))
                {
                    var msgfPlusProgress = mMSGFPlusUtils.ParseMSGFPlusConsoleOutputFile(workingDirectory);
                    UpdateProgress(msgfPlusProgress);
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing MSGF+ console output file: " + ex.Message);
                }
            }
        }

        /// <summary>
        /// Parse the MSConvert console output file to look for errors from MzRefiner
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseMSConvertConsoleOutputfile(string strConsoleOutputFilePath)
        {
            // Example console output
            //
            // format: mzML
            //     m/z: Compression-None, 32-bit
            //     intensity: Compression-None, 32-bit
            //     rt: Compression-None, 32-bit
            // ByteOrder_LittleEndian
            //  indexed="true"
            // outputPath: .
            // extension: .mzML
            // contactFilename:
            // filters:
            //   mzRefiner E:\DMS_WorkDir\Pcarb001_LTQFT_run1_23Sep05_Andro_0705-06_msgfplus.mzid thresholdValue=-1e-10 thresholdStep=10 maxSteps=3
            //
            // filenames:
            //   E:\DMS_WorkDir\Pcarb001_LTQFT_run1_23Sep05_Andro_0705-06.mzML
            //
            // processing file: E:\DMS_WorkDir\Pcarb001_LTQFT_run1_23Sep05_Andro_0705-06.mzML
            // Reading file "E:\DMS_WorkDir\Pcarb001_LTQFT_run1_23Sep05_Andro_0705-06_msgfplus.mzid"...
            // Adjusted filters:
            // 	Old: MS-GF:SpecEValue; -1.79769e+308 <= value && value <= 1e-010
            // 	New: MS-GF:SpecEValue; -1.79769e+308 <= value && value <= 1e-009
            // Adjusted filters:
            // 	Old: MS-GF:SpecEValue; -1.79769e+308 <= value && value <= 1e-009
            // 	New: MS-GF:SpecEValue; -1.79769e+308 <= value && value <= 1e-008
            // Adjusted filters:
            // 	Old: MS-GF:SpecEValue; -1.79769e+308 <= value && value <= 1e-008
            // 	New: MS-GF:SpecEValue; -1.79769e+308 <= value && value <= 1e-007
            // 	Filtered out 13014 identifications because of score.
            // 	Filtered out 128 identifications because of mass error.
            // 	Good data points:                                 839
            // 	Average: global ppm Errors:                       0.77106
            // 	Systematic Drift (mode):                          -11.5
            // 	Systematic Drift (median):                        -11.5
            // 	Measurement Precision (stdev ppm):                26.0205
            // 	Measurement Precision (stdev(mode) ppm):          28.7674
            // 	Measurement Precision (stdev(median) ppm):        28.7674
            // 	Average BinWise stdev (scan):                     24.6157
            // 	Expected % Improvement (scan):                    5.39302
            // 	Expected % Improvement (scan)(mode):              14.4319
            // 	Expected % Improvement (scan)(median):            5.39907
            // 	Average BinWise stdev (smoothed scan):            25.4906
            // 	Expected % Improvement (smoothed scan):           2.03045
            // 	Expected % Improvement (smoothed scan)(mode):     11.3906
            // 	Expected % Improvement (smoothed scan)(median):   2.03672
            // 	Average BinWise stdev (mz):                       23.4562
            // 	Expected % Improvement (mz):                      9.84931
            // 	Expected % Improvement (mz)(mode):                18.4625
            // 	Expected % Improvement (mz)(median):              9.85509
            // 	Average BinWise stdev (smoothed mz):              25.5205
            // 	Expected % Improvement (smoothed mz):             1.91564
            // 	Expected % Improvement (smoothed mz)(mode):       11.2868
            // 	Expected % Improvement (smoothed mz)(median):     1.92192
            // Chose global shift...
            // 	Estimated final stDev:                            26.0205
            // 	Estimated tolerance for 99%: 0 +/-                78.0616
            // writing output file: .\Pcarb001_LTQFT_run1_23Sep05_Andro_0705-06_FIXED.mzML

            // Example warning for sparse data file
            // Low number of good identifications found. Will not perform dependent shifts.
            //    Less than 500 (123) results after filtering.
            //    Filtered out 6830 identifications because of score.

            // Example error for really sparse data file
            // Excluding file ".\mzmlRefineryData\Cyanothece_bad\Cyano_GC_07_10_25Aug09_Draco_09-05-02.mzid" from data set
            //    Less than 100 (16) results after filtering.
            //    Filtered out 4208 identifications because of score.
            //    Filtered out 0 identifications because of mass error.

            // Example error for no data passing the filters
            // Excluding file "C:\DMS_WorkDir1\Caulo_pY_Run5_msgfplus.mzid" from data set.
            //    Less than 100 (0) results after filtering.
            //    Filtered out 8 identifications because of score.
            //    Filtered out 0 identifications because of mass error.

            var reResultsAfterFiltering = new Regex(@"Less than \d+ \(\d+\) results after filtering", RegexOptions.Compiled);

            var reGoodDataPoints = new Regex(@"Good data points:[^\d]+(\d+)", RegexOptions.Compiled);
            var reSpecEValueThreshold = new Regex(@"New: MS-GF:SpecEValue;.+value <= ([^ ]+)", RegexOptions.Compiled);

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
                        var strDataLine = srInFile.ReadLine();

                        if (!string.IsNullOrWhiteSpace(strDataLine))
                        {
                            var strDataLineLCase = strDataLine.Trim().ToLower();

                            if (strDataLineLCase.StartsWith("error:") || strDataLineLCase.Contains("unhandled exception"))
                            {
                                if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                                {
                                    mConsoleOutputErrorMsg = "Error running MzRefinery: " + strDataLine;
                                }
                                else
                                {
                                    mConsoleOutputErrorMsg += "; " + strDataLine;
                                }
                            }
                            else if (strDataLine.StartsWith("Chose "))
                            {
                                mMzRefineryCorrectionMode = string.Copy(strDataLine);
                            }
                            else if (strDataLine.StartsWith("Low number of good identifications found"))
                            {
                                m_EvalMessage = strDataLine;
                                LogMessage("MzRefinery warning: " + strDataLine);
                            }
                            else if (strDataLine.StartsWith("Excluding file") && strDataLine.EndsWith("from data set"))
                            {
                                m_message = "Fewer than 100 matches after filtering; cannot use MzRefinery on this dataset";
                                LogError(m_message);
                            }
                            else
                            {
                                var reMatch = reResultsAfterFiltering.Match(strDataLine);

                                if (reMatch.Success)
                                {
                                    m_EvalMessage = clsGlobal.AppendToComment(m_EvalMessage, strDataLine.Trim());
                                    if (strDataLine.Trim().StartsWith("Less than 100 "))
                                    {
                                        m_UnableToUseMzRefinery = true;
                                    }
                                }

                                reMatch = reGoodDataPoints.Match(strDataLine);
                                if (reMatch.Success)
                                {
                                    if (int.TryParse(reMatch.Groups[1].Value, out var dataPoints))
                                    {
                                        mMzRefinerGoodDataPoints = dataPoints;
                                    }
                                }

                                reMatch = reSpecEValueThreshold.Match(strDataLine);
                                if (reMatch.Success)
                                {
                                    if (double.TryParse(reMatch.Groups[1].Value, out var specEValueThreshold))
                                    {
                                        mMzRefinerSpecEValueThreshold = specEValueThreshold;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing MzRefinery console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        private bool PostProcessMzRefineryResults(FileInfo fiMSGFPlusResults, FileInfo fiFixedMSXmlFile)
        {
            var strOriginalMSXmlFilePath = Path.Combine(m_WorkDir, m_Dataset + fiFixedMSXmlFile.Extension);

            try
            {
                // Create the plots
                var success = StartPpmErrorCharter(fiMSGFPlusResults);

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
                m_message = "Error creating PPM Error charters";
                LogError(m_message, ex);
                return false;
            }

            try
            {
                if (File.Exists(strOriginalMSXmlFilePath))
                {
                    // Delete the original .mzML file
                    DeleteFileWithRetries(strOriginalMSXmlFilePath, m_DebugLevel, 2);
                }
            }
            catch (Exception ex)
            {
                m_message = "Error replacing the original .mzML file with the updated version; cannot delete original";
                LogError(m_message, ex);
                return false;
            }

            try
            {
                // Rename the fixed mzML file
                fiFixedMSXmlFile.MoveTo(strOriginalMSXmlFilePath);
            }
            catch (Exception ex)
            {
                m_message = "Error replacing the original .mzML file with the updated version; cannot rename the fixed file";
                LogError(m_message, ex);
                return false;
            }

            try
            {
                // Compress the .mzXML or .mzML file
                var success = m_IonicZipTools.GZipFile(fiFixedMSXmlFile.FullName, true);

                if (!success)
                {
                    m_message = m_IonicZipTools.Message;
                    return false;
                }
            }
            catch (Exception ex)
            {
                m_message = "Error compressing the fixed .mzXML/.mzML file";
                LogError(m_message, ex);
                return false;
            }

            try
            {
                var fiMzRefFileGzipped = new FileInfo(fiFixedMSXmlFile.FullName + clsAnalysisResources.DOT_GZ_EXTENSION);

                // Copy the .mzXML.gz or .mzML.gz file to the cache
                var remoteCachefilePath = CopyFileToServerCache(mMSXmlCacheFolder.FullName, fiMzRefFileGzipped.FullName, purgeOldFilesIfNeeded: true);

                if (string.IsNullOrEmpty(remoteCachefilePath))
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("CopyFileToServerCache returned false for " + fiMzRefFileGzipped.Name);
                    }
                    return false;
                }

                // Create the _CacheInfo.txt file
                var cacheInfoFilePath = fiMzRefFileGzipped.FullName + "_CacheInfo.txt";
                using (var swOutFile = new StreamWriter(new FileStream(cacheInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swOutFile.WriteLine(remoteCachefilePath);
                }

                m_jobParams.AddResultFileToSkip(fiMzRefFileGzipped.Name);
            }
            catch (Exception ex)
            {
                m_message = "Error copying the .mzML.gz file to the remote cache folder";
                LogError(m_message, ex);
                return false;
            }

            // Compress the MSGF+ .mzID file
            var gzipSuccess = CompressMSGFPlusResults(fiMSGFPlusResults);

            return gzipSuccess;
        }

        private bool StartMzRefinery(FileInfo fiOriginalMzMLFile, FileInfo fiMSGFPlusResults)
        {
            mConsoleOutputErrorMsg = string.Empty;

            LogMessage("Running MzRefinery using MSConvert");

            // Set up and execute a program runner to run MSConvert
            // Provide the path to the .mzML file plus the --filter switch with the information required to run MzRefiner

            var cmdStr = " ";
            cmdStr += fiOriginalMzMLFile.FullName;
            cmdStr += " --outfile " + Path.GetFileNameWithoutExtension(fiOriginalMzMLFile.Name) + "_FIXED.mzML";
            cmdStr += " --filter \"mzRefiner " + fiMSGFPlusResults.FullName;

            // MzRefiner will perform a segmented correction if there are at least 500 matches; it will perform a global shift if between 100 and 500 matches
            // The data is initially filtered by MSGF SpecProb <= 1e-10
            // The reason that we prepend "1e-10" with a dash is to indicate a range of "-infinity to 1e-10"
            cmdStr += " thresholdValue=-1e-10";

            // If there are not 500 matches with 1e-10, the threshold value is multiplied by the thresholdStep value
            // This process is continued at most maxSteps times
            // Thus, using 10 and 2 means the thresholds that will be considered are 1e-10, 1e-9, and 1e-8
            cmdStr += " thresholdStep=10";
            cmdStr += " maxSteps=2\"";

            // These switches assure that the output file is a 32-bit mzML file
            cmdStr += " --32 --mzML";

            if (m_DebugLevel >= 1)
            {
                LogDebug(mMSConvertProgLoc + cmdStr);
            }

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = true;

            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, MZ_REFINERY_CONSOLE_OUTPUT);

            m_progress = MSGFPlusUtils.PROGRESS_PCT_MSGFPLUS_COMPLETE;

            mProgRunnerMode = eMzRefinerProgRunnerMode.MzRefiner;

            // Start MSConvert and wait for it to exit
            var success = mCmdRunner.RunProgram(mMSConvertProgLoc, cmdStr, "MSConvert_MzRefinery", true);

            mProgRunnerMode = eMzRefinerProgRunnerMode.Unknown;

            if (!mCmdRunner.WriteConsoleOutputToFile)
            {
                // Write the console output to a text file
                System.Threading.Thread.Sleep(250);

                using (var swConsoleOutputfile = new StreamWriter(new FileStream(mCmdRunner.ConsoleOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swConsoleOutputfile.WriteLine(mCmdRunner.CachedConsoleOutput);
                }
            }

            // Parse the console output file one more time to check for errors and to make sure mMzRefineryCorrectionMode is up-to-date
            // We will also extract out the final MS-GF:SpecEValue used for filtering the data
            System.Threading.Thread.Sleep(250);
            ParseMSConvertConsoleOutputfile(mCmdRunner.ConsoleOutputFilePath);

            if (!string.IsNullOrEmpty(mMzRefineryCorrectionMode))
            {
                var logMessage = "MzRefinery " + mMzRefineryCorrectionMode.Replace("...", "").TrimEnd('.');
                logMessage += ", " + mMzRefinerGoodDataPoints + " points had SpecEValue <= " + mMzRefinerSpecEValueThreshold.ToString("0.###E+00");

                LogMessage(logMessage);
            }

            if (!string.IsNullOrWhiteSpace(mCmdRunner.CachedConsoleErrors))
            {
                // Append the error messages to the log
                // Note that clsProgRunner will have already included them in the ConsoleOutput.txt file

                var consoleError = "Console error: " + mCmdRunner.CachedConsoleErrors.Replace(Environment.NewLine, "; ");
                if (string.IsNullOrWhiteSpace(mConsoleOutputErrorMsg))
                {
                    mConsoleOutputErrorMsg = consoleError;
                }
                else
                {
                    LogError(consoleError);
                }
                success = false;
            }

            if (!string.IsNullOrEmpty(mConsoleOutputErrorMsg))
            {
                LogError(mConsoleOutputErrorMsg);
                if (mConsoleOutputErrorMsg.Contains("No high-resolution data in input file"))
                {
                    m_message = "No high-resolution data in input file; cannot use MzRefinery on this dataset";
                    LogError(m_message);
                    m_UnableToUseMzRefinery = true;
                    m_ForceGeneratePPMErrorPlots = false;
                }
                else if (mConsoleOutputErrorMsg.Contains("No significant peak (ppm error histogram) found"))
                {
                    m_message = "Signficant peak not found in the ppm error histogram; cannot use MzRefinery on this dataset";
                    LogError(m_message);
                    m_UnableToUseMzRefinery = true;
                    m_ForceGeneratePPMErrorPlots = true;
                }
            }

            if (m_UnableToUseMzRefinery)
                success = false;

            if (!success)
            {
                if (m_UnableToUseMzRefinery)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("MSGF+ identified too few peptides; unable to use MzRefinery with this dataset");
                        m_ForceGeneratePPMErrorPlots = true;
                    }
                    else
                    {
                        LogErrorNoMessageUpdate("Unable to use MzRefinery with this dataset");
                    }
                }

                LogErrorNoMessageUpdate("Error running MSConvert/MzRefinery");

                if (!m_UnableToUseMzRefinery)
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

            m_progress = PROGRESS_PCT_MZREFINERY_COMPLETE;
            m_StatusTools.UpdateAndWrite(m_progress);
            if (m_DebugLevel >= 3)
            {
                LogDebug("MzRefinery Complete");
            }

            return true;
        }

        private bool StartPpmErrorCharter(FileInfo fiMSGFPlusResults)
        {
            LogMessage("Running PPMErrorCharter");

            // Set up and execute a program runner to run the PPMErrorCharter
            var cmdStr = " " + fiMSGFPlusResults.FullName + " " + mMzRefinerSpecEValueThreshold.ToString("0.###E+00");

            if (m_DebugLevel >= 1)
            {
                LogDebug(mPpmErrorCharterProgLoc + cmdStr);
            }

            mCmdRunner = new clsRunDosProgram(m_WorkDir);
            RegisterEvents(mCmdRunner);
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = true;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, ERROR_CHARTER_CONSOLE_OUTPUT_FILE);

            mProgRunnerMode = eMzRefinerProgRunnerMode.PPMErrorCharter;

            // Start the PPM Error Charter and wait for it to exit
            var success = mCmdRunner.RunProgram(mPpmErrorCharterProgLoc, cmdStr, "PPMErrorCharter", true);

            mProgRunnerMode = eMzRefinerProgRunnerMode.Unknown;

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
            var lstCharts = new List<FileInfo>
            {
                new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "_MZRefinery_MassErrors.png")),
                new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "_MZRefinery_Histograms.png"))
            };

            foreach (var fiChart in lstCharts)
            {
                if (!fiChart.Exists)
                {
                    m_message = "PPMError chart not found: " + fiChart.Name;
                    LogError(m_message);
                    return false;
                }
            }

            m_progress = PROGRESS_PCT_PLOTS_GENERATED;
            m_StatusTools.UpdateAndWrite(m_progress);
            if (m_DebugLevel >= 3)
            {
                LogDebug("PPMErrorCharter Complete");
            }

            return true;
        }

        private bool StorePPMErrorStatsInDB()
        {
            var oMassErrorExtractor = new clsMzRefineryMassErrorStatsExtractor(m_mgrParams, m_DebugLevel, blnPostResultsToDB: true);

            var intDatasetID = m_jobParams.GetJobParameter("DatasetID", 0);

            var consoleOutputFilePath = Path.Combine(m_WorkDir, ERROR_CHARTER_CONSOLE_OUTPUT_FILE);
            var success = oMassErrorExtractor.ParsePPMErrorCharterOutput(m_Dataset, intDatasetID, m_JobNum, consoleOutputFilePath);

            if (!success)
            {
                string msg;
                if (string.IsNullOrEmpty(oMassErrorExtractor.ErrorMessage))
                {
                    msg = "Error parsing PMM Error Charter output to extract mass error stats";
                }
                else
                {
                    msg = oMassErrorExtractor.ErrorMessage;
                }

                LogErrorToDatabase(msg + ", job " + m_JobNum);
                m_message = msg;
            }

            m_jobParams.AddResultFileToSkip(ERROR_CHARTER_CONSOLE_OUTPUT_FILE);

            return success;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            var strToolVersionInfo = string.Copy(mMSGFPlusUtils.MSGFPlusVersion);

            // Store paths to key files in ioToolFiles
            var ioToolFiles = new List<FileInfo>
            {
                new FileInfo(mMSGFPlusProgLoc),
                new FileInfo(mMSConvertProgLoc),
                new FileInfo(mPpmErrorCharterProgLoc)
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        private void UpdateProgress(float currentTaskProgressAtStart, float currentTaskProgressAtEnd, float subTaskProgress)
        {
            var progressCompleteOverall = ComputeIncrementalProgress(currentTaskProgressAtStart, currentTaskProgressAtEnd, subTaskProgress);

            UpdateProgress(progressCompleteOverall);
        }

        private void UpdateProgress(float progressCompleteOverall)
        {
            if (m_progress < progressCompleteOverall)
            {
                m_progress = progressCompleteOverall;
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

        private void MSGFPlusUtils_IgnorePreviousErrorEvent()
        {
            m_message = string.Empty;
        }

        #endregion
    }
}
