// This class was converted to be loaded as a pluggable DLL into the New DMS
// Analysis Tool Manager program.  The new ATM supports the mini-pipeline. It
// uses class MsMsSpectrumFilter to filter the _DTA.txt file present in a given folder
//
// Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
// Copyright 2005, Battelle Memorial Institute
// Started October 13, 2005
//
// Converted January 23, 2009 by JDS
// Updated July 2009 by MEM to process a _Dta.txt file instead of a folder of .Dta files
// Updated August 2009 by MEM to generate _ScanStats.txt files, if required

using AnalysisManagerBase;
using MsMsDataFileReader;
using MSMSSpectrumFilter;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;

namespace MSMSSpectrumFilterAM
{
    /// <summary>
    /// Class for running the MSMS Spectrum Filter
    /// </summary>
    public class AnalysisToolRunnerMsMsSpectrumFilter : AnalysisToolRunnerBase
    {
        // Ignore Spelling: dta, msg, pre

        private const int MAX_RUNTIME_HOURS = 5;

        private readonly clsMsMsSpectrumFilter mMsMsSpectrumFilter;

        private string mErrMsg = string.Empty;

        private string mSettingsFileName = string.Empty;

        private ProcessResults mResults;

        private string mDtaTextFileName = string.Empty;

        private ProcessStatus mFilterStatus;

        /// <summary>
        /// Constructor
        /// </summary>
        public AnalysisToolRunnerMsMsSpectrumFilter()
        {
            // Initialize MsMsSpectrumFilterDLL.dll
            mMsMsSpectrumFilter = new clsMsMsSpectrumFilter();
            mMsMsSpectrumFilter.ProgressChanged += MsMsSpectrumFilter_ProgressChanged;
            mMsMsSpectrumFilter.ProgressComplete += MsMsSpectrumFilter_ProgressComplete;
        }

        /// <summary>
        /// Primary entry point for running this tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
        public override CloseOutType RunTool()
        {
            CloseOutType result;

            // Do the base class stuff
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the MSMSSpectrumFilter version info in the database
            if (!StoreToolVersionInfo())
            {
                LogError("Aborting since StoreToolVersionInfo returned false");
                mMessage = "Error determining MSMSSpectrumFilter version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            mFilterStatus = ProcessStatus.SF_STARTING;

            LogMessage("AnalysisToolRunnerMsMsSpectrumFilter.RunTool(), Filtering _Dta.txt file");

            // Verify necessary files are in specified locations
            if (!InitSetup())
            {
                mResults = ProcessResults.SF_FAILURE;
                mFilterStatus = ProcessStatus.SF_ERROR;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Filter the spectra (the process runs in a separate thread)
            mFilterStatus = FilterDTATextFile();

            if (mFilterStatus == ProcessStatus.SF_ERROR)
            {
                mResults = ProcessResults.SF_FAILURE;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (mDebugLevel >= 2)
            {
                LogDebug("AnalysisToolRunnerMsMsSpectrumFilter.RunTool(), Filtering complete");
            }

            // Zip the filtered _Dta.txt file
            if (mResults == ProcessResults.SF_NO_SPECTRA_ALTERED)
            {
                result = CloseOutType.CLOSEOUT_SUCCESS;
                LogWarning("Filtered CDTA file is identical to the original file and was thus not copied to the job results folder", true);
            }
            else
            {
                result = ZipConcatenatedDtaFile();
            }

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                mResults = ProcessResults.SF_FAILURE;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Stop the job timer
            mStopTime = DateTime.UtcNow;

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Make the results folder
            if (mDebugLevel > 3)
            {
                LogDebug("AnalysisToolRunnerMsMsSpectrumFilter.RunTool(), Making results folder");
            }

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        private bool CDTAFilesMatch(string originalCDTAPath, string filteredCDTAPath)
        {
            clsMsMsDataFileReaderBaseClass cdtaReaderOriginal = null;
            clsMsMsDataFileReaderBaseClass cdtaReaderFiltered = null;

            try
            {
                var filteredCDTA = new FileInfo(filteredCDTAPath);
                var originalCDTA = new FileInfo(originalCDTAPath);

                // If the file sizes do not agree within 10 bytes, the files likely do not match
                // (unless we have a Unicode vs. non-Unicode issue, which shouldn't be the case)
                if (Math.Abs(filteredCDTA.Length - originalCDTA.Length) > 10)
                {
                    if (mDebugLevel >= 2)
                    {
                        LogMessage("Filtered CDTA file's size differs by more than 10 bytes vs. the original CDTA file " +
                            "(" + filteredCDTA.Length + " vs. " + originalCDTA.Length + "); assuming the files do not match");
                    }
                    return false;
                }
                else
                {
                    if (mDebugLevel >= 2)
                    {
                        LogMessage("Comparing original CDTA file to filtered CDTA file to see if they contain the same spectral data");
                    }
                }

                // Files are very similar in size; read the spectra data

                cdtaReaderFiltered = new clsDtaTextFileReader(false);
                cdtaReaderOriginal = new clsDtaTextFileReader(false);

                if (!cdtaReaderFiltered.OpenFile(filteredCDTAPath))
                {
                    LogError("Error in CDTAFilesMatch opening " + filteredCDTAPath);
                    return false;
                }

                if (!cdtaReaderOriginal.OpenFile(originalCDTAPath))
                {
                    LogError("Error in CDTAFilesMatch opening " + originalCDTAPath);
                    return false;
                }

                var originalCDTASpectra = 0;
                var filteredCDTASpectra = 0;

                while (cdtaReaderOriginal.ReadNextSpectrum(out var msmsDataListFilter, out var spectrumHeaderInfoOrig))
                {
                    originalCDTASpectra++;

                    if (cdtaReaderFiltered.ReadNextSpectrum(out var msmsDataListOrig, out var spectrumHeaderInfoFilter))
                    {
                        filteredCDTASpectra++;

                        // If the parent ions differ or the MS/MS spectral data differs, the files do not match

                        if (spectrumHeaderInfoOrig.SpectrumTitle != spectrumHeaderInfoFilter.SpectrumTitle)
                        {
                            if (mDebugLevel >= 2)
                            {
                                LogMessage("Spectrum " + originalCDTASpectra + " in the original CDTA file has a different spectrum header " +
                                    "vs. spectrum " + filteredCDTASpectra + " in the filtered CDTA file; files do not match");
                            }
                            return false;
                        }

                        if (msmsDataListOrig.Count != msmsDataListFilter.Count)
                        {
                            if (mDebugLevel >= 2)
                            {
                                LogMessage("Spectrum " + originalCDTASpectra + " in the original CDTA file has a different number of ions " +
                                    "(" + msmsDataListOrig.Count + " vs. " + msmsDataListFilter.Count + "); files do not match");
                            }
                            return false;
                        }

                        for (var index = 0; index <= msmsDataListOrig.Count - 1; index++)
                        {
                            if (msmsDataListOrig[index].Trim() != msmsDataListFilter[index].Trim())
                            {
                                if (mDebugLevel >= 2)
                                {
                                    LogMessage("Spectrum " + originalCDTASpectra + " in the original CDTA file has different ion mass or abundance values; files do not match");
                                }
                                return false;
                            }
                        }
                    }
                    else
                    {
                        // Original CDTA file has more spectra than the filtered one
                        if (mDebugLevel >= 2)
                        {
                            LogMessage("Original CDTA file has more spectra than the filtered one (" + originalCDTASpectra + " vs. " + filteredCDTASpectra + "); files do not match");
                        }
                        return false;
                    }
                }

                if (cdtaReaderFiltered.ReadNextSpectrum(out _, out _))
                {
                    // Filtered CDTA file has more spectra than the original one
                    return false;
                }

                // If we get here, the files match
                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in CDTAFilesMatch", ex);
                return false;
            }
            finally
            {
                try
                {
                    cdtaReaderOriginal?.CloseFile();
                    cdtaReaderFiltered?.CloseFile();
                }
                catch (Exception)
                {
                    // Ignore errors here
                }
            }
        }

        protected virtual int CountDtaFiles(string dtaTextFilePath)
        {
            // Returns the number of DTA files in the _dta.txt file

            // This RegEx matches text of the form:
            // =================================== "File.ScanStart.ScanEnd.Charge.dta" ==================================

            // For example:
            // =================================== "QC_Shew_07_02-pt5-a_27Sep07_EARTH_07-08-15.351.351.1.dta" ==================================

            // It also can match lines where there is extra information associated with the charge state, for example:
            // =================================== "QC_Shew_07_02-pt5-a_27Sep07_EARTH_07-08-15.351.351.1_1_2.dta" ==================================
            // =================================== "vxl_VP2P74_B_4_F12_rn1_14May08_Falcon_080403-F4.1001.1001.2_1_2.dta" ==================================

            const string DTA_FILENAME_REGEX = @"^\s*[=]{5,}\s+\""([^.]+)\.\d+\.\d+\..+dta";

            var dtaCount = 0;

            try
            {
                var reFind = new Regex(DTA_FILENAME_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);

                using var reader = new StreamReader(new FileStream(dtaTextFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                dtaCount = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (!string.IsNullOrEmpty(dataLine))
                    {
                        if (reFind.Match(dataLine).Success)
                        {
                            dtaCount++;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogErrors("CountDtaFiles", "Error counting .Dta files in dtaTextFilePath", ex);
                mFilterStatus = ProcessStatus.SF_ERROR;
            }

            return dtaCount;
        }

        protected virtual ProcessStatus FilterDTATextFile()
        {
            // Initializes mMsMsSpectrumFilter, then starts a separate thread to filter the _Dta.txt file in the working folder
            // If ScanStats files are required, will first call GenerateFinniganScanStatsFiles() to generate those files using the .Raw file

            try
            {
                // Pre-read the parameter file now, so that we can override some of the settings
                var parameterFilePath = Path.Combine(mWorkDir, mSettingsFileName);

                if (mDebugLevel >= 3)
                {
                    LogDebug("Loading parameter file: " + parameterFilePath);
                }

                if (!mMsMsSpectrumFilter.LoadParameterFileSettings(parameterFilePath))
                {
                    mErrMsg = mMsMsSpectrumFilter.GetErrorMessage();

                    if (string.IsNullOrEmpty(mErrMsg))
                    {
                        mErrMsg = "Parameter file load error: " + parameterFilePath;
                    }
                    LogErrors("FilterDTATextFile", mErrMsg);
                    return ProcessStatus.SF_ERROR;
                }

                // Set a few additional settings
                mMsMsSpectrumFilter.OverwriteExistingFiles = true;
                mMsMsSpectrumFilter.OverwriteReportFile = true;
                mMsMsSpectrumFilter.AutoCloseReportFile = false;
                mMsMsSpectrumFilter.LogMessagesToFile = true;
                mMsMsSpectrumFilter.LogFolderPath = mWorkDir;
                mMsMsSpectrumFilter.MaximumProgressUpdateIntervalSeconds = 10;

                // Determine if we need to generate a _ScanStats.txt file
                if (mMsMsSpectrumFilter.ScanStatsFileIsRequired())
                {
                    if (mDebugLevel >= 4)
                    {
                        LogDebug("Calling GenerateFinniganScanStatsFiles");
                    }

                    if (!GenerateFinniganScanStatsFiles())
                    {
                        if (mDebugLevel >= 4)
                        {
                            LogDebug("GenerateFinniganScanStatsFiles returned false");
                        }

                        mFilterStatus = ProcessStatus.SF_ERROR;
                        return mFilterStatus;
                    }

                    if (mDebugLevel >= 4)
                    {
                        LogDebug("GenerateFinniganScanStatsFiles returned true");
                    }
                }

                if (mDebugLevel >= 3)
                {
                    LogDebug("Instantiate the thread to run MsMsSpectraFilter");
                }

                // Instantiate the thread to run MsMsSpectraFilter
                var thread = new System.Threading.Thread(FilterDTATextFileWork);
                thread.Start();

                if (mDebugLevel >= 4)
                {
                    LogDebug("Thread started");
                }

                var startTime = DateTime.UtcNow;

                while (true)
                {
                    // Wait for 2 seconds
                    thread.Join(2000);

                    // Check whether the thread is still running
                    if (!thread.IsAlive)
                        break;

                    // Check whether the thread has been running too long
                    if (!(DateTime.UtcNow.Subtract(startTime).TotalHours > MAX_RUNTIME_HOURS))
                        continue;

                    LogError("MSFileInfoScanner has run for over " + MAX_RUNTIME_HOURS + " hours; aborting");

                    try
                    {
                        thread.Abort();
                    }
                    catch
                    {
                        // Ignore errors here;
                    }

                    break;
                }

                // If we reach here, everything must be good
                mFilterStatus = ProcessStatus.SF_COMPLETE;
            }
            catch (Exception ex)
            {
                LogErrors("FilterDTAFilesInFolder", "Error initializing and running MsMsSpectrumFilter", ex);
                mFilterStatus = ProcessStatus.SF_ERROR;
            }

            return mFilterStatus;
        }

        protected virtual void FilterDTATextFileWork()
        {
            try
            {
                if (mDebugLevel >= 2)
                {
                    LogDebug("  Spectrum Filter Mode: " + mMsMsSpectrumFilter.SpectrumFilterMode);
                    LogDebug("  MS Level Filter: " + mMsMsSpectrumFilter.MSLevelFilter);
                    LogDebug("  ScanTypeFilter: " + mMsMsSpectrumFilter.ScanTypeFilter + " (match type " + mMsMsSpectrumFilter.ScanTypeMatchType + ")");
                    LogDebug("  MSCollisionModeFilter: " + mMsMsSpectrumFilter.MSCollisionModeFilter + " (match type " + mMsMsSpectrumFilter.MSCollisionModeMatchType + ")");
                    LogDebug("  MinimumIonCount: " + mMsMsSpectrumFilter.MinimumIonCount);
                    LogDebug("  IonFilter_RemovePrecursor: " + mMsMsSpectrumFilter.IonFilter_RemovePrecursor);
                    LogDebug("  IonFilter_RemoveChargeReducedPrecursors: " + mMsMsSpectrumFilter.IonFilter_RemoveChargeReducedPrecursors);
                    LogDebug("  IonFilter_RemoveNeutralLossesFromChargeReducedPrecursors: " + mMsMsSpectrumFilter.IonFilter_RemoveNeutralLossesFromChargeReducedPrecursors);
                }

                // Define the input file name
                var inputFilePath = Path.Combine(mWorkDir, mDtaTextFileName);

                if (mDebugLevel >= 3)
                {
                    LogDebug("Call ProcessFilesWildcard for: " + inputFilePath);
                }

                var success = mMsMsSpectrumFilter.ProcessFilesWildcard(inputFilePath, mWorkDir, "");

                try
                {
                    if (success)
                    {
                        if (mDebugLevel >= 3)
                        {
                            LogDebug("ProcessFilesWildcard returned true; now calling .SortSpectrumQualityTextFile");
                        }

                        // Sort the report file (this also closes the file)
                        mMsMsSpectrumFilter.SortSpectrumQualityTextFile();

                        var bakFilePath = inputFilePath + ".bak";

                        if (!File.Exists(bakFilePath))
                        {
                            LogErrors("FilterDTATextFileWork", "CDTA .Bak file not found");
                            mResults = ProcessResults.SF_NO_FILES_CREATED;
                        }

                        // Compare the new _dta.txt file to the _dta.txt.bak file
                        // If they have the same data, do not keep the new _dta.txt file

                        var filesMatch = CDTAFilesMatch(bakFilePath, inputFilePath);

                        PRISM.AppUtils.GarbageCollectNow();

                        // Delete the _dta.txt.bak file
                        File.Delete(bakFilePath);

                        if (filesMatch)
                        {
                            LogMessage("The filtered CDTA file matches the original CDTA file (same number of spectra and same spectral data); the filtered CDTA file will not be retained");

                            File.Delete(inputFilePath);

                            mResults = ProcessResults.SF_NO_SPECTRA_ALTERED;
                        }
                        else
                        {
                            // Count the number of .Dta files remaining in the _dta.txt file
                            if (!VerifyDtaCreation(inputFilePath))
                            {
                                mResults = ProcessResults.SF_NO_FILES_CREATED;
                            }
                            else
                            {
                                mResults = ProcessResults.SF_SUCCESS;
                            }
                        }

                        mFilterStatus = ProcessStatus.SF_COMPLETE;
                    }
                    else
                    {
                        if (mMsMsSpectrumFilter.AbortProcessing)
                        {
                            LogErrors("FilterDTATextFileWork", "Processing aborted");
                            mResults = ProcessResults.SF_ABORTED;
                            mFilterStatus = ProcessStatus.SF_ABORTING;
                        }
                        else
                        {
                            LogErrors("FilterDTATextFileWork", mMsMsSpectrumFilter.GetErrorMessage());
                            mResults = ProcessResults.SF_FAILURE;
                            mFilterStatus = ProcessStatus.SF_ERROR;
                        }
                    }

                    mMsMsSpectrumFilter.CloseLogFileNow();
                }
                catch (Exception ex)
                {
                    LogErrors("FilterDTATextFileWork", "Error performing tasks after mMsMsSpectrumFilter.ProcessFilesWildcard completes", ex);
                    mFilterStatus = ProcessStatus.SF_ERROR;
                }
            }
            catch (Exception ex)
            {
                LogErrors("FilterDTATextFileWork", "Error calling mMsMsSpectrumFilter.ProcessFilesWildcard", ex);
                mFilterStatus = ProcessStatus.SF_ERROR;
            }
        }

        private bool GenerateFinniganScanStatsFiles()
        {
            try
            {
                if (mDebugLevel >= 1)
                {
                    LogDebug("Looking for the _ScanStats.txt files for dataset " + mDatasetName);
                }

                var scanStatsFilesExist = clsMsMsSpectrumFilter.CheckForExistingScanStatsFiles(mWorkDir, mDatasetName);

                if (scanStatsFilesExist)
                {
                    if (mDebugLevel >= 1)
                    {
                        LogMessage("_ScanStats.txt files found for dataset " + mDatasetName);
                    }
                    return true;
                }

                LogMessage("Creating the _ScanStats.txt files for dataset " + mDatasetName);

                // Determine the path to the .Raw file
                var rawFileName = mDatasetName + ".raw";
                var finniganRawFilePath = AnalysisResources.ResolveStoragePath(mWorkDir, rawFileName);

                if (string.IsNullOrEmpty(finniganRawFilePath))
                {
                    // Unable to resolve the file path
                    mErrMsg = "Could not find " + rawFileName + " or " +
                        rawFileName + AnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX +
                        " in the dataset directory; unable to generate the ScanStats files";
                    LogErrors("GenerateFinniganScanStatsFiles", mErrMsg);
                    return false;
                }

                // Look for an existing _ScanStats.txt file in a SIC folder below folder with the .Raw file
                var rawFile = new FileInfo(finniganRawFilePath);

                if (!rawFile.Exists)
                {
                    // File not found at the specified path
                    mErrMsg = "File not found: " + finniganRawFilePath + " -- unable to generate the ScanStats files";
                    LogErrors("GenerateFinniganScanStatsFiles", mErrMsg);
                    return false;
                }

                if (mDebugLevel >= 1)
                {
                    LogMessage("Generating _ScanStats.txt file using " + finniganRawFilePath);
                }

                if (!mMsMsSpectrumFilter.GenerateFinniganScanStatsFiles(finniganRawFilePath, mWorkDir))
                {
                    if (mDebugLevel >= 3)
                    {
                        LogDebug("GenerateFinniganScanStatsFiles returned false");
                    }

                    mErrMsg = mMsMsSpectrumFilter.GetErrorMessage();

                    if (string.IsNullOrEmpty(mErrMsg))
                    {
                        mErrMsg = "GenerateFinniganScanStatsFiles returned false; _ScanStats.txt files not generated";
                    }

                    LogErrors("GenerateFinniganScanStatsFiles", mErrMsg);
                    return false;
                }

                if (mDebugLevel >= 4)
                {
                    LogDebug("GenerateFinniganScanStatsFiles returned true");
                }
                return true;
            }
            catch (Exception ex)
            {
                LogErrors("GenerateFinniganScanStatsFiles", "Error generating _ScanStats.txt files", ex);
                return false;
            }
        }

        private DateTime mLastStatusUpdate = DateTime.MinValue;

        private void HandleProgressUpdate(float percentComplete)
        {
            mProgress = percentComplete;

            // Update the status file (limit the updates to every 5 seconds)
            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds >= 5)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                UpdateStatusRunning(mProgress, mDtaCount);
            }

            LogProgress("MsMsSpectrumFilter");
        }

        protected virtual bool InitSetup()
        {
            // Initializes module variables and verifies that mandatory parameters have been properly specified

            // Manager parameters
            if (mMgrParams == null)
            {
                mErrMsg = "Manager parameters not specified";
                return false;
            }

            // Job parameters
            if (mJobParams == null)
            {
                mErrMsg = "Job parameters not specified";
                return false;
            }

            // Status tools
            if (mStatusTools == null)
            {
                mErrMsg = "Status tools object not set";
                return false;
            }

            // Set the _DTA.Txt file name
            mDtaTextFileName = mDatasetName + "_dta.txt";

            // Set settings file name
            // This is the job parameters file that contains the settings information
            mSettingsFileName = mJobParams.GetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_XML_PARAMS_FILE);

            // Source folder name
            if (string.IsNullOrEmpty(mWorkDir))
            {
                mErrMsg = "mWorkDir variable is empty";
                return false;
            }

            // Source directory exist?
            if (!VerifyDirExists(mWorkDir))
            {
                // Error msg handled by VerifyDirExists
                return false;
            }

            // Settings file exist?
            var settingsNamePath = Path.Combine(mWorkDir, mSettingsFileName);

            if (!VerifyFileExists(settingsNamePath))
            {
                // Error msg handled by VerifyFileExists
                return false;
            }

            // If we got here, everything's OK
            return true;
        }

        private void LogErrors(string source, string message, Exception ex = null)
        {
            mErrMsg = message.Replace("\n", "; ").Replace("\r", "");
            LogError(source + ": " + mErrMsg, ex);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo()
        {
            var toolVersionInfo = string.Empty;
            var appFolderPath = Global.GetAppDirectoryPath();

            if (mDebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of the MSMSSpectrumFilterAM
            if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "MSMSSpectrumFilterAM"))
            {
                return false;
            }

            // Lookup the version of the MsMsDataFileReader
            if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "MsMsDataFileReader"))
            {
                return false;
            }

            // Store the path to MsMsDataFileReader.dll in toolFiles
            var toolFiles = new List<FileInfo> {
                new(Path.Combine(appFolderPath, "MsMsDataFileReader.dll"))
            };

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        protected virtual bool VerifyDirExists(string testDirectory)
        {
            // Verifies that the specified directory exists
            if (Directory.Exists(testDirectory))
            {
                mErrMsg = "";
                return true;
            }

            mErrMsg = "Directory " + testDirectory + " not found";
            return false;
        }

        private bool VerifyDtaCreation(string dtaTextFilePath)
        {
            // Verify at least one .dta file has been created
            if (CountDtaFiles(dtaTextFilePath) < 1)
            {
                mErrMsg = "No DTA files remain after filtering";
                return false;
            }

            return true;
        }

        protected virtual bool VerifyFileExists(string testFile)
        {
            // Verifies specified file exists
            if (File.Exists(testFile))
            {
                mErrMsg = "";
                return true;
            }

            mErrMsg = "File " + testFile + " not found";
            return false;
        }

        /// <summary>
        /// Zips concatenated DTA file to reduce size
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        protected virtual CloseOutType ZipConcatenatedDtaFile()
        {
            // Zips the concatenated DTA file
            var dtaFileName = mDatasetName + "_dta.txt";
            var dtaFilePath = Path.Combine(mWorkDir, dtaFileName);

            // Verify file exists
            if (File.Exists(dtaFilePath))
            {
                LogMessage("Zipping concatenated spectra file, job " + mJob + ", step " + mJobParams.GetParam("Step"));
            }
            else
            {
                LogError("Unable to find concatenated DTA file");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Zip the file
            try
            {
                if (!ZipFile(dtaFilePath, false))
                {
                    LogError("Error zipping concatenated DTA file, job {0}, step {1}", mJob, mJobParams.GetParam("Step"));
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception zipping concatenated DTA file, job {0}, step {1}: {2}", mJob, mJobParams.GetParam("Step"), ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private void MsMsSpectrumFilter_ProgressChanged(string taskDescription, float percentComplete)
        {
            HandleProgressUpdate(percentComplete);
        }

        private void MsMsSpectrumFilter_ProgressComplete()
        {
            HandleProgressUpdate(100);
        }
    }
}
