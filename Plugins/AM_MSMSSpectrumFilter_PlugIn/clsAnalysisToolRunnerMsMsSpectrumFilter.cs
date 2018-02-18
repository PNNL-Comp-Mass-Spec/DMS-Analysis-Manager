// This class was converted to be loaded as a pluggable DLL into the New DMS
// Analysis Tool Manager program.  The new ATM supports the mini-pipeline. It
// uses class clsMsMsSpectrumFilter to filter the _DTA.txt file present in a given folder
//
// Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA)
// Copyright 2005, Battelle Memorial Institute
// Started October 13, 2005
//
// Converted January 23, 2009 by JDS
// Updated July 2009 by MEM to process a _Dta.txt file instead of a folder of .Dta files
// Updated August 2009 by MEM to generate _ScanStats.txt files, if required

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using MsMsDataFileReader;
using MSMSSpectrumFilter;

namespace MSMSSpectrumFilterAM
{
    /// <summary>
    /// Class for running the MSMS Spectrum Filter
    /// </summary>
    public class clsAnalysisToolRunnerMsMsSpectrumFilter : clsAnalysisToolRunnerBase
    {
        private const int MAX_RUNTIME_HOURS = 5;

        private readonly clsMsMsSpectrumFilter m_MsMsSpectrumFilter;

        private string m_ErrMsg = string.Empty;

        private string m_SettingsFileName = string.Empty;

        private ProcessResults m_Results;

        private string m_DTATextFileName = string.Empty;

        private ProcessStatus m_FilterStatus;

        #region "Methods"

        public clsAnalysisToolRunnerMsMsSpectrumFilter()
        {
            // Initialize MsMsSpectrumFilterDLL.dll
            m_MsMsSpectrumFilter = new clsMsMsSpectrumFilter();
            m_MsMsSpectrumFilter.ProgressChanged += m_MsMsSpectrumFilter_ProgressChanged;
            m_MsMsSpectrumFilter.ProgressComplete += m_MsMsSpectrumFilter_ProgressComplete;
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
                m_message = "Error determining MSMSSpectrumFilter version";
                return CloseOutType.CLOSEOUT_FAILED;
            }

            m_FilterStatus = ProcessStatus.SF_STARTING;

            LogMessage("clsAnalysisToolRunnerMsMsSpectrumFilter.RunTool(), Filtering _Dta.txt file");

            // Verify necessary files are in specified locations
            if (!InitSetup())
            {
                m_Results = ProcessResults.SF_FAILURE;
                m_FilterStatus = ProcessStatus.SF_ERROR;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Filter the spectra (the process runs in a separate thread)
            m_FilterStatus = FilterDTATextFile();

            if (m_FilterStatus == ProcessStatus.SF_ERROR)
            {
                m_Results = ProcessResults.SF_FAILURE;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (m_DebugLevel >= 2)
            {
                LogDebug("clsAnalysisToolRunnerMsMsSpectrumFilter.RunTool(), Filtering complete");
            }

            // Zip the filtered _Dta.txt file
            if (m_Results == ProcessResults.SF_NO_SPECTRA_ALTERED)
            {
                result = CloseOutType.CLOSEOUT_SUCCESS;
                LogWarning("Filtered CDTA file is identical to the original file and was thus not copied to the job results folder", true);
            }
            else
            {
                result = ZipConcDtaFile();
            }

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                m_Results = ProcessResults.SF_FAILURE;
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Stop the job timer
            m_StopTime = DateTime.UtcNow;

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Make the results folder
            if (m_DebugLevel > 3)
            {
                LogDebug("clsAnalysisToolRunnerMsMsSpectrumFilter.RunTool(), Making results folder");
            }

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        private bool CDTAFilesMatch(string originalCDTA, string filteredCDTA)
        {
            clsMsMsDataFileReaderBaseClass objOriginalCDTA = null;
            clsMsMsDataFileReaderBaseClass objFilteredCDTA = null;

            try
            {
                var fiFilteredCDTA = new FileInfo(filteredCDTA);
                var fiOriginalCDTA = new FileInfo(originalCDTA);

                // If the file sizes do not agree within 10 bytes, the files likely do not match (unless we have a unicode; non-unicode issue, which shouldn't be the case)
                if (Math.Abs(fiFilteredCDTA.Length - fiOriginalCDTA.Length) > 10)
                {
                    if (m_DebugLevel >= 2)
                    {
                        LogMessage("Filtered CDTA file's size differs by more than 10 bytes vs. the original CDTA file " +
                            "(" + fiFilteredCDTA.Length + " vs. " + fiOriginalCDTA.Length + "); assuming the files do not match");
                    }
                    return false;
                }
                else
                {
                    if (m_DebugLevel >= 2)
                    {
                        LogMessage("Comparing original CDTA file to filtered CDTA file to see if they contain the same spectral data");
                    }
                }

                // Files are very similar in size; read the spectra data

                objFilteredCDTA = new clsDtaTextFileReader(false);
                objOriginalCDTA = new clsDtaTextFileReader(false);

                if (!objFilteredCDTA.OpenFile(filteredCDTA))
                {
                    LogError("Error in CDTAFilesMatch opening " + filteredCDTA);
                    return false;
                }

                if (!objOriginalCDTA.OpenFile(originalCDTA))
                {
                    LogError("Error in CDTAFilesMatch opening " + originalCDTA);
                    return false;
                }

                List<string> msmsDataListFilt;
                var intOriginalCDTASpectra = 0;
                var intFilteredCDTASpectra = 0;

                while (objOriginalCDTA.ReadNextSpectrum(out msmsDataListFilt, out var udtSpectrumHeaderInfoOrig))
                {
                    intOriginalCDTASpectra += 1;

                    if (objFilteredCDTA.ReadNextSpectrum(out var msmsDataListOrig, out var udtSpectrumHeaderInfoFilt))
                    {
                        intFilteredCDTASpectra += 1;

                        // If the parent ions differ or the MS/MS spectral data differs, the files do not match

                        if (udtSpectrumHeaderInfoOrig.SpectrumTitle != udtSpectrumHeaderInfoFilt.SpectrumTitle)
                        {
                            if (m_DebugLevel >= 2)
                            {
                                LogMessage("Spectrum " + intOriginalCDTASpectra + " in the original CDTA file has a different spectrum header " +
                                    "vs. spectrum " + intFilteredCDTASpectra + " in the filtered CDTA file; files do not match");
                            }
                            return false;
                        }

                        if (msmsDataListOrig.Count != msmsDataListFilt.Count)
                        {
                            if (m_DebugLevel >= 2)
                            {
                                LogMessage("Spectrum " + intOriginalCDTASpectra + " in the original CDTA file has a different number of ions " +
                                    "(" + msmsDataListOrig.Count + " vs. " + msmsDataListFilt.Count + "); files do not match");
                            }
                            return false;
                        }

                        for (var intIndex = 0; intIndex <= msmsDataListOrig.Count - 1; intIndex++)
                        {
                            if (msmsDataListOrig[intIndex].Trim() != msmsDataListFilt[intIndex].Trim())
                            {
                                if (m_DebugLevel >= 2)
                                {
                                    LogMessage("Spectrum " + intOriginalCDTASpectra + " in the original CDTA file has different ion mass or abundance values; files do not match");
                                }
                                return false;
                            }
                        }
                    }
                    else
                    {
                        // Original CDTA file has more spectra than the filtered one
                        if (m_DebugLevel >= 2)
                        {
                            LogMessage("Original CDTA file has more spectra than the filtered one (" + intOriginalCDTASpectra + " vs. " + intFilteredCDTASpectra + "); files do not match");
                        }
                        return false;
                    }
                }

                if (objFilteredCDTA.ReadNextSpectrum(out msmsDataListFilt, out _))
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
                    System.Threading.Thread.Sleep(250);
                    objOriginalCDTA?.CloseFile();
                    objFilteredCDTA?.CloseFile();
                }
                catch (Exception)
                {
                    // Ignore errors here
                }
            }
        }

        protected virtual int CountDtaFiles(string strDTATextFilePath)
        {
            // Returns the number of dta files in the _dta.txt file

            // This RegEx matches text of the form:
            // =================================== "File.ScanStart.ScanEnd.Charge.dta" ==================================
            //
            // For example:
            // =================================== "QC_Shew_07_02-pt5-a_27Sep07_EARTH_07-08-15.351.351.1.dta" ==================================
            //
            // It also can match lines where there is extra information associated with the charge state, for example:
            // =================================== "QC_Shew_07_02-pt5-a_27Sep07_EARTH_07-08-15.351.351.1_1_2.dta" ==================================
            // =================================== "vxl_VP2P74_B_4_F12_rn1_14May08_Falcon_080403-F4.1001.1001.2_1_2.dta" ==================================
            const string DTA_FILENAME_REGEX = @"^\s*[=]{5,}\s+\""([^.]+)\.\d+\.\d+\..+dta";

            var intDTACount = 0;

            try
            {
                var reFind = new Regex(DTA_FILENAME_REGEX, RegexOptions.Compiled | RegexOptions.IgnoreCase);

                using (var srInFile = new StreamReader(new FileStream(strDTATextFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    intDTACount = 0;

                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (!string.IsNullOrEmpty(strLineIn))
                        {
                            if (reFind.Match(strLineIn).Success)
                            {
                                intDTACount += 1;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogErrors("CountDtaFiles", "Error counting .Dta files in strDTATextFilePath", ex);
                m_FilterStatus = ProcessStatus.SF_ERROR;
            }

            return intDTACount;
        }

        protected virtual ProcessStatus FilterDTATextFile()
        {
            // Initializes m_MsMsSpectrumFilter, then starts a separate thread to filter the _Dta.txt file in the working folder
            // If ScanStats files are required, will first call GenerateFinniganScanStatsFiles() to generate those files using the .Raw file

            try
            {
                // Pre-read the parameter file now, so that we can override some of the settings
                var strParameterFilePath = Path.Combine(m_WorkDir, m_SettingsFileName);

                if (m_DebugLevel >= 3)
                {
                    LogDebug("Loading parameter file: " + strParameterFilePath);
                }

                if (!m_MsMsSpectrumFilter.LoadParameterFileSettings(strParameterFilePath))
                {
                    m_ErrMsg = m_MsMsSpectrumFilter.GetErrorMessage();
                    if (string.IsNullOrEmpty(m_ErrMsg))
                    {
                        m_ErrMsg = "Parameter file load error: " + strParameterFilePath;
                    }
                    LogErrors("FilterDTATextFile", m_ErrMsg);
                    return ProcessStatus.SF_ERROR;
                }

                // Set a few additional settings
                m_MsMsSpectrumFilter.OverwriteExistingFiles = true;
                m_MsMsSpectrumFilter.OverwriteReportFile = true;
                m_MsMsSpectrumFilter.AutoCloseReportFile = false;
                m_MsMsSpectrumFilter.LogMessagesToFile = true;
                m_MsMsSpectrumFilter.LogFolderPath = m_WorkDir;
                m_MsMsSpectrumFilter.MaximumProgressUpdateIntervalSeconds = 10;

                // Determine if we need to generate a _ScanStats.txt file
                if (m_MsMsSpectrumFilter.ScanStatsFileIsRequired())
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("Calling GenerateFinniganScanStatsFiles");
                    }

                    if (!GenerateFinniganScanStatsFiles())
                    {
                        if (m_DebugLevel >= 4)
                        {
                            LogDebug("GenerateFinniganScanStatsFiles returned False");
                        }

                        m_FilterStatus = ProcessStatus.SF_ERROR;
                        return m_FilterStatus;
                    }

                    if (m_DebugLevel >= 4)
                    {
                        LogDebug("GenerateFinniganScanStatsFiles returned True");
                    }
                }

                if (m_DebugLevel >= 3)
                {
                    LogDebug("Instantiate the thread to run MsMsSpectraFilter");
                }

                // Instantiate the thread to run MsMsSpectraFilter
                var thread = new System.Threading.Thread(FilterDTATextFileWork);
                thread.Start();

                if (m_DebugLevel >= 4)
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
                m_FilterStatus = ProcessStatus.SF_COMPLETE;
            }
            catch (Exception ex)
            {
                LogErrors("FilterDTAFilesInFolder", "Error initializing and running clsMsMsSpectrumFilter", ex);
                m_FilterStatus = ProcessStatus.SF_ERROR;
            }

            return m_FilterStatus;
        }

        protected virtual void FilterDTATextFileWork()
        {
            try
            {
                if (m_DebugLevel >= 2)
                {
                    LogDebug("  Spectrum Filter Mode: " + m_MsMsSpectrumFilter.SpectrumFilterMode);
                    LogDebug("  MS Level Filter: " + m_MsMsSpectrumFilter.MSLevelFilter);
                    LogDebug("  ScanTypeFilter: " + m_MsMsSpectrumFilter.ScanTypeFilter + " (match type " + m_MsMsSpectrumFilter.ScanTypeMatchType + ")");
                    LogDebug("  MSCollisionModeFilter: " + m_MsMsSpectrumFilter.MSCollisionModeFilter + " (match type " + m_MsMsSpectrumFilter.MSCollisionModeMatchType + ")");
                    LogDebug("  MinimumIonCount: " + m_MsMsSpectrumFilter.MinimumIonCount);
                    LogDebug("  IonFilter_RemovePrecursor: " + m_MsMsSpectrumFilter.IonFilter_RemovePrecursor);
                    LogDebug("  IonFilter_RemoveChargeReducedPrecursors: " + m_MsMsSpectrumFilter.IonFilter_RemoveChargeReducedPrecursors);
                    LogDebug("  IonFilter_RemoveNeutralLossesFromChargeReducedPrecursors: " + m_MsMsSpectrumFilter.IonFilter_RemoveNeutralLossesFromChargeReducedPrecursors);
                }

                // Define the input file name
                var strInputFilePath = Path.Combine(m_WorkDir, m_DTATextFileName);

                if (m_DebugLevel >= 3)
                {
                    LogDebug("Call ProcessFilesWildcard for: " + strInputFilePath);
                }

                var blnSuccess = m_MsMsSpectrumFilter.ProcessFilesWildcard(strInputFilePath, m_WorkDir, "");

                try
                {
                    if (blnSuccess)
                    {
                        if (m_DebugLevel >= 3)
                        {
                            LogDebug("ProcessFilesWildcard returned True; now calling .SortSpectrumQualityTextFile");
                        }

                        // Sort the report file (this also closes the file)
                        m_MsMsSpectrumFilter.SortSpectrumQualityTextFile();

                        var strBakFilePath = strInputFilePath + ".bak";

                        if (!File.Exists(strBakFilePath))
                        {
                            LogErrors("FilterDTATextFileWork", "CDTA .Bak file not found");
                            m_Results = ProcessResults.SF_NO_FILES_CREATED;
                        }

                        // Compare the new _dta.txt file to the _dta.txt.bak file
                        // If they have the same data, do not keep the new _dta.txt file

                        var blnFilesMatch = CDTAFilesMatch(strBakFilePath, strInputFilePath);

                        System.Threading.Thread.Sleep(250);
                        PRISM.clsProgRunner.GarbageCollectNow();

                        // Delete the _dta.txt.bak file
                        File.Delete(strBakFilePath);

                        if (blnFilesMatch)
                        {
                            LogMessage("The filtered CDTA file matches the original CDTA file (same number of spectra and same spectral data); the filtered CDTA file will not be retained");

                            File.Delete(strInputFilePath);

                            m_Results = ProcessResults.SF_NO_SPECTRA_ALTERED;
                        }
                        else
                        {
                            // Count the number of .Dta files remaining in the _dta.txt file
                            if (!VerifyDtaCreation(strInputFilePath))
                            {
                                m_Results = ProcessResults.SF_NO_FILES_CREATED;
                            }
                            else
                            {
                                m_Results = ProcessResults.SF_SUCCESS;
                            }
                        }

                        m_FilterStatus = ProcessStatus.SF_COMPLETE;
                    }
                    else
                    {
                        if (m_MsMsSpectrumFilter.AbortProcessing)
                        {
                            LogErrors("FilterDTATextFileWork", "Processing aborted");
                            m_Results = ProcessResults.SF_ABORTED;
                            m_FilterStatus = ProcessStatus.SF_ABORTING;
                        }
                        else
                        {
                            LogErrors("FilterDTATextFileWork", m_MsMsSpectrumFilter.GetErrorMessage());
                            m_Results = ProcessResults.SF_FAILURE;
                            m_FilterStatus = ProcessStatus.SF_ERROR;
                        }
                    }

                    m_MsMsSpectrumFilter.CloseLogFileNow();
                }
                catch (Exception ex)
                {
                    LogErrors("FilterDTATextFileWork", "Error performing tasks after m_MsMsSpectrumFilter.ProcessFilesWildcard completes", ex);
                    m_FilterStatus = ProcessStatus.SF_ERROR;
                }
            }
            catch (Exception ex)
            {
                LogErrors("FilterDTATextFileWork", "Error calling m_MsMsSpectrumFilter.ProcessFilesWildcard", ex);
                m_FilterStatus = ProcessStatus.SF_ERROR;
            }
        }

        private bool GenerateFinniganScanStatsFiles()
        {

            try
            {
                if (m_DebugLevel >= 1)
                {
                    LogDebug("Looking for the _ScanStats.txt files for dataset " + m_Dataset);
                }

                var blnScanStatsFilesExist = clsMsMsSpectrumFilter.CheckForExistingScanStatsFiles(m_WorkDir, m_Dataset);
                if (blnScanStatsFilesExist)
                {
                    if (m_DebugLevel >= 1)
                    {
                        LogMessage("_ScanStats.txt files found for dataset " + m_Dataset);
                    }
                    return true;
                }

                LogMessage("Creating the _ScanStats.txt files for dataset " + m_Dataset);

                // Determine the path to the .Raw file
                var strRawFileName = m_Dataset + ".raw";
                var strFinniganRawFilePath = clsAnalysisResources.ResolveStoragePath(m_WorkDir, strRawFileName);

                if (string.IsNullOrEmpty(strFinniganRawFilePath))
                {
                    // Unable to resolve the file path
                    m_ErrMsg = "Could not find " + strRawFileName + " or " +
                        strRawFileName + clsAnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX +
                        " in the dataset folder; unable to generate the ScanStats files";
                    LogErrors("GenerateFinniganScanStatsFiles", m_ErrMsg);
                    return false;
                }

                // Look for an existing _ScanStats.txt file in a SIC folder below folder with the .Raw file
                var fiRawFile = new FileInfo(strFinniganRawFilePath);

                if (!fiRawFile.Exists)
                {
                    // File not found at the specified path
                    m_ErrMsg = "File not found: " + strFinniganRawFilePath + " -- unable to generate the ScanStats files";
                    LogErrors("GenerateFinniganScanStatsFiles", m_ErrMsg);
                    return false;
                }

                if (m_DebugLevel >= 1)
                {
                    LogMessage("Generating _ScanStats.txt file using " + strFinniganRawFilePath);
                }

                if (!m_MsMsSpectrumFilter.GenerateFinniganScanStatsFiles(strFinniganRawFilePath, m_WorkDir))
                {
                    if (m_DebugLevel >= 3)
                    {
                        LogDebug("GenerateFinniganScanStatsFiles returned False");
                    }

                    m_ErrMsg = m_MsMsSpectrumFilter.GetErrorMessage();
                    if (string.IsNullOrEmpty(m_ErrMsg))
                    {
                        m_ErrMsg = "GenerateFinniganScanStatsFiles returned False; _ScanStats.txt files not generated";
                    }

                    LogErrors("GenerateFinniganScanStatsFiles", m_ErrMsg);
                    return false;
                }

                if (m_DebugLevel >= 4)
                {
                    LogDebug("GenerateFinniganScanStatsFiles returned True");
                }
                return true;
            }
            catch (Exception ex)
            {
                LogErrors("GenerateFinniganScanStatsFiles", "Error generating _ScanStats.txt files", ex);
                return false;
            }

        }

        private DateTime dtLastStatusUpdate = DateTime.MinValue;

        private void HandleProgressUpdate(float percentComplete)
        {
            m_progress = percentComplete;

            // Update the status file (limit the updates to every 5 seconds)
            if (DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5)
            {
                dtLastStatusUpdate = DateTime.UtcNow;
                UpdateStatusRunning(m_progress, m_DtaCount);
            }

            LogProgress("MsMsSpectrumFilter");
        }

        protected virtual bool InitSetup()
        {
            // Initializes module variables and verifies mandatory parameters have been propery specified

            // Manager parameters
            if (m_mgrParams == null)
            {
                m_ErrMsg = "Manager parameters not specified";
                return false;
            }

            // Job parameters
            if (m_jobParams == null)
            {
                m_ErrMsg = "Job parameters not specified";
                return false;
            }

            // Status tools
            if (m_StatusTools == null)
            {
                m_ErrMsg = "Status tools object not set";
                return false;
            }

            // Set the _DTA.Txt file name
            m_DTATextFileName = m_Dataset + "_dta.txt";

            // Set settings file name
            // This is the job parameters file that contains the settings information
            m_SettingsFileName = m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, clsAnalysisResources.JOB_PARAM_XML_PARAMS_FILE);

            // Source folder name
            if (string.IsNullOrEmpty(m_WorkDir))
            {
                m_ErrMsg = "m_WorkDir variable is empty";
                return false;
            }

            // Source directory exist?
            if (!VerifyDirExists(m_WorkDir))
                return false;
            // Error msg handled by VerifyDirExists

            // Settings file exist?
            var SettingsNamePath = Path.Combine(m_WorkDir, m_SettingsFileName);
            if (!VerifyFileExists(SettingsNamePath))
                return false;
            // Error msg handled by VerifyFileExists

            // If we got here, everything's OK
            return true;
        }

        private void LogErrors(string source, string message, Exception ex = null)
        {
            m_ErrMsg = message.Replace("\n", "; ").Replace("\r", "");
            LogError(source + ": " + m_ErrMsg, ex);
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo()
        {
            var strToolVersionInfo = string.Empty;
            var strAppFolderPath = clsGlobal.GetAppFolderPath();

            if (m_DebugLevel >= 2)
            {
                LogDebug("Determining tool version info");
            }

            // Lookup the version of the MSMSSpectrumFilterAM
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "MSMSSpectrumFilterAM"))
            {
                return false;
            }

            // Lookup the version of the MsMsDataFileReader
            if (!StoreToolVersionInfoForLoadedAssembly(ref strToolVersionInfo, "MsMsDataFileReader"))
            {
                return false;
            }

            // Store the path to MsMsDataFileReader.dll in ioToolFiles
            var ioToolFiles = new List<FileInfo> {
                new FileInfo(Path.Combine(strAppFolderPath, "MsMsDataFileReader.dll"))
            };

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion: " + ex.Message);
                return false;
            }
        }

        protected virtual bool VerifyDirExists(string TestDir)
        {
            // Verifies that the specified directory exists
            if (Directory.Exists(TestDir))
            {
                m_ErrMsg = "";
                return true;
            }

            m_ErrMsg = "Directory " + TestDir + " not found";
            return false;
        }

        private bool VerifyDtaCreation(string strDTATextFilePath)
        {
            // Verify at least one .dta file has been created
            if (CountDtaFiles(strDTATextFilePath) < 1)
            {
                m_ErrMsg = "No dta files remain after filtering";
                return false;
            }

            return true;
        }

        protected virtual bool VerifyFileExists(string TestFile)
        {
            // Verifies specified file exists
            if (File.Exists(TestFile))
            {
                m_ErrMsg = "";
                return true;
            }

            m_ErrMsg = "File " + TestFile + " not found";
            return false;
        }

        /// <summary>
        /// Zips concatenated DTA file to reduce size
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        protected virtual CloseOutType ZipConcDtaFile()
        {
            // Zips the concatenated dta file
            var DtaFileName = m_Dataset + "_dta.txt";
            var DtaFilePath = Path.Combine(m_WorkDir, DtaFileName);

            // Verify file exists
            if (File.Exists(DtaFilePath))
            {
                LogMessage("Zipping concatenated spectra file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step"));
            }
            else
            {
                LogError("Unable to find concatenated dta file");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Zip the file
            try
            {
                if (!ZipFile(DtaFilePath, false))
                {
                    var Msg = "Error zipping concat dta file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step");
                    LogError(Msg);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                var Msg = "Exception zipping concat dta file, job " + m_JobNum + ", step " + m_jobParams.GetParam("Step") + ": " + ex.Message;
                LogError(Msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        #endregion

        private void m_MsMsSpectrumFilter_ProgressChanged(string taskDescription, float percentComplete)
        {
            HandleProgressUpdate(percentComplete);
        }

        private void m_MsMsSpectrumFilter_ProgressComplete()
        {
            HandleProgressUpdate(100);
        }
    }
}
