//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2008, Battelle Memorial Institute
// Created 07/29/2008
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using AnalysisManagerBase;
using MsMsDataFileReader;

namespace DTASpectraFileGen
{
    /// <summary>
    /// Base class for DTA generation tool runners
    /// </summary>
    /// <remarks></remarks>
    public class clsDtaGenToolRunner : clsAnalysisToolRunnerBase
    {
        #region "Constants and Enums"

        public const string CDTA_FILE_SUFFIX = clsAnalysisResources.CDTA_EXTENSION;

        private const int CENTROID_CDTA_PROGRESS_START = 70;

        public enum eDTAGeneratorConstants
        {
            Unknown = 0,
            ExtractMSn = 1,
            DeconMSn = 2,
            MSConvert = 3,
            MGFtoDTA = 4,
            DeconConsole = 5,
            RawConverter = 6
        }

        #endregion

        #region "Module-wide variables"

        private bool mCentroidDTAs;
        private bool mConcatenateDTAs;
        private int mStepNum;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs the analysis tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        public override CloseOutType RunTool()
        {
            // Do the stuff in the base class
            if (base.RunTool() != CloseOutType.CLOSEOUT_SUCCESS)
                return CloseOutType.CLOSEOUT_FAILED;

            mStepNum = mJobParams.GetJobParameter("Step", 0);

            // Create spectra files
            var result = CreateMSMSSpectra();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                // Something went wrong
                // In order to help diagnose things, we will move key files into the result folder,
                //  archive it using CopyFailedResultsToArchiveFolder, then return CloseOutType.CLOSEOUT_FAILED
                CopyFailedResultsToArchiveFolder();
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Stop the job timer
            mStopTime = DateTime.UtcNow;

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Get rid of raw data file
            result = DeleteDataFile();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Add all the extensions of the files to delete after run
            mJobParams.AddResultFileExtensionToSkip(CDTA_FILE_SUFFIX);    // Unzipped, concatenated DTA
            mJobParams.AddResultFileExtensionToSkip(".dta");              // DTA files
            mJobParams.AddResultFileExtensionToSkip("DeconMSn_progress.txt");

            // Add any files that are an exception to the captured files to delete list
            mJobParams.AddResultFileToKeep("lcq_dta.txt");

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;


        }

        /// <summary>
        /// Creates DTA files and filters if necessary
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType CreateMSMSSpectra()
        {
            // Make the spectra files
            var Result = MakeSpectraFiles();
            if (Result != CloseOutType.CLOSEOUT_SUCCESS)
                return Result;

            // Concatenate spectra files
            if (mConcatenateDTAs)
            {
                Result = ConcatSpectraFiles();
                if (Result != CloseOutType.CLOSEOUT_SUCCESS)
                    return Result;
            }

            if (mCentroidDTAs)
            {
                Result = CentroidCDTA();
                if (Result != CloseOutType.CLOSEOUT_SUCCESS)
                    return Result;
            }

            // Zip concatenated spectra files
            Result = ZipConcDtaFile();
            if (Result != CloseOutType.CLOSEOUT_SUCCESS)
                return Result;

            // If we got to here, everything's OK
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private eDTAGeneratorConstants GetDTAGenerator(out clsDtaGen spectraGen)
        {
            var eDtaGeneratorType = GetDTAGeneratorInfo(mJobParams, out mConcatenateDTAs, out var strErrorMessage);
            spectraGen = null;

            switch (eDtaGeneratorType)
            {
                case eDTAGeneratorConstants.MGFtoDTA:
                    spectraGen = new clsMGFtoDtaGenMainProcess();

                    break;
                case eDTAGeneratorConstants.MSConvert:
                    spectraGen = new clsDtaGenMSConvert();

                    break;
                case eDTAGeneratorConstants.DeconConsole:
                    LogError("DeconConsole is obsolete and should no longer be used");

                    return eDTAGeneratorConstants.Unknown;
                // spectraGen = New clsDtaGenDeconConsole()

                case eDTAGeneratorConstants.ExtractMSn:
                case eDTAGeneratorConstants.DeconMSn:
                    spectraGen = new clsDtaGenThermoRaw();

                    break;
                case eDTAGeneratorConstants.RawConverter:
                    spectraGen = new clsDtaGenRawConverter();

                    break;
                case eDTAGeneratorConstants.Unknown:
                    if (string.IsNullOrEmpty(strErrorMessage))
                    {
                        LogError("GetDTAGeneratorInfo reported an Unknown DTAGenerator type");
                    }
                    else
                    {
                        LogError(strErrorMessage);
                    }
                    break;
            }

            return eDtaGeneratorType;
        }

        public static eDTAGeneratorConstants GetDTAGeneratorInfo(IJobParams oJobParams, out string strErrorMessage)
        {
            return GetDTAGeneratorInfo(oJobParams, out _, out strErrorMessage);
        }

        public static eDTAGeneratorConstants GetDTAGeneratorInfo(IJobParams oJobParams, out bool blnConcatenateDTAs, out string strErrorMessage)
        {
            var strDTAGenerator = oJobParams.GetJobParameter("DtaGenerator", "");
            var strRawDataType = oJobParams.GetJobParameter("RawDataType", "");
            var blnMGFInstrumentData = oJobParams.GetJobParameter("MGFInstrumentData", false);

            strErrorMessage = string.Empty;
            blnConcatenateDTAs = true;

            if (string.IsNullOrEmpty(strRawDataType))
            {
                strErrorMessage = NotifyMissingParameter(oJobParams, "RawDataType");
                return eDTAGeneratorConstants.Unknown;
            }

            var eRawDataType = clsAnalysisResources.GetRawDataType(strRawDataType);

            if (blnMGFInstrumentData)
            {
                blnConcatenateDTAs = false;
                return eDTAGeneratorConstants.MGFtoDTA;
            }

            switch (eRawDataType)
            {
                case clsAnalysisResources.eRawDataTypeConstants.ThermoRawFile:

                    blnConcatenateDTAs = false;
                    switch (strDTAGenerator.ToLower())
                    {
                        case clsDtaGenThermoRaw.MSCONVERT_FILENAME_LOWER:

                            return eDTAGeneratorConstants.MSConvert;
                        case clsDtaGenThermoRaw.DECON_CONSOLE_FILENAME_LOWER:

                            return eDTAGeneratorConstants.DeconConsole;
                        case clsDtaGenThermoRaw.EXTRACT_MSN_FILENAME_LOWER:
                            blnConcatenateDTAs = true;

                            return eDTAGeneratorConstants.ExtractMSn;
                        case clsDtaGenThermoRaw.DECONMSN_FILENAME_LOWER:

                            return eDTAGeneratorConstants.DeconMSn;
                        case clsDtaGenThermoRaw.RAWCONVERTER_FILENAME_LOWER:

                            return eDTAGeneratorConstants.RawConverter;
                        default:
                            if (string.IsNullOrEmpty(strDTAGenerator))
                            {
                                strErrorMessage = NotifyMissingParameter(oJobParams, "DtaGenerator");
                            }
                            else
                            {
                                strErrorMessage = "Unknown DTAGenerator for Thermo Raw files: " + strDTAGenerator;
                            }

                            return eDTAGeneratorConstants.Unknown;
                    }

                case clsAnalysisResources.eRawDataTypeConstants.mzML:
                    if (strDTAGenerator.ToLower() == clsDtaGenThermoRaw.MSCONVERT_FILENAME.ToLower())
                    {
                        blnConcatenateDTAs = false;
                        return eDTAGeneratorConstants.MSConvert;
                    }
                    else
                    {
                        strErrorMessage = "Invalid DTAGenerator for mzML files: " + strDTAGenerator;
                        return eDTAGeneratorConstants.Unknown;
                    }

                case clsAnalysisResources.eRawDataTypeConstants.AgilentDFolder:
                    blnConcatenateDTAs = true;

                    return eDTAGeneratorConstants.MGFtoDTA;
                default:
                    strErrorMessage = "Unsupported data type for DTA generation: " + strRawDataType;

                    return eDTAGeneratorConstants.Unknown;
            }
        }

        /// <summary>
        /// Detailed method for running a tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        [Obsolete("This method is unused")]
        public CloseOutType DispositionResults()
        {
            // Make sure all files have released locks
            PRISM.ProgRunner.GarbageCollectNow();

            // Get rid of raw data file
            try
            {
                var stepResult = DeleteDataFile();
                if (stepResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return stepResult;
                }
            }
            catch (Exception ex)
            {
                LogError("clsDtaGenToolRunner.DispositionResults(), Exception while deleting data file", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Delete .dta files
            try
            {
                var dtaFiles = Directory.GetFiles(mWorkDir, "*.dta");
                foreach (var TmpFile in dtaFiles)
                {
                    DeleteFileWithRetries(TmpFile);
                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting .dta files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Delete unzipped concatenated dta files
            var cdtaFiles = Directory.GetFiles(mWorkDir, "*" + CDTA_FILE_SUFFIX);
            foreach (var TmpFile in cdtaFiles)
            {
                try
                {
                    if (Path.GetFileName(TmpFile.ToLower()) != "lcq_dta.txt")
                    {
                        DeleteFileWithRetries(TmpFile);
                    }
                }
                catch (Exception ex)
                {
                    LogError("Error deleting concatenated dta file", ex);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;

        }

        private SpectraFileProcessorParams GetDtaGenInitParams()
        {
            var initParams = new SpectraFileProcessorParams
            {
                DebugLevel = mDebugLevel,
                JobParams = mJobParams,
                MgrParams = mMgrParams,
                StatusTools = mStatusTools,
                WorkDir = mWorkDir,
                DatasetName = mDatasetName
            };

            return initParams;
        }

        /// <summary>
        /// Creates DTA files
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType MakeSpectraFiles()
        {
            // Make individual spectra files from input raw data file, using plugin

            LogMessage("Making spectra files, job " + mJob + ", step " + mStepNum);

            var eDtaGeneratorType = GetDTAGenerator(out var spectraGen);

            if (eDtaGeneratorType == eDTAGeneratorConstants.Unknown)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (eDtaGeneratorType == eDTAGeneratorConstants.DeconConsole)
            {
                mCentroidDTAs = false;
            }
            else
            {
                mCentroidDTAs = mJobParams.GetJobParameter("CentroidDTAs", false);
            }

            RegisterEvents(spectraGen);

            // Initialize the plugin

            try
            {
                spectraGen.Setup(GetDtaGenInitParams(), this);
            }
            catch (Exception ex)
            {
                LogError("Exception configuring DTAGenerator", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the Version info in the database
            bool blnSuccess;
            if (eDtaGeneratorType == eDTAGeneratorConstants.MGFtoDTA)
            {
                // MGFtoDTA Dll
                blnSuccess = StoreToolVersionInfoDLL(spectraGen.DtaToolNameLoc);
            }
            else
            {
                if (eDtaGeneratorType == eDTAGeneratorConstants.DeconConsole)
                {
                    // Possibly use a specific version of DeconTools
                    var progLoc = DetermineProgramLocation("DeconToolsProgLoc", "DeconConsole.exe");

                    if (string.IsNullOrWhiteSpace(progLoc))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    spectraGen.UpdateDtaToolNameLoc(progLoc);
                }

                blnSuccess = StoreToolVersionInfo(spectraGen.DtaToolNameLoc, eDtaGeneratorType);
            }

            if (!blnSuccess)
            {
                LogError("Aborting since StoreToolVersionInfo returned false for " + spectraGen.DtaToolNameLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (eDtaGeneratorType == eDTAGeneratorConstants.DeconMSn && mCentroidDTAs)
            {
                var blnUsingExistingResults = mJobParams.GetJobParameter(clsDtaGenResources.USING_EXISTING_DECONMSN_RESULTS, false);

                if (blnUsingExistingResults)
                {
                    // Confirm that the existing DeconMSn results are valid
                    // If they are, we don't need to re-run DeconMSn

                    if (ValidateDeconMSnResults())
                    {
                        mProgress = 100;
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }
                }
            }

            try
            {
                // Start the spectra generation process
                var eResult = StartAndWaitForDTAGenerator(spectraGen, "MakeSpectraFiles", false);

                // Set internal spectra file count to that returned by the spectra generator
                mDtaCount = spectraGen.SpectraFileCount;
                mProgress = spectraGen.Progress;

                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                    return eResult;
            }
            catch (Exception ex)
            {
                LogError("clsDtaGenToolRunner.MakeSpectraFiles: Exception while generating dta files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Creates a centroided .mgf file for the dataset
        /// Then updates the _DTA.txt file with the spectral data from the .mgf file
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        private CloseOutType CentroidCDTA()
        {
            string strCDTAFileOriginal;
            string strCDTAFileCentroided;

            try
            {
                // Rename the _DTA.txt file to _DTA_Original.txt
                var fiCDTA = new FileInfo(Path.Combine(mWorkDir, mDatasetName + CDTA_FILE_SUFFIX));
                if (!fiCDTA.Exists)
                {
                    LogError("File not found in CentroidCDTA: " + fiCDTA.Name);
                    return CloseOutType.CLOSEOUT_NO_DTA_FILES;
                }

                PRISM.ProgRunner.GarbageCollectNow();

                strCDTAFileOriginal = Path.Combine(mWorkDir, mDatasetName + "_DTA_Original.txt");
                fiCDTA.MoveTo(strCDTAFileOriginal);

                mJobParams.AddResultFileToSkip(fiCDTA.Name);
            }
            catch (Exception)
            {
                LogError("Error renaming the original _DTA.txt file in CentroidCDTA");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            try
            {
                // Create a centroided _DTA.txt file from the .Raw file (first creates a .MGF file, then converts to _DTA.txt)
                var oMSConvert = new clsDtaGenMSConvert();
                oMSConvert.Setup(GetDtaGenInitParams(), this);

                oMSConvert.ForceCentroidOn = true;

                var eResult = StartAndWaitForDTAGenerator(oMSConvert, "CentroidCDTA", true);

                if (eResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return eResult;
                }
            }
            catch (Exception ex)
            {
                LogError("Error creating a centroided _DTA.txt file in CentroidCDTA", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            try
            {
                // Rename the new _DTA.txt file to _DTA_Centroided.txt

                var fiCDTA = new FileInfo(Path.Combine(mWorkDir, mDatasetName + CDTA_FILE_SUFFIX));
                if (!fiCDTA.Exists)
                {
                    LogError("File not found in CentroidCDTA (after calling clsDtaGenMSConvert): " + fiCDTA.Name);
                    return CloseOutType.CLOSEOUT_NO_DTA_FILES;
                }

                PRISM.ProgRunner.GarbageCollectNow();

                strCDTAFileCentroided = Path.Combine(mWorkDir, mDatasetName + "_DTA_Centroided.txt");
                fiCDTA.MoveTo(strCDTAFileCentroided);

                mJobParams.AddResultFileToSkip(fiCDTA.Name);
            }
            catch (Exception ex)
            {
                LogError("Error renaming the centroided _DTA.txt file in CentroidCDTA", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            try
            {
                // Read _DTA_Original.txt and _DTA_Centroided.txt in parallel
                // Create the final _DTA.txt file

                var strCDTAFileFinal = Path.Combine(mWorkDir, mDatasetName + CDTA_FILE_SUFFIX);

                var blnSuccess = MergeCDTAs(strCDTAFileOriginal, strCDTAFileCentroided, strCDTAFileFinal);
                if (!blnSuccess)
                {
                    if (string.IsNullOrEmpty(mMessage))
                        mMessage = "MergeCDTAs returned False in CentroidCDTA";
                    LogError(mMessage);
                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                LogError("Error creating final _DTA.txt file in CentroidCDTA", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Concatenates DTA files into a single test file
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType ConcatSpectraFiles()
        {
            // Packages dta files into concatenated text files

            // Make sure at least one .dta file was created
            var diWorkDir = new DirectoryInfo(mWorkDir);
            var intDTACount = diWorkDir.GetFiles("*.dta").Length;

            if (intDTACount == 0)
            {
                LogError("No .DTA files were created");
                return CloseOutType.CLOSEOUT_NO_DTA_FILES;
            }

            if (mDebugLevel >= 1)
            {
                LogMessage("Concatenating spectra files, job " + mJob + ", step " + mStepNum);
            }

            var ConcatTools = new clsConcatToolWrapper(diWorkDir.FullName);

            if (!ConcatTools.ConcatenateFiles(clsConcatToolWrapper.ConcatFileTypes.CONCAT_DTA, mDatasetName))
            {
                LogError("Error packaging results: " + ConcatTools.ErrMsg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveFolder()
        {
            mJobParams.AddResultFileToSkip(Dataset + clsAnalysisResources.CDTA_ZIPPED_EXTENSION);
            mJobParams.AddResultFileToSkip(Dataset + clsAnalysisResources.CDTA_EXTENSION);

            // Skip any .dta files
            mJobParams.AddResultFileExtensionToSkip(".dta");

            base.CopyFailedResultsToArchiveFolder();
        }

        private string GetMSConvertAppPath()
        {
            var ProteoWizardDir = mMgrParams.GetParam("ProteoWizardDir");         // MSConvert.exe is stored in the ProteoWizard folder
            var progLoc = Path.Combine(ProteoWizardDir, clsDtaGenThermoRaw.MSCONVERT_FILENAME);

            return progLoc;
        }

        /// <summary>
        /// Deletes .raw files from working directory
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks>Overridden for other types of input files</remarks>
        private CloseOutType DeleteDataFile()
        {
            // Deletes the .raw file from the working directory

            if (mDebugLevel >= 2)
            {
                LogMessage("clsDtaGenToolRunner.DeleteDataFile, executing method");
            }

            // Delete the .raw file
            try
            {
                var lstFilesToDelete = new List<string>();
                lstFilesToDelete.AddRange(Directory.GetFiles(mWorkDir, "*" + clsAnalysisResources.DOT_RAW_EXTENSION));
                lstFilesToDelete.AddRange(Directory.GetFiles(mWorkDir, "*" + clsAnalysisResources.DOT_MZXML_EXTENSION));
                lstFilesToDelete.AddRange(Directory.GetFiles(mWorkDir, "*" + clsAnalysisResources.DOT_MZML_EXTENSION));
                lstFilesToDelete.AddRange(Directory.GetFiles(mWorkDir, "*" + clsAnalysisResources.DOT_MGF_EXTENSION));

                foreach (var MyFile in lstFilesToDelete)
                {
                    if (mDebugLevel >= 2)
                    {
                        LogMessage("clsDtaGenToolRunner.DeleteDataFile, deleting file " + MyFile);
                    }
                    DeleteFileWithRetries(MyFile);
                }
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error deleting .raw file, job " + mJob + ", step " + mStepNum, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool MergeCDTAs(string strCDTAWithParentIonData, string strCDTAWithFragIonData, string strCDTAFileFinal)
        {
            var dtLastStatus = DateTime.UtcNow;

            try
            {
                var oCDTAReaderParentIons = new clsDtaTextFileReader(false);
                if (!oCDTAReaderParentIons.OpenFile(strCDTAWithParentIonData))
                {
                    LogError("Error opening CDTA file with the parent ion data");
                    return false;
                }

                var oCDTAReaderFragIonData = new clsDtaTextFileReader(true);
                if (!oCDTAReaderFragIonData.OpenFile(strCDTAWithFragIonData))
                {
                    LogError("Error opening CDTA file with centroided spectra data");
                    return false;
                }

                // This dictionary is used to track the spectrum scan numbers in strCDTAWithFragIonData
                // This is used to reduce the number of times that oCDTAReaderFragIonData is closed and re-opened
                var fragIonDataScanStatus = new Dictionary<int, SortedSet<int>>();

                // Cache the Start/End scan combos in strCDTAWithFragIonData
                LogMessage("Scanning " + Path.GetFileName(strCDTAWithFragIonData) + " to cache the scan range for each MS/MS spectrum");

                while (true)
                {
                    var blnNextSpectrumAvailable = oCDTAReaderFragIonData.ReadNextSpectrum(out _, out var udtFragIonDataHeaderCentroid);

                    if (!blnNextSpectrumAvailable)
                    {
                        break;
                    }

                    var scanStart = udtFragIonDataHeaderCentroid.ScanNumberStart;
                    var scanEnd = udtFragIonDataHeaderCentroid.ScanNumberEnd;

                    if (fragIonDataScanStatus.TryGetValue(scanStart, out var endScanList))
                    {
                        if (!endScanList.Contains(scanEnd))
                        {
                            endScanList.Add(scanEnd);
                        }
                    }
                    else
                    {
                        endScanList = new SortedSet<int> { scanEnd };
                        fragIonDataScanStatus.Add(scanStart, endScanList);
                    }
                }

                // Close, then re-open strCDTAWithFragIonData
                oCDTAReaderFragIonData.CloseFile();
                var udtFragIonDataHeader = oCDTAReaderFragIonData.GetNewSpectrumHeaderInfo();

                oCDTAReaderFragIonData = new clsDtaTextFileReader(true);
                if (!oCDTAReaderFragIonData.OpenFile(strCDTAWithFragIonData))
                {
                    LogError("Error re-opening CDTA file with the fragment ion data (after initial scan of the file)");
                    return false;
                }

                LogMessage("Merging " + Path.GetFileName(strCDTAWithParentIonData) + " with " + Path.GetFileName(strCDTAWithFragIonData));

                var intSpectrumCountSkipped = 0;
                using (var swCDTAOut = new StreamWriter(new FileStream(strCDTAFileFinal, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (oCDTAReaderParentIons.ReadNextSpectrum(out _, out var udtParentIonDataHeader))
                    {
                        if (!ScanMatchIsPossible(udtParentIonDataHeader, fragIonDataScanStatus))
                        {
                            LogWarning("MergeCDTAs could not find spectrum with StartScan=" + udtParentIonDataHeader.ScanNumberStart + " and EndScan=" +
                                       udtParentIonDataHeader.ScanNumberEnd + " for " + Path.GetFileName(strCDTAWithParentIonData));
                            intSpectrumCountSkipped += 1;
                            continue;
                        }

                        bool blnNextSpectrumAvailable;

                        while (!ScanHeadersMatch(udtParentIonDataHeader, udtFragIonDataHeader))
                        {
                            blnNextSpectrumAvailable = oCDTAReaderFragIonData.ReadNextSpectrum(out _, out udtFragIonDataHeader);
                            if (!blnNextSpectrumAvailable)
                                break;
                        }

                        blnNextSpectrumAvailable = ScanHeadersMatch(udtParentIonDataHeader, udtFragIonDataHeader);
                        if (!blnNextSpectrumAvailable)
                        {
                            // We never did find a match; this is unexpected
                            // Try closing the FragIonData file, re-opening, and parsing again
                            oCDTAReaderFragIonData.CloseFile();
                            udtFragIonDataHeader = oCDTAReaderFragIonData.GetNewSpectrumHeaderInfo();

                            oCDTAReaderFragIonData = new clsDtaTextFileReader(true);
                            if (!oCDTAReaderFragIonData.OpenFile(strCDTAWithFragIonData))
                            {
                                LogError("Error re-opening CDTA file with the fragment ion data (when blnNextSpectrumAvailable = False)");
                                return false;
                            }

                            while (!ScanHeadersMatch(udtParentIonDataHeader, udtFragIonDataHeader))
                            {
                                blnNextSpectrumAvailable = oCDTAReaderFragIonData.ReadNextSpectrum(out _, out udtFragIonDataHeader);
                                if (!blnNextSpectrumAvailable)
                                    break;
                            }

                            blnNextSpectrumAvailable = ScanHeadersMatch(udtParentIonDataHeader, udtFragIonDataHeader);
                            if (!blnNextSpectrumAvailable)
                            {
                                LogWarning("MergeCDTAs could not find spectrum with StartScan=" + udtParentIonDataHeader.ScanNumberStart +
                                           " and EndScan=" + udtParentIonDataHeader.ScanNumberEnd + " for " +
                                           Path.GetFileName(strCDTAWithParentIonData));
                                intSpectrumCountSkipped += 1;
                            }
                        }

                        if (blnNextSpectrumAvailable)
                        {
                            swCDTAOut.WriteLine();
                            swCDTAOut.WriteLine(udtParentIonDataHeader.SpectrumTitleWithCommentChars);
                            swCDTAOut.WriteLine(udtParentIonDataHeader.ParentIonLineText);

                            var strDataLinesToAppend = RemoveTitleAndParentIonLines(oCDTAReaderFragIonData.GetMostRecentSpectrumFileText());

                            if (string.IsNullOrWhiteSpace(strDataLinesToAppend))
                            {
                                LogError("oCDTAReaderFragIonData.GetMostRecentSpectrumFileText returned empty text for " + "StartScan=" +
                                         udtParentIonDataHeader.ScanNumberStart + " and " + "EndScan=" + udtParentIonDataHeader.ScanNumberEnd +
                                         " in MergeCDTAs for " + Path.GetFileName(strCDTAWithParentIonData));
                                return false;
                            }

                            swCDTAOut.Write(strDataLinesToAppend);
                        }

                        if (DateTime.UtcNow.Subtract(dtLastStatus).TotalSeconds >= 30)
                        {
                            dtLastStatus = DateTime.UtcNow;
                            if (mDebugLevel >= 1)
                            {
                                LogMessage("Merging CDTAs, scan " + udtParentIonDataHeader.ScanNumberStart);
                            }
                        }
                    }
                }

                try
                {
                    oCDTAReaderParentIons.CloseFile();
                    oCDTAReaderFragIonData.CloseFile();
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                if (intSpectrumCountSkipped > 0)
                {
                    var msg = "Skipped " + intSpectrumCountSkipped + " spectra in MergeCDTAs since they were not created by MSConvert";
                    LogWarning(msg, true);
                }
            }
            catch (Exception ex)
            {
                LogError("Error merging CDTA files", ex);
                return false;
            }

            return true;
        }

        private string RemoveTitleAndParentIonLines(string strSpectrumText)
        {
            var sbOutput = new StringBuilder(strSpectrumText.Length);
            var blnPreviousLineWasTitleLine = false;

            using (var trReader = new StringReader(strSpectrumText))
            {
                while (trReader.Peek() > -1)
                {
                    var strLine = trReader.ReadLine();

                    if (strLine != null && strLine.StartsWith("="))
                    {
                        // Skip this line
                        blnPreviousLineWasTitleLine = true;
                    }
                    else if (blnPreviousLineWasTitleLine)
                    {
                        // Skip this line
                        blnPreviousLineWasTitleLine = false;
                    }
                    else if (!string.IsNullOrEmpty(strLine))
                    {
                        // Data line; keep it
                        sbOutput.AppendLine(strLine);
                    }
                }
            }

            return sbOutput.ToString();
        }

        private bool ScanMatchIsPossible(clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType udtParentIonDataHeader,
            IReadOnlyDictionary<int, SortedSet<int>> fragIonDataScanStatus)
        {
            if (fragIonDataScanStatus.TryGetValue(udtParentIonDataHeader.ScanNumberStart, out var endScanList))
            {
                if (endScanList.Contains(udtParentIonDataHeader.ScanNumberEnd))
                {
                    return true;
                }
            }

            return false;
        }

        private bool ScanHeadersMatch(clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType udtParentIonDataHeader,
            clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType udtFragIonDataHeader)
        {
            if (udtParentIonDataHeader.ScanNumberStart == udtFragIonDataHeader.ScanNumberStart)
            {
                if (udtParentIonDataHeader.ScanNumberEnd == udtFragIonDataHeader.ScanNumberEnd)
                {
                    return true;
                }

                // MSConvert wrote out these headers for dataset Athal0503_26Mar12_Jaguar_12-02-26
                // 3160.0001.13.dta
                // 3211.0001.11.dta
                // 3258.0001.12.dta
                // 3259.0001.13.dta

                // Thus, allow a match if ScanNumberStart matches but ScanNumberEnd is less than ScanNumberStart
                if (udtFragIonDataHeader.ScanNumberEnd < udtFragIonDataHeader.ScanNumberStart)
                {
                    return true;
                }
            }

            return false;
        }

        private CloseOutType StartAndWaitForDTAGenerator(ISpectraFileProcessor oDTAGenerator, string strCallingFunction, bool blnSecondPass)
        {
            var retVal = oDTAGenerator.Start();
            if (retVal == ProcessStatus.SF_ERROR)
            {
                LogError("Error starting spectra processor: " + oDTAGenerator.ErrMsg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (mDebugLevel > 0)
            {
                LogMessage("clsDtaGenToolRunner." + strCallingFunction + ": Spectra generation started");
            }

            // Loop until the spectra generator finishes
            while ((oDTAGenerator.Status == ProcessStatus.SF_STARTING) | (oDTAGenerator.Status == ProcessStatus.SF_RUNNING))
            {
                if (blnSecondPass)
                {
                    mProgress = CENTROID_CDTA_PROGRESS_START + oDTAGenerator.Progress * (100f - CENTROID_CDTA_PROGRESS_START) / 100f;
                }
                else
                {
                    if (mCentroidDTAs)
                    {
                        mProgress = oDTAGenerator.Progress * (CENTROID_CDTA_PROGRESS_START / 100f);
                    }
                    else
                    {
                        mProgress = oDTAGenerator.Progress;
                    }
                }

                UpdateStatusRunning(mProgress, oDTAGenerator.SpectraFileCount);

                // Delay for 5 seconds
                clsGlobal.IdleLoop(5);
            }

            UpdateStatusRunning(mProgress, oDTAGenerator.SpectraFileCount);

            // Check for reason spectra generator exited
            if (oDTAGenerator.Results == ProcessResults.SF_FAILURE)
            {
                LogError("Error making DTA files in " + strCallingFunction + ": " + oDTAGenerator.ErrMsg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (oDTAGenerator.Results == ProcessResults.SF_ABORTED)
            {
                LogError("DTA generation aborted in " + strCallingFunction + "");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (oDTAGenerator.Results == ProcessResults.SF_NO_FILES_CREATED)
            {
                LogError("No spectra files created in " + strCallingFunction);
                return CloseOutType.CLOSEOUT_NO_DTA_FILES;
            }

            if (mDebugLevel >= 2)
            {
                LogMessage("clsDtaGenToolRunner." + strCallingFunction + ": Spectra generation completed");
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string strDtaGeneratorAppPath, eDTAGeneratorConstants eDtaGenerator)
        {
            var strToolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogMessage("Determining tool version info");
            }

            var fiDtaGenerator = new FileInfo(strDtaGeneratorAppPath);
            if (!fiDtaGenerator.Exists)
            {
                try
                {
                    LogError("DtaGenerator not found: " + strDtaGeneratorAppPath);
                    strToolVersionInfo = "Unknown";
                    return SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), saveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion", ex);
                    return false;
                }
            }

            // Store strDtaGeneratorAppPath in toolFiles
            var toolFiles = new List<FileInfo> {
                fiDtaGenerator
            };

            if (eDtaGenerator == eDTAGeneratorConstants.DeconConsole || eDtaGenerator == eDTAGeneratorConstants.DeconMSn)
            {
                // Lookup the version of the DeconConsole or DeconMSn application
                string strDllPath;

                var blnSuccess = StoreToolVersionInfoViaSystemDiagnostics(ref strToolVersionInfo, fiDtaGenerator.FullName);
                if (!blnSuccess)
                    return false;

                if (eDtaGenerator == eDTAGeneratorConstants.DeconMSn)
                {
                    // DeconMSn
                    var deconEngineV2File = new FileInfo(Path.Combine(fiDtaGenerator.DirectoryName, "DeconEngineV2.dll"));
                    if (deconEngineV2File.Exists)
                    {
                        // C# version of DeconMSn (released in January 2017)
                        strDllPath = Path.Combine(fiDtaGenerator.DirectoryName, "DeconEngineV2.dll");
                        toolFiles.Add(new FileInfo(strDllPath));
                        blnSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strDllPath);
                    }
                    else
                    {
                        strDllPath = Path.Combine(fiDtaGenerator.DirectoryName, "DeconMSnEngine.dll");
                        toolFiles.Add(new FileInfo(strDllPath));
                        blnSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strDllPath);
                    }

                    if (!blnSuccess)
                        return false;
                }
                else if (eDtaGenerator == eDTAGeneratorConstants.DeconConsole)
                {
                    // DeconConsole re-implementation of DeconMSn (obsolete, superseded by C# version of DeconMSn that uses DeconEngineV2.dll)

                    // Lookup the version of the DeconTools Backend (in the DeconTools folder)
                    // In addition, add it to toolFiles
                    strDllPath = Path.Combine(fiDtaGenerator.DirectoryName, "DeconTools.Backend.dll");
                    toolFiles.Add(new FileInfo(strDllPath));
                    blnSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strDllPath);
                    if (!blnSuccess)
                        return false;

                    // Lookup the version of DeconEngineV2 (in the DeconTools folder)
                    strDllPath = Path.Combine(fiDtaGenerator.DirectoryName, "DeconEngineV2.dll");
                    blnSuccess = StoreToolVersionInfoOneFile(ref strToolVersionInfo, strDllPath);
                    if (!blnSuccess)
                        return false;
                }
            }

            // Possibly also store the MSConvert version
            if (mCentroidDTAs)
            {
                toolFiles.Add(new FileInfo(GetMSConvertAppPath()));
            }

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private bool StoreToolVersionInfoDLL(string strDtaGeneratorDLLPath)
        {
            var strToolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogMessage("Determining tool version info");
            }

            // Lookup the version of the DLL
            StoreToolVersionInfoOneFile(ref strToolVersionInfo, strDtaGeneratorDLLPath);

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new FileInfo(strDtaGeneratorDLLPath)
            };

            // Possibly also store the MSConvert version
            if (mCentroidDTAs)
            {
                toolFiles.Add(new FileInfo(GetMSConvertAppPath()));
            }

            try
            {
                return SetStepTaskToolVersion(strToolVersionInfo, toolFiles, saveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private bool ValidateDeconMSnResults()
        {
            var existingResultsAreValid = false;

            try
            {
                var fiDeconMSnLogFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_DeconMSn_log.txt"));

                if (!fiDeconMSnLogFile.Exists)
                {
                    LogWarning("DeconMSn_log.txt file not found; cannot use pre-existing DeconMSn results");
                    return false;
                }

                var headerLineFound = false;

                using (var srLogFile = new StreamReader(new FileStream(fiDeconMSnLogFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    while (!srLogFile.EndOfStream)
                    {
                        var strLineIn = srLogFile.ReadLine();

                        if (!string.IsNullOrEmpty(strLineIn))
                        {
                            if (headerLineFound)
                            {
                                // Found a data line
                                if (char.IsDigit(strLineIn[0]))
                                {
                                    existingResultsAreValid = true;
                                    break;
                                }
                            }
                            else if (strLineIn.StartsWith("MSn_Scan"))
                            {
                                // Found the header line
                                headerLineFound = true;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateDeconMSnResults", ex);
                return false;
            }

            return existingResultsAreValid;
        }

        /// <summary>
        /// Zips concatenated DTA file to reduce size
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType ZipConcDtaFile()
        {
            var DtaFileName = mDatasetName + clsAnalysisResources.CDTA_EXTENSION;
            var DtaFilePath = Path.Combine(mWorkDir, DtaFileName);

            LogMessage("Zipping concatenated spectra file, job " + mJob + ", step " + mStepNum);

            // Verify the _dta.txt file exists
            if (!File.Exists(DtaFilePath))
            {
                LogWarning("Error: Unable to find concatenated dta file");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Zip the file using IonicZip
            try
            {
                if (ZipFile(DtaFilePath, false))
                {
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }
            }
            catch (Exception ex)
            {
                var msg = "Exception zipping concat dta file, job " + mJob + ", step " + mStepNum + ": " + ex.Message;
                LogError(msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Occasionally the zip file is corrupted and will need to be zipped using ICSharpCode.SharpZipLib instead
            // If the file exists and is not zero bytes in length, try zipping again, but instead use ICSharpCode.SharpZipLib

            var fiZipFile = new FileInfo(GetZipFilePathForFile(DtaFilePath));
            if (!fiZipFile.Exists || fiZipFile.Length <= 0)
            {
                var msg = "Error zipping concat dta file, job " + mJob + ", step " + mStepNum;
                LogError(msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            try
            {

#pragma warning disable 618
                if (ZipFileSharpZipLib(DtaFilePath))
#pragma warning restore 618
                {
                    var warningMsg = string.Format("Zip file created using IonicZip was corrupted; successfully compressed it using SharpZipLib instead: {0}", DtaFileName);
                    LogWarning(warningMsg);
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                var msg = "Error zipping concat dta file using SharpZipLib, job " + mJob + ", step " + mStepNum;
                LogError(msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                var msg = "Exception zipping concat dta file using SharpZipLib, job " + mJob + ", step " + mStepNum;
                LogError(msg, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        #endregion
    }
}
