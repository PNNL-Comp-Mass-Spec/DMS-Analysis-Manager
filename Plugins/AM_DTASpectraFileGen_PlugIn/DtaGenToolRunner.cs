//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2008, Battelle Memorial Institute
// Created 07/29/2008
//
//*********************************************************************************************************

using AnalysisManagerBase;
using MsMsDataFileReader;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.JobConfig;

namespace DTASpectraFileGen
{
    /// <summary>
    /// Base class for DTA generation tool runners
    /// </summary>
    public class DtaGenToolRunner : AnalysisToolRunnerBase
    {
        // Ignore Spelling: backend, Bruker, CDTA, centroided, dta, pre

        public const string CDTA_FILE_SUFFIX = AnalysisResources.CDTA_EXTENSION;

        private const int CENTROID_CDTA_PROGRESS_START = 70;

        public enum DTAGeneratorConstants
        {
            Unknown = 0,
            ExtractMSn = 1,
            DeconMSn = 2,
            MSConvert = 3,
            MGFtoDTA = 4,
            DeconConsole = 5,
            RawConverter = 6
        }

        private bool mCentroidDTAs;
        private bool mConcatenateDTAs;
        private int mStepNum;

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
                // In order to help diagnose things, move the output files into the results directory,
                // archive it using CopyFailedResultsToArchiveDirectory, then return CloseOutType.CLOSEOUT_FAILED
                CopyFailedResultsToArchiveDirectory();
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
        public CloseOutType CreateMSMSSpectra()
        {
            // Make the spectra files
            var result = MakeSpectraFiles();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
                return result;

            // Concatenate spectra files
            if (mConcatenateDTAs)
            {
                result = ConcatSpectraFiles();

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    return result;
            }

            if (mCentroidDTAs)
            {
                result = CentroidCDTA();

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    return result;
            }

            // Zip concatenated spectra files
            result = ZipConcatenatedDtaFile();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
                return result;

            // If we got to here, everything's OK
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private DTAGeneratorConstants GetDTAGenerator(out DtaGen spectraGen)
        {
            var eDtaGeneratorType = GetDTAGeneratorInfo(mJobParams, out mConcatenateDTAs, out var errorMessage);
            spectraGen = null;

            switch (eDtaGeneratorType)
            {
                case DTAGeneratorConstants.MGFtoDTA:
                    spectraGen = new MGFtoDtaGenMainProcess();

                    break;
                case DTAGeneratorConstants.MSConvert:
                    spectraGen = new DtaGenMSConvert();

                    break;
                case DTAGeneratorConstants.DeconConsole:
                    LogError("DeconConsole is obsolete and should no longer be used");

                    return DTAGeneratorConstants.Unknown;
                // spectraGen = New DtaGenDeconConsole()

                case DTAGeneratorConstants.ExtractMSn:
                case DTAGeneratorConstants.DeconMSn:
                    spectraGen = new DtaGenThermoRaw();

                    break;
                case DTAGeneratorConstants.RawConverter:
                    spectraGen = new DtaGenRawConverter();

                    break;
                case DTAGeneratorConstants.Unknown:
                    if (string.IsNullOrEmpty(errorMessage))
                    {
                        LogError("GetDTAGeneratorInfo reported an Unknown DTAGenerator type");
                    }
                    else
                    {
                        LogError(errorMessage);
                    }
                    break;
            }

            return eDtaGeneratorType;
        }

        public static DTAGeneratorConstants GetDTAGeneratorInfo(IJobParams jobParams, out string errorMessage)
        {
            return GetDTAGeneratorInfo(jobParams, out _, out errorMessage);
        }

        public static DTAGeneratorConstants GetDTAGeneratorInfo(IJobParams jobParams, out bool concatenateDTAs, out string errorMessage)
        {
            var dtaGenerator = jobParams.GetJobParameter("DtaGenerator", string.Empty);
            var rawDataTypeName = jobParams.GetJobParameter("RawDataType", string.Empty);
            var mgfInstrumentData = jobParams.GetJobParameter("MGFInstrumentData", false);

            errorMessage = string.Empty;
            concatenateDTAs = true;

            if (string.IsNullOrEmpty(rawDataTypeName))
            {
                errorMessage = NotifyMissingParameter(jobParams, "RawDataType");
                return DTAGeneratorConstants.Unknown;
            }

            var rawDataType = AnalysisResources.GetRawDataType(rawDataTypeName);

            if (mgfInstrumentData)
            {
                concatenateDTAs = false;
                return DTAGeneratorConstants.MGFtoDTA;
            }

            switch (rawDataType)
            {
                case AnalysisResources.RawDataTypeConstants.ThermoRawFile:

                    concatenateDTAs = false;
                    switch (dtaGenerator.ToLower())
                    {
                        case DtaGenThermoRaw.MSCONVERT_FILENAME_LOWER:
                            return DTAGeneratorConstants.MSConvert;

                        case DtaGenThermoRaw.DECON_CONSOLE_FILENAME_LOWER:
                            return DTAGeneratorConstants.DeconConsole;

                        case DtaGenThermoRaw.EXTRACT_MSN_FILENAME_LOWER:
                            concatenateDTAs = true;
                            return DTAGeneratorConstants.ExtractMSn;

                        case DtaGenThermoRaw.DECONMSN_FILENAME_LOWER:
                            return DTAGeneratorConstants.DeconMSn;

                        case DtaGenThermoRaw.RAWCONVERTER_FILENAME_LOWER:
                            return DTAGeneratorConstants.RawConverter;

                        default:
                            if (string.IsNullOrEmpty(dtaGenerator))
                            {
                                errorMessage = NotifyMissingParameter(jobParams, "DtaGenerator");
                            }
                            else
                            {
                                errorMessage = "Unknown DTAGenerator for Thermo Raw files: " + dtaGenerator;
                            }

                            return DTAGeneratorConstants.Unknown;
                    }

                case AnalysisResources.RawDataTypeConstants.mzML:
                    if (string.Equals(dtaGenerator, DtaGenThermoRaw.MSCONVERT_FILENAME, StringComparison.OrdinalIgnoreCase))
                    {
                        concatenateDTAs = false;
                        return DTAGeneratorConstants.MSConvert;
                    }

                    errorMessage = "Invalid DTAGenerator for mzML files: " + dtaGenerator;
                    return DTAGeneratorConstants.Unknown;

                case AnalysisResources.RawDataTypeConstants.AgilentDFolder:
                    concatenateDTAs = true;
                    return DTAGeneratorConstants.MGFtoDTA;

                case AnalysisResources.RawDataTypeConstants.BrukerTOFTdf:

                    concatenateDTAs = false;

                    if (string.Equals(dtaGenerator, DtaGenThermoRaw.MSCONVERT_FILENAME, StringComparison.OrdinalIgnoreCase))
                    {
                        return DTAGeneratorConstants.MSConvert;
                    }

                    if (string.IsNullOrEmpty(dtaGenerator))
                    {
                        errorMessage = NotifyMissingParameter(jobParams, "DtaGenerator");
                    }
                    else
                    {
                        errorMessage = "Bruker analysis.tdf files can only be converted to .mgf using MSConvert";
                    }

                    return DTAGeneratorConstants.Unknown;

                default:
                    errorMessage = "Unsupported data type for DTA generation: " + rawDataType;
                    return DTAGeneratorConstants.Unknown;
            }
        }

        /// <summary>
        /// Detailed method for running a tool
        /// </summary>
        /// <returns>CloseOutType enum indicating success or failure</returns>
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
                LogError("DtaGenToolRunner.DispositionResults(), Exception while deleting data file", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Delete .dta files
            try
            {
                var dtaFiles = Directory.GetFiles(mWorkDir, "*.dta");

                foreach (var targetFile in dtaFiles)
                {
                    DeleteFileWithRetries(targetFile);
                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting .dta files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Delete unzipped concatenated dta files
            var cdtaFiles = Directory.GetFiles(mWorkDir, "*" + CDTA_FILE_SUFFIX);

            foreach (var cdtaFile in cdtaFiles)
            {
                try
                {
                    if (Path.GetFileName(cdtaFile.ToLower()) != "lcq_dta.txt")
                    {
                        DeleteFileWithRetries(cdtaFile);
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
        public CloseOutType MakeSpectraFiles()
        {
            // Make individual spectra files from input raw data file, using plugin

            LogMessage("Making spectra files, job " + mJob + ", step " + mStepNum);

            var eDtaGeneratorType = GetDTAGenerator(out var spectraGen);

            if (eDtaGeneratorType == DTAGeneratorConstants.Unknown)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (eDtaGeneratorType == DTAGeneratorConstants.DeconConsole)
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
            bool success;

            if (eDtaGeneratorType == DTAGeneratorConstants.MGFtoDTA)
            {
                // MGFtoDTA DLL
                success = StoreToolVersionInfoDLL(spectraGen.DtaToolNameLoc);
            }
            else
            {
                if (eDtaGeneratorType == DTAGeneratorConstants.DeconConsole)
                {
                    // Possibly use a specific version of DeconTools
                    var progLoc = DetermineProgramLocation("DeconToolsProgLoc", "DeconConsole.exe");

                    if (string.IsNullOrWhiteSpace(progLoc))
                    {
                        return CloseOutType.CLOSEOUT_FAILED;
                    }

                    spectraGen.UpdateDtaToolNameLoc(progLoc);
                }

                success = StoreToolVersionInfo(spectraGen.DtaToolNameLoc, eDtaGeneratorType);
            }

            if (!success)
            {
                LogError("Aborting since StoreToolVersionInfo returned false for " + spectraGen.DtaToolNameLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (eDtaGeneratorType == DTAGeneratorConstants.DeconMSn && mCentroidDTAs)
            {
                var usingExistingResults = mJobParams.GetJobParameter(DtaGenResources.USING_EXISTING_DECONMSN_RESULTS, false);

                if (usingExistingResults)
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
                var result = StartAndWaitForDTAGenerator(spectraGen, "MakeSpectraFiles", false);

                // Set internal spectra file count to that returned by the spectra generator
                mDtaCount = spectraGen.SpectraFileCount;
                mProgress = spectraGen.Progress;

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                    return result;
            }
            catch (Exception ex)
            {
                LogError("DtaGenToolRunner.MakeSpectraFiles: Exception while generating dta files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Creates a centroided .mgf file for the dataset
        /// Then updates the _DTA.txt file with the spectral data from the .mgf file
        /// </summary>
        private CloseOutType CentroidCDTA()
        {
            string cdtaFileOriginal;
            string cdtaFileCentroided;

            try
            {
                // Rename the _DTA.txt file to _DTA_Original.txt
                var cdtaFileInfo = new FileInfo(Path.Combine(mWorkDir, mDatasetName + CDTA_FILE_SUFFIX));

                if (!cdtaFileInfo.Exists)
                {
                    LogError("File not found in CentroidCDTA: " + cdtaFileInfo.Name);
                    return CloseOutType.CLOSEOUT_NO_DTA_FILES;
                }

                PRISM.AppUtils.GarbageCollectNow();

                cdtaFileOriginal = Path.Combine(mWorkDir, mDatasetName + "_DTA_Original.txt");
                cdtaFileInfo.MoveTo(cdtaFileOriginal);

                mJobParams.AddResultFileToSkip(cdtaFileInfo.Name);
            }
            catch (Exception)
            {
                LogError("Error renaming the original _DTA.txt file in CentroidCDTA");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            try
            {
                // Create a centroided _DTA.txt file from the .Raw file (first creates a .MGF file, then converts to _DTA.txt)
                var msConvertRunner = new DtaGenMSConvert();
                msConvertRunner.Setup(GetDtaGenInitParams(), this);

                msConvertRunner.ForceCentroidOn = true;

                var result = StartAndWaitForDTAGenerator(msConvertRunner, "CentroidCDTA", true);

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
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

                var cdtaFileInfo = new FileInfo(Path.Combine(mWorkDir, mDatasetName + CDTA_FILE_SUFFIX));

                if (!cdtaFileInfo.Exists)
                {
                    LogError("File not found in CentroidCDTA (after calling DtaGenMSConvert): " + cdtaFileInfo.Name);
                    return CloseOutType.CLOSEOUT_NO_DTA_FILES;
                }

                PRISM.AppUtils.GarbageCollectNow();

                cdtaFileCentroided = Path.Combine(mWorkDir, mDatasetName + "_DTA_Centroided.txt");
                cdtaFileInfo.MoveTo(cdtaFileCentroided);

                mJobParams.AddResultFileToSkip(cdtaFileInfo.Name);
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

                var cdtaFileFinal = Path.Combine(mWorkDir, mDatasetName + CDTA_FILE_SUFFIX);

                var success = MergeCDTAs(cdtaFileOriginal, cdtaFileCentroided, cdtaFileFinal);

                if (!success)
                {
                    if (string.IsNullOrEmpty(mMessage))
                        mMessage = "MergeCDTAs returned false in CentroidCDTA";
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
        private CloseOutType ConcatSpectraFiles()
        {
            // Packages dta files into concatenated text files

            // Make sure at least one .dta file was created
            var workDir = new DirectoryInfo(mWorkDir);
            var dTACount = workDir.GetFiles("*.dta").Length;

            if (dTACount == 0)
            {
                LogError("No .DTA files were created");
                return CloseOutType.CLOSEOUT_NO_DTA_FILES;
            }

            if (mDebugLevel >= 1)
            {
                LogMessage("Concatenating spectra files, job " + mJob + ", step " + mStepNum);
            }

            var concatTools = new ConcatToolWrapper(workDir.FullName);

            if (!concatTools.ConcatenateFiles(ConcatToolWrapper.ConcatFileTypes.CONCAT_DTA, mDatasetName))
            {
                LogError("Error packaging results: " + concatTools.ErrMsg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Copy failed results from the working directory to the DMS_FailedResults directory on the local computer
        /// </summary>
        public override void CopyFailedResultsToArchiveDirectory()
        {
            mJobParams.AddResultFileToSkip(Dataset + AnalysisResources.CDTA_ZIPPED_EXTENSION);
            mJobParams.AddResultFileToSkip(Dataset + AnalysisResources.CDTA_EXTENSION);

            // Skip any .dta files
            mJobParams.AddResultFileExtensionToSkip(".dta");

            base.CopyFailedResultsToArchiveDirectory();
        }

        private string GetMSConvertAppPath()
        {
            var proteoWizardDir = mMgrParams.GetParam("ProteoWizardDir");         // MSConvert.exe is stored in the ProteoWizard folder
            return Path.Combine(proteoWizardDir, DtaGenThermoRaw.MSCONVERT_FILENAME);
        }

        /// <summary>
        /// Deletes .raw files from working directory
        /// </summary>
        /// <remarks>Overridden for other types of input files</remarks>
        /// <returns>CloseOutType enum indicating success or failure</returns>
        private CloseOutType DeleteDataFile()
        {
            // Deletes the .raw file from the working directory

            if (mDebugLevel >= 2)
            {
                LogMessage("DtaGenToolRunner.DeleteDataFile, executing method");
            }

            // Delete the .raw file
            try
            {
                var filesToDelete = new List<string>();
                filesToDelete.AddRange(Directory.GetFiles(mWorkDir, "*" + AnalysisResources.DOT_RAW_EXTENSION));
                filesToDelete.AddRange(Directory.GetFiles(mWorkDir, "*" + AnalysisResources.DOT_MZXML_EXTENSION));
                filesToDelete.AddRange(Directory.GetFiles(mWorkDir, "*" + AnalysisResources.DOT_MZML_EXTENSION));
                filesToDelete.AddRange(Directory.GetFiles(mWorkDir, "*" + AnalysisResources.DOT_MGF_EXTENSION));

                foreach (var targetFile in filesToDelete)
                {
                    if (mDebugLevel >= 2)
                    {
                        LogMessage("DtaGenToolRunner.DeleteDataFile, deleting file " + targetFile);
                    }
                    DeleteFileWithRetries(targetFile);
                }
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error deleting .raw file, job " + mJob + ", step " + mStepNum, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool MergeCDTAs(string cdtaWithParentIonData, string cdtaWithFragIonData, string cdtaFileFinal)
        {
            var lastStatus = DateTime.UtcNow;

            try
            {
                var cdtaReaderParentIons = new clsDtaTextFileReader(false);

                if (!cdtaReaderParentIons.OpenFile(cdtaWithParentIonData))
                {
                    LogError("Error opening CDTA file with the parent ion data");
                    return false;
                }

                var cdtaReaderFragIonData = new clsDtaTextFileReader(true);

                if (!cdtaReaderFragIonData.OpenFile(cdtaWithFragIonData))
                {
                    LogError("Error opening CDTA file with centroided spectra data");
                    return false;
                }

                // This dictionary is used to track the spectrum scan numbers in cdtaWithFragIonData
                // This is used to reduce the number of times that the DtaTextFileReader is closed and re-opened
                var fragIonDataScanStatus = new Dictionary<int, SortedSet<int>>();

                // Cache the Start/End scan combos in cdtaWithFragIonData
                LogMessage("Scanning " + Path.GetFileName(cdtaWithFragIonData) + " to cache the scan range for each MS/MS spectrum");

                while (true)
                {
                    var nextSpectrumAvailable = cdtaReaderFragIonData.ReadNextSpectrum(out _, out var fragIonDataHeaderCentroid);

                    if (!nextSpectrumAvailable)
                    {
                        break;
                    }

                    var scanStart = fragIonDataHeaderCentroid.ScanNumberStart;
                    var scanEnd = fragIonDataHeaderCentroid.ScanNumberEnd;

                    if (fragIonDataScanStatus.TryGetValue(scanStart, out var endScanList))
                    {
                        // Add the scan if not yet present
                        endScanList.Add(scanEnd);
                    }
                    else
                    {
                        endScanList = new SortedSet<int> { scanEnd };
                        fragIonDataScanStatus.Add(scanStart, endScanList);
                    }
                }

                // Close, then re-open cdtaWithFragIonData
                cdtaReaderFragIonData.CloseFile();
                var fragIonDataHeader = cdtaReaderFragIonData.GetNewSpectrumHeaderInfo();

                cdtaReaderFragIonData = new clsDtaTextFileReader(true);

                if (!cdtaReaderFragIonData.OpenFile(cdtaWithFragIonData))
                {
                    LogError("Error re-opening CDTA file with the fragment ion data (after initial scan of the file)");
                    return false;
                }

                LogMessage("Merging " + Path.GetFileName(cdtaWithParentIonData) + " with " + Path.GetFileName(cdtaWithFragIonData));

                var spectrumCountSkipped = 0;
                using (var writer = new StreamWriter(new FileStream(cdtaFileFinal, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (cdtaReaderParentIons.ReadNextSpectrum(out _, out var parentIonDataHeader))
                    {
                        if (!ScanMatchIsPossible(parentIonDataHeader, fragIonDataScanStatus))
                        {
                            LogWarning("MergeCDTAs could not find spectrum with StartScan=" + parentIonDataHeader.ScanNumberStart + " and EndScan=" +
                                       parentIonDataHeader.ScanNumberEnd + " for " + Path.GetFileName(cdtaWithParentIonData));
                            spectrumCountSkipped++;
                            continue;
                        }

                        bool nextSpectrumAvailable;

                        while (!ScanHeadersMatch(parentIonDataHeader, fragIonDataHeader))
                        {
                            nextSpectrumAvailable = cdtaReaderFragIonData.ReadNextSpectrum(out _, out fragIonDataHeader);

                            if (!nextSpectrumAvailable)
                                break;
                        }

                        nextSpectrumAvailable = ScanHeadersMatch(parentIonDataHeader, fragIonDataHeader);

                        if (!nextSpectrumAvailable)
                        {
                            // We never did find a match; this is unexpected
                            // Try closing the FragIonData file, re-opening, and parsing again
                            cdtaReaderFragIonData.CloseFile();
                            fragIonDataHeader = cdtaReaderFragIonData.GetNewSpectrumHeaderInfo();

                            cdtaReaderFragIonData = new clsDtaTextFileReader(true);

                            if (!cdtaReaderFragIonData.OpenFile(cdtaWithFragIonData))
                            {
                                LogError("Error re-opening CDTA file with the fragment ion data (when nextSpectrumAvailable is false)");
                                return false;
                            }

                            while (!ScanHeadersMatch(parentIonDataHeader, fragIonDataHeader))
                            {
                                nextSpectrumAvailable = cdtaReaderFragIonData.ReadNextSpectrum(out _, out fragIonDataHeader);

                                if (!nextSpectrumAvailable)
                                    break;
                            }

                            nextSpectrumAvailable = ScanHeadersMatch(parentIonDataHeader, fragIonDataHeader);

                            if (!nextSpectrumAvailable)
                            {
                                LogWarning("MergeCDTAs could not find spectrum with StartScan=" + parentIonDataHeader.ScanNumberStart +
                                           " and EndScan=" + parentIonDataHeader.ScanNumberEnd + " for " +
                                           Path.GetFileName(cdtaWithParentIonData));
                                spectrumCountSkipped++;
                            }
                        }

                        if (nextSpectrumAvailable)
                        {
                            writer.WriteLine();
                            writer.WriteLine(parentIonDataHeader.SpectrumTitleWithCommentChars);
                            writer.WriteLine(parentIonDataHeader.ParentIonLineText);

                            var dataLinesToAppend = RemoveTitleAndParentIonLines(cdtaReaderFragIonData.GetMostRecentSpectrumFileText());

                            if (string.IsNullOrWhiteSpace(dataLinesToAppend))
                            {
                                LogError("DtaTextFileReader.GetMostRecentSpectrumFileText returned empty text for " +
                                         "StartScan=" + parentIonDataHeader.ScanNumberStart + " and " +
                                         "EndScan=" + parentIonDataHeader.ScanNumberEnd +
                                         " in MergeCDTAs for " + Path.GetFileName(cdtaWithParentIonData));
                                return false;
                            }

                            writer.Write(dataLinesToAppend);
                        }

                        if (DateTime.UtcNow.Subtract(lastStatus).TotalSeconds >= 30)
                        {
                            lastStatus = DateTime.UtcNow;

                            if (mDebugLevel >= 1)
                            {
                                LogMessage("Merging CDTAs, scan " + parentIonDataHeader.ScanNumberStart);
                            }
                        }
                    }
                }

                try
                {
                    cdtaReaderParentIons.CloseFile();
                    cdtaReaderFragIonData.CloseFile();
                }
                catch (Exception)
                {
                    // Ignore errors here
                }

                if (spectrumCountSkipped > 0)
                {
                    LogWarning(string.Format("Skipped {0} spectra in MergeCDTAs since they were not created by MSConvert", spectrumCountSkipped), true);
                }
            }
            catch (Exception ex)
            {
                LogError("Error merging CDTA files", ex);
                return false;
            }

            return true;
        }

        private string RemoveTitleAndParentIonLines(string spectrumText)
        {
            var output = new StringBuilder(spectrumText.Length);
            var previousLineWasTitleLine = false;

            using var reader = new StringReader(spectrumText);

            while (reader.Peek() > -1)
            {
                var dataLine = reader.ReadLine();

                if (dataLine != null && dataLine.StartsWith("="))
                {
                    // Skip this line
                    previousLineWasTitleLine = true;
                }
                else if (previousLineWasTitleLine)
                {
                    // Skip this line
                    previousLineWasTitleLine = false;
                }
                else if (!string.IsNullOrEmpty(dataLine))
                {
                    // Data line; keep it
                    output.AppendLine(dataLine);
                }
            }

            return output.ToString();
        }

        private bool RenameZipFileIfRequired(string workDir, string sourceFileName, string finalZipFileName)
        {
            if (string.IsNullOrWhiteSpace(finalZipFileName))
                return true;

            var zipFileToRename = new FileInfo(Path.Combine(workDir, sourceFileName));

            if (!zipFileToRename.Exists)
            {
                LogError("Zip file not found, job " + mJob + ", step " + mStepNum + ": " + zipFileToRename.FullName);
                return false;
            }

            zipFileToRename.MoveTo(Path.Combine(mWorkDir, finalZipFileName));
            return true;
        }

        private bool ScanMatchIsPossible(clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType parentIonDataHeader,
            IReadOnlyDictionary<int, SortedSet<int>> fragIonDataScanStatus)
        {
            if (fragIonDataScanStatus.TryGetValue(parentIonDataHeader.ScanNumberStart, out var endScanList))
            {
                if (endScanList.Contains(parentIonDataHeader.ScanNumberEnd))
                {
                    return true;
                }
            }

            return false;
        }

        private bool ScanHeadersMatch(clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType parentIonDataHeader,
                                      clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType fragIonDataHeader)
        {
            if (parentIonDataHeader.ScanNumberStart == fragIonDataHeader.ScanNumberStart)
            {
                if (parentIonDataHeader.ScanNumberEnd == fragIonDataHeader.ScanNumberEnd)
                {
                    return true;
                }

                // ReSharper disable once CommentTypo
                // MSConvert wrote out these headers for dataset Athal0503_26Mar12_Jaguar_12-02-26
                // 3160.0001.13.dta
                // 3211.0001.11.dta
                // 3258.0001.12.dta
                // 3259.0001.13.dta

                // Thus, allow a match if ScanNumberStart matches but ScanNumberEnd is less than ScanNumberStart
                if (fragIonDataHeader.ScanNumberEnd < fragIonDataHeader.ScanNumberStart)
                {
                    return true;
                }
            }

            return false;
        }

        private CloseOutType StartAndWaitForDTAGenerator(ISpectraFileProcessor dtaGenerator, string callingFunction, bool secondPass)
        {
            var retVal = dtaGenerator.Start();

            if (retVal == ProcessStatus.SF_ERROR)
            {
                LogError("Error starting spectra processor: " + dtaGenerator.ErrMsg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (mDebugLevel > 0)
            {
                LogMessage("DtaGenToolRunner." + callingFunction + ": Spectra generation started");
            }

            // Loop until the spectra generator finishes
            while (dtaGenerator.Status == ProcessStatus.SF_STARTING || dtaGenerator.Status == ProcessStatus.SF_RUNNING)
            {
                if (secondPass)
                {
                    mProgress = CENTROID_CDTA_PROGRESS_START + dtaGenerator.Progress * (100f - CENTROID_CDTA_PROGRESS_START) / 100f;
                }
                else
                {
                    if (mCentroidDTAs)
                    {
                        mProgress = dtaGenerator.Progress * (CENTROID_CDTA_PROGRESS_START / 100f);
                    }
                    else
                    {
                        mProgress = dtaGenerator.Progress;
                    }
                }

                UpdateStatusRunning(mProgress, dtaGenerator.SpectraFileCount);

                // Delay for 5 seconds
                Global.IdleLoop(5);
            }

            UpdateStatusRunning(mProgress, dtaGenerator.SpectraFileCount);

            // Check for reason spectra generator exited
            if (dtaGenerator.Results == ProcessResults.SF_FAILURE)
            {
                LogError("Error making DTA files in " + callingFunction + ": " + dtaGenerator.ErrMsg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (dtaGenerator.Results == ProcessResults.SF_ABORTED)
            {
                LogError("DTA generation aborted in " + callingFunction + "");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (dtaGenerator.Results == ProcessResults.SF_NO_FILES_CREATED)
            {
                LogError("No spectra files created in " + callingFunction);
                return CloseOutType.CLOSEOUT_NO_DTA_FILES;
            }

            if (mDebugLevel >= 2)
            {
                LogMessage("DtaGenToolRunner." + callingFunction + ": Spectra generation completed");
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        private bool StoreToolVersionInfo(string dtaGeneratorAppPath, DTAGeneratorConstants eDtaGenerator)
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogMessage("Determining tool version info");
            }

            var dtaGenerator = new FileInfo(dtaGeneratorAppPath);

            if (!dtaGenerator.Exists)
            {
                try
                {
                    LogError("DtaGenerator not found: " + dtaGeneratorAppPath);
                    toolVersionInfo = "Unknown";
                    return SetStepTaskToolVersion(toolVersionInfo, new List<FileInfo>(), false);
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion", ex);
                    return false;
                }
            }

            // Store dtaGeneratorAppPath in toolFiles
            var toolFiles = new List<FileInfo> {
                dtaGenerator
            };

            if (eDtaGenerator == DTAGeneratorConstants.DeconConsole || eDtaGenerator == DTAGeneratorConstants.DeconMSn)
            {
                // Lookup the version of the DeconConsole or DeconMSn application
                string dllPath;

                var success = mToolVersionUtilities.StoreToolVersionInfoViaSystemDiagnostics(ref toolVersionInfo, dtaGenerator.FullName);

                if (!success)
                    return false;

                if (eDtaGenerator == DTAGeneratorConstants.DeconMSn)
                {
                    // DeconMSn
                    var deconEngineV2File = new FileInfo(Path.Combine(dtaGenerator.DirectoryName, "DeconEngineV2.dll"));

                    if (deconEngineV2File.Exists)
                    {
                        // C# version of DeconMSn (released in January 2017)
                        dllPath = Path.Combine(dtaGenerator.DirectoryName, "DeconEngineV2.dll");
                        toolFiles.Add(new FileInfo(dllPath));
                        success = StoreToolVersionInfoOneFile(ref toolVersionInfo, dllPath);
                    }
                    else
                    {
                        dllPath = Path.Combine(dtaGenerator.DirectoryName, "DeconMSnEngine.dll");
                        toolFiles.Add(new FileInfo(dllPath));
                        success = StoreToolVersionInfoOneFile(ref toolVersionInfo, dllPath);
                    }

                    if (!success)
                        return false;
                }
                else if (eDtaGenerator == DTAGeneratorConstants.DeconConsole)
                {
                    // DeconConsole re-implementation of DeconMSn (obsolete, superseded by C# version of DeconMSn that uses DeconEngineV2.dll)

                    // Lookup the version of the DeconTools backend (in the DeconTools folder)
                    // In addition, add it to toolFiles
                    dllPath = Path.Combine(dtaGenerator.DirectoryName, "DeconTools.Backend.dll");
                    toolFiles.Add(new FileInfo(dllPath));
                    success = StoreToolVersionInfoOneFile(ref toolVersionInfo, dllPath);

                    if (!success)
                        return false;

                    // Lookup the version of DeconEngineV2 (in the DeconTools folder)
                    dllPath = Path.Combine(dtaGenerator.DirectoryName, "DeconEngineV2.dll");
                    success = StoreToolVersionInfoOneFile(ref toolVersionInfo, dllPath);

                    if (!success)
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
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private bool StoreToolVersionInfoDLL(string dtaGeneratorDLLPath)
        {
            var toolVersionInfo = string.Empty;

            if (mDebugLevel >= 2)
            {
                LogMessage("Determining tool version info");
            }

            // Lookup the version of the DLL
            StoreToolVersionInfoOneFile(ref toolVersionInfo, dtaGeneratorDLLPath);

            // Store paths to key files in toolFiles
            var toolFiles = new List<FileInfo> {
                new(dtaGeneratorDLLPath)
            };

            // Possibly also store the MSConvert version
            if (mCentroidDTAs)
            {
                toolFiles.Add(new FileInfo(GetMSConvertAppPath()));
            }

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, toolFiles, false);
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
                var deconMSnLogFile = new FileInfo(Path.Combine(mWorkDir, mDatasetName + "_DeconMSn_log.txt"));

                if (!deconMSnLogFile.Exists)
                {
                    LogWarning("DeconMSn_log.txt file not found; cannot use pre-existing DeconMSn results");
                    return false;
                }

                var headerLineFound = false;

                using var reader = new StreamReader(new FileStream(deconMSnLogFile.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (!string.IsNullOrEmpty(dataLine))
                    {
                        if (headerLineFound)
                        {
                            // Found a data line
                            if (char.IsDigit(dataLine[0]))
                            {
                                existingResultsAreValid = true;
                                break;
                            }
                        }
                        else if (dataLine.StartsWith("MSn_Scan"))
                        {
                            // Found the header line
                            headerLineFound = true;
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
        private CloseOutType ZipConcatenatedDtaFile()
        {
            var convertToCDTA = mJobParams.GetJobParameter("DtaGenerator", "ConvertMGFtoCDTA", true);

            string inputFileExtension;
            string finalZipFileName;

            if (convertToCDTA)
            {
                inputFileExtension = AnalysisResources.CDTA_EXTENSION;
                finalZipFileName = string.Empty;
            }
            else
            {
                inputFileExtension = AnalysisResources.DOT_MGF_EXTENSION;
                finalZipFileName = mDatasetName + "_mgf.zip";
            }

            var inputFileName = mDatasetName + inputFileExtension;
            var inputFilePath = Path.Combine(mWorkDir, inputFileName);

            LogMessage("Zipping concatenated spectra file, job " + mJob + ", step " + mStepNum);

            // Verify the _dta.txt or .mgf file exists
            if (!File.Exists(inputFilePath))
            {
                LogWarning("Error: Unable to find spectrum file " + inputFileName);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Zip the file using System.IO.Compression.ZipFile
            try
            {
                if (ZipFile(inputFilePath, false))
                {
                    if (RenameZipFileIfRequired(mWorkDir, mDatasetName + ".zip", finalZipFileName))
                        return CloseOutType.CLOSEOUT_SUCCESS;

                    return CloseOutType.CLOSEOUT_FAILED;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception zipping spectrum file, job {0}, step {1}: {2}", mJob, mStepNum, ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Occasionally the zip file is corrupted and will need to be zipped using ICSharpCode.SharpZipLib instead
            // If the file exists and is not zero bytes in length, try zipping again, but instead use ICSharpCode.SharpZipLib

            var zipFile = new FileInfo(GetZipFilePathForFile(inputFilePath));

            if (!zipFile.Exists || zipFile.Length <= 0)
            {
                LogError("Error zipping spectrum file, job {0}, step {1}", mJob, mStepNum);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            try
            {
#pragma warning disable CS0618
                if (ZipFileSharpZipLib(inputFilePath))
#pragma warning restore CS0618
                {
                    var warningMsg = string.Format("Zip file created using ZipFileTools was corrupted; successfully compressed it using SharpZipLib instead: {0}", inputFileName);
                    LogWarning(warningMsg);

                    if (RenameZipFileIfRequired(mWorkDir, mDatasetName + ".zip", finalZipFileName))
                        return CloseOutType.CLOSEOUT_SUCCESS;

                    return CloseOutType.CLOSEOUT_FAILED;
                }

                LogError("Error zipping spectrum file using SharpZipLib, job {0}, step {1}", mJob, mStepNum);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                LogError("Exception zipping spectrum file using SharpZipLib, job {0}, step {1}: {2}", mJob, mStepNum, ex.Message);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }
    }
}
