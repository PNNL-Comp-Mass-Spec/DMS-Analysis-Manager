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
using System.Threading;
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

        public const string CDTA_FILE_SUFFIX = "_dta.txt";

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

        private bool m_CentroidDTAs;
        private bool m_ConcatenateDTAs;
        private int m_StepNum;

        #endregion

        #region "Methods"

        /// <summary>
        /// Runs the analysis tool
        /// </summary>
        /// <returns>CloseoutType enum indicating success or failure</returns>
        /// <remarks>This method is used to meet the interface requirement</remarks>
        public override CloseOutType RunTool()
        {
            // Do the stuff in the base class
            if (!(base.RunTool() == CloseOutType.CLOSEOUT_SUCCESS))
                return CloseOutType.CLOSEOUT_FAILED;

            m_StepNum = m_jobParams.GetJobParameter("Step", 0);

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
            m_StopTime = DateTime.UtcNow;

            // Add the current job data to the summary file
            UpdateSummaryFile();

            // Get rid of raw data file
            result = DeleteDataFile();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            // Add all the extensions of the files to delete after run
            m_jobParams.AddResultFileExtensionToSkip(CDTA_FILE_SUFFIX);    // Unzipped, concatenated DTA
            m_jobParams.AddResultFileExtensionToSkip(".dta");              // DTA files
            m_jobParams.AddResultFileExtensionToSkip("DeconMSn_progress.txt");

            // Add any files that are an exception to the captured files to delete list
            m_jobParams.AddResultFileToKeep("lcq_dta.txt");

            var success = CopyResultsToTransferDirectory();

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;


        }

        /// <summary>
        /// Creates DTA files and filters if necessary
        /// </summary>
        /// <returns>CloseoutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType CreateMSMSSpectra()
        {
            //Make the spectra files
            var Result = MakeSpectraFiles();
            if (Result != CloseOutType.CLOSEOUT_SUCCESS)
                return Result;

            //Concatenate spectra files
            if (m_ConcatenateDTAs)
            {
                Result = ConcatSpectraFiles();
                if (Result != CloseOutType.CLOSEOUT_SUCCESS)
                    return Result;
            }

            if (m_CentroidDTAs)
            {
                Result = CentroidCDTA();
                if (Result != CloseOutType.CLOSEOUT_SUCCESS)
                    return Result;
            }

            //Zip concatenated spectra files
            Result = ZipConcDtaFile();
            if (Result != CloseOutType.CLOSEOUT_SUCCESS)
                return Result;

            //If we got to here, everything's OK
            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private eDTAGeneratorConstants GetDTAGenerator(out clsDtaGen spectraGen)
        {
            string strErrorMessage = string.Empty;

            var eDtaGeneratorType = GetDTAGeneratorInfo(m_jobParams, out m_ConcatenateDTAs, out strErrorMessage);
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
            bool blnConcatenateDTAs = false;
            return GetDTAGeneratorInfo(oJobParams, out blnConcatenateDTAs, out strErrorMessage);
        }

        public static eDTAGeneratorConstants GetDTAGeneratorInfo(IJobParams oJobParams, out bool blnConcatenateDTAs, out string strErrorMessage)
        {
            string strDTAGenerator = oJobParams.GetJobParameter("DtaGenerator", "");
            string strRawDataType = oJobParams.GetJobParameter("RawDataType", "");
            bool blnMGFInstrumentData = oJobParams.GetJobParameter("MGFInstrumentData", false);

            strErrorMessage = string.Empty;
            blnConcatenateDTAs = true;

            clsAnalysisResources.eRawDataTypeConstants eRawDataType;

            if (string.IsNullOrEmpty(strRawDataType))
            {
                strErrorMessage = NotifyMissingParameter(oJobParams, "RawDataType");
                return eDTAGeneratorConstants.Unknown;
            }
            else
            {
                eRawDataType = clsAnalysisResources.GetRawDataType(strRawDataType);
            }

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
        /// <returns>CloseoutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        [Obsolete("This method is unused")]
        public CloseOutType DispositionResults()
        {
            //Make sure all files have released locks
            PRISM.clsProgRunner.GarbageCollectNow();
            Thread.Sleep(1000);

            //Get rid of raw data file
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

            //Add the current job data to the summary file
            UpdateSummaryFile();

            //Delete .dta files
            try
            {
                var dtaFiles = Directory.GetFiles(m_WorkDir, "*.dta");
                foreach (string TmpFile in dtaFiles)
                {
                    DeleteFileWithRetries(TmpFile);
                }
            }
            catch (Exception ex)
            {
                LogError("Error deleting .dta files", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            //Delete unzipped concatenated dta files
            var cdtaFiles = Directory.GetFiles(m_WorkDir, "*" + CDTA_FILE_SUFFIX);
            foreach (string TmpFile in cdtaFiles)
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
            SpectraFileProcessorParams initParams = new SpectraFileProcessorParams
            {
                DebugLevel = m_DebugLevel,
                JobParams = m_jobParams,
                MgrParams = m_mgrParams,
                StatusTools = m_StatusTools,
                WorkDir = m_WorkDir,
                DatasetName = m_Dataset
            };

            return initParams;
        }

        /// <summary>
        /// Creates DTA files
        /// </summary>
        /// <returns>CloseoutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType MakeSpectraFiles()
        {
            //Make individual spectra files from input raw data file, using plugin

            LogMessage("Making spectra files, job " + m_JobNum + ", step " + m_StepNum);

            clsDtaGen SpectraGen = null;

            var eDtaGeneratorType = GetDTAGenerator(out SpectraGen);

            if (eDtaGeneratorType == eDTAGeneratorConstants.Unknown)
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (eDtaGeneratorType == eDTAGeneratorConstants.DeconConsole)
            {
                m_CentroidDTAs = false;
            }
            else
            {
                m_CentroidDTAs = m_jobParams.GetJobParameter("CentroidDTAs", false);
            }

            RegisterEvents(SpectraGen);

            // Initialize the plugin

            try
            {
                SpectraGen.Setup(GetDtaGenInitParams(), this);
            }
            catch (Exception ex)
            {
                LogError("Exception configuring DTAGenerator", ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Store the Version info in the database
            bool blnSuccess = false;
            if (eDtaGeneratorType == eDTAGeneratorConstants.MGFtoDTA)
            {
                // MGFtoDTA Dll
                blnSuccess = StoreToolVersionInfoDLL(SpectraGen.DtaToolNameLoc);
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
                    else
                    {
                        SpectraGen.UpdateDtaToolNameLoc(progLoc);
                    }
                }

                blnSuccess = StoreToolVersionInfo(SpectraGen.DtaToolNameLoc, eDtaGeneratorType);
            }

            if (!blnSuccess)
            {
                LogError("Aborting since StoreToolVersionInfo returned false for " + SpectraGen.DtaToolNameLoc);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (eDtaGeneratorType == eDTAGeneratorConstants.DeconMSn && m_CentroidDTAs)
            {
                var blnUsingExistingResults = m_jobParams.GetJobParameter(clsDtaGenResources.USING_EXISTING_DECONMSN_RESULTS, false);

                if (blnUsingExistingResults)
                {
                    // Confirm that the existing DeconMSn results are valid
                    // If they are, then we don't need to re-run DeconMSn

                    if (ValidateDeconMSnResults())
                    {
                        m_progress = 100;
                        return CloseOutType.CLOSEOUT_SUCCESS;
                    }
                }
            }

            try
            {
                // Start the spectra generation process
                var eResult = StartAndWaitForDTAGenerator(SpectraGen, "MakeSpectraFiles", false);

                // Set internal spectra file count to that returned by the spectra generator
                m_DtaCount = SpectraGen.SpectraFileCount;
                m_progress = SpectraGen.Progress;

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
            string strCDTAFileOriginal = null;
            string strCDTAFileCentroided = null;
            string strCDTAFileFinal = null;

            try
            {
                // Rename the _DTA.txt file to _DTA_Original.txt
                var fiCDTA = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + CDTA_FILE_SUFFIX));
                if (!fiCDTA.Exists)
                {
                    LogError("File not found in CentroidCDTA: " + fiCDTA.Name);
                    return CloseOutType.CLOSEOUT_NO_DTA_FILES;
                }

                PRISM.clsProgRunner.GarbageCollectNow();
                Thread.Sleep(50);

                strCDTAFileOriginal = Path.Combine(m_WorkDir, m_Dataset + "_DTA_Original.txt");
                fiCDTA.MoveTo(strCDTAFileOriginal);

                m_jobParams.AddResultFileToSkip(fiCDTA.Name);
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

                var fiCDTA = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + CDTA_FILE_SUFFIX));
                if (!fiCDTA.Exists)
                {
                    LogError("File not found in CentroidCDTA (after calling clsDtaGenMSConvert): " + fiCDTA.Name);
                    return CloseOutType.CLOSEOUT_NO_DTA_FILES;
                }

                PRISM.clsProgRunner.GarbageCollectNow();
                Thread.Sleep(50);

                strCDTAFileCentroided = Path.Combine(m_WorkDir, m_Dataset + "_DTA_Centroided.txt");
                fiCDTA.MoveTo(strCDTAFileCentroided);

                m_jobParams.AddResultFileToSkip(fiCDTA.Name);
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

                bool blnSuccess = false;
                strCDTAFileFinal = Path.Combine(m_WorkDir, m_Dataset + CDTA_FILE_SUFFIX);

                blnSuccess = MergeCDTAs(strCDTAFileOriginal, strCDTAFileCentroided, strCDTAFileFinal);
                if (!blnSuccess)
                {
                    if (string.IsNullOrEmpty(m_message))
                        m_message = "MergeCDTAs returned False in CentroidCDTA";
                    LogError(m_message);
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
        /// <returns>CloseoutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType ConcatSpectraFiles()
        {
            // Packages dta files into concatenated text files

            // Make sure at least one .dta file was created
            var diWorkDir = new DirectoryInfo(m_WorkDir);
            int intDTACount = diWorkDir.GetFiles("*.dta").Length;

            if (intDTACount == 0)
            {
                LogError("No .DTA files were created");
                return CloseOutType.CLOSEOUT_NO_DTA_FILES;
            }
            else if (m_DebugLevel >= 1)
            {
                LogMessage("Concatenating spectra files, job " + m_JobNum + ", step " + m_StepNum);
            }

            clsConcatToolWrapper ConcatTools = new clsConcatToolWrapper(diWorkDir.FullName);

            if (!ConcatTools.ConcatenateFiles(clsConcatToolWrapper.ConcatFileTypes.CONCAT_DTA, m_Dataset))
            {
                LogError("Error packaging results: " + ConcatTools.ErrMsg);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            else
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
        }

        public override void CopyFailedResultsToArchiveFolder()
        {
            m_jobParams.AddResultFileToSkip(Dataset + "_dta.zip");
            m_jobParams.AddResultFileToSkip(Dataset + "_dta.txt");

            // Skip any .dta files
            m_jobParams.AddResultFileExtensionToSkip(".dta");

            base.CopyFailedResultsToArchiveFolder();
        }

        private string GetMSConvertAppPath()
        {
            string ProteoWizardDir = m_mgrParams.GetParam("ProteoWizardDir");         // MSConvert.exe is stored in the ProteoWizard folder
            string progLoc = Path.Combine(ProteoWizardDir, clsDtaGenMSConvert.MSCONVERT_FILENAME);

            return progLoc;
        }

        /// <summary>
        /// Deletes .raw files from working directory
        /// </summary>
        /// <returns>CloseoutType enum indicating success or failure</returns>
        /// <remarks>Overridden for other types of input files</remarks>
        private CloseOutType DeleteDataFile()
        {
            //Deletes the .raw file from the working directory

            if (m_DebugLevel >= 2)
            {
                LogMessage("clsDtaGenToolRunner.DeleteDataFile, executing method");
            }

            //Delete the .raw file
            try
            {
                var lstFilesToDelete = new List<string>();
                lstFilesToDelete.AddRange(Directory.GetFiles(m_WorkDir, "*" + clsAnalysisResources.DOT_RAW_EXTENSION));
                lstFilesToDelete.AddRange(Directory.GetFiles(m_WorkDir, "*" + clsAnalysisResources.DOT_MZXML_EXTENSION));
                lstFilesToDelete.AddRange(Directory.GetFiles(m_WorkDir, "*" + clsAnalysisResources.DOT_MZML_EXTENSION));
                lstFilesToDelete.AddRange(Directory.GetFiles(m_WorkDir, "*" + clsAnalysisResources.DOT_MGF_EXTENSION));

                foreach (string MyFile in lstFilesToDelete)
                {
                    if (m_DebugLevel >= 2)
                    {
                        LogMessage("clsDtaGenToolRunner.DeleteDataFile, deleting file " + MyFile);
                    }
                    DeleteFileWithRetries(MyFile);
                }
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                LogError("Error deleting .raw file, job " + m_JobNum + ", step " + m_StepNum, ex);
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
                    List<string> msMsDataListCentroid = null;

                    clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType udtFragIonDataHeaderCentroid;
                    var blnNextSpectrumAvailable = oCDTAReaderFragIonData.ReadNextSpectrum(out msMsDataListCentroid, out udtFragIonDataHeaderCentroid);
                    if (!blnNextSpectrumAvailable)
                    {
                        break;
                    }

                    var scanStart = udtFragIonDataHeaderCentroid.ScanNumberStart;
                    var scanEnd = udtFragIonDataHeaderCentroid.ScanNumberEnd;

                    SortedSet<int> endScanList = null;
                    if (fragIonDataScanStatus.TryGetValue(scanStart, out endScanList))
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

                Thread.Sleep(10);

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
                    List<string> msMsDataList = null;
                    clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType udtParentIonDataHeader;

                    while (oCDTAReaderParentIons.ReadNextSpectrum(out msMsDataList, out udtParentIonDataHeader))
                    {
                        if (!ScanMatchIsPossible(udtParentIonDataHeader, fragIonDataScanStatus))
                        {
                            LogWarning("MergeCDTAs could not find spectrum with StartScan=" + udtParentIonDataHeader.ScanNumberStart + " and EndScan=" +
                                       udtParentIonDataHeader.ScanNumberEnd + " for " + Path.GetFileName(strCDTAWithParentIonData));
                            intSpectrumCountSkipped += 1;
                            continue;
                        }

                        List<string> msMsDataListCentroid = null;
                        bool blnNextSpectrumAvailable = false;

                        while (!ScanHeadersMatch(udtParentIonDataHeader, udtFragIonDataHeader))
                        {
                            blnNextSpectrumAvailable = oCDTAReaderFragIonData.ReadNextSpectrum(out msMsDataListCentroid, out udtFragIonDataHeader);
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

                            Thread.Sleep(10);

                            oCDTAReaderFragIonData = new clsDtaTextFileReader(true);
                            if (!oCDTAReaderFragIonData.OpenFile(strCDTAWithFragIonData))
                            {
                                LogError("Error re-opening CDTA file with the fragment ion data (when blnNextSpectrumAvailable = False)");
                                return false;
                            }

                            while (!ScanHeadersMatch(udtParentIonDataHeader, udtFragIonDataHeader))
                            {
                                blnNextSpectrumAvailable = oCDTAReaderFragIonData.ReadNextSpectrum(out msMsDataListCentroid, out udtFragIonDataHeader);
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
                            else
                            {
                                swCDTAOut.Write(strDataLinesToAppend);
                            }
                        }

                        if (DateTime.UtcNow.Subtract(dtLastStatus).TotalSeconds >= 30)
                        {
                            dtLastStatus = DateTime.UtcNow;
                            if (m_DebugLevel >= 1)
                            {
                                LogMessage("Merging CDTAs, scan " + udtParentIonDataHeader.ScanNumberStart.ToString());
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
                    m_EvalMessage = "Skipped " + intSpectrumCountSkipped + " spectra in MergeCDTAs since they were not created by MSConvert";
                    LogWarning(m_EvalMessage);
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

                    if (strLine.StartsWith("="))
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
            Dictionary<int, SortedSet<int>> fragIonDataScanStatus)
        {
            SortedSet<int> endScanList = null;
            if (fragIonDataScanStatus.TryGetValue(udtParentIonDataHeader.ScanNumberStart, out endScanList))
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
                else
                {
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
            }

            return false;
        }

        private CloseOutType StartAndWaitForDTAGenerator(clsDtaGen oDTAGenerator, string strCallingFunction, bool blnSecondPass)
        {
            ProcessStatus retVal = oDTAGenerator.Start();
            if (retVal == ProcessStatus.SF_ERROR)
            {
                LogError("Error starting spectra processor: " + oDTAGenerator.ErrMsg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (m_DebugLevel > 0)
            {
                LogMessage("clsDtaGenToolRunner." + strCallingFunction + ": Spectra generation started");
            }

            // Loop until the spectra generator finishes
            while ((oDTAGenerator.Status == ProcessStatus.SF_STARTING) | (oDTAGenerator.Status == ProcessStatus.SF_RUNNING))
            {
                if (blnSecondPass)
                {
                    m_progress = CENTROID_CDTA_PROGRESS_START + oDTAGenerator.Progress * (100f - CENTROID_CDTA_PROGRESS_START) / 100f;
                }
                else
                {
                    if (m_CentroidDTAs)
                    {
                        m_progress = oDTAGenerator.Progress * (CENTROID_CDTA_PROGRESS_START / 100f);
                    }
                    else
                    {
                        m_progress = oDTAGenerator.Progress;
                    }
                }

                UpdateStatusRunning(m_progress, oDTAGenerator.SpectraFileCount);

                Thread.Sleep(5000);                 //Delay for 5 seconds
            }

            UpdateStatusRunning(m_progress, oDTAGenerator.SpectraFileCount);

            //Check for reason spectra generator exited
            if (oDTAGenerator.Results == ProcessResults.SF_FAILURE)
            {
                LogError("Error making DTA files in " + strCallingFunction + ": " + oDTAGenerator.ErrMsg);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            else if (oDTAGenerator.Results == ProcessResults.SF_ABORTED)
            {
                LogError("DTA generation aborted in " + strCallingFunction + "");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (oDTAGenerator.Results == ProcessResults.SF_NO_FILES_CREATED)
            {
                LogError("No spectra files created in " + strCallingFunction);
                return CloseOutType.CLOSEOUT_NO_DTA_FILES;
            }
            else
            {
                if (m_DebugLevel >= 2)
                {
                    LogMessage("clsDtaGenToolRunner." + strCallingFunction + ": Spectra generation completed");
                }
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Stores the tool version info in the database
        /// </summary>
        /// <remarks></remarks>
        private bool StoreToolVersionInfo(string strDtaGeneratorAppPath, eDTAGeneratorConstants eDtaGenerator)
        {
            string strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
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
                    return base.SetStepTaskToolVersion(strToolVersionInfo, new List<FileInfo>(), blnSaveToolVersionTextFile: false);
                }
                catch (Exception ex)
                {
                    LogError("Exception calling SetStepTaskToolVersion", ex);
                    return false;
                }
            }

            // Store strDtaGeneratorAppPath in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(fiDtaGenerator);

            if (eDtaGenerator == eDTAGeneratorConstants.DeconConsole || eDtaGenerator == eDTAGeneratorConstants.DeconMSn)
            {
                // Lookup the version of the DeconConsole or DeconMSn application
                string strDllPath = null;

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
                        ioToolFiles.Add(new FileInfo(strDllPath));
                        blnSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strDllPath);
                    }
                    else
                    {
                        strDllPath = Path.Combine(fiDtaGenerator.DirectoryName, "DeconMSnEngine.dll");
                        ioToolFiles.Add(new FileInfo(strDllPath));
                        blnSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strDllPath);
                    }

                    if (!blnSuccess)
                        return false;
                }
                else if (eDtaGenerator == eDTAGeneratorConstants.DeconConsole)
                {
                    // DeconConsole re-implementation of DeconMSn (obsolete, superseded by C# version of DeconMSn that uses DeconEngineV2.dll)

                    // Lookup the version of the DeconTools Backend (in the DeconTools folder)
                    // In addition, add it to ioToolFiles
                    strDllPath = Path.Combine(fiDtaGenerator.DirectoryName, "DeconTools.Backend.dll");
                    ioToolFiles.Add(new FileInfo(strDllPath));
                    blnSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strDllPath);
                    if (!blnSuccess)
                        return false;

                    // Lookup the version of DeconEngineV2 (in the DeconTools folder)
                    strDllPath = Path.Combine(fiDtaGenerator.DirectoryName, "DeconEngineV2.dll");
                    blnSuccess = base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strDllPath);
                    if (!blnSuccess)
                        return false;
                }
            }

            // Possibly also store the MSConvert version
            if (m_CentroidDTAs)
            {
                ioToolFiles.Add(new FileInfo(GetMSConvertAppPath()));
            }

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        private bool StoreToolVersionInfoDLL(string strDtaGeneratorDLLPath)
        {
            string strToolVersionInfo = string.Empty;

            if (m_DebugLevel >= 2)
            {
                LogMessage("Determining tool version info");
            }

            // Lookup the version of the DLL
            base.StoreToolVersionInfoOneFile(ref strToolVersionInfo, strDtaGeneratorDLLPath);

            // Store paths to key files in ioToolFiles
            List<FileInfo> ioToolFiles = new List<FileInfo>();
            ioToolFiles.Add(new FileInfo(strDtaGeneratorDLLPath));

            // Possibly also store the MSConvert version
            if (m_CentroidDTAs)
            {
                ioToolFiles.Add(new FileInfo(GetMSConvertAppPath()));
            }

            try
            {
                return base.SetStepTaskToolVersion(strToolVersionInfo, ioToolFiles, blnSaveToolVersionTextFile: false);
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
                var fiDeconMSnLogFile = new FileInfo(Path.Combine(m_WorkDir, m_Dataset + "_DeconMSn_log.txt"));

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
        /// <returns>CloseoutType enum indicating success or failure</returns>
        /// <remarks></remarks>
        private CloseOutType ZipConcDtaFile()
        {
            string DtaFileName = m_Dataset + "_dta.txt";
            string DtaFilePath = Path.Combine(m_WorkDir, DtaFileName);

            LogMessage("Zipping concatenated spectra file, job " + m_JobNum + ", step " + m_StepNum);

            // Verify the _dta.txt file exists
            if (!File.Exists(DtaFilePath))
            {
                LogWarning("Error: Unable to find concatenated dta file");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Zip the file using IonicZip
            try
            {
                if (base.ZipFile(DtaFilePath, false))
                {
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }
            }
            catch (Exception ex)
            {
                string msg = "Exception zipping concat dta file, job " + m_JobNum + ", step " + m_StepNum + ": " + ex.Message;
                LogError(msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Occasionally the zip file is corrupted and will need to be zipped using ICSharpCode.SharpZipLib instead
            // If the file exists and is not zero bytes in length, try zipping again, but instead use ICSharpCode.SharpZipLib

            var fiZipFile = new FileInfo(base.GetZipFilePathForFile(DtaFilePath));
            if (!fiZipFile.Exists || fiZipFile.Length <= 0)
            {
                string msg = "Error zipping concat dta file, job " + m_JobNum + ", step " + m_StepNum;
                LogError(msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            try
            {
                Thread.Sleep(250);

                if (base.ZipFileSharpZipLib(DtaFilePath))
                {
                    var warningMsg = string.Format("Zip file created using IonicZip was corrupted; successfully compressed it using SharpZipLib instead: {0}", DtaFileName);
                    LogWarning(warningMsg);
                    return CloseOutType.CLOSEOUT_SUCCESS;
                }

                string msg = "Error zipping concat dta file using SharpZipLib, job " + m_JobNum + ", step " + m_StepNum;
                LogError(msg);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            catch (Exception ex)
            {
                string msg = "Exception zipping concat dta file using SharpZipLib, job " + m_JobNum + ", step " + m_StepNum;
                LogError(msg, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        #endregion
    }
}
