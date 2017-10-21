//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 07/20/2010
//
// This class reads a tab-delimited text file (created by the Peptide File Extractor or by PHRP)
// and creates a tab-delimited text file suitable for processing by MSGF
//
// The class must be derived by a sub-class customized for the specific analysis tool (Sequest, X!Tandem, Inspect, etc.)
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;

using System.IO;
using MsMsDataFileReader;
using PHRPReader;
using PRISM;

namespace AnalysisManagerMSGFPlugin
{
    public abstract class clsMSGFInputCreator : clsEventNotifier
    {
        #region "Constants"

        private const string MSGF_INPUT_FILENAME_SUFFIX = "_MSGF_input.txt";
        public const string MSGF_RESULT_FILENAME_SUFFIX = "_MSGF.txt";

        #endregion

        #region "Module variables"

        protected readonly string mDatasetName;
        protected readonly string mWorkDir;

        private readonly clsPHRPReader.ePeptideHitResultType mPeptideHitResultType;

        private readonly SortedDictionary<int, List<string>> mSkippedLineInfo;
        // This dictionary is initially populated with a string constructed using
        // Scan plus "_" plus charge plus "_" plus the original peptide sequence in the PHRP file
        // It will contain an entry for every line written to the MSGF input file
        // It is later updated by AddUpdateMSGFResult() to store the properly formated MSGF result line for each entry
        // Finally, it will be used by CreateMSGFFirstHitsFile to create the MSGF file that corresponds to the first-hits file

        private readonly SortedDictionary<string, string> mMSGFCachedResults;
        // This dictionary holds a mapping between Scan plus "_" plus charge to the spectrum index in the MGF file (first spectrum has index=1)
        // It is only used if MGFInstrumentData=True

        private SortedDictionary<string, int> mScanAndChargeToMGFIndex;
        // This dictionary is the inverse of mScanAndChargeToMGFIndex
        // mMGFIndexToScan allows for a lookup of Scan Number given the MGF index
        // It is only used if MGFInstrumentData=True

        private SortedDictionary<int, int> mMGFIndexToScan;

        protected string mErrorMessage;
        protected string mPHRPFirstHitsFilePath = string.Empty;

        protected string mPHRPSynopsisFilePath = string.Empty;
        private string mMSGFInputFilePath = string.Empty;

        private string mMSGFResultsFilePath = string.Empty;

        private int mMSGFInputFileLineCount;

        private StreamWriter mLogFile;

        #endregion

        #region "Properties"

        public bool DoNotFilterPeptides { get; set; }

        public string ErrorMessage => mErrorMessage;

        public bool MgfInstrumentData { get; set; }

        public int MSGFInputFileLineCount => mMSGFInputFileLineCount;

        public string MSGFInputFilePath => mMSGFInputFilePath;

        public string MSGFResultsFilePath => mMSGFResultsFilePath;

        public string PHRPFirstHitsFilePath => mPHRPFirstHitsFilePath;

        public string PHRPSynopsisFilePath => mPHRPSynopsisFilePath;

        #endregion

        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="datasetName">Dataset Name</param>
        /// <param name="workDir">Working directory</param>
        /// <param name="eResultType">PeptideHit result type</param>
        /// <remarks></remarks>
        protected clsMSGFInputCreator(string datasetName, string workDir, clsPHRPReader.ePeptideHitResultType eResultType)
        {
            mDatasetName = datasetName;
            mWorkDir = workDir;
            mPeptideHitResultType = eResultType;

            mErrorMessage = string.Empty;

            mSkippedLineInfo = new SortedDictionary<int, List<string>>();

            mMSGFCachedResults = new SortedDictionary<string, string>();
        }

        #region "Functions to be defined in derived classes"

        protected abstract void InitializeFilePaths();
        protected abstract bool PassesFilters(clsPSM objPSM);

        #endregion

        public void AddUpdateMSGFResult(string scanNumber, string charge, string peptide, string msgfResultData)
        {
            try
            {
                mMSGFCachedResults[ConstructMSGFResultCode(scanNumber, charge, peptide)] = msgfResultData;
            }
            catch (Exception)
            {
                // Entry not found; this is unexpected; we will only report the error at the console
                LogError("Entry not found in mMSGFCachedResults for " + ConstructMSGFResultCode(scanNumber, charge, peptide));
            }
        }

        private string AppendText(string text, string addnl, string delimiter = ": ")
        {
            if (string.IsNullOrWhiteSpace(addnl))
            {
                return text;
            }
            return text + delimiter + addnl;
        }

        public void CloseLogFileNow()
        {
            if (mLogFile != null)
            {
                mLogFile.Close();
                mLogFile = null;

                clsProgRunner.GarbageCollectNow();
                System.Threading.Thread.Sleep(100);
            }
        }

        protected string CombineIfValidFile(string folder, string file)
        {
            if (!string.IsNullOrWhiteSpace(file))
            {
                return Path.Combine(folder, file);
            }
            return string.Empty;
        }

        private string ConstructMGFMappingCode(int scanNumber, int charge)
        {
            return scanNumber + "_" + charge;
        }

        private string ConstructMSGFResultCode(int scanNumber, int charge, string peptide)
        {
            return scanNumber + "_" + charge + "_" + peptide;
        }

        private string ConstructMSGFResultCode(string scanNumber, string charge, string peptide)
        {
            return scanNumber + "_" + charge + "_" + peptide;
        }

        private bool CreateMGFScanToIndexMap(string mgfFilePath)
        {
            var spectrumIndex = 0;

            try
            {
                var objMGFReader = new clsMGFReader();

                if (!objMGFReader.OpenFile(mgfFilePath))
                {
                    ReportError("Error opening the .MGF file");
                    return false;
                }

                mScanAndChargeToMGFIndex = new SortedDictionary<string, int>();
                mMGFIndexToScan = new SortedDictionary<int, int>();

                while (true)
                {

                    var spectrumFound = objMGFReader.ReadNextSpectrum(out _, out var udtSpectrumHeaderInfo);
                    if (!spectrumFound)
                        break;

                    spectrumIndex += 1;

                    if (udtSpectrumHeaderInfo.ParentIonChargeCount == 0)
                    {
                        var scanAndCharge = ConstructMGFMappingCode(udtSpectrumHeaderInfo.ScanNumberStart, 0);
                        mScanAndChargeToMGFIndex.Add(scanAndCharge, spectrumIndex);
                    }
                    else
                    {
                        for (var chargeIndex = 0; chargeIndex <= udtSpectrumHeaderInfo.ParentIonChargeCount - 1; chargeIndex++)
                        {
                            var scanAndCharge = ConstructMGFMappingCode(udtSpectrumHeaderInfo.ScanNumberStart, udtSpectrumHeaderInfo.ParentIonCharges[chargeIndex]);
                            mScanAndChargeToMGFIndex.Add(scanAndCharge, spectrumIndex);
                        }
                    }

                    mMGFIndexToScan.Add(spectrumIndex, udtSpectrumHeaderInfo.ScanNumberStart);
                }
            }
            catch (Exception ex)
            {
                ReportError("Error indexing the MGF file: " + ex.Message);
                return false;
            }

            if (spectrumIndex > 0)
            {
                return true;
            }

            ReportError("No spectra were found in the MGF file");
            return false;

        }

        /// <summary>
        /// Read the first-hits file and create a new, parallel file with the MSGF results
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool CreateMSGFFirstHitsFile()
        {
            const int MAX_WARNINGS_TO_REPORT = 10;

            try
            {
                if (string.IsNullOrEmpty(mPHRPFirstHitsFilePath))
                {
                    // This result type does not have a first-hits file
                    return true;
                }

                var startupOptions = GetMinimalMemoryPHRPStartupOptions();
                startupOptions.LoadModsAndSeqInfo = true;

                // Open the first-hits file
                using (var reader = new clsPHRPReader(mPHRPFirstHitsFilePath, mPeptideHitResultType, startupOptions))
                {
                    RegisterEvents(reader);

                    reader.EchoMessagesToConsole = true;

                    if (!reader.CanRead)
                    {
                        ReportError(AppendText("Aborting since PHRPReader is not ready", mErrorMessage));
                        return false;
                    }

                    // Define the path to write the first-hits MSGF results to
                    var msgfFirstHitsResults = Path.GetFileNameWithoutExtension(mPHRPFirstHitsFilePath) + MSGF_RESULT_FILENAME_SUFFIX;
                    msgfFirstHitsResults = Path.Combine(mWorkDir, msgfFirstHitsResults);

                    // Create the output file
                    using (var msgfFHTFile = new StreamWriter(new FileStream(msgfFirstHitsResults, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        // Write out the headers to msgfFHTFile
                        WriteMSGFResultsHeaders(msgfFHTFile);

                        var missingValueCount = 0;

                        while (reader.MoveNext())
                        {
                            var objPSM = reader.CurrentPSM;

                            var peptideResultCode = ConstructMSGFResultCode(objPSM.ScanNumber, objPSM.Charge, objPSM.Peptide);

                            if (mMSGFCachedResults.TryGetValue(peptideResultCode, out var msgfResultData))
                            {
                                if (string.IsNullOrEmpty(msgfResultData))
                                {
                                    // Match text is empty
                                    // We should not write thie out to disk since it would result in empty columns

                                    var warningMessage = "MSGF Results are empty for result code '" + peptideResultCode +
                                                            "'; this is unexpected";
                                    missingValueCount += 1;
                                    if (missingValueCount <= MAX_WARNINGS_TO_REPORT)
                                    {
                                        if (missingValueCount == MAX_WARNINGS_TO_REPORT)
                                        {
                                            warningMessage += "; additional invalid entries will not be reported";
                                        }
                                        ReportWarning(warningMessage);
                                    }
                                    else
                                    {
                                        LogError(warningMessage);
                                    }
                                }
                                else
                                {
                                    // Match found; write out the result
                                    msgfFHTFile.WriteLine(objPSM.ResultID + "\t" + msgfResultData);
                                }
                            }
                            else
                            {
                                // Match not found; this is unexpected

                                var warningMessage = "Match not found for first-hits entry with result code '" +
                                                        peptideResultCode + "'; this is unexpected";

                                // Report the first 10 times this happens
                                missingValueCount += 1;
                                if (missingValueCount <= MAX_WARNINGS_TO_REPORT)
                                {
                                    if (missingValueCount == MAX_WARNINGS_TO_REPORT)
                                    {
                                        warningMessage += "; additional missing entries will not be reported";
                                    }
                                    ReportWarning(warningMessage);
                                }
                                else
                                {
                                    LogError(warningMessage);
                                }
                            }
                        }
                    }

                }

            }
            catch (Exception ex)
            {
                ReportError("Error creating the MSGF first hits file: " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Creates the input file for MSGF
        /// Will contain filter passing peptides from the synopsis file, plus all peptides
        /// in the first-hits file that are not filter passing in the synopsis file
        /// If the synopsis file does not exist, simply processes the first-hits file
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool CreateMSGFInputFileUsingPHRPResultFiles()
        {
            var success = false;

            try
            {
                if (string.IsNullOrEmpty(mDatasetName))
                {
                    ReportError("Dataset name is undefined; unable to continue");
                    return false;
                }

                if (string.IsNullOrEmpty(mWorkDir))
                {
                    ReportError("Working directory is undefined; unable to continue");
                    return false;
                }

                string spectrumFileName;
                if (MgfInstrumentData)
                {
                    spectrumFileName = mDatasetName + ".mgf";

                    // Need to read the .mgf file and create a mapping between the actual scan number and the 1-based index of the data in the .mgf file
                    success = CreateMGFScanToIndexMap(Path.Combine(mWorkDir, spectrumFileName));
                    if (!success)
                    {
                        return false;
                    }
                }
                else
                {
                    // mzXML filename is dataset plus .mzXML
                    // Note that the jrap reader used by MSGF may fail if the .mzXML filename is capitalized differently than this (i.e., it cannot be .mzxml)
                    spectrumFileName = mDatasetName + ".mzXML";
                }

                // Create the MSGF Input file that we will write data to
                using (var swMSGFInputFile = new StreamWriter(new FileStream(mMSGFInputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Write out the headers:  #SpectrumFile  Title  Scan#  Annotation  Charge  Protein_First  Result_ID  Data_Source  Collision_Mode
                    // Note that we're storing the original peptide sequence in the "Title" column, while the marked up sequence (with mod masses) goes in the "Annotation" column
                    swMSGFInputFile.WriteLine(
                        clsMSGFRunner.MSGF_RESULT_COLUMN_SpectrumFile + "\t" +
                        clsMSGFRunner.MSGF_RESULT_COLUMN_Title + "\t" +
                        clsMSGFRunner.MSGF_RESULT_COLUMN_ScanNumber + "\t" +
                        clsMSGFRunner.MSGF_RESULT_COLUMN_Annotation + "\t" +
                        clsMSGFRunner.MSGF_RESULT_COLUMN_Charge + "\t" +
                        clsMSGFRunner.MSGF_RESULT_COLUMN_Protein_First + "\t" +
                        clsMSGFRunner.MSGF_RESULT_COLUMN_Result_ID + "\t" +
                        clsMSGFRunner.MSGF_RESULT_COLUMN_Data_Source + "\t" +
                        clsMSGFRunner.MSGF_RESULT_COLUMN_Collision_Mode);

                    // Initialize some tracking variables
                    mMSGFInputFileLineCount = 1;

                    mSkippedLineInfo.Clear();

                    mMSGFCachedResults.Clear();

                    if (!string.IsNullOrEmpty(mPHRPSynopsisFilePath) && File.Exists(mPHRPSynopsisFilePath))
                    {
                        var startupOptions = GetMinimalMemoryPHRPStartupOptions();
                        startupOptions.LoadModsAndSeqInfo = true;

                        // Read the synopsis file data
                        var reader = new clsPHRPReader(mPHRPSynopsisFilePath, mPeptideHitResultType, startupOptions);
                        RegisterEvents(reader);

                        reader.EchoMessagesToConsole = true;

                        // Report any errors cached during instantiation of the PHRPReader
                        foreach (var message in reader.ErrorMessages)
                        {
                            ReportError(message);
                        }
                        mErrorMessage = string.Empty;

                        // Report any warnings cached during instantiation of the PHRPReader
                        foreach (var message in reader.WarningMessages)
                        {
                            ReportWarning(message);
                        }

                        reader.ClearErrors();
                        reader.ClearWarnings();

                        if (!reader.CanRead)
                        {
                            ReportError(AppendText("Aborting since PHRPReader is not ready", mErrorMessage));
                            return false;
                        }

                        ReadAndStorePHRPData(reader, swMSGFInputFile, spectrumFileName, true);
                        reader.Dispose();

                        success = true;
                    }

                    if (!string.IsNullOrEmpty(mPHRPFirstHitsFilePath) && File.Exists(mPHRPFirstHitsFilePath))
                    {
                        // Now read the first-hits file data

                        var startupOptions = GetMinimalMemoryPHRPStartupOptions();
                        startupOptions.LoadModsAndSeqInfo = true;

                        var reader = new clsPHRPReader(mPHRPFirstHitsFilePath, mPeptideHitResultType, startupOptions);
                        RegisterEvents(reader);

                        reader.EchoMessagesToConsole = true;

                        if (!reader.CanRead)
                        {
                            ReportError(AppendText("Aborting since PHRPReader is not ready", mErrorMessage));
                            return false;
                        }

                        ReadAndStorePHRPData(reader, swMSGFInputFile, spectrumFileName, false);
                        reader.Dispose();

                        success = true;
                    }
                }

                if (!success)
                {
                    ReportError("Neither the _syn.txt nor the _fht.txt file was found");
                }
            }
            catch (Exception ex)
            {
                ReportError("Error reading the PHRP result file to create the MSGF Input file: " + ex.Message);
                return false;
            }

            return success;
        }

        public static clsPHRPStartupOptions GetMinimalMemoryPHRPStartupOptions()
        {
            var startupOptions = new clsPHRPStartupOptions
            {
                LoadModsAndSeqInfo = false,
                LoadMSGFResults = false,
                LoadScanStatsData = false,
                MaxProteinsPerPSM = 1
            };

            return startupOptions;
        }

        public List<string> GetSkippedInfoByResultId(int resultID)
        {

            if (mSkippedLineInfo.TryGetValue(resultID, out var objSkipList))
            {
                return objSkipList;
            }

            return new List<string>();

        }

        /// <summary>
        /// Determines the scan number for the given MGF file spectrum index
        /// </summary>
        /// <param name="mgfSpectrumIndex"></param>
        /// <returns>Scan number if found; 0 if no match</returns>
        /// <remarks></remarks>
        public int GetScanByMGFSpectrumIndex(int mgfSpectrumIndex)
        {

            if (mMGFIndexToScan.TryGetValue(mgfSpectrumIndex, out var scanNumber))
            {
                return scanNumber;
            }

            return 0;

        }

        private void LogError(string errorMessage)
        {
            try
            {
                if (mLogFile == null)
                {
                    var writeHeader = true;

                    var errorLogFilePath = Path.Combine(mWorkDir, "MSGFInputCreator_Log.txt");

                    if (File.Exists(errorLogFilePath))
                    {
                        writeHeader = false;
                    }

                    mLogFile = new StreamWriter(new FileStream(errorLogFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                    {
                        AutoFlush = true
                    };

                    if (writeHeader)
                    {
                        mLogFile.WriteLine("Date\tMessage");
                    }
                }

                mLogFile.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt") + "\t" + errorMessage);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error writing to MSGFInputCreator log file: " + ex.Message);
            }
        }

        /// <summary>
        /// Read data from a synopsis file or first hits file
        /// Write filter-passing synopsis file data to the MSGF input file
        /// Write first-hits data to the MSGF input file only if it isn't in mMSGFCachedResults
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="swMSGFInputFile"></param>
        /// <param name="spectrumFileName"></param>
        /// <param name="parsingSynopsisFile"></param>
        /// <remarks></remarks>
        private void ReadAndStorePHRPData(
            clsPHRPReader reader,
            TextWriter swMSGFInputFile,
            string spectrumFileName,
            bool parsingSynopsisFile)
        {
            string pHRPSource;

            var resultIDPrevious = 0;
            var scanNumberPrevious = 0;
            var chargePrevious = 0;
            var peptidePrevious = string.Empty;

            var mgfIndexLookupFailureCount = 0;

            if (parsingSynopsisFile)
            {
                pHRPSource = clsMSGFRunner.MSGF_PHRP_DATA_SOURCE_SYN;
            }
            else
            {
                pHRPSource = clsMSGFRunner.MSGF_PHRP_DATA_SOURCE_FHT;
            }

            reader.SkipDuplicatePSMs = false;

            while (reader.MoveNext())
            {
                var success = true;

                var objPSM = reader.CurrentPSM;

                // Compute the result code; we'll use it later to search/populate mMSGFCachedResults
                var peptideResultCode = ConstructMSGFResultCode(objPSM.ScanNumber, objPSM.Charge, objPSM.Peptide);

                bool passesFilters;
                if (DoNotFilterPeptides)
                {
                    passesFilters = true;
                }
                else
                {
                    passesFilters = PassesFilters(objPSM);
                }

                if (parsingSynopsisFile)
                {
                    // Synopsis file
                    // Check for duplicate lines

                    if (passesFilters)
                    {
                        // If this line is a duplicate of the previous line, skip it
                        // This happens in Sequest _syn.txt files where the line is repeated for all protein matches

                        if (scanNumberPrevious == objPSM.ScanNumber &&
                            chargePrevious == objPSM.Charge &&
                            peptidePrevious == objPSM.Peptide)
                        {
                            success = false;

                            if (mSkippedLineInfo.TryGetValue(resultIDPrevious, out var objSkipList))
                            {
                                objSkipList.Add(objPSM.ResultID + "\t" + objPSM.ProteinFirst);
                            }
                            else
                            {
                                objSkipList = new List<string> {
                                    objPSM.ResultID + "\t" + objPSM.ProteinFirst
                                };
                                mSkippedLineInfo.Add(resultIDPrevious, objSkipList);
                            }
                        }
                        else
                        {
                            resultIDPrevious = objPSM.ResultID;
                            scanNumberPrevious = objPSM.ScanNumber;
                            chargePrevious = objPSM.Charge;
                            peptidePrevious = string.Copy(objPSM.Peptide);
                        }
                    }
                }
                else
                {
                    // First-hits file
                    // Use all data in the first-hits file, but skip it if it is already in mMSGFCachedResults

                    passesFilters = true;

                    if (mMSGFCachedResults.ContainsKey(peptideResultCode))
                    {
                        success = false;
                    }
                }

                if (!(success && passesFilters))
                    continue;

                int scanNumberToWrite;

                if (MgfInstrumentData)
                {
                    var scanAndCharge = ConstructMGFMappingCode(objPSM.ScanNumber, objPSM.Charge);

                    if (!mScanAndChargeToMGFIndex.TryGetValue(scanAndCharge, out scanNumberToWrite))
                    {
                        // Match not found; try searching for scan and charge 0
                        if (!mScanAndChargeToMGFIndex.TryGetValue(ConstructMGFMappingCode(objPSM.ScanNumber, 0), out scanNumberToWrite))
                        {
                            scanNumberToWrite = 0;

                            mgfIndexLookupFailureCount += 1;
                            if (mgfIndexLookupFailureCount <= 10)
                            {
                                ReportError("Unable to find " + scanAndCharge + " in mScanAndChargeToMGFIndex for peptide " + objPSM.Peptide);
                            }
                        }
                    }
                }
                else
                {
                    scanNumberToWrite = objPSM.ScanNumber;
                }

                // The title column holds the original peptide sequence
                // If a peptide doesn't have any mods, the Title column and the Annotation column will be identical

                // Columns are: #SpectrumFile  Title  Scan#  Annotation  Charge  Protein_First  Result_ID  Data_Source  Collision_Mode
                swMSGFInputFile.WriteLine(
                    spectrumFileName + "\t" +
                    objPSM.Peptide + "\t" +
                    scanNumberToWrite + "\t" +
                    objPSM.PeptideWithNumericMods + "\t" +
                    objPSM.Charge + "\t" +
                    objPSM.ProteinFirst + "\t" +
                    objPSM.ResultID + "\t" +
                    pHRPSource + "\t" +
                    objPSM.CollisionMode);

                mMSGFInputFileLineCount += 1;

                try
                {
                    mMSGFCachedResults.Add(peptideResultCode, "");
                }
                catch (Exception)
                {
                    // Key is already present; this is unexpected, but we can safely ignore this error
                    LogError("Warning in ReadAndStorePHRPData: Key already defined in mMSGFCachedResults: " + peptideResultCode);
                }
            }

            if (mgfIndexLookupFailureCount > 10)
            {
                ReportError("Was unable to find a match in mScanAndChargeToMGFIndex for " + mgfIndexLookupFailureCount + " PSM results");
            }
        }

        protected void ReportError(string message)
        {
            mErrorMessage = message;
            LogError(message);
            OnErrorEvent(message);
        }

        private void ReportWarning(string message)
        {
            LogError(message);
            OnWarningEvent(message);
        }

        /// <summary>
        /// Define the MSGF input and output file paths
        /// </summary>
        /// <remarks>This method is called after InitializeFilePaths updates mPHRPResultFilePath</remarks>
        protected void UpdateMSGFInputOutputFilePaths()
        {
            mMSGFInputFilePath = Path.Combine(mWorkDir,
                Path.GetFileNameWithoutExtension(mPHRPSynopsisFilePath) +
                MSGF_INPUT_FILENAME_SUFFIX);
            mMSGFResultsFilePath = Path.Combine(mWorkDir,
                Path.GetFileNameWithoutExtension(mPHRPSynopsisFilePath) +
                MSGF_RESULT_FILENAME_SUFFIX);
        }

        public void WriteMSGFResultsHeaders(StreamWriter swOutFile)
        {
            swOutFile.WriteLine("Result_ID\tScan\tCharge\tProtein\tPeptide\tSpecProb\tNotes");
        }

    }
}
