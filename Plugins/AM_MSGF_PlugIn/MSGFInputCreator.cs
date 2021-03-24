//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 07/20/2010
//
// This class reads a tab-delimited text file (created by the Peptide File Extractor or by PHRP)
// and creates a tab-delimited text file suitable for processing by MSGF
//
// The class must be derived by a sub-class customized for the specific analysis tool (SEQUEST, X!Tandem, Inspect, etc.)
//
//*********************************************************************************************************

using MsMsDataFileReader;
using PHRPReader;
using PRISM;
using System;
using System.Collections.Generic;
using System.IO;
using PHRPReader.Data;

namespace AnalysisManagerMSGFPlugin
{
    public abstract class MSGFInputCreator : EventNotifier
    {
        #region "Constants"

        private const string MSGF_INPUT_FILENAME_SUFFIX = "_MSGF_input.txt";
        public const string MSGF_RESULT_FILENAME_SUFFIX = "_MSGF.txt";

        #endregion

        #region "Module variables"

        protected readonly string mDatasetName;
        protected readonly string mWorkDir;

        private readonly Enums.PeptideHitResultTypes mPeptideHitResultType;

        private readonly SortedDictionary<int, List<string>> mSkippedLineInfo;

        /// <summary>
        /// MSGF cached results
        /// </summary>
        /// <remarks>
        /// This dictionary is initially populated with a string constructed using
        /// Scan, Charge, and the original peptide sequence in the PHRP file: Scan_Charge_OriginalPeptideSequence
        /// It will contain an entry for every line written to the MSGF input file
        /// It is later updated by AddUpdateMSGFResult() to store the properly formatted MSGF result line for each entry
        /// Finally, it will be used by CreateMSGFFirstHitsFile to create the MSGF file that corresponds to the first-hits file
        /// </remarks>
        private readonly SortedDictionary<string, string> mMSGFCachedResults;

        /// <summary>
        /// This dictionary holds a mapping between Scan_Charge to the spectrum index in the MGF file (first spectrum has index=1)
        /// It is only used if MGFInstrumentData=True
        /// </summary>
        private SortedDictionary<string, int> mScanAndChargeToMGFIndex;

        /// <summary>
        /// This dictionary is the inverse of mScanAndChargeToMGFIndex
        /// </summary>
        /// <remarks>
        /// mMGFIndexToScan allows for a lookup of Scan Number given the MGF index
        /// It is only used if MGFInstrumentData=True
        /// </remarks>
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
        /// <param name="resultType">PeptideHit result type</param>
        protected MSGFInputCreator(string datasetName, string workDir, Enums.PeptideHitResultTypes resultType)
        {
            mDatasetName = datasetName;
            mWorkDir = workDir;
            mPeptideHitResultType = resultType;

            mErrorMessage = string.Empty;

            mSkippedLineInfo = new SortedDictionary<int, List<string>>();

            mMSGFCachedResults = new SortedDictionary<string, string>();
        }

        #region "Functions to be defined in derived classes"

        protected abstract void InitializeFilePaths();
        protected abstract bool PassesFilters(PSM currentPSM);

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

                ProgRunner.GarbageCollectNow();
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
                var mgfReader = new clsMGFReader();

                if (!mgfReader.OpenFile(mgfFilePath))
                {
                    ReportError("Error opening the .MGF file");
                    return false;
                }

                mScanAndChargeToMGFIndex = new SortedDictionary<string, int>();
                mMGFIndexToScan = new SortedDictionary<int, int>();

                while (true)
                {
                    var spectrumFound = mgfReader.ReadNextSpectrum(out _, out var udtSpectrumHeaderInfo);
                    if (!spectrumFound)
                        break;

                    spectrumIndex++;

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
                using var reader = new ReaderFactory(mPHRPFirstHitsFilePath, mPeptideHitResultType, startupOptions);
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
                using var writer = new StreamWriter(new FileStream(msgfFirstHitsResults, FileMode.Create, FileAccess.Write, FileShare.Read));

                // Write out the headers to msgfFHTFile
                WriteMSGFResultsHeaders(writer);

                var missingValueCount = 0;

                while (reader.MoveNext())
                {
                    var currentPSM = reader.CurrentPSM;

                    var peptideResultCode = ConstructMSGFResultCode(currentPSM.ScanNumber, currentPSM.Charge, currentPSM.Peptide);

                    if (mMSGFCachedResults.TryGetValue(peptideResultCode, out var msgfResultData))
                    {
                        if (string.IsNullOrEmpty(msgfResultData))
                        {
                            // Match text is empty
                            // We should not write this out to disk since it would result in empty columns

                            var warningMessage = "MSGF Results are empty for result code '" + peptideResultCode +
                                                 "'; this is unexpected";
                            missingValueCount++;
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
                            writer.WriteLine(currentPSM.ResultID + "\t" + msgfResultData);
                        }
                    }
                    else
                    {
                        // Match not found; this is unexpected

                        var warningMessage = "Match not found for first-hits entry with result code '" +
                                             peptideResultCode + "'; this is unexpected";

                        // Report the first 10 times this happens
                        missingValueCount++;
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
                    // ReSharper disable CommentTypo

                    // mzXML filename is dataset plus .mzXML
                    // Note that the jrap reader used by MSGF may fail if the .mzXML filename is capitalized differently than this (i.e., it cannot be .mzxml)

                    // ReSharper restore CommentTypo
                    spectrumFileName = mDatasetName + ".mzXML";
                }

                // Create the MSGF Input file that we will write data to
                using (var writer = new StreamWriter(new FileStream(mMSGFInputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Write out the headers:  #SpectrumFile  Title  Scan#  Annotation  Charge  Protein_First  Result_ID  Data_Source  Collision_Mode
                    // Note that we're storing the original peptide sequence in the "Title" column, while the marked up sequence (with mod masses) goes in the "Annotation" column
                    writer.WriteLine(
                        MSGFRunner.MSGF_RESULT_COLUMN_SpectrumFile + "\t" +
                        MSGFRunner.MSGF_RESULT_COLUMN_Title + "\t" +
                        MSGFRunner.MSGF_RESULT_COLUMN_ScanNumber + "\t" +
                        MSGFRunner.MSGF_RESULT_COLUMN_Annotation + "\t" +
                        MSGFRunner.MSGF_RESULT_COLUMN_Charge + "\t" +
                        MSGFRunner.MSGF_RESULT_COLUMN_Protein_First + "\t" +
                        MSGFRunner.MSGF_RESULT_COLUMN_Result_ID + "\t" +
                        MSGFRunner.MSGF_RESULT_COLUMN_Data_Source + "\t" +
                        MSGFRunner.MSGF_RESULT_COLUMN_Collision_Mode);

                    // Initialize some tracking variables
                    mMSGFInputFileLineCount = 1;

                    mSkippedLineInfo.Clear();

                    mMSGFCachedResults.Clear();

                    if (!string.IsNullOrEmpty(mPHRPSynopsisFilePath) && File.Exists(mPHRPSynopsisFilePath))
                    {
                        var startupOptions = GetMinimalMemoryPHRPStartupOptions();
                        startupOptions.LoadModsAndSeqInfo = true;

                        // Read the synopsis file data
                        var reader = new ReaderFactory(mPHRPSynopsisFilePath, mPeptideHitResultType, startupOptions);
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

                        ReadAndStorePHRPData(reader, writer, spectrumFileName, true);
                        reader.Dispose();

                        success = true;
                    }

                    if (!string.IsNullOrEmpty(mPHRPFirstHitsFilePath) && File.Exists(mPHRPFirstHitsFilePath))
                    {
                        // Now read the first-hits file data

                        var startupOptions = GetMinimalMemoryPHRPStartupOptions();
                        startupOptions.LoadModsAndSeqInfo = true;

                        var reader = new ReaderFactory(mPHRPFirstHitsFilePath, mPeptideHitResultType, startupOptions);
                        RegisterEvents(reader);

                        reader.EchoMessagesToConsole = true;

                        if (!reader.CanRead)
                        {
                            ReportError(AppendText("Aborting since PHRPReader is not ready", mErrorMessage));
                            return false;
                        }

                        ReadAndStorePHRPData(reader, writer, spectrumFileName, false);
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

        public static StartupOptions GetMinimalMemoryPHRPStartupOptions()
        {
            var startupOptions = new StartupOptions
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
            if (mSkippedLineInfo.TryGetValue(resultID, out var skipList))
            {
                return skipList;
            }

            return new List<string>();
        }

        /// <summary>
        /// Determines the scan number for the given MGF file spectrum index
        /// </summary>
        /// <param name="mgfSpectrumIndex"></param>
        /// <returns>Scan number if found; 0 if no match</returns>
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
        /// <param name="msgfInputFileWriter"></param>
        /// <param name="spectrumFileName"></param>
        /// <param name="parsingSynopsisFile"></param>
        private void ReadAndStorePHRPData(
            ReaderFactory reader,
            TextWriter msgfInputFileWriter,
            string spectrumFileName,
            bool parsingSynopsisFile)
        {
            string phrpSource;

            var resultIDPrevious = 0;
            var scanNumberPrevious = 0;
            var chargePrevious = 0;
            var peptidePrevious = string.Empty;

            var mgfIndexLookupFailureCount = 0;

            if (parsingSynopsisFile)
            {
                phrpSource = MSGFRunner.MSGF_PHRP_DATA_SOURCE_SYN;
            }
            else
            {
                phrpSource = MSGFRunner.MSGF_PHRP_DATA_SOURCE_FHT;
            }

            reader.SkipDuplicatePSMs = false;

            while (reader.MoveNext())
            {
                var success = true;

                var currentPSM = reader.CurrentPSM;

                // Compute the result code; we'll use it later to search/populate mMSGFCachedResults
                var peptideResultCode = ConstructMSGFResultCode(currentPSM.ScanNumber, currentPSM.Charge, currentPSM.Peptide);

                bool passesFilters;
                if (DoNotFilterPeptides)
                {
                    passesFilters = true;
                }
                else
                {
                    passesFilters = PassesFilters(currentPSM);
                }

                if (parsingSynopsisFile)
                {
                    // Synopsis file
                    // Check for duplicate lines

                    if (passesFilters)
                    {
                        // If this line is a duplicate of the previous line, skip it
                        // This happens in SEQUEST _syn.txt files where the line is repeated for all protein matches

                        if (scanNumberPrevious == currentPSM.ScanNumber &&
                            chargePrevious == currentPSM.Charge &&
                            peptidePrevious == currentPSM.Peptide)
                        {
                            success = false;

                            if (mSkippedLineInfo.TryGetValue(resultIDPrevious, out var skipList))
                            {
                                skipList.Add(currentPSM.ResultID + "\t" + currentPSM.ProteinFirst);
                            }
                            else
                            {
                                skipList = new List<string> {
                                    currentPSM.ResultID + "\t" + currentPSM.ProteinFirst
                                };
                                mSkippedLineInfo.Add(resultIDPrevious, skipList);
                            }
                        }
                        else
                        {
                            resultIDPrevious = currentPSM.ResultID;
                            scanNumberPrevious = currentPSM.ScanNumber;
                            chargePrevious = currentPSM.Charge;
                            peptidePrevious = string.Copy(currentPSM.Peptide);
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
                    var scanAndCharge = ConstructMGFMappingCode(currentPSM.ScanNumber, currentPSM.Charge);

                    if (!mScanAndChargeToMGFIndex.TryGetValue(scanAndCharge, out scanNumberToWrite))
                    {
                        // Match not found; try searching for scan and charge 0
                        if (!mScanAndChargeToMGFIndex.TryGetValue(ConstructMGFMappingCode(currentPSM.ScanNumber, 0), out scanNumberToWrite))
                        {
                            scanNumberToWrite = 0;

                            mgfIndexLookupFailureCount++;
                            if (mgfIndexLookupFailureCount <= 10)
                            {
                                ReportError("Unable to find " + scanAndCharge + " in mScanAndChargeToMGFIndex for peptide " + currentPSM.Peptide);
                            }
                        }
                    }
                }
                else
                {
                    scanNumberToWrite = currentPSM.ScanNumber;
                }

                // The title column holds the original peptide sequence
                // If a peptide doesn't have any mods, the Title column and the Annotation column will be identical

                // Columns are: #SpectrumFile  Title  Scan#  Annotation  Charge  Protein_First  Result_ID  Data_Source  Collision_Mode
                msgfInputFileWriter.WriteLine(
                    spectrumFileName + "\t" +
                    currentPSM.Peptide + "\t" +
                    scanNumberToWrite + "\t" +
                    currentPSM.PeptideWithNumericMods + "\t" +
                    currentPSM.Charge + "\t" +
                    currentPSM.ProteinFirst + "\t" +
                    currentPSM.ResultID + "\t" +
                    phrpSource + "\t" +
                    currentPSM.CollisionMode);

                mMSGFInputFileLineCount++;

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
            var synopsisFileName = Path.GetFileNameWithoutExtension(mPHRPSynopsisFilePath);

            mMSGFInputFilePath = Path.Combine(mWorkDir, synopsisFileName + MSGF_INPUT_FILENAME_SUFFIX);

            mMSGFResultsFilePath = Path.Combine(mWorkDir, synopsisFileName + MSGF_RESULT_FILENAME_SUFFIX);
        }

        public void WriteMSGFResultsHeaders(StreamWriter writer)
        {
            writer.WriteLine("Result_ID\tScan\tCharge\tProtein\tPeptide\tSpecProb\tNotes");
        }
    }
}
