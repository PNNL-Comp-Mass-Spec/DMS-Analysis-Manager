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

namespace AnalysisManagerMSGFPlugin
{
    public abstract class clsMSGFInputCreator
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

        #region "Events"

        public event ErrorEventEventHandler ErrorEvent;

        public delegate void ErrorEventEventHandler(string strErrorMessage);

        public event WarningEventEventHandler WarningEvent;

        public delegate void WarningEventEventHandler(string strWarningMessage);

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
        /// <param name="strDatasetName">Dataset Name</param>
        /// <param name="strWorkDir">Working directory</param>
        /// <param name="eResultType">PeptideHit result type</param>
        /// <remarks></remarks>
        protected clsMSGFInputCreator(string strDatasetName, string strWorkDir, clsPHRPReader.ePeptideHitResultType eResultType)
        {
            mDatasetName = strDatasetName;
            mWorkDir = strWorkDir;
            mPeptideHitResultType = eResultType;

            mErrorMessage = string.Empty;

            mSkippedLineInfo = new SortedDictionary<int, List<string>>();

            mMSGFCachedResults = new SortedDictionary<string, string>();
        }

        #region "Functions to be defined in derived classes"

        protected abstract void InitializeFilePaths();
        protected abstract bool PassesFilters(clsPSM objPSM);

        #endregion

        public void AddUpdateMSGFResult(string strScanNumber, string strCharge, string strPeptide, string strMSGFResultData)
        {
            try
            {
                mMSGFCachedResults[ConstructMSGFResultCode(strScanNumber, strCharge, strPeptide)] = strMSGFResultData;
            }
            catch (Exception)
            {
                // Entry not found; this is unexpected; we will only report the error at the console
                LogError("Entry not found in mMSGFCachedResults for " + ConstructMSGFResultCode(strScanNumber, strCharge, strPeptide));
            }
        }

        private string AppendText(string strText, string strAddnl)
        {
            return AppendText(strText, strAddnl, ": ");
        }

        private string AppendText(string strText, string strAddnl, string strDelimiter)
        {
            if (string.IsNullOrWhiteSpace(strAddnl))
            {
                return strText;
            }
            return strText + strDelimiter + strAddnl;
        }

        public void CloseLogFileNow()
        {
            if ((mLogFile != null))
            {
                mLogFile.Close();
                mLogFile = null;

                PRISM.clsProgRunner.GarbageCollectNow();
                System.Threading.Thread.Sleep(100);
            }
        }

        protected string CombineIfValidFile(string strFolder, string strFile)
        {
            if (!string.IsNullOrWhiteSpace(strFile))
            {
                return Path.Combine(strFolder, strFile);
            }
            return string.Empty;
        }

        private string ConstructMGFMappingCode(int intScanNumber, int intCharge)
        {
            return intScanNumber.ToString() + "_" + intCharge.ToString();
        }

        private string ConstructMSGFResultCode(int intScanNumber, int intCharge, string strPeptide)
        {
            return intScanNumber.ToString() + "_" + intCharge.ToString() + "_" + strPeptide;
        }

        private string ConstructMSGFResultCode(string strScanNumber, string strCharge, string strPeptide)
        {
            return strScanNumber + "_" + strCharge + "_" + strPeptide;
        }

        private bool CreateMGFScanToIndexMap(string strMGFFilePath)
        {
            var intSpectrumIndex = 0;

            try
            {
                var objMGFReader = new clsMGFReader();

                if (!objMGFReader.OpenFile(strMGFFilePath))
                {
                    ReportError("Error opening the .MGF file");
                    return false;
                }

                mScanAndChargeToMGFIndex = new SortedDictionary<string, int>();
                mMGFIndexToScan = new SortedDictionary<int, int>();

                while (true)
                {
                    // Read the next available spectrum
                    List<string> msmsDataList;
                    clsMsMsDataFileReaderBaseClass.udtSpectrumHeaderInfoType udtSpectrumHeaderInfo;

                    var blnSpectrumFound = objMGFReader.ReadNextSpectrum(out msmsDataList, out udtSpectrumHeaderInfo);
                    if (!blnSpectrumFound)
                        break;

                    intSpectrumIndex += 1;

                    if (udtSpectrumHeaderInfo.ParentIonChargeCount == 0)
                    {
                        var strScanAndCharge = ConstructMGFMappingCode(udtSpectrumHeaderInfo.ScanNumberStart, 0);
                        mScanAndChargeToMGFIndex.Add(strScanAndCharge, intSpectrumIndex);
                    }
                    else
                    {
                        for (var intChargeIndex = 0; intChargeIndex <= udtSpectrumHeaderInfo.ParentIonChargeCount - 1; intChargeIndex++)
                        {
                            var strScanAndCharge = ConstructMGFMappingCode(udtSpectrumHeaderInfo.ScanNumberStart, udtSpectrumHeaderInfo.ParentIonCharges[intChargeIndex]);
                            mScanAndChargeToMGFIndex.Add(strScanAndCharge, intSpectrumIndex);
                        }
                    }

                    mMGFIndexToScan.Add(intSpectrumIndex, udtSpectrumHeaderInfo.ScanNumberStart);
                }
            }
            catch (Exception ex)
            {
                ReportError("Error indexing the MGF file: " + ex.Message);
                return false;
            }

            if (intSpectrumIndex > 0)
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
                var phrpReader = new clsPHRPReader(mPHRPFirstHitsFilePath, mPeptideHitResultType, startupOptions);
                RegisterPHRPReaderEventHandlers(phrpReader);
                phrpReader.EchoMessagesToConsole = true;

                if (!phrpReader.CanRead)
                {
                    ReportError(AppendText("Aborting since PHRPReader is not ready", mErrorMessage));
                    return false;
                }

                // Define the path to write the first-hits MSGF results to
                var strMSGFFirstHitsResults = Path.GetFileNameWithoutExtension(mPHRPFirstHitsFilePath) + MSGF_RESULT_FILENAME_SUFFIX;
                strMSGFFirstHitsResults = Path.Combine(mWorkDir, strMSGFFirstHitsResults);

                // Create the output file
                using (var swMSGFFHTFile = new StreamWriter(new FileStream(strMSGFFirstHitsResults, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Write out the headers to swMSGFFHTFile
                    WriteMSGFResultsHeaders(swMSGFFHTFile);

                    var intMissingValueCount = 0;

                    while (phrpReader.MoveNext())
                    {
                        var objPSM = phrpReader.CurrentPSM;

                        var strPeptideResultCode = ConstructMSGFResultCode(objPSM.ScanNumber, objPSM.Charge, objPSM.Peptide);

                        string strMSGFResultData;
                        if (mMSGFCachedResults.TryGetValue(strPeptideResultCode, out strMSGFResultData))
                        {
                            if (string.IsNullOrEmpty(strMSGFResultData))
                            {
                                // Match text is empty
                                // We should not write thie out to disk since it would result in empty columns

                                var strWarningMessage = "MSGF Results are empty for result code '" + strPeptideResultCode +
                                                    "'; this is unexpected";
                                intMissingValueCount += 1;
                                if (intMissingValueCount <= MAX_WARNINGS_TO_REPORT)
                                {
                                    if (intMissingValueCount == MAX_WARNINGS_TO_REPORT)
                                    {
                                        strWarningMessage += "; additional invalid entries will not be reported";
                                    }
                                    ReportWarning(strWarningMessage);
                                }
                                else
                                {
                                    LogError(strWarningMessage);
                                }
                            }
                            else
                            {
                                // Match found; write out the result
                                swMSGFFHTFile.WriteLine(objPSM.ResultID + "\t" + strMSGFResultData);
                            }
                        }
                        else
                        {
                            // Match not found; this is unexpected

                            var strWarningMessage = "Match not found for first-hits entry with result code '" +
                                                strPeptideResultCode + "'; this is unexpected";

                            // Report the first 10 times this happens
                            intMissingValueCount += 1;
                            if (intMissingValueCount <= MAX_WARNINGS_TO_REPORT)
                            {
                                if (intMissingValueCount == MAX_WARNINGS_TO_REPORT)
                                {
                                    strWarningMessage += "; additional missing entries will not be reported";
                                }
                                ReportWarning(strWarningMessage);
                            }
                            else
                            {
                                LogError(strWarningMessage);
                            }
                        }
                    }
                }
                // First Hits MSGF writer

                phrpReader.Dispose();
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
        /// If the synopsis file does not exist, then simply processes the first-hits file
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool CreateMSGFInputFileUsingPHRPResultFiles()
        {
            var blnSuccess = false;

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

                string strSpectrumFileName;
                if (MgfInstrumentData)
                {
                    strSpectrumFileName = mDatasetName + ".mgf";

                    // Need to read the .mgf file and create a mapping between the actual scan number and the 1-based index of the data in the .mgf file
                    blnSuccess = CreateMGFScanToIndexMap(Path.Combine(mWorkDir, strSpectrumFileName));
                    if (!blnSuccess)
                    {
                        return false;
                    }
                }
                else
                {
                    // mzXML filename is dataset plus .mzXML
                    // Note that the jrap reader used by MSGF may fail if the .mzXML filename is capitalized differently than this (i.e., it cannot be .mzxml)
                    strSpectrumFileName = mDatasetName + ".mzXML";
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
                        var phrpReader = new clsPHRPReader(mPHRPSynopsisFilePath, mPeptideHitResultType, startupOptions);
                        RegisterPHRPReaderEventHandlers(phrpReader);
                        phrpReader.EchoMessagesToConsole = true;

                        // Report any errors cached during instantiation of mPHRPReader
                        foreach (var strMessage in phrpReader.ErrorMessages)
                        {
                            ReportError(strMessage);
                        }
                        mErrorMessage = string.Empty;

                        // Report any warnings cached during instantiation of mPHRPReader
                        foreach (var strMessage in phrpReader.WarningMessages)
                        {
                            ReportWarning(strMessage);
                        }

                        phrpReader.ClearErrors();
                        phrpReader.ClearWarnings();

                        if (!phrpReader.CanRead)
                        {
                            ReportError(AppendText("Aborting since PHRPReader is not ready", mErrorMessage));
                            return false;
                        }

                        ReadAndStorePHRPData(phrpReader, swMSGFInputFile, strSpectrumFileName, true);
                        phrpReader.Dispose();

                        blnSuccess = true;
                    }

                    if (!string.IsNullOrEmpty(mPHRPFirstHitsFilePath) && File.Exists(mPHRPFirstHitsFilePath))
                    {
                        // Now read the first-hits file data

                        var startupOptions = GetMinimalMemoryPHRPStartupOptions();
                        startupOptions.LoadModsAndSeqInfo = true;

                        var phrpReader = new clsPHRPReader(mPHRPFirstHitsFilePath, mPeptideHitResultType, startupOptions);
                        RegisterPHRPReaderEventHandlers(phrpReader);
                        phrpReader.EchoMessagesToConsole = true;

                        if (!phrpReader.CanRead)
                        {
                            ReportError(AppendText("Aborting since PHRPReader is not ready", mErrorMessage));
                            return false;
                        }

                        ReadAndStorePHRPData(phrpReader, swMSGFInputFile, strSpectrumFileName, false);
                        phrpReader.Dispose();

                        blnSuccess = true;
                    }
                }

                if (!blnSuccess)
                {
                    ReportError("Neither the _syn.txt nor the _fht.txt file was found");
                }
            }
            catch (Exception ex)
            {
                ReportError("Error reading the PHRP result file to create the MSGF Input file: " + ex.Message);
                return false;
            }

            return blnSuccess;
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

        public List<string> GetSkippedInfoByResultId(int intResultID)
        {
            List<string> objSkipList;

            if (mSkippedLineInfo.TryGetValue(intResultID, out objSkipList))
            {
                return objSkipList;
            }

            return new List<string>();

        }

        /// <summary>
        /// Determines the scan number for the given MGF file spectrum index
        /// </summary>
        /// <param name="intMGFSpectrumIndex"></param>
        /// <returns>Scan number if found; 0 if no match</returns>
        /// <remarks></remarks>
        public int GetScanByMGFSpectrumIndex(int intMGFSpectrumIndex)
        {
            int intScanNumber;

            if (mMGFIndexToScan.TryGetValue(intMGFSpectrumIndex, out intScanNumber))
            {
                return intScanNumber;
            }

            return 0;

        }

        private void LogError(string strErrorMessage)
        {
            try
            {
                if (mLogFile == null)
                {
                    var blnWriteHeader = true;

                    var strErrorLogFilePath = Path.Combine(mWorkDir, "MSGFInputCreator_Log.txt");

                    if (File.Exists(strErrorLogFilePath))
                    {
                        blnWriteHeader = false;
                    }

                    mLogFile = new StreamWriter(new FileStream(strErrorLogFilePath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                    {
                        AutoFlush = true
                    };

                    if (blnWriteHeader)
                    {
                        mLogFile.WriteLine("Date\tMessage");
                    }
                }

                mLogFile.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss tt") + "\t" + strErrorMessage);
            }
            catch (Exception ex)
            {
                ErrorEvent?.Invoke("Error writing to MSGFInputCreator log file: " + ex.Message);
            }
        }

        /// <summary>
        /// Read data from a synopsis file or first hits file
        /// Write filter-passing synopsis file data to the MSGF input file
        /// Write first-hits data to the MSGF input file only if it isn't in mMSGFCachedResults
        /// </summary>
        /// <param name="objReader"></param>
        /// <param name="swMSGFInputFile"></param>
        /// <param name="strSpectrumFileName"></param>
        /// <param name="blnParsingSynopsisFile"></param>
        /// <remarks></remarks>
        private void ReadAndStorePHRPData(
            clsPHRPReader objReader,
            StreamWriter swMSGFInputFile,
            string strSpectrumFileName,
            bool blnParsingSynopsisFile)
        {
            string strPHRPSource;

            var intResultIDPrevious = 0;
            var intScanNumberPrevious = 0;
            var intChargePrevious = 0;
            var strPeptidePrevious = string.Empty;

            var intMGFIndexLookupFailureCount = 0;

            if (blnParsingSynopsisFile)
            {
                strPHRPSource = clsMSGFRunner.MSGF_PHRP_DATA_SOURCE_SYN;
            }
            else
            {
                strPHRPSource = clsMSGFRunner.MSGF_PHRP_DATA_SOURCE_FHT;
            }

            objReader.SkipDuplicatePSMs = false;

            while (objReader.MoveNext())
            {
                var blnSuccess = true;

                var objPSM = objReader.CurrentPSM;

                // Compute the result code; we'll use it later to search/populate mMSGFCachedResults
                var strPeptideResultCode = ConstructMSGFResultCode(objPSM.ScanNumber, objPSM.Charge, objPSM.Peptide);

                bool blnPassesFilters;
                if (DoNotFilterPeptides)
                {
                    blnPassesFilters = true;
                }
                else
                {
                    blnPassesFilters = PassesFilters(objPSM);
                }

                if (blnParsingSynopsisFile)
                {
                    // Synopsis file
                    // Check for duplicate lines

                    if (blnPassesFilters)
                    {
                        // If this line is a duplicate of the previous line, then skip it
                        // This happens in Sequest _syn.txt files where the line is repeated for all protein matches

                        if (intScanNumberPrevious == objPSM.ScanNumber &&
                            intChargePrevious == objPSM.Charge &&
                            strPeptidePrevious == objPSM.Peptide)
                        {
                            blnSuccess = false;

                            List<string> objSkipList;
                            if (mSkippedLineInfo.TryGetValue(intResultIDPrevious, out objSkipList))
                            {
                                objSkipList.Add(objPSM.ResultID + "\t" + objPSM.ProteinFirst);
                            }
                            else
                            {
                                objSkipList = new List<string> {
                                    objPSM.ResultID + "\t" + objPSM.ProteinFirst
                                };
                                mSkippedLineInfo.Add(intResultIDPrevious, objSkipList);
                            }
                        }
                        else
                        {
                            intResultIDPrevious = objPSM.ResultID;
                            intScanNumberPrevious = objPSM.ScanNumber;
                            intChargePrevious = objPSM.Charge;
                            strPeptidePrevious = string.Copy(objPSM.Peptide);
                        }
                    }
                }
                else
                {
                    // First-hits file
                    // Use all data in the first-hits file, but skip it if it is already in mMSGFCachedResults

                    blnPassesFilters = true;

                    if (mMSGFCachedResults.ContainsKey(strPeptideResultCode))
                    {
                        blnSuccess = false;
                    }
                }

                if (!(blnSuccess & blnPassesFilters))
                    continue;

                int intScanNumberToWrite;

                if (MgfInstrumentData)
                {
                    var strScanAndCharge = ConstructMGFMappingCode(objPSM.ScanNumber, objPSM.Charge);

                    if (!mScanAndChargeToMGFIndex.TryGetValue(strScanAndCharge, out intScanNumberToWrite))
                    {
                        // Match not found; try searching for scan and charge 0
                        if (!mScanAndChargeToMGFIndex.TryGetValue(ConstructMGFMappingCode(objPSM.ScanNumber, 0), out intScanNumberToWrite))
                        {
                            intScanNumberToWrite = 0;

                            intMGFIndexLookupFailureCount += 1;
                            if (intMGFIndexLookupFailureCount <= 10)
                            {
                                ReportError("Unable to find " + strScanAndCharge + " in mScanAndChargeToMGFIndex for peptide " + objPSM.Peptide);
                            }
                        }
                    }
                }
                else
                {
                    intScanNumberToWrite = objPSM.ScanNumber;
                }

                // The title column holds the original peptide sequence
                // If a peptide doesn't have any mods, then the Title column and the Annotation column will be identical

                // Columns are: #SpectrumFile  Title  Scan#  Annotation  Charge  Protein_First  Result_ID  Data_Source  Collision_Mode
                swMSGFInputFile.WriteLine(
                    strSpectrumFileName + "\t" +
                    objPSM.Peptide + "\t" +
                    intScanNumberToWrite + "\t" +
                    objPSM.PeptideWithNumericMods + "\t" +
                    objPSM.Charge + "\t" +
                    objPSM.ProteinFirst + "\t" +
                    objPSM.ResultID + "\t" +
                    strPHRPSource + "\t" +
                    objPSM.CollisionMode);

                mMSGFInputFileLineCount += 1;

                try
                {
                    mMSGFCachedResults.Add(strPeptideResultCode, "");
                }
                catch (Exception)
                {
                    // Key is already present; this is unexpected, but we can safely ignore this error
                    LogError("Warning in ReadAndStorePHRPData: Key already defined in mMSGFCachedResults: " + strPeptideResultCode);
                }
            }

            if (intMGFIndexLookupFailureCount > 10)
            {
                ReportError("Was unable to find a match in mScanAndChargeToMGFIndex for " + intMGFIndexLookupFailureCount + " PSM results");
            }
        }

        protected void ReportError(string strErrorMessage)
        {
            mErrorMessage = strErrorMessage;
            LogError(mErrorMessage);
            ErrorEvent?.Invoke(mErrorMessage);
        }

        private void ReportWarning(string strWarningMessage)
        {
            LogError(strWarningMessage);
            WarningEvent?.Invoke(strWarningMessage);
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

        /// <summary>
        /// Note that clsPHRPReader is instantiated and disposed of several times
        /// Make it easy to attach the event handlers
        /// </summary>
        /// <param name="reader"></param>
        private void RegisterPHRPReaderEventHandlers(clsPHRPReader reader)
        {
            reader.ErrorEvent += PHRPReader_ErrorEvent;
            reader.MessageEvent += PHRPReader_MessageEvent;
            reader.WarningEvent += PHRPReader_WarningEvent;
        }

        private void PHRPReader_ErrorEvent(string strErrorMessage)
        {
            ReportError(strErrorMessage);
        }

        private void PHRPReader_MessageEvent(string strMessage)
        {
            Console.WriteLine(strMessage);
        }

        private void PHRPReader_WarningEvent(string strWarningMessage)
        {
            ReportWarning(strWarningMessage);
        }
    }
}
