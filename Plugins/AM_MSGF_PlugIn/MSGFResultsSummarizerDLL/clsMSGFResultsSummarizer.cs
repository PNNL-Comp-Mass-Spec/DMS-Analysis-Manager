//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 02/14/2012
//
// This class reads an MSGF results file and accompanying peptide/protein map file
//  to count the number of peptides passing a given MSGF threshold
// Reports PSM count, unique peptide count, and unique protein count
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data;

using PHRPReader;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using PRISM;

// ReSharper disable UnusedMember.Global

namespace MSGFResultsSummarizer
{
    public class clsMSGFResultsSummarizer : EventNotifier
    {
        #region "Constants and Enums"

        public const double DEFAULT_MSGF_THRESHOLD = 1E-10;        // 1E-10
        public const double DEFAULT_EVALUE_THRESHOLD = 0.0001;     // 1E-4   (only used when MSGF Scores are not available)
        public const double DEFAULT_FDR_THRESHOLD = 0.01;          // 1% FDR

        private const string DEFAULT_CONNECTION_STRING = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;";

        private const string STORE_JOB_PSM_RESULTS_SP_NAME = "StoreJobPSMStats";

        private const string MSGF_RESULT_FILENAME_SUFFIX = "_MSGF.txt";

        #endregion

        #region "Structures"

        private struct udtPSMStatsType
        {
            /// <summary>
            /// Number of spectra with a match
            /// </summary>
            /// <remarks></remarks>
            public int TotalPSMs;

            /// <summary>
            /// Number of distinct peptides
            /// </summary>
            /// <remarks>
            /// For modified peptides, collapses peptides with the same sequence and same modifications (+/- 1 residue)
            /// For example, LS*SPATLNSR and LSS*PATLNSR are considered equivalent
            /// But P#EPT*IDES and PEP#T*IDES and P#EPTIDES* are all different
            /// (the collapsing of similar peptides is done in method LoadPSMs with the call to FindNormalizedSequence)
            /// </remarks>
            public int UniquePeptideCount;

            /// <summary>
            /// Number of distinct proteins
            /// </summary>
            /// <remarks></remarks>
            public int UniqueProteinCount;

            public int UniquePhosphopeptideCount;
            public int UniquePhosphopeptidesCTermK;

            public int UniquePhosphopeptidesCTermR;

            /// <summary>
            /// Number of unique peptides that come from Keratin proteins
            /// </summary>
            public int KeratinPeptides;

            /// <summary>
            /// Number of unique peptides that come from Trypsin proteins
            /// </summary>
            public int TrypsinPeptides;

            /// <summary>
            /// Number of unique peptides that are partially or fully tryptic
            /// </summary>
            public int TrypticPeptides;

            public float MissedCleavageRatio;

            public float MissedCleavageRatioPhospho;

            public void Clear()
            {
                TotalPSMs = 0;
                UniquePeptideCount = 0;
                UniqueProteinCount = 0;
                UniquePhosphopeptideCount = 0;
                UniquePhosphopeptidesCTermK = 0;
                UniquePhosphopeptidesCTermR = 0;
                MissedCleavageRatio = 0;
                MissedCleavageRatioPhospho = 0;
                KeratinPeptides = 0;
                TrypsinPeptides = 0;
                TrypticPeptides = 0;
            }
        }

        #endregion

        #region "Member variables"

        private string mErrorMessage = string.Empty;
        private readonly short mDebugLevel;

        private int mSpectraSearched;

        /// <summary>
        /// Value between 0 and 100, indicating the percentage of the MS2 spectra with search results that are
        /// more than 2 scans away from an adjacent spectrum
        /// </summary>
        /// <remarks></remarks>
        private double mPercentMSnScansNoPSM;

        private udtPSMStatsType mMSGFBasedCounts;
        private udtPSMStatsType mFDRBasedCounts;

        private readonly string mDatasetName;
        private readonly int mJob;
        private readonly string mWorkDir;
        private readonly string mConnectionString;

        private readonly ExecuteDatabaseSP mStoredProcedureExecutor;

        // The following is auto-determined in ProcessMSGFResults
        private string mMSGFSynopsisFileName = string.Empty;

        #endregion

        #region "Properties"

        /// <summary>
        /// Dataset name
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks>
        /// Used to contact DMS to lookup the total number of scans and total number of MSn scans
        /// This information is used by
        /// </remarks>
        public string DatasetName { get; set; }

        /// <summary>
        /// Set this to false to disable contacting DMS to look up scan stats for the dataset
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks>When this is false, we cannot compute MaximumScanGapAdjacentMSn or PercentMSnScansNoPSM</remarks>
        public bool ContactDatabase { get; set; }

        public bool DatasetScanStatsLookupError { get; private set; }

        public string ErrorMessage
        {
            get
            {
                if (string.IsNullOrEmpty(mErrorMessage))
                {
                    return string.Empty;
                }
                return mErrorMessage;
            }
        }

        public double EValueThreshold { get; set; } = DEFAULT_EVALUE_THRESHOLD;

        public double FDRThreshold { get; set; } = DEFAULT_FDR_THRESHOLD;

        /// <summary>
        /// Maximum number of scans separating two MS2 spectra with search results
        /// </summary>
        public int MaximumScanGapAdjacentMSn { get; private set; }

        public double MSGFThreshold { get; set; } = DEFAULT_MSGF_THRESHOLD;

        public string MSGFSynopsisFileName
        {
            get
            {
                if (string.IsNullOrEmpty(mMSGFSynopsisFileName))
                {
                    return string.Empty;
                }
                return mMSGFSynopsisFileName;
            }
        }

        public string OutputFolderPath { get; set; } = string.Empty;

        public double PercentMSnScansNoPSM => mPercentMSnScansNoPSM;

        public bool PostJobPSMResultsToDB { get; set; }

        public clsPHRPReader.ePeptideHitResultType ResultType { get; }

        public string ResultTypeName => ResultType.ToString();

        public int SpectraSearched => mSpectraSearched;

        public int TotalPSMsFDR => mFDRBasedCounts.TotalPSMs;

        public int TotalPSMsMSGF => mMSGFBasedCounts.TotalPSMs;

        public bool SaveResultsToTextFile { get; set; } = true;

        public int UniquePeptideCountFDR => mFDRBasedCounts.UniquePeptideCount;

        public int UniquePeptideCountMSGF => mMSGFBasedCounts.UniquePeptideCount;

        public int UniqueProteinCountFDR => mFDRBasedCounts.UniqueProteinCount;

        public int UniqueProteinCountMSGF => mMSGFBasedCounts.UniqueProteinCount;

        public int UniquePhosphopeptideCountFDR => mFDRBasedCounts.UniquePhosphopeptideCount;

        public int UniquePhosphopeptideCountMSGF => mMSGFBasedCounts.UniquePhosphopeptideCount;

        public int UniquePhosphopeptidesCTermK_FDR => mFDRBasedCounts.UniquePhosphopeptidesCTermK;

        public int UniquePhosphopeptidesCTermK_MSGF => mMSGFBasedCounts.UniquePhosphopeptidesCTermK;

        public int UniquePhosphopeptidesCTermR_FDR => mFDRBasedCounts.UniquePhosphopeptidesCTermR;

        public int UniquePhosphopeptidesCTermR_MSGF => mMSGFBasedCounts.UniquePhosphopeptidesCTermR;

        public float MissedCleavageRatioFDR => mFDRBasedCounts.MissedCleavageRatio;

        public float MissedCleavageRatioMSGF => mMSGFBasedCounts.MissedCleavageRatio;

        public float MissedCleavageRatioPhosphoFDR => mFDRBasedCounts.MissedCleavageRatioPhospho;

        public float MissedCleavageRatioPhosphoMSGF => mMSGFBasedCounts.MissedCleavageRatioPhospho;

        public int KeratinPeptidesFDR => mFDRBasedCounts.KeratinPeptides;

        public int KeratinPeptidesMSGF => mMSGFBasedCounts.KeratinPeptides;

        public int TrypsinPeptidesFDR => mFDRBasedCounts.TrypsinPeptides;

        public int TrypsinPeptidesMSGF => mMSGFBasedCounts.TrypsinPeptides;

        public int TrypticPeptidesMSGF => mMSGFBasedCounts.TrypticPeptides;

        public int TrypticPeptidesFDR => mFDRBasedCounts.TrypticPeptides;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="eResultType">Peptide Hit result type</param>
        /// <param name="strDatasetName">Dataset name</param>
        /// <param name="intJob">Job number</param>
        /// <param name="strSourceFolderPath">Source folder path</param>
        /// <remarks></remarks>
        public clsMSGFResultsSummarizer(clsPHRPReader.ePeptideHitResultType eResultType, string strDatasetName, int intJob, string strSourceFolderPath)
            : this(eResultType, strDatasetName, intJob, strSourceFolderPath, DEFAULT_CONNECTION_STRING, debugLevel: 1)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="eResultType">Peptide Hit result type</param>
        /// <param name="strDatasetName">Dataset name</param>
        /// <param name="intJob">Job number</param>
        /// <param name="strSourceFolderPath">Source folder path</param>
        /// <param name="strConnectionString">DMS connection string</param>
        /// <param name="debugLevel">Debug Level</param>
        /// <remarks></remarks>
        public clsMSGFResultsSummarizer(
            clsPHRPReader.ePeptideHitResultType eResultType,
            string strDatasetName,
            int intJob,
            string strSourceFolderPath,
            string strConnectionString,
            short debugLevel)
        {
            ResultType = eResultType;
            mDatasetName = strDatasetName;
            mJob = intJob;
            mWorkDir = strSourceFolderPath;
            mConnectionString = strConnectionString;
            mDebugLevel = debugLevel;

            mStoredProcedureExecutor = new ExecuteDatabaseSP(mConnectionString);
            RegisterEvents(mStoredProcedureExecutor);

            ContactDatabase = true;
        }

        private void AddUpdateUniqueSequence(IDictionary<int, clsUniqueSeqInfo> lstUniqueSeqs, int intSeqId, clsUniqueSeqInfo seqInfoToStore)
        {

            if (lstUniqueSeqs.TryGetValue(intSeqId, out var existingSeqInfo))
            {
                existingSeqInfo.UpdateObservationCount(seqInfoToStore.ObsCount + existingSeqInfo.ObsCount);
            }
            else
            {
                lstUniqueSeqs.Add(intSeqId, seqInfoToStore);
            }
        }

        private void ExamineFirstHitsFile(string strFirstHitsFilePath)
        {
            try
            {
                // Initialize the list that will be used to track the number of spectra searched
                // Keys are Scan_Charge, values are Scan number
                var lstUniqueSpectra = new Dictionary<string, int>();

                var startupOptions = GetMinimalMemoryPHRPStartupOptions();

                using (var reader = new clsPHRPReader(strFirstHitsFilePath, startupOptions))
                {
                    RegisterEvents(reader);

                    while (reader.MoveNext())
                    {
                        var objPSM = reader.CurrentPSM;

                        if (objPSM.Charge >= 0)
                        {
                            var strScanChargeCombo = objPSM.ScanNumber + "_" + objPSM.Charge;

                            if (!lstUniqueSpectra.ContainsKey(strScanChargeCombo))
                            {
                                lstUniqueSpectra.Add(strScanChargeCombo, objPSM.ScanNumber);
                            }
                        }
                    }
                }

                mSpectraSearched = lstUniqueSpectra.Count;

                // Set these to defaults for now
                MaximumScanGapAdjacentMSn = 0;
                mPercentMSnScansNoPSM = 100;

                if (!ContactDatabase)
                {
                    return;
                }

                var scanList = lstUniqueSpectra.Values.Distinct().ToList();

                CheckForScanGaps(scanList);

            }
            catch (Exception ex)
            {
                SetErrorMessage(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        private void CheckForScanGaps(List<int> scanList)
        {
            // Look for scan range gaps in the spectra list
            // The occurrence of large gaps indicates that a processing thread in MSGF+ crashed and the results may be incomplete
            scanList.Sort();

            var success = LookupScanStats(out var totalSpectra, out var totalMSnSpectra);
            if (!success || totalSpectra <= 0)
            {
                DatasetScanStatsLookupError = true;
                return;
            }

            MaximumScanGapAdjacentMSn = 0;

            for (var i = 1; i <= scanList.Count - 1; i++)
            {
                var scanGap = scanList[i] - scanList[i - 1];

                if (scanGap > MaximumScanGapAdjacentMSn)
                {
                    MaximumScanGapAdjacentMSn = scanGap;
                }
            }

            if (totalMSnSpectra > 0)
            {
                mPercentMSnScansNoPSM = (1 - scanList.Count / (float)totalMSnSpectra) * 100.0;
            }
            else
            {
                // Report 100% because we cannot accurately compute this value without knowing totalMSnSpectra
                mPercentMSnScansNoPSM = 100;
            }

            if (totalSpectra > 0)
            {
                // Compare the last scan number seen to the total number of scans
                var scanGap = totalSpectra - scanList[scanList.Count - 1] - 1;

                if (scanGap > MaximumScanGapAdjacentMSn)
                {
                    MaximumScanGapAdjacentMSn = scanGap;
                }
            }
        }

        private float ComputeMissedCleavageRatio(IDictionary<int, clsUniqueSeqInfo> lstUniqueSequences)
        {
            if (lstUniqueSequences.Count == 0)
            {
                return 0;
            }

            var missedCleavages = (from item in lstUniqueSequences where item.Value.MissedCleavage select item.Key).Count();
            var missedCleavageRatio = missedCleavages / (float)lstUniqueSequences.Count;

            return missedCleavageRatio;
        }

        /// <summary>
        /// Lookup the total scans and number of MS/MS scans for the dataset defined by property DatasetName
        /// </summary>
        /// <param name="totalSpectra"></param>
        /// <param name="totalMSnSpectra"></param>
        /// <returns></returns>
        /// <remarks>True if success; false if an error, including if DatasetName is empty or if the dataset is not found in the database</remarks>
        private bool LookupScanStats(out int totalSpectra, out int totalMSnSpectra)
        {
            totalSpectra = 0;
            totalMSnSpectra = 0;

            try
            {
                if (string.IsNullOrEmpty(DatasetName))
                {
                    SetErrorMessage("Dataset name is empty; cannot lookup scan stats");
                    return false;
                }

                var queryScanStats = "" + " SELECT Scan_Count_Total, " +
                                     "        SUM(CASE WHEN Scan_Type LIKE '%MSn' THEN Scan_Count ELSE 0 END) AS ScanCountMSn" +
                                     " FROM V_Dataset_Scans_Export DSE" + " WHERE Dataset = '" + DatasetName + "'" + " GROUP BY Scan_Count_Total";


                var dbTools = new DBTools(mConnectionString);
                RegisterEvents(dbTools);

                var success = dbTools.GetQueryResults(queryScanStats, out var scanStatsFromDb, "LookupScanStats_V_Dataset_Scans_Export");

                if (success && scanStatsFromDb.Count > 0)
                {
                    foreach (var resultRow in scanStatsFromDb)
                    {
                        var scanCountTotal = resultRow[0];
                        var scanCountMSn = resultRow[1];

                        if (!int.TryParse(scanCountTotal, out totalSpectra))
                        {
                            success = false;
                            break;
                        }

                        int.TryParse(scanCountMSn, out totalMSnSpectra);
                        return true;
                    }
                }

                var queryScanTotal = "" + " SELECT [Scan Count]" + " FROM V_Dataset_Export" + " WHERE Dataset = '" + DatasetName + "'";

                success = dbTools.GetQueryResults(queryScanTotal, out var datasetScanCountFromDb, "LookupScanStats_V_Dataset_Export");

                if (success && datasetScanCountFromDb.Count > 0)
                {
                    foreach (var resultRow in datasetScanCountFromDb)
                    {
                        var scanCountTotal = resultRow[0];

                        int.TryParse(scanCountTotal, out totalSpectra);
                        return true;
                    }
                }

                SetErrorMessage("Dataset not found in the database; cannot retrieve scan counts: " + DatasetName);
                return false;
            }
            catch (Exception ex)
            {
                SetErrorMessage("Exception retrieving scan stats from the database: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Either filter by MSGF or filter by FDR, then update the stats
        /// </summary>
        /// <param name="usingMSGFOrEValueFilter">When true, filter by MSGF or EValue, otherwise filter by FDR</param>
        /// <param name="lstNormalizedPSMs">PSM results (keys are NormalizedSeqID, values are the protein and scan info for each normalized sequence)</param>
        /// <param name="lstSeqToProteinMap">Sequence to Protein map information (empty if the _resultToSeqMap file was not found)</param>
        /// <param name="lstSeqInfo">Sequence information (empty if the _resultToSeqMap file was not found)</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool FilterAndComputeStats(bool usingMSGFOrEValueFilter, IDictionary<int, clsPSMInfo> lstNormalizedPSMs,
            IDictionary<int, List<clsProteinInfo>> lstSeqToProteinMap, IDictionary<int, clsSeqInfo> lstSeqInfo)
        {
            var lstFilteredPSMs = new Dictionary<int, clsPSMInfo>();

            var blnSuccess = false;
            var blnFilterPSMs = true;

            // Make sure .PassesFilter is false for all of the observations
            foreach (var kvEntry in lstNormalizedPSMs)
            {
                foreach (var observation in kvEntry.Value.Observations)
                {
                    if (observation.PassesFilter)
                    {
                        observation.PassesFilter = false;
                    }
                }
            }

            if (usingMSGFOrEValueFilter)
            {
                if (ResultType == clsPHRPReader.ePeptideHitResultType.MSAlign)
                {
                    // Filter on EValue
                    blnSuccess = FilterPSMsByEValue(EValueThreshold, lstNormalizedPSMs, lstFilteredPSMs);
                }
                else if (MSGFThreshold < 1)
                {
                    // Filter on MSGF (though for MSPathFinder we're using SpecEValue)
                    blnSuccess = FilterPSMsByMSGF(MSGFThreshold, lstNormalizedPSMs, lstFilteredPSMs);
                }
                else
                {
                    // Do not filter
                    blnFilterPSMs = false;
                }
            }
            else
            {
                blnFilterPSMs = false;
            }

            if (!blnFilterPSMs)
            {
                // Keep all PSMs
                foreach (var kvEntry in lstNormalizedPSMs)
                {
                    foreach (var observation in kvEntry.Value.Observations)
                    {
                        observation.PassesFilter = true;
                    }
                    lstFilteredPSMs.Add(kvEntry.Key, kvEntry.Value);
                }
                blnSuccess = true;
            }

            if (!usingMSGFOrEValueFilter && FDRThreshold < 1)
            {
                // Filter on FDR (we'll compute the FDR using Reverse Proteins, if necessary)
                ReportDebugMessage("Call FilterPSMsByFDR", 3);

                blnSuccess = FilterPSMsByFDR(lstFilteredPSMs);

                ReportDebugMessage("FilterPSMsByFDR returned " + blnSuccess);

                foreach (var entry in lstFilteredPSMs)
                {
                    foreach (var observation in entry.Value.Observations)
                    {
                        if (observation.FDR > FDRThreshold)
                        {
                            observation.PassesFilter = false;
                        }
                    }
                }
            }

            if (blnSuccess)
            {
                // Summarize the results, counting the number of peptides, unique peptides, and proteins
                // We also count phosphopeptides using several metrics
                ReportDebugMessage("Call SummarizeResults for " + lstFilteredPSMs.Count + " Filtered PSMs", 3);

                blnSuccess = SummarizeResults(usingMSGFOrEValueFilter, lstFilteredPSMs, lstSeqToProteinMap, lstSeqInfo);

                ReportDebugMessage("SummarizeResults returned " + blnSuccess, 3);
            }

            return blnSuccess;
        }

        /// <summary>
        /// Filter the data using mFDRThreshold
        /// </summary>
        /// <param name="lstPSMs">PSM results (keys are NormalizedSeqID, values are the protein and scan info for each normalized sequence)</param>
        /// <returns>True if success; false if no reverse hits are present or if none of the data has MSGF values</returns>
        /// <remarks></remarks>
        private bool FilterPSMsByFDR(IDictionary<int, clsPSMInfo> lstPSMs)
        {
            var blnFDRAlreadyComputed = true;
            foreach (var kvEntry in lstPSMs)
            {
                if (kvEntry.Value.BestFDR < 0)
                {
                    blnFDRAlreadyComputed = false;
                    break;
                }
            }

            var lstResultIDtoFDRMap = new Dictionary<int, double>();
            if (blnFDRAlreadyComputed)
            {
                foreach (var kvEntry in lstPSMs)
                {
                    lstResultIDtoFDRMap.Add(kvEntry.Key, kvEntry.Value.BestFDR);
                }
            }
            else
            {
                // Sort the data by ascending SpecEValue, then step through the list and compute FDR
                // Use FDR = #Reverse / #Forward
                //
                // Alternative FDR formula is:  FDR = 2 * #Reverse / (#Forward + #Reverse)
                // But, since MSGF+ uses "#Reverse / #Forward" we'll use that here too
                //
                // If no reverse hits are present or if none of the data has MSGF values, we'll clear lstPSMs and update mErrorMessage

                // Populate a list with the MSGF values and ResultIDs so that we can step through the data and compute the FDR for each entry
                var lstMSGFtoResultIDMap = new List<KeyValuePair<double, int>>();

                var blnValidMSGFOrEValue = false;
                foreach (var kvEntry in lstPSMs)
                {
                    if (kvEntry.Value.BestMSGF < clsPSMInfo.UNKNOWN_MSGF_SPEC_EVALUE)
                    {
                        lstMSGFtoResultIDMap.Add(new KeyValuePair<double, int>(kvEntry.Value.BestMSGF, kvEntry.Key));
                        if (kvEntry.Value.BestMSGF < 1)
                            blnValidMSGFOrEValue = true;
                    }
                    else
                    {
                        lstMSGFtoResultIDMap.Add(new KeyValuePair<double, int>(kvEntry.Value.BestEValue, kvEntry.Key));
                        if (kvEntry.Value.BestEValue < clsPSMInfo.UNKNOWN_EVALUE)
                            blnValidMSGFOrEValue = true;
                    }
                }

                if (!blnValidMSGFOrEValue)
                {
                    // None of the data has MSGF values or E-Values; cannot compute FDR
                    SetErrorMessage("Data does not contain MSGF values or EValues; cannot compute a decoy-based FDR");
                    lstPSMs.Clear();
                    return false;
                }

                // Sort lstMSGFtoResultIDMap
                lstMSGFtoResultIDMap.Sort(new clsMSGFtoResultIDMapComparer());

                var intForwardResults = 0;
                var intDecoyResults = 0;
                var lstMissedResultIDsAtStart = new List<int>();

                foreach (var kvEntry in lstMSGFtoResultIDMap)
                {
                    var strProtein = lstPSMs[kvEntry.Value].Protein.ToLower();

                    // MTS reversed proteins                 'reversed[_]%'
                    // MTS scrambled proteins                'scrambled[_]%'
                    // X!Tandem decoy proteins               '%[:]reversed'
                    // Inspect reversed/scrambled proteins   'xxx.%'
                    // MSGF+ reversed proteins  'rev[_]%'

                    if (strProtein.StartsWith("reversed_") || strProtein.StartsWith("scrambled_") || strProtein.EndsWith(":reversed") ||
                        strProtein.StartsWith("xxx.") || strProtein.StartsWith("rev_"))
                    {
                        intDecoyResults += 1;
                    }
                    else
                    {
                        intForwardResults += 1;
                    }

                    if (intForwardResults > 0)
                    {
                        // Compute and store the FDR for this entry
                        var dblFDRThreshold = intDecoyResults / (float)intForwardResults;
                        lstResultIDtoFDRMap.Add(kvEntry.Value, dblFDRThreshold);

                        if (lstMissedResultIDsAtStart.Count > 0)
                        {
                            foreach (var intResultID in lstMissedResultIDsAtStart)
                            {
                                lstResultIDtoFDRMap.Add(intResultID, dblFDRThreshold);
                            }
                            lstMissedResultIDsAtStart.Clear();
                        }
                    }
                    else
                    {
                        // We cannot yet compute the FDR since all proteins up to this point are decoy proteins
                        // Update lstMissedResultIDsAtStart
                        lstMissedResultIDsAtStart.Add(kvEntry.Value);
                    }
                }

                if (intDecoyResults == 0)
                {
                    // We never encountered any decoy proteins; cannot compute FDR
                    OnWarningEvent("Data does not contain decoy proteins; cannot compute a decoy-based FDR");
                    lstPSMs.Clear();
                    return false;
                }
            }

            // Remove entries from lstPSMs where .FDR is larger than mFDRThreshold

            foreach (var kvEntry in lstResultIDtoFDRMap)
            {
                if (kvEntry.Value > FDRThreshold)
                {
                    lstPSMs.Remove(kvEntry.Key);
                }
            }

            return true;
        }

        private bool FilterPSMsByEValue(double dblEValueThreshold, IDictionary<int, clsPSMInfo> lstPSMs, IDictionary<int, clsPSMInfo> lstFilteredPSMs)
        {
            lstFilteredPSMs.Clear();

            var lstFilteredValues = from item in lstPSMs where item.Value.BestEValue <= dblEValueThreshold select item;

            foreach (var item in lstFilteredValues)
            {
                foreach (var observation in item.Value.Observations)
                {
                    observation.PassesFilter = observation.EValue <= dblEValueThreshold;
                }
                lstFilteredPSMs.Add(item.Key, item.Value);
            }

            return true;
        }

        private bool FilterPSMsByMSGF(double dblMSGFThreshold, IDictionary<int, clsPSMInfo> lstPSMs, IDictionary<int, clsPSMInfo> lstFilteredPSMs)
        {
            lstFilteredPSMs.Clear();

            var lstFilteredValues = from item in lstPSMs where item.Value.BestMSGF <= dblMSGFThreshold select item;

            foreach (var item in lstFilteredValues)
            {
                foreach (var observation in item.Value.Observations)
                {
                    observation.PassesFilter = observation.MSGF <= dblMSGFThreshold;
                }
                lstFilteredPSMs.Add(item.Key, item.Value);
            }

            return true;
        }

        /// <summary>
        /// Search dictNormalizedPeptides for an entry that either exactly matches normalizedPeptide
        /// or nearly matches normalizedPeptide
        /// </summary>
        /// <param name="dictNormalizedPeptides">Existing tracked normalized peptides; key is clean sequence, value is a list of normalized peptide info structs</param>
        /// <param name="newNormalizedPeptide">New normalized peptide</param>
        /// <returns>The Sequence ID of a matching normalized peptide, or -1 if no match</returns>
        /// <remarks>A near match is one where the position of each modified residue is the same or just one residue apart</remarks>
        public static int FindNormalizedSequence(IReadOnlyDictionary<string, List<clsNormalizedPeptideInfo>> dictNormalizedPeptides, clsNormalizedPeptideInfo newNormalizedPeptide)
        {
            // Find normalized peptides with the new normalized peptide's clean sequence

            if (!dictNormalizedPeptides.TryGetValue(newNormalizedPeptide.CleanSequence, out var normalizedPeptideCandidates))
            {
                return clsPSMInfo.UNKNOWN_SEQID;
            }

            // Step through the normalized peptides that correspond to newNormalizedPeptide.CleanSequence
            // Note that each candidate will have an empty CleanSequence value because of how they are stored in dictNormalizedPeptides

            foreach (var candidate in normalizedPeptideCandidates)
            {
                if (newNormalizedPeptide.Modifications.Count == 0 && candidate.Modifications.Count == 0)
                {
                    // Match found
                    return candidate.SeqID;
                }

                if (newNormalizedPeptide.Modifications.Count != candidate.Modifications.Count)
                {
                    // Mod counts do not match
                    continue;
                }

                var residueMatchCount = 0;
                for (var modIndex = 0; modIndex <= newNormalizedPeptide.Modifications.Count - 1; modIndex++)
                {
                    if (newNormalizedPeptide.Modifications[modIndex].Key != candidate.Modifications[modIndex].Key)
                    {
                        // Mod names do not match
                        break;
                    }

                    // Mod names do match
                    if (Math.Abs(newNormalizedPeptide.Modifications[modIndex].Value - candidate.Modifications[modIndex].Value) <= 1)
                    {
                        // The affected residues are at the same index or are one index apart
                        residueMatchCount += 1;
                    }
                }

                if (residueMatchCount == candidate.Modifications.Count)
                {
                    // Match found
                    return candidate.SeqID;
                }
            }

            return clsPSMInfo.UNKNOWN_SEQID;
        }

        private clsPHRPStartupOptions GetMinimalMemoryPHRPStartupOptions()
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

        /// <summary>
        /// Get the RegEx for matching keratin proteins
        /// </summary>
        /// <returns></returns>
        /// <remarks>Used by SMAQC</remarks>
        public static Regex GetKeratinRegEx()
        {
            // Regex to match keratin proteins, including
            //   K1C9_HUMAN, K1C10_HUMAN, K1CI_HUMAN
            //   K2C1_HUMAN, K2C1B_HUMAN, K2C3_HUMAN, K2C6C_HUMAN, K2C71_HUMAN
            //   K22E_HUMAN And K22O_HUMAN
            //   Contaminant_K2C1_HUMAN
            //   Contaminant_K22E_HUMAN
            //   Contaminant_K1C9_HUMAN
            //   Contaminant_K1C10_HUMAN
            return new Regex(@"(K[1-2]C\d+[A-K]*|K22[E,O]|K1CI)_HUMAN", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        }

        /// <summary>
        /// Get the RegEx for matching trypsin proteins
        /// </summary>
        /// <returns></returns>
        /// <remarks>Used by SMAQC</remarks>
        public static Regex GetTrypsinRegEx()
        {
            // Regex to match trypsin proteins, including
            //   TRYP_PIG, sp|TRYP_PIG, Contaminant_TRYP_PIG, Cntm_P00761|TRYP_PIG
            //   Contaminant_TRYP_BOVIN And gi|136425|sp|P00760|TRYP_BOVIN
            //   Contaminant_Trypa
            return new Regex(@"(TRYP_(PIG|BOVIN)|Contaminant_Trypa)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        }

        public static clsNormalizedPeptideInfo GetNormalizedPeptideInfo(string peptideCleanSequence,
            IEnumerable<KeyValuePair<string, int>> modifications, int seqID)
        {
            var normalizedPeptide = new clsNormalizedPeptideInfo(peptideCleanSequence);
            normalizedPeptide.StoreModifications(modifications);
            normalizedPeptide.SeqID = seqID;

            return normalizedPeptide;
        }

        private bool PostJobPSMResults(int intJob)
        {
            const int MAX_RETRY_COUNT = 3;

            bool blnSuccess;

            try
            {
                // Call stored procedure StoreJobPSMStats in DMS5

                var objCommand = new SqlCommand(STORE_JOB_PSM_RESULTS_SP_NAME) { CommandType = CommandType.StoredProcedure };

                objCommand.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                objCommand.Parameters.Add(new SqlParameter("@Job", SqlDbType.Int)).Value = intJob;

                objCommand.Parameters.Add(new SqlParameter("@MSGFThreshold", SqlDbType.Float));
                if (ResultType == clsPHRPReader.ePeptideHitResultType.MSAlign)
                {
                    objCommand.Parameters["@MSGFThreshold"].Value = EValueThreshold;
                }
                else
                {
                    objCommand.Parameters["@MSGFThreshold"].Value = MSGFThreshold;
                }

                objCommand.Parameters.Add(new SqlParameter("@FDRThreshold", SqlDbType.Float)).Value = FDRThreshold;
                objCommand.Parameters.Add(new SqlParameter("@SpectraSearched", SqlDbType.Int)).Value = mSpectraSearched;
                objCommand.Parameters.Add(new SqlParameter("@TotalPSMs", SqlDbType.Int)).Value = mMSGFBasedCounts.TotalPSMs;
                objCommand.Parameters.Add(new SqlParameter("@UniquePeptides", SqlDbType.Int)).Value = mMSGFBasedCounts.UniquePeptideCount;
                objCommand.Parameters.Add(new SqlParameter("@UniqueProteins", SqlDbType.Int)).Value = mMSGFBasedCounts.UniqueProteinCount;
                objCommand.Parameters.Add(new SqlParameter("@TotalPSMsFDRFilter", SqlDbType.Int)).Value = mFDRBasedCounts.TotalPSMs;
                objCommand.Parameters.Add(new SqlParameter("@UniquePeptidesFDRFilter", SqlDbType.Int)).Value = mFDRBasedCounts.UniquePeptideCount;
                objCommand.Parameters.Add(new SqlParameter("@UniqueProteinsFDRFilter", SqlDbType.Int)).Value = mFDRBasedCounts.UniqueProteinCount;

                objCommand.Parameters.Add(new SqlParameter("@MSGFThresholdIsEValue", SqlDbType.TinyInt));
                if (ResultType == clsPHRPReader.ePeptideHitResultType.MSAlign)
                {
                    objCommand.Parameters["@MSGFThresholdIsEValue"].Value = 1;
                }
                else
                {
                    objCommand.Parameters["@MSGFThresholdIsEValue"].Value = 0;
                }

                objCommand.Parameters.Add(new SqlParameter("@PercentMSnScansNoPSM", SqlDbType.Real)).Value = mPercentMSnScansNoPSM;
                objCommand.Parameters.Add(new SqlParameter("@MaximumScanGapAdjacentMSn", SqlDbType.Int)).Value = MaximumScanGapAdjacentMSn;
                objCommand.Parameters.Add(new SqlParameter("@UniquePhosphopeptideCountFDR", SqlDbType.Int)).Value = mFDRBasedCounts.UniquePhosphopeptideCount;
                objCommand.Parameters.Add(new SqlParameter("@UniquePhosphopeptidesCTermK", SqlDbType.Int)).Value = mFDRBasedCounts.UniquePhosphopeptidesCTermK;
                objCommand.Parameters.Add(new SqlParameter("@UniquePhosphopeptidesCTermR", SqlDbType.Int)).Value = mFDRBasedCounts.UniquePhosphopeptidesCTermR;
                objCommand.Parameters.Add(new SqlParameter("@MissedCleavageRatio", SqlDbType.Real)).Value = mFDRBasedCounts.MissedCleavageRatio;
                objCommand.Parameters.Add(new SqlParameter("@MissedCleavageRatioPhospho", SqlDbType.Real)).Value = mFDRBasedCounts.MissedCleavageRatioPhospho;
                objCommand.Parameters.Add(new SqlParameter("@TrypticPeptides", SqlDbType.Int)).Value = mFDRBasedCounts.TrypticPeptides;
                objCommand.Parameters.Add(new SqlParameter("@KeratinPeptides", SqlDbType.Int)).Value = mFDRBasedCounts.KeratinPeptides;
                objCommand.Parameters.Add(new SqlParameter("@TrypsinPeptides", SqlDbType.Int)).Value = mFDRBasedCounts.TrypsinPeptides;

                // Execute the SP (retry the call up to 3 times)
                var result = mStoredProcedureExecutor.ExecuteSP(objCommand, MAX_RETRY_COUNT, out var strErrorMessage);

                if (result == 0)
                {
                    blnSuccess = true;
                }
                else
                {
                    SetErrorMessage("Error storing PSM Results in database, " + STORE_JOB_PSM_RESULTS_SP_NAME + " returned " + result);
                    if (!string.IsNullOrEmpty(strErrorMessage))
                    {
                        mErrorMessage += "; " + strErrorMessage;
                    }
                    blnSuccess = false;
                }
            }
            catch (Exception ex)
            {
                SetErrorMessage("Exception storing PSM Results in database: " + ex.Message);
                blnSuccess = false;
            }

            return blnSuccess;
        }

        /// <summary>
        /// Process this dataset's synopsis file to determine the PSM stats
        /// </summary>
        /// <returns>True if success; false if an error</returns>
        /// <remarks></remarks>
        public bool ProcessMSGFResults()
        {

            DatasetScanStatsLookupError = false;

            try
            {
                mErrorMessage = string.Empty;
                mSpectraSearched = 0;
                mMSGFBasedCounts.Clear();
                mFDRBasedCounts.Clear();

                /////////////////////
                // Define the file paths
                //
                // We use the First-hits file to determine the number of MS/MS spectra that were searched (unique combo of charge and scan number)
                var strPHRPFirstHitsFileName = clsPHRPReader.GetPHRPFirstHitsFileName(ResultType, mDatasetName);

                // We use the Synopsis file to count the number of peptides and proteins observed
                var strPHRPSynopsisFileName = clsPHRPReader.GetPHRPSynopsisFileName(ResultType, mDatasetName);

                if (ResultType == clsPHRPReader.ePeptideHitResultType.XTandem || ResultType == clsPHRPReader.ePeptideHitResultType.MSAlign ||
                    ResultType == clsPHRPReader.ePeptideHitResultType.MODa || ResultType == clsPHRPReader.ePeptideHitResultType.MODPlus ||
                    ResultType == clsPHRPReader.ePeptideHitResultType.MSPathFinder)
                {
                    // These tools do not have first-hits files; use the Synopsis file instead to determine scan counts
                    strPHRPFirstHitsFileName = strPHRPSynopsisFileName;
                }

                mMSGFSynopsisFileName = Path.GetFileNameWithoutExtension(strPHRPSynopsisFileName) + MSGF_RESULT_FILENAME_SUFFIX;

                var strPHRPFirstHitsFilePath = Path.Combine(mWorkDir, strPHRPFirstHitsFileName);
                var strPHRPSynopsisFilePath = Path.Combine(mWorkDir, strPHRPSynopsisFileName);

                if (!File.Exists(strPHRPSynopsisFilePath))
                {
                    SetErrorMessage("File not found: " + strPHRPSynopsisFilePath);
                    return false;
                }

                /////////////////////
                // Determine the number of MS/MS spectra searched
                //
                if (File.Exists(strPHRPFirstHitsFilePath))
                {
                    ExamineFirstHitsFile(strPHRPFirstHitsFilePath);
                }

                ////////////////////
                // Load the PSMs and sequence info
                //

                // The keys in this dictionary are NormalizedSeqID values, which are custom-assigned
                // by this class to keep track of peptide sequences on a basis where modifications are tracked with some wiggle room
                // For example, LS*SPATLNSR and LSS*PATLNSR are considered equivalent
                // But P#EPT*IDES and PEP#T*IDES and P#EPTIDES* are all different
                //
                // The values contain mapped protein name, FDR, and MSGF SpecEValue, and the scans that the normalized peptide was observed in
                // We'll deal with multiple proteins for each peptide later when we parse the _ResultToSeqMap.txt and _SeqToProteinMap.txt files
                // If those files are not found, we'll simply use the protein information stored in lstPSMs
                var lstNormalizedPSMs = new Dictionary<int, clsPSMInfo>();


                var psmsLoaded = LoadPSMs(strPHRPSynopsisFilePath, lstNormalizedPSMs, out _, out var lstSeqToProteinMap, out var lstSeqInfo);
                if (!psmsLoaded)
                {
                    return false;
                }

                ////////////////////
                // Filter on MSGF or EValue and compute the stats
                //
                var usingMSGFOrEValueFilter = true;
                ReportDebugMessage("Call FilterAndComputeStats with blnUsingMSGFOrEValueFilter = true", 3);

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                var success = FilterAndComputeStats(usingMSGFOrEValueFilter, lstNormalizedPSMs, lstSeqToProteinMap, lstSeqInfo);

                ReportDebugMessage("FilterAndComputeStats returned " + success, 3);

                ////////////////////
                // Filter on FDR and compute the stats
                //
                usingMSGFOrEValueFilter = false;
                ReportDebugMessage("Call FilterAndComputeStats with blnUsingMSGFOrEValueFilter = false", 3);

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                var successViaFDR = FilterAndComputeStats(usingMSGFOrEValueFilter, lstNormalizedPSMs, lstSeqToProteinMap, lstSeqInfo);

                ReportDebugMessage("FilterAndComputeStats returned " + success, 3);

                if (!(success || successViaFDR))
                    return false;

                if (SaveResultsToTextFile)
                {
                    // Note: Continue processing even if this step fails
                    SaveResultsToFile();
                }

                if (!PostJobPSMResultsToDB)
                    return true;

                if (ContactDatabase)
                {
                    ReportDebugMessage("Call PostJobPSMResults for job " + mJob);

                    var psmResultsPosted = PostJobPSMResults(mJob);

                    ReportDebugMessage("PostJobPSMResults returned " + psmResultsPosted);

                    return psmResultsPosted;
                }

                SetErrorMessage("Cannot post results to the database because ContactDatabase is False");
                return false;
            }
            catch (Exception ex)
            {
                SetErrorMessage(ex.Message);
                Console.WriteLine(ex.StackTrace);
                return false;
            }
        }

        /// <summary>
        /// Loads the PSMs (peptide identification for each scan)
        /// Normalizes the peptide sequence (mods are tracked, but no longer associated with specific residues) and populates lstNormalizedPSMs
        /// </summary>
        /// <param name="strPHRPSynopsisFilePath"></param>
        /// <param name="lstNormalizedPSMs"></param>
        /// <param name="lstResultToSeqMap"></param>
        /// <param name="lstSeqToProteinMap"></param>
        /// <param name="lstSeqInfo"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool LoadPSMs(string strPHRPSynopsisFilePath, IDictionary<int, clsPSMInfo> lstNormalizedPSMs,
            out SortedList<int, int> lstResultToSeqMap, out SortedList<int, List<clsProteinInfo>> lstSeqToProteinMap,
            out SortedList<int, clsSeqInfo> lstSeqInfo)
        {
            var specEValue = clsPSMInfo.UNKNOWN_MSGF_SPEC_EVALUE;
            var eValue = clsPSMInfo.UNKNOWN_EVALUE;

            bool blnSuccess;

            var blnLoadMSGFResults = true;

            // Regex for determining that a peptide has a missed cleavage (i.e. an internal tryptic cleavage point)
            var reMissedCleavage = new Regex(@"[KR][^P][A-Z]", RegexOptions.Compiled);

            // Regex to match keratin proteins
            var reKeratinProtein = GetKeratinRegEx();

            // Regex to match trypsin proteins
            var reTrypsinProtein = GetTrypsinRegEx();

            lstResultToSeqMap = new SortedList<int, int>();
            lstSeqToProteinMap = new SortedList<int, List<clsProteinInfo>>();
            lstSeqInfo = new SortedList<int, clsSeqInfo>();

            try
            {
                if (ResultType == clsPHRPReader.ePeptideHitResultType.MODa || ResultType == clsPHRPReader.ePeptideHitResultType.MODPlus ||
                    ResultType == clsPHRPReader.ePeptideHitResultType.MSPathFinder)
                {
                    blnLoadMSGFResults = false;
                }

                // Note that this will set .LoadModsAndSeqInfo to false
                // That is fine because we will have access to the modification info from the _SeqInfo.txt file
                // Since we're looking for trypsin and keratin proteins, we need to change MaxProteinsPerPSM back to a large number
                var startupOptions = GetMinimalMemoryPHRPStartupOptions();
                startupOptions.LoadMSGFResults = blnLoadMSGFResults;
                startupOptions.MaxProteinsPerPSM = 1000;

                // Load the result to sequence mapping, sequence IDs, and protein information
                // This also loads the mod description, which we use to determine if a peptide is a phosphopeptide
                var seqMapReader = new clsPHRPSeqMapReader(mDatasetName, mWorkDir, ResultType);

                var sequenceInfoAvailable = false;

                if (!string.IsNullOrEmpty(seqMapReader.ResultToSeqMapFilename))
                {
                    var fiResultToSeqMapFile = new FileInfo(Path.Combine(seqMapReader.InputDirectoryPath, seqMapReader.ResultToSeqMapFilename));

                    if (fiResultToSeqMapFile.Exists)
                    {
                        blnSuccess = seqMapReader.GetProteinMapping(out lstResultToSeqMap, out lstSeqToProteinMap, out lstSeqInfo);

                        if (!blnSuccess)
                        {
                            if (string.IsNullOrEmpty(seqMapReader.ErrorMessage))
                            {
                                SetErrorMessage("GetProteinMapping returned false: unknown error");
                            }
                            else
                            {
                                SetErrorMessage("GetProteinMapping returned false: " + seqMapReader.ErrorMessage);
                            }

                            return false;
                        }

                        sequenceInfoAvailable = true;
                    }
                }

                // Keys in this dictionary are clean sequences (peptide sequence without any mod symbols)
                // Values are lists of modified residue combinations that correspond to the given clean sequence
                // Each combination of residues has a corresponding "best" SeqID associated with it
                //
                // When comparing a new sequence to entries in this dictionary, if the mod locations are all within one residue of an existing normalized sequence,
                //  the new sequence and mods is not added
                // For example, LS*SPATLNSR and LSS*PATLNSR are considered equivalent, and will be tracked as LSSPATLNSR with * at index 1
                // But P#EPT*IDES and PEP#T*IDES and P#EPTIDES* are all different, and are tracked with entries:
                //  PEPTIDES with # at index 0 and * at index 3
                //  PEPTIDES with # at index 2 and * at index 3
                //  PEPTIDES with # at index 0 and * at index 7
                //
                // If sequenceInfoAvailable is True, instead of using mod symbols we use ModNames from the Mod_Description column in the _SeqInfo.txt file
                //   For example, VGVEASEETPQT with Phosph at index 5
                //
                // The SeqID value tracked by udtNormalizedPeptideType is the SeqID of the first sequence to get normalized to the given entry
                // If sequenceInfoAvailable is False, values are the ResultID value of the first peptide to get normalized to the given entry
                //
                var dictNormalizedPeptides = new Dictionary<string, List<clsNormalizedPeptideInfo>>();

                using (var reader = new clsPHRPReader(strPHRPSynopsisFilePath, startupOptions))
                {
                    RegisterEvents(reader);

                    while (reader.MoveNext())
                    {
                        var objPSM = reader.CurrentPSM;

                        if (objPSM.ScoreRank > 1)
                        {
                            // Only keep the first match for each spectrum
                            continue;
                        }

                        var valid = false;

                        if (ResultType == clsPHRPReader.ePeptideHitResultType.MSAlign)
                        {
                            // Use the EValue reported by MSAlign

                            if (objPSM.TryGetScore("EValue", out var eValueText))
                            {
                                valid = double.TryParse(eValueText, out eValue);
                            }
                        }
                        else if (ResultType == clsPHRPReader.ePeptideHitResultType.MODa | ResultType == clsPHRPReader.ePeptideHitResultType.MODPlus)
                        {
                            // MODa / MODPlus results don't have spectral probability, but they do have FDR
                            valid = true;
                        }
                        else if (ResultType == clsPHRPReader.ePeptideHitResultType.MSPathFinder)
                        {
                            // Use SpecEValue in place of SpecProb
                            valid = true;

                            if (objPSM.TryGetScore(clsPHRPParserMSPathFinder.DATA_COLUMN_SpecEValue, out var specEValueText))
                            {
                                if (!string.IsNullOrWhiteSpace(specEValueText))
                                {
                                    valid = double.TryParse(specEValueText, out specEValue);
                                }
                            }

                            // SpecEValue was not present
                            // That's OK, QValue should be present
                        }
                        else
                        {
                            valid = double.TryParse(objPSM.MSGFSpecEValue, out specEValue);
                        }

                        if (!valid)
                        {
                            continue;
                        }

                        // Store in lstNormalizedPSMs

                        var psmInfo = new clsPSMInfo();
                        psmInfo.Clear();

                        psmInfo.Protein = objPSM.ProteinFirst;

                        var psmMSGF = specEValue;
                        var psmEValue = eValue;
                        double psmFDR;

                        if (ResultType == clsPHRPReader.ePeptideHitResultType.MSGFPlus | ResultType == clsPHRPReader.ePeptideHitResultType.MSAlign)
                        {
                            psmFDR = objPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_FDR, clsPSMInfo.UNKNOWN_FDR);
                            if (psmFDR < 0)
                            {
                                psmFDR = objPSM.GetScoreDbl(clsPHRPParserMSGFDB.DATA_COLUMN_EFDR, clsPSMInfo.UNKNOWN_FDR);
                            }
                        }
                        else if (ResultType == clsPHRPReader.ePeptideHitResultType.MODa)
                        {
                            psmFDR = objPSM.GetScoreDbl(clsPHRPParserMODa.DATA_COLUMN_QValue, clsPSMInfo.UNKNOWN_FDR);
                        }
                        else if (ResultType == clsPHRPReader.ePeptideHitResultType.MODPlus)
                        {
                            psmFDR = objPSM.GetScoreDbl(clsPHRPParserMODPlus.DATA_COLUMN_QValue, clsPSMInfo.UNKNOWN_FDR);
                        }
                        else if (ResultType == clsPHRPReader.ePeptideHitResultType.MSPathFinder)
                        {
                            psmFDR = objPSM.GetScoreDbl(clsPHRPParserMSPathFinder.DATA_COLUMN_QValue, clsPSMInfo.UNKNOWN_FDR);
                        }
                        else
                        {
                            psmFDR = clsPSMInfo.UNKNOWN_FDR;
                        }

                        var normalizedPeptide = new clsNormalizedPeptideInfo(string.Empty);

                        var normalized = false;
                        var seqID = clsPSMInfo.UNKNOWN_SEQID;

                        if (sequenceInfoAvailable && lstResultToSeqMap != null)
                        {
                            if (!lstResultToSeqMap.TryGetValue(objPSM.ResultID, out seqID))
                            {
                                seqID = clsPSMInfo.UNKNOWN_SEQID;

                                // This result is not listed in the _ResultToSeqMap file, likely because it was already processed for this scan
                                // Look for a match in dictNormalizedPeptides that matches this peptide's clean sesquence

                                if (dictNormalizedPeptides.TryGetValue(objPSM.PeptideCleanSequence, out var normalizedPeptides))
                                {
                                    foreach (var udtNormalizedItem in normalizedPeptides)
                                    {
                                        if (udtNormalizedItem.SeqID >= 0)
                                        {
                                            // Match found; use the given SeqID value
                                            seqID = udtNormalizedItem.SeqID;
                                            break;
                                        }
                                    }
                                }
                            }

                            if (seqID != clsPSMInfo.UNKNOWN_SEQID)
                            {
                                if (lstSeqInfo.TryGetValue(seqID, out var oSeqInfo))
                                {
                                    normalizedPeptide = NormalizeSequence(objPSM.PeptideCleanSequence, oSeqInfo, seqID);
                                    normalized = true;
                                }
                            }
                        }

                        if (!normalized)
                        {
                            normalizedPeptide = NormalizeSequence(objPSM.Peptide, seqID);
                        }

                        var normalizedSeqID = FindNormalizedSequence(dictNormalizedPeptides, normalizedPeptide);

                        if (normalizedSeqID != clsPSMInfo.UNKNOWN_SEQID)
                        {
                            // We're already tracking this normalized peptide (or one very similar to it)

                            var normalizedPSMInfo = lstNormalizedPSMs[normalizedSeqID];
                            var addObservation = true;

                            foreach (var observation in normalizedPSMInfo.Observations)
                            {
                                if (observation.Scan == objPSM.ScanNumber)
                                {
                                    // Scan already stored

                                    // Update the scores if this PSM has a better score than the cached one
                                    if (psmFDR > clsPSMInfo.UNKNOWN_FDR)
                                    {
                                        if (psmFDR < observation.FDR)
                                        {
                                            observation.FDR = psmFDR;
                                        }
                                    }

                                    if (psmMSGF < observation.MSGF)
                                    {
                                        observation.MSGF = psmMSGF;
                                    }

                                    if (psmEValue < observation.EValue)
                                    {
                                        observation.EValue = psmEValue;
                                    }

                                    addObservation = false;
                                    break;
                                }
                            }

                            if (addObservation)
                            {
                                var observation = new clsPSMInfo.PSMObservation
                                {
                                    Scan = objPSM.ScanNumber,
                                    FDR = psmFDR,
                                    MSGF = psmMSGF,
                                    EValue = psmEValue
                                };

                                normalizedPSMInfo.Observations.Add(observation);
                            }
                        }
                        else
                        {
                            // New normalized sequence
                            // SeqID will typically come from the ResultToSeqMap file
                            // But, if that file is not available, we use the ResultID of the peptide

                            if (seqID == clsPSMInfo.UNKNOWN_SEQID)
                            {
                                seqID = objPSM.ResultID;
                            }


                            if (!dictNormalizedPeptides.TryGetValue(normalizedPeptide.CleanSequence, out var normalizedPeptides))
                            {
                                normalizedPeptides = new List<clsNormalizedPeptideInfo>();
                                dictNormalizedPeptides.Add(normalizedPeptide.CleanSequence, normalizedPeptides);
                            }

                            // Make a new normalized peptide entry that does not have clean sequence
                            // (to conserve memory, since keys in dictionary normalizedPeptides are clean sequence)
                            var normalizedPeptideToStore = new clsNormalizedPeptideInfo(string.Empty);
                            normalizedPeptideToStore.StoreModifications(normalizedPeptide.Modifications);
                            normalizedPeptideToStore.SeqID = seqID;

                            normalizedPeptides.Add(normalizedPeptideToStore);

                            psmInfo.SeqIdFirst = seqID;

                            var lastResidue = normalizedPeptide.CleanSequence[normalizedPeptide.CleanSequence.Length - 1];
                            if (lastResidue == 'K')
                            {
                                psmInfo.CTermK = true;
                            }
                            else if (lastResidue == 'R')
                            {
                                psmInfo.CTermR = true;
                            }

                            // Check whether this peptide has a missed cleavage
                            // This only works for Trypsin
                            if (objPSM.NumMissedCleavages > 0)
                            {
                                psmInfo.MissedCleavage = true;
                            }
                            else if (reMissedCleavage.IsMatch(normalizedPeptide.CleanSequence))
                            {
                                Console.WriteLine("NumMissedCleavages is zero but the peptide matches the MissedCleavage Regex; this is unexpected");
                                psmInfo.MissedCleavage = true;
                            }

                            // Check whether this peptide is from Keratin or a related protein
                            foreach (var proteinName in objPSM.Proteins)
                            {
                                if (reKeratinProtein.IsMatch(proteinName))
                                {
                                    psmInfo.KeratinPeptide = true;
                                    break;
                                }
                            }

                            // Check whether this peptide is from Trypsin or a related protein
                            foreach (var proteinName in objPSM.Proteins)
                            {
                                if (reTrypsinProtein.IsMatch(proteinName))
                                {
                                    psmInfo.TrypsinPeptide = true;
                                    break;
                                }
                            }

                            // Check whether this peptide is partially or fully tryptic
                            if (objPSM.CleavageState == clsPeptideCleavageStateCalculator.ePeptideCleavageStateConstants.Full ||
                                objPSM.CleavageState == clsPeptideCleavageStateCalculator.ePeptideCleavageStateConstants.Partial)
                            {
                                psmInfo.Tryptic = true;
                            }

                            // Check whether this is a phosphopeptide
                            // This check only works if the _ModSummary.txt file was loaded because it relies on the mod name being Phosph
                            foreach (var modification in normalizedPeptide.Modifications)
                            {
                                if (string.Equals(modification.Key, "Phosph", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    psmInfo.Phosphopeptide = true;
                                    break;
                                }
                            }

                            var observation = new clsPSMInfo.PSMObservation
                            {
                                Scan = objPSM.ScanNumber,
                                FDR = psmFDR,
                                MSGF = psmMSGF,
                                EValue = psmEValue
                            };

                            psmInfo.AddObservation(observation);

                            if (lstNormalizedPSMs.ContainsKey(seqID))
                            {
                                Console.WriteLine("Warning: Duplicate key, seqID=" + seqID + "; skipping PSM with ResultID=" + objPSM.ResultID);
                            }
                            else
                            {
                                lstNormalizedPSMs.Add(seqID, psmInfo);
                            }
                        }
                    }
                }

                blnSuccess = true;
            }
            catch (Exception ex)
            {
                SetErrorMessage(ex.Message);
                Console.WriteLine(ex.StackTrace);
                blnSuccess = false;
            }

            return blnSuccess;
        }

        /// <summary>
        /// Parse a sequence with mod symbols
        /// </summary>
        /// <param name="sequenceWithMods"></param>
        /// <param name="seqID"></param>
        /// <returns></returns>
        private clsNormalizedPeptideInfo NormalizeSequence(string sequenceWithMods, int seqID)
        {
            var sbAminoAcids = new StringBuilder(sequenceWithMods.Length);
            var modList = new List<KeyValuePair<string, int>>();

            clsPeptideCleavageStateCalculator.SplitPrefixAndSuffixFromSequence(sequenceWithMods, out var strPrimarySequence, out _, out _);

            for (var index = 0; index <= strPrimarySequence.Length - 1; index++)
            {
                if (clsPHRPReader.IsLetterAtoZ(strPrimarySequence[index]))
                {
                    sbAminoAcids.Append(strPrimarySequence[index]);
                }
                else
                {
                    modList.Add(new KeyValuePair<string, int>(strPrimarySequence[index].ToString(), index));
                }
            }

            return GetNormalizedPeptideInfo(sbAminoAcids.ToString(), modList, seqID);
        }

        private clsNormalizedPeptideInfo NormalizeSequence(string peptideCleanSequence, clsSeqInfo oSeqInfo, int seqID)
        {
            var modList = new List<KeyValuePair<string, int>>();

            if (!string.IsNullOrWhiteSpace(oSeqInfo.ModDescription))
            {
                // Parse the modifications

                var lstMods = oSeqInfo.ModDescription.Split(',');
                foreach (var modDescriptor in lstMods)
                {
                    var colonIndex = modDescriptor.IndexOf(':');
                    string modName;
                    var modIndex = 0;

                    if (colonIndex > 0)
                    {
                        modName = modDescriptor.Substring(0, colonIndex);
                        int.TryParse(modDescriptor.Substring(colonIndex + 1), out modIndex);
                    }
                    else
                    {
                        modName = modDescriptor;
                    }

                    if (string.IsNullOrWhiteSpace(modName))
                    {
                        // Empty mod name; this is unexpected
                        throw new Exception(string.Format("Empty mod name parsed from the ModDescription for SeqID {0}: {1}", oSeqInfo.SeqID, oSeqInfo.ModDescription));
                    }

                    modList.Add(new KeyValuePair<string, int>(modName, modIndex));
                }
            }

            return GetNormalizedPeptideInfo(peptideCleanSequence, modList, seqID);
        }

        private void ReportDebugMessage(string message, int debugLevel = 2)
        {
            if (mDebugLevel >= debugLevel)
                OnDebugEvent(message);
            else
            {
                ConsoleMsgUtils.ShowDebug(message);
            }
        }

        private void SaveResultsToFile()
        {
            var strOutputFilePath = "??";

            try
            {
                if (!string.IsNullOrEmpty(OutputFolderPath))
                {
                    strOutputFilePath = OutputFolderPath;
                }
                else
                {
                    strOutputFilePath = mWorkDir;
                }
                strOutputFilePath = Path.Combine(strOutputFilePath, mDatasetName + "_PSM_Stats.txt");

                using (var writer = new StreamWriter(new FileStream(strOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Header line
                    writer.WriteLine(
                        "Dataset\t" +
                        "Job\t" +
                        "MSGF_Threshold\t" +
                        "FDR_Threshold\t" +
                        "Spectra_Searched\t" +
                        "Total_PSMs_MSGF_Filtered\t" +
                        "Unique_Peptides_MSGF_Filtered\t" +
                        "Unique_Proteins_MSGF_Filtered\t" +
                        "Total_PSMs_FDR_Filtered\t" +
                        "Unique_Peptides_FDR_Filtered\t" +
                        "Unique_Proteins_FDR_Filtered");

                    // Stats
                    writer.WriteLine(mDatasetName + "\t" + mJob + "\t" + MSGFThreshold.ToString("0.00E+00") + "\t" +
                                        FDRThreshold.ToString("0.000") + "\t" + mSpectraSearched + "\t" + mMSGFBasedCounts.TotalPSMs + "\t" +
                                        mMSGFBasedCounts.UniquePeptideCount + "\t" + mMSGFBasedCounts.UniqueProteinCount + "\t" +
                                        mFDRBasedCounts.TotalPSMs + "\t" + mFDRBasedCounts.UniquePeptideCount + "\t" +
                                        mFDRBasedCounts.UniqueProteinCount);
                }
            }
            catch (Exception ex)
            {
                SetErrorMessage("Exception saving results to " + strOutputFilePath + ": " + ex.Message);
            }

        }

        private void SetErrorMessage(string errMsg)
        {
            Console.WriteLine(errMsg);
            mErrorMessage = errMsg;
            OnErrorEvent(errMsg);
        }

        /// <summary>
        /// Summarize the results by inter-relating lstFilteredPSMs, lstResultToSeqMap, and lstSeqToProteinMap
        /// </summary>
        /// <param name="blnUsingMSGFOrEValueFilter"></param>
        /// <param name="lstFilteredPSMs">Filter-passing results (keys are NormalizedSeqID, values are the protein and scan info for each normalized sequence)</param>
        /// <param name="lstSeqToProteinMap">Sequence to protein map (keys are sequence ID, values are proteins)</param>
        /// <param name="lstSeqInfo">Sequence information (keys are sequence ID, values are sequences</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool SummarizeResults(
            bool blnUsingMSGFOrEValueFilter, IDictionary<int, clsPSMInfo> lstFilteredPSMs,
            IDictionary<int, List<clsProteinInfo>> lstSeqToProteinMap, IDictionary<int, clsSeqInfo> lstSeqInfo)
        {
            try
            {
                // The Keys in this dictionary are SeqID values; the values track observation count, whether the peptide ends in K or R, etc.
                // Populated from data in lstFilteredPSMs, where peptides with the same sequence and same modifications (+/- 1 residue) were collapsed
                // For example, LS*SPATLNSR and LSS*PATLNSR are considered equivalent
                // But P#EPT*IDES and PEP#T*IDES and P#EPTIDES* are all different
                // (the collapsing of similar peptides is done in method LoadPSMs with the call to FindNormalizedSequence)
                var lstUniqueSequences = new Dictionary<int, clsUniqueSeqInfo>();

                // The Keys in this dictionary are SeqID values; the values track observation count, whether the peptide ends in K or R, etc.
                var lstUniquePhosphopeptides = new Dictionary<int, clsUniqueSeqInfo>();

                // The Keys in this dictionary are protein names; the values are observation count
                var lstUniqueProteins = new Dictionary<string, int>();

                foreach (var result in lstFilteredPSMs)
                {
                    var observations = result.Value.Observations;
                    var obsCountForResult = (from item in observations where item.PassesFilter select item).Count();

                    if (obsCountForResult == 0)
                    {
                        continue;
                    }

                    // If lstResultToSeqMap has data, the keys in lstFilteredPSMs are SeqID values
                    // Otherwise, the keys are ResultID values
                    var intSeqID = result.Key;

                    // Make a deep copy of result.Value as class clsUniqueSeqInfo
                    var seqInfoToStore = result.Value.CloneAsSeqInfo(obsCountForResult);

                    AddUpdateUniqueSequence(lstUniqueSequences, intSeqID, seqInfoToStore);

                    if (result.Value.Phosphopeptide)
                    {
                        AddUpdateUniqueSequence(lstUniquePhosphopeptides, intSeqID, seqInfoToStore);
                    }

                    var addResultProtein = true;


                    if (lstSeqInfo.Count > 0 && lstSeqInfo.TryGetValue(intSeqID, out _))
                    {
                        // Lookup the proteins for this peptide
                        if (lstSeqToProteinMap.TryGetValue(intSeqID, out var lstProteins))
                        {
                            // Update the observation count for each protein
                            foreach (var objProtein in lstProteins)
                            {
                                if (lstUniqueProteins.TryGetValue(objProtein.ProteinName, out var obsCountOverall))
                                {
                                    lstUniqueProteins[objProtein.ProteinName] = obsCountOverall + obsCountForResult;
                                }
                                else
                                {
                                    lstUniqueProteins.Add(objProtein.ProteinName, obsCountForResult);
                                }

                                // Protein match found; we can ignore result.Value.Protein
                                addResultProtein = false;
                            }
                        }
                    }

                    if (addResultProtein)
                    {
                        var proteinName = result.Value.Protein;

                        if (lstUniqueProteins.TryGetValue(proteinName, out var obsCountOverall))
                        {
                            lstUniqueProteins[proteinName] = obsCountOverall + obsCountForResult;
                        }
                        else
                        {
                            lstUniqueProteins.Add(proteinName, obsCountForResult);
                        }
                    }
                }

                // Obtain the stats to store
                var psmStats = TabulatePSMStats(lstUniqueSequences, lstUniqueProteins, lstUniquePhosphopeptides);

                if (blnUsingMSGFOrEValueFilter)
                {
                    mMSGFBasedCounts = psmStats;
                }
                else
                {
                    mFDRBasedCounts = psmStats;
                }
            }
            catch (Exception ex)
            {
                SetErrorMessage("Exception summarizing results: " + ex.Message);
                return false;
            }

            return true;
        }

        private udtPSMStatsType TabulatePSMStats(
            IDictionary<int, clsUniqueSeqInfo> lstUniqueSequences,
            IDictionary<string, int> lstUniqueProteins,
            IDictionary<int, clsUniqueSeqInfo> lstUniquePhosphopeptides)
        {
            var psmStats = new udtPSMStatsType
            {
                TotalPSMs = (from item in lstUniqueSequences select item.Value.ObsCount).Sum(),
                UniquePeptideCount = lstUniqueSequences.Count,
                UniqueProteinCount = lstUniqueProteins.Count,
                MissedCleavageRatio = ComputeMissedCleavageRatio(lstUniqueSequences),
                KeratinPeptides = (from item in lstUniqueSequences where item.Value.KeratinPeptide select item.Key).Count(),
                TrypsinPeptides = (from item in lstUniqueSequences where item.Value.TrypsinPeptide select item.Key).Count(),
                TrypticPeptides = (from item in lstUniqueSequences where item.Value.Tryptic select item.Key).Count(),
                UniquePhosphopeptideCount = lstUniquePhosphopeptides.Count,
                UniquePhosphopeptidesCTermK = (from item in lstUniquePhosphopeptides where item.Value.CTermK select item.Key).Count(),
                UniquePhosphopeptidesCTermR = (from item in lstUniquePhosphopeptides where item.Value.CTermR select item.Key).Count(),
                MissedCleavageRatioPhospho = ComputeMissedCleavageRatio(lstUniquePhosphopeptides)
            };

            return psmStats;
        }

        private class clsMSGFtoResultIDMapComparer : IComparer<KeyValuePair<double, int>>
        {
            public int Compare(KeyValuePair<double, int> x, KeyValuePair<double, int> y)
            {
                if (x.Key < y.Key)
                {
                    return -1;
                }

                if (x.Key > y.Key)
                {
                    return 1;
                }

                return 0;
            }
        }
    }
}
