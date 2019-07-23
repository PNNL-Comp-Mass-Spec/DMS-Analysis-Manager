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

using PHRPReader;
using PRISM;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

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

        public string OutputDirectoryPath { get; set; } = string.Empty;

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
        /// <param name="resultType">Peptide Hit result type</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="job">Job number</param>
        /// <param name="sourceDirectoryPath">Source directory path</param>
        /// <remarks></remarks>
        public clsMSGFResultsSummarizer(clsPHRPReader.ePeptideHitResultType resultType, string datasetName, int job, string sourceDirectoryPath)
            : this(resultType, datasetName, job, sourceDirectoryPath, DEFAULT_CONNECTION_STRING, debugLevel: 1)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="resultType">Peptide Hit result type</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="job">Job number</param>
        /// <param name="sourceDirectoryPath">Source folder path</param>
        /// <param name="connectionString">DMS connection string</param>
        /// <param name="debugLevel">Debug Level</param>
        /// <remarks></remarks>
        public clsMSGFResultsSummarizer(
            clsPHRPReader.ePeptideHitResultType resultType,
            string datasetName,
            int job,
            string sourceDirectoryPath,
            string connectionString,
            short debugLevel)
        {
            ResultType = resultType;
            mDatasetName = datasetName;
            mJob = job;
            mWorkDir = sourceDirectoryPath;
            mConnectionString = connectionString;
            mDebugLevel = debugLevel;

            mStoredProcedureExecutor = new ExecuteDatabaseSP(mConnectionString);
            RegisterEvents(mStoredProcedureExecutor);

            ContactDatabase = true;
        }

        private void AddUpdateUniqueSequence(IDictionary<int, clsUniqueSeqInfo> uniqueSequences, int seqId, clsUniqueSeqInfo seqInfoToStore)
        {

            if (uniqueSequences.TryGetValue(seqId, out var existingSeqInfo))
            {
                existingSeqInfo.UpdateObservationCount(seqInfoToStore.ObsCount + existingSeqInfo.ObsCount);
            }
            else
            {
                uniqueSequences.Add(seqId, seqInfoToStore);
            }
        }

        private void ExamineFirstHitsFile(string firstHitsFilePath)
        {
            try
            {
                // Initialize the list that will be used to track the number of spectra searched
                // Keys are Scan_Charge, values are Scan number
                var uniqueSpectra = new Dictionary<string, int>();

                var startupOptions = GetMinimalMemoryPHRPStartupOptions();

                using (var reader = new clsPHRPReader(firstHitsFilePath, startupOptions))
                {
                    RegisterEvents(reader);

                    while (reader.MoveNext())
                    {
                        var currentPSM = reader.CurrentPSM;

                        if (currentPSM.Charge >= 0)
                        {
                            var scanChargeCombo = currentPSM.ScanNumber + "_" + currentPSM.Charge;

                            if (!uniqueSpectra.ContainsKey(scanChargeCombo))
                            {
                                uniqueSpectra.Add(scanChargeCombo, currentPSM.ScanNumber);
                            }
                        }
                    }
                }

                mSpectraSearched = uniqueSpectra.Count;

                // Set these to defaults for now
                MaximumScanGapAdjacentMSn = 0;
                mPercentMSnScansNoPSM = 100;

                if (!ContactDatabase)
                {
                    return;
                }

                var scanList = uniqueSpectra.Values.Distinct().ToList();

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
            // The occurrence of large gaps indicates that a processing thread in MS-GF+ crashed and the results may be incomplete
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

        private float ComputeMissedCleavageRatio(IDictionary<int, clsUniqueSeqInfo> uniqueSequences)
        {
            if (uniqueSequences.Count == 0)
            {
                return 0;
            }

            var missedCleavages = (from item in uniqueSequences where item.Value.MissedCleavage select item.Key).Count();
            var missedCleavageRatio = missedCleavages / (float)uniqueSequences.Count;

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
        /// <param name="normalizedPSMs">PSM results (keys are NormalizedSeqID, values are the protein and scan info for each normalized sequence)</param>
        /// <param name="seqToProteinMap">Sequence to Protein map information (empty if the _resultToSeqMap file was not found)</param>
        /// <param name="sequenceInfo">Sequence information (empty if the _resultToSeqMap file was not found)</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool FilterAndComputeStats(bool usingMSGFOrEValueFilter, IDictionary<int, clsPSMInfo> normalizedPSMs,
            IDictionary<int, List<clsProteinInfo>> seqToProteinMap, IDictionary<int, clsSeqInfo> sequenceInfo)
        {
            var filteredPSMs = new Dictionary<int, clsPSMInfo>();

            var success = false;
            var filterPSMs = true;

            // Make sure .PassesFilter is false for all of the observations
            foreach (var kvEntry in normalizedPSMs)
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
                    success = FilterPSMsByEValue(EValueThreshold, normalizedPSMs, filteredPSMs);
                }
                else if (MSGFThreshold < 1)
                {
                    // Filter on MSGF (though for MSPathFinder we're using SpecEValue)
                    success = FilterPSMsByMSGF(MSGFThreshold, normalizedPSMs, filteredPSMs);
                }
                else
                {
                    // Do not filter
                    filterPSMs = false;
                }
            }
            else
            {
                filterPSMs = false;
            }

            if (!filterPSMs)
            {
                // Keep all PSMs
                foreach (var kvEntry in normalizedPSMs)
                {
                    foreach (var observation in kvEntry.Value.Observations)
                    {
                        observation.PassesFilter = true;
                    }
                    filteredPSMs.Add(kvEntry.Key, kvEntry.Value);
                }
                success = true;
            }

            if (!usingMSGFOrEValueFilter && FDRThreshold < 1)
            {
                // Filter on FDR (we'll compute the FDR using Reverse Proteins, if necessary)
                ReportDebugMessage("Call FilterPSMsByFDR", 3);

                success = FilterPSMsByFDR(filteredPSMs);

                ReportDebugMessage("FilterPSMsByFDR returned " + success);

                foreach (var entry in filteredPSMs)
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

            if (success)
            {
                // Summarize the results, counting the number of peptides, unique peptides, and proteins
                // We also count phosphopeptides using several metrics
                ReportDebugMessage("Call SummarizeResults for " + filteredPSMs.Count + " Filtered PSMs", 3);

                success = SummarizeResults(usingMSGFOrEValueFilter, filteredPSMs, seqToProteinMap, sequenceInfo);

                ReportDebugMessage("SummarizeResults returned " + success, 3);
            }

            return success;
        }

        /// <summary>
        /// Filter the data using mFDRThreshold
        /// </summary>
        /// <param name="psmResults">PSM results (keys are NormalizedSeqID, values are the protein and scan info for each normalized sequence)</param>
        /// <returns>True if success; false if no reverse hits are present or if none of the data has MSGF values</returns>
        /// <remarks></remarks>
        private bool FilterPSMsByFDR(IDictionary<int, clsPSMInfo> psmResults)
        {
            var fdrAlreadyComputed = true;
            foreach (var psmResult in psmResults)
            {
                if (psmResult.Value.BestFDR < 0)
                {
                    fdrAlreadyComputed = false;
                    break;
                }
            }

            var resultIDtoFDRMap = new Dictionary<int, double>();
            if (fdrAlreadyComputed)
            {
                foreach (var psmResult in psmResults)
                {
                    resultIDtoFDRMap.Add(psmResult.Key, psmResult.Value.BestFDR);
                }
            }
            else
            {
                // Sort the data by ascending SpecEValue, then step through the list and compute FDR
                // Use FDR = #Reverse / #Forward
                //
                // Alternative FDR formula is:  FDR = 2 * #Reverse / (#Forward + #Reverse)
                // But, since MS-GF+ uses "#Reverse / #Forward" we'll use that here too
                //
                // If no reverse hits are present or if none of the data has MSGF values, we'll clear psmResults and update mErrorMessage

                // Populate a list with the MSGF values and ResultIDs so that we can step through the data and compute the FDR for each entry
                var msgfToResultIDMap = new List<KeyValuePair<double, int>>();

                var validMSGFOrEValue = false;
                foreach (var psmResult in psmResults)
                {
                    if (psmResult.Value.BestMSGF < clsPSMInfo.UNKNOWN_MSGF_SPEC_EVALUE)
                    {
                        msgfToResultIDMap.Add(new KeyValuePair<double, int>(psmResult.Value.BestMSGF, psmResult.Key));
                        if (psmResult.Value.BestMSGF < 1)
                            validMSGFOrEValue = true;
                    }
                    else
                    {
                        msgfToResultIDMap.Add(new KeyValuePair<double, int>(psmResult.Value.BestEValue, psmResult.Key));
                        if (psmResult.Value.BestEValue < clsPSMInfo.UNKNOWN_EVALUE)
                            validMSGFOrEValue = true;
                    }
                }

                if (!validMSGFOrEValue)
                {
                    // None of the data has MSGF values or E-Values; cannot compute FDR
                    SetErrorMessage("Data does not contain MSGF values or EValues; cannot compute a decoy-based FDR");
                    psmResults.Clear();
                    return false;
                }

                // Sort msgfToResultIDMap
                msgfToResultIDMap.Sort(new clsMSGFtoResultIDMapComparer());

                var forwardResults = 0;
                var decoyResults = 0;
                var missedResultIDsAtStart = new List<int>();

                foreach (var resultItem in msgfToResultIDMap)
                {
                    var protein = psmResults[resultItem.Value].Protein.ToLower();

                    // MTS reversed proteins                 'reversed[_]%'
                    // MTS scrambled proteins                'scrambled[_]%'
                    // X!Tandem decoy proteins               '%[:]reversed'
                    // Inspect reversed/scrambled proteins   'xxx.%'
                    // MSGFDB reversed proteins  'rev[_]%'
                    // MS-GF+ reversed proteins  'xxx[_]%'

                    if (protein.StartsWith("reversed_") || protein.StartsWith("scrambled_") || protein.EndsWith(":reversed") ||
                        protein.StartsWith("xxx_") || protein.StartsWith("xxx.") || protein.StartsWith("rev_"))
                    {
                        decoyResults += 1;
                    }
                    else
                    {
                        forwardResults += 1;
                    }

                    if (forwardResults > 0)
                    {
                        // Compute and store the FDR for this entry
                        var fdrThreshold = decoyResults / (float)forwardResults;
                        resultIDtoFDRMap.Add(resultItem.Value, fdrThreshold);

                        if (missedResultIDsAtStart.Count > 0)
                        {
                            foreach (var resultID in missedResultIDsAtStart)
                            {
                                resultIDtoFDRMap.Add(resultID, fdrThreshold);
                            }
                            missedResultIDsAtStart.Clear();
                        }
                    }
                    else
                    {
                        // We cannot yet compute the FDR since all proteins up to this point are decoy proteins
                        // Update missedResultIDsAtStart
                        missedResultIDsAtStart.Add(resultItem.Value);
                    }
                }

                if (decoyResults == 0)
                {
                    // We never encountered any decoy proteins; cannot compute FDR
                    OnWarningEvent("Data does not contain decoy proteins; cannot compute a decoy-based FDR");
                    psmResults.Clear();
                    return false;
                }
            }

            // Remove entries from psmResults where .FDR is larger than mFDRThreshold

            foreach (var resultItem in resultIDtoFDRMap)
            {
                if (resultItem.Value > FDRThreshold)
                {
                    psmResults.Remove(resultItem.Key);
                }
            }

            return true;
        }

        private bool FilterPSMsByEValue(double eValueThreshold, IDictionary<int, clsPSMInfo> psmResults, IDictionary<int, clsPSMInfo> filteredPSMs)
        {
            filteredPSMs.Clear();

            var filteredValues = from item in psmResults where item.Value.BestEValue <= eValueThreshold select item;

            foreach (var item in filteredValues)
            {
                foreach (var observation in item.Value.Observations)
                {
                    observation.PassesFilter = observation.EValue <= eValueThreshold;
                }
                filteredPSMs.Add(item.Key, item.Value);
            }

            return true;
        }

        private bool FilterPSMsByMSGF(double msgfThreshold, IDictionary<int, clsPSMInfo> psmResults, IDictionary<int, clsPSMInfo> filteredPSMs)
        {
            filteredPSMs.Clear();

            var filteredValues = from item in psmResults where item.Value.BestMSGF <= msgfThreshold select item;

            foreach (var item in filteredValues)
            {
                foreach (var observation in item.Value.Observations)
                {
                    observation.PassesFilter = observation.MSGF <= msgfThreshold;
                }
                filteredPSMs.Add(item.Key, item.Value);
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

        private bool PostJobPSMResults(int job)
        {
            const int MAX_RETRY_COUNT = 3;

            bool success;

            try
            {
                // Call stored procedure StoreJobPSMStats in DMS5

                var sqlCommand = new SqlCommand(STORE_JOB_PSM_RESULTS_SP_NAME) { CommandType = CommandType.StoredProcedure };

                sqlCommand.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                sqlCommand.Parameters.Add(new SqlParameter("@Job", SqlDbType.Int)).Value = job;

                sqlCommand.Parameters.Add(new SqlParameter("@MSGFThreshold", SqlDbType.Float));
                if (ResultType == clsPHRPReader.ePeptideHitResultType.MSAlign)
                {
                    sqlCommand.Parameters["@MSGFThreshold"].Value = EValueThreshold;
                }
                else
                {
                    sqlCommand.Parameters["@MSGFThreshold"].Value = MSGFThreshold;
                }

                sqlCommand.Parameters.Add(new SqlParameter("@FDRThreshold", SqlDbType.Float)).Value = FDRThreshold;
                sqlCommand.Parameters.Add(new SqlParameter("@SpectraSearched", SqlDbType.Int)).Value = mSpectraSearched;
                sqlCommand.Parameters.Add(new SqlParameter("@TotalPSMs", SqlDbType.Int)).Value = mMSGFBasedCounts.TotalPSMs;
                sqlCommand.Parameters.Add(new SqlParameter("@UniquePeptides", SqlDbType.Int)).Value = mMSGFBasedCounts.UniquePeptideCount;
                sqlCommand.Parameters.Add(new SqlParameter("@UniqueProteins", SqlDbType.Int)).Value = mMSGFBasedCounts.UniqueProteinCount;
                sqlCommand.Parameters.Add(new SqlParameter("@TotalPSMsFDRFilter", SqlDbType.Int)).Value = mFDRBasedCounts.TotalPSMs;
                sqlCommand.Parameters.Add(new SqlParameter("@UniquePeptidesFDRFilter", SqlDbType.Int)).Value = mFDRBasedCounts.UniquePeptideCount;
                sqlCommand.Parameters.Add(new SqlParameter("@UniqueProteinsFDRFilter", SqlDbType.Int)).Value = mFDRBasedCounts.UniqueProteinCount;

                sqlCommand.Parameters.Add(new SqlParameter("@MSGFThresholdIsEValue", SqlDbType.TinyInt));
                if (ResultType == clsPHRPReader.ePeptideHitResultType.MSAlign)
                {
                    sqlCommand.Parameters["@MSGFThresholdIsEValue"].Value = 1;
                }
                else
                {
                    sqlCommand.Parameters["@MSGFThresholdIsEValue"].Value = 0;
                }

                sqlCommand.Parameters.Add(new SqlParameter("@PercentMSnScansNoPSM", SqlDbType.Real)).Value = mPercentMSnScansNoPSM;
                sqlCommand.Parameters.Add(new SqlParameter("@MaximumScanGapAdjacentMSn", SqlDbType.Int)).Value = MaximumScanGapAdjacentMSn;
                sqlCommand.Parameters.Add(new SqlParameter("@UniquePhosphopeptideCountFDR", SqlDbType.Int)).Value = mFDRBasedCounts.UniquePhosphopeptideCount;
                sqlCommand.Parameters.Add(new SqlParameter("@UniquePhosphopeptidesCTermK", SqlDbType.Int)).Value = mFDRBasedCounts.UniquePhosphopeptidesCTermK;
                sqlCommand.Parameters.Add(new SqlParameter("@UniquePhosphopeptidesCTermR", SqlDbType.Int)).Value = mFDRBasedCounts.UniquePhosphopeptidesCTermR;
                sqlCommand.Parameters.Add(new SqlParameter("@MissedCleavageRatio", SqlDbType.Real)).Value = mFDRBasedCounts.MissedCleavageRatio;
                sqlCommand.Parameters.Add(new SqlParameter("@MissedCleavageRatioPhospho", SqlDbType.Real)).Value = mFDRBasedCounts.MissedCleavageRatioPhospho;
                sqlCommand.Parameters.Add(new SqlParameter("@TrypticPeptides", SqlDbType.Int)).Value = mFDRBasedCounts.TrypticPeptides;
                sqlCommand.Parameters.Add(new SqlParameter("@KeratinPeptides", SqlDbType.Int)).Value = mFDRBasedCounts.KeratinPeptides;
                sqlCommand.Parameters.Add(new SqlParameter("@TrypsinPeptides", SqlDbType.Int)).Value = mFDRBasedCounts.TrypsinPeptides;

                // Execute the SP (retry the call up to 3 times)
                var result = mStoredProcedureExecutor.ExecuteSP(sqlCommand, MAX_RETRY_COUNT, out var errorMessage);

                if (result == 0)
                {
                    success = true;
                }
                else
                {
                    SetErrorMessage("Error storing PSM Results in database, " + STORE_JOB_PSM_RESULTS_SP_NAME + " returned " + result);
                    if (!string.IsNullOrEmpty(errorMessage))
                    {
                        mErrorMessage += "; " + errorMessage;
                    }
                    success = false;
                }
            }
            catch (Exception ex)
            {
                SetErrorMessage("Exception storing PSM Results in database: " + ex.Message);
                success = false;
            }

            return success;
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
                var phrpFirstHitsFileName = clsPHRPReader.GetPHRPFirstHitsFileName(ResultType, mDatasetName);

                // We use the Synopsis file to count the number of peptides and proteins observed
                var phrpSynopsisFileName = clsPHRPReader.GetPHRPSynopsisFileName(ResultType, mDatasetName);

                if (ResultType == clsPHRPReader.ePeptideHitResultType.XTandem || ResultType == clsPHRPReader.ePeptideHitResultType.MSAlign ||
                    ResultType == clsPHRPReader.ePeptideHitResultType.MODa || ResultType == clsPHRPReader.ePeptideHitResultType.MODPlus ||
                    ResultType == clsPHRPReader.ePeptideHitResultType.MSPathFinder)
                {
                    // These tools do not have first-hits files; use the Synopsis file instead to determine scan counts
                    phrpFirstHitsFileName = phrpSynopsisFileName;
                }

                mMSGFSynopsisFileName = Path.GetFileNameWithoutExtension(phrpSynopsisFileName) + MSGF_RESULT_FILENAME_SUFFIX;

                var phrpFirstHitsFilePath = Path.Combine(mWorkDir, phrpFirstHitsFileName);
                var phrpSynopsisFilePath = Path.Combine(mWorkDir, phrpSynopsisFileName);

                if (!File.Exists(phrpSynopsisFilePath))
                {
                    SetErrorMessage("File not found: " + phrpSynopsisFilePath);
                    return false;
                }

                /////////////////////
                // Determine the number of MS/MS spectra searched
                //
                if (File.Exists(phrpFirstHitsFilePath))
                {
                    ExamineFirstHitsFile(phrpFirstHitsFilePath);
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
                // If those files are not found, we'll simply use the protein information stored in psmResults
                var normalizedPSMs = new Dictionary<int, clsPSMInfo>();


                var successLoading = LoadPSMs(phrpSynopsisFilePath, normalizedPSMs, out _, out var seqToProteinMap, out var sequenceInfo);
                if (!successLoading)
                {
                    return false;
                }

                ////////////////////
                // Filter on MSGF or EValue and compute the stats
                //
                var usingMSGFOrEValueFilter = true;
                ReportDebugMessage("Call FilterAndComputeStats with usingMSGFOrEValueFilter = true", 3);

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                var success = FilterAndComputeStats(usingMSGFOrEValueFilter, normalizedPSMs, seqToProteinMap, sequenceInfo);

                ReportDebugMessage("FilterAndComputeStats returned " + success, 3);

                ////////////////////
                // Filter on FDR and compute the stats
                //
                usingMSGFOrEValueFilter = false;
                ReportDebugMessage("Call FilterAndComputeStats with usingMSGFOrEValueFilter = false", 3);

                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                var successViaFDR = FilterAndComputeStats(usingMSGFOrEValueFilter, normalizedPSMs, seqToProteinMap, sequenceInfo);

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
        /// Normalizes the peptide sequence (mods are tracked, but no longer associated with specific residues) and populates normalizedPSMs
        /// </summary>
        /// <param name="phrpSynopsisFilePath"></param>
        /// <param name="normalizedPSMs"></param>
        /// <param name="resultToSeqMap"></param>
        /// <param name="seqToProteinMap"></param>
        /// <param name="sequenceInfo"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool LoadPSMs(string phrpSynopsisFilePath, IDictionary<int, clsPSMInfo> normalizedPSMs,
            out SortedList<int, int> resultToSeqMap, out SortedList<int, List<clsProteinInfo>> seqToProteinMap,
            out SortedList<int, clsSeqInfo> sequenceInfo)
        {
            var specEValue = clsPSMInfo.UNKNOWN_MSGF_SPEC_EVALUE;
            var eValue = clsPSMInfo.UNKNOWN_EVALUE;

            bool success;

            var loadMSGFResults = true;

            // Regex for determining that a peptide has a missed cleavage (i.e. an internal tryptic cleavage point)
            var reMissedCleavage = new Regex(@"[KR][^P][A-Z]", RegexOptions.Compiled);

            // Regex to match keratin proteins
            var reKeratinProtein = GetKeratinRegEx();

            // Regex to match trypsin proteins
            var reTrypsinProtein = GetTrypsinRegEx();

            resultToSeqMap = new SortedList<int, int>();
            seqToProteinMap = new SortedList<int, List<clsProteinInfo>>();
            sequenceInfo = new SortedList<int, clsSeqInfo>();

            try
            {
                if (ResultType == clsPHRPReader.ePeptideHitResultType.MODa || ResultType == clsPHRPReader.ePeptideHitResultType.MODPlus ||
                    ResultType == clsPHRPReader.ePeptideHitResultType.MSPathFinder)
                {
                    loadMSGFResults = false;
                }

                // Note that this will set .LoadModsAndSeqInfo to false
                // That is fine because we will have access to the modification info from the _SeqInfo.txt file
                // Since we're looking for trypsin and keratin proteins, we need to change MaxProteinsPerPSM back to a large number
                var startupOptions = GetMinimalMemoryPHRPStartupOptions();
                startupOptions.LoadMSGFResults = loadMSGFResults;
                startupOptions.MaxProteinsPerPSM = 1000;

                // Load the result to sequence mapping, sequence IDs, and protein information
                // This also loads the mod description, which we use to determine if a peptide is a phosphopeptide
                var seqMapReader = new clsPHRPSeqMapReader(mDatasetName, mWorkDir, ResultType);

                var sequenceInfoAvailable = false;

                if (!string.IsNullOrEmpty(seqMapReader.ResultToSeqMapFilename))
                {
                    var resultToSeqMapFilePath = clsPHRPReader.FindResultToSeqMapFile(seqMapReader.InputDirectoryPath,
                                                                                      phrpSynopsisFilePath,
                                                                                      seqMapReader.ResultToSeqMapFilename,
                                                                                      out _);

                    if (!string.IsNullOrWhiteSpace(resultToSeqMapFilePath))
                    {
                        success = seqMapReader.GetProteinMapping(resultToSeqMap, seqToProteinMap, sequenceInfo);

                        if (!success)
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

                using (var reader = new clsPHRPReader(phrpSynopsisFilePath, startupOptions))
                {
                    RegisterEvents(reader);

                    while (reader.MoveNext())
                    {
                        var currentPSM = reader.CurrentPSM;

                        if (currentPSM.ScoreRank > 1)
                        {
                            // Only keep the first match for each spectrum
                            continue;
                        }

                        var valid = false;

                        if (ResultType == clsPHRPReader.ePeptideHitResultType.MSAlign)
                        {
                            // Use the EValue reported by MSAlign

                            if (currentPSM.TryGetScore("EValue", out var eValueText))
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

                            if (currentPSM.TryGetScore(clsPHRPParserMSPathFinder.DATA_COLUMN_SpecEValue, out var specEValueText))
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
                            valid = double.TryParse(currentPSM.MSGFSpecEValue, out specEValue);
                        }

                        if (!valid)
                        {
                            continue;
                        }

                        // Store in normalizedPSMs

                        var psmInfo = new clsPSMInfo();
                        psmInfo.Clear();

                        psmInfo.Protein = currentPSM.ProteinFirst;

                        var psmMSGF = specEValue;
                        var psmEValue = eValue;
                        double psmFDR;

                        if (ResultType == clsPHRPReader.ePeptideHitResultType.MSGFPlus | ResultType == clsPHRPReader.ePeptideHitResultType.MSAlign)
                        {
                            psmFDR = currentPSM.GetScoreDbl(clsPHRPParserMSGFPlus.DATA_COLUMN_FDR, clsPSMInfo.UNKNOWN_FDR);
                            if (psmFDR < 0)
                            {
                                psmFDR = currentPSM.GetScoreDbl(clsPHRPParserMSGFPlus.DATA_COLUMN_EFDR, clsPSMInfo.UNKNOWN_FDR);
                            }
                        }
                        else if (ResultType == clsPHRPReader.ePeptideHitResultType.MODa)
                        {
                            psmFDR = currentPSM.GetScoreDbl(clsPHRPParserMODa.DATA_COLUMN_QValue, clsPSMInfo.UNKNOWN_FDR);
                        }
                        else if (ResultType == clsPHRPReader.ePeptideHitResultType.MODPlus)
                        {
                            psmFDR = currentPSM.GetScoreDbl(clsPHRPParserMODPlus.DATA_COLUMN_QValue, clsPSMInfo.UNKNOWN_FDR);
                        }
                        else if (ResultType == clsPHRPReader.ePeptideHitResultType.MSPathFinder)
                        {
                            psmFDR = currentPSM.GetScoreDbl(clsPHRPParserMSPathFinder.DATA_COLUMN_QValue, clsPSMInfo.UNKNOWN_FDR);
                        }
                        else
                        {
                            psmFDR = clsPSMInfo.UNKNOWN_FDR;
                        }

                        var normalizedPeptide = new clsNormalizedPeptideInfo(string.Empty);

                        var normalized = false;
                        var seqID = clsPSMInfo.UNKNOWN_SEQID;

                        if (sequenceInfoAvailable && resultToSeqMap != null)
                        {
                            if (!resultToSeqMap.TryGetValue(currentPSM.ResultID, out seqID))
                            {
                                seqID = clsPSMInfo.UNKNOWN_SEQID;

                                // This result is not listed in the _ResultToSeqMap file, likely because it was already processed for this scan
                                // Look for a match in dictNormalizedPeptides that matches this peptide's clean sequence

                                if (dictNormalizedPeptides.TryGetValue(currentPSM.PeptideCleanSequence, out var normalizedPeptides))
                                {
                                    foreach (var normalizedItem in normalizedPeptides)
                                    {
                                        if (normalizedItem.SeqID >= 0)
                                        {
                                            // Match found; use the given SeqID value
                                            seqID = normalizedItem.SeqID;
                                            break;
                                        }
                                    }
                                }
                            }

                            if (seqID != clsPSMInfo.UNKNOWN_SEQID)
                            {
                                if (sequenceInfo.TryGetValue(seqID, out var seqInfo))
                                {
                                    normalizedPeptide = NormalizeSequence(currentPSM.PeptideCleanSequence, seqInfo, seqID);
                                    normalized = true;
                                }
                            }
                        }

                        if (!normalized)
                        {
                            normalizedPeptide = NormalizeSequence(currentPSM.Peptide, seqID);
                        }

                        var normalizedSeqID = FindNormalizedSequence(dictNormalizedPeptides, normalizedPeptide);

                        if (normalizedSeqID != clsPSMInfo.UNKNOWN_SEQID)
                        {
                            // We're already tracking this normalized peptide (or one very similar to it)

                            var normalizedPSMInfo = normalizedPSMs[normalizedSeqID];
                            var addObservation = true;

                            foreach (var observation in normalizedPSMInfo.Observations)
                            {
                                if (observation.Scan == currentPSM.ScanNumber)
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
                                    Scan = currentPSM.ScanNumber,
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
                                seqID = currentPSM.ResultID;
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
                            if (currentPSM.NumMissedCleavages > 0)
                            {
                                psmInfo.MissedCleavage = true;
                            }
                            else if (reMissedCleavage.IsMatch(normalizedPeptide.CleanSequence))
                            {
                                Console.WriteLine("NumMissedCleavages is zero but the peptide matches the MissedCleavage Regex; this is unexpected");
                                psmInfo.MissedCleavage = true;
                            }

                            // Check whether this peptide is from Keratin or a related protein
                            foreach (var proteinName in currentPSM.Proteins)
                            {
                                if (reKeratinProtein.IsMatch(proteinName))
                                {
                                    psmInfo.KeratinPeptide = true;
                                    break;
                                }
                            }

                            // Check whether this peptide is from Trypsin or a related protein
                            foreach (var proteinName in currentPSM.Proteins)
                            {
                                if (reTrypsinProtein.IsMatch(proteinName))
                                {
                                    psmInfo.TrypsinPeptide = true;
                                    break;
                                }
                            }

                            // Check whether this peptide is partially or fully tryptic
                            if (currentPSM.CleavageState == clsPeptideCleavageStateCalculator.ePeptideCleavageStateConstants.Full ||
                                currentPSM.CleavageState == clsPeptideCleavageStateCalculator.ePeptideCleavageStateConstants.Partial)
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
                                Scan = currentPSM.ScanNumber,
                                FDR = psmFDR,
                                MSGF = psmMSGF,
                                EValue = psmEValue
                            };

                            psmInfo.AddObservation(observation);

                            if (normalizedPSMs.ContainsKey(seqID))
                            {
                                Console.WriteLine("Warning: Duplicate key, seqID=" + seqID + "; skipping PSM with ResultID=" + currentPSM.ResultID);
                            }
                            else
                            {
                                normalizedPSMs.Add(seqID, psmInfo);
                            }
                        }
                    }
                }

                success = true;
            }
            catch (Exception ex)
            {
                SetErrorMessage(ex.Message);
                Console.WriteLine(ex.StackTrace);
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Parse a sequence with mod symbols
        /// </summary>
        /// <param name="sequenceWithMods"></param>
        /// <param name="seqID"></param>
        /// <returns></returns>
        private clsNormalizedPeptideInfo NormalizeSequence(string sequenceWithMods, int seqID)
        {
            var aminoAcidList = new StringBuilder(sequenceWithMods.Length);
            var modList = new List<KeyValuePair<string, int>>();

            clsPeptideCleavageStateCalculator.SplitPrefixAndSuffixFromSequence(sequenceWithMods, out var primarySequence, out _, out _);

            for (var index = 0; index <= primarySequence.Length - 1; index++)
            {
                if (clsPHRPReader.IsLetterAtoZ(primarySequence[index]))
                {
                    aminoAcidList.Append(primarySequence[index]);
                }
                else
                {
                    modList.Add(new KeyValuePair<string, int>(primarySequence[index].ToString(), index));
                }
            }

            return GetNormalizedPeptideInfo(aminoAcidList.ToString(), modList, seqID);
        }

        private clsNormalizedPeptideInfo NormalizeSequence(string peptideCleanSequence, clsSeqInfo seqInfo, int seqID)
        {
            var modList = new List<KeyValuePair<string, int>>();

            if (!string.IsNullOrWhiteSpace(seqInfo.ModDescription))
            {
                // Parse the modifications

                var sequenceMods = seqInfo.ModDescription.Split(',');
                foreach (var modDescriptor in sequenceMods)
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
                        throw new Exception(string.Format("Empty mod name parsed from the ModDescription for SeqID {0}: {1}", seqInfo.SeqID, seqInfo.ModDescription));
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
            var outputFilePath = "??";

            try
            {
                if (!string.IsNullOrEmpty(OutputDirectoryPath))
                {
                    outputFilePath = OutputDirectoryPath;
                }
                else
                {
                    outputFilePath = mWorkDir;
                }
                outputFilePath = Path.Combine(outputFilePath, mDatasetName + "_PSM_Stats.txt");

                using (var writer = new StreamWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    // Header line
                    var headers = new List<string>
                    {
                        "Dataset",
                        "Job",
                        "MSGF_Threshold",
                        "FDR_Threshold",
                        "Spectra_Searched",
                        "Total_PSMs_MSGF_Filtered",
                        "Unique_Peptides_MSGF_Filtered",
                        "Unique_Proteins_MSGF_Filtered",
                        "Total_PSMs_FDR_Filtered",
                        "Unique_Peptides_FDR_Filtered",
                        "Unique_Proteins_FDR_Filtered"
                    };

                    writer.WriteLine(string.Join("\t", headers));

                    // Stats
                    var stats = new List<string>
                    {
                        mDatasetName,
                        mJob.ToString(),
                        MSGFThreshold.ToString("0.00E+00"),
                        FDRThreshold.ToString("0.000"),
                        mSpectraSearched.ToString(),
                        mMSGFBasedCounts.TotalPSMs.ToString(),
                        mMSGFBasedCounts.UniquePeptideCount.ToString(),
                        mMSGFBasedCounts.UniqueProteinCount.ToString(),
                        mFDRBasedCounts.TotalPSMs.ToString(),
                        mFDRBasedCounts.UniquePeptideCount.ToString(),
                        mFDRBasedCounts.UniqueProteinCount.ToString()
                    };

                    writer.WriteLine(string.Join("\t", stats));
                }
            }
            catch (Exception ex)
            {
                SetErrorMessage("Exception saving results to " + outputFilePath + ": " + ex.Message);
            }

        }

        private void SetErrorMessage(string errMsg)
        {
            Console.WriteLine(errMsg);
            mErrorMessage = errMsg;
            OnErrorEvent(errMsg);
        }

        /// <summary>
        /// Summarize the results by inter-relating filteredPSMs, resultToSeqMap, and seqToProteinMap
        /// </summary>
        /// <param name="usingMSGFOrEValueFilter"></param>
        /// <param name="filteredPSMs">Filter-passing results (keys are NormalizedSeqID, values are the protein and scan info for each normalized sequence)</param>
        /// <param name="seqToProteinMap">Sequence to protein map (keys are sequence ID, values are proteins)</param>
        /// <param name="sequenceInfo">Sequence information (keys are sequence ID, values are sequences</param>
        /// <returns></returns>
        /// <remarks></remarks>
        private bool SummarizeResults(
            bool usingMSGFOrEValueFilter, IDictionary<int, clsPSMInfo> filteredPSMs,
            IDictionary<int, List<clsProteinInfo>> seqToProteinMap, IDictionary<int, clsSeqInfo> sequenceInfo)
        {
            try
            {
                // The Keys in this dictionary are SeqID values; the values track observation count, whether the peptide ends in K or R, etc.
                // Populated from data in filteredPSMs, where peptides with the same sequence and same modifications (+/- 1 residue) were collapsed
                // For example, LS*SPATLNSR and LSS*PATLNSR are considered equivalent
                // But P#EPT*IDES and PEP#T*IDES and P#EPTIDES* are all different
                // (the collapsing of similar peptides is done in method LoadPSMs with the call to FindNormalizedSequence)
                var uniqueSequences = new Dictionary<int, clsUniqueSeqInfo>();

                // The Keys in this dictionary are SeqID values; the values track observation count, whether the peptide ends in K or R, etc.
                var uniquePhosphopeptides = new Dictionary<int, clsUniqueSeqInfo>();

                // The Keys in this dictionary are protein names; the values are observation count
                var uniqueProteins = new Dictionary<string, int>();

                foreach (var result in filteredPSMs)
                {
                    var observations = result.Value.Observations;
                    var obsCountForResult = (from item in observations where item.PassesFilter select item).Count();

                    if (obsCountForResult == 0)
                    {
                        continue;
                    }

                    // If resultToSeqMap has data, the keys in filteredPSMs are SeqID values
                    // Otherwise, the keys are ResultID values
                    var seqId = result.Key;

                    // Make a deep copy of result.Value as class clsUniqueSeqInfo
                    var seqInfoToStore = result.Value.CloneAsSeqInfo(obsCountForResult);

                    AddUpdateUniqueSequence(uniqueSequences, seqId, seqInfoToStore);

                    if (result.Value.Phosphopeptide)
                    {
                        AddUpdateUniqueSequence(uniquePhosphopeptides, seqId, seqInfoToStore);
                    }

                    var addResultProtein = true;


                    if (sequenceInfo.Count > 0 && sequenceInfo.TryGetValue(seqId, out _))
                    {
                        // Lookup the proteins for this peptide
                        if (seqToProteinMap.TryGetValue(seqId, out var proteins))
                        {
                            // Update the observation count for each protein
                            foreach (var protein in proteins)
                            {
                                if (uniqueProteins.TryGetValue(protein.ProteinName, out var obsCountOverall))
                                {
                                    uniqueProteins[protein.ProteinName] = obsCountOverall + obsCountForResult;
                                }
                                else
                                {
                                    uniqueProteins.Add(protein.ProteinName, obsCountForResult);
                                }

                                // Protein match found; we can ignore result.Value.Protein
                                addResultProtein = false;
                            }
                        }
                    }

                    if (addResultProtein)
                    {
                        var proteinName = result.Value.Protein;

                        if (uniqueProteins.TryGetValue(proteinName, out var obsCountOverall))
                        {
                            uniqueProteins[proteinName] = obsCountOverall + obsCountForResult;
                        }
                        else
                        {
                            uniqueProteins.Add(proteinName, obsCountForResult);
                        }
                    }
                }

                // Obtain the stats to store
                var psmStats = TabulatePSMStats(uniqueSequences, uniqueProteins, uniquePhosphopeptides);

                if (usingMSGFOrEValueFilter)
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
            IDictionary<int, clsUniqueSeqInfo> uniqueSequences,
            IDictionary<string, int> uniqueProteins,
            IDictionary<int, clsUniqueSeqInfo> uniquePhosphopeptides)
        {
            var psmStats = new udtPSMStatsType
            {
                TotalPSMs = (from item in uniqueSequences select item.Value.ObsCount).Sum(),
                UniquePeptideCount = uniqueSequences.Count,
                UniqueProteinCount = uniqueProteins.Count,
                MissedCleavageRatio = ComputeMissedCleavageRatio(uniqueSequences),
                KeratinPeptides = (from item in uniqueSequences where item.Value.KeratinPeptide select item.Key).Count(),
                TrypsinPeptides = (from item in uniqueSequences where item.Value.TrypsinPeptide select item.Key).Count(),
                TrypticPeptides = (from item in uniqueSequences where item.Value.Tryptic select item.Key).Count(),
                UniquePhosphopeptideCount = uniquePhosphopeptides.Count,
                UniquePhosphopeptidesCTermK = (from item in uniquePhosphopeptides where item.Value.CTermK select item.Key).Count(),
                UniquePhosphopeptidesCTermR = (from item in uniquePhosphopeptides where item.Value.CTermR select item.Key).Count(),
                MissedCleavageRatioPhospho = ComputeMissedCleavageRatio(uniquePhosphopeptides)
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
