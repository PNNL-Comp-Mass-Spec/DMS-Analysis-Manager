﻿//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 02/14/2012
//
// This class reads an MS-GF+ results file and accompanying peptide/protein map file
// to count the number of peptides passing a given MSGF threshold
//
// Reports PSM count, unique peptide count, and unique protein count
//
//*********************************************************************************************************

using PHRPReader;
using PRISM;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using PHRPReader.Data;
using PHRPReader.Reader;
using PRISMDatabaseUtils;

// ReSharper disable UnusedMember.Global

namespace MSGFResultsSummarizer
{
    public class ResultsSummarizer : EventNotifier
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: Acetyl, Cntm, gi, itrac, MODa, peptides, phosph, phosphopeptide, phosphopeptides, psm
        // Ignore Spelling: sp, structs, Tpro, Trypa, tryptic, udt, uni, xxx

        // ReSharper restore CommentTypo

        public const double DEFAULT_MSGF_THRESHOLD = 1E-10;        // 1E-10
        public const double DEFAULT_EVALUE_THRESHOLD = 0.0001;     // 1E-4   (only used when MSGF Scores are not available)
        public const double DEFAULT_FDR_THRESHOLD = 0.01;          // 1% FDR

        private const string DEFAULT_CONNECTION_STRING = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;";

        private const string STORE_JOB_PSM_RESULTS_SP_NAME = "StoreJobPSMStats";

        private const string MSGF_RESULT_FILENAME_SUFFIX = "_MSGF.txt";

        private enum ReporterIonTypes
        {
            None = 0,
            iTRAQ4plex = 1,
            iTRAQ8plex = 2,
            TMT6plex = 3,
            TMT16plex = 4
        }

        private string mErrorMessage = string.Empty;
        private readonly short mDebugLevel;
        private readonly bool mTraceMode;

        private PSMStats mMSGFBasedCounts;
        private PSMStats mFDRBasedCounts;

        private readonly string mDatasetName;
        private readonly int mJob;
        private readonly string mWorkDir;
        private readonly string mConnectionString;

        private readonly IDBTools mStoredProcedureExecutor;

        /// <summary>
        /// This will be set to true if the _ModSummary.txt file has iTRAQ or TMT as a dynamic mod
        /// </summary>
        private bool mDynamicReporterIonPTM;

        /// <summary>
        /// The reporter ion type, when mDynamicReporterIonPTM is true
        /// </summary>
        private ReporterIonTypes mDynamicReporterIonType;

        /// <summary>
        /// The mod name, as loaded from the _ModSummary.txt file
        /// </summary>
        private string mDynamicReporterIonName = string.Empty;

        /// <summary>
        /// PHRP _syn.txt file name
        /// </summary>
        /// <remarks>This is auto-determined in ProcessPSMResults</remarks>
        private string mMSGFSynopsisFileName = string.Empty;

        /// <summary>
        /// Dataset name
        /// </summary>
        /// <remarks>
        /// Used to contact DMS to lookup the total number of scans and total number of MSn scans
        /// This information is used by
        /// </remarks>
        public string DatasetName { get; set; }

        /// <summary>
        /// Set this to false to disable contacting DMS to look up scan stats for the dataset
        /// </summary>
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

        /// <summary>
        /// Value between 0 and 100, indicating the percentage of the MS2 spectra with search results that are
        /// more than 2 scans away from an adjacent spectrum
        /// </summary>
        public double PercentMSnScansNoPSM { get; private set; }

        /// <summary>
        /// Store PSM results in the database for the given analysis job
        /// </summary>
        /// <remarks>Skipped if ContactDatabase is false or if the job cannot be determined</remarks>
        public bool PostJobPSMResultsToDB { get; set; }

        public PeptideHitResultTypes ResultType { get; }

        public string ResultTypeName => ResultType.ToString();

        public int SpectraSearched { get; private set; }

        public int TotalPSMsFDR => mFDRBasedCounts?.TotalPSMs ?? 0;

        public int TotalPSMsMSGF => mMSGFBasedCounts?.TotalPSMs ?? 0;

        public bool SaveResultsToTextFile { get; set; } = true;

        public int UniquePeptideCountFDR => mFDRBasedCounts?.UniquePeptideCount ?? 0;

        public int UniquePeptideCountMSGF => mMSGFBasedCounts?.UniquePeptideCount ?? 0;

        public int UniqueProteinCountFDR => mFDRBasedCounts?.UniqueProteinCount ?? 0;

        public int UniqueProteinCountMSGF => mMSGFBasedCounts?.UniqueProteinCount ?? 0;

        public int UniquePhosphopeptideCountFDR => mFDRBasedCounts?.UniquePhosphopeptideCount ?? 0;

        public int UniquePhosphopeptideCountMSGF => mMSGFBasedCounts?.UniquePhosphopeptideCount ?? 0;

        public int UniquePhosphopeptidesCTermK_FDR => mFDRBasedCounts?.UniquePhosphopeptidesCTermK ?? 0;

        public int UniquePhosphopeptidesCTermK_MSGF => mMSGFBasedCounts?.UniquePhosphopeptidesCTermK ?? 0;

        public int UniquePhosphopeptidesCTermR_FDR => mFDRBasedCounts?.UniquePhosphopeptidesCTermR ?? 0;

        public int UniquePhosphopeptidesCTermR_MSGF => mMSGFBasedCounts?.UniquePhosphopeptidesCTermR ?? 0;

        public float MissedCleavageRatioFDR => mFDRBasedCounts?.MissedCleavageRatio ?? 0;

        public float MissedCleavageRatioMSGF => mMSGFBasedCounts?.MissedCleavageRatio ?? 0;

        public float MissedCleavageRatioPhosphoFDR => mFDRBasedCounts?.MissedCleavageRatioPhospho ?? 0;

        public float MissedCleavageRatioPhosphoMSGF => mMSGFBasedCounts?.MissedCleavageRatioPhospho ?? 0;

        public int KeratinPeptidesFDR => mFDRBasedCounts?.KeratinPeptides ?? 0;

        public int KeratinPeptidesMSGF => mMSGFBasedCounts?.KeratinPeptides ?? 0;

        public int TrypsinPeptidesFDR => mFDRBasedCounts?.TrypsinPeptides ?? 0;

        public int TrypsinPeptidesMSGF => mMSGFBasedCounts?.TrypsinPeptides ?? 0;

        public int TrypticPeptidesMSGF => mMSGFBasedCounts?.TrypticPeptides ?? 0;

        public int TrypticPeptidesFDR => mFDRBasedCounts?.TrypticPeptides ?? 0;

        public int AcetylPeptidesMSGF => mMSGFBasedCounts?.AcetylPeptides ?? 0;

        public int AcetylPeptidesFDR => mFDRBasedCounts?.AcetylPeptides ?? 0;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="resultType">Peptide Hit result type</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="job">Job number</param>
        /// <param name="sourceDirectoryPath">Source directory path</param>
        /// <param name="traceMode">When true, show database queries</param>
        public ResultsSummarizer(PeptideHitResultTypes resultType, string datasetName, int job, string sourceDirectoryPath, bool traceMode)
            : this(resultType, datasetName, job, sourceDirectoryPath, DEFAULT_CONNECTION_STRING, debugLevel: 1, traceMode: traceMode)
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
        /// <param name="traceMode">When true, show database queries</param>
        public ResultsSummarizer(
            PeptideHitResultTypes resultType,
            string datasetName,
            int job,
            string sourceDirectoryPath,
            string connectionString,
            short debugLevel,
            bool traceMode)
        {
            ResultType = resultType;
            mDatasetName = datasetName;
            mJob = job;
            mWorkDir = sourceDirectoryPath;

            mConnectionString = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, "MSGFResultsSummarizer");
            mDebugLevel = debugLevel;
            mTraceMode = traceMode;

            mStoredProcedureExecutor = DbToolsFactory.GetDBTools(mConnectionString, debugMode: mTraceMode);
            RegisterEvents(mStoredProcedureExecutor);

            ContactDatabase = true;
        }

        private void AddUpdateUniqueSequence(IDictionary<int, UniqueSeqInfo> uniqueSequences, int seqId, UniqueSeqInfo seqInfoToStore)
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

        /// <summary>
        /// Append PSM stats if available, otherwise append zeros
        /// </summary>
        /// <param name="psmStats"></param>
        /// <param name="stats"></param>
        private void AppendStats(PSMStats psmStats, ICollection<string> stats)
        {
            if (psmStats == null)
            {
                stats.Add("0");
                stats.Add("0");
                stats.Add("0");
            }
            else
            {
                stats.Add(psmStats.TotalPSMs.ToString());
                stats.Add(psmStats.UniquePeptideCount.ToString());
                stats.Add(psmStats.UniqueProteinCount.ToString());
            }
        }

        private void ExamineFirstHitsFile(string firstHitsFilePath)
        {
            try
            {
                // Initialize the dictionary that will be used to track the number of spectra searched (grouped by dataset if MaxQuant results)
                // Keys are dataset name or ID (empty string of not MaxQuant)
                // Values are a dictionary where keys are Scan_Charge and values are scan number

                var uniqueSpectraByDataset = new Dictionary<string, Dictionary<string, int>>();

                var startupOptions = GetMinimalMemoryPHRPStartupOptions();

                OnStatusEvent("Reading PSMs from " + PathUtils.CompactPathString(firstHitsFilePath, 80));
                var lastStatusTime = DateTime.UtcNow;

                using var reader = new ReaderFactory(firstHitsFilePath, startupOptions);
                RegisterEvents(reader);

                while (reader.MoveNext())
                {
                    var currentPSM = reader.CurrentPSM;

                    var datasetIdOrName = ResultType == PeptideHitResultTypes.MaxQuant ? GetMaxQuantDatasetIdOrName(currentPSM) : string.Empty;

                    var scanKey = currentPSM.Charge >= 0 ? currentPSM.ScanNumber + "_" + currentPSM.Charge : currentPSM.ScanNumber.ToString();

                    if (uniqueSpectraByDataset.TryGetValue(datasetIdOrName, out var uniqueSpectra))
                    {
                        if (!uniqueSpectra.ContainsKey(scanKey))
                        {
                            uniqueSpectra.Add(scanKey, currentPSM.ScanNumber);
                        }
                    }
                    else
                    {
                        var newUniqueSpectra = new Dictionary<string, int>
                        {
                            {scanKey, currentPSM.ScanNumber}
                        };
                        uniqueSpectraByDataset.Add(datasetIdOrName, newUniqueSpectra);
                    }

                    if (DateTime.UtcNow.Subtract(lastStatusTime).TotalMilliseconds < 500)
                    {
                        continue;
                    }

                    Console.Write(".");
                    lastStatusTime = DateTime.UtcNow;
                }

                Console.WriteLine();

                SpectraSearched = 0;
                foreach (var item in uniqueSpectraByDataset)
                {
                    SpectraSearched += item.Value.Count;
                }

                // Set these to defaults for now
                MaximumScanGapAdjacentMSn = 0;
                PercentMSnScansNoPSM = 100;

                if (!ContactDatabase)
                {
                    return;
                }

                var scanListByDataset = new Dictionary<string, List<int>>();
                foreach (var item in uniqueSpectraByDataset)
                {
                    scanListByDataset.Add(item.Key, item.Value.Values.Distinct().ToList());
                }

                CheckForScanGaps(scanListByDataset);
            }
            catch (Exception ex)
            {
                SetErrorMessage("Error in ExamineFirstHitsFile: " + ex.Message, ex);
                Console.WriteLine(ex.StackTrace);
            }
        }

        /// <summary>
        /// Look for scan range gaps in the spectra list
        /// The occurrence of large gaps indicates that a processing thread in MS-GF+ crashed and the results may be incomplete
        /// </summary>
        /// <param name="scanListByDataset">Keys are Dataset ID or Dataset Name; values are a list of scan numbers</param>
        private void CheckForScanGaps(Dictionary<string, List<int>> scanListByDataset)
        {
            MaximumScanGapAdjacentMSn = 0;

            var warnIfNotFound = ResultType != PeptideHitResultTypes.MaxQuant;

            foreach (var item in scanListByDataset)
            {
                var success = LookupScanStats(DatasetName, out var totalSpectra, out var totalMSnSpectra, warnIfNotFound);

                if (!success &&
                    ResultType == PeptideHitResultTypes.MaxQuant &&
                    int.TryParse(item.Key, out var datasetID))
                {
                    var lookupSuccess = LookupDatasetNameByID(datasetID, out var datasetName);

                    if (lookupSuccess && !string.IsNullOrWhiteSpace(datasetName))
                    {
                        // Call LookupScanStats again, but with the correct dataset name
                        success = LookupScanStats(datasetName, out totalSpectra, out totalMSnSpectra, true);
                    }
                }

                if (!success || totalSpectra <= 0)
                {
                    DatasetScanStatsLookupError = true;
                    return;
                }

                var maximumScanGap = 0;

                var scanList = item.Value;
                scanList.Sort();

                for (var i = 1; i < scanList.Count; i++)
                {
                    var scanGap = scanList[i] - scanList[i - 1];

                    if (scanGap > maximumScanGap)
                    {
                        maximumScanGap = scanGap;
                    }
                }

                if (totalMSnSpectra > 0)
                {
                    PercentMSnScansNoPSM = (1 - scanList.Count / (float)totalMSnSpectra) * 100.0;
                }
                else
                {
                    // Report 100% because we cannot accurately compute this value without knowing totalMSnSpectra
                    PercentMSnScansNoPSM = 100;
                }

                if (scanList.Count > 0)
                {
                    // Compare the last scan number seen to the total number of scans
                    var scanGap = totalSpectra - scanList[scanList.Count - 1] - 1;

                    if (scanGap > maximumScanGap)
                    {
                        maximumScanGap = scanGap;
                    }
                }

                if (maximumScanGap > MaximumScanGapAdjacentMSn)
                {
                    MaximumScanGapAdjacentMSn = maximumScanGap;
                }
            }
        }

        private float ComputeMissedCleavageRatio(IDictionary<int, UniqueSeqInfo> uniqueSequences)
        {
            if (uniqueSequences.Count == 0)
            {
                return 0;
            }

            var missedCleavages = (from item in uniqueSequences where item.Value.MissedCleavage select item.Key).Count();
            var missedCleavageRatio = missedCleavages / (float)uniqueSequences.Count;

            return missedCleavageRatio;
        }

        private void ComputeMissingReporterIonPercent(Dictionary<int, PSMInfo> filteredPSMs)
        {
            mFDRBasedCounts.PercentPSMsMissingNTermReporterIon = 0;
            mFDRBasedCounts.PercentPSMsMissingReporterIon = 0;

            try
            {
                var filterPassingPSMs = 0;
                var missingNTerminalReporterIon = 0;
                var missingReporterIon = 0;

                foreach (var observation in filteredPSMs.Values.SelectMany(psm => psm.Observations.Where(observation => observation.PassesFilter)))
                {
                    filterPassingPSMs++;
                    if (observation.MissingNTermReporterIon)
                    {
                        missingNTerminalReporterIon++;
                    }

                    if (observation.MissingReporterIon)
                    {
                        missingReporterIon++;
                    }
                }

                if (filterPassingPSMs == 0)
                    return;

                mFDRBasedCounts.PercentPSMsMissingNTermReporterIon = missingNTerminalReporterIon / (float)filterPassingPSMs * 100;
                mFDRBasedCounts.PercentPSMsMissingReporterIon = missingReporterIon / (float)filterPassingPSMs * 100;
            }
            catch (Exception ex)
            {
                SetErrorMessage("Error in VerifyReporterIonPTMs: " + ex.Message, ex);
                Console.WriteLine(ex.StackTrace);
            }
        }

        /// <summary>
        /// Lookup dataset name using dataset ID
        /// </summary>
        /// <remarks>True if success, false if an error, including if the dataset is not found in the database</remarks>
        /// <param name="datasetID"></param>
        /// <param name="datasetName">Output: dataset name, if found</param>
        private bool LookupDatasetNameByID(int datasetID, out string datasetName)
        {
            datasetName = string.Empty;

            try
            {
                if (datasetID <= 0)
                    return false;

                var queryDatasetID = "Select Dataset From V_Dataset_Export Where ID = " + datasetID;

                var dbTools = DbToolsFactory.GetDBTools(mConnectionString, debugMode: mTraceMode);
                RegisterEvents(dbTools);

                // ReSharper disable once ExplicitCallerInfoArgument
                var success = dbTools.GetQueryResults(queryDatasetID, out var queryResults, callingFunction: "LookupDatasetNameByID");

                if (!success)
                {
                    OnWarningEvent("GetQueryResults returned false querying V_Dataset_Export with dataset ID: " + datasetID);
                    return false;
                }

                if (queryResults.Count == 0)
                {
                    OnWarningEvent(string.Format("Dataset ID {0} not found in the database; cannot determine dataset name", datasetID));
                    return false;
                }

                datasetName = queryResults[0][0];
                return true;
            }
            catch (Exception ex)
            {
                SetErrorMessage("Exception looking up dataset name using dataset ID: " + ex.Message, ex);
                return false;
            }
        }

        /// <summary>
        /// Lookup the total scans and number of MS/MS scans for the dataset defined by property DatasetName
        /// </summary>
        /// <remarks>True if success, false if an error, including if DatasetName is empty or if the dataset is not found in the database</remarks>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="totalSpectra">Output: number of spectra in the dataset</param>
        /// <param name="totalMSnSpectra">Output: number of MS/MS spectra in the dataset</param>
        /// <param name="warnIfNotFound">When true, if the dataset is not found, show a warning message</param>
        private bool LookupScanStats(string datasetName, out int totalSpectra, out int totalMSnSpectra, bool warnIfNotFound = true)
        {
            totalSpectra = 0;
            totalMSnSpectra = 0;

            try
            {
                if (string.IsNullOrEmpty(datasetName))
                {
                    SetErrorMessage("Dataset name is empty; cannot lookup scan stats");
                    return false;
                }

                var queryScanStats = " SELECT Scan_Count_Total, " +
                                     "        SUM(CASE WHEN Scan_Type LIKE '%MSn' THEN Scan_Count ELSE 0 END) AS ScanCountMSn" +
                                     " FROM V_Dataset_Scans_Export DSE" + " WHERE Dataset = '" + datasetName + "' GROUP BY Scan_Count_Total";

                var dbTools = DbToolsFactory.GetDBTools(mConnectionString, debugMode: mTraceMode);
                RegisterEvents(dbTools);

                // ReSharper disable once ExplicitCallerInfoArgument
                var scanStatsSuccess = dbTools.GetQueryResults(queryScanStats, out var scanStatsFromDb, callingFunction: "LookupScanStats_V_Dataset_Scans_Export");

                if (scanStatsSuccess && scanStatsFromDb.Count > 0)
                {
                    foreach (var resultRow in scanStatsFromDb)
                    {
                        var scanCountTotal = resultRow[0];
                        var scanCountMSn = resultRow[1];

                        if (!int.TryParse(scanCountTotal, out totalSpectra))
                        {
                            break;
                        }

                        int.TryParse(scanCountMSn, out totalMSnSpectra);
                        return true;
                    }
                }

                var queryScanTotal = " SELECT [Scan Count] FROM V_Dataset_Export WHERE Dataset = '" + datasetName + "'";

                // ReSharper disable once ExplicitCallerInfoArgument
                var scanCountSuccess = dbTools.GetQueryResults(queryScanTotal, out var datasetScanCountFromDb, callingFunction: "LookupScanStats_V_Dataset_Export");

                if (scanCountSuccess && datasetScanCountFromDb.Count > 0)
                {
                    foreach (var resultRow in datasetScanCountFromDb)
                    {
                        var scanCountTotal = resultRow[0];

                        int.TryParse(scanCountTotal, out totalSpectra);
                        return true;
                    }
                }

                if (warnIfNotFound)
                    OnWarningEvent("Dataset not found in the database; cannot retrieve scan counts: " + datasetName);

                return false;
            }
            catch (Exception ex)
            {
                SetErrorMessage("Exception retrieving scan stats from the database: " + ex.Message, ex);
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
        private bool FilterAndComputeStats(
            bool usingMSGFOrEValueFilter,
            IDictionary<int, PSMInfo> normalizedPSMs,
            IDictionary<int, List<ProteinInfo>> seqToProteinMap,
            IDictionary<int, SequenceInfo> sequenceInfo)
        {
            var filteredPSMs = new Dictionary<int, PSMInfo>();

            var success = false;
            bool filterPSMs;

            // Make sure .PassesFilter is false for all of the observations
            foreach (var kvEntry in normalizedPSMs)
            {
                foreach (var observation in kvEntry.Value.Observations.Where(observation => observation.PassesFilter))
                {
                    observation.PassesFilter = false;
                }
            }

            if (usingMSGFOrEValueFilter)
            {
                if (ResultType == PeptideHitResultTypes.MSAlign)
                {
                    // Filter on EValue
                    success = FilterPSMsByEValue(EValueThreshold, normalizedPSMs, filteredPSMs);
                    filterPSMs = true;
                }
                else if (MSGFThreshold < 1)
                {
                    // Filter on MSGF (though for MSPathFinder we're using SpecEValue)
                    success = FilterPSMsByMSGF(MSGFThreshold, normalizedPSMs, filteredPSMs);
                    filterPSMs = true;
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
                    foreach (var observation in entry.Value.Observations.Where(observation => observation.FDR > FDRThreshold))
                    {
                        observation.PassesFilter = false;
                    }
                }
            }

            if (!success)
                return false;

            // Summarize the results, counting the number of peptides, unique peptides, and proteins
            // We also count phosphopeptides using several metrics
            ReportDebugMessage("Call SummarizeResults for " + filteredPSMs.Count + " Filtered PSMs", 3);

            success = SummarizeResults(usingMSGFOrEValueFilter, filteredPSMs, seqToProteinMap, sequenceInfo);

            ReportDebugMessage("SummarizeResults returned " + success, 3);

            if (success && mDynamicReporterIonPTM && !usingMSGFOrEValueFilter)
            {
                // Look for peptides that are missing a reporter ion at the N terminus or on a residue
                ComputeMissingReporterIonPercent(filteredPSMs);
            }

            return success;
        }

        /// <summary>
        /// Filter the data using mFDRThreshold
        /// </summary>
        /// <param name="psmResults">PSM results (keys are NormalizedSeqID, values are the protein and scan info for each normalized sequence)</param>
        /// <returns>True if success; false if no reverse hits are present or if none of the data has MSGF values</returns>
        private bool FilterPSMsByFDR(IDictionary<int, PSMInfo> psmResults)
        {
            var fdrAlreadyComputed = true;
            var valuesWithFDR = 0;
            var resultIDtoFDRMap = new Dictionary<int, double>();

            foreach (var psmResult in psmResults)
            {
                if (psmResult.Value.BestFDR < 0)
                {
                    fdrAlreadyComputed = false;

                    resultIDtoFDRMap.Add(psmResult.Key, 1);
                }
                else
                {
                    valuesWithFDR++;
                    resultIDtoFDRMap.Add(psmResult.Key, psmResult.Value.BestFDR);
                }
            }

            if (fdrAlreadyComputed)
            {
                // Remove entries from psmResults where .FDR is larger than FDRThreshold
                FilterPSMsByFDR(psmResults, resultIDtoFDRMap, FDRThreshold);

                return true;
            }

            // Sort the data by ascending SpecEValue, then step through the list and compute FDR
            // Use FDR = #Reverse / #Forward
            //
            // Alternative FDR formula is: FDR = 2 * #Reverse / (#Forward + #Reverse)
            // But, since MS-GF+ uses "#Reverse / #Forward" we'll use that here too
            //
            // If no reverse hits are present or if none of the data has MSGF values, we'll clear psmResults and update mErrorMessage

            // Populate a list with the MSGF values and ResultIDs so that we can step through the data and compute the FDR for each entry
            var msgfToResultIDMap = new List<KeyValuePair<double, int>>();

            var validMSGFOrEValue = false;
            foreach (var psmResult in psmResults)
            {
                if (psmResult.Value.BestMSGF < PSMInfo.UNKNOWN_MSGF_SPEC_EVALUE)
                {
                    msgfToResultIDMap.Add(new KeyValuePair<double, int>(psmResult.Value.BestMSGF, psmResult.Key));
                    if (psmResult.Value.BestMSGF < 1)
                        validMSGFOrEValue = true;
                }
                else
                {
                    msgfToResultIDMap.Add(new KeyValuePair<double, int>(psmResult.Value.BestEValue, psmResult.Key));
                    if (psmResult.Value.BestEValue < PSMInfo.UNKNOWN_EVALUE)
                        validMSGFOrEValue = true;
                }
            }

            if (!validMSGFOrEValue)
            {
                // None of the data has MSGF values or E-Values; cannot compute FDR using MSGF or E-Value

                if (valuesWithFDR / (double)psmResults.Count > 0.20)
                {
                    // 20% or more of the psmResults does have a valid FDR; use that data for filtering
                    FilterPSMsByFDR(psmResults, resultIDtoFDRMap, FDRThreshold);

                    return true;
                }

                SetErrorMessage("Data does not contain MSGF values or E-Values; cannot compute a decoy-based FDR");
                psmResults.Clear();
                return false;
            }

            // Sort msgfToResultIDMap
            msgfToResultIDMap.Sort(new MSGFtoResultIDMapComparer());

            var forwardResults = 0;
            var decoyResults = 0;
            var missedResultIDsAtStart = new List<int>();

            var resultIDtoFDRMapFromDecoy = new Dictionary<int, double>();

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
                    decoyResults++;
                }
                else
                {
                    forwardResults++;
                }

                if (forwardResults > 0)
                {
                    // Compute and store the FDR for this entry
                    var fdrThreshold = decoyResults / (float)forwardResults;
                    resultIDtoFDRMapFromDecoy.Add(resultItem.Value, fdrThreshold);

                    if (missedResultIDsAtStart.Count > 0)
                    {
                        foreach (var resultID in missedResultIDsAtStart)
                        {
                            resultIDtoFDRMapFromDecoy.Add(resultID, fdrThreshold);
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

                if (valuesWithFDR / (double)psmResults.Count > 0.20)
                {
                    // 20% or more of the psmResults does have a valid FDR; use that data for filtering
                    FilterPSMsByFDR(psmResults, resultIDtoFDRMap, FDRThreshold);

                    return true;
                }

                OnWarningEvent("Data does not contain decoy proteins; cannot compute a decoy-based FDR");
                psmResults.Clear();
                return false;
            }

            // Remove entries from psmResults where .FDR is larger than FDRThreshold
            FilterPSMsByFDR(psmResults, resultIDtoFDRMapFromDecoy, FDRThreshold);

            return true;
        }

        /// <summary>
        /// Remove entries from psmResults where .FDR is larger than fdrThreshold
        /// </summary>
        private void FilterPSMsByFDR(IDictionary<int, PSMInfo> psmResults, Dictionary<int, double> resultIDtoFDRMap, double fdrThreshold)
        {
            foreach (var resultItem in resultIDtoFDRMap)
            {
                if (resultItem.Value > fdrThreshold)
                {
                    psmResults.Remove(resultItem.Key);
                }
            }
        }

        private bool FilterPSMsByEValue(double eValueThreshold, IDictionary<int, PSMInfo> psmResults, IDictionary<int, PSMInfo> filteredPSMs)
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

        private bool FilterPSMsByMSGF(double msgfThreshold, IDictionary<int, PSMInfo> psmResults, IDictionary<int, PSMInfo> filteredPSMs)
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
        /// Search normalizedPeptidesByCleanSequence for an entry that either exactly matches normalizedPeptide
        /// or nearly matches normalizedPeptide
        /// </summary>
        /// <remarks>A near match is one where the position of each modified residue is the same or just one residue apart</remarks>
        /// <param name="normalizedPeptidesByCleanSequence">Existing tracked normalized peptides; key is clean sequence, value is a list of normalized peptide info structs</param>
        /// <param name="newNormalizedPeptide">New normalized peptide</param>
        /// <returns>The Sequence ID of a matching normalized peptide, or -1 if no match</returns>
        public static int FindNormalizedSequence(
            IReadOnlyDictionary<string, List<NormalizedPeptideInfo>> normalizedPeptidesByCleanSequence,
            NormalizedPeptideInfo newNormalizedPeptide)
        {
            // ReSharper disable once CommentTypo
            // Find normalized peptides with the new normalized peptide's clean sequence

            if (!normalizedPeptidesByCleanSequence.TryGetValue(newNormalizedPeptide.CleanSequence, out var normalizedPeptideCandidates))
            {
                return PSMInfo.UNKNOWN_SEQUENCE_ID;
            }

            // Step through the normalized peptides that correspond to newNormalizedPeptide.CleanSequence
            // Note that each candidate will have an empty CleanSequence value because of how they are stored in normalizedPeptidesByCleanSequence

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
                        // The affected residues are at the same residue or are one residue apart
                        residueMatchCount++;
                    }
                }

                if (residueMatchCount == candidate.Modifications.Count)
                {
                    // Match found
                    return candidate.SeqID;
                }
            }

            return PSMInfo.UNKNOWN_SEQUENCE_ID;
        }

        private string GetMaxQuantDatasetIdOrName(PSM currentPSM)
        {
            var datasetID = currentPSM.GetScoreInt(MaxQuantSynFileReader.GetColumnNameByID(MaxQuantSynFileColumns.DatasetID));
            if (datasetID > 0)
                return datasetID.ToString();

            return currentPSM.GetScore(MaxQuantSynFileReader.GetColumnNameByID(MaxQuantSynFileColumns.Dataset));
        }

        private StartupOptions GetMinimalMemoryPHRPStartupOptions()
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

        /// <summary>
        /// Get the RegEx for matching keratin proteins
        /// </summary>
        /// <remarks>Used by SMAQC</remarks>
        public static Regex GetKeratinRegEx()
        {
            // RegEx to match keratin proteins, including
            //   K1C9_HUMAN, K1C10_HUMAN, K1CI_HUMAN
            //   K2C1_HUMAN, K2C1B_HUMAN, K2C3_HUMAN, K2C6C_HUMAN, K2C71_HUMAN
            //   K22E_HUMAN And K22O_HUMAN
            //   Contaminant_K2C1_HUMAN
            //   Contaminant_K22E_HUMAN
            //   Contaminant_K1C9_HUMAN
            //   Contaminant_K1C10_HUMAN
            return new(@"(K[1-2]C\d+[A-K]*|K22[E,O]|K1CI)_HUMAN", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        }

        /// <summary>
        /// Get the RegEx for matching trypsin proteins
        /// </summary>
        /// <remarks>Used by SMAQC</remarks>
        public static Regex GetTrypsinRegEx()
        {
            // RegEx to match trypsin proteins, including
            //   TRYP_PIG, sp|TRYP_PIG, Contaminant_TRYP_PIG, Cntm_P00761|TRYP_PIG
            //   Contaminant_TRYP_BOVIN And gi|136425|sp|P00760|TRYP_BOVIN
            //   Contaminant_Trypa
            return new("(TRYP_(PIG|BOVIN)|Contaminant_Trypa)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        }

        /// <summary>
        /// Initialize a NormalizedPeptideInfo object using a clean sequence and list of modifications,
        /// </summary>
        /// <param name="peptideCleanSequence"></param>
        /// <param name="modifications"></param>
        /// <param name="seqID"></param>
        public static NormalizedPeptideInfo GetNormalizedPeptideInfo(
            string peptideCleanSequence,
            IEnumerable<KeyValuePair<string, int>> modifications,
            int seqID)
        {
            var normalizedPeptide = new NormalizedPeptideInfo(peptideCleanSequence);
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

                var dbTools = mStoredProcedureExecutor;
                var reportThreshold = MSGFThreshold;
                var thresholdIsEValue = 0;
                if (ResultType == PeptideHitResultTypes.MSAlign)
                {
                    reportThreshold = EValueThreshold;
                    thresholdIsEValue = 1;
                }

                // Round to two decimals places
                var percentPSMsMissingNTermReporterIon = (float)Math.Round(mFDRBasedCounts?.PercentPSMsMissingNTermReporterIon ?? 0, 2);
                var percentPSMsMissingReporterIon = (float)Math.Round(mFDRBasedCounts?.PercentPSMsMissingReporterIon ?? 0, 2);

                var cmd = dbTools.CreateCommand(STORE_JOB_PSM_RESULTS_SP_NAME, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@return", SqlType.Int, ParameterDirection.ReturnValue);
                dbTools.AddTypedParameter(cmd, "@job", SqlType.Int, value: job);
                dbTools.AddTypedParameter(cmd, "@msgfThreshold", SqlType.Float, value: reportThreshold);
                dbTools.AddTypedParameter(cmd, "@fdrThreshold", SqlType.Float, value: FDRThreshold);
                dbTools.AddTypedParameter(cmd, "@spectraSearched", SqlType.Int, value: SpectraSearched);
                dbTools.AddTypedParameter(cmd, "@totalPSMs", SqlType.Int, value: mMSGFBasedCounts.TotalPSMs);
                dbTools.AddTypedParameter(cmd, "@uniquePeptides", SqlType.Int, value: mMSGFBasedCounts.UniquePeptideCount);
                dbTools.AddTypedParameter(cmd, "@uniqueProteins", SqlType.Int, value: mMSGFBasedCounts.UniqueProteinCount);
                dbTools.AddTypedParameter(cmd, "@totalPSMsFDRFilter", SqlType.Int, value: mFDRBasedCounts?.TotalPSMs ?? 0);
                dbTools.AddTypedParameter(cmd, "@uniquePeptidesFDRFilter", SqlType.Int, value: mFDRBasedCounts?.UniquePeptideCount ?? 0);
                dbTools.AddTypedParameter(cmd, "@uniqueProteinsFDRFilter", SqlType.Int, value: mFDRBasedCounts?.UniqueProteinCount ?? 0);
                dbTools.AddTypedParameter(cmd, "@msgfThresholdIsEValue", SqlType.TinyInt, value: thresholdIsEValue);
                dbTools.AddTypedParameter(cmd, "@percentMSnScansNoPSM", SqlType.Real, value: PercentMSnScansNoPSM);
                dbTools.AddTypedParameter(cmd, "@maximumScanGapAdjacentMSn", SqlType.Int, value: MaximumScanGapAdjacentMSn);
                dbTools.AddTypedParameter(cmd, "@uniquePhosphopeptideCountFDR", SqlType.Int, value: mFDRBasedCounts?.UniquePhosphopeptideCount ?? 0);
                dbTools.AddTypedParameter(cmd, "@uniquePhosphopeptidesCTermK", SqlType.Int, value: mFDRBasedCounts?.UniquePhosphopeptidesCTermK ?? 0);
                dbTools.AddTypedParameter(cmd, "@uniquePhosphopeptidesCTermR", SqlType.Int, value: mFDRBasedCounts?.UniquePhosphopeptidesCTermR ?? 0);
                dbTools.AddTypedParameter(cmd, "@missedCleavageRatio", SqlType.Real, value: mFDRBasedCounts?.MissedCleavageRatio ?? 0);
                dbTools.AddTypedParameter(cmd, "@missedCleavageRatioPhospho", SqlType.Real, value: mFDRBasedCounts?.MissedCleavageRatioPhospho ?? 0);
                dbTools.AddTypedParameter(cmd, "@trypticPeptides", SqlType.Int, value: mFDRBasedCounts?.TrypticPeptides ?? 0);
                dbTools.AddTypedParameter(cmd, "@keratinPeptides", SqlType.Int, value: mFDRBasedCounts?.KeratinPeptides ?? 0);
                dbTools.AddTypedParameter(cmd, "@trypsinPeptides", SqlType.Int, value: mFDRBasedCounts?.TrypsinPeptides ?? 0);
                dbTools.AddTypedParameter(cmd, "@dynamicReporterIon", SqlType.TinyInt, value: mDynamicReporterIonPTM);
                dbTools.AddTypedParameter(cmd, "@percentPSMsMissingNTermReporterIon", SqlType.Float, value: percentPSMsMissingNTermReporterIon);
                dbTools.AddTypedParameter(cmd, "@percentPSMsMissingReporterIon", SqlType.Float, value: percentPSMsMissingReporterIon);
                dbTools.AddTypedParameter(cmd, "@uniqueAcetylPeptidesFDR", SqlType.Int, value: mFDRBasedCounts?.AcetylPeptides ?? 0);

                // Execute the SP (retry the call up to 3 times)
                var result = mStoredProcedureExecutor.ExecuteSP(cmd, out var errorMessage, MAX_RETRY_COUNT);

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
                SetErrorMessage("Exception storing PSM Results in database: " + ex.Message, ex);
                success = false;
            }

            return success;
        }

        /// <summary>
        /// Process this dataset's synopsis file to determine the PSM stats
        /// </summary>
        /// <remarks>If synopsisFilePath is an empty string it will be auto-determined</remarks>
        /// <param name="synopsisFileNameFromPHRP">Optional: Synopsis file name, as reported by PHRP</param>
        /// <returns>True if success, false if an error</returns>
        public bool ProcessPSMResults(string synopsisFileNameFromPHRP = "")
        {
            DatasetScanStatsLookupError = false;

            try
            {
                mDynamicReporterIonPTM = false;
                mDynamicReporterIonType = ReporterIonTypes.None;
                mDynamicReporterIonName = string.Empty;

                mErrorMessage = string.Empty;

                mMSGFBasedCounts?.Clear();
                mFDRBasedCounts?.Clear();

                SpectraSearched = 0;

                /////////////////////
                // Define the file paths

                // We use the First-hits file to determine the number of MS/MS spectra that were searched (unique combo of charge and scan number)
                string phrpFirstHitsFileName;

                // We use the Synopsis file to count the number of peptides and proteins observed
                string phrpSynopsisFileName;

                // ReSharper disable ConvertIfStatementToConditionalTernaryExpression

                if (string.IsNullOrWhiteSpace(synopsisFileNameFromPHRP))
                {
                    phrpSynopsisFileName = ReaderFactory.GetPHRPSynopsisFileName(ResultType, mDatasetName);
                }
                else
                {
                    phrpSynopsisFileName = synopsisFileNameFromPHRP;
                }

                if (ResultType is
                    PeptideHitResultTypes.XTandem or PeptideHitResultTypes.MSAlign or
                    PeptideHitResultTypes.MODa or PeptideHitResultTypes.MODPlus or
                    PeptideHitResultTypes.MSPathFinder or PeptideHitResultTypes.MaxQuant)
                {
                    // These tools do not have first-hits files; use the Synopsis file instead to determine scan counts
                    phrpFirstHitsFileName = phrpSynopsisFileName;
                }
                else
                {
                    phrpFirstHitsFileName = ReaderFactory.GetPHRPFirstHitsFileName(ResultType, mDatasetName);
                }

                // ReSharper restore ConvertIfStatementToConditionalTernaryExpression

                if (phrpSynopsisFileName == null)
                    throw new NullReferenceException(nameof(phrpSynopsisFileName) + " is null");

                if (string.IsNullOrWhiteSpace(phrpSynopsisFileName))
                    throw new Exception(nameof(phrpSynopsisFileName) + " is an empty string");

                var modSummaryFileName = ReaderFactory.GetPHRPModSummaryFileName(ResultType, DatasetName);

                const string MAXQ_MOD_SUMMARY_FILE_SUFFIX = "_maxq_syn_ModSummary.txt";

                if (ResultType == PeptideHitResultTypes.MaxQuant &&
                    modSummaryFileName.Equals("Aggregation" + MAXQ_MOD_SUMMARY_FILE_SUFFIX, StringComparison.OrdinalIgnoreCase))
                {
                    // Need to switch from Aggregation_maxq_syn_ModSummary.txt to the actual ModSummary.txt file name

                    if (string.IsNullOrWhiteSpace(synopsisFileNameFromPHRP))
                    {
                        SetErrorMessage("Variable phrpSynopsisFileName is an empty string for this aggregation job, cannot summarize results; this is unexpected");
                        return false;
                    }

                    var index = phrpSynopsisFileName.IndexOf("_maxq_syn", StringComparison.OrdinalIgnoreCase);
                    if (index < 0)
                    {
                        SetErrorMessage("Did not find '_maxq_syn' in phrpSynopsisFileName for this aggregation job; this is unexpected: " + phrpSynopsisFileName);
                        return false;
                    }

                    var baseName = phrpSynopsisFileName.Substring(0, index);
                    modSummaryFileName = baseName + MAXQ_MOD_SUMMARY_FILE_SUFFIX;
                }

                mMSGFSynopsisFileName = Path.GetFileNameWithoutExtension(phrpSynopsisFileName) + MSGF_RESULT_FILENAME_SUFFIX;

                var phrpFirstHitsFilePath = Path.Combine(mWorkDir, phrpFirstHitsFileName);
                var phrpSynopsisFilePath = Path.Combine(mWorkDir, phrpSynopsisFileName);
                var phrpModSummaryFilePath = Path.Combine(mWorkDir, modSummaryFileName);

                if (!File.Exists(phrpSynopsisFilePath))
                {
                    SetErrorMessage("File not found, cannot summarize results: " + phrpSynopsisFilePath);
                    return false;
                }

                if (!File.Exists(phrpModSummaryFilePath))
                {
                    OnWarningEvent("ModSummary.txt file not found; will not be able to examine dynamic mods while summarizing results");
                }
                else
                {
                    ParseModSummaryFile(phrpModSummaryFilePath);
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

                // The keys in this dictionary are NormalizedSeqID values, which are custom-assigned
                // by this class to keep track of peptide sequences on a basis where modifications are tracked with some wiggle room
                // For example, LS*SPATLNSR and LSS*PATLNSR are considered equivalent
                // But P#EPT*IDES and PEP#T*IDES and P#EPTIDES* are all different
                //
                // The values contain mapped protein name, FDR, and MSGF SpecEValue, and the scans that the normalized peptide was observed in
                // We'll deal with multiple proteins for each peptide later when we parse the _ResultToSeqMap.txt and _SeqToProteinMap.txt files
                // If those files are not found, we'll simply use the protein information stored in psmResults
                var normalizedPSMs = new Dictionary<int, PSMInfo>();

                var successLoading = LoadPSMs(phrpSynopsisFilePath, normalizedPSMs, out _, out var seqToProteinMap, out var sequenceInfo);
                if (!successLoading)
                {
                    return false;
                }

                ////////////////////
                // Filter on MSGF or EValue and compute the stats
                //
                ReportDebugMessage("Call FilterAndComputeStats with usingMSGFOrEValueFilter = true", 3);

                var success = FilterAndComputeStats(usingMSGFOrEValueFilter: true, normalizedPSMs, seqToProteinMap, sequenceInfo);

                ReportDebugMessage("FilterAndComputeStats returned " + success, 3);

                ////////////////////
                // Filter on FDR and compute the stats
                //
                ReportDebugMessage("Call FilterAndComputeStats with usingMSGFOrEValueFilter = false", 3);

                var successViaFDR = FilterAndComputeStats(usingMSGFOrEValueFilter: false, normalizedPSMs, seqToProteinMap, sequenceInfo);

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

                if (!ContactDatabase)
                {
                    SetErrorMessage("Cannot post results to the database because ContactDatabase is False");
                    return false;
                }

                if (mJob == 0)
                {
                    ReportDebugMessage("Cannot call PostJobPSMResults since the job could not be determined");
                    return false;
                }

                ReportDebugMessage("Call PostJobPSMResults for job " + mJob);

                var psmResultsPosted = PostJobPSMResults(mJob);

                ReportDebugMessage("PostJobPSMResults returned " + psmResultsPosted);

                return psmResultsPosted;
            }
            catch (Exception ex)
            {
                SetErrorMessage("Error in ProcessPSMResults: " + ex.Message, ex);
                Console.WriteLine(ex.StackTrace);
                return false;
            }
        }

        /// <summary>
        /// Loads the PSMs (peptide identification for each scan)
        /// Normalizes the peptide sequence (mods are tracked, but no longer associated with specific residues) and populates normalizedPSMs
        /// </summary>
        /// <param name="phrpSynopsisFilePath"></param>
        /// <param name="normalizedPSMs">Dictionary where keys are Sequence ID and values are PSMInfo objects</param>
        /// <param name="resultToSeqMap">SortedList mapping PSM ResultID to Sequence ID</param>
        /// <param name="seqToProteinMap">Dictionary where keys are sequence ID and values are a list of protein info</param>
        /// <param name="sequenceInfo">Dictionary where keys are sequence ID and values are information about the sequence</param>
        /// <returns>True if success, false if an error</returns>
        private bool LoadPSMs(
            string phrpSynopsisFilePath,
            IDictionary<int, PSMInfo> normalizedPSMs,
            out SortedList<int, int> resultToSeqMap,
            out SortedList<int, List<ProteinInfo>> seqToProteinMap,
            out SortedList<int, SequenceInfo> sequenceInfo)
        {
            var specEValue = PSMInfo.UNKNOWN_MSGF_SPEC_EVALUE;
            var eValue = PSMInfo.UNKNOWN_EVALUE;

            var loadMSGFResults = true;

            // RegEx for determining that a peptide has a missed cleavage (i.e. an internal tryptic cleavage point)
            var missedCleavageMatcher = new Regex("[KR][^P][A-Z]", RegexOptions.Compiled);

            // RegEx to match keratin proteins
            var keratinProteinMatcher = GetKeratinRegEx();

            // RegEx to match trypsin proteins
            var trypsinProteinMatcher = GetTrypsinRegEx();

            resultToSeqMap = new SortedList<int, int>();
            seqToProteinMap = new SortedList<int, List<ProteinInfo>>();
            sequenceInfo = new SortedList<int, SequenceInfo>();

            try
            {
                if (ResultType is
                    PeptideHitResultTypes.MaxQuant or
                    PeptideHitResultTypes.MODa or
                    PeptideHitResultTypes.MODPlus or
                    PeptideHitResultTypes.MSPathFinder or
                    PeptideHitResultTypes.MSAlign)
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
                var seqMapReader = new PHRPSeqMapReader(mDatasetName, mWorkDir, ResultType);

                var sequenceInfoAvailable = false;

                if (!string.IsNullOrEmpty(seqMapReader.ResultToSeqMapFilename))
                {
                    var resultToSeqMapFilePath = ReaderFactory.FindResultToSeqMapFile(seqMapReader.InputDirectoryPath,
                                                                                      phrpSynopsisFilePath,
                                                                                      seqMapReader.ResultToSeqMapFilename,
                                                                                      out _);

                    if (!string.IsNullOrWhiteSpace(resultToSeqMapFilePath))
                    {
                        var success = seqMapReader.GetProteinMapping(resultToSeqMap, seqToProteinMap, sequenceInfo);

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
                // The SeqID value tracked by normalizedPeptideType is the SeqID of the first sequence to get normalized to the given entry
                // If sequenceInfoAvailable is False, values are the ResultID value of the first peptide to get normalized to the given entry
                //
                var normalizedPeptidesByCleanSequence = new Dictionary<string, List<NormalizedPeptideInfo>>();

                // This is used to avoid storing multiple PSMs for a given scan
                // For MaxQuant results, we store DatasetNameOrId_ScanNumber
                // For all other results, we simply store scan number (as a string)
                var scansStored = new SortedSet<string>();

                OnStatusEvent("Reading PSMs from " + PathUtils.CompactPathString(phrpSynopsisFilePath, 80));
                var lastStatusTime = DateTime.UtcNow;

                using var reader = new ReaderFactory(phrpSynopsisFilePath, startupOptions);
                RegisterEvents(reader);

                while (reader.MoveNext())
                {
                    var currentPSM = reader.CurrentPSM;

                    if (currentPSM.ScoreRank > 1)
                    {
                        // Only keep the first match for each spectrum
                        continue;
                    }

                    string datasetIdOrName;
                    string scanKey;
                    if (ResultType == PeptideHitResultTypes.MaxQuant)
                    {
                        datasetIdOrName = GetMaxQuantDatasetIdOrName(currentPSM);
                        scanKey = string.Format("{0}_{1}", datasetIdOrName, currentPSM.ScanNumber);
                    }
                    else
                    {
                        datasetIdOrName = string.Empty;
                        scanKey = currentPSM.ScanNumber.ToString();
                    }

                    if (currentPSM.ScanNumber > 0 && scansStored.Contains(scanKey))
                    {
                        // Skip this PSM since its scan key is already in scansStored
                        continue;
                    }

                    scansStored.Add(scanKey);

                    if (!(DateTime.UtcNow.Subtract(lastStatusTime).TotalMilliseconds < 500))
                    {
                        Console.Write(".");
                        lastStatusTime = DateTime.UtcNow;
                    }

                    var valid = false;

                    if (ResultType == PeptideHitResultTypes.MSAlign)
                    {
                        // Use the EValue reported by MSAlign

                        if (currentPSM.TryGetScore(MSAlignSynFileReader.GetColumnNameByID(MSAlignSynFileColumns.EValue), out var eValueText))
                        {
                            valid = double.TryParse(eValueText, out eValue);
                        }
                    }
                    else if (ResultType is PeptideHitResultTypes.MODa or PeptideHitResultTypes.MODPlus)
                    {
                        // MODa / MODPlus results don't have spectral probability, but they do have FDR
                        valid = true;
                    }
                    else if (ResultType == PeptideHitResultTypes.MSPathFinder)
                    {
                        // Use SpecEValue in place of SpecProb
                        valid = true;

                        if (currentPSM.TryGetScore(MSPathFinderSynFileReader.GetColumnNameByID(MSPathFinderSynFileColumns.SpecEValue), out var specEValueText))
                        {
                            if (!string.IsNullOrWhiteSpace(specEValueText))
                            {
                                valid = double.TryParse(specEValueText, out specEValue);
                            }
                        }

                        // SpecEValue was not present
                        // That's OK, QValue should be present
                    }
                    else if (ResultType == PeptideHitResultTypes.MaxQuant)
                    {
                        // Use PEP for specEValue
                        if (currentPSM.TryGetScore(MaxQuantSynFileReader.GetColumnNameByID(MaxQuantSynFileColumns.PEP), out var posteriorErrorProbabilityText))
                        {
                            valid = double.TryParse(posteriorErrorProbabilityText, out specEValue);
                        }
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

                    var psmInfo = new PSMInfo();
                    psmInfo.Clear();

                    psmInfo.Protein = currentPSM.ProteinFirst;

                    var psmMSGF = specEValue;
                    var psmEValue = eValue;
                    double psmFDR;

                    switch (ResultType)
                    {
                        case PeptideHitResultTypes.MaxQuant:
                            psmFDR = currentPSM.GetScoreDbl(MaxQuantSynFileReader.GetColumnNameByID(MaxQuantSynFileColumns.QValue), PSMInfo.UNKNOWN_FDR);
                            break;

                        case PeptideHitResultTypes.MODa:
                            psmFDR = currentPSM.GetScoreDbl(MODaSynFileReader.GetColumnNameByID(MODaSynFileColumns.QValue), PSMInfo.UNKNOWN_FDR);
                            break;

                        case PeptideHitResultTypes.MODPlus:
                            psmFDR = currentPSM.GetScoreDbl(MODPlusSynFileReader.GetColumnNameByID(MODPlusSynFileColumns.QValue), PSMInfo.UNKNOWN_FDR);
                            break;

                        case PeptideHitResultTypes.MSAlign:
                            psmFDR = currentPSM.GetScoreDbl(MSAlignSynFileReader.GetColumnNameByID(MSAlignSynFileColumns.FDR), PSMInfo.UNKNOWN_FDR);
                            break;

                        case PeptideHitResultTypes.MSGFPlus:
                            psmFDR = currentPSM.GetScoreDbl(MSGFPlusSynFileReader.GetColumnNameByID(MSGFPlusSynFileColumns.QValue), PSMInfo.UNKNOWN_FDR);

                            if (psmFDR < 0)
                            {
                                psmFDR = currentPSM.GetScoreDbl(MSGFPlusSynFileReader.GetColumnNameByID(MSGFPlusSynFileColumns.EFDR), PSMInfo.UNKNOWN_FDR);
                            }

                            break;

                        case PeptideHitResultTypes.MSPathFinder:
                            psmFDR = currentPSM.GetScoreDbl(MSPathFinderSynFileReader.GetColumnNameByID(MSPathFinderSynFileColumns.QValue), PSMInfo.UNKNOWN_FDR);
                            break;

                        case PeptideHitResultTypes.Inspect:
                        case PeptideHitResultTypes.MSFragger:
                        case PeptideHitResultTypes.Sequest:
                        case PeptideHitResultTypes.TopPIC:
                        case PeptideHitResultTypes.XTandem:
                        case PeptideHitResultTypes.Unknown:
                            psmFDR = PSMInfo.UNKNOWN_FDR;
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    var normalizedPeptide = new NormalizedPeptideInfo(string.Empty);

                    var normalized = false;
                    var seqID = PSMInfo.UNKNOWN_SEQUENCE_ID;

                    if (sequenceInfoAvailable && resultToSeqMap != null)
                    {
                        if (!resultToSeqMap.TryGetValue(currentPSM.ResultID, out seqID))
                        {
                            seqID = PSMInfo.UNKNOWN_SEQUENCE_ID;

                            // This result is not listed in the _ResultToSeqMap file, likely because it was already processed for this scan
                            // Look for a match in normalizedPeptidesByCleanSequence that matches this peptide's clean sequence

                            if (normalizedPeptidesByCleanSequence.TryGetValue(currentPSM.PeptideCleanSequence, out var normalizedPeptides))
                            {
                                foreach (var normalizedItem in normalizedPeptides)
                                {
                                    if (normalizedItem.SeqID != PSMInfo.UNKNOWN_SEQUENCE_ID)
                                    {
                                        // Match found; use the given SeqID value
                                        seqID = normalizedItem.SeqID;
                                        break;
                                    }
                                }
                            }
                        }

                        if (seqID != PSMInfo.UNKNOWN_SEQUENCE_ID)
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

                    var normalizedSeqID = FindNormalizedSequence(normalizedPeptidesByCleanSequence, normalizedPeptide);

                    if (normalizedSeqID != PSMInfo.UNKNOWN_SEQUENCE_ID)
                    {
                        // We're already tracking this normalized peptide (or one very similar to it)

                        var normalizedPSMInfo = normalizedPSMs[normalizedSeqID];
                        var addObservation = true;

                        foreach (var observation in normalizedPSMInfo.Observations)
                        {
                            if (!(observation.DatasetIdOrName == datasetIdOrName && observation.Scan == currentPSM.ScanNumber))
                            {
                                continue;
                            }

                            // Scan already stored
                            // Update the scores if this PSM has a better score than the cached one

                            if (psmFDR > PSMInfo.UNKNOWN_FDR && psmFDR < observation.FDR)
                            {
                                observation.FDR = psmFDR;
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

                        if (addObservation)
                        {
                            var observation = new PSMInfo.PSMObservation
                            {
                                DatasetIdOrName = datasetIdOrName,
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

                        if (seqID == PSMInfo.UNKNOWN_SEQUENCE_ID)
                        {
                            seqID = currentPSM.ResultID;
                        }

                        if (!normalizedPeptidesByCleanSequence.TryGetValue(normalizedPeptide.CleanSequence, out var normalizedPeptides))
                        {
                            normalizedPeptides = new List<NormalizedPeptideInfo>();
                            normalizedPeptidesByCleanSequence.Add(normalizedPeptide.CleanSequence, normalizedPeptides);
                        }

                        // Make a new normalized peptide entry that does not have clean sequence
                        // (to conserve memory, since keys in dictionary normalizedPeptides are clean sequence)
                        var normalizedPeptideToStore = new NormalizedPeptideInfo(string.Empty);
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
                        else if (missedCleavageMatcher.IsMatch(normalizedPeptide.CleanSequence))
                        {
                            Console.WriteLine("NumMissedCleavages is zero but the peptide matches the MissedCleavage RegEx; this is unexpected");
                            psmInfo.MissedCleavage = true;
                        }

                        // Check whether this peptide is from Keratin or a related protein
                        foreach (var proteinName in currentPSM.Proteins)
                        {
                            if (keratinProteinMatcher.IsMatch(proteinName))
                            {
                                psmInfo.KeratinPeptide = true;
                                break;
                            }
                        }

                        // Check whether this peptide is from Trypsin or a related protein
                        foreach (var proteinName in currentPSM.Proteins)
                        {
                            if (trypsinProteinMatcher.IsMatch(proteinName))
                            {
                                psmInfo.TrypsinPeptide = true;
                                break;
                            }
                        }

                        // Check whether this peptide is partially or fully tryptic
                        if (currentPSM.CleavageState is
                            PeptideCleavageStateCalculator.PeptideCleavageState.Full or
                            PeptideCleavageStateCalculator.PeptideCleavageState.Partial)
                        {
                            psmInfo.Tryptic = true;
                        }

                        // Check whether this is a phosphopeptide
                        // This check only works if the _ModSummary.txt file was loaded because it relies on the mod name being Phosph
                        foreach (var modification in normalizedPeptide.Modifications)
                        {
                            if (string.Equals(modification.Key, "Phosph", StringComparison.OrdinalIgnoreCase))
                            {
                                psmInfo.Phosphopeptide = true;
                                break;
                            }

                            if (string.Equals(modification.Key, "Acetyl", StringComparison.OrdinalIgnoreCase) ||
                                string.Equals(modification.Key, "AcNoTMT", StringComparison.OrdinalIgnoreCase))
                            {
                                psmInfo.AcetylPeptide = true;
                                break;
                            }
                        }

                        var observation = new PSMInfo.PSMObservation
                        {
                            DatasetIdOrName = datasetIdOrName,
                            Scan = currentPSM.ScanNumber,
                            FDR = psmFDR,
                            MSGF = psmMSGF,
                            EValue = psmEValue
                        };

                        if (mDynamicReporterIonPTM)
                        {
                            ValidateReporterIonPTMs(normalizedPeptide, observation);
                        }

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

                Console.WriteLine();

                return true;
            }
            catch (Exception ex)
            {
                SetErrorMessage("Error in LoadPSMs: " + ex.Message, ex);
                Console.WriteLine(ex.StackTrace);
                return false;
            }
        }

        /// <summary>
        /// Parse a sequence with mod symbols
        /// </summary>
        /// <param name="sequenceWithMods"></param>
        /// <param name="seqID"></param>
        private NormalizedPeptideInfo NormalizeSequence(string sequenceWithMods, int seqID)
        {
            var aminoAcidList = new StringBuilder(sequenceWithMods.Length);
            var modList = new List<KeyValuePair<string, int>>();

            PeptideCleavageStateCalculator.SplitPrefixAndSuffixFromSequence(sequenceWithMods, out var primarySequence, out _, out _);

            for (var index = 0; index <= primarySequence.Length - 1; index++)
            {
                if (ReaderFactory.IsLetterAtoZ(primarySequence[index]))
                {
                    aminoAcidList.Append(primarySequence[index]);
                }
                else
                {
                    modList.Add(new KeyValuePair<string, int>(primarySequence[index].ToString(), aminoAcidList.Length));
                }
            }

            return GetNormalizedPeptideInfo(aminoAcidList.ToString(), modList, seqID);
        }

        private NormalizedPeptideInfo NormalizeSequence(string peptideCleanSequence, SequenceInfo seqInfo, int seqID)
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
                    var residueNumber = 0;

                    if (colonIndex > 0)
                    {
                        modName = modDescriptor.Substring(0, colonIndex);
                        int.TryParse(modDescriptor.Substring(colonIndex + 1), out residueNumber);
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

                    modList.Add(new KeyValuePair<string, int>(modName, residueNumber));
                }
            }

            return GetNormalizedPeptideInfo(peptideCleanSequence, modList, seqID);
        }

        private void ParseModSummaryFile(string phrpModSummaryFilePath)
        {
            try
            {
                var headersParsed = false;
                var columnMap = new Dictionary<string, int>();

                var columnNamesByIdentifier = new Dictionary<string, SortedSet<string>>();

                DataTableUtils.AddColumnIdentifier(columnNamesByIdentifier, "Modification_Symbol");
                DataTableUtils.AddColumnIdentifier(columnNamesByIdentifier, "Modification_Mass");
                DataTableUtils.AddColumnIdentifier(columnNamesByIdentifier, "Target_Residues");
                DataTableUtils.AddColumnIdentifier(columnNamesByIdentifier, "Modification_Type");
                DataTableUtils.AddColumnIdentifier(columnNamesByIdentifier, "Mass_Correction_Tag");
                DataTableUtils.AddColumnIdentifier(columnNamesByIdentifier, "Occurrence_Count");

                var reporterIonNames = new Dictionary<string, ReporterIonTypes>(StringComparer.OrdinalIgnoreCase)
                {
                    // ReSharper disable StringLiteralTypo
                    {"TMT6Tag", ReporterIonTypes.TMT6plex},         // DMS Mass_Correction_Tag name
                    {"TMT6plex", ReporterIonTypes.TMT6plex},        // UniMod name
                    {"TMT16Tag", ReporterIonTypes.TMT16plex},       // DMS Mass_Correction_Tag name
                    {"TMT16plex", ReporterIonTypes.TMT16plex},      // Non-standard (should not be encountered)
                    {"TMTpro", ReporterIonTypes.TMT16plex},         // UniMod name
                    {"itrac", ReporterIonTypes.iTRAQ4plex},         // DMS Mass_Correction_Tag name
                    {"iTRAQ4plex", ReporterIonTypes.iTRAQ4plex},    // UniMod name
                    {"iTRAQ", ReporterIonTypes.iTRAQ4plex},         // UniMod synonym
                    {"iTRAQ8", ReporterIonTypes.iTRAQ8plex},        // DMS Mass_Correction_Tag name
                    {"iTRAQ8plex", ReporterIonTypes.iTRAQ8plex}     // UniMod name
                    // ReSharper restore StringLiteralTypo
                };

                using var reader = new StreamReader(new FileStream(phrpModSummaryFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    if (!headersParsed)
                    {
                        headersParsed = true;

                        if (dataLine.StartsWith("Modification_"))
                        {
                            DataTableUtils.GetColumnMappingFromHeaderLine(columnMap, dataLine, columnNamesByIdentifier);
                            continue;
                        }

                        // Missing header line; assume the order
                        columnMap.Add("Modification_Symbol", 0);
                        columnMap.Add("Modification_Mass", 1);
                        columnMap.Add("Target_Residues", 2);
                        columnMap.Add("Modification_Type", 3);
                        columnMap.Add("Mass_Correction_Tag", 4);
                    }

                    var resultRow = dataLine.Split('\t');

                    // var modificationSymbol = DataTableUtils.GetColumnValue(resultRow, columnMap, "Modification_Symbol");
                    // var targetResidues = DataTableUtils.GetColumnValue(resultRow, columnMap, "Target_Residues");
                    var modificationType = DataTableUtils.GetColumnValue(resultRow, columnMap, "Modification_Type");
                    var massCorrectionTag = DataTableUtils.GetColumnValue(resultRow, columnMap, "Mass_Correction_Tag");

                    if (!modificationType.Equals("D"))
                        continue;

                    if (!reporterIonNames.TryGetValue(massCorrectionTag, out var reporterIonType))
                        continue;

                    if (!mDynamicReporterIonPTM)
                    {
                        mDynamicReporterIonPTM = true;
                        mDynamicReporterIonType = reporterIonType;
                        mDynamicReporterIonName = massCorrectionTag;
                        continue;
                    }

                    if (mDynamicReporterIonType != reporterIonType)
                    {
                        OnWarningEvent(string.Format(
                            "ModSummary.txt file has a mix of reporter ion types: {0} and {1}",
                            mDynamicReporterIonType, reporterIonType));
                    }

                    if (!mDynamicReporterIonName.Equals(massCorrectionTag))
                    {
                        OnWarningEvent(string.Format(
                            "ModSummary.txt file has a mix of reporter ion mod names: {0} and {1}",
                            mDynamicReporterIonName, massCorrectionTag));
                    }
                }
            }
            catch (Exception ex)
            {
                OnWarningEvent("Exception parsing the ModSummary file: " + ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        private void ReportDebugMessage(string message, int debugLevel = 2)
        {
            if (mDebugLevel >= debugLevel)
            {
                OnDebugEvent(message);
            }
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

                using var writer = new StreamWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

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
                    SpectraSearched.ToString()
                };

                AppendStats(mMSGFBasedCounts, stats);
                AppendStats(mFDRBasedCounts, stats);

                writer.WriteLine(string.Join("\t", stats));
            }
            catch (Exception ex)
            {
                SetErrorMessage("Exception saving results to " + outputFilePath + ": " + ex.Message, ex);
            }
        }

        private void SetErrorMessage(string errMsg, Exception ex = null)
        {
            Console.WriteLine(errMsg);
            mErrorMessage = errMsg;
            OnErrorEvent(errMsg, ex);
        }

        /// <summary>
        /// Summarize the results by inter-relating filteredPSMs, resultToSeqMap, and seqToProteinMap
        /// </summary>
        /// <param name="usingMSGFOrEValueFilter"></param>
        /// <param name="filteredPSMs">Filter-passing results (keys are NormalizedSeqID, values are the protein and scan info for each normalized sequence)</param>
        /// <param name="seqToProteinMap">Sequence to protein map (keys are sequence ID, values are proteins)</param>
        /// <param name="sequenceInfo">Sequence information (keys are sequence ID, values are sequences</param>
        private bool SummarizeResults(
            bool usingMSGFOrEValueFilter,
            IDictionary<int, PSMInfo> filteredPSMs,
            IDictionary<int, List<ProteinInfo>> seqToProteinMap,
            IDictionary<int, SequenceInfo> sequenceInfo)
        {
            try
            {
                // The Keys in this dictionary are SeqID values; the values track observation count, whether the peptide ends in K or R, etc.
                // Populated from data in filteredPSMs, where peptides with the same sequence and same modifications (+/- 1 residue) were collapsed
                // For example, LS*SPATLNSR and LSS*PATLNSR are considered equivalent
                // But P#EPT*IDES and PEP#T*IDES and P#EPTIDES* are all different
                // (the collapsing of similar peptides is done in method LoadPSMs with the call to FindNormalizedSequence)
                var uniqueSequences = new Dictionary<int, UniqueSeqInfo>();

                // The Keys in this dictionary are SeqID values; the values track observation count, whether the peptide ends in K or R, etc.
                var uniquePhosphopeptides = new Dictionary<int, UniqueSeqInfo>();

                // The Keys in this dictionary are SeqID values; the values track observation count, whether the peptide ends in K or R, etc.
                var uniqueAcetylPeptides = new Dictionary<int, UniqueSeqInfo>();

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

                    // Make a deep copy of result.Value as class UniqueSeqInfo
                    var seqInfoToStore = result.Value.CloneAsSeqInfo(obsCountForResult);

                    AddUpdateUniqueSequence(uniqueSequences, seqId, seqInfoToStore);

                    if (result.Value.Phosphopeptide)
                    {
                        AddUpdateUniqueSequence(uniquePhosphopeptides, seqId, seqInfoToStore);
                    }

                    if (result.Value.AcetylPeptide)
                    {
                        AddUpdateUniqueSequence(uniqueAcetylPeptides, seqId, seqInfoToStore);
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
                var psmStats = TabulatePSMStats(uniqueSequences, uniqueProteins, uniquePhosphopeptides, uniqueAcetylPeptides);

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
                SetErrorMessage("Exception summarizing results: " + ex.Message, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Tabulate PSM stats
        /// </summary>
        /// <param name="uniqueSequences">Keys in this dictionary are SeqID values; the values track observation count, whether the peptide ends in K or R, etc.</param>
        /// <param name="uniqueProteins">Keys in this dictionary are protein names; the values are observation count</param>
        /// <param name="uniquePhosphopeptides">Keys in this dictionary are SeqID values; the values track observation count, whether the peptide ends in K or R, etc.</param>
        /// <param name="uniqueAcetylPeptides">Keys in this dictionary are SeqID values; the values track observation count, whether the peptide ends in K or R, etc.</param>
        private PSMStats TabulatePSMStats(
            IDictionary<int, UniqueSeqInfo> uniqueSequences,
            IDictionary<string, int> uniqueProteins,
            IDictionary<int, UniqueSeqInfo> uniquePhosphopeptides,
            IDictionary<int, UniqueSeqInfo> uniqueAcetylPeptides)
        {
            var psmStats = new PSMStats()
            {
                TotalPSMs = (from item in uniqueSequences select item.Value.ObsCount).Sum(),
                UniquePeptideCount = uniqueSequences.Count,
                UniqueProteinCount = uniqueProteins.Count,
                MissedCleavageRatio = ComputeMissedCleavageRatio(uniqueSequences),
                KeratinPeptides = (from item in uniqueSequences where item.Value.KeratinPeptide select item.Key).Count(),
                TrypsinPeptides = (from item in uniqueSequences where item.Value.TrypsinPeptide select item.Key).Count(),
                TrypticPeptides = (from item in uniqueSequences where item.Value.Tryptic select item.Key).Count(),
                AcetylPeptides = uniqueAcetylPeptides.Count,
                UniquePhosphopeptideCount = uniquePhosphopeptides.Count,
                UniquePhosphopeptidesCTermK = (from item in uniquePhosphopeptides where item.Value.CTermK select item.Key).Count(),
                UniquePhosphopeptidesCTermR = (from item in uniquePhosphopeptides where item.Value.CTermR select item.Key).Count(),
                MissedCleavageRatioPhospho = ComputeMissedCleavageRatio(uniquePhosphopeptides)
            };

            return psmStats;
        }

        private void ValidateReporterIonPTMs(NormalizedPeptideInfo normalizedPeptide, PSMInfo.PSMObservation observation)
        {
            var labeledNTerminus = false;
            var labeledLysineCount = 0;

            // The search had a reporter ion (like TMT or iTRAQ) as a dynamic mod
            // Check whether this peptide is missing the reporter ion from a required location
            foreach (var modification in normalizedPeptide.Modifications)
            {
                if (string.Equals(modification.Key, mDynamicReporterIonName, StringComparison.OrdinalIgnoreCase))
                {
                    var residueNumber = modification.Value;
                    var currentResidue = normalizedPeptide.CleanSequence[residueNumber - 1];

                    if (residueNumber == 1)
                    {
                        labeledNTerminus = true;
                    }

                    if (currentResidue == 'K')
                    {
                        labeledLysineCount++;
                    }
                }
            }

            var lysineCount = normalizedPeptide.CleanSequence.Count(residue => residue == 'K');

            switch (mDynamicReporterIonType)
            {
                case ReporterIonTypes.iTRAQ4plex:
                case ReporterIonTypes.iTRAQ8plex:
                case ReporterIonTypes.TMT6plex:
                case ReporterIonTypes.TMT16plex:
                    if (normalizedPeptide.CleanSequence.StartsWith("K"))
                    {
                        // Because of how mods are listed in the _SeqInfo.txt file, we need to bump up lysineCount by one
                        lysineCount++;
                    }

                    if (!labeledNTerminus)
                    {
                        observation.MissingNTermReporterIon = true;
                    }

                    if (!labeledNTerminus || labeledLysineCount < lysineCount)
                    {
                        observation.MissingReporterIon = true;
                    }
                    break;

                case ReporterIonTypes.None:
                    throw new Exception("Invalid ReporterIonType encountered in ValidateReporterIonPTMs");

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        /// <summary>
        /// Custom comparer for sorting msgfToResultIDMap
        /// </summary>
        private class MSGFtoResultIDMapComparer : IComparer<KeyValuePair<double, int>>
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
