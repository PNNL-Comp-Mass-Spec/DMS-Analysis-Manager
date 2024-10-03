namespace MSGFResultsSummarizer
{
    public class PSMResults
    {
        // Ignore Spelling: acetyl, acetylated, MSGF, phospho, phosphopeptide, phosphopeptides, PSM, tryptic, Ubiquitin

        /// <summary>
        /// MS-GF SpecProb threshold
        /// </summary>
        public double MSGFThreshold { get; set; }

        /// <summary>
        /// FDR Threshold
        /// </summary>
        public double FDRThreshold { get; set; }

        /// <summary>
        /// Number of spectra searched, as reported by the search tool
        /// </summary>
        public int SpectraSearched { get; set; }

        /// <summary>
        /// Total peptides passing the MS-GF SpecProb threshold
        /// </summary>
        public int TotalPSMs { get; set; }

        /// <summary>
        /// Unique peptide count passing the MS-GF SpecProb threshold
        /// </summary>
        public int UniquePeptides { get; set; }

        /// <summary>
        /// Unique protein count passing the MS-GF SpecProb threshold
        /// </summary>
        public int UniqueProteins { get; set; }

        /// <summary>
        /// Total peptides passing the FDR threshold
        /// </summary>
        public int TotalPSMsFDRFilter { get; set; }

        /// <summary>
        /// Unique peptide count passing the FDR threshold
        /// </summary>
        public int UniquePeptidesFDRFilter { get; set; }

        /// <summary>
        /// Unique protein count passing the FDR threshold
        /// </summary>
        public int UniqueProteinsFDRFilter { get; set; }

        /// <summary>
        /// True if the MSGF Threshold Is E-Value based
        /// </summary>
        public bool MsgfThresholdIsEValue { get; set; }

        /// <summary>
        /// Percent MSn Scans No PSM
        /// </summary>
        public float PercentMSnScansNoPSM { get; set; }

        /// <summary>
        /// Maximum Scan Gap Adjacent MSn
        /// </summary>
        public int MaximumScanGapAdjacentMSn { get; set; }

        /// <summary>
        /// Unique Phosphopeptide Count FDR
        /// </summary>
        public int UniquePhosphopeptideCountFDR { get; set; }

        /// <summary>
        /// Number of distinct phosphopeptides (filtered by FDR) with K on the C-terminus
        /// </summary>
        public int UniquePhosphopeptidesCTermK { get; set; }

        /// <summary>
        /// UNumber of distinct phosphopeptides (filtered by FDR) with R on the C-terminus
        /// </summary>
        public int UniquePhosphopeptidesCTermR { get; set; }

        /// <summary>
        /// Percent of unique peptides (filtered by FDR) with a missed cleavage (internal K or R)
        /// </summary>
        public float MissedCleavageRatio { get; set; }

        /// <summary>
        /// Percent of distinct phosphopeptides (filtered by FDR) with a missed cleavage (internal K or R)
        /// </summary>
        public float MissedCleavageRatioPhospho { get; set; }

        /// <summary>
        /// Unique number of filter-passing peptides that are fully or partially tryptic (filtered using the FDR threshold)
        /// </summary>
        public int TrypticPeptides { get; set; }

        /// <summary>
        /// Number of unique peptides (filtered by FDR) that come from Keratin proteins
        /// </summary>
        public int KeratinPeptides { get; set; }

        /// <summary>
        /// Number of unique peptides (filtered by FDR) that come from Trypsin proteins
        /// </summary>
        public int TrypsinPeptides { get; set; }

        /// <summary>
        /// True if dynamic reporter ions were used
        /// </summary>
        public bool DynamicReporterIon { get; set; }

        /// <summary>
        /// Percent of FDR filter-passing PSMs that are missing a reporter ion on the peptide N-terminus
        /// </summary>
        /// <remarks>
        /// Only applicable if the reporter ion was searched as a dynamic mod
        /// </remarks>
        public float PercentPSMsMissingNTermReporterIon { get; set; }

        /// <summary>
        /// Percent of FDR filter-passing PSMs that are missing a reporter ion from any of the lysine residues or from the peptide N-terminus
        /// </summary>
        /// <remarks>
        /// Only applicable if the reporter ion was searched as a dynamic mod
        /// </remarks>
        public float PercentPSMsMissingReporterIon { get; set; }

        /// <summary>
        /// Unique acetylated peptides passing the FDR threshold (any K with acetyl)
        /// </summary>
        public int UniqueAcetylPeptidesFDR { get; set; }

        /// <summary>
        /// Unique ubiquitinated peptides passing the FDR threshold (any K with ubiquitin)
        /// </summary>
        public int UniqueUbiquitinPeptidesFDR { get; set; }

        /// <summary>
        /// Update the cached PSM result stats using additional PSM result stats
        /// </summary>
        /// <remarks>
        /// Stats are updated by adding the new counts to the existing counts. This is valid for SpectraSearched, TotalPSMs, and TotalPSMsFDRFilter,
        /// but is misleading for UniquePeptides, UniqueProteins, UniquePeptidesFDRFilter, and UniqueProteinsFDRFilter.
        /// </remarks>
        /// <param name="psmResults">PSM results</param>
        public void AppendResults(PSMResults psmResults)
        {
            var originalScanCount = SpectraSearched;
            var scanCountAdded = psmResults.SpectraSearched;

            var originalTotalPSMs = TotalPSMs;
            var psmCountAdded = psmResults.TotalPSMs;

            SpectraSearched += psmResults.SpectraSearched;
            TotalPSMs += psmResults.TotalPSMs;
            UniquePeptides += psmResults.UniquePeptides;
            UniqueProteins += psmResults.UniqueProteins;
            TotalPSMsFDRFilter += psmResults.TotalPSMsFDRFilter;
            UniquePeptidesFDRFilter += psmResults.UniquePeptidesFDRFilter;
            UniqueProteinsFDRFilter += psmResults.UniqueProteinsFDRFilter;

            PercentMSnScansNoPSM = UpdatePercent(PercentMSnScansNoPSM, originalScanCount, scanCountAdded, psmResults.PercentMSnScansNoPSM);

            if (psmResults.MaximumScanGapAdjacentMSn > MaximumScanGapAdjacentMSn)
                MaximumScanGapAdjacentMSn = psmResults.MaximumScanGapAdjacentMSn;

            UniquePhosphopeptideCountFDR += psmResults.UniquePhosphopeptideCountFDR;
            UniquePhosphopeptidesCTermK += psmResults.UniquePhosphopeptidesCTermK;
            UniquePhosphopeptidesCTermR += psmResults.UniquePhosphopeptidesCTermR;

            MissedCleavageRatio = UpdatePercent(MissedCleavageRatio, originalTotalPSMs, psmCountAdded, psmResults.MissedCleavageRatio);

            MissedCleavageRatioPhospho = UpdatePercent(MissedCleavageRatioPhospho, originalTotalPSMs, psmCountAdded, psmResults.MissedCleavageRatioPhospho);

            TrypticPeptides += psmResults.TrypticPeptides;
            KeratinPeptides += psmResults.KeratinPeptides;
            TrypsinPeptides += psmResults.TrypsinPeptides;

            PercentPSMsMissingNTermReporterIon = UpdatePercent(PercentPSMsMissingNTermReporterIon, originalTotalPSMs, psmCountAdded, psmResults.PercentPSMsMissingNTermReporterIon);
            PercentPSMsMissingReporterIon = UpdatePercent(PercentPSMsMissingReporterIon, originalTotalPSMs, psmCountAdded, psmResults.PercentPSMsMissingReporterIon);

            UniqueAcetylPeptidesFDR += psmResults.UniqueAcetylPeptidesFDR;
            UniqueUbiquitinPeptidesFDR += psmResults.UniqueUbiquitinPeptidesFDR;
        }

        private float UpdatePercent(float originalPercent, int originalTotal, int countAdded, float percentForAddedValues)
        {
            var originalCountAffected = originalTotal / 100.0F * originalPercent;

            var newCountAffected = originalCountAffected + countAdded / 100.0F * percentForAddedValues;

            var newTotal = originalTotal + countAdded;

            if (newTotal == 0)
                return 0;

            // Compute the new newAffectedPercent
            return newCountAffected / newTotal * 100;
        }
    }
}
