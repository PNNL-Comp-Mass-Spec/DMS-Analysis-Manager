namespace MSGFResultsSummarizer
{
    internal class PSMStats
    {
        // Ignore Spelling: acetyl, acetylated, phospho, phosphopeptide, phosphopeptides, phosphorylated, tryptic, Ubiquitin

        /// <summary>
        /// Number of spectra with a match
        /// </summary>
        public int TotalPSMs { get; set; }

        // ReSharper disable CommentTypo

        /// <summary>
        /// Number of distinct peptides
        /// </summary>
        /// <remarks>
        /// For modified peptides, collapses peptides with the same sequence and same modifications (+/- 1 residue)
        /// For example, LS*SPATLNSR and LSS*PATLNSR are considered equivalent
        /// But P#EPT*IDES and PEP#T*IDES and P#EPTIDES* are all different
        /// (the collapsing of similar peptides is done in method LoadPSMs with the call to FindNormalizedSequence)
        /// </remarks>
        public int UniquePeptideCount { get; set; }

        // ReSharper restore CommentTypo

        /// <summary>
        /// Number of distinct proteins
        /// </summary>
        public int UniqueProteinCount { get; set; }

        /// <summary>
        /// Number of distinct phosphopeptides
        /// </summary>
        /// <remarks>A peptide is counted as a phosphopeptide if any S, T, or Y is phosphorylated</remarks>
        public int UniquePhosphopeptideCount { get; set; }

        /// <summary>
        /// Number of distinct phosphopeptides with K on the C-terminus
        /// </summary>
        public int UniquePhosphopeptidesCTermK { get; set; }

        /// <summary>
        /// Number of distinct phosphopeptides with R on the C-terminus
        /// </summary>
        public int UniquePhosphopeptidesCTermR { get; set; }

        /// <summary>
        /// Number of unique peptides that come from Keratin proteins
        /// </summary>
        public int KeratinPeptides { get; set; }

        /// <summary>
        /// Number of unique peptides that come from Trypsin proteins
        /// </summary>
        public int TrypsinPeptides { get; set; }

        /// <summary>
        /// Number of unique peptides that are partially or fully tryptic
        /// </summary>
        public int TrypticPeptides { get; set; }

        /// <summary>
        /// Number of unique peptides that have at list one acetylated K
        /// </summary>
        public int AcetylPeptides { get; set; }

        /// <summary>
        /// Number of unique peptides that have at list one ubiquitinated K
        /// </summary>
        public int UbiquitinPeptides { get; set; }

        /// <summary>
        /// Fraction of peptides that have a missed cleavage (internal K or R)
        /// </summary>
        public float MissedCleavageRatio { get; set; }

        /// <summary>
        /// Fraction of phosphopeptides that have a missed cleavage (internal K or R)
        /// </summary>
        public float MissedCleavageRatioPhospho { get; set; }

        /// <summary>
        /// Percent of filter-passing PSMs that are missing a reporter ion on the peptide N-terminus
        /// </summary>
        /// <remarks>Value between 0 and 100</remarks>
        public float PercentPSMsMissingNTermReporterIon { get; set; }

        /// <summary>
        /// Percent of filter-passing PSMs that are missing a reporter ion
        /// </summary>
        /// <remarks>Value between 0 and 100</remarks>
        public float PercentPSMsMissingReporterIon { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public PSMStats()
        {
            Clear();
        }

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
            PercentPSMsMissingNTermReporterIon = 0;
            PercentPSMsMissingReporterIon = 0;
            KeratinPeptides = 0;
            TrypsinPeptides = 0;
            TrypticPeptides = 0;
            AcetylPeptides = 0;
            UbiquitinPeptides = 0;
        }
    }
}
