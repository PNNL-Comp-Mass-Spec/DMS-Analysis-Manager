
namespace MSGFResultsSummarizer
{
    internal class clsPSMStats
    {
        /// <summary>
        /// Number of spectra with a match
        /// </summary>
        /// <remarks></remarks>
        public int TotalPSMs { get; set; }

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

        /// <summary>
        /// Number of distinct proteins
        /// </summary>
        /// <remarks></remarks>
        public int UniqueProteinCount { get; set; }

        public int UniquePhosphopeptideCount { get; set; }
        public int UniquePhosphopeptidesCTermK { get; set; }

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

        public float MissedCleavageRatio { get; set; }

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
        public clsPSMStats()
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
        }
    }
}
