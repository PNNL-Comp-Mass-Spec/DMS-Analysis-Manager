namespace MSGFResultsSummarizer
{
    public class UniqueSeqInfo
    {
        private int mObsCount;

        /// <summary>
        /// Observation count
        /// </summary>
        /// <remarks>Overridden in PSMInfo because it tracks specific PSMObservations</remarks>
        public virtual int ObsCount => mObsCount;

        /// <summary>
        /// True if the C-terminus of the peptide is Lysine
        /// </summary>
        public bool CTermK { get; set; }

        /// <summary>
        /// True if the C-terminus of the peptide is Arginine
        /// </summary>
        public bool CTermR { get; set; }

        /// <summary>
        /// True if the peptide has an internal K or R that is not followed by P
        /// </summary>
        public bool MissedCleavage { get; set; }

        /// <summary>
        /// True if the peptide is from a keratin protein
        /// </summary>
        public bool KeratinPeptide { get; set; }

        /// <summary>
        /// True if the peptide is from a trypsin protein
        /// </summary>
        public bool TrypsinPeptide { get; set; }

        /// <summary>
        /// True if the peptide is partially or fully tryptic
        /// </summary>
        public bool Tryptic { get; set; }

        /// <summary>
        /// True if the peptide has any acetylated K residues
        /// </summary>
        public bool AcetylPeptide { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public UniqueSeqInfo()
        {
            // ReSharper disable once VirtualMemberCallInConstructor
            Clear();
        }

        public virtual void Clear()
        {
            mObsCount = 0;
            CTermK = false;
            CTermR = false;
            MissedCleavage = false;
            KeratinPeptide = false;
            TrypsinPeptide = false;
            Tryptic = false;
        }

        public virtual void UpdateObservationCount(int observationCount)
        {
            mObsCount = observationCount;
        }
    }
}
