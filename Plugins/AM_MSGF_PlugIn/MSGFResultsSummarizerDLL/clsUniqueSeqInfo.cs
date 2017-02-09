namespace MSGFResultsSummarizer
{
    public class clsUniqueSeqInfo
    {
        private int mObsCount;

        /// <summary>
        /// Observation count
        /// </summary>
        /// <returns></returns>
        /// <remarks>Overridden in clsPSMInfo because it tracks specific PSMObservations</remarks>
        public virtual int ObsCount
        {
            get { return mObsCount; }
        }

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
        /// <returns></returns>
        public bool MissedCleavage { get; set; }

        /// <summary>
        /// True if the peptide is from a keratin protein
        /// </summary>
        /// <returns></returns>
        public bool KeratinPeptide { get; set; }

        /// <summary>
        /// True if the peptide is from a trypsin protein
        /// </summary>
        /// <returns></returns>
        public bool TrypsinPeptide { get; set; }

        /// <summary>
        /// True if the peptide is partially or fully tryptic
        /// </summary>
        /// <returns></returns>
        public bool Tryptic { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public clsUniqueSeqInfo()
        {
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
