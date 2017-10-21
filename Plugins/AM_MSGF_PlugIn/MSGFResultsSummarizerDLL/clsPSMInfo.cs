using System;
using System.Collections.Generic;
using System.Linq;

namespace MSGFResultsSummarizer
{
    public class clsPSMInfo : clsUniqueSeqInfo
    {
        [Obsolete("Use UNKNOWN_MSGF_SPEC_EVALUE")]
        public const double UNKNOWN_MSGF_SPECPROB = 10;

        public const double UNKNOWN_MSGF_SPEC_EVALUE = 10;

        public const double UNKNOWN_EVALUE = double.MaxValue;
        public const int UNKNOWN_FDR = -1;
        public const int UNKNOWN_SEQID = -1;

        private readonly List<PSMObservation> mObservations;

        public override int ObsCount => mObservations.Count;

        /// <summary>
        /// True if this is a phosphopeptide
        /// </summary>
        public bool Phosphopeptide { get; set; }

        /// <summary>
        /// Protein name (from the _fht.txt or _syn.txt file)
        /// </summary>
        public string Protein { get; set; }

        /// <summary>
        /// First sequence ID for this normalized peptide
        /// </summary>
        public int SeqIdFirst { get; set; }

        /// <summary>
        /// Details for each PSM that maps to this class
        /// </summary>
        public List<PSMObservation> Observations => mObservations;

        public double BestMSGF
        {
            get
            {
                if (mObservations.Count == 0)
                {
                    return UNKNOWN_MSGF_SPEC_EVALUE;
                }
                else
                {
                    return (from item in mObservations orderby item.MSGF select item.MSGF).First();
                }
            }
        }

        public double BestEValue
        {
            get
            {
                if (mObservations.Count == 0)
                {
                    return UNKNOWN_EVALUE;
                }
                else
                {
                    return (from item in mObservations orderby item.EValue select item.EValue).First();
                }
            }
        }

        public double BestFDR
        {
            get
            {
                if (mObservations.Count == 0)
                {
                    return UNKNOWN_FDR;
                }
                else
                {
                    return (from item in mObservations orderby item.FDR select item.FDR).First();
                }
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public clsPSMInfo()
        {
            mObservations = new List<PSMObservation>();
            Clear();
        }

        /// <summary>
        /// Reset the fields
        /// </summary>
        public override void Clear()
        {
            base.Clear();
            Protein = string.Empty;
            SeqIdFirst = UNKNOWN_SEQID;
            if (mObservations != null)
            {
                mObservations.Clear();
            }
            Phosphopeptide = false;
        }

        /// <summary>
        /// Add a PSM Observation
        /// </summary>
        /// <param name="observation"></param>
        public void AddObservation(PSMObservation observation)
        {
            mObservations.Add(observation);
        }

        /// <summary>
        /// Clone this class as a new clsUniqueSeqInfo instance
        /// </summary>
        /// <param name="obsCountOverride">Observation count override; ignored if less than 0</param>
        /// <returns></returns>
        public clsUniqueSeqInfo CloneAsSeqInfo(int obsCountOverride = -1)
        {
            var seqInfo = new clsUniqueSeqInfo();

            if (obsCountOverride >= 0)
            {
                seqInfo.UpdateObservationCount(obsCountOverride);
            }
            else
            {
                seqInfo.UpdateObservationCount(ObsCount);
            }

            seqInfo.CTermK = CTermK;
            seqInfo.CTermR = CTermR;
            seqInfo.MissedCleavage = MissedCleavage;
            seqInfo.KeratinPeptide = KeratinPeptide;
            seqInfo.TrypsinPeptide = TrypsinPeptide;
            seqInfo.Tryptic = Tryptic;

            return seqInfo;
        }

        public override void UpdateObservationCount(int observationCount)
        {
            throw new InvalidOperationException("Observation count cannot be updated in clsPSMInfo");
        }

        public override string ToString()
        {
            if (mObservations.Count == 0)
            {
                return string.Format("SeqID {0}, {1} (0 observations)", SeqIdFirst, Protein);
            }

            if (mObservations.Count == 1)
            {
                return string.Format("SeqID {0}, {1}, Scan {2} (1 observation)", SeqIdFirst, Protein, mObservations[0].Scan);
            }
            else
            {
                return string.Format("SeqID {0}, {1}, Scans {2}-{3} ({4} observations)", SeqIdFirst, Protein, mObservations[0].Scan,
                    mObservations[mObservations.Count - 1].Scan, mObservations.Count);
            }
        }

        public class PSMObservation
        {
            public int Scan { get; set; }

            /// <summary>
            /// FDR (aka QValue)
            /// </summary>
            public double FDR { get; set; }

            /// <summary>
            /// MSGF SpecEValue; will be UNKNOWN_MSGF_SPEC_EVALUE (10) if MSGF SpecEValue is not available
            /// </summary>
            /// <remarks>MSPathFinder results use this field to store SpecEValue</remarks>
            public double MSGF { get; set; }

            /// <summary>
            /// Only used when MSGF SpecEValue is not available
            /// </summary>
            public double EValue { get; set; }

            public bool PassesFilter { get; set; }

            public PSMObservation()
            {
                Clear();
            }

            public void Clear()
            {
                Scan = 0;
                FDR = UNKNOWN_FDR;
                MSGF = UNKNOWN_MSGF_SPEC_EVALUE;
                EValue = UNKNOWN_EVALUE;
                PassesFilter = false;
            }

            public override string ToString()
            {
                return string.Format("Scan {0}, FDR {1:F4}, MSGF {2:E3}", Scan, FDR, MSGF);
            }
        }
    }
}
