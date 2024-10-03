using System;
using System.Collections.Generic;
using System.Linq;

namespace MSGFResultsSummarizer
{
    public sealed class PSMInfo : UniqueSeqInfo
    {
        // Ignore Spelling: EValue, MSGF, phosphopeptide

        public const double UNKNOWN_MSGF_SPEC_EVALUE = 10;

        public const double UNKNOWN_EVALUE = double.MaxValue;
        public const int UNKNOWN_FDR = -1;
        public const int UNKNOWN_SEQUENCE_ID = -1;

        public override int ObsCount => Observations.Count;

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
        public List<PSMObservation> Observations { get; }

        /// <summary>
        /// Lowest value stored in MSGFSpecEValueOrPEP
        /// </summary>
        public double BestMSGFSpecEValueOrPEP
        {
            get
            {
                if (Observations.Count == 0)
                {
                    return UNKNOWN_MSGF_SPEC_EVALUE;
                }

                return (from item in Observations orderby item.MSGFSpecEValueOrPEP select item.MSGFSpecEValueOrPEP).First();
            }
        }

        public double BestEValue
        {
            get
            {
                if (Observations.Count == 0)
                {
                    return UNKNOWN_EVALUE;
                }

                return (from item in Observations orderby item.EValue select item.EValue).First();
            }
        }

        public double BestFDR
        {
            get
            {
                if (Observations.Count == 0)
                {
                    return UNKNOWN_FDR;
                }

                return (from item in Observations orderby item.FDR select item.FDR).First();
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public PSMInfo()
        {
            Observations = new List<PSMObservation>();
            Clear();
        }

        /// <summary>
        /// Reset the fields
        /// </summary>
        public override void Clear()
        {
            base.Clear();
            Protein = string.Empty;
            SeqIdFirst = UNKNOWN_SEQUENCE_ID;
            Observations?.Clear();
            Phosphopeptide = false;
        }

        /// <summary>
        /// Add a PSM Observation
        /// </summary>
        /// <param name="observation">PSM observation</param>
        public void AddObservation(PSMObservation observation)
        {
            Observations.Add(observation);
        }

        /// <summary>
        /// Clone this class as a new UniqueSeqInfo instance
        /// </summary>
        /// <param name="obsCountOverride">Observation count override; ignored if less than 0</param>
        public UniqueSeqInfo CloneAsSeqInfo(int obsCountOverride = -1)
        {
            var seqInfo = new UniqueSeqInfo();

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
            throw new InvalidOperationException("Observation count cannot be updated in PSMInfo");
        }

        public class PSMObservation
        {
            public string DatasetIdOrName { get; set; }

            public int Scan { get; set; }

            /// <summary>
            /// FDR (aka QValue)
            /// </summary>
            public double FDR { get; set; }

            /// <summary>
            /// For MS-GF+, stores MSGF SpecEValue; will be UNKNOWN_MSGF_SPEC_EVALUE (10) if MSGF SpecEValue is not available
            /// </summary>
            /// <remarks>
            /// <para>
            /// DIA-NN results store Posterior Error Probability (PEP) in this field
            /// </para>
            /// <para>
            /// MaxQuant results store Posterior Error Probability (PEP) in this field
            /// </para>
            /// <para>
            /// MSPathFinder results store SpecEValue in this field
            /// </para>
            /// <para>
            /// MSGFDB (the precursor to MS-GF+) stored MSGF SpecProb values in this field
            /// </para>
            /// </remarks>
            public double MSGFSpecEValueOrPEP { get; set; }

            /// <summary>
            /// Only used when MSGF SpecEValue is not available
            /// </summary>
            public double EValue { get; set; }

            /// <summary>
            /// True if the peptide is missing TMT or iTRAQ at the peptide N-terminus
            /// </summary>
            public bool MissingNTermReporterIon { get; set; }

            /// <summary>
            /// True if the peptide is missing TMT or iTRAQ at one or more locations (N-terminus and each lysine)
            /// </summary>
            public bool MissingReporterIon { get; set; }

            public bool PassesFilter { get; set; }

            /// <summary>
            /// Constructor
            /// </summary>
            public PSMObservation()
            {
                Clear();
            }

            public void Clear()
            {
                DatasetIdOrName = string.Empty;
                Scan = 0;
                FDR = UNKNOWN_FDR;
                MSGFSpecEValueOrPEP = UNKNOWN_MSGF_SPEC_EVALUE;
                EValue = UNKNOWN_EVALUE;
                MissingNTermReporterIon = false;
                MissingReporterIon = false;
                PassesFilter = false;
            }

            /// <summary>
            /// Show the scan number, FDR, and MSGF Spec EValue (or PEP)
            /// </summary>
            public override string ToString()
            {
                return string.Format("Scan {0}, FDR {1:F4}, MSGF {2:E3}", Scan, FDR, MSGFSpecEValueOrPEP);
            }
        }

        /// <summary>
        /// Show the sequence ID, protein, and number of observations (spectra)
        /// </summary>
        public override string ToString()
        {
            // ReSharper disable once UseStringInterpolation

            return Observations.Count switch
            {
                0 => string.Format("SeqID {0}, {1} (0 observations)", SeqIdFirst, Protein),
                1 => string.Format("SeqID {0}, {1}, Scan {2} (1 observation)", SeqIdFirst, Protein, Observations[0].Scan),
                _ => string.Format("SeqID {0}, {1}, Scans {2}-{3} ({4} observations)", SeqIdFirst, Protein, Observations[0].Scan,
                    Observations[Observations.Count - 1].Scan, Observations.Count)
            };
        }
    }
}
