using System.Collections.Generic;

namespace MSGFResultsSummarizer
{
    /// <summary>
    /// Tracks the mods for a given normalized peptide
    /// </summary>
    public class NormalizedPeptideInfo
    {
        /// <summary>
        /// Peptide clean sequence (no mod symbols)
        /// </summary>
        /// <remarks>This field is empty in dictNormalizedPeptides because the keys in the dictionary are the clean sequence</remarks>
        public string CleanSequence { get; }

        /// <summary>
        /// List of modified amino acids
        /// </summary>
        /// <remarks>Keys are mod names or symbols; values are the 1-based residue number</remarks>
        public List<KeyValuePair<string, int>> Modifications { get; }

        /// <summary>
        /// Sequence ID for this normalized peptide
        /// </summary>
        public int SeqID { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="peptideCleanSequence">Peptide clean sequence (no mod symbols)</param>
        public NormalizedPeptideInfo(string peptideCleanSequence)
        {
            CleanSequence = peptideCleanSequence;
            Modifications = new List<KeyValuePair<string, int>>();
            SeqID = -1;
        }

        public void StoreModifications(IEnumerable<KeyValuePair<string, int>> newModifications)
        {
            Modifications.Clear();
            Modifications.AddRange(newModifications);
        }

        /// <summary>
        /// Show the sequence ID, clean sequence, and number of modified residues
        /// </summary>
        public override string ToString()
        {
            return string.Format("{0}: {1}, ModCount={2}",
                SeqID,
                CleanSequence,
                Modifications?.Count ?? 0);
        }
    }
}
