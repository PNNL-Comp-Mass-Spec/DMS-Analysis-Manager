using System;
using System.Collections.Generic;
using System.Linq;

namespace AnalysisManagerExtractionPlugin
{
    public class MSGFPlusPSMs
    {
        // Ignore Spelling: PSM

        public struct PSMInfo
        {
            public string Peptide;
            public double SpecEValue;
            public string DataLine;
        }

        // Keys are the protein and peptide (separated by an underscore)
        // Values are the PSM details, including the original data line from the .TSV file
        private readonly Dictionary<string, PSMInfo> mPSMs;

        // List of SpecEValues associated with this scan/charge
        private readonly SortedSet<double> mSpecEValues;

        private double mBestSpecEValue;
        private double mWorstSpecEValue;

        public int Charge { get; }

        public int MaximumPSMsToKeep { get; }

        public List<PSMInfo> PSMs => mPSMs.Values.ToList();

        public int Scan { get; }

        public MSGFPlusPSMs(int scanNumber, int chargeState, int maximumPSMsToRetain)
        {
            MaximumPSMsToKeep = maximumPSMsToRetain;

            if (MaximumPSMsToKeep < 1)
                MaximumPSMsToKeep = 1;

            mPSMs = new Dictionary<string, PSMInfo>();
            mSpecEValues = new SortedSet<double>();

            mBestSpecEValue = 0;
            mWorstSpecEValue = 0;

            Scan = scanNumber;
            Charge = chargeState;
        }

        /// <summary>
        /// Adds the given PSM if the list has fewer than MaximumPSMsToKeep PSMs, or if the specEValue is less than the worst scoring entry in the list
        /// </summary>
        /// <param name="psm">PSM info</param>
        /// <param name="protein">Protein name</param>
        /// <returns>True if the PSM was stored, otherwise false</returns>
        public bool AddPSM(PSMInfo psm, string protein)
        {
            psm.Peptide = RemovePrefixAndSuffix(psm.Peptide);

            var updateScores = false;
            var addPeptide = false;

            if (mSpecEValues.Count < MaximumPSMsToKeep)
            {
                addPeptide = true;
            }
            else
            {
                if ((from item in mPSMs.Values where item.Peptide == psm.Peptide select item).Any())
                {
                    addPeptide = true;
                }
            }

            var proteinPeptide = protein + "_" + psm.Peptide;

            if (addPeptide)
            {
                if (mPSMs.TryGetValue(proteinPeptide, out var existingPSM))
                {
                    if (existingPSM.SpecEValue > psm.SpecEValue)
                    {
                        existingPSM.SpecEValue = psm.SpecEValue;
                        // Update the dictionary (necessary since existingPSM is a structure and not an object)
                        mPSMs[proteinPeptide] = existingPSM;
                    }
                }
                else
                {
                    mPSMs.Add(proteinPeptide, psm);
                }

                updateScores = true;
            }
            else
            {
                if (psm.SpecEValue < mWorstSpecEValue)
                {
                    if (mPSMs.Count <= 1 || mSpecEValues.Count == 1)
                    {
                        mPSMs.Clear();
                    }
                    else
                    {
                        // Remove all entries in mPSMs for the worst scoring peptide (or tied peptides) in mSpecEValues
                        var keysToRemove = (from item in mPSMs where Math.Abs(item.Value.SpecEValue - mWorstSpecEValue) < double.Epsilon select item.Key).ToList();

                        foreach (var proteinPeptideKey in keysToRemove.Distinct())
                        {
                            mPSMs.Remove(proteinPeptideKey);
                        }
                    }

                    // Add the new PSM
                    mPSMs.Add(proteinPeptide, psm);

                    updateScores = true;
                }
                else if (Math.Abs(psm.SpecEValue - mBestSpecEValue) < double.Epsilon)
                {
                    // The new peptide has the same score as the best scoring peptide; keep it (and don't remove anything)

                    // Add the new PSM
                    mPSMs.Add(proteinPeptide, psm);

                    updateScores = true;
                }
            }

            if (updateScores)
            {
                if (mPSMs.Count > 1)
                {
                    // Make sure all peptides have the same SpecEvalue
                    var bestScoreByPeptide = new Dictionary<string, double>();

                    foreach (var item in mPSMs)
                    {
                        var peptideToFind = item.Value.Peptide;

                        if (bestScoreByPeptide.TryGetValue(peptideToFind, out var storedScore))
                        {
                            bestScoreByPeptide[peptideToFind] = Math.Min(storedScore, item.Value.SpecEValue);
                        }
                        else
                        {
                            bestScoreByPeptide.Add(peptideToFind, item.Value.SpecEValue);
                        }
                    }

                    foreach (var key in mPSMs.Keys.ToList())
                    {
                        var storedPSM = mPSMs[key];
                        var bestScore = bestScoreByPeptide[storedPSM.Peptide];

                        if (bestScore < storedPSM.SpecEValue)
                        {
                            storedPSM.SpecEValue = bestScore;
                            mPSMs[key] = storedPSM;
                        }
                    }
                }

                // Update the distinct list of SpecEValues
                mSpecEValues.Clear();
                mSpecEValues.UnionWith((from item in mPSMs select item.Value.SpecEValue).Distinct());

                mBestSpecEValue = mSpecEValues.First();
                mWorstSpecEValue = mSpecEValues.Last();

                return true;
            }

            return false;
        }

        public static string RemovePrefixAndSuffix(string peptide)
        {
            if (peptide.Length > 4)
            {
                if (peptide[1] == '.')
                {
                    peptide = peptide.Substring(2);
                }
                if (peptide[peptide.Length - 2] == '.')
                {
                    peptide = peptide.Substring(0, peptide.Length - 2);
                }
            }

            return peptide;
        }
    }
}
