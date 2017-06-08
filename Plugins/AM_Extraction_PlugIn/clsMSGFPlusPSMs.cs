using System;
using System.Collections.Generic;
using System.Linq;

namespace AnalysisManagerExtractionPlugin
{
    public class clsMSGFPlusPSMs
    {
        public struct udtPSMType
        {
            public string Peptide;
            public double SpecEValue;
            public string DataLine;
        }

        // Keys are the protein and peptide (separated by an undercore)
        // Values are the PSM details, including the original data line from the .TSV file
        private Dictionary<string, udtPSMType> mPSMs;

        // List of SpecEValues associated with this scan/charge
        private readonly SortedSet<double> mSpecEValues;

        private double mBestSpecEValue;
        private double mWorstSpecEValue;

        private readonly int mCharge;
        private readonly int mScan;

        private readonly int mMaximumPSMsToKeep;

        public int Charge
        {
            get { return mCharge; }
        }

        public int MaximumPSMsToKeep
        {
            get { return mMaximumPSMsToKeep; }
        }

        public List<udtPSMType> PSMs
        {
            get { return mPSMs.Values.ToList(); }
        }

        public int Scan
        {
            get { return mScan; }
        }

        public clsMSGFPlusPSMs(int scanNumber, int chargeState, int maximumPSMsToRetain)
        {
            mMaximumPSMsToKeep = maximumPSMsToRetain;
            if (mMaximumPSMsToKeep < 1)
                mMaximumPSMsToKeep = 1;

            mPSMs = new Dictionary<string, udtPSMType>();
            mSpecEValues = new SortedSet<double>();

            mBestSpecEValue = 0;
            mWorstSpecEValue = 0;

            mScan = scanNumber;
            mCharge = chargeState;
        }

        /// <summary>
        /// Adds the given PSM if the list has fewer than MaximumPSMsToKeep PSMs, or if the specEValue is less than the worst scoring entry in the list
        /// </summary>
        /// <param name="udtPSM"></param>
        /// <param name="protein"></param>
        /// <returns>True if the PSM was stored, otherwise false</returns>
        /// <remarks></remarks>
        public bool AddPSM(udtPSMType udtPSM, string protein)
        {
            udtPSM.Peptide = RemovePrefixAndSuffix(udtPSM.Peptide);

            var updateScores = false;
            var addPeptide = false;

            if (mSpecEValues.Count < mMaximumPSMsToKeep)
            {
                addPeptide = true;
            }
            else
            {
                if ((from item in mPSMs.Values where item.Peptide == udtPSM.Peptide select item).Any())
                {
                    addPeptide = true;
                }
            }

            var proteinPeptide = protein + "_" + udtPSM.Peptide;

            if (addPeptide)
            {
                udtPSMType udtExistingPSM;

                if (mPSMs.TryGetValue(proteinPeptide, out udtExistingPSM))
                {
                    if (udtExistingPSM.SpecEValue > udtPSM.SpecEValue)
                    {
                        udtExistingPSM.SpecEValue = udtPSM.SpecEValue;
                        // Update the dictionary (necessary since udtExistingPSM is a structure and not an object)
                        mPSMs[proteinPeptide] = udtExistingPSM;
                    }
                }
                else
                {
                    mPSMs.Add(proteinPeptide, udtPSM);
                }

                updateScores = true;
            }
            else
            {
                if (udtPSM.SpecEValue < mWorstSpecEValue)
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
                    mPSMs.Add(proteinPeptide, udtPSM);

                    updateScores = true;
                }
                else if (Math.Abs(udtPSM.SpecEValue - mBestSpecEValue) < double.Epsilon)
                {
                    // The new peptide has the same score as the best scoring peptide; keep it (and don't remove anything)

                    // Add the new PSM
                    mPSMs.Add(proteinPeptide, udtPSM);

                    updateScores = true;
                }
            }

            if (updateScores)
            {
                if (mPSMs.Count > 1)
                {
                    // Make sure all peptides have the same SpecEvalue
                    var bestScoreByPeptide = new Dictionary<string, double>();

                    foreach (var psm in mPSMs)
                    {
                        var peptideToFind = psm.Value.Peptide;
                        double storedScore = 0;
                        if (bestScoreByPeptide.TryGetValue(peptideToFind, out storedScore))
                        {
                            bestScoreByPeptide[peptideToFind] = Math.Min(storedScore, psm.Value.SpecEValue);
                        }
                        else
                        {
                            bestScoreByPeptide.Add(peptideToFind, psm.Value.SpecEValue);
                        }
                    }

                    foreach (var key in mPSMs.Keys.ToList())
                    {
                        var udtStoredPSM = mPSMs[key];
                        var bestScore = bestScoreByPeptide[udtStoredPSM.Peptide];
                        if (bestScore < udtStoredPSM.SpecEValue)
                        {
                            udtStoredPSM.SpecEValue = bestScore;
                            mPSMs[key] = udtStoredPSM;
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
            else
            {
                return false;
            }
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
