using PHRPReader;
using PHRPReader.Data;
using PHRPReader.Reader;

namespace AnalysisManagerMSGFPlugin
{
    /// <summary>
    /// This class reads a SEQUEST _syn.txt file in support of creating the input file for MSGF
    /// </summary>
    public sealed class MSGFInputCreatorSequest : MSGFInputCreator
    {
        // Ignore Spelling: DelCn, Minima, tryptic

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="workDir">Working directory</param>
        public MSGFInputCreatorSequest(string datasetName, string workDir)
            : base(datasetName, workDir, PeptideHitResultTypes.Sequest)
        {
            // Initialize the file paths
            // This updates mPHRPFirstHitsFilePath and mPHRPSynopsisFilePath
            InitializeFilePaths();
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for SEQUEST synopsis files
            mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, SequestSynFileReader.GetPHRPFirstHitsFileName(mDatasetName));
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, SequestSynFileReader.GetPHRPSynopsisFileName(mDatasetName));

            UpdateMSGFInputOutputFilePaths();
        }

        protected override bool PassesFilters(PSM currentPSM)
        {
            var passesFilters = false;

            // Examine the score values and possibly filter out this line

            // SEQUEST filter rules are relaxed forms of the MTS Peptide DB Minima (Peptide DB minima 6, filter set 149)
            // All data must have DelCn <= 0.25
            // For Partially or fully tryptic, or protein terminal;
            //    XCorr >= 1.5 for 1+ or 2+
            //    XCorr >= 2.2 for >=3+
            // For non-tryptic:
            //    XCorr >= 1.5 for 1+
            //    XCorr >= 2.0 for 2+
            //    XCorr >= 2.5 for >=3+

            var isProteinTerminus = currentPSM.Peptide.StartsWith("-") || currentPSM.Peptide.EndsWith("-");

            var deltaCN = currentPSM.GetScoreDbl(SequestSynFileReader.GetColumnNameByID(SequestSynopsisFileColumns.DeltaCn));
            var xCorr = currentPSM.GetScoreDbl(SequestSynFileReader.GetColumnNameByID(SequestSynopsisFileColumns.XCorr));

            int cleavageState = PeptideCleavageStateCalculator.CleavageStateToShort(currentPSM.CleavageState);
            var cleavageStateAlt = (short)currentPSM.GetScoreInt(SequestSynFileReader.GetColumnNameByID(SequestSynopsisFileColumns.NTT), 0);

            if (cleavageStateAlt > cleavageState)
            {
                cleavageState = cleavageStateAlt;
            }

            if (deltaCN <= 0.25)
            {
                if (cleavageState >= 1 || isProteinTerminus)
                {
                    // Partially or fully tryptic, or protein terminal
                    if (currentPSM.Charge is 1 or 2)
                    {
                        if (xCorr >= 1.5)
                            passesFilters = true;
                    }
                    else
                    {
                        // Charge is 3 or higher (or zero)
                        if (xCorr >= 2.2)
                            passesFilters = true;
                    }
                }
                else
                {
                    // Non-tryptic
                    if (currentPSM.Charge == 1)
                    {
                        if (xCorr >= 1.5)
                            passesFilters = true;
                    }
                    else if (currentPSM.Charge == 2)
                    {
                        if (xCorr >= 2.0)
                            passesFilters = true;
                    }
                    else
                    {
                        // Charge is 3 or higher (or zero)
                        if (xCorr >= 2.5)
                            passesFilters = true;
                    }
                }
            }

            return passesFilters;
        }
    }
}
