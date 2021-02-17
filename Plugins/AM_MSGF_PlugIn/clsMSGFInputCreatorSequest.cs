//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 07/20/2010
//
// This class reads a SEQUEST _syn.txt file in support of creating the input file for MSGF
//
//*********************************************************************************************************

using PHRPReader;

namespace AnalysisManagerMSGFPlugin
{
    public sealed class clsMSGFInputCreatorSequest : clsMSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="workDir">Working directory</param>
        public clsMSGFInputCreatorSequest(string datasetName, string workDir)
            : base(datasetName, workDir, clsPHRPReader.PeptideHitResultTypes.Sequest)
        {
            // Initialize the file paths
            // This updates mPHRPFirstHitsFilePath and mPHRPSynopsisFilePath
            InitializeFilePaths();
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for SEQUEST synopsis files
            mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserSequest.GetPHRPFirstHitsFileName(mDatasetName));
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserSequest.GetPHRPSynopsisFileName(mDatasetName));

            UpdateMSGFInputOutputFilePaths();
        }

        protected override bool PassesFilters(clsPSM currentPSM)
        {
            bool isProteinTerminus;
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

            if (currentPSM.Peptide.StartsWith("-") || currentPSM.Peptide.EndsWith("-"))
            {
                isProteinTerminus = true;
            }
            else
            {
                isProteinTerminus = false;
            }

            var deltaCN = currentPSM.GetScoreDbl(clsPHRPParserSequest.DATA_COLUMN_DelCn);
            var xCorr = currentPSM.GetScoreDbl(clsPHRPParserSequest.DATA_COLUMN_XCorr);

            int cleavageState = clsPeptideCleavageStateCalculator.CleavageStateToShort(currentPSM.CleavageState);
            var cleavageStateAlt = (short)currentPSM.GetScoreInt(clsPHRPParserSequest.DATA_COLUMN_NumTrypticEnds, 0);

            if (cleavageStateAlt > cleavageState)
            {
                cleavageState = cleavageStateAlt;
            }

            if (deltaCN <= 0.25)
            {
                if (cleavageState >= 1 || isProteinTerminus)
                {
                    // Partially or fully tryptic, or protein terminal
                    if (currentPSM.Charge == 1 || currentPSM.Charge == 2)
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
