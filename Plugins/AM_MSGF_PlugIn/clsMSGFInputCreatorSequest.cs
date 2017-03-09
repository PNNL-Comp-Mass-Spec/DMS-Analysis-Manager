//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 07/20/2010
//
// This class reads a Sequest _syn.txt file in support of creating the input file for MSGF
//
//*********************************************************************************************************

using System;

using PHRPReader;

namespace AnalysisManagerMSGFPlugin
{
    public class clsMSGFInputCreatorSequest : clsMSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="strDatasetName">Dataset name</param>
        /// <param name="strWorkDir">Working directory</param>
        /// <remarks></remarks>
        public clsMSGFInputCreatorSequest(string strDatasetName, string strWorkDir)
            : base(strDatasetName, strWorkDir, clsPHRPReader.ePeptideHitResultType.Sequest)
        {
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for Sequest synopsis files
            mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserSequest.GetPHRPFirstHitsFileName(mDatasetName));
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserSequest.GetPHRPSynopsisFileName(mDatasetName));
        }

        protected override bool PassesFilters(clsPSM objPSM)
        {
            bool blnIsProteinTerminus;
            var blnPassesFilters = false;

            // Examine the score values and possibly filter out this line

            // Sequest filter rules are relaxed forms of the MTS Peptide DB Minima (Peptide DB minima 6, filter set 149)
            // All data must have DelCn <= 0.25
            // For Partially or fully tryptic, or protein terminal;
            //    XCorr >= 1.5 for 1+ or 2+
            //    XCorr >= 2.2 for >=3+
            // For non-tryptic:
            //    XCorr >= 1.5 for 1+
            //    XCorr >= 2.0 for 2+
            //    XCorr >= 2.5 for >=3+

            if (objPSM.Peptide.StartsWith("-") || objPSM.Peptide.EndsWith("-"))
            {
                blnIsProteinTerminus = true;
            }
            else
            {
                blnIsProteinTerminus = false;
            }

            var dblDeltaCN = objPSM.GetScoreDbl(clsPHRPParserSequest.DATA_COLUMN_DelCn);
            var dblXCorr = objPSM.GetScoreDbl(clsPHRPParserSequest.DATA_COLUMN_XCorr);

            int intCleavageState = clsPeptideCleavageStateCalculator.CleavageStateToShort(objPSM.CleavageState);
            var intCleavageStateAlt = (short)objPSM.GetScoreInt(clsPHRPParserSequest.DATA_COLUMN_NumTrypticEnds, 0);

            if (intCleavageStateAlt > intCleavageState)
            {
                intCleavageState = intCleavageStateAlt;
            }

            if (dblDeltaCN <= 0.25)
            {
                if (intCleavageState >= 1 || blnIsProteinTerminus)
                {
                    // Partially or fully tryptic, or protein terminal
                    if (objPSM.Charge == 1 | objPSM.Charge == 2)
                    {
                        if (dblXCorr >= 1.5)
                            blnPassesFilters = true;
                    }
                    else
                    {
                        // Charge is 3 or higher (or zero)
                        if (dblXCorr >= 2.2)
                            blnPassesFilters = true;
                    }
                }
                else
                {
                    // Non-tryptic
                    if (objPSM.Charge == 1)
                    {
                        if (dblXCorr >= 1.5)
                            blnPassesFilters = true;
                    }
                    else if (objPSM.Charge == 2)
                    {
                        if (dblXCorr >= 2.0)
                            blnPassesFilters = true;
                    }
                    else
                    {
                        // Charge is 3 or higher (or zero)
                        if (dblXCorr >= 2.5)
                            blnPassesFilters = true;
                    }
                }
            }

            return blnPassesFilters;
        }
    }
}
