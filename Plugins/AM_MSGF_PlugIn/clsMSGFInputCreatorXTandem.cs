//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 07/20/2010
//
// This class reads an X!Tandem _xt.txt file in support of creating the input file for MSGF
//
//*********************************************************************************************************

using PHRPReader;

namespace AnalysisManagerMSGFPlugin
{
    public class clsMSGFInputCreatorXTandem : clsMSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="strDatasetName">Dataset name</param>
        /// <param name="strWorkDir">Working directory</param>
        /// <remarks></remarks>
        public clsMSGFInputCreatorXTandem(string strDatasetName, string strWorkDir)
            : base(strDatasetName, strWorkDir, clsPHRPReader.ePeptideHitResultType.XTandem)
        {
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for X!Tandem _xt.txt files
            mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserXTandem.GetPHRPFirstHitsFileName(mDatasetName));
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserXTandem.GetPHRPSynopsisFileName(mDatasetName));
        }

        protected override bool PassesFilters(clsPSM objPSM)
        {
            var blnPassesFilters = false;

            // Keep X!Tandem results with Peptide_Expectation_Value_Log(e) <= -0.3
            // This will typically keep all data in the _xt.txt file

            var dblLogEValue = objPSM.GetScoreDbl(clsPHRPParserXTandem.DATA_COLUMN_Peptide_Expectation_Value_LogE, 0);
            if (dblLogEValue <= -0.3)
            {
                blnPassesFilters = true;
            }

            return blnPassesFilters;
        }
    }
}
