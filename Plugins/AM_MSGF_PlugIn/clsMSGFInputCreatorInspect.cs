//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 07/20/2010
//
// This class reads an Inspect _inspect_syn.txt file in support of creating the input file for MSGF
//
//*********************************************************************************************************

using PHRPReader;

namespace AnalysisManagerMSGFPlugin
{
    public class clsMSGFInputCreatorInspect : clsMSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="strDatasetName">Dataset name</param>
        /// <param name="strWorkDir">Working directory</param>
        /// <remarks></remarks>
        public clsMSGFInputCreatorInspect(string strDatasetName, string strWorkDir)
            : base(strDatasetName, strWorkDir, clsPHRPReader.ePeptideHitResultType.Inspect)
        {
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for Inspect synopsis files
            mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserInspect.GetPHRPFirstHitsFileName(mDatasetName));
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserInspect.GetPHRPSynopsisFileName(mDatasetName));
        }

        protected override bool PassesFilters(clsPSM objPSM)
        {
            double dblPValue = 0;
            double dblTotalPRMScore = 0;
            double dblFScore = 0;

            bool blnPassesFilters = false;

            // Keep Inspect results with pValue <= 0.2 Or TotalPRMScore >= 50 or FScore >= 0
            // PHRP has likely already filtered the _inspect_syn.txt file using these filters

            dblPValue = objPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_PValue);
            dblTotalPRMScore = objPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_TotalPRMScore);
            dblFScore = objPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_FScore);

            if (dblPValue <= 0.2 || dblTotalPRMScore >= 50 || dblFScore >= 0)
            {
                blnPassesFilters = true;
            }

            return blnPassesFilters;
        }
    }
}
