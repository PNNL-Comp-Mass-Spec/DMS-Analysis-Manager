//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 04/23/2014
//
// This class reads a MODa _syn.txt file in support of creating the input file for MSGF
//
//*********************************************************************************************************

using PHRPReader;

namespace AnalysisManagerMSGFPlugin
{
    public class clsMSGFInputCreatorMODa : clsMSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="strDatasetName">Dataset name</param>
        /// <param name="strWorkDir">Working directory</param>
        /// <remarks></remarks>
        public clsMSGFInputCreatorMODa(string strDatasetName, string strWorkDir)
            : base(strDatasetName, strWorkDir, clsPHRPReader.ePeptideHitResultType.MODa)
        {
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for MODa _syn.txt files
            mPHRPFirstHitsFilePath = string.Empty;
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserMODa.GetPHRPSynopsisFileName(mDatasetName));
        }

        protected override bool PassesFilters(clsPSM objPSM)
        {
            double dblProbability = 0;

            bool blnPassesFilters = false;

            // Keep MODa results with Probability >= 0.2  (higher probability values are better)
            // This will typically keep all data in the _syn.txt file

            dblProbability = objPSM.GetScoreDbl(clsPHRPParserMODa.DATA_COLUMN_Probability, 0);
            if (dblProbability >= 0.2)
            {
                blnPassesFilters = true;
            }

            return blnPassesFilters;
        }
    }
}
