//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 05/19/2015
//
// This class reads a MODPlus _syn.txt file in support of creating the input file for MSGF
//
//*********************************************************************************************************

using PHRPReader;

namespace AnalysisManagerMSGFPlugin
{
    public class clsMSGFInputCreatorMODPlus : clsMSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="strDatasetName">Dataset name</param>
        /// <param name="strWorkDir">Working directory</param>
        /// <remarks></remarks>
        public clsMSGFInputCreatorMODPlus(string strDatasetName, string strWorkDir)
            : base(strDatasetName, strWorkDir, clsPHRPReader.ePeptideHitResultType.MODPlus)
        {
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for MODPlus _syn.txt files
            mPHRPFirstHitsFilePath = string.Empty;
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserMODPlus.GetPHRPSynopsisFileName(mDatasetName));
        }

        protected override bool PassesFilters(clsPSM objPSM)
        {
            var blnPassesFilters = false;

            // Keep MODPlus results with Probability >= 0.05  (higher probability values are better)
            // This will typically keep all data in the _syn.txt file

            var dblProbability = objPSM.GetScoreDbl(clsPHRPParserMODPlus.DATA_COLUMN_Probability, 0);
            if (dblProbability >= 0.05)
            {
                blnPassesFilters = true;
            }

            return blnPassesFilters;
        }
    }
}
