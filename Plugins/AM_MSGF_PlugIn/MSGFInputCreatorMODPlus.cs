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
    public sealed class MSGFInputCreatorMODPlus : MSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="workDir">Working directory</param>
        public MSGFInputCreatorMODPlus(string datasetName, string workDir)
            : base(datasetName, workDir, clsPHRPReader.PeptideHitResultTypes.MODPlus)
        {
            // Initialize the file paths
            // This updates mPHRPFirstHitsFilePath and mPHRPSynopsisFilePath
            InitializeFilePaths();
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for MODPlus _syn.txt files
            mPHRPFirstHitsFilePath = string.Empty;
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserMODPlus.GetPHRPSynopsisFileName(mDatasetName));

            UpdateMSGFInputOutputFilePaths();
        }

        protected override bool PassesFilters(clsPSM currentPSM)
        {
            var passesFilters = false;

            // Keep MODPlus results with Probability >= 0.05  (higher probability values are better)
            // This will typically keep all data in the _syn.txt file

            var probability = currentPSM.GetScoreDbl(clsPHRPParserMODPlus.DATA_COLUMN_Probability, 0);
            if (probability >= 0.05)
            {
                passesFilters = true;
            }

            return passesFilters;
        }
    }
}
