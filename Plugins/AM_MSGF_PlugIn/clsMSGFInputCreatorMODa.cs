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
    public sealed class clsMSGFInputCreatorMODa : clsMSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="workDir">Working directory</param>
        public clsMSGFInputCreatorMODa(string datasetName, string workDir)
            : base(datasetName, workDir, clsPHRPReader.PeptideHitResultTypes.MODa)
        {
            // Initialize the file paths
            // This updates mPHRPFirstHitsFilePath and mPHRPSynopsisFilePath
            InitializeFilePaths();
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for MODa _syn.txt files
            mPHRPFirstHitsFilePath = string.Empty;
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserMODa.GetPHRPSynopsisFileName(mDatasetName));

            UpdateMSGFInputOutputFilePaths();
        }

        protected override bool PassesFilters(clsPSM currentPSM)
        {
            var passesFilters = false;

            // Keep MODa results with Probability >= 0.2  (higher probability values are better)
            // This will typically keep all data in the _syn.txt file

            var probability = currentPSM.GetScoreDbl(clsPHRPParserMODa.DATA_COLUMN_Probability, 0);
            if (probability >= 0.2)
            {
                passesFilters = true;
            }

            return passesFilters;
        }
    }
}
