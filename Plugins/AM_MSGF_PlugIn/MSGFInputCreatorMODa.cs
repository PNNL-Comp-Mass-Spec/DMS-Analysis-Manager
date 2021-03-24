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
using PHRPReader.Data;
using PHRPReader.Reader;

namespace AnalysisManagerMSGFPlugin
{
    public sealed class MSGFInputCreatorMODa : MSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="workDir">Working directory</param>
        public MSGFInputCreatorMODa(string datasetName, string workDir)
            : base(datasetName, workDir, PHRPReader.Enums.PeptideHitResultTypes.MODa)
        {
            // Initialize the file paths
            // This updates mPHRPFirstHitsFilePath and mPHRPSynopsisFilePath
            InitializeFilePaths();
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for MODa _syn.txt files
            mPHRPFirstHitsFilePath = string.Empty;
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, MODaSynFileReader.GetPHRPSynopsisFileName(mDatasetName));

            UpdateMSGFInputOutputFilePaths();
        }

        protected override bool PassesFilters(PSM currentPSM)
        {
            var passesFilters = false;

            // Keep MODa results with Probability >= 0.2  (higher probability values are better)
            // This will typically keep all data in the _syn.txt file

            var probability = currentPSM.GetScoreDbl(MODaSynFileReader.DATA_COLUMN_Probability, 0);
            if (probability >= 0.2)
            {
                passesFilters = true;
            }

            return passesFilters;
        }
    }
}
