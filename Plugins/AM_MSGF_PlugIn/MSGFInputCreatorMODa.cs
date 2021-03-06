﻿using PHRPReader;
using PHRPReader.Data;
using PHRPReader.Reader;

namespace AnalysisManagerMSGFPlugin
{
    /// <summary>
    /// This class reads a MODa _syn.txt file in support of creating the input file for MSGF
    /// </summary>
    public sealed class MSGFInputCreatorMODa : MSGFInputCreator
    {
        // Ignore Spelling: MODa

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="workDir">Working directory</param>
        public MSGFInputCreatorMODa(string datasetName, string workDir)
            : base(datasetName, workDir, PeptideHitResultTypes.MODa)
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
            // Keep MODa results with Probability >= 0.2  (higher probability values are better)
            // This will typically keep all data in the _syn.txt file

            var probability = currentPSM.GetScoreDbl(MODaSynFileReader.GetColumnNameByID(MODaSynFileColumns.Probability), 0);

            return probability >= 0.2;
        }
    }
}
