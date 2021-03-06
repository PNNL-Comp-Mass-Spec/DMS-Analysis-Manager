﻿using PHRPReader;
using PHRPReader.Data;
using PHRPReader.Reader;

namespace AnalysisManagerMSGFPlugin
{
    /// <summary>
    /// This class reads an Inspect _inspect_syn.txt file in support of creating the input file for MSGF
    /// </summary>
    public sealed class MSGFInputCreatorInspect : MSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="workDir">Working directory</param>
        public MSGFInputCreatorInspect(string datasetName, string workDir)
            : base(datasetName, workDir, PeptideHitResultTypes.Inspect)
        {
            // Initialize the file paths
            // This updates mPHRPFirstHitsFilePath and mPHRPSynopsisFilePath
            InitializeFilePaths();
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for Inspect synopsis files
            mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, InspectSynFileReader.GetPHRPFirstHitsFileName(mDatasetName));
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, InspectSynFileReader.GetPHRPSynopsisFileName(mDatasetName));

            UpdateMSGFInputOutputFilePaths();
        }

        protected override bool PassesFilters(PSM currentPSM)
        {
            // Keep Inspect results with pValue <= 0.2 Or TotalPRMScore >= 50 or FScore >= 0
            // PHRP has likely already filtered the _inspect_syn.txt file using these filters

            var pValue = currentPSM.GetScoreDbl(InspectSynFileReader.GetColumnNameByID(InspectSynFileColumns.PValue));
            var totalPRMScore = currentPSM.GetScoreDbl(InspectSynFileReader.GetColumnNameByID(InspectSynFileColumns.TotalPRMScore));
            var fScore = currentPSM.GetScoreDbl(InspectSynFileReader.GetColumnNameByID(InspectSynFileColumns.FScore));

            return pValue <= 0.2 || totalPRMScore >= 50 || fScore >= 0;
        }
    }
}
