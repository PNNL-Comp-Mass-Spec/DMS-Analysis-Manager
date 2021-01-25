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
    public sealed class clsMSGFInputCreatorInspect : clsMSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="workDir">Working directory</param>
        /// <remarks></remarks>
        public clsMSGFInputCreatorInspect(string datasetName, string workDir)
            : base(datasetName, workDir, clsPHRPReader.PeptideHitResultTypes.Inspect)
        {
            // Initialize the file paths
            // This updates mPHRPFirstHitsFilePath and mPHRPSynopsisFilePath
            InitializeFilePaths();
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for Inspect synopsis files
            mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserInspect.GetPHRPFirstHitsFileName(mDatasetName));
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserInspect.GetPHRPSynopsisFileName(mDatasetName));

            UpdateMSGFInputOutputFilePaths();
        }

        protected override bool PassesFilters(clsPSM currentPSM)
        {
            var passesFilters = false;

            // Keep Inspect results with pValue <= 0.2 Or TotalPRMScore >= 50 or FScore >= 0
            // PHRP has likely already filtered the _inspect_syn.txt file using these filters

            var pValue = currentPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_PValue);
            var totalPRMScore = currentPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_TotalPRMScore);
            var fScore = currentPSM.GetScoreDbl(clsPHRPParserInspect.DATA_COLUMN_FScore);

            if (pValue <= 0.2 || totalPRMScore >= 50 || fScore >= 0)
            {
                passesFilters = true;
            }

            return passesFilters;
        }
    }
}
