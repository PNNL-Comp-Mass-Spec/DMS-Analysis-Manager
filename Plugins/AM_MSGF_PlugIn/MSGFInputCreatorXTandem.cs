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
    public sealed class clsMSGFInputCreatorXTandem : clsMSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="workDir">Working directory</param>
        public clsMSGFInputCreatorXTandem(string datasetName, string workDir)
            : base(datasetName, workDir, clsPHRPReader.PeptideHitResultTypes.XTandem)
        {
            // Initialize the file paths
            // This updates mPHRPFirstHitsFilePath and mPHRPSynopsisFilePath
            InitializeFilePaths();
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for X!Tandem _xt.txt files
            mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserXTandem.GetPHRPFirstHitsFileName(mDatasetName));
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserXTandem.GetPHRPSynopsisFileName(mDatasetName));

            UpdateMSGFInputOutputFilePaths();
        }

        protected override bool PassesFilters(clsPSM currentPSM)
        {
            var passesFilters = false;

            // Keep X!Tandem results with Peptide_Expectation_Value_Log(e) <= -0.3
            // This will typically keep all data in the _xt.txt file

            var logEValue = currentPSM.GetScoreDbl(clsPHRPParserXTandem.DATA_COLUMN_Peptide_Expectation_Value_LogE, 0);
            if (logEValue <= -0.3)
            {
                passesFilters = true;
            }

            return passesFilters;
        }
    }
}
