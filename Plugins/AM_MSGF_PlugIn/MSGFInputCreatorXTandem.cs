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
using PHRPReader.Data;
using PHRPReader.Reader;

namespace AnalysisManagerMSGFPlugin
{
    public sealed class MSGFInputCreatorXTandem : MSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="workDir">Working directory</param>
        public MSGFInputCreatorXTandem(string datasetName, string workDir)
            : base(datasetName, workDir, PeptideHitResultTypes.XTandem)
        {
            // Initialize the file paths
            // This updates mPHRPFirstHitsFilePath and mPHRPSynopsisFilePath
            InitializeFilePaths();
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for X!Tandem _xt.txt files
            mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, XTandemSynFileReader.GetPHRPFirstHitsFileName(mDatasetName));
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, XTandemSynFileReader.GetPHRPSynopsisFileName(mDatasetName));

            UpdateMSGFInputOutputFilePaths();
        }

        protected override bool PassesFilters(PSM currentPSM)
        {
            // Keep X!Tandem results with Peptide_Expectation_Value_Log(e) <= -0.3
            // This will typically keep all data in the _xt.txt file

            var logEValue = currentPSM.GetScoreDbl(XTandemSynFileReader.GetColumnNameByID(XTandemSynFileColumns.EValue), 0);

            return logEValue <= -0.3;
        }
    }
}
