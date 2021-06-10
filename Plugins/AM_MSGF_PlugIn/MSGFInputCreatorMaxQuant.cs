using System;
using PHRPReader;
using PHRPReader.Data;
using PHRPReader.Reader;

namespace AnalysisManagerMSGFPlugin
{
    /// <summary>
    /// This class reads a MaxQuant _syn.txt file in support of creating the input file for MSGF
    /// </summary>
    [Obsolete("Unused")]
    public sealed class MSGFInputCreatorMaxQuant : MSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="workDir">Working directory</param>
        public MSGFInputCreatorMaxQuant(string datasetName, string workDir)
            : base(datasetName, workDir, PeptideHitResultTypes.MaxQuant)
        {
            // Initialize the file paths
            // This updates mPHRPFirstHitsFilePath and mPHRPSynopsisFilePath
            InitializeFilePaths();
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for MaxQuant _syn.txt files
            mPHRPFirstHitsFilePath = string.Empty;
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, MaxQuantSynFileReader.GetPHRPSynopsisFileName(mDatasetName));

            UpdateMSGFInputOutputFilePaths();
        }

        protected override bool PassesFilters(PSM currentPSM)
        {
            // Keep MaxQuant results with Andromeda Score >= 50  (higher probability values are better)
            // Also keep results with Posterior Error Probability (PEP) < 0.01  (lower values are better)
            // This will typically keep all data in the _syn.txt file

            var andromedaScore = currentPSM.GetScoreDbl(MaxQuantSynFileReader.GetColumnNameByID(MaxQuantSynFileColumns.Score), 0);
            var pepValue = currentPSM.GetScoreDbl(MaxQuantSynFileReader.GetColumnNameByID(MaxQuantSynFileColumns.PEP), 0);

            return andromedaScore >= 50 || pepValue < 0.01;
        }
    }
}
