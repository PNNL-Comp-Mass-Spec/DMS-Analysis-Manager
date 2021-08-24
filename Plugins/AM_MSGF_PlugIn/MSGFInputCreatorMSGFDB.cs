using System;
using System.IO;
using PHRPReader;
using PHRPReader.Reader;
using PHRPReader.Data;

namespace AnalysisManagerMSGFPlugin
{
    /// <summary>
    /// This class reads an MS-GF+ _syn.txt file in support of creating the input file for MSGF
    /// </summary>
    public sealed class MSGFInputCreatorMSGFDB : MSGFInputCreator
    {
        // Ignore Spelling: msgfdb, MODa, fht

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="workDir">Working directory</param>
        public MSGFInputCreatorMSGFDB(string datasetName, string workDir)
            : base(datasetName, workDir, PeptideHitResultTypes.MSGFPlus)
        {
            // Initialize the file paths
            // This updates mPHRPFirstHitsFilePath and mPHRPSynopsisFilePath
            InitializeFilePaths();
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for MSGFDB synopsis files
            mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, MSGFPlusSynFileReader.GetPHRPFirstHitsFileName(mDatasetName));
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, MSGFPlusSynFileReader.GetPHRPSynopsisFileName(mDatasetName));

            UpdateMSGFInputOutputFilePaths();
        }

        /// <summary>
        /// Reads a MODa or MODPlus FHT or SYN file and creates the corresponding _syn_MSGF.txt or _fht_MSGF.txt file
        /// using the Probability values for the MSGF score
        /// </summary>
        /// <remarks>
        /// Note that higher probability values are better.
        /// Also, note that Probability is actually just a score between 0 and 1; not a true probability
        /// </remarks>
        /// <param name="sourceFilePath"></param>
        /// <param name="resultType"></param>
        [Obsolete("This function does not appear to be used anywhere")]
        public bool CreateMSGFFileUsingMODaOrModPlusProbabilities(string sourceFilePath, PeptideHitResultTypes resultType)
        {
            try
            {
                if (string.IsNullOrEmpty(sourceFilePath))
                {
                    // Source file not defined
                    mErrorMessage = "Source file not provided to CreateMSGFFileUsingMODaOrModPlusProbabilities";
                    Console.WriteLine(mErrorMessage);
                    return false;
                }

                var startupOptions = GetMinimalMemoryPHRPStartupOptions();

                var probabilityColumnName = MODPlusSynFileReader.GetColumnNameByID(MODPlusSynFileColumns.Probability);

                if (resultType == PeptideHitResultTypes.MODa)
                {
                    probabilityColumnName = MODaSynFileReader.GetColumnNameByID(MODaSynFileColumns.Probability);
                }

                // Open the file (no need to read the Mods and Seq Info since we're not actually running MSGF)
                using var reader = new ReaderFactory(sourceFilePath, resultType, startupOptions);
                RegisterEvents(reader);

                reader.SkipDuplicatePSMs = false;

                // Define the path to write the first-hits MSGF results to
                var mSGFFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(sourceFilePath) + MSGF_RESULT_FILENAME_SUFFIX);

                // Create the output file
                using var writer = new StreamWriter(new FileStream(mSGFFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                // Write out the headers to the writer
                WriteMSGFResultsHeaders(writer);

                while (reader.MoveNext())
                {
                    var currentPSM = reader.CurrentPSM;

                    // Converting MODa/MODPlus probability to a fake Spectral Probability using 1 - probability
                    var probability = currentPSM.GetScoreDbl(probabilityColumnName, 0);
                    var probabilityValue = (1 - probability).ToString("0.0000");

                    // currentPSM.MSGFSpecProb comes from column Probability
                    writer.WriteLine(currentPSM.ResultID + "\t" + currentPSM.ScanNumber + "\t" + currentPSM.Charge + "\t" + currentPSM.ProteinFirst + "\t" +
                                     currentPSM.Peptide + "\t" + probabilityValue + "\t" + string.Empty);
                }
            }
            catch (Exception ex)
            {
                ReportError("Error creating the MSGF file for the MODa / MODPlus file " + Path.GetFileName(sourceFilePath) + ": " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Reads a MSGFDB FHT or SYN file and creates the corresponding _syn_MSGF.txt or _fht_MSGF.txt file
        /// using the MSGFDB_SpecProb values for the MSGF score
        /// </summary>
        /// <param name="sourceFilePath">_msgfplus_fht.txt or _msgfplus_syn.txt file</param>
        public bool CreateMSGFFileUsingMSGFDBSpecProb(string sourceFilePath)
        {
            try
            {
                if (string.IsNullOrEmpty(sourceFilePath))
                {
                    // Source file not defined
                    mErrorMessage = "Source file not provided to CreateMSGFFileUsingMSGFDBSpecProb";
                    Console.WriteLine(mErrorMessage);
                    return false;
                }

                var startupOptions = GetMinimalMemoryPHRPStartupOptions();

                // Open the file (no need to read the Mods and Seq Info since we're not actually running MSGF)
                using var reader = new ReaderFactory(sourceFilePath, PeptideHitResultTypes.MSGFPlus, startupOptions);
                RegisterEvents(reader);

                reader.SkipDuplicatePSMs = false;

                // Define the path to write the first-hits MSGF results to
                var mSGFFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(sourceFilePath) + MSGF_RESULT_FILENAME_SUFFIX);

                // Create the output file
                using var writer = new StreamWriter(new FileStream(mSGFFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                // Write out the headers
                WriteMSGFResultsHeaders(writer);

                while (reader.MoveNext())
                {
                    var currentPSM = reader.CurrentPSM;

                    // currentPSM.MSGFSpecEValue comes from column MSGFDB_SpecProb if MS-GFDB
                    //                      it comes from column MSGFDB_SpecEValue if MS-GF+
                    writer.WriteLine(currentPSM.ResultID + "\t" + currentPSM.ScanNumber + "\t" + currentPSM.Charge + "\t" + currentPSM.ProteinFirst + "\t" +
                                     currentPSM.Peptide + "\t" + currentPSM.MSGFSpecEValue + "\t" + string.Empty);
                }
            }
            catch (Exception ex)
            {
                ReportError("Error creating the MSGF file for MSGFDB file " + Path.GetFileName(sourceFilePath) + ": " + ex.Message);
                return false;
            }

            return true;
        }

        protected override bool PassesFilters(PSM currentPSM)
        {
            // All MS-GF+ data is considered to be "filter-passing"
            return true;
        }
    }
}
