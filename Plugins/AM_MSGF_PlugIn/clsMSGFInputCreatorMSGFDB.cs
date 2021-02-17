//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 07/20/2010
//
// This class reads a msgfdb_syn.txt file in support of creating the input file for MSGF
//
//*********************************************************************************************************

using PHRPReader;
using System;
using System.IO;

namespace AnalysisManagerMSGFPlugin
{
    public sealed class clsMSGFInputCreatorMSGFDB : clsMSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="workDir">Working directory</param>
        public clsMSGFInputCreatorMSGFDB(string datasetName, string workDir)
            : base(datasetName, workDir, clsPHRPReader.PeptideHitResultTypes.MSGFPlus)
        {
            // Initialize the file paths
            // This updates mPHRPFirstHitsFilePath and mPHRPSynopsisFilePath
            InitializeFilePaths();
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for MSGFDB synopsis files
            mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserMSGFPlus.GetPHRPFirstHitsFileName(mDatasetName));
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserMSGFPlus.GetPHRPSynopsisFileName(mDatasetName));

            UpdateMSGFInputOutputFilePaths();
        }

        /// <summary>
        /// Reads a MODa or MODPlus FHT or SYN file and creates the corresponding _syn_MSGF.txt or _fht_MSGF.txt file
        /// using the Probability values for the MSGF score
        /// </summary>
        /// <param name="sourceFilePath"></param>
        /// <param name="resultType"></param>
        /// <remarks>
        /// Note that higher probability values are better.
        /// Also, note that Probability is actually just a score between 0 and 1; not a true probability
        /// </remarks>
        [Obsolete("This function does not appear to be used anywhere")]
        public bool CreateMSGFFileUsingMODaOrModPlusProbabilities(string sourceFilePath, clsPHRPReader.PeptideHitResultTypes resultType)
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

                var probabilityColumnName = clsPHRPParserMODPlus.DATA_COLUMN_Probability;

                if (resultType == clsPHRPReader.PeptideHitResultTypes.MODa)
                {
                    probabilityColumnName = clsPHRPParserMODa.DATA_COLUMN_Probability;
                }

                // Open the file (no need to read the Mods and Seq Info since we're not actually running MSGF)
                using var reader = new clsPHRPReader(sourceFilePath, resultType, startupOptions);
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
                using var reader = new clsPHRPReader(sourceFilePath, clsPHRPReader.PeptideHitResultTypes.MSGFPlus, startupOptions);
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

                    // currentPSM.MSGFSpecEValue comes from column MSGFDB_SpecProb   if MS-GFDB
                    //                    it comes from column MSGFDB_SpecEValue if MS-GF+
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

        protected override bool PassesFilters(clsPSM currentPSM)
        {
            // All MSGFDB data is considered to be "filter-passing"
            return true;
        }
    }
}
