//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
//
// Created 07/20/2010
//
// This class reads a msgfdb_syn.txt file in support of creating the input file for MSGF
//
//*********************************************************************************************************

using System;
using System.IO;
using PHRPReader;

namespace AnalysisManagerMSGFPlugin
{
    public class clsMSGFInputCreatorMSGFDB : clsMSGFInputCreator
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="strDatasetName">Dataset name</param>
        /// <param name="strWorkDir">Working directory</param>
        /// <remarks></remarks>
        public clsMSGFInputCreatorMSGFDB(string strDatasetName, string strWorkDir)
            : base(strDatasetName, strWorkDir, clsPHRPReader.ePeptideHitResultType.MSGFDB)
        {
        }

        protected override void InitializeFilePaths()
        {
            // Customize mPHRPResultFilePath for MSGFDB synopsis files
            mPHRPFirstHitsFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserMSGFDB.GetPHRPFirstHitsFileName(mDatasetName));
            mPHRPSynopsisFilePath = CombineIfValidFile(mWorkDir, clsPHRPParserMSGFDB.GetPHRPSynopsisFileName(mDatasetName));
        }

        /// <summary>
        /// Reads a MODa or MODPlus FHT or SYN file and creates the corresponding _syn_MSGF.txt or _fht_MSGF.txt file
        /// using the Probability values for the MSGF score
        /// </summary>
        /// <param name="strSourceFilePath"></param>
        /// <param name="eResultType"></param>
        /// <param name="strSourceFileDescription"></param>
        /// <returns></returns>
        /// <remarks>Note that higher probability values are better.  Also, note that Probability is actually just a score between 0 and 1; not a true probability</remarks>
        [Obsolete("This function does not appear to be used anywhere")]
        public bool CreateMSGFFileUsingMODaOrModPlusProbabilities(string strSourceFilePath, clsPHRPReader.ePeptideHitResultType eResultType, string strSourceFileDescription)
        {
            string strMSGFFilePath = null;

            try
            {
                if (string.IsNullOrEmpty(strSourceFilePath))
                {
                    // Source file not defined
                    mErrorMessage = "Source file not provided to CreateMSGFFileUsingMODaOrModPlusProbabilities";
                    Console.WriteLine(mErrorMessage);
                    return false;
                }

                clsPHRPStartupOptions startupOptions = GetMinimalMemoryPHRPStartupOptions();

                var probabilityColumnName = clsPHRPParserMODPlus.DATA_COLUMN_Probability;

                if (eResultType == clsPHRPReader.ePeptideHitResultType.MODa)
                {
                    probabilityColumnName = clsPHRPParserMODa.DATA_COLUMN_Probability;
                }

                // Open the file (no need to read the Mods and Seq Info since we're not actually running MSGF)
                using (var objReader = new clsPHRPReader(strSourceFilePath, eResultType, startupOptions))
                {
                    objReader.SkipDuplicatePSMs = false;

                    // Define the path to write the first-hits MSGF results to
                    strMSGFFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(strSourceFilePath) + MSGF_RESULT_FILENAME_SUFFIX);

                    // Create the output file
                    using (var swMSGFFile = new StreamWriter(new FileStream(strMSGFFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        // Write out the headers to swMSGFFHTFile
                        WriteMSGFResultsHeaders(swMSGFFile);

                        while (objReader.MoveNext())
                        {
                            var objPSM = objReader.CurrentPSM;

                            // Converting MODa/MODPlus probability to a fake Spectral Probability using 1 - probability
                            var dblProbability = objPSM.GetScoreDbl(probabilityColumnName, 0);
                            var strProbabilityValue = (1 - dblProbability).ToString("0.0000");

                            // objPSM.MSGFSpecProb comes from column Probability
                            swMSGFFile.WriteLine(objPSM.ResultID + "\t" + objPSM.ScanNumber + "\t" + objPSM.Charge + "\t" + objPSM.ProteinFirst + "\t" +
                                                 objPSM.Peptide + "\t" + strProbabilityValue + "\t" + string.Empty);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                ReportError("Error creating the MSGF file for the MODa / MODPlus file " + Path.GetFileName(strSourceFilePath) + ": " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Reads a MSGFDB FHT or SYN file and creates the corresponding _syn_MSGF.txt or _fht_MSGF.txt file
        /// using the MSGFDB_SpecProb values for the MSGF score
        /// </summary>
        /// <param name="strSourceFilePath">_msgfplus_fht.txt or _msgfplus_syn.txt file</param>
        /// <param name="strSourceFileDescription"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public bool CreateMSGFFileUsingMSGFDBSpecProb(string strSourceFilePath, string strSourceFileDescription)
        {
            try
            {
                if (string.IsNullOrEmpty(strSourceFilePath))
                {
                    // Source file not defined
                    mErrorMessage = "Source file not provided to CreateMSGFFileUsingMSGFDBSpecProb";
                    Console.WriteLine(mErrorMessage);
                    return false;
                }

                clsPHRPStartupOptions startupOptions = GetMinimalMemoryPHRPStartupOptions();

                // Open the file (no need to read the Mods and Seq Info since we're not actually running MSGF)
                using (var objReader = new clsPHRPReader(strSourceFilePath, clsPHRPReader.ePeptideHitResultType.MSGFDB, startupOptions))
                {
                    objReader.SkipDuplicatePSMs = false;

                    // Define the path to write the first-hits MSGF results to
                    var strMSGFFilePath = Path.Combine(mWorkDir, Path.GetFileNameWithoutExtension(strSourceFilePath) + MSGF_RESULT_FILENAME_SUFFIX);

                    // Create the output file
                    using (var swMSGFFile = new StreamWriter(new FileStream(strMSGFFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                    {
                        // Write out the headers to swMSGFFHTFile
                        WriteMSGFResultsHeaders(swMSGFFile);

                        while (objReader.MoveNext())
                        {
                            var objPSM = objReader.CurrentPSM;

                            // objPSM.MSGFSpecProb comes from column MSGFDB_SpecProb   if MS-GFDB
                            //                 it  comes from column MSGFDB_SpecEValue if MS-GF+
                            swMSGFFile.WriteLine(objPSM.ResultID + "\t" + objPSM.ScanNumber + "\t" + objPSM.Charge + "\t" + objPSM.ProteinFirst + "\t" +
                                                 objPSM.Peptide + "\t" + objPSM.MSGFSpecProb + "\t" + string.Empty);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                ReportError("Error creating the MSGF file for MSGFDB file " + Path.GetFileName(strSourceFilePath) + ": " + ex.Message);
                return false;
            }

            return true;
        }

        protected override bool PassesFilters(clsPSM objPSM)
        {
            bool blnPassesFilters = false;

            // All MSGFDB data is considered to be "filter-passing"
            blnPassesFilters = true;

            return blnPassesFilters;
        }
    }
}
