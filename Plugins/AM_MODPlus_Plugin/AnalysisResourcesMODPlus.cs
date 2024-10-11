//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 05/12/2015
//
//*********************************************************************************************************

using System;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerMODPlusPlugin
{
    /// <summary>
    /// Retrieve resources for the MODPlus plugin
    /// </summary>
    public class AnalysisResourcesMODPlus : AnalysisResources
    {
        // Ignore Spelling: na

        internal const string MOD_PLUS_RUNTIME_PARAM_FASTA_FILE_IS_DECOY = "###_MODPlus_Runtime_Param_FastaFileIsDecoy_###";
        internal const int MINIMUM_PERCENT_DECOY = 25;

        /// <summary>
        /// Initialize options
        /// </summary>
        public override void Setup(string stepToolName, IMgrParams mgrParams, IJobParams jobParams, IStatusFile statusTools, MyEMSLUtilities myEMSLUtilities)
        {
            base.Setup(stepToolName, mgrParams, jobParams, statusTools, myEMSLUtilities);
            SetOption(Global.AnalysisResourceOptions.OrgDbRequired, true);
        }

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            var currentTask = "Initializing";

            try
            {
                currentTask = "Retrieve shared resources";

                // Retrieve shared resources, including the JobParameters file from the previous job step
                var result = GetSharedResources();

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                currentTask = "Retrieve FASTA and param file";

                if (!RetrieveFastaAndParamFile())
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }

                currentTask = "Get Input file";

                var msXmlResult = GetMsXmlFile();

                if (msXmlResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return msXmlResult;
                }

                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in GetResources: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + Global.GetExceptionStackTrace(ex));
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        private bool RetrieveFastaAndParamFile()
        {
            var currentTask = "Initializing";

            try
            {
                var proteinCollections = mJobParams.GetParam("ProteinCollectionList", string.Empty);
                var proteinOptions = mJobParams.GetParam("ProteinOptions", string.Empty);

                if (string.IsNullOrEmpty(proteinCollections))
                {
                    LogError("Job parameter ProteinCollectionList not found; unable to check for decoy FASTA file");
                    return false;
                }

                if (string.IsNullOrEmpty(proteinOptions))
                {
                    LogError("Job parameter ProteinOptions not found; unable to check for decoy FASTA file");
                    return false;
                }

                var checkLegacyFastaForDecoy = false;

                if (Global.IsMatch(proteinCollections, "na"))
                {
                    // Legacy FASTA file
                    // Need to open it with a reader and look for entries that start with Reversed_ or XXX_ or XXX.
                    checkLegacyFastaForDecoy = true;
                }
                else
                {
                    if (proteinOptions.IndexOf("seq_direction=decoy", StringComparison.OrdinalIgnoreCase) < 0)
                    {
                        LogError("Job parameter ProteinOptions does not contain seq_direction=decoy; cannot analyze with MODPlus; choose a DMS-generated decoy protein collection");
                        return false;
                    }
                }

                // Retrieve the FASTA file
                var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

                currentTask = "RetrieveOrgDB to " + orgDbDirectoryPath;

                if (!RetrieveOrgDB(orgDbDirectoryPath, out _))
                    return false;

                if (checkLegacyFastaForDecoy)
                {
                    if (!FastaHasDecoyProteins())
                    {
                        return false;
                    }
                }

                mJobParams.AddAdditionalParameter("MODPlus", MOD_PLUS_RUNTIME_PARAM_FASTA_FILE_IS_DECOY, "True");

                LogMessage("Getting param file");

                // Retrieve the parameter file
                // This will also obtain the _ModDefs.txt file using query
                //  SELECT Local_Symbol, Monoisotopic_Mass, Residue_Symbol, Mod_Type_Symbol, Mass_Correction_Tag
                //  FROM V_Param_File_Mass_Mod_Info
                //  WHERE Param_File_Name = 'ParamFileName'

                var paramFileName = mJobParams.GetParam("ParamFileName");

                currentTask = "RetrieveGeneratedParamFile " + paramFileName;

                var success = RetrieveGeneratedParamFile(paramFileName);
                return success;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrieveFastaAndParamFile: " + ex.Message;
                LogError(mMessage + "; task = " + currentTask + "; " + Global.GetExceptionStackTrace(ex));
                return false;
            }
        }

        private bool FastaHasDecoyProteins()
        {
            var localOrgDbFolder = mMgrParams.GetParam("OrgDbDir");
            var fastaFilePath = Path.Combine(localOrgDbFolder, mJobParams.GetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName"));

            var fastaFile = new FileInfo(fastaFilePath);

            if (!fastaFile.Exists)
            {
                // FASTA file not found
                LogError("FASTA file not found: " + fastaFile.Name, "FASTA file not found: " + fastaFile.FullName);
                return false;
            }

            // Determine the fraction of the proteins that start with Reversed_ or XXX_ or XXX.
            var decoyPrefixes = GetDefaultDecoyPrefixes();
            double maxPercentReverse = 0;

            foreach (var decoyPrefix in decoyPrefixes)
            {
                var fractionDecoy = GetDecoyFastaCompositionStats(fastaFile, decoyPrefix, out var proteinCount);

                if (proteinCount == 0)
                {
                    LogError("No proteins found in " + fastaFile.Name);
                    return false;
                }

                var percentReverse = fractionDecoy * 100;

                if (percentReverse >= MINIMUM_PERCENT_DECOY)
                {
                    // At least 25% of the proteins in the FASTA file are reverse proteins
                    return true;
                }

                if (percentReverse > maxPercentReverse)
                {
                    maxPercentReverse = percentReverse;
                }
            }

            var addonMsg = "choose a DMS-generated decoy protein collection or a legacy FASTA file with protein names that start with " +
                           string.Join(" or ", decoyPrefixes);

            if (Math.Abs(maxPercentReverse) < float.Epsilon)
            {
                LogError("Legacy FASTA file " + fastaFile.Name + " does not have any decoy (reverse) proteins; " + addonMsg);
                return false;
            }

            LogError("Fewer than " + MINIMUM_PERCENT_DECOY + "% of the proteins in legacy FASTA file " + fastaFile.Name +
                     " are decoy (reverse) proteins (" + maxPercentReverse.ToString("0") + "%); " + addonMsg);
            return false;
        }
    }
}
