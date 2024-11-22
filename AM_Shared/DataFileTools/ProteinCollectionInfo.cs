using System;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerBase.DataFileTools
{
    /// <summary>
    /// Protein collection info
    /// </summary>
    public class ProteinCollectionInfo
    {
        /// <summary>
        /// Legacy FASTA file name
        /// </summary>
        /// <remarks>Will be "na" when using a protein collection</remarks>
        public string LegacyFastaName { get; set; }

        /// <summary>
        /// Protein collection options
        /// </summary>
        public string ProteinCollectionOptions { get; set; }

        /// <summary>
        /// Protein collection list
        /// </summary>
        public string ProteinCollectionList { get; set; }

        /// <summary>
        /// True if using a legacy (standalone) FASTA file
        /// </summary>
        public bool UsingLegacyFasta { get; set; }

        /// <summary>
        /// True if using a split FASTA file
        /// </summary>
        /// <remarks>
        /// <para>
        /// This will be true if parameter "SplitFasta" is true in a settings file for scripts
        /// MSGFPlus_SplitFasta, MSGFPlus_MzML_SplitFasta, or MSGFPlus_MzML_SplitFasta_NoRefine
        /// </para>
        /// <para>
        /// This property should always be false for scripts MSFragger and FragPipe, since settings files for those scripts
        /// use parameter "DatabaseSplitCount" to enable FASTA file splitting (which is handled natively by the tool)
        /// </para>
        /// </remarks>
        public bool UsingSplitFasta { get; set; }

        /// <summary>
        /// Error message
        /// </summary>
        public string ErrorMessage { get; set; }

        /// <summary>
        /// Set to true once the protein collection or legacy FASTA file has been validated
        /// </summary>
        public bool IsValid { get; set; }

        /// <summary>
        /// Description of the protein sequence source (either protein collection or legacy FASTA file)
        /// </summary>
        public string OrgDBDescription { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParams">Job parameters</param>
        public ProteinCollectionInfo(IJobParams jobParams)
        {
            LegacyFastaName = jobParams.GetParam("LegacyFastaFileName");
            ProteinCollectionOptions = jobParams.GetParam("ProteinOptions");
            ProteinCollectionList = jobParams.GetParam("ProteinCollectionList");

            // The "SplitFasta" settings file parameter is applicable to settings files for scripts MSGFPlus_SplitFasta, MSGFPlus_MzML_SplitFasta, and MSGFPlus_MzML_SplitFasta_NoRefine
            // In contrast, settings files for scripts MSFragger and FragPipe use parameter "DatabaseSplitCount" to enable FASTA file splitting (which is handled natively by MSFragger and FragPipe)

            if (jobParams.TryGetParam("ParallelMSGFPlus", "SplitFasta", out _, false))
            {
                UsingSplitFasta = jobParams.GetJobParameter("ParallelMSGFPlus", "SplitFasta", false);
            }

            // When running DTA_Refinery, assure that UsingSplitFasta is false
            if (UsingSplitFasta && string.Equals(jobParams.GetParam("StepTool"), "DTA_Refinery"))
            {
                UsingSplitFasta = false;
            }

            // Update mOrgDBDescription and UsingLegacyFasta
            UpdateDescription();
        }

        /// <summary>
        /// Validate that either ProteinCollectionList or LegacyFastaName is defined
        /// Updates mOrgDBDescription, UsingLegacyFasta, and IsValid
        /// </summary>
        /// <remarks>Updates ErrorMessage if an error</remarks>
        public void UpdateDescription()
        {
            if (!string.IsNullOrWhiteSpace(ProteinCollectionList) && !string.Equals(ProteinCollectionList, "na", StringComparison.OrdinalIgnoreCase))
            {
                OrgDBDescription = "Protein collection: " + ProteinCollectionList + " with options " + ProteinCollectionOptions;
                UsingLegacyFasta = false;
                IsValid = true;
            }
            else if (!string.IsNullOrWhiteSpace(LegacyFastaName) && !string.Equals(LegacyFastaName, "na", StringComparison.OrdinalIgnoreCase))
            {
                OrgDBDescription = "Legacy DB: " + LegacyFastaName;
                UsingLegacyFasta = true;
                IsValid = true;
            }
            else
            {
                ErrorMessage = "Both the ProteinCollectionList and LegacyFastaFileName parameters are empty or 'na'";
                IsValid = false;
            }
        }
    }
}