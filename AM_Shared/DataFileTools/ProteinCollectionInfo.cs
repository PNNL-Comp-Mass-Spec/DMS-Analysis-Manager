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
            UsingSplitFasta = jobParams.GetJobParameter("SplitFasta", false);

            // When running DTA_Refinery, we override UsingSplitFasta to false
            if (string.Equals(jobParams.GetParam("StepTool"), "DTA_Refinery"))
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