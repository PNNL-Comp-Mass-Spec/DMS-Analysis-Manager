
namespace AnalysisManagerBase
{
    public class clsProteinCollectionInfo
    {

        private string mOrgDBDescription;

        /// <summary>
        /// Legacy Fasta file name
        /// </summary>
        /// <remarks>Will be "na" when using a protein collection</remarks>
        public string LegacyFastaName { get; set; }

        public string ProteinCollectionOptions { get; set; }
        public string ProteinCollectionList { get; set; }

        public bool UsingLegacyFasta { get; set; }
        public bool UsingSplitFasta { get; set; }
        public string ErrorMessage { get; set; }
        public bool IsValid { get; set; }

        public string OrgDBDescription => mOrgDBDescription;

        public clsProteinCollectionInfo(IJobParams jobParams)
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

        public void UpdateDescription()
        {
            if (!string.IsNullOrWhiteSpace(ProteinCollectionList) && ProteinCollectionList.ToLower() != "na")
            {
                mOrgDBDescription = "Protein collection: " + ProteinCollectionList + " with options " + ProteinCollectionOptions;
                UsingLegacyFasta = false;
                IsValid = true;
            }
            else if (!string.IsNullOrWhiteSpace(LegacyFastaName) && LegacyFastaName.ToLower() != "na")
            {
                mOrgDBDescription = "Legacy DB: " + LegacyFastaName;
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