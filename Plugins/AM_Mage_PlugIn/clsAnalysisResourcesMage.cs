using AnalysisManagerBase;
using System.Collections.Generic;
using System.Linq;

namespace AnalysisManager_Mage_PlugIn
{

    /// <summary>
    /// Retrieve resources for the Mage plugin
    /// </summary>
    public class clsAnalysisResourcesMage : clsAnalysisResourcesMAC
    {

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            var mageOperations = m_jobParams.GetParam("MageOperations", string.Empty);

            var jobAnalysistype = m_jobParams.GetParam("AnalysisType", string.Empty);
            var requireDeconJobs = jobAnalysistype == "iTRAQ";

            var requireMasicJobs = mageOperations.Contains("ImportReporterIons");

            var workFlowSteps = m_jobParams.GetParam("ApeWorkflowStepList", string.Empty);
            if (workFlowSteps.Contains("4plex") |
                workFlowSteps.Contains("6plex") |
                workFlowSteps.Contains("8plex"))
            {
                if (!mageOperations.Contains("ImportReporterIons"))
                {
                    LogError("ApeWorkflowStepList contains 4plex, 6plex, or 8plex; MageOperations must contain ImportReporterIons");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                requireMasicJobs = true;
            }

            if (!(requireMasicJobs || requireDeconJobs))
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            // Lookup the jobs associated with this data package
            var dataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1);
            if (dataPackageID < 0)
            {
                LogError("DataPackageID is not defined");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var connectionString = m_mgrParams.GetParam("brokerconnectionstring");

            var peptideHitJobs = clsDataPackageInfoLoader.RetrieveDataPackagePeptideHitJobInfo(
                connectionString, dataPackageID, out var lstAdditionalJobs, out var errorMsg);

            if (!string.IsNullOrEmpty(errorMsg))
            {
                LogError(errorMsg);
                return CloseOutType.CLOSEOUT_FAILED;
            }

            if (peptideHitJobs.Count == 0)
            {
                LogError("Data package " + dataPackageID + " does not have any PeptideHit jobs");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var masicJobsAreValid = true;
            var deconJobsAreValid = true;
            m_message = string.Empty;

            // Confirm that every Peptide_Hit job has a corresponding MASIC job (required to populate table T_Reporter_Ions)
            if (requireMasicJobs)
            {
                masicJobsAreValid = ValidateMasicJobs(dataPackageID, peptideHitJobs, lstAdditionalJobs);
            }

            if (requireDeconJobs)
            {
                deconJobsAreValid = ValidateDeconJobs(dataPackageID, peptideHitJobs, lstAdditionalJobs);
            }

            if (masicJobsAreValid && deconJobsAreValid)
                return CloseOutType.CLOSEOUT_SUCCESS;

            return CloseOutType.CLOSEOUT_FAILED;
        }

        private bool ValidateDeconJobs(
            int dataPackageID,
            IReadOnlyCollection<clsDataPackageJobInfo> peptideHitJobs,
            IReadOnlyCollection<clsDataPackageJobInfo> lstAdditionalJobs)
        {
            return ValidateMatchingJobs(dataPackageID, peptideHitJobs, lstAdditionalJobs, "HMMA_Peak", "DeconTools");
        }

        private bool ValidateMasicJobs(int dataPackageID,
            IReadOnlyCollection<clsDataPackageJobInfo> peptideHitJobs,
            IReadOnlyCollection<clsDataPackageJobInfo> lstAdditionalJobs)
        {
            return ValidateMatchingJobs(dataPackageID, peptideHitJobs, lstAdditionalJobs, "SIC", "MASIC");
        }

        private bool ValidateMatchingJobs(
            int dataPackageID,
            IReadOnlyCollection<clsDataPackageJobInfo> peptideHitJobs,
            IReadOnlyCollection<clsDataPackageJobInfo> lstAdditionalJobs,
            string resultType,
            string toolName)
        {
            var missingMasicCount = 0;

            foreach (var job in peptideHitJobs)
            {
                var datasetID = job.DatasetID;

                var matchFound =
                    (from matchingJob in lstAdditionalJobs
                     where matchingJob.DatasetID == datasetID && matchingJob.ResultType == resultType
                     select matchingJob).Any();

                if (!matchFound)
                    missingMasicCount++;
            }

            if (missingMasicCount <= 0)
            {
                return true;
            }

            var msg = "Data package " + dataPackageID;

            if (missingMasicCount == peptideHitJobs.Count)
                msg += " does not have any " + toolName + " jobs";
            else
            {
                msg += " has " + missingMasicCount + " PeptideHit job";
                if (missingMasicCount > 1)
                    msg += "s that do not";
                else
                    msg += " that does not";

                msg += " have a matching corresponding " + toolName + " job";
            }

            if (toolName == "MASIC")
                msg += "; required in order to extract reporter ion information";

            if (toolName == "DeconTools")
                msg += "; required for IDM";

            LogError(msg);

            return false;
        }

    }
}