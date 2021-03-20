using AnalysisManagerBase;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AnalysisManagerBase.JobConfig;
using PRISMDatabaseUtils;

namespace AnalysisManager_Mage_PlugIn
{
    /// <summary>
    /// Retrieve resources for the Mage plugin
    /// </summary>
    public class AnalysisResourcesMage : AnalysisResourcesMAC
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

            var mageOperations = mJobParams.GetParam("MageOperations", string.Empty);

            var jobAnalysisType = mJobParams.GetParam("AnalysisType", string.Empty);
            var checkDeconJobs = jobAnalysisType == "iTRAQ";

            var requireMasicJobs = mageOperations.Contains("ImportReporterIons");

            var workFlowSteps = mJobParams.GetParam("ApeWorkflowStepList", string.Empty);
            if (workFlowSteps.Contains("4plex") ||
                workFlowSteps.Contains("6plex") ||
                workFlowSteps.Contains("8plex"))
            {
                if (!mageOperations.Contains("ImportReporterIons"))
                {
                    LogError("ApeWorkflowStepList contains 4plex, 6plex, or 8plex; MageOperations must contain ImportReporterIons");
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                requireMasicJobs = true;
            }

            if (!(requireMasicJobs || checkDeconJobs))
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            // Lookup the jobs associated with this data package
            var dataPackageID = mJobParams.GetJobParameter("DataPackageID", -1);
            if (dataPackageID < 0)
            {
                LogError("DataPackageID is not defined");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var connectionString = mMgrParams.GetParam("BrokerConnectionString");

            var dbTools = DbToolsFactory.GetDBTools(connectionString, debugMode: mMgrParams.TraceMode);
            RegisterEvents(dbTools);

            var peptideHitJobs = DataPackageInfoLoader.RetrieveDataPackagePeptideHitJobInfo(
                dbTools, dataPackageID, out var lstAdditionalJobs, out var errorMsg);

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
            mMessage = string.Empty;

            // Confirm that every Peptide_Hit job has a corresponding MASIC job (required to populate table T_Reporter_Ions)
            if (requireMasicJobs)
            {
                masicJobsAreValid = ValidateMasicJobs(dataPackageID, peptideHitJobs, lstAdditionalJobs);
            }

            if (checkDeconJobs)
            {
                ValidateDeconJobs(dataPackageID, peptideHitJobs, lstAdditionalJobs);
            }

            return masicJobsAreValid ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        private void ValidateDeconJobs(
            int dataPackageID,
            IReadOnlyCollection<DataPackageJobInfo> peptideHitJobs,
            IReadOnlyCollection<DataPackageJobInfo> lstAdditionalJobs)
        {
            ValidateMatchingJobs(dataPackageID, peptideHitJobs, lstAdditionalJobs, "HMMA_Peak", "DeconTools");
        }

        private bool ValidateMasicJobs(int dataPackageID,
            IReadOnlyCollection<DataPackageJobInfo> peptideHitJobs,
            IReadOnlyCollection<DataPackageJobInfo> lstAdditionalJobs)
        {
            return ValidateMatchingJobs(dataPackageID, peptideHitJobs, lstAdditionalJobs, "SIC", "MASIC");
        }

        private bool ValidateMatchingJobs(
            int dataPackageID,
            IReadOnlyCollection<DataPackageJobInfo> peptideHitJobs,
            IReadOnlyCollection<DataPackageJobInfo> lstAdditionalJobs,
            string resultType,
            string toolName)
        {
            var missingJobCount = 0;

            foreach (var job in peptideHitJobs)
            {
                var datasetID = job.DatasetID;

                var matchFound =
                    (from matchingJob in lstAdditionalJobs
                     where matchingJob.DatasetID == datasetID && matchingJob.ResultType == resultType
                     select matchingJob).Any();

                if (!matchFound)
                    missingJobCount++;
            }

            if (missingJobCount <= 0)
            {
                return true;
            }

            var msg = new StringBuilder();
            msg.AppendFormat("Data package {0}", dataPackageID);

            if (missingJobCount == peptideHitJobs.Count)
            {
                msg.AppendFormat(" does not have any {0} jobs", toolName);
            }
            else
            {
                msg.AppendFormat(" has {0} PeptideHit job", missingJobCount);
                if (missingJobCount > 1)
                    msg.Append("s that do not");
                else
                    msg.Append(" that does not");

                msg.AppendFormat(" have a matching corresponding {0} job", toolName);
            }

            switch (toolName)
            {
                case "MASIC":
                    LogError(msg + "; required in order to extract reporter ion information");
                    break;

                case "DeconTools":
                    // Data package 3545 does not have any DeconTools jobs; the IDM tool uses _isos.csv files if they exist, but they're not required
                    var warningMessage = msg + "; the IDM tool uses _isos.csv files if they exist, but they're not required";
                    LogWarning(warningMessage);
                    mJobParams.AddAdditionalParameter("AnalysisResourcesClass", "Evaluation_Message", warningMessage);
                    break;

                default:
                    LogError(msg.ToString());
                    break;
            }

            return false;
        }
    }
}