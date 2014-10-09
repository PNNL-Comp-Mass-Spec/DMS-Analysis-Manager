using AnalysisManagerBase;
using AnalysisManager_MAC;
using System.Collections.Generic;
using System.Linq;

namespace AnalysisManager_Mage_PlugIn
{

    public class clsAnalysisResourcesMage : clsAnalysisResourcesMAC
    {
        public override AnalysisManagerBase.IJobParams.CloseOutType GetResources()
        {
            var mageOperations = m_jobParams.GetParam("MageOperations", string.Empty);

            var jobAnalysistype = m_jobParams.GetParam("AnalysisType", string.Empty);
            bool requireDeconJobs = jobAnalysistype == "iTRAQ";

            bool requireMasicJobs = mageOperations.Contains("ImportReporterIons");

            var workFlowSteps = m_jobParams.GetParam("ApeWorkflowStepList", string.Empty);
            if (workFlowSteps.Contains("4plex") |
                workFlowSteps.Contains("6plex") |
                workFlowSteps.Contains("8plex"))
            {
                if (!mageOperations.Contains("ImportReporterIons"))
                {
                    LogError("ApeWorkflowStepList contains 4plex, 6plex, or 8plex; MageOperations must contain ImportReporterIons");
                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }

                requireMasicJobs = true;
            }

            if (!(requireMasicJobs || requireDeconJobs))
            {
                return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
            }

            // Lookup the jobs associated with this data package
            var dataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1);
            if (dataPackageID < 0)
            {
                LogError("DataPackageID is not defined");
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            var connectionString = m_mgrParams.GetParam("brokerconnectionstring");

            List<udtDataPackageJobInfoType> lstAdditionalJobs;
            var peptideHitJobs = RetrieveDataPackagePeptideHitJobInfo(connectionString, dataPackageID, out lstAdditionalJobs);

            if (peptideHitJobs.Count == 0)
            {
                LogError("Data package " + dataPackageID + " does not have any PeptideHit jobs");
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            bool masicJobsAreValid = true;
            bool deconJobsAreValid = true;
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
                return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;

            return IJobParams.CloseOutType.CLOSEOUT_FAILED;
        }

        private bool ValidateDeconJobs(int dataPackageID, List<udtDataPackageJobInfoType> peptideHitJobs, List<udtDataPackageJobInfoType> lstAdditionalJobs)
        {
            return ValidateMatchingJobs(dataPackageID, peptideHitJobs, lstAdditionalJobs, "HMMA_Peak", "DeconTools");
        }

        private bool ValidateMasicJobs(int dataPackageID, List<udtDataPackageJobInfoType> peptideHitJobs, List<udtDataPackageJobInfoType> lstAdditionalJobs)
         {
             return ValidateMatchingJobs(dataPackageID, peptideHitJobs, lstAdditionalJobs, "SIC", "MASIC");
         }

        private bool ValidateMatchingJobs(
            int dataPackageID, List<udtDataPackageJobInfoType> peptideHitJobs, 
            List<udtDataPackageJobInfoType> lstAdditionalJobs, 
            string resultType, 
            string toolName)
        {
            int missingMasicCount = 0;

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

            string msg = "Data package " + dataPackageID;

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

            m_message = clsGlobal.AppendToComment(m_message, msg);
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

            return false;
        }

    }
}