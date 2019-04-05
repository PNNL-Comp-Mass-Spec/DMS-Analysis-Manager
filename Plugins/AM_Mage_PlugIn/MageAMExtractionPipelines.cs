using AnalysisManagerBase;
using Mage;
using MageExtExtractionFilters;
using System;

namespace AnalysisManager_Mage_PlugIn
{

    /// <summary>
    /// Class that defines basic Mage pipelines and functions that
    /// provide sub-operations that make up file extraction operations
    /// that Mac Mage plug-in can execute.
    ///
    /// These extraction operations are essentially identical to the operations
    /// performed by the MageFileExtractor tool.
    /// </summary>
    public class MageAMExtractionPipelines : MageAMPipelineBase
    {

        #region Member Variables

        /// <summary>
        /// The parameters for the slated extraction
        /// </summary>
        protected ExtractionType ExtractionParams;

        #endregion

        #region Constructors

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="mgrParams"></param>
        public MageAMExtractionPipelines(IJobParams jobParams, IMgrParams mgrParams) : base(jobParams, mgrParams)
        {
        }

        #endregion

        /// <summary>
        /// Setup and run Mage Extractor pipeline according to job parameters
        /// </summary>
        public void ExtractFromJobs(string sql)
        {
            GetExtractionParametersFromJobParameters();
            BaseModule jobList = GetListOfDMSItems(sql);
            ExtractFromJobsList(jobList);
        }

        /// <summary>
        /// Get the parameters for the extraction pipeline modules from the job parameters
        /// </summary>
        protected void GetExtractionParametersFromJobParameters()
        {
            ExtractionParams = new ExtractionType();

            var containerPath = System.IO.Path.Combine(WorkingDirPath, ResultsDBFileName);
            ResultsDestination = new DestinationType(DestinationType.Types.SQLite_Output.ToString(), containerPath, "t_results");

            // extraction and filtering parameters
            var extractionType = RequireJobParam("ExtractionType"); // "MSGF+ Synopsis All Proteins" or "Sequest First Hits"

            try
            {
                ExtractionParams.RType = ResultType.TypeList[extractionType];
            }
            catch
            {
                throw new Exception("Unrecognized value for ExtractionType: " + extractionType);
            }

            ExtractionParams.KeepAllResults = GetJobParam("KeepAllResults", "Yes");
            ExtractionParams.ResultFilterSetID = GetJobParam("ResultFilterSetID", "All Pass");
            ExtractionParams.MSGFCutoff = GetJobParam("MSGFCutoff", "All Pass");

        }

        /// <summary>
        /// Build pipeline to perform extraction operation against jobs in jobList
        /// </summary>
        /// <param name="jobList">List of jobs to perform extraction from</param>
        protected void ExtractFromJobsList(BaseModule jobList)
        {
            if (jobList is SimpleSink jobsToProcess){
                JobCount = jobsToProcess.Rows.Count;
            }

            BasePipelineQueue = ExtractionPipelines.MakePipelineQueueToExtractFromJobList(jobList, ExtractionParams, ResultsDestination);
            foreach (var p in BasePipelineQueue.Pipelines.ToArray())
            {
                ConnectPipelineToStatusHandlers(p);
            }
            ConnectPipelineQueueToStatusHandlers(BasePipelineQueue);

            BasePipelineQueue.RunRoot(null);
        }
    }
}
