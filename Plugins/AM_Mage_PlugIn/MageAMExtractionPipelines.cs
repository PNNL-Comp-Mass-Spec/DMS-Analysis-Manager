using System;
using Mage;
using MageExtExtractionFilters;
using AnalysisManagerBase;

namespace AnalysisManager_Mage_PlugIn
{

    /// <summary>
    /// Class that defines basic Mage pipelines and functions that
    /// provide sub-operations that make up file extraction operations
    /// that Mac Mage plug-in can execute.
    ///
    /// These extraction operations are essentially identical to the operations
    /// permormed by the MageFileExtractor tool.
    /// </summary>
    public class MageAMExtractionPipelines : MageAMPipelineBase
    {

        #region Member Variables

        /// <summary>
        /// The parameters for the slated extraction
        /// </summary>
        protected ExtractionType ExtractionParms;

        #endregion

        #region Constructors

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParms"></param>
        /// <param name="mgrParms"></param>
        public MageAMExtractionPipelines(IJobParams jobParms, IMgrParams mgrParms) : base(jobParms, mgrParms)
        {
        }

        #endregion

        /// <summary>
        /// Setup and run Mage Extractor pipleline according to job parameters
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
            ExtractionParms = new ExtractionType();

            var containerPath = System.IO.Path.Combine(WorkingDirPath, ResultsDBFileName);
            ResultsDestination = new DestinationType(DestinationType.Types.SQLite_Output.ToString(), containerPath, "t_results");

            // extraction and filtering parameters
            var extractionType = RequireJobParam("ExtractionType"); // "MSGF+ Synopsis All Proteins" or "Sequest First Hits"

            try
            {
                ExtractionParms.RType = ResultType.TypeList[extractionType];
            }
            catch
            {
                throw new Exception("Unrecognized value for ExtractionType: " + extractionType);
            }

            ExtractionParms.KeepAllResults = GetJobParam("KeepAllResults", "Yes");
            ExtractionParms.ResultFilterSetID = GetJobParam("ResultFilterSetID", "All Pass");
            ExtractionParms.MSGFCutoff = GetJobParam("MSGFCutoff", "All Pass");

        }

        /// <summary>
        /// Build pipeline to perform extraction operation against jobs in jobList
        /// </summary>
        /// <param name="jobList">List of jobs to perform extraction from</param>
        protected void ExtractFromJobsList(BaseModule jobList)
        {
            BasePipelineQueue = ExtractionPipelines.MakePipelineQueueToExtractFromJobList(jobList, ExtractionParms, ResultsDestination);
            foreach (var p in BasePipelineQueue.Pipelines.ToArray())
            {
                ConnectPipelineToStatusHandlers(p);
            }
            ConnectPipelineQueueToStatusHandlers(BasePipelineQueue);
            BasePipelineQueue.RunRoot(null);
        }
    }
}
