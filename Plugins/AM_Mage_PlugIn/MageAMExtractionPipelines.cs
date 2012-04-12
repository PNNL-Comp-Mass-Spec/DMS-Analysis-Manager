using System;
using System.Collections.Generic;
using Mage;
using MageExtExtractionFilters;
using MageDisplayLib;
using AnalysisManagerBase;

namespace AnalysisManager_Mage_PlugIn {

    /// <summary>
    /// Class that defines basic Mage pipelines and functions that 
    /// provide sub-operations that make up file extraction operations 
    /// that Mac Mage plug-in can execute.
    /// 
    /// These extraction operations are essentially identical to the operations
    /// permormed by the MageFileExtractor tool.
    /// </summary>
    public class MageAMExtractionPipelines : MageAMPipelineBase {

        #region Member Variables
   
        /// <summary>
        /// The parameters for the slated extraction
        /// </summary>
        protected ExtractionType mExtractionParms = null;

        #endregion

        #region Constructors 

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParms"></param>
        /// <param name="mgrParms"></param>
        /// <param name="monitor"></param>
        public MageAMExtractionPipelines(IJobParams jobParms, IMgrParams mgrParms) : base(jobParms,  mgrParms) {           
        }

        #endregion

        /// <summary>
        /// Setup and run Mage Extractor pipleline according to job parameters
        /// </summary>
        public void ExtractFromJobs(String sql) {
            GetExtractionParametersFromJobParameters();
            BaseModule jobList = GetListOfDMSItems(sql);
            ExtractFromJobsList(jobList);
        }

        /// <summary>
        /// Get the parameters for the extraction pipeline modules from the job parameters
        /// </summary>
        protected void GetExtractionParametersFromJobParameters() {
            mExtractionParms = new ExtractionType();
            mDestination = new DestinationType();

            // extraction and filtering parameters
            String extractionType = RequireJobParam("ExtractionType"); //"Sequest First Hits"
            mExtractionParms.RType = ResultType.TypeList[extractionType];

            mExtractionParms.KeepAllResults = GetJobParam("KeepAllResults", "Yes");
            mExtractionParms.ResultFilterSetID = GetJobParam("ResultFilterSetID", "All Pass");
            mExtractionParms.MSGFCutoff = GetJobParam("MSGFCutoff", "All Pass");

            // ouput parameters
            mDestination.Type = DestinationType.Types.SQLite_Output;
            mDestination.ContainerPath = System.IO.Path.Combine(mWorkingDir, mResultsDBFileName);
            mDestination.Name = "t_results";
        }

        /// <summary>
        /// Build pipeline to perform extraction operation against jobs in jobList
        /// </summary>
        /// <param name="jobList">List of jobs to perform extraction from</param>
        protected void ExtractFromJobsList(BaseModule jobList) {
            mPipelineQueue = ExtractionPipelines.MakePipelineQueueToExtractFromJobList(jobList, mExtractionParms, mDestination);
            foreach (ProcessingPipeline p in mPipelineQueue.Pipelines.ToArray()) {
                ConnectPipelineToStatusHandlers(p);
            }
            ConnectPipelineQueueToStatusHandlers(mPipelineQueue);
            mPipelineQueue.RunRoot(null);
        }
    }
}
