using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mage;
using MageExtExtractionFilters;
using MageDisplayLib;

namespace AM_Mage_PlugIn {


    class MageAMExtractionPipelines {

        #region Member Variables

        Dictionary<String, String> parms;

        #endregion

        public MageAMExtractionPipelines(Dictionary<String, String> parameters) {
            this.parms = parameters;
        }

        #region Member Variables


        /// <summary>
        /// Pipeline queue for running the multiple pipelines that make up the workflows for this module
        /// </summary>
        private PipelineQueue mPipelineQueue = new PipelineQueue();

        /// <summary>
        /// The parameters for the slated extraction
        /// </summary>
        private ExtractionType mExtractionParms = null;

        /// <summary>
        /// Where extracted results will be delivered
        /// </summary>
        private DestinationType mDestination = null;

        #endregion


        #region Initialization

        #endregion

        /// <summary>
        /// Setup and run Mage Extractor pipleline according to job parameters
        /// </summary>
        internal void Run() {
            GetExtractionParametersFromJobParameters();
            String dataPackageID = "";
            BaseModule jobList = GetListOfJobsFromDataPackage(dataPackageID);
            ExtractFromJobs(jobList);
        }

        /// <summary>
        /// Get the parameters for the extraction pipeline modules from the job parameters
        /// </summary>
        private void GetExtractionParametersFromJobParameters() {
            mExtractionParms = new ExtractionType();
            mDestination = new DestinationType();

            // extraction and filtering parameters
            mExtractionParms.RType = ResultType.TypeList["Sequest First Hits"];
            mExtractionParms.KeepAllResults = "No";
            mExtractionParms.ResultFilterSetID = "203";
            mExtractionParms.MSGFCutoff = "All Pass";

            // ouput parameters
            mDestination.Type = DestinationType.Types.SQLite_Output;
            mDestination.ContainerPath = @"C:\Data\Hunk";
            mDestination.Name = "result.db3";
        }

        /// <summary>
        /// Get a list of jobs to process
        /// </summary>
        /// <returns>A Mage module containing list of jobs</returns>
        private BaseModule GetListOfJobsFromDataPackage(String dataPackageID) {
            SimpleSink jobList = new SimpleSink();

            MSSQLReader reader = new MSSQLReader();
            reader.Server = "gigasax";
            reader.Database = "DMS5";
            reader.SQLText = string.Format("SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0}", dataPackageID);

            ProcessingPipeline pipeline = ProcessingPipeline.Assemble("Get Jobs", reader, jobList);
            ConnectPipelineToStatusHandlers(pipeline);
            pipeline.RunRoot(null);

            return jobList;
        }

        /// <summary>
        /// Build pipeline to perform extraction operation against jobs in jobList
        /// </summary>
        /// <param name="jobList">List of jobs to perform extraction from</param>
        private void ExtractFromJobs(BaseModule jobList) {
            mPipelineQueue = ExtractionPipelines.MakePipelineQueueToExtractFromJobList(jobList, mExtractionParms, mDestination);
            foreach (ProcessingPipeline p in mPipelineQueue.Pipelines.ToArray()) {
                ConnectPipelineToStatusHandlers(p);
            }
            ConnectPipelineQueueToStatusHandlers(mPipelineQueue);
            mPipelineQueue.RunRoot(null);
        }



    }
}
