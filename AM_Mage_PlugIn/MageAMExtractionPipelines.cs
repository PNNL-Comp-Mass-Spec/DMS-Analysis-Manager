using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mage;
using MageExtExtractionFilters;
using MageDisplayLib;
using AnalysisManagerBase;

namespace AnalysisManager_Mage_PlugIn {

    class MageAMExtractionPipelines {

        #region Member Variables

        IJobParams mJobParms;

        IMgrParams mMgrParms;

        #endregion

        public MageAMExtractionPipelines(IJobParams jobParms, IMgrParams mgrParms, IPipelineMonitor monitor) {
            this.mJobParms = jobParms;
            this.mMgrParms = mgrParms;
            this.mMonitor = monitor;
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

        IPipelineMonitor mMonitor = null;

        #endregion


        #region Initialization

        #endregion

        /// <summary>
        /// Setup and run Mage Extractor pipleline according to job parameters
        /// </summary>
        public void ExtractJobsFromDataPackage() {
            GetExtractionParametersFromJobParameters();
            String dataPackageID = mJobParms.GetParam("DataPackageID"); ;
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
            String extractionType = mJobParms.GetParam("ExtractionType"); //"Sequest First Hits"
            mExtractionParms.RType = ResultType.TypeList[extractionType];

            mExtractionParms.KeepAllResults = mJobParms.GetParam("KeepAllResults"); //"Yes"
            mExtractionParms.ResultFilterSetID = mJobParms.GetParam("ResultFilterSetID"); // "All Pass"
            mExtractionParms.MSGFCutoff = mJobParms.GetParam("MSGFCutoff"); // "All Pass"

            // ouput parameters
            mDestination.Type = DestinationType.Types.SQLite_Output;
            mDestination.ContainerPath = @"C:\Data\Hunk"; //  m_mgrParams.GetParam("workdir")
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
            string connStr = mMgrParms.GetParam("brokerconnectionstring"); // Future: modify MSSQLReader to accept connection string

            reader.SQLText = string.Format("SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0}", dataPackageID);

            ProcessingPipeline pipeline = ProcessingPipeline.Assemble("Get Jobs", reader, jobList);
            mMonitor.ConnectPipelineToStatusHandlers(pipeline);
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
                mMonitor.ConnectPipelineToStatusHandlers(p);
            }
            mMonitor.ConnectPipelineQueueToStatusHandlers(mPipelineQueue);
            mPipelineQueue.RunRoot(null);
        }



    }
}
