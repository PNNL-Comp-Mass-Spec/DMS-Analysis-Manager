using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Mage;
using MageExtExtractionFilters;
using MageDisplayLib;
using AnalysisManagerBase;
using System.IO;

namespace AnalysisManager_Mage_PlugIn {

    class MageAMExtractionPipelines {

        public const string RESULT_DB_NAME = "result.db3";

        #region Member Variables

        IJobParams mJobParms;

        IMgrParams mMgrParms;

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

        clsAnalysisToolRunnerMage mMonitor = null;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParms"></param>
        /// <param name="mgrParms"></param>
        /// <param name="monitor"></param>
        public MageAMExtractionPipelines(IJobParams jobParms, IMgrParams mgrParms, clsAnalysisToolRunnerMage monitor) {
            this.mJobParms = jobParms;
            this.mMgrParms = mgrParms;
            this.mMonitor = monitor;
        }

        /// <summary>
        /// Setup and run Mage Extractor pipleline according to job parameters
        /// </summary>
        public void ExtractFromJobs(String dataPackageID) {
            GetExtractionParametersFromJobParameters();
            BaseModule jobList = GetListOfJobs(dataPackageID);
            ExtractFromJobsList(jobList);
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
            String workdir = mMgrParms.GetParam("workdir");
            mDestination.Type = DestinationType.Types.SQLite_Output;
            mDestination.ContainerPath = Path.Combine(workdir, RESULT_DB_NAME);
            mDestination.Name = "t_results";
        }

        /// <summary>
        /// Get a list of jobs to process
        /// </summary>
        /// <param name="sql">Query to use a source of jobs</param>
        /// <returns>A Mage module containing list of jobs</returns>
        private BaseModule GetListOfJobs(string sql) {
            SimpleSink jobList = new SimpleSink();

            MSSQLReader reader = MakeDBReaderModule(sql);

            ProcessingPipeline pipeline = ProcessingPipeline.Assemble("Get Jobs", reader, jobList);
            //  mMonitor.ConnectPipelineToStatusHandlers(pipeline);
            pipeline.RunRoot(null);

            return jobList;
        }

        /// <summary>
        /// Build pipeline to perform extraction operation against jobs in jobList
        /// </summary>
        /// <param name="jobList">List of jobs to perform extraction from</param>
        private void ExtractFromJobsList(BaseModule jobList) {
            mPipelineQueue = ExtractionPipelines.MakePipelineQueueToExtractFromJobList(jobList, mExtractionParms, mDestination);
            foreach (ProcessingPipeline p in mPipelineQueue.Pipelines.ToArray()) {
                //        mMonitor.ConnectPipelineToStatusHandlers(p);
            }
            //    mMonitor.ConnectPipelineQueueToStatusHandlers(mPipelineQueue);
            mPipelineQueue.RunRoot(null);
        }

        /// <summary>
        /// make Mage pipeline using given sql as source of factors and use it 
        /// to create and populate a factors table in a SQLite database (in crosstab format)
        /// </summary>
        /// <param name="sql">Query to use a source of factors</param>
        public void GetDatasetFactors(string sql) {
            MSSQLReader reader = MakeDBReaderModule(sql);

            CrosstabFilter crosstab = new CrosstabFilter();
            crosstab.EntityNameCol = "Dataset";
            crosstab.EntityIDCol = "Dataset_ID";
            crosstab.FactorNameCol = "Factor";
            crosstab.FactorValueCol = "Value";

            SQLiteWriter writer = new SQLiteWriter();
            String workdir = mMgrParms.GetParam("workdir");
            writer.DbPath = Path.Combine(workdir, RESULT_DB_NAME);
            writer.TableName = "t_factors";

            ProcessingPipeline pipeline = ProcessingPipeline.Assemble("CrosstabFactors", reader, crosstab, writer);

            //     mMonitor.ConnectPipelineToStatusHandlers(pipeline);
            pipeline.RunRoot(null);
        }

        /// <summary>
        /// Create a new MSSQLReader module to do a specific query
        /// </summary>
        /// <param name="sql">Query to use</param>
        /// <returns></returns>
        private MSSQLReader MakeDBReaderModule(String sql) {
            MSSQLReader reader = new MSSQLReader();
            reader.ConnectionString = mMgrParms.GetParam("ConnectionString");
            reader.SQLText = sql;
            return reader;
        }


    }
}
