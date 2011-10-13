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

        public MageAMExtractionPipelines(IJobParams jobParms, IMgrParams mgrParms, clsAnalysisToolRunnerMage monitor) {
            this.mJobParms = jobParms;
            this.mMgrParms = mgrParms;
            this.mMonitor = monitor;
        }


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

            // GetDatasetFactorsFromDataPackage(dataPackageID);
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
        /// <returns>A Mage module containing list of jobs</returns>
        private BaseModule GetListOfJobsFromDataPackage(String dataPackageID) {
            String sql = "SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0}";
            sql = string.Format(sql, dataPackageID);

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
        private void ExtractFromJobs(BaseModule jobList) {
            mPipelineQueue = ExtractionPipelines.MakePipelineQueueToExtractFromJobList(jobList, mExtractionParms, mDestination);
            foreach (ProcessingPipeline p in mPipelineQueue.Pipelines.ToArray()) {
                //        mMonitor.ConnectPipelineToStatusHandlers(p);
            }
            //    mMonitor.ConnectPipelineQueueToStatusHandlers(mPipelineQueue);
            mPipelineQueue.RunRoot(null);
        }

        /// <summary>
        /// 
        /// </summary>
        public void GetDatasetFactorsFromDataPackage(String dataPackageID) {
            String sql = "SELECT Dataset, Dataset_ID, Factor, Value FROM V_Custom_Factors_List_Report WHERE Dataset IN (SELECT Dataset FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0})";
            sql = string.Format(sql, dataPackageID);

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
        /// <param name="sql"></param>
        /// <returns></returns>
        private MSSQLReader MakeDBReaderModule(String sql) {
            MSSQLReader reader = new MSSQLReader();
            //reader.Server = "gigasax";
            //reader.Database = "DMS5";
            reader.ConnectionString = mMgrParms.GetParam("brokerconnectionstring");
            reader.SQLText = sql;
            return reader;
        }


    }
}
