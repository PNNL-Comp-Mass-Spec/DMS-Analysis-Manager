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
        public void ExtractFromJobs(String dataPackageID) {
            GetExtractionParametersFromJobParameters();
            BaseModule jobList = GetListOfJobs(dataPackageID);
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
            mDestination.ContainerPath = Path.Combine(mWorkingDir, mResultsDBFileName);
            mDestination.Name = "t_results";
        }

        /// <summary>
        /// Get a list of jobs to process
        /// </summary>
        /// <param name="sql">Query to use a source of jobs</param>
        /// <returns>A Mage module containing list of jobs</returns>
        protected BaseModule GetListOfJobs(string sql) {
            SimpleSink jobList = new SimpleSink();

            MSSQLReader reader = MakeDBReaderModule(sql);

            ProcessingPipeline pipeline = ProcessingPipeline.Assemble("Get Jobs", reader, jobList);
            ConnectPipelineToStatusHandlers(pipeline);
            pipeline.RunRoot(null);

            return jobList;
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
            writer.DbPath = Path.Combine(mWorkingDir, mResultsDBFileName);
            writer.TableName = "t_factors";

            ProcessingPipeline pipeline = ProcessingPipeline.Assemble("CrosstabFactors", reader, crosstab, writer);

            ConnectPipelineToStatusHandlers(pipeline);
            pipeline.RunRoot(null);
        }

        /// <summary>
        /// Create a new MSSQLReader module to do a specific query
        /// </summary>
        /// <param name="sql">Query to use</param>
        /// <returns></returns>
        protected MSSQLReader MakeDBReaderModule(String sql) {
            MSSQLReader reader = new MSSQLReader();
            reader.ConnectionString = RequireMgrParam("ConnectionString");
            reader.SQLText = sql;
            return reader;
        }


    }
}
