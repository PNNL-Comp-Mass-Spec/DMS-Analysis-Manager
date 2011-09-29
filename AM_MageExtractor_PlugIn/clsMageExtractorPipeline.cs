using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using log4net;
using Mage;
using MageExtExtractionFilters;
using MageDisplayLib;
using System.IO;

namespace AnalysisManager_MageExtractor_PlugIn {

    class clsMageExtractorPipeline {

        #region Member Variables

        private ILog traceLog;

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

        public clsMageExtractorPipeline() {
            Initialize();
        }

        #region Initialization

        public void Initialize() {
            // set up configuration folder and files
            SavedState.SetupConfigFiles("MageExtractorCmdLine");

            // Set log4net path and kick the logger into action
            string LogFileName = Path.Combine(SavedState.DataDirectory, "log.txt");
            log4net.GlobalContext.Properties["LogName"] = LogFileName;
            traceLog = LogManager.GetLogger("TraceLog");
            traceLog.Info("Starting");

            // connect the pipeline queue to message handlers
            ConnectPipelineQueueToStatusDisplay(mPipelineQueue);
        }
        #endregion

        /// <summary>
        /// Setup and run Mage Extractor pipleline according to job parameters
        /// </summary>
        internal void Run() {
            GetExtractionParametersFromJobParameters();
            BaseModule jobList = GetListOfJobsToProcess();
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
            mDestination.Type = DestinationType.Types.File_Output;
            mDestination.ContainerPath = @"C:\Data\Hunk";
            mDestination.Name = "mage_extr_plugin_results.txt";
        }

        /// <summary>
        /// Get a list of jobs to process
        /// </summary>
        /// <returns>A Mage module containing list of jobs</returns>
        private BaseModule GetListOfJobsToProcess() {
            SimpleSink jobList;
            jobList = new SimpleSink();
            Dictionary<string, string> queryParameters = new Dictionary<string, string>() { { "Job", "667063,667062,667061,667060,667059" } };
            string queryTemplate = ModuleDiscovery.GetQueryXMLDef("Job_ID_List");


            MSSQLReader reader = new MSSQLReader(queryTemplate, queryParameters);
            ProcessingPipeline pipeline = ProcessingPipeline.Assemble("Get Jobs", reader, jobList);
            ConnectPipelineToStatusDisplay(pipeline);
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
                ConnectPipelineToStatusDisplay(p);
            }
            ConnectPipelineQueueToStatusDisplay(mPipelineQueue);
            mPipelineQueue.RunRoot(null);
        }

        #region Pipeline Utilities

        private void ConnectPipelineToStatusDisplay(ProcessingPipeline pipeline) {
            pipeline.OnStatusMessageUpdated += HandlePipelineUpdate;
            pipeline.OnRunCompleted += HandlePipelineCompletion;
        }

        private void ConnectPipelineQueueToStatusDisplay(PipelineQueue pipelineQueue) {
            pipelineQueue.OnRunCompleted += HandlePipelineQueueCompletion;
            pipelineQueue.OnPipelineStarted += HandlePipelineQueueUpdate;
        }
        #endregion

        #region Pipeline and Queue Update Message Handlers

        private void HandlePipelineUpdate(object sender, MageStatusEventArgs args) {
            Console.WriteLine(args.Message);
        }

        private void HandlePipelineCompletion(object sender, MageStatusEventArgs args) {
            Console.WriteLine(args.Message);
        }

        private void HandlePipelineQueueUpdate(object sender, MageStatusEventArgs args) {
        }

        private void HandlePipelineQueueCompletion(object sender, MageStatusEventArgs args) {
        }

        #endregion



    }
}
