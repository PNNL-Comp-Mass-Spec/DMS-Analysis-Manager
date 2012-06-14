using System;
using Mage;
using MageExtExtractionFilters;
using AnalysisManagerBase;
using System.IO;

namespace AnalysisManager_Mage_PlugIn {

    public class MageAMPipelineBase {

        #region Member Variables

        protected string ResultsDBFileName = "";

        protected string WorkingDirPath;

        protected readonly IJobParams JobParms;

        protected readonly IMgrParams MgrParams;

        /// <summary>
        /// Pipeline queue for running the multiple pipelines that make up the workflows for this module
        /// </summary>
        protected PipelineQueue BasePipelineQueue = new PipelineQueue();


        /// <summary>
        /// Where extracted results will be delivered
        /// </summary>
        protected DestinationType ResultsDestination;

        #endregion

        #region Properties

        public string WorkingDir {
            get {
                return WorkingDirPath;
            }
        }

        #endregion

        #region Constructors

        public MageAMPipelineBase(IJobParams jobParms, IMgrParams mgrParms) {
            JobParms = jobParms;
            MgrParams = mgrParms;
            ResultsDBFileName = RequireJobParam("ResultsBaseName") + ".db3";
            WorkingDirPath = RequireMgrParam("workdir");
        }

        #endregion

        #region Utility Methods

        public string GetResultsDBFilePath() {
            string dbFilePath = Path.Combine(WorkingDirPath, ResultsDBFileName);
            return dbFilePath;
        }

        public string RequireMgrParam(string paramName) {
            string val = MgrParams.GetParam(paramName);
            if (string.IsNullOrWhiteSpace(val)) {
                throw new MageException(string.Format("Required manager parameter '{0}' was missing.", paramName));
            }
            return val;
        }

        public string RequireJobParam(string paramName) {
            string val = JobParms.GetParam(paramName);
			if (string.IsNullOrWhiteSpace(val)) {
                throw new MageException(string.Format("Required job parameter '{0}' was missing.", paramName));
            }
            return val;
        }

        public string GetJobParam(string paramName) {
            return JobParms.GetParam(paramName);
        }

        public string GetJobParam(string paramName, string defaultValue) {
            string val = JobParms.GetParam(paramName);
			if (string.IsNullOrWhiteSpace(val))
				val = defaultValue;
            return val;
        }

        /// <summary>
        /// Create a new MSSQLReader module to do a specific query
        /// </summary>
        /// <param name="sql">Query to use</param>
        /// <returns></returns>
        protected MSSQLReader MakeDBReaderModule(String sql) {
            var reader = new MSSQLReader {ConnectionString = RequireMgrParam("ConnectionString"), SQLText = sql};
            return reader;
        }

        /// <summary>
        /// Get a list of items from DMS
        /// </summary>
        /// <param name="sql">Query to use a source of jobs</param>
        /// <returns>A Mage module containing list of jobs</returns>
        public SimpleSink GetListOfDMSItems(string sql) {
            var itemList = new SimpleSink();

            MSSQLReader reader = MakeDBReaderModule(sql);

            ProcessingPipeline pipeline = ProcessingPipeline.Assemble("Get Items", reader, itemList);
            ConnectPipelineToStatusHandlers(pipeline);
            pipeline.RunRoot(null);

            return itemList;
        }

        /// <summary>
        /// Copy the results SQLite database file produced by the previous job step (if it exists)
        /// to the working directory
        /// </summary>
        public void GetPriorResultsToWorkDir() {
            string dataPackageFolderPath = Path.Combine(RequireJobParam("transferFolderPath"), RequireJobParam("OutputFolderName"));

            string stepInputFolderName = GetJobParam("StepInputFolderName");
            if (stepInputFolderName != "") {
                string priorResultsDBFilePath = Path.Combine(dataPackageFolderPath, stepInputFolderName, ResultsDBFileName);
                if (File.Exists(priorResultsDBFilePath)) {
                    string workingFilePath = Path.Combine(WorkingDirPath, ResultsDBFileName);
                    File.Copy(priorResultsDBFilePath, workingFilePath, true);
                }
            }
        }

        #endregion

        #region Pipeline Event Handler Utilities

        protected void ConnectPipelineToStatusHandlers(ProcessingPipeline pipeline) {
            pipeline.OnStatusMessageUpdated += HandlePipelineUpdate;
            pipeline.OnRunCompleted += HandlePipelineCompletion;
        }

        protected void ConnectPipelineQueueToStatusHandlers(PipelineQueue pipelineQueue) {
            pipelineQueue.OnRunCompleted += HandlePipelineUpdate;
            pipelineQueue.OnPipelineStarted += HandlePipelineCompletion;
        }

        #endregion

        #region Pipeline Update Message Handlers

        private void HandlePipelineUpdate(object sender, MageStatusEventArgs args) {
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, args.Message);
        }

        private void HandlePipelineCompletion(object sender, MageStatusEventArgs args) {
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, args.Message);
        }


        #endregion

    }
}
