using System;
using System.Collections.Generic;
using Mage;
using MageExtExtractionFilters;
using MageDisplayLib;
using AnalysisManagerBase;
using System.IO;

namespace AnalysisManager_Mage_PlugIn {

    public class MageAMPipelineBase {

        #region Member Variables

        protected string mResultsDBFileName = "";

        protected string mWorkingDir;

        protected IJobParams mJobParms;

        protected IMgrParams mMgrParms;

        /// <summary>
        /// Pipeline queue for running the multiple pipelines that make up the workflows for this module
        /// </summary>
        protected PipelineQueue mPipelineQueue = new PipelineQueue();


        /// <summary>
        /// Where extracted results will be delivered
        /// </summary>
        protected DestinationType mDestination = null;

        #endregion

        #region Properties

        public string WorkingDir {
            get {
                return mWorkingDir;
            }
        }

        #endregion

        #region Constructors

        public MageAMPipelineBase(IJobParams jobParms, IMgrParams mgrParms) {
            this.mJobParms = jobParms;
            this.mMgrParms = mgrParms;
            this.mResultsDBFileName = RequireJobParam("ResultsBaseName") + ".db3";
            this.mWorkingDir = RequireMgrParam("workdir");
        }

        #endregion

        #region Utility Methods

        public string GetSQLiteResultsDBFilePath() {
            string dbFilePath = Path.Combine(mWorkingDir, mResultsDBFileName);
            return dbFilePath;
        }

        public string RequireMgrParam(string paramName) {
            string val = mMgrParms.GetParam(paramName);
            if (string.IsNullOrWhiteSpace(val)) {
                throw new MageException(string.Format("Required manager parameter '{0}' was missing.", paramName));
            }
            return val;
        }

        public string RequireJobParam(string paramName) {
            string val = mJobParms.GetParam(paramName);
			if (string.IsNullOrWhiteSpace(val)) {
                throw new MageException(string.Format("Required job parameter '{0}' was missing.", paramName));
            }
            return val;
        }

        public string GetJobParam(string paramName) {
            return mJobParms.GetParam(paramName);
        }

        public string GetJobParam(string paramName, string defaultValue) {
            string val = mJobParms.GetParam(paramName);
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
            MSSQLReader reader = new MSSQLReader();
            reader.ConnectionString = RequireMgrParam("ConnectionString");
            reader.SQLText = sql;
            return reader;
        }

        /// <summary>
        /// Get a list of items from DMS
        /// </summary>
        /// <param name="sql">Query to use a source of jobs</param>
        /// <returns>A Mage module containing list of jobs</returns>
        public SimpleSink GetListOfDMSItems(string sql) {
            SimpleSink itemList = new SimpleSink();

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
        protected void GetPriorResultsToWorkDir() {
            string dataPackageFolderPath = Path.Combine(RequireJobParam("transferFolderPath"), RequireJobParam("OutputFolderName"));

            string stepInputFolderName = GetJobParam("StepInputFolderName");
            if (stepInputFolderName != "") {
                string priorResultsDBFilePath = Path.Combine(dataPackageFolderPath, stepInputFolderName, mResultsDBFileName);
                if (File.Exists(priorResultsDBFilePath)) {
                    string workingFilePath = Path.Combine(mWorkingDir, mResultsDBFileName);
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
