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

        #region Constructors

        public MageAMPipelineBase(IJobParams jobParms, IMgrParams mgrParms) {
            this.mJobParms = jobParms;
            this.mMgrParms = mgrParms;
            this.mResultsDBFileName = RequireJobParam("ResultsBaseName") + ".db3";
            this.mWorkingDir = RequireMgrParam("workdir");
        }

        #endregion

        #region Utility Methods

        public string RequireMgrParam(string paramName) {
            string val = mMgrParms.GetParam(paramName);
            if (val == "") {
                throw new MageException(string.Format("Required manager parameter '{0}' was missing.", paramName));
            }
            return val;
        }

        public string RequireJobParam(string paramName) {
            string val = mJobParms.GetParam(paramName);
            if (val == "") {
                throw new MageException(string.Format("Required job parameter '{0}' was missing.", paramName));
            }
            return val;
        }

        public string GetJobParam(string paramName) {
            return mJobParms.GetParam(paramName);
        }

        public string GetJobParam(string paramName, string defaultValue) {
            string val = mJobParms.GetParam(paramName);
            if (val == "") val = defaultValue;
            return val;
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
