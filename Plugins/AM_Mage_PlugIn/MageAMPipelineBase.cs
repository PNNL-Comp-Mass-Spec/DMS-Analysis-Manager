using System;
using System.Collections.Generic;
using Mage;
using MageExtExtractionFilters;
using AnalysisManagerBase;
using System.IO;
using System.Text.RegularExpressions;
using PRISM;

namespace AnalysisManager_Mage_PlugIn
{

    public class MageAMPipelineBase : clsEventNotifier
    {

        #region Member Variables

        protected readonly Regex mProcessingResults = new Regex(@"Extracting results for job (\d+)", RegexOptions.Compiled);

        protected int mLastProgressJob;

        protected DateTime mLastProgressTime = DateTime.UtcNow;

        #endregion

        #region Properties

        protected string ResultsDBFileName { get; }

        protected string WorkingDirPath { get; }

        protected IJobParams JobParms { get; }

        protected IMgrParams MgrParams { get; }

        /// <summary>
        /// Pipeline queue for running the multiple pipelines that make up the workflows for this module
        /// </summary>
        protected PipelineQueue BasePipelineQueue { get; set; } = new PipelineQueue();

        /// <summary>
        /// Where extracted results will be delivered
        /// </summary>
        protected DestinationType ResultsDestination { get; set; }



        #endregion

        #region Properties

        public string WorkingDir => WorkingDirPath;

        #endregion

        #region Constructors

        public MageAMPipelineBase(IJobParams jobParms, IMgrParams mgrParms)
        {
            JobParms = jobParms;
            MgrParams = mgrParms;
            ResultsDBFileName = RequireJobParam("ResultsBaseName") + ".db3";
            WorkingDirPath = RequireMgrParam("workdir");
        }

        #endregion

        #region Utility Methods

        public string GetResultsDBFilePath()
        {
            var dbFilePath = Path.Combine(WorkingDirPath, ResultsDBFileName);
            return dbFilePath;
        }

        public string RequireMgrParam(string paramName)
        {
            var val = MgrParams.GetParam(paramName);
            if (string.IsNullOrWhiteSpace(val))
            {
                throw new MageException(string.Format("Required manager parameter '{0}' was missing.", paramName));
            }
            return val;
        }

        public string RequireJobParam(string paramName)
        {
            var val = JobParms.GetParam(paramName);
            if (string.IsNullOrWhiteSpace(val))
            {
                throw new MageException(string.Format("Required job parameter '{0}' was missing.", paramName));
            }
            return val;
        }

        public string GetJobParam(string paramName)
        {
            return JobParms.GetParam(paramName);
        }

        public string GetJobParam(string paramName, string defaultValue)
        {
            var val = JobParms.GetParam(paramName);
            if (string.IsNullOrWhiteSpace(val))
                val = defaultValue;
            return val;
        }

        /// <summary>
        /// Create a new MSSQLReader module to do a specific query
        /// </summary>
        /// <param name="sql">Query to use</param>
        /// <returns></returns>
        protected MSSQLReader MakeDBReaderModule(string sql)
        {
            var reader = new MSSQLReader { ConnectionString = RequireMgrParam("ConnectionString"), SQLText = sql };
            return reader;
        }

        /// <summary>
        /// Get a list of items from DMS
        /// </summary>
        /// <param name="sql">Query to use a source of jobs</param>
        /// <returns>A Mage module containing list of jobs</returns>
        public SimpleSink GetListOfDMSItems(string sql)
        {
            var itemList = new SimpleSink();

            var reader = MakeDBReaderModule(sql);

            var pipeline = ProcessingPipeline.Assemble("Get Items", reader, itemList);
            ConnectPipelineToStatusHandlers(pipeline);
            pipeline.RunRoot(null);

            return itemList;
        }

        /// <summary>
        /// Copy the results SQLite database file produced by the previous job step (if it exists)
        /// to the working directory
        /// </summary>
        public void GetPriorResultsToWorkDir()
        {
            var dataPackageFolderPath = Path.Combine(RequireJobParam("transferFolderPath"), RequireJobParam("OutputFolderName"));

            var stepInputFolderName = GetJobParam("StepInputFolderName");
            if (stepInputFolderName != "")
            {
                var priorResultsDBFilePath = Path.Combine(dataPackageFolderPath, stepInputFolderName, ResultsDBFileName);
                if (File.Exists(priorResultsDBFilePath))
                {
                    var workingFilePath = Path.Combine(WorkingDirPath, ResultsDBFileName);
                    OnDebugEvent("Copying results from the previous job step to the working directory; source file: " + workingFilePath);

                    File.Copy(priorResultsDBFilePath, workingFilePath, true);
                }
            }
        }

        #endregion

        #region Pipeline Event Handler Utilities

        protected void ConnectPipelineToStatusHandlers(ProcessingPipeline pipeline)
        {
            pipeline.OnStatusMessageUpdated += HandlePipelineUpdate;
            pipeline.OnRunCompleted += HandlePipelineCompletion;
        }

        protected void ConnectPipelineQueueToStatusHandlers(PipelineQueue pipelineQueue)
        {
            pipelineQueue.OnRunCompleted += HandlePipelineUpdate;
            pipelineQueue.OnPipelineStarted += HandlePipelineCompletion;
        }

        #endregion

        #region Pipeline Update Message Handlers

        private void HandlePipelineUpdate(object sender, MageStatusEventArgs args)
        {
            if (args.Message.StartsWith("preparing to insert tablular data") ||
                args.Message.StartsWith("Starting to insert block of rows") ||
                args.Message.StartsWith("finished inserting block of rows"))
                return;

            var reMatch = mProcessingResults.Match(args.Message);
            if (reMatch.Success)
            {
                int job;
                if (int.TryParse(reMatch.Groups[1].Value, out job))
                {
                    // ReSharper disable once RedundantCheckBeforeAssignment
                    if (mLastProgressJob != job)
                    {
                        mLastProgressJob = job;
                    }
                    else
                    {
                        if (DateTime.UtcNow.Subtract(mLastProgressTime).TotalSeconds < 5)
                            return;
                    }
                    mLastProgressTime = DateTime.UtcNow;
                }
            }

            OnDebugEvent(args.Message);
        }

        private void HandlePipelineCompletion(object sender, MageStatusEventArgs args)
        {
            OnDebugEvent(args.Message);
        }


        #endregion

    }
}
