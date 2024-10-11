using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using Mage;
using MageExtExtractionFilters;
using PRISM;

namespace AnalysisManager_Mage_PlugIn
{
    public class MageAMPipelineBase : EventNotifier
    {
        // Ignore Spelling: Mage, SQL

        private readonly Regex mProcessingResults = new(@"Extracting results for job (\d+)", RegexOptions.Compiled);

        private int mLastProgressJob;

        private DateTime mLastProgressTime = DateTime.UtcNow;

        protected string ResultsDBFileName { get; }

        protected string WorkingDirPath { get; }

        private IJobParams JobParams { get; }

        private IMgrParams MgrParams { get; }

        /// <summary>
        /// Pipeline queue for running the multiple pipelines that make up the workflows for this module
        /// </summary>
        protected PipelineQueue BasePipelineQueue { get; set; } = new();

        /// <summary>
        /// Where extracted results will be delivered
        /// </summary>
        protected DestinationType ResultsDestination { get; set; }

        /// <summary>
        /// Number of jobs to process
        /// </summary>
        /// <remarks>Updated by ExtractFromJobsList in MageAMExtractionPipelines</remarks>
        protected int JobCount { get; set; }

        /// <summary>
        /// Jobs that have been processed
        /// </summary>
        /// <remarks>Updated by HandlePipelineUpdate</remarks>
        private SortedSet<int> JobsProcessed { get; }

        public string WorkingDir => WorkingDirPath;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="mgrParams"></param>
        public MageAMPipelineBase(IJobParams jobParams, IMgrParams mgrParams)
        {
            JobParams = jobParams;
            MgrParams = mgrParams;
            ResultsDBFileName = RequireJobParam("ResultsBaseName") + ".db3";
            WorkingDirPath = RequireMgrParam("WorkDir");

            JobsProcessed = new SortedSet<int>();
        }

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
            var val = JobParams.GetParam(paramName);

            if (string.IsNullOrWhiteSpace(val))
            {
                throw new MageException(string.Format("Required job parameter '{0}' was missing.", paramName));
            }
            return val;
        }

        public string GetJobParam(string paramName)
        {
            return JobParams.GetParam(paramName);
        }

        public string GetJobParam(string paramName, string defaultValue)
        {
            return JobParams.GetJobParameter(paramName, defaultValue);
        }

        /// <summary>
        /// Create a new SQLReader module to do a specific query
        /// </summary>
        /// <param name="sql">Query to use</param>
        protected SQLReader MakeDBReaderModule(string sql)
        {
            var connectionString = RequireMgrParam("ConnectionString");
            var reader = new SQLReader(connectionString)
            {
                SQLText = sql
            };
            return reader;
        }

        /// <summary>
        /// Get a list of items from DMS
        /// </summary>
        /// <param name="sql">Query to use a source of jobs</param>
        /// <param name="jobCountLimit">Optionally set this to a positive value to limit the number of jobs to process (useful when debugging)</param>
        /// <returns>A Mage module containing list of jobs</returns>
        public SimpleSink GetListOfDMSItems(string sql, int jobCountLimit)
        {
            var itemList = new SimpleSink();

            var reader = MakeDBReaderModule(sql);

            var pipeline = ProcessingPipeline.Assemble("Get Items", reader, itemList);
            ConnectPipelineToStatusHandlers(pipeline);
            pipeline.RunRoot(null);

            if (jobCountLimit > 0)
            {
                while (itemList.Rows.Count > jobCountLimit)
                {
                    itemList.Rows.RemoveAt(itemList.Rows.Count - 1);
                }
            }
            return itemList;
        }

        /// <summary>
        /// Copy the results SQLite database file produced by the previous job step (if it exists)
        /// to the working directory
        /// </summary>
        public void GetPriorResultsToWorkDir()
        {
            var dataPackagePath = Path.Combine(RequireJobParam(AnalysisResources.JOB_PARAM_TRANSFER_DIRECTORY_PATH), RequireJobParam(AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME));

            var stepInputFolderName = GetJobParam("StepInputFolderName");

            if (stepInputFolderName != "")
            {
                var priorResultsDBFilePath = Path.Combine(dataPackagePath, stepInputFolderName, ResultsDBFileName);

                if (File.Exists(priorResultsDBFilePath))
                {
                    var workingFilePath = Path.Combine(WorkingDirPath, ResultsDBFileName);
                    OnDebugEvent("Copying results from the previous job step to the working directory; source file: " + workingFilePath);

                    File.Copy(priorResultsDBFilePath, workingFilePath, true);
                }
            }
        }

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

        private void HandlePipelineUpdate(object sender, MageStatusEventArgs args)
        {
            if (args.Message.StartsWith("preparing to insert tabular data") ||
                args.Message.StartsWith("Starting to insert block of rows") ||
                args.Message.StartsWith("finished inserting block of rows"))
            {
                return;
            }

            var reMatch = mProcessingResults.Match(args.Message);

            if (reMatch.Success)
            {
                if (int.TryParse(reMatch.Groups[1].Value, out var job))
                {
                    if (mLastProgressJob != job)
                    {
                        mLastProgressJob = job;

                        if (!JobsProcessed.Contains(job))
                        {
                            JobsProcessed.Add(job);

                            if (JobCount > 0)
                            {
                                var percentComplete = JobsProcessed.Count / (float)JobCount * 100;
                                OnProgressUpdate(args.Message, percentComplete);
                            }
                        }
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
    }
}
