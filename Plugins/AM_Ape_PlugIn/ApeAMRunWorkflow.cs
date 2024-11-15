using System;
using System.IO;
using AnalysisManagerBase;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManager_Ape_PlugIn
{
    internal class ApeAMRunWorkflow : ApeAMBase
    {
        // Ignore Spelling: improv, msgfplus, workflow

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="mgrParams"></param>
        public ApeAMRunWorkflow(IJobParams jobParams, IMgrParams mgrParams) : base(jobParams, mgrParams)
        {
        }

        public bool RunWorkflow()
        {
            var success = true;
            var progressHandler = new Ape.SqlConversionHandler((done, conversionSuccess, _, msg) =>
            {
                if (conversionSuccess)
                    OnStatusEvent(msg);
                else
                    OnWarningEvent(msg);

                if (done)
                {
                    if (conversionSuccess)
                    {
                        OnStatusEvent("Ape successfully ran workflow " + GetJobParam("ApeWorkflowName"));
                        success = true;
                    }
                    else
                    {
                        mErrorMessage = "Error running Ape in ApeAMRunWorkflow";
                        OnErrorEvent(mErrorMessage);
                        success = false;
                    }
                }
            });

            var apeWorkflow = Path.Combine(mWorkingDir, GetJobParam("ApeWorkflowName"));
            var apeDatabase = Path.Combine(mWorkingDir, "Results.db3");

            // Lookup which workflow step groups should be run via APE
            // The names in apeWorkflowStepList apply to the <WorkflowGroup></WorkflowGroup> tag for workflow steps

            // ReSharper disable CommentTypo

            // Example values for job parameter ApeWorkflowStepList
            // iTRAQ:
            //   msgfplus, 4plex, 1pctFDR, default, no_ascore, no_precursor_filter, keep_nonquant
            //   msgfplus, 4plex, 4pctFDR, default, no_ascore, no_precursor_filter, keep_nonquant
            //   msgfplus, 8plex, 1pctFDR, default, no_ascore, no_precursor_filter, keep_nonquant
            // TMT:
            //   msgfplus, 6plex, 1pctFDR, default, no_ascore, no_precursor_filter, keep_nonquant
            //   msgfplus, TMT10Plex, 1pctFDR, default, no_ascore, no_precursor_filter, keep_nonquant

            // ReSharper restore CommentTypo

            var apeWorkflowStepList = GetJobParam("ApeWorkflowStepList");

            if (string.IsNullOrEmpty(apeWorkflowStepList))
            {
                // The job parameter originally was missing the "k" in workflow; try that version instead
                // ReSharper disable once StringLiteralTypo
                apeWorkflowStepList = GetJobParam("ApeWorflowStepList");
            }

            // Check whether we should compact the database
            var apeCompactDatabase = bool.Parse(GetJobParam("ApeCompactDatabase"));

            Ape.SqlServerToSQLite.ProgressChanged += OnProgressChanged;
            Ape.SqlServerToSQLite.StartWorkflow(apeWorkflowStepList, apeWorkflow, apeDatabase, apeDatabase, false, apeCompactDatabase, progressHandler);

            if (!success)
            {
                if (string.IsNullOrEmpty(mErrorMessage))
                    mErrorMessage = "Ape.SqlServerToSQLite.StartWorkflow returned false";
            }
            else
            {
                var analysisType = GetJobParam("AnalysisType", string.Empty);

                if (!string.Equals(analysisType, "improv", StringComparison.OrdinalIgnoreCase))
                {
                    // Add the protein parsimony tables
                    success = StartProteinParsimony(apeDatabase);
                }
            }

            return success;
        }

        private bool StartProteinParsimony(string apeDatabasePath)
        {
            const string SOURCE_TABLE = "T_Row_Metadata";

            bool success;

            try
            {
                var parsimonyRunner = new SetCover.Runner();
                RegisterEvents(parsimonyRunner);

                var apeDatabaseFile = new FileInfo(apeDatabasePath);

                if (apeDatabaseFile.Directory == null)
                    throw new IOException("Error determining the parent directory path for the Ape database");

                // Add the protein parsimony tables
                success = parsimonyRunner.ProcessSQLite(apeDatabaseFile.Directory.FullName, apeDatabaseFile.Name, SOURCE_TABLE);
            }
            catch (Exception ex)
            {
                mErrorMessage = "Error adding the parsimony tables: " + ex.Message;
                success = false;
            }

            return success;
        }
    }
}
