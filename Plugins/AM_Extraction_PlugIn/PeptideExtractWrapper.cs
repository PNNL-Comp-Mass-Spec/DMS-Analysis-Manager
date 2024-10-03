//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2008, Battelle Memorial Institute
// Created 10/09/2008
//
//*********************************************************************************************************

using AnalysisManagerBase;
using PRISM;
using System;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;
using SequestResultsProcessor;

namespace AnalysisManagerExtractionPlugin
{
    /// <summary>
    /// Perform Peptide extraction from SEQUEST results
    /// </summary>
    public class PeptideExtractWrapper : EventNotifier
    {
        private void ExtractTools_EndTask()
        {
            mExtractInProgress = false;
        }

        private DateTime mLastStatusUpdate = DateTime.MinValue;
        private DateTime mLastLogTime = DateTime.MinValue;
        private DateTime mLastConsoleLogTime = DateTime.UtcNow;

        [Obsolete("No longer used")]
        private void ExtractTools_CurrentProgress(double fractionDone)
        {
            const int CONSOLE_LOG_INTERVAL_SECONDS = 60;
            const int MIN_STATUS_INTERVAL_SECONDS = 3;
            const int MIN_LOG_INTERVAL_SECONDS = 15;
            const int MAX_LOG_INTERVAL_SECONDS = 300;

            var updateLog = false;

            // We divide the progress by 3 since creation of the FHT and SYN files takes ~33% of the time, while the remainder is spent running PHRP and PeptideProphet
            mProgress = (float)(100 * fractionDone / 3f);

            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds >= MIN_STATUS_INTERVAL_SECONDS)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                mStatusTools.UpdateAndWrite(mProgress);
            }

            if (mDebugLevel > 3 && DateTime.UtcNow.Subtract(mLastLogTime).TotalSeconds >= MIN_LOG_INTERVAL_SECONDS)
            {
                // Update the log file every 15 seconds if DebugLevel is > 3
                updateLog = true;
            }
            else if (mDebugLevel >= 1 && DateTime.UtcNow.Subtract(mLastLogTime).TotalSeconds >= MAX_LOG_INTERVAL_SECONDS)
            {
                // Update the log file every 10 minutes if DebugLevel is >= 1
                updateLog = true;
            }

            if (DateTime.UtcNow.Subtract(mLastConsoleLogTime).TotalSeconds >= CONSOLE_LOG_INTERVAL_SECONDS)
            {
                // Show progress at the console every 60 seconds
                mLastConsoleLogTime = DateTime.UtcNow;
                OnDebugEvent("Extraction progress: {0:F2}% complete", mProgress);
            }

            if (updateLog)
            {
                mLastLogTime = DateTime.UtcNow;
                OnProgressUpdate("Extraction progress", mProgress);
            }
        }

        private readonly short mDebugLevel;
        private bool mExtractInProgress;
        private SequestFileExtractor mExtractTools;

        private readonly string mDatasetName;
        private readonly string mWorkDir;

        /// <summary>
        /// Percent complete, value between 0-100
        /// </summary>
        private float mProgress;

        private readonly IStatusFile mStatusTools;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">IMgrParams object containing manager settings</param>
        /// <param name="jobParams">IJobParams object containing job parameters</param>
        /// <param name="StatusTools">Status tools instance</param>
        public PeptideExtractWrapper(IMgrParams mgrParams, IJobParams jobParams, ref IStatusFile StatusTools)
        {
            mDebugLevel = (short)mgrParams.GetParam("DebugLevel", 1);
            mStatusTools = StatusTools;

            mDatasetName = AnalysisResources.GetDatasetName(jobParams);
            mWorkDir = mgrParams.GetParam("WorkDir");
        }

        /// <summary>
        /// Performs peptide extraction by calling extractor DLL
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        public CloseOutType PerformExtraction()
        {
            var startParams = new SequestFileExtractor.StartupArguments(mWorkDir, mDatasetName)
            {
                ExpandMultiORF = true,
                FHTXCorrThreshold = 0.0,
                SynXCorrThreshold = 1.5,
                MakeIRRFile = false
            };

            // Verify the concatenated _out.txt file exists
            if (!startParams.CatOutFileExists)
            {
                OnErrorEvent("Concatenated Out file not found");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Configure the extractor and start extraction process
            mExtractTools = new SequestFileExtractor(startParams);
            mExtractTools.EndTask += ExtractTools_EndTask;

            mExtractInProgress = true;

            try
            {
                // Call the DLL
                if (mDebugLevel >= 1)
                {
                    OnDebugEvent("Beginning peptide extraction");
                }

                mExtractTools.ProcessInputFile();

                // Loop until the extraction finishes
                while (mExtractInProgress)
                {
                    Global.IdleLoop(2);
                }

                var synTestResult = TestOutputSynFile();

                if (synTestResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Error messages were generated by TestOutputSynFile, so just exit
                    return synTestResult;
                }

                // Extraction must have finished successfully, so exit
                if (mDebugLevel >= 2)
                {
                    OnDebugEvent("Extraction complete");
                }
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception while extracting files: " + ex.Message, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
            finally
            {
                // Make sure no stray objects are hanging around
                mExtractTools = null;

                // Clean up processes
                AppUtils.GarbageCollectNow();
            }
        }

        private CloseOutType TestOutputSynFile()
        {
            // Verifies that the _syn.txt file was created, and that valid data is present (file size > 0 bytes)

            // Test for presence of _syn.txt file
            var workFiles = Directory.GetFiles(mWorkDir);
            var workFileMatch = string.Empty;

            var suffixChecker = new Regex("_syn.txt$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            foreach (var candidateWorkFile in workFiles)
            {
                if (suffixChecker.IsMatch(candidateWorkFile))
                {
                    workFileMatch = candidateWorkFile;
                    break;
                }
            }

            if (string.IsNullOrWhiteSpace(workFileMatch))
            {
                OnErrorEvent("PeptideExtractor.TestOutputSynFile: No _syn.txt file found");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Get the _syn.txt file size and verify it's > 0 bytes
            var workFile = new FileInfo(workFileMatch);

            if (workFile.Length > 0)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            OnWarningEvent("No data in _syn.txt file");

            return CloseOutType.CLOSEOUT_NO_DATA;
        }
    }
}
