//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2008, Battelle Memorial Institute
// Created 10/09/2008
//
//*********************************************************************************************************

using System;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using PeptideFileExtractor;
using PRISM;

namespace AnalysisManagerExtractionPlugin
{
    /// <summary>
    /// Perform Peptide extraction from Sequest results
    /// </summary>
    /// <remarks></remarks>
    public class clsPeptideExtractWrapper : EventNotifier
    {
        #region "Event Handlers"

        private void m_ExtractTools_EndTask()
        {
            m_ExtractInProgress = false;
        }

        private DateTime dtLastStatusUpdate = DateTime.MinValue;
        private DateTime dtLastLogTime = DateTime.MinValue;
        private DateTime dtLastConsoleLogTime = DateTime.UtcNow;

        private void m_ExtractTools_CurrentProgress(double fractionDone)
        {
            const int CONSOLE_LOG_INTERVAL_SECONDS = 60;
            const int MIN_STATUS_INTERVAL_SECONDS = 3;
            const int MIN_LOG_INTERVAL_SECONDS = 15;
            const int MAX_LOG_INTERVAL_SECONDS = 300;

            var updateLog = false;

            // We divide the progress by 3 since creation of the FHT and SYN files takes ~33% of the time, while the remainder is spent running PHRP and PeptideProphet
            m_Progress = (float)(100 * fractionDone / 3f);

            if (DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= MIN_STATUS_INTERVAL_SECONDS)
            {
                dtLastStatusUpdate = DateTime.UtcNow;
                m_StatusTools.UpdateAndWrite(m_Progress);
            }

            if (m_DebugLevel > 3 && DateTime.UtcNow.Subtract(dtLastLogTime).TotalSeconds >= MIN_LOG_INTERVAL_SECONDS)
            {
                // Update the log file every 15 seconds if DebugLevel is > 3
                updateLog = true;
            }
            else if (m_DebugLevel >= 1 && DateTime.UtcNow.Subtract(dtLastLogTime).TotalSeconds >= MAX_LOG_INTERVAL_SECONDS)
            {
                // Update the log file every 10 minutes if DebugLevel is >= 1
                updateLog = true;
            }

            if (DateTime.UtcNow.Subtract(dtLastConsoleLogTime).TotalSeconds >= CONSOLE_LOG_INTERVAL_SECONDS)
            {
                // Show progress at the console every 60 seconds
                dtLastConsoleLogTime = DateTime.UtcNow;
                OnDebugEvent(string.Format( "Extraction progress: {0:F2}% complete", m_Progress));
            }

            if (updateLog)
            {
                dtLastLogTime = DateTime.UtcNow;
                OnProgressUpdate("Extraction progress", m_Progress);
            }
        }

        private void m_ExtractTools_CurrentStatus(string taskString)
        {
            // Future use?
        }

        #endregion

        #region "Module variables"

        private readonly short m_DebugLevel;
        private bool m_ExtractInProgress;
        private IPeptideFileExtractor m_ExtractTools;

        private readonly string m_DatasetName;
        private readonly string m_WorkDir;

        /// <summary>
        /// Percent complete, value between 0-100
        /// </summary>
        private float m_Progress;

        private readonly IStatusFile m_StatusTools;

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams">IMgrParams object containing manager settings</param>
        /// <param name="jobParams">IJobParams object containing job parameters</param>
        /// <param name="StatusTools"></param>
        /// <remarks></remarks>
        public clsPeptideExtractWrapper(IMgrParams mgrParams, IJobParams jobParams, ref IStatusFile StatusTools)
        {
            m_DebugLevel = (short)mgrParams.GetParam("debuglevel", 1);
            m_StatusTools = StatusTools;

            m_DatasetName = jobParams.GetParam(clsAnalysisResources.JOB_PARAM_DATASET_NAME);
            m_WorkDir = mgrParams.GetParam("workdir");
        }

        /// <summary>
        /// Performs peptide extraction by calling extractor DLL
        /// </summary>
        /// <returns>CloseOutType indicating success or failure</returns>
        /// <remarks></remarks>
        public CloseOutType PerformExtraction()
        {
            var startParams = new clsPeptideFileExtractor.StartupArguments(m_WorkDir, m_DatasetName)
            {
                ExpandMultiORF = true,
                FilterEFS = false,
                FHTFilterScoreThreshold = 0.1,
                FHTXCorrThreshold = 0.0,
                SynXCorrThreshold = 1.5,
                SynFilterScoreThreshold = 0.1,
                MakeIRRFile = false,
                MakeNLIFile = false            // Not actually used by the extractor, since class PeptideHitEntry has COMPUTE_DISCRIMINANT_SCORE = False in the PeptideFileExtractor project
            };

            // Verify the concatenated _out.txt file exists
            if (!startParams.CatOutFileExists)
            {
                OnErrorEvent("Concatenated Out file not found");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Setup the extractor and start extraction process
            m_ExtractTools = new clsPeptideFileExtractor(startParams);
            m_ExtractTools.EndTask += m_ExtractTools_EndTask;
            m_ExtractTools.CurrentProgress += m_ExtractTools_CurrentProgress;
            m_ExtractTools.CurrentStatus += m_ExtractTools_CurrentStatus;

            m_ExtractInProgress = true;

            try
            {
                // Call the dll
                if (m_DebugLevel >= 1)
                {
                    OnDebugEvent("Beginning peptide extraction");
                }

                m_ExtractTools.ProcessInputFile();

                // Loop until the extraction finishes
                while (m_ExtractInProgress)
                {
                    clsGlobal.IdleLoop(2);
                }

                var synTestResult = TestOutputSynFile();
                if (synTestResult != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    // Error messages were generated by TestOutputSynFile, so just exit
                    return synTestResult;
                }

                // Extraction must have finished successfully, so exit
                if (m_DebugLevel >= 2)
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
                m_ExtractTools = null;

                // Clean up processes
                ProgRunner.GarbageCollectNow();
            }
        }

        private CloseOutType TestOutputSynFile()
        {
            // Verifies an _syn.txt file was created, and that valid data was found (file size > 0 bytes)

            // Test for presence of _syn.txt file
            var workFiles = Directory.GetFiles(m_WorkDir);
            var workFileMatch = string.Empty;

            var reCheckSuffix = new Regex(@"_syn.txt$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            foreach (var workFile in workFiles)
            {
                if (reCheckSuffix.IsMatch(workFile))
                {
                    workFileMatch = workFile;
                    break;
                }
            }

            if (string.IsNullOrWhiteSpace(workFileMatch))
            {
                OnErrorEvent("clsPeptideExtractor.TestOutputSynFile: No _syn.txt file found");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            // Get the _syn.txt file size and verify it's > 0 bytes
            var fiWorkFile = new FileInfo(workFileMatch);
            if (fiWorkFile.Length > 0)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            OnWarningEvent("No data in _syn.txt file");

            return CloseOutType.CLOSEOUT_NO_DATA;
        }

        #endregion
    }
}
