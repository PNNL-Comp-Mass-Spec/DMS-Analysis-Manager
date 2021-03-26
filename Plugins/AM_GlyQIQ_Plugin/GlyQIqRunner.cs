using PRISM;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;

namespace AnalysisManagerGlyQIQPlugin
{
    public class GlyQIqRunner
    {
        // Ignore Spelling: GlyQ, Workflows

        public const string GLYQ_IQ_CONSOLE_OUTPUT_PREFIX = "GlyQ-IQ_ConsoleOutput_Core";

        #region "Enums"

        public enum GlyQIqRunnerStatusCodes
        {
            NotStarted = 0,
            Running = 1,
            Success = 2,
            Failure = 3
        }

        #endregion

        #region "Events"

        public event CmdRunnerWaitingEventHandler CmdRunnerWaiting;

        public delegate void CmdRunnerWaitingEventHandler();

        #endregion

        #region "Properties"

        public string BatchFilePath => mBatchFilePath;

        public string ConsoleOutputFilePath => mConsoleOutputFilePath;

        public int Core => mCore;

        /// <summary>
        /// Value between 0 and 100
        /// </summary>
        public double Progress => mProgress;

        public RunDosProgram ProgramRunner => mCmdRunner;

        public GlyQIqRunnerStatusCodes Status => mStatus;

        public ProgRunner.States ProgRunnerStatus
        {
            get
            {
                if (mCmdRunner == null)
                {
                    return ProgRunner.States.NotMonitoring;
                }
                return mCmdRunner.State;
            }
        }

        #endregion

        #region "Member Variables"

        protected string mBatchFilePath;
        protected string mConsoleOutputFilePath;
        protected int mCore;

        protected double mProgress;

        /// <summary>
        /// Dictionary tracking target names, and True/False for whether the target has been reported as being searched in the GlyQ-IQ Console Output window
        /// </summary>
        protected Dictionary<string, bool> mTargets;

        protected GlyQIqRunnerStatusCodes mStatus;

        protected readonly string mWorkingDirectory;

        protected RunDosProgram mCmdRunner;

        #endregion

        public GlyQIqRunner(string workingDirectory, int processingCore, string batchFilePathToUse)
        {
            mWorkingDirectory = workingDirectory;
            mCore = processingCore;
            mBatchFilePath = batchFilePathToUse;
            mStatus = GlyQIqRunnerStatusCodes.NotStarted;

            mTargets = new Dictionary<string, bool>();

            CacheTargets();
        }

        /// <summary>
        /// Forcibly ends GlyQ-IQ
        /// </summary>
        public void AbortProcessingNow()
        {
            mCmdRunner?.AbortProgramNow();
        }

        protected void CacheTargets()
        {
            var fiBatchFile = new FileInfo(mBatchFilePath);
            if (!fiBatchFile.Exists)
            {
                throw new FileNotFoundException("Batch file not found", mBatchFilePath);
            }

            try
            {
                var fileContents = string.Empty;

                using (var reader = new StreamReader(new FileStream(fiBatchFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    if (!reader.EndOfStream)
                    {
                        fileContents = reader.ReadLine();
                    }
                }

                if (string.IsNullOrWhiteSpace(fileContents))
                {
                    throw new Exception("Batch file is empty, " + fiBatchFile.Name);
                }

                // Replace instances of " " with tab characters
                fileContents = fileContents.Replace("\" \"", "\t");

                // Replace any remaining double quotes with a tab character
                fileContents = fileContents.Replace("\"", "\t");

                var parameterList = fileContents.Split('\t');

                // Remove any empty items
                var parameterListFiltered = (from item in parameterList where !string.IsNullOrWhiteSpace(item) select item).ToList();

                if (parameterListFiltered.Count < 6)
                {
                    throw new Exception("Batch file arguments are not in the correct format");
                }

                var targetsFileName = parameterListFiltered[4];
                var workingParametersFolderPath = parameterListFiltered[6];

                var workingParametersDirectory = new DirectoryInfo(workingParametersFolderPath);
                if (!workingParametersDirectory.Exists)
                {
                    throw new DirectoryNotFoundException("Folder not found, " + workingParametersDirectory.FullName);
                }

                var fiTargetsFile = new FileInfo(Path.Combine(workingParametersDirectory.FullName, targetsFileName));
                if (!fiTargetsFile.Exists)
                {
                    throw new FileNotFoundException("Targets file not found, " + fiTargetsFile.FullName);
                }

                char[] columnDelimiters = { '\t' };
                const int CODE_COLUMN_INDEX = 2;

                using (var reader = new StreamReader(new FileStream(fiTargetsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    // Read the header line
                    if (!reader.EndOfStream)
                    {
                        var headerLine = reader.ReadLine();
                        if (headerLine == null)
                            throw new Exception("Header line in the targets file is empty, " + fiTargetsFile.Name);

                        var headers = headerLine.Split('\t');
                        if (headers.Length < 3)
                        {
                            throw new Exception("Header line in the targets file does not have enough columns, " + fiTargetsFile.Name);
                        }

                        if (!string.Equals(headers[CODE_COLUMN_INDEX], "Code", StringComparison.OrdinalIgnoreCase))
                        {
                            throw new Exception("The 3rd column in the header line of the targets file is not 'Code', it is '" +
                                                                 headers[2] + "' in " + fiTargetsFile.Name);
                        }
                    }

                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        var targetInfoColumns = dataLine.Split(columnDelimiters, 4);

                        if (targetInfoColumns.Length > CODE_COLUMN_INDEX + 1)
                        {
                            var targetName = targetInfoColumns[CODE_COLUMN_INDEX];
                            if (!mTargets.ContainsKey(targetName))
                            {
                                mTargets.Add(targetName, false);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error caching the targets file: " + ex.Message, ex);
            }
        }

        public void StartAnalysis()
        {
            mCmdRunner = new RunDosProgram(mWorkingDirectory);
            mCmdRunner.ErrorEvent += CmdRunner_ErrorEvent;
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mProgress = 0;

            mConsoleOutputFilePath = Path.Combine(mWorkingDirectory, GLYQ_IQ_CONSOLE_OUTPUT_PREFIX + mCore + ".txt");

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = false;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = mConsoleOutputFilePath;

            mStatus = GlyQIqRunnerStatusCodes.Running;

            var arguments = string.Empty;
            var blnSuccess = mCmdRunner.RunProgram(BatchFilePath, arguments, "GlyQ-IQ", true);

            if (blnSuccess)
            {
                mStatus = GlyQIqRunnerStatusCodes.Success;
                mProgress = 100;
            }
            else
            {
                mStatus = GlyQIqRunnerStatusCodes.Failure;
            }
        }

        // In the Console output, we look for lines like this:
        // Start Workflows... (FragmentedTargetedIQWorkflow) on 3-6-1-0-0
        //
        // The Target Code is listed at the end of those lines, there 3-6-1-0-0
        // That code corresponds to the third column in the Targets file
        private readonly Regex reStartWorkflows = new("^Start Workflows... .+ on (.+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the GlyQ-IQ console output file to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(strConsoleOutputFilePath))
                {
                    return;
                }

                var analysisFinished = false;

                using (var reader = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (!string.IsNullOrWhiteSpace(dataLine))
                        {
                            var reMatch = reStartWorkflows.Match(dataLine);

                            if (reMatch.Success)
                            {
                                var targetName = reMatch.Groups[1].Value;

                                if (mTargets.ContainsKey(targetName))
                                {
                                    mTargets[targetName] = true;
                                }
                            }
                            else if (dataLine.StartsWith("Target Analysis Finished"))
                            {
                                analysisFinished = true;
                            }
                        }
                    }
                }

                var targetsProcessed = (from item in mTargets where item.Value select item).Count() - 1;
                if (targetsProcessed < 0)
                    targetsProcessed = 0;

                var glyqIqProgress = Math.Round(targetsProcessed / (float)mTargets.Count * 100);

                if (analysisFinished)
                    glyqIqProgress = 100;

                if (glyqIqProgress > mProgress)
                {
                    mProgress = glyqIqProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                    "Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
            }
        }

        /// <summary>
        /// Event handler for event CmdRunner.ErrorEvent
        /// </summary>
        /// <param name="strMessage"></param>
        /// <param name="ex"></param>
        private void CmdRunner_ErrorEvent(string strMessage, Exception ex)
        {
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, strMessage, ex);
        }

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        private void CmdRunner_LoopWaiting()
        {
            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(mConsoleOutputFilePath);
            }

            CmdRunnerWaiting?.Invoke();
        }
    }
}
