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

        public enum GlyQIqRunnerStatusCodes
        {
            NotStarted = 0,
            Running = 1,
            Success = 2,
            Failure = 3
        }

        public event CmdRunnerWaitingEventHandler CmdRunnerWaiting;

        public delegate void CmdRunnerWaitingEventHandler();

        public string BatchFilePath { get; }

        public string ConsoleOutputFilePath { get; private set; }

        public int Core { get; }

        /// <summary>
        /// Value between 0 and 100
        /// </summary>
        public double Progress { get; private set; }

        public RunDosProgram ProgramRunner { get; private set; }

        public GlyQIqRunnerStatusCodes Status { get; private set; }

        public ProgRunner.States ProgRunnerStatus
        {
            get
            {
                if (ProgramRunner == null)
                {
                    return ProgRunner.States.NotMonitoring;
                }
                return ProgramRunner.State;
            }
        }

        /// <summary>
        /// Dictionary tracking target names, and true/false for whether the target has been reported as being searched in the GlyQ-IQ Console Output window
        /// </summary>
        private readonly Dictionary<string, bool> mTargets;

        private readonly string mWorkingDirectory;

        public GlyQIqRunner(string workingDirectory, int processingCore, string batchFilePathToUse)
        {
            mWorkingDirectory = workingDirectory;
            Core = processingCore;
            BatchFilePath = batchFilePathToUse;
            Status = GlyQIqRunnerStatusCodes.NotStarted;

            mTargets = new Dictionary<string, bool>();

            CacheTargets();
        }

        /// <summary>
        /// Forcibly ends GlyQ-IQ
        /// </summary>
        public void AbortProcessingNow()
        {
            ProgramRunner?.AbortProgramNow();
        }

        private void CacheTargets()
        {
            var batchFile = new FileInfo(BatchFilePath);

            if (!batchFile.Exists)
            {
                throw new FileNotFoundException("Batch file not found", BatchFilePath);
            }

            try
            {
                var fileContents = string.Empty;

                using (var reader = new StreamReader(new FileStream(batchFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    if (!reader.EndOfStream)
                    {
                        fileContents = reader.ReadLine();
                    }
                }

                if (string.IsNullOrWhiteSpace(fileContents))
                {
                    throw new Exception("Batch file is empty, " + batchFile.Name);
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

                var targetsFile = new FileInfo(Path.Combine(workingParametersDirectory.FullName, targetsFileName));

                if (!targetsFile.Exists)
                {
                    throw new FileNotFoundException("Targets file not found, " + targetsFile.FullName);
                }

                char[] columnDelimiters = { '\t' };
                const int CODE_COLUMN_INDEX = 2;

                using (var reader = new StreamReader(new FileStream(targetsFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    // Read the header line
                    if (!reader.EndOfStream)
                    {
                        var headerLine = reader.ReadLine();

                        if (headerLine == null)
                            throw new Exception("Header line in the targets file is empty, " + targetsFile.Name);

                        var headers = headerLine.Split('\t');

                        if (headers.Length < 3)
                        {
                            throw new Exception("Header line in the targets file does not have enough columns, " + targetsFile.Name);
                        }

                        if (!string.Equals(headers[CODE_COLUMN_INDEX], "Code", StringComparison.OrdinalIgnoreCase))
                        {
                            throw new Exception("The 3rd column in the header line of the targets file is not 'Code', it is '" +
                                                                 headers[2] + "' in " + targetsFile.Name);
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
            ProgramRunner = new RunDosProgram(mWorkingDirectory);
            ProgramRunner.ErrorEvent += CmdRunner_ErrorEvent;
            ProgramRunner.LoopWaiting += CmdRunner_LoopWaiting;

            Progress = 0;

            ConsoleOutputFilePath = Path.Combine(mWorkingDirectory, GLYQ_IQ_CONSOLE_OUTPUT_PREFIX + Core + ".txt");

            ProgramRunner.CreateNoWindow = true;
            ProgramRunner.CacheStandardOutput = false;
            ProgramRunner.EchoOutputToConsole = false;

            ProgramRunner.WriteConsoleOutputToFile = true;
            ProgramRunner.ConsoleOutputFilePath = ConsoleOutputFilePath;

            Status = GlyQIqRunnerStatusCodes.Running;

            var arguments = string.Empty;
            var success = ProgramRunner.RunProgram(BatchFilePath, arguments, "GlyQ-IQ", true);

            if (success)
            {
                Status = GlyQIqRunnerStatusCodes.Success;
                Progress = 100;
            }
            else
            {
                Status = GlyQIqRunnerStatusCodes.Failure;
            }
        }

        // In the Console output, we look for lines like this:
        // Start Workflows... (FragmentedTargetedIQWorkflow) on 3-6-1-0-0

        // The Target Code is listed at the end of those lines, there 3-6-1-0-0
        // That code corresponds to the third column in the Targets file

        private readonly Regex reStartWorkflows = new("^Start Workflows... .+ on (.+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the GlyQ-IQ console output file to track the search progress
        /// </summary>
        /// <param name="consoleOutputFilePath"></param>
        private void ParseConsoleOutputFile(string consoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(consoleOutputFilePath))
                {
                    return;
                }

                var analysisFinished = false;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
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

                if (glyqIqProgress > Progress)
                {
                    Progress = glyqIqProgress;
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR,
                    "Error parsing console output file (" + consoleOutputFilePath + "): " + ex.Message);
            }
        }

        /// <summary>
        /// Event handler for event CmdRunner.ErrorEvent
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex"></param>
        private void CmdRunner_ErrorEvent(string message, Exception ex)
        {
            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.ERROR, message, ex);
        }

        private DateTime mLastConsoleOutputParse = DateTime.MinValue;

        private void CmdRunner_LoopWaiting()
        {
            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= 15)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(ConsoleOutputFilePath);
            }

            CmdRunnerWaiting?.Invoke();
        }
    }
}
