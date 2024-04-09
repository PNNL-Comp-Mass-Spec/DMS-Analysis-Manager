using AnalysisManagerBase;
using PRISM;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;

namespace AnalysisManagerMODPlusPlugin
{
    public class MODPlusRunner
    {
        public const string MOD_PLUS_CONSOLE_OUTPUT_PREFIX = "MODPlus_ConsoleOutput_Part";

        public const string RESULTS_FILE_SUFFIX = "_modp.txt";

        public enum MODPlusRunnerStatusCodes
        {
            NotStarted = 0,
            Running = 1,
            Success = 2,
            Failure = 3
        }

        public event CmdRunnerWaitingEventHandler CmdRunnerWaiting;

        public delegate void CmdRunnerWaitingEventHandler(List<int> processIDs, float coreUsageCurrent, int secondsBetweenUpdates);

        public bool CommandLineArgsLogged { get; set; }

        public string CommandLineArgs { get; private set; }

        public string ConsoleOutputFilePath { get; private set; }

        public int JavaMemorySizeMB
        {
            get => mJavaMemorySizeMB;
            set
            {
                if (value < 500)
                    value = 500;
                mJavaMemorySizeMB = value;
            }
        }

        public string OutputFilePath { get; private set; }

        public string ParameterFilePath { get; }

        public int ProcessID { get; private set; }

        public float CoreUsage { get; private set; }

        /// <summary>
        /// Value between 0 and 100
        /// </summary>
        public double Progress { get; private set; }

        public RunDosProgram ProgramRunner { get; private set; }

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
        /// Program release date, as reported at the console
        /// </summary>
        public string ReleaseDate { get; private set; }

        public MODPlusRunnerStatusCodes Status { get; private set; }

        public int Thread { get; }

        private int mJavaMemorySizeMB;

        private readonly string mDatasetName;

        private readonly string mWorkingDirectory;

        private readonly string mJavaProgLog;

        private readonly string mModPlusJarFilePath;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dataset"></param>
        /// <param name="processingThread"></param>
        /// <param name="workingDirectory"></param>
        /// <param name="paramFilePath"></param>
        /// <param name="javaProgLoc"></param>
        /// <param name="modPlusJarFilePath"></param>
        public MODPlusRunner(string dataset, int processingThread, string workingDirectory, string paramFilePath, string javaProgLoc, string modPlusJarFilePath)
        {
            mDatasetName = dataset;
            Thread = processingThread;
            mWorkingDirectory = workingDirectory;
            ParameterFilePath = paramFilePath;
            mJavaProgLog = javaProgLoc;
            mModPlusJarFilePath = modPlusJarFilePath;

            Status = MODPlusRunnerStatusCodes.NotStarted;
            ReleaseDate = string.Empty;

            ConsoleOutputFilePath = string.Empty;
            OutputFilePath = string.Empty;
            CommandLineArgs = string.Empty;

            CommandLineArgsLogged = false;
            JavaMemorySizeMB = 3000;
        }

        /// <summary>
        /// Forcibly ends MODPlus
        /// </summary>
        public void AbortProcessingNow()
        {
            ProgramRunner?.AbortProgramNow();
        }

        public void StartAnalysis()
        {
            ProgramRunner = new RunDosProgram(mWorkingDirectory);
            ProgramRunner.ErrorEvent += CmdRunner_ErrorEvent;
            ProgramRunner.LoopWaiting += CmdRunner_LoopWaiting;

            Progress = 0;

            ConsoleOutputFilePath = Path.Combine(mWorkingDirectory, MOD_PLUS_CONSOLE_OUTPUT_PREFIX + Thread + ".txt");

            OutputFilePath = Path.Combine(mWorkingDirectory, mDatasetName + "_Part" + Thread + RESULTS_FILE_SUFFIX);

            ProgramRunner.CreateNoWindow = true;
            ProgramRunner.CacheStandardOutput = false;
            ProgramRunner.EchoOutputToConsole = false;

            ProgramRunner.WriteConsoleOutputToFile = true;
            ProgramRunner.ConsoleOutputFilePath = ConsoleOutputFilePath;

            Status = MODPlusRunnerStatusCodes.Running;

            var arguments = " -Xmx" + JavaMemorySizeMB + "M" +
                            " -jar " + Global.PossiblyQuotePath(mModPlusJarFilePath) +
                            " -i " + Global.PossiblyQuotePath(ParameterFilePath) +
                            " -o " + Global.PossiblyQuotePath(OutputFilePath);

            CommandLineArgs = arguments;

            ProcessID = 0;
            CoreUsage = 1;

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var success = ProgramRunner.RunProgram(mJavaProgLog, arguments, "MODPlus", true);

            if (success)
            {
                Status = MODPlusRunnerStatusCodes.Success;
                Progress = 100;
            }
            else
            {
                Status = MODPlusRunnerStatusCodes.Failure;
            }
        }

        // ReSharper disable CommentTypo

        // Example Console output

        // ************************************************************************************
        // Modplus (version pnnl) - Identification of post-translational modifications
        // Release Date: Apr 28, 2015
        // ************************************************************************************
        //
        // Reading parameters.....
        // - Input datasest : E:\DMS_WorkDir2\SBEP_STM_rip_LB6_12Aug13_Frodo_13-04-15.mzXML (MZXML type)
        // - Input database : C:\DMS_Temp_Org\ID_004313_B6EC8119_Excerpt.fasta
        // - Instrument Resolution: High MS / High MS2 (TRAP)
        // - Enzyme : Trypsin [KR/*], [*/] (Miss Cleavages: 2, #Enzymatic Termini: 1)
        // - Variable modifications : 398 specified (Multiple modifications per peptide)
        // - Precursor ion mass tolerance : 20.0 ppm (C13 error of -1 ~ 2)
        // - Fragment ion mass tolerance : 0.05 Dalton
        //
        // Start searching!
        // Reading MS/MS spectra.....  50396 scans
        // Reading protein database.....  615 proteins / 232536 residues (1)
        //
        // MODPlus | 1/50396
        // MODPlus | 2/50396
        // MODPlus | 3/50396
        // ...
        // MODPlus | 50394/50396
        // MODPlus | 50395/50396
        // MODPlus | 50396/50396
        // [MOD-Plus] Elapsed Time : 6461 Sec

        // ReSharper restore CommentTypo

        private readonly Regex reCheckProgress = new(@"^MODPlus[^0-9]+(\d+)/(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the MODPlus console output file to track the search progress
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

                var spectraSearched = 0;
                var totalSpectra = 1;

                using (var reader = new StreamReader(new FileStream(consoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (string.IsNullOrWhiteSpace(dataLine))
                        {
                            continue;
                        }

                        if (dataLine.StartsWith("release date:", StringComparison.OrdinalIgnoreCase))
                        {
                            ReleaseDate = dataLine.Substring(13).TrimStart();
                            continue;
                        }

                        var reMatch = reCheckProgress.Match(dataLine);

                        if (reMatch.Success)
                        {
                            int.TryParse(reMatch.Groups[1].ToString(), out spectraSearched);
                            int.TryParse(reMatch.Groups[2].ToString(), out totalSpectra);
                            continue;
                        }
                    }
                }

                if (totalSpectra < 1)
                    totalSpectra = 1;

                // Value between 0 and 100
                var progressComplete = Math.Round(spectraSearched / (float)totalSpectra * 100);

                if (progressComplete > Progress)
                {
                    Progress = progressComplete;
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
            const int SECONDS_BETWEEN_UPDATE = 30;

            if (DateTime.UtcNow.Subtract(mLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE)
            {
                mLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(ConsoleOutputFilePath);

                // Note that the call to GetCoreUsage() will take at least 1 second
                CoreUsage = ProgramRunner.GetCoreUsage();
                ProcessID = ProgramRunner.ProcessID;
            }

            var processIDs = new List<int> {
                ProcessID
            };

            CmdRunnerWaiting?.Invoke(processIDs, CoreUsage, SECONDS_BETWEEN_UPDATE);
        }
    }
}
