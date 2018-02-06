using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using AnalysisManagerBase;
using PRISM;
using PRISM.Logging;

namespace AnalysisManagerMODPlusPlugin
{
    public class clsMODPlusRunner
    {
        public const string MOD_PLUS_CONSOLE_OUTPUT_PREFIX = "MODPlus_ConsoleOutput_Part";

        public const string RESULTS_FILE_SUFFIX = "_modp.txt";

        #region "Enums"

        public enum MODPlusRunnerStatusCodes
        {
            NotStarted = 0,
            Running = 1,
            Success = 2,
            Failure = 3
        }

        #endregion

        #region "Events"

        public event CmdRunnerWaitingEventHandler CmdRunnerWaiting;

        public delegate void CmdRunnerWaitingEventHandler(List<int> processIDs, float coreUsageCurrent, int secondsBetweenUpdates);

        #endregion

        #region "Properties"

        public bool CommandLineArgsLogged { get; set; }

        public string CommandLineArgs
        {
            get { return mCommandLineArgs; }
        }

        public string ConsoleOutputFilePath
        {
            get { return mConsoleOutputFilePath; }
        }

        public int JavaMemorySizeMB
        {
            get { return mJavaMemorySizeMB; }
            set
            {
                if (value < 500)
                    value = 500;
                mJavaMemorySizeMB = value;
            }
        }

        public string OutputFilePath
        {
            get { return mOutputFilePath; }
        }

        public string ParameterFilePath
        {
            get { return mParameterFilePath; }
        }

        public int ProcessID
        {
            get { return mProcessID; }
        }

        public float CoreUsage
        {
            get { return mCoreUsageCurrent; }
        }

        /// <summary>
        /// Value between 0 and 100
        /// </summary>
        /// <remarks></remarks>
        public double Progress
        {
            get { return mProgress; }
        }

        public clsRunDosProgram ProgRunner
        {
            get { return mCmdRunner; }
        }

        public clsProgRunner.States ProgRunnerStatus
        {
            get
            {
                if (mCmdRunner == null)
                {
                    return clsProgRunner.States.NotMonitoring;
                }
                return mCmdRunner.State;
            }
        }

        /// <summary>
        /// Program release date, as reported at the console
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string ReleaseDate
        {
            get { return mReleaseDate; }
        }

        public MODPlusRunnerStatusCodes Status
        {
            get { return mStatus; }
        }

        public int Thread
        {
            get { return mThread; }
        }

        #endregion

        #region "Member Variables"

        private string mConsoleOutputFilePath;
        private string mOutputFilePath;
        private string mCommandLineArgs;

        private double mProgress;
        private int mJavaMemorySizeMB;

        private MODPlusRunnerStatusCodes mStatus;
        private string mReleaseDate;

        private readonly string mDataset;
        private readonly int mThread;
        private readonly string mWorkingDirectory;
        private readonly string mParameterFilePath;
        private readonly string mJavaProgLog;
        private readonly string mModPlusJarFilePath;

        private int mProcessID;
        private float mCoreUsageCurrent;

        private clsRunDosProgram mCmdRunner;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dataset"></param>
        /// <param name="processingThread"></param>
        /// <param name="workingDirectory"></param>
        /// <param name="paramFilePath"></param>
        /// <param name="javaProgLoc"></param>
        /// <param name="modPlusJarFilePath"></param>
        /// <remarks></remarks>
        public clsMODPlusRunner(string dataset, int processingThread, string workingDirectory, string paramFilePath, string javaProgLoc, string modPlusJarFilePath)
        {
            mDataset = dataset;
            mThread = processingThread;
            mWorkingDirectory = workingDirectory;
            mParameterFilePath = paramFilePath;
            mJavaProgLog = javaProgLoc;
            mModPlusJarFilePath = modPlusJarFilePath;

            mStatus = MODPlusRunnerStatusCodes.NotStarted;
            mReleaseDate = string.Empty;

            mConsoleOutputFilePath = string.Empty;
            mOutputFilePath = string.Empty;
            mCommandLineArgs = string.Empty;

            CommandLineArgsLogged = false;
            JavaMemorySizeMB = 3000;
        }

        /// <summary>
        /// Forcibly ends MODPlus
        /// </summary>
        /// <remarks></remarks>
        public void AbortProcessingNow()
        {
            if ((mCmdRunner != null))
            {
                mCmdRunner.AbortProgramNow();
            }
        }

        public void StartAnalysis()
        {
            mCmdRunner = new clsRunDosProgram(mWorkingDirectory);
            mCmdRunner.ErrorEvent += CmdRunner_ErrorEvent;
            mCmdRunner.LoopWaiting += CmdRunner_LoopWaiting;

            mProgress = 0;

            mConsoleOutputFilePath = Path.Combine(mWorkingDirectory, MOD_PLUS_CONSOLE_OUTPUT_PREFIX + mThread + ".txt");

            mOutputFilePath = Path.Combine(mWorkingDirectory, mDataset + "_Part" + mThread + RESULTS_FILE_SUFFIX);

            mCmdRunner.CreateNoWindow = true;
            mCmdRunner.CacheStandardOutput = false;
            mCmdRunner.EchoOutputToConsole = false;

            mCmdRunner.WriteConsoleOutputToFile = true;
            mCmdRunner.ConsoleOutputFilePath = mConsoleOutputFilePath;

            mStatus = MODPlusRunnerStatusCodes.Running;

            var cmdStr = string.Empty;

            cmdStr += " -Xmx" + JavaMemorySizeMB + "M";
            cmdStr += " -jar " + clsGlobal.PossiblyQuotePath(mModPlusJarFilePath);
            cmdStr += " -i " + clsGlobal.PossiblyQuotePath(mParameterFilePath);
            cmdStr += " -o " + clsGlobal.PossiblyQuotePath(mOutputFilePath);

            mCommandLineArgs = cmdStr;

            mProcessID = 0;
            mCoreUsageCurrent = 1;

            // Start the program and wait for it to finish
            // However, while it's running, LoopWaiting will get called via events
            var blnSuccess = mCmdRunner.RunProgram(mJavaProgLog, cmdStr, "MODPlus", true);

            if (blnSuccess)
            {
                mStatus = MODPlusRunnerStatusCodes.Success;
                mProgress = 100;
            }
            else
            {
                mStatus = MODPlusRunnerStatusCodes.Failure;
            }
        }

        // Example Console output
        //
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
        //
        private Regex reCheckProgress = new Regex(@"^MODPlus[^0-9]+(\d+)/(\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Parse the MODPlus console output file to track the search progress
        /// </summary>
        /// <param name="strConsoleOutputFilePath"></param>
        /// <remarks></remarks>
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            try
            {
                if (!File.Exists(strConsoleOutputFilePath))
                {
                    return;
                }

                var spectraSearched = 0;
                var totalSpectra = 1;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                        {
                            continue;
                        }

                        if (strLineIn.ToLower().StartsWith("release date:"))
                        {
                            mReleaseDate = strLineIn.Substring(13).TrimStart();
                            continue;
                        }

                        var reMatch = reCheckProgress.Match(strLineIn);
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

                if (progressComplete > mProgress)
                {
                    mProgress = progressComplete;
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

        private DateTime dtLastConsoleOutputParse = DateTime.MinValue;

        private void CmdRunner_LoopWaiting()
        {
            const int SECONDS_BETWEEN_UPDATE = 30;

            if (DateTime.UtcNow.Subtract(dtLastConsoleOutputParse).TotalSeconds >= SECONDS_BETWEEN_UPDATE)
            {
                dtLastConsoleOutputParse = DateTime.UtcNow;

                ParseConsoleOutputFile(mConsoleOutputFilePath);

                // Note that the call to GetCoreUsage() will take at least 1 second
                mCoreUsageCurrent = ProgRunner.GetCoreUsage();
                mProcessID = ProgRunner.ProcessID;
            }

            var processIDs = new List<int>();
            processIDs.Add(mProcessID);

            if (CmdRunnerWaiting != null)
            {
                CmdRunnerWaiting(processIDs, mCoreUsageCurrent, SECONDS_BETWEEN_UPDATE);
            }
        }
    }
}
