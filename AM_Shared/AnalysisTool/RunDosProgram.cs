using System;
using System.IO;
using PRISM;

//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

namespace AnalysisManagerBase.AnalysisTool
{
    /// <summary>
    /// Provides a looping wrapper around a ProgRunner object for running command-line programs
    /// </summary>
    public class RunDosProgram : EventNotifier
    {
        // Ignore Spelling: prog, usr

        /// <summary>
        /// Monitor interval, in milliseconds
        /// </summary>
        /// <remarks>Values over 10 seconds (10000 milliseconds) will result in a 10 second monitoring interval</remarks>
        private int mMonitorInterval = 2000;

        private string mCachedConsoleErrors = string.Empty;

        private bool mAbortProgramPostLogEntry;

        /// <summary>
        /// Program runner
        /// </summary>
        private ProgRunner mProgRunner;

        private DateTime mStopTime;

        private bool mIsRunning;

        /// <summary>
        /// Class is waiting until next time it's due to check status of called program (good time for external processing)
        /// </summary>
        public event LoopWaitingEventHandler LoopWaiting;

        /// <summary>
        /// Delegate for LoopWaitingEventHandler
        /// </summary>
        public delegate void LoopWaitingEventHandler();

        /// <summary>
        /// Text that was written to the console
        /// </summary>
        public event ConsoleOutputEventHandler ConsoleOutputEvent;

        /// <summary>
        /// Delegate for ConsoleOutputEventHandler
        /// </summary>
        /// <param name="newText">Text written to the console</param>
        public delegate void ConsoleOutputEventHandler(string newText);

        /// <summary>
        /// Program execution exceeded MaxRuntimeSeconds
        /// </summary>
        public event TimeoutEventHandler Timeout;

        /// <summary>
        /// Delegate for TimeoutEventHandler
        /// </summary>
        public delegate void TimeoutEventHandler();

        /// <summary>
        /// Text written to the Error stream by the external program (including carriage returns)
        /// </summary>
        public string CachedConsoleErrors
        {
            get
            {
                if (string.IsNullOrWhiteSpace(mCachedConsoleErrors))
                {
                    return string.Empty;
                }

                return mCachedConsoleErrors;
            }
        }

        /// <summary>
        /// Text written to the Console by the external program (including carriage returns)
        /// </summary>
        public string CachedConsoleOutput => mProgRunner == null ? string.Empty : mProgRunner.CachedConsoleOutput;

        /// <summary>
        /// Any text written to the Error buffer by the external program
        /// </summary>
        public string CachedConsoleError => mProgRunner == null ? string.Empty : mProgRunner.CachedConsoleError;

        /// <summary>
        /// When true, cache the text the external program writes to the console
        /// Can retrieve using the CachedConsoleOutput read-only property
        /// Will also fire event ConsoleOutputEvent as new text is written to the console
        /// </summary>
        /// <remarks>If this is true, no window will be shown, even if CreateNoWindow is false</remarks>
        public bool CacheStandardOutput { get; set; }

        /// <summary>
        /// When true, the program name and command line arguments will be added to the top of the console output file
        /// </summary>
        /// <remarks>Defaults to true</remarks>
        public bool ConsoleOutputFileIncludesCommandLine { get; set; } = true;

        /// <summary>
        /// File path to which the console output will be written if WriteConsoleOutputToFile is true
        /// If blank, file path will be auto-defined in the WorkDir when program execution starts
        /// </summary>
        public string ConsoleOutputFilePath { get; set; } = string.Empty;

        /// <summary>
        /// Determine if window should be displayed
        /// </summary>
        /// <remarks>
        /// Will be forced to true if CacheStandardOutput is true
        /// </remarks>
        public bool CreateNoWindow { get; set; } = true;

        /// <summary>
        /// Debug level for logging
        /// </summary>
        /// <remarks>Higher values mean more log messages</remarks>
        public int DebugLevel { get; set; }

        /// <summary>
        /// When true, echoes, in real time, text written to the Console by the external program
        /// Ignored if CreateNoWindow is false
        /// </summary>
        public bool EchoOutputToConsole { get; set; } = true;

        /// <summary>
        /// Exit code when process completes
        /// </summary>
        public int ExitCode { get; private set; }

        /// <summary>
        /// When true, any messages written to the error stream by the program will be reported via event <see cref="ProgRunner_ConsoleErrorEvent"/>
        /// When false, error messages will be appended to mCachedConsoleErrors, but not reported via an event
        /// </summary>
        public bool RaiseConsoleErrorEvents { get; set; } = true;

        /// <summary>
        /// Maximum amount of time (seconds) that the program will be allowed to run; 0 if allowed to run indefinitely
        /// </summary>
        public int MaxRuntimeSeconds { get; private set; }

        /// <summary>
        /// How often (milliseconds) internal monitoring thread checks status of external program
        /// Minimum allowed value is 250 milliseconds
        /// </summary>
        public int MonitorInterval
        {
            get => mMonitorInterval;
            set
            {
                if (value < 250)
                    value = 250;
                mMonitorInterval = value;
            }
        }

        /// <summary>
        /// ProcessID of an externally spawned process
        /// </summary>
        /// <remarks>0 if no external process running</remarks>
        public int ProcessID => mProgRunner?.PID ?? 0;

        /// <summary>
        /// External program that the ProgRunner is running
        /// This is the full path to the program file
        /// </summary>
        public string ProcessPath
        {
            get
            {
                if (string.IsNullOrWhiteSpace(mProgRunner?.Program))
                    return string.Empty;

                return mProgRunner.Program;
            }
        }

        /// <summary>
        /// Returns true if program was aborted via call to AbortProgramNow()
        /// </summary>
        public bool ProgramAborted { get; private set; }

        /// <summary>
        /// Time that the program runner has been running for (or time that it ran if finished)
        /// </summary>
        public TimeSpan RunTime => StopTime.Subtract(StartTime);

        /// <summary>
        /// Time the program runner started (UTC-based)
        /// </summary>
        public DateTime StartTime { get; private set; }

        /// <summary>
        /// Time the program runner finished (UTC-based)
        /// </summary>
        /// <remarks>Will be the current time-of-day if still running</remarks>
        public DateTime StopTime => mIsRunning ? DateTime.UtcNow : mStopTime;

        /// <summary>
        /// Current monitoring state
        /// </summary>
        public ProgRunner.States State => mProgRunner?.State ?? ProgRunner.States.NotMonitoring;

        /// <summary>
        /// Working directory for process execution
        /// </summary>
        public string WorkDir { get; set; }

        /// <summary>
        /// When true, write the standard output to a file in real-time
        /// Will also fire event ConsoleOutputEvent as new text is written to the console
        /// Define the path to the file using property ConsoleOutputFilePath; if not defined, the file
        /// will be created in the WorkDir (though, if WorkDir is blank, will be created in the directory with the Program we're running)
        /// </summary>
        /// <remarks>
        /// Defaults to false
        /// If this is true, no window will be shown, even if CreateNoWindow is false
        /// </remarks>
        public bool WriteConsoleOutputToFile { get; set; } = false;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="workDir">Work directory for input/output files, if any</param>
        /// <param name="debugLevel">Debug level for logging; 1=minimal logging; 5=detailed logging</param>
        public RunDosProgram(string workDir, int debugLevel = 1)
        {
            WorkDir = workDir;
            DebugLevel = debugLevel;
        }

        /// <summary>
        /// Call this method to instruct this class to terminate the running program
        /// Will post an entry to the log
        /// </summary>
        public void AbortProgramNow()
        {
            AbortProgramNow(postLogEntry: true);
        }

        /// <summary>
        /// Call this method to instruct this class to terminate the running program
        /// </summary>
        /// <param name="postLogEntry">True if an entry should be posted to the log</param>
        public void AbortProgramNow(bool postLogEntry)
        {
            mAbortProgramPostLogEntry = postLogEntry;
            ProgramAborted = true;
        }

        /// <summary>
        /// Number of cores in use by the externally spawned process (0 if no external process running)
        /// </summary>
        /// <remarks>Obtaining this value takes a minimum of 1 second since we sample the performance counters</remarks>
        /// <returns>Number of cores in use; -1 if an error</returns>
        public float GetCoreUsage()
        {
            if (mProgRunner == null)
            {
                return 0;
            }

            try
            {
                if (mProgRunner.PID <= 0)
                {
                    // Unknown process ID
                    return 0;
                }

                var coreUsage = Global.ProcessInfo.GetCoreUsageByProcessID(mProgRunner.PID);

                if (coreUsage < 0)
                {
                    return -1;
                }

                return coreUsage;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in GetCoreUsage", ex);
                return -1;
            }
        }

        private void OnLoopWaiting()
        {
            LoopWaiting?.Invoke();
        }

        private void OnTimeout()
        {
            Timeout?.Invoke();
        }

        /// <summary>
        /// Runs a program and waits for it to exit
        /// </summary>
        /// <remarks>Ignores the result code reported by the program</remarks>
        /// <param name="executablePath">The path to the program to run</param>
        /// <param name="arguments">The arguments to pass to the program, for example /N=35</param>
        /// <param name="progName">The name of the program to use for the Window title</param>
        /// <returns>True if success, false if an error</returns>
        // ReSharper disable once UnusedMember.Global
        public bool RunProgram(string executablePath, string arguments, string progName)
        {
            const bool useResCode = false;
            return RunProgram(executablePath, arguments, progName, useResCode);
        }

        /// <summary>
        /// Runs a program and waits for it to exit
        /// </summary>
        /// <remarks>Ignores the result code reported by the program</remarks>
        /// <param name="executablePath">The path to the program to run</param>
        /// <param name="arguments">The arguments to pass to the program, for example: /N=35</param>
        /// <param name="progName">The name of the program to use for the Window title</param>
        /// <param name="useResCode">When true, use the result code to determine success or failure of program execution</param>
        /// <returns>True if success, false if an error</returns>
        public bool RunProgram(string executablePath, string arguments, string progName, bool useResCode)
        {
            const int maxRuntimeSeconds = 0;
            return RunProgram(executablePath, arguments, progName, useResCode, maxRuntimeSeconds);
        }

        /// <summary>
        /// Runs a program and waits for it to exit
        /// </summary>
        /// <remarks>maxRuntimeSeconds will be increased to 15 seconds if it is between 1 and 14 seconds</remarks>
        /// <param name="executablePath">The path to the program to run</param>
        /// <param name="arguments">The arguments to pass to the program, for example /N=35</param>
        /// <param name="progName">The name of the program to use for the Window title</param>
        /// <param name="useResCode">If true, returns false if the ProgRunner ExitCode is non-zero</param>
        /// <param name="maxRuntimeSeconds">If a positive number, program execution will be aborted if the runtime exceeds maxRuntimeSeconds</param>
        /// <returns>True if success, false if an error</returns>
        public bool RunProgram(string executablePath, string arguments, string progName, bool useResCode, int maxRuntimeSeconds)
        {
            // Require a minimum monitoring interval of 250 milliseconds
            if (mMonitorInterval < 250)
                mMonitorInterval = 250;

            if (maxRuntimeSeconds is > 0 and < 15)
            {
                maxRuntimeSeconds = 15;
            }
            MaxRuntimeSeconds = maxRuntimeSeconds;

            if (executablePath.StartsWith("/") && Path.DirectorySeparatorChar == '\\')
            {
                // Log a warning
                OnWarningEvent("Linux-style path on a Windows machine; program execution may fail: " + executablePath);
            }

            // Re-instantiate mProgRunner each time RunProgram is called since it is disposed of later in this method
            // Also necessary to avoid problems caching the console output
            mProgRunner = new ProgRunner
            {
                Arguments = arguments,
                CreateNoWindow = CreateNoWindow,
                MonitoringInterval = mMonitorInterval,
                Name = progName,
                Program = executablePath,
                Repeat = false,
                RepeatHoldOffTime = 0,
                WorkDir = WorkDir,
                CacheStandardOutput = CacheStandardOutput,
                EchoOutputToConsole = EchoOutputToConsole,
                WriteConsoleOutputToFile = WriteConsoleOutputToFile,
                ConsoleOutputFilePath = ConsoleOutputFilePath,
                ConsoleOutputFileIncludesCommandLine = ConsoleOutputFileIncludesCommandLine
            };

            RegisterEvents(mProgRunner);

            mProgRunner.ConsoleErrorEvent += ProgRunner_ConsoleErrorEvent;
            mProgRunner.ConsoleOutputEvent += ProgRunner_ConsoleOutputEvent;
            mProgRunner.ProgChanged += ProgRunner_ProgChanged;

            if (DebugLevel >= 4)
            {
                OnStatusEvent("  ProgRunner.Arguments = " + mProgRunner.Arguments);
                OnStatusEvent("  ProgRunner.Program = " + mProgRunner.Program);
            }

            mCachedConsoleErrors = string.Empty;

            mAbortProgramPostLogEntry = true;
            ProgramAborted = false;

            var runtimeExceeded = false;
            var abortLogged = false;

            var cachedProcessID = 0;

            try
            {
                // Start the program executing
                mProgRunner.StartAndMonitorProgram();

                StartTime = DateTime.UtcNow;
                mStopTime = DateTime.MinValue;
                mIsRunning = true;

                // Loop until program is complete, or until MaxRuntimeSeconds elapses
                while (mProgRunner.State != ProgRunner.States.NotMonitoring)
                {
                    if (cachedProcessID == 0)
                        cachedProcessID = mProgRunner.PID;

                    OnLoopWaiting();
                    AppUtils.SleepMilliseconds(mMonitorInterval);

                    if (MaxRuntimeSeconds > 0)
                    {
                        if (RunTime.TotalSeconds > MaxRuntimeSeconds && !ProgramAborted)
                        {
                            AbortProgramNow(false);
                            runtimeExceeded = true;
                            OnTimeout();
                        }
                    }

                    if (!ProgramAborted)
                        continue;

                    if (mAbortProgramPostLogEntry && !abortLogged)
                    {
                        abortLogged = true;
                        string msg;

                        if (runtimeExceeded)
                        {
                            msg = "  Aborting ProgRunner for " + progName + " since " + MaxRuntimeSeconds + " seconds has elapsed";
                        }
                        else
                        {
                            msg = "  Aborting ProgRunner for " + progName + " since AbortProgramNow() was called";
                        }

                        OnErrorEvent(msg);
                    }

                    mProgRunner.StopMonitoringProgram(kill: true);
                } // end while

                mStopTime = DateTime.UtcNow;
                mIsRunning = false;

                Global.ProcessInfo.ClearCachedPerformanceCounterForProcessID(cachedProcessID);
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Exception running external program {0}", executablePath), ex);
                mProgRunner = null;

                mStopTime = DateTime.UtcNow;
                mIsRunning = false;

                return false;
            }

            // Cache the exit code in ExitCode
            ExitCode = mProgRunner.ExitCode;
            mProgRunner = null;

            if (useResCode && ExitCode != 0)
            {
                if (ProgramAborted && mAbortProgramPostLogEntry || !ProgramAborted)
                {
                    OnErrorEvent("  ProgRunner.ExitCode = {0} for Program = {1}", ExitCode, executablePath);
                }
                return false;
            }

            return !ProgramAborted;
        }

        /// <summary>
        /// Update the executable path and arguments to run a .NET exe using mono
        /// </summary>
        /// <param name="mgrParams">Manager parameters; used to determine the path to mono</param>
        /// <param name="executablePath">Path to the executable; will be updated to be the path to mono</param>
        /// <param name="arguments">The arguments to pass to the program, for example /N=35; will be updated to start with the executable</param>
        /// <returns>True if success, false if a problem</returns>
        public bool UpdateToUseMono(IMgrParams mgrParams, ref string executablePath, ref string arguments)
        {
            // Manager parameter MonoProgLoc is defined in file ManagerSettingsLocal.xml
            var monoProgLoc = mgrParams.GetParam("MonoProgLoc", string.Empty);

            if (string.IsNullOrWhiteSpace(monoProgLoc))
            {
                if (!File.Exists("/usr/local/bin/mono"))
                {
                    OnErrorEvent("Manager parameter MonoProgLoc not defined; cannot run " + Path.GetFileName(executablePath));
                    return false;
                }

                monoProgLoc = "/usr/local/bin/mono";
                OnWarningEvent("Manager parameter MonoProgLoc not defined, but mono was found at " + monoProgLoc);
            }

            var monoExecutable = new FileInfo(monoProgLoc);

            if (!monoExecutable.Exists)
            {
                OnErrorEvent("Mono not found at {0}; cannot run {1}", monoProgLoc, Path.GetFileName(executablePath));
                return false;
            }

            arguments = Global.PossiblyQuotePath(executablePath) + " " + arguments;
            executablePath = monoProgLoc;

            return true;
        }

        private void ProgRunner_ConsoleErrorEvent(string newText)
        {
            if (RaiseConsoleErrorEvents)
            {
                OnErrorEvent("Console error: " + newText);
            }

            if (string.IsNullOrWhiteSpace(mCachedConsoleErrors))
            {
                mCachedConsoleErrors = newText;
            }
            else
            {
                mCachedConsoleErrors += Environment.NewLine + newText;
            }
        }

        private void ProgRunner_ConsoleOutputEvent(string newText)
        {
            ConsoleOutputEvent?.Invoke(newText);
        }

        private void ProgRunner_ProgChanged(ProgRunner obj)
        {
            // This event is ignored by this class
        }
    }
}
