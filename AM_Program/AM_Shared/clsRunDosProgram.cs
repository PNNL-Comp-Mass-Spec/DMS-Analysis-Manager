using System;
using System.Threading;
using PRISM.Processes;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2006, Battelle Memorial Institute
// Created 06/07/2006
//
//*********************************************************************************************************

namespace AnalysisManagerBase
{

    /// <summary>
    /// Provides a looping wrapper around a ProgRunner object for running command-line programs
    /// </summary>
    public class clsRunDosProgram : clsEventNotifier
    {

        #region "Module variables"

        // msec
        private int m_MonitorInterval = 2000;

        private int m_MaxRuntimeSeconds;

        private int m_ExitCode;

        private string m_CachedConsoleErrors = string.Empty;
        private bool m_AbortProgramNow;

        private bool m_AbortProgramPostLogEntry;

        // Runs specified program
        private clsProgRunner m_ProgRunner;

        #endregion

        #region "Events"

        /// <summary>
        /// Class is waiting until next time it's due to check status of called program (good time for external processing)
        /// </summary>
        /// <remarks></remarks>
        public event LoopWaitingEventHandler LoopWaiting;

        public delegate void LoopWaitingEventHandler();

        /// <summary>
        /// Text was written to the console
        /// </summary>
        /// <remarks></remarks>
        public event ConsoleOutputEventEventHandler ConsoleOutputEvent;

        public delegate void ConsoleOutputEventEventHandler(string newText);

        /// <summary>
        /// Program execution exceeded MaxRuntimeSeconds
        /// </summary>
        /// <remarks></remarks>
        public event TimeoutEventHandler Timeout;

        public delegate void TimeoutEventHandler();

        #endregion

        #region "Properties"

        /// <summary>
        /// Text written to the Error stream by the external program (including carriage returns)
        /// </summary>
        public string CachedConsoleErrors
        {
            get
            {
                if (string.IsNullOrWhiteSpace(m_CachedConsoleErrors))
                {
                    return string.Empty;
                }

                return m_CachedConsoleErrors;
            }
        }

        /// <summary>
        /// Text written to the Console by the external program (including carriage returns)
        /// </summary>
        public string CachedConsoleOutput
        {
            get
            {
                if (m_ProgRunner == null)
                {
                    return string.Empty;
                }

                return m_ProgRunner.CachedConsoleOutput;
            }
        }

        /// <summary>
        /// Any text written to the Error buffer by the external program
        /// </summary>
        public string CachedConsoleError
        {
            get
            {
                if (m_ProgRunner == null)
                {
                    return string.Empty;
                }

                return m_ProgRunner.CachedConsoleError;
            }
        }

        /// <summary>
        /// When true then will cache the text the external program writes to the console
        /// Can retrieve using the CachedConsoleOutput readonly property
        /// Will also fire event ConsoleOutputEvent as new text is written to the console
        /// </summary>
        /// <remarks>If this is true, then no window will be shown, even if CreateNoWindow=False</remarks>
        public bool CacheStandardOutput { get; set; } = false;

        /// <summary>
        /// When true, the program name and command line arguments will be added to the top of the console output file
        /// </summary>
        /// <remarks>Defaults to true</remarks>
        public bool ConsoleOutputFileIncludesCommandLine { get; set; } = true;

        /// <summary>
        /// File path to which the console output will be written if WriteConsoleOutputToFile is true
        /// If blank, then file path will be auto-defined in the WorkDir  when program execution starts
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public string ConsoleOutputFilePath { get; set; } = string.Empty;

        /// <summary>
        /// Determine if window should be displayed.
        /// Will be forced to True if CacheStandardOutput = True
        /// </summary>
        public bool CreateNoWindow { get; set; } = true;

        /// <summary>
        /// Debug level for logging
        /// </summary>
        public int DebugLevel { get; set; } = 0;

        /// <summary>
        /// When true, then echoes, in real time, text written to the Console by the external program 
        /// Ignored if CreateNoWindow = False
        /// </summary>
        public bool EchoOutputToConsole { get; set; } = true;

        /// <summary>
        /// Exit code when process completes.
        /// </summary>
        public int ExitCode => m_ExitCode;

        /// <summary>
        /// Maximum amount of time (seconds) that the program will be allowed to run; 0 if allowed to run indefinitely
        /// </summary>
        /// <value></value>
        public int MaxRuntimeSeconds => m_MaxRuntimeSeconds;

        /// <summary>
        /// How often (milliseconds) internal monitoring thread checks status of external program
        /// Minimum allowed value is 250 milliseconds
        /// </summary>
        public int MonitorInterval
        {
            get { return m_MonitorInterval; }
            set
            {
                if (value < 250)
                    value = 250;
                m_MonitorInterval = value;
            }
        }

        /// <summary>
        /// ProcessID of an externally spawned process
        /// </summary>
        /// <remarks>0 if no external process running</remarks>
        public int ProcessID
        {
            get
            {
                if (m_ProgRunner == null)
                {
                    return 0;
                }

                return m_ProgRunner.PID;
            }
        }

        /// <summary>
        /// Returns true if program was aborted via call to AbortProgramNow()
        /// </summary>
        public bool ProgramAborted => m_AbortProgramNow;

        /// <summary>
        /// Current monitoring state
        /// </summary>
        public clsProgRunner.States State
        {
            get
            {
                if (m_ProgRunner == null)
                {
                    return clsProgRunner.States.NotMonitoring;
                }

                return m_ProgRunner.State;
            }
        }

        /// <summary>
        /// Working directory for process execution.
        /// </summary>
        public string WorkDir { get; set; }

        /// <summary>
        /// When true then will write the standard output to a file in real-time
        /// Will also fire event ConsoleOutputEvent as new text is written to the console
        /// Define the path to the file using property ConsoleOutputFilePath; if not defined, the file
        /// will be created in the WorkDir (though, if WorkDir is blank, then will be created in the folder with the Program we're running)
        /// </summary>
        /// <remarks>
        /// Defaults to false
        /// If this is true, no window will be shown, even if CreateNoWindow=False
        /// </remarks>
        public bool WriteConsoleOutputToFile { get; set; } = false;

        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="WorkDir">Workdirectory for input/output files, if any</param>
        /// <remarks></remarks>
        public clsRunDosProgram(string WorkDir)
        {
            this.WorkDir = WorkDir;
        }

        /// <summary>
        /// Call this function to instruct this class to terminate the running program
        /// Will post an entry to the log
        /// </summary>
        public void AbortProgramNow()
        {
            AbortProgramNow(blnPostLogEntry: true);
        }

        /// <summary>
        /// Call this function to instruct this class to terminate the running program
        /// </summary>
        /// <param name="blnPostLogEntry">True if an entry should be posted to the log</param>
        /// <remarks></remarks>
        public void AbortProgramNow(bool blnPostLogEntry)
        {
            m_AbortProgramNow = true;
            m_AbortProgramPostLogEntry = blnPostLogEntry;
        }

        /// <summary>
        /// Number of cores in use by the externally spawned process (0 if no external process running)
        /// </summary>
        /// <returns>Number of cores in use; -1 if an error</returns>
        /// <remarks>Obtaining this value takes a minimum of 1000 msec as we sample the performance counters</remarks>
        public float GetCoreUsage()
        {
            if (m_ProgRunner == null)
            {
                return 0;
            }

            try
            {
                var currentCoreUsage = m_ProgRunner.GetCoreUsage();
                if (currentCoreUsage < 0)
                {
                    return -1;
                }

                return currentCoreUsage;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception in GetCoreUsage", ex);
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
        /// <param name="progNameLoc">The path to the program to run</param>
        /// <param name="cmdLine">The arguments to pass to the program, for example /N=35</param>
        /// <param name="progName">The name of the program to use for the Window title</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>Ignores the result code reported by the program</remarks>
        public bool RunProgram(string progNameLoc, string cmdLine, string progName)
        {
            const bool useResCode = false;
            return RunProgram(progNameLoc, cmdLine, progName, useResCode);
        }

        public bool RunProgram(string progNameLoc, string cmdLine, string progName, bool useResCode)
        {
            const int maxRuntime = 0;
            return RunProgram(progNameLoc, cmdLine, progName, useResCode, maxRuntime);
        }

        /// <summary>
        /// Runs a program and waits for it to exit
        /// </summary>
        /// <param name="progNameLoc">The path to the program to run</param>
        /// <param name="cmdLine">The arguments to pass to the program, for example /N=35</param>
        /// <param name="progName">The name of the program to use for the Window title</param>
        /// <param name="useResCode">If true, then returns False if the ProgRunner ExitCode is non-zero</param>
        /// <param name="maxRuntime">If a positive number, then program execution will be aborted if the runtime exceeds maxRuntime seconds</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks>maxRuntime will be increased to 15 seconds if it is between 1 and 14 seconds</remarks>
        public bool RunProgram(string progNameLoc, string cmdLine, string progName, bool useResCode, int maxRuntime)
        {
            // Require a minimum monitoring interval of 250 mseconds
            if (m_MonitorInterval < 250)
                m_MonitorInterval = 250;

            if (maxRuntime > 0 && maxRuntime < 15)
            {
                maxRuntime = 15;
            }
            m_MaxRuntimeSeconds = maxRuntime;

            // Re-instantiate m_ProgRunner each time RunProgram is called since it is disposed of later in this function
            // Also necessary to avoid problems caching the console output
            m_ProgRunner = new clsProgRunner
            {
                Arguments = cmdLine,
                CreateNoWindow = CreateNoWindow,
                MonitoringInterval = m_MonitorInterval,
                Name = progName,
                Program = progNameLoc,
                Repeat = false,
                RepeatHoldOffTime = 0,
                WorkDir = WorkDir,
                CacheStandardOutput = CacheStandardOutput,
                EchoOutputToConsole = EchoOutputToConsole,
                WriteConsoleOutputToFile = WriteConsoleOutputToFile,
                ConsoleOutputFilePath = ConsoleOutputFilePath,
                ConsoleOutputFileIncludesCommandLine = ConsoleOutputFileIncludesCommandLine
            };

            m_ProgRunner.ConsoleErrorEvent += ProgRunner_ConsoleErrorEvent;
            m_ProgRunner.ConsoleOutputEvent += ProgRunner_ConsoleOutputEvent;
            m_ProgRunner.ProgChanged += ProgRunner_ProgChanged;

            if (DebugLevel >= 4)
            {
                OnStatusEvent("  ProgRunner.Arguments = " + m_ProgRunner.Arguments);
                OnStatusEvent("  ProgRunner.Program = " + m_ProgRunner.Program);
            }

            m_CachedConsoleErrors = string.Empty;

            m_AbortProgramNow = false;
            m_AbortProgramPostLogEntry = true;

            var blnRuntimeExceeded = false;
            var blnAbortLogged = false;
            var dtStartTime = DateTime.UtcNow;

            var cachedProcessID = 0;

            try
            {
                // Start the program executing
                m_ProgRunner.StartAndMonitorProgram();

                // Loop until program is complete, or until m_MaxRuntimeSeconds seconds elapses
                while (m_ProgRunner.State != clsProgRunner.States.NotMonitoring)
                {
                    if (cachedProcessID == 0)
                        cachedProcessID = m_ProgRunner.PID;

                    OnLoopWaiting();
                    Thread.Sleep(m_MonitorInterval);

                    if (m_MaxRuntimeSeconds > 0)
                    {
                        if (DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds > m_MaxRuntimeSeconds && !m_AbortProgramNow)
                        {
                            m_AbortProgramNow = true;
                            blnRuntimeExceeded = true;
                            OnTimeout();
                        }

                        if (m_ProgRunner.State == clsProgRunner.States.StartingProcess &&
                            DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds > 30 &&
                            DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds < 90)
                        {
                            // It has taken over 30 seconds for the thread to start
                            // Try re-joining
                            m_ProgRunner.JoinThreadNow();
                        }

                        if (m_AbortProgramNow)
                        {
                            if (m_AbortProgramPostLogEntry && !blnAbortLogged)
                            {
                                blnAbortLogged = true;
                                string msg;
                                if (blnRuntimeExceeded)
                                {
                                    msg = "  Aborting ProgRunner for " + progName + " since " + m_MaxRuntimeSeconds + " seconds has elapsed";
                                }
                                else
                                {
                                    msg = "  Aborting ProgRunner for " + progName + " since AbortProgramNow() was called";
                                }

                                OnErrorEvent(msg);
                            }
                            m_ProgRunner.StopMonitoringProgram(Kill: true);
                        }
                    }

                    clsProgRunner.ClearCachedPerformanceCounterForProcessID(cachedProcessID);

                }
            }
            catch (Exception ex)
            {
                var msg = "Exception running DOS program " + progNameLoc;
                OnErrorEvent(msg, ex);
                m_ProgRunner = null;
                return false;
            }

            // Cache the exit code in m_ExitCode
            m_ExitCode = m_ProgRunner.ExitCode;
            m_ProgRunner = null;

            if (useResCode & m_ExitCode != 0)
            {
                if ((m_AbortProgramNow && m_AbortProgramPostLogEntry) || !m_AbortProgramNow)
                {
                    var msg = "  ProgRunner.ExitCode = " + m_ExitCode + " for Program = " + progNameLoc;
                    OnErrorEvent(msg);
                }
                return false;
            }

            return !m_AbortProgramNow;
        }

        #endregion

        void ProgRunner_ConsoleErrorEvent(string NewText)
        {
            OnErrorEvent("Console error: " + NewText);

            if (string.IsNullOrWhiteSpace(m_CachedConsoleErrors))
            {
                m_CachedConsoleErrors = NewText;
            }
            else
            {
                m_CachedConsoleErrors += Environment.NewLine + NewText;
            }

        }

        void ProgRunner_ConsoleOutputEvent(string newText)
        {
            ConsoleOutputEvent?.Invoke(newText);
        }

        void ProgRunner_ProgChanged(clsProgRunner obj)
        {
            // This event is ignored by this class
        }
    }

}