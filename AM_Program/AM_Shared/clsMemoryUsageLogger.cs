using PRISM;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Created 02/09/2009
// Last updated 02/03/2016
//*********************************************************************************************************


namespace AnalysisManagerBase
{
    /// <summary>
    /// Memory usage logger
    /// </summary>
    public class clsMemoryUsageLogger : EventNotifier
    {

        #region "Constants and Enums"

        private const char COL_SEP = '\t';

        private const string LOG_FILE_EXTENSION = ".txt";

        private const string LOG_FILE_TIMESTAMP_FORMAT = "yyyy-MM";

        private const string LOG_FILE_MATCH_SPEC = "????-??";

        private const string LOG_FILE_DATE_REGEX = @"(?<Year>\d{4,4})-(?<Month>\d+)";

        private const string MEMORY_USAGE_LOG_PREFIX = "MemoryUsageLog";

        #endregion

        #region "Fields"

        /// <summary>
        /// The minimum interval between appending a new memory usage entry to the log
        /// </summary>
        private float mMinimumMemoryUsageLogIntervalMinutes = 1;

        /// <summary>
        /// Used to determine the amount of free memory
        /// </summary>
        private PerformanceCounter mPerfCounterFreeMemory;
        private PerformanceCounter mPerfCounterPoolPagedBytes;

        private PerformanceCounter mPerfCounterPoolNonPagedBytes;

        private bool mPerfCountersInitialized;

        private DateTime mLastWriteTime;

        #endregion

        #region "Properties"

        /// <summary>
        /// Output folder for the log file
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks>If this is an empty string, the log file is created in the working directory</remarks>
        public string LogFolderPath { get; }

        /// <summary>
        /// The minimum interval between appending a new memory usage entry to the log
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public float MinimumLogIntervalMinutes
        {
            get => mMinimumMemoryUsageLogIntervalMinutes;
            set
            {
                if (value < 0)
                    value = 0;
                mMinimumMemoryUsageLogIntervalMinutes = value;
            }
        }
        #endregion

        #region "Methods"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="logFolderPath">
        /// Folder in which to write the memory log file(s)
        /// If this is an empty string, the log file is created in the working directory</param>
        /// <param name="minLogIntervalMinutes">Minimum log interval, in minutes</param>
        /// <remarks>
        /// Use WriteMemoryUsageLogEntry to append an entry to the log file.
        /// Alternatively use GetMemoryUsageSummary() to retrieve the memory usage as a string</remarks>
        public clsMemoryUsageLogger(string logFolderPath, float minLogIntervalMinutes = 5)
        {
            if (string.IsNullOrWhiteSpace(logFolderPath))
            {
                LogFolderPath = string.Empty;
            }
            else
            {
                LogFolderPath = logFolderPath;
            }

            MinimumLogIntervalMinutes = minLogIntervalMinutes;

            mLastWriteTime = DateTime.MinValue;

        }

        /// <summary>
        /// Assure that old log files are zipped by year
        /// </summary>
        /// <param name="currentLogFilePath"></param>
        private void ArchiveOldLogs(string currentLogFilePath)
        {
            try
            {
                var currentLogFile = new FileInfo(currentLogFilePath);
                var currentLogDirectory = currentLogFile.Directory;

                if (currentLogDirectory == null)
                {
                    return;
                }

                var logFolder = new DirectoryInfo(Path.Combine(currentLogDirectory.FullName, "Logs"));

                // Find all log files that start with "MemoryUsageLog_"
                var logFiles = currentLogDirectory.GetFiles(MEMORY_USAGE_LOG_PREFIX + "*");

                var logFilesByDate = new List<KeyValuePair<DateTime, FileInfo>>();

                // Move all but the two most recent files into the Logs folder
                foreach (var item in logFiles)
                {
                    // Current log file; skip it
                    if (string.Equals(item.FullName, currentLogFile.FullName))
                        continue;

                    logFilesByDate.Add(new KeyValuePair<DateTime, FileInfo>(item.LastWriteTime, item));
                }

                var logFilesToMove = (from item in logFilesByDate orderby item.Key select item.Value).Take(logFilesByDate.Count - 1);
                foreach (var logFile in logFilesToMove)
                {
                    var newPath = Path.Combine(currentLogDirectory.FullName, "Logs", logFile.Name);
                    if (File.Exists(newPath))
                        continue;

                    logFile.MoveTo(newPath);
                }

                // Move MemoryUsageLog files in the Logs folder into year-based subdirectories
                PRISM.Logging.FileLogger.ArchiveOldLogs(logFolder, LOG_FILE_MATCH_SPEC, LOG_FILE_EXTENSION, LOG_FILE_DATE_REGEX);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception archiving old log files", ex);
            }

        }

        /// <summary>
        /// Returns the amount of free memory on the current machine
        /// </summary>
        /// <returns>Free memory, in MB</returns>
        /// <remarks></remarks>
        public float GetFreeMemoryMB()
        {
            if (clsGlobal.LinuxOS)
            {
                return clsGlobal.GetFreeMemoryMB();
            }

            return GetFreeMemoryMBWindows();
        }

        private float GetFreeMemoryMBWindows()
        {
            try
            {
                if (mPerfCounterFreeMemory == null)
                {
                    return clsGlobal.GetFreeMemoryMB();
                }

                return mPerfCounterFreeMemory.NextValue();
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception accessing performance counter mPerfCounterFreeMemory", ex);
                return -1;
            }
        }

        /// <summary>
        /// Memory usage header
        /// </summary>
        /// <returns></returns>
        private string GetMemoryUsageHeader()
        {
            return
                "Date" + COL_SEP +
                "Time" + COL_SEP +
                "ProcessMemoryUsage_MB" + COL_SEP +
                "FreeMemory_MB" + COL_SEP +
                "PoolPaged_MB" + COL_SEP +
                "PoolNonPaged_MB";
        }

        /// <summary>
        /// Summarize memory usage
        /// </summary>
        /// <returns></returns>
        private string GetMemoryUsageSummary()
        {
            if (!mPerfCountersInitialized)
            {
                clsGlobal.CheckStopTrace("InitMemoryUsagePerfCounters");
                InitializePerfCounters();
            }

            clsGlobal.CheckStopTrace("GetProcessMemoryUsageMB");
            var processMemoryUsageMB = GetProcessMemoryUsageMB();

            clsGlobal.CheckStopTrace("GetFreeMemoryMB");
            var freeMemoryMB = GetFreeMemoryMB();

            float poolPagedMemory;
            float poolNonPagedMemory;

            if (clsGlobal.LinuxOS)
            {
                poolPagedMemory = 0;
                poolNonPagedMemory = 0;
            }
            else
            {
                clsGlobal.CheckStopTrace("GetPoolPagedMemory");
                poolPagedMemory = GetPoolPagedMemory();

                clsGlobal.CheckStopTrace("GetPoolNonPagedMemory");
                poolNonPagedMemory = GetPoolNonPagedMemory();
            }

            var currentTime = DateTime.Now;

            var usageSummary =
                $"{currentTime:yyyy-MM-dd}{COL_SEP}" +
                $"{currentTime:hh:mm:ss tt}{COL_SEP}" +
                $"{processMemoryUsageMB:F1}{COL_SEP}" +
                $"{freeMemoryMB:F1}{COL_SEP}" +
                $"{poolPagedMemory:F1}{COL_SEP}" +
                $"{poolNonPagedMemory:F1}";

            return usageSummary;
        }

        /// <summary>
        /// Returns the amount of pool NonPaged memory on the current machine
        /// </summary>
        /// <returns>Pool NonPaged memory, in MB</returns>
        /// <remarks></remarks>
        public float GetPoolNonPagedMemory()
        {
            try
            {
                if (mPerfCounterPoolNonPagedBytes == null)
                {
                    return 0;
                }

                return (float)(mPerfCounterPoolNonPagedBytes.NextValue() / 1024.0 / 1024);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception accessing performance counter mPerfCounterPoolNonPagedBytes", ex);
                return -1;
            }
        }

        /// <summary>
        /// Returns the amount of pool paged memory on the current machine
        /// </summary>
        /// <returns>Pool Paged memory, in MB</returns>
        /// <remarks></remarks>
        public float GetPoolPagedMemory()
        {
            try
            {
                if (mPerfCounterPoolPagedBytes == null)
                {
                    return 0;
                }

                return (float)(mPerfCounterPoolPagedBytes.NextValue() / 1024.0 / 1024);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception accessing performance counter mPerfCounterPoolPagedBytes", ex);
                return -1;
            }
        }

        /// <summary>
        /// Returns the amount of memory that the currently running process is using
        /// </summary>
        /// <returns>Memory usage, in MB</returns>
        /// <remarks></remarks>
        public float GetProcessMemoryUsageMB()
        {
            try
            {
                // Obtain a handle to the current process
                var currentProcess = Process.GetCurrentProcess();

                // The WorkingSet is the total physical memory usage
                return (float)(clsGlobal.BytesToMB(currentProcess.WorkingSet64));
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception determining memory usage of the current process", ex);
                return 0;
            }

        }

        /// <summary>
        /// Initializes the performance counters
        /// </summary>
        public void InitializePerfCounters()
        {
            if (clsGlobal.LinuxOS)
            {
                mPerfCountersInitialized = true;
                return;
            }

            try
            {
                clsGlobal.CheckStopTrace("InitPerfCounterMemoryMB");
                mPerfCounterFreeMemory = new PerformanceCounter("Memory", "Available MBytes") { ReadOnly = true };
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception instantiating performance counter 'Available MBytes'", ex);
            }

            try
            {
                clsGlobal.CheckStopTrace("InitPerfCounterPoolPaged");
                mPerfCounterPoolPagedBytes = new PerformanceCounter("Memory", "Pool Paged Bytes") { ReadOnly = true };
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception instantiating performance counter 'Pool Paged Bytes'", ex);
            }

            try
            {
                clsGlobal.CheckStopTrace("InitPerfCounterPoolNonPaged");
                mPerfCounterPoolNonPagedBytes = new PerformanceCounter("Memory", "Pool NonPaged Bytes") { ReadOnly = true };
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception instantiating performance counter 'Pool NonPaged Bytes'", ex);
            }

            mPerfCountersInitialized = true;

        }

        /// <summary>
        /// Writes a status file tracking memory usage
        /// </summary>
        /// <remarks>Also calls ArchiveOldLogs to assure that old MemoryUsageLog files are zipped by year</remarks>
        public void WriteMemoryUsageLogEntry()
        {
            // Create a new log file each month
            var logFileName = MEMORY_USAGE_LOG_PREFIX + "_" + DateTime.Now.ToString(LOG_FILE_TIMESTAMP_FORMAT) + LOG_FILE_EXTENSION;

            try
            {
                if (DateTime.UtcNow.Subtract(mLastWriteTime).TotalMinutes < mMinimumMemoryUsageLogIntervalMinutes)
                {
                    // Not enough time has elapsed since the last write; exit method
                    return;
                }
                mLastWriteTime = DateTime.UtcNow;

                string logFilePath;

                if (!string.IsNullOrWhiteSpace(LogFolderPath))
                {
                    logFilePath = Path.Combine(LogFolderPath, logFileName);
                }
                else
                {
                    logFilePath = string.Copy(logFileName);
                }

                var writeHeader = !File.Exists(logFilePath);

                using (var writer = new StreamWriter(new FileStream(logFilePath, FileMode.Append, FileAccess.Write, FileShare.Read)))
                {

                    if (writeHeader)
                    {
                        writer.WriteLine(GetMemoryUsageHeader());
                    }

                    writer.WriteLine(GetMemoryUsageSummary());
                }

                ArchiveOldLogs(logFilePath);

            }
            catch
            {
                var msg = "Error writing memory usage to file " + logFileName;

                if (string.IsNullOrWhiteSpace(LogFolderPath))
                {
                    OnWarningEvent(msg);
                }
                else
                {
                    OnWarningEvent(msg + " in folder " + LogFolderPath);
                }

            }

        }

        #endregion

    }
}
