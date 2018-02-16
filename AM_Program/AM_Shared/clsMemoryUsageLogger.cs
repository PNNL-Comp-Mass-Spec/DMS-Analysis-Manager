using System;
using System.Diagnostics;
using System.IO;
using PRISM;

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
    public class clsMemoryUsageLogger : clsEventNotifier
    {

        #region "Constants and Enums"

        private const char COL_SEP = '\t';


        /// <summary>
        /// The minimum interval between appending a new memory usage entry to the log
        /// </summary>
        private float m_MinimumMemoryUsageLogIntervalMinutes = 1;

        /// <summary>
        /// Used to determine the amount of free memory
        /// </summary>
        private PerformanceCounter m_PerfCounterFreeMemory;
        private PerformanceCounter m_PerfCounterPoolPagedBytes;

        private PerformanceCounter m_PerfCounterPoolNonpagedBytes;
        #endregion

        private bool m_PerfCountersIntitialized;

        private DateTime m_LastWriteTime;

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
            get => m_MinimumMemoryUsageLogIntervalMinutes;
            set
            {
                if (value < 0)
                    value = 0;
                m_MinimumMemoryUsageLogIntervalMinutes = value;
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

            m_LastWriteTime = DateTime.MinValue;

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
                if (m_PerfCounterFreeMemory == null)
                {
                    return clsGlobal.GetFreeMemoryMB();
                }

                return m_PerfCounterFreeMemory.NextValue();
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception accessing performance counter m_PerfCounterFreeMemory", ex);
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
                "PoolNonpaged_MB";
        }

        /// <summary>
        /// Summarize memory usage
        /// </summary>
        /// <returns></returns>
        private string GetMemoryUsageSummary()
        {
            if (!m_PerfCountersIntitialized)
            {
                InitializePerfCounters();
            }

            var processMemoryUsageMB = GetProcessMemoryUsageMB();
            var freeMemoryMB = GetFreeMemoryMB();
            float poolPagedMemory;
            float poolNonpagedMemory;

            if (clsGlobal.LinuxOS)
            {
                poolPagedMemory = 0;
                poolNonpagedMemory = 0;
            }
            else
            {
                poolPagedMemory = GetPoolPagedMemory();
                poolNonpagedMemory = GetPoolNonpagedMemory();
            }

            var currentTime = DateTime.Now;

            var usageSummary =
                $"{currentTime:yyyy-MM-dd}{COL_SEP}" +
                $"{currentTime:hh:mm:ss tt}{COL_SEP}" +
                $"{processMemoryUsageMB:F1}{COL_SEP}" +
                $"{freeMemoryMB:F1}{COL_SEP}" +
                $"{poolPagedMemory:F1}{COL_SEP}" +
                $"{poolNonpagedMemory:F1}";

            return usageSummary;
        }

        /// <summary>
        /// Returns the amount of pool nonpaged memory on the current machine
        /// </summary>
        /// <returns>Pool Nonpaged memory, in MB</returns>
        /// <remarks></remarks>
        public float GetPoolNonpagedMemory()
        {
            try
            {
                if (m_PerfCounterPoolNonpagedBytes == null)
                {
                    return 0;
                }

                return (float)(m_PerfCounterPoolNonpagedBytes.NextValue() / 1024.0 / 1024);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception accessing performance counter m_PerfCounterPoolNonpagedBytes", ex);
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
                if (m_PerfCounterPoolPagedBytes == null)
                {
                    return 0;
                }

                return (float)(m_PerfCounterPoolPagedBytes.NextValue() / 1024.0 / 1024);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception accessing performance counter m_PerfCounterPoolPagedBytes", ex);
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
                m_PerfCountersIntitialized = true;
                return;
            }

            try
            {
                m_PerfCounterFreeMemory = new PerformanceCounter("Memory", "Available MBytes") { ReadOnly = true };
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception instantiating performance counter 'Available MBytes'", ex);
            }

            try
            {
                m_PerfCounterPoolPagedBytes = new PerformanceCounter("Memory", "Pool Paged Bytes") { ReadOnly = true };
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception instantiating performance counter 'Pool Paged Bytes'", ex);
            }

            try
            {
                m_PerfCounterPoolNonpagedBytes = new PerformanceCounter("Memory", "Pool NonPaged Bytes") { ReadOnly = true };
            }
            catch (Exception ex)
            {
                OnErrorEvent("Exception instantiating performance counter 'Pool NonPaged Bytes'", ex);
            }

            m_PerfCountersIntitialized = true;

        }

        /// <summary>
        /// Writes a status file tracking memory usage
        /// </summary>
        /// <remarks></remarks>
        public void WriteMemoryUsageLogEntry()
        {

            try
            {
                if (DateTime.UtcNow.Subtract(m_LastWriteTime).TotalMinutes < m_MinimumMemoryUsageLogIntervalMinutes)
                {
                    // Not enough time has elapsed since the last write; exit method
                    return;
                }
                m_LastWriteTime = DateTime.UtcNow;

                // We're creating a new log file each month
                var logFileName = "MemoryUsageLog_" + DateTime.Now.ToString("yyyy-MM") + ".txt";
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

                using (var swOutFile = new StreamWriter(new FileStream(logFilePath, FileMode.Append, FileAccess.Write, FileShare.Read)))
                {

                    if (writeHeader)
                    {
                        swOutFile.WriteLine(GetMemoryUsageHeader());
                    }

                    swOutFile.WriteLine(GetMemoryUsageSummary());

                }

            }
            catch
            {
                // Ignore errors here
            }

        }

        #endregion

    }
}
