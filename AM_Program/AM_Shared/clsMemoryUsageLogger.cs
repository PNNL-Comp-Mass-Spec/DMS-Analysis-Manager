using System;
using System.Diagnostics;
using System.IO;

//*********************************************************************************************************
// Written by Matthew Monroe for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Created 02/09/2009
// Last updated 02/03/2016
//*********************************************************************************************************


namespace AnalysisManagerBase
{
    public class clsMemoryUsageLogger
    {

        #region "Module variables"

        private const char COL_SEP = '\t';

        /// <summary>
        /// Status file name and location
        /// </summary>
        private readonly string m_LogFolderPath;

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
        public string LogFolderPath => m_LogFolderPath;

        /// <summary>
        /// The minimum interval between appending a new memory usage entry to the log
        /// </summary>
        /// <value></value>
        /// <returns></returns>
        /// <remarks></remarks>
        public float MinimumLogIntervalMinutes
        {
            get { return m_MinimumMemoryUsageLogIntervalMinutes; }
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
                m_LogFolderPath = string.Empty;
            }
            else
            {
                m_LogFolderPath = logFolderPath;
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
            try
            {
                if (m_PerfCounterFreeMemory == null)
                {
                    return 0;
                }

                return m_PerfCounterFreeMemory.NextValue();
            }
            catch (Exception)
            {
                return -1;
            }
        }

        public string GetMemoryUsageHeader()
        {
            return "Date" + COL_SEP + "Time" + COL_SEP + "ProcessMemoryUsage_MB" + COL_SEP + "FreeMemory_MB" + COL_SEP + "PoolPaged_MB" + COL_SEP + "PoolNonpaged_MB";
        }

        public string GetMemoryUsageSummary()
        {

            if (!m_PerfCountersIntitialized)
            {
                InitializePerfCounters();
            }

            var currentTime = DateTime.Now;

            return currentTime.ToString("yyyy-MM-dd") + COL_SEP + 
                currentTime.ToString("hh:mm:ss tt") + COL_SEP + 
                GetProcessMemoryUsageMB().ToString("0.0") + COL_SEP + 
                GetFreeMemoryMB().ToString("0.0") + COL_SEP + 
                GetPoolPagedMemory().ToString("0.0") + COL_SEP + 
                GetPoolNonpagedMemory().ToString("0.0");

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

                return Convert.ToSingle(m_PerfCounterPoolNonpagedBytes.NextValue() / 1024.0 / 1024);
            }
            catch (Exception)
            {
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

                return Convert.ToSingle(m_PerfCounterPoolPagedBytes.NextValue() / 1024.0 / 1024);
            }
            catch (Exception)
            {
                return -1;
            }
        }

        /// <summary>
        /// Returns the amount of memory that the currently running process is using
        /// </summary>
        /// <returns>Memory usage, in MB</returns>
        /// <remarks></remarks>
        public static float GetProcessMemoryUsageMB()
        {
            try
            {
                // Obtain a handle to the current process
                var objProcess = Process.GetCurrentProcess();

                // The WorkingSet is the total physical memory usage 
                return Convert.ToSingle(clsGlobal.BytesToMB(objProcess.WorkingSet64));
            }
            catch (Exception)
            {
                return 0;
            }

        }

        /// <summary>
        /// Initializes the performance counters
        /// </summary>
        /// <returns>Any errors that occur; empty string if no errors</returns>
        /// <remarks></remarks>
        public string InitializePerfCounters()
        {
            var msgErrors = string.Empty;

            try
            {
                m_PerfCounterFreeMemory = new PerformanceCounter("Memory", "Available MBytes") {ReadOnly = true};
            }
            catch (Exception ex)
            {
                if (msgErrors.Length > 0)
                    msgErrors += "; ";
                msgErrors += "Error instantiating the Memory: 'Available MBytes' performance counter: " + ex.Message;
            }

            try
            {
                m_PerfCounterPoolPagedBytes = new PerformanceCounter("Memory", "Pool Paged Bytes") {ReadOnly = true};
            }
            catch (Exception ex)
            {
                if (msgErrors.Length > 0)
                    msgErrors += "; ";
                msgErrors += "Error instantiating the Memory: 'Pool Paged Bytes' performance counter: " + ex.Message;
            }

            try
            {
                m_PerfCounterPoolNonpagedBytes = new PerformanceCounter("Memory", "Pool NonPaged Bytes") {ReadOnly = true};
            }
            catch (Exception ex)
            {
                if (msgErrors.Length > 0)
                    msgErrors += "; ";
                msgErrors += "Error instantiating the Memory: 'Pool NonPaged Bytes' performance counter: " + ex.Message;
            }

            m_PerfCountersIntitialized = true;

            return msgErrors;

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
                    // Not enough time has elapsed since the last write; exit sub
                    return;
                }
                m_LastWriteTime = DateTime.UtcNow;

                // We're creating a new log file each month
                var strLogFileName = "MemoryUsageLog_" + DateTime.Now.ToString("yyyy-MM") + ".txt";
                string strLogFilePath;

                if (!string.IsNullOrWhiteSpace(m_LogFolderPath))
                {
                    strLogFilePath = Path.Combine(m_LogFolderPath, strLogFileName);
                }
                else
                {
                    strLogFilePath = string.Copy(strLogFileName);
                }

                var blnWriteHeader = !File.Exists(strLogFilePath);

                using (var swOutFile = new StreamWriter(new FileStream(strLogFilePath, FileMode.Append, FileAccess.Write, FileShare.Read)))
                {

                    if (blnWriteHeader)
                    {
                        GetMemoryUsageHeader();
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
