using System;
using System.Collections.Generic;

namespace AnalysisManagerBase
{
    public class SystemProcessInfo : PRISM.clsEventNotifier
    {
        private readonly PRISM.clsLinuxSystemInfo mLinuxSystemInfo;

        private readonly PRISMWin.clsProcessStats mWindowsProcessStats;

        /// <summary>
        /// Constructor
        /// </summary>
        public SystemProcessInfo()
        {
            const bool LIMIT_LOGGING_BY_TIME_OF_DAY = true;

            mLinuxSystemInfo = new PRISM.clsLinuxSystemInfo(LIMIT_LOGGING_BY_TIME_OF_DAY);

            if (clsGlobal.LinuxOS)
                return;

            mWindowsProcessStats = new PRISMWin.clsProcessStats(LIMIT_LOGGING_BY_TIME_OF_DAY);
            mWindowsProcessStats.ErrorEvent += OnWindowsProcessErrorEvent;

        }

        /// <summary>
        /// Clear the performance counter cached for the given Process ID
        /// </summary>
        /// <remarks></remarks>
        public void ClearCachedPerformanceCounterForProcessID(int processId)
        {
            mWindowsProcessStats?.ClearCachedPerformanceCounterForProcessID(processId);
        }

        public int GetCoreCount()
        {
            if (clsGlobal.LinuxOS)
            {
                return GetCoreCountLinux();
            }

            return GetCoreCountWindows();
        }

        private int GetCoreCountLinux()
        {
            return mLinuxSystemInfo.GetCoreCount();
        }

        private int GetCoreCountWindows()
        {
            if (mWindowsProcessStats == null)
                return 0;

            return mWindowsProcessStats.GetCoreCount();
        }

        public float GetCoreUsageByProcessName(string processName, out List<int> processIDs)
        {
            if (clsGlobal.LinuxOS)
            {
                return GetCoreUsageByProcessNameLinux(processName, out processIDs);
            }

            return GetCoreUsageByProcessNameWindows(processName, out processIDs);
        }

        private float GetCoreUsageByProcessNameLinux(string processName, out List<int> processIDs)
        {
            var argumentText = string.Empty;
            return mLinuxSystemInfo.GetCoreUsageByProcessName(processName, argumentText, out processIDs);
        }

        private float GetCoreUsageByProcessNameWindows(string processName, out List<int> processIDs)
        {
            if (mWindowsProcessStats == null)
            {
                processIDs = new List<int>();
                return 0;
            }

            return mWindowsProcessStats.GetCoreUsageByProcessName(processName, out processIDs);
        }

        public float GetCoreUsageByProcessID(int processID)
        {
            if (clsGlobal.LinuxOS)
            {
                return GetCoreUsageByProcessIDLinux(processID);
            }

            return GetCoreUsageByProcessIDWindows(processID);
        }

        private float GetCoreUsageByProcessIDLinux(int processID)
        {
            return mLinuxSystemInfo.GetCoreUsageByProcessID(processID, out _);
        }

        private float GetCoreUsageByProcessIDWindows(int processID)
        {
            if (mWindowsProcessStats == null)
                return 0;

            return mWindowsProcessStats.GetCoreUsageByProcessID(processID);
        }

        /// <summary>
        /// Returns the CPU usage
        /// </summary>
        /// <returns>Value between 0 and 100</returns>
        /// <remarks>
        /// This is CPU usage for all running applications, not just this application
        /// For CPU usage of a single application use clsGlobal.ProcessInfo.GetCoreUsageByProcessID()
        /// </remarks>
        public float GetCPUUtilization()
        {
            if (clsGlobal.LinuxOS)
            {
                return GetCPUUtilizationLinux();
            }

            return GetCPUUtilizationWindows();
        }

        private float GetCPUUtilizationLinux()
        {
            return mLinuxSystemInfo.GetCPUUtilization();
        }

        private float GetCPUUtilizationWindows()
        {
            if (mWindowsProcessStats == null)
                return 0;

            return mWindowsProcessStats.GetCPUUtilization();
        }

        /// <summary>
        /// Report an error
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex">Exception (allowed to be nothing)</param>
        protected void OnWindowsProcessErrorEvent(string message, Exception ex)
        {
            var virtualMachineOnPIC = clsGlobal.UsingVirtualMachineOnPIC();

            if (!virtualMachineOnPIC)
            {
                OnErrorEvent(message, ex);
            }

        }
    }
}
