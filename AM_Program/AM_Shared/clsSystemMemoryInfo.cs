using System;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using PRISM;

namespace AnalysisManagerBase
{
    /// <summary>
    /// System memory info
    /// </summary>
    /// <remarks>Supports both Linux and Windows</remarks>
    public class SystemMemoryInfo : clsEventNotifier
    {

        private System.Diagnostics.PerformanceCounter mFreeMemoryPerformanceCounter;

        private DateTime mLastDebugInfoTime;

        private readonly PRISM.clsLinuxSystemInfo mLinuxSystemInfo;

        /// <summary>
        /// Constructor
        /// </summary>
        public SystemMemoryInfo()
        {
            const bool LIMIT_LOGGING_BY_TIME_OF_DAY = true;

            mLastDebugInfoTime = DateTime.UtcNow.AddMinutes(-1);

            mLinuxSystemInfo = new PRISM.clsLinuxSystemInfo(LIMIT_LOGGING_BY_TIME_OF_DAY);
        }

        /// <summary>
        /// Determine the free system memory, in MB
        /// </summary>
        /// <returns>Free memory, or -1 if an error</returns>
        public float GetFreeMemoryMB()
        {
            if (clsGlobal.LinuxOS)
            {
                return GetFreeMemoryMBLinux();
            }

            return GetFreeMemoryMBWindows();
        }

        /// <summary>
        /// Determine the free system memory, in MB, on Linux
        /// </summary>
        /// <returns>Free memory, or -1 if an error</returns>
        public float GetFreeMemoryMBLinux()
        {
            var freeMemoryMB = mLinuxSystemInfo.GetFreeMemoryMB();
            return freeMemoryMB;

        }

        /// <summary>
        /// Determine the free system memory, in MB, on Windows
        /// </summary>
        /// <returns>Free memory, or -1 if an error</returns>
        public float GetFreeMemoryMBWindows()
        {
            // TODO (maybe): can use CIM_OperatingSystem to get available physical memory

            var showDebugInfo = DateTime.UtcNow.Subtract(mLastDebugInfoTime).TotalSeconds > 15;
            if (showDebugInfo)
                mLastDebugInfoTime = DateTime.UtcNow;

            try
            {
                if (mFreeMemoryPerformanceCounter == null)
                {
                    mFreeMemoryPerformanceCounter = new System.Diagnostics.PerformanceCounter("Memory", "Available MBytes") {
                        ReadOnly = true
                    };
                }

                var iterations = 0;
                float freeMemoryMB = 0;
                while (freeMemoryMB < float.Epsilon && iterations <= 3)
                {
                    freeMemoryMB = mFreeMemoryPerformanceCounter.NextValue();
                    if (freeMemoryMB < float.Epsilon)
                    {
                        // You sometimes have to call .NextValue() several times before it returns a useful number
                        // Wait 1 second and then try again
                        System.Threading.Thread.Sleep(1000);
                    }
                    iterations += 1;
                }

                if (showDebugInfo)
                    OnDebugEvent(string.Format("  {0,17}: {1,6:0} MB", "Available memory", freeMemoryMB));

                return freeMemoryMB;
            }
            catch (Exception ex)
            {
                if (showDebugInfo)
                    OnDebugEvent("Error in SystemMemoryUsage using mFreeMemoryPerformanceCounter: " + ex.Message);

                var rePub1000 = new Regex(@"Pub-1\d{3,}", RegexOptions.IgnoreCase);
                if (!rePub1000.IsMatch(Environment.MachineName))
                {
                    // Write this to the console now
                    // Log the error if not on a Pub-1000 class machine and if between 12:00 am and 12:30 am

                    if (showDebugInfo)
                        ConditionalLogError("Error instantiating the Memory.[Available MBytes] performance counter", ex);

                    return -1;
                }

                // The Memory performance counters are not available on Windows instances
                // running under VMWare on PIC (machine name will be Pub-1000, Pub-1001, etc.)
                // Try using SystemMemoryLookup instead

                try
                {
                    var memInfo = new SystemMemoryLookup();
                    var memData = memInfo.MemoryStatus;

                    var freeMemoryMB = (float)(memData.ullAvailPhys / 1024.0 / 1024.0);

                    if (showDebugInfo)
                        OnDebugEvent("Available memory from VB: " + freeMemoryMB + " MB");

                    return freeMemoryMB;
                }
                catch (Exception ex2)
                {
                    if (showDebugInfo)
                        ConditionalLogError("Error in SystemMemoryUsage using SystemMemoryLookup", ex2);

                    return -1;
                }
            }

        }

        private void ConditionalLogError(string message, Exception ex = null)
        {

            // To avoid seeing this in the logs continually, we will only post this log message between 12 am and 12:30 am
            // A possible fix for this is to add the user who is running this process to the "Performance Monitor Users" group
            // in "Local Users and Groups" on the machine showing this error.  Alternatively, add the user to the "Administrators" group.
            // In either case, you will need to reboot the computer for the change to take effect
            if (DateTime.Now.Hour == 0 && DateTime.Now.Minute <= 30)
            {
                OnErrorEvent(message + " (this message is only logged between 12 am and 12:30 am)", ex);
            }

        }

        /// <summary>
        /// From http://pinvoke.net/default.aspx/kernel32/GlobalMemoryStatusEx.html
        /// </summary>
        private class SystemMemoryLookup
        {
            /// <summary>
            /// Memory status
            /// </summary>
            public readonly MemoryStatusEx MemoryStatus;
            private const int MemoryTightConst = 80;

            public bool isMemoryTight()
            {
                if (MemoryLoad > MemoryTightConst)
                    return true;
                else
                    return false;
            }

            private uint MemoryLoad { get; }

            /// <summary>
            /// Constructor
            /// </summary>
            public SystemMemoryLookup()
            {
                MemoryStatus = new MemoryStatusEx();
                if (GlobalMemoryStatusEx(MemoryStatus))
                {
                    MemoryLoad = MemoryStatus.dwMemoryLoad;
                    // etc.. Repeat for other structure members
                }
                else
                {
                    throw new Exception("Unable to initalize the GlobalMemoryStatusEx API");
                }
            }

            [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
            public class MemoryStatusEx
            {
                // ReSharper disable once NotAccessedField.Local
                private uint dwLength;

#pragma warning disable 169
#pragma warning disable 649


                /// <summary>
                /// Percent of memory in use (integer)
                /// </summary>
                public uint dwMemoryLoad;

                /// <summary>
                /// Total physical memory (in KB)
                /// </summary>
                public ulong ullTotalPhys;

                /// <summary>
                /// Free physical memory (in KB)
                /// </summary>
                public ulong ullAvailPhys;

                /// <summary>
                /// Page file size (in KB)
                /// </summary>
                public ulong ullTotalPageFile;

                /// <summary>
                /// Free page file space (in KB)
                /// </summary>
                public ulong ullAvailPageFile;

                /// <summary>
                /// Total virtual memory (in KB)
                /// </summary>
                public ulong ullTotalVirtual;

                /// <summary>
                /// Free virtual memoyr (in KB)
                /// </summary>
                public ulong ullAvailVirtual;

                /// <summary>
                /// Free extended memory (in KB)
                /// </summary>
                public ulong ullAvailExtendedVirtual;

#pragma warning restore 649
#pragma warning restore 169

                public MemoryStatusEx()
                {
                    dwLength = (uint)Marshal.SizeOf(typeof(MemoryStatusEx));
                }
            }

            [return: MarshalAs(UnmanagedType.Bool)]
            [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
            static extern bool GlobalMemoryStatusEx([In, Out] MemoryStatusEx lpBuffer);
        }

    }

}
