using System;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;

namespace AnalysisManagerBase
{
    public class SystemMemoryInfo : clsEventNotifier
    {
        private System.Diagnostics.PerformanceCounter mFreeMemoryPerformanceCounter;

        /// <summary>
        /// Determine the free system memory, in MB
        /// </summary>
        /// <returns>Free memory, or -1 if an error</returns>
        public float GetFreeMemoryMB()
        {
            // TODO: can use CIM_OperatingSystem to get available physical memory

            try
            {
                if (mFreeMemoryPerformanceCounter == null)
                {
                    mFreeMemoryPerformanceCounter = new System.Diagnostics.PerformanceCounter("Memory", "Available MBytes");
                    mFreeMemoryPerformanceCounter.ReadOnly = true;
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

                OnStatusEvent("Available memory (MB) = " + freeMemoryMB.ToString("0.0"));

                return freeMemoryMB;
            }
            catch (Exception ex)
            {
                // Write this to the console now
                // We will call OnErrorEvent below if not on a Pub-1000 class machine and if between 12:00 am and 12:30 am
                Console.WriteLine();
                Console.WriteLine("Error in SystemMemoryUsage using mFreeMemoryPerformanceCounter: " + ex.Message);

                var rePub1000 = new Regex(@"Pub-1\d{3,}", RegexOptions.IgnoreCase);
                if (rePub1000.IsMatch(Environment.MachineName))
                {
                    // The Memory performance counters are not available on Windows instances running under VMWare on PIC
                }
                else
                {
                    // To avoid seeing this in the logs continually, we will only post this log message between 12 am and 12:30 am
                    // A possible fix for this is to add the user who is running this process to the "Performance Monitor Users" group
                    // in "Local Users and Groups" on the machine showing this error.  Alternatively, add the user to the "Administrators" group.
                    // In either case, you will need to reboot the computer for the change to take effect
                    if (System.DateTime.Now.Hour == 0 & System.DateTime.Now.Minute <= 30)
                    {
                        OnErrorEvent("Error instantiating the Memory.[Available MBytes] performance counter " +
                            "(this message is only logged between 12 am and 12:30 am)", ex);
                    }

                    return -1;
                }

                try
                {
                    var memInfo = new SystemMemoryLookup();
                    var memData = memInfo.MemoryStatus;

                    var freeMemoryMB = Convert.ToSingle(memData.ullAvailPhys / 1024.0 / 1024.0);
                    OnStatusEvent("Available memory from VB: " + freeMemoryMB + " MB");

                    return freeMemoryMB;
                }
                catch (Exception ex2)
                {
                    OnErrorEvent("Error in SystemMemoryUsage using SystemMemoryLookup", ex2);

                    return -1;
                }
            }
        }

        private class SystemMemoryLookup
        {
            // http://pinvoke.net/default.aspx/kernel32/GlobalMemoryStatusEx.html
            public readonly MemoryStatusEx MemoryStatus;
            private const int MemoryTightConst = 80;

            public bool isMemoryTight()
            {
                if (MemoryLoad > MemoryTightConst)
                    return true;
                else
                    return false;
            }

            public uint MemoryLoad { get; private set; }

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
                public uint dwLength;
                public uint dwMemoryLoad;
                public ulong ullTotalPhys;
                public ulong ullAvailPhys;
                public ulong ullTotalPageFile;
                public ulong ullAvailPageFile;
                public ulong ullTotalVirtual;
                public ulong ullAvailVirtual;
                public ulong ullAvailExtendedVirtual;

                public MemoryStatusEx()
                {
                    this.dwLength = (uint)Marshal.SizeOf(typeof(MemoryStatusEx));
                }
            }

            [return: MarshalAs(UnmanagedType.Bool)]
            [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
            static extern bool GlobalMemoryStatusEx([In, Out] MemoryStatusEx lpBuffer);
        }

    }

}
