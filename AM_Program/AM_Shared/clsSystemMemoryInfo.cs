using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using PRISM;

namespace AnalysisManagerBase
{
    public class SystemMemoryInfo : clsEventNotifier
    {
        private readonly Regex mRegexMemorySize;
        private readonly Regex mRegexMemorySizeNoUnits;

        private System.Diagnostics.PerformanceCounter mFreeMemoryPerformanceCounter;

        private DateTime mLastDebugInfoTime;

        /// <summary>
        /// Constructor
        /// </summary>
        public SystemMemoryInfo()
        {
            mRegexMemorySize = new Regex(@"(?<Size>\d+) +(?<Units>(KB|MB|GB|TB|))", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            mRegexMemorySizeNoUnits = new Regex(@"(?<Size>\d+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            mLastDebugInfoTime = DateTime.UtcNow.AddMinutes(-1);

        }

        /// <summary>
        /// Determine the free system memory, in MB
        /// </summary>
        /// <returns>Free memory, or -1 if an error</returns>
        public float GetFreeMemoryMB()
        {
            return clsGlobal.LinuxOS ? GetFreeMemoryMBLinux() : GetFreeMemoryMBWindows();
        }

        /// <summary>
        /// Determine the free system memory, in MB, on Linux
        /// </summary>
        /// <returns>Free memory, or -1 if an error</returns>
        public float GetFreeMemoryMBLinux()
        {
            const string MEMINFO_FILE_PATH = "/proc/meminfo";

            var showDebugInfo = DateTime.UtcNow.Subtract(mLastDebugInfoTime).TotalSeconds > 15;
            if (showDebugInfo)
                mLastDebugInfoTime = DateTime.UtcNow;

            try
            {

                var memInfoFile = new FileInfo(MEMINFO_FILE_PATH);
                if (!memInfoFile.Exists)
                {
                    if (showDebugInfo)
                        ConditionalLogError("Memory info file not found: " + MEMINFO_FILE_PATH);

                    return -1;
                }

                // CentOS 7 and Ubuntu report statistic MemAvailable:
                //   an estimate of how much memory is available for starting new applications, without swapping
                // If present, we use this value, otherwise we report the sum of the matched stats in memoryStatsToSum
                const string MEMAVAILABLE_KEY = "MemAvailable";

                // Keys in this dictionary are memory stats to find
                // Values are initially false, then set to true if a match is found
                var memoryStatsToSum = new Dictionary<string, bool>
                {
                    {"MemFree", false},
                    {"Inactive(file)", false},
                    {"SReclaimable", false}
                };

                var memoryStatKeys = memoryStatsToSum.Keys;

                float totalAvailableMemoryMB = 0;

                Console.WriteLine();

                using (var reader = new StreamReader(new FileStream(memInfoFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrWhiteSpace(dataLine))
                            continue;

                        if (dataLine.ToLower().StartsWith(MEMAVAILABLE_KEY))
                        {
                            var memAvailableMB = ExtractMemoryMB(dataLine, showDebugInfo);

                            if (showDebugInfo)
                                OnDebugEvent(string.Format("  {0,17}: {1,6:0} MB", "Available memory", memAvailableMB));

                            return memAvailableMB;
                        }

                        foreach (var memoryStatKey in memoryStatKeys)
                        {
                            if (memoryStatsToSum[memoryStatKey])
                            {
                                // Stat already matched
                                continue;
                            }

                            if (!dataLine.StartsWith(memoryStatKey, StringComparison.OrdinalIgnoreCase))
                                continue;

                            var memorySizeMB = ExtractMemoryMB(dataLine, showDebugInfo);
                            if (memorySizeMB > -1)
                            {
                                if (showDebugInfo)
                                    OnDebugEvent(string.Format("  {0,17}: {1,6:0} MB", memoryStatKey, memorySizeMB));

                                totalAvailableMemoryMB += memorySizeMB;
                                memoryStatsToSum[memoryStatKey] = true;
                                break;
                            }
                        }
                    }
                }

                if ((from item in memoryStatsToSum where item.Value select item).Any())
                {
                    if (showDebugInfo)
                    {
                        OnDebugEvent("   ---------------------------");
                        OnDebugEvent(string.Format("  {0,17}: {1,6:0} MB", "Available memory", totalAvailableMemoryMB));
                    }

                    return totalAvailableMemoryMB;
                }

                if (showDebugInfo)
                    ConditionalLogError("MemFree statistic not found in " + MEMINFO_FILE_PATH);

                return -1;

            }
            catch (Exception ex)
            {
                if (showDebugInfo)
                    ConditionalLogError("Error in GetFreeMemoryMBLinux: " + ex.Message);

                return -1;
            }
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

        private float ExtractMemoryMB(string dataLine, bool showDebugInfo)
        {

            Match match;
            string units;

            var matchUnits = mRegexMemorySize.Match(dataLine);

            if (matchUnits.Success)
            {
                match = matchUnits;
                units = matchUnits.Groups["Units"].Value.ToLower();
            }
            else
            {
                var matchNoUnits = mRegexMemorySizeNoUnits.Match(dataLine);

                if (matchNoUnits.Success)
                {
                    match = matchNoUnits;
                    units = "bytes";
                }
                else
                {
                    if (showDebugInfo)
                        ConditionalLogError("Memory size not in the expected format of 12345678 kB; actually " + dataLine);

                    return -1;
                }
            }

            if (!long.TryParse(match.Groups["Size"].Value, out var memorySize))
            {
                if (showDebugInfo)
                    ConditionalLogError("Memory size parse error; could not extract an integer from " + dataLine);

                return -1;
            }

            float memorySizeMB;

            switch (units)
            {
                case "b":
                case "bytes":
                    memorySizeMB = (float)(memorySize / 1024.0 / 1024.0);
                    break;
                case "kb":
                    memorySizeMB = (float)(memorySize / 1024.0);
                    break;
                case "mb":
                    memorySizeMB = (float)(memorySize);
                    break;
                case "gb":
                    memorySizeMB = (float)(memorySize * 1024.0);
                    break;
                case "tb":
                    memorySizeMB = (float)(memorySize * 1024.0 * 1024);
                    break;
                case "pb":
                    memorySizeMB = (float)(memorySize * 1024.0 * 1024 * 1024);
                    break;
                default:
                    if (showDebugInfo)
                        ConditionalLogError("Memory size parse error; unknown units for " + dataLine);

                    return -1;
            }

            return memorySizeMB;
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
