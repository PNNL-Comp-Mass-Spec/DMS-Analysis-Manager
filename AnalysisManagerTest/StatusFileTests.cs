using System;
using System.IO;
using AnalysisManagerBase;
using NUnit.Framework;

namespace AnalysisManagerTest
{
    /// <summary>
    /// Status file tests
    /// </summary>
    [TestFixture]
    public class StatusFileTests
    {
        /// <summary>
        /// Test writing the status file
        /// </summary>
        /// <param name="statusFileName"></param>
        /// <param name="offlineMode"></param>
        /// <param name="linuxOS"></param>
        [Test]
        [TestCase("StatusTest.xml", false, false)]
        [TestCase("StatusTestOffline.xml", true, false)]
        [TestCase("StatusTestLinux.xml", true, true)]
        public void TestWriteStatusFile(string statusFileName, bool offlineMode, bool linuxOS)
        {
            if (offlineMode || linuxOS)
                clsGlobal.EnableOfflineMode(linuxOS);

            var statusFile = new FileInfo(statusFileName);
            var debugLevel = 2;

            // clsStatusTools inherits clsEventNotifier, which will show messages at the console if an event does not have a subscriber
            var statusTools = new clsStatusFile(statusFile.FullName, debugLevel) {
                WriteToConsoleIfNoListener = true
            };

            statusTools.UpdateAndWrite(EnumMgrStatus.STOPPED, EnumTaskStatus.STOPPED, EnumTaskStatusDetail.NO_TASK, 0);

            Console.WriteLine("Status written to " + statusTools.FileNamePath);
        }

    }
}
