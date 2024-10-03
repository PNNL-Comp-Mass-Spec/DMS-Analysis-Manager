using AnalysisManagerBase;
using NUnit.Framework;
using System;
using System.IO;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerTest
{
    /// <summary>
    /// Status file tests
    /// </summary>
    [TestFixture]
    public class StatusFileTests
    {
        private readonly IMgrParams mMgrSettings;

        /// <summary>
        /// Constructor
        /// </summary>
        private StatusFileTests()
        {
            mMgrSettings = new ManagerParameters();
        }

        /// <summary>
        /// Test writing the status file
        /// </summary>
        /// <param name="statusFileName">Status file name</param>
        /// <param name="offlineMode">True if the manager is offline</param>
        /// <param name="linuxOS">True if running on Linux</param>
        [Test]
        [TestCase("StatusTest.xml", false, false)]
        [TestCase("StatusTestOffline.xml", true, false)]
        [TestCase("StatusTestLinux.xml", true, true)]
        public void TestWriteStatusFile(string statusFileName, bool offlineMode, bool linuxOS)
        {
            if (offlineMode || linuxOS)
                Global.EnableOfflineMode(linuxOS);

            var statusFile = new FileInfo(statusFileName);
            const int debugLevel = 2;

            // StatusTools inherits EventNotifier, which will show messages at the console if an event does not have a subscriber
            var statusTools = new StatusFile(mMgrSettings, statusFile.FullName, debugLevel) {
                WriteToConsoleIfNoListener = true
            };

            statusTools.UpdateAndWrite(MgrStatusCodes.STOPPED, TaskStatusCodes.STOPPED, TaskStatusDetailCodes.NO_TASK, 0);

            Console.WriteLine("Status written to " + statusTools.FileNamePath);
        }
    }
}
