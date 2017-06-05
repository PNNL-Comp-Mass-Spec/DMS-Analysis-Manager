using System;
using System.IO;
using AnalysisManagerBase;
using NUnit.Framework;
using PRISM;

namespace AnalysisManagerTest
{
    [TestFixture]
    public class StatusFileTests
    {
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

            var statusTools = new clsStatusFile(statusFile.FullName, debugLevel);
            RegisterEvents(statusTools);

            statusTools.UpdateAndWrite(EnumMgrStatus.STOPPED, EnumTaskStatus.STOPPED, EnumTaskStatusDetail.NO_TASK, 0);

            Console.WriteLine("Status written to " + statusTools.FileNamePath);
        }


        #region "clsEventNotifier events"

        private void RegisterEvents(clsEventNotifier oProcessingClass)
        {
            oProcessingClass.DebugEvent += DebugEventHandler;
            oProcessingClass.StatusEvent += StatusEventHandler;
            oProcessingClass.ErrorEvent += ErrorEventHandler;
            oProcessingClass.WarningEvent += WarningEventHandler;
        }

        private void DebugEventHandler(string statusMessage)
        {
            Console.WriteLine(statusMessage);
        }

        private void StatusEventHandler(string statusMessage)
        {
            Console.WriteLine(statusMessage);
        }

        private void ErrorEventHandler(string errorMessage, Exception ex)
        {
            Console.WriteLine("Error: " + errorMessage);
            Console.WriteLine(clsStackTraceFormatter.GetExceptionStackTraceMultiLine(ex));
        }

        private void WarningEventHandler(string warningMessage)
        {
            Console.WriteLine("Warning: " + warningMessage);
        }

        #endregion
    }
}
