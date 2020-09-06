// The DMS Analysis Manager program runs automated data analysis for PRISM
//
// -------------------------------------------------------------------------------
// Written by Dave Clark, Matthew Monroe, and John Sandoval for the Department of Energy (PNNL, Richland, WA)
//
// E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov
// Website: https://omics.pnl.gov/ or https://www.pnnl.gov/sysbio/ or https://panomics.pnnl.gov/
// -------------------------------------------------------------------------------
//
// Licensed under the 2-Clause BSD License; you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
// https://opensource.org/licenses/BSD-2-Clause
//
// Copyright 2017 Battelle Memorial Institute

using AnalysisManagerBase;
using PRISM;
using PRISM.Logging;
using System;
using System.Threading;
using PRISM.FileProcessor;

namespace AnalysisManagerProg
{
    static class Program
    {
        public const string PROGRAM_DATE = "September 6, 2020";

        private static bool mTraceMode;

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        /// <returns> Returns 0 if no error, error code if an error</returns>
        public static int Main(string[] args)
        {
            var osVersionInfo = new OSVersionInfo();

            var osVersion = osVersionInfo.GetOSVersion();
            if (osVersion.IndexOf("windows", StringComparison.OrdinalIgnoreCase) < 0)
            {
                // Running on Linux
                // Auto-enable offline mode
                EnableOfflineMode(true);
            }

            try
            {
                var exeName = System.IO.Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().GetName().Name);

                var cmdLineParser = new CommandLineParser<CommandLineOptions>(exeName,
                    ProcessFilesOrDirectoriesBase.GetAppVersion(PROGRAM_DATE))
                {
                    ProgramInfo = "This program processes DMS analysis jobs for PRISM. Normal operation is to run the program without any command line switches.",
                    ContactInfo =
                        "Program written by Dave Clark, Matthew Monroe, and John Sandoval for the Department of Energy (PNNL, Richland, WA)" +
                        Environment.NewLine +
                        "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                        "Website: https://omics.pnl.gov/ or https://panomics.pnnl.gov/" + Environment.NewLine + Environment.NewLine +
                        "Licensed under the 2-Clause BSD License; you may not use this file except in compliance with the License.  " +
                        "You may obtain a copy of the License at https://opensource.org/licenses/BSD-2-Clause"
                };

                var parsed = cmdLineParser.ParseArgs(args, false);
                var options = parsed.ParsedResults;
                if (args.Length > 0 && !parsed.Success)
                {
                    // Delay for 1500 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                    Thread.Sleep(1500);
                    return -1;
                }

                mTraceMode = options.TraceMode;
                if (!clsGlobal.OfflineMode)
                {
                    if (!clsGlobal.LinuxOS && options.LinuxOSMode)
                        EnableOfflineMode(true);

                    if (!clsGlobal.OfflineMode && options.OfflineMode)
                        EnableOfflineMode();
                }

                ShowTrace("Command line arguments parsed");

                if (options.ShowVersionOnly)
                {
                    DisplayVersion();
                    clsGlobal.IdleLoop(0.5);
                    return 0;
                }

                // Note: CodeTestMode is enabled using command line switch /T
                if (options.CodeTestMode)
                {
                    ShowTrace("Code test mode enabled");

                    var testHarness = new CodeTest();

                    try
                    {
                        // testHarness.SystemMemoryUsage();
                        // testHarness.GenerateScanStatsFile();
                        // testHarness.TestGetToolVersionInfo();
                        // testHarness.TestConnectRSA();
                        // testHarness.TestGZip();
                        // testHarness.TestZipAndUnzip();
                        // testHarness.TestCopyToRemote();

                        // testHarness.TestLogging();
                        // testHarness.TestStatusLogging();

                        // var connString = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI";
                        // testHarness.TestDatabaseLogging(connString);

                        // testHarness.TestGetProcesses();

                        // testHarness.TestCopyToLocalWithHashCheck();

                        testHarness.TestRunQuery();
                    }
                    catch (Exception ex)
                    {
                        LogTools.LogError("Exception calling clsCodeTest", ex);
                    }

                    ShowTrace("Exiting the application");

                    clsParseCommandLine.PauseAtConsole(500);
                    return 0;
                }

                if (options.DisplayDllVersions)
                {
                    var testHarness = new CodeTest();
                    testHarness.DisplayDllVersions(options.DisplayDllPath);
                    clsParseCommandLine.PauseAtConsole();
                    return 0;
                }

                // Initiate automated analysis
                ShowTrace("Instantiating clsMainProcess");

                var mainProcess = new clsMainProcess(options)
                {
                    DisableMessageQueue = options.DisableMessageQueue,
                    DisableMyEMSL = options.DisableMyEMSL,
                    PushRemoteMgrFilesOnly = options.PushRemoteMgrFilesOnly
                };

                var returnCode = mainProcess.Main();

                ShowTrace("Exiting the application");
                FileLogger.FlushPendingMessages();
                return returnCode;
            }
            catch (Exception ex)
            {
                LogTools.LogError("Error occurred in Program->Main", ex);
                clsParseCommandLine.PauseAtConsole(1500);
                FileLogger.FlushPendingMessages();
                return -1;
            }
        }

        private static void DisplayVersion()
        {
            Console.WriteLine();
            Console.WriteLine("DMS Analysis Manager");
            Console.WriteLine("Version " + ProcessFilesOrDirectoriesBase.GetAppVersion(PROGRAM_DATE));
            Console.WriteLine("Host    " + Environment.MachineName);
            Console.WriteLine("User    " + Environment.UserName);
            Console.WriteLine();

            DisplayOSVersion();
        }

        private static void DisplayOSVersion()
        {
            try
            {
                // For this to work properly on Windows 10, you must add a app.manifest file
                // and uncomment the versions of Windows listed below
                // <compatibility xmlns="urn:schemas-microsoft-com:compatibility.v1">
                //
                // See https://stackoverflow.com/a/36158739/1179467

                var osVersionInfo = new OSVersionInfo();
                var osDescription = osVersionInfo.GetOSVersion();

                Console.WriteLine("OS Version: " + osDescription);
            }
            catch (Exception ex)
            {
                LogTools.LogError("Error displaying the OS version", ex);
            }
        }

        /// <summary>
        /// Enable offline mode
        /// </summary>
        /// <remarks>When offline, does not contact any databases or remote shares</remarks>
        private static void EnableOfflineMode()
        {
            clsMainProcess.EnableOfflineMode(clsGlobal.LinuxOS);
        }

        /// <summary>
        /// Enable offline mode
        /// </summary>
        /// <param name="runningLinux">Set to True if running Linux</param>
        /// <remarks>When offline, does not contact any databases or remote shares</remarks>
        private static void EnableOfflineMode(bool runningLinux)
        {
            clsMainProcess.EnableOfflineMode(runningLinux);
        }

        private static void ShowTrace(string message)
        {
            if (mTraceMode)
            {
                clsMainProcess.ShowTraceMessage(message);
            }
        }
    }
}
