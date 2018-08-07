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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;
using PRISM;
using PRISM.Logging;

namespace AnalysisManagerProg
{
    static class modMain
    {
        public const string PROGRAM_DATE = "August 7, 2018";

        private static bool mCodeTestMode;
        private static bool mTraceMode;
        private static bool mDisableMessageQueue;
        private static bool mDisableMyEMSL;
        private static bool mDisplayDllVersions;
        private static bool mPushRemoteMgrFilesOnly;
        private static bool mShowVersionOnly;

        private static string mDisplayDllPath;

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        /// <returns> Returns 0 if no error, error code if an error</returns>
        public static int Main()
        {

            var commandLineParser = new clsParseCommandLine();

            mCodeTestMode = false;
            mTraceMode = false;
            mDisableMessageQueue = false;
            mDisableMyEMSL = false;
            mDisplayDllVersions = false;
            mPushRemoteMgrFilesOnly = false;
            mDisplayDllPath = string.Empty;
            mShowVersionOnly = false;

            var osVersionInfo = new clsOSVersionInfo();

            var osVersion = osVersionInfo.GetOSVersion();
            if (!osVersion.ToLower().Contains("windows"))
            {
                // Running on Linux
                // Auto-enable offline mode
                EnableOfflineMode(true);
            }

            try
            {
                bool validArgs;

                // Look for /T or /Test on the command line
                // If present, this means "code test mode" is enabled
                //
                // Other valid switches are /I, /NoStatus, /T, /Test, /CodeTest, /Trace, /EL, /Offline, /Version, /Q, and /?
                //
                if (commandLineParser.ParseCommandLine())
                {
                    validArgs = SetOptionsUsingCommandLineParameters(commandLineParser);
                }
                else
                {
                    if (commandLineParser.NoParameters)
                    {
                        validArgs = true;
                    }
                    else
                    {
                        if (commandLineParser.NeedToShowHelp)
                        {
                            ShowProgramHelp();
                        }
                        else
                        {
                            ConsoleMsgUtils.ShowWarning("Error parsing the command line arguments");
                            clsParseCommandLine.PauseAtConsole(750);
                        }
                        return -1;
                    }
                }

                if (commandLineParser.NeedToShowHelp || !validArgs)
                {
                    ShowProgramHelp();
                    return -1;
                }

                ShowTraceMessage("Command line arguments parsed");

                if (mShowVersionOnly)
                {
                    DisplayVersion();
                    clsGlobal.IdleLoop(0.5);
                    return 0;
                }

                // Note: CodeTestMode is enabled using command line switch /T
                if (mCodeTestMode)
                {
                    ShowTraceMessage("Code test mode enabled");

                    var testHarness = new clsCodeTest();

                    try
                    {
                        // testHarness.SystemMemoryUsage();
                        // testHarness.TestDTASplit();
                        // testHarness.TestProteinDBExport(@"C:\DMS_Temp_Org");
                        // testHarness.TestDeleteFiles();
                        // testHarness.GenerateScanStatsFile();
                        // testHarness.TestArchiveFailedResults();
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

                        testHarness.TestCopyToLocalWithHashCheck();

                    }
                    catch (Exception ex)
                    {
                        LogTools.LogError("Exception calling clsCodeTest", ex);
                    }

                    ShowTraceMessage("Exiting the application");

                    clsParseCommandLine.PauseAtConsole(500);
                    return 0;
                }

                if (mDisplayDllVersions)
                {
                    var testHarness = new clsCodeTest();
                    testHarness.DisplayDllVersions(mDisplayDllPath);
                    clsParseCommandLine.PauseAtConsole();
                    return 0;
                }

                // Initiate automated analysis
                ShowTraceMessage("Instantiating clsMainProcess");

                var mainProcess = new clsMainProcess(mTraceMode)
                {
                    DisableMessageQueue = mDisableMessageQueue,
                    DisableMyEMSL = mDisableMyEMSL,
                    PushRemoteMgrFilesOnly = mPushRemoteMgrFilesOnly
                };

                var returnCode = mainProcess.Main();

                ShowTraceMessage("Exiting the application");
                FileLogger.FlushPendingMessages();
                return returnCode;
            }
            catch (Exception ex)
            {
                LogTools.LogError("Error occurred in modMain->Main", ex);
                clsParseCommandLine.PauseAtConsole(1500);
                FileLogger.FlushPendingMessages();
                return -1;
            }

        }

        private static void DisplayVersion()
        {
            Console.WriteLine();
            Console.WriteLine("DMS Analysis Manager");
            Console.WriteLine("Version " + GetAppVersion(PROGRAM_DATE));
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

                var osVersionInfo = new clsOSVersionInfo();
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

        /// <summary>
        /// Returns the .NET assembly version followed by the program date
        /// </summary>
        /// <param name="programDate"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private static string GetAppVersion(string programDate)
        {
            return PRISM.FileProcessor.ProcessFilesOrFoldersBase.GetAppVersion(programDate);
        }

        private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine commandLineParser)
        {
            // Returns True if no problems; otherwise, returns false

            var lstValidParameters = new List<string> {
                "T",
                "CodeTest",
                "Test",
                "Trace",
                "Verbose",
                "NQ",
                "NoMyEMSL",
                "DLL",
                "PushRemote",
                "Offline",
                "Linux",
                "V",
                "Version"
            };

            try
            {
                // Make sure no invalid parameters are present
                if (commandLineParser.InvalidParametersPresent(lstValidParameters))
                {
                    ShowErrorMessage("Invalid commmand line parameters",
                        (from item in commandLineParser.InvalidParameters(lstValidParameters) select "/" + item).ToList());

                    return false;
                }

                // Query objParseCommandLine to see if various parameters are present

                if (commandLineParser.IsParameterPresent("T"))
                    mCodeTestMode = true;

                if (commandLineParser.IsParameterPresent("CodeTest"))
                    mCodeTestMode = true;

                if (commandLineParser.IsParameterPresent("Test"))
                    mCodeTestMode = true;

                if (commandLineParser.IsParameterPresent("Trace"))
                    mTraceMode = true;

                if (commandLineParser.IsParameterPresent("Verbose"))
                    mTraceMode = true;

                if (commandLineParser.IsParameterPresent("NQ"))
                    mDisableMessageQueue = true;

                if (commandLineParser.IsParameterPresent("NoMyEMSL"))
                    mDisableMyEMSL = true;

                if (commandLineParser.IsParameterPresent("DLL"))
                {
                    mDisplayDllVersions = true;
                    if (commandLineParser.RetrieveValueForParameter("DLL", out var value))
                    {
                        if (!string.IsNullOrWhiteSpace(value))
                        {
                            mDisplayDllPath = value;
                        }
                    }
                }

                if (commandLineParser.IsParameterPresent("PushRemote"))
                    mPushRemoteMgrFilesOnly = true;

                if (!clsGlobal.OfflineMode)
                {
                    if (!clsGlobal.LinuxOS && commandLineParser.IsParameterPresent("Linux"))
                        EnableOfflineMode(true);

                    if (!clsGlobal.OfflineMode && commandLineParser.IsParameterPresent("Offline"))
                        EnableOfflineMode();
                }

                if (commandLineParser.IsParameterPresent("V"))
                    mShowVersionOnly = true;

                if (commandLineParser.IsParameterPresent("Version"))
                    mShowVersionOnly = true;

                return true;
            }
            catch (Exception ex)
            {
                LogTools.LogError("Error parsing the command line parameters", ex);
                return false;
            }
        }

        private static void ShowErrorMessage(string title, IEnumerable<string> errorMessages)
        {
            ConsoleMsgUtils.ShowErrors(title, errorMessages);
        }

        private static void ShowProgramHelp()
        {
            try
            {
                var exeName = Path.GetFileName(PRISM.FileProcessor.ProcessFilesOrFoldersBase.GetAppPath());

                Console.WriteLine("This program processes DMS analysis jobs for PRISM. Normal operation is to run the program without any command line switches.");
                Console.WriteLine();
                Console.WriteLine("Program syntax:" + Environment.NewLine +
                                  exeName + " [/NQ] [/NoMyEMSL] [/T] [/Trace]");
                Console.WriteLine("[/DLL] [/PushRemote]");
                Console.WriteLine("[/Offline] [/Linux] [/Version]");

                Console.WriteLine();
                Console.WriteLine("Use /NQ to disable posting status messages to the message queue");
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "Use /NoMyEMSL to disable searching for files in MyEMSL. " +
                                      "This is useful if MyEMSL is offline or the current user does not have read access to SimpleSearch"));
                Console.WriteLine();
                Console.WriteLine("Use /T or /Test to start the program in code test mode.");
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "Use /Trace or /Verbose to enable trace mode, where debug messages are written to the command prompt"));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "Use /DLL to display the version of all DLLs in the same folder as this .exe"));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "Use /DLL:Path to display the version of all DLLs in the specified folder (surround path with double quotes if spaces)"));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "Use /PushRemote to use the DMSUpdateManager to push new/updated files to the remote host associated with this manager. " +
                                      "This is only valid if the manager has parameter RunJobsRemotely set to True in the Manager Control DB. " +
                                      "Ignored if /Offline is used."));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "Use /Offline to enable offline mode (database access and use of external servers is disabled). " +
                                      "Requires that the ManagerSettingsLocal.xml file has several settings defined, including LocalTaskQueuePath and LocalWorkDirPath"));
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "Use /Linux to disable access to Windows-specific methods. " +
                                      "Both /Offline and /Linux are auto-enabled if the path separation character is /"));
                Console.WriteLine();
                Console.WriteLine("Use /Version to see the program version and OS version");
                Console.WriteLine();
                Console.WriteLine(ConsoleMsgUtils.WrapParagraph(
                                      "Program written by Dave Clark, Matthew Monroe, and John Sandoval for the Department of Energy (PNNL, Richland, WA)"));
                Console.WriteLine();

                Console.WriteLine("Version: " + GetAppVersion(PROGRAM_DATE));
                Console.WriteLine();

                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov");
                Console.WriteLine("Website: https://omics.pnl.gov/ or https://panomics.pnnl.gov/");
                Console.WriteLine();

                Console.WriteLine("Licensed under the 2-Clause BSD License; you may not use this file except in compliance with the License.  " +
                                  "You may obtain a copy of the License at https://opensource.org/licenses/BSD-2-Clause");
                Console.WriteLine();

                clsParseCommandLine.PauseAtConsole(1500);
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowWarning("Error displaying the program syntax: " + ex.Message);
            }
        }

        private static void ShowTraceMessage(string message)
        {
            if (mTraceMode)
                clsMainProcess.ShowTraceMessage(message);
        }
    }
}
