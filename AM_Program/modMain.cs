// The DMS Analysis Manager program runs automated data analysis for PRISM
//
// -------------------------------------------------------------------------------
// Written by Dave Clark, Matthew Monroe, and John Sandoval for the Department of Energy (PNNL, Richland, WA)
//
// E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com
// Website: http://omics.pnl.gov/ or http://www.sysbio.org/resources/staff/ or http://panomics.pnnl.gov/
// -------------------------------------------------------------------------------
//
// Licensed under the Apache License, Version 2.0; you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Notice: This computer software was prepared by Battelle Memorial Institute,
// hereinafter the Contractor, under Contract No. DE-AC05-76RL0 1830 with the
// Department of Energy (DOE).  All rights in the computer software are reserved
// by DOE on behalf of the United States Government and the Contractor as
// provided in the Contract.  NEITHER THE GOVERNMENT NOR THE CONTRACTOR MAKES ANY
// WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS
// SOFTWARE.  This notice including this sentence must appear on any copies of
// this computer software.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using AnalysisManagerBase;
using PRISM;

namespace AnalysisManagerProg
{
    static class modMain
    {
        public const string PROGRAM_DATE = "January 25, 2018";

        private static bool mCodeTestMode;
        private static bool mCreateWindowsEventLog;
        private static bool mTraceMode;
        private static bool mDisableMessageQueue;
        private static bool mDisableMyEMSL;
        private static bool mDisplayDllVersions;
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
            mDisplayDllPath = string.Empty;
            mShowVersionOnly = false;

            var osVersionInfo = new clsOSVersionInfo();

            var osVersion = osVersionInfo.GetOSVersion();
            if (osVersion.IndexOf("windows", StringComparison.OrdinalIgnoreCase) < 0)
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
                // Other valid switches are /I, /NoStatus, /T, /Test, /Trace, /EL, /Offline, /Version, /Q, and /?
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
                            Console.WriteLine("Error parsing the command line arguments");
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
                    Thread.Sleep(500);
                    return 0;
                }

                // Note: CodeTestMode is enabled using command line switch /T

                if (mCodeTestMode)
                {
                    ShowTraceMessage("Code test mode enabled");

                    var testHarness = new clsCodeTest { TraceMode = mTraceMode };

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
                        // testHarness.TestZipAndUnzip();
                        // testHarness.TestCopyToRemote();

                        testHarness.TestLogging();
                        //testHarness.TestStatusLogging();

                        //var connString = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI";
                        //testHarness.TestDatabaseLogging(connString);


                    }
                    catch (Exception ex)
                    {
                        clsGlobal.LogError("clsCodeTest exception", ex);
                    }

                    ShowTraceMessage("Exiting application");

                    clsParseCommandLine.PauseAtConsole(500);
                    return 0;
                }

                if (mCreateWindowsEventLog)
                {
                    clsMainProcess.CreateAnalysisManagerEventLog();
                    return 0;
                }

                if (mDisplayDllVersions)
                {
                    var testHarness = new clsCodeTest {
                        TraceMode = mTraceMode
                    };
                    testHarness.DisplayDllVersions(mDisplayDllPath);
                    clsParseCommandLine.PauseAtConsole();
                    return 0;
                }

                // Initiate automated analysis
                ShowTraceMessage("Instantiating clsMainProcess");

                var mainProcess = new clsMainProcess(mTraceMode)
                {
                    DisableMessageQueue = mDisableMessageQueue,
                    DisableMyEMSL = mDisableMyEMSL
                };

                var returnCode = mainProcess.Main();

                return returnCode;
            }
            catch (Exception ex)
            {
                clsGlobal.LogError("Error occurred in modMain->Main: " + Environment.NewLine + ex.Message, ex);
                ShowErrorMessage("Error occurred in modMain->Main: " + ex.Message);
                clsParseCommandLine.PauseAtConsole(1500);
                return -1;
            }

        }

        private static void DisplayVersion()
        {
            Console.WriteLine();
            Console.WriteLine("DMS Analysis Manager");
            Console.WriteLine("Version " + GetAppVersion(PROGRAM_DATE));
            Console.WriteLine();

            DisplayOSVersion();

        }

        private static void DisplayOSVersion()
        {

            try
            {
                var osVersionInfo = new clsOSVersionInfo();
                var osDescription = osVersionInfo.GetOSVersion();

                Console.WriteLine("OS Version: " + osDescription);

            }
            catch (Exception ex)
            {
                clsGlobal.LogError("Error displaying the OS version: " + Environment.NewLine + ex.Message, ex);
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

        private static string GetAppPath()
        {
            return Assembly.GetExecutingAssembly().Location;
        }

        /// <summary>
        /// Returns the .NET assembly version followed by the program date
        /// </summary>
        /// <param name="programDate"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private static string GetAppVersion(string programDate)
        {
            return Assembly.GetExecutingAssembly().GetName().Version + " (" + programDate + ")";
        }

        private static bool SetOptionsUsingCommandLineParameters(clsParseCommandLine commandLineParser)
        {
            // Returns True if no problems; otherwise, returns false

            var lstValidParameters = new List<string> {
                "T",
                "Test",
                "Trace",
                "EL",
                "NQ",
                "NoMyEMSL",
                "DLL",
                "Offline",
                "Linux",
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
                if (commandLineParser.IsParameterPresent("Test"))
                    mCodeTestMode = true;

                if (commandLineParser.IsParameterPresent("Trace"))
                    mTraceMode = true;

                if (objParseCommandLine.IsParameterPresent("EL"))
                    mCreateWindowsEventLog = true;

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

                if (!clsGlobal.OfflineMode)
                {
                    if (!clsGlobal.LinuxOS && commandLineParser.IsParameterPresent("Linux"))
                        EnableOfflineMode(true);

                    if (!clsGlobal.OfflineMode && commandLineParser.IsParameterPresent("Offline"))
                        EnableOfflineMode();
                }

                if (commandLineParser.IsParameterPresent("Version"))
                    mShowVersionOnly = true;

                return true;
            }
            catch (Exception ex)
            {
                clsGlobal.LogError("Error parsing the command line parameters: " + Environment.NewLine + ex.Message, ex);
                return false;
            }
        }

        private static void ShowErrorMessage(string message)
        {
            ConsoleMsgUtils.ShowError(message);
        }

        private static void ShowErrorMessage(string title, IEnumerable<string> errorMessages)
        {
            ConsoleMsgUtils.ShowErrors(title, errorMessages);
        }

        private static void ShowProgramHelp()
        {
            try
            {
                var exeName = Path.GetFileName(GetAppPath());

                Console.WriteLine("This program processes DMS analysis jobs for PRISM. Normal operation is to run the program without any command line switches.");
                Console.WriteLine();
                Console.WriteLine("Program syntax:\n" + exeName + " [/EL] [/NQ] [/NoMyEMSL] [/T] [/Trace]");
                Console.WriteLine("[/DLL] [/Offline] [/Linux] [/Version]");
                Console.WriteLine();

                Console.WriteLine("Use /EL to create the Windows Event Log named '" + clsMainProcess.CUSTOM_LOG_NAME + "' then exit the program. " +
                                  "You should do this from a Windows Command Prompt that you started using 'Run as Administrator'");
                Console.WriteLine();
                Console.WriteLine("Use /NQ to disable posting status messages to the message queue");
                Console.WriteLine();
                Console.WriteLine("Use /NoMyEMSL to disable searching for files in MyEMSL. This is useful if MyEMSL is offline or the current user does not have read access to SimpleSearch");
                Console.WriteLine();
                Console.WriteLine("Use /T or /Test to start the program in code test mode.");
                Console.WriteLine();
                Console.WriteLine("Use /Trace to enable trace mode, where debug messages are written to the command prompt");
                Console.WriteLine();
                Console.WriteLine("Use /DLL to display the version of all DLLs in the same folder as this .exe");
                Console.WriteLine("Use /DLL:Path to display the version of all DLLs in the specified folder (surround path with double quotes if spaces)");
                Console.WriteLine();

                Console.WriteLine("Use /Offline to enable offline mode (database access and use of external servers is disabled). " +
                                  "Requires that the ManagerSettingsLocal.xml file has several settings defined, including LocalTaskQueuePath and LocalWorkDirPath");
                Console.WriteLine();
                Console.WriteLine("Use /Linux to disable access to Windows-specific methods. " +
                                  "Both /Offline and /Linux are auto-enabled if the path separation character is /");
                Console.WriteLine();
                Console.WriteLine("Use /Version to see the program version and OS version");
                Console.WriteLine();
                Console.WriteLine("Program written by Dave Clark, Matthew Monroe, and John Sandoval for the Department of Energy (PNNL, Richland, WA)");
                Console.WriteLine();

                Console.WriteLine("Version: " + GetAppVersion(PROGRAM_DATE));
                Console.WriteLine();

                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com");
                Console.WriteLine("Website: http://omics.pnl.gov/ or http://panomics.pnnl.gov/");
                Console.WriteLine();

                Console.WriteLine("Licensed under the Apache License, Version 2.0; you may not use this file except in compliance with the License.  " +
                                  "You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0");
                Console.WriteLine();

                clsParseCommandLine.PauseAtConsole(1500);
            }
            catch (Exception ex)
            {
                clsGlobal.LogError("Error displaying the program syntax: " + ex.Message, ex);
            }
        }

        private static void ShowTraceMessage(string message)
        {
            if (mTraceMode)
                clsMainProcess.ShowTraceMessage(message);
        }
    }
}
