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
        public const string PROGRAM_DATE = "June 2, 2017";

        private static bool mCodeTestMode;
        private static bool mCreateWindowsEventLog;
        private static bool mTraceMode;
        private static bool mDisableMessageQueue;
        private static bool mDisableMyEMSL;
        private static bool mDisplayDllVersions;
        private static bool mShowVersionOnly;

        private static string mDisplayDllPath;

        public static int Main()
        {
            // Returns 0 if no error, error code if an error

            var objParseCommandLine = new PRISM.clsParseCommandLine();

            mCodeTestMode = false;
            mTraceMode = false;
            mDisableMessageQueue = false;
            mDisableMyEMSL = false;
            mDisplayDllVersions = false;
            mDisplayDllPath = string.Empty;
            mShowVersionOnly = false;

            if (Path.DirectorySeparatorChar == '/')
            {
                // Running on Linux
                // Auto-enable offline mode
                EnableOfflineMode(true);
            }

            try
            {
                // Look for /T or /Test on the command line
                // If present, this means "code test mode" is enabled
                //
                // Other valid switches are /I, /NoStatus, /T, /Test, /Trace, /EL, /Offline, /Version, /Q, and /?
                //
                if (!objParseCommandLine.ParseCommandLine())
                {
                    Console.WriteLine("Error parsing the command line arguments");
                    return -1;
                }

                var validArgs = SetOptionsUsingCommandLineParameters(objParseCommandLine);

                if (objParseCommandLine.NeedToShowHelp || !validArgs)
                {
                    ShowProgramHelp();
                    return -1;
                }

                if (mTraceMode)
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
                    if (mTraceMode)
                        ShowTraceMessage("Code test mode enabled");

                    var objTest = new clsCodeTest { TraceMode = mTraceMode };

                    try
                    {
                        // objTest.SystemMemoryUsage();
                        // objTest.TestDTASplit();
                        // objTest.TestProteinDBExport(@"C:\DMS_Temp_Org");
                        // objTest.TestDeleteFiles();
                        // objTest.TestLogging();
                        // objTest.GenerateScanStatsFile();
                        // objTest.TestArchiveFailedResults();
                        // objTest.TestGetToolVersionInfo();
                        // objTest.TestConnectRSA();
                        objTest.TestStatusLogging();

                    }
                    catch (Exception ex)
                    {
                        clsGlobal.LogError("clsCodeTest exception", ex);
                    }

                    PRISM.clsParseCommandLine.PauseAtConsole(1500);
                    return 0;
                }

                if (mCreateWindowsEventLog)
                {
                    clsMainProcess.CreateAnalysisManagerEventLog();
                    return 0;
                }

                if (mDisplayDllVersions)
                {
                    var objTest = new clsCodeTest {
                        TraceMode = mTraceMode
                    };
                    objTest.DisplayDllVersions(mDisplayDllPath);
                    PRISM.clsParseCommandLine.PauseAtConsole();
                    return 0;
                }

                // Initiate automated analysis
                if (mTraceMode)
                    ShowTraceMessage("Instantiating clsMainProcess");

                var objDMSMain = new clsMainProcess(mTraceMode)
                {
                    DisableMessageQueue = mDisableMessageQueue,
                    DisableMyEMSL = mDisableMyEMSL
                };

                var intReturnCode = objDMSMain.Main();
                return intReturnCode;
            }
            catch (Exception ex)
            {
                clsGlobal.LogError("Error occurred in modMain->Main: " + Environment.NewLine + ex.Message, ex);
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
        /// <param name="strProgramDate"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        private static string GetAppVersion(string strProgramDate)
        {
            return Assembly.GetExecutingAssembly().GetName().Version + " (" + strProgramDate + ")";
        }

        private static bool SetOptionsUsingCommandLineParameters(PRISM.clsParseCommandLine objParseCommandLine)
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
                if (objParseCommandLine.InvalidParametersPresent(lstValidParameters))
                {
                    ShowErrorMessage("Invalid commmand line parameters",
                        (from item in objParseCommandLine.InvalidParameters(lstValidParameters) select "/" + item).ToList());

                    return false;
                }

                // Query objParseCommandLine to see if various parameters are present

                if (objParseCommandLine.IsParameterPresent("T"))
                    mCodeTestMode = true;
                if (objParseCommandLine.IsParameterPresent("Test"))
                    mCodeTestMode = true;

                if (objParseCommandLine.IsParameterPresent("Trace"))
                    mTraceMode = true;

                if (objParseCommandLine.IsParameterPresent("EL"))
                    mCreateWindowsEventLog = true;

                if (objParseCommandLine.IsParameterPresent("NQ"))
                    mDisableMessageQueue = true;

                if (objParseCommandLine.IsParameterPresent("NoMyEMSL"))
                    mDisableMyEMSL = true;

                if (objParseCommandLine.IsParameterPresent("DLL"))
                {
                    mDisplayDllVersions = true;
                    string strValue;
                    if (objParseCommandLine.RetrieveValueForParameter("DLL", out strValue))
                    {
                        if (!string.IsNullOrWhiteSpace(strValue))
                        {
                            mDisplayDllPath = strValue;
                        }
                    }
                }

                if (!clsGlobal.OfflineMode)
                {
                    if (!clsGlobal.LinuxOS && objParseCommandLine.IsParameterPresent("Linux"))
                        EnableOfflineMode(true);

                    if (!clsGlobal.OfflineMode && objParseCommandLine.IsParameterPresent("Offline"))
                        EnableOfflineMode();
                }

                if (objParseCommandLine.IsParameterPresent("Version"))
                    mShowVersionOnly = true;

                return true;
            }
            catch (Exception ex)
            {
                clsGlobal.LogError("Error parsing the command line parameters: " + Environment.NewLine + ex.Message, ex);
                return false;
            }
        }

        private static void ShowErrorMessage(string strMessage)
        {
            const string strSeparator = "------------------------------------------------------------------------------";

            Console.WriteLine();
            Console.WriteLine(strSeparator);
            Console.WriteLine(strMessage);
            Console.WriteLine(strSeparator);
            Console.WriteLine();

            WriteToErrorStream(strMessage);
        }

        private static void ShowErrorMessage(string strTitle, IEnumerable<string> items)
        {
            const string strSeparator = "------------------------------------------------------------------------------";

            Console.WriteLine();
            Console.WriteLine(strSeparator);
            Console.WriteLine(strTitle);
            var strMessage = strTitle + ":";

            foreach (var item in items)
            {
                Console.WriteLine("   " + item);
                strMessage += " " + item;
            }
            Console.WriteLine(strSeparator);
            Console.WriteLine();

            WriteToErrorStream(strMessage);
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

                PRISM.clsParseCommandLine.PauseAtConsole(1500);
            }
            catch (Exception ex)
            {
                clsGlobal.LogError("Error displaying the program syntax: " + ex.Message, ex);
            }
        }

        private static void WriteToErrorStream(string strErrorMessage)
        {
            try
            {
                using (var swErrorStream = new StreamWriter(Console.OpenStandardError()))
                {
                    swErrorStream.WriteLine(strErrorMessage);
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        public static void ShowTraceMessage(string strMessage)
        {
            clsMainProcess.ShowTraceMessage(strMessage);
        }
    }
}
