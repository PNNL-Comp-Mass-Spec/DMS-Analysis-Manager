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

namespace AnalysisManagerProg
{
    static class modMain
    {
        public const string PROGRAM_DATE = "February 3, 2017";
        private static bool mCodeTestMode;
        private static bool mCreateWindowsEventLog;
        private static bool mTraceMode;
        private static bool mDisableMessageQueue;
        private static bool mDisableMyEMSL;
        private static bool mDisplayDllVersions;

        private static string mDisplayDllPath;
        public static int Main()
        {
            // Returns 0 if no error, error code if an error

            int intReturnCode = 0;
            clsParseCommandLine objParseCommandLine = new clsParseCommandLine();

            intReturnCode = 0;
            mCodeTestMode = false;
            mTraceMode = false;
            mDisableMessageQueue = false;
            mDisableMyEMSL = false;
            mDisplayDllVersions = false;
            mDisplayDllPath = "";

            try
            {
                // Look for /T or /Test on the command line
                // If present, this means "code test mode" is enabled
                //
                // Other valid switches are /I, /NoStatus, /T, /Test, /Trace, /EL, /Q, and /?
                //
                if (objParseCommandLine.ParseCommandLine())
                {
                    SetOptionsUsingCommandLineParameters(objParseCommandLine);
                }

                if (objParseCommandLine.NeedToShowHelp)
                {
                    ShowProgramHelp();
                    intReturnCode = -1;
                }
                else
                {
                    if (mTraceMode)
                        ShowTraceMessage("Command line arguments parsed");

                    // Note: CodeTestMode is enabled using command line switch /T

                    if (mCodeTestMode)
                    {
                        if (mTraceMode)
                            ShowTraceMessage("Code test mode enabled");

                        clsCodeTest objTest = new clsCodeTest { TraceMode = mTraceMode };

                        try
                        {
                            //objTest.TestFileDateConversion()
                            //objTest.TestArchiveFileStart()
                            //objTest.TestDTASplit()
                            //objTest.TestUncat("Cyano_Nitrogenase_BU_1_12Apr12_Earth_12-03-24", "F:\Temp\Deconcat")
                            //objTest.TestFileSplitThenCombine()
                            //objTest.TestResultsTransfer()
                            //objTest.TestDeliverResults()
                            //objTest.TestGetFileContents()

                            //objTest.FixICR2LSResultFileNames("E:\DMS_WorkDir", "Test")
                            //objTest.TestFindAndReplace()

                            //objTest.TestProgRunner()
                            //objTest.TestUnzip("f:\'temp\QC_Shew_500_100_fr720_c2_Ek_0000_isos.zip", "f:\temp")

                            //objTest.CheckETDModeEnabledXTandem("input.xml", False)
                            //objTest.TestDTAWatcher("E:\DMS_WorkDir", 5)

                            //objTest.TestProteinDBExport("C:\DMS_Temp_Org")

                            //objTest.TestFindFile()
                            //objTest.TestDeleteFiles()

                            //objTest.TestZipAndUnzip()
                            //objTest.TestMALDIDataUnzip("")

                            //objTest.TestMSGFResultsSummarizer()

                            //objTest.TestProgRunnerIDPicker()

                            //objTest.TestProteinDBExport("c:\dms_temp_org")

                            //objTest.PerformanceCounterTest()
                            //objTest.SystemMemoryUsage()

                            // objTest.TestIonicZipTools()

                            //objTest.RemoveSparseSpectra()

                            // objTest.ProcessDtaRefineryLogFiles()

                            //objTest.TestZip()
                            //objTest.TestGZip()

                            //objTest.ConvertZipToGZip("F:\Temp\GZip\Diabetes_iPSC_KO2_TMT_NiNTA_04_21Oct13_Pippin_13-06-18_msgfplus.zip")

                            //objTest.TestRunQuery()
                            //objTest.TestRunSP()

                            //objTest.ValidateCentroided()

                            //Console.WriteLine(clsGlobal.DecodePassword("Test"))

                            //Console.WriteLine(clsGlobal.UpdateHostName("\\winhpcfs\Projects\dms", "\\picfs.pnl.gov\"))

                            //objTest.TestCosoleOutputParsing()
                            // objTest.TestMSXmlCachePurge()

                            // Dim testLogger = New PRISM.Logging.clsDBLogger()
                            // Console.WriteLine(testLogger.MachineName)

                            // objTest.TestGetVersionInfo()

                            // objTest.ParseMSPathFinderConsoleOutput()

                            // objTest.ParseMSGFDBConsoleOutput()

                            // objTest.RunMSConvert()

                            // objTest.GetLegacyFastaFileSize()

                            // objTest.GenerateScanStatsFile()

                            objTest.TestLogging();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(clsGlobal.GetExceptionStackTrace(ex, true));
                        }

                        return 0;
                    }
                    else if (mCreateWindowsEventLog)
                    {
                        clsMainProcess.CreateAnalysisManagerEventLog();
                    }
                    else if (mDisplayDllVersions)
                    {
                        clsCodeTest objTest = new clsCodeTest();
                        objTest.TraceMode = mTraceMode;
                        objTest.DisplayDllVersions(mDisplayDllPath);
                    }
                    else
                    {
                        // Initiate automated analysis
                        if (mTraceMode)
                            ShowTraceMessage("Instantiating clsMainProcess");

                        var objDMSMain = new clsMainProcess(mTraceMode);
                        objDMSMain.DisableMessageQueue = mDisableMessageQueue;
                        objDMSMain.DisableMyEMSL = mDisableMyEMSL;

                        intReturnCode = objDMSMain.Main();
                    }
                }
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error occurred in modMain->Main: " + Environment.NewLine + ex.Message);
                Console.WriteLine(clsGlobal.GetExceptionStackTrace(ex, true));
                intReturnCode = -1;
            }

            return intReturnCode;
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
            return Assembly.GetExecutingAssembly().GetName().Version.ToString() + " (" + strProgramDate + ")";
        }

        private static void SetOptionsUsingCommandLineParameters(clsParseCommandLine objParseCommandLine)
        {
            // Returns True if no problems; otherwise, returns false

            string strValue = string.Empty;
            var lstValidParameters = new List<string> {
            "T",
            "Test",
            "Trace",
            "EL",
            "NQ",
            "NoMyEMSL",
            "DLL"
        };

            try
            {
                // Make sure no invalid parameters are present
                if (objParseCommandLine.InvalidParametersPresent(lstValidParameters))
                {
                    ShowErrorMessage("Invalid commmand line parameters", (from item in objParseCommandLine.InvalidParameters(lstValidParameters) select "/" + item).ToList());
                }
                else
                {
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
                        if (objParseCommandLine.RetrieveValueForParameter("DLL", out strValue))
                        {
                            if (!string.IsNullOrWhiteSpace(strValue))
                            {
                                mDisplayDllPath = strValue;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error parsing the command line parameters: " + Environment.NewLine + ex.Message);
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
            string strMessage = null;

            Console.WriteLine();
            Console.WriteLine(strSeparator);
            Console.WriteLine(strTitle);
            strMessage = strTitle + ":";

            foreach (string item in items)
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
                Console.WriteLine("This program processes DMS analysis jobs for PRISM. Normal operation is to run the program without any command line switches.");
                Console.WriteLine();
                Console.WriteLine("Program syntax:\n" + Path.GetFileName(GetAppPath()) + " [/EL] [/NQ] [/NoMyEMSL] [/T] [/Trace] [/DLL]");
                Console.WriteLine();

                Console.WriteLine("Use /EL to create the Windows Event Log named '" + clsMainProcess.CUSTOM_LOG_NAME + "' then exit the program.  You should do this from a Windows Command Prompt that you started using 'Run as Administrator'");
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

                Console.WriteLine("Program written by Dave Clark, Matthew Monroe, and John Sandoval for the Department of Energy (PNNL, Richland, WA)");
                Console.WriteLine();

                Console.WriteLine("Version: " + GetAppVersion(PROGRAM_DATE));
                Console.WriteLine();

                Console.WriteLine("E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com");
                Console.WriteLine("Website: http://omics.pnl.gov/ or http://panomics.pnnl.gov/");
                Console.WriteLine();

                Console.WriteLine("Licensed under the Apache License, Version 2.0; you may not use this file except in compliance with the License.  " + "You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0");
                Console.WriteLine();

                Console.WriteLine("Notice: This computer software was prepared by Battelle Memorial Institute, " + "hereinafter the Contractor, under Contract No. DE-AC05-76RL0 1830 with the " + "Department of Energy (DOE).  All rights in the computer software are reserved " + "by DOE on behalf of the United States Government and the Contractor as " + "provided in the Contract.  NEITHER THE GOVERNMENT NOR THE CONTRACTOR MAKES ANY " + "WARRANTY, EXPRESS OR IMPLIED, OR ASSUMES ANY LIABILITY FOR THE USE OF THIS " + "SOFTWARE.  This notice including this sentence must appear on any copies of " + "this computer software.");

                // Delay for 750 msec in case the user double clicked this file from within Windows Explorer (or started the program via a shortcut)
                Thread.Sleep(750);
            }
            catch (Exception ex)
            {
                ShowErrorMessage("Error displaying the program syntax: " + ex.Message);
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
            catch (Exception ex)
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
