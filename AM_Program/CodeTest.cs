﻿using AnalysisManagerBase;
using PRISM;
using PRISM.AppSettings;
using PRISM.Logging;
using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Versioning;
using System.Text;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.OfflineJobs;
using AnalysisManagerBase.StatusReporting;
using Cyclops;
using PRISMDatabaseUtils;

namespace AnalysisManagerProg
{
    /// <summary>
    /// Collection of test code
    /// </summary>
    public class CodeTest : LoggerBase
    {
        // ReSharper disable CommentTypo
        // Ignore Spelling: perf, dta, Inj, mgf, dmsarch, Geobacter, bemidjiensis, Bem, lovelyi, metallireducens, sp, filetype, Micrococcus, luteus, nr
        // Ignore Spelling: const, bool, Qonvert, msgfspecprob, pek, Archaea, Sprot, Trembl, yyyy, dd, hh, ss, tt, Mam, gimli, Rar, Pos
        // ReSharper restore CommentTypo

        private OrganismDatabaseHandler.ProteinExport.GetFASTAFromDMS mFastaTools;
        private bool mGenerationComplete;
        private readonly string mFastaToolsCnStr = "Data Source=proteinseqs;Initial Catalog=Protein_Sequences;Integrated Security=SSPI;";
        private string mFastaFileName = string.Empty;
        private System.Timers.Timer mFastaTimer;

        private bool mFastaGenTimeOut;

        private readonly IMgrParams mMgrSettings;

        private DateTime mLastStatusTime;

        private RunDosProgram mProgRunner;

        // 450 seconds is 7.5 minutes
        private const int FASTA_GEN_TIMEOUT_INTERVAL_SEC = 450;

        /// <summary>
        /// Constructor
        /// </summary>
        public CodeTest()
        {
            const bool TRACE_MODE_ENABLED = true;

            mDebugLevel = 2;
            mLastStatusTime = DateTime.UtcNow.AddMinutes(-1);

            try
            {
                // Load settings from config file AnalysisManagerProg.exe.config
                var options = new CommandLineOptions {TraceMode = TRACE_MODE_ENABLED};

                var mainProcess = new MainProcess(options);
                mainProcess.InitMgrSettings(false);

                var configFileSettings = mainProcess.LoadMgrSettingsFromFile();

                mMgrSettings = new AnalysisMgrSettings(Global.GetAppDirectoryPath(), TRACE_MODE_ENABLED);
                var settingsClass = (AnalysisMgrSettings)mMgrSettings;
                if (settingsClass != null)
                {
                    RegisterEvents(settingsClass);
                    settingsClass.CriticalErrorEvent += CriticalErrorEvent;
                }

                var success = mMgrSettings.LoadSettings(configFileSettings);
                if (!success)
                    return;

                mDebugLevel = 2;

                // Initialize the log file
                var logFileNameBase = MainProcess.GetBaseLogFileName(mMgrSettings);

                // The analysis manager determines when to log or not log based on internal logic
                // Set the LogLevel tracked by FileLogger to DEBUG so that all messages sent to the class are logged
                LogTools.CreateFileLogger(logFileNameBase, BaseLogger.LogLevels.DEBUG);

                if (Global.LinuxOS)
                    mMgrSettings.SetParam(AnalysisMgrSettings.MGR_PARAM_WORK_DIR, mMgrSettings.GetParam(AnalysisMgrSettings.MGR_PARAM_LOCAL_WORK_DIR_PATH));
                else
                    mMgrSettings.SetParam(AnalysisMgrSettings.MGR_PARAM_WORK_DIR, @"C:\DMS_WorkDir");

                mMgrSettings.SetParam(MgrSettings.MGR_PARAM_MGR_NAME, "Monroe_Test");
                mMgrSettings.SetParam("DebugLevel", mDebugLevel.ToString());
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Exception loading settings from AnalysisManagerProg.exe.config", ex);
                Global.IdleLoop(0.5);
            }
        }

        /// <summary>
        /// Display the version of all DLLs in the application folder, including the .NET framework that they were compiled against
        /// </summary>
        /// <param name="displayDllPath"></param>
        /// <param name="fileNameFileSpec"></param>
        public void DisplayDllVersions(string displayDllPath, string fileNameFileSpec = "*.dll")
        {
            try
            {
                DirectoryInfo sourceDirectory;

                if (string.IsNullOrWhiteSpace(displayDllPath))
                {
                    sourceDirectory = new DirectoryInfo(".");
                }
                else
                {
                    sourceDirectory = new DirectoryInfo(displayDllPath);
                }

                List<FileInfo> filesToVersion;
                if (string.IsNullOrWhiteSpace(fileNameFileSpec))
                {
                    filesToVersion = sourceDirectory.GetFiles("*.dll").ToList();
                }
                else
                {
                    filesToVersion = sourceDirectory.GetFiles(fileNameFileSpec).ToList();
                }

                var results = new Dictionary<string, KeyValuePair<string, string>>(StringComparer.OrdinalIgnoreCase);
                var errors = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

                Console.WriteLine("Obtaining versions for " + filesToVersion.Count + " files");

                foreach (var currentFile in filesToVersion)
                {
                    try
                    {
                        Console.Write(".");

                        var fileAssembly = Assembly.LoadFrom(currentFile.FullName);
                        var fileVersion = fileAssembly.ImageRuntimeVersion;
                        var frameworkVersion = "??";

                        var customAttributes = fileAssembly.GetCustomAttributes(typeof(TargetFrameworkAttribute)).ToList();
                        if (customAttributes?.Count > 0)
                        {
                            var frameworkAttribute = (TargetFrameworkAttribute)customAttributes.First();
                            frameworkVersion = frameworkAttribute.FrameworkDisplayName;
                        }
                        else if (fileVersion.StartsWith("v1.", StringComparison.OrdinalIgnoreCase) ||
                                 fileVersion.StartsWith("v2.", StringComparison.OrdinalIgnoreCase))
                        {
                            frameworkVersion = string.Empty;
                        }

                        if (results.ContainsKey(currentFile.FullName))
                        {
                            Console.WriteLine("Skipping duplicate file: " + currentFile.Name + ", " + fileVersion + " and " + frameworkVersion);
                        }
                        else
                        {
                            results.Add(currentFile.FullName, new KeyValuePair<string, string>(fileVersion, frameworkVersion));
                        }
                    }
                    catch (BadImageFormatException ex)
                    {
                        // This may have been a .NET DLL missing a dependency
                        // Try a reflection-only load

                        try
                        {
                            var fileAssembly2 = Assembly.ReflectionOnlyLoadFrom(currentFile.FullName);
                            var fileVersion2 = fileAssembly2.ImageRuntimeVersion;

                            if (results.ContainsKey(currentFile.FullName))
                            {
                                Console.WriteLine("Skipping duplicate file: " + currentFile.Name + ", " + fileVersion2 + " (missing dependencies)");
                            }
                            else
                            {
                                results.Add(currentFile.FullName, new KeyValuePair<string, string>(fileVersion2, "Unknown, missing dependencies"));
                            }
                        }
                        catch (Exception ex2)
                        {
                            if (errors.ContainsKey(currentFile.FullName))
                            {
                                Console.WriteLine("Skipping duplicate error: " + currentFile.Name + ": " + ex2.Message);
                            }
                            else
                            {
                                errors.Add(currentFile.FullName, ex.Message);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        if (errors.ContainsKey(currentFile.FullName))
                        {
                            Console.WriteLine("Skipping duplicate error: " + currentFile.Name + ": " + ex.Message);
                        }
                        else
                        {
                            errors.Add(currentFile.FullName, ex.Message);
                        }
                    }
                }

                Console.WriteLine();
                Console.WriteLine();

                var query = (from item in results orderby item.Key select item).ToList();

                Console.WriteLine("{0,-50} {1,-20} {2}", "Filename", ".NET Version", "Target Framework");
                foreach (var result in query)
                {
                    Console.WriteLine("{0,-50} {1,-20} {2}", Path.GetFileName(result.Key), " " + result.Value.Key, result.Value.Value);
                }

                if (errors.Count > 0)
                {
                    Console.WriteLine();
                    Console.WriteLine();
                    Console.WriteLine("DLLs likely not .NET");

                    var errorList = (from item in errors orderby item.Key select item).ToList();

                    Console.WriteLine("{0,-30} {1}", "Filename", "Error");

                    foreach (var result in errorList)
                    {
                        Console.Write("{0,-30} ", Path.GetFileName(result.Key));
                        var startIndex = 0;
                        while (startIndex < result.Value.Length)
                        {
                            if (startIndex > 0)
                            {
                                Console.Write("{0,-30} ", string.Empty);
                            }

                            if (startIndex + 80 > result.Value.Length)
                            {
                                Console.WriteLine(result.Value.Substring(startIndex, result.Value.Length - startIndex));
                                break;
                            }

                            Console.WriteLine(result.Value.Substring(startIndex, 80));

                            startIndex += 80;
                        }

                        Console.WriteLine();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error finding files to check: " + ex.Message);
            }
        }

        private AnalysisJob InitializeMgrAndJobParams(short debugLevel)
        {
            var jobParams = new AnalysisJob(mMgrSettings, debugLevel);

            mMgrSettings.SetParam(AnalysisMgrSettings.MGR_PARAM_WORK_DIR, GetWorkDirPath());
            mMgrSettings.SetParam(MgrSettings.MGR_PARAM_MGR_NAME, "Monroe_Test");
            mMgrSettings.SetParam("DebugLevel", debugLevel.ToString());

            jobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "StepTool", "TestStepTool");
            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "ToolName", "TestTool");

            jobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "Job", "12345");
            jobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME, "Test_Results");

            return jobParams;
        }

        private CodeTestAM GetCodeTestToolRunner()
        {
            return GetCodeTestToolRunner(out _, out _);
        }

        private CodeTestAM GetCodeTestToolRunner(out AnalysisJob jobParams)
        {
            return GetCodeTestToolRunner(out jobParams, out _);
        }

        private CodeTestAM GetCodeTestToolRunner(out AnalysisJob jobParams, out MyEMSLUtilities myEMSLUtilities)
        {
            const short DEBUG_LEVEL = 2;

            jobParams = InitializeMgrAndJobParams(DEBUG_LEVEL);

            var statusTools = new StatusFile("Status.xml", DEBUG_LEVEL);
            RegisterEvents(statusTools);

            var summaryFile = new SummaryFile();

            myEMSLUtilities = new MyEMSLUtilities(DEBUG_LEVEL, GetWorkDirPath(), true);
            RegisterEvents(myEMSLUtilities);

            var toolRunner = new CodeTestAM();
            toolRunner.Setup("CodeTest", mMgrSettings, jobParams, statusTools, summaryFile, myEMSLUtilities);

            return toolRunner;
        }

        private ResourceTestClass GetResourcesObject(int debugLevel)
        {
            var jobParams = new AnalysisJob(mMgrSettings, 0);

            return GetResourcesObject(debugLevel, jobParams);
        }

        private ResourceTestClass GetResourcesObject(int debugLevel, IJobParams jobParams)
        {
            var resourceTester = new ResourceTestClass();

            var statusTools = new StatusFile("Status.xml", debugLevel);
            RegisterEvents(statusTools);

            var myEMSLUtilities = new MyEMSLUtilities(debugLevel, GetWorkDirPath(), true);
            RegisterEvents(myEMSLUtilities);

            mMgrSettings.SetParam(AnalysisMgrSettings.MGR_PARAM_WORK_DIR, GetWorkDirPath());
            mMgrSettings.SetParam(MgrSettings.MGR_PARAM_MGR_NAME, "Monroe_Test");
            mMgrSettings.SetParam("DebugLevel", debugLevel.ToString());

            jobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "StepTool", "TestStepTool");
            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "ToolName", "TestTool");

            jobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "Job", "12345");
            jobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME, "Test_Results");

            resourceTester.Setup("CodeTest", mMgrSettings, jobParams, statusTools, myEMSLUtilities);

            return resourceTester;
        }

        private string GetWorkDirPath()
        {
            return mMgrSettings.GetParam(AnalysisMgrSettings.MGR_PARAM_WORK_DIR);
        }

        /// <summary>
        /// Initializes mMgrSettings and returns example job params
        /// </summary>
        private AnalysisJob InitializeManagerParams()
        {
            const int debugLevel = 1;

            var jobParams = new AnalysisJob(mMgrSettings, 0);

            mMgrSettings.SetParam(AnalysisMgrSettings.MGR_PARAM_WORK_DIR, @"C:\DMS_WorkDir");
            mMgrSettings.SetParam(MgrSettings.MGR_PARAM_MGR_NAME, "Monroe_Test");
            mMgrSettings.SetParam("DebugLevel", debugLevel.ToString());

            jobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "StepTool", "TestStepTool");
            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "ToolName", "TestTool");

            jobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "Job", "12345");
            jobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME, "Test_Results");

            return jobParams;
        }

        /// <summary>
        /// Instantiate the CPU Usage and Free Memory performance counters
        /// </summary>
        public void PerformanceCounterTest()
        {
            try
            {
                // Note that the Memory and Processor performance monitor categories are not
                // available on Windows instances running under VMWare on PIC
                // Console.WriteLine("Performance monitor categories")
                // Dim perfCats As PerformanceCounterCategory() = PerformanceCounterCategory.GetCategories()
                // For Each category As PerformanceCounterCategory In perfCats.OrderBy(Function(c) c.CategoryName)
                //    Console.WriteLine("Category Name: {0}", category.CategoryName)
                // Next
                // Console.WriteLine()

                var mCPUUsagePerformanceCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total")
                {
                    ReadOnly = true
                };

                var mFreeMemoryPerformanceCounter = new PerformanceCounter("Memory", "Available MBytes") { ReadOnly = true };
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine("Error in PerformanceCounterTest: " + ex.Message);
                Console.WriteLine(Global.GetExceptionStackTrace(ex, true));
                var rePub1000 = new Regex(@"Pub-1\d{3,}", RegexOptions.IgnoreCase);
                if (rePub1000.IsMatch(Environment.MachineName))
                {
                    Console.WriteLine("This is a known issue with Windows instances running under VMWare on PIC");
                }
            }
        }

        /// <summary>
        /// Process a DTA Refinery log files for a range of jobs
        /// </summary>
        public void ProcessDtaRefineryLogFiles()
        {
            // ProcessDtaRefineryLogFiles(968057, 968057)
            // ProcessDtaRefineryLogFiles(968061, 968061)
            // ProcessDtaRefineryLogFiles(968094, 968094)
            // ProcessDtaRefineryLogFiles(968102, 968102)
            // ProcessDtaRefineryLogFiles(968106, 968106)

            // ProcessDtaRefineryLogFiles(968049, 968049)
            // ProcessDtaRefineryLogFiles(968053, 968053)
            // ProcessDtaRefineryLogFiles(968098, 968098)
            ProcessDtaRefineryLogFiles(968470, 968470);
            ProcessDtaRefineryLogFiles(968482, 968482);
        }

        private bool ProcessDtaRefineryLogFiles(int jobStart, int jobEnd)
        {
            // Query the Pipeline DB to find jobs that ran DTA Refinery

            var sql =
                "SELECT JS.Dataset, J.Dataset_ID, JS.Job, JS.Output_Folder, DFP.Dataset_Folder_Path, JS.Transfer_Folder_Path" +
                " FROM DMS_Pipeline.dbo.V_Job_Steps JS INNER JOIN" +
                "      DMS_Pipeline.dbo.T_Jobs J ON JS.Job = J.Job INNER JOIN" +
                "      DMS5.dbo.V_Dataset_Folder_Paths DFP ON J.Dataset_ID = DFP.Dataset_ID" +
                " WHERE (JS.Job Between " + jobStart + " and " + jobEnd + ") AND (JS.Tool = 'DTA_Refinery') AND (JS.State = 5)";

            const string connectionString = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;";
            const short RetryCount = 2;

            var dbTools = DbToolsFactory.GetDBTools(connectionString, debugMode: true);
            RegisterEvents(dbTools);

            var success = dbTools.GetQueryResultsDataTable(sql, out var Dt, RetryCount);

            if (!success)
            {
                Console.WriteLine("Repeated errors running database query");
            }

            if (Dt.Rows.Count < 1)
            {
                // No data was returned
                Console.WriteLine("DTA_Refinery jobs were not found for job range " + jobStart + " - " + jobEnd);
                return false;
            }

            // var workDir = mMgrSettings.GetParam(AnalysisMgrSettings.MGR_PARAM_WORK_DIR);
            // var postResultsToDB = true;

            // Note: add file DtaRefLogMassErrorExtractor to this project to use this functionality
            // var massErrorExtractor = new DtaRefLogMassErrorExtractor(mMgrSettings, workDir, mDebugLevel, postResultsToDB);

            foreach (DataRow curRow in Dt.Rows)
            {
                var dataset = curRow["Dataset"].CastDBVal<string>();
                var datasetID = curRow["Dataset_ID"].CastDBVal<int>();
                var job = curRow["Job"].CastDBVal<int>();
                var dtaRefineryDataFolderPath = Path.Combine(curRow["Dataset_Folder_Path"].CastDBVal<string>(),
                                                             curRow["Output_Folder"].CastDBVal<string>());

                if (!Directory.Exists(dtaRefineryDataFolderPath))
                {
                    dtaRefineryDataFolderPath = Path.Combine(curRow["Transfer_Folder_Path"].CastDBVal<string>(), curRow["Output_Folder"].CastDBVal<string>());
                }

                if (Directory.Exists(dtaRefineryDataFolderPath))
                {
                    Console.WriteLine("Processing " + dtaRefineryDataFolderPath);
                    // massErrorExtractor.ParseDTARefineryLogFile(udtPSMJob.Dataset, udtPSMJob.DatasetID, udtPSMJob.Job, udtPSMJob.DtaRefineryDataFolderPath)
                }
                else
                {
                    Console.WriteLine("Skipping " + dtaRefineryDataFolderPath);
                }
            }

            return true;
        }

        /// <summary>
        /// Use MSConvert to convert a .raw file to .mgf
        /// </summary>
        public void RunMSConvert()
        {
            const string workDir = @"C:\DMS_WorkDir";

            const string  exePath = @"C:\DMS_Programs\ProteoWizard\msconvert.exe";
            const string  dataFilePath = @"C:\DMS_WorkDir\QC_ShewPartialInj_15_02-100ng_Run-1_20Jan16_Pippin_15-08-53.raw";
            const string arguments =
                dataFilePath +
                @" --filter ""peakPicking true 1-"" " +
                @" --filter ""threshold count 500 most-intense"" " +
                @" --mgf -o C:\DMS_WorkDir";

            mProgRunner = new RunDosProgram(workDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = string.Empty
                // Allow the console output filename to be auto-generated
            };
            RegisterEvents(mProgRunner);

            if (!mProgRunner.RunProgram(exePath, arguments, "MSConvert", true))
            {
                Console.WriteLine("Error running MSConvert");
            }
            else
            {
                Console.WriteLine("Done");
            }
        }

        /// <summary>
        /// Test copying a results folder to the Failed Results folder on the current machine
        /// </summary>
        public void TestArchiveFailedResults()
        {
            GetCodeTestToolRunner(out var jobParams);

            if (string.IsNullOrWhiteSpace(mMgrSettings.GetParam(AnalysisMgrSettings.MGR_PARAM_FAILED_RESULTS_DIRECTORY_PATH)))
            {
                if (Global.LinuxOS)
                {
                    var localWorkDirPath = mMgrSettings.GetParam(AnalysisMgrSettings.MGR_PARAM_LOCAL_WORK_DIR_PATH);
                    if (!string.IsNullOrWhiteSpace(localWorkDirPath))
                    {
                        var localWorkDir = new DirectoryInfo(localWorkDirPath);

                        if (localWorkDir.Parent == null)
                            mMgrSettings.SetParam(AnalysisMgrSettings.MGR_PARAM_FAILED_RESULTS_DIRECTORY_PATH, "");
                        else
                            mMgrSettings.SetParam(AnalysisMgrSettings.MGR_PARAM_FAILED_RESULTS_DIRECTORY_PATH, Path.Combine(localWorkDir.Parent.FullName, AnalysisToolRunnerBase.DMS_FAILED_RESULTS_DIRECTORY_NAME));
                    }
                    else
                    {
                        mMgrSettings.SetParam(AnalysisMgrSettings.MGR_PARAM_FAILED_RESULTS_DIRECTORY_PATH, "");
                    }
                }
                else
                {
                    mMgrSettings.SetParam(AnalysisMgrSettings.MGR_PARAM_FAILED_RESULTS_DIRECTORY_PATH, @"C:\" + AnalysisToolRunnerBase.DMS_FAILED_RESULTS_DIRECTORY_NAME);
                }
            }

            var resFolderName = Path.Combine(GetWorkDirPath(), "TestResults");
            var resultsFolder = new DirectoryInfo(resFolderName);
            if (!resultsFolder.Exists)
                resultsFolder.Create();

            var rand = new Random();

            for (var i = 0; i < 5; i++)
            {
                var outFilePath = Path.Combine(resultsFolder.FullName, "TestOutFile" + i + ".txt");

                using var writer = new StreamWriter(new FileStream(outFilePath, FileMode.Create, FileAccess.Write));

                writer.WriteLine("Scan\tIntensity");

                for (var j = 1; j < 1000; j++)
                {
                    writer.WriteLine("{0}\t{1}", j, rand.Next(0, 10000));
                }
            }

            var analysisResults = new AnalysisResults(mMgrSettings, jobParams);
            analysisResults.CopyFailedResultsToArchiveDirectory(Path.Combine(GetWorkDirPath(), resFolderName));
        }

        /// <summary>
        /// Archive a Sequest parameter file by copying to \\gigasax\dms_parameter_Files\Sequest
        /// </summary>
        public void TestArchiveFileStart()
        {
            const string paramFilePath = @"D:\Temp\sequest_N14_NE.params";
            const string targetFolderPath = @"\\gigasax\dms_parameter_Files\Sequest";

            TestArchiveFile(paramFilePath, targetFolderPath);

            // ReSharper disable CommentTypo

            // TestArchiveFile(@"\\n2.emsl.pnl.gov\dmsarch\LCQ_1\LCQ_C1\DR026_dialyzed_7june00_a\Seq200104201154_Auto1001\sequest_Tryp_4_IC.params", targetFolderPath)
            // TestArchiveFile(@"\\proto-4\C1_DMS1\DR026_dialyzed_7june00_a\Seq200104201154_Auto1001\sequest_Tryp_4_IC.params", targetFolderPath)

            // ReSharper restore CommentTypo

            Console.WriteLine("Done syncing files");
        }

        private void TestArchiveFile(string sourceFilePath, string targetFolderPath)
        {
            try
            {
                var lineIgnoreRegExSpecs = new List<Regex> {
                    new("mass_type_parent *=.*", RegexOptions.Compiled | RegexOptions.IgnoreCase)
                };

                var needToArchiveFile = false;

                var fileName = Path.GetFileName(sourceFilePath);
                if (fileName == null)
                {
                    Console.WriteLine("Filename could not be parsed from parameter " + nameof(sourceFilePath));
                    return;
                }

                var targetFilePath = Path.Combine(targetFolderPath, fileName);

                if (!File.Exists(targetFilePath))
                {
                    needToArchiveFile = true;
                }
                else
                {
                    // Read the files line-by-line and compare
                    // Since the first 2 lines of a Sequest parameter file don't matter, and since the 3rd line can vary from computer to computer, we start the comparison at the 4th line

                    if (!Global.TextFilesMatch(sourceFilePath, targetFilePath, 4, 0, true, lineIgnoreRegExSpecs))
                    {
                        // Files don't match; rename the old file

                        var archivedFile = new FileInfo(targetFilePath);

                        var newNameBase = Path.GetFileNameWithoutExtension(targetFilePath) + "_" + archivedFile.LastWriteTime.ToString("yyyy-MM-dd");
                        var newName = newNameBase + Path.GetExtension(targetFilePath);

                        // See if the renamed file exists; if it does, we'll have to tweak the name
                        var revisionNumber = 1;
                        string newPath;
                        while (true)
                        {
                            newPath = Path.Combine(targetFolderPath, newName);
                            if (!File.Exists(newPath))
                            {
                                break;
                            }

                            revisionNumber++;
                            newName = newNameBase + "_v" + revisionNumber + Path.GetExtension(targetFilePath);
                        }

                        archivedFile.MoveTo(newPath);

                        needToArchiveFile = true;
                    }
                }

                if (needToArchiveFile)
                {
                    // Copy the new parameter file to the archive
                    Console.WriteLine("Copying " + Path.GetFileName(sourceFilePath) + " to " + targetFilePath);
                    File.Copy(sourceFilePath, targetFilePath, true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error caught: " + ex.Message);
            }
        }

        /// <summary>
        /// Test using SFTP to list files on a remote host
        /// Connect using an RSA private key file
        /// </summary>
        public void TestConnectRSA()
        {
            if (Global.LinuxOS)
            {
                LogError("Cannot use TestConnectRSA on Linux");
                return;
            }

            const string host = "PrismWeb2";
            const string username = "svc-dms";

            var keyFile = new FileInfo(@"C:\DMS_RemoteInfo\Svc-Dms.key");
            if (!keyFile.Exists)
            {
                LogError("File not found: " + keyFile.FullName);
                return;
            }

            var passPhraseFile = new FileInfo(@"C:\DMS_RemoteInfo\Svc-Dms.pass");
            if (!passPhraseFile.Exists)
            {
                LogError("File not found: " + passPhraseFile.FullName);
                return;
            }

            MemoryStream keyFileStream;
            string passphraseEncoded;

            using (var reader = new StreamReader(new FileStream(keyFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                keyFileStream = new MemoryStream(Encoding.ASCII.GetBytes(reader.ReadToEnd()));
            }

            using (var reader = new StreamReader(new FileStream(passPhraseFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                passphraseEncoded = reader.ReadLine();
            }

            try
            {
                var privateKeyFile = new PrivateKeyFile(keyFileStream, Global.DecodePassword(passphraseEncoded));

                using var sftp = new SftpClient(host, username, privateKeyFile);

                sftp.Connect();
                var files = sftp.ListDirectory(".");
                foreach (var file in files)
                {
                    Console.WriteLine(file.FullName);
                }
            }
            catch (InvalidOperationException ex)
            {
                if (ex.Message.Contains("Invalid data type"))
                    throw new Exception("Pass phrase error connecting to " + host + " as user " + username, ex);

                throw;
            }
        }

        /// <summary>
        /// Test copying a remote file locally, skipping the copy if the file hash matches the hash tracked via a .hashcheck file
        /// </summary>
        public void TestCopyToLocalWithHashCheck()
        {
            const string  remoteFilePath = @"\\gigasax\dms_parameter_Files\Formularity\PNNL_CIA_DB_1500_B.bin";
            const string targetDirectoryPath = @"C:\DMS_Temp_Org";

            var fileTools = new FileTools("AnalysisManager", 1);
            var fileSyncUtil = new FileSyncUtils(fileTools);
            RegisterEvents(fileSyncUtil);

            const int recheckIntervalDays = 1;
            var success = fileSyncUtil.CopyFileToLocal(remoteFilePath, targetDirectoryPath, out var errorMessage, recheckIntervalDays);

            if (success)
                Console.WriteLine("Verified " + Path.Combine(targetDirectoryPath, Path.GetFileName(remoteFilePath)));
            else
                Console.WriteLine("Error: " + errorMessage);

            Console.WriteLine();
        }

        /// <summary>
        /// Test copying a large fasta file to a remote host
        /// </summary>
        public void TestCopyToRemote()
        {
            const short DEBUG_LEVEL = 2;

            var jobParams = InitializeMgrAndJobParams(DEBUG_LEVEL);

            jobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "StepTool", "MSGFPlus");
            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_DATASET_NAME, "TestDataset");

            var transferUtility = new RemoteTransferUtility(mMgrSettings, jobParams);
            RegisterEvents(transferUtility);

            const string sourceFilePath = @"C:\DMS_Temp_Org\uniref50_2013-02-14.fasta";
            const string remoteDirectoryPath = "/file1/temp/DMSOrgDBs";

            var success = transferUtility.CopyFileToRemote(sourceFilePath, remoteDirectoryPath, useLockFile: true);

            Console.WriteLine("Success: " + success);
        }

        /// <summary>
        /// Split apart a _dta.txt file
        /// </summary>
        /// <param name="rootFileName"></param>
        /// <param name="resultsFolder"></param>
        public void TestConcatenation(string rootFileName, string resultsFolder)
        {
            Console.WriteLine("Splitting concatenated DTA file");

            var fileSplitter = new SplitCattedFiles();
            var filesToSkip = new SortedSet<string>();

            fileSplitter.SplitCattedDTAsOnly(rootFileName, resultsFolder, filesToSkip);

            Console.WriteLine("Completed splitting concatenated DTA file");
        }

        /// <summary>
        /// Instantiate an instance of AnalysisToolRunnerDtaSplit
        /// </summary>
        public void TestDTASplit()
        {
            const int debugLevel = 2;

            var jobParams = InitializeMgrAndJobParams(debugLevel);

            var statusTools = new StatusFile("Status.xml", debugLevel);
            RegisterEvents(statusTools);

            var myEMSLUtilities = new MyEMSLUtilities(debugLevel, GetWorkDirPath(), true);
            RegisterEvents(myEMSLUtilities);

            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_DATASET_NAME, "QC_05_2_05Dec05_Doc_0508-08");
            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "NumberOfClonedSteps", "25");
            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "ClonedStepsHaveEqualNumSpectra", "True");

            var mgr = new FileInfo(PRISM.FileProcessor.ProcessFilesOrDirectoriesBase.GetAppPath());
            var mgrFolderPath = mgr.DirectoryName;

            var summaryFile = new SummaryFile();
            summaryFile.Clear();

            var pluginLoader = new PluginLoader(summaryFile, mgrFolderPath);

            var toolRunner = pluginLoader.GetToolRunner("dta_split".ToLower());
            toolRunner.Setup("CodeTest", mMgrSettings, jobParams, statusTools, summaryFile, myEMSLUtilities);
            toolRunner.RunTool();
        }

        /// <summary>
        /// Test creation of a .fasta file from a protein collection
        /// Also calls Running BuildSA
        /// </summary>
        /// <param name="destinationDirectory"></param>
        public bool TestProteinDBExport(string destinationDirectory)
        {
            // ReSharper disable StringLiteralTypo

            // Test what the Protein_Exporter does if a protein collection name is truncated (and thus invalid)
            var proteinCollectionList = "Geobacter_bemidjiensis_Bem_T_2006-10-10,Geobacter_lovelyi_SZ_2007-06-19,Geobacter_metallireducens_GS-15_2007-10-02,Geobacter_sp_";
            const string proteinOptions = "seq_direction=forward,filetype=fasta";

            // Test a 2 MB fasta file:
            proteinCollectionList = "Micrococcus_luteus_NCTC2665_Uniprot_20160119,Tryp_Pig_Bov";

            // Test a 34 MB fasta file
            // proteinCollectionList = "nr_ribosomal_2010-08-17,Tryp_Pig";

            // Test 100 MB fasta file
            // legacyFasta = "na"
            // proteinCollectionList = "GWB1_Rifle_2011_9_13_0_1_2013-03-27,Tryp_Pig_Bov"

            // ReSharper restore StringLiteralTypo

            var success = TestProteinDBExport(destinationDirectory, "na", proteinCollectionList, proteinOptions);

            if (success)
            {
                IJobParams jobParams = InitializeManagerParams();
                jobParams.AddAdditionalParameter("PeptideSearch", AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME, mFastaFileName);

                //const bool msgfPlus = true;
                //var jobNum = "12345";
                //var debugLevel = (short)(mMgrSettings.GetParam("DebugLevel", 1));

                //var JavaProgLoc = @"C:\DMS_Programs\Java\jre8\bin\java.exe";
                //var MSGFDbProgLoc = @"C:\DMS_Programs\MSGFPlus\MSGFPlus.jar";
                //bool fastaFileIsDecoy;
                //string fastaFilePath;

                // Uncomment the following if the MSGFDB plugin is associated with the solution
                //var tool = new AnalysisManagerMSGFDBPlugIn.MSGFDBUtils(
                //    mMgrSettings, jobParams, jobNum, mMgrSettings.GetParam(AnalysisMgrSettings.MGR_PARAM_WORK_DIR), debugLevel, msgfPlus);

                //RegisterEvents(oTool);

                //float fastaFileSizeKB;

                //// Note that fastaFilePath will be populated by this function call
                //var result = tool.InitializeFastaFile(JavaProgLoc, MSGFDbProgLoc, out fastaFileSizeKB, out fastaFileIsDecoy, out fastaFilePath);
            }

            return success;
        }

        /// <summary>
        /// Test creation of a .fasta file from a protein collection
        /// </summary>
        /// <param name="destinationDirectory"></param>
        /// <param name="legacyFasta"></param>
        /// <param name="proteinCollectionList"></param>
        /// <param name="proteinOptions"></param>
        public bool TestProteinDBExport(string destinationDirectory, string legacyFasta, string proteinCollectionList, string proteinOptions)
        {
            // Instantiate fasta tool if not already done
            if (mFastaTools == null)
            {
                if (string.IsNullOrEmpty(mFastaToolsCnStr))
                {
                    Console.WriteLine("Protein database connection string not specified");
                    return false;
                }

                mFastaTools = new OrganismDatabaseHandler.ProteinExport.GetFASTAFromDMS(mFastaToolsCnStr);
                RegisterEvents(mFastaTools);

                mFastaTools.FileGenerationStarted += FileGenerationStarted;
                mFastaTools.FileGenerationCompleted += FileGenerationCompleted;
                mFastaTools.FileGenerationProgress += FileGenerationProgress;
            }

            // Initialize fasta generation state variables
            mGenerationComplete = false;

            // Setup a timer to prevent an infinite loop if there's a fasta generation problem
            mFastaTimer = new System.Timers.Timer();
            mFastaTimer.Elapsed += FastaTimer_Elapsed;

            mFastaTimer.Interval = FASTA_GEN_TIMEOUT_INTERVAL_SEC * 1000;
            mFastaTimer.AutoReset = false;

            // Create the fasta file
            mFastaGenTimeOut = false;
            try
            {
                mFastaTimer.Start();
                mFastaTools.ExportFASTAFile(proteinCollectionList, proteinOptions, legacyFasta, destinationDirectory);
            }
            catch (Exception ex)
            {
                Console.WriteLine("AnalysisResources.CreateFastaFile(), Exception generating OrgDb file: " + ex.Message);
                return false;
            }

            // Wait for fasta creation to finish
            while (!mGenerationComplete)
            {
                Global.IdleLoop(2);
            }

            if (mFastaGenTimeOut)
            {
                // Fasta generator hung - report error and exit
                Console.WriteLine("Timeout error while generating OrgDb file (" + FASTA_GEN_TIMEOUT_INTERVAL_SEC + " seconds have elapsed)");
                return false;
            }

            // If we got to here, everything worked OK
            return true;
        }

        /// <summary>
        /// Test deleting a file
        /// </summary>
        public void TestDeleteFiles()
        {
            const string outFileName = "MyTestDataset_out.txt";

            var toolRunner = GetCodeTestToolRunner(out var jobParams);

            jobParams.AddResultFileToSkip(outFileName);

            toolRunner.RunTool();
        }

        /// <summary>
        /// Test copying results
        /// </summary>
        public void TestDeliverResults()
        {
            var toolRunner = GetCodeTestToolRunner(out var jobParams);

            jobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME, "Test_Results_" + DateTime.Now.ToString("hh_mm_ss"));
            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_TRANSFER_DIRECTORY_PATH, @"\\proto-3\DMS3_XFER");
            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_DATASET_NAME, "Test_Dataset");

            toolRunner.RunTool();
        }

        /// <summary>
        /// Check date formatting
        /// </summary>
        public void TestFileDateConversion()
        {
            var targetFile = new FileInfo(@"D:\JobSteps.png");

            var lastWriteTime = targetFile.LastWriteTime.ToString(CultureInfo.InvariantCulture);

            var resultFiles = Directory.GetFiles(@"C:\Temp\", "*");

            foreach (var fileToCopy in resultFiles)
            {
                Console.WriteLine(fileToCopy);
            }

            Console.WriteLine(lastWriteTime);
        }

        /// <summary>
        /// Display metadata regarding all of the processes running on this system
        /// </summary>
        public void TestGetProcesses()
        {
            var sysInfo = new SystemInfo();
            var processes = sysInfo.GetProcesses();

            Console.WriteLine();
            Console.WriteLine("Enumerating {0} processes", processes.Count);
            Console.WriteLine();

            foreach (var process in processes)
            {
                Console.WriteLine(process.Value.ToStringVerbose());
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Test determining the DLL version of DLLs
        /// </summary>
        public void TestGetToolVersionInfo()
        {
            const string dllFile = "AM_Shared.dll";
            const string dllFile64Bit = "PeptideToProteinMapEngine.dll";

            var toolRunner = GetCodeTestToolRunner();

            var toolVersionInfo = string.Empty;

            toolRunner.StoreToolVersionInfoOneFile(ref toolVersionInfo, dllFile);

            toolRunner.StoreToolVersionInfoOneFile(ref toolVersionInfo, dllFile64Bit);

            Console.WriteLine(toolVersionInfo);
        }

        /// <summary>
        /// Create a log file
        /// </summary>
        public void TestLogging()
        {
            var logFileNameBase = Path.Combine("Logs", "AnalysisMgr");

            // The analysis manager determines when to log or not log based on internal logic
            // Set the LogLevel tracked by FileLogger to DEBUG so that all messages sent to the class are logged
            LogTools.CreateFileLogger(logFileNameBase, BaseLogger.LogLevels.DEBUG);

            GetCodeTestToolRunner(out var jobParams);

            mDebugLevel = 2;
            jobParams.DebugLevel = mDebugLevel;

            for (var debugLevel = 0; debugLevel <= 5; debugLevel++)
            {
                LogMessage("Test status, debugLevel " + debugLevel, debugLevel);
            }

            for (var debugLevel = 0; debugLevel <= 5; debugLevel++)
            {
                LogDebug("Test debug, debugLevel " + debugLevel, debugLevel);
            }

            LogWarning("Test warning");

            for (var debugLevel = 0; debugLevel <= 5; debugLevel++)
            {
                LogMessage("Test error, debugLevel " + debugLevel, debugLevel, true);
            }

            LogError("Test error, no detailed message");

            try
            {
                throw new FileNotFoundException("TestFile.txt");
            }
            catch (Exception ex)
            {
                LogError("Test exception", ex);
            }

            Console.ResetColor();
            foreach (ConsoleColor eColor in Enum.GetValues(typeof(ConsoleColor)))
            {
                Console.Write("{0,-12} [", eColor);

                Console.ForegroundColor = eColor;
                Console.Write("test message");
                Console.ResetColor();
                Console.WriteLine("]");
            }

            Global.ShowTimestampTrace("Logging 'testing complete'");

            LogMessage("Testing complete");

            Global.ShowTimestampTrace("Exiting method TestLogging");
        }

        /// <summary>
        /// Test database logging
        /// </summary>
        /// <param name="connStr">ODBC-style connection string</param>
        public void TestDatabaseLogging(string connStr)
        {
            LogTools.CreateDbLogger(connStr, "CodeTest");

            LogTools.WriteLog(LogTools.LoggerTypes.LogDb, BaseLogger.LogLevels.INFO, "Test analysis manager status message");

            LogError("Test analysis manager error", null, true);

            try
            {
                throw new FileNotFoundException("TestFile.txt");
            }
            catch (Exception ex)
            {
                LogError("Test exception", ex, true);
            }

            var sqlServerLogger = new SQLServerDatabaseLogger
            {
                EchoMessagesToFileLogger = true
            };
            sqlServerLogger.ChangeConnectionInfo("CodeTest2", connStr, "PostLogEntry", "type", "message", "postedBy");
            sqlServerLogger.WriteLog(BaseLogger.LogLevels.FATAL, "SQL Server Fatal Test");

            var odbcConnectionString = ODBCDatabaseLogger.ConvertSqlServerConnectionStringToODBC(connStr);
            var odbcLogger = new ODBCDatabaseLogger
            {
                EchoMessagesToFileLogger = true
            };
            odbcLogger.ChangeConnectionInfo("CodeTest2", odbcConnectionString, "PostLogEntry", "type", "message", "postedBy", 128, 4096, 128);

            odbcLogger.WriteLog(BaseLogger.LogLevels.INFO, "ODBC Log Test");
            odbcLogger.WriteLog(BaseLogger.LogLevels.WARN, "ODBC Warning Test");
        }

        /// <summary>
        /// Determine the size of a legacy FASTA file
        /// </summary>
        public void GetLegacyFastaFileSize()
        {
            var jobParams = new AnalysisJob(mMgrSettings, 0);

            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "ToolName", "MSGFPlus_SplitFasta");

            jobParams.SetParam(AnalysisJob.STEP_PARAMETERS_SECTION, "Step", "50");

            jobParams.SetParam("ParallelMSGFPlus", "NumberOfClonedSteps", "25");
            jobParams.SetParam("ParallelMSGFPlus", "CloneStepRenumberStart", "50");
            jobParams.SetParam("ParallelMSGFPlus", "SplitFasta", "True");

            jobParams.SetParam("PeptideSearch", "legacyFastaFileName", "Uniprot_ArchaeaBacteriaFungi_SprotTrembl_2014-4-16.fasta");
            jobParams.SetParam("PeptideSearch", "OrganismName", "Combined_Organism_Rifle_SS");
            jobParams.SetParam("PeptideSearch", "ProteinCollectionList", "na");
            jobParams.SetParam("PeptideSearch", "ProteinOptions", "na");

            const int debugLevel = 2;
            var resourcer = GetResourcesObject(debugLevel, jobParams);

            var proteinCollectionInfo = new ProteinCollectionInfo(jobParams);

            var spaceRequiredMB = resourcer.LookupLegacyDBDiskSpaceRequiredMB(proteinCollectionInfo, out var legacyFastaName, out var fastaFileSizeGB);

            Console.WriteLine("{0} is {1:F3} GB", legacyFastaName, fastaFileSizeGB);
            Console.WriteLine("The FASTA file plus its index files requires roughly {0:F3} GB", spaceRequiredMB / 1024.0);
        }

        /// <summary>
        /// Manually run cyclops
        /// </summary>
        public void TestRunCyclops()
        {
            bool processingSuccess;

            try
            {
                var candidateRDirectories = new List<string> {
                    @"C:\Program Files\R\R-3.6.0\bin\x64",
                    @"C:\Program Files\R\R-3.5.2patched\bin\x64"
                };

                var paramDictionary = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    {"Job", "1716477"},
                    {"CyclopsWorkflowName", "ITQ_ExportOperation.xml"},
                    {"workDir", @"C:\Temp\Cyclops"},
                    {"Consolidation_Factor", ""},
                    {"Fixed_Effect", ""},
                    {"RunProteinProphet", "False"},
                    {"orgDbDir", @"C:\DMS_Temp_Org"}
                };

                foreach (var candidateDir in candidateRDirectories)
                {
                    LogDebug("Looking for " + candidateDir);
                    if (Directory.Exists(candidateDir))
                    {
                        paramDictionary.Add("RDLL", @"C:\Program Files\R\R-3.6.0\bin\x64");
                        break;
                    }
                }

                if (!paramDictionary.TryGetValue("RDLL", out var rBinPath))
                {
                    LogError("R directory not found");
                    return;
                }
                else
                {
                    LogMessage("R directory path: " + rBinPath);
                }

                var cyclops = new CyclopsController(paramDictionary);
                RegisterEvents(cyclops);

                cyclops.ErrorEvent += Cyclops_ErrorEvent;
                cyclops.WarningEvent += Cyclops_WarningEvent;
                cyclops.StatusEvent += Cyclops_StatusEvent;

                processingSuccess = cyclops.Run();
            }
            catch (Exception ex)
            {
                LogError("Error running Cyclops: " + ex.Message, ex);
                processingSuccess = false;
            }

            Console.WriteLine("processingSuccess = " + processingSuccess);
        }

        /// <summary>
        /// Run a test query
        /// </summary>
        public void TestRunQuery()
        {
            const string sqlStr = "Select top 15 * from t_log_entries where posting_time >= DateAdd(day, -15, GetDate())";

            const string connectionString = "Data Source=gigasax;Initial Catalog=dms_pipeline;Integrated Security=SSPI;";
            const short retryCount = 2;
            const int timeoutSeconds = 30;

            var dbTools = DbToolsFactory.GetDBTools(connectionString, timeoutSeconds, true);
            RegisterEvents(dbTools);

            dbTools.GetQueryResultsDataTable(sqlStr, out var results, retryCount);

            Console.WriteLine("{0,-10} {1,-21} {2,-20} {3}", "Entry_ID", "Date", "Posted_By", "Message");
            foreach (DataRow row in results.Rows)
            {
                Console.WriteLine("{0,-10} {1,-21:yyyy-MM-dd hh:mm tt} {2,-20} {3}", row[0], row[2], row[1], row[4]);
            }
        }

        /// <summary>
        /// Call a stored procedure
        /// </summary>
        public void TestRunSP()
        {
            const string connectionString = "Data Source=gigasax;Initial Catalog=dms_pipeline;Integrated Security=SSPI;";
            const short retryCount = 2;
            const int timeoutSeconds = 30;

            var dbTools = DbToolsFactory.GetDBTools(connectionString, timeoutSeconds, debugMode: true);
            RegisterEvents(dbTools);

            var cmd = dbTools.CreateCommand("GetJobStepParamsAsTable", CommandType.StoredProcedure);

            dbTools.AddParameter(cmd, "@jobNumber", SqlType.Int).Value = 1026591;
            dbTools.AddParameter(cmd, "@stepNumber", SqlType.Int).Value = 3;
            dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512).Direction = ParameterDirection.Output;

            var success = dbTools.ExecuteSPDataTable(cmd, out var results, retryCount);

            foreach (DataRow row in results.Rows)
            {
                Console.WriteLine(row[0] + ": " + row[1]);
            }
        }

        /// <summary>
        /// Convert a zip file to a gzip file
        /// </summary>
        /// <param name="zipFilePath"></param>
        public void ConvertZipToGZip(string zipFilePath)
        {
            const int debugLevel = 2;
            const string workDirPath = @"C:\DMS_WorkDir";

            var dotNetZipTools = new DotNetZipTools(debugLevel, workDirPath);
            RegisterEvents(dotNetZipTools);

            dotNetZipTools.UnzipFile(zipFilePath);

            var workDir = new DirectoryInfo(workDirPath);
            foreach (var mzidFile in workDir.GetFiles("*.mzid"))
            {
                dotNetZipTools.GZipFile(mzidFile.FullName, true);
            }
        }

        /// <summary>
        /// Test creating and decompressing .gz files using PRISM.dll
        /// </summary>
        public void TestGZip()
        {
            var toolRunner = GetCodeTestToolRunner();

            var sourceFile = new FileInfo(@"\\proto-2\UnitTest_Files\ThermoRawFileReader\QC_Mam_16_01_125ng_2pt0-IT22_Run-A_16Oct17_Pippin_AQ_17-10-01.raw");

            var targetDirectoryPath = Path.GetTempPath();

            Console.WriteLine();
            Console.WriteLine("Compressing " + sourceFile.FullName);
            toolRunner.GZipFile(sourceFile.FullName, targetDirectoryPath, false);

            var gzippedFile = new FileInfo(Path.Combine(targetDirectoryPath, sourceFile.Name + ".gz"));

            Console.WriteLine();
            Console.WriteLine("Decompressing " + gzippedFile.FullName);
            toolRunner.GUnzipFile(gzippedFile.FullName, targetDirectoryPath);

            var roundTripFile = new FileInfo(Path.Combine(targetDirectoryPath, sourceFile.Name));

            Console.WriteLine();
            if (roundTripFile.Length == sourceFile.Length)
            {
                Console.WriteLine("Round trip file length matches the original file {0:#,###} bytes", sourceFile.Length);
            }
            else
            {
                Console.WriteLine("File size mismatch: {0:#,###} vs. {1:#,###} bytes", roundTripFile.Length, sourceFile.Length);
            }

            Global.IdleLoop(2);

            Console.WriteLine();
            Console.WriteLine("Deleting files in the temp directory");
            gzippedFile.Delete();
            roundTripFile.Delete();
        }

        /// <summary>
        /// Test unzipping a file
        /// </summary>
        /// <remarks>This uses DotNetZip</remarks>
        public bool TestUnzip(string zipFilePath, string outFolderPath)
        {
            const int debugLevel = 2;
            var resourcer = GetResourcesObject(debugLevel);

            var success = resourcer.UnzipFileStart(zipFilePath, outFolderPath, "TestUnzip");
            // success = resourcer.UnzipFileStart(zipFilePath, outFolderPath, "TestUnzip", True)

            return success;
        }

        /// <summary>
        /// Test zipping a file
        /// </summary>
        /// <remarks>This uses DotNetZip</remarks>
        public void TestZip()
        {
            var toolRunner = GetCodeTestToolRunner();

            const string sourceFilePath = @"F:\Temp\ZipTest\QExact01\UDD-1_27Feb13_Gimli_12-07-03_HCD.mgf";

            toolRunner.ZipFile(sourceFilePath, false);

            var zippedFile = @"F:\Temp\ZipTest\QExact01\" + Path.GetFileNameWithoutExtension(sourceFilePath) + ".zip";

            toolRunner.UnzipFile(zippedFile);

            toolRunner.UnzipFile(zippedFile, @"F:\Temp\ZipTest\UnzipTarget");

            var dotNetZipTools = new DotNetZipTools(1, GetWorkDirPath());
            RegisterEvents(dotNetZipTools);

            dotNetZipTools.ZipDirectory(@"F:\Temp\ZipTest\QExact01\", @"F:\Temp\ZipTest\QExact01_Folder.zip");
        }

        /// <summary>
        /// Test DotNetZip (aka Ionic zip)
        /// </summary>
        public void TestDotNetZipTools()
        {
            var dotNetZipTools = new DotNetZipTools(1, @"C:\DMS_WorkDir");
            RegisterEvents(dotNetZipTools);

            dotNetZipTools.UnzipFile(@"C:\DMS_WorkDir\Temp.zip", @"C:\DMS_WorkDir", "*.png");
            foreach (var item in dotNetZipTools.MostRecentUnzippedFiles)
            {
                Console.WriteLine(item.Key + " - " + item.Value);
            }
        }

        /// <summary>
        /// Retrieve and decompress MALDI data
        /// </summary>
        /// <param name="sourceDatasetFolder"></param>
        public bool TestMALDIDataUnzip(string sourceDatasetFolder)
        {
            const int debugLevel = 2;

            var resourceTester = new ResourceTestClass();

            var statusTools = new StatusFile("Status.xml", debugLevel);
            RegisterEvents(statusTools);

            if (string.IsNullOrEmpty(sourceDatasetFolder))
            {
                sourceDatasetFolder = @"\\Proto-10\9T_FTICR_Imaging\2010_4\ratjoint071110_INCAS_MS";
            }

            GetCodeTestToolRunner(out var jobParams, out var myEMSLUtilities);

            mMgrSettings.SetParam("ChameleonCachedDataFolder", @"H:\9T_Imaging");

            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_DATASET_NAME, "ratjoint071110_INCAS_MS");

            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "DatasetStoragePath", @"\\Proto-10\9T_FTICR_Imaging\2010_4\");
            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "DatasetArchivePath", @"\\adms.emsl.pnl.gov\dmsarch\9T_FTICR_Imaging_1");
            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_TRANSFER_DIRECTORY_PATH, @"\\proto-10\DMS3_Xfer");

            resourceTester.Setup("CodeTest", mMgrSettings, jobParams, statusTools, myEMSLUtilities);

            var success = resourceTester.FileSearch.RetrieveBrukerMALDIImagingFolders(true);

            // success = resourceTester.UnzipFileStart(zipFilePath, outFolderPath, "TestUnzip", True)

            return success;
        }

        /// <summary>
        /// Writes the status file
        /// </summary>
        public void TestStatusLogging()
        {
            var statusTools = new StatusFile("Status.xml", 2);
            RegisterEvents(statusTools);

            statusTools.MgrName = mMgrSettings.ManagerName;

            var exePath = Assembly.GetExecutingAssembly().Location;
            if (exePath == null)
                throw new Exception("Unable to determine the Exe path of the currently executing assembly");

            var exeInfo = new FileInfo(exePath);

            statusTools.ConfigureMemoryLogging(true, 5, exeInfo.DirectoryName);

            statusTools.WriteStatusFile();
        }

        /// <summary>
        /// Test zipping and unzipping with DotNetZip
        /// </summary>
        public void TestZipAndUnzip()
        {
            // Zip Benchmark stats
            //
            // January 2011 tests with a 611 MB file
            //   IonicZip    unzips the file in 70 seconds (reading/writing to the same drive)
            //   IonicZip    unzips the file in 62 seconds (reading/writing from different drives)
            //   WinRar      unzips the file in 36 seconds (reading/writing from different drives)
            //   PKZipC      unzips the file in 38 seconds (reading/writing from different drives)
            //
            // September 2017 tests
            //   IonicZip compressed a 2.6 GB UIMF file in 86 seconds (reading/writing to the same drive)
            //   IonicZip compressed a 556 MB text file in 3.7 seconds (creating a 107 MB file)
            //   IonicZip compressed a folder with 996 MB of data (FASTA files, text files, one .gz file) in 12 seconds (creating a 348 MB zip file)

            var dotNetZipTools = new DotNetZipTools(3, @"F:\Temp");
            RegisterEvents(dotNetZipTools);

            var stopWatch = new Stopwatch();

            stopWatch.Start();
            dotNetZipTools.ZipFile(@"F:\Temp\OHSU_mortality_lipids_137_Pos_12Sep17_Brandi-WCSH7908.uimf", false);
            stopWatch.Stop();
            Console.WriteLine("Elapsed time: {0:F3} seconds", stopWatch.ElapsedMilliseconds / 1000.0);

            stopWatch.Reset();
            stopWatch.Start();
            dotNetZipTools.ZipFile(@"F:\Temp\TestData.txt", false, @"F:\Temp\TestCustom.zip");
            stopWatch.Stop();
            Console.WriteLine("Elapsed time: {0:F3} seconds", stopWatch.ElapsedMilliseconds / 1000.0);

            stopWatch.Reset();
            stopWatch.Start();
            dotNetZipTools.ZipDirectory(@"F:\Temp\FolderTest", @"F:\Temp\ZippedFolderTest.zip");
            stopWatch.Stop();
            Console.WriteLine("Elapsed time: {0:F3} seconds", stopWatch.ElapsedMilliseconds / 1000.0);
        }

        /// <summary>
        /// Generate a scan stats file
        /// </summary>
        public void GenerateScanStatsFiles()
        {
            if (Global.LinuxOS)
            {
                LogError("Cannot use GenerateScanStatsFiles on Linux");
                return;
            }

            const string inputFileName = "QC_Shew_16_01_111_03Feb17_Wally_16-09-27.raw";
            const string workingDir = @"C:\DMS_WorkDir";

            var inputFile = new FileInfo(Path.Combine(workingDir, inputFileName));
            if (!inputFile.Exists)
            {
                LogError("GenerateScanStatsFiles; File not found: " + inputFile.FullName);
                return;
            }

            var success = GenerateScanStatsFiles(inputFile.FullName, workingDir);
            Console.WriteLine("Success: " + success);
        }

        /// <summary>
        /// Generate a scan stats file
        /// </summary>
        public bool GenerateScanStatsFiles(string inputFilePath, string workingDir)
        {
            const string msFileInfoScannerDir = @"C:\DMS_Programs\MSFileInfoScanner";

            var msFileInfoScannerDLLPath = Path.Combine(msFileInfoScannerDir, "MSFileInfoScanner.dll");
            if (!File.Exists(msFileInfoScannerDLLPath))
            {
                Console.WriteLine("File Not Found: " + msFileInfoScannerDLLPath);
                return false;
            }

            var scanStatsGenerator = new ScanStatsGenerator(msFileInfoScannerDLLPath, mDebugLevel);
            RegisterEvents(scanStatsGenerator);

            const int datasetID = 0;

            scanStatsGenerator.ScanStart = 11000;
            scanStatsGenerator.ScanEnd = 12000;

            // Create the _ScanStats.txt and _ScanStatsEx.txt files
            var success = scanStatsGenerator.GenerateScanStatsFiles(inputFilePath, workingDir, datasetID);

            return success;
        }

        private void FileGenerationStarted(string taskMsg)
        {
            // Reset the fasta generation timer
            mFastaTimer.Start();
        }

        private void FileGenerationCompleted(string FullOutputPath)
        {
            // Get the name of the fasta file that was generated
            mFastaFileName = Path.GetFileName(FullOutputPath);

            // Stop the fasta generation timer so no false error occurs
            mFastaTimer?.Stop();

            // Set the completion flag
            mGenerationComplete = true;
        }

        private void FileGenerationProgress(string statusMsg, double fractionDone)
        {
            // Reset the fasta generation timer
            mFastaTimer.Start();
        }

        private void FastaTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            // If this event occurs, it means there was a hang during fasta generation and the manager will have to quit
            mFastaTimer.Stop();

            // Stop the timer to prevent false errors
            mFastaGenTimeOut = true;

            // Set the timeout flag so an error will be reported
            mGenerationComplete = true;

            // Set the completion flag so the fasta generation wait loop will exit
        }

        /// <summary>
        /// Test the program runner by starting X!Tandem
        /// </summary>
        public void TestProgRunner()
        {
            const string appPath = @"F:\My Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\AM_Program\bin\XTandem\tandem.exe";

            var workDir = Path.GetDirectoryName(appPath);

            var progRunner = new RunDosProgram(workDir, mDebugLevel)
            {
                CacheStandardOutput = true,
                CreateNoWindow = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                DebugLevel = 1,
                MonitorInterval = 1000
            };
            RegisterEvents(progRunner);

            progRunner.RunProgram(appPath, "input.xml", "X!Tandem", false);

            if (progRunner.CacheStandardOutput && !progRunner.EchoOutputToConsole)
            {
                Console.WriteLine(progRunner.CachedConsoleOutput);
            }

            if (progRunner.CachedConsoleError.Length > 0)
            {
                Console.WriteLine("Console error output");
                Console.WriteLine(progRunner.CachedConsoleError);
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Test the program runner by running IDPicker
        /// </summary>
        public void TestProgRunnerIDPicker()
        {
            const string mWorkDir = @"C:\DMS_WorkDir";
            var consoleOutputFileName = string.Empty;
            const bool writeConsoleOutputFileRealtime = false;

            const string exePath = @"C:\DMS_Programs\IDPicker\idpQonvert.exe";
            const string arguments =
                @"-MaxFDR 0.1 -ProteinDatabase C:\DMS_Temp_Org\ID_003521_89E56851.fasta " +
                @"-SearchScoreWeights ""msgfspecprob -1"" " +
                @"-OptimizeScoreWeights 1 -NormalizedSearchScores msgfspecprob -DecoyPrefix Reversed_ " +
                @"-dump C:\DMS_WorkDir\Malaria844_msms_29Dec11_Draco_11-10-04.pepXML";

            const string programDescription = "IDPQonvert";

            var progRunner = new RunDosProgram(mWorkDir, mDebugLevel)
            {
                CreateNoWindow = false,
                EchoOutputToConsole = false
            };
            RegisterEvents(progRunner);

            if (string.IsNullOrEmpty(consoleOutputFileName) || !writeConsoleOutputFileRealtime)
            {
                progRunner.CacheStandardOutput = false;
                progRunner.WriteConsoleOutputToFile = false;
            }
            else
            {
                progRunner.CacheStandardOutput = false;
                progRunner.WriteConsoleOutputToFile = true;
                progRunner.ConsoleOutputFilePath = Path.Combine(mWorkDir, consoleOutputFileName);
            }

            var success = progRunner.RunProgram(exePath, arguments, programDescription, true);

            Console.WriteLine(success);
        }

        /// <summary>
        /// Test PurgeOldServerCacheFilesTest
        /// </summary>
        public void TestMSXmlCachePurge()
        {
            var toolRunner = GetCodeTestToolRunner();

            const string cacheFolderPath = @"\\proto-2\past\PurgeTest";

            try
            {
                toolRunner.PurgeOldServerCacheFilesTest(cacheFolderPath, 10);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error calling PurgeOldServerCacheFiles: " + ex.Message);
            }
        }

        /// <summary>
        /// Look for the .PEK and .PAR files in the specified folder
        /// Make sure they are named Dataset_m_dd_yyyy.PAR andDataset_m_dd_yyyy.Pek
        /// </summary>
        /// <param name="folderPath">Folder to examine</param>
        /// <param name="datasetName">Dataset name</param>
        public void FixICR2LSResultFileNames(string folderPath, string datasetName)
        {
            var extensionsToCheck = new List<string>();

            try
            {
                extensionsToCheck.Add("PAR");
                extensionsToCheck.Add("Pek");

                var targetDirectory = new DirectoryInfo(folderPath);

                if (!targetDirectory.Exists)
                {
                    LogError("Directory not found: " + folderPath);
                    return;
                }

                foreach (var extension in extensionsToCheck)
                {
                    foreach (var currentFile in targetDirectory.GetFiles("*." + extension))
                    {
                        if (!currentFile.Name.StartsWith(datasetName, StringComparison.OrdinalIgnoreCase))
                            continue;

                        var desiredName = datasetName + "_" + DateTime.Now.ToString("M_d_yyyy") + "." + extension;

                        if (!string.Equals(currentFile.Name, desiredName, StringComparison.OrdinalIgnoreCase))
                        {
                            try
                            {
                                currentFile.MoveTo(Path.Combine(targetDirectory.FullName, desiredName));
                            }
                            catch (Exception)
                            {
                                // Rename failed; that means the correct file already exists; this is OK
                            }
                        }

                        break;
                    }
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Determine the free system memory
        /// </summary>
        public void SystemMemoryUsage()
        {
            var freeMemoryMB = Global.GetFreeMemoryMB();

            Console.WriteLine();
            Console.WriteLine("Available memory (MB) = {0:F1}", freeMemoryMB);
        }

        /// <summary>
        /// Read the contents file TestInputFile
        /// </summary>
        public void TestGetFileContents()
        {
            const string filePath = "TestInputFile.txt";
            var contents = GetFileContents(filePath);

            Console.WriteLine(contents);
        }

        private string GetFileContents(string filePath)
        {
            var dataFile = new FileInfo(filePath);

            var reader = new StreamReader(new FileStream(dataFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            var fileContents = reader.ReadToEnd();

            if (string.IsNullOrEmpty(fileContents))
            {
                return string.Empty;
            }

            return fileContents;
        }

        /// <summary>
        /// Get the version info of several DLLs
        /// </summary>
        public void TestGetVersionInfo()
        {
            var toolRunner = GetCodeTestToolRunner();

            const string pathToTestx86 = @"F:\My Documents\Projects\DataMining\DMS_Programs\DLLVersionInspector\bin\32bit_Dll_Examples\UIMFLibrary.dll";
            const string pathToTestx64 = @"F:\My Documents\Projects\DataMining\DMS_Programs\DLLVersionInspector\bin\64bit_Dll_Examples\UIMFLibrary.dll";
            const string pathToTestAnyCPU = @"F:\My Documents\Projects\DataMining\DMS_Programs\DLLVersionInspector\bin\AnyCPU_DLL_Examples\UIMFLibrary.dll";

            var toolVersionInfo = string.Empty;
            toolRunner.StoreToolVersionInfoOneFile(ref toolVersionInfo, pathToTestx86);
            Console.WriteLine(toolVersionInfo);

            toolVersionInfo = string.Empty;
            toolRunner.StoreToolVersionInfoOneFile(ref toolVersionInfo, pathToTestx64);
            Console.WriteLine(toolVersionInfo);

            toolVersionInfo = string.Empty;
            toolRunner.StoreToolVersionInfoOneFile(ref toolVersionInfo, pathToTestAnyCPU);
            Console.WriteLine(toolVersionInfo);
        }

        /// <summary>
        /// Test RemoveSparseSpectra
        /// </summary>
        public void RemoveSparseSpectra()
        {
            var cDtaUtilities = new CDTAUtilities();
            RegisterEvents(cDtaUtilities);

            cDtaUtilities.RemoveSparseSpectra(@"C:\DMS_WorkDir", "ALZ_VP2P101_C_SCX_02_7Dec08_Draco_08-10-29_dta.txt");
        }

        /// <summary>
        /// Test ValidateCDTAFileIsCentroided
        /// </summary>
        public void ValidateCentroided()
        {
            const int debugLevel = 2;
            var resourcer = GetResourcesObject(debugLevel);

            resourcer.ValidateCDTAFileIsCentroided(@"\\proto-7\dms3_Xfer\UW_HCV_03_Run2_19Dec13_Pippin_13-07-06\DTA_Gen_1_26_350136\UW_HCV_03_Run2_19Dec13_Pippin_13-07-06_dta.txt");
        }

        private class ResourceTestClass : AnalysisResources
        {
            public override CloseOutType GetResources()
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }
        }

        #region "EventNotifier events"

        private void RegisterEvents(IEventNotifier processingClass)
        {
            processingClass.DebugEvent += DebugEventHandler;
            processingClass.StatusEvent += StatusEventHandler;
            processingClass.ErrorEvent += ErrorEventHandler;
            processingClass.WarningEvent += WarningEventHandler;
            processingClass.ProgressUpdate += ProgressUpdateHandler;
        }

        private void DebugEventHandler(string statusMessage)
        {
            LogDebug(statusMessage);
        }

        private void StatusEventHandler(string statusMessage)
        {
            LogMessage(statusMessage);
        }

        private void CriticalErrorEvent(string message, Exception ex)
        {
            LogError(message, true);
        }

        private void ErrorEventHandler(string errorMessage, Exception ex)
        {
            LogError(errorMessage, ex);
        }

        private void WarningEventHandler(string warningMessage)
        {
            LogWarning(warningMessage);
        }

        private void ProgressUpdateHandler(string progressMessage, float percentComplete)
        {
            if (DateTime.UtcNow.Subtract(mLastStatusTime).TotalSeconds >= 5)
            {
                mLastStatusTime = DateTime.UtcNow;
                if (percentComplete > 0)
                {
                    LogMessage(progressMessage + "; " + percentComplete.ToString("0.0") + "% complete");
                }
                else
                {
                    LogMessage(progressMessage);
                }
            }
        }

        private void Cyclops_ErrorEvent(string message, Exception ex)
        {
            // Cyclops error messages sometimes contain a carriage return followed by a stack trace
            // We don't want that information in mMessage so split on \r and \n
            var messageParts = message.Split('\r', '\n');
            LogError(messageParts[0]);
        }

        private void Cyclops_WarningEvent(string message)
        {
            // Cyclops messages sometimes contain a carriage return followed by a stack trace
            // We don't want that information in mMessage so split on \r and \n
            var messageParts = message.Split('\r', '\n');
            LogWarning(messageParts[0]);
        }

        private void Cyclops_StatusEvent(string message)
        {
            LogMessage(message);
        }
        #endregion

    }
}
