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
using System.Xml;
using AnalysisManagerBase;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.DataFileTools;
using AnalysisManagerBase.FileAndDirectoryTools;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.OfflineJobs;
using AnalysisManagerBase.StatusReporting;
using Cyclops;
using PRISM;
using PRISM.AppSettings;
using PRISM.Logging;
using PRISMDatabaseUtils;
using Renci.SshNet;

// ReSharper disable UnusedMember.Global

namespace AnalysisManagerProg
{
    /// <summary>
    /// Collection of test code
    /// </summary>
    public class CodeTest : LoggerBase
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: Acq, Archaea, Bem, bemidjiensis, bool, Centroided, const, dd, dmsarch, dta, fasta, filetype, Formularity, Geobacter, gimli, gzip
        // Ignore Spelling: hh, Inj, lovelyi, luteus, Mam, metallireducens, mgf, Micrococcus, msgfspecprob, mslevel, na, nr,
        // Ignore Spelling: pek, perf, Pos, Postgres, prog, proteinseqs, Qonvert, Rar, sp, Sprot, SQL, ss, svc-dms, Trembl, tt, yyyy

        // ReSharper restore CommentTypo

        private OrganismDatabaseHandler.ProteinExport.GetFASTAFromDMS mFastaTools;

        private bool mGenerationComplete;

        // SQL Server Fasta Tools connection string: "Data Source=proteinseqs;Initial Catalog=Protein_Sequences;Integrated Security=SSPI;";

        private readonly string mFastaToolsCnStr = "Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms";

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
                var options = new CommandLineOptions { TraceMode = TRACE_MODE_ENABLED };

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
        /// <param name="displayDllPath">Directory to examine</param>
        /// <param name="fileNameFileSpec">File name search pattern</param>
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

            var statusTools = new StatusFile(mMgrSettings, "Status.xml", DEBUG_LEVEL);
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

            var statusTools = new StatusFile(mMgrSettings, "Status.xml", debugLevel);
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
                "SELECT JS.dataset, J.dataset_id, JS.job, JS.output_folder, DFP.dataset_folder_path, JS.transfer_folder_path" +
                " FROM DMS_Pipeline.dbo.V_Job_Steps JS INNER JOIN" +
                "      DMS_Pipeline.dbo.V_Jobs J ON JS.job = J.job INNER JOIN" +
                "      DMS5.dbo.V_Dataset_Folder_Paths DFP ON J.dataset_id = DFP.dataset_id" +
                " WHERE (JS.job Between " + jobStart + " and " + jobEnd + ") AND (JS.tool = 'DTA_Refinery') AND (JS.state = 5)";

            const string connectionString = "Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms";
            const short retryCount = 2;

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, "CodeTest_ProcessDtaRefineryLogFiles");

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: true);
            RegisterEvents(dbTools);

            var success = dbTools.GetQueryResultsDataTable(sql, out var dt, retryCount);

            if (!success)
            {
                Console.WriteLine("Repeated errors running database query");
            }

            if (dt.Rows.Count < 1)
            {
                // No data was returned
                Console.WriteLine("DTA_Refinery jobs were not found for job range " + jobStart + " - " + jobEnd);
                return false;
            }

            // var workDir = mMgrSettings.GetParam(AnalysisMgrSettings.MGR_PARAM_WORK_DIR);
            // var postResultsToDB = true;

            // Note: add file DtaRefLogMassErrorExtractor to this project to use this functionality
            // var massErrorExtractor = new DtaRefLogMassErrorExtractor(mMgrSettings, workDir, mDebugLevel, postResultsToDB);

            foreach (DataRow curRow in dt.Rows)
            {
                var dataset = curRow["dataset"].CastDBVal<string>();
                var datasetID = curRow["dataset_id"].CastDBVal<int>();
                var job = curRow["job"].CastDBVal<int>();
                var dtaRefineryDataFolderPath = Path.Combine(curRow["dataset_folder_path"].CastDBVal<string>(),
                                                             curRow["output_folder"].CastDBVal<string>());

                if (!Directory.Exists(dtaRefineryDataFolderPath))
                {
                    dtaRefineryDataFolderPath = Path.Combine(curRow["transfer_folder_path"].CastDBVal<string>(), curRow["output_folder"].CastDBVal<string>());
                }

                if (Directory.Exists(dtaRefineryDataFolderPath))
                {
                    Console.WriteLine("Processing " + dtaRefineryDataFolderPath);
                    // massErrorExtractor.ParseDTARefineryLogFile(psmjob.Dataset, psmjob.DatasetID, psmjob.Job, psmjob.DtaRefineryDataFolderPath)
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

            const string exePath = @"C:\DMS_Programs\ProteoWizard\msconvert.exe";
            const string dataFilePath = @"C:\DMS_WorkDir\QC_ShewPartialInj_15_02-100ng_Run-1_20Jan16_Pippin_15-08-53.raw";
            const string arguments =
                dataFilePath +
                @" --filter ""peakPicking vendor mslevel=1-"" " +
                @" --filter ""threshold count 500 most-intense"" " +
                @" --mgf -o C:\DMS_WorkDir";

            mProgRunner = new RunDosProgram(workDir, mDebugLevel)
            {
                CreateNoWindow = true,
                CacheStandardOutput = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                // Allow the console output filename to be auto-generated
                ConsoleOutputFilePath = string.Empty
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

            var resultsDirectoryPath = Path.Combine(GetWorkDirPath(), "TestResults");
            var resultsDirectory = new DirectoryInfo(resultsDirectoryPath);

            if (!resultsDirectory.Exists)
                resultsDirectory.Create();

            var rand = new Random();

            for (var i = 0; i < 5; i++)
            {
                var outFilePath = Path.Combine(resultsDirectory.FullName, "TestOutFile" + i + ".txt");

                using var writer = new StreamWriter(new FileStream(outFilePath, FileMode.Create, FileAccess.Write));

                writer.WriteLine("Scan\tIntensity");

                for (var j = 1; j < 1000; j++)
                {
                    writer.WriteLine("{0}\t{1}", j, rand.Next(0, 10000));
                }
            }

            var analysisResults = new AnalysisResults(mMgrSettings, jobParams);
            analysisResults.CopyFailedResultsToArchiveDirectory(Path.Combine(GetWorkDirPath(), resultsDirectoryPath));
        }

        /// <summary>
        /// Archive a SEQUEST parameter file by copying to \\gigasax\dms_parameter_Files\SEQUEST
        /// </summary>
        public void TestArchiveFileStart()
        {
            const string paramFilePath = @"D:\Temp\sequest_N14_NE.params";
            const string targetFolderPath = @"\\gigasax\dms_parameter_Files\SEQUEST";

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
                    // Since the first 2 lines of a SEQUEST parameter file don't matter, and since the 3rd line can vary from computer to computer, we start the comparison at the 4th line

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
            const string remoteFilePath = @"\\gigasax\dms_parameter_Files\Formularity\PNNL_CIA_DB_1500_B.bin";
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
        /// Test copying a large FASTA file to a remote host
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
        /// <param name="datasetName">Dataset name</param>
        /// <param name="resultsFolder">Results folder path</param>
        public void TestConcatenation(string datasetName, string resultsFolder)
        {
            Console.WriteLine("Splitting concatenated DTA file");

            var fileSplitter = new SplitCattedFiles();
            var filesToSkip = new SortedSet<string>();

            fileSplitter.SplitCattedDTAsOnly(datasetName, resultsFolder, filesToSkip);

            Console.WriteLine("Completed splitting concatenated DTA file");
        }

        /// <summary>
        /// Instantiate an instance of AnalysisToolRunnerDtaSplit
        /// </summary>
        public void TestDTASplit()
        {
            const int debugLevel = 2;

            var jobParams = InitializeMgrAndJobParams(debugLevel);

            var statusTools = new StatusFile(mMgrSettings, "Status.xml", debugLevel);
            RegisterEvents(statusTools);

            var myEMSLUtilities = new MyEMSLUtilities(debugLevel, GetWorkDirPath(), true);
            RegisterEvents(myEMSLUtilities);

            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisResources.JOB_PARAM_DATASET_NAME, "QC_05_2_05Dec05_Doc_0508-08");
            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "NumberOfClonedSteps", "25");
            jobParams.SetParam(AnalysisJob.JOB_PARAMETERS_SECTION, "ClonedStepsHaveEqualNumSpectra", "True");

            var mgr = new FileInfo(AppUtils.GetAppPath());
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
        /// <param name="destinationDirectory">Destination directory</param>
        public bool TestProteinDBExport(string destinationDirectory)
        {
            // ReSharper disable StringLiteralTypo

            // Test what the Protein_Exporter does if a protein collection name is truncated (and thus invalid)
            var proteinCollectionList = "Geobacter_bemidjiensis_Bem_T_2006-10-10,Geobacter_lovelyi_SZ_2007-06-19,Geobacter_metallireducens_GS-15_2007-10-02,Geobacter_sp_";
            const string proteinOptions = "seq_direction=forward,filetype=fasta";

            // Test a 2 MB FASTA file:
            proteinCollectionList = "Micrococcus_luteus_NCTC2665_Uniprot_20160119,Tryp_Pig_Bov";

            // Test a 34 MB FASTA file
            // proteinCollectionList = "nr_ribosomal_2010-08-17,Tryp_Pig";

            // Test 100 MB FASTA file
            // legacyFasta = "na"
            // proteinCollectionList = "GWB1_Rifle_2011_9_13_0_1_2013-03-27,Tryp_Pig_Bov"

            // ReSharper restore StringLiteralTypo

            var success = TestProteinDBExport(destinationDirectory, "na", proteinCollectionList, proteinOptions);

            if (success)
            {
                IJobParams jobParams = InitializeManagerParams();
                jobParams.AddAdditionalParameter(AnalysisJob.PEPTIDE_SEARCH_SECTION, AnalysisResources.JOB_PARAM_GENERATED_FASTA_NAME, mFastaFileName);

                //const bool msgfPlus = true;
                //var jobNum = "12345";
                //var debugLevel = (short)(mMgrSettings.GetParam("DebugLevel", 1));

                //var javaProgLoc = @"C:\DMS_Programs\Java\jre11\bin\java.exe";
                //var msgfDbProgLoc = @"C:\DMS_Programs\MSGFPlus\MSGFPlus.jar";
                //bool fastaFileIsDecoy;
                //string fastaFilePath;

                // Uncomment the following if the MSGFDB plugin is associated with the solution
                //var tool = new AnalysisManagerMSGFDBPlugIn.MSGFDBUtils(
                //    mMgrSettings, jobParams, jobNum, mMgrSettings.GetParam(AnalysisMgrSettings.MGR_PARAM_WORK_DIR), debugLevel, msgfPlus);

                //RegisterEvents(tool);

                //float fastaFileSizeKB;

                //// Note that fastaFilePath will be populated by this method call
                //var result = tool.InitializeFastaFile(javaProgLoc, msgfDbProgLoc, out fastaFileSizeKB, out fastaFileIsDecoy, out fastaFilePath);
            }

            return success;
        }

        /// <summary>
        /// Test creation of a .fasta file from a protein collection
        /// </summary>
        /// <param name="destinationDirectory">Destination directory</param>
        /// <param name="legacyFasta">Legacy FASTA file name, or empty string if exporting protein collections</param>
        /// <param name="proteinCollectionList">Protein collection list</param>
        /// <param name="proteinOptions">Protein options</param>
        public bool TestProteinDBExport(string destinationDirectory, string legacyFasta, string proteinCollectionList, string proteinOptions)
        {
            // Instantiate FASTA tool if not already done
            if (mFastaTools == null)
            {
                if (string.IsNullOrEmpty(mFastaToolsCnStr))
                {
                    Console.WriteLine("Protein database connection string not specified");
                    return false;
                }

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(mFastaToolsCnStr, "AnalysisManager_TestProteinDBExport");

                mFastaTools = new OrganismDatabaseHandler.ProteinExport.GetFASTAFromDMS(connectionStringToUse);
                RegisterEvents(mFastaTools);

                mFastaTools.FileGenerationStarted += FileGenerationStarted;
                mFastaTools.FileGenerationCompleted += FileGenerationCompleted;
                mFastaTools.FileGenerationProgress += FileGenerationProgress;
            }

            // Initialize FASTA generation state variables
            mGenerationComplete = false;

            // Set up a timer to prevent an infinite loop if there's a FASTA generation problem
            mFastaTimer = new System.Timers.Timer();
            mFastaTimer.Elapsed += FastaTimer_Elapsed;

            mFastaTimer.Interval = FASTA_GEN_TIMEOUT_INTERVAL_SEC * 1000;
            mFastaTimer.AutoReset = false;

            // Create the FASTA file
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

            // Wait for FASTA creation to finish
            while (!mGenerationComplete)
            {
                Global.IdleLoop(2);
            }

            if (mFastaGenTimeOut)
            {
                // FASTA generator hung - report error and exit
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
        /// Display metadata regarding every process running on this system
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
        /// Test obtaining unique list of abbreviated names
        /// </summary>
        public void TestGetUniquePrefixes()
        {
            var datasets = new List<string>
            {
                "MCF10A_EGF_Plex_2_G_f05_28Jan21_Rage_Rep-21-01-01",
                "MCF10A_EGF_Plex_2_G_f06_28Jan21_Rage_Rep-21-01-01",
                "MCF10A_EGF_Plex_1_G_f05_28Jan21_Rage_Rep-21-01-01",
                "MCF10A_EGF_Plex_1_G_f06_28Jan21_Rage_Rep-21-01-01",
                "QC_Shew_21_01_Run-03_11Jul22_Oak_Jup-22-07-01",
                "QC_Shew_21_01_Run-01_08Jul22_Oak_Jup-22-07-01",
                "QC_Shew_21_01_TMT_R03_Bane_21Jun22_22-03-01",
                "QC_Shew_21_01_10ng_nanoPOTS_12Jul22_WBEH_50_22_07_01_FAIMS_r1",
                "QC_Shew_21_01_10ng_nanoPOTS_12Jul22_WBEH_50_22_07_01_FAIMS_r2",
                "QC_Shew_21_01_10ng_NanoPOTS_12Jul22_WBEH_50_22_07_01_FAIMS_r1",
            };

            var uniquePrefixTool = new ShortestUniquePrefix();

            // Keys in this dictionary are experiment group names
            // Values are the abbreviated name to use
            var datasetAbbreviations = uniquePrefixTool.GetShortestUniquePrefix(datasets, true);

            Console.WriteLine("{0,-61} {1}", "Name", "Abbreviation");

            foreach (var dataset in datasets)
            {
                Console.WriteLine("{0,-61} {1}", dataset, datasetAbbreviations[dataset]);
            }

            Console.WriteLine();
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

            foreach (ConsoleColor color in Enum.GetValues(typeof(ConsoleColor)))
            {
                Console.Write("{0,-12} [", color);

                Console.ForegroundColor = color;
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
        /// <remarks>
        /// <para>
        /// SQL Server connection string: "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI"
        /// </para>
        /// <para>
        /// PostgreSQL connection string: "Host=prismdb2;Port=5432;Database=dms;UserId=d3l243;"
        /// </para>
        /// </remarks>
        /// <param name="connectionString">Database connection string</param>
        public void TestDatabaseLogging(string connectionString)
        {
            var hostName = System.Net.Dns.GetHostName();
            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, "AnalysisManager_CodeTest: " + hostName);

            MainProcess.CreateDbLogger(connectionStringToUse, "CodeTest: " + hostName, true);

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

            var serverType = DbToolsFactory.GetServerTypeFromConnectionString(connectionStringToUse);

            if (serverType != DbServerTypes.MSSQLServer)
                return;

            var sqlServerLogger = new SQLServerDatabaseLogger
            {
                EchoMessagesToFileLogger = true
            };

            sqlServerLogger.ChangeConnectionInfo("CodeTest2", connectionStringToUse, "post_log_entry", "type", "message", "postedBy");
            sqlServerLogger.WriteLog(BaseLogger.LogLevels.FATAL, "SQL Server Fatal Test");

            var odbcConnectionString = ODBCDatabaseLogger.ConvertSqlServerConnectionStringToODBC(connectionStringToUse);
            var odbcLogger = new ODBCDatabaseLogger
            {
                EchoMessagesToFileLogger = true
            };
            odbcLogger.ChangeConnectionInfo("CodeTest2", odbcConnectionString, "post_log_entry", "type", "message", "postedBy", 128, 4096, 128);

            odbcLogger.WriteLog(BaseLogger.LogLevels.INFO, "ODBC Log Test");
            odbcLogger.WriteLog(BaseLogger.LogLevels.WARN, "ODBC Warning Test");
        }

        /// <summary>
        /// Call post_log_entry in the dms database on prismdb1
        /// </summary>
        public void TestDatabaseLoggingPostgres()
        {
            TestDatabaseLogging("Host=prismdb2;Port=5432;Database=dms;UserId=d3l243;");
        }

        /// <summary>
        /// Call post_log_entry in the DMS5 database on Gigasax
        /// </summary>
        public void TestDatabaseLoggingSqlServer()
        {
            TestDatabaseLogging("Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI");
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

            jobParams.SetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "legacyFastaFileName", "Uniprot_ArchaeaBacteriaFungi_SprotTrembl_2014-4-16.fasta");
            jobParams.SetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "OrganismName", "Combined_Organism_Rifle_SS");
            jobParams.SetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "ProteinCollectionList", "na");
            jobParams.SetParam(AnalysisJob.PEPTIDE_SEARCH_SECTION, "ProteinOptions", "na");

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

                LogMessage("R directory path: " + rBinPath);

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
            var dateThreshold = DateTime.Now.Subtract(new TimeSpan(15, 0, 0, 0));

            var sqlStr = string.Format("Select * From t_log_entries where posting_time >= '{0:yyyy-MM-dd}'", dateThreshold);

            const string connectionString = "Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms";
            const short retryCount = 2;
            const int timeoutSeconds = 30;

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, "CodeTest_TestRunQuery");

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, timeoutSeconds, true);
            RegisterEvents(dbTools);

            dbTools.GetQueryResultsDataTable(sqlStr, out var results, retryCount);

            var dataCount = 0;

            Console.WriteLine("{0,-10} {1,-21} {2,-20} {3}", "Entry_ID", "Date", "Posted_By", "Message");

            foreach (DataRow row in results.Rows)
            {
                Console.WriteLine("{0,-10} {1,-21:yyyy-MM-dd hh:mm tt} {2,-20} {3}", row[0], row[2], row[1], row[4]);
                dataCount++;

                if (dataCount >= 15)
                    break;
            }
        }

        /// <summary>
        /// Obtain job step parameters by querying a function
        /// </summary>
        public void TestQueryFunction()
        {
            const string connectionString = "Host=prismdb2;Port=5432;Database=dms;UserId=d3l243;";
            const short retryCount = 2;
            const int timeoutSeconds = 30;

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, "CodeTest_TestQueryFunction");

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, timeoutSeconds, debugMode: true);
            RegisterEvents(dbTools);

            const int jobNumber = 2191759;
            const int stepNumber = 1;

            // Query function sw.get_job_step_params_as_table_use_history()

            var sqlStr = string.Format(
                "SELECT Section, Name, Value FROM sw.get_job_step_params_as_table_use_history({0}, {1})",
                jobNumber, stepNumber);

            dbTools.GetQueryResultsDataTable(sqlStr, out var results, retryCount);

            foreach (DataRow row in results.Rows)
            {
                Console.WriteLine("{0,-16} {1, -30} {2}", row[0], row[1] + ":", row[2]);
            }
        }

        /// <summary>
        /// Obtain job step parameters using a procedure
        /// </summary>
        public void TestRunSP()
        {
            const string connectionString = "Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms";
            const short retryCount = 2;
            const int timeoutSeconds = 30;

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, "CodeTest_TestRunSP");

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, timeoutSeconds, debugMode: true);
            RegisterEvents(dbTools);

            const int jobNumber = 1026591;
            const int stepNumber = 1;

            var cmd = dbTools.CreateCommand("get_job_step_params_as_table", CommandType.StoredProcedure);

            dbTools.AddParameter(cmd, "@job", SqlType.Int).Value = jobNumber;
            dbTools.AddParameter(cmd, "@step", SqlType.Int).Value = stepNumber;
            dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, string.Empty, ParameterDirection.InputOutput);

            var resCode = dbTools.ExecuteSPDataTable(cmd, out var results, retryCount);

            foreach (DataRow row in results.Rows)
            {
                Console.WriteLine("{0}: {1}", row[0], row[1]);
            }
        }

        /// <summary>
        /// Convert a zip file to a gzip file
        /// </summary>
        /// <param name="zipFilePath">Zip file path</param>
        public void ConvertZipToGZip(string zipFilePath)
        {
            const int debugLevel = 2;
            const string workDirPath = @"C:\DMS_WorkDir";

            var zipTools = new ZipFileTools(debugLevel, workDirPath);
            RegisterEvents(zipTools);

            zipTools.UnzipFile(zipFilePath);

            var workDir = new DirectoryInfo(workDirPath);

            foreach (var mzidFile in workDir.GetFiles("*.mzid"))
            {
                zipTools.GZipFile(mzidFile.FullName, true);
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
        /// <remarks>This uses System.IO.Compression.ZipFile</remarks>
        public bool TestUnzip(string zipFilePath, string outFolderPath)
        {
            const int debugLevel = 2;
            var resourcer = GetResourcesObject(debugLevel);

            var success = resourcer.UnzipFileStart(zipFilePath, outFolderPath, "TestUnzip");
            // success = resourcer.UnzipFileStart(zipFilePath, outFolderPath, "TestUnzip", true)

            return success;
        }

        /// <summary>
        /// Test zipping a file
        /// </summary>
        /// <remarks>This uses System.IO.Compression.ZipFile</remarks>
        public void TestZip()
        {
            var toolRunner = GetCodeTestToolRunner();

            const string sourceFilePath = @"F:\Temp\ZipTest\QExact01\UDD-1_27Feb13_Gimli_12-07-03_HCD.mgf";

            toolRunner.ZipFile(sourceFilePath, false);

            var zippedFile = @"F:\Temp\ZipTest\QExact01\" + Path.GetFileNameWithoutExtension(sourceFilePath) + ".zip";

            toolRunner.UnzipFile(zippedFile);

            toolRunner.UnzipFile(zippedFile, @"F:\Temp\ZipTest\UnzipTarget");

            var zipTools = new ZipFileTools(1, GetWorkDirPath());
            RegisterEvents(zipTools);

            zipTools.ZipDirectory(@"F:\Temp\ZipTest\QExact01\", @"F:\Temp\ZipTest\QExact01_Folder.zip");
        }

        /// <summary>
        /// Test System.IO.Compression.ZipFile
        /// </summary>
        public void TestZipTools()
        {
            var zipTools = new ZipFileTools(1, @"C:\DMS_WorkDir");
            RegisterEvents(zipTools);

            zipTools.UnzipFile(@"C:\DMS_WorkDir\Temp.zip", @"C:\DMS_WorkDir", "*.png");

            foreach (var item in zipTools.MostRecentUnzippedFiles)
            {
                Console.WriteLine(item.Key + " - " + item.Value);
            }
        }

        /// <summary>
        /// Retrieve and decompress MALDI data
        /// </summary>
        /// <param name="sourceDatasetFolder">Source dataset directory</param>
        public bool TestMALDIDataUnzip(string sourceDatasetFolder)
        {
            const int debugLevel = 2;

            var resourceTester = new ResourceTestClass();

            var statusTools = new StatusFile(mMgrSettings, "Status.xml", debugLevel);
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

            var success = resourceTester.FileSearchTool.RetrieveBrukerMALDIImagingFolders(true);

            // success = resourceTester.UnzipFileStart(zipFilePath, outFolderPath, "TestUnzip", true)

            return success;
        }

        /// <summary>
        /// Writes the status file
        /// </summary>
        public void TestStatusLogging()
        {
            var statusTools = new StatusFile(mMgrSettings, "Status.xml", 2);
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
        /// Test converting messages to XML, including truncating the messages to a given length
        /// </summary>
        public void TestTruncateString()
        {
            var values = new List<string>
            {
                "This is a test string to validate.",
                "This is a test string to validate and 32 < 50 while 40 > 20. In addition, 15 < 18 but 39 > 45 is false.",
                "The fruit of the day is apples & pears. The large bowl is 15 >= 7. Other information could be added here.",
                "The fruit of the day is apples & pears.\nThe fruit tomorrow is bananas & pineapples.\nSunday is watermelon.",
                "The fruit of the day is apples & pears.\r\nThe fruit tomorrow is bananas & pineapples.\r\nSunday is watermelon."
            };

            // Create a new memory stream in which to write the XML
            var memStream = new MemoryStream();

            var settings = new XmlWriterSettings
            {
                Encoding = Encoding.UTF8,
                Indent = true,
                IndentChars = "  ",
                NewLineHandling = NewLineHandling.None
            };

            string xmlText;

            using (var writer = XmlWriter.Create(memStream, settings))
            {
                // Create the XML document in memory
                writer.WriteStartDocument(true);
                writer.WriteComment("Test document");

                writer.WriteStartElement("Root");
                writer.WriteStartElement("Messages");

                const int MAX_LENGTH = 94;

                foreach (var value in values)
                {
                    var validatedText = StatusFile.ValidateTextLength(value, MAX_LENGTH);

                    writer.WriteElementString("Message", validatedText);
                }

                writer.WriteEndElement(); // Messages
                writer.WriteEndElement(); // Root

                // Close out the XML document (but do not close the writer yet)
                writer.WriteEndDocument();
                writer.Flush();

                // Now use a StreamReader to copy the XML text to a string variable
                memStream.Seek(0, SeekOrigin.Begin);
                var memoryStreamReader = new StreamReader(memStream);
                xmlText = memoryStreamReader.ReadToEnd();

                Console.WriteLine();
                Console.WriteLine(xmlText);
                Console.WriteLine();
            }

            using var fileWriter = new StreamWriter(new FileStream("TestMessages.xml", FileMode.Create, FileAccess.Write, FileShare.None));

            fileWriter.WriteLine(xmlText);

            Console.WriteLine("See file TestMessages.xml");
            Console.WriteLine();
        }

        /// <summary>
        /// Test zipping and unzipping with System.IO.Compression.ZipFile
        /// </summary>
        public void TestZipAndUnzip()
        {
            // Zip Benchmark stats

            // January 2011 tests with a 611 MB file
            //   IonicZip    unzips the file in 70 seconds (reading/writing to the same drive)
            //   IonicZip    unzips the file in 62 seconds (reading/writing from different drives)
            //   WinRar      unzips the file in 36 seconds (reading/writing from different drives)
            //   PKZipC      unzips the file in 38 seconds (reading/writing from different drives)

            // September 2017 tests
            //   IonicZip compressed a 2.6 GB UIMF file in 86 seconds (reading/writing to the same drive)
            //   IonicZip compressed a 556 MB text file in 3.7 seconds (creating a 107 MB file)
            //   IonicZip compressed a folder with 996 MB of data (FASTA files, text files, one .gz file) in 12 seconds (creating a 348 MB zip file)

            var zipTools = new ZipFileTools(3, @"F:\Temp");
            RegisterEvents(zipTools);

            var stopWatch = new Stopwatch();

            stopWatch.Start();
            zipTools.ZipFile(@"F:\Temp\TestDataFile.txt", false, verifyZipFile: true);
            stopWatch.Stop();
            Console.WriteLine("Elapsed time: {0:F3} seconds", stopWatch.ElapsedMilliseconds / 1000.0);

            // 200 MB file: QC_Blank_C_RP_Neg_13May24_Bilbo_Hypergold-24-05-05.raw
            // 2 GB file: OHSU_mortality_lipids_137_Pos_12Sep17_Brandi-WCSH7908.uimf
            // const string SOURCE_FILE = @"F:\Temp\QC_Blank_C_RP_Neg_13May24_Bilbo_Hypergold-24-05-05.raw";

            stopWatch.Reset();
            stopWatch.Start();

            // Uncomment to zip the .raw file
            // var zipFilePath = ZipFileTools.GetZipFilePathForFile(SOURCE_FILE);
            // zipTools.ZipFile(SOURCE_FILE, false, zipFilePath, true);

            stopWatch.Stop();
            Console.WriteLine("Elapsed time: {0:F3} seconds", stopWatch.ElapsedMilliseconds / 1000.0);

            const string ZIPPED_FOLDER_FILE = @"F:\Temp\ZippedFolderTest.zip";
            const string ZIPPED_FOLDER_FILE_NO_RECURSE = @"F:\Temp\ZippedFolderTest_NoRecurse.zip";
            const string ZIPPED_FOLDER_FILE_FILTERED = @"F:\Temp\ZippedFolderTest_Filtered.zip";

            stopWatch.Reset();
            stopWatch.Start();
            zipTools.ZipDirectory(@"F:\Temp\FolderTest", ZIPPED_FOLDER_FILE);
            stopWatch.Stop();
            Console.WriteLine("Elapsed time: {0:F3} seconds", stopWatch.ElapsedMilliseconds / 1000.0);

            stopWatch.Reset();
            stopWatch.Start();
            zipTools.ZipDirectory(@"F:\Temp\FolderTest", ZIPPED_FOLDER_FILE_NO_RECURSE, false);
            stopWatch.Stop();
            Console.WriteLine("Elapsed time: {0:F3} seconds", stopWatch.ElapsedMilliseconds / 1000.0);

            stopWatch.Reset();
            stopWatch.Start();
            zipTools.ZipDirectory(@"F:\Temp\FolderTest", ZIPPED_FOLDER_FILE_FILTERED, true, "Scratch1*");
            stopWatch.Stop();
            Console.WriteLine("Elapsed time: {0:F3} seconds", stopWatch.ElapsedMilliseconds / 1000.0);
            stopWatch.Reset();

            var fileList = new List<FileInfo>
            {
                new(@"F:\Temp\Scratch1.sql"),
                new(@"F:\Temp\Scratch2.sql"),
                new(@"F:\Temp\Scratch3.sql")
            };

            const string ZIPPED_FILE_GROUP1 = @"F:\Temp\TestZippedFiles_OneDirectory.zip";

            stopWatch.Start();
            zipTools.ZipFiles(fileList, ZIPPED_FILE_GROUP1);
            stopWatch.Stop();
            Console.WriteLine("Elapsed time: {0:F3} seconds", stopWatch.ElapsedMilliseconds / 1000.0);

            stopWatch.Reset();
            fileList.Add(new FileInfo(@"F:\Temp\FASTA\Tryp_Pig_Bov.fasta"));
            fileList.Add(new FileInfo(@"F:\Temp\FolderTest\SubDir\Scratch.txt"));
            fileList.Add(new FileInfo(@"F:\Temp\FolderTest\SubDir\Scratch1.txt"));
            fileList.Add(new FileInfo(@"F:\Temp\FolderTest\SubDir\Scratch2.txt"));
            fileList.Add(new FileInfo(@"F:\Temp\FolderTest\SubDir\Scratch2.txt"));      // Adding the same file twice will duplicate the file in the zip file

            const string ZIPPED_FILE_GROUP2 = @"F:\Temp\TestZippedFiles_TwoDirectories.zip";

            stopWatch.Start();
            zipTools.ZipFiles(fileList, ZIPPED_FILE_GROUP2, true);
            stopWatch.Stop();
            Console.WriteLine("Elapsed time: {0:F3} seconds", stopWatch.ElapsedMilliseconds / 1000.0);

            stopWatch.Start();
            zipTools.AddToZipFile(ZIPPED_FILE_GROUP2, new FileInfo(@"F:\Temp\Scratch2.txt"));
            stopWatch.Stop();
            Console.WriteLine("Elapsed time: {0:F3} seconds", stopWatch.ElapsedMilliseconds / 1000.0);

            var zippedFileCreateNew = new FileInfo(@"F:\Temp\TestZippedFiles_NewFile.zip");

            if (zippedFileCreateNew.Exists)
                zippedFileCreateNew.Delete();

            stopWatch.Start();
            zipTools.AddToZipFile(zippedFileCreateNew.FullName, new FileInfo(@"F:\Temp\Scratch2.txt"));
            stopWatch.Stop();
            Console.WriteLine("Elapsed time: {0:F3} seconds", stopWatch.ElapsedMilliseconds / 1000.0);

            zipTools.UnzipFile(ZIPPED_FOLDER_FILE, @"F:\Temp\FolderTest_RoundTrip", string.Empty, ZipFileTools.ExtractExistingFileBehavior.DoNotOverwrite);

            zipTools.UnzipFile(ZIPPED_FOLDER_FILE, @"F:\Temp\FolderTest_RoundTrip", string.Empty, ZipFileTools.ExtractExistingFileBehavior.OverwriteSilently);

            zipTools.UnzipFile(ZIPPED_FOLDER_FILE, @"F:\Temp\FolderTest_RoundTrip_Filtered", "Scratch1*", ZipFileTools.ExtractExistingFileBehavior.DoNotOverwrite);

            zipTools.UnzipFile(ZIPPED_FILE_GROUP2, @"F:\Temp\FolderTest_TwoDirectories_RoundTrip");

            zipTools.UnzipFile(ZIPPED_FILE_GROUP2, @"F:\Temp\FolderTest_TwoDirectories_Filtered_RoundTrip", "Scratch1*");
        }

        /// <summary>
        /// Determine the InstrumentID value used for a set of analysis jobs
        /// </summary>
        /// <param name="jobList">Comma or new-line separated list of job numbers</param>
        /// <param name="resultsFilePath">Results file path</param>
        public void ExamineInstrumentID(string jobList, string resultsFilePath = @"C:\Temp\InstrumentIDsByJob.txt")
        {
            try
            {
                var analysisJobs = jobList.Split('\t', ',', '\r', '\n');
                var jobNumbers = new List<int>();

                foreach (var item in analysisJobs)
                {
                    if (int.TryParse(item, out var jobNumber))
                        jobNumbers.Add(jobNumber);
                }

                // ReSharper disable StringLiteralTypo

                var sql = string.Format(
                    "SELECT J.job, J.dataset, J.instrumentname, J.storagepathserver, J.datasetfolder, J.resultsfolder, J.parameterfilename, DSType.dataset_type, DSType.acq_start " +
                    "FROM V_Analysis_Job_Export J " +
                    "     INNER JOIN ( SELECT id, " +
                    "                         dataset_type, " +
                    "                         acq_start " +
                    "                  FROM V_Dataset_List_Report_2 ) DSType " +
                    "       ON J.datasetid = DSType.id " +
                    "WHERE J.job In ({0}) " +
                    "ORDER BY J.job", string.Join(", ", jobNumbers));

                // ReSharper restore StringLiteralTypo

                const string connectionString = "Host=prismdb2.emsl.pnl.gov;Port=5432;Database=dms;UserId=svc-dms";
                const short retryCount = 2;

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, "CodeTest_ExamineInstrumentID");

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: true);
                RegisterEvents(dbTools);

                var success = dbTools.GetQueryResultsDataTable(sql, out var dt, retryCount);

                if (!success)
                {
                    Console.WriteLine("Repeated errors running database query");
                }

                if (dt.Rows.Count < 1)
                {
                    // No data was returned
                    Console.WriteLine("Job numbers were not found in V_Analysis_Job_Export");
                    return;
                }

                Console.WriteLine("Writing results to " + resultsFilePath);

                using var writer = new StreamWriter(new FileStream(resultsFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));

                writer.WriteLine("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}", "Job", "Dataset", "Instrument", "InstrumentID_Param", "InstrumentMode_ConsoleOutput", "Dataset_Scan_Types", "Acq_Start");

                foreach (DataRow curRow in dt.Rows)
                {
                    var dataset = curRow["dataset"].CastDBVal<string>();
                    var job = curRow["job"].CastDBVal<int>();
                    var instrument = curRow["instrumentname"].CastDBVal<string>();
                    var storagePathServer = curRow["storagepathserver"].CastDBVal<string>();
                    var datasetFolder = curRow["datasetfolder"].CastDBVal<string>();
                    var resultsDirectory = curRow["resultsfolder"].CastDBVal<string>();
                    var parameterFile = curRow["parameterfilename"].CastDBVal<string>();
                    var datasetType = curRow["dataset_type"].CastDBVal<string>();
                    var acqStart = curRow["acq_start"].CastDBVal<string>();

                    var jobDirectory = new DirectoryInfo(Path.Combine(storagePathServer, datasetFolder, resultsDirectory));

                    if (!jobDirectory.Exists)
                    {
                        ConsoleMsgUtils.ShowWarning("Results not found for job {0}: {1}", job, jobDirectory.FullName);
                        continue;
                    }

                    var files = jobDirectory.GetFiles(parameterFile).ToList();

                    FileInfo jobParameterFile;

                    if (files.Count > 0)
                    {
                        jobParameterFile = files[0];
                    }
                    else
                    {
                        // Parameter file not found; it was likely renamed
                        // Look for any MS-GF+ parameter file
                        var files2 = jobDirectory.GetFiles("MSGF*.txt").ToList();

                        if (files2.Count > 0)
                        {
                            FileInfo foundFile = null;

                            foreach (var item in files2)
                            {
                                if (item.Name.EndsWith("ConsoleOutput.txt", StringComparison.OrdinalIgnoreCase))
                                    continue;

                                if (item.Name.EndsWith("_ModDefs.txt", StringComparison.OrdinalIgnoreCase))
                                    continue;

                                if (item.Name.Equals("MSGFDB_mods.txt", StringComparison.OrdinalIgnoreCase))
                                    continue;

                                foundFile = item;
                                break;
                            }

                            if (foundFile == null)
                                continue;

                            jobParameterFile = foundFile;

                            ConsoleMsgUtils.ShowWarning("Parameter file not found for job {0}, but found {1} instead", job, jobParameterFile.Name);
                        }
                        else
                        {
                            ConsoleMsgUtils.ShowWarning("Parameter file not found for job {0}: {1}", job, parameterFile);
                            jobParameterFile = null;
                        }
                    }

                    var instrumentID = string.Empty;

                    if (jobParameterFile != null)
                    {
                        var paramFileReader = new KeyValueParamFileReader("MS-GF+", jobParameterFile.FullName);
                        RegisterEvents(paramFileReader);

                        var paramFileSuccess = paramFileReader.ParseKeyValueParameterFile(out var paramFileEntries);

                        if (!paramFileSuccess)
                        {
                            ConsoleMsgUtils.ShowWarning("ParseKeyValueParameterFileGetAllLines returned false for job {0}, parameter file {1}",
                                job, jobParameterFile.Name);

                            continue;
                        }

                        foreach (var parameter in paramFileEntries)
                        {
                            if (parameter.Key.Equals("InstrumentID", StringComparison.OrdinalIgnoreCase))
                            {
                                instrumentID = parameter.Value;
                                break;
                            }
                        }
                    }

                    // Extract the instrument mode from the console output file
                    FileInfo consoleOutputFile;
                    var consoleOutputFile1 = jobDirectory.GetFiles("MSGFPlus_ConsoleOutput.txt").ToList();
                    var consoleOutputFile2 = jobDirectory.GetFiles("MSGFDB_ConsoleOutput.txt").ToList();

                    if (consoleOutputFile1.Count > 0)
                    {
                        consoleOutputFile = consoleOutputFile1[0];
                    }
                    else if (consoleOutputFile2.Count > 0)
                    {
                        consoleOutputFile = consoleOutputFile2[0];
                    }
                    else
                    {
                        ConsoleMsgUtils.ShowWarning("Console output file not found for job {0}: {1}", job, jobDirectory.FullName);
                        consoleOutputFile = null;
                    }

                    var instrumentMode = string.Empty;

                    if (consoleOutputFile != null)
                    {
                        var reader = new StreamReader(new FileStream(consoleOutputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                        while (!reader.EndOfStream)
                        {
                            var dataLine = reader.ReadLine();

                            if (string.IsNullOrWhiteSpace(dataLine))
                                continue;

                            if (!dataLine.Trim().StartsWith("Instrument:"))
                                continue;

                            instrumentMode = dataLine.Trim().Substring("Instrument:".Length).Trim();
                        }
                    }

                    writer.WriteLine("{0}\t{1}\t{2}\t{3}\t{4}\t{5}", job, dataset, instrument, instrumentID, instrumentMode, datasetType, acqStart);
                }
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Exception in ExamineInstrumentID", ex);
            }
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
            // Reset the FASTA generation timer
            mFastaTimer.Start();
        }

        private void FileGenerationCompleted(string fullOutputPath)
        {
            // Get the name of the FASTA file that was generated
            mFastaFileName = Path.GetFileName(fullOutputPath);

            // Stop the FASTA generation timer so no false error occurs
            mFastaTimer?.Stop();

            // Set the completion flag
            mGenerationComplete = true;
        }

        private void FileGenerationProgress(string statusMsg, double fractionDone)
        {
            // Reset the FASTA generation timer
            mFastaTimer.Start();
        }

        private void FastaTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            // If this event occurs, it means there was a hang during FASTA generation and the manager will have to quit
            mFastaTimer.Stop();

            // Stop the timer to prevent false errors
            mFastaGenTimeOut = true;

            // Set the timeout flag so an error will be reported
            mGenerationComplete = true;

            // Set the completion flag so the FASTA generation wait loop will exit
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
        /// Display the system path environment variable
        /// </summary>
        public void TestGetSystemPath()
        {
            var searchPath = Environment.GetEnvironmentVariable("Path");

            var searchPathProcess = Environment.GetEnvironmentVariable("Path", EnvironmentVariableTarget.Process);

            // This is only valid on Windows
            var searchPathUser = Environment.GetEnvironmentVariable("Path", EnvironmentVariableTarget.User);

            // This is only valid on Windows
            var searchPathMachine = Environment.GetEnvironmentVariable("Path", EnvironmentVariableTarget.Machine);

            Console.WriteLine("System path values");
            Console.WriteLine("{0,10}{1}", "Default:", searchPath);
            Console.WriteLine();

            if (searchPathProcess != null && searchPathProcess.Equals(searchPath))
            {
                Console.WriteLine("{0,10}{1}", "Process:", "Same as the default path");
            }
            else
            {
                Console.WriteLine("{0,10}{1}", "Process:", searchPathProcess);
            }

            Console.WriteLine();

            Console.WriteLine("{0,10}{1}", "User:", searchPathUser);
            Console.WriteLine();

            Console.WriteLine("{0,10}{1}", "Machine:", searchPathMachine);
            Console.WriteLine();
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
    }
}
