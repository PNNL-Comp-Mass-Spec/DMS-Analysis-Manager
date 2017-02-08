using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text.RegularExpressions;
using System.Xml;
using AnalysisManagerBase;

namespace AnalysisManagerProg
{
    public class clsCodeTest : clsLoggerBase
    {
        //Imports Protein_Exporter
        //Imports Protein_Exporter.ExportProteinCollectionsIFC

        private const string WORKING_DIRECTORY = "E:\\DMS_WorkDir";
        private Protein_Exporter.ExportProteinCollectionsIFC.IGetFASTAFromDMS withEventsField_m_FastaTools;
        private Protein_Exporter.ExportProteinCollectionsIFC.IGetFASTAFromDMS m_FastaTools
        {
            get { return withEventsField_m_FastaTools; }
            set
            {
                if (withEventsField_m_FastaTools != null)
                {
                    withEventsField_m_FastaTools.FileGenerationStarted -= m_FastaTools_FileGenerationStarted1;
                    withEventsField_m_FastaTools.FileGenerationCompleted -= m_FastaTools_FileGenerationCompleted;
                    withEventsField_m_FastaTools.FileGenerationProgress -= m_FastaTools_FileGenerationProgress;
                }
                withEventsField_m_FastaTools = value;
                if (withEventsField_m_FastaTools != null)
                {
                    withEventsField_m_FastaTools.FileGenerationStarted += m_FastaTools_FileGenerationStarted1;
                    withEventsField_m_FastaTools.FileGenerationCompleted += m_FastaTools_FileGenerationCompleted;
                    withEventsField_m_FastaTools.FileGenerationProgress += m_FastaTools_FileGenerationProgress;
                }
            }
        }
        private bool m_GenerationStarted = false;
        private bool m_GenerationComplete = false;
        private string m_FastaToolsCnStr = "Data Source=proteinseqs;Initial Catalog=Protein_Sequences;Integrated Security=SSPI;";
        private string m_FastaFileName = "";
        private System.Timers.Timer withEventsField_m_FastaTimer;
        private System.Timers.Timer m_FastaTimer
        {
            get { return withEventsField_m_FastaTimer; }
            set
            {
                if (withEventsField_m_FastaTimer != null)
                {
                    withEventsField_m_FastaTimer.Elapsed -= m_FastaTimer_Elapsed;
                }
                withEventsField_m_FastaTimer = value;
                if (withEventsField_m_FastaTimer != null)
                {
                    withEventsField_m_FastaTimer.Elapsed += m_FastaTimer_Elapsed;
                }
            }
        }

        private bool m_FastaGenTimeOut = false;

        private readonly AnalysisManagerBase.IMgrParams m_mgrParams;

        private string mConsoleOutputErrorMsg = string.Empty;
        private string m_EvalMessage;

        private int m_EvalCode;
        private float m_Progress;
        private int m_MaxScanInFile;
        private FileSystemWatcher withEventsField_mDTAWatcher;
        private FileSystemWatcher mDTAWatcher
        {
            get { return withEventsField_mDTAWatcher; }
            set
            {
                if (withEventsField_mDTAWatcher != null)
                {
                    withEventsField_mDTAWatcher.Created -= mDTAWatcher_Created;
                }
                withEventsField_mDTAWatcher = value;
                if (withEventsField_mDTAWatcher != null)
                {
                    withEventsField_mDTAWatcher.Created += mDTAWatcher_Created;
                }
            }
        }

        private DateTime mLastStatusTime;
        // 7.5 minutes
        private const int FASTA_GEN_TIMEOUT_INTERVAL_SEC = 450;

        private struct udtPSMJobInfoType
        {
            public string Dataset;
            public int DatasetID;
            public int Job;
            public string DtaRefineryDataFolderPath;
        }

        #region "Properties"
        public bool TraceMode { get; set; }
        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <remarks></remarks>
        public clsCodeTest()
        {
            const string CUSTOM_LOG_SOURCE_NAME = "Analysis Manager";
            const string CUSTOM_LOG_NAME = "DMS_AnalysisMgr";
            const bool TRACE_MODE_ENABLED = true;

            m_DebugLevel = 2;
            mLastStatusTime = DateTime.UtcNow.AddMinutes(-1);

            // Get settings from config file
            Dictionary<string, string> lstMgrSettings = new Dictionary<string, string>();

            try
            {
                lstMgrSettings = clsMainProcess.LoadMgrSettingsFromFile();

                m_mgrParams = new clsAnalysisMgrSettings(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME, lstMgrSettings, clsGlobal.GetAppFolderPath(), TRACE_MODE_ENABLED);

                m_DebugLevel = 2;

                m_mgrParams.SetParam("workdir", "E:\\DMS_WorkDir");
                m_mgrParams.SetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME, "Monroe_Test");
                m_mgrParams.SetParam("debuglevel", m_DebugLevel.ToString());
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine("===============================================================");
                Console.WriteLine("Exception loading settings from AnalysisManagerProg.exe.config: " + ex.Message);
                Console.WriteLine("===============================================================");
                Console.WriteLine();
                System.Threading.Thread.Sleep(500);
            }
        }

        public void DisplayDllVersions(string displayDllPath, string fileNameFileSpec = "*.dll")
        {
            try
            {
                DirectoryInfo diSourceFolder = default(DirectoryInfo);

                if (string.IsNullOrWhiteSpace(displayDllPath))
                {
                    diSourceFolder = new DirectoryInfo(".");
                }
                else
                {
                    diSourceFolder = new DirectoryInfo(displayDllPath);
                }

                List<FileInfo> lstFiles = default(List<FileInfo>);
                if (string.IsNullOrWhiteSpace(fileNameFileSpec))
                {
                    lstFiles = diSourceFolder.GetFiles("*.dll").ToList();
                }
                else
                {
                    lstFiles = diSourceFolder.GetFiles(fileNameFileSpec).ToList();
                }

                var dctResults = new Dictionary<string, KeyValuePair<string, string>>(StringComparer.CurrentCultureIgnoreCase);
                var dctErrors = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

                Console.WriteLine("Obtaining versions for " + lstFiles.Count + " files");

                foreach (var fiFile in lstFiles)
                {
                    try
                    {
                        Console.Write(".");

                        Assembly fileAssembly = System.Reflection.Assembly.LoadFrom(fiFile.FullName);
                        string fileVersion = fileAssembly.ImageRuntimeVersion;
                        var frameworkVersion = "??";

                        var customAttributes = fileAssembly.GetCustomAttributes(typeof(TargetFrameworkAttribute)).ToList();
                        if ((customAttributes != null) && customAttributes.Count > 0)
                        {
                            var frameworkAttribute = (TargetFrameworkAttribute)customAttributes.First();
                            frameworkVersion = frameworkAttribute.FrameworkDisplayName;
                        }
                        else if (fileVersion.StartsWith("v1.") || fileVersion.StartsWith("v2."))
                        {
                            frameworkVersion = string.Empty;
                        }

                        if (dctResults.ContainsKey(fiFile.FullName))
                        {
                            Console.WriteLine("Skipping duplicate file: " + fiFile.Name + ", " + fileVersion + " and " + frameworkVersion);
                        }
                        else
                        {
                            dctResults.Add(fiFile.FullName, new KeyValuePair<string, string>(fileVersion, frameworkVersion));
                        }
                    }
                    catch (BadImageFormatException ex)
                    {
                        // This may have been a .NET DLL missing a dependency
                        // Try a reflection-only load

                        try
                        {
                            Assembly fileAssembly2 = Assembly.ReflectionOnlyLoadFrom(fiFile.FullName);
                            string fileVersion2 = fileAssembly2.ImageRuntimeVersion;

                            if (dctResults.ContainsKey(fiFile.FullName))
                            {
                                Console.WriteLine("Skipping duplicate file: " + fiFile.Name + ", " + fileVersion2 + " (missing dependencies)");
                            }
                            else
                            {
                                dctResults.Add(fiFile.FullName, new KeyValuePair<string, string>(fileVersion2, "Unknown, missing dependencies"));
                            }
                        }
                        catch (Exception ex2)
                        {
                            if (dctErrors.ContainsKey(fiFile.FullName))
                            {
                                Console.WriteLine("Skipping duplicate error: " + fiFile.Name + ": " + ex2.Message);
                            }
                            else
                            {
                                dctErrors.Add(fiFile.FullName, ex.Message);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        if (dctErrors.ContainsKey(fiFile.FullName))
                        {
                            Console.WriteLine("Skipping duplicate error: " + fiFile.Name + ": " + ex.Message);
                        }
                        else
                        {
                            dctErrors.Add(fiFile.FullName, ex.Message);
                        }
                    }
                }

                Console.WriteLine();
                Console.WriteLine();

                var query = (from item in dctResults orderby item.Key select item).ToList();

                Console.WriteLine(string.Format("{0,-50} {1,-20} {2}", "Filename", ".NET Version", "Target Framework"));
                foreach (var result in query)
                {
                    Console.WriteLine(string.Format("{0,-50} {1,-20} {2}", Path.GetFileName(result.Key), " " + result.Value.Key, result.Value.Value));
                }

                if (dctErrors.Count > 0)
                {
                    Console.WriteLine();
                    Console.WriteLine();
                    Console.WriteLine("DLLs likely not .NET");

                    var errorList = (from item in dctErrors orderby item.Key select item).ToList();

                    Console.WriteLine(string.Format("{0,-30} {1}", "Filename", "Error"));

                    foreach (var result in errorList)
                    {
                        Console.Write(string.Format("{0,-30} ", Path.GetFileName(result.Key)));
                        var startIndex = 0;
                        while (startIndex < result.Value.Length)
                        {
                            if (startIndex > 0)
                            {
                                Console.Write(string.Format("{0,-30} ", string.Empty));
                            }

                            if (startIndex + 80 > result.Value.Length)
                            {
                                Console.WriteLine(result.Value.Substring(startIndex, result.Value.Length - startIndex));
                                break;
                            }
                            else
                            {
                                Console.WriteLine(result.Value.Substring(startIndex, 80));
                            }

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

        private clsAnalysisJob InitializeMgrAndJobParams(short intDebugLevel)
        {
            var objJobParams = new clsAnalysisJob(m_mgrParams, intDebugLevel);

            m_mgrParams.SetParam("workdir", WORKING_DIRECTORY);
            m_mgrParams.SetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME, "Monroe_Test");
            m_mgrParams.SetParam("debuglevel", intDebugLevel.ToString());

            objJobParams.SetParam("StepParameters", "StepTool", "TestStepTool");
            objJobParams.SetParam("JobParameters", "ToolName", "TestTool");

            objJobParams.SetParam("StepParameters", "Job", "12345");
            objJobParams.SetParam("StepParameters", "OutputFolderName", "Tst_Results");

            return objJobParams;
        }

        private clsCodeTestAM GetCodeTestToolRunner(out clsAnalysisJob objJobParams, out clsMyEMSLUtilities myEMSLUtilities)
        {
            const short DEBUG_LEVEL = 2;

            objJobParams = InitializeMgrAndJobParams(DEBUG_LEVEL);

            clsStatusFile objStatusTools = new clsStatusFile("Status.xml", DEBUG_LEVEL);
            clsSummaryFile objSummaryFile = new clsSummaryFile();

            myEMSLUtilities = new clsMyEMSLUtilities(DEBUG_LEVEL, WORKING_DIRECTORY);
            RegisterEvents(myEMSLUtilities);

            var objToolRunner = new clsCodeTestAM();
            objToolRunner.Setup(m_mgrParams, objJobParams, objStatusTools, objSummaryFile, myEMSLUtilities);

            return objToolRunner;
        }

        private clsResourceTestClass GetResourcesObject(int intDebugLevel)
        {
            IJobParams objJobParams = default(IJobParams);
            objJobParams = new clsAnalysisJob(m_mgrParams, 0);

            return GetResourcesObject(intDebugLevel, objJobParams);
        }

        private clsResourceTestClass GetResourcesObject(int intDebugLevel, IJobParams objJobParams)
        {
            var objResources = new clsResourceTestClass();

            clsStatusFile objStatusTools = new clsStatusFile("Status.xml", intDebugLevel);

            clsMyEMSLUtilities myEMSLUtilities = new clsMyEMSLUtilities(intDebugLevel, WORKING_DIRECTORY);

            m_mgrParams.SetParam("workdir", WORKING_DIRECTORY);
            m_mgrParams.SetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME, "Monroe_Test");
            m_mgrParams.SetParam("debuglevel", intDebugLevel.ToString());
            m_mgrParams.SetParam("zipprogram", "C:\\PKWARE\\PKZIPC\\pkzipc.exe");

            objJobParams.SetParam("StepParameters", "StepTool", "TestStepTool");
            objJobParams.SetParam("JobParameters", "ToolName", "TestTool");

            objJobParams.SetParam("StepParameters", "Job", "12345");
            objJobParams.SetParam("StepParameters", "OutputFolderName", "Tst_Results");

            objResources.Setup(m_mgrParams, objJobParams, objStatusTools, myEMSLUtilities);

            return objResources;
        }

        /// <summary>
        /// Initializes m_mgrParams and returns example job params
        /// </summary>
        /// <returns></returns>
        /// <remarks></remarks>
        private clsAnalysisJob InitializeManagerParams()
        {
            var intDebugLevel = 1;

            clsAnalysisJob objJobParams = new clsAnalysisJob(m_mgrParams, 0);

            m_mgrParams.SetParam("workdir", "E:\\DMS_WorkDir");
            m_mgrParams.SetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME, "Monroe_Test");
            m_mgrParams.SetParam("debuglevel", intDebugLevel.ToString());

            objJobParams.SetParam("StepParameters", "StepTool", "TestStepTool");
            objJobParams.SetParam("JobParameters", "ToolName", "TestTool");

            objJobParams.SetParam("StepParameters", "Job", "12345");
            objJobParams.SetParam("StepParameters", "OutputFolderName", "Tst_Results");

            return objJobParams;
        }

        public void ParseMSGFDBConsoleOutput()
        {
            Console.WriteLine("Test disabled since class not loaded");

            //Dim fiConsoleOutput = New FileInfo("f:\temp\MSGFDB_ConsoleOutput.txt")
            //Dim oJobParams As IJobParams = InitializeManagerParams()

            //Dim utils = New AnalysisManagerMSGFDBPlugIn.clsMSGFDBUtils(m_mgrParams, oJobParams, "1234", fiConsoleOutput.DirectoryName, 2, True)

            //Dim progressOverall = utils.ParseMSGFDBConsoleOutputFile(fiConsoleOutput.DirectoryName)

            //Console.WriteLine("Threads: " & utils.ThreadCountActual)
            //Console.WriteLine("Tasks completed: " & utils.TaskCountCompleted & " / " & utils.TaskCountTotal)
            //Console.WriteLine("Progress: " & progressOverall)
            //Console.WriteLine()
        }

        public void ParseMSPathFinderConsoleOutput()
        {
            Console.WriteLine("Test disabled since class not loaded");

            //    Dim filePath = "f:\temp\MSPathFinder_ConsoleOutput.txt"
            //    Dim msPathFinderTool = New AnalysisManagerMSPathFinderPlugIn.clsAnalysisToolRunnerMSPathFinder()

            //    msPathFinderTool.ParseConsoleOutputFile(filePath)
        }

        //Public Function Test(DestFolder As String) As Boolean
        //       Dim HashString As String = String.Empty

        //	TestException()
        //	Return False

        //	'Instantiate fasta tool if not already done
        //	If m_FastaTools Is Nothing Then
        //		If m_FastaToolsCnStr = "" Then
        //			Console.WriteLine("Protein database connection string not specified")
        //			Return False
        //		End If
        //		m_FastaTools = New Protein_Exporter.clsGetFASTAFromDMS(m_FastaToolsCnStr)
        //	End If

        //	'Initialize fasta generation state variables
        //	m_GenerationStarted = False
        //	m_GenerationComplete = False

        //	'Set up variables for fasta creation call
        //       Dim LegacyFasta As String = "na"
        //	Dim CreationOpts As String = "seq_direction=forward,filetype=fasta"
        //	Dim CollectionList As String = "Geobacter_bemidjiensis_Bem_T_2006-10-10,Geobacter_lovelyi_SZ_2007-06-19,Geobacter_metallireducens_GS-15_2007-10-02,Geobacter_sp_FRC-32_2007-07-07,Geobacter_sulfurreducens_2006-07-07,Geobacter_uraniumreducens_Rf4_2007-06-19"

        //	' Test what the Protein_Exporter does if a protein collection name is truncated (and thus invalid)
        //	CollectionList = "Geobacter_bemidjiensis_Bem_T_2006-10-10,Geobacter_lovelyi_SZ_2007-06-19,Geobacter_metallireducens_GS-15_2007-10-02,Geobacter_sp_"

        //	'Setup a timer to prevent an infinite loop if there's a fasta generation problem
        //	m_FastaTimer = New System.Timers.Timer
        //	m_FastaTimer.Interval = FASTA_GEN_TIMEOUT_INTERVAL_SEC * 1000
        //	m_FastaTimer.AutoReset = False

        //	'Create the fasta file
        //	m_FastaGenTimeOut = False
        //	Try
        //		m_FastaTimer.Start()
        //           HashString = m_FastaTools.ExportFASTAFile(CollectionList, CreationOpts, LegacyFasta, DestFolder)
        //       Catch ex As Exception
        //           Console.WriteLine("clsAnalysisResources.CreateFastaFile(), Exception generating OrgDb file: ", ex.Message)
        //           Return False
        //	End Try

        //	'Wait for fasta creation to finish
        //	While Not m_GenerationComplete
        //		System.Threading.Thread.Sleep(2000)
        //	End While

        //	If m_FastaGenTimeOut Then
        //		'Fasta generator hung - report error and exit
        //		Console.WriteLine("Timeout error while generating OrdDb file (" & FASTA_GEN_TIMEOUT_INTERVAL_SEC.ToString() & " seconds have elapsed)")
        //		Return False
        //	End If

        //	'If we got to here, everything worked OK
        //	Return True

        //End Function

        public void PerformanceCounterTest()
        {
            try
            {
                // Note that the Memory and Processor performance monitor categories are not
                // available on Windows instances running under VMWare on PIC
                //Console.WriteLine("Performance monitor categories")
                //Dim perfCats As PerformanceCounterCategory() = PerformanceCounterCategory.GetCategories()
                //For Each category As PerformanceCounterCategory In perfCats.OrderBy(Function(c) c.CategoryName)
                //	Console.WriteLine("Category Name: {0}", category.CategoryName)
                //Next
                //Console.WriteLine()

                var mCPUUsagePerformanceCounter = new System.Diagnostics.PerformanceCounter("Processor", "% Processor Time", "_Total");
                mCPUUsagePerformanceCounter.ReadOnly = true;

                var mFreeMemoryPerformanceCounter = new System.Diagnostics.PerformanceCounter("Memory", "Available MBytes");
                mFreeMemoryPerformanceCounter.ReadOnly = true;
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine("Error in PerformanceCounterTest: " + ex.Message);
                Console.WriteLine(clsGlobal.GetExceptionStackTrace(ex, true));
                var rePub1000 = new System.Text.RegularExpressions.Regex("Pub-1\\d{3,}", RegexOptions.IgnoreCase);
                if (rePub1000.IsMatch(Environment.MachineName))
                {
                    Console.WriteLine("This is a known issue with Windows instances running under VMWare on PIC");
                }
            }
        }

        public void ProcessDtaRefineryLogFiles()
        {
            //ProcessDtaRefineryLogFiles(968057, 968057)
            //ProcessDtaRefineryLogFiles(968061, 968061)
            //ProcessDtaRefineryLogFiles(968094, 968094)
            //ProcessDtaRefineryLogFiles(968102, 968102)
            //ProcessDtaRefineryLogFiles(968106, 968106)

            //ProcessDtaRefineryLogFiles(968049, 968049)
            //ProcessDtaRefineryLogFiles(968053, 968053)
            //ProcessDtaRefineryLogFiles(968098, 968098)
            ProcessDtaRefineryLogFiles(968470, 968470);
            ProcessDtaRefineryLogFiles(968482, 968482);
        }

        public bool ProcessDtaRefineryLogFiles(int intJobStart, int intJobEnd)
        {
            // Query the Pipeline DB to find jobs that ran DTA Refinery

            // Dim strSql As String = "SELECT JSH.Dataset, J.Dataset_ID, JSH.Job, JSH.Output_Folder, DFP.Dataset_Folder_Path" &
            //   " FROM DMS_Pipeline.dbo.V_Job_Steps_History JSH INNER JOIN" &
            //   "      DMS_Pipeline.dbo.T_Jobs J ON JSH.Job = J.Job INNER JOIN" &
            //   "      DMS5.dbo.V_Dataset_Folder_Paths DFP ON J.Dataset_ID = DFP.Dataset_ID" &
            //   " WHERE (JSH.Job Between " & intJobStart & " and " & intJobEnd & ") AND (JSH.Tool = 'DTA_Refinery') AND (JSH.Most_Recent_Entry = 1) AND (JSH.State = 5)"

            string strSql =
                "SELECT JS.Dataset, J.Dataset_ID, JS.Job, JS.Output_Folder, DFP.Dataset_Folder_Path, JS.Transfer_Folder_Path" +
                " FROM DMS_Pipeline.dbo.V_Job_Steps JS INNER JOIN" +
                "      DMS_Pipeline.dbo.T_Jobs J ON JS.Job = J.Job INNER JOIN" +
                "      DMS5.dbo.V_Dataset_Folder_Paths DFP ON J.Dataset_ID = DFP.Dataset_ID" +
                " WHERE (JS.Job Between " + intJobStart + " and " + intJobEnd + ") AND (JS.Tool = 'DTA_Refinery') AND (JS.State = 5)";

            const string strConnectionString = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;";
            const short RetryCount = 2;

            DataTable Dt = null;

            var blnSuccess = clsGlobal.GetDataTableByQuery(strSql, strConnectionString, "ProcessDtaRefineryLogFiles", RetryCount, out Dt);

            if (!blnSuccess)
            {
                Console.WriteLine("Repeated errors running database query");
            }

            if (Dt.Rows.Count < 1)
            {
                // No data was returned
                Console.WriteLine("DTA_Refinery jobs were not found for job range " + intJobStart + " - " + intJobEnd);
                return false;
            }

            string strWorkDir = m_mgrParams.GetParam("workdir");
            var blnPostResultsToDB = true;

            // Note: add file clsDtaRefLogMassErrorExtractor to this project to use this functionality
            //Dim oMassErrorExtractor = New clsDtaRefLogMassErrorExtractor(m_mgrParams, strWorkDir, m_DebugLevel, blnPostResultsToDB)

            foreach (DataRow CurRow in Dt.Rows)
            {
                var udtPSMJob = new udtPSMJobInfoType
                {
                    Dataset = clsGlobal.DbCStr(CurRow["Dataset"]),
                    DatasetID = clsGlobal.DbCInt(CurRow["Dataset_ID"]),
                    Job = clsGlobal.DbCInt(CurRow["Job"]),
                    DtaRefineryDataFolderPath = Path.Combine(clsGlobal.DbCStr(CurRow["Dataset_Folder_Path"]), clsGlobal.DbCStr(CurRow["Output_Folder"]))
                };

                if (!Directory.Exists(udtPSMJob.DtaRefineryDataFolderPath))
                {
                    udtPSMJob.DtaRefineryDataFolderPath = Path.Combine(clsGlobal.DbCStr(CurRow["Transfer_Folder_Path"]), clsGlobal.DbCStr(CurRow["Output_Folder"]));
                }

                if (Directory.Exists(udtPSMJob.DtaRefineryDataFolderPath))
                {
                    Console.WriteLine("Processing " + udtPSMJob.DtaRefineryDataFolderPath);
                    //oMassErrorExtractor.ParseDTARefineryLogFile(udtPSMJob.Dataset, udtPSMJob.DatasetID, udtPSMJob.Job, udtPSMJob.DtaRefineryDataFolderPath)
                }
                else
                {
                    Console.WriteLine("Skipping " + udtPSMJob.DtaRefineryDataFolderPath);
                }
            }

            return true;
        }

        private clsRunDosProgram m_RunProgTool;

        public void RunMSConvert()
        {
            var workDir = "E:\\DMS_WorkDir";

            var exePath = "C:\\DMS_Programs\\ProteoWizard\\msconvert.exe";
            var dataFilePath = "E:\\DMS_WorkDir\\QC_ShewPartialInj_15_02-100ng_Run-1_20Jan16_Pippin_15-08-53.raw";
            var cmdStr = dataFilePath + " --filter \"peakPicking true 1-\" --filter \"threshold count 500 most-intense\" --mgf -o E:\\DMS_WorkDir";

            m_RunProgTool = new clsRunDosProgram(workDir)
            {
                CreateNoWindow = true,
                CacheStandardOutput = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = ""
                // Allow the console output filename to be auto-generated
            };

            if (!m_RunProgTool.RunProgram(exePath, cmdStr, "MSConvert", true))
            {
                Console.WriteLine("Error running MSConvert");
            }
            else
            {
                Console.WriteLine("Done");
            }
        }

        public void TestArchiveFileStart()
        {
            string strParamFilePath = null;
            string strTargetFolderPath = null;

            strParamFilePath = "D:\\Temp\\sequest_N14_NE.params";
            strTargetFolderPath = "\\\\gigasax\\dms_parameter_Files\\Sequest";

            TestArchiveFile(strParamFilePath, strTargetFolderPath);

            //TestArchiveFile("\\n2.emsl.pnl.gov\dmsarch\LCQ_1\LCQ_C1\DR026_dialyzed_7june00_a\Seq200104201154_Auto1001\sequest_Tryp_4_IC.params", strTargetFolderPath)
            //TestArchiveFile("\\proto-4\C1_DMS1\DR026_dialyzed_7june00_a\Seq200104201154_Auto1001\sequest_Tryp_4_IC.params", strTargetFolderPath)

            Console.WriteLine("Done syncing files");
        }

        public void TestArchiveFile(string strSrcFilePath, string strTargetFolderPath)
        {
            try
            {
                var lstLineIgnoreRegExSpecs = new List<Regex>();
                lstLineIgnoreRegExSpecs.Add(new Regex("mass_type_parent *=.*", RegexOptions.Compiled | RegexOptions.IgnoreCase));

                var blnNeedToArchiveFile = false;

                var strTargetFilePath = Path.Combine(strTargetFolderPath, Path.GetFileName(strSrcFilePath));

                if (!File.Exists(strTargetFilePath))
                {
                    blnNeedToArchiveFile = true;
                }
                else
                {
                    // Read the files line-by-line and compare
                    // Since the first 2 lines of a Sequest parameter file don't matter, and since the 3rd line can vary from computer to computer, we start the comparison at the 4th line

                    if (!clsGlobal.TextFilesMatch(strSrcFilePath, strTargetFilePath, 4, 0, true, lstLineIgnoreRegExSpecs))
                    {
                        // Files don't match; rename the old file

                        var fiArchivedFile = new FileInfo(strTargetFilePath);

                        var strNewNameBase = Path.GetFileNameWithoutExtension(strTargetFilePath) + "_" + fiArchivedFile.LastWriteTime.ToString("yyyy-MM-dd");
                        var strNewName = strNewNameBase + Path.GetExtension(strTargetFilePath);

                        // See if the renamed file exists; if it does, we'll have to tweak the name
                        var intRevisionNumber = 1;
                        string strNewPath;
                        do
                        {
                            strNewPath = Path.Combine(strTargetFolderPath, strNewName);
                            if (!File.Exists(strNewPath))
                            {
                                break;
                            }

                            intRevisionNumber += 1;
                            strNewName = strNewNameBase + "_v" + intRevisionNumber.ToString() + Path.GetExtension(strTargetFilePath);
                        } while (true);

                        fiArchivedFile.MoveTo(strNewPath);

                        blnNeedToArchiveFile = true;
                    }
                }

                if (blnNeedToArchiveFile)
                {
                    // Copy the new parameter file to the archive
                    Console.WriteLine("Copying " + Path.GetFileName(strSrcFilePath) + " to " + strTargetFilePath);
                    File.Copy(strSrcFilePath, strTargetFilePath, true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error caught: " + ex.Message);
            }
        }

        private void TestException()
        {
            InnerTestException();
        }

        private void InnerTestException()
        {
            throw new PathTooLongException();
        }

        public void TestUncat(string rootFileName, string strResultsFolder)
        {
            Console.WriteLine("Splitting concatenated DTA file");

            clsSplitCattedFiles FileSplitter = new clsSplitCattedFiles();
            FileSplitter.SplitCattedDTAsOnly(rootFileName, strResultsFolder);

            Console.WriteLine("Completed splitting concatenated DTA file");
        }

        public void TestDTASplit()
        {
            //'Const intDebugLevel = 2

            //'Dim objJobParams = InitializeMgrAndJobParams(intDebugLevel)
            //'Dim objStatusTools As New clsStatusFile("Status.xml", intDebugLevel)

            //'Dim myEMSLUtilities As New clsMyEMSLUtilities(intDebugLevel, WORKING_DIRECTORY)

            //'objJobParams.SetParam("JobParameters", "DatasetNum", "QC_05_2_05Dec05_Doc_0508-08")
            //'objJobParams.SetParam("JobParameters", "NumberOfClonedSteps", "25")
            //'objJobParams.SetParam("JobParameters", "ClonedStepsHaveEqualNumSpectra", "True")

            //'Dim objToolRunner = New clsAnalysisToolRunnerDtaSplit
            //'objToolRunner.Setup(m_mgrParams, objJobParams, objStatusTools, myEMSLUtilities)

            //'objToolRunner.RunTool()
        }

        public bool TestProteinDBExport(string DestFolder)
        {
            string strLegacyFasta = null;
            string strProteinCollectionList = null;
            string strProteinOptions = null;

            // Test what the Protein_Exporter does if a protein collection name is truncated (and thus invalid)
            strLegacyFasta = "na";
            strProteinCollectionList = "Geobacter_bemidjiensis_Bem_T_2006-10-10,Geobacter_lovelyi_SZ_2007-06-19,Geobacter_metallireducens_GS-15_2007-10-02,Geobacter_sp_";
            strProteinOptions = "seq_direction=forward,filetype=fasta";

            // Test a legacy fasta file:
            strLegacyFasta = "GOs_Surface_Sargasso_Meso_2009-02-11_24.fasta";
            strProteinCollectionList = "";
            strProteinOptions = "";

            // Test a 34 MB fasta file
            strLegacyFasta = "na";
            strProteinCollectionList = "nr_ribosomal_2010-08-17,Tryp_Pig";
            strProteinOptions = "seq_direction=forward,filetype=fasta";

            // Test 100 MB fasta file
            //strLegacyFasta = "na"
            //strProteinCollectionList = "GWB1_Rifle_2011_9_13_0_1_2013-03-27,Tryp_Pig_Bov"
            //strProteinOptions = "seq_direction=forward,filetype=fasta"

            bool blnSuccess = false;
            blnSuccess = TestProteinDBExport(DestFolder, "na", strProteinCollectionList, strProteinOptions);

            if (blnSuccess)
            {
                IJobParams oJobParams = InitializeManagerParams();

                var blnMsgfPlus = true;
                var strJobNum = "12345";
                var intDebugLevel = Convert.ToInt16(m_mgrParams.GetParam("debuglevel", 1));

                var JavaProgLoc = "C:\\Program Files\\Java\\jre8\\bin\\java.exe";
                var MSGFDbProgLoc = "C:\\DMS_Programs\\MSGFDB\\MSGFPlus.jar";
                var FastaFileIsDecoy = false;
                string FastaFilePath = string.Empty;

                oJobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", m_FastaFileName);

                //' Note: This won't compile if the AM_Shared project is loaded in the solution
                //Dim oTool As AnalysisManagerMSGFDBPlugIn.clsMSGFDBUtils
                //oTool = New AnalysisManagerMSGFDBPlugIn.clsMSGFDBUtils(m_mgrParams, oJobParams, strJobNum, m_mgrParams.GetParam("workdir"), intDebugLevel, blnMsgfPlus)

                //Dim FastaFileSizeKB As Single
                //Dim eResult As IJobParams.CloseOutType

                //' Note that FastaFilePath will be populated by this function call
                //eResult = oTool.InitializeFastaFile(JavaProgLoc, MSGFDbProgLoc, FastaFileSizeKB, FastaFileIsDecoy, FastaFilePath)
            }

            return blnSuccess;
        }

        public bool TestProteinDBExport(string DestFolder, string strLegacyFasta, string strProteinCollectionList, string strProteinOptions)
        {
            //Instantiate fasta tool if not already done
            if (m_FastaTools == null)
            {
                if (string.IsNullOrEmpty(m_FastaToolsCnStr))
                {
                    Console.WriteLine("Protein database connection string not specified");
                    return false;
                }
                m_FastaTools = new Protein_Exporter.clsGetFASTAFromDMS(m_FastaToolsCnStr);
            }

            //Initialize fasta generation state variables
            m_GenerationStarted = false;
            m_GenerationComplete = false;

            // Setup a timer to prevent an infinite loop if there's a fasta generation problem
            m_FastaTimer = new System.Timers.Timer();
            m_FastaTimer.Interval = FASTA_GEN_TIMEOUT_INTERVAL_SEC * 1000;
            m_FastaTimer.AutoReset = false;

            // Create the fasta file
            m_FastaGenTimeOut = false;
            try
            {
                m_FastaTimer.Start();
                var HashString = m_FastaTools.ExportFASTAFile(strProteinCollectionList, strProteinOptions, strLegacyFasta, DestFolder);
            }
            catch (Exception ex)
            {
                Console.WriteLine("clsAnalysisResources.CreateFastaFile(), Exception generating OrgDb file: " + ex.Message);
                return false;
            }

            // Wait for fasta creation to finish
            while (!m_GenerationComplete)
            {
                System.Threading.Thread.Sleep(2000);
            }

            if (m_FastaGenTimeOut)
            {
                //Fasta generator hung - report error and exit
                Console.WriteLine("Timeout error while generating OrdDb file (" + FASTA_GEN_TIMEOUT_INTERVAL_SEC.ToString() + " seconds have elapsed)");
                return false;
            }

            //If we got to here, everything worked OK
            return true;
        }

        public void TestDeleteFiles()
        {
            var OutFileName = "MyTestDataset_out.txt";

            clsAnalysisJob objJobParams = null;
            clsMyEMSLUtilities myEMSLUtilities = null;

            clsCodeTestAM objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            objJobParams.AddResultFileToSkip(OutFileName);

            objToolRunner.RunTool();
        }

        public void TestDeliverResults()
        {
            var OutFileName = "MyTestDataset_out.txt";

            clsAnalysisJob objJobParams = null;
            clsMyEMSLUtilities myEMSLUtilities = null;

            clsCodeTestAM objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            objJobParams.SetParam("StepParameters", "OutputFolderName", "Tst_Results_" + System.DateTime.Now.ToString("hh_mm_ss"));
            objJobParams.SetParam("JobParameters", "transferFolderPath", "\\\\proto-3\\DMS3_XFER");
            objJobParams.SetParam("JobParameters", "DatasetNum", "Test_Dataset");

            objToolRunner.RunTool();
        }

        public void TestFileDateConversion()
        {
            FileInfo objTargetFile = default(FileInfo);
            string strDate = null;

            objTargetFile = new FileInfo("D:\\JobSteps.png");

            strDate = objTargetFile.LastWriteTime.ToString();

            string[] ResultFiles = null;

            ResultFiles = Directory.GetFiles("C:\\Temp\\", "*");

            foreach (string FileToCopy in ResultFiles)
            {
                Console.WriteLine(FileToCopy);
            }

            Console.WriteLine(strDate);
        }

        public void TestLogging()
        {
            var logFileNameBase = "Logs\\AnalysisMgr";

            clsLogTools.CreateFileLogger(logFileNameBase);

            clsAnalysisJob objJobParams = null;
            clsMyEMSLUtilities myEMSLUtilities = null;

            clsCodeTestAM objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            m_DebugLevel = 2;
            objJobParams.DebugLevel = m_DebugLevel;

            for (var debugLevel = 0; debugLevel <= 5; debugLevel++)
            {
                LogMessage("Test status, debugLevel " + debugLevel, debugLevel, false);
            }

            for (var debugLevel = 0; debugLevel <= 5; debugLevel++)
            {
                LogMessage("Test error, debugLevel " + debugLevel, debugLevel, true);
            }

            LogError("Test error, no detailed message");

            // To test this function, temporarily make it public
            // objToolRunner.LogError("Test error", "Detailed message of the error")

            try
            {
                throw new FileNotFoundException("TestFile.txt");
            }
            catch (Exception ex)
            {
                LogError("Test exception", ex);
            }

            LogMessage("Testing complete");
        }

        public void GetLegacyFastaFileSize()
        {
            IJobParams objJobParams = default(IJobParams);
            objJobParams = new clsAnalysisJob(m_mgrParams, 0);

            objJobParams.SetParam("JobParameters", "ToolName", "MSGFPlus_SplitFasta");

            objJobParams.SetParam("StepParameters", "Step", "50");

            objJobParams.SetParam("ParallelMSGFPlus", "NumberOfClonedSteps", "25");
            objJobParams.SetParam("ParallelMSGFPlus", "CloneStepRenumberStart", "50");
            objJobParams.SetParam("ParallelMSGFPlus", "SplitFasta", "True");

            objJobParams.SetParam("PeptideSearch", "legacyFastaFileName", "Uniprot_ArchaeaBacteriaFungi_SprotTrembl_2014-4-16.fasta");
            objJobParams.SetParam("PeptideSearch", "OrganismName", "Combined_Organism_Rifle_SS");
            objJobParams.SetParam("PeptideSearch", "ProteinCollectionList", "na");
            objJobParams.SetParam("PeptideSearch", "ProteinOptions", "na");

            var intDebugLevel = 2;
            var objResources = GetResourcesObject(intDebugLevel, objJobParams);

            var proteinCollectionInfo = new clsProteinCollectionInfo(objJobParams);

            var spaceRequiredMB = objResources.LookupLegacyDBDiskSpaceRequiredMB(proteinCollectionInfo);

            string legacyFastaName = null;

            if (proteinCollectionInfo.UsingSplitFasta)
            {
                string errorMessage = string.Empty;
                legacyFastaName = clsAnalysisResources.GetSplitFastaFileName(objJobParams, out errorMessage);
            }
            else
            {
                legacyFastaName = proteinCollectionInfo.LegacyFastaName;
            }

            Console.WriteLine(legacyFastaName + " requires roughly " + spaceRequiredMB.ToString("#,##0") + " MB");
        }

        public void TestRunQuery()
        {
            const string sqlStr = "Select top 50 * from t_log_entries";

            const string connectionString = "Data Source=gigasax;Initial Catalog=dms_pipeline;Integrated Security=SSPI;";
            const string callingFunction = "TestRunQuery";
            const short retryCount = 2;
            const int timeoutSeconds = 30;
            DataTable dtResults = null;

            clsGlobal.GetDataTableByQuery(sqlStr, connectionString, callingFunction, retryCount, out dtResults, timeoutSeconds);

            foreach (DataRow row in dtResults.Rows)
            {
                Console.WriteLine(row[0].ToString() + ": " + row[1].ToString());
            }
        }

        public void TestRunSP()
        {
            var cmd = new SqlCommand
            {
                CommandType = CommandType.StoredProcedure,
                CommandText = "GetJobStepParamsAsTable"
            };
            cmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
            cmd.Parameters.Add(new SqlParameter("@jobNumber", SqlDbType.Int)).Value = 1026591;
            cmd.Parameters.Add(new SqlParameter("@stepNumber", SqlDbType.Int)).Value = 3;
            cmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;

            const string connectionString = "Data Source=gigasax;Initial Catalog=dms_pipeline;Integrated Security=SSPI;";
            const string callingFunction = "TestRunSP";
            const short retryCount = 2;
            const int timeoutSeconds = 30;
            DataTable dtResults = null;

            clsGlobal.GetDataTableByCmd(cmd, connectionString, callingFunction, retryCount, out dtResults, timeoutSeconds);

            foreach (DataRow row in dtResults.Rows)
            {
                Console.WriteLine(row[0].ToString() + ": " + row[1].ToString());
            }
        }

        public void ConvertZipToGZip(string zipFilePath)
        {
            const int debugLevel = 2;
            const string workDir = "e:\\dms_workdir";

            var ionicZipTools = new clsIonicZipTools(debugLevel, workDir);

            ionicZipTools.UnzipFile(zipFilePath);

            var diWorkDir = new DirectoryInfo(workDir);
            foreach (var fiFile in diWorkDir.GetFiles("*.mzid"))
            {
                ionicZipTools.GZipFile(fiFile.FullName, true);
            }
        }

        public void TestGZip()
        {
            clsAnalysisJob objJobParams = null;
            clsMyEMSLUtilities myEMSLUtilities = null;

            clsCodeTestAM objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            const string sourceFilePath = "F:\\Temp\\ZipTest\\QExact01\\UDD-1_27Feb13_Gimli_12-07-03_HCD.mgf";

            objToolRunner.GZipFile(sourceFilePath, "F:\\Temp\\ZipTest\\QExact01\\GZipTarget", false);

            objToolRunner.GZipFile(sourceFilePath, false);

            string gzippedFile = "F:\\Temp\\ZipTest\\QExact01\\" + Path.GetFileName(sourceFilePath) + ".gz";

            objToolRunner.GUnzipFile(gzippedFile);

            objToolRunner.GUnzipFile(gzippedFile, "F:\\Temp\\ZipTest\\GUnzipTarget");
        }

        public bool TestUnzip(string strZipFilePath, string strOutFolderPath)
        {
            var intDebugLevel = 2;

            var objResources = GetResourcesObject(intDebugLevel);

            var blnSuccess = objResources.UnzipFileStart(strZipFilePath, strOutFolderPath, "TestUnzip", false);
            //blnSuccess = objResources.UnzipFileStart(strZipFilePath, strOutFolderPath, "TestUnzip", True)

            return blnSuccess;
        }

        public void TestZip()
        {
            clsAnalysisJob objJobParams = null;
            clsMyEMSLUtilities myEMSLUtilities = null;

            clsCodeTestAM objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            const string sourceFilePath = "F:\\Temp\\ZipTest\\QExact01\\UDD-1_27Feb13_Gimli_12-07-03_HCD.mgf";

            objToolRunner.ZipFile(sourceFilePath, false);

            string zippedFile = "F:\\Temp\\ZipTest\\QExact01\\" + Path.GetFileNameWithoutExtension(sourceFilePath) + ".zip";

            objToolRunner.UnzipFile(zippedFile);

            objToolRunner.UnzipFile(zippedFile, "F:\\Temp\\ZipTest\\UnzipTarget");

            var oZipTools = new clsIonicZipTools(1, WORKING_DIRECTORY);
            oZipTools.ZipDirectory("F:\\Temp\\ZipTest\\QExact01\\", "F:\\Temp\\ZipTest\\QExact01_Folder.zip");
        }

        public void TestIonicZipTools()
        {
            clsIonicZipTools oIonicZipTools = default(clsIonicZipTools);

            oIonicZipTools = new clsIonicZipTools(1, "E:\\DMS_WorkDir");

            oIonicZipTools.UnzipFile("E:\\DMS_WorkDir\\Temp.zip", "E:\\DMS_WorkDir", "*.png");
            foreach (var item in oIonicZipTools.MostRecentUnzippedFiles)
            {
                Console.WriteLine(item.Key + " - " + item.Value);
            }
        }

        public bool TestMALDIDataUnzip(string strSourceDatasetFolder)
        {
            var intDebugLevel = 2;

            clsResourceTestClass objResources = new clsResourceTestClass();

            clsStatusFile objStatusTools = new clsStatusFile("Status.xml", intDebugLevel);
            bool blnSuccess = false;

            if (string.IsNullOrEmpty(strSourceDatasetFolder))
            {
                strSourceDatasetFolder = "\\\\Proto-10\\9T_FTICR_Imaging\\2010_4\\ratjoint071110_INCAS_MS";
            }

            clsAnalysisJob objJobParams = null;
            clsMyEMSLUtilities myEMSLUtilities = null;

            clsCodeTestAM objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            m_mgrParams.SetParam("ChameleonCachedDataFolder", "H:\\9T_Imaging");

            objJobParams.SetParam("JobParameters", "DatasetNum", "ratjoint071110_INCAS_MS");

            objJobParams.SetParam("JobParameters", "DatasetStoragePath", "\\\\Proto-10\\9T_FTICR_Imaging\\2010_4\\");
            objJobParams.SetParam("JobParameters", "DatasetArchivePath", "\\\\adms.emsl.pnl.gov\\dmsarch\\9T_FTICR_Imaging_1");
            objJobParams.SetParam("JobParameters", "transferFolderPath", "\\\\proto-10\\DMS3_Xfer");

            objResources.Setup(m_mgrParams, objJobParams, objStatusTools, myEMSLUtilities);

            blnSuccess = objResources.RetrieveBrukerMALDIImagingFolders(true);

            //blnSuccess = objResources.UnzipFileStart(strZipFilePath, strOutFolderPath, "TestUnzip", True)

            return blnSuccess;
        }

        public void TestZipAndUnzip()
        {
            clsIonicZipTools objZipper = new clsIonicZipTools(3, "F:\\Temp");

            objZipper.ZipFile("F:\\Temp\\Sarc_P12_D12_1104_148_8Sep11_Cheetah_11-05-34.uimf", false);

            objZipper.ZipFile("F:\\Temp\\Schutzer_cf_ff_XTandem_AllProt.txt", false, "F:\\Temp\\TestCustom.zip");

            objZipper.ZipDirectory("F:\\Temp\\STAC", "F:\\Temp\\ZippedFolderTest.zip");

            //objZipper.ZipDirectory("F:\Temp\UnzipTest\0_R00X051Y065", "F:\Temp\UnzipTest\0_R00X051Y065.zip", False)

            //      objZipper.ZipDirectory("F:\Temp\UnzipTest\0_R00X051Y065", "F:\Temp\UnzipTest\ZippedFolders2.zip", True, "*.baf*")

            //      objZipper.ZipDirectory("F:\Temp\UnzipTest\0_R00X051Y065", "F:\Temp\UnzipTest\ZippedFolders3.zip", True, "*.ini")

            //      objZipper.UnzipFile("f:\temp\unziptest\StageMD5_Scratch.zip")

            //      objZipper.UnzipFile("F:\Temp\UnzipTest\ZippedFolders.zip", "F:\Temp\UnzipTest\Unzipped")

            //      objZipper.UnzipFile("F:\Temp\UnzipTest\ZippedFolders.zip", "F:\Temp\UnzipTest\Unzipped2", "*.baf*")

            //      objZipper.UnzipFile("F:\Temp\UnzipTest\ZippedFolders.zip", "F:\Temp\UnzipTest\Unzipped3", "*.baf*", Ionic.Zip.ExtractExistingFileAction.DoNotOverwrite)

            //      objZipper.UnzipFile("F:\Temp\UnzipTest\ZippedFolders3.zip", "F:\Temp\UnzipTest\Unzipped4", "*.ini", Ionic.Zip.ExtractExistingFileAction.OverwriteSilently)

            //      objZipper.UnzipFile("F:\Temp\UnzipTest\ZippedFolders3.zip", "F:\Temp\UnzipTest\Unzipped5", "my*.ini", Ionic.Zip.ExtractExistingFileAction.OverwriteSilently)
        }

        public bool TestFileSplitThenCombine()
        {
            const int SYN_FILE_MAX_SIZE_MB = 200;
            const string PEPPROPHET_RESULT_FILE_SUFFIX = "_PepProphet.txt";

            string SynFile = null;

            string Msg = null;
            string[] strFileList = null;

            float sngParentSynFileSizeMB = 0;
            bool blnSuccess = false;

            int intFileIndex = 0;
            string strPepProphetOutputFilePath = null;
            bool blnIgnorePeptideProphetErrors = false;

            SynFile = "JGI_Fungus_02_13_8Apr09_Griffin_09-02-12_syn.txt";

            //Check to see if Syn file exists
            var fiSynFile = new FileInfo(SynFile);
            if (!fiSynFile.Exists)
            {
                Msg = "clsExtractToolRunner.RunPeptideProphet(); Syn file " + SynFile + " not found; unable to run peptide prophet";
                Console.WriteLine(Msg);
                return false;
            }

            // Check the size of the Syn file
            // If it is too large, then we will need to break it up into multiple parts, process each part separately, and then combine the results
            sngParentSynFileSizeMB = Convert.ToSingle(fiSynFile.Length / 1024.0 / 1024.0);
            if (sngParentSynFileSizeMB <= SYN_FILE_MAX_SIZE_MB)
            {
                strFileList = new string[1];
                strFileList[0] = fiSynFile.FullName;
            }
            else
            {
                // File is too large; split it into multiple chunks
                strFileList = new string[1];
                blnSuccess = SplitFileRoundRobin(fiSynFile.FullName, SYN_FILE_MAX_SIZE_MB * 1024 * 1024, true, ref strFileList);
            }

            //Setup Peptide Prophet and run for each file in strFileList
            for (intFileIndex = 0; intFileIndex <= strFileList.Length - 1; intFileIndex++)
            {
                // Run PeptideProphet

                fiSynFile = new FileInfo(strFileList[intFileIndex]);
                var strSynFileNameAndSize = fiSynFile.Name + " (file size = " + (fiSynFile.Length / 1024.0 / 1024.0).ToString("0.00") + " MB";
                if (strFileList.Length > 1)
                {
                    strSynFileNameAndSize += "; parent syn file is " + sngParentSynFileSizeMB.ToString("0.00") + " MB)";
                }
                else
                {
                    strSynFileNameAndSize += ")";
                }

                if (true)
                {
                    // Make sure the Peptide Prophet output file was actually created
                    strPepProphetOutputFilePath = Path.Combine(Path.GetDirectoryName(strFileList[intFileIndex]),
                        Path.GetFileNameWithoutExtension(strFileList[intFileIndex]) +
                        PEPPROPHET_RESULT_FILE_SUFFIX);

                    if (!File.Exists(strPepProphetOutputFilePath))
                    {
                        Msg = "clsExtractToolRunner.RunPeptideProphet(); Peptide Prophet output file not found for synopsis file " + strSynFileNameAndSize;
                        //'m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)

                        if (blnIgnorePeptideProphetErrors)
                        {
                            //'m_logger.PostEntry("Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' = True", ILogger.logMsgType.logWarning, True)
                        }
                        else
                        {
                            //'eResult = IJobParams.CloseOutType.CLOSEOUT_FAILED
                            break;
                        }
                    }
                }
                else
                {
                    Msg = "clsExtractToolRunner.RunPeptideProphet(); Error running Peptide Prophet on file " + strSynFileNameAndSize + ": ";
                    //'m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)

                    if (blnIgnorePeptideProphetErrors)
                    {
                        //'m_logger.PostEntry("Ignoring peptide prophet execution error since 'IgnorePeptideProphetErrors' = True", ILogger.logMsgType.logWarning, True)
                    }
                    else
                    {
                        //'eResult = IJobParams.CloseOutType.CLOSEOUT_FAILED
                        break;
                    }
                }
            }

            if (strFileList.Length > 1)
            {
                // We now need to recombine the peptide prophet result files

                // Update strFileList() to have the peptide prophet result file names
                var strBaseName = Path.Combine(Path.GetDirectoryName(fiSynFile.FullName), Path.GetFileNameWithoutExtension(SynFile));

                for (intFileIndex = 0; intFileIndex <= strFileList.Length - 1; intFileIndex++)
                {
                    strFileList[intFileIndex] = strBaseName + "_part" + (intFileIndex + 1).ToString() + PEPPROPHET_RESULT_FILE_SUFFIX;
                }

                // Define the final peptide prophet output file name
                strPepProphetOutputFilePath = strBaseName + PEPPROPHET_RESULT_FILE_SUFFIX;

                blnSuccess = InterleaveFiles(strFileList, strPepProphetOutputFilePath, true);

                if (blnSuccess)
                {
                    return true;
                }
                else
                {
                    Msg = "Error interleaving the peptide prophet result files (FileCount=" + strFileList.Length + ")";
                    if (blnIgnorePeptideProphetErrors)
                    {
                        Msg += "; Ignoring the error since 'IgnorePeptideProphetErrors' = True";
                        //'m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, True)
                        return true;
                    }
                    else
                    {
                        //'m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
                        return false;
                    }
                }
            }

            return true;
        }

        private bool InterleaveFiles(string[] strFileList, string strCombinedFilePath, bool blnLookForHeaderLine)
        {
            string msg = null;

            StreamReader[] srInFiles = null;

            string strLineIn = string.Empty;

            bool blnSuccess = false;

            try
            {
                if (strFileList == null || strFileList.Length == 0)
                {
                    // Nothing to do
                    return false;
                }

                var intFileCount = strFileList.Length;
                // ERROR: Not supported in C#: ReDimStatement

                var intLinesRead = new int[intFileCount];

                // Open each of the input files
                for (var intIndex = 0; intIndex <= intFileCount - 1; intIndex++)
                {
                    if (File.Exists(strFileList[intIndex]))
                    {
                        srInFiles[intIndex] = new StreamReader(new FileStream(strFileList[intIndex], FileMode.Open, FileAccess.Read, FileShare.Read));
                    }
                    else
                    {
                        // File not found; unable to continue
                        msg = "Source peptide prophet file not found, unable to continue: " + strFileList[intIndex];
                        //'m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
                        return false;
                    }
                }

                // Create the output file

                var swOutFile = new StreamWriter(new FileStream(strCombinedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                var intTotalLinesRead = 0;
                var blnContinueReading = true;

                while (blnContinueReading)
                {
                    var intTotalLinesReadSaved = intTotalLinesRead;

                    for (var intFileIndex = 0; intFileIndex <= intFileCount - 1; intFileIndex++)
                    {
                        if (!srInFiles[intFileIndex].EndOfStream)
                        {
                            strLineIn = srInFiles[intFileIndex].ReadLine();

                            intLinesRead[intFileIndex] += 1;
                            intTotalLinesRead += 1;

                            if ((strLineIn != null))
                            {
                                var blnProcessLine = true;

                                if (intLinesRead[intFileIndex] == 1 && blnLookForHeaderLine && strLineIn.Length > 0)
                                {
                                    // Check for a header line
                                    var strSplitLine = strLineIn.Split(new char[] { '\t' }, 2);

                                    double temp;
                                    if (strSplitLine.Length > 0 && !double.TryParse(strSplitLine[0], out temp))
                                    {
                                        // First column does not contain a number; this must be a header line
                                        // Write the header to the output file (provided intFileIndex=0)
                                        if (intFileIndex == 0)
                                        {
                                            swOutFile.WriteLine(strLineIn);
                                        }
                                        blnProcessLine = false;
                                    }
                                }

                                if (blnProcessLine)
                                {
                                    swOutFile.WriteLine(strLineIn);
                                }
                            }
                        }
                    }

                    if (intTotalLinesRead == intTotalLinesReadSaved)
                    {
                        blnContinueReading = false;
                    }
                }

                // Close the input files
                for (var intIndex = 0; intIndex <= intFileCount - 1; intIndex++)
                {
                    srInFiles[intIndex].Close();
                }

                // Close the output file
                swOutFile.Close();

                blnSuccess = true;
            }
            catch (System.Exception ex)
            {
                msg = "Exception in clsExtractToolRunner.InterleaveFiles: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                //'m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
                blnSuccess = false;
            }

            return blnSuccess;
        }

        /// <summary>
        /// Examines the X!Tndem param file to determine if ETD mode is enabled
        /// If it is, then sets m_ETDMode to True
        /// </summary>
        /// <param name="strParamFilePath">X!Tandem XML parameter file to read</param>
        /// <param name="blnEtdMode"></param>
        /// <returns>True if success; false if an error</returns>
        public bool CheckETDModeEnabledXTandem(string strParamFilePath, out bool blnEtdMode)
        {
            XmlNodeList objSelectedNodes = null;
            blnEtdMode = false;

            try
            {
                // Open the parameter file
                // Look for either of these lines:
                //   <note type="input" label="scoring, c ions">yes</note>
                //   <note type="input" label="scoring, z ions">yes</note>

                var objParamFile = new XmlDocument();
                objParamFile.PreserveWhitespace = true;
                objParamFile.Load(strParamFilePath);

                for (var intSettingIndex = 0; intSettingIndex <= 1; intSettingIndex++)
                {
                    switch (intSettingIndex)
                    {
                        case 0:
                            objSelectedNodes = objParamFile.DocumentElement.SelectNodes("/bioml/note[@label='scoring, c ions']");
                            break;
                        case 1:
                            objSelectedNodes = objParamFile.DocumentElement.SelectNodes("/bioml/note[@label='scoring, z ions']");
                            break;
                    }

                    if ((objSelectedNodes != null))
                    {
                        for (var intMatchIndex = 0; intMatchIndex <= objSelectedNodes.Count - 1; intMatchIndex++)
                        {
                            // Make sure this node has an attribute of type="input"
                            var objAttributeNode = objSelectedNodes.Item(intMatchIndex).Attributes.GetNamedItem("type");

                            if (objAttributeNode == null)
                            {
                                // Node does not have an attribute named "type"; ignore it
                            }
                            else
                            {
                                if (objAttributeNode.Value.ToLower() == "input")
                                {
                                    // Node does have attribute type="input"
                                    // Now examine the node's InnerText value
                                    if (objSelectedNodes.Item(intMatchIndex).InnerText.ToLower() == "yes")
                                    {
                                        blnEtdMode = true;
                                    }
                                }
                            }
                        }
                    }

                    if (blnEtdMode)
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Reads strSrcFilePath line-by-line and splits into multiple files such that none of the output
        /// files has length greater than lngMaxSizeBytes. It will also check for a header line on the
        /// first line; if a header line is found, then all of the split files will be assigned the same header line
        /// </summary>
        /// <param name="strSrcFilePath">FilePath to parse</param>
        /// <param name="lngMaxSizeBytes">Maximum size of each file</param>
        /// <param name="blnLookForHeaderLine">When true, then looks for a header line by checking if the first column contains a number</param>
        /// <param name="strSplitFileList">Output array listing the full paths to the split files that were created</param>
        /// <returns>True if success, False if failure</returns>
        /// <remarks></remarks>
        private bool SplitFileRoundRobin(string strSrcFilePath, Int64 lngMaxSizeBytes, bool blnLookForHeaderLine, ref string[] strSplitFileList)
        {
            var intLinesRead = 0;
            int intTargetFileIndex = 0;

            string strLineIn = string.Empty;
            string[] strSplitLine = null;

            StreamWriter[] swOutFiles = null;

            int intSplitCount = 0;
            int intIndex = 0;

            bool blnProcessLine = false;
            var blnSuccess = false;

            try
            {
                var fiFileInfo = new FileInfo(strSrcFilePath);
                if (!fiFileInfo.Exists)
                    return false;

                if (fiFileInfo.Length <= lngMaxSizeBytes)
                {
                    // File is already less than the limit
                    strSplitFileList = new string[1];
                    strSplitFileList[0] = fiFileInfo.FullName;

                    blnSuccess = true;
                }
                else
                {
                    // Determine the number of parts to split the file into
                    intSplitCount = Convert.ToInt32(Math.Ceiling(fiFileInfo.Length / Convert.ToDouble(lngMaxSizeBytes)));

                    if (intSplitCount < 2)
                    {
                        // This code should never be reached; we'll set intSplitCount to 2
                        intSplitCount = 2;
                    }

                    // Open the input file
                    var srInFile = new StreamReader(new FileStream(fiFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read));

                    // Create each of the output files
                    strSplitFileList = new string[intSplitCount];
                    // ERROR: Not supported in C#: ReDimStatement

                    var strBaseName = Path.Combine(fiFileInfo.DirectoryName, Path.GetFileNameWithoutExtension(fiFileInfo.Name));

                    for (intIndex = 0; intIndex <= intSplitCount - 1; intIndex++)
                    {
                        strSplitFileList[intIndex] = strBaseName + "_part" + (intIndex + 1).ToString() + Path.GetExtension(fiFileInfo.Name);
                        swOutFiles[intIndex] = new StreamWriter(new FileStream(strSplitFileList[intIndex], FileMode.Create, FileAccess.Write, FileShare.Read));
                    }

                    intLinesRead = 0;
                    intTargetFileIndex = 0;

                    while (!srInFile.EndOfStream)
                    {
                        strLineIn = srInFile.ReadLine();
                        intLinesRead += 1;

                        if ((strLineIn != null))
                        {
                            blnProcessLine = true;

                            if (intLinesRead == 1 && blnLookForHeaderLine && strLineIn.Length > 0)
                            {
                                // Check for a header line
                                strSplitLine = strLineIn.Split(new char[] { '\t' }, 2);

                                double temp;
                                if (strSplitLine.Length > 0 && !double.TryParse(strSplitLine[0], out temp))
                                {
                                    // First column does not contain a number; this must be a header line
                                    // Write the header to each output file
                                    for (intIndex = 0; intIndex <= intSplitCount - 1; intIndex++)
                                    {
                                        swOutFiles[intIndex].WriteLine(strLineIn);
                                    }
                                    blnProcessLine = false;
                                }
                            }

                            if (blnProcessLine)
                            {
                                swOutFiles[intTargetFileIndex].WriteLine(strLineIn);
                                intTargetFileIndex += 1;
                                if (intTargetFileIndex == intSplitCount)
                                    intTargetFileIndex = 0;
                            }
                        }
                    }

                    // Close the input file
                    srInFile.Close();

                    // Close the output files
                    for (intIndex = 0; intIndex <= intSplitCount - 1; intIndex++)
                    {
                        swOutFiles[intIndex].Close();
                    }

                    blnSuccess = true;
                }
            }
            catch (System.Exception ex)
            {
                var msg = "Exception in clsExtractToolRunner.SplitFileRoundRobin: " + ex.Message + "; " + clsGlobal.GetExceptionStackTrace(ex);
                Console.WriteLine(msg);
                //'m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
                blnSuccess = false;
            }

            return blnSuccess;
        }

        public void GenerateScanStatsFile()
        {
            const string inputFilePath = "QC_Shew_16_01_pt5_run7_11Apr16_Tiger_16-02-05.raw";
            const string workingDir = "E:\\DMS_WorkDir";

            var success = GenerateScanStatsFile(Path.Combine(workingDir, inputFilePath), workingDir);
            Console.WriteLine("Success: " + success);
        }

        public bool GenerateScanStatsFile(string strInputFilePath, string workingDir)
        {
            var strMSFileInfoScannerDir = "C:\\DMS_Programs\\MSFileInfoScanner";

            var strMSFileInfoScannerDLLPath = Path.Combine(strMSFileInfoScannerDir, "MSFileInfoScanner.dll");
            if (!File.Exists(strMSFileInfoScannerDLLPath))
            {
                Console.WriteLine("File Not Found: " + strMSFileInfoScannerDLLPath);
                return false;
            }

            var objScanStatsGenerator = new clsScanStatsGenerator(strMSFileInfoScannerDLLPath, m_DebugLevel);
            const int datasetID = 0;

            objScanStatsGenerator.ScanStart = 11000;
            objScanStatsGenerator.ScanEnd = 12000;

            // Create the _ScanStats.txt and _ScanStatsEx.txt files
            var blnSuccess = objScanStatsGenerator.GenerateScanStatsFile(strInputFilePath, workingDir, datasetID);

            return blnSuccess;
        }

        public void TestResultsTransfer()
        {
            var strTransferFolderPath = "\\\\proto-5\\DMS3_XFER";
            var strDatasetFolderPath = "\\\\proto-5\\LTQ_Orb1_DMS2";
            var strDatasetName = "Trmt_hg_03_orbiB_25Jan08_Draco_07-12-24";
            var strInputFolderName = "DTA_Gen_1_12_142914";

            PerformResultsXfer(strTransferFolderPath, strDatasetFolderPath, strDatasetName, strInputFolderName);
        }

        private IJobParams.CloseOutType PerformResultsXfer(string strTransferFolderPath, string strDatasetFolderPath, string strDatasetName, string strInputFolderName)
        {
            m_DebugLevel = 3;

            string Msg = null;
            string FolderToMove = null;
            string DatasetDir = null;
            string TargetDir = null;

            //Verify input folder exists in storage server xfer folder
            FolderToMove = Path.Combine(strTransferFolderPath, strDatasetName);
            FolderToMove = Path.Combine(FolderToMove, strInputFolderName);
            if (!Directory.Exists(FolderToMove))
            {
                Msg = "clsResultXferToolRunner.PerformResultsXfer(); results folder " + FolderToMove + " not found";
                //' m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }
            else if (m_DebugLevel >= 4)
            {
                //' m_logger.PostEntry("Results folder to move: " & FolderToMove, ILogger.logMsgType.logDebug, True)
            }

            // Verify dataset folder exists on storage server
            // If it doesn't exist, we will auto-create it (this behavior was added 4/24/2009)
            DatasetDir = Path.Combine(strDatasetFolderPath, strDatasetName);
            var diDatasetFolder = new DirectoryInfo(DatasetDir);
            if (!diDatasetFolder.Exists)
            {
                Msg = "clsResultXferToolRunner.PerformResultsXfer(); dataset folder " + DatasetDir + " not found; will attempt to make it";
                //' m_logger.PostEntry(Msg, ILogger.logMsgType.logWarning, clsGlobal.LOG_LOCAL_ONLY)

                try
                {
                    if (diDatasetFolder.Parent.Exists)
                    {
                        // Parent folder exists; try to create the dataset folder
                        diDatasetFolder.Create();

                        System.Threading.Thread.Sleep(500);
                        diDatasetFolder.Refresh();
                        if (!diDatasetFolder.Exists)
                        {
                            // Creation of the dataset folder failed; unable to continue
                            Msg = "clsResultXferToolRunner.PerformResultsXfer(); error trying to create missing dataset folder " + DatasetDir + ": folder creation failed for unknown reason";
                            //' m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
                            return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                        }
                    }
                    else
                    {
                        Msg = "clsResultXferToolRunner.PerformResultsXfer(); parent folder not found: " + diDatasetFolder.Parent.FullName + "; unable to continue";
                        //' m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
                        return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                    }
                }
                catch (Exception ex)
                {
                    Msg = "clsResultXferToolRunner.PerformResultsXfer(); error trying to create missing dataset folder " + DatasetDir + ": " + ex.Message;
                    //' m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)

                    return IJobParams.CloseOutType.CLOSEOUT_FAILED;
                }
            }
            else if (m_DebugLevel >= 4)
            {
                //' m_logger.PostEntry("Dataset folder path: " & DatasetDir, ILogger.logMsgType.logDebug, True)
            }

            //Determine if output folder already exists on storage server
            TargetDir = Path.Combine(DatasetDir, strInputFolderName);
            if (Directory.Exists(TargetDir))
            {
                Msg = "clsResultXferToolRunner.PerformResultsXfer(); destination directory " + DatasetDir + " already exists";
                //' m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            //Move the directory
            try
            {
                if (m_DebugLevel >= 3)
                {
                    //' m_logger.PostEntry("Moving '" & FolderToMove & "' to '" & TargetDir & "'", ILogger.logMsgType.logDebug, True)
                }

                Directory.Move(FolderToMove, TargetDir);
            }
            catch (Exception ex)
            {
                Msg = "clsResultXferToolRunner.PerformResultsXfer(); Exception moving results folder " + FolderToMove + ": " + ex.Message;
                //' m_logger.PostEntry(Msg, ILogger.logMsgType.logError, clsGlobal.LOG_LOCAL_ONLY)
                return IJobParams.CloseOutType.CLOSEOUT_FAILED;
            }

            return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
        }

        private void m_FastaTools_FileGenerationStarted1(string taskMsg)
        {
            m_GenerationStarted = true;
            m_FastaTimer.Start();
            //Reset the fasta generation timer
        }

        private void m_FastaTools_FileGenerationCompleted(string FullOutputPath)
        {
            m_FastaFileName = Path.GetFileName(FullOutputPath);
            //Get the name of the fasta file that was generated
            m_FastaTimer.Stop();
            //Stop the fasta generation timer so no false error occurs
            m_GenerationComplete = true;
            //Set the completion flag
        }

        private void m_FastaTools_FileGenerationProgress(string statusMsg, double fractionDone)
        {
            //Reset the fasta generation timer
            m_FastaTimer.Start();
        }

        private void m_FastaTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            //If this event occurs, it means there was a hang during fasta generation and the manager will have to quit
            m_FastaTimer.Stop();
            //Stop the timer to prevent false errors
            m_FastaGenTimeOut = true;
            //Set the timeout flag so an error will be reported
            m_GenerationComplete = true;
            //Set the completion flag so the fasta generation wait loop will exit
        }

        public void TestFindAndReplace()
        {
            string strTest = null;

            const double HPCMaxHours = 2.75;
            const int PPN_VALUE = 8;

            var HPCNodeCount = "3";

            System.DateTime WallTimeMax = Convert.ToDateTime("1/1/2010").AddHours(Convert.ToDouble(HPCMaxHours));
            string WallTimeResult = null;

            int intNodeCount = 0;
            int intTotalCores = 0;

            intNodeCount = Convert.ToInt32(HPCNodeCount);
            intTotalCores = intNodeCount * PPN_VALUE;

            if (intNodeCount == 1)
            {
                // Always use a wall-time value of 30 minutes when only using one node
                WallTimeResult = "00:30:00";
            }
            else
            {
                WallTimeResult = WallTimeMax.ToString("T", System.Globalization.CultureInfo.CreateSpecificCulture("fr-FR"));
                WallTimeResult = WallTimeMax.ToString("HH:mm:ss");
            }

            var NewIDMatchText = "";
            var NewIDReplaceText = "";

            var NewLabelMatchText = "";
            var NewLabelReplaceText = "";

            var OriginalGroupID = 7432;
            var CurrentMaxNum = 10000;

            NewIDMatchText = "id=\"" + OriginalGroupID.ToString();
            NewIDReplaceText = "id=\"" + (OriginalGroupID + CurrentMaxNum).ToString();

            NewLabelMatchText = "label=\"" + OriginalGroupID.ToString();
            NewLabelReplaceText = "label=\"" + (OriginalGroupID + CurrentMaxNum).ToString();

            strTest = "<group id=\"7432\" mh=\"1055.228000\" z=\"2\" rt=\"\" expect=\"1.1e-01\" label=\"SbaltOS185_c39_1:236893-241128 Shewanella_baltica_OS185_contig39 236893..241128\" type=\"model\" sumI=\"5.75\" maxI=\"105413\" fI=\"1054.13\" >";
            FindAndReplace(ref strTest, NewIDMatchText, NewIDReplaceText);
            FindAndReplace(ref strTest, NewLabelMatchText, NewLabelReplaceText);

            strTest = "<protein expect=\"-306.9\" id=\"7432.1\" uid=\"1471\" label=\"SbaltOS185_c39_1:236893-241128 Shewanella_baltica_OS185_contig39 236893..241128\" sumI=\"7.12\" >";
            FindAndReplace(ref strTest, NewIDMatchText, NewIDReplaceText);
            FindAndReplace(ref strTest, NewLabelMatchText, NewLabelReplaceText);

            strTest = "<GAML:Xdata label=\"7432.hyper\" units=\"score\">";
            FindAndReplace(ref strTest, NewIDMatchText, NewIDReplaceText);
            FindAndReplace(ref strTest, NewLabelMatchText, NewLabelReplaceText);
        }

        public void TestFindFile()
        {
            string strFolderPath = null;
            string strFileName = null;
            string strPath = null;

            strFolderPath = "\\\\proto-3\\12T_DMS3\\111410_blank_H061010A_am_000001\\111410_blank_H061010A_am_000001.d";
            strFileName = "apexAcquisition.method";

            strPath = clsAnalysisResources.FindFileInDirectoryTree(strFolderPath, strFileName);
        }

        private void FindAndReplace(ref string lineText, string strOldValue, string strNewValue)
        {
            int intMatchIndex = 0;

            intMatchIndex = lineText.IndexOf(strOldValue, System.StringComparison.Ordinal);

            if (intMatchIndex > 0)
            {
                lineText = lineText.Substring(0, intMatchIndex) + strNewValue + lineText.Substring(intMatchIndex + strOldValue.Length);
            }
            else if (intMatchIndex == 0)
            {
                lineText = strNewValue + lineText.Substring(intMatchIndex + strOldValue.Length);
            }
        }

        public void TestCosoleOutputParsing()
        {
            ParseConsoleOutputFile("F:\\Temp\\MSPathFinder_ConsoleOutput.txt");
        }

        private const string REGEX_MSPathFinder_PROGRESS = "(\\d+)% complete";
        private Regex reCheckProgress = new Regex(REGEX_MSPathFinder_PROGRESS, RegexOptions.Compiled | RegexOptions.IgnoreCase);

        // Example Console output
        //
        // MSPathFinderT 0.12 (June 17, 2014)
        // SpecFilePath: E:\DMS_WorkDir\Synocho_L2_1.pbf
        // DatabaseFilePath: C:\DMS_Temp_Org\ID_003962_71E1A1D4.fasta
        // OutputDir: E:\DMS_WorkDir
        // SearchMode: 1
        // Tda: True
        // PrecursorIonTolerancePpm: 10
        // ProductIonTolerancePpm: 10
        // MinSequenceLength: 21
        // MaxSequenceLength: 300
        // MinPrecursorIonCharge: 2
        // MaxPrecursorIonCharge: 30
        // MinProductIonCharge: 1
        // MaxProductIonCharge: 15
        // MinSequenceMass: 3000
        // MaxSequenceMass: 50000
        // MaxDynamicModificationsPerSequence: 4
        // Modifications:
        // C(2) H(3) N(1) O(1) S(0),C,fix,Everywhere,Carbamidomethyl
        // C(0) H(0) N(0) O(1) S(0),M,opt,Everywhere,Oxidation
        // C(0) H(1) N(0) O(3) S(0) P(1),S,opt,Everywhere,Phospho
        // C(0) H(1) N(0) O(3) S(0) P(1),T,opt,Everywhere,Phospho
        // C(0) H(1) N(0) O(3) S(0) P(1),Y,opt,Everywhere,Phospho
        // C(0) H(-1) N(0) O(0) S(0),C,opt,Everywhere,Dehydro
        // C(2) H(2) N(0) O(1) S(0),*,opt,ProteinNTerm,Acetyl
        // Reading raw file...Elapsed Time: 4.4701 sec
        // Determining precursor masses...Elapsed Time: 59.2987 sec
        // Deconvoluting MS2 spectra...Elapsed Time: 9.5820 sec
        // Generating C:\DMS_Temp_Org\ID_003962_71E1A1D4.icseq and C:\DMS_Temp_Org\ID_003962_71E1A1D4.icanno...    Done.
        // Reading the target database...Elapsed Time: 0.0074 sec
        // Searching the target database
        // Generating C:\DMS_Temp_Org\ID_003962_71E1A1D4.icplcp... Done.

        private DateTime dtLastProgressWriteTime = DateTime.MinValue;
        private Regex reProcessingProteins = new Regex("Processing (\\d+)th proteins", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private void ParseConsoleOutputFile(string strConsoleOutputFilePath)
        {
            const float PROGRESS_PCT_SEARCHING_TARGET_DB = 5;
            const float PROGRESS_PCT_SEARCHING_DECOY_DB = 50;
            const float PROGRESS_PCT_COMPLETE = 99;

            try
            {
                if (!File.Exists(strConsoleOutputFilePath))
                {
                    if (m_DebugLevel >= 4)
                    {
                        LogMessage("Console output file not found: " + strConsoleOutputFilePath);
                    }

                    return;
                }

                if (m_DebugLevel >= 4)
                {
                    LogMessage("Parsing file " + strConsoleOutputFilePath);
                }

                // Value between 0 and 100
                float progressComplete = 0;
                var targetProteinsSearched = 0;
                var decoyProteinsSearched = 0;

                var searchingDecoyDB = false;

                using (var srInFile = new StreamReader(new FileStream(strConsoleOutputFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (string.IsNullOrWhiteSpace(strLineIn))
                        {
                            continue;
                        }

                        var strLineInLCase = strLineIn.ToLower();

                        if (strLineInLCase.StartsWith("error:") || strLineInLCase.Contains("unhandled exception"))
                        {
                            if (string.IsNullOrEmpty(mConsoleOutputErrorMsg))
                            {
                                mConsoleOutputErrorMsg = "Error running MSPathFinder:";
                            }
                            mConsoleOutputErrorMsg += "; " + strLineIn;
                            continue;
                        }
                        else if (strLineIn.StartsWith("Searching the target database"))
                        {
                            progressComplete = PROGRESS_PCT_SEARCHING_TARGET_DB;
                        }
                        else if (strLineIn.StartsWith("Searching the decoy database"))
                        {
                            progressComplete = PROGRESS_PCT_SEARCHING_DECOY_DB;
                            searchingDecoyDB = true;
                        }
                        else
                        {
                            Match oMatch = reCheckProgress.Match(strLineIn);
                            if (oMatch.Success)
                            {
                                float.TryParse(oMatch.Groups[1].ToString(), out progressComplete);
                                continue;
                            }

                            oMatch = reProcessingProteins.Match(strLineIn);
                            if (oMatch.Success)
                            {
                                int proteinsSearched = 0;
                                if (int.TryParse(oMatch.Groups[1].ToString(), out proteinsSearched))
                                {
                                    if (searchingDecoyDB)
                                    {
                                        decoyProteinsSearched = Math.Max(decoyProteinsSearched, proteinsSearched);
                                    }
                                    else
                                    {
                                        targetProteinsSearched = Math.Max(targetProteinsSearched, proteinsSearched);
                                    }
                                }

                                continue;
                            }
                        }
                    }
                }

                if (searchingDecoyDB)
                {
                    progressComplete = clsAnalysisToolRunnerBase.ComputeIncrementalProgress(PROGRESS_PCT_SEARCHING_DECOY_DB, PROGRESS_PCT_COMPLETE, decoyProteinsSearched, targetProteinsSearched);
                }

                if (m_Progress < progressComplete || System.DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 60)
                {
                    m_Progress = progressComplete;

                    if (m_DebugLevel >= 3 || System.DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 20)
                    {
                        dtLastProgressWriteTime = System.DateTime.UtcNow;
                        LogMessage(" ... " + m_Progress.ToString("0") + "% complete");
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (m_DebugLevel >= 2)
                {
                    LogError("Error parsing console output file (" + strConsoleOutputFilePath + "): " + ex.Message);
                }
            }
        }

        public void TestProgRunner()
        {
            string strAppPath = null;

            string strWorkDir = null;
            bool blnSuccess = false;

            strAppPath = "F:\\My Documents\\Projects\\DataMining\\DMS_Managers\\Analysis_Manager\\AM_Program\\bin\\XTandem\\tandem.exe";

            strWorkDir = Path.GetDirectoryName(strAppPath);

            var objProgRunner = new clsRunDosProgram(strWorkDir)
            {
                CacheStandardOutput = true,
                CreateNoWindow = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                DebugLevel = 1,
                MonitorInterval = 1000
            };

            blnSuccess = objProgRunner.RunProgram(strAppPath, "input.xml", "X!Tandem", false);

            if (objProgRunner.CacheStandardOutput & !objProgRunner.EchoOutputToConsole)
            {
                Console.WriteLine(objProgRunner.CachedConsoleOutput);
            }

            if (objProgRunner.CachedConsoleError.Length > 0)
            {
                Console.WriteLine("Console error output");
                Console.WriteLine(objProgRunner.CachedConsoleError);
            }

            Console.WriteLine();
        }

        public void TestProgRunnerIDPicker()
        {
            var m_WorkDir = "E:\\dms_workdir";
            var strConsoleOutputFileName = "";
            var blnWriteConsoleOutputFileRealtime = false;
            bool blnSuccess = false;

            var strExePath = "C:\\DMS_Programs\\IDPicker\\idpQonvert.exe";
            var cmdStr = "-MaxFDR 0.1 -ProteinDatabase C:\\DMS_Temp_Org\\ID_003521_89E56851.fasta -SearchScoreWeights \"msgfspecprob -1\" -OptimizeScoreWeights 1 -NormalizedSearchScores msgfspecprob -DecoyPrefix Reversed_ -dump E:\\DMS_WorkDir\\Malaria844_msms_29Dec11_Draco_11-10-04.pepXML";
            var strProgramDescription = "IDPQonvert";

            var cmdRunner = new clsRunDosProgram(m_WorkDir)
            {
                CreateNoWindow = false,
                EchoOutputToConsole = false
            };

            if (string.IsNullOrEmpty(strConsoleOutputFileName) || !blnWriteConsoleOutputFileRealtime)
            {
                cmdRunner.CacheStandardOutput = false;
                cmdRunner.WriteConsoleOutputToFile = false;
            }
            else
            {
                cmdRunner.CacheStandardOutput = false;
                cmdRunner.WriteConsoleOutputToFile = true;
                cmdRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, strConsoleOutputFileName);
            }

            blnSuccess = cmdRunner.RunProgram(strExePath, cmdStr, strProgramDescription, true);

            Console.WriteLine(blnSuccess);
        }

        public void TestMSXmlCachePurge()
        {
            clsAnalysisJob objJobParams = null;
            clsMyEMSLUtilities myEMSLUtilities = null;

            clsCodeTestAM objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            const string cacheFolderPath = "\\\\proto-2\\past\\PurgeTest";

            try
            {
                objToolRunner.PurgeOldServerCacheFilesTest(cacheFolderPath, 10);
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
        /// <param name="strFolderPath">Folder to examine</param>
        /// <param name="strDatasetName">Dataset name</param>
        /// <remarks></remarks>
        public void FixICR2LSResultFileNames(string strFolderPath, string strDatasetName)
        {
            List<string> objExtensionsToCheck = new List<string>();

            DirectoryInfo fiFolder = default(DirectoryInfo);

            try
            {
                objExtensionsToCheck.Add("PAR");
                objExtensionsToCheck.Add("Pek");

                var strDSNameLCase = strDatasetName.ToLower();

                fiFolder = new DirectoryInfo(strFolderPath);

                if (fiFolder.Exists)
                {
                    foreach (var strExtension in objExtensionsToCheck)
                    {
                        foreach (var fiFile in fiFolder.GetFiles("*." + strExtension))
                        {
                            if (fiFile.Name.ToLower().StartsWith(strDSNameLCase))
                            {
                                var strDesiredName = strDatasetName + "_" + System.DateTime.Now.ToString("M_d_yyyy") + "." + strExtension;

                                if (fiFile.Name.ToLower() != strDesiredName.ToLower())
                                {
                                    try
                                    {
                                        fiFile.MoveTo(Path.Combine(fiFolder.FullName, strDesiredName));
                                    }
                                    catch (Exception ex)
                                    {
                                        // Rename failed; that means the correct file already exists; this is OK
                                    }
                                }

                                break;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
            }
        }

        public void SystemMemoryUsage()
        {
            // The following reports various memory stats
            // However, it doesn't report the available physical memory

            //Try

            //	Dim winQuery As System.Management.ObjectQuery
            //	Dim searcher As System.Management.ManagementObjectSearcher

            //	winQuery = New System.Management.ObjectQuery("SELECT * FROM Win32_LogicalMemoryConfiguration")

            //	searcher = New System.Management.ManagementObjectSearcher(winQuery)

            //	For Each item As System.Management.ManagementObject In searcher.Get()
            //		Console.WriteLine("Total Space = " & item("TotalPageFileSpace").ToString())
            //		Console.WriteLine("Total Physical Memory = " & item("TotalPhysicalMemory").ToString())
            //		Console.WriteLine("Total Virtual Memory = " & item("TotalVirtualMemory").ToString())
            //		Console.WriteLine("Available Virtual Memory = " & item("AvailableVirtualMemory").ToString())
            //	Next
            //Catch ex As Exception
            //	Console.WriteLine()
            //	Console.WriteLine("Error in SystemMemoryUsage (A): " & ex.Message)
            //	Console.WriteLine(clsGlobal.GetExceptionStackTrace(ex, True))

            //End Try

            // TODO: can use CIM_OperatingSystem to get available physical memory

            float sngFreeMemoryMB = 0;

            try
            {
                var mFreeMemoryPerformanceCounter = new System.Diagnostics.PerformanceCounter("Memory", "Available MBytes");
                mFreeMemoryPerformanceCounter.ReadOnly = true;

                sngFreeMemoryMB = mFreeMemoryPerformanceCounter.NextValue();

                Console.WriteLine();
                Console.WriteLine("Available memory (MB) = " + sngFreeMemoryMB.ToString());
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine("Error in SystemMemoryUsage (C): " + ex.Message);
                Console.WriteLine(clsGlobal.GetExceptionStackTrace(ex, true));

                var rePub1000 = new System.Text.RegularExpressions.Regex("Pub-1\\d{3,}", RegexOptions.IgnoreCase);
                if (rePub1000.IsMatch(Environment.MachineName))
                {
                    // The Memory performance counters are not available on Windows instances running under VMWare on PIC
                }
                else
                {
                    // To avoid seeing this in the logs continually, we will only post this log message between 12 am and 12:30 am
                    // A possible fix for this is to add the user who is running this process to the "Performance Monitor Users" group
                    // in "Local Users and Groups" on the machine showing this error.  Alternatively, add the user to the "Administrators" group.
                    // In either case, you will need to reboot the computer for the change to take effect
                    if (System.DateTime.Now.Hour == 0 & System.DateTime.Now.Minute <= 30)
                    {
                        LogError("Error instantiating the Memory.[Available MBytes] performance counter " +
                            "(this message is only logged between 12 am and 12:30 am)", ex);
                    }
                }

                try
                {
                    var memInfo = new SystemMemoryInfo();
                    var memData = memInfo.MemoryStatus;

                    sngFreeMemoryMB = Convert.ToSingle(memData.ullAvailPhys / 1024.0 / 1024.0);
                    Console.WriteLine("Available memory from VB: " + sngFreeMemoryMB + " MB");
                }
                catch (Exception ex2)
                {
                    Console.WriteLine();
                    Console.WriteLine("Error in SystemMemoryUsage using Microsoft.VisualBasic.Devices.ComputerInfo: " + ex2.Message);
                    Console.WriteLine(clsGlobal.GetExceptionStackTrace(ex2, true));
                }
            }
        }

        private class SystemMemoryInfo
        {
            // http://pinvoke.net/default.aspx/kernel32/GlobalMemoryStatusEx.html
            public MemoryStatusEx MemoryStatus;
            public const int MemoryTightConst = 80;

            public bool isMemoryTight()
            {
                if (MemoryLoad > MemoryTightConst)
                    return true;
                else
                    return false;
            }

            public uint MemoryLoad { get; private set; }

            public SystemMemoryInfo()
            {
                MemoryStatus = new MemoryStatusEx();
                if (GlobalMemoryStatusEx(MemoryStatus))
                {
                    MemoryLoad = MemoryStatus.dwMemoryLoad;
                    //etc.. Repeat for other structure members
                }
                else
                {
                    // Use a more appropriate Exception Type. 'Exception' should almost never be thrown
                    throw new Exception("Unable to initalize the GlobalMemoryStatusEx API");
                }
            }

            [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
            public class MemoryStatusEx
            {
                public uint dwLength;
                public uint dwMemoryLoad;
                public ulong ullTotalPhys;
                public ulong ullAvailPhys;
                public ulong ullTotalPageFile;
                public ulong ullAvailPageFile;
                public ulong ullTotalVirtual;
                public ulong ullAvailVirtual;
                public ulong ullAvailExtendedVirtual;
                public MemoryStatusEx()
                {
                    this.dwLength = (uint)Marshal.SizeOf(typeof(MemoryStatusEx));
                }
            }

            [return: MarshalAs(UnmanagedType.Bool)]
            [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
            static extern bool GlobalMemoryStatusEx([In, Out] MemoryStatusEx lpBuffer);
        }

        public void TestDTAWatcher(string strWorkDir, float sngWaitTimeMinutes)
        {
            m_Progress = 0;
            m_MaxScanInFile = 10000;

            // Setup a FileSystemWatcher to watch for new .Dta files being created
            // We can compare the scan number of new .Dta files to the m_MaxScanInFile value to determine % complete
            mDTAWatcher = new FileSystemWatcher(strWorkDir, "*.dta");

            mDTAWatcher.IncludeSubdirectories = false;
            mDTAWatcher.NotifyFilter = NotifyFilters.FileName | NotifyFilters.CreationTime;

            mDTAWatcher.EnableRaisingEvents = true;

            System.DateTime dtStartTime = System.DateTime.UtcNow;

            do
            {
                System.Threading.Thread.Sleep(2000);
                Console.WriteLine("Current progress: " + m_Progress);
            } while (System.DateTime.UtcNow.Subtract(dtStartTime).TotalMinutes < sngWaitTimeMinutes);
        }

        Regex static_UpdateDTAProgress_reDTAFile;
        private void UpdateDTAProgress(string DTAFileName)
        {
            int intScanNumber = 0;

            if (static_UpdateDTAProgress_reDTAFile == null)
            {
                static_UpdateDTAProgress_reDTAFile = new System.Text.RegularExpressions.Regex("(\\d+)\\.\\d+\\.\\d\\.dta$", System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.IgnoreCase);
            }

            try
            {
                // Extract out the scan number from the DTA filename
                var reMatch = static_UpdateDTAProgress_reDTAFile.Match(DTAFileName);
                if (reMatch.Success)
                {
                    if (int.TryParse(reMatch.Groups[1].Value, out intScanNumber))
                    {
                        m_Progress = Convert.ToSingle(intScanNumber / m_MaxScanInFile * 100);
                    }
                }
            }
            catch (Exception ex)
            {
                // Ignore errors here
            }
        }

        private void mDTAWatcher_Created(object sender, FileSystemEventArgs e)
        {
            UpdateDTAProgress(e.Name);
        }

        public void TestGetFileContents()
        {
            var strFilePath = "TestInputFile.txt";
            string strContents = null;

            strContents = GetFileContents(strFilePath);

            Console.WriteLine(strContents);
        }

        private string GetFileContents(string filePath)
        {
            FileInfo fi = new FileInfo(filePath);
            string s = null;

            var tr = new StreamReader(new FileStream(fi.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            s = tr.ReadToEnd();

            if (s == null)
            {
                s = string.Empty;
            }

            return s;
        }

        public void TestGetVersionInfo()
        {
            clsAnalysisJob objJobParams = null;
            clsMyEMSLUtilities myEMSLUtilities = null;

            clsCodeTestAM objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            var pathToTestx86 = "F:\\My Documents\\Projects\\DataMining\\DMS_Programs\\DLLVersionInspector\\bin\\32bit_Dll_Examples\\UIMFLibrary.dll";
            var pathToTestx64 = "F:\\My Documents\\Projects\\DataMining\\DMS_Programs\\DLLVersionInspector\\bin\\64bit_Dll_Examples\\UIMFLibrary.dll";
            var pathToTestAnyCPU = "F:\\My Documents\\Projects\\DataMining\\DMS_Programs\\DLLVersionInspector\\bin\\AnyCPU_DLL_Examples\\UIMFLibrary.dll";

            var strToolVersionInfo = string.Empty;
            objToolRunner.StoreToolVersionInfoOneFile(ref strToolVersionInfo, pathToTestx86);
            Console.WriteLine(strToolVersionInfo);

            strToolVersionInfo = string.Empty;
            objToolRunner.StoreToolVersionInfoOneFile(ref strToolVersionInfo, pathToTestx64);
            Console.WriteLine(strToolVersionInfo);

            strToolVersionInfo = string.Empty;
            objToolRunner.StoreToolVersionInfoOneFile(ref strToolVersionInfo, pathToTestAnyCPU);
            Console.WriteLine(strToolVersionInfo);
        }

        public void RemoveSparseSpectra()
        {
            var oCDTAUtilities = new clsCDTAUtilities();

            oCDTAUtilities.RemoveSparseSpectra("e:\\dms_workdir", "ALZ_VP2P101_C_SCX_02_7Dec08_Draco_08-10-29_dta.txt");
        }

        public void ValidateCentroided()
        {
            const int intDebugLevel = 2;

            clsResourceTestClass objResources = null;
            objResources = GetResourcesObject(intDebugLevel);

            objResources.ValidateCDTAFileIsCentroided("\\\\proto-7\\dms3_Xfer\\UW_HCV_03_Run2_19Dec13_Pippin_13-07-06\\DTA_Gen_1_26_350136\\UW_HCV_03_Run2_19Dec13_Pippin_13-07-06_dta.txt");
        }

        public bool ValidateSequestNodeCount(string strLogFilePath)
        {
            const int ERROR_CODE_A = 2;
            const int ERROR_CODE_B = 4;
            const int ERROR_CODE_C = 8;
            const int ERROR_CODE_D = 16;
            const int ERROR_CODE_E = 32;

            string strParam = null;
            string strLineIn = null;
            string strHostName = null;

            // This dictionary tracks the number of DTAs processed by each node
            Dictionary<string, int> dctHostCounts;

            // This dictionary tracks the number of distinct nodes on each host
            Dictionary<string, int> dctHostNodeCount;

            int intValue = 0;

            // This dictionary tracks the number of DTAs processed per node on each host
            Dictionary<string, float> dctHostProcessingRate;

            // This array is used to compute a median
            float[] sngHostProcessingRateSorted = null;

            bool blnShowDetailedRates = false;

            int intHostCount = 0;
            int intNodeCountStarted = 0;
            int intNodeCountActive = 0;
            int intDTACount = 0;

            int intNodeCountExpected = 0;

            string strProcessingMsg = null;

            try
            {
                blnShowDetailedRates = false;

                if (!File.Exists(strLogFilePath))
                {
                    strProcessingMsg = "Sequest.log file not found; cannot verify the sequest node count";
                    LogWarning(strProcessingMsg + ": " + strLogFilePath);
                    return false;
                }

                // Initialize the RegEx objects
                var reStartingTask = new Regex("Starting the SEQUEST task on (\\d+) node", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                var reWaitingForReadyMsg = new Regex("Waiting for ready messages from (\\d+) node", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                var reReceivedReadyMsg = new Regex("received ready messsage from (.+)\\(", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                var reSpawnedSlaveProcesses = new Regex("Spawned (\\d+) slave processes", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
                var reSearchedDTAFile = new Regex("Searched dta file .+ on (.+)", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

                intHostCount = 0;
                // Value for reStartingTask
                intNodeCountStarted = 0;
                // Value for reWaitingForReadyMsg
                intNodeCountActive = 0;
                // Value for reSpawnedSlaveProcesses
                intDTACount = 0;

                // Note: This value is obtained when the manager params are grabbed from the Manager Control DB
                // Use this query to view/update expected node counts'
                //  SELECT M.M_Name, PV.MgrID, PV.Value
                //  FROM T_ParamValue AS PV INNER JOIN T_Mgrs AS M ON PV.MgrID = M.M_ID
                //  WHERE (PV.TypeID = 122)

                strParam = m_mgrParams.GetParam("SequestNodeCountExpected");
                if (int.TryParse(strParam, out intNodeCountExpected))
                {
                }
                else
                {
                    intNodeCountExpected = 0;
                }

                // Initialize the dictionary that will track the number of spectra processed by each host
                dctHostCounts = new Dictionary<string, int>();

                // Initialize the dictionary that will track the number of distinct nodes on each host
                dctHostNodeCount = new Dictionary<string, int>();

                // Initialize the dictionary that will track processing rates
                dctHostProcessingRate = new Dictionary<string, float>();

                using (var srLogFile = new StreamReader(new FileStream(strLogFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    // Read each line from the input file
                    while (!srLogFile.EndOfStream)
                    {
                        strLineIn = srLogFile.ReadLine();

                        if ((strLineIn != null) && strLineIn.Length > 0)
                        {
                            // See if the line matches one of the expected RegEx values
                            var objMatch = reStartingTask.Match(strLineIn);
                            if ((objMatch != null) && objMatch.Success)
                            {
                                if (!int.TryParse(objMatch.Groups[1].Value, out intHostCount))
                                {
                                    strProcessingMsg = "Unable to parse out the Host Count from the 'Starting the SEQUEST task ...' entry in the Sequest.log file";
                                    LogWarning(strProcessingMsg);
                                }
                            }
                            else
                            {
                                objMatch = reWaitingForReadyMsg.Match(strLineIn);
                                if ((objMatch != null) && objMatch.Success)
                                {
                                    if (!int.TryParse(objMatch.Groups[1].Value, out intNodeCountStarted))
                                    {
                                        strProcessingMsg = "Unable to parse out the Node Count from the 'Waiting for ready messages ...' entry in the Sequest.log file";
                                        LogWarning(strProcessingMsg);
                                    }
                                }
                                else
                                {
                                    objMatch = reReceivedReadyMsg.Match(strLineIn);
                                    if ((objMatch != null) && objMatch.Success)
                                    {
                                        strHostName = objMatch.Groups[1].Value;

                                        if (dctHostNodeCount.TryGetValue(strHostName, out intValue))
                                        {
                                            dctHostNodeCount[strHostName] = intValue + 1;
                                        }
                                        else
                                        {
                                            dctHostNodeCount.Add(strHostName, 1);
                                        }
                                    }
                                    else
                                    {
                                        objMatch = reSpawnedSlaveProcesses.Match(strLineIn);
                                        if ((objMatch != null) && objMatch.Success)
                                        {
                                            if (!int.TryParse(objMatch.Groups[1].Value, out intNodeCountActive))
                                            {
                                                strProcessingMsg = "Unable to parse out the Active Node Count from the 'Spawned xx slave processes ...' entry in the Sequest.log file";
                                                LogWarning(strProcessingMsg);
                                            }
                                        }
                                        else
                                        {
                                            objMatch = reSearchedDTAFile.Match(strLineIn);
                                            if ((objMatch != null) && objMatch.Success)
                                            {
                                                strHostName = objMatch.Groups[1].Value;

                                                if ((strHostName != null))
                                                {
                                                    if (dctHostCounts.TryGetValue(strHostName, out intValue))
                                                    {
                                                        dctHostCounts[strHostName] = intValue + 1;
                                                    }
                                                    else
                                                    {
                                                        dctHostCounts.Add(strHostName, 1);
                                                    }

                                                    intDTACount += 1;
                                                }
                                            }
                                            else
                                            {
                                                // Ignore this line
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                try
                {
                    // Validate the stats

                    strProcessingMsg = "HostCount=" + intHostCount + "; NodeCountActive=" + intNodeCountActive;
                    if (m_DebugLevel >= 1)
                    {
                        LogMessage(strProcessingMsg);
                    }
                    m_EvalMessage = string.Copy(strProcessingMsg);

                    if (intNodeCountActive < intNodeCountExpected || intNodeCountExpected == 0)
                    {
                        strProcessingMsg = "Error: NodeCountActive less than expected value (" + intNodeCountActive + " vs. " + intNodeCountExpected + ")";
                        LogError(strProcessingMsg);

                        // Update the evaluation message and evaluation code
                        // These will be used by sub CloseTask in clsAnalysisJob
                        //
                        // An evaluation code with bit ERROR_CODE_A set will result in DMS_Pipeline DB views
                        //  V_Job_Steps_Stale_and_Failed and V_Sequest_Cluster_Warnings showing this message:
                        //  "SEQUEST node count is less than the expected value"

                        m_EvalMessage += "; " + strProcessingMsg;
                        m_EvalCode = m_EvalCode | ERROR_CODE_A;
                    }
                    else
                    {
                        if (intNodeCountStarted != intNodeCountActive)
                        {
                            strProcessingMsg = "Warning: NodeCountStarted (" + intNodeCountStarted + ") <> NodeCountActive";
                            LogWarning(strProcessingMsg);
                            m_EvalMessage += "; " + strProcessingMsg;
                            m_EvalCode = m_EvalCode | ERROR_CODE_B;

                            // Update the evaluation message and evaluation code
                            // These will be used by sub CloseTask in clsAnalysisJob
                            // An evaluation code with bit ERROR_CODE_A set will result in view V_Sequest_Cluster_Warnings in the DMS_Pipeline DB showing this message:
                            //  "SEQUEST node count is less than the expected value"
                        }
                    }

                    if (dctHostCounts.Count < intHostCount)
                    {
                        // Only record an error here if the number of DTAs processed was at least 2x the number of nodes
                        if (intDTACount >= 2 * intNodeCountActive)
                        {
                            strProcessingMsg = "Error: only " + dctHostCounts.Count + " host" + CheckForPlurality(dctHostCounts.Count) + " processed DTAs";
                            LogError(strProcessingMsg);
                            m_EvalMessage += "; " + strProcessingMsg;
                            m_EvalCode = m_EvalCode | ERROR_CODE_C;
                        }
                    }

                    // See if any of the hosts processed far fewer or far more spectra than the other hosts
                    // When comparing hosts, we need to scale by the number of active nodes on each host
                    // We'll populate intHostProcessingRate() with the number of DTAs processed per node on each host

                    const float LOW_THRESHOLD_MULTIPLIER = 0.33f;
                    const float HIGH_THRESHOLD_MULTIPLIER = 2;

                    int intNodeCountThisHost = 0;
                    int intIndex = 0;

                    float sngProcessingRate = 0;
                    float sngProcessingRateMedian = 0;

                    int intMidpoint = 0;
                    float sngThresholdRate = 0;
                    int intWarningCount = 0;

                    foreach (KeyValuePair<string, int> objItem in dctHostCounts)
                    {
                        intNodeCountThisHost = 0;
                        dctHostNodeCount.TryGetValue(objItem.Key, out intNodeCountThisHost);
                        if (intNodeCountThisHost < 1)
                            intNodeCountThisHost = 1;

                        sngProcessingRate = Convert.ToSingle(objItem.Value / intNodeCountThisHost);
                        dctHostProcessingRate.Add(objItem.Key, sngProcessingRate);
                    }

                    // Determine the median number of spectra processed

                    sngHostProcessingRateSorted = new float[dctHostProcessingRate.Count];

                    intIndex = 0;
                    foreach (KeyValuePair<string, float> objItem in dctHostProcessingRate)
                    {
                        sngHostProcessingRateSorted[intIndex] = objItem.Value;
                        intIndex += 1;
                    }

                    // Now sort sngHostProcessingRateSorted
                    Array.Sort(sngHostProcessingRateSorted, 0, sngHostProcessingRateSorted.Length);

                    if (sngHostProcessingRateSorted.Length <= 2)
                    {
                        intMidpoint = 0;
                    }
                    else
                    {
                        intMidpoint = Convert.ToInt32(Math.Floor((double)sngHostProcessingRateSorted.Length / 2));
                    }

                    sngProcessingRateMedian = sngHostProcessingRateSorted[intMidpoint];

                    // Count the number of hosts that had a processing rate fewer than LOW_THRESHOLD_MULTIPLIER times the the median value
                    intWarningCount = 0;
                    sngThresholdRate = Convert.ToSingle(LOW_THRESHOLD_MULTIPLIER * sngProcessingRateMedian);

                    foreach (KeyValuePair<string, float> objItem in dctHostProcessingRate)
                    {
                        if (objItem.Value < sngThresholdRate)
                        {
                            intWarningCount = +1;
                        }
                    }

                    if (intWarningCount > 0)
                    {
                        strProcessingMsg = "Warning: " + intWarningCount + " host" + CheckForPlurality(intWarningCount) + " processed fewer than " + sngThresholdRate.ToString("0.0") + " DTAs/node, which is " + LOW_THRESHOLD_MULTIPLIER + " times the median value of " + sngProcessingRateMedian.ToString("0.0");
                        LogWarning(strProcessingMsg);

                        m_EvalMessage += "; " + strProcessingMsg;
                        m_EvalCode = m_EvalCode | ERROR_CODE_D;
                        blnShowDetailedRates = true;
                    }

                    // Count the number of nodes that had a processing rate more than HIGH_THRESHOLD_MULTIPLIER times the median value
                    // When comparing hosts, have to scale by the number of active nodes on each host
                    intWarningCount = 0;
                    sngThresholdRate = Convert.ToSingle(HIGH_THRESHOLD_MULTIPLIER * sngProcessingRateMedian);

                    foreach (KeyValuePair<string, float> objItem in dctHostProcessingRate)
                    {
                        if (objItem.Value > sngThresholdRate)
                        {
                            intWarningCount = +1;
                        }
                    }

                    if (intWarningCount > 0)
                    {
                        strProcessingMsg = "Warning: " + intWarningCount + " host" + CheckForPlurality(intWarningCount) + " processed more than " + sngThresholdRate.ToString("0.0") + " DTAs/node, which is " + HIGH_THRESHOLD_MULTIPLIER + " times the median value of " + sngProcessingRateMedian.ToString("0.0");
                        LogWarning(strProcessingMsg);

                        m_EvalMessage += "; " + strProcessingMsg;
                        m_EvalCode = m_EvalCode | ERROR_CODE_E;
                        blnShowDetailedRates = true;
                    }

                    if (m_DebugLevel >= 2 || blnShowDetailedRates)
                    {
                        // Log the number of DTAs processed by each host

                        foreach (KeyValuePair<string, int> objItem in dctHostCounts)
                        {
                            intNodeCountThisHost = 0;
                            dctHostNodeCount.TryGetValue(objItem.Key, out intNodeCountThisHost);
                            if (intNodeCountThisHost < 1)
                                intNodeCountThisHost = 1;

                            sngProcessingRate = 0;
                            dctHostProcessingRate.TryGetValue(objItem.Key, out sngProcessingRate);

                            strProcessingMsg = "Host " + objItem.Key + " processed " + objItem.Value +
                                " DTA" + CheckForPlurality(objItem.Value) +
                                " using " + intNodeCountThisHost + " node" + CheckForPlurality(intNodeCountThisHost) +
                                " (" + sngProcessingRate.ToString("0.0") + " DTAs/node)";

                            LogMessage(strProcessingMsg, 2);
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Error occurred
                    strProcessingMsg = "Error in validating the stats in ValidateSequestNodeCount" + ex.Message;
                    LogError(strProcessingMsg);
                    return false;
                }
            }
            catch (Exception ex)
            {
                // Error occurred
                strProcessingMsg = "Error parsing Sequest.log file in ValidateSequestNodeCount" + ex.Message;
                LogError(strProcessingMsg);
                return false;
            }

            return true;
        }

        private string CheckForPlurality(int intValue)
        {
            if (intValue == 1)
            {
                return "";
            }
            else
            {
                return "s";
            }
        }

        private class clsResourceTestClass : clsAnalysisResources
        {
            public override IJobParams.CloseOutType GetResources()
            {
                return IJobParams.CloseOutType.CLOSEOUT_SUCCESS;
            }
        }

        #region "clsEventNotifier events"

        private void RegisterEvents(clsEventNotifier oProcessingClass)
        {
            oProcessingClass.StatusEvent += StatusEventHandler;
            oProcessingClass.ErrorEvent += ErrorEventHandler;
            oProcessingClass.WarningEvent += WarningEventHandler;
            oProcessingClass.ProgressUpdate += ProgressUpdateHandler;
        }

        private void StatusEventHandler(string statusMessage)
        {
            LogMessage(statusMessage);
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

        #endregion
    }
}
