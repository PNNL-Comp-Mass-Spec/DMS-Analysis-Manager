using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Runtime.Versioning;
using System.Text.RegularExpressions;
using AnalysisManagerBase;

namespace AnalysisManagerProg
{
    /// <summary>
    /// Collection of test code
    /// </summary>
    public class clsCodeTest : clsLoggerBase
    {
        private const string WORKING_DIRECTORY = @"C:\DMS_WorkDir";
        private Protein_Exporter.ExportProteinCollectionsIFC.IGetFASTAFromDMS m_FastaTools;
        private bool m_GenerationComplete;
        private readonly string m_FastaToolsCnStr = "Data Source=proteinseqs;Initial Catalog=Protein_Sequences;Integrated Security=SSPI;";
        private string m_FastaFileName = "";
        private System.Timers.Timer m_FastaTimer;

        private bool m_FastaGenTimeOut;

        private readonly IMgrParams m_mgrParams;

        private DateTime mLastStatusTime;

        // 450 seconds is 7.5 minutes
        private const int FASTA_GEN_TIMEOUT_INTERVAL_SEC = 450;

        #region "Properties"

        /// <summary>
        /// When true, show extra messages at the console
        /// </summary>
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

            try
            {   // Get settings from config file
                var lstMgrSettings = clsMainProcess.LoadMgrSettingsFromFile();

                m_mgrParams = new clsAnalysisMgrSettings(CUSTOM_LOG_SOURCE_NAME, CUSTOM_LOG_NAME, lstMgrSettings, clsGlobal.GetAppFolderPath(), TRACE_MODE_ENABLED);

                m_DebugLevel = 2;

                m_mgrParams.SetParam("workdir", @"C:\DMS_WorkDir");
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

        /// <summary>
        /// Display the version of all DLLs in the application folder, including the .NET framework that they were compiled against
        /// </summary>
        /// <param name="displayDllPath"></param>
        /// <param name="fileNameFileSpec"></param>
        public void DisplayDllVersions(string displayDllPath, string fileNameFileSpec = "*.dll")
        {
            try
            {
                DirectoryInfo diSourceFolder;

                if (string.IsNullOrWhiteSpace(displayDllPath))
                {
                    diSourceFolder = new DirectoryInfo(".");
                }
                else
                {
                    diSourceFolder = new DirectoryInfo(displayDllPath);
                }

                List<FileInfo> lstFiles;
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

                        var fileAssembly = Assembly.LoadFrom(fiFile.FullName);
                        var fileVersion = fileAssembly.ImageRuntimeVersion;
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
                            var fileAssembly2 = Assembly.ReflectionOnlyLoadFrom(fiFile.FullName);
                            var fileVersion2 = fileAssembly2.ImageRuntimeVersion;

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

                Console.WriteLine("{0,-50} {1,-20} {2}", "Filename", ".NET Version", "Target Framework");
                foreach (var result in query)
                {
                    Console.WriteLine("{0,-50} {1,-20} {2}", Path.GetFileName(result.Key), " " + result.Value.Key, result.Value.Value);
                }

                if (dctErrors.Count > 0)
                {
                    Console.WriteLine();
                    Console.WriteLine();
                    Console.WriteLine("DLLs likely not .NET");

                    var errorList = (from item in dctErrors orderby item.Key select item).ToList();

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

        private clsAnalysisJob InitializeMgrAndJobParams(short intDebugLevel)
        {
            var objJobParams = new clsAnalysisJob(m_mgrParams, intDebugLevel);

            m_mgrParams.SetParam("workdir", WORKING_DIRECTORY);
            m_mgrParams.SetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME, "Monroe_Test");
            m_mgrParams.SetParam("debuglevel", intDebugLevel.ToString());

            objJobParams.SetParam("StepParameters", "StepTool", "TestStepTool");
            objJobParams.SetParam("JobParameters", "ToolName", "TestTool");

            objJobParams.SetParam("StepParameters", "Job", "12345");
            objJobParams.SetParam("StepParameters", "OutputFolderName", "Test_Results");

            return objJobParams;
        }

        private clsCodeTestAM GetCodeTestToolRunner(out clsAnalysisJob objJobParams, out clsMyEMSLUtilities myEMSLUtilities)
        {
            const short DEBUG_LEVEL = 2;

            objJobParams = InitializeMgrAndJobParams(DEBUG_LEVEL);

            var statusTools = new clsStatusFile("Status.xml", DEBUG_LEVEL);
            RegisterEvents(statusTools);

            var objSummaryFile = new clsSummaryFile();

            myEMSLUtilities = new clsMyEMSLUtilities(DEBUG_LEVEL, WORKING_DIRECTORY);
            RegisterEvents(myEMSLUtilities);

            var objToolRunner = new clsCodeTestAM();
            objToolRunner.Setup(m_mgrParams, objJobParams, statusTools, objSummaryFile, myEMSLUtilities);

            return objToolRunner;
        }

        private clsResourceTestClass GetResourcesObject(int intDebugLevel)
        {
            var objJobParams = new clsAnalysisJob(m_mgrParams, 0);

            return GetResourcesObject(intDebugLevel, objJobParams);
        }

        private clsResourceTestClass GetResourcesObject(int intDebugLevel, IJobParams objJobParams)
        {
            var objResources = new clsResourceTestClass();

            var statusTools = new clsStatusFile("Status.xml", intDebugLevel);
            RegisterEvents(statusTools);

            var myEMSLUtilities = new clsMyEMSLUtilities(intDebugLevel, WORKING_DIRECTORY);
            RegisterEvents(myEMSLUtilities);

            m_mgrParams.SetParam("workdir", WORKING_DIRECTORY);
            m_mgrParams.SetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME, "Monroe_Test");
            m_mgrParams.SetParam("debuglevel", intDebugLevel.ToString());
            m_mgrParams.SetParam("zipprogram", @"C:\PKWARE\PKZIPC\pkzipc.exe");

            objJobParams.SetParam("StepParameters", "StepTool", "TestStepTool");
            objJobParams.SetParam("JobParameters", "ToolName", "TestTool");

            objJobParams.SetParam("StepParameters", "Job", "12345");
            objJobParams.SetParam("StepParameters", "OutputFolderName", "Test_Results");

            objResources.Setup(m_mgrParams, objJobParams, statusTools, myEMSLUtilities);

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

            var objJobParams = new clsAnalysisJob(m_mgrParams, 0);

            m_mgrParams.SetParam("workdir", @"C:\DMS_WorkDir");
            m_mgrParams.SetParam(clsAnalysisMgrSettings.MGR_PARAM_MGR_NAME, "Monroe_Test");
            m_mgrParams.SetParam("debuglevel", intDebugLevel.ToString());

            objJobParams.SetParam("StepParameters", "StepTool", "TestStepTool");
            objJobParams.SetParam("JobParameters", "ToolName", "TestTool");

            objJobParams.SetParam("StepParameters", "Job", "12345");
            objJobParams.SetParam("StepParameters", "OutputFolderName", "Test_Results");

            return objJobParams;
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

                var mCPUUsagePerformanceCounter = new System.Diagnostics.PerformanceCounter("Processor", "% Processor Time", "_Total")
                {
                    ReadOnly = true
                };

                var mFreeMemoryPerformanceCounter = new System.Diagnostics.PerformanceCounter("Memory", "Available MBytes") { ReadOnly = true };
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine("Error in PerformanceCounterTest: " + ex.Message);
                Console.WriteLine(clsGlobal.GetExceptionStackTrace(ex, true));
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

        private bool ProcessDtaRefineryLogFiles(int intJobStart, int intJobEnd)
        {
            // Query the Pipeline DB to find jobs that ran DTA Refinery

            // Dim strSql As String = "SELECT JSH.Dataset, J.Dataset_ID, JSH.Job, JSH.Output_Folder, DFP.Dataset_Folder_Path" &
            //   " FROM DMS_Pipeline.dbo.V_Job_Steps_History JSH INNER JOIN" &
            //   "      DMS_Pipeline.dbo.T_Jobs J ON JSH.Job = J.Job INNER JOIN" &
            //   "      DMS5.dbo.V_Dataset_Folder_Paths DFP ON J.Dataset_ID = DFP.Dataset_ID" &
            //   " WHERE (JSH.Job Between " & intJobStart & " and " & intJobEnd & ") AND (JSH.Tool = 'DTA_Refinery') AND (JSH.Most_Recent_Entry = 1) AND (JSH.State = 5)"

            var strSql =
                "SELECT JS.Dataset, J.Dataset_ID, JS.Job, JS.Output_Folder, DFP.Dataset_Folder_Path, JS.Transfer_Folder_Path" +
                " FROM DMS_Pipeline.dbo.V_Job_Steps JS INNER JOIN" +
                "      DMS_Pipeline.dbo.T_Jobs J ON JS.Job = J.Job INNER JOIN" +
                "      DMS5.dbo.V_Dataset_Folder_Paths DFP ON J.Dataset_ID = DFP.Dataset_ID" +
                " WHERE (JS.Job Between " + intJobStart + " and " + intJobEnd + ") AND (JS.Tool = 'DTA_Refinery') AND (JS.State = 5)";

            const string strConnectionString = "Data Source=gigasax;Initial Catalog=DMS5;Integrated Security=SSPI;";
            const short RetryCount = 2;

            DataTable Dt;

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

            // var strWorkDir = m_mgrParams.GetParam("workdir");
            // var blnPostResultsToDB = true;

            // Note: add file clsDtaRefLogMassErrorExtractor to this project to use this functionality
            // var oMassErrorExtractor = new clsDtaRefLogMassErrorExtractor(m_mgrParams, strWorkDir, m_DebugLevel, blnPostResultsToDB);

            foreach (DataRow CurRow in Dt.Rows)
            {
                var dataset = clsGlobal.DbCStr(CurRow["Dataset"]);
                var datasetID = clsGlobal.DbCInt(CurRow["Dataset_ID"]);
                var job = clsGlobal.DbCInt(CurRow["Job"]);
                var dtaRefineryDataFolderPath = Path.Combine(clsGlobal.DbCStr(CurRow["Dataset_Folder_Path"]),
                                                             clsGlobal.DbCStr(CurRow["Output_Folder"]));

                if (!Directory.Exists(dtaRefineryDataFolderPath))
                {
                    dtaRefineryDataFolderPath = Path.Combine(clsGlobal.DbCStr(CurRow["Transfer_Folder_Path"]), clsGlobal.DbCStr(CurRow["Output_Folder"]));
                }

                if (Directory.Exists(dtaRefineryDataFolderPath))
                {
                    Console.WriteLine("Processing " + dtaRefineryDataFolderPath);
                    // oMassErrorExtractor.ParseDTARefineryLogFile(udtPSMJob.Dataset, udtPSMJob.DatasetID, udtPSMJob.Job, udtPSMJob.DtaRefineryDataFolderPath)
                }
                else
                {
                    Console.WriteLine("Skipping " + dtaRefineryDataFolderPath);
                }
            }

            return true;
        }

        private clsRunDosProgram m_RunProgTool;

        /// <summary>
        /// Use MSConvert to convert a .raw file to .mgf
        /// </summary>
        public void RunMSConvert()
        {
            var workDir = @"C:\DMS_WorkDir";

            var exePath = @"C:\DMS_Programs\ProteoWizard\msconvert.exe";
            var dataFilePath = @"C:\DMS_WorkDir\QC_ShewPartialInj_15_02-100ng_Run-1_20Jan16_Pippin_15-08-53.raw";
            var cmdStr = dataFilePath + @" --filter ""peakPicking true 1-"" --filter ""threshold count 500 most-intense"" --mgf -o C:\DMS_WorkDir";

            m_RunProgTool = new clsRunDosProgram(workDir)
            {
                CreateNoWindow = true,
                CacheStandardOutput = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                ConsoleOutputFilePath = ""
                // Allow the console output filename to be auto-generated
            };
            RegisterEvents(m_RunProgTool);

            if (!m_RunProgTool.RunProgram(exePath, cmdStr, "MSConvert", true))
            {
                Console.WriteLine("Error running MSConvert");
            }
            else
            {
                Console.WriteLine("Done");
            }
        }

        /// <summary>
        /// Archive a Sequest parameter ifle by copying to \\gigasax\dms_parameter_Files\Sequest
        /// </summary>
        public void TestArchiveFileStart()
        {
            var strParamFilePath = @"D:\Temp\sequest_N14_NE.params";
            var strTargetFolderPath = @"\\gigasax\dms_parameter_Files\Sequest";

            TestArchiveFile(strParamFilePath, strTargetFolderPath);

            // TestArchiveFile(@"\\n2.emsl.pnl.gov\dmsarch\LCQ_1\LCQ_C1\DR026_dialyzed_7june00_a\Seq200104201154_Auto1001\sequest_Tryp_4_IC.params", strTargetFolderPath)
            // TestArchiveFile(@"\\proto-4\C1_DMS1\DR026_dialyzed_7june00_a\Seq200104201154_Auto1001\sequest_Tryp_4_IC.params", strTargetFolderPath)

            Console.WriteLine("Done syncing files");
        }

        private void TestArchiveFile(string strSrcFilePath, string strTargetFolderPath)
        {
            try
            {
                var lstLineIgnoreRegExSpecs = new List<Regex> {
                    new Regex("mass_type_parent *=.*", RegexOptions.Compiled | RegexOptions.IgnoreCase)
                };

                var blnNeedToArchiveFile = false;

                var fileName = Path.GetFileName(strSrcFilePath);
                if (fileName == null)
                {
                    Console.WriteLine("Filename could not be parsed from " + strSrcFilePath);
                    return;
                }

                var strTargetFilePath = Path.Combine(strTargetFolderPath, fileName);

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
                            strNewName = strNewNameBase + "_v" + intRevisionNumber + Path.GetExtension(strTargetFilePath);
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

        /// <summary>
        /// Split apart a _dta.txt file
        /// </summary>
        /// <param name="rootFileName"></param>
        /// <param name="strResultsFolder"></param>
        public void TestUncat(string rootFileName, string strResultsFolder)
        {
            Console.WriteLine("Splitting concatenated DTA file");

            var FileSplitter = new clsSplitCattedFiles();
            FileSplitter.SplitCattedDTAsOnly(rootFileName, strResultsFolder);

            Console.WriteLine("Completed splitting concatenated DTA file");
        }

        /// <summary>
        /// Instantiate an instance of clsAnalysisToolRunnerDtaSplit
        /// </summary>
        public void TestDTASplit()
        {

            const int intDebugLevel = 2;

            var objJobParams = InitializeMgrAndJobParams(intDebugLevel);

            var statusTools = new clsStatusFile("Status.xml", intDebugLevel);
            RegisterEvents(statusTools);

            var myEMSLUtilities = new clsMyEMSLUtilities(intDebugLevel, WORKING_DIRECTORY);
            RegisterEvents(myEMSLUtilities);

            objJobParams.SetParam("JobParameters", "DatasetNum", "QC_05_2_05Dec05_Doc_0508-08");
            objJobParams.SetParam("JobParameters", "NumberOfClonedSteps", "25");
            objJobParams.SetParam("JobParameters", "ClonedStepsHaveEqualNumSpectra", "True");

            var fiMgr = new FileInfo(System.Windows.Forms.Application.ExecutablePath);
            var mgrFolderPath = fiMgr.DirectoryName;

            var summaryFile = new clsSummaryFile();
            summaryFile.Clear();

            var pluginLoader = new clsPluginLoader(summaryFile, mgrFolderPath);

            var objToolRunner = pluginLoader.GetToolRunner("dta_split".ToLower());
            objToolRunner.Setup(m_mgrParams, objJobParams, statusTools, summaryFile, myEMSLUtilities);
            objToolRunner.RunTool();

        }

        /// <summary>
        /// Test creation of a .fasta file from a protein collection
        /// Also calls Running BuildSA
        /// </summary>
        /// <param name="destFolder"></param>
        /// <returns></returns>
        public bool TestProteinDBExport(string destFolder)
        {

            // Test what the Protein_Exporter does if a protein collection name is truncated (and thus invalid)
            var strProteinCollectionList = "Geobacter_bemidjiensis_Bem_T_2006-10-10,Geobacter_lovelyi_SZ_2007-06-19,Geobacter_metallireducens_GS-15_2007-10-02,Geobacter_sp_";
            var strProteinOptions = "seq_direction=forward,filetype=fasta";

            // Test a 34 MB fasta file
            strProteinCollectionList = "nr_ribosomal_2010-08-17,Tryp_Pig";
            strProteinOptions = "seq_direction=forward,filetype=fasta";

            // Test 100 MB fasta file
            // strLegacyFasta = "na"
            // strProteinCollectionList = "GWB1_Rifle_2011_9_13_0_1_2013-03-27,Tryp_Pig_Bov"
            // strProteinOptions = "seq_direction=forward,filetype=fasta"

            var blnSuccess = TestProteinDBExport(destFolder, "na", strProteinCollectionList, strProteinOptions);

            if (blnSuccess)
            {
                IJobParams oJobParams = InitializeManagerParams();
                oJobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", m_FastaFileName);

                //const bool blnMsgfPlus = true;
                //var strJobNum = "12345";
                //var intDebugLevel = Convert.ToInt16(m_mgrParams.GetParam("debuglevel", 1));

                //var JavaProgLoc = @"C:\Program Files\Java\jre8\bin\java.exe";
                //var MSGFDbProgLoc = @"C:\DMS_Programs\MSGFDB\MSGFPlus.jar";
                //bool fastaFileIsDecoy;
                //string fastaFilePath;

                // Uncomment the following if the MSGFDB plugin is associated with the solution
                //var oTool = new AnalysisManagerMSGFDBPlugIn.clsMSGFDBUtils(
                //    m_mgrParams, oJobParams, strJobNum, m_mgrParams.GetParam("workdir"), intDebugLevel, blnMsgfPlus);

                //RegisterEvents(oTool);

                //float fastaFileSizeKB;

                //// Note that fastaFilePath will be populated by this function call
                //var eResult = oTool.InitializeFastaFile(JavaProgLoc, MSGFDbProgLoc, out fastaFileSizeKB, out fastaFileIsDecoy, out fastaFilePath);
            }

            return blnSuccess;
        }

        /// <summary>
        /// Test creation of a .fasta file from a protein collection
        /// </summary>
        /// <param name="destFolder"></param>
        /// <param name="strLegacyFasta"></param>
        /// <param name="strProteinCollectionList"></param>
        /// <param name="strProteinOptions"></param>
        /// <returns></returns>
        public bool TestProteinDBExport(string destFolder, string strLegacyFasta, string strProteinCollectionList, string strProteinOptions)
        {
            // Instantiate fasta tool if not already done
            if (m_FastaTools == null)
            {
                if (string.IsNullOrEmpty(m_FastaToolsCnStr))
                {
                    Console.WriteLine("Protein database connection string not specified");
                    return false;
                }
                m_FastaTools = new Protein_Exporter.clsGetFASTAFromDMS(m_FastaToolsCnStr);
                m_FastaTools.FileGenerationStarted += m_FastaTools_FileGenerationStarted;
                m_FastaTools.FileGenerationCompleted += m_FastaTools_FileGenerationCompleted;
                m_FastaTools.FileGenerationProgress += m_FastaTools_FileGenerationProgress;

            }

            // Initialize fasta generation state variables
            m_GenerationComplete = false;

            // Setup a timer to prevent an infinite loop if there's a fasta generation problem
            m_FastaTimer = new System.Timers.Timer();
            m_FastaTimer.Elapsed += m_FastaTimer_Elapsed;

            m_FastaTimer.Interval = FASTA_GEN_TIMEOUT_INTERVAL_SEC * 1000;
            m_FastaTimer.AutoReset = false;

            // Create the fasta file
            m_FastaGenTimeOut = false;
            try
            {
                m_FastaTimer.Start();
                var hashString = m_FastaTools.ExportFASTAFile(strProteinCollectionList, strProteinOptions, strLegacyFasta, destFolder);
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
                // Fasta generator hung - report error and exit
                Console.WriteLine("Timeout error while generating OrdDb file (" + FASTA_GEN_TIMEOUT_INTERVAL_SEC + " seconds have elapsed)");
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
            var OutFileName = "MyTestDataset_out.txt";

            clsAnalysisJob objJobParams;
            clsMyEMSLUtilities myEMSLUtilities;

            var objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            objJobParams.AddResultFileToSkip(OutFileName);

            objToolRunner.RunTool();
        }

        /// <summary>
        /// Test copying results
        /// </summary>
        public void TestDeliverResults()
        {

            clsAnalysisJob objJobParams;
            clsMyEMSLUtilities myEMSLUtilities;

            var objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            objJobParams.SetParam("StepParameters", "OutputFolderName", "Test_Results_" + DateTime.Now.ToString("hh_mm_ss"));
            objJobParams.SetParam("JobParameters", "transferFolderPath", @"\\proto-3\DMS3_XFER");
            objJobParams.SetParam("JobParameters", "DatasetNum", "Test_Dataset");

            objToolRunner.RunTool();
        }

        /// <summary>
        /// Check date formatting
        /// </summary>
        public void TestFileDateConversion()
        {
            var objTargetFile = new FileInfo(@"D:\JobSteps.png");

            var strDate = objTargetFile.LastWriteTime.ToString(CultureInfo.InvariantCulture);

            var ResultFiles = Directory.GetFiles(@"C:\Temp\", "*");

            foreach (var FileToCopy in ResultFiles)
            {
                Console.WriteLine(FileToCopy);
            }

            Console.WriteLine(strDate);
        }

        /// <summary>
        /// Create a log file
        /// </summary>
        public void TestLogging()
        {
            var logFileNameBase = @"Logs\AnalysisMgr";

            clsLogTools.CreateFileLogger(logFileNameBase);

            clsAnalysisJob objJobParams;
            clsMyEMSLUtilities myEMSLUtilities;

            var objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            m_DebugLevel = 2;
            objJobParams.DebugLevel = m_DebugLevel;

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

            LogMessage("Testing complete");
        }

        /// <summary>
        /// Determine the size of a legacy FASTA file
        /// </summary>
        public void GetLegacyFastaFileSize()
        {
            var objJobParams = new clsAnalysisJob(m_mgrParams, 0);

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

            string legacyFastaName;

            if (proteinCollectionInfo.UsingSplitFasta)
            {
                string errorMessage;
                legacyFastaName = clsAnalysisResources.GetSplitFastaFileName(objJobParams, out errorMessage);
            }
            else
            {
                legacyFastaName = proteinCollectionInfo.LegacyFastaName;
            }

            Console.WriteLine(legacyFastaName + " requires roughly " + spaceRequiredMB.ToString("#,##0") + " MB");
        }

        /// <summary>
        /// Run a test query
        /// </summary>
        public void TestRunQuery()
        {
            const string sqlStr = "Select top 50 * from t_log_entries";

            const string connectionString = "Data Source=gigasax;Initial Catalog=dms_pipeline;Integrated Security=SSPI;";
            const string callingFunction = "TestRunQuery";
            const short retryCount = 2;
            const int timeoutSeconds = 30;
            DataTable dtResults;

            clsGlobal.GetDataTableByQuery(sqlStr, connectionString, callingFunction, retryCount, out dtResults, timeoutSeconds);

            foreach (DataRow row in dtResults.Rows)
            {
                Console.WriteLine(row[0] + ": " + row[1]);
            }
        }

        /// <summary>
        /// Call a stored procedure
        /// </summary>
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
            DataTable dtResults;

            clsGlobal.GetDataTableByCmd(cmd, connectionString, callingFunction, retryCount, out dtResults, timeoutSeconds);

            foreach (DataRow row in dtResults.Rows)
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
            const string workDir = @"C:\DMS_WorkDir";

            var ionicZipTools = new clsIonicZipTools(debugLevel, workDir);
            RegisterEvents(ionicZipTools);

            ionicZipTools.UnzipFile(zipFilePath);

            var diWorkDir = new DirectoryInfo(workDir);
            foreach (var fiFile in diWorkDir.GetFiles("*.mzid"))
            {
                ionicZipTools.GZipFile(fiFile.FullName, true);
            }
        }

        /// <summary>
        /// Test creating and decompressing .gz files using gzip
        /// </summary>
        public void TestGZip()
        {
            clsAnalysisJob objJobParams;
            clsMyEMSLUtilities myEMSLUtilities;

            var objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            const string sourceFilePath = @"F:\Temp\ZipTest\QExact01\UDD-1_27Feb13_Gimli_12-07-03_HCD.mgf";

            objToolRunner.GZipFile(sourceFilePath, @"F:\Temp\ZipTest\QExact01\GZipTarget", false);

            objToolRunner.GZipFile(sourceFilePath, false);

            var gzippedFile = @"F:\Temp\ZipTest\QExact01\" + Path.GetFileName(sourceFilePath) + ".gz";

            objToolRunner.GUnzipFile(gzippedFile);

            objToolRunner.GUnzipFile(gzippedFile, @"F:\Temp\ZipTest\GUnzipTarget");
        }

        /// <summary>
        /// Test unzipping a file
        /// </summary>
        /// <remarks>This uses ionic zip</remarks>
        public bool TestUnzip(string strZipFilePath, string strOutFolderPath)
        {
            var intDebugLevel = 2;

            var objResources = GetResourcesObject(intDebugLevel);

            var blnSuccess = objResources.UnzipFileStart(strZipFilePath, strOutFolderPath, "TestUnzip", false);
            // blnSuccess = objResources.UnzipFileStart(strZipFilePath, strOutFolderPath, "TestUnzip", True)

            return blnSuccess;
        }

        /// <summary>
        /// Test zipping a file
        /// </summary>
        /// <remarks>This uses ionic zip</remarks>
        public void TestZip()
        {
            clsAnalysisJob objJobParams;
            clsMyEMSLUtilities myEMSLUtilities;

            var objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            const string sourceFilePath = @"F:\Temp\ZipTest\QExact01\UDD-1_27Feb13_Gimli_12-07-03_HCD.mgf";

            objToolRunner.ZipFile(sourceFilePath, false);

            var zippedFile = @"F:\Temp\ZipTest\QExact01\" + Path.GetFileNameWithoutExtension(sourceFilePath) + ".zip";

            objToolRunner.UnzipFile(zippedFile);

            objToolRunner.UnzipFile(zippedFile, @"F:\Temp\ZipTest\UnzipTarget");

            var ionicZipTools = new clsIonicZipTools(1, WORKING_DIRECTORY);
            RegisterEvents(ionicZipTools);

            ionicZipTools.ZipDirectory(@"F:\Temp\ZipTest\QExact01\", @"F:\Temp\ZipTest\QExact01_Folder.zip");
        }

        /// <summary>
        /// Test Ionic zip
        /// </summary>
        public void TestIonicZipTools()
        {
            var ionicZipTools = new clsIonicZipTools(1, @"C:\DMS_WorkDir");
            RegisterEvents(ionicZipTools);

            ionicZipTools.UnzipFile(@"C:\DMS_WorkDir\Temp.zip", @"C:\DMS_WorkDir", "*.png");
            foreach (var item in ionicZipTools.MostRecentUnzippedFiles)
            {
                Console.WriteLine(item.Key + " - " + item.Value);
            }
        }

        /// <summary>
        /// Retrieve and decompress MALDI data
        /// </summary>
        /// <param name="strSourceDatasetFolder"></param>
        /// <returns></returns>
        public bool TestMALDIDataUnzip(string strSourceDatasetFolder)
        {
            var intDebugLevel = 2;

            var objResources = new clsResourceTestClass();

            var statusTools = new clsStatusFile("Status.xml", intDebugLevel);
            RegisterEvents(statusTools);

            if (string.IsNullOrEmpty(strSourceDatasetFolder))
            {
                strSourceDatasetFolder = @"\\Proto-10\9T_FTICR_Imaging\2010_4\ratjoint071110_INCAS_MS";
            }

            clsAnalysisJob objJobParams;
            clsMyEMSLUtilities myEMSLUtilities;

            var objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            m_mgrParams.SetParam("ChameleonCachedDataFolder", @"H:\9T_Imaging");

            objJobParams.SetParam("JobParameters", "DatasetNum", "ratjoint071110_INCAS_MS");

            objJobParams.SetParam("JobParameters", "DatasetStoragePath", @"\\Proto-10\9T_FTICR_Imaging\2010_4\");
            objJobParams.SetParam("JobParameters", "DatasetArchivePath", @"\\adms.emsl.pnl.gov\dmsarch\9T_FTICR_Imaging_1");
            objJobParams.SetParam("JobParameters", "transferFolderPath", @"\\proto-10\DMS3_Xfer");

            objResources.Setup(m_mgrParams, objJobParams, statusTools, myEMSLUtilities);

            var blnSuccess = objResources.FileSearch.RetrieveBrukerMALDIImagingFolders(true);

            // blnSuccess = objResources.UnzipFileStart(strZipFilePath, strOutFolderPath, "TestUnzip", True)

            return blnSuccess;
        }

        /// <summary>
        /// Test zipping and unzipping with ionic zip
        /// </summary>
        public void TestZipAndUnzip()
        {
            var ionicZipTools = new clsIonicZipTools(3, @"F:\Temp");
            RegisterEvents(ionicZipTools);

            ionicZipTools.ZipFile(@"F:\Temp\Sarc_P12_D12_1104_148_8Sep11_Cheetah_11-05-34.uimf", false);

            ionicZipTools.ZipFile(@"F:\Temp\Schutzer_cf_ff_XTandem_AllProt.txt", false, @"F:\Temp\TestCustom.zip");

            ionicZipTools.ZipDirectory(@"F:\Temp\STAC", @"F:\Temp\ZippedFolderTest.zip");

            // ionicZipTools.ZipDirectory(@"F:\Temp\UnzipTest\0_R00X051Y065", @"F:\Temp\UnzipTest\0_R00X051Y065.zip", false);

            //      ionicZipTools.ZipDirectory(@"F:\Temp\UnzipTest\0_R00X051Y065", @"F:\Temp\UnzipTest\ZippedFolders2.zip", True, "*.baf*");

            //      ionicZipTools.ZipDirectory(@"F:\Temp\UnzipTest\0_R00X051Y065", @"F:\Temp\UnzipTest\ZippedFolders3.zip", True, "*.ini");

            //      ionicZipTools.UnzipFile(@"F:\temp\unziptest\StageMD5_Scratch.zip");

            //      ionicZipTools.UnzipFile(@"F:\Temp\UnzipTest\ZippedFolders.zip", @"F:\Temp\UnzipTest\Unzipped");

            //      ionicZipTools.UnzipFile(@"F:\Temp\UnzipTest\ZippedFolders.zip", @"F:\Temp\UnzipTest\Unzipped2", "*.baf*");

            //      ionicZipTools.UnzipFile(@"F:\Temp\UnzipTest\ZippedFolders.zip", @"F:\Temp\UnzipTest\Unzipped3", "*.baf*", Ionic.Zip.ExtractExistingFileAction.DoNotOverwrite);

            //      ionicZipTools.UnzipFile(@"F:\Temp\UnzipTest\ZippedFolders3.zip", @"F:\Temp\UnzipTest\Unzipped4", "*.ini", Ionic.Zip.ExtractExistingFileAction.OverwriteSilently);

            //      ionicZipTools.UnzipFile(@"F:\Temp\UnzipTest\ZippedFolders3.zip", @"F:\Temp\UnzipTest\Unzipped5", "my*.ini", Ionic.Zip.ExtractExistingFileAction.OverwriteSilently);
        }
    
        /// <summary>
        /// Generate a scan stats file
        /// </summary>
        public void GenerateScanStatsFile()
        {
            const string inputFilePath = "QC_Shew_16_01_pt5_run7_11Apr16_Tiger_16-02-05.raw";
            const string workingDir = @"C:\DMS_WorkDir";

            var success = GenerateScanStatsFile(Path.Combine(workingDir, inputFilePath), workingDir);
            Console.WriteLine("Success: " + success);
        }

        /// <summary>
        /// Generate a scan stats file
        /// </summary>
        public bool GenerateScanStatsFile(string strInputFilePath, string workingDir)
        {
            var strMSFileInfoScannerDir = @"C:\DMS_Programs\MSFileInfoScanner";

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

        private void m_FastaTools_FileGenerationStarted(string taskMsg)
        {
            // Reset the fasta generation timer
            m_FastaTimer.Start();
        }

        private void m_FastaTools_FileGenerationCompleted(string FullOutputPath)
        {
            // Get the name of the fasta file that was generated
            m_FastaFileName = Path.GetFileName(FullOutputPath);

            // Stop the fasta generation timer so no false error occurs
            m_FastaTimer?.Stop();

            // Set the completion flag
            m_GenerationComplete = true;

        }

        private void m_FastaTools_FileGenerationProgress(string statusMsg, double fractionDone)
        {
            // Reset the fasta generation timer
            m_FastaTimer.Start();
        }

        private void m_FastaTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            // If this event occurs, it means there was a hang during fasta generation and the manager will have to quit
            m_FastaTimer.Stop();

            // Stop the timer to prevent false errors
            m_FastaGenTimeOut = true;

            // Set the timeout flag so an error will be reported
            m_GenerationComplete = true;

            // Set the completion flag so the fasta generation wait loop will exit
        }

        /// <summary>
        /// Test the program runner by starting X!Tandem
        /// </summary>
        public void TestProgRunner()
        {
            var strAppPath = @"F:\My Documents\Projects\DataMining\DMS_Managers\Analysis_Manager\AM_Program\bin\XTandem\tandem.exe";

            var strWorkDir = Path.GetDirectoryName(strAppPath);

            var progRunner = new clsRunDosProgram(strWorkDir)
            {
                CacheStandardOutput = true,
                CreateNoWindow = true,
                EchoOutputToConsole = true,
                WriteConsoleOutputToFile = true,
                DebugLevel = 1,
                MonitorInterval = 1000
            };
            RegisterEvents(progRunner);

            var success = progRunner.RunProgram(strAppPath, "input.xml", "X!Tandem", false);

            if (progRunner.CacheStandardOutput & !progRunner.EchoOutputToConsole)
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
            var m_WorkDir = @"C:\DMS_WorkDir";
            var strConsoleOutputFileName = "";
            var blnWriteConsoleOutputFileRealtime = false;

            var strExePath = @"C:\DMS_Programs\IDPicker\idpQonvert.exe";
            var cmdStr = @"-MaxFDR 0.1 -ProteinDatabase C:\DMS_Temp_Org\ID_003521_89E56851.fasta -SearchScoreWeights ""msgfspecprob -1"" -OptimizeScoreWeights 1 -NormalizedSearchScores msgfspecprob -DecoyPrefix Reversed_ -dump C:\DMS_WorkDir\Malaria844_msms_29Dec11_Draco_11-10-04.pepXML";
            var strProgramDescription = "IDPQonvert";

            var progRunner = new clsRunDosProgram(m_WorkDir)
            {
                CreateNoWindow = false,
                EchoOutputToConsole = false
            };
            RegisterEvents(progRunner);

            if (string.IsNullOrEmpty(strConsoleOutputFileName) || !blnWriteConsoleOutputFileRealtime)
            {
                progRunner.CacheStandardOutput = false;
                progRunner.WriteConsoleOutputToFile = false;
            }
            else
            {
                progRunner.CacheStandardOutput = false;
                progRunner.WriteConsoleOutputToFile = true;
                progRunner.ConsoleOutputFilePath = Path.Combine(m_WorkDir, strConsoleOutputFileName);
            }

            var blnSuccess = progRunner.RunProgram(strExePath, cmdStr, strProgramDescription, true);

            Console.WriteLine(blnSuccess);
        }

        /// <summary>
        /// Test PurgeOldServerCacheFilesTest 
        /// </summary>
        public void TestMSXmlCachePurge()
        {
            clsAnalysisJob objJobParams;
            clsMyEMSLUtilities myEMSLUtilities;

            var objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            const string cacheFolderPath = @"\\proto-2\past\PurgeTest";

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
            var objExtensionsToCheck = new List<string>();

            try
            {
                objExtensionsToCheck.Add("PAR");
                objExtensionsToCheck.Add("Pek");

                var strDSNameLCase = strDatasetName.ToLower();

                var fiFolder = new DirectoryInfo(strFolderPath);

                if (!fiFolder.Exists)
                {
                    LogError("Folder no tfound: " + strFolderPath);
                    return;
                }

                foreach (var strExtension in objExtensionsToCheck)
                {
                    foreach (var fiFile in fiFolder.GetFiles("*." + strExtension))
                    {
                        if (!fiFile.Name.ToLower().StartsWith(strDSNameLCase))
                            continue;

                        var strDesiredName = strDatasetName + "_" + DateTime.Now.ToString("M_d_yyyy") + "." + strExtension;

                        if (fiFile.Name.ToLower() != strDesiredName.ToLower())
                        {
                            try
                            {
                                fiFile.MoveTo(Path.Combine(fiFolder.FullName, strDesiredName));
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

            var sngFreeMemoryMB = clsGlobal.GetFreeMemoryMB();

            Console.WriteLine();
            Console.WriteLine("Available memory (MB) = " + sngFreeMemoryMB.ToString("0.0"));

        }

        /// <summary>
        /// Read the contents file TestInputFile
        /// </summary>
        public void TestGetFileContents()
        {
            var strFilePath = "TestInputFile.txt";
            var strContents = GetFileContents(strFilePath);

            Console.WriteLine(strContents);
        }

        private string GetFileContents(string filePath)
        {
            var fi = new FileInfo(filePath);

            var tr = new StreamReader(new FileStream(fi.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

            var s = tr.ReadToEnd();

            if (string.IsNullOrEmpty(s))
            {
                return string.Empty;
            }

            return s;
        }

        /// <summary>
        /// Get the version info of several DLLs
        /// </summary>
        public void TestGetVersionInfo()
        {
            clsAnalysisJob objJobParams;
            clsMyEMSLUtilities myEMSLUtilities;

            var objToolRunner = GetCodeTestToolRunner(out objJobParams, out myEMSLUtilities);

            var pathToTestx86 = @"F:\My Documents\Projects\DataMining\DMS_Programs\DLLVersionInspector\bin\32bit_Dll_Examples\UIMFLibrary.dll";
            var pathToTestx64 = @"F:\My Documents\Projects\DataMining\DMS_Programs\DLLVersionInspector\bin\64bit_Dll_Examples\UIMFLibrary.dll";
            var pathToTestAnyCPU = @"F:\My Documents\Projects\DataMining\DMS_Programs\DLLVersionInspector\bin\AnyCPU_DLL_Examples\UIMFLibrary.dll";

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

        /// <summary>
        /// Test RemoveSparseSpectra
        /// </summary>
        public void RemoveSparseSpectra()
        {
            var cDtaUtilities = new clsCDTAUtilities();
            RegisterEvents(cDtaUtilities);

            cDtaUtilities.RemoveSparseSpectra(@"C:\DMS_WorkDir", "ALZ_VP2P101_C_SCX_02_7Dec08_Draco_08-10-29_dta.txt");
        }

        /// <summary>
        /// Test ValidateCDTAFileIsCentroided
        /// </summary>
        public void ValidateCentroided()
        {
            const int intDebugLevel = 2;

            var objResources = GetResourcesObject(intDebugLevel);

            objResources.ValidateCDTAFileIsCentroided(@"\\proto-7\dms3_Xfer\UW_HCV_03_Run2_19Dec13_Pippin_13-07-06\DTA_Gen_1_26_350136\UW_HCV_03_Run2_19Dec13_Pippin_13-07-06_dta.txt");
        }

        private class clsResourceTestClass : clsAnalysisResources
        {
            public override CloseOutType GetResources()
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
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
