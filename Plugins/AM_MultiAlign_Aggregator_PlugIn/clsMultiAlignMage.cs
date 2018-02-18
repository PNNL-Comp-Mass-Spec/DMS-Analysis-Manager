using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase;
using Mage;
using MageExtExtractionFilters;
using PRISM;
using PRISM.Logging;

namespace AnalysisManagerMultiAlign_AggregatorPlugIn
{

    public class clsMultiAlignMage : clsEventNotifier
    {

        #region Member Variables

        private string mResultsDBFileName = "";
        private string mWorkingDir;
        private JobParameters mJP;
        private ManagerParameters mMP;
        private string mMessage = "";
        private string mSearchType = "";								// File extension of input data files, e.g. "_LCMSFeatures.txt" or "_isos.csv"
        private string mParamFilename = "";
        private const string MULTIALIGN_INPUT_FILE = "Input.txt";
        private string mJobNum = "";
        private short mDebugLevel;
        private float mProgress;
        private string mMultialignErrorMessage = "";
        private readonly IStatusFile mStatusTools;

        private DateTime mLastStatusUpdate = DateTime.UtcNow;
        private DateTime mLastMultialignLogFileParse = DateTime.UtcNow;
        private DateTime mLastProgressWriteTime = DateTime.UtcNow;

        private SortedDictionary<eProgressSteps, Int16> mProgressStepPercentComplete;
        // This dictionary associates key log text entries with the corresponding progress step for each
        // It is populated by sub InitializeProgressStepDictionaries

        private SortedDictionary<string, eProgressSteps> mProgressStepLogText;
        private enum eProgressSteps
        {
            Starting = 0,
            LoadingMTDB = 1,
            LoadingDatasets = 2,
            LinkingMSFeatures = 3,
            AligningDatasets = 4,
            PerformingClustering = 5,
            PerformingPeakMatching = 6,
            CreatingFinalPlots = 7,
            CreatingReport = 8,
            Complete = 9
        }

        private string m_MultialignErroMessage = string.Empty;

        #endregion

        #region Properties

        public string Message => string.IsNullOrEmpty(mMessage) ? string.Empty : mMessage;

        #endregion

        #region Constructors

        public clsMultiAlignMage(IJobParams jobParms, IMgrParams mgrParms, IStatusFile statusTools)
        {
            mStatusTools = statusTools;
            Intialize(jobParms, mgrParms);
        }

        #endregion

        #region Initialization

        /// <summary>
        /// Set up internal variables
        /// </summary>
        /// <param name="jobParms"></param>
        /// <param name="mgrParms"></param>
        private void Intialize(IJobParams jobParms, IMgrParams mgrParms)
        {
            mJP = new JobParameters(jobParms);
            mMP = new ManagerParameters(mgrParms);

            mResultsDBFileName = mJP.RequireJobParam("ResultsBaseName", "Results") + ".db3";
            mWorkingDir = mMP.RequireMgrParam("workdir");
            mSearchType = mJP.RequireJobParam("MultiAlignSearchType");					// File extension of input data files, can be "_LCMSFeatures.txt" or "_isos.csv"
            mParamFilename = mJP.RequireJobParam("ParmFileName");
            mDebugLevel = Convert.ToInt16(mMP.RequireMgrParam("debuglevel"));
            mJobNum = mJP.RequireJobParam("Job");
        }

        #endregion

        #region Processing

        /// <summary>
        /// Do processing
        /// </summary>
        /// <returns>True if success; otherwise false</returns>
        public bool Run(string sMultiAlignConsolePath)
        {
            var dataPackageID = mJP.RequireJobParam("DataPackageID");

            var blnSuccess = GetMultiAlignParameterFile();
            if (!blnSuccess)
                return false;

            var multialignJobsToProcess = GetListOfDataPackageJobsToProcess(dataPackageID, "Decon2LS_V2");

            if (multialignJobsToProcess.Rows.Count == 0)
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Data package " + dataPackageID + " does not have any Decon2LS_V2 analysis jobs";
                    OnErrorEvent(mMessage);
                }
                return false;
            }

            blnSuccess = CopyMultiAlignInputFiles(multialignJobsToProcess, mSearchType);
            if (!blnSuccess)
                return false;

            blnSuccess = BuildMultiAlignInputTextFile(mSearchType);
            if (!blnSuccess)
                return false;

            InitializeProgressStepDictionaries();

            var cmdRunner = new clsRunDosProgram(mWorkingDir);
            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;
            cmdRunner.ErrorEvent += CmdRunnerOnErrorEvent;

            if (mDebugLevel > 4)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "clsMultiAlignMage.RunTool(): Enter");
            }

            if (string.IsNullOrWhiteSpace(sMultiAlignConsolePath))
            {
                mMessage = "MultiAlignConsolePath is empty";
                return false;
            }

            if (!File.Exists(sMultiAlignConsolePath))
            {
                mMessage = "MultiAlign program not found: " + sMultiAlignConsolePath;
                return false;
            }

            var MultiAlignResultFilename = mJP.GetJobParam("ResultsBaseName");

            if (string.IsNullOrWhiteSpace(MultiAlignResultFilename))
            {
                MultiAlignResultFilename = mJP.RequireJobParam(clsAnalysisResources.JOB_PARAM_DATASET_NAME);
            }

            // Set up and execute a program runner to run MultiAlign
            var cmdStr = " -files " + MULTIALIGN_INPUT_FILE + " -params " + Path.Combine(mWorkingDir, mParamFilename) + " -path " + mWorkingDir + " -name " + mResultsDBFileName + " -plots";
            if (mDebugLevel >= 1)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, sMultiAlignConsolePath + " " + cmdStr);
            }

            cmdRunner.CreateNoWindow = true;
            cmdRunner.CacheStandardOutput = true;
            cmdRunner.EchoOutputToConsole = true;
            cmdRunner.WriteConsoleOutputToFile = false;

            if (!cmdRunner.RunProgram(sMultiAlignConsolePath, cmdStr, "MultiAlign", true))
            {
                if (string.IsNullOrEmpty(mMessage))
                    mMessage = "Error running MultiAlign";

                if (!string.IsNullOrEmpty(m_MultialignErroMessage))
                {
                    mMessage += ": " + mMultialignErrorMessage;
                }
                OnErrorEvent(mMessage + ", job " + mJobNum);

                return false;
            }

            return true;
        }

        #endregion


        #region Mage Pipelines and Utilities

        private bool GetMultiAlignParameterFile()
        {

            try
            {
                // Retrieve the MultiAlign Parameter .xml file specified for this job
                if (string.IsNullOrEmpty(mParamFilename))
                {
                    mMessage = "Job parameter ParmFileName not defined in the settings for this MultiAlign job; unable to continue";
                    OnErrorEvent(mMessage);
                    return false;
                }

                var strParamFileStoragePathKeyName = clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "MultiAlign";
                var strMAParameterFileStoragePath = mMP.RequireMgrParam(strParamFileStoragePathKeyName);
                if (string.IsNullOrEmpty(strMAParameterFileStoragePath))
                {
                    strMAParameterFileStoragePath = @"\\gigasax\DMS_Parameter_Files\MultiAlign";
                    OnWarningEvent("Parameter " + strParamFileStoragePathKeyName +
                        " is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " + strMAParameterFileStoragePath);
                }

                var sourceFilePath = Path.Combine(strMAParameterFileStoragePath, mParamFilename);

                if (!File.Exists(sourceFilePath))
                {
                    mMessage = "MultiAlign parameter file not found: " + strMAParameterFileStoragePath;
                    OnErrorEvent(mMessage);
                    return false;
                }
                File.Copy(sourceFilePath, Path.Combine(mWorkingDir, mParamFilename), true);
            }
            catch (Exception ex)
            {
                mMessage = "Error copying the MultiAlign parameter file";
                OnErrorEvent(mMessage + ": " + ex.Message);
                return false;
            }


            return true;

        }

        /// <summary>
        /// query DMS to get list of data package jobs to process
        /// </summary>
        /// <param name="dataPackageID"></param>
        /// <param name="tool"></param>
        /// <returns></returns>
        private SimpleSink GetListOfDataPackageJobsToProcess(string dataPackageID, string tool)
        {
            var sqlTemplate = @"SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0} AND Tool LIKE '%{1}%'";
            var connStr = mMP.RequireMgrParam("ConnectionString");
            var sql = string.Format(sqlTemplate, dataPackageID, tool);
            var jobList = GetListOfItemsFromDB(sql, connStr);

            if (jobList.Rows.Count == 0)
            {
                mMessage = "Data package " + dataPackageID + " does not have any " + tool + " analysis jobs";
                OnErrorEvent(mMessage + " using query " + sqlTemplate);
            }

            return jobList;
        }

        /// <summary>
        /// Copy MultiAlign input files into working folder from given list of jobs
        /// Looks for DeconTools _LCMSFeatures.txt or _isos.csv files for the given jobs
        /// </summary>
        /// <param name="multialignJobsToProcess"></param>
        /// <param name="fileSpec"></param>
        private bool CopyMultiAlignInputFiles(SimpleSink multialignJobsToProcess, string fileSpec)
        {

            try
            {

                var columnsToIncludeInOutput = "Job, Dataset, Dataset_ID, Tool, Settings_File, Parameter_File, Instrument";
                var fileList = GetListOfFilesFromFolderList(multialignJobsToProcess, fileSpec, columnsToIncludeInOutput);

                // Check for "--No Files Found--" for any of the jobs
                foreach (var row in fileList.Rows)
                {
                    if (row.Length > 4)
                    {
                        if (row[1] == BaseModule.kNoFilesFound)
                        {
                            mMessage = "Did not find any " + fileSpec + " files for job " + row[4] + " at " + row[3];
                            OnErrorEvent(mMessage);
                            return false;
                        }
                    }
                }

                // make module to copy file(s) from server to working directory
                var copier = new FileCopy
                {
                    OutputFolderPath = mWorkingDir,
                    SourceFileColumnName = "Name"
                };

                ProcessingPipeline.Assemble("File_Copy", fileList, copier).RunRoot(null);

            }
            catch (Exception ex)
            {
                mMessage = "Error copying DeconTools result files";
                OnErrorEvent(mMessage + ": " + ex.Message);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Get a list of items using a database query
        /// </summary>
        /// <param name="sql">Query to use a source of jobs</param>
        /// <param name="connectionString"></param>
        /// <returns>A Mage module containing list of jobs</returns>
        public static SimpleSink GetListOfItemsFromDB(string sql, string connectionString)
        {
            var itemList = new SimpleSink();
            var reader = MakeDBReaderModule(sql, connectionString);
            var pipeline = ProcessingPipeline.Assemble("Get Items", reader, itemList);
            pipeline.RunRoot(null);
            return itemList;
        }

        /// <summary>
        /// Create a new MSSQLReader module to do a specific query
        /// </summary>
        /// <param name="sql">Query to use</param>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        public static MSSQLReader MakeDBReaderModule(string sql, string connectionString)
        {
            var reader = new MSSQLReader(connectionString)
            {
                SQLText = sql
            };
            return reader;
        }

        /// <summary>
        /// Get list of selected files from list of folders
        /// </summary>
        /// <param name="folderListSource">Mage object that contains list of folders</param>
        /// <param name="fileNameSelector">File name selector to select files to be included in output list</param>
        /// <param name="passThroughColumns">List of columns from source object to pass through to output list object</param>
        /// <returns>Mage object containing list of files</returns>
        public SimpleSink GetListOfFilesFromFolderList(IBaseModule folderListSource, string fileNameSelector, string passThroughColumns)
        {
            var sinkObject = new SimpleSink();

            // create file filter module and initialize it
            var fileFilter = new FileListFilter
            {
                FileNameSelector = fileNameSelector,
                SourceFolderColumnName = "Folder",
                FileColumnName = "Name",
                OutputColumnList = "Item|+|text, Name|+|text, File_Size_KB|+|text, Folder, " + passThroughColumns,
                FileSelectorMode = "RegEx",
                IncludeFilesOrFolders = "File",
                RecursiveSearch = "No",
                SubfolderSearchName = "*"
            };

            // build, wire, and run pipeline
            ProcessingPipeline.Assemble("FileListPipeline", folderListSource, fileFilter, sinkObject).RunRoot(null);
            return sinkObject;
        }

        /// <summary>
        /// Build the MultiAlign input file
        /// </summary>
        /// <param name="strInputFileExtension"></param>
        /// <returns></returns>
        private bool BuildMultiAlignInputTextFile(string strInputFileExtension)
        {

            const string INPUT_FILENAME = "input.txt";

            var TargetFilePath = Path.Combine(mWorkingDir, INPUT_FILENAME);

            // Create the MultiAlign input file

            try
            {

                var Files = Directory.GetFiles(mWorkingDir, "*" + strInputFileExtension);

                if (Files.Length == 0)
                {
                    mMessage = "Did not find any files of type " + strInputFileExtension + " in folder " + mWorkingDir;
                    OnErrorEvent(mMessage);
                    return false;
                }

                using (var swOutFile = new StreamWriter(new FileStream(TargetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swOutFile.WriteLine("[Files]");
                    var AlignmentDataset = mJP.GetJobParam("AlignmentDataset");
                    foreach (var TmpFile_loopVariable in Files)
                    {
                        var TmpFile = TmpFile_loopVariable;
                        if (!string.IsNullOrWhiteSpace(AlignmentDataset) && TmpFile.ToLower().Contains(AlignmentDataset.ToLower()))
                        {
                            // Append an asterisk to this dataset's path to indicate that it is the base dataset to which the others will be aligned
                            swOutFile.WriteLine(TmpFile + "*");
                        }
                        else
                        {
                            swOutFile.WriteLine(TmpFile);
                        }
                    }

                    // Check to see if a mass tag database has been defined and NO alignment dataset has been defined
                    var AmtDb = mJP.GetJobParam("AMTDB");
                    if (!string.IsNullOrEmpty(AmtDb.Trim()))
                    {
                        swOutFile.WriteLine("[Database]");
                        swOutFile.WriteLine("Database = " + mJP.GetJobParam("AMTDB"));			// For example, MT_Human_Sarcopenia_MixedLC_P692
                        swOutFile.WriteLine("Server = " + mJP.GetJobParam("AMTDBServer"));		// For example, Elmer
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                mMessage = "Error building the input .txt file (" + INPUT_FILENAME + ")";
                OnErrorEvent(mMessage + ": " + ex.Message);
                return false;
            }

        }

        #endregion

        // ------------------------------------------------------------------------------

        #region Command Runner Code


        private void CmdRunnerOnErrorEvent(string strMessage, Exception ex)
        {
            OnErrorEvent(strMessage, ex);
        }

        /// <summary>
        /// Event handler for CmdRunner.LoopWaiting event
        /// </summary>
        private void CmdRunner_LoopWaiting()
        {

            // Update the status file (limit the updates to every 5 seconds)
            if (DateTime.UtcNow.Subtract(mLastStatusUpdate).TotalSeconds >= 5)
            {
                mLastStatusUpdate = DateTime.UtcNow;
                mStatusTools.UpdateAndWrite(EnumMgrStatus.RUNNING, EnumTaskStatus.RUNNING, EnumTaskStatusDetail.RUNNING_TOOL, mProgress);
            }

            if (DateTime.UtcNow.Subtract(mLastMultialignLogFileParse).TotalSeconds >= 15)
            {
                mLastMultialignLogFileParse = DateTime.UtcNow;
                ParseMultiAlignLogFile();
            }
        }

        /// <summary>
        /// Populates dictionary mProgressStepPercentComplete(), which is used by ParseMultiAlignLogFile
        /// </summary>
        /// <remarks></remarks>
        private void InitializeProgressStepDictionaries()
        {
            mProgressStepPercentComplete = new SortedDictionary<eProgressSteps, Int16>
            {
                {eProgressSteps.Starting, 5},
                {eProgressSteps.LoadingMTDB, 6},
                {eProgressSteps.LoadingDatasets, 7},
                {eProgressSteps.LinkingMSFeatures, 45},
                {eProgressSteps.AligningDatasets, 50},
                {eProgressSteps.PerformingClustering, 75},
                {eProgressSteps.PerformingPeakMatching, 85},
                {eProgressSteps.CreatingFinalPlots, 90},
                {eProgressSteps.CreatingReport, 95},
                {eProgressSteps.Complete, 97}
            };


            mProgressStepLogText = new SortedDictionary<string, eProgressSteps>
            {
                {"[LogStart]", eProgressSteps.Starting},
                {" - Loading Mass Tag database from database", eProgressSteps.LoadingMTDB},
                {" - Loading dataset data files", eProgressSteps.LoadingDatasets},
                {" - Linking MS Features", eProgressSteps.LinkingMSFeatures},
                {" - Aligning datasets", eProgressSteps.AligningDatasets},
                {" - Performing clustering", eProgressSteps.PerformingClustering},
                {" - Performing Peak Matching", eProgressSteps.PerformingPeakMatching},
                {" - Creating Final Plots", eProgressSteps.CreatingFinalPlots},
                {" - Creating report", eProgressSteps.CreatingReport},
                {" - Analysis Complete", eProgressSteps.Complete}
            };

        }

        /// <summary>
        /// Parse the MultiAlign log file to track the search progress
        /// Looks in the work directory to auto-determine the log file name
        /// </summary>
        /// <remarks></remarks>
        private void ParseMultiAlignLogFile()
        {
            var strLogFilePath = string.Empty;

            try
            {
                var diWorkDirectory = new DirectoryInfo(mWorkingDir);
                var fiFiles = diWorkDirectory.GetFiles("*-log*.txt");

                if (fiFiles.Length >= 1)
                {
                    strLogFilePath = fiFiles[0].FullName;

                    if (fiFiles.Length > 1)
                    {
                        // Use the newest file in fiFiles
                        var intBestIndex = 0;

                        for (var intIndex = 1; intIndex <= fiFiles.Length - 1; intIndex++)
                        {
                            if (fiFiles[intIndex].LastWriteTimeUtc > fiFiles[intBestIndex].LastWriteTimeUtc)
                            {
                                intBestIndex = intIndex;
                            }
                        }

                        strLogFilePath = fiFiles[intBestIndex].FullName;
                    }

                }

                if (!string.IsNullOrWhiteSpace(strLogFilePath))
                {
                    ParseMultiAlignLogFile(strLogFilePath);
                }

            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 1)
                {
                    OnErrorEvent("Error finding the MultiAlign log file at " + mWorkingDir + ": " + ex.Message);
                }

            }

        }

        /// <summary>
        /// Parse the MultiAlign log file to track the search progress
        /// </summary>
        /// <param name="strLogFilePath">Full path to the log file</param>
        /// <remarks></remarks>
        private void ParseMultiAlignLogFile(string strLogFilePath)
        {

            // The MultiAlign log file is quite big, but we can keep track of progress by looking for known text in the log file lines
            // Dictionary mProgressStepLogText keeps track of the lines of text to match while mProgressStepPercentComplete keeps track of the % complete values to use

            // For certain long-running steps we can compute a more precise version of % complete by keeping track of the number of datasets processed

            // var reExtractPercentFinished = new Regex(@"(\d+)% finished", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            try
            {
                if (!File.Exists(strLogFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "MultiAlign log file not found: " + strLogFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 4)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Parsing file " + strLogFilePath);
                }


                var eProgress = eProgressSteps.Starting;

                var intTotalDatasets = 0;
                var intDatasetsLoaded = 0;
                var intDatasetsAligned = 0;
                var intChargeStatesClustered = 0;

                // Open the file for read; don't lock it (to thus allow MultiAlign to still write to it)
                using (var srInFile = new StreamReader(new FileStream(strLogFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream)
                    {
                        var strLineIn = srInFile.ReadLine();

                        if (!string.IsNullOrWhiteSpace(strLineIn))
                        {
                            var blnMatchFound = false;

                            // Update progress if the line contains any of the entries in mProgressStepLogText
                            foreach (var lstItem in mProgressStepLogText)
                            {
                                if (strLineIn.Contains(lstItem.Key))
                                {
                                    if (eProgress < lstItem.Value)
                                    {
                                        eProgress = lstItem.Value;
                                    }
                                    blnMatchFound = true;
                                    break;
                                }
                            }

                            if (!blnMatchFound)
                            {
                                if (strLineIn.Contains("Dataset Information: "))
                                {
                                    intTotalDatasets += 1;
                                }
                                else if (strLineIn.Contains("- Adding features to cache database"))
                                {
                                    intDatasetsLoaded += 1;
                                }
                                else if (strLineIn.Contains("- Features Aligned -"))
                                {
                                    intDatasetsAligned += 1;
                                }
                                else if (strLineIn.Contains("- Clustering Charge State"))
                                {
                                    intChargeStatesClustered += 1;
                                }
                                else if (strLineIn.Contains("No baseline dataset or database was selected"))
                                {
                                    m_MultialignErroMessage = "No baseline dataset or database was selected";
                                }
                            }

                        }
                    }

                }

                // Compute the actual progress
                short intActualProgress;

                if (mProgressStepPercentComplete.TryGetValue(eProgress, out intActualProgress))
                {
                    float sngActualProgress = intActualProgress;

                    // Possibly bump up dblActualProgress incrementally


                    if (intTotalDatasets > 0)
                    {
                        // This is a number between 0 and 100
                        double dblSubProgressPercent = 0;

                        if (eProgress == eProgressSteps.LoadingDatasets)
                        {
                            dblSubProgressPercent = intDatasetsLoaded * 100 / (double)intTotalDatasets;

                        }
                        else if (eProgress == eProgressSteps.AligningDatasets)
                        {
                            dblSubProgressPercent = intDatasetsAligned * 100 / (double)intTotalDatasets;

                        }
                        else if (eProgress == eProgressSteps.PerformingClustering)
                        {
                            // The majority of the data will be charge 1 through 7
                            // Thus, we're dividing by 7 here, which means dblSubProgressPercent might be larger than 100; we'll account for that below
                            dblSubProgressPercent = intChargeStatesClustered * 100 / (double)7;
                        }

                        if (dblSubProgressPercent > 0)
                        {
                            if (dblSubProgressPercent > 100)
                                dblSubProgressPercent = 100;

                            // Bump up dblActualProgress based on dblSubProgressPercent
                            short intProgressNext;

                            if (mProgressStepPercentComplete.TryGetValue(eProgress + 1, out intProgressNext))
                            {
                                sngActualProgress += (float)(dblSubProgressPercent * (intProgressNext - intActualProgress) / 100.0);
                            }

                        }

                    }

                    if (mProgress < sngActualProgress)
                    {
                        mProgress = sngActualProgress;

                        if (mDebugLevel >= 3 || DateTime.UtcNow.Subtract(mLastProgressWriteTime).TotalMinutes >= 10)
                        {
                            mLastProgressWriteTime = DateTime.UtcNow;
                            LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, " ... " + mProgress.ToString("0.0") + "% complete");
                        }

                    }
                }

            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    OnErrorEvent("Error parsing MultiAlign log file (" + strLogFilePath + "): " + ex.Message);
                }
            }

        }

        #endregion

        // ------------------------------------------------------------------------------
        #region Mage MultiAlign class

        /// <summary>
        /// This is a Mage module that does MultiAlign processing
        /// of results for jobs that are supplied to it via standard tabular input
        /// </summary>
        public class MageMultiAlign : ContentFilter
        {

            #region Member Variables

            private string[] jobFieldNames;

            // indexes to look up values for some key job fields
            private int toolIdx;
            private int paramFileIdx;
            private int resultsFldrIdx;

            #endregion

            #region Properties

            public string WorkingDir { get; set; }
            // public string paramFilename { get; set; }

            #endregion

            #region Constructors

            // constructor
            public MageMultiAlign()
            {

            }

            #endregion

            #region Overrides of Mage ContentFilter

            // set up internal references
            protected override void ColumnDefsFinished()
            {
                // get array of column names
                var cols = new List<string>();
                foreach (var colDef in this.InputColumnDefs)
                {
                    cols.Add(colDef.Name);
                }
                jobFieldNames = cols.ToArray();

                // set up column indexes
                toolIdx = InputColumnPos["Tool"];
                paramFileIdx = InputColumnPos["Parameter_File"];
                resultsFldrIdx = InputColumnPos["Folder"];

            }

            #endregion

            #region MageMultiAlign Mage Pipelines

            // Build and run Mage pipeline to to extract contents of job
            private void ExtractResultsForJob(BaseModule currentJob, ExtractionType extractionParms, string extractedResultsFileName)
            {
                // search job results folders for list of results files to process and accumulate into buffer module
                var fileList = new SimpleSink();
                var plof = ExtractionPipelines.MakePipelineToGetListOfFiles(currentJob, fileList, extractionParms);
                plof.RunRoot(null);

                // extract contents of files
                var destination = new DestinationType("File_Output", WorkingDir, extractedResultsFileName);
                var pefc = ExtractionPipelines.MakePipelineToExtractFileContents(new SinkWrapper(fileList), extractionParms, destination);
                pefc.RunRoot(null);
            }

            #endregion

            #region MageMultiAlign Utility Methods

            // Build Mage source module containing one job to process
            private BaseModule MakeJobSourceModule(string[] fieldNames, object[] jobFields)
            {
                var currentJob = new DataGenerator {AddAdHocRow = fieldNames };
                currentJob.AddAdHocRow = ConvertObjectArrayToStringArray(jobFields);
                return currentJob;
            }

            // Convert array of objects to array of strings
            private static string[] ConvertObjectArrayToStringArray(object[] row)
            {
                var obj = new List<string>();
                foreach (var fld in row)
                {
                    obj.Add(fld.ToString());
                }
                return obj.ToArray();
            }


            #endregion
        }

        #endregion

        // ------------------------------------------------------------------------------
        #region Mage File Import Class

        /// <summary>
        /// Simple Mage FileContentProcessor module
        /// that imports the contents of files that it receives via standard tabular input
        /// to the given SQLite database table
        /// </summary>
        public class MageFileImport : FileContentProcessor
        {

            #region Properties

            // public string DBTableName { get; set; }
            public string DBFilePath { get; set; }
            public string ImportColumnList { get; set; }

            #endregion

            #region Constructors

            // constructor
            public MageFileImport()
            {
                base.SourceFolderColumnName = "Folder";
                base.SourceFileColumnName = "Name";
                base.OutputFolderPath = "ignore";
                base.OutputFileName = "ignore";
            }


            #endregion

        }

        #endregion

        // ------------------------------------------------------------------------------
        #region Classes for handling parameters

        // class for managing IJobParams object
        public class JobParameters
        {
            private readonly IJobParams mJobParms;

            public JobParameters(IJobParams jobParms)
            {
                mJobParms = jobParms;
            }

            public string RequireJobParam(string paramName)
            {
                var val = mJobParms.GetParam(paramName);
                if (string.IsNullOrWhiteSpace(val))
                {
                    throw new MageException(string.Format("Required job parameter '{0}' was missing.", paramName));
                }
                return val;
            }

            public string RequireJobParam(string paramName, string defaultValue)
            {
                var val = mJobParms.GetParam(paramName);
                if (string.IsNullOrWhiteSpace(val))
                {
                    mJobParms.AddAdditionalParameter(clsAnalysisJob.JOB_PARAMETERS_SECTION, paramName, defaultValue);
                    return defaultValue;
                }
                return val;
            }
            public string GetJobParam(string paramName)
            {
                return mJobParms.GetParam(paramName);
            }

            public string GetJobParam(string paramName, string defaultValue)
            {
                var val = mJobParms.GetParam(paramName);
                if (string.IsNullOrWhiteSpace(val))
                    val = defaultValue;
                return val;
            }
        }

        // class for managing IMgrParams object
        public class ManagerParameters
        {
            private readonly IMgrParams mMgrParms;

            public ManagerParameters(IMgrParams mgrParms)
            {
                mMgrParms = mgrParms;
            }

            public string RequireMgrParam(string paramName)
            {
                var val = mMgrParms.GetParam(paramName);
                if (string.IsNullOrWhiteSpace(val))
                {
                    throw new MageException(string.Format("Required manager parameter '{0}' was missing.", paramName));
                }
                return val;
            }
        }


        #endregion
    }
}
