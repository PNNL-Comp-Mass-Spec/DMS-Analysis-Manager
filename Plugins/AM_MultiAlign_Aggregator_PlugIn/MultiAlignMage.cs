using AnalysisManagerBase;
using Mage;
using MageExtExtractionFilters;
using PRISM;
using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using AnalysisManagerBase.StatusReporting;

namespace AnalysisManagerMultiAlign_AggregatorPlugIn
{
    /// <summary>
    /// Runs the MultiAlign pipeline
    /// </summary>
    public class MultiAlignMage : EventNotifier
    {
        #region Member Variables

        private string mResultsDBFileName = "";
        private string mWorkingDir;
        private JobParameters mJobParams;
        private ManagerParameters mMgrParams;
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

        private SortedDictionary<MageProgressSteps, Int16> mProgressStepPercentComplete;
        // This dictionary associates key log text entries with the corresponding progress step for each
        // It is populated by sub InitializeProgressStepDictionaries

        private SortedDictionary<string, MageProgressSteps> mProgressStepLogText;
        private enum MageProgressSteps
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

        #endregion

        #region Properties

        /// <summary>
        /// Status message
        /// </summary>
        public string Message => string.IsNullOrEmpty(mMessage) ? string.Empty : mMessage;

        #endregion

        #region Constructors

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="mgrParams"></param>
        /// <param name="statusTools"></param>
        public MultiAlignMage(IJobParams jobParams, IMgrParams mgrParams, IStatusFile statusTools)
        {
            mStatusTools = statusTools;
            Initialize(jobParams, mgrParams);
        }

        #endregion

        #region Initialization

        /// <summary>
        /// Set up internal variables
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="mgrParams"></param>
        private void Initialize(IJobParams jobParams, IMgrParams mgrParams)
        {
            mJobParams = new JobParameters(jobParams);
            mMgrParams = new ManagerParameters(mgrParams);

            mResultsDBFileName = mJobParams.RequireJobParam("ResultsBaseName", "Results") + ".db3";
            mWorkingDir = mMgrParams.RequireMgrParam("WorkDir");
            mSearchType = mJobParams.RequireJobParam("MultiAlignSearchType");					// File extension of input data files, can be "_LCMSFeatures.txt" or "_isos.csv"
            mParamFilename = mJobParams.RequireJobParam("ParmFileName");
            mDebugLevel = Convert.ToInt16(mMgrParams.RequireMgrParam("DebugLevel"));
            mJobNum = mJobParams.RequireJobParam("Job");
        }

        #endregion

        #region Processing

        /// <summary>
        /// Do processing
        /// </summary>
        /// <returns>True if success; otherwise false</returns>
        public bool Run(string sMultiAlignConsolePath)
        {
            var dataPackageID = mJobParams.RequireJobParam("DataPackageID");

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

            var cmdRunner = new RunDosProgram(mWorkingDir);
            cmdRunner.LoopWaiting += CmdRunner_LoopWaiting;
            cmdRunner.ErrorEvent += CmdRunnerOnErrorEvent;

            if (mDebugLevel > 4)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "MultiAlignMage.RunTool(): Enter");
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

            var MultiAlignResultFilename = mJobParams.GetJobParam("ResultsBaseName");

            if (string.IsNullOrWhiteSpace(MultiAlignResultFilename))
            {
                MultiAlignResultFilename = mJobParams.RequireJobParam(AnalysisResources.JOB_PARAM_DATASET_NAME);
            }

            // Set up and execute a program runner to run MultiAlign
            var arguments = " -files " + MULTIALIGN_INPUT_FILE +
                            " -params " + Path.Combine(mWorkingDir, mParamFilename) +
                            " -path " + mWorkingDir +
                            " -name " + mResultsDBFileName +
                            " -plots";

            if (mDebugLevel >= 1)
            {
                LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, sMultiAlignConsolePath + " " + arguments);
            }

            cmdRunner.CreateNoWindow = true;
            cmdRunner.CacheStandardOutput = true;
            cmdRunner.EchoOutputToConsole = true;
            cmdRunner.WriteConsoleOutputToFile = false;

            if (!cmdRunner.RunProgram(sMultiAlignConsolePath, arguments, "MultiAlign", true))
            {
                if (string.IsNullOrEmpty(mMessage))
                    mMessage = "Error running MultiAlign";

                if (!string.IsNullOrEmpty(mMultialignErrorMessage))
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

                const string strParamFileStoragePathKeyName = Global.STEP_TOOL_PARAM_FILE_STORAGE_PATH_PREFIX + "MultiAlign";
                var strMAParameterFileStoragePath = mMgrParams.RequireMgrParam(strParamFileStoragePathKeyName);
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
        private SimpleSink GetListOfDataPackageJobsToProcess(string dataPackageID, string tool)
        {
            const string sqlTemplate = "SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0} AND Tool LIKE '%{1}%'";
            var connStr = mMgrParams.RequireMgrParam("ConnectionString");
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
        /// Copy MultiAlign input files into working directory from given list of jobs
        /// Looks for DeconTools _LCMSFeatures.txt or _isos.csv files for the given jobs
        /// </summary>
        /// <param name="multialignJobsToProcess"></param>
        /// <param name="fileSpec"></param>
        private bool CopyMultiAlignInputFiles(IBaseModule multialignJobsToProcess, string fileSpec)
        {
            try
            {
                const string columnsToIncludeInOutput = "Job, Dataset, Dataset_ID, Tool, Settings_File, Parameter_File, Instrument";
                var fileList = GetListOfFilesFromDirectoryList(multialignJobsToProcess, fileSpec, columnsToIncludeInOutput);

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
                    OutputDirectoryPath = mWorkingDir,
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
        /// Create a new SQLReader module to do a specific query
        /// </summary>
        /// <param name="sql">Query to use</param>
        /// <param name="connectionString"></param>
        public static SQLReader MakeDBReaderModule(string sql, string connectionString)
        {
            var reader = new SQLReader(connectionString)
            {
                SQLText = sql
            };
            return reader;
        }

        /// <summary>
        /// Get list of selected files from list of directories
        /// </summary>
        /// <param name="directoryListSource">Mage object that contains list of directories</param>
        /// <param name="fileNameSelector">File name selector to select files to be included in output list</param>
        /// <param name="passThroughColumns">List of columns from source object to pass through to output list object</param>
        /// <returns>Mage object containing list of files</returns>
        public SimpleSink GetListOfFilesFromDirectoryList(IBaseModule directoryListSource, string fileNameSelector, string passThroughColumns)
        {
            var sinkObject = new SimpleSink();

            // create file filter module and initialize it
            var fileFilter = new FileListFilter
            {
                FileNameSelector = fileNameSelector,
                SourceDirectoryColumnName = "Directory",
                FileColumnName = "Name",
                OutputColumnList = "Item|+|text, Name|+|text, File_Size_KB|+|text, Directory, " + passThroughColumns,
                FileSelectorMode = "RegEx",
                IncludeFilesOrDirectories = "File",
                RecursiveSearch = "No",
                SubdirectorySearchName = "*"
            };

            // build, wire, and run pipeline
            ProcessingPipeline.Assemble("FileListPipeline", directoryListSource, fileFilter, sinkObject).RunRoot(null);
            return sinkObject;
        }

        /// <summary>
        /// Build the MultiAlign input file
        /// </summary>
        /// <param name="strInputFileExtension"></param>
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
                    mMessage = "Did not find any files of type " + strInputFileExtension + " in directory " + mWorkingDir;
                    OnErrorEvent(mMessage);
                    return false;
                }

                using var writer = new StreamWriter(new FileStream(TargetFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                writer.WriteLine("[Files]");

                var alignmentDataset = mJobParams.GetJobParam("AlignmentDataset");
                foreach (var TmpFile_loopVariable in Files)
                {
                    var TmpFile = TmpFile_loopVariable;
                    if (!string.IsNullOrWhiteSpace(alignmentDataset) && TmpFile.IndexOf(alignmentDataset, StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        // Append an asterisk to this dataset's path to indicate that it is the base dataset to which the others will be aligned
                        writer.WriteLine(TmpFile + "*");
                    }
                    else
                    {
                        writer.WriteLine(TmpFile);
                    }
                }

                // Check to see if a mass tag database has been defined and NO alignment dataset has been defined
                var amtDb = mJobParams.GetJobParam("AMTDB");
                if (!string.IsNullOrEmpty(amtDb.Trim()))
                {
                    writer.WriteLine("[Database]");
                    writer.WriteLine("Database = " + mJobParams.GetJobParam("AMTDB"));			// For example, MT_Human_Sarcopenia_MixedLC_P692
                    writer.WriteLine("Server = " + mJobParams.GetJobParam("AMTDBServer"));		// For example, Elmer
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
                mStatusTools.UpdateAndWrite(MgrStatusCodes.RUNNING, TaskStatusCodes.RUNNING, TaskStatusDetailCodes.RUNNING_TOOL, mProgress);
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
        private void InitializeProgressStepDictionaries()
        {
            mProgressStepPercentComplete = new SortedDictionary<MageProgressSteps, Int16>
            {
                {MageProgressSteps.Starting, 5},
                {MageProgressSteps.LoadingMTDB, 6},
                {MageProgressSteps.LoadingDatasets, 7},
                {MageProgressSteps.LinkingMSFeatures, 45},
                {MageProgressSteps.AligningDatasets, 50},
                {MageProgressSteps.PerformingClustering, 75},
                {MageProgressSteps.PerformingPeakMatching, 85},
                {MageProgressSteps.CreatingFinalPlots, 90},
                {MageProgressSteps.CreatingReport, 95},
                {MageProgressSteps.Complete, 97}
            };

            mProgressStepLogText = new SortedDictionary<string, MageProgressSteps>
            {
                {"[LogStart]", MageProgressSteps.Starting},
                {" - Loading Mass Tag database from database", MageProgressSteps.LoadingMTDB},
                {" - Loading dataset data files", MageProgressSteps.LoadingDatasets},
                {" - Linking MS Features", MageProgressSteps.LinkingMSFeatures},
                {" - Aligning datasets", MageProgressSteps.AligningDatasets},
                {" - Performing clustering", MageProgressSteps.PerformingClustering},
                {" - Performing Peak Matching", MageProgressSteps.PerformingPeakMatching},
                {" - Creating Final Plots", MageProgressSteps.CreatingFinalPlots},
                {" - Creating report", MageProgressSteps.CreatingReport},
                {" - Analysis Complete", MageProgressSteps.Complete}
            };
        }

        /// <summary>
        /// Parse the MultiAlign log file to track the search progress
        /// Looks in the work directory to auto-determine the log file name
        /// </summary>
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
        /// <param name="logFilePath">Full path to the log file</param>
        private void ParseMultiAlignLogFile(string logFilePath)
        {
            // The MultiAlign log file is quite big, but we can keep track of progress by looking for known text in the log file lines
            // Dictionary mProgressStepLogText keeps track of the lines of text to match while mProgressStepPercentComplete keeps track of the % complete values to use

            // For certain long-running steps we can compute a more precise version of % complete by keeping track of the number of datasets processed

            // var reExtractPercentFinished = new Regex(@"(\d+)% finished", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            try
            {
                if (!File.Exists(logFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "MultiAlign log file not found: " + logFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 4)
                {
                    LogTools.WriteLog(LogTools.LoggerTypes.LogFile, BaseLogger.LogLevels.DEBUG, "Parsing file " + logFilePath);
                }

                var eProgress = MageProgressSteps.Starting;

                var totalDatasets = 0;
                var datasetsLoaded = 0;
                var datasetsAligned = 0;
                var chargeStatesClustered = 0;

                // Open the file for read; don't lock it (to thus allow MultiAlign to still write to it)
                using (var reader = new StreamReader(new FileStream(logFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();

                        if (!string.IsNullOrWhiteSpace(dataLine))
                        {
                            var matchFound = false;

                            // Update progress if the line contains any of the entries in mProgressStepLogText
                            foreach (var lstItem in mProgressStepLogText)
                            {
                                if (dataLine.Contains(lstItem.Key))
                                {
                                    if (eProgress < lstItem.Value)
                                    {
                                        eProgress = lstItem.Value;
                                    }
                                    matchFound = true;
                                    break;
                                }
                            }

                            if (!matchFound)
                            {
                                if (dataLine.Contains("Dataset Information: "))
                                {
                                    totalDatasets++;
                                }
                                else if (dataLine.Contains("- Adding features to cache database"))
                                {
                                    datasetsLoaded++;
                                }
                                else if (dataLine.Contains("- Features Aligned -"))
                                {
                                    datasetsAligned++;
                                }
                                else if (dataLine.Contains("- Clustering Charge State"))
                                {
                                    chargeStatesClustered++;
                                }
                                else if (dataLine.Contains("No baseline dataset or database was selected"))
                                {
                                    mMultialignErrorMessage = "No baseline dataset or database was selected";
                                }
                            }
                        }
                    }
                }

                // Compute the actual progress

                if (mProgressStepPercentComplete.TryGetValue(eProgress, out var actualProgress))
                {
                    float progressOverall = actualProgress;

                    // Possibly bump up dblActualProgress incrementally

                    if (totalDatasets > 0)
                    {
                        // This is a number between 0 and 100
                        double subProgressPercent = 0;

                        if (eProgress == MageProgressSteps.LoadingDatasets)
                        {
                            subProgressPercent = datasetsLoaded * 100 / (double)totalDatasets;
                        }
                        else if (eProgress == MageProgressSteps.AligningDatasets)
                        {
                            subProgressPercent = datasetsAligned * 100 / (double)totalDatasets;
                        }
                        else if (eProgress == MageProgressSteps.PerformingClustering)
                        {
                            // The majority of the data will be charge 1 through 7
                            // Thus, we're dividing by 7 here, which means subProgressPercent might be larger than 100; we'll account for that below
                            subProgressPercent = chargeStatesClustered * 100 / (double)7;
                        }

                        if (subProgressPercent > 0)
                        {
                            if (subProgressPercent > 100)
                                subProgressPercent = 100;

                            // Bump up dblActualProgress based on subProgressPercent

                            if (mProgressStepPercentComplete.TryGetValue(eProgress + 1, out var intProgressNext))
                            {
                                progressOverall += (float)(subProgressPercent * (intProgressNext - actualProgress) / 100.0);
                            }
                        }
                    }

                    if (mProgress < progressOverall)
                    {
                        mProgress = progressOverall;

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
                    OnErrorEvent("Error parsing MultiAlign log file (" + logFilePath + "): " + ex.Message);
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
        [Obsolete("Unused")]
        public class MageMultiAlign : ContentFilter
        {
            #region Member Variables

            private string[] mJobFieldNames;

            // indexes to look up values for some key job fields
            private int mToolIndex;
            private int mParamFileIndex;
            private int mResultsDirectoryIndex;

            #endregion

            #region Properties

            /// <summary>
            /// Work directory
            /// </summary>
            public string WorkingDir { get; set; }
            // public string paramFilename { get; set; }

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
                mJobFieldNames = cols.ToArray();

                // set up column indexes
                mToolIndex = InputColumnPos["Tool"];
                mParamFileIndex = InputColumnPos["Parameter_File"];

                if (InputColumnPos.TryGetValue("Directory", out var directoryColIndex))
                    mResultsDirectoryIndex = directoryColIndex;
                else if (InputColumnPos.TryGetValue("Folder", out var folderColIndex))
                    mResultsDirectoryIndex = folderColIndex;
                else
                    throw new Exception("ColumnDefsFinished could not find column Directory or Folder");
            }

            #endregion

            #region MageMultiAlign Mage Pipelines

            // Build and run Mage pipeline to to extract contents of job
            [Obsolete("Unused")]
            private void ExtractResultsForJob(BaseModule currentJob, ExtractionType extractionParams, string extractedResultsFileName)
            {
                // search job results directories for list of results files to process and accumulate into buffer module
                var fileList = new SimpleSink();
                var getFilesPipeline = ExtractionPipelines.MakePipelineToGetListOfFiles(currentJob, fileList, extractionParams);
                getFilesPipeline.RunRoot(null);

                // extract contents of files
                var destination = new DestinationType("File_Output", WorkingDir, extractedResultsFileName);
                var extractContentsPipeline = ExtractionPipelines.MakePipelineToExtractFileContents(new SinkWrapper(fileList), extractionParams, destination);
                extractContentsPipeline.RunRoot(null);
            }

            #endregion

            #region MageMultiAlign Utility Methods

            // Build Mage source module containing one job to process
            [Obsolete("No longer used")]
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

            /// <summary>
            /// Database file path
            /// </summary>
            public string DBFilePath { get; set; }

            /// <summary>
            /// Import column list
            /// </summary>
            public string ImportColumnList { get; set; }

            #endregion

            #region Constructors

            /// <summary>
            /// Constructor
            /// </summary>
            public MageFileImport()
            {
                base.SourceDirectoryColumnName = "Directory";
                base.SourceFileColumnName = "Name";
                base.OutputDirectoryPath = "ignore";
                base.OutputFileName = "ignore";
            }

            #endregion

        }

        #endregion

        // ------------------------------------------------------------------------------
        #region Classes for handling parameters

        /// <summary>
        /// Class for managing IJobParams object
        /// </summary>
        public class JobParameters
        {
            private readonly IJobParams mJobParams;

            /// <summary>
            /// Constructor
            /// </summary>
            /// <param name="jobParams"></param>
            public JobParameters(IJobParams jobParams)
            {
                mJobParams = jobParams;
            }

            /// <summary>
            /// Verify that a job parameter is defined
            /// </summary>
            /// <param name="paramName"></param>
            public string RequireJobParam(string paramName)
            {
                var val = mJobParams.GetParam(paramName);
                if (string.IsNullOrWhiteSpace(val))
                {
                    throw new MageException(string.Format("Required job parameter '{0}' was missing.", paramName));
                }
                return val;
            }

            /// <summary>
            /// Verify that a job parameter is defined
            /// </summary>
            /// <param name="paramName"></param>
            /// <param name="defaultValue"></param>
            public string RequireJobParam(string paramName, string defaultValue)
            {
                var val = mJobParams.GetParam(paramName);
                if (string.IsNullOrWhiteSpace(val))
                {
                    mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, paramName, defaultValue);
                    return defaultValue;
                }
                return val;
            }

            /// <summary>
            /// Get a job parameter
            /// </summary>
            /// <param name="paramName"></param>
            public string GetJobParam(string paramName)
            {
                return mJobParams.GetParam(paramName);
            }

            /// <summary>
            /// Get a job parameter
            /// </summary>
            /// <param name="paramName"></param>
            /// <param name="defaultValue"></param>
            public string GetJobParam(string paramName, string defaultValue)
            {
                var val = mJobParams.GetParam(paramName);
                if (string.IsNullOrWhiteSpace(val))
                    val = defaultValue;
                return val;
            }
        }

        /// <summary>
        /// Class for managing IMgrParams object
        /// </summary>
        public class ManagerParameters
        {
            private readonly IMgrParams mMgrParams;

            /// <summary>
            /// Constructor
            /// </summary>
            /// <param name="mgrParams"></param>
            public ManagerParameters(IMgrParams mgrParams)
            {
                mMgrParams = mgrParams;
            }

            /// <summary>
            /// Verify that a manager parameter is defined
            /// </summary>
            /// <param name="paramName"></param>
            public string RequireMgrParam(string paramName)
            {
                var val = mMgrParams.GetParam(paramName);
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
