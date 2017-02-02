using System;
using System.Collections.Generic;
using AnalysisManagerBase;
using Mage;
using MageExtExtractionFilters;

namespace AnalysisManagerMultiAlign_AggregatorPlugIn
{

    public class clsMultiAlignMage //: clsAnalysisToolRunnerBase
    {

        #region Member Variables

        protected string mResultsDBFileName = "";
        protected string mWorkingDir;
        protected JobParameters mJP;
        protected ManagerParameters mMP;
        protected string mMessage = "";
        protected string mSearchType = "";								// File extension of input data files, e.g. "_LCMSFeatures.txt" or "_isos.csv"
        protected string mParamFilename = "";
        protected const string MULTIALIGN_INPUT_FILE = "Input.txt";
        protected string mJobNum = "";
        protected short mDebugLevel;
        protected float mProgress = 0;
        protected string mMultialignErroMessage = "";
        protected clsRunDosProgram CmdRunner;
        protected IStatusFile mStatusTools; 


        protected System.Collections.Generic.SortedDictionary<eProgressSteps, Int16> mProgressStepPercentComplete;
        // This dictionary associates key log text entries with the corresponding progress step for each
        // It is populated by sub InitializeProgressStepDictionaries

        protected System.Collections.Generic.SortedDictionary<string, eProgressSteps> mProgressStepLogText;
        protected enum eProgressSteps
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

        protected string m_MultialignErroMessage = string.Empty;

        #endregion

        #region Properties
        public string Message
        {
            get
            {
                if (string.IsNullOrEmpty(mMessage))
                    return string.Empty;
                else
                    return mMessage;
            }
        }
                
        #endregion

        #region Constructors

        public clsMultiAlignMage(IJobParams jobParms, IMgrParams mgrParms) {
            Intialize(jobParms, mgrParms);
        }

        #endregion

        #region Initialization

        /// <summary>
        /// Set up internal variables
        /// </summary>
        /// <param name="jobParms"></param>
        /// <param name="mgrParms"></param>
        private void Intialize(IJobParams jobParms, IMgrParams mgrParms) {
            this.mJP = new JobParameters(jobParms);
            this.mMP = new ManagerParameters(mgrParms);

            this.mResultsDBFileName = mJP.RequireJobParam("ResultsBaseName", "Results") + ".db3";
            this.mWorkingDir = mMP.RequireMgrParam("workdir");
            this.mSearchType = mJP.RequireJobParam("MultiAlignSearchType");					// File extension of input data files, can be "_LCMSFeatures.txt" or "_isos.csv"
            this.mParamFilename = mJP.RequireJobParam("ParmFileName");
            this.mDebugLevel = Convert.ToInt16(mMP.RequireMgrParam("debuglevel"));
            this.mJobNum = mJP.RequireJobParam("Job");
        }

        #endregion

        #region Processing

        /// <summary>
        /// Do processing
        /// </summary>
        /// <returns>True if success; otherwise false</returns>
        public bool Run(string sMultiAlignConsolePath) {
            string dataPackageID = mJP.RequireJobParam("DataPackageID");
            bool blnSuccess;

            blnSuccess = GetMultiAlignParameterFile();
            if (!blnSuccess) 
                return false;

            SimpleSink multialignJobsToProcess = GetListOfDataPackageJobsToProcess(dataPackageID, "Decon2LS_V2");

            if (multialignJobsToProcess.Rows.Count == 0)
            {
                if (string.IsNullOrEmpty(mMessage))
                {
                    mMessage = "Data package " + dataPackageID + " does not have any Decon2LS_V2 analysis jobs";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMessage);
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

            if (mDebugLevel > 4) {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "clsMultiAlignMage.RunTool(): Enter");
            }

            if (String.IsNullOrWhiteSpace(sMultiAlignConsolePath))
            {
                mMessage = "MultiAlignConsolePath is empty";
                return false;
            }

            if (!System.IO.File.Exists(sMultiAlignConsolePath))
            {
                mMessage = "MultiAlign program not found: " + sMultiAlignConsolePath;
                return false;
            }

            String MultiAlignResultFilename = mJP.GetJobParam("ResultsBaseName");

            if (string.IsNullOrWhiteSpace(MultiAlignResultFilename))
            {
                MultiAlignResultFilename = mJP.RequireJobParam("DatasetNum");
            }

            //Set up and execute a program runner to run MultiAlign
            CmdStr = " -files " + MULTIALIGN_INPUT_FILE + " -params " + System.IO.Path.Combine(mWorkingDir, mParamFilename) + " -path " + mWorkingDir + " -name " + mResultsDBFileName + " -plots";
            if (mDebugLevel >= 1)
            {
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, sMultiAlignConsolePath + " " + CmdStr);
            }

            cmdRunner.CreateNoWindow = true;
            cmdRunner.CacheStandardOutput = true;
            cmdRunner.EchoOutputToConsole = true;
            cmdRunner.WriteConsoleOutputToFile = false;

            if (!cmdRunner.RunProgram(sMultiAlignConsolePath, CmdStr, "MultiAlign", true))
            {
                if (string.IsNullOrEmpty(mMessage))
                    mMessage = "Error running MultiAlign";

                if (!string.IsNullOrEmpty(m_MultialignErroMessage))
                {
                    mMessage += ": " + mMultialignErroMessage;
                }
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMessage + ", job " + mJobNum);

                return false;
            }

            return true;
        }

        #endregion


        #region Mage Pipelines and Utilities

        private bool GetMultiAlignParameterFile() {

            try
            {
                // Retrieve the MultiAlign Parameter .xml file specified for this job
                if (string.IsNullOrEmpty(mParamFilename))
                {
                    mMessage = "Job parameter ParmFileName not defined in the settings for this MultiAlign job; unable to continue";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMessage);
                    return false;
                }

                string strParamFileStoragePathKeyName = AnalysisManagerBase.clsGlobal.STEPTOOL_PARAMFILESTORAGEPATH_PREFIX + "MultiAlign";
                string strMAParameterFileStoragePath = mMP.RequireMgrParam(strParamFileStoragePathKeyName);
                if (string.IsNullOrEmpty(strMAParameterFileStoragePath))
                {
                    strMAParameterFileStoragePath = @"\\gigasax\DMS_Parameter_Files\MultiAlign";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Parameter " + strParamFileStoragePathKeyName + " is not defined (obtained using V_Pipeline_Step_Tools_Detail_Report in the Broker DB); will assume: " + strMAParameterFileStoragePath);
                }

                string sourceFilePath = System.IO.Path.Combine(strMAParameterFileStoragePath, mParamFilename);

                if (!System.IO.File.Exists(sourceFilePath))
                {
                    mMessage = "MultiAlign parameter file not found: " + strMAParameterFileStoragePath;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMessage);
                    return false;
                }
                System.IO.File.Copy(sourceFilePath, System.IO.Path.Combine(mWorkingDir, mParamFilename), true);
            }
            catch (Exception ex)
            {
                mMessage = "Error copying the MultiAlign parameter file";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMessage + ": " + ex.Message);
                return false;
            }
         

            return true;

        }

        /// <summary>
        /// query DMS to get list of data package jobs to process
        /// </summary>
        /// <param name="dataPackageID"></param>
        /// <returns></returns>
        private SimpleSink GetListOfDataPackageJobsToProcess(string dataPackageID, string tool) {
            string sqlTemplate = @"SELECT * FROM V_Mage_Data_Package_Analysis_Jobs WHERE Data_Package_ID = {0} AND Tool LIKE '%{1}%'";
            string connStr = mMP.RequireMgrParam("ConnectionString");
            string sql = string.Format(sqlTemplate, new string[] { dataPackageID, tool });
            SimpleSink jobList = GetListOfItemsFromDB(sql, connStr);

            if (jobList.Rows.Count == 0)
            {
                mMessage = "Data package " + dataPackageID + " does not have any " + tool + " analysis jobs";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMessage + " using query " + sqlTemplate);
            }

            return jobList;
        }

        /// <summary>
        /// Copy MultiAlign input files into working folder from given list of jobs
        /// Looks for DeconTools _LCMSFeatures.txt or _isos.csv files for the given jobs
        /// </summary>
        /// <param name="multialignJobsToProcess"></param>
        private bool CopyMultiAlignInputFiles(SimpleSink multialignJobsToProcess, string fileSpec) {

            try
            {

                string columnsToIncludeInOutput = "Job, Dataset, Dataset_ID, Tool, Settings_File, Parameter_File, Instrument";
                SimpleSink fileList = GetListOfFilesFromFolderList(multialignJobsToProcess, fileSpec, columnsToIncludeInOutput);

                // Check for "--No Files Found--" for any of the jobs
                foreach (var row in fileList.Rows)
                {
                    if (row.Length > 4)
                    {
                        if (row[1].ToString() == Mage.BaseModule.kNoFilesFound)
                        {
                            mMessage = "Did not find any " + fileSpec + " files for job " + row[4].ToString() + " at " + row[3].ToString();
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMessage);
                            return false;
                        }
                    }
                }

                // make module to copy file(s) from server to working directory
                FileCopy copier = new FileCopy();
                copier.OutputFolderPath = mWorkingDir;
                copier.SourceFileColumnName = "Name";

                ProcessingPipeline.Assemble("File_Copy", fileList, copier).RunRoot(null);

            }
            catch (Exception ex)
            {
                mMessage = "Error copying DeconTools result files";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMessage + ": " + ex.Message);
                return false;
            }

            return true;

        }

        /// <summary>
        /// Get a list of items using a database query
        /// </summary>
        /// <param name="sql">Query to use a source of jobs</param>
        /// <returns>A Mage module containing list of jobs</returns>
        public static SimpleSink GetListOfItemsFromDB(string sql, string connectionString) {
            SimpleSink itemList = new SimpleSink();
            MSSQLReader reader = MakeDBReaderModule(sql, connectionString);
            ProcessingPipeline pipeline = ProcessingPipeline.Assemble("Get Items", reader, itemList);
            pipeline.RunRoot(null);
            return itemList;
        }

        /// <summary>
        /// Create a new MSSQLReader module to do a specific query
        /// </summary>
        /// <param name="sql">Query to use</param>
        /// <returns></returns>
        public static MSSQLReader MakeDBReaderModule(String sql, string connectionString) {
            MSSQLReader reader = new MSSQLReader();
            reader.ConnectionString = connectionString;
            reader.SQLText = sql;
            return reader;
        }

        /// <summary>
        /// Get list of selected files from list of folders
        /// </summary>
        /// <param name="folderListSource">Mage object that contains list of folders</param>
        /// <param name="fileNameSelector">File name selector to select files to be included in output list</param>
        /// <param name="passThroughColumns">List of columns from source object to pass through to output list object</param>
        /// <returns>Mage object containing list of files</returns>
        public SimpleSink GetListOfFilesFromFolderList(IBaseModule folderListSource, string fileNameSelector, string passThroughColumns) {
            SimpleSink sinkObject = new SimpleSink();

            // create file filter module and initialize it
            FileListFilter fileFilter = new FileListFilter();
            fileFilter.FileNameSelector = fileNameSelector;
            fileFilter.SourceFolderColumnName = "Folder";
            fileFilter.FileColumnName = "Name";
            fileFilter.OutputColumnList = "Item|+|text, Name|+|text, File_Size_KB|+|text, Folder, " + passThroughColumns;
            fileFilter.FileSelectorMode = "RegEx";
            fileFilter.IncludeFilesOrFolders = "File";
            fileFilter.RecursiveSearch = "No";
            fileFilter.SubfolderSearchName = "*";

            // build, wire, and run pipeline
            ProcessingPipeline.Assemble("FileListPipeline", folderListSource, fileFilter, sinkObject).RunRoot(null);
            return sinkObject;
        }

        /// <summary>
        /// Build the MultiAlign input file
        /// </summary>
        /// <param name="strInputFileExtension"></param>
        /// <returns></returns>
        protected bool BuildMultiAlignInputTextFile(string strInputFileExtension)
        {

            const string INPUT_FILENAME = "input.txt";

            bool blnSuccess = true;

            string TargetFilePath = System.IO.Path.Combine(mWorkingDir, INPUT_FILENAME);

            string TmpFile = null;
            string[] Files = null;

            // Create the MultiAlign input file 

            try
            {

                Files = System.IO.Directory.GetFiles(mWorkingDir, "*" + strInputFileExtension);

                if (Files.Length == 0)
                {
                    mMessage = "Did not find any files of type " + strInputFileExtension + " in folder " + mWorkingDir;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMessage);
                    return false;
                }

                using (System.IO.StreamWriter swOutFile = new System.IO.StreamWriter(new System.IO.FileStream(TargetFilePath, System.IO.FileMode.Create, System.IO.FileAccess.Write, System.IO.FileShare.Read)))
                {
                    swOutFile.WriteLine("[Files]");
                    string AlignmentDataset = mJP.GetJobParam("AlignmentDataset");
                    foreach (string TmpFile_loopVariable in Files)
                    {
                        TmpFile = TmpFile_loopVariable;
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

                    //Check to see if a mass tag database has been defined and NO alignment dataset has been defined
                    string AmtDb = mJP.GetJobParam("AMTDB");
                    if (!string.IsNullOrEmpty(AmtDb.Trim()))
                    {
                        swOutFile.WriteLine("[Database]");
                        swOutFile.WriteLine("Database = " + mJP.GetJobParam("AMTDB"));			// For example, MT_Human_Sarcopenia_MixedLC_P692
                        swOutFile.WriteLine("Server = " + mJP.GetJobParam("AMTDBServer"));		// For example, Elmer
                    }
                }
            
                blnSuccess = true;
            }
            catch (Exception ex)
            {
                mMessage = "Error building the input .txt file (" + INPUT_FILENAME + ")";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, mMessage + ": " + ex.Message);
                return false;
            }

            return blnSuccess;
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
            System.DateTime dtLastStatusUpdate = System.DateTime.UtcNow;
            System.DateTime dtLastMultialignLogFileParse = System.DateTime.UtcNow;

            // Update the status file (limit the updates to every 5 seconds)
            if (System.DateTime.UtcNow.Subtract(dtLastStatusUpdate).TotalSeconds >= 5)
            {
                dtLastStatusUpdate = System.DateTime.UtcNow;
                mStatusTools.UpdateAndWrite(IStatusFile.EnumMgrStatus.RUNNING, IStatusFile.EnumTaskStatus.RUNNING, IStatusFile.EnumTaskStatusDetail.RUNNING_TOOL, mProgress);
            }

            if (System.DateTime.UtcNow.Subtract(dtLastMultialignLogFileParse).TotalSeconds >= 15)
            {
                dtLastMultialignLogFileParse = System.DateTime.UtcNow;
                ParseMultiAlignLogFile();
            }
        }
        
        /// <summary>
        /// Populates dictionary mProgressStepPercentComplete(), which is used by ParseMultiAlignLogFile
        /// </summary>
        /// <remarks></remarks>
        private void InitializeProgressStepDictionaries()
        {
            mProgressStepPercentComplete = new System.Collections.Generic.SortedDictionary<eProgressSteps, Int16>();

            mProgressStepPercentComplete.Add(eProgressSteps.Starting, 5);
            mProgressStepPercentComplete.Add(eProgressSteps.LoadingMTDB, 6);
            mProgressStepPercentComplete.Add(eProgressSteps.LoadingDatasets, 7);
            mProgressStepPercentComplete.Add(eProgressSteps.LinkingMSFeatures, 45);
            mProgressStepPercentComplete.Add(eProgressSteps.AligningDatasets, 50);
            mProgressStepPercentComplete.Add(eProgressSteps.PerformingClustering, 75);
            mProgressStepPercentComplete.Add(eProgressSteps.PerformingPeakMatching, 85);
            mProgressStepPercentComplete.Add(eProgressSteps.CreatingFinalPlots, 90);
            mProgressStepPercentComplete.Add(eProgressSteps.CreatingReport, 95);
            mProgressStepPercentComplete.Add(eProgressSteps.Complete, 97);

            mProgressStepLogText = new System.Collections.Generic.SortedDictionary<string, eProgressSteps>();
            mProgressStepLogText.Add("[LogStart]", eProgressSteps.Starting);
            mProgressStepLogText.Add(" - Loading Mass Tag database from database", eProgressSteps.LoadingMTDB);
            mProgressStepLogText.Add(" - Loading dataset data files", eProgressSteps.LoadingDatasets);
            mProgressStepLogText.Add(" - Linking MS Features", eProgressSteps.LinkingMSFeatures);
            mProgressStepLogText.Add(" - Aligning datasets", eProgressSteps.AligningDatasets);
            mProgressStepLogText.Add(" - Performing clustering", eProgressSteps.PerformingClustering);
            mProgressStepLogText.Add(" - Performing Peak Matching", eProgressSteps.PerformingPeakMatching);
            mProgressStepLogText.Add(" - Creating Final Plots", eProgressSteps.CreatingFinalPlots);
            mProgressStepLogText.Add(" - Creating report", eProgressSteps.CreatingReport);
            mProgressStepLogText.Add(" - Analysis Complete", eProgressSteps.Complete);

        }

        /// <summary>
        /// Parse the MultiAlign log file to track the search progress
        /// Looks in the work directory to auto-determine the log file name
        /// </summary>
        /// <remarks></remarks>
        private void ParseMultiAlignLogFile()
        {
            System.IO.DirectoryInfo diWorkDirectory = null;
            System.IO.FileInfo[] fiFiles = null;
            string strLogFilePath = string.Empty;

            try
            {
                diWorkDirectory = new System.IO.DirectoryInfo(mWorkingDir);
                fiFiles = diWorkDirectory.GetFiles("*-log*.txt");

                if (fiFiles.Length >= 1)
                {
                    strLogFilePath = fiFiles[0].FullName;

                    if (fiFiles.Length > 1)
                    {
                        // Use the newest file in fiFiles
                        int intBestIndex = 0;

                        for (int intIndex = 1; intIndex <= fiFiles.Length - 1; intIndex++)
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
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error finding the MultiAlign log file at " + mWorkingDir + ": " + ex.Message);
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

            //Static reExtractPercentFinished As New System.Text.RegularExpressions.Regex("(\d+)% finished", Text.RegularExpressions.RegexOptions.Compiled Or Text.RegularExpressions.RegexOptions.IgnoreCase)
            System.DateTime dtLastProgressWriteTime = System.DateTime.UtcNow;

            //Dim oMatch As System.Text.RegularExpressions.Match
            try
            {
                if (!System.IO.File.Exists(strLogFilePath))
                {
                    if (mDebugLevel >= 4)
                    {
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "MultiAlign log file not found: " + strLogFilePath);
                    }

                    return;
                }

                if (mDebugLevel >= 4)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parsing file " + strLogFilePath);
                }


                System.IO.StreamReader srInFile = null;
                string strLineIn = null;
                int intLinesRead = 0;

                eProgressSteps eProgress = eProgressSteps.Starting;

                bool blnMatchFound = false;
                int intTotalDatasets = 0;
                int intDatasetsLoaded = 0;
                int intDatasetsAligned = 0;
                int intChargeStatesClustered = 0;

                // Open the file for read; don't lock it (to thus allow MultiAlign to still write to it)
                srInFile = new System.IO.StreamReader(new System.IO.FileStream(strLogFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite));

                intLinesRead = 0;
                while (!srInFile.EndOfStream)
                {
                    strLineIn = srInFile.ReadLine();
                    intLinesRead += 1;

                    if (!string.IsNullOrWhiteSpace(strLineIn))
                    {
                        blnMatchFound = false;

                        // Update progress if the line contains any of the entries in mProgressStepLogText
                        foreach (System.Collections.Generic.KeyValuePair<string, eProgressSteps> lstItem in mProgressStepLogText)
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

                srInFile.Close();

                // Compute the actual progress
                Int16 intActualProgress = default(Int16);

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
                            dblSubProgressPercent = intDatasetsLoaded * 100 / intTotalDatasets;

                        }
                        else if (eProgress == eProgressSteps.AligningDatasets)
                        {
                            dblSubProgressPercent = intDatasetsAligned * 100 / intTotalDatasets;

                        }
                        else if (eProgress == eProgressSteps.PerformingClustering)
                        {
                            // The majority of the data will be charge 1 through 7
                            // Thus, we're dividing by 7 here, which means dblSubProgressPercent might be larger than 100; we'll account for that below
                            dblSubProgressPercent = intChargeStatesClustered * 100 / 7;
                        }

                        if (dblSubProgressPercent > 0)
                        {
                            if (dblSubProgressPercent > 100)
                                dblSubProgressPercent = 100;

                            // Bump up dblActualProgress based on dblSubProgressPercent
                            Int16 intProgressNext = default(Int16);

                            if (mProgressStepPercentComplete.TryGetValue(eProgress + 1, out intProgressNext))
                            {
                                sngActualProgress += Convert.ToSingle(dblSubProgressPercent * (intProgressNext - intActualProgress) / 100.0);
                            }

                        }

                    }

                    if (mProgress < sngActualProgress)
                    {
                        mProgress = sngActualProgress;

                        if (mDebugLevel >= 3 || System.DateTime.UtcNow.Subtract(dtLastProgressWriteTime).TotalMinutes >= 10)
                        {
                            dtLastProgressWriteTime = System.DateTime.UtcNow;
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, " ... " + mProgress.ToString("0.0") + "% complete");
                        }

                    }
                }

            }
            catch (Exception ex)
            {
                // Ignore errors here
                if (mDebugLevel >= 2)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Error parsing MultiAlign log file (" + strLogFilePath + "): " + ex.Message);
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
        public class MageMultiAlign : ContentFilter {

            #region Member Variables

            protected string[] jobFieldNames;

            // indexes to look up values for some key job fields
            protected int toolIdx;
            protected int paramFileIdx;
            protected int resultsFldrIdx;

            #endregion

            #region Properties

            public string WorkingDir { get; set; }
            //public string paramFilename { get; set; }

            #endregion

            #region Constructors

            // constructor
            public MageMultiAlign() {

            }

            #endregion

            #region Overrides of Mage ContentFilter

            // set up internal references
            protected override void ColumnDefsFinished() {
                // get array of column names
                List<string> cols = new List<string>();
                foreach (MageColumnDef colDef in this.InputColumnDefs) {
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
            private void ExtractResultsForJob(BaseModule currentJob, ExtractionType extractionParms, string extractedResultsFileName) {
                // search job results folders for list of results files to process and accumulate into buffer module
                SimpleSink fileList = new SimpleSink();
                ProcessingPipeline plof = ExtractionPipelines.MakePipelineToGetListOfFiles(currentJob, fileList, extractionParms);
                plof.RunRoot(null);

                // extract contents of files
                DestinationType destination = new DestinationType("File_Output", WorkingDir, extractedResultsFileName);
                ProcessingPipeline pefc = ExtractionPipelines.MakePipelineToExtractFileContents(new SinkWrapper(fileList), extractionParms, destination);
                pefc.RunRoot(null);
            }

            #endregion

            #region MageMultiAlign Utility Methods

            // Build Mage source module containing one job to process
            private BaseModule MakeJobSourceModule(string[] jobFieldNames, object[] jobFields) {
                DataGenerator currentJob = new DataGenerator();
                currentJob.AddAdHocRow = jobFieldNames;
                currentJob.AddAdHocRow = ConvertObjectArrayToStringArray(jobFields);
                return currentJob;
            }

            // Convert array of objects to array of strings
            private static string[] ConvertObjectArrayToStringArray(object[] row) {
                List<string> obj = new List<string>();
                foreach (object fld in row) {
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
        public class MageFileImport : FileContentProcessor {

            #region Properties

            //public string DBTableName { get; set; }
            public string DBFilePath { get; set; }
            public string ImportColumnList { get; set; }

            #endregion

            #region Constructors

            // constructor
            public MageFileImport() {
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
        public class JobParameters {
            protected IJobParams mJobParms;

            public JobParameters(IJobParams jobParms) {
                mJobParms = jobParms;
            }

            public string RequireJobParam(string paramName) {
                string val = mJobParms.GetParam(paramName);
                if (string.IsNullOrWhiteSpace(val)) {
                    throw new MageException(string.Format("Required job parameter '{0}' was missing.", paramName));
                }
                return val;
            }

            public string RequireJobParam(string paramName, string defaultValue)
            {
                string val = mJobParms.GetParam(paramName);
                if (string.IsNullOrWhiteSpace(val))
                {
                    mJobParms.AddAdditionalParameter("JobParameters", paramName, defaultValue);
                    return defaultValue;
                }
                return val;
            }
            public string GetJobParam(string paramName) {
                return mJobParms.GetParam(paramName);
            }

            public string GetJobParam(string paramName, string defaultValue) {
                string val = mJobParms.GetParam(paramName);
                if (string.IsNullOrWhiteSpace(val))
                    val = defaultValue;
                return val;
            }
        }

        // class for managing IMgrParams object
        public class ManagerParameters {
            protected IMgrParams mMgrParms;

            public ManagerParameters(IMgrParams mgrParms) {
                mMgrParms = mgrParms;
            }

            public string RequireMgrParam(string paramName) {
                string val = mMgrParms.GetParam(paramName);
                if (string.IsNullOrWhiteSpace(val)) {
                    throw new MageException(string.Format("Required manager parameter '{0}' was missing.", paramName));
                }
                return val;
            }
        }


        #endregion
    }
}
