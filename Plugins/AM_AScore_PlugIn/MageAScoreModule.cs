using AnalysisManagerBase;
using AScore_DLL;
using AScore_DLL.Managers;
using AScore_DLL.Managers.DatasetManagers;
using AScore_DLL.Managers.SpectraManagers;
using Mage;
using MageExtExtractionFilters;
using MyEMSLReader;
using PRISM;
using PRISM.Logging;
using System;
using System.IO;
using System.Linq;

namespace AnalysisManager_AScore_PlugIn
{
    /// <summary>
    /// This is a Mage module that does AScore processing
    /// of results for jobs that are supplied to it via standard tabular input
    /// </summary>
    public class MageAScoreModule : ContentFilter
    {
        #region Constants

        /// <summary>
        /// Base name for AScore output files
        /// </summary>
        public const string ASCORE_OUTPUT_FILE_NAME_BASE = "AScoreFile";

        #endregion

        #region Member Variables

        private readonly string mConnectionString;

        private string[] jobFieldNames;

        // indexes to look up values for some key job fields
        private int jobIdx;
        private int toolIdx;
        private int paramFileIdx;
        private int resultsDirectoryIdx;
        private int datasetNameIdx;
        private int datasetTypeIdx;
        private int settingsFileIdx;

        private clsDotNetZipTools mDotNetZipTools;

        #endregion

        #region Properties

        public ExtractionType ExtractionParams { get; set; }
        public string ExtractedResultsFileName { get; set; }
        public string WorkingDir { get; set; }
        public string ResultsDBFileName { get; set; }
        public string searchType { get; set; }
        public string ascoreParamFileName { get; set; }

        public string FastaFilePath { get; set; }

        #endregion

        #region Constructors

        // constructor
        public MageAScoreModule(string connectionString)
        {
            mConnectionString = connectionString;
            ExtractedResultsFileName = "extracted_results.txt";
        }

        public void Initialize(clsDotNetZipTools dotNetZipTools)
        {
            mDotNetZipTools = dotNetZipTools;
        }

        #endregion

        #region Overrides of Mage ContentFilter

        /// <summary>
        /// Set up internal references
        /// </summary>
        protected override void ColumnDefsFinished()
        {
            // get array of column names
            jobFieldNames = InputColumnDefs.Select(colDef => colDef.Name).ToArray();

            // set up column indexes
            jobIdx = InputColumnPos["Job"];
            toolIdx = InputColumnPos["Tool"];
            paramFileIdx = InputColumnPos["Parameter_File"];

            if (InputColumnPos.ContainsKey("Folder"))
                resultsDirectoryIdx = InputColumnPos["Folder"];
            else if (InputColumnPos.ContainsKey("Directory"))
                resultsDirectoryIdx = InputColumnPos["Directory"];
            else
                throw new Exception("Dictionary InputColumnPos does not have Directory or Folder; cannot continue in MageAScoreModule");

            datasetNameIdx = InputColumnPos["Dataset"];
            datasetTypeIdx = InputColumnPos["Dataset_Type"];
            settingsFileIdx = InputColumnPos["Settings_File"];
        }

        /// <summary>
        /// Process the job described by the fields in the input values object
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        protected override bool CheckFilter(ref string[] values)
        {

            try
            {
                // extract contents of results file for current job to local file in working directory
                var currentJob = MakeJobSourceModule(jobFieldNames, values);
                ExtractResultsForJob(currentJob, ExtractionParams, ExtractedResultsFileName);

                // copy DTA file for current job to working directory
                var jobText = values[jobIdx];

                if (!int.TryParse(jobText, out var jobNumber))
                {
                    LogTools.LogError("Job number is not numeric: " + jobText);
                    return false;
                }

                var resultsDirectoryPath = values[resultsDirectoryIdx];
                var paramFileNameForPSMTool = values[paramFileIdx];
                var datasetName = values[datasetNameIdx];
                var datasetType = values[datasetTypeIdx];
                var analysisTool = values[toolIdx];

                var dtaFilePath = CopyDTAResults(datasetName, resultsDirectoryPath, jobNumber, analysisTool, mConnectionString);
                if (string.IsNullOrEmpty(dtaFilePath))
                {
                    return false;
                }

                string fragType;
                if (datasetType.IndexOf("HCD", StringComparison.OrdinalIgnoreCase) > 0)
                    fragType = "hcd";
                else if (datasetType.IndexOf("ETD", StringComparison.OrdinalIgnoreCase) > 0)
                {
                    fragType = "etd";
                }
                else
                {
                    var settingsFileName = values[settingsFileIdx];
                    var findFragmentation = (paramFileNameForPSMTool + "_" + settingsFileName).ToLower();
                    if (findFragmentation.Contains("hcd"))
                    {
                        fragType = "hcd";
                    }
                    else if (findFragmentation.Contains("etd"))
                    {
                        fragType = "etd";
                    }
                    else
                    {
                        fragType = "cid";
                    }
                }


                // process extracted results file and DTA file with AScore
                const string ascoreOutputFile = ASCORE_OUTPUT_FILE_NAME_BASE + ".txt";
                var ascoreOutputFilePath = Path.Combine(WorkingDir, ascoreOutputFile);

                var fhtFile = Path.Combine(WorkingDir, ExtractedResultsFileName);
                var dtaFile = Path.Combine(WorkingDir, dtaFilePath);
                var paramFileToUse = Path.Combine(WorkingDir, Path.GetFileNameWithoutExtension(ascoreParamFileName) + "_" + fragType + ".xml");

                if (!File.Exists(paramFileToUse))
                {
                    var msg = "Parameter file not found: " + paramFileToUse;
                    OnWarningMessage(new MageStatusEventArgs(msg));
                    Console.WriteLine(msg);

                    var paramFileToUse2 = Path.Combine(WorkingDir, ascoreParamFileName);
                    if (Path.GetExtension(paramFileToUse2).Length == 0)
                        paramFileToUse2 += ".xml";

                    if (File.Exists(paramFileToUse2))
                    {
                        msg = " ... will instead use: " + paramFileToUse2;
                        OnWarningMessage(new MageStatusEventArgs(msg));
                        Console.WriteLine(msg);
                        paramFileToUse = paramFileToUse2;
                    }
                    else
                    {
                        throw new FileNotFoundException("Parameter file not found: " + paramFileToUse);
                    }
                }

                var ascoreParameters = new ParameterFileManager(paramFileToUse);
                RegisterEvents(ascoreParameters);

                var peptideMassCalculator = new PHRPReader.clsPeptideMassCalculator();

                var spectraCache = new SpectraManagerCache(peptideMassCalculator);
                RegisterEvents(spectraCache);

                spectraCache.OpenFile(dtaFile);

                DatasetManager datasetManager;

                switch (searchType)
                {
                    case "xtandem":
                        datasetManager = new XTandemFHT(fhtFile);
                        break;
                    case "sequest":
                        datasetManager = new SequestFHT(fhtFile);
                        break;
                    case "inspect":
                        datasetManager = new InspectFHT(fhtFile);
                        break;
                    case "msgfdb":
                    case "msgfplus":
                        datasetManager = new MsgfdbFHT(fhtFile);
                        break;
                    default:
                        Console.WriteLine("Incorrect search type check again");
                        return false;
                }

                // Make the call to AScore
                var ascoreEngine = new AScoreProcessor();

                RegisterEvents(ascoreEngine);

                ascoreEngine.RunAScoreOnSingleFile(spectraCache, datasetManager, ascoreParameters, ascoreOutputFilePath, FastaFilePath);

                Console.WriteLine();

                // Confirm that AScore created the output file
                var fiAScoreFile = new FileInfo(ascoreOutputFilePath);
                if (fiAScoreFile.Exists)
                {
                    // Look for the _ProteinMap.txt file
                    // Ascore will create that file if a valid FastaFile is defined
                    var fiProteinMap = new FileInfo(Path.Combine(WorkingDir, Path.GetFileNameWithoutExtension(fiAScoreFile.Name) + "_ProteinMap.txt"));
                    if (fiProteinMap.Exists && fiProteinMap.Length > fiAScoreFile.Length)
                        fiAScoreFile = fiProteinMap;

                    // load AScore results into SQLite database
                    const string tableName = "t_results_ascore";
                    var dbFilePath = Path.Combine(WorkingDir, ResultsDBFileName);
                    clsAScoreMagePipeline.ImportFileToSQLite(fiAScoreFile.FullName, dbFilePath, tableName);
                }

                if (File.Exists(ascoreOutputFilePath))
                {
                    try
                    {
                        clsAnalysisToolRunnerBase.DeleteFileWithRetries(ascoreOutputFilePath, debugLevel: 1, maxRetryCount: 2);
                    }
                    catch (Exception ex)
                    {
                        LogTools.LogError("Error deleting file " + Path.GetFileName(ascoreOutputFilePath) +
                                 " (" + ex.Message + "); may lead to duplicate values in Results.db3", ex);
                    }

                }

                // Delete extracted_results file and DTA file
                if (File.Exists(fhtFile))
                {
                    File.Delete(fhtFile);
                }

                if (File.Exists(dtaFilePath))
                {
                    File.Delete(dtaFilePath);
                }

                return true;
            }
            catch (Exception ex)
            {
                LogTools.LogError("Exception in clsAScoreMage.CheckFilter: " + ex.Message, ex);
                Console.WriteLine(ex.Message);
                throw;
            }

        }

        #endregion

        #region MageAScore Mage Pipelines

        // Build and run Mage pipeline to to extract contents of job
        private void ExtractResultsForJob(BaseModule currentJob, ExtractionType extractionParams, string extractedResultsFileName)
        {
            // search job result directories for list of results files to process and accumulate into buffer module
            var fileList = new SimpleSink();
            var pgFileList = ExtractionPipelines.MakePipelineToGetListOfFiles(currentJob, fileList, extractionParams);
            pgFileList.RunRoot(null);

            // add job metadata to results database via a Mage pipeline
            var resultsDBPath = Path.Combine(WorkingDir, ResultsDBFileName);
            var resultsDB = new DestinationType("SQLite_Output", resultsDBPath, "t_results_metadata");
            var peJobMetadata = ExtractionPipelines.MakePipelineToExportJobMetadata(currentJob, resultsDB);
            peJobMetadata.RunRoot(null);

            // add file metadata to results database via a Mage pipeline
            resultsDB = new DestinationType("SQLite_Output", resultsDBPath, "t_results_file_list");
            var peFileMetadata = ExtractionPipelines.MakePipelineToExportJobMetadata(new SinkWrapper(fileList), resultsDB);
            peFileMetadata.RunRoot(null);

            // Extract contents of files
            var destination = new DestinationType("File_Output", WorkingDir, extractedResultsFileName);
            var peFileContents = ExtractionPipelines.MakePipelineToExtractFileContents(new SinkWrapper(fileList), extractionParams, destination);
            peFileContents.RunRoot(null);
        }

        #endregion

        #region MageAScore Utility Methods

        // look for "_dta.zip" file in job results directory and copy it to working directory and unzip it
        private string CopyDTAResults(string datasetName, string resultsDirectoryPath, int jobNumber, string toolName, string connectionString)
        {
            string dtaZipPathLocal;

            var resultsDirectory = new DirectoryInfo(resultsDirectoryPath);

            if (resultsDirectoryPath.StartsWith(clsAnalysisResources.MYEMSL_PATH_FLAG))
            {
                // Need to retrieve the _DTA.zip file from MyEMSL
                dtaZipPathLocal = CopyDtaResultsFromMyEMSL(datasetName, resultsDirectory, jobNumber, toolName, connectionString);
            }
            else
            {
                dtaZipPathLocal = CopyDTAResultsFromServer(resultsDirectory, jobNumber, toolName, connectionString);
            }

            // If we have changed the string from empty we have found the correct _dta.zip file
            if (string.IsNullOrEmpty(dtaZipPathLocal))
            {
                LogTools.LogError("DTA File not found");
                return null;
            }

            try
            {
                // Unzip the file
                mDotNetZipTools.UnzipFile(dtaZipPathLocal);
            }
            catch (Exception ex)
            {
                LogTools.LogError("Exception copying and unzipping _DTA.zip file: " + ex.Message, ex);
                return null;
            }

            try
            {
                // Perform garage collection to force the Unzip tool to release the file handle
                ProgRunner.GarbageCollectNow();

                clsAnalysisToolRunnerBase.DeleteFileWithRetries(dtaZipPathLocal, debugLevel: 1, maxRetryCount: 2);
            }
            catch (Exception ex)
            {
                LogTools.LogWarning("Unable to delete _dta.zip file: " + ex.Message);
            }

            var unzippedDtaResultsFilePath = Path.ChangeExtension(dtaZipPathLocal, ".txt");
            return unzippedDtaResultsFilePath;
        }

        private string CopyDtaResultsFromMyEMSL(string datasetName, FileSystemInfo resultsDirectory, int jobNumber, string toolName, string connectionString)
        {
            clsAScoreMagePipeline.mMyEMSLDatasetInfo.AddDataset(datasetName);
            var lstArchiveFiles = clsAScoreMagePipeline.mMyEMSLDatasetInfo.FindFiles("*_dta.zip", resultsDirectory.Name, datasetName);

            if (lstArchiveFiles.Count == 0)
            {
                // Lookup the shared results directory name
                var dtaDirectoryName = GetSharedResultsDirectoryName(jobNumber, toolName, connectionString);

                if (string.IsNullOrEmpty(dtaDirectoryName))
                {
                    // Error has already been logged
                    return null;
                }

                lstArchiveFiles = clsAScoreMagePipeline.mMyEMSLDatasetInfo.FindFiles("*_dta.zip", dtaDirectoryName, datasetName);

                if (lstArchiveFiles.Count == 0)
                {
                    LogTools.LogError("DTA file not found in directory " + dtaDirectoryName + " in MyEMSL");
                    return null;
                }
            }

            clsAScoreMagePipeline.mMyEMSLDatasetInfo.AddFileToDownloadQueue(lstArchiveFiles.First().FileInfo);

            if (!clsAScoreMagePipeline.mMyEMSLDatasetInfo.ProcessDownloadQueue(WorkingDir, Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                LogTools.LogError("Error downloading the _DTA.zip file from MyEMSL");
                return null;
            }

            var dtaZipPathLocal = Path.Combine(WorkingDir, lstArchiveFiles.First().FileInfo.Filename);

            return dtaZipPathLocal;
        }

        private string CopyDTAResultsFromServer(DirectoryInfo resultsDirectory, int jobNumber, string toolName, string connectionString)
        {
            // Check if the dta is in the search tool's directory
            string dtaZipSourceFilePath;

            var lstFiles = resultsDirectory.GetFiles("*_dta.zip").ToList();
            if (lstFiles.Count > 0)
            {
                dtaZipSourceFilePath = lstFiles.First().FullName;
            }
            else
            {
                // File not found
                // Prior to January 2015 we would examine the JobParameters file to determine the appropriate dta directory (by looking for parameter SharedResultsFolders)
                // That method is not reliable, so we instead now query V_Job_Steps and V_Job_Steps_History

                var dtaDirectoryName = GetSharedResultsDirectoryName(jobNumber, toolName, connectionString);

                if (string.IsNullOrEmpty(dtaDirectoryName))
                {
                    // Error has already been logged
                    return null;
                }

                if (resultsDirectory.Parent == null || !resultsDirectory.Parent.Exists)
                {
                    LogTools.LogError("DTA directory not found; " + resultsDirectory.FullName + " does not have a parent directory");
                    return null;
                }

                var alternateDtaDirectory = new DirectoryInfo(Path.Combine(resultsDirectory.Parent.FullName, dtaDirectoryName));
                if (!alternateDtaDirectory.Exists)
                {
                    LogTools.LogError("DTA directory not found: " + alternateDtaDirectory.FullName);
                    return null;
                }

                lstFiles = alternateDtaDirectory.GetFiles("*_dta.zip").ToList();
                if (lstFiles.Count == 0)
                {
                    LogTools.LogError("DTA file not found in directory " + alternateDtaDirectory.FullName);
                    return null;
                }

                dtaZipSourceFilePath = lstFiles.First().FullName;
            }

            var fiDtaZipRemote = new FileInfo(dtaZipSourceFilePath);
            var dtaZipPathLocal = Path.Combine(WorkingDir, fiDtaZipRemote.Name);

            // Copy the DTA file locally, overwriting if it already exists
            fiDtaZipRemote.CopyTo(dtaZipPathLocal, true);

            return dtaZipPathLocal;
        }

        /// <summary>
        /// Lookup the shared results directory name for the given job
        /// </summary>
        /// <param name="jobNumber"></param>
        /// <param name="toolName"></param>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        private string GetSharedResultsDirectoryName(int jobNumber, string toolName, string connectionString)
        {
            try
            {
                var sqlWhere = "WHERE Job = " + jobNumber + " AND Tool LIKE '%" + toolName + "%' AND (ISNULL(Input_Folder, '') <> '')";

                var sqlQuery = "";
                sqlQuery += " SELECT Input_Folder, 1 AS Preference, GetDate() AS Saved FROM DMS_Pipeline.dbo.V_Job_Steps " + sqlWhere;
                sqlQuery += " UNION ";
                sqlQuery += " SELECT Input_Folder, 2 AS Preference, Saved FROM DMS_Pipeline.dbo.V_Job_Steps_History " + sqlWhere;
                sqlQuery += " ORDER BY Preference, saved";

                var success = clsGlobal.GetQueryResultsTopRow(sqlQuery, connectionString, out var firstSharedResultsDirectory, "GetSharedResultsDirectoryName");

                if (!success || firstSharedResultsDirectory.Count == 0)
                {
                    LogTools.LogError("Cannot determine shared results directory; match not found for job " + jobNumber + " and tool " + toolName + " in V_Job_Steps or V_Job_Steps_History");
                    return string.Empty;

                }

                // Return the first column (the Input_Folder name)
                var sharedResultsDirectory = firstSharedResultsDirectory.First();
                return sharedResultsDirectory;

            }
            catch (Exception ex)
            {
                LogTools.LogError("Error looking up the input directory for job " + jobNumber + " and tool " + toolName +
                         " in GetSharedResultsDirectoryName: " + ex.Message, ex);
                return string.Empty;
            }


        }

        // Build Mage source module containing one job to process
        private BaseModule MakeJobSourceModule(string[] jobFieldNameList, string[] jobFields)
        {
            var currentJob = new DataGenerator
            {
                AddAdHocRow = jobFieldNameList
            };
            currentJob.AddAdHocRow = jobFields;
            return currentJob;
        }


        #endregion

        #region "Event handlers and methods"

        /// <summary>
        /// Report an error
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex">Exception (allowed to be nothing)</param>
        protected void OnErrorEvent(string message, Exception ex)
        {
            ErrorEvent?.Invoke(message, ex);
        }

        /// <summary>
        /// Report a warning
        /// </summary>
        /// <param name="message"></param>
        protected void OnWarningEvent(string message)
        {
            WarningEvent?.Invoke(message);
        }

        /// <summary>Use this method to chain events between classes</summary>
        /// <param name="sourceClass"></param>
        protected void RegisterEvents(EventNotifier sourceClass)
        {
            sourceClass.ErrorEvent += OnErrorEvent;
            sourceClass.WarningEvent += OnWarningEvent;
        }

        /// <summary>
        /// Error event
        /// </summary>
        public event ErrorEventEventHandler ErrorEvent;

        /// <summary>Error event</summary>
        /// <param name="message"></param>
        /// <param name="ex"></param>
        public delegate void ErrorEventEventHandler(string message, Exception ex);

        /// <summary>
        /// Warning event
        /// </summary>
        public event WarningEventEventHandler WarningEvent;

        /// <summary>Warning event</summary>
        /// <param name="message"></param>
        public delegate void WarningEventEventHandler(string message);

        #endregion
    }
}
