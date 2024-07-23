using AnalysisManagerBase;
using AScore_DLL;
using AScore_DLL.Managers;
using AScore_DLL.Managers.PSM_Managers;
using AScore_DLL.Managers.SpectraManagers;
using Mage;
using MageExtExtractionFilters;
using MyEMSLReader;
using PRISM;
using PRISM.Logging;
using System;
using System.IO;
using System.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.FileAndDirectoryTools;
using PRISMDatabaseUtils;

namespace AnalysisManager_AScore_PlugIn
{
    /// <summary>
    /// This is a Mage module that does AScore processing
    /// of results for jobs that are supplied to it via standard tabular input
    /// </summary>
    public class MageAScoreModule : ContentFilter
    {
        // Ignore Spelling: cid, dta, etd, hcd, InputColumnPos, Mage, msgfdb, msgfplus, sequest, xtandem

        /// <summary>
        /// Base name for AScore output files
        /// </summary>
        public const string ASCORE_OUTPUT_FILE_NAME_BASE = "AScoreFile";

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

        private DotNetZipTools mDotNetZipTools;

        public ExtractionType ExtractionParams { get; set; }
        public string ExtractedResultsFileName { get; set; }
        public bool TraceMode { get; set; }
        public string WorkingDir { get; set; }
        public string ResultsDBFileName { get; set; }
        public string SearchType { get; set; }
        public string AscoreParamFileName { get; set; }

        public string FastaFilePath { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString"></param>
        public MageAScoreModule(string connectionString)
        {
            mConnectionString = connectionString;
            ExtractedResultsFileName = "extracted_results.txt";
        }

        public void Initialize(DotNetZipTools dotNetZipTools)
        {
            mDotNetZipTools = dotNetZipTools;
        }

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
                {
                    fragType = "hcd";
                }
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
                var paramFileToUse = Path.Combine(WorkingDir, Path.GetFileNameWithoutExtension(AscoreParamFileName) + "_" + fragType + ".xml");

                if (!File.Exists(paramFileToUse))
                {
                    var warningMessage = "Parameter file not found: " + paramFileToUse;
                    OnWarningMessage(new MageStatusEventArgs(warningMessage));
                    Console.WriteLine(warningMessage);

                    var paramFileToUse2 = Path.Combine(WorkingDir, AscoreParamFileName);

                    if (Path.GetExtension(paramFileToUse2).Length == 0)
                        paramFileToUse2 += ".xml";

                    if (File.Exists(paramFileToUse2))
                    {
                        var msg = " ... will instead use: " + paramFileToUse2;
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

                var peptideMassCalculator = new PHRPReader.PeptideMassCalculator();

                var spectraCache = new SpectraManagerCache(peptideMassCalculator);
                RegisterEvents(spectraCache);

                spectraCache.OpenFile(dtaFile);

                PsmResultsManager psmResultsManager;

                switch (SearchType)
                {
                    case "xtandem":
                        psmResultsManager = new XTandemFHT(fhtFile);
                        break;

                    case "sequest":
                        psmResultsManager = new SequestFHT(fhtFile);
                        break;

                    case "inspect":
                        psmResultsManager = new InspectFHT(fhtFile);
                        break;

                    case "msgfdb":
                    case "msgfplus":
                        psmResultsManager = new MsgfdbFHT(fhtFile);
                        break;

                    default:
                        Console.WriteLine("Incorrect search type check again");
                        return false;
                }

                // Make the call to AScore
                var ascoreEngine = new AScoreProcessor();

                RegisterEvents(ascoreEngine);

                ascoreEngine.RunAScoreOnSingleFile(spectraCache, psmResultsManager, ascoreParameters, ascoreOutputFilePath, FastaFilePath);

                Console.WriteLine();

                // Confirm that AScore created the output file
                var ascoreFile = new FileInfo(ascoreOutputFilePath);

                if (ascoreFile.Exists)
                {
                    // Look for the _ProteinMap.txt file
                    // AScore will create that file if a valid FastaFile is defined
                    var proteinMap = new FileInfo(Path.Combine(WorkingDir, Path.GetFileNameWithoutExtension(ascoreFile.Name) + "_ProteinMap.txt"));

                    if (proteinMap.Exists && proteinMap.Length > ascoreFile.Length)
                        ascoreFile = proteinMap;

                    // load AScore results into SQLite database
                    const string tableName = "T_Results_AScore";
                    var dbFilePath = Path.Combine(WorkingDir, ResultsDBFileName);
                    AScoreMagePipeline.ImportFileToSQLite(ascoreFile.FullName, dbFilePath, tableName);
                }

                if (File.Exists(ascoreOutputFilePath))
                {
                    try
                    {
                        AnalysisToolRunnerBase.DeleteFileWithRetries(ascoreOutputFilePath, debugLevel: 1, maxRetryCount: 2);
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
                LogTools.LogError("Exception in AScoreMage.CheckFilter: " + ex.Message, ex);
                Console.WriteLine(ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Build and run Mage pipeline to extract contents of job
        /// </summary>
        /// <param name="currentJob"></param>
        /// <param name="extractionParams"></param>
        /// <param name="extractedResultsFileName"></param>
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

        /// <summary>
        /// Look for "_dta.zip" file in job results directory and copy it to working directory and unzip it
        /// </summary>
        /// <param name="datasetName"></param>
        /// <param name="resultsDirectoryPath"></param>
        /// <param name="jobNumber"></param>
        /// <param name="toolName"></param>
        /// <param name="connectionString"></param>
        /// <returns>Full path to the _dta.txt file in the working directory</returns>
        private string CopyDTAResults(string datasetName, string resultsDirectoryPath, int jobNumber, string toolName, string connectionString)
        {
            string dtaZipPathLocal;

            var resultsDirectory = new DirectoryInfo(resultsDirectoryPath);

            if (resultsDirectoryPath.StartsWith(AnalysisResources.MYEMSL_PATH_FLAG))
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
                AppUtils.GarbageCollectNow();

                AnalysisToolRunnerBase.DeleteFileWithRetries(dtaZipPathLocal, debugLevel: 1, maxRetryCount: 2);
            }
            catch (Exception ex)
            {
                LogTools.LogWarning("Unable to delete _dta.zip file: " + ex.Message);
            }

            return Path.ChangeExtension(dtaZipPathLocal, ".txt");
        }

        private string CopyDtaResultsFromMyEMSL(string datasetName, FileSystemInfo resultsDirectory, int jobNumber, string toolName, string connectionString)
        {
            AScoreMagePipeline.mMyEMSLDatasetInfo.AddDataset(datasetName);

            var archiveFiles = AScoreMagePipeline.mMyEMSLDatasetInfo.FindFiles("*_dta.zip", resultsDirectory.Name, datasetName);

            if (archiveFiles.Count == 0)
            {
                // Lookup the shared results directory name
                var dtaDirectoryName = GetSharedResultsDirectoryName(jobNumber, toolName, connectionString);

                if (string.IsNullOrEmpty(dtaDirectoryName))
                {
                    // Error has already been logged
                    return null;
                }

                archiveFiles = AScoreMagePipeline.mMyEMSLDatasetInfo.FindFiles("*_dta.zip", dtaDirectoryName, datasetName);

                if (archiveFiles.Count == 0)
                {
                    LogTools.LogError("DTA file not found in directory " + dtaDirectoryName + " in MyEMSL");
                    return null;
                }
            }

            AScoreMagePipeline.mMyEMSLDatasetInfo.AddFileToDownloadQueue(archiveFiles.First().FileInfo);

            if (!AScoreMagePipeline.mMyEMSLDatasetInfo.ProcessDownloadQueue(WorkingDir, Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                LogTools.LogError("Error downloading the _DTA.zip file from MyEMSL");
                return null;
            }

            var dtaZipPathLocal = Path.Combine(WorkingDir, archiveFiles.First().FileInfo.Filename);

            return dtaZipPathLocal;
        }

        private string CopyDTAResultsFromServer(DirectoryInfo resultsDirectory, int jobNumber, string toolName, string connectionString)
        {
            // Check if the dta is in the search tool's directory
            string dtaZipSourceFilePath;

            var files = resultsDirectory.GetFiles("*_dta.zip").ToList();

            if (files.Count > 0)
            {
                dtaZipSourceFilePath = files.First().FullName;
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

                files = alternateDtaDirectory.GetFiles("*_dta.zip").ToList();

                if (files.Count == 0)
                {
                    LogTools.LogError("DTA file not found in directory " + alternateDtaDirectory.FullName);
                    return null;
                }

                dtaZipSourceFilePath = files.First().FullName;
            }

            var dtaZipRemote = new FileInfo(dtaZipSourceFilePath);
            var dtaZipPathLocal = Path.Combine(WorkingDir, dtaZipRemote.Name);

            // Copy the DTA file locally, overwriting if it already exists
            dtaZipRemote.CopyTo(dtaZipPathLocal, true);

            return dtaZipPathLocal;
        }

        /// <summary>
        /// Lookup the shared results directory name for the given job
        /// </summary>
        /// <param name="jobNumber"></param>
        /// <param name="toolName"></param>
        /// <param name="connectionString"></param>
        private string GetSharedResultsDirectoryName(int jobNumber, string toolName, string connectionString)
        {
            try
            {
                // If the tool name is MSGFPlus_MzML, use MSGFPlus when querying V_Job_Steps_Export

                var toolNameToUse = toolName.EndsWith("_MzML", StringComparison.OrdinalIgnoreCase)
                    ? toolName.Substring(0, toolName.Length - "_MzML".Length)
                    : toolName;

                var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
                var sqlWhere = string.Format("job = {0} AND tool LIKE '%{1}%' AND (Coalesce(input_folder, '') <> '')", jobNumber, toolNameToUse);

                var serverType = DbToolsFactory.GetServerTypeFromConnectionString(connectionString);

                var schemaName = serverType == DbServerTypes.PostgreSQL ? "sw" : "DMS_Pipeline.dbo";

                var sqlQuery = string.Format(
                    "SELECT input_folder, 1 AS preference, '{0}' AS saved FROM {1}.V_Job_Steps_Export WHERE {2} " +
                    "UNION " +
                    "SELECT input_folder, 2 AS preference, saved FROM {1}.V_Job_Steps_History_Export WHERE {2} " +
                    "ORDER BY preference, saved",
                    timestamp, schemaName, sqlWhere);

                var dbTools = DbToolsFactory.GetDBTools(connectionString, debugMode: TraceMode);
                RegisterEvents(dbTools);

                var success = Global.GetQueryResultsTopRow(dbTools, sqlQuery, out var firstSharedResultsDirectory);

                if (!success || firstSharedResultsDirectory.Count == 0)
                {
                    LogTools.LogError("Cannot determine shared results directory; match not found for job " + jobNumber + " and tool " + toolNameToUse + " in V_Job_Steps_Export or V_Job_Steps_History_Export");
                    return string.Empty;
                }

                // Return the first column (the Input_Folder name)
                return firstSharedResultsDirectory.First();
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

        /// <summary>
        /// Report an error
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex">Exception (allowed to be nothing)</param>
        private void OnErrorEvent(string message, Exception ex)
        {
            ErrorEvent?.Invoke(message, ex);
        }

        /// <summary>
        /// Report a warning
        /// </summary>
        /// <param name="message"></param>
        private void OnWarningEvent(string message)
        {
            WarningEvent?.Invoke(message);
        }

        /// <summary>Use this method to chain events between classes</summary>
        /// <param name="sourceClass"></param>
        private void RegisterEvents(IEventNotifier sourceClass)
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
    }
}
