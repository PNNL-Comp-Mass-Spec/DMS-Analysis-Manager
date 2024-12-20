using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.FileAndDirectoryTools;
using AScore_DLL.Managers.PSM_Managers;
using AScore_DLL.Managers.SpectraManagers;
using AScore_DLL.Managers;
using AScore_DLL;
using Mage;
using MageExtExtractionFilters;
using MyEMSLReader;
using PRISM.Logging;
using PRISM;
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

        // Indexes to look up values for some key job fields
        private int jobIdx;
        private int toolIdx;
        private int paramFileIdx;
        private int resultsDirectoryIdx;
        private int datasetNameIdx;
        private int datasetTypeIdx;
        private int settingsFileIdx;

        private ZipFileTools mZipTools;

        public ExtractionType ExtractionParams { get; set; }
        public string ExtractedResultsFileName { get; set; }
        public bool TraceMode { get; set; }
        public string WorkingDir { get; set; }
        public string ResultsDBFileName { get; set; }
        public string SearchType { get; set; }

        /// <summary>
        /// Job parameter "AScoreParamFilename", but without the .xml file extension
        /// </summary>
        public string AscoreParamFileName { get; set; }

        public string FastaFilePath { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        public MageAScoreModule(string connectionString)
        {
            mConnectionString = connectionString;
            ExtractedResultsFileName = "extracted_results.txt";
        }

        public void Initialize(ZipFileTools zipTools)
        {
            mZipTools = zipTools;
        }

        /// <summary>
        /// Set up internal references
        /// </summary>
        protected override void ColumnDefsFinished()
        {
            // Get array of column names
            jobFieldNames = InputColumnDefs.Select(colDef => colDef.Name).ToArray();

            // Set up column indexes
            jobIdx = InputColumnPos["Job"];
            toolIdx = InputColumnPos["Tool"];
            paramFileIdx = InputColumnPos["Parameter_File"];

            if (InputColumnPos.TryGetValue("Folder", out var folderColumnIndex))
            {
                resultsDirectoryIdx = folderColumnIndex;
            }
            else if (InputColumnPos.TryGetValue("Directory", out var directoryColumnIndex))
            {
                resultsDirectoryIdx = directoryColumnIndex;
            }
            else
            {
                throw new Exception("Dictionary InputColumnPos does not have Directory or Folder; cannot continue in MageAScoreModule");
            }

            datasetNameIdx = InputColumnPos["Dataset"];
            datasetTypeIdx = InputColumnPos["Dataset_Type"];
            settingsFileIdx = InputColumnPos["Settings_File"];
        }

        /// <summary>
        /// Process the job described by the fields in the input values object
        /// </summary>
        /// <param name="values">Array with metadata for current job</param>
        protected override bool CheckFilter(ref string[] values)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(AscoreParamFileName))
                {
                    throw new FileNotFoundException("AScore parameter file not defined");
                }

                // Extract contents of results file for current job to local file in working directory
                var currentJob = MakeJobSourceModule(jobFieldNames, values);

                ExtractResultsForJob(currentJob, ExtractionParams, ExtractedResultsFileName);

                // Determine the metadata for the current job
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
                var toolName = values[toolIdx];

                // Retrieve the _ModSummary.txt file for the current job
                CopyResultFilesFromServer(resultsDirectoryPath, string.Format("{0}*_ModSummary.txt", datasetName));

                // Retrieve the spectrum file; should be .mzML.gz (previously _dta.zip)
                var spectrumFilePath = DetermineSpectrumFilePath(resultsDirectoryPath, jobNumber, toolName, mConnectionString);

                if (string.IsNullOrWhiteSpace(spectrumFilePath))
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

                // Process extracted results file and DTA file with AScore
                const string ascoreOutputFile = ASCORE_OUTPUT_FILE_NAME_BASE + ".txt";
                var ascoreOutputFilePath = Path.Combine(WorkingDir, ascoreOutputFile);

                var fhtFile = Path.Combine(WorkingDir, ExtractedResultsFileName);
                var paramFileToUse = Path.Combine(WorkingDir, AscoreParamFileName + "_" + fragType + ".xml");

                if (!File.Exists(paramFileToUse))
                {
                    ShowWarningMessage("Parameter file not found: " + paramFileToUse);

                    var paramFileToUse2 = Path.Combine(WorkingDir, AscoreParamFileName + ".xml");

                    if (File.Exists(paramFileToUse2))
                    {
                        ShowWarningMessage(" ... will instead use: " + paramFileToUse2);
                        paramFileToUse = paramFileToUse2;
                    }
                    else
                    {
                        throw new FileNotFoundException(string.Format("Parameter file not found: {0} or {1}", paramFileToUse, paramFileToUse2));
                    }
                }

                var ascoreParameters = new ParameterFileManager(paramFileToUse);
                RegisterEvents(ascoreParameters);

                var peptideMassCalculator = new PHRPReader.PeptideMassCalculator();

                var spectraCache = new SpectraManagerCache(peptideMassCalculator);
                RegisterEvents(spectraCache);

                spectraCache.OpenFile(spectrumFilePath);

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

                    // Load AScore results into SQLite database
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
                        LogTools.LogError(string.Format("Error deleting file {0} ({1}); may lead to duplicate values in Results.db3", Path.GetFileName(ascoreOutputFilePath), ex.Message), ex);
                    }
                }

                // Delete extracted_results file and DTA file
                if (File.Exists(fhtFile))
                {
                    File.Delete(fhtFile);
                }

                spectraCache.CloseFile();

                if (File.Exists(spectrumFilePath))
                {
                    File.Delete(spectrumFilePath);
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
        /// Retrieve the .mzML file or the _dta.txt file for this job
        /// </summary>
        /// <param name="resultsDirectoryPath">Results directory path</param>
        /// <param name="jobNumber">Job number</param>
        /// <param name="toolName">Tool name</param>
        /// <param name="connectionString">Connection string</param>
        /// <returns>Local file path if found, otherwise an empty string</returns>
        private string DetermineSpectrumFilePath(string resultsDirectoryPath, int jobNumber, string toolName, string connectionString)
        {
            var resultsDirectory = new DirectoryInfo(resultsDirectoryPath);

            // Look for the .mzML.gz file in the results directory (it most likely is not there)
            var mzMLFiles = resultsDirectory.GetFiles("*.mzML.gz");

            if (mzMLFiles.Length > 0)
            {
                var mzMLFile = mzMLFiles[0];

                if (!mZipTools.GUnzipFile(mzMLFile.FullName))
                {
                    ShowWarningMessage("Error unzipping " + mzMLFile.Name);
                    return string.Empty;
                }

                return Path.Combine(WorkingDir, Path.GetFileNameWithoutExtension(mzMLFile.Name));
            }

            // Look for the _dta.zip file in the results directory (it most likely is not there)
            var dtaFiles = resultsDirectory.GetFiles("*_dta.zip");

            if (dtaFiles.Length > 0)
            {
                var dtaFile = dtaFiles[0];

                if (!mZipTools.UnzipFile(dtaFile.FullName))
                {
                    ShowWarningMessage("Error unzipping " + dtaFile.Name);
                    return string.Empty;
                }

                return Path.Combine(WorkingDir, Path.GetFileNameWithoutExtension(dtaFile.Name) + ".txt");
            }

            // Look for the .mzML.gz_CacheInfo.txt file, the .mzML_CacheInfo.txt file, or the _dta.zip file in the shared results directory
            var sharedResultsDirectoryNames = GetSharedResultsDirectories(jobNumber, toolName, connectionString);

            if (sharedResultsDirectoryNames.Count == 0)
            {
                // Error has already been logged
                return string.Empty;
            }

            // ReSharper disable once MergeIntoNegatedPattern
            if (resultsDirectory.Parent == null || !resultsDirectory.Parent.Exists)
            {
                LogTools.LogError("Shared results directory not found; " + resultsDirectory.FullName + " does not have a parent directory");
                return string.Empty;
            }

            // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
            foreach (var sharedResultsDirectoryName in sharedResultsDirectoryNames)
            {
                var sharedResultsDirectory = new DirectoryInfo(Path.Combine(resultsDirectory.Parent.FullName, sharedResultsDirectoryName));

                if (!sharedResultsDirectory.Exists)
                {
                    ShowWarningMessage("Shared results directory not found: " + sharedResultsDirectory.FullName);
                    continue;
                }

                var mzRefineryCacheInfoFiles = sharedResultsDirectory.GetFiles("*.mzML.gz_CacheInfo.txt").ToList();

                if (mzRefineryCacheInfoFiles.Count > 0)
                {
                    return CopyMzMLFileFromServer(mzRefineryCacheInfoFiles[0].FullName, jobNumber);
                }

                var cacheInfoFiles = sharedResultsDirectory.GetFiles("*.mzML_CacheInfo.txt").ToList();

                if (cacheInfoFiles.Count > 0)
                {
                    return CopyMzMLFileFromServer(cacheInfoFiles[0].FullName, jobNumber);
                }

                var sharedDtaFiles = sharedResultsDirectory.GetFiles("*_dta.zip").ToList();

                if (sharedDtaFiles.Count == 0)
                    continue;

                var dtaZipPathLocal = CopyDtaResultsFromServer(sharedDtaFiles[0].FullName);

                return ExtractDtaFile(dtaZipPathLocal) ?? string.Empty;
            }

            ShowWarningMessage(string.Format("Could not find the .mzML.gz file or _dta.zip file for job {0}; checked {1} and the shared results directories", jobNumber, resultsDirectory.Name));

            return string.Empty;
        }

        /// <summary>
        /// Build and run Mage pipeline to extract contents of job
        /// </summary>
        /// <param name="currentJob"></param>
        /// <param name="extractionParams"></param>
        /// <param name="extractedResultsFileName"></param>
        private void ExtractResultsForJob(BaseModule currentJob, ExtractionType extractionParams, string extractedResultsFileName)
        {
            // Search job result directories for list of results files to process and accumulate into buffer module
            var fileList = new SimpleSink();
            var pgFileList = ExtractionPipelines.MakePipelineToGetListOfFiles(currentJob, fileList, extractionParams);
            pgFileList.RunRoot(null);

            // Add job metadata to results database via a Mage pipeline
            var resultsDBPath = Path.Combine(WorkingDir, ResultsDBFileName);
            var resultsDB = new DestinationType("SQLite_Output", resultsDBPath, "t_results_metadata");
            var peJobMetadata = ExtractionPipelines.MakePipelineToExportJobMetadata(currentJob, resultsDB);
            peJobMetadata.RunRoot(null);

            // Add file metadata to results database via a Mage pipeline
            resultsDB = new DestinationType("SQLite_Output", resultsDBPath, "t_results_file_list");
            var peFileMetadata = ExtractionPipelines.MakePipelineToExportJobMetadata(new SinkWrapper(fileList), resultsDB);
            peFileMetadata.RunRoot(null);

            // Extract contents of files
            var destination = new DestinationType("File_Output", WorkingDir, extractedResultsFileName);
            var peFileContents = ExtractionPipelines.MakePipelineToExtractFileContents(new SinkWrapper(fileList), extractionParams, destination);
            peFileContents.RunRoot(null);
        }

        /// <summary>
        /// Look for "_dta.zip" file in the job results directory and copy it to working directory and unzip it
        /// </summary>
        /// <remarks>Also looks for the _dta.zip file in the shared results directory, e.g. DTA_Gen_1_26_955962</remarks>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="resultsDirectoryPath">Results directory path</param>
        /// <param name="jobNumber">Job number</param>
        /// <param name="toolName">Tool name</param>
        /// <param name="connectionString">Connection string</param>
        /// <returns>Full path to the _dta.txt file in the working directory</returns>
        [Obsolete("Superseded by DetermineSpectrumFilePath")]
        private string CopyDtaResults(string datasetName, string resultsDirectoryPath, int jobNumber, string toolName, string connectionString)
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
                dtaZipPathLocal = CopyDtaResultsFromServer(resultsDirectory, jobNumber, toolName, connectionString);
            }

            return ExtractDtaFile(dtaZipPathLocal);
        }

        [Obsolete("Only called from an obsolete method")]
        private string CopyDtaResultsFromMyEMSL(string datasetName, FileSystemInfo resultsDirectory, int jobNumber, string toolName, string connectionString)
        {
            AScoreMagePipeline.mMyEMSLDatasetInfo.AddDataset(datasetName);

            var archiveFiles = AScoreMagePipeline.mMyEMSLDatasetInfo.FindFiles("*_dta.zip", resultsDirectory.Name, datasetName);

            if (archiveFiles.Count == 0)
            {
                // Lookup the shared results directory name
                var dtaDirectoryNames = GetSharedResultsDirectories(jobNumber, toolName, connectionString);

                if (dtaDirectoryNames.Count == 0)
                {
                    // Error has already been logged
                    return string.Empty;
                }

                var dtaDirectoryName = dtaDirectoryNames[0];

                archiveFiles = AScoreMagePipeline.mMyEMSLDatasetInfo.FindFiles("*_dta.zip", dtaDirectoryName, datasetName);

                if (archiveFiles.Count == 0)
                {
                    LogTools.LogError("DTA file not found in directory " + dtaDirectoryName + " in MyEMSL");
                    return string.Empty;
                }
            }

            AScoreMagePipeline.mMyEMSLDatasetInfo.AddFileToDownloadQueue(archiveFiles.First().FileInfo);

            if (!AScoreMagePipeline.mMyEMSLDatasetInfo.ProcessDownloadQueue(WorkingDir, Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                LogTools.LogError("Error downloading the _DTA.zip file from MyEMSL");
                return string.Empty;
            }

            return Path.Combine(WorkingDir, archiveFiles.First().FileInfo.Filename);
        }

        private string CopyDtaResultsFromServer(string dtaZipSourceFilePath)
        {
            var dtaZipRemote = new FileInfo(dtaZipSourceFilePath);
            var dtaZipPathLocal = Path.Combine(WorkingDir, dtaZipRemote.Name);

            // Copy the DTA file locally, overwriting if it already exists
            dtaZipRemote.CopyTo(dtaZipPathLocal, true);

            return dtaZipPathLocal;
        }

        /// <summary>
        /// Copy the _dta.zip file from the server to the working directory
        /// </summary>
        /// <param name="resultsDirectory">Results directory (job results)</param>
        /// <param name="jobNumber">Job number</param>
        /// <param name="toolName">Tool name</param>
        /// <param name="connectionString">Connection String</param>
        /// <returns>Path to the local copy of the _dta.zip file</returns>
        private string CopyDtaResultsFromServer(DirectoryInfo resultsDirectory, int jobNumber, string toolName, string connectionString)
        {
            // Check if the _dta.zip file is in the search results directory
            string dtaZipSourceFilePath;

            var files = resultsDirectory.GetFiles("*_dta.zip").ToList();

            if (files.Count > 0)
            {
                dtaZipSourceFilePath = files[0].FullName;
            }
            else
            {
                // File not found
                // Prior to January 2015 we would examine the JobParameters file to determine the appropriate dta directory (by looking for parameter SharedResultsFolders)
                // That method is not reliable, so we instead now query V_Job_Steps and V_Job_Steps_History

                var dtaDirectoryNames = GetSharedResultsDirectories(jobNumber, toolName, connectionString);

                if (dtaDirectoryNames.Count == 0)
                {
                    // Error has already been logged
                    return string.Empty;
                }

                // ReSharper disable once MergeIntoNegatedPattern
                if (resultsDirectory.Parent == null || !resultsDirectory.Parent.Exists)
                {
                    LogTools.LogError("Shared results directory not found; " + resultsDirectory.FullName + " does not have a parent directory");
                    return string.Empty;
                }

                var alternateDtaDirectory = new DirectoryInfo(Path.Combine(resultsDirectory.Parent.FullName, dtaDirectoryNames[0]));

                if (!alternateDtaDirectory.Exists)
                {
                    LogTools.LogError("Shared results directory not found: " + alternateDtaDirectory.FullName);
                    return string.Empty;
                }

                var sharedResultFiles = alternateDtaDirectory.GetFiles("*_dta.zip").ToList();

                if (sharedResultFiles.Count == 0)
                {
                    LogTools.LogError("DTA file not found in shared results directory " + alternateDtaDirectory.FullName);
                    return string.Empty;
                }

                dtaZipSourceFilePath = sharedResultFiles[0].FullName;
            }

            return CopyDtaResultsFromServer(dtaZipSourceFilePath);
        }

        private string CopyMzMLFileFromServer(string cacheInfoFilePath, int jobNumber)
        {
            try
            {
                using var reader = new StreamReader(new FileStream(cacheInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var sourceFile = new FileInfo(dataLine);

                    if (!sourceFile.Exists)
                    {
                        LogTools.LogError("Unable to retrieve the .mzML file for job {0}; cached .mzML.gz file not found: {1}", jobNumber, sourceFile.FullName);
                        return string.Empty;
                    }

                    var gzipPathLocal = Path.Combine(WorkingDir, sourceFile.Name);

                    ConsoleMsgUtils.ShowDebug("Copying .mzML.gz file to the working directory: " + sourceFile.FullName);

                    sourceFile.CopyTo(gzipPathLocal, true);

                    mZipTools.GUnzipFile(sourceFile.FullName);

                    try
                    {
                        // Perform garbage collection to force the Unzip tool to release the file handle
                        AppUtils.GarbageCollectNow();

                        AnalysisToolRunnerBase.DeleteFileWithRetries(gzipPathLocal, debugLevel: 1, maxRetryCount: 2);
                    }
                    catch (Exception ex)
                    {
                        LogTools.LogWarning("Unable to delete the .mzML.gz file: " + ex.Message);
                    }

                    // Return the path to the .mzML file
                    return gzipPathLocal.Substring(0, gzipPathLocal.Length - 3);
                }

                LogTools.LogError("The .mzML_CacheInfo file for job {0} does not have a .mzML.gz line: {1}", jobNumber, cacheInfoFilePath);
                return string.Empty;
            }
            catch (Exception ex)
            {
                LogTools.LogError(string.Format("Error retrieving the .mzML file for job {0}: {1}", jobNumber, ex.Message), ex);
                return string.Empty;
            }
        }

        /// <summary>
        /// Copy the _dta.zip file from the server to the working directory
        /// </summary>
        /// <param name="resultsDirectoryPath">Results directory (job results)</param>
        /// <param name="searchPattern">Filename search pattern, e.g. DatasetName_ModSummary.txt</param>
        /// <returns>True if one or more files were found and copied to the work directory, otherwise false</returns>
        private void CopyResultFilesFromServer(string resultsDirectoryPath, string searchPattern)
        {
            try
            {
                var resultsDirectory = new DirectoryInfo(resultsDirectoryPath);
                var foundFiles = resultsDirectory.GetFiles(searchPattern).ToList();

                if (foundFiles.Count == 0)
                {
                    return;
                }

                foreach (var sourceFile in foundFiles)
                {
                    var filePathLocal = Path.Combine(WorkingDir, sourceFile.Name);
                    sourceFile.CopyTo(filePathLocal, true);
                }
            }
            catch (Exception ex)
            {
                LogTools.LogError(string.Format("Exception copying files matching {0} from {1}: {2}", searchPattern, resultsDirectoryPath, ex.Message), ex);
            }
        }

        private string ExtractDtaFile(string dtaZipPathLocal)
        {
            if (string.IsNullOrEmpty(dtaZipPathLocal))
            {
                LogTools.LogError("DTA File not found");
                return string.Empty;
            }

            try
            {
                // Unzip the file
                mZipTools.UnzipFile(dtaZipPathLocal);
            }
            catch (Exception ex)
            {
                LogTools.LogError("Exception copying and unzipping _DTA.zip file: " + ex.Message, ex);
                return string.Empty;
            }

            try
            {
                // Perform garbage collection to force the Unzip tool to release the file handle
                AppUtils.GarbageCollectNow();

                AnalysisToolRunnerBase.DeleteFileWithRetries(dtaZipPathLocal, debugLevel: 1, maxRetryCount: 2);
            }
            catch (Exception ex)
            {
                LogTools.LogWarning("Unable to delete the _dta.zip file: " + ex.Message);
            }

            return Path.ChangeExtension(dtaZipPathLocal, ".txt");
        }

        /// <summary>
        /// Lookup the shared results directory name for the given job
        /// </summary>
        /// <param name="jobNumber">Job number</param>
        /// <param name="toolName">Tool name</param>
        /// <param name="connectionString">Connection string</param>
        /// <returns>List of shared results directory names</returns>
        private List<string> GetSharedResultsDirectories(int jobNumber, string toolName, string connectionString)
        {
            try
            {
                // If the tool name is MSGFPlus_MzML, use MSGFPlus when querying V_Job_Steps_Export

                var toolNameToUse = toolName.EndsWith("_MzML", StringComparison.OrdinalIgnoreCase)
                    ? toolName.Substring(0, toolName.Length - "_MzML".Length)
                    : toolName;

                var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");

                var serverType = DbToolsFactory.GetServerTypeFromConnectionString(connectionString);

                var schemaName = serverType == DbServerTypes.PostgreSQL ? "sw" : "DMS_Pipeline.dbo";

                /*
                 * The following could be used to search each of the input_folders for the job steps

                var toolPreferenceSql = string.Format("CASE WHEN tool LIKE '%{0}%' THEN step ELSE step * 100 END as tool_preference", toolNameToUse);
                var sqlWhere = string.Format("job = {0} AND (Coalesce(input_folder, '') <> '')", jobNumber);

                var sqlQuery = string.Format(
                    "SELECT input_folder, {0}, 1 AS history_preference, '{1}' AS saved FROM {2}.V_Job_Steps_Export WHERE {3} " +
                    "UNION " +
                    "SELECT input_folder, {0}, 2 AS history_preference, saved FROM {2}.V_Job_Steps_History_Export WHERE {3} " +
                    "ORDER BY tool_preference, history_preference, saved",
                    toolPreferenceSql, timestamp, schemaName, sqlWhere);
                 */

                var sqlWhere = string.Format("job = {0} AND tool LIKE '%{1}%' AND (Coalesce(input_folder, '') <> '')", jobNumber, toolNameToUse);

                var sqlQuery = string.Format(
                    "SELECT input_folder, 1 AS preference, '{0}' AS saved FROM {1}.V_Job_Steps_Export WHERE {2} " +
                    "UNION " +
                    "SELECT input_folder, 2 AS preference, saved FROM {1}.V_Job_Steps_History_Export WHERE {2} " +
                    "ORDER BY preference, saved",
                    timestamp, schemaName, sqlWhere);

                var dbTools = DbToolsFactory.GetDBTools(connectionString, debugMode: TraceMode);
                RegisterEvents(dbTools);

                var success = dbTools.GetQueryResults(sqlQuery, out var queryResults);

                if (!success || queryResults.Count == 0)
                {
                    LogTools.LogError("Cannot determine shared results directories; match not found for job {0} and tool {1} in V_Job_Steps_Export or V_Job_Steps_History_Export", jobNumber, toolNameToUse);
                    return new List<string>();
                }

                var sharedResultsDirectoryNames = new List<string>();

                foreach (var entry in queryResults)
                {
                    // The first column is the shared results directory name
                    sharedResultsDirectoryNames.Add(entry[0]);
                }

                return sharedResultsDirectoryNames;
            }
            catch (Exception ex)
            {
                LogTools.LogError(string.Format("Error looking up the input directory for job {0} and tool {1} in GetSharedResultsDirectories: {2}", jobNumber, toolName, ex.Message), ex);
                return new List<string>();
            }
        }

        /// <summary>
        /// Build Mage source module containing one job to process
        /// </summary>
        /// <param name="jobFieldNameList"></param>
        /// <param name="jobFields"></param>
        /// <returns></returns>
        private BaseModule MakeJobSourceModule(string[] jobFieldNameList, string[] jobFields)
        {
            var currentJob = new DataGenerator
            {
                AddAdHocRow = jobFieldNameList
            };

            currentJob.AddAdHocRow = jobFields;
            return currentJob;
        }

        private void ShowWarningMessage(string message)
        {
            var warningMessage = message;
            OnWarningMessage(new MageStatusEventArgs(warningMessage));
            Console.WriteLine(warningMessage);
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
