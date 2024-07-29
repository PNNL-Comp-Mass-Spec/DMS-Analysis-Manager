using AnalysisManager_RepoPkgr_PlugIn;
using AnalysisManagerBase;
using AnalysisManagerMsXmlGenPlugIn;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISMDatabaseUtils;

namespace AnalysisManager_RepoPkgr_Plugin
{
    /// <summary>
    /// Class for running the RepoPkgr
    /// </summary>
    public class AnalysisToolRunnerRepoPkgr : AnalysisToolRunnerBase
    {
        // Ignore Spelling: gzip, mage, pre, repo

        private const int PROGRESS_PCT_FASTA_FILES_COPIED = 10;
        private const int PROGRESS_PCT_MSGF_PLUS_RESULTS_COPIED = 25;
        private const int PROGRESS_PCT_SEQUEST_RESULTS_COPIED = 40;
        private const int PROGRESS_PCT_MZID_RESULTS_COPIED = 50;
        private const int PROGRESS_PCT_INSTRUMENT_DATA_COPIED = 95;

        public const string WARNING_INSTRUMENT_DATA_MISSING = "WarningInstrumentDataMissing";

        private bool mIncludeInstrumentData;
        private bool mIncludeSequestResults;
        private bool mIncludeMzXMLFiles;
        private bool mIncludeMSGFPlusResults;
        private bool mIncludeMZidFiles;

        private string mOutputResultsDirectoryPath;

        private MageRepoPkgrPipelines mRepoPackager;

        private string mMSXmlGeneratorAppPath;

        /// <summary>
        /// Primary entry point for running this tool
        /// </summary>
        /// <returns>CloseOutType enum representing completion status</returns>
        public override CloseOutType RunTool()
        {
            try
            {
                // Do the base class stuff
                var result = base.RunTool();

                if (result != CloseOutType.CLOSEOUT_SUCCESS)
                {
                    return result;
                }

                // Store the RepoPkgr version info in the database
                if (!StoreToolVersionInfo())
                {
                    LogError("Aborting since StoreToolVersionInfo returned false");
                    mMessage = "Error determining RepoPkgr version";
                    return CloseOutType.CLOSEOUT_FAILED;
                }

                var instrumentDataWarning = mJobParams.GetJobParameter(WARNING_INSTRUMENT_DATA_MISSING, string.Empty);

                if (!string.IsNullOrEmpty(instrumentDataWarning))
                    mEvalMessage = instrumentDataWarning;

                result = BuildRepoCache();
                return result;
            }
            catch (Exception ex)
            {
                mMessage = "Error in RepoPkgr Plugin->RunTool";
                LogError(mMessage, ex);
                return CloseOutType.CLOSEOUT_FAILED;
            }
        }

        /// <summary>
        /// Find (or generate) necessary files and copy them to repository cache directory for upload
        /// </summary>
        private CloseOutType BuildRepoCache()
        {
            SetOptions();
            SetOutputDirectoryPath();
            SetMagePipelineManager(mOutputResultsDirectoryPath);

            // do operations for repository specified in job parameters
            var targetRepository = mJobParams.GetJobParameter("Repository", "");
            var success = false;

            switch (targetRepository)
            {
                case "PeptideAtlas":
                    success = DoPeptideAtlasOperation();
                    break;
                    // FUTURE: code for other repositories to go here someday
            }

            return success ? CloseOutType.CLOSEOUT_SUCCESS : CloseOutType.CLOSEOUT_FAILED;
        }

        private void CopyFastaFiles()
        {
            var localOrgDBDirectory = mMgrParams.GetParam("OrgDbDir");
            var targetDirectoryPath = Path.Combine(mOutputResultsDirectoryPath, "Organism_Database");

            foreach (var orgDbName in ExtractPackedJobParameterList(AnalysisResourcesRepoPkgr.FASTA_FILES_FOR_DATA_PACKAGE))
            {
                var sourceFilePath = Path.Combine(localOrgDBDirectory, orgDbName);

                // Rename the target file if it was created using Protein collections (and used 3 or fewer protein collections)
                var targetFileName = UpdateOrgDBNameIfRequired(orgDbName);

                var destFilePath = Path.Combine(targetDirectoryPath, targetFileName);
                mFileTools.CopyFile(sourceFilePath, destFilePath, true);
            }
        }

        /// <summary>
        /// Gather files for submission to PeptideAtlas repository
        /// and copy them to repo cache
        /// </summary>
        private bool DoPeptideAtlasOperation()
        {
            // Copy *.fasta files from organism db to appropriate cache subdirectory
            // Files to copy are stored in job parameters named "Job123456_GeneratedFasta"
            CopyFastaFiles();
            mProgress = PROGRESS_PCT_FASTA_FILES_COPIED;
            mStatusTools.UpdateAndWrite(mProgress);

            var dataPkgJobCountMatch = 0;

            if (mIncludeMSGFPlusResults)
            {
                // Find any MSGFPlus jobs in data package and copy their first hits files to appropriate cache subdirectory
                // Note that prior to November 2016 the filenames had _msgfdb_fht; they now have _msgfplus_fht
                mRepoPackager.GetItemsToRepoPkg(
                    "DataPkgJobsQueryTemplate",
                    "MSGFPlus",
                    "*_msgfplus_fht.txt;*_msgfplus_fht_MSGF.txt;*_msgfdb_fht.txt;*_msgfdb_fht_MSGF.txt",
                    "MSGFPlus_Results", "Job");

                dataPkgJobCountMatch = mRepoPackager.DataPackageItems.Rows.Count;
                var dataPkgFileCountMatch = mRepoPackager.AssociatedFiles.Rows.Count;

                if (dataPkgJobCountMatch == 0)
                {
                    LogWarning("Did not find any MS-GF+ jobs in data package " + mRepoPackager.DataPkgId + "; auto-setting mIncludeSequestResults to true");
                    mIncludeSequestResults = true;
                }
                else
                {
                    if (dataPkgFileCountMatch == 0)
                        LogWarning("Found " + dataPkgJobCountMatch + " MS-GF+ jobs in data package " + mRepoPackager.DataPkgId + " but did not find any candidate files to copy");
                    else
                        LogMessage("Copied " + dataPkgFileCountMatch + " files for " + dataPkgJobCountMatch + " MS-GF+ jobs in data package " + mRepoPackager.DataPkgId);
                }
            }
            mProgress = PROGRESS_PCT_MSGF_PLUS_RESULTS_COPIED;
            mStatusTools.UpdateAndWrite(mProgress);

            if (mIncludeSequestResults)
            {
                // Find any SEQUEST jobs in data package and copy their first hits files to appropriate cache subdirectory
                mRepoPackager.GetItemsToRepoPkg("DataPkgJobsQueryTemplate", "SEQUEST", "*_fht.txt", "SEQUEST_Results", "Job");
                dataPkgJobCountMatch = mRepoPackager.DataPackageItems.Rows.Count;
                var dataPkgFileCountMatch = mRepoPackager.AssociatedFiles.Rows.Count;

                if (dataPkgJobCountMatch == 0)
                {
                    LogWarning("Did not find any SEQUEST jobs in data package " + mRepoPackager.DataPkgId);
                }
                else
                {
                    if (dataPkgFileCountMatch == 0)
                        LogWarning("Found " + dataPkgJobCountMatch + " SEQUEST jobs in data package " + mRepoPackager.DataPkgId + " but did not find any candidate files to copy");
                    else
                        LogMessage("Copied " + dataPkgFileCountMatch + " files for " + dataPkgJobCountMatch + " SEQUEST jobs in data package " + mRepoPackager.DataPkgId);
                }
            }
            mProgress = PROGRESS_PCT_SEQUEST_RESULTS_COPIED;
            mStatusTools.UpdateAndWrite(mProgress);

            if (mIncludeMZidFiles)
            {
                // find any MSGFPlus jobs in data package and copy their MZID files to appropriate cache subdirectory
                mRepoPackager.GetItemsToRepoPkg("DataPkgJobsQueryTemplate", "MSGFPlus", "*_msgfplus.zip;*_msgfplus.mzid.gz", @"MSGFPlus_Results\MZID_Files", "Job");
                var zipFileCountConverted = FileUtils.ConvertZipsToGZips(Path.Combine(mOutputResultsDirectoryPath, @"MSGFPlus_Results\MZID_Files"), mWorkDir);

                if (zipFileCountConverted > 0)
                    LogMessage("Converted " + zipFileCountConverted + " _msgfplus.zip files to .mzid.gz files");
            }
            mProgress = PROGRESS_PCT_MZID_RESULTS_COPIED;
            mStatusTools.UpdateAndWrite(mProgress);

            var success = RetrieveInstrumentData(out var datasetsProcessed);

            if (!success)
                return false;

            if (datasetsProcessed == 0)
            {
                if (dataPkgJobCountMatch == 0)
                {
                    mMessage = string.Format(
                        "Data package {0} does not have any analysis jobs associated with it; please add some MASIC or DeconTools jobs then reset this job",
                        mRepoPackager.DataPkgId);

                    LogError(mMessage);
                    return false;
                }

                LogWarning("Data package {0} has {1} associated jobs, but no instrument data files were retrieved",
                    mRepoPackager.DataPkgId, dataPkgJobCountMatch);
            }
            mProgress = PROGRESS_PCT_INSTRUMENT_DATA_COPIED;
            mStatusTools.UpdateAndWrite(mProgress);

            return true;
        }

        /// <summary>
        /// Set the report option flags from job parameters
        /// </summary>
        private void SetOptions()
        {
            // New parameters:
            mIncludeMSGFPlusResults = mJobParams.GetJobParameter("IncludeMSGFResults", true);
            mIncludeMZidFiles = mIncludeMSGFPlusResults;
            mIncludeSequestResults = mJobParams.GetJobParameter("IncludeSequestResults", false);	// This will get auto-changed to true if no MS-GF+ jobs are found in the data package
            mIncludeInstrumentData = mJobParams.GetJobParameter("IncludeInstrumentData", true);
            mIncludeMzXMLFiles = mJobParams.GetJobParameter("IncludeMzXMLFiles", true);
        }

        /// <summary>
        /// Set the path for the repo cache directory
        /// </summary>
        private void SetOutputDirectoryPath()
        {
            var resultsDirectoryName = mJobParams.GetJobParameter(AnalysisResources.JOB_PARAM_OUTPUT_FOLDER_NAME, "");
            var outputRootDirectoryPath = mJobParams.GetJobParameter("CacheFolderPath", "");
            mOutputResultsDirectoryPath = Path.Combine(outputRootDirectoryPath, resultsDirectoryName);
        }

        /// <summary>
        /// Generate handler that provides pre-packaged Mage pipelines
        /// that do the heavy lifting tasks that get data package items,
        /// find associated files, and copy them to repo cache directories
        /// </summary>
        /// <returns>Pipeline handler object</returns>
        private void SetMagePipelineManager(string outputDirectoryPath = "")
        {
            var queryDefs = new QueryDefinitions();
            RegisterEvents(queryDefs);

            var connectionString = mMgrParams.GetParam("ConnectionString");
            var brokerConnectionString = mMgrParams.GetParam("BrokerConnectionString");

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, mMgrName);
            var brokerConnectionStringToUse= DbToolsFactory.AddApplicationNameToConnectionString(brokerConnectionString, mMgrName);

            queryDefs.SetCnStr(QueryDefinitions.TagName.Main, connectionStringToUse);
            queryDefs.SetCnStr(QueryDefinitions.TagName.Broker, brokerConnectionStringToUse);

            var dataPackageID = mJobParams.GetJobParameter("DataPackageID", string.Empty);

            mRepoPackager = new MageRepoPkgrPipelines(dataPackageID, mMgrName, queryDefs);

            if (!string.IsNullOrEmpty(outputDirectoryPath))
            {
                mRepoPackager.OutputResultsDirectoryPath = outputDirectoryPath;
            }
        }

        /// <summary>
        /// Retrieves or creates the .mzXML or mzML file for this dataset
        /// </summary>
        /// <param name="msXmlCreator">mzXML/mzML Creator</param>
        /// <param name="datasetName">Dataset name</param>
        /// <param name="analysisResults">Analysis Results class</param>
        /// <param name="datasetRawFilePaths">Dictionary with dataset names and dataset raw file paths</param>
        /// <param name="datasetYearQuarters">Dictionary with dataset names and year/quarter information</param>
        /// <param name="datasetRawDataTypes">Dictionary with dataset names and the raw data type of the instrument data file</param>
        /// <param name="datasetFilePathLocal">Output parameter: Path to the locally cached dataset file</param>
        /// <returns>The full path to the locally created MzXML file</returns>
        private string CreateMzXMLFileIfMissing(
            MSXMLCreator msXmlCreator,
            string datasetName,
            AnalysisResults analysisResults,
            IReadOnlyDictionary<string, string> datasetRawFilePaths,
            IReadOnlyDictionary<string, string> datasetYearQuarters,
            IReadOnlyDictionary<string, string> datasetRawDataTypes,
          out string datasetFilePathLocal)
        {
            datasetFilePathLocal = string.Empty;

            try
            {
                // Look in mWorkDir for the .mzXML or .mzML file for this dataset
                var candidateFileNames = new List<string>
                    {
                        datasetName + AnalysisResources.DOT_MZXML_EXTENSION,
                        datasetName + AnalysisResources.DOT_MZXML_EXTENSION + AnalysisResources.DOT_GZ_EXTENSION,
                        datasetName + AnalysisResources.DOT_MZML_EXTENSION + AnalysisResources.DOT_GZ_EXTENSION
                    };

                foreach (var candidateFile in candidateFileNames)
                {
                    var filePath = Path.Combine(mWorkDir, candidateFile);

                    if (File.Exists(filePath))
                    {
                        return filePath;
                    }

                    // Look for a StoragePathInfo file
                    filePath += AnalysisResources.STORAGE_PATH_INFO_FILE_SUFFIX;

                    if (File.Exists(filePath))
                    {
                        var destPath = string.Empty;
                        var retrieveSuccess = RetrieveStoragePathInfoTargetFile(filePath, analysisResults, ref destPath);

                        if (retrieveSuccess)
                        {
                            return destPath;
                        }
                        break;
                    }
                }

                // Need to create the .mzXML file
                if (!datasetRawFilePaths.ContainsKey(datasetName))
                {
                    mMessage = "Dataset " + datasetName + " not found in job parameter " +
                        AnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS + "; unable to create the missing .mzXML file";
                    LogError(mMessage);
                    return string.Empty;
                }

                mJobParams.AddResultFileToSkip("MSConvert_ConsoleOutput.txt");

                msXmlCreator.UpdateDatasetName(datasetName);

                // Make sure the dataset file is present in the working directory
                // Copy it locally if necessary

                var datasetFilePathRemote = datasetRawFilePaths[datasetName];

                if (string.IsNullOrEmpty(datasetFilePathRemote))
                {
                    mMessage = "Dataset " + datasetName + " has an empty instrument file path in datasetRawFilePaths";
                    LogError(mMessage);
                    return string.Empty;
                }

                var datasetIsADirectory = Directory.Exists(datasetFilePathRemote);

                datasetFilePathLocal = Path.Combine(mWorkDir, Path.GetFileName(datasetFilePathRemote));

                if (datasetIsADirectory)
                {
                    // Confirm that the dataset directory exists in the working directory

                    if (!Directory.Exists(datasetFilePathLocal))
                    {
                        // Copy the dataset directory locally
                        analysisResults.CopyDirectory(datasetFilePathRemote, datasetFilePathLocal, overwrite: true);
                    }
                }
                else
                {
                    // Confirm that the dataset file exists in the working directory
                    if (!File.Exists(datasetFilePathLocal))
                    {
                        // Copy the dataset file locally
                        analysisResults.CopyFileWithRetry(datasetFilePathRemote, datasetFilePathLocal, overwrite: true);
                    }
                }

                if (!datasetRawDataTypes.TryGetValue(datasetName, out var rawDataType))
                    rawDataType = AnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES;

                mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, "RawDataType", rawDataType);

                var success = msXmlCreator.CreateMZXMLFile();

                if (!success && string.IsNullOrEmpty(mMessage))
                {
                    mMessage = msXmlCreator.ErrorMessage;

                    if (string.IsNullOrEmpty(mMessage))
                    {
                        mMessage = "Unknown error creating the mzXML file for dataset " + datasetName;
                    }
                    else if (!mMessage.Contains(datasetName))
                    {
                        mMessage += "; dataset " + datasetName;
                    }
                }

                if (!success)
                    return string.Empty;

                var localMzXmlFile = new FileInfo(Path.Combine(mWorkDir, datasetName + AnalysisResources.DOT_MZXML_EXTENSION));

                if (!localMzXmlFile.Exists)
                {
                    mMessage = "MSXmlCreator did not create the .mzXML file for dataset " + datasetName;
                    return string.Empty;
                }

                // Copy the .mzXML file to the cache
                // Gzip it first before copying
                var gzippedMzXmlFile = new FileInfo(localMzXmlFile + AnalysisResources.DOT_GZ_EXTENSION);
                success = mZipTools.GZipFile(localMzXmlFile.FullName, true);

                if (!success)
                {
                    mMessage = "Error compressing .mzXML file " + localMzXmlFile.Name + " with GZip: ";
                    return string.Empty;
                }

                gzippedMzXmlFile.Refresh();

                if (!gzippedMzXmlFile.Exists)
                {
                    mMessage = "Compressed .mzXML file not found: " + gzippedMzXmlFile.FullName;
                    LogError(mMessage);
                    return string.Empty;
                }

                var msXmlGeneratorName = Path.GetFileNameWithoutExtension(mMSXmlGeneratorAppPath);

                if (!datasetYearQuarters.TryGetValue(datasetName, out var datasetYearQuarter))
                {
                    datasetYearQuarter = string.Empty;
                }

                CopyMzXMLFileToServerCache(gzippedMzXmlFile.FullName, datasetYearQuarter, msXmlGeneratorName, purgeOldFilesIfNeeded: true);

                mJobParams.AddResultFileToSkip(Path.GetFileName(localMzXmlFile.FullName + Global.SERVER_CACHE_HASHCHECK_FILE_SUFFIX));

                PRISM.AppUtils.GarbageCollectNow();

                return gzippedMzXmlFile.FullName;
            }
            catch (Exception ex)
            {
                mMessage = "Exception in CreateMzXMLFileIfMissing";
                LogError(mMessage, ex);
                return string.Empty;
            }
        }

        private void DeleteFileIgnoreErrors(string filePath)
        {
            try
            {
                if (File.Exists(filePath))
                    File.Delete(filePath);
            }
            catch (Exception ex)
            {
                LogWarning("Unable to delete file " + filePath + ": " + ex.Message);
            }
        }

        private bool ProcessDataset(
            AnalysisResults analysisResults,
            MSXMLCreator msXmlCreator,
            string datasetName,
            IReadOnlyDictionary<string, string> datasetRawFilePaths,
            IReadOnlyDictionary<string, string> datasetYearQuarter,
            IReadOnlyDictionary<string, string> datasetRawDataTypes)
        {
            var datasetFilePathLocal = string.Empty;

            try
            {
                var instrumentDataDirectoryPath = Path.Combine(mOutputResultsDirectoryPath, "Instrument_Data");

                if (!datasetRawDataTypes.TryGetValue(datasetName, out var rawDataType))
                    rawDataType = AnalysisResources.RAW_DATA_TYPE_DOT_RAW_FILES;

                if (rawDataType != AnalysisResources.RAW_DATA_TYPE_DOT_UIMF_FILES && mIncludeMzXMLFiles)
                {
                    // Create the .mzXML or .mzML file if it is missing
                    var mzXmlFilePathLocal = CreateMzXMLFileIfMissing(msXmlCreator, datasetName, analysisResults,
                                                                      datasetRawFilePaths,
                                                                      datasetYearQuarter,
                                                                      datasetRawDataTypes,
                                                                      out datasetFilePathLocal);

                    if (string.IsNullOrEmpty(mzXmlFilePathLocal))
                    {
                        return false;
                    }

                    var sourceFile = new FileInfo(mzXmlFilePathLocal);

                    // Copy the .MzXml file to the final directory
                    var targetFile = new FileInfo(Path.Combine(instrumentDataDirectoryPath, Path.GetFileName(mzXmlFilePathLocal)));

                    if (targetFile.Exists && targetFile.Length == sourceFile.Length)
                        LogDebug("Skipping .mzXML file since already present in the target directory: " + targetFile.FullName);
                    else
                        mFileTools.CopyFileUsingLocks(mzXmlFilePathLocal, targetFile.FullName, true);

                    // Delete the local .mzXml file
                    DeleteFileIgnoreErrors(mzXmlFilePathLocal);
                }

                if (mIncludeInstrumentData)
                {
                    // Copy the .raw file, either from the local working directory or from the remote dataset directory
                    var datasetFilePathSource = datasetRawFilePaths[datasetName];

                    if (!string.IsNullOrEmpty(datasetFilePathLocal))
                    {
                        // Dataset was already copied locally; copy it from the local computer to the staging directory
                        datasetFilePathSource = datasetFilePathLocal;
                    }

                    if (datasetFilePathSource.StartsWith(AnalysisResources.MYEMSL_PATH_FLAG))
                    {
                        // The file was in MyEMSL and should have already been retrieved via AnalysisResourcesRepoPkgr
                        datasetFilePathSource = Path.Combine(mWorkDir, Path.GetFileName(datasetFilePathSource));
                        datasetFilePathLocal = datasetFilePathSource;
                    }

                    var datasetFileIsADirectory = Directory.Exists(datasetFilePathSource);

                    if (datasetFileIsADirectory)
                    {
                        var datasetDirectory = new DirectoryInfo(datasetFilePathSource);
                        var datasetFilePathTarget = Path.Combine(instrumentDataDirectoryPath, datasetDirectory.Name);
                        mFileTools.CopyDirectory(datasetFilePathSource, datasetFilePathTarget);
                    }
                    else
                    {
                        var sourceFile = new FileInfo(datasetFilePathSource);
                        var targetFile = new FileInfo(Path.Combine(instrumentDataDirectoryPath, sourceFile.Name));

                        if (targetFile.Exists && targetFile.Length == sourceFile.Length)
                            LogDebug("Skipping instrument file since already present in the target directory: " + targetFile.FullName);
                        else
                            mFileTools.CopyFileUsingLocks(datasetFilePathSource, targetFile.FullName, true);
                    }

                    if (!string.IsNullOrEmpty(datasetFilePathLocal))
                    {
                        if (datasetFileIsADirectory)
                        {
                            // Delete the local dataset directory
                            if (Directory.Exists(datasetFilePathLocal))
                            {
                                Directory.Delete(datasetFilePathLocal, true);
                            }
                        }
                        else
                        {
                            // Delete the local dataset file
                            DeleteFileIgnoreErrors(datasetFilePathLocal);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error retrieving instrument data for " + datasetName, ex);
                return false;
            }

            return true;
        }

        private bool RetrieveInstrumentData(out int datasetsProcessed)
        {
            // Extract the packed parameters
            var datasetRawFilePaths = ExtractPackedJobParameterDictionary(AnalysisResources.JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS);
            var datasetYearQuarter = ExtractPackedJobParameterDictionary(AnalysisResourcesRepoPkgr.JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER);
            var datasetRawDataTypes = ExtractPackedJobParameterDictionary(AnalysisResources.JOB_PARAM_DICTIONARY_DATASET_RAW_DATA_TYPES);

            datasetsProcessed = 0;

            // The analysisResults object is used to copy files to/from this computer
            var analysisResults = new AnalysisResults(mMgrParams, mJobParams);

            mMSXmlGeneratorAppPath = GetMSXmlGeneratorAppPath();
            var msXmlCreator = new MSXMLCreator(mMSXmlGeneratorAppPath, mWorkDir, mDatasetName, mDebugLevel, mJobParams);

            RegisterEvents(msXmlCreator);

            var successCount = 0;
            var errorCount = 0;

            if (datasetRawFilePaths.Keys.Count == 0)
            {
                mMessage = "Could not retrieve instrument data since datasetRawFilePaths is empty";
                return false;
            }

            // Process each dataset
            foreach (var datasetName in datasetRawFilePaths.Keys)
            {
                var success = ProcessDataset(analysisResults, msXmlCreator, datasetName, datasetRawFilePaths, datasetYearQuarter, datasetRawDataTypes);

                if (success)
                    successCount++;
                else
                    errorCount++;

                datasetsProcessed++;
                mProgress = ComputeIncrementalProgress(PROGRESS_PCT_MZID_RESULTS_COPIED, PROGRESS_PCT_INSTRUMENT_DATA_COPIED, datasetsProcessed, datasetRawFilePaths.Count);
                mStatusTools.UpdateAndWrite(mProgress);
            }

            if (successCount > 0)
            {
                if (errorCount == 0)
                    return true;

                mMessage = "Could not retrieve instrument data for one or more datasets in Data package " + mRepoPackager.DataPkgId + "; SuccessCount = " + successCount + "; FailCount = " + errorCount;
                return false;
            }

            mMessage = "Could not retrieve instrument data for any of the datasets in Data package " + mRepoPackager.DataPkgId + "; FailCount = " + errorCount;
            return false;
        }

        private bool RetrieveStoragePathInfoTargetFile(string storagePathInfoFilePath, AnalysisResults analysisResults, ref string destPath)
        {
            const bool IsDirectory = false;
            return RetrieveStoragePathInfoTargetFile(storagePathInfoFilePath, analysisResults, IsDirectory, ref destPath);
        }

        private bool RetrieveStoragePathInfoTargetFile(string storagePathInfoFilePath, AnalysisResults analysisResults, bool isDirectory, ref string destPath)
        {
            var sourceFilePath = string.Empty;

            try
            {
                destPath = string.Empty;

                if (!File.Exists(storagePathInfoFilePath))
                {
                    mMessage = "StoragePathInfo file not found";
                    LogError(mMessage + ": " + storagePathInfoFilePath);
                    return false;
                }

                using (var reader = new StreamReader(new FileStream(storagePathInfoFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    if (!reader.EndOfStream)
                    {
                        sourceFilePath = reader.ReadLine();
                    }
                }

                if (string.IsNullOrEmpty(sourceFilePath))
                {
                    mMessage = "StoragePathInfo file was empty";
                    LogError(mMessage + ": " + storagePathInfoFilePath);
                    return false;
                }

                destPath = Path.Combine(mWorkDir, Path.GetFileName(sourceFilePath));

                if (isDirectory)
                {
                    analysisResults.CopyDirectory(sourceFilePath, destPath, overwrite: true);
                }
                else
                {
                    analysisResults.CopyFileWithRetry(sourceFilePath, destPath, overwrite: true);
                }
            }
            catch (Exception ex)
            {
                mMessage = "Error in RetrieveStoragePathInfoTargetFile";
                LogError(mMessage, ex);
                return false;
            }

            return true;
        }

        private bool StoreToolVersionInfo()
        {
            var toolVersionInfo = string.Empty;

            // Lookup the version of the AnalysisManagerRepoPkgr plugin
            if (!StoreToolVersionInfoForLoadedAssembly(ref toolVersionInfo, "AnalysisManager_RepoPkgr_Plugin", includeRevision: false))
                return false;

            try
            {
                return SetStepTaskToolVersion(toolVersionInfo, new List<FileInfo>(), false);
            }
            catch (Exception ex)
            {
                LogError("Exception calling SetStepTaskToolVersion", ex);
                return false;
            }
        }

        /// <summary>
        /// Examines orgDbName to see if it is of the form ID_003878_0C3354F8.fasta
        /// If it is, queries the protein sequences database for the names of the protein collections used to generate the file
        /// </summary>
        /// <param name="orgDbName"></param>
        /// <returns>If three or fewer protein collections, returns an updated filename based on the protein collection names.  Otherwise, simply returns orgDbName</returns>
        private string UpdateOrgDBNameIfRequired(string orgDbName)
        {
            var reArchiveFileId = new Regex(@"ID_([0-9]+)_[A-Z0-9]+\.fasta", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            try
            {
                // ID_003878_0C3354F8.fasta
                var reMatch = reArchiveFileId.Match(orgDbName);

                if (!reMatch.Success)
                    return orgDbName;		// Likely used a legacy FASTA file

                var fileID = int.Parse(reMatch.Groups[1].Value);

                var sqlQuery = "SELECT collection_name FROM V_Archived_Output_File_Protein_Collections WHERE archived_file_id = " + fileID;
                const short retryCount = 3;

                var proteinSeqsDBConnectionString = mMgrParams.GetParam("FastaCnString");

                if (string.IsNullOrWhiteSpace(proteinSeqsDBConnectionString))
                {
                    LogError("Error in UpdateOrgDBNameIfRequired: manager parameter FastaCnString is not defined");
                    return orgDbName;
                }

                var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(proteinSeqsDBConnectionString, mMgrName);

                var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
                RegisterEvents(dbTools);

                var success = dbTools.GetQueryResults(sqlQuery, out var results, retryCount, retryDelaySeconds: 2);
                var proteinCollectionList = results.SelectMany(x => x).ToList();

                if (proteinCollectionList.Count is > 0 and < 4)
                {
                    return string.Join("_", proteinCollectionList) + ".fasta";
                }
            }
            catch (Exception ex)
            {
                LogError("Exception in UpdateOrgDBNameIfRequired", ex);
            }

            return orgDbName;
        }
    }
}
