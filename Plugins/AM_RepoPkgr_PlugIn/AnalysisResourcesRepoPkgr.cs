using AnalysisManagerBase;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;
using PRISMDatabaseUtils;

namespace AnalysisManager_RepoPkgr_Plugin
{
    /// <summary>
    /// Retrieve resources for the Repo Packager plugin
    /// </summary>
    public class AnalysisResourcesRepoPkgr : AnalysisResources
    {
        // Ignore Spelling: repo

        /// <summary>
        /// Packed job parameter tracking the FASTA files used by the dataset
        /// </summary>
        public const string FASTA_FILES_FOR_DATA_PACKAGE = "FastaFilesForDataPackage";

        /// <summary>
        /// Packed job parameter for datasets missing MzXML files
        /// </summary>
        public const string JOB_PARAM_DATASETS_MISSING_MZXML_FILES = "PackedParam_DatasetsMissingMzXMLFiles";

        /// <summary>
        /// Packed job parameter for YearQuarter
        /// </summary>
        public const string JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER = "PackedParam_DatasetStorage_YearQuarter";

        /// <summary>
        /// Do any resource-gathering tasks here
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            var localOrgDBDirectory = mMgrParams.GetParam("OrgDbDir");

            // Gigasax.DMS_Pipeline
            var brokerDbConnectionString = mMgrParams.GetParam("BrokerConnectionString");

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(brokerDbConnectionString, mMgrName);

            var dataPkgId = mJobParams.GetJobParameter("DataPackageID", -1);

            // additionalJobs tracks non Peptide-hit jobs (e.g. DeconTools or MASIC jobs)

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: TraceMode);
            RegisterEvents(dbTools);

            var dataPackageInfoLoader = new DataPackageInfoLoader(dbTools, dataPkgId);

            var dataPackagePeptideHitJobs = dataPackageInfoLoader.RetrieveDataPackagePeptideHitJobInfo(out var additionalJobs);
            var success = RetrieveFastaFiles(localOrgDBDirectory, dataPackagePeptideHitJobs);

            if (!success)
                return CloseOutType.CLOSEOUT_NO_FAS_FILES;

            var includeMzXmlFiles = mJobParams.GetJobParameter("IncludeMzXMLFiles", true);

            success = FindInstrumentDataFiles(dataPackageInfoLoader, dataPackagePeptideHitJobs, additionalJobs, includeMzXmlFiles);
            if (!success)
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;

            if (includeMzXmlFiles)
            {
                var allJobs = new List<DataPackageJobInfo>();
                allJobs.AddRange(dataPackagePeptideHitJobs);

                if (additionalJobs != null)
                    allJobs.AddRange(additionalJobs);

                FindMissingMzXmlFiles(allJobs);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        private bool FindInstrumentDataFiles(
            DataPackageInfoLoader dataPackageInfoLoader,
            IEnumerable<DataPackageJobInfo> dataPackagePeptideHitJobs,
            IEnumerable<DataPackageJobInfo> additionalJobs,
            bool includeMzXmlFiles)
        {
            // The keys in this dictionary are DataPackageJobInfo entries; the values in this dictionary are KeyValuePairs of path to the .mzXML or .mzML file and path to the .hashcheck file (if any)
            // The KeyValuePair will have empty strings if the .Raw file needs to be retrieved
            var instrumentDataToRetrieve = new Dictionary<DataPackageJobInfo, KeyValuePair<string, string>>();

            // Keys in this dictionary are dataset name, values are the full path to the instrument data file for the dataset
            var datasetRawFilePaths = new Dictionary<string, string>();

            // Keys in this dictionary are dataset name, values are the raw_data_type for the dataset
            var datasetRawDataTypes = new Dictionary<string, string>();

            // Cache the current dataset and job info
            var currentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

            var missingInstrumentDataCount = 0;

            // Combine the two job lists provided to this method to determine the master list of jobs to process
            var jobsToProcess = dataPackagePeptideHitJobs.ToList();
            jobsToProcess.AddRange(additionalJobs.ToList());

            var jobCountToProcess = jobsToProcess.Count;
            var jobsProcessed = 0;

            var lastProgressUpdate = DateTime.UtcNow;

            foreach (var jobInfo in jobsToProcess)
            {
                jobsProcessed++;
                if (datasetRawDataTypes.ContainsKey(jobInfo.Dataset))
                    continue;

                datasetRawDataTypes.Add(jobInfo.Dataset, jobInfo.RawDataType);

                if (!OverrideCurrentDatasetAndJobInfo(jobInfo))
                {
                    // Error message has already been logged
                    return false;
                }

                if (includeMzXmlFiles)
                {
                    if (jobInfo.RawDataType == RAW_DATA_TYPE_DOT_UIMF_FILES)
                    {
                        // Don't create .mzXML files for .UIMF files
                        // Instead simply add the .uimf path to datasetRawFilePaths
                    }
                    else
                    {
                        // See if a .mzXML or .mzML file already exists for this dataset

                        var mzXMLFilePath = FileSearch.FindMZXmlFile(out var hashcheckFilePath);
                        var mzMLFilePath = string.Empty;

                        if (string.IsNullOrEmpty(mzXMLFilePath))
                        {
                            mzMLFilePath = FileSearch.FindMsXmlFileInCache(MSXMLOutputTypeConstants.mzML, out hashcheckFilePath);
                        }

                        if (!string.IsNullOrEmpty(mzXMLFilePath))
                        {
                            instrumentDataToRetrieve.Add(jobInfo, new KeyValuePair<string, string>(mzXMLFilePath, hashcheckFilePath));
                        }
                        else if (!string.IsNullOrEmpty(mzMLFilePath))
                        {
                            instrumentDataToRetrieve.Add(jobInfo, new KeyValuePair<string, string>(mzMLFilePath, hashcheckFilePath));
                        }
                        else
                        {
                            // mzXML or mzML file not found
                            if (jobInfo.RawDataType == RAW_DATA_TYPE_DOT_RAW_FILES)
                            {
                                // Will need to retrieve the .Raw file for this dataset
                                instrumentDataToRetrieve.Add(jobInfo, new KeyValuePair<string, string>(string.Empty, string.Empty));
                            }
                            else
                            {
                                mMessage = "mzXML/mzML file not found for dataset " + jobInfo.Dataset +
                                            " and dataset file type is not a .Raw file and we thus cannot auto-create the missing mzXML file";
                                LogError(mMessage);
                                return false;
                            }
                        }
                    }
                }

                // Note that FindDatasetFileOrDirectory will return the default dataset directory path, even if the data file is not found
                // Therefore, we need to check that rawFilePath actually exists
                var rawFilePath = DirectorySearch.FindDatasetFileOrDirectory(1, out var isDirectory);

                if (!rawFilePath.StartsWith(MYEMSL_PATH_FLAG))
                {
                    if (!File.Exists(rawFilePath))
                    {
                        rawFilePath = string.Empty;
                        missingInstrumentDataCount++;

                        if (!datasetRawFilePaths.ContainsKey(jobInfo.Dataset))
                        {
                            var msg = "Instrument data file not found for dataset " + jobInfo.Dataset;
                            LogError(msg);
                        }
                    }
                }

                if (!string.IsNullOrEmpty(rawFilePath))
                {
                    if (!datasetRawFilePaths.ContainsKey(jobInfo.Dataset))
                    {
                        if (rawFilePath.StartsWith(MYEMSL_PATH_FLAG))
                        {
                            mMyEMSLUtilities.AddFileToDownloadQueue(mMyEMSLUtilities.RecentlyFoundMyEMSLFiles.First().FileInfo);
                        }

                        datasetRawFilePaths.Add(jobInfo.Dataset, rawFilePath);
                    }
                }

                // Compute a % complete value between 0 and 2%
                var percentComplete = jobsProcessed / (float)jobCountToProcess * 2;
                mStatusTools.UpdateAndWrite(percentComplete);

                if (DateTime.UtcNow.Subtract(lastProgressUpdate).TotalSeconds >= 30)
                {
                    lastProgressUpdate = DateTime.UtcNow;

                    var progressMsg = "Finding instrument data";
                    if (includeMzXmlFiles)
                        progressMsg += " and mzXML files";

                    progressMsg += ": " + jobsProcessed + " / " + jobCountToProcess + " jobs";

                    LogMessage(progressMsg);
                }
            }

            if (missingInstrumentDataCount > 0)
            {
                var jobId = mJobParams.GetJobParameter("Job", "??");
                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", "??");
                var msg = "Instrument data file not found for " + missingInstrumentDataCount +
                    Global.CheckPlural(missingInstrumentDataCount, " dataset", " datasets") + " in data package " + dataPackageID;
                mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, AnalysisToolRunnerRepoPkgr.WARNING_INSTRUMENT_DATA_MISSING, msg);

                msg += " (pipeline job " + jobId + ")";
                LogErrorToDatabase(msg);
            }

            // Restore the dataset and job info for this aggregation job
            OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo);

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return false;
            }

            // Store the dataset paths in a Packed Job Parameter
            StorePackedJobParameterDictionary(datasetRawFilePaths, JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS);

            // Store the dataset RawDataTypes in a Packed Job Parameter
            StorePackedJobParameterDictionary(datasetRawDataTypes, JOB_PARAM_DICTIONARY_DATASET_RAW_DATA_TYPES);

            var retrievalOptions = new DataPackageFileHandler.DataPackageRetrievalOptionsType
            {
                CreateJobPathFiles = true,
                RetrieveMzXMLFile = true
            };

            var dataPackageFileHandler = new DataPackageFileHandler(
                dataPackageInfoLoader.DBTools,
                dataPackageInfoLoader.DataPackageID,
                this);

            var success = dataPackageFileHandler.RetrieveDataPackageMzXMLFiles(instrumentDataToRetrieve, retrievalOptions);

            return success;
        }

        /// <summary>
        /// Find datasets that do not have a .mzXML file
        /// Datasets that need to have .mzXML files created will be added to the packed job parameters, storing the dataset names in "PackedParam_DatasetsMissingMzXMLFiles"
        /// and the dataset Year_Quarter values in "PackedParam_DatasetStorage_YearQuarter"
        /// </summary>
        /// <param name="dataPackagePeptideHitJobs"></param>
        private void FindMissingMzXmlFiles(IEnumerable<DataPackageJobInfo> dataPackagePeptideHitJobs)
        {
            var datasetNames = new SortedSet<string>();
            var datasetYearQuarter = new SortedSet<string>();

            try
            {
                foreach (var job in dataPackagePeptideHitJobs)
                {
                    var candidateFileNames = new List<string>
                    {
                        job.Dataset + DOT_MZXML_EXTENSION,
                        job.Dataset + DOT_MZXML_EXTENSION + DOT_GZ_EXTENSION,
                        job.Dataset + DOT_MZML_EXTENSION + DOT_GZ_EXTENSION
                    };

                    var matchFound = false;

                    foreach (var candidateFile in candidateFileNames)
                    {
                        var filePath = Path.Combine(mWorkDir, candidateFile);

                        if (File.Exists(filePath))
                        {
                            matchFound = true;
                            break;
                        }

                        // Look for a StoragePathInfo file
                        filePath += STORAGE_PATH_INFO_FILE_SUFFIX;

                        if (File.Exists(filePath))
                        {
                            matchFound = true;
                            break;
                        }
                    }

                    if (!matchFound && !datasetNames.Contains(job.Dataset))
                    {
                        datasetNames.Add(job.Dataset);
                        datasetYearQuarter.Add(job.Dataset + "=" + GetDatasetYearQuarter(job.ServerStoragePath));
                    }
                }

                if (datasetNames.Count > 0)
                {
                    StorePackedJobParameterList(datasetNames.ToList(), JOB_PARAM_DATASETS_MISSING_MZXML_FILES);
                    StorePackedJobParameterList(datasetYearQuarter.ToList(), JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER);
                }
            }
            catch (Exception ex)
            {
                mMessage = "Exception in FindMissingMzXmlFiles";
                LogError(mMessage + ": " + ex.Message);
            }
        }

        private bool RetrieveFastaFiles(string orgDbDirectoryPath, IEnumerable<DataPackageJobInfo> dataPackagePeptideHitJobs)
        {
            try
            {
                // This dictionary is used to avoid calling RetrieveOrgDB() for every job
                // The dictionary keys are LegacyFastaFileName, ProteinOptions, and ProteinCollectionList combined with underscores
                // The dictionary values are the name of the generated (or retrieved) FASTA file
                var orgDBParamsToGeneratedFileNameMap = new Dictionary<string, string>();

                // This list tracks the generated FASTA file name
                var generatedOrgDBNames = new List<string>();

                // Cache the current dataset and job info
                var currentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

                foreach (var analysisJob in dataPackagePeptideHitJobs)
                {
                    var dictionaryKey = string.Format("{0}_{1}_{2}",
                        analysisJob.LegacyFastaFileName, analysisJob.ProteinCollectionList, analysisJob.ProteinOptions);

                    if (orgDBParamsToGeneratedFileNameMap.TryGetValue(dictionaryKey, out var orgDbNameGenerated))
                    {
                        // Organism DB was already generated
                    }
                    else
                    {
                        OverrideCurrentDatasetAndJobInfo(analysisJob);
                        mJobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", string.Empty);
                        if (!RetrieveOrgDB(orgDbDirectoryPath, out _))
                        {
                            if (string.IsNullOrEmpty(mMessage))
                                mMessage = "Call to RetrieveOrgDB returned false in AnalysisResourcesRepoPkgr.RetrieveFastaFiles";
                            return false;
                        }
                        orgDbNameGenerated = mJobParams.GetJobParameter("PeptideSearch", "generatedFastaName", string.Empty);
                        if (string.IsNullOrEmpty(orgDbNameGenerated))
                        {
                            mMessage = "FASTA file was not generated when RetrieveFastaFiles called RetrieveOrgDB";
                            LogError(mMessage + " (class AnalysisResourcesRepoPkgr)");
                            return false;
                        }

                        if (orgDbNameGenerated != analysisJob.GeneratedFASTAFileName)
                        {
                            mMessage = "Generated FASTA file name (" + orgDbNameGenerated + ") does not match expected FASTA file name (" +
                                       analysisJob.GeneratedFASTAFileName + "); aborting";

                            LogError(mMessage + " (class AnalysisResourcesRepoPkgr)");
                            return false;
                        }
                        orgDBParamsToGeneratedFileNameMap.Add(dictionaryKey, orgDbNameGenerated);

                        generatedOrgDBNames.Add(orgDbNameGenerated);
                    }
                    // Add a new job parameter that associates orgDbNameGenerated with this job
                    mJobParams.AddAdditionalParameter("PeptideSearch", GetGeneratedFastaParamNameForJob(analysisJob.Job),
                                                       orgDbNameGenerated);
                }

                // Store the names of the generated FASTA files
                // This is a tab separated list of filenames
                StorePackedJobParameterList(generatedOrgDBNames, FASTA_FILES_FOR_DATA_PACKAGE);

                // Restore the dataset and job info for this aggregation job
                OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo);
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrieveFastaFiles";
                LogError(mMessage, ex);
                return false;
            }
            return true;
        }

        private static string GetGeneratedFastaParamNameForJob(int job)
        {
            return "Job" + job + "_GeneratedFasta";
        }
    }
}
