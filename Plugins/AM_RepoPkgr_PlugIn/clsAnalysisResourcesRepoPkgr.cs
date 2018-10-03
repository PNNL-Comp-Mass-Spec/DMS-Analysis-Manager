using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;

namespace AnalysisManager_RepoPkgr_Plugin
{
    /// <summary>
    /// Retrieve resources for the Repo Packager plugin
    /// </summary>
    public class clsAnalysisResourcesRepoPkgr : clsAnalysisResources
    {
        #region Constants

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

        #endregion

        #region Member_Functions

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

            var localOrgDBFolder = mMgrParams.GetParam("OrgDbDir");

            // Gigasax.DMS_Pipeline
            var connectionString = mMgrParams.GetParam("brokerconnectionstring");

            var dataPkgId = mJobParams.GetJobParameter("DataPackageID", -1);

            // lstAdditionalJobs tracks non Peptide-hit jobs (e.g. DeconTools or MASIC jobs)

            var dataPackageInfoLoader = new clsDataPackageInfoLoader(connectionString, dataPkgId);

            var lstDataPackagePeptideHitJobs = dataPackageInfoLoader.RetrieveDataPackagePeptideHitJobInfo(out var lstAdditionalJobs);
            var success = RetrieveFastaFiles(localOrgDBFolder, lstDataPackagePeptideHitJobs);

            if (!success)
                return CloseOutType.CLOSEOUT_NO_FAS_FILES;

            var includeMzXmlFiles = mJobParams.GetJobParameter("IncludeMzXMLFiles", true);

            success = FindInstrumentDataFiles(dataPackageInfoLoader, lstDataPackagePeptideHitJobs, lstAdditionalJobs, includeMzXmlFiles);
            if (!success)
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;

            if (includeMzXmlFiles)
            {
                var lstAllJobs = new List<clsDataPackageJobInfo>();
                lstAllJobs.AddRange(lstDataPackagePeptideHitJobs);

                if (lstAdditionalJobs != null)
                    lstAllJobs.AddRange(lstAdditionalJobs);

                FindMissingMzXmlFiles(lstAllJobs);
            }


            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        #endregion // Member_Functions

        #region Code_Adapted_From_Pride_Plugin

        private bool FindInstrumentDataFiles(
            clsDataPackageInfoLoader dataPackageInfoLoader,
            IEnumerable<clsDataPackageJobInfo> lstDataPackagePeptideHitJobs,
            IEnumerable<clsDataPackageJobInfo> lstAdditionalJobs,
            bool includeMzXmlFiles)
        {

            // The keys in this dictionary are udtJobInfo entries; the values in this dictionary are KeyValuePairs of path to the .mzXML or .mzML file and path to the .hashcheck file (if any)
            // The KeyValuePair will have empty strings if the .Raw file needs to be retrieved
            var dctInstrumentDataToRetrieve = new Dictionary<clsDataPackageJobInfo, KeyValuePair<string, string>>();

            // Keys in this dictionary are dataset name, values are the full path to the instrument data file for the dataset
            var dctDatasetRawFilePaths = new Dictionary<string, string>();

            // Keys in this dictionary are dataset name, values are the raw_data_type for the dataset
            var dctDatasetRawDataTypes = new Dictionary<string, string>();

            // Cache the current dataset and job info
            var udtCurrentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

            var missingInstrumentDataCount = 0;

            // Combine the two job lists provided to this function to determine the master list of jobs to process
            var jobsToProcess = lstDataPackagePeptideHitJobs.ToList();
            jobsToProcess.AddRange(lstAdditionalJobs.ToList());

            var jobCountToProcess = jobsToProcess.Count;
            var jobsProcessed = 0;

            var dtLastProgressUpdate = DateTime.UtcNow;

            foreach (var udtJobInfo in jobsToProcess)
            {
                jobsProcessed++;
                if (dctDatasetRawDataTypes.ContainsKey(udtJobInfo.Dataset))
                    continue;

                dctDatasetRawDataTypes.Add(udtJobInfo.Dataset, udtJobInfo.RawDataType);

                if (!OverrideCurrentDatasetAndJobInfo(udtJobInfo))
                {
                    // Error message has already been logged
                    return false;
                }

                if (includeMzXmlFiles)
                {
                    if (udtJobInfo.RawDataType == RAW_DATA_TYPE_DOT_UIMF_FILES)
                    {
                        // Don't create .mzXML files for .UIMF files
                        // Instead simply add the .uimf path to dctDatasetRawFilePaths
                    }
                    else
                    {
                        // See if a .mzXML or .mzML file already exists for this dataset
                        string strHashcheckFilePath;

                        var mzXMLFilePath = FileSearch.FindMZXmlFile(out strHashcheckFilePath);
                        var mzMLFilePath = string.Empty;

                        if (string.IsNullOrEmpty(mzXMLFilePath))
                        {
                            mzMLFilePath = FileSearch.FindMsXmlFileInCache(MSXMLOutputTypeConstants.mzML, out strHashcheckFilePath);
                        }

                        if (!string.IsNullOrEmpty(mzXMLFilePath))
                        {
                            dctInstrumentDataToRetrieve.Add(udtJobInfo, new KeyValuePair<string, string>(mzXMLFilePath, strHashcheckFilePath));
                        }
                        else if (!string.IsNullOrEmpty(mzMLFilePath))
                        {
                            dctInstrumentDataToRetrieve.Add(udtJobInfo, new KeyValuePair<string, string>(mzMLFilePath, strHashcheckFilePath));
                        }
                        else
                        {
                            // mzXML or mzML file not found
                            if (udtJobInfo.RawDataType == RAW_DATA_TYPE_DOT_RAW_FILES)
                            {
                                // Will need to retrieve the .Raw file for this dataset
                                dctInstrumentDataToRetrieve.Add(udtJobInfo, new KeyValuePair<string, string>(string.Empty, string.Empty));
                            }
                            else
                            {
                                mMessage = "mzXML/mzML file not found for dataset " + udtJobInfo.Dataset +
                                            " and dataset file type is not a .Raw file and we thus cannot auto-create the missing mzXML file";
                                LogError(mMessage);
                                return false;
                            }
                        }

                    }

                }

                bool blnIsFolder;

                // Note that FindDatasetFileOrFolder will return the default dataset directory path, even if the data file is not found
                // Therefore, we need to check that strRawFilePath actually exists
                var strRawFilePath = FolderSearch.FindDatasetFileOrFolder(1, out blnIsFolder);

                if (!strRawFilePath.StartsWith(MYEMSL_PATH_FLAG))
                {
                    if (!File.Exists(strRawFilePath))
                    {
                        strRawFilePath = string.Empty;
                        missingInstrumentDataCount++;

                        if (!dctDatasetRawFilePaths.ContainsKey(udtJobInfo.Dataset))
                        {
                            var msg = "Instrument data file not found for dataset " + udtJobInfo.Dataset;
                            LogError(msg);
                        }
                    }
                }

                if (!string.IsNullOrEmpty(strRawFilePath))
                {
                    if (!dctDatasetRawFilePaths.ContainsKey(udtJobInfo.Dataset))
                    {
                        if (strRawFilePath.StartsWith(MYEMSL_PATH_FLAG))
                        {
                            mMyEMSLUtilities.AddFileToDownloadQueue(mMyEMSLUtilities.RecentlyFoundMyEMSLFiles.First().FileInfo);
                        }

                        dctDatasetRawFilePaths.Add(udtJobInfo.Dataset, strRawFilePath);
                    }
                }

                // Compute a % complete value between 0 and 2%
                var percentComplete = jobsProcessed / (float)jobCountToProcess * 2;
                mStatusTools.UpdateAndWrite(percentComplete);

                if (DateTime.UtcNow.Subtract(dtLastProgressUpdate).TotalSeconds >= 30)
                {
                    dtLastProgressUpdate = DateTime.UtcNow;

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
                    clsGlobal.CheckPlural(missingInstrumentDataCount, " dataset", " datasets") + " in data package " + dataPackageID;
                mJobParams.AddAdditionalParameter(clsAnalysisJob.JOB_PARAMETERS_SECTION, clsAnalysisToolRunnerRepoPkgr.WARNING_INSTRUMENT_DATA_MISSING, msg);

                msg += " (pipeline job " + jobId + ")";
                LogErrorToDatabase(msg);
            }

            // Restore the dataset and job info for this aggregation job
            OverrideCurrentDatasetAndJobInfo(udtCurrentDatasetAndJobInfo);

            if (!ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return false;
            }

            // Store the dataset paths in a Packed Job Parameter
            StorePackedJobParameterDictionary(dctDatasetRawFilePaths, JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS);

            // Store the dataset RawDataTypes in a Packed Job Parameter
            StorePackedJobParameterDictionary(dctDatasetRawDataTypes, JOB_PARAM_DICTIONARY_DATASET_RAW_DATA_TYPES);

            var udtOptions = new clsDataPackageFileHandler.udtDataPackageRetrievalOptionsType
            {
                CreateJobPathFiles = true,
                RetrieveMzXMLFile = true
            };

            var dataPackageFileHandler = new clsDataPackageFileHandler(dataPackageInfoLoader.ConnectionString, dataPackageInfoLoader.DataPackageID, this);

            var success = dataPackageFileHandler.RetrieveDataPackageMzXMLFiles(dctInstrumentDataToRetrieve, udtOptions);

            return success;

        }


        /// <summary>
        /// Find datasets that do not have a .mzXML file
        /// Datasets that need to have .mzXML files created will be added to the packed job parameters, storing the dataset names in "PackedParam_DatasetsMissingMzXMLFiles"
        /// and the dataset Year_Quarter values in "PackedParam_DatasetStorage_YearQuarter"
        /// </summary>
        /// <param name="lstDataPackagePeptideHitJobs"></param>
        /// <remarks></remarks>
        protected void FindMissingMzXmlFiles(IEnumerable<clsDataPackageJobInfo> lstDataPackagePeptideHitJobs)
        {
            var lstDatasets = new SortedSet<string>();
            var lstDatasetYearQuarter = new SortedSet<string>();

            try
            {
                foreach (var udtJob in lstDataPackagePeptideHitJobs)
                {
                    var candidateFileNames = new List<string>
                    {
                        udtJob.Dataset + DOT_MZXML_EXTENSION,
                        udtJob.Dataset + DOT_MZXML_EXTENSION + DOT_GZ_EXTENSION,
                        udtJob.Dataset + DOT_MZML_EXTENSION + DOT_GZ_EXTENSION
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

                    if (!matchFound && !lstDatasets.Contains(udtJob.Dataset))
                    {
                        lstDatasets.Add(udtJob.Dataset);
                        lstDatasetYearQuarter.Add(udtJob.Dataset + "=" + GetDatasetYearQuarter(udtJob.ServerStoragePath));
                    }
                }

                if (lstDatasets.Count > 0)
                {
                    StorePackedJobParameterList(lstDatasets.ToList(), JOB_PARAM_DATASETS_MISSING_MZXML_FILES);
                    StorePackedJobParameterList(lstDatasetYearQuarter.ToList(), JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER);
                }

            }
            catch (Exception ex)
            {
                mMessage = "Exception in FindMissingMzXmlFiles";
                LogError(mMessage + ": " + ex.Message);
            }

        }

        private bool RetrieveFastaFiles(string orgDbDirectoryPath, IEnumerable<clsDataPackageJobInfo> lstDataPackagePeptideHitJobs)
        {
            try
            {
                // This dictionary is used to avoid calling RetrieveOrgDB() for every job
                // The dictionary keys are LegacyFastaFileName, ProteinOptions, and ProteinCollectionList combined with underscores
                // The dictionary values are the name of the generated (or retrieved) fasta file
                var dctOrgDBParamsToGeneratedFileNameMap = new Dictionary<string, string>();

                // This list tracks the generated fasta file name
                var lstGeneratedOrgDBNames = new List<string>();

                // Cache the current dataset and job info
                var udtCurrentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

                foreach (var udtJob in lstDataPackagePeptideHitJobs)
                {
                    var strDictionaryKey = string.Format("{0}_{1}_{2}", udtJob.LegacyFastaFileName, udtJob.ProteinCollectionList,
                                                            udtJob.ProteinOptions);
                    if (dctOrgDBParamsToGeneratedFileNameMap.TryGetValue(strDictionaryKey, out var orgDbNameGenerated))
                    {
                        // Organism DB was already generated
                    }
                    else
                    {
                        OverrideCurrentDatasetAndJobInfo(udtJob);
                        mJobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", string.Empty);
                        if (!RetrieveOrgDB(orgDbDirectoryPath, out _))
                        {
                            if (string.IsNullOrEmpty(mMessage))
                                mMessage = "Call to RetrieveOrgDB returned false in clsAnalysisResourcesRepoPkgr.RetrieveFastaFiles";
                            return false;
                        }
                        orgDbNameGenerated = mJobParams.GetJobParameter("PeptideSearch", "generatedFastaName", string.Empty);
                        if (string.IsNullOrEmpty(orgDbNameGenerated))
                        {
                            mMessage = "FASTA file was not generated when RetrieveFastaFiles called RetrieveOrgDB";
                            LogError(mMessage + " (class clsAnalysisResourcesRepoPkgr)");
                            return false;
                        }
                        if (orgDbNameGenerated != udtJob.OrganismDBName)
                        {
                            mMessage = "Generated FASTA file name (" + orgDbNameGenerated + ") does not match expected fasta file name (" +
                                        udtJob.OrganismDBName + "); aborting";
                            LogError(mMessage + " (class clsAnalysisResourcesRepoPkgr)");
                            return false;
                        }
                        dctOrgDBParamsToGeneratedFileNameMap.Add(strDictionaryKey, orgDbNameGenerated);

                        lstGeneratedOrgDBNames.Add(orgDbNameGenerated);
                    }
                    // Add a new job parameter that associates orgDbNameGenerated with this job
                    mJobParams.AddAdditionalParameter("PeptideSearch", GetGeneratedFastaParamNameForJob(udtJob.Job),
                                                       orgDbNameGenerated);
                }

                // Store the names of the generated fasta files
                // This is a tab separated list of filenames
                StorePackedJobParameterList(lstGeneratedOrgDBNames, FASTA_FILES_FOR_DATA_PACKAGE);

                // Restore the dataset and job info for this aggregation job
                OverrideCurrentDatasetAndJobInfo(udtCurrentDatasetAndJobInfo);
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

        #endregion // Code_Adapted_From_Pride_Plugin

    }
}
