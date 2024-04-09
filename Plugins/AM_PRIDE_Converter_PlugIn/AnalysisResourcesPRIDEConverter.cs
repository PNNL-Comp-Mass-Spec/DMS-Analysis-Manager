using PRISM.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase.AnalysisTool;
using AnalysisManagerBase.JobConfig;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    /// <summary>
    /// Class for retrieving files to be submitted to ProteomeXchange
    /// </summary>
    /// <remarks>Named PRIDEConverter due to us previously pushing data to PRIDE</remarks>
    public class AnalysisResourcesPRIDEConverter : AnalysisResources
    {
        // Ignore Spelling: FASTA, msgf, MzID, MzXML, Parm, protoapps, Xchange

        /// <summary>
        /// Packed parameter DatasetsMissingMzXMLFiles
        /// </summary>
        public const string JOB_PARAM_DATASETS_MISSING_MZXML_FILES = "PackedParam_DatasetsMissingMzXMLFiles";

        /// <summary>
        /// Packed parameter DataPackagePeptideHitJobs
        /// </summary>
        public const string JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS = "PackedParam_DataPackagePeptideHitJobs";

        /// <summary>
        /// Packed parameter DatasetStorage_YearQuarter
        /// </summary>
        public const string JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER = "PackedParam_DatasetStorage_YearQuarter";

        /// <summary>
        /// Job parameter for MSGF report template file
        /// </summary>
        public const string JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME = "MSGFReportFileTemplate";

        /// <summary>
        /// Job parameter for PX submission template file
        /// </summary>
        public const string JOB_PARAM_PX_SUBMISSION_TEMPLATE_FILENAME = "PXSubmissionTemplate";

        /// <summary>
        /// Default MSGF report template filename
        /// </summary>
        public const string DEFAULT_MSGF_REPORT_TEMPLATE_FILENAME = "Template.msgf-report.xml";

        /// <summary>
        /// MSGF report file suffix
        /// </summary>
        public const string MSGF_REPORT_FILE_SUFFIX = "msgf-report.xml";

        /// <summary>
        /// Default PX submission template filename
        /// </summary>
        public const string DEFAULT_PX_SUBMISSION_TEMPLATE_FILENAME = "PX_Submission_Template.px";

        /// <summary>
        /// PX submission file suffix
        /// </summary>
        public const string PX_SUBMISSION_FILE_SUFFIX = ".px";

        /// <summary>
        /// Cache directory path
        /// </summary>
        public const string DEFAULT_CACHE_DIRECTORY_PATH = @"\\protoapps\MassIVE_Staging";

        /// <summary>
        /// Retrieve required files
        /// </summary>
        /// <returns>Closeout code</returns>
        public override CloseOutType GetResources()
        {
            var result = GetSharedResources();

            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            var createPrideXMLFiles = mJobParams.GetJobParameter("CreatePrideXMLFiles", false);

            var cacheFolderPath = mJobParams.GetJobParameter("CacheFolderPath", DEFAULT_CACHE_DIRECTORY_PATH);

            var resultsFolderName = mJobParams.GetParam(JOB_PARAM_OUTPUT_FOLDER_NAME);

            if (string.IsNullOrWhiteSpace(resultsFolderName))
            {
                LogError("Job parameter OutputFolderName is empty");
                return CloseOutType.CLOSEOUT_FAILED;
            }

            var remoteTransferDirectoryPath = Path.Combine(cacheFolderPath, resultsFolderName);

            // Check whether we are only creating the .msgf files
            var createMSGFReportFilesOnly = mJobParams.GetJobParameter("CreateMSGFReportFilesOnly", false);

            var retrievalOptions = new DataPackageFileHandler.DataPackageRetrievalOptionsType
            {
                CreateJobPathFiles = true,
                RemoteTransferDirectoryPath = remoteTransferDirectoryPath,
                RetrieveMzXMLFile = createPrideXMLFiles && !createMSGFReportFilesOnly,
                RetrievePHRPFiles = createPrideXMLFiles,
                RetrieveDTAFiles = mJobParams.GetJobParameter("CreateMGFFiles", true),
                RetrieveMzidFiles = mJobParams.GetJobParameter("IncludeMZidFiles", true),
                RetrievePepXMLFiles = mJobParams.GetJobParameter("IncludePepXMLFiles", false)
            };

            var disableMyEMSL = mJobParams.GetJobParameter("DisableMyEMSL", false);

            if (disableMyEMSL)
            {
                DisableMyEMSLSearch();
            }

            retrievalOptions.AssumeInstrumentDataUnpurged = mJobParams.GetJobParameter("AssumeInstrumentDataUnpurged", true);

            if (createMSGFReportFilesOnly)
            {
                retrievalOptions.RetrieveDTAFiles = false;
                retrievalOptions.RetrieveMzidFiles = false;
            }
            else
            {
                if (createPrideXMLFiles)
                {
                    if (!RetrieveMSGFReportTemplateFile())
                    {
                        return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                    }
                }

                if (!RetrievePXSubmissionTemplateFile())
                {
                    return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
                }
            }

            // Obtain the PHRP-related files for the Peptide_Hit jobs defined for the data package associated with this data aggregation job
            // Possibly also obtain the .mzML file or .Raw file for each dataset
            // Starting in 2013, we switched to submitting .mzid.gz files, .mgf files, and instrument binary files and thus don't need the .mzML file.
            // However, if the MS-GF+ search used a .mzML file and not a _dta.txt file, we _do_ need the .mzML file

            // Prior to May 2013 the .mzML file was required when creating Pride XML files for a "complete" submission
            // However, that mode is no longer required

            var filesRetrieved = RetrieveDataPackagePeptideHitJobPHRPFiles(
                retrievalOptions,
                out var dataPackagePeptideHitJobs,
                0,
                AnalysisToolRunnerPRIDEConverter.PROGRESS_PCT_TOOL_RUNNER_STARTING);

            if (!filesRetrieved)
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Validate the FASTA files (typically generated from protein collections) used for the jobs in dataPackagePeptideHitJobs
            if (!ValidateFastaFiles(dataPackagePeptideHitJobs))
            {
                return CloseOutType.CLOSEOUT_NO_FAS_FILES;
            }

            if (retrievalOptions.RetrieveMzXMLFile)
            {
                // Use dataPackagePeptideHitJobs to look for any datasets for which we will need to create a .mzXML file
                FindMissingMzXmlFiles(dataPackagePeptideHitJobs);
            }

            if (!mMyEMSLUtilities.ProcessMyEMSLDownloadQueue(mWorkDir, MyEMSLReader.Downloader.DownloadLayout.FlatNoSubdirectories))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            StoreDataPackageJobs(dataPackagePeptideHitJobs);

            return CloseOutType.CLOSEOUT_SUCCESS;
        }

        /// <summary>
        /// Find datasets that do not have a .mzXML file
        /// Datasets that need to have .mzXML files created will be added to the packed job parameters, storing the dataset names in "PackedParam_DatasetsMissingMzXMLFiles"
        /// and the dataset Year_Quarter values in "PackedParam_DatasetStorage_YearQuarter"
        /// </summary>
        /// <param name="dataPackagePeptideHitJobs"></param>
        private void FindMissingMzXmlFiles(IEnumerable<DataPackageJobInfo> dataPackagePeptideHitJobs)
        {
            var datasets = new SortedSet<string>();
            var datasetYearQuarter = new SortedSet<string>();

            try
            {
                foreach (var dataPkgJob in dataPackagePeptideHitJobs)
                {
                    var mzXmlFilePath = Path.Combine(mWorkDir, dataPkgJob.Dataset + DOT_MZXML_EXTENSION);

                    if (!File.Exists(mzXmlFilePath))
                    {
                        // Look for a StoragePathInfo file
                        mzXmlFilePath += STORAGE_PATH_INFO_FILE_SUFFIX;

                        if (!File.Exists(mzXmlFilePath))
                        {
                            if (!datasets.Contains(dataPkgJob.Dataset))
                            {
                                datasets.Add(dataPkgJob.Dataset);
                                datasetYearQuarter.Add(dataPkgJob.Dataset + "=" + GetDatasetYearQuarter(dataPkgJob.ServerStoragePath));
                            }
                        }
                    }
                }

                if (datasets.Count > 0)
                {
                    StorePackedJobParameterList(datasets.ToList(), JOB_PARAM_DATASETS_MISSING_MZXML_FILES);
                    StorePackedJobParameterList(datasetYearQuarter.ToList(), JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER);
                }
            }
            catch (Exception ex)
            {
                mMessage = "Exception in FindMissingMzXmlFiles";
                LogError(mMessage + ": " + ex.Message);
            }
        }

        /// <summary>
        /// Generated FASTA parameter name
        /// </summary>
        /// <param name="job"></param>
        public static string GetGeneratedFastaParamNameForJob(int job)
        {
            return "Job" + job + "_GeneratedFasta";
        }

        /// <summary>
        /// MSGF report template filename
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="warnIfJobParamMissing"></param>
        public static string GetMSGFReportTemplateFilename(IJobParams jobParams, bool warnIfJobParamMissing)
        {
            var templateFileName = jobParams.GetJobParameter(JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME, string.Empty);

            if (string.IsNullOrEmpty(templateFileName))
            {
                if (warnIfJobParamMissing)
                {
                    LogTools.LogWarning(
                        "Job parameter " + JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME + " is empty; will assume " + templateFileName);
                }
                templateFileName = DEFAULT_MSGF_REPORT_TEMPLATE_FILENAME;
            }

            return templateFileName;
        }

        /// <summary>
        /// PX submission template filename
        /// </summary>
        /// <param name="JobParams"></param>
        /// <param name="WarnIfJobParamMissing"></param>
        public static string GetPXSubmissionTemplateFilename(IJobParams JobParams, bool WarnIfJobParamMissing)
        {
            var templateFileName = JobParams.GetJobParameter(JOB_PARAM_PX_SUBMISSION_TEMPLATE_FILENAME, string.Empty);

            if (string.IsNullOrEmpty(templateFileName))
            {
                templateFileName = DEFAULT_PX_SUBMISSION_TEMPLATE_FILENAME;

                if (WarnIfJobParamMissing)
                {
                    LogTools.LogWarning(
                        "Job parameter " + JOB_PARAM_PX_SUBMISSION_TEMPLATE_FILENAME + " is empty; will assume " + templateFileName);
                }
            }

            return templateFileName;
        }

        /// <summary>
        /// Prior to September 2020 we retrieved the FASTA files associated with each job
        /// This tool does not actually use FASTA files, so we instead simply validate that the jobs have a FASTA file associated with them
        /// </summary>
        /// <param name="dataPackagePeptideHitJobs"></param>
        /// <returns>True if each job has a FASTA file, otherwise false</returns>
        private bool ValidateFastaFiles(IEnumerable<DataPackageJobInfo> dataPackagePeptideHitJobs)
        {
            var orgDbDirectoryPath = mMgrParams.GetParam("OrgDbDir");

            try
            {
                // This dictionary is used to avoid calling RetrieveOrgDB() for every job
                // The dictionary keys are LegacyFastaFileName, ProteinOptions, and ProteinCollectionList combined with underscores
                // The dictionary values are the value stored in GeneratedFastaName by RetrieveOrgDB
                var orgDBParamsToGeneratedFileNameMap = new Dictionary<string, string>();

                // Cache the current dataset and job info
                var currentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

                foreach (var dataPkgJob in dataPackagePeptideHitJobs)
                {
                    var dictionaryKey = dataPkgJob.LegacyFastaFileName + "_" + dataPkgJob.ProteinCollectionList + "_" + dataPkgJob.ProteinOptions;

                    if (orgDBParamsToGeneratedFileNameMap.TryGetValue(dictionaryKey, out var proteinCollectionListOrLegacyFastaName))
                    {
                        // We already processed this protein collection list of legacy FASTA file
                    }
                    else
                    {
                        OverrideCurrentDatasetAndJobInfo(dataPkgJob);

                        mJobParams.AddAdditionalParameter(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName", string.Empty);

                        if (!RetrieveOrgDB(orgDbDirectoryPath, out _, true))
                        {
                            if (string.IsNullOrEmpty(mMessage))
                                mMessage = "Call to RetrieveOrgDB returned false in AnalysisResourcesPRIDEConverter.RetrieveFastaFiles";
                            OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo);
                            return false;
                        }

                        proteinCollectionListOrLegacyFastaName = mJobParams.GetJobParameter(AnalysisJob.PEPTIDE_SEARCH_SECTION, "GeneratedFastaName", string.Empty);

                        if (string.IsNullOrEmpty(proteinCollectionListOrLegacyFastaName))
                        {
                            mMessage = "FASTA file was not generated when RetrieveFastaFiles called RetrieveOrgDB";
                            LogError(mMessage + " (class AnalysisResourcesPRIDEConverter)");
                            OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo);
                            return false;
                        }

                        orgDBParamsToGeneratedFileNameMap.Add(dictionaryKey, proteinCollectionListOrLegacyFastaName);
                    }

                    // Add a new job parameter that associates proteinCollectionListOrLegacyFastaName with this job
                    // This value was previously used by method CreateMSGFReportFile in AnalysisToolRunnerPRIDEConverter,
                    // but that method (and related methods) were deprecated in September 2020
                    mJobParams.AddAdditionalParameter(AnalysisJob.PEPTIDE_SEARCH_SECTION, GetGeneratedFastaParamNameForJob(dataPkgJob.Job), proteinCollectionListOrLegacyFastaName);
                }

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

        private bool RetrieveMSGFReportTemplateFile()
        {
            // Retrieve the template .msgf-pride.xml file
            // Although there is a default in the PRIDE_Converter parameter file directory, it should ideally be customized and placed in the data package directory

            try
            {
                var templateFileName = GetMSGFReportTemplateFilename(mJobParams, warnIfJobParamMissing: true);

                // First look for the template file in the data package directory
                var dataPackagePath = mJobParams.GetJobParameter(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_TRANSFER_DIRECTORY_PATH, string.Empty);

                if (string.IsNullOrEmpty(dataPackagePath))
                {
                    mMessage = "Job parameter transferDirectoryPath is missing; unable to determine the data package directory path";
                    LogError(mMessage);
                    return false;
                }

                var dataPackageDirectory = new DirectoryInfo(dataPackagePath);
                var matchingFiles = dataPackageDirectory.GetFiles(templateFileName).ToList();

                if (matchingFiles.Count == 0)
                {
                    // File not found; see if any files ending in MSGF_REPORT_FILE_SUFFIX exist in the data package directory
                    matchingFiles = dataPackageDirectory.GetFiles("*" + MSGF_REPORT_FILE_SUFFIX).ToList();

                    if (matchingFiles.Count == 0)
                    {
                        // File not found; see if any files containing MSGF_REPORT_FILE_SUFFIX exist in the data package directory
                        matchingFiles = dataPackageDirectory.GetFiles("*" + MSGF_REPORT_FILE_SUFFIX + "*").ToList();
                    }
                }

                if (matchingFiles.Count > 0)
                {
                    // Template file found in the data package; copy it locally
                    if (!FileSearchTool.RetrieveFile(matchingFiles[0].Name, matchingFiles[0].DirectoryName))
                    {
                        return false;
                    }
                    templateFileName = matchingFiles[0].Name;
                }
                else
                {
                    var paramFileStoragePath = mJobParams.GetParam("ParamFileStoragePath");
                    templateFileName = DEFAULT_MSGF_REPORT_TEMPLATE_FILENAME;

                    LogWarning(
                        "MSGF Report template file not found in the data package directory; retrieving " + templateFileName + "from " +
                        paramFileStoragePath);

                    if (string.IsNullOrEmpty(paramFileStoragePath))
                        paramFileStoragePath = @"\\gigasax\dms_parameter_Files\PRIDE_Converter";

                    if (!FileSearchTool.RetrieveFile(templateFileName, paramFileStoragePath))
                    {
                        return false;
                    }
                }

                // Assure that the MSGF Report Template file job parameter is up-to-date
                mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME, templateFileName);

                mJobParams.AddResultFileToSkip(templateFileName);
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrieveMSGFReportTemplateFile";
                LogError(mMessage, ex);
                return false;
            }

            return true;
        }

        private bool RetrievePXSubmissionTemplateFile()
        {
            // Retrieve the template PX Submission file
            // Although there is a default parameter file in the PRIDE_Converter parameter file directory, it should ideally be customized and placed in the data package directory

            try
            {
                var templateFileName = GetPXSubmissionTemplateFilename(mJobParams, WarnIfJobParamMissing: true);

                // First look for the template file in the data package directory
                // Note that transferDirectoryPath is likely \\protoapps\PeptideAtlas_Staging and not the real data package path

                var transferDirectoryPath = mJobParams.GetJobParameter(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_TRANSFER_DIRECTORY_PATH, string.Empty);

                if (string.IsNullOrEmpty(transferDirectoryPath))
                {
                    mMessage = "Job parameter transferDirectoryPath is missing; unable to determine the data package directory path";
                    LogError(mMessage);
                    return false;
                }

                var connectionString = mMgrParams.GetParam("BrokerConnectionString");

                var dataPackageID = mJobParams.GetJobParameter("DataPackageID", -1);
                var dataPackageStoragePath = GetDataPackageStoragePath(connectionString, dataPackageID);

                var matchFound = false;
                var sourceDirectories = new List<string> {
                    dataPackageStoragePath,
                    transferDirectoryPath
                };

                foreach (var sourceDirectoryPath in sourceDirectories)
                {
                    if (string.IsNullOrEmpty(sourceDirectoryPath))
                        continue;

                    var dataPackageDirectory = new DirectoryInfo(sourceDirectoryPath);
                    var matchingFiles = dataPackageDirectory.GetFiles(templateFileName).ToList();

                    if (matchingFiles.Count == 0)
                    {
                        // File not found; see if any files ending in PX_SUBMISSION_FILE_SUFFIX exist in the data package folder
                        matchingFiles = dataPackageDirectory.GetFiles("*" + PX_SUBMISSION_FILE_SUFFIX).ToList();
                    }

                    if (matchingFiles.Count > 0)
                    {
                        // Template file found in the data package; copy it locally
                        if (!FileSearchTool.RetrieveFile(matchingFiles[0].Name, matchingFiles[0].DirectoryName))
                        {
                            return false;
                        }
                        templateFileName = matchingFiles[0].Name;
                        matchFound = true;
                        break;
                    }
                }

                if (!matchFound)
                {
                    var paramFileStoragePath = mJobParams.GetParam("ParamFileStoragePath");

                    if (string.IsNullOrEmpty(paramFileStoragePath))
                    {
                        paramFileStoragePath = @"\\gigasax\dms_parameter_Files\PRIDE_Converter";
                    }
                    templateFileName = DEFAULT_PX_SUBMISSION_TEMPLATE_FILENAME;

                    if (string.IsNullOrEmpty(dataPackageStoragePath))
                    {
                        LogWarning("View V_DMS_Data_Packages does not have data package {0} (or column Share_Path is empty);" +
                                   "unable to retrieve the PX Submission template file from the data package directory", dataPackageID);
                    }
                    else
                    {
                        LogWarning("PX Submission template file not found in the data package directory: {0}; looked for both {1} and any .px file", dataPackageStoragePath, DEFAULT_PX_SUBMISSION_TEMPLATE_FILENAME);
                    }

                    LogWarning("Retrieving {0} from {1}", templateFileName, paramFileStoragePath);

                    if (!FileSearchTool.RetrieveFile(templateFileName, paramFileStoragePath, 1))
                    {
                        if (string.IsNullOrEmpty(mMessage))
                        {
                            mMessage = string.Format("Template PX file {0} not found in {1}", templateFileName, paramFileStoragePath);
                        }
                        return false;
                    }
                }

                // Assure that the PX Submission Template file job parameter is up-to-date
                mJobParams.AddAdditionalParameter(AnalysisJob.JOB_PARAMETERS_SECTION, JOB_PARAM_PX_SUBMISSION_TEMPLATE_FILENAME, templateFileName);

                mJobParams.AddResultFileToSkip(templateFileName);
            }
            catch (Exception ex)
            {
                mMessage = "Exception in RetrievePXSubmissionTemplateFile";
                LogError(mMessage, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Store the datasets and jobs tracked by dataPackagePeptideHitJobs into a packed job parameter
        /// </summary>
        /// <param name="dataPackagePeptideHitJobs"></param>
        private void StoreDataPackageJobs(IEnumerable<DataPackageJobInfo> dataPackagePeptideHitJobs)
        {
            var dataPackageJobs = new List<string>();

            foreach (var dataPkgJob in dataPackagePeptideHitJobs)
            {
                dataPackageJobs.Add(dataPkgJob.Job.ToString());
            }

            if (dataPackageJobs.Count > 0)
            {
                StorePackedJobParameterList(dataPackageJobs, JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS);
            }
        }
    }
}
