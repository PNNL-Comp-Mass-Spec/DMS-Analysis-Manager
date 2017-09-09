using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    /// <summary>
    /// Class for retrieving files to be submitted to ProteomeXchange
    /// </summary>
    /// <remarks>Named PRIDEConverter due us previously pushing data to PRIDE</remarks>
    public class clsAnalysisResourcesPRIDEConverter : clsAnalysisResources
    {
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
        /// Cache folder path
        /// </summary>
        /// <remarks>Named PeptideAtlas because we previously pushed data to PeptideAtlas</remarks>
        public const string DEFAULT_CACHE_FOLDER_PATH = @"\\protoapps\PeptideAtlas_Staging";

        /// <summary>
        /// Retrieve shared resources, including the JobParameters file from the previous job step
        /// </summary>
        /// <returns></returns>
        public override CloseOutType GetResources()
        {

            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            List<clsDataPackageJobInfo> lstDataPackagePeptideHitJobs;

            var blnCreatePrideXMLFiles = m_jobParams.GetJobParameter("CreatePrideXMLFiles", false);

            // Check whether we are only creating the .msgf files
            var blnCreateMSGFReportFilesOnly = m_jobParams.GetJobParameter("CreateMSGFReportFilesOnly", false);
            var udtOptions = new clsDataPackageFileHandler.udtDataPackageRetrievalOptionsType {
                CreateJobPathFiles = true
            };

            if (blnCreatePrideXMLFiles && !blnCreateMSGFReportFilesOnly)
            {
                udtOptions.RetrieveMzXMLFile = true;
            }
            else
            {
                udtOptions.RetrieveMzXMLFile = false;
            }

            if (blnCreatePrideXMLFiles)
            {
                udtOptions.RetrievePHRPFiles = true;
            }
            else
            {
                udtOptions.RetrievePHRPFiles = false;
            }

            udtOptions.RetrieveDTAFiles = m_jobParams.GetJobParameter("CreateMGFFiles", true);
            udtOptions.RetrieveMZidFiles = m_jobParams.GetJobParameter("IncludeMZidFiles", true);
            udtOptions.RetrievePepXMLFiles = m_jobParams.GetJobParameter("IncludePepXMLFiles", false);

            var disableMyEMSL = m_jobParams.GetJobParameter("DisableMyEMSL", false);
            if (disableMyEMSL)
            {
                DisableMyEMSLSearch();
            }

            udtOptions.AssumeInstrumentDataUnpurged = m_jobParams.GetJobParameter("AssumeInstrumentDataUnpurged", true);

            if (blnCreateMSGFReportFilesOnly)
            {
                udtOptions.RetrieveDTAFiles = false;
                udtOptions.RetrieveMZidFiles = false;
            }
            else
            {
                if (blnCreatePrideXMLFiles)
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
            // Possibly also obtain the .mzXML file or .Raw file for each dataset
            // The .mzXML file is required if we are creating Pride XML files (which were required for a "complete" submission
            //   prior to May 2013; we now submit .mzid.gz files, .mgf files, and instrument binary files and thus don't need the .mzXML file.
            //   However, if the MSGF+ search used searched a .mzML file and not a _dta.txt file, then we _do_ need the .mzid file)
            if (!RetrieveDataPackagePeptideHitJobPHRPFiles(udtOptions, out lstDataPackagePeptideHitJobs, 0,
                    clsAnalysisToolRunnerPRIDEConverter.PROGRESS_PCT_TOOL_RUNNER_STARTING))
            {
                return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;
            }

            // Obtain the FASTA files (typically generated from protein collections) used for the jobs in lstDataPackagePeptideHitJobs
            if (!RetrieveFastaFiles(lstDataPackagePeptideHitJobs))
            {
                return CloseOutType.CLOSEOUT_NO_FAS_FILES;
            }

            if (udtOptions.RetrieveMzXMLFile)
            {
                // Use lstDataPackagePeptideHitJobs to look for any datasets for which we will need to create a .mzXML file
                FindMissingMzXmlFiles(lstDataPackagePeptideHitJobs);
            }

            if (!m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            StoreDataPackageJobs(lstDataPackagePeptideHitJobs);

            return CloseOutType.CLOSEOUT_SUCCESS;
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
                foreach (var dataPkgJob in lstDataPackagePeptideHitJobs)
                {
                    var strMzXmlFilePath = Path.Combine(m_WorkingDir, dataPkgJob.Dataset + DOT_MZXML_EXTENSION);

                    if (!File.Exists(strMzXmlFilePath))
                    {
                        // Look for a StoragePathInfo file
                        strMzXmlFilePath += STORAGE_PATH_INFO_FILE_SUFFIX;
                        if (!File.Exists(strMzXmlFilePath))
                        {
                            if (!lstDatasets.Contains(dataPkgJob.Dataset))
                            {
                                lstDatasets.Add(dataPkgJob.Dataset);
                                lstDatasetYearQuarter.Add(dataPkgJob.Dataset + "=" + GetDatasetYearQuarter(dataPkgJob.ServerStoragePath));
                            }
                        }
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
                m_message = "Exception in FindMissingMzXmlFiles";
                LogError(m_message + ": " + ex.Message);
            }
        }

        /// <summary>
        /// Generated FASTA parameter name
        /// </summary>
        /// <param name="Job"></param>
        /// <returns></returns>
        public static string GetGeneratedFastaParamNameForJob(int Job)
        {
            return "Job" + Job + "_GeneratedFasta";
        }

        /// <summary>
        /// MSGF report template filename
        /// </summary>
        /// <param name="JobParams"></param>
        /// <param name="WarnIfJobParamMissing"></param>
        /// <returns></returns>
        public static string GetMSGFReportTemplateFilename(IJobParams JobParams, bool WarnIfJobParamMissing)
        {
            var strTemplateFileName = JobParams.GetJobParameter(JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME, string.Empty);

            if (string.IsNullOrEmpty(strTemplateFileName))
            {
                if (WarnIfJobParamMissing)
                {
                    clsGlobal.LogWarning(
                        "Job parameter " + JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME + " is empty; will assume " + strTemplateFileName);
                }
                strTemplateFileName = DEFAULT_MSGF_REPORT_TEMPLATE_FILENAME;
            }

            return strTemplateFileName;
        }

        /// <summary>
        /// PX submission template filename
        /// </summary>
        /// <param name="JobParams"></param>
        /// <param name="WarnIfJobParamMissing"></param>
        /// <returns></returns>
        public static string GetPXSubmissionTemplateFilename(IJobParams JobParams, bool WarnIfJobParamMissing)
        {
            var strTemplateFileName = JobParams.GetJobParameter(JOB_PARAM_PX_SUBMISSION_TEMPLATE_FILENAME, string.Empty);

            if (string.IsNullOrEmpty(strTemplateFileName))
            {
                strTemplateFileName = DEFAULT_PX_SUBMISSION_TEMPLATE_FILENAME;
                if (WarnIfJobParamMissing)
                {
                    clsGlobal.LogWarning(
                        "Job parameter " + JOB_PARAM_PX_SUBMISSION_TEMPLATE_FILENAME + " is empty; will assume " + strTemplateFileName);
                }
            }

            return strTemplateFileName;
        }

        protected bool RetrieveFastaFiles(IEnumerable<clsDataPackageJobInfo> lstDataPackagePeptideHitJobs)
        {
            var strLocalOrgDBFolder = m_mgrParams.GetParam("orgdbdir");

            try
            {
                // This dictionary is used to avoid calling RetrieveOrgDB() for every job
                // The dictionary keys are LegacyFastaFileName, ProteinOptions, and ProteinCollectionList combined with underscores
                // The dictionary values are the name of the generated (or retrieved) fasta file
                var dctOrgDBParamsToGeneratedFileNameMap = new Dictionary<string, string>();

                // Cache the current dataset and job info
                var currentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

                foreach (var dataPkgJob in lstDataPackagePeptideHitJobs)
                {
                    var strDictionaryKey = dataPkgJob.LegacyFastaFileName + "_" + dataPkgJob.ProteinCollectionList + "_" + dataPkgJob.ProteinOptions;

                    string strOrgDBNameGenerated;
                    if (dctOrgDBParamsToGeneratedFileNameMap.TryGetValue(strDictionaryKey, out strOrgDBNameGenerated))
                    {
                        // Organism DB was already generated
                    }
                    else
                    {
                        OverrideCurrentDatasetAndJobInfo(dataPkgJob);

                        m_jobParams.AddAdditionalParameter("PeptideSearch", "generatedFastaName", string.Empty);
                        if (!RetrieveOrgDB(strLocalOrgDBFolder))
                        {
                            if (string.IsNullOrEmpty(m_message))
                                m_message = "Call to RetrieveOrgDB returned false in clsAnalysisResourcesPRIDEConverter.RetrieveFastaFiles";
                            OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo);
                            return false;
                        }

                        strOrgDBNameGenerated = m_jobParams.GetJobParameter("PeptideSearch", "generatedFastaName", string.Empty);

                        if (string.IsNullOrEmpty(strOrgDBNameGenerated))
                        {
                            m_message = "FASTA file was not generated when RetrieveFastaFiles called RetrieveOrgDB";
                            LogError(
                                m_message + " (class clsAnalysisResourcesPRIDEConverter)");
                            OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo);
                            return false;
                        }

                        if (strOrgDBNameGenerated != dataPkgJob.OrganismDBName)
                        {
                            if (dataPkgJob.OrganismDBName == null)
                                dataPkgJob.OrganismDBName = "??";

                            m_message = "Generated FASTA file name (" + strOrgDBNameGenerated + ") does not match expected fasta file name (" +
                                        dataPkgJob.OrganismDBName + "); aborting";
                            LogError(
                                m_message + " (class clsAnalysisResourcesPRIDEConverter)");
                            OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo);
                            return false;
                        }

                        dctOrgDBParamsToGeneratedFileNameMap.Add(strDictionaryKey, strOrgDBNameGenerated);
                    }

                    // Add a new job parameter that associates strOrgDBNameGenerated with this job

                    m_jobParams.AddAdditionalParameter("PeptideSearch", GetGeneratedFastaParamNameForJob(dataPkgJob.Job), strOrgDBNameGenerated);
                }

                // Restore the dataset and job info for this aggregation job
                OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo);
            }
            catch (Exception ex)
            {
                m_message = "Exception in RetrieveFastaFiles";
                LogError(m_message, ex);
                return false;
            }

            return true;
        }

        protected bool RetrieveMSGFReportTemplateFile()
        {
            // Retrieve the template .msgf-pride.xml file
            // Although there is a default in the PRIDE_Converter parameter file folder, it should ideally be customized and placed in the data package folder

            try
            {
                var strTemplateFileName = GetMSGFReportTemplateFilename(m_jobParams, WarnIfJobParamMissing: true);

                // First look for the template file in the data package folder
                var strDataPackagePath = m_jobParams.GetJobParameter("JobParameters", "transferFolderPath", string.Empty);
                if (string.IsNullOrEmpty(strDataPackagePath))
                {
                    m_message = "Job parameter transferFolderPath is missing; unable to determine the data package folder path";
                    LogError(m_message);
                    return false;
                }

                var diDataPackageFolder = new DirectoryInfo(strDataPackagePath);
                var fiFiles = diDataPackageFolder.GetFiles(strTemplateFileName).ToList();

                if (fiFiles.Count == 0)
                {
                    // File not found; see if any files ending in MSGF_REPORT_FILE_SUFFIX exist in the data package folder
                    fiFiles = diDataPackageFolder.GetFiles("*" + MSGF_REPORT_FILE_SUFFIX).ToList();

                    if (fiFiles.Count == 0)
                    {
                        // File not found; see if any files containin MSGF_REPORT_FILE_SUFFIX exist in the data package folder
                        fiFiles = diDataPackageFolder.GetFiles("*" + MSGF_REPORT_FILE_SUFFIX + "*").ToList();
                    }
                }

                if (fiFiles.Count > 0)
                {
                    // Template file found in the data package; copy it locally
                    if (!FileSearch.RetrieveFile(fiFiles[0].Name, fiFiles[0].DirectoryName))
                    {
                        return false;
                    }
                    strTemplateFileName = fiFiles[0].Name;
                }
                else
                {
                    var strParamFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");
                    strTemplateFileName = DEFAULT_MSGF_REPORT_TEMPLATE_FILENAME;

                    LogWarning(
                        "MSGF Report template file not found in the data package folder; retrieving " + strTemplateFileName + "from " +
                        strParamFileStoragePath);

                    if (string.IsNullOrEmpty(strParamFileStoragePath))
                        strParamFileStoragePath = @"\\gigasax\dms_parameter_Files\PRIDE_Converter";

                    if (!FileSearch.RetrieveFile(strTemplateFileName, strParamFileStoragePath))
                    {
                        return false;
                    }
                }

                // Assure that the MSGF Report Template file job parameter is up-to-date
                m_jobParams.AddAdditionalParameter("JobParameters", JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME, strTemplateFileName);

                m_jobParams.AddResultFileToSkip(strTemplateFileName);
            }
            catch (Exception ex)
            {
                m_message = "Exception in RetrieveMSGFReportTemplateFile";
                LogError(m_message, ex);
                return false;
            }

            return true;
        }

        protected bool RetrievePXSubmissionTemplateFile()
        {
            // Retrieve the template PX Submission file
            // Although there is a default in the PRIDE_Converter parameter file folder, it should ideally be customized and placed in the data package folder

            try
            {
                var strTemplateFileName = GetPXSubmissionTemplateFilename(m_jobParams, WarnIfJobParamMissing: true);

                // First look for the template file in the data package folder
                // Note that transferFolderPath is likely \\protoapps\PeptideAtlas_Staging and not the real data package path

                var transferFolderPath = m_jobParams.GetJobParameter("JobParameters", "transferFolderPath", string.Empty);
                if (string.IsNullOrEmpty(transferFolderPath))
                {
                    m_message = "Job parameter transferFolderPath is missing; unable to determine the data package folder path";
                    LogError(m_message);
                    return false;
                }

                var ConnectionString = m_mgrParams.GetParam("brokerconnectionstring");
                var dataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1);

                var matchFound = false;
                var lstSourceFolders = new List<string> {
                    GetDataPackageStoragePath(ConnectionString, dataPackageID),
                    transferFolderPath
                };

                foreach (var sourceFolderPath in lstSourceFolders)
                {
                    if (string.IsNullOrEmpty(sourceFolderPath))
                        continue;

                    var diDataPackageFolder = new DirectoryInfo(sourceFolderPath);
                    var fiFiles = diDataPackageFolder.GetFiles(strTemplateFileName).ToList();

                    if (fiFiles.Count == 0)
                    {
                        // File not found; see if any files ending in PX_SUBMISSION_FILE_SUFFIX exist in the data package folder
                        fiFiles = diDataPackageFolder.GetFiles("*" + PX_SUBMISSION_FILE_SUFFIX).ToList();
                    }

                    if (fiFiles.Count > 0)
                    {
                        // Template file found in the data package; copy it locally
                        if (!FileSearch.RetrieveFile(fiFiles[0].Name, fiFiles[0].DirectoryName))
                        {
                            return false;
                        }
                        strTemplateFileName = fiFiles[0].Name;
                        matchFound = true;
                        break;
                    }
                }

                if (!matchFound)
                {
                    var strParamFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");
                    if (string.IsNullOrEmpty(strParamFileStoragePath))
                    {
                        strParamFileStoragePath = @"\\gigasax\dms_parameter_Files\PRIDE_Converter";
                    }
                    strTemplateFileName = DEFAULT_PX_SUBMISSION_TEMPLATE_FILENAME;

                    LogWarning(
                        "PX Submission template file not found in the data package folder; retrieving " + strTemplateFileName + " from " +
                        strParamFileStoragePath);

                    if (!FileSearch.RetrieveFile(strTemplateFileName, strParamFileStoragePath, 1))
                    {
                        if (string.IsNullOrEmpty(m_message))
                        {
                            m_message = "Template PX file " + strTemplateFileName + " to found in the data package folder";
                        }
                        return false;
                    }
                }

                // Assure that the PX Submission Template file job parameter is up-to-date
                m_jobParams.AddAdditionalParameter("JobParameters", JOB_PARAM_PX_SUBMISSION_TEMPLATE_FILENAME, strTemplateFileName);

                m_jobParams.AddResultFileToSkip(strTemplateFileName);
            }
            catch (Exception ex)
            {
                m_message = "Exception in RetrievePXSubmissionTemplateFile";
                LogError(m_message, ex);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Store the datasets and jobs tracked by lstDataPackagePeptideHitJobs into a packed job parameter
        /// </summary>
        /// <param name="lstDataPackagePeptideHitJobs"></param>
        /// <remarks></remarks>
        protected void StoreDataPackageJobs(IEnumerable<clsDataPackageJobInfo> lstDataPackagePeptideHitJobs)
        {
            var lstDataPackageJobs = new List<string>();

            foreach (var dataPkgJob in lstDataPackagePeptideHitJobs)
            {
                lstDataPackageJobs.Add(dataPkgJob.Job.ToString());
            }

            if (lstDataPackageJobs.Count > 0)
            {
                StorePackedJobParameterList(lstDataPackageJobs, JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS);
            }
        }
    }
}
