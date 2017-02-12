using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AnalysisManagerBase;

namespace AnalysisManagerPRIDEConverterPlugIn
{
    public class clsAnalysisResourcesPRIDEConverter : clsAnalysisResources
    {
        public const string JOB_PARAM_DATASETS_MISSING_MZXML_FILES = "PackedParam_DatasetsMissingMzXMLFiles";
        public const string JOB_PARAM_DATA_PACKAGE_PEPTIDE_HIT_JOBS = "PackedParam_DataPackagePeptideHitJobs";
        public const string JOB_PARAM_DICTIONARY_DATASET_STORAGE_YEAR_QUARTER = "PackedParam_DatasetStorage_YearQuarter";

        public const string JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME = "MSGFReportFileTemplate";
        public const string JOB_PARAM_PX_SUBMISSION_TEMPLATE_FILENAME = "PXSubmissionTemplate";

        public const string DEFAULT_MSGF_REPORT_TEMPLATE_FILENAME = "Template.msgf-report.xml";
        public const string MSGF_REPORT_FILE_SUFFIX = "msgf-report.xml";

        public const string DEFAULT_PX_SUBMISSION_TEMPLATE_FILENAME = "PX_Submission_Template.px";
        public const string PX_SUBMISSION_FILE_SUFFIX = ".px";

        public override CloseOutType GetResources()
        {
            // Retrieve shared resources, including the JobParameters file from the previous job step
            var result = GetSharedResources();
            if (result != CloseOutType.CLOSEOUT_SUCCESS)
            {
                return result;
            }

            var lstDataPackagePeptideHitJobs = new List<clsDataPackageJobInfo>();

            bool blnCreatePrideXMLFiles = m_jobParams.GetJobParameter("CreatePrideXMLFiles", false);

            // Check whether we are only creating the .msgf files
            bool blnCreateMSGFReportFilesOnly = m_jobParams.GetJobParameter("CreateMSGFReportFilesOnly", false);
            clsDataPackageFileHandler.udtDataPackageRetrievalOptionsType udtOptions =
                new clsDataPackageFileHandler.udtDataPackageRetrievalOptionsType();

            udtOptions.CreateJobPathFiles = true;

            if (blnCreatePrideXMLFiles & !blnCreateMSGFReportFilesOnly)
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
            if (!base.RetrieveDataPackagePeptideHitJobPHRPFiles(udtOptions, out lstDataPackagePeptideHitJobs, 0,
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
                foreach (clsDataPackageJobInfo dataPkgJob in lstDataPackagePeptideHitJobs)
                {
                    string strMzXmlFilePath = null;
                    strMzXmlFilePath = Path.Combine(m_WorkingDir, dataPkgJob.Dataset + DOT_MZXML_EXTENSION);

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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message + ": " + ex.Message);
            }
        }

        public static string GetGeneratedFastaParamNameForJob(int Job)
        {
            return "Job" + Job.ToString() + "_GeneratedFasta";
        }

        public static string GetMSGFReportTemplateFilename(IJobParams JobParams, bool WarnIfJobParamMissing)
        {
            string strTemplateFileName = JobParams.GetJobParameter(JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME, string.Empty);

            if (string.IsNullOrEmpty(strTemplateFileName))
            {
                if (WarnIfJobParamMissing)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                        "Job parameter " + JOB_PARAM_MSGF_REPORT_TEMPLATE_FILENAME + " is empty; will assume " + strTemplateFileName);
                }
                strTemplateFileName = DEFAULT_MSGF_REPORT_TEMPLATE_FILENAME;
            }

            return strTemplateFileName;
        }

        public static string GetPXSubmissionTemplateFilename(IJobParams JobParams, bool WarnIfJobParamMissing)
        {
            string strTemplateFileName = JobParams.GetJobParameter(JOB_PARAM_PX_SUBMISSION_TEMPLATE_FILENAME, string.Empty);

            if (string.IsNullOrEmpty(strTemplateFileName))
            {
                strTemplateFileName = DEFAULT_PX_SUBMISSION_TEMPLATE_FILENAME;
                if (WarnIfJobParamMissing)
                {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                        "Job parameter " + JOB_PARAM_PX_SUBMISSION_TEMPLATE_FILENAME + " is empty; will assume " + strTemplateFileName);
                }
            }

            return strTemplateFileName;
        }

        protected bool RetrieveFastaFiles(IEnumerable<clsDataPackageJobInfo> lstDataPackagePeptideHitJobs)
        {
            string strLocalOrgDBFolder = m_mgrParams.GetParam("orgdbdir");

            string strDictionaryKey = null;

            string strOrgDBNameGenerated = string.Empty;

            try
            {
                // This dictionary is used to avoid calling RetrieveOrgDB() for every job
                // The dictionary keys are LegacyFastaFileName, ProteinOptions, and ProteinCollectionList combined with underscores
                // The dictionary values are the name of the generated (or retrieved) fasta file
                var dctOrgDBParamsToGeneratedFileNameMap = new Dictionary<string, string>();

                // Cache the current dataset and job info
                var currentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

                foreach (clsDataPackageJobInfo dataPkgJob in lstDataPackagePeptideHitJobs)
                {
                    strDictionaryKey = dataPkgJob.LegacyFastaFileName + "_" + dataPkgJob.ProteinCollectionList + "_" + dataPkgJob.ProteinOptions;

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
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
                                m_message + " (class clsAnalysisResourcesPRIDEConverter)");
                            OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo);
                            return false;
                        }

                        if (strOrgDBNameGenerated != dataPkgJob.OrganismDBName)
                        {
                            if (strOrgDBNameGenerated == null)
                                strOrgDBNameGenerated = "??";
                            if (dataPkgJob.OrganismDBName == null)
                                dataPkgJob.OrganismDBName = "??";

                            m_message = "Generated FASTA file name (" + strOrgDBNameGenerated + ") does not match expected fasta file name (" +
                                        dataPkgJob.OrganismDBName + "); aborting";
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR,
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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
                return false;
            }

            return true;
        }

        protected bool RetrieveMSGFReportTemplateFile()
        {
            // Retrieve the template .msgf-pride.xml file
            // Although there is a default in the PRIDE_Converter parameter file folder, it should ideally be customized and placed in the data package folder

            string strTemplateFileName = null;

            try
            {
                strTemplateFileName = GetMSGFReportTemplateFilename(m_jobParams, WarnIfJobParamMissing: true);

                // First look for the template file in the data package folder
                string strDataPackagePath = m_jobParams.GetJobParameter("JobParameters", "transferFolderPath", string.Empty);
                if (string.IsNullOrEmpty(strDataPackagePath))
                {
                    m_message = "Job parameter transferFolderPath is missing; unable to determine the data package folder path";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
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
                    else
                    {
                        strTemplateFileName = fiFiles[0].Name;
                    }
                }
                else
                {
                    string strParamFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");
                    strTemplateFileName = DEFAULT_MSGF_REPORT_TEMPLATE_FILENAME;

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
                        "MSGF Report template file not found in the data package folder; retrieving " + strTemplateFileName + "from " +
                        strParamFileStoragePath);

                    if (string.IsNullOrEmpty(strParamFileStoragePath))
                        strParamFileStoragePath = "\\\\gigasax\\dms_parameter_Files\\PRIDE_Converter";

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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
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

                string transferFolderPath = m_jobParams.GetJobParameter("JobParameters", "transferFolderPath", string.Empty);
                if (string.IsNullOrEmpty(transferFolderPath))
                {
                    m_message = "Job parameter transferFolderPath is missing; unable to determine the data package folder path";
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message);
                    return false;
                }

                string ConnectionString = m_mgrParams.GetParam("brokerconnectionstring");
                int dataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1);

                var matchFound = false;
                var lstSourceFolders = new List<string>();

                lstSourceFolders.Add(GetDataPackageStoragePath(ConnectionString, dataPackageID));
                lstSourceFolders.Add(transferFolderPath);

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
                        else
                        {
                            strTemplateFileName = fiFiles[0].Name;
                            matchFound = true;
                            break;
                        }
                    }
                }

                if (!matchFound)
                {
                    string strParamFileStoragePath = m_jobParams.GetParam("ParmFileStoragePath");
                    if (string.IsNullOrEmpty(strParamFileStoragePath))
                    {
                        strParamFileStoragePath = @"\\gigasax\dms_parameter_Files\PRIDE_Converter";
                    }
                    strTemplateFileName = DEFAULT_PX_SUBMISSION_TEMPLATE_FILENAME;

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN,
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
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, m_message, ex);
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

            foreach (clsDataPackageJobInfo dataPkgJob in lstDataPackagePeptideHitJobs)
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
