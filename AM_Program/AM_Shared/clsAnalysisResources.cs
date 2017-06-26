
using System;
using System.Collections.Generic;
using System.Data;
using PHRPReader;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Xml;
using System.Xml.Linq;
using MyEMSLReader;
using ParamFileGenerator.MakeParams;
using Renci.SshNet.Sftp;

//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2007, Battelle Memorial Institute
// Created 12/19/2007
//*********************************************************************************************************

namespace AnalysisManagerBase
{
    /// <summary>
    /// Base class for job resource class
    /// </summary>
    public abstract class clsAnalysisResources : clsAnalysisMgrBase, IAnalysisResources
    {

        #region "Constants"

        public const string MYEMSL_PATH_FLAG = clsMyEMSLUtilities.MYEMSL_PATH_FLAG;

        // Note: All of the RAW_DATA_TYPE constants need to be all lowercase

        /// <summary>
        /// Agilent ion trap data, Agilent TOF data
        /// </summary>
        public const string RAW_DATA_TYPE_DOT_D_FOLDERS = "dot_d_folders";

        /// <summary>
        /// FTICR data, including instrument 3T_FTICR, 7T_FTICR, 9T_FTICR, 11T_FTICR, 11T_FTICR_B, and 12T_FTICR
        /// </summary>
        public const string RAW_DATA_TYPE_ZIPPED_S_FOLDERS = "zipped_s_folders";

        /// <summary>
        /// Micromass QTOF data
        /// </summary>
        public const string RAW_DATA_TYPE_DOT_RAW_FOLDER = "dot_raw_folder";

        /// <summary>
        /// Finnigan ion trap/LTQ-FT data
        /// </summary>
        public const string RAW_DATA_TYPE_DOT_RAW_FILES = "dot_raw_files";

        /// <summary>
        /// Agilent/QSTAR TOF data
        /// </summary>
        public const string RAW_DATA_TYPE_DOT_WIFF_FILES = "dot_wiff_files";

        /// <summary>
        /// IMS_UIMF (IMS_Agilent_TOF in DMS)
        /// </summary>
        public const string RAW_DATA_TYPE_DOT_UIMF_FILES = "dot_uimf_files";

        /// <summary>
        /// mzXML
        /// </summary>
        public const string RAW_DATA_TYPE_DOT_MZXML_FILES = "dot_mzxml_files";

        /// <summary>
        /// mzML
        /// </summary>
        public const string RAW_DATA_TYPE_DOT_MZML_FILES = "dot_mzml_files";

        // 12T datasets acquired prior to 7/16/2010 use a Bruker data station and have an analysis.baf file, 0.ser folder, and a XMASS_Method.m subfolder with file apexAcquisition.method
        // Datasets will have an instrument name of 12T_FTICR and raw_data_type of "zipped_s_folders"

        // 12T datasets acquired after 9/1/2010 use the Agilent data station, and thus have a .D folder
        // Datasets will have an instrument name of 12T_FTICR_B and raw_data_type of "bruker_ft"
        // 15T datasets also have raw_data_type "bruker_ft"
        // Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a apexAcquisition.method file
        public const string RAW_DATA_TYPE_BRUKER_FT_FOLDER = "bruker_ft";

        // The following is used by BrukerTOF_01 (e.g. Bruker TOF_TOF)
        // Folder has a .EMF file and a single sub-folder that has an acqu file and fid file
        public const string RAW_DATA_TYPE_BRUKER_MALDI_SPOT = "bruker_maldi_spot";

        // The following is used by instruments 9T_FTICR_Imaging and BrukerTOF_Imaging_01
        // Series of zipped subfolders, with names like 0_R00X329.zip; subfolders inside the .Zip files have fid files
        public const string RAW_DATA_TYPE_BRUKER_MALDI_IMAGING = "bruker_maldi_imaging";

        // The following is used by instrument Maxis_01
        // Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a microTOFQMaxAcquisition.method file; there is not a ser or fid file
        public const string RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER = "bruker_tof_baf";
        public const string RESULT_TYPE_SEQUEST = "Peptide_Hit";
        public const string RESULT_TYPE_XTANDEM = "XT_Peptide_Hit";
        public const string RESULT_TYPE_INSPECT = "IN_Peptide_Hit";

        // Used for MSGFDB and MSGF+
        public const string RESULT_TYPE_MSGFPLUS = "MSG_Peptide_Hit";
        public const string RESULT_TYPE_MSALIGN = "MSA_Peptide_Hit";
        public const string RESULT_TYPE_MODA = "MODa_Peptide_Hit";
        public const string RESULT_TYPE_MODPLUS = "MODPlus_Peptide_Hit";

        public const string RESULT_TYPE_MSPATHFINDER = "MSP_Peptide_Hit";
        public const string DOT_WIFF_EXTENSION = ".wiff";
        public const string DOT_D_EXTENSION = ".d";
        public const string DOT_RAW_EXTENSION = ".raw";

        public const string DOT_UIMF_EXTENSION = ".uimf";
        public const string DOT_GZ_EXTENSION = ".gz";
        public const string DOT_MZXML_EXTENSION = ".mzXML";

        public const string DOT_MZML_EXTENSION = ".mzML";
        public const string DOT_MGF_EXTENSION = ".mgf";

        public const string DOT_CDF_EXTENSION = ".cdf";

        public const string DOT_PBF_EXTENSION = ".pbf";

        /// <summary>
        /// Feature file, generated by the ProMex tool
        /// </summary>
        /// <remarks></remarks>
        public const string DOT_MS1FT_EXTENSION = ".ms1ft";

        public const string STORAGE_PATH_INFO_FILE_SUFFIX = clsFileCopyUtilities.STORAGE_PATH_INFO_FILE_SUFFIX;
        public const string SCAN_STATS_FILE_SUFFIX = "_ScanStats.txt";

        public const string SCAN_STATS_EX_FILE_SUFFIX = "_ScanStatsEx.txt";

        public const string SIC_STATS_FILE_SUFFIX = "_SICStats.txt";

        public const string REPORTERIONS_FILE_SUFFIX = "_ReporterIons.txt";

        public const string DATA_PACKAGE_SPECTRA_FILE_SUFFIX = "_SpectraFile";
        public const string BRUKER_ZERO_SER_FOLDER = "0.ser";
        public const string BRUKER_SER_FILE = "ser";

        public const string BRUKER_FID_FILE = "fid";

        public const string JOB_PARAM_DICTIONARY_DATASET_FILE_PATHS = "PackedParam_DatasetFilePaths";

        // This is used by clsAnalysisResourcesRepoPkgr
        public const string JOB_PARAM_DICTIONARY_DATASET_RAW_DATA_TYPES = "PackedParam_DatasetRawDataTypes";

        // These are used by clsAnalysisResourcesPhosphoFdrAggregator
        public const string JOB_PARAM_DICTIONARY_JOB_DATASET_MAP = "PackedParam_JobDatasetMap";
        public const string JOB_PARAM_DICTIONARY_JOB_SETTINGS_FILE_MAP = "PackedParam_JobSettingsFileMap";

        public const string JOB_PARAM_DICTIONARY_JOB_TOOL_MAP = "PackedParam_JobToolNameMap";

        public const string JOB_PARAM_GENERATED_FASTA_NAME = "generatedFastaName";

        // This constant is used by clsAnalysisToolRunnerMSGFDB, clsAnalysisResourcesMSGFDB, and clsAnalysisResourcesDtaRefinery
        public const string SPECTRA_ARE_NOT_CENTROIDED = "None of the spectra are centroided; unable to process";

        /// <summary>
        /// Instrument data file type enum
        /// </summary>
        public enum eRawDataTypeConstants
        {
            Unknown = 0,
            ThermoRawFile = 1,
            UIMF = 2,
            mzXML = 3,
            mzML = 4,
            AgilentDFolder = 5,
            // Agilent ion trap data, Agilent TOF data
            AgilentQStarWiffFile = 6,
            MicromassRawFolder = 7,
            // Micromass QTOF data
            ZippedSFolders = 8,
            // FTICR data, including instrument 3T_FTICR, 7T_FTICR, 9T_FTICR, 11T_FTICR, 11T_FTICR_B, and 12T_FTICR
            BrukerFTFolder = 9,
            // .D folder is the analysis.baf file; there is also .m subfolder that has a apexAcquisition.method file
            BrukerMALDISpot = 10,
            // has a .EMF file and a single sub-folder that has an acqu file and fid file
            BrukerMALDIImaging = 11,
            // Series of zipped subfolders, with names like 0_R00X329.zip; subfolders inside the .Zip files have fid files
            BrukerTOFBaf = 12
            // Used by Maxis01; Inside the .D folder is the analysis.baf file; there is also .m subfolder that has a microTOFQMaxAcquisition.method file; there is not a ser or fid file
        }

        /// <summary>
        /// Enum for mzXML and mzML
        /// </summary>
        public enum MSXMLOutputTypeConstants
        {
            mzXML = 0,
            mzML = 1
        }

        /// <summary>
        /// Used by Phospho_FDR_Aggregator, Pride_MzXML, and AScore plugins
        /// </summary>
        public enum DataPackageFileRetrievalModeConstants
        {
            Undefined = 0,
            Ascore = 1
        }

        #endregion

        #region "Module variables"
        protected IJobParams m_jobParams;
        protected IMgrParams m_mgrParams;
        protected string m_WorkingDir;
        protected int m_JobNum;

        /// <summary>
        /// Dataset name
        /// </summary>
        /// <remarks>
        /// Update the dataset name using property DatasetName
        /// because we also need to propogate that change
        /// into m_FolderSearch and m_FileSearch
        /// </remarks>
        private string m_DatasetName;

        protected string m_MgrName;

        protected string m_FastaToolsCnStr = "";

        protected string m_FastaFileName = "";
        protected Protein_Exporter.clsGetFASTAFromDMS m_FastaTools;

        protected readonly clsCDTAUtilities m_CDTAUtilities;

        protected clsSplitFastaFileUtilities m_SplitFastaFileUtility;
        protected DateTime m_SplitFastaLastUpdateTime;

        protected float m_SplitFastaLastPercentComplete;

        private DateTime m_LastCDTAUtilitiesUpdateTime;

        private DateTime m_FastaToolsLastLogTime;
        private double m_FastaToolFractionDoneSaved = -1;

        protected clsMyEMSLUtilities m_MyEMSLUtilities;

        private Dictionary<clsGlobal.eAnalysisResourceOptions, bool> m_ResourceOptions;
        private bool m_AuroraAvailable;

        private bool m_MyEMSLSearchDisabled;

        private clsDataPackageJobInfo mCachedDatasetAndJobInfo;

        private bool mCachedDatasetAndJobInfoIsDefined;

        /// <summary>
        /// Spectrum type classifier
        /// </summary>
        public SpectraTypeClassifier.clsSpectrumTypeClassifier mSpectraTypeClassifier;

        private clsFileCopyUtilities m_FileCopyUtilities;

        private clsFolderSearch m_FolderSearch;

        private clsFileSearch m_FileSearch;

        #endregion

        #region "Properties"

        /// <summary>
        /// Dataset name
        /// </summary>
        /// <remarks>Also updates m_FolderSearch and m_FileSearch</remarks>
        public string DatasetName
        {
            get => m_DatasetName;
            set
            {
                m_DatasetName = value;

                if (m_FolderSearch != null)
                    m_FolderSearch.DatasetName = m_DatasetName;

                if (m_FileSearch != null)
                    m_FileSearch.DatasetName = m_DatasetName;
            }
        }

        /// <summary>
        /// Debug level
        /// </summary>
        /// <remarks>Ranges from 0 (minimum output) to 5 (max detail)</remarks>
        public short DebugLevel => m_DebugLevel;

        /// <summary>
        /// Folder search utility
        /// </summary>
        public clsFolderSearch FolderSearch => m_FolderSearch;

        /// <summary>
        /// File search utility
        /// </summary>
        public clsFileSearch FileSearch => m_FileSearch;

        /// <summary>
        /// True when MyEMSL search is diabled
        /// </summary>
        public bool MyEMSLSearchDisabled
        {
            get => m_MyEMSLSearchDisabled;
            set
            {
                m_MyEMSLSearchDisabled = value;
                if (m_FolderSearch != null)
                {
                    if (m_MyEMSLSearchDisabled && !m_FolderSearch.MyEMSLSearchDisabled)
                        m_FolderSearch.MyEMSLSearchDisabled = true;
                }

                if (m_FileSearch != null)
                {
                    if (m_MyEMSLSearchDisabled && !m_FileSearch.MyEMSLSearchDisabled)
                        m_FileSearch.MyEMSLSearchDisabled = true;
                }

            }
        }

        /// <summary>
        /// MyEMSL utilities
        /// </summary>
        public clsMyEMSLUtilities MyEMSLUtilities => m_MyEMSLUtilities;

        /// <summary>
        ///  Explanation of what happened to last operation this class performed
        /// </summary>
        public string Message => m_message;

        /// <summary>
        /// True when the step tool is DataExtractor
        /// </summary>
        private bool RunningDataExtraction => string.Equals(StepToolName, "DataExtractor", StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// Step tool name
        /// </summary>
        public string StepToolName { get; private set; }

        /// <summary>
        /// Work directory
        /// </summary>
        public string WorkDir => m_WorkingDir;

        #endregion

        #region "Methods"
        /// <summary>

        /// Constructor
        /// </summary>
        /// <remarks></remarks>
        protected clsAnalysisResources() : base("clsAnalysisResources")
        {
            m_CDTAUtilities = new clsCDTAUtilities();
            RegisterEvents(m_CDTAUtilities);
            m_CDTAUtilities.ProgressUpdate -= ProgressUpdateHandler;
            m_CDTAUtilities.ProgressUpdate += m_CDTAUtilities_ProgressEvent;

        }

        /// <summary>
        /// Initialize class
        /// </summary>
        /// <param name="stepToolName">Name of the current step tool</param>
        /// <param name="mgrParams">Manager parameter object</param>
        /// <param name="jobParams">Job parameter object</param>
        /// <param name="statusTools">Object for status reporting</param>
        /// <param name="myEMSLUtilities">MyEMSL download Utilities (can be nothing)</param>
        /// <remarks></remarks>
        public virtual void Setup(
            string stepToolName,
            IMgrParams mgrParams,
            IJobParams jobParams,
            IStatusFile statusTools,
            clsMyEMSLUtilities myEMSLUtilities)
        {
            StepToolName = stepToolName;

            m_mgrParams = mgrParams;
            m_jobParams = jobParams;

            m_DebugLevel = (short)m_mgrParams.GetParam("debuglevel", 1);
            m_FastaToolsCnStr = m_mgrParams.GetParam("fastacnstring");
            m_MgrName = m_mgrParams.GetParam("MgrName", "Undefined-Manager");

            m_WorkingDir = m_mgrParams.GetParam("workdir");

            var jobNum = m_jobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job");
            if (!string.IsNullOrEmpty(jobNum))
            {
                int.TryParse(jobNum, out m_JobNum);
            }

            DatasetName = m_jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetNum");

            InitFileTools(m_MgrName, m_DebugLevel);

            m_MyEMSLUtilities = myEMSLUtilities ?? new clsMyEMSLUtilities(m_DebugLevel, m_WorkingDir);

            RegisterEvents(m_MyEMSLUtilities);

            m_FileCopyUtilities = new clsFileCopyUtilities(m_FileTools, m_MyEMSLUtilities, m_MgrName, m_DebugLevel);
            RegisterEvents(m_FileCopyUtilities);

            m_FileCopyUtilities.ResetTimestampForQueueWaitTime += FileCopyUtilities_ResetTimestampForQueueWaitTime;

            m_FileCopyUtilities.CopyWithLocksComplete += FileCopyUtilities_CopyWithLocksComplete;

            m_ResourceOptions = new Dictionary<clsGlobal.eAnalysisResourceOptions, bool>();
            SetOption(clsGlobal.eAnalysisResourceOptions.OrgDbRequired, false);
            SetOption(clsGlobal.eAnalysisResourceOptions.MyEMSLSearchDisabled, false);

            m_StatusTools = statusTools;

            m_AuroraAvailable = m_mgrParams.GetParam("AuroraAvailable", true);

            var myEmslAvailable = m_mgrParams.GetParam("MyEmslAvailable", true);

            m_FolderSearch = new clsFolderSearch(
                m_FileCopyUtilities, m_jobParams, m_MyEMSLUtilities,
                m_DatasetName, m_DebugLevel, m_AuroraAvailable);
            RegisterEvents(m_FolderSearch);

            m_FolderSearch.MyEMSLSearchDisabled = m_MyEMSLSearchDisabled || !myEmslAvailable;

            m_FileSearch = new clsFileSearch(
                m_FileCopyUtilities, m_FolderSearch, m_MyEMSLUtilities,
                m_mgrParams, m_jobParams, m_DatasetName, m_DebugLevel, m_WorkingDir, m_AuroraAvailable);

            RegisterEvents(m_FileSearch);

            m_FileSearch.MyEMSLSearchDisabled = m_MyEMSLSearchDisabled || !myEmslAvailable;
        }

        /// <summary>
        /// Copy the generated FASTA file to the remote host that will be running this job
        /// </summary>
        /// <param name="transferUtility"></param>
        /// <returns>True if success, false if an error</returns>
        protected bool CopyGeneratedOrgDBToRemote(clsRemoteTransferUtility transferUtility)
        {
            var dbFilename = m_jobParams.GetParam("PeptideSearch", JOB_PARAM_GENERATED_FASTA_NAME);
            if (string.IsNullOrWhiteSpace(dbFilename))
            {
                LogError("Cannot copy the generated FASTA remotely; parameter " + JOB_PARAM_GENERATED_FASTA_NAME + " is empty");
                return false;
            }

            var localOrgDbFolderPath = m_mgrParams.GetParam("orgdbdir");
            if (string.IsNullOrWhiteSpace(localOrgDbFolderPath))
            {
                LogError("Cannot copy the generated FASTA remotely; manager parameter orgdbdir is empty");
                return false;
            }

            var localOrgDbFolder = new DirectoryInfo(localOrgDbFolderPath);

            var sourceFasta = new FileInfo(Path.Combine(localOrgDbFolder.FullName, dbFilename));

            if (!sourceFasta.Exists)
            {
                LogError("Cannot copy the generated FASTA remotely; file not found: " + sourceFasta.FullName);
                return false;
            }

            var hashcheckFiles = localOrgDbFolder.GetFiles(sourceFasta.Name + "*.hashcheck");
            if (hashcheckFiles.Length <= 0)
            {
                LogError("Local hashcheck file not found for " + sourceFasta.FullName + "; cannot copy remotely");
                return false;
            }

            var sourceHashcheck = hashcheckFiles.First();

            LogDebug("Verifying that the generated fasta file exists on the remote host");

            // Check whether the file needs to be copied
            // Skip the copy if it exists and has the same size
            // Also require the .hashcheck file to exist and match
            var fileMatchSpec = Path.GetFileNameWithoutExtension(sourceFasta.Name) + "*.*";
            var matchingFiles = transferUtility.GetRemoteFileListing(transferUtility.RemoteOrgDBPath, fileMatchSpec);

            if (matchingFiles.Count > 0)
            {
                SftpFile remoteFasta = null;
                SftpFile remoteHashcheck = null;

                foreach (var remoteFile in matchingFiles)
                {
                    var extension = Path.GetExtension(remoteFile.Value.Name);
                    if (extension == null)
                        continue;

                    if (string.Equals(extension, ".fasta", StringComparison.OrdinalIgnoreCase))
                        remoteFasta = remoteFile.Value;

                    if (string.Equals(extension, ".hashcheck", StringComparison.OrdinalIgnoreCase))
                        remoteHashcheck = remoteFile.Value;
                }

                var filesMatch = RemoteFastaFilesMatch(sourceFasta, sourceHashcheck, remoteFasta, remoteHashcheck, transferUtility);

                if (filesMatch && remoteFasta != null)
                {
                    LogDebug(string.Format("Using existing FASTA file {0} on {1}",
                                           remoteFasta.FullName, transferUtility.RemoteHostName));
                    return true;
                }

            }
            else
            {
                LogDebug(string.Format("Fasta file not found on remote host; copying {0} to {1}", sourceFasta.Name, transferUtility.RemoteHostName));
            }

            var sourceFiles = new List<FileInfo> {
                sourceFasta,
                sourceHashcheck
            };

            var success = transferUtility.CopyFilesToRemote(sourceFiles, transferUtility.RemoteOrgDBPath, useLockFile: true);
            if (success)
                return true;

            LogError(string.Format("Error copying {0} to {1} on {2}", sourceFasta.Name, transferUtility.RemoteOrgDBPath,
                                   transferUtility.RemoteHostName));
            return false;

        }

        /// <summary>
        /// Copy files in the working directory to a remote host, skipping files in filesToIgnore
        /// </summary>
        /// <param name="transferUtility">file transfer utility</param>
        /// <param name="filesToIgnore">Names of files to ignore</param>
        /// <remarks>This method is called by step tools that override CopyResourcesToRemote</remarks>
        /// <returns>True if success, otherwise false</returns>
        protected bool CopyWorkDirFilesToRemote(clsRemoteTransferUtility transferUtility, SortedSet<string> filesToIgnore)
        {

            try
            {
                LogDebug("Copying work dir files to remote host " + transferUtility.RemoteHostName);

                // Find files in the working directory to copy
                var filesToCopy = GetWorkDirFiles(filesToIgnore).ToList();

                if (filesToCopy.Count == 0)
                {
                    LogError("Nothing to copy to the remote host; did not find any eligible files in the working directory");
                    return false;
                }

                // Create the destination folder
                var remoteDirectoryPath = transferUtility.RemoteJobStepWorkDirPath;
                var remoteHost = transferUtility.RemoteHostName;

                var targetFolderVerified = transferUtility.CreateRemoteDirectory(remoteDirectoryPath);
                if (!targetFolderVerified)
                {
                    LogError(string.Format("Unable to create working directory {0} on host {1}", remoteDirectoryPath, remoteHost));
                    m_message = "Unable to create working directory on remote host " + remoteHost;
                    return false;
                }

                // Copy the files
                var success = transferUtility.CopyFilesToRemote(filesToCopy, remoteDirectoryPath);

                if (success)
                {
                    LogMessage(string.Format("Copied {0} files to {1} on host {2}",
                                             filesToCopy.Count, remoteDirectoryPath, remoteHost));
                    return true;
                }

                LogError(string.Format("Failure copying {0} files to {1} on host {2}",
                                         filesToCopy.Count, remoteDirectoryPath, remoteHost));

                m_message = "Failure copying required files to remote host " + remoteHost;
                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception copying resources to remote host " + transferUtility.RemoteHostName, ex);
                return false;
            }

        }

        /// <summary>
        /// Call this function to copy files from the working directory to a remote host for remote processing
        /// Plugins that implement this will skip files that are not be needed by the ToolRunner class of the plugin
        /// Plugins should also copy fasta files if appropriate
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        public virtual bool CopyResourcesToRemote(clsRemoteTransferUtility transferUtility)
        {
            throw new NotImplementedException("Plugin " + StepToolName + " must implement CopyResourcesToRemote to allow for remote processing");
        }

        /// <summary>
        /// Abstract method to retrieve resources
        /// </summary>
        /// <returns></returns>
        /// <remarks>Each step tool implements this method</remarks>
        public abstract CloseOutType GetResources();

        /// <summary>
        /// Retrieve a true/false option of type eAnalysisResourceOptions
        /// </summary>
        /// <param name="resourceOption"></param>
        /// <returns></returns>
        public bool GetOption(clsGlobal.eAnalysisResourceOptions resourceOption)
        {
            if (m_ResourceOptions == null)
                return false;

            if (m_ResourceOptions.TryGetValue(resourceOption, out var enabled))
            {
                return enabled;
            }

            return false;
        }

        /// <summary>
        /// Gets resources required by all step tools
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        protected CloseOutType GetSharedResources()
        {

            var success = GetExistingJobParametersFile();

            if (success)
            {
                return CloseOutType.CLOSEOUT_SUCCESS;
            }

            return CloseOutType.CLOSEOUT_FAILED;
        }

        /// <summary>
        /// Set a true/false option of type eAnalysisResourceOptions
        /// </summary>
        /// <param name="resourceOption"></param>
        /// <param name="enabled"></param>
        public void SetOption(clsGlobal.eAnalysisResourceOptions resourceOption, bool enabled)
        {
            if (m_ResourceOptions == null)
            {
                m_ResourceOptions = new Dictionary<clsGlobal.eAnalysisResourceOptions, bool>();
            }

            if (m_ResourceOptions.ContainsKey(resourceOption))
            {
                m_ResourceOptions[resourceOption] = enabled;
            }
            else
            {
                m_ResourceOptions.Add(resourceOption, enabled);
            }

            if (resourceOption == clsGlobal.eAnalysisResourceOptions.MyEMSLSearchDisabled)
            {
                MyEMSLSearchDisabled = enabled;
            }
        }

        /// <summary>
        /// Add a filename extension to not move to the results folder
        /// </summary>
        /// <param name="extension"></param>
        /// <remarks>Can be a file extension (like .raw) or even a partial file name like _peaks.txt</remarks>
        public void AddResultFileExtensionToSkip(string extension)
        {
            m_jobParams.AddResultFileExtensionToSkip(extension);
        }

        /// <summary>
        /// Add a filename to not move to the results folder
        /// </summary>
        /// <param name="sourceFilename"></param>
        /// <remarks>FileName can be a file path; only the filename will be stored in m_ResultFilesToSkip</remarks>
        public void AddResultFileToSkip(string sourceFilename)
        {
            m_jobParams.AddResultFileToSkip(sourceFilename);
        }

        /// <summary>
        /// Appends file specified file path to the JobInfo file for the given Job
        /// </summary>
        /// <param name="job"></param>
        /// <param name="filePath"></param>
        /// <remarks></remarks>
        protected void AppendToJobInfoFile(int job, string filePath)
        {
            var jobInfoFilePath = clsDataPackageFileHandler.GetJobInfoFilePath(job, m_WorkingDir);

            using (var swJobInfoFile = new StreamWriter(new FileStream(jobInfoFilePath, FileMode.Append, FileAccess.Write, FileShare.Read)))
            {
                swJobInfoFile.WriteLine(filePath);
            }

        }

        /// <summary>
        /// Cache current dataset and job info
        /// </summary>
        /// <remarks>Restore the cached info using RestoreCachedDataAndJobInfo</remarks>
        public void CacheCurrentDataAndJobInfo()
        {
            if (mCachedDatasetAndJobInfoIsDefined)
            {
                throw new Exception("Call RestoreCachedDataAndJobInfo before calling CacheCurrentDataAndJobInfo again");
            }

            // Cache the current dataset and job info
            mCachedDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();
            mCachedDatasetAndJobInfoIsDefined = true;

        }

        /// <summary>
        /// Restore cached datasets and job info
        /// </summary>
        public void RestoreCachedDataAndJobInfo()
        {
            if (!mCachedDatasetAndJobInfoIsDefined)
            {
                throw new Exception("Programming error: RestoreCachedDataAndJobInfo called but mCachedDatasetAndJobInfoIsDefined is false");
            }

            // Restore the dataset and job info for this aggregation job
            OverrideCurrentDatasetAndJobInfo(mCachedDatasetAndJobInfo);

            mCachedDatasetAndJobInfoIsDefined = false;

        }

        /// <summary>
        /// Look for a lock file named dataFilePath + ".lock"
        /// If found, and if less than maxWaitTimeMinutes old, waits for it to be deleted by another process or to age
        /// </summary>
        /// <param name="dataFilePath">Data file path</param>
        /// <param name="dataFileDescription">User friendly description of the data file, e.g. LipidMapsDB</param>
        /// <param name="statusTools">Status Tools object</param>
        /// <param name="maxWaitTimeMinutes">Maximum age of the lock file</param>
        /// <param name="logIntervalMinutes"></param>
        /// <remarks>
        /// Typical steps for using lock files to assure that only one manager is creating a specific file
        /// 1. Call CheckForLockFile() to check for a lock file; wait for it to age
        /// 2. Once CheckForLockFile() exits, check for the required data file; exit the function if the desired file is found
        /// 3. If the file was not found, create a new lock file by calling CreateLockFile()
        /// 4. Do the work and create the data file, including copying to the central location
        /// 5. Delete the lock file by calling DeleteLockFile() or by deleting the file path returned by CreateLockFile()
        /// </remarks>
        public static void CheckForLockFile(
            string dataFilePath,
            string dataFileDescription,
            IStatusFile statusTools,
            int maxWaitTimeMinutes = 120,
            int logIntervalMinutes = 5)
        {
            if (dataFilePath.EndsWith(clsGlobal.LOCK_FILE_EXTENSION, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("dataFilePath may not end in .lock", nameof(dataFilePath));

            var waitingForLockFile = false;
            var dtLockFileCreated = DateTime.UtcNow;

            // Look for a recent .lock file
            var fiLockFile = new FileInfo(dataFilePath + clsGlobal.LOCK_FILE_EXTENSION);

            if (fiLockFile.Exists)
            {
                if (DateTime.UtcNow.Subtract(fiLockFile.LastWriteTimeUtc).TotalMinutes < maxWaitTimeMinutes)
                {
                    waitingForLockFile = true;
                    dtLockFileCreated = fiLockFile.LastWriteTimeUtc;

                    var debugMessage = dataFileDescription +
                        " lock file found; will wait for file to be deleted or age; " +
                        fiLockFile.Name + " created " + fiLockFile.LastWriteTime.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT);
                    clsGlobal.LogDebug(debugMessage);
                }
                else
                {
                    // Lock file has aged; delete it
                    fiLockFile.Delete();
                }
            }

            if (waitingForLockFile)
            {
                var dtLastProgressTime = DateTime.UtcNow;
                if (logIntervalMinutes < 1)
                    logIntervalMinutes = 1;

                while (waitingForLockFile)
                {
                    // Wait 5 seconds
                    Thread.Sleep(5000);

                    fiLockFile.Refresh();

                    if (!fiLockFile.Exists)
                    {
                        // Lock file no longer exists
                        waitingForLockFile = false;
                    }
                    else if (DateTime.UtcNow.Subtract(dtLockFileCreated).TotalMinutes > maxWaitTimeMinutes)
                    {
                        // We have waited too long
                        waitingForLockFile = false;
                    }
                    else
                    {
                        if (DateTime.UtcNow.Subtract(dtLastProgressTime).TotalMinutes >= logIntervalMinutes)
                        {
                            LogDebugMessage("Waiting for lock file " + fiLockFile.Name, statusTools);
                            dtLastProgressTime = DateTime.UtcNow;
                        }
                    }
                }

                // Check for the lock file one more time
                fiLockFile.Refresh();
                if (fiLockFile.Exists)
                {
                    // Lock file is over 2 hours old; delete it
                    DeleteLockFile(dataFilePath);
                }
            }
        }

        /// <summary>
        /// Create a new lock file named dataFilePath + ".lock"
        /// </summary>
        /// <param name="dataFilePath">Data file path</param>
        /// <param name="taskDescription">Description of current task; will be written the lock file, followed by " at yyyy-MM-dd hh:mm:ss tt"</param>
        /// <returns>Full path to the lock file</returns>
        /// <remarks>An exception will be thrown if the lock file already exists</remarks>
        public static string CreateLockFile(string dataFilePath, string taskDescription)
        {

            var lockFilePath = dataFilePath + clsGlobal.LOCK_FILE_EXTENSION;
            using (var swLockFile = new StreamWriter(new FileStream(lockFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.Read)))
            {
                swLockFile.WriteLine(taskDescription + " at " + DateTime.Now.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
            }

            return lockFilePath;

        }

        /// <summary>
        /// Delete the lock file for the correspond data file
        /// </summary>
        /// <param name="dataFilePath"></param>
        /// <remarks></remarks>
        public static void DeleteLockFile(string dataFilePath)
        {
            try
            {
                var lockFilePath = dataFilePath + LOCK_FILE_EXTENSION;

                var fiLockFile = new FileInfo(lockFilePath);
                if (fiLockFile.Exists)
                {
                    fiLockFile.Delete();
                }

            }
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>If the file was found in MyEMSL, sourceFolderPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        protected bool CopyFileToWorkDir(string sourceFileName, string sourceFolderPath, string targetFolderPath)
        {
            return m_FileCopyUtilities.CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath);
        }

        /// <summary>
        /// Copies specified file from storage server to local working directory
        /// </summary>
        /// <param name="sourceFileName">Name of file to copy</param>
        /// <param name="sourceFolderPath">Path to folder where input file is located</param>
        /// <param name="targetFolderPath">Destination directory for file copy</param>
        /// <param name="logMsgTypeIfNotFound">Type of message to log if the file is not found</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>If the file was found in MyEMSL, then sourceFolderPath will be of the form \\MyEMSL@MyEMSLID_84327</remarks>
        public bool CopyFileToWorkDir(string sourceFileName, string sourceFolderPath, string targetFolderPath, clsLogTools.LogLevels logMsgTypeIfNotFound)
        {

            return m_FileCopyUtilities.CopyFileToWorkDir(sourceFileName, sourceFolderPath, targetFolderPath, logMsgTypeIfNotFound);
        }

        /// <summary>
        /// Creates a Fasta file based on Ken's DLL
        /// </summary>
        /// <param name="proteinCollectionInfo"></param>
        /// <param name="destFolder">Folder where file will be created</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks></remarks>
        protected bool CreateFastaFile(clsProteinCollectionInfo proteinCollectionInfo, string destFolder)
        {

            if (m_DebugLevel >= 1)
            {
                LogMessage("Creating fasta file at " + destFolder);
            }

            if (!Directory.Exists(destFolder))
            {
                Directory.CreateDirectory(destFolder);
            }

            // Instantiate fasta tool if not already done
            if (m_FastaTools == null)
            {
                if (string.IsNullOrWhiteSpace(m_FastaToolsCnStr))
                {
                    m_message = "Protein database connection string not specified";
                    LogMessage("Error in CreateFastaFile: " + m_message, 0, true);
                    return false;
                }

            }

            var retryCount = 1;

            while (retryCount > 0)
            {
                try
                {
                    m_FastaTools = new Protein_Exporter.clsGetFASTAFromDMS(m_FastaToolsCnStr);
                    RegisterEvents(m_FastaTools);

                    m_FastaTools.FileGenerationProgress += m_FastaTools_FileGenerationProgress;
                    m_FastaTools.FileGenerationCompleted += m_FastaTools_FileGenerationCompleted;

                    break;
                }
                catch (Exception ex)
                {
                    if (retryCount > 1)
                    {
                        LogError("Error instantiating clsGetFASTAFromDMS", ex);
                        // Sleep 20 seconds after the first failure and 30 seconds after the second failure
                        if (retryCount == 3)
                        {
                            Thread.Sleep(20000);
                        }
                        else
                        {
                            Thread.Sleep(30000);
                        }
                    }
                    else
                    {
                        m_message = "Error retrieving protein collection or legacy FASTA file: ";
                        if (ex.Message.Contains("could not open database connection"))
                        {
                            m_message += "could not open database connection";
                        }
                        else
                        {
                            m_message += ex.Message;
                        }
                        LogError(m_message, ex);
                        LogDebugMessage("Connection string: " + m_FastaToolsCnStr);
                        LogDebugMessage("Current user: " + Environment.UserName);
                        return false;
                    }
                    retryCount -= 1;
                }
            }

            // Initialize fasta generation state variables
            m_FastaFileName = string.Empty;

            // Set up variables for fasta creation call

            if (!proteinCollectionInfo.IsValid)
            {
                if (string.IsNullOrWhiteSpace(proteinCollectionInfo.ErrorMessage))
                {
                    m_message = "Unknown error determining the Fasta file or protein collection to use; unable to obtain Fasta file";
                }
                else
                {
                    m_message = proteinCollectionInfo.ErrorMessage + "; unable to obtain Fasta file";
                }

                LogMessage("Error in CreateFastaFile: " + m_message, 0, true);
                return false;
            }

            string legacyFastaToUse;
            var orgDBDescription = string.Copy(proteinCollectionInfo.OrgDBDescription);

            if (proteinCollectionInfo.UsingSplitFasta && !RunningDataExtraction)
            {
                if (!proteinCollectionInfo.UsingLegacyFasta)
                {
                    LogError("Cannot use protein collections when running a SplitFasta job; choose a Legacy fasta file instead");
                    return false;
                }

                // Running a SplitFasta job; need to update the name of the fasta file to be of the form FastaFileName_NNx_nn.fasta
                // where NN is the number of total cloned steps and nn is this job's specific step number

                legacyFastaToUse = GetSplitFastaFileName(m_jobParams, out m_message, out var numberOfClonedSteps);

                if (string.IsNullOrEmpty(legacyFastaToUse))
                {
                    // The error should have already been logged
                    return false;
                }

                orgDBDescription = "Legacy DB: " + legacyFastaToUse;

                // Lookup connection strings
                // Proteinseqs.Protein_Sequences
                var proteinSeqsDBConnectionString = m_mgrParams.GetParam("fastacnstring");
                if (string.IsNullOrWhiteSpace(proteinSeqsDBConnectionString))
                {
                    LogError("Error in CreateFastaFile: manager parameter fastacnstring is not defined");
                    return false;
                }

                // Gigasax.DMS5
                var dmsConnectionString = m_mgrParams.GetParam("connectionstring");
                if (string.IsNullOrWhiteSpace(proteinSeqsDBConnectionString))
                {
                    LogError("Error in CreateFastaFile: manager parameter connectionstring is not defined");
                    return false;
                }

                // Lookup the MSGFPlus Index Folder path
                var strMSGFPlusIndexFilesFolderPathLegacyDB = m_mgrParams.GetParam("MSGFPlusIndexFilesFolderPathLegacyDB", @"\\Proto-7\MSGFPlus_Index_Files");
                if (string.IsNullOrWhiteSpace(strMSGFPlusIndexFilesFolderPathLegacyDB))
                {
                    strMSGFPlusIndexFilesFolderPathLegacyDB = @"\\Proto-7\MSGFPlus_Index_Files\Other";
                }
                else
                {
                    strMSGFPlusIndexFilesFolderPathLegacyDB = Path.Combine(strMSGFPlusIndexFilesFolderPathLegacyDB, "Other");
                }

                if (m_DebugLevel >= 1)
                {
                    LogMessage("Verifying that split fasta file exists: " + legacyFastaToUse);
                }

                // Make sure the original fasta file has already been split into the appropriate number parts
                // and that DMS knows about them
                //
                // Do not use RegisterEvents with m_SplitFastaFileUtility because we handle the progress with a custom handler
                m_SplitFastaFileUtility = new clsSplitFastaFileUtilities(dmsConnectionString, proteinSeqsDBConnectionString, numberOfClonedSteps, m_MgrName);

                m_SplitFastaFileUtility.SplittingBaseFastafile += m_SplitFastaFileUtility_SplittingBaseFastaFile;
                m_SplitFastaFileUtility.ErrorEvent += m_SplitFastaFileUtility_ErrorEvent;
                m_SplitFastaFileUtility.ProgressUpdate += m_SplitFastaFileUtility_ProgressUpdate;

                m_SplitFastaFileUtility.MSGFPlusIndexFilesFolderPathLegacyDB = strMSGFPlusIndexFilesFolderPathLegacyDB;

                m_SplitFastaLastUpdateTime = DateTime.UtcNow;
                m_SplitFastaLastPercentComplete = 0;

                var success = m_SplitFastaFileUtility.ValidateSplitFastaFile(proteinCollectionInfo.LegacyFastaName, legacyFastaToUse);
                if (!success)
                {
                    m_message = m_SplitFastaFileUtility.ErrorMessage;
                    return false;
                }

            }
            else
            {
                legacyFastaToUse = string.Copy(proteinCollectionInfo.LegacyFastaName);
            }

            if (m_DebugLevel >= 2)
            {
                LogMessage(
                    "ProteinCollectionList=" + proteinCollectionInfo.ProteinCollectionList + "; " +
                    "CreationOpts=" + proteinCollectionInfo.ProteinCollectionOptions + "; " +
                    "LegacyFasta=" + legacyFastaToUse);
            }

            try
            {
                if (m_FastaTools == null)
                {
                    LogError("Call to CreateFastaFile without initializing m_FastaTools");
                    return false;
                }

                var hashString = m_FastaTools.ExportFASTAFile(proteinCollectionInfo.ProteinCollectionList, proteinCollectionInfo.ProteinCollectionOptions, legacyFastaToUse, destFolder);

                if (string.IsNullOrEmpty(hashString))
                {
                    // Fasta generator returned empty hash string
                    LogError("m_FastaTools.ExportFASTAFile returned an empty Hash string for the OrgDB; unable to continue; " + orgDBDescription);
                    return false;
                }

            }
            catch (Exception ex)
            {
                if (ex.Message.StartsWith("Legacy fasta file not found:"))
                {
                    var rePathMatcher = new Regex(@"not found: (?<SourceFolder>.+)\\");
                    var reMatch = rePathMatcher.Match(ex.Message);
                    if (reMatch.Success)
                    {
                        m_message = "Legacy fasta file not found at " + reMatch.Groups["SourceFolder"].Value;
                    }
                    else
                    {
                        m_message = "Legacy fasta file not found in the organism folder for this job";
                    }
                }
                else
                {
                    m_message = "Exception generating OrgDb file";
                }

                LogError("Exception generating OrgDb file; " + orgDBDescription, ex);
                return false;
            }

            if (string.IsNullOrEmpty(m_FastaFileName))
            {
                // Fasta generator never raised event FileGenerationCompleted
                LogError("m_FastaTools did not raise event FileGenerationCompleted; unable to continue; " + orgDBDescription);
                return false;
            }

            var fiFastaFile = new FileInfo(Path.Combine(destFolder, m_FastaFileName));

            if (m_DebugLevel >= 1)
            {
                // Log the name of the .Fasta file we're using
                LogDebugMessage("Fasta generation complete, using database: " + m_FastaFileName, null);

                if (m_DebugLevel >= 2)
                {
                    // Also log the file creation and modification dates

                    try
                    {
                        var fastaFileMsg = "Fasta file last modified: " +
                            GetHumanReadableTimeInterval(
                                DateTime.UtcNow.Subtract(fiFastaFile.LastWriteTimeUtc)) + " ago at " +
                                fiFastaFile.LastWriteTime.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT);

                        fastaFileMsg += "; file created: " +
                            GetHumanReadableTimeInterval(DateTime.UtcNow.Subtract(fiFastaFile.CreationTimeUtc)) + " ago at " +
                            fiFastaFile.CreationTime.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT);

                        fastaFileMsg += "; file size: " + fiFastaFile.Length + " bytes";

                        LogDebugMessage(fastaFileMsg);
                    }
                    catch (Exception)
                    {
                        // Ignore errors here
                    }
                }

            }

            // Create/Update the .LastUsed file for the newly created Fasta File
            var lastUsedFilePath = fiFastaFile.FullName + ".LastUsed";
            try
            {
                using (var swLastUsedFile = new StreamWriter(new FileStream(lastUsedFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    swLastUsedFile.WriteLine(DateTime.UtcNow.ToString(clsAnalysisToolRunnerBase.DATE_TIME_FORMAT));
                }
            }
            catch (Exception ex)
            {
                LogError("Warning: unable to create a new .LastUsed file at " + lastUsedFilePath, ex);
            }

            // If we got to here, everything worked OK
            return true;

        }

        /// <summary>
        /// Creates an XML formatted settings file based on data from broker
        /// </summary>
        /// <param name="FileText">String containing XML file contents</param>
        /// <param name="FileNamePath">Name of file to create</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        [Obsolete("Unused")]
        private bool CreateSettingsFile(string FileText, string FileNamePath)
        {

            var objFormattedXMLWriter = new clsFormattedXMLWriter();

            if (!objFormattedXMLWriter.WriteXMLToFile(FileText, FileNamePath))
            {
                LogError("Error creating settings file " + FileNamePath + ": " + objFormattedXMLWriter.ErrMsg);
                m_message = "Error creating settings file";
                return false;
            }

            return true;
        }

        /// <summary>
        /// Given two dates, returns the most recent date
        /// </summary>
        /// <param name="date1"></param>
        /// <param name="date2"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected static DateTime DateMax(DateTime date1, DateTime date2)
        {
            if (date1 > date2)
            {
                return date1;
            }

            return date2;
        }

        protected void DisableMyEMSLSearch()
        {
            m_MyEMSLUtilities.ClearDownloadQueue();
            MyEMSLSearchDisabled = true;
        }

        /// <summary>
        /// Determines the most appropriate folder to use to obtain dataset files from
        /// Optionally, can require that a certain file also be present in the folder for it to be deemed valid
        /// If no folder is deemed valid, returns the path defined by Job Param "DatasetStoragePath"
        /// </summary>
        /// <param name="dsName">Name of the dataset</param>
        /// <param name="fileNameToFind">Optional: Name of a file that must exist in the dataset folder; can contain a wildcard, e.g. *.zip</param>
        /// <param name="folderNameToFind">Optional: Name of a subfolder that must exist in the dataset folder; can contain a wildcard, e.g. SEQ*</param>
        /// <param name="maxAttempts">Maximum number of attempts</param>
        /// <param name="logFolderNotFound">If true, then log a warning if the folder is not found</param>
        /// <param name="retrievingInstrumentDataFolder">Set to True when retrieving an instrument data folder</param>
        /// <param name="validFolderFound">Output parameter: True if a valid folder is ultimately found, otherwise false</param>
        /// <param name="assumeUnpurged">When true, this function returns the path to the dataset folder on the storage server</param>
        /// <param name="folderNotFoundMessage"></param>
        /// <returns>Path to the most appropriate dataset folder</returns>
        /// <remarks>The path returned will be "\\MyEMSL" if the best folder is in MyEMSL</remarks>
        public string FindValidFolder(string dsName, string fileNameToFind, string folderNameToFind, int maxAttempts, bool logFolderNotFound,
                                      bool retrievingInstrumentDataFolder, out bool validFolderFound, bool assumeUnpurged,
                                      out string folderNotFoundMessage)
        {
            var folderPath = m_FolderSearch.FindValidFolder(
                dsName, fileNameToFind, folderNameToFind, maxAttempts, logFolderNotFound,
                retrievingInstrumentDataFolder, assumeUnpurged, out validFolderFound, out folderNotFoundMessage);

            if (!validFolderFound && !assumeUnpurged)
            {
                m_message = folderNotFoundMessage;
            }

            return folderPath;
        }

        /// <summary>
        /// Creates the _ScanStats.txt file for this job's dataset
        /// </summary>
        /// <returns>True if success, false if a problem</returns>
        /// <remarks>Only valid for Thermo .Raw files and .UIMF files.  Will delete the .Raw (or .UIMF) after creating the ScanStats file</remarks>
        protected bool GenerateScanStatsFile()
        {

            const bool deleteRawDataFile = true;
            return GenerateScanStatsFile(deleteRawDataFile);

        }

        /// <summary>
        /// Creates the _ScanStats.txt file for this job's dataset
        /// </summary>
        /// <param name="deleteRawDataFile">True to delete the .raw (or .uimf) file after creating the ScanStats file </param>
        /// <returns>True if success, false if a problem</returns>
        /// <remarks>Only valid for Thermo .Raw files and .UIMF files</remarks>
        protected bool GenerateScanStatsFile(bool deleteRawDataFile)
        {

            var rawDataType = m_jobParams.GetParam("RawDataType");
            var datasetID = m_jobParams.GetJobParameter("DatasetID", 0);

            var strMSFileInfoScannerDir = m_mgrParams.GetParam("MSFileInfoScannerDir");
            if (string.IsNullOrEmpty(strMSFileInfoScannerDir))
            {
                LogError("Manager parameter 'MSFileInfoScannerDir' is not defined (GenerateScanStatsFile)");
                return false;
            }

            var strMSFileInfoScannerDLLPath = Path.Combine(strMSFileInfoScannerDir, "MSFileInfoScanner.dll");
            if (!File.Exists(strMSFileInfoScannerDLLPath))
            {
                LogError("File Not Found (GenerateScanStatsFile): " + strMSFileInfoScannerDLLPath);
                return false;
            }

            string inputFilePath;

            // Confirm that this dataset is a Thermo .Raw file or a .UIMF file
            switch (GetRawDataType(rawDataType))
            {
                case eRawDataTypeConstants.ThermoRawFile:
                    inputFilePath = m_DatasetName + DOT_RAW_EXTENSION;
                    break;
                case eRawDataTypeConstants.UIMF:
                    inputFilePath = m_DatasetName + DOT_UIMF_EXTENSION;
                    break;
                default:
                    LogError("Invalid dataset type for auto-generating ScanStats.txt file: " + rawDataType);
                    return false;
            }

            inputFilePath = Path.Combine(m_WorkingDir, inputFilePath);

            if (!File.Exists(inputFilePath))
            {
                if (!m_FileSearch.RetrieveSpectra(rawDataType))
                {
                    var extraMsg = m_message;
                    m_message = "Error retrieving spectra file";
                    if (!string.IsNullOrWhiteSpace(extraMsg))
                    {
                        m_message += "; " + extraMsg;
                    }
                    LogMessage(m_message, 0, true);
                    return false;
                }

                // ReSharper disable once RedundantNameQualifier
                if (!m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    return false;
                }
            }

            // Make sure the raw data file does not get copied to the results folder
            m_jobParams.AddResultFileToSkip(Path.GetFileName(inputFilePath));

            var objScanStatsGenerator = new clsScanStatsGenerator(strMSFileInfoScannerDLLPath, m_DebugLevel);
            RegisterEvents(objScanStatsGenerator);

            LogMessage("Generating the ScanStats files for " + Path.GetFileName(inputFilePath));

            // Create the _ScanStats.txt and _ScanStatsEx.txt files
            var success = objScanStatsGenerator.GenerateScanStatsFile(inputFilePath, m_WorkingDir, datasetID);

            if (success)
            {
                LogMessage("Generated ScanStats file using " + inputFilePath);

                Thread.Sleep(125);
                PRISM.clsProgRunner.GarbageCollectNow();

                if (deleteRawDataFile)
                {
                    try
                    {
                        File.Delete(inputFilePath);
                    }
                    catch (Exception)
                    {
                        // Ignore errors here
                    }
                }

            }
            else
            {
                LogError("Error generating ScanStats files with clsScanStatsGenerator", objScanStatsGenerator.ErrorMessage);
                if (objScanStatsGenerator.MSFileInfoScannerErrorCount > 0)
                {
                    LogMessage("MSFileInfoScanner encountered " + objScanStatsGenerator.MSFileInfoScannerErrorCount + " errors");
                }
            }

            return success;

        }

        protected clsDataPackageJobInfo GetCurrentDatasetAndJobInfo()
        {
            const string jobParamsSection = clsAnalysisJob.JOB_PARAMETERS_SECTION;

            var jobNumber = m_jobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Job", 0);
            var dataset = m_jobParams.GetJobParameter(jobParamsSection, "DatasetNum", m_DatasetName);

            var jobInfo = new clsDataPackageJobInfo(jobNumber, dataset)
            {
                DatasetID = m_jobParams.GetJobParameter(jobParamsSection, "DatasetID", 0),
                Instrument = m_jobParams.GetJobParameter(jobParamsSection, "Instrument", string.Empty),
                InstrumentGroup = m_jobParams.GetJobParameter(jobParamsSection, "InstrumentGroup", string.Empty),
                Experiment = m_jobParams.GetJobParameter(jobParamsSection, "Experiment", string.Empty),
                Experiment_Reason = string.Empty,
                Experiment_Comment = string.Empty,
                Experiment_Organism = string.Empty,
                Experiment_NEWT_ID = 0,
                Experiment_NEWT_Name = string.Empty,
                Tool = m_jobParams.GetJobParameter(jobParamsSection, "ToolName", string.Empty),
                NumberOfClonedSteps = m_jobParams.GetJobParameter("NumberOfClonedSteps", 0),
                ResultType = m_jobParams.GetJobParameter(jobParamsSection, "ResultType", string.Empty),
                SettingsFileName = m_jobParams.GetJobParameter(jobParamsSection, "SettingsFileName", string.Empty),
                ParameterFileName = m_jobParams.GetJobParameter("PeptideSearch", "ParmFileName", string.Empty),
                LegacyFastaFileName = m_jobParams.GetJobParameter("PeptideSearch", "legacyFastaFileName", string.Empty)
            };

            jobInfo.OrganismDBName = string.Copy(jobInfo.LegacyFastaFileName);

            jobInfo.ProteinCollectionList = m_jobParams.GetJobParameter("PeptideSearch", "ProteinCollectionList", string.Empty);
            jobInfo.ProteinOptions = m_jobParams.GetJobParameter("PeptideSearch", "ProteinOptions", string.Empty);

            jobInfo.ServerStoragePath = m_jobParams.GetJobParameter(jobParamsSection, "DatasetStoragePath", string.Empty);
            jobInfo.ArchiveStoragePath = m_jobParams.GetJobParameter(jobParamsSection, "DatasetArchivePath", string.Empty);
            jobInfo.ResultsFolderName = m_jobParams.GetJobParameter(jobParamsSection, "inputFolderName", string.Empty);
            jobInfo.DatasetFolderName = m_jobParams.GetJobParameter(jobParamsSection, "DatasetFolderName", string.Empty);
            jobInfo.SharedResultsFolder = m_jobParams.GetJobParameter(jobParamsSection, "SharedResultsFolders", string.Empty);
            jobInfo.RawDataType = m_jobParams.GetJobParameter(jobParamsSection, "RawDataType", string.Empty);

            return jobInfo;

        }

        /// <summary>
        /// Lookups up the storage path for a given data package
        /// </summary>
        /// <param name="connectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
        /// <param name="dataPackageID">Data Package ID</param>
        /// <returns>Storage path if successful, empty path if an error or unknown data package</returns>
        /// <remarks></remarks>
        protected static string GetDataPackageStoragePath(string connectionString, int dataPackageID)
        {

            // Requests Dataset information from a data package
            const short RETRY_COUNT = 3;

            var sqlStr = new System.Text.StringBuilder();

            sqlStr.Append("Select [Share Path] AS StoragePath ");
            sqlStr.Append("From V_DMS_Data_Packages ");
            sqlStr.Append("Where ID = " + dataPackageID);


            // Get a table to hold the results of the query
            var success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), connectionString, "GetDataPackageStoragePath", RETRY_COUNT, out var resultSet);

            if (!success)
            {
                var errorMessage = "GetDataPackageStoragePath; Excessive failures attempting to retrieve data package info from database";
                clsGlobal.LogError(errorMessage);
                resultSet.Dispose();
                return string.Empty;
            }

            // Verify at least one row returned
            if (resultSet.Rows.Count < 1)
            {
                // No data was returned
                // Log an error

                var errorMessage = "GetDataPackageStoragePath; Data package not found: " + dataPackageID;
                clsGlobal.LogError(errorMessage);
                return string.Empty;
            }

            var curRow = resultSet.Rows[0];

            var storagePath = clsGlobal.DbCStr(curRow[0]);

            resultSet.Dispose();
            return storagePath;
        }

        /// <summary>
        /// Examines the folder tree in folderPath to find the a folder with a name like 2013_2
        /// </summary>
        /// <param name="folderPath"></param>
        /// <returns>Matching folder name if found, otherwise an empty string</returns>
        /// <remarks></remarks>
        public static string GetDatasetYearQuarter(string folderPath)
        {

            if (string.IsNullOrEmpty(folderPath))
            {
                return string.Empty;
            }

            // RegEx to find the year_quarter folder name
            // Valid matches include: 2014_1, 2014_01, 2014_4
            var reYearQuarter = new Regex("^[0-9]{4}_0*[1-4]$", RegexOptions.Compiled);

            // Split folderPath on the path separator
            var lstFolders = folderPath.Split(Path.DirectorySeparatorChar).ToList();
            lstFolders.Reverse();

            foreach (var folder in lstFolders)
            {
                var reMatch = reYearQuarter.Match(folder);
                if (reMatch.Success)
                {
                    return reMatch.Value;
                }
            }

            return string.Empty;

        }

        /// <summary>
        /// Examine the fasta file to determine the fraction of the proteins that are decoy (reverse) proteins
        /// </summary>
        /// <param name="fiFastaFile">FASTA file to examine</param>
        /// <param name="proteinCount">Output parameter: total protein count</param>
        /// <returns>Fraction of the proteins that are decoy (for example 0.5 if half of the proteins start with Reversed_)</returns>
        /// <remarks>Decoy proteins start with Reversed_</remarks>
        public static double GetDecoyFastaCompositionStats(FileInfo fiFastaFile, out int proteinCount)
        {

            var decoyProteinPrefix = GetDefaultDecoyPrefixes().First();
            return GetDecoyFastaCompositionStats(fiFastaFile, decoyProteinPrefix, out proteinCount);

        }

        /// <summary>
        /// Examine the fasta file to determine the fraction of the proteins that are decoy (reverse) proteins
        /// </summary>
        /// <param name="fiFastaFile">FASTA file to examine</param>
        /// <param name="decoyProteinPrefix"></param>
        /// <param name="proteinCount">Output parameter: total protein count</param>
        /// <returns>Fraction of the proteins that are decoy (for example 0.5 if half of the proteins start with Reversed_)</returns>
        /// <remarks>Decoy proteins start with decoyProteinPrefix</remarks>
        public static double GetDecoyFastaCompositionStats(FileInfo fiFastaFile, string decoyProteinPrefix, out int proteinCount)
        {

            // Look for protein names that look like:
            // >decoyProteinPrefix
            // where
            // decoyProteinPrefix is typically XXX. or XXX_ or Reversed_

            var prefixToFind = ">" + decoyProteinPrefix;
            var forwardProteinCount = 0;
            var reverseProteinCount = 0;

            using (var srFastaFile = new StreamReader(new FileStream(fiFastaFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
            {
                while (!srFastaFile.EndOfStream)
                {
                    var dataLine = srFastaFile.ReadLine();
                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    if (!dataLine.StartsWith(">"))
                        continue;

                    // Protein header line found
                    if (dataLine.StartsWith(prefixToFind))
                    {
                        reverseProteinCount += 1;
                    }
                    else
                    {
                        forwardProteinCount += 1;
                    }
                }
            }

            double fractionDecoy = 0;

            proteinCount = forwardProteinCount + reverseProteinCount;
            if (proteinCount > 0)
            {
                fractionDecoy = reverseProteinCount / (double)proteinCount;
            }

            return fractionDecoy;

        }

        /// <summary>
        /// Get a list of the default decoy protein prefixes
        /// </summary>
        /// <returns></returns>
        public static List<string> GetDefaultDecoyPrefixes()
        {

            // Decoy proteins created by MSGF+ start with XXX_
            // Decoy proteins created by DMS start with Reversed_
            var decoyPrefixes = new List<string> {
                "Reversed_",
                "XXX_",
                "XXX:"
            };

            return decoyPrefixes;

        }

        /// <summary>
        /// Return a SortedSet with the default files from the WorkDir to ignore
        /// Used by CopyResourcesToRemote when calling CopyWorkDirFilesToRemote
        /// </summary>
        /// <returns></returns>
        protected SortedSet<string> GetDefaultWorkDirFilesToIgnore()
        {
            // Construct the JobParameters filename, for example JobParameters_1394245.xml
            // We skip this file because the remote manager loads parameters from JobParams.xml
            // (JobParams.xml has more info than the JobParameters_1394245.xml file)
            var jobParametersFilename = clsAnalysisJob.JobParametersFilename(m_JobNum);

            var filesToIgnore = new SortedSet<string> { jobParametersFilename };

            return filesToIgnore;
        }

        /// <summary>
        /// Look for a JobParameters file from the previous job step
        /// If found, copy to the working directory, naming in JobParameters_JobNum_PreviousStep.xml
        /// </summary>
        /// <returns>True if success, false if an error</returns>
        private bool GetExistingJobParametersFile()
        {

            try
            {
                var stepNum = m_jobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step", 1);
                if (stepNum == 1)
                {
                    // This is the first step; nothing to retrieve
                    return true;
                }

                var transferFolderPath = GetTransferFolderPathForJobStep(useInputFolder: true);
                if (string.IsNullOrEmpty(transferFolderPath))
                {
                    // Transfer folder parameter is empty; nothing to retrieve
                    return true;
                }

                // Construct the filename, for example JobParameters_1394245.xml
                var jobParametersFilename = clsAnalysisJob.JobParametersFilename(m_JobNum);
                var sourceFile = new FileInfo(Path.Combine(transferFolderPath, jobParametersFilename));

                if (!sourceFile.Exists)
                {
                    // File not found; nothing to copy
                    return true;
                }

                // Copy the file, renaming to avoid a naming collision
                var destFilePath = Path.Combine(m_WorkingDir, Path.GetFileNameWithoutExtension(sourceFile.Name) + "_PreviousStep.xml");
                if (m_FileCopyUtilities.CopyFileWithRetry(sourceFile.FullName, destFilePath, overwrite: true, maxCopyAttempts: 3))
                {
                    if (m_DebugLevel > 3)
                    {
                        LogDebugMessage("GetExistingJobParametersFile, File copied:  " + sourceFile.FullName);
                    }
                }
                else
                {
                    LogError("Error in GetExistingJobParametersFile copying file " + sourceFile.FullName);
                    return false;
                }

                var sourceJobParamXMLFile = new FileInfo(destFilePath);

                var masterJobParamXMLFile = new FileInfo(Path.Combine(m_WorkingDir, clsAnalysisJob.JobParametersFilename(m_JobNum)));

                if (!sourceJobParamXMLFile.Exists)
                {
                    // The file wasn't copied
                    LogError("Job Parameters file from the previous job step was not found at " + sourceJobParamXMLFile.FullName);
                    return false;
                }

                if (!masterJobParamXMLFile.Exists)
                {
                    // The JobParameters XML file was not found
                    LogError("Job Parameters file not found at " + masterJobParamXMLFile.FullName);
                    return false;
                }

                // Update the JobParameters XML file that was created by this job step to include the information from the previous job steps
                var success = MergeJobParamXMLStepParameters(sourceJobParamXMLFile.FullName, masterJobParamXMLFile.FullName);

                if (success)
                {
                    m_jobParams.AddResultFileToSkip(sourceJobParamXMLFile.Name);
                }

                return success;

            }
            catch (Exception ex)
            {
                LogError("Exception in GetExistingJobParametersFile", ex);
                return false;
            }

        }

        /// <summary>
        /// Examine the specified DMS_Temp_Org folder to find the FASTA files and their corresponding .fasta.LastUsed or .hashcheck files
        /// </summary>
        /// <param name="diOrgDbFolder"></param>
        /// <returns>Dictionary of FASTA files, including the last usage date for each</returns>
        private Dictionary<FileInfo, DateTime> GetFastaFilesByLastUse(DirectoryInfo diOrgDbFolder)
        {

            // Keys are the fasta file; values are the dtLastUsed time of the file (nominally obtained from a .hashcheck or .lastused file)
            var dctFastaFiles = new Dictionary<FileInfo, DateTime>();

            foreach (var fiFile in diOrgDbFolder.GetFiles("*.fasta"))
            {
                if (!dctFastaFiles.ContainsKey(fiFile))
                {
                    var dtLastUsed = DateMax(fiFile.LastWriteTimeUtc, fiFile.CreationTimeUtc);

                    // Look for a .hashcheck file
                    var lstHashCheckfiles = diOrgDbFolder.GetFiles(fiFile.Name + "*.hashcheck").ToList();
                    if (lstHashCheckfiles.Count > 0)
                    {
                        dtLastUsed = DateMax(dtLastUsed, lstHashCheckfiles.First().LastWriteTimeUtc);
                    }

                    // Look for a .LastUsed file
                    var lstLastUsedFiles = diOrgDbFolder.GetFiles(fiFile.Name + ".LastUsed").ToList();
                    if (lstLastUsedFiles.Count > 0)
                    {
                        dtLastUsed = DateMax(dtLastUsed, lstLastUsedFiles.First().LastWriteTimeUtc);

                        try
                        {
                            // Read the date stored in the file
                            using (var srLastUsedfile = new StreamReader(new FileStream(lstLastUsedFiles.First().FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                            {
                                if (!srLastUsedfile.EndOfStream)
                                {
                                    var lastUseDate = srLastUsedfile.ReadLine();
                                    if (DateTime.TryParse(lastUseDate, out var dtLastUsedActual))
                                    {
                                        dtLastUsed = DateMax(dtLastUsed, dtLastUsedActual);
                                    }
                                }
                            }
                        }
                        catch (Exception)
                        {
                            // Ignore errors here
                        }

                    }
                    dctFastaFiles.Add(fiFile, dtLastUsed);
                }
            }

            return dctFastaFiles;

        }

        /// <summary>
        /// Converts the given timespan to the total days, hours, minutes, or seconds as a string
        /// </summary>
        /// <param name="dtInterval">Timespan to convert</param>
        /// <returns>Timespan length in human readable form</returns>
        /// <remarks></remarks>
        protected string GetHumanReadableTimeInterval(TimeSpan dtInterval)
        {
            if (dtInterval.TotalDays >= 1)
            {
                // Report Days
                return dtInterval.TotalDays.ToString("0.00") + " days";
            }

            if (dtInterval.TotalHours >= 1)
            {
                // Report hours
                return dtInterval.TotalHours.ToString("0.00") + " hours";
            }

            if (dtInterval.TotalMinutes >= 1)
            {
                // Report minutes
                return dtInterval.TotalMinutes.ToString("0.00") + " minutes";
            }

            // Report seconds
            return dtInterval.TotalSeconds.ToString("0.0") + " seconds";
        }

        /// <summary>
        /// Get the MSXML Cache Folder path that is appropriate for this job
        /// </summary>
        /// <param name="cacheFolderPathBase"></param>
        /// <param name="jobParams"></param>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        /// <remarks>Uses job parameter OutputFolderName, which should be something like MSXML_Gen_1_120_275966</remarks>
        public static string GetMSXmlCacheFolderPath(string cacheFolderPathBase, IJobParams jobParams, out string errorMessage)
        {

            // Lookup the output folder; e.g. MSXML_Gen_1_120_275966
            var outputFolderName = jobParams.GetJobParameter("OutputFolderName", string.Empty);
            if (string.IsNullOrEmpty(outputFolderName))
            {
                errorMessage = "OutputFolderName is empty; cannot construct MSXmlCache path";
                return string.Empty;
            }

            string msXmlToolNameVersionFolder;
            try
            {
                msXmlToolNameVersionFolder = GetMSXmlToolNameVersionFolder(outputFolderName);
            }
            catch (Exception)
            {
                errorMessage = "OutputFolderName is not in the expected form of ToolName_Version_DatasetID (" + outputFolderName + "); cannot construct MSXmlCache path";
                return string.Empty;
            }

            return GetMSXmlCacheFolderPath(cacheFolderPathBase, jobParams, msXmlToolNameVersionFolder, out errorMessage);

        }

        /// <summary>
        /// Get the path to the cache folder; used for retrieving cached .mzML files that are stored in ToolName_Version folders
        /// </summary>
        /// <param name="cacheFolderPathBase">Cache folder base, e.g. \\Proto-11\MSXML_Cache</param>
        /// <param name="jobParams">Job parameters</param>
        /// <param name="msXmlToolNameVersionFolder">ToolName_Version folder, e.g. MSXML_Gen_1_93</param>
        /// <param name="errorMessage">Output parameter: error message</param>
        /// <returns>Path to the cache folder; empty string if an error</returns>
        /// <remarks>Uses job parameter DatasetStoragePath to determine the Year_Quarter string to append to the end of the path</remarks>
        public static string GetMSXmlCacheFolderPath(string cacheFolderPathBase, IJobParams jobParams, string msXmlToolNameVersionFolder, out string errorMessage)
        {

            errorMessage = string.Empty;

            var datasetStoragePath = jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetStoragePath");
            if (string.IsNullOrEmpty(datasetStoragePath))
            {
                datasetStoragePath = jobParams.GetParam(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetArchivePath");
            }

            if (string.IsNullOrEmpty(datasetStoragePath))
            {
                errorMessage = "JobParameters does not contain DatasetStoragePath or DatasetArchivePath; cannot construct MSXmlCache path";
                return string.Empty;
            }

            var yearQuarter = GetDatasetYearQuarter(datasetStoragePath);
            if (string.IsNullOrEmpty(yearQuarter))
            {
                errorMessage = "Unable to extract the dataset Year_Quarter code from " + datasetStoragePath + "; cannot construct MSXmlCache path";
                return string.Empty;
            }

            // Combine the cache folder path, ToolNameVersion, and the dataset Year_Quarter code
            var targetFolderPath = Path.Combine(cacheFolderPathBase, msXmlToolNameVersionFolder, yearQuarter);

            return targetFolderPath;

        }

        /// <summary>
        /// Examine a folder of the form MSXML_Gen_1_93_367204 and remove the DatasetID portion
        /// </summary>
        /// <param name="toolNameVersionDatasetIDFolder">Shared results folder name</param>
        /// <returns>The trimmed folder name if a valid folder; throws an exception if the folder name is not the correct format</returns>
        /// <remarks></remarks>
        public static string GetMSXmlToolNameVersionFolder(string toolNameVersionDatasetIDFolder)
        {

            // Remove the dataset ID from the end of the folder name
            var reToolNameAndVersion = new Regex(@"^(?<ToolNameVersion>.+\d+_\d+)_\d+$");
            var reMatch = reToolNameAndVersion.Match(toolNameVersionDatasetIDFolder);
            if (!reMatch.Success)
            {
                throw new Exception("Folder name is not in the expected form of ToolName_Version_DatasetID; unable to strip out the dataset ID");
            }

            return reMatch.Groups["ToolNameVersion"].ToString();

        }

        /// <summary>
        /// Retrieve the .mzML or .mzXML file associated with this job (based on Job Parameter MSXMLOutputType)
        /// </summary>
        /// <returns>CLOSEOUT_SUCCESS or CLOSEOUT_FAILED</returns>
        /// <remarks>
        /// If MSXMLOutputType is not defined, attempts to retrieve a .mzML file
        /// If the .mzML file is not found, will attempt to create it
        /// </remarks>
        protected CloseOutType GetMsXmlFile()
        {
            var msXmlOutputType = m_jobParams.GetJobParameter("MSXMLOutputType", string.Empty);

            CloseOutType eResult;

            if (msXmlOutputType.ToLower() == "mzxml")
            {
                eResult = GetMzXMLFile();
            }
            else
            {
                eResult = GetMzMLFile();
            }

            return eResult;

        }

        protected CloseOutType GetMzMLFile()
        {

            LogMessage("Getting mzML file");

            const bool unzipFile = true;

            var success = m_FileSearch.RetrieveCachedMzMLFile(unzipFile, out var errorMessage, out var fileMissingFromCache);
            if (!success)
            {
                return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_MZML_EXTENSION);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        protected CloseOutType GetMzXMLFile()
        {

            // Retrieve the .mzXML file for this dataset
            // Do not use RetrieveMZXmlFile since that function looks for any valid MSXML_Gen folder for this dataset
            // Instead, use FindAndRetrieveMiscFiles

            LogMessage("Getting mzXML file");

            // Note that capitalization matters for the extension; it must be .mzXML
            var fileToGet = m_DatasetName + DOT_MZXML_EXTENSION;

            if (!m_FileSearch.FindAndRetrieveMiscFiles(fileToGet, false))
            {
                // Look for a .mzXML file in the cache instead

                const bool unzipFile = true;

                var success = m_FileSearch.RetrieveCachedMzXMLFile(unzipFile, out var errorMessage, out var fileMissingFromCache);
                if (!success)
                {
                    return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_MZXML_EXTENSION);
                }

            }
            m_jobParams.AddResultFileToSkip(fileToGet);

            // ReSharper disable once RedundantNameQualifier
            if (!m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
            {
                return CloseOutType.CLOSEOUT_FAILED;
            }

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        protected CloseOutType GetPBFFile()
        {

            LogMessage("Getting PBF file");


            var success = m_FileSearch.RetrieveCachedPBFFile(out var errorMessage, out var fileMissingFromCache);
            if (!success)
            {
                return HandleMsXmlRetrieveFailure(fileMissingFromCache, errorMessage, DOT_PBF_EXTENSION);
            }

            return CloseOutType.CLOSEOUT_SUCCESS;

        }

        protected CloseOutType HandleMsXmlRetrieveFailure(bool fileMissingFromCache, string errorMessage, string msXmlExtension)
        {

            if (fileMissingFromCache)
            {
                if (string.IsNullOrEmpty(errorMessage))
                {
                    errorMessage = "Cached " + msXmlExtension + " file does not exist; will re-generate it";
                }

                LogMessage("Warning: " + errorMessage);
                return CloseOutType.CLOSEOUT_FILE_NOT_IN_CACHE;
            }

            if (string.IsNullOrEmpty(errorMessage))
            {
                errorMessage = "Unknown error in RetrieveCached" + msXmlExtension.TrimStart('.') + "File";
            }

            LogMessage(errorMessage, 0, true);
            return CloseOutType.CLOSEOUT_FILE_NOT_FOUND;

        }

        /// <summary>
        /// Gets fake job information for a dataset that is associated with a data package yet has no analysis jobs associated with the data package
        /// </summary>
        /// <param name="udtDatasetInfo"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static clsDataPackageJobInfo GetPseudoDataPackageJobInfo(clsDataPackageDatasetInfo udtDatasetInfo)
        {

            // Store the negative of the dataset ID as the job number
            var jobInfo = new clsDataPackageJobInfo(-udtDatasetInfo.DatasetID, udtDatasetInfo.Dataset)
            {
                DatasetID = udtDatasetInfo.DatasetID,
                Instrument = udtDatasetInfo.Instrument,
                InstrumentGroup = udtDatasetInfo.InstrumentGroup,
                Experiment = udtDatasetInfo.Experiment,
                Experiment_Reason = udtDatasetInfo.Experiment_Reason,
                Experiment_Comment = udtDatasetInfo.Experiment_Comment,
                Experiment_Organism = udtDatasetInfo.Experiment_Organism,
                Experiment_NEWT_ID = udtDatasetInfo.Experiment_NEWT_ID,
                Experiment_NEWT_Name = udtDatasetInfo.Experiment_NEWT_Name,
                Tool = "Dataset info (no tool)",
                NumberOfClonedSteps = 0,
                ResultType = "Dataset info (no type)",
                PeptideHitResultType = clsPHRPReader.ePeptideHitResultType.Unknown,
                SettingsFileName = string.Empty,
                ParameterFileName = string.Empty,
                OrganismDBName = string.Empty,
                LegacyFastaFileName = string.Empty,
                ProteinCollectionList = string.Empty,
                ProteinOptions = string.Empty,
                ResultsFolderName = string.Empty,
                DatasetFolderName = udtDatasetInfo.Dataset,
                SharedResultsFolder = string.Empty,
                RawDataType = udtDatasetInfo.RawDataType
            };

            try
            {
                // Archive storage path and server storage path track the folder just above the dataset folder
                var archiveFolder = new DirectoryInfo(udtDatasetInfo.ArchiveStoragePath);
                if (archiveFolder.Parent != null)
                    jobInfo.ArchiveStoragePath = archiveFolder.Parent.FullName;
                else
                    throw new DirectoryNotFoundException("Parent of " + archiveFolder.FullName);
            }
            catch (Exception)
            {
                clsGlobal.LogWarning("Exception in GetPseudoDataPackageJobInfo determining the parent folder of " + udtDatasetInfo.ArchiveStoragePath);
                jobInfo.ArchiveStoragePath = udtDatasetInfo.ArchiveStoragePath.Replace(@"\" + udtDatasetInfo.Dataset, "");
            }

            try
            {
                var storageFolder = new DirectoryInfo(udtDatasetInfo.ServerStoragePath);
                if (storageFolder.Parent != null)
                    jobInfo.ServerStoragePath = storageFolder.Parent.FullName;
                else
                    throw new DirectoryNotFoundException("Parent of " + storageFolder.FullName);
            }
            catch (Exception)
            {
                clsGlobal.LogWarning("Exception in GetPseudoDataPackageJobInfo determining the parent folder of " + udtDatasetInfo.ServerStoragePath);
                jobInfo.ServerStoragePath = udtDatasetInfo.ServerStoragePath.Replace(@"\" + udtDatasetInfo.Dataset, "");
            }

            return jobInfo;

        }

        /// <summary>
        /// Convert a raw data type string to raw data type enum (i.e. instrument data type)
        /// </summary>
        /// <param name="rawDataType"></param>
        /// <returns></returns>
        public static eRawDataTypeConstants GetRawDataType(string rawDataType)
        {

            if (string.IsNullOrEmpty(rawDataType))
            {
                return eRawDataTypeConstants.Unknown;
            }

            switch (rawDataType.ToLower())
            {
                case RAW_DATA_TYPE_DOT_D_FOLDERS:
                    return eRawDataTypeConstants.AgilentDFolder;
                case RAW_DATA_TYPE_ZIPPED_S_FOLDERS:
                    return eRawDataTypeConstants.ZippedSFolders;
                case RAW_DATA_TYPE_DOT_RAW_FOLDER:
                    return eRawDataTypeConstants.MicromassRawFolder;
                case RAW_DATA_TYPE_DOT_RAW_FILES:
                    return eRawDataTypeConstants.ThermoRawFile;
                case RAW_DATA_TYPE_DOT_WIFF_FILES:
                    return eRawDataTypeConstants.AgilentQStarWiffFile;
                case RAW_DATA_TYPE_DOT_UIMF_FILES:
                    return eRawDataTypeConstants.UIMF;
                case RAW_DATA_TYPE_DOT_MZXML_FILES:
                    return eRawDataTypeConstants.mzXML;
                case RAW_DATA_TYPE_DOT_MZML_FILES:
                    return eRawDataTypeConstants.mzML;
                case RAW_DATA_TYPE_BRUKER_FT_FOLDER:
                    return eRawDataTypeConstants.BrukerFTFolder;
                case RAW_DATA_TYPE_BRUKER_MALDI_SPOT:
                    return eRawDataTypeConstants.BrukerMALDISpot;
                case RAW_DATA_TYPE_BRUKER_MALDI_IMAGING:
                    return eRawDataTypeConstants.BrukerMALDIImaging;
                case RAW_DATA_TYPE_BRUKER_TOF_BAF_FOLDER:
                    return eRawDataTypeConstants.BrukerTOFBaf;
                default:
                    return eRawDataTypeConstants.Unknown;
            }

        }

        /// <summary>
        /// Determine the raw data type (i.e. instrument data type)
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        public static string GetRawDataTypeName(IJobParams jobParams, out string errorMessage)
        {

            errorMessage = string.Empty;

            var msXmlOutputType = jobParams.GetParam("MSXMLOutputType");

            if (string.IsNullOrWhiteSpace(msXmlOutputType))
            {
                return jobParams.GetParam("RawDataType");
            }

            switch (msXmlOutputType.ToLower())
            {
                case "mzxml":
                    return RAW_DATA_TYPE_DOT_MZXML_FILES;
                case "mzml":
                    return RAW_DATA_TYPE_DOT_MZML_FILES;
                default:
                    return string.Empty;
            }
        }

        protected string GetRawDataTypeName()
        {

            var rawDataTypeName = GetRawDataTypeName(m_jobParams, out var errorMessage);

            if (string.IsNullOrWhiteSpace(rawDataTypeName))
            {
                if (string.IsNullOrWhiteSpace(errorMessage))
                {
                    LogError("Unable to determine the instrument data type using GetRawDataTypeName");
                }
                else
                {
                    LogError(errorMessage);
                }

                return string.Empty;
            }

            return rawDataTypeName;

        }

        /// <summary>
        /// Get the name of the split fasta file to use for this job
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="errorMessage">Output parameter: error message</param>
        /// <returns>The name of the split fasta file to use</returns>
        /// <remarks>Returns an empty string if an error</remarks>
        public static string GetSplitFastaFileName(IJobParams jobParams, out string errorMessage)
        {

            return GetSplitFastaFileName(jobParams, out errorMessage, out var _);

        }

        /// <summary>
        /// Get the name of the split fasta file to use for this job
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="errorMessage">Output parameter: error message</param>
        /// <param name="numberOfClonedSteps">Output parameter: total number of cloned steps</param>
        /// <returns>The name of the split fasta file to use</returns>
        /// <remarks>Returns an empty string if an error</remarks>
        public static string GetSplitFastaFileName(IJobParams jobParams, out string errorMessage, out int numberOfClonedSteps)
        {
            numberOfClonedSteps = 0;

            var legacyFastaFileName = jobParams.GetJobParameter("LegacyFastaFileName", string.Empty);
            if (string.IsNullOrEmpty(legacyFastaFileName))
            {
                errorMessage = "Parameter LegacyFastaFileName is empty for the job; cannot determine the SplitFasta file name for this job step";
                clsGlobal.LogError(errorMessage);
                return string.Empty;
            }

            numberOfClonedSteps = jobParams.GetJobParameter("NumberOfClonedSteps", 0);
            if (numberOfClonedSteps == 0)
            {
                errorMessage = "Settings file is missing parameter NumberOfClonedSteps; cannot determine the SplitFasta file name for this job step";
                clsGlobal.LogError(errorMessage);
                return string.Empty;
            }

            var iteration = GetSplitFastaIteration(jobParams, out errorMessage);
            if (iteration < 1)
            {
                var toolName = jobParams.GetJobParameter("StepTool", string.Empty);
                if (clsGlobal.IsMatch(toolName, "Mz_Refinery"))
                {
                    // Running MzRefinery
                    // Override iteration to be 1
                    iteration = 1;
                }
                else
                {
                    if (string.IsNullOrEmpty(errorMessage))
                    {
                        errorMessage = "GetSplitFastaIteration computed an iteration value of " + iteration + "; " +
                            "cannot determine the SplitFasta file name for this job step";
                        clsGlobal.LogError(errorMessage);
                    }
                    return string.Empty;
                }
            }

            var fastaNameBase = Path.GetFileNameWithoutExtension(legacyFastaFileName);
            var splitFastaName = fastaNameBase + "_" + numberOfClonedSteps + "x_";

            if (numberOfClonedSteps < 10)
            {
                splitFastaName += iteration.ToString("0") + ".fasta";
            }
            else if (numberOfClonedSteps < 100)
            {
                splitFastaName += iteration.ToString("00") + ".fasta";
            }
            else
            {
                splitFastaName += iteration.ToString("000") + ".fasta";
            }

            return splitFastaName;

        }

        /// <summary>
        /// Compute the split fasta iteration for the current job step
        /// </summary>
        /// <param name="jobParams"></param>
        /// <param name="errorMessage"></param>
        /// <returns></returns>
        public static int GetSplitFastaIteration(IJobParams jobParams, out string errorMessage)
        {

            errorMessage = string.Empty;

            var cloneStepRenumStart = jobParams.GetJobParameter("CloneStepRenumberStart", 0);
            if (cloneStepRenumStart == 0)
            {
                errorMessage = "Settings file is missing parameter CloneStepRenumberStart; cannot determine the SplitFasta iteration value for this job step";
                clsGlobal.LogError(errorMessage);
                return 0;
            }

            var stepNumber = jobParams.GetJobParameter(clsAnalysisJob.STEP_PARAMETERS_SECTION, "Step", 0);
            if (stepNumber == 0)
            {
                errorMessage = "Job parameter Step is missing; cannot determine the SplitFasta iteration value for this job step";
                clsGlobal.LogError(errorMessage);
                return 0;
            }

            return stepNumber - cloneStepRenumStart + 1;

        }

        /// <summary>
        /// Read a JobParameters XML file to find the sections named "StepParameters"
        /// </summary>
        /// <param name="doc">XDocument to scan</param>
        /// <returns>Dictionary where keys are the step number and values are the XElement node with the step parameters for the given step</returns>
        private Dictionary<int, XElement> GetStepParametersSections(XContainer doc)
        {

            var stepNumToParamsMap = new Dictionary<int, XElement>();

            var stepParamSections = doc.Elements("sections").Elements("section").ToList();
            foreach (var section in stepParamSections)
            {
                if (!section.HasAttributes)
                    continue;

                var nameAttrib = section.Attribute("name");
                if (nameAttrib == null)
                    continue;

                if (!string.Equals(nameAttrib.Value, clsAnalysisJob.STEP_PARAMETERS_SECTION))
                    continue;

                var stepAttrib = section.Attribute("step");
                if (stepAttrib == null)
                    continue;

                if (int.TryParse(stepAttrib.Value, out var stepNumber))
                {
                    // Add or update the XML for this section
                    stepNumToParamsMap[stepNumber] = section;
                }

            }

            return stepNumToParamsMap;
        }

        /// <summary>
        /// Get the input or output transfer folder path specific to this job step
        /// </summary>
        /// <param name="useInputFolder">True to use "InputFolderName", False to use "OutputFolderName"</param>
        /// <returns></returns>
        protected string GetTransferFolderPathForJobStep(bool useInputFolder)
        {

            var transferFolderPathBase = m_jobParams.GetParam("transferFolderPath");
            if (string.IsNullOrEmpty(transferFolderPathBase))
            {
                // Transfer folder parameter is empty; return an empty string
                return string.Empty;
            }

            // Append the dataset folder name to the transfer folder path
            var datasetFolderName = m_jobParams.GetParam(clsAnalysisJob.STEP_PARAMETERS_SECTION, "DatasetFolderName");
            if (string.IsNullOrWhiteSpace(datasetFolderName))
                datasetFolderName = m_DatasetName;

            string folderName;

            if (useInputFolder)
            {
                folderName = m_jobParams.GetParam("InputFolderName");
            }
            else
            {
                folderName = m_jobParams.GetParam("OutputFolderName");
            }

            if (string.IsNullOrEmpty(folderName))
            {
                // Input (or output) folder parameter is empty; return an empty string
                return string.Empty;
            }

            var transferFolderPath = Path.Combine(transferFolderPathBase, datasetFolderName, folderName);

            return transferFolderPath;

        }

        /// <summary>
        /// Find files in the working directory
        /// </summary>
        /// <param name="recurse">True to recurse</param>
        /// <returns>Iterator of FileInfo items</returns>
        protected IEnumerable<FileInfo> GetWorkDirFiles(bool recurse = true)
        {
            var filesToIgnore = new SortedSet<string>();

            return GetWorkDirFiles(filesToIgnore, recurse);
        }

        /// <summary>
        /// Find files in the working directory, excluding any in filesToIgnore
        /// </summary>
        /// <param name="filesToIgnore">Names of files to not include in the list of returned files</param>
        /// <param name="recurse">True to recurse</param>
        /// <returns>Iterator of FileInfo items</returns>
        protected IEnumerable<FileInfo> GetWorkDirFiles(SortedSet<string> filesToIgnore, bool recurse = true)
        {
            if (string.IsNullOrWhiteSpace(m_WorkingDir))
                yield break;

            var workDir = new DirectoryInfo(m_WorkingDir);

            var searchOption = recurse ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;

            foreach (var file in workDir.GetFiles("*", searchOption))
            {
                if (filesToIgnore.Contains(file.Name))
                    continue;

                yield return file;
            }

        }

        /// <summary>
        /// Unzip gzipFilePath into the working directory
        /// Existing files will be overwritten
        /// </summary>
        /// <param name="gzipFilePath">.gz file to unzip</param>
        /// <returns>True if success; false if an error</returns>
        protected bool GUnzipFile(string gzipFilePath)
        {
            return m_FileSearch.GUnzipFile(gzipFilePath);
        }

        /// <summary>
        /// Looks up dataset information for a data package
        /// </summary>
        /// <param name="dctDataPackageDatasets"></param>
        /// <returns>True if a data package is defined and it has datasets associated with it</returns>
        /// <remarks></remarks>
        protected bool LoadDataPackageDatasetInfo(out Dictionary<int, clsDataPackageDatasetInfo> dctDataPackageDatasets)
        {

            // Gigasax.DMS_Pipeline
            var connectionString = m_mgrParams.GetParam("brokerconnectionstring");

            var dataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1);

            if (dataPackageID < 0)
            {
                dctDataPackageDatasets = new Dictionary<int, clsDataPackageDatasetInfo>();
                return false;
            }

            return LoadDataPackageDatasetInfo(connectionString, dataPackageID, out dctDataPackageDatasets);
        }

        /// <summary>
        /// Looks up dataset information for a data package
        /// </summary>
        /// <param name="connectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
        /// <param name="dataPackageID">Data Package ID</param>
        /// <param name="dctDataPackageDatasets">Datasets associated with the given data package</param>
        /// <returns>True if a data package is defined and it has datasets associated with it</returns>
        /// <remarks></remarks>
        public static bool LoadDataPackageDatasetInfo(string connectionString, int dataPackageID, out Dictionary<int, clsDataPackageDatasetInfo> dctDataPackageDatasets)
        {

            // Obtains the dataset information for a data package
            const short RETRY_COUNT = 3;

            dctDataPackageDatasets = new Dictionary<int, clsDataPackageDatasetInfo>();

            var sqlStr = new System.Text.StringBuilder();

            // View V_DMS_Data_Package_Datasets is in the DMS_Pipeline database
            // That view references view V_DMS_Data_Package_Aggregation_Datasets in the DMS_Data_Package database
            // That view pulls information from several tables in the DMS_Data_Package database, plus 3 views in DMS5:
            //   V_Dataset_Folder_Path, V_Organism_Export, and V_Dataset_Archive_Path
            // Experiment_NEWT_ID comes from the organism for the experiment, and actually comes from field NCBI_Taxonomy_ID in T_Organisms
            //
            sqlStr.Append(" SELECT Dataset, DatasetID, Instrument, InstrumentGroup, ");
            sqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism, Experiment_NEWT_ID, Experiment_NEWT_Name, ");
            sqlStr.Append("        Dataset_Folder_Path, Archive_Folder_Path, RawDataType");
            sqlStr.Append(" FROM V_DMS_Data_Package_Datasets");
            sqlStr.Append(" WHERE Data_Package_ID = " + dataPackageID);
            sqlStr.Append(" ORDER BY Dataset");


            // Get a table to hold the results of the query
            var success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), connectionString, "LoadDataPackageDatasetInfo", RETRY_COUNT, out var resultSet);

            if (!success)
            {
                var errorMessage = "LoadDataPackageDatasetInfo; Excessive failures attempting to retrieve data package dataset info from database";
                clsGlobal.LogError(errorMessage);
                resultSet.Dispose();
                return false;
            }

            // Verify at least one row returned
            if (resultSet.Rows.Count < 1)
            {
                // No data was returned
                var warningMessage = "LoadDataPackageDatasetInfo; No datasets were found for data package " + dataPackageID;
                clsGlobal.LogError(warningMessage);
                return false;
            }

            foreach (DataRow curRow in resultSet.Rows)
            {
                var udtDatasetInfo = ParseDataPackageDatasetInfoRow(curRow);

                if (!dctDataPackageDatasets.ContainsKey(udtDatasetInfo.DatasetID))
                {
                    dctDataPackageDatasets.Add(udtDatasetInfo.DatasetID, udtDatasetInfo);
                }
            }

            resultSet.Dispose();
            return true;

        }

        /// <summary>
        /// Looks up dataset information for the data package associated with this analysis job
        /// </summary>
        /// <param name="dctDataPackageJobs"></param>
        /// <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
        /// <remarks></remarks>
        protected bool LoadDataPackageJobInfo(out Dictionary<int, clsDataPackageJobInfo> dctDataPackageJobs)
        {

            // Gigasax.DMS_Pipeline
            var connectionString = m_mgrParams.GetParam("brokerconnectionstring");

            var dataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1);

            if (dataPackageID < 0)
            {
                dctDataPackageJobs = new Dictionary<int, clsDataPackageJobInfo>();
                return false;
            }

            return LoadDataPackageJobInfo(connectionString, dataPackageID, out dctDataPackageJobs);
        }

        /// <summary>
        /// Looks up job information for a data package
        /// </summary>
        /// <param name="ConnectionString">Database connection string (DMS_Pipeline DB, aka the broker DB)</param>
        /// <param name="DataPackageID">Data Package ID</param>
        /// <param name="dctDataPackageJobs">Jobs associated with the given data package</param>
        /// <returns>True if a data package is defined and it has analysis jobs associated with it</returns>
        /// <remarks></remarks>
        public static bool LoadDataPackageJobInfo(string ConnectionString, int DataPackageID, out Dictionary<int, clsDataPackageJobInfo> dctDataPackageJobs)
        {

            const short RETRY_COUNT = 3;

            dctDataPackageJobs = new Dictionary<int, clsDataPackageJobInfo>();

            var sqlStr = new System.Text.StringBuilder();

            // Note that this queries view V_DMS_Data_Package_Aggregation_Jobs in the DMS_Pipeline database
            // That view references   view V_DMS_Data_Package_Aggregation_Jobs in the DMS_Data_Package database
            // The two views have the same name, but some columns differ

            sqlStr.Append(" SELECT Job, Dataset, DatasetID, Instrument, InstrumentGroup, ");
            sqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism, Experiment_NEWT_ID, Experiment_NEWT_Name, ");
            sqlStr.Append("        Tool, ResultType, SettingsFileName, ParameterFileName, ");
            sqlStr.Append("        OrganismDBName, ProteinCollectionList, ProteinOptions,");
            sqlStr.Append("        ServerStoragePath, ArchiveStoragePath, ResultsFolder, DatasetFolder, SharedResultsFolder, RawDataType");
            sqlStr.Append(" FROM V_DMS_Data_Package_Aggregation_Jobs");
            sqlStr.Append(" WHERE Data_Package_ID = " + DataPackageID);
            sqlStr.Append(" ORDER BY Dataset, Tool");


            // Get a table to hold the results of the query
            var success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), ConnectionString, "LoadDataPackageJobInfo", RETRY_COUNT, out var resultSet);

            if (!success)
            {
                var errorMessage = "LoadDataPackageJobInfo; Excessive failures attempting to retrieve data package job info from database";
                clsGlobal.LogError(errorMessage);
                resultSet.Dispose();
                return false;
            }

            // Verify at least one row returned
            if (resultSet.Rows.Count < 1)
            {
                // No data was returned
                string warningMessage;

                // If the data package exists and has datasets associated with it, then Log this as a warning but return true
                // Otherwise, log an error and return false

                sqlStr.Clear();
                sqlStr.Append(" SELECT Count(*) AS Datasets");
                sqlStr.Append(" FROM S_V_DMS_Data_Package_Aggregation_Datasets");
                sqlStr.Append(" WHERE Data_Package_ID = " + DataPackageID);

                // Get a table to hold the results of the query
                success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), ConnectionString, "LoadDataPackageJobInfo", RETRY_COUNT, out resultSet);
                if (success && resultSet.Rows.Count > 0)
                {
                    foreach (DataRow curRow in resultSet.Rows)
                    {
                        var datasetCount = clsGlobal.DbCInt(curRow[0]);

                        if (datasetCount > 0)
                        {
                            warningMessage = "LoadDataPackageJobInfo; No jobs were found for data package " + DataPackageID + ", but it does have " + datasetCount + " dataset";
                            if (datasetCount > 1)
                                warningMessage += "s";
                            clsGlobal.LogWarning(warningMessage);
                            return true;
                        }
                    }
                }

                warningMessage = "LoadDataPackageJobInfo; No jobs were found for data package " + DataPackageID;
                clsGlobal.LogWarning(warningMessage);
                return false;
            }

            foreach (DataRow curRow in resultSet.Rows)
            {
                var dataPkgJob = ParseDataPackageJobInfoRow(curRow);

                if (!dctDataPackageJobs.ContainsKey(dataPkgJob.Job))
                {
                    dctDataPackageJobs.Add(dataPkgJob.Job, dataPkgJob);
                }
            }

            resultSet.Dispose();

            return true;

        }

        // ReSharper disable once MemberCanBePrivate.Global
        protected void LogDebugMessage(string debugMessage)
        {
            clsGlobal.LogDebug(debugMessage);
        }

        protected static void LogDebugMessage(string debugMessage, IStatusFile statusTools)
        {
            clsGlobal.LogDebug(debugMessage);

            if (statusTools != null)
            {
                statusTools.CurrentOperation = debugMessage;
                statusTools.UpdateAndWrite(0);
            }
        }

        /// <summary>
        /// Retrieve the information for the specified analysis job
        /// </summary>
        /// <param name="jobNumber">Job number</param>
        /// <param name="jobInfo">Output parameter: Job Info</param>
        /// <returns>True if success; false if an error</returns>
        /// <remarks>This procedure is used by clsAnalysisResourcesQCART</remarks>
        protected bool LookupJobInfo(int jobNumber, out clsDataPackageJobInfo jobInfo)
        {

            const int RETRY_COUNT = 3;

            var sqlStr = new System.Text.StringBuilder();

            // This query uses view V_Analysis_Job_Export_DataPkg in the DMS5 database
            sqlStr.Append("SELECT Job, Dataset, DatasetID, InstrumentName As Instrument, InstrumentGroup,");
            sqlStr.Append("        Experiment, Experiment_Reason, Experiment_Comment, Organism, Experiment_NEWT_ID, Experiment_NEWT_Name, ");
            sqlStr.Append("        Tool, ResultType, SettingsFileName, ParameterFileName,");
            sqlStr.Append("        OrganismDBName, ProteinCollectionList, ProteinOptions,");
            sqlStr.Append("        ServerStoragePath, ArchiveStoragePath, ResultsFolder, DatasetFolder, '' AS SharedResultsFolder, RawDataType ");
            sqlStr.Append("FROM V_Analysis_Job_Export_DataPkg ");
            sqlStr.Append("WHERE Job = " + jobNumber);

            jobInfo = new clsDataPackageJobInfo(0, string.Empty);

            // Gigasax.DMS5
            var dmsConnectionString = m_mgrParams.GetParam("connectionstring");

            // Get a table to hold the results of the query
            var success = clsGlobal.GetDataTableByQuery(sqlStr.ToString(), dmsConnectionString, "LookupJobInfo", RETRY_COUNT, out var resultSet);

            if (!success)
            {
                var errorMessage = "LookupJobInfo; Excessive failures attempting to retrieve data package job info from database";
                LogMessage(errorMessage, 0, true);
                resultSet.Dispose();
                return false;
            }

            // Verify at least one row returned
            if (resultSet.Rows.Count < 1)
            {
                // No data was returned
                LogError("Job " + jobNumber + " not found in view V_Analysis_Job_Export_DataPkg");
                return false;
            }

            jobInfo = ParseDataPackageJobInfoRow(resultSet.Rows[0]);

            return true;

        }

        /// <summary>
        /// Estimate the amount of disk space required for the FASTA file associated with this analysis job
        /// </summary>
        /// <param name="proteinCollectionInfo">Collection info object</param>
        /// <returns>Space required, in MB</returns>
        /// <remarks>Uses both m_jobParams and m_mgrParams; returns 0 if a problem (e.g. the legacy fasta file is not listed in V_Organism_DB_File_Export)</remarks>
        public double LookupLegacyDBDiskSpaceRequiredMB(clsProteinCollectionInfo proteinCollectionInfo)
        {

            try
            {
                var dmsConnectionString = m_mgrParams.GetParam("connectionstring");
                if (string.IsNullOrWhiteSpace(dmsConnectionString))
                {
                    LogError("Error in LookupLegacyDBSizeWithIndices: manager parameter connectionstring is not defined");
                    return 0;
                }

                string legacyFastaName;
                if (proteinCollectionInfo.UsingSplitFasta && !RunningDataExtraction)
                {
                    legacyFastaName = GetSplitFastaFileName(m_jobParams, out var _);
                }
                else
                {
                    legacyFastaName = proteinCollectionInfo.LegacyFastaName;
                }

                var sqlQuery = "SELECT File_Size_KB FROM V_Organism_DB_File_Export WHERE (FileName = '" + legacyFastaName + "')";

                // Results, as a list of columns (first row only if multiple rows)

                var success = clsGlobal.GetQueryResultsTopRow(sqlQuery, dmsConnectionString, out var lstResults, "LookupLegacyDBSizeWithIndices");

                if (!success || lstResults == null || lstResults.Count == 0)
                {
                    // Empty query results
                    var statusMessage = "Warning: Could not determine the legacy fasta file's size for job " + m_JobNum + ", file " + legacyFastaName;
                    if (proteinCollectionInfo.UsingSplitFasta)
                    {
                        // Likely the FASTA file has not yet been split
                        LogMessage(statusMessage + "; likely the split fasta file has not yet been created");
                    }
                    else
                    {
                        LogMessage(statusMessage, 0, true);
                    }

                    return 0;
                }

                if (!int.TryParse(lstResults.First(), out var fileSizeKB))
                {
                    LogMessage("Legacy fasta file size is not numeric, job " + m_JobNum + ", file " + legacyFastaName + ": " + lstResults.First(), 0, true);
                    return 0;
                }

                // Assume that the MSGF+ index files will be 15 times larger than the legacy FASTA file itself
                var fileSizeMB = (fileSizeKB + fileSizeKB * 15) / 1024.0;

                // Pad the expected size by an additional 15%
                return fileSizeMB * 1.15;

            }
            catch (Exception ex)
            {
                LogError("Error in LookupLegacyDBSizeWithIndices", ex);
                return 0;
            }

        }

        /// <summary>
        /// Look for StepParameter section entries in the source file and insert them into the master file
        /// If the master file already has information on the given job step, information for that step is not copied from the source
        /// This is because the master file is assumed to be newer than the source file
        /// </summary>
        /// <param name="sourceJobParamXMLFilePath"></param>
        /// <param name="masterJobParamXMLFilePath"></param>
        /// <returns></returns>
        private bool MergeJobParamXMLStepParameters(string sourceJobParamXMLFilePath, string masterJobParamXMLFilePath)
        {

            try
            {
                var sourceDoc = XDocument.Load(sourceJobParamXMLFilePath);

                var masterDoc = XDocument.Load(masterJobParamXMLFilePath);

                // Keys in the stepParamsSections dictionaries are step numbers
                // Values are the XElement node with the step parameters for the given step

                var stepParamSectionsSource = GetStepParametersSections(sourceDoc);

                var stepParamSectionsMaster = GetStepParametersSections(masterDoc);

                var stepParamSectionsMerged = new Dictionary<int, XElement>();

                // Initialize stepParamSectionsMerged using stepParamSectionsMaster
                foreach (var section in stepParamSectionsMaster)
                {

                    stepParamSectionsMerged.Add(section.Key, section.Value);
                }

                // Add missing steps to stepParamSectionsMerged
                foreach (var section in stepParamSectionsSource)
                {
                    if (!stepParamSectionsMerged.ContainsKey(section.Key))
                    {
                        stepParamSectionsMerged.Add(section.Key, section.Value);
                    }
                }

                // Remove the StepParameter items from the master, then add in the merged items
                foreach (var section in stepParamSectionsMaster)
                {
                    section.Value.Remove();
                }

                var sectionsNode = masterDoc.Elements("sections").First();

                foreach (var section in from item in stepParamSectionsMerged orderby item.Key select item.Value)
                {
                    sectionsNode.Add(section);
                }

                var settings = new XmlWriterSettings
                {
                    Indent = true,
                    IndentChars = "  ",
                    OmitXmlDeclaration = true
                };

                using (var fileWriter = new StreamWriter(new FileStream(masterJobParamXMLFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)))
                {

                    using (var writer = XmlWriter.Create(fileWriter, settings))
                    {
                        masterDoc.Save(writer);
                    }

                }

                return true;

            }
            catch (Exception ex)
            {
                LogError("Error in MergeJobParamXMLStepParameters", ex);
                return false;
            }

        }

        /// <summary>
        /// Moves a file from one folder to another folder
        /// </summary>
        /// <param name="diSourceFolder"></param>
        /// <param name="diTargetFolder"></param>
        /// <param name="sourceFileName"></param>
        /// <remarks></remarks>
        protected void MoveFileToFolder(DirectoryInfo diSourceFolder, DirectoryInfo diTargetFolder, string sourceFileName)
        {
            var fiSourceFile = new FileInfo(Path.Combine(diSourceFolder.FullName, sourceFileName));
            var targetFilePath = Path.Combine(diTargetFolder.FullName, sourceFileName);
            fiSourceFile.MoveTo(targetFilePath);
        }

        /// <summary>
        /// Override current job information, including dataset name, dataset ID, storage paths, Organism Name, Protein Collection, and protein options
        /// </summary>
        /// <param name="dataPkgJob"></param>
        /// <returns></returns>
        /// <remarks> Does not override the job number</remarks>
        public bool OverrideCurrentDatasetAndJobInfo(clsDataPackageJobInfo dataPkgJob)
        {

            var aggregationJob = false;

            if (string.IsNullOrEmpty(dataPkgJob.Dataset))
            {
                LogError("OverrideCurrentDatasetAndJobInfo; Column 'Dataset' not defined for job " + dataPkgJob.Job + " in the data package");
                return false;
            }

            if (clsGlobal.IsMatch(dataPkgJob.Dataset, "Aggregation"))
            {
                aggregationJob = true;
            }

            if (!aggregationJob)
            {
                // Update job params to have the details for the current dataset
                // This is required so that we can use FindDataFile to find the desired files
                if (string.IsNullOrEmpty(dataPkgJob.ServerStoragePath))
                {
                    LogError("OverrideCurrentDatasetAndJobInfo; Column 'ServerStoragePath' not defined for job " + dataPkgJob.Job + " in the data package");
                    return false;
                }

                if (string.IsNullOrEmpty(dataPkgJob.ArchiveStoragePath))
                {
                    LogError("OverrideCurrentDatasetAndJobInfo; Column 'ArchiveStoragePath' not defined for job " + dataPkgJob.Job + " in the data package");
                    return false;
                }

                if (string.IsNullOrEmpty(dataPkgJob.ResultsFolderName))
                {
                    LogError("OverrideCurrentDatasetAndJobInfo; Column 'ResultsFolderName' not defined for job " + dataPkgJob.Job + " in the data package");
                    return false;
                }

                if (string.IsNullOrEmpty(dataPkgJob.DatasetFolderName))
                {
                    LogError("OverrideCurrentDatasetAndJobInfo; Column 'DatasetFolderName' not defined for job " + dataPkgJob.Job + " in the data package");
                    return false;
                }
            }

            m_jobParams.AddDatasetInfo(dataPkgJob.Dataset, dataPkgJob.DatasetID);
            DatasetName = string.Copy(dataPkgJob.Dataset);

            const string jobParamsSection = clsAnalysisJob.JOB_PARAMETERS_SECTION;

            m_jobParams.AddAdditionalParameter(jobParamsSection, "DatasetNum", dataPkgJob.Dataset);
            m_jobParams.AddAdditionalParameter(jobParamsSection, "DatasetID", dataPkgJob.DatasetID.ToString());

            m_jobParams.AddAdditionalParameter(jobParamsSection, "Instrument", dataPkgJob.Instrument);
            m_jobParams.AddAdditionalParameter(jobParamsSection, "InstrumentGroup", dataPkgJob.InstrumentGroup);

            m_jobParams.AddAdditionalParameter(jobParamsSection, "ToolName", dataPkgJob.Tool);
            m_jobParams.AddAdditionalParameter(jobParamsSection, "ResultType", dataPkgJob.ResultType);
            m_jobParams.AddAdditionalParameter(jobParamsSection, "SettingsFileName", dataPkgJob.SettingsFileName);

            m_jobParams.AddAdditionalParameter("PeptideSearch", "ParmFileName", dataPkgJob.ParameterFileName);

            if (string.IsNullOrWhiteSpace(dataPkgJob.OrganismDBName))
            {
                m_jobParams.AddAdditionalParameter("PeptideSearch", JOB_PARAM_GENERATED_FASTA_NAME, "na");
            }
            else
            {
                m_jobParams.AddAdditionalParameter("PeptideSearch", JOB_PARAM_GENERATED_FASTA_NAME, dataPkgJob.OrganismDBName);
            }

            if (string.IsNullOrWhiteSpace(dataPkgJob.ProteinCollectionList) || dataPkgJob.ProteinCollectionList == "na")
            {
                m_jobParams.AddAdditionalParameter("PeptideSearch", "legacyFastaFileName", dataPkgJob.OrganismDBName);
            }
            else
            {
                m_jobParams.AddAdditionalParameter("PeptideSearch", "legacyFastaFileName", "na");
            }

            m_jobParams.AddAdditionalParameter("PeptideSearch", "ProteinCollectionList", dataPkgJob.ProteinCollectionList);
            m_jobParams.AddAdditionalParameter("PeptideSearch", "ProteinOptions", dataPkgJob.ProteinOptions);

            m_jobParams.AddAdditionalParameter(jobParamsSection, "DatasetStoragePath", dataPkgJob.ServerStoragePath);
            m_jobParams.AddAdditionalParameter(jobParamsSection, "DatasetArchivePath", dataPkgJob.ArchiveStoragePath);
            m_jobParams.AddAdditionalParameter(jobParamsSection, "inputFolderName", dataPkgJob.ResultsFolderName);
            m_jobParams.AddAdditionalParameter(jobParamsSection, "DatasetFolderName", dataPkgJob.DatasetFolderName);
            m_jobParams.AddAdditionalParameter(jobParamsSection, "SharedResultsFolders", dataPkgJob.SharedResultsFolder);
            m_jobParams.AddAdditionalParameter(jobParamsSection, "RawDataType", dataPkgJob.RawDataType);

            return true;

        }

        private static clsDataPackageDatasetInfo ParseDataPackageDatasetInfoRow(DataRow curRow)
        {

            var datasetName = clsGlobal.DbCStr(curRow["Dataset"]);
            var datasetId = clsGlobal.DbCInt(curRow["DatasetID"]);

            var datasetInfo = new clsDataPackageDatasetInfo(datasetName, datasetId)
            {
                Instrument = clsGlobal.DbCStr(curRow["Instrument"]),
                InstrumentGroup = clsGlobal.DbCStr(curRow["InstrumentGroup"]),
                Experiment = clsGlobal.DbCStr(curRow["Experiment"]),
                Experiment_Reason = clsGlobal.DbCStr(curRow["Experiment_Reason"]),
                Experiment_Comment = clsGlobal.DbCStr(curRow["Experiment_Comment"]),
                Experiment_Organism = clsGlobal.DbCStr(curRow["Organism"]),
                Experiment_NEWT_ID = clsGlobal.DbCInt(curRow["Experiment_NEWT_ID"]),
                Experiment_NEWT_Name = clsGlobal.DbCStr(curRow["Experiment_NEWT_Name"]),
                ServerStoragePath = clsGlobal.DbCStr(curRow["Dataset_Folder_Path"]),
                ArchiveStoragePath = clsGlobal.DbCStr(curRow["Archive_Folder_Path"]),
                RawDataType = clsGlobal.DbCStr(curRow["RawDataType"])
            };

            return datasetInfo;

        }

        private static clsDataPackageJobInfo ParseDataPackageJobInfoRow(DataRow curRow)
        {

            var dataPkgJob = clsGlobal.DbCInt(curRow["Job"]);
            var dataPkgDataset = clsGlobal.DbCStr(curRow["Dataset"]);

            var jobInfo = new clsDataPackageJobInfo(dataPkgJob, dataPkgDataset)
            {
                DatasetID = clsGlobal.DbCInt(curRow["DatasetID"]),
                Instrument = clsGlobal.DbCStr(curRow["Instrument"]),
                InstrumentGroup = clsGlobal.DbCStr(curRow["InstrumentGroup"]),
                Experiment = clsGlobal.DbCStr(curRow["Experiment"]),
                Experiment_Reason = clsGlobal.DbCStr(curRow["Experiment_Reason"]),
                Experiment_Comment = clsGlobal.DbCStr(curRow["Experiment_Comment"]),
                Experiment_Organism = clsGlobal.DbCStr(curRow["Organism"]),
                Experiment_NEWT_ID = clsGlobal.DbCInt(curRow["Experiment_NEWT_ID"]),
                Experiment_NEWT_Name = clsGlobal.DbCStr(curRow["Experiment_NEWT_Name"]),
                Tool = clsGlobal.DbCStr(curRow["Tool"]),
                ResultType = clsGlobal.DbCStr(curRow["ResultType"])
            };

            jobInfo.PeptideHitResultType = clsPHRPReader.GetPeptideHitResultType(jobInfo.ResultType);
            jobInfo.SettingsFileName = clsGlobal.DbCStr(curRow["SettingsFileName"]);
            jobInfo.ParameterFileName = clsGlobal.DbCStr(curRow["ParameterFileName"]);
            jobInfo.OrganismDBName = clsGlobal.DbCStr(curRow["OrganismDBName"]);
            jobInfo.ProteinCollectionList = clsGlobal.DbCStr(curRow["ProteinCollectionList"]);
            jobInfo.ProteinOptions = clsGlobal.DbCStr(curRow["ProteinOptions"]);

            // This will be updated later for SplitFasta jobs (using function LookupJobParametersFromHistory)
            jobInfo.NumberOfClonedSteps = 0;

            if (string.IsNullOrWhiteSpace(jobInfo.ProteinCollectionList) || jobInfo.ProteinCollectionList == "na")
            {
                jobInfo.LegacyFastaFileName = string.Copy(jobInfo.OrganismDBName);
            }
            else
            {
                jobInfo.LegacyFastaFileName = "na";
            }

            jobInfo.ServerStoragePath = clsGlobal.DbCStr(curRow["ServerStoragePath"]);
            jobInfo.ArchiveStoragePath = clsGlobal.DbCStr(curRow["ArchiveStoragePath"]);
            jobInfo.ResultsFolderName = clsGlobal.DbCStr(curRow["ResultsFolder"]);
            jobInfo.DatasetFolderName = clsGlobal.DbCStr(curRow["DatasetFolder"]);
            jobInfo.SharedResultsFolder = clsGlobal.DbCStr(curRow["SharedResultsFolder"]);
            jobInfo.RawDataType = clsGlobal.DbCStr(curRow["RawDataType"]);

            return jobInfo;

        }

        /// <summary>
        /// Download any queued files from MyEMSL
        /// </summary>
        /// <returns></returns>
        public bool ProcessMyEMSLDownloadQueue()
        {
            if (m_MyEMSLUtilities.FilesToDownload.Count > 0)
            {
                // ReSharper disable once RedundantNameQualifier
                if (!m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Download any queued files from MyEMSL
        /// </summary>
        /// <param name="downloadFolderPath"></param>
        /// <param name="folderLayout"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool ProcessMyEMSLDownloadQueue(string downloadFolderPath, Downloader.DownloadFolderLayout folderLayout)
        {
            var success = m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(downloadFolderPath, folderLayout);
            return success;
        }

        /// <summary>
        /// Delete the specified FASTA file and its associated files
        /// </summary>
        /// <param name="diOrgDbFolder"></param>
        /// <param name="fiFileToPurge"></param>
        /// <param name="legacyFastaFileBaseName"></param>
        /// <returns>Number of bytes deleted</returns>
        // ReSharper disable once SuggestBaseTypeForParameter
        private long PurgeFastaFiles(DirectoryInfo diOrgDbFolder, FileInfo fiFileToPurge, string legacyFastaFileBaseName)
        {

            var baseName = Path.GetFileNameWithoutExtension(fiFileToPurge.Name);

            if (!string.IsNullOrWhiteSpace(legacyFastaFileBaseName) && baseName.StartsWith(legacyFastaFileBaseName, StringComparison.InvariantCultureIgnoreCase))
            {
                // The current job needs this file; do not delete it
                return 0;
            }

            // Delete all files associated with this fasta file
            var lstFilesToDelete = new List<FileInfo>();
            lstFilesToDelete.AddRange(diOrgDbFolder.GetFiles(baseName + ".*"));

            if (m_DebugLevel >= 1)
            {
                var fileText = string.Format("{0,2} file", lstFilesToDelete.Count);
                if (lstFilesToDelete.Count != 1)
                {
                    fileText += "s";
                }
                LogDebugMessage("Deleting " + fileText + " associated with " + fiFileToPurge.FullName);
            }

            long bytesDeleted = 0;

            try
            {
                foreach (var fiFileToDelete in lstFilesToDelete)
                {
                    var fileSizeBytes = fiFileToDelete.Length;
                    fiFileToDelete.Delete();
                    bytesDeleted += fileSizeBytes;
                }
            }
            catch (Exception ex)
            {
                LogError("Error in PurgeFastaFiles", ex);
            }

            return bytesDeleted;

        }

        /// <summary>
        /// Purges old fasta files (and related suffix array files) from localOrgDbFolder
        /// </summary>
        /// <param name="localOrgDbFolder"></param>
        /// <param name="freeSpaceThresholdPercent">Value between 1 and 50</param>
        /// <param name="requiredFreeSpaceMB">If greater than 0, the free space that we anticipate will be needed for the given fasta file</param>
        /// <param name="legacyFastaFileBaseName">
        /// Legacy fasta file name (without .fasta)
        /// For split fasta jobs, should not include the splitcount and segment number, e.g. should not include _25x_07 or _25x_08
        /// </param>
        /// <remarks></remarks>
        protected void PurgeFastaFilesIfLowFreeSpace(string localOrgDbFolder, int freeSpaceThresholdPercent, double requiredFreeSpaceMB, string legacyFastaFileBaseName)
        {
            if (freeSpaceThresholdPercent < 1)
                freeSpaceThresholdPercent = 1;
            if (freeSpaceThresholdPercent > 50)
                freeSpaceThresholdPercent = 50;

            try
            {
                var diOrgDbFolder = new DirectoryInfo(localOrgDbFolder);
                if (diOrgDbFolder.FullName.Length <= 2)
                {
                    LogMessage("Warning: Org DB folder length is less than 3 characters; this is unexpected: " + diOrgDbFolder.FullName);
                    return;
                }

                // Look for file MaxDirSize.txt which defines the maximum space that the files can use
                var fiMaxDirSize = new FileInfo(Path.Combine(diOrgDbFolder.FullName, "MaxDirSize.txt"));

                var driveLetter = diOrgDbFolder.FullName.Substring(0, 2);

                if (!driveLetter.EndsWith(":"))
                {
                    // The folder is not local to this computer
                    if (!fiMaxDirSize.Exists)
                    {
                        LogError("Warning: Orb DB folder path does not have a colon and could not find file " + fiMaxDirSize.Name + "; cannot manage drive space usage: " + diOrgDbFolder.FullName);
                        return;
                    }

                    // MaxDirSize.txt file found; delete the older FASTA files to free up space
                    PurgeFastaFilesUsingSpaceUsedThreshold(fiMaxDirSize, legacyFastaFileBaseName);
                    return;
                }

                var localDriveInfo = new DriveInfo(driveLetter);
                var percentFreeSpaceAtStart = localDriveInfo.AvailableFreeSpace / (double)localDriveInfo.TotalSize * 100;

                if ((percentFreeSpaceAtStart >= freeSpaceThresholdPercent))
                {
                    if (m_DebugLevel >= 2)
                    {
                        var freeSpaceGB = clsGlobal.BytesToGB(localDriveInfo.AvailableFreeSpace);
                        LogMessage(string.Format("Free space on {0} ({1:F1} GB) is over {2}% of the total space; purge not required", localDriveInfo.Name, freeSpaceGB, freeSpaceThresholdPercent));
                    }
                }
                else
                {
                    PurgeFastaFilesUsingFreeSpaceThreshold(localDriveInfo, diOrgDbFolder, legacyFastaFileBaseName, freeSpaceThresholdPercent, requiredFreeSpaceMB, percentFreeSpaceAtStart);
                }

                if (fiMaxDirSize.Exists)
                {
                    // MaxDirSize.txt file exists; possibly delete additional FASTA files to free up space
                    PurgeFastaFilesUsingSpaceUsedThreshold(fiMaxDirSize, legacyFastaFileBaseName);
                }

            }
            catch (Exception ex)
            {
                LogError("Error in PurgeFastaFilesIfLowFreeSpace", ex);
            }

        }

        private void PurgeFastaFilesUsingFreeSpaceThreshold(
            DriveInfo localDriveInfo,
            DirectoryInfo diOrgDbFolder,
            string legacyFastaFileBaseName,
            int freeSpaceThresholdPercent,
            double requiredFreeSpaceMB,
            double percentFreeSpaceAtStart)
        {
            var freeSpaceGB = clsGlobal.BytesToGB(localDriveInfo.AvailableFreeSpace);

            var logInfoMessages = m_DebugLevel >= 1 && freeSpaceGB < 100 || m_DebugLevel >= 2 && freeSpaceGB < 250 || m_DebugLevel >= 3;

            if (logInfoMessages)
            {
                LogMessage(string.Format("Free space on {0} ({1:F1} GB) is {2:F1}% of the total space; purge required since less than threshold of {3}%",
                    localDriveInfo.Name, freeSpaceGB, percentFreeSpaceAtStart, freeSpaceThresholdPercent));
            }

            // Obtain a dictionary of FASTA files where Keys are FileInfo and values are last usage date
            var dctFastaFiles = GetFastaFilesByLastUse(diOrgDbFolder);

            var lstFastaFilesByLastUse = (from item in dctFastaFiles orderby item.Value select item.Key);
            long totalBytesPurged = 0;

            foreach (var fiFileToPurge in lstFastaFilesByLastUse)
            {
                // Abort this process if the LastUsed date of this file is less than 5 days old
                if (dctFastaFiles.TryGetValue(fiFileToPurge, out var dtLastUsed))
                {
                    if (DateTime.UtcNow.Subtract(dtLastUsed).TotalDays < 5)
                    {
                        if (logInfoMessages)
                        {
                            LogMessage("All fasta files in " + diOrgDbFolder.FullName + " are less than 5 days old; " +
                                "will not purge any more files to free disk space");
                        }
                        break;
                    }
                }

                // Delete all files associated with this fasta file
                // However, do not delete it if the name starts with legacyFastaFileBaseName
                var bytesDeleted = PurgeFastaFiles(diOrgDbFolder, fiFileToPurge, legacyFastaFileBaseName);
                totalBytesPurged += bytesDeleted;

                // Re-check the disk free space
                var percentFreeSpace = localDriveInfo.AvailableFreeSpace / (double)localDriveInfo.TotalSize * 100;
                var updatedFreeSpaceGB = clsGlobal.BytesToGB(localDriveInfo.AvailableFreeSpace);

                if (requiredFreeSpaceMB > 0 && updatedFreeSpaceGB * 1024.0 < requiredFreeSpaceMB)
                {
                    // Required free space is known, and we're not yet there
                    // Keep deleting files
                    if (m_DebugLevel >= 2)
                    {
                        LogDebugMessage(string.Format("Free space on {0} ({1:F1} GB) is now {2:F1}% of the total space", localDriveInfo.Name, updatedFreeSpaceGB, percentFreeSpace));
                    }
                }
                else
                {
                    // Either required free space is not known, or we have more than enough free space

                    if ((percentFreeSpace >= freeSpaceThresholdPercent))
                    {
                        // Target threshold reached
                        if (m_DebugLevel >= 1)
                        {
                            LogMessage(string.Format("Free space on {0} ({1:F1} GB) is now over {2}% of the total space; " + "deleted {3:F1} GB of cached files",
                                localDriveInfo.Name, updatedFreeSpaceGB, freeSpaceThresholdPercent, clsGlobal.BytesToGB(totalBytesPurged)));
                        }
                        break;
                    }

                    if (m_DebugLevel >= 2)
                    {
                        // Keep deleting until we reach the target threshold for free space
                        LogDebugMessage(string.Format("Free space on {0} ({1:F1} GB) is now {2:F1}% of the total space", localDriveInfo.Name, updatedFreeSpaceGB, percentFreeSpace));
                    }
                }
            }

            // We have deleted all of the files that can be deleted
            var finalFreeSpaceGB = clsGlobal.BytesToGB(localDriveInfo.AvailableFreeSpace);

            if (requiredFreeSpaceMB > 0 && finalFreeSpaceGB * 1024.0 < requiredFreeSpaceMB)
            {
                LogMessage(string.Format("Warning: unable to delete enough files to free up the required space on {0} " +
                                         "({1:F1} GB vs. {2:F1} GB); " + "deleted {3:F1} GB of cached files",
                    localDriveInfo.Name, finalFreeSpaceGB, requiredFreeSpaceMB / 1024.0, clsGlobal.BytesToGB(totalBytesPurged)));
            }

        }

        /// <summary>
        /// Use the space usage defined in MaxDirSize.txt to decide if any FASTA files need to be deleted
        /// </summary>
        /// <param name="fiMaxDirSize">MaxDirSize.txt file in the local organism DB folder</param>
        /// <param name="legacyFastaFileBaseName">Base FASTA file name for the current analysis job</param>
        private void PurgeFastaFilesUsingSpaceUsedThreshold(FileInfo fiMaxDirSize, string legacyFastaFileBaseName)
        {
            try
            {
                var diOrgDbFolder = fiMaxDirSize.Directory;
                if (diOrgDbFolder == null)
                {
                    LogErrorToDatabase("Cannot purge fasta files to manage drive space usage; " +
                                       "unable to determine the parent directory of " +
                                       fiMaxDirSize.FullName + " on " + System.Net.Dns.GetHostName());
                    return;
                }

                var errorSuffix = "; cannot manage drive space usage: " + diOrgDbFolder.FullName;
                var maxSizeGB = 0;

                using (var reader = new StreamReader(new FileStream(fiMaxDirSize.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!reader.EndOfStream)
                    {
                        var dataLine = reader.ReadLine();
                        if (string.IsNullOrEmpty(dataLine) || dataLine.StartsWith("#"))
                        {
                            continue;
                        }

                        var lineParts = dataLine.Split('=');
                        if (lineParts.Length < 2)
                        {
                            continue;
                        }

                        if (string.Equals(lineParts[0], "MaxSizeGB", StringComparison.InvariantCultureIgnoreCase))
                        {
                            if (!int.TryParse(lineParts[1], out maxSizeGB))
                            {
                                LogError("MaxSizeGB line does not contain an integer in " + fiMaxDirSize.FullName + errorSuffix);
                                return;
                            }
                            break;
                        }

                    }
                }

                if (maxSizeGB == 0)
                {
                    LogError("MaxSizeGB line not found in " + fiMaxDirSize.FullName + errorSuffix);
                    return;
                }

                long spaceUsageBytes = 0;

                foreach (var fiFile in diOrgDbFolder.GetFiles("*", SearchOption.TopDirectoryOnly))
                {
                    spaceUsageBytes += fiFile.Length;
                }

                var spaceUsageGB = clsGlobal.BytesToGB(spaceUsageBytes);
                if (spaceUsageGB <= maxSizeGB)
                {
                    // Space usage is under the threshold
                    var statusMessage = string.Format("Space usage in {0} is {1:F1} GB, which is below the threshold of {2} GB; nothing to purge", diOrgDbFolder.FullName, spaceUsageGB, maxSizeGB);
                    LogMessage(statusMessage, 3);
                    return;
                }

                // Space usage is too high; need to purge some files
                // Obtain a dictionary of FASTA files where Keys are FileInfo and values are last usage date
                var dctFastaFiles = GetFastaFilesByLastUse(diOrgDbFolder);

                var lstFastaFilesByLastUse = from item in dctFastaFiles orderby item.Value select item.Key;

                var bytesToPurge = (long)(spaceUsageBytes - maxSizeGB * 1024.0 * 1024 * 1024);
                long totalBytesPurged = 0;

                foreach (var fiFileToPurge in lstFastaFilesByLastUse)
                {
                    // Abort this process if the LastUsed date of this file is less than 5 days old
                    if (dctFastaFiles.TryGetValue(fiFileToPurge, out var dtLastUsed))
                    {
                        if (DateTime.UtcNow.Subtract(dtLastUsed).TotalDays < 5)
                        {
                            LogMessage("All fasta files in " + diOrgDbFolder.FullName + " are less than 5 days old; " +
                                "will not purge any more files to free disk space");
                            break;
                        }
                    }

                    // Delete all files associated with this fasta file
                    // However, do not delete it if the name starts with legacyFastaFileBaseName
                    var bytesDeleted = PurgeFastaFiles(diOrgDbFolder, fiFileToPurge, legacyFastaFileBaseName);
                    totalBytesPurged += bytesDeleted;

                    if (totalBytesPurged < bytesToPurge)
                    {
                        // Keep deleting files
                        if (m_DebugLevel >= 2)
                        {
                            LogDebugMessage(string.Format("Purging FASTA files: {0:F1} / {1:F1} MB deleted",
                                clsGlobal.BytesToMB(totalBytesPurged), clsGlobal.BytesToMB(bytesToPurge)));
                        }
                    }
                    else
                    {
                        // Enough files have been deleted
                        LogMessage(string.Format("Space usage in {0} is now below {1} GB; deleted {2:F1} GB of cached files",
                            diOrgDbFolder.FullName, maxSizeGB, clsGlobal.BytesToGB(totalBytesPurged)));
                        return;
                    }
                }

                if (totalBytesPurged < bytesToPurge)
                {
                    LogMessage(string.Format("Warning: unable to delete enough files to lower the space usage in {0} to below {1} GB; " +
                        "deleted {2:F1} GB of cached files", diOrgDbFolder.FullName, maxSizeGB, clsGlobal.BytesToGB(totalBytesPurged)));
                }

            }
            catch (Exception ex)
            {
                LogError("Error in PurgeFastaFilesUsingSpaceUsedThreshold", ex);
            }

        }

        private bool RemoteFastaFilesMatch(
            FileInfo sourceFasta, FileInfo sourceHashcheck,
            SftpFile remoteFasta, SftpFile remoteHashcheck,
            clsRemoteTransferUtility transferUtility)
        {

            var remoteHostName = transferUtility.RemoteHostName;

            if (remoteFasta == null)
            {
                LogDebug(string.Format("Fasta file not found on remote host; copying {0} to {1}", sourceFasta.Name, remoteHostName));
                return false;
            }

            if (remoteHashcheck == null)
            {
                LogDebug(string.Format("Fasta .hashckeck file not found on remote host; copying {0} to {1}", sourceFasta.Name, remoteHostName));
                return false;
            }

            if (remoteFasta.Length == sourceFasta.Length && remoteHashcheck.Length == sourceHashcheck.Length)
            {
                return true;
            }

            // File size mismatch
            if (remoteFasta.Length < sourceFasta.Length)
            {
                // Another manager may be copying the file at present
                // If the modification time is within the last 15 minutes, wait for the other manager to finish copying the file
                var filesMatch = transferUtility.WaitForRemoteFileCopy(sourceFasta, remoteFasta, out var abortCopy);

                if (abortCopy)
                {
                    LogError(string.Format("File size mismatch; WaitForRemoteFileCopy reports abortCopy=true for remote file {0}",
                                           remoteFasta.FullName));
                    return false;
                }

                if (filesMatch)
                {
                    LogDebug(string.Format("Using existing FASTA file {0} on {1}",
                                           remoteFasta.FullName, transferUtility.RemoteHostName));
                    return true;
                }

                LogDebug(string.Format("Copying {0} to {1}", sourceFasta.Name, transferUtility.RemoteHostName));
            }
            else
            {

                LogDebug(string.Format("Fasta file size on remote host is different than local file ({0} bytes vs. {1} bytes locally); " +
                                       "copying {2} to {3}", remoteFasta.Length, sourceFasta.Length, sourceFasta.Name,
                                       transferUtility.RemoteHostName));
            }

            return false;
        }

        /// <summary>
        /// Looks for the specified file in the given folder
        /// If present, returns the full path to the file
        /// If not present, looks for a file named FileName_StoragePathInfo.txt; if that file is found, opens the file and reads the path
        /// If the file isn't found (and the _StoragePathInfo.txt file isn't present), returns an empty string
        /// </summary>
        /// <param name="FolderPath">The folder to look in</param>
        /// <param name="FileName">The file name to find</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ResolveStoragePath(string FolderPath, string FileName)
        {

            var physicalFilePath = string.Empty;

            var filePath = Path.Combine(FolderPath, FileName);

            if (File.Exists(filePath))
            {
                // The desired file is located in folder FolderPath
                physicalFilePath = filePath;
            }
            else
            {
                // The desired file was not found
                filePath += STORAGE_PATH_INFO_FILE_SUFFIX;

                if (File.Exists(filePath))
                {
                    // The _StoragePathInfo.txt file is present
                    // Open that file to read the file path on the first line of the file

                    using (var srInFile = new StreamReader(new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                    {
                        var lineIn = srInFile.ReadLine();
                        physicalFilePath = lineIn;
                    }

                }
            }

            return physicalFilePath;

        }

        /// <summary>
        /// Looks for the STORAGE_PATH_INFO_FILE_SUFFIX file in the working folder
        /// If present, looks for a file named _StoragePathInfo.txt; if that file is found, opens the file and reads the path
        /// If the file named _StoragePathInfo.txt isn't found, then looks for a ser file in the specified folder
        /// If found, returns the path to the ser file
        /// If not found, then looks for a 0.ser folder in the specified folder
        /// If found, returns the path to the 0.ser folder
        /// Otherwise, returns an empty string
        /// </summary>
        /// <param name="FolderPath">The folder to look in</param>
        /// <returns></returns>
        /// <remarks></remarks>
        public static string ResolveSerStoragePath(string FolderPath)
        {
            string physicalFilePath;

            var filePath = Path.Combine(FolderPath, STORAGE_PATH_INFO_FILE_SUFFIX);

            if (File.Exists(filePath))
            {
                // The desired file is located in folder FolderPath
                // The _StoragePathInfo.txt file is present
                // Open that file to read the file path on the first line of the file

                using (var srInFile = new StreamReader(new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {

                    var lineIn = srInFile.ReadLine();
                    physicalFilePath = lineIn;

                }
            }
            else
            {
                // The desired file was not found

                // Look for a ser file in the dataset folder
                physicalFilePath = Path.Combine(FolderPath, BRUKER_SER_FILE);
                var fiFile = new FileInfo(physicalFilePath);

                if (!fiFile.Exists)
                {
                    // See if a folder named 0.ser exists in FolderPath
                    physicalFilePath = Path.Combine(FolderPath, BRUKER_ZERO_SER_FOLDER);
                    var diFolder = new DirectoryInfo(physicalFilePath);
                    if (!diFolder.Exists)
                    {
                        physicalFilePath = string.Empty;
                    }
                }

            }

            return physicalFilePath;

        }

        /// <summary>
        /// Retrieve the files specified by the file processing options parameter
        /// </summary>
        /// <param name="fileSpecList">
        /// File processing options, examples:
        /// sequest:_syn.txt:nocopy,sequest:_fht.txt:nocopy,sequest:_dta.zip:nocopy,sequest:_syn_ModSummary.txt:nocopy,masic_finnigan:_ScanStatsEx.txt:nocopy
        /// sequest:_syn.txt,sequest:_syn_MSGF.txt,sequest:_fht.txt,sequest:_fht_MSGF.txt,sequest:_dta.zip,sequest:_syn_ModSummary.txt
        /// MSGFPlus:_msgfplus_syn.txt,MSGFPlus:_msgfplus_fht.txt,MSGFPlus:_dta.zip,MSGFPlus:_syn_ModSummary.txt,masic_finnigan:_ScanStatsEx.txt,masic_finnigan:_ReporterIons.txt:copy
        /// MSGFPlus:_msgfplus_syn.txt,MSGFPlus:_msgfplus_syn_ModSummary.txt,MSGFPlus:_dta.zip
        /// </param>
        /// <param name="fileRetrievalMode">Used by plugins to indicate the types of files that are required (in case fileSpecList is not configured correctly for a given data package job)</param>
        /// <param name="dctDataPackageJobs"></param>
        /// <returns>True if success, false if a problem</returns>
        /// <remarks>
        /// This function is used by plugins PhosphoFDRAggregator and PRIDEMzXML
        /// However, PrideMzXML is dormant as of September 2013
        /// </remarks>
        protected bool RetrieveAggregateFiles(List<string> fileSpecList, DataPackageFileRetrievalModeConstants fileRetrievalMode,
            out Dictionary<int, clsDataPackageJobInfo> dctDataPackageJobs)
        {

            bool success;

            try
            {
                if (!LoadDataPackageJobInfo(out dctDataPackageJobs))
                {
                    m_message = "Error looking up datasets and jobs using LoadDataPackageJobInfo";
                    dctDataPackageJobs = null;
                    return false;
                }
            }
            catch (Exception ex)
            {
                LogError("RetrieveAggregateFiles; Exception calling LoadDataPackageJobInfo", ex);
                dctDataPackageJobs = null;
                return false;
            }

            try
            {
                var diWorkingDirectory = new DirectoryInfo(m_WorkingDir);

                // Cache the current dataset and job info
                var currentDatasetAndJobInfo = GetCurrentDatasetAndJobInfo();

                foreach (var dataPkgJob in dctDataPackageJobs)
                {
                    if (!OverrideCurrentDatasetAndJobInfo(dataPkgJob.Value))
                    {
                        return false;
                    }

                    // See if this job matches any of the entries in fileSpecList
                    var fileSpecListCurrent = new List<string>();

                    foreach (var fileSpec in fileSpecList)
                    {
                        var fileSpecTerms = fileSpec.Trim().Split(':').ToList();
                        if (dataPkgJob.Value.Tool.StartsWith(fileSpecTerms[0].Trim(), StringComparison.InvariantCultureIgnoreCase))
                        {
                            fileSpecListCurrent = fileSpecList;
                            break;
                        }
                    }

                    if (fileSpecListCurrent.Count == 0)
                    {
                        switch (fileRetrievalMode)
                        {
                            case DataPackageFileRetrievalModeConstants.Ascore:

                                if (dataPkgJob.Value.Tool.StartsWith("msgf", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    // MSGF+
                                    fileSpecListCurrent = new List<string> {
                                        "MSGFPlus:_msgfplus_syn.txt",
                                        "MSGFPlus:_msgfplus_syn_ModSummary.txt",
                                        "MSGFPlus:_dta.zip"
                                    };

                                }

                                if (dataPkgJob.Value.Tool.StartsWith("sequest", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    // Sequest
                                    fileSpecListCurrent = new List<string> {
                                        "sequest:_syn.txt",
                                        "sequest:_syn_MSGF.txt",
                                        "sequest:_syn_ModSummary.txt",
                                        "sequest:_dta.zip"
                                    };

                                }

                                if (dataPkgJob.Value.Tool.StartsWith("xtandem", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    // XTandem
                                    fileSpecListCurrent = new List<string> {
                                        "xtandem:_xt_syn.txt",
                                        "xtandem:_xt_syn_ModSummary.txt",
                                        "xtandem:_dta.zip"
                                    };

                                }

                                break;
                        }
                    }

                    if (fileSpecListCurrent.Count == 0)
                    {
                        continue;
                    }

                    var spectraFileKey = "Job" + dataPkgJob.Key + DATA_PACKAGE_SPECTRA_FILE_SUFFIX;

                    foreach (var fileSpec in fileSpecListCurrent)
                    {
                        var fileSpecTerms = fileSpec.Trim().Split(':').ToList();
                        var sourceFileName = dataPkgJob.Value.Dataset + fileSpecTerms[1].Trim();
                        var sourceFolderPath = "??";

                        var saveMode = "nocopy";
                        if (fileSpecTerms.Count > 2)
                        {
                            saveMode = fileSpecTerms[2].Trim();
                        }

                        try
                        {
                            if (!dataPkgJob.Value.Tool.StartsWith(fileSpecTerms[0].Trim(), StringComparison.InvariantCultureIgnoreCase))
                            {
                                continue;
                            }

                            // To avoid collisions, files for this job will be placed in a subfolder based on the Job number
                            var diTargetFolder = new DirectoryInfo(Path.Combine(m_WorkingDir, "Job" + dataPkgJob.Key));
                            if (!diTargetFolder.Exists)
                                diTargetFolder.Create();

                            if (sourceFileName.EndsWith("_dta.zip", StringComparison.InvariantCultureIgnoreCase) &&
                                dataPkgJob.Value.Tool.EndsWith("_mzml", StringComparison.InvariantCultureIgnoreCase))
                            {
                                // This is a .mzML job; it is not going to have a _dta.zip file
                                // Setting sourceFolderPath to an empty string so that GetMzMLFile will get called below
                                sourceFolderPath = string.Empty;
                            }
                            else
                            {
                                sourceFolderPath = m_FileSearch.FindDataFile(sourceFileName);

                                if (string.IsNullOrEmpty(sourceFolderPath))
                                {
                                    // Source file not found

                                    var alternateSourceFileName = string.Empty;

                                    if (sourceFileName.ToLower().Contains("_msgfdb"))
                                    {
                                        // Auto-look for the _msgfplus version of this file
                                        alternateSourceFileName = clsGlobal.ReplaceIgnoreCase(sourceFileName, "_msgfdb", "_msgfplus");
                                    }
                                    else if (sourceFileName.ToLower().Contains("_msgfplus"))
                                    {
                                        // Auto-look for the _msgfdb version of this file
                                        alternateSourceFileName = clsGlobal.ReplaceIgnoreCase(sourceFileName, "_msgfplus", "_msgfdb");
                                    }

                                    if (!string.IsNullOrEmpty(alternateSourceFileName))
                                    {
                                        sourceFolderPath = m_FileSearch.FindDataFile(alternateSourceFileName);
                                        if (!string.IsNullOrEmpty(sourceFolderPath))
                                        {
                                            sourceFileName = alternateSourceFileName;
                                        }
                                    }
                                }

                            }

                            if (string.IsNullOrEmpty(sourceFolderPath))
                            {
                                if (sourceFileName.EndsWith("_dta.zip", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    // Look for a mzML.gz file instead


                                    var retrieved = m_FileSearch.RetrieveCachedMSXMLFile(DOT_MZML_EXTENSION, false,
                                        out var errorMessage, out var _);

                                    if (!retrieved)
                                    {
                                        if (string.IsNullOrWhiteSpace(errorMessage))
                                        {
                                            errorMessage = "Unknown error looking for the .mzML file for " + dataPkgJob.Value.Dataset + ", job " + dataPkgJob.Key;
                                        }

                                        LogError(errorMessage);
                                        return false;
                                    }

                                    sourceFileName = dataPkgJob.Value.Dataset + DOT_MZML_EXTENSION + DOT_GZ_EXTENSION;
                                    m_jobParams.AddAdditionalParameter("DataPackageMetadata", spectraFileKey, sourceFileName);
                                    m_jobParams.AddResultFileExtensionToSkip(DOT_GZ_EXTENSION);

                                    MoveFileToFolder(diWorkingDirectory, diTargetFolder, sourceFileName);

                                    if (m_DebugLevel >= 1)
                                    {
                                        LogMessage("Retrieved the .mzML file for " + dataPkgJob.Value.Dataset + ", job " + dataPkgJob.Key);
                                    }

                                    continue;
                                }

                                m_message = "Could not find a valid folder with file " + sourceFileName + " for job " + dataPkgJob.Key;
                                if (m_DebugLevel >= 1)
                                {
                                    LogMessage(m_message, 0, true);
                                }
                                return false;
                            }

                            if (!m_FileCopyUtilities.CopyFileToWorkDir(sourceFileName, sourceFolderPath, m_WorkingDir, clsLogTools.LogLevels.ERROR))
                            {
                                m_message = "CopyFileToWorkDir returned False for " + sourceFileName + " using folder " + sourceFolderPath + " for job " + dataPkgJob.Key;
                                if (m_DebugLevel >= 1)
                                {
                                    LogMessage(m_message, 0, true);
                                }
                                return false;
                            }

                            if (sourceFileName.EndsWith("_dta.zip", StringComparison.InvariantCultureIgnoreCase))
                            {
                                m_jobParams.AddAdditionalParameter("DataPackageMetadata", spectraFileKey, sourceFileName);
                            }

                            if (saveMode.ToLower() != "copy")
                            {
                                m_jobParams.AddResultFileToSkip(sourceFileName);
                            }

                            MoveFileToFolder(diWorkingDirectory, diTargetFolder, sourceFileName);

                            if (m_DebugLevel >= 1)
                            {
                                LogMessage("Copied " + sourceFileName + " from folder " + sourceFolderPath);
                            }

                        }
                        catch (Exception ex)
                        {
                            LogError("RetrieveAggregateFiles; Exception during copy of file: " + sourceFileName + " from folder " + sourceFolderPath + " for job " + dataPkgJob.Key, ex);
                            return false;

                        }

                    }
                }

                // ReSharper disable once RedundantNameQualifier
                if (!m_MyEMSLUtilities.ProcessMyEMSLDownloadQueue(m_WorkingDir, MyEMSLReader.Downloader.DownloadFolderLayout.FlatNoSubfolders))
                {
                    return false;
                }

                // Restore the dataset and job info for this aggregation job
                OverrideCurrentDatasetAndJobInfo(currentDatasetAndJobInfo);

                success = true;

            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveAggregateFiles", ex);
                success = false;
            }

            return success;

        }

        /// <summary>
        /// Retrieves the PHRP files for the PeptideHit jobs defined for the data package associated with this aggregation job
        /// Also creates a batch file that can be manually run to retrieve the instrument data files
        /// </summary>
        /// <param name="udtOptions">File retrieval options</param>
        /// <param name="lstDataPackagePeptideHitJobs">Job info for the peptide_hit jobs associated with this data package (output parameter)</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool RetrieveDataPackagePeptideHitJobPHRPFiles(
            clsDataPackageFileHandler.udtDataPackageRetrievalOptionsType udtOptions,
            out List<clsDataPackageJobInfo> lstDataPackagePeptideHitJobs)
        {

            const float progressPercentAtStart = 0;
            const float progressPercentAtFinish = 20;
            return RetrieveDataPackagePeptideHitJobPHRPFiles(udtOptions, out lstDataPackagePeptideHitJobs, progressPercentAtStart, progressPercentAtFinish);
        }

        /// <summary>
        /// Retrieves the PHRP files for the PeptideHit jobs defined for the data package associated with this aggregation job
        /// Also creates a batch file that can be manually run to retrieve the instrument data files
        /// </summary>
        /// <param name="udtOptions">File retrieval options</param>
        /// <param name="lstDataPackagePeptideHitJobs">Output parameter: Job info for the peptide_hit jobs associated with this data package (output parameter)</param>
        /// <param name="progressPercentAtStart">Percent complete value to use for computing incremental progress</param>
        /// <param name="progressPercentAtFinish">Percent complete value to use for computing incremental progress</param>
        /// <returns>True if success, false if an error</returns>
        /// <remarks></remarks>
        protected bool RetrieveDataPackagePeptideHitJobPHRPFiles(
            clsDataPackageFileHandler.udtDataPackageRetrievalOptionsType udtOptions,
            out List<clsDataPackageJobInfo> lstDataPackagePeptideHitJobs,
            float progressPercentAtStart,
            float progressPercentAtFinish)
        {

            // Gigasax.DMS_Pipeline
            var connectionString = m_mgrParams.GetParam("brokerconnectionstring");

            var dataPackageID = m_jobParams.GetJobParameter("DataPackageID", -1);

            var dataPackageFileHander = new clsDataPackageFileHandler(connectionString, dataPackageID, this);
            RegisterEvents(dataPackageFileHander);

            var success = dataPackageFileHander.RetrieveDataPackagePeptideHitJobPHRPFiles(udtOptions, out lstDataPackagePeptideHitJobs, progressPercentAtStart, progressPercentAtFinish);

            return success;

        }

        /// <summary>
        /// Create a fasta file for Sequest, X!Tandem, Inspect, or MSGFPlus analysis
        /// </summary>
        /// <param name="localOrgDBFolder">Folder on analysis machine where fasta files are stored</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        /// <remarks>Stores the name of the FASTA file as a new job parameter named "generatedFastaName" in section "PeptideSearch"</remarks>
        protected bool RetrieveOrgDB(string localOrgDBFolder)
        {
            const int freeSpaceThresholdPercent = 20;

            Console.WriteLine();
            if (m_DebugLevel >= 3)
            {
                LogMessage("Obtaining Org DB file");
            }

            try
            {
                var proteinCollectionInfo = new clsProteinCollectionInfo(m_jobParams);

                double requiredFreeSpaceMB = 0;

                if (proteinCollectionInfo.UsingLegacyFasta)
                {
                    // Estimate the drive space required to download the fasta file and its associated MSGF+ the index files
                    requiredFreeSpaceMB = LookupLegacyDBDiskSpaceRequiredMB(proteinCollectionInfo);
                }

                // Delete old fasta files and suffix array files if getting low on disk space
                // Do not delete any files related to the current Legacy Fasta file (if defined)

                const int freeSpaceThresholdPercent = 20;

                var legacyFastaFileBaseName = string.Empty;

                if (proteinCollectionInfo.UsingLegacyFasta && !string.IsNullOrWhiteSpace(proteinCollectionInfo.LegacyFastaName) &&
                    proteinCollectionInfo.LegacyFastaName.ToLower() != "na")
                {
                    legacyFastaFileBaseName = Path.GetFileNameWithoutExtension(proteinCollectionInfo.LegacyFastaName);
                }

                PurgeFastaFilesIfLowFreeSpace(localOrgDBFolder, freeSpaceThresholdPercent, requiredFreeSpaceMB, legacyFastaFileBaseName);

                // Make a new fasta file from scratch
                if (!CreateFastaFile(proteinCollectionInfo, localOrgDBFolder))
                {
                    // There was a problem. Log entries in lower-level routines provide documentation
                    return false;
                }

                // Fasta file was successfully generated. Put the name of the generated fastafile in the
                // job data class for other methods to use
                if (!m_jobParams.AddAdditionalParameter("PeptideSearch", JOB_PARAM_GENERATED_FASTA_NAME, m_FastaFileName))
                {
                    LogError("Error adding parameter 'generatedFastaName' to m_jobParams");
                    return false;
                }

                // Delete old fasta files and suffix array files if getting low on disk space
                // No need to pass a value for legacyFastaFileBaseName because a .fasta.LastUsed file will have been created/updated by CreateFastaFile
                // This method is Windows-specific
                PurgeFastaFilesIfLowFreeSpace(localOrgDBFolder, freeSpaceThresholdPercent, 0, "");

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception in RetrieveOrgDB", ex);
                return false;
            }

            // We got to here OK, so return
            return true;

        }

        /// <summary>
        /// Overrides base class version of the function to creates a Sequest params file compatible
        /// with the Bioworks version on this System. Uses ParamFileGenerator dll provided by Ken Auberry
        /// </summary>
        /// <param name="paramFileName">Name of param file to be created</param>
        /// <returns>True for success; False for failure</returns>
        protected bool RetrieveGeneratedParamFile(string paramFileName)
        {

            IGenerateFile paramFileGenerator = null;

            try
            {
                LogMessage("Retrieving parameter file " + paramFileName);

                paramFileGenerator = new clsMakeParameterFile
                {
                    TemplateFilePath = m_mgrParams.GetParam("paramtemplateloc")
                };

                // Note that job parameter "generatedFastaName" gets defined by RetrieveOrgDB
                // Furthermore, the full path to the fasta file is only necessary when creating Sequest parameter files
                var toolName = m_jobParams.GetParam("ToolName", string.Empty);
                if (string.IsNullOrWhiteSpace(toolName))
                {
                    m_message = "Job parameter ToolName is empty";
                    return false;
                }

                var paramFileType = SetParamfileType(toolName);
                if (paramFileType == IGenerateFile.ParamFileType.Invalid)
                {
                    m_message = "Tool " + toolName + " is not supported by the ParamFileGenerator; update clsAnalysisResources and ParamFileGenerator.dll";
                    return false;
                }

                var fastaFilePath = Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("PeptideSearch", JOB_PARAM_GENERATED_FASTA_NAME));

                // Gigasax.DMS5
                var connectionString = m_mgrParams.GetParam("connectionstring");
                var datasetID = m_jobParams.GetJobParameter(clsAnalysisJob.JOB_PARAMETERS_SECTION, "DatasetID", 0);

                var success = paramFileGenerator.MakeFile(paramFileName, paramFileType, fastaFilePath, m_WorkingDir, connectionString, datasetID);

                // Examine the size of the ModDefs.txt file
                // Add it to the ignore list if it is empty (no point in tracking a 0-byte file)
                var fiModDefs = new FileInfo(Path.Combine(m_WorkingDir, Path.GetFileNameWithoutExtension(paramFileName) + "_ModDefs.txt"));
                if (fiModDefs.Exists && fiModDefs.Length == 0)
                {
                    m_jobParams.AddResultFileToSkip(fiModDefs.Name);
                }

                if (success)
                {
                    if (m_DebugLevel >= 3)
                    {
                        LogMessage("Successfully retrieved param file: " + paramFileName);
                    }

                    return true;
                }

                LogError(m_message + ": " + ParFileGen.LastError);
                return false;
            }
            catch (Exception ex)
            {
                if (string.IsNullOrWhiteSpace(m_message))
                {
                    m_message = "Error retrieving parameter file";
                }

                LogError(m_message, ex);

                if (!string.IsNullOrWhiteSpace(paramFileGenerator?.LastError))
                {
                    LogMessage("Error converting param file: " + paramFileGenerator.LastError, 0, true);
                }
                return false;
            }

        }

        /// <summary>
        /// Creates the specified settings file from db info
        /// </summary>
        /// <returns>TRUE if file created successfully; FALSE otherwise</returns>
        /// <remarks>Use this overload with jobs where settings file is retrieved from database</remarks>
        [Obsolete("Unused")]
        protected internal bool RetrieveSettingsFileFromDb()
        {

            var OutputFile = Path.Combine(m_WorkingDir, m_jobParams.GetParam("SettingsFileName"));

            return CreateSettingsFile(m_jobParams.GetParam("ParameterXML"), OutputFile);

        }

        /// <summary>
        /// Create file JobParams.xml in the working directory using in-memory job parameters
        /// </summary>
        /// <remarks>Adds JobParams.xml to the list of files to skip by calling m_jobParams.AddResultFileToSkip</remarks>
        protected void SaveCurrentJobParameters()
        {
            string xmlText;

            var objMemoryStream = new MemoryStream();
            using (var xWriter = new XmlTextWriter(objMemoryStream, System.Text.Encoding.UTF8))
            {

                xWriter.Formatting = Formatting.Indented;
                xWriter.Indentation = 2;
                xWriter.IndentChar = ' ';

                // Create the XML document in memory
                xWriter.WriteStartDocument(true);

                // General job information
                // Root level element
                xWriter.WriteStartElement("sections");

                foreach (var section in m_jobParams.GetAllSectionNames())
                {
                    xWriter.WriteStartElement("section");
                    xWriter.WriteAttributeString("name", section);

                    foreach (var parameter in m_jobParams.GetAllParametersForSection(section))
                    {
                        xWriter.WriteStartElement("item");
                        xWriter.WriteAttributeString("key", parameter.Key);
                        xWriter.WriteAttributeString("value", parameter.Value);
                        xWriter.WriteEndElement();
                    }

                    xWriter.WriteEndElement();      // section
                }

                // Close out the XML document (but do not close XWriter yet)
                xWriter.WriteEndDocument();
                xWriter.Flush();

                // Now use a StreamReader to copy the XML text to a string variable
                objMemoryStream.Seek(0, SeekOrigin.Begin);
                var srMemoryStreamReader = new StreamReader(objMemoryStream);
                xmlText = srMemoryStreamReader.ReadToEnd();

                srMemoryStreamReader.Close();
                objMemoryStream.Close();

                // Since xmlText now contains the XML, we can now safely close the xWriter
            }

            var jobParamsFile = new FileInfo(Path.Combine(WorkDir, clsAnalysisJob.OFFLINE_JOB_PARAMS_FILE));
            using (var writer = new StreamWriter(new FileStream(jobParamsFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                writer.WriteLine(xmlText);
            }

            m_jobParams.AddResultFileToSkip(jobParamsFile.Name);

        }

        /// <summary>
        /// Specifies the Bioworks version for use by the Param File Generator DLL
        /// </summary>
        /// <param name="toolName">Version specified in mgr config file</param>
        /// <returns>IGenerateFile.ParamFileType based on input version</returns>
        /// <remarks></remarks>
        protected IGenerateFile.ParamFileType SetParamfileType(string toolName)
        {

            var toolNameToTypeMapping = new Dictionary<string, IGenerateFile.ParamFileType>(StringComparer.OrdinalIgnoreCase)
            {
                {"sequest", IGenerateFile.ParamFileType.BioWorks_Current},
                {"xtandem", IGenerateFile.ParamFileType.X_Tandem},
                {"inspect", IGenerateFile.ParamFileType.Inspect},
                {"msgfplus", IGenerateFile.ParamFileType.MSGFPlus},
                {"msalign_histone", IGenerateFile.ParamFileType.MSAlignHistone},
                {"msalign", IGenerateFile.ParamFileType.MSAlign},
                {"moda", IGenerateFile.ParamFileType.MODa},
                {"mspathfinder", IGenerateFile.ParamFileType.MSPathFinder},
                {"modplus", IGenerateFile.ParamFileType.MODPlus}
            };


            if (toolNameToTypeMapping.TryGetValue(toolName, out var paramFileType))
            {
                return paramFileType;
            }

            var toolNameLCase = toolName.ToLower();

            foreach (var entry in toolNameToTypeMapping)
            {
                if (toolNameLCase.Contains(entry.Key.ToLower()))
                {
                    return entry.Value;
                }
            }

            return IGenerateFile.ParamFileType.Invalid;
        }

        /// <summary>
        /// Converts the dictionary items to a list of key/value pairs separated by an equals sign
        /// Next, calls StorePackedJobParameterList to store the list (items will be separated by tab characters)
        /// </summary>
        /// <param name="dctItems">Dictionary items to store as a packed job parameter</param>
        /// <param name="parameterName">Packed job parameter name</param>
        /// <remarks></remarks>
        protected void StorePackedJobParameterDictionary(Dictionary<string, int> dctItems, string parameterName)
        {
            var lstItems = new List<string>();

            foreach (var item in dctItems)
            {
                lstItems.Add(item.Key + "=" + item.Value);
            }

            StorePackedJobParameterList(lstItems, parameterName);

        }
        /// <summary>
        /// Converts the dictionary items to a list of key/value pairs separated by an equals sign
        /// Next, calls StorePackedJobParameterList to store the list (items will be separated by tab characters)
        /// </summary>
        /// <param name="dctItems">Dictionary items to store as a packed job parameter</param>
        /// <param name="parameterName">Packed job parameter name</param>
        /// <remarks></remarks>
        public void StorePackedJobParameterDictionary(Dictionary<string, string> dctItems, string parameterName)
        {
            var lstItems = new List<string>();

            foreach (var item in dctItems)
            {
                lstItems.Add(item.Key + "=" + item.Value);
            }

            StorePackedJobParameterList(lstItems, parameterName);

        }

        /// <summary>
        /// Convert a string list to a packed job parameter (items are separated by tab characters)
        /// </summary>
        /// <param name="lstItems">List items to store as a packed job parameter</param>
        /// <param name="parameterName">Packed job parameter name</param>
        /// <remarks></remarks>
        protected void StorePackedJobParameterList(List<string> lstItems, string parameterName)
        {
            m_jobParams.AddAdditionalParameter(clsAnalysisJob.JOB_PARAMETERS_SECTION, parameterName, clsGlobal.FlattenList(lstItems, "\t"));

        }

        /// <summary>
        /// Unzips all files in the specified Zip file
        /// If the file is less than 1.25 GB in size then uses Ionic.Zip
        /// Otherwise, uses PKZipC (provided PKZipC.exe exists)
        /// </summary>
        /// <param name="zipFilePath">File to unzip</param>
        /// <param name="outFolderPath">Target directory for the extracted files</param>
        /// <param name="callingFunctionName">Calling function name (used for debugging purposes)</param>
        /// <param name="forceExternalZipProgramUse">If True, then force use of PKZipC.exe</param>
        /// <returns>True if success, otherwise false</returns>
        /// <remarks></remarks>
        public bool UnzipFileStart(string zipFilePath, string outFolderPath, string callingFunctionName, bool forceExternalZipProgramUse)
        {
            var success = m_FileSearch.UnzipFileStart(zipFilePath, outFolderPath, callingFunctionName, forceExternalZipProgramUse);
            return success;
        }

        /// <summary>
        /// Update m_message, which is logged in the pipeline job steps table when the job step finishes
        /// </summary>
        /// <param name="statusMessage">New status message</param>
        /// <param name="appendToExisting">True to append to m_message; false to overwrite it</param>
        public void UpdateStatusMessage(string statusMessage, bool appendToExisting = false)
        {
            if (appendToExisting)
            {
                m_message = clsGlobal.AppendToComment(m_message, statusMessage);
            }
            else
            {
                m_message = statusMessage;
            }

            LogDebugMessage(m_message);
        }

        /// <summary>
        /// Removes any spectra with 2 or fewer ions in a _DTA.txt ifle
        /// </summary>
        /// <param name="workDir">Folder with the CDTA file</param>
        /// <param name="inputFileName">CDTA filename</param>
        /// <returns>True if success; false if an error</returns>
        protected bool ValidateCDTAFileRemoveSparseSpectra(string workDir, string inputFileName)
        {
            var success = m_CDTAUtilities.RemoveSparseSpectra(workDir, inputFileName);
            if (!success && string.IsNullOrEmpty(m_message))
            {
                m_message = "m_CDTAUtilities.RemoveSparseSpectra returned False";
            }

            return success;

        }

        /// <summary>
        /// Makes sure the specified _DTA.txt file has scan=x and cs=y tags in the parent ion line
        /// </summary>
        /// <param name="sourceFilePath">Input _DTA.txt file to parse</param>
        /// <param name="replaceSourceFile">If True, then replaces the source file with and updated file</param>
        /// <param name="deleteSourceFileIfUpdated">
        /// Only valid if replaceSourceFile=True;
        /// If True, then the source file is deleted if an updated version is created.
        /// If false, then the source file is renamed to .old if an updated version is created.
        /// </param>
        /// <param name="outputFilePath">
        /// Output file path to use for the updated file; required if replaceSourceFile=False; ignored if replaceSourceFile=True
        /// </param>
        /// <returns>True if success; false if an error</returns>
        protected bool ValidateCDTAFileScanAndCSTags(string sourceFilePath, bool replaceSourceFile, bool deleteSourceFileIfUpdated, string outputFilePath)
        {
            var success = m_CDTAUtilities.ValidateCDTAFileScanAndCSTags(sourceFilePath, replaceSourceFile, deleteSourceFileIfUpdated, outputFilePath);
            if (!success && string.IsNullOrEmpty(m_message))
            {
                m_message = "m_CDTAUtilities.ValidateCDTAFileScanAndCSTags returned False";
            }

            return success;

        }

        /// <summary>
        /// Condenses CDTA files that are over 2 GB in size
        /// </summary>
        /// <param name="workDir"></param>
        /// <param name="inputFileName"></param>
        /// <returns></returns>
        /// <remarks></remarks>
        protected bool ValidateCDTAFileSize(string workDir, string inputFileName)
        {
            var success = m_CDTAUtilities.ValidateCDTAFileSize(workDir, inputFileName);
            if (!success && string.IsNullOrEmpty(m_message))
            {
                m_message = "m_CDTAUtilities.ValidateCDTAFileSize returned False";
            }

            return success;

        }

        /// <summary>
        /// Validate that data in a _dta.txt ifle is centroided
        /// </summary>
        /// <param name="strCDTAPath"></param>
        /// <returns></returns>
        public bool ValidateCDTAFileIsCentroided(string strCDTAPath)
        {

            try
            {
                // Read the m/z values in the _dta.txt file
                // Examine the data in each spectrum to determine if it is centroided

                mSpectraTypeClassifier = new SpectraTypeClassifier.clsSpectrumTypeClassifier();
                mSpectraTypeClassifier.ErrorEvent += mSpectraTypeClassifier_ErrorEvent;
                mSpectraTypeClassifier.ReadingSpectra += mSpectraTypeClassifier_ReadingSpectra;

                var success = mSpectraTypeClassifier.CheckCDTAFile(strCDTAPath);

                if (!success)
                {
                    if (string.IsNullOrEmpty(m_message))
                    {
                        LogError("SpectraTypeClassifier encountered an error while parsing the _dta.txt file");
                    }

                    return false;
                }

                var fractionCentroided = mSpectraTypeClassifier.FractionCentroided();

                var commentSuffix = " (" + mSpectraTypeClassifier.TotalSpectra() + " total spectra)";

                if (fractionCentroided > 0.8)
                {
                    // At least 80% of the spectra are centroided

                    if (fractionCentroided > 0.999)
                    {
                        LogMessage("All of the spectra are centroided" + commentSuffix);
                    }
                    else
                    {
                        LogMessage((fractionCentroided * 100).ToString("0") + "% of the spectra are centroided" + commentSuffix);
                    }

                    return true;

                }

                if (fractionCentroided > 0.001)
                {
                    // Less than 80% of the spectra are centroided
                    // Post a message similar to:
                    //   MSGF+ will likely skip 90% of the spectra because they did not appear centroided
                    m_message = "MSGF+ will likely skip " + ((1 - fractionCentroided) * 100).ToString("0") + "% of the spectra because they do not appear centroided";
                    LogMessage(m_message + commentSuffix);
                    return false;
                }

                // None of the spectra are centroided; unable to process with MSGF+
                m_message = SPECTRA_ARE_NOT_CENTROIDED + " with MSGF+";
                LogMessage(m_message + commentSuffix, 0, true);
                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception in ValidateCDTAFileIsCentroided", ex);
                return false;
            }

        }

        /// <summary>
        /// Validate that the specified file exists and has at least one tab-delimited row with a numeric value in the first column
        /// </summary>
        /// <param name="filePath">Path to the file</param>
        /// <param name="fileDescription">File description, e.g. Synopsis</param>
        /// <param name="errorMessage"></param>
        /// <returns>True if the file has data; otherwise false</returns>
        /// <remarks></remarks>
        public static bool ValidateFileHasData(string filePath, string fileDescription, out string errorMessage)
        {
            const int numericDataColIndex = 0;
            return ValidateFileHasData(filePath, fileDescription, out errorMessage, numericDataColIndex);
        }

        /// <summary>
        /// Validate that the specified file exists and has at least one tab-delimited row with a numeric value
        /// </summary>
        /// <param name="filePath">Path to the file</param>
        /// <param name="fileDescription">File description, e.g. Synopsis</param>
        /// <param name="errorMessage"></param>
        /// <param name="numericDataColIndex">Index of the numeric data column; use -1 to simply look for any text in the file</param>
        /// <returns>True if the file has data; otherwise false</returns>
        /// <remarks></remarks>
        public static bool ValidateFileHasData(string filePath, string fileDescription, out string errorMessage, int numericDataColIndex)
        {

            var dataFound = false;

            errorMessage = string.Empty;

            try
            {
                var fiFileInfo = new FileInfo(filePath);

                if (!fiFileInfo.Exists)
                {
                    errorMessage = fileDescription + " file not found: " + fiFileInfo.Name;
                    return false;
                }

                if (fiFileInfo.Length == 0)
                {
                    errorMessage = fileDescription + " file is empty (zero-bytes)";
                    return false;
                }

                // Open the file and confirm it has data rows
                using (var srInFile = new StreamReader(new FileStream(fiFileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                {
                    while (!srInFile.EndOfStream && !dataFound)
                    {
                        var lineIn = srInFile.ReadLine();
                        if (string.IsNullOrEmpty(lineIn))
                            continue;

                        if (numericDataColIndex < 0)
                        {
                            dataFound = true;
                        }
                        else
                        {
                            // Split on the tab character and check if the first column is numeric
                            var splitLine = lineIn.Split('\t');

                            if (splitLine.Length <= numericDataColIndex)
                                continue;

                            if (double.TryParse(splitLine[numericDataColIndex], out var _))
                            {
                                dataFound = true;
                            }
                        }
                    }
                }

                if (!dataFound)
                {
                    errorMessage = fileDescription + " is empty (no data)";
                }

            }
            catch (Exception)
            {
                errorMessage = "Exception validating " + fileDescription + " file";
                return false;
            }

            return dataFound;

        }

        /// <summary>
        /// Validates that sufficient free memory is available to run Java
        /// </summary>
        /// <param name="memorySizeJobParamName">Name of the job parameter that defines the amount of memory (in MB) that must be available on the system</param>
        /// <param name="logFreeMemoryOnSuccess">If True, then post a log entry if sufficient memory is, in fact, available</param>
        /// <returns>True if sufficient free memory; false if not enough free memory</returns>
        /// <remarks>Typical names for javaMemorySizeJobParamName are MSGFJavaMemorySize, MSGFDBJavaMemorySize, and MSDeconvJavaMemorySize.
        /// These parameters are loaded from DMS Settings Files (table T_Settings_Files in DMS5, copied to table T_Job_Parameters in DMS_Pipeline)
        /// </remarks>
        protected bool ValidateFreeMemorySize(string memorySizeJobParamName, bool logFreeMemoryOnSuccess = true)
        {
            // Lookup parameter memorySizeJobParamName; assume 2000 MB if not defined
            var freeMemoryRequiredMB = m_jobParams.GetJobParameter(memorySizeJobParamName, 2000);

            // Require freeMemoryRequiredMB be at least 0.5 GB
            if (freeMemoryRequiredMB < 512)
                freeMemoryRequiredMB = 512;

            if (m_DebugLevel < 1)
                logFreeMemoryOnSuccess = false;

            return ValidateFreeMemorySize(freeMemoryRequiredMB, StepToolName, logFreeMemoryOnSuccess);

        }

        /// <summary>
        /// Verify that the system has the specified amount of memory available
        /// </summary>
        /// <param name="freeMemoryRequiredMB"></param>
        /// <param name="stepToolName"></param>
        /// <param name="logFreeMemoryOnSuccess"></param>
        /// <returns></returns>
        public static bool ValidateFreeMemorySize(int freeMemoryRequiredMB, string stepToolName, bool logFreeMemoryOnSuccess)
        {

            var freeMemoryMB = clsGlobal.GetFreeMemoryMB();

            if (freeMemoryRequiredMB >= freeMemoryMB)
            {
                var errMsg = "Not enough free memory to run " + stepToolName + "; " +
                             "need " + freeMemoryRequiredMB + " MB but " +
                             "system has " + freeMemoryMB.ToString("0") + " MB available";

                clsGlobal.LogError(errMsg);
                return false;
            }

            if (logFreeMemoryOnSuccess)
            {
                // Example message: MSGF+ will use 4000 MB; system has 7296 MB available
                var message = stepToolName + " will use " + freeMemoryRequiredMB + " MB; " +
                             "system has " + freeMemoryMB.ToString("0") + " MB available";
                clsGlobal.LogDebug(message);
            }

            return true;
        }

        #endregion

        #region "Event Handlers"

        private void m_CDTAUtilities_ProgressEvent(string taskDescription, float percentComplete)
        {

            if (m_DebugLevel >= 1)
            {
                if (m_DebugLevel == 1 && DateTime.UtcNow.Subtract(m_LastCDTAUtilitiesUpdateTime).TotalSeconds >= 60 ||
                    m_DebugLevel > 1 && DateTime.UtcNow.Subtract(m_LastCDTAUtilitiesUpdateTime).TotalSeconds >= 20)
                {
                    m_LastCDTAUtilitiesUpdateTime = DateTime.UtcNow;

                    LogDebugMessage(" ... CDTAUtilities: " + percentComplete.ToString("0.00") + "% complete");
                }
            }

        }

        private void m_FastaTools_FileGenerationCompleted(string FullOutputPath)
        {
            // Get the name of the fasta file that was generated
            m_FastaFileName = Path.GetFileName(FullOutputPath);
        }

        private void m_FastaTools_FileGenerationProgress(string statusMsg, double fractionDone)
        {

            const int MINIMUM_LOG_INTERVAL_SEC = 10;

            var forcelog = m_DebugLevel >= 1 && statusMsg.Contains(Protein_Exporter.clsGetFASTAFromDMS.LOCK_FILE_PROGRESS_TEXT);

            if (m_DebugLevel >= 3 || forcelog)
            {
                // Limit the logging to once every MINIMUM_LOG_INTERVAL_SEC seconds
                if (forcelog || DateTime.UtcNow.Subtract(m_FastaToolsLastLogTime).TotalSeconds >= MINIMUM_LOG_INTERVAL_SEC ||
                    fractionDone - m_FastaToolFractionDoneSaved >= 0.25)
                {
                    m_FastaToolsLastLogTime = DateTime.UtcNow;
                    m_FastaToolFractionDoneSaved = fractionDone;
                    LogDebugMessage("Generating Fasta file, " + (fractionDone * 100).ToString("0.0") + "% complete, " + statusMsg);
                }
            }

        }

        private void m_SplitFastaFileUtility_ErrorEvent(string errorMessage, Exception ex)
        {
            LogError(errorMessage, ex);
        }

        private void m_SplitFastaFileUtility_ProgressUpdate(string progressMessage, float percentComplete)
        {

            if (m_DebugLevel >= 1)
            {

                if (m_DebugLevel == 1 && DateTime.UtcNow.Subtract(m_SplitFastaLastUpdateTime).TotalSeconds >= 60 ||
                    m_DebugLevel > 1 && DateTime.UtcNow.Subtract(m_SplitFastaLastUpdateTime).TotalSeconds >= 20 ||
                    percentComplete >= 100 && m_SplitFastaLastPercentComplete < 100)
                {
                    m_SplitFastaLastUpdateTime = DateTime.UtcNow;
                    m_SplitFastaLastPercentComplete = percentComplete;

                    if (percentComplete > 0)
                    {
                        LogDebugMessage(" ... " + progressMessage + ", " + percentComplete + "% complete");
                    }
                    else
                    {
                        LogDebugMessage(" ... SplitFastaFile: " + progressMessage);
                    }
                }
            }

        }

        private void m_SplitFastaFileUtility_SplittingBaseFastaFile(string baseFastaFileName, int numSplitParts)
        {
            LogDebugMessage("Splitting " + baseFastaFileName + " into " + numSplitParts + " parts");
        }

        #endregion

        #region "FileCopyUtilities Events"

        private void FileCopyUtilities_CopyWithLocksComplete(DateTime startTimeUtc, string destFilePath)
        {
            LogCopyStats(startTimeUtc, destFilePath);
        }

        private void FileCopyUtilities_ResetTimestampForQueueWaitTime()
        {
            ResetTimestampForQueueWaitTimeLogging();
        }

        #endregion

        #region "SpectraTypeClassifier Events"

        private void mSpectraTypeClassifier_ErrorEvent(string message)
        {
        }

        private void mSpectraTypeClassifier_ReadingSpectra(int spectraProcessed)
        {
            LogDebugMessage(" ... " + spectraProcessed + " spectra parsed in the _dta.txt file");
        }

        #endregion

    }

}